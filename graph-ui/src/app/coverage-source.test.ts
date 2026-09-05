/*
 * Die Coverage-Quelle: wer wie oft gefragt wird.
 *
 * Der Client wird durch einen Doppelgaenger ersetzt, der jede Anfrage
 * mitschreibt. Gefragt wird nicht, ob die Antworten richtig gelesen werden
 * (das steht in tree-model.test.ts), sondern ob paginiert wird, wann
 * aufgehoert wird, und ob ein Fehler ein Fehler bleibt statt ein leerer
 * Befund zu werden.
 */
import { describe, expect, it, vi } from 'vitest';
import {
    COVERAGE_MAX_PAGES,
    COVERAGE_ROOT_SCOPE,
    COVERAGE_SCOPE_LIMIT,
    loadCoverage,
    loadPathCoverage,
} from './coverage-source';
import type { RpcIntelligenceClient } from '../provider/rpc-client';

const STATUS = {
    project: 'p',
    parse_partial: { files: [{ path: 'src/a.ts', error_ranges: '4-9' }], count: 1, truncated: false },
    skipped: { files: [], count: 0, truncated: false },
    not_indexed: { dirs: [], dirs_count: 0, files: [], files_count: 0, truncated: false },
};

const METADATA = {
    generation: 'g',
    index_mode: 'full',
    recorded_at: 'r',
    recording_status: 'complete',
    ignored_files_stored: 2,
    ignored_files_total: 7,
    hash_records_complete: true,
    coverage_version: 2,
    generation_matches: true,
};

/** Eine Scope-Seite bauen, so wie der Server sie schreibt. */
function page(entries: { path: string; kind: string }[], hasMore: boolean, nextOffset?: number) {
    return {
        project: 'p',
        metadata: METADATA,
        paths: [],
        scopes: [
            {
                requested_scope: '.',
                scope: '.',
                total: 42,
                has_more: hasMore,
                ...(nextOffset === undefined ? {} : { next_offset: nextOffset }),
                entries: entries.map((entry) => ({ ...entry, detail: '' })),
                status: 'known_gaps',
            },
        ],
    };
}

/** Ein Client, der genau die vorbereiteten Antworten gibt und alles mitschreibt. */
function fakeClient(pages: unknown[], status: unknown = STATUS) {
    const calls: Record<string, unknown>[] = [];
    let index = 0;
    const client = {
        indexStatusPayload: vi.fn(async () => {
            calls.push({ tool: 'index_status' });
            return status;
        }),
        checkIndexCoverage: vi.fn(async (_project: string, args: Record<string, unknown>) => {
            calls.push({ tool: 'check_index_coverage', ...args });
            const answer = pages[Math.min(index, pages.length - 1)];
            index += 1;
            return answer;
        }),
    } as unknown as RpcIntelligenceClient;
    return { client, calls };
}

describe('loadCoverage', () => {

    it('fragt beide Quellen und legt sie zusammen', async () => {
        const { client, calls } = fakeClient([page([{ path: 'src/b.ts', kind: 'discovery' }], false)]);
        const reading = await loadCoverage(client, 'p');
        expect(calls[0]).toEqual({ tool: 'index_status' });
        expect(calls[1]).toMatchObject({
            tool: 'check_index_coverage',
            scopes: [COVERAGE_ROOT_SCOPE],
            scopeLimit: COVERAGE_SCOPE_LIMIT,
            scopeOffset: 0,
        });
        expect(reading.index.records.get('src/a.ts')?.state).toBe('partial');
        expect(reading.index.records.get('src/b.ts')?.state).toBe('skipped');
    });

    it('reicht die Metadaten des Stores durch', async () => {
        const { client } = fakeClient([page([], false)]);
        const reading = await loadCoverage(client, 'p');
        expect(reading.answer.metadata.ignoredFilesStored).toBe(2);
        expect(reading.answer.metadata.ignoredFilesTotal).toBe(7);
    });

    it('holt weitere Seiten, solange der Server has_more sagt', async () => {
        const { client, calls } = fakeClient([
            page([{ path: 'a.ts', kind: 'discovery' }], true, 1),
            page([{ path: 'b.ts', kind: 'discovery' }], true, 2),
            page([{ path: 'c.ts', kind: 'discovery' }], false),
        ]);
        const reading = await loadCoverage(client, 'p');
        const offsets = calls
            .filter((call) => call['tool'] === 'check_index_coverage')
            .map((call) => call['scopeOffset']);
        expect(offsets).toEqual([0, 1, 2]);
        expect(reading.scopes).toHaveLength(3);
        expect([...reading.index.records.keys()]).toEqual(
            expect.arrayContaining(['a.ts', 'b.ts', 'c.ts']),
        );
    });

    it('haelt an, wenn der Server has_more sagt und den Zeiger nicht bewegt', async () => {
        // Ohne diesen Abbruch waere das eine Endlosschleife im Browser.
        const { client, calls } = fakeClient([page([{ path: 'a.ts', kind: 'discovery' }], true, 0)]);
        const reading = await loadCoverage(client, 'p');
        expect(calls.filter((call) => call['tool'] === 'check_index_coverage')).toHaveLength(1);
        expect(reading.index.truncations.length).toBe(1);
    });

    it('hoert spaetestens am Seitendeckel auf und laesst die Kappung stehen', async () => {
        const { client, calls } = fakeClient(
            Array.from({ length: 40 }, (_unused, i) => page([{ path: `f${i}.ts`, kind: 'discovery' }], true, i + 1)),
        );
        const reading = await loadCoverage(client, 'p');
        expect(calls.filter((call) => call['tool'] === 'check_index_coverage'))
            .toHaveLength(COVERAGE_MAX_PAGES);
        expect(reading.index.truncations.length).toBeGreaterThan(0);
    });

    it('bricht ab, statt einen leeren Befund zu liefern', async () => {
        const client = {
            indexStatusPayload: vi.fn(async () => STATUS),
            checkIndexCoverage: vi.fn(async () => {
                throw new Error('boom');
            }),
        } as unknown as RpcIntelligenceClient;
        await expect(loadCoverage(client, 'p')).rejects.toThrow('boom');
    });
});

describe('loadPathCoverage', () => {

    it('fragt genau einen Pfad und liefert die Frische', async () => {
        const calls: Record<string, unknown>[] = [];
        const client = {
            checkIndexCoverage: vi.fn(async (_project: string, args: Record<string, unknown>) => {
                calls.push(args);
                return {
                    project: 'p',
                    metadata: METADATA,
                    paths: [
                        {
                            requested_path: 'src/a.ts',
                            path: 'src/a.ts',
                            status: 'partial',
                            freshness: 'metadata_changed',
                            recommended_action: 'read_source_and_reindex',
                            coverage: [{ path: 'src/a.ts', kind: 'parse_partial', detail: '4-9', match: 'exact' }],
                        },
                    ],
                    scopes: [],
                };
            }),
        } as unknown as RpcIntelligenceClient;
        const answer = await loadPathCoverage(client, 'p', 'src/a.ts');
        expect(calls[0]).toEqual({ paths: ['src/a.ts'] });
        expect(answer?.freshness).toBe('metadata_changed');
        expect(answer?.coverage[0]?.match).toBe('exact');
    });

    it('liefert undefined, wenn der Server zu dem Pfad nichts sagt', async () => {
        const client = {
            checkIndexCoverage: vi.fn(async () => ({ paths: [], scopes: [] })),
        } as unknown as RpcIntelligenceClient;
        expect(await loadPathCoverage(client, 'p', 'x.ts')).toBeUndefined();
    });
});
