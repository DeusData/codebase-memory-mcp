/*
 * Das Baum-Modell gegen eine woertlich abgeschriebene Antwort von /api/tree.
 *
 * Die Nutzlast unten ist das, was der gebaute Server am 2026-08-28 fuer
 * `GET /api/tree?project=probe-small` geliefert hat, inklusive der beiden
 * Eigenheiten, die dieses Modul behandelt: der Ordner `src` kommt zweimal
 * (einmal als "dir", einmal als "file", weil der Folder-Knoten seinen eigenen
 * Pfad als file_path traegt), und `{}` ist ein Kind, das keine Datei ist.
 * Eine geglaettete Nutzlast waere hier wertlos: genau diese Zeilen sind der
 * Grund fuer den Code.
 */
import { describe, expect, it } from 'vitest';
import {
    aggregateCoverage,
    buildCoverageIndex,
    compareChildren,
    coverageStateForKind,
    directoryPaths,
    flattenTree,
    mergeCoverageIntoLevels,
    moveCursor,
    namesAPath,
    normalizeCoveragePath,
    parentPath,
    readCoverageAnswer,
    readIndexStatusCoverage,
    readTreeLevel,
    treeIntent,
    worstCoverage,
} from './tree-model';
import type { TreeLevel, TreeRow } from './tree-model';

/** Woertlich: GET /api/tree?project=probe-small */
const ROOT_PAYLOAD = {
    path: '',
    files: 18,
    symbols: 71,
    children: [
        { name: 'src', path: 'src', kind: 'dir', symbols: 62, files: 13, region: 3 },
        { name: 'test', path: 'test', kind: 'dir', symbols: 2, files: 1, region: 3 },
        { name: 'HERKUNFT.md', path: 'HERKUNFT.md', kind: 'file', symbols: 3, files: 1, region: 3 },
        { name: '{}', path: '{}', kind: 'file', symbols: 2, files: 1, region: 3 },
        { name: 'src', path: 'src', kind: 'file', symbols: 1, files: 1, region: 3 },
        { name: 'test', path: 'test', kind: 'file', symbols: 1, files: 1, region: 3 },
    ],
};

/** Woertlich: GET /api/tree?project=probe-small&path=src/services */
const SERVICES_PAYLOAD = {
    path: 'src/services',
    files: 2,
    symbols: 10,
    children: [
        { name: 'userService.ts', path: 'src/services/userService.ts', kind: 'file', symbols: 6, files: 1 },
        { name: 'orderService.ts', path: 'src/services/orderService.ts', kind: 'file', symbols: 4, files: 1 },
    ],
};

describe('readTreeLevel', () => {

    const level = readTreeLevel(ROOT_PAYLOAD);

    it('legt den Ordner und seinen Folder-Knoten zu einer Zeile zusammen', () => {
        const src = level.children.filter((child) => child.path === 'src');
        expect(src).toHaveLength(1);
        expect(src[0]?.kind).toBe('dir');
        expect(level.foldedDuplicates).toBe(2);
    });

    it('laesst weg, was keinen Pfad benennt, und zaehlt es', () => {
        expect(level.children.map((child) => child.name)).not.toContain('{}');
        expect(level.droppedNonPaths).toBe(1);
    });

    it('haelt die Zahlen des Servers fest', () => {
        expect(level.files).toBe(18);
        expect(level.symbols).toBe(71);
        expect(level.children.find((child) => child.path === 'src')?.symbols).toBe(62);
    });

    it('sortiert Ordner vor Dateien, beides alphabetisch', () => {
        expect(level.children.map((child) => child.path)).toEqual(['src', 'test', 'HERKUNFT.md']);
    });

    it('vertraegt eine Antwort, die gar keine ist', () => {
        const empty = readTreeLevel(undefined);
        expect(empty.children).toEqual([]);
        expect(empty.files).toBe(0);
    });

    it('nimmt missed nur mit, wenn der Server es sagt', () => {
        const withMissed = readTreeLevel({
            path: '',
            children: [{ name: 'a', path: 'a', kind: 'dir', symbols: 1, files: 1, missed: 2 }],
        });
        expect(withMissed.children[0]?.missed).toBe(2);
        expect(level.children[0]?.missed).toBeUndefined();
    });
});

describe('namesAPath', () => {

    it('kennt genau die eine Form, die kein Pfad ist', () => {
        expect(namesAPath({ name: '{}', path: '{}' })).toBe(false);
        expect(namesAPath({ name: '', path: '' })).toBe(false);
        // Alles andere bleibt: eine verschluckte Datei waere schlimmer als eine
        // haessliche Zeile.
        expect(namesAPath({ name: 'weird name.ts', path: 'weird name.ts' })).toBe(true);
    });
});

describe('compareChildren', () => {

    it('stellt Ordner vor Dateien', () => {
        const dir = { name: 'z', path: 'z', kind: 'dir' as const, symbols: 0, files: 0 };
        const file = { name: 'a', path: 'a', kind: 'file' as const, symbols: 0, files: 0 };
        expect(compareChildren(dir, file)).toBeLessThan(0);
    });
});

describe('flattenTree', () => {

    const levels = new Map<string, TreeLevel>([
        ['', readTreeLevel(ROOT_PAYLOAD)],
        ['src/services', readTreeLevel(SERVICES_PAYLOAD)],
    ]);

    it('zeigt ohne aufgeklappte Ordner nur die oberste Ebene', () => {
        const rows = flattenTree(levels, new Set());
        expect(rows.map((row) => row.path)).toEqual(['src', 'test', 'HERKUNFT.md']);
        expect(rows.every((row) => row.depth === 0)).toBe(true);
    });

    it('haengt eine geladene Ebene unter ihren aufgeklappten Ordner', () => {
        const withServices = new Map(levels);
        withServices.set('src', readTreeLevel({
            path: 'src',
            children: [{ name: 'services', path: 'src/services', kind: 'dir', symbols: 10, files: 2 }],
        }));
        const rows = flattenTree(withServices, new Set(['src', 'src/services']));
        expect(rows.map((row) => row.path)).toEqual([
            'src',
            'src/services',
            'src/services/orderService.ts',
            'src/services/userService.ts',
            'test',
            'HERKUNFT.md',
        ]);
        expect(rows.find((row) => row.path === 'src/services/userService.ts')?.depth).toBe(2);
    });

    it('erzeugt fuer einen aufgeklappten, aber ungeladenen Ordner keine Zeilen', () => {
        const rows = flattenTree(levels, new Set(['test']));
        expect(rows.map((row) => row.path)).toEqual(['src', 'test', 'HERKUNFT.md']);
        expect(rows.find((row) => row.path === 'test')?.loaded).toBe(false);
        expect(rows.find((row) => row.path === 'test')?.expanded).toBe(true);
    });
});

describe('directoryPaths', () => {

    it('nennt die Ordner einer Ebene in Anzeigeordnung', () => {
        expect(directoryPaths(readTreeLevel(ROOT_PAYLOAD))).toEqual(['src', 'test']);
    });
});

describe('parentPath', () => {

    it('geht eine Ebene hoch und endet an der Wurzel', () => {
        expect(parentPath('src/services/userService.ts')).toBe('src/services');
        expect(parentPath('src')).toBe('');
    });
});

const row = (kind: 'dir' | 'file', expanded = false): TreeRow => ({
    name: 'x',
    path: 'x',
    kind,
    symbols: 0,
    files: 0,
    depth: 0,
    expanded,
    loaded: true,
});

describe('treeIntent', () => {

    it('oeffnet eine Datei mit Enter', () => {
        expect(treeIntent('Enter', row('file'))).toBe('open');
    });

    it('klappt einen Ordner mit Enter auf und wieder zu', () => {
        expect(treeIntent('Enter', row('dir'))).toBe('expand');
        expect(treeIntent('Enter', row('dir', true))).toBe('collapse');
    });

    it('geht mit Rechts in einen aufgeklappten Ordner hinein', () => {
        expect(treeIntent('ArrowRight', row('dir'))).toBe('expand');
        expect(treeIntent('ArrowRight', row('dir', true))).toBe('down');
        expect(treeIntent('ArrowRight', row('file'))).toBe('none');
    });

    it('klappt mit Links zu, sonst geht es zum Elternordner', () => {
        expect(treeIntent('ArrowLeft', row('dir', true))).toBe('collapse');
        expect(treeIntent('ArrowLeft', row('file'))).toBe('toParent');
    });

    it('laesst jede andere Taste durch', () => {
        expect(treeIntent('a', row('file'))).toBe('none');
        expect(treeIntent('Enter', undefined)).toBe('none');
    });
});

describe('moveCursor', () => {

    it('haelt an den Enden fest, statt umzubrechen', () => {
        expect(moveCursor(3, 0, -1)).toBe(0);
        expect(moveCursor(3, 2, 1)).toBe(2);
        expect(moveCursor(3, 1, 1)).toBe(2);
        expect(moveCursor(0, 0, 1)).toBe(0);
    });
});

/*
 * Der Coverage-Join.
 *
 * Die beiden Nutzlasten unten sind die Formen, die der gebaute Server aus
 * PR 1860 schreibt (mcp.c, add_coverage_report und handle_check_index_coverage).
 * Sie stehen hier vollstaendig, inklusive der Felder, die dieses Projekt nicht
 * liest: eine gekuerzte Nutzlast waere ein Test gegen die eigene Erwartung
 * statt gegen die Antwort.
 */

/** Die Coverage-Beilage einer index_status-Antwort. */
const STATUS_PAYLOAD = {
    project: 'probe-small',
    nodes: 71,
    edges: 132,
    status: 'ready',
    root_path: '/tmp/probe-small',
    parse_partial: {
        files: [{ path: 'src/broken.ts', error_ranges: '12-18,24' }],
        count: 1,
        truncated: false,
    },
    skipped: {
        files: [{ path: 'assets/beleg.png', reason: 'unsupported extension', phase: 'discovery' }],
        count: 1,
        truncated: false,
    },
    not_indexed: {
        dirs: ['node_modules'],
        dirs_count: 1,
        files: [{ path: 'secret.env', reason: '.gitignore' }],
        files_count: 1,
        truncated: false,
        note: 'Purposely not indexed - excluded BY DESIGN via gitignore/.cbmignore/skip-lists.',
    },
    coverage_note: 'Best-effort signal, not a completeness guarantee.',
};

/** Eine Scope-Antwort von check_index_coverage, Wurzel-Scope. */
const SCOPE_PAYLOAD = {
    project: 'probe-small',
    signal: 'best_effort',
    indexed_at: '2026-08-28T20:00:00Z',
    metadata: {
        generation: '2026-08-28T20:00:00Z',
        index_mode: 'full',
        recorded_at: '2026-08-28T20:00:01Z',
        recording_status: 'complete',
        ignored_files_stored: 0,
        ignored_files_total: 4,
        hash_records_complete: true,
        coverage_version: 2,
        generation_matches: true,
    },
    paths: [],
    scopes: [
        {
            requested_scope: '.',
            scope: '.',
            total: 3,
            has_more: false,
            entries: [
                { path: 'src/broken.ts', kind: 'parse_partial', detail: '12-18,24' },
                { path: 'assets/beleg.png', kind: 'discovery', detail: 'unsupported extension' },
                { path: 'src/late.ts', kind: 'not_indexed_file', detail: '.cbmignore' },
            ],
            status: 'known_gaps',
        },
    ],
    caveat: 'Best-effort signal only.',
};

describe('normalizeCoveragePath', () => {

    it('macht aus den Schreibweisen beider Quellen einen Pfad', () => {
        expect(normalizeCoveragePath('./src/a.ts')).toBe('src/a.ts');
        expect(normalizeCoveragePath('/src/a.ts')).toBe('src/a.ts');
        expect(normalizeCoveragePath('src\\a.ts')).toBe('src/a.ts');
        expect(normalizeCoveragePath('src/')).toBe('src');
    });
});

describe('coverageStateForKind', () => {

    it('kennt genau die Namen, die der Server schreibt', () => {
        expect(coverageStateForKind('parse_partial')).toBe('partial');
        expect(coverageStateForKind('not_indexed_file')).toBe('not-indexed');
        expect(coverageStateForKind('not_indexed_dir')).toBe('not-indexed');
        expect(coverageStateForKind('ignored_file')).toBe('ignored');
    });

    it('nimmt jede andere Phase als uebersprungen, so wie der Server es tut', () => {
        expect(coverageStateForKind('discovery')).toBe('skipped');
        expect(coverageStateForKind('parse_failed')).toBe('skipped');
    });
});

describe('worstCoverage', () => {

    it('stellt eine unerklaerte Luecke ueber eine erklaerte und beide ueber partiell', () => {
        expect(worstCoverage('indexed', 'partial')).toBe('partial');
        expect(worstCoverage('partial', 'not-indexed')).toBe('not-indexed');
        expect(worstCoverage('not-indexed', 'skipped')).toBe('skipped');
        expect(worstCoverage('skipped', 'partial')).toBe('skipped');
    });
});

describe('readIndexStatusCoverage', () => {

    const status = readIndexStatusCoverage(STATUS_PAYLOAD);

    it('liest die drei Listen mit Zahlen und Kappungsflagge', () => {
        expect(status.parsePartial).toEqual([{ path: 'src/broken.ts', errorRanges: '12-18,24' }]);
        expect(status.skipped[0]?.reason).toBe('unsupported extension');
        expect(status.notIndexedDirs).toEqual(['node_modules']);
        expect(status.notIndexedFiles[0]?.path).toBe('secret.env');
        expect(status.parsePartialCount).toBe(1);
        expect(status.notIndexedTruncated).toBe(false);
    });

    it('macht aus einer Antwort ohne Beilage leere Listen und keine Behauptung', () => {
        const empty = readIndexStatusCoverage({ project: 'x', nodes: 1 });
        expect(empty.parsePartial).toEqual([]);
        expect(empty.skippedCount).toBe(0);
        expect(empty.notIndexedDirs).toEqual([]);
    });
});

describe('readCoverageAnswer', () => {

    const answer = readCoverageAnswer(SCOPE_PAYLOAD);

    it('liest den Scope samt Seitenansage', () => {
        expect(answer.scopes[0]?.scope).toBe('.');
        expect(answer.scopes[0]?.total).toBe(3);
        expect(answer.scopes[0]?.hasMore).toBe(false);
        expect(answer.scopes[0]?.entries).toHaveLength(3);
    });

    it('haelt die Metadaten fest, inklusive der ignored-Zahlen', () => {
        expect(answer.metadata.recordingStatus).toBe('complete');
        expect(answer.metadata.ignoredFilesStored).toBe(0);
        expect(answer.metadata.ignoredFilesTotal).toBe(4);
        expect(answer.metadata.generationMatches).toBe(true);
    });

    it('liest eine Pfad-Antwort mit Frische und Empfehlung', () => {
        const single = readCoverageAnswer({
            project: 'probe-small',
            metadata: {},
            paths: [
                {
                    requested_path: 'src/a.ts',
                    path: 'src/a.ts',
                    status: 'no_recorded_issue',
                    freshness: 'metadata_changed',
                    recommended_action: 'read_source_and_reindex',
                    coverage: [],
                },
            ],
            scopes: [],
        });
        expect(single.paths[0]?.freshness).toBe('metadata_changed');
        expect(single.paths[0]?.recommendedAction).toBe('read_source_and_reindex');
    });
});

describe('buildCoverageIndex', () => {

    const index = buildCoverageIndex({
        status: readIndexStatusCoverage(STATUS_PAYLOAD),
        scopes: readCoverageAnswer(SCOPE_PAYLOAD).scopes,
    });

    it('legt jeden Pfad genau einmal ab und nennt seine Quellen', () => {
        const broken = index.records.get('src/broken.ts');
        expect(broken?.state).toBe('partial');
        expect(broken?.sources).toEqual([
            'index_status.parse_partial',
            'check_index_coverage.scope:.',
        ]);
    });

    it('nimmt einen Pfad auf, den nur die Scope-Liste kennt', () => {
        expect(index.records.get('src/late.ts')?.state).toBe('not-indexed');
        expect(index.records.get('src/late.ts')?.reason).toBe('.cbmignore');
    });

    it('unterscheidet Ordner von Dateien', () => {
        expect(index.records.get('node_modules')?.kind).toBe('dir');
        expect(index.records.get('secret.env')?.kind).toBe('file');
    });

    it('zaehlt, was der Server gezaehlt hat, und nicht, was ankam', () => {
        expect(index.counts.partial).toBe(1);
        expect(index.counts.skipped).toBe(1);
        expect(index.counts.notIndexedDirs).toBe(1);
        expect(index.counts.notIndexedFiles).toBe(1);
        expect(index.counts.scopeEntries).toBe(3);
    });

    it('macht aus einer gekappten Liste eine ehrliche Zeile', () => {
        const cut = buildCoverageIndex({
            status: readIndexStatusCoverage({
                ...STATUS_PAYLOAD,
                skipped: { files: [], count: 812, truncated: true },
            }),
        });
        expect(cut.truncations.some((line) => /cut the skipped list/.test(line))).toBe(true);
        expect(cut.truncations.some((line) => /812/.test(line))).toBe(true);
    });

    it('meldet eine Scope-Seite, die nicht zu Ende geholt wurde', () => {
        const more = buildCoverageIndex({
            scopes: readCoverageAnswer({
                ...SCOPE_PAYLOAD,
                scopes: [{ ...SCOPE_PAYLOAD.scopes[0], has_more: true, total: 4000 }],
            }).scopes,
        });
        expect(more.truncations.some((line) => /4000 recorded/.test(line))).toBe(true);
    });

    it('sagt bei gar keiner Quelle nichts, statt Vollstaendigkeit zu behaupten', () => {
        const nothing = buildCoverageIndex({});
        expect(nothing.records.size).toBe(0);
        expect(nothing.truncations).toEqual([]);
    });
});

describe('aggregateCoverage', () => {

    const index = buildCoverageIndex({ status: readIndexStatusCoverage(STATUS_PAYLOAD) });

    it('gibt einem Ordner die schlechteste Stufe seines Teilbaums', () => {
        // assets/beleg.png ist skipped, src/broken.ts nur partial.
        expect(aggregateCoverage(index, 'assets')).toBe('skipped');
        expect(aggregateCoverage(index, 'src')).toBe('partial');
    });

    it('nimmt den eigenen Befund eines Ordners mit', () => {
        expect(aggregateCoverage(index, 'node_modules')).toBe('not-indexed');
    });

    it('laesst einen sauberen Ordner sauber', () => {
        expect(aggregateCoverage(index, 'test')).toBe('indexed');
    });

    it('verwechselt keinen Praefix mit einem Ordner', () => {
        // "src" darf nicht auf "srcfoo/x.ts" passen.
        const tricky = buildCoverageIndex({
            status: readIndexStatusCoverage({
                skipped: { files: [{ path: 'srcfoo/x.ts', reason: 'r', phase: 'p' }], count: 1 },
            }),
        });
        expect(aggregateCoverage(tricky, 'src')).toBe('indexed');
        expect(aggregateCoverage(tricky, 'srcfoo')).toBe('skipped');
    });
});

describe('mergeCoverageIntoLevels', () => {

    const graphLevels = new Map<string, TreeLevel>([
        ['', readTreeLevel({
            path: '',
            files: 3,
            symbols: 12,
            children: [
                { name: 'src', path: 'src', kind: 'dir', symbols: 12, files: 3 },
                { name: 'test', path: 'test', kind: 'dir', symbols: 2, files: 1 },
            ],
        })],
        ['src', readTreeLevel({
            path: 'src',
            children: [
                { name: 'broken.ts', path: 'src/broken.ts', kind: 'file', symbols: 2, files: 1 },
                { name: 'clean.ts', path: 'src/clean.ts', kind: 'file', symbols: 5, files: 1 },
            ],
        })],
    ]);
    const index = buildCoverageIndex({
        status: readIndexStatusCoverage(STATUS_PAYLOAD),
        scopes: readCoverageAnswer(SCOPE_PAYLOAD).scopes,
    });
    const merged = mergeCoverageIntoLevels(graphLevels, index);

    it('laesst partial gegen den Graphen gewinnen', () => {
        // src/broken.ts steht im Graphen UND in parse_partial. Der Konflikt
        // wird zugunsten des Problems entschieden.
        const child = merged.get('src')?.children.find((entry) => entry.path === 'src/broken.ts');
        expect(child?.coverage).toBe('partial');
        expect(child?.coverageReason).toBe('12-18,24');
    });

    it('laesst eine Datei ohne Befund indexed', () => {
        expect(merged.get('src')?.children.find((entry) => entry.path === 'src/clean.ts')?.coverage)
            .toBe('indexed');
    });

    it('haengt Pfade ein, die der Graph nicht kennt, samt ihrer Ordner', () => {
        const root = merged.get('')?.children.map((child) => child.path) ?? [];
        expect(root).toContain('assets');
        expect(root).toContain('node_modules');
        expect(root).toContain('secret.env');
        expect(merged.get('assets')?.children.map((child) => child.path))
            .toEqual(['assets/beleg.png']);
        expect(merged.get('assets')?.synthetic).toBe(true);
    });

    it('haelt eine Ebene aus dem Graphen als nicht synthetisch fest', () => {
        expect(merged.get('src')?.synthetic).toBeUndefined();
    });

    it('gibt Ordnern die schlechteste Stufe ihrer Kinder', () => {
        const root = merged.get('') ?? { children: [] };
        const of = (path: string) => root.children.find((child) => child.path === path)?.coverage;
        expect(of('assets')).toBe('skipped');
        expect(of('src')).toBe('not-indexed');
        expect(of('node_modules')).toBe('not-indexed');
        expect(of('test')).toBe('indexed');
    });

    it('laesst die Antworten des Servers unberuehrt', () => {
        expect(graphLevels.get('src')?.children[0]?.coverage).toBeUndefined();
    });

    it('zeigt jeden eingehaengten Pfad auch wirklich als Zeile', () => {
        const expanded = new Set(['src', 'assets', 'node_modules']);
        const paths = flattenTree(merged, expanded).map((row) => row.path);
        for (const record of index.records.values()) {
            expect(paths).toContain(record.path);
        }
    });
});
