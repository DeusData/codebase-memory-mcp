import { describe, expect, it, vi } from 'vitest';
import type { SymbolSearchHit } from '../core/intelligence-provider';
import { CbmRpcProvider } from '../provider/cbm-rpc-provider';
import { RpcIntelligenceClient } from '../provider/rpc-client';
import { FakeRpc } from '../test-support/rpc-recordings';
import { searchByMeaning } from './find-by-meaning';
import { fileCandidates, localSuggestions, rankLocalCandidates, settledSearchHits } from './local-suggestions';
import { fileQueryPath } from './path-query';
import { rankHits } from './semantic-search';

const path = 'src/daemon/application.c';
const wrong: SymbolSearchHit = { name: 'CBM_DAEMON_APPLICATION_INTERNAL_H', kind: 'unknown',
    filePath: 'src/daemon/application_internal.h', qualifiedName: 'p.internal.header', line: 5 };
const symbol: SymbolSearchHit = { name: 'cbm_daemon_application_new', kind: 'function',
    filePath: path, qualifiedName: 'p.application.new', line: 2909 };

describe('explicit file path search', () => {
    it('puts the exact loaded file before popular symbols and similarly named files', () => {
        const hits = localSuggestions({ symbols: [wrong, symbol], files: [wrong.filePath!, path] }, path, () => 1000000);
        expect(hits[0].pathMatch).toBe('file');
        expect(hits[0].hit.filePath).toBe(path);
        expect(hits[0].hit.line).toBeUndefined();
        expect(hits[0].matched).toEqual(['exact file path']);
        expect(hits[1].hit).toEqual(symbol);
    });

    it('keeps an exact path lookup pending when the loaded tree and cached symbols contain only other files', () => {
        expect(localSuggestions({ symbols: [wrong], files: [] }, path)).toEqual([]);
        // App combines a previous prefix reply with the current loaded pool.
        // Neither source may offer the similarly named header as an Enter target.
        const cached = { ...wrong, name: 'applicationCached', filePath: 'tests/test_application.c' };
        expect(rankLocalCandidates([cached, wrong], path, () => 1000000)).toEqual([]);
        expect(rankLocalCandidates([cached, wrong], './src/daemon/application.c')).toEqual([]);
        expect(rankLocalCandidates([cached, wrong], 'src\\daemon\\application.c')).toEqual([]);
    });

    it('permits only exact-file symbols provisionally and preserves ordinary word suggestions', () => {
        const hits = rankLocalCandidates([wrong, symbol], path);
        expect(hits.map(row => row.hit)).toEqual([symbol]);
        expect(hits[0].pathMatch).toBe('symbol');
        expect(rankLocalCandidates([wrong], 'application')[0].hit).toEqual(wrong);
    });

    it('keeps an exact loaded file when a nonempty token reply only finds other symbols', () => {
        const loaded = rankHits(fileCandidates([path]), path);
        const indexed = rankHits([wrong], path);
        expect(settledSearchHits(indexed, loaded)).toEqual({ hits: loaded, source: 'loaded' });
    });

    it('prefers a newly verified exact index file over the loaded candidate', () => {
        const loaded = rankHits(fileCandidates([path]), path);
        const indexed = rankHits([{ name: 'application.c', kind: 'unknown', filePath: path }], path);
        expect(settledSearchHits(indexed, loaded)).toEqual({ hits: indexed, source: 'index' });
    });

    it('looks up the complete literal path before token queries, including without a loaded tree', async () => {
        const rpc = new FakeRpc([{ tool: 'query_graph', text: 'rows: 1  (cols: n.name n.file_path)\n  application.c src/daemon/application.c\nreturned: 1\ntotal: 1\ntotal_relation: eq\nhas_more: false\ntruncated: false\n' }]);
        const provider = new CbmRpcProvider(new RpcIntelligenceClient({ fetch: rpc.fetch }));
        const answer = await searchByMeaning(provider, '/workspace', path, { projectName: 'cbm-pr2068' });
        expect(answer.hits).toHaveLength(1);
        expect(answer.hits[0].hit).toEqual({ name: 'application.c', kind: 'unknown', filePath: path });
        expect(rpc.calls).toHaveLength(1);
        expect(rpc.calls[0].args.query).toContain('n.file_path = "src/daemon/application.c"');
        expect(answer.complete).toBe(false); // An exact lookup is not a complete prefix cache.
    });

    it('falls back without inventing a file when no exact graph file exists', async () => {
        const searchFile = vi.fn(async () => undefined), searchSymbols = vi.fn(async () => [wrong]);
        const answer = await searchByMeaning({ searchFile, searchSymbols }, '/', path);
        expect(searchFile).toHaveBeenCalledWith('/', path, {});
        expect(answer.hits.every(hit => hit.hit.filePath !== path)).toBe(true);
        expect(searchSymbols).toHaveBeenCalled();
    });

    it('does not attribute a mismatched provider file to the requested path', async () => {
        const answer = await searchByMeaning({ searchFile: async () => wrong, searchSymbols: async () => [] }, '/', path);
        expect(answer.hits).toEqual([]);
    });

    it('discards an exact lookup result after cancellation', async () => {
        const controller = new AbortController();
        const searchSymbols = vi.fn(async () => []);
        const answer = await searchByMeaning({ searchFile: async () => {
            controller.abort(); return { name: 'application.c', kind: 'unknown', filePath: path };
        }, searchSymbols }, '/', path, { signal: controller.signal });
        expect(answer.aborted).toBe(true);
        expect(answer.hits).toEqual([]);
        expect(searchSymbols).not.toHaveBeenCalled();
    });

    it('normalizes relative path notation without fuzzy case folding or directory traversal', () => {
        expect(fileQueryPath('./src/daemon/application.c')).toBe(path);
        expect(fileQueryPath('src\\daemon\\application.c')).toBe(path);
        expect(fileQueryPath('README.md')).toBe('README.md');
        for (const value of ['../private.c', '/tmp/file.c', 'C:\\private.c', 'src/../private.c', 'src/', 'find callers'])
            expect(fileQueryPath(value)).toBeUndefined();
        expect(rankHits(fileCandidates(['README.md']), 'readme.md')[0]?.pathMatch).toBeUndefined();
    });

    it('finds a short exact filename even when its tokens are too short for word search', () => {
        const hit = rankHits(fileCandidates(['a.c']), 'a.c')[0];
        expect(hit.hit.filePath).toBe('a.c');
        expect(hit.pathMatch).toBe('file');
    });
});
