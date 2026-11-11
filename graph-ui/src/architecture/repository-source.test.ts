import { describe, expect, it, vi } from 'vitest';
import { fetchRepositorySnapshot, readRepositorySnapshot, resolveRepositorySelection } from './repository-source';

describe('logical repository snapshot', () => {
    it('retains source-free route identities without inventing a source file and derives call degree only from calls', () => {
        const graph = readRepositorySnapshot({ generation: 'fixture-g1', indexed_at: '2026-09-09', total_nodes: 3,
            nodes_truncated: false, edges_truncated: false,
            nodes: [{ id: 1, label: 'Route', name: '/api/test' },
                { id: 2, label: 'Function', name: 'serve', file_path: 'src/api.ts', is_entry: true, start_line: 12, package_name: 'api' },
                { id: 3, label: 'Function', name: 'test', file_path: 'tests/api.ts', is_test: true }],
            edges: [{ id: 8, source: 3, target: 2, type: 'CALLS', line: 30 }, { id: 9, source: 3, target: 2, type: 'IMPORTS' }] });
        expect(graph.nodes[0].file_path).toBeUndefined();
        expect(graph.nodes[1]).toMatchObject({ status: 'entry', in_calls: 1, package_name: 'api' });
        expect(graph.nodes[2]).toMatchObject({ status: 'test', out_calls: 1 });
        expect(graph.edges[0]).toMatchObject({ id: 8, source: 3, target: 2, type: 'CALLS', line: 30 });
        expect(graph.nodesTruncated).toBe(false);
    });
    it('moves source navigation and documentation together with a stable symbol after reindexing', () => {
        const snapshot = readRepositorySnapshot({ generation: 'new', nodes: [{ id: 12, name: 'serve', qualified_name: 'p.serve', file_path: 'server.ts', start_line: 40, docstring: 'Current docs' }], edges: [] });
        const old = { ...snapshot.nodes[0], id: 3, start_line: 10, documentation: 'Old docs' };
        expect(resolveRepositorySelection(old, snapshot)).toMatchObject({ id: 12, start_line: 40, documentation: 'Current docs' });
        expect(resolveRepositorySelection({ ...old, qualified_name: 'p.removed' }, snapshot)).toBeUndefined();
    });
    it('refuses missing snapshot metadata instead of labeling an empty graph as complete', () => {
        expect(() => readRepositorySnapshot({ nodes: [], edges: [] })).toThrow('snapshot metadata');
        expect(readRepositorySnapshot({ nodes: [], edges: [], generation: 'fixture' }).nodesTruncated).toBe(true);
    });
});

it('recovers from an interrupted response and respects cancellation before retrying', async () => {
    vi.useFakeTimers();
    const fetcher = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch')).mockResolvedValue({ ok: true, json: async () => ({ generation: 'reconnected', nodes: [], edges: [] }) });
    vi.stubGlobal('fetch', fetcher);
    try {
        const ready = fetchRepositorySnapshot('actual-project', new AbortController().signal);
        await vi.runAllTimersAsync();
        expect((await ready).generation).toBe('reconnected'); expect(fetcher).toHaveBeenCalledTimes(2);
        fetcher.mockReset().mockRejectedValue(new TypeError('Failed to fetch'));
        const controller = new AbortController();
        const cancelled = fetchRepositorySnapshot('previous-project', controller.signal);
        const assertion = expect(cancelled).rejects.toThrow('Aborted');
        await Promise.resolve(); controller.abort(); await assertion;
        await vi.runAllTimersAsync(); expect(fetcher).toHaveBeenCalledTimes(1);
    } finally { vi.unstubAllGlobals(); vi.useRealTimers(); }
});

it('does not retry a malformed graph or a permanent server response', async () => {
    const fetcher = vi.fn().mockResolvedValueOnce({ ok: true, json: async () => ({ nodes: [], edges: [] }) })
        .mockResolvedValueOnce({ ok: false, status: 404 });
    vi.stubGlobal('fetch', fetcher);
    try {
        await expect(fetchRepositorySnapshot('p', new AbortController().signal)).rejects.toThrow('snapshot metadata');
        await expect(fetchRepositorySnapshot('p', new AbortController().signal)).rejects.toThrow('HTTP 404');
        expect(fetcher).toHaveBeenCalledTimes(2);
    } finally { vi.unstubAllGlobals(); }
});
