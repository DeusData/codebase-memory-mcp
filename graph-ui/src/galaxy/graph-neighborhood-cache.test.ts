import { describe, expect, it, vi } from 'vitest';
import { GraphNeighborhoodCache, GraphNeighborhoodCachePool } from './graph-neighborhood-cache';
import { loadGraphScope, type GraphQueryClient } from './graph-scope';
import type { QueryGraphResult } from '../provider/rpc-schemas';
import type { GraphNode } from './types';

const node = (id: number, file = 'src/a.ts'): GraphNode => ({ id, name: `n${id}`, qualified_name: `p.n${id}`, file_path: file,
    label: 'Function', start_line: id * 10, end_line: id * 10 + 5, x: 0, y: 0, z: 0, color: '#abcdef', size: 3 });
const row = (value: GraphNode, prefix = '') => Object.fromEntries(Object.entries({ id: value.id, label: value.label, name: value.name,
    qn: value.qualified_name, file: value.file_path, start_line: value.start_line, end_line: value.end_line }).map(([key, value]) => [prefix + key, String(value ?? '')]));
const page = (records: Record<string, string>[], extra: Partial<QueryGraphResult> = {}): QueryGraphResult => {
    const columns = Object.keys(records[0] ?? { id: '' });
    return { columns, rows: records.map(record => columns.map(column => record[column]!)), total: records.length, ...extra };
};
const edgeRow = (id: number, a: GraphNode, b: GraphNode, type = 'CALLS') => ({ edge_id: String(id), edge_type: type, edge_line: '2', ...row(a, 'a_'), ...row(b, 'b_') });
const selected = { kind: 'node' as const, id: 1, name: 'n1', qualifiedName: 'p.n1' };

describe('verified neighborhood reuse', () => {
    it('turns a cold three-query file load into zero-query filtering and symbol selection', async () => {
        const a = node(1), b = node(2), c = node(3, 'src/c.ts');
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a), row(b)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, c), edgeRow(2, b, c, 'IMPORTS')]))
            .mockResolvedValueOnce(page([edgeRow(3, c, a, 'HTTP_CALLS')]));
        const cache = new GraphNeighborhoodCache('p', 'one'), first = cache.begin();
        const file = { kind: 'file' as const, path: 'src/a.ts', name: 'a.ts' };
        const cold = await loadGraphScope('p', file, 1, 'both', undefined, { client: { queryGraph }, cache: first });
        expect(queryGraph).toHaveBeenCalledTimes(3); // Root membership, outbound, inbound.
        expect(cache.size.nodes).toBe(0); // Not published until generation verification.
        first.commit();
        const filtered = await loadGraphScope('p', file, 1, 'both', undefined, { client: { queryGraph }, cache: cache.begin(), edgeTypes: ['CALLS'] });
        expect(filtered.data.edges.map(edge => edge.id)).toEqual([1]);
        const symbol = await loadGraphScope('p', { ...selected, id: 2, name: 'n2', qualifiedName: 'p.n2' }, 1, 'both', undefined,
            { client: { queryGraph }, cache: cache.begin() });
        expect(symbol.data.edges.map(edge => edge.id)).toEqual([2]);
        expect(cold.data.edges).toHaveLength(3);
        expect(queryGraph).toHaveBeenCalledTimes(3);
    });
    it('fetches only the missing next frontier and does not certify returned neighbors', async () => {
        const a = node(1), b = node(2), c = node(3);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)]))
            .mockResolvedValueOnce(page([]))
            .mockResolvedValueOnce(page([edgeRow(2, b, c)]));
        const cache = new GraphNeighborhoodCache('p', 'one'), initial = cache.begin();
        await loadGraphScope('p', selected, 1, 'both', undefined, { client: { queryGraph }, cache: initial }); initial.commit();
        expect(cache.begin().neighborhood(2, 'outbound')).toBeUndefined();
        const expand = cache.begin();
        const result = await loadGraphScope('p', selected, 2, 'outbound', undefined, { client: { queryGraph }, cache: expand }); expand.commit();
        expect(result.data.nodes.map(node => node.id)).toEqual([1, 2, 3]);
        expect(queryGraph).toHaveBeenCalledTimes(4);
        expect(queryGraph.mock.calls[3]![1]).toContain('WHERE (a.qualified_name = "p.n2")');
        await loadGraphScope('p', { ...selected, id: 2, name: 'n2', qualifiedName: 'p.n2' }, 1, 'outbound', undefined,
            { client: { queryGraph }, cache: cache.begin() });
        expect(queryGraph).toHaveBeenCalledTimes(4);
    });
    it('unions typed proofs but never mistakes their union for all edge types', async () => {
        const a = node(1), b = node(2), c = node(3);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)]))
            .mockResolvedValueOnce(page([edgeRow(2, a, c, 'IMPORTS')]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b), edgeRow(2, a, c, 'IMPORTS')]));
        const cache = new GraphNeighborhoodCache('p', 'one');
        for (const edgeTypes of [['CALLS'], ['IMPORTS']]) {
            const tx = cache.begin();
            await loadGraphScope('p', selected, 1, 'outbound', undefined, { client: { queryGraph }, cache: tx, edgeTypes }); tx.commit();
        }
        const result = await loadGraphScope('p', selected, 1, 'outbound', undefined,
            { client: { queryGraph }, cache: cache.begin(), edgeTypes: ['IMPORTS', 'CALLS'] });
        expect(result.data.edges).toHaveLength(2); expect(queryGraph).toHaveBeenCalledTimes(3);
        expect(cache.begin().neighborhood(1, 'inbound', ['CALLS'])).toBeUndefined();
        const all = cache.begin();
        await loadGraphScope('p', selected, 1, 'outbound', undefined, { client: { queryGraph }, cache: all }); all.commit();
        expect(queryGraph).toHaveBeenCalledTimes(4);
    });
    it('retains empty root membership without inferring anything from a capped layout', async () => {
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValue(page([]));
        const cache = new GraphNeighborhoodCache('p', 'one'), first = cache.begin();
        const scope = { kind: 'file' as const, path: 'src/empty.ts', name: 'empty.ts' };
        const layout = { nodes: [node(1, 'src/empty.ts')], edges: [], total_nodes: 5000 };
        const result = await loadGraphScope('p', scope, 1, 'both', layout, { client: { queryGraph }, cache: first }); first.commit();
        expect(result.roots.size).toBe(0);
        await loadGraphScope('p', scope, 1, 'both', layout, { client: { queryGraph }, cache: cache.begin() });
        expect(queryGraph).toHaveBeenCalledTimes(1);
    });
    it('does not publish a partially fetched or cancelled adjacency as complete', async () => {
        const a = node(1), b = node(2), c = node(3), abort = new AbortController();
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)], { total: 2, nextCursor: 'rest', hasMore: true }))
            .mockImplementationOnce(async () => { abort.abort(); return page([edgeRow(2, a, c)], { total: 2, offset: 1 }); });
        const cache = new GraphNeighborhoodCache('p', 'one'), tx = cache.begin();
        await expect(loadGraphScope('p', selected, 1, 'outbound', undefined,
            { client: { queryGraph }, cache: tx, signal: abort.signal })).rejects.toThrow();
        expect(cache.size.nodes).toBe(0);
        tx.commit(); // Even an erroneous caller cannot certify the incomplete pages.
        expect(cache.begin().neighborhood(1, 'outbound')).toBeUndefined();
        expect(cache.size.edges).toBe(0);
    });
    it('invalidates publication evidence and rejects a late old-generation commit', () => {
        const pool = new GraphNeighborhoodCachePool(), old = pool.get('p', 'one'), tx = old.begin();
        tx.rememberRoots('root', [node(1)]);
        tx.rememberNeighborhood([1], 'outbound', undefined, [node(1), node(2)], [{ id: 1, source: 1, target: 2, type: 'CALLS' }]);
        tx.commit();
        const late = old.begin(); late.rememberRoots('later', [node(3)]);
        const current = pool.get('p', 'two'); late.commit();
        expect(current.size.nodes).toBe(0); expect(old.size.nodes).toBe(0);
        expect(current.begin().roots('root')).toBeUndefined();
        expect(current.begin().neighborhood(1, 'outbound')).toBeUndefined();
    });
    it('evicts proofs with their evidence and bounds retained projects and records', () => {
        const pool = new GraphNeighborhoodCachePool(2, { nodes: 2, edges: 1, roots: 1, proofs: 1 });
        const cache = pool.get('p', 'one'), initial = cache.begin();
        initial.rememberRoots('first', [node(1)]);
        initial.rememberNeighborhood([1], 'outbound', undefined, [node(1), node(2)], [{ id: 1, source: 1, target: 2, type: 'CALLS' }]); initial.commit();
        const next = cache.begin(); next.rememberRoots('next', [node(3)]); next.commit();
        expect(cache.begin().neighborhood(1, 'outbound')).toBeUndefined();
        expect(cache.size).toEqual({ nodes: 1, edges: 0, roots: 1, proofs: 0 });
        const huge = cache.begin(); huge.rememberRoots('huge', [node(4), node(5), node(6)]); huge.commit();
        expect(cache.size.nodes).toBe(1);
        pool.get('q', 'one'); pool.get('r', 'one');
        expect(pool.size).toBe(2); expect(cache.valid).toBe(false); expect(cache.size.nodes).toBe(0);
    });
});
