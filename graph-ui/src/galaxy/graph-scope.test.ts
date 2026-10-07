import { describe, expect, it, vi } from 'vitest';
import { arrangeScopedGraph, expandPastLimit, frontierCallCount, graphEdgeTypesKey, hierarchyLabelWidth, limitGraphRender, loadGraphScope, nextLayerEstimate, readGraphPages, scenePictureFor, SCOPED_HIERARCHY_ROW_GAP, scopedHierarchy, type GraphQueryClient } from './graph-scope';
import type { QueryGraphResult } from '../provider/rpc-schemas';
import type { GraphData, GraphNode } from './types';

const node = (id: number, file = 'src/a.ts', start = 1, end = 10): GraphNode => ({ id, name: `n${id}`, qualified_name: `p.n${id}`, file_path: file,
    label: 'Function', start_line: start, end_line: end, x: id, y: 0, z: 0, color: '#abcabc', size: 3 });
const row = (value: GraphNode, prefix = '') => Object.fromEntries(Object.entries({ id: value.id, label: value.label, name: value.name, qn: value.qualified_name,
    file: value.file_path, start_line: value.start_line, end_line: value.end_line }).map(([key, value]) => [prefix + key, String(value ?? '')]));
const page = (records: Record<string, string>[], extra: Partial<QueryGraphResult> = {}): QueryGraphResult => {
    const columns = Object.keys(records[0] ?? { id: '' });
    return { columns, rows: records.map(record => columns.map(column => record[column]!)), total: records.length, ...extra };
};
const edgeRow = (id: number, a: GraphNode, b: GraphNode, type = 'CALLS') => ({ edge_id: String(id), edge_type: type, edge_line: '2', ...row(a, 'a_'), ...row(b, 'b_') });

describe('complete scoped graph acquisition', () => {
    it('reads every cursor page, including identities absent from the overall layout', async () => {
        const a = node(8001), b = node(9100, 'src/b.ts'), c = node(9200, 'src/c.ts'), d = node(9300, 'src/d.ts');
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)], { total: 2, offset: 0, nextOffset: 1, nextCursor: 'next', hasMore: true, truncated: true }))
            .mockResolvedValueOnce(page([edgeRow(3, a, d)], { total: 2, offset: 1, hasMore: false, truncated: false }))
            .mockResolvedValueOnce(page([edgeRow(2, c, a)]));
        const result = await loadGraphScope('p', { kind: 'file', path: 'src/a.ts', name: 'a.ts' }, 1, 'both',
            { nodes: [node(1, 'unrelated.ts')], edges: [], total_nodes: 10000 }, { client: { queryGraph } });
        expect(result.data.nodes.map(node => node.id).sort()).toEqual([8001, 9100, 9200, 9300]);
        expect(result.data.edges).toHaveLength(3);
        expect([...result.roots]).toEqual([8001]);
        expect(queryGraph.mock.calls[1]![1]).toContain('a.file_path');
        expect(queryGraph.mock.calls[3]![1]).toContain('MATCH (b)<-[r]-(a) WHERE b.file_path');
        expect(queryGraph.mock.calls[1]![1]).not.toContain(' OR b.file_path');
        expect(queryGraph.mock.calls[1]![1]).not.toContain('LIMIT');
        expect(queryGraph.mock.calls[0]![1]).toContain('AS start_line');
        expect(queryGraph.mock.calls[0]![1]).toContain('AS end_line');
        expect(queryGraph.mock.calls[0]![1]).not.toMatch(/ AS (?:start|end)(?:,| |$)/);
        expect(queryGraph.mock.calls[2]![2]).toBe('next');
    });
    it('narrows literal code selection before fetching dependencies and excludes unrelated file symbols', async () => {
        const a = node(1, 'src/a.ts', 1, 10), b = node(2, 'src/a.ts', 20, 30), external = node(3, 'src/b.ts');
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a), row(b)]))
            .mockResolvedValueOnce(page([edgeRow(7, b, external)]))
            .mockResolvedValueOnce(page([]));
        const result = await loadGraphScope('p', { kind: 'file', name: 'a.ts', path: 'src/a.ts', range: { startLine: 24, endLine: 25 } }, 1, 'both', undefined, { client: { queryGraph } });
        expect([...result.roots]).toEqual([2]);
        expect(result.data.nodes.map(node => node.id).sort()).toEqual([2, 3]);
        expect(queryGraph.mock.calls[1]![1]).not.toContain('p.n1');
    });
    it('adds one directed frontier per layer, preserving prior relationships and roots', async () => {
        const a = node(1), b = node(2), c = node(3);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)]))
            .mockResolvedValueOnce(page([edgeRow(2, b, c)]));
        const result = await loadGraphScope('p', { kind: 'node', id: 1, name: 'n1', qualifiedName: 'p.n1' }, 2, 'outbound', undefined, { client: { queryGraph } });
        expect(result.data.nodes).toHaveLength(3); expect(result.data.edges).toHaveLength(2);
        expect(queryGraph.mock.calls[1]![1]).toContain('WHERE (a.qualified_name = "p.n1")');
        expect(queryGraph.mock.calls[2]![1]).toContain('WHERE (a.qualified_name = "p.n2")');
        expect(result.depth).toBe(2); expect(result.exhausted).toBe(false);
    });
    it('does not call a capped response complete when no continuation exists', async () => {
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValue(page([{ id: '1' }], { total: 500, hasMore: true, truncated: true }));
        await expect(readGraphPages({ queryGraph }, 'p', 'query', 'id')).rejects.toThrow('Incomplete graph response');
    });
    it('rejects repeated cursor rows and mismatched snapshots', async () => {
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([{ id: '1' }], { total: 2, nextCursor: 'next' }))
            .mockResolvedValueOnce(page([{ id: '1' }], { total: 2, offset: 1 }));
        await expect(readGraphPages({ queryGraph }, 'p', 'query', 'id')).rejects.toThrow('repeated a row');
    });
    it('honors cancellation before any query and before continuation', async () => {
        const abort = new AbortController(); abort.abort();
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>();
        await expect(readGraphPages({ queryGraph }, 'p', 'query', 'id', abort.signal)).rejects.toThrow();
        expect(queryGraph).not.toHaveBeenCalled();
    });
    it('loads all internal cluster edges even for an unexpanded cluster', async () => {
        const a = node(1), b = node(2), outside = node(3, 'other/b.ts');
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a), row(b)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b), edgeRow(2, b, outside)]))
            .mockResolvedValueOnce(page([]));
        const result = await loadGraphScope('p', { kind: 'folder', path: 'src', name: 'src/' }, 0, 'both', undefined, { client: { queryGraph } });
        expect(result.data.nodes.map(node => node.id)).toEqual([1, 2]);
        expect(result.data.edges.map(edge => edge.id)).toEqual([1]);
    });
    it.each(['file', 'folder'] as const)('preserves edge-limit partial metadata when an unexpanded %s stops between pages', async kind => {
        const a = node(1), b = node(2), c = node(3);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a), row(b), row(c)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b), edgeRow(2, b, c)], { total: 3, offset: 0, nextOffset: 2, nextCursor: 'third', hasMore: true }))
            .mockResolvedValueOnce(page([edgeRow(3, c, a)], { total: 3, offset: 2 }));
        const result = await loadGraphScope('p', { kind, path: kind === 'file' ? 'src/a.ts' : 'src', name: kind }, 0, 'both', undefined,
            { client: { queryGraph }, limits: { nodes: 100, edges: 1 } });
        expect(queryGraph).toHaveBeenCalledTimes(2);
        expect(result.depth).toBe(0);
        expect(result.data.nodes.map(node => node.id)).toEqual([1, 2, 3]);
        expect(result.data.edges.map(edge => edge.id)).toEqual([1, 2]);
        expect(result.partial).toEqual({ layer: 1, nodes: 3, edges: 2, limit: 'edges' });
    });
});

it('render limits retain selected roots and never retain dangling edges', () => {
    const nodes = [node(1), node(2), node(3)];
    const edges = [{ source: 1, target: 3, type: 'CALLS' }, { source: 2, target: 3, type: 'CALLS' }];
    const result = limitGraphRender({ nodes, edges, total_nodes: 3 }, 1, 1, new Set([2, 3]));
    expect(result.nodes.map(node => node.id)).toEqual([2]); expect(result.edges).toEqual([]);
    const larger = limitGraphRender({ nodes, edges, total_nodes: 3 }, 2, 1, new Set([2, 3]));
    expect(larger.edges).toEqual([edges[1]]);
});
it('hop rings are deterministic and retain source colors and direction', () => {
    const nodes = [node(1), node(2), node(3)], edges = [{ source: 1, target: 2, type: 'CALLS' }, { source: 2, target: 3, type: 'CALLS' }];
    const a = arrangeScopedGraph(nodes, edges, new Set([1])), b = arrangeScopedGraph([...nodes].reverse(), edges, new Set([1]));
    expect(a).toEqual(b); expect(a.edges).toEqual(edges); expect(a.nodes.map(node => node.color)).toEqual(nodes.map(node => node.color));
    expect(a.nodes.map(node => node.z)).toEqual([-0, -18, -36]);
});

it('keeps directed discovery depth and positions when a later edge closes a cycle', async () => {
    const a = node(1), b = node(2), c = node(3);
    const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
        .mockResolvedValueOnce(page([edgeRow(1, a, b)]))
        .mockResolvedValueOnce(page([edgeRow(2, b, c)]))
        .mockResolvedValueOnce(page([edgeRow(3, c, a)]));
    const scope = { kind: 'node' as const, id: 1, name: 'n1', qualifiedName: 'p.n1' };
    const before = await loadGraphScope('p', scope, 2, 'outbound', undefined, { client: { queryGraph } });
    const after = await loadGraphScope('p', scope, 3, 'outbound', undefined, { client: { queryGraph }, previous: before });
    expect(after.levels?.get(3)).toBe(2);
    expect(after.data.nodes).toEqual(before.data.nodes);
    expect(after.data.edges).toHaveLength(3);
    expect(queryGraph).toHaveBeenCalledTimes(4); // only the new frontier was fetched
});

it('pins an exact selected ID even when qualified names are duplicated', async () => {
    const a = node(1), duplicate = { ...node(4, 'other/file.ts'), qualified_name: a.qualified_name }, b = node(2), outsider = node(5);
    const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a), row(duplicate)]))
        .mockResolvedValueOnce(page([edgeRow(1, a, b), edgeRow(2, duplicate, outsider)]));
    const result = await loadGraphScope('p', { kind: 'node', id: 1, name: 'n1', qualifiedName: 'p.n1' }, 1, 'outbound', undefined, { client: { queryGraph } });
    expect([...result.roots]).toEqual([1]);
    expect(result.data.nodes.map(node => node.id)).toEqual([1, 2]);
    expect(result.data.edges.map(edge => edge.id)).toEqual([1]);
});

describe('typed trace traversal', () => {
    const scope = { kind: 'node' as const, id: 1, name: 'n1', qualifiedName: 'p.n1' };
    it('filters every directed frontier before discovering nodes, including a closing cycle', async () => {
        const a = node(1), b = node(2), c = node(3), excluded = node(4), hiddenDescendant = node(5);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)], { total: 2, offset: 0, nextOffset: 1, nextCursor: 'next', hasMore: true }))
            .mockResolvedValueOnce(page([edgeRow(2, a, excluded, 'IMPORTS')], { total: 2, offset: 1 }))
            .mockResolvedValueOnce(page([edgeRow(3, b, c, 'HTTP_CALLS'), edgeRow(4, excluded, hiddenDescendant)]))
            .mockResolvedValueOnce(page([edgeRow(5, c, a)]));
        const result = await loadGraphScope('p', scope, 3, 'outbound', undefined,
            { client: { queryGraph }, edgeTypes: ['HTTP_CALLS', 'CALLS', 'CALLS'] });
        expect(result.data.nodes.map(node => node.id).sort()).toEqual([1, 2, 3]);
        expect(result.data.edges.map(edge => edge.id)).toEqual([1, 3, 5]);
        expect(result.levels?.get(3)).toBe(2);
        expect(queryGraph.mock.calls[1]![1]).toContain('MATCH (a)-[r:CALLS|HTTP_CALLS]->(b) WHERE (a.qualified_name = "p.n1")');
        expect(queryGraph.mock.calls[2]![2]).toBe('next');
        expect(queryGraph.mock.calls[3]![1]).toContain('WHERE (a.qualified_name = "p.n2")');
        expect(queryGraph.mock.calls[3]![1]).not.toContain('p.n4');
        expect(queryGraph.mock.calls[4]![1]).toContain('WHERE (a.qualified_name = "p.n3")');
    });
    it('keeps inbound and outbound typed queries separately seeded', async () => {
        const a = node(1), b = node(2), c = node(3);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b, 'IMPORTS')]))
            .mockResolvedValueOnce(page([edgeRow(2, c, a, 'IMPORTS')]));
        const result = await loadGraphScope('p', scope, 1, 'both', undefined, { client: { queryGraph }, edgeTypes: ['IMPORTS'] });
        expect(result.data.edges).toHaveLength(2);
        expect(queryGraph.mock.calls[1]![1]).toContain('MATCH (a)-[r:IMPORTS]->(b) WHERE (a.qualified_name = "p.n1")');
        expect(queryGraph.mock.calls[2]![1]).toContain('MATCH (b)<-[r:IMPORTS]-(a) WHERE (b.qualified_name = "p.n1")');
    });
    it('treats no selected types as roots only without incident-edge requests', async () => {
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValue(page([row(node(1)), row(node(2))]));
        const result = await loadGraphScope('p', { kind: 'folder', path: 'src', name: 'src/' }, 3, 'both', undefined,
            { client: { queryGraph }, edgeTypes: [] });
        expect(result.data.nodes.map(node => node.id).sort()).toEqual([1, 2]);
        expect(result.data.edges).toEqual([]);
        expect(result.exhausted).toBe(true);
        expect(queryGraph).toHaveBeenCalledTimes(1);
        expect(graphEdgeTypesKey()).not.toBe(graphEdgeTypesKey([]));
    });
    it('restarts from the root on filter changes instead of retaining excluded layers', async () => {
        const a = node(1), b = node(2), c = node(3);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b), edgeRow(2, a, c, 'IMPORTS')]))
            .mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)]));
        const previous = await loadGraphScope('p', scope, 1, 'outbound', undefined, { client: { queryGraph } });
        const result = await loadGraphScope('p', scope, 1, 'outbound', undefined, { client: { queryGraph }, previous, edgeTypes: ['CALLS'] });
        expect(previous.data.nodes).toHaveLength(3);
        expect(result.data.nodes.map(node => node.id).sort()).toEqual([1, 2]);
        expect(queryGraph.mock.calls[2]![1]).toMatch(/^MATCH \(n\) WHERE/);
        expect(result.traversalKey).not.toBe(previous.traversalKey);
    });
    it('reuses the frontier for equivalent normalized filters', async () => {
        const a = node(1), b = node(2), c = node(3);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)]))
            .mockResolvedValueOnce(page([edgeRow(2, b, c, 'IMPORTS')]));
        const previous = await loadGraphScope('p', scope, 1, 'outbound', undefined, { client: { queryGraph }, edgeTypes: ['IMPORTS', 'CALLS'] });
        const result = await loadGraphScope('p', scope, 2, 'outbound', undefined,
            { client: { queryGraph }, previous, edgeTypes: ['CALLS', 'IMPORTS', 'CALLS'] });
        expect(result.data.nodes).toHaveLength(3);
        expect(queryGraph).toHaveBeenCalledTimes(3);
        expect(result.traversalKey).toBe(previous.traversalKey);
    });
    it('rejects invalid type identifiers before any query', async () => {
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>();
        await expect(loadGraphScope('p', scope, 1, 'both', undefined,
            { client: { queryGraph }, edgeTypes: ['CALLS]->(outside)'] })).rejects.toThrow('Invalid graph relationship type');
        expect(queryGraph).not.toHaveBeenCalled();
    });
});

it('wraps large hierarchy levels into readable blocks without losing or overlapping evidence', () => {
    const roots = new Set(Array.from({ length: 368 }, (_, index) => index + 8000));
    const nodes = Array.from({ length: 1687 }, (_, index) => ({ ...node(index + 8000), z: index < 368 ? 0 : -18 }));
    const edges = nodes.slice(368).map(target => ({ source: 8000, target: target.id, type: 'CALLS' }));
    const result = scopedHierarchy({ data: { nodes, edges, total_nodes: nodes.length }, roots, depth: 1, exhausted: false }, 'large file');
    expect(result.data.nodes).toHaveLength(1687);
    expect(result.data.edges).toHaveLength(edges.length);
    expect(new Set(result.data.nodes.map(node => `${node.x}:${node.y}`)).size).toBe(1687);
    const xs = result.data.nodes.map(node => node.x), ys = result.data.nodes.map(node => node.y);
    const width = Math.max(...xs) - Math.min(...xs), height = Math.max(...ys) - Math.min(...ys);
    expect(width / height).toBeGreaterThan(.5); expect(width / height).toBeLessThan(4);
    const first = result.placements.filter(node => node.hop === 0), second = result.placements.filter(node => node.hop === 1);
    expect(Math.min(...second.map(node => node.x)) - Math.max(...first.map(node => node.x))).toBeGreaterThanOrEqual(160);
    const byId = new Map(result.data.nodes.map(node => [node.id, node]));
    for (const edge of result.data.edges) {
        expect(byId.get(edge.source)?.qualified_name).toBe('p.n8000');
        expect(byId.has(edge.target)).toBe(true);
        expect(edge.type).toBe('CALLS');
    }
    expect(result.data.nodes.every(node => node.color === '#abcabc')).toBe(true);
});
it('takes hop columns from discovered levels, not from the global layout depth of a preview', () => {
    // A preview from the overall layout keeps server coordinates; z says nothing about hops.
    const nodes = [{ ...node(1), z: -250 }, { ...node(2), z: 40 }, { ...node(3), z: -90 }];
    const levels = new Map([[1, 0], [2, 1], [3, 2]]);
    const result = scopedHierarchy({ data: { nodes, edges: [], total_nodes: 3 }, roots: new Set([1]), depth: 2, exhausted: false, levels }, 'preview');
    expect(Object.fromEntries(result.placements.map(placement => [placement.name, placement.hop]))).toEqual({ n1: 0, n2: 1, n3: 2 });
    expect(result.depth).toBe(3);
});
it('retains simple columns for small hierarchy levels', () => {
    const nodes = [{ ...node(1), z: 0 }, { ...node(2), z: -18 }, { ...node(3), z: -18 }];
    const result = scopedHierarchy({ data: { nodes, edges: [], total_nodes: 3 }, roots: new Set([1]), depth: 1, exhausted: false }, 'small');
    expect(result.data.nodes.map(node => [node.x, node.y])).toEqual([[0, 0], [160, 16], [160, -16]]);
});

describe('hand test K5: the scoped hierarchy reads incoming left, root in the middle, outgoing right', () => {
    const at = (result: ReturnType<typeof scopedHierarchy>, name: string) => result.placements.find(placement => placement.name === name)!;
    const scoped = (nodes: GraphNode[], edges: { id?: number; source: number; target: number; type: string; line?: number }[], levels: [number, number][]) =>
        scopedHierarchy({ data: { nodes, edges, total_nodes: nodes.length }, roots: new Set([1]), depth: Math.max(...levels.map(([, hop]) => hop)), exhausted: false,
            levels: new Map(levels) }, 'root');

    it('puts callers and tests left of the root, its bases right, and keeps the root at the centre', () => {
        // JSONBAgg: tests call and test it, its module defines it, it inherits two bases.
        const nodes = [node(1), node(10), node(11), node(30), node(40), node(41)];
        const result = scoped(nodes, [{ source: 10, target: 1, type: 'CALLS', line: 5 }, { source: 10, target: 1, type: 'TESTS' },
            { source: 11, target: 1, type: 'CALLS', line: 9 }, { source: 30, target: 1, type: 'DEFINES' },
            { source: 1, target: 40, type: 'INHERITS' }, { source: 1, target: 41, type: 'INHERITS' }],
        [[1, 0], [10, 1], [11, 1], [30, 1], [40, 1], [41, 1]]);
        expect([at(result, 'n1').x, at(result, 'n1').y]).toEqual([0, 0]);
        for (const name of ['n10', 'n11', 'n30']) expect(at(result, name).x).toBeLessThan(0);
        for (const name of ['n40', 'n41']) expect(at(result, name).x).toBeGreaterThan(0);
        expect(result.placements.find(placement => placement.name === 'n10')?.side).toBe(-1);
        // Every node maps back to its scope identity, for paths drawn in this picture.
        expect(result.sourceIds?.[at(result, 'n40').id]).toBe(40);
    });

    it('orders outgoing calls by their call-site line, top to bottom, before other relationship types', () => {
        const nodes = [node(1), node(2), node(3), node(4), node(5)];
        const result = scoped(nodes, [{ source: 1, target: 2, type: 'CALLS', line: 30 }, { source: 1, target: 3, type: 'CALLS', line: 12 },
            { source: 1, target: 4, type: 'CALLS', line: 20 }, { source: 1, target: 5, type: 'IMPORTS' }], [[1, 0], [2, 1], [3, 1], [4, 1], [5, 1]]);
        const top = [...result.placements].filter(placement => placement.hop === 1).sort((a, b) => b.y - a.y).map(placement => placement.name);
        expect(top).toEqual(['n3', 'n4', 'n2', 'n5']);
    });

    it('keeps a pure outgoing chain to the right and a pure incoming chain to the left, a layer per column', () => {
        const nodes = [node(1), node(2), node(3), node(5), node(6)];
        // 1 calls 2, 2 calls 3 (outgoing twice); 5 calls 1, 6 calls 5 (incoming twice).
        const result = scoped(nodes, [{ source: 1, target: 2, type: 'CALLS' }, { source: 2, target: 3, type: 'CALLS' },
            { source: 5, target: 1, type: 'CALLS' }, { source: 6, target: 5, type: 'CALLS' }], [[1, 0], [2, 1], [5, 1], [3, 2], [6, 2]]);
        expect(at(result, 'n3').x).toBeGreaterThan(at(result, 'n2').x);
        expect(at(result, 'n6').x).toBeLessThan(at(result, 'n5').x);
        expect(at(result, 'n5').x).toBeLessThan(0);
        expect(result.placements.some(placement => placement.mixed)).toBe(false);
        expect(result.band).toBeUndefined();
    });

    /*
     * Review of K5: from two layers on, every node stood on its parent's side,
     * so the far-left "incoming" column of JSONBAgg held the callees of its
     * tests (len, str, print) and the classes general.py defines next to it.
     * A node reached through both directions belongs to neither side.
     */
    it('puts a callee of a caller and a caller of a callee in a labelled band below, never in an incoming or outgoing column', () => {
        const nodes = [node(1), node(10), node(20), node(30), node(40), node(50), node(60), node(70)];
        const result = scoped(nodes, [
            { source: 10, target: 1, type: 'CALLS' }, // a test calls the root: incoming
            { source: 10, target: 20, type: 'CALLS' }, // the test calls len: a callee of a caller
            { source: 30, target: 10, type: 'CALLS' }, // something calls the test: incoming twice
            { source: 1, target: 40, type: 'INHERITS' }, // the root inherits a base: outgoing
            { source: 40, target: 50, type: 'DEFINES_METHOD' }, // the base defines a method: outgoing twice
            { source: 60, target: 40, type: 'INHERITS' }, // a sibling inherits the base: a caller of a callee
            { source: 20, target: 70, type: 'CALLS' }, // reached from a mixed node: mixed as well
        ], [[1, 0], [10, 1], [40, 1], [20, 2], [30, 2], [50, 2], [60, 2], [70, 3]]);
        const mixed = result.placements.filter(placement => placement.mixed).map(placement => placement.name).sort();
        expect(mixed).toEqual(['n20', 'n60', 'n70']);
        // The pure chains keep their columns.
        expect(at(result, 'n30').x).toBeLessThan(at(result, 'n10').x);
        expect(at(result, 'n50').x).toBeGreaterThan(at(result, 'n40').x);
        // The band lies below every column, with a gap for its heading, and the heading counts it.
        const columns = result.placements.filter(placement => !placement.mixed);
        const lowest = Math.min(...columns.map(placement => placement.y));
        for (const name of mixed) expect(at(result, name).y).toBeLessThan(lowest - 2 * SCOPED_HIERARCHY_ROW_GAP);
        expect(result.band).toMatchObject({ count: 3 });
        expect(result.band!.y).toBeLessThan(lowest);
        expect(result.band!.y).toBeGreaterThan(Math.max(...mixed.map(name => at(result, name).y)));
        // The band is centred under the root, not under one of the sides.
        const xs = mixed.map(name => at(result, name).x);
        expect(Math.abs((Math.min(...xs) + Math.max(...xs)) / 2)).toBeLessThan(1);
        // Its frame, the heading on the top edge, holds every band node and none of the columns.
        for (const name of mixed) {
            const placement = at(result, name);
            expect(placement.x).toBeGreaterThan(result.band!.left); expect(placement.x).toBeLessThan(result.band!.right);
            expect(placement.y).toBeGreaterThan(result.band!.bottom); expect(placement.y).toBeLessThan(result.band!.y);
        }
        for (const placement of columns) expect(placement.y).toBeGreaterThan(result.band!.y);
    });

    it('names every node up to the budget; above it the root and its direct neighbours keep names and single columns', () => {
        const fan = (count: number, first: number) => Array.from({ length: count }, (_, at) => node(first + at));
        const small = scoped([node(1), ...fan(20, 100)], fan(20, 100).map(entry => ({ source: entry.id, target: 1, type: 'CALLS' })),
            [[1, 0], ...fan(20, 100).map(entry => [entry.id, 1] as [number, number])]);
        expect(small.names).toBe('all');
        // 40 direct callers, each with four callers of its own: 201 nodes.
        const callers = fan(40, 100), outer = fan(160, 1000);
        const edges = [...callers.map(entry => ({ source: entry.id, target: 1, type: 'CALLS' })),
            ...outer.map((entry, at) => ({ source: entry.id, target: callers[at % 40]!.id, type: 'CALLS' }))];
        const large = scoped([node(1), ...callers, ...outer], edges,
            [[1, 0], ...callers.map(entry => [entry.id, 1] as [number, number]), ...outer.map(entry => [entry.id, 2] as [number, number])]);
        expect(large.names).toBe('neighbours');
        // The direct callers stand in one column, spaced for their names.
        const first = large.placements.filter(placement => placement.hop === 1);
        expect(new Set(first.map(placement => placement.x)).size).toBe(1);
        expect(new Set(large.namedIds)).toEqual(new Set(large.placements.filter(placement => placement.hop <= 1).map(placement => placement.id)));
        // A hub with more direct neighbours than the budget names only the root, and a side of them that fits.
        const hub = fan(200, 100), callees = fan(5, 2000);
        const none = scoped([node(1), ...hub, ...callees], [...hub.map(entry => ({ source: entry.id, target: 1, type: 'CALLS' })),
            ...callees.map(entry => ({ source: 1, target: entry.id, type: 'CALLS' }))],
        [[1, 0], ...hub.map(entry => [entry.id, 1] as [number, number]), ...callees.map(entry => [entry.id, 1] as [number, number])]);
        expect(none.names).toBe('none');
        const namedNames = none.placements.filter(placement => none.namedIds?.includes(placement.id)).map(placement => placement.name).sort();
        expect(namedNames).toEqual(['n1', ...callees.map(entry => entry.name)].sort());
    });

    it('spaces columns by the names they carry, so long names keep their full width', () => {
        const long = { ...node(2), name: 'test_jsonb_agg_jsonfield_order_by' };
        const result = scoped([node(1), long], [{ source: 2, target: 1, type: 'CALLS' }], [[1, 0], [2, 1]]);
        expect(Math.abs(at(result, long.name).x)).toBeGreaterThanOrEqual((hierarchyLabelWidth(long.name) + hierarchyLabelWidth('n1')) / 2);
        expect(hierarchyLabelWidth(long.name)).toBeGreaterThan(200);
    });
});

it('keeps the whole graph on screen while the first scope picture is still empty', () => {
    const graph = (count: number): GraphData => ({ nodes: Array.from({ length: count }, (_, id) => ({ id, name: `n${id}`, label: 'Function', x: 0, y: 0, z: 0, size: 1, color: '#999999' })), edges: [], total_nodes: count });
    const layout = graph(5), stale = graph(2), current = graph(3), empty = graph(0);
    // An empty preview of a symbol outside the loaded layout is no picture.
    expect(scenePictureFor(empty, undefined, layout)).toBe(layout);
    expect(scenePictureFor(undefined, empty, layout)).toBe(layout);
    expect(scenePictureFor(empty, stale, layout)).toBe(stale);
    expect(scenePictureFor(current, stale, layout)).toBe(current);
    expect(scenePictureFor(undefined, stale, layout)).toBe(stale);
});

describe('handtest K8: deep layers load in few large pages, report progress and stop at the render limit', () => {
    const scope = { kind: 'node' as const, id: 1, name: 'n1', qualifiedName: 'p.n1' };
    const star = (from: GraphNode, count: number, first: number) => Array.from({ length: count }, (_, at) => edgeRow(first + at, from, node(first + at)));

    it('asks for large pages, so one hop is not split into dozens of re-executed continuations', async () => {
        const a = node(1), b = node(2);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)])).mockResolvedValueOnce(page([]));
        await loadGraphScope('p', scope, 1, 'both', undefined, { client: { queryGraph } });
        expect(queryGraph).toHaveBeenCalledTimes(3);
        for (const call of queryGraph.mock.calls) {
            expect(call[3]?.maxRows).toBeGreaterThanOrEqual(2000);
            expect(call[3]?.maxOutputTokens).toBeGreaterThanOrEqual(100_000);
        }
    });

    it('reports loaded nodes and edges after every request', async () => {
        const a = node(1), b = node(2), c = node(3);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)])).mockResolvedValueOnce(page([edgeRow(2, b, c)]));
        const progress = vi.fn();
        await loadGraphScope('p', scope, 2, 'outbound', undefined, { client: { queryGraph }, onProgress: progress });
        expect(progress.mock.calls.map(([value]) => [value.layer, value.nodes, value.edges, value.requests])).toEqual([[1, 2, 1, 2], [2, 3, 2, 3]]);
    });

    it('stops at the node limit, marks the layer partial and never continues from it', async () => {
        const a = node(1), b = node(2);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page([edgeRow(1, a, b)]))
            .mockResolvedValueOnce(page(star(b, 6, 10)));
        const result = await loadGraphScope('p', scope, 3, 'outbound', undefined, { client: { queryGraph }, limits: { nodes: 5, edges: 1000 } });
        expect(result.partial).toEqual({ layer: 2, nodes: 8, edges: 7, limit: 'nodes' });
        expect(result.exhausted).toBe(false);
        expect(result.depth).toBe(2);
        // Hop 3 was never requested: the layer before it was already over the limit.
        expect(queryGraph).toHaveBeenCalledTimes(3);
        const fresh = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)])).mockResolvedValue(page([]));
        await loadGraphScope('p', scope, 3, 'outbound', undefined, { client: { queryGraph: fresh }, previous: result, limits: { nodes: 50, edges: 1000 } });
        // A partial layer is no base for the next one: the load starts again from the root.
        expect(fresh.mock.calls[0]![1]).toMatch(/^MATCH \(n\) WHERE/);
    });

    /*
     * Hand test 2026-10-04 (G3): the warning named no reason, and next to it
     * stood the growth estimate (402 nodes), which alone stays far below the
     * limit. The warning says which limit it expects to pass, and the calls
     * counted at the edge drive it when they are the larger number.
     */
    it('G3: says which render limit the next layer likely passes and what drives it, and none where no limit applies', () => {
        const outlook = { layer: 3, frontier: 75, estimate: 402, calls: 9031, loaded: { nodes: 90, edges: 201 }, limits: { nodes: 5000, edges: 20000 } };
        expect(expandPastLimit(outlook)).toEqual({ limit: 'nodes', by: 'calls' });
        expect(expandPastLimit({ ...outlook, calls: 10 })).toBeUndefined();
        expect(expandPastLimit({ ...outlook, calls: undefined, loaded: { nodes: 4700, edges: 9000 } })).toEqual({ limit: 'nodes', by: 'growth' });
        expect(expandPastLimit({ ...outlook, calls: 3000, loaded: { nodes: 90, edges: 18500 } })).toEqual({ limit: 'edges', by: 'calls' });
        // The Explore mini-Galaxy loads without render limits: nothing to pass.
        expect(expandPastLimit({ ...outlook, limits: undefined })).toBeUndefined();
    });

    it('estimates the next layer from the frontier and the growth of the last layer', () => {
        const nodes = [1, 2, 3, 4, 5, 6, 7].map(id => node(id));
        const levels = new Map([[1, 0], [2, 1], [3, 1], [4, 2], [5, 2], [6, 2], [7, 2]]);
        const scoped = { data: { nodes, edges: [], total_nodes: 7 }, roots: new Set([1]), depth: 2, exhausted: false, levels };
        expect(nextLayerEstimate(scoped)).toEqual({ layer: 3, frontier: 4, perNode: 2, estimate: 8 });
        expect(nextLayerEstimate({ ...scoped, exhausted: true })).toBeUndefined();
        expect(nextLayerEstimate({ ...scoped, partial: { layer: 2, nodes: 7, edges: 6, limit: 'nodes' } })).toBeUndefined();
    });

    /* Review of K8: one batch of layer 3 read eight continuation pages, and the toolbar stood still for 8 s. */
    const continued = (records: Record<string, string>[][]) => {
        const total = records.flat().length;
        let at = 0;
        return records.map((rows, index) => {
            const offset = at; at += rows.length;
            return page(rows, { total, offset, ...(index < records.length - 1 ? { nextCursor: `c${index + 1}`, nextOffset: at } : {}) });
        });
    };

    it('reports progress after every page of a long batch, not only when the batch ends', async () => {
        const a = node(1);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]));
        for (const result of continued([star(a, 2, 10), star(a, 2, 12), star(a, 2, 14)])) queryGraph.mockResolvedValueOnce(result);
        const progress = vi.fn();
        await loadGraphScope('p', scope, 1, 'outbound', undefined, { client: { queryGraph }, onProgress: progress });
        expect(progress.mock.calls.map(([value]) => [value.nodes, value.edges, value.requests])).toEqual([[3, 2, 2], [5, 4, 3], [7, 6, 4]]);
    });

    it('stops inside a batch at the page that passes the limit, and never caches the cut neighbourhood', async () => {
        const a = node(1);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]));
        for (const result of continued([star(a, 3, 10), star(a, 3, 13), star(a, 3, 16)])) queryGraph.mockResolvedValueOnce(result);
        const cache = { node: () => undefined, roots: () => undefined, rememberRoots: vi.fn(), neighborhood: () => undefined, rememberNeighborhood: vi.fn() };
        const result = await loadGraphScope('p', scope, 2, 'outbound', undefined,
            { client: { queryGraph }, limits: { nodes: 5, edges: 1000 }, cache: cache as never });
        // Root, then two of the three pages: the second page passed five nodes.
        expect(queryGraph).toHaveBeenCalledTimes(3);
        expect(result.partial).toEqual({ layer: 1, nodes: 7, edges: 6, limit: 'nodes' });
        expect(result.data.nodes).toHaveLength(7);
        expect(cache.rememberNeighborhood).not.toHaveBeenCalled();
    });

    /*
     * Review of K8: growing like the last layer predicted about 400 nodes for layer 3 of JSONBAgg,
     * and over 9,000 came. Its 75 edge nodes carry 9,006 indexed calls (len, create, str, list ...);
     * the index counts them per node without walking a relationship.
     */
    describe('the indexed calls at the edge of a scope', () => {
        const degrees = (rows: [number, number, number][]) => page(rows.map(([id, calls_in, calls_out]) => ({ id: String(id), calls_in: String(calls_in), calls_out: String(calls_out) })));
        const edge = (node1: GraphNode, node2: GraphNode, node3: GraphNode) => ({
            data: { nodes: [node1, node2, node3], edges: [{ id: 1, source: 1, target: 2, type: 'CALLS' }, { id: 2, source: 1, target: 3, type: 'IMPORTS' }], total_nodes: 3 },
            roots: new Set([1]), depth: 1, exhausted: false, frontier: [2, 3], levels: new Map([[1, 0], [2, 1], [3, 1]]) });

        it('adds the calls into and out of the edge nodes and leaves out the ones already loaded', async () => {
            const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(degrees([[2, 5, 1], [3, 0, 4]]));
            expect(await frontierCallCount({ queryGraph }, 'p', edge(node(1), node(2), node(3)), 'both', undefined)).toBe(9);
            expect(queryGraph).toHaveBeenCalledTimes(1);
            expect(queryGraph.mock.calls[0]![1]).toMatch(/^MATCH \(n\) WHERE \(n\.qualified_name = "p\.n2" OR n\.qualified_name = "p\.n3"\) RETURN id\(n\) AS id, n\.in_degree AS calls_in, n\.out_degree AS calls_out$/);
        });

        it('counts only the traced direction, and nothing when calls are not traced', async () => {
            const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValue(degrees([[2, 5, 1], [3, 0, 4]]));
            expect(await frontierCallCount({ queryGraph }, 'p', edge(node(1), node(2), node(3)), 'outbound', undefined)).toBe(5);
            expect(await frontierCallCount({ queryGraph }, 'p', edge(node(1), node(2), node(3)), 'inbound', ['CALLS', 'TESTS'])).toBe(4);
            queryGraph.mockClear();
            expect(await frontierCallCount({ queryGraph }, 'p', edge(node(1), node(2), node(3)), 'both', ['IMPORTS'])).toBeUndefined();
            expect(await frontierCallCount({ queryGraph }, 'p', { ...edge(node(1), node(2), node(3)), exhausted: true }, 'both', undefined)).toBeUndefined();
            expect(queryGraph).not.toHaveBeenCalled();
        });
    });

    it('stops at the edge limit too', async () => {
        const a = node(1);
        const queryGraph = vi.fn<GraphQueryClient['queryGraph']>().mockResolvedValueOnce(page([row(a)]))
            .mockResolvedValueOnce(page(star(a, 4, 10)));
        const result = await loadGraphScope('p', scope, 2, 'outbound', undefined, { client: { queryGraph }, limits: { nodes: 100, edges: 3 } });
        expect(result.partial).toMatchObject({ layer: 1, edges: 4, limit: 'edges' });
    });
});
