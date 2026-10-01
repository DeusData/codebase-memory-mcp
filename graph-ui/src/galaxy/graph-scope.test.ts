import { describe, expect, it, vi } from 'vitest';
import { arrangeScopedGraph, graphEdgeTypesKey, limitGraphRender, loadGraphScope, readGraphPages, scopedHierarchy, type GraphQueryClient } from './graph-scope';
import type { QueryGraphResult } from '../provider/rpc-schemas';
import type { GraphNode } from './types';

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
it('retains simple columns for small hierarchy levels', () => {
    const nodes = [{ ...node(1), z: 0 }, { ...node(2), z: -18 }, { ...node(3), z: -18 }];
    const result = scopedHierarchy({ data: { nodes, edges: [], total_nodes: 3 }, roots: new Set([1]), depth: 1, exhausted: false }, 'small');
    expect(result.data.nodes.map(node => [node.x, node.y])).toEqual([[0, 0], [160, 16], [160, -16]]);
});
