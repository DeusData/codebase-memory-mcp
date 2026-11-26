import { describe, expect, it } from 'vitest';
import { selectionContext } from './selection-context';
import type { GraphData, GraphNode } from '../galaxy/types';

const node = (id: number, status: GraphNode['status'] = 'normal'): GraphNode => ({
    id, file_path: `src/${id}.ts`, name: `s${id}`, status, label: 'Function',
    x: 0, y: 0, z: 0, size: 1, color: '#fff',
});
const graph: GraphData = { total_nodes: 4, nodes: [node(1, 'entry'), node(2), node(3), node(4, 'entry')],
    edges: [{ source: 1, target: 2, type: 'CALLS' }, { source: 2, target: 3, type: 'CALLS' },
        { source: 3, target: 2, type: 'CALLS' }, { source: 4, target: 3, type: 'IMPORTS' }] };

describe('selection relevance', () => {
    it('explains a bounded, reproducible entry CALLS path in invocation order', () => {
        const result = selectionContext(graph, graph.nodes[2], 'src/3.ts');
        expect(result.entryPath.map(edge => [edge.source.id, edge.target.id])).toEqual([[1, 2], [2, 3]]);
        expect(result.entryPath.every(edge => edge.type === 'CALLS')).toBe(true);
        expect(result.incoming.map(edge => edge.type)).toEqual(['CALLS', 'IMPORTS']);
    });
    it('never treats imports alone as a call or data-flow path', () => {
        const result = selectionContext({ ...graph, edges: graph.edges.filter(edge => edge.type === 'IMPORTS') }, graph.nodes[2], 'src/3.ts');
        expect(result.entryPath).toEqual([]);
        expect(result.incoming[0].type).toBe('IMPORTS');
    });
    it('names an incomplete path search instead of saying no entry path exists', () => {
        const long: GraphData = { total_nodes: 7, nodes: Array.from({ length: 7 }, (_, index) => node(index, index === 0 ? 'entry' : 'normal')),
            edges: Array.from({ length: 6 }, (_, index) => ({ source: index, target: index + 1, type: 'CALLS' })) };
        const result = selectionContext(long, long.nodes[6], 'src/6.ts');
        expect(result.entryPath).toEqual([]);
        expect(result.pathSearchLimited).toBe(true);
    });
    it('resolves stable source identity after reindexing and rejects a recycled numeric ID', () => {
        const selected = { ...graph.nodes[2], qualified_name: 'project.target', id: 98 };
        const current = { ...graph, nodes: graph.nodes.map(n => n.id === 3 ? { ...n, qualified_name: 'project.target' } : n) };
        expect(selectionContext(current, selected, 'src/3.ts').entryPath).toHaveLength(2);
        expect(selectionContext(graph, { ...graph.nodes[2], name: 'removed' }, 'src/3.ts').incoming).toEqual([]);
    });
    it('returns an honest empty context for unavailable data and unknown files', () => {
        expect(selectionContext(undefined, undefined, '').incoming).toEqual([]);
        expect(selectionContext(graph, undefined, 'not-indexed.ts').incoming).toEqual([]);
    });
});

describe('hand test K13: relationships from the loaded Galaxy scope', () => {
    // JSONBAgg in django-demo: 11 tests CALL and TEST it, its module DEFINES it, and it INHERITS twice.
    const root = { ...node(1), name: 'JSONBAgg', qualified_name: 'p.JSONBAgg', file_path: 'general.py' };
    const tests = Array.from({ length: 11 }, (_, at) => ({ ...node(10 + at), file_path: 'tests/test_aggregates.py' }));
    const module = { ...node(30), label: 'Module', file_path: 'general.py' };
    const bases = [node(40), node(41)];
    const scope: GraphData = { total_nodes: 15, nodes: [root, ...tests, module, ...bases], edges: [
        ...tests.flatMap(test => [{ source: test.id, target: 1, type: 'CALLS', line: 5 }, { source: test.id, target: 1, type: 'TESTS' }]),
        { source: 30, target: 1, type: 'DEFINES' }, { source: 1, target: 40, type: 'INHERITS' }, { source: 1, target: 41, type: 'INHERITS' }] };

    it('keeps every relationship type of the scope, not only the repository-map relations', () => {
        const result = selectionContext(scope, root, 'general.py', undefined, { allRelations: true });
        expect(result.incoming).toHaveLength(23);
        expect(result.outgoing).toHaveLength(2);
        expect(result.incomingByType).toEqual([['CALLS', 11], ['TESTS', 11], ['DEFINES', 1]]);
        expect(result.outgoingByType).toEqual([['INHERITS', 2]]);
    });

    it('still limits the repository map snapshot to its map relations', () => {
        const result = selectionContext(scope, root, 'general.py');
        expect(result.incoming).toHaveLength(11);
        expect(result.incomingByType).toEqual([['CALLS', 11]]);
    });
});
