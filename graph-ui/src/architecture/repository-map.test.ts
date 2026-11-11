import { describe, expect, it } from 'vitest';
import { areaConnections, areaLevels, areaOf, areaTrail, repositoryMap } from './repository-map';
import type { GraphData, GraphNode } from '../galaxy/types';

export const node = (id: number, file_path: string, name = `symbol${id}`, status: GraphNode['status'] = 'normal'): GraphNode => ({
    id, file_path, name, status, label: 'Function', qualified_name: `fixture.${name}`, start_line: id + 1,
    x: 0, y: 0, z: 0, size: 1, color: '#fff',
});
export const graph: GraphData = { total_nodes: 4, nodes: [node(1, 'src/api/server.ts', 'start', 'entry'),
    node(2, 'src/service/users.ts', 'users'), node(3, 'src/store/db.ts', 'save'), node(4, 'test/users.test.ts', 'testUsers', 'test')],
edges: [{ source: 1, target: 2, type: 'CALLS' }, { source: 2, target: 3, type: 'CALLS' },
    { source: 2, target: 3, type: 'IMPORTS' }, { source: 4, target: 2, type: 'CALLS' }] };

describe('area trails', () => {
    it('repeats the repository area rule below each opened area', () => {
        expect(areaTrail('django/contrib/admin/options.py')).toEqual(['django', 'django/contrib', 'django/contrib/admin']);
        expect(areaTrail('django/shortcuts.py')).toEqual(['django']);
        expect(areaTrail('graph-ui/src/app/AtlasChrome.tsx')).toEqual(['graph-ui', 'graph-ui/src/app']);
        expect(areaTrail('graph-ui/src/App.tsx')).toEqual(['graph-ui', 'graph-ui/src']);
        expect(areaTrail('src/mcp/mcp.c')).toEqual(['src/mcp']);
        expect(areaTrail('Makefile')).toEqual(['(root)']);
        for (const path of ['django/contrib/admin/options.py', 'graph-ui/src/app/AtlasChrome.tsx', 'src/mcp/mcp.c', 'Makefile']) {
            expect(areaTrail(path)[0]).toBe(areaOf(path));
        }
    });
    it('reconstructs the drill-in steps of an area from the area alone', () => {
        expect(areaLevels('django/contrib/admin')).toEqual(['django', 'django/contrib', 'django/contrib/admin']);
        expect(areaLevels('graph-ui/src')).toEqual(['graph-ui', 'graph-ui/src']);
        expect(areaLevels('src/mcp')).toEqual(['src/mcp']);
        expect(areaLevels('(root)')).toEqual(['(root)']);
    });
});

describe('repository map evidence', () => {
    it('retains exact directional edges and their source declarations across areas', () => {
        const map = repositoryMap(graph);
        const service = map.areas.find(area => area.path === 'src/service')!;
        expect(service.incoming.map(edge => edge.source.name)).toEqual(['start', 'testUsers']);
        expect(areaConnections(service, 'outgoing').map(row => [row.area, row.type, row.evidence.length]))
            .toEqual([['src/store', 'CALLS', 1], ['src/store', 'IMPORTS', 1]]);
        expect(service.outgoing[0].target).toBe(graph.nodes[2]);
        expect(service.outgoing[0].source.start_line).toBe(3);
        expect(map.areas.find(area => area.path === 'src/api')?.entryPoints[0].name).toBe('start');
    });
    it('does not invent edges, double-count retries, or turn containment into dependencies', () => {
        const map = repositoryMap({ ...graph, edges: [...graph.edges, graph.edges[0],
            { source: 1, target: 77, type: 'CALLS' }, { source: 1, target: 3, type: 'CONTAINS' }] });
        expect(map.evidence).toHaveLength(4);
        expect(map.unresolvedEdges).toBe(1);
    });
    it('keeps segment boundaries and source areas meaningful without merging similarly named folders', () => {
        expect(areaOf('src/api/a.ts')).toBe('src/api');
        expect(areaOf('src/api2/b.ts')).toBe('src/api2');
        expect(areaOf('README.md')).toBe('(root)');
        expect(areaOf('packages/client/src/a.ts')).toBe('packages/client');
        expect(areaOf('tests/unit/users.ts')).toBe('tests');
    });
    it('keeps impossible index coordinates as unverified metadata, not source links', () => {
        const source = { ...graph.nodes[0], start_line: 20, end_line: 80 };
        const map = repositoryMap({ ...graph, nodes: [source, ...graph.nodes.slice(1)], edges: [
            { id: 7, source: 1, target: 2, type: 'CALLS', line: 6989 },
            { id: 8, source: 1, target: 3, type: 'CALLS', line: 61 },
        ] });
        expect(map.evidence[0]).toMatchObject({ id: 7, line: undefined, unverifiedLine: 6989 });
        expect(map.evidence[1]).toMatchObject({ id: 8, line: 61, unverifiedLine: undefined });
    });
});
