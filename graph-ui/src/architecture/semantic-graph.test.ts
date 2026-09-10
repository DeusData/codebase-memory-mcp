import { describe, expect, it } from 'vitest';
import { buildSemanticGraph, semanticEntryPoints } from './semantic-graph';
import type { GraphData, GraphNode } from '../galaxy/types';

const node = (id: number, file: string, name = `symbol${id}`, status: GraphNode['status'] = 'normal'): GraphNode => ({
    id, name, label: 'Function', file_path: file, qualified_name: `fixture.${name}`, status,
    start_line: 10, end_line: 30, x: 0, y: 0, z: 0, color: '', size: 1,
});
const fixture: GraphData = { nodes: [node(1, 'src/api/server.ts', 'start', 'entry'),
    node(2, 'src/service/users.ts', 'users'), node(3, 'src/store/db.ts', 'save'),
    node(4, 'src/service/read.ts', 'read')], total_nodes: 4, edges: [
    { id: 1, source: 1, target: 2, type: 'CALLS', line: 15 },
    { id: 2, source: 2, target: 3, type: 'CALLS' },
    { id: 3, source: 2, target: 3, type: 'IMPORTS' },
    { id: 4, source: 2, target: 4, type: 'CALLS' },
    { id: 5, source: 3, target: 1, type: 'CALLS' },
] };

describe('semantic architecture projection', () => {
    it('preserves direction, relationship type and exact source evidence through aggregation', () => {
        const model = buildSemanticGraph(fixture, { view: 'dependencies' });
        expect(model.nodes.map(item => item.id)).toEqual(['area:src/api', 'area:src/service', 'area:src/store']);
        expect(model.edges.find(edge => edge.source === 'area:src/api')).toMatchObject({
            source: 'area:src/api', target: 'area:src/service', type: 'CALLS', count: 1,
        });
        const call = model.edges.find(edge => edge.source === 'area:src/api')!;
        expect(call.evidence[0].source).toBe(fixture.nodes[0]);
        expect(call.evidence[0].line).toBe(15);
        expect(model.edges.filter(edge => edge.source === 'area:src/service' && edge.target === 'area:src/store').map(edge => edge.type).sort())
            .toEqual(['CALLS', 'IMPORTS']);
        expect(model.nodes.every(item => item.graphNode === undefined)).toBe(true);
        expect(buildSemanticGraph(fixture, { view: 'dependencies', relations: [] }).edges).toEqual(model.edges);
    });
    it('drills directories into files and files into real selectable symbols', () => {
        const area = buildSemanticGraph(fixture, { view: 'overview', areaPath: 'src/service' });
        expect(area.nodes.filter(item => item.kind === 'file').map(item => item.id)).toEqual(['file:src/service/read.ts', 'file:src/service/users.ts']);
        expect(area.edges.find(edge => edge.target === 'file:src/service/read.ts')).toMatchObject({ source: 'file:src/service/users.ts', target: 'file:src/service/read.ts' });
        const file = buildSemanticGraph(fixture, { view: 'overview', filePath: 'src/service/users.ts' });
        expect(file.nodes.find(item => item.kind === 'symbol')?.graphNode).toBe(fixture.nodes[1]);
    });
    it('retains external source areas and exact boundary evidence when drilling a file', () => {
        const model = buildSemanticGraph(fixture, { view: 'dependencies', filePath: 'src/service/users.ts' });
        const symbol = model.nodes.find(item => item.graphNode === fixture.nodes[1])!;
        const outside = model.nodes.find(item => item.id === 'area:src/store')!;
        expect(outside.detail).toBe('External source area · inferred from file paths');
        const call = model.edges.find(edge => edge.source === symbol.id && edge.target === outside.id && edge.type === 'CALLS')!;
        expect(call.evidence[0].source).toBe(fixture.nodes[1]);
        expect(call.evidence[0].target).toBe(fixture.nodes[2]);
        expect(model.edges.some(edge => edge.source === 'area:src/store' && edge.target === 'area:src/api')).toBe(false);
        expect(buildSemanticGraph({ ...fixture, nodes: [...fixture.nodes].reverse(), edges: [...fixture.edges].reverse() },
            { view: 'dependencies', filePath: 'src/service/users.ts' })).toEqual(model);
    });
    it('keeps file-level imports on the file instead of assigning them to a symbol', () => {
        const file = { ...node(10, 'src/service/users.ts', 'users.ts'), label: 'File' };
        const graph = { ...fixture, nodes: [...fixture.nodes, file], edges: [
            { source: file.id, target: fixture.nodes[2].id, type: 'IMPORTS' },
        ] };
        const model = buildSemanticGraph(graph, { view: 'dependencies', filePath: file.file_path });
        expect(model.edges[0]).toMatchObject({ source: 'file:src/service/users.ts', target: 'area:src/store', type: 'IMPORTS' });
        expect(model.edges[0].evidence[0].source).toBe(file);
    });
    it('is invariant under backend row ordering and does not mutate the graph', () => {
        const original = JSON.stringify(fixture);
        const reversed = { ...fixture, nodes: [...fixture.nodes].reverse(), edges: [...fixture.edges].reverse() };
        for (const view of ['overview', 'dependencies', 'entryPoints', 'routes'] as const) {
            expect(buildSemanticGraph(reversed, { view })).toEqual(buildSemanticGraph(fixture, { view }));
        }
        expect(JSON.stringify(fixture)).toBe(original);
    });
    it('caps visible graph independently and accounts for every omitted projected node and edge', () => {
        const graph: GraphData = { nodes: Array.from({ length: 70 }, (_, id) => node(id, `services/service${id}/index.ts`)),
            edges: Array.from({ length: 69 }, (_, id) => ({ source: id, target: id + 1, type: 'CALLS' })), total_nodes: 70 };
        const model = buildSemanticGraph(graph, { view: 'overview', maxEdges: 5 });
        expect(model.nodes).toHaveLength(40);
        expect(model.omittedNodes).toBe(30);
        expect(model.edges).toHaveLength(5);
        expect(model.omittedEdges + model.edges.length).toBe(69);
        expect(model.edges.every(edge => model.nodes.some(item => item.id === edge.source)
            && model.nodes.some(item => item.id === edge.target))).toBe(true);
    });
    it('computes shortest static reachability through cycles and exposes the hop boundary', () => {
        const model = buildSemanticGraph(fixture, { view: 'entryPoints', entryId: 1, depth: 1 });
        expect(model.nodes.map(item => [item.label, item.depth])).toEqual([['start', 0], ['users', 1]]);
        expect(model.warnings.join(' ')).toContain('1-hop boundary');
        const deep = buildSemanticGraph(fixture, { view: 'entryPoints', entryId: 1, depth: 8 });
        expect(deep.nodes).toHaveLength(4);
        expect(deep.nodes.find(item => item.label === 'start')?.depth).toBe(0);
        expect(deep.nodes.find(item => item.label === 'save')?.depth).toBe(2);
        expect(deep.edges.some(edge => edge.evidence[0].source.id === 3 && edge.evidence[0].target.id === 1)).toBe(true);
        expect(deep.edges.every(edge => edge.type === 'CALLS')).toBe(true);
        expect(deep.positionMeaning).toContain('not runtime execution order');
    });
    it('uses entry classifications and exact identities rather than names and excludes tests', () => {
        const graph: GraphData = { nodes: [node(1, 'a.ts', 'main'), node(2, 'b.ts', 'another'),
            node(3, 'test.ts', 'test', 'test')], edges: [], total_nodes: 3 };
        expect(semanticEntryPoints(graph)).toEqual([]);
        expect(semanticEntryPoints(graph, ['fixture.another', 'fixture.test']).map(item => item.id)).toEqual([2]);
        expect(buildSemanticGraph(graph, { view: 'entryPoints', entryId: 2 }).nodes[0].graphNode?.id).toBe(2);
    });
    it('retains a recursive symbol edge when its source and target are the same', () => {
        const model = buildSemanticGraph({ ...fixture, edges: [{ source: 1, target: 1, type: 'CALLS' }] }, { view: 'entryPoints' });
        expect(model.edges).toHaveLength(1);
        expect(model.edges[0].source).toBe(model.edges[0].target);
        expect(model.edges[0].evidence[0].source).toBe(fixture.nodes[0]);
    });
    it('keeps unresolved endpoints and invalid callsite coordinates honest', () => {
        const model = buildSemanticGraph({ ...fixture, edges: [
            { source: 1, target: 2, type: 'CALLS', line: 900 }, { source: 1, target: 99, type: 'CALLS' },
        ] }, { view: 'dependencies' });
        expect(model.edges[0].evidence[0].line).toBeUndefined();
        expect(model.warnings.join(' ')).toContain('endpoint outside');
    });
});

describe('route identities and service evidence', () => {
    const route: GraphNode = { id: 10, label: 'Route', name: '/users', qualified_name: 'fixture.route.users', x: 0, y: 0, z: 0, size: 1, color: '' };
    it('renders route registrations without manufacturing relationships to named handlers', () => {
        const model = buildSemanticGraph({ ...fixture, nodes: [...fixture.nodes, route] }, { view: 'routes',
            routes: [{ method: 'GET', path: '/users', filePath: 'src/api/server.ts', line: 10, handler: 'users', origin: 'source' }] });
        expect(model.nodes.filter(item => item.kind === 'route')).toHaveLength(2);
        expect(model.edges).toEqual([]);
        expect(model.nodes.some(item => item.detail.includes('handler relationship not resolved'))).toBe(true);
    });
    it('deduplicates exact indexed route registrations without guessing unresolved source registrations', () => {
        const model = buildSemanticGraph({ ...fixture, nodes: [...fixture.nodes, route] }, { view: 'routes',
            routes: [{ method: 'GET', path: '/users', origin: 'index' }] });
        expect(model.nodes.filter(item => item.kind === 'route')).toHaveLength(1);
    });
    it('uses QN and file identity across generations; re-used numeric IDs cannot select a different symbol', () => {
        const current = { ...fixture.nodes[0], id: 101, start_line: 20 };
        const unrelated = { ...fixture.nodes[1], id: 1 };
        const graph = { nodes: [current, unrelated, route], edges: [], total_nodes: 3 };
        const model = buildSemanticGraph(graph, { view: 'routes', routeSnapshot: { truncated: false, warnings: [], relationships: [
            { source: fixture.nodes[0], target: { ...route, id: 999 }, type: 'HTTP_CALLS', routePath: '/users' },
        ] } });
        expect(model.edges[0].evidence[0].source).toBe(current);
        expect(model.edges[0].evidence[0].target).toBe(route);
        expect(model.nodes.find(item => item.kind === 'route')?.graphNode).toBe(route);
        expect(model.nodes.find(item => item.kind === 'area')?.members).toEqual([current]);
    });
    it('keeps callers, paths and explicit handlers as separate typed relationships', () => {
        const graph = { ...fixture, nodes: [...fixture.nodes, route] };
        const model = buildSemanticGraph(graph, { view: 'routes', routeSnapshot: { truncated: true, warnings: [], relationships: [
            { source: fixture.nodes[0], target: route, type: 'HTTP_CALLS', routePath: '/users', via: 'fetch' },
            { source: route, target: fixture.nodes[1], type: 'HANDLES' },
        ] } });
        expect(model.edges.map(edge => edge.type).sort()).toEqual(['HANDLES', 'HTTP_CALLS']);
        expect(model.nodes.find(item => item.id === 'caller-area:src/api')?.position[0]).toBe(-32);
        expect(model.nodes.find(item => item.id === 'handler-area:src/service')?.position[0]).toBe(32);
        expect(model.nodes.find(item => item.kind === 'route')?.position[0]).toBe(0);
        expect(model.warnings.join(' ')).toContain('missing lines do not prove');
    });
    it('keeps a matching route and its caller context when filtering by path', () => {
        const caller = node(1, 'services/gateway/client.ts', 'request');
        const graph: GraphData = { nodes: [caller, route], edges: [], total_nodes: 2 };
        const model = buildSemanticGraph(graph, { view: 'routes', filter: '/users', routeSnapshot: {
            truncated: false, warnings: [], relationships: [{ source: caller, target: route, type: 'HTTP_CALLS' }],
        } });
        expect(model.nodes.map(item => item.kind).sort()).toEqual(['area', 'route']);
        expect(model.edges).toHaveLength(1);
        expect(model.edges[0].evidence[0].source).toBe(caller);
        expect(model.warnings.join(' ')).toContain('one-hop neighbors');
    });
    it('retains a path match before its equally connected caller at a tight scene cap', () => {
        const callers = [node(1, 'services/gateway/client.ts', 'request')];
        const graph: GraphData = { nodes: [...callers, route], edges: [], total_nodes: 2 };
        const model = buildSemanticGraph(graph, { view: 'routes', filter: '/users', maxNodes: 1, routeSnapshot: {
            truncated: false, warnings: [], relationships: callers.map(source => ({ source, target: route, type: 'HTTP_CALLS' as const })),
        } });
        expect(model.nodes).toHaveLength(1);
        expect(model.nodes.some(item => item.kind === 'route')).toBe(true);
        expect(model.edges).toHaveLength(0);
        expect(model.omittedNodes).toBe(1);
    });
    it('packs large route lanes into compact deterministic bands without overlap', () => {
        const routes = Array.from({ length: 35 }, (_, id) => ({ ...route, id: 100 + id, name: `/api/${id}`, qualified_name: `fixture.route.${id}` }));
        const caller = node(1, 'services/gateway/client.ts', 'request');
        const graph: GraphData = { nodes: [caller, ...routes], edges: [], total_nodes: 36 };
        const options = { view: 'routes' as const, routeSnapshot: { truncated: false, warnings: [],
            relationships: routes.map(target => ({ source: caller, target, type: 'HTTP_CALLS' as const })) } };
        const model = buildSemanticGraph(graph, options);
        const routeNodes = model.nodes.filter(item => item.kind === 'route');
        expect(new Set(routeNodes.map(item => item.position[2])).size).toBeLessThanOrEqual(5);
        expect(Math.max(...routeNodes.map(item => item.position[2])) - Math.min(...routeNodes.map(item => item.position[2]))).toBeLessThanOrEqual(80);
        const callerX = model.nodes.find(item => item.kind === 'area')!.position[0];
        expect(Math.min(...routeNodes.map(item => item.position[0])) - callerX).toBeGreaterThan(12);
        expect(new Set(routeNodes.map(item => item.position.join(','))).size).toBe(35);
        expect(buildSemanticGraph({ ...graph, nodes: [...graph.nodes].reverse() }, options)).toEqual(model);
    });
});

it('packs entry-point siblings while preserving depth order between bands', () => {
    const root = node(1, 'src/main.ts', 'main', 'entry');
    const children = Array.from({ length: 30 }, (_, id) => node(id + 2, 'src/work.ts', `work${id}`));
    const leaf = node(100, 'src/leaf.ts', 'leaf');
    const graph: GraphData = { nodes: [root, ...children, leaf], total_nodes: 32,
        edges: [...children.map(child => ({ source: root.id, target: child.id, type: 'CALLS' })),
            { source: children[0].id, target: leaf.id, type: 'CALLS' }] };
    const model = buildSemanticGraph(graph, { view: 'entryPoints' });
    const siblings = model.nodes.filter(item => item.depth === 1);
    expect(new Set(siblings.map(item => item.position[2])).size).toBeLessThanOrEqual(5);
    expect(Math.min(...siblings.map(item => item.position[0]))).toBeGreaterThan(model.nodes.find(item => item.depth === 0)!.position[0]);
    expect(Math.max(...siblings.map(item => item.position[0]))).toBeLessThan(model.nodes.find(item => item.depth === 2)!.position[0]);
});
