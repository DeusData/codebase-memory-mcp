import { describe, expect, it } from 'vitest';
import { buildSemanticGraph, distinctLabels, layoutFolderHierarchy, readableRoute, semanticEntryPoints, type SemanticNode, type SemanticPlatform } from './semantic-graph';
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

const containsNode = (platform: SemanticPlatform, item: SemanticNode) => Math.abs(platform.position[0] - item.position[0]) + 10 <= platform.width / 2
    && Math.abs(platform.position[2] - item.position[2]) + 10 <= platform.depth / 2;
const hierarchyNode = (id: string, filePath?: string, line = 1): SemanticNode => ({ id, kind: 'symbol', label: id, detail: '',
    position: [0, 0, 0], count: 1, members: [], filePath, line });

describe('source folder hierarchy layout', () => {
    it('connects overview and dependency projections to real ancestor platforms without changing evidence', () => {
        const input: GraphData = { ...fixture, nodes: [...fixture.nodes, node(20, 'apps/web/app.ts'), node(21, 'README.md')], total_nodes: 6 };
        const full = buildSemanticGraph(input, { view: 'overview' });
        expect(full.platforms?.map(platform => platform.path).sort()).toEqual(['apps', 'src']);
        const source = full.platforms!.find(platform => platform.path === 'src')!;
        full.nodes.filter(item => item.areaPath?.startsWith('src/')).forEach(item => expect(containsNode(source, item)).toBe(true));
        expect(full.nodes.find(item => item.areaPath === '(root)')?.position[1]).toBeCloseTo(0.08);
        const filtered = buildSemanticGraph(input, { view: 'dependencies', filter: 'src/store' });
        expect(filtered.platforms?.map(platform => platform.path)).toEqual(['src']);
        filtered.nodes.forEach(item => expect(item.position).toEqual(full.nodes.find(original => original.id === item.id)!.position));
        filtered.edges.forEach(edge => expect(edge).toEqual(full.edges.find(original => original.id === edge.id)));
        // The cap applies before the layout, so a capped map frames only what it draws.
        const capped = buildSemanticGraph(input, { view: 'overview', maxNodes: 1 });
        const cappedSource = capped.platforms!.find(platform => platform.path === 'src')!;
        expect(containsNode(cappedSource, capped.nodes[0])).toBe(true);
        expect(cappedSource.width * cappedSource.depth).toBeLessThan(source.width * source.depth);
        const imports = buildSemanticGraph(input, { view: 'dependencies', relations: ['IMPORTS'] });
        imports.nodes.forEach(item => expect(item.position).toEqual(full.nodes.find(original => original.id === item.id)!.position));
    });
    it('packs nested siblings and node footprints separately while retaining ancestor containment', () => {
        const nodes = Array.from({ length: 24 }, (_, index) => hierarchyNode(`symbol${index}`,
            `services/service${Math.floor(index / 8)}/src/file${Math.floor(index / 3)}.ts`, index));
        const platforms = layoutFolderHierarchy(nodes);
        platforms.forEach((platform, index) => platforms.slice(index + 1).forEach(other => {
            if (platform.path.startsWith(`${other.path}/`) || other.path.startsWith(`${platform.path}/`)) return;
            expect(Math.abs(platform.position[0] - other.position[0]) >= (platform.width + other.width) / 2
                || Math.abs(platform.position[2] - other.position[2]) >= (platform.depth + other.depth) / 2).toBe(true);
        }));
        for (const item of nodes) {
            platforms.filter(platform => item.filePath!.startsWith(`${platform.path}/`)).forEach(platform => expect(containsNode(platform, item)).toBe(true));
            const parent = platforms.find(platform => platform.path === item.filePath!.slice(0, item.filePath!.lastIndexOf('/')))!;
            expect(item.position[1] - parent.position[1]).toBeCloseTo(0.08);
        }
        nodes.forEach((item, index) => nodes.slice(index + 1).forEach(other => expect(Math.hypot(item.position[0] - other.position[0], item.position[2] - other.position[2])).toBeGreaterThanOrEqual(20)));
    });
    it('keeps root files flat, caps real ancestor depth, and separates unknown source without inventing a path', () => {
        const nodes = [hierarchyNode('root', 'root.ts'), hierarchyNode('deep', 'services/api/src/internal/jobs/run.ts'), hierarchyNode('unknown')];
        const before = nodes.map(item => ({ ...item, position: undefined }));
        const platforms = layoutFolderHierarchy(nodes);
        expect(platforms.filter(platform => platform.path).map(platform => platform.path)).toEqual(['services', 'services/api', 'services/api/src']);
        expect(platforms.every(platform => platform.level <= 3)).toBe(true);
        expect(platforms.find(platform => platform.label === 'Unknown source')?.path).toBe('');
        expect(nodes[0].position[1]).toBeCloseTo(0.08);
        expect(nodes[1].filePath).toBe('services/api/src/internal/jobs/run.ts');
        expect(nodes.map(item => ({ ...item, position: undefined }))).toEqual(before);
        const reversed = nodes.map(item => ({ ...item, position: [0, 0, 0] as [number, number, number] })).reverse();
        expect(layoutFolderHierarchy(reversed)).toEqual(platforms);
        reversed.forEach(item => expect(item.position).toEqual(nodes.find(original => original.id === item.id)!.position));
        expect(layoutFolderHierarchy([])).toEqual([]);
    });
    it('keeps direct source-folder files beside descendant area bricks inside their common source container', () => {
        const input: GraphData = { nodes: [node(1, 'src/main.ts'), node(2, 'src/store/db.ts')], edges: [], total_nodes: 2 };
        const model = buildSemanticGraph(input, { view: 'overview' });
        expect(model.nodes.map(item => item.id)).toEqual(['area:src', 'area:src/store']);
        expect(model.platforms?.map(platform => platform.path)).toEqual(['src']);
        model.nodes.forEach(item => expect(containsNode(model.platforms![0], item)).toBe(true));
        expect(model.edges).toEqual([]);
    });
});

describe('drilling into large areas', () => {
    // A Django-like top-level folder: 45 sub-packages, two direct files, and callers outside it.
    const inside = Array.from({ length: 45 }, (_, index) => node(100 + index, `django/app${String(index).padStart(2, '0')}/models/base.py`, `model${index}`));
    const large: GraphData = { total_nodes: 52, nodes: [...inside, node(200, 'django/shortcuts.py', 'render'), node(201, 'django/__init__.py', 'setup'),
        node(202, 'tests/admin_views/tests.py', 'test_admin'), node(203, 'tests/runtests.py', 'run'), node(204, 'setup.py', 'install'),
        node(205, 'django/app00/views/list.py', 'list_view'), node(206, 'django/app01/models/fields/json.py', 'JSONField')],
    edges: [...inside.slice(1).map((item, index) => ({ source: item.id, target: inside[index].id, type: 'CALLS' })),
        ...inside.slice(0, 6).map(item => ({ source: 202, target: item.id, type: 'CALLS' })), { source: 203, target: 200, type: 'CALLS' },
        ...inside.slice(0, 4).map(item => ({ source: 204, target: item.id, type: 'IMPORTS' })), { source: 204, target: 201, type: 'IMPORTS' },
        { source: 205, target: 100, type: 'CALLS' }, { source: 206, target: 100, type: 'CALLS' }] };
    const insidePlatform = (model: ReturnType<typeof buildSemanticGraph>) => model.platforms!.find(platform => platform.path === 'django')!;

    it('opens a top-level folder at its next level, not at its files', () => {
        expect(buildSemanticGraph(large, { view: 'overview' }).nodes.find(item => item.id === 'area:django')?.label).toBe('django');
        const model = buildSemanticGraph(large, { view: 'overview', areaPath: 'django', maxNodes: 80 });
        const inner = model.nodes.filter(item => !item.external);
        expect(inner.filter(item => item.kind === 'area')).toHaveLength(45);
        expect(inner.filter(item => item.kind === 'file').map(item => [item.id, item.label])).toEqual([
            ['file:django/__init__.py', '__init__.py'], ['file:django/shortcuts.py', 'shortcuts.py']]);
        expect(model.nodes.find(item => item.id === 'area:django/app00')).toMatchObject({ label: 'app00', areaPath: 'django/app00', count: 2 });
        expect(model.nodes.some(item => item.filePath?.startsWith('django/app'))).toBe(false);
        expect(model.edges.find(edge => edge.source === 'area:django/app01' && edge.target === 'area:django/app00')?.count).toBe(2);
    });
    it('caps before the folder layout and keeps outside areas in their own lane', () => {
        const model = buildSemanticGraph(large, { view: 'overview', areaPath: 'django' });
        expect(model.nodes).toHaveLength(40);
        expect(model.omittedNodes).toBe(model.totalNodes - 40);
        const outside = model.nodes.filter(item => item.external);
        expect(outside.map(item => item.id).sort()).toEqual(['area:(root)', 'area:tests']);
        outside.forEach(item => expect(item.detail).toBe('External source area · inferred from file paths'));
        const platform = insidePlatform(model);
        const lane = model.platforms!.find(item => item.id === 'hierarchy:outside')!;
        expect(lane.label).toBe('Outside');
        expect(model.platforms!.map(item => item.id).sort()).toEqual(['hierarchy:folder:django', 'hierarchy:outside']);
        model.nodes.filter(item => !item.external).forEach(item => expect(containsNode(platform, item)).toBe(true));
        outside.forEach(item => { expect(containsNode(lane, item)).toBe(true); expect(containsNode(platform, item)).toBe(false); });
        // The platform is exactly the packing of the drawn bricks, not of all 47 inner parts.
        const drawn = model.nodes.filter(item => !item.external).map(item => ({ ...item, position: [0, 0, 0] as [number, number, number] }));
        const repacked = layoutFolderHierarchy(drawn, 'django').find(item => item.path === 'django')!;
        expect([platform.width, platform.depth]).toEqual([repacked.width, repacked.depth]);
        const complete = insidePlatform(buildSemanticGraph(large, { view: 'overview', areaPath: 'django', maxNodes: 80 }));
        expect(platform.width * platform.depth).toBeLessThan(complete.width * complete.depth);
    });
    it('names outside areas as siblings of a nested area and keeps relative labels below it', () => {
        const nested = buildSemanticGraph(large, { view: 'overview', areaPath: 'django/app00' });
        expect(nested.title).toBe('django/app00');
        expect(nested.nodes.filter(item => !item.external).map(item => [item.id, item.label])).toEqual([
            ['area:django/app00/models', 'models'], ['area:django/app00/views', 'views']]);
        expect(nested.nodes.filter(item => item.external).map(item => item.id).sort()).toEqual(['area:(root)', 'area:django/app01', 'area:tests']);
        expect(nested.platforms!.find(item => item.path === 'django/app00')?.label).toBe('django/app00');
        const deeper = buildSemanticGraph(large, { view: 'overview', areaPath: 'django/app01/models' });
        expect(deeper.nodes.filter(item => !item.external).map(item => item.label)).toEqual(['fields', 'base.py']);
        expect(deeper.platforms!.filter(item => item.id !== 'hierarchy:outside').map(item => item.path)).toEqual(['django/app01/models']);
    });
    it('applies the source-container rule relative to the opened area', () => {
        const ui: GraphData = { total_nodes: 3, edges: [], nodes: [node(1, 'graph-ui/src/App.tsx', 'App'),
            node(2, 'graph-ui/src/app/AtlasChrome.tsx', 'AtlasChrome'), node(3, 'graph-ui/vite.config.ts', 'config')] };
        const model = buildSemanticGraph(ui, { view: 'overview', areaPath: 'graph-ui' });
        expect(model.nodes.map(item => [item.id, item.label])).toEqual([['area:graph-ui/src', 'src'], ['area:graph-ui/src/app', 'src/app'],
            ['file:graph-ui/vite.config.ts', 'vite.config.ts']]);
        expect(buildSemanticGraph(ui, { view: 'overview', areaPath: 'graph-ui/src' }).nodes.map(item => item.id)).toEqual(['file:graph-ui/src/App.tsx']);
    });
});

describe('semantic architecture projection', () => {
    it('represents disconnected and inventory-only files without inventing graph evidence', () => {
        const graph: GraphData = { nodes: [node(1, 'src/api/main.ts'), { ...node(2, 'isolated/empty.ts'), label: 'File' }], edges: [], total_nodes: 2 };
        const knownFiles = ['src/api/main.ts', 'isolated/empty.ts', 'assets/logo.svg'];
        const model = buildSemanticGraph(graph, { view: 'overview', knownFiles });
        expect(model.nodes.map(node => node.id)).toEqual(['area:assets', 'area:isolated', 'area:src/api']);
        expect(model.edges).toEqual([]);
        const empty = buildSemanticGraph(graph, { view: 'overview', knownFiles, areaPath: 'isolated' });
        expect(empty.nodes[0].filePath).toBe('isolated/empty.ts');
        const missing = buildSemanticGraph(graph, { view: 'overview', knownFiles, filePath: 'assets/logo.svg' });
        expect(missing.nodes[0]).toMatchObject({ kind: 'file', filePath: 'assets/logo.svg', members: [], count: 0 });
        expect(missing.nodes[0].graphNode).toBeUndefined();
        expect(buildSemanticGraph(graph, { view: 'overview', knownFiles, visibleFiles: new Set(['assets/logo.svg']) }).nodes.map(node => node.id)).toEqual(['area:assets']);
    });
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
    it('groups endpoints by their first path segment with counts and lists them again under a filter', () => {
        const routes = ['/accounts/login/', '/accounts/logout/', '/admin/', '/'].map((name, index) => ({ ...route, id: 300 + index, name, qualified_name: `fixture.route.${index}` }));
        const caller = node(1, 'services/gateway/client.ts', 'request');
        const graph: GraphData = { nodes: [caller, ...routes], edges: [], total_nodes: 5 };
        const routeSnapshot = { truncated: false, warnings: [], relationships: routes.map(target => ({ source: caller, target, type: 'HTTP_CALLS' as const })) };
        const model = buildSemanticGraph(graph, { view: 'routes', groupRoutes: true, routeSnapshot });
        expect(model.nodes.filter(item => item.kind === 'route').map(item => item.label).sort()).toEqual(['/', '/accounts · 2', '/admin/']);
        const group = model.nodes.find(item => item.routePrefix === '/accounts')!;
        expect(group).toMatchObject({ id: 'route-group:/accounts', count: 2, kindLabel: 'Route group' });
        expect(group.members.map(item => item.name)).toEqual(['/accounts/login/', '/accounts/logout/']);
        const calls = model.edges.find(edge => edge.target === group.id)!;
        expect(calls).toMatchObject({ source: 'caller-area:services/gateway', type: 'HTTP_CALLS', count: 2 });
        expect(calls.evidence.map(item => item.target)).toEqual([routes[0], routes[1]]);
        expect(buildSemanticGraph({ ...graph, nodes: [...graph.nodes].reverse() }, { view: 'routes', groupRoutes: true, routeSnapshot })).toEqual(model);
        const filtered = buildSemanticGraph(graph, { view: 'routes', groupRoutes: true, filter: '/accounts', routeSnapshot });
        expect(filtered.nodes.filter(item => item.kind === 'route').map(item => item.label)).toEqual(['/accounts/login/', '/accounts/logout/']);
    });
    it('shortens the shared start of routes that would read alike and keeps the distinguishing part', () => {
        const paths = ['/generic-lastmod/index.xml', '/generic-lastmod/sitemap.xml', '/callable-lastmod-full-sitemap.xml', '/callable-lastmod-partial-sitemap.xml', '/about/'];
        const routes = paths.map((name, index) => ({ ...route, id: 600 + index, name, qualified_name: `fixture.route.${600 + index}`, file_path: 'tests/sitemaps_tests/urls/http.py' }));
        const graph: GraphData = { nodes: routes, edges: [], total_nodes: routes.length };
        const shown = (model: ReturnType<typeof buildSemanticGraph>) => Object.fromEntries(model.nodes.filter(item => item.kind === 'route')
            .map(item => [item.label, item.shortLabel ?? item.label]));
        // "Show these 2 routes" filters by the group prefix: both labels started "/generic-lastmod…" and read alike.
        expect(shown(buildSemanticGraph(graph, { view: 'routes', groupRoutes: true, filter: '/generic-lastmod' })))
            .toEqual({ '/generic-lastmod/index.xml': '…/index.xml', '/generic-lastmod/sitemap.xml': '…/sitemap.xml' });
        // Distinct first segments are not grouped, yet were cut to the same "/callable-lastmo…".
        const grouped = shown(buildSemanticGraph(graph, { view: 'routes', groupRoutes: true }));
        expect(grouped['/callable-lastmod-full-sitemap.xml']).toBe('…full-sitemap.xml');
        expect(grouped['/callable-lastmod-partial-sitemap.xml']).toBe('…partial-sitemap.xml');
        expect(grouped['/about/']).toBe('/about/');
        expect(grouped['/generic-lastmod · 2']).toBe('/generic-lastmod · 2');
    });
    it('shows the differing end of routes that share a long start without an inner separator', () => {
        const short = distinctLabels(['/sitemapindex1.xml', '/sitemapindex2.xml', '/sitemapindex-with-a-tail1', '/sitemapindex-with-a-tail2']);
        // The cut cannot fall on a separator inside "/sitemapindex", so the chip keeps the end that fits and differs.
        expect(short.get('/sitemapindex1.xml')).toBe('…mapindex1.xml');
        expect(short.get('/sitemapindex2.xml')).toBe('…mapindex2.xml');
        // With a separator inside the shared start the cut still falls on it.
        expect(short.get('/sitemapindex-with-a-tail1')).toBe('…tail1');
        for (const label of short.values()) expect(label.length).toBeLessThanOrEqual(14);
    });
    it('selects exactly the routes of a group when the filter names its prefix', () => {
        const at = (id: number, name: string, file_path: string): GraphNode => ({ ...route, id, name, qualified_name: `fixture.route.${id}`, file_path });
        const routes = [at(400, '/', 'app/urls.py'), at(401, '/', 'shop/urls.py'), at(402, '/', 'blog/urls.py'),
            at(403, '/admin/', 'django/contrib/admin/sites.py'), at(404, '/admin/y/', 'django/contrib/admin/sites.py'),
            at(405, '/admin_views/x/', 'tests/urls.py'), at(406, '/login/', 'django/contrib/admin/sites.py'),
            at(407, '/accounts/login/', 'app/urls.py'), at(408, 'accounts/profile/', 'app/urls.py'), at(409, 'GET /accounts/logout/', 'app/urls.py')];
        const handler = node(1, 'django/contrib/admin/sites.py', 'login');
        const graph: GraphData = { nodes: [handler, ...routes], edges: [], total_nodes: 11 };
        const routeSnapshot = { truncated: false, warnings: [], relationships: [routes[3], routes[4], routes[6]].map(source => ({ source, target: handler, type: 'HANDLES' as const })) };
        const options = { view: 'routes' as const, groupRoutes: true, routeSnapshot };
        const grouped = buildSemanticGraph(graph, options);
        // The root path names no shared segment: its routes stay single instead of becoming a group the filter cannot select.
        expect(grouped.nodes.filter(item => item.kind === 'route').map(item => item.label).sort())
            .toEqual(['/', '/', '/', '/accounts · 3', '/admin · 2', '/admin_views/x/', '/login/']);
        const routesFor = (prefix: string) => {
            const model = buildSemanticGraph(graph, { ...options, filter: prefix });
            return model.nodes.filter(item => item.kind === 'route').map(item => item.label).sort();
        };
        for (const group of grouped.nodes.filter(item => item.routePrefix)) {
            expect(routesFor(group.routePrefix!)).toEqual(group.members.map(member => member.name).sort());
            expect(routesFor(group.routePrefix!)).toHaveLength(group.count);
        }
        // Neither '/admin_views' nor a route registered under django/contrib/admin joins the '/admin' group, though its handler area stays as context.
        const admin = buildSemanticGraph(graph, { ...options, filter: '/admin' });
        expect(admin.nodes.map(item => item.label).sort()).toEqual(['/admin/', '/admin/y/', 'django']);
        expect(admin.edges.map(edge => [edge.source, edge.target])).toEqual([['route:Route:fixture.route.403@django/contrib/admin/sites.py', 'handler-area:django'],
            ['route:Route:fixture.route.404@django/contrib/admin/sites.py', 'handler-area:django']]);
        // Text that names no group prefix is still a search over labels, paths and details.
        expect(routesFor('admin')).toEqual(['/admin/', '/admin/y/', '/admin_views/x/', '/login/']);
        expect(routesFor('/')).toHaveLength(10);
        expect(buildSemanticGraph(graph, { view: 'routes', filter: '/admin', routeSnapshot }).nodes.some(item => item.label === '/admin_views/x/')).toBe(true);
    });
    it('keeps routes of a product test package and hides those of test roots', () => {
        const at = (id: number, name: string, file_path: string): GraphNode => ({ ...route, id, name, qualified_name: `fixture.route.${id}`, file_path });
        const graph: GraphData = { nodes: [at(500, '/client/', 'django/test/client.py'), at(501, '/fixture/', 'tests/urls.py'),
            at(502, '/java/', 'src/test/java/RoutesTest.java'), at(503, '/root/', 'test/urls.js'), at(504, '/spec/', 'web/__tests__/routes.ts')], edges: [], total_nodes: 5 };
        const model = buildSemanticGraph(graph, { view: 'routes', hideTestRoutes: true });
        expect(model.nodes.map(item => item.label)).toEqual(['/client/']);
        expect(model.hiddenRoutes).toBe(4);
    });
    it('hides routes whose evidence lies only in test code and keeps test callers of other routes', () => {
        const testCaller = node(2, 'tests/admin_views/tests.py', 'test_login');
        const handler = node(3, 'django/contrib/admin/actions.py', 'delete_selected');
        const testOnly = { ...route, id: 310, name: '/malformed_post/', qualified_name: 'fixture.route.test' };
        const shared = { ...route, id: 311, name: '/{pk}/delete_selected', qualified_name: 'fixture.route.admin' };
        const graph: GraphData = { nodes: [testCaller, handler, testOnly, shared], edges: [], total_nodes: 4 };
        const options = { view: 'routes' as const, routes: [{ method: 'GET', path: '/fixture/', filePath: 'tests/urls.py', line: 3, origin: 'source' as const },
            { method: 'GET', path: '/shop/', filePath: 'shop/urls.py', line: 4, origin: 'source' as const }],
        routeSnapshot: { truncated: false, warnings: [], relationships: [
            { source: testCaller, target: testOnly, type: 'HTTP_CALLS' as const }, { source: testCaller, target: shared, type: 'HTTP_CALLS' as const },
            { source: handler, target: shared, type: 'HANDLES' as const }] } };
        const all = buildSemanticGraph(graph, options);
        expect(all.nodes.filter(item => item.kind === 'route')).toHaveLength(4);
        expect(all.hiddenRoutes).toBe(0);
        const shown = buildSemanticGraph(graph, { ...options, hideTestRoutes: true });
        expect(shown.nodes.filter(item => item.kind === 'route').map(item => item.label).sort()).toEqual(['/{pk}/delete_selected', 'GET /shop/']);
        expect(shown.hiddenRoutes).toBe(2);
        expect(shown.edges.map(edge => [edge.source, edge.type])).toEqual([['caller-area:tests', 'HTTP_CALLS'], ['handler-area:django', 'HANDLES']]);
        expect(buildSemanticGraph(graph, { view: 'overview' }).hiddenRoutes).toBeUndefined();
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

it('shows percent-encoded route paths as people read them and keeps reserved characters encoded', () => {
    expect(readableRoute('/%C3%A9dit/')).toBe('/\u00e9dit/');
    expect(readableRoute('/files/a%2Fb')).toBe('/files/a%2Fb');
    expect(readableRoute('/broken/%E0%A4%A')).toBe('/broken/%E0%A4%A');
    expect(readableRoute('/plain/')).toBe('/plain/');
});
