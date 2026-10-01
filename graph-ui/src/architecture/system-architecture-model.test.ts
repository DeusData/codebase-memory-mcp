import { describe, expect, it } from 'vitest';
import { isContiguousPath, rankSystemPaths, systemBehaviorGraph, systemComponentGraph, systemOverviewGraph, systemBehaviorOverviewGraph } from './system-architecture-model';
import type { SystemComponent, SystemDependency, SystemPath, SystemProjection, SystemSymbol } from './system-architecture-source';

const symbol = (id: number, component = 'app'): SystemSymbol => ({ id, name: `f${id}`, qualified_name: `app.f${id}`, label: 'Function', component_id: component, file_path: `src/${component}.ts`, start_line: id });
const component = (id: string): SystemComponent => ({ id, label: id, basis: 'declared_module', member_count: 5, file_count: 1, representatives: [] });
const dependency = (source: string, target: string, type = 'CALLS'): SystemDependency => ({ source, target, type, count: 1, witnesses: [] });
const projection = (components: SystemComponent[], dependencies: SystemDependency[] = []): SystemProjection => ({
    schema_version: 1, status: 'ready', kind: 'static_projection', complete: true, components, dependencies,
    cycles: [], entrypoints: [], paths: [], totals: {}, limits: {}, warnings: [],
});
const path = (nodes: SystemSymbol[], edgeBase = 100): SystemPath => ({ entrypoint_id: nodes[0].id, nodes,
    edges: nodes.slice(1).map((node, index) => ({ id: edgeBase + index, source_id: nodes[index].id, target_id: node.id, type: 'CALLS' })),
});

const overviewProjection = (ids: string[], dependencies: SystemDependency[]): SystemProjection => {
    const components = ids.map(id => ({ ...component(`component-${id}`), group_id: id }));
    return { ...projection(components), overview: { complete: true, grouping_basis: 'common_source_directory_aggregate',
        groups: ids.map(id => ({ id, label: `src/${id}`, role: 'non_test', component_count: 1, member_count: 5,
            file_count: 1, component_ids: [`component-${id}`], representatives: [] })),
        components, connections: dependencies, totals: { groups: ids.length, components: ids.length }, limits: { omitted_connections: 0 } } };
};

describe('persistent system overview', () => {
    it('keeps directory siblings on a shared platform without assigning execution order', () => {
        const data = overviewProjection(['pkg/a', 'src/b', 'tools', 'src/a', 'pkg/b'], [dependency('pkg/a', 'src/a')]);
        data.overview!.groups.forEach(group => { group.label = group.id; });
        const model = systemOverviewGraph(data);
        expect(model.lanes.map(lane => lane.label)).toEqual(['pkg', 'src', 'tools']);
        for (const node of model.nodes) {
            const lane = model.lanes.find(item => item.label === node.label.split('/')[0])!;
            expect(Math.abs(node.position[0] - lane.position[0]) + node.size![0] / 2).toBeLessThan(lane.width / 2);
            expect(Math.abs(node.position[1] - lane.position[1]) + node.size![1] / 2).toBeLessThan(lane.height! / 2);
        }
        const changedEdges = systemOverviewGraph({ ...data, overview: { ...data.overview!, connections: [dependency('src/a', 'pkg/b')] } });
        expect(changedEdges.nodes.map(node => node.position)).toEqual(model.nodes.map(node => node.position));
        expect(changedEdges.scopeKey).toBe(model.scopeKey);
        expect(changedEdges.edges.map(edge => [edge.source, edge.target])).toEqual([['src/a', 'pkg/b']]);
    });
    it('orders by source paths rather than opaque group IDs and separates missing locations', () => {
        const data = overviewProjection(['opaque-z', 'opaque-a', 'opaque-q', 'opaque-r'], []);
        data.overview!.groups.forEach((group, index) => { group.label = ['src/api', 'src/store', '(root)', '@no-source'][index]; });
        const base = systemOverviewGraph(data);
        expect(base.lanes.map(lane => lane.label)).toEqual(['Repository root', 'src/api', 'src/store', 'Location unavailable']);
        const renamed = systemOverviewGraph({ ...data, overview: { ...data.overview!, groups: [...data.overview!.groups].reverse().map(group => ({ ...group, id: `new-${group.label}` })) } });
        for (const node of base.nodes) expect(renamed.nodes.find(item => item.label === node.label)?.position).toEqual(node.position);
        expect(new Set(base.nodes.map(node => node.position.join(','))).size).toBe(4);
    });
    it('uses common source directories as layout hints when no aggregate directory is reported', () => {
        const data = projection([{ ...component('opaque'), representatives: [
            { ...symbol(1), file_path: 'client/api/start.ts' }, { ...symbol(2), file_path: 'client/api/end.ts' },
        ] }, component('unknown')]);
        const model = systemOverviewGraph(data);
        expect(model.lanes.map(lane => lane.label)).toEqual(['client/api', 'Location unavailable']);
        expect(model.nodes[0].component?.basis).toBe('declared_module');
    });
    it('represents every returned group and preserves all typed directed relationships, including internal aggregates', () => {
        const dependencies = [dependency('a', 'b'), { ...dependency('a', 'b', 'IMPORTS'), count: 3 },
            dependency('b', 'c'), dependency('c', 'a'), { ...dependency('a', 'a', 'USAGE'), count: 7 }];
        const model = systemOverviewGraph(overviewProjection(['a', 'b', 'c', 'isolated'], dependencies));
        expect(model.nodes.map(node => node.id).sort()).toEqual(['a', 'b', 'c', 'isolated']);
        expect(model.edges).toHaveLength(4);
        expect(model.edges.flatMap(edge => edge.dependencies ?? [])).toEqual(expect.arrayContaining(dependencies));
        expect(model.edges.reduce((sum, edge) => sum + edge.count, 0)).toBe(13);
        expect(model.edges.filter(edge => edge.cycle)).toHaveLength(3);
        expect(model.edges.find(edge => edge.source === edge.target)?.cycle).toBe(false);
        expect(model.omittedNodes).toBe(0); expect(model.omittedEdges).toBe(0);
    });
    it('keeps coordinates and layout identity stable across data order, selection scopes and relationship filters', () => {
        const data = overviewProjection(['a', 'b', 'c'], [dependency('a', 'b'), dependency('b', 'c', 'IMPORTS')]);
        const base = systemOverviewGraph(data);
        const reversed = { ...data, overview: { ...data.overview!, groups: [...data.overview!.groups].reverse(), connections: [...data.overview!.connections].reverse() } };
        const focused = systemOverviewGraph(reversed, { focusId: 'b', relationshipTypes: ['CALLS'] });
        expect(focused.scopeKey).toBe(base.scopeKey);
        for (const node of focused.nodes) expect(node.position).toEqual(base.nodes.find(other => other.id === node.id)!.position);
        const filtered = systemOverviewGraph(data, { filter: 'src/b' });
        expect(filtered.nodes).toHaveLength(1); expect(filtered.nodes[0].position).toEqual(base.nodes.find(node => node.id === 'b')!.position);
    });
    it('expands within a fixed boundary and retains full membership in an inspectable remainder', () => {
        const data = overviewProjection(['a', 'b'], [dependency('a', 'b')]);
        const members = Array.from({ length: 20 }, (_, index) => ({ ...component(`member-${index.toString().padStart(2, '0')}`), group_id: 'a' }));
        data.overview!.components = [...members, data.overview!.components[1]];
        data.overview!.groups[0] = { ...data.overview!.groups[0], component_count: members.length, component_ids: members.map(item => item.id) };
        const base = systemOverviewGraph(data), expanded = systemOverviewGraph(data, { expandedGroupIds: ['a'], focusId: 'member-19' });
        expect(expanded.nodes.find(node => node.id === 'b')!.position).toEqual(base.nodes.find(node => node.id === 'b')!.position);
        expect(expanded.nodes.find(node => node.id === 'a')!.size).toEqual(base.nodes.find(node => node.id === 'a')!.size);
        expect(expanded.edges.map(edge => [edge.source, edge.target])).toEqual([['a', 'b']]);
        const children = expanded.nodes.filter(node => node.parentId === 'a');
        expect(children).toHaveLength(12);
        expect(new Set(children.flatMap(node => node.componentIds ?? []))).toEqual(new Set(members.map(item => item.id)));
        expect(children.find(node => node.id === expanded.focusId)?.component?.id).toBe('member-19');
        expect(children.find(node => node.kind === 'remainder')?.componentIds).toHaveLength(9);
    });
    it('does not derive a visible cycle through hidden test groups or excluded relationship types', () => {
        const data = overviewProjection(['a', 'test'], [dependency('a', 'test'), dependency('test', 'a', 'IMPORTS')]);
        data.overview!.groups[1].role = 'test';
        expect(systemOverviewGraph(data, { cyclesOnly: true }).nodes).toHaveLength(0);
        expect(systemOverviewGraph(data, { cyclesOnly: true, includeTests: true, relationshipTypes: ['CALLS'] }).nodes).toHaveLength(0);
        expect(systemOverviewGraph(data, { cyclesOnly: true, includeTests: true }).edges).toHaveLength(2);
    });
    it('leaves the overview unhighlighted while choosing a target', () => {
        const data = overviewProjection(['a', 'b'], [dependency('a', 'b')]);
        data.behavior = { mode: 'targets', source_id: 1, complete: true, limits_hit: [], reachable_targets: [], totals: {}, nodes: [], edges: [], cycles: [], max_hops: 8 };
        const base = systemOverviewGraph(data);
        expect(systemBehaviorOverviewGraph(data, base, [path([symbol(1), symbol(2)])], 0)).toBe(base);
    });
    it('preserves corridor joins, cycle edges and exact provenance while reusing all overview coordinates', () => {
        const data = overviewProjection(['a', 'b', 'c', 'd'], [dependency('a', 'b'), dependency('a', 'c'), dependency('b', 'd'), dependency('c', 'd'), dependency('d', 'b')]);
        const symbols = ['a', 'b', 'c', 'd'].map((id, index) => ({ ...symbol(index + 1, `component-${id}`), group_id: id }));
        const edge = (id: number, source: number, target: number) => ({ id, source_id: source, target_id: target, type: 'CALLS', callsite: { file_path: 'src/a.c', line: id } });
        const evidence = [edge(10, 1, 2), edge(11, 1, 3), edge(12, 2, 4), edge(13, 3, 4), edge(14, 4, 2)];
        data.behavior = { mode: 'corridor', source_id: 1, target_id: 4, complete: true, limits_hit: [], reachable_targets: [], totals: {}, nodes: symbols, edges: evidence, cycles: [{ node_ids: [2, 4] }], max_hops: 8 };
        const paths = [{ entrypoint_id: 1, nodes: [symbols[0], symbols[1], symbols[3]], edges: [evidence[0], evidence[2]] },
            { entrypoint_id: 1, nodes: [symbols[0], symbols[2], symbols[3]], edges: [evidence[1], evidence[3]] }];
        const base = systemOverviewGraph(data), first = systemBehaviorOverviewGraph(data, base, paths, 0), second = systemBehaviorOverviewGraph(data, base, paths, 1);
        expect(first.nodes.map(node => [node.id, node.position])).toEqual(base.nodes.map(node => [node.id, node.position]));
        expect(first.scopeKey).toBe(base.scopeKey); expect(second.scopeKey).toBe(base.scopeKey);
        expect(second.edges.map(item => item.id)).toEqual(first.edges.map(item => item.id));
        expect(first.edges.flatMap(item => item.behaviorEdges ?? []).sort((a, b) => a.id - b.id)).toEqual(evidence);
        expect(first.nodes.find(node => node.id === 'd')?.pathIndices).toEqual([0, 1]);
        expect(first.edges.find(item => item.source === 'd' && item.target === 'b')?.behaviorEdges).toEqual([evidence[4]]);
        expect(first.nodes).toHaveLength(4);
    });
});

describe('focused component neighborhoods', () => {
    it('draws only anchor-touching dependencies and keeps all underlying typed evidence', () => {
        const first = dependency('client', 'service'), otherType = dependency('client', 'service', 'IMPORTS');
        first.witnesses = [{ edge_id: 42, source: symbol(1, 'client'), target: symbol(2, 'service') }];
        const data = projection(['client', 'service', 'store'].map(component), [first, otherType,
            dependency('service', 'store'), dependency('client', 'store')]);
        const model = systemComponentGraph(data, '', false, false, false, 'service');
        expect(model.focusId).toBe('service');
        expect(model.edges.every(edge => edge.source === 'service' || edge.target === 'service')).toBe(true);
        expect(model.edges.map(edge => edge.type)).toEqual(['CALLS', 'IMPORTS', 'CALLS']);
        expect(model.edges[0].dependency?.witnesses).toBe(first.witnesses);
        expect(model.omittedEdges).toBe(1);
    });
    it('places callers before the anchor and callees after it', () => {
        const data = projection(['client', 'service', 'store', 'peer'].map(component), [dependency('client', 'service'),
            dependency('service', 'store'), dependency('service', 'peer'), dependency('peer', 'service')]);
        const model = systemComponentGraph(data, '', false, false, false, 'service');
        const nodes = new Map(model.nodes.map(node => [node.id, node]));
        expect(nodes.get('client')!.position[0]).toBeLessThan(nodes.get('service')!.position[0]);
        expect(nodes.get('store')!.position[0]).toBeGreaterThan(nodes.get('service')!.position[0]);
        expect(nodes.get('peer')!.position[1]).toBeGreaterThan(nodes.get('service')!.position[1]);
        expect(model.edges.filter(edge => edge.source === 'peer' || edge.target === 'peer')).toHaveLength(2);
    });
    it('bounds the neighborhood while accounting for the remaining components and neighbors', () => {
        const data = projection(Array.from({ length: 31 }, (_, index) => component(`c${index}`)),
            Array.from({ length: 30 }, (_, index) => dependency('c0', `c${index + 1}`)));
        const model = systemComponentGraph(data, '', false);
        expect(model.focusId).toBe('c0'); expect(model.nodes).toHaveLength(8); expect(model.edges).toHaveLength(7);
        expect(model.omittedNodes).toBe(23); expect(model.omittedNeighbors).toBe(23);
        expect(model.nodes.length + model.omittedNodes).toBe(data.components.length);
        expect(model.nodes.slice(1).every(node => node.position[0] >= 60)).toBe(true);
        expect(model.captions!.every(caption => caption.position[1] >= Math.max(...model.nodes.map(node => node.position[1])) + 17)).toBe(true);
    });
    it('retains both relationship directions without exceeding the total neighborhood cap', () => {
        const ids = Array.from({ length: 20 }, (_, index) => `c${index}`);
        const data = projection(['focus', ...ids].map(component), ids.map((id, index) => index < 10
            ? dependency('focus', id) : dependency(id, 'focus')));
        const model = systemComponentGraph(data, '', false, false, false, 'focus');
        expect(model.nodes).toHaveLength(14);
        expect(model.nodes.filter(node => node.position[0] < 0).length).toBeGreaterThanOrEqual(6);
        expect(model.nodes.filter(node => node.position[0] > 0).length).toBeGreaterThanOrEqual(6);
        expect(model.omittedNeighbors).toBe(7);
        expect(model.captions?.map(caption => caption.label)).toEqual(['Used by', 'Focus', 'Uses']);
    });
    it('prioritizes strong direct focal relationships over globally popular weak neighbors', () => {
        const local = Array.from({ length: 7 }, (_, index) => `local${index}`);
        const hubs = Array.from({ length: 8 }, (_, index) => `hub${index}`);
        const satellites = Array.from({ length: 8 }, (_, index) => `satellite${index}`);
        const data = projection(['focus', ...local, ...hubs, ...satellites].map(component), [
            ...local.map(id => ({ ...dependency('focus', id), count: id === 'local0' ? 1 : 2 })),
            { ...dependency('focus', 'local0', 'IMPORTS'), count: 2 },
            ...hubs.map(id => dependency('focus', id)),
            ...hubs.flatMap(hub => satellites.map(satellite => ({ ...dependency(hub, satellite), count: 50 }))),
        ]);
        const model = systemComponentGraph(data, '', false, false, false, 'focus');
        expect(model.nodes.slice(1).map(node => node.id)).toEqual(local);
        expect(model.nodes.slice(1).every(node => node.position[0] > 0)).toBe(true);
        expect(model.edges.filter(edge => edge.target === 'local0').map(edge => [edge.type, edge.count])).toEqual([['CALLS', 1], ['IMPORTS', 2]]);
        expect(model.omittedNeighbors).toBe(8);
        expect(model.nodes.length + model.omittedNodes).toBe(data.components.length);
    });
    it('allows any included isolated component to become the new focus without unrelated edges', () => {
        const data = projection(['client', 'service', 'isolated'].map(component), [dependency('client', 'service')]);
        const before = systemComponentGraph(data, '', false, true);
        const after = systemComponentGraph(data, '', false, true, false, 'isolated');
        expect(after.focusId).toBe('isolated'); expect(after.nodes.map(node => node.id)).toEqual(['isolated']);
        expect(after.edges).toHaveLength(0); expect(after.omittedNodes).toBe(2);
        expect(after.scopeKey).not.toBe(before.scopeKey);
    });
});

describe('behavior prefix trees', () => {
    it('shares common prefixes while retaining path membership and actual handoffs', () => {
        const first = symbol(1, 'api'), second = symbol(2, 'service');
        const left = path([first, second, symbol(3, 'store')]);
        const right = path([first, second, symbol(4, 'queue')]); right.edges[1].id = 102;
        const model = systemBehaviorGraph(projection([]), [left, right]);
        expect(model.nodes).toHaveLength(4); expect(model.edges).toHaveLength(3);
        expect(model.nodes.find(node => node.symbol?.id === 2)?.pathIndices).toEqual([0, 1]);
        expect(model.nodes.find(node => node.symbol?.id === 3)?.pathIndices).toEqual([0]);
        expect(model.nodes.find(node => node.symbol?.id === 4)?.pathIndices).toEqual([1]);
        expect(model.edges.every(edge => edge.handoff)).toBe(true);
        expect(model.nodes.find(node => node.symbol?.id === 3)?.position[1]).not.toBe(model.nodes.find(node => node.symbol?.id === 4)?.position[1]);
    });
    it('never reconverges separate branches through a shared symbol into a synthetic path', () => {
        const first = symbol(1), last = symbol(4);
        const paths = [path([first, symbol(2), last], 100), path([first, symbol(3), last], 200)];
        const model = systemBehaviorGraph(projection([]), paths);
        const endings = model.nodes.filter(node => node.symbol?.id === 4);
        expect(endings).toHaveLength(2); expect(endings[0].id).not.toBe(endings[1].id);
        for (const [index, original] of paths.entries()) {
            const edges = model.edges.filter(edge => edge.pathIndices?.includes(index));
            expect(edges.map(edge => edge.pathEdge)).toEqual(original.edges);
            expect(edges[0].target).toBe(edges[1].source);
        }
    });
    it('preserves relationship type distinctions instead of merging different evidence', () => {
        const first = path([symbol(1), symbol(2)]);
        const second = path([symbol(1), symbol(2)]); second.edges[0].type = 'ASYNC_CALLS';
        const model = systemBehaviorGraph(projection([]), [first, second]);
        expect(model.nodes).toHaveLength(3);
        expect(model.edges.map(edge => edge.type)).toEqual(['CALLS', 'ASYNC_CALLS']);
        expect(model.edges.map(edge => edge.pathIndices)).toEqual([[0], [1]]);
    });
    it('prefers a longer component-crossing path without changing the input indices', () => {
        const paths = [path([symbol(1), symbol(2)]), path([symbol(1), symbol(3, 'service'), symbol(4, 'store')], 200)];
        expect(rankSystemPaths(paths)).toEqual([1, 0]);
        const model = systemBehaviorGraph(projection([]), paths);
        expect(model.preferredPathIndex).toBe(1);
        expect(model.nodes.find(node => node.symbol?.id === 4)?.pathIndices).toEqual([1]);
    });
    it('keeps only one entrypoint and rejects disconnected input paths', () => {
        const valid = path([symbol(1), symbol(2)]), disconnected = path([symbol(1), symbol(3)]);
        disconnected.edges[0].source_id = 88;
        const model = systemBehaviorGraph(projection([]), [disconnected, valid, path([symbol(9), symbol(10)])]);
        expect(isContiguousPath(disconnected)).toBe(false);
        expect(model.nodes.map(node => node.symbol?.id)).toEqual([1, 2]);
        expect(model.nodes.every(node => node.pathIndices?.join() === '1')).toBe(true);
    });
    it('caps visible prefixes while retaining their parents and reporting every omission', () => {
        const paths = Array.from({ length: 20 }, (_, index) => path([symbol(1), ...Array.from({ length: 4 }, (_, depth) => symbol(index * 10 + depth + 2))], index * 10));
        const model = systemBehaviorGraph(projection([]), paths);
        expect(model.nodes).toHaveLength(36); expect(model.omittedNodes).toBe(45);
        expect(model.edges).toHaveLength(35); expect(model.omittedEdges).toBe(45);
        const ids = new Set(model.nodes.map(node => node.id));
        expect(model.edges.every(edge => ids.has(edge.source) && ids.has(edge.target))).toBe(true);
        expect(model.nodes.filter(node => node.id !== model.nodes[0].id).every(node => model.edges.some(edge => edge.target === node.id))).toBe(true);
    });
    it('brings a previously omitted selected branch into view without reassigning prefix identities', () => {
        const paths = Array.from({ length: 20 }, (_, index) => path([symbol(1), ...Array.from({ length: 4 }, (_, depth) => symbol(index * 10 + depth + 2))], index * 10));
        const initial = systemBehaviorGraph(projection([]), paths);
        const selected = systemBehaviorGraph(projection([]), paths, 19);
        expect(initial.nodes.some(node => node.symbol?.id === paths[19].nodes.at(-1)?.id)).toBe(false);
        expect(selected.nodes.filter(node => node.pathIndices?.includes(19)).map(node => node.symbol?.id)).toEqual(paths[19].nodes.map(node => node.id));
        expect(selected.nodes).toHaveLength(36);
        for (const node of selected.nodes) {
            const previous = initial.nodes.find(candidate => candidate.id === node.id);
            if (previous) expect(previous.symbol?.id).toBe(node.symbol?.id);
        }
        expect(selected.scopeKey).not.toBe(initial.scopeKey);
    });
    it('keeps positions and fit scope stable when changing highlights inside an uncapped tree', () => {
        const paths = [path([symbol(1), symbol(2)], 10), path([symbol(1), symbol(3)], 20)];
        const first = systemBehaviorGraph(projection([]), paths, 0), second = systemBehaviorGraph(projection([]), paths, 1);
        expect(first.scopeKey).toBe(second.scopeKey);
        expect(first.nodes.map(node => [node.id, node.position])).toEqual(second.nodes.map(node => [node.id, node.position]));
    });
});
