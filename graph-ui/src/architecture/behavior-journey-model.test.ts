import { describe, expect, it } from 'vitest';
import { behaviorJourney } from './behavior-journey-model';
import type { SystemBehavior, SystemPath, SystemPathEdge, SystemProjection, SystemSymbol } from './system-architecture-source';

const symbol = (id: number, component = 'app'): SystemSymbol => ({ id, name: `f${id}`, qualified_name: `app.f${id}`,
    label: 'Function', component_id: component, file_path: `src/${component}.ts`, start_line: id + 1 });
const edge = (id: number, source: number, target: number, type = 'CALLS'): SystemPathEdge => ({ id, source_id: source, target_id: target, type });
const path = (ids: number[], base = 100): SystemPath => ({ entrypoint_id: ids[0], nodes: ids.map(id => symbol(id)),
    edges: ids.slice(1).map((id, index) => edge(base + index, ids[index], id)) });
const projection = (changes: Partial<SystemProjection> = {}): SystemProjection => ({ schema_version: 1, status: 'ready',
    kind: 'static_projection', complete: true, components: [], dependencies: [], cycles: [], entrypoints: [symbol(1)],
    paths: [], totals: {}, limits: {}, warnings: [], ...changes });
const behavior = (nodes: SystemSymbol[], edges: SystemPathEdge[], changes: Partial<SystemBehavior> = {}): SystemBehavior => ({
    mode: 'targets', source_id: 1, complete: true, limits_hit: [], reachable_targets: [], totals: {}, nodes, edges,
    cycles: [], max_hops: 20, ...changes,
});
const drawnPairs = (data: ReturnType<typeof behaviorJourney>) => data.scene.edges.map(item => {
    const nodes = new Map(data.scene.nodes.map(node => [node.id, node.symbol!.id]));
    return [nodes.get(item.source), nodes.get(item.target)];
});

describe('focused Behavior journeys', () => {
    it('shows direct branches at the same depth instead of connecting siblings in catalog order', () => {
        const data = projection({ behavior: behavior([symbol(1), symbol(2), symbol(3)], [edge(10, 1, 2), edge(11, 1, 3)], {
            reachable_targets: [{ ...symbol(3), distance: 1 }, { ...symbol(2), distance: 1 }, { ...symbol(9), distance: 4 }],
        }) });
        const result = behaviorJourney(data);
        expect(result.mode).toBe('choices');
        expect(drawnPairs(result)).toEqual([[1, 2], [1, 3]]);
        expect(result.scene.nodes.slice(1).map(node => node.depth)).toEqual([1, 1]);
        expect(result.scene.nodes.slice(1).map(node => node.position[0])).toEqual([88, 88]);
        expect(result.scene.nodes.map(node => node.symbol!.id)).not.toContain(9);
        expect(result.choices.find(choice => choice.id === 9)).toMatchObject({ direct: false, distance: 4 });
        expect(result.scene.focusId).toBeUndefined();
    });

    it('uses exact-source witnesses and valid first hops but never aggregate counts or references', () => {
        const data = projection({ paths: [path([1, 2, 8])], dependencies: [
            { source: 'app', target: 'other', type: 'CALLS', count: 500, witnesses: [] },
            { source: 'app', target: 'other', type: 'CALLS', count: 2, witnesses: [
                { edge_id: 20, source: symbol(1), target: symbol(3), callsite: { file_path: 'src/app.ts', line: 8 } },
                { edge_id: 21, source: symbol(99), target: symbol(4) },
            ] },
            { source: 'app', target: 'other', type: 'CALL_REFERENCE', count: 1, witnesses: [{ edge_id: 22, source: symbol(1), target: symbol(5) }] },
            { source: 'app', target: 'other', type: 'IMPORTS', count: 1, witnesses: [{ edge_id: 23, source: symbol(1), target: symbol(6) }] },
        ] });
        const result = behaviorJourney(data);
        expect(drawnPairs(result)).toEqual([[1, 2], [1, 3]]);
        expect(result.scene.edges[1].pathEdge?.callsite).toEqual({ file_path: 'src/app.ts', line: 8 });
        expect(result.scene.nodes.map(node => node.symbol!.id)).toEqual([1, 2, 3]);
    });

    it('admits only exact-source invocation witnesses from overview connections and keeps the fan stable', () => {
        const connections = [
            { source: 'aggregate-a', target: 'aggregate-b', type: 'ASYNC_CALLS', count: 50, witnesses: [
                { edge_id: 20, source: symbol(1), target: symbol(3, 'worker') },
                { edge_id: 21, source: symbol(7), target: symbol(8, 'worker') },
            ] },
            { source: 'aggregate-a', target: 'aggregate-c', type: 'CALLS', count: 20, witnesses: [
                { edge_id: 22, source: symbol(1), target: symbol(2, 'store') },
            ] },
        ];
        const data = projection({ overview: { complete: false, grouping_basis: 'common_source_directory_aggregate',
            groups: [], components: [], connections, totals: {}, limits: {} } });
        const result = behaviorJourney(data);
        expect(result.scene.nodes.map(node => node.symbol!.id)).toEqual([1, 2, 3]);
        expect(result.scene.edges.map(item => item.type)).toEqual(['ASYNC_CALLS', 'CALLS']);
        expect(result.scene.nodes.some(node => node.symbol!.id === 8)).toBe(false);
        expect(result.limits.sampled).toBe(true);
        const reordered = behaviorJourney({ ...data, overview: { ...data.overview!, connections: [...connections].reverse() } });
        expect(reordered.scene.nodes).toEqual(result.scene.nodes);
        expect(reordered.scene.scopeKey).toBe(result.scene.scopeKey);
    });

    it('selects one complete path for the exact entry and target while preserving input order', () => {
        const first = path([1, 2, 5]), second = path([1, 3, 5], 200);
        second.nodes[1] = symbol(3, 'store');
        const data = projection({ paths: [first, path([7, 3, 5]), path([1, 3, 6]), second],
            components: [{ id: 'store', label: 'Storage', basis: 'declared_module', member_count: 10, file_count: 2, representatives: [] }] });
        const result = behaviorJourney(data, { entryId: 1, targetId: 5, pathIndex: 1 });
        expect(result.paths).toEqual([first, second]); expect(result.path).toBe(second); expect(result.mode).toBe('path');
        expect(drawnPairs(result)).toEqual([[1, 3], [3, 5]]);
        expect(result.scene.nodes.map(node => node.depth)).toEqual([0, 1, 2]);
        expect(result.scene.nodes.map(node => node.label)).toEqual(['1. f1', '2. f3', '3. f5']);
        expect(result.scene.nodes.map(node => node.position[0])).toEqual([0, 72, 144]);
        expect(result.scene.nodes.every(node => Math.abs(node.position[2]) < 2)).toBe(true);
        expect(result.scene.lanes.map(lane => lane.label)).toContain('Storage');
        expect(result.scene.edges.map(item => item.pathEdge)).toEqual(second.edges);
        expect(result.scene.focusId).toBeUndefined();
    });

    it('rejects jumps, missing hops, mismatched entry IDs and non-invocation path edges', () => {
        const jump = path([1, 2, 3]); jump.edges[1] = edge(101, 9, 3);
        const missing = path([1, 3]); missing.edges = [];
        const wrongEntry = path([2, 3]); wrongEntry.entrypoint_id = 1;
        const reference = path([1, 3]); reference.edges[0].type = 'CALL_REFERENCE';
        const result = behaviorJourney(projection({ paths: [jump, missing, wrongEntry, reference] }), { entryId: 1, targetId: 3 });
        expect(result.mode).toBe('empty'); expect(result.path).toBeUndefined();
        expect(result.scene.edges).toEqual([]); expect(result.counts.invalidPaths).toBe(4);
    });

    it('finds a shortest corridor path with stable evidence-order ties and terminates on cycles', () => {
        const edges = [edge(1, 1, 2), edge(2, 1, 3), edge(3, 2, 1), edge(4, 2, 5), edge(5, 3, 5)];
        const data = projection({ behavior: behavior([1, 2, 3, 5].map(id => symbol(id)), edges, { mode: 'corridor', target_id: 5 }) });
        const result = behaviorJourney(data, { entryId: 1, targetId: 5 });
        expect(result.mode).toBe('corridor'); expect(drawnPairs(result)).toEqual([[1, 2], [2, 5]]);
        expect(result.path?.edges.map(item => item.id)).toEqual([1, 4]);
        expect(behaviorJourney({ ...data, behavior: { ...data.behavior!, edges: [...edges].reverse() } }, { targetId: 5 }).path).toEqual(result.path);
        expect(behaviorJourney(data, { entryId: 1, targetId: 99 }).mode).toBe('empty');
        expect(behaviorJourney(data, { entryId: 2, targetId: 5 }).mode).toBe('empty');
    });

    it('does not bridge two witnesses that merely share a component or a catalog distance', () => {
        const data = projection({ behavior: behavior([1, 2, 3, 4].map(id => symbol(id)), [edge(1, 1, 2), edge(2, 3, 4)], {
            mode: 'corridor', target_id: 4, reachable_targets: [{ ...symbol(4), distance: 2 }],
        }) });
        const result = behaviorJourney(data, { targetId: 4 });
        expect(result.mode).toBe('empty'); expect(result.paths).toEqual([]); expect(result.scene.edges).toEqual([]);
    });

    it('keeps separate numbered occurrences when an explicit recorded path revisits a symbol', () => {
        const result = behaviorJourney(projection({ paths: [path([1, 2, 1, 3])] }), { targetId: 3 });
        expect(result.scene.nodes.map(node => node.symbol!.id)).toEqual([1, 2, 1, 3]);
        expect(new Set(result.scene.nodes.map(node => node.id)).size).toBe(4);
        expect(drawnPairs(result)).toEqual([[1, 2], [2, 1], [1, 3]]);
    });

    it('filters complete paths while retaining every connecting hop', () => {
        const result = behaviorJourney(projection({ paths: [path([1, 2, 5]), path([1, 3, 5], 200)] }), { targetId: 5, filter: 'f2' });
        expect(result.paths).toHaveLength(1); expect(result.counts.filteredPaths).toBe(1);
        expect(result.scene.nodes.map(node => node.symbol!.id)).toEqual([1, 2, 5]);
        expect(drawnPairs(result)).toEqual([[1, 2], [2, 5]]);
        expect(behaviorJourney(projection({ paths: [path([1, 2, 5])] }), { targetId: 5, filter: 'absent' }).mode).toBe('empty');
    });

    it('bounds a direct fan while reserving a real edge for every displayed callee', () => {
        const targets = Array.from({ length: 20 }, (_, index) => ({ ...symbol(index + 2), name: `target${String(index).padStart(2, '0')}` }));
        const edges = [...Array.from({ length: 130 }, (_, index) => edge(index, 1, 2)),
            ...targets.slice(1).map((target, index) => edge(200 + index, 1, target.id))];
        const data = projection({ behavior: behavior([symbol(1), ...targets], edges, {
            complete: false, limits_hit: ['server-node-cap'], reachable_targets: targets.map(target => ({ ...target, distance: 1 })),
        }) });
        const result = behaviorJourney(data);
        expect(result.scene.nodes).toHaveLength(13); expect(result.scene.edges).toHaveLength(120);
        expect(result.choices.filter(choice => choice.direct)).toHaveLength(12);
        const connected = new Set(result.scene.edges.map(item => item.target));
        expect(result.scene.nodes.slice(1).every(node => connected.has(node.id))).toBe(true);
        expect(result.counts.omittedNodes).toBe(8); expect(result.counts.omittedChoices).toBe(8);
        expect(result.counts.omittedEdges).toBe(edges.length - 120);
        expect(result.limits.sampled).toBe(true);
        expect(result.limits.hit).toEqual(expect.arrayContaining(['server-node-cap', 'direct-choices', 'direct-edges']));
    });

    it('omits an oversized whole path instead of truncating it into a claim that reaches the target', () => {
        const large = path(Array.from({ length: 61 }, (_, index) => index + 1));
        const result = behaviorJourney(projection({ paths: [large] }), { targetId: 61 });
        expect(result.mode).toBe('empty'); expect(result.path).toBeUndefined(); expect(result.scene.nodes).toEqual([]);
        expect(result.counts.omittedNodes).toBe(61); expect(result.counts.omittedEdges).toBe(60);
        expect(result.limits.hit).toContain('whole-path-size');
    });

    it('caps corridor exploration without deriving shortcuts from unreachable catalog rows', () => {
        const nodes = Array.from({ length: 70 }, (_, index) => symbol(index + 1));
        const edges = nodes.slice(1).map((target, index) => edge(index, nodes[index].id, target.id));
        const result = behaviorJourney(projection({ behavior: behavior(nodes, edges, { mode: 'corridor', target_id: 70 }) }), { targetId: 70 });
        expect(result.mode).toBe('empty'); expect(result.scene.nodes.length).toBeLessThanOrEqual(60);
        expect(result.limits.hit).toContain('corridor-node-search'); expect(result.limits.sampled).toBe(true);
    });
});
