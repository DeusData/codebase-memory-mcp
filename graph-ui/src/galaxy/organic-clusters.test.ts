import { describe, expect, it, vi } from 'vitest';
import { layoutOrganicClusters } from './organic-clusters';
import type { GraphData, GraphEdge, GraphNode } from './types';

const node = (id: number, file = 'src/shared.ts'): GraphNode => ({ id, x: 100, y: 200, z: 300,
    name: `n${id}`, qualified_name: `sample.n${id}`, file_path: file, label: 'Function',
    size: 2, color: id % 2 ? '#74899a' : '#aa987f' });
const edge = (source: number, target: number, type = 'CALLS'): GraphEdge => ({ source, target, type });
function cliques(): GraphData {
    const edges: GraphEdge[] = [];
    for (const offset of [0, 5]) for (let i = offset; i < offset + 5; i++) {
        for (let j = i + 1; j < offset + 5; j++) edges.push(edge(i, j));
    }
    edges.push(edge(4, 5));
    return { nodes: Array.from({ length: 11 }, (_, id) => node(id)), edges, total_nodes: 11 };
}
const positions = (data: GraphData) => [...data.nodes].sort((a, b) => a.id - b.id)
    .map(({ id, x, y, z }) => ({ id, x, y, z }));

describe('organic relationship clusters', () => {
    it('reuses warm geometry without force work while taking colors and source metadata from the fresh graph', () => {
        const source = cliques();
        const initial = layoutOrganicClusters(source, { rootIds: new Set([0]) });
        initial.groups[0]!.label = 'caller mutation must not contaminate the cache';
        const fresh: GraphData = { ...source, total_nodes: 100,
            nodes: source.nodes.map(candidate => ({ ...candidate, name: `fresh ${candidate.name}`, color: '#765432', start_line: 90 })),
            edges: source.edges.map(candidate => ({ ...candidate, strategy: 'fresh-resolution', confidence: .9 })) };
        const force = vi.spyOn(Math, 'hypot');
        try {
            const reused = layoutOrganicClusters(fresh, { rootIds: new Set([0]) });
            expect(force).not.toHaveBeenCalled();
            expect(positions(reused.data)).toEqual(positions(initial.data));
            expect(reused.data.nodes[0]).toMatchObject({ name: 'fresh n0', color: '#765432', start_line: 90 });
            expect(reused.data.total_nodes).toBe(100);
            expect(reused.data.edges).toBe(fresh.edges);
            expect(reused.groups[0]!.label).toBe('src');
        } finally { force.mockRestore(); }
    });

    it('skips spatial-cell and spring passes when every supplied previous position is pinned', () => {
        const source = cliques(), initial = layoutOrganicClusters(source);
        const shifted = { ...initial.data, nodes: initial.data.nodes.map(candidate => ({ ...candidate, x: candidate.x + 123 })) };
        const force = vi.spyOn(Math, 'hypot');
        try {
            const pinned = layoutOrganicClusters(source, { previous: shifted });
            expect(force).not.toHaveBeenCalled();
            expect(positions(pinned.data)).toEqual(positions(shifted));
            const changed = { ...shifted, nodes: shifted.nodes.map(candidate => ({ ...candidate, y: candidate.y - 81 })) };
            expect(positions(layoutOrganicClusters(source, { previous: changed }).data)).toEqual(positions(changed));
        } finally { force.mockRestore(); }
    });

    it('does not reuse stale geometry or group metadata across edge filters, roots, or changed source directories', () => {
        const source = cliques();
        layoutOrganicClusters(source, { rootIds: new Set([0]) });
        const filtered = layoutOrganicClusters({ ...source, edges: source.edges.filter(candidate => candidate.source !== 4 || candidate.target !== 5) }, { rootIds: new Set([0]) });
        expect(filtered.groups.every(group => group.boundaryEdges === 0)).toBe(true);
        const rootedElsewhere = layoutOrganicClusters(source, { rootIds: new Set([7]) });
        expect(rootedElsewhere.data.nodes.find(candidate => candidate.id === 7)).toMatchObject({ x: 0, y: 0, z: 0 });
        expect(rootedElsewhere.groups.find(group => group.nodeIds.includes(7))?.rootCount).toBe(1);
        const renamed = layoutOrganicClusters({ ...source, nodes: source.nodes.map(candidate => ({ ...candidate, file_path: 'new/location/source.ts' })) }, { rootIds: new Set([7]) });
        expect(renamed.groups.every(group => group.label === 'new/location')).toBe(true);
    });

    it('finds densely related groups across a weak bridge, even when all nodes share a source file', () => {
        const layout = layoutOrganicClusters(cliques(), { rootIds: new Set([0]) });
        const left = layout.groupByNode.get(0), right = layout.groupByNode.get(5);
        expect(left).not.toBe(right);
        for (let i = 0; i < 5; i++) expect(layout.groupByNode.get(i)).toBe(left);
        for (let i = 5; i < 10; i++) expect(layout.groupByNode.get(i)).toBe(right);
        expect(layout.groupByNode.get(10)).not.toBe(left);
        expect(layout.groups.find(group => group.id === left)).toMatchObject({ rootCount: 1, internalEdges: 10, boundaryEdges: 1 });
        expect(layout.data.nodes.find(candidate => candidate.id === 0)).toMatchObject({ x: 0, y: 0, z: 0 });
    });

    it('is deterministic under node/edge ordering and never mutates source metadata or relationships', () => {
        const source = cliques(), before = structuredClone(source);
        source.edges.push({ ...edge(1, 0, 'USES_TYPE'), id: 108, line: 4, confidence: .8, strategy: 'resolved' });
        const originalEdges = [...source.edges];
        const first = layoutOrganicClusters(source);
        const second = layoutOrganicClusters({ ...source, nodes: [...source.nodes].reverse(), edges: [...source.edges].reverse() });
        expect(positions(first.data)).toEqual(positions(second.data));
        expect(first.groups).toEqual(second.groups);
        expect(first.data.edges).toBe(source.edges);
        expect(first.data.edges).toEqual(originalEdges);
        expect(source.nodes).toEqual(before.nodes);
        expect(first.data.nodes.map(({ x: _x, y: _y, z: _z, ...rest }) => rest))
            .toEqual(source.nodes.map(({ x: _x, y: _y, z: _z, ...rest }) => rest));
    });

    it('keeps every earlier position fixed while an expansion grows around its actual neighbors', () => {
        const initial = layoutOrganicClusters({ nodes: [node(0), node(1)], edges: [edge(0, 1)], total_nodes: 2 }, { rootIds: new Set([0]) });
        const expanded = layoutOrganicClusters({ nodes: [node(0), node(1), node(2)], edges: [edge(0, 1), edge(1, 2)], total_nodes: 3 },
            { rootIds: new Set([0]), previous: initial.data });
        expect(positions({ ...expanded.data, nodes: expanded.data.nodes.slice(0, 2) })).toEqual(positions(initial.data));
        const added = expanded.data.nodes[2]!, neighbor = expanded.data.nodes[1]!;
        expect(Math.hypot(added.x - neighbor.x, added.y - neighbor.y, added.z - neighbor.z)).toBeLessThan(150);
        expect(Math.hypot(added.x - neighbor.x, added.y - neighbor.y, added.z - neighbor.z)).toBeGreaterThan(0);
    });

    it('keeps the root at the origin across expansions, for a fresh trace and for a previous picture without it', () => {
        const origin = { x: 0, y: 0, z: 0 };
        const rootAt = (layout: { data: GraphData }, id: number) => {
            const found = layout.data.nodes.find(candidate => candidate.id === id)!;
            return { x: found.x + 0, y: found.y + 0, z: found.z + 0 };
        };
        const first = layoutOrganicClusters({ nodes: [node(0), node(1), node(2)], edges: [edge(0, 1), edge(2, 0)], total_nodes: 3 },
            { rootIds: new Set([0]) });
        expect(rootAt(first, 0)).toEqual(origin);
        const second = layoutOrganicClusters({ nodes: [0, 1, 2, 3, 4].map(id => node(id)), edges: [edge(0, 1), edge(2, 0), edge(1, 3), edge(4, 2)], total_nodes: 5 },
            { rootIds: new Set([0]), previous: first.data });
        expect(rootAt(second, 0)).toEqual(origin);
        expect(positions({ ...second.data, nodes: second.data.nodes.slice(0, 3) })).toEqual(positions(first.data));
        // A different direction or edge filter starts without previous positions.
        const fresh = layoutOrganicClusters({ ...second.data, nodes: second.data.nodes.map(candidate => ({ ...candidate, x: 500 })) }, { rootIds: new Set([0]) });
        expect(rootAt(fresh, 0)).toEqual(origin);
        const elsewhere = layoutOrganicClusters({ nodes: [node(0), node(1), node(2), node(9)], edges: [edge(0, 1), edge(9, 2)], total_nodes: 4 },
            { rootIds: new Set([9]), previous: first.data });
        expect(rootAt(elsewhere, 9)).toEqual(origin);
        expect(positions({ ...elsewhere.data, nodes: elsewhere.data.nodes.slice(0, 3) })).toEqual(positions(first.data));
    });

    it('uses source folders only as labels, not artificial communities or layout coordinates', () => {
        const source = cliques(), first = layoutOrganicClusters(source);
        const renamed = layoutOrganicClusters({ ...source, nodes: source.nodes.map(candidate => ({ ...candidate, file_path: `other/folder-${candidate.id}/file.ts` })) });
        expect(positions(renamed.data)).toEqual(positions(first.data));
        expect([...renamed.groupByNode]).toEqual([...first.groupByNode]);
        expect(first.groups[0]!.label).toBe('src');
    });

    it('uses irregular volume rather than rings, aligned rows or a flattened plane for a hub neighborhood', () => {
        const source: GraphData = { nodes: Array.from({ length: 80 }, (_, id) => node(id)),
            edges: Array.from({ length: 79 }, (_, index) => edge(0, index + 1)), total_nodes: 80 };
        const { data } = layoutOrganicClusters(source, { rootIds: new Set([0]) });
        const neighbors = data.nodes.slice(1);
        const radii = neighbors.map(point => Math.hypot(point.x, point.y, point.z));
        expect(Math.max(...radii) - Math.min(...radii)).toBeGreaterThan(10);
        expect(new Set(neighbors.map(point => point.z.toFixed(2))).size).toBeGreaterThan(60);
        expect(new Set(neighbors.map(point => `${point.x.toFixed(2)}:${point.y.toFixed(2)}:${point.z.toFixed(2)}`)).size).toBe(79);
    });

    it('retains every node and typed edge in a large scope, including isolates and self references', () => {
        const count = 5200;
        const nodes = Array.from({ length: count }, (_, id) => node(id, `src/part-${Math.floor(id / 40)}/file.ts`));
        const edges: GraphEdge[] = [];
        for (let id = 0; id < count - 100; id++) {
            if (id % 40 !== 39) edges.push(edge(id, id + 1));
            if (id % 40 < 36) edges.push(edge(id, id + 4, 'USES_TYPE'));
        }
        edges.push(edge(0, 0, 'CALLS'));
        const source = { nodes, edges, total_nodes: count }, layout = layoutOrganicClusters(source);
        expect(layout.data.nodes.map(candidate => candidate.id)).toEqual(nodes.map(candidate => candidate.id));
        expect(layout.data.edges).toBe(edges);
        expect(layout.groupByNode.size).toBe(count);
        expect(layout.groups.flatMap(group => group.nodeIds).sort((a, b) => a - b)).toEqual(nodes.map(candidate => candidate.id));
        expect(layout.data.nodes.every(point => [point.x, point.y, point.z].every(Number.isFinite))).toBe(true);
    });

    it('handles empty input and retains disconnected nodes without inventing relationships', () => {
        const empty: GraphData = { nodes: [], edges: [], total_nodes: 0 };
        expect(layoutOrganicClusters(empty)).toEqual({ data: empty, groups: [], groupByNode: new Map() });
        const data = { nodes: [node(1), node(2), node(3)], edges: [], total_nodes: 3 };
        const result = layoutOrganicClusters(data);
        expect(result.groups).toHaveLength(3);
        expect(result.data.edges).toEqual([]);
        expect(new Set(result.data.nodes.map(point => `${point.x}:${point.y}:${point.z}`)).size).toBe(3);
    });
});
