import { describe, expect, it } from 'vitest';
import { buildCoverageShadow, COVERAGE_SHADOW_COLOR, coverageShadowPositions, resolveCoverageShadowNode } from './coverage-shadow';
import { readGraphData } from './layout-source';
import type { GraphData, GraphNode } from './types';

const node = (id: number, label = 'File'): GraphNode => ({ id, x: 1, y: 2, z: 3, label, name: `node-${id}`, file_path: `src/${id}.ts`, size: 4, color: '#00ff88', status: 'structural' });
const fixture = (): GraphData => ({
    nodes: [node(1), node(-1)], edges: [], total_nodes: 2,
    missed_graph: { nodes: [node(1, 'Folder'), node(2)], edges: [{ source: 1, target: 2, type: 'CONTAINS_FILE' }], offset: { x: 100, y: -300, z: 10 } },
});

describe('coverage layout payload', () => {
    it('preserves the separate typed skeleton and offset without adding it to code nodes', () => {
        const data = readGraphData(fixture());
        expect(data.nodes).toHaveLength(2);
        expect(data.total_nodes).toBe(2);
        expect(data.missed_graph?.nodes).toHaveLength(2);
        expect(data.missed_graph?.offset).toEqual({ x: 100, y: -300, z: 10 });
        expect(data.missed_graph?.nodes[1]?.file_path).toBe('src/2.ts');
    });
    it('does not invent an offset for absent or malformed payloads', () => {
        for (const missed of [undefined, null, {}, { nodes: [node(1)] }, { offset: { x: 0, y: 0, z: Infinity } }]) {
            expect(readGraphData({ missed_graph: missed }).missed_graph).toBeUndefined();
        }
    });
    it('drops invalid skeleton coordinates and ignores nested skeleton objects', () => {
        const raw = fixture();
        const missed = { ...raw.missed_graph!, nodes: [node(1), { id: 2, x: 1, y: 2 }], missed_graph: raw.missed_graph };
        const data = readGraphData({ ...raw, missed_graph: missed });
        expect(data.missed_graph?.nodes).toHaveLength(1);
        expect(data.missed_graph).not.toHaveProperty('missed_graph');
    });
});

describe('coverage shadow layer', () => {
    it('offsets once, paints white, and leaves the response untouched', () => {
        const data = fixture();
        const before = structuredClone(data);
        const shadow = buildCoverageShadow(data)!;
        expect(shadow.nodes[0]).toMatchObject({ x: 101, y: -298, z: 13, color: COVERAGE_SHADOW_COLOR, sourceId: 1, layer: 'coverage-shadow' });
        expect(buildCoverageShadow(data)?.nodes).toEqual(shadow.nodes);
        expect(data).toEqual(before);
        expect(shadow.counts).toEqual({ nodes: 2, files: 1, folders: 1 });
    });
    it('remaps overlapping IDs including negative code IDs and remaps all edge endpoints', () => {
        const data = fixture();
        const shadow = buildCoverageShadow(data)!;
        for (const source of data.nodes) expect(shadow.ids.has(source.id)).toBe(false);
        expect(new Set(shadow.nodes.map((entry) => entry.id)).size).toBe(2);
        expect(shadow.edges).toEqual([{ source: shadow.nodes[0]!.id, target: shadow.nodes[1]!.id, type: 'CONTAINS_FILE' }]);
        expect([...coverageShadowPositions(shadow)]).toEqual([101, -298, 13, 101, -298, 13]);
    });
    it('drops duplicate nodes and dangling edges rather than cross-linking to code', () => {
        const data = fixture();
        data.missed_graph!.nodes.push(node(1));
        data.missed_graph!.edges.push({ source: 2, target: -1, type: 'CALLS' });
        const shadow = buildCoverageShadow(data)!;
        expect(shadow.nodes).toHaveLength(2);
        expect(shadow.edges).toHaveLength(1);
    });
    it('does not expose source status as a coverage failure reason', () => {
        const selected = buildCoverageShadow(fixture())!.nodes[1]!;
        expect(selected.status).toBe('structural');
        expect(selected).not.toHaveProperty('coverageReason');
        expect(selected).not.toHaveProperty('missedRanges');
    });
    it('resolves only current shadow objects, not code nodes or stale snapshots', () => {
        const data = fixture();
        const shadow = buildCoverageShadow(data)!;
        const current = shadow.nodes[0]!;
        expect(resolveCoverageShadowNode(shadow, current)).toBe(current);
        expect(resolveCoverageShadowNode(shadow, data.nodes[0]!)).toBeNull();
        expect(resolveCoverageShadowNode(buildCoverageShadow(data), current)).toBeNull();
        expect(resolveCoverageShadowNode(null, current)).toBeNull();
    });
    it('omits empty, invalid, or overflowing shadow geometry', () => {
        const data = fixture();
        expect(buildCoverageShadow({ nodes: [], edges: [], total_nodes: 0 })).toBeNull();
        data.missed_graph!.nodes = [];
        expect(buildCoverageShadow(data)).toBeNull();
        data.missed_graph!.nodes = [node(1)];
        data.missed_graph!.offset.x = NaN;
        expect(buildCoverageShadow(data)).toBeNull();
        data.missed_graph!.offset.x = Number.MAX_VALUE;
        data.missed_graph!.nodes[0]!.x = Number.MAX_VALUE;
        expect(buildCoverageShadow(data)).toBeNull();
        data.missed_graph!.offset.x = 0;
        expect(buildCoverageShadow(data)).toBeNull();
    });
});
