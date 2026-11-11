import type { GraphData, GraphEdge, GraphNode } from './types';

export const COVERAGE_SHADOW_COLOR = '#e9eef5';

export interface CoverageShadowNode extends GraphNode {
    layer: 'coverage-shadow';
    /** Original identifier in the missed graph, never a code-graph identifier. */
    sourceId: number;
}

export interface CoverageShadow {
    nodes: CoverageShadowNode[];
    edges: GraphEdge[];
    ids: Set<number>;
    nodeById: Map<number, CoverageShadowNode>;
    counts: { nodes: number; files: number; folders: number };
}

/** Apply the server offset exactly once and isolate every shadow render identity. */
export function buildCoverageShadow(data: GraphData): CoverageShadow | null {
    const missed = data.missed_graph;
    if (!missed || missed.nodes.length === 0) return null;
    if (![missed.offset.x, missed.offset.y, missed.offset.z].every(Number.isFinite)) return null;
    const occupied = new Set(data.nodes.map((node) => node.id));
    const remapped = new Map<number, number>();
    const nodes: CoverageShadowNode[] = [];
    let nextId = -1;
    for (const source of missed.nodes) {
        if (remapped.has(source.id)) continue;
        const x = source.x + missed.offset.x;
        const y = source.y + missed.offset.y;
        const z = source.z + missed.offset.z;
        if (!Number.isFinite(source.id) || ![x, y, z].every((value) => Number.isFinite(Math.fround(value)))) continue;
        while (occupied.has(nextId)) nextId -= 1;
        const id = nextId--;
        occupied.add(id);
        remapped.set(source.id, id);
        nodes.push({ ...source, id, sourceId: source.id, layer: 'coverage-shadow', x, y, z, color: COVERAGE_SHADOW_COLOR });
    }
    if (nodes.length === 0) return null;
    const edges: GraphEdge[] = [];
    for (const edge of missed.edges) {
        const source = remapped.get(edge.source);
        const target = remapped.get(edge.target);
        if (source !== undefined && target !== undefined) edges.push({ ...edge, source, target });
    }
    return {
        nodes,
        edges,
        ids: new Set(nodes.map((node) => node.id)),
        nodeById: new Map(nodes.map((node) => [node.id, node])),
        counts: {
            nodes: nodes.length,
            files: nodes.filter((node) => node.label === 'File').length,
            folders: nodes.filter((node) => node.label === 'Folder').length,
        },
    };
}

/** Geometry contains only edges whose endpoints belong to this coverage layer. */
export function coverageShadowPositions(shadow: CoverageShadow): Float32Array {
    const values: number[] = [];
    for (const edge of shadow.edges) {
        const source = shadow.nodeById.get(edge.source);
        const target = shadow.nodeById.get(edge.target);
        if (!source || !target) continue;
        values.push(source.x, source.y, source.z, target.x, target.y, target.z);
    }
    return new Float32Array(values);
}

/** Accept only a node from the current shadow layer, never a code node or an old snapshot. */
export function resolveCoverageShadowNode(shadow: CoverageShadow | null | undefined, node: GraphNode | null): CoverageShadowNode | null {
    if (!node || !shadow) return null;
    const current = shadow.nodeById.get(node.id);
    return current === node ? current : null;
}
