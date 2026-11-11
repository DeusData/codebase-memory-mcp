import { normalizeEdgeType } from './edge-style';

interface LoadEdge { source: string; target: string; type: string; types?: readonly string[] }
export interface ConnectionLoad { links: number; neighbors: number; strength: number }

/** Visual load counts distinct directed, typed lines, not aggregated calls or runtime traffic. */
export function connectionLoad(nodes: readonly { id: string }[], edges: readonly LoadEdge[]): Map<string, ConnectionLoad> {
    const links = new Map(nodes.map(node => [node.id, new Set<string>()]));
    const neighbors = new Map(nodes.map(node => [node.id, new Set<string>()]));
    for (const edge of edges) {
        if (!links.has(edge.source) || !links.has(edge.target)) continue;
        const types = new Set((edge.types?.length ? edge.types : [edge.type]).map(normalizeEdgeType));
        for (const type of types) {
            const key = JSON.stringify([edge.source, edge.target, type]);
            links.get(edge.source)!.add(key); links.get(edge.target)!.add(key);
        }
        if (edge.source !== edge.target) {
            neighbors.get(edge.source)!.add(edge.target); neighbors.get(edge.target)!.add(edge.source);
        }
    }
    const maximum = Math.max(0, ...[...links.values()].map(items => items.size));
    return new Map(nodes.map(node => {
        const count = links.get(node.id)!.size;
        return [node.id, { links: count, neighbors: neighbors.get(node.id)!.size,
            strength: maximum ? Math.log1p(count) / Math.log1p(maximum) : 0 }];
    }));
}
