import type { GraphData, GraphNode } from '../galaxy/types';

export const MAP_RELATIONS = ['CALLS', 'IMPORTS', 'USAGE', 'INHERITS', 'IMPLEMENTS', 'DATA_FLOWS'] as const;
export type MapRelation = typeof MAP_RELATIONS[number];
export interface MapEvidence { source: GraphNode; target: GraphNode; type: MapRelation; id?: number; line?: number; unverifiedLine?: number; strategy?: string; confidence?: number }
export interface RepositoryArea {
    path: string;
    files: string[];
    nodes: GraphNode[];
    entryPoints: GraphNode[];
    interfaces: GraphNode[];
    incoming: MapEvidence[];
    outgoing: MapEvidence[];
}
export interface RepositoryMap {
    areas: RepositoryArea[];
    evidence: MapEvidence[];
    fileCount: number;
    representedNodes: number;
    totalNodes: number;
    unresolvedEdges: number;
}

/** A navigational partition, never a claim about domain ownership. */
export function areaOf(path: string): string {
    const parts = path.split('/').filter(Boolean);
    if (parts.length < 2) return '(root)';
    // Source containers have a useful second level; all other directories remain
    // visible intact instead of guessing a programming-language-specific layout.
    if (['src', 'lib', 'internal', 'packages', 'apps', 'services'].includes(parts[0]) && parts.length > 2) {
        return parts.slice(0, 2).join('/');
    }
    return parts[0];
}

const isSource = (node: GraphNode) => Boolean(node.file_path && node.file_path !== '{}'
    && !['Project', 'Folder', 'Package', 'Branch'].includes(node.label));
const priority = (a: GraphNode, b: GraphNode) => (b.in_calls ?? 0) - (a.in_calls ?? 0)
    || (b.out_calls ?? 0) - (a.out_calls ?? 0) || a.name.localeCompare(b.name);

/** Linear in the already loaded graph. Every aggregate retains its real edges. */
export function repositoryMap(graph: GraphData): RepositoryMap {
    const areas = new Map<string, RepositoryArea>();
    const nodes = new Map(graph.nodes.filter(isSource).map(node => [node.id, node]));
    const files = new Map<string, Set<string>>();
    for (const node of nodes.values()) {
        const path = areaOf(node.file_path!);
        let area = areas.get(path);
        if (!area) {
            area = { path, files: [], nodes: [], entryPoints: [], interfaces: [], incoming: [], outgoing: [] };
            areas.set(path, area); files.set(path, new Set());
        }
        files.get(path)!.add(node.file_path!);
        area.nodes.push(node);
        if (node.status === 'entry') area.entryPoints.push(node);
        if (node.status === 'exported') area.interfaces.push(node);
    }
    const evidence: MapEvidence[] = [];
    const seen = new Set<string>();
    let unresolvedEdges = 0;
    for (const edge of graph.edges) {
        if (!MAP_RELATIONS.includes(edge.type as MapRelation)) continue;
        const key = `${edge.source}:${edge.type}:${edge.target}`;
        if (seen.has(key)) continue;
        seen.add(key);
        const source = nodes.get(edge.source); const target = nodes.get(edge.target);
        if (!source || !target) { unresolvedEdges++; continue; }
        // Some parser edges retain preprocessed coordinates. An out-of-range
        // number is evidence metadata, never a verified navigable call site.
        const validSite = edge.line !== undefined && source.start_line !== undefined && source.end_line !== undefined
            && edge.line >= source.start_line && edge.line <= source.end_line;
        const finding: MapEvidence = { source, target, type: edge.type as MapRelation, id: edge.id,
            line: validSite ? edge.line : undefined, unverifiedLine: validSite ? undefined : edge.line,
            strategy: edge.strategy, confidence: edge.confidence };
        evidence.push(finding);
        const from = areas.get(areaOf(source.file_path!))!;
        const to = areas.get(areaOf(target.file_path!))!;
        if (from !== to) { from.outgoing.push(finding); to.incoming.push(finding); }
    }
    for (const area of areas.values()) {
        area.files = [...files.get(area.path)!].sort();
        area.entryPoints.sort(priority); area.interfaces.sort(priority); area.nodes.sort(priority);
    }
    return {
        areas: [...areas.values()].sort((a, b) => b.incoming.length + b.outgoing.length
            - a.incoming.length - a.outgoing.length || a.path.localeCompare(b.path)),
        evidence, fileCount: new Set([...files.values()].flatMap(set => [...set])).size,
        representedNodes: nodes.size, totalNodes: graph.total_nodes, unresolvedEdges,
    };
}

export function areaConnections(area: RepositoryArea, direction: 'incoming' | 'outgoing') {
    const rows = new Map<string, { area: string; type: MapRelation; evidence: MapEvidence[] }>();
    for (const edge of area[direction]) {
        const other = areaOf((direction === 'incoming' ? edge.source : edge.target).file_path!);
        const key = `${other}:${edge.type}`;
        const row = rows.get(key) ?? { area: other, type: edge.type, evidence: [] };
        row.evidence.push(edge); rows.set(key, row);
    }
    return [...rows.values()].sort((a, b) => b.evidence.length - a.evidence.length || a.area.localeCompare(b.area));
}
