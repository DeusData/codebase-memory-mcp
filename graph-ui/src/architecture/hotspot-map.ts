import type { ArchitectureHotspot } from '../core/intelligence-provider';
import type { GraphData, GraphNode } from '../galaxy/types';
import { layoutFolderHierarchy, pruneHierarchyPlatforms, type SemanticGraph, type SemanticNode } from './semantic-graph';
import { areaOf } from './repository-map';
import { hotspotScore } from '../provider/cbm-rpc-provider';

export interface HotspotGroup { findings: ArchitectureHotspot[]; maxFanIn?: number; peakScore: number }
export interface HotspotCatalog {
    findings: ArchitectureHotspot[];
    byFile: Map<string, HotspotGroup>;
    byArea: Map<string, HotspotGroup>;
    bySymbolId: Map<number, HotspotGroup>;
    maxFanIn: number;
    unmapped: number;
}
const positive = (value?: number) => value !== undefined && Number.isFinite(value) && value > 0 ? value : 0;
const score = (finding: ArchitectureHotspot) => hotspotScore({ ...finding, fanIn: positive(finding.fanIn), complexity: positive(finding.complexity), cognitive: positive(finding.cognitive), loopDepth: positive(finding.loopDepth) });
export const hotspotIdentity = (finding: ArchitectureHotspot) => `${finding.qualifiedName ?? finding.name}@${finding.filePath ?? ''}:${finding.line ?? ''}`;
const identity = hotspotIdentity;
const compare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
function summarize(findings: ArchitectureHotspot[]): HotspotGroup {
    const measured = findings.flatMap(finding => finding.fanIn !== undefined && Number.isFinite(finding.fanIn) && finding.fanIn >= 0 ? [finding.fanIn] : []);
    return { findings, maxFanIn: measured.length ? Math.max(...measured) : undefined, peakScore: Math.max(0, ...findings.map(score)) };
}

export function collectHotspots(findings: ArchitectureHotspot[], graph: GraphData): HotspotCatalog {
    const byName = new Map<string, GraphNode[]>();
    for (const node of graph.nodes) if (node.qualified_name) { const matches = byName.get(node.qualified_name) ?? []; matches.push(node); byName.set(node.qualified_name, matches); }
    const unique = new Map<string, ArchitectureHotspot>();
    for (const finding of findings) {
        if (score(finding) <= 0) continue;
        const matches = finding.qualifiedName ? byName.get(finding.qualifiedName) ?? [] : [];
        const candidates = matches.filter(node => !finding.filePath || node.file_path === finding.filePath);
        const symbol = candidates.length === 1 ? candidates[0] : undefined;
        const value = { ...finding, filePath: finding.filePath ?? symbol?.file_path, line: finding.line ?? symbol?.start_line };
        const key = identity(value); const previous = unique.get(key);
        if (!previous || score(value) > score(previous)) unique.set(key, value);
    }
    const ranked = [...unique.values()].sort((a, b) => score(b) - score(a) || compare(identity(a), identity(b)));
    const files = new Map<string, ArchitectureHotspot[]>(); const areas = new Map<string, ArchitectureHotspot[]>();
    const symbols = new Map<number, ArchitectureHotspot[]>();
    for (const finding of ranked) {
        const candidates = (finding.qualifiedName ? byName.get(finding.qualifiedName) ?? [] : []).filter(node => !finding.filePath || node.file_path === finding.filePath);
        if (candidates.length === 1) {
            const id = candidates[0].id; const values = symbols.get(id) ?? [];
            values.push(finding); symbols.set(id, values);
        }
    }
    for (const finding of ranked) if (finding.filePath) {
        const file = files.get(finding.filePath) ?? []; file.push(finding); files.set(finding.filePath, file);
        const area = areaOf(finding.filePath); const members = areas.get(area) ?? []; members.push(finding); areas.set(area, members);
    }
    return { findings: ranked, byFile: new Map([...files].map(([key, group]) => [key, summarize(group)])),
        byArea: new Map([...areas].map(([key, group]) => [key, summarize(group)])),
        bySymbolId: new Map([...symbols].map(([key, group]) => [key, summarize(group)])),
        maxFanIn: Math.max(0, ...ranked.map(finding => positive(finding.fanIn))), unmapped: ranked.filter(finding => !finding.filePath).length };
}

export function hotspotsForNode(node: SemanticNode, catalog: HotspotCatalog): HotspotGroup | undefined {
    if (node.kind === 'area') return node.areaPath ? catalog.byArea.get(node.areaPath) : undefined;
    if (node.kind === 'file') return node.filePath ? catalog.byFile.get(node.filePath) : undefined;
    return node.graphNode ? catalog.bySymbolId.get(node.graphNode.id) : undefined;
}

/** Static fan-in percentage; missing measurements stay distinct from zero. */
export function gravityPercent(fanIn: number | undefined, referenceFanIn: number): number | undefined {
    if (fanIn === undefined || !Number.isFinite(fanIn) || fanIn < 0) return undefined;
    return Math.min(1, fanIn / Math.max(1, Number.isFinite(referenceFanIn) ? referenceFanIn : 1)) * 100;
}

/** Use the same square-root compression as source height, with no field at zero. */
export function gravityStrength(fanIn: number | undefined, referenceFanIn: number): number {
    return Math.sqrt((gravityPercent(fanIn, referenceFanIn) ?? 0) / 100);
}

export function hotspotSignals(finding: ArchitectureHotspot): string[] {
    const signals: string[] = [];
    if (finding.fanIn !== undefined && Number.isFinite(finding.fanIn)) signals.push(`Fan-in ${finding.fanIn}`);
    if (finding.complexity !== undefined && Number.isFinite(finding.complexity)) signals.push(`Complexity ${finding.complexity}`);
    if (finding.cognitive !== undefined && Number.isFinite(finding.cognitive)) signals.push(`Cognitive ${finding.cognitive}`);
    if (finding.loopDepth !== undefined && Number.isFinite(finding.loopDepth)) signals.push(`Loop depth ${finding.loopDepth}`);
    if (finding.allocationInLoop) signals.push('Allocation in a loop');
    if (finding.scanInLoop) signals.push('Linear scan in a loop');
    if (finding.unguardedRecursion) signals.push('Unguarded recursion');
    return signals;
}

/** A dedicated hotspot view keeps exact symbol identities and real connecting edges. */
export function buildHotspotGraph(graph: GraphData, catalog: HotspotCatalog, filter = ''): SemanticGraph {
    const byName = new Map<string, GraphNode[]>();
    for (const node of graph.nodes) if (node.qualified_name) {
        const list = byName.get(node.qualified_name) ?? []; list.push(node); byName.set(node.qualified_name, list);
    }
    const match = filter.trim().toLowerCase();
    const nodes: SemanticNode[] = []; const byGraphId = new Map<number, string>(); const seen = new Set<string>();
    const matchingNodes = new Map<string, SemanticNode>();
    for (const finding of catalog.findings) {
        const candidates = finding.qualifiedName ? byName.get(finding.qualifiedName) ?? [] : [];
        const exact = candidates.filter(node => (!finding.filePath || node.file_path === finding.filePath) && ['Function', 'Method'].includes(node.label));
        const symbol = exact.length === 1 ? exact[0] : undefined;
        const id = symbol ? `hotspot:${symbol.id}` : `hotspot-file:${finding.filePath ?? identity(finding)}`;
        const projected: SemanticNode = { id, kind: symbol || !finding.filePath ? 'symbol' : 'file', label: symbol?.name ?? finding.filePath ?? finding.name,
            detail: symbol ? hotspotSignals(finding).join(' · ') : 'Ranked hotspot; symbol identity is outside the loaded graph',
            position: [0, 0, 0], count: 1, graphNode: symbol, filePath: finding.filePath, line: finding.line,
            areaPath: finding.filePath ? areaOf(finding.filePath) : undefined, members: symbol ? [symbol] : [] };
        if (!matchingNodes.has(id) && `${finding.name} ${finding.filePath ?? ''} ${hotspotSignals(finding).join(' ')}`.toLowerCase().includes(match)) matchingNodes.set(id, projected);
        if (seen.has(id)) continue; seen.add(id);
        nodes.push(projected);
        if (symbol) byGraphId.set(symbol.id, id);
    }
    const platforms = layoutFolderHierarchy(nodes);
    const positions = new Map(nodes.map(node => [node.id, node.position]));
    const matchedNodes = [...matchingNodes.values()].map(node => ({ ...node, position: positions.get(node.id)! }));
    const allEdges = graph.edges.filter(edge => matchingNodes.has(byGraphId.get(edge.source)!) && matchingNodes.has(byGraphId.get(edge.target)!) && ['CALLS', 'IMPORTS', 'USAGE', 'DATA_FLOWS', 'INHERITS', 'IMPLEMENTS'].includes(edge.type))
        .sort((a, b) => compare(`${a.source}:${a.type}:${a.target}:${a.id ?? ''}`, `${b.source}:${b.type}:${b.target}:${b.id ?? ''}`));
    const totalNodes = matchedNodes.length; const shown = matchedNodes.slice(0, 40); const visible = new Set(shown.map(node => node.id));
    const graphById = new Map(graph.nodes.map(node => [node.id, node]));
    const edges = allEdges.filter(edge => visible.has(byGraphId.get(edge.source)!) && visible.has(byGraphId.get(edge.target)!)).slice(0, 100).map((edge, index) => ({
        id: `hotspot-edge:${edge.id ?? index}`, source: byGraphId.get(edge.source)!, target: byGraphId.get(edge.target)!, type: edge.type, count: 1,
        evidence: [{ source: graphById.get(edge.source)!, target: graphById.get(edge.target)!, type: edge.type, id: edge.id, strategy: edge.strategy }],
    }));
    return { view: 'hotspots', scopeKey: `hotspots:${filter}`, title: 'Hotspots and their connections',
        positionMeaning: 'Platforms follow source folders; symbol positions follow file and source order, independently of rank. Gravity fields encode static fan-in, not runtime activity.',
        nodes: shown, platforms: pruneHierarchyPlatforms(platforms, shown), edges, totalNodes, totalEdges: allEdges.length, omittedNodes: totalNodes - shown.length, omittedEdges: allEdges.length - edges.length,
        warnings: ['Hotspots are ranked review signals, not confirmed defects. Unmarked code has not been certified safe.', ...(graph.total_nodes > graph.nodes.length ? ['Some hotspot identities or relationships are outside the loaded graph.'] : [])] };
}
