import type { GraphData } from '../galaxy/types';
import type { HotspotCatalog } from './hotspot-map';
import { areaOf, MAP_RELATIONS } from './repository-map';

export interface HotspotArea {
    path: string;
    findings: number;
    files: number;
    dependentFiles: number;
    peakFanIn?: number;
}

/** Unique outside files with direct incoming dependencies, never transitive impact or runtime traffic. */
export function hotspotAreas(catalog: HotspotCatalog, graph: GraphData): HotspotArea[] {
    const nodes = new Map(graph.nodes.map(node => [node.id, node]));
    const incoming = new Map<string, Set<string>>();
    for (const edge of graph.edges) {
        if (!(MAP_RELATIONS as readonly string[]).includes(edge.type)) continue;
        const source = nodes.get(edge.source)?.file_path;
        const target = nodes.get(edge.target)?.file_path;
        if (!source || !target || areaOf(source) === areaOf(target)) continue;
        const area = areaOf(target);
        if (!catalog.byArea.has(area)) continue;
        const files = incoming.get(area) ?? new Set<string>();
        files.add(source); incoming.set(area, files);
    }
    return [...catalog.byArea].sort(([a, first], [b, second]) => second.peakScore - first.peakScore || a.localeCompare(b))
        .map(([path, group]) => ({ path, findings: group.findings.length,
            files: new Set(group.findings.flatMap(finding => finding.filePath ? [finding.filePath] : [])).size,
            dependentFiles: incoming.get(path)?.size ?? 0, peakFanIn: group.maxFanIn }));
}
