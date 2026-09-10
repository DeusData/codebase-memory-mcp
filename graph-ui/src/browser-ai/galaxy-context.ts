import type { GraphData, GraphNode } from '../galaxy/types';
import type { BrowserChatContext } from './chat-model';

export const GALAXY_CONTEXT_LIMITS = { incidentEdges: 12, neighbors: 6, fieldCharacters: 256 } as const;

/** Freeze facts already present in the loaded layout, without fetching code or claiming index completeness. */
export function galaxyNodeContext(graph: GraphData | undefined, nodeId: number, project: string, snapshotId: string): BrowserChatContext | undefined {
    const selected = graph?.nodes.find(node => node.id === nodeId);
    if (!graph || !selected) return undefined;
    const truncatedFields: string[] = [];
    const field = (value: string | undefined, path: string): string | undefined => {
        if (value === undefined || value.length <= GALAXY_CONTEXT_LIMITS.fieldCharacters) return value;
        truncatedFields.push(path);
        return `${value.slice(0, GALAXY_CONTEXT_LIMITS.fieldCharacters)}…`;
    };
    const record = (node: GraphNode, path: string) => ({
        id: node.id,
        name: field(node.name, `${path}.name`),
        qualifiedName: field(node.qualified_name, `${path}.qualifiedName`),
        filePath: field(node.file_path, `${path}.filePath`),
        startLine: node.start_line,
        endLine: node.end_line,
        kind: field(node.label, `${path}.kind`),
        status: node.status,
        entrypointStatus: node.status === 'entry' ? 'classified-entry' : 'not-established',
        inCalls: node.in_calls,
        outCalls: node.out_calls,
    });
    const incident = graph.edges.filter(edge => edge.source === nodeId || edge.target === nodeId);
    const neighborIds = [...new Set(incident.map(edge => edge.source === nodeId ? edge.target : edge.source).filter(id => id !== nodeId))];
    const nodeMap = new Map(graph.nodes.map(node => [node.id, node]));
    const availableNeighbors = neighborIds.map(id => nodeMap.get(id)).filter((node): node is GraphNode => node !== undefined);
    const selectedNode = record(selected, 'selectedNode');
    const incidentEdges = incident.slice(0, GALAXY_CONTEXT_LIMITS.incidentEdges).map((edge, index) => ({ source: edge.source, target: edge.target, type: field(edge.type, `incidentEdges[${index}].type`) }));
    const neighbors = availableNeighbors.slice(0, GALAXY_CONTEXT_LIMITS.neighbors).map((node, index) => record(node, `neighbors[${index}]`));
    return {
        id: snapshotId,
        label: `Graph node · ${selected.name.length > 60 ? `${selected.name.slice(0, 60)}…` : selected.name}`,
        text: JSON.stringify({
            kind: 'galaxy-selection-snapshot',
            source: 'loaded-galaxy-layout',
            project: field(project, 'project'),
            selectedNode,
            incidentEdges,
            neighbors,
            layout: { loadedNodes: graph.nodes.length, reportedTotalNodes: graph.total_nodes, loadedEdges: graph.edges.length, selectedIncidentEdges: incident.length },
            coordinates: 'Source lines are one-based when provided; absent fields are unavailable, not zero.',
            indexCoverage: { state: 'unavailable', reason: 'The loaded layout does not establish index coverage.' },
            generation: { state: 'unavailable', reason: 'The loaded layout does not provide an index generation.' },
            bounds: GALAXY_CONTEXT_LIMITS,
            omissions: { incidentEdges: incident.length - incidentEdges.length, neighbors: neighborIds.length - neighbors.length, neighborMetadataUnavailable: neighborIds.length - availableNeighbors.length, truncatedFields },
            limitations: 'This is a bounded sample of loaded layout facts, not an exhaustive graph. Absence from this snapshot does not prove absence from the codebase. Status and entrypoint classifications are backend labels, not runtime observations. Text fields listed in truncatedFields contain only the recorded prefix. No source code was fetched.',
        }, null, 2),
    };
}
