import { describe, expect, it } from 'vitest';
import type { GraphData, GraphNode } from '../galaxy/types';
import { GALAXY_CONTEXT_LIMITS, galaxyNodeContext } from './galaxy-context';

const node = (id: number, fields: Partial<GraphNode> = {}): GraphNode => ({ id, name: `node-${id}`, label: 'Function', x: 0, y: 0, z: 0, size: 1, color: '#fff', ...fields });
function fixture(): GraphData {
    return {
        nodes: [node(1, { name: 'main', qualified_name: 'app.main', file_path: 'src/main.c', start_line: 8, end_line: 21, status: 'entry', in_calls: 0, out_calls: 2 }), node(2, { name: 'initialize' }), node(3, { name: 'caller' })],
        edges: [{ source: 1, target: 2, type: 'CALLS' }, { source: 3, target: 1, type: 'CALLS' }, { source: 2, target: 3, type: 'IMPORTS' }],
        total_nodes: 200,
    };
}
function snapshot(graph: GraphData) {
    const context = galaxyNodeContext(graph, 1, 'sample', 'snapshot');
    expect(context, 'an existing selected node produces a context snapshot').toBeDefined();
    return context!;
}

describe('Galaxy selection context', () => {
    it('offers nothing when the graph or selected node is unavailable', () => {
        expect(galaxyNodeContext(undefined, 1, 'sample', 'snapshot')).toBeUndefined();
        expect(galaxyNodeContext(fixture(), 999, 'sample', 'snapshot')).toBeUndefined();
    });

    it('captures graph identity, source location, classification and loaded relationships', () => {
        const context = snapshot(fixture());
        expect(context.id).toBe('snapshot'); expect(context.label).toContain('main');
        const payload = JSON.parse(context.text);
        expect(payload).toMatchObject({ kind: 'galaxy-selection-snapshot', project: 'sample', selectedNode: { id: 1, name: 'main', qualifiedName: 'app.main', filePath: 'src/main.c', startLine: 8, endLine: 21, kind: 'Function', status: 'entry', entrypointStatus: 'classified-entry', inCalls: 0, outCalls: 2 } });
        expect(payload.incidentEdges).toEqual([{ source: 1, target: 2, type: 'CALLS' }, { source: 3, target: 1, type: 'CALLS' }]);
        expect(payload.neighbors.map((item: { id: number }) => item.id)).toEqual([2, 3]);
        expect(payload.source).toBe('loaded-galaxy-layout');
        expect(payload).not.toHaveProperty('code');
    });

    it('does not mutate a snapshot when graph data changes', () => {
        const graph = fixture(); const before = snapshot(graph);
        graph.nodes[0].name = 'changed'; graph.edges[0].type = 'changed'; graph.nodes[1].name = 'changed';
        const payload = JSON.parse(before.text);
        expect(payload.selectedNode.name).toBe('main'); expect(payload.incidentEdges[0].type).toBe('CALLS'); expect(payload.neighbors[0].name).toBe('initialize');
    });

    it('caps incident edges and neighbor metadata while reporting exact omissions from the loaded graph', () => {
        const graph: GraphData = { nodes: Array.from({ length: 31 }, (_, index) => node(index + 1)), edges: Array.from({ length: 30 }, (_, index) => ({ source: 1, target: index + 2, type: 'CALLS' })), total_nodes: 31 };
        const payload = JSON.parse(snapshot(graph).text);
        expect(payload.incidentEdges).toHaveLength(GALAXY_CONTEXT_LIMITS.incidentEdges);
        expect(payload.neighbors).toHaveLength(GALAXY_CONTEXT_LIMITS.neighbors);
        expect(payload.omissions.incidentEdges).toBe(30 - GALAXY_CONTEXT_LIMITS.incidentEdges);
        expect(payload.omissions.neighbors).toBe(30 - GALAXY_CONTEXT_LIMITS.neighbors);
        expect(payload.limitations).toContain('not an exhaustive graph');
    });

    it('keeps index coverage and generation unavailable even when loaded node count equals reported total', () => {
        const graph = fixture(); graph.total_nodes = graph.nodes.length;
        const payload = JSON.parse(snapshot(graph).text);
        expect(payload.indexCoverage.state).toBe('unavailable'); expect(payload.generation.state).toBe('unavailable');
        expect(payload.layout).toMatchObject({ loadedNodes: 3, reportedTotalNodes: 3, loadedEdges: 3 });
    });

    it('does not convert missing node metadata into zero or a negative entrypoint claim', () => {
        const graph: GraphData = { nodes: [node(1)], edges: [], total_nodes: 1 };
        const payload = JSON.parse(snapshot(graph).text);
        expect(payload.selectedNode).not.toHaveProperty('filePath'); expect(payload.selectedNode).not.toHaveProperty('startLine');
        expect(payload.selectedNode).not.toHaveProperty('inCalls'); expect(payload.selectedNode.entrypointStatus).toBe('not-established');
        expect(payload.incidentEdges).toEqual([]); expect(payload.limitations).toContain('Absence from this snapshot does not prove absence');
    });

    it('reports missing neighbor metadata and limits exceptionally large labels explicitly', () => {
        const graph = fixture(); graph.nodes[0].name = 'n'.repeat(1000); graph.edges.push({ source: 1, target: 999, type: 'CALLS' });
        const payload = JSON.parse(snapshot(graph).text);
        expect(payload.selectedNode.name.length).toBeLessThanOrEqual(GALAXY_CONTEXT_LIMITS.fieldCharacters + 1);
        expect(payload.omissions.truncatedFields).toContain('selectedNode.name');
        expect(payload.omissions.neighborMetadataUnavailable).toBe(1);
        expect(payload.neighbors.some((item: { id: number }) => item.id === 999)).toBe(false);
    });

    it('keeps large layout records bounded for a local prompt', () => {
        const graph: GraphData = { nodes: Array.from({ length: 100 }, (_, index) => node(index + 1, { name: 'n'.repeat(5000), qualified_name: 'q'.repeat(5000), file_path: 'p'.repeat(5000), label: 'k'.repeat(5000) })), edges: Array.from({ length: 99 }, (_, index) => ({ source: 1, target: index + 2, type: 'r'.repeat(5000) })), total_nodes: 100 };
        const context = snapshot(graph);
        expect(context.text.length).toBeLessThan(18000);
        expect(JSON.parse(context.text).omissions.truncatedFields.length).toBeGreaterThan(0);
    });
});
