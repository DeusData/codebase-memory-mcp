import { describe, expect, it } from 'vitest';
import { layoutNodeForSelection, selectedGraphContext } from './selected-node';
import type { GraphData, GraphNode } from './types';

const node = (id: number, name: string): GraphNode => ({ id, name, qualified_name: `p.${name}`, label: 'Function', x: 0, y: 0, z: 0, size: 1, color: '#fff' });
const layout: GraphData = { nodes: [node(0, 'unrelated'), node(51, 'selected')], edges: [{ source: 51, target: 0, type: 'CALLS' }], total_nodes: 200 };

describe('selected graph identity', () => {
    it('uses the canonical symbol even when a hierarchy ID belongs to another layout node', () => {
        const selected = node(0, 'selected');
        expect(layoutNodeForSelection(layout, selected)).toBe(layout.nodes[1]);
        const context = JSON.parse(selectedGraphContext(layout, selected, 'p', 'snapshot').text);
        expect(context.selectedNode).toMatchObject({ id: 51, qualifiedName: 'p.selected' });
        expect(context.incidentEdges).toEqual([{ source: 51, target: 0, type: 'CALLS' }]);
    });

    it('retains an out-of-layout selection without borrowing facts from a colliding ID', () => {
        const selected = { ...node(0, 'outside'), file_path: 'src/outside.ts', start_line: 9 };
        expect(layoutNodeForSelection(layout, selected)).toBeUndefined();
        const context = JSON.parse(selectedGraphContext(layout, selected, 'p', 'snapshot').text);
        expect(context.source).toBe('selected-graph-node');
        expect(context.selectedNode).toEqual({ name: 'outside', qualifiedName: 'p.outside', filePath: 'src/outside.ts', startLine: 9, kind: 'Function' });
        expect(context.selectedNode).not.toHaveProperty('id');
        expect(context.relationships.state).toBe('unavailable');
        expect(context).not.toHaveProperty('incidentEdges');
        expect(context).not.toHaveProperty('neighbors');
    });

    it('accepts unnamed canonical node objects without trusting anonymous projection IDs', () => {
        const anonymous = node(0, 'resource');
        delete anonymous.qualified_name;
        const graph = { ...layout, nodes: [anonymous] };
        expect(layoutNodeForSelection(graph, anonymous)).toBe(anonymous);
        expect(layoutNodeForSelection(graph, { ...anonymous })).toBeUndefined();
    });
});
