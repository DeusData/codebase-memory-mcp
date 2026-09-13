import { expect, it } from 'vitest';
import { projectReaderHierarchy } from './reader-hierarchy';
import type { GraphData, GraphNode } from './types';

const node = (id: number, name: string, file_path = 'src/service.ts', label = 'Method'): GraphNode => ({
    id, name, qualified_name: `sample.${name}`, file_path, label,
    x: id * 5, y: 10, z: 3, color: '#668899', size: 3, start_line: id, end_line: id + 1,
});
const layout: GraphData = {
    total_nodes: 8,
    nodes: [node(1, 'file', 'src/service.ts', 'File'), node(2, 'class', 'src/service.ts', 'Class'),
        node(3, 'method'), node(4, 'isolated'), node(5, 'caller', 'src/client.ts'), node(6, 'store', 'src/db.ts'),
        node(7, 'unrelated', 'src/other.ts'), node(8, 'alsoUnrelated', 'src/other.ts')],
    edges: [{ source: 1, target: 2, type: 'DEFINES' }, { source: 2, target: 3, type: 'DEFINES_METHOD' },
        { id: 41, source: 5, target: 3, type: 'CALLS', line: 50, confidence: 0.9, strategy: 'resolved' },
        { source: 3, target: 6, type: 'USES_TYPE' }, { source: 6, target: 7, type: 'CALLS' }],
};

it('orders recorded definitions between incoming and outgoing relationships without inventing edges', () => {
    const projected = projectReaderHierarchy(layout, 'src/service.ts')!;
    const positions = new Map(projected.data.nodes.map(entry => [entry.name, entry.x]));
    expect(positions.get('caller')).toBeLessThan(positions.get('file')!);
    expect(positions.get('file')).toBeLessThan(positions.get('class')!);
    expect(positions.get('class')).toBeLessThan(positions.get('method')!);
    expect(positions.get('method')).toBeLessThan(positions.get('store')!);
    expect(projected.data.nodes.map(entry => entry.name)).toContain('isolated');
    expect(projected.data.nodes.map(entry => entry.name)).not.toContain('unrelated');
    expect(projected.data.edges).toHaveLength(4);
    expect(projected.data.edges.find(edge => edge.id === 41)).toMatchObject({ type: 'CALLS', line: 50, confidence: 0.9, strategy: 'resolved' });
    expect(projected.data.nodes.find(entry => entry.name === 'method')).toMatchObject({ file_path: 'src/service.ts', start_line: 3, color: '#668899' });
    expect(layout.nodes[0]!.x).toBe(5);
    expect(projectReaderHierarchy({ ...layout, nodes: [...layout.nodes].reverse(), edges: [...layout.edges].reverse() }, 'src/service.ts')).toEqual(projected);
});

it('keeps selected isolated nodes before neighbors at the cap and reports both bounds', () => {
    const projected = projectReaderHierarchy({ ...layout, total_nodes: 100 }, 'src/service.ts', undefined, 4)!;
    expect(projected.data.nodes.map(entry => entry.name).sort()).toEqual(['class', 'file', 'isolated', 'method']);
    expect(projected.truncated).toBe(true);
    expect(projected.edgeNote).toContain('2 related nodes omitted at the 4-node limit');
    expect(projected.edgeNote).toContain('8 of 100 indexed nodes loaded');
});

it('keeps definition cycles finite and preserves their recorded edges', () => {
    const edges = [{ source: 1, target: 2, type: 'DEFINES' }, { source: 2, target: 1, type: 'DEFINES' }];
    const projected = projectReaderHierarchy({ nodes: layout.nodes.slice(0, 2), edges, total_nodes: 2 }, 'src/service.ts')!;
    expect(projected.data.edges).toHaveLength(2);
    expect(projected.data.nodes.every(entry => Number.isFinite(entry.x) && Number.isFinite(entry.y))).toBe(true);
    expect(projected.depth).toBe(1);
    expect(projectReaderHierarchy(layout, 'src/missing.ts')).toBeUndefined();
});
