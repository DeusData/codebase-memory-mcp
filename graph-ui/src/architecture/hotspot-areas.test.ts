import { expect, it } from 'vitest';
import type { GraphData, GraphNode } from '../galaxy/types';
import { collectHotspots } from './hotspot-map';
import { hotspotAreas } from './hotspot-areas';

const node = (id: number, path: string): GraphNode => ({ id, name: `n${id}`, qualified_name: `sample.n${id}`,
    file_path: path, label: 'Function', x: 0, y: 0, z: 0, size: 1, color: '' });

it('counts unique outside dependent files without confusing containment, direction or same-area calls', () => {
    const graph: GraphData = { total_nodes: 6, nodes: [node(1, 'src/core/a.ts'), node(2, 'src/core/b.ts'),
        node(3, 'apps/web/a.ts'), node(4, 'apps/web/a.ts'), node(5, 'apps/cli/main.ts'), node(6, 'tests/core.ts')], edges: [
        { source: 3, target: 1, type: 'CALLS' }, { source: 4, target: 2, type: 'USAGE' },
        { source: 5, target: 2, type: 'IMPORTS' }, { source: 2, target: 1, type: 'CALLS' },
        { source: 1, target: 6, type: 'CALLS' }, { source: 6, target: 1, type: 'DEFINES' },
        { source: 99, target: 1, type: 'CALLS' },
    ] };
    const catalog = collectHotspots([{ name: 'n1', qualifiedName: 'sample.n1', fanIn: 9 },
        { name: 'n1', qualifiedName: 'sample.n1', fanIn: 9 },
        { name: 'n2', qualifiedName: 'sample.n2', complexity: 8 }], graph);
    expect(hotspotAreas(catalog, graph)).toEqual([{ path: 'src/core', findings: 2, files: 2, dependentFiles: 2, peakFanIn: 9 }]);
});

it('keeps missing measurements distinct from zero and does not infer missing source areas', () => {
    const graph: GraphData = { total_nodes: 0, nodes: [], edges: [] };
    const catalog = collectHotspots([{ name: 'unknown', complexity: 7 }, { name: 'known', filePath: 'src/core/a.ts', complexity: 8 }], graph);
    expect(hotspotAreas(catalog, graph)).toEqual([{ path: 'src/core', findings: 1, files: 1, dependentFiles: 0, peakFanIn: undefined }]);
});
