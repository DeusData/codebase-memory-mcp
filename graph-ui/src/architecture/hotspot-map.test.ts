import { describe, expect, it } from 'vitest';
import type { GraphData, GraphNode } from '../galaxy/types';
import type { ArchitectureHotspot } from '../core/intelligence-provider';
import { buildSemanticGraph } from './semantic-graph';
import { buildHotspotGraph, collectHotspots, gravityPercent, gravityStrength, hotspotSignals, hotspotsForNode } from './hotspot-map';

const node = (id: number, name: string, file: string): GraphNode => ({ id, name, qualified_name: `project.${name}`, label: 'Function', file_path: file, start_line: 10, end_line: 80, x: 0, y: 0, z: 0, size: 1, color: '' });
const graph: GraphData = { nodes: [node(1, 'busy', 'src/api/busy.ts'), node(2, 'complex', 'src/api/complex.ts'), node(3, 'plain', 'other/plain.py')],
    edges: [{ id: 5, source: 1, target: 2, type: 'CALLS' }], total_nodes: 3 };
const findings: ArchitectureHotspot[] = [{ name: 'busy', qualifiedName: 'project.busy', fanIn: 100 },
    { name: 'complex', qualifiedName: 'project.complex', complexity: 25, cognitive: 30 },
    { name: 'plain', qualifiedName: 'project.plain', fanIn: 0, complexity: 0 }];

describe('hotspot overlays and gravity', () => {
    it('joins exact symbol identities and aggregates a folder by peak fan-in, not a misleading sum', () => {
        const catalog = collectHotspots(findings, graph);
        expect(catalog.findings).toHaveLength(2);
        const overview = buildSemanticGraph(graph, { view: 'overview' });
        const area = overview.nodes.find(node => node.areaPath === 'src/api')!;
        expect(hotspotsForNode(area, catalog)).toMatchObject({ maxFanIn: 100, findings: expect.any(Array) });
        expect(hotspotsForNode(area, catalog)?.findings).toHaveLength(2);
        expect(catalog.byFile.get('src/api/complex.ts')?.maxFanIn).toBeUndefined();
    });
    it('does not turn complexity or missing fan-in into a gravity measurement', () => {
        const catalog = collectHotspots(findings, graph);
        expect(gravityStrength(catalog.byFile.get('src/api/complex.ts')!.maxFanIn, catalog.maxFanIn)).toBe(0);
        expect(hotspotSignals(findings[1])).toEqual(['Complexity 25', 'Cognitive 30']);
        expect(hotspotSignals({ name: 'zero', fanIn: 0 })).toEqual(['Fan-in 0']);
        expect(hotspotSignals({ name: 'unknown' })).toEqual([]);
    });
    it('uses a bounded percentage field without changing semantic positions', () => {
        const strengths = [0, 1, 10, 100, 1000000].map(count => gravityStrength(count, 1000000));
        expect(strengths).toEqual([...strengths].sort((a, b) => a - b));
        expect(strengths.every(value => value >= 0 && value <= 1)).toBe(true);
        expect(gravityStrength(Infinity, 10)).toBe(0);
        expect(gravityStrength(1e9, 100)).toBe(1);
        expect(gravityPercent(100, 100)).toBe(100);
        expect(gravityPercent(25, 100)).toBe(25);
        expect(gravityPercent(0, 100)).toBe(0);
        expect(gravityPercent(undefined, 100)).toBeUndefined();
        expect(gravityStrength(1, 100)).toBeCloseTo(0.1);
        expect(gravityStrength(25, 100)).toBeCloseTo(0.5);
        expect(gravityStrength(1, 100)).toBeLessThan(gravityStrength(100, 100) / 5);
    });
    it('keeps actual hotspot nodes and exact edges, without inventing calls to missing identities', () => {
        const catalog = collectHotspots([...findings, { name: 'outside', qualifiedName: 'project.outside', filePath: 'unloaded/source.ts', fanIn: 50 }], graph);
        const model = buildHotspotGraph(graph, catalog);
        expect(model.view).toBe('hotspots');
        expect(model.nodes.find(node => node.graphNode?.id === 1)?.graphNode).toBe(graph.nodes[0]);
        const outside = model.nodes.find(node => node.filePath === 'unloaded/source.ts')!;
        expect(outside.graphNode).toBeUndefined();
        expect(outside.members).toEqual([]);
        expect(model.edges).toHaveLength(1);
        expect(model.edges[0].evidence[0].id).toBe(5);
        expect(model.edges[0].evidence[0].source).toBe(graph.nodes[0]);
    });
    it('preserves source metadata and source navigation under a file filter', () => {
        const catalog = collectHotspots(findings, graph);
        const model = buildHotspotGraph(graph, catalog, 'busy.ts');
        expect(model.nodes).toHaveLength(1);
        expect(model.nodes[0]).toMatchObject({ label: 'busy', filePath: 'src/api/busy.ts', line: 10 });
        expect(model.edges).toEqual([]);
        expect(catalog.maxFanIn).toBe(100);
    });
    it('keeps actual folder positions stable when text filters or hotspot rank change', () => {
        const input: GraphData = { nodes: [node(1, 'busy', 'src/api/busy.ts'), node(2, 'complex', 'src/api/complex.ts'),
            node(3, 'save', 'src/store/save.ts'), node(4, 'render', 'apps/web/render.ts')], edges: graph.edges, total_nodes: 4 };
        const ranked = input.nodes.map((item, index) => ({ name: item.name, qualifiedName: item.qualified_name, fanIn: (index + 1) * 10 }));
        const full = buildHotspotGraph(input, collectHotspots(ranked, input));
        expect(full.platforms?.map(platform => platform.path).sort()).toEqual(['apps', 'apps/web', 'src', 'src/api', 'src/store']);
        for (const item of full.nodes) {
            const folder = full.platforms!.find(platform => platform.path === item.filePath!.slice(0, item.filePath!.lastIndexOf('/')))!;
            expect(Math.abs(item.position[0] - folder.position[0]) + 10).toBeLessThanOrEqual(folder.width / 2);
            expect(Math.abs(item.position[2] - folder.position[2]) + 10).toBeLessThanOrEqual(folder.depth / 2);
        }
        const filtered = buildHotspotGraph(input, collectHotspots(ranked, input), 'busy.ts');
        expect(filtered.platforms?.map(platform => platform.path)).toEqual(['src', 'src/api']);
        expect(filtered.nodes[0].position).toEqual(full.nodes.find(item => item.id === filtered.nodes[0].id)!.position);
        const reordered = buildHotspotGraph(input, collectHotspots(ranked.map((finding, index) => ({ ...finding, fanIn: (ranked.length - index) * 100 })), input));
        expect(reordered.nodes.map(item => item.id)).not.toEqual(full.nodes.map(item => item.id));
        reordered.nodes.forEach(item => expect(item.position).toEqual(full.nodes.find(original => original.id === item.id)!.position));
        expect(reordered.platforms).toEqual(full.platforms);
        expect(reordered.edges).toEqual(full.edges);
        expect(full.edges[0].evidence[0].source).toBe(input.nodes[0]);
    });
    it('retains the matching finding for an unresolved file while using its unfiltered placement', () => {
        const input: GraphData = { nodes: [], edges: [], total_nodes: 0 };
        const catalog = collectHotspots([{ name: 'first', filePath: 'src/api/file.ts', line: 2, fanIn: 100 },
            { name: 'second', filePath: 'src/api/file.ts', line: 50, complexity: 20 },
            { name: 'elsewhere', filePath: 'other/file.ts', fanIn: 5 }], input);
        const full = buildHotspotGraph(input, catalog);
        const filtered = buildHotspotGraph(input, catalog, 'second');
        expect(filtered.nodes).toHaveLength(1);
        expect(filtered.nodes[0]).toMatchObject({ line: 50, filePath: 'src/api/file.ts', graphNode: undefined });
        expect(filtered.nodes[0].position).toEqual(full.nodes.find(item => item.filePath === 'src/api/file.ts')!.position);
        expect(filtered.edges).toEqual([]);
    });
    it('does not guess a source file for an ambiguous qualified identity', () => {
        const ambiguous = { ...graph, nodes: [...graph.nodes, { ...graph.nodes[0], id: 11, file_path: 'other/busy.ts' }], total_nodes: 4 };
        const catalog = collectHotspots([findings[0]], ambiguous);
        expect(catalog.findings[0].filePath).toBeUndefined();
        expect(catalog.unmapped).toBe(1);
        expect(buildHotspotGraph(ambiguous, catalog).nodes[0].graphNode).toBeUndefined();
    });
    it('does not attach an ambiguous pathless finding to a separately resolved method', () => {
        const duplicate = { ...graph.nodes[0], id: 11, file_path: 'other/busy.ts' };
        const ambiguous = { ...graph, nodes: [...graph.nodes, duplicate], total_nodes: 4 };
        const explicit = { ...findings[0], filePath: 'src/api/busy.ts', fanIn: 5 };
        const catalog = collectHotspots([findings[0], explicit], ambiguous);
        const model = buildHotspotGraph(ambiguous, catalog);
        const resolved = model.nodes.find(node => node.graphNode?.id === 1)!;
        expect(hotspotsForNode(resolved, catalog)?.findings).toHaveLength(1);
        expect(hotspotsForNode(resolved, catalog)?.maxFanIn).toBe(5);
    });
    it('distinguishes an unknown group fan-in from a measured zero', () => {
        const catalog = collectHotspots([{ ...findings[1], fanIn: 0 }, { name: 'missing', filePath: 'other/missing.ts', complexity: 20 }], graph);
        expect(catalog.byFile.get('src/api/complex.ts')?.maxFanIn).toBe(0);
        expect(catalog.byFile.get('other/missing.ts')?.maxFanIn).toBeUndefined();
    });
});
