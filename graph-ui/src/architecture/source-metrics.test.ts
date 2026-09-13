import { describe, expect, it } from 'vitest';
import type { GraphData, GraphNode } from '../galaxy/types';
import { buildSemanticGraph } from './semantic-graph';
import { collectSourceMetrics, measureSourceNode, sourceBrickHeight, sourceLanguage, languageColor, sourceNodeSizePercent } from './source-metrics';

const node = (id: number, file_path: string, label: string, start_line?: number, end_line?: number): GraphNode => ({
    id, file_path, label, start_line, end_line, name: `node${id}`, x: 0, y: 0, z: 0, size: 1, color: '',
});
const graph: GraphData = { nodes: [node(1, 'src/api/a.ts', 'File', 0, 0), node(2, 'src/api/a.ts', 'Module', 1, 100),
    node(3, 'src/api/a.ts', 'Class', 2, 95), node(4, 'src/api/a.ts', 'Method', 10, 80),
    node(5, 'src/api/b.py', 'Module', 1, 300), node(6, 'src/api/b.py', 'Module', 1, 300),
    node(7, 'src/store/save.go', 'Module', 1, 600), node(8, 'README.md', 'File', 0, 0)],
    edges: [{ source: 4, target: 7, type: 'CALLS' }], total_nodes: 8 };

describe('source size and language encoding', () => {
    it('counts each whole-file module extent once without adding nested or duplicate spans', () => {
        const catalog = collectSourceMetrics(graph);
        expect(catalog.total).toMatchObject({ lines: 1000, measuredFiles: 3, files: 4 });
        expect(catalog.areas.get('src/api')).toMatchObject({ lines: 400, measuredFiles: 2, files: 2 });
        expect(catalog.areas.get('src/api')?.languages.map(language => [language.name, language.lines])).toEqual([['Python', 300], ['TypeScript', 100]]);
        expect(catalog.total.languages.reduce((sum, language) => sum + language.lines, 0)).toBe(1000);
    });
    it('keeps unknown file sizes and unknown language distinct from zero', () => {
        const catalog = collectSourceMetrics(graph, ['assets/custom.undocumented']);
        expect(catalog.files.get('README.md')?.lines).toBeUndefined();
        expect(catalog.files.get('assets/custom.undocumented')?.language).toBe('Unknown');
        expect(catalog.partial).toBe(true);
        expect(sourceBrickHeight(undefined, 1000)).not.toBe(sourceBrickHeight(0, 1000));
        const onlySymbols = collectSourceMetrics({ nodes: [node(1, 'file.ts', 'Function', 1, 999)], edges: [], total_nodes: 1 });
        expect(onlySymbols.total.lines).toBeUndefined();
    });
    it('normalizes file identity and uses the longest complete module range deterministically', () => {
        const input: GraphData = { nodes: [node(1, './src/api/a.ts', 'Module', 1, 90), node(2, 'src\\api\\a.ts', 'Module', 1, 100),
            node(3, 'src/api/a.ts', 'Module', 8, 800)], edges: [], total_nodes: 3 };
        expect(collectSourceMetrics(input).total).toMatchObject({ lines: 100, files: 1 });
        expect(collectSourceMetrics(input)).toEqual(collectSourceMetrics({ ...input, nodes: [...input.nodes].reverse() }));
    });
    it('keeps measurements, language colors and scale stable under scene caps, filters and drilldown', () => {
        const catalog = collectSourceMetrics(graph);
        const overview = buildSemanticGraph(graph, { view: 'overview' });
        const filtered = buildSemanticGraph(graph, { view: 'dependencies', filter: 'src/api', maxNodes: 1 });
        const area = overview.nodes.find(node => node.areaPath === 'src/api')!;
        expect(measureSourceNode(filtered.nodes[0], catalog)).toEqual(measureSourceNode(area, catalog));
        expect(sourceNodeSizePercent(filtered.nodes[0], catalog)).toBe(sourceNodeSizePercent(area, catalog));
        const detail = buildSemanticGraph(graph, { view: 'overview', areaPath: 'src/api' });
        expect(measureSourceNode(detail.nodes.find(node => node.filePath === 'src/api/a.ts')!, catalog)?.lines).toBe(100);
        expect(catalog.referenceLines).toBe(600);
        expect(catalog.referenceKind).toBe('folder');
        expect(catalog.referencePath).toBe('src/store');
        expect(languageColor('TypeScript')).toBe(languageColor(sourceLanguage('other/project/test.tsx')));
    });
    it('compresses million-line outliers into finite bounded monotonic heights', () => {
        const heights = [0, 1, 100, 10000, 1000000].map(lines => sourceBrickHeight(lines, 1000000));
        heights.forEach((height, index) => { expect(Number.isFinite(height)).toBe(true); expect(height).toBeGreaterThanOrEqual(1.5); expect(height).toBeLessThanOrEqual(14); if (index) expect(height).toBeGreaterThan(heights[index - 1]); });
        expect(sourceBrickHeight(1e12, 1000000)).toBe(14);
        expect(sourceBrickHeight(NaN, 100)).toBe(sourceBrickHeight(undefined, 100));
        expect(Number.isFinite(sourceBrickHeight(100, Infinity))).toBe(true);
        expect(sourceBrickHeight(Number.MAX_VALUE, 1)).toBe(14);
        expect(sourceBrickHeight(-1, 100)).toBe(sourceBrickHeight(undefined, 100));
    });
    it('gives one, ten and one hundred percent sizes a perceptible bounded height spread', () => {
        const heights = [100, 1000, 10000].map(lines => sourceBrickHeight(lines, 10000));
        expect(heights[0]).toBeCloseTo(2.75);
        expect(heights[1]).toBeCloseTo(1.5 + 12.5 * Math.sqrt(0.1));
        expect(heights[2]).toBe(14);
        expect(heights[1] - heights[0]).toBeGreaterThan(2);
        expect(heights[2] - heights[1]).toBeGreaterThan(8);
        expect(sourceBrickHeight(0, 10000)).toBe(1.5);
    });
    it('uses the largest non-root folder as 100 percent despite large root-direct files', () => {
        const input: GraphData = { nodes: [node(1, 'root.ts', 'Module', 1, 1000000), node(2, 'other.ts', 'Module', 1, 2000000),
            node(3, 'src/large/a.ts', 'Module', 1, 800), node(4, 'src/small/a.ts', 'Module', 1, 333)], edges: [], total_nodes: 4 };
        const catalog = collectSourceMetrics(input);
        const overview = buildSemanticGraph(input, { view: 'overview' });
        const area = (path: string) => overview.nodes.find(item => item.areaPath === path)!;
        expect(catalog.referenceLines).toBe(800);
        expect(catalog.referencePath).toBe('src/large');
        expect(catalog.referenceKind).toBe('folder');
        expect(sourceNodeSizePercent(area('src/large'), catalog)).toBe(100);
        expect(sourceNodeSizePercent(area('src/small'), catalog)).toBe(41.625);
        expect(sourceNodeSizePercent(area('(root)'), catalog)).toBe(0);
        expect(measureSourceNode(area('(root)'), catalog)?.lines).toBe(3000000);
        expect(catalog.total.lines).toBe(3001133);
        const file = buildSemanticGraph(input, { view: 'overview', areaPath: 'src/small' }).nodes.find(item => item.kind === 'file')!;
        expect(sourceNodeSizePercent(file, catalog)).toBe(41.625);
        const rootFile = buildSemanticGraph(input, { view: 'overview', areaPath: '(root)' }).nodes.find(item => item.kind === 'file')!;
        expect(sourceNodeSizePercent(rootFile, catalog)).toBe(100);
    });
    it('uses an explicit largest-file fallback for a root-only repository without changing root measurements', () => {
        const input: GraphData = { nodes: [node(1, 'big.ts', 'Module', 1, 800), node(2, 'small.py', 'Module', 1, 333)], edges: [], total_nodes: 2 };
        const catalog = collectSourceMetrics(input);
        const root = buildSemanticGraph(input, { view: 'overview' }).nodes[0];
        expect(catalog.referenceKind).toBe('file');
        expect(catalog.referencePath).toBe('big.ts');
        expect(catalog.referenceLines).toBe(800);
        expect(catalog.areas.get('(root)')?.lines).toBe(1133);
        expect(sourceNodeSizePercent(root, catalog)).toBe(0);
        const files = buildSemanticGraph(input, { view: 'overview', areaPath: '(root)' });
        expect(sourceNodeSizePercent(files.nodes.find(item => item.filePath === 'small.py')!, catalog)).toBe(41.625);
    });
    it('leaves unknown non-root sizes undefined and preserves the root baseline with no measurements', () => {
        const input: GraphData = { nodes: [node(1, 'src/api/a.ts', 'Function', 1, 99), node(2, 'README.md', 'File')], edges: [], total_nodes: 2 };
        const catalog = collectSourceMetrics(input);
        const overview = buildSemanticGraph(input, { view: 'overview' });
        expect(catalog.referencePath).toBeUndefined();
        expect(catalog.referenceKind).toBe('file');
        expect(catalog.referenceLines).toBe(1);
        expect(sourceNodeSizePercent(overview.nodes.find(item => item.areaPath === 'src/api')!, catalog)).toBeUndefined();
        expect(sourceNodeSizePercent(overview.nodes.find(item => item.areaPath === '(root)')!, catalog)).toBe(0);
        const file = buildSemanticGraph(input, { view: 'overview', areaPath: 'src/api' }).nodes[0];
        expect(sourceNodeSizePercent(file, catalog)).toBeUndefined();
    });
    it('resolves equal-size reference ties by path independently of graph row order', () => {
        const input: GraphData = { nodes: [node(1, 'src/z/a.ts', 'Module', 1, 200), node(2, 'src/a/z.ts', 'Module', 1, 200)], edges: [], total_nodes: 2 };
        expect(collectSourceMetrics(input).referencePath).toBe('src/a');
        expect(collectSourceMetrics({ ...input, nodes: [...input.nodes].reverse() }).referencePath).toBe('src/a');
        const rootOnly: GraphData = { nodes: [node(1, 'z.ts', 'Module', 1, 200), node(2, 'a.ts', 'Module', 1, 200)], edges: [], total_nodes: 2 };
        expect(collectSourceMetrics(rootOnly).referencePath).toBe('a.ts');
        expect(collectSourceMetrics({ ...rootOnly, nodes: [...rootOnly.nodes].reverse() }).referencePath).toBe('a.ts');
    });
    it('classifies file types without guessing ambiguous header or MATLAB/Objective-C files', () => {
        expect(sourceLanguage('types.H')).toBe('C / C++');
        expect(sourceLanguage('plugin.m')).toBe('Objective-C / MATLAB');
        expect(sourceLanguage('Dockerfile.dev')).toBe('Dockerfile');
        expect(sourceLanguage('source.C')).toBe('C++');
        expect(sourceLanguage('LICENSE')).toBe('Unknown');
    });
    it('distinguishes real connections, isolated files and missing graph evidence', () => {
        const catalog = collectSourceMetrics(graph, ['outside/not-loaded.ts']);
        expect(catalog.files.get('src/api/a.ts')?.connection).toBe('connected');
        expect(catalog.files.get('src/api/b.py')?.connection).toBe('none');
        expect(catalog.files.get('outside/not-loaded.ts')?.connection).toBe('unknown');
        const partial = collectSourceMetrics({ ...graph, total_nodes: 99 });
        expect(partial.files.get('src/api/b.py')?.connection).toBe('unknown');
        expect(partial.files.get('src/api/a.ts')?.connection).toBe('connected');
    });
});
