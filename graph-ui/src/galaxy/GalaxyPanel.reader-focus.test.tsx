// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel, { type GalaxyPanelProps } from './GalaxyPanel';
import type { GraphData } from './types';

vi.mock('./GraphScene', async importOriginal => ({
    ...await importOriginal<typeof import('./GraphScene')>(),
    GraphScene: ({ highlightedIds, data }: { highlightedIds: Set<number> | null; data: GraphData }) => <>
        <output data-testid="graph-selection">{JSON.stringify([...highlightedIds ?? []].sort((a, b) => a - b))}</output>
        <output data-testid="graph-data">{JSON.stringify(data)}</output>
    </>,
}));
let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});

it('opens a file hierarchy without a caret symbol and follows literal marked ranges', async () => {
    const common = { file_path: 'src/service.ts', x: 0, y: 0, z: 0, size: 1, color: '#999999' };
    const nodes = [
        { ...common, id: 10, name: 'service.ts', qualified_name: 'sample.file', label: 'File', start_line: 1, end_line: 50 },
        { ...common, id: 11, name: 'Service', qualified_name: 'sample.Service', label: 'Class', start_line: 2, end_line: 49 },
        { ...common, id: 12, name: 'read', qualified_name: 'sample.Service.read', label: 'Method', start_line: 5, end_line: 15 },
        { ...common, id: 13, name: 'write', qualified_name: 'sample.Service.write', label: 'Method', start_line: 20, end_line: 35 },
        { ...common, id: 14, name: 'isolated', qualified_name: 'sample.isolated', label: 'Function', start_line: 40, end_line: 45 },
        { ...common, id: 15, name: 'caller', qualified_name: 'sample.caller', label: 'Function', file_path: 'src/client.ts' },
        { ...common, id: 16, name: 'database', qualified_name: 'sample.database', label: 'Function', file_path: 'src/db.ts' },
        { ...common, id: 17, name: 'unrelated', qualified_name: 'sample.unrelated', label: 'Function', file_path: 'src/other.ts' },
    ];
    const edges = [
        { source: 10, target: 11, type: 'DEFINES' },
        { source: 11, target: 12, type: 'DEFINES_METHOD' },
        { source: 11, target: 13, type: 'DEFINES_METHOD' },
        { source: 15, target: 12, type: 'CALLS', line: 8 },
        { source: 12, target: 16, type: 'CALLS', line: 10 },
        { source: 13, target: 16, type: 'USES_TYPE' },
        { source: 16, target: 17, type: 'CALLS' },
    ];
    const fetchLayout = vi.fn(async () => new Response(JSON.stringify({ nodes, edges, total_nodes: 100 })));
    const props: GalaxyPanelProps = { project: 'sample', visible: true, onOpenNode: vi.fn(), fetch: fetchLayout,
        focusFilePath: 'src/service.ts' };
    const names = () => ((JSON.parse(host.querySelector('[data-testid="graph-data"]')!.textContent!) as GraphData)
        .nodes.map(node => node.qualified_name).sort());
    const relationships = () => {
        const data = JSON.parse(host.querySelector('[data-testid="graph-data"]')!.textContent!) as GraphData;
        const nameById = new Map(data.nodes.map(node => [node.id, node.qualified_name]));
        return data.edges.map(edge => `${nameById.get(edge.source)}>${nameById.get(edge.target)}:${edge.type}`).sort();
    };
    await act(async () => root.render(<GalaxyPanel {...props} />));
    await act(async () => host.querySelector<HTMLButtonElement>('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.click());
    expect(host.querySelector('.atlas-galaxy-placeholder')).toBeNull();
    const wholeFile = ['sample.file', 'sample.Service', 'sample.Service.read', 'sample.Service.write', 'sample.isolated', 'sample.caller', 'sample.database'].sort();
    expect(names()).toEqual(wholeFile);
    expect(relationships()).toContain('sample.Service.write>sample.database:USES_TYPE');
    expect(relationships()).not.toContain('sample.database>sample.unrelated:CALLS');
    expect(host.textContent).toContain('loaded graph');
    await act(async () => root.render(<GalaxyPanel {...props} focusSourceRange={{ startLine: 8, endLine: 10 }} />));
    expect(names()).toEqual(['sample.Service', 'sample.Service.read', 'sample.caller', 'sample.database'].sort());
    expect(relationships()).toEqual([
        'sample.Service>sample.Service.read:DEFINES_METHOD',
        'sample.Service.read>sample.database:CALLS',
        'sample.caller>sample.Service.read:CALLS',
    ].sort());
    await act(async () => root.render(<GalaxyPanel {...props} />));
    expect(names()).toEqual(wholeFile);
    expect(fetchLayout).toHaveBeenCalledOnce();
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); globalThis.__atlasGalaxy = undefined; });

it('selects the whole file, narrows only to marked code, and restores the file when the mark clears', async () => {
    const common = { file_path: 'src/a.ts', x: 0, y: 0, z: 0, size: 1, color: '#999999' };
    const nodes = [
        { ...common, id: 1, name: 'first', qualified_name: 'sample.first', label: 'Function', start_line: 2, end_line: 8 },
        { ...common, id: 2, name: 'second', qualified_name: 'sample.second', label: 'Function', start_line: 10, end_line: 20 },
        { ...common, id: 3, name: 'isolated', qualified_name: 'sample.isolated', label: 'Function', start_line: 25, end_line: 28 },
        { ...common, id: 4, name: 'a.ts', label: 'File', start_line: 1, end_line: 40 },
        { ...common, id: 5, name: 'owner', label: 'Class', start_line: 1, end_line: 30 },
        { ...common, id: 6, name: 'external', label: 'Function', file_path: 'src/b.ts', start_line: 1, end_line: 10 },
    ];
    const edges = [{ source: 1, target: 6, type: 'CALLS' }, { source: 5, target: 2, type: 'DEFINES_METHOD' }];
    const fetchLayout = vi.fn(async () => new Response(JSON.stringify({ nodes, edges, total_nodes: nodes.length })));
    const props: GalaxyPanelProps = { project: 'sample', visible: true, onOpenNode: vi.fn(), fetch: fetchLayout,
        focusFilePath: 'src/a.ts', focusQualifiedName: 'sample.first' };
    const selected = () => JSON.parse(host.querySelector('[data-testid="graph-selection"]')!.textContent!) as number[];
    await act(async () => root.render(<GalaxyPanel {...props} />));
    expect(selected()).toEqual([1, 2, 3, 4, 5]);
    await act(async () => root.render(<GalaxyPanel {...props} focusQualifiedName="sample.second" />));
    expect(selected()).toEqual([1, 2, 3, 4, 5]);
    await act(async () => root.render(<GalaxyPanel {...props} focusSourceRange={{ startLine: 12, endLine: 14 }} />));
    expect(selected()).toEqual([2]);
    await act(async () => root.render(<GalaxyPanel {...props} focusSourceRange={{ startLine: 6, endLine: 12 }} />));
    expect(selected()).toEqual([1, 2]);
    await act(async () => root.render(<GalaxyPanel {...props} />));
    expect(selected()).toEqual([1, 2, 3, 4, 5]);
    expect(fetchLayout).toHaveBeenCalledOnce();
});
