// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { beforeEach, afterEach, expect, it, vi } from 'vitest';
import SpatialArchitecture from './SpatialArchitecture';
import { loadRouteGraph } from './route-graph-source';
import type { ArchitectureSceneProps } from './ArchitectureScene';
import type { GraphData, GraphNode } from '../galaxy/types';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';

vi.mock('./ArchitectureScene', () => ({ ArchitectureScene: (props: ArchitectureSceneProps) => <div data-testid="scene" data-active={String(props.active)}>{props.model.nodes.map(node => <button key={node.id} data-node={node.id} onClick={() => props.onSelect(node.id)}>{node.label}</button>)}</div> }));
vi.mock('./route-graph-source', () => ({ loadRouteGraph: vi.fn(async () => ({ relationships: [], truncated: false, warnings: [] })) }));

const node: GraphNode = { id: 1, label: 'Function', name: 'start', qualified_name: 'sample.start', file_path: 'src/api/main.ts', start_line: 12, end_line: 30, status: 'entry', x: 0, y: 0, z: 0, size: 1, color: '' };
const graph: GraphData = { nodes: [node], edges: [], total_nodes: 1 };
const overview: ArchitectureOverviewDto = { projectName: 'sample', totalSymbols: 1, totalRelations: 0, symbolKinds: [], relationKinds: [], languages: [], groups: [], boundaries: [], layers: [], clusters: [], entryPoints: [], routes: [], hotspots: [], files: [] };
let host: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.appendChild(host); root = createRoot(host);
    vi.clearAllMocks();
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });
async function click(text: string) {
    const button = [...host.querySelectorAll('button')].find(button => button.textContent === text);
    expect(button).toBeDefined(); await act(async () => button!.click());
}
it('drills inferred areas to files and symbols before enabling real-node impact actions', async () => {
    const onSelect = vi.fn(); const onNavigate = vi.fn();
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={graph} overview={overview} view="overview" filter="" active onSelect={onSelect} onNavigate={onNavigate} onView={vi.fn()} selectionPanel={<button>Analyze selected symbol</button>} />));
    await click('src/api');
    expect(onSelect).not.toHaveBeenCalled();
    expect(host.textContent).not.toContain('Analyze selected symbol');
    await click('Open area →'); await click('src/api/main.ts'); await click('Open file symbols →'); await click('start');
    expect(onSelect).toHaveBeenCalledExactlyOnceWith(node);
    expect(host.textContent).toContain('Analyze selected symbol');
    await click('Open source'); expect(onNavigate).toHaveBeenCalledWith('src/api/main.ts', 12, 'start');
});
it('passes hidden-workspace suspension to the scene and preserves the scene across view switches', async () => {
    const props = { project: 'sample', graph, overview, filter: '', onNavigate: vi.fn(), onView: vi.fn() };
    await act(async () => root.render(<SpatialArchitecture {...props} view="overview" active />));
    const scene = host.querySelector('[data-testid="scene"]');
    await act(async () => root.render(<SpatialArchitecture {...props} view="entryPoints" active={false} />));
    expect(host.querySelector('[data-testid="scene"]')).toBe(scene);
    expect(scene?.getAttribute('data-active')).toBe('false');
});
it('cancels a route request on generation change and ignores its late result', async () => {
    let finishOld!: (value: Awaited<ReturnType<typeof loadRouteGraph>>) => void;
    vi.mocked(loadRouteGraph).mockImplementationOnce(() => new Promise(resolve => { finishOld = resolve; }));
    const props = { project: 'sample', graph, overview, filter: '', onNavigate: vi.fn(), onView: vi.fn() };
    await act(async () => root.render(<SpatialArchitecture {...props} view="routes" active generation="old" />));
    const oldSignal = vi.mocked(loadRouteGraph).mock.calls[0][1]?.signal;
    await act(async () => root.render(<SpatialArchitecture {...props} view="routes" active generation="new" />));
    expect(oldSignal?.aborted).toBe(true);
    await act(async () => finishOld({ relationships: [], truncated: true, warnings: ['STALE RESPONSE'] }));
    expect(host.textContent).not.toContain('STALE RESPONSE');
    expect(loadRouteGraph).toHaveBeenCalledTimes(2);
});
it('does not load route evidence while the workspace is hidden', async () => {
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={graph} overview={overview} view="routes" filter="" active={false} onNavigate={vi.fn()} onView={vi.fn()} />));
    expect(loadRouteGraph).not.toHaveBeenCalled();
});
it('keeps the chosen entry identity across reindexing when numeric IDs are reused', async () => {
    const second = { ...node, id: 2, name: 'other', qualified_name: 'sample.other' };
    const props = { project: 'sample', overview, filter: '', onNavigate: vi.fn(), onView: vi.fn() };
    await act(async () => root.render(<SpatialArchitecture {...props} graph={{ nodes: [node, second], edges: [], total_nodes: 2 }} generation="old" view="entryPoints" active />));
    const select = host.querySelector<HTMLSelectElement>('select[aria-label="Entry point"]')!;
    await act(async () => { select.value = '2'; select.dispatchEvent(new Event('change', { bubbles: true })); });
    await act(async () => root.render(<SpatialArchitecture {...props} graph={{ nodes: [{ ...node, id: 2 }, { ...second, id: 101 }], edges: [], total_nodes: 2 }} generation="new" view="entryPoints" active />));
    expect(select.value).toBe('101');
    expect(host.querySelector('[data-testid="scene"]')?.textContent).toBe('other');
});
