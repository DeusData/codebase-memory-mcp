// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { beforeEach, afterEach, expect, it, vi } from 'vitest';
import SpatialArchitecture from './SpatialArchitecture';
import { loadRouteGraph } from './route-graph-source';
import type { ArchitectureSceneProps } from './ArchitectureScene';
import type { GraphData, GraphNode } from '../galaxy/types';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';

vi.mock('./ArchitectureScene', () => ({ ArchitectureScene: (props: ArchitectureSceneProps) => <div data-testid="scene" data-active={String(props.active)}>{props.model.nodes.map(node => <button key={node.id} data-node={node.id} onClick={() => props.onSelect(node.id)}>{node.label}</button>)}<button aria-label="Empty background" onClick={props.onClearSelection} /></div> }));
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
    await click('Open area →'); await click('main.ts'); await click('Open file symbols →'); await click('start');
    expect(onSelect).toHaveBeenCalledExactlyOnceWith(node);
    expect(host.textContent).toContain('Analyze selected symbol');
    await click('Open source'); expect(onNavigate).toHaveBeenCalledWith('src/api/main.ts', 12, 'start');
});
it('returns from a file symbol to the repository map on empty background', async () => {
    const clear = vi.fn();
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={graph} overview={overview} view="overview" filter="" active onSelect={vi.fn()} onClearSelection={clear} onNavigate={vi.fn()} onView={vi.fn()} selectionPanel={<button>Analyze selected symbol</button>} />));
    await click('src/api'); await click('Open area →'); await click('main.ts'); await click('Open file symbols →'); await click('start');
    expect(host.textContent).toContain('Analyze selected symbol');
    await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Empty background"]')!.click());
    expect(host.querySelector('[data-testid="scene"]')?.textContent).toContain('src/api');
    expect(host.textContent).not.toContain('Analyze selected symbol');
    expect(clear).toHaveBeenCalledOnce();
});
it('summarizes the opened area in the inspector instead of the whole repository', async () => {
    const nested: GraphData = { nodes: [{ ...node, file_path: 'django/contrib/admin/options.py' }, { ...node, id: 2, name: 'render', qualified_name: 'sample.render', file_path: 'django/shortcuts.py' },
        { ...node, id: 3, name: 'setup', qualified_name: 'sample.setup', file_path: 'setup.py' }], edges: [], total_nodes: 3 };
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={nested} overview={overview} view="overview" filter="" active onNavigate={vi.fn()} onView={vi.fn()} />));
    const inspector = () => host.querySelector('[aria-label="Architecture inspector"]');
    expect(inspector()?.querySelector('h3')?.textContent).toBe('3 files');
    await click('django'); await click('Open area →');
    // Nothing is selected inside the opened area: the column speaks about django (2 of the 3 files), not the repository.
    expect(inspector()?.querySelector('.spatial-eyebrow')?.textContent).toBe('Opened area');
    expect(inspector()?.querySelector('h3')?.textContent).toBe('django');
    expect(inspector()?.textContent).toContain('2 files');
    expect(inspector()?.textContent).not.toContain('3 files');
});
it('opens nested areas level by level and walks back through the location trail', async () => {
    const nested: GraphData = { nodes: [{ ...node, file_path: 'django/contrib/admin/options.py' }, { ...node, id: 2, name: 'render', qualified_name: 'sample.render', file_path: 'django/shortcuts.py' }], edges: [], total_nodes: 2 };
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={nested} overview={overview} view="overview" filter="" active onNavigate={vi.fn()} onView={vi.fn()} />));
    const scene = () => [...host.querySelectorAll('[data-testid="scene"] [data-node]')].map(item => item.textContent);
    const trail = () => [...host.querySelectorAll('[aria-label="Architecture location"] button')].map(item => item.textContent);
    await click('django'); await click('Open area →');
    expect(scene()).toEqual(['contrib', 'shortcuts.py']);
    await click('contrib'); await click('Open area →');
    expect(scene()).toEqual(['admin']);
    expect(trail()).toEqual(['sample', 'django', 'contrib']);
    const django = [...host.querySelectorAll<HTMLButtonElement>('[aria-label="Architecture location"] button')].find(item => item.textContent === 'django')!;
    await act(async () => django.click());
    expect(scene()).toEqual(['contrib', 'shortcuts.py']);
});
it('publishes source-area evidence without inventing a symbol and clears it on background', async () => {
    const onSelectionEvidence = vi.fn();
    await act(async () => root.render(<SpatialArchitecture project="sample" generation="g1" graph={graph} overview={overview} view="overview" filter="" active onSelectionEvidence={onSelectionEvidence} onNavigate={vi.fn()} onView={vi.fn()} />));
    await click('src/api');
    const snapshot = JSON.parse(onSelectionEvidence.mock.lastCall![0].text).evidence;
    expect(snapshot.project).toBe('sample'); expect(snapshot.generation).toBe('g1');
    expect(snapshot.selected).toMatchObject({ kind: 'area', label: 'src/api', memberCount: 1 });
    expect(snapshot.selected.members[0]).toMatchObject({ qualifiedName: 'sample.start', filePath: 'src/api/main.ts' });
    expect(snapshot.limitations.interpretation).toContain('static references');
    await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Empty background"]')!.click());
    expect(onSelectionEvidence).toHaveBeenLastCalledWith(undefined);
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
it('groups endpoints, hides test routes by default and narrows a group through the filter', async () => {
    const route = (id: number, name: string, file_path: string): GraphNode => ({ id, label: 'Route', name, qualified_name: `sample.route.${id}`, file_path, x: 0, y: 0, z: 0, size: 1, color: '' });
    const input: GraphData = { nodes: [node, route(2, '/accounts/login/', 'app/urls.py'), route(3, '/accounts/logout/', 'app/urls.py'), route(4, '/fixture/', 'tests/urls.py'),
        route(5, '/accounts_old/', 'accounts/urls.py')], edges: [], total_nodes: 5 };
    const onFilter = vi.fn();
    const render = (filter: string) => act(async () => root.render(<SpatialArchitecture project="sample" graph={input} overview={overview} view="routes" filter={filter} active onNavigate={vi.fn()} onView={vi.fn()} onFilter={onFilter} />));
    await render('');
    const scene = () => [...host.querySelectorAll('[data-testid="scene"] [data-node]')].map(item => item.textContent);
    expect(scene()).toEqual(['/accounts · 2', '/accounts_old/']);
    const toggle = [...host.querySelectorAll('label')].find(label => label.textContent?.startsWith('Include test routes'))!;
    expect(toggle.textContent).toBe('Include test routes (1 hidden)');
    await click('/accounts · 2'); await click('Show these 2 routes →');
    expect(onFilter).toHaveBeenCalledExactlyOnceWith('/accounts');
    // The filter the group hands over shows its two routes, not every route whose text contains it.
    await render('/accounts');
    expect(scene()).toEqual(['/accounts/login/', '/accounts/logout/']);
    await render('');
    await act(async () => toggle.querySelector('input')!.click());
    expect(scene()).toEqual(['/accounts · 2', '/fixture/', '/accounts_old/']);
    expect(toggle.textContent).toBe('Include test routes');
});
it('says the test routes are still being checked while route evidence loads, then counts them', async () => {
    let finish!: (value: Awaited<ReturnType<typeof loadRouteGraph>>) => void;
    vi.mocked(loadRouteGraph).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
    const route = (id: number, name: string, file_path: string): GraphNode => ({ id, label: 'Route', name, qualified_name: `sample.route.${id}`, file_path, x: 0, y: 0, z: 0, size: 1, color: '' });
    const input: GraphData = { nodes: [node, route(2, '/accounts/login/', 'app/urls.py'), route(4, '/fixture/', 'tests/urls.py')], edges: [], total_nodes: 3 };
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={input} overview={overview} view="routes" filter="" active onNavigate={vi.fn()} onView={vi.fn()} />));
    const toggle = () => [...host.querySelectorAll('label')].find(label => label.textContent?.startsWith('Include test routes'))!.textContent;
    // Before the evidence arrives a count would be a guess that later jumps (1, then 155 in Django).
    expect(toggle()).toBe('Include test routes (checking…)');
    await act(async () => finish({ relationships: [], truncated: false, warnings: [] }));
    expect(toggle()).toBe('Include test routes (1 hidden)');
});
it('does not load route evidence while the workspace is hidden', async () => {
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={graph} overview={overview} view="routes" filter="" active={false} onNavigate={vi.fn()} onView={vi.fn()} />));
    expect(loadRouteGraph).not.toHaveBeenCalled();
});
it('explains the brick encodings and allows a uniform-height structural view', async () => {
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={graph} overview={overview} view="overview" filter="" active onNavigate={vi.fn()} onView={vi.fn()} />));
    const height = host.querySelector<HTMLSelectElement>('select[aria-label="Brick height"]');
    expect(height).not.toBeNull();
    expect(height?.value).toBe('lines');
    expect(height?.selectedOptions[0].textContent).toBe('Source size');
    expect(host.querySelector('select[aria-label="Brick color"]')).not.toBeNull();
    await act(async () => { height!.value = 'uniform'; height!.dispatchEvent(new Event('change', { bubbles: true })); });
    expect(height?.value).toBe('uniform');
    expect(height?.selectedOptions[0].textContent).toBe('Uniform');
});
it('keeps isolated and coverage-only files reachable through the inventory and visibility filter', async () => {
    const coverage = { records: new Map([['assets/logo.svg', { path: 'assets/logo.svg', kind: 'file' as const, state: 'not-indexed' as const, reason: 'excluded type', sources: ['coverage'] }]]),
        truncations: [], counts: { partial: 0, skipped: 0, notIndexedDirs: 0, notIndexedFiles: 1, scopeEntries: 1 } };
    const onNavigate = vi.fn();
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={graph} overview={{ ...overview, files: ['src/api/main.ts', 'empty/unused.ts'] }} coverage={coverage} view="overview" filter="" active onNavigate={onNavigate} onView={vi.fn()} />));
    expect(host.textContent).toContain('File inventory · 3 known files');
    expect(host.textContent).toContain('assets/logo.svg');
    const files = host.querySelector<HTMLSelectElement>('select[aria-label="File visibility"]')!;
    await act(async () => { files.value = 'connected'; files.dispatchEvent(new Event('change', { bubbles: true })); });
    expect(host.querySelector('.spatial-inventory-list')?.textContent).toBe('');
    await act(async () => { files.value = 'all'; files.dispatchEvent(new Event('change', { bubbles: true })); });
    const read = host.querySelector<HTMLButtonElement>('button[aria-label="Read assets/logo.svg"]')!;
    await act(async () => read.click());
    expect(onNavigate).toHaveBeenCalledWith('assets/logo.svg', 1);
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

it('focuses an aggregated hotspot area and restores all areas on empty background', async () => {
    const other: GraphNode = { ...node, id: 2, name: 'save', qualified_name: 'sample.save', file_path: 'src/store/save.ts' };
    const input: GraphData = { nodes: [node, other], edges: [{ source: 1, target: 2, type: 'CALLS' }], total_nodes: 2 };
    const data = { ...overview, hotspots: [{ name: 'start', qualifiedName: node.qualified_name, fanIn: 10 }, { name: 'save', qualifiedName: other.qualified_name, fanIn: 5 }] };
    await act(async () => root.render(<SpatialArchitecture project="sample" graph={input} overview={data} view="hotspots" filter="" active onNavigate={vi.fn()} onView={vi.fn()} />));
    expect(host.querySelector('[data-testid="scene"]')?.textContent).toContain('start');
    expect(host.querySelector('[data-testid="scene"]')?.textContent).toContain('save');
    const area = [...host.querySelectorAll<HTMLButtonElement>('[aria-label="Hotspots by source area"] button')].find(button => button.querySelector('strong')?.textContent === 'src/store')!;
    expect(area.textContent).toContain('1 outside dependent files');
    await act(async () => area.click());
    expect(host.querySelector('[data-testid="scene"]')?.textContent).not.toContain('start');
    expect(host.querySelector('[data-testid="scene"]')?.textContent).toContain('save');
    expect(host.querySelector('[aria-label="Hotspot area"]')?.textContent).toContain('src/store');
    await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Empty background"]')!.click());
    expect(host.querySelector('[data-testid="scene"]')?.textContent).toContain('start');
    expect(host.querySelector('[aria-label="Hotspot area"]')).toBeNull();
    expect(host.querySelector('input[type="search"]')).toBeNull();
});
/*
 * Hand test 2026-10-04 (A3): "Refresh connections" fetched everything again and nothing on screen changed. Now the
 * button says it runs, the map keeps the connections it shows meanwhile, and a status names the time and the result.
 */
it('A3: Refresh connections says that it runs and what it found, and keeps the map while it runs', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    try {
        vi.setSystemTime(new Date(2026, 9, 4, 13, 45, 12));
        const route: GraphNode = { id: 2, label: 'Route', name: '/api/', qualified_name: 'sample.route.2', file_path: 'app/urls.py', x: 0, y: 0, z: 0, size: 1, color: '' };
        const input: GraphData = { nodes: [node, route], edges: [], total_nodes: 2 };
        await act(async () => root.render(<SpatialArchitecture project="sample" graph={input} overview={overview} view="routes" filter="" active onNavigate={vi.fn()} onView={vi.fn()} />));
        const refresh = () => [...host.querySelectorAll('button')].find(button => /^Refresh(ing)? connections/.test(button.textContent ?? ''))!;
        const status = () => host.querySelector('[role="status"].atlas-refresh-status')?.textContent;
        expect(refresh().textContent).toBe('Refresh connections');
        expect(status()).toBe('');

        let finish!: (value: Awaited<ReturnType<typeof loadRouteGraph>>) => void;
        vi.mocked(loadRouteGraph).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
        await act(async () => refresh().click());
        expect(refresh().textContent).toBe('Refreshing connections…');
        expect(refresh().getAttribute('aria-disabled')).toBe('true');
        // The map keeps what it shows; it does not fall back to "reading" while the refresh runs.
        expect(host.textContent).not.toContain('Reading indexed endpoint connections…');
        expect(host.textContent).not.toContain('(checking…)');
        // A second press while it runs starts nothing.
        await act(async () => refresh().click());
        expect(loadRouteGraph).toHaveBeenCalledTimes(2);
        await act(async () => finish({ relationships: [], truncated: false, warnings: [] }));
        expect(refresh().textContent).toBe('Refresh connections');
        expect(refresh().getAttribute('aria-disabled')).toBeNull();
        expect(status()).toBe('Up to date at 13:45:12: no changes since the last load');

        vi.setSystemTime(new Date(2026, 9, 4, 13, 46, 3));
        vi.mocked(loadRouteGraph).mockResolvedValueOnce({ relationships: [{ source: route, target: node, type: 'HANDLES', id: 7 }], truncated: false, warnings: [] });
        await act(async () => refresh().click());
        expect(status()).toBe('Connections refreshed at 13:46:03');

        vi.mocked(loadRouteGraph).mockRejectedValueOnce(new Error('daemon unavailable'));
        await act(async () => refresh().click());
        expect(status()).toBe('Refresh failed at 13:46:03: daemon unavailable');
    } finally { vi.useRealTimers(); }
});
