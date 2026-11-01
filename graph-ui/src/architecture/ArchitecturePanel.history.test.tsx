// @vitest-environment jsdom
/*
 * Handtest K27: ein Zurueck und ein Vor fuer ganz Architecture, mit demselben
 * Verlaufsmodell wie Galaxy (K2). Die Szenen sind Attrappen mit Knoepfen fuer
 * Auswahl, Doppelklick und leere Flaeche; Analyse und Routen kommen aus
 * festen Antworten.
 */
import { act, type ReactNode } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ArchitecturePanel, { type ArchitecturePanelProps } from './ArchitecturePanel';
import type { ArchitectureSceneProps } from './ArchitectureScene';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';
import type { GraphData, GraphNode } from '../galaxy/types';
import type { SystemSceneModel } from './system-architecture-model';
import type { SystemArchitectureLoader, SystemProjection, SystemSymbol } from './system-architecture-source';

vi.mock('./ArchitectureScene', () => ({ ArchitectureScene: (props: ArchitectureSceneProps) => <div data-testid="scene">
    {props.model.nodes.map(node => <button key={node.id} data-node={node.id} onClick={() => props.onSelect(node.id)} onDoubleClick={() => props.onOpen?.(node.id)}>{node.label}</button>)}
    <button aria-label="Empty background" onClick={props.onClearSelection} /></div> }));
vi.mock('./ContainerMap', () => ({ default: ({ filter, navigation }: { filter: string; navigation?: ReactNode }) =>
    <div data-testid="container-map" data-filter={filter}><div className="atlas-arch-context" data-testid="container-location">{navigation}</div></div> }));
vi.mock('./route-graph-source', () => ({ loadRouteGraph: vi.fn(async () => ({ relationships: [], truncated: false, warnings: [] })) }));
vi.mock('./BehaviorSourceEvidence', () => ({ default: () => <div /> }));
vi.mock('../app/atlas-api', () => ({ AtlasApi: class { flows() { return Promise.resolve([]); } } }));
vi.mock('./SystemArchitectureScene', () => ({ default: ({ model, onSelectNode, onExpandNode, onClearSelection, planar }: {
    model: SystemSceneModel; onSelectNode: (id: string) => void; onExpandNode?: (id: string) => void; onClearSelection?: () => void; planar?: boolean;
}) => <div data-testid="system-scene" data-focus={model.focusId ?? ''} data-planar={String(Boolean(planar))}><button onClick={onClearSelection}>System background</button>
    {model.nodes.map(node => <button key={node.id} data-system-node={node.id} onClick={() => onSelectNode(node.id)} onDoubleClick={() => onExpandNode?.(node.id)}>{node.label}</button>)}</div> }));

const graphNode = (id: number, name: string, file_path: string, label = 'Function'): GraphNode =>
    ({ id, label, name, qualified_name: `sample.${name}`, file_path, start_line: 3, x: 0, y: 0, z: 0, size: 1, color: '' });
const graph: GraphData = { nodes: [graphNode(1, 'render', 'django/shortcuts.py'), graphNode(2, 'options', 'django/contrib/admin/options.py'),
    graphNode(3, 'setup', 'setup.py'), graphNode(10, '/edit/one/', 'app/urls.py', 'Route'), graphNode(11, '/edit/two/', 'app/urls.py', 'Route'),
    graphNode(12, '/home/', 'app/urls.py', 'Route')], edges: [], total_nodes: 6 };
const overview: ArchitectureOverviewDto = { projectName: 'sample', totalSymbols: 6, totalRelations: 0, symbolKinds: [], relationKinds: [], languages: [],
    groups: [], boundaries: [], layers: [], clusters: [], entryPoints: [], routes: [], hotspots: [], files: ['django/shortcuts.py', 'django/contrib/admin/options.py', 'setup.py', 'app/urls.py'] };

const symbol = (id: number, component: string, name: string): SystemSymbol => ({
    id, name, qualified_name: `sample.${name}`, label: 'Function', file_path: `src/${component}.ts`, start_line: id * 10, component_id: component, group_id: `g:${component}`,
});
function projection(): SystemProjection {
    const main = symbol(1, 'api', 'main'), handle = symbol(2, 'service', 'handle'), save = symbol(3, 'store', 'save'), commit = symbol(4, 'store', 'commit');
    const components = [main, handle, save].map(item => ({ id: item.component_id, label: item.component_id, basis: 'interaction_community', member_count: 4, file_count: 1, representatives: [item], group_id: `g:${item.component_id}` }));
    const dependencies = [{ source: 'api', target: 'service', type: 'CALLS', count: 2, witnesses: [{ edge_id: 10, source: main, target: handle }] },
        { source: 'service', target: 'store', type: 'CALLS', count: 1, witnesses: [{ edge_id: 11, source: handle, target: save }] }];
    return {
        schema_version: 1, status: 'ready', kind: 'static_projection', complete: true, components, dependencies, cycles: [], entrypoints: [main, handle],
        paths: [{ entrypoint_id: 1, nodes: [main, handle, save], edges: [{ id: 10, source_id: 1, target_id: 2, type: 'CALLS' }, { id: 11, source_id: 2, target_id: 3, type: 'CALLS' }] },
            { entrypoint_id: 2, nodes: [handle, save], edges: [{ id: 11, source_id: 2, target_id: 3, type: 'CALLS' }] },
            { entrypoint_id: 3, nodes: [save, commit], edges: [{ id: 12, source_id: 3, target_id: 4, type: 'CALLS' }] }],
        totals: { nodes: 12, files: 3, edges: 3, accounted_nodes: 12, structural_nodes: 0 }, limits: {}, warnings: [],
        overview: { complete: true, grouping_basis: 'common_source_directory_aggregate', components,
            groups: components.map(item => ({ id: `g:${item.id}`, label: item.label, component_count: 1, member_count: 4, file_count: 1, component_ids: [item.id], representatives: item.representatives })),
            connections: dependencies.map(edge => ({ ...edge, source: `g:${edge.source}`, target: `g:${edge.target}` })), totals: { groups: 3, components: 3 }, limits: {} },
    };
}
/** main reaches save on two indexed paths: through handle, and directly. */
function twoPaths(): SystemProjection {
    const data = projection();
    const [main, , save] = data.paths[0]!.nodes;
    data.paths = [...data.paths, { entrypoint_id: 1, nodes: [main!, save!], edges: [{ id: 13, source_id: 1, target_id: 3, type: 'CALLS' }] }];
    return data;
}

let container: HTMLDivElement;
let root: Root;
let storageDescriptor: PropertyDescriptor | undefined;
let loader: ReturnType<typeof vi.fn<SystemArchitectureLoader>>;

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
    storageDescriptor = Object.getOwnPropertyDescriptor(window, 'localStorage');
    const contents = new Map<string, string>();
    Object.defineProperty(window, 'localStorage', { configurable: true, value: {
        getItem: (key: string) => contents.get(key) ?? null, setItem: (key: string, value: string) => { contents.set(key, value); },
    } });
    loader = vi.fn<SystemArchitectureLoader>().mockImplementation(async () => ({ status: 'ready', generation: 'g1', result: projection() }));
});
afterEach(async () => {
    await act(async () => root.unmount()); container.remove();
    if (storageDescriptor) Object.defineProperty(window, 'localStorage', storageDescriptor);
    else delete (window as unknown as Record<string, unknown>).localStorage;
});

async function render(changes: Partial<ArchitecturePanelProps> = {}): Promise<void> {
    await act(async () => root.render(<ArchitecturePanel projectName="sample" overview={overview} graph={graph} graphGeneration="g1"
        systemArchitectureLoader={loader} onNavigate={vi.fn()} active {...changes} />));
    await settle();
}
async function settle(): Promise<void> {
    for (let round = 0; round < 4; round++) await act(async () => { await vi.dynamicImportSettled(); await new Promise(resolve => setTimeout(resolve, 0)); });
}
const buttons = (scope: ParentNode = container) => [...scope.querySelectorAll<HTMLButtonElement>('button')];
const byText = (text: string, scope?: ParentNode) => buttons(scope).find(button => button.textContent === text);
async function press(target: HTMLElement | null | undefined): Promise<void> {
    expect(target).toBeTruthy();
    await act(async () => target!.click()); await settle();
}
const clickText = (text: string, scope?: ParentNode) => press(byText(text, scope));
async function doubleClick(selector: string): Promise<void> {
    const target = container.querySelector(selector); expect(target).not.toBeNull();
    await act(async () => target!.dispatchEvent(new MouseEvent('dblclick', { bubbles: true }))); await settle();
}
async function choose(label: string, value: string): Promise<void> {
    const select = container.querySelector<HTMLSelectElement>(`select[aria-label="${label}"]`); expect(select).not.toBeNull();
    await act(async () => { select!.value = value; select!.dispatchEvent(new Event('change', { bubbles: true })); }); await settle();
}
async function type(value: string): Promise<void> {
    const search = container.querySelector<HTMLInputElement>('input[type="search"]')!;
    await act(async () => {
        Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(search, value);
        search.dispatchEvent(new Event('input', { bubbles: true }));
    });
}
const history = () => container.querySelector<HTMLElement>('[role="group"][aria-label="Architecture history"]');
const back = () => history()?.querySelector<HTMLButtonElement>('button[aria-label="Back"]') ?? null;
const forward = () => history()?.querySelector<HTMLButtonElement>('button[aria-label="Forward"]') ?? null;
const goBack = () => press(back());
const goForward = () => press(forward());
const view = () => container.querySelector('.atlas-arch-tab[aria-pressed="true"]')?.getAttribute('data-view');
const trail = () => [...container.querySelectorAll('[aria-label="Architecture location"] button')].map(item => item.textContent);
const planar = () => container.querySelector('[aria-label="Map camera"] button[aria-pressed="true"]')?.textContent;
const perspective = () => container.querySelector('[aria-label="Routes perspective"] button[aria-pressed="true"]')?.textContent;
const filter = () => container.querySelector<HTMLInputElement>('input[type="search"]')?.value;
const focus = () => container.querySelector('[data-testid="system-scene"]')?.getAttribute('data-focus');
const heading = () => container.querySelector('.behavior-heading h2')?.textContent;
const sceneLabels = () => [...container.querySelectorAll('[data-testid="scene"] [data-node]')].map(item => item.textContent);
/** System structure and Behavior use the shared history rather than another local Back. */
const inViewBack = () => buttons().find(button => button.textContent === '← Back' && !history()?.contains(button));
const systemCamera = () => container.querySelector('[aria-label="System camera"] button[aria-pressed="true"]')?.textContent;
const behaviorCamera = () => container.querySelector('[aria-label="Behavior camera"] button[aria-pressed="true"]')?.textContent;
const chainPosition = () => container.querySelector('[aria-label="Walk the call chain"] span')?.textContent;
const pathButtons = () => [...container.querySelectorAll<HTMLButtonElement>('[aria-label="Indexed paths"] button')];
const chosenPath = () => pathButtons().findIndex(button => button.getAttribute('aria-pressed') === 'true') + 1;
const lastEvidence = (listener: ReturnType<typeof vi.fn>) => listener.mock.lastCall?.[0] as { label: string; text: string } | undefined;
const key = (init: KeyboardEventInit, target: EventTarget = window) => {
    const event = new KeyboardEvent('keydown', { bubbles: true, cancelable: true, ...init });
    act(() => { target.dispatchEvent(event); });
    return event;
};
async function openArea(name: string): Promise<void> {
    await press(container.querySelector<HTMLButtonElement>(`[data-testid="scene"] [data-node="area:${name}"]`) ?? byText(name, container.querySelector('[data-testid="scene"]')!));
    await clickText('Open area →');
}
const selectView = (name: string) => press(container.querySelector<HTMLButtonElement>(`[data-view="${name}"]`));
const position = () => history()?.getAttribute('data-position');
async function openRouteGroup(): Promise<void> {
    await press(byText('/edit · 2', container.querySelector('[data-testid="scene"]')!));
    await clickText('Show these 2 routes →');
}

describe('Architecture back and forward (K27)', () => {
    it('restores meaningful visits across views, with each visit’s camera and location', async () => {
        await render();
        expect(back()?.disabled).toBe(true);
        expect(back()?.title).toBe('Nothing to go back to yet');
        expect(forward()?.disabled).toBe(true);
        await openArea('django');
        expect(trail()).toEqual(['sample', 'django']);
        await clickText('Plan', container.querySelector('[aria-label="Map camera"]')!);
        expect(position()).toBe('2/2');
        expect(back()?.title).toBe('Back to Overview (Alt+Left)');
        await doubleClick('[data-node="file:django/shortcuts.py"]');
        expect(container.querySelector('[aria-label="Architecture location"]')?.textContent).toContain('shortcuts.py');
        expect(position()).toBe('3/3');
        expect(back()?.title).toBe('Back to Overview · django · Plan (Alt+Left)');
        await selectView('routes');
        await clickText('Endpoints', container.querySelector('[aria-label="Routes perspective"]')!);
        expect(position()).toBe('3/3');
        await openRouteGroup();
        expect(position()).toBe('4/4');

        await goBack();
        expect([view(), perspective(), filter()]).toEqual(['routes', 'Endpoints', '']);
        await goBack();
        expect([view(), trail(), planar()]).toEqual(['overview', ['sample', 'django'], 'Plan']);
        expect(forward()?.title).toBe('Forward to Routes · Endpoints · Plan (Alt+Right)');
        await goBack();
        expect([trail(), planar()]).toEqual([['sample'], '3D']);
        expect(back()?.disabled).toBe(true);
        await goForward(); await goForward(); await goForward();
        expect([view(), perspective(), filter(), planar()]).toEqual(['routes', 'Endpoints', '/edit', 'Plan']);
        expect(forward()?.disabled).toBe(true);

        await goBack(); await goBack();
        await press(byText('sample', container.querySelector('[aria-label="Architecture location"]')!));
        expect(trail()).toEqual(['sample']);
        expect(back()?.title).toBe('Back to Overview · django · Plan (Alt+Left)');
        expect(forward()?.disabled).toBe(true);
    });

    it('replaces view and Routes perspective context while route-group drill-ins create stops', async () => {
        await render();
        await selectView('routes');
        expect(perspective()).toBe('Service map');
        expect(container.querySelector('[data-testid="container-map"]')).not.toBeNull();
        await clickText('Endpoints', container.querySelector('[aria-label="Routes perspective"]')!);
        expect(sceneLabels()).toContain('/edit · 2');
        expect(position()).toBe('1/1');
        expect(back()?.disabled).toBe(true);
        await openRouteGroup();
        expect(filter()).toBe('/edit');
        expect(sceneLabels()).toEqual(['/edit/one/', '/edit/two/']);
        expect(back()?.title).toBe('Back to Routes · Endpoints (Alt+Left)');

        await goBack();
        expect([view(), perspective(), filter()]).toEqual(['routes', 'Endpoints', '']);
        expect(back()?.disabled).toBe(true);
        await clickText('Service map', container.querySelector('[aria-label="Routes perspective"]')!);
        expect(position()).toBe('1/2');
        expect(container.querySelector('[data-testid="container-map"]')).not.toBeNull();
        expect(forward()?.disabled).toBe(false);
        await goForward();
        expect([view(), perspective(), filter()]).toEqual(['routes', 'Endpoints', '/edit']);
        expect(sceneLabels()).toEqual(['/edit/one/', '/edit/two/']);
        await goBack();
        expect([perspective(), filter()]).toEqual(['Service map', '']);
    });

    it('saves every typed filter immediately without adding stops', async () => {
        await render();
        await selectView('routes');
        await type('/e'); await type('/ed'); await type('/edit');
        expect(filter()).toBe('/edit');
        expect(position()).toBe('1/1');
        expect(back()?.disabled).toBe(true);
        expect(forward()?.disabled).toBe(true);
        await type('');
        await clickText('Endpoints', container.querySelector('[aria-label="Routes perspective"]')!);
        await openRouteGroup();
        await type('/edit/one');
        expect(position()).toBe('2/2');
        await goBack();
        expect(filter()).toBe('');
        expect(forward()?.title).toBe('Forward to Routes · Endpoints · /edit/one (Alt+Right)');
        await goForward();
        expect(filter()).toBe('/edit/one');
        expect(position()).toBe('2/2');
    });

    it('records System structure focus and restores expanded groups without expansion-only stops', async () => {
        await render();
        await selectView('structure');
        expect(focus()).toBe('');
        expect(inViewBack()).toBeUndefined();
        expect(back()?.disabled).toBe(true);
        await doubleClick('[data-system-node="g:api"]');
        expect(focus()).toBe('g:api');
        expect(container.querySelector('[data-system-node="member:g:api:api"]')).not.toBeNull();
        expect(back()?.title).toBe('Back to System structure (Alt+Left)');
        await press(container.querySelector<HTMLButtonElement>('[data-system-node="g:api"]'));
        await clickText('Collapse group');
        expect(position()).toBe('2/2');
        expect(container.querySelector('[data-system-node="member:g:api:api"]')).toBeNull();
        await clickText('Expand group');
        expect(position()).toBe('2/2');
        await press(container.querySelector<HTMLButtonElement>('[data-system-node="g:service"]'));
        await clickText('Focus here');
        expect(focus()).toBe('g:service');
        expect(back()?.title).toBe('Back to System structure · api · 1 group open (Alt+Left)');
        expect(inViewBack()).toBeUndefined();

        await goBack();
        expect(focus()).toBe('g:api');
        expect(container.querySelector('[data-system-node="member:g:api:api"]')).not.toBeNull();
        await goBack();
        expect(focus()).toBe('');
        expect(container.querySelector('[data-system-node="member:g:api:api"]')).toBeNull();
        expect(back()?.disabled).toBe(true);
        await goForward();
        expect([view(), focus()]).toEqual(['structure', 'g:api']);
        await clickText('Whole system');
        expect(focus()).toBe('');
        expect(forward()?.disabled).toBe(true);
        expect(back()?.title).toBe('Back to System structure · api · 1 group open (Alt+Left)');
    });

    it('steps through Behavior starts and followed calls, and an automatic start is no extra step', async () => {
        await render();
        await selectView('structure');
        await doubleClick('[data-system-node="g:api"]');
        await selectView('behavior');
        expect(heading()).toBe('What can main call?');
        expect(position()).toBe('2/2');
        expect(back()?.title).toBe('Back to System structure (Alt+Left)');
        await choose('Behavior entry point', '2');
        expect(heading()).toBe('What can handle call?');
        expect(back()?.title).toBe('Back to Behavior · main (Alt+Left)');
        await doubleClick('[data-system-node="journey-choice:3"]');
        expect(heading()).toBe('What can save call?');
        expect(inViewBack()).toBeUndefined();
        expect(back()?.title).toBe('Back to Behavior · handle (Alt+Left)');
        expect(forward()?.disabled).toBe(true);

        await goBack();
        expect(heading()).toBe('What can handle call?');
        expect(forward()?.title).toBe('Forward to Behavior · save · followed from handle (Alt+Right)');
        await goBack();
        expect(heading()).toBe('What can main call?');
        await goBack();
        expect([view(), focus()]).toEqual(['structure', '']);
        await goForward(); await goForward(); await goForward();
        expect(heading()).toBe('What can save call?');
        expect(container.querySelector<HTMLSelectElement>('select[aria-label="Behavior entry point"]')?.value).toBe('3');
        await clickText('System background');
        expect(heading()).toBe('What can handle call?');
        expect(back()?.title).toBe('Back to Behavior · save · followed from handle (Alt+Left)');
    });

    it('names the shown Behavior start after the suggested start was rejected for a stale snapshot', async () => {
        let calls = 0;
        loader.mockImplementation(async request => {
            calls++;
            if (request.entryNodeId !== undefined && request.expectedGeneration === 'g1') return { status: 'failed', generation: 'g2', error: 'Entry snapshot changed.' };
            return { status: 'ready', generation: calls === 1 ? 'g1' : 'g2', result: projection() };
        });
        await render();
        await openArea('django');
        await selectView('behavior');
        expect(heading()).toBe('What can main call?');
        expect(position()).toBe('2/2');
        expect(back()?.title).toBe('Back to Overview (Alt+Left)');
        await choose('Behavior entry point', '2');
        expect(heading()).toBe('What can handle call?');
        expect(back()?.title).toBe('Back to Behavior · main (Alt+Left)');
        await goBack();
        expect(heading()).toBe('What can main call?');
        expect(forward()?.title).toBe('Forward to Behavior · handle (Alt+Right)');
        await goBack();
        expect(view()).toBe('overview');
        expect(forward()?.title).toBe('Forward to Behavior · main (Alt+Right)');
    });

    it('restores System Plan or 3D with the focused visit without making camera or tab stops', async () => {
        await render();
        await selectView('structure');
        expect(systemCamera()).toBe('3D');
        expect(container.querySelector('[data-testid="system-scene"]')?.getAttribute('data-planar')).toBe('false');
        await doubleClick('[data-system-node="g:api"]');
        await clickText('Plan', container.querySelector('[aria-label="System camera"]')!);
        expect(container.querySelector('[data-testid="system-scene"]')?.getAttribute('data-planar')).toBe('true');
        expect(position()).toBe('2/2');
        await press(container.querySelector<HTMLButtonElement>('[data-system-node="g:service"]'));
        await clickText('Focus here');
        await selectView('overview');
        expect(position()).toBe('3/3');
        expect(back()?.title).toBe('Back to System structure · api · 1 group open · Plan (Alt+Left)');

        await goBack();
        expect([view(), focus(), systemCamera()]).toEqual(['structure', 'g:api', 'Plan']);
        expect(container.querySelector('[data-testid="system-scene"]')?.getAttribute('data-planar')).toBe('true');
        await goBack();
        expect([view(), focus(), systemCamera()]).toEqual(['structure', '', '3D']);
        await goForward(); await goForward();
        expect(view()).toBe('overview');
        await selectView('behavior');
        expect(behaviorCamera()).toBe('3D');
        expect(position()).toBe('3/3');
    });

    it('records Behavior destinations and paths while retaining chain position and camera as context', async () => {
        loader.mockImplementation(async () => ({ status: 'ready', generation: 'g1', result: twoPaths() }));
        await render();
        await selectView('behavior');
        expect(heading()).toBe('What can main call?');
        expect(position()).toBe('1/1');
        await choose('Behavior destination', '3');
        expect(heading()).toBe('main → save');
        expect([chosenPath(), chainPosition()]).toEqual([1, '1 / 3']);
        await clickText('Next →');
        expect(chainPosition()).toBe('2 / 3');
        expect(position()).toBe('2/2');
        expect(back()?.title).toBe('Back to Behavior · main (Alt+Left)');
        await press(pathButtons()[1]);
        expect([chosenPath(), chainPosition()]).toEqual([2, '1 / 2']);
        expect(back()?.title).toBe('Back to Behavior · main → save (Alt+Left)');
        await clickText('Plan', container.querySelector('[aria-label="Behavior camera"]')!);
        expect(position()).toBe('3/3');
        expect(back()?.title).toBe('Back to Behavior · main → save (Alt+Left)');

        await goBack();
        expect([heading(), behaviorCamera(), chosenPath(), chainPosition()]).toEqual(['main → save', '3D', 1, '2 / 3']);
        expect(forward()?.title).toBe('Forward to Behavior · main → save · Path 2 · Plan (Alt+Right)');
        await goBack();
        expect([heading(), container.querySelector('[aria-label="Walk the call chain"]')]).toEqual(['What can main call?', null]);
        expect(back()?.disabled).toBe(true);
        await goForward();
        expect([chosenPath(), chainPosition()]).toEqual([1, '2 / 3']);
        await goForward();
        expect([behaviorCamera(), chosenPath(), chainPosition()]).toEqual(['Plan', 2, '1 / 2']);
        await selectView('structure');
        expect(systemCamera()).toBe('3D');
        expect(position()).toBe('3/3');
    });

    it('preserves the forward branch while typing and saves the latest text before a recent jump', async () => {
        await render();
        await selectView('routes');
        await clickText('Endpoints', container.querySelector('[aria-label="Routes perspective"]')!);
        await openRouteGroup();
        await goBack();
        await type('/home');
        expect(position()).toBe('1/2');
        expect(forward()?.title).toBe('Forward to Routes · Endpoints · /edit (Alt+Right)');
        await goForward();
        expect(filter()).toBe('/edit');
        await goBack();
        expect(filter()).toBe('/home');
        await type('/admin');
        const recent = container.querySelector<HTMLDetailsElement>('details.atlas-arch-recent')!;
        const editPlace = [...recent.querySelectorAll<HTMLButtonElement>('[aria-label="Recently visited places"] li button')]
            .find(item => item.querySelector('span')?.textContent === 'Endpoints · /edit');
        await press(editPlace);
        expect(filter()).toBe('/edit');
        expect(position()).toBe('2/2');
        expect(forward()?.disabled).toBe(true);
        expect(back()?.title).toBe('Back to Routes · Endpoints · /admin (Alt+Left)');
        await goBack();
        expect(filter()).toBe('/admin');
    });

    it('reports opened-area evidence only in its view and restores it with meaningful history visits', async () => {
        const onSelectionEvidence = vi.fn();
        await render({ onSelectionEvidence });
        await openArea('django');
        expect(lastEvidence(onSelectionEvidence)?.label).toBe('django');
        await selectView('routes');
        await clickText('Endpoints', container.querySelector('[aria-label="Routes perspective"]')!);
        expect(sceneLabels()).toContain('/edit · 2');
        expect(lastEvidence(onSelectionEvidence)).toBeUndefined();
        await selectView('hotspots');
        expect(lastEvidence(onSelectionEvidence)).toBeUndefined();
        await selectView('overview');
        expect(lastEvidence(onSelectionEvidence)?.label).toBe('django');
        expect(position()).toBe('2/2');
        await doubleClick('[data-node="file:django/shortcuts.py"]');
        await selectView('routes');
        await goBack();
        expect([view(), trail()]).toEqual(['overview', ['sample', 'django']]);
        expect(lastEvidence(onSelectionEvidence)?.label).toBe('django');
        await goForward();
        expect([view(), perspective()]).toEqual(['routes', 'Endpoints']);
        expect(lastEvidence(onSelectionEvidence)).toBeUndefined();
    });

    it('drops the forward branch on a new focus after Back and keeps at most 25 real stops', async () => {
        await render();
        await selectView('structure');
        await doubleClick('[data-system-node="g:api"]');
        await doubleClick('[data-system-node="g:service"]');
        await goBack();
        expect(focus()).toBe('g:api');
        expect(forward()?.disabled).toBe(false);
        await selectView('behavior');
        expect(position()).toBe('2/3');
        expect(forward()?.disabled).toBe(false);
        await choose('Behavior entry point', '2');
        expect(forward()?.disabled).toBe(true);
        await goBack();
        expect(heading()).toBe('What can main call?');
        expect(forward()?.title).toBe('Forward to Behavior · handle (Alt+Right)');

        for (let step = 0; step < 15; step++) {
            await choose('Behavior entry point', '2');
            await choose('Behavior entry point', '1');
        }
        expect(position()).toBe('25/25');
        let steps = 0;
        while (!back()!.disabled && steps < 100) { await goBack(); steps++; }
        expect(steps).toBe(24);
    });

    it('answers Alt+Left and Alt+Right only in the active workspace and never while typing', async () => {
        await render();
        await openArea('django');
        await selectView('routes');
        let event = key({ key: 'ArrowLeft', altKey: true });
        expect(event.defaultPrevented).toBe(true);
        await settle();
        expect(view()).toBe('overview');
        event = key({ key: 'ArrowRight', altKey: true });
        await settle();
        expect(view()).toBe('routes');
        const search = container.querySelector<HTMLInputElement>('input[type="search"]')!;
        event = key({ key: 'ArrowLeft', altKey: true }, search);
        expect(event.defaultPrevented).toBe(false);
        expect(view()).toBe('routes');
        expect(key({ key: 'ArrowLeft' }).defaultPrevented).toBe(false);
        expect(key({ key: 'ArrowLeft', altKey: true, metaKey: true }).defaultPrevented).toBe(false);
        await render({ escapeTaken: true });
        expect(key({ key: 'ArrowLeft', altKey: true }).defaultPrevented).toBe(false);
        expect(view()).toBe('routes');
        await render({ active: false });
        expect(key({ key: 'ArrowLeft', altKey: true }).defaultPrevented).toBe(false);
        await settle();
        expect(view()).toBe('routes');
    });

    it('jumps to a recent place as a new navigation and starts a fresh history per project', async () => {
        await render();
        await openArea('django');
        await doubleClick('[data-node="file:django/shortcuts.py"]');
        await selectView('routes');
        await clickText('Endpoints', container.querySelector('[aria-label="Routes perspective"]')!);
        await openRouteGroup();
        await goBack();
        const recent = container.querySelector<HTMLDetailsElement>('details.atlas-arch-recent')!;
        expect(recent).not.toBeNull();
        expect(recent.querySelector('summary')?.getAttribute('aria-label')).toBe('Recent');
        const places = [...recent.querySelectorAll('[aria-label="Recently visited places"] li button')];
        expect(places.map(item => [item.querySelector('strong')?.textContent, item.querySelector('span')?.textContent ?? ''])).toEqual([
            ['Routes', 'Endpoints'], ['Routes', 'Endpoints · /edit'], ['Overview', 'django'], ['Overview', ''],
        ]);
        expect(places[0]?.getAttribute('aria-current')).toBe('true');
        await act(async () => { recent.open = true; });
        await press(places[2] as HTMLButtonElement);
        expect([view(), trail()]).toEqual(['overview', ['sample', 'django']]);
        expect(recent.open).toBe(false);
        expect(forward()?.disabled).toBe(true);
        expect(back()?.title).toBe('Back to Routes · Endpoints (Alt+Left)');

        await render({ projectName: 'other', overview: { ...overview, projectName: 'other' } });
        expect(back()?.disabled).toBe(true);
        expect(forward()?.disabled).toBe(true);
        expect(container.querySelector('details.atlas-arch-recent')).toBeNull();
    });
});

describe('Architecture history controls beside the current location', () => {
    const recentMenu = () => container.querySelector<HTMLDetailsElement>('details.atlas-arch-recent');
    async function withRecent(): Promise<HTMLDetailsElement> {
        await render();
        await openArea('django');
        const menu = recentMenu();
        expect(menu).not.toBeNull();
        await act(async () => { menu!.open = true; });
        return menu!;
    }

    it('keeps compact, accessible history controls inside the current view and out of the subtab row', async () => {
        await render();
        await openArea('django');
        const contexts = [
            ['overview', '[data-testid="spatial-architecture"]'],
            ['routes', '[data-testid="container-map"]'],
            ['hotspots', '[data-testid="spatial-architecture"]'],
            ['structure', '[data-testid="system-architecture"]'],
            ['behavior', '.behavior-journey'],
        ];
        for (const [name, selector] of contexts) {
            await selectView(name!);
            const row = container.querySelector<HTMLElement>('.atlas-arch-tabrow')!;
            expect([...row.children].map(item => item.getAttribute('aria-label'))).toEqual(['Architecture views']);
            expect(row.querySelector('[aria-label="Architecture history"]')).toBeNull();
            expect(container.querySelectorAll('[aria-label="Architecture history"]')).toHaveLength(1);
            const currentView = container.querySelector(selector!);
            expect(currentView?.contains(history()!)).toBe(true);
            expect(history()?.parentElement).toBe(currentView?.querySelector(name === 'behavior' ? '.behavior-requests' : '.atlas-arch-context'));
            expect(back()?.textContent).toBe('');
            expect(forward()?.textContent).toBe('');
            expect(back()?.querySelector('svg')?.getAttribute('aria-hidden')).toBe('true');
            expect(forward()?.querySelector('svg')?.getAttribute('aria-hidden')).toBe('true');
            expect(back()?.getAttribute('aria-label')).toBe('Back');
            expect(forward()?.getAttribute('aria-label')).toBe('Forward');
            expect(recentMenu()?.querySelector('summary')?.getAttribute('aria-label')).toBe('Recent');
            expect(recentMenu()?.querySelector('summary svg')?.getAttribute('aria-hidden')).toBe('true');
        }
        await selectView('routes');
        await clickText('Endpoints', container.querySelector('[aria-label="Routes perspective"]')!);
        expect(container.querySelector('[data-testid="spatial-architecture"]')?.contains(history()!)).toBe(true);
        expect(back()?.title).toBe('Back to Overview (Alt+Left)');
    });

    it('closes Recent on a press elsewhere and on Escape, which stays with the menu', async () => {
        const menu = await withRecent();
        await act(async () => { container.querySelector('.atlas-arch-tabs')!.dispatchEvent(new MouseEvent('pointerdown', { bubbles: true })); });
        expect(menu.open).toBe(false);
        await act(async () => { menu.open = true; });
        expect(key({ key: 'Escape' }, menu.querySelector('summary')!).defaultPrevented).toBe(true);
        expect(menu.open).toBe(false);
        expect([view(), trail()]).toEqual(['overview', ['sample', 'django']]);
    });

    it('closes Recent when Back or Alt+Right changes the place', async () => {
        const menu = await withRecent();
        await goBack();
        expect(trail()).toEqual(['sample']);
        expect(menu.open).toBe(false);
        await act(async () => { menu.open = true; });
        expect(key({ key: 'ArrowRight', altKey: true }).defaultPrevented).toBe(true);
        await settle();
        expect(trail()).toEqual(['sample', 'django']);
        expect(menu.open).toBe(false);
    });
});
