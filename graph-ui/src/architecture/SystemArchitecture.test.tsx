// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import SystemArchitecture, { flowEntries, suggestedBehaviorEntry, type FlowsLoader, type SystemArchitectureProps } from './SystemArchitecture';
import { isContiguousPath, systemBehaviorGraph, systemComponentGraph, systemComponents, type SystemSceneModel } from './system-architecture-model';
import { loadSystemArchitecture, readSystemArchitecture, type SystemArchitectureLoader, type SystemArchitectureResponse, type SystemProjection, type SystemSymbol } from './system-architecture-source';
import { callToolJson } from '../provider/rpc-transport';

vi.mock('../provider/rpc-transport', () => ({ callToolJson: vi.fn() }));
vi.mock('./BehaviorSourceEvidence', () => ({ default: ({ symbol, call }: { symbol: SystemSymbol; call?: { id: number } }) =>
    <div data-testid="behavior-source-evidence" data-symbol={symbol.id} data-call={call?.id} /> }));
vi.mock('./SystemArchitectureScene', () => ({ default: ({ model, onSelectNode, onSelectEdge, onClearSelection, selectedNode, highlightedPathIndex, planar }: {
    model: SystemSceneModel; onSelectNode: (id: string) => void; onSelectEdge: (id: string) => void; highlightedPathIndex?: number; planar?: boolean; onClearSelection?: () => void; selectedNode?: string;
}) => <div data-testid="system-scene" data-selected={selectedNode} data-planar={planar} data-positions={JSON.stringify(model.nodes.map(node => node.position))} data-focus={model.focusId ?? ''} data-highlighted-path={highlightedPathIndex ?? 'all'} data-preferred-path={model.preferredPathIndex ?? 'all'}>
    <button onClick={onClearSelection}>Empty background</button>{model.nodes.map(node => <button key={node.id} data-node={node.id} onClick={() => onSelectNode(node.id)}>{node.label}</button>)}
    {model.edges.map(edge => <button key={edge.id} data-edge={edge.id} data-corridor={edge.inCorridor || undefined} onClick={() => onSelectEdge(edge.id)}>{edge.type}</button>)}</div> }));

const symbol = (id: number, component: string, name = `symbol${id}`): SystemSymbol => ({
    id, name, qualified_name: `sample.${name}`, label: 'Function', file_path: `src/${component}.ts`, start_line: id * 10, component_id: component,
});
function fixture(): SystemProjection {
    const first = symbol(1, 'api', 'start'), next = symbol(2, 'service', 'handle'), last = symbol(3, 'store', 'save');
    return {
        schema_version: 1, status: 'ready', kind: 'static_projection', complete: true,
        components: [first, next, last].map(item => ({ id: item.component_id, label: item.component_id, basis: 'interaction_community', member_count: 10, file_count: 2, representatives: [item] })),
        dependencies: [{ source: 'api', target: 'service', type: 'CALLS', count: 8, witnesses: [{ edge_id: 10, source: first, target: next }] },
            { source: 'service', target: 'store', type: 'CALLS', count: 5, witnesses: [{ edge_id: 11, source: next, target: last }] }],
        cycles: [], entrypoints: [first, next], paths: [{ entrypoint_id: 1, nodes: [first, next, last], edges: [
            { id: 10, source_id: 1, target_id: 2, type: 'CALLS' }, { id: 11, source_id: 2, target_id: 3, type: 'CALLS' },
        ] }], totals: { nodes: 30, files: 6, edges: 15, accounted_nodes: 30, structural_nodes: 6 },
        limits: { omitted_components: 0, paths_truncated: false }, warnings: [],
    };
}
const response = (result = fixture()): SystemArchitectureResponse => ({ status: 'ready', generation: 'g1', result });
function overviewFixture(): SystemProjection {
    const data = fixture();
    const enrich = (item: SystemSymbol) => ({ ...item, group_id: `g:${item.component_id}` });
    data.components = data.components.map(item => ({ ...item, group_id: `g:${item.id}`, representatives: item.representatives.map(enrich) }));
    data.paths = data.paths.map(path => ({ ...path, nodes: path.nodes.map(enrich) }));
    data.entrypoints = data.entrypoints.map(enrich);
    data.overview = { complete: true, grouping_basis: 'common_source_directory_aggregate',
        groups: data.components.map(item => ({ id: `g:${item.id}`, label: item.label, component_count: 1, member_count: item.member_count, file_count: item.file_count,
            component_ids: [item.id], representatives: item.representatives })), components: data.components,
        connections: data.dependencies.map(edge => ({ ...edge, source: `g:${edge.source}`, target: `g:${edge.target}`, witnesses: edge.witnesses.map(witness => ({ ...witness, source: enrich(witness.source), target: enrich(witness.target), callsite: { file_path: 'src/call-site.ts', line: 91 }, resolution: { strategy: 'lsp_direct', candidates: 1 } })) })),
        totals: { groups: 3, components: 3 }, limits: { omitted_connections: 0 } };
    data.behavior = { mode: 'targets', source_id: 1, complete: true, max_hops: 10, limits_hit: [],
        reachable_targets: data.paths[0].nodes.slice(1).map((item, index) => ({ ...item, distance: index + 1 })), totals: { reachable_nodes: 3 }, nodes: [], edges: [], cycles: [] };
    return data;
}
function corridorFixture(): SystemProjection {
    const data = overviewFixture();
    data.behavior = { ...data.behavior!, mode: 'corridor', target_id: 3, nodes: data.paths[0].nodes, edges: data.paths[0].edges };
    return data;
}
/** The response the server returns when the projection exceeds its JSON memory budget (Django 5.2.7). */
function limitedEmptyFixture(): SystemProjection {
    return {
        schema_version: 1, status: 'limited', kind: 'static_projection', complete: false,
        components: [], dependencies: [], cycles: [], entrypoints: [], paths: [],
        warnings: ['Component candidates are inferred from indexed declarations and interactions; they do not establish intended responsibilities or deployment boundaries.',
            'Paths are bounded static witnesses, not observed executions. Callback references are not invocation evidence. Index coverage gaps remain possible.',
            'The architecture response exceeded its memory budget; narrow the requested projection.'],
        totals: { nodes: 52402, files: 4654, edges: 274549, accounted_nodes: 0, structural_nodes: 0, components: 0, dependencies: 0, entrypoints: 0, cycles: 0, relationship_edges: 0, unmodeled_edges: 0 },
        limits: { node_budget: 100000, edge_budget: 500000, omitted_components: 0, omitted_dependencies: 0, omitted_entrypoints: 0, omitted_cycles: 0, paths_truncated: false, path_depth: 10 },
        overview: { complete: false, grouping_basis: 'common_source_directory_aggregate', groups: [], components: [], connections: [],
            totals: { groups: 0, components: 0, accounted_nodes: 0, connections: 0 }, limits: { omitted_connections: 0 } },
        behavior: { mode: 'targets', source_id: 0, complete: false, corridor_complete: false, max_hops: 10, limits_hit: ['analysis_budget'],
            reachable_targets: [], nodes: [], edges: [], cycles: [], totals: { reachable_nodes: 0, corridor_nodes: 0, corridor_edges: 0 } },
    };
}
function addOverviewContext(data: SystemProjection) {
    const extra = { id: 'context', label: 'context', basis: 'declared_module', member_count: 1, file_count: 1, representatives: [], group_id: 'g:context' };
    data.components.push(extra);
    data.overview!.groups.push({ id: 'g:context', label: 'context', component_count: 1, member_count: 1, file_count: 1, component_ids: ['context'], representatives: [] });
}
let container: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
    vi.clearAllMocks();
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.useRealTimers(); });
async function render(loader: SystemArchitectureLoader, props: Partial<SystemArchitectureProps> = {}) {
    await act(async () => { root.render(<SystemArchitecture project="sample" view="structure" filter="" active onNavigate={vi.fn()} loader={loader} {...props} />); });
}
async function click(selector: string) { const button = container.querySelector<HTMLButtonElement>(selector); expect(button).not.toBeNull(); await act(async () => button!.click()); }
async function choose(label: string, value: string) {
    const select = container.querySelector<HTMLSelectElement>(`[aria-label="${label}"]`); expect(select).not.toBeNull();
    await act(async () => { select!.value = value; select!.dispatchEvent(new Event('change', { bubbles: true })); });
}
async function clickText(text: string) {
    const button = [...container.querySelectorAll('button')].find(item => item.textContent === text); expect(button).toBeDefined();
    await act(async () => button!.click());
}

describe('system architecture evidence', () => {
    it('changes camera locally without refetching or rearranging the graph', async () => {
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(overviewFixture()));
        await render(loader);
        const scene = () => container.querySelector('[data-testid="system-scene"]')!;
        const positions = scene().getAttribute('data-positions');
        const requests = loader.mock.calls.length;
        expect(scene().getAttribute('data-planar')).toBe('false');
        await clickText('Plan');
        expect(scene().getAttribute('data-planar')).toBe('true');
        expect(scene().getAttribute('data-positions')).toBe(positions);
        expect(loader).toHaveBeenCalledTimes(requests);
        await clickText('3D');
        expect(scene().getAttribute('data-planar')).toBe('false');
    });
    it('clears component focus and inspection while retaining the projection on empty background', async () => {
        const clear = vi.fn(); await render(vi.fn().mockResolvedValue(response(overviewFixture())), { onClearSelection: clear });
        await clickText('Plan'); await click('[data-node="g:api"]'); await clickText('Focus here');
        expect(container.querySelector('[data-testid="system-scene"]')?.getAttribute('data-focus')).toBe('g:api');
        await clickText('Empty background');
        const scene = container.querySelector('[data-testid="system-scene"]')!;
        expect(scene.getAttribute('data-focus')).toBe(''); expect(scene.getAttribute('data-selected')).toBeNull();
        expect(scene.getAttribute('data-planar')).toBe('true');
        expect(scene.querySelectorAll('[data-node]')).toHaveLength(3);
        expect(clear).toHaveBeenCalledOnce();
    });
    it('opens aggregate connection evidence from an accessible inspector control', async () => {
        await render(vi.fn().mockResolvedValue(response(overviewFixture())));
        await click('[data-node="g:api"]');
        await click('[aria-label="Inspect outgoing connection to service"]');
        const inspector = container.querySelector('[aria-label="System evidence inspector"]')!;
        expect(inspector.textContent).toContain('api → service');
        expect(inspector.textContent).toContain('Open call site · src/call-site.ts:91');
        expect(inspector.textContent).toContain('Resolved by lsp direct');
    });
    it('offers deeper destinations from contiguous same-source witnesses beyond the target catalog', async () => {
        const data = overviewFixture();
        data.behavior!.reachable_targets = data.behavior!.reachable_targets.slice(0, 1);
        data.behavior!.totals.omitted_targets = 1;
        const foreign = symbol(99, 'other', 'foreign');
        data.paths.push({ entrypoint_id: 2, nodes: [symbol(2, 'service'), foreign], edges: [{ id: 99, source_id: 2, target_id: 99, type: 'CALLS' }] });
        const broken = symbol(98, 'other', 'broken');
        data.paths.push({ entrypoint_id: 1, nodes: [data.entrypoints[0], broken], edges: [{ id: 98, source_id: 77, target_id: 98, type: 'CALLS' }] });
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(data));
        await render(loader, { view: 'behavior' });
        await choose('Behavior entry point', '1');
        const values = [...container.querySelectorAll<HTMLOptionElement>('[aria-label="Behavior destination"] option')].map(option => option.value);
        expect(values).toContain('3'); expect(values).not.toContain('99'); expect(values).not.toContain('98');
        await choose('Behavior destination', '3');
        expect(loader.mock.calls.at(-1)?.[0]).toMatchObject({ entryNodeId: 1, targetNodeId: 3, expectedGeneration: 'g1' });
    });
    it('validates additive overview and call-site evidence and rejects broken corridor endpoints', () => {
        const data = overviewFixture();
        expect(readSystemArchitecture(response(data)).result?.overview?.groups).toHaveLength(3);
        data.behavior = { ...data.behavior!, mode: 'corridor', target_id: 3, nodes: data.paths[0].nodes, edges: [{ id: 42, source_id: 1, target_id: 99, type: 'CALLS' }] };
        expect(() => readSystemArchitecture(response(data))).toThrow('invalid shape');
    });
    it('passes the exact target and snapshot through the RPC boundary', async () => {
        vi.mocked(callToolJson).mockResolvedValue(response());
        const controller = new AbortController();
        await loadSystemArchitecture({ project: 'sample', entryNodeId: 1, targetNodeId: 3, expectedGeneration: 'g1' }, controller.signal);
        expect(vi.mocked(callToolJson).mock.calls[0][1]).toMatchObject({ entry_node_id: 1, target_node_id: 3, expected_generation: 'g1' });
    });
    it('requests the shared MCP projection and propagates cancellation', async () => {
        vi.mocked(callToolJson).mockResolvedValue(response());
        const controller = new AbortController();
        const result = await loadSystemArchitecture({ project: 'sample', entryNodeId: 2, expectedGeneration: 'projection-g1' }, controller.signal);
        expect(result.status).toBe('ready');
        expect(callToolJson).toHaveBeenCalledWith('get_architecture', { project: 'sample', aspects: ['system_structure'], format: 'json', entry_node_id: 2, expected_generation: 'projection-g1' }, { signal: controller.signal });
    });
    it('rejects malformed source locations before they can drive navigation', () => {
        const value = response();
        (value.result!.components[0].representatives[0] as unknown as Record<string, unknown>).file_path = { path: 'bad' };
        expect(() => readSystemArchitecture(value)).toThrow('invalid shape');
    });
    it('accepts limited projections without inventing complete coverage', () => {
        const data = fixture(); data.status = 'limited'; data.complete = false; data.warnings = ['Node budget reached.'];
        expect(readSystemArchitecture(response(data)).result?.complete).toBe(false);
    });
    it('accepts the empty limited projection exactly as the server sends it', () => {
        const wire = { status: 'ready', generation: 'g1', result: { ...limitedEmptyFixture(), project: 'django-demo', elapsed_ms: 310,
            overview: { ...limitedEmptyFixture().overview, grouping_depth: 1 } } };
        expect(readSystemArchitecture(wire).result?.status).toBe('limited');
    });
    it('rejects projected paths whose underlying symbols do not join', () => {
        const data = fixture(); data.paths[0].edges[1].source_id = 99;
        expect(isContiguousPath(data.paths[0])).toBe(false);
        expect(systemBehaviorGraph(data, data.paths[0]).nodes).toHaveLength(0);
    });
    it('preserves repeated symbols as distinct path steps without manufacturing a self-edge', () => {
        const data = fixture(), path = data.paths[0];
        path.nodes[2] = path.nodes[0]; path.edges[1].target_id = 1;
        const model = systemBehaviorGraph(data, path);
        expect(model.nodes.map(node => node.id)).toEqual(['step:0', 'step:1', 'step:2']);
        expect(model.edges.map(edge => [edge.source, edge.target])).toEqual([['step:0', 'step:1'], ['step:1', 'step:2']]);
    });
    it('limits cycle filtering to relationships inside the same reported component cycle', () => {
        const data = fixture(); data.cycles = [{ component_ids: ['api', 'service'] }, { component_ids: ['store'] }];
        expect(systemComponentGraph(data, '', true).edges.map(edge => edge.source)).toEqual(['api']);
    });
    it('bounds the 3D scene without discarding returned component membership', () => {
        const data = fixture(); data.components = Array.from({ length: 100 }, (_, index) => ({ id: `c${index}`, label: `C${index}`, basis: 'declared_module', member_count: 100, file_count: 10, representatives: [] }));
        const scene = systemComponentGraph(data, '', false, true);
        expect(scene.nodes.length).toBeLessThanOrEqual(16); expect(scene.omittedNodes + scene.nodes.length).toBe(100); expect(data.components).toHaveLength(100);
    });
    it('leads with connected collaboration groups and keeps isolated modules available by opt-in', () => {
        const data = fixture(); data.components.unshift({ id: 'aaa', label: 'README.md', basis: 'declared_module', member_count: 1000, file_count: 1, representatives: [] });
        expect(systemComponents(data, '', false).map(component => component.id)).toEqual(['service', 'api', 'store']);
        expect(systemComponentGraph(data, '', false).nodes.some(node => node.label === 'README.md')).toBe(false);
        expect(systemComponents(data, '', false, true).at(-1)?.label).toBe('README.md');
    });
    it('filters explicit test roles without guessing missing roles or inflating ranking with hidden test callers', () => {
        const data = fixture(); data.components[2].role = 'test'; data.components[2].role_basis = 'indexed_is_test_or_test_path';
        expect(systemComponents(data, '', false).map(component => component.id)).toEqual(['api', 'service']);
        expect(systemComponents(data, '', false, false, true).map(component => component.id)).toEqual(['service', 'api', 'store']);
        expect(data.components).toHaveLength(3);
    });
});

describe('system architecture workspace', () => {
    it('keeps the selected path when inspecting an operation and excludes unrelated overview context', async () => {
        const targets = overviewFixture(), corridor = corridorFixture(); addOverviewContext(targets); addOverviewContext(corridor);
        const loader = vi.fn<SystemArchitectureLoader>().mockImplementation(request => Promise.resolve(response(request.targetNodeId === 3 ? corridor : targets)));
        await render(loader, { view: 'behavior' }); await choose('Behavior entry point', '1'); await choose('Behavior destination', '3');
        const scene = container.querySelector('[data-testid="system-scene"]')!;
        const positions = scene.getAttribute('data-positions'), requests = loader.mock.calls.length;
        expect(scene.querySelectorAll('[data-node]')).toHaveLength(3);
        expect(scene.textContent).not.toContain('context');
        await click('[data-node="journey:1:2:3"]');
        expect(scene.getAttribute('data-positions')).toBe(positions);
        expect(scene.getAttribute('data-focus')).toBe('');
        expect(loader).toHaveBeenCalledTimes(requests);
        expect(container.querySelector('[aria-label="Select step 3: save"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(container.querySelector<HTMLInputElement>('[aria-label="Call-chain position"]')?.value).toBe('2');
    });
    it('uses an explicit Behavior call context independently of hidden Structure filters', async () => {
        const targets = overviewFixture(), corridor = corridorFixture();
        for (const data of [targets, corridor]) {
            addOverviewContext(data); data.components.find(item => item.id === 'store')!.role = 'test';
            data.overview!.groups.find(item => item.id === 'g:store')!.role = 'test';
        }
        const loader = vi.fn<SystemArchitectureLoader>().mockImplementation(request => Promise.resolve(response(request.targetNodeId === 3 ? corridor : targets)));
        await render(loader); await choose('Connection view', 'types');
        await render(loader, { view: 'behavior' }); await choose('Behavior entry point', '1'); await choose('Behavior destination', '3');
        expect(container.textContent).toContain('CALL CHAIN');
        expect(container.querySelector('[data-node="journey:1:2:3"]')).not.toBeNull();
        expect(container.querySelector('[data-node="g:context"]')).toBeNull();
        expect(container.querySelectorAll('[data-edge]')).toHaveLength(2);
        await render(loader);
        expect(container.querySelector<HTMLSelectElement>('[aria-label="Connection view"]')?.value).toBe('types');
        expect(container.querySelector('[data-node="g:store"]')).toBeNull();
        expect(container.querySelector('[data-node="g:context"]')).toBeNull();
    });
    it('returns to immediate calls, and leaves Back to the Architecture workspace (K27)', async () => {
        const loader = vi.fn<SystemArchitectureLoader>().mockImplementation(request => Promise.resolve(response(request.targetNodeId === 3 ? corridorFixture() : overviewFixture())));
        await render(loader, { view: 'behavior' });
        await choose('Behavior entry point', '1'); await choose('Behavior destination', '3');
        await clickText('Immediate calls');
        expect(container.querySelector<HTMLSelectElement>('[aria-label="Behavior destination"]')?.value).toBe('');
        expect(container.querySelectorAll('[data-node]')).toHaveLength(2);
        expect(container.textContent).toContain('DIRECT CALLS · UNORDERED');
        // The old "← Back" buttons are absorbed into the one Back beside the Architecture subtabs.
        expect([...container.querySelectorAll('button')].some(item => item.textContent === '← Back')).toBe(false);
        await render(loader, { view: 'structure' });
        expect([...container.querySelectorAll('button')].some(item => item.textContent === '← Back')).toBe(false);
        expect([...container.querySelectorAll('.system-scope-actions button')].map(item => item.textContent)).toEqual(['Whole system']);
    });
    it('filters whole paths without drawing disconnected remnants and restores the path without refetching', async () => {
        const loader = vi.fn<SystemArchitectureLoader>().mockImplementation(request => Promise.resolve(response(request.targetNodeId === 3 ? corridorFixture() : overviewFixture())));
        await render(loader, { view: 'behavior' }); await choose('Behavior entry point', '1'); await choose('Behavior destination', '3');
        const requests = loader.mock.calls.length;
        await render(loader, { view: 'behavior', filter: 'no-such-symbol' });
        expect(container.querySelector('[data-testid="system-scene"]')).toBeNull();
        expect(container.querySelectorAll('[data-edge]')).toHaveLength(0);
        expect(container.textContent).toContain('No connected path matches this filter.');
        expect(container.querySelector('[aria-label="Walk the call chain"]')).toBeNull();
        await render(loader, { view: 'behavior' });
        expect(container.querySelectorAll('[data-node]')).toHaveLength(3);
        expect(container.querySelector('[aria-label="Select step 1: start"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(loader).toHaveBeenCalledTimes(requests);
    });
    it('discards stale destination catalogs when the analysis snapshot changes within the same project generation', async () => {
        let phase: 'initial' | 'updated' = 'initial';
        let finishCatalog!: (value: SystemArchitectureResponse) => void;
        const updated = overviewFixture(); updated.behavior = undefined; updated.paths = [];
        const loader = vi.fn<SystemArchitectureLoader>().mockImplementation(request => {
            if (phase === 'initial') return Promise.resolve(response(overviewFixture()));
            if (request.entryNodeId === undefined) return Promise.resolve({ status: 'ready', generation: 'g2', result: updated });
            if (request.expectedGeneration === 'g2' && request.targetNodeId === undefined) return new Promise(resolve => { finishCatalog = resolve; });
            return new Promise(() => {});
        });
        await render(loader, { view: 'behavior', generation: 'outer' }); await choose('Behavior entry point', '1');
        expect(container.querySelector('[aria-label="Behavior destination"]')?.textContent).toContain('save');
        phase = 'updated'; await render(loader, { generation: 'outer' }); await render(loader, { view: 'behavior', generation: 'outer' });
        expect(container.querySelector('[aria-label="Behavior destination"]')?.textContent).not.toContain('save');
        await choose('Behavior entry point', '1');
        const destination = container.querySelector<HTMLSelectElement>('[aria-label="Behavior destination"]')!;
        expect(destination.disabled).toBe(true); expect(destination.textContent).not.toContain('save');
        expect(loader.mock.calls.at(-1)?.[0]).toMatchObject({ entryNodeId: 1, expectedGeneration: 'g2' });
        const fresh = overviewFixture(); fresh.paths = []; fresh.behavior!.reachable_targets = [{ ...symbol(4, 'store', 'newSave'), group_id: 'g:store', distance: 2 }];
        await act(async () => finishCatalog({ status: 'ready', generation: 'g2', result: fresh }));
        expect(destination.disabled).toBe(false); expect(destination.textContent).toContain('newSave'); expect(destination.textContent).not.toContain('save');
        await choose('Behavior destination', '4');
        expect(loader.mock.calls.at(-1)?.[0]).toMatchObject({ entryNodeId: 1, targetNodeId: 4, expectedGeneration: 'g2' });
    });
    it('discloses destinations omitted from the bounded reachable-target list', async () => {
        const data = overviewFixture(); data.behavior!.totals.omitted_targets = 27;
        await render(vi.fn().mockResolvedValue(response(data)), { view: 'behavior' });
        expect(container.textContent).toContain('27 reachable destinations are outside the returned catalog.');
    });
    it('hides the previous journey while querying a chosen destination', async () => {
        const data = overviewFixture();
        let finish!: (value: SystemArchitectureResponse) => void;
        const loader = vi.fn<SystemArchitectureLoader>().mockImplementation(request => request.targetNodeId === 3 ? new Promise(resolve => { finish = resolve; }) : Promise.resolve(response(data)));
        await render(loader, { view: 'behavior' });
        await act(async () => { const select = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!; select.value = '1'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(container.querySelectorAll('[data-node]')).toHaveLength(2);
        await act(async () => { const select = container.querySelector<HTMLSelectElement>('[aria-label="Behavior destination"]')!; select.value = '3'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(loader.mock.calls.at(-1)?.[0]).toEqual({ project: 'sample', entryNodeId: 1, targetNodeId: 3, expectedGeneration: 'g1', includeBehaviorEvidence: true });
        expect(container.querySelector('[data-testid="system-scene"]')).toBeNull();
        expect(container.textContent).toContain('Updating this journey');
        expect(container.querySelector('[aria-label="Walk the call chain"]')).toBeNull();
        const corridor = overviewFixture(); corridor.behavior = { ...corridor.behavior!, mode: 'corridor', target_id: 3, nodes: corridor.paths[0].nodes, edges: corridor.paths[0].edges };
        await act(async () => finish(response(corridor)));
        expect(container.querySelectorAll('[data-node]')).toHaveLength(3);
        expect(container.querySelector('[aria-label="Select step 1: start"]')?.getAttribute('aria-pressed')).toBe('true');
    });
    it('opens a recorded call site separately from its caller definition', async () => {
        const onNavigate = vi.fn();
        await render(vi.fn().mockResolvedValue(response(overviewFixture())), { onNavigate });
        await click('[data-edge]');
        const inspector = container.querySelector('[aria-label="System evidence inspector"]')!;
        await act(async () => [...inspector.querySelectorAll('button')].find(item => item.textContent?.startsWith('Open call site'))!.click());
        expect(onNavigate).toHaveBeenCalledWith('src/call-site.ts', 91, 'start');
        expect(inspector.textContent).toContain('Resolved by lsp direct');
        expect(inspector.textContent).not.toContain('95%');
    });
    it('keeps the same topology when a component is selected for inspection', async () => {
        await render(vi.fn().mockResolvedValue(response()));
        const topology = () => [...container.querySelectorAll('[data-node], [data-edge]')].map(item => item.getAttribute('data-node') ?? item.getAttribute('data-edge'));
        const before = topology();
        await click('[data-node="api"]');
        expect(topology()).toEqual(before);
        expect(container.querySelector('[aria-label="System evidence inspector"] h3')?.textContent).toBe('api');
    });
    it('prefers a conventional executable entry over test runners and install helpers', () => {
        const entries = [
            { ...symbol(1, 'install', 'main'), file_path: 'install.sh' },
            { ...symbol(2, 'test', 'main'), file_path: 'tests/test_main.c' },
            { ...symbol(3, 'go', 'main'), file_path: 'pkg/go/cmd/tool/main.go' },
            { ...symbol(4, 'core', 'main'), file_path: 'src/main.c' },
        ];
        expect(suggestedBehaviorEntry(entries)?.id).toBe(4);
        expect(suggestedBehaviorEntry([symbol(5, 'helper', 'format')])).toBeUndefined();
        const handlers = [{ ...symbol(6, 'commands', 'handle'), file_path: 'django/core/management/commands/runserver.py' },
            { ...symbol(7, 'tests', 'handle'), file_path: 'tests/admin_scripts/management/commands/base_command.py' },
            { ...symbol(8, 'app', 'render'), file_path: 'django/shortcuts.py' }];
        expect(suggestedBehaviorEntry(handlers)).toBeUndefined();
        expect(suggestedBehaviorEntry(handlers, true)?.id).toBe(6);
        // Ranked flows keep their order: a shallower but lower-ranked handler does not win.
        expect(suggestedBehaviorEntry([handlers[0], { ...symbol(9, 'docs', 'run'), file_path: 'docs/_ext/djangodocs.py' }], true)?.id).toBe(6);
        expect(suggestedBehaviorEntry([...handlers, ...entries], true)?.id).toBe(4);
    });
    it('reads flow entries once each and never invents an identity', () => {
        const node = (id: number | undefined, name: string, filePath?: string) => ({ ...(id === undefined ? {} : { id }), name, ...(filePath ? { filePath } : {}) });
        expect(flowEntries([
            { id: 0, label: 'handle → tick', entry: node(36163, 'handle', 'django/core/management/commands/runserver.py'), terminal: node(44340, 'tick'), steps: 40 },
            { id: 1, label: 'handle → len', entry: node(36163, 'handle', 'django/core/management/commands/runserver.py'), terminal: node(8040, 'len'), steps: 3 },
            { id: 2, label: 'unknown', entry: node(undefined, 'anonymous'), terminal: node(1, 'x'), steps: 1 },
        ])).toEqual([{ id: 36163, name: 'handle', qualified_name: '', label: '', file_path: 'django/core/management/commands/runserver.py', component_id: '' }]);
    });
    it('automatically requests the executable entry once and leaves manual selection authoritative', async () => {
        const data = fixture(); data.entrypoints[0] = { ...data.entrypoints[0], name: 'main', file_path: 'src/main.c' };
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(data));
        await render(loader, { view: 'behavior' });
        expect(loader).toHaveBeenCalledTimes(2); expect(loader.mock.calls[1][0].entryNodeId).toBe(1);
        await act(async () => { const select = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!; select.value = '2'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(loader).toHaveBeenCalledTimes(3); expect(loader.mock.calls[2][0].entryNodeId).toBe(2);
    });
    it('shows direct calls before narrowing to one branch and synchronizes its selected operation with source', async () => {
        const data = fixture(), fork = symbol(4, 'queue', 'publish'), unrelated = symbol(8, 'other', 'elsewhere');
        data.paths.push({ entrypoint_id: 1, nodes: [data.paths[0].nodes[0], data.paths[0].nodes[1], fork], edges: [data.paths[0].edges[0], { id: 12, source_id: 2, target_id: 4, type: 'ASYNC_CALLS' }] });
        data.paths.push({ entrypoint_id: 8, nodes: [unrelated], edges: [] });
        const onNavigate = vi.fn();
        await render(vi.fn().mockResolvedValue(response(data)), { view: 'behavior', onNavigate });
        expect(container.querySelector('[data-testid="system-scene"]')?.textContent).toContain('handle');
        expect(container.querySelector('[data-testid="system-scene"]')?.textContent).not.toContain('save');
        expect(container.querySelector('[data-testid="system-scene"]')?.textContent).not.toContain('publish');
        await choose('Behavior entry point', '1'); await choose('Behavior destination', '4');
        const scene = container.querySelector('[data-testid="system-scene"]')!;
        expect(scene.textContent).toContain('publish'); expect(scene.textContent).not.toContain('save'); expect(scene.textContent).not.toContain('elsewhere');
        await click('[aria-label="Select step 3: publish"]');
        const inspector = container.querySelector('[aria-label="Behavior evidence inspector"]')!;
        expect(inspector.textContent).toContain('Operation 3'); expect(inspector.textContent).toContain('publish');
        expect(inspector.querySelector('[data-testid="behavior-source-evidence"]')?.getAttribute('data-symbol')).toBe('4');
        await clickText('Open definition ↗');
        expect(onNavigate).toHaveBeenCalledWith('src/queue.ts', 40, 'publish');
    });
    it('includes a component beyond the former immediate neighborhood for inspection', async () => {
        const data = fixture();
        data.components.push({ id: 'far', label: 'far', basis: 'declared_module', member_count: 1, file_count: 1, representatives: [symbol(7, 'far')] });
        data.dependencies.push({ source: 'store', target: 'far', type: 'IMPORTS', count: 1, witnesses: [] });
        await render(vi.fn().mockResolvedValue(response(data)));
        await click('[data-node="far"]');
        expect(container.querySelector('[data-node="far"]')).not.toBeNull();
        expect(container.querySelector('[aria-label="System evidence inspector"] h3')?.textContent).toBe('far');
    });
    it('keeps the inspector aligned when filtering removes the previous focus', async () => {
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue(response());
        await render(loader); await click('[data-node="api"]');
        await render(loader, { filter: 'store' });
        expect(container.querySelector('[aria-label="System evidence inspector"] h3')?.textContent).toBe('store');
    });
    it('keeps generic references available without letting them dominate the initial call map', async () => {
        const data = fixture();
        data.components.push({ id: 'referenceHub', label: 'referenceHub', basis: 'declared_module', member_count: 1, file_count: 1, representatives: [] });
        data.dependencies.push(...['api', 'service', 'store'].map(target => ({ source: 'referenceHub', target, type: 'USAGE', count: 1000, witnesses: [] })));
        await render(vi.fn().mockResolvedValue(response(data)));
        expect(container.querySelector('[data-node="referenceHub"]')).toBeNull();
        expect(container.textContent).toContain('2 of 5 returned connections');
        await act(async () => { const select = container.querySelector<HTMLSelectElement>('[aria-label="Connection view"]')!; select.value = 'all'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(container.querySelector('[data-node="referenceHub"]')).not.toBeNull();
        await click('[data-node="referenceHub"]');
        expect(container.querySelector('[aria-label="System evidence inspector"] h3')?.textContent).toBe('referenceHub');
    });
    it('resets step navigation for a filtered branch and distinguishes a filter miss from absent evidence', async () => {
        const data = fixture(), branch = symbol(4, 'queue', 'publish');
        data.paths.push({ entrypoint_id: 1, nodes: [data.paths[0].nodes[0], branch, data.paths[0].nodes[2]], edges: [
            { id: 12, source_id: 1, target_id: 4, type: 'CALLS' }, { id: 13, source_id: 4, target_id: 3, type: 'CALLS' },
        ] });
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(data));
        await render(loader, { view: 'behavior' }); await choose('Behavior entry point', '1'); await choose('Behavior destination', '3');
        await click('[aria-label="Select step 3: save"]');
        await render(loader, { view: 'behavior', filter: 'publish' });
        const inspector = container.querySelector('[aria-label="Behavior evidence inspector"]')!;
        expect(container.querySelector('[aria-label="Select step 1: start"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(container.querySelector('[data-testid="system-scene"]')?.textContent).toContain('publish');
        expect(container.querySelector('[data-testid="system-scene"]')?.textContent).not.toContain('handle');
        await clickText('Next →');
        expect(container.querySelector('[aria-label="Select step 2: publish"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(inspector.textContent).toContain('publish');
        await render(loader, { view: 'behavior', filter: 'no-such-symbol' });
        expect(container.textContent).toContain('No connected path matches this filter.');
    });
    it('shows progress on first paint and polls pending work without overlapping requests', async () => {
        vi.useFakeTimers();
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValueOnce({ status: 'pending', generation: 'g1', retry_after_ms: 1 }).mockResolvedValue(response());
        await render(loader);
        expect(container.textContent).toContain('Analyzing interactions'); expect(loader).toHaveBeenCalledTimes(1);
        await act(async () => { await vi.advanceTimersByTimeAsync(499); }); expect(loader).toHaveBeenCalledTimes(1);
        await act(async () => { await vi.advanceTimersByTimeAsync(1); });
        expect(loader).toHaveBeenCalledTimes(2); expect(container.textContent).toContain('3 components');
        await act(async () => { await vi.advanceTimersByTimeAsync(5000); }); expect(loader).toHaveBeenCalledTimes(2);
    });
    it('aborts pending work when the view becomes inactive and starts no further poll', async () => {
        vi.useFakeTimers();
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue({ status: 'pending', generation: 'g1' });
        await render(loader); const signal = loader.mock.calls[0][1];
        await render(loader, { active: false }); expect(signal.aborted).toBe(true);
        await act(async () => { await vi.advanceTimersByTimeAsync(5000); }); expect(loader).toHaveBeenCalledTimes(1);
    });
    it('ignores a previous project response after project navigation', async () => {
        let finish!: (value: SystemArchitectureResponse) => void;
        const loader = vi.fn<SystemArchitectureLoader>().mockImplementationOnce(() => new Promise(resolve => { finish = resolve; })).mockResolvedValue(response());
        await render(loader); const oldSignal = loader.mock.calls[0][1];
        await render(loader, { project: 'other' }); expect(oldSignal.aborted).toBe(true);
        const obsolete = fixture(); obsolete.components[0].label = 'OBSOLETE PROJECT';
        await act(async () => finish(response(obsolete)));
        expect(container.textContent).not.toContain('OBSOLETE PROJECT');
        expect(loader.mock.calls[1][0].project).toBe('other');
    });
    it('requests the selected behavior entry and does not reuse it for another project', async () => {
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue(response());
        await render(loader, { view: 'behavior' });
        await act(async () => { const select = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!; select.value = '2'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(loader.mock.calls.at(-1)?.[0]).toEqual({ project: 'sample', entryNodeId: 2, expectedGeneration: 'g1', includeBehaviorEvidence: true });
        await render(loader, { view: 'behavior', project: 'other' });
        expect(loader.mock.calls.at(-1)?.[0]).toEqual({ project: 'other', entryNodeId: undefined, includeBehaviorEvidence: true });
    });
    it('discards numeric entry identities after the indexed generation changes', async () => {
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue(response());
        await render(loader, { view: 'behavior', generation: 'g1' });
        await act(async () => { const select = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!; select.value = '2'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(loader.mock.calls.at(-1)?.[0].entryNodeId).toBe(2);
        await render(loader, { view: 'behavior', generation: 'g2' });
        expect(loader.mock.calls.at(-1)?.[0].entryNodeId).toBeUndefined();
    });
    it('guards entry identities with the analysis generation and resets a stale selection once', async () => {
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValueOnce({ ...response(), generation: 'analysis-g1' })
            .mockResolvedValueOnce({ status: 'failed', generation: 'analysis-g2', error: 'Entry snapshot changed.' })
            .mockResolvedValue({ ...response(), generation: 'analysis-g2' });
        await render(loader, { view: 'behavior', generation: 'different-parent-generation' });
        await act(async () => { const select = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!; select.value = '2'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(loader.mock.calls[1][0]).toEqual({ project: 'sample', entryNodeId: 2, expectedGeneration: 'analysis-g1', includeBehaviorEvidence: true });
        expect(loader.mock.calls[2][0]).toEqual({ project: 'sample', entryNodeId: undefined, includeBehaviorEvidence: true });
        expect(loader).toHaveBeenCalledTimes(3);
        // The stale choice is dropped; the Start field names the operation the reopened journey shows.
        expect(container.querySelector('.behavior-heading h2')?.textContent).toBe('What can start call?');
        expect(container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')?.value).toBe('1');
        expect(container.textContent).not.toContain('Entry snapshot changed.');
    });
    it('opens exact source witnesses and synchronizes selection by qualified name', async () => {
        const data = fixture(), onNavigate = vi.fn(), onSelect = vi.fn();
        const graphNode = { id: 999, name: 'start', qualified_name: 'sample.start', file_path: 'src/api.ts', label: 'Function', x: 0, y: 0, z: 0, size: 1, color: '' };
        await render(vi.fn().mockResolvedValue(response(data)), { onNavigate, onSelect, graph: { nodes: [graphNode], edges: [], total_nodes: 1 } });
        await click('[data-edge]');
        const inspector = container.querySelector('[aria-label="System evidence inspector"]')!;
        const source = [...inspector.querySelectorAll('button')].find(button => button.textContent === 'start')!;
        await act(async () => source.click());
        expect(onNavigate).toHaveBeenCalledWith('src/api.ts', 10, 'start'); expect(onSelect).toHaveBeenCalledWith(graphNode);
        expect(inspector.textContent).toContain('8 indexed relationships · 1 source examples.');
    });
    it('excludes a disconnected behavior path while retaining only supported direct calls', async () => {
        const data = fixture(); data.paths[0].edges[1].source_id = 999;
        await render(vi.fn().mockResolvedValue(response(data)), { view: 'behavior' });
        expect([...container.querySelectorAll('[data-node]')].map(node => node.textContent)).toEqual(['start', 'handle']);
        expect(container.querySelectorAll('[data-edge]')).toHaveLength(1);
        expect(container.querySelector('[aria-label="All operations in this call chain"]')).toBeNull();
        expect(container.textContent).toContain('1 disconnected or unsupported paths excluded.');
    });
    it('renders limited-analysis warnings and accurate representative membership counts', async () => {
        const data = fixture(); data.complete = false; data.status = 'limited'; data.warnings = ['Some callback targets are unresolved.'];
        await render(vi.fn().mockResolvedValue(response(data))); await click('[data-node="api"]');
        const inspector = container.querySelector('[aria-label="System evidence inspector"]')!;
        expect(inspector.textContent).toContain('10 symbols across 2 files'); expect(inspector.textContent).toContain('Representative sources');
        expect(container.textContent).toContain('Partial analysis'); expect(container.textContent).toContain('Some callback targets are unresolved.');
    });
    it('lets users include test components while preserving total and filtered counts', async () => {
        const data = fixture(); data.components[2].role = 'test'; data.components[2].role_basis = 'indexed_is_test_or_test_path';
        await render(vi.fn().mockResolvedValue(response(data)));
        expect(container.querySelector('[data-node="store"]')).toBeNull();
        expect(container.textContent).toContain('3 components returned · 2 shown');
        const toggle = [...container.querySelectorAll('label')].find(label => label.textContent?.includes('Include test components'))!.querySelector('input')!;
        await act(async () => toggle.click());
        expect(container.querySelector('[data-node="store"]')).not.toBeNull();
        expect(container.textContent).toContain('Test code'); expect(container.textContent).toContain('3 components returned');
    });
    it('explains an empty limited projection with its own warning before any filter message', async () => {
        await render(vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(limitedEmptyFixture())));
        const reason = () => container.querySelector('.system-scene-empty[role="status"]')?.textContent ?? '';
        expect(reason()).toContain('The analysis returned no system structure for this project.');
        expect(reason()).toContain('The architecture response exceeded its memory budget; narrow the requested projection.');
        expect(reason()).not.toContain('Component candidates are inferred');
        const cycles = [...container.querySelectorAll('label')].find(label => label.textContent?.includes('Group cycles'))!.querySelector('input')!;
        await act(async () => cycles.click());
        expect(container.textContent).not.toContain('No component cycles match');
        expect(reason()).toContain('exceeded its memory budget');
    });
    it('keeps the budget message for an empty projection that names no limit', async () => {
        const data = limitedEmptyFixture(); data.status = 'ready'; data.warnings = data.warnings.slice(0, 2);
        await render(vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(data)));
        expect(container.querySelector('.system-scene-empty[role="status"]')?.textContent).toBe('No component projection is available within this analysis budget.');
    });
    it('fills an empty behavior start list from ranked flows and requests a conventional handler', async () => {
        const flowsLoader = vi.fn<FlowsLoader>().mockResolvedValue([
            { id: 0, label: 'run_tests → setup', entry: { id: 9, name: 'run_tests', filePath: 'tests/runtests.py' }, terminal: { id: 10, name: 'setup' }, steps: 12 },
            { id: 1, label: 'handle → tick', entry: { id: 36163, name: 'handle', filePath: 'django/core/management/commands/runserver.py' }, terminal: { id: 44340, name: 'tick' }, steps: 40 },
        ]);
        const chosen = limitedEmptyFixture();
        chosen.behavior = { ...chosen.behavior!, source_id: 36163 };
        chosen.warnings = [...chosen.warnings, 'Optional behavior evidence was omitted entirely to keep the base graph within the response memory budget.'];
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValueOnce(response(limitedEmptyFixture())).mockResolvedValue(response(chosen));
        await render(loader, { view: 'behavior', flowsLoader });
        expect(flowsLoader).toHaveBeenCalledExactlyOnceWith('sample');
        expect(loader.mock.calls.at(-1)?.[0]).toEqual({ project: 'sample', entryNodeId: 36163, expectedGeneration: 'g1', includeBehaviorEvidence: true });
        const start = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!;
        // Alphabetical (hand test 2026-10-04, A4); with every operation suggested a Suggested group would only repeat the list.
        expect([...start.options].map(option => option.textContent)).toEqual(['Choose an operation…',
            'handle · django/core/management/commands/runserver.py', 'run_tests · tests/runtests.py']);
        expect(start.querySelectorAll('optgroup')).toHaveLength(1);
        expect(start.value).toBe('36163');
        const state = container.querySelector('.behavior-loading[role="status"]')!;
        expect(state.textContent).toContain('The analysis returned no call evidence.');
        expect(state.textContent).toContain('exceeded its memory budget');
        expect(state.textContent).toContain('Optional behavior evidence was omitted entirely');
        await choose('Behavior entry point', '9');
        expect(loader.mock.calls.at(-1)?.[0].entryNodeId).toBe(9);
        // The chosen start keeps its place in the alphabetical list.
        expect([...start.options].map(option => option.value)).toEqual(['', '36163', '9']);
        expect(start.value).toBe('9');
    });
    it('does not ask for flows once the projection classifies enough entry points', async () => {
        const flowsLoader = vi.fn<FlowsLoader>().mockResolvedValue([]);
        const many = fixture(); many.entrypoints = [1, 2, 3, 4, 5, 6, 7, 8].map(id => symbol(id, 'api', `entry${id}`));
        await render(vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(many)), { view: 'behavior', flowsLoader });
        expect(flowsLoader).not.toHaveBeenCalled();
    });
    it('adds ranked flows after a handful of classified entry points, once each', async () => {
        const flowsLoader = vi.fn<FlowsLoader>().mockResolvedValue([
            { id: 0, label: 'handle → tick', entry: { id: 36163, name: 'handle', filePath: 'django/core/management/commands/runserver.py' }, terminal: { id: 44340, name: 'tick' }, steps: 40 },
            { id: 1, label: 'handle → save', entry: { id: 2, name: 'handle', filePath: 'src/service.ts' }, terminal: { id: 3, name: 'save' }, steps: 2 },
        ]);
        await render(vi.fn<SystemArchitectureLoader>().mockResolvedValue(response()), { view: 'behavior', flowsLoader });
        expect(flowsLoader).toHaveBeenCalledExactlyOnceWith('sample');
        const start = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!;
        // Every operation alphabetically (hand test 2026-10-04, A4), the flow that repeats a classified entry point once.
        // The journey's start and the two ranked flows would suggest all three, so no Suggested group repeats them.
        expect([...start.querySelectorAll('optgroup')].map(group => [group.label, [...group.querySelectorAll('option')].map(option => option.value)]))
            .toEqual([['All operations · 3', ['36163', '2', '1']]]);
    });
    it('A4: suggests the automatic start and the top ranked flows, and names look-alike flow starts by their class', async () => {
        const MODELS = 'django/db/migrations/operations/models.py';
        const flow = (id: number, name: string, filePath: string) => ({ id, label: name, entry: { id, name, filePath }, terminal: { id: 1, name: 'x' }, steps: 3 });
        const flowsLoader = vi.fn<FlowsLoader>().mockResolvedValue([flow(41, 'database_backwards', MODELS), flow(42, 'database_backwards', MODELS),
            flow(43, 'handle', 'django/core/management/commands/loaddata.py'), flow(44, 'b4', 'b.py'), flow(45, 'b5', 'b.py'), flow(46, 'b6', 'b.py')]);
        const operationNames = vi.fn(async (_project: string, entries: SystemSymbol[]) => entries.map(entry => entry.id === 41
            ? { ...entry, qualified_name: 'p.ops.models.CreateModel.database_backwards' } : entry.id === 42 ? { ...entry, qualified_name: 'p.ops.models.DeleteModel.database_backwards' } : entry));
        const data = overviewFixture();
        data.entrypoints = [{ ...data.entrypoints[0], name: 'main' }, data.entrypoints[1]];
        await render(vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(data)), { view: 'behavior', flowsLoader, operationNames });
        expect(operationNames).toHaveBeenCalledOnce();
        expect(operationNames.mock.calls[0]?.[1].map(entry => entry.id)).toEqual([41, 42, 43, 44, 45, 46]);
        const start = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!;
        const suggested = [...start.querySelectorAll('optgroup')[0]!.querySelectorAll('option')].map(option => option.textContent);
        // The automatic start, then the first five ranked flows in their order.
        expect(suggested).toEqual(['main · src/api.ts', `database_backwards (CreateModel) · ${MODELS}`, `database_backwards (DeleteModel) · ${MODELS}`,
            'handle · django/core/management/commands/loaddata.py', 'b4 · b.py', 'b5 · b.py']);
        expect(start.selectedOptions[0]?.textContent).toBe('main · src/api.ts');
    });
    it('A4: suggests the start the projection chose itself, and keeps suggesting it after another start is picked', async () => {
        // django-demo: "main · …/manage.py-tpl" is no conventional entry for the page, the projection picks it on its own.
        const flow = (id: number, name: string, filePath: string) => ({ id, label: name, entry: { id, name, filePath }, terminal: { id: 1, name: 'x' }, steps: 3 });
        const flowsLoader = vi.fn<FlowsLoader>().mockResolvedValue([flow(43, 'handle', 'loaddata.py'), flow(44, 'b4', 'b.py')]);
        const data = overviewFixture();
        data.entrypoints = [{ ...data.entrypoints[0], name: 'main', file_path: 'conf/manage.py-tpl' }, data.entrypoints[1]];
        // No operation requested: the server names none (0), and the journey starts at the first classified entry point.
        data.behavior = { ...data.behavior!, source_id: 0 };
        await render(vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(data)), { view: 'behavior', flowsLoader });
        const start = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!;
        const suggested = () => [...start.querySelectorAll('optgroup')[0]!.querySelectorAll('option')].map(option => option.value);
        expect(start.value).toBe('1');
        expect(suggested()).toEqual(['1', '43', '44']);
        await choose('Behavior entry point', '43');
        expect(start.value).toBe('43');
        expect(suggested()).toEqual(['1', '43', '44']);
    });
    it('shows failures without retrying continuously', async () => {
        vi.useFakeTimers(); const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue({ status: 'failed', generation: 'g1', error: 'Analysis budget exceeded.' });
        await render(loader); await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
        expect(container.querySelector('[role="alert"]')?.textContent).toContain('Analysis budget exceeded'); expect(loader).toHaveBeenCalledTimes(1);
    });
});

/* Hand test 2026-10-04 (A3): "Refresh analysis" and the Behavior "Refresh" read again and said nothing. */
describe('system architecture refresh feedback', () => {
    const refresh = (pattern: RegExp) => [...container.querySelectorAll('button')].find(element => pattern.test(element.textContent ?? ''))!;
    const status = () => container.querySelector('[role="status"].atlas-refresh-status')?.textContent;
    afterEach(() => { vi.useRealTimers(); });

    it('A3: Refresh analysis is busy while it runs, then names the time and whether the index changed', async () => {
        vi.useFakeTimers({ toFake: ['Date'] });
        vi.setSystemTime(new Date(2026, 9, 4, 18, 0, 9));
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(overviewFixture()));
        await render(loader);
        let finish!: (value: SystemArchitectureResponse) => void;
        loader.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
        await act(async () => refresh(/^Refresh(ing)? analysis/).click());
        expect(refresh(/^Refresh(ing)? analysis/).textContent).toBe('Refreshing analysis…');
        expect(refresh(/^Refresh(ing)? analysis/).getAttribute('aria-disabled')).toBe('true');
        // The structure stays on screen meanwhile.
        expect(container.querySelector('[data-testid="system-scene"]')).not.toBeNull();
        await act(async () => finish(response(overviewFixture())));
        expect(refresh(/^Refresh(ing)? analysis/).textContent).toBe('Refresh analysis');
        expect(status()).toBe('Up to date at 18:00:09: no changes since the last load');
        loader.mockResolvedValueOnce({ ...response(overviewFixture()), generation: 'g2' });
        await act(async () => refresh(/^Refresh(ing)? analysis/).click());
        expect(status()).toBe('Analysis refreshed at 18:00:09');
        loader.mockRejectedValueOnce(new Error('Daemon stopped.'));
        await act(async () => refresh(/^Refresh(ing)? analysis/).click());
        expect(status()).toBe('Refresh failed at 18:00:09: Daemon stopped.');
    });

    it('A3: the Behavior Refresh says the same beside its button', async () => {
        vi.useFakeTimers({ toFake: ['Date'] });
        vi.setSystemTime(new Date(2026, 9, 4, 18, 1, 0));
        const loader = vi.fn<SystemArchitectureLoader>().mockResolvedValue(response(overviewFixture()));
        await render(loader, { view: 'behavior' });
        const button = () => container.querySelector<HTMLButtonElement>('.behavior-heading .atlas-refresh button')!;
        expect(button()?.textContent).toBe('Refresh');
        let finish!: (value: SystemArchitectureResponse) => void;
        loader.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
        await act(async () => button().click());
        expect(button().textContent).toBe('Refreshing…');
        await act(async () => finish(response(overviewFixture())));
        expect(button().textContent).toBe('Refresh');
        expect(container.querySelector('.behavior-heading [role="status"]')?.textContent).toBe('Up to date at 18:01:00: no changes since the last load');
    });
});
