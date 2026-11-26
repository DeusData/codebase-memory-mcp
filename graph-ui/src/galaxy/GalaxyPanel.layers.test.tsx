// @vitest-environment jsdom
/*
 * Handtest K8: eine tiefe Ebene haengt nicht mehr minutenlang. Die Leiste
 * zeigt, was schon geladen ist, "−" bricht das Laden ab und kehrt sofort zur
 * vorigen Ebene zurueck, und eine Ebene ueber dem Render-Limit stoppt dort und
 * sagt, dass sie unvollstaendig ist.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel from './GalaxyPanel';
import { viewPreferencesKey } from '../settings/view-preferences';
import { scopeFetch, scopeNode } from './test-scope-fetch';
import type { GraphEdge } from './types';

const scene = vi.hoisted(() => ({ separateNodes: undefined as boolean | undefined, nodes: 0 }));
vi.mock('./GraphScene', async importOriginal => ({
    ...await importOriginal<typeof import('./GraphScene')>(),
    GraphScene: ({ separateNodes, data }: { separateNodes?: boolean; data: { nodes: unknown[] } }) => {
        scene.separateNodes = separateNodes; scene.nodes = data.nodes.length;
        return <output data-testid="scene" />;
    },
}));

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => {
    await act(async () => root.unmount()); host.remove(); globalThis.__atlasGalaxy = undefined;
    window.localStorage.clear();
});

const seam = () => globalThis.__atlasGalaxy!;
const settle = (check: () => void) => vi.waitFor(async () => {
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
    check();
});
const status = () => host.querySelector('.atlas-graph-scope-count');
const button = (label: string) => [...host.querySelectorAll('button')].find(entry => entry.textContent === label)!;
const minus = () => host.querySelector<HTMLButtonElement>('button[aria-label="Remove graph layer"]')!;
/* G3: the Expand explanation is the Galaxy's own tooltip; its text stands on the button as data-hint. */
const hint = (element: HTMLElement) => element.getAttribute('data-hint');
const layers = () => [...host.querySelectorAll('.atlas-graph-exploration span')].map(entry => entry.textContent ?? '').find(text => /^\d+ layers?$/.test(text));

it('K8: shows loaded nodes and edges while a layer loads, and "−" cancels back to the previous layer at once', async () => {
    const nodes = [1, 2, 3, 4, 5, 6].map(id => scopeNode(id));
    const edges: GraphEdge[] = [{ id: 1, source: 1, target: 2, type: 'CALLS' }, { id: 2, source: 2, target: 3, type: 'CALLS' },
        { id: 3, source: 4, target: 2, type: 'CALLS' }, { id: 4, source: 3, target: 5, type: 'CALLS' }, { id: 5, source: 6, target: 1, type: 'TESTS' }];
    let block = false, release: () => void = () => {};
    let edgeQueries = 0;
    const { fetch } = scopeFetch({ nodes, edges, gate: async () => {
        edgeQueries += 1;
        // Layer 2: the outbound batch arrives, the inbound one hangs like the slow server.
        if (block && edgeQueries > 1) await new Promise<void>(resolve => { release = resolve; });
    } });
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={fetch} />));
    await settle(() => expect(seam().nodes).toBe(6));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(status()?.textContent).toBe('3 nodes · 2 edges'));
    expect(minus().disabled).toBe(true);

    block = true; edgeQueries = 0;
    await act(async () => button('Expand +1').click());
    await settle(() => expect(status()?.textContent).toBe('Loading layer 2: 4 nodes, 3 edges so far'));
    expect(status()?.getAttribute('data-state')).toBe('loading');
    // The tooltip counts the requests, so a long batch reads as work in progress (review of K8).
    expect(status()?.getAttribute('title')).toMatch(/^Loading layer 2: 4 nodes, 3 edges so far\. Request \d+ to the index; "−" cancels\.$/);
    // While loading, "−" is the way out, and it says so.
    expect(minus().disabled).toBe(false);
    expect(minus().title).toBe('Cancel loading layer 2 and return to 1 layer');
    await act(async () => minus().click());
    // At once: the previous layer is complete again, nothing waits for the hanging request.
    expect(layers()).toBe('1 layer');
    expect(status()?.getAttribute('data-state')).not.toBe('loading');
    expect(minus().disabled).toBe(true);
    await settle(() => expect(status()?.textContent).toBe('3 nodes · 2 edges'));
    await act(async () => { release(); });
    await settle(() => expect(status()?.textContent).toBe('3 nodes · 2 edges'));
    // A cancelled layer is a step back, not a new step: Forward retries it, and the history holds no duplicate.
    expect(seam().history.entries).toEqual(['All graph', 'n1 · 1 layer', 'n1 · 2 layers']);
    expect(seam().history.index).toBe(1);
    expect(host.querySelector<HTMLButtonElement>('button[aria-label="Forward"]')?.title).toBe('Forward to n1 · 2 layers (Alt+Right)');
});

it('K8: a layer past the render limit stops there, says it is partial and cannot be expanded further', async () => {
    window.localStorage.setItem(viewPreferencesKey('sample'), JSON.stringify({ version: 1, preferences: { galaxyNodes: 500, galaxyEdges: 1000 } }));
    const fan = Array.from({ length: 600 }, (_, at) => scopeNode(100 + at));
    const nodes = [scopeNode(1), scopeNode(2), ...fan];
    const edges: GraphEdge[] = [{ id: 1, source: 1, target: 2, type: 'CALLS' },
        ...fan.map((node, at) => ({ id: 10 + at, source: 2, target: node.id, type: 'CALLS' }))];
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={scopeFetch({ nodes, edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBeGreaterThan(0));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(status()?.textContent).toBe('2 nodes · 1 edge'));
    expect(hint(button('Expand +1'))).toContain('Load layer 2: 1 node to expand');

    await act(async () => button('Expand +1').click());
    await settle(() => expect(status()?.textContent).toBe('Partial: 602 nodes · 601 edges'));
    expect(status()?.getAttribute('data-state')).toBe('partial');
    // G3: 602 nodes loaded with a limit of 500, so the words say "after passing", not "at".
    expect(status()?.getAttribute('title')).toBe('Layer 2 stopped loading after the request that took it past the render limit of 500 nodes, so it is incomplete; '
        + 'the scene draws at most 500 nodes. Raise the limit under Limits or trace fewer edge types to load all of it.');
    expect(scene.nodes).toBe(500);
    // G3: blocked, not disabled: a disabled button takes neither the pointer nor the focus, and the reason would be out of reach.
    expect(button('Expand +1').getAttribute('aria-disabled')).toBe('true');
    expect(hint(button('Expand +1'))).toContain('stopped loading after it passed the render limit');
    await act(async () => { button('Expand +1').dispatchEvent(new MouseEvent('mouseover', { bubbles: true })); });
    expect(document.querySelector('[data-testid="atlas-hint"][data-hint-for="galaxy-expand"]')?.textContent).toContain('stopped loading after it passed the render limit');
    await act(async () => { button('Expand +1').dispatchEvent(new MouseEvent('mouseout', { bubbles: true })); });
    await act(async () => button('Expand +1').click());
    expect(layers()).toBe('2 layers');
    expect(status()?.textContent).toBe('Partial: 602 nodes · 601 edges');
    // The partial layer can still be left the normal way.
    await act(async () => minus().click());
    await settle(() => expect(status()?.textContent).toBe('2 nodes · 1 edge'));
});

it('C1: tells the chat that a layer stopped at the render limit, not that the scope is complete', async () => {
    window.localStorage.setItem(viewPreferencesKey('sample'), JSON.stringify({ version: 1, preferences: { galaxyNodes: 500, galaxyEdges: 1000 } }));
    const fan = Array.from({ length: 600 }, (_, at) => scopeNode(100 + at));
    const edges: GraphEdge[] = [{ id: 1, source: 1, target: 2, type: 'CALLS' }, ...fan.map((node, at) => ({ id: 10 + at, source: 2, target: node.id, type: 'CALLS' }))];
    const onSelectionEvidence = vi.fn();
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} onSelectionEvidence={onSelectionEvidence}
        fetch={scopeFetch({ nodes: [scopeNode(1), scopeNode(2), ...fan], edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBeGreaterThan(0));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(status()?.textContent).toBe('2 nodes · 1 edge'));
    const limitations = () => JSON.parse(onSelectionEvidence.mock.lastCall![0].text).evidence.limitations;
    expect(limitations().state).toBe('complete-indexed-scope');
    await act(async () => button('Expand +1').click());
    await settle(() => expect(status()?.textContent).toBe('Partial: 602 nodes · 601 edges'));
    await settle(() => expect(limitations()).toMatchObject({ state: 'render-limit-partial', renderLimit: { layer: 2, kind: 'nodes', limit: 500 } }));
});

it('K8: a large finished layer keeps its arranged cloud instead of being pushed apart on screen', async () => {
    const fan = Array.from({ length: 1600 }, (_, at) => scopeNode(100 + at));
    const nodes = [scopeNode(1), ...fan];
    const edges: GraphEdge[] = fan.map((node, at) => ({ id: 10 + at, source: 1, target: node.id, type: 'CALLS' }));
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={scopeFetch({ nodes, edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBeGreaterThan(0));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(status()?.textContent).toBe(`${(1601).toLocaleString()} nodes · ${(1600).toLocaleString()} edges`));
    expect(scene.nodes).toBe(1601);
    // Screen separation pushed a dense cloud of thousands of nodes into a cross of long lines (layer 3 of JSONBAgg).
    expect(scene.separateNodes).toBe(false);
});

it('K8: Expand warns before loading when the nodes at the edge have more indexed calls than the render limit allows', async () => {
    window.localStorage.setItem(viewPreferencesKey('sample'), JSON.stringify({ version: 1, preferences: { galaxyNodes: 500, galaxyEdges: 1000 } }));
    const nodes = [1, 2, 3].map(id => scopeNode(id));
    const edges: GraphEdge[] = [{ id: 1, source: 1, target: 2, type: 'CALLS' }, { id: 2, source: 3, target: 1, type: 'CALLS' }];
    // n2 is called from all over the code, like len or str at the edge of JSONBAgg's second layer.
    const { fetch, calls } = scopeFetch({ nodes, edges, degrees: { 2: { in: 1270, out: 3 }, 3: { in: 0, out: 2 } } });
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={fetch} />));
    await settle(() => expect(seam().nodes).toBe(3));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(button('Expand +1').getAttribute('data-warning')).toBe('true'));
    // 1,270 + 3 + 2 calls, less the two already loaded. G3: the calls drive the warning, so the smaller growth estimate is not offered beside it.
    const text = `Likely past the render limit of 500 nodes. Load layer 2: 2 nodes to expand. The index lists ${(1273).toLocaleString()} calls at them that are not loaded yet, `
        + `and each can bring a new node; 3 nodes are loaded now. Loading stops after the first request to the index that takes it past 500 nodes or ${(1000).toLocaleString()} edges, `
        + `so the layer can end above the limit; it is then marked partial, and the scene draws at most 500 nodes and ${(1000).toLocaleString()} edges.`;
    expect(hint(button('Expand +1'))).toBe(text);
    expect(calls.filter(call => String(call.args.query).includes('n.in_degree'))).toHaveLength(1);

    /*
     * G3: the warning came as a native title, after the browser's delay. It is
     * the Galaxy's own tooltip now: at once on hover and on keyboard focus,
     * described by aria-describedby, closed on leave, blur and Escape.
     */
    const expand = button('Expand +1');
    expect(expand.hasAttribute('title')).toBe(false);
    const shown = () => document.querySelector<HTMLElement>('[data-testid="atlas-hint"][data-hint-for="galaxy-expand"]');
    expect(shown()).toBeNull();
    await act(async () => { expand.dispatchEvent(new MouseEvent('mouseover', { bubbles: true })); });
    expect(shown()?.textContent).toBe(text);
    expect(shown()?.getAttribute('role')).toBe('tooltip');
    expect(expand.getAttribute('aria-describedby')).toBe(shown()?.id);
    await act(async () => { expand.dispatchEvent(new MouseEvent('mouseout', { bubbles: true })); });
    expect(shown()).toBeNull();
    expect(expand.hasAttribute('aria-describedby')).toBe(false);
    await act(async () => { expand.focus(); });
    expect(shown()?.textContent).toBe(text);
    // Escape on the focused button closes the tooltip and nothing else: the scope stays.
    // A real key press can be cancelled; that is what keeps the Escape of the tooltip from leaving the scope too.
    await act(async () => { expand.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true })); });
    expect(shown()).toBeNull();
    expect(button('Expand +1')).toBe(expand);
    expect(status()?.textContent).toBe('3 nodes · 2 edges');
    await act(async () => { expand.blur(); });
    await act(async () => { expand.focus(); });
    expect(shown()).not.toBeNull();
    await act(async () => { expand.blur(); });
    expect(shown()).toBeNull();
});
