// @vitest-environment jsdom
/*
 * Handtest K9 und K2: ein Klick ins Leere verlaesst den Ausschnitt nicht mehr,
 * und Zurueck/Vor fuehren durch die letzten Ausschnitte. Die Szene ist eine
 * Attrappe mit einem Knopf fuer die leere Flaeche; die Beziehungen kommen ueber
 * dieselbe RPC-Strecke wie im Betrieb (test-scope-fetch.ts).
 */
import { act, useState } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel from './GalaxyPanel';
import type { GraphData } from './types';
import type { ScenePath } from './PathLayer';
import { scopeFetch, scopeNode } from './test-scope-fetch';

const scene = vi.hoisted(() => ({ path: undefined as ScenePath | undefined, nodes: 0 }));
vi.mock('./GraphScene', async importOriginal => ({
    ...await importOriginal<typeof import('./GraphScene')>(),
    GraphScene: ({ data, path, onBackgroundClick }: { data: GraphData; path?: ScenePath; onBackgroundClick?: () => void }) => {
        scene.path = path; scene.nodes = data.nodes.length;
        return <button type="button" onClick={onBackgroundClick}>Empty canvas</button>;
    },
}));

const nodes = [1, 2, 3, 4, 5].map(id => scopeNode(id));
const edges = [
    { id: 11, source: 1, target: 2, type: 'CALLS', line: 3 }, { id: 12, source: 3, target: 1, type: 'CALLS', line: 4 },
    { id: 13, source: 2, target: 4, type: 'CALLS', line: 2 }, { id: 14, source: 4, target: 5, type: 'IMPORTS' },
    { id: 15, source: 1, target: 5, type: 'CALLS', line: 1 },
];

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); globalThis.__atlasGalaxy = undefined; });

const seam = () => globalThis.__atlasGalaxy!;
const settle = (check: () => void) => vi.waitFor(async () => {
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
    check();
});
const button = (label: string) => [...host.querySelectorAll('button')].find(entry => entry.textContent === label);
const named = (name: string) => host.querySelector<HTMLButtonElement>(`button[aria-label="${name}"]`);
const scopeName = () => host.querySelector('.atlas-graph-scope-name')?.textContent ?? '';
const layers = () => [...host.querySelectorAll('.atlas-graph-exploration span')].map(entry => entry.textContent ?? '').find(text => /^\d+ layers?$/.test(text));
const pathPanel = () => host.querySelector('[data-testid="atlas-galaxy-path-panel"]');
const pickPath = (name: string) => act(async () => {
    host.querySelector<HTMLDetailsElement>('.atlas-graph-path-picker')!.open = true;
    [...host.querySelectorAll<HTMLButtonElement>('.atlas-graph-path-menu li button')]
        .find(entry => entry.querySelector('strong')?.textContent === name)!.click();
});
const key = (init: KeyboardEventInit, target: EventTarget = window) => {
    const event = new KeyboardEvent('keydown', { bubbles: true, cancelable: true, ...init });
    act(() => { target.dispatchEvent(event); });
    return event;
};

async function openScope(clearSelection = vi.fn()) {
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()}
        onClearSelection={clearSelection} fetch={scopeFetch({ nodes, edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('4 nodes · 3 edges'));
}

it('K9: a click on empty canvas keeps the scope, its layers and trace and only clears the path highlight', async () => {
    const clearSelection = vi.fn();
    await openScope(clearSelection);
    await act(async () => button('Expand +1')!.click());
    await settle(() => expect(seam().nodes).toBe(5));
    await pickPath('n4');
    expect(pathPanel()).not.toBeNull();

    await act(async () => button('Empty canvas')!.click());
    expect(scopeName()).toBe('n1');
    expect(layers()).toBe('2 layers');
    expect(seam().nodes).toBe(5);
    expect(pathPanel()).toBeNull();
    expect(scene.path).toBeUndefined();
    expect(clearSelection).not.toHaveBeenCalled();
    // A second click on empty canvas changes nothing either.
    await act(async () => button('Empty canvas')!.click());
    expect(scopeName()).toBe('n1');
});

it('K9: Escape clears an open path first and then leaves the scope, unless another surface takes the key', async () => {
    await openScope();
    await pickPath('n2');
    expect(key({ key: 'Escape' }).defaultPrevented).toBe(true);
    expect(pathPanel()).toBeNull();
    expect(scopeName()).toBe('n1');
    // Typing in a field: Escape stays with the field.
    expect(key({ key: 'Escape' }, host.querySelector('input[aria-label="Find a graph node"]')!).defaultPrevented).toBe(false);
    expect(scopeName()).toBe('n1');
    expect(key({ key: 'Escape' }).defaultPrevented).toBe(true);
    await settle(() => expect(seam().nodes).toBe(5));
    expect(host.querySelector('.atlas-graph-scope-name')).toBeNull();
});

it('K2: Back and Forward walk the recent scopes, name their targets and stop at both ends', async () => {
    await openScope();
    expect(named('Back')?.disabled).toBe(false);
    expect(named('Back')?.title).toBe('Back to All graph (Alt+Left)');
    expect(named('Forward')?.disabled).toBe(true);
    await act(async () => button('Expand +1')!.click());
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n4'); });
    await settle(() => expect(scopeName()).toBe('n4'));
    expect(named('Back')?.title).toBe('Back to n1 · 2 layers (Alt+Left)');

    await act(async () => named('Back')!.click());
    await settle(() => expect(seam().nodes).toBe(5));
    expect(scopeName()).toBe('n1');
    expect(layers()).toBe('2 layers');
    expect(named('Forward')?.title).toBe('Forward to n4 · 1 layer (Alt+Right)');
    await act(async () => named('Back')!.click());
    await settle(() => expect(layers()).toBe('1 layer'));
    await act(async () => named('Back')!.click());
    await settle(() => expect(host.querySelector('.atlas-graph-scope-name')).toBeNull());
    expect(seam().nodes).toBe(5);
    expect(named('Back')?.disabled).toBe(true);

    await act(async () => named('Forward')!.click());
    await settle(() => expect(scopeName()).toBe('n1'));
    expect(layers()).toBe('1 layer');
    // A new navigation after going back drops the forward branch.
    await act(async () => { seam().clickNode('sample.n3'); });
    await settle(() => expect(scopeName()).toBe('n3'));
    expect(named('Forward')?.disabled).toBe(true);
});

it('K2: a project that arrives after the first render starts its history with All graph', async () => {
    const { fetch } = scopeFetch({ nodes, edges });
    await act(async () => root.render(<GalaxyPanel project="" visible workspaceExpanded onOpenNode={vi.fn()} fetch={fetch} />));
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={fetch} />));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(scopeName()).toBe('n1'));
    expect(seam().history).toMatchObject({ index: 1, entries: ['All graph', 'n1 · 1 layer'] });
    expect(named('Back')?.title).toBe('Back to All graph (Alt+Left)');
    // A project switch with an open scope starts again from the new project's whole graph.
    await act(async () => root.render(<GalaxyPanel project="other" visible workspaceExpanded onOpenNode={vi.fn()} fetch={fetch} />));
    await settle(() => expect(host.querySelector('.atlas-graph-scope-name')).toBeNull());
    expect(seam().history.entries).toEqual(['All graph']);
});

it('K2: All graph is an entry, so Back returns to the scope that was open', async () => {
    await openScope();
    await act(async () => button('All graph')!.click());
    await settle(() => expect(host.querySelector('.atlas-graph-scope-name')).toBeNull());
    expect(named('Back')?.title).toBe('Back to n1 · 1 layer (Alt+Left)');
    await act(async () => named('Back')!.click());
    await settle(() => expect(scopeName()).toBe('n1'));
});

it('K2: Back restores the trace direction and the path that a click on empty canvas cleared', async () => {
    await openScope();
    await pickPath('n2');
    await act(async () => button('Empty canvas')!.click());
    expect(pathPanel()).toBeNull();
    expect(named('Back')?.title).toBe('Back to n1 · 1 layer · path to n2 (Alt+Left)');
    await act(async () => named('Back')!.click());
    expect(pathPanel()?.textContent).toContain('Path to n2');

    await act(async () => {
        const select = host.querySelector<HTMLSelectElement>('select[aria-label="Trace direction"]')!;
        select.value = 'inbound'; select.dispatchEvent(new Event('change', { bubbles: true }));
    });
    await settle(() => expect(seam().nodes).toBe(2));
    await act(async () => named('Back')!.click());
    await settle(() => expect(seam().nodes).toBe(4));
    expect(host.querySelector<HTMLSelectElement>('select[aria-label="Trace direction"]')!.value).toBe('both');
});

it('K2: Alt+Left and Alt+Right go back and forward, but not while typing', async () => {
    await openScope();
    await act(async () => { seam().clickNode('sample.n2'); });
    await settle(() => expect(scopeName()).toBe('n2'));
    const field = host.querySelector('input[aria-label="Find a graph node"]')!;
    expect(key({ key: 'ArrowLeft', altKey: true }, field).defaultPrevented).toBe(false);
    expect(scopeName()).toBe('n2');
    expect(key({ key: 'ArrowLeft', altKey: true }).defaultPrevented).toBe(true);
    await settle(() => expect(scopeName()).toBe('n1'));
    expect(key({ key: 'ArrowRight', altKey: true }).defaultPrevented).toBe(true);
    await settle(() => expect(scopeName()).toBe('n2'));
    // Without Alt the arrows stay with the page.
    expect(key({ key: 'ArrowLeft' }).defaultPrevented).toBe(false);
});

it('K2: the recent list jumps straight to an earlier root with its last depth', async () => {
    await openScope();
    await act(async () => button('Expand +1')!.click());
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n4'); });
    await settle(() => expect(scopeName()).toBe('n4'));
    await act(async () => { seam().clickNode('sample.n5'); });
    await settle(() => expect(scopeName()).toBe('n5'));
    const recent = host.querySelector<HTMLDetailsElement>('details.atlas-graph-recent')!;
    expect([...recent.querySelectorAll('li button strong')].map(entry => entry.textContent)).toEqual(['n5', 'n4', 'n1']);
    recent.open = true;
    await act(async () => [...recent.querySelectorAll<HTMLButtonElement>('li button')].find(entry => entry.querySelector('strong')?.textContent === 'n1')!.click());
    await settle(() => expect(scopeName()).toBe('n1'));
    expect(layers()).toBe('2 layers');
    expect(named('Forward')?.disabled).toBe(true);
    expect(named('Back')?.title).toBe('Back to n5 · 1 layer (Alt+Left)');
});

/* Handtest 2026-10-04 (A2): die Liste blieb ueber der Szene offen, Schritt um Schritt. */
it('A2: the recent list closes on a press elsewhere, on Escape without leaving the scope, and when Back changes the root', async () => {
    await openScope();
    await act(async () => { seam().clickNode('sample.n2'); });
    await settle(() => expect(scopeName()).toBe('n2'));
    const recent = host.querySelector<HTMLDetailsElement>('details.atlas-graph-recent')!;
    await act(async () => { recent.open = true; });
    await act(async () => { button('Empty canvas')!.dispatchEvent(new MouseEvent('pointerdown', { bubbles: true })); });
    expect(recent.open).toBe(false);

    await act(async () => { recent.open = true; });
    expect(key({ key: 'Escape' }, recent.querySelector('summary')!).defaultPrevented).toBe(true);
    expect(recent.open).toBe(false);
    expect(scopeName()).toBe('n2');

    await act(async () => { recent.open = true; });
    await act(async () => named('Back')!.click());
    await settle(() => expect(scopeName()).toBe('n1'));
    expect(recent.open).toBe(false);
});

/*
 * Wie App.tsx: die Auswahl lebt ausserhalb des Panels, "Selection details"
 * steht nur, solange sie besteht, und `onClearSelection` loescht sie.
 */
function SelectionHarness({ fetch }: { fetch: typeof globalThis.fetch }) {
    const [selected, setSelected] = useState<string | undefined>();
    return <GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={fetch}
        onSelectNode={node => setSelected(node.name)} onClearSelection={() => setSelected(undefined)}
        selectionPanel={selected ? <p data-testid="selected-node">{selected}</p> : undefined} />;
}
const selectedNode = () => host.querySelector('[data-testid="selected-node"]')?.textContent;

it('K2/K8: Back to the same root at another depth, and a cancelled layer, keep the selection and its details', async () => {
    let block = false, release: () => void = () => {};
    const { fetch } = scopeFetch({ nodes, edges, gate: async () => {
        if (block) await new Promise<void>(resolve => { release = resolve; });
    } });
    await act(async () => root.render(<SelectionHarness fetch={fetch} />));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('4 nodes · 3 edges'));
    expect(selectedNode()).toBe('n1');
    await act(async () => button('Expand +1')!.click());
    await settle(() => expect(layers()).toBe('2 layers'));
    expect(selectedNode()).toBe('n1');

    // Back to the same root one layer less: the root stays selected, Selection details stays open to read.
    await act(async () => named('Back')!.click());
    await settle(() => expect(layers()).toBe('1 layer'));
    expect(scopeName()).toBe('n1');
    expect(selectedNode()).toBe('n1');
    expect(host.querySelector('.atlas-galaxy-selection-details')).not.toBeNull();
    await act(async () => named('Forward')!.click());
    await settle(() => expect(layers()).toBe('2 layers'));
    expect(selectedNode()).toBe('n1');

    // "−" while a layer loads cancels back one step; the selection is the same root and stays.
    block = true;
    await act(async () => button('Expand +1')!.click());
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.getAttribute('data-state')).toBe('loading'));
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Remove graph layer"]')!.click());
    await settle(() => expect(layers()).toBe('2 layers'));
    expect(selectedNode()).toBe('n1');
    expect(host.querySelector('.atlas-galaxy-selection-details')).not.toBeNull();
    block = false;
    await act(async () => { release(); });

    // Back to All graph still clears it: the whole graph has no selected root.
    await act(async () => button('All graph')!.click());
    await settle(() => expect(host.querySelector('.atlas-graph-scope-name')).toBeNull());
    expect(selectedNode()).toBeUndefined();
});
