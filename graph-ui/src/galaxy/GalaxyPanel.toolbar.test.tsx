// @vitest-environment jsdom
/*
 * Handtest K3: die Leiste eines Ausschnitts bleibt auch bei offenem Chat
 * einzeilig. jsdom misst keine Breiten; hier steht der Aufbau, der das
 * moeglich macht (die Messung selbst steht in tools/handtest-fixes-galaxy.mjs):
 * die Wurzel ist zugleich der Knopf zur Quelle, Limits, Gruppen und der
 * Hinweis auf abgeschnittene Knoten stehen im Menue "⋯", und die Zeile bricht
 * nicht um.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel from './GalaxyPanel';
import { scopeFetch, scopeNode } from './test-scope-fetch';

vi.mock('./GraphScene', async importOriginal => ({
    ...await importOriginal<typeof import('./GraphScene')>(),
    GraphScene: () => <output data-testid="scene" />,
}));

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

it('K3: the scoped toolbar keeps one row: root name opens the source, limits and groups sit in the ⋯ menu', async () => {
    const nodes = [1, 2, 3].map(id => scopeNode(id));
    const edges = [{ id: 1, source: 1, target: 2, type: 'CALLS' }, { id: 2, source: 3, target: 1, type: 'CALLS' }];
    const onOpenNode = vi.fn();
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={onOpenNode} fetch={scopeFetch({ nodes, edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBe(3));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('3 nodes · 2 edges'));

    const bar = host.querySelector<HTMLElement>('.atlas-graph-exploration')!;
    expect(bar.dataset.scoped).toBe('true');
    // The root is the way to its source; no separate "Open source" button in the row.
    const name = bar.querySelector<HTMLButtonElement>('button.atlas-graph-scope-name')!;
    expect(name.textContent).toBe('n1');
    expect(name.title).toBe('Open the source of n1 in Explore: src/n1.ts:1');
    expect([...bar.querySelectorAll(':scope > button')].map(entry => entry.textContent)).not.toContain('Open source');
    await act(async () => name.click());
    expect(onOpenNode).toHaveBeenCalledWith(expect.objectContaining({ id: 1 }));
    // Limits live in the ⋯ menu, and so does the open-source action for keyboard users.
    const more = bar.querySelector<HTMLDetailsElement>('details.atlas-graph-more')!;
    expect(more.querySelector('select[aria-label="Rendered node limit"]')).not.toBeNull();
    expect(more.querySelector('select[aria-label="Rendered edge limit"]')).not.toBeNull();
    expect(bar.querySelector(':scope > .atlas-graph-scope-groups')).toBeNull();
    // The trace direction keeps its accessible name without the visible word.
    expect(bar.querySelector('select[aria-label="Trace direction"]')).not.toBeNull();
    expect(bar.textContent).not.toContain('Trace');
});

it('K2: the history group and the root button say what they are to assistive technology', async () => {
    const nodes = [1, 2, 3].map(id => scopeNode(id));
    const edges = [{ id: 1, source: 1, target: 2, type: 'CALLS' }, { id: 2, source: 3, target: 1, type: 'CALLS' }];
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={scopeFetch({ nodes, edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBe(3));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('3 nodes · 2 edges'));
    await act(async () => { seam().clickNode('sample.n2'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-name')?.textContent).toBe('n2'));

    // Back and Forward are the history, not the list of recent roots inside it.
    const group = host.querySelector<HTMLElement>('.atlas-graph-history')!;
    expect(group.getAttribute('role')).toBe('group');
    expect(group.getAttribute('aria-label')).toBe('History');
    expect(group.querySelector('ul')?.getAttribute('aria-label')).toBe('Recently visited roots');
    // The root button names its action and keeps the visible name in it.
    const name = host.querySelector<HTMLButtonElement>('button.atlas-graph-scope-name')!;
    expect(name.getAttribute('aria-label')).toBe('Open the source of n2');
    expect(name.textContent).toBe('n2');
    // K2 as planned: "← Back" and "Forward →" in words while the row has room, the glyphs alone in its compact levels.
    const back = group.querySelector<HTMLButtonElement>('button[aria-label="Back"]')!;
    const forward = group.querySelector<HTMLButtonElement>('button[aria-label="Forward"]')!;
    expect(back.querySelector('.atlas-fit-wide')?.textContent).toBe('← Back');
    expect(back.querySelector('.atlas-fit-narrow')?.getAttribute('data-label')).toBe('←');
    expect(forward.querySelector('.atlas-fit-wide')?.textContent).toBe('Forward →');
    expect(forward.querySelector('.atlas-fit-narrow')?.getAttribute('data-label')).toBe('→');
    // The tooltips stay.
    expect(back.title).toBe('Back to n1 · 1 layer (Alt+Left)');
    expect(forward.title).toBe('Nothing to go forward to');
});

it('K3: the scoped toolbar measures its fit and carries a short label for each wide control', async () => {
    const nodes = [1, 2, 3].map(id => scopeNode(id));
    const edges = [{ id: 1, source: 1, target: 2, type: 'CALLS' }, { id: 2, source: 3, target: 1, type: 'CALLS' }];
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={scopeFetch({ nodes, edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBe(3));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('3 nodes · 2 edges'));
    const bar = host.querySelector<HTMLElement>('.atlas-graph-exploration')!;
    // jsdom lays nothing out, so everything fits.
    expect(bar.dataset.fit).toBe('full');
    const narrow = (element: Element | null | undefined) => element?.querySelector('.atlas-fit-narrow')?.getAttribute('data-label');
    const expand = [...bar.querySelectorAll('button')].find(entry => entry.textContent === 'Expand +1')!;
    expect(expand.getAttribute('aria-label')).toBe('Expand +1');
    expect(narrow(expand)).toBe('+1');
    expect(narrow(bar.querySelector('.atlas-graph-path-picker > summary'))).toBe('Path…');
    expect(narrow(bar.querySelector('.atlas-trace-edge-filter > summary'))).toBe('Types · All');
    expect(narrow([...bar.querySelectorAll('button')].find(entry => entry.textContent === 'Call order'))).toBe('Calls');
    // At 1,494 px with the chat open and the recent list shown, the short "All" keeps the count whole.
    const allGraph = [...bar.querySelectorAll('button')].find(entry => entry.textContent === 'All graph');
    expect(narrow(allGraph)).toBe('All');
    expect(allGraph?.getAttribute('aria-label')).toBe('All graph');
    expect(allGraph?.title).toBe('Leave the scope and show the whole graph (Esc)');
    // The short label is drawn by CSS only: the text of each control stays what it was.
    expect(bar.querySelector('.atlas-trace-edge-filter > summary')?.textContent).toBe('Edge types · All');
    // Out of the scope the toolbar wraps as before and is not measured.
    await act(async () => [...bar.querySelectorAll('button')].find(entry => entry.textContent === 'All graph')!.click());
    await settle(() => expect(host.querySelector('.atlas-graph-scope-name')).toBeNull());
    expect(host.querySelector<HTMLElement>('.atlas-graph-exploration')!.dataset.fit).toBeUndefined();
});
