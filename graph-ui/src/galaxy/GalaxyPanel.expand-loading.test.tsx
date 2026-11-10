// @vitest-environment jsdom
/*
 * Review zu K31: waehrend eine Ebene laedt, ist Expand +1 gesperrt
 * (aria-disabled), nimmt Zeiger und Fokus, zeigte aber keinen Tooltip: der
 * Text fuer diesen Zustand fehlte, und ein leerer Hinweis zeigt nichts. Jetzt
 * sagt der Knopf, welche Ebene laedt und dass "−" sie abbricht.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel from './GalaxyPanel';
import { galaxyLayerText } from './galaxy-strings';
import { scopeFetch, scopeNode } from './test-scope-fetch';
import type { GraphEdge } from './types';

vi.mock('./GraphScene', async importOriginal => ({
    ...await importOriginal<typeof import('./GraphScene')>(),
    GraphScene: () => <output data-testid="scene" />,
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

const settle = (check: () => void) => vi.waitFor(async () => {
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
    check();
});
const status = () => host.querySelector('.atlas-graph-scope-count');
const expand = () => [...host.querySelectorAll('button')].find(entry => entry.getAttribute('aria-label') === 'Expand +1')!;
const shown = () => document.querySelector<HTMLElement>('[data-testid="atlas-hint"][data-hint-for="galaxy-expand"]');

it('K31: while a layer loads, the blocked Expand +1 says which layer loads and that "−" cancels it, on hover and on focus', async () => {
    const nodes = [1, 2, 3, 4, 5, 6].map(id => scopeNode(id));
    const edges: GraphEdge[] = [{ id: 1, source: 1, target: 2, type: 'CALLS' }, { id: 2, source: 2, target: 3, type: 'CALLS' },
        { id: 3, source: 4, target: 2, type: 'CALLS' }, { id: 4, source: 3, target: 5, type: 'CALLS' }, { id: 5, source: 6, target: 1, type: 'TESTS' }];
    let block = false, release: () => void = () => {}, queries = 0;
    const { fetch } = scopeFetch({ nodes, edges, gate: async () => {
        queries += 1;
        if (block && queries > 1) await new Promise<void>(resolve => { release = resolve; });
    } });
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={fetch} />));
    await settle(() => expect(globalThis.__atlasGalaxy?.nodes).toBe(6));
    await act(async () => { globalThis.__atlasGalaxy!.clickNode('sample.n1'); });
    await settle(() => expect(status()?.textContent).toBe('3 nodes · 2 edges'));

    block = true; queries = 0;
    await act(async () => expand().click());
    await settle(() => expect(status()?.getAttribute('data-state')).toBe('loading'));
    expect(expand().getAttribute('aria-disabled')).toBe('true');
    expect(expand().getAttribute('data-hint')).toBe('Layer 2 is loading; "−" cancels it.');
    expect(galaxyLayerText.expandLoading(2)).toBe('Layer 2 is loading; "−" cancels it.');

    await act(async () => { expand().dispatchEvent(new MouseEvent('mouseover', { bubbles: true })); });
    expect(shown()?.textContent).toBe('Layer 2 is loading; "−" cancels it.');
    await act(async () => { expand().dispatchEvent(new MouseEvent('mouseout', { bubbles: true })); });
    expect(shown()).toBeNull();
    await act(async () => { expand().focus(); });
    expect(shown()?.textContent).toBe('Layer 2 is loading; "−" cancels it.');
    // A press while it loads still does nothing.
    await act(async () => expand().click());
    expect([...host.querySelectorAll('.atlas-graph-exploration span')].some(entry => entry.textContent === '2 layers')).toBe(true);
    await act(async () => { expand().blur(); release(); });
    await settle(() => expect(status()?.getAttribute('data-state')).not.toBe('loading'));
    // Loaded: the tooltip speaks about the next layer again.
    expect(expand().getAttribute('data-hint') ?? '').not.toContain('is loading');
});
