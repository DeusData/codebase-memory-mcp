// @vitest-environment jsdom
/*
 * Hand test of 2026-10-04 (H1): the chat answers "erkläre die aktuelle hierarchy" from
 * the loaded scope, and to say how to read the picture it has to know which picture
 * Galaxy shows. The evidence carries it; switching the picture is no new selection,
 * so the explanation of the scope is not written again.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel from './GalaxyPanel';
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

const seam = () => globalThis.__atlasGalaxy!;
const settle = (check: () => void) => vi.waitFor(async () => {
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
    check();
});
const status = () => host.querySelector('.atlas-graph-scope-count');

it('H1: tells the chat whether Galaxy shows the cloud or the hierarchy, with the same content identity', async () => {
    const nodes = [1, 2, 3].map(id => scopeNode(id));
    const edges: GraphEdge[] = [{ id: 1, source: 1, target: 2, type: 'CALLS' }, { id: 2, source: 3, target: 1, type: 'CALLS' }];
    const onSelectionEvidence = vi.fn();
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} onSelectionEvidence={onSelectionEvidence}
        fetch={scopeFetch({ nodes, edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBe(3));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(status()?.textContent).toBe('3 nodes · 2 edges'));
    const last = () => onSelectionEvidence.mock.lastCall![0] as { id: string; text: string };
    const display = () => JSON.parse(last().text).evidence.scope.display;
    await settle(() => expect(display()).toBe('galaxy'));
    const { id } = last();
    await act(async () => host.querySelector<HTMLButtonElement>('button.atlas-graph-mode-chip[data-mode="hierarchy"]')!.click());
    expect(seam().mode).toBe('hierarchy');
    await settle(() => expect(display()).toBe('hierarchy'));
    // The same scope in another picture: the chat keeps its explanation.
    expect(last().id).toBe(id);
    await act(async () => host.querySelector<HTMLButtonElement>('button.atlas-graph-mode-chip[data-mode="galaxy"]')!.click());
    await settle(() => expect(display()).toBe('galaxy'));
    expect(last().id).toBe(id);
});
