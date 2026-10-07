// @vitest-environment jsdom
/*
 * Handtest K13: "Selection details" bekommt in Galaxy den geladenen
 * Ausschnitt, nicht nur den gekappten Repository-Schnappschuss.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel from './GalaxyPanel';
import type { SelectionScope } from '../why/SelectionContext';
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

const settle = (check: () => void) => vi.waitFor(async () => {
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
    check();
});

it('K13: hands the loaded scope to the selection panel, with its completeness and trace', async () => {
    const nodes = [1, 2, 3, 4].map(id => scopeNode(id));
    const edges = [{ id: 1, source: 2, target: 1, type: 'CALLS' }, { id: 2, source: 2, target: 1, type: 'TESTS' },
        { id: 3, source: 3, target: 1, type: 'DEFINES' }, { id: 4, source: 1, target: 4, type: 'INHERITS' }];
    const seen: (SelectionScope | undefined)[] = [];
    const panel = (scope: SelectionScope | undefined) => { seen.push(scope); return <p>selection evidence</p>; };
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} selectionPanel={panel}
        fetch={scopeFetch({ nodes, edges }).fetch} />));
    await settle(() => expect(globalThis.__atlasGalaxy?.nodes).toBe(4));
    expect(seen.at(-1)).toBeUndefined();
    await act(async () => { globalThis.__atlasGalaxy!.clickNode('sample.n1'); });
    await settle(() => expect(seen.at(-1)?.complete).toBe(true));
    const scope = seen.at(-1)!;
    expect(scope.direction).toBe('both');
    expect(scope.depth).toBe(1);
    expect(scope.graph.edges.map(edge => edge.type).sort()).toEqual(['CALLS', 'DEFINES', 'INHERITS', 'TESTS']);
    expect(host.querySelector('.atlas-galaxy-selection-details')?.textContent).toContain('selection evidence');
});
