// @vitest-environment jsdom
/*
 * Handtest K5: die Hierarchie eines Ausschnitts. Eingehendes links, Wurzel in
 * der Mitte, Ausgehendes rechts, ein ehrlicher Hinweis, und Pfad und
 * Aufrufreihe gibt es auch hier, mit Hervorhebung im Bild der Hierarchie.
 */
import { act, isValidElement } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel from './GalaxyPanel';
import type { GraphData } from './types';
import type { ScenePath } from './PathLayer';
import { scopeFetch, scopeNode } from './test-scope-fetch';
import { GALAXY_LEGEND_KEY } from './galaxy-legend';
import { HierarchyBandLabel, HierarchyEdgeLabels } from './HierarchyEdgeLabels';

const scene = vi.hoisted(() => ({ data: undefined as GraphData | undefined, path: undefined as ScenePath | undefined, highlighted: null as Set<number> | null,
    labelMaxTextWidth: undefined as number | undefined, overlay: false, overlayNode: undefined as unknown, showLabels: false, labelBudget: undefined as number | undefined,
    labelIds: undefined as ReadonlySet<number> | undefined }));
vi.mock('./GraphScene', async importOriginal => ({
    ...await importOriginal<typeof import('./GraphScene')>(),
    GraphScene: ({ data, path, highlightedIds, labelMaxTextWidth, overlay, showLabels, labelBudget, labelIds }: { data: GraphData; path?: ScenePath; highlightedIds: Set<number> | null;
        labelMaxTextWidth?: number; overlay?: unknown; showLabels: boolean; labelBudget?: number; labelIds?: ReadonlySet<number> }) => {
        scene.data = data; scene.path = path; scene.highlighted = highlightedIds; scene.labelMaxTextWidth = labelMaxTextWidth; scene.overlay = Boolean(overlay);
        scene.overlayNode = overlay; scene.showLabels = showLabels; scene.labelBudget = labelBudget; scene.labelIds = labelIds;
        return <output data-testid="scene" />;
    },
}));
/** Ein Element der Ueberlagerung, ohne es zu zeichnen. */
function overlayProps<T>(node: unknown, type: unknown): T | undefined {
    if (!isValidElement(node)) return Array.isArray(node) ? node.map(entry => overlayProps<T>(entry, type)).find(Boolean) : undefined;
    if (node.type === type) return node.props as T;
    return overlayProps<T>((node.props as { children?: unknown }).children, type);
}
/** Die Kantenschilder der Hierarchie im Baum der Ueberlagerung. */
const edgeLabelProps = (node: unknown) => overlayProps<{ edges: { source: number; target: number }[] }>(node, HierarchyEdgeLabels);

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

it('K5: the hierarchy of a scope puts callers left and callees right, says so, and keeps Path to and Call order', async () => {
    const nodes = [1, 2, 3, 4, 5].map(id => scopeNode(id));
    const edges = [{ id: 1, source: 1, target: 2, type: 'CALLS', line: 30 }, { id: 2, source: 1, target: 3, type: 'CALLS', line: 12 },
        { id: 3, source: 4, target: 1, type: 'CALLS', line: 2 }, { id: 4, source: 5, target: 1, type: 'TESTS' }];
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={scopeFetch({ nodes, edges }).fetch}
        legendStore={{ getItem: (key: string) => (key === GALAXY_LEGEND_KEY ? 'open' : null), setItem: vi.fn() } as unknown as Storage} />));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('5 nodes · 4 edges'));
    await act(async () => host.querySelector<HTMLButtonElement>('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.click());
    expect(seam().mode).toBe('hierarchy');

    const placed = Object.fromEntries(seam().hierarchy!.placements.map(placement => [placement.name, placement]));
    expect(placed.n1).toMatchObject({ x: 0, y: 0 });
    expect(placed.n4!.x).toBeLessThan(0); expect(placed.n5!.x).toBeLessThan(0);
    expect(placed.n2!.x).toBeGreaterThan(0); expect(placed.n3!.y).toBeGreaterThan(placed.n2!.y); // line 12 above line 30
    // Full names, the edge types at the lines, and an honest description.
    expect(scene.labelMaxTextWidth).toBeGreaterThanOrEqual(1600);
    expect(scene.overlay).toBe(true);
    const chip = host.querySelector('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.closest('[data-hint-name], span, div')!;
    expect(host.innerHTML).toContain('incoming relationships on the left, the root in the middle, outgoing on the right');
    expect(chip).toBeTruthy();
    expect(host.querySelector('[data-entry="positions"]')?.textContent).toContain('Incoming relationships on the left');
    expect(host.querySelector('[data-entry="positions"]')?.textContent).not.toContain('ordered by name');

    // Call order and Path to work in this picture and highlight its own nodes.
    await act(async () => button('Call order')!.click());
    const sceneId = (name: string) => scene.data!.nodes.find(node => node.name === name)!.id;
    expect(scene.path?.steps.map(step => [step.from, step.to])).toEqual([[sceneId('n1'), sceneId('n3')], [sceneId('n1'), sceneId('n2')]]);
    expect([...scene.highlighted ?? []].sort()).toEqual([sceneId('n1'), sceneId('n2'), sceneId('n3')].sort());
    await act(async () => button('Call order')!.click());
    host.querySelector<HTMLDetailsElement>('.atlas-graph-path-picker')!.open = true;
    await act(async () => [...host.querySelectorAll<HTMLButtonElement>('.atlas-graph-path-menu li button')]
        .find(entry => entry.querySelector('strong')?.textContent === 'n5')!.click());
    expect(host.querySelector('[data-testid="atlas-galaxy-path-panel"] strong')?.textContent).toBe('Path to n5 · 1 hop');
    expect(scene.path?.steps.map(step => [step.edge.source, step.edge.target])).toEqual([[sceneId('n5'), sceneId('n1')]]);
});

it('K5: the hint follows the trace direction', async () => {
    const nodes = [1, 2].map(id => scopeNode(id));
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()}
        fetch={scopeFetch({ nodes, edges: [{ id: 1, source: 1, target: 2, type: 'CALLS' }] }).fetch} />));
    await settle(() => expect(seam().nodes).toBe(2));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('2 nodes · 1 edge'));
    await act(async () => {
        const select = host.querySelector<HTMLSelectElement>('select[aria-label="Trace direction"]')!;
        select.value = 'outbound'; select.dispatchEvent(new Event('change', { bubbles: true }));
    });
    expect(host.innerHTML).toContain('what the root reaches, one column per layer to the right');
});

/*
 * Review of K5: above sixty nodes the hierarchy of a scope drew no name at all
 * (JSONBAgg at two layers, 90 nodes) but eighty edge-type labels. Names now
 * stand up to 150 nodes. Above that the root and its direct neighbours keep
 * theirs (second review), and only a hub with more neighbours than that has
 * none; a note in the picture says so and how to get them back.
 */
const renderFan = async (callers: number, outer = 0) => {
    const first = Array.from({ length: callers }, (_, at) => scopeNode(100 + at));
    const second = Array.from({ length: outer }, (_, at) => scopeNode(1000 + at));
    const nodes = [scopeNode(1), ...first, ...second];
    const edges = [...first.map((node, at) => ({ id: 10 + at, source: node.id, target: 1, type: 'CALLS' })),
        ...second.map((node, at) => ({ id: 5000 + at, source: node.id, target: first[at % callers]!.id, type: 'CALLS' }))];
    await act(async () => root.render(<GalaxyPanel key={`${callers}:${outer}`} project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={scopeFetch({ nodes, edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBe(nodes.length));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe(`${callers + 1} nodes · ${callers} edges`));
    await act(async () => host.querySelector<HTMLButtonElement>('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.click());
    if (outer > 0) {
        await act(async () => button('Expand +1')!.click());
        await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe(`${nodes.length} nodes · ${edges.length} edges`));
    }
};
const hierarchyHint = () => host.querySelector('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.closest('[data-hint]')?.getAttribute('data-hint') ?? host.innerHTML;

it('K5: a two-layer scope of 90 nodes keeps its names in the hierarchy; a hub of 200 direct neighbours names only its root, and the picture says so', async () => {
    await renderFan(89);
    expect(scene.showLabels).toBe(true);
    expect(scene.labelBudget).toBeGreaterThanOrEqual(90);
    expect(scene.labelIds).toBeUndefined();
    expect(edgeLabelProps(scene.overlayNode)?.edges).toHaveLength(89);
    expect(host.innerHTML).not.toContain('names and edge types show');
    expect(host.querySelector('[data-testid="atlas-hierarchy-key"]')).toBeNull();

    await renderFan(200);
    // Only the root keeps its name; no edge label stands between two named nodes.
    expect(scene.showLabels).toBe(true);
    expect([...scene.labelIds ?? []].map(id => scene.data!.nodes.find(node => node.id === id)?.name)).toEqual(['n1']);
    expect(edgeLabelProps(scene.overlayNode)).toBeUndefined();
    expect(hierarchyHint()).toContain('names and edge types show for up to 150 nodes, and the root has 200 direct neighbours, so only the root carries a name; trace one direction or fewer edge types to see the rest');
    expect(host.querySelector('[data-testid="atlas-hierarchy-key"]')?.textContent)
        .toBe('201 nodes, 200 of them direct neighbours of the root: names for the root only (up to 150 names). Trace one direction or fewer edge types to see the rest.');
});

it('K5: above 150 nodes the root and its direct neighbours keep their names and edge labels, and the note says how to see the rest', async () => {
    await renderFan(40, 160);
    expect(scene.showLabels).toBe(true);
    const named = new Set([...scene.labelIds ?? []].map(id => scene.data!.nodes.find(node => node.id === id)?.name));
    expect(named.size).toBe(41);
    expect(named.has('n1')).toBe(true);
    expect(named.has('n100')).toBe(true);
    expect(named.has('n1000')).toBe(false);
    // Edge labels only where both ends carry a name: the lines at the root.
    const edges = edgeLabelProps(scene.overlayNode)?.edges ?? [];
    expect(edges).toHaveLength(40);
    expect(hierarchyHint()).toContain('only the root and its direct neighbours carry names');
    expect(host.querySelector('[data-testid="atlas-hierarchy-key"]')?.textContent).toMatch(/201 nodes: names for the root and its 40 direct neighbours only.*Remove layers or trace fewer edge types/);
});

/*
 * Review of K5, second round: from two layers on the columns mixed the
 * directions again. A node reached through both stands in a band of its
 * own, the band carries a heading in the picture, and the hint counts it.
 */
it('K5: at two layers a callee of a caller stands in the labelled band, and the hint says so; one layer has no band', async () => {
    const nodes = [1, 10, 20, 40, 60].map(id => scopeNode(id));
    const edges = [{ id: 1, source: 10, target: 1, type: 'CALLS' }, { id: 2, source: 10, target: 20, type: 'CALLS' },
        { id: 3, source: 1, target: 40, type: 'INHERITS' }, { id: 4, source: 60, target: 40, type: 'INHERITS' }];
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={scopeFetch({ nodes, edges }).fetch}
        legendStore={{ getItem: (key: string) => (key === GALAXY_LEGEND_KEY ? 'open' : null), setItem: vi.fn() } as unknown as Storage} />));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('3 nodes · 2 edges'));
    await act(async () => host.querySelector<HTMLButtonElement>('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.click());
    expect(overlayProps(scene.overlayNode, HierarchyBandLabel)).toBeUndefined();
    expect(hierarchyHint()).not.toContain('band');
    expect(hierarchyHint()).toContain('the type and direction of each relationship at its line');

    await act(async () => button('Expand +1')!.click());
    await settle(() => expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('5 nodes · 4 edges'));
    const placed = Object.fromEntries(seam().hierarchy!.placements.map(placement => [placement.name, placement]));
    expect(placed.n20).toMatchObject({ mixed: true });
    expect(placed.n60).toMatchObject({ mixed: true });
    expect(placed.n10!.x).toBeLessThan(0);
    expect(placed.n40!.x).toBeGreaterThan(0);
    expect(overlayProps<{ band: { count: number } }>(scene.overlayNode, HierarchyBandLabel)?.band.count).toBe(2);
    expect(hierarchyHint()).toContain('2 nodes reached through both directions, such as a callee of a caller, stand in the band below');
    // The legend explains the band as well.
    expect(host.querySelector('[data-entry="positions"]')?.textContent).toContain('band below');
});
