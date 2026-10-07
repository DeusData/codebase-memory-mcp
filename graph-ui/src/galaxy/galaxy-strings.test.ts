import { expect, it } from 'vitest';
import { galaxyHierarchyText, galaxyLayerText } from './galaxy-strings';
import { expandPastLimit, type ExpandOutlook } from './graph-scope';

/*
 * Hand test 2026-10-04 (G3): at two layers of JSONBAgg the tooltip said
 * "Likely past the render limit", then "growing like the last layer it adds
 * about 402 nodes" (90 + 402 is far below 5,000), and "Loading stops at the
 * render limit of 5,000 nodes", while the layer then ended at 5,548 nodes.
 * Every sentence is true now: the warning names the limit and the number that
 * drives it, and the limit sentence says what loading really does.
 */
it('G3: the Expand hint names what drives the warning and what the render limit really does', () => {
    const n = (value: number) => value.toLocaleString();
    const outlook: ExpandOutlook = { layer: 3, frontier: 75, estimate: 402, calls: 9031, loaded: { nodes: 90, edges: 201 }, limits: { nodes: 5000, edges: 20000 } };
    const limit = `Loading stops after the first request to the index that takes it past ${n(5000)} nodes or ${n(20000)} edges, so the layer can end above the limit; `
        + `it is then marked partial, and the scene draws at most ${n(5000)} nodes and ${n(20000)} edges.`;
    expect(galaxyLayerText.expandHint(outlook, expandPastLimit(outlook))).toBe(`Likely past the render limit of ${n(5000)} nodes. Load layer 3: 75 nodes to expand. `
        + `The index lists ${n(9031)} calls at them that are not loaded yet, and each can bring a new node; 90 nodes are loaded now. ${limit}`);
    // The smaller growth estimate is not offered next to the warning it would contradict.
    expect(galaxyLayerText.expandHint(outlook, expandPastLimit(outlook))).not.toContain('402');

    // Driven by the growth of the last layer, without a call count (a trace without CALLS).
    const growth = { ...outlook, calls: undefined, loaded: { nodes: 4700, edges: 9000 } };
    expect(galaxyLayerText.expandHint(growth, expandPastLimit(growth))).toBe(`Likely past the render limit of ${n(5000)} nodes. Load layer 3: 75 nodes to expand. `
        + `Growing like the last layer it adds about 402 nodes, and a hub can add many more; ${n(4700)} nodes are loaded now. ${limit}`);
    // Driven by the edge limit.
    const edges = { ...outlook, calls: 3000, loaded: { nodes: 90, edges: 18500 } };
    expect(galaxyLayerText.expandHint(edges, expandPastLimit(edges))).toBe(`Likely past the render limit of ${n(20000)} edges. Load layer 3: 75 nodes to expand. `
        + `The index lists ${n(3000)} calls at them that are not loaded yet, and each can bring a new node; ${n(18500)} edges are loaded now. ${limit}`);
    // No warning: no "loaded now", and the growth estimate stands where it is the larger number.
    const quiet = { ...outlook, calls: 120 };
    expect(galaxyLayerText.expandHint(quiet, expandPastLimit(quiet))).toBe('Load layer 3: 75 nodes to expand. '
        + `The index lists 120 calls at them that are not loaded yet; growing like the last layer it adds about 402 nodes. ${limit}`);
    // The Explore mini-Galaxy loads without render limits, and the hint claims none.
    const explore = { ...outlook, limits: undefined };
    expect(galaxyLayerText.expandHint(explore, expandPastLimit(explore))).toBe('Load layer 3: 75 nodes to expand. '
        + `The index lists ${n(9031)} calls at them that are not loaded yet, and each can bring a new node.`);
    // A partial layer says it stopped after passing the limit, not at it.
    expect(galaxyLayerText.partialTitle(3, 5000, 'nodes')).toBe(`Layer 3 stopped loading after the request that took it past the render limit of ${n(5000)} nodes, `
        + `so it is incomplete; the scene draws at most ${n(5000)} nodes. Raise the limit under Limits or trace fewer edge types to load all of it.`);
    expect(galaxyLayerText.expandPartial).toBe('This layer stopped loading after it passed the render limit, so there is no complete edge to grow from. Raise the limit under Limits first.');
});

/*
 * Second review of K5: the hint and the note in the picture say what carries
 * a name above the budget, for every depth and trace direction, and only give
 * advice that helps there.
 */
it('K5: the hierarchy hint and note say who is named and what brings the rest back', () => {
    const view = (names: 'all' | 'neighbours' | 'none', sides = { root: true, incoming: 0, outgoing: 0 }) => ({ mixed: 0, names, budget: 150, neighbours: 554, sides });
    expect(galaxyHierarchyText.hint('both', { ...view('all'), mixed: 3 }))
        .toBe('hierarchy: incoming relationships on the left, the root in the middle, outgoing on the right; each column is one layer further in the same direction; '
            + '3 nodes reached through both directions, such as a callee of a caller, stand in the band below; the type and direction of each relationship at its line');
    expect(galaxyHierarchyText.hint('inbound', view('neighbours'))).toBe('hierarchy: what reaches the root, one column per layer to the left; '
        + 'above 150 nodes only the root and its direct neighbours carry names, so remove layers or trace fewer edge types to see all names');
    // One layer with a hub: removing a layer is no advice; one side still fits the budget.
    expect(galaxyHierarchyText.hint('both', view('none', { root: true, incoming: 0, outgoing: 11 })))
        .toContain('and the root has 554 direct neighbours, so only the root and its 11 outgoing neighbours carry names; trace one direction or fewer edge types to see the rest');
    expect(galaxyHierarchyText.hint('outbound', view('none'))).toContain('so only the root carries a name; trace fewer edge types to see the rest');
    expect(galaxyHierarchyText.namesNote('both', 555, 'none', 554, 150, { root: true, incoming: 0, outgoing: 11 }))
        .toBe('555 nodes, 554 of them direct neighbours of the root: names for the root and its 11 outgoing neighbours only (up to 150 names). Trace one direction or fewer edge types to see the rest.');
    // Hundreds of roots (a folder): nobody is named, and the note says so.
    expect(galaxyHierarchyText.namesNote('inbound', 1687, 'none', 1319, 150, { root: false, incoming: 0, outgoing: 0 }))
        .toBe(`${(1687).toLocaleString()} nodes: no names above 150 nodes. Trace fewer edge types to see them.`);
    expect(galaxyHierarchyText.hint('both', view('none', { root: false, incoming: 0, outgoing: 0 })))
        .toContain('names and edge types show for up to 150 nodes, so trace one direction or fewer edge types to see them');
    expect(galaxyHierarchyText.bandTitle(1)).toBe('Mixed directions · 1 node');
});
