import type { ExpandOutlook } from './graph-scope';

/**
 * The words of the Galaxy path view, beside the view like the other domain
 * copy (agents/agent-strings.ts, architecture/strings.ts).
 *
 * One rule for every sentence here: a path is counted in hops over the
 * relationships that are loaded in this scope, nothing more. It is not a
 * runtime trace and it does not claim that nothing else connects the two.
 */
export const galaxyPathText = {
    pathTo: 'Path to…',
    pathToTitle: 'Highlight the shortest path from the root to a node in this scope',
    pathSearch: 'Find a path target',
    pathSearchPlaceholder: 'Node in this scope…',
    pathNoMatch: 'No matching node in the loaded scope.',
    pathMore: (count: number) => `${count.toLocaleString()} more, type to narrow`,
    callOrder: 'Call order',
    /** The short labels of a narrow toolbar (review of K3). */
    pathToNarrow: 'Path…',
    callOrderNarrow: 'Calls',
    callOrderTitle: 'Step through the outgoing calls of the root in source line order',
    callOrderUnavailable: 'The root has no outgoing calls in the loaded scope',
    panel: 'Path steps',
    pathHeading: (target: string, hops: number) => `Path to ${target} · ${hops === 1 ? '1 hop' : `${hops} hops`}`,
    callsHeading: (root: string, calls: number) => `Calls of ${root} · ${calls === 1 ? '1 call' : `${calls} calls`}`,
    noPath: (target: string) => `No path to ${target} over the loaded relationships in this trace direction. Expand the scope or trace both directions.`,
    isRoot: (target: string) => `${target} is the root of this scope.`,
    hop: (hop: number) => `hop ${hop}`,
    line: (line: number) => `line ${line}`,
    lineUnknown: 'no line',
    /** The hop as the reader follows it, with the indexed direction kept. */
    step: (from: string, type: string, to: string, forward: boolean) =>
        (forward ? `${from} --${type}--> ${to}` : `${from} <--${type}-- ${to}`),
    previous: 'Previous',
    next: 'Next',
    clear: 'Clear',
    clearTitle: 'Return to the whole scope (Esc)',
    position: (index: number, total: number) => `${index} of ${total}`,
};

/** Back, Forward and the recent roots of the scoped Galaxy (K2). */
export const galaxyHistoryText = {
    back: 'Back',
    forward: 'Forward',
    backGlyph: '←',
    forwardGlyph: '→',
    /** The visible words while the row has room (K2: "← Back" and "Forward →"); the glyphs alone in its compact levels. */
    backWide: '← Back',
    forwardWide: 'Forward →',
    backTo: (label: string) => `Back to ${label} (Alt+Left)`,
    forwardTo: (label: string) => `Forward to ${label} (Alt+Right)`,
    noBack: 'Nothing to go back to yet',
    noForward: 'Nothing to go forward to',
    recent: 'Recent',
    recentGlyph: '▾',
    recentTitle: 'Jump straight to a recently visited root with its last depth, direction and edge types',
    recentList: 'Recently visited roots',
    /** The accessible name of the Back, Forward and Recent group. */
    group: 'History',
    allGraph: 'All graph',
    allGraphNarrow: 'All',
    allGraphTitle: 'Leave the scope and show the whole graph (Esc)',
    layers: (depth: number) => (depth === 1 ? '1 layer' : `${depth} layers`),
    direction: { inbound: 'incoming', outbound: 'outgoing' } as Record<'inbound' | 'outbound', string>,
    noTypes: 'no edge types',
    hierarchy: 'hierarchy',
    callOrder: 'call order',
    pathTo: (name: string) => `path to ${name}`,
};

const count = (value: number, one: string, many: string) => `${value.toLocaleString()} ${value === 1 ? one : many}`;

/**
 * Who carries a name in the hierarchy of a scope: everyone, the root and its
 * direct neighbours, or (`none`) only the root and a side of its direct
 * neighbours that fits the budget.
 */
export type HierarchyNames = 'all' | 'neighbours' | 'none';
/** With `names: 'none'`: whether the root still carries its name (not with hundreds of roots) and how many direct neighbours on each side do. */
export interface HierarchyNamedSides { root: boolean; incoming: number; outgoing: number }

type TraceDirectionWord = 'both' | 'inbound' | 'outbound';
/* With the direct neighbours unnamed the root alone has too many: fewer layers would not help, a narrower trace would. */
const narrower = (direction: TraceDirectionWord) => (direction === 'both' ? 'trace one direction or fewer edge types' : 'trace fewer edge types');
const namedFew = (sides: HierarchyNamedSides) => ['the root',
    sides.incoming > 0 ? `its ${count(sides.incoming, 'incoming neighbour', 'incoming neighbours')}` : '',
    sides.outgoing > 0 ? `its ${count(sides.outgoing, 'outgoing neighbour', 'outgoing neighbours')}` : ''].filter(Boolean).join(' and ');

/**
 * The hierarchy of a Galaxy scope (hand test K5): what the columns mean, per
 * trace direction and depth. Second review: from two layers on, a column only
 * holds chains that run one way to the root, the rest stands in a band below,
 * and every line says its type and direction. Names above the budget: see
 * `HierarchyNames`; the advice names what brings them back.
 */
export const galaxyHierarchyText = {
    hint: (direction: TraceDirectionWord, view: { mixed: number; names: HierarchyNames; budget: number; neighbours: number; sides: HierarchyNamedSides }) => [
        direction === 'inbound'
            ? 'hierarchy: what reaches the root, one column per layer to the left'
            : direction === 'outbound'
                ? 'hierarchy: what the root reaches, one column per layer to the right'
                : 'hierarchy: incoming relationships on the left, the root in the middle, outgoing on the right; each column is one layer further in the same direction',
        view.mixed > 0 ? `${count(view.mixed, 'node', 'nodes')} reached through both directions, such as a callee of a caller, stand in the band below` : '',
        view.names === 'all' ? 'the type and direction of each relationship at its line'
            : view.names === 'neighbours'
                ? `above ${view.budget.toLocaleString()} nodes only the root and its direct neighbours carry names, so remove layers or trace fewer edge types to see all names`
                : !view.sides.root ? `names and edge types show for up to ${view.budget.toLocaleString()} nodes, so ${narrower(direction)} to see them`
                    : `names and edge types show for up to ${view.budget.toLocaleString()} nodes, and the root has ${count(view.neighbours, 'direct neighbour', 'direct neighbours')}, `
                        + `so only ${namedFew(view.sides)} ${view.sides.incoming + view.sides.outgoing > 0 ? 'carry names' : 'carries a name'}; ${narrower(direction)} to see the rest`,
    ].filter(Boolean).join('; '),
    /** The heading of the band in the picture. */
    bandTitle: (mixed: number) => `Mixed directions · ${count(mixed, 'node', 'nodes')}`,
    bandDetail: 'reached through both incoming and outgoing relationships, such as a callee of a caller',
    /** The note in the picture when names are missing, and how to get them. */
    namesNote: (direction: TraceDirectionWord, nodes: number, names: Exclude<HierarchyNames, 'all'>, neighbours: number, budget: number, sides: HierarchyNamedSides) => (names === 'neighbours'
        ? `${count(nodes, 'node', 'nodes')}: names for the root and its ${count(neighbours, 'direct neighbour', 'direct neighbours')} only (up to ${budget.toLocaleString()} names). Remove layers or trace fewer edge types to name every node.`
        : !sides.root ? `${count(nodes, 'node', 'nodes')}: no names above ${budget.toLocaleString()} nodes. ${narrower(direction).replace(/^t/, 'T')} to see them.`
            : `${count(nodes, 'node', 'nodes')}, ${neighbours.toLocaleString()} of them direct neighbours of the root: names for ${namedFew(sides)} only (up to ${budget.toLocaleString()} names). `
                + `${narrower(direction).replace(/^t/, 'T')} to see the rest.`),
};

/** Loading, cancelling and the render limit of a scope layer (hand test K8). */
export const galaxyLayerText = {
    checking: 'Checking index…',
    loading: (layer: number) => `Loading layer ${layer}…`,
    // A comma, not the dot of the finished counts: tools that wait for "N nodes · M edges" must not take this for done.
    loadingProgress: (layer: number, nodes: number, edges: number) =>
        `Loading layer ${layer}: ${count(nodes, 'node', 'nodes')}, ${count(edges, 'edge', 'edges')} so far`,
    /** The tooltip of a running load: which request it is on, page by page (review of K8). */
    loadingRequest: (request: number) => `Request ${request.toLocaleString()} to the index; "−" cancels.`,
    arranging: 'Arranging nodes…',
    counts: (nodes: number, edges: number) => `${count(nodes, 'node', 'nodes')} · ${count(edges, 'edge', 'edges')}`,
    endOfTrace: ' · end of trace',
    // "Partial" leads, so a narrow toolbar that cuts the end never cuts the warning.
    partial: (counts: string) => `Partial: ${counts}`,
    partialPreview: 'Partial preview',
    previewLoading: (layer: number) => `Partial preview while layer ${layer} loads`,
    // G3: a layer stops after the request that passes the limit, so it can hold more than the limit (5,548 of 5,000); the scene draws up to the limit.
    partialTitle: (layer: number, limit: number, kind: 'nodes' | 'edges') =>
        `Layer ${layer} stopped loading after the request that took it past the render limit of ${limit.toLocaleString()} ${kind}, so it is incomplete; `
        + `the scene draws at most ${limit.toLocaleString()} ${kind}. Raise the limit under Limits or trace fewer edge types to load all of it.`,
    removeLayer: 'Remove the outermost layer',
    cancelLoading: (layer: number) => `Cancel loading layer ${layer} and return to ${layer - 1 === 1 ? '1 layer' : `${layer - 1} layers`}`,
    /* Measured, not promised: a hub at the edge can bring far more than the last layer did (JSONBAgg layer 3: about 400 expected, over 9,000 loaded). */
    /* With `calls` the index has counted the calls at the edge nodes (review of K8): a floor, where the growth alone missed the hubs. */
    /*
     * Hand test 2026-10-04 (G3): every sentence true on its own. The warning
     * names the limit and the number behind it (`expandPastLimit` in
     * graph-scope.ts); the growth estimate stands only where it is the larger
     * number, so "about 402 nodes" no longer sits next to a warning it
     * contradicts. Loading stops after the request that passes a limit, not
     * at the limit (JSONBAgg layer 3 ended at 5,548 nodes), and where no limit
     * applies (the Explore mini-Galaxy) the hint claims none.
     */
    expandHint: (outlook: ExpandOutlook, past: { limit: 'nodes' | 'edges'; by: 'calls' | 'growth' } | undefined) => {
        const { layer, frontier, estimate, calls, loaded, limits } = outlook;
        const unit = (kind: 'nodes' | 'edges', value: number) => count(value, kind === 'nodes' ? 'node' : 'edge', kind);
        const evidence = calls === undefined
            ? `Growing like the last layer it adds about ${estimate.toLocaleString()} nodes, and a hub can add many more`
            : `The index lists ${count(calls, 'call', 'calls')} at them that ${calls === 1 ? 'is' : 'are'} not loaded yet`
                + (calls >= estimate ? `, and each can bring a new node` : `; growing like the last layer it adds about ${estimate.toLocaleString()} nodes`);
        const kind = past?.limit;
        return [
            past && limits && kind ? `Likely past the render limit of ${unit(kind, limits[kind])}.` : '',
            `Load layer ${layer}: ${count(frontier, 'node', 'nodes')} to expand.`,
            `${evidence}${kind ? `; ${unit(kind, loaded[kind])} ${loaded[kind] === 1 ? 'is' : 'are'} loaded now` : ''}.`,
            limits ? `Loading stops after the first request to the index that takes it past ${unit('nodes', limits.nodes)} or ${unit('edges', limits.edges)}, `
                + `so the layer can end above the limit; it is then marked partial, and the scene draws at most ${unit('nodes', limits.nodes)} and ${unit('edges', limits.edges)}.` : '',
        ].filter(Boolean).join(' ');
    },
    expandPartial: 'This layer stopped loading after it passed the render limit, so there is no complete edge to grow from. Raise the limit under Limits first.',
    expandEnd: 'End of trace: no relationship leads further.',
    /** Review of K31: Expand +1 is blocked while a layer loads, and says why. */
    expandLoading: (layer: number) => `Layer ${layer} is loading; "−" cancels it.`,
};

/** Compact toolbar words, so the scoped toolbar keeps to one row at 1600 px. */
export const galaxyToolbarText = {
    groups: (count: number) => `${count.toLocaleString()} groups`,
    groupsTitle: (count: number) => `${count.toLocaleString()} connection groups. Groups reflect connections in this trace, not inferred architecture components.`,
    limits: 'Limits',
    limitsTitle: 'Rendered node and edge limits',
    more: 'More',
    moreGlyph: '⋯',
    moreTitle: 'More: open the source, rendered node and edge limits, connection groups',
    openSource: 'Open source',
    openRootTitle: (name: string, path: string, line?: number) => `Open the source of ${name} in Explore: ${path}${line ? `:${line}` : ''}`,
    /** The root button's accessible name: its action, with the visible name in it. */
    openRootLabel: (name: string) => `Open the source of ${name}`,
    traceTitle: 'Trace direction: incoming, outgoing or both',
    expand: 'Expand +1',
    expandNarrow: '+1',
    outsideLimits: (nodes: number, edges: number) => `${nodes.toLocaleString()} nodes · ${edges.toLocaleString()} edges outside render limits`,
};

/**
 * The names of Branch nodes (hand test 2026-10-04, round 4, N1). The index
 * puts one Branch node above the top level folders and files: the checkout it
 * read. Its own name is the branch, "DETACHED" for a detached HEAD or
 * "working-tree" where no branch is known, and on its own that read like a
 * folder of that name. The shown name says what it is; the real one stays in
 * the tooltip and the detail line (node-names.ts).
 */
export interface BranchNameWords {
    detached: string;
    workingTree: string;
    branch: (name: string) => string;
    /** The project in front: "django-demo · detached HEAD". */
    inProject: (project: string, what: string) => string;
}

export const galaxyNodeNameText: BranchNameWords & {
    indexName: (name: string, qualifiedName?: string) => string;
    branchTitle: (shown: string, name: string, qualifiedName?: string) => string;
} = {
    detached: 'detached HEAD',
    workingTree: 'working tree',
    branch: (name: string) => `branch ${name}`,
    inProject: (project: string, what: string) => `${project} · ${what}`,
    /** The detail line of the hover card. */
    indexName: (name: string, qualifiedName?: string) => `Name in the index: ${name}${qualifiedName ? ` (${qualifiedName})` : ''}`,
    /** The tooltip wherever the shown name stands. */
    branchTitle: (shown: string, name: string, qualifiedName?: string) =>
        `${shown}: the checkout the index read, above its top level folders and files. Name in the index: ${name}${qualifiedName ? ` (${qualifiedName})` : ''}.`,
};

/** What a click on a node in the hover card will do. */
export const galaxyCardText = {
    nothingToOpen: 'no file in the index: nothing to open',
    openFile: 'click to open the file and follow the twin',
};

/**
 * The hierarchy without anything to show, and its ring (hand test
 * 2026-10-04, round 4, N2). The ring follows the symbol open in Explore's
 * reader, so the note about it stands only beside that reader; in the Galaxy
 * tab the root of a scope stands in the middle of its hierarchy.
 */
export const galaxyHierarchyNoteText = {
    /** The hierarchy chip beside Explore while there is no walk and no open symbol. */
    unavailable: 'hierarchy: open a symbol or choose where to start, then this shows what it reaches',
    /** The hierarchy chip in the Galaxy tab before a node is selected. */
    unavailableWorkspace: 'hierarchy: select a node first, then this shows what reaches it and what it reaches',
    /** Beside Explore, when no node of the walk is the symbol open there. */
    noFocus: 'None of these nodes is open in Explore; the ring marks the symbol open there.',
};
