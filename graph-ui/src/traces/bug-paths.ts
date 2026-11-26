/**
 * The way in the index expects, the way a recording took, and the difference
 * between them.
 *
 * Written to the specification of `bugPaths` in CodeAtlasIDE
 * (codeatlas-core/src/common/trace-protocol.ts, `BugPathsDto` and the three
 * bounds; codeatlas-views/src/browser/traces/bug-wizard-widget.tsx for what the
 * four steps do with it). It is a reimplementation rather than a port, and the
 * reason is the whole content of this header: there, the observed calls live in
 * a file CodeAtlas keeps itself and can be read whole. Here they live in the
 * analysis backend, and the backend answers a narrower question than the one the
 * reference asks.
 *
 * ## What this backend will and will not say about a recording
 *
 * Observed calls are stored by `ingest_traces` in `observed_calls`, keyed by the
 * pair of qualified names (cbm/src/mcp/mcp.c, `handle_ingest_traces`). There is
 * exactly one way to read them back, and it is not a listing:
 * `cbm_atlas_attach_observed` (cbm/src/ui/http_server.c) hangs an `observed`
 * object on a hop of an answer that was computed from the *static* graph, and
 * the two routes that call it are the only two that ever carry one.
 *
 * **GET /api/trace** takes `project`, `from`, `to`, `mode` and an optional
 * `guards`. `from` and `to` are a qualified name or `#<node id>`; `mode` is
 * `calls` (CALLS edges) or `data` (DATA_FLOWS edges, which the TypeScript
 * analysis does not write, so this file always asks for `calls`); `guards=1`
 * adds the branch conditions, which this panel does not use. The answer is a
 * breadth-first *shortest* path with `reachable`, `explored`, `hops` and a
 * `path` array, and each entry from the second onwards may carry `observed`
 * `{count, label, last_seen}` for the call from the entry before it.
 *
 * **GET /api/flow** takes `project`, `id` and the same optional `guards`, and
 * answers one ranked entry-to-terminal walk as `steps`, each with its `parent`
 * index. The same `observed` objects are hung on the pairs
 * `(steps[step.parent], step)`. **GET /api/flows** takes `project` alone and
 * lists the walks with their ids.
 *
 * The consequence is exact and it is stated here because it decides what the
 * fourth step of the wizard can honestly claim: **a recorded call is only ever
 * readable where the index also records the call.** A pair that ran and that the
 * index has no relation for is stored by the engine and cannot be asked for
 * again through either route, because there is no static hop to hang it on.
 *
 * So the two divergence lists mean this, and the panel says so in as many words:
 *
 *  - `staticOnly`: a call on a chain the index draws into the target that no
 *    reading of a recording came back for. "Expected, never observed."
 *  - `runtimeOnly`: a call a recording holds that is not on any of those chains.
 *    Each row additionally carries {@link BugPathEdge.indexRecordsCall}, asked
 *    of the index at the time the row is built, so the reader is told which of
 *    the two things it is: a call the index knows and that simply is not on the
 *    way in, or a call the index has no relation for at all. The second is the
 *    reference's meaning of the list; with this backend it can be reported when
 *    it appears and cannot be discovered by looking.
 *
 * ## The three readings, and their bounds
 *
 * **Upstream chains** come from the index, one `callsIn` per chain head, level by
 * level: who calls the target, who calls them. {@link BUG_PATH_DEFAULT_DEPTH}
 * hops by default and {@link BUG_PATH_MAX_CHAINS} chains, both the reference's
 * numbers, and both bounds are reported through `truncated` rather than being
 * quietly applied: a chain that was cut looks exactly like a chain that ended.
 *
 * **Traces** are asked once per chain, from the chain's head to the target. That
 * is the shortest static path between the two, which is not always the chain
 * itself; where they differ, the observation still belongs to the pair the
 * server named, and this file keys everything on the pair rather than on a
 * position in a list.
 *
 * **Flows** are the reading that can see beside and below the target. A trace
 * into the target only ever annotates calls on the way in, so a recording that
 * went on somewhere after the target would be invisible to it; the ranked walks
 * carry those hops, and {@link BUG_PATH_MAX_FLOWS} of them are read.
 *
 * ## Nothing here holds a symbol reference
 *
 * A node of a trace or a flow carries a name and a file and no qualified name,
 * and what a recording carries is whatever somebody's recorder wrote. So every
 * row of this document is a name, and {@link resolveHop} looks it up in the
 * index at the moment of the click, the way the reference's `resolveSymbolNamed`
 * does. Publishing a `SymbolRef` without a `nodeId` would tell the rest of the
 * product that a perfectly well indexed symbol is not indexed.
 */

import { COLUMNS, CALLABLE_LABELS, callsIn, declarationsInFiles } from '../provider/cypher';
import { toOptionalNumber } from '../provider/rpc-schemas';
import type { SymbolRef } from '../core/focus-protocol';
import type { ProviderQueryOptions, ResolveResult, SymbolSearchHit } from '../core/intelligence-provider';
import type { FlowDetail, FlowSummary, ObservedRecord, TraceAnswer } from './trace-schemas';

export type { ObservedRecord } from './trace-schemas';

/** How many hops up the chain walk goes when nobody says. The reference's number. */
export const BUG_PATH_DEFAULT_DEPTH = 4;

/** The hardest bound the walk accepts, whatever a caller asks for. */
export const BUG_PATH_MAX_DEPTH = 8;

/** How many chains one answer carries. More than a reader compares in one sitting. */
export const BUG_PATH_MAX_CHAINS = 6;

/**
 * How many ranked walks are read for their observations.
 *
 * Each one is its own request, and the list is ranked, so the first few are the
 * ones a reader would look at anyway. What is not read is reported.
 */
export const BUG_PATH_MAX_FLOWS = 8;

/** One symbol on a chain, as a name rather than as a reference. */
export interface BugPathNode {
    name: string;
    qualifiedName?: string;
    /** Workspace-relative path of the declaring file, when a reading named one. */
    filePath?: string;
    /** 1-based declaration line, when the index gave one. */
    line?: number;
    /** True when the index records nothing calling this symbol, so the chain starts here. */
    entryPoint?: boolean;
    /** The observation of the call that reached this node from the one before it. */
    observed?: ObservedRecord;
}

/** One call, named at both ends so the direction is never in doubt. */
export interface BugPathEdge {
    from: BugPathNode;
    to: BugPathNode;
    observed?: ObservedRecord;
    /**
     * Whether the index records this call, asked when the row was built.
     *
     * Only filled for {@link BugPathsDto.runtimeOnly}, where it is the whole
     * difference between "the index knows this call, it is just not on the way
     * in" and "the index has no such call at all". `undefined` means nobody
     * could ask, which happens when a reading named no qualified name.
     */
    indexRecordsCall?: boolean;
}

/** Everything the wizard draws. */
export interface BugPathsDto {
    target: BugPathNode;
    /** Upstream chains from the index, outermost caller first, target last. */
    staticPaths: BugPathNode[][];
    /** Runs of consecutive observed calls, in the order the server reported them. */
    observedPaths: BugPathNode[][];
    /** Expected calls no reading of a recording came back for. */
    staticOnly: BugPathEdge[];
    /** Observed calls that are not on any expected chain. */
    runtimeOnly: BugPathEdge[];
    /** True when a bound stopped the chain walk while callers were still to follow. */
    truncated: boolean;
    /** Distinct observed calls this reading found. Zero means: nothing came back. */
    observedEvents: number;
    /** The bounds that were in force, so a surface can name them. */
    depth: number;
    chains: number;
    /** How many ranked walks were read, and whether the list was longer. */
    flowsRead: number;
    flowsTruncated: boolean;
}

/** What the chain walk needs from the engine, and nothing else. */
export interface BugPathIndex {
    queryRows(project: string, query: string): Promise<Record<string, string>[]>;
}

/** What the observation reading needs. One method per route that carries one. */
export interface BugPathObservations {
    trace(project: string, from: string, to: string): Promise<TraceAnswer>;
    flows(project: string): Promise<FlowSummary[]>;
    flow(project: string, id: number): Promise<FlowDetail>;
}

export interface BugPathOptions {
    project: string;
    depth?: number;
    chains?: number;
    maxFlows?: number;
}

/** A bound brought inside the range this reading will honour. */
function clamp(value: number | undefined, fallback: number, maximum: number): number {
    if (value === undefined || !Number.isFinite(value)) {
        return fallback;
    }
    return Math.min(maximum, Math.max(1, Math.floor(value)));
}

/**
 * How one symbol is identified across the two readings.
 *
 * The file and the name, because that is the only pair both readings carry: the
 * index gives a qualified name and the trace and flow answers do not. Two
 * symbols of the same name in one file would collide, and there is no such
 * thing.
 */
export function nodeKey(node: { name: string; filePath?: string }): string {
    return `${node.filePath ?? ''}|${node.name}`;
}

/** How one call is identified. Ordered, always, because a call has a direction. */
export function edgeKey(from: { name: string; filePath?: string }, to: { name: string; filePath?: string }): string {
    return `${nodeKey(from)}->${nodeKey(to)}`;
}

/** Ordinal comparison, never `localeCompare`: two machines must agree. */
function compareText(a: string, b: string): number {
    if (a === b) {
        return 0;
    }
    return a < b ? -1 : 1;
}

/** The symbols the index records as calling one qualified name, in a fixed order. */
async function callersOf(
    index: BugPathIndex,
    project: string,
    qualifiedName: string,
): Promise<BugPathNode[]> {
    let rows: Record<string, string>[];
    try {
        rows = await index.queryRows(project, callsIn(qualifiedName));
    } catch {
        // A refused query is not a finding about the code. The chain stops here
        // and the surface says the walk ended, which is what happened.
        return [];
    }
    const out: BugPathNode[] = [];
    const seen = new Set<string>();
    for (const row of rows) {
        const name = row[COLUMNS.callsIn[0]] ?? '';
        if (name.length === 0) {
            continue;
        }
        const node: BugPathNode = { name };
        const qualified = row[COLUMNS.callsIn[1]] ?? '';
        if (qualified.length > 0) {
            node.qualifiedName = qualified;
        }
        const filePath = row[COLUMNS.callsIn[2]] ?? '';
        if (filePath.length > 0) {
            node.filePath = filePath;
        }
        const line = toOptionalNumber(row[COLUMNS.callsIn[3]]);
        if (line !== undefined) {
            node.line = line;
        }
        const key = nodeKey(node);
        if (seen.has(key)) {
            continue;
        }
        seen.add(key);
        out.push(node);
    }
    return out.sort((a, b) => compareText(a.qualifiedName ?? nodeKey(a), b.qualifiedName ?? nodeKey(b)));
}

/**
 * The upstream chains, level by level.
 *
 * Breadth first rather than depth first, so the bound on chains cuts the widest
 * part of the tree and not an arbitrary branch of it, and so two runs against
 * one index produce the same document.
 */
export async function staticChains(
    index: BugPathIndex,
    project: string,
    target: BugPathNode,
    depth: number,
    maxChains: number,
): Promise<{ chains: BugPathNode[][]; truncated: boolean }> {
    const complete: BugPathNode[][] = [];
    let open: BugPathNode[][] = [[target]];
    let truncated = false;

    for (let level = 0; level < depth && open.length > 0; level++) {
        const next: BugPathNode[][] = [];
        for (const chain of open) {
            const head = chain[0];
            if (head.qualifiedName === undefined) {
                // Nothing to ask with. The chain ends and says nothing about
                // whether anything calls it.
                complete.push(chain);
                continue;
            }
            const callers = await callersOf(index, project, head.qualifiedName);
            if (callers.length === 0) {
                complete.push([{ ...head, entryPoint: true }, ...chain.slice(1)]);
                continue;
            }
            let extended = false;
            for (const caller of callers) {
                // A caller already on the chain is a cycle. It is not followed,
                // and the chain it closes is kept as it stands: the call back
                // into it is still a call the index records, and the divergence
                // lists see it through the chain it is on.
                if (chain.some((node) => nodeKey(node) === nodeKey(caller))) {
                    continue;
                }
                extended = true;
                next.push([caller, ...chain]);
            }
            if (!extended) {
                complete.push(chain);
            }
        }
        const room = Math.max(0, maxChains - complete.length);
        if (next.length > room) {
            truncated = true;
        }
        open = next.slice(0, room);
        if (level + 1 === depth && open.length > 0) {
            truncated = true;
        }
    }

    const chains = [...complete, ...open].slice(0, maxChains);
    if (complete.length + open.length > maxChains) {
        truncated = true;
    }
    chains.sort((a, b) => compareText(a.map(nodeKey).join('>'), b.map(nodeKey).join('>')));
    return { chains, truncated };
}

/** Every call one set of chains draws, keyed by its ordered pair. */
export function chainEdges(chains: readonly BugPathNode[][]): Map<string, BugPathEdge> {
    const edges = new Map<string, BugPathEdge>();
    for (const chain of chains) {
        for (let at = 1; at < chain.length; at++) {
            const from = chain[at - 1];
            const to = chain[at];
            const key = edgeKey(from, to);
            if (!edges.has(key)) {
                edges.set(key, { from, to });
            }
        }
    }
    return edges;
}

/**
 * The runs of consecutive observed calls inside one trace answer.
 *
 * A run and not the whole path: the path is what the index connects, and only
 * the hops carrying an `observed` object were reported by a recording. Joining
 * two runs across an unobserved hop in between would draw a journey nobody
 * recorded.
 */
export function observedRuns(path: readonly { name: string; filePath?: string; observed?: ObservedRecord }[]): BugPathNode[][] {
    const runs: BugPathNode[][] = [];
    let current: BugPathNode[] = [];
    for (let at = 1; at < path.length; at++) {
        const step = path[at];
        if (step.observed === undefined) {
            if (current.length >= 2) {
                runs.push(current);
            }
            current = [];
            continue;
        }
        if (current.length === 0) {
            const previous = path[at - 1];
            current.push({ name: previous.name, filePath: previous.filePath });
        }
        current.push({ name: step.name, filePath: step.filePath, observed: step.observed });
    }
    if (current.length >= 2) {
        runs.push(current);
    }
    return runs;
}

/** Every observed call one flow answer reports, as ordered pairs. */
export function flowObservations(flow: FlowDetail): { from: BugPathNode; to: BugPathNode; observed: ObservedRecord }[] {
    const out: { from: BugPathNode; to: BugPathNode; observed: ObservedRecord }[] = [];
    flow.steps.forEach((step) => {
        if (step.observed === undefined || step.parent < 0 || step.parent >= flow.steps.length) {
            return;
        }
        const parent = flow.steps[step.parent];
        out.push({
            from: { name: parent.name, filePath: parent.filePath },
            to: { name: step.name, filePath: step.filePath },
            observed: step.observed,
        });
    });
    return out;
}

/** True when a flow's steps hold the symbol the wizard is about. */
export function flowTouches(flow: FlowDetail, target: BugPathNode): boolean {
    const key = nodeKey(target);
    return flow.steps.some((step) => nodeKey(step) === key);
}

/**
 * The qualified names of the symbols a reading named only by file and name.
 *
 * Two queries whatever the number of files, the same reading the change
 * assessment makes. A symbol the index cannot match keeps its file and its name
 * and gets no qualified name, which is exactly the case where nothing can be
 * asked about it.
 */
export async function declarationNames(
    index: BugPathIndex,
    project: string,
    filePaths: readonly string[],
): Promise<Map<string, { qualifiedName?: string; line?: number }>> {
    const found = new Map<string, { qualifiedName?: string; line?: number }>();
    const files = [...new Set(filePaths.filter((path) => path.length > 0))].sort(compareText);
    if (files.length === 0) {
        return found;
    }
    for (const label of CALLABLE_LABELS) {
        const query = declarationsInFiles(label, files);
        if (query === undefined) {
            continue;
        }
        let rows: Record<string, string>[];
        try {
            rows = await index.queryRows(project, query);
        } catch {
            continue;
        }
        for (const row of rows) {
            const name = row[COLUMNS.declarations[0]] ?? '';
            const filePath = row[COLUMNS.declarations[2]] ?? '';
            if (name.length === 0) {
                continue;
            }
            const key = nodeKey({ name, filePath });
            if (found.has(key)) {
                continue;
            }
            const qualifiedName = row[COLUMNS.declarations[1]] ?? '';
            const line = toOptionalNumber(row[COLUMNS.declarations[3]]);
            found.set(key, {
                ...(qualifiedName.length > 0 ? { qualifiedName } : {}),
                ...(line === undefined ? {} : { line }),
            });
        }
    }
    return found;
}

/**
 * Whether the index records one named call.
 *
 * Asked rather than assumed, and asked with the same reading the chains are
 * built from, so the two cannot disagree. Answers `undefined` when there is
 * nothing to ask with, which a surface reports as "nobody could look" and never
 * as "no".
 */
async function callIsRecorded(
    index: BugPathIndex,
    project: string,
    from: BugPathNode,
    to: BugPathNode,
): Promise<boolean | undefined> {
    if (to.qualifiedName === undefined) {
        return undefined;
    }
    const callers = await callersOf(index, project, to.qualifiedName);
    if (from.qualifiedName !== undefined) {
        return callers.some((caller) => caller.qualifiedName === from.qualifiedName);
    }
    const key = nodeKey(from);
    return callers.some((caller) => nodeKey(caller) === key);
}

/**
 * The whole document: what the index expects, what a recording holds, and where
 * the two differ.
 */
export async function bugPaths(
    index: BugPathIndex,
    observations: BugPathObservations,
    target: BugPathNode,
    options: BugPathOptions,
): Promise<BugPathsDto> {
    const depth = clamp(options.depth, BUG_PATH_DEFAULT_DEPTH, BUG_PATH_MAX_DEPTH);
    const chainCap = clamp(options.chains, BUG_PATH_MAX_CHAINS, BUG_PATH_MAX_CHAINS);
    const flowCap = clamp(options.maxFlows, BUG_PATH_MAX_FLOWS, BUG_PATH_MAX_FLOWS);
    const project = options.project;

    const walked = await staticChains(index, project, target, depth, chainCap);
    const expected = chainEdges(walked.chains);

    // ---------------------------------------------------- observations ------
    const observedEdges = new Map<string, { from: BugPathNode; to: BugPathNode; observed: ObservedRecord }>();
    const observedPaths: BugPathNode[][] = [];

    const remember = (from: BugPathNode, to: BugPathNode, observed: ObservedRecord): void => {
        const key = edgeKey(from, to);
        if (!observedEdges.has(key)) {
            observedEdges.set(key, { from, to, observed });
        }
    };

    if (target.qualifiedName !== undefined) {
        const asked = new Set<string>();
        for (const chain of walked.chains) {
            /*
             * From the outside in, until one end answers.
             *
             * The trace resolves its endpoints against the callable graph, and a
             * chain can start at something that is not one: file-level code that
             * calls `main` is a Module node, the index records the call, and
             * `/api/trace` answers "source is not an indexed callable". Stopping
             * at the head would therefore lose every observation on a chain that
             * begins outside a function, which is most chains of most programs.
             * So the next hop is asked, and the next, until an answer comes back
             * reachable. Each source is asked once across all chains.
             */
            for (let at = 0; at + 1 < chain.length; at++) {
                const from = chain[at];
                if (from.qualifiedName === undefined || asked.has(from.qualifiedName)) {
                    continue;
                }
                asked.add(from.qualifiedName);
                let answer: TraceAnswer;
                try {
                    answer = await observations.trace(project, from.qualifiedName, target.qualifiedName);
                } catch {
                    continue;
                }
                if (!answer.reachable) {
                    continue;
                }
                for (let hop = 1; hop < answer.path.length; hop++) {
                    const step = answer.path[hop];
                    if (step.observed !== undefined) {
                        const previous = answer.path[hop - 1];
                        remember(
                            { name: previous.name, filePath: previous.filePath },
                            { name: step.name, filePath: step.filePath },
                            step.observed,
                        );
                    }
                }
                observedPaths.push(...observedRuns(answer.path));
                break;
            }
        }
    }

    let flowsRead = 0;
    let flowsTruncated = false;
    try {
        const summaries = await observations.flows(project);
        flowsTruncated = summaries.length > flowCap;
        for (const summary of summaries.slice(0, flowCap)) {
            const detail = await observations.flow(project, summary.id);
            flowsRead += 1;
            if (!flowTouches(detail, target)) {
                continue;
            }
            for (const seen of flowObservations(detail)) {
                remember(seen.from, seen.to, seen.observed);
            }
        }
    } catch {
        // The ranked walks are the reading that can see past the target. Losing
        // them costs breadth and nothing else, and the two lists below still
        // describe what the traces found.
    }

    // ------------------------------------------------------- identities -----
    const files: string[] = [];
    for (const entry of observedEdges.values()) {
        files.push(entry.from.filePath ?? '', entry.to.filePath ?? '');
    }
    for (const chain of observedPaths) {
        for (const node of chain) {
            files.push(node.filePath ?? '');
        }
    }
    const names = await declarationNames(index, project, files);
    const named = (node: BugPathNode): BugPathNode => {
        const known = names.get(nodeKey(node));
        return known === undefined ? node : { ...node, ...known };
    };

    // ------------------------------------------------------- divergence -----
    const staticOnly: BugPathEdge[] = [];
    for (const [key, edge] of expected) {
        if (!observedEdges.has(key)) {
            staticOnly.push(edge);
        }
    }
    const runtimeOnly: BugPathEdge[] = [];
    for (const [key, entry] of observedEdges) {
        if (expected.has(key)) {
            continue;
        }
        const from = named(entry.from);
        const to = named(entry.to);
        const indexRecordsCall = await callIsRecorded(index, project, from, to);
        runtimeOnly.push({
            from,
            to,
            observed: entry.observed,
            ...(indexRecordsCall === undefined ? {} : { indexRecordsCall }),
        });
    }

    const byPair = (a: BugPathEdge, b: BugPathEdge): number =>
        compareText(edgeKey(a.from, a.to), edgeKey(b.from, b.to));
    staticOnly.sort(byPair);
    runtimeOnly.sort(byPair);
    observedPaths.sort((a, b) => compareText(a.map(nodeKey).join('>'), b.map(nodeKey).join('>')));

    return {
        target,
        staticPaths: walked.chains,
        observedPaths: dedupeChains(observedPaths).map((chain) => chain.map(named)),
        staticOnly,
        runtimeOnly,
        truncated: walked.truncated,
        observedEvents: observedEdges.size,
        depth,
        chains: chainCap,
        flowsRead,
        flowsTruncated,
    };
}

/** One journey shown once, however many traces reported it. */
function dedupeChains(chains: readonly BugPathNode[][]): BugPathNode[][] {
    const seen = new Set<string>();
    const out: BugPathNode[][] = [];
    for (const chain of chains) {
        const key = chain.map(nodeKey).join('>');
        if (seen.has(key)) {
            continue;
        }
        seen.add(key);
        out.push(chain);
    }
    return out;
}

/** What {@link resolveHop} needs from a provider. A slice, so a test needs no server. */
export interface HopResolver {
    searchSymbols(
        root: string,
        pattern: string,
        limit?: number,
        opts?: ProviderQueryOptions,
    ): Promise<SymbolSearchHit[]>;
    resolveSymbolAt(
        root: string,
        filePath: string,
        oneBasedLine: number,
        opts?: ProviderQueryOptions,
    ): Promise<ResolveResult>;
}

/** How many candidates a hop lookup asks for. The reference's number. */
export const WIZARD_SEARCH_LIMIT = 20;

/**
 * The symbol behind a name, looked up at the moment of the click.
 *
 * The order is deliberate. A hop that already carries a file and a declaration
 * line is resolved straight there, because that is the reading the index gave
 * and it cannot be improved on by searching. Anything else goes through the
 * search first and then through the same resolution, so what comes back is
 * always a symbol the index named at a line it holds, never a place a recording
 * mentioned. A name the index does not know resolves to nothing, and the caller
 * opens nothing rather than opening something plausible.
 */
export async function resolveHop(
    provider: HopResolver,
    root: string,
    hop: BugPathNode,
    opts: ProviderQueryOptions = {},
): Promise<SymbolRef | undefined> {
    if (hop.filePath !== undefined && hop.line !== undefined) {
        const resolved = await provider
            .resolveSymbolAt(root, hop.filePath, hop.line, opts)
            .catch(() => undefined);
        if (resolved?.kind === 'ok') {
            return resolved.symbol;
        }
    }
    const hits = await provider
        .searchSymbols(root, hop.name, WIZARD_SEARCH_LIMIT, opts)
        .catch(() => [] as SymbolSearchHit[]);
    const match = pickHit(hits, hop);
    if (match?.filePath === undefined || match.line === undefined) {
        return undefined;
    }
    const resolved = await provider
        .resolveSymbolAt(root, match.filePath, match.line, opts)
        .catch(() => undefined);
    return resolved?.kind === 'ok' ? resolved.symbol : undefined;
}

/**
 * The candidate that is this hop, out of what the search returned.
 *
 * Exact identities first, in the order of how much they identify: the qualified
 * name, then the file and the name together, then the name on its own. The last
 * one is a guess only in the sense that a repository may hold two functions of
 * one name; the file is checked before it, so the guess is only ever reached
 * when no reading named a file.
 */
export function pickHit(hits: readonly SymbolSearchHit[], hop: BugPathNode): SymbolSearchHit | undefined {
    if (hop.qualifiedName !== undefined) {
        const exact = hits.find((hit) => hit.qualifiedName === hop.qualifiedName);
        if (exact !== undefined) {
            return exact;
        }
    }
    if (hop.filePath !== undefined) {
        const inFile = hits.find((hit) => hit.filePath === hop.filePath && hit.name === hop.name);
        if (inFile !== undefined) {
            return inFile;
        }
    }
    return hits.find((hit) => hit.name === hop.name);
}
