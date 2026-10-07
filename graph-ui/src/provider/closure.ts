/**
 * The connected part of the graph around one symbol: what it reaches, bounded
 * twice and honest about both bounds.
 *
 * Written to the specification of `getClosure` in CodeAtlasIDE
 * (theia-extensions/codeatlas-intelligence/src/node/intelligence-server-impl.ts,
 * `getClosure`/`walkClosure`/`resolveCallee`, plus the DTO and the four bounds
 * in codeatlas-core/src/common/intelligence-rpc.ts). It is a reimplementation
 * rather than a port: there the walk sits in a Theia backend behind an RPC
 * facade with a cache keyed on the index generation, and here it is a function
 * the browser calls straight against the provider. Every rule below is that
 * specification's rule; what is not carried over is the RPC shell, the cache and
 * the `symbols`/`edges` split of the DTO (see {@link ClosureResult}).
 *
 * ## Five properties are load-bearing
 *
 * **The walk is breadth first and alphabetical within a layer.** Two calls
 * against one index therefore produce the same document, which is what lets a
 * forward walk be compared with the one a colleague sees and keeps a tour from
 * reshuffling between two readings of the same code. The engine's own row order
 * is not stable enough to rely on. The comparison is ordinal, never
 * `localeCompare`: that is the same determinism rule the tour generator states,
 * and it is the one place this implementation is deliberately stricter than the
 * reference, which sorts one intermediate list by collation before re-sorting it
 * ordinally. Two machines with different collations would otherwise emit the
 * edges of one layer in two orders.
 *
 * **A symbol is visited once.** A qualified name already reached is not expanded
 * again, so a recursive chain terminates; the edge back into it is still
 * recorded, because a cycle is a fact about the code and a walk that dropped the
 * closing edge would draw a chain that stops for no reason.
 *
 * **Every symbol is resolved before it is published.** A call site names a file
 * and a line; that is a place, not a symbol, and publishing it as one would hand
 * a reference with no `nodeId` to the rest of the product, which reads that as
 * "not indexed". So each newly reached callee is resolved back into the symbol
 * the index knows, at the declaration line rather than at the call line. A
 * callee the index will not resolve is kept, without a `nodeId`, because leaving
 * it out would silently shorten the answer and inventing one would be a lie
 * about the index.
 *
 * **Both bounds are visible.** A layer past `depth` and a symbol past `cap` both
 * set `truncated`, and `visited` counts what the walk saw including what it
 * refused, so a surface can say how much is missing rather than drawing a floor
 * as a total.
 *
 * **Edges only ever join two returned symbols.** A call whose target the cap
 * refused is not an edge with a dangling end; it is counted as truncation.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { CallSite } from '../core/semantic-ir';
import type {
    FactKind,
    ProviderQueryOptions,
    ResolveResult,
    SymbolFacts,
} from '../core/intelligence-provider';
import { toEditorRange } from '../core/positions';
import { toFileUri, toWorkspaceRelative } from '../ir/file-uri';

/**
 * How far a closure walks when nobody says.
 *
 * Three hops is what a reader can hold in their head at once: the symbol, what
 * it calls, and what those call. Four is a picture nobody reads.
 */
export const CLOSURE_DEFAULT_DEPTH = 3;

/**
 * How many symbols a closure returns when nobody says.
 *
 * Fifteen is the point at which a forward walk stops being a sitting and starts
 * being a survey.
 */
export const CLOSURE_DEFAULT_CAP = 15;

/** Hardest bounds this walk accepts, whatever a caller asks for. */
export const CLOSURE_MAX_DEPTH = 6;
export const CLOSURE_MAX_CAP = 60;

/** One symbol the walk reached, and how it got there. */
export interface ClosureNode {
    symbol: SymbolRef;
    /** Hops away from the root. The root itself is zero. */
    hop: number;
    /**
     * Qualified name of the symbol whose call first reached this one.
     *
     * Absent on the root, which nothing reached. Carried rather than derived
     * from `edges`, because a surface that names the caller of a step should not
     * have to pick one of several edges and hope it picked the first.
     */
    via?: string;
}

/** One call the walk recorded, between two symbols it also returned. */
export interface ClosureEdge {
    /** Qualified name of the calling symbol. Always one of the returned nodes. */
    from: string;
    /** Qualified name of the called symbol. Always one of the returned nodes. */
    to: string;
    /** 1-based graph line of the call expression, inside the caller's file. */
    line?: number;
}

/**
 * The connected part of the graph around one symbol.
 *
 * `nodes` is in walk order: the root first, then each layer, alphabetically
 * within the layer. That order is the contract, not an accident of the
 * traversal.
 *
 * The one shape difference from the reference DTO: there the reached symbols are
 * a flat `symbols: SymbolRef[]` and the distance is only recoverable by walking
 * `edges`. Here each node carries its `hop` and its `via`, because the consumer
 * on this side is a step player that has to present the walk in hop order and
 * say who reached what. The information is the same information; it is carried
 * rather than recomputed, so two surfaces cannot disagree about it.
 */
export interface ClosureResult {
    /** The symbol the walk started at, as it was asked for. */
    root: SymbolRef;
    /** Reached symbols in walk order, root first. Never longer than `cap`. */
    nodes: ClosureNode[];
    /** Calls between the symbols above, in walk order. */
    edges: ClosureEdge[];
    /** True when a bound stopped the walk while symbols were still to reach. */
    truncated: boolean;
    /**
     * Distinct symbols the walk looked at, including the ones a bound refused.
     * `visited - nodes.length` is how many are missing from the answer.
     */
    visited: number;
    /** Hops away from the root the walk was allowed to go. */
    depth: number;
    /** Symbols the walk was allowed to return. */
    cap: number;
}

/** Knobs for {@link getClosure}. Both are clamped; see the constants above. */
export interface ClosureOptions extends ProviderQueryOptions {
    /** Hops away from the root. Defaults to {@link CLOSURE_DEFAULT_DEPTH}. */
    depth?: number;
    /** Symbols the answer may hold. Defaults to {@link CLOSURE_DEFAULT_CAP}. */
    cap?: number;
}

/**
 * What the walk needs from a provider, and nothing else.
 *
 * Deliberately a slice rather than the whole `IntelligenceProvider`, for the
 * same reason `SymbolSearcher` in the meaning search is one: the walk can then
 * be proven against a handful of invented rows, with no server, no transport and
 * no test that has to know how /rpc talks.
 */
export interface ClosureSource {
    getFacts(
        workspaceRoot: string,
        symbol: SymbolRef,
        kinds: FactKind[],
        opts?: ProviderQueryOptions,
    ): Promise<SymbolFacts>;
    resolveSymbolAt(
        workspaceRoot: string,
        filePath: string,
        oneBasedLine: number,
        opts?: ProviderQueryOptions,
    ): Promise<ResolveResult>;
}

/**
 * A caller's bound, brought inside the range this walk will honour.
 *
 * A bound is a promise about how much work one call may cost, so it is clamped
 * here rather than trusted: a caller that asked for a depth of two hundred would
 * otherwise turn one click into a sweep of the whole repository.
 */
export function clampBound(value: number | undefined, fallback: number, maximum: number): number {
    if (value === undefined || !Number.isFinite(value)) {
        return fallback;
    }
    return Math.min(maximum, Math.max(1, Math.floor(value)));
}

/**
 * How one symbol is identified inside a closure.
 *
 * The qualified name whenever the index gave one, because that is what the index
 * itself keys on and what an edge has to be able to name. A symbol without one is
 * identified by its file and its name, which is not an index identity and is not
 * treated as one anywhere: it simply keeps two different unresolved callees from
 * being merged into one row.
 */
export function closureKeyOf(symbol: SymbolRef): string {
    return symbol.qualifiedName ?? `${symbol.uri}#${symbol.name}`;
}

/**
 * How one call site is identified before its target has been resolved.
 *
 * Empty when the index named no target, which is a call the closure cannot join
 * up: the walk cannot expand it and no edge can point at it. Such a call is
 * still a step of the symbol that makes it, where it belongs; what it is not is
 * a node of a graph.
 */
export function closureCallKey(call: CallSite): string {
    return call.targetQualifiedName ?? '';
}

/** Ordinal comparison. See the note on determinism in the file header. */
function compareText(a: string, b: string): number {
    if (a === b) {
        return 0;
    }
    return a < b ? -1 : 1;
}

/**
 * A callee the index would not resolve, as the honest reference to it.
 *
 * No `nodeId`, deliberately: the rest of the product reads its absence as "the
 * index has no entry for this", which is exactly what happened. The qualified
 * name is kept because the walk already has it and it is what names the symbol
 * in an edge; the location is whatever the call site reported.
 */
export function unresolvedCallee(workspaceRoot: string, call: CallSite): SymbolRef {
    const line = call.targetLine ?? 1;
    const range = toEditorRange(line, line);
    return {
        name: call.targetName,
        qualifiedName: call.targetQualifiedName,
        kind: 'unknown',
        uri: toFileUri(workspaceRoot, call.targetFile) ?? '',
        range,
        selectionRange: { start: range.start, end: range.start },
    };
}

/**
 * One reached callee, as the symbol the index knows rather than as the place the
 * call site named.
 *
 * The declaration line is what is resolved against, never the call line: a call
 * to `validateUser` on line 24 of `userService.ts` is declared on line 19 of
 * `validate.ts`, and resolving the first would answer with whatever encloses
 * line 24, which is the caller.
 */
async function resolveCallee(
    source: ClosureSource,
    workspaceRoot: string,
    call: CallSite,
    opts: ProviderQueryOptions,
): Promise<SymbolRef> {
    const relative = toWorkspaceRelative(workspaceRoot, call.targetFile);
    const line = call.targetLine;
    if (relative.length > 0 && line !== undefined) {
        const resolved = await source
            .resolveSymbolAt(workspaceRoot, relative, line, opts)
            .catch(() => undefined);
        if (resolved?.kind === 'ok') {
            return resolved.symbol;
        }
    }
    return unresolvedCallee(workspaceRoot, call);
}

/**
 * Walk forward from one symbol over the calls the index recorded.
 *
 * Breadth first, one fact read per reached symbol, admission decided over a
 * whole layer at once. Admitting per calling symbol instead would order the
 * layer by whichever caller happened to be walked first, which is the provider's
 * row order wearing a disguise.
 */
export async function getClosure(
    source: ClosureSource,
    workspaceRoot: string,
    root: SymbolRef,
    options: ClosureOptions = {},
): Promise<ClosureResult> {
    const depth = clampBound(options.depth, CLOSURE_DEFAULT_DEPTH, CLOSURE_MAX_DEPTH);
    const cap = clampBound(options.cap, CLOSURE_DEFAULT_CAP, CLOSURE_MAX_CAP);
    const opts: ProviderQueryOptions = {
        ...(options.projectName === undefined ? {} : { projectName: options.projectName }),
        ...(options.generation === undefined ? {} : { generation: options.generation }),
    };

    const rootKey = closureKeyOf(root);
    const nodes: ClosureNode[] = [{ symbol: root, hop: 0 }];
    const edges: ClosureEdge[] = [];
    /** Call-site identity to the identity of the symbol it resolved to. */
    const published = new Map<string, string>([[rootKey, rootKey]]);
    /** Identities that are in `nodes`. */
    const admitted = new Set<string>([rootKey]);
    /** Call-site identities a bound turned away, counted but not returned. */
    const refused = new Set<string>();
    let truncated = false;
    let frontier: SymbolRef[] = [root];

    for (let level = 0; level < depth && frontier.length > 0; level++) {
        /** Every call this layer reported, in frontier order then alphabetical. */
        const found: { from: string; to: string; call: CallSite }[] = [];
        for (const from of frontier) {
            const facts = await source
                .getFacts(workspaceRoot, from, ['callees'], opts)
                .catch(() => ({}) as SymbolFacts);
            const fromKey = closureKeyOf(from);
            const layer = (facts.callees?.value ?? [])
                .map((call) => ({ from: fromKey, to: closureCallKey(call), call }))
                .filter((entry) => entry.to.length > 0)
                .sort((a, b) => compareText(a.to, b.to));
            found.push(...layer);
        }

        const next: SymbolRef[] = [];
        for (const to of [...new Set(found.map((entry) => entry.to))].sort(compareText)) {
            if (published.has(to)) {
                continue;
            }
            if (nodes.length >= cap) {
                // The cap turned it away. Counted in `visited`, absent from
                // `nodes`, and no edge is drawn for it: an arrow into a symbol
                // the answer does not hold would be a picture of something a
                // reader cannot look at.
                refused.add(to);
                truncated = true;
                continue;
            }
            const first = found.find((entry) => entry.to === to)!;
            const resolved = await resolveCallee(source, workspaceRoot, first.call, opts);
            const key = closureKeyOf(resolved);
            published.set(to, key);
            if (!admitted.has(key)) {
                admitted.add(key);
                nodes.push({ symbol: resolved, hop: level + 1, via: first.from });
                next.push(resolved);
            }
        }

        // Then the edges, in the order the calls were reported: grouped by the
        // symbol that makes them, which is the order a reader following one
        // function down the page would meet them in.
        for (const entry of found) {
            const target = published.get(entry.to);
            if (target === undefined) {
                continue;
            }
            edges.push({
                from: entry.from,
                to: target,
                ...(entry.call.line !== undefined ? { line: entry.call.line } : {}),
            });
        }

        // A layer that still had somewhere to go when the depth ran out is a
        // walk that stopped short, and says so.
        if (level + 1 === depth && next.length > 0) {
            truncated = true;
        }
        frontier = next;
    }

    return { root, nodes, edges, truncated, visited: admitted.size + refused.size, depth, cap };
}
