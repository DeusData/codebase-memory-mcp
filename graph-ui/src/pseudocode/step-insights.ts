/**
 * What lies behind a call, as far as what this window already holds says.
 *
 * No React, no DOM, no fetching, and above all no second server way: this
 * module reads the graph the galaxy already loaded (`/api/layout`, one answer
 * per project, kept in App state) and the call sites the twin already has, and
 * from those two it says what the index records about the symbol a step calls.
 * A step that reads "call validateId" can then also say that validateId raises
 * ValidationError, which is the one thing the code beside the block does not
 * show and the reader would otherwise have to go and look up.
 *
 * ## Four rules, and the third is the reason this file is small
 *
 * **Only a recorded relation becomes a note.** A note is a RAISES, a CALLS or a
 * CONFIGURES edge the index wrote between two nodes. There is no reading of the
 * callee's body here, no guess from its name, and no sentence assembled out of
 * two relations that would say more together than either says alone.
 *
 * **A callee is matched by its qualified name and by nothing else.** The
 * fixture of this project has two functions called `create`, in two files, and
 * a match on the bare name would put the notes of one under the steps of the
 * other. A call site the index resolved carries the qualified name the graph
 * uses for the same symbol, so the join is exact or it does not happen.
 *
 * **What is not in memory is not fetched.** The three kinds below exist because
 * the loaded layout answers them. Everything a reader might also want to know
 * about a callee (what it reads, what it writes, what it does in which order)
 * would need a `facts` request per callee, and this block is the one surface of
 * the product that costs nothing. What is out of reach is named in
 * {@link ENRICHMENT_MISSING} rather than quietly left out, so the next cycle
 * starts from a list instead of from a guess.
 *
 * **An empty note is not an empty callee.** A step with nothing beside it is a
 * step whose callee the loaded graph records none of these three relations for.
 * It is not a callee that raises nothing. That sentence is
 * {@link PSEUDOCODE_BEHIND_NOTE} and it stands behind the block's provenance
 * mark, because a note that only appears when there is something to say cannot
 * carry its own limit.
 */

import type { CallSite } from '../core/semantic-ir';

import {
    insightCalls,
    insightMore,
    insightRaises,
    insightReadsEnv,
} from './pseudocode-strings';

/**
 * One node of the loaded layout, in the shape this module needs it.
 *
 * Structurally a subset of `GraphNode` (src/galaxy/types.ts), so App can pass
 * the loaded layout straight in. Declared here rather than imported from the
 * galaxy, because what this module wants is "the graph this window holds" and
 * not "the scene": the day a second surface holds the same relations, it can
 * satisfy this interface without becoming a galaxy.
 */
export interface InsightNode {
    id: number;
    name: string;
    qualified_name?: string;
    label?: string;
    file_path?: string;
    start_line?: number;
}

/** One relation of the loaded layout, in the shape this module needs it. */
export interface InsightEdge {
    source: number;
    target: number;
    type: string;
}

/** The graph this window already has. */
export interface InsightGraph {
    nodes: readonly InsightNode[];
    edges: readonly InsightEdge[];
}

/**
 * Which relation produced a note.
 *
 * Three, and each one is a relation name the engine of this project really
 * writes for TypeScript (measured against the built index of
 * fixtures/atlas-sample on 2026-08-29: RAISES 3, CALLS 25, CONFIGURES 6).
 */
export type StepInsightKind = 'raises' | 'calls-on' | 'reads-env';

/** One thing the index records about the symbol a step calls. */
export interface StepInsight {
    kind: StepInsightKind;
    /** The sentence as it stands beside the step. */
    text: string;
}

/**
 * How many names one note spells out before it counts the rest.
 *
 * Three is what fits beside a step without the note becoming the line. The
 * fourth and every one after it are not dropped: they are counted, and the
 * count says so ({@link insightMore}). This is a display bound of one note and
 * not a rule about which steps are worth showing; the block has no such rule
 * and this cycle deliberately did not introduce one.
 */
export const STEP_INSIGHT_CAP = 3;

/** What a step's callee is, in the graph's terms. */
const RELATION = {
    raises: 'RAISES',
    calls: 'CALLS',
    configures: 'CONFIGURES',
} as const;

/**
 * What this block will not say, and why, so the decision stays checkable.
 *
 * Every entry is a thing a reader could reasonably want beside a step and that
 * the loaded graph does not carry. None of them is invented and none of them is
 * fetched; they are written into the proof artifact of this cycle
 * (verification/w8c/pseudocode.json, `enrichmentAvailable.missing`) so a later
 * cycle can pick one up with a server way behind it instead of rediscovering
 * the gap.
 */
export const ENRICHMENT_MISSING: readonly { kind: string; reason: string }[] = [
    {
        kind: 'what the callee reads or writes',
        reason:
            'The layout carries USAGE edges without a direction and without a kind, so "query goes to the '
            + 'database" cannot be read off them: a USAGE edge to `rows` says the two are related, not that '
            + 'the symbol reads it. The `reads` and `writes` families of the callee would answer it, and '
            + 'those come from a facts request per callee, which is a second server way this block does not '
            + 'take.',
    },
    {
        kind: 'the callee\'s own steps, in order',
        reason:
            'The graph says that the callee calls something, not in which order or under which condition. '
            + 'The walk behind the flow explainer has the order; reading it here would mean running that walk '
            + 'for every step of every block.',
    },
    {
        kind: 'whether a raised error escapes through this call',
        reason:
            'RAISES records where an error is constructed and thrown, never which caller lets it through. '
            + 'The note therefore says what the callee can raise and never what this symbol propagates.',
    },
    {
        kind: 'the callee\'s signature and return type',
        reason:
            'The layout nodes carry a name, a place, a kind and two call counts. Parameters and return types '
            + 'are in neither the layout nor the twin\'s IR of the calling symbol.',
    },
    {
        kind: 'anything at all about an unresolved callee',
        reason:
            'A call site the index did not resolve carries no qualified name, and this module joins on the '
            + 'qualified name alone (two functions named `create` in two files are the reason). Such a step '
            + 'says at its own line that the index records no place for it.',
    },
];

/** The measurement AC3 asks for: what the loaded data really gave, and what it cannot give. */
export interface EnrichmentAvailability {
    /**
     * Kinds that produced at least one note here, with how many of the called
     * symbols carry them.
     *
     * Counted per callee and not per step line, because that is what this
     * module knows: two steps that call the same symbol get the same note from
     * one reading. The proof run counts the LINES that carry a note separately
     * (`enrichedSteps`), and the two numbers are allowed to differ.
     */
    usable: { kind: StepInsightKind; source: string; symbols: number }[];
    /**
     * Kinds this module reads and that had nothing to say about this block.
     *
     * The third list exists so that `usable` cannot be read as the whole of
     * what is in reach. A kind that answered nothing here is not a kind that
     * cannot answer, and flattening the two would turn a quiet block into a
     * missing feature in the record.
     */
    silent: { kind: StepInsightKind; source: string }[];
    /** Kinds this block does not show, each with the reason it does not. */
    missing: { kind: string; reason: string }[];
}

/** Where a usable kind came from. Named per kind, because the reason differs per kind. */
const SOURCE: Readonly<Record<StepInsightKind, string>> = {
    raises: 'RAISES edges of the layout this window already loaded (/api/layout)',
    'calls-on': 'CALLS edges of the layout this window already loaded (/api/layout)',
    'reads-env': 'CONFIGURES edges to an environment node of the layout this window already loaded',
};

/**
 * The notes for one block's steps, keyed by the callee's qualified name.
 *
 * Total: no graph, an empty graph and a callee the graph does not know all
 * produce an empty map rather than an exception, because all three are ordinary
 * states of this window (the galaxy may not have answered yet) and none of them
 * is a statement about the code.
 */
export function stepInsightsOf(
    calls: readonly CallSite[],
    graph: InsightGraph | undefined,
): Map<string, StepInsight[]> {
    const out = new Map<string, StepInsight[]>();
    if (graph === undefined || graph.nodes.length === 0) {
        return out;
    }
    const byId = new Map<number, InsightNode>();
    const byQualifiedName = new Map<string, InsightNode>();
    for (const node of graph.nodes) {
        byId.set(node.id, node);
        const qualified = node.qualified_name;
        if (qualified !== undefined && qualified.length > 0 && !byQualifiedName.has(qualified)) {
            byQualifiedName.set(qualified, node);
        }
    }
    const outgoing = new Map<number, InsightEdge[]>();
    for (const edge of graph.edges) {
        const list = outgoing.get(edge.source);
        if (list === undefined) {
            outgoing.set(edge.source, [edge]);
        } else {
            list.push(edge);
        }
    }

    for (const call of calls) {
        const qualified = call.targetQualifiedName;
        if (qualified === undefined || qualified.length === 0 || out.has(qualified)) {
            continue;
        }
        const node = byQualifiedName.get(qualified);
        if (node === undefined) {
            continue;
        }
        const edges = outgoing.get(node.id) ?? [];
        const notes = notesOf(edges, byId);
        if (notes.length > 0) {
            out.set(qualified, notes);
        }
    }
    return out;
}

/**
 * What the loaded data gave for this block, and what it cannot give at all.
 *
 * The `usable` half is measured over the notes that were really produced, not
 * declared: a kind that answered nothing here does not appear, because the
 * point of the measurement is to say what was in reach and not what the code
 * hoped for. The `missing` half is the standing list, plus the whole of it when
 * this window holds no graph at all.
 */
export function enrichmentAvailabilityOf(
    insights: ReadonlyMap<string, StepInsight[]>,
    graph: InsightGraph | undefined,
): EnrichmentAvailability {
    const symbols: Record<StepInsightKind, number> = { raises: 0, 'calls-on': 0, 'reads-env': 0 };
    for (const notes of insights.values()) {
        for (const kind of new Set(notes.map((note) => note.kind))) {
            symbols[kind] += 1;
        }
    }
    const kinds = Object.keys(symbols) as StepInsightKind[];
    const usable = kinds
        .filter((kind) => symbols[kind] > 0)
        .map((kind) => ({ kind, source: SOURCE[kind], symbols: symbols[kind] }));
    const silent = kinds
        .filter((kind) => symbols[kind] === 0)
        .map((kind) => ({ kind, source: SOURCE[kind] }));
    const missing = [...ENRICHMENT_MISSING];
    if (graph === undefined || graph.nodes.length === 0) {
        missing.unshift({
            kind: 'every kind below',
            reason:
                'This window holds no layout for the project yet, so nothing about a called symbol was in '
                + 'reach when this block was built. That is a statement about this window and not about the '
                + 'code.',
        });
    }
    return { usable, silent, missing };
}

// ---------------------------------------------------------------------------

/** The notes one callee's outgoing edges produce, strongest first. */
function notesOf(edges: readonly InsightEdge[], byId: ReadonlyMap<number, InsightNode>): StepInsight[] {
    const raised = named(edges, RELATION.raises, byId);
    const env = named(edges, RELATION.configures, byId);
    const calls = edges.filter((edge) => edge.type === RELATION.calls).length;

    const notes: StepInsight[] = [];
    // Raised errors first: of the three, it is the one a reader of the calling
    // code cannot see at all and the one that changes what they do next.
    if (raised.length > 0) {
        notes.push({ kind: 'raises', text: listed(raised, insightRaises) });
    }
    for (const key of env.slice(0, STEP_INSIGHT_CAP)) {
        notes.push({ kind: 'reads-env', text: insightReadsEnv(key) });
    }
    if (env.length > STEP_INSIGHT_CAP) {
        notes.push({ kind: 'reads-env', text: insightMore(env.length - STEP_INSIGHT_CAP) });
    }
    if (calls > 0) {
        notes.push({ kind: 'calls-on', text: insightCalls(calls) });
    }
    return notes;
}

/** The names one relation points at, deduplicated, in the graph's order. */
function named(
    edges: readonly InsightEdge[],
    type: string,
    byId: ReadonlyMap<number, InsightNode>,
): string[] {
    const out: string[] = [];
    for (const edge of edges) {
        if (edge.type !== type) {
            continue;
        }
        const name = byId.get(edge.target)?.name;
        if (name !== undefined && name.length > 0 && !out.includes(name)) {
            out.push(name);
        }
    }
    return out;
}

/** One sentence over up to {@link STEP_INSIGHT_CAP} names, and a count for the rest. */
function listed(names: readonly string[], phrase: (name: string) => string): string {
    const shown = names.slice(0, STEP_INSIGHT_CAP);
    const rest = names.length - shown.length;
    const sentence = phrase(shown.join(', '));
    return rest > 0 ? `${sentence}, ${insightMore(rest)}` : sentence;
}
