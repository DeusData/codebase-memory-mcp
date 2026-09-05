/*
 * Herkunft: portiert am 2026-08-29 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/pseudocode/flow-model.ts
 * (174 Zeilen). Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert
 * uebernommen: flowNodes als Pre-Order-Faltung der Closure mit Zyklus-Badge und
 * FLOW_NODE_CAP, adjacencyOf, flowSteps als genau die nummerierten Zeilen,
 * flowHighlightOf als Abgleich ueber Labels statt ueber Bibliotheks-Ids, und
 * baseNameOf.
 *
 * Drei Aenderungen gegenueber dem Original, alle drei genannt:
 *
 *  - `TraceTreeNode` kommt dort aus dem Call-Navigator (navigator/
 *    trace-tree-model.ts), den dieses Projekt nicht hat. Hier steht der
 *    Ausschnitt, den die Faltung wirklich fuellt, als {@link FlowNode}: dieselben
 *    Felder, dieselben Namen, dieselbe Bedeutung von `enriched`.
 *  - `sanitizeLabel` kommt dort aus diagrams/mermaid-builder.ts. Dieses Projekt
 *    zeichnet kein Mermaid (siehe {@link flowSequence}), also steht die Funktion
 *    hier, wortgleich mit dem Original, damit der Abgleich weiterhin gegen genau
 *    die Zeichenkette laeuft, die der Kasten wirklich haelt.
 *  - `sequenceFromTrace` schreibt dort Mermaid-Text. Hier gibt
 *    {@link flowSequence} dasselbe Ergebnis als Modell zurueck: dieselben
 *    Lebenslinien, dieselben Pfeile, dieselben zwei Kappungen, dieselbe
 *    Zyklus-Notiz. Was wegfaellt, ist der Text; was bleibt, ist die Rechnung.
 *    Begruendung steht bei der Funktion.
 */

/**
 * The flow explainer's model: one walk, seen as a diagram and as a list.
 *
 * No React, no DOM, no services. The explainer's whole claim is that the picture
 * on the left and the steps on the right are two readings of one answer, and the
 * only way to keep that true is for both to be derived here, from the same
 * closure, by functions a test can call.
 *
 * **A cycle is drawn once and marked.** A closure records the edge that closes a
 * chain, because the cycle is a fact about the code; a tree cannot hold it, so
 * the node it points at is emitted with the cycle badge and not walked into. The
 * box turns that badge into a note inside the picture.
 *
 * **The tree is bounded here as well as in the drawing.** A closure of fifteen
 * symbols can unfold into a very large number of distinct paths, and the drawing
 * would cap what it drew but only after this file had built all of them. The
 * bound below is what keeps a button press cheap.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { ClosureEdge } from '../provider/closure';
import type { ClosureDocument, PseudocodeDocument, PseudocodeLine } from './pseudocode-builder';
import { identityOf } from './pseudocode-builder';

/**
 * How many nodes the unfolded walk may hold.
 *
 * Comfortably above the drawing's own interaction cap, so the picture is capped
 * by the rule that says so inside it rather than by this one, and far below the
 * point where unfolding a dense closure costs anything a reader would notice.
 */
export const FLOW_NODE_CAP = 60;

/** The badges a closure can support, which is the cycle and nothing else. */
export interface FlowBadges {
    cycle?: boolean;
    /** Never set here; present so the shape matches what the reference walks. */
    isTest?: boolean;
    construction?: boolean;
}

/**
 * One node of the unfolded walk.
 *
 * The fields the reference's `TraceTreeNode` carries and this fold actually
 * fills. `enriched` is false throughout on purpose: nothing here looked up
 * whether a symbol is test code, so nothing here claims to have.
 */
export interface FlowNode {
    kind: 'symbol';
    id: string;
    ref: SymbolRef;
    badges: FlowBadges;
    /** Distance from the root in invocation steps. A direct neighbour is 1. */
    depth: number;
    /** Qualified names from the root down to and including this node. */
    path: readonly string[];
    enriched: boolean;
}

/**
 * The closure, unfolded into the pre-order walk the sequence box reads.
 *
 * The root itself is not in the list: the box takes the root's lifeline as an
 * argument, and a node at depth zero would draw an arrow from the root to
 * itself. Everything else is one node per edge followed, so a symbol reached
 * from two callers appears twice, which is what a sequence diagram is: a
 * timeline of calls and not a set of symbols.
 */
export function flowNodes(closure: ClosureDocument): FlowNode[] {
    const symbols = new Map<string, SymbolRef>();
    for (const symbol of closure.symbols) {
        symbols.set(identityOf(symbol), symbol);
    }
    const outgoing = adjacencyOf(closure.edges);
    const out: FlowNode[] = [];
    const rootKey = identityOf(closure.root);

    const walk = (from: string, path: readonly string[], depth: number): void => {
        for (const to of outgoing.get(from) ?? []) {
            if (out.length >= FLOW_NODE_CAP) {
                return;
            }
            const ref = symbols.get(to);
            if (ref === undefined) {
                continue;
            }
            const cycle = path.includes(to);
            const next = cycle ? path : [...path, to];
            out.push({
                kind: 'symbol',
                id: `flow.${out.length}`,
                ref,
                badges: cycle ? { cycle: true } : {},
                depth,
                path: next,
                // Nothing here looked up whether the symbol is test code or
                // whether the step constructs a class, so nothing here claims to
                // have. The badges a closure can support are the cycle and
                // nothing else.
                enriched: false,
            });
            if (!cycle) {
                walk(to, next, depth + 1);
            }
        }
    };
    walk(rootKey, [rootKey], 1);
    return out;
}

/** Which symbols each symbol calls, in the order the walk recorded, deduplicated. */
function adjacencyOf(edges: readonly ClosureEdge[]): Map<string, string[]> {
    const out = new Map<string, string[]>();
    const seen = new Set<string>();
    for (const edge of edges) {
        const pair = `${edge.from} ${edge.to}`;
        if (seen.has(pair)) {
            continue;
        }
        seen.add(pair);
        const list = out.get(edge.from) ?? [];
        list.push(edge.to);
        out.set(edge.from, list);
    }
    return out;
}

/** One walkable step of the explainer: a numbered line, and where it sits in the block. */
export interface FlowStep {
    /** Index of the line inside {@link PseudocodeDocument.lines}. */
    lineIndex: number;
    line: PseudocodeLine;
}

/**
 * The lines a reader can walk, in block order.
 *
 * The numbered ones and only those. A group heading is this product's own word
 * over a symbol and a note is a statement about the block; stopping on either
 * would be Prev/Next walking through the furniture.
 */
export function flowSteps(document: PseudocodeDocument): FlowStep[] {
    const out: FlowStep[] = [];
    document.lines.forEach((line, lineIndex) => {
        if (line.order !== undefined) {
            out.push({ lineIndex, line });
        }
    });
    return out;
}

/** What one step lights up in the drawing. */
export interface FlowHighlight {
    /** The lifeline: the file the step's target is declared in. */
    participant?: string;
    /** The arrow: the name the drawing puts on it, which is the target's own name. */
    message?: string;
}

/**
 * Which labels in the drawing belong to one step.
 *
 * Labels rather than element ids, and that is a decision rather than a
 * shortcut. The drawing library's internal ids are its own business and have
 * changed shape between releases; the labels are this product's strings, put
 * there by the sequence builder from the same facts this step came from. Matching
 * on them means the highlight cannot point at the wrong arrow, and cannot break
 * because a library renamed a node.
 *
 * A raise or an environment read has no lifeline of its own in a sequence of
 * calls, so it lights nothing up and says so by answering with an empty
 * highlight; the editor still follows it, which is where that fact lives.
 */
export function flowHighlightOf(line: PseudocodeLine): FlowHighlight {
    if (line.kind !== 'step') {
        return {};
    }
    // Sanitised exactly as the builder sanitises them, so what is compared is
    // the string the drawing actually holds rather than the one the fact did.
    return {
        participant: line.targetFile === undefined ? undefined : sanitizeLabel(baseNameOf(line.targetFile)),
        message: line.targetName === undefined ? undefined : sanitizeLabel(line.targetName),
    };
}

/** The last path segment of a URI, which is what a lifeline is labelled with. */
export function baseNameOf(uri: string): string {
    const path = uri.split('?')[0].split('#')[0];
    const segments = path.split('/');
    return segments[segments.length - 1] || path;
}

// ---------------------------------------------------------------------------
// Der Kasten
// ---------------------------------------------------------------------------

/** Longest label drawn. A longer one is cut rather than allowed to widen a box off screen. */
export const LABEL_CAP = 60;

/**
 * A label the box will render rather than choke on.
 *
 * Quotes end a flowchart label, semicolons end a flowchart statement, hashes
 * open an entity code and angle brackets open markup. None of them can be
 * escaped consistently across the two diagram kinds the reference writes, so
 * they are dropped: a symbol called `render<T>` is drawn as `renderT`, which is
 * a readable approximation, where the alternative is a panel that renders
 * nothing because of somebody's type parameter.
 *
 * This surface draws DOM and would survive an angle bracket. The rule is kept
 * anyway and unchanged, because {@link flowHighlightOf} matches a step against
 * the label the box holds: two different sanitisers would be a highlight that
 * silently stops matching.
 */
export function sanitizeLabel(text: string): string {
    const flattened = text
        // A newline inside a label ends the statement it sits in, and every
        // other control character is invisible in the drawing and confusing in
        // the source, so the whole range becomes one space.
        .replace(/[\u0000-\u001F\u007F]+/g, ' ')
        .replace(/["'`;#<>[\]{}|]/g, '')
        .replace(/\s+/g, ' ')
        .trim();
    return flattened.length > LABEL_CAP ? `${flattened.slice(0, LABEL_CAP - 1).trimEnd()}.` : flattened;
}

/** How many lifelines one sequence box draws before the rest are left out. */
export const SEQUENCE_PARTICIPANT_CAP = 12;

/** How many arrows one sequence box draws before the rest are left out. */
export const SEQUENCE_INTERACTION_CAP = 25;

/** One arrow of the box: who calls whom, under which name. */
export interface FlowInteraction {
    /** Lifeline label the arrow leaves. */
    from: string;
    /** Lifeline label the arrow arrives at. */
    to: string;
    /** The name on the arrow, which is the callee's own name, sanitised. */
    message: string;
    /** True when this arrow closes a chain the walk had already been through. */
    cycle: boolean;
}

/** One walk, as the box draws it. */
export interface FlowSequence {
    /** Lifeline labels in the order the columns stand, the root first. */
    participants: string[];
    interactions: FlowInteraction[];
    /** Arrows a bound left out. Zero when nothing was left out. */
    omitted: number;
}

/**
 * A sequence of one walk, in the order the tree holds it.
 *
 * Wortgleich mit `sequenceFromTrace` des Referenzprojekts (diagrams/
 * mermaid-builder.ts), bis auf den letzten Schritt: dort werden die
 * Lebenslinien und Pfeile in Mermaid-Text geschrieben, hier werden sie
 * zurueckgegeben. Der Grund ist nicht Geschmack. Diese Oberflaeche liefert
 * keine Diagramm-Bibliothek aus (air-gapped, kein CDN, PLAN Abschnitt 4), und
 * eine zweite Rechnung fuer denselben Kasten waere genau die zweite Antwort auf
 * "was ist ein Pfeil", die das Original vermeidet. Also wird die Rechnung
 * portiert und nur die Ausgabe getauscht.
 *
 * `rootLabel` names the lifeline the walk starts at: the caller passes the file
 * the root symbol is declared in, because every other lifeline is a file too and
 * a box that mixed the two granularities would read as though one column were a
 * different kind of thing.
 *
 * `nodes` is a flattened tree, parents before children. That order is what makes
 * the caller of each arrow recoverable without a parent pointer: in a pre-order
 * walk the caller of a node at depth d is the last node seen at depth d-1, and
 * the root stands at depth 0. A node whose caller was left out by a cap is left
 * out as well, so the box is always a connected part of the walk rather than a
 * set of arrows starting nowhere.
 */
export function flowSequence(rootLabel: string, nodes: readonly FlowNode[]): FlowSequence {
    const participants: string[] = [];
    const seen = new Set<string>();
    const add = (label: string): void => {
        if (!seen.has(label)) {
            seen.add(label);
            participants.push(label);
        }
    };
    const root = sanitizeLabel(rootLabel) || 'root';
    add(root);

    /** The lifeline each depth is currently drawn at. Undefined where a cap cut the branch. */
    const callerAt: (string | undefined)[] = [root];
    const interactions: FlowInteraction[] = [];
    let omitted = 0;

    for (const node of nodes) {
        if (node.depth < 1) {
            continue;
        }
        const caller = callerAt[node.depth - 1];
        const callee = moduleLabel(node);
        const room = seen.has(callee) || seen.size < SEQUENCE_PARTICIPANT_CAP;
        if (caller === undefined || interactions.length >= SEQUENCE_INTERACTION_CAP || !room) {
            omitted += 1;
            callerAt[node.depth] = undefined;
            continue;
        }
        add(callee);
        interactions.push({
            from: caller,
            to: callee,
            message: sanitizeLabel(node.ref.name) || callee,
            cycle: node.badges.cycle === true,
        });
        callerAt[node.depth] = callee;
    }

    return { participants, interactions, omitted };
}

/** The lifeline one node is drawn on: its file, or its own name when it has no file. */
export function moduleLabel(node: FlowNode): string {
    const uri = node.ref.uri;
    if (uri.length === 0) {
        return sanitizeLabel(node.ref.name) || 'unknown';
    }
    return sanitizeLabel(baseNameOf(uri)) || sanitizeLabel(node.ref.name) || 'unknown';
}

/**
 * Which arrow of the box one step lights up, or none.
 *
 * The label match of {@link flowHighlightOf}, applied to the arrows the box
 * actually holds. Answers -1 when the step lights nothing up, which is the
 * honest answer for a raise, an environment read and a call whose target the
 * bounds left out of the picture. The caller says so rather than colouring an
 * arbitrary arrow.
 */
export function flowArrowFor(sequence: FlowSequence, line: PseudocodeLine): number {
    const highlight = flowHighlightOf(line);
    if (highlight.participant === undefined || highlight.message === undefined) {
        return -1;
    }
    return sequence.interactions.findIndex(
        (arrow) => arrow.to === highlight.participant && arrow.message === highlight.message,
    );
}
