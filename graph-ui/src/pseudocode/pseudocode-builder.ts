/*
 * Herkunft: portiert am 2026-08-29 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/pseudocode/pseudocode-builder.ts
 * (456 Zeilen). Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert
 * uebernommen: die vier Scopes, die fuenf Zeilenarten, die drei verschiedenen
 * sourceRef-Semantiken samt ihrer Begruendung, verbOf als Strategie-Abbildung,
 * PseudocodeHonesty mit capped, pseudocodeText, applyRefinedPseudocode mit der
 * exakten Zeilen-, Reihenfolgen- und Nummernpruefung, leadingNumberOf und
 * identityOf.
 *
 * Aenderungen gegenueber dem Original, beide genannt statt versteckt:
 *
 *  - Die Importpfade zeigen auf die ebenfalls portierten Dateien dieses
 *    Projekts (SymbolRef, CallSite, SemanticIR und toGraphLine aus src/core/,
 *    der Wortlaut aus ./pseudocode-strings).
 *  - W8c: eine Zeile traegt jetzt auch, WOHIN sie fuehrt (`targetLine`,
 *    `targetRef`) und was der Index ueber das aufgerufene Symbol schon
 *    hergibt (`behind`, aus ./step-insights.ts). Beides steht neben `text` und
 *    nicht darin: `pseudocodeText` und damit die Pruefung einer Umformulierung
 *    (applyRefinedPseudocode) sehen genau dieselben Zeilen wie vorher.
 *  - Statt des `ClosureDto` der Theia-RPC nimmt der Builder ein
 *    {@link ClosureDocument}. Dieses Projekt hat keine RPC-Fassade; sein Walk
 *    ist `src/provider/closure.ts` und liefert `nodes` (Symbol plus Hop plus
 *    via) statt einer flachen `symbols`-Liste. Das ist dieselbe Antwort in einer
 *    reicheren Form, und `closureDocumentOf` faltet sie auf die Form zurueck,
 *    die der Builder liest. Der Builder bleibt damit wortgleich mit dem
 *    Original, und die Umrechnung steht an einer Stelle statt in jedem Aufrufer.
 */

/**
 * Pseudocode from semantic IR. No React, no DOM, no services.
 *
 * A numbered list of steps under the heading "pseudocode" is the most
 * believable thing this product can put on a screen and therefore the easiest
 * place in it to lie. Four rules keep it a rendering of the index.
 *
 * **Only a fact becomes a line.** Every step is a call site the index reported,
 * every raise line is a raised type it reported, every environment line is a
 * read it reported. There is no control flow here, no `if`, no `for`, no
 * `return`: the graph does not record them, so a block that drew them would be
 * inventing the one thing a reader would most want to trust. What is left is
 * narrower than pseudocode usually means and is exactly what CodeAtlas knows.
 *
 * **Every line points somewhere.** A step, a raise and an environment read all
 * carry a `sourceRef`, so a reader can check any of them against the code in one
 * click. The three do not point at the same kind of place, and that difference
 * is documented on {@link PseudocodeLine.sourceRef} rather than smoothed over: a
 * step points at its call site inside the symbol being described, and a raise
 * points at the site the index recorded, which is very often in the callee's
 * file. Group and note lines carry no ref, because they are this file's own
 * words rather than a finding.
 *
 * **The verb is the strategy.** `construct` when the index called the site a
 * construction, `call` otherwise. Guessing a richer verb from the callee's name
 * ("validates", "persists") would be the product writing the comment the code
 * does not have.
 *
 * **What is missing is counted.** {@link PseudocodeHonesty} says how many of the
 * symbols in scope contributed anything and names the ones that did not, and
 * `capped` is true when the walk the block was built from stopped at a bound.
 * A block assembled from ten symbols of which two had facts is not a
 * description of ten symbols, and nothing else on screen can tell a reader that.
 *
 * Everything here is pure. The surface owns the fetching, the scope resolution
 * and the refinement; this file owns what the answer looks like once the facts
 * are in hand, which is the part worth testing without a browser.
 */

import type { SymbolRef } from '../core/focus-protocol';
import { toGraphLine } from '../core/positions';
import type { CallSite, SemanticIR } from '../core/semantic-ir';
import type { ClosureEdge, ClosureResult } from '../provider/closure';

import { enrichmentAvailabilityOf, stepInsightsOf } from './step-insights';
import type { EnrichmentAvailability, InsightGraph, StepInsight } from './step-insights';
import {
    PSEUDOCODE_PARTIAL_SCOPE,
    PSEUDOCODE_VERB_CALL,
    PSEUDOCODE_VERB_CONSTRUCT,
    pseudocodeCappedNote,
    pseudocodeClassTitle,
    pseudocodeClosureTitle,
    pseudocodeEmptyGroup,
    pseudocodeEnvLine,
    pseudocodeMethodGroup,
    pseudocodeRaiseLine,
    pseudocodeSelectionTitle,
    pseudocodeStepLine,
    pseudocodeSymbolGroup,
    pseudocodeSymbolTitle,
} from './pseudocode-strings';

/**
 * The walk, in the shape the block and the diagram read it.
 *
 * Field for field the `ClosureDto` of the reference project. It exists here
 * because this project's walk answers with `nodes`, and a reader of the builder
 * should not have to know that: what a block is built from is a root, the
 * symbols the walk returned, the edges between them, and the two figures that
 * say how much of the walk is missing.
 */
export interface ClosureDocument {
    root: SymbolRef;
    /** Reached symbols in walk order, the root first. */
    symbols: SymbolRef[];
    /** Calls between the symbols above. */
    edges: ClosureEdge[];
    /** True when a bound stopped the walk while symbols were still to reach. */
    truncated: boolean;
    /** Distinct symbols the walk looked at, including the ones a bound refused. */
    visited: number;
}

/** This project's walk, in the shape the reference's consumers were written for. */
export function closureDocumentOf(result: ClosureResult): ClosureDocument {
    return {
        root: result.root,
        symbols: result.nodes.map((node) => node.symbol),
        edges: result.edges,
        truncated: result.truncated,
        visited: result.visited,
    };
}

/** Which question a block answers. Each one groups its symbols differently. */
export type PseudocodeScopeKind = 'symbol' | 'class' | 'selection' | 'closure';

/**
 * What a block is about.
 *
 * `label` is the name in the heading: the symbol for the first three scopes and
 * the walk's root for the fourth. It is passed in rather than derived from the
 * first IR because a selection can legitimately hold no IR at all, and a heading
 * that disappeared when the answer was empty would leave the reader looking at a
 * block with nothing to say and no way to tell what it was about.
 */
export interface PseudocodeScope {
    kind: PseudocodeScopeKind;
    label?: string;
    /**
     * True when the surface could not look at every line of the scope, or could
     * not keep every symbol it found there.
     *
     * Only the `class` and `selection` scopes can set it, and only they can
     * know: they are resolved by asking the index what encloses a line, that
     * question is asked at a stride over long ranges, and the answers are kept
     * under a ceiling. Both bounds can drop a member, and a block that dropped
     * one silently would be a list of methods that is quietly not the list of
     * methods. The `closure` scope has its own, sharper statement of the same
     * thing; see {@link PseudocodeHonesty.capped}.
     */
    partial?: boolean;
}

/** What a block is built from. */
export interface PseudocodeInput {
    /**
     * One IR per symbol in scope.
     *
     * For `symbol` the list holds one; for `class` the class's members in the
     * order the surface resolved them; for `selection` the symbols the selection
     * touched; for `closure` however many of the walk's symbols the surface
     * managed to fetch, in any order, because the walk itself decides the order.
     */
    irs: SemanticIR[];
    /** The walk, for the `closure` scope. Ignored by the other three. */
    closure?: ClosureDocument;
    /**
     * The graph this window already holds, for what lies behind a call (W8c).
     *
     * Optional and never fetched. Absent means the block is exactly what it was
     * before: the facts of the symbols in scope, and no note beside a step. It
     * does not mean the callees do nothing, which is why the sentence saying so
     * stands behind the block's provenance mark whether or not a note appeared.
     */
    graph?: InsightGraph;
}

/** What kind of line this is, which is also what a reader may do with it. */
export type PseudocodeLineKind =
    /** A call the index reported, numbered. */
    | 'step'
    /** An error type the index reported the symbol can raise, numbered. */
    | 'raise'
    /** An environment value the index reported the symbol reads, numbered. */
    | 'env'
    /** A heading over one symbol's lines. This file's own word, never a finding. */
    | 'group'
    /** Something said about the block itself: a bound, or an absence. */
    | 'note';

/**
 * Where one line came from.
 *
 * `line` is a 1-based graph line, the convention every fact in the IR uses. It
 * is deliberately not converted here: a surface reveals it through
 * `positions.ts`, which is the one place in the product allowed to add or
 * subtract one from a line number.
 */
export interface PseudocodeSourceRef {
    uri: string;
    line: number;
}

/** One line of a block. */
export interface PseudocodeLine {
    text: string;
    kind: PseudocodeLineKind;
    /**
     * The place this line was read from, absent on `group` and `note` lines.
     *
     * Three different kinds of place, on purpose:
     *
     *  - a `step` points at its call site, inside the file of the symbol whose
     *    group it belongs to. That is where the reader is now.
     *  - a `raise` points at the site the index recorded for the raised type,
     *    which is usually inside the callee that raises it rather than inside
     *    the symbol being described. The index records where the error comes
     *    from, not where this symbol lets it through.
     *  - an `env` read points at the reading symbol's declaration, because the
     *    0.9.0 graph records the relation without a line and there is nowhere
     *    more precise to send anybody.
     *
     * A fact with no usable location falls back to the declaration of the symbol
     * whose group the line is in, so a line is never unclickable while claiming
     * to have come from somewhere.
     */
    sourceRef?: PseudocodeSourceRef;
    /** Identity of the symbol whose facts produced this line, when one did. */
    group?: string;
    /** The number the line wears, for the numbered kinds. */
    order?: number;
    /** What the line points at: the callee, the raised type, the environment key. */
    targetName?: string;
    /** Absolute URI of the file the target is declared in, when the facts name one. */
    targetFile?: string;
    /**
     * 1-based graph line the target is declared on, when the facts recorded one
     * (W8c).
     *
     * Deliberately beside {@link sourceRef} and not instead of it. A step's
     * `sourceRef` is the call site, inside the file the reader is looking at;
     * this is the declaration of the symbol being called, in another file. Both
     * are true and they are two different places, which is the whole reason the
     * facts view shows "validateId validate.ts:33" beside a site line rather
     * than one number.
     */
    targetLine?: number;
    /**
     * Where following this line leads, ready to open (W8c).
     *
     * For a step that is the callee's declaration; for a raise and an
     * environment read it is the same place {@link sourceRef} names, because
     * for those two the recorded site already IS the thing the line is about.
     * Absent when the index recorded no file for the target, and the surface
     * says so at the line rather than leaving it silent.
     */
    targetRef?: PseudocodeSourceRef;
    /**
     * What the index records about the symbol this step calls (W8c).
     *
     * Empty for every kind but `step`, and empty for a step whose callee the
     * loaded graph knows nothing about. Never part of {@link text}: a
     * refinement is checked line by line against the text it was sent, and a
     * note that travelled inside the text would make the block's own additions
     * look like the model's.
     */
    behind?: StepInsight[];
}

/** How much of the scope actually made it into the block. */
export interface PseudocodeHonesty {
    /** Symbols in scope that contributed at least one line. */
    coveredSymbols: number;
    /** Symbols in scope that contributed nothing, by name. */
    uncovered: string[];
    /** True when the walk the block was built from stopped at one of its bounds. */
    capped: boolean;
}

/** One block, ready to render, copy or send. */
export interface PseudocodeDocument {
    title: string;
    lines: PseudocodeLine[];
    honest: PseudocodeHonesty;
    /** Symbols in scope, in the order their groups appear. */
    scopeSymbols: number;
    /**
     * What the already-loaded data gave for the steps of this block, and what
     * it cannot give at all (W8c).
     *
     * It travels with the document because it is the answer to a question a
     * reader of the code cannot answer from the screen: why does this step say
     * something and that one nothing. The proof run writes it out; a later
     * cycle that adds a server way starts from the `missing` half.
     */
    enrichment: EnrichmentAvailability;
}

/**
 * Turn one scope's facts into a block.
 *
 * Total: every scope answers, including the ones with nothing to say. A scope
 * that came back empty renders a heading, a note and an honest footer, which is
 * a better answer than a blank panel and a much better one than a block that
 * quietly omitted the symbols it could not describe.
 */
export function buildPseudocode(scope: PseudocodeScope, input: PseudocodeInput): PseudocodeDocument {
    const lines: PseudocodeLine[] = [];
    const uncovered: string[] = [];
    let covered = 0;
    /** One counter for the whole block: a group heading never restarts it. */
    const counter = { next: 1 };

    const ordered = orderedScope(scope, input);
    const grouped = scope.kind !== 'symbol';
    // One join for the whole block, over every symbol in scope: the graph is
    // one answer and walking it once per group would be the same reading done
    // as many times as the block has headings.
    const insights = stepInsightsOf(
        ordered.flatMap((entry) => entry.ir?.steps.value ?? []),
        input.graph,
    );

    for (const entry of ordered) {
        if (grouped) {
            lines.push({
                kind: 'group',
                text: scope.kind === 'class'
                    ? pseudocodeMethodGroup(entry.name)
                    : pseudocodeSymbolGroup(entry.name),
                group: entry.key,
            });
        }
        const produced = entry.ir === undefined ? [] : linesOf(entry.ir, entry.key, counter, insights);
        if (produced.length === 0) {
            uncovered.push(entry.name);
            lines.push({ kind: 'note', text: pseudocodeEmptyGroup(entry.name), group: entry.key });
            continue;
        }
        covered += 1;
        lines.push(...produced);
    }

    const hidden = hiddenSymbolsOf(scope, input);
    if (hidden > 0) {
        lines.push({ kind: 'note', text: pseudocodeCappedNote(hidden) });
    }
    if (scope.partial === true) {
        lines.push({ kind: 'note', text: PSEUDOCODE_PARTIAL_SCOPE });
    }

    return {
        title: titleOf(scope, ordered),
        lines,
        honest: {
            coveredSymbols: covered,
            uncovered,
            capped: (scope.kind === 'closure' && input.closure?.truncated === true) || scope.partial === true,
        },
        scopeSymbols: ordered.length,
        enrichment: enrichmentAvailabilityOf(insights, input.graph),
    };
}

/** The plain text a reader copies, and the text a refinement is asked to rewrite. */
export function pseudocodeText(document: PseudocodeDocument): string {
    return document.lines.map((line) => line.text).join('\n');
}

/**
 * Take a rewritten block and put its wording back on the deterministic lines,
 * or refuse.
 *
 * The refusal is the point. A rewrite is prose over a list of findings, and the
 * only way it can stay that is if every line it comes back with maps onto
 * exactly one line that was sent. So the mapping is positional and checked: the
 * answer must hold the same number of lines in the same order, each numbered
 * line must carry the same number it was sent with, and each unnumbered line
 * must still be unnumbered. Anything else is a different list, and a different
 * list about somebody's code is not something this product will render.
 *
 * Undefined means "keep the deterministic block". The caller says why.
 */
export function applyRefinedPseudocode(
    document: PseudocodeDocument,
    refined: string,
): PseudocodeDocument | undefined {
    const incoming = refined.split('\n').map((line) => line.trim()).filter((line) => line.length > 0);
    if (incoming.length !== document.lines.length) {
        return undefined;
    }
    const lines: PseudocodeLine[] = [];
    for (let index = 0; index < document.lines.length; index++) {
        const original = document.lines[index];
        const candidate = incoming[index];
        const numbered = leadingNumberOf(candidate);
        if (original.order !== numbered) {
            return undefined;
        }
        lines.push({ ...original, text: candidate });
    }
    return { ...document, lines };
}

/** The number a line leads with, or undefined when it leads with something else. */
export function leadingNumberOf(text: string): number | undefined {
    const match = /^(\d+)[.)]\s/.exec(text);
    return match === null ? undefined : Number(match[1]);
}

// ---------------------------------------------------------------------------

/** One symbol of the scope, with the IR that describes it when the surface had one. */
interface ScopeEntry {
    key: string;
    name: string;
    ir?: SemanticIR;
}

/**
 * The scope's symbols, in the order their groups appear.
 *
 * For three of the four scopes that is the order the surface resolved them in,
 * because the surface is what knows how it found them: source order for a class,
 * selection order for a selection. For a closure it is the walk's own order,
 * which is the backend's contract and is the same for two readings of one index;
 * taking the order of whatever IRs happened to arrive first would make the block
 * depend on which round trip won a race.
 */
function orderedScope(scope: PseudocodeScope, input: PseudocodeInput): ScopeEntry[] {
    const byKey = new Map<string, SemanticIR>();
    for (const ir of input.irs) {
        byKey.set(identityOf(ir.symbol), ir);
    }
    if (scope.kind !== 'closure' || input.closure === undefined) {
        return input.irs.map((ir) => ({ key: identityOf(ir.symbol), name: ir.symbol.name, ir }));
    }
    return input.closure.symbols.map((symbol) => ({
        key: identityOf(symbol),
        name: symbol.name,
        ir: byKey.get(identityOf(symbol)),
    }));
}

/** How many symbols the block is missing because a bound cut the walk short. */
function hiddenSymbolsOf(scope: PseudocodeScope, input: PseudocodeInput): number {
    const closure = input.closure;
    if (scope.kind !== 'closure' || closure === undefined || !closure.truncated) {
        return 0;
    }
    return Math.max(1, closure.visited - closure.symbols.length);
}

function titleOf(scope: PseudocodeScope, ordered: readonly ScopeEntry[]): string {
    const label = scope.label ?? ordered[0]?.name ?? '';
    switch (scope.kind) {
        case 'class':
            return pseudocodeClassTitle(label);
        case 'selection':
            return pseudocodeSelectionTitle(ordered.length);
        case 'closure':
            return pseudocodeClosureTitle(label);
        default:
            return pseudocodeSymbolTitle(label);
    }
}

/**
 * One symbol's lines: its steps, then what it can raise, then what it reads.
 *
 * The order is the reader's, not the graph's. Steps come first because they are
 * the body of the thing; the raised types and the environment reads are
 * conditions around it, and putting either above the steps would make the first
 * line of a function's description something that happens on one path through
 * it.
 */
function linesOf(
    ir: SemanticIR,
    key: string,
    counter: { next: number },
    insights: ReadonlyMap<string, StepInsight[]>,
): PseudocodeLine[] {
    const declaration = declarationRefOf(ir);
    const lines: PseudocodeLine[] = [];

    for (const call of ir.steps.value) {
        const order = counter.next++;
        const behind = insights.get(call.targetQualifiedName ?? '') ?? [];
        lines.push({
            kind: 'step',
            order,
            group: key,
            text: pseudocodeStepLine(order, verbOf(call), call.targetName),
            sourceRef: stepRefOf(ir, call) ?? declaration,
            targetName: call.targetName,
            targetFile: call.targetFile,
            targetLine: call.targetLine,
            // The declaration of the callee, which is where following the step
            // takes a reader. `targetLine` is used and never `line`: the call
            // site's number means nothing inside the callee's file.
            targetRef: refOf(call.targetFile, call.targetLine),
            ...(behind.length > 0 ? { behind } : {}),
        });
    }

    for (const raised of ir.throws.value) {
        const order = counter.next++;
        lines.push({
            kind: 'raise',
            order,
            group: key,
            text: pseudocodeRaiseLine(order, raised.type),
            sourceRef: refOf(raised.file, raised.line) ?? declaration,
            targetName: raised.type,
            targetFile: raised.file,
            targetLine: raised.line,
            // The same place the text of the line opens. A raise is not a
            // journey from here to somewhere else: the site the index recorded
            // is the thing the line is about.
            targetRef: refOf(raised.file, raised.line),
        });
    }

    for (const read of environmentReadsOf(ir)) {
        const order = counter.next++;
        lines.push({
            kind: 'env',
            order,
            group: key,
            text: pseudocodeEnvLine(order, read.name),
            sourceRef: refOf(read.file, read.line) ?? declaration,
            targetName: read.name,
            targetFile: read.file,
            targetLine: read.line,
            targetRef: refOf(read.file, read.line),
        });
    }

    return lines;
}

/**
 * Which of the symbol's reads are environment values.
 *
 * `reads` holds every piece of state the index attributed to the symbol, and
 * only the ones it classified as coming from outside the process belong in a
 * line that says "from the environment". A `global` is what the 0.9.0 provider
 * records an environment read as; anything else is a field or a variable and is
 * already visible in the twin's data section.
 */
function environmentReadsOf(ir: SemanticIR): SemanticIR['reads']['value'] {
    return ir.reads.value.filter((read) => read.kind === 'global');
}

/** `construct` when the index called it one, `call` otherwise. Nothing is inferred. */
function verbOf(call: CallSite): string {
    return call.strategy === 'construction' ? PSEUDOCODE_VERB_CONSTRUCT : PSEUDOCODE_VERB_CALL;
}

/**
 * Where a step happens: the call site, in the file of the symbol making the
 * call.
 *
 * Never the callee's declaration. A call to `validateUser` on line 24 of
 * `userService.ts` is declared on line 19 of `validate.ts`, and both numbers are
 * true about different files; a step is a thing this symbol does, so it points
 * at the line inside this symbol. Following the callee is what the twin's rows
 * are for.
 */
function stepRefOf(ir: SemanticIR, call: CallSite): PseudocodeSourceRef | undefined {
    return call.line === undefined ? undefined : { uri: ir.symbol.uri, line: call.line };
}

function refOf(file: string | undefined, line: number | undefined): PseudocodeSourceRef | undefined {
    if (file === undefined || file.length === 0 || line === undefined) {
        return undefined;
    }
    return { uri: file, line };
}

/** The symbol's own declaration, in graph line space. The fallback every line can reach. */
function declarationRefOf(ir: SemanticIR): PseudocodeSourceRef {
    return { uri: ir.symbol.uri, line: toGraphLine(ir.symbol.range.start.line) };
}

/**
 * How a symbol is identified across the block.
 *
 * The qualified name is the index's own identifier, so it is what a closure's
 * edges name and what an IR can be looked up by. A symbol without one is not
 * indexed; its file and name still tell two of them apart, which is all this
 * key is asked to do.
 */
export function identityOf(symbol: SymbolRef): string {
    return symbol.qualifiedName ?? `${symbol.uri}#${symbol.name}`;
}
