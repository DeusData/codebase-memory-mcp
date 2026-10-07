/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/twin/twin-view-model.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * buildSections, evidenceFor, stepTarget, worstState (samt der writes-Ausnahme
 * in dataSection), runtimeSection, locationLabel, displayFile, refAt,
 * evidenceTarget und CONSTRUCTION_STRATEGY.
 *
 * Aenderungen gegenueber dem Original: die Importpfade (SymbolRef,
 * toEditorRange, IR-Typen und runtimeCitationsFor kommen aus src/core/ statt
 * aus @codeatlas/core).
 *
 * Die in W2b deklarierte Auslassung ist beglichen: importsSection ("What it
 * pulls in", Original Z. 470-500) ist reaktiviert am 2026-08-29 mit dem
 * Pseudocode-Port, W4c. Die ImportsGroup, aus der sie gebaut wird, kommt aus
 * src/pseudocode/imports-group.ts; woher diese Gruppe ihre Daten bekommt, steht
 * in src/pseudocode/imports-source.ts.
 */

/**
 * The semantic IR, arranged for reading. No React, no DOM, no services.
 *
 * The twin renders sections of rows, and deciding what a row says, what it
 * points at and which citation backs it is not a rendering concern: it is a
 * reading of the IR, and it is the part that will be reused when the depth
 * slider starts showing more or less of the same model. Keeping it here means
 * the next surface that wants the same content writes different JSX rather
 * than a second interpretation of the IR.
 *
 * Two conventions run through the whole file.
 *
 * A row's `factPath` is the address of its citation inside the IR, in the same
 * `fact` or `fact[row]` grammar the backend's evidence service parses. The
 * providers build the evidence array in lockstep with the value array, so row
 * `n` of a fact is backed by citation `n`; where that lockstep does not hold
 * the popover shows nothing rather than the wrong line.
 *
 * A row's `siteLine` is a 1-based graph line inside the *focused symbol's*
 * file, and a row's `target` is where clicking it navigates, which is usually a
 * different file. Conflating the two is the bug this separation exists to
 * prevent: a call to `validateUser` on line 24 of `userService.ts` is declared
 * on line 19 of `validate.ts`, and both numbers are true about different files.
 */

import type { SymbolRef } from '../core/focus-protocol';
import { toEditorRange } from '../core/positions';
import type {
    CallSite,
    Evidence,
    Fact,
    KnowledgeState,
    SemanticIR
} from '../core/semantic-ir';
import { runtimeCitationsFor } from '../core/trace-protocol';
import type { ImportsGroup } from '../pseudocode/imports-group';
import {
    IMPORTS_SECTION_EMPTY,
    IMPORTS_SOURCE_NOTE,
    IMPORTS_SOURCE_UNREAD
} from '../pseudocode/pseudocode-strings';

import {
    BADGE_CONSTRUCTION,
    BADGE_CONSTRUCTION_TOOLTIP,
    BADGE_TEST,
    BADGE_TEST_TOOLTIP,
    CALLERS_EMPTY,
    EFFECTS_NOT_IN_INDEX,
    EFFECTS_HTTP_LABEL,
    EFFECTS_ROUTES_LABEL,
    EFFECTS_WRITES_LABEL,
    ENV_READS_LABEL,
    ERRORS_EMPTY,
    ERRORS_HANDLING_NOT_VISIBLE,
    PURPOSE_INFERRED_NOTE,
    RISKS_INFERRED_NOTE,
    RUNTIME_SECTION_EMPTY,
    RUNTIME_SECTION_NOTE,
    RUNTIME_UNEXPECTED_BADGE,
    RUNTIME_UNEXPECTED_TOOLTIP,
    SECTION_CALLERS,
    SECTION_EFFECTS,
    SECTION_ERRORS,
    SECTION_IMPORTS,
    SECTION_PURPOSE,
    SECTION_RISKS,
    SECTION_RUNTIME,
    SECTION_STATE,
    SECTION_STEPS,
    SECTION_TESTS,
    STATE_EMPTY,
    STEPS_EMPTY,
    SUBJECT_CALLERS,
    SUBJECT_EFFECTS,
    SUBJECT_ERRORS,
    SUBJECT_IMPORTS,
    SUBJECT_PURPOSE,
    SUBJECT_RISKS,
    SUBJECT_RUNTIME,
    SUBJECT_STATE,
    SUBJECT_STEPS,
    SUBJECT_TESTS,
    TESTS_EMPTY_INFERRED,
    TYPE_REFS_LABEL,
    WRITES_LABEL,
    runtimeObservedCount,
    runtimeRowCount,
    testCallerCount
} from './strings';

/** Strategy the provider stamps on a call whose target is a class. */
export const CONSTRUCTION_STRATEGY = 'construction';

/** A small word rendered beside a row's label. */
export interface TwinBadge {
    text: string;
    tooltip: string;
}

/**
 * The three semantic groupings the technical depths lead with.
 *
 * Not a fourth taxonomy over the sections: a block is a way of *drawing* a
 * section that answers one of the three questions a reader scans for, which is
 * what a symbol holds, how it fails, and what it touches outside itself. The
 * sections keep their names, their facets and their citations; the block only
 * says which of the three questions they belong to, so the renderer can put an
 * eyebrow over them and a reader can find the failure modes without reading
 * seven headings.
 *
 * A section without a block is not lesser: purpose, steps, callers and tests are
 * about the symbol itself rather than about what it reaches, and forcing them
 * into one of three buckets would be a taxonomy invented for the sake of tidy
 * rendering.
 */
export type TwinBlock = 'data' | 'errors' | 'effects';

/** One line inside a section. */
export interface TwinRow {
    /** Stable within a render, used as the React key and as the popover's anchor id. */
    id: string;
    /** Optional heading this row belongs under, for sections that hold two lists. */
    group?: string;
    label: string;
    /** Secondary text: where the row's target lives, as `file:line`. */
    detail?: string;
    badge?: TwinBadge;
    /** Where activating the row navigates, when there is somewhere to go. */
    target?: SymbolRef;
    /**
     * 1-based graph line of this row's site inside the focused symbol's own
     * file. Only steps have one; it is what the caret is matched against.
     */
    siteLine?: number;
    /**
     * Address of this row's citation inside the IR. Empty for a row that makes
     * no claim about the code, which is how the renderer knows not to offer an
     * evidence button that would lead nowhere.
     */
    factPath: string;
    /**
     * How sure the resolver was, 0 to 1, when it recorded a figure at all.
     *
     * Filled in only at the densest depth. A number rather than a formatted
     * string, so a consumer that wants to sort or threshold on it can, and so
     * the renderer can give it an element of its own instead of burying it in
     * a detail line.
     */
    confidence?: number;
    /**
     * The certainty of this row, worded.
     *
     * Present at the densest depth on every row that makes a claim, whether or
     * not a figure was recorded: a blank where a reviewer expects a number is
     * an unanswered question, and "there is no score because nothing was
     * guessed" is a different answer from "nobody scored it".
     */
    confidenceLabel?: string;
    /** One sentence explaining the label above, shown on hover. */
    confidenceNote?: string;
    /**
     * Provenance appended after the row's own text at the densest depth: which
     * heuristic resolved it and how many citations back it.
     */
    extras?: string[];
    /**
     * Draw this row as a chip rather than as a line.
     *
     * A presentation hint and nothing more: the row carries the same label, the
     * same target and the same citation either way. It exists because a
     * configuration key and a type name are short, unordered and read as a set,
     * where a call site is long, ordered and read as a sequence. Rendering the
     * first as a column of full-width rows is what made the state section the
     * least legible block in the panel.
     */
    display?: 'chip';
}

/** One block of the twin. */
export interface TwinSection {
    /** Slug used in the section's test id, for example `steps`. */
    name: string;
    title: string;
    /** How the absence sentence names this section's content. */
    subject: string;
    state: KnowledgeState;
    /** Address of the section's own citations inside the IR. */
    factPath: string;
    /**
     * Which of the three semantic groupings this section answers, if any.
     *
     * Purely a rendering grouping; see {@link TwinBlock}.
     */
    block?: TwinBlock;
    /** Codicon class drawn beside the heading. Never the only carrier of meaning. */
    icon?: string;
    /** Single paragraph sections put their text here instead of in rows. */
    text?: string;
    rows: TwinRow[];
    /**
     * One sentence about what the index cannot say here, rendered under the rows
     * rather than instead of them.
     *
     * Distinct from `emptyText`, which stands in *for* an absent list, and from
     * `stateNote`, which explains the marker. This is for a section that has
     * facts and a hole beside them: the error paths are recorded and where they
     * are caught is not, and a reader looking at a list of raised types has to
     * be told that the missing half is missing rather than absent.
     */
    note?: string;
    /** What to say when there are no rows. Never blank. */
    emptyText: string;
    /**
     * Provenance sentence for the state marker, when the generic one would be
     * wrong. Sections whose content CodeAtlas derives itself set this; every
     * other section takes the sentence that matches its knowledge state.
     */
    stateNote?: string;
    /**
     * How many rows there are, already worded, shown beside the heading at the
     * densest depth. Absent at every other depth: a count next to a heading is
     * useful to someone auditing a list and noise to someone reading one.
     */
    countLabel?: string;
    /** True when the section is showing something the index actually returned. */
    populated: boolean;
}

/** Severity order used to collapse two facts into one section state. */
const STATE_RANK: Record<KnowledgeState, number> = {
    known: 0,
    inferred: 1,
    ambiguous: 2,
    unsupported: 3,
    notIndexed: 4,
    unknown: 5
};

/**
 * The least trustworthy of several states.
 *
 * A section that shows one family read from the index and one family nobody
 * could answer is not a `known` section, and saying so would launder the gap.
 */
export function worstState(states: KnowledgeState[]): KnowledgeState {
    return states.reduce<KnowledgeState>(
        (worst, state) => (STATE_RANK[state] > STATE_RANK[worst] ? state : worst),
        'known'
    );
}

/** Last path segment of a URI, for `file:line` captions. */
export function displayFile(uri: string | undefined): string | undefined {
    if (!uri) {
        return undefined;
    }
    const path = uri.split('?')[0].split('#')[0];
    const segments = path.split('/');
    return decodeURIComponent(segments[segments.length - 1] || path);
}

/** `validate.ts:19`, or just the file when the line is unrecorded. */
export function locationLabel(uri: string | undefined, line: number | undefined): string | undefined {
    const file = displayFile(uri);
    if (file === undefined) {
        return undefined;
    }
    return line === undefined ? file : `${file}:${line}`;
}

/**
 * A navigable reference to one line.
 *
 * Deliberately carries no `nodeId`: this is a place in a file, not a resolved
 * graph node, and claiming otherwise would make the focus pipeline report an
 * index-backed focus for something the index never resolved.
 */
export function refAt(
    uri: string,
    oneBasedLine: number | undefined,
    name: string,
    qualifiedName?: string
): SymbolRef {
    const range = toEditorRange(oneBasedLine ?? 1, oneBasedLine ?? 1);
    return {
        name,
        qualifiedName,
        kind: 'unknown',
        uri,
        range,
        selectionRange: { start: range.start, end: range.start }
    };
}

/** The location one citation points at, when it recorded one. */
export function evidenceTarget(entry: Evidence, name: string): SymbolRef | undefined {
    if (!entry.file) {
        return undefined;
    }
    return refAt(entry.file, entry.range?.startLine, name);
}

/**
 * Citations for one fact, or for one row of it, mirroring the backend's grammar.
 *
 * A row of a call fact can carry a second kind of citation. The index's own
 * evidence array stays one entry per row, because the row addressing rests on
 * that; what an imported recording adds is appended after it, by the one shared
 * matcher both sides use, and always says which of the two it is. The graph edge
 * comes first: what the analyzer read is the claim, and what somebody watched
 * happen is corroboration of it.
 */
export function evidenceFor(ir: SemanticIR, factPath: string): Evidence[] {
    const match = /^([A-Za-z]+)(?:\[(\d+)\])?$/.exec(factPath);
    if (!match) {
        return [];
    }
    const fact = (ir as unknown as Record<string, Fact<unknown> | undefined>)[match[1]];
    if (!fact) {
        return [];
    }
    if (match[2] === undefined) {
        return [...fact.evidence];
    }
    // Only trust the row index when the two arrays line up: a mislabelled
    // citation is worse than no citation.
    if (!Array.isArray(fact.value) || fact.value.length !== fact.evidence.length) {
        return [];
    }
    const entry = fact.evidence[Number(match[2])];
    const observed = runtimeCitationsFor(ir, factPath);
    if (entry === undefined) {
        return observed;
    }
    return observed.length === 0 ? [entry] : [entry, ...observed];
}

/** Where following one step leads: the declaration of its target, not its call site. */
export function stepTarget(call: CallSite): SymbolRef | undefined {
    if (!call.targetFile) {
        return undefined;
    }
    return refAt(call.targetFile, call.targetLine ?? call.line, call.targetName, call.targetQualifiedName);
}

// ---------------------------------------------------------------------------

function stepsSection(ir: SemanticIR): TwinSection {
    const rows: TwinRow[] = ir.steps.value.map((call, index) => ({
        id: `step-${index}`,
        label: call.targetName,
        detail: locationLabel(call.targetFile, call.targetLine ?? call.line),
        badge: call.strategy === CONSTRUCTION_STRATEGY
            ? { text: BADGE_CONSTRUCTION, tooltip: BADGE_CONSTRUCTION_TOOLTIP }
            : undefined,
        target: stepTarget(call),
        siteLine: call.line,
        factPath: `steps[${index}]`
    }));
    return {
        name: 'steps',
        title: SECTION_STEPS,
        subject: SUBJECT_STEPS,
        state: ir.steps.state,
        factPath: 'steps',
        rows,
        emptyText: STEPS_EMPTY,
        populated: rows.length > 0
    };
}

function callersSection(ir: SemanticIR): TwinSection {
    const rows: TwinRow[] = ir.calledBy.value.map((caller, index) => ({
        id: `caller-${index}`,
        label: caller.name,
        detail: locationLabel(caller.file, caller.line),
        badge: caller.isTest ? { text: BADGE_TEST, tooltip: BADGE_TEST_TOOLTIP } : undefined,
        target: caller.file ? refAt(caller.file, caller.line, caller.name, caller.qualifiedName) : undefined,
        factPath: `calledBy[${index}]`
    }));
    return {
        name: 'callers',
        title: SECTION_CALLERS,
        subject: SUBJECT_CALLERS,
        state: ir.calledBy.state,
        factPath: 'calledBy',
        rows,
        emptyText: CALLERS_EMPTY,
        populated: rows.length > 0
    };
}

/**
 * The DATA block: what the symbol reads, what it writes and what shapes it
 * names.
 *
 * All three families are drawn as chips, because they are a set rather than a
 * sequence: nothing about `DB_URL` coming before `User` is a fact about the
 * code, and a column of full-width rows implied an order that was never there.
 *
 * Writes are folded in without changing the section's knowledge state. The
 * 0.9.0 engine records no write relation at all, so the family is `unsupported`
 * and empty; letting that decide the heading's marker would put "the index
 * cannot answer this" over a list of environment values the index answered
 * perfectly well. The rows are shown when there are rows, and the marker keeps
 * describing the families that actually produced them.
 */
function dataSection(ir: SemanticIR): TwinSection {
    const typeRefs = ir.typeRefs;
    const envRows: TwinRow[] = ir.reads.value.map((entry, index) => ({
        id: `env-${index}`,
        group: ENV_READS_LABEL,
        label: entry.name,
        detail: locationLabel(entry.file, entry.line),
        target: entry.file ? refAt(entry.file, entry.line, entry.name, entry.qualifiedName) : undefined,
        factPath: `reads[${index}]`,
        display: 'chip'
    }));
    const writeRows: TwinRow[] = ir.writes.value.map((entry, index) => ({
        id: `write-${index}`,
        group: WRITES_LABEL,
        label: entry.name,
        detail: locationLabel(entry.file, entry.line),
        target: entry.file ? refAt(entry.file, entry.line, entry.name, entry.qualifiedName) : undefined,
        factPath: `writes[${index}]`,
        display: 'chip'
    }));
    const typeRows: TwinRow[] = (typeRefs?.value ?? []).map((entry, index) => ({
        id: `type-${index}`,
        group: TYPE_REFS_LABEL,
        label: entry.name,
        detail: locationLabel(entry.file, entry.line),
        target: entry.file ? refAt(entry.file, entry.line, entry.name, entry.qualifiedName) : undefined,
        factPath: `typeRefs[${index}]`,
        display: 'chip'
    }));
    const rows = [...envRows, ...writeRows, ...typeRows];
    return {
        name: 'state',
        title: SECTION_STATE,
        subject: SUBJECT_STATE,
        block: 'data',
        state: worstState([ir.reads.state, typeRefs?.state ?? 'unsupported']),
        factPath: 'reads',
        rows,
        emptyText: STATE_EMPTY,
        populated: rows.length > 0
    };
}

/**
 * The DATA block's second half: what the file around this symbol pulls in.
 *
 * The only section in the panel that is about the file rather than about the
 * symbol, and it is here because that is where a reader looks for it: the
 * imports and the environment values are the same question asked twice, which
 * is what this code has been handed from outside itself.
 *
 * Drawn as chips for the same reason the reads are: a set, not a sequence.
 * Nothing about `insert` coming before `query` in an import statement is a fact
 * about the code.
 *
 * The state is `inferred` and says so, and that is not modesty. The dependency
 * is the index speaking, the statement and the name are CodeAtlas reading the
 * file, and whether a name is used is a rule applied to one symbol's recorded
 * facts. Calling any of that `known` would put the strength of a graph edge
 * behind a judgement the graph did not make.
 *
 * Built from the group rather than from the IR, because the imports are not in
 * the IR: they are a file-level answer, fetched once per file, and folding them
 * into a per-symbol document would fetch the same list once per function a
 * reader walks through.
 */
export function importsSection(group: ImportsGroup): TwinSection {
    const rows: TwinRow[] = group.entries.map(entry => ({
        id: entry.id,
        label: entry.label,
        detail: entry.module,
        badge: { text: entry.marker, tooltip: entry.note },
        target: entry.sourceRef === undefined
            ? undefined
            : refAt(entry.sourceRef.uri, entry.sourceRef.line, entry.label),
        factPath: entry.factPath,
        display: 'chip'
    }));
    const notes = [group.cappedNote, group.sourceRead ? undefined : IMPORTS_SOURCE_UNREAD, IMPORTS_SOURCE_NOTE]
        .filter((note): note is string => note !== undefined);
    return {
        name: 'imports',
        title: SECTION_IMPORTS,
        subject: SUBJECT_IMPORTS,
        block: 'data',
        // A rule over recorded facts, exactly like the risks section. The note
        // beside the marker says which part came from where.
        state: 'inferred',
        stateNote: IMPORTS_SOURCE_NOTE,
        factPath: '',
        text: rows.length > 0 ? group.tally : undefined,
        rows,
        note: rows.length > 0 ? notes.join(' ') : undefined,
        emptyText: IMPORTS_SECTION_EMPTY,
        populated: rows.length > 0
    };
}

/**
 * The EFFECTS block: what crosses the process boundary.
 *
 * Routes exposed, calls that leave the machine, and writes to something that
 * outlives the process. The 0.9.0 engine records none of the three, so at the
 * moment this is a heading and one honest sentence naming what would be in it.
 * It is rendered anyway rather than omitted, because a reader deciding whether a
 * function is safe to change needs to know that "no effects listed" means the
 * index does not look for them, not that there are none.
 */
function effectsSection(ir: SemanticIR): TwinSection {
    const groups: Readonly<Record<string, string>> = {
        'exposes-route': EFFECTS_ROUTES_LABEL,
        'http-call': EFFECTS_HTTP_LABEL,
        'io-write': EFFECTS_WRITES_LABEL
    };
    const rows: TwinRow[] = ir.externalEffects.value.map((effect, index) => ({
        id: `effect-${index}`,
        group: groups[effect.kind] ?? effect.kind,
        label: effect.detail,
        factPath: `externalEffects[${index}]`,
        display: 'chip'
    }));
    return {
        name: 'effects',
        title: SECTION_EFFECTS,
        subject: SUBJECT_EFFECTS,
        block: 'effects',
        state: ir.externalEffects.state,
        // No citations to offer while the family is unsupported: an evidence
        // button over a claim CodeAtlas has not made leads nowhere.
        factPath: rows.length > 0 ? 'externalEffects' : '',
        rows,
        emptyText: EFFECTS_NOT_IN_INDEX,
        populated: rows.length > 0
    };
}

function errorsSection(ir: SemanticIR): TwinSection {
    const aligned = ir.throws.value.length === ir.throws.evidence.length;
    const rows: TwinRow[] = ir.throws.value.map((entry, index) => {
        // The 0.9.0 engine records no raise-site line, so the fact's own line is
        // a declaration-level fallback. The citation, however, points at the
        // error type's declaration, which is the one place worth opening.
        const citation = aligned ? ir.throws.evidence[index] : undefined;
        const target = citation
            ? evidenceTarget(citation, entry.type)
            : (entry.file ? refAt(entry.file, entry.line, entry.type) : undefined);
        return {
            id: `throw-${index}`,
            label: entry.type,
            detail: locationLabel(target?.uri ?? entry.file, citation?.range?.startLine ?? entry.line),
            target,
            factPath: `throws[${index}]`
        };
    });
    return {
        name: 'errors',
        title: SECTION_ERRORS,
        subject: SUBJECT_ERRORS,
        block: 'errors',
        state: ir.throws.state,
        factPath: 'throws',
        rows,
        // Only said when there is something to say it about. "Where these are
        // handled is not visible" over an empty list would be a sentence about
        // nothing, and the empty sentence already covers that case.
        note: rows.length > 0 ? ERRORS_HANDLING_NOT_VISIBLE : undefined,
        emptyText: ERRORS_EMPTY,
        populated: rows.length > 0
    };
}

function testsSection(ir: SemanticIR): TwinSection {
    const rows: TwinRow[] = ir.tests.value.map((test, index) => ({
        id: `test-${index}`,
        label: test.name,
        detail: locationLabel(test.file, test.line),
        target: test.file ? refAt(test.file, test.line, test.name) : undefined,
        factPath: `tests[${index}]`
    }));
    return {
        name: 'tests',
        title: SECTION_TESTS,
        subject: SUBJECT_TESTS,
        state: ir.tests.state,
        factPath: 'tests',
        text: rows.length > 0 ? testCallerCount(rows.length) : undefined,
        rows,
        // Never "not tested": this is a search through callers that found none.
        emptyText: TESTS_EMPTY_INFERRED,
        populated: rows.length > 0
    };
}

/**
 * The runtime facet, once a recording has been imported.
 *
 * `undefined` when the IR carries no `runtime` fact at all, which is how the
 * caller knows to fall back to the sentence naming the lens and how to fill it.
 * The distinction is the whole honesty of this section: an absent fact means
 * nobody imported a recording, and a present, empty one means a recording was
 * imported and never reached this symbol. Both get a sentence and they are not
 * the same sentence.
 *
 * Every row is one observed callee with the number of times it was seen, and
 * nothing else is inferred from it. A row that the index does not record as a
 * call wears a marker, because that disagreement is the strongest finding a
 * recording can produce and burying it among the others would waste it.
 */
export function runtimeSection(ir: SemanticIR): TwinSection | undefined {
    const fact = ir.runtime;
    if (fact === undefined) {
        return undefined;
    }
    const rows: TwinRow[] = fact.value.map((call, index) => ({
        id: `runtime-${index}`,
        label: call.targetName,
        detail: locationLabel(call.targetFile, call.line),
        badge: call.unexpected
            ? { text: RUNTIME_UNEXPECTED_BADGE, tooltip: RUNTIME_UNEXPECTED_TOOLTIP }
            : undefined,
        target: call.targetFile === undefined
            ? undefined
            : refAt(call.targetFile, call.line, call.targetName, call.targetQualifiedName),
        factPath: `runtime[${index}]`,
        extras: [runtimeRowCount(call.count)]
    }));
    const total = fact.value.reduce((sum, call) => sum + call.count, 0);
    return {
        name: 'runtime',
        title: SECTION_RUNTIME,
        subject: SUBJECT_RUNTIME,
        state: fact.state,
        // Never the generic state sentence: this section's content did not come
        // from the index, so an explanation phrased in terms of the index would
        // be describing the wrong source.
        stateNote: RUNTIME_SECTION_NOTE,
        factPath: 'runtime',
        text: rows.length > 0 ? runtimeObservedCount(rows.length, total) : undefined,
        rows,
        note: rows.length > 0 ? RUNTIME_SECTION_NOTE : undefined,
        emptyText: RUNTIME_SECTION_EMPTY,
        populated: rows.length > 0
    };
}

function risksSection(ir: SemanticIR): TwinSection {
    const rows: TwinRow[] = ir.risks.map(risk => ({
        id: `risk-${risk.id}`,
        label: risk.message,
        detail: risk.severity,
        // The citations for a risk are the callers that make the rule fire.
        factPath: 'calledBy'
    }));
    return {
        name: 'risks',
        title: SECTION_RISKS,
        subject: SUBJECT_RISKS,
        // Always inferred: a risk is a rule applied to facts, never a fact.
        state: 'inferred',
        stateNote: RISKS_INFERRED_NOTE,
        factPath: 'calledBy',
        rows,
        emptyText: '',
        populated: rows.length > 0
    };
}

/**
 * Every section the twin can render, in reading order, whether or not it holds
 * anything. A caller decides what to hide; nothing is dropped here, because an
 * omitted section is indistinguishable from a section with nothing in it and
 * that is the confusion this product exists to remove.
 *
 * The order is the order a reader asks: what is this for, what does it do, who
 * needs it, what does it hold, how does it fail, what does it touch outside
 * itself, what checks it, what should I watch. The three middle ones carry a
 * block marker so the renderer can group them; the order itself is unchanged by
 * that, because it was already the order those questions arrive in.
 */
export function buildSections(ir: SemanticIR): TwinSection[] {
    const purpose: TwinSection = {
        name: 'purpose',
        title: SECTION_PURPOSE,
        subject: SUBJECT_PURPOSE,
        state: ir.purpose.state,
        stateNote: PURPOSE_INFERRED_NOTE,
        factPath: 'purpose',
        text: ir.purpose.value,
        rows: [],
        emptyText: '',
        populated: ir.purpose.value.length > 0
    };
    return [
        purpose,
        stepsSection(ir),
        callersSection(ir),
        dataSection(ir),
        errorsSection(ir),
        effectsSection(ir),
        testsSection(ir),
        risksSection(ir)
    ];
}
