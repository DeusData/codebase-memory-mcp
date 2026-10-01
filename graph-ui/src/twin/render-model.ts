/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/twin/render-model.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * buildTwinViewModel mit allen vier Tiefen, visibleSections, MAX_PROSE_CHIPS,
 * MAX_GUIDED_FACTS, die Facetten-Zuordnung SECTION_FACETS, die beiden
 * Platzhalter-Sektionen und visibleTextOf.
 *
 * Aenderungen gegenueber dem Original: die Importpfade auf die ebenfalls
 * portierten Dateien dieses Projekts.
 *
 * Die in W2b deklarierte Auslassung ist beglichen: die Imports-Gruppe ("What it
 * pulls in", Original Z. 302-330, withImportsSection) ist reaktiviert am
 * 2026-08-29 mit dem Pseudocode-Port, W4c. Der Data-Block traegt die
 * Import-Antwort wieder, aus src/pseudocode/imports-group.ts.
 */

/**
 * One semantic IR, five readers looking at it. No DOM, no services, no JSX.
 *
 * The slider is the product's central claim, and since W13 it asks a different
 * question. It used to ask how much to say. It now asks who is reading, and the
 * five answers are the reader's own words: vibe coder, junior, medior, senior,
 * architect. The claim is the same and stronger: the same recorded facts,
 * *chosen* for the reader in front of them, with nothing invented on the way
 * down and nothing hidden on the way up. That only survives if the five
 * presentations are one function of the IR rather than five panels that happen
 * to be pointed at the same symbol, which is why this module exists and why it
 * is pure.
 *
 * The five, and the question each of them answers:
 *
 *  - **0 vibe coder**: what happens here, and why does it matter to you? One
 *    paragraph, clickable names, no list and no jargon it can avoid.
 *  - **1 junior**: in what order does it happen, and why? The calls in the
 *    order the body reaches them, with the connective between them, and a word
 *    explained the first time it is used.
 *  - **2 medior**: what is actually written here? The recorded facts with
 *    their jump targets and no groundwork. This one is the old technical depth,
 *    unchanged, down to the section order.
 *  - **3 senior**: what does this cost me? The failure paths, what is
 *    unchecked, who depends on it, and what a change here drags with it.
 *  - **4 architect**: what does this sit on, and where does our knowledge end?
 *    The ground under it, what carries it, what it owes, and the limits of the
 *    index as a first-class part of the answer rather than a footnote.
 *
 * Five rules run through everything below.
 *
 * **One reading of the IR, five shapes of it.** The medior level is
 * `buildSections` unchanged; the other four are derived from the same sections,
 * or from the same facts, and never from a second interpretation. A row that
 * says `validate.ts:19` for the medior cannot say `validate.ts:21` for the
 * architect, because there is only one place that decided.
 *
 * **The level changes what is said, never what is true.** The vibe coder's
 * level spends counts and consequences where the architect's spends names, line
 * numbers and confidences; neither is allowed to state something the other
 * would contradict. In particular the vibe coder's level carries no qualified
 * name and no confidence figure anywhere a reader can see one: a reader who has
 * asked for prose has not asked to be told that a resolution scored 0.55, and
 * showing it anyway is how a "simple" mode becomes a lying one.
 *
 * **No level is allowed to be empty, and no level is allowed to be silent.** A
 * block with nothing in it says so in that reader's own words. "No error path
 * was recorded here" is an answer to the senior's question; a blank space is
 * not, and it is indistinguishable from a level that does not work.
 *
 * **Facets subtract, they never add.** A facet that is off removes its section.
 * A facet that is on and has no answer yet renders the heading with the
 * sentence saying so. Both are honest; silently rendering nothing for a lens
 * the reader explicitly asked for is not.
 *
 * **Nothing here knows which preset is active.** The input is a
 * {@link ResolvedPresentation}: depth, facets, terminology, callouts. That is
 * deliberate. The moment a renderer can ask "am I in Learning" it starts
 * growing behaviour that no slider position can reproduce, and the slider stops
 * being the truth about what is on screen.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { CallSite, KnowledgeState, SemanticIR } from '../core/semantic-ir';
import {
    Facet
} from './presentation-profile';
import type {
    DepthLevel,
    ResolvedPresentation,
    TerminologyLevel
} from './presentation-profile';

import type { ReaderLine } from './reader-rewrite';
import { readerLines } from './reader-rewrite';
import {
    CHANGES_NOT_IN_TWIN,
    CONFIDENCE_EXACT,
    CONFIDENCE_EXACT_TOOLTIP,
    CONFIDENCE_SCORED_TOOLTIP,
    CONFIDENCE_UNSCORED,
    CONFIDENCE_UNSCORED_TOOLTIP,
    LENS_ANSWERED_ELSEWHERE_NOTE,
    PLAIN_SUBJECT_ERRORS,
    PLAIN_SUBJECT_TESTS,
    PROSE_CHIPS_LEAD,
    PROSE_GENERATED_NOTE,
    PROSE_TESTS_NONE,
    RUNTIME_NOT_IN_TWIN,
    SECTION_ICONS,
    SECTION_LABELS_PLAIN,
    SECTION_LABELS_TECHNICAL,
    SUBJECT_CALLERS,
    SUBJECT_CHANGES,
    SUBJECT_EFFECTS,
    SUBJECT_ERRORS,
    SUBJECT_PURPOSE,
    SUBJECT_RUNTIME,
    SUBJECT_STATE,
    SUBJECT_STEPS,
    SUBJECT_TESTS,
    citationCount,
    confidenceLabel,
    plainAbsence,
    plainKind,
    proseCalls,
    joinNames,
    proseLead,
    proseMoreParts,
    proseReads,
    proseTestsSome,
    proseThrows,
    rowCountLabel,
    strategyLabel,

    // --- W13: the five readers -------------------------------------------
    COST_EMPTY_DEPENDS,
    COST_EMPTY_FAILS,
    COST_EMPTY_MOVES,
    COST_EMPTY_UNTESTED,
    COST_LEAD_DEPENDS,
    COST_LEAD_FAILS,
    COST_LEAD_MOVES,
    COST_LEAD_UNTESTED,
    COST_TITLE_DEPENDS,
    COST_TITLE_FAILS,
    COST_TITLE_MOVES,
    COST_TITLE_UNTESTED,
    DEBT_ALLOC_IN_LOOP,
    DEBT_RECURSIVE,
    DEBT_SCAN_IN_LOOP,
    DEBT_UNGUARDED_RECURSION,
    GROUND_EMPTY_CARRIED,
    GROUND_EMPTY_DEBTS,
    GROUND_EMPTY_SITS_ON,
    GROUND_ENTRY_POINT,
    GROUND_EXPORTED,
    GROUND_LEAD_CARRIED,
    GROUND_LEAD_DEBTS,
    GROUND_LEAD_LIMITS,
    GROUND_LEAD_SITS_ON,
    GROUND_NOT_EXPORTED,
    GROUND_TITLE_CARRIED,
    GROUND_TITLE_DEBTS,
    GROUND_TITLE_SITS_ON,
    JUNIOR_NO_STEPS,
    JUNIOR_ORDER_LEAD,
    LEVEL_QUESTIONS,
    LIMIT_CALLS_DEDUPLICATED,
    LIMIT_NO_HANDLER_SITES,
    LIMIT_NO_ROUTES,
    LIMIT_NO_TEST_RELATION,
    STEP_CONNECTIVES,
    STEP_CONNECTIVE_LAST,
    TERM_EXPLANATIONS,
    absenceSentence,
    costWeight,
    debtBranches,
    debtLoopDepth,
    groundReach,
    juniorStep,
    juniorStepAt,
    limitGeneration,
    limitOfFamily
} from './strings';
import type { ImportsGroup } from '../pseudocode/imports-group';
import {
    buildSections,
    displayFile,
    evidenceFor,
    importsSection,
    locationLabel,
    refAt,
    runtimeSection,
    stepTarget
} from './twin-view-model';
import type { TwinRow, TwinSection } from './twin-view-model';

/**
 * How the twin's body is laid out for one reader.
 *
 * Five modes rather than a level number, because a widget rendering JSX should
 * not be doing arithmetic on a scale whose meaning lives here. The names are
 * about shape and not about the reader: `cost` is what the senior's level looks
 * like, and a second reader who wanted that shape would get the same word.
 */
export type TwinRenderMode = 'prose' | 'guided' | 'sections' | 'cost' | 'ground';

/** A clickable name in the narrative depth. Plain name only, never qualified. */
export interface TwinChip {
    label: string;
    target?: SymbolRef;
}

/** One line of the guided depth's short list. */
export interface TwinFactRow {
    id: string;
    /** Which family this came from, in the active terminology. */
    label: string;
    /** The plain name of the thing, exactly as the code spells it. */
    name: string;
    target?: SymbolRef;
    /** Address of this row's citation inside the IR. */
    factPath: string;
}

/** One call, in the order the body reaches it, with the word that joins it on. */
export interface TwinStepLine {
    id: string;
    /** 1-based position in the reading order. */
    order: number;
    /** The whole sentence, connective included. */
    text: string;
    target?: SymbolRef;
    factPath: string;
}

/** A word this level used, explained once. */
export interface TwinTerm {
    term: string;
    explanation: string;
}

/**
 * One question inside a level, with what the index answers to it.
 *
 * The same shape carries the senior's four costs and the architect's three
 * grounds, and that is deliberate: they are the same idea (a question, the
 * sentence that frames it, the rows that answer it, and what to say when there
 * are none) asked by two different people, and two near-identical types would
 * be two places to forget the empty sentence.
 */
export interface TwinReaderBlock {
    name: string;
    title: string;
    /** What this block is, in this reader's voice. Never blank. */
    lead: string;
    rows: TwinRow[];
    /** What it says when there are no rows. Never blank. */
    emptyText: string;
    /** How much work this block implies, worded. Absent when there is none. */
    weight?: string;
}

/** One thing this page cannot tell the architect, and why. */
export interface TwinLimit {
    id: string;
    text: string;
    /**
     * Where the limit comes from.
     *
     *  - `state`: a fact family this index did not answer for this symbol.
     *  - `engine`: something this engine does not record for anybody.
     *  - `generation`: the age of the build everything above was read from.
     */
    kind: 'state' | 'engine' | 'generation';
}

/** Everything the twin's body needs, already shaped for one reader. */
export interface TwinViewModel {
    mode: TwinRenderMode;
    depth: DepthLevel;
    terminology: TerminologyLevel;
    /**
     * The question this level answers, said out loud before it answers it.
     *
     * Present on every level, and it is the one element every level has: a
     * reader who moves the slider sees the question change before they have
     * read a word of the body.
     */
    question: string;
    /** The exact symbol and source span every level is answering about. */
    subject: string;
    /** Short cross-cutting facts that remain visible in every level shape. */
    context: string[];
    /** Lead prose. Empty where the body leads with rows instead. */
    paragraphs: string[];
    /** Clickable names, vibe coder only. Empty everywhere else. */
    chips: TwinChip[];
    /** Honest sentences about what is absent, in this level's own vocabulary. */
    notes: string[];
    /** The ordered calls with their connectives, junior only. */
    steps: TwinStepLine[];
    /** The words the junior level used, explained once each. Junior only. */
    terms: TwinTerm[];
    /** The short list the guided level used to lead with. Empty everywhere now. */
    facts: TwinFactRow[];
    /** The full sections, medior only. Empty everywhere else. */
    sections: TwinSection[];
    /** The senior's costs, or the architect's grounds. Empty on the other three. */
    blocks: TwinReaderBlock[];
    /** Where this index stops, architect only. */
    limits: TwinLimit[];
    /**
     * Every assembled sentence on this level, addressable by id.
     *
     * This is what the local model is allowed to reword and nothing else. Rows
     * are not in it on purpose: a row is a name, a file and a line as the index
     * spells them, and there is no rewording of `validate.ts:19` that is worth
     * the risk of one.
     */
    rewritable: ReaderLine[];
}

/**
 * How many callees the narrative depth names.
 *
 * Five is where a sentence stops being a sentence. Past that the list is
 * summarised, and the summary says how many were left out rather than trailing
 * off, because "and more" is the kind of vagueness this panel exists to remove.
 */
export const MAX_PROSE_CHIPS = 5;

/** How many rows the guided depth leads with, for the same reason. */
export const MAX_GUIDED_FACTS = 5;

/**
 * Which lenses can bring a section on screen.
 *
 * A section appears when any one of its facets is on. `steps` belongs to two of
 * them on purpose: it is both the narrative of what the symbol does, which is
 * Logic, and the list of what it calls, which is Calls. Making it belong to
 * only one would mean a reader who asked for Calls and got no list of calls.
 */
const SECTION_FACETS: Readonly<Record<string, readonly Facet[]>> = {
    purpose: [Facet.Logic],
    steps: [Facet.Logic, Facet.Calls],
    callers: [Facet.Calls],
    state: [Facet.Data],
    errors: [Facet.Errors],
    // What a symbol writes, exposes and reaches is data crossing a boundary, so
    // it follows the same lens as the data it holds. Giving effects a lens of
    // their own would mean a reader who asked for Data and got told what the
    // symbol reads but not what it writes.
    effects: [Facet.Data],
    tests: [Facet.Tests],
    // A risk is a rule over calls and failure modes, so it follows whichever of
    // the two the reader is looking through.
    risks: [Facet.Logic, Facet.Errors]
};

/**
 * The two lenses whose section is a heading and a sentence rather than facts.
 *
 * Runtime is only half a placeholder. When a recording has been imported the IR
 * carries a `runtime` fact and the section is real; when it has not, the sentence
 * below is what the reader gets, and it names the surface that does hold observed
 * calls and the command that puts them in the index. Keeping both cases in one
 * list is deliberate: the facet decides whether the section appears at all, and
 * that decision must not depend on whether anybody has imported anything.
 */
const PLACEHOLDER_SECTIONS: readonly { name: string; facet: Facet; subject: string; sentence: string }[] = [
    { name: 'runtime', facet: Facet.Runtime, subject: SUBJECT_RUNTIME, sentence: RUNTIME_NOT_IN_TWIN },
    { name: 'changes', facet: Facet.Changes, subject: SUBJECT_CHANGES, sentence: CHANGES_NOT_IN_TWIN }
];

/**
 * The whole body of the twin for one reader.
 *
 * Never throws and never returns nothing: an IR with every family empty still
 * produces a mode, a question, a paragraph or a set of headings, and the
 * sentences that explain the emptiness.
 */
export function buildTwinViewModel(ir: SemanticIR, presentation: ResolvedPresentation): TwinViewModel {
    switch (presentation.depth) {
        case 0:
            return narrative(ir, presentation);
        case 1:
            return junior(ir, presentation);
        case 3:
            return senior(ir, presentation);
        case 4:
            return architect(ir, presentation);
        default:
            return technical(ir, presentation);
    }
}

/**
 * The fields every level shares, so a new level cannot forget one.
 *
 * A builder that returned an object literal of its own would be one `terms: []`
 * away from an undefined array in a renderer, and the renderer would find out
 * about it in a browser rather than in a type error.
 */
function emptyModel(
    ir: SemanticIR,
    presentation: ResolvedPresentation,
    mode: TwinRenderMode,
): TwinViewModel {
    return {
        mode,
        depth: presentation.depth,
        terminology: presentation.terminology,
        question: LEVEL_QUESTIONS[presentation.depth] ?? '',
        subject: subjectLine(ir),
        context: contextLines(ir, presentation.facets),
        paragraphs: [],
        chips: [],
        notes: [],
        steps: [],
        terms: [],
        facts: [],
        sections: [],
        blocks: [],
        limits: [],
        rewritable: []
    };
}

// ---------------------------------------------------------------------------
// section selection, shared by every depth
// ---------------------------------------------------------------------------

/** True when the reader has asked for at least one of the lenses this section answers. */
function sectionVisible(name: string, facets: ReadonlySet<Facet>): boolean {
    const owners = SECTION_FACETS[name];
    // A section nobody claimed is shown: a new family should be visible by
    // default rather than invisible until somebody remembers to map it.
    return owners === undefined || owners.some(facet => facets.has(facet));
}

function labelsFor(terminology: TerminologyLevel): Readonly<Record<string, string>> {
    return terminology === 'plain' ? SECTION_LABELS_PLAIN : SECTION_LABELS_TECHNICAL;
}

/**
 * One placeholder section: a heading, and the sentence saying that this panel
 * does not answer the lens behind it and which surface does.
 *
 * The row carries an empty fact path, which is how the renderer knows there is
 * no citation to offer. An evidence button over a claim CodeAtlas has not made
 * would be an affordance that leads nowhere.
 */
function placeholderSection(
    spec: { name: string; subject: string; sentence: string },
    labels: Readonly<Record<string, string>>
): TwinSection {
    return {
        name: spec.name,
        title: labels[spec.name] ?? spec.name,
        subject: spec.subject,
        icon: SECTION_ICONS[spec.name],
        state: 'unsupported',
        stateNote: LENS_ANSWERED_ELSEWHERE_NOTE,
        factPath: '',
        rows: [{ id: `${spec.name}-placeholder`, label: spec.sentence, factPath: '' }],
        emptyText: spec.sentence,
        populated: false
    };
}

/**
 * The sections this presentation shows, titled in its terminology.
 *
 * A section with no rows and no sentence is dropped here rather than in the
 * widget: a heading over nothing is exactly the shape this panel avoids, and
 * deciding it in one place keeps the depths from disagreeing about it.
 */
export function visibleSections(ir: SemanticIR, presentation: ResolvedPresentation): TwinSection[] {
    const labels = labelsFor(presentation.terminology);
    const sections: TwinSection[] = buildSections(ir)
        .filter(section => sectionVisible(section.name, presentation.facets))
        .filter(section => section.populated || section.emptyText.length > 0)
        .map(section => ({
            ...section,
            title: labels[section.name] ?? section.title,
            icon: SECTION_ICONS[section.name]
        }));
    for (const spec of PLACEHOLDER_SECTIONS) {
        if (!presentation.facets.has(spec.facet)) {
            continue;
        }
        // A lens with something behind it renders what is behind it. The
        // placeholder is the fallback and not the default: a reader who has
        // imported a recording must not be told the lens is unbuilt.
        const answered = spec.name === 'runtime' ? runtimeSection(ir) : undefined;
        sections.push(answered === undefined
            ? placeholderSection(spec, labels)
            : { ...answered, title: labels[answered.name] ?? answered.title, icon: SECTION_ICONS[answered.name] });
    }
    return sections;
}

/**
 * The same body, with the file's imports folded into the DATA block.
 *
 * A second pass rather than an argument to {@link buildTwinViewModel}, and the
 * reason is that the imports are not in the IR. They are a file-level answer
 * that arrives on its own round trip, later than the document the rest of this
 * file is a function of, and threading an optional late arrival through four
 * depth builders would make every one of them a function of two things instead
 * of one. This way the depths stay pure functions of the IR and the panel adds
 * what it has when it has it.
 *
 * Three rules, all of them the same rules the sections above follow.
 *
 * **The lens decides.** The group answers "what has this code been handed from
 * outside itself", which is the Data lens, so a reader who turned Data off does
 * not get it. Same rule as `state` and `effects`.
 *
 * **Only where a list of names is an answer.** The vibe coder and the junior
 * spend their words on counts, order and consequences; a list of import names
 * for the vibe coder would be exactly the identifier dump that level exists to
 * remove. The other three each fold it into the block whose question it
 * answers, which is why this function has three arms rather than one.
 *
 * **Beside the state section, never instead of it.** What a symbol reads and
 * what its file pulls in are two answers to one question and are read together.
 */
export function withImportsSection(
    model: TwinViewModel,
    group: ImportsGroup | undefined,
    presentation: ResolvedPresentation
): TwinViewModel {
    if (group === undefined || !presentation.facets.has(Facet.Data)) {
        return model;
    }
    const labels = labelsFor(presentation.terminology);
    const built = importsSection(group);
    if (model.mode === 'ground') {
        /*
         * For the architect the imports are not a section beside the others.
         * They are half the answer to "what does this sit on": a name the file
         * pulls in is a dependency the index cannot see a call to, and putting
         * it in a list of its own would leave the ground block claiming a
         * shorter foundation than the file really has.
         */
        const at = model.blocks.findIndex((block) => block.name === 'sits-on');
        if (at < 0) {
            return model;
        }
        const rows = [
            ...model.blocks[at].rows,
            ...built.rows.map((row) => ({ ...row, id: `import-${row.id}`, group: built.title }))
        ];
        const blocks = [...model.blocks];
        blocks[at] = {
            ...blocks[at],
            rows,
            ...(rows.length > 0 ? { weight: rowCountLabel(rows.length) } : {})
        };
        return { ...model, blocks };
    }
    if (model.mode === 'cost') {
        /*
         * For the senior they belong to what a change drags with it, for the
         * same reason: a name the file pulls in moves when the file moves, and
         * the senior is the reader counting the places they have to look.
         */
        const at = model.blocks.findIndex((block) => block.name === 'moves');
        if (at < 0) {
            return model;
        }
        const rows = [
            ...model.blocks[at].rows,
            ...built.rows.map((row) => ({ ...row, id: `import-${row.id}`, group: built.title }))
        ];
        const blocks = [...model.blocks];
        blocks[at] = {
            ...blocks[at],
            rows,
            ...(rows.length > 0 ? { weight: costWeight(rows.length) } : {})
        };
        return { ...model, blocks };
    }
    if (model.mode !== 'sections') {
        return model;
    }
    const section: TwinSection = {
        ...built,
        title: labels[built.name] ?? built.title,
        icon: SECTION_ICONS[built.name]
    };
    const at = model.sections.findIndex(entry => entry.name === 'state');
    const sections = [...model.sections];
    sections.splice(at < 0 ? sections.length : at + 1, 0, section);
    return { ...model, sections, rewritable: sectionLines(sections) };
}

// ---------------------------------------------------------------------------
// level 2, medior: the recorded facts, as they are
// ---------------------------------------------------------------------------

/**
 * The assembled sentences of a set of sections, in reading order.
 *
 * A section's own paragraph and its empty sentence are the only two things on
 * this level that CodeAtlas wrote rather than read, so they are the only two a
 * rewrite may touch. The rows are the index's own spelling and stay out of it.
 */
function sectionLines(sections: readonly TwinSection[]): ReaderLine[] {
    return readerLines(sections.flatMap((section) => [
        { id: `section-${section.name}-text`, text: section.text ?? '' },
        { id: `section-${section.name}-empty`, text: section.rows.length === 0 ? section.emptyText : '' },
        { id: `section-${section.name}-note`, text: section.note ?? '' }
    ]));
}

function technical(ir: SemanticIR, presentation: ResolvedPresentation): TwinViewModel {
    const sections = visibleSections(ir, presentation);
    return {
        ...emptyModel(ir, presentation, 'sections'),
        sections,
        rewritable: sectionLines(sections)
    };
}

// ---------------------------------------------------------------------------
// what a reviewer wants appended to a row, wherever a row is shown to one
// ---------------------------------------------------------------------------

/**
 * The call site a row came from, when it came from one.
 *
 * Steps are the only family whose records carry a resolution confidence and a
 * strategy of their own; everything else has to fall back to its citation.
 */
function callFor(ir: SemanticIR, factPath: string): CallSite | undefined {
    const match = /^steps\[(\d+)\]$/.exec(factPath);
    return match ? ir.steps.value[Number(match[1])] : undefined;
}

/**
 * How certain one row is, worded, whether or not anyone put a number on it.
 *
 * The 0.9.0 engine scores nothing: it reads its call, caller and raise
 * relations straight off the graph, so a recorded confidence is absent because
 * there was never anything to score rather than because a field went missing.
 * Rendering a blank there would leave a reviewer unable to tell that apart from
 * a derived claim nobody scored, so both say which one they are.
 */
function certaintyOf(confidence: number | undefined, state: KnowledgeState): { label: string; note: string } {
    if (confidence !== undefined) {
        return { label: confidenceLabel(confidence), note: CONFIDENCE_SCORED_TOOLTIP };
    }
    return state === 'known'
        ? { label: CONFIDENCE_EXACT, note: CONFIDENCE_EXACT_TOOLTIP }
        : { label: CONFIDENCE_UNSCORED, note: CONFIDENCE_UNSCORED_TOOLTIP };
}

/**
 * One row, with what a reviewer needs appended to it.
 *
 * Confidence stays a number on the row as well as a label: the renderer puts it
 * in its own element so it can be found, checked and styled as the qualified
 * claim it is, and a consumer that wants to threshold on it does not have to
 * parse a sentence.
 */
function densifyRow(ir: SemanticIR, row: TwinRow, state: KnowledgeState): TwinRow {
    // A row with no fact path makes no claim about the code, so it has no
    // certainty to report and no citations to count.
    if (row.factPath.length === 0) {
        return row;
    }
    const citations = evidenceFor(ir, row.factPath);
    const call = callFor(ir, row.factPath);
    const confidence = call?.confidence ?? citations[0]?.confidence;
    const strategy = call?.strategy ?? citations[0]?.strategy;
    const certainty = certaintyOf(confidence, state);
    const extras: string[] = [];
    if (strategy !== undefined) {
        extras.push(strategyLabel(strategy));
    }
    if (citations.length > 0) {
        extras.push(citationCount(citations.length));
    }
    return {
        ...row,
        confidence,
        confidenceLabel: certainty.label,
        confidenceNote: certainty.note,
        extras: extras.length > 0 ? extras : undefined
    };
}

// ---------------------------------------------------------------------------
// level 1, junior: the order, the connective between the steps, and the words
// ---------------------------------------------------------------------------

/**
 * The lead paragraph, assembled from the clauses the active lenses allow.
 *
 * Each clause is gated by the facet that owns its fact, so a reader who turned
 * Data off is not told how many environment values there are. The alternative,
 * one fixed sentence over all three counts, would quietly reintroduce the facts
 * the facets were asked to remove.
 */
function overviewClauses(ir: SemanticIR, facets: ReadonlySet<Facet>): string[] {
    const clauses = [proseLead(ir.symbol.name, ir.symbol.kind)];
    const absent: string[] = [];
    if (facets.has(Facet.Logic) || facets.has(Facet.Calls)) {
        const callees = ir.steps.value.map((call) => call.targetName).filter((name) => name.length > 0);
        if (callees.length > 0) {
            clauses.push(proseCalls(ir.symbol.name, callees));
        } else {
            absent.push('outgoing call');
        }
    }
    if (facets.has(Facet.Data)) {
        const reads = ir.reads.value.map((entry) => entry.name).filter((name) => name.length > 0);
        if (reads.length > 0) {
            clauses.push(proseReads(ir.symbol.name, reads));
        } else if (sharedNames(ir).length === 0) {
            absent.push('environment read');
        }
    }
    if (facets.has(Facet.Errors)) {
        const errors = ir.throws.value.map((entry) => entry.type).filter((name) => name.length > 0);
        if (errors.length > 0) {
            clauses.push(proseThrows(ir.symbol.name, errors));
        } else {
            absent.push('stopping error');
        }
    }
    if (absent.length > 0) {
        clauses.push(`The index records no ${joinNames(absent)} for ${ir.symbol.name}.`);
    }
    return clauses;
}

/** The first factual sentence on every level, including the positive leaf case. */
function subjectLine(ir: SemanticIR): string {
    const lines = Math.max(1, ir.symbol.range.end.line - ir.symbol.range.start.line);
    const file = displayFile(ir.symbol.uri) ?? ir.symbol.uri;
    if (ir.steps.value.length === 0) {
        return `${ir.symbol.name} is a ${plainKind(ir.symbol.kind)} spanning ${lines} lines in ${file}. `
            + 'The index resolves no outgoing calls from this symbol.';
    }
    const start = ir.symbol.range.start.line + 1;
    const end = ir.symbol.range.end.line;
    return `${ir.symbol.name} is a ${plainKind(ir.symbol.kind)} spanning ${lines} lines in ${file}, `
        + `from line ${start} through line ${end}.`;
}

/** Type references whose declaration lies outside the focused symbol. */
function sharedNames(ir: SemanticIR): string[] {
    const start = ir.symbol.range.start.line + 1;
    const end = ir.symbol.range.end.line;
    const names = (ir.typeRefs?.value ?? [])
        .filter((entry) => entry.file !== ir.symbol.uri
            || entry.line === undefined
            || entry.line < start
            || entry.line > end)
        .map((entry) => entry.name)
        .filter((name) => name.length > 0);
    return [...new Set(names)];
}

/** Facts whose impact is useful before any level-specific shape begins. */
function contextLines(ir: SemanticIR, facets: ReadonlySet<Facet>): string[] {
    const lines: string[] = [];
    if (facets.has(Facet.Data)) {
        const names = sharedNames(ir);
        if (names.length > 0) {
            lines.push(`${ir.symbol.name} leans on shared names declared outside its own lines: ${joinNames(names)}.`);
        }
    }
    if (facets.has(Facet.Calls) && ir.calledBy.value.length > 0) {
        const callers = ir.calledBy.value.map((caller) => callerLabel(caller.name)).filter((name) => name.length > 0);
        lines.push(`${callers.length} callers reach ${ir.symbol.name}: ${joinNames(callers)}.`);
    }
    return lines;
}

/** Module callers are shown as plain labels, never as path-shaped jargon. */
function callerLabel(name: string): string {
    if (!name.includes('/') && !/\.(?:[cm]?[jt]sx?)$/.test(name)) {
        return name;
    }
    const base = name.split('/').filter((part) => part.length > 0).at(-1) ?? name;
    return base.replace(/\.(?:[cm]?[jt]sx?)$/, '').replace(/[._-]+/g, ' ');
}

/**
 * The breadth-first short list the guided level used to lead with.
 *
 * One row from each visible family before a second row from any of them. Not
 * built for any level since W13: the junior's question is about order, and a
 * breadth-first sample of five families is the one shape that answers every
 * question except that one. It is kept, exported and tested because it is the
 * right answer to "show me a little of everything", which is a question a
 * future surface will ask, and rebuilding it from the same three rules later
 * would be rebuilding a thing that already worked.
 */
export function topFacts(sections: TwinSection[], limit: number): TwinFactRow[] {
    const queues = sections.filter(section => section.rows.length > 0);
    const facts: TwinFactRow[] = [];
    for (let round = 0; facts.length < limit; round++) {
        let took = false;
        for (const section of queues) {
            const row = section.rows[round];
            if (row === undefined) {
                continue;
            }
            took = true;
            facts.push({
                id: `${section.name}-${row.id}`,
                label: section.title,
                name: row.label,
                target: row.target,
                factPath: row.factPath
            });
            if (facts.length === limit) {
                return facts;
            }
        }
        if (!took) {
            return facts;
        }
    }
    return facts;
}

/**
 * The sentences a family's emptiness deserves.
 *
 * A level that shows no sections shows no empty sentence either, and without
 * this "no test caller was found" would be visible for the medior and invisible
 * for the junior, which would make the slider hide a finding rather than choose
 * one.
 */
function emptyNotes(sections: TwinSection[]): string[] {
    return sections
        .filter(section => section.rows.length === 0 && section.emptyText.length > 0)
        .map(section => section.emptyText);
}

/**
 * The calls in the order the body reaches them, joined by a word.
 *
 * The connective is the whole point of this level and it is also the place
 * where a builder is most tempted to lie. "First", "then", "after that" are
 * claims about order, and order is exactly what the index records: the call
 * sites arrive in source order. "Because" and "so that" would be claims about
 * intent, and nothing in the index knows the intent, so no word here implies
 * one.
 *
 * Duplicate targets are kept. The vibe coder's chips drop them because a
 * sentence naming the same function twice reads badly; a numbered order that
 * dropped one would be a different order from the one the code runs.
 */
function juniorSteps(ir: SemanticIR): TwinStepLine[] {
    const calls = ir.steps.value;
    return calls.map((call, index) => {
        const last = index === calls.length - 1 && calls.length > 1;
        const connective = index === 0
            ? STEP_CONNECTIVES[0]
            : (last ? STEP_CONNECTIVE_LAST : STEP_CONNECTIVES[1 + ((index - 1) % (STEP_CONNECTIVES.length - 1))]);
        const where = locationLabel(call.targetFile, call.targetLine ?? call.line);
        return {
            id: `s${index}`,
            order: index + 1,
            text: `${juniorStep(connective, call.targetName)}${where === undefined ? '' : juniorStepAt(where)}.`,
            target: stepTarget(call),
            factPath: `steps[${index}]`
        };
    });
}

/**
 * The words this level used, explained once each, and only the ones it used.
 *
 * Tied to what is on screen rather than to a fixed vocabulary list: a glossary
 * that explains "entry point" on a symbol that is not one is a page of reading
 * in front of the answer, and it teaches the junior that the panel talks past
 * them.
 */
function juniorTerms(ir: SemanticIR, facets: ReadonlySet<Facet>): TwinTerm[] {
    const wanted: string[] = [];
    const add = (term: string): void => {
        if (TERM_EXPLANATIONS[term] !== undefined && !wanted.includes(term)) {
            wanted.push(term);
        }
    };
    if (ir.steps.value.length > 0 && (facets.has(Facet.Logic) || facets.has(Facet.Calls))) {
        add('call site');
    }
    if (facets.has(Facet.Calls) && ir.calledBy.value.length > 0) {
        add('caller');
    }
    if (facets.has(Facet.Errors) && ir.throws.value.length > 0) {
        add('raise');
    }
    if (facets.has(Facet.Data) && ir.reads.value.length > 0) {
        add('environment read');
    }
    if (facets.has(Facet.Tests) && ir.tests.value.length > 0) {
        add('test caller');
    }
    if (ir.complexity.state === 'known' && ir.complexity.value.isEntryPoint) {
        add('entry point');
    }
    return wanted.map((term) => ({ term, explanation: TERM_EXPLANATIONS[term] }));
}

function junior(ir: SemanticIR, presentation: ResolvedPresentation): TwinViewModel {
    const facets = presentation.facets;
    const sections = visibleSections(ir, presentation);
    const showSteps = facets.has(Facet.Logic) || facets.has(Facet.Calls);
    const steps = showSteps ? juniorSteps(ir) : [];
    const paragraphs = [overviewClauses(ir, facets).join(' ')];
    if (showSteps) {
        // The lead is only honest when there is an order to lead into, and the
        // sentence about there being none is honest only when the reader asked
        // for the lens that would have shown one.
        paragraphs.push(steps.length > 0 ? JUNIOR_ORDER_LEAD : JUNIOR_NO_STEPS);
    }
    const notes = emptyNotes(sections);
    return {
        ...emptyModel(ir, presentation, 'guided'),
        paragraphs,
        notes,
        steps,
        terms: juniorTerms(ir, facets),
        rewritable: readerLines([
            ...paragraphs.map((text, index) => ({ id: `p${index}`, text })),
            ...steps.map((step) => ({ id: step.id, text: step.text })),
            ...notes.map((text, index) => ({ id: `n${index}`, text }))
        ])
    };
}

// ---------------------------------------------------------------------------
// level 3, senior: what it costs
// ---------------------------------------------------------------------------

/**
 * One cost block, built from sections that are already there.
 *
 * `undefined` when the lens behind it is off, which is how a facet keeps
 * subtracting on this level too. An empty block is not the same thing: a lens
 * that is on and found nothing renders its heading and the sentence saying so.
 */
function costBlock(
    sections: readonly TwinSection[],
    name: string,
    title: string,
    lead: string,
    emptyText: string,
    from: readonly string[]
): TwinReaderBlock | undefined {
    const used = from
        .map((sectionName) => sections.find((section) => section.name === sectionName))
        .filter((section): section is TwinSection => section !== undefined);
    if (used.length === 0) {
        return undefined;
    }
    // Rows from more than one family keep the family name as their group, so a
    // reader can see that "what moves" is three questions answered together.
    const rows: TwinRow[] = used.flatMap((section) => section.rows.map((row) => ({
        ...row,
        id: `${section.name}-${row.id}`,
        group: used.length > 1 ? section.title : row.group
    })));
    return {
        name,
        title,
        lead,
        rows,
        emptyText,
        ...(rows.length > 0 ? { weight: costWeight(rows.length) } : {})
    };
}

function senior(ir: SemanticIR, presentation: ResolvedPresentation): TwinViewModel {
    const sections = visibleSections(ir, presentation);
    const blocks = [
        costBlock(sections, 'fails', COST_TITLE_FAILS, COST_LEAD_FAILS, COST_EMPTY_FAILS, ['errors']),
        costBlock(sections, 'unchecked', COST_TITLE_UNTESTED, COST_LEAD_UNTESTED, COST_EMPTY_UNTESTED, ['tests']),
        costBlock(sections, 'depends', COST_TITLE_DEPENDS, COST_LEAD_DEPENDS, COST_EMPTY_DEPENDS, ['callers']),
        costBlock(sections, 'moves', COST_TITLE_MOVES, COST_LEAD_MOVES, COST_EMPTY_MOVES,
            ['steps', 'state', 'imports', 'effects'])
    ].filter((block): block is TwinReaderBlock => block !== undefined);
    // The risks the medior sees as a section are consequences, so they are said
    // here as sentences beside the blocks rather than as a fourth list.
    const notes = sections
        .filter((section) => section.name === 'risks')
        .flatMap((section) => section.rows.map((row) => row.label));
    return {
        ...emptyModel(ir, presentation, 'cost'),
        notes,
        blocks,
        rewritable: readerLines([
            ...blocks.flatMap((block) => [
                { id: `block-${block.name}-lead`, text: block.lead },
                { id: `block-${block.name}-empty`, text: block.rows.length === 0 ? block.emptyText : '' }
            ]),
            ...notes.map((text, index) => ({ id: `n${index}`, text }))
        ])
    };
}

// ---------------------------------------------------------------------------
// level 4, architect: the ground, the carriers, the debts, and the edge
// ---------------------------------------------------------------------------

/**
 * The module a call lands in, as a reader would name it.
 *
 * The last two segments of the recorded path, which is enough to tell
 * `util/validate.ts` from `repo/validate.ts` and short enough to read in a
 * column. The whole recorded path is on the row's detail, so nothing is hidden,
 * only folded.
 */
function moduleLabel(uri: string): string {
    const path = uri.split('?')[0].split('#')[0];
    const segments = path.split('/').filter((part) => part.length > 0);
    return segments.slice(-2).join('/') || (displayFile(uri) ?? uri);
}

/**
 * What this symbol sits on: one row per module, with how many call sites reach
 * into it.
 *
 * A module reached from six places and a module reached once are two different
 * relationships, and a flat list of twenty call sites hides which is which.
 * That is the whole difference between this and the medior's step list: same
 * facts, grouped by the thing the architect is asking about.
 */
function groundRows(ir: SemanticIR): TwinRow[] {
    const byModule = new Map<string, { calls: CallSite[]; firstIndex: number }>();
    ir.steps.value.forEach((call, index) => {
        const key = call.targetFile;
        if (key === undefined || key.length === 0) {
            return;
        }
        const entry = byModule.get(key);
        if (entry === undefined) {
            byModule.set(key, { calls: [call], firstIndex: index });
        } else {
            entry.calls.push(call);
        }
    });
    return [...byModule.entries()].map(([uri, entry]) => {
        const certainty = certaintyOf(
            entry.calls.find((call) => call.confidence !== undefined)?.confidence,
            ir.steps.state
        );
        const strategy = entry.calls.find((call) => call.strategy !== undefined)?.strategy;
        return {
            id: `ground-${entry.firstIndex}`,
            label: moduleLabel(uri),
            detail: entry.calls.map((call) => call.targetName).join(', '),
            target: refAt(uri, entry.calls[0].targetLine ?? entry.calls[0].line, moduleLabel(uri)),
            factPath: `steps[${entry.firstIndex}]`,
            confidenceLabel: certainty.label,
            confidenceNote: certainty.note,
            extras: [
                groundReach(entry.calls.length),
                ...(strategy === undefined ? [] : [strategyLabel(strategy)])
            ]
        };
    });
}

/** What holds this symbol up: its callers, plus what the outside world can reach. */
function carrierRows(ir: SemanticIR, sections: readonly TwinSection[]): TwinRow[] {
    const callers = sections.find((section) => section.name === 'callers');
    const rows: TwinRow[] = (callers?.rows ?? []).map((row) => densifyRow(ir, row, callers?.state ?? 'unknown'));
    if (ir.complexity.state === 'known') {
        const flags = ir.complexity.value;
        rows.push({
            id: 'carrier-reach',
            label: flags.isEntryPoint
                ? GROUND_ENTRY_POINT
                : (flags.isExported ? GROUND_EXPORTED : GROUND_NOT_EXPORTED),
            factPath: 'complexity'
        });
    }
    return rows;
}

/** What this symbol owes: the rules that fired, and the shapes that cost. */
function debtRows(ir: SemanticIR, sections: readonly TwinSection[]): TwinRow[] {
    const risks = sections.find((section) => section.name === 'risks');
    const rows: TwinRow[] = (risks?.rows ?? []).map((row) => ({ ...row, id: `debt-${row.id}` }));
    // Structural flags are only said when somebody measured them. This engine
    // reports `complexity` as unsupported for TypeScript, and a zero read off an
    // unsupported family is not a zero, it is a blank.
    if (ir.complexity.state === 'known') {
        const flags = ir.complexity.value;
        const said: string[] = [];
        if (flags.unguardedRecursion) {
            said.push(DEBT_UNGUARDED_RECURSION);
        } else if (flags.recursive) {
            said.push(DEBT_RECURSIVE);
        }
        if (flags.linearScanInLoop) {
            said.push(DEBT_SCAN_IN_LOOP);
        }
        if (flags.allocInLoop) {
            said.push(DEBT_ALLOC_IN_LOOP);
        }
        if (flags.loopDepth > 1) {
            said.push(debtLoopDepth(flags.loopDepth));
        }
        if (flags.cyclomatic > 1) {
            said.push(debtBranches(flags.cyclomatic));
        }
        rows.push(...said.map((label, index) => ({
            id: `debt-flag-${index}`,
            label,
            factPath: 'complexity'
        })));
    }
    return rows;
}

/**
 * Where this index stops, as a list a reader can count.
 *
 * Three kinds, and the difference between them is the whole value of the block:
 * a family this build did not answer for this symbol is a gap somebody could
 * close by indexing again; something the engine does not record for anybody is a
 * gap nobody can close from here; and the generation is the age of every
 * sentence above it.
 */
function indexLimits(ir: SemanticIR, facets: ReadonlySet<Facet>): TwinLimit[] {
    const limits: TwinLimit[] = [];
    const families: { key: string; subject: string; state: KnowledgeState }[] = [
        { key: 'purpose', subject: SUBJECT_PURPOSE, state: ir.purpose.state },
        { key: 'steps', subject: SUBJECT_STEPS, state: ir.steps.state },
        { key: 'callers', subject: SUBJECT_CALLERS, state: ir.calledBy.state },
        { key: 'state', subject: SUBJECT_STATE, state: ir.reads.state },
        { key: 'errors', subject: SUBJECT_ERRORS, state: ir.throws.state },
        { key: 'effects', subject: SUBJECT_EFFECTS, state: ir.externalEffects.state },
        { key: 'tests', subject: SUBJECT_TESTS, state: ir.tests.state }
    ];
    for (const family of families) {
        const sentence = absenceSentence(family.state, family.subject);
        if (sentence.length > 0) {
            limits.push({ id: `limit-${family.key}`, kind: 'state', text: limitOfFamily(family.subject, sentence) });
        }
    }
    for (const [index, sentence] of [
        LIMIT_NO_TEST_RELATION,
        LIMIT_NO_HANDLER_SITES,
        LIMIT_NO_ROUTES,
        LIMIT_CALLS_DEDUPLICATED
    ].entries()) {
        limits.push({ id: `limit-engine-${index}`, kind: 'engine', text: sentence });
    }
    for (const spec of PLACEHOLDER_SECTIONS) {
        if (facets.has(spec.facet)) {
            limits.push({ id: `limit-lens-${spec.name}`, kind: 'engine', text: spec.sentence });
        }
    }
    limits.push({ id: 'limit-generation', kind: 'generation', text: limitGeneration(ir.generation) });
    return limits;
}

function architect(ir: SemanticIR, presentation: ResolvedPresentation): TwinViewModel {
    const facets = presentation.facets;
    const sections = visibleSections(ir, presentation);
    const blocks: TwinReaderBlock[] = [];
    if (facets.has(Facet.Logic) || facets.has(Facet.Calls)) {
        const rows = groundRows(ir);
        blocks.push({
            name: 'sits-on',
            title: GROUND_TITLE_SITS_ON,
            lead: GROUND_LEAD_SITS_ON,
            rows,
            emptyText: GROUND_EMPTY_SITS_ON,
            ...(rows.length > 0 ? { weight: rowCountLabel(rows.length) } : {})
        });
    }
    if (facets.has(Facet.Calls)) {
        const rows = carrierRows(ir, sections);
        blocks.push({
            name: 'carried-by',
            title: GROUND_TITLE_CARRIED,
            lead: GROUND_LEAD_CARRIED,
            rows,
            emptyText: GROUND_EMPTY_CARRIED,
            ...(rows.length > 0 ? { weight: rowCountLabel(rows.length) } : {})
        });
    }
    if (facets.has(Facet.Logic) || facets.has(Facet.Errors)) {
        const rows = debtRows(ir, sections);
        blocks.push({
            name: 'debts',
            title: GROUND_TITLE_DEBTS,
            lead: GROUND_LEAD_DEBTS,
            rows,
            emptyText: GROUND_EMPTY_DEBTS,
            ...(rows.length > 0 ? { weight: rowCountLabel(rows.length) } : {})
        });
    }
    const limits = indexLimits(ir, facets);
    return {
        ...emptyModel(ir, presentation, 'ground'),
        blocks,
        limits,
        rewritable: readerLines([
            ...blocks.flatMap((block) => [
                { id: `block-${block.name}-lead`, text: block.lead },
                { id: `block-${block.name}-empty`, text: block.rows.length === 0 ? block.emptyText : '' }
            ]),
            { id: 'limits-lead', text: GROUND_LEAD_LIMITS },
            ...limits.map((limit) => ({ id: limit.id, text: limit.text }))
        ])
    };
}

// ---------------------------------------------------------------------------
// level 0, vibe coder: prose, counts, and the handful of names worth clicking
// ---------------------------------------------------------------------------

/** Distinct callees in the order the body reaches them, capped. */
function proseChips(ir: SemanticIR, limit: number): { chips: TwinChip[]; hidden: number } {
    const seen = new Set<string>();
    const distinct: CallSite[] = [];
    for (const call of ir.steps.value) {
        const key = call.targetQualifiedName ?? call.targetName;
        if (key.length === 0 || seen.has(key)) {
            continue;
        }
        seen.add(key);
        distinct.push(call);
    }
    return {
        chips: distinct.slice(0, limit).map(call => ({ label: call.targetName, target: stepTarget(call) })),
        hidden: Math.max(0, distinct.length - limit)
    };
}

/**
 * One honest sentence about whether anything tests this, in plain words.
 *
 * Never "not tested". The engine records no test relation for TypeScript, so
 * this is a search through callers that found none, and the sentence says which
 * of the two it is.
 */
function testsNote(ir: SemanticIR): string {
    if (ir.tests.value.length > 0) {
        return proseTestsSome(ir.tests.value.length);
    }
    const absence = plainAbsence(ir.tests.state, PLAIN_SUBJECT_TESTS);
    return ir.tests.state === 'known' || ir.tests.state === 'inferred'
        ? PROSE_TESTS_NONE
        : absence;
}

/** The same, for failure: an empty list is only a finding once someone looked. */
function errorsNote(ir: SemanticIR): string {
    if (ir.throws.value.length > 0) {
        return '';
    }
    return plainAbsence(ir.throws.state, PLAIN_SUBJECT_ERRORS);
}

function narrative(ir: SemanticIR, presentation: ResolvedPresentation): TwinViewModel {
    const facets = presentation.facets;
    const paragraphs = [overviewClauses(ir, facets).join(' ')];
    const notes: string[] = [];
    // The paragraph above was assembled from counts, and a reader who cannot
    // see the sections cannot see that for themselves.
    if (facets.has(Facet.Logic)) {
        notes.push(PROSE_GENERATED_NOTE);
    }

    const showCallees = facets.has(Facet.Logic) || facets.has(Facet.Calls);
    const { chips, hidden } = showCallees ? proseChips(ir, MAX_PROSE_CHIPS) : { chips: [], hidden: 0 };
    if (chips.length > 0) {
        paragraphs.push(PROSE_CHIPS_LEAD);
    }
    if (hidden > 0) {
        notes.push(proseMoreParts(hidden));
    }
    if (facets.has(Facet.Errors)) {
        const note = errorsNote(ir);
        if (note.length > 0) {
            notes.push(note);
        }
    }
    if (facets.has(Facet.Tests)) {
        const note = testsNote(ir);
        if (note.length > 0) {
            notes.push(note);
        }
    }
    for (const spec of PLACEHOLDER_SECTIONS) {
        if (!facets.has(spec.facet)) {
            continue;
        }
        // The same rule the section list follows: a lens with an answer says its
        // answer, and only a lens with none says it has none. At this depth the
        // answer is a count and never a list of identifiers, which is what the
        // narrative depth is for.
        const answered = spec.name === 'runtime' ? runtimeSection(ir) : undefined;
        notes.push(answered === undefined
            ? spec.sentence
            : (answered.text ?? answered.emptyText));
    }
    return {
        ...emptyModel(ir, presentation, 'prose'),
        paragraphs,
        chips,
        notes,
        rewritable: readerLines([
            ...paragraphs.map((text, index) => ({ id: `p${index}`, text })),
            ...notes.map((text, index) => ({ id: `n${index}`, text }))
        ])
    };
}

/**
 * Everything this model puts in front of a reader, as one string.
 *
 * Used by the depth tests to assert what the narrative depth may not say. It is
 * deliberately the visible text and not a serialisation of the whole object: a
 * chip carries a navigation target whose qualified name is how the twin follows
 * it, and that name is metadata the reader never sees. Asserting over the
 * object would either fail on data that is never rendered or force the chips to
 * drop the identity that makes them clickable.
 */
export function visibleTextOf(model: TwinViewModel): string {
    const parts = [
        model.question,
        model.subject,
        ...model.context,
        ...model.paragraphs,
        ...model.notes,
        ...model.chips.map(chip => chip.label),
        ...model.steps.map(step => step.text),
        ...model.terms.flatMap(term => [term.term, term.explanation]),
        ...model.facts.flatMap(fact => [fact.label, fact.name]),
        ...model.limits.map(limit => limit.text)
    ];
    for (const block of model.blocks) {
        parts.push(block.title, block.lead, block.weight ?? '');
        if (block.rows.length === 0) {
            parts.push(block.emptyText);
        }
        for (const row of block.rows) {
            parts.push(
                row.group ?? '',
                row.label,
                row.detail ?? '',
                row.badge?.text ?? '',
                row.confidenceLabel ?? '',
                ...(row.extras ?? [])
            );
        }
    }
    for (const section of model.sections) {
        parts.push(
            section.title,
            section.text ?? '',
            section.stateNote ?? '',
            section.countLabel ?? '',
            // The sentence about the half of a section the index cannot see is
            // on screen beside the rows, so it is part of what the reader is
            // being told and belongs in what the depth tests assert over.
            section.note ?? ''
        );
        // Same rule the renderer follows: the absence sentence is on screen
        // only when there is nothing else in the section.
        if (section.rows.length === 0) {
            parts.push(section.emptyText);
        }
        for (const row of section.rows) {
            parts.push(row.label, row.detail ?? '', row.badge?.text ?? '', row.confidenceLabel ?? '', ...(row.extras ?? []));
        }
    }
    return parts.filter(part => part.length > 0).join('\n');
}
