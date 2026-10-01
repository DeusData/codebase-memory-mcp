/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/strings.ts
 * Gleicher Urheber (Bernhard Jackiewicz).
 *
 * Die Quelldatei ist 2639 Zeilen lang und traegt den Wortlaut ALLER Flaechen des
 * Referenzprojekts. Hier stehen die Exporte, die der Semantic Twin benutzt, und
 * die stehen WOERTLICH da, samt ihrer Begruendungen: der Unterschied zwischen
 * "keine Aufrufer" und "niemand hat nachgesehen" wird in diesem Produkt
 * ausschliesslich vom Wortlaut getragen, und eine gekuerzte Fassung waere eine
 * andere Aussage.
 *
 * Uebernommen ist genau das, was die portierten Module und das Panel dieses
 * Projekts lesen: die Ueberschriften und Subjekte der Sektionen, ihre
 * Leer-Saetze, die Block-Augenbrauen, die Marker der Wissenszustaende, die
 * Absenz-Saetze in beiden Vokabularen, die Prosa der Erzaehl-Tiefe, die
 * Beleg-Sprache, die dichten Provenienz-Bausteine, die Regler-Beschriftungen,
 * die Runtime-Sektion, der Verstehens-Streifen und die Wegweiser-Saetze.
 *
 * Weggelassen wurde der Wortlaut der Flaechen, die es hier nicht gibt: Atlas
 * Chat, die optionale KI-Verfeinerung samt Daten-Policy und Sende-Vorschau, die
 * Review-Checkliste als Panel, der Call Navigator, die Workspace Map, die
 * Impact-Ansicht, Onboarding und Touren, die Bug-Wizard-Diagramme, die
 * Konzept-Erklaerungen und die "Why are you here?"-Karten. Das ist keine
 * Auswahl nach Geschmack: zu jeder dieser Zeichenketten gehoert ein Panel, und
 * ein Satz ueber ein Panel, das dieses Projekt nicht hat, waere hier eine
 * Behauptung ueber eine Flaeche, die niemand sehen kann.
 *
 * Aenderungen an dem, was uebernommen wurde: eine, und sie ist begruendet.
 *
 * `CALLERS_EMPTY` weicht seit dem 2026-08-29 vom Referenz-Wortlaut ab. Das
 * unabhaengige Audit dieses Zyklus hat eine Fixture mit dynamischem Versand
 * ueber eine Registry gebaut und darin belegt, dass der Referenzsatz
 * ("Nothing in the indexed workspace calls this symbol.") falsch ist: die
 * beiden nur ueber die Registry erreichbaren Handler WERDEN im indizierten
 * Arbeitsbereich aufgerufen, nur eben nicht ueber eine CALLS-Kante, die der
 * Index fuehrt. Der Satz behauptete damit etwas ueber den Arbeitsbereich,
 * waehrend seine drei Nachbarn (STEPS_EMPTY, ERRORS_EMPTY, STATE_EMPTY) die
 * Luecke ausdruecklich dem Index zuschreiben. Eine wortgleiche Portierung
 * eines belegt falschen Satzes waere Portierungstreue gegen die
 * Ehrlichkeitsregel, und die Regel gewinnt. Der neue Wortlaut steht bei der
 * Konstante, mit derselben Begruendung.
 *
 * Sonst keine Aenderungen ausser den Importpfaden (Facet aus der ebenfalls
 * portierten presentation-profile.ts, CodeAtlasSymbolKind und KnowledgeState
 * aus src/core/).
 */

/**
 * Every sentence CodeAtlas says in a view, in one file.
 *
 * Three reasons this is not scattered across the widgets.
 *
 * The copy is the product. This IDE's claim is that it tells the truth about
 * what it knows, and the difference between "no callers" and "nobody looked"
 * is carried entirely by wording. Wording that lives next to the JSX gets
 * rewritten by whoever is fixing a layout bug; wording that lives here gets
 * read as a whole and stays consistent.
 *
 * The copy is reviewable. A reader can open this file and audit every claim
 * the product makes without reading a line of React.
 *
 * The copy is translatable later. Nothing here is assembled from fragments
 * mid-sentence: the parameterised strings are functions, so a translation can
 * reorder the parameters instead of being forced into English word order.
 */

import type { CodeAtlasSymbolKind } from '../core/focus-protocol';
import type { KnowledgeState } from '../core/semantic-ir';
import { Facet } from './presentation-profile';

/** `n thing` or `n things`, so a generated sentence never reads as machine output. */
export function countOf(count: number, singular: string, plural: string): string {
    return `${count} ${count === 1 ? singular : plural}`;
}

// ---------------------------------------------------------------------------
// Empty states, one per surface. Never a blank panel.
// ---------------------------------------------------------------------------

export const TWIN_EMPTY_MESSAGE = 'Place the cursor inside a function, method, or class.';
export const TWIN_EMPTY_HINT = 'CodeAtlas follows the caret and explains whatever it lands in.';

// ---------------------------------------------------------------------------
// Twin identity and provenance
// ---------------------------------------------------------------------------

/**
 * Shown instead of every fact section when the file is genuinely not in the
 * index. This sentence exists to make sure a reader never sees an empty
 * "Called by" list and concludes the symbol has no callers.
 */
export const TWIN_FILE_NOT_INDEXED = 'This file is not indexed yet.';

/** Says what would change the sentence above. */
export const TWIN_FILE_NOT_INDEXED_HINT =
    'CodeAtlas is still reading this workspace. Facts about this symbol appear as soon as it has.';

/** Shown while the IR for a symbol is being built. */
export const TWIN_LOADING = 'Reading what the index knows about this symbol.';

/** Shown when the IR call itself failed, which is a different failure from an empty fact. */
export const TWIN_LOAD_FAILED = 'CodeAtlas could not reach the analysis backend for this symbol.';

/**
 * What the flow switch in the twin head says, in both states.
 *
 * Until W8b it said `[-]` and `[+]`. The reader tested this surface on
 * 2026-08-29 and did not understand the signs; the sentence that explained them
 * lived in a native tooltip, which is exactly the thing that cycle took out of
 * this product. A word costs four characters more and answers the question
 * before it is asked.
 */
export function twinFoldLabel(open: boolean): string {
    return open ? 'collapse flow' : 'open flow';
}

/**
 * The mark at the bottom edge of the twin, when its content is longer than its
 * place.
 *
 * The user's screenshot of 2026-08-29 showed the line "Order exact 1 citation"
 * ending half behind the GALAXY header, with nothing saying there was more. The
 * third readability rule of W9 asks a box that cuts a line in half to SAY so;
 * the twin was the one scrolling box in the right column that did not. It reads
 * like the legend's mark, because a second wording for the same fact would be a
 * second idiom.
 */
export const TWIN_MORE_BELOW = 'more below';
export const TWIN_MORE_ABOVE = 'more above';

// ---------------------------------------------------------------------------
// Section headings and the subjects their absence sentences are about
// ---------------------------------------------------------------------------

export const SECTION_PURPOSE = 'Purpose';
export const SECTION_STEPS = 'Steps';
export const SECTION_CALLERS = 'Called by';
export const SECTION_STATE = 'State and config';
export const SECTION_ERRORS = 'Error paths';
export const SECTION_TESTS = 'Tests';
export const SECTION_RISKS = 'Risks';

/**
 * Two sections that exist as headings without facts under them.
 *
 * They are rendered only when the reader has asked for the lens, and they say
 * plainly that this panel does not answer it and which surface does. A lens that
 * silently renders nothing would read as "there is nothing to see", which is a
 * claim about the code rather than about the panel.
 */
export const SECTION_RUNTIME = 'Runtime behaviour';
export const SECTION_CHANGES = 'Recent changes';

/**
 * What a symbol does to the world outside its own process.
 *
 * A heading before it is facts, like the two above, and for the same reason: at
 * 0.9.0 the engine records no route, no outbound call and no write, so the
 * section says that rather than rendering an empty list that would read as "this
 * symbol touches nothing".
 */
export const SECTION_EFFECTS = 'Effects';

/**
 * What the file around this symbol pulls in.
 *
 * A section about the file rather than about the symbol, and the only one in the
 * panel that is. It is here because a reader meeting a function for the first
 * time reads its imports before its body, and because the honest half of that
 * list is the part CodeAtlas can add: which of those imports this particular
 * symbol demonstrably touches, and which it does not.
 */
export const SECTION_IMPORTS = 'Pulls in';

export const SUBJECT_PURPOSE = 'The purpose of this symbol';
export const SUBJECT_STEPS = 'What this symbol does, step by step,';
export const SUBJECT_CALLERS = 'The list of callers';
export const SUBJECT_STATE = 'The configuration this symbol reads';
export const SUBJECT_ERRORS = 'The errors this symbol can raise';
export const SUBJECT_TESTS = 'The tests covering this symbol';
export const SUBJECT_RISKS = 'What is risky about this symbol';
export const SUBJECT_RUNTIME = 'What this symbol does while it runs';
export const SUBJECT_CHANGES = 'What changed in this symbol recently';
export const SUBJECT_EFFECTS = 'What this symbol does outside its own process';
export const SUBJECT_IMPORTS = 'What the file around this symbol pulls in';

/**
 * Two sections carry their own provenance sentence instead of the generic one.
 *
 * The generic `inferred` sentence names indexed callers, because that is where
 * every inference the provider makes comes from. These two are inferred by
 * CodeAtlas itself rather than by the provider, and telling a reader that a
 * generated summary was "derived from indexed callers" would be a wrong answer
 * to a question they were right to ask.
 */
export const PURPOSE_INFERRED_NOTE =
    'This sentence is generated from the calls, environment values and error types the index recorded. It is not a docstring and nobody wrote it.';

export const RISKS_INFERRED_NOTE =
    'These are rules applied to what the index recorded, not findings from running the code.';

// ---------------------------------------------------------------------------
// Section bodies
// ---------------------------------------------------------------------------

export const STEPS_EMPTY = 'This symbol calls nothing that the index resolved.';

/**
 * What the caller section says when the index resolved no caller.
 *
 * The sentence names the INDEX as the source of the gap and not the workspace,
 * and that is the whole difference between it and the wording it replaced. The
 * reference project says "Nothing in the indexed workspace calls this symbol.",
 * which is a claim about the code: it is false for every symbol reached through
 * a registry, a dispatch table or any other indirection the engine does not
 * record as a CALLS edge, and the audit of 2026-08-29 demonstrated exactly that
 * on a prepared fixture. "No caller of this symbol appears in what the index
 * resolved" is true in both cases, and it leaves the reader with the right next
 * question, which is what the index can see, not whether the code is dead.
 *
 * It now reads like its three neighbours, and that is the point: an absence
 * sentence in this panel says what was looked at, never what is there.
 */
export const CALLERS_EMPTY = 'No caller of this symbol appears in what the index resolved.';
export const ENV_READS_LABEL = 'Reads from the environment';
export const TYPE_REFS_LABEL = 'Uses types';
export const WRITES_LABEL = 'Writes';
export const STATE_EMPTY = 'This symbol reads no environment values and names no types.';
export const ERRORS_EMPTY = 'This symbol raises no error type that the index recorded.';

// ---------------------------------------------------------------------------
// The three semantic blocks
// ---------------------------------------------------------------------------

/**
 * The eyebrow above the three blocks the technical depths group facts into.
 *
 * One word each, in capitals, because they are read at a glance and never read
 * as a sentence: a reader scanning the panel is looking for the shape of the
 * symbol, which is what it holds, how it fails and what it touches. The section
 * heading underneath keeps saying what the rows actually are, so the eyebrow
 * adds a grouping without replacing a description.
 */
export const BLOCK_LABEL_DATA = 'DATA';
export const BLOCK_LABEL_ERRORS = 'ERRORS';
export const BLOCK_LABEL_EFFECTS = 'EFFECTS';

/**
 * What the panel says about where an error ends up.
 *
 * The obvious thing to render beside a raised type is "handled at X", and it is
 * the single most tempting invention in this whole panel: a reader wants it, the
 * shape of the answer is obvious, and a plausible one could be assembled from
 * the caller list. The 0.9.0 engine records no handler relation at all, so any
 * such line would be CodeAtlas guessing which caller catches what, and being
 * wrong about it in exactly the situation where a reader would rely on it most.
 *
 * So the block says what is true: the index can see where an error is raised and
 * cannot see where it is caught. The sentence names the limitation as the
 * index's rather than the code's, because "handling not found" would read as a
 * finding about the code and this is not one.
 */
export const ERRORS_HANDLING_NOT_VISIBLE =
    'Where these are handled is not visible to the index: it records where an error is raised, not where it is caught.';

/**
 * What the effects block says, given that the index carries no effect relations.
 *
 * Deliberately three named kinds rather than a vague "no effects": a reader has
 * to be able to tell that routes, outbound calls and writes are the three things
 * this block would hold, so that its emptiness is legible as a gap in the index
 * rather than as a claim that this symbol is pure.
 *
 * Until 2026-08-29 the sentence ended with a date for the effect relations that
 * nobody here sets. That was a promise about another product's roadmap made in
 * this product's voice: this
 * side does not decide what the analysis server records, and a reader who waited
 * for it would have been waiting on a schedule nobody here holds. What is true is
 * that the gap is written down and handed over, and the sentence says that.
 */
export const EFFECTS_NOT_IN_INDEX =
    'The index of this server records no routes, no outbound calls and no writes, so this block stays empty: '
    + 'that is a gap in the index and not a finding about this symbol. It is written down as Ask 2 in '
    + 'UPSTREAM-ASKS.md.';

/** Group headings inside the effects block, used once the relations exist. */
export const EFFECTS_ROUTES_LABEL = 'Exposes routes';
export const EFFECTS_HTTP_LABEL = 'Calls out';
export const EFFECTS_WRITES_LABEL = 'Writes';

/** Badge on a step whose target is constructed rather than called. */
export const BADGE_CONSTRUCTION = 'new';
export const BADGE_CONSTRUCTION_TOOLTIP = 'This step constructs a class rather than calling a function.';

/** Badge on a caller the index flagged as test code. */
export const BADGE_TEST = 'test';
export const BADGE_TEST_TOOLTIP = 'The index flagged this caller as test code.';

/** How the tests section counts what it found. */
export function testCallerCount(count: number): string {
    return count === 1 ? '1 test caller' : `${count} test callers`;
}

/**
 * The tests section's empty case.
 *
 * Never "not tested": the engine records no test relation for TypeScript, so
 * this is a search through callers that found none, and the sentence says so.
 */
export const TESTS_EMPTY_INFERRED =
    'No test callers found (inferred from callers, not from a test relation): decide if this needs a test.';

// ---------------------------------------------------------------------------
// Evidence affordance
// ---------------------------------------------------------------------------

export const EVIDENCE_BUTTON_LABEL = 'Evidence';
export const EVIDENCE_BUTTON_TOOLTIP = 'Show what this claim is based on.';
export const EVIDENCE_POPOVER_TITLE = 'Evidence';
export const EVIDENCE_CLOSE_LABEL = 'Close';
export const EVIDENCE_EMPTY = 'No citation was recorded for this claim.';
export const EVIDENCE_NO_LOCATION = 'no recorded location';

/** Attribution line under one citation: which provider said it and from which index build. */
export function evidenceAttribution(providerId: string, generation: number): string {
    return `${providerId}, index generation ${generation}`;
}

// ---------------------------------------------------------------------------
// Knowledge state markers
// ---------------------------------------------------------------------------

/** Short word shown next to a section heading whose fact is not a direct reading. */
export function stateMarkerLabel(state: KnowledgeState): string {
    switch (state) {
        case 'inferred': return 'inferred';
        case 'ambiguous': return 'ambiguous';
        case 'unsupported': return 'not recorded';
        case 'notIndexed': return 'not indexed';
        case 'unknown': return 'no answer';
        case 'known': return '';
    }
}

/**
 * One sentence saying why a fact is empty or untrusted.
 *
 * This is a deliberate second copy of the backend's `explainAbsence`. The
 * backend needs the sentences to explain evidence it serves to any client; the
 * twin needs them for a tooltip it renders thousands of times while the caret
 * moves, and shipping a round trip per tooltip to avoid duplicating six strings
 * would be the wrong trade. Keep the two in step: a change to one of these
 * sentences belongs in `evidence-service.ts` as well, and the sentences are
 * kept identical so a diff of the two files is the check.
 */
export function absenceSentence(state: KnowledgeState, subject: string): string {
    switch (state) {
        case 'notIndexed':
            return `${subject} is unknown because this file is not in the index yet.`;
        case 'unsupported':
            return `${subject} is unknown because the analyzer does not record this for this language.`;
        case 'inferred':
            return `${subject} is derived from indexed callers, not read directly.`;
        case 'unknown':
            return `${subject} is unknown because no analysis backend answered.`;
        case 'ambiguous':
        case 'known':
            return '';
    }
}

// ---------------------------------------------------------------------------
// Terminology: the same seven sections, named twice
// ---------------------------------------------------------------------------

/**
 * Section headings in the two terminologies.
 *
 * Not a translation of one another. The technical set names the relation the
 * index recorded, which is what a reviewer wants; the plain set names the
 * question a reader arrived with, which is what someone new to the code wants.
 * Both are true about the same rows, and neither is a simplification of the
 * other: "Called by" and "Who uses it" are the same fact addressed to different
 * people.
 */
export const SECTION_LABELS_TECHNICAL: Readonly<Record<string, string>> = {
    purpose: SECTION_PURPOSE,
    steps: SECTION_STEPS,
    callers: SECTION_CALLERS,
    state: SECTION_STATE,
    imports: SECTION_IMPORTS,
    errors: SECTION_ERRORS,
    effects: SECTION_EFFECTS,
    tests: SECTION_TESTS,
    risks: SECTION_RISKS,
    runtime: SECTION_RUNTIME,
    changes: SECTION_CHANGES
};

export const SECTION_LABELS_PLAIN: Readonly<Record<string, string>> = {
    purpose: 'What this is for',
    steps: 'What it does, in order',
    callers: 'Who uses it',
    state: 'What it reads',
    imports: 'What the file brings in',
    errors: 'How it can fail',
    effects: 'What it touches outside',
    tests: 'What checks it',
    risks: 'What to watch out for',
    runtime: 'What happens when it runs',
    changes: 'What changed recently'
};

/**
 * One glyph per section, in both terminologies.
 *
 * Icons rather than words alone because the panel is scanned before it is read,
 * and a reader who has met the panel twice finds the failure modes by the shape
 * beside the heading rather than by reading five headings. They are codicons,
 * which every Theia theme ships and restyles, so nothing here pins a colour or
 * an asset.
 *
 * An icon is never the only carrier of meaning: the heading beside it says the
 * same thing in words, which is what makes it safe to add one at all.
 *
 * (Portierungsnotiz: die Werte sind unveraendert uebernommen, damit ein Diff der
 * beiden Dateien ein Diff bleibt. Diese Oberflaeche liefert keine Codicon-Schrift
 * aus und zeichnet deshalb keine Glyphe; das Panel schreibt den Namen als
 * `data-icon` an die Ueberschrift, und die Ueberschrift daneben sagt weiterhin
 * in Worten dasselbe.)
 */
export const SECTION_ICONS: Readonly<Record<string, string>> = {
    purpose: 'codicon-info',
    steps: 'codicon-list-ordered',
    callers: 'codicon-references',
    state: 'codicon-database',
    imports: 'codicon-package',
    errors: 'codicon-warning',
    effects: 'codicon-globe',
    tests: 'codicon-beaker',
    risks: 'codicon-alert',
    runtime: 'codicon-pulse',
    changes: 'codicon-git-commit'
};

// ---------------------------------------------------------------------------
// The two lenses this panel names but does not answer itself
// ---------------------------------------------------------------------------

/**
 * What the runtime lens says when no recording has been imported for this symbol.
 *
 * Until 2026-08-29 this sentence said that CodeAtlas "does not watch code while
 * it runs yet" and sent the reader to a menu entry for importing recordings. Both
 * halves were wrong here: this product DOES show observed calls, on the BUG hunt
 * hops and in the flow, and the menu entry it named does not exist in this
 * surface. A sentence that sends a reader to a menu item they will not find is
 * worse than one that says nothing, because they will look for it.
 *
 * So the sentence says where the observed calls are and how they get in. The way
 * in is the command line, because this surface writes nothing (see the help
 * page, section "What it cannot do").
 */
export const RUNTIME_NOT_IN_TWIN =
    'This server hands observed calls to trace and flow answers, not to the twin, so this section stays empty. '
    + 'Recorded runs show up on the hops of the BUG hunt and in the flow; they get into the index through '
    + 'ingest_traces on the command line.';

/**
 * What the changes lens says.
 *
 * Until 2026-08-29 this claimed that CodeAtlas "does not read version history
 * yet", which the impact panel had been disproving since W4d: it reads exactly
 * that, out of detect_changes, for the worktree and against a reference. The twin
 * is what does not carry it, and that is what the sentence now says, with the
 * surface that does carry it named.
 */
export const CHANGES_NOT_IN_TWIN =
    'The twin carries no version history, so this section stays empty. What a change here would reach is '
    + 'answered by the change scope panel ([c]hange scope), which reads it from detect_changes for the '
    + 'worktree or against a reference you name.';

/** Why these two sections wear a marker even though nothing is missing from the index. */
export const LENS_ANSWERED_ELSEWHERE_NOTE =
    'Nothing is missing from the index here: the twin does not answer this lens, and it says so rather than '
    + 'showing an empty list.';

// ---------------------------------------------------------------------------
// The narrative depth: counts and consequences, in words, with no identifiers
// ---------------------------------------------------------------------------

/**
 * A symbol kind in ordinary English.
 *
 * The narrative depth is read by someone who has not decided yet whether they
 * care about the difference between an interface and a type alias, so it does
 * not spend their attention on one.
 */
export function plainKind(kind: CodeAtlasSymbolKind): string {
    switch (kind) {
        case 'function': return 'function';
        case 'method': return 'method';
        case 'class': return 'class';
        case 'interface': return 'contract other code has to satisfy';
        case 'module': return 'file';
        case 'variable': return 'value';
        case 'type': return 'shape';
        case 'route': return 'endpoint';
        case 'unknown': return 'piece of code';
    }
}

export function proseLead(name: string, kind: CodeAtlasSymbolKind): string {
    return `${name} is a ${plainKind(kind)}.`;
}

export function proseCalls(name: string, callees: readonly string[]): string {
    return `${name} hands work to ${countOf(callees.length, 'other piece of code', 'other pieces of code')}: `
        + `${joinNames(callees)}.`;
}

export function proseReads(name: string, values: readonly string[]): string {
    return `${name} reads ${countOf(values.length, 'value', 'values')} from the environment it runs in: `
        + `${joinNames(values)}.`;
}

export function proseThrows(name: string, errors: readonly string[]): string {
    return `${name} can stop with ${countOf(errors.length, 'kind of error', 'kinds of error')}: `
        + `${joinNames(errors)}.`;
}

/** A short English list that never hides its last member. */
export function joinNames(names: readonly string[]): string {
    if (names.length <= 1) {
        return names[0] ?? '';
    }
    if (names.length === 2) {
        return `${names[0]} and ${names[1]}`;
    }
    return `${names.slice(0, -1).join(', ')}, and ${names[names.length - 1]}`;
}

/** Lead-in to the clickable names. The only identifiers the narrative depth shows. */
export const PROSE_CHIPS_LEAD = 'The parts it leans on, in the order they come up:';

export function proseMoreParts(hidden: number): string {
    return `${countOf(hidden, 'further part is', 'further parts are')} not listed here; the deeper settings show all of them.`;
}

/** Said out loud, because the sentence above it was assembled rather than written. */
export const PROSE_GENERATED_NOTE =
    'Nobody wrote that description. CodeAtlas put it together from what the index recorded, so it can be wrong about intent and right about counts.';

export const PROSE_TESTS_NONE =
    'Nothing that looks like a test calls this, so no automatic check would notice a change here.';

export function proseTestsSome(count: number): string {
    return `${countOf(count, 'test calls', 'tests call')} this, so a change here would be noticed.`;
}

/** Plain subjects for the plain absence sentences below. */
export const PLAIN_SUBJECT_TESTS = 'Whether anything tests this';
export const PLAIN_SUBJECT_ERRORS = 'How this can fail';

/**
 * The absence sentences again, without the vocabulary.
 *
 * Same six cases as {@link absenceSentence} and deliberately not the same
 * wording: "the analyzer does not record this for this language" is precise and
 * means nothing to a reader who does not know there is an analyzer.
 */
export function plainAbsence(state: KnowledgeState, subject: string): string {
    switch (state) {
        case 'notIndexed':
            return `${subject} is unknown: CodeAtlas has not read this file yet.`;
        case 'unsupported':
            return `${subject} is unknown: CodeAtlas cannot see this for this language.`;
        case 'inferred':
            return `${subject} was worked out from what calls it, not read from the code itself.`;
        case 'unknown':
            return `${subject} is unknown: nothing answered when CodeAtlas asked.`;
        case 'ambiguous':
        case 'known':
            return '';
    }
}

// ---------------------------------------------------------------------------
// The guided depth
// ---------------------------------------------------------------------------

export const GUIDED_FACTS_LEAD = 'Start with these:';

// ---------------------------------------------------------------------------
// The dense depth: what a reviewer wants appended to every row
// ---------------------------------------------------------------------------

/**
 * How sure the resolver was. Two decimals because a provider that scores at all
 * reports two, and rounding a 0.55 to "medium" would hide the only interesting
 * part.
 */
export function confidenceLabel(value: number): string {
    return `confidence ${value.toFixed(2)}`;
}

export const CONFIDENCE_SCORED_TOOLTIP =
    'How sure the resolver was that this is the right target, as the provider recorded it.';

/**
 * The two things a row can say when no figure was recorded.
 *
 * An absent score is not a missing field to be hidden: it is the difference
 * between a relation the index read off the graph and one somebody derived. The
 * 0.9.0 engine scores nothing and reads its call, caller and raise relations
 * directly, so the honest label on those rows is that there is nothing to
 * score, and the honest label on a derived one is that nobody scored it. A row
 * that showed a blank where a reviewer expects a number would leave them
 * guessing which of the two they were looking at.
 */
export const CONFIDENCE_EXACT = 'exact';
export const CONFIDENCE_EXACT_TOOLTIP =
    'Read directly from the index. There is no score because nothing was guessed.';

export const CONFIDENCE_UNSCORED = 'unscored';
export const CONFIDENCE_UNSCORED_TOOLTIP =
    'This claim is not a direct reading and the provider recorded no score for it. The marker beside the heading says how it was arrived at.';

/** Which heuristic resolved the row, named the way the provider named it. */
export function strategyLabel(strategy: string): string {
    return `via ${strategy}`;
}

export function citationCount(count: number): string {
    return count === 1 ? '1 citation' : `${count} citations`;
}

/** Count beside a section heading, so a long list is readable while collapsed. */
export function rowCountLabel(count: number): string {
    return count === 1 ? '1 entry' : `${count} entries`;
}

// ---------------------------------------------------------------------------
// The presentation toolbar
// ---------------------------------------------------------------------------

/**
 * The slider asks who is reading, not how much to say.
 *
 * Until W13 it said "Detail level", and the label was the honest name of what
 * the control did: it made the same answer longer or shorter. That was the
 * weaker question. The same recorded facts do not need to be longer for a
 * senior than for a junior; they need to be *chosen differently*, because the
 * two arrived with different questions. So the label names the reader, the
 * value beside it names which reader is selected, and each level opens by
 * stating the question it answers.
 */
export const DEPTH_SLIDER_LABEL = 'Who is reading';
export const DEPTH_SLIDER_TOOLTIP =
    'The same recorded facts, chosen for one reader. Five readers ask five questions of one symbol: what is '
    + 'this and why does it matter, in what order does it happen, what is actually written here, what does it '
    + 'cost me, and what does it sit on. Nothing is invented on the way down and nothing is hidden on the way up.';

/**
 * The five readers, indexed by level, in the words the reader chose.
 *
 * Lower case on purpose: these are names of people, not names of settings, and
 * "Vibe Coder" reads like a product tier where "vibe coder" reads like someone
 * sitting down with the code.
 */
export const DEPTH_LABELS: readonly string[] = [
    'vibe coder',
    'junior',
    'medior',
    'senior',
    'architect',
];

/**
 * The question each level answers, said out loud at the top of the body.
 *
 * Not decoration and not a heading: it is the whole claim of the slider made
 * checkable. A reader who moves the slider and cannot tell what changed can
 * read the first line and see which question is now being answered, and a
 * level whose body does not answer its own question is a bug anyone can spot.
 */
export const LEVEL_QUESTIONS: readonly string[] = [
    'What happens here, and why does it matter to you?',
    'In what order does it happen, and why?',
    'What is actually written here?',
    'What does this cost you?',
    'What does this sit on, and where does our knowledge end?',
];

// ---------------------------------------------------------------------------
// Level 1, junior: the order, and the words it is told in
// ---------------------------------------------------------------------------

/**
 * The connectives between the steps.
 *
 * They say order and nothing else, because order is the one thing the index
 * really records: the call sites arrive in the sequence the body reaches them.
 * A connective that said "because" or "so that" would be a claim about intent,
 * which is exactly the invention this panel exists to avoid, and it would be
 * invisible as an invention because it reads like grammar.
 */
export const STEP_CONNECTIVES: readonly string[] = ['First', 'Then', 'After that', 'Next'];

/** The last one, when there is more than one step. */
export const STEP_CONNECTIVE_LAST = 'And last';

export function juniorStep(connective: string, name: string): string {
    return `${connective} it hands work to ${name}`;
}

/** Where that step lands, when the index knows where. */
export function juniorStepAt(location: string): string {
    return ` (${location})`;
}

export const JUNIOR_ORDER_LEAD =
    'These are the calls in the order the body reaches them. The order is read from the code; why they are in '
    + 'that order is not recorded anywhere, so nothing below claims it.';

export const JUNIOR_NO_STEPS =
    'The index resolves no outgoing calls from this symbol, so it records no call order to walk through. '
    + 'This is only the boundary of the resolved-call reading.';

/** The heading over the words explained on first sight. */
export const JUNIOR_TERMS_LEAD = 'Words used above, once each:';

/**
 * The glossary, keyed by the word.
 *
 * Only the words this level actually puts on screen are shown, and each is
 * shown once. A glossary that lists every term whether or not it appeared
 * would be a page of vocabulary in front of the answer.
 */
export const TERM_EXPLANATIONS: Readonly<Record<string, string>> = {
    'call site':
        'a place in this code where it hands work to another piece of code. The index records where each one '
        + 'is and what it points at.',
    caller:
        'a piece of code that hands work to this one. If you change what this expects, every caller is where '
        + 'the change lands.',
    raise:
        'stopping with an error instead of returning a result. The index records which error types can leave '
        + 'this symbol, not where they are caught.',
    'environment read':
        'a value this code takes from outside itself, such as a setting or a configuration key, rather than '
        + 'from what it was handed.',
    'test caller':
        'a caller that lives in test code. It is how CodeAtlas answers "is this checked", because this index '
        + 'records no test relation of its own for TypeScript.',
    'entry point':
        'a place the outside world can start this program: a route, a command, a handler. Work arrives here '
        + 'rather than being handed on from somewhere inside.',
};

// ---------------------------------------------------------------------------
// Level 3, senior: what it costs
// ---------------------------------------------------------------------------

export const COST_TITLE_FAILS = 'How it can fail';
export const COST_TITLE_UNTESTED = 'What is unchecked';
export const COST_TITLE_DEPENDS = 'Who depends on it';
export const COST_TITLE_MOVES = 'What moves with a change';

export const COST_LEAD_FAILS =
    'The error types recorded as leaving this symbol. Where they are caught is not in this index, so the list '
    + 'is the cost you can see and not the whole cost.';
export const COST_LEAD_UNTESTED =
    'What would notice if this broke. This engine records no test relation for TypeScript, so what is listed '
    + 'here is callers that live in test code.';
export const COST_LEAD_DEPENDS =
    'Everything the index records as calling this. Change what it expects or what it returns and this is the '
    + 'list you have to walk.';
export const COST_LEAD_MOVES =
    'What a change here reaches on the way out: the calls it makes, the values it reads and what it touches '
    + 'outside its own process.';

export const COST_EMPTY_FAILS =
    'No error path was recorded here, so a change cannot break one you have not seen. What is not recorded is '
    + 'where a raise from further down is caught, and this engine does not read that at all.';
export const COST_EMPTY_UNTESTED =
    'Nothing that looks like a test calls this. A change here would be caught by a reader or by production, '
    + 'and by nothing in between.';
export const COST_EMPTY_DEPENDS =
    'Nothing in the indexed workspace calls this. Either it is reached from outside the index, or it is dead, '
    + 'and the index cannot tell you which of the two.';
export const COST_EMPTY_MOVES =
    'This reaches nothing outside itself that the index recorded: no call, no environment read, no effect '
    + 'crossing the process boundary. A change here stays where you put it.';

/** How the senior level names an item's cost in one word beside it. */
export function costWeight(count: number): string {
    return `${countOf(count, 'place', 'places')} to check`;
}

// ---------------------------------------------------------------------------
// Level 4, architect: the ground, the carriers, the debts, and the limits
// ---------------------------------------------------------------------------

export const GROUND_TITLE_SITS_ON = 'What it sits on';
export const GROUND_TITLE_CARRIED = 'What carries it';
export const GROUND_TITLE_DEBTS = 'What it owes';
export const GROUND_TITLE_LIMITS = 'Where this index stops';

export const GROUND_LEAD_SITS_ON =
    'The modules underneath this symbol, one line each, with how many call sites reach into them. A module '
    + 'reached from many places is a dependency; a module reached once is a detail.';
export const GROUND_LEAD_CARRIED =
    'What holds this up: the callers, and whether the outside world can reach it directly.';
export const GROUND_LEAD_DEBTS =
    'What is owed against this symbol, from the rules CodeAtlas applies to the recorded facts. Every line is a '
    + 'rule over facts and not a fact, which is why they all wear the derived marker.';
export const GROUND_LEAD_LIMITS =
    'What is not on this page, and why. This is part of the answer and not a footnote: an architect who cannot '
    + 'see the edge of the index will read a silence as a finding.';

export const GROUND_EMPTY_SITS_ON =
    'This sits on nothing the index could resolve: no call from here reaches a file it knows. That is either a '
    + 'leaf or an unresolved edge, and the limits below say which of the two this index can tell apart.';
export const GROUND_EMPTY_CARRIED =
    'Nothing carries this: no recorded caller, and nothing marking it as reachable from outside. A symbol with '
    + 'no carrier is either an entry the index cannot see or a piece nobody uses.';
export const GROUND_EMPTY_DEBTS =
    'No rule fired against this symbol. That is not a clean bill: the rules CodeAtlas can apply are the ones '
    + 'listed under what this index stops at, and the ones it cannot apply stay silent either way.';

/** How many call sites reach into one module. */
export function groundReach(count: number): string {
    return `${countOf(count, 'call site', 'call sites')}`;
}

/** The architect's word for a symbol the outside world can start. */
export const GROUND_ENTRY_POINT =
    'The outside world can start here: this symbol is recorded as an entry point, so a caller list of zero is '
    + 'not the same as unused.';
export const GROUND_EXPORTED =
    'Exported from its module, so code outside the index can call it without the index knowing.';
export const GROUND_NOT_EXPORTED =
    'Not exported, so everything that can call it is inside this module and inside this index.';

/** The debts that come from the structural flags rather than from a rule. */
export const DEBT_RECURSIVE = 'Calls itself. Depth is bounded by whatever the caller passes in, not by the code.';
export const DEBT_UNGUARDED_RECURSION =
    'Recursive with no base case the analyzer could see. If that reading is right, the bound is the stack.';
export const DEBT_SCAN_IN_LOOP =
    'A linear scan runs inside a loop, which is the shape a quadratic hides in.';
export const DEBT_ALLOC_IN_LOOP = 'Allocates inside a loop, so the cost grows with the iteration count.';

export function debtLoopDepth(depth: number): string {
    return `Loops nested ${depth} deep inside this symbol alone.`;
}

export function debtBranches(count: number): string {
    return `${count} independent branches through the body, so that many paths have to hold.`;
}

/** How the limits block words one family the index could not answer. */
export function limitOfFamily(subject: string, sentence: string): string {
    return `${subject}: ${sentence}`;
}

export function limitGeneration(generation: number): string {
    return `Everything above was read from index generation ${generation}. An edit after that build is not in it, `
        + 'and this page cannot tell you whether there was one.';
}

export const LIMIT_NO_TEST_RELATION =
    'This engine records no test relation for TypeScript at all, so "what checks it" is answered by looking at '
    + 'callers that live in test files, and never by the index saying so.';
export const LIMIT_NO_HANDLER_SITES =
    'Raised error types are recorded; the places they are caught are not. A short error list is therefore not '
    + 'evidence that the failure is contained.';
export const LIMIT_NO_ROUTES =
    'Routes for TypeScript are read out of the source text by CodeAtlas, not reported by the engine, so an '
    + 'entry point the engine did not mark can still exist.';
export const LIMIT_CALLS_DEDUPLICATED =
    'Two calls to the same target inside one body are recorded once, with the last site. A count of call sites '
    + 'is therefore a count of targets and not of calls.';

// ---------------------------------------------------------------------------
// The local model, when it is on: it rewords, it does not add
// ---------------------------------------------------------------------------

/** The button that asks the model to say the same thing to this reader. */
export const VOICE_LABEL = 'Say it for this reader';
export const VOICE_TITLE =
    'Hand the sentences above to the local model and let it word them for the selected reader. Every name, '
    + 'number, file and line has to survive, in the same order, or the rewrite is thrown away and you are told.';
export const VOICE_RUNNING = 'The local model is rewording the sentences above.';
export const VOICE_RESTORE_LABEL = 'Back to the built text';
export const VOICE_RESTORE_TITLE =
    'Drop the reworded sentences and show the ones CodeAtlas assembled from the index.';

/** The marker beside a sentence the model worded. Same idiom as the state markers. */
export const VOICE_MARKER = '[~]';
export const VOICE_MARKER_NOTE =
    'Worded by the local model from the sentence CodeAtlas built. The facts in it were checked against the '
    + 'original one by one; the wording is the model.';
export const VOICE_APPLIED =
    'The sentences above were reworded by the local model. Every name, number, file and line in them was '
    + 'checked against the built text.';

export function voiceRefused(reason: string): string {
    return `The rewrite was thrown away and the built text is what you see. Reason: ${reason}`;
}

export const VOICE_UNAVAILABLE =
    'The local model is off, so these sentences are the ones CodeAtlas assembled. Nothing here waits for it.';

export const FACET_LABELS: Readonly<Record<Facet, string>> = {
    [Facet.Logic]: 'Logic',
    [Facet.Calls]: 'Calls',
    [Facet.Data]: 'Data',
    [Facet.Errors]: 'Errors',
    [Facet.Tests]: 'Tests',
    [Facet.Runtime]: 'Runtime',
    [Facet.Changes]: 'Changes'
};

export function facetTooltip(label: string, on: boolean): string {
    return on ? `Hide ${label}` : `Show ${label}`;
}

// ---------------------------------------------------------------------------
// The mini understanding panel
// ---------------------------------------------------------------------------

export const MINI_TITLE = 'Left to understand';

/**
 * What follows the figure in the strip's header.
 *
 * Split out from {@link miniProgress} so the renderer can give the figure an
 * element of its own without the two drifting apart: a driver that compares the
 * strip with the checklist panel reads a number rather than parsing a sentence,
 * and the sentence a reader sees is still assembled in one place.
 */
export const MINI_PERCENT_SUFFIX = '% confirmed';

export function miniProgress(percent: number): string {
    return `${percent}${MINI_PERCENT_SUFFIX}`;
}

/**
 * What the percentage above is counting, and what would move it.
 *
 * The figure is confirmations only. A reader who has followed every line and
 * confirmed nothing still sees zero, and the sentence says why so the zero is
 * not read as a judgement about how much work they have done.
 */
export const MINI_PROGRESS_NOTE =
    'That figure counts what you have confirmed, not what you have looked at. Tick items in the Understanding Checklist to move it.';

export function miniReadCalls(count: number): string {
    return `Read the ${countOf(count, 'function', 'functions')} this calls`;
}

export function miniErrorPath(types: string[]): string {
    return types.length === 1
        ? `Check the error path (${types[0]})`
        : `Check the error paths (${types.join(', ')})`;
}

export function miniCallers(count: number): string {
    return `See the ${countOf(count, 'caller', 'callers')}`;
}

export function miniConfig(names: string[]): string {
    return names.length === 1
        ? `Confirm ${names[0]} is set`
        : `Confirm ${countOf(names.length, 'configuration value is', 'configuration values are')} set`;
}

export function miniShape(names: string[]): string {
    return names.length === 1
        ? `Know the shape of ${names[0]}`
        : `Know the shapes of the ${countOf(names.length, 'type', 'types')} this names`;
}

export function miniConstructions(count: number): string {
    return `Read the ${countOf(count, 'class', 'classes')} this constructs`;
}

export function miniTests(count: number): string {
    return count === 1
        ? 'Re-read the 1 test that covers this'
        : `Re-read the ${count} tests that cover this`;
}

/** Same finding as TESTS_EMPTY_INFERRED, shortened to fit a checklist line. */
export const MINI_TESTS_NONE = 'No test callers found: decide if this needs a test';

/**
 * What the strip promises when a reader hovers a line.
 *
 * The strip led with a percentage for four cycles and the lines under it were
 * read as a list of complaints. They are not: each one starts a short guided
 * walk through the places that answer it, which is the difference between a
 * panel that grades a reader and a panel that takes them somewhere.
 */
export const MINI_ITEM_TOOLTIP = 'Walk the places that answer this, one stop at a time.';

// ---------------------------------------------------------------------------
// The guided hop
// ---------------------------------------------------------------------------

/**
 * A short walk through the two or three places that answer one open question.
 *
 * The wording carries one rule the mechanism cannot. A stop says why the place
 * it opened matters, and that sentence is written from what the checklist item
 * already claims, never from anything about the code CodeAtlas has not recorded.
 * The reader is being taken somewhere and told why that somewhere is on the
 * list; they are not being told what they will find when they get there.
 */
export function hopProgress(stop: number, total: number): string {
    return `Stop ${stop} of ${total}`;
}

export const HOP_NEXT_LABEL = 'Next';
export const HOP_DONE_LABEL = 'Done';
export const HOP_CLOSE_TOOLTIP = 'End the walk. Everything you have already been taken to stays marked as explored.';

/**
 * Why each stop of a walk is worth the trip, one sentence per checklist family.
 *
 * Deliberately about the obligation rather than about the target: "you cannot
 * say what this does without knowing what it delegates to" is true of every
 * callee and needs nothing the index has not recorded, where "this validates the
 * user" would be a claim about a function CodeAtlas has not read.
 */
export const HOP_WHY: Readonly<Record<string, string>> = {
    'core-logic': 'What this symbol does is mostly what it delegates to. This is one of the places it hands work to.',
    'error-handling': 'This is where the failure comes from. Reading it is how you find out what a caller has to survive.',
    callers: 'Someone depends on this. What they expect is the constraint on any change you make here.',
    config: 'This value comes from the environment, so it is set outside the code and fails only where it is missing.',
    inputs: 'This is a shape the symbol assumes it was handed. Knowing it is how you tell a valid input from an invalid one.',
    implementations: 'This class is constructed here rather than called, so reading it is a separate obligation from following a call.',
    tests: 'A test is the shortest available description of what this is supposed to do.'
};

/** Fallback for a category nobody has written a sentence for yet. */
export const HOP_WHY_FALLBACK = 'This is one of the places the checklist points at for this question.';

export function hopWhy(category: string): string {
    return HOP_WHY[category] ?? HOP_WHY_FALLBACK;
}

// The runtime facet of the twin, once a recording has been imported ----------

/** The section's own sentence when the store was read and holds nothing for this symbol. */
export const RUNTIME_SECTION_EMPTY =
    'A recording has been imported and none of it passes through this symbol.';

/** Heading text over the observed rows, with the total. */
export function runtimeObservedCount(calls: number, total: number): string {
    return `${calls} observed ${calls === 1 ? 'call' : 'calls'}, ${total} ${total === 1 ? 'time' : 'times'} in all.`;
}

/** One observed row's count, beside the callee's name. */
export function runtimeRowCount(count: number): string {
    return `${count}x`;
}

/** The marker on an observed call the index does not record. */
export const RUNTIME_UNEXPECTED_BADGE = 'not in the index';
export const RUNTIME_UNEXPECTED_TOOLTIP =
    'The recording holds this call and the analyzer records no such call from this symbol.';

/** Why the runtime section wears its marker: nothing is missing from the index here. */
export const RUNTIME_SECTION_NOTE =
    'Read from an imported recording, not from the index. CodeAtlas did not watch this run.';

// The runtime badge in the evidence popover ---------------------------------

/** Shown on a citation that came from a recording rather than from the index. */
export const EVIDENCE_RUNTIME_BADGE = 'runtime';
export const EVIDENCE_RUNTIME_TOOLTIP =
    'This citation comes from an imported recording, not from the analyzer.';

/** How many times the recording saw it, said under the badge. */
export function evidenceObservations(count: number): string {
    return `observed ${count} ${count === 1 ? 'time' : 'times'}`;
}
