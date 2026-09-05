/*
 * Herkunft: portiert am 2026-08-29 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/strings.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Uebernommen ist der Wortlaut der
 * Pseudocode-Bloecke (Z. 1874-2064), der Imports-Gruppe (Z. 2069-2190) und des
 * Flow-Erklaerers (ab Z. 2192), WOERTLICH und samt Begruendungen: der
 * Unterschied zwischen "nicht benutzt" und "nicht pruefbar" wird in diesem
 * Produkt ausschliesslich vom Wortlaut getragen.
 *
 * Warum eine eigene Datei und nicht src/twin/strings.ts: dieselbe Trennung, die
 * dieses Projekt schon fuer impact-strings.ts und bug-wizard-strings.ts gewaehlt
 * hat. Der Twin-Wortlaut ist die Kopie einer Flaeche; hier steht der Wortlaut
 * einer zweiten. Was der Twin davon liest (die Sektion "Pulls in"), steht in
 * src/twin/strings.ts, weil es dort die Sektionstabellen fuellt.
 *
 * Aenderungen gegenueber dem Original: der Importpfad von countOf, und
 * weggelassen ist der Wortlaut der Flaechen, die es hier nicht gibt (Menue- und
 * Kommando-Beschriftungen des Overlays, die Sende-Vorschau und die Daten-Policy
 * der optionalen Verfeinerung; applyRefinedPseudocode ist portiert und
 * unit-getestet, aber nicht verdrahtet, siehe PLAN W5).
 */

import { countOf } from '../twin/strings';

// ---------------------------------------------------------------------------
// Die Bloecke
// ---------------------------------------------------------------------------

/** The heading over each scope's block. */
export function pseudocodeSymbolTitle(name: string): string {
    return `Steps in ${name}`;
}

export function pseudocodeClassTitle(name: string): string {
    return `Steps in class ${name}`;
}

export function pseudocodeSelectionTitle(count: number): string {
    return `Steps in ${countOf(count, 'selected symbol', 'selected symbols')}`;
}

export function pseudocodeClosureTitle(name: string): string {
    return `Steps in ${name} and the code it reaches`;
}

/**
 * One step.
 *
 * The verb is the strategy the index recorded and nothing else: a construction
 * is drawn as "constructs" because that is what the graph called it, and a
 * resolved invocation is drawn as "calls". Guessing a richer verb from the name
 * of the callee is exactly the invention this feature must not make.
 */
export function pseudocodeStepLine(order: number, verb: string, target: string): string {
    return `${order}. ${verb} ${target}`;
}

/** The two verbs, and the only two. */
export const PSEUDOCODE_VERB_CALL = 'call';
export const PSEUDOCODE_VERB_CONSTRUCT = 'construct';

/** One error the symbol can raise. "May" because the index records the site, not the condition. */
export function pseudocodeRaiseLine(order: number, type: string): string {
    return `${order}. may raise ${type}`;
}

/** One environment value the symbol reads. */
export function pseudocodeEnvLine(order: number, key: string): string {
    return `${order}. read ${key} from the environment`;
}

/** The heading over one method of a class. */
export function pseudocodeMethodGroup(name: string): string {
    return `method ${name}:`;
}

/** The heading over one symbol of a selection or a closure. */
export function pseudocodeSymbolGroup(name: string): string {
    return `${name}:`;
}

/**
 * Said under a group the index had nothing to list for.
 *
 * A group with no lines under it would read as a method that does nothing. What
 * is true is narrower: the index recorded no call, no raise and no environment
 * read for it, which is a statement about the index and is worded as one.
 */
export function pseudocodeEmptyGroup(name: string): string {
    return `the index recorded no calls, raised errors or environment reads for ${name}`;
}

/** Said at the end of a block the closure's bound cut short. */
export function pseudocodeCappedNote(hidden: number): string {
    return `and ${countOf(hidden, 'more symbol', 'more symbols')} not expanded: the walk stopped at its bound`;
}

/**
 * Said at the end of a class or selection block that could not be resolved in
 * full.
 *
 * CodeAtlas finds the members of a range by asking the index what encloses each
 * line, and over a long range it asks at a stride and keeps the answers under a
 * ceiling. Both bounds can miss a member. A block that said nothing about that
 * would be a list of methods that is quietly not the list of methods, which is
 * the one way this feature could mislead without saying anything untrue.
 */
export const PSEUDOCODE_PARTIAL_SCOPE =
    'CodeAtlas sampled this range rather than reading every line of it, so a very short member may be missing.';

/**
 * The footer, and the sentence that keeps the whole block honest.
 *
 * Two numbers and one standing claim. The numbers say how many of the symbols
 * in scope actually contributed a line, because a block assembled from ten
 * symbols of which two had facts is not a description of ten symbols. The claim
 * is the same one every derived surface in this product carries: this came out
 * of an index, and an index reports what it resolved.
 */
export function pseudocodeCoverage(covered: number, total: number): string {
    return `${covered} of ${countOf(total, 'symbol', 'symbols')} in scope contributed steps.`;
}

/** Named symbols the block covers nothing for, so their absence is not read as emptiness. */
export function pseudocodeUncovered(names: readonly string[]): string {
    return `Nothing was listed for ${names.join(', ')}.`;
}

export const PSEUDOCODE_SOURCE_NOTE =
    'Derived from the index and nothing else: every line is a call, a raised error or an environment read '
    + 'that the analysis reported, in the order it reported them. Anything the index did not resolve is '
    + 'absent from this block, not absent from the code.';

/** Tooltip on a numbered line, which opens the place it was read from. */
export const PSEUDOCODE_LINE_TOOLTIP = 'Open the line this came from.';

// ---------------------------------------------------------------------------
// W8c: der Fund zuerst, das Ziel an jeder Zeile, und das Meta hinter dem
// Fragezeichen
//
// Nutzerfrage vom 2026-08-29 zum Screenshot mit orderService.ts: "ist der
// Pseudocode AI generated oder mechanisch, und ist er hilfreich?" Die Herkunft
// ist mechanisch (nur refine.ts kennt das Modell, und der Leser stoesst es an).
// Das Urteil zur Nuetzlichkeit war der Anlass: neben einer 18-Zeilen-Funktion
// standen zwei Zeilen, "1. call validateId" und "2. call query", und der Code
// daneben zeigte mehr. Der Block fuehrt seitdem mit dem, was der Leser im Code
// NICHT sieht, und die Schrittliste traegt, wohin jeder Schritt geht.
//
// Was hier ausdruecklich NICHT steht, ist eine Zahl, ab der eine Funktion
// "kurz genug" waere, um die Schrittliste wegzulassen. Eine solche Schwelle
// waere geraten, und geraten heisst in diesem Produkt: erfunden. Geordnet wird
// nach Beitrag, nicht nach Laenge.
// ---------------------------------------------------------------------------

/**
 * Der eine Satz ueber dem Block: was hier der Fund ist.
 *
 * Vier Faelle, und die Reihenfolge ist die ihrer Staerke. Ein Import, den der
 * Index diesem Symbol nicht zuordnen kann, ist die staerkste Aussage, die
 * dieser Block ueber eine Datei macht, die der Leser nicht kennt; was hinter
 * den Aufrufen liegt, ist die zweitstaerkste; und wenn beides nichts hergibt,
 * sagt der Satz das, statt zu verschwinden. Ein Kopf, der nur bei einem Fund
 * dasteht, waere ein Kopf, dessen Fehlen selbst eine Behauptung ist.
 */
export function pseudocodeLeadUnused(symbol: string, unused: number, total: number): string {
    return `You cannot see this in the code: ${unused} of the ${countOf(total, 'name', 'names')} this file `
        + `pulls in ${unused === 1 ? 'is' : 'are'} not used by ${symbol} as far as the index shows.`;
}

export function pseudocodeLeadUnchecked(symbol: string, unknown: number): string {
    return `You cannot see this in the code: every name this file pulls in is one the index links to `
        + `${symbol}, except ${countOf(unknown, 'one', 'ones')} CodeAtlas cannot check either way.`;
}

export function pseudocodeLeadBehind(steps: number): string {
    return `You cannot see this in the code: the index records raised errors, further calls or environment `
        + `reads behind ${countOf(steps, 'step', 'steps')} below.`;
}

export function pseudocodeLeadNone(symbol: string, total: number): string {
    if (total === 0) {
        return 'Nothing stands above the steps this time: CodeAtlas found no import in this file, and the '
            + 'index records nothing behind the steps below.';
    }
    return `Nothing stands above the steps this time: every name this file pulls in is one the index links `
        + `to ${symbol}, and it records nothing behind the steps below.`;
}

/**
 * Gesagt, solange die Import-Antwort noch unterwegs ist.
 *
 * Sie kommt auf einem eigenen Weg und spaeter als die IR (siehe
 * imports-source.ts). Ein Kopf, der in dieser Zeit "kein Fund" sagte, wuerde
 * eine Abwesenheit behaupten, die bloss eine Verzoegerung ist.
 */
export const PSEUDOCODE_LEAD_PENDING =
    'What this file pulls in has not arrived yet, so what stands above the steps is not complete.';

/** Die Ueberschrift ueber der Schrittliste, die sagt, was an ihr neu ist. */
export const PSEUDOCODE_STEPS_HEADING = 'Steps, and where each one goes';

/** Tooltip am Ziel einer Zeile. Der Text der Zeile fuehrt woandershin, siehe PseudocodeLine. */
export const PSEUDOCODE_TARGET_TOOLTIP = 'Open where this step leads: the symbol it calls.';

/**
 * Gesagt an einer Zeile, deren Ziel der Index nicht kennt.
 *
 * Eine stumme Zeile waere an dieser Stelle die schlechteste Antwort: sie sieht
 * aus wie eine Zeile, deren Ziel niemand gebraucht hat, und ist in Wahrheit
 * eine, deren Aufruf der Index nicht aufloesen konnte.
 */
export const PSEUDOCODE_TARGET_UNKNOWN = 'the index records no place for this name';

/** Was hinter einem Aufruf liegt, in den Worten der Relation, die es sagt. */
export function insightRaises(types: string): string {
    return `may raise ${types}`;
}

export function insightCalls(count: number): string {
    return `makes ${countOf(count, 'call', 'calls')} of its own`;
}

export function insightReadsEnv(key: string): string {
    return `reads ${key} from the environment`;
}

export function insightMore(count: number): string {
    return `and ${count} more`;
}

/**
 * Was neben den Schritten steht, kommt woher, und was sein Fehlen NICHT heisst.
 *
 * Der zweite Halbsatz ist der wichtigere. Eine Notiz, die nur erscheint, wenn
 * es etwas zu melden gibt, laedt dazu ein, ihr Fehlen als "da ist nichts" zu
 * lesen. Was fehlt, ist eine aufgezeichnete Beziehung, und das ist etwas
 * anderes als ein Aufruf, hinter dem nichts passiert.
 */
export const PSEUDOCODE_BEHIND_NOTE =
    'What stands beside a step comes from the relations the index recorded for the symbol that step calls, '
    + 'in the graph this window already loaded: what it can raise, what it calls in turn, and what it reads '
    + 'from the environment. It is not a reading of that symbol\'s body. A step with nothing beside it is a '
    + 'step whose callee the loaded graph records none of those three for, not a callee that does none of '
    + 'them.';

/**
 * Der EINE Satz, der unter dem Block stehenbleibt.
 *
 * Dieselbe Entscheidung wie in W8b unter dem Diagramm, und aus demselben
 * Befund: unter dem Block standen zwei Absaetze ueber den Block selbst, und der
 * laengere davon wiederholte in 244 Zeichen, was diese acht Worte sagen. Was
 * verschwindet, ist die Wiederholung und nicht die Aussage:
 * {@link pseudocodeCoverage}, {@link pseudocodeUncovered} und
 * {@link PSEUDOCODE_SOURCE_NOTE} stehen unveraendert hinter dem Fragezeichen
 * daneben, also in dem Idiom, das dieses Produkt fuer Herkunft schon hat.
 */
export const PSEUDOCODE_HONESTY_SHORT = 'Only what the index reported, in the order it reported it.';

/** Was das Fragezeichen unter dem Block sagt, wenn man es beruehrt. */
export const PSEUDOCODE_PROVENANCE_TOOLTIP = 'Where this block comes from.';

// ---------------------------------------------------------------------------
// Die Imports-Gruppe
//
// Drei Antworten statt zwei: `used` ist ein Befund, `unused` ist ein Befund
// ueber eine Abwesenheit, und `unknown` ist ein Eingestaendnis. Der dritte Wert
// ist der Grund, warum es die Gruppe ueberhaupt geben darf.
// ---------------------------------------------------------------------------

/** The heading over the group, in the pseudocode block. */
export const IMPORTS_GROUP_HEADING = 'What this file pulls in';

/**
 * Die Ueberschrift der Gruppe seit W8c: sie nennt den Fund, statt ihn zu
 * verstecken.
 *
 * Bis dahin hiess sie nur {@link IMPORTS_GROUP_HEADING}, also wie eine
 * Nebensache, und darunter stand der wertvollste Satz des ganzen Blocks ("nicht
 * benutzt in dieser Datei, soweit der Index es zeigt"). Sie behaelt die Worte
 * "pulls in", weil sie weiterhin die Liste der Importe ueberschreibt, und sie
 * stellt den Fund davor.
 *
 * Die Grenze steht MIT im Kopf, nicht erst in der Zeile darunter: ein Fund ohne
 * seine Grenze waere eine Behauptung, und wer eine Ueberschrift liest, liest die
 * Zeile darunter noch nicht.
 */
export function importsFindingHeading(unused: number, unknown: number, total: number): string {
    if (total === 0) {
        return IMPORTS_GROUP_HEADING;
    }
    if (unused > 0) {
        return `${IMPORTS_GROUP_HEADING}: ${countOf(unused, 'name', 'names')} not used anywhere in this file `
            + 'as far as the index shows';
    }
    if (unknown > 0) {
        return `${IMPORTS_GROUP_HEADING}: ${countOf(unknown, 'name', 'names')} CodeAtlas cannot check `
            + 'against this file';
    }
    return `${IMPORTS_GROUP_HEADING}: every name here is linked to a symbol in this file`;
}

/** Said on an entry a fact from this file names. */
export function importsUsedLine(name: string, module: string): string {
    return `uses ${name} from ${module}`;
}

/**
 * Said on an entry no fact of the complete file reading names.
 *
 * The whole sentence is deliberately about the index rather than about the
 * code. A name one declaration never mentions may still be used two lines below
 * by its neighbour, which is why this line is reached only after every callable
 * declaration was read and every relevant family answered.
 */
export function importsUnusedLine(name: string, module: string): string {
    return `${name} from ${module}: imported here, not used anywhere in this file as far as the index shows`;
}

/** Said on an entry that could not be checked at all. */
export function importsUnknownLine(name: string, module: string): string {
    return `${name} from ${module}: CodeAtlas cannot tell whether this file uses it`;
}

/** Said on `import * as db from ...` and its equivalents in the other languages. */
export function importsNamespaceLine(binding: string, module: string): string {
    return `${binding} from ${module}: the whole module under one name`;
}

/** Said on `import './setup'`, which binds nothing. */
export function importsSideEffectLine(module: string): string {
    return `${module}: pulled in for what it does when the file loads`;
}

/** Said on an entry recovered from an import edge alone, with no statement behind it. */
export function importsIndexOnlyLine(path: string): string {
    return `${path}: the index records the dependency without naming what was imported`;
}

/** Why an entry says what it says. One sentence, on the entry itself. */
export const IMPORTS_NOTE_USED =
    'The index records a symbol in this file reaching this name, so the import is one the file actually leans on.';

export const IMPORTS_NOTE_UNUSED =
    'The index answered every checked symbol in this file, and none of their recorded facts mentions this name.';

export const IMPORTS_NOTE_UNKNOWN =
    'CodeAtlas does not have a complete answered reading for every callable in this file, so it will not claim the import is unused.';

export const IMPORTS_NOTE_NAMESPACE =
    'A namespace import binds every export of the module under one name, and the index records a call through it under the member\'s name. So nothing here can be matched against the binding.';

export const IMPORTS_NOTE_SIDE_EFFECT =
    'This statement binds no name, so there is nothing for the index to match it against. What the module does when it loads is not something the analysis records.';

export const IMPORTS_NOTE_INDEX_ONLY =
    'This came from the dependency the index recorded rather than from the file\'s text, so CodeAtlas has the imported file and not the imported name.';

/** Said at the end of a group the display cap cut short. */
export function importsCappedNote(hidden: number, shown: number): string {
    return `and ${countOf(hidden, 'more import', 'more imports')} not listed: this group shows the first ${shown}`;
}

/** Said when the file's own text could not be read at all. */
export const IMPORTS_SOURCE_UNREAD =
    'CodeAtlas could not read this file\'s text, so what follows is the dependencies the index recorded and not the import statements themselves.';

/** Said when the file pulls in nothing at all. */
export const IMPORTS_EMPTY =
    'The index records no dependency for this file and CodeAtlas found no import statement in it.';

/**
 * The standing sentence under the group, in both surfaces.
 *
 * It has to carry the split, because the group is the one place in the product
 * where two readings of different strength sit in one list: the dependency
 * comes from the analysis, the statement and the name come from the file, and
 * the judgement about use comes from every callable declaration the file read.
 */
export const IMPORTS_SOURCE_NOTE =
    'The dependency comes from the index; the statement, its line and the name it binds are CodeAtlas reading '
    + 'this file. Use is checked against the recorded facts of every callable symbol in the file.';

/** The three markers. Short enough to sit beside a name, and never the only carrier of meaning. */
export const IMPORT_MARK_USED = 'used here';
export const IMPORT_MARK_UNUSED = 'not used here';
export const IMPORT_MARK_UNKNOWN = 'not checkable';

/** Tooltip on an entry that points at its statement. */
export const IMPORTS_LINE_TOOLTIP = 'Open the import statement this came from.';

/** The twin section's own sentences. */
export const IMPORTS_SECTION_EMPTY = IMPORTS_EMPTY;

export const IMPORTS_SECTION_NOTE = IMPORTS_SOURCE_NOTE;

/** How the group's own tally reads, under the entries. */
export function importsTally(used: number, unused: number, unknown: number): string {
    return `${used} used in this file, ${unused} not used in it, ${unknown} that CodeAtlas cannot check.`;
}

// ---------------------------------------------------------------------------
// Der Flow-Erklaerer
// ---------------------------------------------------------------------------

export const EXPLAINER_LABEL = 'Explain flow';

export const EXPLAINER_TOOLTIP =
    'Draw the code this symbol reaches as a sequence, beside the steps it is made of, and walk them one at '
    + 'a time.';

/** Beschriftung des Kopfes, je nach Zustand. Der Kopf ist ein Knopf, kein Etikett. */
export function explainerHeadTooltip(open: boolean): string {
    return open ? 'Close the flow of this symbol.' : EXPLAINER_TOOLTIP;
}

/**
 * Die Kopfzeile des Erklaerers.
 *
 * Wortgleich mit `explainerTitle` des Referenzprojekts. Der Name des Symbols
 * und nicht der der Datei: der Walk beginnt an einem Symbol.
 */
export function explainerTitle(name: string): string {
    return `Flow from ${name}`;
}

/** Die Ueberschrift ueber der Schritt-Liste. */
export const EXPLAINER_STEPS_TITLE = 'Steps';

/*
 * Der Schliess-Knopf und der Escape-Hinweis stehen seit W8 nicht mehr hier.
 *
 * Der Erklaerer ist ein Reiter des Erklaeren-Bereichs geworden, und der Bereich
 * hat genau einen Knopf zum Zuklappen und genau einen Escape-Hinweis. Zwei
 * Knoepfe uebereinander, von denen einer die Flaeche und einer den Reiter
 * zumacht, waeren die Sorte Verdopplung, die dieser Zyklus abgeschafft hat.
 */

export const EXPLAINER_PREV_LABEL = 'Previous';
export const EXPLAINER_NEXT_LABEL = 'Next';
export const EXPLAINER_PREV_TOOLTIP = 'Go back one step.';
export const EXPLAINER_NEXT_TOOLTIP =
    'Go on one step: the editor follows and the diagram lights up.';

/**
 * Die Position im Stepper. Immer beide Zahlen: "3" allein sagt nichts.
 *
 * "n of m" und nicht "n / m", wortgleich mit dem Referenz-Explainer: der
 * Schraegstrich liest sich wie ein Bruch, und ein Standort ist kein Anteil.
 */
export function explainerPosition(step: number, total: number): string {
    return `${step} of ${total}`;
}

/** Vor dem ersten Schritt. Kein "0 / 8", das waere ein Schritt, den es nicht gibt. */
export function explainerNotStarted(total: number): string {
    return `${countOf(total, 'step', 'steps')}`;
}

/**
 * Gesagt, wenn ein Schritt im Kasten nichts anleuchtet.
 *
 * Ein erhobener Fehlertyp und eine Umgebungslesung haben in einer Folge von
 * Aufrufen keine eigene Lebenslinie. Der Editor folgt trotzdem; der Kasten sagt
 * warum er still bleibt, statt irgendeinen Pfeil zu faerben.
 */
export const EXPLAINER_NO_DIAGRAM_HIT =
    'This step is not an arrow in the picture: the index recorded it as an error path or an environment read, and neither has a lifeline of its own in a sequence of calls. The code is still opened.';

/** Ueber dem Kasten: was die Spalten sind. */
export const EXPLAINER_PARTICIPANTS_LABEL = 'files';

/** Ueber der Pfeilliste. */
export const EXPLAINER_ARROWS_LABEL = 'calls';

/** Gesagt, wenn der Walk gar keinen Halt hergab. */
export const EXPLAINER_NO_STEPS =
    'The index recorded no calls out of this symbol, so there is no flow to walk.';

/**
 * Der erste der zwei Ehrlichkeits-Absaetze, wortgleich aus dem
 * Referenz-Explainer (EXPLAINER_SOURCE_NOTE).
 *
 * Er traegt die eine Behauptung, an der die ganze Flaeche haengt: das Bild
 * links und die Liste rechts sind zwei Lesungen desselben Walks. Der zweite
 * Absatz ist {@link PSEUDOCODE_SOURCE_NOTE}, der schon ueber dem Block steht.
 *
 * Seit W8b steht er nicht mehr als Absatz unter dem Bild, sondern hinter dem
 * Fragezeichen daneben. Warum, steht an {@link EXPLAINER_HONESTY_SHORT}.
 */
export const EXPLAINER_SOURCE_NOTE =
    'The diagram and the list are two readings of one walk: one lifeline per file, one line per call the '
    + 'index reported. Neither shows a call the index did not resolve.';

/**
 * Der EINE Satz, der unter dem Bild stehenbleibt.
 *
 * ## Was hier gekuerzt wurde, und was ausdruecklich nicht
 *
 * Unter dem Diagramm standen bis W8b vier Absaetze mit zusammen 954 Zeichen.
 * Drei davon (EXPLAINER_SOURCE_NOTE mit 172, PSEUDOCODE_SOURCE_NOTE mit 265,
 * EXPLAINER_GENERATED_NOTE mit 174) sagen in drei Anlaeufen DIESELBE Sache:
 * dieses Bild zeigt genau das Gemeldete und kann unvollstaendig sein. Der
 * vierte (explainerCappedNote, 343) war der einzige mit Zahlen und stand am
 * Ende, wo ihn nach drei Absaetzen niemand mehr liest.
 *
 * Gekuerzt wird die WIEDERHOLUNG und nicht die Aussage. Nichts davon ist
 * verschwunden:
 *
 *  - die drei Herkunftssaetze stehen hinter dem Fragezeichen neben dem Bild,
 *    also in dem Idiom, das dieses Produkt fuer Herkunft schon hat (der
 *    Beleg-Knopf am Twin),
 *  - die Zahlen des Walks stehen als Beschriftung am RAND des Bildes, dort wo
 *    die Grenze gilt,
 *  - und dieser eine Satz bleibt darunter stehen.
 *
 * Eine Ehrlichkeit, die als Textwand erscheint, wird ueberlesen wie ein
 * Cookie-Banner und erreicht damit das Gegenteil ihres Zwecks. Das ist der
 * ganze Grund dieser Aenderung, und es ist kein Zurueckdrehen einer
 * Ehrlichkeitsregel: der Leser bekommt dieselben Saetze, nur nicht alle drei
 * gleichzeitig ins Gesicht.
 */
export const EXPLAINER_HONESTY_SHORT =
    'Neither the picture nor the list shows a call the index did not resolve.';

/**
 * Steht statt {@link EXPLAINER_HONESTY_SHORT}, wenn der Walk unaufgeloeste
 * Aufrufe wirklich melden konnte.
 *
 * "at least" ist keine Bescheidenheitsformel, sondern die Wahrheit ueber die
 * Messung: gezaehlt werden die Symbole, die der Index GENANNT und nicht
 * aufgeloest hat. Ein Aufruf, den er ganz ohne qualifizierten Namen meldet,
 * faellt schon im Walk heraus (src/provider/closure.ts) und ist hier nicht mehr
 * zaehlbar. Eine glatte Zahl waere an dieser Stelle die genauere Formulierung
 * einer ungenaueren Messung.
 */
export function explainerUnresolvedNote(count: number): string {
    return `At least ${countOf(count, 'call', 'calls')} the index did not resolve `
        + `${count === 1 ? 'is' : 'are'} missing from the picture and from the list.`;
}

/**
 * Der ZWEITE Satz unter dem Bild: was jenseits des Kastens liegt.
 *
 * Woertlich der letzte Satz von {@link explainerCappedNote}, und nur er. Die
 * beiden Zahlen davor stehen seit W8b als Beschriftung am Rand des Bildes
 * ({@link explainerWalkBound}), also dort, wo die Grenze gilt; was der Satz
 * darueber hinaus sagt, sagt kein anderer: dass der Kasten eine Ebene tief ist
 * und die naechste nicht mehr zeigt. Gekuerzt ist damit die Wiederholung der
 * Zahlen und nicht die Aussage.
 *
 * Er ist ausserdem der Grund, aus dem der Block ZWEI Absaetze behaelt statt
 * einem: der Beweislauf von W5c misst seit seiner eigenen Abnahme, dass unter
 * dem Bild zwei ehrliche Absaetze stehen, und ein Zyklus, der eine
 * Ehrlichkeitszusage eines frueheren aufloest, muesste dafuer einen besseren
 * Grund haben als "kuerzer". Der Contract von W8b nennt als Grenze 400 Zeichen
 * und hoechstens zwei Absaetze; beide Saetze zusammen sind 142.
 */
export const EXPLAINER_BEYOND_BOX =
    'What those calls reach in turn is one hop further than this box draws.';

/** Was das Fragezeichen neben dem Bild sagt, wenn man es beruehrt. */
export const EXPLAINER_PROVENANCE_TOOLTIP = 'Where this picture comes from.';

/**
 * Die Beschriftung am Rand des Bildes: wie weit der Walk ging und wie viel er
 * behalten durfte.
 *
 * Kurz, weil sie IM Bild steht und nicht darunter. Der lange Satz dazu
 * ({@link explainerCappedNote}) ist damit nicht weg, er ist nur nicht mehr der
 * vierte Absatz einer Textwand: er steht hinter dem Fragezeichen daneben.
 */
export function explainerWalkBound(depth: number, cap: number): string {
    return `walk: ${countOf(depth, 'hop', 'hops')}, at most ${countOf(cap, 'symbol', 'symbols')}`;
}

/**
 * Gesagt, weil das Bild zusammengesetzt und nicht gezeichnet wurde.
 *
 * Dieselbe Ehrlichkeit wie die Generated-Note ueber der erzeugten Prosa
 * (PROSE_GENERATED_NOTE): eine Abbildung, die aussieht, als haette jemand sie
 * entworfen, muss sagen, dass sie aus einem Index gerechnet ist.
 */
export const EXPLAINER_GENERATED_NOTE =
    'Nobody drew this picture. CodeAtlas laid it out from what the index recorded for this walk, so it can '
    + 'be incomplete about the code and exact about what was reported.';

/** Die Notiz im Bild, wenn eine Kante in eine schon besuchte Kette zurueckfuehrt. */
export function explainerCycleNote(count: number): string {
    return `${countOf(count, 'arrow closes', 'arrows close')} a chain the walk had already been through; `
        + 'those are drawn dashed and not followed further.';
}

/**
 * Die Notiz im Bild, wenn eine Grenze Pfeile weggelassen hat.
 *
 * Die Deckel kommen als Zahlen herein und stehen nicht hier: sie gehoeren
 * flow-model.ts, und diese Datei nennt sie nur. Eine eigene Kopie waere eine
 * Zahl, die eines Tages eine andere Grenze beschreibt als die, die gegriffen
 * hat.
 */
export function explainerOmittedNote(
    count: number,
    participantCap: number,
    interactionCap: number,
): string {
    return `${countOf(count, 'call is', 'calls are')} not drawn: the picture keeps at most `
        + `${participantCap} lifelines and ${interactionCap} arrows.`;
}

/** Gesagt, wenn die Closure gar keinen Aufruf hergab. */
export const EXPLAINER_EMPTY =
    'The index records no call out of this symbol, so there is no sequence to draw. That is a statement about the index, not about the code.';

/** Gesagt, wenn der Walk an einer Grenze aufgehoert hat. */
export function explainerCappedNote(depth: number, cap: number): string {
    return `The picture shows the calls this symbol makes itself: the walk went ${countOf(depth, 'hop', 'hops')} and kept at most ${countOf(cap, 'symbol', 'symbols')}. What those calls reach in turn is one hop further than this box draws.`;
}

/** Beschriftung des Umschalters auf die Pseudocode-Ansicht. */
export const PSEUDOCODE_TAB_LABEL = 'pseudocode';
export const PSEUDOCODE_TAB_TOOLTIP =
    'Show the facts the index recorded for this symbol as a numbered block, with what its file pulls in.';

/** Beschriftung des Umschalters zurueck auf den Twin-Koerper. */
export const TWIN_TAB_LABEL = 'facts';
export const TWIN_TAB_TOOLTIP = 'Back to the panel.';

/** Steht statt des Kastens, solange der Walk noch laeuft. */
export const FLOW_LOADING = 'Reading what this symbol reaches.';

/**
 * Steht statt des Kastens, wenn der Walk nichts hergab.
 *
 * Ein Satz ueber diesen Versuch und nicht ueber den Code: ein Symbol, das der
 * Index nicht kennt, und ein Backend, das nicht antwortet, sind beides Gruende,
 * hier nichts zu zeigen, und keiner davon heisst "dieses Symbol ruft nichts".
 */
export const FLOW_UNAVAILABLE =
    'CodeAtlas has no walk for this symbol right now. Place the caret in an indexed function, or reindex the workspace.';
