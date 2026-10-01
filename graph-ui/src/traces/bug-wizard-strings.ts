/*
 * Herkunft: die Saetze der Schritte 1 bis 4 stammen aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/strings.ts
 * (Abschnitt BUG_WIZARD_*). Gleicher Urheber (Bernhard Jackiewicz). Woertlich
 * uebernommen wurde alles, was hier dasselbe bedeutet.
 *
 * Zwei Gruppen sind bewusst anders formuliert, beide unten am Ort begruendet:
 *
 * - Der leere Zustand nennt ein anderes Kommando. Dort importiert eine
 *   Menue-Aktion eine Datei in den eigenen Trace-Speicher; hier gibt es keinen
 *   eigenen Speicher, und Laeufe kommen ueber `ingest_traces` der CLI in die
 *   Engine. Ein abgeschriebener Satz waere eine Anleitung, die nicht
 *   funktioniert.
 * - Die zweite Divergenz-Liste heisst nicht "Observed, not in the index". Warum
 *   nicht, steht im Kopf von bug-paths.ts: dieses Backend gibt eine Beobachtung
 *   nur an einer Kante zurueck, die der Index auch kennt, also waere die
 *   Ueberschrift des Vorbilds hier eine Aussage ueber etwas, das diese Liste
 *   gar nicht sehen koennte. Die Zeile selbst sagt danach je Fall, was der
 *   Index ueber genau diesen Aufruf weiss.
 */

/** Wie die Flaeche heisst und was sie vergleicht. */
export const BUG_WIZARD_TITLE = 'Hunt a bug';

/**
 * Wie das [a]tlas-Menue den Weg hierher nennt. Kleingeschrieben wie die anderen
 * Eintraege, und seit dem 2026-08-29 mit seinem Buchstaben (Audit-Befund 12).
 *
 * Aus "hunt a bug" wurde "[b]ug hunt", weil das Kuerzel der Anfang des Wortes
 * sein muss: `[h]unt a bug` haette den Buchstaben h belegt, und h ist in dieser
 * Oberflaeche nichts. Der Buchstabe b ist der, den jemand sucht, der einen Bug
 * jagt.
 */
export const BUG_WIZARD_MENU_LABEL = '[b]ug hunt';

export const BUG_WIZARD_SUBLINE =
    'Compare the path the index expects into a symbol with the path a recording took.';

export const BUG_WIZARD_STEP_TARGET = 'Choose the symbol';
export const BUG_WIZARD_STEP_STATIC = 'Expected path';
export const BUG_WIZARD_STEP_OBSERVED = 'Observed path';
export const BUG_WIZARD_STEP_DIVERGENCE = 'Where they differ';

export const BUG_WIZARD_NO_PROJECT = 'No indexed project, so there is no path to walk.';
export const BUG_WIZARD_NO_TARGET =
    'No symbol chosen yet. Put the caret in a function, or search for one.';
export const BUG_WIZARD_CHANGE_TARGET = 'Change symbol';
export const BUG_WIZARD_CHANGE_TOOLTIP = 'Search the index for the symbol this bug is about.';
export const BUG_WIZARD_BUSY = 'Reading the index, the traces and the ranked walks.';
export const BUG_WIZARD_FAILED = 'The paths into this symbol could not be read.';
export const BUG_WIZARD_CLOSE = '[esc] close';

/** Said above the expected chains, so nobody reads them as a runtime claim. */
export const BUG_WIZARD_STATIC_NOTE =
    'Read from the index: who calls this symbol, and who calls them. Nothing here says any of it ran.';

/** The label the reference puts on the reading, kept because it is the point. */
export const BUG_WIZARD_STATIC_ORIGIN = 'from the index';

/** Said when the index records nothing calling the symbol at all. */
export const BUG_WIZARD_STATIC_EMPTY =
    'The index records nothing calling this symbol, so there is no expected path into it.';

/** Said when the walk stopped at its bound rather than at an entry point. */
export function bugWizardTruncated(depth: number, chains: number): string {
    return `The walk stopped after ${depth} ${depth === 1 ? 'hop' : 'hops'} or ${chains} chains `
        + 'with callers still to follow, so a chain shown here may be a floor rather than the whole way in.';
}

/** The marker on a chain head that nothing calls. */
export const BUG_WIZARD_ENTRY_BADGE = 'entry point';
export const BUG_WIZARD_ENTRY_TOOLTIP =
    'The index records nothing calling this symbol, so the chain starts here.';

/**
 * The honest empty state, which is the screen most readers meet first.
 *
 * It has to do three things at once: refuse to claim anything about runtime,
 * say what a recording is, and say how to bring one in. A panel that only did
 * the first would be honest and useless.
 */
export const BUG_WIZARD_NO_TRACES =
    'No observed call came back for this symbol. CodeAtlas does not watch code while it runs; '
    + 'it reads back what somebody handed the analysis backend.';

export const BUG_WIZARD_NO_TRACES_HOW =
    'Hand it a run with the CLI, against the same HOME the server reads. The tool is not on /rpc, '
    + 'so this page cannot do it for you:';

/** Das Kommando, mit dem Namen des Projekts darin. */
export function bugWizardIngestCommand(project: string): string {
    return `echo '{"project":"${project}","label":"my-run","traces":[`
        + '{"path":["<caller qualified name>","<callee qualified name>"],"count":1}'
        + "]}' | codebase-memory-mcp cli ingest_traces";
}

export const BUG_WIZARD_NO_TRACES_FORMAT =
    'One entry is either {"path": [...]} with 2 to 256 qualified names, adjacent ones being caller '
    + 'and callee, or a bare {"caller": "...", "callee": "..."}. "count" and "label" are optional. '
    + 'Only pairs whose both ends are in the index are stored; up to ten unresolved names come back '
    + 'so a recorder can be fixed rather than silently losing data.';

/**
 * Wo eine Beobachtung wieder lesbar ist, und wo nicht.
 *
 * Der Satz gehoert in den leeren Zustand, weil er erklaert, warum diese Flaeche
 * moeglicherweise nichts zeigt, obwohl jemand einen Lauf eingespielt hat. Ohne
 * ihn wuerde ein Leser die Abwesenheit fuer eine Aussage ueber seinen Lauf
 * halten.
 */
export const BUG_WIZARD_NO_TRACES_WHERE =
    'The backend hands an observation back only where it also records the call, so a recorded call '
    + 'the index has no relation for is stored and cannot be read again through /api/trace or /api/flow.';

/** Said above the observed chains, so nobody reads them as a reading of the index. */
export const BUG_WIZARD_OBSERVED_NOTE =
    'Read back from the analysis backend, along the calls the index connects. CodeAtlas did not '
    + 'observe any of this itself.';

/** How many times an observed call was seen, and under which run. */
export function bugWizardObservedCount(count: number, label: string): string {
    const times = `observed ${count} ${count === 1 ? 'time' : 'times'}`;
    return label.length === 0 ? times : `${times}, run "${label}"`;
}

export function bugWizardLastSeen(lastSeen: string): string {
    return lastSeen.length === 0 ? '' : `last seen ${lastSeen}`;
}

/** Wie viele der gereihten Ablaeufe gelesen wurden, wenn es nicht alle waren. */
export function bugWizardFlowsCapped(read: number): string {
    return `Only the first ${read} ranked walks were read, so an observation on a lower ranked one is not on this page.`;
}

export const BUG_WIZARD_STATIC_ONLY_LABEL = 'Expected, never observed';
export const BUG_WIZARD_STATIC_ONLY_NOTE =
    'The index records these calls on the way in, and nothing came back saying any of them ran.';

export const BUG_WIZARD_RUNTIME_ONLY_LABEL = 'Observed, not on the expected chains';
export const BUG_WIZARD_RUNTIME_ONLY_NOTE =
    'A recording holds these calls and none of them is on a chain into the symbol above.';

/** Was eine Zeile der zweiten Liste ueber den Index sagt. Drei Faelle, drei Saetze. */
export const BUG_WIZARD_EDGE_IN_INDEX =
    'the index records this call, it is simply not on a way in';
export const BUG_WIZARD_EDGE_NOT_IN_INDEX =
    'the index records no such call at all';
export const BUG_WIZARD_EDGE_UNASKED =
    'the index was not asked: no qualified name came back for one end';

export const BUG_WIZARD_NO_DIVERGENCE =
    'Every observed call is on a chain the index draws, and every expected call was observed.';

/** The verb between the two ends of an edge, so the direction is on screen. */
export const BUG_WIZARD_EDGE_VERB = 'calls';

/** The same edge as one sentence, for the row's tooltip and for a screen reader. */
export function bugWizardEdgeLabel(from: string, to: string): string {
    return `${from} ${BUG_WIZARD_EDGE_VERB} ${to}`;
}

/** Tooltip on a hop, which is a live lookup and not a stored reference. */
export function bugWizardHopTooltip(name: string): string {
    return `Open ${name}. The symbol is looked up in the index at the moment you click, never remembered.`;
}
