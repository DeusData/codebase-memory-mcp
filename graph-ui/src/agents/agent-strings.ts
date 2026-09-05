/**
 * Die Saetze des Instruments.
 *
 * Sie stehen hier und nicht im Katalog (src/i18n/messages.ts), aus demselben
 * Grund wie die Saetze der Galaxie und der Aenderungsansicht: der Katalog fuehrt
 * den Rahmen des Produkts, den jede Flaeche gleich benennt, und ein Fachgebiet
 * fuehrt seine eigenen Woerter neben sich. Was hier steht, sagt nur diese eine
 * Ebene.
 *
 * Eine Regel gilt fuer jeden Satz darin: **er sagt, was gezaehlt wurde, und
 * behauptet nichts darueber hinaus.** Kein Fortschritt, keine Prozentzahl, kein
 * "arbeitet an", kein "denkt nach". Ein Agent, der eine Datei geoeffnet hat, hat
 * eine Datei geoeffnet.
 */

import type { WorkKind } from './agent-event';

/** Was ein Buchstabe der Arbeit bedeutet, in einem Satz. */
export const WORK_KIND_TEXT: Readonly<Record<WorkKind, string>> = {
    read: 'read a file',
    write: 'changed a file',
    search: 'searched',
    test: 'ran a test command',
    other: 'ran a command the tool name does not classify',
};

/** Das kurze Wort, das in der Zeile steht. */
export const WORK_KIND_WORD: Readonly<Record<WorkKind, string>> = {
    read: 'reading',
    write: 'writing',
    search: 'searching',
    test: 'testing',
    other: 'command',
};

export const agentStrings = {
    title: 'LIVE AGENTS',
    panelLabel: 'live agents on the graph',

    /* ------------------------------------------------------------ der Kopf */

    agentCount: (count: number): string =>
        (count === 1 ? '1 actor' : `${count} actors`),
    perMinute: (count: number): string =>
        (count === 1 ? '1 event in the last minute' : `${count} events in the last minute`),
    orderIntact: 'order intact',
    orderMissed: (count: number): string =>
        (count === 1 ? '1 event missed' : `${count} events missed`),
    orderTitle:
        'every run numbers its events. A number that never arrived is counted here; it is not '
        + 'filled in and not drawn.',
    joinedAt: (run: string, seq: number): string => `joined run ${run} at number ${seq}`,

    /* ------------------------------------------------------ die drei Lagen */

    foldTitle: (open: boolean): string =>
        (open ? 'fold the instrument down to one line' : 'unfold the instrument'),
    fold: (open: boolean): string => (open ? 'fold' : 'unfold'),
    expand: (expanded: boolean): string => (expanded ? 'compact' : 'expand'),
    expandTitle: (expanded: boolean): string =>
        (expanded
            ? 'back to the compact instrument, so it explains the graph instead of covering it'
            : 'one card per actor: the path it walked and the events behind every number'),
    collapsedLine: (count: number): string =>
        (count === 0
            ? 'no actor on the graph'
            : count === 1 ? '1 actor on the graph' : `${count} actors on the graph`),

    /* ------------------------------------------------------- die Quelle */

    sourceOff: 'live mode is off, and nothing is asked',
    sourceOffDetail:
        'not one request goes to the bridge while this is off. Turn it on in the atlas menu with '
        + '[g] live agents, or type "live agents" in the command line.',
    sourceConnecting: 'connecting to the bridge',
    sourceNone: 'live mode is on and no bridge is answering',
    sourceNoneDetail:
        'this window has no backend and cannot start one. The bridge reads the event file and hands '
        + 'it on; without it there is no source, and an empty graph here would be the claim that '
        + 'nobody is working:',
    sourceConnected: (file: string): string => `reading ${file}`,
    sourceReplay: (file: string): string => `replaying ${file}`,
    replayNote:
        'these events were recorded earlier. Their spacing is the recorded spacing, shifted forward '
        + 'so the last one is now.',
    unreadableLines: (count: number): string =>
        (count === 1
            ? '1 line of the file was not readable JSON and was skipped'
            : `${count} lines of the file were not readable JSON and were skipped`),

    /* --------------------------------------------------------- die Zeilen */

    kindTitle: (kind: WorkKind, letter: string): string => `${letter}: ${WORK_KIND_TEXT[kind]}`,
    placeRange: (name: string, from: number, to: number): string => `${name}, lines ${from} to ${to}`,
    placeFile: (name: string): string => name,
    placeUncertain: (name: string): string => `${name} (no line range in the index)`,
    placeUncertainTitle:
        'the graph knows this node but carries no end line for it, so this says where the event is '
        + 'and not that it lies inside a range.',
    placeNone: 'not placeable on the graph',
    placeNoneTitle: (why: string): string => `${why}. The raw event stands below.`,
    ago: (seconds: number): string =>
        (seconds < 60
            ? `${Math.max(0, Math.round(seconds))}s`
            : `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`),
    hereFor: (spell: string): string => `here for ${spell}`,
    lastSeen: (spell: string): string => `last event ${spell} ago`,
    stripTitle: (seconds: number, count: number): string =>
        `${count} events in the last ${seconds} seconds, one bar per second, counted from the events `
        + 'themselves',
    testedTitle: (name: string): string =>
        `the command names ${name}, which the index knows. The dashed line goes there.`,
    testedUnknown: 'the command names no file this index knows, so there is no line to draw',

    /* ------------------------------------------------- die Selbstauskunft */

    intentPrefix: 'agent says:',
    intentTitle:
        'reported by the agent, not measured. This line exists because the event carried an intent '
        + 'field; without one there is no such line.',

    /* --------------------------------------------------- was nicht passt */

    unmappedTitle: (count: number): string =>
        (count === 1
            ? '1 event is not placeable on the graph'
            : `${count} events are not placeable on the graph`),
    unmappedIntro:
        'they stay here instead of disappearing: an event that falls out of the view would be the '
        + 'silent claim that it never happened.',
    unmappedRow: (tool: string, path: string, why: string): string =>
        `${tool} ${path.length > 0 ? path : '(no path)'}: ${why}`,

    /* ---------------------------------------------------- die Umschalter */

    filterLabel: 'show',
    filterYou: 'you',
    filterAgent: 'agents',
    filterBoth: 'both',
    filterTitle: (option: string): string =>
        (option === 'you'
            ? 'only your own navigation'
            : option === 'agent' ? 'only the agents' : 'your navigation and the agents'),
    youName: 'you',
    youPlace: 'the symbol you opened',

    follow: 'follow',
    followTitle: (on: boolean): string =>
        (on
            ? 'stop following: the camera stays where you put it'
            : 'the camera flies to the actor that moved last'),
    trails: 'trails',
    trailsTitle: (on: boolean): string =>
        (on
            ? 'hide the path row of each actor'
            : 'show the path row: the symbols each actor touched, newest first'),
    /*
     * Bis W11b hiess dieser Schalter "cinema". Der Nutzer hat ihn am 2026-08-30
     * benutzt und woertlich gesagt: "bin jetzt im cinema mode, sollte fullscreen
     * heissen." Er heisst jetzt so, und zwar an jeder Stelle: Schalter,
     * Beschriftung, Kommandozeile, Naht und gespeicherte Wahl.
     */
    fullscreen: 'fullscreen',
    fullscreenTitle: (on: boolean): string =>
        (on
            ? 'leave the full window and put the graph back into its panel. Escape does the same.'
            : 'the graph fills the window: bigger bodies, the instrument as a column at the edge, '
                + 'the timeline underneath.'),
    fullscreenScope:
        'fullscreen is the frame and not a tour: the camera goes where you send it, here as well, '
        + 'and it is still there when you leave.',
    windowLabel: 'window',
    windowOption: (ms: number): string =>
        (ms === 0 ? 'all kept' : ms < 60000 ? `${ms / 1000}s` : `${ms / 60000}m`),
    windowTitle: 'how far back the path row, the trail on the graph and the timeline reach',
    pathEmpty: 'no path yet: no event of this actor named a file',

    /* ------------------------------------------------- die Spur am Graphen */

    trailLegendTitle: 'agent trail',
    trailLegendDetail:
        'the dashed line behind an agent is the path it walked: the last symbols one actor touched, '
        + 'newest first, drawn under the real edges. It is NOT a relation in the code. The solid '
        + 'coloured lines come from the index; this one comes from events and fades with the window '
        + 'you picked.',

    /* -------------------------------------------------------- der Deckel */

    capLine: (drawn: number, total: number): string =>
        `${drawn} of ${total} bodies drawn: the layer stops at ${drawn}`,
    capTitle:
        'the cap is a promise about computing time, not about the truth: every actor stays in this '
        + 'list with all its numbers. What is missing is the body on the graph, and this line is '
        + 'here so the picture is never quietly incomplete.',

    /* -------------------------------------------------------- die Ruhe */

    idleTitle: (seconds: number): string =>
        `no event for ${Math.round(seconds)}s: this body stands still and stays pale until the next `
        + 'one. Nothing here is animated while nothing happens.',

    /* --------------------------------------------------------- der Puls */

    pulseTitle: (events: number, periodMs: number): string =>
        (events === 0
            ? 'no event in the last minute, so this body does not breathe'
            : `${events} events in the last minute: one breath every ${(periodMs / 1000).toFixed(1)}s. `
                + 'The pulse is the count, not decoration.'),

    /* ------------------------------------------------------ die Wiedergabe */

    followLine: (name: string, kind: string, place: string, lines: readonly number[]): string =>
        `${name} ${kind} ${place.length > 0 ? place : 'nothing this index knows'}`
        + (lines.length === 2 ? `, lines ${lines[0]} to ${lines[1]}` : ''),
    followLineTitle:
        'the event the camera just followed. Actor, kind of work, symbol and line range, all four '
        + 'read from the event and the index; nothing else is in this line.',

    /* ------------------------------------------------------ der Zeitstrahl */

    timelineLabel: 'timeline',
    timelineLive: 'LIVE',
    timelinePaused: 'PAUSED',
    timelineReplay: 'REPLAY',
    timelineLiveTitle: 'the window ends now and moves with it',
    timelinePauseTitle:
        'hold the window where it is. Events keep arriving and are all there when you let it run '
        + 'again: what stops is the scrolling, not the listening.',
    timelineResume: 'resume',
    timelinePause: 'pause',
    timelineReplayNote: (spell: string): string =>
        `showing the state of ${spell} ago, not now`,
    timelineReplayTitle:
        'a click on the timeline shows the state at that moment. Everything on the screen belongs to '
        + 'then, and it says so, because an old state looks exactly like the current one.',
    timelineBackToLive: 'back to live',
    timelineTrackTitle: (name: string, count: number): string =>
        `${name}: ${count === 1 ? '1 event' : `${count} events`} in this window, one tick each`,
    timelineEmpty: 'no event of this actor in this window',
    timelineNarrow: (width: number): string =>
        `the graph area is ${width} px wide; the timeline needs 600 to keep one tick apart from the `
        + 'next',

    /* ---------------------------------------------------------- der Ticker */

    tickerLabel: 'events',
    tickerLine: (name: string, kind: string, place: string, lines: readonly number[]): string =>
        `${name} ${kind} ${place.length > 0 ? place : 'nothing this index knows'}`
        + (lines.length === 2 ? `, lines ${lines[0]} to ${lines[1]}` : ''),
    tickerTitle:
        'the last events in plain words. One line per event, and every part of it is read from the '
        + 'event: who, what kind of work, which symbol, which lines.',

    /* ---------------------------------------------------------- die Ebene */

    layerOff: 'the agent layer is off in the settings, so nothing is drawn on the graph',
} as const;

/**
 * Das Etikett des Menuepunkts.
 *
 * Es traegt den ZUSTAND und nicht die Wirkung, wie `[l]ocal llm off` daneben:
 * "off" heisst "es ist aus" und nicht "hier ausschalten". Dieselbe Lesart wie
 * bei [a]tlas, dessen `data-state` ebenfalls die Lage faerbt.
 *
 * Der Buchstabe steht in Klammern am Anfang, weil AtlasChrome ihn dort
 * heraustrennt (splitMenuLabel) und ein Terminal keine Unterstreichung hat.
 */
export function agentsMenuLabel(on: boolean): string {
    return on ? '[g] live agents on' : '[g] live agents off';
}
