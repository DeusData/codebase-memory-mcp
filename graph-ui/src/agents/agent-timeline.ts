/**
 * Der Zeitstrahl unten: eine Spur je Akteur, ein Strich je Ereignis.
 *
 * ## Was er ist
 *
 * Eine Ablesung und keine Zusammenfassung. Jeder Strich ist genau ein Ereignis,
 * an der Stelle, an der seine Zeit im Fenster liegt, in der Farbe seines
 * Akteurs. Es gibt keine Glaettung, keine Balken je Sekunde und keine Kurve:
 * eine Kurve waere eine Aussage darueber, wie dicht die Arbeit "eigentlich"
 * war, und dieses Fenster weiss nur, wann ein Werkzeug fertig war.
 *
 * ## Die drei Lagen, und warum sie verschieden heissen
 *
 *  - **live**: das Fenster endet jetzt und wandert mit.
 *  - **paused**: das Fenster endet dort, wo der Leser angehalten hat. Die
 *    Ereignisse laufen weiter ein und stehen nach dem Fortsetzen da; angehalten
 *    ist das NACHLAUFEN und nicht das Zuhoeren. Ein Pausenknopf, der die Quelle
 *    abhaengt, waere eine Luecke in der Aufzeichnung, die niemand angefordert
 *    hat.
 *  - **replay**: der Leser hat auf eine Stelle geklickt und sieht den Zustand
 *    von damals. Das ist die gefaehrlichste der drei Lagen, denn ein alter
 *    Zustand sieht aus wie der jetzige; sie ist darum die einzige, die die
 *    ganze Ansicht kennzeichnet.
 */

import type { WorkKind } from './agent-event';

/**
 * Die Fenster, die der Zeitstrahl kennt. `0` heisst: alles, was behalten wurde.
 *
 * Dieselben vier wie im Designbild (60s, 5m, 15m, unbegrenzt).
 */
export const TIMELINE_WINDOWS: readonly number[] = [60000, 300000, 900000, 0];

/**
 * Ab welcher Breite der Zeitstrahl ueberhaupt gezeigt wird, in Pixeln.
 *
 * Gemessen und nicht an einen Modus gebunden: in dem 441 Pixel breiten Panel
 * dieses Layouts waere eine Spur ueber fuenfzehn Minuten ein Strich, in dem
 * kein Ereignis mehr von seinem Nachbarn zu unterscheiden ist. Sechshundert
 * Pixel tragen bei fuenfzehn Minuten eine Aufloesung von 1.5 Sekunden je Pixel;
 * darunter waere die Anzeige eine Behauptung ueber eine Genauigkeit, die das
 * Bild nicht hat.
 */
export const TIMELINE_MIN_WIDTH = 600;

/** Wie viele Zeilen der Ereignis-Ticker im Vollbild traegt. */
export const TICKER_LIMIT = 6;

/** Ein Strich auf einer Spur. */
export interface TimelineTick {
    ts: number;
    /** Wo er im Fenster liegt, zwischen 0 (Anfang) und 1 (Ende). */
    at: number;
    kind: WorkKind;
}

/** Eine Spur: ein Akteur und seine Striche. */
export interface TimelineTrack {
    id: string;
    name: string;
    color: string;
    letter: string;
    you: boolean;
    idle: boolean;
    ticks: readonly TimelineTick[];
    /** Wie viele Ereignisse dieses Akteurs im Fenster liegen. */
    count: number;
}

/** In welcher Lage der Zeitstrahl steht. */
export type TimelineMode = 'live' | 'paused' | 'replay';

/** Der ganze Zeitstrahl. */
export interface Timeline {
    mode: TimelineMode;
    /** Der Anfang des Fensters, in Millisekunden seit 1970. */
    from: number;
    /** Sein Ende. */
    to: number;
    windowMs: number;
    tracks: readonly TimelineTrack[];
    /** Wie viele Striche insgesamt darauf stehen. */
    ticks: number;
}

/** Was ein Akteur zum Zeitstrahl beitraegt. */
export interface TimelineActor {
    id: string;
    name: string;
    color: string;
    letter: string;
    you: boolean;
    idle: boolean;
    firstTs: number;
    events: readonly { ts: number; kind: WorkKind }[];
}

export interface TimelineInput {
    actors: readonly TimelineActor[];
    /** Die Gegenwart. */
    now: number;
    windowMs: number;
    /** Wo der Leser angehalten hat, wenn er angehalten hat. */
    pausedAt?: number | undefined;
    /** Wohin der Leser gesprungen ist, wenn er gesprungen ist. */
    replayAt?: number | undefined;
}

/**
 * Den Zeitstrahl bauen.
 *
 * Das Ende des Fensters ist die Gegenwart, der Haltepunkt oder der
 * Sprungpunkt, in dieser Reihenfolge: ein Sprung in die Vergangenheit schlaegt
 * eine Pause, weil er die staerkere Aussage ist.
 */
export function buildTimeline(input: TimelineInput): Timeline {
    const mode: TimelineMode = input.replayAt !== undefined
        ? 'replay'
        : input.pausedAt !== undefined ? 'paused' : 'live';
    const to = input.replayAt ?? input.pausedAt ?? input.now;
    const earliest = input.actors.reduce(
        (min, actor) => Math.min(min, actor.firstTs),
        Number.POSITIVE_INFINITY,
    );
    const span = input.windowMs > 0
        ? input.windowMs
        : Math.max(1000, to - (Number.isFinite(earliest) ? earliest : to - 1000));
    const from = to - span;

    let ticks = 0;
    const tracks = input.actors.map((actor) => {
        const own: TimelineTick[] = [];
        for (const event of actor.events) {
            if (event.ts < from || event.ts > to) {
                continue;
            }
            own.push({
                ts: event.ts,
                at: span === 0 ? 1 : Number(((event.ts - from) / span).toFixed(6)),
                kind: event.kind,
            });
        }
        ticks += own.length;
        return {
            id: actor.id,
            name: actor.name,
            color: actor.color,
            letter: actor.letter,
            you: actor.you,
            idle: actor.idle,
            ticks: own,
            count: own.length,
        };
    });

    return { mode, from, to, windowMs: span, tracks, ticks };
}

/** Die Zeit an dieser Stelle des Strahls. `fraction` liegt zwischen 0 und 1. */
export function timeAtFraction(timeline: Timeline, fraction: number): number {
    const clamped = fraction <= 0 ? 0 : fraction >= 1 ? 1 : fraction;
    return Math.round(timeline.from + clamped * timeline.windowMs);
}
