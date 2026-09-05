/**
 * Was aus einem Strom von Ereignissen wird: Akteure, ihre letzten Schritte, und
 * die Zahlen, die im Kopf des Instruments stehen.
 *
 * Alles hier ist eine ZAEHLUNG. Es gibt keinen Fortschritt, keine Bewertung und
 * keine Vermutung darueber, was ein Agent gerade denkt: was dasteht, ist die
 * Zahl der Ereignisse, ihre Zeit, ihre Reihenfolge und der Ort, den sie nennen.
 * Ein Feld, das nicht aus einem Ereignis kommt, gibt es nicht.
 *
 * ## Wer ein Akteur ist
 *
 * Der Anzeigename. Nicht der Lauf: derselbe Agent, neu gestartet, ist derselbe
 * Agent mit einem zweiten Lauf, und zwei Koerper fuer ihn waeren zwei Wesen, wo
 * eines arbeitet. Die Laeufe werden trotzdem einzeln gefuehrt, denn die
 * Reihenfolge gilt je Lauf.
 *
 * ## Wie eine Luecke entsteht
 *
 * `seq` zaehlt je Lauf fortlaufend. Kommt nach 6 die 9, fehlen zwei. Das wird
 * gezaehlt und gemeldet, statt eine lueckenlose Geschichte zu zeichnen.
 *
 * Das ERSTE Ereignis eines Laufs zaehlt nie als Luecke, auch wenn es die Nummer
 * 40 traegt: dann hat die Oberflaeche spaeter zugehoert, und das ist keine
 * verlorene Nachricht. Was dasteht, ist die Nummer, bei der sie eingestiegen
 * ist.
 */

import type { AgentEvent, WorkKind } from './agent-event';
import { workKindOf } from './agent-event';

/** Wie viele Ereignisse je Akteur behalten werden. */
export const ACTOR_EVENT_CAP = 80;

/** Wie viele Sekunden der Aktivitaetsstreifen zeigt. */
export const STRIP_SECONDS = 30;

/** Das Fenster, ueber das die Ereignisrate gerechnet wird. */
export const RATE_WINDOW_MS = 60000;

/**
 * Wie weit ein Ereignis der Uhr dieses Fensters vorauslaufen darf.
 *
 * Die Uhr des Instruments tickt im Sekundentakt; ein Ereignis, das der Leser
 * selbst ausloest, entsteht zwischen zwei Ticks und ist dann fuer den Bruchteil
 * einer Sekunde "in der Zukunft". Zwei Sekunden Spielraum decken das ab, ohne
 * dass eine wirklich falsche Zeitangabe durchrutscht.
 */
export const CLOCK_SLACK_MS = 2000;

/**
 * Wie lange nach seinem letzten Ereignis ein Akteur noch im Bild steht.
 *
 * Drei Minuten, und die Zahl ist eine Abwaegung und keine Messung: kuerzer, und
 * ein Agent, der eine lange Datei liest, verschwindet mitten in der Arbeit;
 * laenger, und der Graph traegt Koerper von Agenten, die laengst fertig sind.
 * Wann ein Akteur zuletzt etwas getan hat, steht in seiner Zeile, damit die
 * Grenze nachvollziehbar ist statt unsichtbar.
 */
export const ACTIVE_WINDOW_MS = 180000;

/** Ein Lauf eines Akteurs, mit seiner Reihenfolge. */
export interface RunState {
    run: string;
    /** Die Nummer, mit der die Oberflaeche in diesen Lauf eingestiegen ist. */
    joinedAt: number;
    lastSeq: number;
    /** Wie viele Nummern zwischen zwei gesehenen Ereignissen fehlten. */
    missed: number;
}

/** Ein Akteur, so wie das Instrument ihn zeigt. */
export interface ActorState {
    /** Der Anzeigename. Er ist die Identitaet und damit die Quelle der Farbe. */
    id: string;
    name: string;
    /** Ob dieser Akteur die eigene Navigation des Lesers ist. */
    you: boolean;
    runs: readonly RunState[];
    /** Die behaltenen Ereignisse, aeltestes zuerst. */
    events: readonly AgentEvent[];
    /** Das letzte Ereignis. Es bestimmt Art und Ort. */
    last: AgentEvent;
    kind: WorkKind;
    /** Wann das erste behaltene Ereignis kam. */
    firstTs: number;
    lastTs: number;
    /** Wie viele Ereignisse dieser Akteur insgesamt geliefert hat. */
    count: number;
    /** Wie viele Nummern in seinen Laeufen fehlten. */
    missed: number;
}

/** Der ganze Zustand des Stroms. */
export interface AgentsState {
    actors: readonly ActorState[];
    /** Wie viele Ereignisse angekommen sind. */
    events: number;
    /** Wie viele fehlten. */
    missed: number;
    /** Die zuletzt gesehene Nummer je Lauf, fuer die Wiederaufnahme. */
    seen: ReadonlyMap<string, number>;
    /** Wie viele Zeilen die Bruecke als unlesbar gemeldet hat. */
    unreadable: number;
}

/** Der leere Zustand. Kein Akteur, keine Zahl, keine Behauptung. */
export function emptyAgentsState(): AgentsState {
    return { actors: [], events: 0, missed: 0, seen: new Map(), unreadable: 0 };
}

function withRun(runs: readonly RunState[], event: AgentEvent): {
    runs: RunState[];
    missed: number;
} {
    const found = runs.find((entry) => entry.run === event.run);
    if (found === undefined) {
        return {
            runs: [...runs, { run: event.run, joinedAt: event.seq, lastSeq: event.seq, missed: 0 }],
            missed: 0,
        };
    }
    const gap = event.seq > found.lastSeq + 1 ? event.seq - found.lastSeq - 1 : 0;
    return {
        runs: runs.map((entry) =>
            (entry.run === event.run
                ? { ...entry, lastSeq: Math.max(entry.lastSeq, event.seq), missed: entry.missed + gap }
                : entry)),
        missed: gap,
    };
}

/** Ein Ereignis in den Zustand aufnehmen. Rein, damit ein Test ihn nachbauen kann. */
export function withEvent(state: AgentsState, event: AgentEvent): AgentsState {
    const id = event.agent;
    const existing = state.actors.find((actor) => actor.id === id);
    const seen = new Map(state.seen);
    const previous = seen.get(event.run);
    if (previous === undefined || event.seq > previous) {
        seen.set(event.run, event.seq);
    }

    if (existing === undefined) {
        const actor: ActorState = {
            id,
            name: id,
            you: false,
            runs: [{ run: event.run, joinedAt: event.seq, lastSeq: event.seq, missed: 0 }],
            events: [event],
            last: event,
            kind: workKindOf(event.tool, event.detail),
            firstTs: event.ts,
            lastTs: event.ts,
            count: 1,
            missed: 0,
        };
        return {
            actors: [...state.actors, actor],
            events: state.events + 1,
            missed: state.missed,
            seen,
            unreadable: state.unreadable,
        };
    }

    const { runs, missed } = withRun(existing.runs, event);
    const events = [...existing.events, event].slice(-ACTOR_EVENT_CAP);
    // Das letzte Ereignis ist das mit der spaetesten Zeit und nicht das zuletzt
    // eingetroffene: bei einer Wiederaufnahme koennen aeltere Ereignisse
    // nachkommen, und ein Koerper, der dabei zurueckspringt, waere eine
    // Bewegung, die es nicht gab.
    const last = event.ts >= existing.lastTs ? event : existing.last;
    const updated: ActorState = {
        ...existing,
        runs,
        events,
        last,
        kind: workKindOf(last.tool, last.detail),
        firstTs: Math.min(existing.firstTs, event.ts),
        lastTs: Math.max(existing.lastTs, event.ts),
        count: existing.count + 1,
        missed: existing.missed + missed,
    };
    return {
        actors: state.actors.map((actor) => (actor.id === id ? updated : actor)),
        events: state.events + 1,
        missed: state.missed + missed,
        seen,
        unreadable: state.unreadable,
    };
}

/**
 * Die eigene Bewegung des Lesers aufnehmen.
 *
 * Sie laeuft durch dieselbe Ebene und dieselbe Zaehlung, und sie geht NICHT in
 * die Ereignisdatei: sie entsteht hier, in diesem Fenster, und die Bruecke
 * kennt nur eine Leserichtung. Der Akteur traegt darum ein eigenes Kennzeichen,
 * damit der Umschalter ihn von den Agenten trennen kann.
 */
export function withYouEvent(state: AgentsState, event: AgentEvent): AgentsState {
    const next = withEvent(state, event);
    return {
        ...next,
        actors: next.actors.map((actor) =>
            (actor.id === event.agent ? { ...actor, you: true } : actor)),
    };
}

/** Ob dieser Akteur gerade im Bild steht. */
export function isActive(actor: ActorState, now: number): boolean {
    return now - actor.lastTs <= ACTIVE_WINDOW_MS;
}

/** Die Akteure, die gerade im Bild stehen, neueste Bewegung zuerst. */
export function activeActors(state: AgentsState, now: number): ActorState[] {
    return state.actors
        .filter((actor) => isActive(actor, now))
        .sort((a, b) => b.lastTs - a.lastTs || (a.id < b.id ? -1 : 1));
}

/**
 * Der Aktivitaetsstreifen: je Sekunde die Zahl der Ereignisse.
 *
 * Aelteste Sekunde zuerst, damit der Streifen von links nach rechts laeuft wie
 * die Zeit. Ein Balken ohne Ereignis ist eine Null und kein Ziermuster: wo
 * nichts war, steht nichts.
 */
export function activityStrip(
    events: readonly AgentEvent[],
    now: number,
    seconds: number = STRIP_SECONDS,
): number[] {
    const bars = new Array<number>(seconds).fill(0);
    for (const event of events) {
        const age = now - event.ts;
        if (age >= seconds * 1000 || age < -CLOCK_SLACK_MS) {
            continue;
        }
        /*
         * Ein Ereignis, das der Uhr dieses Fensters um Sekundenbruchteile
         * VORAUS ist, gehoert in die juengste Sekunde und nicht ins Nichts.
         *
         * Die Uhr des Instruments tickt im Sekundentakt (AGENT_TICK_MS), und
         * die eigene Navigation des Lesers erzeugt ihr Ereignis zwischen zwei
         * Ticks. Es wegzulassen hiesse, einen Streifen zu zeigen, der "in den
         * letzten dreissig Sekunden ist nichts passiert" sagt, waehrend gerade
         * etwas passiert ist. Was weiter in der Zukunft liegt als
         * {@link CLOCK_SLACK_MS}, faellt weiter heraus: das waere kein
         * Taktversatz mehr, sondern eine Zeitangabe, der dieses Fenster nicht
         * folgen kann.
         */
        const index = age < 0 ? seconds - 1 : seconds - 1 - Math.floor(age / 1000);
        bars[index] = (bars[index] ?? 0) + 1;
    }
    return bars;
}

/**
 * Der Zustand, wie er zu einem frueheren Zeitpunkt aussah.
 *
 * Fuer den Sprung des Zeitstrahls in die Vergangenheit (W11b AC4). Es wird
 * NICHTS erfunden und nichts nachgerechnet: die Ereignisse hinter diesem
 * Zeitpunkt werden weggelassen, und was uebrig bleibt, ist genau das, was die
 * Ansicht damals gehabt haette. Ein Akteur, dessen Ereignisse alle spaeter
 * liegen, faellt weg, denn damals war er noch nicht da.
 *
 * Die Zaehler `events` und `missed` werden dabei aus den behaltenen Ereignissen
 * neu gezaehlt und nicht uebernommen: die Zahl im Kopf gehoert zum Bild
 * darunter, und eine Gesamtzahl aus der Gegenwart ueber einem Bild von damals
 * waere die stille Behauptung, damals sei schon alles gezaehlt gewesen.
 */
export function stateUntil(state: AgentsState, until: number): AgentsState {
    const actors: ActorState[] = [];
    let missed = 0;
    let events = 0;
    for (const actor of state.actors) {
        const kept = actor.events.filter((event) => event.ts <= until);
        if (kept.length === 0) {
            continue;
        }
        const last = kept.reduce((latest, event) => (event.ts >= latest.ts ? event : latest), kept[0] as AgentEvent);
        let gaps = 0;
        const bySeq = new Map<string, number>();
        for (const event of kept) {
            const known = bySeq.get(event.run);
            if (known !== undefined && event.seq > known + 1) {
                gaps += event.seq - known - 1;
            }
            if (known === undefined || event.seq > known) {
                bySeq.set(event.run, event.seq);
            }
        }
        missed += gaps;
        events += kept.length;
        actors.push({
            ...actor,
            events: kept,
            last,
            kind: workKindOf(last.tool, last.detail),
            firstTs: Math.min(...kept.map((event) => event.ts)),
            lastTs: last.ts,
            count: kept.length,
            missed: gaps,
        });
    }
    return { actors, events, missed, seen: state.seen, unreadable: state.unreadable };
}

/** Wie viele Ereignisse dieses Akteurs im Fenster vor `now` liegen. */
export function eventsInWindow(
    actor: ActorState,
    now: number,
    windowMs: number,
): number {
    let count = 0;
    for (const event of actor.events) {
        const age = now - event.ts;
        if (age >= 0 && age < windowMs) {
            count += 1;
        }
    }
    return count;
}

/** Wie viele Ereignisse in der letzten Minute ankamen. Gezaehlt, nicht geschaetzt. */
export function eventsPerMinute(state: AgentsState, now: number): number {
    let count = 0;
    for (const actor of state.actors) {
        for (const event of actor.events) {
            if (now - event.ts < RATE_WINDOW_MS && now - event.ts >= 0) {
                count += 1;
            }
        }
    }
    return count;
}

/**
 * Wie lange dieser Akteur schon an derselben Stelle ist.
 *
 * Gerechnet ueber die zusammenhaengende Kette der letzten Ereignisse mit
 * demselben Ort: wer viermal hintereinander dieselbe Datei bearbeitet, ist seit
 * dem ersten dieser vier dort und nicht erst seit dem letzten.
 */
export function timeAtPlace(actor: ActorState, now: number): number {
    const place = (event: AgentEvent): string => `${event.path}`;
    const current = place(actor.last);
    let since = actor.last.ts;
    for (let i = actor.events.length - 1; i >= 0; i -= 1) {
        const event = actor.events[i] as AgentEvent;
        if (place(event) !== current) {
            break;
        }
        since = event.ts;
    }
    return Math.max(0, now - since);
}

/**
 * Die zuletzt beruehrten Pfade, neuester zuerst, ohne Wiederholung.
 *
 * Die Wegzeile des Designbildes, und sie besteht aus Ereignissen: jeder Eintrag
 * ist ein Pfad, den ein Ereignis genannt hat. Was keinen Pfad nennt, steht
 * nicht darin.
 */
export function recentPaths(
    actor: ActorState,
    now: number,
    windowMs: number,
    limit = 6,
): string[] {
    const out: string[] = [];
    for (let i = actor.events.length - 1; i >= 0; i -= 1) {
        const event = actor.events[i] as AgentEvent;
        if (windowMs > 0 && now - event.ts > windowMs) {
            break;
        }
        if (event.path.length === 0 || out.includes(event.path)) {
            continue;
        }
        out.push(event.path);
        if (out.length >= limit) {
            break;
        }
    }
    return out;
}
