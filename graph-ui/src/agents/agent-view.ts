/**
 * Was das Instrument zeigt und was die Ebene zeichnet, EINMAL gerechnet.
 *
 * Beide brauchen dieselbe Antwort: welcher Akteur steht wo, in welcher Art von
 * Arbeit, seit wann, mit welcher Farbe. Zwei Rechnungen dafuer waeren zwei
 * Wahrheiten ueber dasselbe Bild, und die Stelle, an der sie auseinanderlaufen,
 * faellt niemandem auf: ein Koerper an einem Knoten und eine Zeile, die einen
 * anderen nennt.
 *
 * Was hier NICHT passiert: deuten. Jedes Feld kommt aus einem Ereignis oder aus
 * dem Index; die einzigen gerechneten Zahlen sind Zeitabstaende und Zaehlungen.
 */

import type { GraphNode } from '../galaxy/types';
import { WORK_KIND_LETTERS, workKindOf } from './agent-event';
import type { AgentEvent, WorkKind } from './agent-event';
import { agentColor, agentLetters } from './agent-colors';
import { buildPlacementIndex, placeEvent } from './agent-placement';
import type { Placement, PlacementIndex } from './agent-placement';
import {
    STRIP_SECONDS,
    activeActors,
    activityStrip,
    eventsInWindow,
    eventsPerMinute,
    recentPaths,
    timeAtPlace,
} from './agent-store';
import type { ActorState, AgentsState } from './agent-store';
import {
    DRAWN_BODIES_CAP,
    IDLE_MS,
    PULSE_WINDOW_MS,
    TRAIL_NODE_LIMIT,
    currentBursts,
    pulseOf,
    writeBurstsOf,
} from './agent-motion';
import type { Pulse, WriteBurst } from './agent-motion';
import { TICKER_LIMIT } from './agent-timeline';

/** Wer gezeigt wird. */
export type ActorFilter = 'you' | 'agent' | 'both';

/** Die drei Lagen des Instruments. */
export type HudSize = 'collapsed' | 'compact' | 'expanded';

/**
 * Wie weit die Wegzeile und die Spur zurueckreichen. `0` heisst: so weit wie
 * behalten wird.
 *
 * Seit W11b vier Stufen statt drei: das Designbild nennt 60s, 5m, 15m und
 * unbegrenzt, und die fuenfzehn Minuten fehlten. Sie sind die Stufe, in der man
 * sieht, ob jemand seit einer Viertelstunde um dieselben drei Dateien kreist.
 */
export const TRAIL_WINDOWS: readonly number[] = [60000, 300000, 900000, 0];

/** Hoechstens so viele Zeilen fuer das, was sich nicht verorten laesst. */
export const UNMAPPED_LIMIT = 4;

/** Hoechstens so viele Zeilen im kompakten Zustand. */
export const COMPACT_ROW_LIMIT = 3;

/** Ein Akteur, fertig fuer die Anzeige. */
export interface ActorView {
    id: string;
    name: string;
    you: boolean;
    color: string;
    /** Der Buchstabe am Koerper. Er unterscheidet die Akteure ohne die Farbe. */
    letter: string;
    kind: WorkKind;
    /** Der Buchstabe der Art (R, W, S, T, O). Er steht im Instrument. */
    kindLetter: string;
    placement: Placement;
    /** Der Knoten, den der Koerper umkreist. Fehlt, wenn nichts zu verorten war. */
    node: GraphNode | undefined;
    /** Der geprueften Bereich eines Testlaufs, wenn der Befehl ihn nennt. */
    testedNode: GraphNode | undefined;
    /** Die Knoten, deren Name das Suchmuster traegt. Nur beim Suchen. */
    ghostNodes: readonly GraphNode[];
    last: AgentEvent;
    lastTs: number;
    /** Wie lange dieser Akteur schon an derselben Stelle ist, in Millisekunden. */
    hereMs: number;
    count: number;
    missed: number;
    /** Ereignisse je Sekunde der letzten dreissig, aus den Ereignissen selbst. */
    strip: readonly number[];
    stripTotal: number;
    /**
     * Die Selbstauskunft des letzten Ereignisses. Leer, wenn keine da war.
     *
     * Leer heisst hier wirklich leer: es gibt keinen Ersatztext und keine
     * abgeleitete Absicht.
     */
    intent: string;
    /** Die zuletzt beruehrten Pfade, neuester zuerst. */
    paths: readonly string[];
    /** Wie lange das letzte Ereignis dieses Akteurs her ist, in Millisekunden. */
    sinceMs: number;
    /**
     * Ob dieser Akteur seit ueber einer Minute nichts geliefert hat.
     *
     * Dann steht sein Koerper still, atmet nicht, wird blass und rutscht im
     * Instrument nach unten. Er verschwindet nicht: sein Lauf laeuft weiter.
     */
    idle: boolean;
    /** Wie viele Ereignisse dieser Akteur im Pulsfenster geliefert hat. */
    recentEvents: number;
    /** Der Puls, aus genau dieser Zahl gerechnet. Ohne Ereignisse: keiner. */
    pulse: Pulse;
    /**
     * Die zuletzt besuchten Knoten, neuester zuerst.
     *
     * Der aktuelle Ort steht vorn, damit die Spur am Koerper anfaengt. Was
     * ausserhalb des Fensters liegt oder sich nicht verorten liess, steht nicht
     * darin: eine Spur durch einen Knoten, den kein Ereignis genannt hat, waere
     * ein Weg, den niemand gegangen ist.
     */
    trail: readonly GraphNode[];
    /** Die Schreib-Brueche, die gerade eine Welle tragen. */
    waves: readonly WriteBurst[];
    /** Ob dieser Koerper gezeichnet wird, oder ob der Deckel ihn zurueckhaelt. */
    drawn: boolean;
}

/** Ein Ereignis, das sich nicht verorten liess. */
export interface UnmappedEvent {
    ts: number;
    agent: string;
    tool: string;
    path: string;
    detail: string;
    why: string;
}

/** Eine Zeile des Ereignis-Tickers. Nur Gemessenes, in Worten. */
export interface TickerEntry {
    ts: number;
    actor: string;
    name: string;
    color: string;
    kind: WorkKind;
    tool: string;
    /** Der Name des getroffenen Knotens. Leer, wenn es keinen gab. */
    place: string;
    /** Der Zeilenbereich, wenn das Ereignis einen nannte. */
    lines: readonly number[];
    path: string;
}

/** Alles, was das Instrument und die Ebene brauchen. */
export interface AgentsView {
    /** Die gezeigten Akteure, nach dem Umschalter gefiltert. */
    actors: readonly ActorView[];
    /** Alle aktiven Akteure, ungefiltert. Der Kopf zaehlt an dieser Liste. */
    all: readonly ActorView[];
    unmapped: readonly UnmappedEvent[];
    events: number;
    missed: number;
    perMinute: number;
    unreadable: number;
    /** Der Deckel, ueber dem keine Koerper mehr gezeichnet werden. */
    cap: number;
    /** Wie viele Akteure der Deckel gerade zurueckhaelt. */
    capped: number;
    /** Die letzten Ereignisse im Klartext, neuestes zuerst. */
    ticker: readonly TickerEntry[];
}

export interface AgentsViewInput {
    state: AgentsState;
    nodes: readonly GraphNode[];
    now: number;
    filter: ActorFilter;
    trailWindowMs: number;
    /** Der Index, wenn der Aufrufer ihn schon hat. Sonst wird er gebaut. */
    index?: PlacementIndex;
    /** Hoechstens so viele Koerper werden gezeichnet. */
    cap?: number;
    /** Wie viele Zeilen der Ticker traegt. */
    tickerLimit?: number;
}

function nodeById(nodes: readonly GraphNode[]): Map<number, GraphNode> {
    const out = new Map<number, GraphNode>();
    for (const node of nodes) {
        out.set(node.id, node);
    }
    return out;
}

/**
 * Die zuletzt besuchten Knoten dieses Akteurs, neuester zuerst.
 *
 * Gefragt wird jedes behaltene Ereignis im Fenster, von hinten nach vorn; was
 * sich verorten laesst und noch nicht in der Liste steht, kommt dazu, bis der
 * Deckel erreicht ist. Ein Knoten steht damit genau einmal darin, auch wenn ein
 * Agent dreimal an dieselbe Stelle zurueckgekommen ist: die Spur ist ein Weg
 * und keine Strichliste.
 */
function trailOf(
    actor: ActorState,
    now: number,
    windowMs: number,
    index: PlacementIndex,
    byId: Map<number, GraphNode>,
): GraphNode[] {
    const out: GraphNode[] = [];
    const seen = new Set<number>();
    for (let i = actor.events.length - 1; i >= 0; i -= 1) {
        const event = actor.events[i] as AgentEvent;
        if (windowMs > 0 && now - event.ts > windowMs) {
            break;
        }
        if (event.ts > now) {
            continue;
        }
        const placement = placeEvent(event, workKindOf(event.tool, event.detail), index);
        if (placement.nodeId === undefined || seen.has(placement.nodeId)) {
            continue;
        }
        const node = byId.get(placement.nodeId);
        if (node === undefined) {
            continue;
        }
        seen.add(placement.nodeId);
        out.push(node);
        if (out.length >= TRAIL_NODE_LIMIT) {
            break;
        }
    }
    return out;
}

/** Die Schreib-Brueche dieses Akteurs, aus seinen verorteten Ereignissen. */
function burstsOf(actor: ActorState, index: PlacementIndex): WriteBurst[] {
    return writeBurstsOf(actor.events.map((event) => {
        const kind = workKindOf(event.tool, event.detail);
        const placement = placeEvent(event, kind, index);
        return {
            ts: event.ts,
            nodeId: placement.nodeId ?? -1,
            write: kind === 'write',
            key: `${actor.id}:${event.run}:${event.seq}`,
        };
    }));
}

function viewOf(
    actor: ActorState,
    letter: string,
    input: AgentsViewInput,
    index: PlacementIndex,
    byId: Map<number, GraphNode>,
): ActorView {
    const placement = placeEvent(actor.last, actor.kind, index);
    const strip = activityStrip(actor.events, input.now, STRIP_SECONDS);
    const sinceMs = Math.max(0, input.now - actor.lastTs);
    const idle = sinceMs > IDLE_MS;
    /*
     * Der Puls kommt aus der GEZAEHLTEN Zahl der Ereignisse im Fenster, und ein
     * ruhiger Akteur bekommt gar keinen. Damit ist der Puls selbst eine
     * Auskunft: er hoert auf, wenn die Arbeit aufhoert. Ein Herzschlag, der im
     * Leerlauf weiterschlaegt, waere die Behauptung einer Arbeit, die nicht
     * stattfindet.
     */
    const recentEvents = idle ? 0 : eventsInWindow(actor, input.now, PULSE_WINDOW_MS);
    return {
        id: actor.id,
        name: actor.name,
        you: actor.you,
        color: agentColor(actor.id),
        letter,
        kind: actor.kind,
        kindLetter: WORK_KIND_LETTERS[actor.kind],
        placement,
        node: placement.nodeId === undefined ? undefined : byId.get(placement.nodeId),
        testedNode: placement.testedNodeId === undefined
            ? undefined
            : byId.get(placement.testedNodeId),
        ghostNodes: placement.ghostIds
            .map((id) => byId.get(id))
            .filter((node): node is GraphNode => node !== undefined),
        last: actor.last,
        lastTs: actor.lastTs,
        hereMs: timeAtPlace(actor, input.now),
        count: actor.count,
        missed: actor.missed,
        strip,
        stripTotal: strip.reduce((sum, value) => sum + value, 0),
        intent: actor.last.intent ?? '',
        paths: recentPaths(actor, input.now, input.trailWindowMs),
        sinceMs,
        idle,
        recentEvents,
        pulse: pulseOf(recentEvents),
        trail: trailOf(actor, input.now, input.trailWindowMs, index, byId),
        waves: idle ? [] : currentBursts(burstsOf(actor, index), actor.lastTs),
        drawn: true,
    };
}

/**
 * Die Sicht bauen.
 *
 * Die Liste, die sich nicht verorten liess, entsteht aus denselben Ereignissen
 * und nicht aus einer zweiten Quelle: gefragt wird jedes behaltene Ereignis
 * jedes aktiven Akteurs, und was `none` ergibt, steht darin, neuestes zuerst.
 */
export function buildAgentsView(input: AgentsViewInput): AgentsView {
    const index = input.index ?? buildPlacementIndex(input.nodes);
    const byId = nodeById(input.nodes);
    const active = activeActors(input.state, input.now);
    const letters = agentLetters(active.map((actor) => ({ id: actor.id, name: actor.name })));

    const all = active.map((actor) =>
        viewOf(actor, letters.get(actor.id) ?? '?', input, index, byId));

    const shown = all.filter((actor) => {
        if (input.filter === 'both') {
            return true;
        }
        return input.filter === 'you' ? actor.you : !actor.you;
    });

    /*
     * Der Deckel greift an der GEZEICHNETEN Liste und nicht an der gezeigten.
     *
     * Die Reihenfolge ist die von `activeActors`: die juengste Bewegung zuerst.
     * Wer ueber den Deckel faellt, ist damit der, dessen letztes Ereignis am
     * laengsten her ist, und er steht weiter im Instrument, mit allen Zahlen.
     * Was fehlt, ist sein Koerper auf dem Graphen, und genau das sagt das
     * Instrument in einer eigenen Zeile.
     */
    const cap = input.cap ?? DRAWN_BODIES_CAP;
    const actors = shown.map((actor, position) => ({ ...actor, drawn: position < cap }));
    const capped = Math.max(0, shown.length - cap);
    /*
     * Auch die ungefilterte Liste traegt den Deckel, damit sie nicht behauptet,
     * es waeren neun Koerper gezeichnet, wo acht stehen. Sie ist die Liste, an
     * der der Kopf zaehlt und an der der Beweislauf liest.
     */
    const drawnIds = new Set(actors.filter((actor) => actor.drawn).map((actor) => actor.id));
    const allWithCap = all.map((actor) => ({ ...actor, drawn: drawnIds.has(actor.id) }));

    const unmapped: UnmappedEvent[] = [];
    const seen = new Set<string>();
    for (const actor of active) {
        for (let i = actor.events.length - 1; i >= 0; i -= 1) {
            const event = actor.events[i] as AgentEvent;
            // Die Art wird je Ereignis gefragt und nicht vom Akteur genommen:
            // die Art des Akteurs ist die seines LETZTEN Ereignisses, und ein
            // Testlauf von eben macht ein Lesen von vorhin nicht zu einem.
            const placement = placeEvent(event, workKindOf(event.tool, event.detail), index);
            if (placement.kind !== 'none') {
                continue;
            }
            const key = `${event.run}:${event.seq}`;
            if (seen.has(key)) {
                continue;
            }
            seen.add(key);
            unmapped.push({
                ts: event.ts,
                agent: event.agent,
                tool: event.tool,
                path: event.path,
                detail: event.detail,
                why: placement.why,
            });
        }
    }
    unmapped.sort((a, b) => b.ts - a.ts);

    /*
     * Der Ticker: die letzten Ereignisse aller gezeigten Akteure, neuestes
     * zuerst. Jede Zeile ist ein Ereignis und nennt, was daran gemessen ist:
     * wer, welches Werkzeug, welcher Knoten, welche Zeilen. Was das Ereignis
     * nicht sagt, steht nicht darin.
     */
    const ticker: TickerEntry[] = [];
    const byActorId = new Map(actors.map((entry) => [entry.id, entry]));
    for (const actor of active) {
        const view = byActorId.get(actor.id);
        if (view === undefined) {
            continue;
        }
        for (let i = actor.events.length - 1; i >= 0; i -= 1) {
            const event = actor.events[i] as AgentEvent;
            if (event.ts > input.now) {
                continue;
            }
            const kind = workKindOf(event.tool, event.detail);
            const placement = placeEvent(event, kind, index);
            ticker.push({
                ts: event.ts,
                actor: view.id,
                name: view.name,
                color: view.color,
                kind,
                tool: event.tool,
                place: placement.kind === 'none' ? '' : placement.name,
                lines: event.lines === undefined ? [] : [...event.lines],
                path: event.path,
            });
        }
    }
    ticker.sort((a, b) => b.ts - a.ts);

    return {
        actors,
        all: allWithCap,
        unmapped: unmapped.slice(0, UNMAPPED_LIMIT),
        events: input.state.events,
        missed: input.state.missed,
        perMinute: eventsPerMinute(input.state, input.now),
        unreadable: input.state.unreadable,
        cap,
        capped,
        ticker: ticker.slice(0, input.tickerLimit ?? TICKER_LIMIT),
    };
}
