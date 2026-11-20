/** Same-origin daemon polling over the existing sequential HTTP transport.
 * Off means zero requests. Cursors are committed only after a validated page;
 * reconnects retain state, and a replaced SQLite database resets its generation. */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { AgentEvent } from './agent-event';
import { readAgentEvent } from './agent-event';
import { emptyAgentsState, withEvent } from './agent-store';
import type { AgentsState } from './agent-store';

/** Kept as a source compatibility name; the current daemon port is used. */
export const DEFAULT_BRIDGE_PORT = 9749;
export const RECONNECT_MS = 3000;
export const POLL_MS = 1000;
export function bridgeCommand(_port: number): string {
    return 'codebase-memory-mcp --ui';
}

/** Die Lage der Quelle. */
export type SourceState =
    /** Der Live-Modus ist aus. Es geht keine Anfrage hinaus. */
    | 'off'
    /** Eine Verbindung laeuft gerade an. */
    | 'connecting'
    /** Der Daemon antwortet mit gespeicherten Ereignissen. */
    | 'connected'
    /** Der Modus ist an und niemand antwortet. */
    | 'no-source';

/** Quelleninformation des Daemons (Feldnamen kompatibel zum bisherigen HUD). */
export interface BridgeHello {
    /** `live` liest die lokale SQLite-Historie, `replay` bezeichnet Testaufzeichnungen. */
    mode: string;
    /** Lesbare Bezeichnung der Datenquelle. */
    file: string;
    /** Wie viele Ereignisse der Daemon aktuell aufbewahrt. */
    events: number;
    /** Wie viele empfangene Ereignisse nicht lesbar waren. */
    unreadable: number;
}

/** Was die Oberflaeche ueber ihre Quelle weiss. */
export interface AgentSourceStatus {
    state: SourceState;
    /** Der Ursprung, mit dem geredet wird. Leer, solange der Modus aus ist. */
    origin: string;
    /** Wie viele Anfragen dieses Modul gestellt hat, seit die Seite laedt. */
    requests: number;
    /** Wie oft die Verbindung abgerissen ist. */
    drops: number;
    /** Metadaten der zuletzt gelesenen Daemon-Antwort. */
    hello: BridgeHello | undefined;
    /** Der letzte Fehler, woertlich. Leer, wenn keiner. */
    error: string;
}

/** Ein Rahmen des Ereignisstroms: der Name und der Rumpf. */
export interface SseFrame {
    event: string;
    data: string;
}

/**
 * Einen Rahmen lesen.
 *
 * Nur die zwei Felder, die dieser Draht braucht. Zeilen, die mit einem
 * Doppelpunkt beginnen, sind Kommentare (die Bruecke schickt sie, damit die
 * Leitung offen bleibt) und ergeben nichts.
 */
export function parseSseFrame(raw: string): SseFrame | undefined {
    let name = 'message';
    const data: string[] = [];
    for (const line of raw.split('\n')) {
        if (line.startsWith(':') || line.length === 0) {
            continue;
        }
        if (line.startsWith('event:')) {
            name = line.slice(6).trim();
            continue;
        }
        if (line.startsWith('data:')) {
            data.push(line.slice(5).trimStart());
        }
    }
    return data.length === 0 ? undefined : { event: name, data: data.join('\n') };
}

/** Der aktuelle Daemon-Port; ein alter agents-Parameter hat keine Wirkung. */
export function bridgePortFromSearch(_search: string): number {
    return typeof location === 'undefined' ? DEFAULT_BRIDGE_PORT : Number(location.port) || 80;
}

export interface AgentStreamOptions {
    /** Ob der Live-Modus an ist. Aus heisst: keine Anfrage. */
    on: boolean;
    /** Kompatibilitaetsfeld fuer alte Aufrufer; der Transport verwendet same-origin. */
    port?: number;
    project?: string;
    /** Ersetzbares fetch, damit Tests ohne Netz laufen. */
    fetch?: typeof globalThis.fetch | undefined;
}

/** Was der Aufrufer bekommt. */
export interface AgentStream {
    state: AgentsState;
    status: AgentSourceStatus;
    /** Ein eigenes Browsereignis lokal dazulegen. */
    push: (event: AgentEvent, you: boolean) => void;
}

/**
 * Der Strom, wie ihn die Panels bekommen: samt Port, damit das Instrument den
 * vorhandenen Daemon-Port anzeigen kann.
 */
export interface AgentsRuntime extends AgentStream {
    port: number;
    /** Ob der Live-Modus an ist. */
    on: boolean;
}

/**
 * Den Strom fuehren.
 *
 * Der Zustand liegt in einem Ref und wird in den React-Zustand gespiegelt: eine
 * Verbindung, die zwanzig Ereignisse in einer Sekunde liefert, soll nicht
 * zwanzig Zustandsketten aufmachen, aus denen jede die vorige ueberschreibt.
 */
export function useAgentStream(options: AgentStreamOptions): AgentStream {
    const project = options.project ?? '';
    const fetchImpl = options.fetch;
    const on = options.on;

    const stateRef = useRef<AgentsState>(emptyAgentsState());
    const [reading, setReading] = useState({ project, value: stateRef.current });
    const requests = useRef(0);
    const drops = useRef(0);
    const cursor = useRef(0);
    const incomplete = useRef(false);
    const generation = useRef<string | undefined>(undefined);
    const previousProject = useRef(project);
    const [source, setSource] = useState<{ project: string; value: AgentSourceStatus }>({ project, value: {
        state: 'off',
        origin: '',
        requests: 0,
        drops: 0,
        hello: undefined,
        error: '',
    } });

    const push = useCallback((event: AgentEvent, you: boolean) => {
        if (previousProject.current !== project) return;
        const next = withEvent(stateRef.current, event);
        stateRef.current = you
            ? {
                ...next,
                actors: next.actors.map((actor) =>
                    (actor.id === event.agent ? { ...actor, you: true } : actor)),
            }
            : next;
        setReading({ project, value: stateRef.current });
    }, [project]);

    useEffect(() => {
        if (previousProject.current !== project) {
            previousProject.current = project;
            cursor.current = 0;
            generation.current = undefined;
            incomplete.current = false;
            stateRef.current = emptyAgentsState();
            setReading({ project, value: stateRef.current });
        }
        if (!on) {
            setSource({ project, value: {
                state: 'off',
                origin: '',
                requests: requests.current,
                drops: drops.current,
                hello: undefined,
                error: '',
            } });
            return;
        }
        const origin = typeof location === 'undefined' ? '' : location.origin;
        const doFetch = fetchImpl ?? globalThis.fetch;
        let stopped = false;
        let controller: AbortController | null = null;
        let timer: ReturnType<typeof setTimeout> | undefined;

        const announce = (patch: Partial<AgentSourceStatus>): void => {
            setSource((current) => ({ project, value: {
                ...(current.project === project ? current.value : { state: 'connecting', hello: undefined, error: '' }),
                origin,
                requests: requests.current,
                drops: drops.current,
                ...patch,
            } }));
        };

        const connect = async (): Promise<void> => {
            if (stopped) {
                return;
            }
            controller = new AbortController();
            const url = `/api/agent-events?project=${encodeURIComponent(project)}&after=${cursor.current}&limit=200`;
            requests.current += 1;
            if (cursor.current === 0) announce({ state: 'connecting', error: '' });
            try {
                const response = await doFetch(url, { signal: controller.signal });
                if (!response.ok) throw new Error(`daemon activity: HTTP ${response.status}`);
                const page = await response.json() as {
                    events: unknown[]; cursor: number; generation: string; reset: boolean;
                    has_more: boolean; truncated: boolean; retained: number;
                };
                if (!Array.isArray(page.events) || !Number.isSafeInteger(page.cursor)
                    || page.cursor < 0 || typeof page.generation !== 'string') {
                    throw new Error('Invalid daemon activity response');
                }
                if (stopped) return;
                if (page.reset || (generation.current !== undefined && generation.current !== page.generation)) {
                    stateRef.current = emptyAgentsState();
                    cursor.current = 0;
                    incomplete.current = false;
                    generation.current = page.generation;
                    setReading({ project, value: stateRef.current });
                    timer = setTimeout(() => void connect(), 0);
                    return;
                }
                generation.current = page.generation;
                let next = stateRef.current;
                for (const raw of page.events) {
                    const event = readAgentEvent(raw);
                    if (event !== undefined) next = withEvent(next, event);
                    else next = { ...next, unreadable: next.unreadable + 1 };
                }
                stateRef.current = next;
                cursor.current = page.cursor;
                incomplete.current ||= page.truncated;
                setReading({ project, value: next });
                announce({ state: 'connected', error: incomplete.current
                    ? 'Older activity expired from local retention; this history is incomplete.' : '',
                    hello: { mode: 'live', file: 'daemon SQLite activity', events: page.retained, unreadable: next.unreadable } });
                timer = setTimeout(() => void connect(), page.has_more ? 0 : POLL_MS);
            } catch (failure) {
                if (stopped) return;
                drops.current += 1;
                announce({ state: 'no-source', error: failure instanceof Error ? failure.message : String(failure) });
                timer = setTimeout(() => void connect(), RECONNECT_MS);
            }
        };

        void connect();
        return () => {
            stopped = true;
            if (timer !== undefined) {
                clearTimeout(timer);
            }
            controller?.abort();
        };
    }, [on, project, fetchImpl]);

    // A passive effect resets the cursor, but the first render of another
    // project must already hide the previous project's events and provenance.
    return useMemo(() => ({
        state: reading.project === project ? reading.value : emptyAgentsState(),
        status: source.project === project ? source.value : {
            state: on ? 'connecting' as const : 'off' as const, origin: '',
            requests: requests.current, drops: drops.current, hello: undefined, error: '',
        },
        push,
    }), [reading, source, project, on, push]);
}
