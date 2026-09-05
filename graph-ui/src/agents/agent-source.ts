/**
 * Die Verbindung zur Bruecke, und der Zustand, wenn es keine gibt.
 *
 * ## Aus heisst aus
 *
 * Solange der Live-Modus aus ist, geht von hier KEINE Anfrage hinaus. Nicht
 * eine Probe, nicht ein `HEAD`, nicht ein Versuch herauszufinden, ob eine
 * Bruecke laeuft. Dasselbe Versprechen wie beim lokalen Modell (src/llm), und
 * derselbe Grund: ein Schalter, der im Hintergrund weiter fragt, ist kein
 * Schalter, sondern eine Farbe. Der Zaehler {@link AgentSourceStatus.requests}
 * steht im Testgriff, damit die Null gemessen werden kann und nicht geglaubt
 * werden muss.
 *
 * ## Und ohne Bruecke heisst ohne Bruecke
 *
 * Ist der Modus an und antwortet niemand, sagt der Zustand `no-source`, und das
 * Instrument zeigt den Befehl, der die Bruecke startet. Ein leerer Graph waere
 * an dieser Stelle die Behauptung, es arbeite gerade niemand, und das ist eine
 * Aussage, die dieses Fenster nicht treffen kann.
 *
 * ## Warum `fetch` und nicht `EventSource`
 *
 * Der Draht ist Server-Sent-Events, das Lesen ist ein `fetch` mit einem
 * Stroem-Leser. Drei Gruende, in dieser Reihenfolge:
 *
 *  1. **Wiederaufnahme.** Die Bruecke nimmt die zuletzt gesehene Nummer je Lauf
 *     als Abfrageparameter. `EventSource` schickt beim Wiederverbinden seine
 *     eigene `Last-Event-ID` und sonst nichts; die Nummern MEHRERER Laeufe
 *     passen dort nicht hinein.
 *  2. **Abbrechen.** Ein `AbortController` beendet die Verbindung in dem
 *     Augenblick, in dem der Schalter faellt. `EventSource.close()` tut das auch,
 *     verbindet aber vorher von selbst neu, und "von selbst" ist genau das, was
 *     ein ausgeschalteter Modus nicht tun darf.
 *  3. **Messbarkeit.** Ein `fetch` ist eine Anfrage, die man zaehlen kann, und
 *     die Zusicherung dieses Moduls ist eine Zahl.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import type { AgentEvent } from './agent-event';
import { readAgentEvent, sinceParameter } from './agent-event';
import { emptyAgentsState, withEvent } from './agent-store';
import type { AgentsState } from './agent-store';

/** Der Vorgabeport der Bruecke. Derselbe wie in tools/agent-bridge.mjs. */
export const DEFAULT_BRIDGE_PORT = 4142;

/** Wie lange nach einem Abriss gewartet wird, bevor neu verbunden wird. */
export const RECONNECT_MS = 3000;

/** Was der Zustand `no-source` als Weg nach vorn nennt. */
export function bridgeCommand(port: number): string {
    return port === DEFAULT_BRIDGE_PORT
        ? 'node tools/agent-bridge.mjs'
        : `node tools/agent-bridge.mjs --port ${port}`;
}

/** Die Lage der Quelle. */
export type SourceState =
    /** Der Live-Modus ist aus. Es geht keine Anfrage hinaus. */
    | 'off'
    /** Eine Verbindung laeuft gerade an. */
    | 'connecting'
    /** Die Bruecke antwortet und schickt Ereignisse. */
    | 'connected'
    /** Der Modus ist an und niemand antwortet. */
    | 'no-source';

/** Was die Bruecke ueber sich gesagt hat. */
export interface BridgeHello {
    /** `live` verfolgt eine Datei, `replay` spielt eine Aufzeichnung ab. */
    mode: string;
    /** Die Datei, die sie liest. */
    file: string;
    /** Wie viele Zeilen sie gelesen hat. */
    events: number;
    /** Wie viele Zeilen kein JSON waren. */
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
    /** Was die Bruecke ueber sich gesagt hat, wenn sie es gesagt hat. */
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

/** Der Port der Bruecke, wie die Adresszeile ihn nennt. */
export function bridgePortFromSearch(search: string): number {
    try {
        const raw = new URLSearchParams(search).get('agents');
        const port = Number(raw);
        return Number.isFinite(port) && port > 0 && port < 65536 ? port : DEFAULT_BRIDGE_PORT;
    } catch {
        return DEFAULT_BRIDGE_PORT;
    }
}

export interface AgentStreamOptions {
    /** Ob der Live-Modus an ist. Aus heisst: keine Anfrage. */
    on: boolean;
    /** Der Port der Bruecke. */
    port?: number;
    /** Ersetzbares fetch, damit Tests ohne Netz laufen. */
    fetch?: typeof globalThis.fetch | undefined;
}

/** Was der Aufrufer bekommt. */
export interface AgentStream {
    state: AgentsState;
    status: AgentSourceStatus;
    /** Ein eigenes Ereignis dazulegen, ohne die Bruecke zu fragen. */
    push: (event: AgentEvent, you: boolean) => void;
}

/**
 * Der Strom, wie ihn die Panels bekommen: samt Port, damit das Instrument den
 * Befehl nennen kann, der die Bruecke startet.
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
    const port = options.port ?? DEFAULT_BRIDGE_PORT;
    const fetchImpl = options.fetch;
    const on = options.on;

    const stateRef = useRef<AgentsState>(emptyAgentsState());
    const [state, setState] = useState<AgentsState>(stateRef.current);
    const requests = useRef(0);
    const drops = useRef(0);
    const [status, setStatus] = useState<AgentSourceStatus>({
        state: 'off',
        origin: '',
        requests: 0,
        drops: 0,
        hello: undefined,
        error: '',
    });

    const push = useCallback((event: AgentEvent, you: boolean) => {
        const next = withEvent(stateRef.current, event);
        stateRef.current = you
            ? {
                ...next,
                actors: next.actors.map((actor) =>
                    (actor.id === event.agent ? { ...actor, you: true } : actor)),
            }
            : next;
        setState(stateRef.current);
    }, []);

    useEffect(() => {
        if (!on) {
            setStatus({
                state: 'off',
                origin: '',
                requests: requests.current,
                drops: drops.current,
                hello: undefined,
                error: '',
            });
            return;
        }
        const origin = `http://127.0.0.1:${port}`;
        const doFetch = fetchImpl ?? globalThis.fetch;
        let stopped = false;
        let controller: AbortController | null = null;
        let timer: ReturnType<typeof setTimeout> | undefined;

        const announce = (patch: Partial<AgentSourceStatus>): void => {
            setStatus((current) => ({
                ...current,
                origin,
                requests: requests.current,
                drops: drops.current,
                ...patch,
            }));
        };

        const connect = async (): Promise<void> => {
            if (stopped) {
                return;
            }
            controller = new AbortController();
            const since = sinceParameter(stateRef.current.seen);
            const url = `${origin}/events${since.length > 0 ? `?since=${encodeURIComponent(since)}` : ''}`;
            requests.current += 1;
            announce({ state: 'connecting', error: '' });
            try {
                const response = await doFetch(url, {
                    headers: { Accept: 'text/event-stream' },
                    signal: controller.signal,
                });
                if (!response.ok || response.body === null) {
                    throw new Error(`the bridge answered with HTTP ${response.status}`);
                }
                announce({ state: 'connected', error: '' });
                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let buffer = '';
                for (;;) {
                    const chunk = await reader.read();
                    if (chunk.done) {
                        break;
                    }
                    buffer += decoder.decode(chunk.value, { stream: true });
                    const parts = buffer.split('\n\n');
                    buffer = parts.pop() ?? '';
                    let changed = false;
                    for (const part of parts) {
                        const frame = parseSseFrame(part);
                        if (frame === undefined) {
                            continue;
                        }
                        if (frame.event === 'hello') {
                            try {
                                const hello = JSON.parse(frame.data) as BridgeHello;
                                announce({ state: 'connected', hello });
                                stateRef.current = {
                                    ...stateRef.current,
                                    unreadable: Number(hello.unreadable) || 0,
                                };
                                changed = true;
                            } catch {
                                /* Ein unlesbarer Gruss aendert nichts an den Ereignissen. */
                            }
                            continue;
                        }
                        if (frame.event !== 'trace') {
                            continue;
                        }
                        try {
                            const event = readAgentEvent(JSON.parse(frame.data));
                            if (event !== undefined) {
                                stateRef.current = withEvent(stateRef.current, event);
                                changed = true;
                            }
                        } catch {
                            /* Eine unlesbare Zeile ist eine Zeile weniger, kein Abbruch. */
                        }
                    }
                    if (changed && !stopped) {
                        setState(stateRef.current);
                    }
                }
                if (!stopped) {
                    drops.current += 1;
                    announce({ state: 'no-source', error: 'the bridge closed the stream' });
                    timer = setTimeout(() => void connect(), RECONNECT_MS);
                }
            } catch (failure) {
                if (stopped) {
                    return;
                }
                const message = failure instanceof Error ? failure.message : String(failure);
                announce({ state: 'no-source', error: message });
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
    }, [on, port, fetchImpl]);

    return useMemo(() => ({ state, status, push }), [state, status, push]);
}
