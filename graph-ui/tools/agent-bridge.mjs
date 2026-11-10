#!/usr/bin/env node
/**
 * Die Bruecke: eine Ereignisdatei, gelesen und als Server-Sent-Events
 * weitergereicht.
 *
 *   node tools/agent-bridge.mjs [--file <pfad>] [--port 4142]
 *   node tools/agent-bridge.mjs --replay fixtures/agent-events/w11a-replay.jsonl
 *
 * ## Was sie ist, und was sie ausdruecklich nicht ist
 *
 * Sie **liest**. Sie oeffnet die Ereignisdatei zum Lesen, verfolgt, was
 * angehaengt wird, und schickt jede vollstaendige Zeile weiter. Sie schreibt
 * nie in die Datei, sie legt sie nicht an, sie loescht sie nicht, und sie nimmt
 * kein Ereignis von aussen entgegen: es gibt keine Route, die etwas
 * hinzufuegt. Damit kann die Oberflaeche die Aufzeichnung nicht veraendern, und
 * das ist der Grund, aus dem die eigenen Bewegungen des Lesers ("you") gar
 * nicht erst hierher kommen.
 *
 * Sie spricht **nur Loopback**. Sie lauscht auf 127.0.0.1, sie weist jede
 * Verbindung ab, die nicht von dort kommt, und sie erlaubt als Ursprung nur
 * 127.0.0.1 und localhost. Ein Netzwerkdienst ist sie damit nicht.
 *
 * Sie ist **nicht noetig**. Laeuft sie nicht, laeuft die Oberflaeche weiter und
 * sagt, dass keine Quelle verbunden ist, samt dem Befehl, der die Bruecke
 * startet. Ein leerer Graph, der wie Ruhe aussieht, waere die Behauptung, es
 * arbeite gerade niemand.
 *
 * ## Zwei Betriebsarten, und warum die zweite existiert
 *
 * **live**: die Datei wird verfolgt. Neue Zeilen gehen sofort hinaus. Das ist
 * der Betrieb.
 *
 * **replay**: die Datei wird einmal gelesen und danach steht der Takt still,
 * bis jemand ihn setzt (`POST /replay/advance`). Ohne diese Naht muesste ein
 * Beweislauf auf die Wanduhr warten, und eine Aufzeichnung mit echten
 * Zeitabstaenden dauert Minuten; mit ihr ist derselbe Ablauf in Sekunden
 * gefahren und, was mehr zaehlt, in jedem Lauf an derselben Stelle. Die
 * Zeitstempel werden dabei auf die Gegenwart geschoben und behalten ihre
 * Abstaende: eine Ansicht, die "vor drei Sekunden" sagt, soll nicht "vor acht
 * Monaten" meinen. Was verschoben wurde, steht in jedem Ereignis (`ts_recorded`)
 * und im Willkommensereignis (`mode: "replay"`), damit die Oberflaeche einen
 * Wiedergabelauf nie als Gegenwart ausgeben kann.
 *
 * ## Wiederaufnahme
 *
 * `GET /events?since=<lauf>:<nummer>,<lauf>:<nummer>` schickt nur, was hinter
 * den genannten Nummern liegt. Der Klient merkt sich die zuletzt gesehene
 * Nummer je Lauf und nennt sie beim Wiederverbinden; was dazwischen verloren
 * ging, sieht er an der Luecke in den Nummern und sagt es.
 *
 * ## Das Format einer Zeile
 *
 *   ts    Millisekunden seit 1970
 *   agent Anzeigename
 *   run   Kennung eines Laufs
 *   seq   fortlaufend je Lauf
 *   phase "start" oder "end"
 *   tool  Werkzeugname
 *   path  optionaler Pfad, repo-relativ
 *   lines optionaler Bereich [von, bis]
 *   detail  optionaler Befehl oder Suchbegriff
 *   intent  optionale Selbstauskunft des Agenten
 *
 * Unbekannte Felder werden durchgereicht und nicht als Fehler behandelt: eine
 * Quelle, die mehr weiss, soll nicht daran scheitern, dass sie es sagt. Eine
 * Zeile, die kein JSON ist, wird gezaehlt und uebersprungen.
 */

import { createReadStream, existsSync, statSync, watch } from 'node:fs';
import http from 'node:http';
import { homedir } from 'node:os';
import { isAbsolute, join, resolve } from 'node:path';

/** Der Vorgabeport. Er gehoert dem Arbeitsplatz; Beweislaeufe nehmen eigene. */
export const DEFAULT_BRIDGE_PORT = 4142;

/** Die Vorgabedatei, dieselbe wie in agents/hooks/atlas-trace.py. */
export const DEFAULT_TRACE_FILE = join(homedir(), '.atlas-trace', 'events.jsonl');

/** Wie oft nachgesehen wird, ob die Datei gewachsen ist (Millisekunden). */
const POLL_MS = 250;

/** Wie oft ein Kommentar durch die Leitung geht, damit sie offen bleibt. */
const KEEPALIVE_MS = 15000;

const ALLOWED_ORIGIN = /^https?:\/\/(127\.0\.0\.1|localhost|\[::1\])(:\d+)?$/;
const LOOPBACK = new Set(['127.0.0.1', '::1', '::ffff:127.0.0.1']);

const log = (...parts) => console.log('[agent-bridge]', ...parts);

/** Die Argumente, ohne Bibliothek. */
export function parseArgs(argv) {
    const options = { file: DEFAULT_TRACE_FILE, port: DEFAULT_BRIDGE_PORT, mode: 'live' };
    for (let i = 0; i < argv.length; i += 1) {
        const arg = argv[i];
        if (arg === '--file' && argv[i + 1] !== undefined) {
            options.file = argv[i + 1];
            i += 1;
        } else if (arg === '--replay' && argv[i + 1] !== undefined) {
            options.file = argv[i + 1];
            options.mode = 'replay';
            i += 1;
        } else if (arg === '--port' && argv[i + 1] !== undefined) {
            options.port = Number(argv[i + 1]);
            i += 1;
        }
    }
    options.file = isAbsolute(options.file) ? options.file : resolve(process.cwd(), options.file);
    return options;
}

/**
 * Die Wiederaufnahme-Angabe lesen.
 *
 * `lauf:nummer,lauf:nummer`. Alles, was nicht so aussieht, wird uebergangen:
 * eine unlesbare Angabe soll den Strom nicht verhindern, sondern nur nichts
 * ueberspringen.
 */
export function parseSince(raw) {
    const seen = new Map();
    if (typeof raw !== 'string' || raw.length === 0) {
        return seen;
    }
    for (const part of raw.split(',')) {
        const at = part.lastIndexOf(':');
        if (at <= 0) {
            continue;
        }
        const run = part.slice(0, at);
        const seq = Number(part.slice(at + 1));
        if (Number.isFinite(seq)) {
            seen.set(run, seq);
        }
    }
    return seen;
}

/** Ob dieses Ereignis hinter der zuletzt gesehenen Nummer seines Laufs liegt. */
export function isAfter(event, seen) {
    const known = seen.get(String(event.run ?? ''));
    if (known === undefined) {
        return true;
    }
    const seq = Number(event.seq);
    return !Number.isFinite(seq) || seq > known;
}

/**
 * Die Bruecke starten.
 *
 * Als Funktion und nicht nur als Skript, weil der Beweislauf sie im selben
 * Prozess fahren koennte; er tut es nicht (er startet sie als eigenen Prozess,
 * damit auch das Starten und Beenden gemessen wird), aber ein Modul, das nur
 * als Skript existiert, ist nicht pruefbar.
 */
export async function startBridge({ file, port, mode = 'live' }) {
    const state = {
        /** Jede gelesene Zeile, in der Reihenfolge der Datei. */
        events: [],
        /** Wie viele Zeilen kein JSON waren. */
        unreadable: 0,
        /** Wie weit die Datei schon gelesen ist. */
        offset: 0,
        /** Ein unvollstaendiger Rest am Ende der Datei. */
        rest: '',
        /** Im Wiedergabemodus: wie viele Ereignisse schon hinausgingen. */
        emitted: 0,
        /** Der Zeitversatz der Wiedergabe, in Millisekunden. */
        shift: 0,
    };
    const clients = new Set();

    const readNew = () =>
        new Promise((done) => {
            if (!existsSync(file)) {
                done(0);
                return;
            }
            let size = 0;
            try {
                size = statSync(file).size;
            } catch {
                done(0);
                return;
            }
            if (size <= state.offset) {
                /* Eine kuerzer gewordene Datei ist eine neue Datei. Wieder von vorn. */
                if (size < state.offset) {
                    state.offset = 0;
                    state.rest = '';
                }
                done(0);
                return;
            }
            const stream = createReadStream(file, { start: state.offset, end: size - 1, encoding: 'utf8' });
            let chunk = '';
            stream.on('data', (part) => { chunk += part; });
            stream.on('error', () => done(0));
            stream.on('end', () => {
                state.offset = size;
                const body = state.rest + chunk;
                const parts = body.split('\n');
                state.rest = parts.pop() ?? '';
                let added = 0;
                for (const line of parts) {
                    const trimmed = line.trim();
                    if (trimmed.length === 0) {
                        continue;
                    }
                    try {
                        const parsed = JSON.parse(trimmed);
                        if (parsed !== null && typeof parsed === 'object') {
                            state.events.push(parsed);
                            added += 1;
                        } else {
                            state.unreadable += 1;
                        }
                    } catch {
                        state.unreadable += 1;
                    }
                }
                done(added);
            });
        });

    /** Ein Ereignis so, wie es hinausgeht: im Wiedergabemodus auf jetzt geschoben. */
    const shaped = (event) => {
        if (mode !== 'replay') {
            return event;
        }
        const ts = Number(event.ts);
        if (!Number.isFinite(ts)) {
            return { ...event, replay: true };
        }
        return { ...event, ts: ts + state.shift, ts_recorded: ts, replay: true };
    };

    const send = (client, name, payload) => {
        try {
            client.write(`event: ${name}\ndata: ${JSON.stringify(payload)}\n\n`);
        } catch {
            clients.delete(client);
        }
    };

    const broadcast = (events) => {
        for (const client of clients) {
            for (const event of events) {
                if (isAfter(event, client.seen)) {
                    send(client, 'trace', shaped(event));
                }
            }
        }
    };

    await readNew();
    if (mode === 'replay' && state.events.length > 0) {
        /*
         * Der Versatz wird EINMAL gesetzt, an der letzten aufgezeichneten Zeit:
         * damit endet die Wiedergabe in der Gegenwart und alles davor liegt
         * genau so weit zurueck wie damals.
         */
        const last = Number(state.events[state.events.length - 1].ts);
        state.shift = Number.isFinite(last) ? Date.now() - last : 0;
    }

    const hello = () => ({
        mode,
        file,
        events: state.events.length,
        unreadable: state.unreadable,
        emitted: mode === 'replay' ? state.emitted : state.events.length,
        fileExists: existsSync(file),
    });

    const server = http.createServer((req, res) => {
        const remote = req.socket.remoteAddress ?? '';
        if (!LOOPBACK.has(remote)) {
            res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('loopback only');
            return;
        }
        const origin = req.headers.origin;
        const headers = {};
        if (typeof origin === 'string' && ALLOWED_ORIGIN.test(origin)) {
            headers['Access-Control-Allow-Origin'] = origin;
        } else if (typeof origin === 'string') {
            res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('loopback origins only');
            return;
        }
        const url = new URL(req.url ?? '/', `http://127.0.0.1:${port}`);

        if (req.method === 'OPTIONS') {
            res.writeHead(204, { ...headers, 'Access-Control-Allow-Headers': 'accept' });
            res.end();
            return;
        }

        /*
         * Nur GET, ausser fuer den einen Takt der Wiedergabe. Das ist die Form
         * der Zusicherung "sie liest nur": eine Bruecke, die auf jedes Verb
         * antwortet, muesste erklaeren, was sie mit einem PUT macht.
         */
        if (req.method !== 'GET' && url.pathname !== '/replay/advance') {
            res.writeHead(405, { ...headers, 'Content-Type': 'text/plain; charset=utf-8' });
            res.end('the bridge reads; it takes nothing in');
            return;
        }

        if (url.pathname === '/health') {
            res.writeHead(200, { ...headers, 'Content-Type': 'application/json; charset=utf-8' });
            res.end(JSON.stringify(hello()));
            return;
        }

        if (url.pathname === '/events') {
            res.writeHead(200, {
                ...headers,
                'Content-Type': 'text/event-stream; charset=utf-8',
                'Cache-Control': 'no-store',
                Connection: 'keep-alive',
            });
            res.write(': open\n\n');
            const client = res;
            client.seen = parseSince(url.searchParams.get('since'));
            clients.add(client);
            send(client, 'hello', hello());
            const upTo = mode === 'replay' ? state.emitted : state.events.length;
            for (let i = 0; i < upTo; i += 1) {
                const event = state.events[i];
                if (isAfter(event, client.seen)) {
                    send(client, 'trace', shaped(event));
                }
            }
            const beat = setInterval(() => {
                try {
                    client.write(': beat\n\n');
                } catch {
                    clients.delete(client);
                }
            }, KEEPALIVE_MS);
            req.on('close', () => {
                clearInterval(beat);
                clients.delete(client);
            });
            return;
        }

        if (url.pathname === '/replay/advance' && req.method === 'POST') {
            if (mode !== 'replay') {
                res.writeHead(409, { ...headers, 'Content-Type': 'application/json; charset=utf-8' });
                res.end(JSON.stringify({ error: 'the bridge is following a file, not replaying one' }));
                return;
            }
            const count = Number(url.searchParams.get('count') ?? '1');
            const wanted = Number.isFinite(count) && count > 0 ? Math.floor(count) : 1;
            const from = state.emitted;
            const to = Math.min(state.events.length, from + wanted);
            broadcast(state.events.slice(from, to));
            state.emitted = to;
            res.writeHead(200, { ...headers, 'Content-Type': 'application/json; charset=utf-8' });
            res.end(JSON.stringify({
                emitted: to - from,
                total: state.emitted,
                remaining: state.events.length - state.emitted,
            }));
            return;
        }

        res.writeHead(404, { ...headers, 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('the bridge knows /health, /events and, when replaying, /replay/advance');
    });

    await new Promise((ready, fail) => {
        server.once('error', fail);
        server.listen(port, '127.0.0.1', ready);
    });

    let watcher = null;
    let timer = null;
    if (mode === 'live') {
        const pull = () => {
            const before = state.events.length;
            void readNew().then((added) => {
                if (added > 0) {
                    broadcast(state.events.slice(before));
                }
            });
        };
        timer = setInterval(pull, POLL_MS);
        try {
            watcher = watch(file, pull);
        } catch {
            /* Ohne Beobachter bleibt der Takt oben. Das ist langsamer und nicht falsch. */
        }
    }

    return {
        port,
        mode,
        file,
        state,
        close: () =>
            new Promise((done) => {
                if (timer !== null) {
                    clearInterval(timer);
                }
                watcher?.close();
                for (const client of clients) {
                    try {
                        client.end();
                    } catch {
                        /* schon zu */
                    }
                }
                clients.clear();
                server.closeAllConnections?.();
                server.close(() => done());
            }),
    };
}

/* Als Skript gestartet: lauschen und sagen, was gilt. */
if (process.argv[1] !== undefined && process.argv[1].endsWith('agent-bridge.mjs')) {
    const options = parseArgs(process.argv.slice(2));
    startBridge(options).then((bridge) => {
        log(`${bridge.mode} on http://127.0.0.1:${bridge.port}`);
        log(`file: ${bridge.file}${existsSync(bridge.file) ? '' : ' (not there yet, waiting)'}`);
        log(`events read: ${bridge.state.events.length}, unreadable lines: ${bridge.state.unreadable}`);
        const stop = () => {
            void bridge.close().then(() => process.exit(0));
        };
        process.on('SIGINT', stop);
        process.on('SIGTERM', stop);
    }).catch((error) => {
        console.error('[agent-bridge] could not start:', error.message);
        process.exitCode = 1;
    });
}
