/**
 * Ein Stichprobenzaehler ueber den eigenen Prozessbaum: was hat gerade eine
 * Gegenstelle offen, und liegt die auf Loopback?
 *
 * ## Warum ein Beweislauf das selbst misst, obwohl das Gate es auch tut
 *
 * tools/net-deny-gate.mjs beobachtet das Kommando VON AUSSEN und schreibt sein
 * Ergebnis erst, wenn das Kommando fertig ist. Der Lauf selbst kann diese Zahl
 * also nicht in sein eigenes Artefakt schreiben; er wuerde eine Datei lesen, die
 * es zu seiner Laufzeit noch nicht gibt. Der Abnahmetest verlangt die Zahl aber
 * in airgap.json, und eine abgeschriebene Zahl waere keine zweite Beobachtung,
 * sondern dieselbe an einem zweiten Ort.
 *
 * Also misst der Lauf selbst, mit denselben Regeln: derselbe `classifyLine` und
 * derselbe `isLoopbackHost` aus dem Gate, importiert und nicht nachgebaut. Nur
 * die zwanzig Zeilen um `pgrep` und `lsof` herum stehen hier noch einmal. Das
 * Gate umzubauen, damit es diese zwanzig Zeilen hergibt, waere eine Aenderung an
 * dem Werkzeug, das jeden bisherigen Beweis dieses Projekts getragen hat, und
 * zwar aus Bequemlichkeit eines neuen Laufs. Der Preis dafuer ist hoeher als
 * zwanzig Zeilen.
 *
 * Die Grenze der Aussage ist dieselbe wie beim Gate und wird genauso genannt:
 * eine Verbindung, die zwischen zwei Abtastungen aufgebaut, benutzt und
 * geschlossen wird, kann durchrutschen. Deshalb steht die Zahl der Stichproben
 * im Ergebnis. Ohne sie waere eine Null nicht lesbar.
 */

import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

import { classifyLine } from '../net-deny-gate.mjs';

const execFileAsync = promisify(execFile);

async function childPids(pid) {
    try {
        const { stdout } = await execFileAsync('pgrep', ['-P', String(pid)]);
        return stdout.split('\n').map((line) => line.trim()).filter(Boolean).map(Number);
    } catch {
        // pgrep endet mit 1, wenn es keine Kinder gibt. Das ist der Normalfall.
        return [];
    }
}

/** Der Prozessbaum unter (und einschliesslich) pid. */
async function processTree(pid) {
    const seen = new Set();
    const queue = [pid];
    while (queue.length > 0) {
        const current = queue.shift();
        if (seen.has(current)) {
            continue;
        }
        seen.add(current);
        for (const child of await childPids(current)) {
            if (!seen.has(child)) {
                queue.push(child);
            }
        }
    }
    return [...seen];
}

async function sockets(pids) {
    if (pids.length === 0) {
        return { lines: [], error: null };
    }
    try {
        const { stdout } = await execFileAsync(
            'lsof',
            ['-a', '-i', '-n', '-P', '-p', pids.join(',')],
            { maxBuffer: 8 * 1024 * 1024 },
        );
        return { lines: stdout.split('\n').filter((line) => line.trim().length > 0), error: null };
    } catch (err) {
        // Exit 1 heisst bei lsof "nichts gefunden", nicht "kaputt".
        if (err && err.code === 1) {
            return { lines: [], error: null };
        }
        return { lines: [], error: err && err.message ? err.message : String(err) };
    }
}

/**
 * Den eigenen Prozessbaum abtasten, bis `stop()` gerufen wird.
 *
 * @param {{intervalMs?: number, pid?: number}} options
 */
export function startSocketSampler(options = {}) {
    const intervalMs = options.intervalMs ?? 600;
    const rootPid = options.pid ?? process.pid;
    const state = {
        samples: 0,
        lsofErrors: 0,
        maxProcessTreeSize: 0,
        violations: [],
        loopback: new Map(),
    };
    let running = true;
    const started = Date.now();

    const once = async () => {
        const pids = await processTree(rootPid);
        state.maxProcessTreeSize = Math.max(state.maxProcessTreeSize, pids.length);
        const { lines, error } = await sockets(pids);
        if (error !== null) {
            state.lsofErrors += 1;
        }
        state.samples += 1;
        for (const line of lines) {
            const socket = classifyLine(line);
            if (socket === null) {
                continue;
            }
            if (socket.loopback) {
                const key = `${socket.protocol} ${socket.remote}`;
                state.loopback.set(key, (state.loopback.get(key) ?? 0) + 1);
                continue;
            }
            state.violations.push({
                atMs: Date.now() - started,
                protocol: socket.protocol,
                command: socket.command,
                pid: socket.pid,
                local: socket.local,
                remote: socket.remote,
            });
        }
    };

    const loop = (async () => {
        while (running) {
            await once();
            if (!running) {
                break;
            }
            await new Promise((wake) => setTimeout(wake, intervalMs));
        }
    })();

    return {
        async stop() {
            running = false;
            await loop;
            await once();
            return {
                samples: state.samples,
                intervalMs,
                durationMs: Date.now() - started,
                maxProcessTreeSize: state.maxProcessTreeSize,
                lsofErrors: state.lsofErrors,
                outboundViolations: state.violations.length,
                violations: state.violations,
                loopbackConnections: [...state.loopback.entries()]
                    .map(([endpoint, seen]) => ({ endpoint, seen }))
                    .sort((a, b) => a.endpoint.localeCompare(b.endpoint)),
            };
        },
        get samples() {
            return state.samples;
        },
    };
}
