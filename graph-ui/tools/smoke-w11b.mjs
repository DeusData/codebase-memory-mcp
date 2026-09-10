#!/usr/bin/env node
/*
 * W11b-Smoke: ein Agent teleportiert nicht.
 *
 * Er fliegt in etwa einer halben Sekunde auf einer gebogenen Bahn zum naechsten
 * Symbol, mit kurzem Kometenschweif, und hinter ihm bleibt die Spur der zuletzt
 * besuchten Knoten stehen. Er atmet im Takt seiner Ereignisse und haelt an,
 * wenn sie aufhoeren. Dazu drei filmische Zustaende mit je einer Regel
 * dahinter, der Zeitstrahl unten und der Vollbildmodus.
 *
 * ## Die fuenf Kunstgriffe dieses Laufs
 *
 * 1. **Vier Akte, vier Bruecken.** Die Wiedergabe schiebt die Zeitstempel der
 *    Aufzeichnung auf die Gegenwart, und zwar EINMAL, beim Start der Bruecke.
 *    Ein Lauf, der danach zwei Minuten misst, misst am Ende an Akteuren, deren
 *    letztes Ereignis zwei Minuten zurueckliegt: die sind dann ruhig, und das
 *    ist richtig so. Was gemessen werden soll, ist aber die Bewegung. Jeder Akt
 *    bekommt darum seine eigene Bruecke auf einem eigenen Port und laedt die
 *    Seite neu; damit steht jede Messung an frischen Zeiten, statt an Zeiten,
 *    die der Lauf selbst hat altern lassen.
 *
 * 2. **Die Bahn wird nicht abgefragt, sie wird aufgezeichnet.** Der Koerper
 *    schreibt waehrend eines Fluges jeden Punkt, den er zeichnet, in eine
 *    Tabelle (src/galaxy/AgentLayer.tsx, `agentMotion`). Dieser Lauf liest sie
 *    NACH dem Flug und rechnet die Kruemmung selbst aus den Punkten nach. Eine
 *    halbe Sekunde lang alle 25 ms nachzufragen hiesse, die Messung an der
 *    Antwortzeit einer Fernsteuerung aufzuhaengen.
 *
 * 3. **Die Radien werden unabhaengig nachgerechnet.** Welchen Bahnradius drei
 *    Akteure am selben Symbol bekommen, rechnet dieser Lauf aus der Regel
 *    (Radius der Art, dann Kennung, dann Mindestabstand) noch einmal aus und
 *    haelt das Ergebnis gegen die Ansicht. Die Ansicht gegen ihre eigene Zahl
 *    zu pruefen waere keine Pruefung.
 *
 * 4. **Der Puls wird an ZWEI Instrumenten gemessen.** Die Groesse des Koerpers
 *    kommt aus dem DOM (`getBoundingClientRect`), das Licht eines Knotens aus
 *    den Pixeln eines Bildausschnitts. Beide ueber dieselben Bilder. Der Satz,
 *    der dabei geprueft wird, ist "ein Knoten leuchtet gleichmaessig, ein Agent
 *    atmet", und er ist erst dann gemessen, wenn beide Zahlen daneben stehen.
 *
 * 5. **Die Bewegung wird als Bildserie belegt.** Sechzehn Einzelbilder in
 *    gleichen Abstaenden der gesteuerten Zeit (ein Bild je Wiedergabeschritt),
 *    ein Kontaktabzug daraus, dazu eine Aufnahme fuer den Menschen. Zwei
 *    aufeinanderfolgende Bilder, zwischen denen sich nichts aendert, sind ein
 *    Befund und kein Erfolg; dieser Lauf vergleicht sie Pixel fuer Pixel.
 *
 * ## Ports
 *
 * Ab 4520. Dem Nutzer gehoeren 4141 (Modell-Sidecar), 4142 (seine Bruecke),
 * 4390/4391 und 4392/4393 (zwei Vorschauen). Keinen davon fasst dieser Lauf an,
 * weder startend noch beendend.
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w11b).
 */

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
    countListeners,
    findFreePort,
    indexRepository,
    sleep,
    startServer,
    stopServer,
} from './lib/cbm-server.mjs';
import {
    READABILITY_EXCLUSIONS,
    closeTooltips,
    measureReadability,
    resetScroll,
} from './lib/readability.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const EVENTS = join(ROOT, 'fixtures', 'agent-events', 'w11b-replay.jsonl');
const PROJECT = 'codeatlasweb-w11b';
const OUT_DIR = join(ROOT, 'verification', 'w11');
const FRAME_DIR = join(OUT_DIR, 'frames');
const OUT_JSON = join(OUT_DIR, 'motion.json');
const CONTACT_SHEET = join(OUT_DIR, 'contact-sheet.png');
const VIDEO_FILE = join(OUT_DIR, 'live.webm');
const SHOT_CINEMA = join(OUT_DIR, 'cinema.png');
const SHOT_TRAILS = join(OUT_DIR, 'trails.png');
const SHOT_FOLLOW = join(OUT_DIR, 'follow.png');
const SHOT_TIMELINE = join(OUT_DIR, 'timeline.png');

const FFMPEG = '/opt/homebrew/bin/ffmpeg';

/** 4141, 4142, 4390-4393 gehoeren dem Nutzer. Dieser Lauf faengt bei 4520 an. */
const MIN_PORT = 4520;

/** Wie viele Ereignisse die Datei traegt. */
const FIXTURE_EVENTS = 73;

/** Wie viele Ereignisse ein grober Schritt der Wiedergabe einspielt. */
const STEP_SIZE = 4;

/** Wie lange nach einem groben Schritt gewartet wird. */
const STEP_SETTLE_MS = 180;

/** Wie lange nach einem EINZELNEN Ereignis gewartet wird: ein Flug plus Luft. */
const SINGLE_SETTLE_MS = 720;

/** Wie viele Einzelbilder die Serie traegt. */
const SERIES_FRAMES = 16;

/** Wie viele Proben der Puls bekommt. */
const PULSE_SAMPLES = 10;

/** Die Bahnradien je Art, wortgleich mit src/galaxy/AgentLayer.tsx (ORBITS). */
const ORBIT_RADII = { read: 34, write: 15, search: 34, test: 18, other: 24 };

/** Der Mindestabstand zweier Bahnen um denselben Knoten (ORBIT_SEPARATION). */
const ORBIT_SEPARATION = 34;

/** Die erwartete Dauer eines Ortswechsels, in Millisekunden (TRANSITION_MS). */
const EXPECTED_TRANSITION_MS = 450;

/** Der Deckel gezeichneter Koerper (DRAWN_BODIES_CAP). */
const EXPECTED_CAP = 8;

/** Das Symbol, das der Leser oeffnet, damit "you" ein neunter Akteur wird. */
const YOU_FILE = 'src/services/userService.ts';

const MAIN_VIEWPORT = { width: 1680, height: 1050 };
const SECOND_VIEWPORT = { width: 1440, height: 900 };

/** Ab dieser Summe ueber die drei Kanaele hat sich ein Pixel geaendert. */
const DIFF_EPSILON = 12;

/** Chromium ohne Aussenwelt, plus die GL-Flags aus smoke-w9. */
const CHROMIUM_ARGS = [
    '--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1, EXCLUDE localhost',
    '--disable-background-networking',
    '--disable-component-update',
    '--disable-domain-reliability',
    '--disable-client-side-phishing-detection',
    '--disable-sync',
    '--disable-default-apps',
    '--no-first-run',
    '--no-default-browser-check',
    '--metrics-recording-only',
    '--no-pings',
    '--disable-breakpad',
    '--disable-features=Translate,OptimizationHints,MediaRouter,AutofillServerCommunication,InterestFeedContentSuggestions,DialMediaRouteProvider,CalculateNativeWinOcclusion',
    '--use-angle=swiftshader',
    '--enable-unsafe-swiftshader',
    '--ignore-gpu-blocklist',
];

const log = (...parts) => console.log('[smoke-w11b]', ...parts);
const serverLog = [];

function run(command, args, options = {}) {
    return new Promise((done) => {
        const child = spawn(command, args, {
            cwd: ROOT,
            env: {
                ...process.env,
                NO_UPDATE_NOTIFIER: '1',
                npm_config_update_notifier: 'false',
                npm_config_audit: 'false',
                npm_config_fund: 'false',
                ...(options.env ?? {}),
            },
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        let out = '';
        child.stdout.on('data', (d) => { out += d.toString(); });
        child.stderr.on('data', (d) => { out += d.toString(); });
        child.on('error', (error) => done({ code: 127, out: out + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out }));
    });
}

// ----------------------------------------------------------------- Bruecke ---

const bridges = [];

/** Die Bruecke im Wiedergabemodus starten und warten, bis sie antwortet. */
async function startBridge(port, sink) {
    const child = spawn(process.execPath, [
        join(ROOT, 'tools', 'agent-bridge.mjs'),
        '--replay', EVENTS,
        '--port', String(port),
    ], { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout.on('data', (d) => sink.push(`[${port} stdout] ${d.toString().trimEnd()}`));
    child.stderr.on('data', (d) => sink.push(`[${port} stderr] ${d.toString().trimEnd()}`));
    let exited = null;
    child.on('exit', (code, signal) => { exited = { code, signal }; });
    bridges.push(child);

    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
        if (exited !== null) {
            throw new Error(`die Bruecke endete vorzeitig (code=${exited.code})\n${sink.join('\n')}`);
        }
        try {
            const response = await fetch(`http://127.0.0.1:${port}/health`);
            const body = await response.json();
            if (response.status === 200) {
                return { child, health: body };
            }
        } catch {
            // lauscht noch nicht
        }
        await sleep(150);
    }
    child.kill('SIGKILL');
    throw new Error(`die Bruecke war binnen 15000 ms nicht auf ${port} bereit`);
}

async function stopBridge(child) {
    if (child === null || child === undefined || child.exitCode !== null) {
        return;
    }
    child.kill('SIGTERM');
    for (let i = 0; i < 40; i += 1) {
        if (child.exitCode !== null || child.signalCode !== null) {
            return;
        }
        await sleep(100);
    }
    child.kill('SIGKILL');
    await sleep(300);
}

/** Die Wiedergabe um `count` Ereignisse weiterdrehen. */
async function advance(port, count) {
    const response = await fetch(`http://127.0.0.1:${port}/replay/advance?count=${count}`, {
        method: 'POST',
    });
    if (!response.ok) {
        throw new Error(`POST /replay/advance antwortete mit ${response.status}`);
    }
    return response.json();
}

// -------------------------------------------------------------- Testgriffe ---

const agentSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasAgents;
        return seam === undefined ? null : JSON.parse(JSON.stringify(seam));
    });

/** Nur die Teile, die sich in jedem Bild aendern. Klein und schnell. */
const livePulse = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasAgents;
        const cores = {};
        const opacity = {};
        for (const node of document.querySelectorAll('[data-testid="atlas-agent-body"]')) {
            const actor = node.getAttribute('data-actor') ?? '';
            const core = node.querySelector('[data-testid="atlas-agent-core"]');
            const box = core?.getBoundingClientRect();
            cores[actor] = box === undefined ? 0 : Number(box.height.toFixed(3));
            opacity[actor] = Number(globalThis.getComputedStyle(node).opacity);
        }
        return {
            at: Date.now(),
            cores,
            opacity,
            scales: { ...(seam?.pulses ?? {}) },
            angles: { ...(seam?.angles ?? {}) },
            positions: JSON.parse(JSON.stringify(seam?.positions ?? {})),
            tails: { ...(seam?.tails ?? {}) },
        };
    });

/** Die Kameralage, so klein wie moeglich: sie wird oft gelesen. */
const cameraAt = (page) =>
    page.evaluate(() => {
        const camera = globalThis.__atlasAgents?.camera;
        return camera === undefined
            ? null
            : { at: Date.now(), x: camera.position.x, y: camera.position.y, z: camera.position.z };
    });

/** Was auf dem Graphen wirklich als Koerper steht. */
const bodies = (page) =>
    page.evaluate(() =>
        [...document.querySelectorAll('[data-testid="atlas-agent-body"]')].map((node) => {
            const core = node.querySelector('[data-testid="atlas-agent-core"]');
            const coreBox = core?.getBoundingClientRect();
            const box = node.getBoundingClientRect();
            return {
                actor: node.getAttribute('data-actor') ?? '',
                kind: node.getAttribute('data-kind') ?? '',
                idle: node.getAttribute('data-idle') === 'true',
                pulseMs: Number(node.getAttribute('data-pulse-ms') ?? '-1'),
                coreHeight: coreBox === undefined ? 0 : Number(coreBox.height.toFixed(2)),
                x: Math.round(box.x + box.width / 2),
                y: Math.round(box.y + box.height / 2),
            };
        }));

/**
 * Laufende Animationen auf den Elementen der Agentenebene, je Akteur.
 *
 * Gezaehlt werden auch Uebergaenge (`transition`), denn sie sind fuer diese
 * Frage dasselbe: ein Element, das sich veraendert. Bei einem Akteur, der
 * arbeitet, laufen sie (der Puls schreibt in jedem Bild eine neue Groesse); bei
 * einem ruhigen darf keine laufen, und genau das ist AC6.
 */
const runningAnimations = (page) =>
    page.evaluate(() => {
        const all = typeof document.getAnimations === 'function' ? document.getAnimations() : [];
        const rows = [];
        for (const animation of all) {
            const target = animation.effect?.target ?? null;
            if (target === null || typeof target.closest !== 'function') {
                continue;
            }
            const owner = target.closest('.atlas-agent, .atlas-agent-ghost, .atlas-agent-wave');
            if (owner === null) {
                continue;
            }
            rows.push({
                state: animation.playState,
                actor: owner.getAttribute('data-actor') ?? '',
                className: target.getAttribute('class') ?? '',
            });
        }
        return rows.filter((row) => row.state === 'running');
    });

/** Was das Instrument wirklich sagt. */
const hud = (page) =>
    page.evaluate(() => {
        const tidy = (value) => (value ?? '').replace(/\s+/g, ' ').trim();
        const root = document.querySelector('[data-testid="atlas-agents"]');
        if (root === null) {
            return { present: false, rows: [], switches: [], windows: [], ticker: [] };
        }
        const rect = root.getBoundingClientRect();
        return {
            present: true,
            size: root.getAttribute('data-size') ?? '',
            column: root.getAttribute('data-column') === 'true',
            source: root.getAttribute('data-source') ?? '',
            rect: {
                x: Math.round(rect.x), y: Math.round(rect.y),
                width: Math.round(rect.width), height: Math.round(rect.height),
            },
            cap: (() => {
                const node = root.querySelector('[data-testid="atlas-agents-cap"]');
                return node === null
                    ? { present: false, text: '', capped: 0, cap: 0, drawn: 0 }
                    : {
                        present: true,
                        text: tidy(node.textContent),
                        capped: Number(node.getAttribute('data-capped') ?? '0'),
                        cap: Number(node.getAttribute('data-cap') ?? '0'),
                        drawn: Number(node.getAttribute('data-drawn') ?? '0'),
                    };
            })(),
            rows: [...root.querySelectorAll('[data-testid="atlas-agents-row"]')].map((row) => {
                const name = row.querySelector('[data-testid="atlas-agents-name"]');
                const place = row.querySelector('[data-testid="atlas-agents-place"]');
                return {
                    actor: row.getAttribute('data-actor') ?? '',
                    idle: row.getAttribute('data-idle') === 'true',
                    drawn: row.getAttribute('data-drawn') === 'true',
                    lines: Number(row.getAttribute('data-lines') ?? '1'),
                    name: tidy(name?.textContent),
                    nameClipped: name === null
                        ? true
                        : name.scrollWidth > name.clientWidth + 1,
                    place: tidy(place?.textContent),
                    placeClipped: place === null
                        ? true
                        : place.scrollWidth > place.clientWidth + 1,
                    opacity: Number(globalThis.getComputedStyle(row).opacity),
                    path: tidy(row.querySelector('[data-testid="atlas-agents-path"]')?.textContent),
                    height: Math.round(row.getBoundingClientRect().height),
                };
            }),
            switches: [...root.querySelectorAll('[data-testid="atlas-agents-switch"]')]
                .map((node) => ({
                    name: node.getAttribute('data-switch') ?? '',
                    active: node.getAttribute('data-active') === 'true',
                    label: tidy(node.textContent),
                })),
            windows: [...root.querySelectorAll('[data-testid="atlas-agents-window-option"]')]
                .map((node) => ({
                    option: Number(node.getAttribute('data-option') ?? '-1'),
                    active: node.getAttribute('data-active') === 'true',
                    label: tidy(node.textContent),
                })),
            ticker: [...root.querySelectorAll('[data-testid="atlas-agents-ticker-row"]')]
                .map((node) => ({
                    actor: node.getAttribute('data-actor') ?? '',
                    kind: node.getAttribute('data-kind') ?? '',
                    place: node.getAttribute('data-place') ?? '',
                    lines: node.getAttribute('data-lines') ?? '',
                    text: tidy(node.textContent),
                })),
            text: tidy(root.textContent),
        };
    });

/** Was der Zeitstrahl zeigt. */
const timelineState = (page) =>
    page.evaluate(() => {
        const tidy = (value) => (value ?? '').replace(/\s+/g, ' ').trim();
        const root = document.querySelector('[data-testid="atlas-agents-timeline"]');
        if (root === null) {
            return { present: false, tracks: [], ticks: 0 };
        }
        const rect = root.getBoundingClientRect();
        return {
            present: true,
            mode: root.getAttribute('data-mode') ?? '',
            tracks: [...root.querySelectorAll('[data-testid="atlas-agents-timeline-track"]')]
                .map((node) => ({
                    actor: node.getAttribute('data-actor') ?? '',
                    count: Number(node.getAttribute('data-count') ?? '-1'),
                    ticks: node.querySelectorAll('[data-testid="atlas-agents-timeline-tick"]').length,
                })),
            ticks: Number(root.getAttribute('data-ticks') ?? '-1'),
            windowMs: Number(root.getAttribute('data-window') ?? '-1'),
            from: Number(root.getAttribute('data-from') ?? '-1'),
            to: Number(root.getAttribute('data-to') ?? '-1'),
            modeLabel: tidy(root.querySelector('[data-testid="atlas-agents-timeline-mode"]')?.textContent),
            replayNote: tidy(
                root.querySelector('[data-testid="atlas-agents-timeline-replay"]')?.textContent,
            ),
            rect: {
                x: Math.round(rect.x), y: Math.round(rect.y),
                width: Math.round(rect.width), height: Math.round(rect.height),
            },
        };
    });

/** Das Rechteck der Zeichenflaeche, in Fensterkoordinaten. */
const sceneRect = (page) =>
    page.evaluate(() => {
        const node = document.querySelector('[data-testid="atlas-galaxy-scene"]');
        if (node === null) {
            return null;
        }
        const rect = node.getBoundingClientRect();
        return {
            x: Math.round(rect.x), y: Math.round(rect.y),
            width: Math.round(rect.width), height: Math.round(rect.height),
        };
    });

/** Wo das Panel steht, und wie gross das Fenster ist. */
const panelRect = (page) =>
    page.evaluate(() => {
        const node = document.querySelector('[data-testid="atlas-galaxy"]');
        const rect = node?.getBoundingClientRect();
        return {
            fullscreen: node?.getAttribute('data-fullscreen') === 'true',
            replay: node?.getAttribute('data-replay') === 'true',
            x: rect === undefined ? -1 : Math.round(rect.x),
            y: rect === undefined ? -1 : Math.round(rect.y),
            width: rect === undefined ? -1 : Math.round(rect.width),
            height: rect === undefined ? -1 : Math.round(rect.height),
            viewport: { width: globalThis.innerWidth, height: globalThis.innerHeight },
        };
    });

/** Die Bildrate, aus derselben Naht, die das Einstellungen-Panel liest. */
const frameRate = (page) =>
    page.evaluate(() => {
        const perf = globalThis.__atlasGalaxyPerf;
        return perf === undefined ? null : JSON.parse(JSON.stringify(perf));
    });

/**
 * Die Eigendrehung der Szene zuruecksetzen.
 *
 * Die Szene dreht sich nach 60 Sekunden ohne Beruehrung von selbst
 * (IdleAutoRotate in src/galaxy/GraphScene.tsx). Ein Lauf, der laenger dauert,
 * misst dann eine Bewegung, die er selbst nicht bestellt hat: ein Knoten, der
 * unter einem Messkasten wegwandert, sieht aus wie ein Knoten, der flackert.
 * Ein Mausrad-Ereignis mit dem Wert null setzt den Zaehler zurueck und aendert
 * die Ansicht nicht (OrbitControls zoomt nur bei einem Vorzeichen).
 */
async function keepAwake(page) {
    await page.evaluate(() => {
        const canvas = document.querySelector('[data-testid="atlas-galaxy-scene"] canvas');
        canvas?.dispatchEvent(new WheelEvent('wheel', { deltaY: 0, bubbles: true }));
    });
    await page.waitForTimeout(700);
}

/** Ein Bild aufnehmen und IN der Seite ablegen. Wortgleich mit smoke-w9/w10/w11a. */
async function grab(page, key, clip) {
    const shot = await page.screenshot({ clip });
    return page.evaluate(async ({ name, data }) => {
        const image = new Image();
        image.src = `data:image/png;base64,${data}`;
        await image.decode();
        const canvas = document.createElement('canvas');
        canvas.width = image.naturalWidth;
        canvas.height = image.naturalHeight;
        const ctx = canvas.getContext('2d', { willReadFrequently: true });
        ctx.drawImage(image, 0, 0);
        globalThis.__w11b = globalThis.__w11b ?? {};
        globalThis.__w11b[name] = ctx.getImageData(0, 0, canvas.width, canvas.height);
        return { width: canvas.width, height: canvas.height };
    }, { name: key, data: shot.toString('base64') });
}

/** Wie viel sich zwischen zwei abgelegten Bildern geaendert hat. */
const compareImages = (page, options) =>
    page.evaluate((input) => {
        const store = globalThis.__w11b ?? {};
        const first = store[input.base];
        const second = store[input.variant];
        if (first === undefined || second === undefined) {
            return null;
        }
        if (first.width !== second.width || first.height !== second.height) {
            return null;
        }
        let changed = 0;
        const total = first.data.length / 4;
        for (let i = 0; i < first.data.length; i += 4) {
            const delta = Math.abs(first.data[i] - second.data[i])
                + Math.abs(first.data[i + 1] - second.data[i + 1])
                + Math.abs(first.data[i + 2] - second.data[i + 2]);
            if (delta > input.epsilon) {
                changed += 1;
            }
        }
        return {
            changed,
            total,
            changedFraction: Number((changed / total).toFixed(6)),
        };
    }, options);

/**
 * Den hellsten Fleck eines abgelegten Bildes suchen, der von jedem Koerper weit
 * genug entfernt ist.
 *
 * Das ist der Knoten, an dem gemessen wird, ob er gleichmaessig leuchtet. Er
 * wird EINMAL gewaehlt, am ersten Bild der Reihe, und seine Koordinaten stehen
 * im Artefakt: eine Stelle, die je Bild neu gesucht wuerde, waere keine
 * Messung an derselben Stelle.
 */
const pickNodeBox = (page, options) =>
    page.evaluate((input) => {
        const frame = (globalThis.__w11b ?? {})[input.name];
        if (frame === undefined) {
            return null;
        }
        const far = (x, y) => input.bodies.every((body) =>
            Math.hypot(x - body.x, y - body.y) > input.distance);
        let best = null;
        for (let y = input.half; y < frame.height - input.half; y += 4) {
            for (let x = input.half; x < frame.width - input.half; x += 4) {
                const i = (y * frame.width + x) * 4;
                const luminance = 0.2126 * frame.data[i]
                    + 0.7152 * frame.data[i + 1] + 0.0722 * frame.data[i + 2];
                if ((best === null || luminance > best.luminance) && far(x, y)) {
                    best = { x, y, luminance: Number(luminance.toFixed(2)) };
                }
            }
        }
        return best;
    }, options);

/** Die mittlere Helligkeit eines Kastens in einem abgelegten Bild. */
const boxLuminance = (page, options) =>
    page.evaluate((input) => {
        const frame = (globalThis.__w11b ?? {})[input.name];
        if (frame === undefined) {
            return null;
        }
        let sum = 0;
        let count = 0;
        for (let y = input.y - input.half; y <= input.y + input.half; y += 1) {
            for (let x = input.x - input.half; x <= input.x + input.half; x += 1) {
                if (x < 0 || y < 0 || x >= frame.width || y >= frame.height) {
                    continue;
                }
                const i = (y * frame.width + x) * 4;
                sum += 0.2126 * frame.data[i]
                    + 0.7152 * frame.data[i + 1] + 0.0722 * frame.data[i + 2];
                count += 1;
            }
        }
        return count === 0 ? null : Number((sum / count).toFixed(4));
    }, options);

/** Wo jeder scrollbare Bereich steht. Wortgleich mit smoke-w9/w10/w11a. */
const scrollState = (page) =>
    page.evaluate(() => {
        const regions = [];
        for (const node of document.body.querySelectorAll('*')) {
            if (node.closest('.monaco-editor') !== null) {
                continue;
            }
            const style = globalThis.getComputedStyle(node);
            const scrollsY = (style.overflowY === 'auto' || style.overflowY === 'scroll')
                && node.scrollHeight > node.clientHeight + 1;
            const scrollsX = (style.overflowX === 'auto' || style.overflowX === 'scroll')
                && node.scrollWidth > node.clientWidth + 1;
            if (!scrollsY && !scrollsX) {
                continue;
            }
            regions.push({
                name: node.getAttribute('data-testid')
                    ?? (node.getAttribute('class') ?? '').split(/\s+/).filter(Boolean)[0]
                    ?? node.tagName.toLowerCase(),
                top: Math.round(node.scrollTop),
                left: Math.round(node.scrollLeft),
                hidden: Math.round(node.scrollHeight - node.clientHeight),
            });
        }
        return { regions, atRest: regions.every((region) => region.top <= 1 && region.left <= 1) };
    });

// ------------------------------------------------------------ Klickstrecke ---

async function openApp(page, origin, search) {
    await page.goto(`${origin}/?project=${PROJECT}${search}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 30000 });
    await page.waitForFunction(
        () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
        undefined,
        { timeout: 60000 },
    );
    await page.waitForFunction(
        () => globalThis.__atlasAgents !== undefined,
        undefined,
        { timeout: 30000 },
    );
}

/** Den Live-Modus ueber das Menue umlegen, so wie ein Leser es tut. */
async function liveOn(page) {
    await page.click('[data-menu="a-agents"]');
    try {
        await page.waitForFunction(
            () => (globalThis.__atlasAgents?.sourceState ?? '') === 'connected',
            undefined,
            { timeout: 25000 },
        );
    } catch (error) {
        const state = await page.evaluate(() => {
            const seam = globalThis.__atlasAgents;
            const menu = document.querySelector('[data-menu="a-agents"]');
            return {
                seam: seam === undefined
                    ? null
                    : {
                        on: seam.on, sourceState: seam.sourceState, origin: seam.origin,
                        requests: seam.requests, drops: seam.drops, error: seam.error,
                        port: seam.port,
                    },
                menuLabel: (menu?.textContent ?? '').trim(),
            };
        });
        throw new Error(`${error.message}\nNaht: ${JSON.stringify(state)}`);
    }
}

/**
 * Den Leser zu einem Akteur machen.
 *
 * Nicht das Oeffnen der Datei allein: der eigene Koerper entsteht am SYMBOL vor
 * dem Leser, und das ist der Caret im Editor. Dieselbe Strecke wie in
 * smoke-w11a.
 */
async function becomeActor(page) {
    await page.click(`[data-testid="atlas-tree-row"][data-path="${YOU_FILE}"]`);
    await page.waitForFunction(
        (path) => (globalThis.__atlasReader?.document?.path ?? '') === path,
        YOU_FILE,
        { timeout: 30000 },
    );
    await page.evaluate(() => {
        globalThis.__atlasReader?.editor?.setPosition?.({ lineNumber: 24, column: 5 });
        globalThis.__atlasReader?.editor?.focus?.();
    });
    await page.waitForFunction(
        () => (globalThis.__atlasAgents?.actors ?? []).some((actor) => actor.you),
        undefined,
        { timeout: 25000 },
    );
    await page.waitForTimeout(400);
}

/** Einen Schalter des Instruments auf einen Wert bringen. */
async function setSwitch(page, name, wanted) {
    const state = await page.evaluate((which) => {
        const node = document.querySelector(
            `[data-testid="atlas-agents-switch"][data-switch="${which}"]`,
        );
        return node === null ? null : node.getAttribute('data-active') === 'true';
    }, name);
    if (state === null || state === wanted) {
        return state !== null;
    }
    await page.click(`[data-testid="atlas-agents-switch"][data-switch="${name}"]`);
    await page.waitForTimeout(500);
    return true;
}

async function shoot(page, file, name, extras) {
    await closeTooltips(page);
    await resetScroll(page, READABILITY_EXCLUSIONS);
    await page.waitForTimeout(300);
    const state = await scrollState(page);
    await page.screenshot({ path: file, fullPage: false });
    const size = (await stat(file)).size;
    const shot = {
        name,
        atRest: state.atRest,
        bytes: size,
        regions: state.regions,
        why: state.atRest ? '' : 'ein Bereich stand nicht am Anfang, siehe regions',
    };
    extras.shots.push(shot);
    log(`${name}: aufgenommen (atRest=${state.atRest}, ${Math.round(size / 1024)} KB)`);
    return shot;
}

/** Die Kruemmung einer aufgezeichneten Bahn, aus ihren eigenen Punkten. */
function measureTrace(trace) {
    const from = trace.from;
    const to = trace.to;
    const dx = to.x - from.x;
    const dy = to.y - from.y;
    const chord = Math.hypot(dx, dy);
    const off = (point) => (chord === 0
        ? Math.hypot(point.x - from.x, point.y - from.y)
        : Math.abs(dy * point.x - dx * point.y + to.x * from.y - to.y * from.x) / chord);
    const offsets = trace.samples.map((sample) => off(sample));
    const times = trace.samples.map((sample) => sample.t);
    const middle = trace.samples[Math.floor(trace.samples.length / 2)];
    return {
        actor: trace.actor,
        fromNode: trace.fromNode,
        toNode: trace.toNode,
        chord: Number(chord.toFixed(2)),
        samples: trace.samples.length,
        durationMs: Math.round(trace.durationMs),
        curvature: chord === 0 ? 0 : Number((Math.max(...offsets) / chord).toFixed(5)),
        middleOffset: Number(off(middle).toFixed(3)),
        firstOffset: Number((offsets[0] ?? 0).toFixed(3)),
        lastOffset: Number((offsets[offsets.length - 1] ?? 0).toFixed(3)),
        timesIncrease: times.every((value, index) => index === 0 || value > times[index - 1]),
        done: trace.done === true,
    };
}

/** Die erwarteten Bahnradien, unabhaengig nachgerechnet. */
function expectedRadii(actors) {
    const groups = new Map();
    for (const actor of actors) {
        if (actor.nodeId < 0 || !actor.drawn) {
            continue;
        }
        groups.set(actor.nodeId, [...(groups.get(actor.nodeId) ?? []), actor]);
    }
    const out = {};
    for (const group of groups.values()) {
        const ordered = [...group].sort((a, b) =>
            (ORBIT_RADII[a.kind] ?? 24) - (ORBIT_RADII[b.kind] ?? 24)
            || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
        let last = Number.NEGATIVE_INFINITY;
        for (const actor of ordered) {
            const radius = Math.max(ORBIT_RADII[actor.kind] ?? 24, last + ORBIT_SEPARATION);
            out[actor.id] = radius;
            last = radius;
        }
    }
    return out;
}

const spread = (values) => {
    const clean = values.filter((value) => Number.isFinite(value) && value > 0);
    if (clean.length === 0) {
        return { min: 0, max: 0, ratio: 0, relative: 0 };
    }
    const min = Math.min(...clean);
    const max = Math.max(...clean);
    return {
        min: Number(min.toFixed(4)),
        max: Number(max.toFixed(4)),
        ratio: Number((max / min).toFixed(4)),
        relative: Number(((max - min) / max).toFixed(5)),
    };
};

// ------------------------------------------------------------------- Lauf ----

async function main() {
    const totalStarted = Date.now();
    const timings = {};
    let home = null;
    let runtimeDir = null;
    let serverChild = null;
    let proxy = null;
    let browser = null;
    let videoDir = null;
    let serverPort = 0;
    let uiPort = 0;
    const bridgePorts = [];
    let failure = null;

    const report = {
        transitionIsCurved: false,
        transitionSamples: [],
        transitionMs: 0,
        cometTailPresent: false,
        neverInTwoPlaces: false,
        trailNodes: 0,
        trailBelowEdges: false,
        trailDashed: false,
        trailInLegend: false,
        trailWindowSwitchable: false,
        trailsToggleOff: false,
        sameNodeDifferentRadii: false,
        radiiDeterministic: false,
        burstMakesOneWave: false,
        followSpringNoOvershoot: false,
        followLineShowsMeasuredOnly: false,
        pulseFollowsActivity: false,
        pulseFrames: 0,
        pulseStopsWhenIdle: false,
        pulseDistinctFromNodeGlow: false,
        timelineTracksPerActor: 0,
        timelinePauseKeepsEvents: false,
        timelineScrubMarkedAsReplay: false,
        cinemaFillsViewport: false,
        cinemaHasTicker: false,
        cinemaEscapeKeepsCamera: false,
        cinemaSizes: [],
        idleHasNoAnimation: false,
        idleAgentFadesNotDisappears: false,
        framesPerSecondMin: 0,
        drawnBodiesCap: 0,
        capReported: false,
        effectsToggleable: false,
        thriftProfileKeepsCinemaUsable: false,
        frameSeriesCount: 0,
        contactSheetWritten: false,
        videoWritten: false,
        framesDifferBetweenSteps: false,
        hudNamesReadable: false,
        overlapViolations: 0,
        clippingViolations: 0,
        cutWithoutHint: 0,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = {
        blockedRequests: [],
        consoleErrors: [],
        pageErrors: [],
        readability: [],
        shots: [],
        bridgeLog: [],
        acts: {},
    };

    try {
        if (!existsSync(BINARY)) {
            throw new Error(`Binary fehlt: ${BINARY}`);
        }
        if (!existsSync(EVENTS)) {
            throw new Error(`Ereignisdatei fehlt: ${EVENTS}`);
        }

        // ------------------------------------------------------------ 1. Bau
        log('npm run build');
        const buildStarted = Date.now();
        const build = await run('npm', ['run', 'build']);
        timings.buildMs = Date.now() - buildStarted;
        if (build.code !== 0) {
            throw new Error(`npm run build endete mit ${build.code}: ${build.out.trim().slice(-600)}`);
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w11b-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w11b-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);

        // -------------------------------------------------------- 3. Index
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // ------------------------------------------------- 4. Server, Proxy
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        const taken = [serverPort, uiPort];
        for (let i = 0; i < 4; i += 1) {
            const port = await findFreePort(MIN_PORT, taken);
            taken.push(port);
            bridgePorts.push(port);
        }
        extras.ports = { serverPort, uiPort, bridgePorts };
        log(`C-Server ${serverPort}, dist/ ${uiPort}, Bruecken ${bridgePorts.join(', ')}`);

        // ------------------------------------------------------- 5. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const origin = `http://127.0.0.1:${uiPort}`;
        const bridgeOrigins = bridgePorts.map((port) => `http://127.0.0.1:${port}`);

        await mkdir(OUT_DIR, { recursive: true });
        /*
         * Nur die eigenen Bilder wegraeumen, nicht den ganzen Ordner: daneben
         * liegt die Serie von W11a (`orbit-NN.png`), und die Reihenfolge zweier
         * Beweislaeufe darf kein Ergebnis entscheiden.
         */
        await mkdir(FRAME_DIR, { recursive: true });
        for (const name of await readdir(FRAME_DIR)) {
            if (name.startsWith('step-')) {
                await rm(join(FRAME_DIR, name), { force: true });
            }
        }

        const newContext = async (options = {}) => {
            const context = await browser.newContext({
                viewport: { ...MAIN_VIEWPORT },
                ...options,
            });
            await context.route(
                (url) => !bridgeOrigins.some((entry) => url.href.startsWith(entry)),
                async (route) => {
                    const url = route.request().url();
                    if (url.startsWith(origin) || url.startsWith('data:') || url.startsWith('blob:')) {
                        await route.continue();
                        return;
                    }
                    extras.blockedRequests.push(url);
                    await route.abort();
                },
            );
            const page = await context.newPage();
            page.on('console', (message) => {
                if (message.type() === 'error') {
                    extras.consoleErrors.push(message.text());
                }
            });
            page.on('pageerror', (error) => extras.pageErrors.push(String(error)));
            return { context, page };
        };

        const readability = async (page, name) => {
            const measured = await measureReadability(page, READABILITY_EXCLUSIONS);
            extras.readability.push({
                name,
                candidates: measured.candidates,
                overlaps: measured.overlaps,
                clipped: measured.clipped,
            });
            report.overlapViolations += measured.overlaps.length;
            report.clippingViolations += measured.clipped.length;
            report.cutWithoutHint += measured.clipped
                .filter((entry) => entry.kind === 'cut-without-hint').length;
            return measured;
        };

        /* ============================================================ AKT A */
        /*
         * Die Bewegung selbst: der Flug, der Schweif, die Spur, die Radien, der
         * Puls und die Ruhe. Alles an frischen Zeiten, direkt nach der
         * Wiedergabe.
         */
        const actA = {};
        extras.acts.a = actA;
        {
            const port = bridgePorts[0];
            const bridge = await startBridge(port, extras.bridgeLog);
            actA.health = bridge.health;
            if (bridge.health.events !== FIXTURE_EVENTS) {
                throw new Error(
                    `die Bruecke las ${bridge.health.events} Ereignisse, erwartet ${FIXTURE_EVENTS}`,
                );
            }
            const { context, page } = await newContext();
            await openApp(page, origin, `&agents=${port}`);
            await liveOn(page);

            /* Der Leser wird ein neunter Akteur, damit der Deckel wirklich greift. */
            await becomeActor(page);

            /* Die Spur ist an: sie ist der Gegenstand von AC2. */
            await setSwitch(page, 'trails', true);

            /* Grob bis auf die letzten 24 Ereignisse. */
            let remaining = FIXTURE_EVENTS;
            while (remaining > 24) {
                const answer = await advance(port, STEP_SIZE);
                remaining = answer.remaining;
                await page.waitForTimeout(STEP_SETTLE_MS);
            }

            /*
             * Und die letzten einzeln, mit Luft dazwischen: nur so bleibt ein
             * Flug stehen, statt vom naechsten ueberschrieben zu werden.
             */
            const traces = new Map();
            const tailSeen = [];
            const twoPlaces = [];
            while (remaining > 0) {
                const answer = await advance(port, 1);
                remaining = answer.remaining;
                for (let probe = 0; probe < 6; probe += 1) {
                    await page.waitForTimeout(50);
                    const live = await livePulse(page);
                    for (const [actor, points] of Object.entries(live.tails)) {
                        if (points >= 2) {
                            tailSeen.push({ actor, points, at: live.at });
                        }
                    }
                    const counted = await page.evaluate(() => {
                        const seen = {};
                        for (const node of document.querySelectorAll(
                            '[data-testid="atlas-agent-body"]',
                        )) {
                            const actor = node.getAttribute('data-actor') ?? '';
                            seen[actor] = (seen[actor] ?? 0) + 1;
                        }
                        return seen;
                    });
                    twoPlaces.push(Math.max(0, ...Object.values(counted)));
                }
                await page.waitForTimeout(SINGLE_SETTLE_MS - 300);
                const seam = await agentSeam(page);
                for (const trace of Object.values(seam?.motion ?? {})) {
                    if (trace.done !== true || trace.samples.length < 3) {
                        continue;
                    }
                    traces.set(`${trace.actor}:${trace.fromNode}->${trace.toNode}`, trace);
                }
            }

            const measured = [...traces.values()].map(measureTrace)
                .sort((a, b) => b.chord - a.chord);
            actA.traces = measured;
            const best = measured[0];
            report.transitionIsCurved = best !== undefined
                && best.curvature > 0.05
                && best.middleOffset > best.firstOffset
                && best.middleOffset > best.lastOffset
                && measured.filter((trace) => trace.curvature > 0.05).length >= 2;
            if (best !== undefined) {
                const trace = traces.get(`${best.actor}:${best.fromNode}->${best.toNode}`);
                const points = trace.samples;
                report.transitionSamples = [
                    { at: 'start', ...points[0] },
                    { at: 'middle', ...points[Math.floor(points.length / 2)] },
                    { at: 'end', ...points[points.length - 1] },
                ].map((point) => ({
                    at: point.at,
                    t: Math.round(point.t),
                    x: Number(point.x.toFixed(2)),
                    y: Number(point.y.toFixed(2)),
                    z: Number(point.z.toFixed(2)),
                }));
                actA.bestTrace = { ...best, from: trace.from, to: trace.to };
            }
            const durations = measured.map((trace) => trace.durationMs).sort((a, b) => a - b);
            report.transitionMs = durations.length === 0
                ? 0
                : durations[Math.floor(durations.length / 2)];
            report.cometTailPresent = tailSeen.length >= 3;
            /*
             * "Nie an zwei Orten" hat zwei Haelften, und beide werden gemessen:
             * im BILD steht je Akteur hoechstens ein Koerper, und in der
             * aufgezeichneten Bahn gehoert zu jedem Zeitpunkt genau ein Punkt.
             */
            report.neverInTwoPlaces = twoPlaces.length > 0
                && Math.max(...twoPlaces) <= 1
                && measured.length > 0
                && measured.every((trace) => trace.timesIncrease);
            actA.tailSeen = tailSeen.slice(0, 12);
            actA.tailObservations = tailSeen.length;
            actA.bodyCounts = { samples: twoPlaces.length, max: Math.max(0, ...twoPlaces) };
            log(`AC1: ${measured.length} Fluege, beste Kruemmung ${best?.curvature}, `
                + `Dauer ${report.transitionMs} ms, Schweif ${tailSeen.length}x gesehen`);

            /* ------------------------------------------------- AC2: die Spur */
            /*
             * Gemessen am Fenster von fuenf Minuten und nicht an dem von einer.
             * Der Grund ist eine Eigenschaft der Wiedergabe und keine der Spur:
             * das Fenster von einer Minute wandert mit der Wanduhr, und was der
             * Lauf bis hierher an Zeit gebraucht hat, faellt hinten heraus. Die
             * Zusicherung ist der DECKEL (hoechstens zehn Knoten), und der ist
             * an einem Fenster zu pruefen, in dem der Akteur mehr als zehn
             * Symbole beruehrt hat.
             */
            await page.click('[data-testid="atlas-agents-window-option"][data-option="300000"]');
            await page.waitForTimeout(700);
            const seam = await agentSeam(page);
            actA.seam = seam;
            const trails = (seam?.actors ?? []).map((actor) => actor.trail.length);
            report.trailNodes = Math.max(0, ...trails);
            report.trailBelowEdges = seam !== null
                && seam.renderOrders.trails > 0
                && seam.renderOrders.trail < seam.renderOrders.others;
            report.trailDashed = seam !== null
                && seam.renderOrders.dash[0] > 0 && seam.renderOrders.dash[1] > 0;
            actA.trails = {
                lengths: Object.fromEntries((seam?.actors ?? [])
                    .map((actor) => [actor.id, actor.trail.length])),
                renderOrders: seam?.renderOrders,
            };

            /* Die Legende sagt den Unterschied noch einmal in Worten. */
            await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
            await page.waitForTimeout(400);
            const legend = await page.evaluate(() => {
                const node = document.querySelector('[data-entry="agent-trail"]');
                return node === null
                    ? { present: false, text: '' }
                    : { present: true, text: (node.textContent ?? '').replace(/\s+/g, ' ').trim() };
            });
            report.trailInLegend = legend.present
                && /not a relation in the code/i.test(legend.text)
                && /under the real edges/i.test(legend.text);
            actA.legend = legend;
            await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
            await page.waitForTimeout(300);

            /* Vier Fenster, und die Spur folgt ihnen. */
            const windows = [];
            for (const option of [900000, 0, 60000, 300000]) {
                await page.click(
                    `[data-testid="atlas-agents-window-option"][data-option="${option}"]`,
                );
                await page.waitForTimeout(600);
                const view = await agentSeam(page);
                windows.push({
                    option,
                    trailWindowMs: view?.trailWindowMs ?? -1,
                    trails: Object.fromEntries((view?.actors ?? [])
                        .map((actor) => [actor.id, actor.trail.length])),
                });
            }
            const offered = (await hud(page)).windows.map((entry) => entry.option).sort((a, b) => a - b);
            report.trailWindowSwitchable = offered.join(',') === '0,60000,300000,900000'
                && windows.every((entry) => entry.trailWindowMs === entry.option);
            actA.windows = { offered, measured: windows };

            /* Und TRAILS schaltet sie ganz ab. */
            await setSwitch(page, 'trails', false);
            await page.waitForTimeout(1700);
            const withoutTrails = await agentSeam(page);
            report.trailsToggleOff = (withoutTrails?.renderOrders.trails ?? -1) === 0;
            actA.trailsOff = withoutTrails?.renderOrders;
            await setSwitch(page, 'trails', true);
            await page.waitForTimeout(500);
            log(`AC2: Spur ${report.trailNodes} Knoten, unter den Kanten `
                + `${report.trailBelowEdges}, gestrichelt ${report.trailDashed}, `
                + `Legende ${report.trailInLegend}, aus ${report.trailsToggleOff}`);

            /* --------------------------------- AC3a: dieselbe Stelle, andere Bahn */
            const now = await agentSeam(page);
            const byNode = new Map();
            for (const actor of (now?.actors ?? []).filter((entry) => entry.drawn)) {
                if (actor.nodeId < 0) {
                    continue;
                }
                byNode.set(actor.nodeId, [...(byNode.get(actor.nodeId) ?? []), actor]);
            }
            const shared = [...byNode.values()].filter((group) => group.length >= 2);
            const expected = expectedRadii(now?.actors ?? []);
            report.sameNodeDifferentRadii = shared.length >= 1
                && shared.every((group) => {
                    const radii = group.map((actor) => now.radii[actor.id]);
                    return new Set(radii).size === radii.length;
                });
            report.radiiDeterministic = Object.keys(expected).length > 0
                && Object.entries(expected)
                    .every(([id, radius]) => now.radii[id] === radius);
            actA.radii = {
                measured: now?.radii,
                expected,
                shared: shared.map((group) => group.map((actor) => ({
                    id: actor.id, kind: actor.kind, nodeId: actor.nodeId,
                    radius: now.radii[actor.id],
                }))),
                rule: 'Radius der Art, dann Kennung, dann Mindestabstand 34 Welteinheiten. '
                    + 'Unabhaengig aus derselben Regel nachgerechnet, nicht aus der Ansicht gelesen.',
            };
            log(`AC3a: ${shared.length} Symbole mit mehreren Akteuren, Radien verschieden `
                + `${report.sameNodeDifferentRadii}, nachgerechnet ${report.radiiDeterministic}`);

            /*
             * Der Deckel wird HIER gemessen und nicht am Ende des Aktes.
             *
             * Ein Akteur faellt drei Minuten nach seinem letzten Ereignis aus
             * dem Bild (ACTIVE_WINDOW_MS), und die Wiedergabe laesst die
             * Zeitstempel mit jeder Sekunde des Zusehens altern. Der neunte
             * Akteur, an dem der Deckel ueberhaupt greift, ist damit nicht
             * beliebig lange da; gemessen wird, solange er da ist.
             */
            report.drawnBodiesCap = now?.cap ?? 0;
            const capHud = await hud(page);
            report.capReported = capHud.cap.present
                && capHud.cap.capped >= 1
                && capHud.cap.cap === EXPECTED_CAP
                && capHud.cap.drawn === EXPECTED_CAP
                && /bodies drawn/i.test(capHud.cap.text);
            actA.cap = {
                actors: (now?.actors ?? []).length,
                drawn: now?.drawn ?? -1,
                capped: now?.capped ?? -1,
                line: capHud.cap,
            };
            log(`AC7 Deckel: ${report.drawnBodiesCap}, gemeldet ${report.capReported} `
                + `(${capHud.cap.text})`);

            /* --------------------------------------------- AC3b: der Puls */
            await keepAwake(page);
            const stage = await sceneRect(page);
            const drawn = await bodies(page);
            const pulseFrames = [];
            for (let i = 0; i < PULSE_SAMPLES; i += 1) {
                const live = await livePulse(page);
                await grab(page, `pulse-${i}`, stage);
                pulseFrames.push(live);
                await page.waitForTimeout(90);
            }
            /*
             * Der Kasten wird so weit weg von jedem Koerper gewaehlt, wie die
             * Flaeche es hergibt: erst 140 Pixel, dann weniger. Der genommene
             * Abstand steht im Artefakt, damit die Messung nicht besser aussieht
             * als sie ist.
             */
            const bodyPoints = drawn.map((body) => ({
                x: body.x - stage.x, y: body.y - stage.y,
            }));
            let nodeBox = null;
            let nodeDistance = 0;
            for (const distance of [140, 100, 70, 45, 28]) {
                nodeBox = await pickNodeBox(page, {
                    name: 'pulse-0', half: 12, distance, bodies: bodyPoints,
                });
                if (nodeBox !== null && nodeBox.luminance > 20) {
                    nodeDistance = distance;
                    break;
                }
                nodeBox = null;
            }
            const nodeLuminance = [];
            for (let i = 0; i < PULSE_SAMPLES; i += 1) {
                if (nodeBox === null) {
                    break;
                }
                nodeLuminance.push(await boxLuminance(page, {
                    name: `pulse-${i}`, x: nodeBox.x, y: nodeBox.y, half: 12,
                }));
            }

            const actors = Object.fromEntries((now?.actors ?? []).map((a) => [a.id, a]));
            const perActor = {};
            for (const id of Object.keys(pulseFrames[0]?.cores ?? {})) {
                const sizes = pulseFrames.map((frame) => frame.cores[id]);
                const scales = pulseFrames.map((frame) => frame.scales[id] ?? 0);
                perActor[id] = {
                    events: actors[id]?.recentEvents ?? -1,
                    pulseMs: actors[id]?.pulseMs ?? -1,
                    amplitude: actors[id]?.pulseAmplitude ?? -1,
                    idle: actors[id]?.idle ?? false,
                    sizes,
                    size: spread(sizes),
                    scale: spread(scales),
                };
            }
            const busy = Object.entries(perActor)
                .filter(([, entry]) => !entry.idle && entry.events > 0)
                .sort((a, b) => b[1].events - a[1].events);
            const busiest = busy[0];
            const quietest = busy[busy.length - 1];
            const still = Object.entries(perActor).find(([, entry]) => entry.idle);
            report.pulseFrames = pulseFrames.length;
            report.pulseFollowsActivity = busiest !== undefined && quietest !== undefined
                && busiest[0] !== quietest[0]
                && busiest[1].events > quietest[1].events
                && busiest[1].pulseMs < quietest[1].pulseMs
                && busiest[1].amplitude > quietest[1].amplitude
                && busiest[1].size.ratio > quietest[1].size.ratio
                && busiest[1].size.ratio > 1.05;
            report.pulseStopsWhenIdle = still !== undefined
                && still[1].pulseMs === 0
                && still[1].size.ratio === 1
                && pulseFrames.length >= 6;
            const nodeSpread = spread(nodeLuminance);
            report.pulseDistinctFromNodeGlow = nodeBox !== null
                && nodeLuminance.length >= 6
                && nodeSpread.relative < 0.02
                && busiest !== undefined
                && busiest[1].size.relative > 0.15;
            actA.pulse = {
                frames: pulseFrames.length,
                perActor,
                busiest: busiest?.[0] ?? '',
                quietest: quietest?.[0] ?? '',
                idleActor: still?.[0] ?? '',
                nodeBox,
                nodeDistance,
                nodeLuminance,
                nodeSpread,
                method:
                    'Die Groesse des Koerpers kommt aus dem DOM (getBoundingClientRect der Kern-'
                    + 'Kugel), das Licht eines Knotens aus den Pixeln eines Kastens von 25 mal 25 '
                    + 'Pixeln um den hellsten Punkt des ersten Bildes, der mindestens 140 Pixel von '
                    + 'jedem Koerper entfernt liegt. Beide ueber dieselben zehn Bilder. Ein Knoten '
                    + 'leuchtet gleichmaessig, ein Agent atmet: die eine Zahl steht still, die '
                    + 'andere nicht.',
            };
            log(`AC3b: ${pulseFrames.length} Bilder, ${busiest?.[0]} `
                + `(${busiest?.[1].events} Ereignisse, ${busiest?.[1].pulseMs} ms, `
                + `Groesse x${busiest?.[1].size.ratio}) gegen ${quietest?.[0]} `
                + `(${quietest?.[1].events}, ${quietest?.[1].pulseMs} ms, `
                + `x${quietest?.[1].size.ratio}), Knotenlicht ${nodeSpread.relative}`);

            /* ----------------------------------------------- AC6: die Ruhe */
            const idleActor = still?.[0] ?? '';
            const allAnimations = await runningAnimations(page);
            const animations = allAnimations.filter((entry) => entry.actor === idleActor);
            const idlePositions = pulseFrames.map((frame) => frame.positions[idleActor]);
            const idleAngles = pulseFrames.map((frame) => frame.angles[idleActor]);
            const idleStill = idleActor.length > 0
                && idlePositions.every((point, index) =>
                    index === 0 || (point !== undefined
                        && Math.abs(point.x - idlePositions[0].x) < 0.001
                        && Math.abs(point.y - idlePositions[0].y) < 0.001))
                && new Set(idleAngles).size === 1;
            report.idleHasNoAnimation = idleStill
                && animations.length === 0
                && pulseFrames.length >= 6;
            const hudNow = await hud(page);
            const idleRow = hudNow.rows.find((row) => row.actor === idleActor);
            /*
             * "Rutscht nach unten" heisst: hinter JEDEN Akteur, der noch
             * arbeitet. Nicht "ganz nach unten": unter ihm koennen weitere
             * ruhige stehen, und wer laenger schweigt, steht tiefer.
             */
            const busyRows = hudNow.rows.filter((row) => !row.idle).length;
            report.idleAgentFadesNotDisappears = idleRow !== undefined
                && idleRow.idle
                && idleRow.opacity < 1
                && hudNow.rows.indexOf(idleRow) >= busyRows
                && (pulseFrames[0]?.opacity[idleActor] ?? 1) < 1;
            actA.idle = {
                actor: idleActor,
                sinceMs: actors[idleActor]?.sinceMs ?? -1,
                angles: idleAngles,
                positions: idlePositions,
                animations,
                animationsInLayer: allAnimations,
                busyRows,
                row: idleRow,
                rows: hudNow.rows.map((row) => row.actor),
                method:
                    'Gemessen an den Elementen des RUHIGEN Akteurs: dort laeuft keine Animation und '
                    + 'kein Uebergang, sein Winkel und seine Position aendern sich ueber zehn '
                    + 'Proben nicht, und seine Koerpergroesse bleibt gleich. Die Animationen der '
                    + 'ganzen Ebene stehen daneben; sie gehoeren zu Akteuren, die arbeiten, und der '
                    + 'Puls ist ihre Arbeit.',
            };
            log(`AC6: ${idleActor} steht still ${idleStill}, laufende Animationen `
                + `${animations.length}, blass ${report.idleAgentFadesNotDisappears}`);

            /* ------------------------------------- AC7: Bildrate und Deckel */
            const rates = [];
            for (let i = 0; i < 8; i += 1) {
                await page.waitForTimeout(600);
                const perf = await frameRate(page);
                if (perf?.running === true && perf.fps > 0) {
                    rates.push(Number(perf.fps.toFixed(2)));
                }
            }
            report.framesPerSecondMin = rates.length === 0 ? 0 : Math.min(...rates);
            actA.performance = { rates, agents: 8, actors: (now?.actors ?? []).length };
            log(`AC7: Bildrate min ${report.framesPerSecondMin} bei acht Agenten`);

            await readability(page, 'act-a-panel');
            await context.close();
            await stopBridge(bridge.child);
        }

        /* ============================================================ AKT B */
        /*
         * Der Zeitstrahl, die Kamera und die Welle. Die Welle steht am Ende,
         * weil sie nur zwei Sekunden lebt: sie wird gemessen, sobald das letzte
         * Ereignis der Aufzeichnung eingespielt ist.
         */
        const actB = {};
        extras.acts.b = actB;
        {
            const port = bridgePorts[1];
            const bridge = await startBridge(port, extras.bridgeLog);
            const { context, page } = await newContext();
            await openApp(page, origin, `&agents=${port}`);
            await liveOn(page);
            await setSwitch(page, 'fullscreen', true);
            await page.waitForTimeout(700);

            let remaining = FIXTURE_EVENTS;
            while (remaining > 14) {
                const answer = await advance(port, STEP_SIZE);
                remaining = answer.remaining;
                await page.waitForTimeout(STEP_SETTLE_MS);
            }

            /* -------------------------------------------- AC4: der Zeitstrahl */
            const first = await timelineState(page);
            report.timelineTracksPerActor = first.tracks.length;
            await page.click('[data-testid="atlas-agents-timeline-pause"]');
            await page.waitForTimeout(400);
            const paused = await timelineState(page);
            const eventsBefore = (await agentSeam(page))?.events ?? -1;
            const pausedTo = paused.to;
            for (let i = 0; i < 3; i += 1) {
                const answer = await advance(port, 1);
                remaining = answer.remaining;
                await page.waitForTimeout(SINGLE_SETTLE_MS);
            }
            const duringPause = await timelineState(page);
            const eventsDuring = (await agentSeam(page))?.events ?? -1;
            await page.click('[data-testid="atlas-agents-timeline-pause"]');
            await page.waitForTimeout(600);
            const resumed = await timelineState(page);
            const eventsAfter = (await agentSeam(page))?.events ?? -1;
            /*
             * "Ohne Ereignisse zu verlieren" wird an der ZAHL der behaltenen
             * Ereignisse gemessen und nicht an den Strichen im Fenster: das
             * Fenster von einer Minute wandert weiter, sobald die Pause
             * aufhoert, und dabei faellt hinten heraus, was zu alt geworden
             * ist. Das ist kein Verlust, sondern der Zweck eines Fensters.
             * Nachgesehen wird darum zusaetzlich mit dem Fenster "alles
             * Behaltene": dort muss jedes Ereignis stehen.
             */
            await page.click('[data-testid="atlas-agents-timeline-window"][data-option="0"]');
            await page.waitForTimeout(700);
            const wide = await timelineState(page);
            const keptSeam = await agentSeam(page);
            const kept = (keptSeam?.actors ?? []).reduce((sum, actor) => sum + actor.count, 0);
            await page.click('[data-testid="atlas-agents-timeline-window"][data-option="60000"]');
            await page.waitForTimeout(600);
            report.timelinePauseKeepsEvents = paused.mode === 'paused'
                && duringPause.mode === 'paused'
                && duringPause.to === pausedTo
                && eventsDuring > eventsBefore
                && resumed.mode === 'live'
                && resumed.to > pausedTo
                && eventsAfter === eventsDuring
                && wide.ticks >= kept;
            actB.timeline = {
                first, paused, duringPause, resumed, wide,
                keptEvents: kept,
                events: { before: eventsBefore, during: eventsDuring, after: eventsAfter },
            };

            /* Ein Klick auf den Strahl springt zurueck, und die Ansicht sagt es. */
            const lanes = await page.evaluate(() => {
                const node = document.querySelector('[data-testid="atlas-agents-timeline-lanes"]');
                if (node === null) {
                    return null;
                }
                const rect = node.getBoundingClientRect();
                return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
            });
            if (lanes !== null) {
                await page.mouse.click(lanes.x + lanes.width * 0.35, lanes.y + lanes.height / 2);
                await page.waitForTimeout(700);
            }
            const scrubbed = await timelineState(page);
            const scrubbedPanel = await panelRect(page);
            const scrubbedSeam = await agentSeam(page);
            report.timelineScrubMarkedAsReplay = scrubbed.mode === 'replay'
                && /REPLAY/i.test(scrubbed.modeLabel)
                && /not now/i.test(scrubbed.replayNote)
                && scrubbedPanel.replay === true
                && scrubbed.to < first.to
                && (scrubbedSeam?.events ?? 0) < eventsAfter;
            actB.scrub = {
                state: scrubbed,
                panelReplay: scrubbedPanel.replay,
                eventsThen: scrubbedSeam?.events ?? -1,
                eventsNow: eventsAfter,
            };
            await shoot(page, SHOT_TIMELINE, 'timeline', extras);
            await page.click('[data-testid="atlas-agents-timeline-live"]');
            await page.waitForTimeout(600);
            log(`AC4: ${report.timelineTracksPerActor} Spuren, Pause `
                + `${report.timelinePauseKeepsEvents}, Wiedergabe `
                + `${report.timelineScrubMarkedAsReplay}`);

            /* ---------------------------------- AC3c: die Kamera mit Feder */
            await setSwitch(page, 'follow', true);
            await page.waitForTimeout(1200);
            let flight = null;
            while (remaining > 5 && flight === null) {
                const before = (await agentSeam(page))?.follow_?.nodeId ?? -1;
                const answer = await advance(port, 1);
                remaining = answer.remaining;
                const samples = [];
                for (let i = 0; i < 90; i += 1) {
                    const point = await cameraAt(page);
                    if (point !== null) {
                        samples.push(point);
                    }
                    await page.waitForTimeout(20);
                }
                const seamNow = await agentSeam(page);
                const goal = seamNow?.follow_;
                if (goal !== undefined && goal !== null && goal.nodeId !== before
                    && samples.length > 10) {
                    flight = { goal, samples };
                }
            }
            if (flight !== null) {
                const start = flight.samples[0];
                const goal = { x: flight.goal.position[0], y: flight.goal.position[1],
                    z: flight.goal.position[2] };
                const total = Math.hypot(goal.x - start.x, goal.y - start.y, goal.z - start.z);
                const direction = total === 0
                    ? { x: 0, y: 0, z: 0 }
                    : { x: (goal.x - start.x) / total, y: (goal.y - start.y) / total,
                        z: (goal.z - start.z) / total };
                let overshoot = 0;
                const along = flight.samples.map((point) =>
                    (point.x - start.x) * direction.x
                    + (point.y - start.y) * direction.y
                    + (point.z - start.z) * direction.z);
                for (const value of along) {
                    overshoot = Math.max(overshoot, value - total);
                }
                const steps = [];
                for (let i = 1; i < flight.samples.length; i += 1) {
                    const a = flight.samples[i - 1];
                    const b = flight.samples[i];
                    const dt = Math.max(1, b.at - a.at);
                    steps.push(Math.hypot(b.x - a.x, b.y - a.y, b.z - a.z) / dt);
                }
                let jerk = 0;
                for (let i = 1; i < steps.length; i += 1) {
                    jerk = Math.max(jerk, Math.abs(steps[i] - steps[i - 1]));
                }
                const overshootFraction = total === 0 ? 0 : Number((overshoot / total).toFixed(5));
                const moved = Math.max(...along) - Math.min(...along);
                report.followSpringNoOvershoot = total > 1
                    && moved > total * 0.2
                    && overshootFraction <= 0.02;
                actB.follow = {
                    goal, start, total: Number(total.toFixed(2)),
                    overshoot: Number(overshoot.toFixed(3)),
                    overshootFraction,
                    threshold: 0.02,
                    maxSpeedChangePerMs: Number(jerk.toFixed(5)),
                    samples: flight.samples.length,
                    movedAlongGoal: Number(moved.toFixed(2)),
                    method:
                        'Die Kameralage wird 90 Mal im Abstand von etwa 20 ms gelesen und auf die '
                        + 'Gerade von der Ausgangslage zum Ziel projiziert. Ueberschwingen ist der '
                        + 'groesste Wert JENSEITS des Ziels, als Anteil der Gesamtstrecke. Die '
                        + 'Feder ist kritisch gedaempft (Daempfungsgrad 1), kann also rechnerisch '
                        + 'nicht ueberschwingen; gemessen wird, ob sie es auch nicht tut.',
                };
            }
            const followLine = await page.evaluate(() => {
                const node = document.querySelector('[data-testid="atlas-agents-followline"]');
                return node === null
                    ? { present: false, text: '', actor: '', place: '', lines: '' }
                    : {
                        present: true,
                        text: (node.textContent ?? '').replace(/\s+/g, ' ').trim(),
                        actor: node.getAttribute('data-actor') ?? '',
                        kind: node.getAttribute('data-kind') ?? '',
                        place: node.getAttribute('data-place') ?? '',
                        lines: node.getAttribute('data-lines') ?? '',
                    };
            });
            if (followLine.present) {
                const seamNow = await agentSeam(page);
                const actor = (seamNow?.actors ?? []).find((entry) => entry.id === followLine.actor);
                const words = { read: 'reading', write: 'writing', search: 'searching',
                    test: 'testing', other: 'command' };
                const expectedText = `${actor?.name} ${words[followLine.kind]} ${followLine.place}`
                    + (actor?.lastLines?.length === 2
                        ? `, lines ${actor.lastLines[0]} to ${actor.lastLines[1]}`
                        : '');
                report.followLineShowsMeasuredOnly = actor !== undefined
                    && followLine.text === expectedText
                    && followLine.place === actor.placeName
                    && followLine.lines === (actor.lastLines ?? []).join('-');
                actB.followLine = { seen: followLine, expected: expectedText };
            }
            await shoot(page, SHOT_FOLLOW, 'follow', extras);
            log(`AC3c: Ueberschwingen ${actB.follow?.overshootFraction}, Zeile `
                + `${report.followLineShowsMeasuredOnly}`);

            /* ------------------------------------------- AC3b: EINE Welle */
            while (remaining > 0) {
                const answer = await advance(port, 1);
                remaining = answer.remaining;
                await page.waitForTimeout(300);
            }
            await page.waitForTimeout(180);
            const waveSeam = await agentSeam(page);
            const waveNodes = await page.evaluate(() =>
                [...document.querySelectorAll('[data-testid="atlas-agent-wave"]')].map((node) => ({
                    actor: node.getAttribute('data-actor') ?? '',
                    node: Number(node.getAttribute('data-node') ?? '-1'),
                    events: Number(node.getAttribute('data-events') ?? '-1'),
                })));
            const burstActor = 'implementer';
            const own = (waveSeam?.waves ?? []).filter((wave) => wave.actor === burstActor);
            const ownNodes = waveNodes.filter((wave) => wave.actor === burstActor);
            report.burstMakesOneWave = own.length === 1
                && own[0].events === 5
                && ownNodes.length === 1
                && ownNodes[0].events === 5;
            actB.wave = {
                seam: waveSeam?.waves ?? [],
                drawn: waveNodes,
                events: waveSeam?.events ?? -1,
                rule: 'Fuenf Schreibereignisse auf demselben Knoten, jeweils weniger als 2500 ms '
                    + 'auseinander, sind EIN Bruch. Ein Bruch traegt eine Welle, nicht ein '
                    + 'Ereignis eine.',
            };
            log(`AC3b Welle: ${own.length} Bruch mit ${own[0]?.events} Ereignissen, `
                + `gezeichnet ${ownNodes.length}`);

            await context.close();
            await stopBridge(bridge.child);
        }

        /* ============================================================ AKT C */
        /*
         * Der Vollbildmodus in zwei Fenstergroessen, die Lesbarkeit, die
         * Schalter der teuren Wirkungen und das Sparprofil.
         */
        const actC = {};
        extras.acts.c = actC;
        {
            const port = bridgePorts[2];
            const bridge = await startBridge(port, extras.bridgeLog);
            const { context, page } = await newContext();
            await openApp(page, origin, `&agents=${port}`);
            await liveOn(page);
            await becomeActor(page);
            await setSwitch(page, 'trails', true);
            let remaining = FIXTURE_EVENTS;
            while (remaining > 0) {
                const answer = await advance(port, 6);
                remaining = answer.remaining;
                await page.waitForTimeout(140);
            }
            await page.waitForTimeout(600);

            /* Erst das Panel mit Spuren, dann das Vollbild. */
            await shoot(page, SHOT_TRAILS, 'trails', extras);
            await readability(page, 'act-c-panel-trails');

            await setSwitch(page, 'fullscreen', true);
            await page.waitForTimeout(1200);

            const sizes = [];
            for (const viewport of [MAIN_VIEWPORT, SECOND_VIEWPORT]) {
                await page.setViewportSize(viewport);
                await page.waitForTimeout(1400);
                const before = report.overlapViolations;
                const clippedBefore = report.clippingViolations;
                await readability(page, `fullscreen-${viewport.width}x${viewport.height}`);
                const panel = await panelRect(page);
                const instrument = await hud(page);
                const strip = await timelineState(page);
                const overlap = (a, b) => a !== null && b !== null
                    && a.x < b.x + b.width && b.x < a.x + a.width
                    && a.y < b.y + b.height && b.y < a.y + a.height;
                sizes.push({
                    viewport,
                    fills: panel.fullscreen
                        && panel.width === viewport.width
                        && panel.height === viewport.height
                        && panel.x === 0 && panel.y === 0,
                    panel,
                    hud: instrument.rect,
                    column: instrument.column,
                    timeline: strip.present ? strip.rect : null,
                    hudOverTimeline: strip.present && overlap(instrument.rect, strip.rect),
                    rows: instrument.rows.map((row) => ({
                        actor: row.actor, lines: row.lines, name: row.name,
                        nameClipped: row.nameClipped, placeClipped: row.placeClipped,
                    })),
                    overlapViolations: report.overlapViolations - before,
                    clippingViolations: report.clippingViolations - clippedBefore,
                });
            }
            await page.setViewportSize(MAIN_VIEWPORT);
            await page.waitForTimeout(1200);
            report.cinemaSizes = sizes;
            report.cinemaFillsViewport = sizes.length >= 2
                && sizes.every((entry) => entry.fills && !entry.hudOverTimeline);
            /*
             * Die Namen der Akteure: an der VORGABEBREITE der Spalte gemessen
             * und an keiner gezogenen. "chec..." ist sauber gekuerzt und sagt
             * trotzdem niemandem, wer da arbeitet.
             */
            report.hudNamesReadable = sizes.every((entry) =>
                entry.column
                && entry.rows.length > 0
                && entry.rows.every((row) => row.lines === 2 && !row.nameClipped));

            const instrument = await hud(page);
            report.cinemaHasTicker = instrument.ticker.length >= 3
                && instrument.ticker.every((line) => line.text.length > 0);
            /* Der Ticker nennt nur Gemessenes: jede Zeile steht so in der Naht. */
            const tickerSeam = (await agentSeam(page))?.ticker ?? [];
            const tickerHonest = instrument.ticker.length === tickerSeam.length
                && instrument.ticker.every((line, index) => line.text === tickerSeam[index]?.text);
            actC.ticker = { shown: instrument.ticker, seam: tickerSeam, honest: tickerHonest };
            report.cinemaHasTicker = report.cinemaHasTicker && tickerHonest;

            await shoot(page, SHOT_CINEMA, 'cinema', extras);

            /* Escape fuehrt zurueck, und die Kameralage bleibt. */
            await keepAwake(page);
            await page.waitForTimeout(900);
            const cameraBefore = await cameraAt(page);
            await page.keyboard.press('Escape');
            await page.waitForTimeout(1600);
            const afterPanel = await panelRect(page);
            const cameraAfter = await cameraAt(page);
            const drift = cameraBefore === null || cameraAfter === null
                ? -1
                : Math.hypot(
                    cameraAfter.x - cameraBefore.x,
                    cameraAfter.y - cameraBefore.y,
                    cameraAfter.z - cameraBefore.z,
                );
            const reach = cameraBefore === null
                ? 0
                : Math.hypot(cameraBefore.x, cameraBefore.y, cameraBefore.z);
            const driftFraction = reach === 0 ? 1 : Number((drift / reach).toFixed(5));
            report.cinemaEscapeKeepsCamera = afterPanel.fullscreen === false
                && drift >= 0 && driftFraction < 0.01;
            actC.escape = {
                before: cameraBefore, after: cameraAfter,
                drift: Number(drift.toFixed(4)), reach: Number(reach.toFixed(2)), driftFraction,
                threshold: 0.01,
                method:
                    'Die Kameralage wird vor und nach Escape gelesen und der Abstand an ihrer '
                    + 'Entfernung vom Ursprung gemessen. Der Vollbildmodus aendert die Groesse der '
                    + 'Zeichenflaeche, und auf eine neue Groesse passt dieses Panel sonst das Bild '
                    + 'ein; genau diese eine Einpassung ist unterdrueckt, weil AC5 sie verbietet.',
            };
            log(`AC5: fuellt ${report.cinemaFillsViewport}, Ticker ${report.cinemaHasTicker}, `
                + `Escape ${report.cinemaEscapeKeepsCamera} (Drift ${drift.toFixed(3)}), `
                + `Namen lesbar ${report.hudNamesReadable}`);

            /* ------------------------------------- AC7b: die vier Schalter */
            await page.click('[data-menu="a-settings"]');
            await page.waitForSelector('[data-testid="atlas-settings"]', { timeout: 15000 });
            await page.waitForTimeout(3600);
            const effects = await page.evaluate(() => {
                const wanted = ['agentTails', 'agentTrails', 'agentWaves', 'agentTimeline'];
                return wanted.map((name) => {
                    const node = document.querySelector(
                        `[data-testid="atlas-settings-effect"][data-setting="${name}"]`,
                    );
                    if (node === null) {
                        return { name, present: false };
                    }
                    const section = node.closest('[data-testid="atlas-settings-section"]');
                    const measure = document.querySelector(
                        `[data-testid="atlas-settings-measure"][data-setting="${name}"]`,
                    );
                    return {
                        name,
                        present: true,
                        section: section?.getAttribute('data-section') ?? '',
                        value: node.getAttribute('data-value') ?? '',
                        detail: (node.querySelector('.atlas-settings-choice-detail')?.textContent ?? '')
                            .replace(/\s+/g, ' ').trim(),
                        hasMeasure: measure !== null,
                    };
                });
            });
            /* Einen davon wirklich umlegen und die Messung daneben lesen. */
            await page.click(
                '[data-testid="atlas-settings-effect"][data-setting="agentTrails"] '
                + '[data-testid="atlas-settings-option"][data-option="false"]',
            );
            await page.waitForTimeout(4200);
            const measured = await page.evaluate(() => {
                const node = document.querySelector(
                    '[data-testid="atlas-settings-measure"][data-setting="agentTrails"]',
                );
                return node === null
                    ? null
                    : {
                        verdict: node.getAttribute('data-verdict') ?? '',
                        text: (node.textContent ?? '').replace(/\s+/g, ' ').trim(),
                    };
            });
            report.effectsToggleable = effects.every((entry) =>
                entry.present && entry.section === 'display' && entry.hasMeasure)
                && measured !== null
                && measured.verdict !== 'not-measured'
                && measured.verdict !== 'measuring';
            actC.effects = { entries: effects, measured };

            /* Das Sparprofil, und der Vollbildmodus danach. */
            await page.click('[data-testid="atlas-settings-profile"][data-profile="thrifty"]');
            await page.waitForTimeout(1200);
            await page.click('[data-testid="atlas-settings-close"]');
            await page.waitForTimeout(800);
            await setSwitch(page, 'fullscreen', true);
            await page.waitForTimeout(1600);
            const thriftySeam = await agentSeam(page);
            const thriftyPanel = await panelRect(page);
            const thriftyRates = [];
            for (let i = 0; i < 5; i += 1) {
                await page.waitForTimeout(600);
                const perf = await frameRate(page);
                if (perf?.running === true && perf.fps > 0) {
                    thriftyRates.push(Number(perf.fps.toFixed(2)));
                }
            }
            const beforeThrifty = report.overlapViolations;
            const clippedThrifty = report.clippingViolations;
            await readability(page, 'fullscreen-thrifty');
            const thriftyHud = await hud(page);
            report.thriftProfileKeepsCinemaUsable = thriftySeam !== null
                && thriftySeam.effects.tails === false
                && thriftySeam.effects.trails === false
                && thriftySeam.effects.waves === false
                && thriftySeam.effects.timeline === false
                && thriftyPanel.fullscreen === true
                && thriftyPanel.width === MAIN_VIEWPORT.width
                && thriftyRates.length > 0
                && Math.min(...thriftyRates) > 0
                && thriftyHud.present
                && /agent layer is off in the settings/i.test(thriftyHud.text)
                && report.overlapViolations === beforeThrifty
                && report.clippingViolations === clippedThrifty;
            actC.thrifty = {
                effects: thriftySeam?.effects,
                layerOn: thriftySeam?.layerOn,
                panel: thriftyPanel,
                rates: thriftyRates,
                hudSaysWhy: /agent layer is off in the settings/i.test(thriftyHud.text),
            };
            log(`AC7b: Schalter ${report.effectsToggleable}, Sparprofil `
                + `${report.thriftProfileKeepsCinemaUsable} (${thriftyRates.join('/')} fps)`);

            /* Zurueck auf die Vorgabe, damit der naechste Akt frisch anfaengt. */
            await setSwitch(page, 'fullscreen', false);
            await page.click('[data-menu="a-settings"]');
            await page.waitForSelector('[data-testid="atlas-settings"]', { timeout: 15000 });
            await page.click('[data-testid="atlas-settings-profile"][data-profile="default"]');
            await page.waitForTimeout(900);
            await page.click('[data-testid="atlas-settings-close"]');
            await page.waitForTimeout(500);

            await context.close();
            await stopBridge(bridge.child);
        }

        /* ============================================================ AKT D */
        /*
         * Die Bildserie und die Aufnahme. Eigener Kontext, weil Playwright das
         * Video beim Anlegen des Kontexts einschaltet und beim Schliessen
         * fertigschreibt.
         */
        const actD = {};
        extras.acts.d = actD;
        {
            const port = bridgePorts[3];
            const bridge = await startBridge(port, extras.bridgeLog);
            videoDir = await mkdtemp(join(tmpdir(), 'codeatlasweb-w11b-video-'));
            const { context, page } = await newContext({
                recordVideo: { dir: videoDir, size: { ...MAIN_VIEWPORT } },
            });
            await openApp(page, origin, `&agents=${port}`);
            await liveOn(page);
            await setSwitch(page, 'trails', true);
            await setSwitch(page, 'follow', false);
            await setSwitch(page, 'fullscreen', true);
            await page.waitForTimeout(1600);
            const stage = await sceneRect(page);
            const side = Math.round(Math.min(stage.width, stage.height) * 0.92);
            const clip = {
                x: Math.round(stage.x + (stage.width - side) / 2),
                y: Math.round(stage.y + (stage.height - side) / 2),
                width: side,
                height: side,
            };

            /*
             * Ein Bild je Wiedergabeschritt: gleiche Abstaende der gesteuerten
             * Zeit, und nicht gleiche Abstaende der Wanduhr. Genau das ist die
             * Zeit, die dieser Lauf steuert.
             */
            /*
             * Die Schritte summieren sich genau auf die Datei: jeder Schritt
             * spielt mindestens vier Ereignisse ein, und keiner spielt null.
             * Ein Schritt ohne Ereignis waere ein Bild, das dem vorigen gleicht,
             * und genau das ist der Befund, den diese Serie ausschliessen soll.
             */
            const base = Math.floor(FIXTURE_EVENTS / SERIES_FRAMES);
            const extra = FIXTURE_EVENTS - base * SERIES_FRAMES;
            const series = [];
            for (let i = 0; i < SERIES_FRAMES; i += 1) {
                const answer = await advance(port, base + (i < extra ? 1 : 0));
                await page.waitForTimeout(360);
                const file = join(FRAME_DIR, `step-${String(i + 1).padStart(2, '0')}.png`);
                await page.screenshot({ path: file, clip });
                await grab(page, `series-${i}`, clip);
                const live = await livePulse(page);
                series.push({
                    index: i + 1,
                    file: `frames/step-${String(i + 1).padStart(2, '0')}.png`,
                    emitted: answer.total,
                    remaining: answer.remaining,
                    bodies: Object.keys(live.positions).length,
                    positions: Object.fromEntries(Object.entries(live.positions).map(
                        ([id, point]) => [id, [Number(point.x.toFixed(1)), Number(point.y.toFixed(1))]],
                    )),
                });
            }
            report.frameSeriesCount = series.length;
            const diffs = [];
            for (let i = 1; i < series.length; i += 1) {
                const diff = await compareImages(page, {
                    base: `series-${i - 1}`, variant: `series-${i}`, epsilon: DIFF_EPSILON,
                });
                diffs.push({ from: i, to: i + 1, ...(diff ?? {}) });
            }
            report.framesDifferBetweenSteps = diffs.length >= SERIES_FRAMES - 1
                && diffs.every((diff) => (diff.changedFraction ?? 0) > 0.0002);
            actD.series = series;
            actD.diffs = diffs;
            actD.clip = clip;
            log(`AC7c: ${series.length} Bilder, kleinste Aenderung `
                + `${Math.min(...diffs.map((diff) => diff.changedFraction ?? 0))}`);

            const video = page.video();
            await context.close();
            if (video !== null) {
                await rm(VIDEO_FILE, { force: true });
                await video.saveAs(VIDEO_FILE);
                await video.delete().catch(() => undefined);
            }
            report.videoWritten = existsSync(VIDEO_FILE) && (await stat(VIDEO_FILE)).size > 20 * 1024;
            actD.videoBytes = existsSync(VIDEO_FILE) ? (await stat(VIDEO_FILE)).size : 0;
            await stopBridge(bridge.child);

            /* Der Kontaktabzug: sechzehn Bilder in einem. */
            const frameFiles = (await readdir(FRAME_DIR)).filter((name) => name.endsWith('.png')).sort();
            if (existsSync(FFMPEG) && frameFiles.length >= SERIES_FRAMES) {
                const sheet = await run(FFMPEG, [
                    '-y', '-hide_banner', '-loglevel', 'error',
                    '-framerate', '1',
                    '-i', join(FRAME_DIR, 'step-%02d.png'),
                    '-frames:v', '1',
                    '-filter_complex', 'tile=4x4:padding=6:margin=6:color=0x0a0e0d,scale=1800:-1',
                    CONTACT_SHEET,
                ]);
                actD.contactSheet = { code: sheet.code, out: sheet.out.trim().slice(0, 400) };
            } else {
                actD.contactSheet = { code: -1, out: `ffmpeg fehlt unter ${FFMPEG}` };
            }
            report.contactSheetWritten = existsSync(CONTACT_SHEET)
                && (await stat(CONTACT_SHEET)).size > 30 * 1024;
            log(`AC7c Kontaktabzug: ${report.contactSheetWritten}, Video ${report.videoWritten}`);
        }
    } catch (err) {
        failure = err;
        console.error('[smoke-w11b] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w11b] Server-Log:\n' + serverLog.slice(-15).join('\n'));
        }
        if (extras.bridgeLog.length > 0) {
            console.error('[smoke-w11b] Bruecken-Log:\n' + extras.bridgeLog.slice(-15).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (proxy !== null) {
        await proxy.close();
    }
    for (const child of bridges) {
        await stopBridge(child);
    }
    await stopServer(serverChild);

    /*
     * Mehrfach nachsehen statt einmal.
     *
     * Ein Prozess, der eben ein SIGTERM bekommen hat, gibt seinen Port nicht in
     * derselben Millisekunde frei; smoke-w6-full misst dafuer 1557 ms am
     * Modellport. Eine einzige Zaehlung waere darum eine Aussage ueber die
     * Reaktionszeit dieser Maschine und nicht ueber Prozessreste.
     */
    const ports = [serverPort, uiPort, ...bridgePorts].filter((value) => value > 0);
    const looks = [];
    let leftovers = [];
    for (let attempt = 0; attempt < 10; attempt += 1) {
        leftovers = [];
        for (const port of ports) {
            leftovers.push({ port, listeners: await countListeners(port) });
        }
        const total = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
        looks.push({ attempt: attempt + 1, atMs: Date.now() - totalStarted, total });
        if (total === 0) {
            break;
        }
        await sleep(400);
    }
    extras.leftovers = leftovers;
    extras.leftoverLooks = looks;
    report.leftoverProcesses = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
    log('leftoverProcesses:', report.leftoverProcesses, JSON.stringify(looks));

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            events: 'fixtures/agent-events/w11b-replay.jsonl (nur gelesen)',
            method:
                'Vier Akte, vier Bruecken, vier frische Wiedergaben: die Wiedergabe schiebt die '
                + 'Zeitstempel EINMAL auf die Gegenwart, und ein Lauf, der danach zwei Minuten '
                + 'misst, misst an gealterten Zeiten. Die Bahn wird nicht abgefragt, sondern vom '
                + 'Koerper selbst aufgezeichnet (agentMotion) und hier aus ihren Punkten '
                + 'nachgerechnet. Die Bahnradien rechnet dieser Lauf unabhaengig aus der Regel '
                + 'nach. Der Puls wird an zwei Instrumenten gemessen: die Koerpergroesse aus dem '
                + 'DOM, das Knotenlicht aus den Pixeln. Die Bildserie hat gleiche Abstaende der '
                + 'GESTEUERTEN Zeit, ein Bild je Wiedergabeschritt.',
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = [SHOT_CINEMA, SHOT_TRAILS, SHOT_FOLLOW, SHOT_TIMELINE]
        .every((file) => existsSync(file));
    const ok =
        failure === null
        && report.transitionIsCurved === true
        && report.transitionSamples.length >= 3
        && report.transitionMs >= 250 && report.transitionMs <= 900
        && report.cometTailPresent === true
        && report.neverInTwoPlaces === true
        && report.trailNodes >= 6 && report.trailNodes <= 10
        && report.trailBelowEdges === true
        && report.trailDashed === true
        && report.trailInLegend === true
        && report.trailWindowSwitchable === true
        && report.trailsToggleOff === true
        && report.sameNodeDifferentRadii === true
        && report.radiiDeterministic === true
        && report.burstMakesOneWave === true
        && report.followSpringNoOvershoot === true
        && report.followLineShowsMeasuredOnly === true
        && report.pulseFollowsActivity === true
        && report.pulseFrames >= 6
        && report.pulseStopsWhenIdle === true
        && report.pulseDistinctFromNodeGlow === true
        && report.timelineTracksPerActor >= 2
        && report.timelinePauseKeepsEvents === true
        && report.timelineScrubMarkedAsReplay === true
        && report.cinemaFillsViewport === true
        && report.cinemaHasTicker === true
        && report.cinemaEscapeKeepsCamera === true
        && report.cinemaSizes.length >= 2
        && report.idleHasNoAnimation === true
        && report.idleAgentFadesNotDisappears === true
        && report.framesPerSecondMin > 0
        && report.drawnBodiesCap > 0
        && report.capReported === true
        && report.effectsToggleable === true
        && report.thriftProfileKeepsCinemaUsable === true
        && report.frameSeriesCount >= 12
        && report.contactSheetWritten === true
        && report.videoWritten === true
        && report.framesDifferBetweenSteps === true
        && report.hudNamesReadable === true
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.cutWithoutHint === 0
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && shotsOk
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w11b] W11b-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w11b] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir, videoDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W11b-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w11b] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
