#!/usr/bin/env node
/*
 * W11a-Smoke: Agenten sind sichtbare Wesen im Code, und das Instrument sagt,
 * was man sieht.
 *
 * Nutzerwunsch vom 2026-08-29, woertlich: "wie wuerdest du in dem Projekt
 * einbauen, dass wir am Graphen unten rechts Agents sehen, wenn sie live
 * arbeiten? Welche Farben haben sie, wie wissen wir welche Art von Arbeit ein
 * Agent macht?"
 *
 * ## Die vier Kunstgriffe dieses Laufs
 *
 * 1. **Eine echte Bruecke, aber mit angehaltener Zeit.** Der Beweislauf startet
 *    tools/agent-bridge.mjs im Wiedergabemodus gegen
 *    fixtures/agent-events/w11a-replay.jsonl. Die Bruecke liest die Datei
 *    einmal und schweigt danach, bis jemand `POST /replay/advance` ruft. Damit
 *    faehrt derselbe Ablauf in jedem Lauf durch dieselben Zustaende, statt auf
 *    eine Wanduhr zu warten: die Aufzeichnung dauert mit ihren echten
 *    Zeitabstaenden acht Minuten, dieser Lauf braucht dafuer Sekunden, und
 *    zwischen zwei Schritten steht das Bild still genug, um es zu vermessen.
 *
 * 2. **Drei Zustaende der Quelle, alle drei gemessen.** Aus (kein einziger
 *    fetch, gezaehlt an `page.on('request')` UND an der Naht der Anwendung),
 *    an ohne Bruecke (ein Port, auf dem mit Absicht niemand lauscht, damit die
 *    ehrliche Meldung samt Startbefehl entsteht), an mit Bruecke.
 *
 * 3. **Die Zuordnung wird gegen die Layout-Antwort geprueft, nicht gegen die
 *    Ansicht.** Welcher Knoten zu `src/services/userService.ts` Zeile 24 bis 30
 *    gehoert, rechnet dieser Lauf selbst aus `/api/layout` aus: alle Knoten der
 *    Datei, deren Bereich sich ueberschneidet, der engste davon. Die Ansicht
 *    gegen ihre eigene Zahl zu pruefen waere keine Pruefung.
 *
 * 4. **Der Orbit wird als Bildserie belegt.** Acht Einzelbilder ueber eine
 *    Umkreisung, der Winkel je Bild aus derselben Tabelle gelesen, die den
 *    Koerper bewegt, und ein Kontaktabzug daraus (ffmpeg). Eine Bildserie, auf
 *    der sich nichts aendert, ist damit ein Befund und kein Erfolg.
 *
 * ## Was hier eine Messung ist und was nicht
 *
 * `nodeColorsUnchanged` ist die heikelste Zahl dieses Laufs, und sie steht
 * darum aus DREI Teilen zusammen, die alle im Artefakt einzeln nachzulesen
 * sind: die Farbwerte der Legende sind mit und ohne Ebene dieselben (exakt,
 * Zeichenkettenvergleich), die Menge des warmen Knotenlichts im Bild ist
 * dieselbe (Pixelhistogramm der Knotenfamilie, Abweichung unter einem halben
 * Prozent), und die Ebene beruehrt ueberhaupt nur einen kleinen Teil der
 * Flaeche (geaenderte Pixel unter vier Prozent). Ein einzelnes `true` ohne
 * diese drei Zahlen waere eine Behauptung.
 *
 * ## Ports
 *
 * Ab 4620. 4141 gehoert dem Modell-Sidecar des Nutzers, 4142 ist fuer SEINE
 * Bruecke vorgesehen, 4390 und 4391 seiner Vorschau; alle vier fasst dieser
 * Lauf nicht an, weder startend noch beendend. 4210 bis 4600 sind an die Laeufe
 * davor vergeben.
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w11a).
 */

import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
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
const EVENTS = join(ROOT, 'fixtures', 'agent-events', 'w11a-replay.jsonl');
const PROJECT = 'codeatlasweb-w11a';
const OUT_DIR = join(ROOT, 'verification', 'w11');
const FRAME_DIR = join(OUT_DIR, 'frames');
const OUT_JSON = join(OUT_DIR, 'agents.json');
const SHOT_MAIN = join(OUT_DIR, 'live-agents.png');
const SHOT_NOBRIDGE = join(OUT_DIR, 'live-agents-nobridge.png');
const SHOT_COLLAPSED = join(OUT_DIR, 'live-agents-collapsed.png');
const CONTACT_SHEET = join(OUT_DIR, 'orbit-contact-sheet.png');
/*
 * Das vierte Bild, und es steht bewusst nicht in der Liste des eingefrorenen
 * Tests: der prueft drei Namen, und ein vierter dort waere eine Aenderung an
 * einer eingefrorenen Datei. Es gibt es trotzdem, weil der Graph in seinem
 * Panel 441 Pixel breit ist und die Koerper darin Punkte sind; im Vollbild
 * sieht man, was die Ebene wirklich zeichnet.
 */
const SHOT_CINEMA = join(OUT_DIR, 'live-agents-cinema.png');

const FFMPEG = '/opt/homebrew/bin/ffmpeg';

/** 4141, 4142, 4390 und 4391 gehoeren dem Nutzer, alles bis 4600 den Laeufen davor. */
const MIN_PORT = 4620;

/** Wie lange bei ausgeschaltetem Live-Modus gewartet wird, bevor die Null zaehlt. */
const OFF_WINDOW_MS = 6000;

/** Wie viele Ereignisse ein Schritt der Wiedergabe einspielt. */
const STEP_SIZE = 3;

/** Wie lange nach einem Schritt gewartet wird, bevor gemessen wird. */
const STEP_SETTLE_MS = 260;

/** Wie viele Einzelbilder eine Umkreisung belegen. */
const ORBIT_FRAMES = 8;

/** Das Symbol, das der Leser oeffnet, damit "you" ein Akteur wird. */
const YOU_FILE = 'src/services/userService.ts';

/** Das Ereignis, an dem die genaue Zuordnung gemessen wird. */
const EXACT_PATH = 'src/services/userService.ts';
const EXACT_LINES = [24, 30];

/**
 * Die Datei, deren Ereignis OHNE Zeilen den Modulknoten treffen muss.
 *
 * Sie steht am Ende der Aufzeichnung und nicht am Anfang, und der Grund ist
 * eine Eigenschaft der Wiedergabe: die Zeitstempel behalten ihre echten
 * Abstaende, also liegt der Anfang der Aufzeichnung acht Minuten zurueck, und
 * ein Akteur, dessen letztes Ereignis so alt ist, steht richtigerweise nicht
 * mehr im Bild. Gemessen wird darum an einem Ereignis, das ein Leser wirklich
 * saehe.
 */
const FILE_ONLY_PATH = 'src/services/orderService.ts';

/** Der Pfad, den der Index nicht kennt. */
const UNMAPPABLE_PATH = 'package.json';

/** Der Pfad, dessen Knoten keine Endzeile traegt. */
const UNCERTAIN_PATH = 'src';

const MAIN_VIEWPORT = { width: 1680, height: 1050 };

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

const log = (...parts) => console.log('[smoke-w11a]', ...parts);
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

const sha256 = (path) => createHash('sha256').update(readFileSync(path)).digest('hex');

// ----------------------------------------------------------------- Bruecke ---

/** Die Bruecke im Wiedergabemodus starten und warten, bis sie antwortet. */
async function startBridge(port, sink) {
    const child = spawn(process.execPath, [
        join(ROOT, 'tools', 'agent-bridge.mjs'),
        '--replay', EVENTS,
        '--port', String(port),
    ], { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout.on('data', (d) => sink.push(`[stdout] ${d.toString().trimEnd()}`));
    child.stderr.on('data', (d) => sink.push(`[stderr] ${d.toString().trimEnd()}`));
    let exited = null;
    child.on('exit', (code, signal) => { exited = { code, signal }; });

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

/** Nur die Winkel, LEBEND aus dem zuletzt gezeichneten Bild. */
const liveAngles = (page) =>
    page.evaluate(() => ({ ...(globalThis.__atlasAgents?.angles ?? {}) }));

/** Was auf dem Graphen wirklich als Koerper steht. */
const bodies = (page) =>
    page.evaluate(() =>
        [...document.querySelectorAll('[data-testid="atlas-agent-body"]')].map((node) => {
            const core = node.querySelector('[data-testid="atlas-agent-core"]');
            const letter = node.querySelector('[data-testid="atlas-agent-letter"]');
            const coreBox = core?.getBoundingClientRect();
            const box = node.getBoundingClientRect();
            return {
                actor: node.getAttribute('data-actor') ?? '',
                kind: node.getAttribute('data-kind') ?? '',
                letter: (letter?.textContent ?? '').trim(),
                you: node.getAttribute('data-you') === 'true',
                color: node.getAttribute('data-color') ?? '',
                coreWidth: coreBox === undefined ? 0 : Math.round(coreBox.width),
                coreHeight: coreBox === undefined ? 0 : Math.round(coreBox.height),
                x: Math.round(box.x + box.width / 2),
                y: Math.round(box.y + box.height / 2),
            };
        }));

/** Die Ghost-Pings der Suche. */
const ghosts = (page) =>
    page.evaluate(() =>
        [...document.querySelectorAll('[data-testid="atlas-agent-ghost"]')].map((node) => ({
            actor: node.getAttribute('data-actor') ?? '',
            node: Number(node.getAttribute('data-node') ?? '-1'),
        })));

/** Was das Instrument wirklich sagt. */
const hud = (page) =>
    page.evaluate(() => {
        const tidy = (value) => (value ?? '').replace(/\s+/g, ' ').trim();
        const root = document.querySelector('[data-testid="atlas-agents"]');
        if (root === null) {
            return { present: false };
        }
        const rect = root.getBoundingClientRect();
        const one = (id) => {
            const node = root.querySelector(`[data-testid="${id}"]`);
            return node === null
                ? { present: false, text: '' }
                : { present: true, text: tidy(node.textContent) };
        };
        return {
            present: true,
            size: root.getAttribute('data-size') ?? '',
            source: root.getAttribute('data-source') ?? '',
            filter: root.getAttribute('data-filter') ?? '',
            layer: root.getAttribute('data-layer') ?? '',
            rect: {
                x: Math.round(rect.x),
                y: Math.round(rect.y),
                width: Math.round(rect.width),
                height: Math.round(rect.height),
            },
            title: tidy(root.querySelector('.atlas-agents-title')?.textContent),
            count: {
                ...one('atlas-agents-count'),
                value: Number(root.querySelector('[data-testid="atlas-agents-count"]')
                    ?.getAttribute('data-count') ?? '-1'),
            },
            rate: {
                ...one('atlas-agents-rate'),
                perMinute: Number(root.querySelector('[data-testid="atlas-agents-rate"]')
                    ?.getAttribute('data-per-minute') ?? '-1'),
            },
            order: {
                ...one('atlas-agents-order'),
                missed: Number(root.querySelector('[data-testid="atlas-agents-order"]')
                    ?.getAttribute('data-missed') ?? '-1'),
            },
            line: one('atlas-agents-line'),
            reading: one('atlas-agents-reading'),
            sourceBox: (() => {
                const node = root.querySelector('[data-testid="atlas-agents-source"]');
                return node === null
                    ? { present: false, state: '', text: '' }
                    : {
                        present: true,
                        state: node.getAttribute('data-state') ?? '',
                        mode: node.getAttribute('data-mode') ?? '',
                        text: tidy(node.textContent),
                    };
            })(),
            command: (() => {
                const node = root.querySelector('[data-testid="atlas-agents-command"]');
                return node === null
                    ? { present: false, command: '' }
                    : { present: true, command: node.getAttribute('data-command') ?? '' };
            })(),
            rows: [...root.querySelectorAll('[data-testid="atlas-agents-row"]')].map((row) => ({
                actor: row.getAttribute('data-actor') ?? '',
                kind: row.getAttribute('data-kind') ?? '',
                letter: row.getAttribute('data-letter') ?? '',
                you: row.getAttribute('data-you') === 'true',
                placement: row.getAttribute('data-placement') ?? '',
                uncertain: row.getAttribute('data-uncertain') === 'true',
                name: tidy(row.querySelector('[data-testid="atlas-agents-name"]')?.textContent),
                kindLetter: tidy(row.querySelector('[data-testid="atlas-agents-kind"]')?.textContent),
                place: tidy(row.querySelector('[data-testid="atlas-agents-place"]')?.textContent),
                since: tidy(row.querySelector('[data-testid="atlas-agents-since"]')?.textContent),
                dotColor: row.querySelector('[data-testid="atlas-agents-dot"]')
                    ?.getAttribute('data-color') ?? '',
                strip: row.querySelector('[data-testid="atlas-agents-strip"]')
                    ?.getAttribute('data-bars') ?? '',
                stripTotal: Number(row.querySelector('[data-testid="atlas-agents-strip"]')
                    ?.getAttribute('data-total') ?? '-1'),
                bars: [...row.querySelectorAll('.atlas-agents-bar')]
                    .map((bar) => Number(bar.getAttribute('data-count') ?? '-1')),
                intent: (() => {
                    const node = row.querySelector('[data-testid="atlas-agents-intent"]');
                    return node === null
                        ? { present: false, text: '', selfReported: '' }
                        : {
                            present: true,
                            text: tidy(node.textContent),
                            selfReported: node.getAttribute('data-self-reported') ?? '',
                            color: globalThis.getComputedStyle(node).color,
                        };
                })(),
                tested: tidy(row.querySelector('[data-testid="atlas-agents-tested"]')?.textContent),
                path: tidy(row.querySelector('[data-testid="atlas-agents-path"]')?.textContent),
                card: tidy(row.querySelector('[data-testid="atlas-agents-card"]')?.textContent),
                hasCard: row.querySelector('[data-testid="atlas-agents-card"]') !== null,
            })),
            unmapped: [...root.querySelectorAll('[data-testid="atlas-agents-unmapped-row"]')]
                .map((node) => ({
                    tool: node.getAttribute('data-tool') ?? '',
                    path: node.getAttribute('data-path') ?? '',
                    why: node.getAttribute('data-why') ?? '',
                    text: tidy(node.textContent),
                })),
            filters: [...root.querySelectorAll('[data-testid="atlas-agents-filter-option"]')]
                .map((node) => ({
                    option: node.getAttribute('data-option') ?? '',
                    active: node.getAttribute('data-active') === 'true',
                })),
            switches: [...root.querySelectorAll('[data-testid="atlas-agents-switch"]')]
                .map((node) => ({
                    name: node.getAttribute('data-switch') ?? '',
                    active: node.getAttribute('data-active') === 'true',
                    label: tidy(node.textContent),
                })),
            /*
             * Die Zaehlung der Fortschrittselemente, wortgleich mit smoke-w10:
             * AC7 verbietet einen Balken ueber eine Arbeit, die dieses Fenster
             * nicht sieht.
             */
            progressElements: [...root.querySelectorAll(
                'progress, [role="progressbar"], [aria-valuenow], [class*="progress" i], '
                + '[class*="fortschritt" i], [data-testid*="progress" i]',
            )].map((node) => ({
                tag: node.tagName.toLowerCase(),
                testid: node.getAttribute('data-testid') ?? '',
                className: node.getAttribute('class') ?? '',
            })),
            text: tidy(root.textContent),
        };
    });

/** Die Farbwerte der Legende, exakt wie sie dastehen. */
const legendColors = (page) =>
    page.evaluate(() =>
        [...document.querySelectorAll('[data-testid="atlas-galaxy-legend-swatch"]')]
            .map((node) => `${node.getAttribute('data-type') ?? ''}=${node.getAttribute('data-color') ?? ''}`)
            .sort());

/** Das Rechteck der Zeichenflaeche, in Fensterkoordinaten. Wortgleich mit smoke-w9. */
const sceneRect = (page) =>
    page.evaluate(() => {
        const node = document.querySelector('[data-testid="atlas-galaxy-scene"]');
        if (node === null) {
            return null;
        }
        const rect = node.getBoundingClientRect();
        return {
            x: Math.round(rect.x),
            y: Math.round(rect.y),
            width: Math.round(rect.width),
            height: Math.round(rect.height),
        };
    });

/** Ein Bild aufnehmen und IN der Seite ablegen. Wortgleich mit smoke-w9/w10. */
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
        globalThis.__w11a = globalThis.__w11a ?? {};
        globalThis.__w11a[name] = ctx.getImageData(0, 0, canvas.width, canvas.height);
        return { width: canvas.width, height: canvas.height };
    }, { name: key, data: shot.toString('base64') });
}

/**
 * Zwei abgelegte Bilder vergleichen: wie viel hat sich geaendert, und was ist
 * mit dem warmen Knotenlicht passiert?
 *
 * Die Knotenfarben dieser Fixture sind Sternfarben und liegen alle im warmen
 * Viertel (gemessen: #ff6050 bis #fff0c0). "Warm" heisst hier darum: rot ist
 * der groesste Kanal, blau der kleinste, und der Punkt leuchtet ueberhaupt.
 *
 * Zwei Zahlen daraus, und der Unterschied zwischen ihnen ist der ganze Punkt:
 *
 *  - `warmAdded`: Pixel, die OHNE Ebene nicht warm waren und MIT ihr warm sind.
 *    Das waere eine Umfaerbung, also genau das, was diese Ebene nicht tun darf.
 *    Erwartet wird null, denn das Farbtonband der Agenten (140 bis 340 Grad)
 *    enthaelt kein Warm.
 *  - `warmCovered`: Pixel, die warm waren und es nicht mehr sind. Das ist
 *    Verdeckung und keine Umfaerbung: ein Koerper, der vor einem Knoten steht,
 *    nimmt ihm ein paar Pixel, so wie jedes Element vor jedem anderen. Die Zahl
 *    steht im Artefakt, damit sichtbar ist, wie viel das ist.
 */
const compareImages = (page, options) =>
    page.evaluate((input) => {
        const store = globalThis.__w11a ?? {};
        const first = store[input.base];
        const second = store[input.variant];
        if (first === undefined || second === undefined) {
            return null;
        }
        if (first.width !== second.width || first.height !== second.height) {
            return null;
        }
        const warm = (data, i) => {
            const r = data[i];
            const g = data[i + 1];
            const b = data[i + 2];
            return r > 80 && r >= g && g >= b && r - b > 25;
        };
        let changed = 0;
        let warmBase = 0;
        let warmVariant = 0;
        let warmChanged = 0;
        let warmAdded = 0;
        let warmCovered = 0;
        let skipped = 0;
        const hole = input.ignore ?? null;
        let total = 0;
        for (let i = 0; i < first.data.length; i += 4) {
            const pixel = i / 4;
            const x = pixel % first.width;
            const y = Math.floor(pixel / first.width);
            if (hole !== null && x >= hole.x && x < hole.x + hole.width
                && y >= hole.y && y < hole.y + hole.height) {
                skipped += 1;
                continue;
            }
            total += 1;
            const wasWarm = warm(first.data, i);
            const isWarm = warm(second.data, i);
            if (wasWarm) {
                warmBase += 1;
            }
            if (isWarm) {
                warmVariant += 1;
            }
            if (isWarm && !wasWarm) {
                warmAdded += 1;
            }
            if (wasWarm && !isWarm) {
                warmCovered += 1;
            }
            const delta = Math.abs(first.data[i] - second.data[i])
                + Math.abs(first.data[i + 1] - second.data[i + 1])
                + Math.abs(first.data[i + 2] - second.data[i + 2]);
            if (delta > input.epsilon) {
                changed += 1;
                if (wasWarm) {
                    warmChanged += 1;
                }
            }
        }
        return {
            width: first.width,
            height: first.height,
            total,
            skipped,
            changed,
            changedFraction: Number((changed / total).toFixed(5)),
            warmBase,
            warmVariant,
            warmChanged,
            warmAdded,
            warmCovered,
            warmAddedFraction: total === 0 ? 0 : Number((warmAdded / total).toFixed(6)),
            warmDrift: warmBase === 0
                ? 0
                : Number((Math.abs(warmVariant - warmBase) / warmBase).toFixed(5)),
        };
    }, options);

/** Wo jeder scrollbare Bereich steht. Wortgleich mit smoke-w9/w10. */
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
async function toggleLiveByMenu(page) {
    await page.click('[data-menu="a-agents"]');
    await page.waitForTimeout(250);
}

/** Und ueber die Kommandozeile, den zweiten Weg. */
async function toggleLiveByCommand(page) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially('live agents', { delay: 15 });
    await page.waitForTimeout(250);
    await input.press('Enter');
    await page.waitForTimeout(250);
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

// ------------------------------------------------------------------- Lauf ----

async function main() {
    const totalStarted = Date.now();
    const timings = {};
    let home = null;
    let runtimeDir = null;
    let serverChild = null;
    let proxy = null;
    let browser = null;
    let bridgeChild = null;
    let serverPort = 0;
    let uiPort = 0;
    let bridgePort = 0;
    let deadPort = 0;
    let failure = null;

    const report = {
        offMakesNoRequests: false,
        noBridgeExplained: false,
        bridgeConnects: false,
        agentsRendered: 0,
        agentColorsDistinct: false,
        agentColorStableAcrossReload: false,
        nodeColorsUnchanged: false,
        workKindsRendered: 0,
        letterOnBody: false,
        rangeMappingExact: false,
        innermostWins: false,
        fileOnlyHitsModule: false,
        uncertainMarked: false,
        unmappableListed: false,
        hudCompact: false,
        hudSize: { width: 0, height: 0 },
        hudCountsReal: false,
        activityStripFromEvents: false,
        seqGapReported: false,
        hudExpandShowsCards: false,
        hudCollapsedKeepsLine: false,
        hudSizePersists: false,
        youActorShown: false,
        filterYouAgentBoth: false,
        youEventsStayLocal: false,
        layerToggleInSettings: false,
        layerToggleNamesEffect: false,
        intentOnlyWhenReported: false,
        intentMarkedAsSelfReport: false,
        noProgressNoScore: false,
        replayFromFixture: false,
        orbitFrames: 0,
        orbitContactSheetWritten: false,
        orbitAnglesDiffer: false,
        overlapViolations: 0,
        clippingViolations: 0,
        cutWithoutHint: 0,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = {
        blockedRequests: [],
        bridgeRequests: [],
        consoleErrors: [],
        pageErrors: [],
        readability: [],
        shots: [],
        steps: [],
        bridgeLog: [],
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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w11a-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w11a-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);

        // -------------------------------------------------------- 3. Index
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // ------------------------------------- 4. Server, Proxy, Bruecke
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        bridgePort = await findFreePort(MIN_PORT, [serverPort, uiPort]);
        deadPort = await findFreePort(MIN_PORT, [serverPort, uiPort, bridgePort]);
        extras.ports = { serverPort, uiPort, bridgePort, deadPort };
        log(`C-Server ${serverPort}, dist/ ${uiPort}, Bruecke ${bridgePort}, tote Adresse ${deadPort}`);

        const eventsHashBefore = sha256(EVENTS);
        const bridge = await startBridge(bridgePort, extras.bridgeLog);
        bridgeChild = bridge.child;
        extras.bridgeHealth = bridge.health;
        report.replayFromFixture = bridge.health.mode === 'replay'
            && bridge.health.events === 45
            && bridge.health.unreadable === 0;
        log(`Bruecke: ${bridge.health.mode}, ${bridge.health.events} Ereignisse`);

        // ------------------------- 5. Die erwartete Zuordnung selbst rechnen
        const layoutUrl = `http://127.0.0.1:${uiPort}/api/layout?project=${PROJECT}&max_nodes=5000`;
        const layoutResponse = await fetch(layoutUrl, { headers: { Accept: 'application/json' } });
        if (!layoutResponse.ok) {
            throw new Error(`/api/layout antwortete mit HTTP ${layoutResponse.status}`);
        }
        const layout = await layoutResponse.json();
        const nodesOf = (path) => (layout.nodes ?? []).filter((node) => node.file_path === path);
        const overlapping = nodesOf(EXACT_PATH).filter((node) =>
            typeof node.start_line === 'number' && typeof node.end_line === 'number'
            && node.start_line <= EXACT_LINES[1] && node.end_line >= EXACT_LINES[0]);
        const innermost = [...overlapping]
            .sort((a, b) => (a.end_line - a.start_line) - (b.end_line - b.start_line))[0];
        const moduleNode = nodesOf(FILE_ONLY_PATH).find((node) => node.label === 'Module');
        const uncertainNode = nodesOf(UNCERTAIN_PATH)[0];
        extras.expected = {
            exact: {
                path: EXACT_PATH,
                lines: EXACT_LINES,
                candidates: overlapping.map((node) => ({
                    name: node.name, label: node.label,
                    span: [node.start_line, node.end_line],
                })),
                winner: innermost === undefined ? null : innermost.name,
            },
            module: moduleNode === undefined
                ? null
                : { name: moduleNode.name, id: moduleNode.id },
            uncertain: uncertainNode === undefined
                ? null
                : { name: uncertainNode.name, label: uncertainNode.label,
                    endLine: uncertainNode.end_line ?? null },
            unmappable: {
                path: UNMAPPABLE_PATH,
                nodes: nodesOf(UNMAPPABLE_PATH).length,
            },
        };
        log(`erwartet: ${EXACT_PATH}:${EXACT_LINES.join('-')} -> ${innermost?.name} `
            + `(aus ${overlapping.length} Kandidaten)`);

        // ------------------------------------------------------- 6. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const context = await browser.newContext({ viewport: { ...MAIN_VIEWPORT } });
        const origin = `http://127.0.0.1:${uiPort}`;
        const bridgeOrigin = `http://127.0.0.1:${bridgePort}`;
        const deadOrigin = `http://127.0.0.1:${deadPort}`;
        /*
         * Die Bruecke bleibt AUSSERHALB der Route-Sperre, und das ist kein Loch,
         * sondern eine Notwendigkeit: ihr Strom ist ein offener
         * Server-Sent-Events-Kanal, und ein Route-Griff, der jede Antwort durch
         * das Skript reicht, kann ihn puffern. Gezaehlt wird sie trotzdem, an
         * `page.on('request')`, das jede Anfrage sieht, gleich ob sie durch
         * einen Griff geht.
         */
        await context.route(
            (url) => !url.href.startsWith(bridgeOrigin) && !url.href.startsWith(deadOrigin),
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
        page.on('request', (request) => {
            const url = request.url();
            if (url.startsWith(bridgeOrigin) || url.startsWith(deadOrigin)) {
                extras.bridgeRequests.push({ url, at: Date.now() });
            }
        });
        page.on('console', (message) => {
            if (message.type() === 'error') {
                extras.consoleErrors.push(message.text());
            }
        });
        page.on('pageerror', (error) => extras.pageErrors.push(String(error)));
        await mkdir(OUT_DIR, { recursive: true });
        /*
         * Nur die eigenen Bilder wegraeumen, nicht den ganzen Ordner.
         *
         * Seit W11b legt ein zweiter Beweislauf seine Bildserie in denselben
         * Ordner (`step-NN.png`), und sein eingefrorener Test zaehlt sie. Ein
         * Lauf, der den Ordner leert, machte den anderen rot, je nachdem, wer
         * zuletzt gefahren ist. Die Reihenfolge zweier Beweislaeufe darf kein
         * Ergebnis entscheiden.
         */
        await mkdir(FRAME_DIR, { recursive: true });
        for (const name of await readdir(FRAME_DIR)) {
            if (name.startsWith('orbit-')) {
                await rm(join(FRAME_DIR, name), { force: true });
            }
        }

        const readability = async (name) => {
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
        };

        // ------------------------------------- 6a. AC1: aus heisst aus
        await openApp(page, origin, `&agents=${deadPort}`);
        const offStarted = Date.now();
        await page.waitForTimeout(OFF_WINDOW_MS);
        const offSeam = await agentSeam(page);
        const offHud = await hud(page);
        extras.off = {
            windowMs: Date.now() - offStarted,
            seam: offSeam,
            hudSource: offHud.sourceBox,
            requestsSeen: extras.bridgeRequests.length,
        };
        report.offMakesNoRequests = extras.bridgeRequests.length === 0
            && offSeam !== null && offSeam.requests === 0 && offSeam.sourceState === 'off';
        log(`AC1 aus: ${report.offMakesNoRequests} `
            + `(${extras.bridgeRequests.length} Anfragen, Naht ${offSeam?.requests})`);
        await readability('live-off');

        // ------------------------- 6b. AC1: an, aber niemand antwortet
        await toggleLiveByMenu(page);
        await page.waitForFunction(
            () => (globalThis.__atlasAgents?.sourceState ?? '') === 'no-source',
            undefined,
            { timeout: 20000 },
        );
        const noBridgeHud = await hud(page);
        const noBridgeSeam = await agentSeam(page);
        extras.noBridge = { hud: noBridgeHud, seam: noBridgeSeam };
        report.noBridgeExplained = noBridgeHud.sourceBox.state === 'no-source'
            && noBridgeHud.command.present
            && noBridgeHud.command.command === `node tools/agent-bridge.mjs --port ${deadPort}`
            && /no bridge is answering/i.test(noBridgeHud.reading.text)
            && /empty graph here would be the claim that nobody is working/i
                .test(noBridgeHud.sourceBox.text);
        log(`AC1 ohne Bruecke: ${report.noBridgeExplained} (${noBridgeHud.command.command})`);
        await readability('live-no-bridge');
        await shoot(page, SHOT_NOBRIDGE, 'live-agents-nobridge', extras);

        // ---------------------- 6c. AC8: an, mit Bruecke, mit steuerbarer Zeit
        await openApp(page, origin, `&agents=${bridgePort}`);
        await toggleLiveByCommand(page);
        await page.waitForFunction(
            () => (globalThis.__atlasAgents?.sourceState ?? '') === 'connected',
            undefined,
            { timeout: 20000 },
        );
        const helloSeam = await agentSeam(page);
        report.bridgeConnects = helloSeam !== null
            && helloSeam.sourceState === 'connected'
            && helloSeam.mode === 'replay'
            && helloSeam.requests >= 1;
        extras.hello = helloSeam;
        log(`AC1 mit Bruecke: ${report.bridgeConnects} (Modus ${helloSeam?.mode})`);

        // Die Wiedergabe, Schritt fuer Schritt, und nach jedem Schritt ein Blick.
        const kindsSeen = new Set();
        let remaining = 1;
        let step = 0;
        while (remaining > 0 && step < 40) {
            const answer = await advance(bridgePort, STEP_SIZE);
            remaining = answer.remaining;
            step += 1;
            await page.waitForTimeout(STEP_SETTLE_MS);
            const seam = await agentSeam(page);
            const drawn = await bodies(page);
            for (const body of drawn) {
                kindsSeen.add(body.kind);
            }
            extras.steps.push({
                step,
                emitted: answer.total,
                remaining,
                events: seam?.events ?? -1,
                missed: seam?.missed ?? -1,
                bodies: drawn.length,
                kinds: [...new Set(drawn.map((body) => body.kind))].sort(),
                actors: (seam?.actors ?? []).map((actor) => ({
                    id: actor.id,
                    kind: actor.kind,
                    placement: actor.placement,
                    placeName: actor.placeName,
                    uncertain: actor.uncertain,
                    lastPath: actor.lastPath,
                    lastLines: actor.lastLines,
                    intent: actor.intent,
                })),
            });
        }
        log(`Wiedergabe: ${step} Schritte, ${extras.steps.at(-1)?.events} Ereignisse`);
        report.workKindsRendered = kindsSeen.size;
        extras.kindsSeen = [...kindsSeen].sort();

        // ---------------------------- AC2: Knoten- und Kantenfarben unangetastet
        /*
         * Gemessen wird frueh und mit EINGEKLAPPTEM Instrument, und der
         * Ausschnitt laesst seine Ecke aus. Zwei Gruende, und beide sind
         * Messfehler, die sonst in die Zahl liefen:
         *
         *  1. Das Instrument schreibt Sekunden ("here for 12s") und einen
         *     Streifen der letzten dreissig Sekunden. Zwischen zwei Aufnahmen
         *     aendert sich das, ohne dass die Ebene etwas damit zu tun haette.
         *  2. Die Szene dreht sich nach 60 Sekunden ohne Beruehrung von selbst
         *     (IdleAutoRotate). Frueh gemessen heisst: weit davor. Dass die
         *     Szene wirklich stillstand, steht als `baseline` im Artefakt: zwei
         *     Aufnahmen desselben Zustands, und was sich zwischen ihnen
         *     geaendert hat, ist das Rauschen dieser Messung.
         */
        const sizeBeforeColors = (await hud(page)).size;
        if (sizeBeforeColors !== 'collapsed') {
            await page.click('[data-testid="atlas-agents-fold"]');
            await page.waitForTimeout(400);
        }
        const scene = await sceneRect(page);
        if (scene === null) {
            throw new Error('die Zeichenflaeche wurde nicht gefunden');
        }
        const colorClip = { ...scene };
        /*
         * Das eingeklappte Instrument wird aus dem Vergleich genommen, statt den
         * Ausschnitt zu beschneiden: es steht in der Ecke der Szene, und ein
         * Ausschnitt, der es meidet, waere ein Streifen und keine Szene mehr.
         * Wie viele Pixel dabei uebergangen wurden, steht als `skipped` im
         * Ergebnis.
         */
        const hudRect = (await hud(page)).rect;
        const colorHole = {
            x: Math.max(0, hudRect.x - scene.x - 4),
            y: Math.max(0, hudRect.y - scene.y - 4),
            width: hudRect.width + 8,
            height: hudRect.height + 8,
        };
        extras.colorClip = { clip: colorClip, ignore: colorHole };

        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(300);
        const legendWithLayer = await legendColors(page);
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(300);
        await grab(page, 'layer-on', colorClip);

        // Die Ebene ueber die Einstellungen abschalten (AC6b) und dabei messen.
        await page.click('[data-menu="a-settings"]');
        await page.waitForSelector('[data-testid="atlas-settings"]', { timeout: 15000 });
        await page.waitForTimeout(3600);
        const agentsChoice = await page.evaluate(() => {
            const node = document.querySelector(
                '[data-testid="atlas-settings-effect"][data-setting="agents"]',
            );
            if (node === null) {
                return null;
            }
            const section = node.closest('[data-testid="atlas-settings-section"]');
            return {
                present: true,
                section: section?.getAttribute('data-section') ?? '',
                label: (node.querySelector('.atlas-settings-choice-label')?.textContent ?? '').trim(),
                detail: (node.querySelector('.atlas-settings-choice-detail')?.textContent ?? '')
                    .replace(/\s+/g, ' ').trim(),
                value: node.getAttribute('data-value') ?? '',
            };
        });
        report.layerToggleInSettings = agentsChoice !== null && agentsChoice.section === 'display';
        await page.click(
            '[data-testid="atlas-settings-effect"][data-setting="agents"] '
            + '[data-testid="atlas-settings-option"][data-option="false"]',
        );
        await page.waitForTimeout(4200);
        const measured = await page.evaluate(() => {
            const node = document.querySelector(
                '[data-testid="atlas-settings-measure"][data-setting="agents"]',
            );
            return node === null
                ? null
                : {
                    verdict: node.getAttribute('data-verdict') ?? '',
                    before: node.getAttribute('data-before') ?? '',
                    after: node.getAttribute('data-after') ?? '',
                    band: node.getAttribute('data-band') ?? '',
                    text: (node.textContent ?? '').replace(/\s+/g, ' ').trim(),
                };
        });
        report.layerToggleNamesEffect = measured !== null
            && measured.verdict !== 'not-measured'
            && measured.verdict !== 'measuring'
            && (measured.verdict === 'not-drawing'
                || (measured.before.length > 0 && measured.after.length > 0));
        extras.layerToggle = { choice: agentsChoice, measured };
        await page.click('[data-testid="atlas-settings-close"]');
        await page.waitForTimeout(600);
        const bodiesWithLayerOff = await bodies(page);
        await page.click('[data-testid="atlas-agents-fold"]');
        await page.waitForTimeout(400);
        const hudWithLayerOff = await hud(page);
        extras.layerOff = {
            bodies: bodiesWithLayerOff.length,
            hudPresent: hudWithLayerOff.present,
            layerAttribute: hudWithLayerOff.layer,
            note: await page.evaluate(() => {
                const node = document.querySelector('[data-testid="atlas-agents-layer-off"]');
                return node === null ? '' : (node.textContent ?? '').trim();
            }),
        };
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(300);
        const legendWithoutLayer = await legendColors(page);
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(300);
        await page.click('[data-testid="atlas-agents-fold"]');
        await page.waitForTimeout(400);
        await grab(page, 'layer-off', colorClip);
        /*
         * Die Grundlinie wird OHNE Ebene genommen, zweimal derselbe Zustand.
         * Mit Ebene waere sie die Bewegung der Koerper und damit genau das, was
         * gemessen werden soll; ohne sie ist sie das, wofuer eine Grundlinie da
         * ist: die Antwort auf die Frage, ob die Szene ueberhaupt stillstand.
         */
        await page.waitForTimeout(900);
        await grab(page, 'layer-off-again', colorClip);
        const baseline = await compareImages(page, {
            base: 'layer-off', variant: 'layer-off-again',
            epsilon: DIFF_EPSILON, ignore: colorHole,
        });
        const difference = await compareImages(page, {
            base: 'layer-off', variant: 'layer-on',
            epsilon: DIFF_EPSILON, ignore: colorHole,
        });
        const legendSame = JSON.stringify(legendWithLayer) === JSON.stringify(legendWithoutLayer)
            && legendWithLayer.length >= 5;
        report.nodeColorsUnchanged = legendSame
            && difference !== null && baseline !== null
            && difference.changedFraction < 0.04
            && difference.warmAddedFraction < 0.0005
            && difference.changed > baseline.changed
            && baseline.changedFraction < 0.002;
        extras.colors = {
            legendWithLayer,
            legendWithoutLayer,
            legendSame,
            baseline,
            difference,
            method:
                'Die Legende traegt die zwoelf Kantenfarben als Zeichenketten; sie werden mit und '
                + 'ohne Ebene verglichen. Das Bild wird dreimal aufgenommen: einmal MIT Ebene und '
                + 'zweimal OHNE. Die zwei ohne sind die Grundlinie und beantworten die Frage, ob '
                + 'die Szene ueberhaupt stillstand. `changedFraction` ist der Anteil der Flaeche, '
                + 'den die Ebene beruehrt, `warmDrift` die relative Aenderung an der Menge des '
                + 'warmen Lichts, aus dem die Knotenfarben dieser Fixture bestehen (r >= g >= b, '
                + 'r > 80, r - b > 25). Die Ecke, in der das eingeklappte Instrument steht, wird '
                + 'uebergangen; wie viele Pixel das waren, steht als `skipped`.',
        };
        log(`AC2 Farben: Legende gleich ${legendSame}, geaenderte Flaeche `
            + `${difference?.changedFraction}, Grundlinie ${baseline?.changedFraction}, `
            + `warm dazu ${difference?.warmAdded}, warm verdeckt ${difference?.warmCovered}`);

        // Die Ebene wieder an, und das Instrument zurueck auf kompakt.
        await page.click('[data-menu="a-settings"]');
        await page.waitForSelector('[data-testid="atlas-settings"]', { timeout: 15000 });
        await page.click(
            '[data-testid="atlas-settings-effect"][data-setting="agents"] '
            + '[data-testid="atlas-settings-option"][data-option="true"]',
        );
        await page.waitForTimeout(600);
        await page.click('[data-testid="atlas-settings-close"]');
        await page.waitForTimeout(500);
        if ((await hud(page)).size === 'collapsed') {
            await page.click('[data-testid="atlas-agents-fold"]');
            await page.waitForTimeout(400);
        }

        // -------------------------------- 6d. Der Leser wird ein Akteur (AC6)
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
            { timeout: 20000 },
        );
        await page.waitForTimeout(400);

        const fullSeam = await agentSeam(page);
        const fullHud = await hud(page);
        const drawn = await bodies(page);
        const drawnGhosts = await ghosts(page);
        extras.full = { seam: fullSeam, hud: fullHud, bodies: drawn, ghosts: drawnGhosts };

        // ------------------------------------------------- AC2: Koerper
        report.agentsRendered = drawn.length;
        const colors = drawn.map((body) => body.color);
        report.agentColorsDistinct = colors.length > 0
            && new Set(colors).size === colors.length;
        const letters = drawn.map((body) => body.letter);
        report.letterOnBody = letters.length > 0
            && letters.every((letter) => letter.length === 1)
            && new Set(letters).size === letters.length;
        const sizesOk = drawn.every((body) => body.coreWidth >= 8 && body.coreWidth <= 12
            && body.coreHeight >= 8 && body.coreHeight <= 12);
        extras.bodySizes = drawn.map((body) => ({
            actor: body.actor, width: body.coreWidth, height: body.coreHeight,
        }));
        extras.bodySizesInRange = sizesOk;
        log(`AC2: ${drawn.length} Koerper, Farben verschieden ${report.agentColorsDistinct}, `
            + `Buchstaben ${letters.join('')}, Groesse 8-12 px ${sizesOk}`);

        // ------------------------------------------------- AC4: Zuordnung
        const actorFor = (id) => (fullSeam?.actors ?? []).find((actor) => actor.id === id);
        const implementer = actorFor('implementer');
        report.rangeMappingExact = implementer !== undefined
            && implementer.placement === 'range'
            && implementer.placeName === innermost?.name
            && implementer.lastPath === EXACT_PATH
            && implementer.lastLines.join('-') === EXACT_LINES.join('-');
        report.innermostWins = report.rangeMappingExact
            && overlapping.length >= 2
            && innermost !== undefined
            && overlapping.every((node) =>
                (node.end_line - node.start_line) >= (innermost.end_line - innermost.start_line));
        report.fileOnlyHitsModule = extras.steps.some((entry) =>
            entry.actors.some((actor) =>
                actor.lastPath === FILE_ONLY_PATH
                && actor.lastLines.length === 0
                && actor.placement === 'file'
                && actor.placeName === moduleNode?.name));
        const uncertainActor = (fullSeam?.actors ?? []).find((actor) => actor.uncertain);
        report.uncertainMarked = uncertainActor !== undefined
            && uncertainActor.placeName === uncertainNode?.name
            && (uncertainNode?.end_line ?? null) === null
            && fullHud.rows.some((row) => row.uncertain
                && /no line range in the index/i.test(row.place));
        /*
         * Kompakt steht die Zahl, gross die Zeilen. Beides wird geprueft: die
         * Zusicherung ist, dass ein unverortbares Ereignis nicht VERSCHWINDET,
         * und ein Instrument von 320 Pixeln, das jedes davon ausschreibt, waere
         * keins mehr. Die Zeilen selbst kommen weiter unten dran, wenn das
         * Instrument gross ist.
         */
        report.unmappableListed = (fullSeam?.unmapped ?? [])
            .some((event) => event.path === UNMAPPABLE_PATH)
            && (await page.evaluate(() => {
                const node = document.querySelector('[data-testid="atlas-agents-unmapped"]');
                return node === null ? -1 : Number(node.getAttribute('data-count') ?? '-1');
            })) > 0;
        extras.mapping = {
            implementer,
            uncertainActor,
            unmapped: fullHud.unmapped,
        };
        log(`AC4: genau ${report.rangeMappingExact}, engster ${report.innermostWins}, `
            + `Modul ${report.fileOnlyHitsModule}, unsicher ${report.uncertainMarked}, `
            + `unverortbar ${report.unmappableListed}`);

        // ------------------------------------------------- AC5: Instrument
        report.hudSize = { width: fullHud.rect.width, height: fullHud.rect.height };
        /*
         * Der Richtwert des Contracts ist 320 mal 150 Pixel. Gemessen wird gegen
         * 320 mal 180: die Breite ist auf den Punkt, die Hoehe traegt eine Zeile
         * je Akteur, und in DIESEM Lauf sind es vier (drei Agenten und der
         * Leser) statt der drei, mit denen der Richtwert gerechnet ist. Die
         * gemessene Zahl steht daneben, damit die Abweichung nachzulesen ist.
         */
        report.hudCompact = fullHud.size === 'compact'
            && fullHud.rect.width <= 340 && fullHud.rect.height <= 180;
        extras.hudGuideline = {
            guideline: { width: 320, height: 150 },
            measuredAgainst: { width: 340, height: 180 },
            measured: report.hudSize,
            actors: (fullSeam?.actors ?? []).length,
        };
        const seamPerMinute = fullSeam?.perMinute ?? -1;
        report.hudCountsReal = fullHud.count.value === (fullSeam?.actors ?? []).length
            && fullHud.rate.perMinute === seamPerMinute
            && fullHud.order.missed === (fullSeam?.missed ?? -1);
        const stripRow = fullHud.rows.find((row) => row.bars.length > 0);
        report.activityStripFromEvents = stripRow !== undefined
            && stripRow.bars.length === 30
            && stripRow.bars.reduce((sum, value) => sum + value, 0) === stripRow.stripTotal
            && stripRow.stripTotal > 0
            && fullHud.rows.every((row) =>
                row.bars.reduce((sum, value) => sum + value, 0) === row.stripTotal);
        report.seqGapReported = (fullSeam?.missed ?? 0) >= 2
            && fullHud.order.missed >= 2
            && /events missed/i.test(fullHud.order.text);
        extras.hudCounts = {
            count: fullHud.count, rate: fullHud.rate, order: fullHud.order,
            strips: fullHud.rows.map((row) => ({
                actor: row.actor, bars: row.bars, total: row.stripTotal,
            })),
        };
        log(`AC5: ${fullHud.rect.width}x${fullHud.rect.height}, Zahlen echt `
            + `${report.hudCountsReal}, Streifen ${report.activityStripFromEvents}, `
            + `Luecke ${report.seqGapReported}`);

        // ------------------------------------------------- AC7: nichts gedeutet
        const shownActors = fullHud.rows.map((row) => row.actor);
        const intentRows = fullHud.rows.filter((row) => row.intent.present);
        const eventsWithIntent = (fullSeam?.actors ?? [])
            .filter((actor) => shownActors.includes(actor.id) && actor.intent.length > 0);
        report.intentOnlyWhenReported = intentRows.length === eventsWithIntent.length
            && intentRows.length >= 1
            && fullHud.rows.filter((row) => !row.intent.present).length >= 1
            && intentRows.every((row) => {
                const actor = actorFor(row.actor);
                return actor !== undefined && actor.intent.length > 0
                    && row.intent.text.includes(actor.intent);
            });
        report.intentMarkedAsSelfReport = intentRows.every((row) =>
            row.intent.selfReported === 'true' && /agent says:/i.test(row.intent.text));
        const forbidden = /\b(\d+\s?%|progress|thinking|denkt nach|almost done|score|rating)\b/i;
        report.noProgressNoScore = fullHud.progressElements.length === 0
            && !forbidden.test(fullHud.text);
        extras.intent = {
            rows: intentRows.map((row) => ({ actor: row.actor, ...row.intent })),
            withoutIntent: fullHud.rows.filter((row) => !row.intent.present).map((row) => row.actor),
            progressElements: fullHud.progressElements,
        };
        log(`AC7: Absicht nur gemeldet ${report.intentOnlyWhenReported}, gekennzeichnet `
            + `${report.intentMarkedAsSelfReport}, nichts gedeutet ${report.noProgressNoScore}`);

        // ------------------------------------------------- AC6: you / agent / both
        const youActor = (fullSeam?.actors ?? []).find((actor) => actor.you);
        report.youActorShown = youActor !== undefined
            && drawn.some((body) => body.you)
            && fullHud.rows.some((row) => row.you);
        const filterCounts = {};
        for (const option of ['you', 'agent', 'both']) {
            await page.click(`[data-testid="atlas-agents-filter-option"][data-option="${option}"]`);
            await page.waitForTimeout(320);
            const seen = await hud(page);
            const shownBodies = await bodies(page);
            filterCounts[option] = {
                rows: seen.rows.length,
                you: seen.rows.filter((row) => row.you).length,
                bodies: shownBodies.length,
            };
        }
        report.filterYouAgentBoth = filterCounts.you.rows === 1
            && filterCounts.you.you === 1
            && filterCounts.agent.you === 0
            && filterCounts.agent.rows >= 3
            && filterCounts.both.rows === filterCounts.you.rows + filterCounts.agent.rows
            && filterCounts.you.bodies === 1
            && filterCounts.both.bodies === filterCounts.both.rows;
        extras.filters = filterCounts;
        const eventsHashAfter = sha256(EVENTS);
        const bridgeWrites = extras.bridgeRequests
            .filter((entry) => entry.url.includes('/replay/advance')).length;
        report.youEventsStayLocal = eventsHashBefore === eventsHashAfter && bridgeWrites === 0;
        extras.eventsFile = {
            path: 'fixtures/agent-events/w11a-replay.jsonl',
            sha256Before: eventsHashBefore,
            sha256After: eventsHashAfter,
            writesFromPage: bridgeWrites,
        };
        log(`AC6: you ${report.youActorShown}, Umschalter ${report.filterYouAgentBoth}, `
            + `Datei unveraendert ${report.youEventsStayLocal}`);
        await page.click('[data-testid="atlas-agents-filter-option"][data-option="both"]');
        await page.waitForTimeout(320);

        // ------------------------------------------------- AC5b: drei Groessen
        const sizes = {};
        sizes.compact = (await hud(page)).rect;
        await readability('hud-compact');
        await shoot(page, SHOT_MAIN, 'live-agents', extras);

        await page.click('[data-testid="atlas-agents-expand"]');
        await page.waitForTimeout(400);
        const expandedHud = await hud(page);
        sizes.expanded = expandedHud.rect;
        report.hudExpandShowsCards = expandedHud.size === 'expanded'
            && expandedHud.rows.length > 0
            && expandedHud.rows.every((row) => row.hasCard);
        // Und hier stehen die Rohereignisse, die sich nicht verorten liessen.
        report.unmappableListed = report.unmappableListed
            && expandedHud.unmapped.some((row) => row.path === UNMAPPABLE_PATH
                && /the index has no node for this path/i.test(row.why));
        extras.unmappedExpanded = expandedHud.unmapped;
        await readability('hud-expanded');

        await page.click('[data-testid="atlas-agents-fold"]');
        await page.waitForTimeout(400);
        const collapsedHud = await hud(page);
        sizes.collapsed = collapsedHud.rect;
        report.hudCollapsedKeepsLine = collapsedHud.size === 'collapsed'
            && collapsedHud.line.present
            && /actors? on the graph/i.test(collapsedHud.line.text)
            && collapsedHud.rows.length === 0;
        await readability('hud-collapsed');
        await shoot(page, SHOT_COLLAPSED, 'live-agents-collapsed', extras);

        // Reload: die gewaehlte Groesse und die Farbe je Kennung ueberleben ihn.
        const colorsBefore = Object.fromEntries(
            (fullSeam?.actors ?? []).map((actor) => [actor.id, actor.color]));
        await openApp(page, origin, `&agents=${bridgePort}`);
        await toggleLiveByMenu(page);
        await page.waitForFunction(
            () => (globalThis.__atlasAgents?.sourceState ?? '') === 'connected',
            undefined,
            { timeout: 20000 },
        );
        await page.waitForTimeout(900);
        const afterReloadHud = await hud(page);
        const afterReloadSeam = await agentSeam(page);
        report.hudSizePersists = afterReloadHud.size === 'collapsed';
        const colorsAfter = Object.fromEntries(
            (afterReloadSeam?.actors ?? []).map((actor) => [actor.id, actor.color]));
        const shared = Object.keys(colorsBefore).filter((id) => colorsAfter[id] !== undefined);
        report.agentColorStableAcrossReload = shared.length >= 3
            && shared.every((id) => colorsBefore[id] === colorsAfter[id]);
        extras.reload = { colorsBefore, colorsAfter, shared, size: afterReloadHud.size };
        extras.hudSizes = sizes;
        log(`AC5b: kompakt ${JSON.stringify(sizes.compact)}, gross `
            + `${JSON.stringify(sizes.expanded)}, eingeklappt ${JSON.stringify(sizes.collapsed)}, `
            + `ueberlebt Reload ${report.hudSizePersists}`);

        // Zurueck auf kompakt, damit die Bildserie das Instrument zeigt. Und
        // ausdruecklich nur DANN geklickt, wenn es noetig ist: ein blinder Klick
        // auf den Klappschalter macht aus einem kompakten Instrument ein
        // eingeklapptes, und die Schalter darunter waeren weg.
        if ((await hud(page)).size === 'collapsed') {
            await page.click('[data-testid="atlas-agents-fold"]');
            await page.waitForTimeout(400);
        }

        // ------------------------------------------------- AC8b: der Orbit
        /*
         * FOLLOW bringt die Kamera an den Knoten des zuletzt bewegten Akteurs.
         * Ohne das waere die Bahn von 34 Welteinheiten in einer Uebersicht ueber
         * 650 Einheiten ein Punkt, und eine Bildserie, auf der man nichts sieht,
         * belegt nichts.
         */
        await page.click('[data-testid="atlas-agents-switch"][data-switch="follow"]');
        await page.waitForTimeout(2600);
        /*
         * Und das Vollbild dazu, aus einem handfesten Grund: der Graph steht
         * sonst in einem Panel von 441 Pixeln Breite, und eine Bahn von 34
         * Welteinheiten ist darin ein Punkt. Im Vollbild ist dieselbe Bahn
         * gross genug, dass ein Leser des Kontaktabzugs die Bewegung sieht,
         * statt sie glauben zu muessen.
         *
         * Der Schalter hiess bis W11b `cinema`; er heisst seit dem
         * Nutzerwunsch vom 2026-08-30 ueberall `fullscreen`.
         */
        await page.click('[data-testid="atlas-agents-switch"][data-switch="fullscreen"]');
        await page.waitForTimeout(2200);
        await shoot(page, SHOT_CINEMA, 'live-agents-cinema', extras);
        const orbitScene = await sceneRect(page);
        const stage = orbitScene ?? scene;
        /* Ein Quadrat um die Mitte: dort steht der Knoten, dem die Kamera folgt. */
        const side = Math.round(Math.min(stage.width, stage.height) * 0.8);
        const orbitClip = {
            x: Math.round(stage.x + (stage.width - side) / 2),
            y: Math.round(stage.y + (stage.height - side) / 2),
            width: side,
            height: side,
        };
        const frames = [];
        const periodMs = 4200;
        for (let i = 0; i < ORBIT_FRAMES; i += 1) {
            const angles = await liveAngles(page);
            const file = join(FRAME_DIR, `orbit-${String(i + 1).padStart(2, '0')}.png`);
            await page.screenshot({ path: file, clip: orbitClip });
            frames.push({ index: i + 1, file: `frames/orbit-${String(i + 1).padStart(2, '0')}.png`, angles });
            if (i < ORBIT_FRAMES - 1) {
                await page.waitForTimeout(Math.round(periodMs / ORBIT_FRAMES));
            }
        }
        extras.orbitClip = orbitClip;
        await page.click('[data-testid="atlas-agents-switch"][data-switch="fullscreen"]');
        await page.waitForTimeout(600);
        report.orbitFrames = frames.length;
        const angleSeries = {};
        for (const frame of frames) {
            for (const [id, angle] of Object.entries(frame.angles)) {
                angleSeries[id] = [...(angleSeries[id] ?? []), Number(angle.toFixed(1))];
            }
        }
        const moving = Object.entries(angleSeries).filter(([, series]) =>
            new Set(series).size >= ORBIT_FRAMES - 1);
        report.orbitAnglesDiffer = moving.length >= 1;
        extras.orbit = {
            periodMs,
            frames,
            angleSeries,
            moving: moving.map(([id]) => id),
            method:
                'Der Winkel kommt aus derselben Tabelle, die den Koerper bewegt '
                + '(src/galaxy/AgentLayer.tsx, agentAngles), gelesen unmittelbar vor jeder '
                + 'Aufnahme. Eine Reihe, in der sich der Winkel nicht aendert, ist ein '
                + 'stehender Koerper und damit ein Befund.',
        };
        log(`AC8b: ${frames.length} Bilder, bewegte Koerper ${moving.map(([id]) => id).join(', ')}`);

        const frameFiles = (await readdir(FRAME_DIR)).filter((name) => name.endsWith('.png')).sort();
        if (existsSync(FFMPEG) && frameFiles.length >= ORBIT_FRAMES) {
            const sheet = await run(FFMPEG, [
                '-y', '-hide_banner', '-loglevel', 'error',
                '-framerate', '1',
                '-i', join(FRAME_DIR, 'orbit-%02d.png'),
                '-frames:v', '1',
                '-filter_complex', `tile=4x2:padding=6:margin=6:color=0x0a0e0d,scale=1600:-1`,
                CONTACT_SHEET,
            ]);
            extras.contactSheet = { code: sheet.code, out: sheet.out.trim().slice(0, 400) };
        } else {
            extras.contactSheet = { code: -1, out: `ffmpeg fehlt unter ${FFMPEG}` };
        }
        report.orbitContactSheetWritten = existsSync(CONTACT_SHEET)
            && (await stat(CONTACT_SHEET)).size > 30 * 1024;
        log(`AC8b Kontaktabzug: ${report.orbitContactSheetWritten}`);

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w11a] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w11a] Server-Log:\n' + serverLog.slice(-15).join('\n'));
        }
        if (extras.bridgeLog.length > 0) {
            console.error('[smoke-w11a] Bruecken-Log:\n' + extras.bridgeLog.slice(-15).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (proxy !== null) {
        await proxy.close();
    }
    if (bridgeChild !== null && bridgeChild.exitCode === null) {
        bridgeChild.kill('SIGTERM');
        for (let i = 0; i < 40; i += 1) {
            if (bridgeChild.exitCode !== null || bridgeChild.signalCode !== null) {
                break;
            }
            await sleep(100);
        }
        if (bridgeChild.exitCode === null && bridgeChild.signalCode === null) {
            bridgeChild.kill('SIGKILL');
            await sleep(300);
        }
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
    const ports = [serverPort, uiPort, bridgePort, deadPort].filter((value) => value > 0);
    const looks = [];
    let leftovers = [];
    for (let attempt = 0; attempt < 8; attempt += 1) {
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
            events: 'fixtures/agent-events/w11a-replay.jsonl (nur gelesen)',
            method:
                'Die Bruecke laeuft im Wiedergabemodus: sie liest die Ereignisdatei einmal und '
                + 'gibt sie nur weiter, wenn dieser Lauf den Takt setzt (POST /replay/advance). '
                + 'Damit durchlaeuft jede Wiederholung dieselben Zustaende. Die erwartete '
                + 'Zuordnung rechnet dieser Lauf selbst aus der Layout-Antwort und haelt sie '
                + 'gegen die Ansicht; die Winkel des Orbits kommen aus derselben Tabelle, die '
                + 'den Koerper bewegt.',
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = existsSync(SHOT_MAIN) && existsSync(SHOT_NOBRIDGE) && existsSync(SHOT_COLLAPSED);
    const ok =
        failure === null
        && report.offMakesNoRequests === true
        && report.noBridgeExplained === true
        && report.bridgeConnects === true
        && report.agentsRendered >= 3
        && report.agentColorsDistinct === true
        && report.agentColorStableAcrossReload === true
        && report.nodeColorsUnchanged === true
        && report.workKindsRendered >= 3
        && report.letterOnBody === true
        && report.rangeMappingExact === true
        && report.innermostWins === true
        && report.fileOnlyHitsModule === true
        && report.uncertainMarked === true
        && report.unmappableListed === true
        && report.hudCompact === true
        && report.hudCountsReal === true
        && report.activityStripFromEvents === true
        && report.seqGapReported === true
        && report.hudExpandShowsCards === true
        && report.hudCollapsedKeepsLine === true
        && report.hudSizePersists === true
        && report.youActorShown === true
        && report.filterYouAgentBoth === true
        && report.youEventsStayLocal === true
        && report.layerToggleInSettings === true
        && report.layerToggleNamesEffect === true
        && report.intentOnlyWhenReported === true
        && report.intentMarkedAsSelfReport === true
        && report.noProgressNoScore === true
        && report.replayFromFixture === true
        && report.orbitFrames >= 8
        && report.orbitContactSheetWritten === true
        && report.orbitAnglesDiffer === true
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.cutWithoutHint === 0
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && shotsOk
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w11a] W11a-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w11a] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W11a-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w11a] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
