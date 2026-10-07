#!/usr/bin/env node
/*
 * W5c-Smoke: der Flow-Erklaerer als Overlay, der Status-Punkt an jeder Datei,
 * das dauerhaft sichtbare Graph-Panel, die lesbare Hierarchie und die
 * scrollende Tab-Leiste, alles an einem echten Server.
 *
 * Vier Wellen Nutzerfeedback vom 2026-08-29 stehen dahinter, und keine davon
 * laesst sich in jsdom beantworten. Die Unit-Tests zeigen an aufgezeichneten
 * Fakten, dass die Geometrie stimmt und dass die Liste die Zeilen des Blocks
 * traegt. Sie koennen nicht zeigen, dass NICHTS vor dem Overlay liegt (das ist
 * eine Frage an `elementFromPoint` in einem Browser mit echtem Layout), dass
 * der Grund wirklich dunkel gemalt wird (eine Frage an die gerechnete
 * Hintergrundfarbe), dass das Graph-Panel waehrend einer Fuehrung im Viewport
 * steht (eine Frage an getBoundingClientRect), dass sich keine zwei
 * Beschriftungen der Hierarchie ueberlagern (eine Frage an die Kaesten, die die
 * Szene wirklich gezeichnet hat) und dass die Tab-Leiste bei elf offenen
 * Dateien scrollt statt umzubrechen.
 *
 * Ablauf:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4360, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *   6a. Legende: im frischen Browser default zu
 *   6b. Explorer: der Status-Punkt an jeder Datei, auch am Gutfall
 *   6c. Tab-Leiste: elf Dateien oeffnen, scrollen, nichts ueberlagert
 *   6d. Overlay auf Tiefe 0
 *   6e. Overlay auf Tiefe 3: Diagramm, Liste, Stepper, Ehrlichkeit, Escape
 *   6f. Twin-Ueberlaenge: das Graph-Panel bleibt stehen
 *   6g. Topsort-Fuehrung: die Galaxy folgt und bleibt im Bild
 *   6h. Entry-Walk createUser (gross) und getOrder (klein): Hierarchie lesbar
 *   7. abraeumen, Restprozesse zaehlen, JSON und zwei Screenshots schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w5c).
 *
 * ## Drei Entscheidungen, die man sonst raten muesste
 *
 * **Die Fixture wird nur gelesen.** Dieser Lauf aendert keine Datei: er braucht
 * keinen Diff, sondern nur einen Index. fixtures/atlas-sample bleibt
 * byte-identisch.
 *
 * **CBM_RUNTIME_DIR wird gesetzt.** Der Daemon des Servers und jede CLI
 * verabreden sich in einem Rendezvous-Verzeichnis, das per Konto und nicht per
 * HOME gilt. Ohne das waere ein Lauf neben einer anderen CBM-Instanz nicht rot,
 * sondern kaputt. Wortgleich mit tools/smoke-w4c.mjs.
 *
 * **Die Ueberlappung der Beschriftungen wird in WELTkoordinaten gemessen.** Die
 * Namen der Szene sind Sprites in einem WebGL-Bild und haben kein DOM-Rechteck.
 * Die Ebene, die sie zeichnet, meldet die Kaesten, die sie wirklich gesetzt hat
 * (NodeLabels.onLayout). Alle Knoten dieser Ansicht liegen auf z=0 und die
 * Kamera steht frontal davor, also ist eine Ueberlappung dieser Rechtecke genau
 * eine Ueberlappung auf dem Schirm. Eine zweite Rechnung im Beweislauf waere
 * eine zweite Wahrheit ueber dieselbe Schrift.
 */

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
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
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w5c';
const OUT_DIR = join(ROOT, 'verification', 'w5');
const OUT_JSON = join(OUT_DIR, 'flowfix.json');
const MIN_PORT = 4360;

/** Das Symbol, ueber das der Erklaerer spricht. Dasselbe wie in W4c. */
const TARGET = 'createUser';
const TARGET_FILE = 'src/services/userService.ts';

/** Das Symbol mit dem langen Fakten-Block, an dem die Twin-Ueberlaenge haengt. */
const LONG_TARGET = 'hotspotScan';

/**
 * Die Fensterbreite, in der die Tab-Leiste gemessen wird.
 *
 * Schmaler als der Rest des Laufs, und das ist die Messung und kein Trick: elf
 * Tabs der Fixture sind zusammen rund 1430 Pixel breit und passen in ein 1680
 * Pixel breites Fenster ohne Weiteres. Die Zusicherung lautet nicht "die
 * Leiste scrollt immer", sondern "wenn sie ueberlaeuft, scrollt sie, statt
 * umzubrechen oder in die Nachbarn zu wachsen". Gemessen wird sie also in
 * einem Fenster, in dem sie wirklich ueberlaeuft; danach geht der Lauf in
 * seiner eigentlichen Groesse weiter.
 */
const TABS_VIEWPORT = { width: 960, height: 1050 };
const MAIN_VIEWPORT = { width: 1680, height: 1050 };

/** Die zwei Walk-Groessen der Hierarchie-Messung. */
const LARGE_WALK = 'createUser';
const SMALL_WALK = 'getOrder';

/** Chromium ohne Aussenwelt, plus die Software-GL-Flags aus smoke-w4e. */
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

const log = (...parts) => console.log('[smoke-w5c]', ...parts);
const serverLog = [];

function run(command, args) {
    return new Promise((done) => {
        const child = spawn(command, args, {
            cwd: ROOT,
            env: {
                ...process.env,
                NO_UPDATE_NOTIFIER: '1',
                npm_config_update_notifier: 'false',
                npm_config_audit: 'false',
                npm_config_fund: 'false',
            },
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        let out = '';
        child.stdout.on('data', (d) => {
            out += d.toString();
        });
        child.stderr.on('data', (d) => {
            out += d.toString();
        });
        child.on('error', (error) => done({ code: 127, out: out + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out }));
    });
}

// ------------------------------------------------------------- Testgriffe ---

/**
 * Alles, was das Overlay gerade zeigt, in einem Zug abgelesen.
 *
 * Die Hintergrundfarbe kommt aus getComputedStyle und nicht aus der Klasse:
 * gefragt ist, was der Browser wirklich malt. Die vier Punkte fuer
 * `elementFromPoint` liegen bei 20 und 80 Prozent der Overlay-Flaeche, also
 * sicher innerhalb und weit genug auseinander, dass ein einzelnes Element sie
 * nicht zufaellig alle traegt.
 */
const overlaySeam = (page) =>
    page.evaluate(() => {
        const overlay = document.querySelector('[data-testid="atlas-flow-overlay"]');
        if (overlay === null) {
            return { present: false };
        }
        const list = (id) => [...document.querySelectorAll(`[data-testid="${id}"]`)];
        const text = (id) =>
            document.querySelector(`[data-testid="${id}"]`)?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        const box = document.querySelector('[data-testid="atlas-flow"]');
        const rect = overlay.getBoundingClientRect();
        const points = [[0.2, 0.2], [0.8, 0.2], [0.2, 0.8], [0.8, 0.8]].map(([fx, fy]) => [
            rect.left + rect.width * fx,
            rect.top + rect.height * fy,
        ]);
        const hitChains = [];
        const hits = points.map(([x, y]) => {
            const hit = document.elementFromPoint(x, y);
            // Was an dieser Stelle wirklich liegt, als Kette. Ein blosser
            // Elementname sagt bei einem Befund nicht, WO das Ding sitzt, und
            // genau das ist die Frage, wenn dieser Halt einmal rot wird.
            const chain = [];
            for (let node = hit, i = 0; node !== null && i < 6; node = node.parentElement, i += 1) {
                chain.push(`${node.tagName}.${(node.getAttribute('class') ?? '').split(' ')[0]}`);
            }
            hitChains.push({ x: Math.round(x), y: Math.round(y), chain });
            if (hit === null) {
                return 'nothing';
            }
            if (hit === overlay || overlay.contains(hit)) {
                return 'overlay';
            }
            return hit.getAttribute('data-testid') ?? hit.tagName.toLowerCase();
        });
        const rgb = globalThis.getComputedStyle(overlay).backgroundColor;
        const parts = /rgba?\(([^)]+)\)/.exec(rgb);
        const channels = parts === null ? [] : parts[1].split(',').map((value) => Number(value.trim()));
        // Die uebliche Helligkeitsformel. Der Abnahmetest verlangt < 0.2.
        const luminance = channels.length >= 3
            ? (0.299 * channels[0] + 0.587 * channels[1] + 0.114 * channels[2]) / 255
            : 1;
        return {
            present: true,
            title: text('atlas-flow-overlay-title'),
            // Der Ausgang gehoert seit W8 der Zone. Gelesen wird er weiterhin,
            // damit im Artefakt steht, dass es einen gibt und wie er heisst.
            esc: text('atlas-explain-collapse'),
            closePresent: document.querySelector('[data-testid="atlas-explain-collapse"]') !== null,
            diagramPresent: document.querySelector('[data-testid="atlas-flow-diagram"]') !== null,
            lifelines: list('atlas-flow-lifeline').length,
            participants: list('atlas-flow-participant').map((node) => node.getAttribute('data-label')),
            arrows: list('atlas-flow-arrow').map((node) => ({
                index: Number(node.getAttribute('data-index') ?? '-1'),
                to: node.getAttribute('data-to'),
                message: node.getAttribute('data-message'),
                shape: node.getAttribute('data-shape'),
                current: node.getAttribute('data-current') === 'true',
            })),
            loops: list('atlas-flow-raise').map((node) => ({
                participant: node.getAttribute('data-participant'),
                current: node.getAttribute('data-current') === 'true',
                text: node.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            })),
            groups: list('atlas-flow-group').map((node) => node.textContent?.trim() ?? ''),
            absences: list('atlas-flow-absence').map((node) => node.textContent?.replace(/\s+/g, ' ').trim() ?? ''),
            honesty: list('atlas-flow-honesty').map((node) => node.textContent?.replace(/\s+/g, ' ').trim() ?? ''),
            /*
             * Die Herkunftssaetze stehen seit W8b hinter dem Fragezeichen
             * neben dem Bild (W8b AC6). Aus vier Absaetzen mit zusammen 954
             * Zeichen wurde EIN Satz plus die Zahlen am Rand des Bildes, weil
             * eine Ehrlichkeit, die als Textwand erscheint, ueberlesen wird wie
             * ein Cookie-Banner. Verschwunden ist nichts: dieser Lauf liest die
             * drei Saetze weiterhin, nur eben dort, wo sie jetzt stehen.
             */
            provenance: document.querySelector('[data-testid="atlas-flow-provenance"]')
                ?.getAttribute('data-hint') ?? '',
            generated: text('atlas-flow-generated'),
            steps: Number(box?.getAttribute('data-steps') ?? '0'),
            activeStep: Number(box?.getAttribute('data-active-step') ?? '-1'),
            activeArrow: Number(box?.getAttribute('data-active-arrow') ?? '-1'),
            markedSteps: list('atlas-flow-step')
                .map((node) => (node.getAttribute('data-active') === 'true'
                    ? Number(node.getAttribute('data-step') ?? '-1')
                    : -1))
                .filter((index) => index >= 0),
            position: text('atlas-flow-position'),
            backgroundColor: rgb,
            luminance,
            hits,
            hitChains,
            rect: { x: rect.x, y: rect.y, width: rect.width, height: rect.height },
        };
    });

/** Wo der Editor gerade steht: Datei und Zeile. Wortgleich mit smoke-w4c. */
const readerSeam = (page) =>
    page.evaluate(() => ({
        path: globalThis.__atlasReader?.document?.path ?? '',
        line: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
    }));

/** Die Status-Punkte des Explorers, so wie sie wirklich dastehen. */
const dotsSeam = (page) =>
    page.evaluate(() => {
        const shown = (node) => {
            const rect = node.getBoundingClientRect();
            const style = globalThis.getComputedStyle(node);
            return rect.width > 0 && rect.height > 0
                && style.visibility !== 'hidden' && style.display !== 'none';
        };
        const dots = [...document.querySelectorAll('[data-testid="atlas-tree-dot"]')].filter(shown);
        const legend = [...document.querySelectorAll('[data-testid="atlas-tree-legend-entry"]')];
        const good = legend.find((entry) => entry.getAttribute('data-coverage') === 'indexed');
        return {
            total: dots.length,
            byState: dots.reduce((tally, dot) => {
                const state = dot.getAttribute('data-coverage') ?? 'unknown';
                tally[state] = (tally[state] ?? 0) + 1;
                return tally;
            }, {}),
            /*
             * Seit W8b traegt die ZEILE den Satz und nicht mehr der Punkt
             * darin, und er steht in einem eigenen Tooltip im Dokument statt in
             * einem `title` (siehe src/ui/tooltip/). Beides hing bis dahin am
             * selben Text; zwei Kaesten fuer eine Auskunft waeren zwei Kaesten,
             * die sich beim Zeigen gegenseitig verdecken.
             *
             * Die Zusicherung dieses Laufs ist unveraendert und wird hier
             * unveraendert geprueft: zu JEDEM Punkt steht eine Auskunft ueber
             * Stufe und Grund einen Zeiger entfernt. Gelesen wird sie an der
             * Zeile, in der der Punkt sitzt.
             */
            titled: dots.filter((dot) => (
                dot.closest('[data-hint]')?.getAttribute('data-hint') ?? ''
            ).length > 0).length,
            sampleTitle: dots[0]?.closest('[data-hint]')?.getAttribute('data-hint') ?? '',
            legendGood: good === undefined
                ? null
                : {
                    hasDot: good.querySelector('[data-testid="atlas-tree-legend-dot"]') !== null,
                    tone: good.querySelector('[data-testid="atlas-tree-legend-dot"]')
                        ?.getAttribute('data-tone') ?? '',
                    text: good.textContent?.replace(/\s+/g, ' ').trim() ?? '',
                },
        };
    });

/** Die Tab-Leiste, mit den Rechtecken ihrer Nachbarn. */
const tabsSeam = (page) =>
    page.evaluate(() => {
        const bar = document.querySelector('[data-testid="atlas-tabs"]');
        if (bar === null) {
            return { present: false };
        }
        const barRect = bar.getBoundingClientRect();
        const tabs = [...bar.querySelectorAll('.atlas-tab')].map((tab) => {
            const rect = tab.getBoundingClientRect();
            return {
                path: tab.getAttribute('data-path'),
                active: tab.getAttribute('data-active') === 'true',
                x: rect.x, y: rect.y, width: rect.width, height: rect.height,
            };
        });
        const neighbour = (id) => {
            const node = document.querySelector(`[data-testid="${id}"]`);
            if (node === null) {
                return null;
            }
            const rect = node.getBoundingClientRect();
            return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
        };
        return {
            present: true,
            scrollLeft: bar.scrollLeft,
            scrollWidth: bar.scrollWidth,
            clientWidth: bar.clientWidth,
            bar: { x: barRect.x, y: barRect.y, width: barRect.width, height: barRect.height },
            tabs,
            overflow: [...document.querySelectorAll('[data-testid="atlas-tabs-overflow"]')].map((mark) => ({
                side: mark.getAttribute('data-side'),
                on: mark.getAttribute('data-on') === 'true',
            })),
            tree: neighbour('atlas-tree'),
            twin: neighbour('atlas-twin'),
        };
    });

/** Das Graph-Panel: seine Lage, seine Untergrenze und die Anteile im Panel. */
const panelSeam = (page) =>
    page.evaluate(() => {
        const panel = document.querySelector('[data-testid="atlas-galaxy"]');
        if (panel === null) {
            return { present: false };
        }
        const rect = panel.getBoundingClientRect();
        const of = (id) => {
            const node = document.querySelector(`[data-testid="${id}"]`);
            if (node === null) {
                return null;
            }
            const box = node.getBoundingClientRect();
            return { x: box.x, y: box.y, width: box.width, height: box.height };
        };
        const twinBody = document.querySelector('.atlas-twin-body');
        return {
            present: true,
            rect: { x: rect.x, y: rect.y, width: rect.width, height: rect.height },
            minHeight: Number.parseFloat(globalThis.getComputedStyle(panel).minHeight) || 0,
            viewport: { width: globalThis.innerWidth, height: globalThis.innerHeight },
            inViewport:
                rect.height > 0
                && rect.top >= 0
                && rect.bottom <= globalThis.innerHeight + 0.5
                && rect.left >= 0
                && rect.right <= globalThis.innerWidth + 0.5,
            mode: panel.getAttribute('data-mode') ?? '',
            scene: of('atlas-galaxy-scene'),
            legend: of('atlas-galaxy-legend'),
            legendToggle: document.querySelector('[data-testid="atlas-galaxy-legend-toggle"]')
                ?.getAttribute('aria-expanded') ?? '',
            collapsePresent: document.querySelector('[data-testid="atlas-galaxy-collapse"]') !== null,
            twinBodyScrolls: twinBody === null
                ? false
                : twinBody.scrollHeight > twinBody.clientHeight + 1,
            sideOverflow: (() => {
                const side = document.querySelector('.atlas-side');
                return side === null ? 0 : side.scrollHeight - side.clientHeight;
            })(),
        };
    });

/** Der Griff des Graph-Panels, plus die Kaesten der Beschriftungen. */
const graphSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasGalaxy;
        return seam === undefined
            ? null
            : {
                mode: seam.mode,
                bloom: seam.bloom,
                targetChanges: seam.targetChanges,
                hierarchyNodes: seam.hierarchy?.nodes ?? 0,
                hierarchyDepth: seam.hierarchy?.depth ?? 0,
                labelBoxes: (seam.labelBoxes ?? []).map((box) => ({
                    name: box.name,
                    x: box.x,
                    y: box.y,
                    width: box.width,
                    height: box.height,
                })),
            };
    });

/**
 * Wie viele der gemeldeten Namenskaesten sich ueberlagern.
 *
 * Paarweise und ohne Toleranz nach unten: zwei Rechtecke, die sich beruehren,
 * ueberlagern sich nicht, zwei die sich schneiden, schon. Ein Bruchteil einer
 * Welteinheit Ueberschneidung waere auf dem Schirm ein Bruchteil eines Pixels;
 * die Toleranz von 0.5 haelt das aus dem Befund heraus, ohne eine echte
 * Ueberlagerung zu verstecken.
 */
function labelOverlaps(boxes) {
    let count = 0;
    for (let i = 0; i < boxes.length; i += 1) {
        for (let j = i + 1; j < boxes.length; j += 1) {
            const a = boxes[i];
            const b = boxes[j];
            const dx = Math.abs(a.x - b.x) - (a.width + b.width) / 2;
            const dy = Math.abs(a.y - b.y) - (a.height + b.height) / 2;
            if (dx < -0.5 && dy < -0.5) {
                count += 1;
            }
        }
    }
    return count;
}

/** Ob sich zwei Rechtecke schneiden. Beruehren zaehlt nicht. */
function rectsOverlap(a, b) {
    if (a === null || b === null) {
        return false;
    }
    return a.x + a.width > b.x + 0.5
        && b.x + b.width > a.x + 0.5
        && a.y + a.height > b.y + 0.5
        && b.y + b.height > a.y + 0.5;
}

// ------------------------------------------------------------- Klickstrecke -

/** Die Seite laden und warten, bis Statusleiste, Baum und Galaxie stehen. */
async function openApp(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 30000 });
}

/** Bis in die letzte Ebene aufklappen, ueber genau den Klick, den ein Leser tut. */
async function expandAll(page) {
    for (let round = 0; round < 12; round += 1) {
        const clicked = await page.evaluate(() => {
            const rows = [...document.querySelectorAll(
                '[data-testid="atlas-tree-row"][data-kind="dir"][data-expanded="false"]',
            )];
            for (const row of rows) {
                row.click();
            }
            return rows.length;
        });
        if (clicked === 0) {
            return round;
        }
        await page.waitForTimeout(400);
    }
    return 12;
}

/** Zu einem Symbol navigieren, ueber dieselbe Suche, die die Kommandozeile fuehrt. */
async function openSymbol(page, name, file, qualified) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(name, { delay: 40 });
    await page.waitForSelector(`[data-testid="atlas-search-row"][data-name="${name}"]`, { timeout: 30000 });
    await page.waitForTimeout(600);
    await page.click(`[data-testid="atlas-search-row"][data-name="${name}"]`);
    await page.waitForFunction(
        (expected) => globalThis.__atlasReader?.document?.path === expected,
        file,
        { timeout: 40000 },
    );
    await page.waitForFunction(
        (expected) => new RegExp(`${expected}$`).test(globalThis.__atlasTwin?.qualifiedName ?? ''),
        qualified,
        { timeout: 40000 },
    );
}

/** Die Detailstufe setzen, ueber denselben Regler, den ein Leser schiebt. */
async function setDepth(page, depth) {
    await page.locator('[data-testid="atlas-twin-depth"]').fill(String(depth));
    await page.waitForFunction(
        (expected) => document.querySelector('[data-testid="atlas-twin-depth"]')?.value === String(expected),
        depth,
        { timeout: 10000 },
    );
    await page.waitForTimeout(200);
}

/** Das Overlay ueber den flow()-Kopf oeffnen und warten, bis es gefuellt ist. */
async function openOverlay(page) {
    await page.click('[data-testid="atlas-twin-subject"]');
    await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { timeout: 30000 });
    await page.waitForFunction(
        () => Number(document.querySelector('[data-testid="atlas-flow"]')?.getAttribute('data-arrows') ?? '0') > 0,
        undefined,
        { timeout: 60000 },
    );
}

/** Die Frage wieder aufrufen und einen Modus waehlen. Wortgleich mit smoke-w4e. */
async function openWhyAndChoose(page, intent) {
    await page.click('[data-menu="a-why"]');
    await page.waitForSelector('[data-testid="atlas-why"]', { timeout: 20000 });
    await page.click(`[data-testid="atlas-why-card"][data-intent="${intent}"]`);
}

/** Im Einstiegsdialog suchen und den Treffer mit diesem Namen waehlen. */
async function chooseEntryHit(page, name) {
    await page.waitForSelector('[data-testid="atlas-entry"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-entry-row"]', { timeout: 60000 });
    const input = page.locator('[data-testid="atlas-entry-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(name, { delay: 40 });
    await page.waitForSelector(`[data-testid="atlas-entry-hit"][data-name="${name}"]`, { timeout: 30000 });
    await page.waitForTimeout(600);
    await page.click(`[data-testid="atlas-entry-hit"][data-name="${name}"]`);
    await page.waitForFunction(() => globalThis.__atlasTour?.kind === 'entry', undefined, { timeout: 60000 });
    await page.waitForFunction(
        () => (globalThis.__atlasGalaxy?.hierarchy?.nodes ?? 0) > 0,
        undefined,
        { timeout: 60000 },
    );
    // Die Kamerafahrt und der erste Bildlauf der Szene, damit die
    // Beschriftungen wirklich gesetzt sind, bevor jemand sie misst.
    await page.waitForTimeout(1500);
}

/** Eine Taste ans Fenster geben, ohne dass ein Eingabefeld sie schluckt. */
async function pressGlobally(page, key) {
    await page.click('.atlas-brand');
    await page.keyboard.press(key);
}

async function main() {
    const totalStarted = Date.now();
    let serverChild = null;
    let proxy = null;
    let browser = null;
    let home = null;
    let runtimeDir = null;
    let serverPort = 0;
    let uiPort = 0;
    let failure = null;
    const timings = {};

    const result = {
        overlayOpensAtDepth0: false,
        overlayOpensAtDepth3: false,
        overlayNotOccluded: false,
        lifelines: 0,
        arrowCount: 0,
        selfLoopShown: false,
        groupedSymbols: 0,
        absenceSentences: 0,
        stepperSyncsAll: false,
        escCloses: false,
        darkStyled: false,
        honestyParagraphs: 0,
        /**
         * Ob die drei Herkunftssaetze noch da sind, seit sie nicht mehr als
         * Absaetze unter dem Bild stehen.
         *
         * Die Zahl darueber ist seit W8b eins statt zwei, und das ist die
         * Aenderung selbst und kein Verlust: gekuerzt wurde die WIEDERHOLUNG.
         * Diese Zusicherung ist der Beleg dafuer, und sie ist strenger als die
         * alte Zaehlung, weil sie den Wortlaut prueft und nicht die Menge.
         */
        honestyProvenanceKept: false,
        indexedDotsVisible: 0,
        statusTooltipShown: false,
        legendExplainsGoodCase: false,
        graphPanelVisibleDuringTour: false,
        hierarchyVisibleDuringWalk: false,
        panelNeverScrolledAway: false,
        graphPanelMinHeight: 0,
        hierarchyLabelOverlapsSmallWalk: -1,
        hierarchyLabelOverlapsLargeWalk: -1,
        hierarchyBloomReduced: false,
        legendMaxShare: 1,
        graphMinShare: 0,
        legendDefaultCollapsed: false,
        tabsOverflowScrolls: false,
        tabsNoWrap: false,
        tabsNoOverlap: false,
        activeTabInView: false,
        tabsOpenedForProof: 0,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = { blockedRequests: [], consoleErrors: [], pageErrors: [] };

    try {
        if (!existsSync(BINARY)) {
            throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
        }
        if (!existsSync(FIXTURE)) {
            throw new Error(`Fixture fehlt: ${FIXTURE}`);
        }

        // ------------------------------------------------------------ 1. Bau
        log('npm run build');
        const buildStarted = Date.now();
        const build = await run('npm', ['run', 'build']);
        timings.buildMs = Date.now() - buildStarted;
        if (build.code !== 0) {
            throw new Error(`npm run build endete mit ${build.code}: ${build.out.trim().slice(-600)}`);
        }
        if (!existsSync(join(DIST, 'index.html'))) {
            throw new Error('dist/index.html fehlt nach dem Build');
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w5c-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w5c-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);
        log('Rendezvous:', runtimeDir);

        // -------------------------------------------------------- 3. Index
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // ------------------------------------------------- 4. Server, Proxy
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        timings.serverStartMs = started.durationMs;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        result.port = uiPort;
        extras.serverPort = serverPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        // ------------------------------------------------------- 5. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const context = await browser.newContext({ viewport: { ...MAIN_VIEWPORT } });
        const origin = `http://127.0.0.1:${uiPort}`;

        await context.route('**/*', async (route) => {
            const url = route.request().url();
            if (url.startsWith(origin) || url.startsWith('data:') || url.startsWith('blob:')) {
                await route.continue();
                return;
            }
            extras.blockedRequests.push(url);
            await route.abort();
        });

        const page = await context.newPage();
        page.on('console', (message) => {
            if (message.type() === 'error') {
                extras.consoleErrors.push(message.text());
            }
        });
        page.on('pageerror', (error) => extras.pageErrors.push(String(error)));
        await mkdir(OUT_DIR, { recursive: true });

        await openApp(page, origin);

        // ------------------------- 6a. Die Legende im frischen Browser: zu
        const atStart = await panelSeam(page);
        extras.panelAtStart = atStart;
        result.graphPanelMinHeight = atStart.minHeight;
        result.legendDefaultCollapsed = atStart.legend === null && atStart.legendToggle === 'false';
        log(`Panel: Mindesthoehe ${atStart.minHeight}px, Legende offen=${atStart.legendToggle}, `
            + `Zuklapp-Schalter=${atStart.collapsePresent}`);

        // ------------------------------------- 6b. Die Status-Punkte im Baum
        const rounds = await expandAll(page);
        await page.waitForTimeout(400);
        const dots = await dotsSeam(page);
        extras.dots = dots;
        extras.expandRounds = rounds;
        result.indexedDotsVisible = dots.byState.indexed ?? 0;
        result.statusTooltipShown = dots.total > 0 && dots.titled === dots.total;
        result.legendExplainsGoodCase =
            dots.legendGood !== null
            && dots.legendGood.hasDot === true
            && dots.legendGood.tone === 'indexed'
            && /no recorded issue/.test(dots.legendGood.text);
        log(`Explorer: ${dots.total} Punkte (${JSON.stringify(dots.byState)}), `
            + `${dots.titled} mit Tooltip, Gutfall in der Legende=${result.legendExplainsGoodCase}`);

        // -------------------------------------------- 6c. Die Tab-Leiste
        const files = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid="atlas-tree-row"][data-kind="file"]')]
                .map((row) => row.getAttribute('data-path')));
        extras.treeFiles = files;
        for (const path of files) {
            await page.evaluate((wanted) => {
                const row = [...document.querySelectorAll('[data-testid="atlas-tree-row"]')]
                    .find((candidate) => candidate.getAttribute('data-path') === wanted);
                row?.click();
            }, path);
            await page.waitForTimeout(220);
        }
        await page.waitForTimeout(600);
        // Siehe TABS_VIEWPORT: gemessen wird in einem Fenster, in dem die elf
        // Tabs wirklich mehr Platz brauchen, als die Leiste hat.
        await page.setViewportSize(TABS_VIEWPORT);
        await page.waitForTimeout(500);
        const tabsBefore = await tabsSeam(page);
        extras.tabsViewport = TABS_VIEWPORT;
        result.tabsOpenedForProof = tabsBefore.tabs.length;
        // Das Rad ueber der Leiste bewegt sie waagerecht.
        await page.mouse.move(
            tabsBefore.bar.x + tabsBefore.bar.width / 2,
            tabsBefore.bar.y + tabsBefore.bar.height / 2,
        );
        await page.mouse.wheel(0, 260);
        await page.waitForTimeout(300);
        const tabsAfterWheel = await tabsSeam(page);
        // Und ein Zug mit der Maus tut dasselbe.
        await page.mouse.move(
            tabsAfterWheel.bar.x + tabsAfterWheel.bar.width * 0.6,
            tabsAfterWheel.bar.y + tabsAfterWheel.bar.height / 2,
        );
        await page.mouse.down();
        await page.mouse.move(
            tabsAfterWheel.bar.x + tabsAfterWheel.bar.width * 0.2,
            tabsAfterWheel.bar.y + tabsAfterWheel.bar.height / 2,
            { steps: 8 },
        );
        await page.mouse.up();
        await page.waitForTimeout(300);
        const tabsAfterDrag = await tabsSeam(page);
        extras.tabs = { before: tabsBefore, afterWheel: tabsAfterWheel, afterDrag: tabsAfterDrag };

        const tops = tabsAfterDrag.tabs.map((tab) => Math.round(tab.y));
        result.tabsNoWrap = tabsAfterDrag.tabs.length > 0 && new Set(tops).size === 1;
        result.tabsOverflowScrolls =
            tabsBefore.scrollWidth > tabsBefore.clientWidth + 1
            && tabsAfterWheel.scrollLeft > tabsBefore.scrollLeft
            && tabsAfterDrag.scrollLeft > tabsAfterWheel.scrollLeft
            && tabsAfterWheel.overflow.some((mark) => mark.side === 'left' && mark.on === true);
        let overlapping = false;
        for (let i = 0; i < tabsAfterDrag.tabs.length; i += 1) {
            for (let j = i + 1; j < tabsAfterDrag.tabs.length; j += 1) {
                overlapping = overlapping || rectsOverlap(tabsAfterDrag.tabs[i], tabsAfterDrag.tabs[j]);
            }
        }
        result.tabsNoOverlap =
            !overlapping
            && !rectsOverlap(tabsAfterDrag.bar, tabsAfterDrag.tree)
            && !rectsOverlap(tabsAfterDrag.bar, tabsAfterDrag.twin);
        // Der aktive Tab, in Sicht geholt: die zuletzt geoeffnete Datei.
        await page.evaluate((wanted) => {
            const row = [...document.querySelectorAll('[data-testid="atlas-tree-row"]')]
                .find((candidate) => candidate.getAttribute('data-path') === wanted);
            row?.click();
        }, files[0]);
        await page.waitForTimeout(600);
        const tabsActive = await tabsSeam(page);
        extras.tabsActive = tabsActive;
        const active = tabsActive.tabs.find((tab) => tab.active);
        result.activeTabInView =
            active !== undefined
            && active.x >= tabsActive.bar.x - 1
            && active.x + active.width <= tabsActive.bar.x + tabsActive.bar.width + 1;
        log(`Tabs: ${result.tabsOpenedForProof} offen, scrollWidth ${tabsBefore.scrollWidth} > `
            + `clientWidth ${tabsBefore.clientWidth}, Rad ${tabsBefore.scrollLeft} -> `
            + `${tabsAfterWheel.scrollLeft}, Zug -> ${tabsAfterDrag.scrollLeft}, `
            + `kein Umbruch=${result.tabsNoWrap}, aktiver Tab in Sicht=${result.activeTabInView}`);
        await page.setViewportSize(MAIN_VIEWPORT);
        await page.waitForTimeout(500);

        // ------------------------------------------- 6d. Overlay auf Tiefe 0
        await openSymbol(page, TARGET, TARGET_FILE, 'userService\\.createUser');
        await setDepth(page, 0);
        await openOverlay(page);
        const atDepth0 = await overlaySeam(page);
        extras.atDepth0 = { ...atDepth0, arrows: atDepth0.arrows.length, loops: atDepth0.loops.length };
        result.overlayOpensAtDepth0 =
            atDepth0.present === true
            && atDepth0.diagramPresent === true
            && atDepth0.arrows.length > 0
            && atDepth0.title === `Flow from ${TARGET}`;
        await page.screenshot({ path: join(OUT_DIR, 'flow-overlay-depth0.png'), fullPage: false });
        log(`Tiefe 0: Overlay mit ${atDepth0.arrows.length} Pfeilen, Titel "${atDepth0.title}"`);
        /*
         * Zumachen, und zwar ueber den Knopf, den es dafuer gibt.
         *
         * Bis W8 sass er in der Kopfzeile des Overlays (`atlas-flow-close`).
         * Seit W8 ist der Erklaerer ein Reiter des Erklaeren-Bereichs, und der
         * Bereich hat genau einen Knopf zum Zuklappen; ein zweiter im Reiter
         * waere die Verdopplung, die jener Zyklus abgeschafft hat. Die
         * Zusicherung dieses Halts ist unveraendert und wird unveraendert
         * gemessen: nach dem Klick steht der Erklaerer nicht mehr im Baum.
         */
        await page.click('[data-testid="atlas-explain-collapse"]');
        await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { state: 'detached', timeout: 10000 });

        // ------------------------------------------- 6e. Overlay auf Tiefe 3
        await setDepth(page, 3);
        await openOverlay(page);
        const opened = await overlaySeam(page);
        extras.opened = { ...opened, arrows: opened.arrows.length };
        result.overlayOpensAtDepth3 =
            opened.present === true && opened.diagramPresent === true && opened.arrows.length > 0;
        result.overlayNotOccluded =
            opened.hits.length === 4 && opened.hits.every((hit) => hit === 'overlay');
        result.lifelines = opened.lifelines;
        result.arrowCount = opened.arrows.length;
        result.selfLoopShown =
            opened.loops.length > 0 && opened.loops.every((loop) => /may raise/.test(loop.text));
        result.groupedSymbols = opened.groups.length;
        result.absenceSentences = opened.absences.filter((line) =>
            /^the index recorded no calls, raised errors or environment reads for /.test(line)).length;
        result.darkStyled = opened.luminance < 0.2;
        result.honestyParagraphs = opened.honesty.length;
        result.honestyProvenanceKept =
            /two readings of one walk/.test(opened.provenance)
            && /Derived from the index and nothing else/.test(opened.provenance)
            && /Nobody drew this picture/.test(opened.provenance);
        extras.honestyProvenance = opened.provenance;
        log(`Overlay: ${result.lifelines} Lebenslinien, ${result.arrowCount} Pfeile, `
            + `${opened.loops.length} Schleifen, ${result.groupedSymbols} Gruppen, `
            + `${result.absenceSentences} Absenz-Saetze, Grund ${opened.backgroundColor} `
            + `(Luminanz ${opened.luminance.toFixed(3)}), Treffer ${JSON.stringify(opened.hits)}`);

        // Der Stepper bewegt Diagramm, Liste UND Editor.
        const beforeStep = await readerSeam(page);
        await page.click('[data-testid="atlas-flow-next"]');
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-flow"]')?.getAttribute('data-active-step') === '0',
            undefined,
            { timeout: 10000 },
        );
        await page.waitForTimeout(500);
        const first = await overlaySeam(page);
        const afterFirst = await readerSeam(page);
        await page.click('[data-testid="atlas-flow-next"]');
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-flow"]')?.getAttribute('data-active-step') === '1',
            undefined,
            { timeout: 10000 },
        );
        await page.waitForTimeout(500);
        const second = await overlaySeam(page);
        const afterSecond = await readerSeam(page);
        extras.stepper = {
            beforeStep,
            afterFirst,
            afterSecond,
            first: { activeArrow: first.activeArrow, marked: first.markedSteps, position: first.position },
            second: { activeArrow: second.activeArrow, marked: second.markedSteps, position: second.position },
        };
        const diagramMoved =
            first.activeArrow >= 0
            && second.activeArrow >= 0
            && first.activeArrow !== second.activeArrow
            && first.arrows.some((arrow) => arrow.current && arrow.index === first.activeArrow)
            && second.arrows.some((arrow) => arrow.current && arrow.index === second.activeArrow);
        const listMoved =
            first.markedSteps.length === 1
            && second.markedSteps.length === 1
            && first.markedSteps[0] === 0
            && second.markedSteps[0] === 1
            && first.position === `1 of ${first.steps}`
            && second.position === `2 of ${second.steps}`;
        const editorMoved =
            afterFirst.line > 0
            && afterSecond.line > 0
            && (afterFirst.line !== afterSecond.line || afterFirst.path !== afterSecond.path);
        result.stepperSyncsAll = diagramMoved && listMoved && editorMoved;
        log(`Stepper: Pfeil ${first.activeArrow} -> ${second.activeArrow}, `
            + `Liste ${first.markedSteps} -> ${second.markedSteps}, `
            + `Editor ${afterFirst.path}:${afterFirst.line} -> ${afterSecond.path}:${afterSecond.line}`);

        await page.screenshot({ path: join(OUT_DIR, 'flow-overlay.png'), fullPage: false });
        log('flow-overlay.png geschrieben');

        // Escape schliesst das Overlay.
        await page.keyboard.press('Escape');
        await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { state: 'detached', timeout: 10000 })
            .catch(() => undefined);
        const afterEscape = await overlaySeam(page);
        extras.afterEscape = afterEscape;
        result.escCloses = afterEscape.present === false;
        log(`Escape: Overlay noch da=${afterEscape.present}`);

        // ------------------------------- 6f. Twin-Ueberlaenge, Panel bleibt
        //
        // hotspotScan ist die absichtliche Haeufung der Fixture: drei
        // geschachtelte Schleifen mit vier Aufrufen darin, also der laengste
        // Fakten-Block, den dieses Projekt hat. Genau daran ist der Befund
        // entstanden, dass die rechte Spalte ueberlaeuft.
        await openSymbol(page, LONG_TARGET, 'src/repo/db.ts', 'db\\.hotspotScan');
        await setDepth(page, 3);
        await page.waitForTimeout(600);
        const longPanel = await panelSeam(page);
        extras.panelWithLongTwin = longPanel;
        result.panelNeverScrolledAway =
            longPanel.inViewport === true
            && longPanel.twinBodyScrolls === true
            && longPanel.sideOverflow <= 1;
        log(`Twin-Ueberlaenge: Panel im Viewport=${longPanel.inViewport}, `
            + `Twin scrollt intern=${longPanel.twinBodyScrolls}, `
            + `Spalten-Ueberlauf=${longPanel.sideOverflow}px`);

        // --------------------------------- 6g. Die Fuehrung durchs Projekt
        await openWhyAndChoose(page, 'understand');
        await page.waitForSelector('[data-testid="atlas-tour"]', { timeout: 40000 });
        await page.waitForFunction(() => (globalThis.__atlasTour?.steps ?? 0) > 0, undefined, {
            timeout: 40000,
        });
        await page.waitForTimeout(800);
        const tourStart = await panelSeam(page);
        const graphAtTourStart = await graphSeam(page);
        await pressGlobally(page, 'Enter');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 1, undefined, { timeout: 30000 });
        await page.waitForTimeout(1200);
        const tourStep2 = await panelSeam(page);
        const graphAtStep2 = await graphSeam(page);
        await pressGlobally(page, 'Enter');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 2, undefined, { timeout: 30000 });
        await page.waitForTimeout(1200);
        const tourStep3 = await panelSeam(page);
        const graphAtStep3 = await graphSeam(page);
        extras.tour = {
            start: tourStart,
            step2: tourStep2,
            step3: tourStep3,
            targetChanges: [
                graphAtTourStart?.targetChanges ?? -1,
                graphAtStep2?.targetChanges ?? -1,
                graphAtStep3?.targetChanges ?? -1,
            ],
        };
        result.graphPanelVisibleDuringTour =
            tourStart.inViewport === true
            && tourStep2.inViewport === true
            && tourStep3.inViewport === true
            // Und sie folgt: jeder Schritt setzt ein frisches Kameraziel.
            && (graphAtStep3?.targetChanges ?? 0) > (graphAtTourStart?.targetChanges ?? 0);
        log(`Fuehrung: Panel im Viewport ${tourStart.inViewport}/${tourStep2.inViewport}/`
            + `${tourStep3.inViewport}, Kamerafahrten ${JSON.stringify(extras.tour.targetChanges)}`);

        // ------------------------------------- 6h. Der Entry-Walk, zweimal
        await openWhyAndChoose(page, 'entry');
        await chooseEntryHit(page, LARGE_WALK);
        const walkPanel = await panelSeam(page);
        const largeGraph = await graphSeam(page);
        result.hierarchyVisibleDuringWalk =
            walkPanel.inViewport === true && walkPanel.mode === 'hierarchy';
        result.hierarchyLabelOverlapsLargeWalk = labelOverlaps(largeGraph?.labelBoxes ?? []);
        result.hierarchyBloomReduced = (largeGraph?.bloom ?? 1) <= 0.25;
        extras.largeWalk = {
            nodes: largeGraph?.hierarchyNodes ?? 0,
            depth: largeGraph?.hierarchyDepth ?? 0,
            labels: largeGraph?.labelBoxes.length ?? 0,
            bloom: largeGraph?.bloom ?? null,
            boxes: largeGraph?.labelBoxes ?? [],
        };
        log(`Entry-Walk ${LARGE_WALK}: ${extras.largeWalk.nodes} Knoten, `
            + `${extras.largeWalk.labels} Beschriftungen, `
            + `${result.hierarchyLabelOverlapsLargeWalk} Ueberlappungen, Bloom ${extras.largeWalk.bloom}`);

        // Die Anteile im Panel, mit AUFGEKLAPPTER Legende: das ist der Fall,
        // in dem der Graph seinen Platz verlieren koennte.
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForSelector('[data-testid="atlas-galaxy-legend"]', { timeout: 10000 });
        await page.waitForTimeout(400);
        const withLegend = await panelSeam(page);
        extras.panelWithLegend = withLegend;
        result.legendMaxShare = withLegend.legend === null
            ? 0
            : Number((withLegend.legend.height / withLegend.rect.height).toFixed(4));
        result.graphMinShare = withLegend.scene === null
            ? 0
            : Number((withLegend.scene.height / withLegend.rect.height).toFixed(4));
        log(`Panel-Anteile bei offener Legende: Legende ${result.legendMaxShare}, `
            + `Graph ${result.graphMinShare}`);
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(300);

        // Der kleine Walk: dieselbe Frage an vier Symbole statt an acht.
        await openWhyAndChoose(page, 'entry');
        await chooseEntryHit(page, SMALL_WALK);
        const smallGraph = await graphSeam(page);
        result.hierarchyLabelOverlapsSmallWalk = labelOverlaps(smallGraph?.labelBoxes ?? []);
        extras.smallWalk = {
            nodes: smallGraph?.hierarchyNodes ?? 0,
            depth: smallGraph?.hierarchyDepth ?? 0,
            labels: smallGraph?.labelBoxes.length ?? 0,
            bloom: smallGraph?.bloom ?? null,
            boxes: smallGraph?.labelBoxes ?? [],
        };
        result.hierarchyBloomReduced =
            result.hierarchyBloomReduced && (smallGraph?.bloom ?? 1) <= 0.25;
        log(`Entry-Walk ${SMALL_WALK}: ${extras.smallWalk.nodes} Knoten, `
            + `${extras.smallWalk.labels} Beschriftungen, `
            + `${result.hierarchyLabelOverlapsSmallWalk} Ueberlappungen`);

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w5c] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w5c] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopServer(serverChild);
    await sleep(500);
    const leftovers = [];
    for (const port of [serverPort, uiPort].filter((value) => value > 0)) {
        leftovers.push(await countListeners(port));
    }
    result.leftoverProcesses = leftovers.reduce((sum, value) => sum + value, 0);
    log('leftoverProcesses:', result.leftoverProcesses);

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...result,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            target: TARGET,
            walks: { large: LARGE_WALK, small: SMALL_WALK },
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const ok =
        failure === null
        && result.overlayOpensAtDepth0 === true
        && result.overlayOpensAtDepth3 === true
        && result.overlayNotOccluded === true
        && result.lifelines >= 3
        && result.arrowCount >= 4
        && result.selfLoopShown === true
        && result.groupedSymbols >= 3
        && result.absenceSentences >= 1
        && result.stepperSyncsAll === true
        && result.escCloses === true
        && result.darkStyled === true
        && result.honestyParagraphs === 2
        && result.honestyProvenanceKept === true
        && result.indexedDotsVisible >= 10
        && result.statusTooltipShown === true
        && result.legendExplainsGoodCase === true
        && result.graphPanelVisibleDuringTour === true
        && result.hierarchyVisibleDuringWalk === true
        && result.panelNeverScrolledAway === true
        && result.graphPanelMinHeight >= 280
        && result.hierarchyLabelOverlapsSmallWalk === 0
        && result.hierarchyLabelOverlapsLargeWalk === 0
        && result.hierarchyBloomReduced === true
        && result.legendMaxShare <= 0.4
        && result.graphMinShare >= 0.6
        && result.legendDefaultCollapsed === true
        && result.tabsOverflowScrolls === true
        && result.tabsNoWrap === true
        && result.tabsNoOverlap === true
        && result.activeTabInView === true
        && result.tabsOpenedForProof >= 10
        && result.port >= MIN_PORT
        && result.leftoverProcesses === 0
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w5c] W5c-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w5c] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W5c-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w5c] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
