#!/usr/bin/env node
/*
 * W10b-Smoke: die Griffe erklaeren nichts mehr, die Ansichts-Knoepfe klappen,
 * die Hierarchie waechst aus dem Fokus, und der Graph passt ins Bild.
 *
 * ## Warum dieser Lauf existiert
 *
 * Fuenf Nutzerbefunde vom 2026-08-29 Abend und 2026-08-30 Vormittag. Sie sind
 * einzeln klein und haben dieselbe Wirkung: sie halten jemanden auf, der die
 * Oberflaeche benutzen will.
 *
 *  1. "diese Meldung nicht anzeigen ... Bitte an allen Bordern die Meldung
 *     entfernen." Der vierzeilige Kasten an den vier Zonengriffen.
 *  2. "die beiden Buttons unten links sollten auch aufklappen und zuklappen
 *     koennen." Der Ansichts-Schalter war ein reiner Umschalter.
 *  3. "und hierarchies werden auch nicht angezeigt." Die Projektion hing am
 *     Einstiegs-Spaziergang; ein Symbol im Fokus reichte nicht.
 *  4. "mich nervt, dass die Galaxy manchmal einfach flach ist" und "auch soll
 *     der Graph am besten standardmaessig immer ganz sichtbar sein."
 *  5. Der Vorschau-Server kopiert `dist` beim Start und sieht spaetere Bauten
 *     nie. Das ist kein Produktfehler, sondern ein Loch im Ablauf.
 *
 * ## Was hier gemessen wird und nicht behauptet
 *
 * Der Kern dieses Laufs ist Punkt 4, und er ist eine Aussage ueber ein BILD.
 * Sie wird darum am Bild bewiesen und nicht an der Absicht: die Szene meldet
 * ueber `globalThis.__atlasGalaxyFit`, wo JEDER ihrer Knoten in der
 * Zeichenflaeche liegt (Projektion mit genau der Kamera, die gerade zeichnet;
 * src/galaxy/GraphScene.tsx). Der Lauf liest daran zwei Zahlen: wie viele
 * Knoten ausserhalb liegen (null) und wie viel Rand der aeusserste noch hat
 * (mehr als null Pixel).
 *
 * Die zweite Haelfte von Punkt 4 ist die AUSRICHTUNG, und die wird
 * UNABHAENGIG geprueft: dieser Lauf rechnet die Hauptachsen der geladenen
 * Knotenpositionen selbst (Jacobi, `principalAxes` unten) und haelt die
 * duennste Achse gegen die Blickrichtung, die die Kamera gemeldet hat. Die
 * Rechnung der Oberflaeche und die Rechnung dieses Laufs teilen keine Zeile
 * Code; stimmen sie ueberein, steht die Kamera wirklich senkrecht auf der
 * groessten Flaeche.
 *
 * Gemessen wird ausserdem, dass die Messung ueberhaupt etwas sieht: bevor der
 * Knopf "fit view" gedrueckt wird, verdreht der Lauf die Kamera mit Zug und
 * Rad, bis Knoten aus dem Bild fallen, und schreibt die Zahl auf. Ein
 * Nachweis "null Knoten ausserhalb" von einer Messung, die nie etwas anderes
 * gemeldet hat, waere kein Nachweis.
 *
 * ## Zwei Groessen, und warum die zweite synthetisch ist
 *
 * Die Fixture dieses Projekts hat 76 Knoten. Der Nutzer indiziert daneben sein
 * echtes Projekt: 5000 Knoten, 5209 Kanten, 17 Kantenarten. Die Einpassung muss
 * bei beiden stimmen, also misst dieser Lauf beide. Die grosse Wolke kommt
 * dabei ueber DIESELBE Route (`/api/layout`) in dieselbe Oberflaeche, ihr
 * Inhalt ist aber erzeugt und nicht indiziert: sie traegt genau die Form der
 * echten Antwort (Zahlen oben) und eine gedrehte, in den drei Richtungen
 * verschieden weite Punktwolke. Das steht so im Artefakt, damit niemand die
 * Zahl fuer eine Messung an echtem Code haelt.
 *
 * ## Ablauf
 *
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4640, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *      a. Ruhezustand: kein Tooltip an den Griffen, Tastatur und ARIA da
 *      b. der graue hierarchy-Knopf sagt, was fehlt
 *      c. ein Symbol oeffnen: Hierarchie aus dem Fokus, Kopf nennt die Herkunft
 *      d. der Ansichts-Schalter klappt zu, auf und um, und stimmt mit dem
 *         beschrifteten Schalter daneben ueberein
 *      e. die Einpassung in drei Fenstergroessen und in beiden Ansichten
 *      f. verdrehen, "fit view", "reset layout": jedes Mal wieder alles im Bild
 *      g. dieselbe Messung mit 5000 Knoten
 *      h. der dokumentierte Vorschau-Ablauf, einmal wirklich ausgefuehrt
 *   6. abraeumen, Restprozesse geduldig zaehlen, JSON und drei Bilder
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w10b).
 *
 * ## Ports
 *
 * Ab 4640. 4390/4391 und 4392/4393 gehoeren den beiden Vorschauen des Nutzers,
 * 4141 seinem Modell-Sidecar, 4142 der Agenten-Bruecke, 3001 der Vorschau des
 * Referenzprojekts. Alles darunter gehoert frueheren Laeufen. Dieser Lauf fasst
 * keinen davon an.
 */

import { spawn } from 'node:child_process';
import { existsSync, readdirSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
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
    scrollRegionsToEnd,
} from './lib/readability.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w10b';
const OUT_DIR = join(ROOT, 'verification', 'w10b');
const OUT_JSON = join(OUT_DIR, 'fixes.json');
const SHOT_HANDLES = join(OUT_DIR, 'handles.png');
const SHOT_HIERARCHY = join(OUT_DIR, 'hierarchy-from-focus.png');
const SHOT_COLLAPSED = join(OUT_DIR, 'graph-collapsed.png');

/** Contract-Invariante. Alles darunter gehoert dem Nutzer oder frueheren Laeufen. */
const MIN_PORT = 4640;

/** Das Symbol, ueber das gelaufen wird. Dasselbe wie in W5c, W8, W8b und W9. */
const FOCUS_TARGET = 'createUser';
const FOCUS_FILE = 'src/services/userService.ts';

/**
 * Die drei Fenstergroessen aus AC5.
 *
 * Die erste ist die der uebrigen Beweislaeufe, die zweite ein kleineres
 * Fenster, die dritte ein hohes und schmales. Der Graph sitzt in der rechten
 * Spalte, also aendert jede von ihnen das Seitenverhaeltnis der Zeichenflaeche,
 * und genau daran haengt die Rahmung.
 */
const VIEWPORTS = [
    { width: 1680, height: 1050 },
    { width: 1400, height: 880 },
    { width: 1180, height: 1360 },
];

/** Die Form der zweiten Vorschau des Nutzers, in Zahlen. Siehe Kopf. */
const BIG_NODES = 5000;
const BIG_EDGES = 5209;
const BIG_KINDS = 17;

/** Chromium ohne Aussenwelt, plus die Software-GL-Flags aus smoke-w5c. */
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

const log = (...parts) => console.log('[smoke-w10b]', ...parts);
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
        child.stdout.on('data', (d) => { out += d.toString(); });
        child.stderr.on('data', (d) => { out += d.toString(); });
        child.on('error', (error) => done({ code: 127, out: out + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out }));
    });
}

// ------------------------------------------- die eigene Rechnung des Laufs ---

/**
 * Die Hauptachsen einer Punktwolke, unabhaengig von der Oberflaeche gerechnet.
 *
 * Absichtlich eine zweite Rechnung und keine Wiederverwendung: die Zusicherung
 * aus AC5 lautet, dass die Kamera senkrecht auf der groessten Flaeche steht.
 * Sie mit derselben Funktion zu pruefen, die sie erzeugt hat, hiesse zu
 * pruefen, ob eine Zahl gleich sich selbst ist. Hier steht darum ein eigenes
 * Jacobi-Verfahren; verglichen wird am Ende nur das Ergebnis, also die
 * Richtung, die die Kamera gemeldet hat.
 */
function principalAxes(points) {
    if (points.length === 0) {
        return null;
    }
    const mean = [0, 0, 0];
    for (const p of points) {
        mean[0] += p.x;
        mean[1] += p.y;
        mean[2] += p.z;
    }
    for (let i = 0; i < 3; i += 1) {
        mean[i] /= points.length;
    }
    const c = [[0, 0, 0], [0, 0, 0], [0, 0, 0]];
    for (const p of points) {
        const d = [p.x - mean[0], p.y - mean[1], p.z - mean[2]];
        for (let i = 0; i < 3; i += 1) {
            for (let j = 0; j < 3; j += 1) {
                c[i][j] += d[i] * d[j];
            }
        }
    }
    for (let i = 0; i < 3; i += 1) {
        for (let j = 0; j < 3; j += 1) {
            c[i][j] /= points.length;
        }
    }
    const v = [[1, 0, 0], [0, 1, 0], [0, 0, 1]];
    for (let sweep = 0; sweep < 32; sweep += 1) {
        for (const [p, q] of [[0, 1], [0, 2], [1, 2]]) {
            if (Math.abs(c[p][q]) < 1e-14) {
                continue;
            }
            const theta = (c[q][q] - c[p][p]) / (2 * c[p][q]);
            const t = Math.sign(theta || 1) / (Math.abs(theta) + Math.sqrt(theta * theta + 1));
            const cs = 1 / Math.sqrt(t * t + 1);
            const sn = t * cs;
            for (let k = 0; k < 3; k += 1) {
                const kp = c[k][p];
                const kq = c[k][q];
                c[k][p] = cs * kp - sn * kq;
                c[k][q] = sn * kp + cs * kq;
            }
            for (let k = 0; k < 3; k += 1) {
                const pk = c[p][k];
                const qk = c[q][k];
                c[p][k] = cs * pk - sn * qk;
                c[q][k] = sn * pk + cs * qk;
            }
            for (let k = 0; k < 3; k += 1) {
                const kp = v[k][p];
                const kq = v[k][q];
                v[k][p] = cs * kp - sn * kq;
                v[k][q] = sn * kp + cs * kq;
            }
        }
    }
    // Sortiert wird nach der AUSDEHNUNG und nicht nach der Varianz: ins Bild
    // muss der Kasten. Dieselbe Wahl wie in der Oberflaeche, aber hier aus dem
    // Zweck heraus getroffen und nicht abgeschrieben.
    const axes = [0, 1, 2].map((column) => {
        const axis = [v[0][column], v[1][column], v[2][column]];
        const length = Math.hypot(...axis) || 1;
        const unit = axis.map((value) => value / length);
        let min = Infinity;
        let max = -Infinity;
        for (const p of points) {
            const value = p.x * unit[0] + p.y * unit[1] + p.z * unit[2];
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return { axis: unit, extent: max - min };
    });
    axes.sort((a, b) => b.extent - a.extent);
    return {
        widest: axes[0],
        middle: axes[1],
        thinnest: axes[2],
    };
}

/** Der Betrag des Skalarprodukts zweier Richtungen. 1 heisst: dieselbe Achse. */
const alignment = (a, b) => Math.abs(a[0] * b[0] + a[1] * b[1] + a[2] * b[2]);

/**
 * Eine Layout-Antwort in der Form der echten, mit 5000 Knoten.
 *
 * Deterministisch erzeugt (eigener Zufallsgenerator, fester Startwert), damit
 * zwei Laeufe dieselbe Wolke messen. Die Form ist die einer Galaxie, wie der
 * Server sie rechnet: eine Scheibe, in einer Richtung deutlich duenner als in
 * den beiden anderen, und danach im Raum gedreht, damit die duennste Richtung
 * KEINE Weltachse ist. Genau daran haengt die Zusicherung.
 */
function syntheticLayout() {
    let state = 20260830;
    const random = () => {
        state = (state * 1103515245 + 12345) % 2147483648;
        return state / 2147483648;
    };
    const kinds = [
        'CALLS', 'IMPORTS', 'DEFINES', 'USAGE', 'RAISES', 'RETURNS', 'READS', 'WRITES',
        'EXTENDS', 'IMPLEMENTS', 'DEPENDS', 'CONFIGURES', 'TESTS', 'ROUTES', 'EMITS',
        'HANDLES', 'CONTAINS',
    ].slice(0, BIG_KINDS);
    const nodes = [];
    // Eine feste Drehung, zusammengesetzt aus zwei Winkeln.
    const ca = Math.cos(0.7);
    const sa = Math.sin(0.7);
    const cb = Math.cos(0.4);
    const sb = Math.sin(0.4);
    for (let i = 0; i < BIG_NODES; i += 1) {
        const radius = 1500 * Math.sqrt(random());
        const angle = random() * Math.PI * 2;
        const a = radius * Math.cos(angle);
        const b = radius * 0.62 * Math.sin(angle);
        const g = (random() - 0.5) * 190;
        const x1 = a * ca - b * sa;
        const y1 = a * sa + b * ca;
        const z1 = g;
        nodes.push({
            id: i,
            x: x1,
            y: y1 * cb - z1 * sb,
            z: y1 * sb + z1 * cb,
            label: 'Function',
            name: `symbol${i}`,
            qualified_name: `synthetic.symbol${i}`,
            file_path: `src/generated/part${i % 200}.ts`,
            start_line: 1 + (i % 400),
            end_line: 4 + (i % 400),
            size: 2 + (i % 9),
            color: '#8fa3b0',
        });
    }
    const edges = [];
    for (let i = 0; i < BIG_EDGES; i += 1) {
        edges.push({
            source: Math.floor(random() * BIG_NODES),
            target: Math.floor(random() * BIG_NODES),
            type: kinds[i % kinds.length],
        });
    }
    return { nodes, edges, total_nodes: BIG_NODES };
}

// ------------------------------------------------------------- Testgriffe ---

/** Der Griff des Graph-Panels, ohne die Funktion darin. */
const galaxySeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasGalaxy;
        if (seam === undefined) {
            return null;
        }
        const { clickNode, labelBoxes, hierarchy, ...rest } = seam;
        return JSON.parse(JSON.stringify({
            ...rest,
            labels: labelBoxes?.length ?? 0,
            hierarchy: hierarchy === undefined ? null : {
                root: hierarchy.root,
                rootName: hierarchy.rootName,
                nodes: hierarchy.nodes,
                depth: hierarchy.depth,
                placements: hierarchy.placements,
                edges: hierarchy.edges,
            },
        }));
    });

/** Die Messung der Szene: wo JEDER Knoten im Bild liegt. */
const fitMeasure = (page) =>
    page.evaluate(() => {
        const probe = globalThis.__atlasGalaxyFit;
        return probe === undefined ? null : JSON.parse(JSON.stringify(probe.measure()));
    });

/** Die vier Griffe, samt allem, was an ihnen etwas erklaeren koennte. */
const separators = (page) =>
    page.evaluate(() =>
        [...document.querySelectorAll('[role="separator"]')].map((node) => ({
            testId: node.getAttribute('data-testid') ?? '',
            orientation: node.getAttribute('data-orientation') ?? '',
            label: node.getAttribute('aria-label') ?? '',
            valueText: node.getAttribute('aria-valuetext') ?? '',
            valueNow: node.getAttribute('aria-valuenow') ?? '',
            valueMin: node.getAttribute('aria-valuemin') ?? '',
            valueMax: node.getAttribute('aria-valuemax') ?? '',
            tabIndex: node.tabIndex,
            /* Die drei Wege, auf denen sich hier ein Kasten erklaeren koennte. */
            hint: node.getAttribute('data-hint') ?? '',
            title: node.getAttribute('title') ?? '',
            describedBy: node.getAttribute('aria-describedby') ?? '',
        })));

/** Wie viele eigene Tooltips ueberhaupt im Dokument haengen. */
const hintCount = (page) =>
    page.evaluate(() => document.querySelectorAll('[data-hint]').length);

/** Der Ansichts-Schalter, sein Rahmen und was jeder Chip gerade tut. */
const viewToggle = (page) =>
    page.evaluate(() => {
        const group = document.querySelector('[data-testid="atlas-graph-mode"]');
        if (group === null) {
            return null;
        }
        const panel = document.querySelector('[data-testid="atlas-galaxy"]');
        const fold = document.querySelector('[data-testid="atlas-galaxy-collapse"]');
        return {
            role: group.getAttribute('role') ?? '',
            groupOpen: group.getAttribute('data-open') ?? '',
            visible: panel?.getAttribute('data-visible') ?? '',
            mode: panel?.getAttribute('data-mode') ?? '',
            foldLabel: (fold?.textContent ?? '').replace(/\s+/g, ' ').trim(),
            foldExpanded: fold?.getAttribute('aria-expanded') ?? '',
            chips: [...group.querySelectorAll('[data-testid="atlas-graph-mode-chip"]')].map((chip) => ({
                mode: chip.getAttribute('data-mode') ?? '',
                pressed: chip.getAttribute('aria-pressed') ?? '',
                active: chip.getAttribute('data-active') === 'true',
                action: chip.getAttribute('data-action') ?? '',
                available: chip.getAttribute('data-available') === 'true',
                ariaDisabled: chip.getAttribute('aria-disabled') ?? '',
                hint: chip.getAttribute('data-hint') ?? '',
            })),
        };
    });

/** Der Satz unter dem Bild, so wie er dasteht. */
const noteText = (page) =>
    page.evaluate(() => {
        const node = document.querySelector('[data-testid="atlas-galaxy-note"]');
        return node === null ? '' : (node.textContent ?? '').replace(/\s+/g, ' ').trim();
    });

/** Der Kopf des Panels. */
const headline = (page) =>
    page.evaluate(() => {
        const node = document.querySelector('[data-testid="atlas-galaxy-headline"]');
        return node === null ? '' : (node.textContent ?? '').replace(/\s+/g, ' ').trim();
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
            x: Math.round(rect.x),
            y: Math.round(rect.y),
            width: Math.round(rect.width),
            height: Math.round(rect.height),
        };
    });

/**
 * Die Uhr der Leerlauf-Drehung zuruecksetzen, ohne die Kamera zu bewegen.
 *
 * Wortgleich aus smoke-w9: ein Radereignis mit Weg null setzt `autoRotate`
 * zurueck, und OrbitControls rechnet daraus einen Zoomfaktor von genau 1.
 */
const nudge = (page) =>
    page.evaluate(() => {
        const canvas = document.querySelector('[data-testid="atlas-galaxy-scene"] canvas');
        if (canvas === null) {
            return false;
        }
        canvas.dispatchEvent(new WheelEvent('wheel', { deltaY: 0, bubbles: true, cancelable: true }));
        return true;
    });

/** Wo jeder scrollbare Bereich steht. Wortgleich aus smoke-w9 (Beweisbilder). */
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
        return {
            regions,
            atRest: regions.every((region) => region.top <= 1 && region.left <= 1),
        };
    });

// ------------------------------------------------------------- Klickstrecke -

async function openApp(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 30000 });
    await page.waitForFunction(
        () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
        undefined,
        { timeout: 60000 },
    );
    await page.waitForFunction(
        () => globalThis.__atlasGalaxyFit !== undefined,
        undefined,
        { timeout: 60000 },
    );
    await page.waitForTimeout(700);
}

/** Ein Symbol ueber die Kommandozeile oeffnen. Wortgleich aus smoke-w8b. */
async function openSymbol(page, name, file) {
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
        (expected) => (globalThis.__atlasTwin?.symbol ?? '') === expected,
        name,
        { timeout: 40000 },
    );
    await page.waitForTimeout(500);
}

/** Einen der beiden Ansichts-Chips klicken. */
async function clickChip(page, mode) {
    await page.click(`[data-testid="atlas-graph-mode-chip"][data-mode="${mode}"]`);
    await page.waitForTimeout(450);
}

/**
 * Denselben Chip anklicken, waehrend er `aria-disabled` traegt.
 *
 * Ueber `dispatchEvent` und nicht ueber `page.click`, und der Grund gehoert zur
 * Sache: Playwright haelt ein `aria-disabled="true"` fuer "nicht bedienbar" und
 * wartet, bis das Attribut verschwindet. Ein Browser tut das nicht. Genau darum
 * traegt der Knopf `aria-disabled` und nicht `disabled` (AC3): ein gesperrter
 * Knopf bekaeme keine Zeigerereignisse und koennte sich nicht erklaeren. Was
 * hier gemessen wird, ist also der Weg, den ein Leser wirklich geht.
 */
async function clickDespiteAriaDisabled(page, selector) {
    await page.dispatchEvent(selector, 'click');
    await page.waitForTimeout(450);
}

/**
 * Ein Beweisbild im Ruhezustand.
 *
 * Wie in smoke-w9, plus einer Zeile mehr: der Zeiger geht ZUERST aus dem Weg.
 * Er bleibt nach einem Klick liegen, wo er zuletzt war, und ein Kasten, der
 * dort aufgeht, steht im Bild ueber dem Text, den das Bild beweisen soll. Der
 * erste Anlauf dieses Laufs hatte genau das: der Tooltip des aktiven Chips lag
 * ueber der Kopfzeile, die die Herkunft der Wurzel nennt. Dass keiner mehr
 * offen ist, wird gemessen und steht am Bild.
 */
async function shootAtRest(page, file, name) {
    await closeTooltips(page);
    await page.mouse.move(1, 1);
    await page.waitForTimeout(250);
    await resetScroll(page, READABILITY_EXCLUSIONS);
    await page.waitForTimeout(350);
    const state = await scrollState(page);
    const openHints = await page.evaluate(() =>
        document.querySelectorAll('[data-testid="atlas-hint"]').length);
    await page.screenshot({ path: file, fullPage: false });
    log(`${name}: aufgenommen im Ruhezustand=${state.atRest}, offene Kaesten=${openHints}`);
    return { name, atRest: state.atRest, openHints, regions: state.regions };
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

    const report = {
        /* AC1 */
        splitterTooltips: -1,
        splitterKeyboardStillWorks: false,
        splitterAriaKept: false,
        splitterCount: 0,
        /* AC2 */
        toggleCollapsesActive: false,
        toggleOpensFromCollapsed: false,
        toggleSwitchesView: false,
        toggleAgreesWithLabelledButton: false,
        /* AC3 */
        hierarchyFromFocus: false,
        hierarchyHeadNamesRoot: false,
        hierarchyDisabledExplains: false,
        hierarchyDeterminismUnchanged: false,
        /* AC4 */
        previewFlowDocumented: false,
        previewBundleCheckWorks: false,
        /* AC5 */
        graphFitsOnOpen: false,
        allNodesInsideViewport: false,
        fitMarginPx: -1,
        cameraOnLargestFace: false,
        refitControlWorks: false,
        resetLayoutRefits: false,
        fitMeasuredInSizes: 0,
        fitMeasurements: 0,
        nodesOutsideBeforeRefit: 0,
        bigGraphNodes: 0,
        bigGraphFits: false,
        bigGraphMarginPx: -1,
        /* Ruhe und Lesbarkeit */
        overlapViolations: 0,
        clippingViolations: 0,
        cutWithoutHint: 0,
        screenshotsAtRest: false,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = {
        blockedRequests: [],
        consoleErrors: [],
        pageErrors: [],
        readability: [],
        fits: [],
        shots: [],
        toggle: [],
        separators: [],
    };

    /* Der Schalter fuer die synthetische Layout-Antwort. Siehe Kopf. */
    let bigLayout = null;

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

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w10b-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w10b-run-');
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
        extras.serverPort = serverPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        /*
         * Das echte Layout, unabhaengig von der Oberflaeche geholt.
         *
         * Aus DIESEN Positionen rechnet der Lauf gleich seine eigenen
         * Hauptachsen. Die Oberflaeche liest dieselbe Route, aber der Vergleich
         * darf nicht ueber Zahlen laufen, die sie selbst gemeldet hat.
         */
        const layoutUrl = `http://127.0.0.1:${uiPort}/api/layout?project=${PROJECT}&max_nodes=5000`;
        const layoutResponse = await fetch(layoutUrl, { headers: { Accept: 'application/json' } });
        if (!layoutResponse.ok) {
            throw new Error(`/api/layout antwortete mit HTTP ${layoutResponse.status}`);
        }
        const layout = await layoutResponse.json();
        const layoutPoints = (layout.nodes ?? []).map((node) => ({
            x: Number(node.x), y: Number(node.y), z: Number(node.z),
        }));
        const layoutAxes = principalAxes(layoutPoints);
        extras.layout = {
            url: `/api/layout?project=${PROJECT}&max_nodes=5000`,
            nodes: layoutPoints.length,
            edges: (layout.edges ?? []).length,
            extents: layoutAxes === null ? null : {
                widest: Number(layoutAxes.widest.extent.toFixed(2)),
                middle: Number(layoutAxes.middle.extent.toFixed(2)),
                thinnest: Number(layoutAxes.thinnest.extent.toFixed(2)),
            },
            thinnestAxis: layoutAxes === null ? null : layoutAxes.thinnest.axis,
        };
        log(`/api/layout: ${layoutPoints.length} Knoten, Ausdehnungen `
            + `${JSON.stringify(extras.layout.extents)}`);

        // ------------------------------------------------------- 5. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const context = await browser.newContext({ viewport: { ...VIEWPORTS[0] } });
        const origin = `http://127.0.0.1:${uiPort}`;
        await context.route('**/*', async (route) => {
            const url = route.request().url();
            /*
             * Die synthetische Antwort geht durch DIESELBE Route wie die echte,
             * und nur, solange der Lauf sie eingeschaltet hat. Sie ist erzeugt
             * und nicht indiziert; das steht so im Artefakt.
             */
            if (bigLayout !== null && url.startsWith(`${origin}/api/layout`)) {
                await route.fulfill({
                    status: 200,
                    contentType: 'application/json; charset=utf-8',
                    body: bigLayout,
                });
                return;
            }
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

        const readability = async (name) => {
            const top = await measureReadability(page, READABILITY_EXCLUSIONS);
            const scrolled = await scrollRegionsToEnd(page, READABILITY_EXCLUSIONS);
            await page.waitForTimeout(200);
            const bottom = await measureReadability(page, READABILITY_EXCLUSIONS);
            await resetScroll(page, READABILITY_EXCLUSIONS);
            extras.readability.push({
                name,
                scrolledRegions: scrolled.length,
                top: { candidates: top.candidates, overlaps: top.overlaps, clipped: top.clipped },
                bottom: { candidates: bottom.candidates, overlaps: bottom.overlaps, clipped: bottom.clipped },
            });
            report.overlapViolations += top.overlaps.length + bottom.overlaps.length;
            report.clippingViolations += top.clipped.length + bottom.clipped.length;
            report.cutWithoutHint += [...top.clipped, ...bottom.clipped]
                .filter((entry) => entry.kind === 'cut-without-hint').length;
        };

        /** Einmal messen, aufschreiben und zurueckgeben. */
        const measureFit = async (where, extra = {}) => {
            await nudge(page);
            await page.waitForTimeout(250);
            const seen = await fitMeasure(page);
            const seam = await galaxySeam(page);
            const entry = {
                where,
                ...extra,
                mode: seam?.mode ?? '',
                fits: seam?.fits ?? -1,
                lastFit: seam?.lastFit ?? null,
                measured: seen,
            };
            extras.fits.push(entry);
            report.fitMeasurements += 1;
            log(`Einpassung ${where}: ${seen?.inside ?? -1} von ${seen?.nodes ?? -1} im Bild, `
                + `Rand ${seen?.marginPx ?? -1} px, Fuellung `
                + `${seen?.fill?.horizontal ?? -1}x${seen?.fill?.vertical ?? -1}`);
            return entry;
        };

        await openApp(page, origin);

        // ---------------------------------- 5a. Die Griffe, im Ruhezustand
        const handles = await separators(page);
        extras.separators = handles;
        report.splitterCount = handles.length;
        report.splitterTooltips = handles.filter((handle) =>
            handle.hint.length > 0 || handle.title.length > 0 || handle.describedBy.length > 0).length;
        report.splitterAriaKept = handles.length === 4
            && handles.every((handle) =>
                handle.label.length > 0
                && /\d+\s*pixels/.test(handle.valueText)
                && handle.valueNow.length > 0
                && handle.valueMin.length > 0
                && handle.valueMax.length > 0);
        extras.hintsInDocument = await hintCount(page);
        log(`Griffe: ${handles.length}, davon mit Kasten: ${report.splitterTooltips}, `
            + `ARIA vollstaendig: ${report.splitterAriaKept}, `
            + `Tooltips im Dokument insgesamt: ${extras.hintsInDocument}`);

        /*
         * Die Faehigkeit, die NICHT entfernt wurde.
         *
         * Gemessen an der Zahl, die der Griff selbst meldet, und an der Breite,
         * die die Zone danach wirklich hat: ein `aria-valuenow`, das sich
         * bewegt, waehrend nichts breiter wird, waere eine Anzeige und keine
         * Bedienung.
         */
        const widthOfExplorer = () => page.evaluate(() => {
            const node = document.querySelector('[data-testid="atlas-tree"]');
            return node === null ? -1 : Math.round(node.getBoundingClientRect().width);
        });
        const valueOf = () => page.evaluate(() => Number(
            document.querySelector('[data-testid="atlas-split-left"]')?.getAttribute('aria-valuenow') ?? '-1'));
        await page.focus('[data-testid="atlas-split-left"]');
        const keyBefore = { value: await valueOf(), width: await widthOfExplorer() };
        await page.keyboard.press('ArrowRight');
        await page.waitForTimeout(160);
        const afterStep = { value: await valueOf(), width: await widthOfExplorer() };
        await page.keyboard.press('Shift+ArrowRight');
        await page.waitForTimeout(160);
        const afterBigStep = { value: await valueOf(), width: await widthOfExplorer() };
        await page.keyboard.press('ArrowLeft');
        await page.keyboard.press('Shift+ArrowLeft');
        await page.waitForTimeout(200);
        const afterBack = { value: await valueOf(), width: await widthOfExplorer() };
        extras.keyboard = { keyBefore, afterStep, afterBigStep, afterBack };
        report.splitterKeyboardStillWorks =
            afterStep.value > keyBefore.value
            && afterBigStep.value - afterStep.value > afterStep.value - keyBefore.value
            && afterStep.width > keyBefore.width
            && afterBack.value === keyBefore.value;
        log(`Tastatur am Griff: ${JSON.stringify(extras.keyboard)} -> `
            + `${report.splitterKeyboardStillWorks}`);

        await readability('Ruhezustand, Griffe ohne Kasten');
        extras.shots.push(await shootAtRest(page, SHOT_HANDLES, 'handles.png'));

        // --------------------------- 5b. Der graue Knopf sagt, was ihm fehlt
        const beforeFocus = await viewToggle(page);
        extras.toggle.push({ where: 'ohne Fokus', state: beforeFocus });
        const hierarchyChipBefore = beforeFocus?.chips.find((chip) => chip.mode === 'hierarchy');
        await clickDespiteAriaDisabled(
            page,
            '[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]',
        );
        const noteAfterDisabledClick = await noteText(page);
        const stillGalaxy = (await viewToggle(page))?.mode === 'galaxy';
        report.hierarchyDisabledExplains =
            hierarchyChipBefore !== undefined
            && hierarchyChipBefore.ariaDisabled === 'true'
            && hierarchyChipBefore.available === false
            && /open a symbol|pick a way in/i.test(hierarchyChipBefore.hint)
            && /open a symbol|pick a way in/i.test(noteAfterDisabledClick)
            && stillGalaxy;
        extras.disabledChip = { chip: hierarchyChipBefore, note: noteAfterDisabledClick, stillGalaxy };
        log(`grauer hierarchy-Knopf erklaert sich: ${report.hierarchyDisabledExplains} `
            + `("${noteAfterDisabledClick}")`);

        // ----------------------- 5c. Die Einpassung beim Oeffnen, drei Groessen
        const sizesSeen = new Set();
        const openFits = [];
        for (const viewport of VIEWPORTS) {
            await page.setViewportSize(viewport);
            await page.waitForTimeout(900);
            const entry = await measureFit(`galaxy ${viewport.width}x${viewport.height}`, {
                viewport,
                view: 'galaxy',
                atOpen: true,
            });
            openFits.push(entry);
            sizesSeen.add(`${viewport.width}x${viewport.height}`);
        }
        await page.setViewportSize(VIEWPORTS[0]);
        await page.waitForTimeout(700);

        /*
         * Die Ausrichtung, gegen die EIGENE Rechnung des Laufs gehalten.
         *
         * `direction` ist die Blickrichtung, die die Szene gemeldet hat;
         * `layoutAxes.thinnest` ist die duennste Achse der Wolke, aus den
         * Positionen der Layout-Antwort selbst gerechnet. Stimmen sie ueberein,
         * sieht die Kamera die duenne Richtung entlang, also auf die groesste
         * Flaeche.
         */
        const alignments = openFits.map((entry) => {
            const direction = entry.measured?.camera?.direction ?? [0, 0, 0];
            return {
                where: entry.where,
                toThinnest: layoutAxes === null ? -1
                    : Number(alignment(direction, layoutAxes.thinnest.axis).toFixed(4)),
                toWidest: layoutAxes === null ? -1
                    : Number(alignment(direction, layoutAxes.widest.axis).toFixed(4)),
            };
        });
        extras.alignment = { measured: alignments, method: 'eigene Jacobi-Rechnung, siehe principalAxes' };
        report.cameraOnLargestFace = alignments.length === VIEWPORTS.length
            && alignments.every((entry) => entry.toThinnest > 0.99 && entry.toWidest < 0.1);
        log(`Kamera auf der groessten Flaeche: ${report.cameraOnLargestFace} `
            + `(${JSON.stringify(alignments)})`);

        // -------------------------- 5d. Die Hierarchie aus dem Symbol im Fokus
        await openSymbol(page, FOCUS_TARGET, FOCUS_FILE);
        await page.waitForFunction(
            () => globalThis.__atlasGalaxy?.hierarchyAvailable === true,
            undefined,
            { timeout: 60000 },
        );
        const withFocus = await viewToggle(page);
        extras.toggle.push({ where: 'mit Fokus, vor dem Umschalten', state: withFocus });
        const seamWithFocus = await galaxySeam(page);
        // Ein Fokus macht die Hierarchie moeglich und waehlt sie nicht.
        const stayedInGalaxy = seamWithFocus?.mode === 'galaxy';
        await clickChip(page, 'hierarchy');
        await page.waitForTimeout(900);
        const hierarchySeam = await galaxySeam(page);
        const hierarchyHead = await headline(page);
        report.hierarchyFromFocus =
            stayedInGalaxy
            && hierarchySeam?.mode === 'hierarchy'
            && hierarchySeam?.hierarchyOrigin === 'focus'
            && (hierarchySeam?.hierarchy?.nodes ?? 0) > 1
            && hierarchySeam?.hierarchy?.rootName === FOCUS_TARGET;
        /*
         * Gemessen wird nicht nur, dass der Satz die Herkunft NENNT, sondern
         * auch, dass sie in der Spalte zu LESEN ist: diese Zeile bricht nicht
         * um und endet in einem Auslassungszeichen. Ein Kopf, der die Auskunft
         * hinter der Kante traegt, hat sie nicht gegeben.
         */
        const headClipped = await page.evaluate(() => {
            const node = document.querySelector('[data-testid="atlas-galaxy-headline"]');
            if (node === null) {
                return { clipped: true, visibleChars: 0 };
            }
            const style = globalThis.getComputedStyle(node);
            const perChar = node.scrollWidth / Math.max(1, (node.textContent ?? '').length);
            return {
                clipped: node.scrollWidth > node.clientWidth + 1,
                visibleChars: Math.floor(node.clientWidth / Math.max(1, perChar)),
                fontSize: style.fontSize,
            };
        });
        extras.headline = { text: hierarchyHead, ...headClipped };
        report.hierarchyHeadNamesRoot =
            /hierarchy of createUser \(in focus\)/.test(hierarchyHead)
            && hierarchyHead.indexOf('in focus') < headClipped.visibleChars;
        extras.hierarchy = {
            stayedInGalaxy,
            origin: hierarchySeam?.hierarchyOrigin ?? '',
            headline: hierarchyHead,
            nodes: hierarchySeam?.hierarchy?.nodes ?? 0,
            depth: hierarchySeam?.hierarchy?.depth ?? 0,
            root: hierarchySeam?.hierarchy?.root ?? '',
            placements: hierarchySeam?.hierarchy?.placements ?? [],
        };
        log(`Hierarchie aus dem Fokus: ${report.hierarchyFromFocus}, Kopf nennt die Herkunft: `
            + `${report.hierarchyHeadNamesRoot} ("${hierarchyHead}")`);

        /*
         * Der Determinismus der Projektion, an denselben drei Fragen wie in
         * W9: die Spalte ist der Hop mal einer festen Breite, in einer Spalte
         * stehen die Namen ordinal sortiert, und jedes Symbol jenseits der
         * Wurzel wird von einer Walk-Kante aus der Spalte davor erreicht.
         * Dazu die vierte: zweimal dasselbe Bild ergibt zweimal dieselben Orte.
         */
        const placements = extras.hierarchy.placements;
        const walkEdges = hierarchySeam?.hierarchy?.edges ?? [];
        const hopOf = new Map(placements.map((placement) => [placement.key, placement.hop]));
        const columnWidth = placements.find((placement) => placement.hop === 1)?.x ?? 0;
        const columnsOk = columnWidth > 0
            && placements.every((placement) => placement.x === placement.hop * columnWidth);
        const sortedOk = [...new Set(placements.map((placement) => placement.hop))].every((hop) => {
            const level = placements.filter((placement) => placement.hop === hop);
            const names = level.map((placement) => `${placement.name} ${placement.key}`);
            return JSON.stringify(names) === JSON.stringify([...names].sort());
        });
        const reachedByWalk = placements.every((placement) => {
            if (placement.hop === 0) {
                return true;
            }
            return walkEdges.some((edge) =>
                edge.to === placement.key && (hopOf.get(edge.from) ?? -1) === placement.hop - 1);
        });
        await clickChip(page, 'galaxy');
        await clickChip(page, 'hierarchy');
        await page.waitForTimeout(700);
        const again = await galaxySeam(page);
        const sameTwice = JSON.stringify(again?.hierarchy?.placements ?? [])
            === JSON.stringify(placements);
        report.hierarchyDeterminismUnchanged =
            placements.length > 1 && columnsOk && sortedOk && reachedByWalk && sameTwice;
        extras.determinism = { columnWidth, columnsOk, sortedOk, reachedByWalk, sameTwice };
        log(`Determinismus der Projektion: ${report.hierarchyDeterminismUnchanged} `
            + `${JSON.stringify(extras.determinism)}`);

        // Die Einpassung in der zweiten Ansicht, wieder in drei Groessen.
        for (const viewport of VIEWPORTS) {
            await page.setViewportSize(viewport);
            await page.waitForTimeout(900);
            await measureFit(`hierarchy ${viewport.width}x${viewport.height}`, {
                viewport,
                view: 'hierarchy',
                atOpen: true,
            });
            sizesSeen.add(`${viewport.width}x${viewport.height}`);
        }
        await page.setViewportSize(VIEWPORTS[0]);
        await page.waitForTimeout(700);
        report.fitMeasuredInSizes = sizesSeen.size;

        await readability('Hierarchie aus dem Fokus');
        extras.shots.push(await shootAtRest(page, SHOT_HIERARCHY, 'hierarchy-from-focus.png'));

        // -------------------------- 5e. Der Ansichts-Schalter klappt (AC2)
        const stateBeforeCollapse = await viewToggle(page);
        await clickChip(page, 'hierarchy');
        const collapsedByChip = await viewToggle(page);
        report.toggleCollapsesActive =
            stateBeforeCollapse?.visible === 'true'
            && collapsedByChip?.visible === 'false'
            && collapsedByChip?.mode === 'hierarchy';
        extras.toggle.push({ where: 'nach Klick auf den aktiven Chip', state: collapsedByChip });
        log(`aktiver Chip klappt zu: ${report.toggleCollapsesActive}`);

        await readability('Graph zugeklappt');
        extras.shots.push(await shootAtRest(page, SHOT_COLLAPSED, 'graph-collapsed.png'));

        await clickChip(page, 'galaxy');
        const openedByChip = await viewToggle(page);
        report.toggleOpensFromCollapsed =
            openedByChip?.visible === 'true' && openedByChip?.mode === 'galaxy';
        extras.toggle.push({ where: 'nach Klick bei zugeklappter Sektion', state: openedByChip });
        log(`Chip klappt auf und waehlt: ${report.toggleOpensFromCollapsed}`);

        await clickChip(page, 'hierarchy');
        const switched = await viewToggle(page);
        report.toggleSwitchesView =
            switched?.visible === 'true'
            && switched?.mode === 'hierarchy'
            && switched.chips.filter((chip) => chip.pressed === 'true').length === 1;
        extras.toggle.push({ where: 'nach Klick auf den anderen Chip', state: switched });
        log(`der andere Chip wechselt: ${report.toggleSwitchesView}`);

        /*
         * Beide Wege, derselbe Zustand.
         *
         * Erst ueber den beschrifteten Schalter zu und wieder auf, dann ueber
         * den Chip zu und wieder auf. Verglichen wird, was danach dasteht:
         * dieselbe Sichtbarkeit, dieselbe Ansicht, dieselbe Beschriftung am
         * Schalter. Zwei Wege in eine Lage, die sich unterscheidet, waeren zwei
         * Wahrheiten ueber denselben Zustand.
         */
        await page.click('[data-testid="atlas-galaxy-collapse"]');
        await page.waitForTimeout(400);
        const closedByLabel = await viewToggle(page);
        await page.click('[data-testid="atlas-galaxy-collapse"]');
        await page.waitForTimeout(500);
        const openedByLabel = await viewToggle(page);
        await clickChip(page, 'hierarchy');
        const closedByChip = await viewToggle(page);
        await clickChip(page, 'hierarchy');
        const openedAgainByChip = await viewToggle(page);
        const sameState = (a, b) => a !== null && b !== null
            && a.visible === b.visible && a.mode === b.mode
            && a.foldLabel === b.foldLabel && a.foldExpanded === b.foldExpanded
            && JSON.stringify(a.chips.map((chip) => [chip.mode, chip.pressed, chip.action]))
                === JSON.stringify(b.chips.map((chip) => [chip.mode, chip.pressed, chip.action]));
        report.toggleAgreesWithLabelledButton =
            sameState(closedByLabel, closedByChip) && sameState(openedByLabel, openedAgainByChip);
        extras.toggle.push({ where: 'zwei Wege', closedByLabel, closedByChip, openedByLabel, openedAgainByChip });
        log(`beide Wege ergeben denselben Zustand: ${report.toggleAgreesWithLabelledButton}`);

        // --------------------------- 5f. Verdrehen, "fit view", "reset layout"
        await clickChip(page, 'galaxy');
        await page.waitForTimeout(700);
        await measureFit('galaxy nach dem Aufklappen', { view: 'galaxy' });

        const rect = await sceneRect(page);
        extras.sceneRect = rect;
        if (rect === null) {
            throw new Error('die Zeichenflaeche hat kein Rechteck');
        }
        const cx = rect.x + Math.round(rect.width / 2);
        const cy = rect.y + Math.round(rect.height / 2);
        await page.mouse.move(cx, cy);
        await page.mouse.down();
        await page.mouse.move(cx + 260, cy - 190, { steps: 14 });
        await page.mouse.up();
        await page.waitForTimeout(600);
        /*
         * Ein Radereignis ist EIN Schritt, egal wie gross sein Weg ist
         * (OrbitControls fragt nur nach dem Vorzeichen). Also zwoelf davon:
         * jeder holt die Kamera um rund sieben Prozent naeher, zusammen auf gut
         * die Haelfte, und damit liegt die Wolke sicher ausserhalb des Bildes.
         */
        await page.mouse.move(cx, cy);
        for (let notch = 0; notch < 12; notch += 1) {
            await page.mouse.wheel(0, -120);
            await page.waitForTimeout(60);
        }
        await page.waitForTimeout(1400);
        const turned = await measureFit('nach dem Verdrehen und Heranzoomen', {
            view: 'galaxy',
            turned: true,
        });
        report.nodesOutsideBeforeRefit = turned.measured?.outside ?? 0;

        await page.click('[data-testid="atlas-galaxy-fit"]');
        await page.waitForTimeout(900);
        const refitted = await measureFit('nach "fit view"', { view: 'galaxy' });
        report.refitControlWorks =
            report.nodesOutsideBeforeRefit > 0
            && (refitted.measured?.outside ?? -1) === 0
            && (refitted.measured?.marginPx ?? -1) > 0;
        log(`vor der Einpassung ausserhalb: ${report.nodesOutsideBeforeRefit}, `
            + `Knopf bringt zurueck: ${report.refitControlWorks}`);

        /* Und derselbe Weg ueber "reset layout" (alt+r). */
        await page.mouse.move(cx, cy);
        await page.mouse.down();
        await page.mouse.move(cx - 300, cy + 160, { steps: 12 });
        await page.mouse.up();
        await page.waitForTimeout(500);
        // Zurueck in die Mitte der Szene, BEVOR gescrollt wird: ein Radereignis
        // geht an das Element unter dem Zeiger, und der stand nach dem Zug am
        // Rand der Zeichenflaeche.
        await page.mouse.move(cx, cy);
        for (let notch = 0; notch < 12; notch += 1) {
            await page.mouse.wheel(0, -120);
            await page.waitForTimeout(60);
        }
        await page.waitForTimeout(1200);
        const turnedAgain = await measureFit('vor "reset layout"', { view: 'galaxy', turned: true });
        await page.keyboard.press('Alt+r');
        await page.waitForTimeout(1100);
        const afterReset = await measureFit('nach "reset layout"', { view: 'galaxy' });
        /*
         * Zwei Arten, die Ansicht zu verlieren, und beide zaehlen.
         *
         * Ein Zug kann Knoten aus dem Bild schieben, und er kann die Kamera in
         * die duenne Richtung drehen: dann ist alles noch im Bild und trotzdem
         * nichts mehr zu sehen ("mich nervt, dass die Galaxy manchmal einfach
         * flach ist"). Der Nachweis verlangt darum, dass die Ansicht vorher in
         * EINER der beiden Weisen verloren war und danach in BEIDEN wieder da
         * ist: jeder Knoten im Bild UND die Kamera wieder senkrecht auf der
         * groessten Flaeche.
         */
        const facing = (entry) => (layoutAxes === null ? -1 : Number(alignment(
            entry.measured?.camera?.direction ?? [0, 0, 0],
            layoutAxes.thinnest.axis,
        ).toFixed(4)));
        extras.reset = {
            outsideBefore: turnedAgain.measured?.outside ?? -1,
            facingBefore: facing(turnedAgain),
            fillBefore: turnedAgain.measured?.fill ?? null,
            outsideAfter: afterReset.measured?.outside ?? -1,
            facingAfter: facing(afterReset),
            fillAfter: afterReset.measured?.fill ?? null,
        };
        report.resetLayoutRefits =
            ((turnedAgain.measured?.outside ?? 0) > 0 || extras.reset.facingBefore < 0.9)
            && (afterReset.measured?.outside ?? -1) === 0
            && extras.reset.facingAfter > 0.99;
        log(`"reset layout" passt wieder ein: ${report.resetLayoutRefits} `
            + `${JSON.stringify(extras.reset)}`);

        // ------------------------------ 5g. Dieselbe Messung mit 5000 Knoten
        bigLayout = JSON.stringify(syntheticLayout());
        await openApp(page, origin);
        await page.waitForTimeout(1200);
        const bigSeam = await galaxySeam(page);
        const bigFit = await measureFit('galaxy mit 5000 Knoten (synthetisch)', {
            view: 'galaxy',
            synthetic: true,
        });
        report.bigGraphNodes = bigFit.measured?.nodes ?? 0;
        report.bigGraphMarginPx = bigFit.measured?.marginPx ?? -1;
        report.bigGraphFits =
            report.bigGraphNodes >= BIG_NODES
            && (bigFit.measured?.outside ?? -1) === 0
            && (bigFit.measured?.behind ?? -1) === 0
            && report.bigGraphMarginPx > 0;
        extras.bigGraph = {
            note:
                'Die Antwort ist ERZEUGT und nicht indiziert: 5000 Knoten, 5209 Kanten, 17 '
                + 'Kantenarten, eine gedrehte Scheibe. Sie kommt ueber dieselbe Route in dieselbe '
                + 'Oberflaeche wie eine echte Antwort. Gemessen wird die Einpassung, nicht der Index.',
            nodes: BIG_NODES,
            edges: BIG_EDGES,
            kinds: BIG_KINDS,
            seamNodes: bigSeam?.nodes ?? 0,
            lastFit: bigFit.lastFit,
        };
        const bigAxes = principalAxes(JSON.parse(bigLayout).nodes);
        const bigDirection = bigFit.measured?.camera?.direction ?? [0, 0, 0];
        extras.bigGraph.alignmentToThinnest = bigAxes === null ? -1
            : Number(alignment(bigDirection, bigAxes.thinnest.axis).toFixed(4));
        log(`5000 Knoten: ${report.bigGraphNodes} gemessen, ausserhalb `
            + `${bigFit.measured?.outside ?? -1}, Rand ${report.bigGraphMarginPx} px, `
            + `Ausrichtung ${extras.bigGraph.alignmentToThinnest}`);
        await readability('5000 Knoten, eingepasst');
        bigLayout = null;

        // ---------------------------------- Die Bilanz ueber alle Messungen
        const graphFits = extras.fits.filter((entry) => entry.atOpen === true);
        report.graphFitsOnOpen = graphFits.length >= 6
            && graphFits.every((entry) => (entry.measured?.outside ?? -1) === 0
                && (entry.measured?.behind ?? -1) === 0);
        /*
         * Alles, was NICHT absichtlich verdreht wurde. Die beiden verdrehten
         * Messungen sind der Gegenbeweis (die Messung sieht etwas) und gehoeren
         * darum nicht in die Bilanz.
         */
        const allFits = extras.fits.filter((entry) => entry.turned !== true);
        report.allNodesInsideViewport = allFits.length >= 9 && allFits.every((entry) =>
            (entry.measured?.outside ?? -1) === 0 && (entry.measured?.nodes ?? 0) > 0);
        report.fitMarginPx = Number(Math.min(...allFits.map((entry) =>
            entry.measured?.marginPx ?? -1)).toFixed(2));
        log(`alle Knoten im Bild: ${report.allNodesInsideViewport}, kleinster Rand `
            + `${report.fitMarginPx} px ueber ${allFits.length} Messungen`);

        // ------------------------- 5h. Der Vorschau-Ablauf, wirklich gefahren
        const readme = await readFile(join(ROOT, 'README.md'), 'utf8');
        const documented =
            /dist/.test(readme)
            && /(neu ?starten|restart)/i.test(readme)
            && /index-/.test(readme)
            && /kopiert/i.test(readme);
        /*
         * Und die Probe aufs Exempel: der dokumentierte Vergleich, hier
         * ausgefuehrt. Dieser Auslieferer bedient dist/ direkt, also MUESSEN die
         * beiden Kennungen uebereinstimmen; taeten sie es nicht, waere entweder
         * der Bau oder die Anleitung falsch.
         */
        const builtBundle = readdirSync(join(DIST, 'assets'))
            .filter((name) => /^index-[A-Za-z0-9_-]+\.js$/.test(name))
            .sort()[0] ?? '';
        const servedHtml = await (await fetch(`${origin}/`)).text();
        const servedBundle = (servedHtml.match(/index-[A-Za-z0-9_-]+\.js/) ?? [''])[0];
        report.previewBundleCheckWorks = builtBundle.length > 0 && builtBundle === servedBundle;
        report.previewFlowDocumented = documented && report.previewBundleCheckWorks;
        extras.preview = { builtBundle, servedBundle, documented };
        log(`Vorschau-Ablauf dokumentiert: ${documented}, Kennung gebaut ${builtBundle} `
            + `= ausgeliefert ${servedBundle}`);

        report.screenshotsAtRest =
            extras.shots.length === 3
            && extras.shots.every((shot) => shot.atRest === true && shot.openHints === 0);
        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w10b] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w10b] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopServer(serverChild);
    await sleep(600);

    /*
     * Gezaehlt wird mit Geduld, und die Geduld hat eine Grenze.
     *
     * Dasselbe Muster wie in smoke-w5b und smoke-w6-full: das Betriebssystem
     * gibt einen Port erst nach dem Ende des Prozesses frei, gemessen bis zu
     * 1557 ms. Ein einziger Blick faellt genau in dieses Fenster, und der Lauf
     * wuerde rot, obwohl nichts mehr laeuft. Wie lange gewartet wurde, steht im
     * Artefakt, damit ein langsamer werdender Abbau auffaellt.
     */
    const FREE_PORT_TIMEOUT_MS = 5000;
    const leftovers = [];
    for (const port of [serverPort, uiPort].filter((value) => value > 0)) {
        const startedAt = Date.now();
        let listeners = await countListeners(port);
        while (listeners > 0 && Date.now() - startedAt < FREE_PORT_TIMEOUT_MS) {
            await sleep(250);
            listeners = await countListeners(port);
        }
        leftovers.push({ port, listeners, waitedMs: Date.now() - startedAt });
    }
    extras.leftovers = leftovers;
    report.leftoverProcesses = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
    log('leftoverProcesses:', report.leftoverProcesses, JSON.stringify(leftovers));

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            focusTarget: FOCUS_TARGET,
            viewports: VIEWPORTS,
            method:
                'Die Einpassung wird am BILD gemessen: die Szene projiziert jeden Knoten mit genau '
                + 'der Kamera, die gerade zeichnet, in die Zeichenflaeche (globalThis.__atlasGalaxyFit '
                + 'in src/galaxy/GraphScene.tsx). Gezaehlt werden die Knoten ausserhalb und der '
                + 'kleinste verbleibende Rand in Pixeln. Die AUSRICHTUNG wird unabhaengig geprueft: '
                + 'dieser Lauf rechnet die Hauptachsen der geladenen Positionen selbst (Jacobi, '
                + 'principalAxes in dieser Datei) und haelt die duennste Achse gegen die gemeldete '
                + 'Blickrichtung. Dass die Messung ueberhaupt etwas sieht, wird vorher gezeigt: die '
                + 'Kamera wird mit Zug und Rad verdreht, bis Knoten aus dem Bild fallen.',
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = [SHOT_HANDLES, SHOT_HIERARCHY, SHOT_COLLAPSED].every((file) => existsSync(file));
    const ok =
        failure === null
        && report.splitterCount === 4
        && report.splitterTooltips === 0
        && report.splitterKeyboardStillWorks === true
        && report.splitterAriaKept === true
        && report.toggleCollapsesActive === true
        && report.toggleOpensFromCollapsed === true
        && report.toggleSwitchesView === true
        && report.toggleAgreesWithLabelledButton === true
        && report.hierarchyFromFocus === true
        && report.hierarchyHeadNamesRoot === true
        && report.hierarchyDisabledExplains === true
        && report.hierarchyDeterminismUnchanged === true
        && report.previewFlowDocumented === true
        && report.previewBundleCheckWorks === true
        && report.graphFitsOnOpen === true
        && report.allNodesInsideViewport === true
        && report.fitMarginPx > 0
        && report.cameraOnLargestFace === true
        && report.refitControlWorks === true
        && report.resetLayoutRefits === true
        && report.fitMeasuredInSizes >= 3
        && report.bigGraphFits === true
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.cutWithoutHint === 0
        && report.screenshotsAtRest === true
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && shotsOk
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w10b] W10b-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w10b] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W10b-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w10b] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
