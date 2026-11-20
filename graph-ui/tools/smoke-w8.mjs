#!/usr/bin/env node
/*
 * W8-Smoke: feste Plaetze statt gestapelter Fenster, und der Leser zieht die
 * Grenzen.
 *
 * ## Der Befund, den dieser Lauf beweist
 *
 * Nutzerbefund vom 2026-08-29 mit Screenshot: Flow-Erklaerer, Antwort-Panel und
 * Schrittkarte standen gleichzeitig offen, ueberlagerten sich, und alle drei
 * waren angeschnitten. Der Massstab dazu kam vom Nutzer selbst: "es soll
 * intuitiv sein wie Apple und nicht verwirren; die Oberflaeche ist
 * hauptsaechlich dafuer da, dass Devs Code besser verstehen".
 *
 * Gemessen wird darum nicht, dass es die Reiter GIBT, sondern dass der Zustand,
 * den der Befund beschreibt, nicht mehr herstellbar ist: der Lauf oeffnet
 * nacheinander alles, was frueher uebereinander lag, und zaehlt jedes Mal, wie
 * viele Inhalte gleichzeitig im Baum stehen.
 *
 * ## Warum dieser Lauf ohne Modell auskommt
 *
 * Fuer AC5 braucht er einen Chat-Verlauf. Ein Zug entsteht auch OHNE laufendes
 * Modell: eine Frage bei ausgeschaltetem Modell landet als abgelehnter Zug im
 * Verlauf, mit ihrem Grund darin. Das ist keine Notloesung, sondern genau die
 * Ehrlichkeitsregel, die W5a fuer diesen Fall gebaut hat, und sie ist hier
 * dienlich: der Lauf belegt den Zustand des Chats, ohne den Sidecar des Nutzers
 * auf 4141 anzufassen und ohne ein eigenes Modell zu starten.
 *
 * ## Was hier NICHT gemessen wird und warum
 *
 * Der Zug mit der Maus wird gefahren und nicht nachgestellt: `mouse.down`,
 * `mouse.move`, `mouse.up` auf dem Griff. Was dabei zaehlt, ist nicht die Zahl
 * im Griff, sondern das RECHTECK der Zone daneben: eine Zahl, die sich aendert,
 * ohne dass sich etwas bewegt, waere ein Griff, der nur so tut.
 *
 * ## Ablauf
 *
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (nur gelesen)
 *   4. C-Server auf einem freien Port >= 4440, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt
 *      a. Ruhezustand: eingeklappter Erklaeren-Bereich, Zonen ohne Ueberlagerung
 *      b. jeder leere Reiter sagt, warum er leer ist
 *      c. Zustand aufbauen: Frage, Fuehrung, Flow-Schritte
 *      d. Reiterwechsel: immer genau ein Inhalt, und keiner verliert etwas
 *      e. Escape, Verlauf, clear: was W7c fuer den Chat erkaempft hat
 *      f. die vier Griffe: Maus, Tastatur, Doppelklick, Anschlaege
 *      g. Reload: die gezogenen Masse stehen noch
 *      h. der Weg zurueck, ueber die Zeile und ueber das Menue
 *      i. die beiden Extremlagen, jeweils mit Lesbarkeitsmessung
 *   6. abraeumen, Restprozesse zaehlen, JSON, drei Bilder im Ruhezustand
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w8).
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
import {
    READABILITY_EXCLUSIONS,
    measureReadability,
    resetScroll,
    scrollRegionsToEnd,
} from './lib/readability.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w8';
const OUT_DIR = join(ROOT, 'verification', 'w8');
const OUT_JSON = join(OUT_DIR, 'layout.json');
const SHOT_DEFAULT = join(OUT_DIR, 'layout-default.png');
const SHOT_LARGE = join(OUT_DIR, 'layout-explain-large.png');
const SHOT_CUSTOM = join(OUT_DIR, 'layout-custom.png');

/** 4390/4391 und 4141 gehoeren dem Nutzer. Ab hier ist frei (Contract AC7). */
const MIN_PORT = 4440;

/** Die Schrittweite der Pfeiltasten, wie src/layout/layout-model.ts sie setzt. */
const LAYOUT_STEP = 16;
const LAYOUT_BIG_STEP = 64;

/**
 * Die vier Griffe, mit dem Mass, das jeder von ihnen zieht.
 *
 * `grow` ist das Vorzeichen der Bewegung nach RECHTS beziehungsweise nach
 * UNTEN, die die Zone GROESSER macht. Es steht hier als Zahl und nicht als
 * Vermutung, weil zwei der vier Griffe die Richtung umkehren: der Bereich unter
 * dem Reader waechst nach oben, die rechte Spalte nach links. Ein Lauf, der das
 * verwechselt, zieht an einem Anschlag und meldet einen Griff, der klemmt.
 */
const SPLITTERS = [
    { testId: 'atlas-split-left', key: 'leftWidth', axis: 'x', grow: 1, zone: '.atlas-tree' },
    { testId: 'atlas-split-explain', key: 'explainHeight', axis: 'y', grow: -1, zone: '[data-testid="atlas-explain"]' },
    { testId: 'atlas-split-right', key: 'rightWidth', axis: 'x', grow: -1, zone: '.atlas-side' },
    { testId: 'atlas-split-twin', key: 'twinHeight', axis: 'y', grow: 1, zone: '.atlas-twin' },
];

/**
 * Die Zonen, die einander nie ueberlagern duerfen.
 *
 * Die BLAETTER und nicht die Rahmen: `.atlas-main` enthaelt den Reader und den
 * Erklaeren-Bereich, `.atlas-side` enthaelt die drei rechten Flaechen. Ein
 * Rahmen ueberlagert seine eigenen Kinder per Bauart, und ihn mitzumessen waere
 * ein Befund, der nur die Schachtelung beschreibt.
 */
const ZONES = [
    { name: 'tree', selector: '.atlas-tree' },
    { name: 'reader', selector: '[data-testid="atlas-reader"]' },
    { name: 'explain', selector: '[data-testid="atlas-explain"]' },
    { name: 'llm', selector: '.atlas-llm' },
    { name: 'twin', selector: '.atlas-twin' },
    { name: 'galaxy', selector: '.atlas-galaxy' },
];

/** Die Wurzeln der fuenf Flaechen, die sich einen Platz teilen. */
const PANEL_ROOTS = [
    { tab: 'flow', selector: '[data-testid="atlas-flow-overlay"]' },
    { tab: 'walk', selector: '[data-testid="atlas-tour"]' },
    { tab: 'chat', selector: '[data-testid="atlas-chat"]' },
    { tab: 'bug', selector: '[data-testid="atlas-bugwizard"]' },
    { tab: 'change', selector: '[data-testid="atlas-impact"]' },
];

/** Das Symbol, ueber das der Vorwaerts-Walk laeuft. Dasselbe wie in W5c und W9. */
const WALK_TARGET = 'createUser';
const TARGET_FILE = 'src/services/userService.ts';
const TARGET_QUALIFIED = 'userService\\.createUser';

const MAIN_VIEWPORT = { width: 1680, height: 1050 };

/** Chromium ohne Aussenwelt, plus die Software-GL-Flags fuer die Galaxie. */
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

const log = (...parts) => console.log('[smoke-w8]', ...parts);
const serverLog = [];

function run(command, args) {
    return new Promise((done) => {
        const child = spawn(command, args, {
            cwd: ROOT,
            env: {
                ...process.env,
                NO_UPDATE_NOTIFIER: '1',
                npm_config_update_notifier: 'false',
            },
        });
        let out = '';
        child.stdout.on('data', (chunk) => {
            out += String(chunk);
        });
        child.stderr.on('data', (chunk) => {
            out += String(chunk);
        });
        child.on('close', (code) => done({ code, out }));
    });
}

/** Der Griff der Oberflaeche auf das Layout. */
const layoutSeam = (page) => page.evaluate(() => globalThis.__atlasLayout ?? null);

/** Die Rechtecke der Zonen und der Griffe, gerundet. */
const rectsOf = (page, zones, splitters) =>
    page.evaluate(({ zoneList, splitterList }) => {
        const box = (selector) => {
            const node = document.querySelector(selector);
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
        };
        const out = { zones: {}, splitters: {} };
        for (const zone of zoneList) {
            out.zones[zone.name] = box(zone.selector);
        }
        for (const splitter of splitterList) {
            out.splitters[splitter.testId] = box(`[data-testid="${splitter.testId}"]`);
        }
        return out;
    }, { zoneList: zones, splitterList: splitters });

/** Zwei Rechtecke, die sich um mehr als einen Pixel schneiden. */
function intersects(a, b) {
    if (a === null || b === null) {
        return false;
    }
    const overlapX = Math.min(a.x + a.width, b.x + b.width) - Math.max(a.x, b.x);
    const overlapY = Math.min(a.y + a.height, b.y + b.height) - Math.max(a.y, b.y);
    return overlapX > 1 && overlapY > 1;
}

/** Welche Zonenpaare sich schneiden. Leer heisst: keine. */
function overlappingZones(rects) {
    const names = Object.keys(rects.zones);
    const hits = [];
    for (let i = 0; i < names.length; i += 1) {
        for (let j = i + 1; j < names.length; j += 1) {
            if (intersects(rects.zones[names[i]], rects.zones[names[j]])) {
                hits.push([names[i], names[j]]);
            }
        }
    }
    return hits;
}

/**
 * Was der Erklaeren-Bereich gerade zeigt.
 *
 * `panels` ist die Zahl der Inhaltskaesten (hoechstens einer), `roots` die
 * Liste der Flaechen, die wirklich im Baum stehen. Beides zusammen ist die
 * Messung zu AC1: der Befund vom 2026-08-29 war ein Bildschirm mit `roots`
 * gleich drei.
 */
const explainState = (page, panelRoots) =>
    page.evaluate((roots) => {
        const zone = document.querySelector('[data-testid="atlas-explain"]');
        const text = (selector) =>
            document.querySelector(selector)?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        const boxOf = (selector) => {
            const node = document.querySelector(selector);
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
        };
        return {
            present: zone !== null,
            open: zone?.getAttribute('data-open') === 'true',
            tab: zone?.getAttribute('data-tab') ?? '',
            panels: document.querySelectorAll('[data-testid="atlas-explain-panel"]').length,
            roots: roots
                .filter((entry) => document.querySelector(entry.selector) !== null)
                .map((entry) => entry.tab),
            rootBoxes: Object.fromEntries(roots.map((entry) => [entry.tab, boxOf(entry.selector)])),
            note: text('[data-testid="atlas-explain-note"]'),
            empty: text('[data-testid="atlas-explain-empty"]'),
            tabs: [...document.querySelectorAll('[data-testid="atlas-explain-tab"]')].map((tab) => ({
                id: tab.getAttribute('data-tab') ?? '',
                enabled: tab.getAttribute('data-enabled') === 'true',
                on: tab.getAttribute('data-on') === 'true',
                label: tab.textContent?.trim() ?? '',
            })),
            chatTurns: document.querySelectorAll('[data-testid="atlas-chat-turn"]').length,
            chatBox: boxOf('[data-testid="atlas-chat"]'),
            bodyBox: boxOf('[data-testid="atlas-explain-panel"]'),
            walkPosition: text('[data-testid="atlas-tour-progress"]'),
            flowPosition: text('[data-testid="atlas-flow-position"]'),
        };
    }, panelRoots);

/** Einen Reiter waehlen und warten, bis der Bereich ihn zeigt. */
async function chooseTab(page, tab) {
    await page.click(`[data-testid="atlas-explain-tab"][data-tab="${tab}"]`);
    await page.waitForFunction(
        (wanted) => globalThis.__atlasLayout?.explainTab === wanted
            && globalThis.__atlasLayout?.explainOpen === true,
        tab,
        { timeout: 15000 },
    );
    await page.waitForTimeout(250);
}

/** Den Wert eines Masses aus dem Griff der Oberflaeche. */
const sizeOf = (page, key) =>
    page.evaluate((wanted) => globalThis.__atlasLayout?.sizes?.[wanted] ?? -1, key);

/** Die Grenzen eines Masses, wie die Oberflaeche sie selbst nennt. */
const boundsOf = (page, key) =>
    page.evaluate((wanted) => globalThis.__atlasLayout?.bounds?.[wanted] ?? null, key);

/** Einen Griff mit der Maus ziehen. Zurueck kommt, wo er vorher und nachher lag. */
async function dragSplitter(page, testId, delta, axis) {
    const handle = await page.$(`[data-testid="${testId}"]`);
    const box = await handle.boundingBox();
    const from = { x: box.x + box.width / 2, y: box.y + box.height / 2 };
    await page.mouse.move(from.x, from.y);
    await page.mouse.down();
    // In zwei Schritten, damit ein Griff, der auf das erste Ereignis nicht
    // reagiert, nicht faelschlich als beweglich gilt.
    await page.mouse.move(
        from.x + (axis === 'x' ? delta / 2 : 0),
        from.y + (axis === 'y' ? delta / 2 : 0),
        { steps: 4 },
    );
    await page.mouse.move(
        from.x + (axis === 'x' ? delta : 0),
        from.y + (axis === 'y' ? delta : 0),
        { steps: 4 },
    );
    await page.mouse.up();
    await page.waitForTimeout(200);
}

/** Eine Taste am Griff, nachdem er den Fokus hat. */
async function pressAtSplitter(page, testId, key, shift = false) {
    await page.focus(`[data-testid="${testId}"]`);
    await page.keyboard.press(shift ? `Shift+${key}` : key);
    await page.waitForTimeout(120);
}

/** Ein Mass auf einen Wert bringen, ueber die Tastatur, mit Anschlag. */
async function pushSplitter(page, splitter, direction, times) {
    const key = splitter.axis === 'x'
        ? (direction > 0 ? 'ArrowRight' : 'ArrowLeft')
        : (direction > 0 ? 'ArrowDown' : 'ArrowUp');
    await page.focus(`[data-testid="${splitter.testId}"]`);
    for (let i = 0; i < times; i += 1) {
        await page.keyboard.press(`Shift+${key}`);
    }
    await page.waitForTimeout(250);
}

/** Die Anwendung oeffnen und warten, bis sie steht. */
async function openApp(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-explain"]', { timeout: 30000 });
    await page.waitForFunction(() => globalThis.__atlasLayout !== undefined, undefined, { timeout: 30000 });
    await page.waitForTimeout(900);
}

/**
 * Wo jeder scrollbare Bereich steht. Wortgleich mit smoke-w9: die Beweisbilder
 * dieses Zyklus entstehen im Ruhezustand, wie die dortigen auch.
 */
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

/** Ein Beweisbild im Ruhezustand. Wortgleich mit smoke-w9. */
async function shootAtRest(page, file, name) {
    await resetScroll(page, READABILITY_EXCLUSIONS);
    await page.waitForTimeout(350);
    const state = await scrollState(page);
    await page.screenshot({ path: file, fullPage: false });
    log(`${name}: aufgenommen im Ruhezustand=${state.atRest}`);
    return { name, atRest: state.atRest, regions: state.regions };
}

/** Die Frage wieder aufrufen und einen Modus waehlen. Wortgleich mit smoke-w9. */
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
    await page.waitForTimeout(1400);
}

/**
 * Zu einem Symbol navigieren, ueber dieselbe Suche, die die Kommandozeile fuehrt.
 * Wortgleich mit smoke-w5c.
 */
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

/** Eine Frage in die Kommandozeile tippen und abschicken. */
async function askInLine(page, question) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(question, { delay: 25 });
    await page.keyboard.press('Enter');
    await page.waitForTimeout(500);
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
        zonesNeverOverlap: false,
        singleExplainTabVisible: false,
        explainTabs: [],
        disabledTabsExplainThemselves: false,
        allFourSplittersDrag: false,
        splittersKeyboard: false,
        splitterDoubleClickResets: false,
        layoutPersistsReload: false,
        resetLayoutWorks: false,
        explainCollapsedOnOpen: false,
        explainOpensOnDemand: false,
        tabSwitchKeepsState: false,
        stateProbes: { chatLines: 0, walkStep: -1, flowStep: -1 },
        minZoneRespected: false,
        chatTabKeepsEscape: false,
        chatTabHistorySurvives: false,
        chatTabClearStillClears: false,
        chatHeightFollowsZone: false,
        overlapViolations: 0,
        clippingViolations: 0,
        cutWithoutHint: 0,
        extremeLayouts: [],
        screenshotsAtRest: false,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = {
        blockedRequests: [],
        consoleErrors: [],
        pageErrors: [],
        readability: [],
        shots: [],
        zoneRects: {},
        splitters: {},
        tabCycle: [],
        openedOnDemand: [],
        disabledReasons: [],
        persistence: {},
        reset: {},
        extremes: {},
        chat: {},
    };

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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w8-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w8-run-');
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

        const readability = async (name) => {
            const top = await measureReadability(page, READABILITY_EXCLUSIONS);
            const scrolled = await scrollRegionsToEnd(page, READABILITY_EXCLUSIONS);
            await page.waitForTimeout(200);
            const bottom = await measureReadability(page, READABILITY_EXCLUSIONS);
            await resetScroll(page, READABILITY_EXCLUSIONS);
            const clipped = [...top.clipped, ...bottom.clipped];
            const entry = {
                name,
                scrolledRegions: scrolled.length,
                overlaps: top.overlaps.length + bottom.overlaps.length,
                clipped: clipped.length,
                cutWithoutHint: clipped.filter((item) => item.kind === 'cut-without-hint').length,
                top: { candidates: top.candidates, overlaps: top.overlaps, clipped: top.clipped },
                bottom: {
                    candidates: bottom.candidates,
                    overlaps: bottom.overlaps,
                    clipped: bottom.clipped,
                },
            };
            extras.readability.push(entry);
            report.overlapViolations += entry.overlaps;
            report.clippingViolations += entry.clipped;
            report.cutWithoutHint += entry.cutWithoutHint;
            log(`Lesbarkeit ${name}: ${entry.overlaps} Ueberlagerungen, `
                + `${entry.clipped} Beschneidungen (davon ${entry.cutWithoutHint} ohne Hinweis)`);
            return entry;
        };

        // ------------------------------- 6a. Ruhezustand: eingeklappt (AC4)
        await openApp(page, origin);
        const atRest = await explainState(page, PANEL_ROOTS);
        const seamAtRest = await layoutSeam(page);
        extras.atRest = { dom: atRest, seam: seamAtRest };
        report.explainTabs = atRest.tabs.map((tab) => tab.id);
        report.explainCollapsedOnOpen =
            atRest.present === true
            && atRest.open === false
            && atRest.panels === 0
            && atRest.roots.length === 0
            && seamAtRest?.explainOpen === false;
        log(`Ruhezustand: Bereich offen=${atRest.open}, Inhalte=${atRest.roots.length}, `
            + `Reiter=[${report.explainTabs.join(', ')}]`);

        const defaultRects = await rectsOf(page, ZONES, SPLITTERS);
        extras.zoneRects.default = defaultRects;
        const defaultOverlaps = overlappingZones(defaultRects);
        const allZonesVisible = ZONES.every((zone) => {
            const rect = defaultRects.zones[zone.name];
            return rect !== null && rect.width > 8 && rect.height > 8;
        });
        log(`Zonen im Ruhezustand: ${JSON.stringify(defaultRects.zones)}`);

        await readability('default');
        extras.shots.push(await shootAtRest(page, SHOT_DEFAULT, 'layout-default'));

        // ------------------- 6b. Ein leerer Reiter sagt, warum er leer ist
        const disabled = atRest.tabs.filter((tab) => !tab.enabled).map((tab) => tab.id);
        for (const tab of disabled) {
            await chooseTab(page, tab);
            const state = await explainState(page, PANEL_ROOTS);
            extras.disabledReasons.push({
                tab,
                reason: state.empty,
                roots: state.roots,
                panels: state.panels,
            });
        }
        report.disabledTabsExplainThemselves =
            disabled.length > 0
            && extras.disabledReasons.every((entry) =>
                entry.reason.length >= 40 && entry.roots.length === 0 && entry.panels === 1);
        log(`Leere Reiter: ${disabled.join(', ')} nennen ihren Grund `
            + `(${report.disabledTabsExplainThemselves})`);

        // --------------------------- 6c. Zustand aufbauen, auf drei Reitern
        //
        // Die Reihenfolge ist keine Bequemlichkeit: der Walk setzt bei jedem
        // Schritt das Subjekt des Twins, und der Flow haengt am Subjekt. Erst
        // laufen, dann das Bild zum erreichten Symbol aufschlagen; andersherum
        // waere das Bild eine Stelle, die der naechste Schritt wegwirft.
        await askInLine(page, 'what does this project do?');
        await page.waitForFunction(
            () => (globalThis.__atlasChat?.turns ?? []).length >= 1,
            undefined,
            { timeout: 30000 },
        );
        const afterAsk = await layoutSeam(page);
        extras.openedOnDemand.push({ trigger: 'chat question', tab: afterAsk?.explainTab, open: afterAsk?.explainOpen });

        log('Frage gestellt, Reiter:', (await layoutSeam(page))?.explainTab);

        /*
         * Der Vorwaerts-Walk und nicht die Fuehrung durchs Projekt.
         *
         * Die Projekt-Fuehrung geht ueber DATEIEN, und ein Dateischritt hat kein
         * Symbol; der Twin blieb dabei leer und der Flow-Reiter haette nichts zu
         * zeigen. Der Vorwaerts-Walk laeuft ueber Symbole, also hat jeder seiner
         * Schritte einen Flow. Genau diese Unterscheidung steht auch auf der
         * Schrittkarte selbst (`[d] diagram` ist auf Dateischritten aus), und
         * dieser Lauf braucht die Seite, auf der es etwas zu messen gibt.
         */
        await openWhyAndChoose(page, 'entry');
        await chooseEntryHit(page, WALK_TARGET);
        const afterWalk = await layoutSeam(page);
        extras.openedOnDemand.push({ trigger: 'walk', tab: afterWalk?.explainTab, open: afterWalk?.explainOpen });
        log('Walk laeuft, Reiter:', afterWalk?.explainTab);
        await page.click('[data-testid="atlas-tour-next"]');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 1, undefined, { timeout: 40000 });
        await page.waitForTimeout(1400);

        /*
         * Das Subjekt zurueck auf die Wurzel des Walks, ueber die Suche.
         *
         * Der zweite Schritt des Walks ist `insert`, und `insert` ruft nichts
         * auf: sein Flow hat null Pfeile. Das ist kein Fehler, sondern die
         * ehrliche Antwort des Index, und dieser Lauf braucht ein Symbol, an
         * dem sich ein Schritt ueberhaupt gehen laesst. `createUser` ist es, und
         * es ist dasselbe, an dem W5c den Stepper beweist.
         *
         * Der Walk bleibt dabei stehen, wo er steht: das Subjekt des Twins und
         * der Schritt der Fuehrung sind zwei Dinge, und dass sie es sind, ist
         * gleich Teil der Messung zu AC5.
         */
        await openSymbol(page, WALK_TARGET, TARGET_FILE, TARGET_QUALIFIED);
        await page.waitForTimeout(800);

        await page.click('[data-testid="atlas-twin-subject"]');
        await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { timeout: 30000 });
        const afterFlow = await layoutSeam(page);
        extras.openedOnDemand.push({ trigger: 'flow head', tab: afterFlow?.explainTab, open: afterFlow?.explainOpen });
        await page.waitForFunction(
            () => Number(document.querySelector('[data-testid="atlas-flow"]')?.getAttribute('data-arrows') ?? '0') > 0,
            undefined,
            { timeout: 60000 },
        );
        await page.click('[data-testid="atlas-flow-next"]');
        await page.waitForFunction(
            () => (globalThis.__atlasLayout?.state?.flowStep ?? -1) >= 0,
            undefined,
            { timeout: 20000 },
        );
        await page.click('[data-testid="atlas-flow-next"]');
        log('Flow steht auf Schritt', (await layoutSeam(page))?.state?.flowStep);
        // Warten, bis der Caret sich gesetzt hat: der Twin folgt ihm mit einer
        // Entprellung, und ein Reiterwechsel mitten darin waere ein Subjekt, das
        // sich aus einem anderen Grund als dem Wechsel aendert.
        await page.waitForTimeout(1200);

        report.explainOpensOnDemand = extras.openedOnDemand.length === 3
            && extras.openedOnDemand[0].tab === 'chat' && extras.openedOnDemand[0].open === true
            && extras.openedOnDemand[1].tab === 'walk' && extras.openedOnDemand[1].open === true
            && extras.openedOnDemand[2].tab === 'flow' && extras.openedOnDemand[2].open === true;
        log(`Auf Zuruf geoeffnet: ${JSON.stringify(extras.openedOnDemand)}`);

        // ------------------------------- 6d. Immer genau ein Inhalt (AC1)
        const cycle = ['flow', 'walk', 'chat', 'bug', 'change', 'flow', 'chat', 'walk'];
        for (const tab of cycle) {
            await chooseTab(page, tab);
            const state = await explainState(page, PANEL_ROOTS);
            const boxes = Object.entries(state.rootBoxes).filter(([, box]) => box !== null);
            let clashes = 0;
            for (let i = 0; i < boxes.length; i += 1) {
                for (let j = i + 1; j < boxes.length; j += 1) {
                    if (intersects(boxes[i][1], boxes[j][1])) {
                        clashes += 1;
                    }
                }
            }
            const rects = await rectsOf(page, ZONES, SPLITTERS);
            extras.tabCycle.push({
                tab,
                panels: state.panels,
                roots: state.roots,
                clashes,
                zoneOverlaps: overlappingZones(rects),
            });
        }
        report.singleExplainTabVisible = extras.tabCycle.every((entry) =>
            entry.panels === 1 && entry.roots.length <= 1 && entry.clashes === 0);
        report.zonesNeverOverlap =
            defaultOverlaps.length === 0
            && allZonesVisible
            && extras.tabCycle.every((entry) => entry.zoneOverlaps.length === 0);
        log(`Reiterwechsel: hoechstens ein Inhalt=${report.singleExplainTabVisible}, `
            + `Zonen ohne Ueberlagerung=${report.zonesNeverOverlap}`);

        await readability('explain-open');

        // ---------------------------- 6e. Nichts geht verloren (AC5, AC5b)
        await chooseTab(page, 'chat');
        const chatBefore = await explainState(page, PANEL_ROOTS);
        await chooseTab(page, 'walk');
        const walkBefore = await explainState(page, PANEL_ROOTS);
        await chooseTab(page, 'flow');
        const flowBefore = await explainState(page, PANEL_ROOTS);
        const seamBefore = (await layoutSeam(page))?.state ?? {};

        // Einmal ueber alle fuenf und zurueck.
        for (const tab of ['bug', 'change', 'chat', 'walk', 'flow', 'chat', 'flow']) {
            await chooseTab(page, tab);
        }

        await chooseTab(page, 'chat');
        const chatAfter = await explainState(page, PANEL_ROOTS);
        await chooseTab(page, 'walk');
        const walkAfter = await explainState(page, PANEL_ROOTS);
        await chooseTab(page, 'flow');
        const flowAfter = await explainState(page, PANEL_ROOTS);
        const seamAfter = (await layoutSeam(page))?.state ?? {};

        extras.stateBeforeAfter = {
            before: {
                chatTurns: chatBefore.chatTurns,
                walkPosition: walkBefore.walkPosition,
                flowPosition: flowBefore.flowPosition,
                seam: seamBefore,
            },
            after: {
                chatTurns: chatAfter.chatTurns,
                walkPosition: walkAfter.walkPosition,
                flowPosition: flowAfter.flowPosition,
                seam: seamAfter,
            },
        };
        report.tabSwitchKeepsState =
            chatBefore.chatTurns >= 1
            && chatAfter.chatTurns === chatBefore.chatTurns
            && walkBefore.walkPosition.length > 0
            && walkAfter.walkPosition === walkBefore.walkPosition
            && flowBefore.flowPosition.length > 0
            && flowAfter.flowPosition === flowBefore.flowPosition
            && seamAfter.chatTurns === seamBefore.chatTurns
            && seamAfter.walkStep === seamBefore.walkStep
            && seamAfter.flowStep === seamBefore.flowStep;
        report.stateProbes = {
            chatLines: chatAfter.chatTurns,
            walkStep: Number.isInteger(seamAfter.walkStep) ? seamAfter.walkStep : -1,
            flowStep: Number.isInteger(seamAfter.flowStep) ? seamAfter.flowStep : -1,
        };
        log(`Zustand ueber den Wechsel: Chat ${chatBefore.chatTurns} -> ${chatAfter.chatTurns}, `
            + `Walk "${walkBefore.walkPosition}" -> "${walkAfter.walkPosition}", `
            + `Flow "${flowBefore.flowPosition}" -> "${flowAfter.flowPosition}"`);

        // AC5b: Escape klappt ein und kostet nichts.
        await chooseTab(page, 'chat');
        const beforeEscape = await explainState(page, PANEL_ROOTS);
        await page.click('[data-testid="atlas-chat-head"]');
        await page.keyboard.press('Escape');
        await page.waitForTimeout(400);
        const afterEscape = await explainState(page, PANEL_ROOTS);
        const seamAfterEscape = await layoutSeam(page);
        await chooseTab(page, 'chat');
        const afterReopen = await explainState(page, PANEL_ROOTS);
        extras.chat = {
            beforeEscape: { turns: beforeEscape.chatTurns, open: beforeEscape.open },
            afterEscape: {
                turns: afterEscape.chatTurns,
                open: afterEscape.open,
                note: afterEscape.note,
                seamTurns: seamAfterEscape?.state?.chatTurns ?? -1,
            },
            afterReopen: { turns: afterReopen.chatTurns, open: afterReopen.open },
        };
        report.chatTabKeepsEscape =
            beforeEscape.open === true
            && afterEscape.open === false
            && afterEscape.roots.length === 0
            && (seamAfterEscape?.state?.chatTurns ?? -1) === beforeEscape.chatTurns
            && afterEscape.note.length > 0;
        report.chatTabHistorySurvives =
            afterReopen.chatTurns === beforeEscape.chatTurns && beforeEscape.chatTurns >= 1;
        log(`Escape am Chat: offen ${beforeEscape.open} -> ${afterEscape.open}, `
            + `Zuege ${beforeEscape.chatTurns} -> ${afterReopen.chatTurns}, `
            + `Streifen sagt "${afterEscape.note}"`);

        // AC5b: die Hoehe kommt von der Zone.
        const chatHeightBefore = (await explainState(page, PANEL_ROOTS)).chatBox;
        const explainBefore = await sizeOf(page, 'explainHeight');
        await dragSplitter(page, 'atlas-split-explain', -120, 'y');
        const chatHeightAfter = (await explainState(page, PANEL_ROOTS)).chatBox;
        const explainAfter = await sizeOf(page, 'explainHeight');
        extras.chat.height = {
            before: { zone: explainBefore, chat: chatHeightBefore },
            after: { zone: explainAfter, chat: chatHeightAfter },
        };
        report.chatHeightFollowsZone =
            explainAfter > explainBefore + 40
            && chatHeightAfter !== null
            && chatHeightBefore !== null
            && chatHeightAfter.height > chatHeightBefore.height + 40;
        log(`Hoehe der Zone ${explainBefore} -> ${explainAfter}, `
            + `Chat ${chatHeightBefore?.height} -> ${chatHeightAfter?.height}`);

        // ------------------------------------------ 6f. Die vier Griffe (AC2)
        const dragResults = [];
        const keyResults = [];
        const resetResults = [];
        const boundResults = [];
        for (const splitter of SPLITTERS) {
            const bounds = await boundsOf(page, splitter.key);
            // Erst in die Mitte des Bereichs, damit ein Zug in beide Richtungen
            // Platz hat und ein Anschlag nicht als "unbeweglich" durchgeht.
            const middle = Math.round((bounds.min + bounds.max) / 2);
            const start = await sizeOf(page, splitter.key);
            const steps = Math.round(Math.abs(middle - start) / LAYOUT_BIG_STEP) + 1;
            await pushSplitter(page, splitter, middle > start ? splitter.grow : -splitter.grow, steps);

            const before = await sizeOf(page, splitter.key);
            const rectsBefore = await rectsOf(page, ZONES, SPLITTERS);
            const zoneBefore = await page.evaluate((selector) => {
                const node = document.querySelector(selector);
                if (node === null) {
                    return null;
                }
                const rect = node.getBoundingClientRect();
                return { width: Math.round(rect.width), height: Math.round(rect.height) };
            }, splitter.zone);

            await dragSplitter(page, splitter.testId, 60 * splitter.grow, splitter.axis);
            const after = await sizeOf(page, splitter.key);
            const zoneAfter = await page.evaluate((selector) => {
                const node = document.querySelector(selector);
                if (node === null) {
                    return null;
                }
                const rect = node.getBoundingClientRect();
                return { width: Math.round(rect.width), height: Math.round(rect.height) };
            }, splitter.zone);
            const measured = splitter.axis === 'x' ? 'width' : 'height';
            dragResults.push({
                key: splitter.key,
                before,
                after,
                zoneBefore,
                zoneAfter,
                moved: after - before,
                zoneMoved: (zoneAfter?.[measured] ?? 0) - (zoneBefore?.[measured] ?? 0),
            });

            // Doppelklick: genau diese Grenze zurueck auf die Vorgabe.
            //
            // Hier und nicht am Ende, weil die Tastaturmessung darunter Platz in
            // BEIDE Richtungen braucht: nach dem Zug steht der Griff dicht am
            // Anschlag, und ein grosser Schritt, der dort abgeschnitten wird,
            // saehe aus wie ein Griff, der die Schrittweite nicht kann.
            await page.dblclick(`[data-testid="${splitter.testId}"]`);
            await page.waitForTimeout(250);
            const seam = await layoutSeam(page);
            resetResults.push({
                key: splitter.key,
                value: seam?.sizes?.[splitter.key] ?? -1,
                expected: seam?.defaults?.[splitter.key] ?? -2,
                beforeReset: after,
            });

            // Tastatur: ein Schritt, ein grosser Schritt, und beide zurueck.
            const keyStart = await sizeOf(page, splitter.key);
            const forward = splitter.axis === 'x' ? 'ArrowRight' : 'ArrowDown';
            const backward = splitter.axis === 'x' ? 'ArrowLeft' : 'ArrowUp';
            await pressAtSplitter(page, splitter.testId, forward);
            const afterOne = await sizeOf(page, splitter.key);
            await pressAtSplitter(page, splitter.testId, backward);
            const afterBack = await sizeOf(page, splitter.key);
            await pressAtSplitter(page, splitter.testId, forward, true);
            const afterBig = await sizeOf(page, splitter.key);
            await pressAtSplitter(page, splitter.testId, backward, true);
            keyResults.push({
                key: splitter.key,
                start: keyStart,
                afterOne,
                afterBack,
                afterBig,
                step: Math.abs(afterOne - keyStart),
                bigStep: Math.abs(afterBig - afterBack),
            });

            // Die Anschlaege: ganz an das eine Ende, ganz an das andere, und
            // die Zone muss beide Male noch da sein.
            await pushSplitter(page, splitter, splitter.grow, 40);
            const maxValue = await sizeOf(page, splitter.key);
            const rectsAtMax = await rectsOf(page, ZONES, SPLITTERS);
            await pushSplitter(page, splitter, -splitter.grow, 80);
            const minValue = await sizeOf(page, splitter.key);
            const rectsAtMin = await rectsOf(page, ZONES, SPLITTERS);
            const alive = (rects) => ZONES.every((zone) => {
                const rect = rects.zones[zone.name];
                return rect !== null && rect.width > 8 && rect.height > 8;
            });
            boundResults.push({
                key: splitter.key,
                bounds,
                maxValue,
                minValue,
                zonesAliveAtMax: alive(rectsAtMax),
                zonesAliveAtMin: alive(rectsAtMin),
                overlapsAtMax: overlappingZones(rectsAtMax),
                overlapsAtMin: overlappingZones(rectsAtMin),
            });
            await page.dblclick(`[data-testid="${splitter.testId}"]`);
            await page.waitForTimeout(200);
            extras.zoneRects[`${splitter.key}-max`] = rectsAtMax;
            extras.zoneRects[`${splitter.key}-min`] = rectsAtMin;
        }
        extras.splitters = { drag: dragResults, keyboard: keyResults, reset: resetResults, bounds: boundResults };
        report.allFourSplittersDrag =
            dragResults.length === 4
            && dragResults.every((entry) => Math.abs(entry.moved) >= 30 && Math.abs(entry.zoneMoved) >= 30);
        report.splittersKeyboard =
            keyResults.length === 4
            && keyResults.every((entry) => entry.step === LAYOUT_STEP
                && entry.afterBack === entry.start
                && entry.bigStep === LAYOUT_BIG_STEP);
        report.splitterDoubleClickResets =
            resetResults.length === 4
            && resetResults.every((entry) =>
                entry.value === entry.expected && entry.beforeReset !== entry.expected);
        report.minZoneRespected =
            boundResults.length === 4
            && boundResults.every((entry) =>
                entry.maxValue === entry.bounds.max
                && entry.minValue === entry.bounds.min
                && entry.zonesAliveAtMax === true
                && entry.zonesAliveAtMin === true
                && entry.overlapsAtMax.length === 0
                && entry.overlapsAtMin.length === 0);
        log(`Griffe: Ziehen ${report.allFourSplittersDrag}, Tastatur ${report.splittersKeyboard}, `
            + `Doppelklick ${report.splitterDoubleClickResets}, Anschlaege ${report.minZoneRespected}`);

        // ---------------------------------------- 6g. Reload haelt die Masse
        await pushSplitter(page, SPLITTERS[0], SPLITTERS[0].grow, 2);
        await pushSplitter(page, SPLITTERS[1], SPLITTERS[1].grow, 2);
        await pushSplitter(page, SPLITTERS[2], SPLITTERS[2].grow, 2);
        await pushSplitter(page, SPLITTERS[3], SPLITTERS[3].grow, 2);
        const custom = (await layoutSeam(page))?.sizes ?? {};
        const customRects = await rectsOf(page, ZONES, SPLITTERS);
        extras.zoneRects.custom = customRects;
        await chooseTab(page, 'walk');
        await readability('custom-layout');
        extras.shots.push(await shootAtRest(page, SHOT_CUSTOM, 'layout-custom'));

        await page.reload({ waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-explain"]', { timeout: 30000 });
        await page.waitForFunction(() => globalThis.__atlasLayout !== undefined, undefined, { timeout: 30000 });
        await page.waitForTimeout(1200);
        const reloaded = (await layoutSeam(page))?.sizes ?? {};
        extras.persistence = { custom, reloaded, storageKey: (await layoutSeam(page))?.storageKey ?? '' };
        report.layoutPersistsReload =
            Object.keys(custom).length === 4
            && Object.keys(custom).every((key) => custom[key] === reloaded[key])
            && Object.keys(custom).some((key) => custom[key] !== (extras.atRest.seam?.defaults?.[key] ?? -1));
        log(`Reload: ${JSON.stringify(custom)} -> ${JSON.stringify(reloaded)} `
            + `(${report.layoutPersistsReload})`);

        // Nach dem Reload ist der Bereich wieder eingeklappt und der Verlauf
        // weg: er lebt im Speicher dieser Sitzung und nirgends sonst. Das ist
        // dieselbe Zusicherung wie in W7c und keine Nebenwirkung dieses Umbaus.
        const afterReload = await explainState(page, PANEL_ROOTS);
        extras.afterReload = { open: afterReload.open, roots: afterReload.roots, tab: afterReload.tab };

        // --------------------------------- 6h. Der Weg zurueck (AC3)
        await askInLine(page, 'reset layout');
        await page.waitForTimeout(500);
        const afterCommand = await layoutSeam(page);
        const commandLine = await page.inputValue('[data-testid="atlas-command-input"]');

        await pushSplitter(page, SPLITTERS[0], SPLITTERS[0].grow, 3);
        await pushSplitter(page, SPLITTERS[1], SPLITTERS[1].grow, 3);
        const beforeMenu = await layoutSeam(page);
        await page.click('[data-menu="a-layout"]');
        await page.waitForTimeout(400);
        const afterMenu = await layoutSeam(page);
        extras.reset = {
            byCommand: { isDefault: afterCommand?.isDefault, sizes: afterCommand?.sizes, lineCleared: commandLine === '' },
            beforeMenu: { isDefault: beforeMenu?.isDefault, sizes: beforeMenu?.sizes },
            byMenu: { isDefault: afterMenu?.isDefault, sizes: afterMenu?.sizes },
        };
        report.resetLayoutWorks =
            afterCommand?.isDefault === true
            && commandLine === ''
            && beforeMenu?.isDefault === false
            && afterMenu?.isDefault === true;
        log(`reset layout: ueber die Zeile ${afterCommand?.isDefault}, `
            + `ueber das Menue ${afterMenu?.isDefault}`);

        // -------------------------------------- 6i. Die zwei Extremlagen (AC6)
        //
        // Der Erklaeren-Bereich muss dafuer offen sein UND etwas zeigen: eine
        // Extremlage mit einem leeren Reiter waere eine Aussage ueber einen
        // Streifen und ein Beweisbild ohne Gegenstand. Der Reload hat die
        // Sitzung geleert (der Verlauf lebt im Speicher und nirgends sonst),
        // also wird sie hier wieder aufgebaut: eine Datei, ein Symbol, eine
        // Frage.
        await openSymbol(page, WALK_TARGET, TARGET_FILE, TARGET_QUALIFIED);
        await askInLine(page, 'what does this function do?');
        await page.waitForFunction(
            () => (globalThis.__atlasChat?.turns ?? []).length >= 1,
            undefined,
            { timeout: 30000 },
        );
        await chooseTab(page, 'flow');
        await page.waitForFunction(
            () => Number(document.querySelector('[data-testid="atlas-flow"]')?.getAttribute('data-arrows') ?? '0') > 0,
            undefined,
            { timeout: 60000 },
        );
        await page.click('[data-testid="atlas-flow-next"]');
        await page.waitForTimeout(900);
        for (const extreme of [
            { name: 'explain-max', direction: 1 },
            { name: 'explain-min', direction: -1 },
        ]) {
            await pushSplitter(page, SPLITTERS[1], SPLITTERS[1].grow * extreme.direction, 45);
            await page.waitForTimeout(400);
            const bounds = await boundsOf(page, 'explainHeight');
            const value = await sizeOf(page, 'explainHeight');
            const rects = await rectsOf(page, ZONES, SPLITTERS);
            const before = {
                overlaps: report.overlapViolations,
                clipped: report.clippingViolations,
                cut: report.cutWithoutHint,
            };
            const entry = await readability(extreme.name);
            report.extremeLayouts.push({
                name: extreme.name,
                explainHeight: value,
                atBound: extreme.direction > 0 ? value === bounds.max : value === bounds.min,
                readerHeight: rects.zones.reader?.height ?? 0,
                zoneOverlaps: overlappingZones(rects).length,
                overlapViolations: entry.overlaps,
                clippingViolations: entry.clipped,
                cutWithoutHint: entry.cutWithoutHint,
            });
            extras.extremes[extreme.name] = { rects, delta: before };
            if (extreme.name === 'explain-max') {
                extras.shots.push(await shootAtRest(page, SHOT_LARGE, 'layout-explain-large'));
            }
        }
        log(`Extremlagen: ${JSON.stringify(report.extremeLayouts)}`);

        // --------------------------- 6j. clear bleibt der einzige Loeschweg
        await page.dblclick('[data-testid="atlas-split-explain"]');
        await page.waitForTimeout(250);
        await chooseTab(page, 'chat');
        const beforeClear = await explainState(page, PANEL_ROOTS);
        await page.click('[data-testid="atlas-chat-clear"]');
        await page.waitForTimeout(400);
        const afterClear = await explainState(page, PANEL_ROOTS);
        extras.chat.clear = {
            before: beforeClear.chatTurns,
            after: afterClear.chatTurns,
            afterEmpty: afterClear.empty.slice(0, 80),
        };
        report.chatTabClearStillClears =
            beforeClear.chatTurns >= 1
            && afterClear.chatTurns === 0
            && afterClear.roots.includes('chat') === false
            && afterClear.empty.length >= 40;
        log(`clear: ${beforeClear.chatTurns} -> ${afterClear.chatTurns} Zuege, `
            + `danach steht der Grund da (${report.chatTabClearStillClears})`);

        report.screenshotsAtRest =
            extras.shots.length === 3 && extras.shots.every((shot) => shot.atRest === true);
        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w8] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w8] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
    const leftovers = [];
    for (const port of [serverPort, uiPort].filter((value) => value > 0)) {
        leftovers.push({ port, listeners: await countListeners(port) });
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
            method:
                'Gemessen wird an den Rechtecken und an den Griffen der Oberflaeche, nicht an '
                + 'Klicks: ein Reiterwechsel gilt als bewiesen, wenn danach genau ein Inhaltskasten '
                + 'im Baum steht und keine zwei Zonen sich schneiden; ein Griff gilt als beweglich, '
                + 'wenn sich nach dem Zug sowohl die Zahl der Oberflaeche als auch das Rechteck der '
                + 'Zone daneben geaendert hat. Die Anschlaege werden gegen die Grenzen geprueft, die '
                + 'die Oberflaeche selbst nennt (aria-valuemin/max, __atlasLayout.bounds), und nicht '
                + 'gegen eine hier abgeschriebene Zahl.',
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = existsSync(SHOT_DEFAULT) && existsSync(SHOT_LARGE) && existsSync(SHOT_CUSTOM);
    const ok =
        failure === null
        && report.zonesNeverOverlap === true
        && report.singleExplainTabVisible === true
        && report.explainTabs.length >= 4
        && report.disabledTabsExplainThemselves === true
        && report.allFourSplittersDrag === true
        && report.splittersKeyboard === true
        && report.splitterDoubleClickResets === true
        && report.layoutPersistsReload === true
        && report.resetLayoutWorks === true
        && report.explainCollapsedOnOpen === true
        && report.explainOpensOnDemand === true
        && report.tabSwitchKeepsState === true
        && report.stateProbes.chatLines >= 1
        && report.minZoneRespected === true
        && report.chatTabKeepsEscape === true
        && report.chatTabHistorySurvives === true
        && report.chatTabClearStillClears === true
        && report.chatHeightFollowsZone === true
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.cutWithoutHint === 0
        && report.extremeLayouts.length >= 2
        && report.extremeLayouts.every((entry) => entry.overlapViolations === 0
            && entry.clippingViolations === 0 && entry.cutWithoutHint === 0 && entry.atBound === true)
        && report.screenshotsAtRest === true
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && shotsOk
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w8] W8-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w8] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W8-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w8] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
