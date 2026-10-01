#!/usr/bin/env node
/*
 * W4e-Smoke: die Hierarchie des Vorwaerts-Walks, in einem echten Browser.
 *
 * Was hier bewiesen wird, koennen die Unit-Tests nicht beweisen. Sie zeigen an
 * einem erfundenen Walk, dass die Projektion zweimal dasselbe Bild baut, dass
 * die Spalte am Hop haengt und dass keine Kante verschwindet. Sie sagen nichts
 * darueber, ob dieser Server einen Walk liefert, der tief genug fuer eine
 * Hierarchie ist, ob das Panel beim Waehlen eines Einstiegspunkts von selbst
 * umschaltet, ob DIESELBE Szene die Projektion zeichnet, ohne ihren
 * WebGL-Kontext wegzuwerfen, ob der Ring dem Schritt folgt und ob der Kopf
 * dabei die Wahrheit sagt.
 *
 * Ablauf, wie bei smoke-w4a und smoke-w4d:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis
 *   3. fixtures/atlas-sample indizieren (nur gelesen, nichts praepariert)
 *   4. C-Server auf einem freien Port >= 4330, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre, plus die beiden
 *      Software-GL-Flags
 *   6a. frische App: das Panel steht auf galaxy und sagt, dass ihm ein Walk
 *       fehlt
 *   6b. Einstiegsmodus, createUser waehlen: das Panel schaltet von selbst um
 *   6c. die Projektion vermessen: Knoten, Ebenen, Wurzel, Spalten je Hop
 *   6d. den Canvas markieren und fotografieren
 *   6e. Enter: der Ring wandert auf einen anderen Knoten
 *   6f. Klick auf einen Knoten: Reader und Twin folgen
 *   6g. Chip galaxy: der Zaehler-Kopf ist zurueck, und es ist derselbe Canvas
 *       mit einem lebenden Kontext
 *   6h. der Deckel-Beweis: derselbe Walk mit ?codeatlasClosureCap=3, der Kopf
 *       muss den Deckel nennen
 *   7. abraeumen, Restprozesse zaehlen, JSON und Screenshot schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w4e).
 *
 * ## Drei Entscheidungen, die man sonst raten muesste
 *
 * **Der gleiche-Canvas-Beweis ist eine Markierung, kein Vergleich von Bildern.**
 * Vor dem Umschalten bekommt das Canvas-Element ein Attribut und eine Referenz
 * in einer globalen Variable; nach dem Umschalten muessen beide noch am selben
 * Element haengen und der WebGL-Kontext darf nicht verloren sein. Zwei
 * Screenshots zu vergleichen wuerde messen, ob sich das Bild geaendert hat, und
 * das soll es ja.
 *
 * **Der Kopf wird zweimal gelesen, mit und ohne Deckel.** Ein Satz ueber eine
 * Grenze, den man nie sieht, ist kein bewiesener Satz, und einer, der immer
 * dasteht, ist keine Aussage. Also einmal mit den Vorgaben, wo er FEHLEN muss,
 * und einmal mit `?codeatlasClosureCap=3`, wo er stehen und seine Zahlen nennen
 * muss. Dieselbe Beweisform wie in smoke-w4a.
 *
 * **CBM_RUNTIME_DIR wird gesetzt.** Der Daemon des Servers und jede CLI
 * verabreden sich in einem Rendezvous-Verzeichnis, das per Konto und nicht per
 * HOME gilt: laeuft irgendwo sonst auf der Maschine eine CBM-Instanz mit einem
 * anderen Cache-Verzeichnis, lehnt jede CLI dieses Laufs ab, und der Lauf waere
 * nicht rot, sondern kaputt. Wortgleich mit tools/smoke-w4b.mjs und -w4d.
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
const PROJECT = 'codeatlasweb-w4e';
const OUT_DIR = join(ROOT, 'verification', 'w4');
const OUT_JSON = join(OUT_DIR, 'hierarchy.json');
const SHOT = join(OUT_DIR, 'hierarchy.png');
const MIN_PORT = 4330;

/** Das Symbol, ab dem der Walk laeuft. Dasselbe wie in smoke-w4a. */
const ENTRY_SYMBOL = 'createUser';
const ENTRY_FILE = 'src/services/userService.ts';

/** Der Deckel, mit dem der zweite Walk gefahren wird. Klein genug, dass er greift. */
const SMALL_CAP = 3;

/** Chromium ohne Aussenwelt, plus die beiden Software-GL-Flags aus smoke-w3. */
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

const log = (...parts) => console.log('[smoke-w4e]', ...parts);
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
            process.stdout.write(d);
        });
        child.stderr.on('data', (d) => {
            out += d.toString();
            process.stderr.write(d);
        });
        child.on('error', (error) => done({ code: 127, out: out + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out }));
    });
}

// ------------------------------------------------------------- Testgriffe ---

/** Alles, was das Graph-Panel gerade ueber sich sagt. */
const graphSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasGalaxy;
        return seam === undefined
            ? null
            : {
                mode: seam.mode,
                hierarchyAvailable: seam.hierarchyAvailable,
                headline: seam.headline,
                pulsedQn: seam.pulsedQn,
                nodes: seam.nodes,
                highlightedCount: seam.highlightedCount,
                targetChanges: seam.targetChanges,
                legendEntries: seam.legendEntries,
                hierarchy: seam.hierarchy ?? null,
            };
    });

/** Was im Kopf des Panels steht, und welcher Chip gedrueckt ist. */
const headSeam = (page) =>
    page.evaluate(() => ({
        headline:
            document.querySelector('[data-testid="atlas-galaxy-headline"]')?.textContent
                ?.replace(/\s+/g, ' ').trim() ?? '',
        panelMode: document.querySelector('[data-testid="atlas-galaxy"]')?.getAttribute('data-mode') ?? '',
        groupPresent: document.querySelector('[data-testid="atlas-graph-mode"]') !== null,
        chips: [...document.querySelectorAll('[data-testid="atlas-graph-mode-chip"]')].map((chip) => ({
            mode: chip.getAttribute('data-mode'),
            active: chip.getAttribute('data-active'),
            pressed: chip.getAttribute('aria-pressed'),
            disabled: chip.disabled === true,
            title: chip.getAttribute('title') ?? '',
        })),
        legend: [...document.querySelectorAll('[data-testid="atlas-galaxy-legend-entry"]')].map((entry) => ({
            key: entry.getAttribute('data-entry'),
            text: entry.textContent?.replace(/\s+/g, ' ').trim() ?? '',
        })),
        pulseRings: [...document.querySelectorAll('[data-testid="atlas-hierarchy-pulse"]')].map((ring) =>
            ring.getAttribute('data-qn') ?? ''),
        note: document.querySelector('[data-testid="atlas-galaxy-note"]')?.textContent?.trim() ?? '',
    }));

/** Wo Reader und Twin gerade stehen. Wortgleich mit smoke-w4a. */
const readerSeam = (page) =>
    page.evaluate(() => ({
        readerPath: globalThis.__atlasReader?.document?.path ?? '',
        twin: globalThis.__atlasTwin?.symbol ?? '',
        twinQn: globalThis.__atlasTwin?.qualifiedName ?? '',
    }));

/** Der Griff der Fuehrung. */
const tourSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasTour;
        return seam === undefined
            ? null
            : { kind: seam.kind, steps: seam.steps, index: seam.index, titles: seam.titles };
    });

/**
 * Was auf dem Canvas wirklich zu sehen ist.
 *
 * Wortgleiche Technik wie in smoke-w3: `preserveDrawingBuffer` ist aus, also
 * wird direkt in einem requestAnimationFrame-Rueckruf gelesen. Zusaetzlich wird
 * hier gezaehlt, in wie vielen getrennten senkrechten Baendern helle Pixel
 * liegen: eine Hierarchie mit drei Ebenen soll auch als Bild mehrere Spalten
 * haben und nicht eine Wolke sein.
 */
async function canvasReading(page) {
    return page.evaluate(async () => {
        const canvas = document.querySelector('.atlas-galaxy-scene canvas');
        if (canvas === null) {
            return { found: false };
        }
        const gl = canvas.getContext('webgl2') ?? canvas.getContext('webgl');
        if (gl === null) {
            return { found: true, context: false };
        }
        const debug = gl.getExtension('WEBGL_debug_renderer_info');
        const renderer = debug === null ? '' : String(gl.getParameter(debug.UNMASKED_RENDERER_WEBGL));
        const width = gl.drawingBufferWidth;
        const height = gl.drawingBufferHeight;
        const pixels = await new Promise((done) => {
            requestAnimationFrame(() => {
                const buffer = new Uint8Array(width * height * 4);
                gl.readPixels(0, 0, width, height, gl.RGBA, gl.UNSIGNED_BYTE, buffer);
                done(buffer);
            });
        });
        let lit = 0;
        let brightest = 0;
        const perColumn = new Array(width).fill(0);
        for (let i = 0; i < pixels.length; i += 4) {
            const value = Math.max(pixels[i], pixels[i + 1], pixels[i + 2]);
            if (value > 40) {
                lit += 1;
            }
            if (value > 120) {
                perColumn[(i / 4) % width] += 1;
            }
            if (value > brightest) {
                brightest = value;
            }
        }
        let bands = 0;
        let inBand = false;
        for (const count of perColumn) {
            const on = count > 2;
            if (on && !inBand) {
                bands += 1;
            }
            inBand = on;
        }
        return {
            found: true,
            context: true,
            contextLost: gl.isContextLost(),
            renderer,
            width,
            height,
            litPixels: lit,
            brightest,
            brightColumnBands: bands,
        };
    });
}

/** Den Canvas markieren, damit "derselbe Canvas" spaeter pruefbar ist. */
const markCanvas = (page) =>
    page.evaluate(() => {
        const canvas = document.querySelector('.atlas-galaxy-scene canvas');
        if (canvas === null) {
            return false;
        }
        canvas.dataset.w4eMark = 'the-one-canvas';
        globalThis.__w4eCanvas = canvas;
        return true;
    });

/** Ob es noch derselbe Canvas mit einem lebenden Kontext ist. */
const canvasIdentity = (page) =>
    page.evaluate(() => {
        const all = [...document.querySelectorAll('.atlas-galaxy-scene canvas')];
        const canvas = all[0] ?? null;
        const gl = canvas === null ? null : (canvas.getContext('webgl2') ?? canvas.getContext('webgl'));
        return {
            count: all.length,
            sameElement: canvas !== null && globalThis.__w4eCanvas === canvas,
            mark: canvas?.dataset?.w4eMark ?? '',
            hasContext: gl !== null,
            contextLost: gl === null ? true : gl.isContextLost(),
        };
    });

// ------------------------------------------------------------- Klickstrecke -

/**
 * Eine Taste ans Fenster geben, ohne dass ein Eingabefeld sie schluckt.
 * Wortgleich mit smoke-w4a.
 */
async function pressGlobally(page, key) {
    await page.click('.atlas-brand');
    await page.keyboard.press(key);
}

/** Die Frage wieder aufrufen und einen Modus waehlen. */
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
    // Ueber die Entprellung hinaus, damit die Liste die eines fertigen Wortes ist.
    await page.waitForTimeout(600);
    await page.click(`[data-testid="atlas-entry-hit"][data-name="${name}"]`);
    await page.waitForFunction(() => globalThis.__atlasTour?.kind === 'entry', undefined, {
        timeout: 60000,
    });
    await page.waitForFunction(
        () => (globalThis.__atlasGalaxy?.hierarchy?.nodes ?? 0) > 0,
        undefined,
        { timeout: 60000 },
    );
}

/** Die Seite laden und warten, bis Statusleiste und Galaxie stehen. */
async function openApp(page, origin, query = '') {
    await page.goto(`${origin}/?project=${PROJECT}${query}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 30000 });
    await page.waitForFunction(() => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0, undefined, {
        timeout: 40000,
    });
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
        modeAutoSwitches: false,
        hierarchyNodes: 0,
        hierarchyDepth: 0,
        rootIsChosen: false,
        columnsMatchHops: false,
        stepPulseFollowsTour: false,
        clickFollows: false,
        toggleBackToGalaxy: false,
        headerHonest: false,
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
            throw new Error(`npm run build endete mit ${build.code}`);
        }
        if (!existsSync(join(DIST, 'index.html'))) {
            throw new Error('dist/index.html fehlt nach dem Build');
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w4e-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w4e-run-');
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
        const context = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
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

        // ------------------------------- 6a. Frische App: noch kein Walk
        await openApp(page, origin);
        const beforeWalk = await graphSeam(page);
        const headBeforeWalk = await headSeam(page);
        extras.beforeWalk = { seam: beforeWalk, head: headBeforeWalk };
        log(`vor dem Walk: mode=${beforeWalk.mode}, Kopf "${headBeforeWalk.headline.slice(0, 70)}"`);

        // ------------------------------ 6b. Einstiegsmodus, createUser waehlen
        await openWhyAndChoose(page, 'entry');
        await chooseEntryHit(page, ENTRY_SYMBOL);
        const afterWalk = await graphSeam(page);
        const headAfterWalk = await headSeam(page);
        extras.afterWalk = { seam: afterWalk, head: headAfterWalk };
        extras.entryTour = await tourSeam(page);

        // Kein Klick auf einen Chip bis hierher: was jetzt dasteht, hat das
        // Panel selbst entschieden.
        result.modeAutoSwitches =
            beforeWalk.mode === 'galaxy'
            && beforeWalk.hierarchyAvailable === false
            && afterWalk.mode === 'hierarchy'
            && afterWalk.hierarchyAvailable === true
            && headAfterWalk.panelMode === 'hierarchy'
            && headAfterWalk.groupPresent === true
            && headAfterWalk.chips.some((chip) => chip.mode === 'hierarchy' && chip.pressed === 'true');
        log(`Umschalten von selbst: ${result.modeAutoSwitches} (${beforeWalk.mode} -> ${afterWalk.mode})`);

        // --------------------------------------- 6c. Die Projektion vermessen
        const hierarchy = afterWalk.hierarchy;
        result.hierarchyNodes = hierarchy?.nodes ?? 0;
        result.hierarchyDepth = hierarchy?.depth ?? 0;
        result.rootIsChosen =
            hierarchy !== null
            && hierarchy.rootName === ENTRY_SYMBOL
            && new RegExp(`\\.${ENTRY_SYMBOL}$`).test(hierarchy.root)
            && (hierarchy.placements.find((placement) => placement.key === hierarchy.root)?.hop === 0);
        extras.hierarchy = hierarchy;

        // Spalten je Hop: alle Knoten eines Hops teilen genau ein x, zwei Hops
        // teilen nie eines, und die Wurzel steht ganz links.
        const byHop = new Map();
        for (const placement of hierarchy?.placements ?? []) {
            byHop.set(placement.hop, [...(byHop.get(placement.hop) ?? []), placement.x]);
        }
        const hops = [...byHop.keys()].sort((a, b) => a - b);
        const columnOf = new Map(hops.map((hop) => [hop, byHop.get(hop)[0]]));
        const oneXPerHop = hops.every((hop) => byHop.get(hop).every((x) => x === columnOf.get(hop)));
        const distinctColumns = new Set([...columnOf.values()]).size === hops.length;
        const risesWithHop = hops.every((hop, at) => at === 0 || columnOf.get(hop) > columnOf.get(hops[at - 1]));
        const rootLeftmost = columnOf.get(0) === Math.min(...columnOf.values());
        result.columnsMatchHops =
            hops.length >= 2 && oneXPerHop && distinctColumns && risesWithHop && rootLeftmost;
        extras.columns = {
            hops,
            columnOf: Object.fromEntries(columnOf),
            oneXPerHop,
            distinctColumns,
            risesWithHop,
            rootLeftmost,
        };
        log(`Projektion: ${result.hierarchyNodes} Knoten, ${result.hierarchyDepth} Ebenen, `
            + `Spalten ${JSON.stringify(Object.fromEntries(columnOf))}`);

        // ------------------------------- 6d. Der Canvas, markiert und im Bild
        /*
         * Die Legende wird fuer das Beweisbild zugeklappt und danach wieder
         * aufgeklappt. Sie nimmt in einem 420 Pixel hohen Panel die Haelfte des
         * Platzes, und ein Bild, auf dem die Spalten in einen 190 Pixel hohen
         * Streifen gequetscht sind, beweist die Spaltenstruktur nicht. Ihre
         * Zeilen sind vorher gelesen, also geht dabei keine Aussage verloren;
         * dass der Kontext ein Zuklappen samt Groessenaenderung uebersteht, ist
         * nebenbei ein zweiter Beleg fuer denselben Canvas.
         */
        /*
         * Erst aufklappen, dann lesen.
         *
         * Seit W5c ist die Legende per Vorgabe ZU (das Panel ist 420 Pixel hoch
         * und die aufgeklappte Legende nahm fast die Haelfte davon). Dieser Lauf
         * las ihre Zeilen aber weiter an der Stelle, an der er sie frueher
         * offen vorfand, und bekam seither eine leere Liste: `legendSwapped`
         * war rot, ohne dass an der Legende etwas fehlte. Der Widerspruch
         * zwischen beiden Laeufen loest sich hier und nicht in der Zusicherung:
         * die Vorgabe bleibt zu, und wer die Zeilen braucht, macht sie auf, so
         * wie ein Leser es tut. Der Klick danach klappt sie fuer das Beweisbild
         * wieder zu, genau wie vorher.
         */
        if (await page.getAttribute('[data-testid="atlas-galaxy-legend-toggle"]', 'aria-expanded') !== 'true') {
            await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
            await page.waitForSelector('[data-testid="atlas-galaxy-legend"]', { timeout: 10000 });
            await page.waitForTimeout(250);
        }
        const legendInHierarchy = (await headSeam(page)).legend;
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        // Ein paar Frames Zeit, damit die Kamera ihren Anflug fertig hat und die
        // Szene wirklich gezeichnet ist, bevor sie vermessen wird.
        await page.waitForTimeout(2500);
        extras.canvasInHierarchy = await canvasReading(page);
        extras.canvasMarked = await markCanvas(page);
        log('Canvas (hierarchy):', JSON.stringify(extras.canvasInHierarchy));

        await page.screenshot({ path: SHOT, fullPage: true });
        log('hierarchy.png geschrieben');

        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(500);

        // ---------------------------------------- 6e. Der Ring folgt dem Schritt
        const pulseBefore = (await graphSeam(page)).pulsedQn;
        const ringsBefore = (await headSeam(page)).pulseRings;
        await pressGlobally(page, 'Enter');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 1, undefined, { timeout: 30000 });
        await page.waitForFunction(
            (previous) => {
                const now = globalThis.__atlasGalaxy?.pulsedQn ?? '';
                return now.length > 0 && now !== previous;
            },
            pulseBefore,
            { timeout: 60000 },
        );
        await page.waitForTimeout(400);
        const pulseAfter = (await graphSeam(page)).pulsedQn;
        const ringsAfter = (await headSeam(page)).pulseRings;
        const walkKeys = new Set((hierarchy?.placements ?? []).map((placement) => placement.key));
        result.stepPulseFollowsTour =
            pulseBefore.length > 0
            && pulseAfter.length > 0
            && pulseAfter !== pulseBefore
            && walkKeys.has(pulseBefore)
            && walkKeys.has(pulseAfter)
            && ringsBefore.length === 1
            && ringsAfter.length === 1
            && ringsBefore[0] === pulseBefore
            && ringsAfter[0] === pulseAfter;
        extras.pulse = { before: pulseBefore, after: pulseAfter, ringsBefore, ringsAfter };
        log(`Ring: ${pulseBefore} -> ${pulseAfter}`);

        // ----------------------------------------- 6f. Ein Klick in das Bild
        const standing = await readerSeam(page);
        const clickTarget = (hierarchy?.placements ?? [])
            .filter((placement) =>
                placement.file.length > 0
                && placement.key !== pulseAfter
                && placement.file !== standing.readerPath)
            .sort((a, b) => b.hop - a.hop)[0];
        if (clickTarget === undefined) {
            throw new Error('kein Knoten der Projektion mit Datei ausserhalb der offenen Datei gefunden');
        }
        extras.click = { standing, target: clickTarget };
        const clicked = await page.evaluate(
            (key) => globalThis.__atlasGalaxy?.clickNode(key) ?? false,
            clickTarget.key,
        );
        await page.waitForFunction(
            (expected) => (globalThis.__atlasReader?.document?.path ?? '') === expected,
            clickTarget.file,
            { timeout: 40000 },
        );
        await page.waitForFunction(
            (expected) => (globalThis.__atlasTwin?.qualifiedName ?? '') === expected,
            clickTarget.key,
            { timeout: 40000 },
        );
        const afterClick = await readerSeam(page);
        extras.click.after = afterClick;
        result.clickFollows =
            clicked === true
            && afterClick.readerPath === clickTarget.file
            && afterClick.twinQn === clickTarget.key
            && afterClick.readerPath !== standing.readerPath;
        log(`Klick auf ${clickTarget.name}: Reader ${standing.readerPath} -> ${afterClick.readerPath}`);

        // ------------------------------------- 6g. Zurueck auf die Galaxie
        await page.click('[data-testid="atlas-graph-mode-chip"][data-mode="galaxy"]');
        await page.waitForFunction(() => globalThis.__atlasGalaxy?.mode === 'galaxy', undefined, {
            timeout: 20000,
        });
        await page.waitForTimeout(800);
        const headInGalaxy = await headSeam(page);
        const seamInGalaxy = await graphSeam(page);
        const identity = await canvasIdentity(page);
        const canvasInGalaxy = await canvasReading(page);
        extras.backToGalaxy = {
            head: headInGalaxy,
            seam: seamInGalaxy,
            identity,
            canvas: canvasInGalaxy,
            legendInHierarchy,
        };
        const galaxyCounters = /\d+ nodes, \d+ edges from \/api\/layout/;
        result.toggleBackToGalaxy =
            seamInGalaxy.mode === 'galaxy'
            && seamInGalaxy.hierarchyAvailable === true
            && galaxyCounters.test(headInGalaxy.headline)
            && !/hierarchy of /.test(headInGalaxy.headline)
            && headInGalaxy.pulseRings.length === 0
            && identity.count === 1
            && identity.sameElement === true
            && identity.mark === 'the-one-canvas'
            && identity.contextLost === false
            && canvasInGalaxy.litPixels > 0;
        log(`zurueck auf galaxy: Kopf "${headInGalaxy.headline.slice(0, 70)}", `
            + `derselbe Canvas ${identity.sameElement}, Kontext verloren ${identity.contextLost}`);

        // Die Legende muss im hierarchy-Modus die angepassten Zeilen gezeigt
        // haben und im galaxy-Modus wieder die alten.
        const positionsInHierarchy =
            legendInHierarchy.find((entry) => entry.key === 'positions')?.text ?? '';
        const positionsInGalaxy =
            headInGalaxy.legend.find((entry) => entry.key === 'positions')?.text ?? '';
        extras.legend = {
            hierarchyEntries: legendInHierarchy.length,
            galaxyEntries: headInGalaxy.legend.length,
            positionsInHierarchy,
            positionsInGalaxy,
        };
        const legendSwapped =
            legendInHierarchy.length >= 3
            && /call depth from the entry point/.test(positionsInHierarchy)
            && /not the server layout/.test(positionsInHierarchy)
            && /computed by the server/.test(positionsInGalaxy);
        extras.legendSwapped = legendSwapped;
        log(`Legende getauscht: ${legendSwapped}`);

        // ---------------------------------------------- 6h. Der Deckel-Beweis
        const defaultHead = headAfterWalk.headline;
        const shape = new RegExp(`^hierarchy of ${ENTRY_SYMBOL}: (\\d+) symbols?, depth (\\d+)$`);
        const match = shape.exec(defaultHead);
        const defaultHonest =
            match !== null
            && Number(match[1]) === result.hierarchyNodes
            && Number(match[2]) === result.hierarchyDepth
            && !/walk capped/.test(defaultHead);

        await openApp(page, origin, `&codeatlasClosureCap=${SMALL_CAP}`);
        await openWhyAndChoose(page, 'entry');
        await chooseEntryHit(page, ENTRY_SYMBOL);
        const cappedSeam = await graphSeam(page);
        const cappedHead = await headSeam(page);
        extras.capped = { seam: cappedSeam, head: cappedHead };
        const capSentence = new RegExp(`walk capped at ${SMALL_CAP} symbols \\(depth \\d+\\)`);
        const cappedHonest =
            cappedSeam.mode === 'hierarchy'
            && cappedSeam.hierarchy?.truncated === true
            && cappedSeam.hierarchy?.cap === SMALL_CAP
            && cappedSeam.hierarchy?.nodes === SMALL_CAP
            && new RegExp(`^hierarchy of ${ENTRY_SYMBOL}: ${SMALL_CAP} symbols, depth \\d+;`)
                .test(cappedHead.headline)
            && capSentence.test(cappedHead.headline);
        result.headerHonest = defaultHonest && cappedHonest;
        extras.header = { defaultHead, defaultHonest, cappedHead: cappedHead.headline, cappedHonest };
        log(`Kopf ohne Deckel: "${defaultHead}"`);
        log(`Kopf mit Deckel:  "${cappedHead.headline}"`);

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w4e] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w4e] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
            entrySymbol: ENTRY_SYMBOL,
            entryFile: ENTRY_FILE,
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotOk = existsSync(SHOT);
    const ok =
        failure === null
        && result.modeAutoSwitches === true
        && result.hierarchyNodes >= 4
        && result.hierarchyDepth >= 2
        && result.rootIsChosen === true
        && result.columnsMatchHops === true
        && result.stepPulseFollowsTour === true
        && result.clickFollows === true
        && result.toggleBackToGalaxy === true
        && result.headerHonest === true
        && extras.legendSwapped === true
        && result.port >= MIN_PORT
        && result.leftoverProcesses === 0
        && shotOk
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w4e] W4e-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w4e] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W4e-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w4e] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
