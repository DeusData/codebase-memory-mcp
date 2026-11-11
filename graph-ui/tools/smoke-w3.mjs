#!/usr/bin/env node
/*
 * W3-Smoke: die Galaxie an einem echten Server, und der Fokus in beide
 * Richtungen.
 *
 * Was hier bewiesen wird, koennen die Unit-Tests nicht beweisen. Sie zeigen an
 * erfundenen Knoten, dass ein qualifizierter Name den richtigen Punkt findet,
 * dass Nachbarn Nachbarn sind und dass die Rangfolge der Suche dieselbe ist
 * wie im Referenzprojekt. Sie sagen nichts darueber, ob dieser Server ein
 * Layout liefert, ob ein Browser die Szene ueberhaupt zeichnet, ob ein
 * getipptes Wort in der Fusszeile zu einer Liste wird, ob ein Enter darauf die
 * Datei oeffnet und die Kamera bewegt, und ob ein Klick in die Galaxie
 * zurueckfuehrt in den Reader.
 *
 * Ablauf, wie bei smoke-w2b:
 *   1. `npm run build`
 *   2. isoliertes HOME, fixtures/atlas-sample ueber die CLI indizieren
 *   3. C-Server auf einem freien Port >= 4250 starten
 *   4. dist/ auf einem zweiten Port ausliefern, /rpc und /api dorthin proxen
 *      (auch /api/layout: die Galaxie holt ihr Layout ueber genau diese Route)
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *   6. Klickstrecke: Layout abwarten, "createUser" tippen, Enter, dann einen
 *      Knoten in der Galaxie anklicken
 *   7. abraeumen, Restprozesse zaehlen, verification/w3/galaxy.json schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w3).
 *
 * ## WebGL im Headless-Browser
 *
 * Headless-Chromium hat keine GPU. Ohne Zutun faellt es auf SwiftShader
 * zurueck, und seit Chrome 127 verlangt dieser Rueckfall ein ausdrueckliches
 * Flag. Beide Flags stehen unten in CHROMIUM_ARGS und sind der Grund, warum
 * dieser Lauf headless bleiben kann: `--use-angle=swiftshader` waehlt den
 * Software-Renderer, `--enable-unsafe-swiftshader` erlaubt ihn. Ob er
 * wirklich gegriffen hat, wird nicht vermutet, sondern gemessen: der Bericht
 * traegt den Renderer-String aus WEBGL_debug_renderer_info und die Zahl der
 * nicht schwarzen Pixel des gerenderten Canvas.
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
const PROJECT = 'codeatlasweb-w3';
const OUT_DIR = join(ROOT, 'verification', 'w3');
const OUT_JSON = join(OUT_DIR, 'galaxy.json');
const MIN_PORT = 4250;

const SEARCH_TERM = 'createUser';
const SERVICE_FILE = 'src/services/userService.ts';
const TARGET_FILE = 'src/util/validate.ts';
/** Das Symbol, auf das in der Galaxie geklickt wird, an seinem Namensende. */
const CLICK_TARGET_SUFFIX = 'validate.validateUser';

/** Ueber die Entprellung der Suche hinaus warten (src/search/find-by-meaning.ts). */
const SEARCH_SETTLE_MS = 600;

/**
 * Chromium ohne Aussenwelt. Wortgleich mit smoke-w2b, plus die beiden
 * Software-GL-Flags: die Flags sind die Absicht, das Netz-Deny-Gate daneben
 * ist der Beweis.
 */
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

const log = (...parts) => console.log('[smoke-w3]', ...parts);
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

/**
 * Wie viel auf dem Canvas der Galaxie zu sehen ist.
 *
 * Die Dateigroesse des Screenshots ist ein Indiz und kein Befund: ein
 * schwarzes Bild mit einer vollen Oberflaeche drumherum kann auch gross
 * werden. Also wird der Canvas selbst gelesen. `preserveDrawingBuffer` ist
 * aus (so kommt die Szene aus der Uebernahme), deshalb wird direkt nach einem
 * Frame gelesen: `readPixels` in einem requestAnimationFrame-Rueckruf sieht
 * den Puffer, bevor er verworfen wird.
 */
async function canvasBrightness(page) {
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
        for (let i = 0; i < pixels.length; i += 4) {
            const value = Math.max(pixels[i], pixels[i + 1], pixels[i + 2]);
            if (value > 40) {
                lit += 1;
            }
            if (value > brightest) {
                brightest = value;
            }
        }
        return {
            found: true,
            context: true,
            renderer,
            width,
            height,
            litPixels: lit,
            brightest,
        };
    });
}

async function main() {
    const totalStarted = Date.now();
    let serverChild = null;
    let proxy = null;
    let browser = null;
    let home = null;
    let serverPort = 0;
    let uiPort = 0;
    let failure = null;
    const timings = {};
    const report = {
        searchResultCount: 0,
        searchTopNames: [],
        enterOpenedFile: '',
        twinSubject: '',
        twinSubjectAfterClick: '',
        flyToCount: 0,
        clickOpenedFile: '',
        highlightedCount: 0,
        layoutNodes: 0,
        layoutSource: '/api/layout',
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

        // -------------------------------------------------------- 2. Index
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w3-home-'));
        log('isoliertes HOME:', home);
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // -------------------------------------------------------- 3. Server
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        timings.serverStartMs = started.durationMs;
        log(`C-Server bereit auf ${serverPort} nach ${started.durationMs} ms`);

        // --------------------------------------------------------- 4. Proxy
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        extras.serverPort = serverPort;
        log(`dist/ liegt auf ${uiPort}, /rpc und /api gehen nach ${serverPort}`);

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

        // ------------------------------------------- 6a. Layout abwarten
        await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 20000 });
        await page.waitForFunction(() => (globalThis.__atlasGalaxy?.nodes ?? 0) >= 50, undefined, {
            timeout: 30000,
        });
        report.layoutNodes = await page.evaluate(() => globalThis.__atlasGalaxy?.nodes ?? 0);
        extras.galaxyHeadline = await page.evaluate(
            () =>
                document.querySelector('[data-testid="atlas-galaxy-headline"]')?.textContent?.trim() ?? '',
        );
        log(`Layout geladen: ${report.layoutNodes} Knoten`);

        // Ein paar Frames Zeit, damit die Szene wirklich gezeichnet ist,
        // bevor sie fotografiert und vermessen wird.
        await page.waitForTimeout(1200);
        extras.canvas = await canvasBrightness(page);
        log('Canvas:', JSON.stringify(extras.canvas));

        await mkdir(OUT_DIR, { recursive: true });
        await page.screenshot({ path: join(OUT_DIR, 'galaxy.png'), fullPage: true });
        log('galaxy.png geschrieben (Galaxie ohne Auswahl, ganze Wolke)');

        // ------------------------------------------------ 6b. Suche tippen
        const commandInput = page.locator('[data-testid="atlas-command-input"]');
        await commandInput.click();
        await commandInput.pressSequentially(SEARCH_TERM, { delay: 40 });
        await page.waitForSelector('[data-testid="atlas-search-results"]', { timeout: 10000 });
        await page.waitForFunction(
            () => document.querySelectorAll('[data-testid="atlas-search-row"]').length > 0,
            undefined,
            { timeout: 20000 },
        );
        // Ueber die Entprellung hinaus warten, damit die Liste die fertige ist
        // und nicht die eines halb getippten Wortes.
        await page.waitForTimeout(SEARCH_SETTLE_MS);

        report.searchTopNames = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid="atlas-search-row"]')].map(
                (node) => node.getAttribute('data-name') ?? '',
            ),
        );
        report.searchResultCount = report.searchTopNames.length;
        extras.searchHeadline = await page.evaluate(
            () =>
                document.querySelector('[data-testid="atlas-search-headline"]')?.textContent?.trim() ?? '',
        );
        extras.searchRows = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid="atlas-search-row"]')].map((node) =>
                (node.textContent ?? '').replace(/\s+/g, ' ').trim(),
            ),
        );
        log(`Suchtreffer: ${report.searchTopNames.join(', ')}`);

        await page.screenshot({ path: join(OUT_DIR, 'search.png'), fullPage: true });
        log('search.png geschrieben (Overlay offen)');

        // Wieviel Kamerafahrt es vor dem Enter gab. Ohne diese Zahl waere
        // "es ist geflogen" nicht von "es war schon da" zu unterscheiden.
        extras.flyToBeforeEnter = await page.evaluate(
            () => globalThis.__atlasGalaxy?.targetChanges ?? 0,
        );

        // ----------------------------------------------------- 6c. Enter
        await page.keyboard.press('Enter');
        await page.waitForFunction(
            () => globalThis.__atlasReader?.document?.path === 'src/services/userService.ts',
            undefined,
            { timeout: 20000 },
        );
        await page.waitForFunction(
            () => /userService\.createUser$/.test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            undefined,
            { timeout: 30000 },
        );
        await page.waitForFunction(
            (before) => (globalThis.__atlasGalaxy?.targetChanges ?? 0) > before,
            extras.flyToBeforeEnter,
            { timeout: 20000 },
        );

        const afterEnter = await page.evaluate(() => ({
            breadcrumb: (document.querySelector('[data-testid="atlas-breadcrumb"]')?.textContent ?? '')
                .replace(/\s+/g, ' ')
                .trim(),
            tabs: [...document.querySelectorAll('.atlas-tab')].map((node) => node.getAttribute('data-path')),
            readerPath: globalThis.__atlasReader?.document?.path ?? '',
            twin: globalThis.__atlasTwin?.qualifiedName ?? '',
            twinName: globalThis.__atlasTwin?.symbol ?? '',
            targetChanges: globalThis.__atlasGalaxy?.targetChanges ?? 0,
            highlightedCount: globalThis.__atlasGalaxy?.highlightedCount ?? 0,
            lastTargetQn: globalThis.__atlasGalaxy?.lastTargetQn ?? '',
            commandValue: document.querySelector('[data-testid="atlas-command-input"]')?.value ?? '',
            overlayGone: document.querySelector('[data-testid="atlas-search-results"]') === null,
        }));
        extras.afterEnter = afterEnter;
        // Die geoeffnete Datei wird an zwei Stellen gelesen und nur dann
        // gemeldet, wenn beide dasselbe sagen: die Breadcrumb ist das, was der
        // Leser sieht, der Tab das, was die Anwendung fuehrt.
        const enterFileAgrees =
            afterEnter.tabs.includes(SERVICE_FILE) &&
            afterEnter.breadcrumb.includes('userService.ts') &&
            afterEnter.readerPath === SERVICE_FILE;
        report.enterOpenedFile = enterFileAgrees ? SERVICE_FILE : '';
        report.twinSubject = afterEnter.twin;
        report.highlightedCount = afterEnter.highlightedCount;
        log(`nach Enter: ${report.enterOpenedFile}, Twin ${report.twinSubject}, `
            + `${afterEnter.highlightedCount} Knoten markiert, ${afterEnter.targetChanges} Fahrten`);

        // ------------------------------------- 6d. Klick in die Galaxie
        const clickQn = await page.evaluate(async (project) => {
            const res = await fetch(`/api/layout?project=${project}&max_nodes=5000`);
            const data = await res.json();
            const hit = (data.nodes ?? []).find((node) =>
                (node.qualified_name ?? '').endsWith('validate.validateUser'),
            );
            return hit?.qualified_name ?? '';
        }, PROJECT);
        extras.clickQn = clickQn;
        if (clickQn.length === 0) {
            throw new Error(`kein Knoten mit ${CLICK_TARGET_SUFFIX} im Layout`);
        }

        const clicked = await page.evaluate((qn) => globalThis.__atlasGalaxy?.clickNode(qn) ?? false, clickQn);
        extras.clickAccepted = clicked;
        if (clicked !== true) {
            throw new Error(`clickNode(${clickQn}) hat den Knoten nicht gefunden`);
        }

        await page.waitForFunction(
            () => globalThis.__atlasReader?.document?.path === 'src/util/validate.ts',
            undefined,
            { timeout: 20000 },
        );
        await page.waitForFunction(
            () => /validate\.validateUser$/.test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            undefined,
            { timeout: 30000 },
        );

        const afterClick = await page.evaluate(() => ({
            breadcrumb: (document.querySelector('[data-testid="atlas-breadcrumb"]')?.textContent ?? '')
                .replace(/\s+/g, ' ')
                .trim(),
            tabs: [...document.querySelectorAll('.atlas-tab')].map((node) => node.getAttribute('data-path')),
            readerPath: globalThis.__atlasReader?.document?.path ?? '',
            twin: globalThis.__atlasTwin?.qualifiedName ?? '',
            targetChanges: globalThis.__atlasGalaxy?.targetChanges ?? 0,
            highlightedCount: globalThis.__atlasGalaxy?.highlightedCount ?? 0,
            lastTargetQn: globalThis.__atlasGalaxy?.lastTargetQn ?? '',
        }));
        extras.afterClick = afterClick;
        const clickFileAgrees =
            afterClick.tabs.includes(TARGET_FILE) &&
            afterClick.breadcrumb.includes('validate.ts') &&
            afterClick.readerPath === TARGET_FILE;
        report.clickOpenedFile = clickFileAgrees ? TARGET_FILE : '';
        report.twinSubjectAfterClick = afterClick.twin;
        report.flyToCount = afterClick.targetChanges;
        log(`nach dem Klick: ${report.clickOpenedFile}, Twin ${report.twinSubjectAfterClick}, `
            + `${report.flyToCount} Fahrten`);

        // Ein drittes Bild, das keine Abnahme verlangt und das man trotzdem
        // sehen will: die Galaxie im Fokus. Erst hier beschriftet die Szene,
        // und zwar genau die Nachbarschaft, die gerade gefragt wurde.
        await page.waitForTimeout(1200);
        extras.canvasFocused = await canvasBrightness(page);
        await page.screenshot({ path: join(OUT_DIR, 'galaxy-focus.png'), fullPage: true });
        log('galaxy-focus.png geschrieben (validateUser und Nachbarn, Kamera angefahren)');

        // --------------------------------------- 6e. Der [a]tlas-Schalter
        /*
         * Drei Aussagen in einem Stueck Strecke:
         *
         *  1. Ein `a` in der Kommandozeile ist ein Buchstabe und kein Kuerzel.
         *     Wer "createUser" tippt, tippt ein a mit; klappte dabei die
         *     Galaxie weg, waere das Kuerzel eine Falle.
         *  2. Mit Alt/Option ist dasselbe `a` der Schalter.
         *  3. Nach dem Wiederaufklappen zeichnet dieselbe Szene weiter. Das
         *     ist der Beweis fuer den gehaltenen WebGL-Kontext: waere sie
         *     ausgehaengt worden, stuende hier ein neuer, schwarzer Canvas.
         *
         * Nachgezogen am 2026-08-29 (W7a): das Kuerzel traegt seit dem
         * Nutzerbefund Alt/Option, weil ein blanker Buchstabe in die
         * Kommandozeile gehoert und nicht in ein Panel (src/app/keyboard.ts).
         * Aussage 1 wird dadurch nicht schwaecher, sondern gilt jetzt ueberall:
         * ein blankes `a` schaltet nirgends mehr.
         */
        const galaxyVisible = () =>
            page.evaluate(
                () =>
                    document.querySelector('[data-testid="atlas-galaxy"]')?.getAttribute('data-visible') ===
                    'true',
            );
        await page.locator('[data-testid="atlas-command-input"]').focus();
        await page.keyboard.press('a');
        const visibleWhileTyping = await galaxyVisible();
        await page.keyboard.press('Backspace');

        await page.locator('.atlas-brand').click();
        await page.keyboard.press('Alt+a');
        await page.waitForFunction(
            () =>
                document.querySelector('[data-testid="atlas-galaxy"]')?.getAttribute('data-visible') ===
                'false',
            undefined,
            { timeout: 10000 },
        );
        const hiddenAfterKey = !(await galaxyVisible());

        await page.click('[data-testid="atlas-menu-item"][data-menu="a"]');
        await page.waitForFunction(
            () =>
                document.querySelector('[data-testid="atlas-galaxy"]')?.getAttribute('data-visible') ===
                'true',
            undefined,
            { timeout: 10000 },
        );
        await page.waitForTimeout(800);
        const canvasAfterReopen = await canvasBrightness(page);
        extras.toggle = {
            visibleWhileTyping,
            hiddenAfterKey,
            visibleAfterMenuClick: await galaxyVisible(),
            canvasAfterReopen,
            nodesAfterReopen: await page.evaluate(() => globalThis.__atlasGalaxy?.nodes ?? 0),
        };
        log('Schalter:', JSON.stringify({ ...extras.toggle, canvasAfterReopen: undefined }));

        // ------------------------------------------- 6f. Was noch auffiel
        extras.galaxyNote = await page.evaluate(
            () => document.querySelector('[data-testid="atlas-galaxy-note"]')?.textContent?.trim() ?? '',
        );
        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        /*
         * Was die Entprellung gekostet hat, in Anfragen.
         *
         * Zehn Tastendruecke, und der Server hat zwei Suchen gesehen: eine je
         * Wort der einen fertigen Anfrage ("createUser" wird zu create und
         * user). Ohne die 200 ms waeren es bis zu zwanzig gewesen.
         */
        extras.debounce = {
            typedChars: SEARCH_TERM.length,
            searchGraphCalls: proxy.log.rpcTools['search_graph'] ?? 0,
        };

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w3] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w3] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
    report.leftoverProcesses = leftovers.reduce((sum, value) => sum + value, 0);
    log('leftoverProcesses:', report.leftoverProcesses);

    timings.totalMs = Date.now() - totalStarted;
    report.project = PROJECT;
    report.fixture = 'fixtures/atlas-sample';
    report.searchTerm = SEARCH_TERM;
    report.timings = timings;
    report.extras = extras;
    report.generatedAt = new Date().toISOString();
    report.error = failure ? failure.message : null;

    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(OUT_JSON, JSON.stringify(report, null, 2) + '\n', 'utf8');
    log('geschrieben:', OUT_JSON);

    const ok =
        failure === null &&
        report.searchResultCount >= 1 &&
        report.searchTopNames.includes('createUser') &&
        report.enterOpenedFile === SERVICE_FILE &&
        /userService\.createUser$/.test(report.twinSubject) &&
        report.clickOpenedFile === TARGET_FILE &&
        /validate\.validateUser$/.test(report.twinSubjectAfterClick) &&
        report.flyToCount >= 2 &&
        report.highlightedCount >= 2 &&
        report.layoutNodes >= 50 &&
        report.port >= MIN_PORT &&
        report.leftoverProcesses === 0 &&
        extras.canvas?.litPixels > 0 &&
        extras.toggle?.visibleWhileTyping === true &&
        extras.toggle?.hiddenAfterKey === true &&
        extras.toggle?.visibleAfterMenuClick === true &&
        extras.toggle?.canvasAfterReopen?.litPixels > 0 &&
        extras.blockedRequests.length === 0 &&
        extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w3] W3-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w3] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    if (home) {
        await rm(home, { recursive: true, force: true });
    }
    log('W3-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w3] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
