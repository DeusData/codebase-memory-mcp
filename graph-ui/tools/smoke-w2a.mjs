#!/usr/bin/env node
/*
 * W2a-Smoke: das Chrome, der Baum und der Reader an einem echten Server.
 *
 * Was hier bewiesen wird, kann kein Unit-Test beweisen: dass die gebaute Seite
 * aus dist/ in einem echten Browser ein CODEATLAS-Terminal zeigt, dass der Baum
 * aus /api/tree kommt und nicht aus einer Attrappe, dass ein Klick auf eine
 * Datei echten Quelltext vom Server in einen read-only Monaco bringt, und dass
 * eine Datei, die der Server abschneidet, im Reader als abgeschnitten
 * dasteht.
 *
 * Ablauf:
 *   1. `npm run build` (schreibt den Versions-Chip zur Buildzeit hinein)
 *   2. isoliertes HOME, beide Fixtures ueber die CLI indizieren
 *   3. C-Server auf einem freien Port >= 4230 starten
 *   4. dist/ auf einem zweiten Port ausliefern, /rpc und /api dorthin proxen
 *      (der Server nimmt keinen fremden Origin an, siehe tools/lib/static-proxy.mjs)
 *   5. Chromium, mit abgeklemmter Namensaufloesung und einer Route-Sperre, die
 *      jede Anfrage ausserhalb des eigenen Ursprungs abbricht und mitschreibt
 *   6. Klickstrecke: Baum lesen, Ordner auf und zu, Datei oeffnen, Editor
 *      pruefen, dagegen tippen, Screenshots
 *   7. grosse Datei: Kappung im Reader messen, Fenster-Semantik direkt an /rpc
 *   8. abraeumen, Restprozesse zaehlen, verification/w2/reader.json schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w2a).
 */

import { spawn } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
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
const FIXTURE_SMALL = join(ROOT, 'fixtures', 'atlas-sample');
const FIXTURE_LARGE = join(ROOT, 'fixtures', 'atlas-sample-large');
const PROJECT_SMALL = 'codeatlasweb-w2a';
const PROJECT_LARGE = 'codeatlasweb-w2a-large';
const OUT_DIR = join(ROOT, 'verification', 'w2');
const OUT_JSON = join(OUT_DIR, 'reader.json');
const MIN_PORT = 4230;

const OPEN_FILE = 'src/services/userService.ts';
const LARGE_FILE = 'src/big.ts';

/** Die Testmarken, die tests/scaffold/w2a.test.mjs im Chrome sehen will. */
const REQUIRED_TESTIDS = [
    'atlas-header',
    'atlas-menu',
    'atlas-tabs',
    'atlas-command',
    'atlas-statusbar',
    'atlas-tree',
    'atlas-breadcrumb',
];

/**
 * Chromium ohne Aussenwelt.
 *
 * `--host-resolver-rules` ist der harte Riegel: jeder Name ausser Loopback
 * loest ins Leere auf, also kann selbst ein Dienst des Browsers, den keine
 * Option abschaltet, nichts erreichen. Der Rest schaltet die bekannten
 * Hintergrunddienste ab, damit erst gar nichts versucht wird. Das Netz-Deny-Gate
 * beobachtet daneben unabhaengig weiter: die Flags sind die Absicht, das Gate
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
];

const log = (...parts) => console.log('[smoke-w2a]', ...parts);
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

/** Ein MCP-Werkzeug direkt am Server rufen, ohne Browser dazwischen. */
async function callTool(port, name, args) {
    const response = await fetch(`http://127.0.0.1:${port}/rpc`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name, arguments: args } }),
    });
    const body = await response.json();
    if (body.error) {
        throw new Error(`${name}: ${JSON.stringify(body.error)}`);
    }
    const text = body.result?.content?.[0]?.text ?? '';
    try {
        return JSON.parse(text);
    } catch {
        return { raw: text };
    }
}

const rowSelector = (path) => `[data-testid="atlas-tree-row"][data-path="${path}"]`;

/**
 * Ein geladenes Dokument fuer das Artefakt, ohne seinen Quelltext.
 *
 * Der Quelltext gehoert nicht ins Beweisartefakt: er steht schon im Fixture,
 * und 500 Zeilen generierter Code wuerden die Zahlen begraben, um die es hier
 * geht. Was bleibt, ist alles, was eine Aussage traegt, plus die Laenge als
 * Anker.
 */
function documentSummary(doc) {
    if (doc === null || doc === undefined) {
        return null;
    }
    const { source, ...rest } = doc;
    return {
        ...rest,
        sourceChars: source.length,
        sourceLines: source.split('\n').length,
        sourceFirstLine: source.split('\n')[0] ?? '',
    };
}

/** Der Zustand eines Ordners, abgelesen an seinem Pfeil-Zeichen. */
async function twistyOf(page, path) {
    return (await page.locator(`${rowSelector(path)} .atlas-tree-twisty`).textContent())?.trim() ?? '';
}

/**
 * Einen Ordner offen haben, ohne ihn zuzuklappen, wenn er schon offen ist.
 *
 * Die Oberflaeche klappt den Baum beim Laden auf (siehe EAGER_LEVEL_BUDGET in
 * src/App.tsx). Ein blindes Klicken auf jeden Ordner der Strecke wuerde ihn
 * also schliessen. Was hier zurueckkommt, sagt, was wirklich passiert ist.
 */
async function ensureExpanded(page, path) {
    const before = await twistyOf(page, path);
    if (before === '▾') {
        return 'already open';
    }
    await page.click(rowSelector(path));
    await page.waitForFunction(
        (selector) => document.querySelector(selector)?.textContent?.trim() === '▾',
        `${rowSelector(path)} .atlas-tree-twisty`,
        { timeout: 10000 },
    );
    return 'clicked open';
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
        testids: [],
        brandText: '',
        versionChip: '',
        menuText: '',
        commandPlaceholder: '',
        treeEntries: 0,
        treeSource: '',
        openedFile: '',
        readerContainsCreateUser: false,
        readerReadOnly: false,
        editAttemptChangedContent: true,
        breadcrumb: '',
        tabOpened: false,
        rpcTool: '',
        windowSemantics: '',
        truncationHonest: false,
        largeFileLines: 0,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = { blockedRequests: [], consoleErrors: [], pageErrors: [] };

    try {
        if (!existsSync(BINARY)) {
            throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
        }
        for (const fixture of [FIXTURE_SMALL, FIXTURE_LARGE]) {
            if (!existsSync(fixture)) {
                throw new Error(`Fixture fehlt: ${fixture}`);
            }
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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w2a-home-'));
        /*
         * Eigenes Rendezvous-Verzeichnis, nachgezogen am 2026-08-29 (W7a).
         *
         * Der Daemon des Servers und jede CLI verabreden sich in einem
         * Verzeichnis, das per Konto und nicht per HOME gilt. Laeuft auf der
         * Maschine sonst noch eine CBM-Instanz mit einem anderen
         * Cache-Verzeichnis (eine Vorschau zum Beispiel), lehnt jede CLI dieses
         * Laufs ab, und der Lauf waere nicht rot, sondern kaputt. Wortgleich mit
         * tools/smoke-w4b.mjs und tools/smoke-w6-full.mjs.
         */
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w2a-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);
        log('Rendezvous:', runtimeDir);
        const indexedSmall = await indexRepository(BINARY, {
            home,
            repoPath: FIXTURE_SMALL,
            project: PROJECT_SMALL,
        });
        const indexedLarge = await indexRepository(BINARY, {
            home,
            repoPath: FIXTURE_LARGE,
            project: PROJECT_LARGE,
        });
        extras.indexed = {
            small: { nodes: indexedSmall.nodes, edges: indexedSmall.edges },
            large: { nodes: indexedLarge.nodes, edges: indexedLarge.edges },
        };
        log(`indiziert: klein ${indexedSmall.nodes}/${indexedSmall.edges}, gross ${indexedLarge.nodes}/${indexedLarge.edges}`);

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

        // Die Route-Sperre: was nicht zum eigenen Ursprung geht, geht gar nicht,
        // und steht danach im Artefakt.
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

        // ------------------------------------------ 6. Klickstrecke, klein
        await page.goto(`${origin}/?project=${PROJECT_SMALL}`, { waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 20000 });
        // Warten, bis der Baum fertig aufgeklappt ist: die Ebenen kommen eine
        // Anfrage nach der anderen, und die erste Zeile ist noch nicht der Baum.
        await page.waitForFunction(
            () => document.querySelectorAll('[data-testid="atlas-tree-row"]').length >= 10,
            undefined,
            { timeout: 20000 },
        );

        report.testids = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid]')].map((node) => node.getAttribute('data-testid')),
        );
        report.brandText = (await page.locator('.atlas-brand').textContent())?.trim() ?? '';
        report.versionChip = (await page.locator('[data-testid="atlas-version"]').textContent())?.trim() ?? '';
        report.menuText = (await page.locator('[data-testid="atlas-menu"]').innerText()).replace(/\s+/g, ' ').trim();
        report.commandPlaceholder =
            (await page.locator('[data-testid="atlas-command-input"]').getAttribute('placeholder')) ?? '';
        report.treeEntries = await page.locator('[data-testid="atlas-tree-row"]').count();

        extras.headerChips = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid="atlas-header"] .atlas-chip')].map((node) =>
                node.textContent?.trim(),
            ),
        );
        extras.statusBar = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid="atlas-statusbar"] .atlas-chip')].map((node) =>
                node.textContent?.trim(),
            ),
        );
        extras.treeNote = (await page.locator('.atlas-tree-note').textContent())?.trim() ?? '';

        // Beweis, dass ein Ordner sich wirklich auf- und zuklappen laesst: das
        // Aufklappen beim Laden allein zeigt das nicht.
        await page.click(rowSelector('test'));
        await page.waitForSelector(rowSelector('test/userService.test.ts'), { state: 'detached', timeout: 10000 });
        await page.click(rowSelector('test'));
        await page.waitForSelector(rowSelector('test/userService.test.ts'), { timeout: 10000 });
        extras.folderToggleWorks = true;

        // Die Strecke src -> services -> Datei.
        extras.clickPath = [
            `src: ${await ensureExpanded(page, 'src')}`,
            `src/services: ${await ensureExpanded(page, 'src/services')}`,
        ];
        await page.click(rowSelector(OPEN_FILE));
        await page.waitForFunction(
            () => globalThis.__atlasReader?.status === 'ready',
            undefined,
            { timeout: 20000 },
        );
        // Der Editor malt asynchron. Ohne diese Zusage waere der Screenshot
        // vielleicht der von einer leeren Flaeche.
        await page.waitForFunction(
            () => (document.querySelector('.view-lines')?.textContent ?? '').length > 0,
            undefined,
            { timeout: 20000 },
        );
        report.openedFile = OPEN_FILE;

        const readerState = await page.evaluate(() => {
            const seam = globalThis.__atlasReader;
            return {
                value: seam?.value() ?? '',
                readOnly: seam?.readOnly() === true,
                document: seam?.document ?? null,
            };
        });
        report.readerContainsCreateUser = readerState.value.includes('createUser');
        report.readerReadOnly = readerState.readOnly;
        extras.readerDocument = documentSummary(readerState.document);

        // Dagegen tippen. Ein read-only Editor darf sich davon nicht ruehren.
        await page.click('.atlas-reader-editor .view-lines');
        await page.keyboard.type('THIS MUST NOT LAND');
        await page.keyboard.press('Enter');
        await page.waitForTimeout(300);
        const afterTyping = await page.evaluate(() => globalThis.__atlasReader?.value() ?? '');
        report.editAttemptChangedContent = afterTyping !== readerState.value;
        extras.editAttempt = { typed: 'THIS MUST NOT LAND', lengthBefore: readerState.value.length, lengthAfter: afterTyping.length };

        report.breadcrumb = (await page.locator('[data-testid="atlas-breadcrumb"]').innerText()).replace(/\s+/g, ' ').trim();
        report.tabOpened = (await page.locator('[data-testid="atlas-tab"]').count()) > 0;
        extras.tabNames = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid="atlas-tab"]')].map((node) => node.textContent?.trim()),
        );

        await mkdir(OUT_DIR, { recursive: true });
        await page.screenshot({ path: join(OUT_DIR, 'app-chrome.png'), fullPage: true });
        await page.locator('.atlas-main').screenshot({ path: join(OUT_DIR, 'reader.png') });
        log('Screenshots geschrieben');

        // Woher der Baum kam, aus dem Mitschrieb des Proxys und nicht aus einer
        // Beschriftung: die Route wurde wirklich gerufen.
        const treeCalls = Object.keys(proxy.log.apiRoutes).filter((route) => route === '/api/tree');
        report.treeSource = treeCalls.length > 0 ? '/api/tree' : 'unknown';
        report.rpcTool = proxy.log.rpcTools['get_code_snippet'] > 0 ? 'get_code_snippet' : 'unknown';
        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        extras.treeSourceChip = await page.evaluate(
            () => document.querySelector('[data-chip="tree"]')?.textContent?.trim() ?? '',
        );

        // ------------------------------------------ 7. grosse Datei
        await page.goto(`${origin}/?project=${PROJECT_LARGE}`, { waitUntil: 'load' });
        await page.waitForFunction(
            () => document.querySelectorAll('[data-testid="atlas-tree-row"]').length >= 3,
            undefined,
            { timeout: 20000 },
        );
        await ensureExpanded(page, 'src');
        await page.click(rowSelector(LARGE_FILE));
        await page.waitForFunction(
            () => globalThis.__atlasReader?.status === 'ready',
            undefined,
            { timeout: 20000 },
        );

        const large = await page.evaluate(() => {
            const seam = globalThis.__atlasReader;
            const note = document.querySelector('[data-testid="atlas-truncation"]');
            return {
                document: seam?.document ?? null,
                loadedLines: (seam?.value() ?? '').split('\n').length,
                noteVisible: note !== null,
                noteText: note?.textContent?.trim() ?? '',
            };
        });
        extras.largeDocument = documentSummary(large.document);
        extras.largeNote = large.noteText;
        extras.largeLoadedLines = large.loadedLines;

        // Zusatzbeleg, den der eingefrorene Test nicht verlangt: die ehrliche
        // Zeile unter einer gekappten Datei, im Bild. Eine Zeichenkette im
        // Artefakt beweist, dass der Satz gebaut wurde; das Bild beweist, dass
        // er auch dasteht.
        await page.screenshot({ path: join(OUT_DIR, 'reader-truncated.png'), fullPage: true });

        const graphLastLine = large.document?.fileLastLine ?? 0;
        const loadedLastLine = large.document?.lastLine ?? 0;
        report.largeFileLines = graphLastLine;

        const onDisk = readFileSync(join(FIXTURE_LARGE, LARGE_FILE), 'utf8').split('\n').length;
        extras.largeFileLinesOnDisk = onDisk;

        // Ehrlich heisst: entweder ist die Datei ganz da, oder es steht
        // dran, welche Zeilen fehlen.
        report.truncationHonest = large.document?.truncated === true
            ? large.noteVisible && large.noteText.includes(`${loadedLastLine + 1}-${graphLastLine}`)
            : loadedLastLine === graphLastLine;

        // Die Fenster-Semantik direkt am Server, ohne Browser dazwischen.
        const withoutWindow = await callTool(serverPort, 'get_code_snippet', {
            project: PROJECT_LARGE,
            qualified_name: `${PROJECT_LARGE}.src.big`,
            format: 'json',
        });
        const withWindow = await callTool(serverPort, 'get_code_snippet', {
            project: PROJECT_LARGE,
            qualified_name: `${PROJECT_LARGE}.src.big`,
            start_line: 501,
            end_line: 600,
            format: 'json',
        });
        report.windowSemantics = withWindow.start_line === 501 ? 'works' : 'ignored';
        extras.windowMeasurement = {
            asked: { start_line: 501, end_line: 600 },
            gotWithWindow: { start_line: withWindow.start_line, end_line: withWindow.end_line, clipped: withWindow.source_clipped === true },
            gotWithoutWindow: { start_line: withoutWindow.start_line, end_line: withoutWindow.end_line, clipped: withoutWindow.source_clipped === true },
            note: withWindow.start_line === 501
                ? 'der Server respektiert start_line/end_line'
                : 'get_code_snippet nimmt start_line/end_line an und ignoriert sie: dieselbe Antwort wie ohne Fenster',
        };

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w2a] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w2a] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
    report.projects = { small: PROJECT_SMALL, large: PROJECT_LARGE };
    report.fixtures = { small: 'fixtures/atlas-sample', large: 'fixtures/atlas-sample-large' };
    report.timings = timings;
    report.extras = extras;
    report.generatedAt = new Date().toISOString();
    report.error = failure ? failure.message : null;

    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(OUT_JSON, JSON.stringify(report, null, 2) + '\n', 'utf8');
    log('geschrieben:', OUT_JSON);

    const missingTestids = REQUIRED_TESTIDS.filter((id) => !report.testids.includes(id));
    const ok =
        failure === null &&
        missingTestids.length === 0 &&
        /CODEATLAS/.test(report.brandText) &&
        /^v\d+\.\d+\.\d+/.test(report.versionChip) &&
        // Nachgezogen am 2026-08-29 (W7a, Nutzerauftrag): der Lauf nagelte
        // [f]ile fest, also ausgerechnet einen der vier Menuepunkte ohne
        // Verdrahtung. Eine Attrappe darf keine Zusicherung sein. Verlangt
        // bleibt das Buchstaben-Menue als Form, belegt am verdrahteten [a]tlas.
        /\[a\]tlas/.test(report.menuText) &&
        /type a command or ask the atlas/.test(report.commandPlaceholder) &&
        report.treeEntries >= 10 &&
        report.treeSource === '/api/tree' &&
        report.openedFile === OPEN_FILE &&
        report.readerContainsCreateUser === true &&
        report.readerReadOnly === true &&
        report.editAttemptChangedContent === false &&
        /src\s*(>|›)\s*services/.test(report.breadcrumb) &&
        report.tabOpened === true &&
        report.rpcTool === 'get_code_snippet' &&
        /^(works|ignored)$/.test(report.windowSemantics) &&
        report.truncationHonest === true &&
        report.largeFileLines > 500 &&
        report.port >= MIN_PORT &&
        report.leftoverProcesses === 0 &&
        extras.blockedRequests.length === 0 &&
        extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w2a] W2a-Smoke NICHT gruen.');
        if (missingTestids.length > 0) {
            console.error('[smoke-w2a] fehlende Testmarken:', missingTestids.join(', '));
        }
        if (home) {
            console.error('[smoke-w2a] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W2a-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w2a] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
