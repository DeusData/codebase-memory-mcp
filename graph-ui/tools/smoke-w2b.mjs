#!/usr/bin/env node
/*
 * W2b-Smoke: der Semantic Twin an einem echten Server, ueber eine echte
 * Caret-Strecke.
 *
 * Was hier bewiesen wird, kann kein Unit-Test beweisen. Die Unit-Tests zeigen,
 * dass das portierte Render-Modell aus einer aufgezeichneten IR dieselben vier
 * Tiefen baut wie im Referenzprojekt. Sie sagen nichts darueber, ob in einem
 * echten Browser ein Klick in eine Zeile ein Symbol aufloest, ob der Graph
 * dieses Servers die sechs Schritte von createUser hergibt, ob die Nummern im
 * Rand des Editors zu der Liste im Panel passen, und ob ein zweiter Klick
 * innerhalb desselben Symbols den Server noch einmal fragt.
 *
 * Ablauf, wie bei smoke-w2a:
 *   1. `npm run build`
 *   2. isoliertes HOME, fixtures/atlas-sample ueber die CLI indizieren
 *   3. C-Server auf einem freien Port >= 4240 starten
 *   4. dist/ auf einem zweiten Port ausliefern, /rpc und /api dorthin proxen
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *   6. Klickstrecke: Datei oeffnen, in die validateUser-Zeile klicken, Panel
 *      messen, Tiefen durchfahren, Beleg aufklappen, Caret versetzen, folgen
 *   7. abraeumen, Restprozesse zaehlen, verification/w2/twin.json schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w2b).
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
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w2b';
const OUT_DIR = join(ROOT, 'verification', 'w2');
const OUT_JSON = join(OUT_DIR, 'twin.json');
const MIN_PORT = 4240;

const SERVICE_FILE = 'src/services/userService.ts';
const TARGET_FILE = 'src/util/validate.ts';

/** Ueber die Entprellung des Carets hinaus warten (src/App.tsx). */
const SETTLE_MS = 700;

/**
 * Chromium ohne Aussenwelt. Wortgleich mit smoke-w2a: die Flags sind die
 * Absicht, das Netz-Deny-Gate daneben ist der Beweis.
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

const log = (...parts) => console.log('[smoke-w2b]', ...parts);
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
 * Die Zeile, in der ein Ausdruck steht, aus der Datei gelesen.
 *
 * Eine feste Zahl waere eine zweite Wahrheit ueber das Fixture, die beim
 * naechsten Zeilenumbruch still falsch wird. Gezaehlt wird 1-basiert, so wie
 * der Graph und Monaco zaehlen.
 */
function lineContaining(source, needle) {
    const index = source.split('\n').findIndex((line) => line.includes(needle));
    if (index < 0) {
        throw new Error(`"${needle}" steht nicht in ${SERVICE_FILE}`);
    }
    return index + 1;
}

const rowSelector = (path) => `[data-testid="atlas-tree-row"][data-path="${path}"]`;

/** Einen Ordner offen haben, ohne ihn zuzuklappen, wenn er schon offen ist. */
async function ensureExpanded(page, path) {
    const twisty = `${rowSelector(path)} .atlas-tree-twisty`;
    const before = (await page.locator(twisty).textContent())?.trim() ?? '';
    if (before === '▾') {
        return 'already open';
    }
    await page.click(rowSelector(path));
    await page.waitForFunction(
        (selector) => document.querySelector(selector)?.textContent?.trim() === '▾',
        twisty,
        { timeout: 10000 },
    );
    return 'clicked open';
}

/**
 * In eine Quelltextzeile klicken, so wie ein Leser es tut.
 *
 * Monaco zeichnet jede sichtbare Zeile als eigenes `.view-line`. Gesucht wird
 * die, deren Text den Ausdruck enthaelt; geklickt wird auf sie, und danach wird
 * am Editor selbst nachgelesen, wo der Caret gelandet ist. Ohne diese
 * Rueckfrage waere "geklickt" und "Caret steht dort" dasselbe Wort fuer zwei
 * verschiedene Dinge.
 */
async function clickSourceLine(page, needle, expectedLine) {
    await page.locator('.atlas-reader-editor .view-line', { hasText: needle }).first().click();
    await page.waitForFunction(
        (line) => globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber === line,
        expectedLine,
        { timeout: 10000 },
    );
}

/**
 * Den Tiefenregler bewegen.
 *
 * React fuehrt an einem kontrollierten Feld einen eigenen Wert-Beobachter.
 * Direktes Setzen von `value` aktualisiert ihn mit, und React haelt das
 * Ereignis danach fuer eine Wiederholung. Der Setter des Prototyps geht daran
 * vorbei, und erst dann ist das `input` ein echter Wertwechsel.
 */
async function setDepth(page, depth) {
    await page.evaluate((value) => {
        const slider = document.querySelector('[data-testid="atlas-twin-depth"]');
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
        setter?.call(slider, String(value));
        slider?.dispatchEvent(new Event('input', { bubbles: true }));
    }, depth);
    await page.waitForFunction(
        (value) => document.querySelector('[data-testid="atlas-twin-depth"]')?.value === String(value),
        depth,
        { timeout: 10000 },
    );
    // Der Koerper wird nach dem Regler neu gezeichnet; ohne diese Zusage waere
    // die naechste Messung noch die der vorigen Tiefe. Gewartet wird auf den
    // Modus, den der Koerper selbst nennt, und nicht auf eine Wartezeit.
    const modes = ['prose', 'guided', 'sections', 'dense'];
    await page.waitForFunction(
        (mode) => document.querySelector('.atlas-twin-body')?.getAttribute('data-mode') === mode,
        modes[depth],
        { timeout: 10000 },
    );
}

/** Der sichtbare Text des Panels, ohne den Rest der Seite. */
async function twinText(page) {
    return (await page.locator('[data-testid="atlas-twin"]').innerText()).replace(/\s+/g, ' ').trim();
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
        sectionsPopulated: 0,
        stepsCount: 0,
        stepsOrdered: false,
        firstStep: '',
        envReadShown: '',
        throwShown: '',
        missingTestsHonest: false,
        depthProseNoQualifiedNames: false,
        depthDenseShowsConfidence: false,
        badgeCount: 0,
        caretSyncNoRefetch: false,
        followNavigatesEditor: false,
        evidenceVisible: false,
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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w2b-home-'));
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

        // -------------------------------------------- 6. Datei und Caret
        const source = readFileSync(join(FIXTURE, SERVICE_FILE), 'utf8');
        const validateLine = lineContaining(source, 'validateUser(');
        const insertLine = lineContaining(source, 'insert(');
        extras.caretLines = { validateUser: validateLine, insert: insertLine };
        log(`Caret-Zeilen aus der Datei: validateUser@${validateLine}, insert@${insertLine}`);

        await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-twin"]', { timeout: 20000 });
        await page.waitForFunction(
            () => document.querySelectorAll('[data-testid="atlas-tree-row"]').length >= 10,
            undefined,
            { timeout: 20000 },
        );

        await ensureExpanded(page, 'src');
        await ensureExpanded(page, 'src/services');
        await page.click(rowSelector(SERVICE_FILE));
        await page.waitForFunction(() => globalThis.__atlasReader?.status === 'ready', undefined, {
            timeout: 20000,
        });
        await page.waitForFunction(
            () => (document.querySelector('.view-lines')?.textContent ?? '').length > 0,
            undefined,
            { timeout: 20000 },
        );

        await clickSourceLine(page, 'validateUser(', validateLine);
        await page.waitForFunction(() => globalThis.__atlasTwin?.symbol === 'createUser', undefined, {
            timeout: 20000,
        });
        await page.waitForFunction(
            () => document.querySelectorAll('[data-testid="codeatlas-twin-step"]').length >= 6,
            undefined,
            { timeout: 20000 },
        );
        extras.fetchesAfterFirstResolve = await page.evaluate(() => window.__atlasTwinFetches ?? 0);

        // --------------------------------------- Tiefe 2: die Fakten selbst
        const sections = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid^="codeatlas-twin-section-"]')].map((node) => ({
                name: node.getAttribute('data-testid')?.replace('codeatlas-twin-section-', '') ?? '',
                populated: node.getAttribute('data-populated') === 'true',
            })),
        );
        extras.sections = sections;
        report.sectionsPopulated = sections.filter((section) => section.populated).length;

        const steps = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid="codeatlas-twin-step"]')].map((node) => ({
                label: node.querySelector('.atlas-twin-row-label')?.textContent?.trim() ?? '',
                line: Number(node.getAttribute('data-line')),
            })),
        );
        extras.steps = steps;
        report.stepsCount = steps.length;
        report.firstStep = steps[0]?.label ?? '';
        report.stepsOrdered = steps.every(
            (step, index) => index === 0 || steps[index - 1].line <= step.line,
        );

        const rowLabelsOf = async (section) =>
            page.evaluate(
                (name) =>
                    [
                        ...document.querySelectorAll(
                            `[data-testid="codeatlas-twin-section-${name}"] .atlas-twin-row-label`,
                        ),
                    ].map((node) => node.textContent?.trim() ?? ''),
                section,
            );
        const stateLabels = await rowLabelsOf('state');
        const errorLabels = await rowLabelsOf('errors');
        extras.stateLabels = stateLabels;
        extras.errorLabels = errorLabels;
        report.envReadShown = stateLabels.includes('DB_URL') ? 'DB_URL' : '';
        report.throwShown = errorLabels.includes('ValidationError') ? 'ValidationError' : '';

        const testsEmpty = await page.evaluate(
            () =>
                document.querySelector('[data-testid="codeatlas-twin-empty-tests"]')?.textContent?.trim() ??
                '',
        );
        extras.testsSentence = testsEmpty;
        report.missingTestsHonest = testsEmpty.includes('No test callers found');

        report.badgeCount = await page.evaluate(
            () => document.querySelectorAll('.codeatlas-step-badge').length,
        );
        extras.badgeClasses = await page.evaluate(() =>
            [...document.querySelectorAll('.codeatlas-step-badge')].map((node) => node.className),
        );

        // Wie viele Gewissheits-Angaben die technische Tiefe zeigt. Die Antwort
        // muss null sein: eine Zahl neben jeder Zeile gehoert dem Pruefer und
        // nicht dem Leser, und wenn sie hier schon staende, waere die dichte
        // Tiefe keine eigene Aussage mehr.
        const certaintyAtTechnical = await page.evaluate(
            () => document.querySelectorAll('[data-testid="codeatlas-row-confidence"]').length,
        );

        // ------------------------------------------------ Belege aufklappen
        await page.click('[data-testid="codeatlas-evidence-btn"][data-factpath="steps[0]"]');
        await page.waitForSelector('[data-testid="codeatlas-evidence-popover"]', { timeout: 10000 });
        const evidenceText = await page.evaluate(
            () =>
                document.querySelector('[data-testid="codeatlas-evidence-popover"]')?.textContent?.trim() ??
                '',
        );
        extras.evidenceText = evidenceText;
        report.evidenceVisible = evidenceText.length > 0 && evidenceText.includes('index generation');

        await mkdir(OUT_DIR, { recursive: true });
        await page.screenshot({ path: join(OUT_DIR, 'twin.png'), fullPage: true });
        log('twin.png geschrieben (Tiefe 2, ein Beleg offen)');

        // ------------------------------------------------ Tiefe 3: dicht
        await setDepth(page, 3);
        const denseText = await twinText(page);
        extras.denseSample = denseText.slice(0, 400);

        /*
         * Was "zeigt confidence" an diesem Backend heisst.
         *
         * Die dichte Tiefe haengt an jede Zeile, die etwas ueber den Code
         * behauptet, ihre Gewissheit: entweder die Zahl, die der Provider
         * aufgezeichnet hat, oder das Wort, das sagt, warum es keine gibt. Die
         * 0.9.0-Engine hinter diesem Server bewertet nichts, sie liest ihre
         * Aufruf-, Aufrufer- und Wurf-Relationen direkt aus dem Graphen. Also
         * steht hier `exact` auf einer direkt gelesenen und `unscored` auf
         * einer abgeleiteten Zeile, und die Form `confidence 0.90` kommt an
         * diesem Backend nie vor.
         *
         * Auf `confidence 0.\d\d` zu pruefen hiesse darum, die Engine zu
         * messen und nicht das Panel, und ein gruener Haken dafuer waere nur zu
         * haben, indem irgendwer eine Zahl erfindet. Gemessen wird stattdessen
         * die Aussage, die dem Panel gehoert: die Spalte ist da, sie ist auf
         * jeder behauptenden Zeile gefuellt, und sie ist an der technischen
         * Tiefe nicht da. Dass die Zahlform steht, wenn ein Provider eine Zahl
         * liefert, beweist src/twin/render-model.test.ts an der
         * aufgezeichneten IR ("confidence 0.90").
         */
        const certainty = await page.evaluate(() => {
            const cells = [...document.querySelectorAll('[data-testid="codeatlas-row-confidence"]')];
            return {
                count: cells.length,
                labels: [...new Set(cells.map((node) => node.textContent?.trim() ?? ''))],
                scored: cells.filter((node) => node.getAttribute('data-scored') === 'true').length,
                blank: cells.filter((node) => (node.textContent?.trim() ?? '').length === 0).length,
            };
        });
        extras.certainty = {
            atTechnicalDepth: certaintyAtTechnical,
            atDenseDepth: certainty,
            numericFormPresent: /confidence \d\.\d\d/.test(denseText),
            note:
                'Diese Engine zeichnet keinen Score auf, also lauten die Angaben exact und unscored. '
                + 'Die Zahlform kommt hier nicht vor und wird an der aufgezeichneten IR bewiesen '
                + '(src/twin/render-model.test.ts).',
        };
        report.depthDenseShowsConfidence =
            certaintyAtTechnical === 0 && certainty.count > 0 && certainty.blank === 0;

        await page.screenshot({ path: join(OUT_DIR, 'twin-dense.png'), fullPage: true });
        log('twin-dense.png geschrieben (Tiefe 3)');

        // ------------------------------------------------ Tiefe 0: Prosa
        await setDepth(page, 0);
        const proseText = await twinText(page);
        extras.proseSample = proseText.slice(0, 400);
        report.depthProseNoQualifiedNames = !/\w+\.\w+\.\w+/.test(proseText);
        extras.proseQualifiedHit = /\w+\.\w+\.\w+/.exec(proseText)?.[0] ?? null;

        await setDepth(page, 2);
        await page.waitForFunction(
            () => document.querySelectorAll('[data-testid="codeatlas-twin-step"]').length >= 6,
            undefined,
            { timeout: 10000 },
        );

        // ---------------------------------- Caret versetzen, kein Refetch
        const currentBefore = await page.evaluate(
            () =>
                document
                    .querySelector('[data-testid="codeatlas-twin-step"][data-current="true"]')
                    ?.getAttribute('data-line') ?? '',
        );
        const fetchesBefore = await page.evaluate(() => window.__atlasTwinFetches ?? 0);
        await clickSourceLine(page, 'insert(', insertLine);
        await page.waitForFunction(
            (line) =>
                document
                    .querySelector('[data-testid="codeatlas-twin-step"][data-current="true"]')
                    ?.getAttribute('data-line') === String(line),
            insertLine,
            { timeout: 10000 },
        );
        // Ueber die Entprellung hinaus warten: ein Refetch, der erst danach
        // kaeme, waere sonst nicht gemessen, sondern verpasst.
        await page.waitForTimeout(SETTLE_MS);
        const fetchesAfter = await page.evaluate(() => window.__atlasTwinFetches ?? 0);
        const currentAfter = await page.evaluate(
            () =>
                document
                    .querySelector('[data-testid="codeatlas-twin-step"][data-current="true"]')
                    ?.getAttribute('data-line') ?? '',
        );
        const stillCreateUser = await page.evaluate(() => globalThis.__atlasTwin?.symbol === 'createUser');
        extras.caretSync = {
            currentBefore,
            currentAfter,
            fetchesBefore,
            fetchesAfter,
            stillCreateUser,
        };
        report.caretSyncNoRefetch =
            fetchesBefore === fetchesAfter &&
            stillCreateUser === true &&
            currentBefore !== currentAfter &&
            currentAfter === String(insertLine);

        // --------------------------------------------- Einem Schritt folgen
        await page.click('[data-testid="codeatlas-twin-step"] button');
        await page.waitForFunction(() => globalThis.__atlasTwin?.symbol === 'validateUser', undefined, {
            timeout: 20000,
        });
        await page.waitForFunction(
            () => globalThis.__atlasReader?.document?.path === 'src/util/validate.ts',
            undefined,
            { timeout: 20000 },
        );
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-twin"]')?.getAttribute('data-status') === 'ready',
            undefined,
            { timeout: 20000 },
        );
        const afterFollow = await page.evaluate(() => ({
            breadcrumb: (document.querySelector('[data-testid="atlas-breadcrumb"]')?.textContent ?? '')
                .replace(/\s+/g, ' ')
                .trim(),
            tabs: [...document.querySelectorAll('.atlas-tab')].map((node) => node.getAttribute('data-path')),
            subject:
                document.querySelector('[data-testid="atlas-twin-subject"]')?.textContent?.trim() ?? '',
            symbol: globalThis.__atlasTwin?.symbol ?? '',
            caret: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
        }));
        extras.afterFollow = afterFollow;
        report.followNavigatesEditor =
            afterFollow.breadcrumb.includes('validate.ts') &&
            afterFollow.tabs.includes(TARGET_FILE) &&
            afterFollow.symbol === 'validateUser';

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        extras.fetchesTotal = await page.evaluate(() => window.__atlasTwinFetches ?? 0);

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w2b] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w2b] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
    report.openedFile = SERVICE_FILE;
    report.timings = timings;
    report.extras = extras;
    report.generatedAt = new Date().toISOString();
    report.error = failure ? failure.message : null;

    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(OUT_JSON, JSON.stringify(report, null, 2) + '\n', 'utf8');
    log('geschrieben:', OUT_JSON);

    const ok =
        failure === null &&
        report.sectionsPopulated >= 5 &&
        report.stepsCount === 6 &&
        report.stepsOrdered === true &&
        report.firstStep === 'validateUser' &&
        report.envReadShown === 'DB_URL' &&
        report.throwShown === 'ValidationError' &&
        report.missingTestsHonest === true &&
        report.depthProseNoQualifiedNames === true &&
        report.depthDenseShowsConfidence === true &&
        report.badgeCount >= 3 &&
        report.caretSyncNoRefetch === true &&
        report.followNavigatesEditor === true &&
        report.evidenceVisible === true &&
        report.port >= MIN_PORT &&
        report.leftoverProcesses === 0 &&
        extras.blockedRequests.length === 0 &&
        extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w2b] W2b-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w2b] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    if (home) {
        await rm(home, { recursive: true, force: true });
    }
    log('W2b-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w2b] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
