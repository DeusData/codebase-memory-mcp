#!/usr/bin/env node
/*
 * W5a-Smoke: der Opt-out ist ein Produktmodus, der Sidecar sagt die Wahrheit
 * ueber seinen Prozess, und eine committete Policy schlaegt jede Praeferenz.
 *
 * Was hier bewiesen wird, koennen die Unit-Tests nicht beweisen. Sie zeigen an
 * erfundenen Antworten, dass `probeSidecar` drei Adressen fragt und keine
 * vierte, und dass die Vorrangregel in jeder Kombination dasselbe ergibt. Sie
 * sagen nichts darueber, ob eine frisch geladene Seite mit leerem
 * localStorage wirklich KEIN Byte Richtung 4141 schickt, ob der gebaute
 * llama-server aus vendor/llama ein Modell laedt und sich im Panel wiederfindet,
 * ob der Zustand nach dem Beenden ehrlich zurueckfaellt, und ob eine
 * policy.json im indizierten ZIELprojekt ueber den vorhandenen Lese-Weg
 * ueberhaupt ankommt.
 *
 * Ablauf, wie bei smoke-w4d und smoke-w4e:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis
 *   3. zwei Projekte indizieren: fixtures/atlas-sample (nur gelesen) und eine
 *      KOPIE davon mit .codeatlas/policy.json {"llm":"deny"}
 *   4. C-Server auf einem freien Port >= 4300, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, mit einer Route-Sperre, die 127.0.0.1:4141
 *      ausdruecklich DURCHLAESST und mitzaehlt
 *   6a. frische App: llm aus, und der Zaehler auf 4141 steht auf null
 *   6b. einschalten ohne Prozess: die Anleitung nennt llm/start.sh
 *   6c. llm/start.sh class-a, warten auf ready: Modell und Chip
 *   6d. llm/stop.sh: der Zustand faellt ehrlich zurueck
 *   6e. das Policy-Projekt, mit einer VORHER auf "an" gesetzten Praeferenz
 *   7. abraeumen, Restprozesse zaehlen (auch llama-server), JSON und Bilder
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w5a). 4141 ist
 * Loopback und zaehlt dort nicht als Verstoss; das ist der Punkt, an dem sich
 * "air-gapped" und "lokales Modell" nicht widersprechen.
 *
 * ## Drei Entscheidungen, die man sonst raten muesste
 *
 * **Die Null wird zweimal gemessen, aus zwei Richtungen.** Der Route-Handler im
 * Browser zaehlt jede Anfrage an 4141, und der Griff `__atlasLlm.probes` zaehlt
 * jeden Aufruf von `probeSidecar` in der Anwendung. Eine der beiden Zahlen
 * allein waere angreifbar: der Handler saehe eine Anfrage nicht, die eine
 * andere Schicht stellt, und der Zaehler saehe eine nicht, die an ihm vorbei
 * geht. Beide muessen null sein, solange das LLM aus ist.
 *
 * **Die Praeferenz des Policy-Projekts wird VOR dem Laden auf "an" gesetzt.**
 * Ein Schalter, den man erst nach dem Sperren drueckt, beweist nur, dass ein
 * Klick nichts tut. Hier steht die Praeferenz schon im Speicher, wenn die Seite
 * das erste Mal laedt: die Policy muss also etwas ueberstimmen, das es
 * wirklich gibt, und nicht nur einen Klick abfangen.
 *
 * **Die Policy-Datei liegt in einer KOPIE der Fixture.** fixtures/atlas-sample
 * ist Beweisgrundlage von vier fruehereren Laeufen und bleibt byte-identisch.
 * Eine Datei dort anzulegen waere eine Aenderung an der Grundlage, mit der die
 * anderen Laeufe verglichen werden.
 */

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { cp, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
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
const PROJECT = 'codeatlasweb-w5a';
const POLICY_PROJECT = 'codeatlasweb-w5a-policy';
const OUT_DIR = join(ROOT, 'verification', 'w5');
const OUT_JSON = join(OUT_DIR, 'sidecar.json');
const SHOT_OFF = join(OUT_DIR, 'sidecar-off.png');
const SHOT_READY = join(OUT_DIR, 'sidecar-ready.png');
const MIN_PORT = 4300;

/** Der Produktport des Sidecars. Nicht verhandelbar, er steht in llm/start.sh. */
const SIDECAR_PORT = 4141;
const SIDECAR_ORIGIN = `http://127.0.0.1:${SIDECAR_PORT}`;

/*
 * Warum der Chip an seinem Ende geprueft wird und nicht als ganze Zeichenkette.
 *
 * Bis W7c hiess er "llm ready: ...", seit W7c "local llm ready: ...", weil der
 * Nutzer wissen wollte, dass das Modell auf seinem Rechner laeuft. Sechs
 * Vergleiche auf Gleichheit haben diesen Lauf seither rot gemacht, ohne dass am
 * Produkt etwas kaputt war, und weil das Ergebnis-Artefakt committet war, fiel
 * es keinem Test auf. Geprueft wird jetzt die AUSSAGE des Chips (welcher
 * Zustand), nicht sein Praefix (wessen Modell). Wird der Zustand selbst falsch,
 * schlaegt der Lauf weiterhin fehl: `endsWith` ist an dieser Stelle nicht
 * nachsichtiger, nur unabhaengig von einer Umbenennung, die niemanden
 * ueberrascht hat ausser diesem Skript.
 */

/** Die Modellwahl dieses Laufs und der Name, der im Panel stehen muss. */
const MODEL_CHOICE = 'class-a';
const MODEL_NAME = 'Qwen3.5-2B';

/** Wie lange auf `ready` gewartet wird. Qwen3.5-2B laedt in Sekunden, der Poll ist 3 s. */
const READY_TIMEOUT_MS = 120000;

/** Chromium ohne Aussenwelt. Wortgleich mit smoke-w4e, ohne die GL-Flags. */
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

const log = (...parts) => console.log('[smoke-w5a]', ...parts);
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
            },
            stdio: ['ignore', 'pipe', 'pipe'],
            ...options,
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

/** Alles, was die Anwendung ueber das lokale Modell sagt. */
const llmSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasLlm;
        return seam === undefined ? null : { ...seam };
    });

/** Was im Panel und in der Statusleiste wirklich steht. */
const llmDom = (page) =>
    page.evaluate(() => {
        const panel = document.querySelector('[data-testid="atlas-llm"]');
        const text = (selector) =>
            document.querySelector(selector)?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        /*
         * Der Chip wird an seinem Thema erkannt, nicht an seinem Wortlaut.
         *
         * `data-chip` traegt das Etikett selbst (AtlasChrome.tsx:281), und das
         * Etikett hat sich in W7c geaendert: aus "llm" wurde "local llm", weil
         * der Nutzer wissen wollte, dass das Modell auf seinem Rechner laeuft.
         * Der Vergleich auf Gleichheit hat diesen Lauf seither rot gemacht,
         * ohne dass am Produkt etwas kaputt war. Gesucht wird jetzt der Chip,
         * der das Modell meint; ob er morgen anders heisst, aendert daran
         * nichts, und die Zusicherung darunter bleibt Wort fuer Wort dieselbe.
         */
        const chip = [...document.querySelectorAll('[data-testid="atlas-statusbar"] .atlas-chip')]
            .find((entry) => (entry.getAttribute('data-chip') ?? '').includes('llm'));
        const toggle = document.querySelector('[data-testid="atlas-llm-toggle"]');
        const menu = document.querySelector('[data-menu="a-llm"]');
        return {
            panelPresent: panel !== null,
            panelState: panel?.getAttribute('data-state') ?? '',
            panelText: panel?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            message: text('[data-testid="atlas-llm-message"]'),
            command: text('[data-testid="atlas-llm-command"]'),
            hint: text('[data-testid="atlas-llm-hint"]'),
            model: text('[data-testid="atlas-llm-model"]'),
            rows: [...document.querySelectorAll('[data-testid="atlas-llm-row"]')].map((row) => ({
                key: row.getAttribute('data-row') ?? '',
                value: row.querySelector('.atlas-llm-row-value')?.textContent?.trim() ?? '',
            })),
            chipPresent: chip !== undefined,
            chipText: chip?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            toggleLabel: toggle?.textContent?.trim() ?? '',
            toggleDisabled: toggle?.disabled === true,
            menuLabel: menu?.textContent?.trim() ?? '',
        };
    });

/** Die Seite laden und warten, bis Statusleiste und Policy-Antwort stehen. */
async function openApp(page, origin, project) {
    await page.goto(`${origin}/?project=${project}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-llm"]', { timeout: 30000 });
    // Erst wenn die Policy beantwortet ist, ist "aus" eine Aussage und nicht
    // nur der Zustand vor der ersten Antwort.
    await page.waitForFunction(
        () => (globalThis.__atlasLlm?.policyVerdict ?? '') !== '',
        undefined,
        { timeout: 60000 },
    );
}

/** Auf eine Lage des Sidecars warten. */
const waitForState = (page, state, timeout) =>
    page.waitForFunction(
        (expected) => globalThis.__atlasLlm?.state === expected,
        state,
        { timeout },
    );

async function main() {
    const totalStarted = Date.now();
    let serverChild = null;
    let proxy = null;
    let browser = null;
    let home = null;
    let runtimeDir = null;
    let policyRepo = null;
    let serverPort = 0;
    let uiPort = 0;
    let failure = null;
    let sidecarStarted = false;
    const timings = {};

    const result = {
        llmOffByDefault: false,
        zeroLlmRequestsWhileOff: false,
        notRunningHonest: false,
        statusReady: false,
        modelShown: '',
        chipShown: false,
        stopFallsBackHonestly: false,
        policyBlocks: false,
        switchIneffective: false,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = { blockedRequests: [], consoleErrors: [], pageErrors: [], sidecarRequests: [] };

    /*
     * Der Mitschnitt. Er zaehlt JEDE Anfrage an 4141, egal welche Schicht sie
     * stellt, und er laesst sie durch: der Sidecar ist Loopback und erlaubt.
     * Alles andere ausserhalb des eigenen Ursprungs wird weiter abgewiesen.
     */
    const routeFor = (origin) => async (route) => {
        const url = route.request().url();
        if (url.startsWith(SIDECAR_ORIGIN)) {
            extras.sidecarRequests.push({ url, atMs: Date.now() - totalStarted });
            await route.continue();
            return;
        }
        if (url.startsWith(origin) || url.startsWith('data:') || url.startsWith('blob:')) {
            await route.continue();
            return;
        }
        extras.blockedRequests.push(url);
        await route.abort();
    };

    try {
        if (!existsSync(BINARY)) {
            throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
        }
        if (!existsSync(FIXTURE)) {
            throw new Error(`Fixture fehlt: ${FIXTURE}`);
        }
        if (!existsSync(join(ROOT, 'vendor', 'llama', 'llama-server'))) {
            throw new Error('vendor/llama/llama-server fehlt (siehe vendor/llama/HERKUNFT.md)');
        }

        // ------------------------------------------------------------ 1. Bau
        log('npm run build');
        const buildStarted = Date.now();
        const build = await run('npm', ['run', 'build']);
        timings.buildMs = Date.now() - buildStarted;
        if (build.code !== 0) {
            throw new Error(`npm run build endete mit ${build.code}`);
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w5a-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w5a-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);

        // ------------------------------------------------------- 3. Projekte
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };

        // Die KOPIE. Die Fixture selbst wird nur gelesen.
        policyRepo = await mkdtemp('/private/tmp/codeatlasweb-w5a-policy-');
        await cp(FIXTURE, policyRepo, { recursive: true });
        await mkdir(join(policyRepo, '.codeatlas'), { recursive: true });
        await writeFile(
            join(policyRepo, '.codeatlas', 'policy.json'),
            JSON.stringify({ llm: 'deny' }, null, 2) + '\n',
            'utf8',
        );
        const indexedPolicy = await indexRepository(BINARY, {
            home,
            repoPath: policyRepo,
            project: POLICY_PROJECT,
        });
        extras.indexedPolicy = { nodes: indexedPolicy.nodes, edges: indexedPolicy.edges };
        log(`indiziert: ${PROJECT} und ${POLICY_PROJECT} (Kopie mit .codeatlas/policy.json)`);

        // ------------------------------------------------- 4. Server, Proxy
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        result.port = uiPort;
        extras.serverPort = serverPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        // Bevor der Browser laeuft: der Port des Sidecars muss frei sein, sonst
        // beweist Phase 6b nichts.
        const busyBefore = await countListeners(SIDECAR_PORT);
        extras.sidecarListenersBefore = busyBefore;
        if (busyBefore > 0) {
            throw new Error(
                `auf ${SIDECAR_PORT} lauscht schon etwas (${busyBefore}); erst llm/stop.sh fahren`,
            );
        }

        // ------------------------------------------------------- 5. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const origin = `http://127.0.0.1:${uiPort}`;

        const context = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
        await context.route('**/*', routeFor(origin));
        const page = await context.newPage();
        page.on('console', (message) => {
            if (message.type() === 'error') {
                extras.consoleErrors.push(message.text());
            }
        });
        page.on('pageerror', (error) => extras.pageErrors.push(String(error)));
        await mkdir(OUT_DIR, { recursive: true });

        // ------------------------------- 6a. Erststart: aus, und zwar wirklich
        await openApp(page, origin, PROJECT);
        // Zwei volle Poll-Intervalle stehenlassen. Ein Opt-out, der erst nach
        // dem zweiten Takt anfaengt zu fragen, waere hier sonst unsichtbar.
        await page.waitForTimeout(7000);
        const offSeam = await llmSeam(page);
        const offDom = await llmDom(page);
        extras.off = { seam: offSeam, dom: offDom };
        result.llmOffByDefault =
            offSeam !== null
            && offSeam.state === 'off'
            && offSeam.preferenceOn === false
            && offSeam.policyVerdict === 'absent'
            && offDom.panelState === 'off'
            && offDom.chipText.endsWith('llm off');
        result.zeroLlmRequestsWhileOff =
            extras.sidecarRequests.length === 0 && offSeam?.probes === 0;
        log(`Erststart: state=${offSeam?.state}, Chip "${offDom.chipText}", `
            + `Anfragen an ${SIDECAR_PORT}: ${extras.sidecarRequests.length}, Proben: ${offSeam?.probes}`);

        await page.screenshot({ path: SHOT_OFF, fullPage: true });
        log('sidecar-off.png geschrieben');

        // -------------------------- 6b. Einschalten, ohne dass etwas laeuft
        await page.click('[data-menu="a-llm"]');
        await waitForState(page, 'not-running', 30000);
        const notRunning = await llmDom(page);
        const notRunningSeam = await llmSeam(page);
        extras.notRunning = { seam: notRunningSeam, dom: notRunning };
        result.notRunningHonest =
            notRunning.panelState === 'not-running'
            && notRunning.command.includes('llm/start.sh')
            && notRunning.message.includes(String(SIDECAR_PORT))
            && notRunning.message.includes('cannot start a process')
            && notRunning.chipText.endsWith('llm not running')
            && notRunningSeam?.preferenceOn === true
            && extras.sidecarRequests.length > 0;
        log(`eingeschaltet ohne Prozess: "${notRunning.command}" (Chip "${notRunning.chipText}")`);

        // ------------------------------------------ 6c. Den Sidecar starten
        const startStarted = Date.now();
        const startRun = await run('sh', [join(ROOT, 'llm', 'start.sh'), MODEL_CHOICE]);
        sidecarStarted = true;
        extras.startScript = { exit: startRun.code, out: startRun.out.trim().split('\n') };
        if (startRun.code !== 0) {
            throw new Error(`llm/start.sh endete mit ${startRun.code}`);
        }
        await waitForState(page, 'ready', READY_TIMEOUT_MS);
        timings.readyMs = Date.now() - startStarted;
        const ready = await llmDom(page);
        const readySeam = await llmSeam(page);
        extras.ready = { seam: readySeam, dom: ready };
        result.modelShown = ready.model;
        result.statusReady =
            ready.panelState === 'ready'
            && readySeam?.state === 'ready'
            && ready.model.includes(MODEL_NAME)
            && ready.rows.some((row) => row.key === 'model' && row.value.includes(MODEL_NAME))
            && ready.rows.some((row) => row.key === 'context' && row.value.includes('3072'))
            && ready.rows.some((row) => row.key === 'weights' && row.value.length > 0);
        result.chipShown = ready.chipPresent && ready.chipText.endsWith(`llm ready: ${MODEL_NAME}`);
        log(`ready nach ${timings.readyMs} ms: Modell "${ready.model}", Chip "${ready.chipText}"`);

        await page.screenshot({ path: SHOT_READY, fullPage: true });
        log('sidecar-ready.png geschrieben');

        // --------------------------------------- 6d. Den Sidecar beenden
        const stopRun = await run('sh', [join(ROOT, 'llm', 'stop.sh')]);
        extras.stopScript = { exit: stopRun.code, out: stopRun.out.trim().split('\n') };
        sidecarStarted = false;
        await waitForState(page, 'not-running', 30000);
        const afterStop = await llmDom(page);
        extras.afterStop = afterStop;
        result.stopFallsBackHonestly =
            stopRun.code === 0
            && afterStop.panelState === 'not-running'
            && afterStop.command.includes('llm/start.sh')
            && afterStop.chipText.endsWith('llm not running')
            && afterStop.model === ''
            && afterStop.rows.length === 0;
        log(`nach dem Beenden: "${afterStop.message.slice(0, 60)}" (Chip "${afterStop.chipText}")`);

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        await context.close();

        // ------------------------------- 6e. Das Projekt mit der Policy
        const policyContext = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
        await policyContext.route('**/*', routeFor(origin));
        // Die Praeferenz steht schon auf "an", BEVOR die Seite laedt. Warum,
        // steht im Kopf dieser Datei.
        await policyContext.addInitScript((project) => {
            window.localStorage.setItem(`atlas-llm:${project}`, JSON.stringify({ on: true }));
        }, POLICY_PROJECT);
        const policyPage = await policyContext.newPage();
        policyPage.on('pageerror', (error) => extras.pageErrors.push(String(error)));

        const requestsBeforePolicy = extras.sidecarRequests.length;
        await openApp(policyPage, origin, POLICY_PROJECT);
        await policyPage.waitForTimeout(7000);
        const policySeam = await llmSeam(policyPage);
        const policyDom = await llmDom(policyPage);
        extras.policy = { seam: policySeam, dom: policyDom };
        result.policyBlocks =
            policySeam !== null
            && policySeam.policyVerdict === 'deny'
            && policySeam.policyPath === '.codeatlas/policy.json'
            && policySeam.state === 'disabled-by-policy'
            && policySeam.preferenceOn === true
            && policyDom.panelState === 'disabled-by-policy'
            && policyDom.message.includes('.codeatlas/policy.json')
            && policyDom.message.includes(POLICY_PROJECT)
            && policyDom.chipText.endsWith('llm off by policy')
            && policyDom.toggleDisabled === true;

        // Der Schalter im Menue ist NICHT disabled. Er wird gedrueckt, und
        // danach muss alles genau so dastehen wie vorher.
        await policyPage.click('[data-menu="a-llm"]');
        await policyPage.waitForTimeout(4000);
        const afterClick = await llmSeam(policyPage);
        const afterClickDom = await llmDom(policyPage);
        extras.policyAfterClick = { seam: afterClick, dom: afterClickDom };
        result.switchIneffective =
            afterClick !== null
            && afterClick.state === 'disabled-by-policy'
            && afterClick.preferenceOn === true
            && afterClick.probes === 0
            && afterClickDom.chipText.endsWith('llm off by policy')
            && extras.sidecarRequests.length === requestsBeforePolicy;
        extras.policyRequestsDelta = extras.sidecarRequests.length - requestsBeforePolicy;
        log(`Policy: verdict=${policySeam?.policyVerdict}, state=${policySeam?.state}, `
            + `Klick folgenlos=${result.switchIneffective}`);

        /*
         * Der Lese-Weg, ausdruecklich festgehalten (Contract AC4: "dokumentiere
         * den Weg"). Die Datei kommt ueber get_code_snippet auf dem Modul-Knoten
         * der policy.json, also ueber dieselbe Strecke wie jede Datei im Reader.
         * Der Modul-Knoten wird hier noch einmal direkt gefragt, damit im
         * Ergebnis steht, was die Anwendung gefunden hat, und nicht nur, dass sie
         * etwas gefunden hat.
         */
        extras.policyReadPath = await policyPage.evaluate(async (project) => {
            const call = async (name, args) => {
                const response = await fetch('/rpc', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        jsonrpc: '2.0', id: 1, method: 'tools/call',
                        params: { name, arguments: args },
                    }),
                });
                const payload = await response.json();
                return payload?.result?.content?.[0]?.text ?? '';
            };
            return {
                tool: 'get_code_snippet',
                via: 'Modul-Knoten der Datei, wie in src/reader/file-source.ts',
                moduleQuery: await call('query_graph', {
                    project,
                    query: 'MATCH (n:Module) WHERE n.file_path = ".codeatlas/policy.json" '
                        + 'RETURN n.qualified_name, n.start_line, n.end_line LIMIT 1',
                }),
                snippet: (await call('get_code_snippet', {
                    project,
                    qualified_name: `${project}.codeatlas.policy`,
                    format: 'json',
                })).slice(0, 400),
            };
        }, POLICY_PROJECT);

        await policyContext.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w5a] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w5a] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    // Der Sidecar wird auch dann beendet, wenn der Lauf unterwegs gescheitert
    // ist. Ein liegengebliebener llama-server haelt ein Modell im Speicher und
    // den Produktport besetzt.
    if (sidecarStarted) {
        const rescue = await run('sh', [join(ROOT, 'llm', 'stop.sh')]);
        extras.rescueStop = { exit: rescue.code, out: rescue.out.trim().split('\n') };
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopServer(serverChild);
    await sleep(800);

    /*
     * Gezaehlt wird mit Geduld, und die Geduld hat eine Grenze.
     *
     * llm/stop.sh wartet, bis der Prozess weg ist, und meldet dann "beendet".
     * Den Port gibt das Betriebssystem erst danach frei: gemessen 1557 ms.
     * Ein einziger Blick 900 ms nach dem Beenden faellt genau in dieses
     * Fenster, und der Lauf wird rot, obwohl nichts mehr laeuft. Ein
     * Beweislauf, der gelegentlich ohne Grund rot wird, ist schlimmer als
     * keiner: man gewoehnt sich an sein Rot und uebersieht das echte.
     *
     * Fuenf Sekunden Nachsehen ist nicht nachsichtiger, sondern genauer: ein
     * Prozess, der wirklich weiterlaeuft, laeuft auch dann noch. Wie lange es
     * gedauert hat, steht im Artefakt, damit ein langsamer werdender Abbau
     * auffaellt, statt sich hinter der Wartezeit zu verstecken.
     */
    /*
     * Fuenfzehn Sekunden statt fuenf, seit dem 2026-08-30.
     *
     * Gemessen: der Modellport gibt sich im Normalfall 1557 ms, unter Last
     * mehrerer paralleler Laeufe aber mehr als 5128 ms. Beide Male war der
     * Prozess laengst beendet und nur der Port noch belegt. Wer hier zu
     * frueh aufhoert zu warten, meldet einen Rest, den es nicht gibt, und
     * ein Lauf, der gelegentlich grundlos rot wird, ist schlimmer als
     * keiner. Ein Prozess, der wirklich weiterlaeuft, laeuft auch nach
     * fuenfzehn Sekunden noch; die gewartete Zeit steht im Artefakt.
     */
    const FREE_PORT_TIMEOUT_MS = 15000;
    const leftovers = [];
    for (const port of [serverPort, uiPort, SIDECAR_PORT].filter((value) => value > 0)) {
        const startedAt = Date.now();
        let listeners = await countListeners(port);
        while (listeners > 0 && Date.now() - startedAt < FREE_PORT_TIMEOUT_MS) {
            await sleep(250);
            listeners = await countListeners(port);
        }
        leftovers.push({ port, listeners, waitedMs: Date.now() - startedAt });
    }
    extras.leftovers = leftovers;
    result.leftoverProcesses = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
    log('leftoverProcesses:', result.leftoverProcesses, JSON.stringify(leftovers));

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...result,
            sidecarPort: SIDECAR_PORT,
            project: PROJECT,
            policyProject: POLICY_PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen; die Policy liegt in einer Kopie)',
            model: { choice: MODEL_CHOICE, expected: MODEL_NAME },
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = existsSync(SHOT_OFF) && existsSync(SHOT_READY);
    const ok =
        failure === null
        && result.llmOffByDefault === true
        && result.zeroLlmRequestsWhileOff === true
        && result.notRunningHonest === true
        && result.statusReady === true
        && result.chipShown === true
        && result.stopFallsBackHonestly === true
        && result.policyBlocks === true
        && result.switchIneffective === true
        && /Qwen3\.5-2B/.test(result.modelShown)
        && result.port >= MIN_PORT
        && result.leftoverProcesses === 0
        && shotsOk
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    for (const directory of [policyRepo].filter(Boolean)) {
        await rm(directory, { recursive: true, force: true });
    }

    if (!ok) {
        console.error('[smoke-w5a] W5a-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w5a] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W5a-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w5a] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
