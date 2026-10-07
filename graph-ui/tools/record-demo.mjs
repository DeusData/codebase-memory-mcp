#!/usr/bin/env node
/*
 * Die Demo-Aufzeichnung: ein festes Drehbuch, einmal durchgefahren und gefilmt.
 *
 *   node tools/record-demo.mjs        (npm run demo:record)
 *
 * Das Ergebnis ist kein Beweis, sondern eine Vorfuehrung. Alle Beweise stehen
 * schon: tools/smoke-w6-full.mjs faehrt dieselbe Oberflaeche und stellt an jedem
 * Halt die harten Fragen (Netzstille, Ueberlagerung, Beschneidung, Budgets).
 * Dieser Lauf hat eine andere Aufgabe: Martin soll das Produkt SEHEN, ohne es
 * zu installieren. Darum ist hier alles auf Zusehbarkeit ausgelegt und nichts
 * auf Messung.
 *
 * Ablauf:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis, eigene Fixture-Kopie
 *   3. zwei Projekte indizieren: die Fixture (nur gelesen) und eine KOPIE mit
 *      eigenem git-Repository und einer echten Aenderung fuer den Blast-Radius
 *   4. die Aufzeichnung ingestieren, BEVOR die Kamera laeuft
 *   5. C-Server und dist/ auf Ports ab 4350, Sidecar auf 4141 warmlaufen lassen
 *   6. Chromium mit Videoaufnahme, das Drehbuch Halt fuer Halt
 *   7. abraeumen, Restprozesse zaehlen, Video und Drehbuch schreiben
 *
 * ## Fuenf Entscheidungen, die man sonst raten muesste
 *
 * **Der Sidecar startet VOR der Kamera.** llama-server laedt sein Modell in
 * zweistelligen Sekunden, und diese Zeit gehoert in keinen Film. Im Video
 * passiert nur das, was die Oberflaeche tut: der Schalter geht an, das Panel
 * meldet `ready`, die Frage wird beantwortet. Der Prozessstart davor ist keine
 * Produkteigenschaft, sondern eine Vorbedingung, und llm/start.sh sagt selbst,
 * dass er von Hand oder aus einem Lauf kommt (llm/start.sh, Kopf).
 *
 * **Die Zeitmarken zaehlen ab dem ersten Bild und nicht ab dem Prozessstart.**
 * demo.json ist damit ein Inhaltsverzeichnis in das Video: wer `atMs` in den
 * Abspielort umrechnet, landet an genau dem Halt. Eine Marke ab Prozessstart
 * waere um den Bau, das Indizieren und das Modellladen verschoben und damit
 * unbrauchbar.
 *
 * **Jeder Halt haelt sichtbar an.** Ein Beweislauf darf so schnell klicken, wie
 * die Anwendung antwortet; ein Film nicht. Die Pausen stehen als Konstanten
 * beieinander (BEAT und die drei laengeren Fassungen) und nicht verstreut im
 * Drehbuch, damit das Tempo an einer Stelle einstellbar bleibt.
 *
 * **Der Twin haengt am Caret und nicht an der Suche.** Das Drehbuch oeffnet
 * die Route-Datei im Baum und klickt in die Zeile mit dem Aufruf, so wie ein
 * Leser es taete; erst danach kommt die Suche, und zwar auf ein ANDERES Symbol.
 * Damit ist die Kamerafahrt der Galaxy echt: sie faehrt von registerUserRoutes
 * zu createUser. Haette der Twin schon vorher auf createUser gestanden, waere
 * "flyTo" ein Wort ohne Bild.
 *
 * **Die Aenderungsansicht bekommt ihr eigenes Projekt.** Ein Blast-Radius ohne
 * Aenderung ist leer; die Aenderung gehoert aber nicht in fixtures/, das
 * byte-identisch bleibt. Also liegt eine Kopie mit eigenem git-Repository in
 * einem mkdtemp-Verzeichnis, wird als zweites Projekt indiziert und
 * verschwindet am Ende. Im Film ist der Wechsel als Neuladen zu sehen, und das
 * ist ehrlicher als eine Aenderung, die aus dem Nichts kommt.
 */

import { execFile, spawn } from 'node:child_process';
import { existsSync, statSync } from 'node:fs';
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import {
    countListeners,
    findFreePort,
    indexRepository,
    sleep,
    startServer,
    stopServer,
} from './lib/cbm-server.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';
import { startSocketSampler } from './lib/socket-sampler.mjs';

const execFileAsync = promisify(execFile);

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const OUT_DIR = join(ROOT, 'verification', 'w6', 'demo');
const VIDEO_FILE = join(OUT_DIR, 'demo.webm');
const REPORT_FILE = join(OUT_DIR, 'demo.json');
const EVAL_JSON = join(ROOT, 'verification', 'w5', 'eval.json');

const PROJECT = 'codeatlasweb-demo';
const PROJECT_IMPACT = 'codeatlasweb-demo-arbeitskopie';

/**
 * Der Portbereich dieser Aufzeichnung.
 *
 * 4350 aufwaerts, und 4390/4391 bleiben ausgespart: dort laeuft die Vorschau
 * des Nachbarprojekts. Ein Lauf, der sie belegt, nimmt jemandem den Bildschirm
 * weg, waehrend er zusieht.
 */
const MIN_PORT = 4350;
const RESERVED_PORTS = [4390, 4391];

const SIDECAR_PORT = 4141;
const SIDECAR_ORIGIN = `http://127.0.0.1:${SIDECAR_PORT}`;
const SIDECAR_READY_TIMEOUT_MS = 240000;

/** Gross genug fuer die drei Spalten, und das Video hat dieselbe Kantenlaenge. */
const VIEWPORT = { width: 1680, height: 1050 };

/**
 * Das Tempo. Ein Halt haelt an, sonst ist es kein Halt.
 *
 * Vier Laengen und nicht eine: eine Karte mit drei Saetzen braucht laenger als
 * ein Regler, der um eine Stufe wandert, und eine Kamerafahrt braucht laenger
 * als beides. Die Zahlen stehen hier beieinander, damit das Tempo des ganzen
 * Films an einer Stelle einstellbar ist.
 */
const BEAT = 2000;
const BEAT_READ = 4000;
const BEAT_LONG = 5500;
const BEAT_SHOW = 7000;

/** Die Datei, die im Baum geoeffnet wird, und die Zeile, in die geklickt wird. */
const ROUTE_FILE = 'src/routes/users.ts';
const ROUTE_SYMBOL = 'registerUserRoutes';
const ROUTE_CARET_NEEDLE = 'const user = createUser(req.body);';

/** Dieselben zwei Angaben fuer den Dienst, an dem der Chat-Teil steht. */
const SERVICE_FILE = 'src/services/userService.ts';
const SERVICE_CARET_NEEDLE = 'const parsed = validateUser(input);';

/** Das Symbol, zu dem die Suche und die Kamera springen. */
const TARGET = 'createUser';
const TARGET_QUALIFIED = 'userService\\.createUser';

/** Der Einstieg, an dem die Hierarchie gezeigt wird. */
const WALK_ENTRY = 'createUser';

/** Der Lauf, unter dem die Aufzeichnung eingespielt wird. Wie in W4b und W6a. */
const RUN_LABEL = 'demo-run';
const RUN_COUNT = 3;

/** Die Frage an den Atlas. Sie steht hier woertlich, weil sie im Bild steht. */
const QUESTION = 'Wer ruft createUser?';

/** Die Modellwahl, mit der llm/start.sh den Klasse-A-Sieger startet. Wie in W6a. */
const CHOICE_OF = {
    'Qwen3.5-2B': 'class-a',
    'LFM2.5-1.2B': 'class-a-lfm',
    'MiniCPM5-1B': 'class-a-minicpm',
    'Qwen2.5-Coder-1.5B': 'class-a-coder',
    'Qwen3.5-4B': 'class-b',
    'gemma-4-E4B': 'class-b-gemma',
};

/** Die Aenderung an der Kopie, wortgleich mit tools/smoke-w6-full.mjs. */
const ADDED_FUNCTION = `
export function countUsers(): number {
    return listUsers().length;
}
`;
const CHANGED_FILE = 'src/services/userService.ts';
const REWIRED_FROM = 'const entity = new UserEntity(`user-${listUsers().length + 1}`, parsed.email, parsed.name);';
const REWIRED_TO = 'const entity = new UserEntity(`user-${countUsers() + 1}`, parsed.email, parsed.name);';

/** Chromium ohne Aussenwelt, plus die Software-GL-Flags. Wortgleich mit W6a. */
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

const log = (...parts) => console.log('[demo]', ...parts);
const serverLog = [];

function run(command, args, options = {}) {
    return new Promise((done) => {
        const child = spawn(command, args, {
            cwd: options.cwd ?? ROOT,
            env: {
                ...process.env,
                ...(options.env ?? {}),
                NO_UPDATE_NOTIFIER: '1',
                npm_config_update_notifier: 'false',
                npm_config_audit: 'false',
                npm_config_fund: 'false',
            },
            stdio: ['pipe', 'pipe', 'pipe'],
        });
        let out = '';
        let err = '';
        child.stdout.on('data', (chunk) => {
            out += chunk.toString();
        });
        child.stderr.on('data', (chunk) => {
            err += chunk.toString();
        });
        child.on('error', (error) => done({ code: 127, out, err: err + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out, err }));
        if (options.stdin !== undefined) {
            child.stdin.end(options.stdin);
        } else {
            child.stdin.end();
        }
    });
}

/** Ein Werkzeug ueber die CLI, mit dem HOME dieses Laufs. Wortgleich mit W6a. */
async function cli(tool, payload, home) {
    const result = await run(BINARY, ['cli', tool], { env: { HOME: home }, stdin: `${JSON.stringify(payload)}\n` });
    const line = result.out.split('\n').map((entry) => entry.trim()).filter(Boolean).pop();
    if (result.code !== 0 || line === undefined) {
        throw new Error(`cli ${tool} endete mit ${result.code}: ${result.err.trim().slice(-400)}`);
    }
    return JSON.parse(line);
}

/**
 * Die Zeile, in der ein Ausdruck steht, aus der Datei gelesen.
 *
 * Uebernommen aus tools/smoke-w2b.mjs. Eine feste Zahl waere eine zweite
 * Wahrheit ueber das Fixture, die beim naechsten Zeilenumbruch still falsch
 * wird.
 */
function lineContaining(source, needle, where) {
    const index = source.split('\n').findIndex((line) => line.includes(needle));
    if (index < 0) {
        throw new Error(`"${needle}" steht nicht in ${where}`);
    }
    return index + 1;
}

async function main() {
    const totalStarted = Date.now();
    let serverChild = null;
    let proxy = null;
    let browser = null;
    let home = null;
    let runtimeDir = null;
    let workspace = null;
    let videoDir = null;
    let serverPort = 0;
    let uiPort = 0;
    let failure = null;
    let sidecarStarted = false;
    let recordingStarted = 0;

    /** Das Drehbuch, so wie es wirklich gefahren wurde. */
    const steps = [];
    const timings = {};
    const extras = {
        blockedRequests: [],
        consoleErrors: [],
        pageErrors: [],
        failedResponses: [],
    };
    const result = {
        steps,
        durationMs: 0,
        leftoverProcesses: 0,
        videoBytes: 0,
        outboundViolations: 0,
        samples: 0,
        pageErrors: 0,
        consoleErrors: 0,
        citationFollowed: false,
    };

    const sampler = startSocketSampler({ intervalMs: 1000 });

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
        if (!existsSync(EVAL_JSON)) {
            throw new Error(`${EVAL_JSON} fehlt: erst 'npm run eval:llm' fahren.`);
        }
        const evalReport = JSON.parse(await readFile(EVAL_JSON, 'utf8'));
        const winner = evalReport?.winnerClassA?.name ?? '';
        const choice = CHOICE_OF[winner];
        if (choice === undefined) {
            throw new Error(`unbekannter Klasse-A-Sieger in eval.json: "${winner}"`);
        }
        extras.model = { name: winner, choice };
        log(`Modell aus eval.json: ${winner} (llm/start.sh ${choice})`);

        // ------------------------------------------------------------ 1. Bau
        log('npm run build');
        const buildStarted = Date.now();
        const build = await run('npm', ['run', 'build']);
        timings.buildMs = Date.now() - buildStarted;
        if (build.code !== 0) {
            throw new Error(`npm run build endete mit ${build.code}: ${(build.out + build.err).trim().slice(-800)}`);
        }
        if (!existsSync(join(DIST, 'index.html'))) {
            throw new Error('dist/index.html fehlt nach dem Build');
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-demo-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-demo-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        workspace = await mkdtemp(join(tmpdir(), 'codeatlasweb-demo-fixture-'));
        videoDir = await mkdtemp(join(tmpdir(), 'codeatlasweb-demo-video-'));
        log('isoliertes HOME:', home);
        log('Rendezvous:', runtimeDir);

        // -------------------------------------- 3. Die zwei Projekte im Index
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };

        const repo = join(workspace, 'atlas-sample');
        await cp(FIXTURE, repo, { recursive: true });
        const git = (...args) =>
            execFileAsync('git', args, {
                cwd: repo,
                env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_SYSTEM: '/dev/null' },
            });
        await git('init', '-q', '-b', 'main');
        await git('config', 'user.name', 'codeatlasweb-demo');
        await git('config', 'user.email', 'demo@localhost');
        await git('add', '-A');
        await git('commit', '-q', '-m', 'the fixture as it stands');
        const changedFile = join(repo, CHANGED_FILE);
        const original = await readFile(changedFile, 'utf8');
        if (!original.includes(REWIRED_FROM)) {
            throw new Error(`die Fixture-Kopie enthaelt die erwartete Zeile nicht: ${REWIRED_FROM}`);
        }
        await writeFile(changedFile, original.replace(REWIRED_FROM, REWIRED_TO) + ADDED_FUNCTION, 'utf8');
        const indexedImpact = await indexRepository(BINARY, { home, repoPath: repo, project: PROJECT_IMPACT });
        extras.indexedImpact = { nodes: indexedImpact.nodes, edges: indexedImpact.edges };
        log(`indiziert: ${indexed.nodes} Knoten (Fixture), ${indexedImpact.nodes} Knoten (Arbeitskopie)`);

        // ---------------------------- 4. Die Aufzeichnung, vor der Vorfuehrung
        const qn = (suffix) => `${PROJECT}.${suffix}`;
        const ingest = await cli(
            'ingest_traces',
            {
                project: PROJECT,
                label: RUN_LABEL,
                traces: [
                    {
                        path: [
                            qn('src.routes.users.registerUserRoutes'),
                            qn('src.services.userService.createUser'),
                            qn('src.util.validate.validateUser'),
                        ],
                        count: RUN_COUNT,
                    },
                    {
                        caller: qn('src.services.userService.listUsers'),
                        callee: qn('src.util.validate.validateUser'),
                    },
                ],
            },
            home,
        );
        extras.ingest = ingest;
        log(`ingest_traces: ${ingest.pairs_stored} Paare, ${ingest.paths_stored} Pfade`);

        // ------------------------------------------------- 5. Server, Sidecar
        serverPort = await findFreePort(MIN_PORT, RESERVED_PORTS);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [...RESERVED_PORTS, serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        extras.serverPort = serverPort;
        extras.uiPort = uiPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        const busyBefore = await countListeners(SIDECAR_PORT);
        if (busyBefore > 0) {
            throw new Error(`auf ${SIDECAR_PORT} lauscht schon etwas (${busyBefore}); erst llm/stop.sh fahren`);
        }
        const sidecarStartedAt = Date.now();
        const startRun = await run('sh', [join(ROOT, 'llm', 'start.sh'), choice]);
        sidecarStarted = true;
        if (startRun.code !== 0) {
            throw new Error(`llm/start.sh endete mit ${startRun.code}: ${(startRun.out + startRun.err).slice(-400)}`);
        }
        const healthDeadline = Date.now() + SIDECAR_READY_TIMEOUT_MS;
        for (;;) {
            let status = 0;
            try {
                const probe = await fetch(`${SIDECAR_ORIGIN}/health`);
                status = probe.status;
                await probe.arrayBuffer();
            } catch {
                status = 0;
            }
            if (status === 200) {
                break;
            }
            if (Date.now() > healthDeadline) {
                throw new Error(`der Sidecar auf ${SIDECAR_PORT} war nicht bereit`);
            }
            await sleep(500);
        }
        timings.sidecarReadyMs = Date.now() - sidecarStartedAt;
        log(`Sidecar bereit nach ${timings.sidecarReadyMs} ms (vor der Kamera, mit Absicht)`);

        // ---------------------------------------------- 6. Kamera und Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const origin = `http://127.0.0.1:${uiPort}`;
        const context = await browser.newContext({
            viewport: { ...VIEWPORT },
            recordVideo: { dir: videoDir, size: { ...VIEWPORT } },
        });
        await context.route('**/*', async (route) => {
            const url = route.request().url();
            if (url.startsWith(SIDECAR_ORIGIN) || url.startsWith(origin)
                || url.startsWith('data:') || url.startsWith('blob:')) {
                await route.continue();
                return;
            }
            extras.blockedRequests.push(url);
            await route.abort();
        });

        const page = await context.newPage();
        page.on('console', (message) => {
            if (message.type() === 'error') {
                const where = message.location();
                extras.consoleErrors.push({
                    text: message.text(),
                    url: where?.url ?? '',
                    line: where?.lineNumber ?? 0,
                });
            }
        });
        page.on('response', (response) => {
            if (response.status() >= 400) {
                extras.failedResponses.push({ url: response.url(), status: response.status() });
            }
        });
        page.on('pageerror', (error) => extras.pageErrors.push(String(error)));

        recordingStarted = Date.now();

        // -------------------------------------------------- Die Werkzeuge --

        /** Ein benannter Halt: der Moment, in dem das Bild steht, plus die Ruhe. */
        const halt = async (name, holdMs = BEAT) => {
            const atMs = Date.now() - recordingStarted;
            steps.push({ step: steps.length + 1, name, atMs });
            log(`Halt ${String(steps.length).padStart(2, '0')} ${name} @ ${(atMs / 1000).toFixed(1)} s`);
            await page.waitForTimeout(holdMs);
        };

        const openApp = async (project) => {
            await page.goto(`${origin}/?project=${project}`, { waitUntil: 'load' });
            await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
            await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
            await page.waitForSelector('[data-testid="atlas-galaxy-scene"]', { timeout: 60000 });
            await page.waitForFunction(
                () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
                undefined,
                { timeout: 60000 },
            );
        };

        const expandAll = async () => {
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
        };

        const typeQuery = async (name) => {
            const input = page.locator('[data-testid="atlas-command-input"]');
            await input.click();
            await input.fill('');
            await input.pressSequentially(name, { delay: 55 });
            await page.waitForSelector(
                `[data-testid="atlas-search-row"][data-name="${name}"]`,
                { timeout: 30000 },
            );
        };

        const chooseHit = async (name, expectQualified) => {
            await page.click(`[data-testid="atlas-search-row"][data-name="${name}"]`);
            await page.waitForFunction(
                (expected) => new RegExp(`${expected}$`).test(globalThis.__atlasTwin?.qualifiedName ?? ''),
                expectQualified,
                { timeout: 60000 },
            );
            await page.waitForFunction(
                () => document.querySelector('[data-testid="atlas-twin"]')
                    ?.getAttribute('data-status') === 'ready',
                undefined,
                { timeout: 60000 },
            );
        };

        const setDepth = async (depth) => {
            await page.locator('[data-testid="atlas-twin-depth"]').fill(String(depth));
            await page.waitForFunction(
                (expected) =>
                    document.querySelector('[data-testid="atlas-twin-depth"]')?.value === String(expected),
                depth,
                { timeout: 10000 },
            );
        };

        const pressGlobally = async (key) => {
            await page.click('.atlas-brand');
            await page.keyboard.press(key);
        };

        /**
         * In eine Quelltextzeile klicken, so wie ein Leser es tut.
         *
         * Uebernommen aus tools/smoke-w2b.mjs: Monaco zeichnet jede sichtbare
         * Zeile als eigenes `.view-line`, und nach dem Klick wird am Editor
         * selbst nachgelesen, wo der Caret gelandet ist.
         */
        const clickSourceLine = async (needle, expectedLine) => {
            await page.locator('.atlas-reader-editor .view-line', { hasText: needle }).first().click();
            await page.waitForFunction(
                (line) => globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber === line,
                expectedLine,
                { timeout: 15000 },
            );
        };

        // ------------------------------------------------------ Das Drehbuch

        // 1. Start: Chrome, Baum, Galaxy.
        await openApp(PROJECT);
        await halt('start-chrome-and-galaxy', BEAT_LONG);
        await expandAll();
        await halt('explorer-tree-expanded', BEAT_READ);

        // 2. Das Why-Panel und die Fuehrung durch das Projekt.
        await page.click('[data-menu="a-why"]');
        await page.waitForSelector('[data-testid="atlas-why"]', { timeout: 20000 });
        await halt('why-panel-open', BEAT_READ);

        await page.click('[data-testid="atlas-why-card"][data-intent="understand"]');
        await page.waitForSelector('[data-testid="atlas-tour"]', { timeout: 60000 });
        await page.waitForFunction(() => (globalThis.__atlasTour?.steps ?? 0) > 0, undefined, { timeout: 60000 });
        await halt('why-understand-starts-the-tour', BEAT_LONG);

        for (const step of [1, 2]) {
            await pressGlobally('Enter');
            await page.waitForFunction(
                (expected) => globalThis.__atlasTour?.index === expected,
                step,
                { timeout: 40000 },
            );
            await halt(`tour-step-${step + 1}`, BEAT_LONG);
        }
        extras.tour = await page.evaluate(() => ({
            kind: globalThis.__atlasTour?.kind ?? '',
            steps: globalThis.__atlasTour?.steps ?? 0,
            index: globalThis.__atlasTour?.index ?? -1,
            title: globalThis.__atlasTour?.title ?? '',
        }));

        // 3. Die Fuehrung verlassen: q.
        await pressGlobally('q');
        await page.waitForSelector('[data-testid="atlas-tour"]', { state: 'detached', timeout: 20000 });
        await halt('tour-left-with-q', BEAT);

        // 4. Eine Datei oeffnen und den Caret in die Aufrufzeile setzen.
        const routeSource = await readFile(join(FIXTURE, ROUTE_FILE), 'utf8');
        const caretLine = lineContaining(routeSource, ROUTE_CARET_NEEDLE, ROUTE_FILE);
        await page.click(`[data-testid="atlas-tree-row"][data-path="${ROUTE_FILE}"]`);
        await page.waitForFunction(
            (expected) => globalThis.__atlasReader?.document?.path === expected,
            ROUTE_FILE,
            { timeout: 40000 },
        );
        await halt('reader-opens-the-route-file', BEAT_READ);

        await clickSourceLine(ROUTE_CARET_NEEDLE, caretLine);
        await page.waitForFunction(
            (expected) => (globalThis.__atlasTwin?.qualifiedName ?? '').endsWith(expected),
            ROUTE_SYMBOL,
            { timeout: 60000 },
        );
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-twin"]')?.getAttribute('data-status') === 'ready',
            undefined,
            { timeout: 60000 },
        );
        await halt('twin-follows-the-caret', BEAT_READ);

        // 5. Die vier Tiefen des Twins.
        for (const depth of [0, 1, 2, 3]) {
            await setDepth(depth);
            await halt(`twin-depth-${depth}`, BEAT_READ);
        }

        // 6. Der flow()-Kasten und zwei Schritte im Stepper.
        await page.click('[data-testid="atlas-twin-subject"]');
        await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { timeout: 30000 });
        await page.waitForFunction(
            () => Number(document.querySelector('[data-testid="atlas-flow"]')
                ?.getAttribute('data-arrows') ?? '0') > 0,
            undefined,
            { timeout: 60000 },
        );
        extras.flow = await page.evaluate(() => {
            const box = document.querySelector('[data-testid="atlas-flow"]');
            return {
                arrows: Number(box?.getAttribute('data-arrows') ?? '0'),
                steps: Number(box?.getAttribute('data-steps') ?? '0'),
                title: document.querySelector('[data-testid="atlas-flow-overlay-title"]')?.textContent?.trim() ?? '',
            };
        });
        await halt('flow-box-open', BEAT_LONG);

        for (const step of [0, 1]) {
            await page.click('[data-testid="atlas-flow-next"]');
            await page.waitForFunction(
                (expected) => document.querySelector('[data-testid="atlas-flow"]')
                    ?.getAttribute('data-active-step') === String(expected),
                step,
                { timeout: 15000 },
            );
            await halt(`flow-stepper-step-${step + 1}`, BEAT_READ);
        }
        await page.keyboard.press('Escape');
        await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { state: 'detached', timeout: 15000 })
            .catch(() => undefined);

        // 7. Der Pseudocode-Tab und zurueck zu den Fakten.
        await page.click('[data-testid="atlas-pseudocode-toggle"]');
        await page.waitForSelector('[data-testid="atlas-pseudocode"]', { timeout: 20000 });
        extras.pseudocode = await page.evaluate(() => ({
            lines: document.querySelectorAll('[data-testid="atlas-pseudocode-line"]').length,
            imports: document.querySelectorAll('[data-testid="atlas-pseudocode-import"]').length,
        }));
        await halt('pseudocode-tab', BEAT_SHOW);
        await page.click('[data-testid="atlas-twin-tab-facts"]');
        await page.waitForTimeout(300);

        // 8. Suche createUser, Enter, und die Kamera faehrt hin.
        const beforeFly = await page.evaluate(() => ({
            targetChanges: globalThis.__atlasGalaxy?.targetChanges ?? 0,
            lastTargetQn: globalThis.__atlasGalaxy?.lastTargetQn ?? '',
        }));
        await typeQuery(TARGET);
        await halt('search-createUser', BEAT);
        await page.locator('[data-testid="atlas-command-input"]').press('Enter');
        await page.waitForFunction(
            (expected) => new RegExp(`${expected}$`).test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            TARGET_QUALIFIED,
            { timeout: 60000 },
        );
        await halt('search-enter-opens-createUser', BEAT_READ);

        await page.waitForFunction(
            (before) => (globalThis.__atlasGalaxy?.targetChanges ?? 0) > before,
            beforeFly.targetChanges,
            { timeout: 60000 },
        );
        extras.flyTo = await page.evaluate(() => ({
            targetChanges: globalThis.__atlasGalaxy?.targetChanges ?? 0,
            lastTargetQn: globalThis.__atlasGalaxy?.lastTargetQn ?? '',
            highlightedCount: globalThis.__atlasGalaxy?.highlightedCount ?? 0,
        }));
        extras.flyTo.from = beforeFly.lastTargetQn;
        await halt('galaxy-flies-to-the-symbol', BEAT_SHOW);

        // 9. Der Entry-Modus und die Hierarchie-Ansicht des Walks.
        await page.click('[data-menu="a-why"]');
        await page.waitForSelector('[data-testid="atlas-why"]', { timeout: 20000 });
        await page.click('[data-testid="atlas-why-card"][data-intent="entry"]');
        await page.waitForSelector('[data-testid="atlas-entry"]', { timeout: 40000 });
        await page.waitForSelector('[data-testid="atlas-entry-row"]', { timeout: 60000 });
        await halt('entry-mode-dialog', BEAT_READ);

        const entryInput = page.locator('[data-testid="atlas-entry-input"]');
        await entryInput.click();
        await entryInput.fill('');
        await entryInput.pressSequentially(WALK_ENTRY, { delay: 55 });
        await page.waitForSelector(
            `[data-testid="atlas-entry-hit"][data-name="${WALK_ENTRY}"]`,
            { timeout: 30000 },
        );
        await page.waitForTimeout(600);
        await page.click(`[data-testid="atlas-entry-hit"][data-name="${WALK_ENTRY}"]`);
        await page.waitForFunction(() => globalThis.__atlasTour?.kind === 'entry', undefined, { timeout: 60000 });
        await page.waitForFunction(
            () => (globalThis.__atlasGalaxy?.hierarchy?.nodes ?? 0) > 0,
            undefined,
            { timeout: 60000 },
        );
        extras.hierarchy = await page.evaluate(() => ({
            mode: globalThis.__atlasGalaxy?.mode ?? '',
            nodes: globalThis.__atlasGalaxy?.hierarchy?.nodes ?? 0,
            depth: globalThis.__atlasGalaxy?.hierarchy?.depth ?? 0,
        }));
        await halt('hierarchy-view-of-the-walk', BEAT_SHOW);

        await pressGlobally('Enter');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 1, undefined, { timeout: 40000 });
        await halt('hierarchy-walk-step-2', BEAT_LONG);
        await pressGlobally('q');
        await page.waitForSelector('[data-testid="atlas-tour"]', { state: 'detached', timeout: 20000 });

        // 10. Der BUG-Wizard, mit der Aufzeichnung von vorhin.
        await typeQuery(TARGET);
        await chooseHit(TARGET, TARGET_QUALIFIED);
        await page.click('[data-menu="a-bug"]');
        await page.waitForSelector('[data-testid="atlas-bugwizard"]', { timeout: 30000 });
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-bugwizard"]')
                ?.getAttribute('data-status') === 'ready',
            undefined,
            { timeout: 90000 },
        );
        extras.wizard = await page.evaluate(() => {
            const panel = document.querySelector('[data-testid="atlas-bugwizard"]');
            return {
                events: Number(panel?.getAttribute('data-events') ?? '0'),
                staticPaths: Number(panel?.getAttribute('data-static-paths') ?? '0'),
                observedPaths: Number(panel?.getAttribute('data-observed-paths') ?? '0'),
                staticOnly: Number(panel?.getAttribute('data-static-only') ?? '0'),
                runtimeOnly: Number(panel?.getAttribute('data-runtime-only') ?? '0'),
            };
        });
        await halt('bug-wizard-shows-the-divergence', BEAT_SHOW);
        await page.click('[data-testid="atlas-bugwizard-close"]');
        await page.waitForTimeout(400);

        // 11. Der Blast-Radius, an der Arbeitskopie mit der echten Aenderung.
        await openApp(PROJECT_IMPACT);
        await page.click('[data-menu="a-impact"]');
        await page.waitForSelector('[data-testid="atlas-impact"]', { timeout: 30000 });
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-impact"]')
                ?.getAttribute('data-status') === 'ready',
            undefined,
            { timeout: 120000 },
        );
        extras.impact = await page.evaluate(() => {
            const panel = document.querySelector('[data-testid="atlas-impact"]');
            return {
                badge: panel?.getAttribute('data-badge') ?? '',
                direct: Number(panel?.getAttribute('data-direct') ?? '0'),
                downstream: Number(panel?.getAttribute('data-downstream') ?? '0'),
                endpoints: Number(panel?.getAttribute('data-endpoints') ?? '0'),
            };
        });
        await halt('impact-blast-radius', BEAT_SHOW);
        await page.click('[data-testid="atlas-impact-close"]');
        await page.waitForTimeout(400);

        /*
         * 12. Der Sidecar an, eine Frage, ein Zitat, und wieder aus.
         *
         * Vor der Frage wird der Dienst geoeffnet und der Caret hineingesetzt.
         * Nicht der Schoenheit wegen: der Context-Compiler baut seine Karten um
         * das Symbol vor dem Leser herum, und eine Frage ohne Fokus waere eine
         * andere Frage als die, die ein Leser wirklich stellt.
         */
        await openApp(PROJECT);
        await expandAll();
        const serviceSource = await readFile(join(FIXTURE, SERVICE_FILE), 'utf8');
        const serviceCaretLine = lineContaining(serviceSource, SERVICE_CARET_NEEDLE, SERVICE_FILE);
        await page.click(`[data-testid="atlas-tree-row"][data-path="${SERVICE_FILE}"]`);
        await page.waitForFunction(
            (expected) => globalThis.__atlasReader?.document?.path === expected,
            SERVICE_FILE,
            { timeout: 40000 },
        );
        await clickSourceLine(SERVICE_CARET_NEEDLE, serviceCaretLine);
        await page.waitForFunction(
            (expected) => new RegExp(`${expected}$`).test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            TARGET_QUALIFIED,
            { timeout: 60000 },
        );
        await page.waitForFunction(
            () => (globalThis.__atlasLlm?.policyVerdict ?? '') !== '',
            undefined,
            { timeout: 60000 },
        );
        await halt('chat-sidecar-still-off', BEAT_READ);

        await page.click('[data-menu="a-llm"]');
        await page.waitForFunction(
            () => globalThis.__atlasLlm?.state === 'ready',
            undefined,
            { timeout: SIDECAR_READY_TIMEOUT_MS },
        );
        extras.llmReady = await page.evaluate(() => ({ ...globalThis.__atlasLlm }));
        await halt('chat-sidecar-ready', BEAT_READ);

        const askStarted = Date.now();
        const commandInput = page.locator('[data-testid="atlas-command-input"]');
        await commandInput.click();
        await commandInput.fill('');
        await commandInput.pressSequentially(QUESTION, { delay: 45 });
        await page.waitForTimeout(700);
        await commandInput.press('Enter');
        await page.waitForFunction(
            () => (globalThis.__atlasChat?.turns ?? []).some(
                (turn) => turn.question.startsWith('Wer ruft') && turn.status === 'answered',
            ),
            undefined,
            { timeout: 240000 },
        );
        timings.answerMs = Date.now() - askStarted;
        extras.answer = await page.evaluate(() => {
            const turn = (globalThis.__atlasChat?.turns ?? []).find(
                (entry) => entry.question.startsWith('Wer ruft') && entry.status === 'answered',
            );
            return turn === undefined ? null : {
                klass: turn.klass,
                cards: turn.cards,
                citations: turn.citations,
                answerLength: turn.answer.length,
                tokensPerSecond: turn.tokensPerSecond,
            };
        });
        await halt('chat-answer-with-citations', BEAT_SHOW);

        const beforeCitation = await page.evaluate(() => ({
            path: globalThis.__atlasReader?.document?.path ?? '',
            line: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
        }));
        const citation = page.locator('[data-testid="atlas-chat-citation"]').first();
        if (await citation.count() > 0) {
            await citation.click();
            await page.waitForTimeout(2500);
            const afterCitation = await page.evaluate(() => ({
                path: globalThis.__atlasReader?.document?.path ?? '',
                line: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
            }));
            extras.citationClick = { before: beforeCitation, after: afterCitation };
            result.citationFollowed = afterCitation.path.length > 0
                && (afterCitation.path !== beforeCitation.path || afterCitation.line !== beforeCitation.line);
        }
        await halt('chat-citation-opens-the-source', BEAT_SHOW);

        await page.click('[data-menu="a-llm"]');
        await page.waitForFunction(
            () => globalThis.__atlasLlm?.state === 'off',
            undefined,
            { timeout: 30000 },
        );
        await halt('chat-sidecar-off-again', BEAT_LONG);

        result.durationMs = Date.now() - recordingStarted;

        // Das Video wird erst beim Schliessen des Kontexts fertiggeschrieben.
        const video = page.video();
        await context.close();
        if (video === null) {
            throw new Error('Playwright hat kein Video aufgezeichnet');
        }
        await mkdir(OUT_DIR, { recursive: true });
        await rm(VIDEO_FILE, { force: true });
        await video.saveAs(VIDEO_FILE);
        await video.delete().catch(() => undefined);
        result.videoBytes = statSync(VIDEO_FILE).size;
        log(`Video geschrieben: ${VIDEO_FILE} (${(result.videoBytes / (1024 * 1024)).toFixed(2)} MB, `
            + `${(result.durationMs / 1000).toFixed(1)} s)`);
    } catch (err) {
        failure = err;
        console.error('[demo] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[demo] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (sidecarStarted) {
        const rescue = await run('sh', [join(ROOT, 'llm', 'stop.sh')]);
        extras.stopScript = { exit: rescue.code, out: (rescue.out + rescue.err).trim().split('\n').slice(-6) };
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopServer(serverChild);
    await sleep(900);

    const leftovers = [];
    for (const port of [serverPort, uiPort, SIDECAR_PORT].filter((value) => value > 0)) {
        leftovers.push({ port, listeners: await countListeners(port) });
    }
    extras.leftovers = leftovers;
    result.leftoverProcesses = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);

    const netz = await sampler.stop();
    result.samples = netz.samples;
    result.outboundViolations = netz.outboundViolations;
    extras.net = netz;

    result.consoleErrors = extras.consoleErrors.length;
    result.pageErrors = extras.pageErrors.length;
    timings.totalMs = Date.now() - totalStarted;

    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        REPORT_FILE,
        JSON.stringify({
            ...result,
            video: {
                path: 'verification/w6/demo/demo.webm',
                width: VIEWPORT.width,
                height: VIEWPORT.height,
                bytes: result.videoBytes,
            },
            timeline:
                'atMs zaehlt ab dem ersten Bild der Aufzeichnung, nicht ab dem Start des Laufs: '
                + 'so ist jeder Halt im Video an seiner Marke zu finden.',
            projects: { main: PROJECT, impact: PROJECT_IMPACT },
            fixture: 'fixtures/atlas-sample (nur gelesen), plus eine Kopie mit eigenem '
                + 'git-Repository und einer echten Aenderung fuer den Blast-Radius',
            question: QUESTION,
            sidecar: {
                port: SIDECAR_PORT,
                note: 'llama-server laeuft vor der Aufzeichnung; im Video ist nur der Schalter '
                    + 'der Oberflaeche zu sehen und nicht das Laden des Modells.',
            },
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', REPORT_FILE);

    const ok =
        failure === null
        && steps.length >= 15
        && result.videoBytes > 1024 * 1024
        && result.leftoverProcesses === 0
        && result.outboundViolations === 0
        && result.pageErrors === 0
        && result.consoleErrors === 0
        && result.citationFollowed === true
        && extras.blockedRequests.length === 0;

    log(`Halte ${steps.length}, Dauer ${(result.durationMs / 1000).toFixed(1)} s, `
        + `Restprozesse ${result.leftoverProcesses}, Stichproben ${result.samples}`);

    if (!ok) {
        console.error('[demo] Die Aufzeichnung ist NICHT gruen.');
        for (const entry of extras.consoleErrors.slice(0, 10)) {
            console.error('  console:', JSON.stringify(entry));
        }
        for (const entry of extras.failedResponses.slice(0, 10)) {
            console.error('  response:', entry.status, entry.url);
        }
        for (const entry of extras.pageErrors.slice(0, 10)) {
            console.error('  pageerror:', entry);
        }
        if (home) {
            console.error('[demo] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir, workspace, videoDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log(`Aufzeichnung gruen: ${steps.length} Halte in ${(result.durationMs / 1000).toFixed(1)} s Video.`);
}

main().catch((err) => {
    console.error('[demo] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
