#!/usr/bin/env node
/*
 * W6a-Smoke: die GANZE Klickstrecke des Produkts, in einem Lauf, ohne Netz.
 *
 * Bis W5c hatte jeder Zyklus seinen eigenen Beweislauf, und jeder davon fuhr
 * ein Stueck der Oberflaeche. Zusammen decken sie alles ab, und trotzdem fehlt
 * eine Aussage: dass die Stuecke NEBENEINANDER funktionieren. Ein Panel, das
 * allein gruen ist und neben dem Nachbarn ueberlaeuft, ist in keinem der
 * einzelnen Laeufe rot. Dieser Lauf faehrt darum alles hintereinander, in
 * einem Browser, auf einem Server, mit einem Index, und stellt an jedem Halt
 * dieselben drei Fragen an das Bild.
 *
 * Ablauf:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis
 *   3. drei Projekte indizieren: die Fixture (nur gelesen), die grosse Fixture
 *      fuer die gekappte Datei (nur gelesen), und eine KOPIE der Fixture mit
 *      eigenem git-Repository und einer echten Aenderung fuer den Blast-Radius
 *   4. C-Server auf einem freien Port >= 4340, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre; 127.0.0.1:4141 ist
 *      ausdruecklich erlaubt und wird mitgezaehlt
 *   6. die Strecke, Halt fuer Halt und jeder benannt
 *   7. abraeumen, Restprozesse zaehlen, drei JSON und die Bilder schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w6).
 *
 * ## Fuenf Entscheidungen, die man sonst raten muesste
 *
 * **Drei Projekte statt eines.** Die Strecke braucht drei verschiedene
 * Ausgangslagen: eine unberuehrte Fixture fuer alles Lesende, eine grosse Datei
 * fuer die Kappungszeile (die Aussage "hier hoert das Geladene auf" ist an einer
 * 42-Zeilen-Datei nicht zu sehen), und eine geaenderte Kopie fuer die
 * Aenderungsansicht. fixtures/ bleibt byte-identisch; die Kopie liegt in einem
 * mkdtemp-Verzeichnis und verschwindet am Ende.
 *
 * **Der Lauf misst die Netzstille selbst.** Warum er sie nicht vom Gate
 * abschreibt, steht im Kopf von tools/lib/socket-sampler.mjs. Kurz: das Gate
 * schreibt sein Ergebnis erst, wenn dieser Lauf schon vorbei ist.
 *
 * **Das Lesbarkeits-Gate laeuft an JEDEM Halt, nicht an ausgewaehlten.** Die
 * Regeln stehen in tools/lib/readability.mjs. Ein Halt, an dem nur ein
 * Screenshot faellt, ist ein Halt, an dem niemand hingesehen hat.
 *
 * **Die warme Twin-Messung nimmt zehn VERSCHIEDENE Symbole.** Dasselbe Symbol
 * zweimal zu oeffnen misst den Cache in src/twin/ir-cache.ts und nicht den
 * Server. Warm heisst hier: der Server laeuft, sein Index ist gelesen, seine
 * Seiten sind im Cache des Betriebssystems. Gemessen wird vom Klick auf den
 * Treffer bis zu dem Moment, in dem der Twin das neue Subjekt fertig zeigt; das
 * Tippen und die Entprellung davor stehen als `searchOverlayMs` getrennt.
 *
 * **Der Versions-Chip wird abgelesen, nicht gerechnet.** Er zeigt seit W6a nur
 * die Fassung; der Zustand des Arbeitsbaums steht als eigenes Element daneben
 * (src/app/build-info.ts). Beides wird gelesen und beides steht im Ergebnis,
 * damit ein Bild dieses Laufs einer Fassung UND einem Baumzustand zuzuordnen
 * ist.
 */

import { execFile, spawn } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
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
import {
    DELIBERATE_OVERLAYS,
    READABILITY_EXCLUSIONS,
    measureReadability,
    resetScroll,
    scrollRegionsToEnd,
} from './lib/readability.mjs';

const execFileAsync = promisify(execFile);

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const FIXTURE_LARGE = join(ROOT, 'fixtures', 'atlas-sample-large');
const OUT_DIR = join(ROOT, 'verification', 'w6');
const WALK_DIR = join(OUT_DIR, 'walk');
const AIRGAP_JSON = join(OUT_DIR, 'airgap.json');
const BUDGETS_JSON = join(OUT_DIR, 'budgets.json');
const EVAL_JSON = join(ROOT, 'verification', 'w5', 'eval.json');

/**
 * Die Fassung, die der Chip zeigen muss, aus der einen Stelle, an der sie steht.
 *
 * Bis zum 2026-08-29 stand hier `'v1.0.0'` als Zahl in der Bedingung. Das war
 * derselbe Fehler, gegen den dieser Zyklus geschrieben ist, nur eine Ebene
 * tiefer: eine Behauptung ueber das Produkt an einem zweiten Ort, die falsch
 * wird, sobald der erste sich aendert. Als der Eigentuemer die Fassung auf
 * 0.0.1 setzte, war die Messung richtig und die Bedingung falsch. Die Zahl hat
 * genau einen Ort, und das ist package.json.
 */
const EXPECTED_VERSION_CHIP = `v${JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf8')).version}`;

const PROJECT = 'codeatlasweb-w6';
const PROJECT_LARGE = 'codeatlasweb-w6-large';
const PROJECT_IMPACT = 'codeatlasweb-w6-impact';
const MIN_PORT = 4340;

const SIDECAR_PORT = 4141;
const SIDECAR_ORIGIN = `http://127.0.0.1:${SIDECAR_PORT}`;
const READY_TIMEOUT_MS = 240000;

const VIEWPORT = { width: 1680, height: 1050 };

/** Das Symbol, an dem Twin, Flow und Pseudocode gezeigt werden. */
const TARGET = 'createUser';
const TARGET_FILE = 'src/services/userService.ts';
const TARGET_QUALIFIED = 'userService\\.createUser';

/** Die grosse Datei, an der die Kappungszeile zu sehen ist. */
const LARGE_FILE = 'src/big.ts';

/** Die zehn verschiedenen Symbole der warmen Messung. */
const WARM_SYMBOLS = [
    'listUsers', 'validateUser', 'getOrder', 'hotspotScan', 'insert',
    'query', 'walk', 'registerUserRoutes', 'main', 'createUser',
];

/** Der Einstieg, an dem die Hierarchie gezeigt wird. */
const WALK_ENTRY = 'createUser';

/** Der Lauf, unter dem die Aufzeichnung eingespielt wird. Wie in W4b. */
const RUN_LABEL = 'w6-run';
const RUN_COUNT = 3;

/** Die Frage an den Atlas. Sie steht hier woertlich, weil sie der Beweis ist. */
const QUESTION = 'Wer ruft createUser?';

/** Die Modellwahl, mit der llm/start.sh den Klasse-A-Sieger startet. Wie in W5b. */
const CHOICE_OF = {
    'Qwen3.5-2B': 'class-a',
    'LFM2.5-1.2B': 'class-a-lfm',
    'MiniCPM5-1B': 'class-a-minicpm',
    'Qwen2.5-Coder-1.5B': 'class-a-coder',
    'Qwen3.5-4B': 'class-b',
    'gemma-4-E4B': 'class-b-gemma',
};

/**
 * Die Aenderung an der Kopie, wortgleich mit tools/smoke-w4b.mjs.
 *
 * Beide Haelften sind noetig, und der Grund ist eine gemessene Eigenschaft des
 * Servers: eine nur angehaengte Funktion, die niemand ruft, hat einen leeren
 * Blast-Radius. Erst der Aufruf aus createUser heraus macht createUser zum
 * Ausgangspunkt, und dessen Aufrufer sind die Route-Datei und der Server.
 */
const ADDED_FUNCTION = `
export function countUsers(): number {
    return listUsers().length;
}
`;
const REWIRED_FROM = 'const entity = new UserEntity(`user-${listUsers().length + 1}`, parsed.email, parsed.name);';
const REWIRED_TO = 'const entity = new UserEntity(`user-${countUsers() + 1}`, parsed.email, parsed.name);';

/** Chromium ohne Aussenwelt, plus die Software-GL-Flags. Wortgleich mit W5c. */
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

const log = (...parts) => console.log('[smoke-w6]', ...parts);
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

/** Ein Werkzeug ueber die CLI, mit dem HOME dieses Laufs. Wortgleich mit W4b. */
async function cli(tool, payload, home) {
    const result = await run(BINARY, ['cli', tool], { env: { HOME: home }, stdin: `${JSON.stringify(payload)}\n` });
    const line = result.out.split('\n').map((entry) => entry.trim()).filter(Boolean).pop();
    if (result.code !== 0 || line === undefined) {
        throw new Error(`cli ${tool} endete mit ${result.code}: ${result.err.trim().slice(-400)}`);
    }
    return JSON.parse(line);
}

/** Der Percentil-Wert einer Messreihe, nach der Nearest-Rank-Methode. */
function percentile(values, p) {
    if (values.length === 0) {
        return Number.NaN;
    }
    const sorted = [...values].sort((a, b) => a - b);
    const rank = Math.ceil((p / 100) * sorted.length);
    return sorted[Math.min(Math.max(rank, 1), sorted.length) - 1];
}

async function main() {
    const totalStarted = Date.now();
    let serverChild = null;
    let proxy = null;
    let browser = null;
    let home = null;
    let runtimeDir = null;
    let workspace = null;
    let serverPort = 0;
    let uiPort = 0;
    let failure = null;
    let sidecarStarted = false;
    const timings = {};

    const result = {
        outboundViolations: 0,
        samples: 0,
        clickSteps: 0,
        pageErrors: 0,
        consoleErrors: 0,
        leftoverProcesses: 0,
        chatCitationClicked: false,
        /*
         * Die zwei Aussagen der Audit-Befunde 12 und 13, als eigene Zahlen im
         * Ergebnis. Sie stehen hier oben und nicht nur in `extras`, weil sie
         * jetzt zur Strecke gehoeren: ein Halt, dessen Ergebnis man nur im
         * Anhang findet, ist ein Halt, den beim naechsten Mal niemand ansieht.
         */
        atlasRowShortcutsShown: false,
        atlasRowOpenedByKey: false,
        tourDiagramOpened: false,
        overlapViolations: 0,
        clippingViolations: 0,
        scrolledRegions: 0,
        versionChipShown: '',
        versionChipExpected: EXPECTED_VERSION_CHIP,
        buildSuffixShown: '',
        screenshots: 0,
    };
    const budgets = {
        twinWarmP95Ms: Number.NaN,
        twinWarmSamples: 0,
        twinColdMs: Number.NaN,
        searchOverlayMs: Number.NaN,
        galaxyLoadMs: Number.NaN,
    };
    const extras = {
        blockedRequests: [],
        consoleErrors: [],
        pageErrors: [],
        sidecarRequests: [],
        failedResponses: [],
        steps: [],
        overlaps: [],
        clipped: [],
        scrolledRegionNames: [],
        warmSamples: [],
    };

    const sampler = startSocketSampler({ intervalMs: 1000 });

    try {
        if (!existsSync(BINARY)) {
            throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
        }
        for (const fixture of [FIXTURE, FIXTURE_LARGE]) {
            if (!existsSync(fixture)) {
                throw new Error(`Fixture fehlt: ${fixture}`);
            }
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
        extras.winner = { name: winner, choice };
        log(`Klasse-A-Sieger aus eval.json: ${winner} (llm/start.sh ${choice})`);

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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w6-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w6-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        workspace = await mkdtemp(join(tmpdir(), 'codeatlasweb-w6-fixture-'));
        log('isoliertes HOME:', home);
        log('Rendezvous:', runtimeDir);

        // ------------------------------------ 3. Die drei Projekte im Index
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        const indexedLarge = await indexRepository(
            BINARY, { home, repoPath: FIXTURE_LARGE, project: PROJECT_LARGE },
        );
        extras.indexedLarge = { nodes: indexedLarge.nodes, edges: indexedLarge.edges };

        const repo = join(workspace, 'atlas-sample');
        await cp(FIXTURE, repo, { recursive: true });
        const git = (...args) =>
            execFileAsync('git', args, {
                cwd: repo,
                env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_SYSTEM: '/dev/null' },
            });
        await git('init', '-q', '-b', 'main');
        await git('config', 'user.name', 'codeatlasweb-smoke');
        await git('config', 'user.email', 'smoke@localhost');
        await git('add', '-A');
        await git('commit', '-q', '-m', 'the fixture as it stands');
        const changedFile = join(repo, TARGET_FILE);
        const original = await readFile(changedFile, 'utf8');
        if (!original.includes(REWIRED_FROM)) {
            throw new Error(`die Fixture-Kopie enthaelt die erwartete Zeile nicht: ${REWIRED_FROM}`);
        }
        await writeFile(changedFile, original.replace(REWIRED_FROM, REWIRED_TO) + ADDED_FUNCTION, 'utf8');
        const indexedImpact = await indexRepository(
            BINARY, { home, repoPath: repo, project: PROJECT_IMPACT },
        );
        extras.indexedImpact = { nodes: indexedImpact.nodes, edges: indexedImpact.edges };
        log(`indiziert: ${indexed.nodes}/${indexedLarge.nodes}/${indexedImpact.nodes} Knoten`);

        // ------------------------------------------------- 4. Server, Proxy
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        timings.serverStartMs = started.durationMs;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        extras.serverPort = serverPort;
        extras.uiPort = uiPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        const busyBefore = await countListeners(SIDECAR_PORT);
        extras.sidecarListenersBefore = busyBefore;
        if (busyBefore > 0) {
            throw new Error(`auf ${SIDECAR_PORT} lauscht schon etwas (${busyBefore}); erst llm/stop.sh fahren`);
        }

        // ------------------------------------------------------- 5. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const origin = `http://127.0.0.1:${uiPort}`;
        const context = await browser.newContext({ viewport: { ...VIEWPORT } });
        await context.route('**/*', async (route) => {
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
        });

        const page = await context.newPage();
        page.on('console', (message) => {
            if (message.type() === 'error') {
                // Mit Herkunft: eine Konsolenzeile ohne Fundstelle ist ein
                // Befund, den niemand nachsehen kann.
                const where = message.location();
                extras.consoleErrors.push({
                    text: message.text(),
                    url: where?.url ?? '',
                    line: where?.lineNumber ?? 0,
                });
            }
        });
        // Jede Antwort, die kein Erfolg war, mit ihrer Adresse. Der Browser
        // schreibt fuer sie eine Konsolenzeile ohne Adresse; diese hier hat eine.
        page.on('response', (response) => {
            if (response.status() >= 400) {
                extras.failedResponses.push({ url: response.url(), status: response.status() });
            }
        });
        page.on('pageerror', (error) => extras.pageErrors.push(String(error)));
        // Die Bilder des letzten Laufs weg, bevor die dieses Laufs entstehen:
        // eine Zahl, die alte Bilder mitzaehlt, waere keine Zahl ueber diesen Lauf.
        await rm(WALK_DIR, { recursive: true, force: true });
        await mkdir(WALK_DIR, { recursive: true });

        // ------------------------------------------------------ Die Halte --

        /**
         * Ein benannter Halt: messen, scrollen, wieder messen.
         *
         * Der zweite Durchgang ist der Punkt: ein Panel, das oben ordentlich
         * aussieht und unten kollidiert, kollidiert.
         */
        const halt = async (name, options = {}) => {
            const before = await measureReadability(page);
            const scrolled = options.scroll === false ? [] : await scrollRegionsToEnd(page);
            let after = null;
            if (scrolled.length > 0) {
                await page.waitForTimeout(180);
                after = await measureReadability(page);
                await resetScroll(page);
                await page.waitForTimeout(120);
            }
            const overlaps = [...before.overlaps, ...(after?.overlaps ?? [])];
            const clipped = [...before.clipped, ...(after?.clipped ?? [])];
            for (const entry of overlaps) {
                extras.overlaps.push({ step: name, ...entry });
            }
            for (const entry of clipped) {
                extras.clipped.push({ step: name, ...entry });
            }
            for (const region of scrolled) {
                if (!extras.scrolledRegionNames.includes(region.name)) {
                    extras.scrolledRegionNames.push(region.name);
                }
            }
            result.clickSteps += 1;
            result.overlapViolations += overlaps.length;
            result.clippingViolations += clipped.length;
            extras.steps.push({
                step: result.clickSteps,
                name,
                atMs: Date.now() - totalStarted,
                candidates: before.candidates,
                layers: before.layers,
                scrolledRegions: scrolled.map((region) => region.name),
                overlaps: overlaps.length,
                clipped: clipped.length,
            });
            log(`Halt ${String(result.clickSteps).padStart(2, '0')} ${name}: `
                + `${before.candidates} Textelemente, ${scrolled.length} Bereiche gescrollt, `
                + `${overlaps.length} Ueberlagerungen, ${clipped.length} Beschneidungen`);
            if (options.shot !== undefined) {
                await page.screenshot({ path: join(WALK_DIR, options.shot), fullPage: false });
            }
        };

        /** Die Seite laden. Liefert den Zeitpunkt, an dem die Fahrt begann. */
        const openApp = async (project) => {
            const navigated = Date.now();
            await page.goto(`${origin}/?project=${project}`, { waitUntil: 'load' });
            await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
            await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
            return navigated;
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

        /** Tippen bis der Treffer dasteht. Liefert, wie lange das Fenster brauchte. */
        const typeQuery = async (name) => {
            const input = page.locator('[data-testid="atlas-command-input"]');
            await input.click();
            await input.fill('');
            const typed = Date.now();
            await input.pressSequentially(name, { delay: 30 });
            await page.waitForSelector(
                `[data-testid="atlas-search-row"][data-name="${name}"]`,
                { timeout: 30000 },
            );
            const shown = Date.now() - typed;
            await page.waitForTimeout(500);
            return shown;
        };

        /** Den Treffer waehlen und warten, bis der Twin ihn fertig zeigt. */
        const chooseHit = async (name, expectQualified) => {
            const clicked = Date.now();
            await page.click(`[data-testid="atlas-search-row"][data-name="${name}"]`);
            await page.waitForFunction(
                (expected) => new RegExp(`${expected}$`).test(globalThis.__atlasTwin?.qualifiedName ?? ''),
                expectQualified,
                { timeout: 60000 },
            );
            /*
             * Fertig heisst: das Panel steht auf `ready`, nicht "irgendein
             * Element ist da". Die Marke der Faktenliste haengt an der
             * gewaehlten Sprachstufe und waere darum eine Bedingung, die je
             * nach Leseprofil nie eintritt.
             */
            await page.waitForFunction(
                () => document.querySelector('[data-testid="atlas-twin"]')
                    ?.getAttribute('data-status') === 'ready',
                undefined,
                { timeout: 60000 },
            );
            return Date.now() - clicked;
        };

        const setDepth = async (depth) => {
            await page.locator('[data-testid="atlas-twin-depth"]').fill(String(depth));
            await page.waitForFunction(
                (expected) =>
                    document.querySelector('[data-testid="atlas-twin-depth"]')?.value === String(expected),
                depth,
                { timeout: 10000 },
            );
            await page.waitForTimeout(250);
        };

        const openWhyAndChoose = async (intent) => {
            await page.click('[data-menu="a-why"]');
            await page.waitForSelector('[data-testid="atlas-why"]', { timeout: 20000 });
            await page.click(`[data-testid="atlas-why-card"][data-intent="${intent}"]`);
        };

        /*
         * Eine Taste druecken, waehrend nirgends getippt wird.
         *
         * Seit W7a (Nutzerbefund 2026-08-29) tragen die Menuekuerzel
         * Alt/Option: ein blanker Buchstabe ist Text und faellt in die
         * Kommandozeile, statt im Hintergrund ein Panel zu oeffnen. Die
         * Zusicherung dieses Laufs bleibt dieselbe und wird sogar schaerfer:
         * geprueft wird weiterhin, dass der Eintrag ueber die TASTE erreichbar
         * ist, jetzt aber ueber die Taste, die er laut Menuezeile und Hilfe
         * wirklich hat. Bliebe sie wirkungslos, liefe der Lauf in die
         * Zeitueberschreitung.
         */
        const pressGlobally = async (key) => {
            await page.click('.atlas-brand');
            await page.keyboard.press(key);
        };

        /** Ein Menuekuerzel: derselbe Weg, mit dem Modifikator, den es braucht. */
        const pressMenuShortcut = async (letter) => {
            await pressGlobally(`Alt+${letter}`);
        };

        // ------------------------------------------- 6a. Baum und Legende
        /*
         * Die Ladezeit des Graph-Panels: vom Aufruf der Adresse bis das Panel
         * wirklich Knoten hat.
         *
         * Vom Aufruf an und nicht ab dem Moment, in dem der Baum steht: Baum
         * und Layout werden nebeneinander geholt, und wer ab dem Baum misst,
         * misst die Differenz zweier gleichzeitiger Antworten und nennt sie
         * Ladezeit. Die blosse Szene im DOM waere ebenfalls zu frueh; sie steht
         * schon da, bevor /api/layout geantwortet hat.
         */
        const galaxyStarted = await openApp(PROJECT);
        await page.waitForSelector('[data-testid="atlas-galaxy-scene"]', { timeout: 60000 });
        await page.waitForFunction(
            () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
            undefined,
            { timeout: 60000 },
        );
        budgets.galaxyLoadMs = Date.now() - galaxyStarted;
        const version = await page.evaluate(() => ({
            chip: document.querySelector('[data-testid="atlas-version"]')?.textContent?.trim() ?? '',
            suffix: document.querySelector('[data-testid="atlas-version-suffix"]')?.textContent?.trim() ?? '',
        }));
        result.versionChipShown = version.chip;
        result.buildSuffixShown = version.suffix;
        log(`Versions-Chip "${version.chip}" (erwartet ${EXPECTED_VERSION_CHIP}), `
            + `Bau-Zusatz "${version.suffix}"`);
        await halt('app-open', { shot: '01-app-open.png' });

        const rounds = await expandAll();
        extras.expandRounds = rounds;
        await halt('explorer-expanded', { shot: '02-explorer-expanded.png' });

        const legend = await page.evaluate(() => ({
            entries: [...document.querySelectorAll('[data-testid="atlas-tree-legend-entry"]')]
                .map((entry) => entry.textContent?.replace(/\s+/g, ' ').trim() ?? ''),
            dots: document.querySelectorAll('[data-testid="atlas-tree-dot"]').length,
            source: document.querySelector('[data-testid="atlas-tree-legend-source"]')
                ?.textContent?.trim() ?? '',
        }));
        extras.legend = legend;
        await halt('explorer-coverage-legend', { shot: '03-explorer-coverage-legend.png' });

        // ------------------------------------------------------ 6b. Reader
        await page.click(`[data-testid="atlas-tree-row"][data-path="${TARGET_FILE}"]`);
        await page.waitForFunction(
            (expected) => globalThis.__atlasReader?.document?.path === expected,
            TARGET_FILE,
            { timeout: 40000 },
        );
        await halt('reader-open-file', { shot: '04-reader-open-file.png' });

        // Die gekappte Datei steht im zweiten Projekt: an 42 Zeilen ist die
        // Kappungszeile nicht zu sehen, und ohne sie waere die Strecke um genau
        // die Aussage aermer, um die es hier geht.
        await openApp(PROJECT_LARGE);
        await expandAll();
        await page.click(`[data-testid="atlas-tree-row"][data-path="${LARGE_FILE}"]`);
        await page.waitForFunction(
            () => globalThis.__atlasReader?.status === 'ready',
            undefined,
            { timeout: 40000 },
        );
        const capped = await page.evaluate(() => ({
            truncated: globalThis.__atlasReader?.document?.truncated === true,
            note: document.querySelector('[data-testid="atlas-truncation"]')?.textContent?.trim() ?? '',
        }));
        extras.cappedFile = capped;
        log(`gekappte Datei: truncated=${capped.truncated}, Zeile "${capped.note.slice(0, 80)}"`);
        await halt('reader-capped-file', { shot: '05-reader-capped-file.png' });

        // ------------------------------------------- 6c. Twin, alle Tiefen
        await openApp(PROJECT);
        await expandAll();
        budgets.searchOverlayMs = await typeQuery(TARGET);
        await halt('search-overlay-open', { shot: '06-search-overlay.png' });
        budgets.twinColdMs = await chooseHit(TARGET, TARGET_QUALIFIED);
        log(`Twin kalt: ${budgets.twinColdMs} ms, Suchfenster ${budgets.searchOverlayMs} ms`);

        for (const depth of [0, 1, 2, 3]) {
            await setDepth(depth);
            await halt(`twin-depth-${depth}`, depth === 3 ? { shot: '07-twin-depth-3.png' } : {});
        }

        const evidenceButtons = await page.locator('[data-testid="codeatlas-evidence-btn"]').count();
        extras.evidenceButtons = evidenceButtons;
        if (evidenceButtons > 0) {
            await page.locator('[data-testid="codeatlas-evidence-btn"]').first().click();
            await page.waitForSelector('[data-testid="codeatlas-evidence-popover"]', { timeout: 15000 });
            extras.evidenceText = await page.evaluate(
                () => document.querySelector('[data-testid="codeatlas-evidence-popover"]')
                    ?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            );
        }
        await halt('twin-evidence', { shot: '08-twin-evidence.png' });
        if (evidenceButtons > 0) {
            await page.locator('[data-testid="codeatlas-evidence-btn"]').first().click();
            await page.waitForTimeout(200);
        }

        // ------------------------------------------- 6d. Flow und Stepper
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
                lifelines: document.querySelectorAll('[data-testid="atlas-flow-lifeline"]').length,
                title: document.querySelector('[data-testid="atlas-flow-overlay-title"]')
                    ?.textContent?.trim() ?? '',
            };
        });
        await halt('flow-overlay-open', { shot: '09-flow-overlay.png' });

        await page.click('[data-testid="atlas-flow-next"]');
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-flow"]')
                ?.getAttribute('data-active-step') === '0',
            undefined,
            { timeout: 15000 },
        );
        await page.waitForTimeout(400);
        await page.click('[data-testid="atlas-flow-next"]');
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-flow"]')
                ?.getAttribute('data-active-step') === '1',
            undefined,
            { timeout: 15000 },
        );
        await page.waitForTimeout(400);
        extras.flowStepper = await page.evaluate(() => ({
            position: document.querySelector('[data-testid="atlas-flow-position"]')?.textContent?.trim() ?? '',
            readerLine: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
            readerPath: globalThis.__atlasReader?.document?.path ?? '',
        }));
        await halt('flow-stepper-second-step', { shot: '10-flow-stepper.png' });

        await page.keyboard.press('Escape');
        await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { state: 'detached', timeout: 15000 })
            .catch(() => undefined);

        // -------------------------------------------- 6e. Pseudocode-Ansicht
        await page.click('[data-testid="atlas-pseudocode-toggle"]');
        await page.waitForSelector('[data-testid="atlas-pseudocode"]', { timeout: 20000 });
        extras.pseudocode = await page.evaluate(() => ({
            lines: document.querySelectorAll('[data-testid="atlas-pseudocode-line"]').length,
            imports: document.querySelectorAll('[data-testid="atlas-pseudocode-import"]').length,
            title: document.querySelector('[data-testid="atlas-pseudocode-title"]')?.textContent?.trim() ?? '',
        }));
        await halt('pseudocode-view', { shot: '11-pseudocode.png' });

        // ---------------------------------- 6f. Suche mit Enter, und Budgets
        await typeQuery('loadConfig');
        await page.locator('[data-testid="atlas-command-input"]').press('Enter');
        await page.waitForFunction(
            () => (globalThis.__atlasReader?.document?.path ?? '').length > 0,
            undefined,
            { timeout: 40000 },
        );
        extras.enterOpened = await page.evaluate(() => ({
            path: globalThis.__atlasReader?.document?.path ?? '',
            twin: globalThis.__atlasTwin?.qualifiedName ?? '',
        }));
        await halt('search-enter-opens', { shot: '12-search-enter.png' });

        /*
         * Zehn warme Wechsel, jeder auf ein anderes Symbol, nach einem Neuladen.
         *
         * Das Neuladen leert den Cache in src/twin/ir-cache.ts. Ohne es waere
         * eine der zehn Messungen ein Cache-Treffer nahe null, und ein p95 mit
         * einem geschenkten Wert darin ist kein p95 dieses Servers. Der Server
         * bleibt dabei warm: er laeuft seit Minuten und hat den Index gelesen,
         * und genau das heisst hier warm.
         */
        await openApp(PROJECT);
        for (const symbol of WARM_SYMBOLS) {
            await typeQuery(symbol);
            const ms = await chooseHit(symbol, symbol);
            extras.warmSamples.push({ symbol, ms });
        }
        const warm = extras.warmSamples.map((entry) => entry.ms);
        budgets.twinWarmSamples = warm.length;
        budgets.twinWarmP95Ms = percentile(warm, 95);
        log(`Twin warm: ${warm.length} Messungen, p95 ${budgets.twinWarmP95Ms} ms, `
            + `min ${Math.min(...warm)} ms, max ${Math.max(...warm)} ms`);
        await halt('twin-warm-budget', { shot: '13-twin-warm.png' });

        // ------------------------------------------------- 6g. Galaxy
        await page.waitForSelector('[data-testid="atlas-galaxy-scene"]', { timeout: 60000 });
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForSelector('[data-testid="atlas-galaxy-legend"]', { timeout: 15000 });
        await page.waitForTimeout(400);
        extras.galaxyLegend = await page.evaluate(() => ({
            entries: [...document.querySelectorAll('[data-testid="atlas-galaxy-legend-entry"]')]
                .map((entry) => entry.textContent?.replace(/\s+/g, ' ').trim() ?? ''),
            headline: document.querySelector('[data-testid="atlas-galaxy-headline"]')?.textContent?.trim() ?? '',
        }));
        await halt('galaxy-legend-open', { shot: '14-galaxy-legend.png' });
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(300);

        /*
         * ------------------------------- 6g-b. Die Buchstaben der Atlas-Zeile
         *
         * Befund 12 des unabhaengigen Audits: die vier Eintraege der Atlas-Zeile
         * trugen keinen Buchstaben, in einer Oberflaeche, deren ganzes Vorbild
         * die Tastatur ist. Dieser Halt liest, was auf ihnen STEHT, und oeffnet
         * die Frage nach dem Warum danach mit der Taste und nicht mit der Maus.
         *
         * Beides zusammen ist der Beweis: ein Etikett, das `[w]hy` sagt, waehrend
         * `w` nichts tut, waere schlimmer als eines ohne Klammer.
         */
        extras.atlasRow = await page.evaluate(() => {
            const items = [...document.querySelectorAll('[data-menu^="a-"]')];
            return items.map((item) => ({
                menu: item.getAttribute('data-menu') ?? '',
                label: item.textContent?.replace(/\s+/g, ' ').trim() ?? '',
                letter: /\[([a-z])\]/.exec(item.textContent ?? '')?.[1] ?? '',
                title: item.getAttribute('title') ?? '',
            }));
        });
        /*
         * Gezaehlt wird nicht mehr auf eine feste Zahl.
         *
         * Bis W8 waren es vier Aktionen, seit W8 sind es fuenf ("[r]eset
         * layout" kam dazu, weil ein verstellbares Layout einen Weg zurueck
         * braucht). Die Zusicherung war nie die Zahl, sondern: jede Aktion
         * traegt ihren eigenen Buchstaben, und keiner kommt doppelt vor. Genau
         * das steht jetzt da, und die Zeile darf wachsen, ohne dass dieser
         * Beweis dabei etwas anderes behauptet als vorher.
         */
        result.atlasRowShortcutsShown = extras.atlasRow.length >= 4
            && extras.atlasRow.every((entry) => entry.letter.length === 1)
            && new Set(extras.atlasRow.map((entry) => entry.letter)).size
                === extras.atlasRow.length;
        log(`Atlas-Zeile: ${extras.atlasRow.map((e) => e.label).join('  ')} `
            + `(alle mit eigenem Buchstaben: ${result.atlasRowShortcutsShown})`);

        await pressMenuShortcut('w');
        await page.waitForSelector('[data-testid="atlas-why"]', { timeout: 20000 });
        await page.waitForTimeout(400);
        result.atlasRowOpenedByKey = true;
        await halt('atlas-row-shortcuts', { shot: '14b-atlas-row-shortcuts.png' });
        // Wieder zu, ueber den ehrlichen Ausgang der Karte: die Frage soll die
        // Strecke danach nicht verstellen.
        await page.click('[data-testid="atlas-why-decline"]');
        await page.waitForSelector('[data-testid="atlas-why"]', { state: 'detached', timeout: 15000 })
            .catch(() => undefined);
        await page.waitForTimeout(300);

        // ------------------------------------------------------ 6h. Why
        await openWhyAndChoose('understand');
        await page.waitForSelector('[data-testid="atlas-tour"]', { timeout: 60000 });
        await page.waitForFunction(() => (globalThis.__atlasTour?.steps ?? 0) > 0, undefined, {
            timeout: 60000,
        });
        await page.waitForTimeout(600);
        await halt('why-answered-tour-start', { shot: '15-tour-step-1.png' });

        for (const step of [1, 2]) {
            await pressGlobally('Enter');
            await page.waitForFunction(
                (expected) => globalThis.__atlasTour?.index === expected,
                step,
                { timeout: 40000 },
            );
            await page.waitForTimeout(900);
            await halt(`tour-step-${step + 1}`, step === 2 ? { shot: '16-tour-step-3.png' } : {});
        }
        extras.tour = await page.evaluate(() => ({
            kind: globalThis.__atlasTour?.kind ?? '',
            steps: globalThis.__atlasTour?.steps ?? 0,
            index: globalThis.__atlasTour?.index ?? -1,
            title: globalThis.__atlasTour?.title ?? '',
        }));

        // ------------------------------------------------- 6i. Entry-Walk
        await openWhyAndChoose('entry');
        await page.waitForSelector('[data-testid="atlas-entry"]', { timeout: 40000 });
        await page.waitForSelector('[data-testid="atlas-entry-row"]', { timeout: 60000 });
        await halt('entry-dialog-open', { shot: '17-entry-dialog.png' });
        const entryInput = page.locator('[data-testid="atlas-entry-input"]');
        await entryInput.click();
        await entryInput.fill('');
        await entryInput.pressSequentially(WALK_ENTRY, { delay: 30 });
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
        await page.waitForTimeout(1500);
        extras.walk = await page.evaluate(() => ({
            mode: globalThis.__atlasGalaxy?.mode ?? '',
            nodes: globalThis.__atlasGalaxy?.hierarchy?.nodes ?? 0,
            depth: globalThis.__atlasGalaxy?.hierarchy?.depth ?? 0,
        }));
        await halt('entry-walk-hierarchy', { shot: '18-entry-walk-hierarchy.png' });

        /*
         * ------------------------- 6i-b. Die Aktion [d] auf der Schrittkarte
         *
         * Befund 13 des unabhaengigen Audits: PLAN Abschnitt 4 zaehlt
         * `[d] diagram` zu den Aktionen der unteren Karte, und der Erklaerer
         * hing stattdessen am flow()-Kopf des Twins. Gezeigt wird hier der Weg,
         * den ein Leser waehrend einer Fuehrung nimmt: Taste auf der Karte, Bild
         * zum Schritt, Escape zurueck.
         *
         * Am Vorwaerts-Walk und nicht an der Projekt-Fuehrung, und das ist kein
         * Zufall: die Schritte eines Vorwaerts-Walks sind Symbole, also hat
         * jeder von ihnen einen Flow. Ein Dateischritt haette keinen, und die
         * Karte sagt das dort ehrlich statt einen Knopf anzubieten, der nichts
         * zeichnen kann; genau diese Lesung steht als `tourDiagram` daneben.
         */
        extras.tourDiagram = await page.evaluate(() => {
            const button = document.querySelector('[data-testid="atlas-tour-diagram"]');
            return {
                present: button !== null,
                label: button?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
                available: button?.getAttribute('data-available') ?? '',
                disabled: button?.hasAttribute('disabled') ?? false,
                title: button?.getAttribute('title') ?? '',
            };
        });
        log(`Karten-Aktion: "${extras.tourDiagram.label}" `
            + `(bedienbar: ${extras.tourDiagram.available})`);
        await pressGlobally('d');
        await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { timeout: 30000 });
        await page.waitForFunction(
            () => Number(document.querySelector('[data-testid="atlas-flow"]')
                ?.getAttribute('data-arrows') ?? '0') > 0,
            undefined,
            { timeout: 60000 },
        );
        await page.waitForTimeout(400);
        extras.tourDiagramFlow = await page.evaluate(() => {
            const box = document.querySelector('[data-testid="atlas-flow"]');
            return {
                arrows: Number(box?.getAttribute('data-arrows') ?? '0'),
                steps: Number(box?.getAttribute('data-steps') ?? '0'),
                title: document.querySelector('[data-testid="atlas-flow-overlay-title"]')
                    ?.textContent?.trim() ?? '',
                /*
                 * Ob die Fuehrung weiterlaeuft, wird am Zustand gefragt und
                 * nicht am Bild.
                 *
                 * Bis W8 stand die Tour-Karte als eigene Flaeche im Baum, und
                 * ihre Anwesenheit war ein brauchbarer Ersatz fuer die Frage.
                 * Seit W8 teilen sich Flow und Fuehrung einen Platz mit
                 * Reitern, und es haengt immer nur der sichtbare im DOM: die
                 * Karte ist beim geoeffneten Diagramm also ausgehaengt,
                 * waehrend die Fuehrung sehr wohl weiterlaeuft. Die Naht sagt,
                 * was wirklich gilt, und ist damit die genauere Frage, nicht
                 * die nachsichtigere.
                 */
                tourStillOpen: (globalThis.__atlasTour?.steps ?? 0) > 0,
                tourStepIndex: globalThis.__atlasTour?.index ?? -1,
            };
        });
        result.tourDiagramOpened =
            extras.tourDiagram.present === true
            && extras.tourDiagram.available === 'true'
            && extras.tourDiagram.label.includes('[d]')
            && extras.tourDiagramFlow.arrows > 0
            && extras.tourDiagramFlow.tourStillOpen === true;
        log(`Bild zum Schritt: ${extras.tourDiagramFlow.arrows} Pfeile, `
            + `Fuehrung laeuft weiter: ${extras.tourDiagramFlow.tourStillOpen}`);
        await halt('tour-card-diagram', { shot: '18b-tour-diagram.png' });
        await page.keyboard.press('Escape');
        await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { state: 'detached', timeout: 15000 })
            .catch(() => undefined);
        await page.waitForTimeout(300);

        await pressGlobally('Enter');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 1, undefined, { timeout: 40000 });
        await page.waitForTimeout(900);
        await halt('entry-walk-step-2');

        // Der Umschalter zwischen den beiden Ansichten des Graph-Panels. Er ist
        // erst da, wenn es einen Walk gibt, den man zeigen koennte.
        await page.click('[data-testid="atlas-graph-mode-chip"][data-mode="galaxy"]');
        await page.waitForFunction(
            () => globalThis.__atlasGalaxy?.mode === 'galaxy',
            undefined,
            { timeout: 20000 },
        );
        await page.waitForTimeout(600);
        await halt('galaxy-mode-toggle', { shot: '19-graph-mode-galaxy.png' });
        await page.click('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]');
        await page.waitForFunction(
            () => globalThis.__atlasGalaxy?.mode === 'hierarchy',
            undefined,
            { timeout: 20000 },
        );
        await page.waitForTimeout(600);
        await halt('hierarchy-mode-toggle');
        await pressGlobally('q');
        await page.waitForTimeout(400);

        // ------------------------------------------------- 6j. BUG-Wizard
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
        extras.wizardBeforeIngest = await page.evaluate(() => {
            const panel = document.querySelector('[data-testid="atlas-bugwizard"]');
            return {
                events: Number(panel?.getAttribute('data-events') ?? '0'),
                staticPaths: Number(panel?.getAttribute('data-static-paths') ?? '0'),
                noTraces: document.querySelector('[data-testid="atlas-bugwizard-no-traces"]') !== null,
            };
        });
        await halt('bugwizard-without-traces', { shot: '20-bugwizard-no-traces.png' });

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

        await openApp(PROJECT);
        await typeQuery(TARGET);
        await chooseHit(TARGET, TARGET_QUALIFIED);
        // Mit der Taste statt mit der Maus: derselbe Eintrag, der andere Weg
        // hinein (Audit-Befund 12). Bliebe die Taste wirkungslos, liefe der
        // Lauf hier in die Zeitueberschreitung und nicht an ihr vorbei.
        await pressMenuShortcut('b');
        await page.waitForSelector('[data-testid="atlas-bugwizard"]', { timeout: 30000 });
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-bugwizard"]')
                ?.getAttribute('data-status') === 'ready',
            undefined,
            { timeout: 90000 },
        );
        await page.waitForTimeout(600);
        extras.wizardAfterIngest = await page.evaluate(() => {
            const panel = document.querySelector('[data-testid="atlas-bugwizard"]');
            return {
                events: Number(panel?.getAttribute('data-events') ?? '0'),
                staticPaths: Number(panel?.getAttribute('data-static-paths') ?? '0'),
                observedPaths: Number(panel?.getAttribute('data-observed-paths') ?? '0'),
                staticOnly: Number(panel?.getAttribute('data-static-only') ?? '0'),
                runtimeOnly: Number(panel?.getAttribute('data-runtime-only') ?? '0'),
            };
        });
        log(`Wizard mit Aufzeichnung: ${JSON.stringify(extras.wizardAfterIngest)}`);
        await halt('bugwizard-divergence', { shot: '21-bugwizard-divergence.png' });
        await page.click('[data-testid="atlas-bugwizard-close"]');
        await page.waitForTimeout(300);

        // --------------------------------------------------- 6k. Impact
        await openApp(PROJECT_IMPACT);
        await pressMenuShortcut('c');
        await page.waitForSelector('[data-testid="atlas-impact"]', { timeout: 30000 });
        await halt('impact-open');
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-impact"]')
                ?.getAttribute('data-status') === 'ready',
            undefined,
            { timeout: 120000 },
        );
        await page.waitForTimeout(500);
        extras.impact = await page.evaluate(() => {
            const panel = document.querySelector('[data-testid="atlas-impact"]');
            return {
                badge: panel?.getAttribute('data-badge') ?? '',
                direct: Number(panel?.getAttribute('data-direct') ?? '0'),
                downstream: Number(panel?.getAttribute('data-downstream') ?? '0'),
                endpoints: Number(panel?.getAttribute('data-endpoints') ?? '0'),
                tiles: document.querySelectorAll('[data-testid="atlas-impact-tile"]').length,
            };
        });
        log(`Blast-Radius: ${JSON.stringify(extras.impact)}`);
        await halt('impact-reading', { shot: '22-impact.png' });
        await page.click('[data-testid="atlas-impact-close"]');
        await page.waitForTimeout(300);

        // ------------------------------------------ 6l. Sidecar und Chat
        await openApp(PROJECT);
        await page.waitForFunction(
            () => (globalThis.__atlasLlm?.policyVerdict ?? '') !== '',
            undefined,
            { timeout: 60000 },
        );
        extras.llmOff = await page.evaluate(() => ({ ...globalThis.__atlasLlm }));
        await halt('sidecar-off', { shot: '23-sidecar-off.png' });

        const startStarted = Date.now();
        const startRun = await run('sh', [join(ROOT, 'llm', 'start.sh'), choice]);
        sidecarStarted = true;
        extras.startScript = { exit: startRun.code, out: (startRun.out + startRun.err).trim().split('\n').slice(-6) };
        if (startRun.code !== 0) {
            throw new Error(`llm/start.sh endete mit ${startRun.code}`);
        }
        /*
         * Erst warten, bis der Prozess wirklich antwortet, dann einschalten.
         *
         * llama-server antwortet auf /health mit 503, solange er das Modell
         * laedt. Das ist richtig so und die Anwendung geht richtig damit um (sie
         * bleibt auf `starting`), aber der Browser schreibt fuer jede Antwort
         * jenseits von 400 eine Zeile in die Konsole, und die Abnahme verlangt
         * eine leere Konsole. Der Lauf schaltet darum in derselben Reihenfolge
         * ein, die das Panel selbst vorschlaegt: erst llm/start.sh, dann den
         * Schalter. Was der Sidecar waehrend des Ladens sagt, steht trotzdem im
         * Ergebnis, unter sidecarHealth.
         */
        extras.sidecarHealth = [];
        const healthDeadline = Date.now() + READY_TIMEOUT_MS;
        for (;;) {
            let status = 0;
            try {
                const probe = await fetch(`${SIDECAR_ORIGIN}/health`);
                status = probe.status;
                await probe.arrayBuffer();
            } catch {
                status = 0;
            }
            extras.sidecarHealth.push({ atMs: Date.now() - startStarted, status });
            if (status === 200) {
                break;
            }
            if (Date.now() > healthDeadline) {
                throw new Error(`der Sidecar auf ${SIDECAR_PORT} war nicht bereit`);
            }
            await sleep(500);
        }
        await page.click('[data-menu="a-llm"]');
        await page.waitForFunction(
            () => globalThis.__atlasLlm?.state === 'ready',
            undefined,
            { timeout: READY_TIMEOUT_MS },
        );
        timings.sidecarReadyMs = Date.now() - startStarted;
        log(`Sidecar ready nach ${timings.sidecarReadyMs} ms`);
        extras.llmReady = await page.evaluate(() => ({ ...globalThis.__atlasLlm }));
        await halt('sidecar-ready', { shot: '24-sidecar-ready.png' });

        const askStarted = Date.now();
        const commandInput = page.locator('[data-testid="atlas-command-input"]');
        await commandInput.click();
        await commandInput.fill('');
        await commandInput.pressSequentially(QUESTION, { delay: 8 });
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
        await page.waitForTimeout(600);
        extras.answer = await page.evaluate(() => {
            const turn = (globalThis.__atlasChat?.turns ?? []).find(
                (entry) => entry.question.startsWith('Wer ruft') && entry.status === 'answered',
            );
            return turn === undefined ? null : {
                status: turn.status,
                klass: turn.klass,
                cards: turn.cards,
                citations: turn.citations,
                answerLength: turn.answer.length,
                tokensPerSecond: turn.tokensPerSecond,
            };
        });
        log(`Antwort nach ${timings.answerMs} ms, Zitate ${JSON.stringify(extras.answer?.citations ?? [])}`);
        await halt('chat-answered', { shot: '25-chat-answer.png' });

        // Ein [K]-Zitat anklicken: der Reader muss danach woanders stehen.
        const before = await page.evaluate(() => ({
            path: globalThis.__atlasReader?.document?.path ?? '',
            line: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
        }));
        const citation = page.locator('[data-testid="atlas-chat-citation"]').first();
        const citations = await citation.count();
        if (citations > 0) {
            extras.citedCard = await citation.getAttribute('data-card');
            await citation.click();
            await page.waitForTimeout(2500);
            const after = await page.evaluate(() => ({
                path: globalThis.__atlasReader?.document?.path ?? '',
                line: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
            }));
            extras.citationClick = { before, after };
            result.chatCitationClicked =
                after.path.length > 0 && (after.path !== before.path || after.line !== before.line);
        }
        log(`Zitat-Klick: ${result.chatCitationClicked}`);
        await halt('chat-citation-followed', { shot: '26-chat-citation.png' });

        await page.click('[data-testid="atlas-chat-cards-toggle"]');
        await page.waitForTimeout(400);
        extras.cardsShown = await page.locator('[data-testid="atlas-chat-card"]').count();
        await halt('chat-cards-open', { shot: '27-chat-cards.png' });

        // Und wieder aus. "Aus" ist eine Lage und keine fehlende Funktion.
        await pressMenuShortcut('l');
        await page.waitForFunction(
            () => globalThis.__atlasLlm?.state === 'off',
            undefined,
            { timeout: 30000 },
        );
        extras.llmOffAgain = await page.evaluate(() => ({ ...globalThis.__atlasLlm }));
        await halt('sidecar-off-again', { shot: '28-sidecar-off-again.png' });

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w6] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w6] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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

    /*
     * Gezaehlt wird mit Geduld, und die Geduld hat eine Grenze.
     *
     * llm/stop.sh wartet, bis der Prozess weg ist (`kill -0` schlaegt fehl),
     * und meldet dann "beendet". Das Betriebssystem gibt den Port aber erst
     * danach frei, und zweimal hintereinander lag genau dieser Moment auf der
     * einen Messung: Prozess tot, Port noch belegt, Lauf rot. Ein Beweislauf,
     * der gelegentlich ohne Grund rot wird, ist schlimmer als keiner, weil man
     * sich an sein Rot gewoehnt und das echte uebersieht.
     *
     * Es wird darum bis zu fuenf Sekunden lang nachgesehen, ob der Port frei
     * wird. Das ist nicht nachsichtiger, sondern genauer: ein Prozess, der
     * wirklich weiterlaeuft, laeuft auch nach fuenf Sekunden noch, und dann
     * steht seine Zahl hier. Wie lange es wirklich gedauert hat, steht im
     * Artefakt, damit ein schleichend langsamer werdender Abbau auffaellt,
     * statt sich hinter der Wartezeit zu verstecken.
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

    const netz = await sampler.stop();
    result.samples = netz.samples;
    result.outboundViolations = netz.outboundViolations;
    extras.net = netz;

    result.consoleErrors = extras.consoleErrors.length;
    result.pageErrors = extras.pageErrors.length;
    result.scrolledRegions = extras.scrolledRegionNames.length;
    result.screenshots = existsSync(WALK_DIR)
        ? readdirSync(WALK_DIR).filter((name) => name.endsWith('.png')).length
        : 0;

    timings.totalMs = Date.now() - totalStarted;

    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        AIRGAP_JSON,
        JSON.stringify({
            ...result,
            projects: { main: PROJECT, large: PROJECT_LARGE, impact: PROJECT_IMPACT },
            fixture: 'fixtures/atlas-sample und fixtures/atlas-sample-large (nur gelesen), '
                + 'plus eine Kopie mit eigenem git-Repository fuer die Aenderungsansicht',
            sidecarPort: SIDECAR_PORT,
            question: QUESTION,
            readability: {
                exclusions: READABILITY_EXCLUSIONS,
                deliberateOverlays: DELIBERATE_OVERLAYS,
                scrolledRegionNames: extras.scrolledRegionNames,
                overlaps: extras.overlaps,
                clipped: extras.clipped,
            },
            steps: extras.steps,
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    await writeFile(
        BUDGETS_JSON,
        JSON.stringify({
            ...budgets,
            warmSamples: extras.warmSamples,
            method: {
                warm:
                    'Zehn verschiedene Symbole, nacheinander ueber die Bedeutungssuche geoeffnet. '
                    + 'Gemessen vom Klick auf den Treffer bis der Twin das neue Subjekt fertig zeigt '
                    + '(Aufloesung und IR-Bau ueber /rpc, dann das Bild). Das Tippen und die '
                    + 'Entprellung davor stehen als searchOverlayMs getrennt. Zehn VERSCHIEDENE '
                    + 'Symbole, weil dasselbe zweimal den Cache in src/twin/ir-cache.ts messen '
                    + 'wuerde und nicht den Server.',
                cold: 'Der erste Symbolwechsel nach dem Laden der Seite, gleich gemessen.',
                searchOverlay:
                    'Vom ersten Tastendruck bis die Trefferzeile mit diesem Namen im Fenster steht, '
                    + 'einschliesslich der Entprellung von 200 ms.',
                galaxy:
                    'Vom Aufruf der Adresse bis das Graph-Panel Knoten meldet (Szene im DOM UND '
                    + '__atlasGalaxy.nodes > 0). Enthaelt den Seitenaufbau, weil ein Leser ihn '
                    + 'ebenfalls abwartet.',
            },
            generatedAt: new Date().toISOString(),
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', AIRGAP_JSON, 'und', BUDGETS_JSON);

    const ok =
        failure === null
        && result.outboundViolations === 0
        && result.samples >= 60
        && result.clickSteps >= 27
        && result.pageErrors === 0
        && result.consoleErrors === 0
        && result.leftoverProcesses === 0
        && result.chatCitationClicked === true
        && result.atlasRowShortcutsShown === true
        && result.atlasRowOpenedByKey === true
        && result.tourDiagramOpened === true
        && result.overlapViolations === 0
        && result.clippingViolations === 0
        && result.scrolledRegions >= 6
        && result.screenshots >= 10
        && result.versionChipShown === EXPECTED_VERSION_CHIP
        && budgets.twinWarmSamples >= 10
        && budgets.twinWarmP95Ms <= 800
        && extras.blockedRequests.length === 0;

    log(`Halte ${result.clickSteps}, Bilder ${result.screenshots}, Stichproben ${result.samples}, `
        + `Ueberlagerungen ${result.overlapViolations}, Beschneidungen ${result.clippingViolations}, `
        + `gescrollte Bereiche ${result.scrolledRegions} (${extras.scrolledRegionNames.join(', ')})`);

    if (!ok) {
        console.error('[smoke-w6] W6a-Smoke NICHT gruen.');
        for (const entry of extras.overlaps.slice(0, 10)) {
            console.error(`  Ueberlagerung @${entry.step} [${entry.layer}] `
                + `"${entry.a.text}" x "${entry.b.text}" (${entry.overlapX}x${entry.overlapY}px)`);
        }
        for (const entry of extras.clipped.slice(0, 10)) {
            console.error(`  Beschneidung @${entry.step} ${entry.element.path} "${entry.element.text}" `
                + `ragt ${entry.overflowPx}px aus ${entry.container.path} (${entry.axis})`);
        }
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
            console.error('[smoke-w6] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        if (workspace) {
            console.error('[smoke-w6] Fixture-Kopie bleibt liegen:', workspace);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir, workspace]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log(`W6a-Smoke gruen nach ${timings.totalMs} ms.`);
}

main().catch((err) => {
    console.error('[smoke-w6] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
