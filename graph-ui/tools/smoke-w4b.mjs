#!/usr/bin/env node
/*
 * W4b-Smoke: der BUG-Assistent und der Blast-Radius an einem echten Server.
 *
 * Was hier bewiesen wird, koennen die Unit-Tests nicht beweisen. Sie zeigen an
 * erfundenen Zeilen, dass eine Divergenz die Differenz zweier Mengen ist und
 * dass ein Ref mit zwei Punkten abgelehnt wird. Sie sagen nichts darueber, ob
 * dieser Server eine eingespielte Aufzeichnung wieder herausgibt, ob der Satz
 * ueber die fehlende Aufzeichnung dort steht, wo ein Leser ihn saehe, ob ein
 * Klick auf einen Hop den Reader bewegt, ob eine echte Aenderung an einer echten
 * Datei eine Kachelzeile und ein Wort mit Begruendung ergibt, und ob bei einem
 * kaputten Ref wirklich nichts auf den Draht geht.
 *
 * Ablauf:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis, KOPIE der Fixture
 *   3. git init + Basis-Commit in der Kopie, dann indizieren
 *   4. C-Server auf einem freien Port >= 4270, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *   6a. Assistent ohne Aufzeichnung: der ehrliche Satz und die Anleitung
 *   6b. ingest_traces ueber die CLI, danach Assistent mit Divergenz
 *   6c. eine exportierte Funktion dazu, Reindex, dann der Blast-Radius
 *   7. abraeumen, Restprozesse zaehlen, zwei JSON schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w4b).
 *
 * ## Drei Entscheidungen, die man sonst raten muesste
 *
 * **Die Fixture wird kopiert, nie angefasst.** Der Impact-Beweis braucht eine
 * echte Aenderung an einer echten Datei; fixtures/atlas-sample ist die Grundlage
 * von vier anderen Beweislaeufen und bleibt byte-identisch. Alles passiert in
 * einem mkdtemp-Verzeichnis, das am Ende wieder verschwindet.
 *
 * **Die Kopie bekommt ein eigenes git-Repository.** `detect_changes` vergleicht
 * gegen git-Refs, also braucht die Kopie einen Basis-Commit, bevor irgendetwas
 * geaendert wird. Es wird ausschliesslich in der Kopie initialisiert und
 * committet; kein fremdes Repository wird beruehrt.
 *
 * **CBM_RUNTIME_DIR wird gesetzt.** Der Daemon des Servers und jede CLI
 * verabreden sich in einem Rendezvous-Verzeichnis, das per Konto und nicht per
 * HOME gilt: laeuft irgendwo sonst auf der Maschine eine CBM-Instanz mit einem
 * anderen Cache-Verzeichnis, lehnt jede CLI dieses Laufs mit "the active account
 * daemon uses a different cache directory" ab, und der Lauf waere nicht rot,
 * sondern kaputt. Der Parameter (cbm/src/daemon/bootstrap.c) verlegt das
 * Rendezvous in ein privates Verzeichnis dieses Laufs. Es liegt unter
 * /private/tmp und nicht unter os.tmpdir(): die Pruefung des Verzeichnisses
 * lehnt die pro-Benutzer-Ordner von macOS ab, /private/tmp ist die Vorgabe des
 * Werkzeugs selbst.
 */

import { execFile, spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
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

const execFileAsync = promisify(execFile);

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w4b';
const OUT_DIR = join(ROOT, 'verification', 'w4');
const BUG_JSON = join(OUT_DIR, 'bugwizard.json');
const IMPACT_JSON = join(OUT_DIR, 'impact.json');
const MIN_PORT = 4270;

/** Das Symbol, ueber das der Assistent Auskunft gibt. */
const TARGET = 'createUser';
const TARGET_FILE = 'src/services/userService.ts';

/** Der Hop, dessen Klick den Reader in eine andere Datei bewegen muss. */
const HOP = 'main';
const HOP_FILE = 'src/server.ts';

/** Der Lauf, unter dem die Aufzeichnung eingespielt wird. */
const RUN_LABEL = 'smoke-run';
const RUN_COUNT = 3;

/** Ein Ref, den git nie akzeptieren wuerde. Zwei Punkte hintereinander. */
const BROKEN_REF = 'main..dev';

/**
 * Die Aenderung, die der Kopie zugefuegt wird: eine neue exportierte Funktion,
 * und eine bestehende, die sie benutzt.
 *
 * Beide Haelften sind noetig, und der Grund ist eine gemessene Eigenschaft
 * dieses Servers. `detect_changes` schraenkt seine Ausgangspunkte auf die
 * geaenderten Hunks ein und meldet als `impacted` die transitiven AUFRUFER
 * dieser Ausgangspunkte. Eine nur angehaengte Funktion, die niemand ruft, ist
 * ein Ausgangspunkt mit leerem Blast-Radius; das ist die richtige Antwort und
 * beweist nichts. Erst die zweite Haelfte, ein Aufruf der neuen Funktion aus
 * createUser heraus, macht createUser zum Ausgangspunkt, und dessen Aufrufer
 * sind die Route-Datei und der Server. So hat die Aenderung einen Downstream
 * UND einen Endpunkt, und beide sind gemessen statt behauptet.
 */
const ADDED_FUNCTION = `
export function countUsers(): number {
    return listUsers().length;
}
`;

/** Die Zeile in createUser, die auf die neue Funktion umgestellt wird. */
const REWIRED_FROM = 'const entity = new UserEntity(`user-${listUsers().length + 1}`, parsed.email, parsed.name);';
const REWIRED_TO = 'const entity = new UserEntity(`user-${countUsers() + 1}`, parsed.email, parsed.name);';

/** Chromium ohne Aussenwelt. Wortgleich mit smoke-w4a. */
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
];

const log = (...parts) => console.log('[smoke-w4b]', ...parts);
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
        child.stdout.on('data', (d) => {
            out += d.toString();
        });
        child.stderr.on('data', (d) => {
            err += d.toString();
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

/** Ein Werkzeug ueber die CLI, mit dem HOME dieses Laufs. */
async function cli(tool, payload, home) {
    const result = await run(BINARY, ['cli', tool], { env: { HOME: home }, stdin: `${JSON.stringify(payload)}\n` });
    const line = result.out.split('\n').map((entry) => entry.trim()).filter(Boolean).pop();
    if (result.code !== 0 || line === undefined) {
        throw new Error(`cli ${tool} endete mit ${result.code}: ${result.err.trim().slice(-400)}`);
    }
    try {
        return JSON.parse(line);
    } catch {
        throw new Error(`cli ${tool} lieferte kein JSON: ${line.slice(0, 400)}`);
    }
}

/** Der qualifizierte Name, unter dem der Index ein Symbol dieses Projekts fuehrt. */
const qn = (suffix) => `${PROJECT}.${suffix}`;

/** Zum Symbol navigieren, ueber dieselbe Suche, die die Kommandozeile fuehrt. */
async function openTarget(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(TARGET, { delay: 40 });
    await page.waitForSelector(`[data-testid="atlas-search-row"][data-name="${TARGET}"]`, { timeout: 30000 });
    // Ueber die Entprellung hinaus, damit die Liste die eines fertigen Wortes ist.
    await page.waitForTimeout(600);
    await page.click(`[data-testid="atlas-search-row"][data-name="${TARGET}"]`);
    await page.waitForFunction(
        (expected) => globalThis.__atlasReader?.document?.path === expected,
        TARGET_FILE,
        { timeout: 40000 },
    );
    await page.waitForFunction(
        () => /userService\.createUser$/.test(globalThis.__atlasTwin?.qualifiedName ?? ''),
        undefined,
        { timeout: 40000 },
    );
}

/** Den Assistenten oeffnen und auf seine fertige Lesung warten. */
async function openWizard(page) {
    await page.click('[data-menu="a-bug"]');
    await page.waitForSelector('[data-testid="atlas-bugwizard"]', { timeout: 20000 });
    await page.waitForFunction(
        () => document.querySelector('[data-testid="atlas-bugwizard"]')?.getAttribute('data-status') === 'ready',
        undefined,
        { timeout: 60000 },
    );
}

/** Alles, was der Assistent gerade zeigt, in einem Zug abgelesen. */
const wizardSeam = (page) =>
    page.evaluate(() => {
        const panel = document.querySelector('[data-testid="atlas-bugwizard"]');
        const text = (id) =>
            document.querySelector(`[data-testid="${id}"]`)?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        const list = (id) => [...document.querySelectorAll(`[data-testid="${id}"]`)];
        return {
            status: panel?.getAttribute('data-status') ?? '',
            target: panel?.getAttribute('data-target') ?? '',
            events: Number(panel?.getAttribute('data-events') ?? '0'),
            staticPaths: Number(panel?.getAttribute('data-static-paths') ?? '0'),
            observedPaths: Number(panel?.getAttribute('data-observed-paths') ?? '0'),
            staticOnly: Number(panel?.getAttribute('data-static-only') ?? '0'),
            runtimeOnly: Number(panel?.getAttribute('data-runtime-only') ?? '0'),
            staticChains: list('atlas-bugwizard-static-chain').map((node) =>
                [...node.querySelectorAll('[data-testid="atlas-bugwizard-hop"], [data-testid="atlas-bugwizard-observed-hop"]')]
                    .map((hop) => hop.getAttribute('data-name')),
            ),
            observedChains: list('atlas-bugwizard-observed-chain').map((node) =>
                [...node.querySelectorAll('[data-testid="atlas-bugwizard-hop"], [data-testid="atlas-bugwizard-observed-hop"]')]
                    .map((hop) => hop.getAttribute('data-name')),
            ),
            observedHops: list('atlas-bugwizard-observed-hop').map((node) => ({
                name: node.getAttribute('data-name'),
                count: Number(node.getAttribute('data-count') ?? '0'),
                label: node.getAttribute('data-label') ?? '',
                lastSeen: node.getAttribute('data-last-seen') ?? '',
            })),
            edges: list('atlas-bugwizard-edge').map((node) => ({
                kind: node.getAttribute('data-kind'),
                from: node.getAttribute('data-from'),
                to: node.getAttribute('data-to'),
                verdict:
                    node.querySelector('[data-testid="atlas-bugwizard-edge-verdict"]')?.textContent?.trim() ?? '',
            })),
            noTraces: {
                present: document.querySelector('[data-testid="atlas-bugwizard-no-traces"]') !== null,
                message: text('atlas-bugwizard-no-traces-message'),
                how: text('atlas-bugwizard-no-traces-how'),
                command: text('atlas-bugwizard-no-traces-command'),
                format: text('atlas-bugwizard-no-traces-format'),
                where: text('atlas-bugwizard-no-traces-where'),
            },
            noDivergence: text('atlas-bugwizard-no-divergence'),
            truncated: text('atlas-bugwizard-truncated'),
        };
    });

/** Alles, was die Aenderungsansicht gerade zeigt. */
const impactSeam = (page) =>
    page.evaluate(() => {
        const panel = document.querySelector('[data-testid="atlas-impact"]');
        const list = (id) => [...document.querySelectorAll(`[data-testid="${id}"]`)];
        return {
            status: panel?.getAttribute('data-status') ?? '',
            mode: panel?.getAttribute('data-mode') ?? '',
            badge: panel?.getAttribute('data-badge') ?? '',
            tiles: list('atlas-impact-tile').map((node) => ({
                label: node.getAttribute('data-tile'),
                value: node.querySelector('.atlas-impact-tile-value')?.textContent?.trim() ?? '',
            })),
            rules: list('atlas-impact-badge-rule').map((node) =>
                (node.textContent ?? '').replace(/\s+/g, ' ').trim()),
            narrative:
                document.querySelector('.atlas-impact-narrative-text')?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            evidence: list('atlas-impact-evidence').map((node) => ({
                source: node.getAttribute('data-source'),
                claim:
                    node.querySelector('.atlas-impact-evidence-claim')?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
                value:
                    node.querySelector('.atlas-impact-evidence-value')?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            })),
            direct: list('atlas-impact-direct-row').map((node) =>
                node.querySelector('[data-testid="atlas-impact-open"]')?.getAttribute('data-name') ?? ''),
            downstream: list('atlas-impact-downstream-row').map((node) =>
                node.querySelector('[data-testid="atlas-impact-open"]')?.getAttribute('data-name') ?? ''),
            endpoints: list('atlas-impact-endpoint').map((node) => ({
                label: node.getAttribute('data-endpoint'),
                via: node.getAttribute('data-via'),
            })),
            untested: list('atlas-impact-untested').length,
            routeNote:
                document.querySelector('[data-testid="atlas-impact-route-note"]')?.textContent?.trim() ?? '',
            refError:
                document.querySelector('[data-testid="atlas-impact-ref-error"]')?.textContent?.trim() ?? '',
        };
    });

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
    const timings = {};

    const bug = {
        staticChains: 0,
        observedPathShown: false,
        observedCount: 0,
        observedLabel: '',
        staticOnlyCount: 0,
        runtimeOnlyCount: 0,
        noTracesHonest: false,
        hopClickNavigates: false,
        port: 0,
        leftoverProcesses: 0,
    };
    const impact = {
        directCount: 0,
        downstreamCount: 0,
        endpointNamed: '',
        badge: '',
        badgeRulesExplained: '',
        narrativeEvidenceOk: false,
        invalidRefNoEngineCall: false,
        tilesShown: 0,
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
            throw new Error(`npm run build endete mit ${build.code}: ${build.err.trim().slice(-600)}`);
        }
        if (!existsSync(join(DIST, 'index.html'))) {
            throw new Error('dist/index.html fehlt nach dem Build');
        }

        // -------------------------------------- 2. HOME, Rendezvous, Kopie
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w4b-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w4b-run-');
        // Von hier an erbt jeder Kindprozess das eigene Rendezvous: die CLI
        // ueber `run`, der Server ueber startServer. Siehe Kopf dieser Datei.
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        workspace = await mkdtemp(join(tmpdir(), 'codeatlasweb-w4b-fixture-'));
        const repo = join(workspace, 'atlas-sample');
        await cp(FIXTURE, repo, { recursive: true });
        log('isoliertes HOME:', home);
        log('Rendezvous:', runtimeDir);
        log('Fixture-Kopie:', repo);

        // -------------------------------------------- 3. git und Basis-Commit
        const git = (...args) =>
            execFileAsync('git', args, { cwd: repo, env: { ...process.env, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_SYSTEM: '/dev/null' } });
        await git('init', '-q', '-b', 'main');
        await git('config', 'user.name', 'codeatlasweb-smoke');
        await git('config', 'user.email', 'smoke@localhost');
        await git('add', '-A');
        await git('commit', '-q', '-m', 'the fixture as it stands');
        const baseCommit = (await git('rev-parse', 'HEAD')).stdout.trim();
        extras.baseCommit = baseCommit;
        log('Basis-Commit der Kopie:', baseCommit.slice(0, 12));

        // -------------------------------------------------------- 4. Index
        const indexed = await indexRepository(BINARY, { home, repoPath: repo, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // -------------------------------------------------------- 5. Server
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        timings.serverStartMs = started.durationMs;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        bug.port = uiPort;
        impact.port = uiPort;
        extras.serverPort = serverPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        // ------------------------------------------------------- 6. Browser
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

        // ------------------------------- 6a. Der Assistent ohne Aufzeichnung
        await openTarget(page, origin);
        await openWizard(page);
        const before = await wizardSeam(page);
        extras.beforeIngest = before;
        bug.noTracesHonest =
            before.events === 0
            && before.noTraces.present
            && before.noTraces.message.length > 20
            && before.noTraces.how.length > 20
            && before.noTraces.command.includes('codebase-memory-mcp cli ingest_traces')
            && before.noTraces.command.includes(PROJECT)
            && before.noTraces.format.includes('caller')
            && before.noTraces.where.length > 20
            // Der erwartete Pfad steht weiterhin da: eine halbe Antwort ist eine.
            && before.staticPaths >= 1;
        log(`ohne Aufzeichnung: ${before.staticPaths} Ketten, ehrlich: ${bug.noTracesHonest}`);
        await page.screenshot({ path: join(OUT_DIR, 'bugwizard-no-traces.png'), fullPage: true });
        log('bugwizard-no-traces.png geschrieben');

        // ------------------------------------------- 6b. Aufzeichnung rein
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
                    // Ein Paar, das der Index nicht als Aufruf fuehrt. Es wird
                    // gespeichert (beide Enden sind indiziert) und ist danach
                    // ueber /api/trace und /api/flow nicht wieder lesbar, weil
                    // dort nur Kanten annotiert werden, die der Index auch
                    // kennt. Genau das steht als Befund in extras.
                    {
                        caller: qn('src.services.userService.listUsers'),
                        callee: qn('src.util.validate.validateUser'),
                    },
                ],
            },
            home,
        );
        extras.ingest = ingest;
        log(`ingest_traces: ${ingest.pairs_stored} Paare, ${ingest.paths_stored} Pfade, `
            + `${ingest.pairs_unmatched} unaufgeloest`);

        await openTarget(page, origin);
        await openWizard(page);
        const after = await wizardSeam(page);
        extras.afterIngest = after;
        bug.staticChains = after.staticPaths;
        bug.observedPathShown = after.observedPaths >= 1 && after.observedHops.length >= 1;
        bug.observedCount = after.observedHops[0]?.count ?? 0;
        bug.observedLabel = after.observedHops[0]?.label ?? '';
        bug.staticOnlyCount = after.staticOnly;
        bug.runtimeOnlyCount = after.runtimeOnly;
        log(`mit Aufzeichnung: ${after.staticPaths} Ketten, ${after.observedPaths} beobachtete Pfade, `
            + `${after.staticOnly} erwartet-nie-beobachtet, ${after.runtimeOnly} beobachtet-neben-den-Ketten`);
        await page.screenshot({ path: join(OUT_DIR, 'bugwizard-divergence.png'), fullPage: true });
        log('bugwizard-divergence.png geschrieben');

        // Ein Hop-Klick bewegt den Reader. Gemessen an einem Hop, der in einer
        // anderen Datei steht als der, die gerade offen ist.
        const hop = page.locator(`[data-testid="atlas-bugwizard-hop"][data-name="${HOP}"] button`).first();
        extras.hopClicked = { name: HOP, expected: HOP_FILE, count: await hop.count() };
        if (extras.hopClicked.count > 0) {
            await hop.click();
            await page
                .waitForFunction(
                    (expected) => globalThis.__atlasReader?.document?.path === expected,
                    HOP_FILE,
                    { timeout: 30000 },
                )
                .catch(() => undefined);
            const reader = await page.evaluate(() => globalThis.__atlasReader?.document?.path ?? '');
            extras.hopClicked.readerAfter = reader;
            bug.hopClickNavigates = reader === HOP_FILE;
        }
        log(`Hop-Klick auf ${HOP}: ${bug.hopClickNavigates}`);

        // ------------------------------ 6c. Eine echte Aenderung, dann Reindex
        const changedFile = join(repo, TARGET_FILE);
        const original = await readFile(changedFile, 'utf8');
        if (!original.includes(REWIRED_FROM)) {
            throw new Error(`die Fixture-Kopie enthaelt die erwartete Zeile nicht: ${REWIRED_FROM}`);
        }
        await writeFile(changedFile, original.replace(REWIRED_FROM, REWIRED_TO) + ADDED_FUNCTION, 'utf8');
        const status = (await git('status', '--porcelain')).stdout.trim();
        extras.gitStatus = status.split('\n').filter(Boolean);
        const reindexed = await indexRepository(BINARY, { home, repoPath: repo, project: PROJECT });
        extras.reindexed = { nodes: reindexed.nodes, edges: reindexed.edges };
        log(`Aenderung geschrieben, neu indiziert: ${reindexed.nodes} Knoten`);

        await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
        await page.click('[data-menu="a-impact"]');
        await page.waitForSelector('[data-testid="atlas-impact"]', { timeout: 20000 });
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-impact"]')?.getAttribute('data-status') === 'ready',
            undefined,
            { timeout: 90000 },
        );
        const reading = await impactSeam(page);
        extras.impact = reading;
        impact.directCount = reading.direct.length;
        impact.downstreamCount = reading.downstream.length;
        impact.endpointNamed = reading.endpoints[0]?.label ?? '';
        impact.badge = reading.badge;
        impact.badgeRulesExplained = reading.rules.join(' ');
        impact.tilesShown = reading.tiles.length;
        impact.narrativeEvidenceOk =
            reading.evidence.length >= 4
            && reading.evidence.every((entry) => entry.claim.length > 0 && entry.value.length > 0)
            && reading.evidence.every((entry) => reading.narrative.includes(entry.claim));
        log(`Blast-Radius: ${impact.directCount} direkt, ${impact.downstreamCount} Aufrufer, `
            + `Endpunkt "${impact.endpointNamed}", Wort ${impact.badge}, ${impact.tilesShown} Kacheln`);
        await page.screenshot({ path: join(OUT_DIR, 'impact.png'), fullPage: true });
        log('impact.png geschrieben');

        // Ein kaputter Ref darf nichts auf den Draht schicken. Gezaehlt wird am
        // Proxy, also an dem, was wirklich gesendet wurde.
        const callsBefore = proxy.log.rpcTools['detect_changes'] ?? 0;
        await page.click('[data-testid="atlas-impact-mode-since-ref"]');
        await page.waitForSelector('[data-testid="atlas-impact-ref-input"]', { timeout: 10000 });
        await page.fill('[data-testid="atlas-impact-ref-input"]', BROKEN_REF);
        await page.click('[data-testid="atlas-impact-ref-go"]');
        await page.waitForSelector('[data-testid="atlas-impact-ref-error"]', { timeout: 10000 });
        // Grosszuegig warten: haette der Klick doch eine Anfrage ausgeloest,
        // waere sie bis hierher laengst durch den Proxy gegangen.
        await page.waitForTimeout(1500);
        const callsAfter = proxy.log.rpcTools['detect_changes'] ?? 0;
        const refError = await page.evaluate(
            () => document.querySelector('[data-testid="atlas-impact-ref-error"]')?.textContent?.trim() ?? '',
        );
        extras.refCheck = { callsBefore, callsAfter, refError, ref: BROKEN_REF };
        impact.invalidRefNoEngineCall =
            // Beide Richtungen: der Zaehler steht still, UND er hat sich vorher
            // bewegt. Ohne die zweite Haelfte waere "unveraendert" auch dann
            // wahr, wenn dieser Lauf nie eine Aenderungsmenge gelesen haette.
            callsBefore > 0
            && callsAfter === callsBefore
            && refError.includes('Nothing was asked of the analysis backend');
        log(`kaputter Ref: detect_changes ${callsBefore} -> ${callsAfter}, Satz: ${refError.slice(0, 80)}`);

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w4b] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w4b] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
    bug.leftoverProcesses = leftovers.reduce((sum, value) => sum + value, 0);
    impact.leftoverProcesses = bug.leftoverProcesses;
    log('leftoverProcesses:', bug.leftoverProcesses);

    timings.totalMs = Date.now() - totalStarted;
    const shared = {
        project: PROJECT,
        fixture: 'fixtures/atlas-sample (Kopie in einem mkdtemp-Verzeichnis)',
        target: TARGET,
        timings,
        generatedAt: new Date().toISOString(),
        error: failure ? failure.message : null,
    };

    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(BUG_JSON, JSON.stringify({ ...bug, ...shared, extras }, null, 2) + '\n', 'utf8');
    await writeFile(
        IMPACT_JSON,
        JSON.stringify({ ...impact, ...shared, extras: extras.impact ?? null, refCheck: extras.refCheck ?? null }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', BUG_JSON, 'und', IMPACT_JSON);

    const ok =
        failure === null
        && bug.staticChains >= 1
        && bug.observedPathShown === true
        && bug.observedCount === RUN_COUNT
        && bug.observedLabel === RUN_LABEL
        && bug.staticOnlyCount >= 1
        && bug.runtimeOnlyCount >= 1
        && bug.noTracesHonest === true
        && bug.hopClickNavigates === true
        && bug.port >= MIN_PORT
        && bug.leftoverProcesses === 0
        && impact.directCount >= 1
        && impact.downstreamCount >= 1
        && impact.endpointNamed.length > 0
        && ['low', 'medium', 'high'].includes(impact.badge)
        && impact.badgeRulesExplained.length > 10
        && impact.narrativeEvidenceOk === true
        && impact.invalidRefNoEngineCall === true
        && impact.tilesShown >= 5
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w4b] W4b-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w4b] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        if (workspace) {
            console.error('[smoke-w4b] Fixture-Kopie bleibt liegen:', workspace);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, workspace, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W4b-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w4b] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
