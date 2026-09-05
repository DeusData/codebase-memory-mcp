#!/usr/bin/env node
/*
 * W7c-Smoke: der Chat findet das Symbol, das der Leser meint, und das Panel
 * benimmt sich wie ein Panel.
 *
 * Fuenf Nutzerbefunde vom 2026-08-29 stehen dahinter, zwei davon echte Fehler
 * mit belegter Ursache:
 *
 *  1. "@createuser explain this funktion": das Such-Overlay zeigte createUser
 *     (src/services/userService.ts L23), der Chat antwortete "the index holds no
 *     symbol called 'createuser'". Zwei Lesungen desselben Index widersprachen
 *     sich auf einem Bildschirm. Ursache: buchstabengetreue Vergleiche in
 *     src/compiler/subject-resolver.ts.
 *  2. Ein vorhandener Fokus wurde verschenkt: mention nicht aufloesbar hiess gar
 *     kein Subjekt, obwohl der Twin auf createUser stand.
 *  3. Mehrdeutigkeit (@create trifft zwei Symbole) wurde stillschweigend
 *     aufgeloest statt angeboten.
 *  4. Das Panel liess sich nicht in der Hoehe ziehen, die Kontext-Auswahl
 *     scrollte weg, und der einzige Weg, es loszuwerden, kostete den Verlauf.
 *  5. "[l]lm on/off" sagte nicht, dass das Modell lokal laeuft.
 *
 * ## Der eine Kunstgriff dieses Laufs, und warum er einer ist
 *
 * Das Produkt redet mit 127.0.0.1:4141, und dieser Port gehoert dem Nutzer:
 * dort laeuft sein eigener Sidecar, waehrend dieser Lauf laeuft. Ihn zu belegen
 * oder zu beenden waere ein Beweislauf, der die Arbeitsumgebung anfasst, die er
 * beweisen soll. Also startet dieser Lauf sein EIGENES llama-server-Exemplar auf
 * einem eigenen Port (>= 4420) und leitet die Anfragen dorthin um, und zwar an
 * genau der Naht, an der die Anwendung sie selbst stellt: `window.fetch`. Alles
 * davor (der Zustandsautomat des Sidecars, die Bereitschaftsprobe, der Chat, der
 * Antwortvertrag) laeuft unveraendert.
 *
 * Nachgewiesen wird beides: dass die Antwort von einem echten Modell kommt UND
 * dass an 4141 kein einziges Byte ging (`productPortRequests`, muss 0 sein). Der
 * Route-Handler bricht eine Anfrage dorthin ausdruecklich ab, statt sie
 * durchzulassen: ein Lauf, der den fremden Prozess "nur einmal kurz" fragt, hat
 * ihn angefasst.
 *
 * ## Ablauf
 *
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (nur gelesen)
 *   4. C-Server auf einem freien Port >= 4420, dist/ auf einem zweiten
 *   5. eigener llama-server auf einem dritten, mit dem Klasse-A-Sieger
 *   6. Chromium ohne Aussenwelt, mit Route-Sperre und der Umleitung oben
 *      a. Modell an, dann eine Frage OHNE Symbol und OHNE Fokus
 *      b. Datei oeffnen, Caret in createUser: jetzt gibt es einen Fokus
 *      c. "@createuser explain this function": Karten, Antwort, Zitate
 *      d. "@create": Kandidatenliste, einer wird geklickt, Frage laeuft weiter
 *      e. ein Name, den es wirklich nicht gibt, bei vorhandenem Fokus
 *      f. Kopfzeile beim Scrollen, Hoehe per Tastatur, Hoehe per Maus
 *      g. Escape, Schliessen-Knopf, Wiederoeffnen, clear
 *      h. die drei Stellen, an denen das lokale Modell benannt wird
 *      i. Reload: die gezogene Hoehe steht noch
 *   7. abraeumen, Restprozesse zaehlen (auch den eigenen Sidecar), JSON, Bilder
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w7c).
 */

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
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
import { LLAMA_SERVER, startLlama, stopLlama, llamaProps } from './lib/llama.mjs';
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
const PROJECT = 'codeatlasweb-w7c';
const OUT_DIR = join(ROOT, 'verification', 'w7');
const OUT_JSON = join(OUT_DIR, 'chat.json');
const EVAL_JSON = join(ROOT, 'verification', 'w5', 'eval.json');
const SHOT_ANSWER = join(OUT_DIR, 'chat-answer.png');
const SHOT_RESIZED = join(OUT_DIR, 'chat-resized.png');

/** 4390/4391 gehoeren der Vorschau des Nutzers, 4400 der Eval. Ab hier ist frei. */
const MIN_PORT = 4420;

/** Der Port des Produkts. Dieser Lauf belegt ihn nicht und redet nicht mit ihm. */
const PRODUCT_SIDECAR_PORT = 4141;
const PRODUCT_SIDECAR_ORIGIN = `http://127.0.0.1:${PRODUCT_SIDECAR_PORT}`;

/** Die Modelldatei je Klasse-A-Sieger. Dieselbe Bruecke wie in smoke-w5b. */
const MODEL_OF = {
    'Qwen3.5-2B': { file: 'Qwen3.5-2B-Q4_K_M.gguf', ctx: 3072 },
    'LFM2.5-1.2B': { file: 'LFM2.5-1.2B-Instruct-Q4_K_M.gguf', ctx: 3072 },
    'MiniCPM5-1B': { file: 'MiniCPM5-1B-Q4_K_M.gguf', ctx: 3072 },
    'Qwen2.5-Coder-1.5B': { file: 'qwen2.5-coder-1.5b-instruct-q4_k_m.gguf', ctx: 3072 },
};

/**
 * Die fuenf Fragen dieses Laufs, woertlich.
 *
 * Sie stehen hier und nicht verstreut im Ablauf, weil sie der Beweis sind: die
 * erste ist die des Nutzer-Screenshots, die letzte ist dieselbe wie die zweite
 * und unterscheidet sich nur darin, ob ein Symbol im Fokus steht.
 */
const Q_LOWERCASE = '@createuser explain this function';
const Q_NO_CARD = 'Was macht @nichtsDergleichenImIndex?';
const Q_AMBIGUOUS = 'Was macht @create?';
const Q_FOCUS_FALLBACK = '@nichtsDergleichenImIndex explain this function';

/** Wie lange auf `ready` und auf eine Antwort gewartet wird. */
const READY_TIMEOUT_MS = 240000;
const ANSWER_TIMEOUT_MS = 240000;

/** Chromium ohne Aussenwelt. Wortgleich mit den Laeufen davor. */
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

const log = (...parts) => console.log('[smoke-w7c]', ...parts);
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

const chatSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasChat;
        if (seam === undefined) {
            return null;
        }
        const { validateRefine, ...rest } = seam;
        return JSON.parse(JSON.stringify(rest));
    });

const llmSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasLlm;
        return seam === undefined ? null : { ...seam };
    });

/** Was im Chat-Panel wirklich steht, samt der Geometrie, um die es hier geht. */
const chatDom = (page) =>
    page.evaluate(() => {
        const text = (element) => element?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        const box = (element) => {
            if (element === null) {
                return null;
            }
            const rect = element.getBoundingClientRect();
            return {
                top: Math.round(rect.top),
                bottom: Math.round(rect.bottom),
                left: Math.round(rect.left),
                height: Math.round(rect.height),
                width: Math.round(rect.width),
            };
        };
        const panel = document.querySelector('[data-testid="atlas-chat"]');
        const scroll = document.querySelector('[data-testid="atlas-chat-scroll"]');
        const depth = document.querySelector('[data-testid="atlas-chat-depth"]');
        /*
         * Der Rahmen des Chats ist seit W8 die Zone, in der er als Reiter liegt.
         *
         * W7c hatte dem Panel eine eigene Hoehe, einen eigenen Griff und einen
         * eigenen Schliess-Knopf gegeben, alles auf Nutzerbefunde hin. Die
         * Zusicherungen dahinter gelten unveraendert; sie werden nur an dem
         * Element gemessen, das sie jetzt haelt. Was NICHT umgezogen ist, wird
         * weiter am Chat selbst gelesen: der stehende Kopf, die
         * Kontext-Auswahl, der Bildlauf des Verlaufs und `clear`.
         */
        const zone = document.querySelector('[data-testid="atlas-explain"]');
        const handle = document.querySelector('[data-testid="atlas-split-explain"]');
        const fold = document.querySelector('[data-testid="atlas-explain-collapse"]');
        return {
            present: panel !== null,
            open: zone?.getAttribute('data-open') === 'true'
                && zone?.getAttribute('data-tab') === 'chat',
            // Die Zahl der Zuege kommt aus dem Griff und nicht aus dem DOM: sie
            // ist die Frage "ist mein Verlauf noch da", und die muss auch dann
            // beantwortbar sein, wenn der Bereich gerade eingeklappt ist.
            turns: (globalThis.__atlasChat?.turns ?? []).length,
            height: globalThis.__atlasLayout?.sizes?.explainHeight ?? 0,
            panelBox: box(panel),
            headBox: box(document.querySelector('[data-testid="atlas-chat-head"]')),
            depthBox: box(depth),
            scrollBox: box(scroll),
            scrollTop: Math.round(scroll?.scrollTop ?? 0),
            scrollHeight: Math.round(scroll?.scrollHeight ?? 0),
            clientHeight: Math.round(scroll?.clientHeight ?? 0),
            resizePresent: handle !== null,
            resizeBox: box(handle),
            resizeCursor: handle === null ? '' : getComputedStyle(handle).cursor,
            resizeRole: handle?.getAttribute('role') ?? '',
            resizeTabIndex: handle?.getAttribute('tabindex') ?? '',
            closeLabel: text(fold),
            closeTitle: fold?.getAttribute('title') ?? '',
            clearLabel: text(document.querySelector('[data-testid="atlas-chat-clear"]')),
            clearTitle: document.querySelector('[data-testid="atlas-chat-clear"]')
                ?.getAttribute('title') ?? '',
            reopenPresent:
                document.querySelector('[data-testid="atlas-explain-tab"][data-tab="chat"]') !== null,
            collapsedNote: text(document.querySelector('[data-testid="atlas-explain-note"]')),
            /*
             * Je Zug, was in SEINEM Kasten steht.
             *
             * Die flachen Listen darunter sind ueber alle Zuege gezogen und
             * beantworten "steht es ueberhaupt da". Fuer die Zusicherungen
             * dieses Laufs ist das zu grob: ob DIESE Antwort ein Zitat traegt,
             * ist eine Frage an einen Zug und nicht an das Panel.
             */
            turnViews: [...document.querySelectorAll('[data-testid="atlas-chat-turn"]')]
                .map((entry) => ({
                    question: text(entry.querySelector('[data-testid="atlas-chat-question"]')),
                    status: entry.getAttribute('data-status') ?? '',
                    citations: [...new Set(
                        [...entry.querySelectorAll('[data-testid="atlas-chat-citation"]')]
                            .map((mark) => mark.getAttribute('data-card') ?? ''),
                    )],
                    unknownCitations:
                        entry.querySelectorAll('[data-testid="atlas-chat-citation-unknown"]').length,
                    fallback: text(entry.querySelector('[data-testid="atlas-chat-fallback"]')),
                    choiceHead: text(entry.querySelector('[data-testid="atlas-chat-choice-head"]')),
                    candidates: [...entry.querySelectorAll('[data-testid="atlas-chat-candidate"]')]
                        .map((mark) => ({
                            label: text(mark),
                            qualified: mark.getAttribute('data-qualified') ?? '',
                            visible: mark.getBoundingClientRect().width > 0,
                        })),
                })),
            questions: [...document.querySelectorAll('[data-testid="atlas-chat-question"]')]
                .map((entry) => text(entry)),
            answerLines: [...document.querySelectorAll('[data-testid="atlas-chat-line"]')]
                .map((entry) => text(entry)),
            citations: [...document.querySelectorAll('[data-testid="atlas-chat-citation"]')]
                .map((entry) => entry.getAttribute('data-card') ?? ''),
            messages: [...document.querySelectorAll('[data-testid="atlas-chat-message"]')]
                .map((entry) => text(entry)),
            fallbacks: [...document.querySelectorAll('[data-testid="atlas-chat-fallback"]')]
                .map((entry) => text(entry)),
            choiceHeads: [...document.querySelectorAll('[data-testid="atlas-chat-choice-head"]')]
                .map((entry) => text(entry)),
            candidates: [...document.querySelectorAll('[data-testid="atlas-chat-candidate"]')]
                .map((entry) => ({
                    label: text(entry),
                    qualified: entry.getAttribute('data-qualified') ?? '',
                    visible: entry.getBoundingClientRect().width > 0,
                })),
            viewportHeight: window.innerHeight,
        };
    });

/** Wie das lokale Modell an seinen drei Stellen heisst. */
const llmNaming = (page) =>
    page.evaluate(() => {
        const text = (element) => element?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        const chip = [...document.querySelectorAll('.atlas-statusbar .atlas-chip')]
            .find((entry) => (entry.getAttribute('data-chip') ?? '').includes('llm'));
        return {
            menuRaw: text(document.querySelector('[data-menu="a-llm"]')),
            menuTitle: document.querySelector('[data-menu="a-llm"]')?.getAttribute('title') ?? '',
            chip: text(chip ?? null),
            chipLabel: chip?.getAttribute('data-chip') ?? '',
            heading: text(document.querySelector('.atlas-llm-title')),
        };
    });

/**
 * Ein Etikett ohne seine Merk-Klammern.
 *
 * `[l]ocal llm on` ist EIN Wort mit einer Markierung darin: die Klammern sagen,
 * welcher Buchstabe das Kuerzel ist, und sind nicht Teil des Namens. Das Produkt
 * liest sie an derselben Stelle genauso (`splitMenuLabel` in
 * src/app/AtlasChrome.tsx). Der rohe Text steht trotzdem mit im Bericht, damit
 * niemand diese Lesung glauben muss.
 */
const withoutMnemonic = (label) => label.replace(/[[\]]/g, '').trim();

/** Nur die Anfragen, die wirklich eine Antwort erbeten haben. */
const completionsIn = (requests) =>
    requests.filter((entry) => entry.url.includes('/v1/chat/completions')).length;

/** Eine Frage in die Kommandozeile tippen und abschicken, wie ein Leser es tut. */
async function ask(page, question) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.type(question, { delay: 8 });
    await page.waitForTimeout(600);
    await input.press('Enter');
}

/** Warten, bis ein Zug zu dieser Frage diesen Zustand hat. */
const waitForTurn = (page, needle, status, timeout) =>
    page.waitForFunction(
        ({ needle: word, status: wanted }) =>
            (globalThis.__atlasChat?.turns ?? []).some(
                (turn) => turn.question.includes(word) && turn.status === wanted,
            ),
        { needle, status },
        { timeout },
    );

const turnOf = (seam, needle) =>
    (seam?.turns ?? []).filter((turn) => turn.question.includes(needle)).slice(-1)[0];

const viewOf = (dom, needle) =>
    (dom?.turnViews ?? []).filter((view) => view.question.includes(needle)).slice(-1)[0];

async function openApp(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForFunction(
        () => (globalThis.__atlasLlm?.policyVerdict ?? '') !== '',
        undefined,
        { timeout: 60000 },
    );
}

const waitForLlm = (page, state, timeout) =>
    page.waitForFunction(
        (expected) => globalThis.__atlasLlm?.state === expected,
        state,
        { timeout },
    );

async function main() {
    const totalStarted = Date.now();
    let serverChild = null;
    let llama = null;
    let proxy = null;
    let browser = null;
    let home = null;
    let runtimeDir = null;
    let serverPort = 0;
    let uiPort = 0;
    let llamaPort = 0;
    let failure = null;
    const timings = {};

    const report = {
        lowercaseMentionResolves: false,
        lowercaseMentionCitations: 0,
        mentionCandidateListShown: false,
        mentionCandidateCount: 0,
        mentionCandidatePickAnswers: false,
        focusFallbackAnswered: false,
        focusFallbackExplained: false,
        noCardStillHonest: false,
        chatResizeWorks: false,
        chatResizePersists: false,
        chatResizeByKeyboard: false,
        chatHeaderStaysVisible: false,
        chatClosesByEscape: false,
        chatClosesByButton: false,
        historySurvivesClose: false,
        clearStillClears: false,
        llmMenuLabel: '',
        llmStatusChip: '',
        llmPanelHeading: '',
        overlapViolations: 0,
        clippingViolations: 0,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = {
        blockedRequests: [],
        consoleErrors: [],
        pageErrors: [],
        sidecarRequests: [],
        productPortRequests: [],
        readability: [],
    };

    const routeFor = (origin, llamaOrigin) => async (route) => {
        const url = route.request().url();
        if (url.startsWith(PRODUCT_SIDECAR_ORIGIN)) {
            /*
             * Darf nicht vorkommen und wird darum abgebrochen statt
             * durchgelassen: auf 4141 laeuft der Sidecar des Nutzers, und ein
             * Beweislauf, der ihn "nur einmal kurz" fragt, hat ihn angefasst.
             */
            extras.productPortRequests.push(url);
            await route.abort();
            return;
        }
        if (url.startsWith(llamaOrigin)) {
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
            throw new Error(`Binary fehlt: ${BINARY}`);
        }
        if (!existsSync(FIXTURE)) {
            throw new Error(`Fixture fehlt: ${FIXTURE}`);
        }
        if (!existsSync(LLAMA_SERVER)) {
            throw new Error('vendor/llama/llama-server fehlt (siehe vendor/llama/HERKUNFT.md)');
        }
        if (!existsSync(EVAL_JSON)) {
            throw new Error(`${EVAL_JSON} fehlt: erst 'npm run eval:llm' fahren.`);
        }

        const evalReport = JSON.parse(await readFile(EVAL_JSON, 'utf8'));
        const winner = evalReport?.winnerClassA?.name ?? '';
        const model = MODEL_OF[winner];
        if (model === undefined) {
            throw new Error(`unbekannter Klasse-A-Sieger in eval.json: "${winner}"`);
        }
        if (!existsSync(join(ROOT, 'models', model.file))) {
            throw new Error(`Modell fehlt: models/${model.file}`);
        }
        extras.model = { winner, ...model };
        log(`Klasse-A-Sieger aus eval.json: ${winner} (models/${model.file})`);

        // ------------------------------------------------------------ 1. Bau
        log('npm run build');
        const buildStarted = Date.now();
        const build = await run('npm', ['run', 'build']);
        timings.buildMs = Date.now() - buildStarted;
        if (build.code !== 0) {
            throw new Error(`npm run build endete mit ${build.code}`);
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w7c-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w7c-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);

        // -------------------------------------------------------- 3. Projekt
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };

        // ------------------------------------------- 4. Server, Proxy, Modell
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        extras.serverPort = serverPort;

        llamaPort = await findFreePort(MIN_PORT, [serverPort, uiPort]);
        const llamaOrigin = `http://127.0.0.1:${llamaPort}`;
        const llamaLog = [];
        const llamaStarted = Date.now();
        llama = await startLlama({
            modelFile: model.file,
            contextTokens: model.ctx,
            port: llamaPort,
            log: llamaLog,
        });
        timings.llamaReadyMs = Date.now() - llamaStarted;
        extras.llama = { port: llamaPort, props: await llamaProps(llamaPort), readyMs: llama.readyMs };
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}, eigener Sidecar auf ${llamaPort}`);

        // ------------------------------------------------------- 5. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const origin = `http://127.0.0.1:${uiPort}`;
        const context = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
        /*
         * Die Umleitung, an der Naht, an der die Anwendung ihre Anfragen selbst
         * stellt. Siehe Kopf: 4141 gehoert dem Nutzer.
         */
        await context.addInitScript(({ from, to }) => {
            const original = globalThis.fetch.bind(globalThis);
            globalThis.fetch = (input, init) => {
                const url = typeof input === 'string'
                    ? input
                    : input instanceof URL ? input.href : input?.url ?? '';
                if (typeof url === 'string' && url.startsWith(from)) {
                    return original(to + url.slice(from.length), init);
                }
                return original(input, init);
            };
        }, { from: PRODUCT_SIDECAR_ORIGIN, to: llamaOrigin });
        await context.route('**/*', routeFor(origin, llamaOrigin));
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
                bottom: {
                    candidates: bottom.candidates,
                    overlaps: bottom.overlaps,
                    clipped: bottom.clipped,
                },
            });
            report.overlapViolations += top.overlaps.length + bottom.overlaps.length;
            report.clippingViolations += top.clipped.length + bottom.clipped.length;
        };

        await openApp(page, origin);

        // ------------------------------------------------- 6a. Modell an
        await page.click('[data-menu="a-llm"]');
        await waitForLlm(page, 'ready', READY_TIMEOUT_MS);
        log('eigener Sidecar bereit, Anwendung meldet ready');

        /*
         * Die Frage ohne Symbol UND ohne Fokus. Sie steht VOR dem Oeffnen der
         * Datei, weil sie genau das beweisen soll: ohne beides bleibt der
         * vereinbarte Satz, und das Modell wird nicht gefragt.
         */
        const beforeNoCard = completionsIn(extras.sidecarRequests);
        await ask(page, Q_NO_CARD);
        await waitForTurn(page, 'nichtsDergleichen', 'no-cards', 60000);
        await page.waitForTimeout(400);
        const noCardSeam = await chatSeam(page);
        const noCardDom = await chatDom(page);
        const noCardTurn = turnOf(noCardSeam, 'nichtsDergleichen');
        const noCardView = viewOf(noCardDom, 'nichtsDergleichen');
        extras.noCard = {
            turn: noCardTurn,
            view: noCardView,
            messages: noCardDom.messages,
            completionsBefore: beforeNoCard,
            completionsAfter: completionsIn(extras.sidecarRequests),
        };
        report.noCardStillHonest =
            noCardTurn !== undefined
            && noCardTurn.status === 'no-cards'
            && noCardTurn.cards === 0
            && /no card/i.test(noCardTurn.answer)
            && noCardTurn.focusFallbackUsed === ''
            && noCardView !== undefined
            && noCardView.fallback === ''
            && noCardView.candidates.length === 0
            && noCardDom.messages.some((message) => message.includes('was not asked'))
            && completionsIn(extras.sidecarRequests) === beforeNoCard;
        log(`ohne Symbol und ohne Fokus: "${noCardTurn?.answer}" `
            + `(keine Anfrage: ${completionsIn(extras.sidecarRequests) === beforeNoCard})`);

        // ------------------------------------- 6b. Ein Symbol in den Fokus
        await page.click('[data-testid="atlas-tree-row"][data-path="src/services/userService.ts"]');
        await page.waitForFunction(
            () => (globalThis.__atlasReader?.document?.path ?? '') === 'src/services/userService.ts',
            undefined,
            { timeout: 30000 },
        );
        await page.evaluate(() => {
            globalThis.__atlasReader?.editor?.setPosition?.({ lineNumber: 24, column: 5 });
            globalThis.__atlasReader?.editor?.focus?.();
        });
        await page.waitForFunction(
            () => (globalThis.__atlasTwin?.qualifiedName ?? '').endsWith('createUser'),
            undefined,
            { timeout: 30000 },
        );
        extras.focusSymbol = await page.evaluate(() => globalThis.__atlasTwin?.qualifiedName ?? '');
        log(`Fokus im Twin: ${extras.focusSymbol}`);

        // ------------------------ 6c. Die Frage des Screenshots, kleingeschrieben
        const askStarted = Date.now();
        await ask(page, Q_LOWERCASE);
        await waitForTurn(page, '@createuser', 'answered', ANSWER_TIMEOUT_MS);
        timings.answerMs = Date.now() - askStarted;
        await page.waitForTimeout(500);
        const lowerSeam = await chatSeam(page);
        const lowerDom = await chatDom(page);
        const lowerTurn = turnOf(lowerSeam, '@createuser');
        const lowerView = viewOf(lowerDom, '@createuser');
        /*
         * Gezaehlt wird, was in diesem Zug als Zitat DASTEHT: jedes `[Kn]`, das
         * eine wirklich uebergebene Karte nennt, wird vom Panel zu einem Knopf
         * (segmentsOf in src/compiler/answer-contract.ts), und alles andere
         * bleibt Text mit Warnung. Das ist die Zahl, die der Leser sieht.
         *
         * `turn.citations` aus dem Griff steht daneben und kann kleiner sein:
         * die Zitatpruefung laesst bei einer am Token-Limit abgeschnittenen
         * Antwort die letzte Zeile aus, und ein Modell, das seine ganze Antwort
         * in EINE Zeile schreibt, verliert damit alle. Das ist eine Eigenschaft
         * der Pruefung und keine des Zitats; beide Zahlen stehen im Bericht.
         */
        extras.lowercase = {
            turn: lowerTurn,
            view: lowerView,
            citationsInSeam: lowerTurn?.citations.length ?? 0,
            answerLines: lowerDom.answerLines,
        };
        report.lowercaseMentionCitations = lowerView?.citations.length ?? 0;
        /*
         * `focusFallbackUsed === ''` ist der scharfe Teil: die Antwort kommt
         * daher, dass der NAME aufgeloest wurde, und nicht daher, dass zufaellig
         * derselbe Fokus danebenstand. Ohne diese Bedingung wuerde AC1 von AC3
         * bewiesen.
         */
        report.lowercaseMentionResolves =
            lowerTurn !== undefined
            && lowerTurn.status === 'answered'
            && lowerTurn.cards >= 1
            && lowerTurn.focusFallbackUsed === ''
            && lowerView !== undefined
            && lowerView.fallback === ''
            && report.lowercaseMentionCitations >= 1;
        log(`"@createuser": ${lowerTurn?.cards} Karten, `
            + `${report.lowercaseMentionCitations} Zitate im Kasten `
            + `(${lowerTurn?.citations.length} in der Zitatpruefung), `
            + `Rueckfall auf den Fokus: ${lowerTurn?.focusFallbackUsed === '' ? 'nein' : 'ja'}`);

        await page.screenshot({ path: SHOT_ANSWER, fullPage: false });
        log('chat-answer.png geschrieben');

        // -------------------------------------------- 6d. Die Mehrdeutigkeit
        const beforeChoice = completionsIn(extras.sidecarRequests);
        await ask(page, Q_AMBIGUOUS);
        await waitForTurn(page, '@create?', 'needs-choice', 60000);
        await page.waitForTimeout(400);
        const choiceSeam = await chatSeam(page);
        const choiceDom = await chatDom(page);
        const choiceTurn = turnOf(choiceSeam, '@create?');
        const choiceView = viewOf(choiceDom, '@create?');
        report.mentionCandidateCount = choiceTurn?.candidates.length ?? 0;
        report.mentionCandidateListShown =
            choiceTurn !== undefined
            && choiceTurn.status === 'needs-choice'
            && choiceTurn.candidates.length >= 2
            && choiceView !== undefined
            && choiceView.candidates.length >= 2
            && choiceView.candidates.every(
                (entry) => entry.visible && /:\d+\)/.test(entry.label),
            )
            && choiceView.choiceHead.includes('create')
            && completionsIn(extras.sidecarRequests) === beforeChoice;
        extras.choice = {
            turn: choiceTurn,
            shown: choiceView?.candidates ?? [],
            head: choiceView?.choiceHead ?? '',
        };
        log(`"@create": ${report.mentionCandidateCount} Kandidaten angeboten, keine Anfrage: `
            + `${completionsIn(extras.sidecarRequests) === beforeChoice}`);

        // Einen Kandidaten anklicken: die Frage laeuft mit dem gewaehlten Symbol weiter.
        const shownCandidates = choiceView?.candidates ?? [];
        const picked = shownCandidates[1] ?? shownCandidates[0];
        if (picked === undefined) {
            throw new Error('keine Kandidaten angeboten, es gibt nichts zu waehlen');
        }
        await page.click(`[data-testid="atlas-chat-candidate"][data-qualified="${picked.qualified}"]`);
        await page.waitForFunction(
            (qualified) => (globalThis.__atlasChat?.turns ?? []).some(
                (turn) => turn.status === 'answered' && turn.question.includes('@create?')
                    && turn.cards > 0 && turn.rule !== '' && qualified.length > 0,
            ),
            picked.qualified,
            { timeout: ANSWER_TIMEOUT_MS },
        );
        await page.waitForTimeout(400);
        const pickedSeam = await chatSeam(page);
        const pickedTurn = (pickedSeam?.turns ?? [])
            .filter((turn) => turn.question.includes('@create?') && turn.status === 'answered')
            .slice(-1)[0];
        extras.picked = { qualified: picked.qualified, turn: pickedTurn };
        report.mentionCandidatePickAnswers =
            pickedTurn !== undefined && pickedTurn.cards >= 1 && pickedTurn.answer.length > 0;
        log(`Kandidat ${picked.qualified} gewaehlt: ${pickedTurn?.cards} Karten`);

        // ------------------------------------- 6e. Der Rueckfall auf den Fokus
        await ask(page, Q_FOCUS_FALLBACK);
        await waitForTurn(page, 'nichtsDergleichenImIndex explain', 'answered', ANSWER_TIMEOUT_MS);
        await page.waitForTimeout(400);
        const fallbackSeam = await chatSeam(page);
        const fallbackDom = await chatDom(page);
        const fallbackTurn = turnOf(fallbackSeam, 'nichtsDergleichenImIndex explain');
        const fallbackView = viewOf(fallbackDom, 'nichtsDergleichenImIndex explain');
        const fallbackLine = fallbackView?.fallback ?? '';
        extras.focusFallback = { turn: fallbackTurn, view: fallbackView, line: fallbackLine };
        report.focusFallbackAnswered =
            fallbackTurn !== undefined
            && fallbackTurn.status === 'answered'
            && fallbackTurn.cards >= 1
            && fallbackTurn.focusFallbackUsed.endsWith('createUser');
        report.focusFallbackExplained =
            fallbackLine.includes('nichtsDergleichenImIndex')
            && fallbackLine.includes('createUser')
            && /not found in the index/i.test(fallbackLine);
        log(`Rueckfall: "${fallbackLine}"`);

        await readability('chat mit Verlauf');

        // ------------------------------------- 6f. Kopfzeile, Hoehe, Tastatur
        const scrollChatToEnd = () =>
            page.evaluate(() => {
                const box = document.querySelector('[data-testid="atlas-chat-scroll"]');
                if (box !== null) {
                    box.scrollTop = box.scrollHeight;
                }
                return box === null ? 0 : box.scrollTop;
            });

        const headerVisible = async (label) => {
            await scrollChatToEnd();
            await page.waitForTimeout(250);
            const dom = await chatDom(page);
            const chips = dom.depthBox;
            const panel = dom.panelBox;
            const ok =
                chips !== null
                && panel !== null
                && chips.height > 0
                && chips.top >= panel.top - 1
                && chips.bottom <= panel.bottom + 1
                && chips.top >= 0
                && chips.bottom <= dom.viewportHeight
                && dom.scrollTop > 0;
            extras.header = [...(extras.header ?? []), { label, chips, panel, scrollTop: dom.scrollTop }];
            log(`Kopfzeile ${label}: Chips ${JSON.stringify(chips)}, gescrollt ${dom.scrollTop} `
                + `-> sichtbar ${ok}`);
            return ok;
        };

        const atDefaultHeight = await headerVisible('bei Vorgabehoehe');

        // Die Tastatur: der Griff bekommt den Fokus und die Pfeile bewegen ihn.
        const heightBeforeKeys = (await chatDom(page)).height;
        await page.focus('[data-testid="atlas-split-explain"]');
        const handleFocused = await page.evaluate(() =>
            document.activeElement?.getAttribute('data-testid') === 'atlas-split-explain');
        for (let i = 0; i < 6; i += 1) {
            await page.keyboard.press('ArrowDown');
        }
        await page.waitForTimeout(200);
        const heightAfterDown = (await chatDom(page)).height;
        for (let i = 0; i < 3; i += 1) {
            await page.keyboard.press('ArrowUp');
        }
        await page.waitForTimeout(200);
        const heightAfterUp = (await chatDom(page)).height;
        report.chatResizeByKeyboard =
            handleFocused && heightAfterDown < heightBeforeKeys && heightAfterUp > heightAfterDown;
        extras.keyboardResize = {
            handleFocused, heightBeforeKeys, heightAfterDown, heightAfterUp,
        };
        log(`Tastatur am Griff: ${heightBeforeKeys} -> ${heightAfterDown} -> ${heightAfterUp}`);

        const atSmallHeight = await headerVisible('bei kleiner Hoehe');
        report.chatHeaderStaysVisible = atDefaultHeight && atSmallHeight;

        // Die Maus: an der oberen Kante ziehen.
        const beforeDrag = await chatDom(page);
        const handle = beforeDrag.resizeBox;
        if (handle === null) {
            throw new Error('der Griff hat kein Rechteck');
        }
        const handleX = handle.left + Math.round(handle.width / 2);
        const handleY = handle.top + Math.round(handle.height / 2);
        await page.mouse.move(handleX, handleY);
        await page.mouse.down();
        await page.mouse.move(handleX, handleY - 180, { steps: 12 });
        await page.mouse.up();
        await page.waitForTimeout(300);
        const afterDrag = await chatDom(page);
        report.chatResizeWorks =
            afterDrag.height > beforeDrag.height
            && (afterDrag.panelBox?.height ?? 0) > (beforeDrag.panelBox?.height ?? 0)
            && beforeDrag.resizeCursor === 'row-resize'
            && beforeDrag.resizeRole === 'separator'
            && beforeDrag.resizeTabIndex === '0';
        extras.drag = {
            before: { height: beforeDrag.height, box: beforeDrag.panelBox },
            after: { height: afterDrag.height, box: afterDrag.panelBox },
            cursor: beforeDrag.resizeCursor,
        };
        log(`Ziehen: ${beforeDrag.height} -> ${afterDrag.height} Pixel `
            + `(Cursor "${beforeDrag.resizeCursor}")`);
        const draggedHeight = afterDrag.height;

        await page.screenshot({ path: SHOT_RESIZED, fullPage: false });
        log('chat-resized.png geschrieben');

        await readability('chat gezogen');

        // ------------------------------------ 6g. Escape, Knopf, clear
        const turnsBeforeClose = (await chatDom(page)).questions.length;
        await page.focus('[data-testid="atlas-split-explain"]');
        await page.keyboard.press('Escape');
        await page.waitForTimeout(300);
        const closedByEscape = await chatDom(page);
        /*
         * Escape klappt ein und kostet nichts. Vier Zahlen statt drei, weil der
         * Chat seit W8 als Reiter aushaengt, wenn die Zone zugeht: dass er
         * nicht mehr im Baum steht, ist der Sinn der Sache und darf nicht mit
         * "der Verlauf ist weg" verwechselt werden. Also wird beides gemessen,
         * die Zone ist zu UND der Verlauf steht noch, und die Zeile im
         * eingeklappten Streifen sagt es dem Leser mit derselben Zahl.
         */
        report.chatClosesByEscape =
            closedByEscape.present === false
            && !closedByEscape.open
            && closedByEscape.turns === turnsBeforeClose
            && closedByEscape.collapsedNote.includes(String(turnsBeforeClose))
            && closedByEscape.reopenPresent;
        log(`Escape: offen ${closedByEscape.open}, Zuege noch ${closedByEscape.turns}`);

        await page.click('[data-testid="atlas-explain-tab"][data-tab="chat"]');
        await page.waitForTimeout(300);
        const reopened = await chatDom(page);
        report.historySurvivesClose =
            reopened.open
            && reopened.questions.length === turnsBeforeClose
            && turnsBeforeClose > 0;
        extras.close = {
            turnsBeforeClose,
            afterEscape: { open: closedByEscape.open, turns: closedByEscape.turns },
            afterReopen: { open: reopened.open, questions: reopened.questions.length },
            collapsedNote: closedByEscape.collapsedNote,
            closeLabel: reopened.closeLabel,
            closeTitle: reopened.closeTitle,
            clearLabel: reopened.clearLabel,
            clearTitle: reopened.clearTitle,
        };
        log(`Wiederoeffnen: ${reopened.questions.length} von ${turnsBeforeClose} Fragen wieder da`);

        await page.click('[data-testid="atlas-explain-collapse"]');
        await page.waitForTimeout(300);
        const closedByButton = await chatDom(page);
        report.chatClosesByButton =
            closedByButton.present === false
            && !closedByButton.open
            && closedByButton.turns === turnsBeforeClose
            /*
             * Die zwei Knoepfe sagen, welcher was kostet. Gemessen an ihren
             * Beschriftungen und Titeln und nicht an einer Absicht: ein Paar,
             * bei dem man raten muss, welcher der unwiderrufliche ist, waere
             * schlimmer als der eine Knopf, den es vorher gab.
             */
            && /esc/i.test(reopened.closeLabel)
            && /keeps its history/i.test(reopened.closeTitle)
            && /clear/i.test(reopened.clearLabel)
            && /deleted/i.test(reopened.clearTitle);

        await page.click('[data-testid="atlas-explain-tab"][data-tab="chat"]');
        await page.waitForTimeout(300);
        await page.click('[data-testid="atlas-chat-clear"]');
        await page.waitForTimeout(400);
        const cleared = await chatDom(page);
        const clearedSeam = await chatSeam(page);
        report.clearStillClears =
            !cleared.present && (clearedSeam?.turns.length ?? -1) === 0;
        extras.cleared = { present: cleared.present, turns: clearedSeam?.turns.length ?? -1 };
        log(`clear: Panel weg ${!cleared.present}, Zuege ${clearedSeam?.turns.length}`);

        // ------------------------------- 6h. Wie das Modell ueberall heisst
        const naming = await llmNaming(page);
        report.llmMenuLabel = withoutMnemonic(naming.menuRaw);
        report.llmStatusChip = naming.chip;
        report.llmPanelHeading = naming.heading;
        extras.naming = naming;
        log(`Benennung: Menue "${naming.menuRaw}" (ohne Klammern "${report.llmMenuLabel}"), `
            + `Chip "${naming.chip}", Ueberschrift "${naming.heading}"`);

        await readability('nach clear');

        // ------------------------------------- 6i. Reload: die Hoehe steht noch
        await openApp(page, origin);
        await page.waitForTimeout(600);
        const afterReload = await chatSeam(page);
        report.chatResizePersists = (afterReload?.height ?? 0) === draggedHeight;
        extras.persistence = { draggedHeight, afterReload: afterReload?.height ?? 0 };
        log(`nach dem Reload: Hoehe ${afterReload?.height} (gezogen war ${draggedHeight})`);

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        extras.completions = completionsIn(extras.sidecarRequests);
        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w7c] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w7c] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (llama !== null) {
        await stopLlama(llama.child);
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopServer(serverChild);
    await sleep(900);

    /*
     * Gezaehlt werden die drei Ports DIESES Laufs, den eigenen Sidecar
     * eingeschlossen. 4141 steht daneben zur Kenntnis und faerbt nichts: dort
     * laeuft der Prozess des Nutzers, den dieser Lauf weder gestartet hat noch
     * beenden darf. Dieselbe Regel wie in tools/eval-check.mjs.
     */
    const leftovers = [];
    for (const port of [serverPort, uiPort, llamaPort].filter((value) => value > 0)) {
        leftovers.push({ port, listeners: await countListeners(port) });
    }
    extras.leftovers = leftovers;
    report.leftoverProcesses = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
    extras.productPort = {
        port: PRODUCT_SIDECAR_PORT,
        sockets: await countListeners(PRODUCT_SIDECAR_PORT),
        usedByThisRun: false,
        requestsFromThisRun: extras.productPortRequests.length,
        note: 'Der Produktport. Dieser Lauf startet dort nichts, beendet dort nichts und schickt '
            + 'nichts dorthin: die Anfragen der Anwendung werden an der fetch-Naht auf den eigenen '
            + 'Sidecar umgeleitet. Die Zahl beschreibt den Rechner, nicht diesen Lauf.',
    };
    log('leftoverProcesses:', report.leftoverProcesses, JSON.stringify(leftovers));

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            llmMenuLabelRaw: extras.naming?.menuRaw ?? '',
            sidecarPort: llamaPort,
            productSidecarPort: PRODUCT_SIDECAR_PORT,
            productPortRequests: extras.productPortRequests.length,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            questions: {
                lowercaseMention: Q_LOWERCASE,
                noCard: Q_NO_CARD,
                ambiguous: Q_AMBIGUOUS,
                focusFallback: Q_FOCUS_FALLBACK,
            },
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = existsSync(SHOT_ANSWER) && existsSync(SHOT_RESIZED);
    const ok =
        failure === null
        && report.lowercaseMentionResolves === true
        && report.lowercaseMentionCitations >= 1
        && report.mentionCandidateListShown === true
        && report.mentionCandidateCount >= 2
        && report.mentionCandidatePickAnswers === true
        && report.focusFallbackAnswered === true
        && report.focusFallbackExplained === true
        && report.noCardStillHonest === true
        && report.chatResizeWorks === true
        && report.chatResizePersists === true
        && report.chatResizeByKeyboard === true
        && report.chatHeaderStaysVisible === true
        && report.chatClosesByEscape === true
        && report.chatClosesByButton === true
        && report.historySurvivesClose === true
        && report.clearStillClears === true
        && /local llm (on|off)/i.test(report.llmMenuLabel)
        && /local llm/i.test(report.llmStatusChip)
        && /local (model|llm)/i.test(report.llmPanelHeading)
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && extras.productPortRequests.length === 0
        && shotsOk
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w7c] W7c-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w7c] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W7c-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w7c] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
