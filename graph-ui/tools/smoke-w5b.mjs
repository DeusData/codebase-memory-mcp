#!/usr/bin/env node
/*
 * W5b-Smoke: die Kommandozeile ist der Chat, die Zitate sind Knoepfe, und der
 * Compiler haelt sich an die Karten, die er selbst geschrieben hat.
 *
 * Was hier bewiesen wird, koennen die Unit-Tests nicht beweisen. Sie zeigen an
 * erfundenen Antworten, dass der Klassifikator deterministisch ist, dass die
 * Karten unter dem Budget bleiben und dass ein `[K3]` ein Knopf wird. Sie sagen
 * nichts darueber, ob eine gebaute Seite mit einem echten Sidecar auf 4141 eine
 * Frage aus der Fusszeile beantwortet, ob ein Klick auf ein Zitat wirklich in
 * der Zieldatei landet, ob der Schalter "aus" die Anfrage wirklich verhindert,
 * und ob der refine-Knopf ohne Modell wirklich fehlt.
 *
 * Ablauf, wie bei smoke-w5a:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis
 *   3. fixtures/atlas-sample indizieren (nur gelesen)
 *   4. C-Server auf einem freien Port >= 4310, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, mit einer Route-Sperre, die 127.0.0.1:4141
 *      ausdruecklich DURCHLAESST und mitzaehlt
 *   6a. LLM aus: die Frage landet im Panel, aber KEIN Byte geht an 4141
 *   6b. llm/start.sh mit dem Klasse-A-Sieger, warten auf ready
 *   6c. "Wer ruft createUser?" in die Kommandozeile, Enter
 *   6d. Zitat anklicken: der Reader steht in der Zieldatei
 *   6e. Karten aufklappen, Tiefenschalter lesen und umstellen
 *   6f. refine: Knopf da bei ready, und der echte Validator lehnt Unsinn ab
 *   6g. den Fokus ueber den Caret aufloesen, das belegen, und dann eine Frage
 *       ohne Karte: der vereinbarte Satz faellt, ohne Anfrage
 *   7. abraeumen, Restprozesse zaehlen (auch den Sidecar), JSON und Bild
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w5b). 4141 ist
 * Loopback und zaehlt dort nicht als Verstoss.
 *
 * ## Drei Entscheidungen, die man sonst raten muesste
 *
 * **Der Klasse-A-Sieger kommt aus verification/w5/eval.json.** Nicht aus einer
 * Konstante hier: das Kopf-an-Kopf-Rennen entscheidet, welches Modell das
 * Produkt fahren soll, und ein Beweislauf, der ein anderes faehrt, beweist
 * etwas ueber ein Modell, das niemand gewaehlt hat. Fehlt die Datei, bricht der
 * Lauf ab statt still auf 1b zu fallen.
 *
 * **Die Null wird zweimal gemessen, so wie in W5a.** Der Route-Handler zaehlt
 * jede Anfrage an 4141, und `__atlasLlm.probes` zaehlt jeden Aufruf der Anwendung.
 * Beide muessen null sein, solange das Modell aus ist, auch nachdem eine Frage
 * gestellt wurde.
 *
 * **Die Ablehnung einer kaputten Umformulierung wird am echten Validator
 * gemessen.** `__atlasChat.validateRefine` ruft dieselbe Funktion, die der Knopf
 * ruft, mit einer absichtlich falschen Antwort. Ein Lauf, der die Regel
 * nachbaut, prueft seine eigene Kopie und nicht das Produkt.
 *
 * **Die Frage ohne Karte steht ganz am Ende, und der Fokus wird vorher
 * aufgeloest.** Seit W7c antwortet der Chat auf einen nicht aufloesbaren
 * @mention zum Symbol im Fokus, wenn eines dasteht, und sagt in einer eigenen
 * Zeile, dass er das getan hat. Der vereinbarte Keine-Karte-Satz gilt dem
 * anderen Fall: kein aufloesbares Symbol UND kein Fokus. Diese Frage an einer
 * Stelle zu stellen, an der noch das Subjekt eines frueheren Abschnitts im Twin
 * steht, wuerde also den falschen Fall messen. Sie steht deshalb hinter allen
 * Abschnitten, die ein Subjekt brauchen; davor wandert der Caret ueber die
 * Oberflaeche auf eine Zeile, in der keine Funktion steht, und dass der Twin
 * danach wirklich leer ist, wird gemessen und mitgeschrieben statt angenommen.
 */

import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
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
const PROJECT = 'codeatlasweb-w5b';
const OUT_DIR = join(ROOT, 'verification', 'w5');
const OUT_JSON = join(OUT_DIR, 'chat.json');
const EVAL_JSON = join(OUT_DIR, 'eval.json');
const SHOT_CHAT = join(OUT_DIR, 'chat.png');
const MIN_PORT = 4310;

const SIDECAR_PORT = 4141;
const SIDECAR_ORIGIN = `http://127.0.0.1:${SIDECAR_PORT}`;

/** Die Frage des Contracts. Sie steht hier woertlich, weil sie der Beweis ist. */
const QUESTION = 'Wer ruft createUser?';

/**
 * Eine Frage, fuer die der Index nichts hat. Sie muss ohne Anfrage enden.
 *
 * Sie wird ohne Fokus gestellt; siehe die vierte Entscheidung im Kopf.
 */
const NO_CARD_QUESTION = 'Was macht @nichtsDergleichenImIndex?';

/** Die Datei, in der dieser Lauf den Caret setzt. */
const FOCUS_FILE = 'src/services/userService.ts';

/** Eine Zeile im Rumpf von createUser: dort hat der Twin ein Subjekt. */
const SYMBOL_LINE = 24;

/**
 * Die Leerzeile im Kopf derselben Datei, ueber allem, was ein Subjekt waere.
 *
 * `resolveSymbolAt` sucht nur nach Method, Function und Class, und die erste
 * Funktion dieser Datei faengt erst in Zeile 9 an. Der Caret hier ist der Weg
 * der Oberflaeche, ein Subjekt wieder loszuwerden: die Anwendung raeumt den
 * Twin daraufhin selbst und sagt, man solle den Cursor in eine Funktion
 * setzen. Bewusst nicht eine Leerzeile ZWISCHEN zwei Funktionen: wo die
 * Engine das Ende der einen sieht, ist eine Frage an die Engine, und dieser
 * Lauf will an dieser Stelle nichts fragen, was er nicht messen will.
 */
const NO_SYMBOL_LINE = 3;

/** Wie lange auf `ready` gewartet wird. */
const READY_TIMEOUT_MS = 180000;

/** Chromium ohne Aussenwelt. Wortgleich mit smoke-w5a. */
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

const log = (...parts) => console.log('[smoke-w5b]', ...parts);
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

/**
 * Die Modellwahl, mit der llm/start.sh den Klasse-A-Sieger startet.
 *
 * Die Abbildung steht hier und nicht im Skript, weil das Skript Wahlen kennt
 * und die Eval Modelle: `1b` und `Qwen3.5-2B` sind dieselbe Datei unter zwei
 * Namen, und die Bruecke gehoert an die Stelle, die beide liest.
 */
const CHOICE_OF = {
    'Qwen3.5-2B': 'class-a',
    'LFM2.5-1.2B': 'class-a-lfm',
    'MiniCPM5-1B': 'class-a-minicpm',
    'Qwen2.5-Coder-1.5B': 'class-a-coder',
    'Qwen3.5-4B': 'class-b',
    'gemma-4-E4B': 'class-b-gemma',
};

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

/** Was im Chat-Panel wirklich steht. */
const chatDom = (page) =>
    page.evaluate(() => {
        const text = (element) => element?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        const panel = document.querySelector('[data-testid="atlas-chat"]');
        const depth = document.querySelector('[data-testid="atlas-chat-depth"]');
        return {
            present: panel !== null,
            turns: Number(panel?.getAttribute('data-turns') ?? '0'),
            questions: [...document.querySelectorAll('[data-testid="atlas-chat-question"]')]
                .map((entry) => text(entry)),
            answerLines: [...document.querySelectorAll('[data-testid="atlas-chat-line"]')]
                .map((entry) => text(entry)),
            citations: [...document.querySelectorAll('[data-testid="atlas-chat-citation"]')]
                .map((entry) => entry.getAttribute('data-card') ?? ''),
            unknownCitations: [...document.querySelectorAll('[data-testid="atlas-chat-citation-unknown"]')]
                .map((entry) => text(entry)),
            messages: [...document.querySelectorAll('[data-testid="atlas-chat-message"]')]
                .map((entry) => text(entry)),
            warnings: [...document.querySelectorAll('[data-testid="atlas-chat-warning"]')]
                .map((entry) => text(entry)),
            cardsToggle: text(document.querySelector('[data-testid="atlas-chat-cards-toggle"]')),
            cardsShown: document.querySelectorAll('[data-testid="atlas-chat-card"]').length,
            provenance: [...document.querySelectorAll('[data-testid="atlas-chat-provenance"]')]
                .map((entry) => text(entry)),
            depthPresent: depth !== null,
            depthValue: depth?.getAttribute('data-depth') ?? '',
            depthOptions: [...document.querySelectorAll('[data-testid="atlas-chat-depth-option"]')]
                .map((entry) => ({
                    value: entry.getAttribute('data-value') ?? '',
                    on: entry.getAttribute('data-on') === 'true',
                    title: entry.getAttribute('title') ?? '',
                })),
            depthNote: text(document.querySelector('[data-testid="atlas-chat-depth-note"]')),
        };
    });

/** Wo der Reader gerade steht. Derselbe Griff, den smoke-w4c benutzt. */
const readerAt = (page) =>
    page.evaluate(() => ({
        path: globalThis.__atlasReader?.document?.path ?? '',
        line: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
    }));

/**
 * Was im Twin steht: aus dem Griff UND aus dem, was auf dem Schirm zu lesen ist.
 *
 * Beides, weil dieser Lauf mit dieser Aufnahme belegen will, dass vor der Frage
 * ohne Karte wirklich kein Subjekt dasteht. Ein leerer Griff allein liesse
 * offen, ob das Panel noch das alte Symbol zeigt.
 */
const twinState = (page) =>
    page.evaluate(() => {
        const text = (element) => element?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        const panel = document.querySelector('[data-testid="atlas-twin"]');
        return {
            symbol: globalThis.__atlasTwin?.symbol ?? '',
            qualifiedName: globalThis.__atlasTwin?.qualifiedName ?? '',
            sections: (globalThis.__atlasTwin?.sectionNames ?? []).length,
            domStatus: panel?.getAttribute('data-status') ?? '',
            domSubject: text(document.querySelector('[data-testid="atlas-twin-subject"]')),
            domEmpty: text(document.querySelector('[data-testid="atlas-twin-empty"]')),
            path: globalThis.__atlasReader?.document?.path ?? '',
            caretLine: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
        };
    });

/** Steht wirklich kein Subjekt mehr im Twin? Griff und Schirm muessen es sagen. */
const TWIN_IS_EMPTY = () =>
    (globalThis.__atlasTwin?.qualifiedName ?? '') === ''
    && document.querySelector('[data-testid="atlas-twin"]')
        ?.getAttribute('data-status') === 'empty';

/**
 * Den Fokus ueber die Oberflaeche aufloesen: Caret auf eine Zeile ohne Symbol.
 *
 * Gesetzt wird so lange nach, bis der Twin wirklich leer ist. Ein einzelnes
 * `setPosition` kann verpuffen, wenn der Editor die Datei gerade erst
 * uebernimmt: der Wechsel der aktiven Datei setzt die Caret-Zustaende der
 * Anwendung zurueck, und ein Sprung, der davor liegt, ist danach weg. Der
 * Rueckgabewert sagt, ob es gelungen ist; geraten wird hier nichts.
 */
async function dissolveFocus(page, lineNumber) {
    for (let attempt = 0; attempt < 8; attempt += 1) {
        await page.evaluate((line) => {
            const editor = globalThis.__atlasReader?.editor;
            editor?.setPosition?.({ lineNumber: line, column: 1 });
            editor?.focus?.();
        }, lineNumber);
        const cleared = await page
            .waitForFunction(TWIN_IS_EMPTY, undefined, { timeout: 5000 })
            .then(() => true)
            .catch(() => false);
        if (cleared) {
            return true;
        }
    }
    return false;
}

/**
 * Die Zeilen des Pseudocode-Blocks, so wie sie auf dem Bildschirm stehen.
 *
 * Ausdruecklich nur die Kinder der Zeilen-Liste. Die Import-Gruppe darueber
 * traegt dieselbe Marke wie eine Gruppenzeile des Blocks, und sie
 * mitzuzaehlen hiesse, dem Validator eine Zeile mehr zu schicken, als der
 * Block hat: die Ablehnung waere dann richtig aus dem falschen Grund.
 */
const pseudocodeLines = (page) =>
    page.evaluate(() =>
        [...document.querySelectorAll('.atlas-pseudocode-lines > li')]
            .map((entry) => entry.textContent?.trim() ?? ''));

/** Nur die Anfragen, die wirklich eine Antwort erbeten haben. */
const completionsIn = (requests) =>
    requests.filter((entry) => entry.url.includes('/v1/chat/completions')).length;

/** Der Zustand des refine-Knopfes im Pseudocode-Block. */
const refineDom = (page) =>
    page.evaluate(() => ({
        buttonPresent: document.querySelector('[data-testid="atlas-pseudocode-refine-btn"]') !== null,
        rowPresent: document.querySelector('[data-testid="atlas-pseudocode-refine"]') !== null,
        note: document.querySelector('[data-testid="atlas-pseudocode-refine-note"]')
            ?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
    }));

/** Eine Frage in die Kommandozeile tippen und abschicken, wie ein Leser es tut. */
async function ask(page, question) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.type(question, { delay: 8 });
    await page.waitForTimeout(600);
    await input.press('Enter');
}

async function openApp(page, origin, project) {
    await page.goto(`${origin}/?project=${project}`, { waitUntil: 'load' });
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
    let proxy = null;
    let browser = null;
    let home = null;
    let runtimeDir = null;
    let serverPort = 0;
    let uiPort = 0;
    let failure = null;
    let sidecarStarted = false;
    const timings = {};

    const result = {
        answered: false,
        citationsInAnswer: 0,
        citationClickNavigates: false,
        cardsShown: 0,
        offHonest: false,
        noCardHonest: false,
        neighborDepthDefault: 0,
        neighborDepthAdjustable: false,
        neighborQualityNoteShown: false,
        refineGuarded: false,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = { blockedRequests: [], consoleErrors: [], pageErrors: [], sidecarRequests: [] };

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
        if (!existsSync(EVAL_JSON)) {
            throw new Error(
                `${EVAL_JSON} fehlt: erst 'npm run eval:llm' fahren. Der Klasse-A-Sieger dieses `
                + 'Rennens ist das Modell, das dieser Lauf faehrt.',
            );
        }

        const evalReport = JSON.parse(await readFile(EVAL_JSON, 'utf8'));
        const winner = evalReport?.winnerClassA?.name ?? '';
        const choice = CHOICE_OF[winner];
        if (choice === undefined) {
            throw new Error(`unbekannter Klasse-A-Sieger in eval.json: "${winner}"`);
        }
        extras.winner = {
            name: winner,
            choice,
            passRate: evalReport.winnerClassA.passRate,
            citationCompliance: evalReport.winnerClassA.citationCompliance,
        };
        log(`Klasse-A-Sieger aus eval.json: ${winner} (llm/start.sh ${choice})`);

        // ------------------------------------------------------------ 1. Bau
        log('npm run build');
        const buildStarted = Date.now();
        const build = await run('npm', ['run', 'build']);
        timings.buildMs = Date.now() - buildStarted;
        if (build.code !== 0) {
            throw new Error(`npm run build endete mit ${build.code}`);
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w5b-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w5b-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);

        // -------------------------------------------------------- 3. Projekt
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };

        // ------------------------------------------------- 4. Server, Proxy
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        result.port = uiPort;
        extras.serverPort = serverPort;
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

        await openApp(page, origin, PROJECT);

        // ------------------------------ 6a. Fragen mit ausgeschaltetem Modell
        await ask(page, QUESTION);
        await page.waitForSelector('[data-testid="atlas-chat"]', { timeout: 15000 });
        await page.waitForTimeout(1500);
        const offDom = await chatDom(page);
        const offSeam = await chatSeam(page);
        const offLlm = await llmSeam(page);
        extras.off = { dom: offDom, seam: offSeam, llm: offLlm };
        result.offHonest =
            offDom.present
            && offDom.turns === 1
            && offSeam?.turns[0]?.status === 'refused'
            && offSeam?.turns[0]?.refusal === 'off'
            && offDom.messages.some((message) => message.includes('the local model is off'))
            && offDom.messages.some((message) => message.includes(String(SIDECAR_PORT)))
            && extras.sidecarRequests.length === 0
            && offLlm?.probes === 0;
        log(`LLM aus: Zug "${offSeam?.turns[0]?.status}", Anfragen an ${SIDECAR_PORT}: `
            + `${extras.sidecarRequests.length}`);

        // Der refine-Knopf darf jetzt NICHT dastehen. Dafuer muss ein Symbol im
        // Twin stehen und die Pseudocode-Ansicht offen sein.
        await page.click(`[data-testid="atlas-tree-row"][data-path="${FOCUS_FILE}"]`);
        await page.waitForFunction(
            (path) => (globalThis.__atlasReader?.document?.path ?? '') === path,
            FOCUS_FILE,
            { timeout: 30000 },
        );
        // Der Caret in den Rumpf von createUser, damit der Twin ein Subjekt hat.
        await page.evaluate((line) => {
            globalThis.__atlasReader?.editor?.setPosition?.({ lineNumber: line, column: 5 });
            globalThis.__atlasReader?.editor?.focus?.();
        }, SYMBOL_LINE);
        await page.waitForFunction(
            () => (globalThis.__atlasTwin?.qualifiedName ?? '').endsWith('createUser'),
            undefined,
            { timeout: 30000 },
        );
        await page.click('[data-testid="atlas-pseudocode-toggle"]');
        await page.waitForSelector('[data-testid="atlas-pseudocode"]', { timeout: 15000 });
        const refineOff = await refineDom(page);
        extras.refineOff = refineOff;

        // ---------------------------------------- 6b. Den Sidecar starten
        const startStarted = Date.now();
        const startRun = await run('sh', [join(ROOT, 'llm', 'start.sh'), choice]);
        sidecarStarted = true;
        extras.startScript = { exit: startRun.code, out: startRun.out.trim().split('\n') };
        if (startRun.code !== 0) {
            throw new Error(`llm/start.sh endete mit ${startRun.code}`);
        }
        await page.click('[data-menu="a-llm"]');
        await waitForLlm(page, 'ready', READY_TIMEOUT_MS);
        timings.readyMs = Date.now() - startStarted;
        log(`Sidecar ready nach ${timings.readyMs} ms`);

        // ------------------------------------------- 6c. Die eigentliche Frage
        const askStarted = Date.now();
        await ask(page, QUESTION);
        await page.waitForFunction(
            () => (globalThis.__atlasChat?.turns ?? []).some(
                (turn) => turn.question.startsWith('Wer ruft') && turn.status === 'answered',
            ),
            undefined,
            { timeout: 180000 },
        );
        timings.answerMs = Date.now() - askStarted;
        await page.waitForTimeout(500);
        const answeredSeam = await chatSeam(page);
        const answeredTurn = answeredSeam.turns.find(
            (turn) => turn.question.startsWith('Wer ruft') && turn.status === 'answered',
        );
        extras.answeredTurn = answeredTurn;
        result.answered = answeredTurn !== undefined && answeredTurn.answer.length > 0;
        result.citationsInAnswer = answeredTurn?.citations.length ?? 0;
        log(`Antwort nach ${timings.answerMs} ms, Zitate: ${result.citationsInAnswer}`);

        // ------------------------------------------ 6d. Ein Zitat anklicken
        await page.click('[data-testid="atlas-chat-cards-toggle"]');
        await page.waitForTimeout(400);
        const withCards = await chatDom(page);
        result.cardsShown = withCards.cardsShown;
        extras.answeredDom = withCards;

        const before = await readerAt(page);
        const citation = page.locator('[data-testid="atlas-chat-citation"]').first();
        const citedCard = await citation.getAttribute('data-card');
        await citation.click();
        await page.waitForTimeout(2500);
        const after = await readerAt(page);
        extras.citationClick = { citedCard, before, after };
        result.citationClickNavigates =
            after.path.length > 0
            && (after.path !== before.path || after.line !== before.line);
        log(`Zitat ${citedCard} geklickt: ${before.path}:${before.line} -> ${after.path}:${after.line}`);

        // -------------------------------------------- 6e. Die Tiefen-Einstellung
        const depthDom = await chatDom(page);
        result.neighborDepthDefault = Number(depthDom.depthValue);
        result.neighborQualityNoteShown =
            depthDom.depthNote.includes('can make it worse')
            && depthDom.depthNote.includes('neighbour');
        extras.depth = { dom: depthDom };
        await page.click('[data-testid="atlas-chat-depth-option"][data-value="2"]');
        await page.waitForTimeout(300);
        const afterDepth = await chatDom(page);
        const afterDepthSeam = await chatSeam(page);
        extras.depthAfter = { dom: afterDepth, seam: { depth: afterDepthSeam.depth } };
        result.neighborDepthAdjustable =
            depthDom.depthOptions.map((option) => option.value).join(',') === '0,1,2'
            && afterDepth.depthValue === '2'
            && afterDepthSeam.depth === 2;
        // Zurueck auf die Vorgabe, damit das Bild den Normalfall zeigt.
        await page.click('[data-testid="atlas-chat-depth-option"][data-value="1"]');
        await page.waitForTimeout(300);
        log(`Tiefe: Vorgabe ${result.neighborDepthDefault}, umstellbar ${result.neighborDepthAdjustable}`);

        await page.screenshot({ path: SHOT_CHAT, fullPage: true });
        log('chat.png geschrieben');

        // ------------------------------------------------------ 6f. Refine
        await page.waitForSelector('[data-testid="atlas-pseudocode"]', { timeout: 20000 });
        const refineReady = await refineDom(page);
        /*
         * Drei absichtlich kaputte Antworten, gegen den ECHTEN Validator, den
         * auch der Knopf ruft. Die dritte ist die interessanteste: sie hat die
         * richtige Zeilenzahl und nur an einer Stelle eine falsche Nummer, also
         * genau die Aenderung, die eine Umformulierung nicht machen darf und die
         * ein Leser am Text nicht sehen wuerde.
         */
        const blockLines = await pseudocodeLines(page);
        extras.blockLines = blockLines;
        const validation = await page.evaluate((lines) => {
            const seam = globalThis.__atlasChat;
            if (seam === undefined) {
                return null;
            }
            const renumbered = lines
                .map((line) => (/^\d+[.)]\s/.test(line) ? line.replace(/^\d+/, '99') : line))
                .join('\n');
            return {
                brokenCount: seam.validateRefine('1. call validateUser'),
                brokenNumbers: seam.validateRefine(renumbered),
                empty: seam.validateRefine(''),
                goodEnough: seam.validateRefine(lines.join('\n')),
            };
        }, blockLines);
        extras.refine = { off: extras.refineOff, ready: refineReady, validation };
        result.refineGuarded =
            extras.refineOff.buttonPresent === false
            && refineReady.buttonPresent === true
            && validation?.brokenCount.applied === false
            && validation.brokenCount.reason.includes('different number of lines')
            && validation.brokenNumbers.applied === false
            && validation.brokenNumbers.reason.includes('different number than it was sent with')
            && validation.empty.applied === false
            && validation.goodEnough.applied === true;
        log(`refine: aus=${extras.refineOff.buttonPresent}, ready=${refineReady.buttonPresent}, `
            + `Ablehnung "${validation?.brokenCount.reason}"`);

        // ------------------- 6g. Den Fokus aufloesen, dann die Frage ohne Karte
        /*
         * Erst der Weg zurueck auf eine Stelle ohne Symbol, ueber dieselbe
         * Oberflaeche, die den Fokus vorher gesetzt hat: dieselbe Datei, der
         * Caret in den Kopf ueber der ersten Funktion. Der Twin wird nicht von
         * aussen geleert, sondern die Anwendung raeumt ihn selbst, weil an
         * dieser Zeile nichts steht, was sie erklaeren koennte.
         */
        if ((await readerAt(page)).path !== FOCUS_FILE) {
            await page.click(`[data-testid="atlas-tree-row"][data-path="${FOCUS_FILE}"]`);
            await page.waitForFunction(
                (path) => (globalThis.__atlasReader?.document?.path ?? '') === path,
                FOCUS_FILE,
                { timeout: 30000 },
            );
        }
        if (!(await dissolveFocus(page, NO_SYMBOL_LINE))) {
            throw new Error(
                `der Twin blieb besetzt, obwohl der Caret auf Zeile ${NO_SYMBOL_LINE} von `
                + `${FOCUS_FILE} steht: ${JSON.stringify(await twinState(page))}`,
            );
        }
        const twinBeforeNoCard = await twinState(page);
        log(`Twin vor der Frage: Status "${twinBeforeNoCard.domStatus}", Subjekt `
            + `"${twinBeforeNoCard.symbol}", qualifiziert "${twinBeforeNoCard.qualifiedName}"`);

        /*
         * Gezaehlt werden nur Antwortanfragen und nicht jede Anfrage an 4141:
         * die Bereitschaftsprobe laeuft alle drei Sekunden weiter, und ein
         * Vergleich ueber alle Anfragen wuerde messen, wie lange dieser
         * Abschnitt gedauert hat.
         */
        const beforeNoCard = completionsIn(extras.sidecarRequests);
        await ask(page, NO_CARD_QUESTION);
        await page.waitForFunction(
            () => (globalThis.__atlasChat?.turns ?? []).some(
                (turn) => turn.question.includes('nichtsDergleichen') && turn.status === 'no-cards',
            ),
            undefined,
            { timeout: 60000 },
        );
        await page.waitForTimeout(500);
        const noCardSeam = await chatSeam(page);
        const noCardDom = await chatDom(page);
        const noCardTurn = noCardSeam.turns.find((turn) => turn.question.includes('nichtsDergleichen'));
        extras.noCard = {
            twinBefore: twinBeforeNoCard,
            turn: noCardTurn,
            completions: completionsIn(extras.sidecarRequests),
            messages: noCardDom.messages,
        };
        /*
         * `focusFallbackUsed === ''` ist seit W7c der scharfe Teil. Ohne diese
         * Bedingung koennte der Satz auch dann noch fallen, wenn die Antwort in
         * Wahrheit einem danebenstehenden Fokus gegolten haette; der gemessene
         * Twin-Zustand daneben sagt, dass es gar keinen gab.
         */
        result.noCardHonest =
            twinBeforeNoCard.qualifiedName === ''
            && twinBeforeNoCard.domStatus === 'empty'
            && noCardTurn !== undefined
            && noCardTurn.status === 'no-cards'
            && noCardTurn.cards === 0
            && /no card/i.test(noCardTurn.answer)
            && noCardTurn.focusFallbackUsed === ''
            && noCardDom.messages.some((message) => message.includes('was not asked'))
            && completionsIn(extras.sidecarRequests) === beforeNoCard;
        log(`ohne Symbol und ohne Fokus: "${noCardTurn?.answer}" (keine neue Anfrage: `
            + `${completionsIn(extras.sidecarRequests) === beforeNoCard}, Rueckfall: `
            + `${noCardTurn?.focusFallbackUsed === '' ? 'nein' : 'ja'})`);

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w5b] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w5b] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (sidecarStarted) {
        const rescue = await run('sh', [join(ROOT, 'llm', 'stop.sh')]);
        extras.stopScript = { exit: rescue.code, out: rescue.out.trim().split('\n') };
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopServer(serverChild);
    await sleep(900);

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
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            question: QUESTION,
            noCardQuestion: NO_CARD_QUESTION,
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotOk = existsSync(SHOT_CHAT);
    const ok =
        failure === null
        && result.answered === true
        && result.citationsInAnswer >= 1
        && result.citationClickNavigates === true
        && result.cardsShown >= 2
        && result.offHonest === true
        && result.noCardHonest === true
        && result.neighborDepthDefault === 1
        && result.neighborDepthAdjustable === true
        && result.neighborQualityNoteShown === true
        && result.refineGuarded === true
        && result.port >= MIN_PORT
        && result.leftoverProcesses === 0
        && shotOk
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w5b] W5b-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w5b] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W5b-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w5b] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
