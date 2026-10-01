#!/usr/bin/env node
/*
 * W4a-Smoke: die Einstiegsmodi an einem echten Server.
 *
 * Was hier bewiesen wird, koennen die Unit-Tests nicht beweisen. Sie zeigen an
 * einer eingefrorenen Fixture, dass der Topsort dieselbe Ordnung liefert, dass
 * ein Closure-Walk terminiert und dass eine Karte ihre Tasten beschriftet. Sie
 * sagen nichts darueber, ob dieser Server eine Zusammenfassung liefert, aus der
 * eine Fuehrung entsteht, ob ein Enter im Browser den Reader bewegt, ob der Twin
 * einem Schritt folgt, ob die stille Anzeige dabei steigt und ob der Satz ueber
 * einen gekappten Walk wirklich dort steht, wo ein Leser ihn saehe.
 *
 * Ablauf, wie bei smoke-w3:
 *   1. `npm run build`
 *   2. isoliertes HOME, fixtures/atlas-sample ueber die CLI indizieren
 *   3. C-Server auf einem freien Port >= 4260 starten
 *   4. dist/ auf einem zweiten Port ausliefern, /rpc und /api dorthin proxen
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *   6. Klickstrecke: Frage, Projekt-Fuehrung, Vorwaerts-Walk, Deckel-Beweis
 *   7. abraeumen, Restprozesse zaehlen, verification/w4/tours.json schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w4a).
 *
 * ## Der Deckel-Beweis
 *
 * An einem Projekt mit zehn Dateien kappt der Vorgabe-Deckel von fuenfzehn
 * Symbolen nichts, und ein Satz, den man nie sieht, ist kein bewiesener Satz.
 * Also wird der Vorwaerts-Walk zweimal gefahren: einmal mit den Vorgaben, wo am
 * Ende KEIN Deckel-Satz stehen darf, und einmal mit `?codeatlasClosureCap=3`,
 * wo er stehen MUSS und die Zahlen des Aufrufs nennen muss. Erst beide
 * Richtungen zusammen sind der Beweis; eine davon allein waere entweder eine
 * unbewiesene Behauptung oder ein Satz, der immer dasteht.
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
const PROJECT = 'codeatlasweb-w4a';
const OUT_DIR = join(ROOT, 'verification', 'w4');
const OUT_JSON = join(OUT_DIR, 'tours.json');
const MIN_PORT = 4260;

/** Das Symbol, an dem der Vorwaerts-Walk beginnt. */
const ENTRY_SYMBOL = 'createUser';
const ENTRY_FILE = 'src/services/userService.ts';

/** Der Deckel, mit dem der zweite Walk gefahren wird. Klein genug, dass er greift. */
const SMALL_CAP = 3;

/**
 * Die Woerter, die in keinem sichtbaren Text vorkommen duerfen.
 *
 * Dieselbe Liste steht in src/why/why-model.ts als AVOIDED_WORDS, wo die Texte
 * geschrieben werden. Hier steht sie noch einmal, weil dieser Lauf gegen das
 * fertige Bild prueft und nicht gegen den Quelltext: eine Vokabel, die ueber
 * einen dritten Weg auf den Bildschirm kommt, faellt nur hier auf.
 */
const AVOIDED_WORDS = ['learn', 'lesson', 'course', 'tutorial', 'student'];

/** Chromium ohne Aussenwelt. Wortgleich mit smoke-w3, ohne die GL-Flags. */
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

const log = (...parts) => console.log('[smoke-w4a]', ...parts);
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

/** Der Griff der Fuehrung, so wie die Anwendung ihn gerade fuehrt. */
const tourSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasTour;
        return seam === undefined
            ? null
            : {
                id: seam.id,
                kind: seam.kind,
                title: seam.title,
                steps: seam.steps,
                index: seam.index,
                paths: seam.paths,
                titles: seam.titles,
                endNote: seam.endNote,
            };
    });

/** Was in der Statusleiste ueber die stille Anzeige steht, und was der Speicher haelt. */
const checklistSeam = (page) =>
    page.evaluate(() => ({
        seam: globalThis.__atlasChecklist ?? null,
        chip:
            document.querySelector('[data-chip="explored"]')?.textContent?.replace(/\s+/g, ' ').trim()
            ?? null,
    }));

/** Wo Reader und Twin gerade stehen. */
const readerSeam = (page) =>
    page.evaluate(() => ({
        readerPath: globalThis.__atlasReader?.document?.path ?? '',
        twin: globalThis.__atlasTwin?.symbol ?? '',
        twinQn: globalThis.__atlasTwin?.qualifiedName ?? '',
    }));

/**
 * Eine Taste ans Fenster geben, ohne dass ein Eingabefeld sie schluckt.
 *
 * Der Klick auf die Marke nimmt den Fokus aus dem Editor und aus jedem Feld;
 * `tourKeyForEvent` laesst die Taste dann durch.
 */
async function pressGlobally(page, key) {
    await page.click('.atlas-brand');
    await page.keyboard.press(key);
}

/** Die Frage wieder aufrufen und einen Modus waehlen. */
async function openWhyAndChoose(page, intent) {
    await page.click('[data-menu="a-why"]');
    await page.waitForSelector('[data-testid="atlas-why"]', { timeout: 15000 });
    await page.click(`[data-testid="atlas-why-card"][data-intent="${intent}"]`);
}

/** Im Einstiegsdialog suchen und den Treffer mit diesem Namen waehlen. */
async function chooseEntryHit(page, name) {
    await page.waitForSelector('[data-testid="atlas-entry"]', { timeout: 20000 });
    const input = page.locator('[data-testid="atlas-entry-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(name, { delay: 40 });
    await page.waitForSelector(`[data-testid="atlas-entry-hit"][data-name="${name}"]`, { timeout: 20000 });
    // Ueber die Entprellung hinaus, damit die Liste die eines fertigen Wortes ist.
    await page.waitForTimeout(600);
    await page.click(`[data-testid="atlas-entry-hit"][data-name="${name}"]`);
    await page.waitForFunction(() => globalThis.__atlasTour?.kind === 'entry', undefined, {
        timeout: 40000,
    });
    await page.waitForSelector('[data-testid="atlas-tour"]', { timeout: 20000 });
}

/** Bis ans Ende eines Walks klicken, ohne ihn zu beenden. */
async function walkToLastStep(page) {
    const seam = await tourSeam(page);
    for (let step = seam.index; step < seam.steps - 1; step += 1) {
        await page.click('[data-testid="atlas-tour-next"]');
        await page.waitForFunction(
            (expected) => globalThis.__atlasTour?.index === expected,
            step + 1,
            { timeout: 20000 },
        );
        await page.waitForTimeout(150);
    }
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
        whyShown: false,
        modesNeutral: false,
        steps: 0,
        orderCorrect: false,
        deterministic: false,
        playerKeyboardNavigates: false,
        twinFollowsStep: false,
        stepMarksVisited: false,
        exploredCounterRises: false,
        /** Der Zaehler der Statusleiste am selben Symbol, vor und nach einem Vermerk. */
        exploredBefore: null,
        exploredAfter: null,
        entryPointTourStartsAtChosen: false,
        entryFirstStep: '',
        entrySteps: 0,
        entryHasNoConfigPrelude: false,
        closureTruncationHonest: false,
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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w4a-home-'));
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

        // ------------------------------------ 6a. Frische App: die Frage
        await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
        // Ein frischer Kontext hat einen leeren Speicher. Ausdruecklich gemessen,
        // damit "die Frage kam" nicht heisst "sie kam trotz einer Antwort".
        extras.storageAtStart = await page.evaluate(() =>
            Object.keys(globalThis.localStorage ?? {}).filter((key) => key.startsWith('atlas-')),
        );
        await page.waitForSelector('[data-testid="atlas-why"]', { timeout: 30000 });
        report.whyShown = true;
        await mkdir(OUT_DIR, { recursive: true });

        const visibleAtWhy = await page.evaluate(() => document.body.innerText ?? '');
        extras.whyCards = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid="atlas-why-card"]')].map((node) => ({
                intent: node.getAttribute('data-intent'),
                text: (node.textContent ?? '').replace(/\s+/g, ' ').trim(),
            })),
        );
        const hits = AVOIDED_WORDS.filter((word) => visibleAtWhy.toLowerCase().includes(word));
        extras.avoidedWordsFound = hits;
        extras.whyHeadline = await page.evaluate(
            () => document.querySelector('[data-testid="atlas-why-headline"]')?.textContent?.trim() ?? '',
        );
        report.modesNeutral = hits.length === 0;
        log(`Frage sichtbar, ${extras.whyCards.length} Karten, verbotene Woerter: ${hits.length}`);

        // Die stille Anzeige hat hier nichts zu zaehlen und zeigt deshalb nichts.
        extras.exploredChipAtStart = (await checklistSeam(page)).chip;

        await page.screenshot({ path: join(OUT_DIR, 'why.png'), fullPage: true });
        log('why.png geschrieben (die Frage im leeren Editorbereich)');

        // ---------------------------------- 6b. Die Fuehrung durchs Projekt
        await page.click('[data-testid="atlas-why-card"][data-intent="understand"]');
        await page.waitForSelector('[data-testid="atlas-tour"]', { timeout: 40000 });
        await page.waitForFunction(() => (globalThis.__atlasTour?.steps ?? 0) > 0, undefined, {
            timeout: 40000,
        });

        const projectTour = await tourSeam(page);
        report.steps = projectTour.steps;
        extras.projectTour = projectTour;
        log(`Fuehrung: ${projectTour.steps} Schritte, ${projectTour.paths.join(' -> ')}`);

        // config.ts und types.ts muessen vor routes/ und server.ts kommen.
        const at = (path) => projectTour.paths.indexOf(path);
        const early = [at('src/config.ts'), at('src/types.ts')];
        const late = projectTour.paths
            .map((path, index) => ({ path, index }))
            .filter((entry) => entry.path.startsWith('src/routes/') || entry.path === 'src/server.ts')
            .map((entry) => entry.index);
        extras.order = { early, late };
        report.orderCorrect =
            early.every((index) => index >= 0)
            && late.length > 0
            && early.every((first) => late.every((second) => first < second));

        // Determinismus: dieselbe Erzeugung noch einmal, im selben Browser.
        const again = await page.evaluate(async () => {
            const seam = globalThis.__atlasTour;
            return seam?.regenerate === undefined ? null : await seam.regenerate();
        });
        const firstJson = await page.evaluate(() => globalThis.__atlasTour?.json ?? '');
        report.deterministic = again !== null && again === firstJson && firstJson.length > 0;
        extras.determinism = { bytes: firstJson.length, identical: report.deterministic };
        log(`Determinismus: ${report.deterministic} (${firstJson.length} Bytes)`);

        const beforeEnter = await readerSeam(page);
        extras.beforeEnter = beforeEnter;

        // Enter: ein Schritt weiter, der Reader zeigt eine andere Datei, der
        // Twin folgt.
        await pressGlobally(page, 'Enter');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 1, undefined, { timeout: 20000 });
        await page.waitForFunction(
            (previous) => (globalThis.__atlasReader?.document?.path ?? '') !== previous,
            beforeEnter.readerPath,
            { timeout: 30000 },
        );
        await page.waitForFunction(
            (previous) => (globalThis.__atlasTwin?.symbol ?? '') !== previous,
            beforeEnter.twin,
            { timeout: 30000 },
        );
        await page.waitForFunction(
            () => /config\.loadConfig$/.test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            undefined,
            { timeout: 40000 },
        );
        const afterEnter = await readerSeam(page);
        extras.afterEnter = afterEnter;
        report.twinFollowsStep =
            afterEnter.readerPath !== beforeEnter.readerPath
            && afterEnter.readerPath === 'src/config.ts'
            && afterEnter.twin !== beforeEnter.twin
            && /config\.loadConfig$/.test(afterEnter.twinQn);

        // Der betretene Schritt hat vermerkt, und die Statusleiste zeigt es.
        await page.waitForFunction(() => (globalThis.__atlasChecklist?.marks ?? 0) > 0, undefined, {
            timeout: 30000,
        });
        const marksAtStep2 = await checklistSeam(page);
        extras.marksAtStep2 = marksAtStep2;
        report.stepMarksVisited =
            marksAtStep2.seam !== null
            && marksAtStep2.seam.marks >= 1
            && /config\.loadConfig$/.test(marksAtStep2.seam.symbol)
            && marksAtStep2.chip !== null
            && /explored \d+ of \d+/.test(marksAtStep2.chip);

        await page.screenshot({ path: join(OUT_DIR, 'tour.png'), fullPage: true });
        log('tour.png geschrieben (Schrittkarte unter dem Reader)');

        // Noch ein Schritt: die Zahl der Vermerke muss steigen.
        await pressGlobally(page, 'Enter');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 2, undefined, { timeout: 20000 });
        await page.waitForFunction(
            () => /db\.query$/.test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            undefined,
            { timeout: 40000 },
        );
        await page.waitForFunction(
            (before) => (globalThis.__atlasChecklist?.marks ?? 0) > before,
            marksAtStep2.seam?.marks ?? 0,
            { timeout: 30000 },
        );
        const marksAtStep3 = await checklistSeam(page);
        extras.marksAtStep3 = marksAtStep3;
        report.exploredCounterRises =
            marksAtStep3.seam !== null
            && marksAtStep2.seam !== null
            && marksAtStep3.seam.marks > marksAtStep2.seam.marks
            && marksAtStep3.chip !== null
            && /explored \d+ of \d+/.test(marksAtStep3.chip);
        log(`Vermerke: ${marksAtStep2.seam?.marks} -> ${marksAtStep3.seam?.marks}, `
            + `Statusleiste "${marksAtStep2.chip}" -> "${marksAtStep3.chip}"`);

        // ArrowLeft: zurueck, ohne die Fuehrung zu verlassen.
        await pressGlobally(page, 'ArrowLeft');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 1, undefined, { timeout: 20000 });
        const afterBack = await tourSeam(page);
        report.playerKeyboardNavigates = afterBack !== null && afterBack.index === 1;

        // q: Schluss. Die Karte ist weg, die Datei bleibt offen.
        await pressGlobally(page, 'q');
        await page.waitForFunction(() => globalThis.__atlasTour === undefined, undefined, { timeout: 20000 });
        extras.afterQuit = {
            cardGone: (await page.$('[data-testid="atlas-tour"]')) === null,
            reader: await readerSeam(page),
        };
        log('Fuehrung mit q beendet');

        // -------------------------------- 6c. Der Vorwaerts-Walk ab createUser
        await openWhyAndChoose(page, 'entry');
        // Auf die Zusammenfassung warten, bevor die Liste gelesen wird: sonst
        // stuende hier "der Index hat keinen Einstiegspunkt gemeldet", und das
        // waere ein Befund ueber die Wartezeit dieses Laufs und nicht ueber den
        // Index.
        await page.waitForSelector('[data-testid="atlas-entry-row"]', { timeout: 60000 });
        extras.entryHeadline = await page.evaluate(
            () => document.querySelector('[data-testid="atlas-entry-headline"]')?.textContent?.trim() ?? '',
        );
        extras.entryRouteNote = await page.evaluate(
            () =>
                document.querySelector('[data-testid="atlas-entry-route-note"]')?.textContent?.trim()
                ?? null,
        );
        extras.entryOffered = await page.evaluate(() =>
            [...document.querySelectorAll('[data-testid="atlas-entry-row"]')].map((node) => ({
                name: node.getAttribute('data-name'),
                openable: node.getAttribute('data-openable'),
                text: (node.textContent ?? '').replace(/\s+/g, ' ').trim(),
            })),
        );
        await chooseEntryHit(page, ENTRY_SYMBOL);

        const entryTour = await tourSeam(page);
        extras.entryTour = entryTour;
        report.entrySteps = entryTour.steps;
        report.entryFirstStep = entryTour.titles[0] ?? '';
        report.entryPointTourStartsAtChosen =
            entryTour.index === 0
            && /createUser/.test(entryTour.titles[0] ?? '')
            && entryTour.paths[0] === ENTRY_FILE;

        const chosenAt = entryTour.titles.findIndex((title) => title.includes(ENTRY_SYMBOL));
        const prelude = entryTour.paths
            .slice(0, chosenAt < 0 ? entryTour.paths.length : chosenAt)
            .filter((path) => path === 'src/config.ts' || path === 'src/types.ts');
        report.entryHasNoConfigPrelude = chosenAt === 0 && prelude.length === 0;
        log(`Vorwaerts-Walk: ${entryTour.steps} Schritte ab "${report.entryFirstStep}"`);

        // ---------------------- Die stille Anzeige an EINEM Symbol, vorher/nachher
        /*
         * Der Zaehler der Statusleiste gilt fuer das Symbol vor dem Leser, also
         * kann er nur dann steigen, wenn dasselbe Symbol zweimal davorsteht und
         * dazwischen etwas vermerkt wurde. Genau das wird hier gefahren:
         * createUser steht da, eine seiner Zeilen wird verfolgt (das vermerkt
         * ein zweites Item AN createUser), und der Walk kommt ueber Enter und
         * ArrowLeft zu createUser zurueck. Die beiden Ablesungen sind dieselbe
         * Zahl ueber derselben Gesamtzahl.
         */
        await page.waitForFunction(
            () => /userService\.createUser$/.test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            undefined,
            { timeout: 40000 },
        );
        const stepRows = page.locator('[data-testid="codeatlas-twin-step"] button.atlas-twin-row-activate');
        await page.waitForFunction(
            () =>
                document.querySelectorAll(
                    '[data-testid="codeatlas-twin-step"] button.atlas-twin-row-activate',
                ).length >= 2,
            undefined,
            { timeout: 40000 },
        );
        const exploredBefore = (await checklistSeam(page)).chip;
        extras.followedRow = await stepRows.nth(1).innerText();
        await stepRows.nth(1).click();
        await page.waitForFunction(
            () => !/userService\.createUser$/.test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            undefined,
            { timeout: 30000 },
        );
        await pressGlobally(page, 'Enter');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 1, undefined, { timeout: 20000 });
        await pressGlobally(page, 'ArrowLeft');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 0, undefined, { timeout: 20000 });
        await page.waitForFunction(
            () => /userService\.createUser$/.test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            undefined,
            { timeout: 40000 },
        );
        const exploredAfter = (await checklistSeam(page)).chip;
        const readCount = (chip) => {
            const match = /explored (\d+) of (\d+)/.exec(chip ?? '');
            return match === null ? null : { visited: Number(match[1]), total: Number(match[2]) };
        };
        const before = readCount(exploredBefore);
        const after = readCount(exploredAfter);
        report.exploredBefore = exploredBefore;
        report.exploredAfter = exploredAfter;
        extras.sameSymbolCounter = { before, after };
        const sameSymbolRose =
            before !== null && after !== null && after.total === before.total && after.visited > before.visited;
        // Beide Lesarten zusammen: die Vermerke des Projekts steigen ueber die
        // Schritte hinweg, UND derselbe Zaehler ueber demselben Symbol steigt.
        report.exploredCounterRises = report.exploredCounterRises && sameSymbolRose;
        log(`stille Anzeige an einem Symbol: "${exploredBefore}" -> "${exploredAfter}"`);

        // Ein ungekappter Walk sagt am Ende nichts ueber einen Deckel.
        await walkToLastStep(page);
        const capAtDefault = await page.evaluate(
            () => document.querySelector('[data-testid="atlas-tour-cap"]')?.textContent?.trim() ?? null,
        );
        extras.defaultWalk = { endNote: entryTour.endNote, capShown: capAtDefault };

        // ------------------------------------------ 6d. Der Deckel-Beweis
        await page.goto(`${origin}/?project=${PROJECT}&codeatlasClosureCap=${SMALL_CAP}`, {
            waitUntil: 'load',
        });
        await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
        await openWhyAndChoose(page, 'entry');
        await chooseEntryHit(page, ENTRY_SYMBOL);
        const cappedTour = await tourSeam(page);
        await walkToLastStep(page);
        const capAtCapped = await page.evaluate(
            () => document.querySelector('[data-testid="atlas-tour-cap"]')?.textContent?.trim() ?? null,
        );
        extras.cappedWalk = { steps: cappedTour.steps, endNote: cappedTour.endNote, capShown: capAtCapped };
        log(`gekappter Walk: ${cappedTour.steps} Schritte, Satz: ${capAtCapped}`);

        const capSentence = new RegExp(`walk capped at ${SMALL_CAP} symbols \\(depth \\d+\\)`);
        // Beide Richtungen: ohne Deckel kein Satz, mit Deckel der Satz samt
        // seinen Zahlen. Nur zusammen ist das ein Beweis.
        report.closureTruncationHonest =
            capAtDefault === null
            && entryTour.endNote === ''
            && capAtCapped !== null
            && capSentence.test(capAtCapped)
            && cappedTour.steps === SMALL_CAP;

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w4a] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w4a] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
    report.entrySymbol = ENTRY_SYMBOL;
    report.timings = timings;
    report.extras = extras;
    report.generatedAt = new Date().toISOString();
    report.error = failure ? failure.message : null;

    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(OUT_JSON, JSON.stringify(report, null, 2) + '\n', 'utf8');
    log('geschrieben:', OUT_JSON);

    const ok =
        failure === null
        && report.whyShown === true
        && report.modesNeutral === true
        && report.steps >= 5
        && report.orderCorrect === true
        && report.deterministic === true
        && report.playerKeyboardNavigates === true
        && report.twinFollowsStep === true
        && report.stepMarksVisited === true
        && report.exploredCounterRises === true
        && report.entryPointTourStartsAtChosen === true
        && /createUser/.test(report.entryFirstStep)
        && report.entrySteps >= 3
        && report.entryHasNoConfigPrelude === true
        && report.closureTruncationHonest === true
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w4a] W4a-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w4a] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    if (home) {
        await rm(home, { recursive: true, force: true });
    }
    log('W4a-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w4a] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
