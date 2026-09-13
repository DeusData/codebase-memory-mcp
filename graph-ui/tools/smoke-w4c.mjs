#!/usr/bin/env node
/*
 * W4c-Smoke: der Flow-Erklaerer, der Stepper und die Pseudocode-Ansicht an
 * einem echten Server.
 *
 * Was hier bewiesen wird, koennen die Unit-Tests nicht beweisen. Sie zeigen an
 * aufgezeichneten Fixtures, dass der Builder keine Zeile erfindet und dass ein
 * Fehlerpfad keinen Pfeil bekommt. Sie sagen nichts darueber, ob der
 * flow()-Kopf im Browser wirklich ein fokussierbarer Knopf ist, ob ein Klick
 * darauf den Erklaerer schliesst und wieder oeffnet, ob er im dunklen
 * Token-Design steht (gemessen, nicht behauptet: die Luminanz der wirklich
 * berechneten Hintergrundfarbe), ob der Stepper Bild, STEPS-Liste UND Editor
 * bewegt, und ob die wieder eingebaute Imports-Gruppe an einer echten Datei
 * ueberhaupt einen Eintrag hat.
 *
 * ## Angepasst am 2026-08-29 (W5c), ohne eine Zusicherung aufzugeben
 *
 * Der Erklaerer ist seit W5c kein Kasten in der rechten Spalte mehr, sondern
 * ein Overlay ueber der Editorflaeche (Nutzerfeedback: in der Spalte war er
 * nicht zu lesen). Dieser Lauf misst weiterhin dieselben Felder mit derselben
 * Bedeutung an denselben Testmarken, die mit umgezogen sind: `atlas-flow` mit
 * seinen Zaehlern, `atlas-flow-box` als die Flaeche, deren Luminanz gemessen
 * wird, `atlas-flow-participant`, `atlas-flow-arrow` und `atlas-flow-raise`.
 * Drei Dinge sind anders, und alle drei sind der Umzug selbst:
 *
 *  - Der Erklaerer ist per Vorgabe ZU, weil eine Flaeche ueber dem Editor nicht
 *    ungefragt aufgehen darf. Der Lauf oeffnet ihn also erst und misst den Kopf
 *    danach, statt ihn im offenen Zustand vorzufinden.
 *  - Die Position steht als "1 of 11" statt "1 / 11", wortgleich mit dem
 *    Referenz-Explainer.
 *  - Escape schliesst das Overlay, statt nur die Stepper-Sitzung zu beenden.
 *    `escapeEndsSession` misst weiterhin, dass die Sitzung endet, und
 *    zusaetzlich, dass die Flaeche vom Editor verschwindet.
 *
 * Ablauf:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4280, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *   6a. createUser fokussieren, Erklaerer oeffnen, Kopf-Knopf pruefen und schalten
 *   6b. Stepper: Bild, STEPS-Zeile und Editor
 *   6c. Escape beendet die Sitzung
 *   6d. Pseudocode-Ansicht: Zeilen, Imports-Gruppe, Klick, ehrlicher Block
 *   6e. Der Data-Block des Twin traegt die Imports-Antwort wieder
 *   7. abraeumen, Restprozesse zaehlen, JSON und zwei Screenshots schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w4c).
 *
 * ## Zwei Entscheidungen, die man sonst raten muesste
 *
 * **Die Fixture wird nur gelesen.** Dieser Lauf aendert keine Datei: er braucht
 * keinen Diff, sondern nur einen Index. fixtures/atlas-sample bleibt
 * byte-identisch, und es wird nichts kopiert, was nicht kopiert werden muss.
 *
 * **CBM_RUNTIME_DIR wird gesetzt.** Der Daemon des Servers und jede CLI
 * verabreden sich in einem Rendezvous-Verzeichnis, das per Konto und nicht per
 * HOME gilt: laeuft irgendwo sonst auf der Maschine eine CBM-Instanz mit einem
 * anderen Cache-Verzeichnis, lehnt jede CLI dieses Laufs ab, und der Lauf waere
 * nicht rot, sondern kaputt. Wortgleich mit tools/smoke-w4b.mjs.
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
const PROJECT = 'codeatlasweb-w4c';
const OUT_DIR = join(ROOT, 'verification', 'w4');
const OUT_JSON = join(OUT_DIR, 'flow.json');
const MIN_PORT = 4280;

/** Das Symbol, ueber das der Erklaerer spricht. */
const TARGET = 'createUser';
const TARGET_FILE = 'src/services/userService.ts';

/** Chromium ohne Aussenwelt. Wortgleich mit smoke-w4b. */
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

const log = (...parts) => console.log('[smoke-w4c]', ...parts);
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
        child.stdin.end();
    });
}

/** Zum Symbol navigieren, ueber dieselbe Suche, die die Kommandozeile fuehrt. */
async function openTarget(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(TARGET, { delay: 40 });
    await page.waitForSelector(`[data-testid="atlas-search-row"][data-name="${TARGET}"]`, { timeout: 30000 });
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

/** Warten, bis das Bild wirklich gefuellt ist. Ein leeres Bild ist keine Antwort. */
async function waitForFlow(page) {
    await page.waitForSelector('[data-testid="atlas-flow"]', { timeout: 60000 });
    await page.waitForFunction(
        () => Number(document.querySelector('[data-testid="atlas-flow"]')?.getAttribute('data-arrows') ?? '0') > 0,
        undefined,
        { timeout: 60000 },
    );
}

/**
 * Den Erklaerer ueber den flow()-Kopf oeffnen.
 *
 * Seit W5c ist er per Vorgabe zu: eine Flaeche ueber dem Editor geht nicht
 * ungefragt auf. Der Kopf ist derselbe Knopf wie vorher, und dass er einer ist,
 * misst dieser Lauf gleich danach.
 */
async function openFlow(page) {
    await page.click('[data-testid="atlas-twin-subject"]');
    await waitForFlow(page);
}

/**
 * Alles, was der Kasten gerade zeigt, in einem Zug abgelesen.
 *
 * Die Hintergrundfarbe wird ueber getComputedStyle geholt und nicht aus der
 * Klasse geschlossen: gefragt ist, was der Browser wirklich malt, und die
 * Luminanz danach ist die Zahl, die der Abnahmetest liest.
 */
const flowSeam = (page) =>
    page.evaluate(() => {
        const box = document.querySelector('[data-testid="atlas-flow"]');
        const inner = document.querySelector('[data-testid="atlas-flow-box"]');
        const list = (id) => [...document.querySelectorAll(`[data-testid="${id}"]`)];
        const text = (id) =>
            document.querySelector(`[data-testid="${id}"]`)?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        const rgb = inner === null ? '' : globalThis.getComputedStyle(inner).backgroundColor;
        const parts = /rgba?\(([^)]+)\)/.exec(rgb);
        const channels = parts === null ? [] : parts[1].split(',').map((value) => Number(value.trim()));
        // Die uebliche Helligkeitsformel. Der Test verlangt < 0.2; weiss ist 1.
        const luminance = channels.length >= 3
            ? (0.299 * channels[0] + 0.587 * channels[1] + 0.114 * channels[2]) / 255
            : 1;
        return {
            present: box !== null,
            participants: list('atlas-flow-participant').map((node) => node.getAttribute('data-label')),
            arrows: list('atlas-flow-arrow').map((node) => ({
                index: Number(node.getAttribute('data-index') ?? '-1'),
                to: node.getAttribute('data-to'),
                message: node.getAttribute('data-message'),
                current: node.getAttribute('data-current') === 'true',
            })),
            raises: list('atlas-flow-raise').map((node) => node.textContent?.trim() ?? ''),
            steps: Number(box?.getAttribute('data-steps') ?? '0'),
            activeStep: Number(box?.getAttribute('data-active-step') ?? '-1'),
            activeArrow: Number(box?.getAttribute('data-active-arrow') ?? '-1'),
            position: text('atlas-flow-position'),
            current: text('atlas-flow-current'),
            backgroundColor: rgb,
            luminance,
            markedStepRows: [...document.querySelectorAll('[data-testid="codeatlas-twin-step"]')]
                .map((node, index) => (node.getAttribute('data-current') === 'true' ? index : -1))
                .filter((index) => index >= 0),
        };
    });

/** Der Kopf des Panels, als das, was er im DOM wirklich ist. */
const headSeam = (page) =>
    page.evaluate(() => {
        const head = document.querySelector('[data-testid="atlas-twin-subject"]');
        if (head === null) {
            return { tag: '', role: '', expanded: '', focusable: false, cursor: '' };
        }
        head.focus();
        return {
            tag: head.tagName,
            role: head.getAttribute('role') ?? (head.tagName === 'BUTTON' ? 'button' : ''),
            expanded: head.getAttribute('aria-expanded') ?? '',
            focusable: document.activeElement === head,
            cursor: globalThis.getComputedStyle(head).cursor,
            disabled: head.disabled === true,
        };
    });

/** Alles, was die Pseudocode-Ansicht gerade zeigt. */
const pseudocodeSeam = (page) =>
    page.evaluate(() => {
        const panel = document.querySelector('[data-testid="atlas-pseudocode"]');
        const list = (id) => [...document.querySelectorAll(`[data-testid="${id}"]`)];
        const text = (id) =>
            document.querySelector(`[data-testid="${id}"]`)?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        return {
            present: panel !== null,
            title: text('atlas-pseudocode-title'),
            lines: list('atlas-pseudocode-line').map((node) => ({
                order: Number(node.getAttribute('data-order') ?? '0'),
                kind: node.getAttribute('data-kind'),
                alarm: node.getAttribute('data-alarm') === 'true',
                text: node.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            })),
            groups: list('atlas-pseudocode-group').map((node) => node.textContent?.trim() ?? ''),
            imports: list('atlas-pseudocode-import').map((node) => ({
                usage: node.getAttribute('data-usage'),
                text: node.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            })),
            tally: text('atlas-pseudocode-tally'),
            honest: text('atlas-pseudocode-honest'),
            /*
             * Seit W8c stehen die Saetze ueber den Block selbst nicht mehr als
             * Absaetze unter ihm, sondern hinter dem Fragezeichen daneben
             * (dasselbe Idiom wie am Diagramm seit W8b). Der Griff liest sie
             * dort, wortgleich: `data-hint` traegt den Text, den der Tooltip
             * zeigt. Was dieser Lauf zusichert, ist unveraendert, dass es sie
             * gibt und dass sie erreichbar sind.
             */
            provenance: document.querySelector('[data-testid="atlas-pseudocode-provenance"]')
                ?.getAttribute('data-hint') ?? '',
        };
    });

/** Der Data-Block des Twin, soweit er die Import-Antwort betrifft. */
const twinImportsSeam = (page) =>
    page.evaluate(() => {
        const section = document.querySelector('[data-testid="codeatlas-twin-section-imports"]');
        return {
            present: section !== null,
            block: section?.getAttribute('data-block') ?? '',
            title: section?.querySelector('.atlas-twin-section-title')?.textContent?.trim() ?? '',
            text: section?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            rows: section === null ? 0 : section.querySelectorAll('[data-testid="codeatlas-twin-row"]').length,
        };
    });

/** Wo der Editor gerade steht: Datei und angeleuchtete Zeile. */
const readerSeam = (page) =>
    page.evaluate(() => ({
        path: globalThis.__atlasReader?.document?.path ?? '',
        line: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
    }));

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

    const flow = {
        flowHeadClickable: false,
        flowTogglesOnHeadClick: false,
        flowDarkStyled: false,
        flowParticipants: 0,
        flowSteps: 0,
        flowStepperStops: 0,
        stepperMovesEditor: false,
        stepperMovesDiagram: false,
        stepsListSync: false,
        mayRaiseShown: false,
        escapeEndsSession: false,
        pseudocodeLines: 0,
        pseudocodeHasImportsGroup: false,
        pseudocodeLineClickNavigates: false,
        twinImportsGroupShown: false,
        honestBlockShown: false,
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
            throw new Error(`npm run build endete mit ${build.code}: ${build.err.trim().slice(-600)}`);
        }
        if (!existsSync(join(DIST, 'index.html'))) {
            throw new Error('dist/index.html fehlt nach dem Build');
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w4c-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w4c-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);
        log('Rendezvous:', runtimeDir);

        // -------------------------------------------------------- 3. Index
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // -------------------------------------------------------- 4. Server
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        timings.serverStartMs = started.durationMs;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        flow.port = uiPort;
        extras.serverPort = serverPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

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
        await mkdir(OUT_DIR, { recursive: true });

        // ---------------------------- 6a. Der Erklaerer und sein Kopf-Knopf
        await openTarget(page, origin);
        await openFlow(page);
        const opened = await flowSeam(page);
        const head = await headSeam(page);
        extras.head = head;
        extras.opened = opened;
        flow.flowHeadClickable =
            head.tag === 'BUTTON'
            && head.focusable === true
            && head.disabled === false
            && head.expanded === 'true'
            && head.cursor === 'pointer';
        flow.flowParticipants = opened.participants.length;
        flow.flowSteps = opened.arrows.length;
        flow.flowStepperStops = opened.steps;
        flow.flowDarkStyled = opened.luminance < 0.2;
        flow.mayRaiseShown = opened.raises.some((line) => line.includes('may raise'));
        log(`Bild: ${flow.flowParticipants} Spalten, ${flow.flowSteps} Pfeile, `
            + `${flow.flowStepperStops} Halte, Hintergrund ${opened.backgroundColor} `
            + `(Luminanz ${opened.luminance.toFixed(3)})`);

        // Der Kopf schaltet: zu, und wieder auf.
        await page.click('[data-testid="atlas-twin-subject"]');
        await page.waitForSelector('[data-testid="atlas-flow"]', { state: 'detached', timeout: 10000 });
        const closed = await page.evaluate(
            () => document.querySelector('[data-testid="atlas-twin-subject"]')?.getAttribute('aria-expanded') ?? '',
        );
        await openFlow(page);
        const reopened = await flowSeam(page);
        flow.flowTogglesOnHeadClick =
            closed === 'false' && reopened.present === true && reopened.arrows.length === opened.arrows.length;
        extras.toggle = { closedExpanded: closed, reopenedArrows: reopened.arrows.length };
        log(`Kopf-Klick: zu (aria-expanded=${closed}), wieder auf mit ${reopened.arrows.length} Pfeilen`);

        // -------------------------------------------------- 6b. Der Stepper
        const beforeStep = await readerSeam(page);
        await page.click('[data-testid="atlas-flow-next"]');
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-flow"]')?.getAttribute('data-active-step') === '0',
            undefined,
            { timeout: 10000 },
        );
        const first = await flowSeam(page);
        const afterFirst = await readerSeam(page);
        await page.click('[data-testid="atlas-flow-next"]');
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-flow"]')?.getAttribute('data-active-step') === '1',
            undefined,
            { timeout: 10000 },
        );
        const second = await flowSeam(page);
        const afterSecond = await readerSeam(page);
        extras.stepper = { beforeStep, first, afterFirst, second, afterSecond };
        flow.stepperMovesDiagram =
            first.activeArrow >= 0
            && second.activeArrow >= 0
            && first.activeArrow !== second.activeArrow
            && first.arrows.some((arrow) => arrow.current && arrow.index === first.activeArrow)
            && second.arrows.some((arrow) => arrow.current && arrow.index === second.activeArrow);
        flow.stepperMovesEditor =
            afterFirst.line > 0
            && afterSecond.line > 0
            && (afterFirst.line !== afterSecond.line || afterFirst.path !== afterSecond.path);
        flow.stepsListSync =
            first.markedStepRows.length === 1
            && second.markedStepRows.length === 1
            && first.markedStepRows[0] !== second.markedStepRows[0]
            && first.position === `1 of ${first.steps}`
            && second.position === `2 of ${second.steps}`;
        log(`Stepper: Pfeil ${first.activeArrow} -> ${second.activeArrow}, `
            + `Editor ${afterFirst.path}:${afterFirst.line} -> ${afterSecond.path}:${afterSecond.line}, `
            + `STEPS-Zeile ${first.markedStepRows} -> ${second.markedStepRows}`);

        await page.screenshot({ path: join(OUT_DIR, 'flow.png'), fullPage: true });
        log('flow.png geschrieben');

        // ------------------------------------------ 6c. Escape beendet sie
        //
        // Seit W5c schliesst Escape das Overlay. Die Zusicherung bleibt
        // dieselbe und wird strenger: die Sitzung endet UND die Flaeche gibt
        // den Editor wieder frei.
        await page.keyboard.press('Escape');
        await page.waitForSelector('[data-testid="atlas-flow"]', { state: 'detached', timeout: 10000 })
            .catch(() => undefined);
        const afterEscape = await flowSeam(page);
        extras.afterEscape = {
            present: afterEscape.present,
            activeStep: afterEscape.activeStep,
            position: afterEscape.position,
        };
        flow.escapeEndsSession =
            afterEscape.present === false
            && afterEscape.activeStep === -1
            && afterEscape.activeArrow === -1;
        log(`Escape: Erklaerer noch da=${afterEscape.present}, aktiver Schritt ${afterEscape.activeStep}`);

        // ------------------------------------- 6d. Die Pseudocode-Ansicht
        await page.click('[data-testid="atlas-pseudocode-toggle"]');
        await page.waitForSelector('[data-testid="atlas-pseudocode"]', { timeout: 20000 });
        await page.waitForFunction(
            () => Number(document.querySelector('[data-testid="atlas-pseudocode"]')?.getAttribute('data-imports') ?? '0') > 0,
            undefined,
            { timeout: 30000 },
        ).catch(() => undefined);
        const block = await pseudocodeSeam(page);
        extras.pseudocode = block;
        flow.pseudocodeLines = block.lines.length;
        flow.pseudocodeHasImportsGroup =
            block.groups.some((heading) => /pulls in/i.test(heading)) && block.imports.length >= 1;
        // Beide Orte zaehlen: auf der Flaeche stehen sie seit W8c nicht mehr,
        // hinter dem Fragezeichen stehen sie wortgleich weiter.
        const honestEverywhere = `${block.honest} ${block.provenance}`;
        flow.honestBlockShown =
            honestEverywhere.includes('in scope contributed steps')
            && honestEverywhere.includes('Derived from the index and nothing else');
        log(`Pseudocode: ${flow.pseudocodeLines} Zeilen, ${block.imports.length} Importe, `
            + `Gruppen ${JSON.stringify(block.groups)}`);

        // Fuer das Bild ist der Erklaerer bereits zu (Escape oben): er hat sein
        // eigenes Beweisbild, und zwei halbe Flaechen uebereinander waeren ein
        // Bild, auf dem man beides nicht liest.
        await page.evaluate(() => {
            document.querySelector('[data-testid="atlas-pseudocode"]')
                ?.scrollIntoView({ block: 'start' });
        });
        await page.waitForTimeout(200);
        await page.screenshot({ path: join(OUT_DIR, 'pseudocode.png'), fullPage: true });
        log('pseudocode.png geschrieben');

        // Eine nummerierte Zeile fuehrt an ihren Ort. Gemessen an der Zeile,
        // die der Editor danach anleuchtet, und ausdruecklich an der Raise-
        // Zeile: die zeigt in die Datei des Aufgerufenen, also beweist ihr
        // Klick beides auf einmal, den Sprung und die richtige Datei.
        //
        // Das Bild wird VORHER gemacht: dieser Klick verlaesst die Datei, und
        // ein Screenshot danach zeigte den Block eines anderen Symbols.
        const raiseLine = block.lines.findIndex((line) => line.kind === 'raise');
        const clickAt = raiseLine >= 0 ? raiseLine : 0;
        const beforeClick = await readerSeam(page);
        await page.click(`[data-testid="atlas-pseudocode-line"] >> nth=${clickAt} >> button`);
        await page.waitForTimeout(1200);
        const afterClick = await readerSeam(page);
        extras.lineClick = { clickAt, kind: block.lines[clickAt]?.kind, beforeClick, afterClick };
        flow.pseudocodeLineClickNavigates =
            afterClick.line > 0 && (afterClick.line !== beforeClick.line || afterClick.path !== beforeClick.path);
        log(`Zeilen-Klick (${block.lines[clickAt]?.kind}): `
            + `${beforeClick.path}:${beforeClick.line} -> ${afterClick.path}:${afterClick.line}`);

        // ------------------------------ 6e. Der Data-Block des Twin wieder
        //
        // Erst zurueck zu createUser: der Klick oben ist in validate.ts gelandet,
        // und der Twin beschreibt seitdem, was dort steht. Die Import-Antwort
        // dieser anderen Datei waere eine richtige Antwort auf eine andere
        // Frage.
        await openTarget(page, origin);
        await page.waitForSelector('[data-testid="codeatlas-twin-section-imports"]', { timeout: 20000 });
        await page.waitForFunction(
            () => (document.querySelector('[data-testid="codeatlas-twin-section-imports"]')
                ?.querySelectorAll('[data-testid="codeatlas-twin-row"]').length ?? 0) > 0,
            undefined,
            { timeout: 30000 },
        ).catch(() => undefined);
        const twinImports = await twinImportsSeam(page);
        extras.twinImports = twinImports;
        flow.twinImportsGroupShown =
            twinImports.present
            && twinImports.block === 'data'
            && twinImports.rows >= 1
            && /used by this symbol/.test(twinImports.text);
        log(`Twin-Data-Block: "${twinImports.title}" mit ${twinImports.rows} Zeilen`);

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w4c] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w4c] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
    flow.leftoverProcesses = leftovers.reduce((sum, value) => sum + value, 0);
    log('leftoverProcesses:', flow.leftoverProcesses);

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...flow,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            target: TARGET,
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const ok =
        failure === null
        && flow.flowHeadClickable === true
        && flow.flowTogglesOnHeadClick === true
        && flow.flowDarkStyled === true
        && flow.flowParticipants >= 3
        && flow.flowSteps === 6
        && flow.stepperMovesEditor === true
        && flow.stepperMovesDiagram === true
        && flow.stepsListSync === true
        && flow.mayRaiseShown === true
        && flow.escapeEndsSession === true
        && flow.pseudocodeLines >= 6
        && flow.pseudocodeHasImportsGroup === true
        && flow.pseudocodeLineClickNavigates === true
        && flow.twinImportsGroupShown === true
        && flow.honestBlockShown === true
        && flow.port >= MIN_PORT
        && flow.leftoverProcesses === 0
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w4c] W4c-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w4c] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W4c-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w4c] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
