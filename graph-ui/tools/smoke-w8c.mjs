#!/usr/bin/env node
/*
 * W8c-Smoke: der Pseudocode-Block fuehrt mit dem, was der Code nicht zeigt.
 *
 * ## Warum dieser Lauf existiert
 *
 * Nutzerfrage vom 2026-08-29 zum Screenshot mit orderService.ts: "ist der
 * Pseudocode AI generated oder mechanisch, und ist er hilfreich?" Die Herkunft
 * war schnell belegt (mechanisch: im Bild steht LOCAL_MODEL off, und
 * src/pseudocode/ kennt das Modell nur in refine.ts, das der Leser selbst
 * anstoesst). Die zweite Haelfte der Frage war die teure: neben einer
 * 18-Zeilen-Funktion standen zwei Zeilen, "1. call validateId" und "2. call
 * query", waehrend der Code daneben try/catch, `rows.find`, den fruehen
 * Ausstieg, den Objektbau und ein `console.error` zeigte. Der Block war eine
 * Zusammenfassung, die weniger enthielt als das Original.
 *
 * Also misst dieser Lauf vier Dinge an einem echten Server, und keines davon
 * kann ein Unit-Test messen:
 *
 *  1. **Der Block fuehrt mit dem Fund.** Gemessen an der Lage der Kaesten im
 *     gerenderten Baum (`getBoundingClientRect`), nicht an der Reihenfolge im
 *     Quelltext: was oben steht, steht oben, auch wenn CSS etwas anderes
 *     wollte. Dazu die Gegenprobe zur Laengenschwelle: derselbe Aufbau bei
 *     einer Funktion mit zwei Schritten wie bei einer mit sechs, und in beiden
 *     Faellen ist jede Zeile des Dokuments auch im DOM.
 *  2. **Jeder Schritt traegt sein Ziel.** Gezaehlt wird nicht "hat einen
 *     Ortstext", sondern "ist ein Knopf, dessen Klick den Reader an genau die
 *     Datei und Zeile des AUFGERUFENEN Symbols bringt". Und wo der Index keine
 *     Stelle kennt, steht das an der Zeile.
 *  3. **Was hinter dem Aufruf liegt, kostet keine Anfrage.** Die Zaehler des
 *     Proxys werden vor dem Umschalten auf den Block und danach gelesen; die
 *     Differenz muss null sein. Was dabei WIRKLICH vorlag, steht als
 *     `enrichmentAvailable.usable` im Artefakt, und was nicht vorliegt, als
 *     `missing` samt Grund.
 *  4. **Kein Meta-Rauschen mehr.** Die Zeichen ueber den Block selbst werden
 *     an der Flaeche gemessen (was sichtbar dasteht) und die verschobenen
 *     Saetze am Griff des Panels. Beide Zahlen stehen im Artefakt, damit die
 *     Kuerzung nachrechenbar ist statt behauptet.
 *
 * ## Was hier gemessen und nicht behauptet wird
 *
 * `metaCharsBefore` ist die Summe der Saetze ueber den Block selbst, die bis
 * W8c auf der Flaeche standen: die Deckungszeile, die Namen ohne Deckung und
 * die Herkunftsnotiz. Sie sind unveraendert dieselben Zeichenketten; W8c hat
 * sie hinter das Fragezeichen geraeumt und keinen Buchstaben an ihnen
 * geaendert. Der Lauf liest ihre Laenge am Griff des laufenden Panels
 * (`metaMovedChars`) und die sichtbare Laenge am gerenderten Kasten, und
 * rechnet daraus beide Werte. So ist der Ausgangswert eine Messung an dem, was
 * das Produkt heute wirklich haelt, und keine Erinnerung an einen alten Stand.
 *
 * ## Ablauf
 *
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4560, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *      a. getOrder: der kurze Fall aus dem Nutzerbild
 *      b. der Block: Reihenfolge, Kopf, Ziele, was dahinter liegt, Meta
 *      c. ein Ziel anklicken: der Reader steht auf dem Aufgerufenen
 *      d. createUser: der lange Fall, derselbe Aufbau
 *   6. abraeumen, Restprozesse zaehlen, JSON und zwei Bilder im Ruhezustand
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w8c).
 *
 * ## Ports
 *
 * Ab 4560. 4390 und 4391 gehoeren der Vorschau des Nutzers, 4141 seinem
 * Modell-Sidecar, 4360 und 4400 den Eval-Laeufen, 4440, 4460 und 4540 den
 * Laeufen von W8, W9 und W8b. Dieser Lauf fasst keinen davon an.
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
import {
    READABILITY_EXCLUSIONS,
    closeTooltips,
    measureReadability,
    resetScroll,
    scrollRegionsToEnd,
} from './lib/readability.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w8c';
const OUT_DIR = join(ROOT, 'verification', 'w8c');
const OUT_JSON = join(OUT_DIR, 'pseudocode.json');
const SHOT_SHORT = join(OUT_DIR, 'pseudocode-short.png');
const SHOT_LONG = join(OUT_DIR, 'pseudocode-long.png');

/** Contract AC7. Alles darunter gehoert dem Nutzer oder frueheren Laeufen. */
const MIN_PORT = 4560;

const VIEWPORT = { width: 1680, height: 1050 };

/**
 * Die zwei Faelle, und warum genau diese zwei.
 *
 * `getOrder` ist die Funktion aus dem Nutzerbild: achtzehn Zeilen Code, zwei
 * aufgeloeste Aufrufe, ein Import, den der Index diesem Symbol nicht zuordnen
 * kann. `createUser` ist der Gegenfall mit sechs Aufrufen, einer erhobenen
 * Fehlerart und einer Umgebungslesung. Wenn beide denselben Aufbau zeigen, gibt
 * es keine Laenge, ab der etwas verschwindet.
 */
const SHORT = { name: 'getOrder', file: 'src/services/orderService.ts' };
const LONG = { name: 'createUser', file: 'src/services/userService.ts' };

/** Chromium ohne Aussenwelt, wortgleich mit smoke-w8b. */
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

const log = (...parts) => console.log('[smoke-w8c]', ...parts);
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
        child.stdout.on('data', (d) => { out += d.toString(); });
        child.stderr.on('data', (d) => { out += d.toString(); });
        child.on('error', (error) => done({ code: 127, out: out + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out }));
    });
}

// ------------------------------------------------------------- Testgriffe ---

/**
 * Der Block, wie er wirklich dasteht.
 *
 * Gelesen wird der gerenderte Baum und nicht der Griff: der Griff sagt, was das
 * Dokument haelt, und dieser Lauf muss sagen, was ein Leser sieht. Beide
 * stehen im Artefakt, und wo sie sich unterscheiden, ist das ein Befund.
 */
const blockSeam = (page) =>
    page.evaluate(() => {
        const block = document.querySelector('[data-testid="atlas-pseudocode"]');
        if (block === null) {
            return null;
        }
        const clean = (node) => (node?.textContent ?? '').replace(/\s+/g, ' ').trim();
        const top = (selector) => {
            const node = block.querySelector(selector);
            return node === null ? -1 : Math.round(node.getBoundingClientRect().top);
        };
        const lines = [...block.querySelectorAll('[data-testid="atlas-pseudocode-line"]')];
        const honest = block.querySelector('[data-testid="atlas-pseudocode-honest"]');
        const provenance = block.querySelector('[data-testid="atlas-pseudocode-provenance"]');
        const honestText = clean(honest);
        const provenanceText = clean(provenance);
        return {
            lead: {
                text: clean(block.querySelector('[data-testid="atlas-pseudocode-lead"]')),
                kind: block.querySelector('[data-testid="atlas-pseudocode-lead"]')
                    ?.getAttribute('data-kind') ?? '',
                top: top('[data-testid="atlas-pseudocode-lead"]'),
            },
            imports: {
                heading: clean(block.querySelector('[data-testid="atlas-pseudocode-group"]')),
                top: top('[data-testid="atlas-pseudocode-imports"]'),
                entries: [...block.querySelectorAll('[data-testid="atlas-pseudocode-import"]')].map((node) => ({
                    usage: node.getAttribute('data-usage') ?? '',
                    finding: node.getAttribute('data-finding') === 'true',
                    text: clean(node),
                    /* Der Fund muss sich auch SEHEN lassen, nicht nur heissen. */
                    accent: window.getComputedStyle(node).borderLeftWidth,
                })),
                tally: clean(block.querySelector('[data-testid="atlas-pseudocode-tally"]')),
            },
            stepsHead: {
                text: clean(block.querySelector('[data-testid="atlas-pseudocode-steps-head"]')),
                top: top('[data-testid="atlas-pseudocode-steps-head"]'),
            },
            firstLineTop: lines.length === 0
                ? -1
                : Math.round(lines[0].getBoundingClientRect().top),
            lines: lines.map((node) => {
                const place = node.querySelector('[data-testid="atlas-pseudocode-target"]');
                return {
                    kind: node.getAttribute('data-kind') ?? '',
                    order: Number(node.getAttribute('data-order') ?? '0'),
                    text: clean(node.querySelector('.atlas-pseudocode-line-btn')),
                    target: clean(place),
                    targetKnown: place?.getAttribute('data-known') === 'true',
                    targetClickable: place?.tagName === 'BUTTON',
                    behind: [...node.querySelectorAll('[data-testid="atlas-pseudocode-behind"]')]
                        .map((mark) => ({ kind: mark.getAttribute('data-kind') ?? '', text: clean(mark) })),
                };
            }),
            honest: {
                text: honestText,
                /* Ohne das Fragezeichen selbst: ein Zeichen ist kein Satz. */
                chars: honestText.replace(provenanceText, '').trim().length,
                provenanceHint: provenance?.getAttribute('data-hint') ?? '',
                moved: Number(provenance?.getAttribute('data-moved') ?? '-1'),
            },
            seam: JSON.parse(JSON.stringify(globalThis.__atlasTwin?.pseudocode ?? null)),
        };
    });

/** Wo der Reader steht: Datei und Zeile. */
const readerSeam = (page) =>
    page.evaluate(() => ({
        path: globalThis.__atlasReader?.document?.path ?? '',
        line: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? 0,
    }));

/** Wo jeder scrollbare Bereich steht. Wortgleich mit smoke-w8 und smoke-w8b. */
const scrollState = (page) =>
    page.evaluate(() => {
        const regions = [];
        for (const node of document.body.querySelectorAll('*')) {
            if (node.closest('.monaco-editor') !== null) {
                continue;
            }
            const style = globalThis.getComputedStyle(node);
            const scrollsY = (style.overflowY === 'auto' || style.overflowY === 'scroll')
                && node.scrollHeight > node.clientHeight + 1;
            const scrollsX = (style.overflowX === 'auto' || style.overflowX === 'scroll')
                && node.scrollWidth > node.clientWidth + 1;
            if (!scrollsY && !scrollsX) {
                continue;
            }
            regions.push({
                name: node.getAttribute('data-testid')
                    ?? (node.getAttribute('class') ?? '').split(/\s+/).filter(Boolean)[0]
                    ?? node.tagName.toLowerCase(),
                top: Math.round(node.scrollTop),
                left: Math.round(node.scrollLeft),
                hidden: Math.round(node.scrollHeight - node.clientHeight),
            });
        }
        return { regions, atRest: regions.every((region) => region.top <= 1 && region.left <= 1) };
    });

/** Ein Beweisbild im Ruhezustand. Wortgleich mit smoke-w8b. */
async function shootAtRest(page, file, name) {
    await closeTooltips(page);
    await resetScroll(page, READABILITY_EXCLUSIONS);
    await page.waitForTimeout(350);
    const state = await scrollState(page);
    await page.screenshot({ path: file, fullPage: false });
    log(`${name}: aufgenommen im Ruhezustand=${state.atRest}`);
    return { name, atRest: state.atRest, regions: state.regions };
}

/** Zu einem Symbol navigieren, ueber dieselbe Suche, die die Kommandozeile fuehrt. */
async function openSymbol(page, target) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(target.name, { delay: 40 });
    await page.waitForSelector(
        `[data-testid="atlas-search-row"][data-name="${target.name}"]`,
        { timeout: 30000 },
    );
    await page.waitForTimeout(600);
    await page.click(`[data-testid="atlas-search-row"][data-name="${target.name}"]`);
    await page.waitForFunction(
        (expected) => globalThis.__atlasReader?.document?.path === expected,
        target.file,
        { timeout: 40000 },
    );
    await page.waitForFunction(
        (expected) => (globalThis.__atlasTwin?.symbol ?? '') === expected,
        target.name,
        { timeout: 40000 },
    );
    // Der Block wird aus der IR gebaut, und die kommt nach dem Subjekt.
    await page.waitForFunction(
        () => (globalThis.__atlasTwin?.pseudocode?.lines ?? 0) > 0,
        undefined,
        { timeout: 40000 },
    );
    await page.waitForTimeout(500);
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
        // AC1
        findingFirst: false,
        headSaysWhatTheFindingIs: false,
        noLengthThreshold: false,
        // AC2
        stepTargetsClickable: false,
        stepClickOpensReader: false,
        stepsWithoutTargetExplained: false,
        // AC3
        enrichmentAvailable: { usable: [], silent: [], missing: [] },
        enrichedSteps: 0,
        noExtraServerRequest: false,
        // AC4
        importFindingProminent: false,
        importHonestyWordsKept: false,
        // AC5
        metaCharsBefore: 0,
        metaCharsAfter: 0,
        // AC6
        modelRequestsWhileOff: 0,
        refineStillGated: false,
        // AC7
        overlapViolations: 0,
        clippingViolations: 0,
        cutWithoutHint: 0,
        screenshotsAtRest: false,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = {
        blockedRequests: [],
        consoleErrors: [],
        pageErrors: [],
        readability: [],
        blocks: {},
        shots: [],
        requestsAroundTab: null,
        refineProbe: null,
        llm: null,
    };

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
            throw new Error(`npm run build endete mit ${build.code}: ${build.out.trim().slice(-600)}`);
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w8c-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w8c-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);

        // -------------------------------------------------------- 3. Index
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // ------------------------------------------------- 4. Server, Proxy
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        extras.serverPort = serverPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        // ------------------------------------------------------- 5. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const context = await browser.newContext({ viewport: { ...VIEWPORT } });
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

        /** Lesbarkeit an diesem Halt, oben und unten. Wortgleich mit smoke-w8b. */
        const readability = async (name) => {
            await closeTooltips(page);
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
            report.cutWithoutHint += [...top.clipped, ...bottom.clipped]
                .filter((entry) => entry.kind === 'cut-without-hint').length;
        };

        await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
        await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 30000 });
        /*
         * Auf das geladene Layout warten, und zwar ausdruecklich.
         *
         * Es ist die Quelle, aus der der Block liest, was hinter einem Aufruf
         * liegt. Ein Lauf, der vor seiner Ankunft misst, wuerde "nichts
         * verfuegbar" aufschreiben und damit eine Verzoegerung als Befund
         * ausgeben.
         */
        await page.waitForFunction(
            () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
            undefined,
            { timeout: 60000 },
        );
        await page.waitForTimeout(900);

        // ------------------------------------------- 5a. Der kurze Fall
        await openSymbol(page, SHORT);

        /*
         * Die Zaehler vor und nach dem Umschalten (AC3, letzter Halbsatz).
         *
         * Das Umschalten auf den Block ist die Handlung, die nichts kosten
         * darf: die IR steht, die Importe stehen, das Layout steht. Kommt hier
         * eine Anfrage dazu, waere der Block ein zweiter Serverweg geworden.
         */
        const before = {
            api: { ...proxy.log.apiRoutes },
            rpc: { ...proxy.log.rpcTools },
        };
        await page.click('[data-testid="atlas-pseudocode-toggle"]');
        await page.waitForSelector('[data-testid="atlas-pseudocode"]', { timeout: 20000 });
        await page.waitForTimeout(900);
        const after = {
            api: { ...proxy.log.apiRoutes },
            rpc: { ...proxy.log.rpcTools },
        };
        const grew = (left, right) => Object.keys(right)
            .filter((key) => (right[key] ?? 0) > (left[key] ?? 0))
            .map((key) => ({ key, before: left[key] ?? 0, after: right[key] }));
        const newApi = grew(before.api, after.api);
        const newRpc = grew(before.rpc, after.rpc);
        extras.requestsAroundTab = { before, after, newApi, newRpc };
        report.noExtraServerRequest = newApi.length === 0 && newRpc.length === 0;
        log(`Umschalten auf den Block: ${newApi.length} neue Routen, ${newRpc.length} neue Werkzeuge`);

        const short = await blockSeam(page);
        extras.blocks.short = short;
        if (short === null) {
            throw new Error('der Pseudocode-Block steht nicht da');
        }

        // AC1: die Lage im Bild, nicht die Reihenfolge im Quelltext.
        report.findingFirst = short.lead.top >= 0
            && short.imports.top > short.lead.top
            && short.firstLineTop > short.imports.top
            && short.stepsHead.top > short.imports.top
            && short.stepsHead.top < short.firstLineTop;
        report.headSaysWhatTheFindingIs = short.lead.text.length > 0
            && short.lead.kind === 'unused-imports'
            && short.lead.text.includes('as far as the index shows');
        log(`Reihenfolge: Kopf ${short.lead.top}, Importe ${short.imports.top}, `
            + `Schritte ${short.firstLineTop} -> Fund zuerst ${report.findingFirst}`);
        log(`Kopf: "${short.lead.text}"`);

        // AC2: jede Schrittzeile traegt ihr Ziel, und die ohne Ziel sagt es.
        const steps = short.lines.filter((line) => line.kind === 'step');
        report.stepTargetsClickable = steps.length > 0
            && steps.every((line) => line.target.length > 0)
            && steps.filter((line) => line.targetKnown).every((line) => line.targetClickable);
        report.stepsWithoutTargetExplained = steps
            .filter((line) => !line.targetKnown)
            .every((line) => line.target.includes('the index records no place'));
        log(`Schritte: ${steps.length}, mit Ort ${steps.filter((l) => l.targetKnown).length}, `
            + `anklickbar ${report.stepTargetsClickable}`);

        // AC4: der Fund traegt seinen Rang und seine Grenze.
        report.importFindingProminent = short.imports.entries.some((entry) => entry.finding)
            && short.imports.heading.includes('not used by this symbol')
            && short.imports.entries
                .filter((entry) => entry.finding)
                .every((entry) => Number.parseFloat(entry.accent) > 0)
            && short.imports.top < short.firstLineTop;
        report.importHonestyWordsKept = short.imports.entries
            .some((entry) => entry.text.includes('not used by this symbol as far as the index shows'))
            && short.imports.tally.includes('that CodeAtlas cannot check')
            && short.imports.heading.includes('as far as the index shows');
        log(`Importgruppe: "${short.imports.heading}" / "${short.imports.tally}"`);

        // AC5: die Meta-Zeichen, vorher und nachher, beide gemessen.
        report.metaCharsAfter = short.honest.chars;
        report.metaCharsBefore = short.honest.chars + short.honest.moved;
        log(`Meta: sichtbar ${report.metaCharsAfter} Zeichen, verschoben ${short.honest.moved}, `
            + `Ausgangswert ${report.metaCharsBefore}`);

        // AC3: was wirklich vorlag, und was nicht.
        report.enrichmentAvailable = short.seam?.enrichment ?? { usable: [], silent: [], missing: [] };
        report.enrichedSteps = short.lines.filter((line) => line.behind.length > 0).length;
        log(`Anreicherung: ${report.enrichmentAvailable.usable.length} Sorten nutzbar, `
            + `${report.enrichmentAvailable.missing.length} nicht, ${report.enrichedSteps} Schritte tragen sie`);

        /*
         * Die Galaxie zuklappen, bevor das Bild entsteht.
         *
         * Kein Kunstgriff, sondern der Weg, den ein Leser geht, der einen
         * Block lesen will: der Schalter heisst "collapse galaxy" und steht
         * daneben. Der Grund ist die Bedingung an die Beweisbilder: sie
         * entstehen im RUHEZUSTAND, also mit zurueckgesetztem Bildlauf, und in
         * einer 350 Pixel hohen Spalte waere die Schrittliste dann unter der
         * Kante. Ein Bild, auf dem der Fund oben steht und die Schritte nicht
         * zu sehen sind, koennte die Reihenfolge nicht zeigen, um die es hier
         * geht. Der Kasten sagt uebrigens auch im engen Zustand, dass es
         * weitergeht (die Marke "more below" des Twin, seit W8b).
         */
        const collapse = page.locator('[data-testid="atlas-galaxy-collapse"][data-fold="collapse"]');
        if (await collapse.count() > 0) {
            await collapse.click();
            await page.waitForTimeout(700);
        }
        extras.galaxyCollapsed = await page.evaluate(() =>
            document.querySelector('[data-testid="atlas-galaxy-collapse"]')?.getAttribute('data-fold') ?? '');

        await readability('getOrder, Block offen');
        extras.shots.push(await shootAtRest(page, SHOT_SHORT, 'pseudocode-short.png'));

        // ------------------------------ 5b. Ein Ziel fuehrt an seinen Ort
        /*
         * NACH dem Bild: dieser Klick verlaesst die Datei, und ein Bild danach
         * zeigte den Block eines anderen Symbols.
         */
        const clickable = steps.findIndex((line) => line.targetClickable);
        const beforeClick = await readerSeam(page);
        await page.click(
            '[data-testid="atlas-pseudocode-line"][data-kind="step"] '
            + `>> nth=${Math.max(0, clickable)} >> [data-testid="atlas-pseudocode-target"]`,
        );
        await page.waitForTimeout(1400);
        const afterClick = await readerSeam(page);
        const wanted = steps[Math.max(0, clickable)]?.target ?? '';
        const [wantedFile, wantedLine] = wanted.split(':');
        report.stepClickOpensReader = afterClick.path.endsWith(wantedFile)
            && afterClick.line === Number(wantedLine);
        extras.stepClick = { beforeClick, afterClick, wanted };
        log(`Ziel-Klick: ${beforeClick.path}:${beforeClick.line} -> `
            + `${afterClick.path}:${afterClick.line} (erwartet ${wanted})`);

        // ------------------------------------------- 5c. Der lange Fall
        await openSymbol(page, LONG);
        await page.waitForSelector('[data-testid="atlas-pseudocode"]', { timeout: 20000 });
        await page.waitForTimeout(700);
        const long = await blockSeam(page);
        extras.blocks.long = long;
        if (long === null) {
            throw new Error('der Pseudocode-Block des langen Falls steht nicht da');
        }

        /*
         * AC1, zweite Haelfte: keine Laengenschwelle.
         *
         * Derselbe Aufbau in derselben Reihenfolge, und in BEIDEN Faellen so
         * viele Zeilen im DOM, wie das Dokument haelt. Eine Schwelle, die bei
         * kurzen Funktionen etwas weglaesst, faellt hier auf, ohne dass der
         * Lauf raten muesste, wo sie stehen koennte.
         */
        const sameShape = (one, other) =>
            one.lead.top >= 0 && other.lead.top >= 0
            && one.imports.top > one.lead.top && other.imports.top > other.lead.top
            && one.firstLineTop > one.imports.top && other.firstLineTop > other.imports.top
            && one.stepsHead.text === other.stepsHead.text;
        report.noLengthThreshold = sameShape(short, long)
            && short.lines.length === (short.seam?.lines ?? -1)
            && long.lines.length === (long.seam?.lines ?? -1)
            && long.lines.length > short.lines.length;
        log(`Zeilen: kurz ${short.lines.length} (Dokument ${short.seam?.lines}), `
            + `lang ${long.lines.length} (Dokument ${long.seam?.lines}) -> `
            + `keine Schwelle ${report.noLengthThreshold}`);

        // Die Anreicherung ueber beide Faelle: was IRGENDWO vorlag, zaehlt.
        const merged = new Map();
        for (const entry of [
            ...(short.seam?.enrichment?.usable ?? []),
            ...(long.seam?.enrichment?.usable ?? []),
        ]) {
            const seen = merged.get(entry.kind);
            merged.set(entry.kind, seen === undefined
                ? { ...entry }
                : { ...seen, symbols: seen.symbols + entry.symbols });
        }
        /*
         * `silent` ist die dritte Liste und der Grund, warum `usable` nicht
         * als "mehr kann es nicht" gelesen werden darf: eine Sorte, die an
         * diesen zwei Bloecken nichts zu sagen hatte, ist keine Sorte, die
         * nichts sagen kann. Was IRGENDWO in diesem Lauf geantwortet hat,
         * faellt aus ihr heraus.
         */
        const silent = [
            ...(short.seam?.enrichment?.silent ?? []),
            ...(long.seam?.enrichment?.silent ?? []),
        ].filter((entry, at, all) =>
            !merged.has(entry.kind) && all.findIndex((other) => other.kind === entry.kind) === at);
        report.enrichmentAvailable = {
            usable: [...merged.values()],
            silent,
            missing: short.seam?.enrichment?.missing ?? [],
        };
        report.enrichedSteps += long.lines.filter((line) => line.behind.length > 0).length;
        extras.behind = {
            short: short.lines.flatMap((line) => line.behind.map((note) => `${line.text}: ${note.text}`)),
            long: long.lines.flatMap((line) => line.behind.map((note) => `${line.text}: ${note.text}`)),
        };
        log(`Was hinter den Aufrufen steht: ${JSON.stringify(extras.behind.short)}`);

        // Die Zusicherungen aus AC2 und AC4 gelten an BEIDEN Faellen.
        const longSteps = long.lines.filter((line) => line.kind === 'step');
        report.stepTargetsClickable = report.stepTargetsClickable
            && longSteps.length > 0
            && longSteps.every((line) => line.target.length > 0)
            && longSteps.filter((line) => line.targetKnown).every((line) => line.targetClickable);
        report.stepsWithoutTargetExplained = report.stepsWithoutTargetExplained
            && longSteps
                .filter((line) => !line.targetKnown)
                .every((line) => line.target.includes('the index records no place'));
        report.importHonestyWordsKept = report.importHonestyWordsKept
            && long.imports.tally.includes('that CodeAtlas cannot check');
        report.headSaysWhatTheFindingIs = report.headSaysWhatTheFindingIs
            && long.lead.text.length > 0
            && long.lead.kind.length > 0;

        // AC5 gilt ebenfalls an beiden: der laengere Block darf nicht mehr
        // Meta tragen als der kurze.
        report.metaCharsAfter = Math.max(report.metaCharsAfter, long.honest.chars);

        // AC6: die verschobenen Saetze sind wortgleich erreichbar geblieben.
        const kept = (hint) => hint.includes('in scope contributed steps')
            && hint.includes('Derived from the index and nothing else')
            && hint.includes('absent from this block, not absent from the code');
        extras.provenanceKept = {
            short: kept(short.honest.provenanceHint),
            long: kept(long.honest.provenanceHint),
        };

        await readability('createUser, Block offen');
        extras.shots.push(await shootAtRest(page, SHOT_LONG, 'pseudocode-long.png'));

        // ------------------------------------------------ 5d. Das Modell
        /*
         * Kein Byte Richtung 4141, von zwei Seiten belegt: der Mitschnitt der
         * abgewiesenen Anfragen dieses Kontexts, und der Zaehler der Anwendung
         * selbst, der nur hochgeht, wenn wirklich gefragt wurde.
         */
        extras.llm = await page.evaluate(() =>
            JSON.parse(JSON.stringify(globalThis.__atlasLlm ?? null)));
        const towardsModel = extras.blockedRequests.filter(
            (url) => url.includes(':4141') || url.includes('/v1/chat/completions') || url.includes('/completion'),
        );
        report.modelRequestsWhileOff = towardsModel.length + (extras.llm?.probes ?? 0);
        /*
         * Die Umformulierung bleibt, was sie ist: vom Leser angestossen,
         * positionsgenau geprueft, bei Abweichung verworfen. Belegt an drei
         * Dingen, die alle drei am laufenden System gelten: kein Knopf, solange
         * das Modell nicht bereit ist; der echte Validator lehnt eine
         * umnummerierte Antwort ab und nimmt die unveraenderte an; und
         * src/pseudocode/refine.ts traegt weiterhin genau diese Regel.
         */
        const refineSource = readFileSync(join(ROOT, 'src', 'pseudocode', 'refine.ts'), 'utf8');
        const validation = await page.evaluate(() => {
            const seam = globalThis.__atlasChat;
            if (seam === undefined) {
                return null;
            }
            /*
             * Genau die Kinder der Zeilen-Liste, wortgleich mit smoke-w5b:
             * die Importgruppe darueber traegt dieselbe Marke wie eine
             * Gruppenzeile, und sie mitzuzaehlen hiesse, dem Validator eine
             * Zeile mehr zu schicken, als der Block hat.
             */
            const lines = [...document.querySelectorAll('.atlas-pseudocode-lines > li')]
                .map((entry) => (entry.textContent ?? '').replace(/\s+/g, ' ').trim());
            const renumbered = lines
                .map((line) => (/^\d+[.)]\s/.test(line) ? line.replace(/^\d+/, '99') : line))
                .join('\n');
            return {
                asSent: seam.validateRefine(lines.join('\n')),
                renumbered: seam.validateRefine(renumbered),
                shorter: seam.validateRefine('1. call validateUser'),
            };
        });
        extras.refineProbe = {
            buttonPresent: await page.locator('[data-testid="atlas-pseudocode-refine-btn"]').count() > 0,
            validation,
            appliesPositionally: /applyRefinedPseudocode/.test(refineSource),
            readerTriggered: /RefineOutcome/.test(refineSource),
        };
        report.refineStillGated = extras.refineProbe.buttonPresent === false
            && validation?.asSent.applied === true
            && validation.renumbered.applied === false
            && validation.shorter.applied === false
            && extras.refineProbe.appliesPositionally === true;
        log(`Modell aus: ${report.modelRequestsWhileOff} Anfragen; refine gesichert: `
            + `${report.refineStillGated} (Knopf da: ${extras.refineProbe.buttonPresent})`);

        report.screenshotsAtRest =
            extras.shots.length === 2 && extras.shots.every((shot) => shot.atRest === true);
        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w8c] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w8c] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopServer(serverChild);
    await sleep(600);
    const leftovers = [];
    for (const port of [serverPort, uiPort].filter((value) => value > 0)) {
        leftovers.push({ port, listeners: await countListeners(port) });
    }
    extras.leftovers = leftovers;
    report.leftoverProcesses = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
    log('leftoverProcesses:', report.leftoverProcesses, JSON.stringify(leftovers));

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            shortCase: SHORT,
            longCase: LONG,
            metaMethod:
                'metaCharsAfter ist der sichtbare Text ueber den Block selbst, aus dem gerenderten Kasten '
                + 'gelesen und ohne das Fragezeichen. metaCharsBefore ist derselbe Kasten plus die Saetze, '
                + 'die W8c dahinter geraeumt hat (Deckung, ungenannte Symbole, Herkunftsnotiz), gelesen am '
                + 'Griff des laufenden Panels. Diese drei Zeichenketten sind unveraendert; ihre Laenge ist '
                + 'damit der Zustand vor diesem Zyklus, gemessen an dem, was das Produkt heute haelt.',
            enrichmentMethod:
                'Gemessen wird, was OHNE zusaetzliche Anfrage vorlag: die Layout-Antwort, die die Galaxie '
                + 'ohnehin geladen hat, verbunden ueber den qualifizierten Namen des aufgerufenen Symbols '
                + '(nie ueber den blossen Namen, denn dieses Projekt hat zwei Symbole namens `create`). '
                + '`usable` sind die Sorten, die an den zwei gemessenen Bloecken wirklich etwas ergeben '
                + 'haben; `silent` sind die, die dieser Block liest und die hier nichts zu melden hatten; '
                + '`missing` ist die stehende Liste dessen, was kein geladenes Datum hergibt, samt Grund '
                + '(src/pseudocode/step-insights.ts).',
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = [SHOT_SHORT, SHOT_LONG].every((file) => existsSync(file));
    const ok =
        failure === null
        && report.findingFirst === true
        && report.headSaysWhatTheFindingIs === true
        && report.noLengthThreshold === true
        && report.stepTargetsClickable === true
        && report.stepClickOpensReader === true
        && report.stepsWithoutTargetExplained === true
        && Array.isArray(report.enrichmentAvailable.usable)
        && Array.isArray(report.enrichmentAvailable.missing)
        && report.enrichmentAvailable.missing.length > 0
        && (report.enrichmentAvailable.usable.length === 0 || report.enrichedSteps >= 1)
        && report.noExtraServerRequest === true
        && report.importFindingProminent === true
        && report.importHonestyWordsKept === true
        && report.metaCharsBefore > 0
        && report.metaCharsAfter <= report.metaCharsBefore / 4
        && report.modelRequestsWhileOff === 0
        && report.refineStillGated === true
        && extras.provenanceKept?.short === true
        && extras.provenanceKept?.long === true
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.cutWithoutHint === 0
        && report.screenshotsAtRest === true
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && shotsOk
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w8c] W8c-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w8c] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W8c-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w8c] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
