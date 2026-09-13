#!/usr/bin/env node
/*
 * W13-Smoke: der Regler fragt nicht mehr wie viel, sondern fuer wen.
 *
 * ## Was dieser Lauf beantwortet, und warum keiner davor es konnte
 *
 * Nutzerbefund vom 2026-08-29, woertlich: "ich seh gar keine Aenderungen". Der
 * Befund war weder bestaetigt noch widerlegt, weil die Probe, die ihn messen
 * sollte, an den falschen Kennungen hing und vier Nullen meldete. Er wird hier
 * nicht geglaubt und nicht wegerklaert, sondern gemessen, und zwar an der
 * schaerfsten Stelle: bei AUSGESCHALTETEM Modell.
 *
 * Fuenf Stufen, ein Symbol, fuenf Messungen. Fuer jede Stufe die Zeichenzahl
 * des gerenderten Koerpers, seine erste Zeile, die Zahl der Abschnitte und die
 * Menge der Testmarken, die im DOM stehen. Daraus zwei Zahlen, die keine
 * Behauptung sind: `allLevelsDiffer` (keine zwei Stufen ergeben denselben Text)
 * und je Stufe ein `uniqueElement`, ausgerechnet als die Marke, die genau auf
 * dieser einen Stufe vorkommt. Eine Anwendung, die ohne Modell vollstaendig
 * sein soll, darf keinen Regler haben, der ohne Modell nichts tut.
 *
 * ## Die vier Kunstgriffe dieses Laufs
 *
 * 1. **Das eigene Element wird ausgerechnet, nicht aufgezaehlt.** Der Lauf
 *    sammelt je Stufe alle `data-testid` des Twin-Koerpers und schneidet die
 *    fuenf Mengen gegeneinander. Eine Liste im Quelltext dieses Laufs waere
 *    eine Erwartung, die gruen bleibt, wenn das Produkt sie nicht mehr
 *    erfuellt.
 *
 * 2. **Der Sidecar wird IM BROWSER beantwortet und nie angefasst.** Port 4141
 *    gehoert dem Nutzer; dieser Lauf startet dort nichts, beendet dort nichts
 *    und verbindet sich dorthin nicht. Jede Anfrage, die die Anwendung dorthin
 *    richtet, faengt der Route-Griff von Playwright ab und beantwortet sie aus
 *    einem Skript, bevor eine Verbindung entsteht. Was dabei angefragt wurde,
 *    steht vollstaendig im Artefakt (`extras.sidecar.requests`).
 *
 * 3. **Der Waechter wird durch die echte Funktion gefuehrt.** Die sieben
 *    gefaelschten Umformulierungen laufen durch `applyReaderRewrite`, an
 *    derselben Stelle, an der der Knopf sie hindurchschickt
 *    (`__atlasTwin.validateRewrite`). Ein Lauf, der die Regel nachbaut und
 *    gegen den Nachbau prueft, prueft den Nachbau.
 *
 * 4. **Der leere Fall wird gesucht und nicht angenommen.** Fuer AC7 geht der
 *    Lauf mehrere Symbole durch, bis er eins findet, dessen Senior-Stufe
 *    keinen einzigen Fehlerpfad hat, und misst dort, ob die Flaeche trotzdem
 *    etwas sagt. Welches Symbol es war, steht im Artefakt.
 *
 * ## Was hier NICHT gemessen wird, und das ist wichtig
 *
 * Die Schreibqualitaet eines echten Modells. Der Modellport des Nutzers ist
 * tabu, und ein zweites llama-server auf einem eigenen Port waere in diesem
 * Lauf kein Beweis ueber das Produkt, sondern ein zweiter Prozess mit eigener
 * Ladezeit. Gemessen wird die LEITUNG und der WAECHTER: dass die Anwendung mit
 * dem Modell an genau diese Saetze verschickt, dass eine treue Antwort
 * angewendet und mit ihrem Herkunftsvermerk gezeichnet wird, und dass eine
 * Antwort mit einem erfundenen Namen verworfen wird und der Leser den Grund
 * liest. Was der Stub zurueckgibt, ist eine deterministische, faktentreue
 * Umschreibung und keine Modellprosa; das steht auch im Artefakt.
 *
 * ## Ablauf
 *
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4600, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre und der Sidecar-Stub
 *      a. createUser oeffnen, die fuenf Stufen durchfahren und vermessen
 *      b. Reload und Symbolwechsel: haelt die Wahl?
 *      c. der leere Fall auf der Senior-Stufe
 *      d. der Waechter, sieben Faelschungen und eine treue Antwort
 *      e. Modell an: anwenden, zeichnen, verwerfen, Grund lesen
 *   6. abraeumen, Restprozesse mehrfach zaehlen, JSON und fuenf Bilder
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w13).
 *
 * ## Ports
 *
 * Ab 4600. 4141 gehoert dem Modell-Sidecar des Nutzers, 4142 seiner
 * Agenten-Bruecke, 4390/4391 und 4392/4393 seinen zwei Vorschauen; alles bis
 * 4600 gehoert den Laeufen davor. Dieser Lauf fasst keinen davon an.
 */

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, rm, stat, writeFile } from 'node:fs/promises';
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
const PROJECT = 'codeatlasweb-w13';
const OUT_DIR = join(ROOT, 'verification', 'w13');
const OUT_JSON = join(OUT_DIR, 'reader.json');

/** Contract: alles darunter gehoert dem Nutzer oder frueheren Laeufen. */
const MIN_PORT = 4600;

const VIEWPORT = { width: 1680, height: 1050 };

/** Der Modellport des Nutzers. Wird beantwortet, nie verbunden, nie gestartet. */
const SIDECAR_ORIGIN = 'http://127.0.0.1:4141';

/**
 * Die fuenf Leser, in der Reihenfolge des Reglers, mit dem Dateinamen ihres
 * Beweisbildes.
 */
const LEVELS = [
    /*
     * `wants` ist die Marke, von der dieser Lauf ERWARTET, dass sie nur auf
     * dieser Stufe steht. Sie ist keine Zusicherung: was im Artefakt landet,
     * ist die ausgerechnete Schnittmenge, und `wants` waehlt daraus nur aus,
     * welche der eigenen Marken genannt wird. Steht sie nicht in der
     * Schnittmenge, faellt der Lauf auf die erste ausgerechnete zurueck und die
     * Abweichung ist im Artefakt zu sehen (uniqueElements neben uniqueElement).
     */
    { level: 0, name: 'vibe coder', shot: 'level-vibe-coder.png', wants: 'codeatlas-twin-chip' },
    { level: 1, name: 'junior', shot: 'level-junior.png', wants: 'codeatlas-twin-term' },
    { level: 2, name: 'medior', shot: 'level-medior.png', wants: 'codeatlas-twin-section-steps' },
    { level: 3, name: 'senior', shot: 'level-senior.png', wants: 'codeatlas-twin-reader-fails' },
    { level: 4, name: 'architect', shot: 'level-architect.png', wants: 'codeatlas-twin-limit' },
];

/**
 * Das Symbol, an dem alle fuenf Stufen fotografiert werden.
 *
 * `createUser` und kein anderes: es ist das eine Symbol der Fixture, an dem
 * jede Faktenfamilie gefuellt ist (sechs Aufrufe, zwei Aufrufer, ein erhobener
 * Fehlertyp, eine Umgebungslesung, ein Testaufrufer). An einem leereren Symbol
 * waeren zwei Stufen aus Mangel an Fakten aehnlich, und der Unterschied im Bild
 * waere eine Aussage ueber die Fixture statt ueber den Regler.
 */
const MAIN = { name: 'createUser', file: 'src/services/userService.ts' };

/** Das zweite Symbol, fuer den Symbolwechsel aus AC6. */
const OTHER = { name: 'getOrder', file: 'src/services/orderService.ts' };

/**
 * Wo der Lauf nach einem Symbol OHNE Fehlerpfad sucht (AC7).
 *
 * Eine Liste und keine feste Wahl: welches Symbol der Index ohne erhobenen
 * Fehlertyp fuehrt, ist eine Eigenschaft des Index und nicht dieses Laufs, und
 * eine feste Wahl waere rot, sobald jemand die Fixture ergaenzt.
 */
const EMPTY_CANDIDATES = [
    { name: 'getOrder', file: 'src/services/orderService.ts' },
    { name: 'listUsers', file: 'src/services/userService.ts' },
    { name: 'toUser', file: 'src/services/userService.ts' },
    { name: 'insert', file: 'src/repo/db.ts' },
    { name: 'query', file: 'src/repo/db.ts' },
];

/** Chromium ohne Aussenwelt, wortgleich mit smoke-w8b und smoke-w8c. */
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

const log = (...parts) => console.log('[smoke-w13]', ...parts);
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

// ------------------------------------------------- Faelschungen des Modells ---

/**
 * Dieselbe Fakten-Zerlegung wie `src/twin/reader-rewrite.ts`, hier noch einmal.
 *
 * Bewusst kopiert und nicht importiert: sie steht in TypeScript, dieser Lauf
 * ist ein Skript, und ein Import ueber den Bau hinweg waere eine Abhaengigkeit
 * dieses Laufs auf das Ergebnis desselben Baus, den er pruefen soll. Sie wird
 * hier NUR benutzt, um Faelschungen zu bauen; geprueft wird ausschliesslich
 * durch die echte Funktion in der Seite.
 */
const FACT_TOKEN =
    /[A-Za-z_$][A-Za-z0-9_$]*(?:[./\\][A-Za-z0-9_$]+)+|[A-Za-z_$][A-Za-z0-9_$]*[A-Z][A-Za-z0-9_$]*|\d+(?:\.\d+)?/g;

const factsOf = (text) => text.match(FACT_TOKEN) ?? [];

/**
 * Eine treue Umschreibung: jeder Fakt bleibt, wo er ist.
 *
 * Ein vorangestellter Halbsatz aus lauter kleingeschriebenen Woertern. Er
 * traegt keinen Fakt, verschiebt keinen und bleibt weit unter der Laengendecke,
 * und er ist als Umschreibung erkennbar: der Text ist danach ein anderer.
 */
const faithful = (lines) => lines.map((line) => `put plainly, ${line.text}`).join('\n');

/** Die sieben Faelschungen, jede mit ihrem Namen und ihrem Grund. */
function forgeries(lines) {
    const texts = lines.map((line) => line.text);
    const firstWithFact = lines.findIndex((line) => line.facts.length > 0);
    const withNumber = lines.findIndex((line) => line.facts.some((fact) => /^\d+$/.test(fact)));
    const withFile = lines.findIndex((line) => line.facts.some((fact) => /\.[a-z]+$/.test(fact)));
    const withName = lines.findIndex((line) =>
        line.facts.some((fact) => /^[a-z][A-Za-z0-9_$]*[A-Z]/.test(fact)));
    const out = [];
    const swap = (index, from, to, label, why) => {
        if (index < 0) {
            return;
        }
        const copy = [...texts];
        copy[index] = copy[index].replace(from, to);
        if (copy[index] === texts[index]) {
            return;
        }
        out.push({ label, why, answer: copy.join('\n') });
    };
    if (withName >= 0) {
        const name = lines[withName].facts.find((fact) => /^[a-z][A-Za-z0-9_$]*[A-Z]/.test(fact));
        swap(withName, name, `${name}s`, 'renamed',
            'ein Name, der um einen Buchstaben abweicht, zeigt auf ein Symbol, das es nicht gibt');
    }
    if (withNumber >= 0) {
        const number = lines[withNumber].facts.find((fact) => /^\d+$/.test(fact));
        swap(withNumber, number, String(Number(number) + 1), 'renumbered',
            'eine Zahl, die um eins abweicht, ist eine falsche Zeile oder eine falsche Zaehlung');
    }
    if (withFile >= 0) {
        const file = lines[withFile].facts.find((fact) => /\.[a-z]+$/.test(fact));
        swap(withFile, file, file.replace(/\.[a-z]+$/, '.js'), 'refiled',
            'eine andere Datei ist ein anderer Ort');
    }
    if (firstWithFact >= 0) {
        const copy = [...texts];
        copy[firstWithFact] = `${copy[firstWithFact]} It also calls saveUserProfile.`;
        out.push({
            label: 'invented',
            why: 'ein dazuerfundener Name ist die teuerste Abweichung: die Zeile sieht danach genauso '
                + 'vertrauenswuerdig aus wie vorher',
            answer: copy.join('\n'),
        });
    }
    if (texts.length > 1) {
        const copy = [...texts];
        [copy[0], copy[1]] = [copy[1], copy[0]];
        out.push({
            label: 'reordered',
            why: 'dieselben Saetze in anderer Reihenfolge sind eine andere Aussage ueber den Ablauf',
            answer: copy.join('\n'),
        });
    }
    out.push({
        label: 'shortened',
        why: 'weniger Saetze als gegeben heisst, dass einer verschwunden ist',
        answer: texts.slice(0, 1).join('\n'),
    });
    out.push({
        label: 'padded',
        why: 'jeder Fakt behalten und die Seite gesprengt ist trotzdem keine Umformulierung',
        answer: texts
            .map((text, index) => (index === 0
                ? `${text} ${'and here is a great deal of prose that nobody asked for, '.repeat(6)}`
                : text))
            .join('\n'),
    });
    return out;
}

// ------------------------------------------------------------- Testgriffe ---

/** Was auf der Stufe wirklich dasteht, gelesen am gerenderten Koerper. */
const bodySeam = (page) =>
    page.evaluate(() => {
        const body = document.querySelector('.atlas-twin-body');
        if (body === null) {
            return null;
        }
        const clean = (value) => (value ?? '').replace(/\s+/g, ' ').trim();
        const marks = [...body.querySelectorAll('[data-testid]')]
            .map((node) => node.getAttribute('data-testid') ?? '')
            .filter((mark) => mark.length > 0);
        return {
            mode: body.getAttribute('data-mode') ?? '',
            levelName: body.getAttribute('data-level') ?? '',
            text: clean(body.textContent),
            question: clean(body.querySelector('[data-testid="codeatlas-twin-question"]')?.textContent),
            sections: body.querySelectorAll(
                '[data-testid^="codeatlas-twin-section-"], [data-testid^="codeatlas-twin-reader-"]:not('
                + '[data-testid="codeatlas-twin-reader-lead"]):not('
                + '[data-testid="codeatlas-twin-reader-weight"])',
            ).length,
            marks: [...new Set(marks)].sort(),
            voiced: body.querySelectorAll('[data-testid="codeatlas-twin-voiced"]').length,
            voiceNote: clean(body.querySelector('[data-testid="codeatlas-twin-voice-note"]')?.textContent),
            voiceRefused: body.querySelector('[data-testid="codeatlas-twin-voice-note"]')
                ?.getAttribute('data-refused') === 'true',
            voiceButton: body.querySelector('[data-testid="codeatlas-twin-voice-btn"]') !== null,
            emptyBlocks: [...body.querySelectorAll('[data-testid^="codeatlas-twin-reader-"]')]
                .filter((node) => node.getAttribute('data-populated') === 'false')
                .map((node) => ({
                    name: node.getAttribute('data-block-name') ?? '',
                    text: clean(node.textContent),
                })),
            seam: JSON.parse(JSON.stringify(globalThis.__atlasTwin ?? null, (key, value) =>
                (typeof value === 'function' ? undefined : value))),
        };
    });

/** Was der Regler sagt: die Beschriftung, der Wertebereich und der Name daneben. */
const sliderSeam = (page) =>
    page.evaluate(() => {
        const input = document.querySelector('[data-testid="atlas-twin-depth"]');
        const label = document.querySelector('.atlas-twin-depth-label');
        const name = document.querySelector('[data-testid="atlas-twin-depth-name"]');
        return {
            present: input !== null,
            label: (label?.textContent ?? '').trim(),
            ariaLabel: input?.getAttribute('aria-label') ?? '',
            ariaValueText: input?.getAttribute('aria-valuetext') ?? '',
            min: input?.getAttribute('min') ?? '',
            max: input?.getAttribute('max') ?? '',
            value: input?.value ?? '',
            name: (name?.textContent ?? '').trim(),
        };
    });

/** Wo jeder scrollbare Bereich steht. Wortgleich mit smoke-w8b und smoke-w8c. */
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

// ------------------------------------------------------------ Klickstrecke ---

async function openApp(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
    await page.waitForFunction(
        () => globalThis.__atlasTwin !== undefined,
        undefined,
        { timeout: 30000 },
    );
}

/** Zu einem Symbol navigieren, ueber dieselbe Suche, die die Kommandozeile fuehrt. */
async function openSymbol(page, target) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(target.name, { delay: 30 });
    await page.waitForSelector(
        `[data-testid="atlas-search-row"][data-name="${target.name}"]`,
        { timeout: 30000 },
    );
    await page.waitForTimeout(500);
    await page.click(`[data-testid="atlas-search-row"][data-name="${target.name}"]`);
    await page.waitForFunction(
        (expected) => (globalThis.__atlasTwin?.symbol ?? '') === expected,
        target.name,
        { timeout: 40000 },
    );
    await page.waitForFunction(
        () => document.querySelector('[data-testid="atlas-twin"]')
            ?.getAttribute('data-status') === 'ready',
        undefined,
        { timeout: 40000 },
    );
    await page.waitForTimeout(400);
}

/** Den Regler auf eine Stufe stellen, so wie ein Leser ihn zieht. */
async function setLevel(page, level) {
    await page.locator('[data-testid="atlas-twin-depth"]').fill(String(level));
    await page.waitForFunction(
        (expected) => (globalThis.__atlasTwin?.level ?? -1) === expected,
        level,
        { timeout: 15000 },
    );
    await page.waitForTimeout(260);
}

async function shootAtRest(page, file, name) {
    await closeTooltips(page);
    await resetScroll(page, READABILITY_EXCLUSIONS);
    await page.waitForTimeout(320);
    const state = await scrollState(page);
    await page.screenshot({ path: file, fullPage: false });
    const bytes = (await stat(file)).size;
    log(`${name}: aufgenommen (atRest=${state.atRest}, ${Math.round(bytes / 1024)} KB)`);
    return { name, atRest: state.atRest, bytes, regions: state.regions };
}

// ------------------------------------------------------------------- Lauf ----

async function main() {
    const totalStarted = Date.now();
    const timings = {};
    let home = null;
    let runtimeDir = null;
    let serverChild = null;
    let proxy = null;
    let browser = null;
    let serverPort = 0;
    let uiPort = 0;
    let failure = null;

    const report = {
        // AC1
        levels: [],
        allLevelsDiffer: false,
        measuredWithModelOff: false,
        // AC2
        sliderNamesReader: false,
        levelNameShown: false,
        // AC3, AC4
        modelRephrasesOnly: false,
        rejectedRewrites: 0,
        rejectionShownToReader: false,
        provenanceVisible: false,
        // AC5, AC6, AC7
        noRequestsWhileOff: 0,
        levelSurvivesReload: false,
        levelSurvivesSymbolChange: false,
        emptyLevelExplainsItself: false,
        // AC8
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
        shots: [],
        levelBodies: [],
        guard: { forgeries: [], faithful: null },
        sidecar: { requests: [], answers: [] },
        emptyCase: null,
        hold: null,
    };

    // Was der Stub als naechstes antwortet. Wird vor jedem Klick gesetzt.
    let nextAnswer = '';

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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w13-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w13-run-');
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
        extras.ports = { serverPort, uiPort };
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        // ------------------------------------------------------- 5. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const context = await browser.newContext({ viewport: { ...VIEWPORT } });
        const origin = `http://127.0.0.1:${uiPort}`;

        /*
         * Die Sperre zuerst, der Stub danach, und die Reihenfolge ist kein
         * Geschmack: Playwright prueft die ZULETZT gehaengte Route zuerst. Ein
         * Stub, der vor der Sperre haengt, kommt nie an die Reihe, und die
         * Anfrage an 4141 wird abgebrochen statt beantwortet. Gemessen: im
         * ersten Lauf standen elf abgebrochene /health in blockedRequests.
         */
        await context.route('**/*', async (route) => {
            const url = route.request().url();
            if (url.startsWith(origin) || url.startsWith('data:') || url.startsWith('blob:')) {
                await route.continue();
                return;
            }
            extras.blockedRequests.push(url);
            await route.abort();
        });

        /*
         * Der Sidecar-Stub. Verbunden wird dabei nichts: `route.fulfill`
         * beantwortet aus dem Griff heraus, bevor eine Verbindung entsteht.
         */
        await context.route(`${SIDECAR_ORIGIN}/**`, async (route) => {
            const url = route.request().url();
            const path = url.slice(SIDECAR_ORIGIN.length);
            extras.sidecar.requests.push({ path, at: Date.now() - totalStarted });
            const json = (body) => route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify(body),
            });
            if (path.startsWith('/health')) {
                return json({ status: 'ok' });
            }
            if (path.startsWith('/props')) {
                return json({
                    model_path: 'models/stub-for-w13.gguf',
                    n_ctx: 4096,
                    total_slots: 1,
                    default_generation_settings: { n_ctx: 4096 },
                });
            }
            if (path.startsWith('/v1/models')) {
                return json({ data: [{ id: 'models/stub-for-w13.gguf', object: 'model' }] });
            }
            if (path.startsWith('/v1/chat/completions')) {
                extras.sidecar.answers.push({ chars: nextAnswer.length });
                return json({
                    choices: [{ message: { content: nextAnswer }, finish_reason: 'stop' }],
                    usage: { prompt_tokens: 1, completion_tokens: 1 },
                });
            }
            return route.fulfill({ status: 404, contentType: 'text/plain', body: 'not answered' });
        });

        const page = await context.newPage();
        const sidecarRequests = [];
        page.on('request', (request) => {
            if (request.url().startsWith(SIDECAR_ORIGIN)) {
                sidecarRequests.push({ url: request.url(), at: Date.now() - totalStarted });
            }
        });
        page.on('console', (message) => {
            if (message.type() === 'error') {
                extras.consoleErrors.push(message.text());
            }
        });
        page.on('pageerror', (error) => extras.pageErrors.push(String(error)));
        await mkdir(OUT_DIR, { recursive: true });

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
                bottom: { candidates: bottom.candidates, overlaps: bottom.overlaps, clipped: bottom.clipped },
            });
            report.overlapViolations += top.overlaps.length + bottom.overlaps.length;
            report.clippingViolations += top.clipped.length + bottom.clipped.length;
            report.cutWithoutHint += [...top.clipped, ...bottom.clipped]
                .filter((entry) => entry.kind === 'cut-without-hint').length;
        };

        // ------------------------------- 5a. Die fuenf Stufen, ein Symbol
        await openApp(page, origin);
        await openSymbol(page, MAIN);

        /*
         * Die Galaxie einklappen, bevor fotografiert wird.
         *
         * Kein Aufhuebschen: der Twin teilt sich die rechte Spalte mit dem
         * Modell-Panel und dem Graphen, und mit allen dreien uebereinander sind
         * von jeder Stufe die ersten fuenf Zeilen zu sehen. Der Auftrag lautet,
         * dass der Unterschied IM BILD steht, und fuenf Bilder, auf denen
         * jeweils dieselben fuenf Zeilen abgeschnitten werden, erfuellen ihn
         * nicht. Eingeklappt wird ueber genau den Knopf, den ein Leser dafuer
         * hat; gemessen (Zeichen, Marken, Ueberschneidungen) wird an demselben
         * Zustand, in dem fotografiert wird.
         */
        await page.click('[data-testid="atlas-galaxy-collapse"]');
        await page.waitForTimeout(500);

        const slider = await sliderSeam(page);
        extras.slider = slider;
        /*
         * AC2 ist zwei Fragen und nicht eine: fragt die Beschriftung nach dem
         * LESER (und nicht mehr nach der Menge), und steht der Name der
         * gewaehlten Stufe daneben? Die erste wird an drei Dingen gemessen: das
         * Wort "detail" ist weg, das Wort "reading" ist da, und der Regler
         * traegt dieselbe Beschriftung als ansagbaren Namen.
         */
        report.sliderNamesReader = slider.present
            && /read/i.test(slider.label)
            && !/detail/i.test(slider.label)
            && slider.ariaLabel === slider.label
            && slider.min === '0'
            && slider.max === '4';
        log(`AC2 Beschriftung: "${slider.label}" -> ${report.sliderNamesReader}`);

        for (const entry of LEVELS) {
            await setLevel(page, entry.level);
            const body = await bodySeam(page);
            if (body === null) {
                throw new Error(`Stufe ${entry.level} hat keinen Koerper gerendert`);
            }
            if (body.levelName !== entry.name) {
                throw new Error(
                    `Stufe ${entry.level} nennt sich "${body.levelName}" statt "${entry.name}"`,
                );
            }
            extras.levelBodies.push({ ...entry, ...body });
            await readability(`level-${entry.level}-${entry.name}`);
            extras.shots.push(await shootAtRest(page, join(OUT_DIR, entry.shot), entry.shot));
        }

        /*
         * Das eigene Element jeder Stufe: die Marke, die auf genau dieser einen
         * Stufe im Koerper steht. Ausgerechnet und nicht aufgezaehlt.
         */
        const marksPerLevel = extras.levelBodies.map((body) => new Set(body.marks));
        report.levels = extras.levelBodies.map((body, index) => {
            const others = marksPerLevel.filter((_, at) => at !== index);
            const own = [...marksPerLevel[index]]
                .filter((mark) => others.every((other) => !other.has(mark)));
            return {
                name: body.name,
                level: body.level,
                mode: body.mode,
                chars: body.text.length,
                sections: body.sections,
                firstLine: body.question,
                uniqueElement: own.includes(body.wants) ? body.wants : (own[0] ?? ''),
                uniqueElementAsExpected: own.includes(body.wants),
                uniqueElements: own,
                marks: body.marks.length,
            };
        });
        const texts = extras.levelBodies.map((body) => body.text);
        report.allLevelsDiffer = new Set(texts).size === LEVELS.length
            && report.levels.every((entry) => entry.chars > 0 && entry.uniqueElement.length > 0);
        report.levelNameShown = extras.levelBodies.every((body, index) =>
            body.seam?.levelName === LEVELS[index].name)
            && (await sliderSeam(page)).name === 'architect';
        for (const entry of report.levels) {
            log(`Stufe ${entry.level} ${entry.name}: ${entry.chars} Zeichen, `
                + `${entry.sections} Abschnitte, eigenes Element ${entry.uniqueElement}`);
        }
        log(`AC1 alle fuenf verschieden: ${report.allLevelsDiffer}`);

        // ------------------------------------- 5b. Der Waechter, ohne Modell
        /*
         * Gemessen auf der Junior-Stufe, und die Wahl ist der halbe Test: ihre
         * Saetze sind die einzigen, die alle vier Sorten Fakt zugleich tragen
         * (Name, Datei, Zeile und Zahl), weil sie die Schritte in ihrer
         * Reihenfolge ausschreiben. Auf einer Stufe, deren Saetze nur Prosa
         * sind, koennte dieser Lauf eine umgeschriebene Datei gar nicht
         * faelschen und wuerde eine Luecke als Erfolg melden.
         */
        await setLevel(page, 1);
        const subject = await page.evaluate(() =>
            JSON.parse(JSON.stringify(globalThis.__atlasTwin?.subject ?? [])));
        if (subject.length === 0) {
            throw new Error('die Architekten-Stufe hat keinen Satz, den ein Modell formulieren duerfte');
        }
        const fakes = forgeries(subject);
        const guard = await page.evaluate((input) => {
            const check = globalThis.__atlasTwin?.validateRewrite;
            if (typeof check !== 'function') {
                return null;
            }
            return {
                faithful: check(input.faithful),
                unchanged: check(input.unchanged),
                forgeries: input.forgeries.map((entry) => ({
                    label: entry.label,
                    ...check(entry.answer),
                })),
            };
        }, {
            faithful: faithful(subject),
            unchanged: subject.map((line) => line.text).join('\n'),
            forgeries: fakes,
        });
        if (guard === null) {
            throw new Error('der Griff auf den Waechter fehlt');
        }
        extras.guard.faithful = guard.faithful;
        extras.guard.unchanged = guard.unchanged;
        extras.guard.forgeries = guard.forgeries.map((entry, index) => ({
            ...entry,
            why: fakes[index].why,
            answerHead: fakes[index].answer.slice(0, 160),
        }));
        report.rejectedRewrites = guard.forgeries.filter((entry) => entry.applied === false).length;
        const guardHolds = guard.faithful.applied === true
            && guard.unchanged.applied === true
            && guard.forgeries.length >= 6
            && guard.forgeries.every((entry) => entry.applied === false && entry.reason.length > 0);
        log(`AC3 Waechter: treu angewendet ${guard.faithful.applied}, `
            + `${report.rejectedRewrites} von ${guard.forgeries.length} Faelschungen verworfen`);

        // ------------------------------ 5c. AC5: aus heisst aus, auch am Regler
        const llmOff = await page.evaluate(() =>
            JSON.parse(JSON.stringify(globalThis.__atlasLlm ?? null)));
        extras.llmOff = llmOff;
        report.noRequestsWhileOff = sidecarRequests.length + (llmOff?.probes ?? 0);
        report.measuredWithModelOff = report.noRequestsWhileOff === 0
            && (llmOff?.state ?? '') === 'off'
            && extras.levelBodies.every((body) => body.voiced === 0 && body.voiceButton === false);
        log(`AC5 Modell aus: ${report.noRequestsWhileOff} Anfragen `
            + `(Mitschnitt ${sidecarRequests.length}, Naht ${llmOff?.probes})`);

        // ---------------------------------- 5d. AC6: die Wahl haelt
        await setLevel(page, 4);
        await page.reload({ waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
        await page.waitForFunction(
            () => (globalThis.__atlasTwin?.level ?? -1) === 4,
            undefined,
            { timeout: 20000 },
        );
        const afterReload = await sliderSeam(page);
        report.levelSurvivesReload = afterReload.value === '4' && afterReload.name === 'architect';

        await openSymbol(page, MAIN);
        const beforeSwitch = await sliderSeam(page);
        await openSymbol(page, OTHER);
        const afterSwitch = await sliderSeam(page);
        report.levelSurvivesSymbolChange = beforeSwitch.value === '4'
            && afterSwitch.value === '4'
            && afterSwitch.name === 'architect';
        extras.hold = { afterReload, beforeSwitch, afterSwitch };
        log(`AC6 haelt: Reload ${report.levelSurvivesReload}, `
            + `Symbolwechsel ${report.levelSurvivesSymbolChange}`);

        // ---------------------------- 5e. AC7: die leere Stufe sagt es selbst
        const emptyTried = [];
        for (const candidate of EMPTY_CANDIDATES) {
            try {
                await openSymbol(page, candidate);
            } catch (error) {
                // Ein Name, den die Suche dieses Index nicht fuehrt, ist kein
                // Befund ueber die Stufe. Der Lauf geht weiter und schreibt auf,
                // welche Kandidaten er ueberhaupt oeffnen konnte.
                emptyTried.push({ symbol: candidate.name, opened: false, reason: String(error).slice(0, 120) });
                continue;
            }
            await setLevel(page, 3);
            const body = await bodySeam(page);
            emptyTried.push({
                symbol: candidate.name,
                opened: true,
                emptyBlocks: (body?.emptyBlocks ?? []).map((block) => block.name),
            });
            const fails = body?.emptyBlocks.find((block) => block.name === 'fails');
            if (fails !== undefined) {
                extras.emptyCase = {
                    symbol: candidate.name,
                    file: candidate.file,
                    block: fails.name,
                    text: fails.text,
                    allEmptyBlocks: body.emptyBlocks.map((block) => block.name),
                };
                report.emptyLevelExplainsItself = fails.text.length > 60
                    && /no error path was recorded here/i.test(fails.text)
                    && body.emptyBlocks.every((block) => block.text.length > 60);
                break;
            }
        }
        extras.emptyTried = emptyTried;
        log(`AC7 leere Stufe: ${report.emptyLevelExplainsItself} `
            + `(${extras.emptyCase?.symbol ?? 'kein Symbol ohne Fehlerpfad gefunden'})`);

        // -------------------------- 5f. AC3 und AC4 am laufenden System
        await openSymbol(page, MAIN);
        await setLevel(page, 0);
        const beforeModel = await bodySeam(page);
        nextAnswer = faithful(beforeModel.seam.subject);
        // Das Suchfenster liegt nach dem Symbolwechsel noch ueber der Zeile.
        await page.keyboard.press('Escape');
        await page.waitForTimeout(200);
        await page.click('[data-menu="a-llm"]');
        await page.waitForFunction(
            () => globalThis.__atlasLlm?.preferenceOn === true,
            undefined,
            { timeout: 10000 },
        );
        await page.waitForFunction(
            () => (globalThis.__atlasLlm?.state ?? '') === 'ready',
            undefined,
            { timeout: 30000 },
        );
        await page.waitForSelector('[data-testid="codeatlas-twin-voice-btn"]', { timeout: 15000 });
        await page.click('[data-testid="codeatlas-twin-voice-btn"]');
        await page.waitForFunction(
            () => (globalThis.__atlasTwin?.voiceState ?? '') === 'applied',
            undefined,
            { timeout: 30000 },
        );
        const applied = await bodySeam(page);
        extras.applied = {
            voiced: applied.voiced,
            note: applied.voiceNote,
            text: applied.text.slice(0, 400),
            sentences: applied.seam?.rewritableSentences ?? 0,
        };
        /*
         * AC4: ein formulierter Satz traegt seinen Vermerk, so wie
         * `PURPOSE inferred [?]` es tut. Gemessen an der Zahl der Marken im
         * Koerper und daran, dass sie so viele sind wie umformulierte Saetze.
         */
        report.provenanceVisible = applied.voiced > 0
            && applied.voiced === (applied.seam?.voicedSentences ?? -1)
            && applied.text.includes('put plainly,');
        await readability('modell an, umformuliert');
        log(`AC4 Herkunft sichtbar: ${report.provenanceVisible} (${applied.voiced} Vermerke)`);

        // Und jetzt die Antwort, die etwas dazuerfindet.
        const invented = fakes.find((entry) => entry.label === 'invented');
        nextAnswer = beforeModel.seam.subject
            .map((line, index) => (index === 0
                ? `put plainly, ${line.text} It also calls saveUserProfile.`
                : `put plainly, ${line.text}`))
            .join('\n');
        await page.click('[data-testid="codeatlas-twin-voice-btn"]');
        await page.waitForFunction(
            () => (globalThis.__atlasTwin?.voiceState ?? '') === 'refused',
            undefined,
            { timeout: 30000 },
        );
        const refused = await bodySeam(page);
        extras.refused = {
            note: refused.voiceNote,
            marked: refused.voiceRefused,
            voiced: refused.voiced,
            seamMessage: refused.seam?.voiceMessage ?? '',
        };
        report.rejectionShownToReader = refused.voiceRefused === true
            && /thrown away/i.test(refused.voiceNote)
            && /saveUserProfile/.test(refused.voiceNote)
            && refused.voiced === 0;
        report.modelRephrasesOnly = guardHolds
            && report.provenanceVisible
            && report.rejectionShownToReader
            && invented !== undefined;
        await readability('modell an, verworfen');
        log(`AC3 verworfen und gesagt: ${report.rejectionShownToReader} `
            + `("${refused.voiceNote.slice(0, 120)}")`);

        report.screenshotsAtRest = extras.shots.length === LEVELS.length
            && extras.shots.every((shot) => shot.atRest === true && shot.bytes > 30 * 1024);
        extras.apiRoutes = { ...proxy.log.apiRoutes };
        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w13] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w13] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
    }

    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopServer(serverChild);

    /*
     * Mehrfach nachsehen statt einmal.
     *
     * Ein Prozess, der eben ein SIGTERM bekommen hat, gibt seinen Port nicht in
     * derselben Millisekunde frei; smoke-w6-full misst dafuer 1557 ms am
     * Modellport. Eine einzige Zaehlung waere eine Aussage ueber die
     * Reaktionszeit dieser Maschine und nicht ueber Prozessreste.
     */
    const ports = [serverPort, uiPort].filter((value) => value > 0);
    const looks = [];
    let leftovers = [];
    for (let attempt = 0; attempt < 8; attempt += 1) {
        leftovers = [];
        for (const port of ports) {
            leftovers.push({ port, listeners: await countListeners(port) });
        }
        const total = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
        looks.push({ attempt: attempt + 1, atMs: Date.now() - totalStarted, total });
        if (total === 0) {
            break;
        }
        await sleep(400);
    }
    extras.leftovers = leftovers;
    extras.leftoverLooks = looks;
    report.leftoverProcesses = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
    log('leftoverProcesses:', report.leftoverProcesses, JSON.stringify(looks));

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            symbol: MAIN,
            method:
                'Die fuenf Stufen werden an EINEM Symbol gemessen, bei ausgeschaltetem Modell. Der Text '
                + 'ist der gerenderte Koerper des Twin (.atlas-twin-body), nicht das Modell dahinter; '
                + 'das eigene Element einer Stufe ist die data-testid, die genau auf ihr vorkommt, und '
                + 'wird aus dem Schnitt der fuenf Markenmengen ausgerechnet statt hier aufgezaehlt. '
                + 'AC3 laeuft durch die ECHTE Pruefung (__atlasTwin.validateRewrite, dieselbe Funktion, '
                + 'die der Knopf ruft): eine treue Umschreibung, die unveraenderte Vorlage und sieben '
                + 'Faelschungen, jede mit einer anderen Abweichung.',
            sidecarMethod:
                'Port 4141 gehoert dem Nutzer. Dieser Lauf startet dort nichts, beendet dort nichts und '
                + 'verbindet sich dorthin nicht: jede Anfrage der Anwendung an diesen Ursprung wird von '
                + 'einem Route-Griff im Browser beantwortet, bevor eine Verbindung entsteht, und steht '
                + 'unter extras.sidecar.requests. Was der Stub zurueckgibt, ist eine deterministische, '
                + 'faktentreue Umschreibung (ein vorangestellter Halbsatz aus kleingeschriebenen '
                + 'Woertern) und keine Modellprosa. Gemessen wird damit die Leitung und der Waechter, '
                + 'nicht die Schreibqualitaet eines Modells.',
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = LEVELS.every((entry) => existsSync(join(OUT_DIR, entry.shot)));
    const ok =
        failure === null
        && report.levels.length === 5
        && report.allLevelsDiffer === true
        && report.measuredWithModelOff === true
        && report.sliderNamesReader === true
        && report.levelNameShown === true
        && report.modelRephrasesOnly === true
        && report.rejectedRewrites >= 6
        && report.rejectionShownToReader === true
        && report.provenanceVisible === true
        && report.noRequestsWhileOff === 0
        && report.levelSurvivesReload === true
        && report.levelSurvivesSymbolChange === true
        && report.emptyLevelExplainsItself === true
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
        console.error('[smoke-w13] W13-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w13] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W13-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w13] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
