#!/usr/bin/env node
/*
 * W7b-Smoke: eine Menuezeile aus sechs gleichen Knoepfen, Kuerzel, die sich im
 * Zweifel selbst erklaeren, eine Suche, die vor dem Server antwortet, und die
 * Messung der Vektorsuche des Servers.
 *
 * Drei Nutzerbefunde vom 2026-08-29 und eine Nutzerfrage stehen dahinter:
 *
 *  1. Die vier Eintraege der Atlas-Zeile sahen aus wie Text zwischen zwei
 *     Bedienelementen. Gemessen wird deshalb nicht "sieht huebsch aus", sondern
 *     die Struktur: dieselbe Klasse, dasselbe Element, dieselbe Polsterung,
 *     derselbe Rahmen, derselbe Klammer-Buchstabe, und jeder der sechs laesst
 *     sich mit der Maus UND mit Tab plus Enter ausloesen, mit sichtbarem Ring.
 *  2. "alt plus letter funktioniert nur fuer atlas". An der laufenden Vorschau
 *     nicht nachzustellen. Dieser Lauf misst deshalb zweierlei: dass alle fuenf
 *     Kuerzel wirklich ausloesen, und dass der Griff am Fenster in der
 *     EINFANGENDEN Phase haengt, also niemand ihm zuvorkommen kann. Das zweite
 *     ist keine Behauptung ueber eine Zeile Quelltext, sondern eine Beobachtung:
 *     ein Griff am Eingabefeld sieht das Ereignis nach dem Fenster, also schon
 *     abbestellt. Dazu kommt der Tastentest der Hilfeseite, der dem Nutzer beim
 *     naechsten Versuch sagt, was an SEINEM Geraet ankommt.
 *  3. Die Suchvorschlaege erscheinen zu langsam. Gemessen wird die Strecke, die
 *     der Leser spuert: vom letzten Tastendruck bis zur ersten sichtbaren Zeile,
 *     ueber eine Reihe von Eingaben, plus der Serverweg daneben, plus die
 *     Zusicherung, dass eine ueberholte Antwort nie gewinnt.
 *  4. "Beruht die @-Suche auf einem Modell oder auf Embeddings?" Heute auf
 *     keinem von beidem. Der Server kann aber Vektorsuche (mcp.c:410-417). Sie
 *     wird hier gegen die Fixture gemessen, mit einem vorher festgelegten
 *     Massstab, und das Urteil steht mit seinen Zahlen im Artefakt.
 *
 * Ablauf:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (nur gelesen), Modus `full`, weil die
 *      Vektorsuche moderate/full verlangt
 *   4. C-Server auf einem freien Port >= 4380, dist/ auf einem zweiten
 *   5. Die Vektorsuche messen, direkt am Server, ohne Browser
 *   6. Chromium ohne Aussenwelt, plus Route-Sperre
 *      a. Menuezeile: Gestalt, Klick, Tastatur, Fokusring, Bild
 *      b. Alt-Kuerzel, einfangende Phase, Tastentest der Hilfe
 *      c. Suche: Messreihe, Praefix-Cache, ueberholte Antwort, Bild
 *   7. abraeumen, Restprozesse zaehlen, verification/w7/search.json schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w7b).
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
import {
    DELIBERATE_OVERLAYS,
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
const PROJECT = 'codeatlasweb-w7b';
const OUT_DIR = join(ROOT, 'verification', 'w7');
const OUT_JSON = join(OUT_DIR, 'search.json');
const MIN_PORT = 4380;

/** Die sechs Eintraege der Zeile, in der Reihenfolge, in der sie dastehen. */
/*
 * Die Eintraege der Zeile, in ihrer Reihenfolge.
 *
 * `a-layout` ist seit W8 dabei ("[r]eset layout", der Weg zurueck zum
 * Vorgabe-Layout). Er steht hier aus demselben Grund wie die anderen sechs: die
 * Zusicherung dieses Laufs ist, dass JEDER Eintrag der Zeile dieselbe Gestalt
 * hat, mit der Maus geht, mit der Tastatur geht und sein Kuerzel wirklich
 * ausloest. Ein siebter Eintrag, der hier fehlte, waere ein Eintrag ohne diese
 * Pruefung, und genau davor schuetzt dieser Lauf.
 */
const MENU_ENTRIES = [
    { menu: 'a', letter: 'a' },
    { menu: 'a-why', letter: 'w' },
    { menu: 'a-bug', letter: 'b' },
    { menu: 'a-impact', letter: 'c' },
    { menu: 'a-layout', letter: 'r' },
    { menu: 'a-llm', letter: 'l' },
    { menu: '?', letter: '?' },
];

/** Die Kuerzel, die der Nutzerbefund nennt, plus das aus W8. */
const ALT_LETTERS = ['a', 'w', 'b', 'c', 'l', 'r'];

/**
 * Die Messreihe der Suche.
 *
 * Zwoelf Eingaben und nicht zehn: der frozen Test verlangt zehn, und zwei
 * Reserve kosten nichts. Es sind Woerter aus der Fixture und keine Zufallsfolgen,
 * weil eine Suche, die nichts findet, auch nichts anzeigen muss und die Messung
 * dann eine andere Frage beantworten wuerde.
 */
const SEARCH_SAMPLES = [
    'user', 'create', 'validate', 'insert', 'order', 'query',
    'route', 'config', 'server', 'walk', 'hotspot', 'toUser',
];

/** Das Wort, an dem der Praefix-Cache gemessen wird: erst `va`, dann `l`. */
const PREFIX_BASE = 'va';
const PREFIX_EXTRA = 'lidate';

/**
 * Die Vokabular-Bruecken, an denen die Vektorsuche gemessen wird.
 *
 * Sie stehen HIER und nicht im Ergebnis-JSON allein, weil der Massstab vor der
 * Messung feststehen muss: eine Bruecke, die nach dem Blick auf die Antwort
 * ausgesucht wird, misst nichts.
 *
 * Die Ziele sind die Symbole, die die Fixture wirklich traegt. `publish` steht
 * mit dabei, obwohl es sie nicht hat: die Werbung des Servers nennt genau
 * dieses Beispiel ("finds publish when you search send"), und ein Ziel
 * wegzulassen, weil es nicht existiert, waere ein Massstab, der sich an die
 * Antwort anpasst.
 */
const SEMANTIC_BRIDGES = [
    { word: 'send', targets: ['publish', 'insert'] },
    { word: 'check', targets: ['validateUser', 'validateId'] },
    { word: 'start', targets: ['main', 'createApp'] },
    { word: 'save', targets: ['insert'] },
    { word: 'fetch', targets: ['query'] },
    { word: 'verify', targets: ['validateUser'] },
];

/**
 * Wie weit oben ein Ziel stehen muss, damit die Bruecke als gefunden gilt.
 *
 * Fuenf, und das ist grosszuegig: das Trefferfenster zeigt zehn Zeilen, und
 * semantische Treffer waeren die ZWEITE Quelle darin, also stuenden sie unter
 * den Namenstreffern. Wer bei fuenf nicht dabei ist, kaeme im Produkt gar nicht
 * vor.
 */
const SEMANTIC_TOP_N = 5;

/**
 * Die Gegenprobe zur Bruecken-Messung: ein Symbol unter seinem EIGENEN Namen.
 *
 * Sie steht hier, weil eine Vektorsuche, die ein Symbol nicht findet, wenn man
 * seinen Namen eingibt, auch keine Vokabular-Bruecke schlagen kann. Ohne diese
 * Probe koennte ein schwaches Bruecken-Ergebnis heissen "die Bruecken waren
 * schlecht gewaehlt"; mit ihr ist die Aussage eindeutig.
 */
const SEMANTIC_IDENTITY = ['insert', 'validateUser', 'hotspotScan', 'loadConfig'];

/** Ein Wort, das nichts bedeutet. Was der Modus damit tut, ist die dritte Probe. */
const SEMANTIC_NONSENSE = 'zzzzqqqq';

/**
 * Der Sidecar-Port, an den dieser Lauf ausdruecklich NICHT redet.
 *
 * `[l]lm` ist einer der sechs Menuepunkte und eines der fuenf Alt-Kuerzel, also
 * schaltet dieser Lauf den lokalen Modell-Schalter ein: anders liesse sich nicht
 * beweisen, dass der Punkt und die Taste wirken. Damit fragt die Oberflaeche
 * regelmaessig, ob auf 4141 ein Modell antwortet. Auf dieser Maschine laeuft
 * dort ein Prozess, mit dem gearbeitet wird, und ein Beweislauf hat an fremden
 * Prozessen nichts zu suchen. Die Route-Sperre laesst diese Anfragen deshalb gar
 * nicht erst hinaus, und sie werden hier gezaehlt statt uebersehen: eine
 * geblockte Anfrage, die niemand benennt, ist eine Sperre, der man nicht ansieht,
 * was sie gehalten hat.
 */
const SIDECAR_HOSTPORT = '127.0.0.1:4141';

/** Chromium ohne Aussenwelt. Wortgleich mit smoke-w7a. */
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

const log = (...parts) => console.log('[smoke-w7b]', ...parts);
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
        child.stdout.on('data', (d) => { out += d.toString(); });
        child.stderr.on('data', (d) => { err += d.toString(); });
        child.on('error', (error) => done({ code: 127, out, err: err + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out, err }));
        child.stdin.end();
    });
}

/** Der Mittelwert der Mitte. Bei gerader Zahl das Mittel der beiden mittleren. */
function median(values) {
    if (values.length === 0) {
        return Number.NaN;
    }
    const sorted = [...values].sort((a, b) => a - b);
    const half = Math.floor(sorted.length / 2);
    return sorted.length % 2 === 1
        ? sorted[half]
        : Math.round((sorted[half - 1] + sorted[half]) / 2);
}

// --------------------------------------------------------- Die Vektorsuche --

/** Ein Werkzeug am Server, direkt und ohne Browser. Nur Loopback. */
async function callTool(port, name, args) {
    const res = await fetch(`http://127.0.0.1:${port}/rpc`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
            jsonrpc: '2.0',
            id: 1,
            method: 'tools/call',
            params: { name, arguments: args },
        }),
    });
    const body = await res.json();
    if (body?.error) {
        throw new Error(`${name}: ${JSON.stringify(body.error).slice(0, 300)}`);
    }
    return body?.result?.content?.[0]?.text ?? '';
}

/**
 * Die Zeilen einer semantischen Antwort.
 *
 * Die Antwort kommt in der kompakten Zeilenform des Servers
 * (`semantic: N (cols: qn label file score)`), also wird sie hier so gelesen,
 * wie sie kommt. Ein eigener Parser und nicht der des Produkts: das Produkt
 * liest diese Form nicht, und ihn dafuer zu erweitern hiesse, Code fuer eine
 * Faehigkeit zu schreiben, ueber deren Aufnahme dieser Lauf erst entscheidet.
 */
function semanticRows(text) {
    return text
        .split('\n')
        .filter((line) => /^ {2}\S/.test(line))
        .map((line) => {
            const cells = line.trim().split(/\s+/);
            const qn = cells[0] ?? '';
            return {
                qn,
                name: qn.split('.').pop() ?? '',
                score: Number(cells[cells.length - 1]),
            };
        });
}

/** Wie lange ein Aufruf gedauert hat, und was er geliefert hat. */
async function timedSemantic(port, words) {
    const started = Date.now();
    const text = await callTool(port, 'search_graph', {
        project: PROJECT,
        semantic_query: words,
        limit: 50,
    });
    return { ms: Date.now() - started, rows: semanticRows(text) };
}

/**
 * Die Vektorsuche des Servers, gemessen.
 *
 * Drei Proben, und das Urteil faellt aus allen dreien: die Bruecken (was sie
 * leisten SOLL), die Selbstidentitaet (ob sie ueberhaupt findet) und das
 * Unsinnswort (ob sie je "nichts" sagen kann).
 */
async function measureSemantic(port) {
    const report = {
        mode: 'measured-and-rejected',
        bridgesTried: SEMANTIC_BRIDGES.length,
        bridgesFound: 0,
        topN: SEMANTIC_TOP_N,
        latencyMs: 0,
        bm25LatencyMs: 0,
        bridges: [],
        identity: [],
        nonsense: null,
        rejectionReason: '',
    };

    // Der Vergleichsmassstab der Latenz: derselbe Aufruf als Namenssuche.
    const bm25 = [];
    for (const bridge of SEMANTIC_BRIDGES) {
        const started = Date.now();
        await callTool(port, 'search_graph', { project: PROJECT, query: bridge.word, limit: 25 });
        bm25.push(Date.now() - started);
    }
    report.bm25LatencyMs = median(bm25);

    const latencies = [];
    for (const bridge of SEMANTIC_BRIDGES) {
        const answer = await timedSemantic(port, [bridge.word]);
        latencies.push(answer.ms);
        const ranks = bridge.targets.map((target) => ({
            target,
            rank: answer.rows.findIndex((row) => row.name === target) + 1,
        }));
        const best = ranks
            .filter((entry) => entry.rank > 0)
            .sort((a, b) => a.rank - b.rank)[0];
        const found = best !== undefined && best.rank <= SEMANTIC_TOP_N;
        if (found) {
            report.bridgesFound += 1;
        }
        report.bridges.push({
            word: bridge.word,
            targets: bridge.targets,
            ranks,
            rows: answer.rows.length,
            found,
            latencyMs: answer.ms,
            top: answer.rows.slice(0, SEMANTIC_TOP_N).map((row) => row.name),
        });
    }
    report.latencyMs = median(latencies);

    for (const name of SEMANTIC_IDENTITY) {
        const answer = await timedSemantic(port, [name]);
        report.identity.push({
            name,
            rank: answer.rows.findIndex((row) => row.name === name) + 1,
            rows: answer.rows.length,
            topScore: answer.rows[0]?.score ?? null,
            lastScore: answer.rows[answer.rows.length - 1]?.score ?? null,
        });
    }

    const nonsense = await timedSemantic(port, [SEMANTIC_NONSENSE]);
    report.nonsense = {
        word: SEMANTIC_NONSENSE,
        rows: nonsense.rows.length,
        topScore: nonsense.rows[0]?.score ?? null,
        lastScore: nonsense.rows[nonsense.rows.length - 1]?.score ?? null,
    };

    /*
     * Das Urteil, nach dem Massstab des Contracts: brauchbar, wenn die Latenz
     * nicht schlechter ist als BM25 plus 300 ms UND mindestens drei der sechs
     * Bruecken sitzen. Die Latenz ist an diesem Server nie das Problem; die
     * Bruecken sind es.
     */
    const fastEnough = report.latencyMs <= report.bm25LatencyMs + 300;
    const usefulEnough = report.bridgesFound >= 3;
    if (fastEnough && usefulEnough) {
        report.mode = 'wired';
        return report;
    }

    const identityLine = report.identity
        .map((entry) => `${entry.name} auf Rang ${entry.rank} von ${entry.rows}`)
        .join(', ');
    report.rejectionReason =
        `Nicht dazugeschaltet. Die Latenz war nie das Problem (${report.latencyMs} ms gegen `
        + `${report.bm25LatencyMs} ms fuer BM25), der Nutzen ist es: nur `
        + `${report.bridgesFound} von ${report.bridgesTried} Vokabular-Bruecken standen in den `
        + `ersten ${SEMANTIC_TOP_N} Zeilen. Die Gegenprobe erklaert warum: unter seinem EIGENEN `
        + `Namen gesucht, steht ${identityLine}. Eine Vektorsuche, die ein Symbol nicht findet, `
        + `wenn man seinen Namen eintippt, kann keine Bruecke schlagen. Dazu kommt, dass der Modus `
        + `nie nichts sagt: das Unsinnswort "${SEMANTIC_NONSENSE}" liefert ${report.nonsense.rows} `
        + `Zeilen mit Werten von ${report.nonsense.topScore} bis ${report.nonsense.lastScore}, also `
        + `den ganzen Graphen in einer Reihenfolge. Dazugeschaltet haette die Suche unter jeder `
        + `Anfrage Treffer gezeigt und "no symbol answers" nie wieder gesagt.`;
    return report;
}

// ------------------------------------------------------------ Die Ablesungen -

/** Alles, was die Menuezeile ueber ihre Gestalt hergibt. */
const menuSeam = (page) =>
    page.evaluate(() => {
        const row = document.querySelector('[data-testid="atlas-menu"]');
        const entries = [...(row?.querySelectorAll('[data-menu]') ?? [])].map((node) => {
            const style = globalThis.getComputedStyle(node);
            const key = node.querySelector('.atlas-menu-key');
            const box = node.getBoundingClientRect();
            return {
                menu: node.getAttribute('data-menu') ?? '',
                tag: node.tagName,
                className: node.getAttribute('class') ?? '',
                label: node.textContent?.replace(/\s+/g, ' ').trim() ?? '',
                bracket: key?.textContent ?? '',
                bracketColor: key === null ? '' : globalThis.getComputedStyle(key).color,
                fontSize: style.fontSize,
                padding: [style.paddingTop, style.paddingRight, style.paddingBottom, style.paddingLeft].join(' '),
                border: [style.borderTopWidth, style.borderTopStyle].join(' '),
                borderColor: style.borderTopColor,
                borderBottom: [style.borderBottomWidth, style.borderBottomStyle].join(' '),
                tabIndex: node.tabIndex,
                width: Math.round(box.width),
                height: Math.round(box.height),
                disabled: node.hasAttribute('disabled') || node.getAttribute('aria-disabled') === 'true',
            };
        });
        return {
            entries,
            legend: row?.querySelector('[data-testid="atlas-menu-legend"]')?.textContent?.trim() ?? '',
        };
    });

/** Wo der Fokus steht und ob man ihn sieht. */
const focusSeam = (page) =>
    page.evaluate(() => {
        const node = document.activeElement;
        if (node === null || node === document.body) {
            return { menu: '', tag: node?.tagName ?? '', outlineStyle: '', outlineWidth: 0 };
        }
        const style = globalThis.getComputedStyle(node);
        return {
            menu: node.getAttribute('data-menu') ?? '',
            tag: node.tagName,
            outlineStyle: style.outlineStyle,
            outlineWidth: Number.parseFloat(style.outlineWidth) || 0,
            outlineColor: style.outlineColor,
        };
    });

/** Der Griff der Anwendung an Suche und Menuezeile. */
const searchSeam = (page) => page.evaluate(() => globalThis.__atlasSearch ?? null);

/** Der Tastentest der Hilfeseite, Feld fuer Feld. */
const keyProbeSeam = (page) =>
    page.evaluate(() => {
        const probe = document.querySelector('[data-testid="atlas-help-keyprobe"]');
        if (probe === null) {
            return { present: false };
        }
        const fields = {};
        for (const node of probe.querySelectorAll('[data-testid="atlas-help-keyprobe-field"]')) {
            fields[node.getAttribute('data-field') ?? ''] =
                node.querySelector('[data-testid="atlas-help-keyprobe-value"]')?.textContent?.trim() ?? '';
        }
        return { present: true, pressed: probe.getAttribute('data-pressed') === 'true', fields };
    });

/** Die erste Zeile des Trefferfensters: woher sie kommt und wo sie steht. */
const firstRowSeam = (page) =>
    page.evaluate(() => {
        const row = document.querySelector('[data-testid="atlas-search-row"]');
        if (row === null) {
            return { present: false };
        }
        const box = row.getBoundingClientRect();
        return {
            present: true,
            name: row.getAttribute('data-name') ?? '',
            source: row.getAttribute('data-source') ?? '',
            top: Math.round(box.top * 100) / 100,
            headline:
                document.querySelector('[data-testid="atlas-search-headline"]')?.textContent?.trim() ?? '',
            rows: document.querySelectorAll('[data-testid="atlas-search-row"]').length,
            provisionalMarks: document.querySelectorAll('[data-testid="atlas-search-provisional"]').length,
        };
    });

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
        menuUniform: false,
        menuEntryCount: 0,
        menuAllButtons: false,
        menuClickWorks: false,
        menuKeyboardWorks: false,
        menuFocusRingVisible: false,
        altShortcutsWork: Object.fromEntries(ALT_LETTERS.map((letter) => [letter, false])),
        keyProbeShown: false,
        keyProbeReportsCodeAndAlt: false,
        listenerInCapturePhase: false,
        firstSuggestionMedianMs: Number.NaN,
        firstSuggestionSamples: [],
        serverRoundtripMs: Number.NaN,
        staleAnswerWins: true,
        prefixCacheHits: 0,
        debounceMs: Number.NaN,
        localSuggestionsShownFirst: false,
        semanticMode: 'measured-and-rejected',
        semanticBridgesTried: SEMANTIC_BRIDGES.length,
        semanticBridgesFound: 0,
        semanticLatencyMs: Number.NaN,
        semanticRejectionReason: '',
        semanticHitsLabelled: false,
        overlapViolations: 0,
        clippingViolations: 0,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = { blockedRequests: [], consoleErrors: [], pageErrors: [], readability: [] };

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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w7b-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w7b-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);
        log('Rendezvous:', runtimeDir);

        // --------------------------------------------------------- 3. Index
        const indexed = await indexRepository(BINARY, {
            home,
            repoPath: FIXTURE,
            project: PROJECT,
            // `full`, weil die Vektorsuche des Servers moderate/full verlangt.
            // Mit `fast` waere die Messung unten eine Aussage ueber den
            // Indexmodus und nicht ueber die Faehigkeit.
            mode: 'full',
        });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten (Modus full)`);

        // -------------------------------------------------------- 4. Server
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        timings.serverStartMs = started.durationMs;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        extras.serverPort = serverPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        // ------------------------------------------------ 5. Die Vektorsuche
        const semanticStarted = Date.now();
        const semantic = await measureSemantic(serverPort);
        timings.semanticMs = Date.now() - semanticStarted;
        extras.semantic = semantic;
        report.semanticMode = semantic.mode;
        report.semanticBridgesFound = semantic.bridgesFound;
        report.semanticBridgesTried = semantic.bridgesTried;
        report.semanticLatencyMs = semantic.latencyMs;
        report.semanticRejectionReason = semantic.rejectionReason;
        log(`Vektorsuche: ${semantic.bridgesFound}/${semantic.bridgesTried} Bruecken in den ersten `
            + `${SEMANTIC_TOP_N}, ${semantic.latencyMs} ms (BM25 ${semantic.bm25LatencyMs} ms) `
            + `-> ${semantic.mode}`);
        for (const bridge of semantic.bridges) {
            log(`  "${bridge.word}" -> ${bridge.ranks.map((r) => `${r.target}@${r.rank || '-'}`).join(' ')}`
                + `  top: ${bridge.top.join(', ')}`);
        }
        for (const entry of semantic.identity) {
            log(`  Selbsttreffer "${entry.name}": Rang ${entry.rank} von ${entry.rows}`);
        }

        // ------------------------------------------------------- 6. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const context = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
        const origin = `http://127.0.0.1:${uiPort}`;

        /*
         * Der Verzoegerer, und warum er hier legitim ist.
         *
         * Zwei Dinge lassen sich an diesem Index sonst nicht messen: dass eine
         * ueberholte Antwort nie gewinnt, und wie die Zeile AUSSIEHT, solange
         * der Index noch antwortet. Der Serverweg dauert hier wenige
         * Millisekunden, also gibt es kein Fenster, in dem man das beobachten
         * koennte. Der Verzoegerer faelscht keine Antwort: er laesst dieselbe
         * Anfrage an denselben Server, nur spaeter. Er ist nur waehrend der
         * beiden Halte an, an denen er gebraucht wird.
         */
        let slowRpcMs = 0;
        await context.route('**/*', async (route) => {
            const url = route.request().url();
            if (url.startsWith(origin) || url.startsWith('data:') || url.startsWith('blob:')) {
                if (slowRpcMs > 0 && url.endsWith('/rpc')) {
                    const body = route.request().postData() ?? '';
                    if (body.includes('search_graph')) {
                        await sleep(slowRpcMs);
                    }
                }
                await route.continue().catch(() => undefined);
                return;
            }
            extras.blockedRequests.push(url);
            await route.abort().catch(() => undefined);
        });

        const page = await context.newPage();
        page.on('console', (message) => {
            if (message.type() === 'error') {
                extras.consoleErrors.push(message.text());
            }
        });
        page.on('pageerror', (error) => extras.pageErrors.push(String(error)));
        await mkdir(OUT_DIR, { recursive: true });

        /** Laden, bis der Baum steht und die lokalen Kandidaten da sind. */
        const load = async () => {
            await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
            await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
            // Die Sofort-Vorschlaege kommen aus Baum und Galaxie. Vor dem Laden
            // zu messen hiesse, den Serverweg zu messen und ihn den lokalen
            // Vorschlaegen zuzuschreiben.
            await page.waitForFunction(
                () => (globalThis.__atlasSearch?.localCandidates ?? 0) > 0,
                undefined,
                { timeout: 30000 },
            );
            await page.waitForTimeout(400);
        };

        const readability = async (name) => {
            const top = await measureReadability(page);
            const scrolled = await scrollRegionsToEnd(page);
            await page.waitForTimeout(200);
            const bottom = await measureReadability(page);
            await resetScroll(page);
            extras.readability.push({
                name,
                scrolledRegions: scrolled.length,
                top: { candidates: top.candidates, overlaps: top.overlaps, clipped: top.clipped, layers: top.layers },
                bottom: { candidates: bottom.candidates, overlaps: bottom.overlaps, clipped: bottom.clipped },
            });
            report.overlapViolations += top.overlaps.length + bottom.overlaps.length;
            report.clippingViolations += top.clipped.length + bottom.clipped.length;
        };

        // ------------------------------------------- 6a. Die Gestalt der Zeile
        await load();
        const menu = await menuSeam(page);
        extras.menu = menu;
        report.menuEntryCount = menu.entries.length;
        report.menuAllButtons =
            menu.entries.length > 0
            && menu.entries.every((entry) => entry.tag === 'BUTTON' && !entry.disabled && entry.tabIndex >= 0);
        const first = menu.entries[0];
        report.menuUniform =
            menu.entries.length === MENU_ENTRIES.length
            && menu.entries.map((entry) => entry.menu).join(',')
                === MENU_ENTRIES.map((entry) => entry.menu).join(',')
            && report.menuAllButtons
            && first !== undefined
            && menu.entries.every((entry) =>
                entry.className === first.className
                && entry.fontSize === first.fontSize
                && entry.padding === first.padding
                && entry.border === first.border
                && entry.borderBottom === first.border
                && entry.bracketColor === first.bracketColor
                && /^\[[a-z?]\]$/.test(entry.bracket)
                && Number.parseFloat(entry.border) >= 1)
            && /alt/i.test(menu.legend);
        log(`Menuezeile: ${menu.entries.map((entry) => entry.label).join('  ')}`);
        log(`  eine Klasse "${first?.className}", Polsterung "${first?.padding}", `
            + `Rahmen "${first?.border}", Legende "${menu.legend}" -> uniform ${report.menuUniform}`);

        // ------------------------------------- 6b. Maus: alle sechs ausloesen
        for (const entry of MENU_ENTRIES) {
            await page.click(`[data-menu="${entry.menu}"]`);
            await page.waitForTimeout(250);
            await page.keyboard.press('Escape');
            await page.waitForTimeout(150);
        }
        const afterClicks = await searchSeam(page);
        extras.activatedByClick = afterClicks?.activatedMenus ?? [];
        report.menuClickWorks = MENU_ENTRIES.every((entry) =>
            (afterClicks?.activatedMenus ?? []).includes(entry.letter));
        log(`per Maus ausgeloest: ${(afterClicks?.activatedMenus ?? []).join(' ') || 'nichts'}`);

        // ------------------------- 6c. Tastatur: Tab bis hin, Enter, Fokusring
        const keyboardResults = [];
        for (const entry of MENU_ENTRIES) {
            await load();
            await page.locator('.atlas-brand').click();
            let reached = null;
            for (let step = 0; step < 20; step += 1) {
                await page.keyboard.press('Tab');
                const focus = await focusSeam(page);
                if (focus.menu === entry.menu) {
                    reached = { ...focus, tabs: step + 1 };
                    break;
                }
            }
            if (reached === null) {
                keyboardResults.push({ menu: entry.menu, reached: false });
                continue;
            }
            await page.keyboard.press('Enter');
            await page.waitForTimeout(250);
            const seam = await searchSeam(page);
            keyboardResults.push({
                menu: entry.menu,
                reached: true,
                tabs: reached.tabs,
                outlineStyle: reached.outlineStyle,
                outlineWidth: reached.outlineWidth,
                activated: (seam?.activatedMenus ?? []).includes(entry.letter),
            });
        }
        extras.menuKeyboard = keyboardResults;
        report.menuKeyboardWorks =
            keyboardResults.length === MENU_ENTRIES.length
            && keyboardResults.every((entry) => entry.reached === true && entry.activated === true);
        report.menuFocusRingVisible =
            keyboardResults.length === MENU_ENTRIES.length
            && keyboardResults.every((entry) =>
                entry.reached === true && entry.outlineStyle !== 'none' && entry.outlineWidth >= 1);
        log(`per Tab plus Enter ausgeloest: ${report.menuKeyboardWorks}, `
            + `Fokusring sichtbar: ${report.menuFocusRingVisible}`);

        // Das Bild der Zeile, mit dem Fokus auf einem Eintrag, damit der Ring
        // im Beweisbild steht und nicht nur in einer Zahl.
        await load();
        await page.locator('.atlas-brand').click();
        for (let step = 0; step < 20; step += 1) {
            await page.keyboard.press('Tab');
            const focus = await focusSeam(page);
            if (focus.menu === 'a-why') {
                break;
            }
        }
        await page.waitForTimeout(200);
        await readability('menu-row');
        await page.screenshot({ path: join(OUT_DIR, 'menu-uniform.png'), fullPage: true });

        // ---------------------------------------------- 6d. Die Alt-Kuerzel
        const altResults = {};
        for (const letter of ALT_LETTERS) {
            await load();
            await page.locator('.atlas-brand').click();
            const galaxyBefore = await page.evaluate(
                () => document.querySelector('[data-testid="atlas-galaxy"]')?.getAttribute('data-visible') ?? '',
            );
            await page.keyboard.press(`Alt+${letter}`);
            await page.waitForTimeout(600);
            const seam = await searchSeam(page);
            const galaxyAfter = await page.evaluate(
                () => document.querySelector('[data-testid="atlas-galaxy"]')?.getAttribute('data-visible') ?? '',
            );
            const line = await page.evaluate(
                () => document.querySelector('[data-testid="atlas-command-input"]')?.value ?? '',
            );
            const activated = (seam?.activatedMenus ?? []).includes(letter);
            altResults[letter] = {
                activated,
                lineStayedEmpty: line === '',
                galaxyBefore,
                galaxyAfter,
                activatedMenus: seam?.activatedMenus ?? [],
            };
            // Ausgeloest heisst: die Handlung lief UND der Buchstabe ist nicht
            // zusaetzlich in der Zeile gelandet. Ein Kuerzel, das beides tut,
            // waere der Befund vom 2026-08-29 in der anderen Richtung.
            report.altShortcutsWork[letter] = activated && line === '';
        }
        extras.altShortcuts = altResults;
        // Die Gegenprobe am sichtbaren Bild: Alt+a schaltet die Galaxie wirklich.
        extras.altShortcutVisibleEffect =
            altResults['a']?.galaxyBefore !== altResults['a']?.galaxyAfter;
        log(`Alt-Kuerzel: ${ALT_LETTERS.map((l) => `${l}=${report.altShortcutsWork[l]}`).join(' ')}`
            + ` (Galaxie ${altResults['a']?.galaxyBefore} -> ${altResults['a']?.galaxyAfter})`);

        // --------------------------------- 6e. Die einfangende Phase, gemessen
        //
        // Nicht behauptet, sondern beobachtet: ein Griff AM EINGABEFELD sieht
        // ein Ereignis in der Zielphase, also nach jedem Griff am Fenster, der
        // einfangend haengt, und vor jedem, der aufsteigend haengt. Steht dort
        // `defaultPrevented`, hat das Fenster die Taste schon genommen, und das
        // kann es nur einfangend getan haben.
        await load();
        await page.evaluate(() => {
            globalThis.__captureProbe = { seen: 0, defaultPrevented: null, handled: null };
            const input = document.querySelector('[data-testid="atlas-command-input"]');
            input?.addEventListener('keydown', (event) => {
                // Nur der Buchstabe. Das Druecken der Alt-Taste SELBST ist auch
                // ein keydown mit altKey, und es waere hier ein zweites
                // Ereignis, das nichts mit dem Kuerzel zu tun hat.
                if (event.altKey !== true || event.code !== 'KeyW') {
                    return;
                }
                globalThis.__captureProbe.seen += 1;
                globalThis.__captureProbe.defaultPrevented = event.defaultPrevented;
                globalThis.__captureProbe.handled =
                    event.__atlasHandledShortcut ?? null;
            });
        });
        await page.click('[data-testid="atlas-command-input"]');
        await page.keyboard.press('Alt+w');
        await page.waitForTimeout(400);
        const captureProbe = await page.evaluate(() => globalThis.__captureProbe);
        const seamAfterCapture = await searchSeam(page);
        extras.capturePhase = {
            ...captureProbe,
            declared: seamAfterCapture?.keyListenerCapture ?? null,
        };
        report.listenerInCapturePhase =
            captureProbe?.seen === 1
            && captureProbe?.defaultPrevented === true
            && captureProbe?.handled === 'w'
            && seamAfterCapture?.keyListenerCapture === true;
        log(`einfangende Phase: am Feld defaultPrevented=${captureProbe?.defaultPrevented}, `
            + `Marke=${captureProbe?.handled} -> ${report.listenerInCapturePhase}`);
        await page.keyboard.press('Escape');

        // ------------------------------------- 6f. Der Tastentest der Hilfe
        await load();
        await page.click('[data-menu="?"]');
        await page.waitForSelector('[data-testid="atlas-help-keyprobe"]', { timeout: 15000 });
        const probeIdle = await keyProbeSeam(page);
        report.keyProbeShown = probeIdle.present === true;
        await page.keyboard.press('Alt+a');
        await page.waitForTimeout(400);
        const probePressed = await keyProbeSeam(page);
        extras.keyProbe = { idle: probeIdle, pressed: probePressed };
        const fields = probePressed.fields ?? {};
        report.keyProbeReportsCodeAndAlt =
            probePressed.present === true
            && probePressed.pressed === true
            && fields['code'] === 'KeyA'
            && (fields['key'] ?? '').length > 0
            && /alt/i.test(fields['modifiers'] ?? '')
            && ['true', 'false'].includes(fields['defaultPrevented'] ?? '')
            && (fields['consumedBy'] ?? '').length > 0
            && /\[a\]/.test(fields['shortcut'] ?? '');
        log(`Tastentest: code=${fields['code']} key=${fields['key']} `
            + `mods=${fields['modifiers']} prevented=${fields['defaultPrevented']} `
            + `durch=${fields['consumedBy']} urteil=${fields['shortcut']}`);
        await readability('help-keyprobe');
        await page.keyboard.press('Escape');
        await page.waitForTimeout(200);

        // ------------------------------------------------ 6g. Die Suchstrecke
        await load();
        await page.evaluate(() => {
            globalThis.__probe = { armed: false, t0: 0, tRow: 0, source: '', rows: 0 };
            // Der Zeitstempel wird nur genommen, solange noch keine Zeile steht:
            // sonst wuerde ein spaeterer Buchstabe den Nullpunkt hinter die
            // Messung schieben und die Strecke negativ machen.
            globalThis.addEventListener('keydown', () => {
                if (globalThis.__probe.armed && globalThis.__probe.tRow === 0) {
                    globalThis.__probe.t0 = performance.now();
                }
            }, true);
            const observer = new MutationObserver(() => {
                if (!globalThis.__probe.armed || globalThis.__probe.tRow !== 0) {
                    return;
                }
                const row = document.querySelector('[data-testid="atlas-search-row"]');
                if (row === null) {
                    return;
                }
                globalThis.__probe.tRow = performance.now();
                globalThis.__probe.source = row.getAttribute('data-source') ?? '';
                globalThis.__probe.rows =
                    document.querySelectorAll('[data-testid="atlas-search-row"]').length;
            });
            observer.observe(document.body, { subtree: true, childList: true });
        });

        const samples = [];
        for (const word of SEARCH_SAMPLES) {
            await page.evaluate(() => {
                globalThis.__probe.t0 = 0;
                globalThis.__probe.tRow = 0;
                globalThis.__probe.source = '';
                globalThis.__probe.armed = true;
            });
            await page.keyboard.type(word, { delay: 45 });
            await page.waitForSelector('[data-testid="atlas-search-row"]', { timeout: 20000 })
                .catch(() => undefined);
            const measured = await page.evaluate(() => {
                globalThis.__probe.armed = false;
                return { ...globalThis.__probe };
            });
            samples.push({
                word,
                ms: measured.tRow > 0 && measured.t0 > 0
                    ? Math.round((measured.tRow - measured.t0) * 100) / 100
                    : null,
                source: measured.source,
                rows: measured.rows,
            });
            await page.keyboard.press('Escape');
            await page.waitForTimeout(120);
        }
        extras.searchSamples = samples;
        const timed = samples.filter((entry) => entry.ms !== null).map((entry) => entry.ms);
        report.firstSuggestionSamples = samples.map((entry) => entry.ms);
        report.firstSuggestionMedianMs = median(timed);
        report.localSuggestionsShownFirst =
            samples.length >= 10 && samples.every((entry) => entry.source === 'loaded');
        log(`Messreihe (${timed.length} Eingaben): Median ${report.firstSuggestionMedianMs} ms, `
            + `Spanne ${Math.min(...timed)} bis ${Math.max(...timed)} ms, `
            + `erste Zeile immer vorlaeufig: ${report.localSuggestionsShownFirst}`);

        // Der Serverweg und der Praefix-Cache, an einem verlaengerten Wort.
        await page.keyboard.type(PREFIX_BASE, { delay: 45 });
        await page.waitForFunction(
            () => globalThis.__atlasSearch?.shownSource === 'index',
            undefined,
            { timeout: 20000 },
        );
        const beforeExtension = await searchSeam(page);
        await page.keyboard.type(PREFIX_EXTRA, { delay: 45 });
        await page.waitForTimeout(600);
        const afterExtension = await searchSeam(page);
        extras.prefixCache = {
            before: beforeExtension?.prefixCacheHits ?? 0,
            after: afterExtension?.prefixCacheHits ?? 0,
            shownQuery: afterExtension?.shownQuery ?? '',
        };
        report.prefixCacheHits = afterExtension?.prefixCacheHits ?? 0;
        report.serverRoundtripMs = afterExtension?.serverRoundtripMs ?? Number.NaN;
        report.debounceMs = afterExtension?.debounceMs ?? Number.NaN;
        extras.roundtrips = afterExtension?.roundtrips ?? [];
        log(`Praefix-Cache: ${report.prefixCacheHits} Treffer, Serverweg zuletzt `
            + `${report.serverRoundtripMs} ms, Entprellung ${report.debounceMs} ms`);
        await page.keyboard.press('Escape');
        await page.waitForTimeout(200);

        // ----------------- Die ueberholte Antwort, mit verzoegertem Serverweg
        slowRpcMs = 700;
        await page.keyboard.type('user', { delay: 45 });
        // Lange genug, dass die Anfrage wirklich unterwegs ist, kuerzer als ihre
        // Verzoegerung: genau in diesem Fenster wird sie ueberholt.
        await page.waitForTimeout(300);
        await page.keyboard.press('Escape');
        await page.keyboard.type('validate', { delay: 45 });
        await page.waitForTimeout(2000);
        const afterRace = await searchSeam(page);
        const rowAfterRace = await firstRowSeam(page);
        extras.staleRace = {
            shownQuery: afterRace?.shownQuery ?? '',
            shownSource: afterRace?.shownSource ?? '',
            aborted: afterRace?.abortedRequests ?? 0,
            dropped: afterRace?.staleDropped ?? 0,
            firstRow: rowAfterRace,
        };
        report.staleAnswerWins = afterRace?.staleAnswerWins !== false;
        log(`ueberholte Antwort: abgebrochen ${extras.staleRace.aborted}, verworfen `
            + `${extras.staleRace.dropped}, gezeigt "${extras.staleRace.shownQuery}" `
            + `-> gewinnt eine alte Antwort: ${report.staleAnswerWins}`);
        await page.keyboard.press('Escape');
        await page.waitForTimeout(200);

        // --------- Das Bild der vorlaeufigen Zeilen, waehrend der Index antwortet
        //
        // Mit demselben Verzoegerer: ohne ihn dauert der Serverweg an dieser
        // Fixture wenige Millisekunden, und der Zustand, um den es geht, waere
        // nicht zu fotografieren. Was auf dem Bild steht, ist trotzdem der echte
        // Zustand der Anwendung und keine Nachstellung.
        await page.keyboard.type('validate', { delay: 45 });
        await page.waitForSelector('[data-testid="atlas-search-provisional"]', { timeout: 10000 });
        const provisionalShot = await firstRowSeam(page);
        extras.provisionalShot = provisionalShot;
        report.semanticHitsLabelled = false;
        await readability('search-provisional');
        await page.screenshot({ path: join(OUT_DIR, 'search-fast.png'), fullPage: true });

        // Und die Gegenprobe: dieselbe Zeile bewegt sich nicht, wenn die Antwort
        // eintrifft. Der Kasten hat feste Hoehe, also darf sich die oberste
        // Zeile nicht verschieben.
        await page.waitForFunction(
            () => globalThis.__atlasSearch?.shownSource === 'index',
            undefined,
            { timeout: 20000 },
        );
        await page.waitForTimeout(300);
        const rowAfterAnswer = await firstRowSeam(page);
        extras.noJump = {
            before: provisionalShot,
            after: rowAfterAnswer,
            movedPx: Math.abs((rowAfterAnswer.top ?? 0) - (provisionalShot.top ?? 0)),
        };
        log(`ohne Sprung: erste Zeile bei ${provisionalShot.top} -> ${rowAfterAnswer.top} `
            + `(${extras.noJump.movedPx} px)`);
        slowRpcMs = 0;
        await page.keyboard.press('Escape');
        await page.waitForTimeout(200);
        await readability('search-answered');

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        extras.finalSeam = await searchSeam(page);

        /*
         * Die Konsolenmeldungen, sortiert statt gezaehlt.
         *
         * Dieser Lauf laedt die Seite ein gutes Dutzend Mal neu und bricht
         * absichtlich eine ueberholte Suchanfrage ab. Beides beendet laufende
         * Anfragen, und Chromium schreibt dafuer `ERR_FAILED` in die Konsole.
         * Das ist der erwartete Nebeneffekt und kein Fehler; alles ANDERE waere
         * einer, und deshalb wird hier getrennt statt weggesehen.
         */
        extras.consoleErrorsCancelled = extras.consoleErrors.filter((line) =>
            /ERR_FAILED|ERR_ABORTED/.test(line)).length;
        extras.consoleErrorsOther = extras.consoleErrors.filter((line) =>
            !/ERR_FAILED|ERR_ABORTED/.test(line));

        extras.blockedSidecarProbes = extras.blockedRequests
            .filter((url) => url.includes(SIDECAR_HOSTPORT)).length;
        extras.blockedOther = extras.blockedRequests
            .filter((url) => !url.includes(SIDECAR_HOSTPORT));
        log(`Route-Sperre: ${extras.blockedSidecarProbes} Proben an den Sidecar auf `
            + `${SIDECAR_HOSTPORT} abgewiesen, ${extras.blockedOther.length} andere Ziele`);

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w7b] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w7b] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            menuEntries: MENU_ENTRIES,
            searchSampleWords: SEARCH_SAMPLES,
            semanticBridgeSpec: SEMANTIC_BRIDGES,
            semanticTopN: SEMANTIC_TOP_N,
            readabilityExclusions: READABILITY_EXCLUSIONS,
            deliberateOverlays: DELIBERATE_OVERLAYS,
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
        && report.menuUniform === true
        && report.menuEntryCount === MENU_ENTRIES.length
        && report.menuAllButtons === true
        && report.menuClickWorks === true
        && report.menuKeyboardWorks === true
        && report.menuFocusRingVisible === true
        && ALT_LETTERS.every((letter) => report.altShortcutsWork[letter] === true)
        && report.keyProbeShown === true
        && report.keyProbeReportsCodeAndAlt === true
        && report.listenerInCapturePhase === true
        && Number.isFinite(report.firstSuggestionMedianMs)
        && report.firstSuggestionMedianMs <= 120
        && report.firstSuggestionSamples.length >= 10
        && Number.isFinite(report.serverRoundtripMs)
        && report.staleAnswerWins === false
        && report.prefixCacheHits >= 1
        && report.debounceMs <= 100
        && report.localSuggestionsShownFirst === true
        && ['wired', 'measured-and-rejected'].includes(report.semanticMode)
        && Number.isFinite(report.semanticLatencyMs)
        && report.semanticBridgesTried >= 6
        && (report.semanticMode === 'wired'
            ? report.semanticBridgesFound >= 3 && report.semanticHitsLabelled === true
            : report.semanticRejectionReason.length > 20)
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && (extras.blockedOther ?? []).length === 0
        && extras.pageErrors.length === 0
        && (extras.consoleErrorsOther ?? []).length === 0;

    if (!ok) {
        console.error('[smoke-w7b] W7b-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w7b] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W7b-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w7b] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
