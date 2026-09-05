#!/usr/bin/env node
/*
 * W4d-Smoke: der Explorer zeigt die Datei-Wahrheit, und was fehlt, wird
 * gemessen statt vermutet.
 *
 * Was hier bewiesen wird, koennen die Unit-Tests nicht beweisen. Sie zeigen an
 * abgeschriebenen Antworten, dass der Join keine Stufe erfindet und dass ein
 * Konflikt zugunsten des Problems entschieden wird. Sie sagen nichts darueber,
 * WELCHE Liste der Server fuer eine kaputte, eine binaere und eine
 * gitignorierte Datei wirklich fuellt. Genau das ist die Frage dieses Laufs,
 * und sie wird beantwortet, indem die Faelle praepariert, indiziert, an der
 * Serverantwort abgelesen und danach im Baum wiedererkannt werden.
 *
 * Ablauf:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis
 *   3. eine KOPIE von fixtures/atlas-sample im Scratch, mit vier Praeparaten:
 *      saubere TS-Datei (unveraendert aus der Fixture), kaputte TS-Datei,
 *      Binaerdatei mit Nicht-Code-Endung, .gitignore samt gitignorierter Datei
 *   4. die Kopie indizieren (die Fixture selbst bleibt byte-identisch)
 *   5. C-Server auf einem freien Port >= 4320, dist/ auf einem zweiten
 *   6. die Wahrheit am Server ablesen: index_status, check_index_coverage
 *      (scopes und paths), /api/tree rekursiv
 *   7. Chromium ohne Aussenwelt, plus Route-Sperre
 *   8a. Baum aufklappen und Zeile fuer Zeile gegen das Gemessene halten
 *   8b. Galaxy-Legende: Eintraege, Klappen, Ueberleben des Reloads
 *   8c. Reader: partielle Datei mit Inhalt und Notiz, inhaltslose Datei mit
 *       ehrlicher Erklaerung statt Fehler
 *   8d. Frische: eine Datei nach dem Index aendern und die Notiz lesen
 *   9. abraeumen, Restprozesse zaehlen, JSON und Screenshot schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w4d).
 *
 * ## Drei Entscheidungen, die man sonst raten muesste
 *
 * **Praepariert wird nur an der Kopie.** fixtures/atlas-sample bleibt
 * byte-identisch; eine kaputte Datei in der Fixture waere ein Praeparat, das
 * jeder spaetere Lauf mitschleppt, ohne davon zu wissen.
 *
 * **Die Binaerdatei traegt feste Bytes.** Kein Zufall: derselbe Lauf soll
 * zweimal dieselbe Datei erzeugen, sonst waere die Groesse in der Antwort des
 * Servers von Lauf zu Lauf eine andere Zahl.
 *
 * **CBM_RUNTIME_DIR wird gesetzt.** Der Daemon des Servers und jede CLI
 * verabreden sich in einem Rendezvous-Verzeichnis, das per Konto und nicht per
 * HOME gilt: laeuft irgendwo sonst auf der Maschine eine CBM-Instanz mit einem
 * anderen Cache-Verzeichnis, lehnt jede CLI dieses Laufs ab, und der Lauf waere
 * nicht rot, sondern kaputt. Wortgleich mit tools/smoke-w4b.mjs.
 */

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
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
const PROJECT = 'codeatlasweb-w4d';
const OUT_DIR = join(ROOT, 'verification', 'w4');
const OUT_JSON = join(OUT_DIR, 'coverage.json');
const SHOT = join(OUT_DIR, 'explorer-coverage.png');
const MIN_PORT = 4320;

/** Die vier Praeparate, je mit dem Pfad, unter dem sie in der Kopie liegen. */
const CLEAN_FILE = 'src/services/userService.ts';
const BROKEN_FILE = 'src/broken.ts';
const BINARY_FILE = 'assets/beleg.png';
const IGNORED_FILE = 'ignored-note.ts';
/** Die Datei, die nach dem Index geaendert wird, um die Frische zu pruefen. */
const FRESHNESS_FILE = 'src/config.ts';

/**
 * Eine TypeScript-Datei mit echten Syntaxfehlern.
 *
 * Absichtlich unbalancierte Klammern in zwei Zeilen: tree-sitter faengt sich
 * danach wieder, und genau das ist der interessante Fall. Eine Datei, die von
 * der ersten Zeile an Muell ist, wuerde der Indexer moeglicherweise komplett
 * verwerfen, und dann waere gemessen, was mit Muell passiert, und nicht, was
 * mit einer Datei passiert, die jemand halb fertig gespeichert hat.
 */
const BROKEN_SOURCE = `// Deliberately broken for the W4d coverage measurement.
export function brokenGreeting(name: string): string {
    const parts = [ 'hello', name ;
    if (parts.length > {
        return parts.join(' '
    }
    return name;
}

export function stillHere(value: number): number {
    return value * 2;
}
`;

/** Chromium ohne Aussenwelt. Wortgleich mit smoke-w4c. */
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

const log = (...parts) => console.log('[smoke-w4d]', ...parts);
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

/**
 * Feste Bytes fuer die Binaerdatei: PNG-Signatur plus ein deterministisches
 * Muster. Kein Math.random, siehe Kopf.
 */
function fixedBinary(size = 4096) {
    const bytes = Buffer.alloc(size);
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]).copy(bytes, 0);
    for (let i = 8; i < size; i += 1) {
        bytes[i] = (i * 37 + 11) & 0xff;
    }
    return bytes;
}

/** Die Fixture-Kopie mit ihren Praeparaten. Die Fixture selbst wird nur gelesen. */
async function prepareCopy(scratch) {
    const copy = join(scratch, 'atlas-sample-w4d');
    await cp(FIXTURE, copy, { recursive: true });
    await writeFile(join(copy, BROKEN_FILE), BROKEN_SOURCE, 'utf8');
    await mkdir(join(copy, 'assets'), { recursive: true });
    await writeFile(join(copy, BINARY_FILE), fixedBinary());
    // Die gitignorierte Datei ist gueltiges TypeScript und wuerde ohne die
    // Regel indiziert. Nur so misst der Lauf die Regel und nicht die Endung.
    await writeFile(
        join(copy, IGNORED_FILE),
        'export function ignoredHelper(value: string): string {\n'
        + '    return value.trim();\n'
        + '}\n',
        'utf8',
    );
    await writeFile(join(copy, '.gitignore'), `${IGNORED_FILE}\n`, 'utf8');
    return copy;
}

/** Ein Werkzeug ueber POST /rpc rufen, direkt am Server und ohne Origin. */
async function rpc(port, tool, args) {
    const response = await fetch(`http://127.0.0.1:${port}/rpc`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            jsonrpc: '2.0',
            id: Date.now() % 100000,
            method: 'tools/call',
            params: { name: tool, arguments: { ...args, format: 'json' } },
        }),
    });
    const body = await response.json();
    const text = body?.result?.content?.[0]?.text;
    if (typeof text !== 'string') {
        throw new Error(`/rpc ${tool}: Antwort ohne result.content[0].text: ${JSON.stringify(body).slice(0, 300)}`);
    }
    if (body.result.isError === true) {
        throw new Error(`/rpc ${tool}: ${text.slice(0, 300)}`);
    }
    try {
        return JSON.parse(text);
    } catch {
        throw new Error(`/rpc ${tool}: Antworttext war kein JSON: ${text.slice(0, 300)}`);
    }
}

/** Den Graph-Baum rekursiv abfragen, so wie die Oberflaeche es tut. */
async function graphFiles(port, path = '', seen = new Set(), files = new Set(), dirs = new Set()) {
    if (seen.has(path)) {
        return { files, dirs };
    }
    seen.add(path);
    const query = new URLSearchParams({ project: PROJECT });
    if (path.length > 0) {
        query.set('path', path);
    }
    const response = await fetch(`http://127.0.0.1:${port}/api/tree?${query.toString()}`, {
        headers: { Accept: 'application/json' },
    });
    const body = await response.json();
    for (const child of Array.isArray(body?.children) ? body.children : []) {
        if (typeof child?.path !== 'string' || child.path === '{}' || child.path.length === 0) {
            continue;
        }
        if (child.kind === 'dir') {
            dirs.add(child.path);
            await graphFiles(port, child.path, seen, files, dirs);
        } else if (!dirs.has(child.path)) {
            files.add(child.path);
        }
    }
    return { files, dirs };
}

/** Alle Scope-Seiten holen, bis der Server keine mehr meldet. */
async function allScopeEntries(port) {
    const pages = [];
    let offset = 0;
    for (let page = 0; page < 20; page += 1) {
        const answer = await rpc(port, 'check_index_coverage', {
            project: PROJECT,
            scopes: ['.'],
            scope_limit: 1000,
            scope_offset: offset,
        });
        pages.push(answer);
        const scope = answer?.scopes?.[0];
        if (!scope || scope.has_more !== true) {
            break;
        }
        const next = typeof scope.next_offset === 'number' ? scope.next_offset : offset;
        if (next <= offset) {
            break;
        }
        offset = next;
    }
    const entries = [];
    for (const answer of pages) {
        for (const entry of answer?.scopes?.[0]?.entries ?? []) {
            entries.push(entry);
        }
    }
    return { pages, entries, metadata: pages[pages.length - 1]?.metadata ?? {} };
}

// ------------------------------------------------------------- Browser ------

/** Der Coverage-Griff der Anwendung. */
const coverageSeam = (page) =>
    page.evaluate(() => ({
        rows: globalThis.__atlasCoverage?.rows ?? [],
        records: globalThis.__atlasCoverage?.records ?? [],
        truncations: globalThis.__atlasCoverage?.truncations ?? [],
        counts: globalThis.__atlasCoverage?.counts ?? {},
        metadata: globalThis.__atlasCoverage?.metadata ?? {},
        open: globalThis.__atlasCoverage?.open,
        error: globalThis.__atlasCoverage?.error ?? '',
    }));

/** Was der Baum wirklich zeigt, mit den Werten, die der Browser malt. */
const treeSeam = (page) =>
    page.evaluate(() => ({
        rows: [...document.querySelectorAll('[data-testid="atlas-tree-row"][data-path]')].map((row) => {
            const style = globalThis.getComputedStyle(row);
            const mark = row.querySelector('[data-testid="atlas-tree-mark"]');
            return {
                path: row.getAttribute('data-path'),
                kind: row.getAttribute('data-kind'),
                coverage: row.getAttribute('data-coverage'),
                expanded: row.getAttribute('data-expanded'),
                title: row.getAttribute('title') ?? '',
                mark: mark?.textContent ?? '',
                markColor: mark === null ? '' : globalThis.getComputedStyle(mark).color,
                opacity: Number(style.opacity),
                color: style.color,
            };
        }),
        legend: {
            present: document.querySelector('[data-testid="atlas-tree-legend"]') !== null,
            entries: [...document.querySelectorAll('[data-testid="atlas-tree-legend-entry"]')]
                .map((entry) => entry.textContent?.replace(/\s+/g, ' ').trim() ?? ''),
            source: document.querySelector('[data-testid="atlas-tree-legend-source"]')?.textContent ?? '',
        },
        truncationLines: [...document.querySelectorAll('[data-testid="atlas-tree-truncation"]')]
            .map((line) => line.textContent ?? ''),
        note: document.querySelector('.atlas-tree-note')?.textContent ?? '',
    }));

/** Alles, was die Galaxy-Legende gerade zeigt. */
const galaxySeam = (page) =>
    page.evaluate(() => {
        const toggle = document.querySelector('[data-testid="atlas-galaxy-legend-toggle"]');
        return {
            togglePresent: toggle !== null,
            expanded: toggle?.getAttribute('aria-expanded') ?? '',
            arrow: toggle?.textContent?.trim() ?? '',
            present: document.querySelector('[data-testid="atlas-galaxy-legend"]') !== null,
            entries: [...document.querySelectorAll('[data-testid="atlas-galaxy-legend-entry"]')]
                .map((entry) => ({
                    key: entry.getAttribute('data-entry'),
                    text: entry.textContent?.replace(/\s+/g, ' ').trim() ?? '',
                })),
            swatches: [...document.querySelectorAll('[data-testid="atlas-galaxy-legend-swatch"]')]
                .map((swatch) => ({
                    type: swatch.getAttribute('data-type'),
                    color: swatch.getAttribute('data-color'),
                    painted: globalThis.getComputedStyle(
                        swatch.querySelector('.atlas-galaxy-legend-dot'),
                    ).backgroundColor,
                })),
        };
    });

/** Was der Reader gerade zeigt: Lage, Platzhalter und die Notiz darueber. */
const readerSeam = (page) =>
    page.evaluate(() => {
        const placeholder = document.querySelector('[data-testid="atlas-reader-placeholder"]');
        const note = document.querySelector('[data-testid="atlas-coverage-note"]');
        return {
            status: globalThis.__atlasReader?.status ?? '',
            path: globalThis.__atlasReader?.document?.path ?? '',
            lines: (globalThis.__atlasReader?.value?.() ?? '').split('\n').length,
            hasContent: (globalThis.__atlasReader?.value?.() ?? '').length > 0,
            placeholderState: placeholder?.getAttribute('data-state') ?? '',
            placeholder: placeholder?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            coverageNote: note?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            coverageNoteState: note?.getAttribute('data-coverage') ?? '',
            statusCoverage:
                document.querySelector('[data-chip="coverage"]')?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
        };
    });

/** Bis in die letzte Ebene aufklappen, ueber genau den Klick, den ein Leser tut. */
async function expandAll(page) {
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
}

/** Die Seite laden und warten, bis Baum und Coverage-Join beide da sind. */
async function openApp(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
    await page.waitForFunction(
        () => (globalThis.__atlasCoverage?.records?.length ?? -1) >= 0
            && !/joining the coverage lists/.test(
                document.querySelector('.atlas-tree-note')?.textContent ?? 'joining the coverage lists',
            ),
        undefined,
        { timeout: 40000 },
    );
}

/** Eine Datei im Baum anklicken und warten, bis der Reader eine Lage hat. */
async function openInTree(page, path) {
    await page.click(`[data-testid="atlas-tree-row"][data-path="${path}"]`);
    await page.waitForFunction(
        (expected) => {
            const seam = globalThis.__atlasReader;
            if (seam === undefined) {
                return false;
            }
            if (seam.status === 'unavailable' || seam.status === 'failed') {
                return true;
            }
            return seam.status === 'ready' && seam.document?.path === expected;
        },
        path,
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
    let scratch = null;
    let serverPort = 0;
    let uiPort = 0;
    let failure = null;
    const timings = {};

    const result = {
        treeShowsAllDiscovered: false,
        states: [],
        partialOrSkippedVisible: false,
        dimmedNotIndexed: false,
        legendShown: false,
        brokenFileHonest: false,
        freshnessNoteWorks: false,
        undiscoveredGap: '',
        galaxyLegendToggles: false,
        galaxyLegendEntries: 0,
        galaxyLegendStatePersists: false,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = { blockedRequests: [], consoleErrors: [], pageErrors: [], measured: {} };

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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w4d-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w4d-run-');
        scratch = await mkdtemp('/private/tmp/codeatlasweb-w4d-fix-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);
        log('Rendezvous:', runtimeDir);

        // ------------------------------------------ 3. Die praeparierte Kopie
        const repo = await prepareCopy(scratch);
        extras.fixtureCopy = repo;
        extras.prepared = {
            clean: CLEAN_FILE,
            broken: BROKEN_FILE,
            binary: BINARY_FILE,
            gitignored: IGNORED_FILE,
            gitignoreLine: IGNORED_FILE,
        };
        log('Fixture-Kopie:', repo);

        // -------------------------------------------------------- 4. Index
        const indexed = await indexRepository(BINARY, { home, repoPath: repo, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges, skipped: indexed.skipped_count };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // -------------------------------------------------------- 5. Server
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        timings.serverStartMs = started.durationMs;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        result.port = uiPort;
        extras.serverPort = serverPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        // --------------------------------- 6. Die Wahrheit am Server ablesen
        const status = await rpc(serverPort, 'index_status', { project: PROJECT });
        const scopes = await allScopeEntries(serverPort);
        const { files: graphFileSet, dirs: graphDirSet } = await graphFiles(serverPort);

        const measured = {
            parsePartial: status?.parse_partial ?? null,
            skipped: status?.skipped ?? null,
            notIndexed: status?.not_indexed ?? null,
            scopeEntries: scopes.entries,
            scopeMetadata: scopes.metadata,
            scopePages: scopes.pages.length,
            graphFiles: [...graphFileSet].sort(),
            graphDirs: [...graphDirSet].sort(),
        };
        extras.measured = measured;

        /** In welcher Liste ein Pfad steht. Die Antwort auf die Kernfrage. */
        const whereIs = (path) => {
            const found = [];
            if (measured.graphFiles.includes(path)) {
                found.push('graph');
            }
            if ((measured.parsePartial?.files ?? []).some((entry) => entry.path === path)) {
                found.push('index_status.parse_partial');
            }
            if ((measured.skipped?.files ?? []).some((entry) => entry.path === path)) {
                found.push('index_status.skipped');
            }
            if ((measured.notIndexed?.files ?? []).some((entry) => entry.path === path)) {
                found.push('index_status.not_indexed.files');
            }
            if ((measured.notIndexed?.dirs ?? []).includes(path)) {
                found.push('index_status.not_indexed.dirs');
            }
            for (const entry of measured.scopeEntries) {
                if (entry.path === path) {
                    found.push(`check_index_coverage.scope(kind=${entry.kind})`);
                }
            }
            return found;
        };
        extras.whereIs = {
            [CLEAN_FILE]: whereIs(CLEAN_FILE),
            [BROKEN_FILE]: whereIs(BROKEN_FILE),
            [BINARY_FILE]: whereIs(BINARY_FILE),
            [IGNORED_FILE]: whereIs(IGNORED_FILE),
        };
        log('gemessen: broken ->', JSON.stringify(extras.whereIs[BROKEN_FILE]));
        log('gemessen: binary ->', JSON.stringify(extras.whereIs[BINARY_FILE]));
        log('gemessen: gitignored ->', JSON.stringify(extras.whereIs[IGNORED_FILE]));

        // Die ausdrueckliche Pfad-Abfrage zur gitignorierten Datei (AC5).
        const ignoredPathAnswer = await rpc(serverPort, 'check_index_coverage', {
            project: PROJECT,
            paths: [IGNORED_FILE],
        });
        extras.ignoredPathAnswer = ignoredPathAnswer?.paths?.[0] ?? null;
        extras.ignoredMetadata = {
            ignored_files_stored: ignoredPathAnswer?.metadata?.ignored_files_stored,
            ignored_files_total: ignoredPathAnswer?.metadata?.ignored_files_total,
            recording_status: ignoredPathAnswer?.metadata?.recording_status,
            generation_matches: ignoredPathAnswer?.metadata?.generation_matches,
        };

        // ------------------------------------------------------- 7. Browser
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

        // --------------------------------------------- 8a. Der Baum im Bild
        await openApp(page, origin);
        const rounds = await expandAll(page);
        const tree = await treeSeam(page);
        const seam = await coverageSeam(page);
        extras.expandRounds = rounds;
        extras.tree = tree;
        extras.seam = { ...seam, rows: seam.rows.length };
        extras.seamRecords = seam.records;

        const shownPaths = new Set(tree.rows.map((row) => row.path));
        const expectedPaths = new Set([
            ...measured.graphFiles,
            ...(measured.parsePartial?.files ?? []).map((entry) => entry.path),
            ...(measured.skipped?.files ?? []).map((entry) => entry.path),
            ...(measured.notIndexed?.files ?? []).map((entry) => entry.path),
            ...(measured.notIndexed?.dirs ?? []),
            ...measured.scopeEntries.map((entry) => entry.path),
        ]);
        const missing = [...expectedPaths].filter((path) => !shownPaths.has(path));
        result.treeShowsAllDiscovered = missing.length === 0 && expectedPaths.size > 0;
        extras.missingFromTree = missing;
        log(`Baum: ${tree.rows.length} Zeilen, ${expectedPaths.size} erwartete Pfade, `
            + `${missing.length} fehlen`);

        result.states = [...new Set(tree.rows.map((row) => row.coverage))].sort();
        log('Stufen im Baum:', JSON.stringify(result.states));

        // Die kaputte Datei muss die Stufe tragen, die der Server ihr gegeben
        // hat, und ein Zeichen zeigen. Welche Stufe das ist, entscheidet der
        // Server; dieser Lauf schreibt sie mit.
        const brokenRow = tree.rows.find((row) => row.path === BROKEN_FILE);
        const binaryRow = tree.rows.find((row) => row.path === BINARY_FILE);
        const ignoredRow = tree.rows.find((row) => row.path === IGNORED_FILE);
        extras.rows = { broken: brokenRow, binary: binaryRow, ignored: ignoredRow };
        result.partialOrSkippedVisible =
            brokenRow !== undefined
            && (brokenRow.coverage === 'partial' || brokenRow.coverage === 'skipped')
            && brokenRow.mark.length > 0
            && brokenRow.title.length > 0;

        // Gemessen wird an Dateien, nicht an Ordnern: ein Ordner traegt die
        // schlechteste Stufe seines Inhalts und bleibt normal hell, weil sie
        // von einer Datei unter vielen kommen kann. Sein Punkt sagt genug.
        const dimmable = tree.rows.filter((row) =>
            row.kind === 'file'
            && (row.coverage === 'skipped' || row.coverage === 'not-indexed' || row.coverage === 'ignored'));
        const folders = tree.rows.filter((row) => row.kind === 'dir' && row.coverage !== 'indexed');
        result.dimmedNotIndexed = dimmable.length > 0 && dimmable.every((row) => row.opacity < 1);
        extras.dimmed = dimmable.map((row) => ({ path: row.path, coverage: row.coverage, opacity: row.opacity }));
        extras.markedFolders = folders.map((row) =>
            ({ path: row.path, coverage: row.coverage, mark: row.mark, opacity: row.opacity }));
        if (dimmable.length === 0) {
            extras.dimmedNotIndexedNote =
                'no file row carried skipped, not-indexed or ignored in this run, so nothing could be dimmed';
        }

        result.legendShown =
            tree.legend.present
            && tree.legend.entries.length >= 2
            && tree.legend.source.includes('files it never met are invisible');
        log(`Legende: ${tree.legend.entries.length} Stufen, Quellensatz ${result.legendShown}`);

        // --------------------------------------------- 8b. Die Galaxy-Legende
        await page.waitForSelector('[data-testid="atlas-galaxy-legend-toggle"]', { timeout: 30000 });
        await page.waitForFunction(
            () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
            undefined,
            { timeout: 40000 },
        ).catch(() => undefined);
        const legendOpen = await galaxySeam(page);
        result.galaxyLegendEntries = legendOpen.entries.length;
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(300);
        const legendClosed = await galaxySeam(page);
        result.galaxyLegendToggles =
            legendOpen.present
            && legendOpen.expanded === 'true'
            && legendOpen.arrow.startsWith('▾')
            && !legendClosed.present
            && legendClosed.expanded === 'false'
            && legendClosed.arrow.startsWith('▸');
        extras.galaxyLegend = { open: legendOpen, closed: legendClosed };
        log(`Galaxy-Legende: ${result.galaxyLegendEntries} Eintraege, `
            + `${legendOpen.swatches.length} Kantenfarben, Klappen ${result.galaxyLegendToggles}`);

        // Zugeklappt neu laden: der Zustand muss den Reload ueberleben.
        await openApp(page, origin);
        const afterReload = await galaxySeam(page);
        result.galaxyLegendStatePersists = !afterReload.present && afterReload.expanded === 'false';
        extras.galaxyLegend.afterReload = afterReload;
        log(`Galaxy-Legende nach Reload: offen=${afterReload.present}`);

        // Wieder aufklappen und noch einmal laden, damit das Beweisbild die
        // Legende zeigt und zugleich beweist, dass auch "offen" ueberlebt.
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(300);
        await openApp(page, origin);
        const afterSecondReload = await galaxySeam(page);
        extras.galaxyLegend.afterSecondReload = afterSecondReload;
        result.galaxyLegendStatePersists =
            result.galaxyLegendStatePersists && afterSecondReload.present;

        // ------------------------------------------------ 8c. Der Reader
        await expandAll(page);
        await openInTree(page, BROKEN_FILE);
        const brokenReader = await readerSeam(page);
        extras.reader = { broken: brokenReader };
        const brokenStage = brokenRow?.coverage ?? 'indexed';
        // Was ehrlich heisst, haengt an der gemessenen Stufe: eine partielle
        // Datei muss ihren Inhalt UND die Notiz zeigen, eine ohne Inhalt die
        // Erklaerung statt eines Fehlers.
        const brokenPartialOk =
            brokenReader.status === 'ready'
            && brokenReader.hasContent
            && brokenReader.coverageNote.includes('partially parsed');
        const brokenEmptyOk =
            brokenReader.status === 'unavailable'
            && brokenReader.placeholder.includes('get_code_snippet')
            && brokenReader.placeholder.includes('Upstream-Ask 1');
        const brokenHonest = brokenStage === 'partial' ? brokenPartialOk : brokenEmptyOk;
        log(`Reader auf ${BROKEN_FILE} (Stufe ${brokenStage}): status=${brokenReader.status}, `
            + `Notiz="${brokenReader.coverageNote.slice(0, 80)}"`);

        // Eine Datei ohne Inhalt: die ehrliche Erklaerung statt eines Fehlers.
        const emptyPath = ignoredRow !== undefined ? IGNORED_FILE
            : binaryRow !== undefined ? BINARY_FILE
                : undefined;
        let emptyReader = null;
        if (emptyPath !== undefined) {
            await openInTree(page, emptyPath);
            emptyReader = await readerSeam(page);
            extras.reader.empty = { path: emptyPath, ...emptyReader };
            log(`Reader auf ${emptyPath}: status=${emptyReader.status}, `
                + `Platzhalter="${emptyReader.placeholder.slice(0, 90)}"`);
        }
        const emptyHonest =
            emptyReader !== null
            && emptyReader.status === 'unavailable'
            && emptyReader.placeholderState === 'unavailable'
            && emptyReader.placeholder.includes('get_code_snippet')
            && !/failed|error/i.test(emptyReader.placeholder);
        result.brokenFileHonest = brokenHonest && emptyHonest;

        // ------------------------------------ Das Beweisbild mit den Stufen
        await openInTree(page, BROKEN_FILE);
        await page.evaluate(() => {
            document.querySelector('[data-testid="atlas-tree"]')?.scrollIntoView({ block: 'start' });
            // Der Cursor hat die Liste beim Oeffnen mitgezogen. Fuer das Bild
            // zaehlt der Anfang des Baums, sonst fehlt die oberste Stufe.
            const list = document.querySelector('.atlas-tree-list');
            if (list !== null) {
                list.scrollTop = 0;
            }
        });
        await page.waitForTimeout(300);
        await page.screenshot({ path: SHOT, fullPage: true });
        log('explorer-coverage.png geschrieben');

        // ---------------------------------------------------- 8d. Die Frische
        await openInTree(page, FRESHNESS_FILE);
        const beforeChange = await readerSeam(page);
        const original = await readFile(join(repo, FRESHNESS_FILE), 'utf8');
        await writeFile(
            join(repo, FRESHNESS_FILE),
            `${original}\n// changed after the index run, for the W4d freshness measurement\n`,
            'utf8',
        );
        // Weg von der Datei und zurueck: die Frische wird beim Oeffnen gefragt,
        // und ein zweites Mal auf dieselbe Datei zu klicken fragt nicht neu.
        await openInTree(page, CLEAN_FILE);
        await openInTree(page, FRESHNESS_FILE);
        const afterChange = await readerSeam(page);
        const afterSeam = await coverageSeam(page);
        extras.freshness = {
            file: FRESHNESS_FILE,
            before: { chip: beforeChange.statusCoverage },
            after: { chip: afterChange.statusCoverage, open: afterSeam.open },
        };
        result.freshnessNoteWorks =
            afterChange.statusCoverage.length > 0
            && /metadata_changed/.test(afterChange.statusCoverage)
            && afterSeam.open?.freshness === 'metadata_changed';
        log(`Frische: vorher "${beforeChange.statusCoverage}", nachher "${afterChange.statusCoverage}"`);

        // ------------------------------------------- Die gemessene Aussage
        const ignoredSources = extras.whereIs[IGNORED_FILE];
        const ignoredAnswer = extras.ignoredPathAnswer;
        const meta = extras.ignoredMetadata;
        result.undiscoveredGap = ignoredSources.length === 0
            ? `gitignored file "${IGNORED_FILE}" appears nowhere: not in the graph, not in any `
              + 'index_status list, not among the coverage scope entries. '
              + `check_index_coverage(paths) answers status "${ignoredAnswer?.status ?? 'none'}", `
              + `freshness "${ignoredAnswer?.freshness ?? 'none'}"; metadata says `
              + `ignored_files_stored=${meta.ignored_files_stored}, ignored_files_total=${meta.ignored_files_total}. `
              + 'The explorer therefore cannot show it, and the legend says so.'
            : `gitignored file "${IGNORED_FILE}" is listed by the server: ${ignoredSources.join(', ')}. `
              + `check_index_coverage(paths) answers status "${ignoredAnswer?.status ?? 'none'}", `
              + `freshness "${ignoredAnswer?.freshness ?? 'none'}", `
              + `recommended_action "${ignoredAnswer?.recommended_action ?? 'none'}"; metadata says `
              + `ignored_files_stored=${meta.ignored_files_stored}, ignored_files_total=${meta.ignored_files_total}, `
              + `recording_status "${meta.recording_status}". `
              + `The explorer shows it as "${ignoredRow?.coverage ?? 'not shown'}", `
              + 'so a file excluded by .gitignore is NOT an undiscovered gap on this server; '
              + 'the remaining gap is a file the discovery never walked at all, which no source names.';
        log('undiscoveredGap:', result.undiscoveredGap.slice(0, 160));

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w4d] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w4d] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
    result.leftoverProcesses = leftovers.reduce((sum, value) => sum + value, 0);
    log('leftoverProcesses:', result.leftoverProcesses);

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...result,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen; praepariert wurde eine Scratch-Kopie)',
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
        && result.treeShowsAllDiscovered === true
        && new Set(result.states).size >= 2
        && result.partialOrSkippedVisible === true
        && result.dimmedNotIndexed === true
        && result.legendShown === true
        && result.brokenFileHonest === true
        && result.freshnessNoteWorks === true
        && result.undiscoveredGap.length > 10
        && result.galaxyLegendToggles === true
        && result.galaxyLegendEntries >= 3
        && result.galaxyLegendStatePersists === true
        && result.port >= MIN_PORT
        && result.leftoverProcesses === 0
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w4d] W4d-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w4d] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        if (scratch) {
            console.error('[smoke-w4d] Fixture-Kopie bleibt liegen:', scratch);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir, scratch]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W4d-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w4d] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
