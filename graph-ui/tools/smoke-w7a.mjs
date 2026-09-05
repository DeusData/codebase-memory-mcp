#!/usr/bin/env node
/*
 * W7a-Smoke: die entkernte Menuezeile, die eingebaute Hilfeseite, die
 * ehrlichen Twin-Saetze und die Kommandozeile, die Tippen wirklich annimmt.
 *
 * Was hier bewiesen wird, kann kein Unit-Test beweisen. Die Unit-Tests zeigen,
 * dass der Katalog keine Attrappe mehr traegt, dass die Hilfe ihre sieben
 * Abschnitte in der richtigen Reihenfolge baut und dass ein blanker Buchstabe
 * kein Kuerzel mehr ist. Sie sagen nichts darueber, ob im gebauten Programm
 * wirklich nur noch verdrahtete Punkte in der Zeile stehen, ob ein Klick auf
 * [?]help die Seite aufschlaegt, ob die Taste `?` dasselbe tut, ob Escape sie
 * schliesst, ob sich in ihr etwas ueberlagert oder aus seinem Kasten ragt, und
 * vor allem: ob das Wort "create", ohne vorher zu klicken getippt, in der
 * Kommandozeile ankommt, statt ueber sein `c` ein Panel aufzuschlagen. Genau
 * das war der Nutzerbefund vom 2026-08-29.
 *
 * Ablauf:
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4370, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *   6a. Menuezeile lesen: was steht da, ist alles verdrahtet, gibt es noch
 *       irgendwo einen "not wired"-Tooltip
 *   6b. Hilfe per Klick, Abschnitte, Grenzen, Tastentabelle, Netz-Links,
 *       Farbe, Lesbarkeit (auch durchgescrollt), Bild
 *   6c. Hilfe per Taste `?`, zu per Escape
 *   6d. Die Kommandozeile: tippen ohne Klick, kein Panel dabei, Fokus sichtbar,
 *       Fokustaste, Ein-Zeichen-Hinweis, kein nativer Tooltip
 *   6e. Der Twin: die beiden Linsen aufschalten und ihre Saetze lesen
 *   7. abraeumen, Restprozesse zaehlen, verification/w7/help.json schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w7a).
 *
 * ## Warum die verdrahtete Tastenliste aus dem QUELLTEXT gelesen wird
 *
 * `helpListsEveryShortcut` vergleicht die Tabelle der Hilfe nicht mit einer
 * Liste, die in dieser Datei steht: das waere ein dritter Katalog und damit die
 * dritte Gelegenheit, dass etwas auseinanderlaeuft. Gelesen wird die Verdrahtung
 * selbst (src/app/shortcuts.ts, src/app/keyboard.ts, tour-player.ts,
 * overlay-model.ts), und verglichen wird sie mit dem, was im Browser wirklich
 * in der Tabelle steht.
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
const PROJECT = 'codeatlasweb-w7a';
const OUT_DIR = join(ROOT, 'verification', 'w7');
const OUT_JSON = join(OUT_DIR, 'help.json');
const MIN_PORT = 4370;

/** Das Symbol, an dem die beiden Twin-Linsen gelesen werden. */
const TARGET = 'createUser';

/** Das Wort aus dem Nutzerbefund: sein `c` schlug die Aenderungsansicht auf. */
const TYPED_WORD = 'create';

/** Die drei Verbote, die woertlich in der Hilfe stehen muessen. */
const LIMIT_PHRASES = ['read-only', 'cannot edit', 'cannot run', 'no terminal', 'no cloud'];

/** Die Flaechen, deren Auftauchen ein "ein Panel ging auf" waere. */
const PANEL_TESTIDS = [
    'atlas-impact',
    'atlas-bugwizard',
    'atlas-entry',
    'atlas-flow-overlay',
    'atlas-help',
];

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

const log = (...parts) => console.log('[smoke-w7a]', ...parts);
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

/** Die `case`-Tasten einer Absichtsfunktion, aus ihrem Quelltext. */
function casesOf(source, functionName) {
    const start = source.indexOf(`export function ${functionName}(`);
    if (start < 0) {
        throw new Error(`${functionName} nicht gefunden`);
    }
    const end = source.indexOf('\n}', start);
    const body = source.slice(start, end);
    const keys = [...body.matchAll(/case '([^']+)':/g)].map((match) => match[1]);
    return [...new Set(keys.map((key) => (key.length === 1 ? key.toLowerCase() : key)))].sort();
}

/**
 * Die verdrahteten Tasten, aus der Verdrahtung selbst gelesen.
 *
 * Drei Quellen, so wie es im Programm drei sind. Die Menuezeile fuehrt ihre
 * Buchstaben als Liste (dort entstehen sie), die beiden anderen Bereiche
 * entscheiden je Taste in einer Funktion, und deren `case`-Zweige sind die
 * Liste.
 */
function wiredKeysFromSource() {
    const shortcuts = readFileSync(join(ROOT, 'src', 'app', 'shortcuts.ts'), 'utf8');
    const declared = /WIRED_MENU_SHORTCUTS: readonly string\[\] = \[([^\]]*)\]/.exec(shortcuts)?.[1] ?? '';
    const menu = declared
        .split(',')
        .map((word) => word.trim().replace(/^'|'$/g, ''))
        .filter((word) => word.length > 0);
    const keyboard = readFileSync(join(ROOT, 'src', 'app', 'keyboard.ts'), 'utf8');
    const focusKey = /FOCUS_COMMAND_KEY = '([^']+)'/.exec(keyboard)?.[1] ?? '';
    const player = readFileSync(join(ROOT, 'src', 'tours', 'tour-player.ts'), 'utf8');
    const overlay = readFileSync(join(ROOT, 'src', 'search', 'overlay-model.ts'), 'utf8');
    return {
        menu: [...menu, focusKey].sort(),
        walk: casesOf(player, 'playerIntent'),
        search: casesOf(overlay, 'overlayIntent'),
    };
}

/** Alles, was die Menuezeile gerade zeigt. */
const menuSeam = (page) =>
    page.evaluate(() => {
        const row = document.querySelector('[data-testid="atlas-menu"]');
        const items = [...(row?.querySelectorAll('[data-menu]') ?? [])].map((node) => ({
            menu: node.getAttribute('data-menu') ?? '',
            label: node.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            state: node.getAttribute('data-state') ?? '',
            disabled: node.getAttribute('aria-disabled') === 'true' || node.hasAttribute('disabled'),
            title: node.getAttribute('title') ?? '',
            tag: node.tagName,
        }));
        const titles = [...document.querySelectorAll('[title]')].map((node) => node.getAttribute('title') ?? '');
        return {
            items,
            legend: row?.querySelector('[data-testid="atlas-menu-legend"]')?.textContent?.trim() ?? '',
            titles,
        };
    });

/** Alles, was die Hilfeseite gerade zeigt, in einem Zug abgelesen. */
const helpSeam = (page) =>
    page.evaluate(() => {
        const page_ = document.querySelector('[data-testid="atlas-help"]');
        if (page_ === null) {
            return { present: false };
        }
        const style = globalThis.getComputedStyle(page_);
        const parts = /rgba?\(([^)]+)\)/.exec(style.backgroundColor);
        const channels = parts === null ? [] : parts[1].split(',').map((value) => Number(value.trim()));
        const luminance = channels.length >= 3
            ? (0.299 * channels[0] + 0.587 * channels[1] + 0.114 * channels[2]) / 255
            : 1;
        const text = page_.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        return {
            present: true,
            sections: [...page_.querySelectorAll('[data-testid="atlas-help-section"]')]
                .map((node) => node.getAttribute('data-section') ?? ''),
            shortcuts: [...page_.querySelectorAll('[data-testid="atlas-help-shortcut"]')].map((node) => ({
                scope: node.getAttribute('data-scope') ?? '',
                key: node.getAttribute('data-key') ?? '',
                label: node.querySelector('td')?.textContent?.trim() ?? '',
                does: [...node.querySelectorAll('td')][2]?.textContent?.trim() ?? '',
            })),
            paths: [...page_.querySelectorAll('[data-testid="atlas-help-path"]')]
                .map((node) => node.textContent?.trim() ?? ''),
            anchors: page_.querySelectorAll('a').length,
            text,
            backgroundColor: style.backgroundColor,
            color: style.color,
            fontFamily: style.fontFamily,
            luminance,
        };
    });

/** Wo die Tastatur gerade hingeht und was die Zeile zeigt. */
const commandSeam = (page) =>
    page.evaluate((panelIds) => {
        const line = document.querySelector('[data-testid="atlas-command"]');
        const input = document.querySelector('[data-testid="atlas-command-input"]');
        const prompt = document.querySelector('[data-testid="atlas-command-prompt"]');
        const results = document.querySelector('[data-testid="atlas-search-results"]');
        return {
            value: input === null ? '' : input.value,
            focused: document.activeElement === input,
            activeElement: document.activeElement?.tagName ?? '',
            dataFocused: line?.getAttribute('data-focused') ?? '',
            promptColor: prompt === null ? '' : globalThis.getComputedStyle(prompt).color,
            lineBorder: line === null ? '' : globalThis.getComputedStyle(line).borderTopColor,
            lineBackground: line === null ? '' : globalThis.getComputedStyle(line).backgroundColor,
            hasTitle: (input?.hasAttribute('title') ?? false) || (line?.hasAttribute('title') ?? false),
            hint: document.querySelector('.atlas-command-hint')?.textContent?.trim() ?? '',
            resultsShown: results !== null,
            resultsHeadline:
                document.querySelector('[data-testid="atlas-search-headline"]')?.textContent?.trim() ?? '',
            openPanels: panelIds.filter((id) => document.querySelector(`[data-testid="${id}"]`) !== null),
            galaxyVisible:
                document.querySelector('[data-testid="atlas-galaxy"]')?.getAttribute('data-visible') ?? '',
        };
    }, PANEL_TESTIDS);

/** Die beiden Linsen des Twins, so wie sie im Panel stehen. */
const twinLensSeam = (page) =>
    page.evaluate(() => {
        const sectionText = (name) =>
            document.querySelector(`[data-testid="codeatlas-twin-section-${name}"]`)
                ?.textContent?.replace(/\s+/g, ' ').trim() ?? '';
        return {
            runtime: sectionText('runtime'),
            changes: sectionText('changes'),
            effects: sectionText('effects'),
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
        menuItems: [],
        noNotWiredTooltip: false,
        everyMenuItemWired: false,
        helpOpensByClick: false,
        helpOpensByKey: false,
        helpEscCloses: false,
        helpSectionsShown: 0,
        helpListsEveryShortcut: false,
        helpNamesLimits: false,
        helpHasNoWebLinks: false,
        darkStyled: false,
        overlapViolations: 0,
        clippingViolations: 0,
        twinRuntimeSentenceHonest: false,
        twinChangesSentenceHonest: false,
        typingWithoutClickReachesLine: false,
        typingNeverOpensPanelAccidentally: false,
        focusVisible: false,
        focusKeyWorks: false,
        oneCharHintInResultRow: false,
        noNativeTooltipOnCommandLine: false,
        menuShortcutByAltWorks: false,
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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w7a-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w7a-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);
        log('Rendezvous:', runtimeDir);

        // --------------------------------------------------------- 3. Index
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
        report.port = uiPort;
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

        /** Laden, bis der Baum steht. Jeder Halt beginnt so. */
        const load = async () => {
            await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
            await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
            await page.waitForTimeout(500);
        };

        /**
         * Die Lesbarkeit an dieser Stelle, oben und durchgescrollt.
         *
         * Zweimal, weil eine Seite, die oben ordentlich aussieht und unten
         * kollidiert, eine Seite ist, die kollidiert.
         */
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

        // ------------------------------------------- 6a. Die Menuezeile
        await load();
        const menu = await menuSeam(page);
        extras.menu = menu;
        report.menuItems = menu.items.map((item) => item.label);
        report.noNotWiredTooltip = !menu.titles.some((title) => /not wired/i.test(title));
        const wired = wiredKeysFromSource();
        extras.wiredKeys = wired;
        const letterOf = (label) => /\[([a-z?])\]/.exec(label)?.[1];
        report.everyMenuItemWired =
            menu.items.length > 0
            && menu.items.every((item) => !item.disabled && item.tag === 'BUTTON')
            && menu.items.every((item) => {
                const letter = letterOf(item.label);
                return letter !== undefined && wired.menu.includes(letter);
            });
        log(`Menuezeile: ${report.menuItems.join('  ')} (Legende "${menu.legend}")`);
        await page.screenshot({ path: join(OUT_DIR, 'menu.png'), fullPage: true });

        // --------------------------------- 6b. Die Hilfe, per Klick geoeffnet
        await page.click('[data-menu="?"]');
        await page.waitForSelector('[data-testid="atlas-help"]', { timeout: 15000 });
        await page.waitForTimeout(400);
        const help = await helpSeam(page);
        extras.help = {
            sections: help.sections,
            shortcuts: help.shortcuts,
            paths: help.paths,
            anchors: help.anchors,
            backgroundColor: help.backgroundColor,
            color: help.color,
            fontFamily: help.fontFamily,
            luminance: help.luminance,
            textLength: help.text.length,
        };
        report.helpOpensByClick = help.present === true;
        report.helpSectionsShown = help.sections.length;
        report.helpNamesLimits = LIMIT_PHRASES.every((phrase) => help.text.includes(phrase));
        report.helpHasNoWebLinks = help.anchors === 0 && !/https?:\/\//i.test(help.text);
        report.darkStyled = help.luminance < 0.2 && /mono/i.test(help.fontFamily);

        const shownKeys = {
            menu: [...new Set(help.shortcuts
                .filter((row) => ['mnemonic', 'bare', 'line'].includes(row.scope))
                .map((row) => row.key))].sort(),
            walk: [...new Set(help.shortcuts.filter((row) => row.scope === 'walk').map((row) => row.key))].sort(),
            search: [...new Set(help.shortcuts.filter((row) => row.scope === 'search').map((row) => row.key))].sort(),
        };
        extras.shownKeys = shownKeys;
        report.helpListsEveryShortcut =
            JSON.stringify(shownKeys.menu) === JSON.stringify(wired.menu)
            && JSON.stringify(shownKeys.walk) === JSON.stringify(wired.walk)
            && JSON.stringify(shownKeys.search) === JSON.stringify(wired.search)
            && help.shortcuts.every((row) => row.does.length > 0);
        log(`Hilfe: ${help.sections.join(', ')} (${help.shortcuts.length} Tasten, `
            + `Grenzen woertlich: ${report.helpNamesLimits})`);

        await readability('help-open');
        await page.screenshot({ path: join(OUT_DIR, 'help.png'), fullPage: true });

        // ------------------------------- 6c. Zu per Knopf, auf per Taste, zu per Escape
        await page.click('[data-testid="atlas-help-close"]');
        await page.waitForSelector('[data-testid="atlas-help"]', { state: 'detached', timeout: 10000 });
        extras.helpCloseButtonWorks = true;

        await page.locator('.atlas-brand').click();
        await page.keyboard.press('?');
        await page.waitForSelector('[data-testid="atlas-help"]', { timeout: 10000 }).catch(() => undefined);
        report.helpOpensByKey = (await page.locator('[data-testid="atlas-help"]').count()) > 0;
        await page.keyboard.press('Escape');
        await page.waitForSelector('[data-testid="atlas-help"]', { state: 'detached', timeout: 10000 })
            .catch(() => undefined);
        report.helpEscCloses = (await page.locator('[data-testid="atlas-help"]').count()) === 0;
        log(`Hilfe per Taste: ${report.helpOpensByKey}, Escape schliesst: ${report.helpEscCloses}`);

        // ---------------------------------------------- 6d. Die Kommandozeile
        //
        // Frisch geladen, damit der Fokus dort liegt, wo er nach dem Laden
        // wirklich liegt: nirgends. Genau in diesem Zustand hat der Nutzer
        // getippt und nichts gesehen.
        await load();
        const before = await commandSeam(page);
        extras.commandBeforeTyping = before;
        await page.keyboard.type(TYPED_WORD, { delay: 60 });
        await page.waitForTimeout(600);
        const afterTyping = await commandSeam(page);
        extras.commandAfterTyping = afterTyping;
        report.typingWithoutClickReachesLine =
            before.activeElement === 'BODY' && afterTyping.value === TYPED_WORD;
        report.typingNeverOpensPanelAccidentally =
            afterTyping.openPanels.length === 0
            && afterTyping.galaxyVisible === before.galaxyVisible;
        report.noNativeTooltipOnCommandLine = afterTyping.hasTitle === false;
        report.focusVisible =
            before.dataFocused === 'false'
            && afterTyping.dataFocused === 'true'
            && afterTyping.focused === true
            && afterTyping.promptColor !== before.promptColor;
        log(`getippt ohne Klick: "${afterTyping.value}" (Panels offen: `
            + `${afterTyping.openPanels.join(', ') || 'keins'})`);
        /*
         * Ein Bild vom Befund selbst: die Zeile traegt das getippte Wort, der
         * Prompt leuchtet, das Trefferfenster steht darueber, und kein Panel ist
         * aufgegangen. Der frozen Test verlangt dieses Bild nicht; der Nutzer
         * hat den Fehler an einem Screenshot gemeldet, und die Antwort darauf
         * gehoert als Screenshot ins Artefakt.
         */
        await page.screenshot({ path: join(OUT_DIR, 'command.png'), fullPage: true });

        // Der Ein-Zeichen-Hinweis, dort wo die Treffer stehen wuerden.
        await load();
        await page.keyboard.press('c');
        await page.waitForTimeout(400);
        const oneChar = await commandSeam(page);
        extras.commandOneChar = oneChar;
        report.oneCharHintInResultRow =
            oneChar.value === 'c'
            && oneChar.resultsShown === true
            && /one more letter/i.test(oneChar.resultsHeadline);
        await page.screenshot({ path: join(OUT_DIR, 'onechar.png'), fullPage: true });
        log(`ein Zeichen: "${oneChar.value}", Fenster sagt "${oneChar.resultsHeadline}"`);

        // Die Fokustaste holt die Zeile, ohne sich selbst hineinzuschreiben.
        await load();
        const focusKey = /FOCUS_COMMAND_KEY = '([^']+)'/
            .exec(readFileSync(join(ROOT, 'src', 'app', 'keyboard.ts'), 'utf8'))?.[1] ?? '/';
        await page.keyboard.press(focusKey);
        await page.waitForTimeout(300);
        const afterFocusKey = await commandSeam(page);
        extras.commandAfterFocusKey = { key: focusKey, ...afterFocusKey };
        report.focusKeyWorks = afterFocusKey.focused === true && afterFocusKey.value === '';
        log(`Fokustaste "${focusKey}": Zeile fokussiert ${afterFocusKey.focused}, `
            + `Inhalt "${afterFocusKey.value}"`);

        /*
         * Die Gegenprobe zu AC8: der Buchstabe TUT weiterhin etwas, nur mit
         * Alt/Option davor.
         *
         * Ohne diesen Halt waere die Zusicherung des Zyklus halbiert: "kein
         * blanker Buchstabe oeffnet ein Panel" ist billig zu haben, indem man
         * die Kuerzel abschaltet. Gemessen wird deshalb beides am selben Lauf,
         * an zwei Kuerzeln: `a` schaltet die Galaxie, `c` schlaegt die
         * Aenderungsansicht auf (dasselbe Kuerzel, das im Nutzerbefund
         * versehentlich ausgeloest wurde).
         */
        await load();
        const galaxyBefore = await page.evaluate(
            () => document.querySelector('[data-testid="atlas-galaxy"]')?.getAttribute('data-visible') ?? '',
        );
        await page.locator('.atlas-brand').click();
        await page.keyboard.press('Alt+a');
        await page.waitForFunction(
            (before) => document.querySelector('[data-testid="atlas-galaxy"]')?.getAttribute('data-visible') !== before,
            galaxyBefore,
            { timeout: 15000 },
        ).catch(() => undefined);
        const afterAltA = await commandSeam(page);
        await page.keyboard.press('Alt+c');
        await page.waitForSelector('[data-testid="atlas-impact"]', { timeout: 30000 }).catch(() => undefined);
        const afterAltC = await commandSeam(page);
        extras.altShortcuts = {
            galaxyBefore,
            galaxyAfterAltA: afterAltA.galaxyVisible,
            lineAfterAltA: afterAltA.value,
            panelsAfterAltC: afterAltC.openPanels,
            lineAfterAltC: afterAltC.value,
        };
        report.menuShortcutByAltWorks =
            afterAltA.galaxyVisible !== galaxyBefore
            && afterAltA.value === ''
            && afterAltC.openPanels.includes('atlas-impact')
            && afterAltC.value === '';
        log(`Alt-Kuerzel: Galaxie ${galaxyBefore} -> ${afterAltA.galaxyVisible}, `
            + `Panels nach Alt+c: ${afterAltC.openPanels.join(', ') || 'keins'}`);
        await page.keyboard.press('Escape');
        await page.waitForTimeout(300);

        await readability('command-line');

        // ------------------------------------------------- 6e. Die zwei Linsen
        //
        // Zum Symbol ueber dieselbe Suche, die die Kommandozeile fuehrt, und
        // diesmal ohne einen Klick in die Zeile: das ist die zweite Haelfte des
        // Beweises fuer AC8.
        await load();
        await page.keyboard.type(TARGET, { delay: 40 });
        await page.waitForSelector(`[data-testid="atlas-search-row"][data-name="${TARGET}"]`, { timeout: 30000 });
        await page.waitForTimeout(500);
        await page.click(`[data-testid="atlas-search-row"][data-name="${TARGET}"]`);
        await page.waitForFunction(
            () => /userService\.createUser$/.test(globalThis.__atlasTwin?.qualifiedName ?? ''),
            undefined,
            { timeout: 40000 },
        );
        // Die beiden Linsen sind per Vorgabe aus (TWIN_PROFILE): sie werden hier
        // eingeschaltet, weil ihre Saetze sonst gar nicht dastehen.
        for (const facet of ['runtime', 'changes']) {
            await page.click(`[data-facet="${facet}"]`);
            await page.waitForSelector(`[data-testid="codeatlas-twin-section-${facet}"]`, { timeout: 20000 });
        }
        await page.waitForTimeout(400);
        const lenses = await twinLensSeam(page);
        extras.twinLenses = lenses;
        report.twinRuntimeSentenceHonest =
            /BUG hunt/.test(lenses.runtime)
            && /ingest_traces/.test(lenses.runtime)
            && !/\byet\b/i.test(lenses.runtime)
            && !/Import runtime traces/i.test(lenses.runtime);
        report.twinChangesSentenceHonest =
            /\[c\]hange scope/.test(lenses.changes)
            && /detect_changes/.test(lenses.changes)
            && !/\byet\b/i.test(lenses.changes);
        log(`Runtime-Zeile ehrlich: ${report.twinRuntimeSentenceHonest}, `
            + `Changes-Zeile ehrlich: ${report.twinChangesSentenceHonest}`);
        await readability('twin-lenses');

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };

        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w7a] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w7a] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
            typedWord: TYPED_WORD,
            limitPhrases: LIMIT_PHRASES,
            panelsWatched: PANEL_TESTIDS,
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
        && report.menuItems.length > 0
        && report.noNotWiredTooltip === true
        && report.everyMenuItemWired === true
        && report.helpOpensByClick === true
        && report.helpOpensByKey === true
        && report.helpEscCloses === true
        && report.helpSectionsShown >= 6
        && report.helpListsEveryShortcut === true
        && report.helpNamesLimits === true
        && report.helpHasNoWebLinks === true
        && report.darkStyled === true
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.twinRuntimeSentenceHonest === true
        && report.twinChangesSentenceHonest === true
        && report.typingWithoutClickReachesLine === true
        && report.typingNeverOpensPanelAccidentally === true
        && report.focusVisible === true
        && report.focusKeyWorks === true
        && report.oneCharHintInResultRow === true
        && report.noNativeTooltipOnCommandLine === true
        && report.menuShortcutByAltWorks === true
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0;

    if (!ok) {
        console.error('[smoke-w7a] W7a-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w7a] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W7a-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w7a] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
