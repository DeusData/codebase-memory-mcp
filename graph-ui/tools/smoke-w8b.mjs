#!/usr/bin/env node
/*
 * W8b-Smoke: Knoepfe sagen, was sie tun, und nichts legt sich mehr ungefragt
 * darueber.
 *
 * ## Warum dieser Lauf existiert
 *
 * Vier Nutzerbefunde vom 2026-08-29 Nachmittag, mit Screenshots bei 100 und bei
 * 67 Prozent Zoom. Sie haengen alle an derselben Wurzel: die Oberflaeche
 * erklaerte sich ueber Zeichen und ueber Tooltips, die sie weder platzieren
 * noch messen konnte.
 *
 * Der erste Befund ist der, der diesen Lauf noetig macht. Der Tooltip "76
 * nodes, 178 edges from /api/layout" lag ueber dem Detail-Regler und ueber den
 * Chips Logic, Calls, Data. Der Nutzer dazu: "ich hab damals schon in Auftrag
 * gegeben, dass sich Dinge ueberlappen, was uncool ist." Er hat recht, und die
 * Ursache ist gemessen: das waren native `title`-Tooltips, 78 im Produktivcode.
 * Der Browser zeichnet sie AUSSERHALB des Dokuments, unter dem Mauszeiger, ohne
 * Rechteck. Deshalb hat kein Beweislauf dieses Projekts sie je gesehen: die
 * Ueberlagerungsmessung liest `getBoundingClientRect`, und ein nativer Tooltip
 * hat keine. Es gab 78 Flaechen, die sich ueber beliebigen Inhalt legen
 * konnten, und null Messung darauf.
 *
 * Also misst dieser Lauf zwei Dinge, und das zweite ist erst moeglich, weil das
 * erste stimmt:
 *
 *  1. **Kein nativer Tooltip erklaert mehr etwas.** Gezaehlt wird nicht "hat
 *     ein `title`", sondern "hat einen `title`, der mehr sagt als der sichtbare
 *     Text": ein Attribut, das nur wiederholt, was danebensteht, ist ein
 *     anderer Fehler und faellt ersatzlos weg. Gezaehlt wird an JEDEM Halt der
 *     Strecke, nicht einmal am Anfang.
 *  2. **Jeder eigene Tooltip wird EINZELN geoeffnet und vermessen.** Ein Lauf,
 *     der einmal hinsieht und "keine Ueberlagerung" meldet, haette ueber die
 *     anderen siebenundsiebzig nichts gesagt. Geoeffnet wird mit dem Zeiger UND
 *     mit der Tastatur, denn ein Tooltip, den nur die Maus oeffnet, ist keiner.
 *
 * ## Was hier nicht behauptet, sondern gemessen wird
 *
 * Drei Zahlen dieses Artefakts sind MESSUNGEN und keine Zusicherungen, und sie
 * duerfen deshalb auch anders ausfallen, ohne dass der Lauf rot wird:
 *
 *  - `unresolvedCallsReported`: ob der Index an dieser Stelle ueberhaupt
 *    unaufgeloeste Aufrufe meldet. Das Vorgehen aus AC6.2 haengt an der
 *    Antwort: meldet er sie, steht die Zahl unter dem Bild; meldet er sie
 *    nicht, bleibt genau ein allgemeiner Satz. Was die Zahl NICHT sehen kann,
 *    steht unter `unresolvedCallsMethod` im Artefakt, und das gehoert dazu:
 *    ein Aufruf, den der Index ganz ohne qualifizierten Namen meldet, faellt
 *    schon im Walk heraus (src/provider/closure.ts filtert ihn) und ist an
 *    dieser Stelle nicht mehr zaehlbar.
 *  - `honestyBlockChars` und `honestyBlockParagraphs`: was unter dem Diagramm
 *    wirklich steht, aus dem gerenderten Baum gelesen und nicht aus den
 *    Konstanten gerechnet.
 *  - `handleHitAreaPx`: das kleinste Mass aller vier Griffe quer zu ihrer
 *    Achse, aus `getBoundingClientRect` und nicht aus der CSS-Datei.
 *
 * ## Der Zoom, und warum er ein groesseres Fenster ist
 *
 * Chromium kann seinen Zoom in einem headless-Kontext nicht wie ein Nutzer
 * stellen. Was 67 Prozent Zoom fuer das LAYOUT bedeutet, ist aber exakt
 * beschreibbar: bei gleichem Fenster wird der CSS-Ansichtsbereich um den
 * Kehrwert groesser. 1680 mal 1050 bei 100 Prozent sind 2507 mal 1567 bei 67.
 * Genau das stellt dieser Lauf ein, und die Rechnung steht im Artefakt.
 *
 * ## Ablauf
 *
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4540, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *      a. Ruhezustand: keine nativen Tooltips, die Griffe, die Beispiele
 *      b. jeden Tooltip einzeln oeffnen und messen, mit Zeiger und Tastatur
 *      c. die Schalter: Wort statt Zeichen, und der Name folgt der Ansicht
 *      d. der Twin voll, der Graph offen, 100 Prozent, kein Ziehen
 *      e. der Flow: der Block kurz, die Grenze am Bild, der Schrittsatz da
 *      f. der Chat: Tiefe aendern, Angebot, neuer Zug, alter Zug unveraendert
 *      g. der Kopf des Chats bei 100 und bei 67 Prozent
 *      h. die Frage: Escape, und eine Datei oeffnen
 *   6. abraeumen, Restprozesse zaehlen, JSON und vier Bilder im Ruhezustand
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w8b).
 *
 * ## Ports
 *
 * Ab 4540. 4390 und 4391 gehoeren der Vorschau des Nutzers, 4141 seinem
 * Modell-Sidecar, 4360 und 4400 den Eval-Laeufen, 4440 und 4460 den Laeufen von
 * W8 und W9. Dieser Lauf fasst keinen davon an.
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
    READABILITY_EXCLUSIONS,
    TOOLTIP_PROTECTED,
    closeTooltips,
    measureReadability,
    measureTooltip,
    nativeTitles,
    resetScroll,
    scrollRegionsToEnd,
    tooltipCover,
    tooltipTriggers,
} from './lib/readability.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w8b';
const OUT_DIR = join(ROOT, 'verification', 'w8b');
const OUT_JSON = join(OUT_DIR, 'ux.json');
const SHOT_TOOLTIP = join(OUT_DIR, 'tooltip-open.png');
const SHOT_WORDS = join(OUT_DIR, 'collapse-words.png');
const SHOT_TWIN = join(OUT_DIR, 'twin-full.png');
const SHOT_FLOW = join(OUT_DIR, 'flow-short.png');

/** Contract AC7. 4390/4391/4141 gehoeren dem Nutzer, 4440 und 4460 den Laeufen davor. */
const MIN_PORT = 4540;

/** Der Zoom des zweiten Screenshots des Nutzers, als Faktor. */
const SMALL_ZOOM = 0.67;

const MAIN_VIEWPORT = { width: 1680, height: 1050 };

/**
 * Derselbe Bildschirm bei 67 Prozent Zoom.
 *
 * Ein Zoom von z verkleinert jedes CSS-Pixel auf z Geraetepixel, also passen in
 * dasselbe Fenster 1/z mal so viele davon. Das ist der ganze Unterschied fuer
 * ein Layout, und es ist der Unterschied, den der zweite Screenshot des Nutzers
 * zeigt.
 */
const SMALL_VIEWPORT = {
    width: Math.round(MAIN_VIEWPORT.width / SMALL_ZOOM),
    height: Math.round(MAIN_VIEWPORT.height / SMALL_ZOOM),
};

/** Das Symbol, ueber das gelaufen wird. Dasselbe wie in W5c, W8 und W9. */
const WALK_TARGET = 'createUser';
const TARGET_FILE = 'src/services/userService.ts';
const TARGET_QUALIFIED = 'userService\\.createUser';

/** Die Datei, die der Nutzer im Explorer angeklickt hat. */
const EXPLORER_FILE = 'src/services/orderService.ts';

/** Chromium ohne Aussenwelt, plus die Software-GL-Flags aus smoke-w5c. */
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

const log = (...parts) => console.log('[smoke-w8b]', ...parts);
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
 * Die Ein- und Ausklapper, so wie sie dastehen.
 *
 * Gelesen wird ueber `data-fold`, und das ist keine Bequemlichkeit: die Frage
 * aus AC3 gilt fuer die Schalter, die eine SEKTION auf- und zumachen, und nicht
 * fuer die Ausklapp-Dreiecke des Dateibaums. Ein Selektor, der alles einsammelt,
 * was ein Zeichen traegt, wuerde ein universelles Idiom melden, das der Nutzer
 * nie beanstandet hat. Die Marke sagt also, WELCHE Schalter die Regel meint,
 * und `data-fold-of` sagt, WORUEBER jeder von ihnen redet.
 */
const foldSwitches = (page) =>
    page.evaluate(() =>
        [...document.querySelectorAll('[data-fold]')].map((node) => ({
            state: node.getAttribute('data-fold') ?? '',
            of: node.getAttribute('data-fold-of') ?? '',
            label: (node.textContent ?? '').replace(/\s+/g, ' ').trim(),
            testId: node.getAttribute('data-testid') ?? '',
            expanded: node.getAttribute('aria-expanded') ?? '',
        })));

/** Der Ansichts-Umschalter: sein Rahmen, seine Rolle und was der aktive Chip sagt. */
const viewToggle = (page) =>
    page.evaluate(() => {
        const group = document.querySelector('[data-testid="atlas-graph-mode"]');
        if (group === null) {
            return null;
        }
        const style = window.getComputedStyle(group);
        const chips = [...group.querySelectorAll('[data-testid="atlas-graph-mode-chip"]')].map((chip) => ({
            mode: chip.getAttribute('data-mode') ?? '',
            pressed: chip.getAttribute('aria-pressed') ?? '',
            active: chip.getAttribute('data-active') === 'true',
            hint: chip.getAttribute('data-hint') ?? '',
            disabled: chip.hasAttribute('disabled'),
        }));
        return {
            role: group.getAttribute('role') ?? '',
            borderWidth: Number.parseFloat(style.borderTopWidth) || 0,
            borderStyle: style.borderTopStyle,
            chips,
        };
    });

/** Die vier Griffe, in Pixeln und mit ihrer Marke. */
const handles = (page) =>
    page.evaluate(() =>
        [...document.querySelectorAll('.atlas-splitter')].map((node) => {
            const rect = node.getBoundingClientRect();
            const mark = window.getComputedStyle(node, '::after');
            const line = window.getComputedStyle(node, '::before');
            const orientation = node.getAttribute('data-orientation') ?? '';
            return {
                testId: node.getAttribute('data-testid') ?? '',
                orientation,
                /* Quer zur Linie: das ist die Flaeche, die eine Maus treffen muss. */
                hit: Math.round(orientation === 'vertical' ? rect.width : rect.height),
                className: node.getAttribute('class') ?? '',
                hint: node.getAttribute('data-hint') ?? '',
                mark: {
                    content: mark.content,
                    background: mark.backgroundColor,
                    width: mark.width,
                    height: mark.height,
                },
                line: { background: line.backgroundColor },
            };
        }));

/** Die Beispiele ueber der Kommandozeile, so wie sie dastehen. */
const commandExamples = (page) =>
    page.evaluate(() => {
        const input = document.querySelector('[data-testid="atlas-command-input"]');
        return {
            placeholder: input?.getAttribute('placeholder') ?? '',
            open: document.querySelector('[data-testid="atlas-command-examples"]') !== null,
            rows: [...document.querySelectorAll('[data-testid="atlas-command-example"]')].map((node) => ({
                id: node.getAttribute('data-example') ?? '',
                symbol: node.getAttribute('data-symbol') ?? '',
                text: node.querySelector('.atlas-command-example-text')?.textContent?.trim() ?? '',
                tag: node.tagName,
                focusable: node.tabIndex >= 0,
            })),
        };
    });

/** Was unter dem Diagramm steht, gezaehlt am gerenderten Baum. */
const honestyBlock = (page) =>
    page.evaluate(() => {
        const block = document.querySelector('[data-testid="atlas-flow-honesty-block"]');
        const paragraphs = [...document.querySelectorAll('[data-testid="atlas-flow-honesty"]')]
            .map((node) => (node.textContent ?? '').replace(/\s+/g, ' ').trim())
            .filter((text) => text.length > 0);
        const bound = document.querySelector('[data-testid="atlas-flow-walk-bound"]');
        const svg = document.querySelector('[data-testid="atlas-flow-diagram"]');
        const boundBox = bound?.getBoundingClientRect();
        const svgBox = svg?.getBoundingClientRect();
        const provenance = document.querySelector('[data-testid="atlas-flow-provenance"]');
        return {
            present: block !== null,
            paragraphs,
            chars: paragraphs.reduce((sum, text) => sum + text.length, 0),
            bound: {
                present: bound !== null,
                text: bound?.textContent?.trim() ?? '',
                depth: bound?.getAttribute('data-depth') ?? '',
                cap: bound?.getAttribute('data-cap') ?? '',
                /* Am Bild heisst: sein Rechteck liegt im Rechteck des Bildes. */
                onDiagram: bound !== undefined && boundBox !== undefined && svgBox !== undefined
                    && boundBox.left >= svgBox.left - 1 && boundBox.right <= svgBox.right + 1
                    && boundBox.top >= svgBox.top - 1 && boundBox.bottom <= svgBox.bottom + 1,
            },
            provenance: {
                present: provenance !== null,
                hint: provenance?.getAttribute('data-hint') ?? '',
            },
            unresolved: Number(
                document.querySelector('[data-testid="atlas-flow-honesty"]')
                    ?.getAttribute('data-unresolved') ?? '-1',
            ),
            stepNote: document.querySelector('[data-testid="atlas-flow-no-hit"]')
                ?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
        };
    });

/** Der Verlauf des Chats, so weit dieser Lauf ihn braucht. */
const chatState = (page) =>
    page.evaluate(() => ({
        turns: [...document.querySelectorAll('[data-testid="atlas-chat-turn"]')].map((node) => ({
            depth: node.getAttribute('data-depth') ?? '',
            status: node.getAttribute('data-status') ?? '',
            question: node.querySelector('[data-testid="atlas-chat-question"]')
                ?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            rerun: node.querySelector('[data-testid="atlas-chat-rerun"]')
                ?.textContent?.trim() ?? '',
        })),
        depth: document.querySelector('[data-testid="atlas-chat-depth"]')
            ?.getAttribute('data-depth') ?? '',
    }));

/**
 * Ob der Kopf des Chats wirklich zu sehen ist.
 *
 * Nicht "steht im Baum", sondern "hat nach allen Kaesten darueber und nach dem
 * Fenster noch Flaeche": genau der Fall des Nutzers war ein Kopf, der im Baum
 * stand und aus dem Bild gewandert war.
 */
const chatHeadVisible = (page) =>
    page.evaluate(() => {
        const head = document.querySelector('[data-testid="atlas-chat-head"]');
        if (head === null) {
            return { present: false, visible: false };
        }
        const clip = (node) => {
            const rect = node.getBoundingClientRect();
            let { left, top, right, bottom } = rect;
            for (
                let current = node.parentElement;
                current !== null && current !== document.documentElement;
                current = current.parentElement
            ) {
                const style = window.getComputedStyle(current);
                const box = current.getBoundingClientRect();
                if (style.overflowX !== 'visible') {
                    left = Math.max(left, box.left);
                    right = Math.min(right, box.right);
                }
                if (style.overflowY !== 'visible') {
                    top = Math.max(top, box.top);
                    bottom = Math.min(bottom, box.bottom);
                }
            }
            left = Math.max(left, 0);
            top = Math.max(top, 0);
            right = Math.min(right, window.innerWidth);
            bottom = Math.min(bottom, window.innerHeight);
            return { width: right - left, height: bottom - top };
        };
        const seen = (selector) => {
            const node = document.querySelector(selector);
            if (node === null) {
                return false;
            }
            const box = clip(node);
            return box.width > 2 && box.height > 2;
        };
        const box = clip(head);
        return {
            present: true,
            visible: box.width > 2 && box.height > 2,
            chips: seen('[data-testid="atlas-chat-depth"]'),
            clear: seen('[data-testid="atlas-chat-clear"]'),
            height: Math.round(box.height),
        };
    });

/** Der Zustand der Frage nach dem Warum und dessen, was hinter ihr liegt. */
const whyState = (page) =>
    page.evaluate(() => ({
        why: document.querySelector('[data-testid="atlas-why"]') !== null,
        decline: document.querySelector('[data-testid="atlas-why-decline"]')
            ?.textContent?.trim() ?? '',
        readerPath: globalThis.__atlasReader?.document?.path ?? '',
        breadcrumb: document.querySelector('[data-testid="atlas-breadcrumb"]')
            ?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
        readerVisible: (() => {
            const reader = document.querySelector('[data-testid="atlas-reader"]');
            if (reader === null) {
                return false;
            }
            const rect = reader.getBoundingClientRect();
            if (rect.width <= 0 || rect.height <= 0) {
                return false;
            }
            /*
             * Nicht nur "es gibt einen Reader", sondern "in seiner Mitte liegt
             * er selbst obenauf". Genau das war der Befund: der Klick wirkte,
             * die Datei war offen, und davor stand die Frage.
             */
            const at = document.elementFromPoint(
                Math.round(rect.left + rect.width / 2),
                Math.round(rect.top + rect.height / 2),
            );
            return at !== null && reader.contains(at);
        })(),
    }));

/** Der Twin, mit der Frage, ob seine Kante etwas sagt. */
const twinEdge = (page) =>
    page.evaluate(() => {
        const body = document.querySelector('.atlas-twin-body');
        const mark = document.querySelector('[data-testid="atlas-twin-more"]');
        if (body === null) {
            return { present: false };
        }
        const markStyle = mark === null ? null : window.getComputedStyle(mark);
        const markBox = mark?.getBoundingClientRect();
        return {
            present: true,
            hidden: Math.round(body.scrollHeight - body.clientHeight),
            scrollHeight: Math.round(body.scrollHeight),
            clientHeight: Math.round(body.clientHeight),
            empty: document.querySelector('[data-testid="atlas-twin-empty"]') !== null,
            depth: document.querySelector('[data-testid="atlas-twin-depth-name"]')?.textContent?.trim() ?? '',
            top: Math.round(body.scrollTop),
            mark: mark === null ? '' : mark.getAttribute('data-scroll-hint') ?? '',
            markText: mark?.textContent?.trim() ?? '',
            markVisible: mark !== null && markStyle !== null && markBox !== undefined
                && markBox.width > 0 && markBox.height > 0
                && markStyle.display !== 'none' && markStyle.visibility !== 'hidden',
        };
    });

// ------------------------------------------------------------- Klickstrecke -

async function openApp(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 30000 });
    await page.waitForFunction(
        () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
        undefined,
        { timeout: 60000 },
    );
    await page.waitForTimeout(900);
}

/** Wo jeder scrollbare Bereich steht. Wortgleich mit smoke-w8 und smoke-w9. */
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

/**
 * Ein Beweisbild im Ruhezustand.
 *
 * Wortgleich mit smoke-w8 und smoke-w9: erst jeden Bereich an den Anfang, dann
 * warten, dann die Lage aufschreiben, dann das Bild. In dieser Reihenfolge,
 * damit die aufgeschriebene Lage die des Bildes ist.
 */
async function shootAtRest(page, file, name, keepTooltip = false) {
    if (!keepTooltip) {
        // Ein Kasten, der noch offen steht, weil der Zeiger nach einem Klick
        // liegengeblieben ist, gehoert nicht auf ein Bild vom Ruhezustand.
        await closeTooltips(page);
    }
    await resetScroll(page, READABILITY_EXCLUSIONS);
    await page.waitForTimeout(350);
    const state = await scrollState(page);
    await page.screenshot({ path: file, fullPage: false });
    log(`${name}: aufgenommen im Ruhezustand=${state.atRest}`
        + (keepTooltip ? ', mit offenem Tooltip' : ''));
    return { name, atRest: state.atRest, regions: state.regions };
}

/** Die Frage wieder aufrufen und einen Modus waehlen. Wortgleich mit smoke-w9. */
async function openWhyAndChoose(page, intent) {
    await page.click('[data-menu="a-why"]');
    await page.waitForSelector('[data-testid="atlas-why"]', { timeout: 20000 });
    await page.click(`[data-testid="atlas-why-card"][data-intent="${intent}"]`);
}

/** Im Einstiegsdialog suchen und den Treffer mit diesem Namen waehlen. */
async function chooseEntryHit(page, name) {
    await page.waitForSelector('[data-testid="atlas-entry"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-entry-row"]', { timeout: 60000 });
    const input = page.locator('[data-testid="atlas-entry-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(name, { delay: 40 });
    await page.waitForSelector(`[data-testid="atlas-entry-hit"][data-name="${name}"]`, { timeout: 30000 });
    await page.waitForTimeout(600);
    await page.click(`[data-testid="atlas-entry-hit"][data-name="${name}"]`);
    await page.waitForFunction(() => globalThis.__atlasTour?.kind === 'entry', undefined, { timeout: 60000 });
    await page.waitForTimeout(1600);
}

/** Zu einem Symbol navigieren, ueber dieselbe Suche, die die Kommandozeile fuehrt. */
async function openSymbol(page, name, file, qualified) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(name, { delay: 40 });
    await page.waitForSelector(`[data-testid="atlas-search-row"][data-name="${name}"]`, { timeout: 30000 });
    await page.waitForTimeout(600);
    await page.click(`[data-testid="atlas-search-row"][data-name="${name}"]`);
    await page.waitForFunction(
        (expected) => globalThis.__atlasReader?.document?.path === expected,
        file,
        { timeout: 40000 },
    );
    /*
     * Gewartet wird auf das SUBJEKT des Twin und nicht auf seinen
     * qualifizierten Namen.
     *
     * Der qualifizierte Name entsteht erst, wenn der Index die Stelle des
     * Carets aufgeloest hat, und diese Aufloesung haengt daran, dass der Caret
     * sich BEWEGT. Steht der Reader schon auf derselben Datei und derselben
     * Zeile (nach einem Walk auf genau dieses Symbol ist er das), bewegt er
     * sich nicht, und die Aufloesung laeuft gar nicht erst an. Das Subjekt ist
     * gesetzt, der qualifizierte Name bleibt leer, und ein Lauf, der auf ihn
     * wartet, wartet auf eine Antwort, die niemand mehr stellt. Der Name des
     * Subjekts sagt dasselbe ueber das, was dieser Lauf misst.
     */
    await page.waitForFunction(
        (expected) => (globalThis.__atlasTwin?.symbol ?? '') === expected,
        name,
        { timeout: 40000 },
    );
    void qualified;
    await page.waitForTimeout(400);
}

/** Eine Frage in die Kommandozeile tippen und abschicken. */
async function askInLine(page, question) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(question, { delay: 20 });
    await page.keyboard.press('Enter');
    await page.waitForTimeout(600);
}

/** Einen Reiter des Erklaeren-Bereichs waehlen. Wortgleich mit smoke-w8. */
async function chooseTab(page, tab) {
    await page.click(`[data-testid="atlas-explain-tab"][data-tab="${tab}"]`);
    await page.waitForFunction(
        (wanted) => globalThis.__atlasLayout?.explainTab === wanted
            && globalThis.__atlasLayout?.explainOpen === true,
        tab,
        { timeout: 15000 },
    );
    await page.waitForTimeout(300);
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
        // AC1 / AC2
        nativeTitlesWithExplanation: -1,
        domTooltipsMeasured: 0,
        tooltipCoversNothing: false,
        tooltipOpensByKeyboard: false,
        tooltipClosesByEscape: false,
        // AC3 / AC4
        collapseLabelsAreWords: false,
        collapseLabelFollowsView: false,
        viewToggleShowsState: false,
        // AC5
        twinCutHasHint: false,
        twinAtHundredPercentReadable: false,
        // AC6
        honestyBlockChars: -1,
        honestyBlockParagraphs: -1,
        walkBoundOnDiagram: false,
        unresolvedCallsReported: false,
        stepNoteKept: false,
        // AC6b
        depthChangeOffersRerun: false,
        rerunMakesNewTurn: false,
        oldTurnKeepsItsDepth: false,
        // AC6c
        placeholderShowsExample: false,
        examplesClickable: 0,
        examplesUseRealSymbols: false,
        // AC6d
        handleVisibleWithoutHover: false,
        handleHitAreaPx: -1,
        allZoneHandlesLookAlike: false,
        // AC6e
        chatHeadVisibleAt100: false,
        chatHeadVisibleAt67: false,
        // AC6f
        whyClosesOnOpenFile: false,
        whyClosesByEscape: false,
        fileOpensBehindNothing: false,
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
        tooltips: [],
        nativeTitleHits: [],
        folds: [],
        shots: [],
        tooltipProtected: TOOLTIP_PROTECTED,
        zoom: {
            factor: SMALL_ZOOM,
            full: MAIN_VIEWPORT,
            small: SMALL_VIEWPORT,
            method:
                'Ein Zoom von z verkleinert jedes CSS-Pixel auf z Geraetepixel, also passen in '
                + 'dasselbe Fenster 1/z mal so viele davon. Chromium stellt seinen Zoom headless '
                + 'nicht wie ein Nutzer; das GLEICHWERTIGE Fenster ist die messbare Fassung '
                + 'derselben Lage.',
        },
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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w8b-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w8b-run-');
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
        const context = await browser.newContext({ viewport: { ...MAIN_VIEWPORT } });
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

        /** Lesbarkeit an diesem Halt, oben und unten. Wortgleich mit smoke-w9. */
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
            return { top, bottom };
        };

        /** Die Zaehlung aus AC1, an diesem Halt. */
        const countTitles = async (where) => {
            const hits = await nativeTitles(page);
            if (hits.length > 0) {
                extras.nativeTitleHits.push({ where, hits });
            }
            report.nativeTitlesWithExplanation =
                Math.max(report.nativeTitlesWithExplanation, 0) + hits.length;
            return hits.length;
        };

        /**
         * Jeden Tooltip dieses Halts einzeln oeffnen und messen.
         *
         * Der eigentliche Beweis zu AC2. Geoeffnet wird abwechselnd mit dem
         * Zeiger und mit der Tastatur, damit beide Wege ueber die ganze Strecke
         * belegt sind und nicht nur an einem Musterknopf.
         */
        const sweepTooltips = async (where) => {
            const triggers = await tooltipTriggers(page);
            let covered = 0;
            for (const trigger of triggers) {
                const how = trigger.focusable && trigger.index % 2 === 1 ? 'keyboard' : 'pointer';
                const opened = await measureTooltip(page, trigger.index, how);
                await page.waitForTimeout(40);
                const seen = await tooltipCover(page, trigger.index);
                await closeTooltips(page);
                if (!seen.open) {
                    extras.tooltips.push({ where, ...trigger, how, opened: false });
                    continue;
                }
                report.domTooltipsMeasured += 1;
                if (how === 'keyboard') {
                    report.tooltipOpensByKeyboard = true;
                }
                if (seen.covers.length > 0) {
                    covered += 1;
                }
                extras.tooltips.push({
                    where,
                    name: trigger.name,
                    how,
                    text: trigger.text.slice(0, 70),
                    described: opened.described,
                    side: seen.side,
                    rect: seen.rect,
                    covers: seen.covers,
                });
            }
            log(`${where}: ${triggers.length} Ausloeser, ${report.domTooltipsMeasured} gemessen, `
                + `${covered} davon verdecken etwas`);
            return covered;
        };

        await openApp(page, origin);

        // ------------------------------------ 5a. Der Ruhezustand, gezaehlt
        let covering = 0;
        await countTitles('start');
        covering += await sweepTooltips('start');

        // Die Griffe (AC6d).
        const grips = await handles(page);
        extras.handles = grips;
        report.handleHitAreaPx = grips.length === 0
            ? -1
            : Math.min(...grips.map((grip) => grip.hit));
        const transparent = (value) => value === 'rgba(0, 0, 0, 0)' || value === 'transparent';
        report.handleVisibleWithoutHover = grips.length >= 3
            && grips.every((grip) => !transparent(grip.mark.background)
                && Number.parseFloat(grip.mark.width) > 0
                && Number.parseFloat(grip.mark.height) > 0);
        report.allZoneHandlesLookAlike = grips.length >= 3
            && grips.every((grip) => grip.className === grips[0].className
                && grip.mark.background === grips[0].mark.background
                && grip.hint === grips[0].hint);
        log(`Griffe: ${grips.length}, Trefferflaeche ${report.handleHitAreaPx}px, `
            + `Marke ohne Hover ${report.handleVisibleWithoutHover}, `
            + `alle gleich ${report.allZoneHandlesLookAlike}`);

        // Die Beispiele der Kommandozeile (AC6c).
        await page.click('[data-testid="atlas-command-input"]');
        await page.waitForTimeout(300);
        const examples = await commandExamples(page);
        extras.commandExamples = examples;
        report.examplesClickable = examples.rows.filter(
            (row) => row.tag === 'BUTTON' && row.focusable && row.text.length > 0,
        ).length;
        report.placeholderShowsExample = /@\w/.test(examples.placeholder)
            || examples.placeholder.includes('?');
        /*
         * Die Namen gegen den INDEX halten und nicht gegen dieselbe geladene
         * Liste, aus der sie stammen. Eine Liste gegen sich selbst zu pruefen
         * waere keine Pruefung.
         *
         * Gefragt wird ueber die Suche der Kommandozeile, also ueber genau den
         * Weg, den der Leser geht, wenn er das Beispiel abtippt: eine Zeile
         * gilt erst, wenn sie mit `data-source="index"` dasteht, also wenn der
         * SERVER geantwortet hat. Die Sofort-Vorschlaege ("loaded") kommen aus
         * dem, was der Browser schon hat, und die zaehlen hier nicht.
         */
        const symbols = [...new Set(examples.rows.map((row) => row.symbol))].filter(Boolean);
        const checked = [];
        for (const symbol of symbols) {
            const line = page.locator('[data-testid="atlas-command-input"]');
            await line.click();
            await line.fill('');
            await line.pressSequentially(symbol, { delay: 30 });
            const found = await page
                .waitForSelector(
                    `[data-testid="atlas-search-row"][data-name="${symbol}"][data-source="index"]`,
                    { timeout: 30000 },
                )
                .then(() => true)
                .catch(() => false);
            checked.push({ symbol, found, askedAs: symbol });
            await line.fill('');
            await page.keyboard.press('Escape');
            await page.waitForTimeout(200);
        }
        extras.exampleSymbols = checked;
        report.examplesUseRealSymbols = checked.length > 0
            && checked.every((entry) => entry.found === true);
        await page.click('[data-testid="atlas-command-input"]');
        await page.waitForTimeout(300);
        log(`Beispiele: Platzhalter "${examples.placeholder}", ${report.examplesClickable} anklickbar, `
            + `Namen im Index ${report.examplesUseRealSymbols} (${JSON.stringify(symbols)})`);

        // Ein Beispiel schreibt sich in die Zeile, ohne abzuschicken.
        const before = await chatState(page);
        await page.click('[data-testid="atlas-command-example"][data-example="at"]');
        await page.waitForTimeout(300);
        extras.exampleWritesLine = {
            value: await page.inputValue('[data-testid="atlas-command-input"]'),
            turnsBefore: before.turns.length,
            turnsAfter: (await chatState(page)).turns.length,
        };
        await page.fill('[data-testid="atlas-command-input"]', '');
        await page.keyboard.press('Escape');
        await page.waitForTimeout(200);

        await readability('Ruhezustand mit Beispielen');
        extras.shots.push(await shootAtRest(page, SHOT_WORDS, 'collapse-words.png'));

        // ------------------------------ 5b. Escape schliesst einen Tooltip
        const first = (await tooltipTriggers(page))[0];
        if (first !== undefined) {
            await measureTooltip(page, first.index, 'pointer');
            await page.waitForTimeout(120);
            const open = await tooltipCover(page, first.index);
            await page.keyboard.press('Escape');
            await page.waitForTimeout(150);
            const gone = await tooltipCover(page, first.index);
            report.tooltipClosesByEscape = open.open === true && gone.open === false;
            extras.escapeOnTooltip = { name: first.name, opened: open.open, afterEscape: gone.open };
            log(`Escape schliesst den Tooltip: ${report.tooltipClosesByEscape}`);
        }
        await closeTooltips(page);

        // -------------------------------------- 5c. Die Schalter (AC3, AC4)
        const foldsInGalaxy = await foldSwitches(page);
        extras.folds.push({ where: 'galaxy', switches: foldsInGalaxy });
        const wordy = (entry) => /[a-z]{3,}/i.test(entry.label)
            && !/^\s*\[[-+]\]\s*$/.test(entry.label)
            && !/^[▸▾▴◂+\-\[\]]+$/.test(entry.label);
        report.collapseLabelsAreWords = foldsInGalaxy.length >= 3 && foldsInGalaxy.every(wordy);
        const galaxyFold = foldsInGalaxy.find((entry) => entry.testId === 'atlas-galaxy-collapse');
        const followsInGalaxy = galaxyFold !== undefined
            && galaxyFold.of === 'galaxy'
            && galaxyFold.label.includes('galaxy')
            && galaxyFold.label.startsWith('collapse');

        const toggleBefore = await viewToggle(page);
        extras.viewToggle = { galaxy: toggleBefore };
        report.viewToggleShowsState = toggleBefore !== null
            && toggleBefore.role === 'group'
            && toggleBefore.borderWidth >= 1
            && toggleBefore.borderStyle !== 'none'
            && toggleBefore.chips.filter((chip) => chip.pressed === 'true').length === 1
            && toggleBefore.chips.some((chip) => chip.active && /already/i.test(chip.hint));
        log(`Ansichts-Umschalter: Rahmen ${toggleBefore?.borderWidth}px, Rolle `
            + `${toggleBefore?.role}, aktiver Chip sagt es: ${report.viewToggleShowsState}`);

        // ------------------------------ 5d. Der Walk, die Hierarchie, der Name
        await openWhyAndChoose(page, 'entry');
        await chooseEntryHit(page, WALK_TARGET);
        await page.waitForFunction(
            () => (globalThis.__atlasGalaxy?.hierarchy?.nodes ?? 0) > 0,
            undefined,
            { timeout: 60000 },
        );
        await page.waitForTimeout(600);
        extras.twinAfterWalk = await page.evaluate(() =>
            JSON.parse(JSON.stringify(globalThis.__atlasTwin ?? null)));
        log('Twin nach dem Walk:', JSON.stringify(extras.twinAfterWalk?.symbol ?? null),
            JSON.stringify(extras.twinAfterWalk?.qualifiedName ?? null));
        const foldsInHierarchy = await foldSwitches(page);
        extras.folds.push({ where: 'hierarchy', switches: foldsInHierarchy });
        const hierarchyFold = foldsInHierarchy.find((entry) => entry.testId === 'atlas-galaxy-collapse');
        report.collapseLabelsAreWords = report.collapseLabelsAreWords
            && foldsInHierarchy.length >= 3 && foldsInHierarchy.every(wordy);
        report.collapseLabelFollowsView = followsInGalaxy
            && hierarchyFold !== undefined
            && hierarchyFold.of === 'hierarchy'
            && hierarchyFold.label.includes('hierarchy');
        extras.viewToggle.hierarchy = await viewToggle(page);
        report.viewToggleShowsState = report.viewToggleShowsState
            && (extras.viewToggle.hierarchy?.chips ?? [])
                .some((chip) => chip.active && /already/i.test(chip.hint));
        log(`Schalter in Worten: ${report.collapseLabelsAreWords}, folgt der Ansicht: `
            + `${report.collapseLabelFollowsView} ("${galaxyFold?.label}" -> "${hierarchyFold?.label}")`);

        await countTitles('hierarchie');
        covering += await sweepTooltips('hierarchie');
        await readability('hierarchie mit Walk');

        // ------------------------------------------------- 5e. Der Twin (AC5)
        /*
         * Erst einen Schritt weiter, dann zurueck auf die Wurzel. Wortgleich
         * mit smoke-w8, und der Grund ist derselbe: der Twin loest das Symbol
         * am CARET auf, und ein Caret, der schon dort steht, bewegt sich nicht.
         * Ohne den Umweg waere die Wahl im Suchfenster eine Wahl ohne Wirkung,
         * und der Twin bliebe leer, obwohl der Reader die richtige Datei zeigt.
         */
        await page.click('[data-testid="atlas-tour-next"]');
        await page.waitForFunction(() => globalThis.__atlasTour?.index === 1, undefined, { timeout: 40000 });
        await page.waitForTimeout(1400);
        try {
            await openSymbol(page, WALK_TARGET, TARGET_FILE, TARGET_QUALIFIED);
        } catch (error) {
            extras.openSymbolFailure = {
                message: error instanceof Error ? error.message : String(error),
                reader: await page.evaluate(() => globalThis.__atlasReader?.document?.path ?? ''),
                twin: await page.evaluate(() =>
                    JSON.parse(JSON.stringify(globalThis.__atlasTwin ?? null))),
                rows: await page.evaluate(() =>
                    [...document.querySelectorAll('[data-testid="atlas-search-row"]')].map((row) => ({
                        name: row.getAttribute('data-name') ?? '',
                        source: row.getAttribute('data-source') ?? '',
                    }))),
            };
            throw error;
        }
        await page.waitForTimeout(500);
        /*
         * "Twin voll" ist der Zustand aus dem Nutzerbild, und er wird
         * hergestellt wie ein Leser ihn herstellt: mit dem Detail-Regler auf
         * seiner hoechsten Stufe. KEIN Ziehen an einer Grenze, das ist die
         * Bedingung aus AC5; der Regler ist eine Lesetiefe und keine Groesse.
         */
        const slider = page.locator('[data-testid="atlas-twin-depth"]');
        await slider.focus();
        for (let i = 0; i < 3; i += 1) {
            await page.keyboard.press('ArrowRight');
            await page.waitForTimeout(150);
        }
        await page.waitForTimeout(700);
        const twin = await twinEdge(page);
        extras.twin = twin;
        report.twinCutHasHint = twin.present === true
            && twin.hidden > 1
            && twin.markVisible === true
            && twin.mark.includes('bottom');
        const twinReadability = await readability('Twin voll, Graph offen, 100 Prozent');
        report.twinAtHundredPercentReadable =
            twinReadability.top.clipped.filter((entry) => entry.kind === 'cut-without-hint').length === 0
            && twinReadability.top.overlaps.length === 0;
        extras.shots.push(await shootAtRest(page, SHOT_TWIN, 'twin-full.png'));

        /*
         * Das Beweisbild zum ersten Befund, an genau der Stelle, an der er
         * fotografiert wurde.
         *
         * Der Nutzer hat am 2026-08-29 einen Tooltip fotografiert, der ueber
         * dem Detail-Regler des Twin lag. Aufgenommen wird darum der Tooltip
         * DIESES Reglers, offen: er steht neben seinem Regler und nicht darauf,
         * und die Chips daneben sind zu lesen.
         */
        const sliderHint = (await tooltipTriggers(page))
            .find((entry) => entry.name === 'twin-depth');
        if (sliderHint !== undefined) {
            await measureTooltip(page, sliderHint.index, 'pointer');
            await page.waitForTimeout(200);
            extras.tooltipShot = await tooltipCover(page, sliderHint.index);
            extras.shots.push(await shootAtRest(page, SHOT_TOOLTIP, 'tooltip-open.png', true));
            await closeTooltips(page);
            log(`tooltip-open.png zeigt "${extras.tooltipShot.name}" auf Seite `
                + `"${extras.tooltipShot.side}", verdeckt ${extras.tooltipShot.covers.length}`);
        }
        log(`Twin: ${twin.hidden}px unter der Kante, Marke "${twin.markText}" (${twin.mark}), `
            + `lesbar bei 100 Prozent: ${report.twinAtHundredPercentReadable}`);

        // ----------------------------------------------- 5f. Der Flow (AC6)
        /*
         * Ueber den Kopf des Twin und nicht ueber den Reiter, wortgleich mit
         * smoke-w8: der Kopf setzt das Subjekt UND schlaegt den Reiter auf, und
         * genau diesen Weg geht ein Leser. Ein Reiterwechsel allein zeigt den
         * Reiter zu dem Subjekt, das gerade gilt, und wenn noch keines gilt,
         * zeigt er seinen Grund statt eines Bildes.
         */
        await page.click('[data-testid="atlas-twin-subject"]');
        await page.waitForSelector('[data-testid="atlas-flow-overlay"]', { timeout: 30000 });
        await page.waitForFunction(
            () => Number(document.querySelector('[data-testid="atlas-flow"]')?.getAttribute('data-arrows') ?? '0') > 0,
            undefined,
            { timeout: 60000 },
        );
        await page.waitForSelector('[data-testid="atlas-flow-diagram"]', { timeout: 30000 });
        await page.waitForTimeout(600);
        const honesty = await honestyBlock(page);
        extras.honesty = honesty;
        report.honestyBlockParagraphs = honesty.paragraphs.length;
        report.honestyBlockChars = honesty.chars;
        report.walkBoundOnDiagram = honesty.bound.present === true
            && honesty.bound.onDiagram === true
            && honesty.bound.depth.length > 0
            && honesty.bound.cap.length > 0
            && honesty.bound.text.includes(honesty.bound.depth)
            && honesty.bound.text.includes(honesty.bound.cap);
        report.unresolvedCallsReported = honesty.unresolved > 0;
        log(`Ehrlichkeitsblock: ${report.honestyBlockParagraphs} Absaetze, `
            + `${report.honestyBlockChars} Zeichen; Grenze am Bild "${honesty.bound.text}" `
            + `(${report.walkBoundOnDiagram}); unaufgeloeste Aufrufe gemeldet: `
            + `${report.unresolvedCallsReported} (${honesty.unresolved})`);

        /*
         * Der schrittbezogene Satz BLEIBT (AC6, letzter Absatz). Gesucht wird
         * ein Schritt, der im Bild nichts anleuchtet: ein erhobener Fehlertyp
         * oder eine Umgebungslesung. Der Lauf geht die Schritte durch, bis er
         * einen findet, statt zu behaupten, es gebe einen.
         */
        const steps = await page.evaluate(() => globalThis.__atlasFlow?.steps ?? 0);
        extras.stepNoteSearch = { steps, foundAt: -1, note: '' };
        for (let index = 0; index < steps; index += 1) {
            await page.evaluate((at) => {
                const buttons = [...document.querySelectorAll('[data-testid="atlas-flow-step-button"]')];
                buttons[at]?.click();
            }, index);
            await page.waitForTimeout(120);
            const note = await page.evaluate(() =>
                document.querySelector('[data-testid="atlas-flow-no-hit"]')
                    ?.textContent?.replace(/\s+/g, ' ').trim() ?? '');
            if (note.length > 0) {
                extras.stepNoteSearch = { steps, foundAt: index, note };
                report.stepNoteKept = note.includes('not an arrow in the picture');
                break;
            }
        }
        log(`schrittbezogener Satz: ${report.stepNoteKept} `
            + `(Schritt ${extras.stepNoteSearch.foundAt} von ${steps})`);

        await countTitles('flow');
        covering += await sweepTooltips('flow');
        await readability('Flow-Erklaerer offen');

        /*
         * Fuer das Bild wird der Bereich hochgezogen, und danach wieder
         * zurueckgesetzt.
         *
         * Das Beweisbild zu AC6 soll den gekuerzten Ehrlichkeitsblock ZEIGEN,
         * und der steht unter dem Diagramm. In der Vorgabehoehe der Zone liegt
         * er unterhalb der Kante; ein Bild, das ihn nicht enthaelt, belegt
         * ueber ihn nichts. Gezogen wird ueber die Tastatur am Griff, also
         * ueber genau die Bedienung, die W8 fuer ihn gebaut hat, und der
         * Doppelklick danach setzt genau diese eine Grenze wieder auf ihre
         * Vorgabe, damit die Messungen danach in der Vorgabelage stehen.
         */
        await page.focus('[data-testid="atlas-split-explain"]');
        for (let i = 0; i < 8; i += 1) {
            await page.keyboard.press('Shift+ArrowUp');
        }
        await page.waitForTimeout(500);
        extras.shots.push(await shootAtRest(page, SHOT_FLOW, 'flow-short.png'));
        await page.dblclick('[data-testid="atlas-split-explain"]');
        await page.waitForTimeout(400);

        // ------------------------------------------- 5g. Der Chat (AC6b, AC6e)
        await askInLine(page, `@${WALK_TARGET} what does it do?`);
        await page.waitForTimeout(700);
        await chooseTab(page, 'chat');
        await page.waitForSelector('[data-testid="atlas-chat-turn"]', { timeout: 20000 });
        const askedAt = await chatState(page);
        extras.chat = { asked: askedAt };

        // Die Tiefe aendern: das Angebot muss an der letzten Antwort erscheinen.
        const otherDepth = askedAt.depth === '2' ? '0' : '2';
        await page.click(`[data-testid="atlas-chat-depth-option"][data-value="${otherDepth}"]`);
        await page.waitForTimeout(400);
        const offered = await chatState(page);
        extras.chat.offered = offered;
        const last = offered.turns[offered.turns.length - 1];
        report.depthChangeOffersRerun = last !== undefined
            && last.rerun.length > 0
            && last.rerun.includes(otherDepth);

        if (report.depthChangeOffersRerun) {
            await page.click('[data-testid="atlas-chat-rerun"]');
            await page.waitForTimeout(900);
            const after = await chatState(page);
            extras.chat.after = after;
            report.rerunMakesNewTurn = after.turns.length === offered.turns.length + 1
                && after.turns[after.turns.length - 1].question === last.question;
            report.oldTurnKeepsItsDepth =
                after.turns[offered.turns.length - 1]?.depth === askedAt.depth
                && after.turns[after.turns.length - 1]?.depth === otherDepth;
        }
        log(`Tiefe geaendert: Angebot "${last?.rerun}" (${report.depthChangeOffersRerun}), `
            + `neuer Zug ${report.rerunMakesNewTurn}, alter Zug behaelt seine Tiefe `
            + `${report.oldTurnKeepsItsDepth}`);

        /*
         * Der Kopf des Chats bleibt stehen, egal was scrollt (AC6e).
         *
         * Gemessen im schlimmsten Fall des Nutzerbildes: Erklaeren-Bereich
         * offen, langer Verlauf, und jeder Bereich ans Ende gefahren. Vor W8
         * lagen Flow-Erklaerer und Chat uebereinander und waren zusammen hoeher
         * als das Fenster, und der ganze Chat wanderte dabei aus dem Bild.
         */
        for (let i = 0; i < 3; i += 1) {
            await askInLine(page, `@${WALK_TARGET} who calls it ${i}?`);
        }
        await chooseTab(page, 'chat');
        await scrollRegionsToEnd(page, READABILITY_EXCLUSIONS);
        await page.waitForTimeout(300);
        const head100 = await chatHeadVisible(page);
        report.chatHeadVisibleAt100 = head100.visible === true
            && head100.chips === true && head100.clear === true;
        extras.chatHead = { at100: head100 };
        await countTitles('chat');
        covering += await sweepTooltips('chat');

        await page.setViewportSize({ ...SMALL_VIEWPORT });
        await page.waitForTimeout(700);
        await scrollRegionsToEnd(page, READABILITY_EXCLUSIONS);
        await page.waitForTimeout(300);
        const head67 = await chatHeadVisible(page);
        report.chatHeadVisibleAt67 = head67.visible === true
            && head67.chips === true && head67.clear === true;
        extras.chatHead.at67 = head67;
        await readability('Chat bei 67 Prozent');
        await page.setViewportSize({ ...MAIN_VIEWPORT });
        await page.waitForTimeout(600);
        log(`Kopf des Chats sichtbar: 100 Prozent ${report.chatHeadVisibleAt100}, `
            + `67 Prozent ${report.chatHeadVisibleAt67}`);

        // ------------------------------------------ 5h. Die Frage (AC6f)
        await openWhyAndChoose2(page);
        const whyOpen = await whyState(page);
        await page.keyboard.press('Escape');
        await page.waitForTimeout(300);
        const afterEscape = await whyState(page);
        report.whyClosesByEscape = whyOpen.why === true && afterEscape.why === false;
        extras.why = { opened: whyOpen, afterEscape };
        log(`Escape schliesst die Frage: ${report.whyClosesByEscape} `
            + `(Knopf heisst "${whyOpen.decline}")`);

        // Von Hand wieder auf, dann eine Datei im Explorer anklicken.
        await openWhyAndChoose2(page);
        const beforeClick = await whyState(page);
        await page.click(`[data-testid="atlas-tree-row"][data-path="${EXPLORER_FILE}"]`);
        await page.waitForFunction(
            (expected) => globalThis.__atlasReader?.document?.path === expected,
            EXPLORER_FILE,
            { timeout: 40000 },
        );
        await page.waitForTimeout(700);
        const afterClick = await whyState(page);
        report.whyClosesOnOpenFile = beforeClick.why === true && afterClick.why === false;
        report.fileOpensBehindNothing = afterClick.readerPath === EXPLORER_FILE
            && afterClick.readerVisible === true
            && afterClick.why === false;
        extras.why.explorer = { before: beforeClick, after: afterClick, path: EXPLORER_FILE };
        log(`Frage schliesst beim Oeffnen: ${report.whyClosesOnOpenFile}; `
            + `der Code steht danach im Reader: ${report.fileOpensBehindNothing} `
            + `(${afterClick.readerPath})`);

        await countTitles('nach dem Oeffnen');
        covering += await sweepTooltips('nach dem Oeffnen');
        await readability('Datei offen, Frage weg');

        report.tooltipCoversNothing = covering === 0 && report.domTooltipsMeasured > 0;
        report.screenshotsAtRest =
            extras.shots.length === 4 && extras.shots.every((shot) => shot.atRest === true);
        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w8b] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w8b] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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

    if (report.nativeTitlesWithExplanation < 0) {
        report.nativeTitlesWithExplanation = -1;
    }

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            walkTarget: WALK_TARGET,
            explorerFile: EXPLORER_FILE,
            method:
                'Jeder eigene Tooltip wird EINZELN geoeffnet (abwechselnd mit dem Zeiger und mit '
                + 'der Tastatur) und danach gegen die geschuetzten Flaechen gehalten: den eigenen '
                + 'Ausloeser, Regler und Eingabefelder, und die mit data-hint-keep ausgewiesenen '
                + 'Beschriftungen der Sektion. Dieselbe Liste, nach der die Oberflaeche selbst '
                + 'platziert (src/ui/tooltip/tooltip-model.ts). Native title-Attribute werden an '
                + 'jedem Halt gezaehlt, und zwar nur die, die mehr sagen als der sichtbare Text.',
            unresolvedCallsMethod:
                'Gezaehlt werden die Symbole des Walks, die der Index GENANNT und nicht aufgeloest '
                + 'hat (kind "unknown", siehe unresolvedCallee in src/provider/closure.ts). Was '
                + 'diese Zahl NICHT sieht: ein Aufruf, den der Index ganz ohne qualifizierten '
                + 'Namen meldet, wird schon im Walk verworfen (getClosure filtert auf einen '
                + 'nichtleeren Zielschluessel) und ist an dieser Stelle nicht mehr zaehlbar. Die '
                + 'Zahl ist damit eine Untergrenze, und der Satz unter dem Bild sagt "at least".',
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = [SHOT_TOOLTIP, SHOT_WORDS, SHOT_TWIN, SHOT_FLOW].every((file) => existsSync(file));
    const ok =
        failure === null
        && report.nativeTitlesWithExplanation === 0
        && report.domTooltipsMeasured >= 10
        && report.tooltipCoversNothing === true
        && report.tooltipOpensByKeyboard === true
        && report.tooltipClosesByEscape === true
        && report.collapseLabelsAreWords === true
        && report.collapseLabelFollowsView === true
        && report.viewToggleShowsState === true
        && report.twinCutHasHint === true
        && report.twinAtHundredPercentReadable === true
        && report.honestyBlockChars <= 400
        && report.honestyBlockChars >= 0
        && report.honestyBlockParagraphs <= 2
        && report.honestyBlockParagraphs >= 1
        && report.walkBoundOnDiagram === true
        && typeof report.unresolvedCallsReported === 'boolean'
        && report.stepNoteKept === true
        && report.depthChangeOffersRerun === true
        && report.rerunMakesNewTurn === true
        && report.oldTurnKeepsItsDepth === true
        && report.placeholderShowsExample === true
        && report.examplesClickable >= 3
        && report.examplesUseRealSymbols === true
        && report.handleVisibleWithoutHover === true
        && report.handleHitAreaPx >= 10
        && report.allZoneHandlesLookAlike === true
        && report.chatHeadVisibleAt100 === true
        && report.chatHeadVisibleAt67 === true
        && report.whyClosesOnOpenFile === true
        && report.whyClosesByEscape === true
        && report.fileOpensBehindNothing === true
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
        console.error('[smoke-w8b] W8b-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w8b] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W8b-Smoke gruen.');
}

/**
 * Die Frage von Hand aufrufen, ohne eine Karte zu waehlen.
 *
 * Genau die Lage aus dem Nutzerbefund: `whyReopened` ist gesetzt, eine Datei ist
 * offen, und die Frage steht trotzdem da. Wer hier eine Karte klickte, waere in
 * einem anderen Fall.
 *
 * Der Zeiger geht danach von der Menuezeile weg, und das ist keine Kosmetik.
 * Ein Klick von Playwright faehrt den Zeiger auf den Knopf und LAESST ihn dort;
 * der Tooltip dieses Menuepunktes steht damit offen, und Escape gehoert dann
 * ihm (er nimmt die Taste in der einfangenden Phase, siehe src/ui/tooltip/Hint.tsx).
 * Das ist die richtige Reihenfolge fuer einen Leser und die falsche Lage fuer
 * diese Messung: gefragt ist, ob Escape DIE FRAGE schliesst, und ein Leser, der
 * die Frage vor sich hat, hat den Zeiger nicht mehr auf dem Menuepunkt, ueber
 * den er sie aufgerufen hat. Der Zeiger geht darum in die Mitte der Frage.
 */
async function openWhyAndChoose2(page) {
    await page.click('[data-menu="a-why"]');
    await page.waitForSelector('[data-testid="atlas-why"]', { timeout: 20000 });
    const box = await page.locator('[data-testid="atlas-why-headline"]').boundingBox();
    if (box !== null) {
        await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    }
    await closeTooltips(page);
    await page.waitForTimeout(400);
}

main().catch((err) => {
    console.error('[smoke-w8b] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
