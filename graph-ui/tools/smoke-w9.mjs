#!/usr/bin/env node
/*
 * W9-Smoke: die Kanten zeigen, was sie sind, die Legende zaehlt, was da ist,
 * der Filter nimmt eine Art aus dem Bild, und die Hierarchie kennt mehr als
 * Aufrufe.
 *
 * Martins Befund vom 2026-08-29: "Man sieht kaum verschiedenartige Kanten im
 * Graph. Das sollte ja schon mit drin sein." Er hat recht, und die Ursache ist
 * gemessen: /api/layout liefert fuer fixtures/atlas-sample zwoelf Kantenarten,
 * gezeichnet wurden sie mit einer Deckkraft zwischen 0.04 und 0.25, und bei
 * diesen Werten sind zwoelf Farben ein gleichmaessiger Schleier.
 *
 * ## Warum dieser Lauf Pixel liest und nicht Absichten
 *
 * Die Zusicherung dieses Zyklus ist eine ueber ein BILD. Ein Unit-Test kann
 * zeigen, dass die Deckkraft-Kurve die Zahlen liefert, die im Kopf von
 * EdgeLines.tsx stehen; er kann nicht zeigen, dass ein Leser danach zwoelf
 * Farben sieht. Also wird hier die gerenderte Szene ausgelesen.
 *
 * Gemessen wird mit einer Differenz und nicht mit einer Farbsuche, und das ist
 * der eine Kunstgriff dieses Laufs:
 *
 *   1. Ein Bild der Szene, wie sie dasteht (die Grundlage).
 *   2. Fuer jede Kantenart: dieselbe Szene, diese eine Art abgeschaltet.
 *   3. Was zwischen beiden Bildern an Licht FEHLT, ist genau das Licht, das
 *      diese Art beigetragen hat. Seine Richtung im Farbraum ist die Farbe, in
 *      der diese Art wirklich gezeichnet wurde.
 *
 * Der Unterschied zu "such alle Pixel in der Farbe X" ist kein technischer: die
 * Suche findet auch Mischungen und Sterne, die zufaellig so aussehen, und sie
 * beweist nie, dass ein Pixel VON dieser Kante kommt. Die Differenz beweist
 * genau das, weil zwischen den beiden Bildern nichts anderes veraendert wurde.
 * Der Filter ist damit nicht nur die Sache, die geprueft wird, sondern auch das
 * Messinstrument.
 *
 * Zwei Schwellen gehoeren dazu und stehen beide im Artefakt:
 *
 *  - `colorDistanceThreshold` (40): ab diesem Abstand gelten zwei gemessene
 *    Farben als verschieden. Verglichen wird euklidisch in RGB, nachdem beide
 *    auf den gleichen Maximalkanal gebracht wurden; sonst wuerde eine dunkler
 *    gezeichnete Kante als eigene Farbe zaehlen. Der kleinste Abstand zwischen
 *    zwei Tabellenfarben, die in diesem Fixture zusammen im Bild stehen, ist
 *    so gerechnet 57.6 (CALLS gegen den Vorgabeton), der zweitkleinste 67.8.
 *    40 liegt darunter und ueber dem, was die Messung selbst streut.
 *  - `colorMatchThreshold` (25): ab diesem Abstand gilt ein Pixel als "zeigt
 *    diese Art". Enger als die erste, weil hier eine Verwechslung teurer ist:
 *    ein einziges Mischpixel, das zufaellig in die Naehe faellt, wuerde die
 *    Zusicherung "die Art ist aus dem Bild verschwunden" scheitern lassen,
 *    obwohl sie stimmt.
 *
 * ## Was die Kamera macht, waehrend gemessen wird
 *
 * Nichts, und das ist eine Bedingung der Messung. Die Szene dreht sich nach 60
 * Sekunden ohne Zutun von selbst weiter (IdleAutoRotate), und zwei Bilder aus
 * verschiedenen Kamerapositionen sind nicht vergleichbar. Vor jeder Aufnahme
 * geht darum ein Radereignis mit Weg null an den Canvas: es stellt die Uhr
 * zurueck, ohne die Kamera zu bewegen (getZoomScale(0) ist 1). Bewiesen wird
 * das nicht behauptet: am Ende wird die Grundlage noch einmal aufgenommen und
 * gegen die erste gehalten (`sceneDriftMeanAbs`, muss praktisch null sein).
 *
 * ## Was auf den Beweisbildern steht
 *
 * Der Ruhezustand, und nichts sonst. Bis W9-1 scrollte dieser Lauf vor jeder
 * Aufnahme die Legende zu den Kantenarten, weil sie hinter drei Absaetzen
 * Fliesstext lagen. Auf den Bildern stand danach oben und unten je ein
 * angeschnittener Satzrest, und ein Bild, das einen Zustand zeigt, den ein
 * Leser nie zu sehen bekommt, beweist ueber die Oberflaeche nichts. Vor jeder
 * Aufnahme geht jetzt jeder Bereich an den Anfang; dass er dort steht, wird
 * gemessen und steht als `screenshotsAtRest` samt der Lage jedes Bereichs im
 * Artefakt. Die Arten stehen dafuer in der Legende oben (galaxy-legend.ts).
 *
 * ## Ablauf
 *
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4460, dist/ auf einem zweiten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre
 *   6a. /api/layout selbst holen und die Kantenarten zaehlen
 *   6b. Legende aufklappen: Zeilen gegen die gezaehlten Arten halten
 *   6c. Grundlage aufnehmen, dann Art fuer Art abschalten und die Differenz
 *       messen: welche Farbe hat diese Art im Bild wirklich
 *   6d. Eine Art bleibt aus: Zeile noch da, gedimmt, Kopf sagt die Zahl
 *   6e. Einstieg waehlen (createUser), Hierarchie: Walk-Kanten und die
 *       Beziehungen, die der Index dazu kennt, beide zaehlbar und filterbar
 *   6f. Spalten unveraendert, mit und ohne die zusaetzlichen Kanten
 *   7. abraeumen, Restprozesse zaehlen, JSON und drei Bilder schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w9).
 *
 * ## Ports
 *
 * Ab 4460. 4390 und 4391 gehoeren der Vorschau des Nutzers, 4141 seinem
 * Modell-Sidecar, 4360 und 4400 den Eval-Laeufen. Dieser Lauf fasst keinen
 * davon an.
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
    measureReadability,
    resetScroll,
    scrollRegionsToEnd,
} from './lib/readability.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w9';
const OUT_DIR = join(ROOT, 'verification', 'w9');
const OUT_JSON = join(OUT_DIR, 'edges.json');
const SHOT_GALAXY = join(OUT_DIR, 'galaxy-edges.png');
const SHOT_HIERARCHY = join(OUT_DIR, 'hierarchy-edges.png');
const SHOT_FILTER = join(OUT_DIR, 'legend-filter.png');

/** 4390/4391 und 4141 gehoeren dem Nutzer, 4360/4400 der Eval. Ab hier ist frei. */
const MIN_PORT = 4460;

/** Ab diesem Abstand sind zwei gemessene Farben verschiedene Farben. */
const COLOR_DISTANCE_THRESHOLD = 40;

/** Ab diesem Abstand zeigt ein Pixel diese Art. Enger, siehe Kopf. */
const COLOR_MATCH_THRESHOLD = 25;

/**
 * Ab dieser Summe ueber die drei Kanaele hat sich ein Pixel geaendert.
 *
 * Zwoelf, also im Mittel vier Stufen je Kanal. Die Aufnahmen sind verlustfrei
 * und die Szene rendert deterministisch; was darunter liegt, ist die
 * Rundung des Compositors und keine Kante.
 */
const DIFF_EPSILON = 12;

/**
 * Ab dieser Summe ueber die drei Kanaele leuchtet ein Pixel ueberhaupt.
 *
 * Gerechnet nach Abzug des Szenengrunds (#0D0F12). Darunter ist die Richtung
 * im Farbraum nicht mehr bestimmbar: bei drei Stufen Helligkeit entscheidet
 * die Rundung, welche Farbe herauskommt.
 */
const LIGHT_FLOOR = 30;

/** Der Grund der Szene, wie GraphScene ihn setzt. */
const SCENE_BACKGROUND = [13, 15, 18];

/** Das Symbol, ueber das der Einstiegs-Walk laeuft. Dasselbe wie in W5c. */
const WALK_TARGET = 'createUser';

/** Die Art, an der das Ausblenden gemessen wird, wenn es sie gibt. */
const FILTER_CANDIDATES = ['USAGE', 'IMPORTS', 'DEFINES'];

const MAIN_VIEWPORT = { width: 1680, height: 1050 };

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

const log = (...parts) => console.log('[smoke-w9]', ...parts);
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

/** Der Griff des Graph-Panels, ohne die Funktion darin. */
const galaxySeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasGalaxy;
        if (seam === undefined) {
            return null;
        }
        const { clickNode, labelBoxes, ...rest } = seam;
        return JSON.parse(JSON.stringify({ ...rest, labels: labelBoxes?.length ?? 0 }));
    });

/** Die Zeilen der Legende, so wie sie wirklich dastehen. */
const legendRows = (page) =>
    page.evaluate(() =>
        [...document.querySelectorAll('[data-testid="atlas-galaxy-legend-swatch"]')].map((row) => {
            const rect = row.getBoundingClientRect();
            const style = globalThis.getComputedStyle(row);
            return {
                type: row.getAttribute('data-type') ?? '',
                count: Number(row.getAttribute('data-count') ?? '-1'),
                color: row.getAttribute('data-color') ?? '',
                hidden: row.getAttribute('data-hidden') === 'true',
                pressed: row.getAttribute('aria-pressed') ?? '',
                tag: row.tagName,
                title: row.getAttribute('title') ?? '',
                text: (row.textContent ?? '').replace(/\s+/g, ' ').trim(),
                opacity: Number(style.opacity),
                struckThrough: style.textDecorationLine.includes('line-through'),
                visible:
                    rect.width > 0
                    && rect.height > 0
                    && style.visibility !== 'hidden'
                    && style.display !== 'none',
                rect: { x: rect.x, y: rect.y, width: rect.width, height: rect.height },
            };
        }));

/**
 * Der Kopf des Panels, beide Zeilen, mit der Frage, ob sie ganz dastehen.
 *
 * Oben der Satz ueber das Bild (welches Symbol, wie viele, wie tief), darunter
 * seit W9 die Zeile ueber die Linien (woraus sie bestehen, was der Filter
 * wegnimmt). Gelesen wird, was WIRKLICH dasteht, samt `title` und samt der
 * Frage, ob der Kasten den Text abschneidet: aus "der Kopf sagt es" soll
 * niemand schliessen muessen, dass es auch zu lesen ist.
 */
const panelHead = (page) =>
    page.evaluate(() => {
        const read = (id) => {
            const node = document.querySelector(`[data-testid="${id}"]`);
            if (node === null) {
                return { present: false, text: '', title: '', clipped: false, lines: 0 };
            }
            const style = globalThis.getComputedStyle(node);
            return {
                present: true,
                text: node.textContent?.replace(/\s+/g, ' ').trim() ?? '',
                title: node.getAttribute('title') ?? '',
                clipped: node.scrollWidth > node.clientWidth + 1,
                lines: Math.round(
                    node.getBoundingClientRect().height / (Number.parseFloat(style.lineHeight) || 1),
                ),
            };
        };
        const headline = read('atlas-galaxy-headline');
        const edges = read('atlas-galaxy-edgenote');
        return { headline, edges, text: `${headline.text} ${edges.text}`.trim() };
    });

/** Das Rechteck der Zeichenflaeche, in Fensterkoordinaten. */
const sceneRect = (page) =>
    page.evaluate(() => {
        const node = document.querySelector('[data-testid="atlas-galaxy-scene"]');
        if (node === null) {
            return null;
        }
        const rect = node.getBoundingClientRect();
        return {
            x: Math.round(rect.x),
            y: Math.round(rect.y),
            width: Math.round(rect.width),
            height: Math.round(rect.height),
        };
    });

/**
 * Die Uhr der Leerlauf-Drehung zuruecksetzen, ohne die Kamera zu bewegen.
 *
 * Ein Radereignis mit Weg null: IdleAutoRotate hoert darauf und setzt
 * `autoRotate` zurueck, OrbitControls rechnet daraus einen Zoomfaktor von
 * genau 1. Siehe Kopf.
 */
const nudge = (page) =>
    page.evaluate(() => {
        const canvas = document.querySelector('[data-testid="atlas-galaxy-scene"] canvas');
        if (canvas === null) {
            return false;
        }
        canvas.dispatchEvent(new WheelEvent('wheel', { deltaY: 0, bubbles: true, cancelable: true }));
        return true;
    });

/** Aus der Szene herauszoomen, wie ein Leser es tut, um alles zu sehen. */
const zoomOut = (page, notches) =>
    page.evaluate((steps) => {
        const canvas = document.querySelector('[data-testid="atlas-galaxy-scene"] canvas');
        if (canvas === null) {
            return false;
        }
        for (let i = 0; i < steps; i += 1) {
            canvas.dispatchEvent(new WheelEvent('wheel', { deltaY: 120, bubbles: true, cancelable: true }));
        }
        return true;
    }, notches);

/**
 * Ein Bild der Zeichenflaeche aufnehmen und im Browser ablegen.
 *
 * Der Umweg ueber die Seite hat einen Grund: der Canvas laeuft ohne
 * `preserveDrawingBuffer`, `toDataURL` gaebe also ein leeres Bild. Was
 * Playwright aufnimmt, ist dagegen genau das, was auf dem Schirm steht, also
 * das, worueber die Zusicherung redet. Die Pixel bleiben in der Seite, weil
 * eine Million Zahlen ueber die Bruecke ins Skript zu schieben Minuten kosten
 * wuerde; gerechnet wird dort, zurueck kommen Kennzahlen.
 */
async function grab(page, key, clip) {
    const shot = await page.screenshot({ clip });
    return page.evaluate(async ({ name, data }) => {
        const image = new Image();
        image.src = `data:image/png;base64,${data}`;
        await image.decode();
        const canvas = document.createElement('canvas');
        canvas.width = image.naturalWidth;
        canvas.height = image.naturalHeight;
        const ctx = canvas.getContext('2d', { willReadFrequently: true });
        ctx.drawImage(image, 0, 0);
        globalThis.__w9 = globalThis.__w9 ?? {};
        globalThis.__w9[name] = ctx.getImageData(0, 0, canvas.width, canvas.height);
        return { width: canvas.width, height: canvas.height };
    }, { name: key, data: shot.toString('base64') });
}

/**
 * Zwei abgelegte Bilder vergleichen.
 *
 * `gainedPixels` ist die Zahl der Pixel, an denen das erste Bild HELLER ist als
 * das zweite: genau das Licht, das die abgeschaltete Art beigetragen hat.
 * `lostPixels` ist die Gegenrichtung und muss null sein: eine Art abzuschalten
 * darf nirgends Licht hinzufuegen.
 */
const compareShots = (page, options) =>
    page.evaluate((input) => {
        const store = globalThis.__w9 ?? {};
        const first = store[input.base];
        const second = store[input.variant];
        if (first === undefined || second === undefined) {
            return null;
        }
        const a = first.data;
        const b = second.data;
        const ground = input.background;
        const normalise = (r, g, blue) => {
            const top = Math.max(r, g, blue);
            return top <= 0 ? [0, 0, 0] : [(r * 255) / top, (g * 255) / top, (blue * 255) / top];
        };
        const distance = (p, q) =>
            Math.sqrt((p[0] - q[0]) ** 2 + (p[1] - q[1]) ** 2 + (p[2] - q[2]) ** 2);
        const hexOf = (rgb) => '#' + rgb
            .map((value) => Math.max(0, Math.min(255, Math.round(value))).toString(16).padStart(2, '0'))
            .join('');
        const wanted = input.hex === ''
            ? null
            : normalise(
                parseInt(input.hex.slice(1, 3), 16),
                parseInt(input.hex.slice(3, 5), 16),
                parseInt(input.hex.slice(5, 7), 16),
            );

        let gained = 0;
        let lost = 0;
        let sumR = 0;
        let sumG = 0;
        let sumB = 0;
        let absSum = 0;
        let matchBefore = 0;
        let matchAfter = 0;
        let litBefore = 0;
        for (let i = 0; i < a.length; i += 4) {
            const dr = a[i] - b[i];
            const dg = a[i + 1] - b[i + 1];
            const db = a[i + 2] - b[i + 2];
            absSum += Math.abs(dr) + Math.abs(dg) + Math.abs(db);
            const plus = Math.max(0, dr) + Math.max(0, dg) + Math.max(0, db);
            const minus = Math.max(0, -dr) + Math.max(0, -dg) + Math.max(0, -db);
            if (plus >= input.epsilon) {
                gained += 1;
                sumR += Math.max(0, dr);
                sumG += Math.max(0, dg);
                sumB += Math.max(0, db);
            }
            if (minus >= input.epsilon) {
                lost += 1;
            }
            if (wanted !== null) {
                const ar = a[i] - ground[0];
                const ag = a[i + 1] - ground[1];
                const ab = a[i + 2] - ground[2];
                if (ar + ag + ab >= input.floor) {
                    litBefore += 1;
                    const seen = normalise(Math.max(0, ar), Math.max(0, ag), Math.max(0, ab));
                    if (distance(seen, wanted) <= input.matchThreshold) {
                        matchBefore += 1;
                    }
                }
                const br = b[i] - ground[0];
                const bg = b[i + 1] - ground[1];
                const bb = b[i + 2] - ground[2];
                if (br + bg + bb >= input.floor) {
                    const seen = normalise(Math.max(0, br), Math.max(0, bg), Math.max(0, bb));
                    if (distance(seen, wanted) <= input.matchThreshold) {
                        matchAfter += 1;
                    }
                }
            }
        }
        const pixels = a.length / 4;
        const mean = gained === 0 ? null : [sumR / gained, sumG / gained, sumB / gained];
        return {
            pixels,
            gainedPixels: gained,
            lostPixels: lost,
            meanDiff: mean === null ? null : mean.map((value) => Number(value.toFixed(2))),
            renderedColor: mean === null ? '' : hexOf(normalise(mean[0], mean[1], mean[2])),
            meanAbsDiff: Number((absSum / pixels / 3).toFixed(4)),
            litPixels: litBefore,
            matchBefore,
            matchAfter,
        };
    }, options);

/** Der Abstand zweier Farben, so wie der Lauf ihn rechnet. */
function colorDistance(first, second) {
    const parse = (hex) => [
        parseInt(hex.slice(1, 3), 16),
        parseInt(hex.slice(3, 5), 16),
        parseInt(hex.slice(5, 7), 16),
    ];
    const normalise = (rgb) => {
        const top = Math.max(...rgb);
        return top <= 0 ? [0, 0, 0] : rgb.map((value) => (value * 255) / top);
    };
    const a = normalise(parse(first));
    const b = normalise(parse(second));
    return Math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 + (a[2] - b[2]) ** 2);
}

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
}

/** Die Legende aufklappen, ueber genau den Schalter, den ein Leser klickt. */
async function openLegend(page) {
    const expanded = await page.getAttribute('[data-testid="atlas-galaxy-legend-toggle"]', 'aria-expanded');
    if (expanded !== 'true') {
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
    }
    await page.waitForSelector('[data-testid="atlas-galaxy-legend"]', { timeout: 10000 });
    await page.waitForTimeout(250);
}

/**
 * Wo jeder scrollbare Bereich gerade steht, und ob er am Anfang steht.
 *
 * Die Frage gehoert zu den Beweisbildern (W9-1). Bis dahin scrollte dieser Lauf
 * die Legende vor der Aufnahme zu den Kantenarten, weil sie hinter drei
 * Absaetzen Fliesstext lagen; auf dem Bild stand danach oben und unten je ein
 * angeschnittener Satzrest, und das Bild zeigte einen Zustand, den kein Leser je
 * zu sehen bekommt. Aufgenommen wird jetzt der RUHEZUSTAND, und dass es einer
 * ist, steht als Zahl im Artefakt statt als Zusicherung im Kopf.
 *
 * Die Innereien des Editors bleiben aussen vor: er fuehrt seinen Bildlauf
 * selbst, und ihn von aussen zu stellen hiesse, an einer fremden Bibliothek zu
 * drehen. Dieselbe Ausnahme wie in tools/lib/readability.mjs.
 */
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
            const marks = [
                ...node.querySelectorAll('[data-scroll-hint]'),
                ...(node.parentElement === null
                    ? []
                    : [...node.parentElement.children].filter((child) =>
                        child !== node && child.hasAttribute('data-scroll-hint'))),
            ];
            regions.push({
                name: node.getAttribute('data-testid')
                    ?? (node.getAttribute('class') ?? '').split(/\s+/).filter(Boolean)[0]
                    ?? node.tagName.toLowerCase(),
                top: Math.round(node.scrollTop),
                left: Math.round(node.scrollLeft),
                hidden: Math.round(node.scrollHeight - node.clientHeight),
                hint: marks.map((mark) => mark.getAttribute('data-scroll-hint') ?? '').join(','),
            });
        }
        return {
            regions,
            atRest: regions.every((region) => region.top <= 1 && region.left <= 1),
        };
    });

/**
 * Ein Beweisbild im Ruhezustand aufnehmen.
 *
 * Erst jeden Bereich an den Anfang, dann warten, bis die Oberflaeche das
 * mitbekommen hat (die Kante der Legende haengt an ihrem Bildlauf und wird neu
 * gezeichnet), dann die Lage aufschreiben, dann das Bild. In dieser
 * Reihenfolge, damit die aufgeschriebene Lage die des Bildes ist und nicht die
 * davor.
 */
async function shootAtRest(page, file, name) {
    await resetScroll(page, READABILITY_EXCLUSIONS);
    await page.waitForTimeout(350);
    const state = await scrollState(page);
    await page.screenshot({ path: file, fullPage: false });
    log(`${name}: aufgenommen im Ruhezustand=${state.atRest} `
        + `(${state.regions.map((region) => `${region.name} ${region.top}`).join(', ')})`);
    return { name, atRest: state.atRest, regions: state.regions };
}

/** Eine Kantenart an- oder abschalten. */
async function toggleKind(page, type) {
    await page.click(`[data-testid="atlas-galaxy-legend-swatch"][data-type="${type}"]`);
    await page.waitForTimeout(260);
}

/** Die Frage wieder aufrufen und einen Modus waehlen. Wortgleich mit smoke-w5c. */
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
    await page.waitForFunction(
        () => (globalThis.__atlasGalaxy?.hierarchy?.nodes ?? 0) > 0,
        undefined,
        { timeout: 60000 },
    );
    // Die Kamerafahrt und der erste Bildlauf der Szene abwarten.
    await page.waitForTimeout(1800);
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
        layoutEdgeTypes: {},
        distinctColorsRendered: 0,
        renderedColors: [],
        colorDistanceThreshold: COLOR_DISTANCE_THRESHOLD,
        colorMatchThreshold: COLOR_MATCH_THRESHOLD,
        legendShowsCounts: false,
        legendMatchesLayout: false,
        legendListsAbsentTypes: true,
        legendSortedByCount: false,
        filterHidesType: false,
        filterKeepsRowVisible: false,
        filterRowDimmed: false,
        filterHeaderText: '',
        filterSurvivesViewSwitch: false,
        hierarchyWalkEdges: 0,
        hierarchyExtraEdges: 0,
        hierarchyHeaderExplainsCounts: false,
        hierarchyExtraEdgesFilterable: false,
        hierarchyDeterminismUnchanged: false,
        hierarchyColumnsFromCallsOnly: false,
        overlapViolations: 0,
        clippingViolations: 0,
        /*
         * Die Kennzahl aus W9-1: eine Zeile, die an einer Bildlaufkante halb
         * dasteht, ohne dass der Kasten sagt, dass es weitergeht. Sie steckt in
         * `clippingViolations` mit drin (die Messung fuehrt beide Faelle in
         * derselben Liste, damit jeder Lauf sie ohne eigene Aenderung
         * mitzaehlt); hier steht sie noch einmal allein, weil sie einen anderen
         * Fehler benennt als ein hart abgeschnittener Kasten.
         */
        cutWithoutHint: 0,
        /** Ob die drei Beweisbilder den Ruhezustand zeigen. Siehe shootAtRest. */
        screenshotsAtRest: false,
        port: 0,
        leftoverProcesses: 0,
    };
    const extras = {
        blockedRequests: [],
        consoleErrors: [],
        pageErrors: [],
        readability: [],
        measurements: [],
        shots: [],
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
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w9-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w9-run-');
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

        // ------------------------------- 5. Die Kantenarten selbst zaehlen
        /*
         * Dieselbe Route, die das Panel laedt, ein zweites Mal und unabhaengig
         * gelesen. Die Legende wird gleich gegen DIESE Zahlen gehalten, und
         * eine Legende gegen die Zahlen zu pruefen, die sie selbst gemeldet
         * hat, waere keine Pruefung.
         */
        const layoutUrl = `http://127.0.0.1:${uiPort}/api/layout?project=${PROJECT}&max_nodes=5000`;
        const layoutResponse = await fetch(layoutUrl, { headers: { Accept: 'application/json' } });
        if (!layoutResponse.ok) {
            throw new Error(`/api/layout antwortete mit HTTP ${layoutResponse.status}`);
        }
        const layout = await layoutResponse.json();
        const tally = {};
        for (const edge of layout.edges ?? []) {
            const type = typeof edge.type === 'string' ? edge.type : '';
            if (type.length > 0) {
                tally[type] = (tally[type] ?? 0) + 1;
            }
        }
        report.layoutEdgeTypes = Object.fromEntries(
            Object.entries(tally).sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)),
        );
        extras.layout = {
            url: `/api/layout?project=${PROJECT}&max_nodes=5000`,
            nodes: (layout.nodes ?? []).length,
            edges: (layout.edges ?? []).length,
        };
        log(`/api/layout: ${extras.layout.nodes} Knoten, ${extras.layout.edges} Kanten, `
            + `${Object.keys(report.layoutEdgeTypes).length} Kantenarten`);

        // ------------------------------------------------------- 6. Browser
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
            report.cutWithoutHint += [...top.clipped, ...bottom.clipped]
                .filter((entry) => entry.kind === 'cut-without-hint').length;
        };

        await openApp(page, origin);

        // --------------------------------------------- 6a. Die Legende liest
        await openLegend(page);
        const rows = await legendRows(page);
        const kinds = rows.filter((row) => row.tag === 'BUTTON');
        extras.legendRows = rows;
        const layoutTypes = Object.keys(report.layoutEdgeTypes);
        report.legendShowsCounts =
            kinds.length > 0
            && kinds.every((row) => Number.isInteger(row.count) && row.count > 0
                && row.text.includes(String(row.count)));
        report.legendMatchesLayout =
            kinds.length === layoutTypes.length
            && kinds.every((row) => report.layoutEdgeTypes[row.type] === row.count);
        report.legendListsAbsentTypes =
            kinds.some((row) => report.layoutEdgeTypes[row.type] === undefined);
        report.legendSortedByCount = kinds.every((row, position) => {
            if (position === 0) {
                return true;
            }
            const before = kinds[position - 1];
            return before.count > row.count || (before.count === row.count && before.type < row.type);
        });
        log(`Legende: ${kinds.length} Arten, Zahlen ${report.legendShowsCounts}, `
            + `deckungsgleich mit /api/layout ${report.legendMatchesLayout}, `
            + `sortiert ${report.legendSortedByCount}`);

        // --------------------------------- 6b. Das Bild, Art fuer Art gemessen
        const seamBefore = await galaxySeam(page);
        extras.galaxyAtCapture = {
            nodes: seamBefore?.nodes ?? 0,
            drawnEdges: seamBefore?.drawnEdges ?? 0,
            highlightedCount: seamBefore?.highlightedCount ?? 0,
            mode: seamBefore?.mode ?? '',
        };
        /*
         * Ein Stueck herauszoomen, wie ein Leser, der das ganze Bild sehen
         * will. Die Kamera steht danach still; alles Weitere passiert im
         * Filter und nicht in der Kamera.
         */
        await zoomOut(page, 3);
        await page.waitForTimeout(1500);
        const clip = await sceneRect(page);
        if (clip === null || clip.width < 100 || clip.height < 100) {
            throw new Error(`die Zeichenflaeche ist zu klein zum Messen: ${JSON.stringify(clip)}`);
        }
        extras.sceneRect = clip;

        const baseInfo = await grab(page, 'base', clip);
        extras.captureSize = baseInfo;
        extras.shots.push(await shootAtRest(page, SHOT_GALAXY, 'galaxy-edges.png'));
        log(`Grundlage aufgenommen: ${baseInfo.width}x${baseInfo.height} Pixel, galaxy-edges.png geschrieben`);

        const filterType = FILTER_CANDIDATES.find((candidate) =>
            kinds.some((row) => row.type === candidate)) ?? kinds[0]?.type ?? '';
        if (filterType === '') {
            throw new Error('die Legende bietet keine Kantenart an, es gibt nichts zu filtern');
        }
        extras.filterType = filterType;

        for (const kind of kinds) {
            await nudge(page);
            await toggleKind(page, kind.type);
            await page.waitForTimeout(220);
            await grab(page, 'variant', clip);
            const measured = await compareShots(page, {
                base: 'base',
                variant: 'variant',
                hex: kind.color,
                epsilon: DIFF_EPSILON,
                floor: LIGHT_FLOOR,
                matchThreshold: COLOR_MATCH_THRESHOLD,
                background: SCENE_BACKGROUND,
            });
            const entry = {
                type: kind.type,
                count: kind.count,
                tableColor: kind.color,
                renderedColor: measured?.renderedColor ?? '',
                pixels: measured?.gainedPixels ?? 0,
                addedPixels: measured?.lostPixels ?? 0,
                matchBefore: measured?.matchBefore ?? 0,
                matchAfter: measured?.matchAfter ?? 0,
                litPixels: measured?.litPixels ?? 0,
                meanDiff: measured?.meanDiff ?? null,
            };
            entry.distanceToTable = entry.renderedColor === ''
                ? null
                : Number(colorDistance(entry.renderedColor, kind.color).toFixed(2));
            extras.measurements.push(entry);
            log(`  ${kind.type} (${kind.count}): ${entry.pixels} Pixel, gemessen `
                + `${entry.renderedColor} gegen Tabelle ${kind.color} `
                + `(Abstand ${entry.distanceToTable}), Pixel dieser Farbe `
                + `${entry.matchBefore} -> ${entry.matchAfter}`);

            if (kind.type === filterType) {
                // Der Halt, an dem das Ausblenden selbst gemessen wird.
                const hiddenRows = await legendRows(page);
                const row = hiddenRows.find((candidate) => candidate.type === filterType);
                report.filterKeepsRowVisible = row !== undefined && row.visible === true;
                report.filterRowDimmed =
                    row !== undefined && row.hidden === true && row.opacity > 0 && row.opacity <= 0.6;
                const head = await panelHead(page);
                report.filterHeaderText = head.edges.text;
                extras.filterHeadline = head;
                report.filterHidesType =
                    (measured?.matchBefore ?? 0) > 0
                    && (measured?.matchAfter ?? -1) === 0
                    && (measured?.gainedPixels ?? 0) > 0
                    && (measured?.lostPixels ?? -1) === 0;
                extras.filterRow = row;
                extras.shots.push(await shootAtRest(page, SHOT_FILTER, 'legend-filter.png'));
                await readability(`galaxy mit ausgeblendeter Art ${filterType}`);
                log(`Filter ${filterType}: Zeile sichtbar ${report.filterKeepsRowVisible}, `
                    + `gedimmt ${report.filterRowDimmed} (opacity ${row?.opacity}), `
                    + `Kopf "${report.filterHeaderText}"`);
            }

            await toggleKind(page, kind.type);
        }

        /*
         * Die Kamera stand still: die Grundlage noch einmal, gegen die erste
         * gehalten. Ohne diese Zahl waere jede Messung oben eine Behauptung
         * ueber eine Szene, von der niemand weiss, ob sie sich zwischendurch
         * gedreht hat.
         */
        await nudge(page);
        await page.waitForTimeout(300);
        await grab(page, 'again', clip);
        const drift = await compareShots(page, {
            base: 'base',
            variant: 'again',
            hex: '',
            epsilon: DIFF_EPSILON,
            floor: LIGHT_FLOOR,
            matchThreshold: COLOR_MATCH_THRESHOLD,
            background: SCENE_BACKGROUND,
        });
        report.sceneDriftMeanAbs = drift?.meanAbsDiff ?? -1;
        report.sceneDriftPixels = (drift?.gainedPixels ?? 0) + (drift?.lostPixels ?? 0);
        log(`Kameradrift ueber alle Aufnahmen: ${report.sceneDriftMeanAbs} mittlere Stufe, `
            + `${report.sceneDriftPixels} veraenderte Pixel`);

        // ------------------------------- 6c. Wie viele Farben stehen im Bild
        const seen = [];
        const measured = [...extras.measurements].sort((a, b) => b.pixels - a.pixels);
        for (const entry of measured) {
            if (entry.pixels <= 0 || entry.renderedColor === '') {
                entry.distinct = false;
                continue;
            }
            const clash = seen.find((other) =>
                colorDistance(other.renderedColor, entry.renderedColor) < COLOR_DISTANCE_THRESHOLD);
            entry.distinct = clash === undefined;
            entry.sameColorAs = clash === undefined ? null : clash.type;
            if (entry.distinct) {
                seen.push(entry);
            }
        }
        report.renderedColors = seen.map((entry) => ({
            type: entry.type,
            color: entry.renderedColor,
            tableColor: entry.tableColor,
            pixels: entry.pixels,
            distanceToTable: entry.distanceToTable,
        }));
        report.distinctColorsRendered = seen.length;
        extras.colorDistances = [];
        for (let i = 0; i < seen.length; i += 1) {
            for (let j = i + 1; j < seen.length; j += 1) {
                extras.colorDistances.push({
                    a: seen[i].type,
                    b: seen[j].type,
                    distance: Number(colorDistance(seen[i].renderedColor, seen[j].renderedColor).toFixed(2)),
                });
            }
        }
        log(`unterscheidbare Farben im Bild: ${report.distinctColorsRendered} `
            + `(${seen.map((entry) => `${entry.type} ${entry.renderedColor}`).join(', ')})`);

        await readability('galaxy mit offener Legende');

        // ------------------------- 6d. Der Filter ueberlebt den Ansichtswechsel
        await toggleKind(page, filterType);
        const hiddenBeforeSwitch = (await galaxySeam(page))?.hiddenKinds ?? [];

        await openWhyAndChoose(page, 'entry');
        await chooseEntryHit(page, WALK_TARGET);
        const inHierarchy = await galaxySeam(page);
        report.filterSurvivesViewSwitch =
            inHierarchy?.mode === 'hierarchy'
            && (inHierarchy?.hiddenKinds ?? []).includes(filterType)
            && hiddenBeforeSwitch.includes(filterType);
        extras.hiddenAcrossSwitch = {
            beforeSwitch: hiddenBeforeSwitch,
            afterSwitch: inHierarchy?.hiddenKinds ?? [],
        };
        log(`Filter ueberlebt den Wechsel: ${report.filterSurvivesViewSwitch} `
            + `(${JSON.stringify(inHierarchy?.hiddenKinds)})`);

        // --------------------------------------- 6e. Die Hierarchie, gezaehlt
        const hierarchy = inHierarchy?.hierarchy;
        if (hierarchy === undefined) {
            throw new Error('die Hierarchie steht nicht, obwohl ein Walk laeuft');
        }
        report.hierarchyWalkEdges = hierarchy.walkEdges;
        report.hierarchyExtraEdges = hierarchy.extraEdges;
        extras.hierarchy = {
            root: hierarchy.rootName,
            symbols: hierarchy.nodes,
            depth: hierarchy.depth,
            walkEdges: hierarchy.walkEdges,
            extraEdges: hierarchy.extraEdges,
            extras: hierarchy.extras,
            headline: inHierarchy?.headline ?? '',
            edgeKinds: inHierarchy?.edgeKinds ?? [],
        };
        const head = await panelHead(page);
        extras.hierarchyHead = head;
        report.hierarchyHeaderExplainsCounts =
            head.edges.present === true
            && head.edges.clipped === false
            && head.edges.text.includes(`${hierarchy.walkEdges} call`)
            && head.edges.text.includes('from the walk')
            && head.edges.text.includes(`${hierarchy.extraEdges} link`)
            && head.edges.text.includes('from the index');
        log(`Hierarchie: ${hierarchy.nodes} Symbole, ${hierarchy.walkEdges} Walk-Kanten, `
            + `${hierarchy.extraEdges} dazu (${hierarchy.extras.map((edge) => edge.type).join(', ')}); `
            + `Kopf "${head.headline.text}" / "${head.edges.text}"`);

        await openLegend(page);
        extras.shots.push(await shootAtRest(page, SHOT_HIERARCHY, 'hierarchy-edges.png'));
        await readability('hierarchie mit den zusaetzlichen Kanten');

        // --------------------- 6f. Filterbar, und die Spalten bleiben stehen
        const beforeHide = await galaxySeam(page);
        const extraKinds = (beforeHide?.edgeKinds ?? [])
            .filter((kind) => kind.type !== 'CALLS' && kind.hidden === false);
        const placementsWithExtras = beforeHide?.hierarchy?.placements ?? [];
        const drawnBefore = beforeHide?.drawnEdges ?? 0;
        for (const kind of extraKinds) {
            await toggleKind(page, kind.type);
        }
        const afterHide = await galaxySeam(page);
        const hiddenSum = extraKinds.reduce((sum, kind) => sum + kind.count, 0);
        report.hierarchyExtraEdgesFilterable =
            extraKinds.length > 0
            && (afterHide?.drawnEdges ?? -1) === drawnBefore - hiddenSum
            && (afterHide?.drawnEdges ?? -1) === hierarchy.walkEdges;
        const rowsInHierarchy = await legendRows(page);
        report.hierarchyExtraEdgesFilterable =
            report.hierarchyExtraEdgesFilterable
            && extraKinds.every((kind) => {
                const row = rowsInHierarchy.find((candidate) => candidate.type === kind.type);
                return row !== undefined && row.visible === true && row.hidden === true;
            });
        const placementsWithoutExtras = afterHide?.hierarchy?.placements ?? [];
        report.hierarchyDeterminismUnchanged =
            placementsWithExtras.length > 0
            && JSON.stringify(placementsWithExtras) === JSON.stringify(placementsWithoutExtras);
        extras.hierarchyFilter = {
            hiddenKinds: extraKinds.map((kind) => ({ type: kind.type, count: kind.count })),
            drawnBefore,
            drawnAfter: afterHide?.drawnEdges ?? -1,
            walkEdges: hierarchy.walkEdges,
            headAfter: await panelHead(page),
        };
        log(`zusaetzliche Kanten filterbar: ${report.hierarchyExtraEdgesFilterable} `
            + `(${extras.hierarchyFilter.drawnBefore} -> ${extras.hierarchyFilter.drawnAfter} Linien), `
            + `Spalten unveraendert: ${report.hierarchyDeterminismUnchanged}`);

        /*
         * Die Spalten kommen aus dem Aufruf-Walk.
         *
         * Drei Bedingungen, und die dritte ist die eigentliche: jede Spalte ist
         * ihr Hop mal einer festen Breite, in einer Spalte stehen die Namen
         * ordinal sortiert, und jedes Symbol jenseits der Wurzel wird von einer
         * WALK-Kante aus der Spalte davor erreicht. Eine dazugelegte Beziehung
         * kann demnach nichts angeordnet haben.
         */
        const placements = placementsWithExtras;
        const walkEdges = beforeHide?.hierarchy?.edges ?? [];
        const hopOf = new Map(placements.map((placement) => [placement.key, placement.hop]));
        const columnWidth = placements.find((placement) => placement.hop === 1)?.x ?? 0;
        const columnsOk = columnWidth > 0
            && placements.every((placement) => placement.x === placement.hop * columnWidth);
        const sortedOk = [...new Set(placements.map((placement) => placement.hop))].every((hop) => {
            const level = placements.filter((placement) => placement.hop === hop);
            const names = level.map((placement) => `${placement.name} ${placement.key}`);
            return JSON.stringify(names) === JSON.stringify([...names].sort());
        });
        const reachedByWalk = placements.every((placement) => {
            if (placement.hop === 0) {
                return true;
            }
            return walkEdges.some((edge) =>
                edge.to === placement.key && (hopOf.get(edge.from) ?? -1) === placement.hop - 1);
        });
        report.hierarchyColumnsFromCallsOnly = columnsOk && sortedOk && reachedByWalk;
        extras.columns = {
            columnWidth,
            columnsOk,
            sortedOk,
            reachedByWalk,
            placements,
        };
        log(`Spalten aus dem Aufruf-Walk: ${report.hierarchyColumnsFromCallsOnly} `
            + `(Breite ${columnWidth}, Ordnung ${sortedOk}, erreicht ${reachedByWalk})`);

        report.screenshotsAtRest =
            extras.shots.length === 3 && extras.shots.every((shot) => shot.atRest === true);
        extras.apiRoutes = { ...proxy.log.apiRoutes };
        extras.rpcTools = { ...proxy.log.rpcTools };
        await context.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w9] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w9] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
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
            walkTarget: WALK_TARGET,
            method:
                'Gemessen wird am gerenderten Bild: eine Aufnahme der Zeichenflaeche, dann je '
                + 'Kantenart dieselbe Aufnahme mit dieser Art abgeschaltet. Die Differenz ist das '
                + 'Licht, das genau diese Art beigetragen hat; ihre Richtung im Farbraum (auf den '
                + 'gleichen Maximalkanal gebracht) ist die Farbe, in der sie gezeichnet wurde. Zwei '
                + `Farben gelten ab einem Abstand von ${COLOR_DISTANCE_THRESHOLD} als verschieden, `
                + `ein Pixel zeigt eine Art ab einem Abstand unter ${COLOR_MATCH_THRESHOLD}.`,
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    const shotsOk = existsSync(SHOT_GALAXY) && existsSync(SHOT_HIERARCHY) && existsSync(SHOT_FILTER);
    const ok =
        failure === null
        && Object.keys(report.layoutEdgeTypes).length >= 5
        && report.distinctColorsRendered >= 5
        && report.renderedColors.length >= 5
        && report.legendShowsCounts === true
        && report.legendMatchesLayout === true
        && report.legendListsAbsentTypes === false
        && report.legendSortedByCount === true
        && report.filterHidesType === true
        && report.filterKeepsRowVisible === true
        && report.filterRowDimmed === true
        && /\d+\s+of\s+\d+/i.test(report.filterHeaderText)
        && report.filterSurvivesViewSwitch === true
        && report.hierarchyWalkEdges >= 1
        && report.hierarchyExtraEdges >= 1
        && report.hierarchyHeaderExplainsCounts === true
        && report.hierarchyExtraEdgesFilterable === true
        && report.hierarchyDeterminismUnchanged === true
        && report.hierarchyColumnsFromCallsOnly === true
        && report.sceneDriftPixels === 0
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
        console.error('[smoke-w9] W9-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w9] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W9-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w9] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
