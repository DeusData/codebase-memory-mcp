#!/usr/bin/env node
/*
 * Browserpruefung der Handtest-Korrekturen, Strang Architecture und Logs
 * (K18 bis K23, K25, K26 aus verification/call-feedback-2026-10-02/KORREKTURPLAN.md).
 *
 *   node tools/handtest-fixes-architecture.mjs --origin http://127.0.0.1:4373 \
 *        --out /tmp/handtest/architecture [--tag after] [--only K18,K19] \
 *        [--project django-demo] [--control cbm] [--profile /tmp/profile]
 *
 * Jede Pruefung faehrt genau den Ablauf aus dem Korrekturplan, misst den
 * Befund in der laufenden Seite und schreibt PASS oder FAIL mit dem Messwert.
 * Die Bilder liegen je Punkt unter <out>/<K>/<tag>-NN-name.png, bei einer
 * Bewegung auch nach 0 ms, 250 ms, 1 s und 3 s. Daneben schreibt das Skript je
 * Punkt ein index-<tag>.md mit jedem Bild und dem, was es zeigt, und
 * report-<tag>.json.
 *
 * Nachpruefung der Durchsicht: K19-lines (Behavior und Service map ohne
 * blaue Linien), K20-hover (Ordnernamen bleiben beim Ueberfahren der Schilder
 * stehen), K25 von einem frischen Profil aus und K26-identity (gleicher Text
 * von zwei Stellen wird zweimal voll gesendet, ein Zaehler traegt seine
 * Stelle).
 *
 * Beschriftungen und Kamera haengen von der Groesse der Zeichenflaeche ab. K20,
 * K21 und K25 laufen darum zweimal: mit 1600x1000 und in der Ansicht des
 * Handtests (1494x728, Chat offen), in der die leeren Behavior-Kaesten
 * auffielen.
 *
 * Es startet keinen Server und schreibt nichts in das Protokoll des Servers:
 * POST /api/ui-log wird im Browser beantwortet und nur mitgeschnitten, und
 * navigator.sendBeacon (die letzte Sendung beim Verlassen einer Seite, die am
 * Routing vorbeigeht) bleibt in der Seite. So landen die Pruefsitzungen nicht
 * im System-Log des Nutzers.
 */

import { chromium } from 'playwright';
import { mkdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { inflateSync } from 'node:zlib';

const argv = process.argv.slice(2);
const arg = (name, fallback) => {
    const index = argv.indexOf(`--${name}`);
    if (index < 0) return fallback;
    const value = argv[index + 1];
    return value === undefined || value.startsWith('--') ? true : value;
};
const ORIGIN = String(arg('origin', 'http://127.0.0.1:4373')).replace(/\/$/, '');
const OUT = resolve(String(arg('out', 'verification/handtest-fixes/architecture')));
const TAG = String(arg('tag', 'after'));
const PROFILE = resolve(String(arg('profile', join(OUT, '..', 'profile-architecture'))));
const PROJECT = String(arg('project', 'django-demo'));
const CONTROL = String(arg('control', 'cbm'));
const ONLY = String(arg('only', 'K18,K19,K20,K21,K22,K23,K25,K26')).split(',');
const VIEWPORT = { width: 1600, height: 1000 };
const SCALE = 2;
// Ohne Fenster, wie die Galaxy-Pruefung; `--headed` zeigt es.
const HEADED = arg('headed', false) === true;
const THREE_CLOCK = 'THREE.THREE.Clock: This module has been deprecated. Please use THREE.Timer instead.';

const wait = (ms) => new Promise((done) => setTimeout(done, ms));

/* ------------------------------------------------------------------ */
/* Ergebnis und Bilder                                                  */

const checks = [];
const images = new Map();
const counters = new Map();

/** Die Ansicht des Handtests: schmalere, niedrigere Zeichenflaeche mit offenem Chat. */
let layout = 'standard';
const LAYOUTS = { standard: { width: 1600, height: 1000, chat: false }, handtest: { width: 1494, height: 728, chat: true } };

function check(id, title, pass, measured) {
    const name = layout === 'standard' ? id : `${id}@${layout}`;
    checks.push({ id: name, title, pass: Boolean(pass), measured });
    console.log(`${pass ? 'PASS' : 'FAIL'}  ${name}  ${title}  ${JSON.stringify(measured)}`);
}

async function shot(page, id, label, note = '') {
    const next = (counters.get(id) ?? 0) + 1;
    counters.set(id, next);
    const file = `${TAG}-${String(next).padStart(2, '0')}-${layout === 'standard' ? '' : `${layout}-`}${label}.png`;
    await mkdir(join(OUT, id), { recursive: true });
    const png = await page.screenshot({ path: join(OUT, id, file) });
    images.set(id, [...images.get(id) ?? [], { file, note }]);
    return png;
}

/** Bilder direkt nach einer Handlung und nach 250 ms, 1 s und 3 s. */
async function series(page, id, label, action, note) {
    await action();
    await shot(page, id, `${label}-0ms`, `${note}: direkt nach der Handlung`);
    await wait(250); await shot(page, id, `${label}-250ms`, `${note}: nach 250 ms`);
    await wait(750); await shot(page, id, `${label}-1s`, `${note}: nach 1 s`);
    await wait(2000); return shot(page, id, `${label}-3s`, `${note}: nach 3 s`);
}

/* ------------------------------------------------------------------ */
/* Pixel aus einem Bildschirmfoto, ohne weitere Abhaengigkeit            */

function decodePng(buffer) {
    let offset = 8; let width = 0; let height = 0; let channels = 4; const idat = [];
    while (offset < buffer.length) {
        const length = buffer.readUInt32BE(offset); const type = buffer.toString('ascii', offset + 4, offset + 8);
        const data = buffer.subarray(offset + 8, offset + 8 + length);
        if (type === 'IHDR') { width = data.readUInt32BE(0); height = data.readUInt32BE(4); channels = data[9] === 6 ? 4 : 3; }
        if (type === 'IDAT') idat.push(data);
        offset += length + 12;
    }
    const raw = inflateSync(Buffer.concat(idat)); const stride = width * channels; const pixels = Buffer.alloc(stride * height);
    for (let y = 0; y < height; y++) {
        const filter = raw[y * (stride + 1)]; const line = raw.subarray(y * (stride + 1) + 1, (y + 1) * (stride + 1));
        for (let x = 0; x < stride; x++) {
            const left = x >= channels ? pixels[y * stride + x - channels] : 0; const up = y ? pixels[(y - 1) * stride + x] : 0;
            const corner = x >= channels && y ? pixels[(y - 1) * stride + x - channels] : 0;
            const paeth = () => { const p = left + up - corner; const a = Math.abs(p - left); const b = Math.abs(p - up); const c = Math.abs(p - corner); return a <= b && a <= c ? left : b <= c ? up : corner; };
            const value = filter === 1 ? left : filter === 2 ? up : filter === 3 ? (left + up) >> 1 : filter === 4 ? paeth() : 0;
            pixels[y * stride + x] = (line[x] + value) & 255;
        }
    }
    return { width, height, channels, pixels };
}

/** Die haeufigste Farbe in einem 7x7-Feld um einen Punkt in CSS-Pixeln: Rasterlinien fallen heraus. */
function colorAt(image, x, y) {
    const counts = new Map();
    for (let dy = -3; dy <= 3; dy++) for (let dx = -3; dx <= 3; dx++) {
        const px = Math.min(image.width - 1, Math.max(0, Math.round(x * SCALE) + dx)); const py = Math.min(image.height - 1, Math.max(0, Math.round(y * SCALE) + dy));
        const at = (py * image.width + px) * image.channels; const key = `${image.pixels[at]},${image.pixels[at + 1]},${image.pixels[at + 2]}`;
        counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    return [...counts].sort((a, b) => b[1] - a[1])[0][0].split(',').map(Number);
}
const hex = ([r, g, b]) => `#${[r, g, b].map((value) => value.toString(16).padStart(2, '0')).join('')}`;
const close = (a, b, tolerance = 6) => a.every((value, index) => Math.abs(value - b[index]) <= tolerance);
/**
 * Bildpunkte in einem Rechteck (CSS-Pixel), deren staerkster Kanal deutlich
 * Blau ist, wie die geteilte CALLS-Farbe #579fc7 oder das Blaugrau #85b4c5
 * der aus Quelltext gebauten Dienste, und solche, die deutlich
 * gruen sind wie die Aufruffarbe der Palette.
 */
function hueCounts(image, rect) {
    let blue = 0; let green = 0;
    for (let y = Math.max(0, Math.round(rect.top * SCALE)); y < Math.min(image.height, Math.round(rect.bottom * SCALE)); y++) {
        for (let x = Math.max(0, Math.round(rect.left * SCALE)); x < Math.min(image.width, Math.round(rect.right * SCALE)); x++) {
            const at = (y * image.width + x) * image.channels; const r = image.pixels[at]; const g = image.pixels[at + 1]; const b = image.pixels[at + 2];
            if (b - g >= 12 && b - r >= 40) blue += 1;
            if (g - b >= 25 && g - r >= 60) green += 1;
        }
    }
    return { blue, green };
}

/* ------------------------------------------------------------------ */
/* Seite                                                                */

let uiLogPosts = [];

async function open(page, project, workspace = 'architecture') {
    const size = LAYOUTS[layout];
    await page.setViewportSize({ width: size.width, height: size.height });
    await page.goto(`${ORIGIN}/?project=${encodeURIComponent(project)}&workspace=${workspace}`, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.atlas-shell', { timeout: 30000 });
    await wait(1500);
    const toggle = page.locator('.atlas-agent-chat-toggle').first();
    if (await toggle.count() && (await toggle.getAttribute('aria-expanded') === 'true') !== size.chat) { await toggle.click(); await wait(800); }
}
const tab = (page, view) => page.locator(`button.atlas-arch-tab[data-view="${view}"]`).first();
async function sceneSettled(page, selector, timeout = 60000) {
    await page.waitForFunction((sel) => document.querySelectorAll(sel).length > 0, selector, { timeout }).catch(() => {});
    await wait(2500);
}

/**
 * Sichtbare Beschriftungen in der Zeichenflaeche und ihre Ueberdeckungen. Ein
 * Etikett darf den Innenabstand eines Nachbarn beruehren, nie seine Schrift:
 * gemessen wird darum die Schriftflaeche des einen gegen den Kasten des anderen.
 */
async function labelGeometry(page) {
    return page.evaluate(() => {
        const canvas = document.querySelector('.atlas-architecture canvas')?.getBoundingClientRect();
        const kinds = { 'architecture-node-label': 'chip', 'architecture-folder-label': 'folder', 'system-scene-node-label': 'chip', 'system-scene-lane-label': 'lane' };
        const items = [];
        for (const element of document.querySelectorAll('.architecture-node-label, .architecture-folder-label, .system-scene-node-label, .system-scene-lane-label')) {
            const style = getComputedStyle(element); const box = element.getBoundingClientRect();
            if (element.offsetParent === null || style.visibility === 'hidden' || style.display === 'none' || box.width < 1 || box.height < 1) continue;
            if (canvas && (box.right < canvas.left || box.left > canvas.right || box.bottom < canvas.top || box.top > canvas.bottom)) continue;
            const side = (name) => (parseFloat(style.getPropertyValue(`padding-${name}`)) || 0) + (parseFloat(style.getPropertyValue(`border-${name}-width`)) || 0);
            const text = { left: box.left + side('left'), right: box.right - side('right'), top: box.top + side('top'), bottom: box.bottom - side('bottom') };
            const kind = Object.entries(kinds).find(([name]) => element.classList.contains(name))?.[1] ?? 'label';
            const strong = element.querySelector('strong') ?? element;
            items.push({ kind, text: element.textContent?.trim() ?? '', title: element.getAttribute('title') ?? '', box: { left: box.left, right: box.right, top: box.top, bottom: box.bottom }, textBox: text,
                truncated: strong.scrollWidth > strong.clientWidth + 1, inside: Boolean(canvas) && box.left >= canvas.left - 1 && box.right <= canvas.right + 1 && box.top >= canvas.top - 1 && box.bottom <= canvas.bottom + 1 });
        }
        const hit = (a, b) => Math.min(a.right, b.right) - Math.max(a.left, b.left) > 0.5 && Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top) > 0.5;
        const overlaps = [];
        for (let i = 0; i < items.length; i++) for (let j = i + 1; j < items.length; j++) {
            if (hit(items[i].textBox, items[j].box) || hit(items[j].textBox, items[i].box)) overlaps.push(`${items[i].kind}:${items[i].text} / ${items[j].kind}:${items[j].text}`);
        }
        return { canvas: canvas ? { left: canvas.left, right: canvas.right, top: canvas.top, bottom: canvas.bottom } : null, labels: items, overlaps };
    });
}
const summary = (geometry) => ({ labels: geometry.labels.length, chips: geometry.labels.filter((item) => item.kind === 'chip').length,
    folders: geometry.labels.filter((item) => item.kind === 'folder' || item.kind === 'lane').length, overlaps: geometry.overlaps.length, overlapping: geometry.overlaps.slice(0, 6) });

async function selectEntry(page, pattern) {
    const select = page.getByLabel('Behavior entry point').first();
    const options = await select.locator('option').allTextContents();
    const index = options.findIndex((option) => pattern.test(option));
    if (index < 0) return null;
    await select.selectOption({ index });
    return options[index];
}
async function behaviorState(page) {
    return page.evaluate(() => {
        const select = document.querySelector('select[aria-label="Behavior entry point"]');
        const scene = document.querySelector('.system-scene');
        return { heading: document.querySelector('.behavior-heading h2')?.textContent?.trim() ?? '',
            value: select?.value ?? null, selected: select?.selectedOptions[0]?.textContent?.trim() ?? '',
            navigation: document.querySelector('.behavior-navigation')?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            pages: document.querySelector('nav[aria-label="Direct call pages"]')?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            sceneNodes: scene ? Number(scene.getAttribute('data-node-count') ?? NaN) : null };
    });
}

/* ------------------------------------------------------------------ */
/* K18: das Auswahlfeld "Start" zeigt die automatisch gewaehlte Operation */

async function k18(page) {
    await open(page, PROJECT);
    await shot(page, 'K18', 'architecture-geoeffnet', 'Architecture fuer django-demo, vor dem Wechsel nach Behavior');
    await series(page, 'K18', 'behavior', () => tab(page, 'behavior').click(), 'Behavior angeklickt');
    await page.waitForFunction(() => /What can .+ call\?/i.test(document.querySelector('.behavior-heading h2')?.textContent ?? ''), null, { timeout: 30000 }).catch(() => {});
    await wait(1500);
    const state = await behaviorState(page);
    await shot(page, 'K18', 'behavior-fertig', 'Ueberschrift und Auswahlfeld nebeneinander');
    const name = state.heading.match(/What can (.+) call\?/i)?.[1] ?? '';
    check('K18', 'Behavior: das Start-Feld zeigt die Operation, die die Ueberschrift nennt',
        Boolean(name) && state.value !== '' && state.selected.toLowerCase().startsWith(`${name.toLowerCase()} ·`), state);
}

/* ------------------------------------------------------------------ */
/* K19: ein gruenes Farbschema in allen Szenen                           */

async function sceneColors(page, png, id, label) {
    const image = decodePng(png);
    const measured = await page.evaluate(() => {
        const canvas = document.querySelector('.atlas-architecture canvas')?.getBoundingClientRect();
        const style = (selector, property) => { const element = document.querySelector(selector); return element ? getComputedStyle(element).getPropertyValue(property) : null; };
        return { canvas: canvas ? { left: canvas.left, top: canvas.top, right: canvas.right, bottom: canvas.bottom } : null,
            label: style('.architecture-node-label:not(.is-selected), .system-scene-node-label', 'background-color'),
            labelBorder: style('.architecture-node-label:not(.is-selected), .system-scene-node-label', 'border-top-color'),
            inspector: style('.spatial-inspector, .behavior-inspector, .system-inspector', 'background-color') };
    });
    const background = measured.canvas ? [colorAt(image, measured.canvas.left + 10, measured.canvas.top + 10), colorAt(image, measured.canvas.right - 10, measured.canvas.top + 10)] : [];
    return { scene: label, background: background.map(hex), green: background.every(([r, g, b]) => g >= b && g >= r), label: measured.label, labelBorder: measured.labelBorder, inspector: measured.inspector, raw: background };
}

async function k19(page) {
    await open(page, PROJECT);
    const scenes = [];
    const pngs = [];
    await tab(page, 'overview').click();
    await sceneSettled(page, '.architecture-node-label');
    let png = await shot(page, 'K19', 'overview', 'Overview (Referenz)'); pngs.push(png);
    scenes.push(await sceneColors(page, png, 'K19', 'overview'));
    await tab(page, 'routes').click(); await wait(6000);
    png = await shot(page, 'K19', 'routes-service-map', 'Routes, Service map'); pngs.push(png);
    scenes.push({ ...(await sceneColors(page, png, 'K19', 'service-map')), note: 'ohne Deployment-Dateien keine Szene' });
    const endpoints = page.getByRole('button', { name: 'Endpoints', exact: true });
    if (await endpoints.count()) await endpoints.click();
    await sceneSettled(page, '.architecture-node-label');
    png = await shot(page, 'K19', 'routes-endpoints', 'Routes, Endpoints'); pngs.push(png);
    scenes.push(await sceneColors(page, png, 'K19', 'endpoints'));
    await tab(page, 'hotspots').click();
    await sceneSettled(page, '.architecture-node-label');
    png = await shot(page, 'K19', 'hotspots', 'Hotspots'); pngs.push(png);
    scenes.push(await sceneColors(page, png, 'K19', 'hotspots'));
    // "Hotspots sind flach": so weit nach unten gedreht wie moeglich bleibt die Karte geneigt, nie eine Linie.
    const spread = async () => page.evaluate(() => {
        const ys = [...document.querySelectorAll('.architecture-node-label:not(.is-hidden)')].map((label) => { const box = label.getBoundingClientRect(); return box.top + box.height / 2; });
        return ys.length > 1 ? Math.max(...ys) - Math.min(...ys) : 0;
    });
    const tiltCanvas = await page.locator('.atlas-architecture canvas').first().boundingBox();
    const spreadBefore = await spread();
    if (tiltCanvas) {
        await page.mouse.move(tiltCanvas.x + tiltCanvas.width / 2, tiltCanvas.y + tiltCanvas.height / 2);
        await page.mouse.down();
        await page.mouse.move(tiltCanvas.x + tiltCanvas.width / 2, tiltCanvas.y + tiltCanvas.height / 2 - 600, { steps: 20 });
        await page.mouse.up();
        await wait(1200);
    }
    const spreadAfter = await spread();
    await shot(page, 'K19', 'hotspots-ganz-gekippt', 'Hotspots nach dem Drehen bis an die untere Grenze');
    check('K19-tilt', 'Hotspots lassen sich nicht flach auf eine Linie drehen', spreadBefore > 0 && spreadAfter / spreadBefore >= 0.3,
        { labelSpreadBefore: Math.round(spreadBefore), labelSpreadAfter: Math.round(spreadAfter), ratio: spreadBefore ? Number((spreadAfter / spreadBefore).toFixed(2)) : null });
    await page.getByRole('button', { name: 'Fit map' }).first().click(); await wait(800);
    await tab(page, 'structure').click();
    await sceneSettled(page, '.system-scene-node-label', 40000);
    png = await shot(page, 'K19', 'system-structure', 'System structure'); pngs.push(png);
    scenes.push(await sceneColors(page, png, 'K19', 'structure'));
    await tab(page, 'behavior').click();
    await sceneSettled(page, '.system-scene-node-label', 40000);
    png = await shot(page, 'K19', 'behavior', 'Behavior'); pngs.push(png);
    scenes.push(await sceneColors(page, png, 'K19', 'behavior'));
    // Alle Ansichten nebeneinander, so wie der Vergleich im Korrekturplan es verlangt.
    const sheet = await page.context().newPage();
    await sheet.setViewportSize({ width: 2400, height: 1060 });
    await sheet.setContent(`<body style="margin:0;background:#000;display:grid;grid-template-columns:repeat(3,800px);gap:4px">${pngs.map((buffer) => `<img style="width:800px" src="data:image/png;base64,${buffer.toString('base64')}">`).join('')}</body>`);
    await wait(500);
    await mkdir(join(OUT, 'K19'), { recursive: true });
    await sheet.screenshot({ path: join(OUT, 'K19', `${TAG}-00-vergleich.png`) });
    images.set('K19', [{ file: `${TAG}-00-vergleich.png`, note: 'Alle sechs Ansichten nebeneinander: Overview, Service map, Endpoints / Hotspots, System structure, Behavior' }, ...images.get('K19') ?? []]);
    await sheet.close();
    const drawn = scenes.filter((scene) => scene.background.length);
    const reference = drawn[0]?.raw[0];
    const sameBackground = drawn.every((scene) => scene.raw.every((color) => reference && close(color, reference)));
    const labels = new Set(drawn.map((scene) => scene.label));
    check('K19', 'Alle Architecture-Szenen im selben gruenen Schema wie Overview (Hintergrund, Schilder, Inspektor)',
        drawn.length >= 4 && sameBackground && drawn.every((scene) => scene.green) && labels.size === 1,
        { scenes: scenes.map(({ raw, ...rest }) => rest), sameBackground, labelBackgrounds: [...labels] });
}

/** "behaviour wieder blau statt gruen": keine blauen Linien in Behavior und in der Service map. */
async function k19Lines(page) {
    const canvasRect = () => page.evaluate(() => { const box = document.querySelector('.atlas-architecture canvas')?.getBoundingClientRect(); return box ? { left: box.left, top: box.top, right: box.right, bottom: box.bottom } : null; });
    await open(page, PROJECT);
    await tab(page, 'behavior').click();
    await sceneSettled(page, '.system-scene-node-label', 40000);
    await wait(1500);
    let png = await shot(page, 'K19', 'behavior-linien', 'Behavior in django-demo: Aufruflinien in der Farbe der Palette');
    let rect = await canvasRect();
    const behavior = rect ? hueCounts(decodePng(png), rect) : null;
    await open(page, CONTROL);
    await tab(page, 'routes').click();
    await page.waitForFunction(() => document.querySelectorAll('.container-map .architecture-node-label').length > 0, null, { timeout: 60000 }).catch(() => {});
    await wait(3000);
    png = await shot(page, 'K19', 'cbm-service-map', 'Service map in cbm mit Diensten: Dienste und Aufruflinien in der Palette');
    rect = await canvasRect();
    const services = rect ? hueCounts(decodePng(png), rect) : null;
    const key = await page.evaluate(() => { const element = document.querySelector('.container-call-key'); return element ? getComputedStyle(element).backgroundColor : null; });
    const keyGreen = /rgb\((\d+), (\d+), (\d+)\)/.exec(key ?? '');
    const keyIsGreen = Boolean(keyGreen) && Number(keyGreen[2]) > Number(keyGreen[1]) && Number(keyGreen[2]) > Number(keyGreen[3]);
    const chips = await page.locator('.container-map .architecture-node-label').count();
    check('K19-lines', 'Behavior und Service map zeichnen ihre Aufrufe gruen, keine blauen Linien',
        behavior !== null && services !== null && behavior.blue <= 40 && behavior.green > 0 && services.blue <= 40 && keyIsGreen && chips > 0,
        { behavior, serviceMap: services, serviceChips: chips, callKey: key });
}

/* ------------------------------------------------------------------ */
/* K20: Beschriftungen ohne Ueberdeckung, rechte Spalte im Bereich        */

async function k20(page) {
    await open(page, PROJECT);
    await tab(page, 'overview').click();
    await sceneSettled(page, '.architecture-node-label');
    await shot(page, 'K20', 'overview-wurzel', 'Overview an der Wurzel');
    const area = page.locator('button.architecture-node-label', { hasText: /^\W*django\s*$/ }).first();
    // django-demo liest seine Architektur bei vollem Server auch einmal laenger als eine halbe Minute.
    await area.waitFor({ timeout: 120000 });
    await area.click(); await wait(800);
    const openArea = page.getByRole('button', { name: /Open area/ }).first();
    await series(page, 'K20', 'django-geoeffnet', async () => { if (await openArea.count()) await openArea.click(); else await area.dblclick(); }, 'Bereich django geoeffnet');
    const inside = await labelGeometry(page);
    // Ueberdeckungen haengen vom Zoom ab: zwei Stufen hinaus, dann zwei hinein, jede gemessen.
    const zoomed = [];
    const canvasBox = await page.locator('.atlas-architecture canvas').first().boundingBox();
    for (const [index, delta] of [240, 240, -240, -240, -240].entries()) {
        if (!canvasBox) break;
        await page.mouse.move(canvasBox.x + canvasBox.width / 2, canvasBox.y + canvasBox.height / 2);
        await page.mouse.wheel(0, delta); await wait(700);
        const geometry = await labelGeometry(page);
        zoomed.push({ step: index + 1, delta, labels: geometry.labels.length, overlaps: geometry.overlaps.length, outsideCovered: geometry.overlaps.filter((pair) => pair.includes('folder:Outside')).length, overlapping: geometry.overlaps.slice(0, 3) });
        if (index === 1 || index === 4) await shot(page, 'K20', `django-zoom-${index + 1}`, `Overview in django nach Zoomstufe ${index + 1}`);
    }
    await page.getByRole('button', { name: 'Fit map' }).first().click(); await wait(1200);
    const outside = inside.labels.find((item) => item.kind === 'folder' && /^Outside$/.test(item.text));
    const outsideCovered = [...inside.overlaps.filter((pair) => pair.includes('folder:Outside')), ...zoomed.flatMap((step) => step.outsideCovered ? [`zoom ${step.step}`] : [])];
    const inspector = await page.evaluate(() => ({ eyebrow: document.querySelector('.spatial-inspector .spatial-eyebrow')?.textContent?.trim() ?? '',
        heading: document.querySelector('.spatial-inspector h3')?.textContent?.trim() ?? '',
        text: document.querySelector('.spatial-inspector')?.textContent?.replace(/\s+/g, ' ').trim().slice(0, 200) ?? '' }));
    check('K20a', 'Overview in django: Spurlabel "Outside" sichtbar und von keinem Schild verdeckt', Boolean(outside) && outsideCovered.length === 0,
        { outside: outside ? outside.box : null, covered: outsideCovered });
    check('K20b', 'Overview in django: rechte Spalte fasst den geoeffneten Bereich zusammen', /^django$/.test(inspector.heading) && /2[.,]?310 files/.test(inspector.text) && !/^Repository$/i.test(inspector.eyebrow), inspector);
    check('K20-overview', 'Overview in django: keine Beschriftung ueberdeckt eine andere, auch nicht beim Zoomen', inside.overlaps.length === 0 && zoomed.every((step) => step.overlaps === 0), { ...summary(inside), zoomed });

    await series(page, 'K20', 'system-structure', () => tab(page, 'structure').click(), 'System structure geoeffnet');
    await sceneSettled(page, '.system-scene-node-label', 40000);
    const structure = await labelGeometry(page);
    await shot(page, 'K20', 'system-structure-fertig', 'System structure mit Gruppen und Plattennamen');
    const chipNames = new Set(structure.labels.filter((item) => item.kind === 'chip').map((item) => item.text.toLowerCase()));
    const doubled = structure.labels.filter((item) => item.kind === 'lane' && (chipNames.has(item.text.toLowerCase()) || (/^repository root$/i.test(item.text) && chipNames.has('(root)'))));
    check('K20c', 'System structure: ein Name pro Gruppe, keine Ueberdeckung', doubled.length === 0 && structure.overlaps.length === 0,
        { ...summary(structure), doubled: doubled.map((item) => item.text) });

    await series(page, 'K20', 'hotspots', () => tab(page, 'hotspots').click(), 'Hotspots geoeffnet');
    await sceneSettled(page, '.architecture-node-label');
    const hotspots = await labelGeometry(page);
    await shot(page, 'K20', 'hotspots-fertig', 'Hotspots mit Bereichsnamen unter den Schildern');
    check('K20d', 'Hotspots: Bereichsnamen und Schilder ueberlagern sich nicht', hotspots.overlaps.length === 0 && hotspots.labels.some((item) => item.kind === 'folder'), summary(hotspots));
    if (layout === 'standard') await hoverSweep(page);
}

/** Die sichtbaren Schilder der Karte. */
const shownChips = (page) => page.evaluate(() => [...document.querySelectorAll('button.architecture-node-label[data-node-id]:not(.is-hidden)')].map((label) => label.dataset.nodeId).sort());

/** Wo jeder Ordnername steht: Ecke, Lage und ob er sichtbar ist. */
const folderPlacement = (page) => page.evaluate(() => [...document.querySelectorAll('.architecture-folder-label[data-folder-id]')].map((label) => {
    const box = label.getBoundingClientRect(); const hidden = getComputedStyle(label).visibility === 'hidden';
    return `${label.dataset.folderId}:${hidden ? 'hidden' : `${label.dataset.align}@${Math.round(box.left)},${Math.round(box.top)}`}`;
}).sort());

/**
 * Durchsicht: das Ueberfahren eines Schilds liess Ordnernamen springen oder
 * verschwinden. Jedes sichtbare Schild wird ueberfahren; gemessen wird, ob
 * sich ein Ordnername bewegt. Zwischen zwei Schildern geht der Zeiger aus der
 * Zeichenflaeche, damit kein Kasten darunter haengen bleibt.
 */
async function hoverSweep(page) {
    const sweeps = [];
    for (const [view, scope] of [['hotspots', 'Hotspots'], ['overview', 'Overview in django']]) {
        if (view === 'overview') {
            await tab(page, 'overview').click();
            await sceneSettled(page, '.architecture-node-label');
            const area = page.locator('button.architecture-node-label', { hasText: /^\W*django\s*$/ }).first();
            if (await area.count()) { await area.click(); await wait(800); }
            const openArea = page.getByRole('button', { name: /Open area/ }).first();
            if (await openArea.count()) await openArea.click();
            await sceneSettled(page, '.architecture-folder-label');
        } else {
            await tab(page, 'hotspots').click();
            await sceneSettled(page, '.architecture-node-label');
        }
        // Ohne Auswahl: ein gewaehltes Schild darf Platz beanspruchen, und neben ihm aendert ein Zeiger nichts. Ein Klick ins Leere hebt sie auf.
        if (await page.locator('.architecture-node-label.is-selected').count()) {
            const box = await page.locator('.atlas-architecture canvas').first().boundingBox();
            if (box) { await page.mouse.click(box.x + 12, box.y + box.height - 12); await wait(900); }
        }
        const selected = await page.locator('.architecture-node-label.is-selected').count();
        await page.mouse.move(5, 5); await wait(600);
        const resting = await folderPlacement(page);
        const restingChips = await shownChips(page);
        await shot(page, 'K20', `${view}-ruhend`, `${scope}: Ordnernamen ohne Zeiger`);
        const ids = await page.evaluate(() => [...document.querySelectorAll('button.architecture-node-label:not(.is-hidden):not(.is-selected)')].map((label) => label.dataset.nodeId).filter(Boolean));
        const moved = [];
        const hid = [];
        let shown = false;
        for (const id of ids.slice(0, 14)) {
            const chip = page.locator(`button.architecture-node-label[data-node-id="${id.replace(/"/g, '\\"')}"]`).first();
            if (!(await chip.isVisible().catch(() => false))) continue;
            await chip.hover({ timeout: 3000 }).catch(() => {});
            await wait(450);
            const hovered = await folderPlacement(page);
            const changed = hovered.filter((item, index) => item !== resting[index]);
            if (changed.length) moved.push({ chip: id, changed: changed.slice(0, 4) });
            const now = new Set(await shownChips(page));
            const gone = restingChips.filter((other) => !now.has(other));
            if (gone.length) hid.push({ chip: id, hidden: gone.slice(0, 4) });
            if (!shown) { await shot(page, 'K20', `${view}-schild-ueberfahren`, `${scope}: Zeiger auf dem Schild ${id}, Ordnernamen bleiben`); shown = true; }
            await page.mouse.move(5, 5); await wait(350);
        }
        sweeps.push({ scope, selected, chipsHovered: Math.min(ids.length, 14), folders: resting.length, moved: moved.length, examples: moved.slice(0, 3),
            chipsHidden: hid.length, hiddenExamples: hid.slice(0, 3) });
    }
    check('K20-hover', 'Ordnernamen und Nachbarschilder bleiben stehen, waehrend der Zeiger ueber die Schilder faehrt (Hotspots, Overview in django)',
        sweeps.every((sweep) => sweep.selected === 0 && sweep.chipsHovered > 0 && sweep.folders > 0 && sweep.moved === 0 && sweep.chipsHidden === 0), { sweeps });
}

/* ------------------------------------------------------------------ */
/* K21: jeder Aufruf-Kasten hat seinen Namen                              */

async function journeyLabels(page) {
    const geometry = await labelGeometry(page);
    const chips = geometry.labels.filter((item) => item.kind === 'chip');
    const state = await behaviorState(page);
    return { state, chips: chips.map((item) => ({ text: item.text, title: item.title, inside: item.inside, truncated: item.truncated })), overlaps: geometry.overlaps };
}

async function k21(page) {
    await open(page, PROJECT);
    await tab(page, 'behavior').click();
    await sceneSettled(page, '.system-scene-node-label', 40000);
    await shot(page, 'K21', 'behavior-start', 'Behavior mit der automatisch gewaehlten Operation');
    let chosen = null;
    await series(page, 'K21', 'handle-loaddata', async () => { chosen = await selectEntry(page, /^handle · django\/core\/management\/commands\/loaddata\.py$/); }, 'handle · loaddata.py gewaehlt');
    await page.waitForFunction(() => /What can handle call\?/i.test(document.querySelector('.behavior-heading h2')?.textContent ?? ''), null, { timeout: 30000 }).catch(() => {});
    await wait(2500);
    const measured = await journeyLabels(page);
    await shot(page, 'K21', 'handle-loaddata-fertig', 'Alle Aufruf-Kaesten mit Namen');
    const named = measured.chips.filter((chip) => chip.text.replace(/^(START|POSSIBLE CALL|HOP \d+)/, '').trim().length > 0);
    check('K21', 'Behavior handle · loaddata.py: jeder Kasten der Seite zeigt seinen Namen, gekuerzt mit Tooltip statt ausgeblendet',
        chosen !== null && Number.isFinite(measured.state.sceneNodes) && named.length === measured.state.sceneNodes && measured.chips.every((chip) => chip.inside && chip.title.length > 0) && measured.overlaps.length === 0,
        { chosen, sceneNodes: measured.state.sceneNodes, labels: measured.chips.length, named: named.length, chips: measured.chips, overlaps: measured.overlaps });
}

/* ------------------------------------------------------------------ */
/* K25: Zahlen passen, alle Kaesten der Seite im Bild, Einpassen beim Blaettern */

async function switchProject(page, name) {
    await page.locator('.atlas-shell details summary', { hasText: PROJECT }).first().click();
    const entry = page.locator('.atlas-project-results button', { has: page.locator('.atlas-project-result-name', { hasText: new RegExp(`^${name}$`) }) }).first();
    await entry.waitFor({ timeout: 20000 });
    await entry.click();
}

async function k25(page) {
    // Wie im Handtest: Behavior in django-demo offen, dann der Projektwechsel nach cbm.
    await open(page, PROJECT);
    await tab(page, 'behavior').click();
    await sceneSettled(page, '.system-scene-node-label', 40000);
    await shot(page, 'K25', 'behavior-django-vor-wechsel', 'Behavior in django-demo vor dem Projektwechsel');
    await series(page, 'K25', 'behavior-cbm', () => switchProject(page, CONTROL), 'Projektwechsel nach cbm mit offenem Behavior');
    // cbm oeffnet in der Unteransicht, die sein Profil kennt; ein frisches Profil zeigt Overview. Behavior darum ausdruecklich waehlen.
    await page.waitForFunction((name) => new URLSearchParams(location.search).get('project') === name || document.querySelector('.atlas-shell details summary')?.textContent?.includes(name), CONTROL, { timeout: 30000 }).catch(() => {});
    await wait(1500);
    if (await tab(page, 'behavior').getAttribute('aria-selected') !== 'true') await tab(page, 'behavior').click();
    await page.waitForFunction(() => /What can main call\?/i.test(document.querySelector('.behavior-heading h2')?.textContent ?? ''), null, { timeout: 40000 }).catch(() => {});
    await sceneSettled(page, '.system-scene-node-label', 40000);
    const first = await journeyLabels(page);
    await shot(page, 'K25', 'seite-1', 'Erste Seite der direkten Aufrufe von main');
    const direct = Number(first.state.navigation.match(/(\d+) direct callees?/)?.[1] ?? NaN);
    const pageTotal = Number(first.state.pages.match(/of (\d+)/)?.[1] ?? NaN);
    const explained = /not drawn|beyond the display limit/i.test(first.state.pages);
    // Ohne gesetzten Filter darf der Zaehler keinen Filter nennen.
    const noFilterClaim = !/matching the filter/i.test(first.state.pages);
    const more = page.getByRole('button', { name: /More calls/ }).first();
    let second = null;
    if (await more.count() && await more.isEnabled()) {
        await series(page, 'K25', 'mehr-aufrufe', () => more.click(), 'More calls angeklickt');
        second = await journeyLabels(page);
    }
    const allInside = (reading) => reading && reading.chips.length === reading.state.sceneNodes && reading.chips.every((chip) => chip.inside);
    check('K25', 'Behavior cbm main: Anzahl direkter Aufrufe und Seitenzaehler stimmen ueberein, alle Kaesten jeder Seite im Bild',
        Number.isFinite(direct) && (direct === pageTotal || explained) && noFilterClaim && allInside(first) && (second === null || allInside(second)),
        { direct, pageTotal, pages: first.state.pages, navigation: first.state.navigation, noFilterClaim, firstPage: { sceneNodes: first.state.sceneNodes, inside: first.chips.filter((chip) => chip.inside).length, labels: first.chips.map((chip) => chip.text) },
            secondPage: second ? { pages: second.state.pages, sceneNodes: second.state.sceneNodes, inside: second.chips.filter((chip) => chip.inside).length, labels: second.chips.map((chip) => chip.text) } : null });
}

/* ------------------------------------------------------------------ */
/* K22: Service map ohne Deployment-Dateien                               */

async function k22(page) {
    await open(page, PROJECT);
    await series(page, 'K22', 'routes', () => tab(page, 'routes').click(), 'Routes, Service map fuer django-demo');
    // Die Service map laedt nach; erst ihr eigener Text sagt, ob die Suche nach Compose-Dateien fertig ist.
    await page.waitForFunction(() => {
        const map = document.querySelector('.container-map');
        return Boolean(map) && !/Finding indexed Compose definitions/.test(map.textContent ?? '');
    }, null, { timeout: 60000 }).catch(() => {});
    await wait(1000);
    const text = await page.locator('.container-map').first().innerText().catch(() => '');
    await shot(page, 'K22', 'service-map-meldung', 'Meldung der Service map');
    const endpoints = page.getByRole('button', { name: /Show endpoints/ }).first();
    let switched = false;
    if (await endpoints.count()) {
        await endpoints.click(); await wait(2500);
        switched = await page.getByRole('button', { name: 'Endpoints', exact: true }).first().getAttribute('aria-pressed') === 'true';
        await shot(page, 'K22', 'endpoints-nach-knopf', 'Nach dem Knopf: Endpoints');
    }
    check('K22', 'Service map ohne Deployment-Dateien sagt das klar und fuehrt zu Endpoints, kein Lesefehler',
        /No Docker Compose deployment files/i.test(text) && !/Could not read/i.test(text) && switched, { message: text.split('\n').find((line) => /deployment|Compose|Could not/i.test(line)) ?? text.slice(0, 160), switched });
}

/* ------------------------------------------------------------------ */
/* K23: unterscheidbare Routen-Schilder                                   */

async function k23(page) {
    await open(page, PROJECT);
    await tab(page, 'routes').click(); await wait(2000);
    await page.getByRole('button', { name: 'Endpoints', exact: true }).first().click();
    await sceneSettled(page, '.architecture-node-label');
    await shot(page, 'K23', 'endpoints', 'Endpoints gruppiert');
    // Die Gruppe wie im Handtest waehlen; ist ihr Schild gerade ausgeblendet, ueber "Browse map".
    const group = page.locator('button.architecture-node-label:not(.is-hidden)', { hasText: /^\/generic-lastmod\s*·\s*2/ }).first();
    if (await group.count()) await group.click();
    else {
        await page.locator('.spatial-node-list summary').first().click();
        await page.locator('.spatial-node-list button', { hasText: /^\/generic-lastmod · 2/ }).first().click();
    }
    await wait(800);
    await shot(page, 'K23', 'gruppe-gewaehlt', 'Gruppe /generic-lastmod gewaehlt');
    const show = page.getByRole('button', { name: /Show these 2 routes/ }).first();
    if (await show.count()) await series(page, 'K23', 'zwei-routen', () => show.click(), 'Show these 2 routes');
    const geometry = await labelGeometry(page);
    const routes = geometry.labels.filter((item) => item.kind === 'chip' && /^\W*(…|\/generic-lastmod)/.test(item.text) && !/·\s*2/.test(item.text));
    const texts = routes.map((item) => item.text);
    check('K23', 'Gefilterte Routen zeigen das Unterscheidende, voller Pfad im Tooltip',
        routes.length === 2 && new Set(texts).size === 2 && routes.every((item) => !item.truncated && /\/generic-lastmod\//.test(item.title)),
        { routes: routes.map((item) => ({ text: item.text, title: item.title.split('\n')[0], truncated: item.truncated })) });
}

/* ------------------------------------------------------------------ */
/* K26: gleiche Konsolenmeldungen einmal mit Zaehler                       */

async function k26(page) {
    await open(page, CONTROL, 'system');
    uiLogPosts = [];
    await page.evaluate((message) => { for (let i = 0; i < 120; i++) console.warn(message); }, THREE_CLOCK);
    await wait(2500);
    // Wie beim Schliessen des Tabs: der letzte Stand geht mit "final" hinaus.
    await page.evaluate(() => { window.dispatchEvent(new Event('pagehide')); });
    await wait(800);
    const beacons = await page.evaluate(async () => Promise.all(window.__beacons.filter((item) => item.url.includes('/api/ui-log')).map(async (item) => JSON.parse(await item.data.text()))));
    const entries = [...uiLogPosts, ...beacons].flatMap((post) => post.entries ?? []).filter((entry) => entry.message === THREE_CLOCK);
    const counted = entries.map((entry) => entry.detail ?? '').filter(Boolean);
    check('K26-transport', '120 gleiche Warnungen gehen als Erstmeldung plus Zaehler an /api/ui-log, nicht 120-mal',
        entries.length > 0 && entries.length <= 4 && counted.some((detail) => /120/.test(detail)), { posted: entries.length, details: counted });

    // Durchsicht: derselbe Fehlertext von zwei Stellen sind zwei Fehler; nur wirklich gleiche werden gezaehlt.
    uiLogPosts = [];
    await page.evaluate(() => {
        window.__beacons = [];
        // Ein Aufrufort fuer alle: der Stapel ist bei allen gleich, nur Datei und Zeile unterscheiden die erste Stelle von der zweiten.
        const places = [['probe-a.js', 10], ...Array.from({ length: 12 }, () => ['probe-b.js', 99])];
        for (const [file, line] of places) window.dispatchEvent(new ErrorEvent('error', { message: 'Uncaught TypeError: handtest probe', filename: file, lineno: line, colno: 3, error: new TypeError('handtest probe') }));
    });
    await wait(2500);
    await page.evaluate(() => { window.dispatchEvent(new Event('pagehide')); });
    await wait(800);
    const probeBeacons = await page.evaluate(async () => Promise.all(window.__beacons.filter((item) => item.url.includes('/api/ui-log')).map(async (item) => JSON.parse(await item.data.text()))));
    const probes = [...uiLogPosts, ...probeBeacons].flatMap((post) => post.entries ?? []).filter((entry) => entry.message === 'Uncaught TypeError: handtest probe')
        .map((entry) => ({ url: entry.url ?? null, line: entry.line ?? null, count: Number(/^(\d+) identical/.exec(entry.detail ?? '')?.[1] ?? 1), stack: Boolean(entry.stack) }));
    const full = probes.filter((entry) => entry.count === 1);
    const countsB = probes.filter((entry) => entry.count > 1);
    check('K26-identity', 'Gleicher Fehlertext von zwei Stellen geht zweimal voll hinaus, der Zaehler nennt seine Stelle',
        full.length === 2 && new Set(full.map((entry) => entry.url)).size === 2 && countsB.length > 0 && countsB.every((entry) => entry.url === 'probe-b.js' && entry.line === 99) && Math.max(...countsB.map((entry) => entry.count)) === 12,
        { probes });

    await page.getByRole('tab', { name: 'Logs' }).click();
    await page.getByLabel('Log severity').selectOption('warn');
    await wait(4000);
    const rows = await page.evaluate(() => [...document.querySelectorAll('.system-log-record')].map((row) => row.textContent ?? ''));
    // Eine Meldung und ihre eigenen Zaehler gehoeren in eine Zeile: gleiche Felder in zwei Zeilen waeren dieselbe Meldung zweimal.
    const split = await page.evaluate(() => {
        const keys = new Map();
        for (const row of document.querySelectorAll('.system-log-record')) {
            let entry; try { entry = JSON.parse(row.querySelector('pre')?.textContent ?? ''); } catch { continue; }
            if (!entry || typeof entry.message !== 'string') continue;
            const detail = typeof entry.detail === 'string' ? (/^\d+ identical entries in this session/.test(entry.detail) ? entry.detail.split('\n').slice(1).join('\n') : entry.detail) : '';
            const key = JSON.stringify([entry.level, entry.source, entry.project ?? '', entry.message, detail.slice(0, 2048), (entry.stack ?? '').slice(0, 2048), entry.url ?? '', entry.line ?? null, entry.col ?? null]);
            keys.set(key, (keys.get(key) ?? 0) + 1);
        }
        return [...keys].filter(([, count]) => count > 1).map(([key, count]) => ({ message: JSON.parse(key)[3].slice(0, 80), rows: count }));
    });
    const clockRows = rows.filter((row) => row.includes('THREE.THREE.Clock'));
    const counts = clockRows.map((row) => Number(row.match(/×(\d+)/)?.[1] ?? 1));
    await shot(page, 'K26', 'system-logs', 'System › Logs, Warnungen und Fehler');
    await page.evaluate(() => { const log = document.querySelector('.system-log'); if (log) log.scrollTop = log.scrollHeight; });
    await wait(300);
    await shot(page, 'K26', 'system-logs-ende', 'System › Logs, Ende der Liste');
    check('K26-logs', 'System › Logs fasst gleiche Meldungen zu einer Zeile mit Zaehler zusammen',
        clockRows.length > 0 && clockRows.length <= 3 && counts.some((count) => count > 1) && split.length === 0, { rows: rows.length, clockRows: clockRows.length, counts, splitRows: split });
}

/* ------------------------------------------------------------------ */

const context = await chromium.launchPersistentContext(PROFILE, {
    headless: !HEADED, ...(HEADED ? {} : { channel: 'chromium' }), viewport: VIEWPORT, deviceScaleFactor: SCALE,
    args: ['--enable-unsafe-webgpu', '--ignore-gpu-blocklist'],
});
await context.addInitScript(() => {
    try { localStorage.setItem('cbm.workspace.setup', 'done'); } catch { /* ohne Speicher geht es auch */ }
    // Eine Bake beim Verlassen der Seite umgeht das Routing des Browsers: sie bleibt hier und erreicht den Server nie.
    window.__beacons = [];
    navigator.sendBeacon = (url, data) => { window.__beacons.push({ url: String(url), data }); return true; };
});
await context.route('**/api/ui-log', async (route) => {
    if (route.request().method() !== 'POST') return route.continue();
    try { uiLogPosts.push(route.request().postDataJSON()); } catch { /* eine Bake ohne lesbaren Inhalt */ }
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{"accepted":0}' });
});
const page = context.pages()[0] ?? await context.newPage();
const pageErrors = [];
page.on('pageerror', (error) => { pageErrors.push(error.message); console.error(`[pageerror] ${error.message}`); });
const steps = { K18: k18, K19: async (current) => { await k19(current); await k19Lines(current); }, K20: k20, K21: k21, K22: k22, K23: k23, K25: k25, K26: k26 };
// Beschriftungen und Kamera haengen von der Groesse der Zeichenflaeche ab: diese drei auch in der Ansicht des Handtests.
const bothLayouts = new Set(['K20', 'K21', 'K25']);
for (const id of ONLY) {
    for (const name of bothLayouts.has(id) ? ['standard', 'handtest'] : ['standard']) {
        layout = name;
        try { await steps[id]?.(page); } catch (error) { check(id, 'Ablauf abgebrochen', false, String(error?.message ?? error)); }
    }
    layout = 'standard';
}
check('X1', 'Keine Seitenfehler im Lauf', pageErrors.length === 0, { pageErrors });
await context.close();

for (const [id, list] of images) {
    const results = checks.filter((item) => item.id.startsWith(id));
    const lines = [`# ${id} (${TAG})`, '', ...results.map((item) => `- ${item.pass ? 'PASS' : 'FAIL'} ${item.id}: ${item.title}`), '', '| Bild | Zeigt |', '|---|---|',
        ...list.map((item) => `| ${item.file} | ${item.note} |`), '', '```json', JSON.stringify(results.map((item) => ({ id: item.id, measured: item.measured })), null, 1).slice(0, 6000), '```', ''];
    await writeFile(join(OUT, id, `index-${TAG}.md`), lines.join('\n'));
}
await mkdir(OUT, { recursive: true });
// Ein Teillauf (--only) ueberschreibt den Bericht des ganzen Laufs nicht.
const partial = String(arg('only', '')) ? `-${ONLY.join('-')}` : '';
await writeFile(join(OUT, `report-${TAG}${partial}.json`), JSON.stringify({ origin: ORIGIN, tag: TAG, checks }, null, 2));
const failed = checks.filter((item) => !item.pass);
console.log(`${checks.length - failed.length}/${checks.length} bestanden`);
if (failed.length) process.exitCode = 1;
