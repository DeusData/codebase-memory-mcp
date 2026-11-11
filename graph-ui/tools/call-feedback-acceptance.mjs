#!/usr/bin/env node
/*
 * Abnahme der Fixes zum Review-Call vom 02.10.2026.
 *
 *   node tools/call-feedback-acceptance.mjs --origin http://127.0.0.1:4350 \
 *        --out /tmp/acceptance --profile /tmp/profile [--project django-demo] [--control cbm] \
 *        [--only galaxy,chat,architecture] [--no-model]
 *
 * Wo tools/call-feedback-capture.mjs nur aufzeichnet, entscheidet dieses Skript:
 * jede Pruefung steht fuer genau einen Punkt aus
 * docs/development/pr-2068-call-feedback-plan.md, misst ihn in der laufenden
 * Seite und schreibt bestanden oder nicht bestanden mit dem gemessenen Wert
 * nach report.json. Daneben liegen die Bilder jeder Pruefung und ein Video,
 * aus dem fuer die Bewegungen (Expandieren, Zoomen, Pfad durchschalten)
 * Bildstreifen mit sechs Bildern pro Sekunde entstehen. Ein Pruefer, der kein
 * Video ansehen kann, sieht so auch die Uebergaenge und nicht nur das Ende.
 *
 * Es startet keinen Server. Es braucht einen laufenden Server mit UI und ein
 * Browserprofil, in dem das lokale Modell schon liegt (sonst --no-model).
 */

import { chromium } from 'playwright';
import { execFile } from 'node:child_process';
import { mkdir, readdir, rename, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';

const run = promisify(execFile);
const argv = process.argv.slice(2);
const arg = (name, fallback) => {
    const index = argv.indexOf(`--${name}`);
    if (index < 0) return fallback;
    const value = argv[index + 1];
    return value === undefined || value.startsWith('--') ? true : value;
};

const ORIGIN = String(arg('origin', 'http://127.0.0.1:4350')).replace(/\/$/, '');
const OUT = resolve(String(arg('out', 'verification/call-feedback-2026-10-02/abnahme')));
const PROFILE = resolve(String(arg('profile', join(OUT, '..', 'profile'))));
const PROJECT = String(arg('project', 'django-demo'));
const CONTROL = String(arg('control', 'cbm'));
const ONLY = String(arg('only', 'galaxy,chat,architecture')).split(',');
const MODEL = arg('no-model', false) !== true;
// Ohne Fenster, wie die Chat-Pruefung: ANGLE auf Metal gibt dem Modell die GPU mit shader-f16; `--headed` zeigt das Fenster.
const HEADED = arg('headed', false) === true;
const VIEWPORT = { width: 1600, height: 1000 };
// Ein vollstaendiges ffmpeg zuerst: das von Playwright mitgelieferte kennt nur die Filter fuer die Aufnahme.
const FFMPEG = ['/opt/homebrew/bin/ffmpeg', '/usr/local/bin/ffmpeg', join(homedir(), 'Library/Caches/ms-playwright/ffmpeg-1011/ffmpeg-mac')].find((path) => existsSync(path));

const wait = (ms) => new Promise((done) => setTimeout(done, ms));
const started = Date.now();
const since = () => (Date.now() - started) / 1000;

/* ------------------------------------------------------------------ */
/* Server                                                             */

let rpcId = 1;
async function tool(name, args) {
    const res = await fetch(`${ORIGIN}/rpc`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: rpcId++, method: 'tools/call', params: { name, arguments: args } }),
    });
    const body = await res.json();
    const text = body?.result?.content?.[0]?.text ?? '';
    try { return JSON.parse(text); } catch { return { raw: text }; }
}

/** Die eingehenden Kanten eines Namens, so wie der Index sie kennt. */
async function incoming(name) {
    const answer = await tool('query_graph', { project: PROJECT, query: `MATCH (a)-[r]->(b) WHERE b.name = '${name}' RETURN a.name AS source, type(r) AS type ORDER BY type, source LIMIT 500` });
    const rows = String(answer.raw ?? '').split('\n').map((line) => line.trim()).filter((line) => /^"?[\w.]+"?\s+"?[A-Z_]+"?$/.test(line));
    return rows.map((line) => { const [source, type] = line.replace(/"/g, '').split(/\s+/); return { source, type }; });
}

/* ------------------------------------------------------------------ */
/* In jede Seite                                                       */

function pageProbe() {
    try { localStorage.setItem('cbm.workspace.setup', 'done'); } catch { /* ohne Speicher geht es auch */ }
    const mountCursor = () => {
        if (document.getElementById('probe-cursor')) return;
        const dot = document.createElement('div');
        dot.id = 'probe-cursor';
        dot.style.cssText = 'position:fixed;left:-100px;top:-100px;width:22px;height:22px;margin:-11px 0 0 -11px;border:2px solid #ff3b6b;'
            + 'border-radius:50%;box-shadow:0 0 0 2px rgba(0,0,0,.6);pointer-events:none;z-index:2147483647;';
        document.documentElement.appendChild(dot);
        document.addEventListener('mousemove', (event) => { dot.style.left = `${event.clientX}px`; dot.style.top = `${event.clientY}px`; }, true);
    };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mountCursor); else mountCursor();
    window.__probeErrors = [];
    window.addEventListener('error', (event) => window.__probeErrors.push(String(event.message)));
    window.__probeCanvasSeq = 0;
    window.__probeCanvasId = () => {
        const canvas = document.querySelector('.atlas-galaxy canvas');
        if (!canvas) return null;
        if (!canvas.dataset.probeId) canvas.dataset.probeId = String(++window.__probeCanvasSeq);
        return canvas.dataset.probeId;
    };
    // Alles, was an den Modell-Worker geht, auch ueber spaeter angelegte Worker.
    window.__probeWorker = [];
    const NativeWorker = window.Worker;
    window.Worker = class ProbeWorker extends NativeWorker {
        constructor(url, options) {
            super(url, options);
            this.__probeSrc = String(url);
        }
        postMessage(message, transfer) {
            try {
                if (message && typeof message === 'object' && Array.isArray(message.messages)) {
                    window.__probeWorker.push({ t: performance.now(), kind: message.kind, profile: message.options?.generationProfile ?? message.generationProfile ?? '', messages: message.messages });
                }
            } catch { /* nur Beobachtung */ }
            return super.postMessage(message, transfer);
        }
    };
    window.__probeTimeline = [];
    setInterval(() => {
        const fit = globalThis.__atlasGalaxyFit?.measure?.();
        const galaxy = globalThis.__atlasGalaxy;
        window.__probeTimeline.push({
            t: Math.round(performance.now()), canvas: window.__probeCanvasId?.() ?? null,
            camera: fit ? fit.camera.position.map((v) => Number(v.toFixed(2))) : null,
            fits: galaxy?.fits ?? null, nodes: galaxy?.nodes ?? null,
        });
        if (window.__probeTimeline.length > 8000) window.__probeTimeline.splice(0, 2000);
    }, 100);
}

/* ------------------------------------------------------------------ */
/* Ergebnis                                                            */

const checks = [];
const marks = [];
let shotIndex = 0;

function check(id, title, pass, measured, images = []) {
    checks.push({ id, title, pass: Boolean(pass), measured, images });
    console.log(`${pass ? 'PASS' : 'FAIL'}  ${id}  ${title}  ${typeof measured === 'string' ? measured : JSON.stringify(measured)}`);
}

async function shot(page, label) {
    shotIndex += 1;
    const file = `${String(shotIndex).padStart(3, '0')}-${label}.png`;
    await page.screenshot({ path: join(OUT, 'bilder', file) });
    return file;
}

/** Eine Bewegung, die als Bildstreifen aus dem Video kommen soll. */
function mark(label, from, to) { marks.push({ label, from, to }); }

async function state(page) {
    return page.evaluate(() => {
        const fit = globalThis.__atlasGalaxyFit?.measure?.();
        const galaxy = globalThis.__atlasGalaxy;
        return {
            canvas: window.__probeCanvasId?.() ?? null,
            fits: galaxy?.fits ?? null, nodes: galaxy?.nodes ?? null, drawnEdges: galaxy?.drawnEdges ?? null,
            outside: fit?.outside ?? null, inside: fit?.inside ?? null, camera: fit?.camera ?? null,
            viewport: fit?.viewport ?? null,
            toolbar: document.querySelector('.atlas-graph-exploration')?.textContent?.replace(/\s+/g, ' ').trim() ?? '',
        };
    });
}

async function open(page, project, workspace) {
    await page.goto(`${ORIGIN}/?project=${encodeURIComponent(project)}&workspace=${workspace}`, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.atlas-shell', { timeout: 30000 });
    await wait(1500);
}

async function galaxyReady(page) {
    await page.waitForFunction(() => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0 && globalThis.__atlasGalaxyFit !== undefined, null, { timeout: 60000 });
    await wait(1200);
}

async function select(page, name) {
    const input = page.getByRole('searchbox', { name: 'Find a graph node' }).first();
    await input.click();
    await input.fill('');
    await input.type(name, { delay: 25 });
    const choice = page.locator('.atlas-galaxy-search-results button', { has: page.locator('strong', { hasText: new RegExp(`^${name}$`) }) }).first();
    await choice.waitFor({ timeout: 20000 });
    await choice.hover();
    await choice.click();
}

/** Bis die Statuszeile einen fertigen Ausschnitt meldet. */
async function scopeSettled(page, timeout = 20000) {
    await page.waitForFunction(() => {
        const text = document.querySelector('.atlas-graph-exploration')?.textContent ?? '';
        return /\d+ nodes? · \d+ edges?/.test(text) && !/Loading relationships/.test(text);
    }, null, { timeout }).catch(() => {});
    await wait(1500);
}

const timelineSince = (page, t0) => page.evaluate((from) => window.__probeTimeline.filter((row) => row.t >= from), t0);
const now = (page) => page.evaluate(() => Math.round(performance.now()));
const intersects = (a, b) => Boolean(a && b) && !(a.x >= b.x + b.width || b.x >= a.x + a.width || a.y >= b.y + b.height || b.y >= a.y + a.height);

/* ------------------------------------------------------------------ */
/* Galaxie                                                              */

async function galaxyChecks(page) {
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    const before = await state(page);
    const images = [await shot(page, 'galaxie-ganzer-graph')];
    const t0 = await now(page);
    const v0 = since();
    await select(page, 'JSONBAgg');
    images.push(await shot(page, 'jsonbagg-sofort'));
    const blankFrames = [];
    for (let i = 0; i < 8; i += 1) {
        blankFrames.push(await page.evaluate(() => globalThis.__atlasGalaxyFit?.measure?.().nodes ?? 0));
        await wait(40);
    }
    await wait(250); images.push(await shot(page, 'jsonbagg-250ms'));
    await wait(750); images.push(await shot(page, 'jsonbagg-1s'));
    await scopeSettled(page);
    images.push(await shot(page, 'jsonbagg-fertig'));
    const selected = await state(page);
    mark('auswahl-jsonbagg', v0, since());

    // G2: eine Ebene mit Kanten, kein isolierter Knoten, und weniger als 1 geht nicht.
    const minus = page.getByRole('button', { name: 'Remove graph layer' });
    const minusDisabled = await minus.isDisabled().catch(() => null);
    const layerText = await page.locator('.atlas-graph-exploration').first().evaluate((bar) => [...bar.querySelectorAll('span, output, strong, small')].map((el) => el.textContent?.trim() ?? '').find((text) => /^\d+ layers?$/.test(text)) ?? '').catch(() => '');
    check('G2', 'Auswahl oeffnet eine Ebene mit Kanten', /^1 layer$/.test(layerText) && (selected.drawnEdges ?? 0) > 0 && minusDisabled === true,
        { layerText, drawnEdges: selected.drawnEdges, minusDisabled }, images.slice(-1));

    // G1: dieselbe Zeichenflaeche, Expand passt nicht neu ein.
    const expand = page.getByRole('button', { name: 'Expand +1' });
    const v1 = since();
    const fitsBeforeExpand = selected.fits;
    await expand.click();
    const expandImages = [await shot(page, 'expand1-sofort')];
    await wait(250); expandImages.push(await shot(page, 'expand1-250ms'));
    await wait(750); expandImages.push(await shot(page, 'expand1-1s'));
    await scopeSettled(page);
    expandImages.push(await shot(page, 'expand1-fertig'));
    const expanded = await state(page);
    mark('expand-1', v1, since());
    const timeline = await timelineSince(page, t0);
    const canvases = [...new Set(timeline.map((row) => row.canvas))];
    const errors = await page.evaluate(() => window.__probeErrors.slice());
    check('G1', 'Zeichenflaeche bleibt beim Auswaehlen und Expandieren dieselbe, Expand passt nicht neu ein',
        canvases.length === 1 && canvases[0] !== null && expanded.fits === fitsBeforeExpand && errors.length === 0 && blankFrames.every((count) => count > 0),
        { canvasIds: canvases, sceneNodesRightAfterSelect: blankFrames, fitsBefore: before.fits, fitsAfterSelect: fitsBeforeExpand, fitsAfterExpand: expanded.fits, pageErrors: errors }, [...images.slice(1, 3), ...expandImages]);

    // G4: die Wurzel bleibt markiert, nahe der Bildmitte, und alles bleibt im Bild.
    const root = await page.locator('[data-testid="atlas-galaxy-root-marker"]').first().boundingBox().catch(() => null);
    const canvasBox = await page.locator('.atlas-galaxy canvas').first().boundingBox();
    const offset = root && canvasBox ? Math.hypot(root.x + root.width / 2 - (canvasBox.x + canvasBox.width / 2), root.y + root.height / 2 - (canvasBox.y + canvasBox.height / 2)) / Math.min(canvasBox.width, canvasBox.height) : null;
    check('G4', 'Wurzel nach dem Expandieren markiert, mittig und alle Knoten im Bild',
        root !== null && offset !== null && offset < 0.2 && expanded.outside === 0,
        { rootMarker: root, offsetFromCentre: offset === null ? null : Number(offset.toFixed(3)), outside: expanded.outside, inside: expanded.inside }, expandImages.slice(-1));

    // G7: die Werkzeugleiste bleibt eine Zeile, bei geschlossenem und (Handtest K3) bei offenem Chat.
    const toolbarRows = async (chat) => {
        const measured = await page.evaluate(() => {
            const bar = document.querySelector('.atlas-graph-exploration');
            if (!bar) return null;
            // Eine Zeile je Mitte der Kinder der Leiste; die Kinder ihrer Kinder (Text in Knoepfen) zaehlen nicht extra.
            const items = [...bar.children].filter((el) => el.getBoundingClientRect().height > 0);
            const centres = items.map((el) => { const r = el.getBoundingClientRect(); return Math.round((r.top + r.bottom) / 2 / 16); });
            const box = bar.getBoundingClientRect();
            return { rows: [...new Set(centres)].length, width: Math.round(box.width), height: Math.round(box.height), overflow: bar.scrollWidth > bar.clientWidth + 1, fit: bar.dataset.fit ?? null };
        });
        return { chat, ...measured };
    };
    const toolbar = [await toolbarRows('closed')];
    const g7Images = [await shot(page, 'g7-chat-zu')];
    await page.getByRole('button', { name: 'Open chat' }).first().click().catch(() => {});
    await page.waitForSelector('.cbm-chat-input-row', { timeout: 10000 }).catch(() => {});
    await wait(1500);
    toolbar.push(await toolbarRows('open'));
    g7Images.push(await shot(page, 'g7-chat-offen'));
    await page.getByRole('button', { name: 'Hide chat' }).first().click().catch(() => {});
    await wait(1200);
    check('G7', 'Galaxie-Werkzeugleiste bricht bei 1600 px nicht um, mit geschlossenem und mit offenem Chat',
        toolbar.length === 2 && toolbar.every((row) => row.rows === 1 && row.height < 70 && !row.overflow) && toolbar[1].width < toolbar[0].width,
        { toolbar }, g7Images);

    // G3: Mausrad zoomt zum Zeiger.
    const zoomImages = [];
    const analysis = [];
    const v2 = since();
    for (const spot of [{ label: 'rechts-oben', fx: 0.78, fy: 0.25 }, { label: 'links-unten', fx: 0.2, fy: 0.78 }]) {
        await page.getByRole('button', { name: 'fit view' }).click().catch(() => {});
        await wait(1200);
        const x = canvasBox.x + canvasBox.width * spot.fx;
        const y = canvasBox.y + canvasBox.height * spot.fy;
        await page.mouse.move(x, y, { steps: 6 });
        await wait(300);
        const a = (await state(page)).camera;
        zoomImages.push(await shot(page, `zoom-${spot.label}-start`));
        for (let step = 1; step <= 4; step += 1) {
            await page.mouse.wheel(0, -240);
            await wait(500);
            zoomImages.push(await shot(page, `zoom-${spot.label}-${step}`));
        }
        const b = (await state(page)).camera;
        if (a && b) {
            const delta = a.position.map((v, i) => b.position[i] - v);
            const length = Math.hypot(...delta) || 1;
            analysis.push({ spot: spot.label, moved: Number(length.toFixed(2)), along: Number((Math.abs(delta.reduce((sum, v, i) => sum + v * a.direction[i], 0)) / length).toFixed(4)) });
        }
    }
    mark('zoom', v2, since());
    check('G3', 'Mausrad-Zoom wandert zum Zeiger (Anteil entlang der Blickrichtung deutlich unter 1)',
        analysis.length === 2 && analysis.every((row) => row.along < 0.97 && row.moved > 1), analysis, zoomImages);

    // G5: Pfad zu einem Knoten, Schritte vor und zurueck, Esc raeumt auf.
    await page.getByRole('button', { name: 'fit view' }).click().catch(() => {});
    await wait(1000);
    const pathImages = [];
    const v3 = since();
    const picker = page.locator('details.atlas-graph-path-picker');
    let pathResult = { opened: false };
    if (await picker.count()) {
        await picker.locator('summary').click();
        await wait(300);
        const search = page.getByRole('searchbox', { name: 'Find a path target' });
        await search.type('Func', { delay: 25 });
        await wait(500);
        pathImages.push(await shot(page, 'pfad-suche'));
        const candidate = page.locator('.atlas-graph-path-menu button', { hasText: /^\W*Func\b/ }).first().or(page.locator('.atlas-graph-path-menu button').first()).first();
        if (await candidate.count()) {
            await candidate.click();
            await wait(1200);
            pathImages.push(await shot(page, 'pfad-angezeigt'));
            const panel = page.locator('[data-testid="atlas-galaxy-path-panel"]');
            const heading = await panel.innerText().catch(() => '');
            const nextButton = panel.getByRole('button', { name: 'Next' });
            const positions = [];
            for (let step = 0; step < 3 && await nextButton.isEnabled().catch(() => false); step += 1) {
                await nextButton.click();
                await wait(600);
                pathImages.push(await shot(page, `pfad-schritt-${step + 1}`));
                positions.push((await panel.innerText().catch(() => '')).match(/\d+ of \d+/)?.[0] ?? '');
            }
            await page.mouse.move(canvasBox.x + 40, canvasBox.y + 40);
            await page.keyboard.press('Escape');
            await wait(600);
            pathImages.push(await shot(page, 'pfad-esc'));
            const cleared = !(await panel.count()) || !(await panel.isVisible().catch(() => false));
            pathResult = { opened: true, heading: heading.split('\n').slice(0, 4).join(' | '), positions, clearedByEscape: cleared };
        }
    }
    mark('pfad', v3, since());
    check('G5a', 'Pfad zu einem Knoten wird angezeigt, laesst sich durchschalten und mit Esc verlassen',
        pathResult.opened && /Path to .* · \d+ hops?/.test(pathResult.heading ?? '') && (pathResult.positions?.length ?? 0) > 0 && pathResult.clearedByEscape, pathResult, pathImages);

    // G5: Aufrufreihenfolge fuer eine Funktion mit Aufrufen.
    const orderImages = [];
    await select(page, 'call_command');
    await scopeSettled(page);
    const order = page.getByRole('button', { name: 'Call order' });
    let orderResult = { available: false };
    if (await order.count() && await order.isEnabled().catch(() => false)) {
        await order.click();
        await wait(1200);
        orderImages.push(await shot(page, 'aufrufreihenfolge'));
        const panel = page.locator('[data-testid="atlas-galaxy-path-panel"]');
        const text = await panel.innerText().catch(() => '');
        const lines = [...text.matchAll(/line (\d+)/g)].map((m) => Number(m[1]));
        orderResult = { available: true, heading: text.split('\n')[0], lines, sorted: lines.every((v, i) => i === 0 || v >= lines[i - 1]) };
        await page.keyboard.press('Escape');
    }
    check('G5b', 'Aufrufreihenfolge der Wurzel in Zeilenreihenfolge', orderResult.available && (orderResult.lines?.length ?? 0) > 1 && orderResult.sorted, orderResult, orderImages);
}

/* ------------------------------------------------------------------ */
/* Chat                                                                 */

async function chatChecks(page) {
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);

    // C6: Roboter und Lampe in der Kopfzeile, Klick oeffnet die Konfiguration.
    const action = page.locator('.atlas-browser-ai-action').first();
    const header = await action.boundingBox().catch(() => null);
    const headerInfo = await action.evaluate((el) => ({ svg: Boolean(el.querySelector('svg')), text: el.textContent?.trim() ?? '', title: el.getAttribute('title') ?? '', label: el.getAttribute('aria-label') ?? '' })).catch(() => null);
    const headerImage = header ? `${String(++shotIndex).padStart(3, '0')}-kopfzeile.png` : null;
    if (header) await page.screenshot({ path: join(OUT, 'bilder', headerImage), clip: { x: Math.max(0, header.x - 300), y: Math.max(0, header.y - 10), width: Math.min(VIEWPORT.width - Math.max(0, header.x - 300), header.width + 360), height: header.height + 20 } });
    await action.click();
    await wait(700);
    const dialogOpen = await page.getByRole('dialog').filter({ hasText: 'Agent configuration' }).isVisible().catch(() => false);
    const configImage = await shot(page, 'agent-konfiguration');
    check('C6', 'Kopfzeile zeigt Roboter und Lampe, Modellname im Tooltip, Klick oeffnet die Konfiguration',
        Boolean(headerInfo?.svg) && /Qwen2\.5 Coder 0\.5B/.test(`${headerInfo?.title} ${headerInfo?.label}`) && !/Qwen2\.5/.test(headerInfo?.text ?? '') && dialogOpen,
        { ...headerInfo, dialogOpen }, [headerImage, configImage].filter(Boolean));

    // C5: Token-Grenzen einstellen, nach dem Neuladen noch da.
    const hasFields = (await page.locator('#cbm-chat-input-tokens').count()) > 0 && (await page.locator('#cbm-chat-output-tokens').count()) > 0;
    let persisted = null;
    if (hasFields) {
        await page.locator('#cbm-chat-output-tokens').fill('384');
        await page.locator('#cbm-chat-output-tokens').press('Tab');
        await wait(400);
        await page.reload({ waitUntil: 'domcontentloaded' });
        await page.waitForSelector('.atlas-shell');
        await galaxyReady(page);
        await page.locator('.atlas-browser-ai-action').first().click();
        await wait(700);
        persisted = { set: '384', afterReload: await page.locator('#cbm-chat-output-tokens').inputValue().catch(() => null), input: await page.locator('#cbm-chat-input-tokens').inputValue().catch(() => null) };
    }
    check('C5', 'Eingabe- und Ausgabe-Token einstellbar und nach dem Neuladen gespeichert', hasFields && persisted?.afterReload === '384', persisted, [await shot(page, 'token-grenzen')]);
    if (hasFields) { await page.locator('#cbm-chat-output-tokens').fill('512'); await page.locator('#cbm-chat-output-tokens').press('Tab'); await wait(300); }

    // Modell in DIESER Seite laden; ab hier wird nicht mehr neu geladen.
    let modelReady = false;
    if (MODEL) {
        const load = page.getByRole('button', { name: /Download & load|Load model|Reload model/ });
        if (await load.count()) await load.first().click();
        modelReady = await page.waitForFunction(() => /Agent active/.test(`${document.querySelector('.atlas-browser-ai-action')?.getAttribute('aria-label') ?? ''}`), null, { timeout: 900000 }).then(() => true).catch(() => false);
    }
    await page.keyboard.press('Escape').catch(() => {});
    await wait(600);
    await select(page, 'JSONBAgg');
    await scopeSettled(page);
    const toggle = page.getByRole('button', { name: 'Open chat' });
    if (await toggle.count()) { await toggle.click(); await wait(1200); }

    // C3: Selection details liegt nicht auf der Eingabe, und ueber Senden liegt nichts.
    const prompt = page.locator('#cbm-chat-prompt');
    const composer = await page.locator('.cbm-chat-input-row').first().boundingBox().catch(() => null);
    const details = await page.locator('.atlas-galaxy-selection-details').first().boundingBox().catch(() => null);
    await prompt.fill('x');
    await wait(200);
    const sendBox = await page.locator('.cbm-chat-send').first().boundingBox().catch(() => null);
    const topAtSend = sendBox ? await page.evaluate(({ x, y }) => {
        const el = document.elementFromPoint(x, y);
        return { tag: el?.tagName ?? '', cls: String(el?.className ?? ''), insideSend: Boolean(el?.closest('.cbm-chat-send')), insideDetails: Boolean(el?.closest('.atlas-galaxy-selection-details')) };
    }, { x: sendBox.x + sendBox.width / 2, y: sendBox.y + sendBox.height / 2 }) : null;
    const sendClickable = modelReady ? await page.getByRole('button', { name: 'Send ↑' }).click({ trial: true, timeout: 3000 }).then(() => true).catch((error) => String(error.message).split('\n')[0]) : 'model not loaded';
    await prompt.fill('');
    check('C3', 'Selection details ueberlagert die Chat-Eingabe nicht, ueber Senden liegt nichts, Senden per Maus klickbar',
        composer !== null && !intersects(composer, details) && topAtSend?.insideSend === true && sendClickable === true,
        { composer, details, topAtSend, sendClickable }, [await shot(page, 'chat-offen')]);
    if (!modelReady) { check('C-Modell', 'Lokales Modell geladen', false, 'Modell nicht innerhalb von 15 min aktiv'); return; }

    // C1 (seit K7/K14): Die Fakten stehen fest gelistet in der Karte, das Modell bekommt nur den
    // Quelltext und schreibt einen Satz. Geprueft: keine internen Schluessel im Prompt, Quelltext im
    // Prompt, Fakten aus dem Index in der Karte.
    await page.waitForFunction(() => {
        const section = document.querySelector('.cbm-chat-explanation');
        return section && section.querySelector('.cbm-chat-retry') && !/Explaining selection|Preparing|Waiting for the complete scope/.test(section.textContent ?? '');
    }, null, { timeout: 240000 }).catch(() => {});
    const explanationImage = await shot(page, 'erklaerung');
    const sent = await page.evaluate(() => window.__probeWorker.slice());
    const autoPrompt = sent.filter((entry) => entry.kind === 'chat').map((entry) => entry.messages.map((m) => m.content).join('\n')).pop() ?? '';
    const forbidden = ['Snapshot.', 'renderedNodes', 'renderedEdges', 'Snapshot.generation', '$.relationships', 'Upstream omissions', 'Selected.roots', '[graph-'].filter((key) => autoPrompt.includes(key));
    const truth = await incoming('JSONBAgg');
    const callers = [...new Set(truth.filter((row) => row.type === 'CALLS').map((row) => row.source))];
    const byType = truth.reduce((acc, row) => { acc[row.type] = (acc[row.type] ?? 0) + 1; return acc; }, {});
    const card = await page.locator('.cbm-chat-explanation').innerText().catch(() => '');
    const factsInCard = Object.entries(byType).every(([type, count]) => card.includes(`${type} ${count}`)) && card.includes(`Incoming: ${truth.length} relationship`);
    const sourceInPrompt = autoPrompt.includes('class JSONBAgg(');
    check('C1', 'Erklaerung: Fakten aus dem Index fest in der Karte, Prompt mit Quelltext und ohne interne Feldnamen', autoPrompt.length > 0 && forbidden.length === 0 && sourceInPrompt && factsInCard,
        { forbiddenFound: forbidden, sourceInPrompt, factsInCard, truthByType: byType, callers: callers.length, cardStart: card.slice(0, 400), promptStart: autoPrompt.slice(0, 300) }, [explanationImage]);
    await writeFile(join(OUT, 'prompt-erklaerung.txt'), autoPrompt);

    // C2: "Who calls JSONBAgg?" nennt alle Aufrufer, ohne Wiederholung.
    await prompt.click();
    await prompt.fill('Who calls JSONBAgg?');
    const v4 = since();
    await page.getByRole('button', { name: 'Send ↑' }).click();
    await wait(1500);
    await page.waitForFunction(() => !document.querySelector('.cbm-chat-send[aria-label="Stop"]'), null, { timeout: 300000 }).catch(() => {});
    await wait(1000);
    mark('frage-wer-ruft', v4, since());
    const answerImage = await shot(page, 'antwort-wer-ruft');
    const transcript = await page.locator('.cbm-chat-transcript').innerText().catch(() => '');
    const answer = transcript.slice(transcript.lastIndexOf('Who calls JSONBAgg?'));
    const missing = callers.filter((name) => !answer.includes(name));
    const lines = answer.split('\n').map((line) => line.trim()).filter(Boolean);
    const repeated = Object.entries(lines.reduce((acc, line) => { acc[line] = (acc[line] ?? 0) + 1; return acc; }, {})).filter(([line, count]) => count >= 3 && line.length > 8);
    check('C2', 'Antwort auf "Who calls JSONBAgg?" nennt alle Aufrufer aus dem Index ohne Wiederholungen', callers.length > 0 && missing.length === 0 && repeated.length === 0,
        { callers: callers.length, missing, repeated: repeated.slice(0, 3), answerStart: answer.slice(0, 900) }, [answerImage]);
    await writeFile(join(OUT, 'antwort-wer-ruft.txt'), answer);

    // C4: zurueck zur selben Auswahl, die Erklaerung kommt aus dem Cache.
    const firstExplanation = await page.locator('.cbm-chat-explanation').innerText().catch(() => '');
    await select(page, 'BaseCommand');
    await scopeSettled(page);
    await page.waitForFunction(() => { const section = document.querySelector('.cbm-chat-explanation'); return !section || !/Explaining selection/.test(section.textContent ?? ''); }, null, { timeout: 240000 }).catch(() => {});
    const mark0 = await page.evaluate(() => window.__probeWorker.length);
    await select(page, 'JSONBAgg');
    await scopeSettled(page);
    const back1 = await page.locator('.cbm-chat-explanation').innerText().catch(() => '');
    const cacheImages = [await shot(page, 'zurueck-zu-jsonbagg')];
    await wait(4000);
    cacheImages.push(await shot(page, 'zurueck-zu-jsonbagg-4s'));
    const back2 = await page.locator('.cbm-chat-explanation').innerText().catch(() => '');
    const newRuns = await page.evaluate((from) => window.__probeWorker.slice(from).filter((entry) => entry.kind === 'chat').length, mark0);
    check('C4', 'Rueckkehr zur selben Auswahl erzeugt keine neue Erklaerung',
        newRuns === 0 && !/Explaining selection/.test(back1 + back2) && back2.replace(/Explain again/g, '').trim().length > 20,
        { newModelRuns: newRuns, explainingShown: /Explaining selection/.test(back1 + back2), sameText: back2.trim() === firstExplanation.trim() }, cacheImages);
}

/* ------------------------------------------------------------------ */
/* Architektur                                                          */

async function labelOverlaps(page) {
    return page.evaluate(() => {
        const boxes = [...document.querySelectorAll('button.architecture-node-label')]
            .filter((el) => !el.classList.contains('is-hidden') && el.offsetParent !== null)
            .map((el) => ({ r: el.getBoundingClientRect(), text: el.textContent?.trim() ?? '' }))
            .filter((item) => item.r.width > 0 && item.r.height > 0);
        let overlaps = 0;
        for (let i = 0; i < boxes.length; i += 1) for (let j = i + 1; j < boxes.length; j += 1) {
            // Gemessen am Textbereich, ohne den Innenabstand des Etiketts: so entscheidet auch die Szene.
            const a = boxes[i].r; const b = boxes[j].r; const ix = 6; const iy = 4;
            if (!(a.left + ix >= b.right - ix || b.left + ix >= a.right - ix || a.top + iy >= b.bottom - iy || b.top + iy >= a.bottom - iy)) overlaps += 1;
        }
        return { labels: boxes.length, overlaps, sample: boxes.slice(0, 12).map((item) => item.text) };
    });
}

async function architectureChecks(page) {
    const view = (name) => page.locator(`button.atlas-arch-tab[data-view="${name}"]`).first();
    await open(page, PROJECT, 'architecture');
    await view('overview').click();
    await page.waitForFunction(() => document.querySelectorAll('button.architecture-node-label').length > 2, null, { timeout: 30000 }).catch(() => {});
    await wait(2500);
    const rootImage = await shot(page, 'overview-wurzel');
    const rootLabels = await labelOverlaps(page);
    const area = page.locator('button.architecture-node-label', { hasText: /^\W*django\s*$/ }).first();
    let inside = null;
    const images = [rootImage];
    if (await area.count()) {
        await area.click();
        await wait(600);
        const openArea = page.getByRole('button', { name: /Open area/ }).first();
        const v5 = since();
        if (await openArea.count()) await openArea.click(); else await area.dblclick();
        await wait(250); images.push(await shot(page, 'overview-django-250ms'));
        await wait(2500); images.push(await shot(page, 'overview-django-fertig'));
        mark('overview-django', v5, since());
        inside = await labelOverlaps(page);
        inside.fullPaths = inside.sample.filter((label) => label.startsWith('django/')).length;
    }
    check('A1', 'In "django" lesbare Bereiche ohne volle Pfade und ohne gestapelte Beschriftungen', inside !== null && inside.labels > 3 && inside.overlaps <= 2 && inside.fullPaths === 0,
        { root: rootLabels, inside }, images);

    await view('structure').click();
    await page.waitForFunction(() => /\d+ groups? ·/.test(document.body.textContent ?? ''), null, { timeout: 30000 }).catch(() => {});
    await wait(2500);
    const structureImage = await shot(page, 'system-structure');
    const structureText = await page.locator('body').innerText();
    const groups = Number(structureText.match(/(\d[\d.,]*) groups? ·/)?.[1]?.replace(/[.,]/g, '') ?? '0');
    const backend = await tool('get_architecture', { project: PROJECT, aspects: ['system_structure'], format: 'json' });
    const result = backend?.result ?? backend;
    check('A2', 'System structure fuer Django zeigt Gruppen (oder den echten Grund)',
        groups > 0 || /exceeded its memory budget|limited/i.test(structureText),
        { groupsShown: groups, backendStatus: result?.status, components: result?.totals?.components, entrypoints: result?.totals?.entrypoints, warnings: (result?.warnings ?? []).slice(-1) }, [structureImage]);

    await view('behavior').click();
    await wait(6000);
    const start = page.getByLabel('Behavior entry point').or(page.locator('select').first()).first();
    const options = await start.locator('option').allTextContents().catch(() => []);
    const behaviorImages = [await shot(page, 'behavior-startliste')];
    const realOptions = options.filter((option) => !/Choose an operation/.test(option));
    let calls = null;
    if (realOptions.length) {
        await start.selectOption({ index: Math.min(1, options.length - 1) }).catch(() => {});
        await wait(5000);
        behaviorImages.push(await shot(page, 'behavior-ausgewaehlt'));
        calls = (await page.locator('body').innerText()).match(/(\d+) direct callees?/)?.[0] ?? null;
    }
    check('A3', 'Behavior bietet fuer Django Einstiegspunkte an', realOptions.length > 0, { entries: realOptions.length, sample: realOptions.slice(0, 8), afterSelect: calls }, behaviorImages);

    await view('routes').click();
    await wait(2500);
    const endpoints = page.getByRole('button', { name: 'Endpoints', exact: true });
    if (await endpoints.count()) { await endpoints.click(); await wait(1000); }
    const checkingText = await page.locator('label.spatial-gravity-toggle', { hasText: /Include test routes/ }).innerText().catch(() => '');
    await page.waitForFunction(() => !/Reading indexed endpoint connections/.test(document.querySelector('.spatial-architecture')?.textContent ?? ''), null, { timeout: 60000 }).catch(() => {});
    await wait(1500);
    const routesImage = await shot(page, 'routes-endpoints');
    const toggleText = await page.locator('label.spatial-gravity-toggle', { hasText: /Include test routes/ }).innerText().catch(() => '');
    const routeLabels = await labelOverlaps(page);
    check('A4', 'Endpoints gruppiert, Test-Routen standardmaessig ausgeblendet, Beschriftungen ohne Stapel',
        /Include test routes \(\d[\d.,]* hidden\)/.test(toggleText) && !/\(1 hidden\)/.test(toggleText) && /checking|hidden/.test(checkingText) && routeLabels.overlaps <= 3,
        { whileLoading: checkingText, toggle: toggleText, labels: routeLabels.labels, overlaps: routeLabels.overlaps, sample: routeLabels.sample }, [routesImage]);

    // Gegenprobe: dieses Repository behaelt seine Ansichten.
    await open(page, CONTROL, 'architecture');
    await wait(3500);
    await view('behavior').click();
    await wait(6000);
    const controlOptions = await page.getByLabel('Behavior entry point').or(page.locator('select').first()).first().locator('option').allTextContents().catch(() => []);
    check('A-Gegenprobe', 'Gegenprobe: Behavior im eigenen Repository hat weiterhin Einstiegspunkte', controlOptions.length > 10, { entries: controlOptions.length }, [await shot(page, 'gegenprobe-behavior')]);
}

/* ------------------------------------------------------------------ */
/* Bildstreifen aus dem Video                                           */

async function strips(videoPath) {
    if (!FFMPEG || !videoPath) return [];
    const made = [];
    for (const item of marks) {
        const file = join(OUT, 'streifen', `${item.label}.png`);
        const duration = Math.max(1, Math.min(12, item.to - item.from + 0.5));
        // Sechs Bilder pro Sekunde, sechs je Zeile: eine Zeile pro Sekunde.
        const rows = Math.ceil(duration);
        try {
            await run(FFMPEG, ['-y', '-loglevel', 'error', '-ss', String(Math.max(0, item.from - 0.3)), '-t', String(duration), '-i', videoPath,
                '-vf', `fps=6,scale=400:-1,tile=6x${rows}:padding=4:color=black`, '-frames:v', '1', file]);
            made.push(file);
        } catch (error) {
            console.error(`[streifen] ${item.label}: ${String(error?.stderr ?? error).split('\n')[0]}`);
        }
    }
    return made;
}

/* ------------------------------------------------------------------ */

await mkdir(join(OUT, 'bilder'), { recursive: true });
await mkdir(join(OUT, 'streifen'), { recursive: true });
await mkdir(join(OUT, 'video'), { recursive: true });
const context = await chromium.launchPersistentContext(PROFILE, {
    headless: !HEADED, viewport: VIEWPORT, deviceScaleFactor: 2,
    args: ['--enable-unsafe-webgpu', '--ignore-gpu-blocklist', '--use-angle=metal', '--enable-gpu'],
    recordVideo: { dir: join(OUT, 'video'), size: VIEWPORT },
});
await context.addInitScript(pageProbe);
const page = context.pages()[0] ?? await context.newPage();
const pageErrors = [];
page.on('pageerror', (error) => { pageErrors.push(error.message); console.error(`[pageerror] ${error.message}`); });
const failures = [];
const section = async (name, fn) => {
    try { await fn(); } catch (error) { failures.push({ name, error: String(error?.stack ?? error) }); console.error(`[${name}] ABBRUCH ${error?.message ?? error}`); }
};
if (ONLY.includes('galaxy')) await section('galaxy', () => galaxyChecks(page));
if (ONLY.includes('chat')) await section('chat', () => chatChecks(page));
if (ONLY.includes('architecture')) await section('architecture', () => architectureChecks(page));
check('X1', 'Keine Seitenfehler im gesamten Lauf', pageErrors.length === 0, { pageErrors });
const video = page.video();
await context.close();
let videoPath = null;
try {
    const raw = await video?.path();
    if (raw) { videoPath = join(OUT, 'video', 'lauf.webm'); await rename(raw, videoPath); }
} catch { videoPath = (await readdir(join(OUT, 'video'))).map((name) => join(OUT, 'video', name)).find((name) => name.endsWith('.webm')) ?? null; }
const made = await strips(videoPath);
const passed = checks.filter((item) => item.pass).length;
await writeFile(join(OUT, 'report.json'), JSON.stringify({ origin: ORIGIN, project: PROJECT, control: CONTROL, passed, total: checks.length, checks, failures, marks, strips: made, video: videoPath }, null, 2));
const lines = ['# Abnahme Call-Feedback', '', `${passed} von ${checks.length} Pruefungen bestanden.`, '', '| Punkt | Pruefung | Ergebnis | Messwert |', '|---|---|---|---|'];
for (const item of checks) lines.push(`| ${item.id} | ${item.title} | ${item.pass ? 'bestanden' : 'NICHT bestanden'} | ${JSON.stringify(item.measured).slice(0, 240).replace(/\|/g, '/')} |`);
if (failures.length) lines.push('', '## Abbrueche', '', ...failures.map((item) => `- ${item.name}: ${item.error.split('\n')[0]}`));
await writeFile(join(OUT, 'report.md'), `${lines.join('\n')}\n`);
console.log(`${passed}/${checks.length} bestanden`);
if (passed !== checks.length || failures.length) process.exitCode = 1;
