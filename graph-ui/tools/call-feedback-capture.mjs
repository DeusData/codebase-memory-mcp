#!/usr/bin/env node
/*
 * Screenshot-Serien zum Feedback aus dem Review-Call vom 02.10.2026.
 *
 *   node tools/call-feedback-capture.mjs --origin http://127.0.0.1:4350 \
 *        --out verification/call-feedback-2026-10-02/ist --profile /tmp/profile \
 *        [--project django-demo] [--control cbm] [--scenarios galaxy,chat,architecture] [--headless]
 *
 * Ein Video kann der Pruefer nicht ansehen. Darum zerlegt dieses Skript jede
 * Bewegung in einzelne Bilder: vor der Aktion, beim Hovern, direkt nach dem
 * Klick und dann nach 250 ms, 1 s und 3 s, jeder Mausrad-Schritt einzeln. Ein
 * eingeblendeter Ring zeigt, wo der Mauszeiger steht, denn Screenshots zeigen
 * ihn nicht. Zu jedem Bild schreibt das Skript den gemessenen Zustand mit:
 * Kamera, ob die Zeichenflaeche dieselbe geblieben ist, die Zaehler der
 * Galaxie und die Statuszeile. Ohne diese Zahlen waere ein Zuruecksetzen
 * zwischen zwei Bildern unsichtbar.
 *
 * Es startet keinen Server. Es braucht einen laufenden Server mit UI, dessen
 * Index die Projekte enthaelt, und ein eigenes Browserprofil, damit ein
 * heruntergeladenes Modell beim naechsten Lauf wieder da ist.
 */

import { chromium } from 'playwright';
import { mkdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const argv = process.argv.slice(2);
const arg = (name, fallback) => {
    const index = argv.indexOf(`--${name}`);
    if (index < 0) return fallback;
    const value = argv[index + 1];
    return value === undefined || value.startsWith('--') ? true : value;
};

const ORIGIN = String(arg('origin', 'http://127.0.0.1:4350')).replace(/\/$/, '');
const OUT = resolve(String(arg('out', 'verification/call-feedback-2026-10-02/ist')));
const PROFILE = resolve(String(arg('profile', join(OUT, '..', 'profile'))));
const PROJECT = String(arg('project', 'django-demo'));
const CONTROL = String(arg('control', 'cbm'));
const SCENARIOS = String(arg('scenarios', 'galaxy,chat,architecture,header,system')).split(',');
const HEADLESS = arg('headless', false) === true;
const CHAT_MODEL = arg('chat-model', true) !== 'false';

const VIEWPORT = { width: 1600, height: 1000 };

/* ------------------------------------------------------------------ */
/* Server-Abfragen, damit die Bilder gegen echte Kanten gehalten werden */

let rpcId = 1;
async function tool(name, args) {
    const res = await fetch(`${ORIGIN}/rpc`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: rpcId++, method: 'tools/call', params: { name, arguments: args } }),
    });
    const body = await res.json();
    const text = body?.result?.content?.[0]?.text ?? '';
    try { return JSON.parse(text); } catch { return { raw: text, error: body?.error }; }
}

async function cypher(project, query) {
    return tool('query_graph', { project, query });
}

/* ------------------------------------------------------------------ */
/* Was in jede Seite eingesetzt wird                                   */

function pageProbe() {
    try { localStorage.setItem('cbm.workspace.setup', 'done'); } catch { /* ohne Speicher geht es auch */ }

    // Der sichtbare Mauszeiger.
    const mountCursor = () => {
        if (document.getElementById('probe-cursor')) return;
        const dot = document.createElement('div');
        dot.id = 'probe-cursor';
        dot.style.cssText = 'position:fixed;left:-100px;top:-100px;width:22px;height:22px;margin:-11px 0 0 -11px;'
            + 'border:2px solid #ff3b6b;border-radius:50%;box-shadow:0 0 0 2px rgba(0,0,0,.6);pointer-events:none;z-index:2147483647;';
        const cross = document.createElement('div');
        cross.style.cssText = 'position:absolute;left:9px;top:9px;width:4px;height:4px;background:#ff3b6b;border-radius:50%;';
        dot.appendChild(cross);
        document.documentElement.appendChild(dot);
        document.addEventListener('mousemove', (event) => {
            dot.style.left = `${event.clientX}px`;
            dot.style.top = `${event.clientY}px`;
        }, true);
    };
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mountCursor);
    else mountCursor();

    // Welche Zeichenflaeche gerade steht: eine neu aufgebaute bekommt eine neue Nummer.
    window.__probeCanvasSeq = 0;
    window.__probeCanvasId = () => {
        const canvas = document.querySelector('.atlas-galaxy canvas') ?? document.querySelector('canvas');
        if (!canvas) return null;
        if (!canvas.dataset.probeId) canvas.dataset.probeId = String(++window.__probeCanvasSeq);
        return canvas.dataset.probeId;
    };

    // Was zwischen Seite und Modell-Worker hin und her geht: der echte Prompt.
    window.__probeWorker = [];
    const keep = (entry) => {
        window.__probeWorker.push(entry);
        if (window.__probeWorker.length > 4000) window.__probeWorker.splice(0, 1000);
    };
    const safe = (value) => {
        try { return JSON.parse(JSON.stringify(value, (key, item) => (typeof item === 'string' && item.length > 20000 ? `${item.slice(0, 20000)}...` : item))); }
        catch { return String(value); }
    };
    const NativeWorker = window.Worker;
    window.Worker = class ProbeWorker extends NativeWorker {
        constructor(url, options) {
            super(url, options);
            this.__probeSrc = String(url);
            if (this.__probeSrc.includes('browser-ai')) {
                this.addEventListener('message', (event) => keep({ dir: 'from-worker', t: performance.now(), data: safe(event.data) }));
            }
        }
        postMessage(message, transfer) {
            if (this.__probeSrc.includes('browser-ai')) keep({ dir: 'to-worker', t: performance.now(), data: safe(message) });
            return super.postMessage(message, transfer);
        }
    };

    // Ein Zeitstrahl der Kamera, alle 100 ms, unabhaengig von den Bildern.
    window.__probeTimeline = [];
    setInterval(() => {
        const fit = globalThis.__atlasGalaxyFit?.measure?.();
        const galaxy = globalThis.__atlasGalaxy;
        window.__probeTimeline.push({
            t: Math.round(performance.now()),
            canvas: window.__probeCanvasId?.() ?? null,
            camera: fit ? fit.camera.position.map((v) => Number(v.toFixed(2))) : null,
            direction: fit ? fit.camera.direction.map((v) => Number(v.toFixed(3))) : null,
            nodes: galaxy?.nodes ?? null,
            fits: galaxy?.fits ?? null,
            targets: galaxy?.targetChanges ?? null,
        });
        if (window.__probeTimeline.length > 6000) window.__probeTimeline.splice(0, 2000);
    }, 100);
}

/* ------------------------------------------------------------------ */
/* Bilder und Zustand                                                  */

class Series {
    constructor(page, name, title) {
        this.page = page;
        this.dir = join(OUT, name);
        this.name = name;
        this.title = title;
        this.entries = [];
        this.n = 0;
        this.notes = [];
    }

    async state() {
        return this.page.evaluate(() => {
            const fit = globalThis.__atlasGalaxyFit?.measure?.();
            const galaxy = globalThis.__atlasGalaxy;
            const status = [...document.querySelectorAll('.atlas-galaxy-toolbar, .atlas-galaxy-scope-status, [role="status"]')]
                .map((el) => el.textContent?.trim()).filter(Boolean).slice(0, 6);
            return {
                url: location.pathname + location.search,
                canvas: window.__probeCanvasId?.() ?? null,
                canvases: document.querySelectorAll('canvas').length,
                galaxy: galaxy ? {
                    nodes: galaxy.nodes, fits: galaxy.fits, targetChanges: galaxy.targetChanges,
                    highlighted: galaxy.highlightedCount, lastTargetQn: galaxy.lastTargetQn, mode: galaxy.mode,
                    headline: galaxy.headline, drawnEdges: galaxy.drawnEdges, edgeNote: galaxy.edgeNote,
                    lastFit: galaxy.lastFit,
                } : null,
                fit: fit ? {
                    nodes: fit.nodes, inside: fit.inside, outside: fit.outside, behind: fit.behind,
                    marginPx: fit.marginPx, box: fit.box, fill: fit.fill, camera: fit.camera, worst: fit.worst,
                } : null,
                status,
            };
        });
    }

    async shot(label, note, options = {}) {
        this.n += 1;
        const file = `${String(this.n).padStart(2, '0')}-${label}.png`;
        await mkdir(this.dir, { recursive: true });
        await this.page.screenshot({ path: join(this.dir, file), fullPage: false, ...(options.clip ? { clip: options.clip } : {}) });
        const state = await this.state().catch((error) => ({ error: String(error) }));
        this.entries.push({ file, note, at: Date.now(), state, ...(options.extra ? { extra: options.extra } : {}) });
        console.log(`[${this.name}] ${file}  ${note}`);
        return state;
    }

    note(text) {
        this.notes.push(text);
        console.log(`[${this.name}] note: ${text}`);
    }

    async save(extra = {}) {
        await mkdir(this.dir, { recursive: true });
        const timeline = await this.page.evaluate(() => window.__probeTimeline ?? []).catch(() => []);
        await writeFile(join(this.dir, 'state.json'), JSON.stringify({ title: this.title, entries: this.entries, notes: this.notes, timeline, ...extra }, null, 2));
        const lines = [`# ${this.title}`, '', '| Bild | Was es zeigt | Zeichenflaeche | Kamera |', '|---|---|---|---|'];
        for (const entry of this.entries) {
            const cam = entry.state?.fit?.camera?.position?.map((v) => v.toFixed(0)).join(', ') ?? '';
            lines.push(`| ${entry.file} | ${entry.note} | ${entry.state?.canvas ?? ''} | ${cam} |`);
        }
        if (this.notes.length) lines.push('', '## Notizen', '', ...this.notes.map((note) => `- ${note}`));
        await writeFile(join(this.dir, 'index.md'), `${lines.join('\n')}\n`);
    }
}

const wait = (ms) => new Promise((done) => setTimeout(done, ms));

/** Nach einer Aktion: sofort, 250 ms, 1 s, 3 s. */
async function settle(series, label, note) {
    await series.shot(`${label}-0ms`, `${note} (sofort)`);
    await wait(250);
    await series.shot(`${label}-250ms`, `${note} (nach 250 ms)`);
    await wait(750);
    await series.shot(`${label}-1s`, `${note} (nach 1 s)`);
    await wait(2000);
    await series.shot(`${label}-3s`, `${note} (nach 3 s)`);
}

async function open(page, project, workspace) {
    await page.goto(`${ORIGIN}/?project=${encodeURIComponent(project)}&workspace=${workspace}`, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.atlas-shell', { timeout: 30000 });
    await wait(1500);
}

async function galaxyReady(page, timeout = 60000) {
    await page.waitForFunction(() => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0 && globalThis.__atlasGalaxyFit !== undefined, null, { timeout });
    await wait(1500);
}

async function canvasCenter(page) {
    const box = await page.locator('.atlas-galaxy canvas').first().boundingBox();
    if (!box) throw new Error('keine Galaxie-Zeichenflaeche');
    return { box, cx: box.x + box.width / 2, cy: box.y + box.height / 2 };
}

async function searchAndSelect(page, name) {
    const input = page.getByRole('searchbox', { name: 'Find a graph node' }).first();
    await input.click();
    await input.fill('');
    await input.type(name, { delay: 30 });
    const choice = page.locator('.atlas-galaxy-search-results button', { has: page.locator('strong', { hasText: new RegExp(`^${name}$`) }) }).first();
    await choice.waitFor({ timeout: 20000 });
    return choice;
}

async function selectWithLayer(page, name) {
    const choice = await searchAndSelect(page, name);
    await choice.click();
    await wait(3000);
    const expand = page.getByRole('button', { name: 'Expand +1' });
    const text = await page.locator('.atlas-galaxy').innerText().catch(() => '');
    if (/\b0 layers\b/.test(text) && await expand.isEnabled().catch(() => false)) {
        await expand.click();
        await wait(4000);
    }
}

/* ------------------------------------------------------------------ */
/* Szenarien                                                           */

async function galaxySelection(page, symbol, prefix, title) {
    const series = new Series(page, `${prefix}-${symbol.toLowerCase()}`, title);
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    await series.shot('ganzer-graph', 'Galaxie, ganzer Graph, nichts ausgewaehlt');
    const choice = await searchAndSelect(page, symbol);
    await series.shot('suche', `Suche nach ${symbol}, Trefferliste offen`);
    await choice.hover();
    await choice.click();
    await settle(series, 'auswahl', `${symbol} ausgewaehlt`);
    await wait(3000);
    await series.shot('auswahl-6s', `${symbol} ausgewaehlt (nach 6 s, ohne Eingabe)`);

    const expand = page.getByRole('button', { name: 'Expand +1' });
    if (await expand.isEnabled().catch(() => false)) {
        await expand.hover();
        await series.shot('expand-hover', 'Maus auf Expand +1');
        await expand.click();
        await settle(series, 'expand1', 'Expand +1 geklickt');
        if (await expand.isEnabled().catch(() => false)) {
            await expand.click();
            await settle(series, 'expand2', 'Expand +1 ein zweites Mal');
        }
    } else series.note('Expand +1 war nicht klickbar');
    await series.save();
    return series;
}

async function galaxyTraceIncoming(page) {
    const series = new Series(page, 'g3-basecommand-incoming', 'BaseCommand, Trace Incoming, nur CALLS, 2 Ebenen');
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    const choice = await searchAndSelect(page, 'BaseCommand');
    await choice.click();
    await wait(2500);
    await series.shot('auswahl', 'BaseCommand ausgewaehlt');
    await page.getByLabel('Trace direction').selectOption('inbound');
    await settle(series, 'incoming', 'Richtung Incoming');
    const filter = page.locator('details.atlas-trace-edge-filter').first();
    await filter.locator('summary').click();
    await wait(400);
    await series.shot('kantentypen-offen', 'Edge-types-Liste offen');
    const only = page.getByRole('button', { name: 'Only CALLS', exact: true });
    if (await only.count()) {
        await only.first().click();
        await settle(series, 'nur-calls', 'Nur CALLS');
    } else series.note('Kein Only-Knopf fuer CALLS gefunden');
    await filter.locator('summary').click().catch(() => {});
    const expand = page.getByRole('button', { name: 'Expand +1' });
    if (await expand.isEnabled().catch(() => false)) {
        await expand.click();
        await settle(series, 'expand1', 'Expand +1 mit nur CALLS, Incoming');
    }
    await series.save();
}

async function galaxyZoom(page, whole = false) {
    const series = new Series(page, whole ? 'g5-zoom-ganzer-graph' : 'g4-zoom',
        whole ? 'Mausrad-Zoom an zwei Cursorpositionen, ganzer Graph' : 'Mausrad-Zoom an zwei Cursorpositionen, JSONBAgg mit einer Ebene');
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    if (!whole) {
        const choice = await searchAndSelect(page, 'JSONBAgg');
        await choice.click();
        await wait(3000);
        const expand = page.getByRole('button', { name: 'Expand +1' });
        const layers = await page.locator('.atlas-galaxy').innerText().catch(() => '');
        if (/\b0 layers\b/.test(layers) && await expand.isEnabled().catch(() => false)) await expand.click();
        await wait(5000);
    } else {
        await wait(2000);
    }
    const { box, cx, cy } = await canvasCenter(page);
    const spots = [
        { label: 'rechts-oben', x: Math.min(box.x + box.width - 80, cx + box.width * 0.28), y: Math.max(box.y + 80, cy - box.height * 0.25) },
        { label: 'links-unten', x: Math.max(box.x + 80, cx - box.width * 0.3), y: Math.min(box.y + box.height - 80, cy + box.height * 0.28) },
    ];
    const measures = [];
    for (const spot of spots) {
        await page.getByRole('button', { name: 'fit view' }).click().catch(() => {});
        await wait(1500);
        await page.mouse.move(spot.x, spot.y, { steps: 8 });
        await wait(300);
        const before = await series.shot(`${spot.label}-start`, `Cursor ${spot.label} (${Math.round(spot.x)}, ${Math.round(spot.y)}), vor dem Zoomen`);
        measures.push({ spot: spot.label, step: 0, camera: before?.fit?.camera });
        for (let step = 1; step <= 5; step += 1) {
            await page.mouse.wheel(0, -240);
            await wait(700);
            const after = await series.shot(`${spot.label}-zoom${step}`, `Cursor ${spot.label}, Mausrad-Schritt ${step} hinein`);
            measures.push({ spot: spot.label, step, camera: after?.fit?.camera });
        }
    }
    // Wohin die Kamera beim Zoomen wandert: entlang der Blickrichtung (Zoom zur Mitte)
    // oder seitlich weg (Zoom zum Cursor).
    const analysis = [];
    for (const spot of spots) {
        const rows = measures.filter((row) => row.spot === spot.label && row.camera);
        if (rows.length < 2) continue;
        const a = rows[0].camera;
        const b = rows[rows.length - 1].camera;
        const delta = a.position.map((v, i) => b.position[i] - v);
        const length = Math.hypot(...delta) || 1;
        const along = Math.abs(delta.reduce((sum, v, i) => sum + v * a.direction[i], 0)) / length;
        analysis.push({ spot: spot.label, moved: Number(length.toFixed(2)), alongViewDirection: Number(along.toFixed(4)) });
    }
    series.note(`Kamerabewegung beim Zoomen (1.0 = genau entlang der Blickrichtung, also Zoom zur Bildmitte): ${JSON.stringify(analysis)}`);
    await series.save({ zoomAnalysis: analysis, measures });
}

async function chatScenario(page) {
    const series = new Series(page, 'c1-chat', 'Chat in der Galaxie: Ueberlappung, Prompt, Antwort, Cache');
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    await selectWithLayer(page, 'JSONBAgg');
    await series.shot('chat-zu', 'JSONBAgg mit einer Ebene, Chat geschlossen');
    const toggle = page.getByRole('button', { name: 'Open chat' });
    if (await toggle.count()) {
        await toggle.click();
        await wait(1200);
    }
    await series.shot('chat-offen', 'Chat geoeffnet');
    const measureOverlap = async (label) => {
        const row = page.locator('.cbm-chat-input-row');
        if (!(await row.count())) { series.note(`${label}: keine Chat-Eingabe sichtbar`); return; }
        const composer = await row.first().boundingBox();
        const details = await page.locator('.atlas-galaxy-selection-details').first().boundingBox().catch(() => null);
        const overlap = composer && details ? !(details.x > composer.x + composer.width || details.x + details.width < composer.x
            || details.y > composer.y + composer.height || details.y + details.height < composer.y) : null;
        series.note(`${label}: Selection details ${JSON.stringify(details)}; Chat-Eingabe ${JSON.stringify(composer)}; ueberlappen: ${overlap}`);
        if (composer) {
            const x = Math.max(0, composer.x - 220);
            const y = Math.max(0, composer.y - 120);
            await series.shot(`${label}-eingabe-ausschnitt`, `${label}: Ausschnitt Chat-Eingabe unten rechts`, {
                clip: { x, y, width: Math.min(VIEWPORT.width - x, composer.width + 240), height: Math.min(260, VIEWPORT.height - y) },
            });
        }
    };
    await measureOverlap('vor-agent');

    if (CHAT_MODEL) {
        const enable = page.getByRole('button', { name: 'Enable agent' });
        if (await enable.count()) {
            await enable.click();
            await wait(800);
            await series.shot('agent-konfiguration', 'Agent configuration geoeffnet');
            const load = page.getByRole('button', { name: /Download & load|Load model|Reload model/ });
            if (await load.count()) {
                await load.first().click();
                series.note('Modell wird geladen (Download beim ersten Lauf)');
                await page.waitForFunction(() => !document.querySelector('.cbm-chat-enable'), null, { timeout: 900000 }).catch(() => series.note('Modell nicht innerhalb von 15 min geladen'));
            }
            await page.keyboard.press('Escape').catch(() => {});
            await wait(1000);
        }
        await series.shot('agent-bereit', 'Agent nach dem Laden');
        await measureOverlap('agent-bereit');
        // Die automatische Erklaerung abwarten.
        await page.waitForFunction(() => {
            const section = document.querySelector('.cbm-chat-explanation');
            return section && !section.textContent?.includes('Preparing') && section.querySelector('.cbm-chat-retry');
        }, null, { timeout: 240000 }).catch(() => series.note('Keine fertige automatische Erklaerung binnen 4 min'));
        await series.shot('erklaerung', 'Automatische Erklaerung der Auswahl');
        const auto = await page.evaluate(() => window.__probeWorker.filter((entry) => entry.dir === 'to-worker'));
        series.note(`Nachrichten an den Worker bis zur Erklaerung: ${auto.length}`);

        const before = await page.evaluate(() => window.__probeWorker.length);
        const prompt = page.locator('#cbm-chat-prompt');
        await prompt.click();
        await prompt.fill('Who calls JSONBAgg? List every caller and the edge type.');
        await series.shot('frage', 'Frage eingegeben: Who calls JSONBAgg?');
        try {
            await page.getByRole('button', { name: 'Send ↑' }).click({ timeout: 4000 });
            series.note('Senden-Knopf per Maus klickbar');
        } catch (error) {
            const reason = String(error?.message ?? error).match(/<summary>[^<]*<\/summary>[^\n]*intercepts pointer events/)?.[0] ?? String(error?.message ?? error).split('\n')[0];
            series.note(`Senden-Knopf per Maus NICHT klickbar: ${reason}`);
            await prompt.press('Enter');
        }
        await wait(1500);
        await series.shot('antwort-laeuft', 'Antwort wird erzeugt');
        await page.waitForFunction(() => !document.querySelector('.cbm-chat-send[aria-label="Stop"]'), null, { timeout: 300000 }).catch(() => series.note('Antwort nicht binnen 5 min fertig'));
        await wait(800);
        await series.shot('antwort', 'Antwort fertig');
        const transcript = await page.locator('.cbm-chat-transcript').innerText().catch(() => '');
        const sent = await page.evaluate((from) => window.__probeWorker.slice(from), before);

        // Zum Vergleich: die echten eingehenden Kanten aus dem Index.
        const truth = await cypher(PROJECT, "MATCH (a)-[r]->(b) WHERE b.name = 'JSONBAgg' RETURN a.name AS source, type(r) AS type, a.file_path AS file ORDER BY type, source LIMIT 200");

        // Cache: weg und wieder zurueck zur selben Auswahl.
        await selectWithLayer(page, 'BaseCommand');
        await wait(4000);
        await series.shot('andere-auswahl', 'Andere Auswahl (BaseCommand, eine Ebene)');
        const mark = await page.evaluate(() => window.__probeWorker.length);
        await selectWithLayer(page, 'JSONBAgg');
        await series.shot('zurueck-1s', 'Zurueck zu JSONBAgg (nach 1,5 s)');
        await wait(6000);
        await series.shot('zurueck-7s', 'Zurueck zu JSONBAgg (nach 7,5 s)');
        const again = await page.evaluate((from) => window.__probeWorker.slice(from).filter((entry) => entry.dir === 'to-worker').length, mark);
        series.note(`Neue Worker-Auftraege nach Rueckkehr zur selben Auswahl: ${again} (0 hiesse: aus dem Cache)`);
        await series.save({ transcript, workerAfterQuestion: sent, workerAuto: auto, truth });
    } else {
        await series.save();
    }
}

async function headerScenario(page) {
    const series = new Series(page, 'h1-header', 'Agent-Anzeige oben rechts');
    await open(page, PROJECT, 'galaxy');
    await wait(1500);
    const header = await page.locator('.atlas-agent-group, .atlas-browser-ai-action').first().boundingBox().catch(() => null);
    await series.shot('kopfzeile', 'Kopfzeile');
    if (header) await series.shot('agent-ausschnitt', 'Ausschnitt Agent-Anzeige', { clip: { x: Math.max(0, header.x - 360), y: Math.max(0, header.y - 12), width: Math.min(VIEWPORT.width - Math.max(0, header.x - 360), header.width + 420), height: header.height + 24 } });
    await series.save();
}

async function architectureScenario(page, project, prefix) {
    const series = new Series(page, `${prefix}-architecture-${project}`, `Architecture-Tabs, Projekt ${project}`);
    await open(page, project, 'architecture');
    await wait(4000);
    await series.shot('overview-wurzel', 'Overview auf Wurzelebene');
    const tabs = ['Overview', 'Routes', 'Hotspots', 'System structure', 'Behavior'];
    const views = { Overview: 'overview', Routes: 'routes', Hotspots: 'hotspots', 'System structure': 'structure', Behavior: 'behavior' };
    const tab = (name) => page.locator(`button.atlas-arch-tab[data-view="${views[name]}"]`).first();

    // Reinklicken: den groessten Bereich doppelt anklicken.
    const area = project === 'django-demo' ? 'django' : 'src/mcp';
    const label = page.locator('button.architecture-node-label', { hasText: new RegExp(`^\\W*${area.replace('/', '\\/')}\\s*$`) }).first();
    if (await label.count()) {
        await label.hover();
        await series.shot('overview-hover', `Maus auf Bereich ${area}`);
        await label.click();
        await wait(600);
        await series.shot('overview-auswahl', `Bereich ${area} ausgewaehlt`);
        const openArea = page.getByRole('button', { name: /Open area/ }).first();
        if (await openArea.count()) {
            await openArea.hover();
            await series.shot('overview-open-area-hover', 'Maus auf Open area');
            await openArea.click();
        } else {
            await page.locator(`button.architecture-node-label.is-selected`).first().dblclick();
        }
        await settle(series, 'overview-drillin', `In ${area} hineingeklickt`);
        const fit = page.getByRole('button', { name: 'Fit map' });
        if (await fit.count()) { await fit.click(); await wait(1200); await series.shot('overview-drillin-fit', 'Fit map nach dem Hineinklicken'); }
        const canvas = await page.locator('.architecture-scene canvas, canvas').first().boundingBox();
        if (canvas) {
            await page.mouse.move(canvas.x + canvas.width * 0.7, canvas.y + canvas.height * 0.3, { steps: 6 });
            for (let step = 1; step <= 3; step += 1) { await page.mouse.wheel(0, -240); await wait(600); await series.shot(`overview-zoom${step}`, `Mausrad-Schritt ${step}, Cursor rechts oben`); }
        }
    } else series.note(`Bereich ${area} als Label nicht gefunden`);

    for (const name of tabs.slice(1)) {
        await tab(name).click().catch(() => series.note(`Tab ${name} nicht gefunden`));
        await wait(name === 'System structure' || name === 'Behavior' ? 9000 : 4000);
        await series.shot(`tab-${name.toLowerCase().replace(/\s+/g, '-')}`, `Tab ${name}`);
        if (name === 'Routes') {
            const endpoints = page.getByRole('button', { name: 'Endpoints', exact: true });
            if (await endpoints.count()) { await endpoints.click(); await wait(3000); await series.shot('routes-endpoints', 'Routes, Endpoints'); }
        }
        if (name === 'System structure') {
            const evidence = page.locator('summary', { hasText: 'Evidence and limits' }).first();
            if (await evidence.count()) { await evidence.click(); await wait(500); await series.shot('system-evidence', 'Evidence and limits aufgeklappt'); }
        }
        if (name === 'Behavior') {
            const start = page.locator('select').first();
            const options = await start.locator('option').allTextContents().catch(() => []);
            series.note(`Behavior, Startliste (${options.length} Eintraege): ${JSON.stringify(options.slice(0, 15))}`);
        }
    }
    const backend = await tool('get_architecture', { project, aspects: ['system_structure'], format: 'json' });
    const summary = {
        status: backend?.status ?? backend?.result?.status, warnings: backend?.warnings ?? backend?.result?.warnings,
        totals: backend?.totals ?? backend?.result?.totals, limits: backend?.limits ?? backend?.result?.limits,
        elapsed_ms: backend?.elapsed_ms ?? backend?.result?.elapsed_ms,
        groups: (backend?.groups ?? backend?.result?.groups ?? []).length, entrypoints: (backend?.entrypoints ?? backend?.result?.entrypoints ?? []).length,
        keys: Object.keys(backend ?? {}),
    };
    series.note(`get_architecture system_structure: ${JSON.stringify(summary)}`);
    await series.save({ systemStructure: summary });
}

async function systemScenario(page) {
    const series = new Series(page, 's1-system', 'System-Tab: Konfiguration, Indizes, Watcher, Logs');
    await open(page, PROJECT, 'system');
    await wait(3000);
    await series.shot('system', 'System-Tab');
    for (const name of ['Configuration', 'Indexes', 'Logs']) {
        const control = page.getByRole('tab', { name, exact: true }).first();
        if (await control.count()) { await control.click().catch(() => {}); await wait(1500); await series.shot(`system-${name.toLowerCase()}`, `System, ${name}`); }
    }
    await series.save();
}

/* ------------------------------------------------------------------ */

const context = await chromium.launchPersistentContext(PROFILE, {
    headless: HEADLESS,
    viewport: VIEWPORT,
    deviceScaleFactor: 2,
    args: ['--enable-unsafe-webgpu', '--ignore-gpu-blocklist'],
});
await context.addInitScript(pageProbe);
const page = context.pages()[0] ?? await context.newPage();
page.on('pageerror', (error) => console.error(`[pageerror] ${error.message}`));
await mkdir(OUT, { recursive: true });

const failures = [];
const run = async (name, fn) => {
    try { await fn(); }
    catch (error) { failures.push({ name, error: String(error?.stack ?? error) }); console.error(`[${name}] FEHLER ${error?.message ?? error}`); }
};

if (SCENARIOS.includes('galaxy')) {
    await run('g1', () => galaxySelection(page, 'JSONBAgg', 'g1', 'JSONBAgg auswaehlen, warten, zweimal Expand +1'));
    await run('g2', () => galaxySelection(page, 'Func', 'g2', 'Func (grosser Fall) auswaehlen, warten, Expand +1'));
    await run('g3', () => galaxyTraceIncoming(page));
    await run('g4', () => galaxyZoom(page));
    await run('g5', () => galaxyZoom(page, true));
}
if (SCENARIOS.includes('zoom')) {
    await run('g4', () => galaxyZoom(page));
    await run('g5', () => galaxyZoom(page, true));
}
if (SCENARIOS.includes('header')) await run('h1', () => headerScenario(page));
if (SCENARIOS.includes('architecture')) {
    await run('a1', () => architectureScenario(page, PROJECT, 'a1'));
    await run('a2', () => architectureScenario(page, CONTROL, 'a2'));
}
if (SCENARIOS.includes('system')) await run('s1', () => systemScenario(page));
if (SCENARIOS.includes('chat')) await run('c1', () => chatScenario(page));

await writeFile(join(OUT, 'run.json'), JSON.stringify({ origin: ORIGIN, project: PROJECT, control: CONTROL, scenarios: SCENARIOS, failures }, null, 2));
await context.close();
if (failures.length) { console.error(`${failures.length} Szenario(s) mit Fehler`); process.exitCode = 1; }
