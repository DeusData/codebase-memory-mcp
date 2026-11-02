#!/usr/bin/env node
/*
 * Browser-Pruefung der Galaxy-Korrekturen aus dem Handtest vom 03.10.2026
 * (graph-ui/verification/call-feedback-2026-10-02/KORREKTURPLAN.md, K2, K3,
 * K5, K6, K8, K9, K13).
 *
 *   node tools/handtest-fixes-galaxy.mjs --origin http://127.0.0.1:4371 \
 *        --out /tmp/handtest-galaxy [--only K2,K9] [--project django-demo]
 *
 * Jede Pruefung faehrt genau den Ablauf aus dem Korrekturplan, misst in der
 * laufenden Seite und druckt PASS oder FAIL mit den Messwerten. Die Bilder
 * liegen je Punkt unter <out>/<K>/, daneben eine index.md, die jedes Bild und
 * das, was es zeigt, auffuehrt. Gegen einen Stand ohne die Korrekturen
 * gefahren, sollen die Pruefungen fehlschlagen; das ist der Vorher-Beweis.
 *
 * Es startet keinen Server. Gebraucht wird ein Ursprung mit UI und /rpc, etwa
 * tools/lib/static-proxy.mjs vor einem laufenden Server.
 *
 * Nach dem Review der Korrekturen kamen dazu: die Auswahl bleibt bei Zurueck
 * auf dieselbe Wurzel und beim Abbruch (K2/K8), die Leiste passt auch bei
 * 1.494, 1.440, 1.366 und 1.280 px mit offenem Chat, ohne etwas abzuschneiden
 * (K3), die Hierarchie bei zwei Ebenen traegt Namen (K5), das Laden zaehlt
 * Seite fuer Seite und warnt vorher (K8), und der Quelltext nennt die echte
 * letzte Zeile (K13).
 *
 * Nach der Vollstaendigkeitspruefung kamen dazu: bei zwei Ebenen steht kein
 * Aufgerufener eines Tests in der Spalte "incoming", gemischt Erreichtes steht
 * im beschrifteten Band, jedes Kantenschild traegt Art und Pfeil, und ueber
 * 150 Knoten behalten Wurzel und direkte Nachbarn ihre Namen (K5); die Namen
 * im Mini-Galaxy von Explore liegen nicht aufeinander (K12); Bereich und
 * gezeigte Zeilen enden bei der echten letzten Zeile (K13); Zurueck und Vor
 * tragen Worte, solange die Leiste Platz hat (K2).
 *
 * Der Browser laeuft ohne Fenster (`headless`), mit dem Kanal `chromium`:
 * nur so zeichnet WebGL auf der GPU und nicht in SwiftShader, das die
 * Galaxie von 5.000 Knoten so langsam macht, dass der Baum in Explore nicht
 * fertig wird. `--headed` zeigt das Fenster.
 */

import { chromium } from 'playwright';
import { appendFile, mkdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';

const argv = process.argv.slice(2);
const arg = (name, fallback) => {
    const index = argv.indexOf(`--${name}`);
    if (index < 0) return fallback;
    const value = argv[index + 1];
    return value === undefined || value.startsWith('--') ? true : value;
};

const ORIGIN = String(arg('origin', 'http://127.0.0.1:4371')).replace(/\/$/, '');
const OUT = resolve(String(arg('out', 'verification/handtest-galaxy')));
const PROFILE = resolve(String(arg('profile', join(OUT, 'profile'))));
const PROJECT = String(arg('project', 'django-demo'));
const ONLY = String(arg('only', 'K9,K2,K3,K13,K6,K5,K8,K12')).split(',');
const HEADED = arg('headed', false) === true;
const VIEWPORT = { width: 1600, height: 1000 };
const ROOT = 'JSONBAgg';
const wait = (ms) => new Promise((done) => setTimeout(done, ms));

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

/** Der qualifizierte Name eines Symbols, so wie der Index ihn kennt. */
async function qualifiedName(name) {
    const answer = await tool('query_graph', { project: PROJECT, format: 'json', query: `MATCH (n) WHERE n.name = "${name}" RETURN n.qualified_name ORDER BY n.qualified_name LIMIT 1` });
    return answer?.rows?.[0]?.[0] ?? '';
}

/** Die direkten Beziehungen eines Symbols nach Richtung und Art, als Wahrheit fuer K13. */
async function relationCounts(name) {
    const count = async (pattern) => {
        const answer = await tool('query_graph', { project: PROJECT, format: 'json', query: `MATCH ${pattern} WHERE n.name = "${name}" RETURN type(r) AS type, count(r) AS edges ORDER BY type` });
        return Object.fromEntries((answer?.rows ?? []).map(([type, edges]) => [type, Number(edges)]));
    };
    return { incoming: await count('(a)-[r]->(n)'), outgoing: await count('(n)-[r]->(b)') };
}

/* ------------------------------------------------------------------ */
/* In jede Seite                                                       */

function pageProbe() {
    try { localStorage.setItem('cbm.workspace.setup', 'done'); } catch { /* ohne Speicher geht es auch */ }
    window.__probeErrors = [];
    window.addEventListener('error', (event) => window.__probeErrors.push(String(event.message)));
    window.__probeRpc = [];
    const nativeFetch = window.fetch.bind(window);
    window.fetch = async (input, init) => {
        const url = typeof input === 'string' ? input : input?.url ?? '';
        if (url.endsWith('/rpc') && init?.body) {
            try {
                const body = JSON.parse(String(init.body));
                window.__probeRpc.push({ t: Math.round(performance.now()), tool: body?.params?.name, cursor: Boolean(body?.params?.arguments?.cursor), maxRows: body?.params?.arguments?.max_rows ?? null });
            } catch { /* nur Beobachtung */ }
        }
        return nativeFetch(input, init);
    };
}

/* ------------------------------------------------------------------ */
/* Ergebnis                                                            */

const results = [];
const counters = new Map();

function check(id, title, pass, measured) {
    results.push({ id, title, pass: Boolean(pass), measured });
    console.log(`${pass ? 'PASS' : 'FAIL'}  ${id}  ${title}  ${JSON.stringify(measured)}`);
    return appendFile(join(OUT, id, 'index.md'), `\n**${pass ? 'PASS' : 'FAIL'}** ${title}\n\n\`\`\`json\n${JSON.stringify(measured, null, 2)}\n\`\`\`\n`);
}

/** Ein Bild, mit dem, was es zeigen soll, in der index.md des Punktes. */
async function shot(page, id, label, caption) {
    const next = (counters.get(id) ?? 0) + 1;
    counters.set(id, next);
    const file = `${String(next).padStart(2, '0')}-${label}.png`;
    await page.screenshot({ path: join(OUT, id, file) });
    await appendFile(join(OUT, id, 'index.md'), `- \`${file}\`: ${caption}\n`);
    return file;
}

async function section(page, id, title) {
    await mkdir(join(OUT, id), { recursive: true });
    await writeFile(join(OUT, id, 'index.md'), `# ${id}: ${title}\n\nOrigin ${ORIGIN}, project ${PROJECT}, viewport ${VIEWPORT.width}x${VIEWPORT.height}.\n\n`);
}

/* ------------------------------------------------------------------ */
/* Bedienung                                                           */

const toolbar = (page) => page.locator('.atlas-graph-exploration').first();
const scopeName = (page) => page.locator('.atlas-graph-scope-name').first().innerText({ timeout: 1000 }).catch(() => '');
const countText = (page) => page.locator('.atlas-graph-scope-count').first().innerText({ timeout: 1000 }).catch(() => '');
const layerText = (page) => toolbar(page).evaluate((bar) => [...bar.querySelectorAll('span')].map((el) => el.textContent?.trim() ?? '')
    .find((text) => /^\d+ layers?$/.test(text)) ?? '').catch(() => '');

async function open(page) {
    await page.goto(`${ORIGIN}/?project=${encodeURIComponent(PROJECT)}&workspace=galaxy`, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.atlas-shell', { timeout: 30000 });
    await page.waitForFunction(() => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0 && globalThis.__atlasGalaxyFit !== undefined, null, { timeout: 60000 });
    await wait(1200);
}

async function select(page, name) {
    const input = page.getByRole('searchbox', { name: 'Find a graph node' }).first();
    await input.click();
    await input.fill('');
    await input.type(name, { delay: 20 });
    const choice = page.locator('.atlas-galaxy-search-results button', { has: page.locator('strong', { hasText: new RegExp(`^${name}$`) }) }).first();
    await choice.waitFor({ timeout: 20000 });
    await choice.click();
}

/** Bis die Statuszeile einen fertigen Ausschnitt meldet. */
async function settled(page, timeout = 30000) {
    const started = Date.now();
    const ok = await page.waitForFunction(() => {
        const text = document.querySelector('.atlas-graph-scope-count')?.textContent ?? '';
        return /\d[\d,.]* nodes? · \d[\d,.]* edges?/.test(text) && !/Loading|Checking/.test(text);
    }, null, { timeout }).then(() => true).catch(() => false);
    await wait(900);
    return { ok, ms: Date.now() - started };
}

async function expand(page) {
    await page.getByRole('button', { name: 'Expand +1' }).click();
    return settled(page, 60000);
}

const canvasBox = (page) => page.locator('.atlas-galaxy canvas').first().boundingBox();

/* ------------------------------------------------------------------ */
/* K9: ein Klick ins Leere verlaesst den Ausschnitt nicht               */

async function checkK9(page) {
    await section(page, 'K9', 'A click on empty canvas keeps the scope; only All graph, Escape or Back leave it');
    await open(page);
    await select(page, ROOT);
    await settled(page);
    await expand(page);
    const before = { scope: await scopeName(page), layers: await layerText(page), count: await countText(page) };
    await shot(page, 'K9', 'before-click', `${ROOT} at ${before.layers}, ${before.count}: the scope before the click on empty canvas`);
    const box = await canvasBox(page);
    const root = await page.locator('[data-testid="atlas-galaxy-root-marker"] i').first().boundingBox().catch(() => null);
    // Wie im Handtest: knapp neben einen Knoten, und einmal in eine leere Ecke.
    const spots = [
        root ? { label: 'beside-root', x: root.x + root.width / 2 + 34, y: root.y + root.height / 2 - 30 } : null,
        { label: 'empty-corner', x: box.x + 60, y: box.y + box.height - 60 },
    ].filter(Boolean);
    const after = [];
    for (const spot of spots) {
        await page.mouse.click(spot.x, spot.y);
        await shot(page, 'K9', `${spot.label}-0ms`, `0 ms after a click at ${spot.label} (${Math.round(spot.x)}, ${Math.round(spot.y)})`);
        await wait(250);
        await shot(page, 'K9', `${spot.label}-250ms`, '250 ms after the click');
        await wait(750);
        await shot(page, 'K9', `${spot.label}-1s`, '1 s after the click: scope, layers and toolbar unchanged');
        await wait(2000);
        await shot(page, 'K9', `${spot.label}-3s`, '3 s after the click');
        after.push({ spot: spot.label, scope: await scopeName(page), layers: await layerText(page), count: await countText(page),
            headline: await page.locator('[data-testid="atlas-galaxy-headline"]').innerText().catch(() => '') });
    }
    const kept = after.every((row) => row.scope === before.scope && row.layers === before.layers && row.count === before.count);
    await check('K9', 'Clicks on empty canvas keep scope, layers and counts', kept, { before, after });

    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await page.keyboard.press('Escape');
    await wait(1500);
    const escaped = { scope: await scopeName(page), headline: await page.locator('[data-testid="atlas-galaxy-headline"]').innerText().catch(() => '') };
    await shot(page, 'K9', 'escape', `After Escape: back on the whole graph (${escaped.headline})`);
    await page.keyboard.press('Alt+ArrowLeft');
    const back = await settled(page);
    const restored = { scope: await scopeName(page), layers: await layerText(page), count: await countText(page), ms: back.ms };
    await shot(page, 'K9', 'alt-left', `After Alt+Left: ${restored.scope} at ${restored.layers} again`);
    await check('K9', 'Escape leaves the scope, Back (Alt+Left) returns to it', escaped.scope === '' && restored.scope === before.scope && restored.layers === before.layers,
        { escaped, restored });
}

/* ------------------------------------------------------------------ */
/* K2: Zurueck und Vor                                                  */

async function checkK2(page) {
    await section(page, 'K2', 'Back and Forward through the visited scopes, bounded, with a recent list');
    await open(page);
    const backButton = page.getByRole('button', { name: 'Back', exact: true });
    const forwardButton = page.getByRole('button', { name: 'Forward', exact: true });
    const state = async () => ({ scope: await scopeName(page), layers: await layerText(page), count: await countText(page),
        back: await backButton.getAttribute('title', { timeout: 1000 }).catch(() => null),
        backDisabled: await backButton.isDisabled({ timeout: 1000 }).catch(() => null),
        forward: await forwardButton.getAttribute('title', { timeout: 1000 }).catch(() => null),
        forwardDisabled: await forwardButton.isDisabled({ timeout: 1000 }).catch(() => null),
        history: await page.evaluate(() => { const h = globalThis.__atlasGalaxy?.history; return h ? { index: h.index, entries: h.entries.length } : null; }) });
    const steps = [];
    await select(page, ROOT);
    await settled(page);
    steps.push({ step: `select ${ROOT}`, ...await state() });
    await shot(page, 'K2', 'select-root', `${ROOT} selected from the whole graph: Back names "All graph"`);
    /*
     * Vollstaendigkeitspruefung: der Plan will "← Back" und "Forward →". In
     * Worten, solange die Leiste in voller Stufe passt; in den knappen Stufen
     * (offener Chat, schmales Fenster) nur die Pfeile. Die Tooltips bleiben.
     */
    const words = () => page.evaluate(() => {
        const bar = document.querySelector('.atlas-graph-exploration');
        const button = (label) => bar?.querySelector(`button[aria-label="${label}"]`);
        // Was zu sehen ist: das ausgeschriebene Wort, oder in den knappen Stufen das Zeichen, das CSS aus `data-label` zeichnet.
        const shown = (element) => {
            const wide = element?.querySelector('.atlas-fit-wide'), narrow = element?.querySelector('.atlas-fit-narrow');
            if (wide && getComputedStyle(wide).display !== 'none') return wide.textContent.trim();
            if (narrow && getComputedStyle(narrow).display !== 'none') return getComputedStyle(narrow, '::before').content.replace(/^"|"$/g, '');
            return element?.innerText.trim() ?? null;
        };
        return { fit: bar?.dataset.fit ?? null, back: shown(button('Back')), forward: shown(button('Forward')),
            backTitle: button('Back')?.title ?? null, forwardTitle: button('Forward')?.title ?? null };
    });
    const wide = await words();
    await page.locator('.atlas-graph-history').screenshot({ path: join(OUT, 'K2', 'history-words-full.png') }).catch(() => {});
    await shot(page, 'K2', 'history-words-full', `Chat closed at 1600 px, toolbar fit "${wide.fit}": Back reads "${wide.back}", Forward "${wide.forward}"`);
    await page.getByRole('button', { name: 'Open chat' }).first().click().catch(() => {});
    await page.setViewportSize({ width: 1440, height: VIEWPORT.height });
    await wait(1500);
    const narrow = await words();
    await page.locator('.atlas-graph-history').screenshot({ path: join(OUT, 'K2', 'history-words-compact.png') }).catch(() => {});
    await shot(page, 'K2', 'history-words-compact', `Chat open at 1440 px, toolbar fit "${narrow.fit}": Back reads "${narrow.back}", Forward "${narrow.forward}"`);
    await page.getByRole('button', { name: 'Hide chat' }).first().click().catch(() => {});
    await page.setViewportSize(VIEWPORT);
    await wait(1200);
    await check('K2', 'Back and Forward read "← Back" and "Forward →" at the full toolbar level and only the arrows in a compact level; the tooltips stay',
        wide.fit === 'full' && wide.back === '← Back' && wide.forward === 'Forward →' && narrow.fit !== 'full' && narrow.back === '←' && narrow.forward === '→'
        && /^Back to All graph/.test(wide.backTitle ?? '') && /^Back to All graph/.test(narrow.backTitle ?? '') && wide.forwardTitle === 'Nothing to go forward to',
        { wide, narrow });
    await expand(page);
    steps.push({ step: 'expand', ...await state() });
    const child = await qualifiedName('test_jsonb_agg_jsonfield_order_by');
    await page.evaluate((qn) => globalThis.__atlasGalaxy?.clickNode(qn), child);
    await settled(page);
    steps.push({ step: 'click test_jsonb_agg_jsonfield_order_by', ...await state() });
    await shot(page, 'K2', 'new-root', 'A click on a test made it the new root; Back names the previous scope');
    await page.locator('.atlas-graph-history').screenshot({ path: join(OUT, 'K2', 'history-buttons.png') }).catch(() => {});

    if (await backButton.count() === 0) {
        await check('K2', 'Back and Forward buttons exist in the scoped toolbar', false, { steps });
        return;
    }
    const a11y = await page.evaluate(() => ({ group: document.querySelector('.atlas-graph-history')?.getAttribute('aria-label') ?? null,
        rootButton: document.querySelector('button.atlas-graph-scope-name')?.getAttribute('aria-label') ?? null }));
    const t0 = Date.now();
    await backButton.click();
    await shot(page, 'K2', 'back-0ms', '0 ms after Back');
    await wait(250);
    await shot(page, 'K2', 'back-250ms', '250 ms after Back');
    const backOne = await settled(page);
    steps.push({ step: 'Back', ms: Date.now() - t0, settledOk: backOne.ok, ...await state() });
    await shot(page, 'K2', 'back-1', `After Back: ${ROOT} at 2 layers again, Forward names the test`);
    await backButton.click();
    await settled(page);
    // Review: Back to the same root at another depth keeps the selection and its details.
    const sameRoot = { ...await state(), details: await page.locator('.atlas-galaxy-selection-details').count() };
    steps.push({ step: 'Back (same root, 1 layer)', ...sameRoot });
    await shot(page, 'K2', 'back-same-root', `Back to ${ROOT} at 1 layer: Selection details still there (${sameRoot.details})`);
    await backButton.click();
    await wait(1500);
    steps.push({ step: 'Back', ...await state() });
    await shot(page, 'K2', 'back-all-graph', 'Third Back: the whole graph, Back disabled, Forward enabled');
    await forwardButton.click();
    await settled(page);
    steps.push({ step: 'Forward', ...await state() });
    await page.keyboard.press('Alt+ArrowRight');
    await settled(page);
    steps.push({ step: 'Alt+Right', ...await state() });
    await page.keyboard.press('Alt+ArrowRight');
    await settled(page);
    steps.push({ step: 'Alt+Right', ...await state() });
    await shot(page, 'K2', 'forward-end', 'Forward twice by keyboard: back at the test root, Forward disabled');

    const recent = page.locator('details.atlas-graph-recent');
    let recentNames = [];
    if (await recent.count()) {
        await recent.locator('summary').click();
        await wait(300);
        recentNames = await recent.locator('li button strong').allInnerTexts();
        await shot(page, 'K2', 'recent-open', `Recent list: ${recentNames.join(', ')}`);
        await recent.locator('li button', { has: page.locator('strong', { hasText: new RegExp(`^${ROOT}$`) }) }).first().click();
        await settled(page);
        steps.push({ step: `Recent ${ROOT}`, ...await state() });
        await shot(page, 'K2', 'recent-jump', `Jump from the recent list: ${ROOT} with its last depth, Forward dropped`);
    }
    // Begrenzt: viele Schritte, und der Verlauf haelt hoechstens 25 Eintraege.
    for (let i = 0; i < 30; i += 1) {
        await page.getByRole('button', { name: i % 2 ? 'Expand +1' : 'Remove graph layer' }).click({ timeout: 3000 }).catch(() => {});
        await settled(page, 10000);
    }
    await settled(page);
    let backs = 0;
    for (; backs < 40 && !(await backButton.isDisabled()); backs += 1) { await backButton.click(); await wait(60); }
    await settled(page);
    steps.push({ step: `Back until disabled (${backs} steps)`, ...await state() });
    const at = (index) => steps[index] ?? {};
    await check('K2', 'Back to the same root at another depth keeps the selection; the history group and the root button are named for assistive technology',
        sameRoot.scope === ROOT && sameRoot.layers === '1 layer' && sameRoot.details === 1 && a11y.group === 'History'
        && a11y.rootButton === 'Open the source of test_jsonb_agg_jsonfield_order_by', { sameRoot, a11y });
    const pass = at(0).back?.startsWith('Back to All graph') && at(2).back?.startsWith(`Back to ${ROOT} · 2 layers`)
        && at(3).scope === ROOT && at(3).layers === '2 layers' && at(3).forward?.startsWith('Forward to test_jsonb_agg_jsonfield_order_by')
        && at(4).layers === '1 layer' && at(5).scope === '' && at(5).backDisabled === true && at(6).scope === ROOT
        && at(8).scope === 'test_jsonb_agg_jsonfield_order_by' && at(8).forwardDisabled === true
        && recentNames.slice(0, 2).join() === `test_jsonb_agg_jsonfield_order_by,${ROOT}` && at(9).scope === ROOT && at(9).forwardDisabled === true
        && backs <= 24 && (at(10).history?.entries ?? 99) <= 25;
    await check('K2', 'Back/Forward/Alt+arrows/Recent restore root, depth and direction; history stays bounded', pass, { steps, recentNames, backsUntilStart: backs });
}

/* ------------------------------------------------------------------ */
/* K8: die dritte Ebene haengt nicht, "−" bricht ab                      */

const scopeState = (page) => page.locator('.atlas-graph-scope-count').first().getAttribute('data-state', { timeout: 1000 }).catch(() => null);
const rpcSince = (page, t0) => page.evaluate((from) => window.__probeRpc.filter((row) => row.t >= from), t0);
const pageNow = (page) => page.evaluate(() => Math.round(performance.now()));
const progressShown = (page) => page.locator('.atlas-graph-render-progress').first().innerText({ timeout: 500 }).catch(() => '');

async function checkK8(page) {
    await section(page, 'K8', 'Expand to layer 3 of JSONBAgg (both directions, all edge types) completes or warns quickly; "−" cancels within 1 s');
    await open(page);
    await select(page, ROOT);
    await settled(page);
    await expand(page);
    await shot(page, 'K8', 'two-layers', `${ROOT} at 2 layers: ${await countText(page)}`);
    const minus = page.getByRole('button', { name: 'Remove graph layer' });
    const expandButton = page.getByRole('button', { name: 'Expand +1' });
    // Review: the counted calls at the edge reach the toolbar a moment after the layer is complete.
    // G3 (2026-10-04): the explanation is the Galaxy's own tooltip (Hint), its text on the button as data-hint, no native title.
    await page.waitForFunction(() => /The index lists/.test([...document.querySelectorAll('.atlas-graph-exploration button')]
        .find((el) => el.getAttribute('aria-label') === 'Expand +1')?.getAttribute('data-hint') ?? ''), null, { timeout: 10000 }).catch(() => {});
    const expandTitle = await expandButton.getAttribute('data-hint');
    const expandWarning = await expandButton.getAttribute('data-warning');
    await check('K8', 'Before layer 3 loads, Expand warns: the index counts the calls waiting at the edge nodes',
        expandWarning === 'true' && /^Likely past the render limit of [\d.,]+ nodes\. Load layer 3: \d+ nodes to expand\. The index lists [\d.,]+ calls at them/.test(expandTitle ?? ''),
        { expandTitle, expandWarning });

    // 1. Abbrechen waehrend die dritte Ebene laedt.
    await expandButton.click();
    await wait(400);
    const during = { status: await countText(page), state: await scopeState(page), minusDisabled: await minus.isDisabled(), minusTitle: await minus.getAttribute('title'),
        progress: await progressShown(page) };
    await shot(page, 'K8', 'cancel-loading-400ms', `400 ms into layer 3: "${during.status}", "−" enabled with "${during.minusTitle}"`);
    const c0 = Date.now();
    await minus.click();
    await page.waitForFunction(() => document.querySelector('.atlas-graph-scope-count')?.getAttribute('data-state') !== 'loading'
        && [...document.querySelectorAll('.atlas-graph-exploration span')].some((el) => el.textContent?.trim() === '2 layers'), null, { timeout: 10000 }).catch(() => {});
    const cancelMs = Date.now() - c0;
    await shot(page, 'K8', 'cancel-0ms', `Right after "−": back on 2 layers in ${cancelMs} ms`);
    await wait(250);
    await shot(page, 'K8', 'cancel-250ms', '250 ms after "−"');
    await wait(750);
    const cancelled = { layers: await layerText(page), status: await countText(page), state: await scopeState(page), ms: cancelMs };
    await shot(page, 'K8', 'cancel-1s', `1 s after "−": ${cancelled.layers}, ${cancelled.status}`);
    cancelled.details = await page.locator('.atlas-galaxy-selection-details').count();
    await check('K8', '"−" cancels a running layer and returns to the previous one within 1 s, and the selection stays', during.state === 'loading' && during.minusDisabled === false
        && cancelMs <= 1000 && cancelled.layers === '2 layers' && /^90 nodes · 201 edges/.test(cancelled.status) && cancelled.details === 1, { expandTitle, during, cancelled });

    // 2. Die dritte Ebene ganz laden.
    await wait(1500);
    const t0 = await pageNow(page);
    const w0 = Date.now();
    await expandButton.click();
    const frames = [];
    for (const [label, at] of [['0ms', 0], ['250ms', 250], ['1s', 1000], ['2s', 2000], ['3s', 3000]]) {
        const elapsed = Date.now() - w0;
        if (at > elapsed) await wait(at - elapsed);
        frames.push({ at: label, status: await countText(page), state: await scopeState(page), progress: await progressShown(page),
            title: await page.locator('.atlas-graph-scope-count').first().getAttribute('title').catch(() => null) });
        await shot(page, 'K8', `layer3-${label}`, `${label} after Expand: "${frames.at(-1).status}"${frames.at(-1).progress ? `, overlay "${frames.at(-1).progress}"` : ''}`);
    }
    // Review: the counts move page by page instead of standing still for a whole batch.
    const loadingStatuses = [...new Set(frames.filter((frame) => frame.state === 'loading').map((frame) => frame.status))];
    await page.waitForFunction(() => ['complete', 'partial'].includes(document.querySelector('.atlas-graph-scope-count')?.getAttribute('data-state') ?? ''), null, { timeout: 180000 }).catch(() => {});
    const doneMs = Date.now() - w0;
    const calls = await rpcSince(page, t0);
    const queries = calls.filter((row) => row.tool === 'query_graph');
    await shot(page, 'K8', 'layer3-done', `Layer 3 finished after ${doneMs} ms with ${queries.length} query_graph calls: "${await countText(page)}"`);
    await wait(2000);
    const done = { ms: doneMs, status: await countText(page), state: await scopeState(page),
        title: await page.locator('.atlas-graph-scope-count').first().getAttribute('title').catch(() => null),
        overlayAfter2s: await progressShown(page), queryGraph: queries.length, withCursor: queries.filter((row) => row.cursor).length,
        maxRows: [...new Set(queries.map((row) => row.maxRows))], layers: await layerText(page),
        expandDisabled: await expandButton.isDisabled(), expandTitle: await expandButton.getAttribute('data-hint') };
    await shot(page, 'K8', 'layer3-done-2s', `2 s later: no "Updating view" overlay (${done.overlayAfter2s ? 'still shown' : 'gone'})`);
    await check('K8', 'Layer 3 completes (or stops at the render limit with a partial note) within 30 s in few large requests, and the overlay goes away',
        ['complete', 'partial'].includes(done.state ?? '') && doneMs <= 30000 && done.queryGraph <= 40 && done.overlayAfter2s === '' && done.layers === '3 layers',
        { frames, done });
    const doneRequests = frames.filter((frame) => /Request \d+ to the index/.test(frame.title ?? '')).length;
    await check('K8', 'While layer 3 loads, the counts move page by page and the tooltip names the running request',
        doneMs <= 3000 || (loadingStatuses.length >= 2 && doneRequests >= 1), { loadingStatuses, doneMs, titles: frames.map((frame) => frame.title) });
}

/* ------------------------------------------------------------------ */
/* K3: eine Zeile, auch bei offenem Chat                                */

async function toolbarRows(page) {
    return page.evaluate(() => {
        const bar = document.querySelector('.atlas-graph-exploration');
        if (!bar) return null;
        const box = bar.getBoundingClientRect();
        const items = [...bar.children].filter((el) => el.getBoundingClientRect().height > 0);
        const centres = items.map((el) => { const r = el.getBoundingClientRect(); return Math.round((r.top + r.bottom) / 2); });
        const rows = [...new Set(centres.map((centre) => Math.round(centre / 16)))].length;
        const clipped = items.filter((el) => el.scrollWidth > el.clientWidth + 1 && getComputedStyle(el).textOverflow === 'ellipsis')
            .map((el) => el.textContent?.trim().slice(0, 40));
        const style = getComputedStyle(bar);
        const edge = box.right - parseFloat(style.paddingRight) - parseFloat(style.borderRightWidth);
        const outside = items.filter((el) => el.getBoundingClientRect().right > edge + 0.5).map((el) => (el.getAttribute('aria-label') || el.textContent || el.tagName).trim().slice(0, 24));
        const more = bar.querySelector('details.atlas-graph-more > summary')?.getBoundingClientRect();
        // Steht die Wurzel schmaler, als Text und Hoechstbreite es wollen (Innenbreite, wie toolbar-fit.tsx rechnet)?
        const name = bar.querySelector('.atlas-graph-scope-name');
        const nameStyle = name ? getComputedStyle(name) : null;
        const nameMax = nameStyle ? parseFloat(nameStyle.maxWidth) : NaN;
        const nameRoom = nameStyle && Number.isFinite(nameMax) ? (nameStyle.boxSizing === 'border-box'
            ? nameMax - parseFloat(nameStyle.borderLeftWidth) - parseFloat(nameStyle.borderRightWidth)
            : nameMax + parseFloat(nameStyle.paddingLeft) + parseFloat(nameStyle.paddingRight)) : Infinity;
        const rootSqueezed = name ? name.clientWidth + 1 < Math.min(name.scrollWidth, nameRoom) : false;
        return { width: Math.round(box.width), height: Math.round(box.height), rows, overflow: bar.scrollWidth > bar.clientWidth + 1, clipped, fit: bar.dataset.fit ?? null,
            root: name?.textContent ?? null, rootWidth: name ? name.clientWidth : null, rootSqueezed,
            outside, moreInside: Boolean(more && more.width > 0 && more.right <= edge + 0.5 && more.left >= box.left),
            items: items.map((el) => `${(el.getAttribute('aria-label') || el.textContent || el.tagName).trim().slice(0, 18)}:${Math.round(el.getBoundingClientRect().width)}`) };
    });
}

async function checkK3(page) {
    await section(page, 'K3', 'The scoped Galaxy toolbar stays one row at 1600 px, with the chat closed and open');
    await open(page);
    const chatToggle = async (open) => {
        const button = page.getByRole('button', { name: open ? 'Open chat' : 'Hide chat' }).first();
        if (await button.count()) { await button.click(); await wait(1500); }
    };
    const rows = [];
    const measure = async (label, caption) => {
        const value = await toolbarRows(page);
        rows.push({ label, ...value });
        await shot(page, 'K3', label, `${caption}: toolbar ${value?.width} x ${value?.height} px, ${value?.rows} row(s)`);
        await page.locator('.atlas-graph-exploration').first().screenshot({ path: join(OUT, 'K3', `${label}-toolbar.png`) }).catch(() => {});
    };
    await select(page, ROOT);
    await settled(page);
    await measure('closed-root', `Chat closed, ${ROOT} at 1 layer`);
    await chatToggle(true);
    await measure('open-root', `Chat open, ${ROOT} at 1 layer`);
    await expand(page);
    await measure('open-two-layers', 'Chat open, 2 layers');
    const child = await qualifiedName('test_jsonb_agg_jsonfield_order_by');
    await page.evaluate((qn) => globalThis.__atlasGalaxy?.clickNode(qn), child);
    await settled(page);
    await measure('open-long-root', 'Chat open, long root name test_jsonb_agg_jsonfield_order_by');
    const back = page.getByRole('button', { name: 'Back', exact: true });
    // Ohne Zurueck (Stand vor K2) fuehrt die Suche zur selben Lage.
    if (await back.count()) await back.click(); else { await select(page, ROOT); await settled(page); await expand(page); }
    await settled(page);
    await page.getByRole('button', { name: 'Expand +1' }).click();
    await wait(600);
    await measure('open-loading', 'Chat open while layer 3 loads');
    await page.waitForFunction(() => ['complete', 'partial'].includes(document.querySelector('.atlas-graph-scope-count')?.getAttribute('data-state') ?? ''), null, { timeout: 120000 }).catch(() => {});
    await wait(1500);
    await measure('open-partial', 'Chat open, layer 3 stopped at the render limit');
    await chatToggle(false);
    await measure('closed-partial', 'Chat closed again, layer 3 partial');
    const pass = rows.length === 7 && rows.every((row) => row.rows === 1 && row.height < 60 && !row.overflow && row.outside.length === 0);
    await check('K3', 'One toolbar row in every state, chat open (bar about 1170 px) and closed (1600 px)', pass, { rows });
    // Vollstaendigkeitspruefung zu K2: die Worte an Zurueck und Vor kosten die volle Stufe, nicht den Namen der Wurzel.
    await check('K3', 'At the full level (words on Back and Forward) the root name is never squeezed below its text; where it would be, the toolbar takes the compact level',
        rows.every((row) => row.fit !== 'full' || !row.rootSqueezed) && rows.some((row) => row.fit === 'full'),
        { rows: rows.map((row) => ({ label: row.label, fit: row.fit, root: row.root, rootWidth: row.rootWidth, rootSqueezed: row.rootSqueezed })) });

    /*
     * Review: bei 1.494 px (das Fenster des Handtests) und darunter schnitt die
     * Leiste Zaehler und Menue "⋯" ab. Jetzt passt sie ohne Abschneiden: eine
     * Zeile bis 1.440 px, darunter zwei, und das Menue mit den Limits oeffnet.
     */
    await chatToggle(true);
    const narrow = [];
    const atWidth = async (width, state) => {
        await page.setViewportSize({ width, height: VIEWPORT.height });
        await wait(900);
        const value = await toolbarRows(page);
        const more = page.locator('details.atlas-graph-more').first();
        await more.locator('> summary').click({ timeout: 3000 }).catch(() => {});
        await wait(300);
        const limitsVisible = await page.getByRole('combobox', { name: 'Rendered node limit' }).first().isVisible().catch(() => false);
        const label = `${state}-${width}`;
        await shot(page, 'K3', label, `Chat open at ${width} px, ${state}: fit "${value?.fit}", ${value?.rows} row(s), ${value?.outside.length} item(s) past the edge, ⋯ menu open with Limits ${limitsVisible ? 'visible' : 'missing'}`);
        await page.keyboard.press('Escape');
        await more.evaluate((el) => { el.open = false; }).catch(() => {});
        await page.locator('.atlas-graph-exploration').first().screenshot({ path: join(OUT, 'K3', `${label}-toolbar.png`) }).catch(() => {});
        narrow.push({ label, width, ...value, limitsVisible });
    };
    for (const width of [1494, 1440, 1366, 1280]) await atWidth(width, 'partial');
    await page.setViewportSize(VIEWPORT);
    await wait(600);
    if (await back.count()) await back.click();
    await settled(page);
    for (const width of [1494, 1440, 1366, 1280]) await atWidth(width, 'two-layers');
    await page.setViewportSize(VIEWPORT);
    await wait(600);
    const narrowPass = narrow.length === 8 && narrow.every((row) => row.outside.length === 0 && !row.overflow && row.moreInside && row.limitsVisible
        && (row.width >= 1440 ? row.rows === 1 && row.height < 60 : row.rows <= 2));
    await check('K3', 'With the chat open at 1494, 1440, 1366 and 1280 px nothing is cut off: one row down to 1440 px, two below, the ⋯ menu with Limits opens',
        narrowPass, { narrow });
}

/* ------------------------------------------------------------------ */
/* K13: Selection details aus dem geladenen Ausschnitt                  */

const typeList = (counts) => Object.entries(counts).sort(([a, x], [b, y]) => y - x || a.localeCompare(b)).map(([type, n]) => `${type} ${n}`).join(' · ');

async function checkK13(page) {
    await section(page, 'K13', 'Selection details lists the relationships of the loaded scope; "Next lines" at the end of a file says so');
    const truth = await relationCounts(ROOT);
    const total = (counts) => Object.values(counts).reduce((sum, n) => sum + n, 0);
    await open(page);
    await select(page, ROOT);
    await settled(page);
    const details = page.locator('.atlas-galaxy-selection-details').first();
    await details.locator('> summary').click();
    await wait(700);
    const summaries = await details.locator('summary').allInnerTexts();
    const source = await details.locator('.selection-context-source').innerText().catch(() => '');
    await shot(page, 'K13', 'details-root', `Selection details for ${ROOT}: ${summaries.filter((text) => /relationships/.test(text)).join(' | ')}`);
    await details.screenshot({ path: join(OUT, 'K13', 'details-root-panel.png') }).catch(() => {});
    const wantIn = `Incoming relationships · ${total(truth.incoming)} (${typeList(truth.incoming)})`;
    const wantOut = `Outgoing relationships · ${total(truth.outgoing)} (${typeList(truth.outgoing)})`;
    await check('K13', `Selection details of ${ROOT} match the index: incoming and outgoing by type, from the loaded scope`,
        summaries.includes(wantIn) && summaries.includes(wantOut) && /loaded Galaxy scope/.test(source), { truth, wantIn, wantOut, summaries, source });

    // Eine Auswahl hinter dem Deckel des Schnappschusses (tests/ liegt hinter 20.000 Knoten).
    const child = await qualifiedName('test_jsonb_agg_jsonfield_order_by');
    const childTruth = await relationCounts('test_jsonb_agg_jsonfield_order_by');
    await page.evaluate((qn) => globalThis.__atlasGalaxy?.clickNode(qn), child);
    await settled(page);
    await wait(800);
    const childText = await details.innerText().catch(() => '');
    const childSummaries = await details.locator('summary').allInnerTexts();
    await shot(page, 'K13', 'details-test-behind-cap', `Selection details for a test behind the snapshot cap: ${childSummaries.filter((text) => /relationships/.test(text)).join(' | ')}`);
    const childOut = `Outgoing relationships · ${total(childTruth.outgoing)} (${typeList(childTruth.outgoing)})`;
    await check('K13', 'A selected test outside the capped snapshot still shows its relationships from the scope', childSummaries.includes(childOut)
        && !/absent from this bounded index snapshot/.test(childText), { childTruth, childOut, childSummaries });

    // "Read source evidence": general.py endet in Zeile 65 (dazu die leere Zeile 66).
    await page.getByRole('button', { name: 'Back', exact: true }).click().catch(() => {});
    await settled(page);
    await wait(800);
    await details.getByRole('button', { name: 'Read source evidence' }).click();
    await page.waitForSelector('nav[aria-label="Source pages"]', { timeout: 20000 }).catch(() => {});
    await wait(500);
    const pager = page.locator('nav[aria-label="Source pages"]').first();
    const pagerText = await pager.innerText().catch(() => '');
    const next = pager.getByRole('button', { name: 'Next lines' });
    const nextState = { disabled: await next.isDisabled().catch(() => null), title: await next.getAttribute('title').catch(() => null) };
    const lines = await page.locator('.source-evidence-lines code > span').count();
    const numbers = await page.locator('.source-evidence-lines .source-evidence-line-number').allInnerTexts();
    await page.locator('.source-evidence-lines').evaluate((el) => { el.scrollTop = el.scrollHeight; }).catch(() => {});
    await wait(300);
    await shot(page, 'K13', 'source-evidence-end', `Source evidence for general.py scrolled to the end: "${pagerText.replace(/\s+/g, ' ')}", last drawn line ${numbers.at(-1)}`);
    await page.locator('nav[aria-label="Source pages"]').first().screenshot({ path: join(OUT, 'K13', 'source-pager.png') }).catch(() => {});
    await check('K13', '"Next lines" is disabled at the real end of general.py and names its last line, 65', /end of file/.test(pagerText) && nextState.disabled === true
        && nextState.title === 'Line 65 is the last line of this file.', { pagerText, nextState, renderedLines: lines });
    // Vollstaendigkeitspruefung: Bereich, gezeigte Zeilen und Tooltip enden alle bei 65, keine leere Zeile 66.
    await check('K13', 'The range text and the drawn lines agree with the last real line: "Lines 42 to 65 · end of file", last drawn line 65',
        /Lines 42 to 65 · end of file/.test(pagerText) && numbers.at(-1) === '65' && !numbers.includes('66'),
        { pagerText: pagerText.replace(/\s+/g, ' '), firstLine: numbers[0], lastLine: numbers.at(-1), drawn: numbers.length });
    await page.keyboard.press('Escape');
}

/* ------------------------------------------------------------------ */
/* K6: Kamera auf den Pfad, Kantenlabels frei von Namen                 */

async function pickPath(page, name) {
    const picker = page.locator('details.atlas-graph-path-picker').first();
    await picker.locator('summary').click();
    const search = page.getByRole('searchbox', { name: 'Find a path target' });
    await search.fill('');
    await search.type(name, { delay: 15 });
    await page.locator('.atlas-graph-path-menu button', { has: page.locator('strong', { hasText: new RegExp(`^${name}$`) }) }).first().click();
}

async function pathLayout(page) {
    return page.evaluate(() => {
        const rect = (el) => { const r = el.getBoundingClientRect(); return { left: r.left, right: r.right, top: r.top, bottom: r.bottom, text: el.textContent?.trim() ?? '' }; };
        const labels = [...document.querySelectorAll('.atlas-galaxy-path-label')].map(rect).filter((r) => r.right > r.left);
        const names = [...document.querySelectorAll('.atlas-galaxy-path-node b, .atlas-galaxy-root-marker b')].map(rect).filter((r) => r.right > r.left);
        const rings = [...document.querySelectorAll('.atlas-galaxy-path-node > i, .atlas-galaxy-root-marker > i')].map(rect);
        const hit = (a, b) => !(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top);
        const overlaps = [];
        for (const label of labels) for (const name of names) if (hit(label, name)) overlaps.push(`${label.text} x ${name.text}`);
        for (let i = 0; i < labels.length; i += 1) for (let j = i + 1; j < labels.length; j += 1) if (hit(labels[i], labels[j])) overlaps.push(`${labels[i].text} x ${labels[j].text}`);
        const canvas = document.querySelector('.atlas-galaxy canvas')?.getBoundingClientRect();
        const panel = document.querySelector('[data-testid="atlas-galaxy-path-panel"]')?.getBoundingClientRect();
        const centres = rings.map((r) => ({ x: (r.left + r.right) / 2, y: (r.top + r.bottom) / 2 }));
        let spread = 0;
        for (let i = 0; i < centres.length; i += 1) for (let j = i + 1; j < centres.length; j += 1) spread = Math.max(spread, Math.hypot(centres[i].x - centres[j].x, centres[i].y - centres[j].y));
        const inside = canvas ? centres.every((c) => c.x > canvas.left && c.x < canvas.right && c.y > canvas.top && c.y < canvas.bottom) : false;
        const underPanel = panel ? centres.filter((c) => c.x > panel.left && c.x < panel.right && c.y > panel.top && c.y < panel.bottom).length : 0;
        const box = (r) => `${r.text}@${Math.round(r.left)},${Math.round(r.top)},${Math.round(r.right)},${Math.round(r.bottom)}`;
        return { labels: labels.length, names: names.map((n) => n.text), overlaps, spreadPx: Math.round(spread), inside, underPanel,
            boxes: overlaps.length ? [...labels, ...names].map(box) : undefined,
            camera: globalThis.__atlasGalaxyFit?.measure?.().camera.position.map((v) => Math.round(v)) ?? null };
    });
}

async function checkK6(page) {
    await section(page, 'K6', 'Showing a path flies the camera onto its nodes, and no edge label lies under a node name');
    await open(page);
    await select(page, ROOT);
    await settled(page);
    const target = 'test_jsonb_agg_charfield_order_by';
    const before = await pathLayout(page);
    await shot(page, 'K6', 'before-path', `${ROOT} at 1 layer before Path to (camera ${JSON.stringify(before.camera)})`);
    await pickPath(page, target);
    const frames = [];
    for (const [label, ms] of [['0ms', 0], ['250ms', 250], ['1s', 750], ['3s', 2000]]) {
        await wait(ms);
        frames.push({ at: label, ...await pathLayout(page) });
        await shot(page, 'K6', `path-1hop-${label}`, `${label} after Path to ${target}: spread ${frames.at(-1).spreadPx} px, ${frames.at(-1).overlaps.length} label overlaps`);
    }
    const oneHop = frames.at(-1);
    await check('K6', `Path to ${target} (1 hop): camera moved, path nodes far apart and in view, no edge label under a name`,
        JSON.stringify(oneHop.camera) !== JSON.stringify(before.camera) && oneHop.spreadPx >= 200 && oneHop.inside && oneHop.underPanel === 0
        && oneHop.overlaps.length === 0 && oneHop.labels > 0, { before: before.camera, frames });

    // Ein laengerer Pfad bei zwei Ebenen.
    await page.keyboard.press('Escape');
    await wait(400);
    await expand(page);
    await pickPath(page, 'Func');
    await wait(3000);
    const twoHops = await pathLayout(page);
    const heading = await page.locator('[data-testid="atlas-galaxy-path-panel"] strong').first().innerText().catch(() => '');
    await shot(page, 'K6', 'path-func', `${heading}: spread ${twoHops.spreadPx} px, ${twoHops.overlaps.length} overlaps`);
    for (let step = 0; step < 2; step += 1) {
        await page.locator('[data-testid="atlas-galaxy-path-panel"]').getByRole('button', { name: 'Next' }).click().catch(() => {});
        await wait(700);
    }
    const stepped = await pathLayout(page);
    await shot(page, 'K6', 'path-func-stepped', `After stepping: ${stepped.overlaps.length} overlaps, camera ${JSON.stringify(stepped.camera)}`);
    await check('K6', 'A two-hop path is framed too, and stepping keeps labels clear of names', twoHops.overlaps.length === 0 && stepped.overlaps.length === 0
        && twoHops.inside && twoHops.underPanel === 0 && /Path to Func/.test(heading), { heading, twoHops, stepped });
    await page.keyboard.press('Escape');
}

/* ------------------------------------------------------------------ */
/* K5: Hierarchie links eingehend, rechts ausgehend                     */

async function hierarchyLayout(page) {
    return page.evaluate(() => {
        const galaxy = globalThis.__atlasGalaxy, fit = globalThis.__atlasGalaxyFit;
        const canvas = document.querySelector('.atlas-galaxy canvas')?.getBoundingClientRect();
        const boxes = galaxy?.labelBoxes ?? [];
        // Namen: Weltkaesten der Szene, mit derselben Kamera in Pixel gerechnet.
        const names = boxes.map((box) => {
            // Stand vor K5: ohne `project` gibt es keine Namen-Messung.
            const [a, b] = typeof fit?.project === 'function'
                ? fit.project([{ x: box.x - box.width / 2, y: box.y + box.height / 2, z: 0 }, { x: box.x + box.width / 2, y: box.y - box.height / 2, z: 0 }]) : [];
            return a && b && canvas ? { text: box.name, left: canvas.left + Math.min(a.x, b.x), right: canvas.left + Math.max(a.x, b.x), top: canvas.top + Math.min(a.y, b.y), bottom: canvas.top + Math.max(a.y, b.y), worldWidth: box.width } : null;
        }).filter(Boolean);
        const labels = [...document.querySelectorAll('[data-testid="atlas-hierarchy-edge-label"], .atlas-galaxy-path-label')].map((el) => {
            const r = el.getBoundingClientRect(); return { text: el.textContent?.trim() ?? '', left: r.left, right: r.right, top: r.top, bottom: r.bottom };
        }).filter((r) => r.right > r.left);
        const hit = (a, b) => !(a.right <= b.left + 1 || b.right <= a.left + 1 || a.bottom <= b.top + 1 || b.bottom <= a.top + 1);
        const overlaps = [];
        for (const label of labels) for (const name of names) if (hit(label, name)) overlaps.push(`${label.text} x ${name.text}`);
        for (let i = 0; i < names.length; i += 1) for (let j = i + 1; j < names.length; j += 1) if (hit(names[i], names[j])) overlaps.push(`${names[i].text} x ${names[j].text}`);
        // Abgeschnitten heisst: der Kasten hat die Breitengrenze der Textur erreicht (1600 px bei Schrift 12, rund 312 Einheiten).
        const truncated = names.filter((name) => name.worldWidth >= 305).map((name) => name.text);
        const placements = galaxy?.hierarchy?.placements ?? [];
        const chip = document.querySelector('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]');
        const hint = chip?.closest('[data-hint]')?.getAttribute('data-hint') ?? chip?.getAttribute('data-hint') ?? '';
        // Vollstaendigkeitspruefung: die Ueberschrift des Bandes und die Notiz bei fehlenden Namen, beide im DOM gemessen.
        const bandElement = document.querySelector('[data-testid="atlas-hierarchy-band-label"]');
        const bandRect = bandElement?.getBoundingClientRect();
        const band = bandElement && bandRect ? { text: bandElement.textContent?.replace(/\s+/g, ' ').trim() ?? '', left: bandRect.left, right: bandRect.right,
            top: bandRect.top, bottom: bandRect.bottom, inCanvas: Boolean(canvas && bandRect.bottom > canvas.top && bandRect.top < canvas.bottom && bandRect.right > canvas.left && bandRect.left < canvas.right) } : null;
        const bandOverlaps = band ? [...names, ...labels].filter((other) => hit(band, other)).map((other) => other.text) : [];
        const key = document.querySelector('[data-testid="atlas-hierarchy-key"]')?.textContent?.trim() ?? null;
        return { mode: galaxy?.mode, placements: placements.map((p) => ({ name: p.name, key: p.key, x: Math.round(p.x), y: Math.round(p.y), hop: p.hop, side: p.side ?? null, mixed: p.mixed === true })),
            edges: galaxy?.hierarchy?.edges ?? [], bandInfo: galaxy?.hierarchy?.band ?? null, namesMode: galaxy?.hierarchy?.names ?? null,
            edgeLabels: labels.map((label) => label.text), names: names.length, nameTexts: names.map((name) => name.text), overlaps, truncated, hint, band, bandOverlaps, key };
    });
}

/** Ein Ausschnitt um die Leinwand, damit die Namen auch im Bild zu lesen sind. */
async function canvasShot(page, id, label, caption) {
    const box = await canvasBox(page);
    const next = (counters.get(id) ?? 0) + 1;
    counters.set(id, next);
    const file = `${String(next).padStart(2, '0')}-${label}.png`;
    await page.screenshot({ path: join(OUT, id, file), clip: box });
    await appendFile(join(OUT, id, 'index.md'), `- \`${file}\`: ${caption}\n`);
    return file;
}

async function checkK5(page) {
    await section(page, 'K5', 'Hierarchy of a scope: incoming left, root centred, outgoing right, typed edges, full names, call order by line, path and call order');
    const truth = await relationCounts(ROOT);
    await open(page);
    await select(page, ROOT);
    await settled(page);
    await page.locator('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]').click();
    await wait(2500);
    const view = await hierarchyLayout(page);
    await shot(page, 'K5', 'hierarchy-root', `${ROOT} at 1 layer in hierarchy: ${view.edgeLabels.length} edge labels, ${view.overlaps.length} overlaps`);
    const root = view.placements.find((p) => p.name === ROOT);
    const left = view.placements.filter((p) => p.x < 0), right = view.placements.filter((p) => p.x > 0);
    const incoming = Object.values(truth.incoming).reduce((sum, n) => sum + n, 0);
    await check('K5', 'Incoming on the left, root at the centre, outgoing on the right, typed edge labels, full names, no overlaps, honest hint',
        root?.x === 0 && left.length > 0 && right.length === 2 && left.length + right.length + 1 === view.placements.length
        && view.edgeLabels.some((text) => /CALLS · TESTS/.test(text)) && view.edgeLabels.some((text) => /DEFINES/.test(text)) && view.edgeLabels.some((text) => /INHERITS/.test(text))
        && view.overlaps.length === 0 && view.truncated.length === 0 && /incoming relationships on the left/.test(view.hint),
        { truth: { incomingEdges: incoming, ...truth }, left: left.map((p) => p.name), right: right.map((p) => p.name), edgeLabels: view.edgeLabels,
            overlaps: view.overlaps, truncated: view.truncated, hint: view.hint });

    // Pfad in der Hierarchie, zu einem Test links.
    await pickPath(page, 'test_jsonb_agg_charfield_order_by');
    await wait(2500);
    const pathView = await hierarchyLayout(page);
    const pathHeading = await page.locator('[data-testid="atlas-galaxy-path-panel"] strong').first().innerText().catch(() => '');
    await shot(page, 'K5', 'hierarchy-path', `${pathHeading} in hierarchy: highlighted path, ${pathView.overlaps.length} overlaps`);
    await check('K5', 'Path to works in the hierarchy and highlights its nodes', /Path to test_jsonb_agg_charfield_order_by · 1 hop/.test(pathHeading)
        && pathView.mode === 'hierarchy' && pathView.edgeLabels.includes('CALLS'), { pathHeading, edgeLabels: pathView.edgeLabels, overlaps: pathView.overlaps });
    await page.keyboard.press('Escape');
    await wait(500);

    // Aufrufreihe: Spalte rechts nach Aufrufzeile.
    await select(page, 'call_command');
    await settled(page);
    await page.locator('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]').click();
    await wait(2500);
    const callView = await hierarchyLayout(page);
    await page.getByRole('button', { name: 'Call order' }).click();
    await wait(1500);
    const panelText = await page.locator('[data-testid="atlas-galaxy-path-panel"]').innerText().catch(() => '');
    const callees = [...panelText.matchAll(/line (\d+)\s+call_command --CALLS--> (\S+)/g)].map((m) => ({ line: Number(m[1]), name: m[2] }));
    const firstSeen = [...new Map(callees.map((entry) => [entry.name, entry])).values()].map((entry) => entry.name);
    const rightColumn = callView.placements.filter((p) => p.x > 0 && p.hop === 1).sort((a, b) => b.y - a.y).map((p) => p.name);
    const columnOrder = rightColumn.filter((name) => firstSeen.includes(name));
    await shot(page, 'K5', 'hierarchy-call-order', `Call order of call_command in hierarchy: ${callees.length} calls, right column top to bottom ${columnOrder.slice(0, 5).join(', ')}...`);
    await check('K5', 'The outgoing column of call_command reads in call-site line order, and Call order works in hierarchy', callees.length > 1
        && JSON.stringify(columnOrder) === JSON.stringify(firstSeen) && callView.overlaps.length === 0,
    { callees, columnOrder, firstSeen, overlaps: callView.overlaps, truncated: callView.truncated });
    await page.keyboard.press('Escape');
    await wait(500);

    /*
     * Review: bei zwei Ebenen (90 Knoten) stand kein einziger Name, aber achtzig
     * Kantenschilder auf beliebigen Paaren. Jetzt tragen bis 150 Knoten alle
     * ihren Namen.
     */
    await select(page, ROOT);
    await settled(page);
    if (await page.locator('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"][data-active="true"]').count() === 0) {
        await page.locator('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]').click();
    }
    await expand(page);
    await wait(3000);
    const twoLayers = await hierarchyLayout(page);
    const count = await countText(page);
    await shot(page, 'K5', 'hierarchy-two-layers', `${ROOT} at 2 layers in hierarchy, fitted (${count}): ${twoLayers.names} names, ${twoLayers.edgeLabels.length} edge labels shown, ${twoLayers.overlaps.length} overlaps, band "${twoLayers.band?.text ?? 'none'}"`);
    const placed = twoLayers.placements;
    const leftFirst = placed.filter((p) => !p.mixed && p.side === -1 && p.hop === 1);
    const rightFirst = placed.filter((p) => !p.mixed && p.side === 1 && p.hop === 1);
    // Hineinzoomen auf die linke Spalte der ersten Ebene: dort werden die Namen lesbar, und erst dann stehen die Kantenschilder.
    const middle = (list) => list[Math.floor(list.length / 2)] ?? { x: 0, y: 0 };
    await zoomAt(page, middle(leftFirst), 6);
    const zoomed = await hierarchyLayout(page);
    await shot(page, 'K5', 'hierarchy-two-layers-zoomed', `Zoomed in on the first incoming column at 2 layers: names read in full, ${zoomed.edgeLabels.length} edge labels on the lines in view, ${zoomed.overlaps.length} overlaps`);
    await check('K5', 'At 2 layers (90 nodes) the hierarchy draws every name; edge labels wait until the names read and then sit clear of them',
        /^90 nodes/.test(count) && twoLayers.names >= 85 && twoLayers.overlaps.length === 0 && twoLayers.truncated.length === 0
        && zoomed.edgeLabels.length > 0 && zoomed.overlaps.length === 0 && /the type and direction of each relationship at its line/.test(twoLayers.hint),
    { count, fitted: { names: twoLayers.names, edgeLabels: twoLayers.edgeLabels, overlaps: twoLayers.overlaps },
        zoomed: { names: zoomed.names, edgeLabels: zoomed.edgeLabels, overlaps: zoomed.overlaps }, truncated: twoLayers.truncated, hint: twoLayers.hint });

    /*
     * Vollstaendigkeitspruefung (K5 unvollstaendig): ab zwei Ebenen stand jeder
     * Knoten auf der Seite seines Elternknotens, und ganz links unter
     * "incoming" standen die Aufgerufenen der Tests (len, str, print) und die
     * Klassen, die general.py definiert. Wahrheit aus dem Index: was die
     * Knoten der ersten Spalte links selbst aufrufen oder definieren, darf
     * links nur stehen, wenn es auch in eine von ihnen hineinfuehrt.
     */
    const forward = new Map();
    for (const node of leftFirst) {
        const answer = await tool('query_graph', { project: PROJECT, format: 'json', query: `MATCH (a)-[r]->(b) WHERE a.qualified_name = "${node.key}" RETURN b.qualified_name, b.name, type(r)` });
        for (const [qn, name, type] of answer?.rows ?? []) if (qn !== placed.find((p) => p.hop === 0)?.key) forward.set(qn, { name, type, from: node.name });
    }
    const leftKeys = new Set(leftFirst.map((p) => p.key)), rightKeys = new Set(rightFirst.map((p) => p.key));
    const into = (key, targets) => twoLayers.edges.some((edge) => edge.from === key && targets.has(edge.to));
    const outOf = (key, sources) => twoLayers.edges.some((edge) => edge.to === key && sources.has(edge.from));
    const leftOuter = placed.filter((p) => !p.mixed && p.side === -1 && p.hop >= 2);
    const rightOuter = placed.filter((p) => !p.mixed && p.side === 1 && p.hop >= 2);
    const calleesOnLeft = leftOuter.filter((p) => forward.has(p.key) && !into(p.key, leftKeys)).map((p) => `${p.name} (${forward.get(p.key).type} from ${forward.get(p.key).from})`);
    const wrongLeft = leftOuter.filter((p) => !into(p.key, leftKeys)).map((p) => p.name);
    const wrongRight = rightOuter.filter((p) => !outOf(p.key, rightKeys)).map((p) => p.name);
    const mixed = placed.filter((p) => p.mixed);
    const lowestColumn = Math.min(...placed.filter((p) => !p.mixed).map((p) => p.y));
    const bandCount = Number(/Mixed directions · (\d+) nodes?/.exec(twoLayers.band?.text ?? '')?.[1] ?? NaN);
    const typicalCallees = ['len', 'str', 'print', 'list', 'dict', 'pop'].filter((name) => leftOuter.some((p) => p.name === name));
    await check('K5', `At 2 layers no callee of a caller sits in an incoming column: ${leftOuter.length} outer incoming nodes all reach the root through incoming relationships, ${rightOuter.length} outer outgoing nodes through outgoing ones`,
        forward.size > 0 && calleesOnLeft.length === 0 && wrongLeft.length === 0 && wrongRight.length === 0 && typicalCallees.length === 0,
        { firstLeft: leftFirst.map((p) => p.name), forwardOfFirstLeft: forward.size, calleesOnLeft, wrongLeft, wrongRight, typicalCallees,
            leftOuter: leftOuter.map((p) => p.name), rightOuter: rightOuter.map((p) => p.name) });
    await check('K5', `Nodes reached through mixed directions stand in the labelled band below the columns ("${twoLayers.band?.text ?? ''}"), and the hint counts them`,
        mixed.length > 0 && bandCount === mixed.length && twoLayers.bandInfo?.count === mixed.length && mixed.every((p) => p.y < lowestColumn)
        && twoLayers.band?.inCanvas === true && twoLayers.bandOverlaps.length === 0
        && twoLayers.hint.includes(`${mixed.length} nodes reached through both directions, such as a callee of a caller, stand in the band below`),
        { mixed: mixed.length, bandText: twoLayers.band?.text, bandCount, bandInfo: twoLayers.bandInfo, lowestColumn, highestBand: Math.max(...mixed.map((p) => p.y)),
            bandOverlaps: twoLayers.bandOverlaps, hint: twoLayers.hint, bandNames: mixed.map((p) => p.name) });

    // Auf das Band zoomen: dort stehen Namen und Kantenschilder mit Pfeil, und die Ueberschrift bleibt frei.
    await page.getByRole('button', { name: 'fit view' }).click().catch(() => {});
    await wait(1500);
    await zoomAt(page, { x: twoLayers.bandInfo?.x ?? 0, y: (twoLayers.bandInfo?.y ?? 0) - 60 }, 5);
    const bandView = await hierarchyLayout(page);
    await shot(page, 'K5', 'hierarchy-two-layers-band', `Zoomed in on the band: "${bandView.band?.text ?? ''}", ${bandView.edgeLabels.length} edge labels, ${bandView.overlaps.length} overlaps, band heading overlaps ${bandView.bandOverlaps.length}`);
    const unlabelled = [...zoomed.edgeLabels, ...bandView.edgeLabels].filter((text) => !/[→←↑↓]/.test(text) || /×/.test(text));
    await check('K5', 'At 2 layers every edge label names its type and carries an arrow from source to target; none merges a fan without direction',
        zoomed.edgeLabels.length > 0 && bandView.edgeLabels.length > 0 && unlabelled.length === 0 && bandView.overlaps.length === 0 && bandView.bandOverlaps.length === 0,
        { zoomed: zoomed.edgeLabels, band: bandView.edgeLabels, unlabelled, overlaps: bandView.overlaps, bandOverlaps: bandView.bandOverlaps });

    // Ein Layer zurueck: kein Band, der Hinweis sagt nichts von einem Band.
    await page.getByRole('button', { name: 'Remove graph layer' }).click();
    await settled(page);
    await wait(1500);
    const oneLayer = await hierarchyLayout(page);
    await check('K5', 'Back at 1 layer there is no band and the hint describes only the columns, so the text is right for each depth',
        oneLayer.band === null && oneLayer.bandInfo === null && !/band/.test(oneLayer.hint) && /incoming relationships on the left, the root in the middle, outgoing on the right/.test(oneLayer.hint),
        { hint: oneLayer.hint, band: oneLayer.band });

    /*
     * Ueber der Namensgrenze: Aggregate bei zwei Ebenen (rund 600 Knoten, 34
     * direkte Nachbarn) behaelt die Namen der Wurzel und ihrer Nachbarn, und
     * die Notiz im Bild sagt, wie man die uebrigen bekommt. call_command hat
     * schon bei einer Ebene rund 540 direkte Nachbarn: keine Namen, und die
     * Notiz sagt, dass nur eine engere Spur hilft.
     */
    await select(page, 'Aggregate');
    await settled(page);
    if (await page.locator('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"][data-active="true"]').count() === 0) {
        await page.locator('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]').click();
    }
    await expand(page);
    await wait(3000);
    const large = await hierarchyLayout(page);
    const largeCount = await countText(page);
    const neighbours = large.placements.filter((p) => p.hop === 1);
    await shot(page, 'K5', 'hierarchy-aggregate-two-layers', `Aggregate at 2 layers (${largeCount}): names "${large.namesMode}", ${large.names} names drawn, note "${large.key ?? ''}"`);
    await zoomAt(page, { x: 0, y: 0 }, 5);
    const largeZoomed = await hierarchyLayout(page);
    await shot(page, 'K5', 'hierarchy-aggregate-zoomed', `Aggregate zoomed on the root: ${largeZoomed.names} names, ${largeZoomed.edgeLabels.length} edge labels at the root, ${largeZoomed.overlaps.length} overlaps`);
    const named = new Set(large.nameTexts);
    await check('K5', 'Above 150 nodes the root and its direct neighbours keep their names (and their edge labels when zoomed), and a visible note says how to see the rest',
        Number(/^([\d.,]+) nodes/.exec(largeCount)?.[1].replace(/[.,]/g, '') ?? 0) > 150 && large.namesMode === 'neighbours' && neighbours.length > 0 && neighbours.length <= 150
        && neighbours.every((p) => named.has(p.name)) && large.names <= neighbours.length + 1
        && /names for the root and its \d+ direct neighbours only/.test(large.key ?? '') && /Remove layers or trace fewer edge types/.test(large.key ?? '')
        && /only the root and its direct neighbours carry names/.test(large.hint) && largeZoomed.edgeLabels.length > 0 && largeZoomed.overlaps.length === 0,
        { count: largeCount, namesMode: large.namesMode, neighbours: neighbours.length, namesDrawn: large.names, note: large.key, hint: large.hint,
            zoomedEdgeLabels: largeZoomed.edgeLabels.slice(0, 12), overlaps: largeZoomed.overlaps });

    await select(page, 'call_command');
    await settled(page);
    if (await page.locator('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"][data-active="true"]').count() === 0) {
        await page.locator('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]').click();
    }
    await wait(3000);
    const hub = await hierarchyLayout(page);
    const hubCount = await countText(page);
    await shot(page, 'K5', 'hierarchy-call-command-hub', `call_command at 1 layer (${hubCount}): names "${hub.namesMode}", ${hub.names} names drawn, note "${hub.key ?? ''}"`);
    await zoomAt(page, { x: 0, y: 0 }, 4);
    await shot(page, 'K5', 'hierarchy-call-command-zoomed', 'call_command zoomed on the root: its name and the names of its outgoing calls, with typed edge labels');
    const hubOutgoing = hub.placements.filter((p) => p.hop === 1 && p.side === 1);
    const hubNamed = new Set(hub.nameTexts);
    await check('K5', 'A root with more direct neighbours than the name budget keeps its own name and those of the side that fits (the outgoing calls), and says so in the picture, with advice that fits one layer',
        hub.namesMode === 'none' && hubNamed.has('call_command') && hubOutgoing.length > 0 && hubOutgoing.every((p) => hubNamed.has(p.name)) && hub.names === hubOutgoing.length + 1
        && new RegExp(`names for the root and its ${hubOutgoing.length} outgoing neighbours only \\(up to 150 names\\)\\. Trace one direction or fewer edge types to see the rest`).test(hub.key ?? '')
        && /so only the root and its \d+ outgoing neighbours carry names; trace one direction or fewer edge types to see the rest/.test(hub.hint) && !/remove/i.test(hub.key ?? ''),
        { count: hubCount, namesMode: hub.namesMode, names: hub.names, outgoing: hubOutgoing.map((p) => p.name), named: hub.nameTexts, note: hub.key, hint: hub.hint });
}

/** Zum Weltpunkt `world` hinzoomen: Zeiger dorthin, dann `steps` Mausrad-Schritte (die Szene zoomt zum Zeiger). */
async function zoomAt(page, world, steps) {
    const box = await canvasBox(page);
    const [at] = await page.evaluate((point) => globalThis.__atlasGalaxyFit?.project?.([{ x: point.x, y: point.y, z: 0 }]) ?? [], world);
    const x = at ? box.x + Math.min(Math.max(at.x, 20), box.width - 20) : box.x + box.width / 2;
    const y = at ? box.y + Math.min(Math.max(at.y, 20), box.height - 20) : box.y + box.height / 2;
    await page.mouse.move(x, y);
    for (let step = 0; step < steps; step += 1) { await page.mouse.wheel(0, -300); await wait(200); }
    // Den Zeiger aus dem Bild nehmen, damit keine Karte eines Knotens ueber der Messung liegt.
    await page.mouse.move(box.x + box.width - 4, box.y - 30);
    await wait(2000);
}

/* ------------------------------------------------------------------ */
/* K12: die Namen im Mini-Galaxy von Explore liegen nicht aufeinander     */

async function openWorkflowFile(page) {
    await page.goto(`${ORIGIN}/?project=${encodeURIComponent(PROJECT)}&workspace=explore`, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.atlas-shell', { timeout: 30000 });
    await wait(1500);
    for (const path of ['.github', '.github/workflows']) {
        const row = page.locator(`.atlas-tree-row[data-path="${path}"]`).first();
        await row.waitFor({ timeout: 30000 });
        for (let attempt = 0; attempt < 4 && (await row.getAttribute('data-expanded')) !== 'true'; attempt++) { await row.click(); await wait(1200); }
    }
    await page.locator('.atlas-tree-row[data-path=".github/workflows/new_contributor_pr.yml"]').first().click();
    await page.waitForFunction(() => /New contributor message/.test(document.body.innerText), null, { timeout: 30000 }).catch(() => {});
    await page.waitForFunction(() => document.querySelectorAll('.atlas-galaxy [data-testid="atlas-galaxy-root-marker"]').length > 1, null, { timeout: 30000 }).catch(() => {});
    await wait(3000);
}

/** Alle Beschriftungen im Mini-Galaxy, im DOM gemessen, dazu die Namen der Szene; Paare, die sich ueberlagern. */
async function miniLabels(page) {
    return page.evaluate(() => {
        const panel = document.querySelector('.atlas-galaxy');
        const canvas = panel?.querySelector('canvas')?.getBoundingClientRect();
        const shown = (el) => { const style = getComputedStyle(el); return style.visibility !== 'hidden' && style.display !== 'none' && el.getBoundingClientRect().width > 0; };
        const dom = [...(panel?.querySelectorAll('.atlas-galaxy-root-marker b, .atlas-galaxy-path-node b, .atlas-galaxy-path-label, [data-testid="atlas-hierarchy-edge-label"], .atlas-hierarchy-band-label') ?? [])]
            .filter(shown).map((el) => { const r = el.getBoundingClientRect(); return { text: el.textContent?.trim() ?? '', left: r.left, right: r.right, top: r.top, bottom: r.bottom }; });
        const fit = globalThis.__atlasGalaxyFit;
        const sprites = (globalThis.__atlasGalaxy?.labelBoxes ?? []).map((box) => {
            const [a, b] = typeof fit?.project === 'function' ? fit.project([{ x: box.x - box.width / 2, y: box.y + box.height / 2, z: 0 }, { x: box.x + box.width / 2, y: box.y - box.height / 2, z: 0 }]) : [];
            return a && b && canvas ? { text: box.name, left: canvas.left + Math.min(a.x, b.x), right: canvas.left + Math.max(a.x, b.x), top: canvas.top + Math.min(a.y, b.y), bottom: canvas.top + Math.max(a.y, b.y) } : null;
        }).filter(Boolean);
        const all = [...dom, ...sprites];
        const hit = (a, b) => !(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top);
        const overlaps = [];
        for (let i = 0; i < all.length; i += 1) for (let j = i + 1; j < all.length; j += 1) if (hit(all[i], all[j])) overlaps.push(`${all[i].text} x ${all[j].text}`);
        const outside = canvas ? all.filter((r) => r.left < canvas.left - 0.5 || r.right > canvas.right + 0.5 || r.top < canvas.top - 0.5 || r.bottom > canvas.bottom + 0.5).map((r) => r.text) : [];
        // Der Ring einer Marke sitzt auf ihrem Anker (die Marke selbst ist 0 x 0 px gross).
        const markers = [...(panel?.querySelectorAll('[data-testid="atlas-galaxy-root-marker"]') ?? [])].map((el) => {
            const r = el.getBoundingClientRect();
            return { name: el.querySelector('b')?.textContent ?? '', slot: el.getAttribute('data-name-slot'),
                ringInCanvas: Boolean(canvas && r.left >= canvas.left && r.left <= canvas.right && r.top >= canvas.top && r.top <= canvas.bottom) };
        });
        const round = (r) => ({ text: r.text, left: Math.round(r.left), top: Math.round(r.top), right: Math.round(r.right), bottom: Math.round(r.bottom) });
        return { labels: all.map(round), overlaps, outside, markers, canvas: canvas ? { width: Math.round(canvas.width), height: Math.round(canvas.height) } : null,
            scope: panel?.querySelector('.atlas-graph-scope-name')?.textContent ?? '', count: panel?.querySelector('.atlas-graph-scope-count')?.textContent ?? '' };
    });
}

async function checkK12(page) {
    await section(page, 'K12', 'Explore mini-Galaxy of .github/workflows/new_contributor_pr.yml: no two labels overlap, as in the main Galaxy');
    await openWorkflowFile(page);
    const wanted = ['permissions', 'new_contributor_pr.yml', '.github/workflows/new_contributor_pr.yml', 'jobs'];
    const views = [];
    const look = async (label, caption) => {
        const measured = await miniLabels(page);
        views.push({ label, ...measured });
        await shot(page, 'K12', label, `${caption}: ${measured.labels.length} labels, ${measured.overlaps.length} overlapping pairs`);
        const panel = await page.locator('.atlas-galaxy').first().boundingBox();
        if (panel) {
            const file = `${label}-mini.png`;
            await page.screenshot({ path: join(OUT, 'K12', file), clip: panel });
            await appendFile(join(OUT, 'K12', 'index.md'), `- \`${file}\`: the mini-Galaxy alone (${Math.round(panel.width)} x ${Math.round(panel.height)} px)\n`);
        }
    };
    await look('chat-closed', 'Chat closed, the YAML file open');
    await page.getByRole('button', { name: 'Open chat' }).first().click().catch(() => {});
    await wait(2500);
    await look('chat-open', 'Chat open as in the hand test (17:39/17:40)');
    await page.getByRole('button', { name: 'Hide chat' }).first().click().catch(() => {});
    await wait(800);
    // Kein Name ist verloren: jede Marke, deren Ring im Bild liegt, traegt ihren Namen; bei offenem Chat (der Lage des Handtests) alle vier.
    const lost = views.flatMap((view) => view.markers.filter((marker) => marker.ringInCanvas && marker.slot === 'hidden').map((marker) => `${view.label}: ${marker.name}`));
    const handTest = views.find((view) => view.label === 'chat-open');
    const pass = views.length === 2 && views.every((view) => view.overlaps.length === 0 && view.outside.length === 0 && view.markers.length > 1)
        && lost.length === 0 && wanted.every((name) => handTest?.labels.some((entry) => entry.text === name));
    await check('K12', 'The labels of the mini-Galaxy do not overlap (0 overlapping label rects in the DOM), stay inside it, every root in view keeps its name, and with the chat open the four names of the hand test are all shown',
        pass, { lost, views });
}

/* ------------------------------------------------------------------ */

const CHECKS = { K9: checkK9, K2: checkK2, K8: checkK8, K3: checkK3, K13: checkK13, K6: checkK6, K5: checkK5, K12: checkK12 };

await mkdir(OUT, { recursive: true });
const context = await chromium.launchPersistentContext(PROFILE, {
    headless: !HEADED, ...(HEADED ? {} : { channel: 'chromium' }), viewport: VIEWPORT, deviceScaleFactor: 2,
    args: ['--enable-unsafe-webgpu', '--ignore-gpu-blocklist'],
});
await context.addInitScript(pageProbe);
const page = context.pages()[0] ?? await context.newPage();
const pageErrors = [];
page.on('pageerror', (error) => { pageErrors.push(error.message); console.error(`[pageerror] ${error.message}`); });
for (const id of ONLY) {
    const run = CHECKS[id];
    if (!run) continue;
    try { await run(page); } catch (error) {
        console.error(`[${id}] ABORT ${error?.stack ?? error}`);
        await check(id, 'check ran to completion', false, { error: String(error?.message ?? error).split('\n')[0] }).catch(() => {});
    }
}
await context.close();
const passed = results.filter((row) => row.pass).length;
await writeFile(join(OUT, 'report.json'), JSON.stringify({ origin: ORIGIN, project: PROJECT, passed, total: results.length, results, pageErrors }, null, 2));
console.log(`${passed}/${results.length} PASS${pageErrors.length ? `, page errors: ${pageErrors.length}` : ''}`);
if (passed !== results.length) process.exitCode = 1;
