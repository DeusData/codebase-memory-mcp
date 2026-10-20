#!/usr/bin/env node
/*
 * Browserpruefung zu K27 (verification/call-feedback-2026-10-02/KORREKTURPLAN.md):
 * ein Zurueck und ein Vor fuer ganz Architecture, mit demselben Verlaufsmodell
 * wie Galaxy (K2).
 *
 *   node tools/handtest-fixes-k27.mjs --origin http://127.0.0.1:4375 \
 *        --out /tmp/handtest/k27 [--project django-demo] [--control cbm] [--profile /tmp/profile]
 *
 * Der Ablauf auf django-demo: Overview, Bereich "django" oeffnen, Routes,
 * Endpoints, Gruppe "/edit" oeffnen ("Show these N routes"), System
 * structure, eine Gruppe aufklappen, Behavior (der vorgeschlagene Start ist
 * kein eigener Schritt), Start "handle · .../loaddata.py" waehlen, einem
 * Aufruf folgen. Dann Schritt fuer Schritt zurueck bis zum Anfang und wieder
 * vor; an jedem Schritt werden Untertab, Brotkruemel, Routen-Perspektive und
 * -Filter, System-Fokus und aufgeklappte Gruppen, Behavior-Start und die
 * Tooltips von Zurueck und Vor gemessen. Danach Alt+Links/Rechts (auch beim
 * Tippen im Filter, wo sie nichts tun duerfen), ein Sprung aus "Recent",
 * eine neue Navigation nach Zurueck (Vor faellt weg) und Galaxy als eigener
 * Arbeitsbereich. Nach der Pruefung (Review zu K27) dazu: Plan oder 3D in
 * System structure und in Behavior als Schritt, der Stand in einer Aufrufkette
 * (Pfad und Operation), der nach Zurueck und Vor wieder da ist, Text im
 * Routen-Filter, der beim sofortigen Zurueck nicht verloren geht, und an jedem
 * Schritt kein zweites "← Back" mehr in den Ansichten. Zum Schluss der
 * Projektwechsel nach cbm (frischer Verlauf).
 *
 * Jeder Schritt schreibt ein Bild <out>/NN-name.png; <out>/index.md fuehrt
 * jedes Bild mit dem, was es zeigt, und dem Messwert, <out>/report.json alle
 * Pruefungen. Der Browser laeuft ohne Fenster. Das Skript startet keinen
 * Server und schreibt nichts in das Protokoll des Servers: POST /api/ui-log
 * wird im Browser beantwortet und navigator.sendBeacon bleibt in der Seite.
 */

import { chromium } from 'playwright';
import { mkdir, writeFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { prepareHandtestProfile } from './lib/handtest-profile.mjs';

const argv = process.argv.slice(2);
const arg = (name, fallback) => {
    const index = argv.indexOf(`--${name}`);
    if (index < 0) return fallback;
    const value = argv[index + 1];
    return value === undefined || value.startsWith('--') ? true : value;
};
const ORIGIN = String(arg('origin', 'http://127.0.0.1:4375')).replace(/\/$/, '');
const OUT = resolve(String(arg('out', 'verification/handtest-fixes/k27')));
const PROFILE = resolve(String(arg('profile', join(OUT, '..', 'profile-k27'))));
const PROJECT = String(arg('project', 'django-demo'));
const CONTROL = String(arg('control', 'cbm'));
const VIEWPORT = { width: 1600, height: 1000 };

const wait = (ms) => new Promise((done) => setTimeout(done, ms));
const checks = [];
const images = [];
let counter = 0;

function check(id, title, pass, measured) {
    checks.push({ id, title, pass: Boolean(pass), measured });
    console.log(`${pass ? 'PASS' : 'FAIL'}  ${id}  ${title}  ${JSON.stringify(measured)}`);
}

async function shot(page, name, note, measured) {
    counter += 1;
    const file = `${String(counter).padStart(2, '0')}-${name}.png`;
    // Ein Klick tief in der Ansicht (Next, ein Pfad) rollt den Container, der ihn haelt; das Bild zeigt die Reiter und den Verlauf.
    await page.evaluate(() => { window.scrollTo(0, 0); for (const element of document.querySelectorAll('*')) if (element.scrollTop > 0) element.scrollTop = 0; });
    await wait(150);
    await page.screenshot({ path: join(OUT, file) });
    images.push({ file, note, measured });
}

/* ------------------------------------------------------------------ */
/* Was die Seite gerade zeigt                                           */

async function state(page) {
    return page.evaluate(() => {
        const root = document.querySelector('.atlas-architecture');
        if (!root) return null;
        const text = (element) => element?.textContent?.replace(/\s+/g, ' ').trim() ?? null;
        const history = root.querySelector('[role="group"][aria-label="Architecture history"]');
        const button = (label) => { const element = history?.querySelector(`button[aria-label="${label}"]`); return element ? { disabled: element.disabled, title: element.getAttribute('title') } : null; };
        const location = root.querySelector('[aria-label="Architecture location"]');
        const select = root.querySelector('select[aria-label="Behavior entry point"]');
        const destination = root.querySelector('select[aria-label="Behavior destination"]');
        const systemLabels = [...root.querySelectorAll('.system-scene-node-label')];
        const paths = [...root.querySelectorAll('[aria-label="Indexed paths"] button')];
        return {
            view: root.querySelector('.atlas-arch-tab[aria-pressed="true"]')?.getAttribute('data-view') ?? null,
            trail: location ? [...location.children].filter((element) => element.tagName === 'BUTTON' || (element.tagName === 'SPAN' && element.textContent !== '/')).map((element) => element.textContent) : null,
            camera: text(root.querySelector('[aria-label="Map camera"] button[aria-pressed="true"]')),
            perspective: text(root.querySelector('[aria-label="Routes perspective"] button[aria-pressed="true"]')),
            filter: root.querySelector('input[type="search"]')?.value ?? null,
            routeChips: root.querySelectorAll('button.architecture-node-label[data-node-id^="route"]').length,
            focus: systemLabels.filter((element) => element.dataset.focus === 'true' && element.dataset.presentation === 'system').map((element) => text(element.querySelector('strong'))),
            expanded: systemLabels.filter((element) => element.dataset.expanded === 'true').map((element) => text(element.querySelector('strong'))),
            heading: text(root.querySelector('.behavior-heading h2')),
            start: select ? text(select.selectedOptions[0]) : null,
            destination: destination ? text(destination.selectedOptions[0]) : null,
            systemCamera: text(root.querySelector('[aria-label="System camera"] button[aria-pressed="true"]')),
            behaviorCamera: text(root.querySelector('[aria-label="Behavior camera"] button[aria-pressed="true"]')),
            chain: text(root.querySelector('[aria-label="Walk the call chain"] span')),
            path: paths.length ? paths.findIndex((element) => element.getAttribute('aria-pressed') === 'true') + 1 : null,
            /*
             * Das "← Back", das System structure und Behavior frueher in der eigenen Leiste hatten. Das gemeinsame
             * Zurueck traegt seit dem Handtest vom 2026-10-04 (A1) dieselben Worte wie in Galaxy und zaehlt nicht mit.
             */
            inViewBack: [...root.querySelectorAll('button')].filter((element) => element.textContent?.trim() === '← Back' && !history?.contains(element)).length,
            back: button('Back'), forward: button('Forward'),
            position: history?.getAttribute('data-position') ?? null,
            recent: [...(history?.querySelectorAll('[aria-label="Recently visited places"] li button') ?? [])].map((element) => ({
                name: text(element.querySelector('strong')), detail: text(element.querySelector('span')) ?? '', current: element.getAttribute('aria-current') === 'true' })),
        };
    });
}

/** Wartet, bis die Seite den erwarteten Ort zeigt; liefert den letzten Messwert. */
async function settle(page, expected, timeout = 90000) {
    const started = Date.now();
    let last = null;
    while (Date.now() - started < timeout) {
        last = await state(page);
        if (last && matches(last, expected).length === 0) { await wait(900); return await state(page); }
        await wait(400);
    }
    return last;
}

/** Was vom Erwarteten abweicht. Ein fehlendes Feld im Erwarteten wird nicht geprueft. */
function matches(actual, expected) {
    const misses = [];
    for (const [field, value] of Object.entries(expected)) {
        const got = actual?.[field];
        const same = value instanceof RegExp ? typeof got === 'string' && value.test(got) : JSON.stringify(got) === JSON.stringify(value);
        if (!same) misses.push(`${field}: ${JSON.stringify(got)} != ${value instanceof RegExp ? value : JSON.stringify(value)}`);
    }
    return misses;
}

const tab = (page, view) => page.locator(`.atlas-architecture button.atlas-arch-tab[data-view="${view}"]`).first();
const historyButton = (page, label) => page.locator(`.atlas-architecture [aria-label="Architecture history"] button[aria-label="${label}"]`).first();

/* ------------------------------------------------------------------ */
/* Ablauf                                                               */

const runProfile = await prepareHandtestProfile(PROFILE);
await mkdir(OUT, { recursive: true });
const context = await chromium.launchPersistentContext(runProfile, {
    headless: true, viewport: VIEWPORT, deviceScaleFactor: 2,
    args: ['--enable-unsafe-webgpu', '--ignore-gpu-blocklist'],
});
await context.addInitScript(() => {
    try { localStorage.setItem('cbm.workspace.setup', 'done'); } catch { /* ohne Speicher geht es auch */ }
    window.__beacons = [];
    navigator.sendBeacon = (url, data) => { window.__beacons.push({ url: String(url), data }); return true; };
});
await context.route('**/api/ui-log', async (route) => {
    if (route.request().method() !== 'POST') return route.continue();
    return route.fulfill({ status: 200, contentType: 'application/json', body: '{"accepted":0}' });
});
const page = context.pages()[0] ?? await context.newPage();
const pageErrors = [];
page.on('pageerror', (error) => { pageErrors.push(error.message); console.error(`[pageerror] ${error.message}`); });

/** Die Schritte des Hinwegs: was jeder zeigt und wie sein Tooltip ihn nennt. */
const steps = [];
async function record(name, note, expectedHere, label) {
    // An jedem Schritt: ein Zurueck fuer ganz Architecture, keines mehr in den Ansichten.
    const expected = { ...expectedHere, inViewBack: 0 };
    const measured = await settle(page, expected);
    const misses = matches(measured, expected);
    steps.push({ name, expected, label });
    check(`F${steps.length - 1}`, `Hinweg ${name}: ${note}`, misses.length === 0, { misses, measured });
    await shot(page, `hin-${name}`, `Hinweg, Schritt ${steps.length - 1}: ${note}`, measured);
    return measured;
}

try {
    await page.goto(`${ORIGIN}/?project=${encodeURIComponent(PROJECT)}&workspace=architecture`, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.atlas-shell', { timeout: 30000 });
    await page.waitForFunction(() => document.querySelectorAll('.atlas-architecture button.architecture-node-label').length > 0, null, { timeout: 180000 });
    await wait(1500);
    let measured = await record('overview', 'Overview an der Wurzel, Zurueck und Vor gesperrt',
        { view: 'overview', trail: [PROJECT], position: '1/1' }, 'Overview');
    check('C0', 'Am Anfang: Zurueck und Vor gesperrt, mit Tooltip', measured?.back?.disabled && measured?.forward?.disabled
        && measured.back.title === 'Nothing to go back to yet' && measured.forward.title === 'Nothing to go forward to', { back: measured?.back, forward: measured?.forward });

    // 1. Bereich django oeffnen
    await page.locator('.atlas-architecture button.architecture-node-label', { hasText: /^\W*django\s*$/ }).first().click();
    await wait(600);
    await page.locator('.atlas-architecture').getByRole('button', { name: /^Open area/ }).first().click();
    await record('django', 'Bereich django geoeffnet, Brotkruemel django-demo / django', { view: 'overview', trail: [PROJECT, 'django'] }, 'Overview · django');

    // 2. Routes, Service map
    await tab(page, 'routes').click();
    await record('routes', 'Routes, Service map', { view: 'routes', perspective: 'Service map', filter: '' }, 'Routes · Service map');

    // 3. Endpoints
    await page.locator('.atlas-architecture [aria-label="Routes perspective"] button', { hasText: 'Endpoints' }).click();
    await page.waitForFunction(() => document.querySelectorAll('.atlas-architecture button.architecture-node-label[data-node-id="route-group:/edit"]').length > 0, null, { timeout: 120000 });
    const groups = await record('endpoints', 'Routes, Endpoints mit Routengruppen', { view: 'routes', perspective: 'Endpoints', filter: '' }, 'Routes · Endpoints');
    const allRoutes = groups?.routeChips ?? 0;

    // 4. Gruppe /edit oeffnen
    await page.locator('.atlas-architecture button.architecture-node-label[data-node-id="route-group:/edit"]').click();
    await wait(600);
    const showRoutes = page.locator('.atlas-architecture button', { hasText: /^Show these \d+ routes/ }).first();
    const showText = (await showRoutes.textContent())?.trim();
    await showRoutes.click();
    measured = await record('edit', `Gruppe /edit geoeffnet (${showText})`, { view: 'routes', perspective: 'Endpoints', filter: '/edit' }, 'Routes · Endpoints · /edit');
    const editRoutes = measured?.routeChips ?? 0;

    // 5. System structure
    await tab(page, 'structure').click();
    await page.waitForFunction(() => document.querySelectorAll('.atlas-architecture .system-scene-node-label[data-kind="group"]').length > 0, null, { timeout: 180000 });
    await record('structure', 'System structure, ganzes System', { view: 'structure', focus: [], expanded: [] }, 'System structure');

    // 6. Eine Gruppe aufklappen (Doppelklick: aufklappen und fokussieren, wie bisher)
    const groupLabels = await page.locator('.atlas-architecture .system-scene-node-label[data-kind="group"]').evaluateAll((elements) =>
        elements.map((element) => ({ id: element.dataset.nodeId, name: element.querySelector('strong')?.textContent?.trim() ?? '' })));
    const group = groupLabels.find((item) => item.name === 'django') ?? groupLabels[0];
    await page.locator(`.atlas-architecture .system-scene-node-label[data-node-id="${group.id}"]`).dblclick();
    await record('group', `Gruppe ${group.name} aufgeklappt und fokussiert`, { view: 'structure', focus: [group.name], expanded: [group.name] }, `System structure · ${group.name} · 1 group open`);

    // 7. Behavior: der vorgeschlagene Start fuellt den Schritt, statt einen eigenen abzulegen
    await tab(page, 'behavior').click();
    await page.waitForFunction(() => /What can .+ call\?/.test(document.querySelector('.behavior-heading h2')?.textContent ?? ''), null, { timeout: 180000 });
    measured = await record('behavior', 'Behavior mit dem vorgeschlagenen Start main', { view: 'behavior', heading: 'What can main call?' }, 'Behavior · main');
    check('C7', 'Der automatische Start main ist kein eigener Schritt (Zurueck fuehrt zur System structure)',
        measured?.back?.title === `Back to System structure · ${group.name} · 1 group open (Alt+Left)`, { back: measured?.back, position: measured?.position });

    // 8. Start handle · .../loaddata.py
    const select = page.locator('.atlas-architecture select[aria-label="Behavior entry point"]');
    const options = await select.locator('option').allTextContents();
    const index = options.findIndex((option) => /^handle · .*loaddata\.py$/.test(option.trim()));
    await select.selectOption({ index });
    await page.waitForFunction(() => document.querySelector('.behavior-heading h2')?.textContent === 'What can handle call?', null, { timeout: 180000 });
    await page.waitForFunction(() => document.querySelectorAll('.atlas-architecture .system-scene-node-label[data-node-id^="journey-choice:"]').length > 1, null, { timeout: 120000 });
    measured = await record('handle', `Start ${options[index]?.trim()}`, { view: 'behavior', heading: 'What can handle call?', start: /^handle · .*loaddata\.py$/ }, 'Behavior · handle');

    // 9. Einem Aufruf folgen (Doppelklick)
    const callees = await page.locator('.atlas-architecture .system-scene-node-label[data-node-id^="journey-choice:"]').evaluateAll((elements) =>
        elements.map((element) => ({ id: element.dataset.nodeId, name: element.querySelector('strong')?.textContent?.trim() ?? '' })));
    const handleId = callees[0]?.id;
    const callee = callees.find((item) => item.id !== handleId && item.name && !/^handle$/.test(item.name)) ?? callees[1];
    await page.locator(`.atlas-architecture .system-scene-node-label[data-node-id="${callee.id}"]`).dblclick();
    await record('follow', `Aufruf ${callee.name} gefolgt`, { view: 'behavior', heading: `What can ${callee.name} call?` }, `Behavior · ${callee.name} · followed from handle`);

    const last = steps.length - 1;
    const labels = steps.map((step) => step.label);
    check('C-routes', 'Die Gruppe /edit zeigt weniger Routen als alle Gruppen zusammen', editRoutes > 0 && allRoutes > 0, { allRouteChips: allRoutes, editRouteChips: editRoutes, button: showText });

    // Zurueck Schritt fuer Schritt bis zum Anfang
    for (let at = last - 1; at >= 0; at--) {
        await historyButton(page, 'Back').click();
        const expected = { ...steps[at].expected, back: at > 0 ? { disabled: false, title: `Back to ${labels[at - 1]} (Alt+Left)` } : { disabled: true, title: 'Nothing to go back to yet' },
            forward: { disabled: false, title: `Forward to ${labels[at + 1]} (Alt+Right)` }, position: `${at + 1}/${last + 1}` };
        measured = await settle(page, expected);
        const misses = matches(measured, expected);
        check(`B${at}`, `Zurueck auf ${labels[at]}`, misses.length === 0, { misses, measured });
        await shot(page, `zurueck-${String(at).padStart(2, '0')}-${steps[at].name}`, `Zurueck, Schritt ${at}: ${labels[at]}; Tooltips nennen ${at > 0 ? labels[at - 1] : 'nichts'} und ${labels[at + 1]}`, measured);
    }
    // Und wieder vor bis zum Ende
    for (let at = 1; at <= last; at++) {
        await historyButton(page, 'Forward').click();
        const expected = { ...steps[at].expected, back: { disabled: false, title: `Back to ${labels[at - 1]} (Alt+Left)` },
            forward: at < last ? { disabled: false, title: `Forward to ${labels[at + 1]} (Alt+Right)` } : { disabled: true, title: 'Nothing to go forward to' }, position: `${at + 1}/${last + 1}` };
        measured = await settle(page, expected);
        const misses = matches(measured, expected);
        check(`V${at}`, `Vor auf ${labels[at]}`, misses.length === 0, { misses, measured });
        await shot(page, `vor-${String(at).padStart(2, '0')}-${steps[at].name}`, `Vor, Schritt ${at}: ${labels[at]}`, measured);
    }

    // Alt+Links und Alt+Rechts
    await page.locator('.atlas-architecture .behavior-heading h2').click();
    await page.keyboard.press('Alt+ArrowLeft');
    measured = await settle(page, { ...steps[last - 1].expected, position: `${last}/${last + 1}` });
    check('K-left', 'Alt+Links geht einen Schritt zurueck', matches(measured, { ...steps[last - 1].expected, position: `${last}/${last + 1}` }).length === 0, measured);
    await shot(page, 'alt-links', `Alt+Links: zurueck auf ${labels[last - 1]}`, measured);
    await page.keyboard.press('Alt+ArrowRight');
    measured = await settle(page, { ...steps[last].expected, position: `${last + 1}/${last + 1}` });
    check('K-right', 'Alt+Rechts geht einen Schritt vor', matches(measured, { ...steps[last].expected, position: `${last + 1}/${last + 1}` }).length === 0, measured);
    await shot(page, 'alt-rechts', `Alt+Rechts: vor auf ${labels[last]}`, measured);

    // Beim Tippen im Routen-Filter gehoeren die Pfeile dem Feld
    for (let step = 0; step < 5; step++) await historyButton(page, 'Back').click().then(() => wait(250));
    measured = await settle(page, { ...steps[4].expected, position: '5/10' });
    const search = page.locator('.atlas-architecture input[type="search"]');
    await search.focus();
    const typingFocus = await page.evaluate(() => document.activeElement?.getAttribute('type') === 'search');
    await page.keyboard.press('Alt+ArrowLeft');
    await wait(1200);
    const typing = await state(page);
    check('K-typing', 'Alt+Links im Routen-Filter blaettert nicht', typingFocus && typing?.view === 'routes' && typing.filter === '/edit' && typing.position === '5/10', { typingFocus, ...typing });
    await shot(page, 'alt-links-beim-tippen', 'Alt+Links im Filterfeld: Routes · Endpoints · /edit bleibt stehen', typing);
    await page.evaluate(() => (document.activeElement instanceof HTMLElement ? document.activeElement.blur() : undefined));
    for (let step = 0; step < 5; step++) await historyButton(page, 'Forward').click().then(() => wait(250));
    measured = await settle(page, { ...steps[last].expected, position: '10/10' });

    // Ein Sprung aus "Recent" ist eine neue Navigation
    await page.locator('.atlas-architecture details.atlas-arch-recent > summary').click();
    await wait(400);
    measured = await state(page);
    await shot(page, 'recent-offen', 'Recent geoeffnet: die zuletzt besuchten Orte, der aktuelle ausgegraut', measured);
    // Begrenzt auf 8 Orte: nach Hin- und Rueckweg stehen die acht zuletzt besuchten darin, der aktuelle zuerst.
    check('R-list', 'Recent listet hoechstens 8 Orte, neueste zuerst, den aktuellen markiert',
        measured?.recent?.length === 8 && measured.recent[0]?.current && measured.recent.some((item) => item.name === 'Routes' && item.detail === 'Endpoints · /edit'), measured?.recent);
    await page.locator('.atlas-architecture [aria-label="Recently visited places"] li button', { has: page.locator('span', { hasText: /^Endpoints · \/edit$/ }) }).first().click();
    const jumped = { view: 'routes', perspective: 'Endpoints', filter: '/edit', forward: { disabled: true, title: 'Nothing to go forward to' },
        back: { disabled: false, title: `Back to ${labels[last]} (Alt+Left)` }, position: `${last + 2}/${last + 2}` };
    measured = await settle(page, jumped);
    check('R-jump', 'Recent: Sprung nach Routes · Endpoints · /edit als neuer Schritt', matches(measured, jumped).length === 0, { misses: matches(measured, jumped), measured });
    await shot(page, 'recent-sprung', 'Nach dem Sprung aus Recent: Routes · Endpoints · /edit, Vor gesperrt', measured);

    // Eine neue Navigation nach Zurueck verwirft den Vorwaertszweig
    await historyButton(page, 'Back').click();
    measured = await settle(page, { ...steps[last].expected, forward: { disabled: false, title: 'Forward to Routes · Endpoints · /edit (Alt+Right)' } });
    await shot(page, 'zurueck-vor-neuer-navigation', `Zurueck auf ${labels[last]}, Vor nennt Routes · Endpoints · /edit`, measured);
    await tab(page, 'hotspots').click();
    const dropped = { view: 'hotspots', forward: { disabled: true, title: 'Nothing to go forward to' }, back: { disabled: false, title: `Back to ${labels[last]} (Alt+Left)` } };
    measured = await settle(page, dropped);
    check('N-drop', 'Neue Navigation nach Zurueck: Vor faellt weg', matches(measured, dropped).length === 0, { misses: matches(measured, dropped), measured });
    await shot(page, 'neue-navigation', 'Hotspots nach Zurueck: der Vorwaertszweig ist verworfen', measured);

    // Galaxy hat einen eigenen Verlauf; Alt+Pfeile wirken nur im aktiven Arbeitsbereich
    const before = measured?.position;
    await page.locator('[data-workspace-tab="galaxy"]').first().click();
    await wait(2500);
    // Der Fokus verlaesst die Reiterleiste: dort sind die Pfeile ihre eigene Tastenfuehrung zwischen den Arbeitsbereichen.
    await page.evaluate(() => (document.activeElement instanceof HTMLElement ? document.activeElement.blur() : undefined));
    await page.keyboard.press('Alt+ArrowLeft');
    await wait(800);
    const workspace = await page.locator('.atlas-shell').first().getAttribute('data-workspace');
    await shot(page, 'galaxy-alt-links', `Galaxy aktiv (${workspace}), Alt+Links: Architecture bleibt unberuehrt`, null);
    await page.locator('[data-workspace-tab="architecture"]').first().click();
    await wait(1500);
    measured = await state(page);
    check('G-separate', 'Alt+Links in Galaxy aendert den Architecture-Verlauf nicht', workspace === 'galaxy' && measured?.view === 'hotspots' && measured.position === before, { workspace, before, after: measured?.position, view: measured?.view });

    // Plan oder 3D in System structure ist ein Schritt, den Zurueck wiederherstellt
    const structureLabel = `System structure · ${group.name} · 1 group open`;
    await tab(page, 'structure').click();
    await page.waitForFunction(() => document.querySelectorAll('.atlas-architecture .system-scene-node-label[data-kind="group"]').length > 0, null, { timeout: 180000 });
    measured = await settle(page, { view: 'structure', focus: [group.name], systemCamera: '3D' });
    await page.locator('.atlas-architecture [aria-label="System camera"] button', { hasText: /^Plan$/ }).click();
    const structurePlan = { view: 'structure', systemCamera: 'Plan', focus: [group.name], inViewBack: 0, back: { disabled: false, title: `Back to ${structureLabel} (Alt+Left)` } };
    measured = await settle(page, structurePlan);
    check('S-plan', 'System structure: Plan ist ein eigener Schritt, Zurueck nennt die 3D-Ansicht', matches(measured, structurePlan).length === 0, { misses: matches(measured, structurePlan), measured });
    await shot(page, 'structure-plan', `System structure in Plan; Zurueck nennt ${structureLabel}`, measured);
    await tab(page, 'overview').click();
    const leftPlan = { view: 'overview', back: { disabled: false, title: `Back to ${structureLabel} · Plan (Alt+Left)` } };
    measured = await settle(page, leftPlan);
    check('S-plan-label', 'Overview danach: Zurueck nennt System structure mit Plan', matches(measured, leftPlan).length === 0, { misses: matches(measured, leftPlan), measured });
    await historyButton(page, 'Back').click();
    const backPlan = { view: 'structure', systemCamera: 'Plan', focus: [group.name], expanded: [group.name], inViewBack: 0 };
    measured = await settle(page, backPlan);
    check('S-plan-back', 'Zurueck: System structure kommt in Plan wieder, mit Fokus und Gruppe', matches(measured, backPlan).length === 0, { misses: matches(measured, backPlan), measured });
    await shot(page, 'structure-plan-zurueck', 'Zurueck auf System structure: wieder in Plan, Fokus und Gruppe wie vorher', measured);
    await historyButton(page, 'Back').click();
    const back3d = { view: 'structure', systemCamera: '3D', focus: [group.name] };
    measured = await settle(page, back3d);
    check('S-3d-back', 'Noch einmal Zurueck: System structure in 3D', matches(measured, back3d).length === 0, { misses: matches(measured, back3d), measured });
    await shot(page, 'structure-3d-zurueck', 'Noch einmal Zurueck: System structure in 3D', measured);

    // Behavior: Pfad, Operation in der Aufrufkette und Plan oder 3D gehoeren zum Eintrag
    await tab(page, 'behavior').click();
    await page.waitForFunction(() => /What can .+ call\?/.test(document.querySelector('.behavior-heading h2')?.textContent ?? ''), null, { timeout: 180000 });
    await page.waitForFunction(() => [...document.querySelectorAll('select[aria-label="Behavior entry point"] option')].some((option) => /^handle · .*loaddata\.py$/.test(option.textContent.trim())), null, { timeout: 180000 });
    const startOptions = await select.locator('option').allTextContents();
    await select.selectOption({ index: startOptions.findIndex((option) => /^handle · .*loaddata\.py$/.test(option.trim())) });
    await page.waitForFunction(() => document.querySelector('.behavior-heading h2')?.textContent === 'What can handle call?', null, { timeout: 180000 });
    await page.waitForFunction(() => document.querySelectorAll('.atlas-architecture select[aria-label="Behavior destination"] option').length > 1, null, { timeout: 180000 });
    const destination = page.locator('.atlas-architecture select[aria-label="Behavior destination"]');
    const destinations = await destination.locator('option').evaluateAll((elements) => elements.map((element) => ({ value: element.value, text: element.textContent?.trim() ?? '' })));
    const reach = destinations.find((option) => /^CommandError · /.test(option.text)) ?? destinations[1];
    const reachName = reach.text.split(' · ')[0];
    await destination.selectOption(reach.value);
    await page.waitForFunction(() => /^1 \/ \d+$/.test(document.querySelector('[aria-label="Walk the call chain"] span')?.textContent ?? ''), null, { timeout: 180000 });
    const reachLabel = `Behavior · handle → ${reachName}`;
    measured = await settle(page, { view: 'behavior', heading: `handle → ${reachName}`, chain: /^1 \/ \d+$/ });
    const operations = Number(measured?.chain?.split(' / ')[1] ?? 0);
    const manyPaths = (measured?.path ?? 0) > 0;
    await shot(page, 'behavior-ziel', `Behavior: handle bis ${reachName}, ${operations} Operationen, ${manyPaths ? 'mehrere Pfade' : 'ein Pfad'}`, measured);
    await page.locator('.atlas-architecture [aria-label="Walk the call chain"] button', { hasText: /^Next/ }).click();
    const walked = { view: 'behavior', chain: `2 / ${operations}`, back: { disabled: false, title: 'Back to Behavior · handle (Alt+Left)' } };
    measured = await settle(page, walked);
    check('J-walk', 'Next in der Aufrufkette ist kein eigener Schritt: Zurueck nennt weiter den Start ohne Ziel', operations >= 3 && matches(measured, walked).length === 0, { operations, misses: matches(measured, walked), measured });
    await shot(page, 'behavior-kette-2', `Behavior: Operation 2 von ${operations}; Zurueck nennt Behavior · handle`, measured);
    let pathLabel = reachLabel;
    let secondOperations = operations;
    if (manyPaths) {
        await page.locator('.atlas-architecture [aria-label="Indexed paths"] button').nth(1).click();
        measured = await settle(page, { view: 'behavior', path: 2, chain: /^1 \/ \d+$/, back: { disabled: false, title: `Back to ${reachLabel} (Alt+Left)` } });
        check('J-path', 'Ein anderer Pfad zum Ziel ist ein Schritt', measured?.path === 2 && measured?.back?.title === `Back to ${reachLabel} (Alt+Left)`, measured);
        secondOperations = Number(measured?.chain?.split(' / ')[1] ?? 0);
        pathLabel = `${reachLabel} · Path 2`;
        await shot(page, 'behavior-pfad-2', `Behavior: Pfad 2; Zurueck nennt ${reachLabel}`, measured);
    }
    await page.locator('.atlas-architecture [aria-label="Behavior camera"] button', { hasText: /^Plan$/ }).click();
    const behaviorPlan = { view: 'behavior', behaviorCamera: 'Plan', inViewBack: 0, back: { disabled: false, title: `Back to ${pathLabel} (Alt+Left)` } };
    measured = await settle(page, behaviorPlan);
    check('J-plan', 'Behavior: Plan ist ein eigener Schritt', matches(measured, behaviorPlan).length === 0, { misses: matches(measured, behaviorPlan), measured });
    await shot(page, 'behavior-plan', `Behavior in Plan; Zurueck nennt ${pathLabel}`, measured);
    await tab(page, 'overview').click();
    measured = await settle(page, { view: 'overview', back: { disabled: false, title: `Back to ${pathLabel} · Plan (Alt+Left)` } });
    check('J-plan-label', 'Overview danach: Zurueck nennt Behavior mit Pfad und Plan', measured?.back?.title === `Back to ${pathLabel} · Plan (Alt+Left)`, measured?.back);
    await historyButton(page, 'Back').click();
    const againPlan = { view: 'behavior', heading: `handle → ${reachName}`, behaviorCamera: 'Plan', chain: `1 / ${secondOperations}`, ...(manyPaths ? { path: 2 } : {}) };
    measured = await settle(page, againPlan);
    check('J-back-plan', 'Zurueck: Behavior kommt in Plan wieder, auf demselben Pfad', matches(measured, againPlan).length === 0, { misses: matches(measured, againPlan), measured });
    await shot(page, 'behavior-plan-zurueck', 'Zurueck auf Behavior: Plan und Pfad wie vorher', measured);
    await historyButton(page, 'Back').click();
    const again3d = { view: 'behavior', behaviorCamera: '3D', ...(manyPaths ? { path: 2 } : { chain: `2 / ${operations}` }) };
    measured = await settle(page, again3d);
    check('J-back-3d', 'Noch einmal Zurueck: Behavior in 3D', matches(measured, again3d).length === 0, { misses: matches(measured, again3d), measured });
    await shot(page, 'behavior-3d-zurueck', 'Noch einmal Zurueck: Behavior in 3D', measured);
    if (manyPaths) {
        await historyButton(page, 'Back').click();
        const againWalked = { view: 'behavior', heading: `handle → ${reachName}`, path: 1, chain: `2 / ${operations}`, behaviorCamera: '3D' };
        measured = await settle(page, againWalked);
        check('J-back-step', 'Zurueck auf Pfad 1: die Kette steht wieder auf Operation 2', matches(measured, againWalked).length === 0, { misses: matches(measured, againWalked), measured });
        await shot(page, 'behavior-kette-zurueck', `Zurueck auf Pfad 1: wieder Operation 2 von ${operations}`, measured);
        await historyButton(page, 'Forward').click();
        measured = await settle(page, { view: 'behavior', path: 2, behaviorCamera: '3D' });
    }
    await historyButton(page, 'Forward').click();
    measured = await settle(page, againPlan);
    check('J-forward-plan', 'Vor: Behavior wieder in Plan auf demselben Pfad', matches(measured, againPlan).length === 0, { misses: matches(measured, againPlan), measured });
    await shot(page, 'behavior-plan-vor', 'Vor auf Behavior: Plan und Pfad wieder da', measured);

    // Tippen im Routen-Filter und sofort Zurueck: der Text geht nicht verloren
    await tab(page, 'routes').click();
    measured = await settle(page, { view: 'routes' });
    const filterBefore = measured?.filter ?? '';
    const perspectiveName = measured?.perspective ?? 'Endpoints';
    await page.locator('.atlas-architecture input[type="search"]').click();
    // Gemessen in der Seite: vom letzten Tastendruck bis zum Klick, der den Knopf erreicht.
    // Date.now() um click() herum zaehlte auch Playwrights eigene Wartezeit und schwankte unter Last.
    await page.evaluate(() => {
        window.__typedAt = 0; window.__backAt = 0;
        document.addEventListener('input', () => { window.__typedAt = performance.now(); }, true);
        document.addEventListener('click', (event) => {
            if (event.target instanceof Element && event.target.closest('button') && !window.__backAt) window.__backAt = performance.now();
        }, true);
    });
    await page.keyboard.press('ControlOrMeta+A');
    await page.keyboard.type('/adm');
    await page.evaluate(() => { window.__backAt = 0; });
    await historyButton(page, 'Back').click();
    const typingElapsed = Math.round(await page.evaluate(() => window.__backAt - window.__typedAt));
    const typedBack = { view: 'routes', filter: filterBefore, forward: { disabled: false, title: `Forward to Routes · ${perspectiveName} · /adm (Alt+Right)` } };
    measured = await settle(page, typedBack);
    check('T-back', 'Getippt und sofort Zurueck (vor der Pause von 600 ms): Zurueck verlaesst den Text, Vor nennt ihn', typingElapsed < 600 && matches(measured, typedBack).length === 0,
        { typingElapsed, misses: matches(measured, typedBack), measured });
    await shot(page, 'tippen-zurueck', `Nach "/adm" sofort Zurueck (${typingElapsed} ms): Filter ${JSON.stringify(filterBefore)}, Vor nennt /adm`, measured);
    await historyButton(page, 'Forward').click();
    measured = await settle(page, { view: 'routes', filter: '/adm' });
    check('T-forward', 'Vor: der getippte Text ist wieder da', measured?.filter === '/adm', measured);
    await shot(page, 'tippen-vor', 'Vor: Routes mit dem Filter /adm', measured);

    // Projektwechsel nach cbm: frischer Verlauf
    await page.locator('.atlas-shell details summary', { hasText: PROJECT }).first().click();
    const entry = page.locator('.atlas-project-results button', { has: page.locator('.atlas-project-result-name', { hasText: new RegExp(`^${CONTROL}$`) }) }).first();
    await entry.waitFor({ timeout: 20000 });
    await entry.click();
    const fresh = { position: '1/1', back: { disabled: true, title: 'Nothing to go back to yet' }, forward: { disabled: true, title: 'Nothing to go forward to' }, recent: [] };
    measured = await settle(page, fresh, 60000);
    await wait(2500);
    measured = await state(page);
    check('P-fresh', `Projektwechsel nach ${CONTROL}: frischer Verlauf ohne Zurueck, Vor und Recent`, matches(measured, fresh).length === 0, { misses: matches(measured, fresh), measured });
    await shot(page, 'cbm-frisch', `Architecture in ${CONTROL} nach dem Projektwechsel: frischer Verlauf`, measured);
    await tab(page, 'routes').click();
    const startedOn = architectureName(measured?.view);
    measured = await settle(page, { view: 'routes', position: '2/2', back: { disabled: false, title: `Back to ${startedOn} (Alt+Left)` } }, 60000);
    check('P-step', `${CONTROL}: der erste Schritt fuehrt nur in ${CONTROL} zurueck`, measured?.position === '2/2' && measured?.back?.title === `Back to ${startedOn} (Alt+Left)`, { position: measured?.position, back: measured?.back });
    await shot(page, 'cbm-erster-schritt', `${CONTROL}: Routes, Zurueck nennt den Start in ${CONTROL}`, measured);
} catch (error) {
    check('X0', 'Ablauf abgebrochen', false, String(error?.stack ?? error));
    await shot(page, 'abbruch', 'Stand beim Abbruch', await state(page).catch(() => null)).catch(() => {});
}

function architectureName(view) {
    return { overview: 'Overview', routes: 'Routes · Service map', hotspots: 'Hotspots', structure: 'System structure', behavior: 'Behavior' }[view] ?? 'Overview';
}

check('X1', 'Keine Seitenfehler im Lauf', pageErrors.length === 0, { pageErrors });
await context.close();

const lines = ['# K27: Zurueck und Vor in Architecture', '', `Ursprung ${ORIGIN}, Projekt ${PROJECT}, Kontrolle ${CONTROL}, Ansicht ${VIEWPORT.width}x${VIEWPORT.height}, ohne Fenster.`, '',
    ...checks.map((item) => `- ${item.pass ? 'PASS' : 'FAIL'} ${item.id}: ${item.title}`), '', '| Bild | Zeigt | Gemessen |', '|---|---|---|',
    ...images.map((item) => `| ${item.file} | ${item.note} | ${item.measured ? `${item.measured.view ?? ''} · ${JSON.stringify(item.measured.trail ?? '')} · ${item.measured.perspective ?? ''} · filter ${JSON.stringify(item.measured.filter)} · focus ${JSON.stringify(item.measured.focus)} · system ${item.measured.systemCamera ?? '-'} · ${item.measured.heading ?? ''} · behavior ${item.measured.behaviorCamera ?? '-'} · path ${item.measured.path ?? '-'} · chain ${item.measured.chain ?? '-'} · in-view Back ${item.measured.inViewBack ?? '-'} · ${item.measured.position ?? ''} · ${item.measured.back?.title ?? ''} / ${item.measured.forward?.title ?? ''}`.replaceAll('|', '/') : ''} |`), ''];
await writeFile(join(OUT, 'index.md'), lines.join('\n'));
await writeFile(join(OUT, 'report.json'), JSON.stringify({ origin: ORIGIN, project: PROJECT, control: CONTROL, checks }, null, 2));
const failed = checks.filter((item) => !item.pass);
console.log(`${checks.length - failed.length}/${checks.length} bestanden`);
if (failed.length) process.exitCode = 1;
