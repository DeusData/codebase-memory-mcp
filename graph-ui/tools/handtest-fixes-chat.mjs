#!/usr/bin/env node
/*
 * Browser checks for the chat and local agent items of the hand test of 2026-10-03
 * (graph-ui/verification/call-feedback-2026-10-02/KORREKTURPLAN.md: K1, K4, K7, K10,
 * K11, K12, K14, K15, K16, K17, K24).
 *
 *   node tools/handtest-fixes-chat.mjs --origin http://127.0.0.1:4392 --out /tmp/chat \
 *        --profile /tmp/profile-with-cached-model [--only K14,K7] [--runs 3] [--headed]
 *
 * The browser runs headless unless --headed is given; ANGLE on Metal gives the headless
 * browser the GPU with shader-f16 the local model needs (without it WebGPU falls back to
 * SwiftShader, which has no f16).
 *
 * One check per item. Each drives the flow the plan describes, measures it in the page
 * (DOM text and boxes, the messages sent to the model worker, the agent lamp) and prints
 * PASS or FAIL with the measured values. Screenshots go to <out>/<K-id>/ with an
 * index.md that names each image; model outputs go to <out>/<K-id>/outputs.json.
 *
 * It starts no server. It needs a running server with the UI and a browser profile in
 * which the local model is cached for this origin (Cache Storage is per origin).
 * With --with-download the last K24 check deletes that cached model and downloads it again (network
 * access to Hugging Face), to switch the project while the download runs; the cache is full again after it.
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
const ORIGIN = String(arg('origin', 'http://127.0.0.1:4392')).replace(/\/$/, '');
const OUT = resolve(String(arg('out', 'verification/handtest-fixes-chat')));
const PROFILE = resolve(String(arg('profile', join(OUT, '..', 'profile'))));
const PROJECT = String(arg('project', 'django-demo'));
const CONTROL = String(arg('control', 'cbm'));
const RUNS = Number(arg('runs', 3));
const WITH_DOWNLOAD = arg('with-download', false) === true;
const HEADED = arg('headed', false) === true;
const ALL = ['K15', 'K16', 'K11', 'K17', 'K14', 'K7', 'K4', 'K1', 'K12', 'K10', 'K24'];
const ONLY = String(arg('only', ALL.join(','))).split(',');
const VIEWPORT = { width: 1600, height: 1000 };
const wait = (ms) => new Promise((done) => setTimeout(done, ms));

/* ------------------------------------------------------------------ */
/* In every page                                                       */

function pageProbe() {
    try { localStorage.setItem('cbm.workspace.setup', 'done'); } catch { /* without storage it still works */ }
    // Everything sent to the model worker, including workers created later.
    window.__probeWorker = [];
    window.__probeAnswers = [];
    // Model workers created in this page: a project switch must not create another (K24).
    window.__probeModelWorkers = 0;
    // Every request of the page, with the address the page showed when it was sent (K24).
    window.__probeFetches = [];
    const nativeFetch = window.fetch.bind(window);
    window.fetch = (input, init) => {
        let entry;
        try {
            const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
            // A log post is kept whole enough to read its page and entries (K24).
            const keep = /\/api\/ui-log/.test(url) ? 8000 : 600;
            entry = { t: Math.round(performance.now()), url, body: typeof init?.body === 'string' ? init.body.slice(0, keep) : '', page: location.search };
            window.__probeFetches.push(entry);
        } catch { /* observation only */ }
        const answer = nativeFetch(input, init);
        // When it ended and how: a request still open at a switch is either aborted or lands nowhere (K24).
        if (entry) answer.then(() => { entry.done = Math.round(performance.now()); }, (error) => { entry.done = Math.round(performance.now()); entry.failed = String(error?.name ?? error); });
        return answer;
    };
    window.__probePage = Math.random().toString(36).slice(2);
    const NativeWorker = window.Worker;
    window.Worker = class ProbeWorker extends NativeWorker {
        constructor(url, options) {
            super(url, options);
            if (/browser-ai/.test(String(url))) window.__probeModelWorkers += 1;
            // The model's raw answers, before the chat checks or shortens them.
            this.addEventListener('message', (event) => {
                if (event.data?.kind === 'answer') window.__probeAnswers.push({ t: Math.round(performance.now()), output: event.data.output, stopReason: event.data.stopReason });
            });
        }
        postMessage(message, transfer) {
            try {
                if (message && typeof message === 'object' && typeof message.kind === 'string') {
                    window.__probeWorker.push({ t: Math.round(performance.now()), kind: message.kind, profile: message.generationProfile ?? '',
                        messages: Array.isArray(message.messages) ? message.messages : undefined });
                }
            } catch { /* observation only */ }
            return super.postMessage(message, transfer);
        }
    };
    window.__probeErrors = [];
    window.addEventListener('error', (event) => window.__probeErrors.push(String(event.message)));
}

/* ------------------------------------------------------------------ */
/* Result                                                              */

const results = [];
const modelRequests = [];
const images = new Map();
function check(id, title, pass, measured) {
    results.push({ id, title, pass: Boolean(pass), measured });
    console.log(`${pass ? 'PASS' : 'FAIL'}  ${id}  ${title}  ${JSON.stringify(measured)}`);
}
async function shot(page, id, label, proves, options = {}) {
    await mkdir(join(OUT, id), { recursive: true });
    const list = images.get(id) ?? [];
    const file = `${String(list.length + 1).padStart(2, '0')}-${label}.png`;
    await page.screenshot({ path: join(OUT, id, file), ...options });
    list.push({ file, proves }); images.set(id, list);
    return file;
}
async function save(id, name, value) {
    await mkdir(join(OUT, id), { recursive: true });
    await writeFile(join(OUT, id, name), typeof value === 'string' ? value : JSON.stringify(value, null, 2));
}

/* ------------------------------------------------------------------ */
/* Page helpers                                                        */

async function freshHistory(page) {
    // A page of this origin without the app, so the chat history database is not open.
    await page.goto(`${ORIGIN}/probe-blank`, { waitUntil: 'domcontentloaded' });
    await page.evaluate(() => new Promise((done) => { const request = indexedDB.deleteDatabase('cbm-browser-chat-history'); request.onsuccess = request.onerror = request.onblocked = () => done(); }));
}
async function open(page, project, workspace) {
    await page.goto(`${ORIGIN}/?project=${encodeURIComponent(project)}&workspace=${workspace}`, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.atlas-shell', { timeout: 30000 });
    await wait(1500);
}
const agentLabel = (page) => page.locator('.atlas-browser-ai-action').first().getAttribute('aria-label').catch(() => '');
async function agentActive(page, timeout = 300000) {
    return page.waitForFunction(() => /Agent active/.test(document.querySelector('.atlas-browser-ai-action')?.getAttribute('aria-label') ?? ''), null, { timeout })
        .then(() => true).catch(() => false);
}
async function openConfig(page) {
    if (!(await page.locator('dialog.cbm-agent-settings').count())) await page.locator('.atlas-browser-ai-action').first().click();
    await page.locator('dialog.cbm-agent-settings').waitFor({ timeout: 10000 });
    await wait(400);
}
async function closeConfig(page) {
    if (await page.locator('dialog.cbm-agent-settings').count()) await page.getByRole('button', { name: 'Close agent configuration' }).click();
    await wait(300);
}
async function loadModel(page) {
    if (/Agent active/.test(await agentLabel(page) ?? '')) return true;
    await openConfig(page);
    const load = page.locator('dialog.cbm-agent-settings button').filter({ hasText: /^(Load model|Download & load|Reload model)/ });
    if (await load.count()) await load.first().click();
    const ok = await agentActive(page);
    await closeConfig(page);
    return ok;
}
async function openChat(page) {
    const toggle = page.getByRole('button', { name: 'Open chat' });
    if (await toggle.count()) { await toggle.first().click(); await wait(800); }
}
/** Switch workspace by its tab: no reload, so the loaded model stays. */
async function tab(page, workspace) {
    await page.locator(`[data-workspace-tab="${workspace}"]`).first().click();
    await wait(1500);
}
async function galaxyReady(page) {
    await page.waitForFunction(() => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0, null, { timeout: 60000 }).catch(() => {});
    await wait(1000);
}
async function select(page, name) {
    const input = page.getByRole('searchbox', { name: 'Find a graph node' }).first();
    await input.click();
    await input.fill('');
    await input.type(name, { delay: 25 });
    const choice = page.locator('.atlas-galaxy-search-results button', { has: page.locator('strong', { hasText: new RegExp(`^${name}$`) }) }).first();
    await choice.waitFor({ timeout: 20000 });
    await choice.click();
}
async function scopeSettled(page, timeout = 30000) {
    await page.waitForFunction(() => {
        const text = document.querySelector('.atlas-graph-exploration')?.textContent ?? '';
        return /\d+ nodes? · \d+ edges?/.test(text) && !/Loading relationships|Checking index/.test(text);
    }, null, { timeout }).catch(() => {});
    await wait(1200);
}
/** Until the automatic explanation has finished (its "Explain again" button is back). */
async function explanationDone(page, timeout = 180000) {
    await page.waitForFunction(() => {
        const section = document.querySelector('.cbm-chat-explanation');
        return Boolean(section?.querySelector('.cbm-chat-retry')) && !/Explaining selection|Preparing explanation|adding one sentence/.test(section.textContent ?? '');
    }, null, { timeout }).catch(() => {});
    await wait(400);
}
async function explanationText(page) {
    return (await page.locator('.cbm-chat-explanation').innerText().catch(() => '')).replace(/\n?Explain again\s*$/, '').trim();
}
async function ask(page, question, timeout = 240000) {
    const before = await page.locator('.cbm-chat-turn').count();
    const prompt = page.locator('#cbm-chat-prompt');
    await prompt.click();
    await prompt.fill(question);
    await prompt.press('Enter');
    await page.waitForFunction((count) => document.querySelectorAll('.cbm-chat-turn').length > count, before, { timeout: 30000 }).catch(() => {});
    await page.waitForFunction(() => !document.querySelector('.cbm-chat-send[aria-label="Stop"]') && !/Thinking…/.test(document.querySelector('.cbm-chat-turn:last-of-type')?.textContent ?? ''), null, { timeout }).catch(() => {});
    await wait(600);
    return page.locator('.cbm-chat-turn').last().innerText().catch(() => '');
}
const workerMessages = (page) => page.evaluate(() => window.__probeWorker.slice());
const markWorker = (page) => page.evaluate(() => window.__probeWorker.length);
const sentSince = async (page, mark, kind = 'chat') => (await workerMessages(page)).slice(mark).filter((entry) => entry.kind === kind);
const promptText = (entry) => (entry?.messages ?? []).map((message) => `[${message.role}] ${message.content}`).join('\n\n');
async function clickExplainAgain(page) {
    const button = page.locator('.cbm-chat-explanation .cbm-chat-retry');
    await button.click();
    await wait(800);
    await explanationDone(page);
}
async function newConversation(page) {
    const button = page.locator('.cbm-chat-header .cbm-chat-new');
    if (!(await button.count())) return;
    page.once('dialog', (dialog) => { void dialog.accept(); });
    await button.click();
    await wait(600);
}

/* ------------------------------------------------------------------ */
/* Checks                                                              */

/** Galaxy with JSONBAgg selected; returns the worker mark from just before the selection. */
async function galaxyWithJsonb(page) {
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    await loadModel(page);
    await openChat(page);
    const mark = await markWorker(page);
    await select(page, 'JSONBAgg');
    await scopeSettled(page);
    return mark;
}
const answersSince = async (page, from) => (await page.evaluate(() => window.__probeAnswers.slice())).filter((entry) => entry.t >= from);
const now = (page) => page.evaluate(() => Math.round(performance.now()));

async function k15(page) {
    await galaxyWithJsonb(page);
    await explanationDone(page);
    const toggle = page.locator('.cbm-chat-explanation .cbm-chat-response-source > summary');
    await shot(page, 'K15', 'before-expand', 'The explanation with its collapsed Source toggle.');
    await toggle.click();
    await wait(500);
    const blocks = await page.evaluate(() => [...document.querySelectorAll('.cbm-chat-explanation .cbm-chat-source-content pre')].map((pre) => ({
        scrollWidth: pre.scrollWidth, clientWidth: pre.clientWidth, whiteSpace: getComputedStyle(pre).whiteSpace, text: pre.textContent.slice(0, 80) })));
    const panel = await page.locator('.cbm-chat-explanation .cbm-chat-source-content').boundingBox();
    await shot(page, 'K15', 'after-expand', 'The expanded Source block: every fact and code line wraps inside the panel, nothing is cut at the right edge.');
    if (panel) await shot(page, 'K15', 'after-expand-crop', 'Crop of the expanded Source block.', { clip: { x: panel.x - 8, y: panel.y - 8, width: panel.width + 16, height: Math.min(panel.height + 16, 700) } });
    // The facts of the hand test (17:45): long caller lists and paths, here under a listed answer.
    await ask(page, 'Who calls JSONBAgg?');
    const turn = page.locator('.cbm-chat-turn').last();
    await turn.locator('.cbm-chat-response-source > summary').click();
    await wait(500);
    const facts = await turn.evaluate((element) => [...element.querySelectorAll('.cbm-chat-source-content pre')].map((pre) => ({
        scrollWidth: pre.scrollWidth, clientWidth: pre.clientWidth, whiteSpace: getComputedStyle(pre).whiteSpace, text: pre.textContent.slice(0, 80) })));
    const factsBox = await turn.locator('.cbm-chat-source-content').boundingBox();
    if (factsBox) await shot(page, 'K15', 'facts-expanded-crop', 'The expanded Source of a listed answer: the long CALLS/TESTS lines and paths wrap, "test_empty_result_set" and "django/contrib/postgres/..." are complete.',
        { clip: { x: factsBox.x - 8, y: Math.max(0, factsBox.y - 8), width: factsBox.width + 16, height: Math.min(factsBox.height + 16, 700) } });
    const all = [...blocks, ...facts];
    const clipped = all.filter((block) => block.scrollWidth > block.clientWidth + 1);
    check('K15', 'Expanded Source blocks wrap long lines (no horizontal overflow)', blocks.length > 0 && facts.length > 0 && clipped.length === 0 && all.every((block) => block.whiteSpace === 'pre-wrap'),
        { explanationBlocks: blocks.length, factBlocks: facts.length, clipped, whiteSpace: [...new Set(all.map((block) => block.whiteSpace))], factsStart: facts.map((block) => block.text) });
}

async function k16(page) {
    await galaxyWithJsonb(page);
    await explanationDone(page);
    const mark = await markWorker(page);
    const last = () => page.locator('.cbm-chat-turn').last();
    const buttons = async () => (await last().locator('button.cbm-chat-retry').allInnerTexts()).map((text) => text.trim());
    const typo = await ask(page, 'wer ruf jsonbagg auf');
    await shot(page, 'K16', 'typo-listed', '"wer ruf jsonbagg auf" is answered with the listed callers from the graph.');
    const shortTypo = await ask(page, 'wer rft jsonbagg auf');
    const suggestion = await ask(page, 'jsonbagg aufrufe?');
    const germanButtons = await buttons();
    await shot(page, 'K16', 'suggestion', '"jsonbagg aufrufe?" gets "Meintest du: Aufrufer von JSONBAgg?" with the German choices "Liste anzeigen" and "Modell fragen".');
    const box = await last().boundingBox();
    if (box) await shot(page, 'K16', 'suggestion-crop', 'Crop of the German suggestion and its German buttons.', { clip: { x: box.x - 8, y: Math.max(0, box.y - 8), width: box.width + 16, height: Math.min(box.height + 16, 500) } });
    await last().getByRole('button', { name: 'Liste anzeigen' }).click();
    await wait(600);
    const listed = await last().innerText();
    const listedButtons = await buttons();
    await shot(page, 'K16', 'suggestion-listed', 'After "Liste anzeigen" the suggestion is replaced by the complete listed answer; its button reads "Modell fragen".');
    // The direction is read after the whole selection phrase: "this class call" asks what it calls.
    const direction = await ask(page, 'Does this class call super?');
    const englishButtons = await buttons();
    await shot(page, 'K16', 'direction', '"Does this class call super?" is offered as "what JSONBAgg calls", with the English choices.');
    const modelCalls = await sentSince(page, mark);
    // A real word one edit from "ruft" is not corrected: "Luft" goes to the model, not to a suggestion.
    const luftMark = await markWorker(page);
    const luft = await ask(page, 'Hat diese Klasse Luft?');
    const luftCalls = await sentSince(page, luftMark);
    await shot(page, 'K16', 'luft-to-model', '"Hat diese Klasse Luft?" is not read as a call question: the model answers it.');
    const callers = ['test_default_argument', 'test_empty_result_set', 'test_jsonb_agg', 'test_values_list'];
    check('K16', 'Typo caller questions are listed; uncertain ones are suggested in the right direction and language; no model call', /Aufrufer \(CALLS\) von JSONBAgg im geladenen Graphen/.test(typo)
        && callers.every((name) => typo.includes(name)) && /Aufrufer \(CALLS\) von JSONBAgg im geladenen Graphen/.test(shortTypo) && /Meintest du: Aufrufer von JSONBAgg\?/.test(suggestion)
        && JSON.stringify(germanButtons) === JSON.stringify(['Liste anzeigen', 'Modell fragen']) && /Aufrufer \(CALLS\) von JSONBAgg im geladenen Graphen/.test(listed)
        && JSON.stringify(listedButtons) === JSON.stringify(['Modell fragen']) && /Did you mean: what JSONBAgg calls\?/.test(direction)
        && JSON.stringify(englishButtons) === JSON.stringify(['Show the list', 'Ask the model']) && modelCalls.length === 0 && luftCalls.length === 1 && !/Meintest du|Did you mean/.test(luft),
    { typoAnswerStart: typo.slice(0, 120), shortTypo: shortTypo.slice(0, 120), suggestion: suggestion.slice(0, 200), germanButtons, afterShowList: listed.slice(0, 120), listedButtons,
        direction: direction.slice(0, 200), englishButtons, modelCallsBeforeLuft: modelCalls.length, luftModelCalls: luftCalls.length, luft: luft.slice(0, 200) });
    // A typo of up to two letters in the selected name, any case, and "wo" anywhere in the question (completeness check of 2026-10-04).
    const typoMark = await markWorker(page);
    const orders = [];
    for (const question of ['jsonbagg wird wo aufgerufen', 'wo wird jsonbagg aufgerufen', 'von wo wird jsonbagg aufgerufen']) orders.push({ question, answer: await ask(page, question) });
    await shot(page, 'K16', 'word-order-listed', '"jsonbagg wird wo aufgerufen", "wo wird ..." and "von wo wird ...": each lists the callers of JSONBAgg from the graph.');
    const typos = [];
    for (const [question, language] of [['wer ruft JSONBAg auf', 'de'], ['wer ruft jsonbgg auf?', 'de'], ['who calls jsonbag', 'en'], ['callers of jsonbag', 'en']]) typos.push({ question, language, answer: await ask(page, question) });
    const typoBox = await last().boundingBox();
    await shot(page, 'K16', 'name-typo-suggested', '"callers of jsonbag" (and "wer ruft JSONBAg auf", "wer ruft jsonbgg auf?", "who calls jsonbag"): "Did you mean: callers of JSONBAgg?" with Show the list and Ask the model.');
    if (typoBox) await shot(page, 'K16', 'name-typo-crop', 'Crop of the suggestion for "callers of jsonbag".', { clip: { x: typoBox.x - 8, y: Math.max(0, typoBox.y - 8), width: typoBox.width + 16, height: Math.min(typoBox.height + 16, 400) } });
    await last().getByRole('button', { name: 'Show the list' }).click();
    await wait(600);
    const typoListed = await last().innerText();
    await shot(page, 'K16', 'name-typo-listed', 'After "Show the list": the complete callers of JSONBAgg.');
    const typoModelCalls = (await sentSince(page, typoMark)).length;
    check('K16', 'A typo in the selected name (up to two letters, any case) is offered for the selection, a free word order with "wo" is listed; none of them reach the model',
        orders.every((item) => /Aufrufer \(CALLS\) von JSONBAgg im geladenen Graphen/.test(item.answer) && callers.every((name) => item.answer.includes(name)))
        && typos.every((item) => item.language === 'de' ? /Meintest du: Aufrufer von JSONBAgg\?/.test(item.answer) && /entspricht nicht dem Namen der Auswahl \(JSONBAgg\)/.test(item.answer)
            : /Did you mean: callers of JSONBAgg\?/.test(item.answer) && /does not match the name of the selection \(JSONBAgg\)/.test(item.answer))
        && /callers \(CALLS\) of JSONBAgg in the loaded graph/.test(typoListed) && typoModelCalls === 0,
    { orders: orders.map((item) => `${item.question} => ${item.answer.replace(/\s+/g, ' ').slice(0, 140)}`), typos: typos.map((item) => `${item.question} => ${item.answer.replace(/\s+/g, ' ').slice(0, 160)}`),
        afterShowList: typoListed.replace(/\s+/g, ' ').slice(0, 140), modelCalls: typoModelCalls });
}

async function k11(page) {
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    await loadModel(page);
    await openChat(page);
    await shot(page, 'K11', 'all-graph-before', 'Galaxy shows the whole graph (All graph); nothing is selected.');
    const mark = await markWorker(page);
    const german = await ask(page, 'was kannst du mir über den code sagen');
    const english = await ask(page, 'test');
    await shot(page, 'K11', 'after-questions', 'Both questions get the chat\'s own reply saying what to select; the model was not asked.');
    const sent = (await workerMessages(page)).slice(mark).filter((entry) => entry.kind === 'chat' || entry.kind === 'count');
    check('K11', 'Without context the model is not called and the reply says what to select (de/en)', sent.length === 0
        && /Wähle einen Knoten in Galaxy oder einen Teil in Architecture, oder öffne eine Datei in Explore/.test(german) && /Select a node in Galaxy or a part in Architecture, or open a file in Explore/.test(english),
    { workerMessages: sent.length, german: german.slice(0, 220), english: english.slice(0, 200) });
}

async function openWorkflowFile(page) {
    await open(page, PROJECT, 'explore');
    for (const path of ['.github', '.github/workflows']) {
        const row = page.locator(`.atlas-tree-row[data-path="${path}"]`).first();
        await row.waitFor({ timeout: 30000 });
        for (let attempt = 0; attempt < 4 && (await row.getAttribute('data-expanded')) !== 'true'; attempt++) { await row.click(); await wait(1200); }
    }
    const file = page.locator('.atlas-tree-row[data-path=".github/workflows/new_contributor_pr.yml"]').first();
    await file.waitFor({ timeout: 30000 });
    await file.click();
    await page.waitForFunction(() => /New contributor message/.test(document.body.innerText), null, { timeout: 30000 }).catch(() => {});
    await wait(2500);
}

async function k17(page) {
    await openWorkflowFile(page);
    await loadModel(page);
    await openChat(page);
    await explanationDone(page);
    // A question the model answers: "was kannst du mir über ..." is answered from the file since C7, and the point here is a model answer that must not come back.
    const exploreAnswer = await ask(page, 'Welche Aktion nutzt dieses aktuelle File?');
    await shot(page, 'K17', 'explore-answer', 'A question about the YAML workflow in Explore and its answer.');
    await tab(page, 'galaxy');
    await galaxyReady(page);
    await select(page, 'JSONBAgg');
    await scopeSettled(page);
    await explanationDone(page);
    let mark = await markWorker(page);
    const galaxyAnswer = await ask(page, 'What does JSONBAgg do?');
    const [request] = await sentSince(page, mark);
    const text = promptText(request);
    await shot(page, 'K17', 'galaxy-answer', 'In Galaxy a divider marks the new topic JSONBAgg; New conversation sits in the chat header.');
    const header = await page.locator('.cbm-chat-header').innerText();
    const header1 = await page.locator('.cbm-chat-header .cbm-chat-new').boundingBox();
    if (header1) await shot(page, 'K17', 'header-crop', 'Crop of the chat header with the New conversation button.', { clip: { x: header1.x - 260, y: header1.y - 14, width: header1.width + 320, height: header1.height + 28 } });
    const assistantTurns = (request?.messages ?? []).filter((message) => message.role === 'assistant').length;
    // Back to the same workflow file: "Zurück zu" and its own earlier turn is sent again, nothing of JSONBAgg (B4).
    await tab(page, 'explore');
    await page.waitForFunction(() => /New contributor message/.test(document.body.innerText), null, { timeout: 30000 }).catch(() => {});
    await explanationDone(page);
    mark = await markWorker(page);
    const returnAnswer = await ask(page, 'Und was noch?');
    const [back] = await sentSince(page, mark);
    const backText = promptText(back);
    const divider = await page.locator('.cbm-chat-topic-break').allInnerTexts();
    await shot(page, 'K17', 'explore-return', 'Back in Explore on the same file: a divider "Zurück zu: .github/workflows/new_contributor_pr.yml"; the earlier question about the file is in the prompt, nothing of JSONBAgg.');
    const dividerBox = await page.locator('.cbm-chat-topic-break').last().boundingBox();
    if (dividerBox) await shot(page, 'K17', 'explore-return-crop', 'Crop of the divider above the returned question.', { clip: { x: dividerBox.x - 8, y: Math.max(0, dividerBox.y - 8), width: dividerBox.width + 16, height: 220 } });
    const backAssistant = (back?.messages ?? []).filter((message) => message.role === 'assistant');
    const backUsers = (back?.messages ?? []).filter((message) => message.role === 'user').map((message) => message.content.split('\n').at(-1));
    check('K17', 'Earlier answers about another topic are not sent; coming back to a file says "Zurück zu" and sends that file\'s own earlier turn; topic breaks and New conversation visible', Boolean(request) && !/Welche Aktion nutzt dieses aktuelle File/.test(text)
        && assistantTurns === 0 && /New conversation/.test(header) && divider.some((item) => /New topic: JSONBAgg/.test(item))
        && Boolean(back) && backAssistant.length === 1 && !/JSONBAgg/.test(backText)
        && JSON.stringify(backUsers) === JSON.stringify(['Welche Aktion nutzt dieses aktuelle File?', 'Und was noch?']) && divider.some((item) => /Zurück zu: \.github\/workflows\/new_contributor_pr\.yml\. Die früheren Nachrichten dazu werden wieder mitgeschickt/.test(item)),
    { assistantMessagesInPrompt: assistantTurns, flake8InPrompt: /flake8/i.test(text), exploreQuestionInPrompt: /aktuelle File/.test(text), header, divider, exploreAnswer: exploreAnswer.slice(0, 200), galaxyAnswer: galaxyAnswer.slice(0, 200),
        returnPrompt: { assistantMessages: backAssistant.map((message) => message.content.slice(0, 160)), userMessages: backUsers, jsonbAggInPrompt: /JSONBAgg/.test(backText) }, returnAnswer: returnAnswer.slice(0, 200) });
    await save('K17', 'prompt-galaxy.txt', text);
    await save('K17', 'prompt-explore-return.txt', backText);
}

async function setAutomatic(page, on) {
    await openConfig(page);
    const box = page.getByLabel('Explain selections automatically');
    if (on) await box.check(); else await box.uncheck();
    await closeConfig(page);
}

async function k14(page) {
    const mark = await galaxyWithJsonb(page);
    await explanationDone(page);
    const [request] = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation');
    const text = promptText(request);
    await page.locator('.cbm-chat-explanation .cbm-chat-response-source > summary').click();
    await wait(400);
    const disclosure = await page.locator('.cbm-chat-explanation .cbm-chat-source-content').innerText().catch(() => '');
    await shot(page, 'K14', 'explanation-source', 'The JSONBAgg explanation with its Source disclosure open: the class source (lines 50-54) is there and "Source unavailable" is gone.');
    const card = await explanationText(page);
    await save('K14', 'prompt-jsonbagg.txt', text);
    // Automatic explanations off: a listed answer reads the source itself, and "Ask the model" sends it (review finding).
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    await loadModel(page);
    await setAutomatic(page, false);
    await openChat(page);
    await select(page, 'JSONBAgg');
    await scopeSettled(page);
    const offMark = await markWorker(page);
    await ask(page, 'Who calls JSONBAgg?');
    const automaticRuns = (await sentSince(page, offMark)).length;
    const turn = page.locator('.cbm-chat-turn').last();
    await turn.locator('.cbm-chat-response-source > summary').click();
    await wait(500);
    const listedSource = await turn.locator('.cbm-chat-source-content').innerText().catch(() => '');
    await shot(page, 'K14', 'listed-source-automatic-off', 'Automatic explanations off: the listed answer "Who calls JSONBAgg?" shows the class source in its Source disclosure, no "Source unavailable".');
    const askMark = await markWorker(page);
    await turn.getByRole('button', { name: 'Ask the model' }).click();
    await page.waitForFunction(() => !document.querySelector('.cbm-chat-send[aria-label="Stop"]') && !/Thinking…/.test(document.querySelector('.cbm-chat-turn:last-of-type')?.textContent ?? ''), null, { timeout: 240000 }).catch(() => {});
    await wait(800);
    const [asked] = await sentSince(page, askMark);
    const askedText = promptText(asked);
    const answer = await page.locator('.cbm-chat-turn').last().innerText().catch(() => '');
    await shot(page, 'K14', 'ask-model-with-source', 'After "Ask the model" on the listed answer: the model answered from a prompt that carries the class source.');
    await setAutomatic(page, true);
    await save('K14', 'prompt-ask-model.txt', askedText);
    check('K14', 'Galaxy explanation and a listed answer with automatic explanations off carry the selected symbol source; no "Source unavailable"', /class JSONBAgg\(OrderableAggMixin, Aggregate\)/.test(text) && /function = "JSONB_AGG"/.test(text)
        && !/Source unavailable/.test(text + disclosure) && /class JSONBAgg\(OrderableAggMixin, Aggregate\)/.test(disclosure)
        && automaticRuns === 0 && /class JSONBAgg\(OrderableAggMixin, Aggregate\)/.test(listedSource) && !/Source unavailable/.test(listedSource)
        && /function = "JSONB_AGG"/.test(askedText) && !/Source unavailable/.test(askedText),
    { sourceInPrompt: /class JSONBAgg/.test(text), sourceUnavailable: /Source unavailable/.test(text + disclosure), card,
        automaticOff: { modelMessagesForListedAnswer: automaticRuns, listedSourceStart: listedSource.slice(0, 260), askPromptHasSource: /function = "JSONB_AGG"/.test(askedText),
            askPromptSourceUnavailable: /Source unavailable/.test(askedText), answer: answer.replace(/\s+/g, ' ').slice(0, 260) } });
}

async function setTrace(page, direction, onlyType) {
    await page.locator('select[aria-label="Trace direction"]').selectOption(direction);
    await scopeSettled(page);
    if (onlyType) {
        const summary = page.locator('summary', { hasText: /^Edge types/ }).first();
        await summary.click();
        await wait(400);
        await page.getByRole('button', { name: `Only ${onlyType}`, exact: true }).click();
        await wait(400);
        await summary.click().catch(() => {});
        await scopeSettled(page);
    }
}

async function k7(page) {
    const outputs = [];
    // Galaxy, incoming and CALLS only: the case of the hand test at 17:20.
    await galaxyWithJsonb(page);
    await explanationDone(page);
    let mark = await markWorker(page), from = await now(page);
    await setTrace(page, 'inbound', 'CALLS');
    await explanationDone(page);
    let [request] = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation');
    let card = await explanationText(page);
    outputs.push({ selection: 'JSONBAgg incoming CALLS', card, model: (await answersSince(page, from))[0]?.output, prompt: promptText(request) });
    await shot(page, 'K7', 'galaxy-incoming-calls', 'JSONBAgg, incoming, CALLS only: listed facts, at most one model sentence, no "list of integers / list of strings".');
    const galaxyOk = Boolean(request) && /Incoming: 11 relationships from 11 symbols \(CALLS 11\)/.test(card) && !/list of (?:integers|strings)|output is/i.test(card)
        && /Never state types, parameters, inputs, outputs/.test(promptText(request)) && /class JSONBAgg\(OrderableAggMixin, Aggregate\)/.test(promptText(request));
    // Architecture, Overview: the django area (hand test 17:53).
    await tab(page, 'architecture');
    await openChat(page);
    await page.getByRole('button', { name: 'Overview', exact: true }).click();
    await wait(5000);
    await page.getByText(/Browse map/).first().click();
    await wait(600);
    mark = await markWorker(page); from = await now(page);
    await page.locator('button', { hasText: /^djangoarea/ }).first().click();
    await wait(1500);
    await explanationDone(page);
    [request] = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation');
    card = await explanationText(page);
    outputs.push({ selection: 'Architecture overview django area', card, model: (await answersSince(page, from))[0]?.output, prompt: promptText(request) });
    await shot(page, 'K7', 'architecture-area', 'Architecture overview, django area: readable facts (files, lines, languages, hotspots, connections), no "Finding a line number" list.');
    // Without source the listed facts are the explanation; the model is not asked to guess.
    const areaOk = !request && /Selected source area: django/.test(card) && /Connections /.test(card) && !/Selected\.|members\[\d+\]|startLine|Finding a|Visible operations/.test(card)
        && /not generated by the model/.test(card);
    // The same area opened, with nothing selected inside it (hand test 17:55): facts about the area, not "Opened: django".
    mark = await markWorker(page); from = await now(page);
    await page.getByRole('button', { name: 'Open area →' }).first().click();
    await wait(2500);
    await explanationDone(page);
    [request] = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation');
    const openedCard = await explanationText(page);
    // The card counts the parts the view counts ("23 parts · 100 connections"): inside the area and outside it.
    const viewParts = /([\d,]+) parts? · [\d,]+ connections?/.exec(await page.locator('.atlas-workspace-stage').innerText().catch(() => ''))?.[1] ?? null;
    const cardParts = /Parts shown \(([\d,]+)/.exec(openedCard)?.[1] ?? null;
    outputs.push({ selection: 'Architecture overview, django opened, nothing selected', card: openedCard, model: (await answersSince(page, from))[0]?.output, prompt: promptText(request), viewParts, cardParts });
    await shot(page, 'K7', 'architecture-area-opened', 'Overview inside django with nothing selected: the card names the opened area, its lines and languages, the parts shown, its hotspot findings and the connections between its parts.');
    const openedOk = !request && /Opened source area: django\./.test(openedCard) && /indexed lines in [\d,]+ measured files of [\d,]+; files by language: /.test(openedCard)
        && /Parts shown \([\d,]+(?:, largest first\)|\): [\d,]+ inside the area, largest first): /.test(openedCard) && viewParts !== null && cardParts === viewParts && /hotspot findings?: /.test(openedCard) && /Connections of its parts: /.test(openedCard) && /not generated by the model/.test(openedCard)
        && !/^Opened: /m.test(openedCard);
    // Architecture, Behavior: main of manage.py-tpl.
    mark = await markWorker(page); from = await now(page);
    await page.getByRole('button', { name: 'Behavior' }).click();
    await wait(6000);
    await explanationDone(page);
    [request] = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation');
    card = await explanationText(page);
    outputs.push({ selection: 'Architecture behavior', card, model: (await answersSince(page, from))[0]?.output, prompt: promptText(request) });
    await shot(page, 'K7', 'architecture-behavior', 'Architecture Behavior: the starting operation, its call sites with the functions they reach (os.environ.setdefault, execute_from_command_line), no invented operations.');
    const behaviorOk = /Starting operation: main/.test(card) && (!request || /def main\(\)/.test(promptText(request))) && !/Finding a|Visible operations/.test(card)
        && /line 9 calls os\.environ\.setdefault/.test(card) && /line 18 calls execute_from_command_line/.test(card);
    await save('K7', 'outputs.json', outputs);
    check('K7', 'Automatic explanations rest on listed facts (Galaxy incoming CALLS, Architecture area selected and opened with the parts the view counts, Behavior)', galaxyOk && areaOk && openedOk && behaviorOk,
        { galaxyOk, areaOk, openedOk, behaviorOk, openedParts: { card: cardParts, view: viewParts }, cards: outputs.map((item) => `${item.selection}: ${item.card.replace(/\s+/g, ' ').slice(0, 520)}`) });
}

async function k4(page) {
    const outputs = [];
    await galaxyWithJsonb(page);
    for (const name of ['JSONBAgg', 'BaseCommand']) {
        let mark = await markWorker(page), from = await now(page);
        if (name !== 'JSONBAgg') { await select(page, name); await scopeSettled(page); }
        for (let run = 0; run < RUNS; run++) {
            if (run === 0) await explanationDone(page); else { mark = await markWorker(page); from = await now(page); await clickExplainAgain(page); }
            const [request] = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation');
            const [raw] = await answersSince(page, from);
            outputs.push({ name, run: run + 1, card: await explanationText(page), model: raw?.output, promptHasNumberedIds: /\[(?:graph|source)-\d+\]/.test(promptText(request)), modelCalled: Boolean(request) });
        }
        await shot(page, 'K4', `explanation-${name}`, `The automatic explanation of ${name} after ${RUNS} runs: no "Graph 1/2/3".`);
    }
    await save('K4', 'outputs.json', outputs);
    const echoes = outputs.filter((item) => /\bgraph[\s-]*\d\b/i.test(item.card));
    check('K4', `No "Graph 1/2/3" in ${outputs.length} real explanations of JSONBAgg and BaseCommand; prompt without [graph-N]`, echoes.length === 0 && outputs.every((item) => !item.promptHasNumberedIds),
        { runs: outputs.length, echoes: echoes.length, modelRuns: outputs.filter((item) => item.modelCalled).length, cards: outputs.map((item) => `${item.name}#${item.run}: ${item.card.replace(/\s+/g, ' ').slice(-220)}`) });
}

async function setOutputLimit(page, value) {
    await openConfig(page);
    const field = page.locator('#cbm-chat-output-tokens');
    await field.fill(String(value));
    await field.press('Enter');
    await wait(300);
    await closeConfig(page);
}

async function k1(page) {
    await galaxyWithJsonb(page);
    await explanationDone(page);
    await setOutputLimit(page, 48);
    await ask(page, 'Explain this class in detail, line by line.');
    const note = page.locator('.cbm-chat-turn').last().locator('details.cbm-chat-limit-note');
    const summary = await note.locator('summary').innerText().catch(() => '');
    await shot(page, 'K1', 'note-collapsed', 'Under the cut answer: "Token limit reached: the answer was cut short".');
    await note.locator('summary').click();
    await wait(400);
    const expanded = await note.innerText().catch(() => '');
    await shot(page, 'K1', 'note-expanded', 'The expanded note: the limits the answer ran into, Change the output limit, the larger models with download size.');
    await note.getByRole('button', { name: 'Change the output limit' }).click();
    await wait(700);
    const focus = await page.evaluate(() => ({ id: document.activeElement?.id ?? '', dialog: Boolean(document.querySelector('dialog.cbm-agent-settings[open]')) }));
    await shot(page, 'K1', 'config-output-focused', 'The button opens the agent configuration with the Output field focused.');
    await page.locator('#cbm-chat-output-tokens').fill('512');
    await page.locator('#cbm-chat-output-tokens').press('Enter');
    await closeConfig(page);
    check('K1', 'Cut answer explains the token limit, opens the configuration at Output and lists larger models', summary === 'Token limit reached: the answer was cut short'
        && /all 48 output tokens/.test(expanded) && /2,048 tokens/.test(expanded) && /Qwen3 0\.6B · 579 MB download/.test(expanded) && /LFM2\.5 1\.2B · 764 MB download/.test(expanded)
        && /Qwen3\.5 2B · 1\.40 GB download/.test(expanded) && focus.dialog && focus.id === 'cbm-chat-output-tokens', { summary, expanded, focus });
    // The automatic variant names both limits as well (completeness check of 2026-10-04). A code file in Explore
    // gets up to two model sentences; with 32 output tokens the explanation runs into the limit.
    await setOutputLimit(page, 32);
    await tab(page, 'explore');
    await openTreeFile(page, 'django/contrib/postgres/aggregates/general.py');
    await explanationDone(page);
    let automatic = '';
    for (let run = 0; run < RUNS + 3 && !automatic; run++) {
        const cut = page.locator('.cbm-chat-explanation details.cbm-chat-limit-note');
        if (await cut.count()) {
            await cut.locator('summary').click();
            await wait(400);
            automatic = await cut.innerText().catch(() => '');
        } else await clickExplainAgain(page);
    }
    await shot(page, 'K1', 'automatic-note-expanded', 'An automatic explanation cut at 32 output tokens: the note names the output limit and the input limit of automatic explanations and of a chat question.');
    const box = await page.locator('.cbm-chat-explanation details.cbm-chat-limit-note').boundingBox().catch(() => null);
    if (box) await shot(page, 'K1', 'automatic-note-crop', 'Crop of the expanded note under the automatic explanation.', { clip: { x: box.x - 8, y: Math.max(0, box.y - 8), width: box.width + 16, height: Math.min(box.height + 16, 420) } });
    await setOutputLimit(page, 512);
    check('K1', 'The note under a cut automatic explanation names its input limit and its output limit, and those of a question', /Automatic explanations stop after 32 output tokens/.test(automatic)
        && /read at most 1,536 input tokens/.test(automatic) && /may read up to 2,048 input tokens/.test(automatic), { automatic });
}

/** Opens a file of the project root (or below) in Explore by clicking its tree row. */
async function openTreeFile(page, path) {
    const parts = path.split('/');
    for (let depth = 1; depth < parts.length; depth++) {
        const row = page.locator(`.atlas-tree-row[data-path="${parts.slice(0, depth).join('/')}"]`).first();
        await row.waitFor({ timeout: 30000 });
        for (let attempt = 0; attempt < 4 && (await row.getAttribute('data-expanded')) !== 'true'; attempt++) { await row.click(); await wait(1200); }
    }
    const file = page.locator(`.atlas-tree-row[data-path="${path}"]`).first();
    if (!(await file.count())) return false;
    await file.click();
    await wait(3500);
    return true;
}
const cardButton = (page, name) => page.locator('.cbm-chat-explanation').getByRole('button', { name, exact: true });

async function k12(page) {
    const outputs = [];
    await openWorkflowFile(page);
    await loadModel(page);
    await openChat(page);
    let mark = await markWorker(page);
    await explanationDone(page);
    // The automatic card of a configuration file: the facts read from it, and no model text (K12).
    const [automatic] = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation');
    const card = await explanationText(page);
    const askButton = await cardButton(page, 'Ask the model').count();
    outputs.push({ kind: 'automatic', card, modelCalled: Boolean(automatic) });
    await shot(page, 'K12', 'automatic', 'Explore with the workflow open: the automatic card lists name, trigger, the one job and the action read from the file, says it is not generated by the model, and offers "Ask the model".');
    // Only on request: "Ask the model" lets the model write about the file, marked as generated.
    mark = await markWorker(page);
    let from = await now(page);
    await cardButton(page, 'Ask the model').click();
    await wait(800);
    await explanationDone(page);
    const [asked] = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation');
    const askedCard = await explanationText(page);
    outputs.push({ kind: 'ask-the-model', card: askedCard, model: (await answersSince(page, from))[0]?.output, prompt: promptText(asked) });
    await shot(page, 'K12', 'ask-the-model', 'After "Ask the model": the facts stay above, the model text follows and is marked as generated (or left out when it names what the file does not show).');
    // Other configuration and text files of the project: facts only, no model.
    const others = [];
    for (const path of ['package.json', 'tox.ini', 'pyproject.toml', 'README.rst', '.editorconfig']) {
        mark = await markWorker(page);
        if (!(await openTreeFile(page, path))) continue;
        await explanationDone(page);
        const calls = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation').length;
        const text = await explanationText(page);
        others.push({ path, card: text.replace(/\s+/g, ' ').slice(0, 300), factsOnly: /Read from the file; not generated by the model\./.test(text), modelCalls: calls });
        if (others.length <= 3) await shot(page, 'K12', `facts-${path.replace(/[^\w]+/g, '-')}`, `${path}: the automatic card shows only what is read from the file; no model call.`);
    }
    // A YAML list of mappings is a list: django's pre-commit file holds `repos`, a list of 5 items with repo, rev and hooks,
    // not "10 keys: rev, hooks, rev, ..." (review of the K12 facts). The Read the Docs file is a mapping with a list below `python.install`.
    const yaml = [];
    for (const [path, wanted] of [['.pre-commit-config.yaml', /Top-level keys \(1\): repos \(list of 5; item keys: repo, rev, hooks\)\./],
        ['.readthedocs.yml', /Top-level keys \(5\): version, build \(2 keys: os, tools\), sphinx \(2 keys: configuration, fail_on_warning\), python \(1 key: install\), formats\./]]) {
        mark = await markWorker(page);
        const opened = await openTreeFile(page, path);
        await explanationDone(page);
        const text = await explanationText(page);
        const calls = (await sentSince(page, mark)).filter((entry) => entry.profile === 'automatic-explanation').length;
        await page.locator('.cbm-chat-explanation').screenshot({ path: join(OUT, 'K12', `yaml-${path.replace(/[^\w]+/g, '-')}-card.png`) }).catch(() => {});
        const file = await shot(page, 'K12', `yaml-${path.replace(/[^\w]+/g, '-')}`, `${path}: the automatic card counts what the YAML holds (a list as a list, its items' keys as item keys); crop: yaml-${path.replace(/[^\w]+/g, '-')}-card.png.`);
        yaml.push({ path, opened, file, card: text.replace(/\s+/g, ' ').slice(0, 400), wanted: wanted.test(text.replace(/\s+/g, ' ')),
            repeatedKeys: /keys: (\w+), (\w+), \1, \2/.test(text), factsOnly: /Read from the file; not generated by the model\./.test(text), modelCalls: calls });
    }
    check('K12', 'YAML facts count a list of mappings as a list with item keys (.pre-commit-config.yaml: repos, list of 5) and a mapping by its keys (.readthedocs.yml)',
        yaml.length === 2 && yaml.every((item) => item.opened && item.wanted && !item.repeatedKeys && item.factsOnly && item.modelCalls === 0), { yaml });
    // Explicit questions about the workflow go to the model, as before (back by its tree row: no reload, the model stays loaded).
    // Since C7 "what does this file do" questions are answered from the file instead: no request, the counted facts, the read-from-file note.
    await openTreeFile(page, '.github/workflows/new_contributor_pr.yml');
    await explanationDone(page);
    const questions = ['was kannst du mir über dieses aktuelle File sagen', 'Wie viele Jobs gibt es in dieser Datei?', 'Welche Jobs gibt es in dieser Datei?', 'What does this file do?', 'How many jobs does this workflow have?'];
    const runs = Math.max(RUNS, questions.length);
    for (let run = 0; run < runs; run++) {
        mark = await markWorker(page);
        const answer = await ask(page, questions[run % questions.length]);
        const [request] = await sentSince(page, mark);
        outputs.push({ kind: 'question', run: run + 1, question: questions[run % questions.length], answer, prompt: promptText(request) });
        if (run === 0) await shot(page, 'K12', 'question-answer', 'The answer to "was kannst du mir über dieses aktuelle File sagen" for the YAML workflow.');
        if (run === 1) await shot(page, 'K12', 'jobs-count', 'The answer to "Wie viele Jobs gibt es in dieser Datei?": the prompt carries the counted facts (1 job: build).');
        await newConversation(page);
    }
    await save('K12', 'outputs.json', { outputs, others });
    const questionsAsked = outputs.filter((item) => item.kind === 'question');
    const FROM_FILE = ['was kannst du mir über dieses aktuelle File sagen', 'What does this file do?'];
    const modelQuestions = questionsAsked.filter((item) => !FROM_FILE.includes(item.question));
    const fromFile = questionsAsked.filter((item) => FROM_FILE.includes(item.question));
    const answeredFromFile = fromFile.length > 0 && fromFile.every((item) => !item.prompt && /1 (?:job|Job): build/.test(item.answer)
        && /Read from the file; not generated by the model\.|Aus der Datei gelesen, nicht vom Modell erzeugt\./.test(item.answer));
    // The open file is in the system section of the request, named as a workflow, with its counted facts, and its JSON record is gone.
    const fileInPrompt = modelQuestions.every((item) => /--- BEGIN EXACT SOURCE TEXT ---\nname: New contributor message/.test(item.prompt)
        && /The current file is a GitHub Actions workflow \(YAML configuration, not program code\)\./.test(item.prompt) && !/"status":"ready"/.test(item.prompt));
    const factsInPrompt = modelQuestions.every((item) => /Facts read from the file \(counted, not guessed\):/.test(item.prompt) && /1 job: `build`/.test(item.prompt))
        && /Facts read from the file \(counted, not guessed\):/.test(promptText(asked));
    const factsInCard = /1 job: build \("Hello new contributor", runs on ubuntu-latest, 1 step\)/.test(card) && /Trigger: pull_request_target \(types: opened\)/.test(card);
    const factsOnly = !automatic && askButton === 1 && /Read from the file; not generated by the model\./.test(card) && card.split('\n').filter(Boolean).every((line) => /^(?:Agent|ⓘ Source|Workflow name|Trigger|1 job|Actions used|Read from the file|Ask the model)/.test(line.trim()));
    const askedLabelled = Boolean(asked) && /1 job: build/.test(askedCard)
        && (/Facts read from the file; the text after them is generated by the model\./.test(askedCard) || /The model's text (?:named|claimed) something the file does not show \(here: .+?\) and was left out\./.test(askedCard));
    const othersFactsOnly = others.length >= 2 && others.every((item) => item.modelCalls === 0 && item.factsOnly);
    const invented = questionsAsked.filter((item) => /flake8|python|\bpip\b|\.py\b/i.test(item.answer));
    const counts = questionsAsked.filter((item) => /Wie viele|How many/.test(item.question)).map((item) => item.answer.replace(/^You\s+[^\n]*\n+Agent\s+(?:ⓘ Source\s+)?/, '').replace(/\s+/g, ' ').slice(0, 200));
    const wrongCount = counts.filter((answer) => /\b(?:[2-9]|zwei|drei|vier|two|three|four)\s+(?:jobs?|Jobs?)\b/i.test(answer));
    const unsupportedNotes = await page.locator('.cbm-chat-answer-note').count();
    check('K12', 'The automatic card of a configuration file shows only facts read from it; the model writes only on "Ask the model" or a question, marked as generated; answers invent no flake8/Python and no wrong job count',
        factsOnly && factsInCard && askedLabelled && othersFactsOnly && fileInPrompt && factsInPrompt && answeredFromFile && invented.length === 0 && wrongCount.length === 0,
    { factsOnly, factsInCard, askedLabelled, othersFactsOnly, fileInPrompt, factsInPrompt, answeredFromFile, invented: invented.length, wrongCount: wrongCount.length, counts, unsupportedNotes,
        automatic: card.replace(/\s+/g, ' ').slice(0, 420), askTheModel: askedCard.replace(/\s+/g, ' ').slice(0, 520), others,
        answers: questionsAsked.map((item) => `${item.question} => ${item.answer.replace(/\s+/g, ' ').slice(0, 260)}`) });
}

async function k10(page) {
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    const loaded = await loadModel(page);
    await page.reload({ waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.atlas-shell');
    await wait(2500);
    const lampAfterReload = await agentLabel(page);
    await openConfig(page);
    await wait(800);
    const buttons = await page.locator('dialog.cbm-agent-settings .cbm-chat-model-actions button').allInnerTexts();
    const option = await page.locator('#cbm-chat-model option:checked').innerText().catch(() => '');
    const cache = await page.evaluate(async () => { const name = (await caches.keys()).find((key) => key.startsWith('cbm-browser-ai-qwen25')); return name ? (await (await caches.open(name)).keys()).length : 0; });
    await shot(page, 'K10', 'config-after-reload', 'After a reload the configuration says "Cached" and offers "Load model (cached, no download)".');
    // Optional automatic loading, off by default.
    const autoLoad = page.getByLabel('Load the chosen model on start when it is cached');
    const autoDefault = await autoLoad.isChecked().catch(() => null);
    await autoLoad.check();
    await closeConfig(page);
    const requestsBefore = modelRequests.length;
    const t0 = Date.now();
    await page.reload({ waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.atlas-shell');
    await shot(page, 'K10', 'auto-load-0ms', 'Right after the reload with automatic loading on: the lamp is loading, no click.');
    const active = await agentActive(page, 120000);
    const seconds = (Date.now() - t0) / 1000;
    await shot(page, 'K10', 'auto-load-active', 'The agent is active again without any click.');
    const network = modelRequests.length - requestsBefore;
    await openConfig(page);
    await page.getByLabel('Load the chosen model on start when it is cached').uncheck();
    await closeConfig(page);
    check('K10', 'After reload: cached state shown, no download offered; optional automatic loading works (default off)', loaded && /Load model \(cached, no download\)/.test(buttons.join(' '))
        && !/Download & load/.test(buttons.join(' ')) && /Cached/.test(option) && autoDefault === false && active && network === 0,
    { lampAfterReload, buttons, option, cacheEntries: cache, autoDefault, autoLoadActiveAfterSeconds: active ? seconds : null, modelNetworkRequests: network });
}

/* K24: what each workspace shows of a project, after a switch in the page. */
const K24_VIEWS = [
    { id: 'explore', workspace: 'explore' },
    { id: 'galaxy', workspace: 'galaxy' },
    { id: 'architecture-overview', workspace: 'architecture', view: 'Overview', sub: 'Structure' },
    { id: 'architecture-entry-points', workspace: 'architecture', view: 'Overview', sub: 'Entry points' },
    { id: 'architecture-service-map', workspace: 'architecture', view: 'Routes', sub: 'Service map' },
    { id: 'architecture-endpoints', workspace: 'architecture', view: 'Routes', sub: 'Endpoints' },
    { id: 'architecture-hotspots', workspace: 'architecture', view: 'Hotspots' },
    { id: 'architecture-system-structure', workspace: 'architecture', view: 'System structure' },
    { id: 'architecture-behavior', workspace: 'architecture', view: 'Behavior' },
    { id: 'adr', workspace: 'adr' },
    { id: 'coverage', workspace: 'coverage' },
    { id: 'system-logs', workspace: 'system', sub: 'Logs' },
];
/** Words only the other project's views show: its source paths and symbol prefixes. */
const PROJECT_MARKERS = {
    'django-demo': /\bdjango\/|manage\.py-tpl|\bjs_tests\b|django-demo\/|JSONBAgg/,
    cbm: /src\/main\.c|\bgraph-ui\/|\bcbm_[a-z]|\bcbm\/|test-infrastructure/,
};
const BUSY = /Loading|Reading|Preparing|Checking|Resolving|Updating view/;
/** The workspace text once it stopped changing (or after `timeout`). */
async function settledStage(page, timeout = 30000) {
    let last = '', same = 0;
    const t0 = Date.now();
    while (Date.now() - t0 < timeout) {
        await wait(900);
        const text = await page.locator('.atlas-workspace-stage').innerText().catch(() => '');
        same = text === last && !BUSY.test(text) ? same + 1 : 0;
        if (same >= 2) return text;
        last = text;
    }
    return last;
}
async function showView(page, item) {
    await page.locator(`[data-workspace-tab="${item.workspace}"]`).first().click();
    await wait(700);
    if (item.view) { await page.locator('.atlas-workspace-stage').getByRole('button', { name: item.view, exact: true }).first().click().catch(() => {}); await wait(700); }
    if (item.sub) { await page.locator('.atlas-workspace-stage').getByRole('button', { name: item.sub, exact: true }).first().click().catch(() => {}); await wait(700); }
}
/** Paths and dotted names a view shows; a stale view shows ones the fresh view of its project does not. */
const pathTokens = (text) => new Set(text.match(/[\w.@-]+\/[\w./@-]+/g) ?? []);
/** What the Galaxy draws: its headline names the indexed node count, which differs per project, while the stage text has no paths. */
const galaxyGraph = (page) => page.evaluate(() => ({ headline: globalThis.__atlasGalaxy?.headline ?? '', nodes: globalThis.__atlasGalaxy?.nodes ?? 0, target: globalThis.__atlasGalaxy?.lastTargetQn ?? '' }));
async function viewTexts(page, label, shots) {
    const texts = {};
    let galaxy;
    for (const item of K24_VIEWS) {
        await showView(page, item);
        texts[item.id] = await settledStage(page, item.workspace === 'galaxy' ? 45000 : 30000);
        if (item.workspace === 'galaxy') { await galaxyReady(page); galaxy = await galaxyGraph(page); }
        if (item.workspace === 'system') texts[item.id] += `\n${await page.locator('select option[value="project"]').first().innerText().catch(() => '')}`;
        if (shots) await shot(page, 'K24', `${label}-${item.id}`, `${label}: ${item.id} after the switch.`);
    }
    return { texts, galaxy };
}
/** The same graph as the fresh Galaxy of the project: headline, node count and no selection carried over. */
const sameGraph = (seen, fresh) => Boolean(seen && fresh) && seen.headline === fresh.headline && seen.nodes === fresh.nodes && seen.target === fresh.target;
/** Requests sent once the address named `shown` that name `project` (URL parameter or JSON argument);
 * the log transport carries entries logged before the switch by design. */
async function requestsNaming(page, from, shown, project) {
    const fetches = (await page.evaluate(() => window.__probeFetches.slice()))
        .filter((entry) => entry.t >= from && new URLSearchParams(entry.page).get('project') === shown && !/\/api\/ui-log/.test(entry.url));
    const names = (entry) => {
        const named = new Set();
        try { const value = new URL(entry.url, location.href).searchParams.get('project'); if (value) named.add(value); } catch { /* not a URL */ }
        for (const match of entry.body.matchAll(/"(?:project|project_name)"\s*:\s*"([^"]+)"/g)) named.add(match[1]);
        return named;
    };
    return { total: fetches.length, naming: fetches.filter((entry) => names(entry).has(project)).map((entry) => ({ t: entry.t, url: entry.url.slice(0, 120), body: entry.body.slice(0, 160), page: entry.page })) };
}
const lampFrames = async (page, label, t0) => {
    const frames = [];
    for (const at of [0, 250, 1000, 3000]) {
        const delay = at - (Date.now() - t0);
        if (delay > 0) await wait(delay);
        frames.push({ at, label: await agentLabel(page), file: await shot(page, 'K24', `${label}-${at}ms`, `${label}, ${at} ms after the click: the agent lamp.`) });
    }
    return frames;
};
const activeWorkspace = (page) => page.locator('[data-workspace-tab][aria-selected="true"]').first().getAttribute('data-workspace-tab').catch(() => '');
/** The same text as the fresh view of the project, apart from numbers (times, memory) and spacing. */
const sameText = (left, right) => left.replace(/\d[\d,.:]*/g, '#').replace(/\s+/g, ' ') === right.replace(/\d[\d,.:]*/g, '#').replace(/\s+/g, ' ');
async function switchTo(page, project) {
    await page.locator('.atlas-project-switcher summary').first().click();
    await page.locator('.atlas-project-picker button', { has: page.locator('.atlas-project-result-name', { hasText: new RegExp(`^${project}`) }) }).first().click();
}

const pageState = (page) => page.evaluate(() => ({ id: window.__probePage, origin: performance.timeOrigin, workers: window.__probeModelWorkers }));
const projectShown = (page) => page.evaluate(() => new URLSearchParams(location.search).get('project'));
const chatToggle = (page) => page.locator('.atlas-agent-chat-toggle').first();
/** The ways a switch can meet the page that a plain switch does not: chat closed, an answer running,
 * the old project still loading, and a log line written after it (review of K24). */
async function k24Situations(page, reference) {
    const lampOk = (frames, last = /Agent active/) => frames.every((frame) => /Agent (?:active|working)/.test(frame.label ?? '')) && last.test(frames.at(-1)?.label ?? '');
    // 1. Chat closed: the model stays, the chat stays closed, and nothing is loaded again.
    {
        await tab(page, 'galaxy');
        if ((await chatToggle(page).getAttribute('aria-expanded')) === 'true') { await chatToggle(page).click(); await wait(600); }
        const here = await projectShown(page), other = here === CONTROL ? PROJECT : CONTROL;
        const before = await pageState(page), mark = await markWorker(page), t0 = Date.now();
        await switchTo(page, other);
        const frames = await lampFrames(page, `chat-closed-to-${other}`, t0);
        const after = await pageState(page);
        const chatExpanded = await chatToggle(page).getAttribute('aria-expanded');
        const prepares = (await sentSince(page, mark, 'prepare')).length;
        check('K24', 'A switch with the chat closed keeps the model loaded and the chat closed', after.id === before.id && after.workers === before.workers && prepares === 0
            && lampOk(frames) && chatExpanded === 'false' && (await projectShown(page)) === other,
        { from: here, to: other, samePage: after.id === before.id, modelWorkersCreated: after.workers - before.workers, prepares, chatExpanded, lamp: frames.map(({ at, label }) => ({ at, label })) });
    }
    // 2. An answer running in the old project: the lamp goes from working to active, the old answer never shows,
    //    and the handed-over model answers in the new project.
    {
        if ((await projectShown(page)) !== PROJECT) { await switchTo(page, PROJECT); await wait(2500); }
        await tab(page, 'galaxy');
        await galaxyReady(page);
        await openChat(page);
        if ((await chatToggle(page).getAttribute('aria-expanded')) !== 'true') { await chatToggle(page).click(); await wait(800); }
        await select(page, 'JSONBAgg');
        await scopeSettled(page);
        await explanationDone(page);
        const question = 'Beschreibe ausführlich, wofür diese Klasse in Django verwendet wird, mit vielen Details.';
        const prompt = page.locator('#cbm-chat-prompt');
        await prompt.click(); await prompt.fill(question); await prompt.press('Enter');
        const running = await page.waitForFunction(() => Boolean(document.querySelector('.cbm-chat-send[aria-label="Stop"]')), null, { timeout: 30000 }).then(() => true).catch(() => false);
        await wait(600);
        const lampBefore = await agentLabel(page);
        const before = await pageState(page), mark = await markWorker(page), t0 = Date.now();
        await switchTo(page, CONTROL);
        const frames = await lampFrames(page, 'running-answer-to-cbm', t0);
        await page.waitForFunction(() => /Agent active/.test(document.querySelector('.atlas-browser-ai-action')?.getAttribute('aria-label') ?? ''), null, { timeout: 30000 }).catch(() => {});
        const settledLamp = await agentLabel(page);
        const transcript = await page.locator('.cbm-chat-transcript').innerText().catch(() => '');
        await shot(page, 'K24', 'running-answer-cbm-after-switch', 'cbm right after a switch while a django-demo answer was running: the old question and answer are not in cbm\'s chat; the lamp is back on "Agent active".');
        // A question in cbm that the model answers on the handed-over worker, about a file of cbm open in Explore.
        await tab(page, 'explore');
        const fileOpened = await openTreeFile(page, 'src/main.c');
        await explanationDone(page);
        const asked = await markWorker(page);
        const answer = await ask(page, 'Was macht diese Datei?');
        const [chat] = await sentSince(page, asked);
        await shot(page, 'K24', 'running-answer-cbm-model-answer', 'cbm after that switch: the model, handed over from django-demo, answers a question about cbm\'s src/main.c.');
        const after = await pageState(page);
        const prepares = (await sentSince(page, mark, 'prepare')).length;
        const answerText = answer.replace(/^You\s+[^\n]*\n+/, '').trim();
        const lastTurn = page.locator('.cbm-chat-turn').last();
        const failedTurn = (await lastTurn.locator('.cbm-chat-turn-error').count()) + (await lastTurn.getByText(/Stopped before an answer|Stopped · partial answer/).count());
        check('K24', 'A switch during a running answer: the lamp goes from working to active, the old answer never shows, and the handed-over model answers in the new project',
            running && /Agent working/.test(lampBefore ?? '') && frames.every((frame) => /Agent (?:active|working)/.test(frame.label ?? '')) && /Agent active/.test(settledLamp ?? '')
            && !transcript.includes(question) && !/JSONBAgg/.test(transcript) && fileOpened && Boolean(chat) && /src\/main\.c/.test(promptText(chat)) && !/JSONBAgg/.test(promptText(chat)) && answerText.length > 20
            && failedTurn === 0 && after.id === before.id && after.workers === before.workers && prepares === 0,
        { running, lampBefore, lamp: frames.map(({ at, label }) => ({ at, label })), settledLamp, oldQuestionInCbm: transcript.includes(question), fileOpened, modelAsked: Boolean(chat),
            promptNamesOld: /JSONBAgg/.test(promptText(chat)), failedTurn, answer: answerText.replace(/\s+/g, ' ').slice(0, 300), samePage: after.id === before.id, modelWorkersCreated: after.workers - before.workers, prepares });
    }
    // 3. The old project still loading: open django-demo's Galaxy and switch before its graph is there. The server
    //    is made slow for that one layout (4 s), so the request is surely still open when the switch happens.
    {
        if ((await projectShown(page)) !== PROJECT) { await switchTo(page, PROJECT); await wait(2500); }
        await tab(page, 'explore');
        const slowLayout = (url) => /\/api\/layout/.test(url.pathname) && url.searchParams.get('project') === PROJECT;
        const delay = async (route) => { await wait(4000); await route.continue().catch(() => {}); };
        await page.route(slowLayout, delay);
        const clicked = await now(page);
        await page.locator('[data-workspace-tab="galaxy"]').first().click();
        await page.waitForFunction((from) => window.__probeFetches.some((entry) => entry.t >= from && /\/api\/layout/.test(entry.url)), clicked, { timeout: 15000 }).catch(() => {});
        await wait(300);
        const t = await now(page);
        const openAtSwitch = (await page.evaluate(() => window.__probeFetches.slice())).filter((entry) => entry.t >= clicked && /\/api\/layout/.test(entry.url) && entry.done === undefined).map((entry) => entry.url.slice(0, 100));
        await switchTo(page, CONTROL);
        await page.waitForFunction((name) => new URLSearchParams(location.search).get('project') === name, CONTROL, { timeout: 30000 }).catch(() => {});
        await galaxyReady(page);
        await settledStage(page, 45000);
        const graph = await galaxyGraph(page);
        await wait(4000);
        await page.unroute(slowLayout, delay);
        const ended = (await page.evaluate(() => window.__probeFetches.slice())).filter((entry) => entry.t >= clicked && entry.t < t && /\/api\/layout/.test(entry.url)).map((entry) => ({ url: entry.url.slice(0, 100), endedAfterSwitchMs: entry.done === undefined ? null : entry.done - t, failed: entry.failed ?? null }));
        const leaks = await requestsNaming(page, t, CONTROL, PROJECT);
        const text = await page.locator('.atlas-workspace-stage').innerText().catch(() => '');
        await shot(page, 'K24', 'switch-while-loading-cbm-galaxy', 'Switched to cbm while django-demo\'s Galaxy was still loading: cbm\'s own graph, nothing of django-demo.');
        check('K24', 'A switch while the old project is still loading shows the new project\'s own Galaxy; no request after the switch names the old project',
            openAtSwitch.length > 0 && sameGraph(graph, reference[CONTROL].galaxy) && leaks.naming.length === 0 && !PROJECT_MARKERS[PROJECT].test(text),
        { openAtSwitch, oldLayoutRequests: ended, graph, fresh: reference[CONTROL].galaxy, requestsAfterSwitch: leaks.total, requestsNamingOldProject: leaks.naming.slice(0, 5), stale: PROJECT_MARKERS[PROJECT].exec(text)?.[0] ?? null });
    }
    // 4. A log line written after a switch is posted under the page it was written on, the new one.
    {
        const here = await projectShown(page), other = here === CONTROL ? PROJECT : CONTROL;
        const t = await now(page);
        await page.evaluate(() => console.warn('k24 log line before the switch'));
        await switchTo(page, other);
        await page.waitForFunction((name) => new URLSearchParams(location.search).get('project') === name, other, { timeout: 30000 }).catch(() => {});
        await page.evaluate(() => console.warn('k24 log line after the switch'));
        await wait(4500);
        const posts = (await page.evaluate(() => window.__probeFetches.slice())).filter((entry) => entry.t >= t && /\/api\/ui-log/.test(entry.url)).map((entry) => {
            try { const body = JSON.parse(entry.body); return { page: body.page, messages: body.entries.map((item) => item.message), projects: [...new Set(body.entries.map((item) => item.project))] }; }
            catch { return { page: /"page"\s*:\s*"([^"]*)"/.exec(entry.body)?.[1], messages: [entry.body.slice(0, 200)], projects: [] }; }
        });
        const pageOf = (marker) => posts.find((post) => post.messages.some((message) => message.includes(marker)))?.page;
        const projectOf = (value) => new URLSearchParams(String(value ?? '').replace(/^[^?]*/, '')).get('project');
        const mixed = posts.filter((post) => post.projects.some((project) => project && project !== projectOf(post.page)));
        check('K24', 'A log line is posted under the page it was written on: before the switch the old one, after it the new one',
            projectOf(pageOf('k24 log line before the switch')) === here && projectOf(pageOf('k24 log line after the switch')) === other && mixed.length === 0,
        { from: here, to: other, before: pageOf('k24 log line before the switch'), after: pageOf('k24 log line after the switch'), posts: posts.map((post) => ({ page: post.page, projects: post.projects, messages: post.messages.slice(0, 4).map((message) => message.slice(0, 80)) })), mixed: mixed.length });
    }
    // 5. Only with --with-download: it deletes the cached model and downloads it again (567 MB from Hugging Face).
    if (!WITH_DOWNLOAD) return;
    //    A download still running at the switch: the configuration of the new project shows how far it got and follows it.
    //    The cached model of this profile is deleted first and downloaded again; the download ends in the new project.
    {
        const bar = async () => page.locator('.cbm-chat-loading').first().evaluate((element) => ({ value: element.querySelector('progress')?.value ?? null, file: element.querySelector('span')?.textContent ?? '' })).catch(() => null);
        await openConfig(page);
        await page.locator('dialog.cbm-agent-settings').getByRole('button', { name: 'Delete cached model', exact: true }).click();
        await page.waitForFunction(() => /Cached model files deleted/.test(document.querySelector('dialog.cbm-agent-settings')?.textContent ?? ''), null, { timeout: 30000 }).catch(() => {});
        await page.locator('dialog.cbm-agent-settings button').filter({ hasText: /^Download & load$/ }).click();
        const downloading = await page.waitForFunction(() => {
            const element = document.querySelector('.cbm-chat-loading');
            return /onnx/.test(element?.querySelector('span')?.textContent ?? '') && (element?.querySelector('progress')?.value ?? 0) > 2;
        }, null, { timeout: 120000 }).then(() => true).catch(() => false);
        const atSwitch = await bar();
        await page.locator('.cbm-chat-loading').first().scrollIntoViewIfNeeded().catch(() => {});
        await page.locator('.cbm-chat-loading').first().screenshot({ path: join(OUT, 'K24', 'download-before-switch-bar.png') }).catch(() => {});
        await shot(page, 'K24', 'download-before-switch', 'The model downloading in the configuration, just before the switch (crop of the bar: download-before-switch-bar.png).');
        await closeConfig(page);
        const here = await projectShown(page), other = here === CONTROL ? PROJECT : CONTROL;
        const before = await pageState(page), mark = await markWorker(page), t0 = Date.now();
        await switchTo(page, other);
        const lamp = [];
        for (const at of [0, 250, 1000]) { const delay = at - (Date.now() - t0); if (delay > 0) await wait(delay); lamp.push(await agentLabel(page)); }
        await openConfig(page);
        const shown = await bar();
        await page.locator('.cbm-chat-loading').first().scrollIntoViewIfNeeded().catch(() => {});
        await page.locator('.cbm-chat-loading').first().screenshot({ path: join(OUT, 'K24', 'download-after-switch-bar.png') }).catch(() => {});
        await shot(page, 'K24', 'download-after-switch', `The configuration of ${other} right after the switch: the bar shows how far the download of the last project got, with its file (crop: download-after-switch-bar.png).`);
        await wait(2500);
        const later = await bar();
        const active = await agentActive(page, 300000);
        await shot(page, 'K24', 'download-done-in-new-project', `The download ended in ${other}: the model is loaded.`);
        await closeConfig(page);
        const after = await pageState(page);
        const prepares = (await sentSince(page, mark, 'prepare')).length;
        check('K24', 'A download still running at a switch shows its progress in the configuration of the new project and ends there, without a second worker or load',
            // Each file of the model counts from 0 to 100: a later value is compared within the same file.
            downloading && (atSwitch?.value ?? 0) > 2 && (shown?.value ?? 0) > 0 && /onnx/.test(shown?.file ?? '') && (shown?.file !== atSwitch?.file || shown.value >= atSwitch.value)
            && (!later || later.file !== shown?.file || later.value >= shown.value)
            && lamp.every((label) => /Agent loading/.test(label ?? '')) && active && after.workers === before.workers && after.id === before.id && prepares === 0,
        { from: here, to: other, downloading, atSwitch, shownAfterSwitch: shown, twoSecondsLater: later, lamp, active, modelWorkersCreated: after.workers - before.workers, prepares, samePage: after.id === before.id });
    }
}

async function k24(page) {
    // Fresh page loads of both projects are the reference for what each view shows (no switch involved).
    const fresh = await page.context().newPage();
    const reference = {};
    for (const project of [PROJECT, CONTROL]) {
        await open(fresh, project, 'explore');
        await wait(4000);
        reference[project] = await viewTexts(fresh, `fresh-${project}`, false);
        await shot(fresh, 'K24', `fresh-${project}-system-logs`, `Reference: a fresh page load of ${project} (last view of the reference pass).`);
    }
    await fresh.close();
    await save('K24', 'reference.json', reference);
    // The flow of the hand test (18:04): django-demo with the agent active and a selection, then cbm, then back.
    await open(page, PROJECT, 'galaxy');
    await galaxyReady(page);
    const loaded = await loadModel(page);
    await openChat(page);
    await select(page, 'JSONBAgg');
    await scopeSettled(page);
    await explanationDone(page);
    await ask(page, 'Who calls JSONBAgg?');
    await shot(page, 'K24', 'before-switch', `${PROJECT} with the agent active, JSONBAgg selected and a listed answer in the chat.`);
    const page0 = await page.evaluate(() => ({ id: window.__probePage, origin: performance.timeOrigin, workers: window.__probeModelWorkers }));
    const requestsBefore = modelRequests.length;
    const legs = [];
    for (const [from, to, how] of [[PROJECT, CONTROL, 'switcher'], [CONTROL, PROJECT, 'switcher'], [PROJECT, CONTROL, 'back'], [CONTROL, PROJECT, 'forward']]) {
        const mark = await markWorker(page);
        const t = await now(page);
        const workspaceBefore = await activeWorkspace(page);
        const t0 = Date.now();
        if (how === 'switcher') await switchTo(page, to);
        else if (how === 'back') await page.goBack({ waitUntil: 'commit' });
        else await page.goForward({ waitUntil: 'commit' });
        const frames = await lampFrames(page, `${how}-to-${to}`, t0);
        await page.waitForFunction((name) => new URLSearchParams(location.search).get('project') === name, to, { timeout: 30000 }).catch(() => {});
        const shown = await page.locator('.atlas-project-switcher summary').first().getAttribute('aria-label').catch(() => '');
        const workspaceAfter = await activeWorkspace(page);
        const chat = await page.locator('.cbm-chat-transcript').innerText().catch(() => '');
        // Every workspace on every leg, Back and Forward included.
        const { texts, galaxy } = await viewTexts(page, `${how}-to-${to}`, true);
        const views = Object.entries(texts).map(([id, text]) => {
            const stale = PROJECT_MARKERS[from].exec(text)?.[0];
            const expected = pathTokens(reference[to].texts[id] ?? '');
            const unexpected = [...pathTokens(text)].filter((token) => !expected.has(token) && PROJECT_MARKERS[from].test(token));
            return { id, stale: stale ?? null, unexpected: unexpected.slice(0, 5), chars: text.length, referenceChars: (reference[to].texts[id] ?? '').length, sameAsFresh: sameText(text, reference[to].texts[id] ?? '') };
        });
        await wait(1500);
        const leaks = await requestsNaming(page, t, to, from);
        const state = await page.evaluate(() => ({ id: window.__probePage, origin: performance.timeOrigin, workers: window.__probeModelWorkers }));
        const prepares = (await workerMessages(page)).slice(mark).filter((entry) => entry.kind === 'prepare').length;
        legs.push({ from, to, how, project: shown, url: await page.evaluate(() => location.search), workspace: { before: workspaceBefore, after: workspaceAfter }, samePage: state.id === page0.id && state.origin === page0.origin,
            modelWorkersCreated: state.workers - page0.workers, prepares, lamp: frames.map(({ at, label }) => ({ at, label })),
            chatHasOldTurn: to === CONTROL ? /Who calls JSONBAgg/.test(chat) : null, chatKeptOwnTurn: to === PROJECT ? /Who calls JSONBAgg/.test(chat) : null,
            views, galaxy, galaxyFresh: reference[to].galaxy, galaxySameAsFresh: sameGraph(galaxy, reference[to].galaxy), requestsAfterSwitch: leaks.total, requestsNamingOldProject: leaks.naming });
    }
    const downloads = modelRequests.length - requestsBefore;
    await save('K24', 'legs.json', legs);
    const legOk = (leg) => leg.samePage && leg.workspace.before === leg.workspace.after && leg.modelWorkersCreated === 0 && leg.prepares === 0 && leg.lamp.every((frame) => /Agent active/.test(frame.label ?? ''))
        && new RegExp(leg.to).test(leg.project ?? '') && leg.requestsNamingOldProject.length === 0 && leg.views.length === K24_VIEWS.length && leg.views.every((view) => !view.stale && !view.unexpected.length) && leg.galaxySameAsFresh
        && (leg.to !== CONTROL || leg.chatHasOldTurn === false) && (leg.to !== PROJECT || leg.how !== 'switcher' || leg.chatKeptOwnTurn === true);
    check('K24', 'A project switch stays in the page: the model stays loaded (no reload, no new worker, lamp active), every workspace shows only the new project and the Galaxy its graph, no request names the old one; Back/Forward the same, all views checked',
        loaded && downloads === 0 && legs.every(legOk),
        { modelDownloads: downloads, legs: legs.map((leg) => ({ ...leg, views: leg.views.filter((view) => view.stale || view.unexpected.length), viewsChecked: leg.views.length,
            viewsSameAsFresh: leg.views.filter((view) => view.sameAsFresh).map((view) => view.id), viewsDifferingFromFresh: leg.views.filter((view) => !view.sameAsFresh).map((view) => view.id),
            requestsNamingOldProject: leg.requestsNamingOldProject.slice(0, 5) })) });
    await k24Situations(page, reference);
    // The configuration says what a switch does with the model.
    await openConfig(page);
    const tooltip = await page.locator('#cbm-chat-auto-load').locator('xpath=..').getAttribute('title').catch(() => '');
    await shot(page, 'K24', 'config-tooltip', 'The agent configuration; the tooltip of "Load the chosen model on start" says a project switch keeps the model in the page.');
    await closeConfig(page);
    check('K24', 'The setting tooltip says a switch keeps the model in the page and the option is for opening or reloading', /A project switch stays in this page and keeps a loaded model without loading it again/.test(tooltip ?? '')
        && /opened or reloaded/.test(tooltip ?? ''), { tooltip });
}

/* ------------------------------------------------------------------ */

const CHECKS = { K15: k15, K16: k16, K11: k11, K17: k17, K14: k14, K7: k7, K4: k4, K1: k1, K12: k12, K10: k10, K24: k24 };
await mkdir(OUT, { recursive: true });
const context = await chromium.launchPersistentContext(PROFILE, {
    headless: !HEADED, viewport: VIEWPORT, deviceScaleFactor: 2, args: ['--enable-unsafe-webgpu', '--ignore-gpu-blocklist', '--use-angle=metal', '--enable-gpu'],
});
await context.addInitScript(pageProbe);
// Model downloads, from the page or its workers: a cached load makes none.
context.on('request', (request) => { if (/huggingface\.co|hf\.co/.test(request.url())) modelRequests.push(request.url()); });
const page = context.pages()[0] ?? await context.newPage();
const pageErrors = [];
page.on('pageerror', (error) => { pageErrors.push(error.message); console.error(`[pageerror] ${error.message}`); });
for (const id of ALL.filter((item) => ONLY.includes(item))) {
    try {
        await freshHistory(page);
        await CHECKS[id](page);
    } catch (error) {
        check(id, 'aborted', false, String(error?.message ?? error).split('\n')[0]);
        await shot(page, id, 'aborted', 'State when the check aborted.').catch(() => {});
    }
    const list = images.get(id) ?? [];
    const result = results.filter((item) => item.id === id);
    await save(id, 'index.md', [`# ${id}`, '', ...result.map((item) => `${item.pass ? 'PASS' : 'FAIL'} - ${item.title}\n\n\`\`\`json\n${JSON.stringify(item.measured, null, 2)}\n\`\`\``), '',
        '| Image | What it shows |', '|---|---|', ...list.map((item) => `| [${item.file}](${item.file}) | ${item.proves} |`), ''].join('\n'));
}
await context.close();
await writeFile(join(OUT, 'report.json'), JSON.stringify({ origin: ORIGIN, project: PROJECT, results, pageErrors }, null, 2));
const passed = results.filter((item) => item.pass).length;
console.log(`${passed}/${results.length} passed${pageErrors.length ? `; ${pageErrors.length} page errors` : ''}`);
if (passed !== results.length) process.exitCode = 1;
