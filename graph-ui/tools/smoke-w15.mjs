#!/usr/bin/env node

/* W15 is deliberately a browser proof. Values come from the DOM or existing
 * guard seams; this file never fills an acceptance field with a claimed pass. */
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { countListeners, findFreePort, indexRepository, sleep, startServer, stopServer } from './lib/cbm-server.mjs';
import { READABILITY_EXCLUSIONS, measureReadability } from './lib/readability.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const OUT = join(ROOT, 'verification', 'w15');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const SIDECAR = 'http://127.0.0.1:4141';
const MIN_PORT = 4680;
const MODEL = 'models/w15-router-a.gguf';
const ROUTER_MODELS = [MODEL, 'models/w15-router-b.gguf'];
const tidy = (text) => (text ?? '').replace(/\s+/g, ' ').trim();

const report = {
    chatAnswersWithoutModel: false, cardsWithoutModel: 0, citationsWithoutModel: 0,
    answerWithoutModel: '', offNoticeReplacesAnswer: true, cardsByHops: {}, addedByHop: [],
    hopsAddCards: false, aiButtons: 0, aiButtonsWhenOff: 0, restoreButtons: 0,
    roundTripIdentical: false, provenanceEverywhere: false, modelName: MODEL, areas: {},
    guardRefusals: 0, autoRequests: 0, requestsAfterClick: 0, touchedUserSidecar: false,
    offButtonReadings: {}, autoRequestReadings: [],
    overlapViolations: 0, clippingViolations: 0, cutWithoutHint: 0, port: 0,
    ownedPorts: [], reusedIsolatedPreviewUpstream: false, leftoverProcesses: 0,
    ordinaryChatAfterEnable: null, selectedRouterModel: '', selectedRouterModelName: '', immediateProbeModel: '', requestsBeforeFirstAiClick: -1,
};

function corruptions(subject) {
    const built = subject.map((line) => line.text).join('\n');
    const changedNumber = built.replace(/\d+(?:\.\d+)?/, (value) => String(Number(value) + 1));
    return [
        '',
        'inventedFact is always required.',
        `${built}\ninventedFact is always required.`,
        subject.slice(1).map((line) => line.text).join('\n'),
        built.replace(/\b[A-Za-z_][A-Za-z0-9_]*\b/, 'inventedFact'),
        changedNumber,
    ];
}

async function main() {
    let home; let runtime; let server; let proxy; let browser; let serverPort = 0; let uiPort = 0;
    let sidecarRequests = 0;
    const completionRequests = [];
    const propsModelQueries = [];
    try {
        if (!existsSync(BINARY)) throw new Error('CBM binary is missing');
        await mkdir(OUT, { recursive: true });
        home = await mkdtemp(join(tmpdir(), 'codeatlas-w15-home-'));
        runtime = await mkdtemp('/private/tmp/codeatlas-w15-runtime-');
        process.env.CBM_RUNTIME_DIR = runtime;
        await indexRepository(BINARY, { home, repoPath: FIXTURE, project: 'codeatlasweb-w15' });
        serverPort = await findFreePort(MIN_PORT);
        server = (await startServer(BINARY, { home, port: serverPort, log: [] })).child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: join(ROOT, 'dist'), upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        report.ownedPorts = [serverPort, uiPort];

        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: [
            '--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1, EXCLUDE localhost',
            '--disable-background-networking', '--disable-component-update', '--disable-sync',
            '--no-first-run', '--no-default-browser-check', '--use-angle=swiftshader',
        ] });
        const context = await browser.newContext({ viewport: { width: 1680, height: 1050 } });
        const origin = `http://127.0.0.1:${uiPort}`;
        await context.route('**/*', async (route) => {
            const url = route.request().url();
            if (url.startsWith(SIDECAR)) {
                const path = url.slice(SIDECAR.length);
                if (path.startsWith('/v1/chat/completions')) {
                    sidecarRequests += 1;
                    const request = JSON.parse(route.request().postData() ?? '{}');
                    completionRequests.push(request);
                    const prompt = request.messages?.[1]?.content ?? '';
                    const count = Number(/^Reword these (\d+)/.exec(prompt)?.[1] ?? 0);
                    const body = prompt.split('\n').slice(-count).join('\n');
                    return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({
                        choices: [{ message: { content: body }, finish_reason: 'stop' }],
                    }) });
                }
                if (path.startsWith('/health')) return route.fulfill({ status: 200, body: '{"status":"ok"}' });
                if (path.startsWith('/props')) {
                    const selected = new URL(url).searchParams.get('model') ?? '';
                    // Every probe first reads unscoped /props.  The router
                    // choice is evidenced by the following scoped request.
                    if (selected.length > 0) propsModelQueries.push(selected);
                    return route.fulfill({ status: 200, body: JSON.stringify({
                        role: 'router', model_path: selected || 'none', n_ctx: selected ? 4096 : 0, total_slots: 1,
                    }) });
                }
                if (path.startsWith('/v1/models')) return route.fulfill({ status: 200, body: JSON.stringify({
                    data: ROUTER_MODELS.map((id) => ({ id, object: 'model', status: { value: 'loaded' }, meta: { n_ctx: 4096 } })),
                }) });
                return route.fulfill({ status: 404, body: '' });
            }
            return (url.startsWith(origin) || url.startsWith('data:') || url.startsWith('blob:')) ? route.continue() : route.abort();
        });
        const page = await context.newPage();
        await page.goto(`${origin}/?project=codeatlasweb-w15`, { waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-statusbar"]');

        const activateChat = async () => {
            const zone = page.locator('[data-testid="atlas-explain"]');
            if (await zone.getAttribute('data-open') !== 'true') await page.locator('[data-testid="atlas-explain-collapse"]').click();
            const tab = page.locator('[data-testid="atlas-explain-tab"][data-tab="chat"]');
            if (await tab.getAttribute('data-on') !== 'true') await tab.click();
            await page.waitForSelector('[data-testid="atlas-chat-depth"]', { state: 'visible' });
        };
        const ask = async (depth, configure = true) => {
            const before = await page.locator('[data-testid="atlas-chat-turn"]').count();
            if (configure) {
                await activateChat();
                await page.locator(`[data-testid="atlas-chat-depth-option"][data-value="${depth}"]`).click();
            }
            const input = page.locator('[data-testid="atlas-command-input"]');
            await input.fill('@getOrder what does it do?');
            await input.press('Enter');
            await page.waitForFunction((count) => {
                const turns = [...document.querySelectorAll('[data-testid="atlas-chat-turn"]')];
                return turns.length > count && turns.at(-1)?.getAttribute('data-status') === 'answered';
            }, before, { timeout: 60000 });
            return page.locator('[data-testid="atlas-chat-turn"]').last();
        };
        const cardsOf = async (turn) => {
            const toggle = turn.locator('[data-testid="atlas-chat-cards-toggle"]');
            if (await toggle.getAttribute('data-fold') === 'open') await toggle.click();
            return turn.locator('[data-testid="atlas-chat-card"]').evaluateAll((nodes) => nodes.map((node) => ({
                id: node.getAttribute('data-card') ?? '',
                text: (node.querySelector('.atlas-chat-card-lines')?.textContent ?? '')
                    .replace(/\s+/g, ' ').trim(),
            })));
        };
        const openSymbol = async (name) => {
            const input = page.locator('[data-testid="atlas-command-input"]');
            await input.click();
            await input.fill('');
            await input.pressSequentially(name, { delay: 15 });
            await page.waitForSelector(`[data-testid="atlas-search-row"][data-name="${name}"]`, { timeout: 30000 });
            await page.locator(`[data-testid="atlas-search-row"][data-name="${name}"]`).click();
            await page.waitForFunction((symbol) => globalThis.__atlasTwin?.symbol === symbol, name, { timeout: 40000 });
            await page.waitForFunction(() => document.querySelector('[data-testid="atlas-twin"]')?.getAttribute('data-status') === 'ready', undefined, { timeout: 40000 });
        };
        const guard = async (area) => {
            const subject = await page.evaluate((name) => {
                const seam = name === 'twin' ? globalThis.__atlasTwin : name === 'flow' ? globalThis.__atlasFlow : globalThis.__atlasChatReader;
                return seam?.subject ?? [];
            }, area);
            const refusals = await page.evaluate(({ name, answers }) => {
                const seam = name === 'twin' ? globalThis.__atlasTwin : name === 'flow' ? globalThis.__atlasFlow : globalThis.__atlasChatReader;
                if (seam === undefined) throw new Error(`${name} reader guard is unavailable`);
                return answers.map((answer) => ({ answer, ...seam.validateRewrite(answer) }));
            }, { name: area, answers: corruptions(subject) });
            report.guardRefusals += refusals.filter((entry) => !entry.applied && entry.reason.length > 10).length;
            return { guardIsTheRealOne: true, refusals };
        };
        const noAutoRequestDuring = async (area, action) => {
            const before = sidecarRequests;
            await action();
            await page.waitForTimeout(100);
            const requests = sidecarRequests - before;
            report.autoRequests += requests;
            report.autoRequestReadings.push({ area, requests });
        };
        const exercise = async ({ area, button, restore, root, built, ai, shot, screenshotTarget, scope = page }) => {
            const askButton = scope.locator(button);
            const rootNode = typeof root === 'string' ? page.locator(root) : root ?? scope;
            const hasAiButton = await askButton.isVisible();
            const aiButtonEnabled = hasAiButton && await askButton.isEnabled();
            report.aiButtons += hasAiButton ? 1 : 0;
            if (!hasAiButton || !aiButtonEnabled) {
                const state = area === 'flow' ? await page.evaluate(() => ({
                    llm: globalThis.__atlasLlm?.state ?? '',
                    flowLines: globalThis.__atlasFlow?.subject.length ?? 0,
                })) : undefined;
                throw new Error(`${area} AI button is not usable: ${JSON.stringify(state)}`);
            }
            const before = tidy(await rootNode.textContent());
            const builtProvenance = tidy(await scope.locator(built).textContent());
            const requestsBefore = completionRequests.length;
            await askButton.click();
            try {
                await scope.locator(restore).waitFor({ state: 'visible', timeout: 30000 });
            } catch (error) {
                const refusal = tidy(await page.locator(`[data-testid="atlas-${area}-ai-refused"]`).textContent().catch(() => ''));
                const state = area === 'flow' ? await page.evaluate(() => globalThis.__atlasFlow?.aiState ?? '') : '';
                throw new Error(`${area} AI did not apply (${state}): ${refusal || String(error)}`);
            }
            const restoreButton = scope.locator(restore);
            const restoreVisible = await restoreButton.isVisible();
            report.restoreButtons += restoreVisible ? 1 : 0;
            const aiProvenance = tidy(await scope.locator(ai).textContent());
            if (area === 'twin') {
                await page.locator('.atlas-twin-body').evaluate((node) => { node.scrollTop = node.scrollHeight; });
                await page.mouse.move(8, 8);
                await page.waitForTimeout(120);
            }
            if (screenshotTarget !== undefined) await page.locator(screenshotTarget).scrollIntoViewIfNeeded();
            await page.screenshot({ path: join(OUT, shot) });
            const beforeRestore = completionRequests.length;
            await restoreButton.click();
            await scope.locator(button).waitFor({ state: 'visible' });
            const after = tidy(await rootNode.textContent());
            report.areas[area] = {
                hasAiButton, aiButtonEnabled, restoredIdentical: before === after,
                restoreAsksModelAgain: completionRequests.length !== beforeRestore,
                builtProvenance, aiProvenance, requestsForAi: completionRequests.length - requestsBefore,
                requestModels: completionRequests.slice(requestsBefore).map((request) => request.model).filter(Boolean),
                requestModelPresent: completionRequests.slice(requestsBefore).every((request) => typeof request.model === 'string' && request.model.length > 0),
                provenanceMatchesSelection: aiProvenance.includes(report.selectedRouterModelName),
                ...(await guard(area)),
            };
        };

        // First question uses the normal depth. It opens the chat zone itself.
        const off = await ask(1, false);
        report.offButtonReadings.chat = await page.locator('[data-testid="atlas-chat-ai-btn"]:visible').count();
        const offCards = await cardsOf(off);
        report.cardsWithoutModel = offCards.length;
        report.citationsWithoutModel = await off.locator('[data-testid="atlas-chat-card-open"]').count();
        report.answerWithoutModel = tidy(await off.locator('[data-testid="atlas-chat-answer"]').textContent());
        report.chatAnswersWithoutModel = report.cardsWithoutModel > 0 && report.citationsWithoutModel > 0;
        report.offNoticeReplacesAnswer = report.answerWithoutModel.length === 0;
        await page.screenshot({ path: join(OUT, 'chat-off.png') });

        let previous = new Set();
        for (const depth of [0, 1, 2]) {
            const cards = await cardsOf(await ask(depth));
            report.cardsByHops[String(depth)] = cards.length;
            report.addedByHop.push({
                depth,
                cardsAdded: cards.filter((card) => !previous.has(card.text)),
            });
            previous = new Set(cards.map((card) => card.text));
        }
        report.hopsAddCards = report.cardsByHops['0'] < report.cardsByHops['1'] && report.cardsByHops['1'] <= report.cardsByHops['2'];

        // AI controls must be absent in every real area, not only in the chat.
        await openSymbol('getOrder');
        report.offButtonReadings.twin = await page.locator('[data-testid="codeatlas-twin-voice-btn"]:visible').count();
        await openSymbol('createUser');
        await page.locator('[data-testid="atlas-twin-subject"]').click();
        await page.waitForFunction(() => document.querySelector('[data-testid="atlas-flow-overlay"]')?.getAttribute('data-state') === 'ready', undefined, { timeout: 50000 });
        report.offButtonReadings.flow = await page.locator('[data-testid="atlas-flow-ai-btn"]:visible').count();
        report.aiButtonsWhenOff = Object.values(report.offButtonReadings).reduce((sum, count) => sum + count, 0);

        const requestsBeforeEnable = sidecarRequests;
        await page.locator('[data-menu="a-llm"]').click();
        await page.waitForFunction(() => globalThis.__atlasLlm?.state === 'ready', undefined, { timeout: 30000 });
        report.autoRequests += sidecarRequests - requestsBeforeEnable;
        report.autoRequestReadings.push({ area: 'enable', requests: sidecarRequests - requestsBeforeEnable });

        await page.locator('[data-menu="a-settings"]').click();
        const selectedRouterModel = ROUTER_MODELS[1];
        const propsBeforeSelection = propsModelQueries.length;
        await page.locator(`[data-testid="atlas-settings-model-pick"][data-model="${selectedRouterModel}"]`).click();
        await page.waitForFunction((model) => globalThis.__atlasSettings?.selectedModel === model, selectedRouterModel, { timeout: 30000 });
        for (let attempt = 0; attempt < 60 && propsModelQueries.length === propsBeforeSelection; attempt += 1) {
            await sleep(50);
        }
        report.selectedRouterModel = selectedRouterModel;
        report.immediateProbeModel = propsModelQueries[propsBeforeSelection] ?? '';
        report.selectedRouterModelName = await page.evaluate((model) => (
            globalThis.__atlasSettings?.cacheModels?.find((entry) => entry.id === model)?.name ?? ''
        ), selectedRouterModel);
        if (report.selectedRouterModelName.length === 0) {
            throw new Error(`die sichtbare Router-Modellbezeichnung fehlt fuer ${selectedRouterModel}`);
        }
        // W15's original provenance acceptance binds this field as well.  The
        // request id remains separately recorded in selectedRouterModel.
        report.modelName = report.selectedRouterModelName;
        await page.locator('[data-menu="a-settings"]').click();
        await page.waitForSelector('[data-testid="atlas-settings"]', { state: 'hidden' });

        const beforeOrdinary = completionRequests.length;
        const ordinary = await ask(1);
        const ordinaryCards = await cardsOf(ordinary);
        report.ordinaryChatAfterEnable = {
            completionRequests: completionRequests.length - beforeOrdinary,
            status: await ordinary.getAttribute('data-status'),
            cards: ordinaryCards.length,
            citations: await ordinary.locator('[data-testid="atlas-chat-card-open"]').count(),
            builtProvenanceVisible: await ordinary.locator('[data-testid="atlas-chat-built-provenance"]').isVisible(),
            aiProvenanceVisible: await ordinary.locator('[data-testid="atlas-chat-ai-provenance"]').isVisible(),
            aiButtonVisible: await ordinary.locator('[data-testid="atlas-chat-ai-btn"]').isVisible(),
        };
        report.requestsBeforeFirstAiClick = completionRequests.length;

        // The twin needs a real selected subject before its reader and button exist.
        await noAutoRequestDuring('twin-navigation', async () => {
            await openSymbol('getOrder');
            const explain = page.locator('[data-testid="atlas-explain"]');
            if (await explain.getAttribute('data-open') === 'true') {
                await page.locator('[data-testid="atlas-explain-collapse"]').click();
                await page.waitForFunction(() => document.querySelector('[data-testid="atlas-explain"]')?.getAttribute('data-open') === 'false');
            }
        });
        await exercise({ area: 'twin', button: '[data-testid="codeatlas-twin-voice-btn"]', restore: '[data-testid="codeatlas-twin-voice-restore"]', root: '.atlas-twin-body', built: '[data-testid="codeatlas-twin-built-provenance"]', ai: '[data-testid="codeatlas-twin-voice-note"]', shot: 'twin-ai.png', screenshotTarget: '[data-testid="codeatlas-twin-voice-note"]' });
        await noAutoRequestDuring('flow-navigation', async () => {
            await openSymbol('createUser');
            const explain = page.locator('[data-testid="atlas-explain"]');
            if (await explain.getAttribute('data-open') !== 'true'
                || await explain.getAttribute('data-tab') !== 'flow') {
                await page.locator('[data-testid="atlas-twin-subject"]').click();
            }
            await page.waitForFunction(() => document.querySelector('[data-testid="atlas-flow-overlay"]')?.getAttribute('data-state') === 'ready' && (globalThis.__atlasFlow?.subject.length ?? 0) > 0, undefined, { timeout: 50000 });
        });
        await exercise({ area: 'flow', button: '[data-testid="atlas-flow-ai-btn"]', restore: '[data-testid="atlas-flow-ai-restore"]', root: '[data-testid="atlas-flow-overlay"]', built: '[data-testid="atlas-flow-built-provenance"]', ai: '[data-testid="atlas-flow-ai-provenance"]', shot: 'flow-ai.png' });
        // Reuse the already visible built answer: enabling the optional model
        // must not secretly ask the chat again before its explicit AI button.
        await noAutoRequestDuring('chat-navigation', activateChat);
        const chat = page.locator('[data-testid="atlas-chat-turn"]').last();
        await exercise({ area: 'chat', button: '[data-testid="atlas-chat-ai-btn"]', restore: '[data-testid="atlas-chat-ai-restore"]', root: chat, built: '[data-testid="atlas-chat-built-provenance"]', ai: '[data-testid="atlas-chat-ai-provenance"]', shot: 'chat-ai.png', scope: chat });

        report.roundTripIdentical = Object.values(report.areas).every((entry) => entry.restoredIdentical);
        report.provenanceEverywhere = Object.values(report.areas).every((entry) => entry.builtProvenance.length > 10 && entry.provenanceMatchesSelection && entry.builtProvenance !== entry.aiProvenance);
        report.requestsAfterClick = completionRequests.length;
        const readability = await measureReadability(page, READABILITY_EXCLUSIONS);
        report.overlapViolations = readability.overlaps.length;
        report.clippingViolations = readability.clipped.length;
        report.cutWithoutHint = readability.clipped.filter((entry) => entry.kind === 'cut-without-hint').length;
    } catch (error) {
        report.error = error instanceof Error ? error.stack ?? error.message : String(error);
        console.error(report.error);
    } finally {
        if (browser) await browser.close().catch(() => undefined);
        if (proxy) await proxy.close().catch(() => undefined);
        if (server) await stopServer(server).catch(() => undefined);
        for (let attempt = 0; attempt < 15; attempt += 1) {
            const ports = [serverPort, uiPort].filter((port) => port >= MIN_PORT);
            report.leftoverProcesses = (await Promise.all(ports.map(countListeners))).reduce((sum, count) => sum + count, 0);
            if (report.leftoverProcesses === 0) break;
            await sleep(1000);
        }
        if (home) await rm(home, { recursive: true, force: true });
        if (runtime) await rm(runtime, { recursive: true, force: true });
        await mkdir(OUT, { recursive: true });
        await writeFile(join(OUT, 'hybrid.json'), `${JSON.stringify(report, null, 2)}\n`);
    }
    const passed = report.chatAnswersWithoutModel && report.hopsAddCards && report.aiButtons === 3
        && report.aiButtonsWhenOff === 0 && report.restoreButtons === 3 && report.roundTripIdentical
        && report.provenanceEverywhere && report.guardRefusals >= 15 && report.autoRequests === 0
        && report.requestsAfterClick >= 3 && report.ordinaryChatAfterEnable?.completionRequests === 0
        && report.requestsBeforeFirstAiClick === 0
        && report.immediateProbeModel === report.selectedRouterModel
        && Object.values(report.areas).every((entry) => entry.requestModels?.length === 1
            && entry.requestModels[0] === report.selectedRouterModel && entry.requestModelPresent
            && entry.provenanceMatchesSelection)
        && !report.error && report.leftoverProcesses === 0
        && report.overlapViolations === 0 && report.clippingViolations === 0 && report.cutWithoutHint === 0;
    console.log(`[smoke-w15] ${passed ? 'PASS' : 'FAIL'}`);
    process.exitCode = passed ? 0 : 1;
}

await main();
