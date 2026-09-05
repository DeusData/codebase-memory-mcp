#!/usr/bin/env node

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';

import {
    countListeners,
    findFreePort,
    indexRepository,
    sleep,
    startServer,
    stopServer,
} from './lib/cbm-server.mjs';
import { READABILITY_EXCLUSIONS, measureReadability } from './lib/readability.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w14';
const CANONICAL_OUT_DIR = join(ROOT, 'verification', 'w14');
const OUT_DIR = process.env.W14_OUT_DIR === undefined || process.env.W14_OUT_DIR.trim() === ''
    ? CANONICAL_OUT_DIR
    : resolve(process.env.W14_OUT_DIR);
const OUT_JSON = join(OUT_DIR, 'symbols.json');
const BEFORE_LEVELS = join(CANONICAL_OUT_DIR, 'before-levels.json.gz.b64');
const MIN_PORT = 4680;
const VIEWPORT = { width: 1680, height: 1050 };
const SIDECAR_ORIGIN = 'http://127.0.0.1:4141';

const LEVELS = [
    { value: 0, name: 'vibe coder' },
    { value: 1, name: 'junior' },
    { value: 2, name: 'medior' },
    { value: 3, name: 'senior' },
    { value: 4, name: 'architect' },
];

const SYMBOLS = [
    { name: 'createUser', file: 'src/services/userService.ts' },
    { name: 'getOrder', file: 'src/services/orderService.ts' },
    { name: 'listUsers', file: 'src/services/userService.ts' },
    { name: 'query', file: 'src/repo/db.ts' },
    { name: 'toUser', file: 'src/services/userService.ts' },
    { name: 'insert', file: 'src/repo/db.ts' },
    { name: 'hotspotScan', file: 'src/repo/db.ts' },
    { name: 'validateUser', file: 'src/util/validate.ts' },
];

const UNUSED_IMPORT_PROBE = {
    name: 'registerOrderRoutes',
    file: 'src/routes/orders.ts',
};

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
    '--disable-features=Translate,OptimizationHints,MediaRouter,AutofillServerCommunication',
    '--use-angle=swiftshader',
    '--enable-unsafe-swiftshader',
    '--ignore-gpu-blocklist',
];

const log = (...parts) => console.log('[smoke-w14]', ...parts);

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
        child.stdout.on('data', (data) => { out += data.toString(); });
        child.stderr.on('data', (data) => { out += data.toString(); });
        child.on('error', (error) => done({ code: 127, out: out + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out }));
    });
}

async function openApp(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
    await page.waitForFunction(() => globalThis.__atlasTwin !== undefined, undefined, { timeout: 30000 });
}

async function openSymbol(page, target) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(target.name, { delay: 15 });
    await page.waitForSelector(
        `[data-testid="atlas-search-row"][data-name="${target.name}"]`,
        { timeout: 30000 },
    );
    await page.click(`[data-testid="atlas-search-row"][data-name="${target.name}"]`);
    await page.waitForFunction(
        (name) => globalThis.__atlasTwin?.symbol === name,
        target.name,
        { timeout: 40000 },
    );
    await page.waitForFunction(
        () => document.querySelector('[data-testid="atlas-twin"]')?.getAttribute('data-status') === 'ready',
        undefined,
        { timeout: 40000 },
    );
    await page.waitForTimeout(300);
}

async function setLevel(page, value) {
    await page.locator('[data-testid="atlas-twin-depth"]').fill(String(value));
    await page.waitForFunction(
        (level) => globalThis.__atlasTwin?.level === level,
        value,
        { timeout: 15000 },
    );
    await page.waitForTimeout(150);
}

const clean = (value) => (value ?? '').replace(/\s+/g, ' ').trim();

async function bodyReading(page) {
    return page.evaluate(() => {
        const body = document.querySelector('.atlas-twin-body');
        const tidy = (value) => (value ?? '').replace(/\s+/g, ' ').trim();
        if (body === null) {
            return null;
        }
        return {
            text: tidy(body.textContent),
            subject: tidy(body.querySelector('[data-testid="codeatlas-twin-subject-line"]')?.textContent),
            overview: tidy(body.querySelector('[data-testid="codeatlas-twin-prose"] '
                + '[data-testid="codeatlas-twin-paragraph"]')?.textContent),
            marks: [...body.querySelectorAll('[data-testid]')]
                .map((node) => node.getAttribute('data-testid') ?? '')
                .filter(Boolean),
            steps: globalThis.__atlasTwin?.pseudocode?.numbered ?? 0,
        };
    });
}

async function facetSignatures(page) {
    return page.evaluate(() => [...document.querySelectorAll('.atlas-twin-body [data-testid]')]
        .map((node) => `${node.getAttribute('data-testid')}:${(node.textContent ?? '').replace(/\s+/g, ' ').trim()}`)
        .sort());
}

function removedFrom(before, after) {
    const remaining = [...after];
    const removed = [];
    for (const item of before) {
        const at = remaining.indexOf(item);
        if (at < 0) {
            removed.push(item);
        } else {
            remaining.splice(at, 1);
        }
    }
    return removed;
}

async function measureFacets(page, symbol) {
    await setLevel(page, 2);
    await page.click('[data-testid="atlas-twin-tab-facts"]');
    const buttons = page.locator('.atlas-twin-facet');
    const count = await buttons.count();
    const readings = [];
    for (let index = 0; index < count; index += 1) {
        const button = buttons.nth(index);
        const label = clean(await button.textContent());
        const facet = await button.getAttribute('data-facet');
        if ((await button.getAttribute('data-on')) !== 'true') {
            await button.click();
            await page.waitForTimeout(100);
        }
        const before = await facetSignatures(page);
        await button.click();
        await page.waitForTimeout(120);
        const after = await facetSignatures(page);
        await button.click();
        await page.waitForTimeout(120);
        const restored = await facetSignatures(page);
        readings.push({
            symbol,
            label,
            facet,
            shown: true,
            marksRemoved: removedFrom(before, after),
            removesSomething: removedFrom(before, after).length > 0,
            restores: JSON.stringify(restored) === JSON.stringify(before),
        });
    }
    return readings;
}

async function measureFlow(page, target, alreadyOpen = false) {
    if (!alreadyOpen) {
        await openSymbol(page, target);
    }
    await page.keyboard.press('Escape');
    const open = await page.locator('[data-testid="atlas-explain"]').getAttribute('data-open');
    const tab = await page.locator('[data-testid="atlas-explain"]').getAttribute('data-tab');
    if (open !== 'true' || tab !== 'flow') {
        await page.click('[data-testid="atlas-twin-subject"]');
    }
    await page.waitForFunction(
        () => document.querySelector('[data-testid="atlas-flow-overlay"]')
            ?.getAttribute('data-state') === 'ready',
        undefined,
        { timeout: 50000 },
    );
    await page.waitForTimeout(250);
    return page.evaluate(() => {
        const overlay = document.querySelector('[data-testid="atlas-flow-overlay"]');
        const tidy = (value) => (value ?? '').replace(/\s+/g, ' ').trim();
        return {
            steps: globalThis.__atlasFlow?.steps ?? -1,
            svgBoxes: overlay?.querySelectorAll('svg rect').length ?? 0,
            pagerButtons: overlay?.querySelectorAll(
                '[data-testid="atlas-flow-prev"], [data-testid="atlas-flow-next"]',
            ).length ?? 0,
            reason: tidy(overlay?.querySelector('[data-testid="atlas-flow-empty"], [data-testid="atlas-flow-message"]')?.textContent),
        };
    });
}

async function importReading(page, target) {
    await openSymbol(page, target);
    await page.click('[data-testid="atlas-pseudocode-toggle"]');
    await page.waitForSelector('[data-testid="atlas-pseudocode-imports"]', { timeout: 50000 });
    await page.waitForTimeout(300);
    return page.evaluate(() => {
        const group = document.querySelector('[data-testid="atlas-pseudocode-imports"]');
        const tidy = (value) => (value ?? '').replace(/\s+/g, ' ').trim();
        return {
            entries: Number(group?.getAttribute('data-entries') ?? 0),
            findings: Number(group?.getAttribute('data-findings') ?? 0),
            text: tidy(group?.textContent),
        };
    });
}

async function sourceUseReading() {
    const source = await readFile(join(FIXTURE, UNUSED_IMPORT_PROBE.file), 'utf8');
    const outsideImportDeclarations = source.split('\n')
        .filter((line) => !/^\s*import\b/.test(line))
        .join('\n');
    return {
        sourceImportCount: (source.match(/^\s*import\s*\{[^}]*\binsert\b[^}]*\}\s*from\s*['"][^'"]+['"];?\s*$/gm) ?? []).length,
        sourceUseCount: (outsideImportDeclarations.match(/\binsert\b/g) ?? []).length,
    };
}

async function beforeTextReading() {
    const encoded = await readFile(BEFORE_LEVELS, 'utf8');
    const baseline = JSON.parse(gunzipSync(Buffer.from(encoded.trim(), 'base64')).toString('utf8'));
    const expected = new Set(SYMBOLS.map((target) => target.name));
    if (!Array.isArray(baseline.levels) || baseline.levels.length !== 40) {
        throw new Error('stored W14 baseline does not contain 40 readings');
    }
    const masked = baseline.levels.map((entry) => {
        if (!expected.has(entry.symbol) || typeof entry.text !== 'string') {
            throw new Error('stored W14 baseline contains an unexpected reading');
        }
        const escaped = entry.symbol.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        return entry.text.replace(new RegExp(escaped, 'g'), '<symbol>');
    });
    return {
        textsBeforeMeasured: masked.length,
        distinctTextsBefore: new Set(masked).size,
    };
}

async function screenshotReading(page, level) {
    return page.evaluate((expectedLevel) => {
        const isVisible = (node) => {
            const style = globalThis.getComputedStyle(node);
            const rect = node.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden'
                && Number(style.opacity) > 0 && rect.width > 0 && rect.height > 0;
        };
        const tooltipSelector = '[role="tooltip"], [data-tooltip], .atlas-tooltip, .atlas-hint-tooltip';
        return {
            symbol: globalThis.__atlasTwin?.symbol ?? null,
            level: document.querySelector('[data-testid="atlas-twin-depth-name"]')?.textContent?.trim() ?? null,
            expectedLevel,
            steps: globalThis.__atlasTwin?.pseudocode?.numbered ?? 0,
            visibleTooltips: [...document.querySelectorAll(tooltipSelector)].filter(isVisible).length,
        };
    }, level);
}

async function ask(page, question) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(question, { delay: 8 });
    await page.waitForTimeout(700);
    await input.press('Enter');
}

async function screenshot(page, name) {
    await page.screenshot({ path: join(OUT_DIR, name), fullPage: false });
}

async function clearTooltips(page) {
    await page.keyboard.press('Escape');
    await page.mouse.move(VIEWPORT.width - 1, VIEWPORT.height - 1);
    await page.waitForFunction(() => [...document.querySelectorAll('[data-testid="atlas-hint"]')]
        .every((node) => {
            const style = globalThis.getComputedStyle(node);
            return style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0;
        }), undefined, { timeout: 5000 });
}

async function main() {
    const report = {
        symbols: {},
        comparedWithNameMasked: true,
        identicalGroups: [],
        distinctTexts: 0,
        textsBeforeMeasured: 0,
        distinctTextsBefore: 0,
        nameInBody: 0,
        leafLeadsPositive: false,
        leafLeads: [],
        negationsPerLeaf: 0,
        sharedStateNamed: false,
        callersNamed: 0,
        facetsAllRemove: false,
        facets: [],
        noEmptyDiagram: false,
        neverBlankOverlay: false,
        flows: {},
        importFindingIsFileLevel: false,
        importReadings: {},
        selfRelativisingNote: false,
        menusDoNotOverlap: false,
        everyTargetStillReachable: false,
        shortcutsStillWork: false,
        helpText: '',
        topBar: [],
        tabBar: [],
        menuOverlap: [],
        unreachable: [],
        noTokenLineWithoutCards: false,
        overlapViolations: 0,
        clippingViolations: 0,
        cutWithoutHint: 0,
        port: 0,
        leftoverProcesses: 0,
        screenshotReadings: {},
    };
    const extras = { blockedRequests: [], consoleErrors: [], pageErrors: [], ports: {}, imports: {}, facetRuns: [] };
    let home;
    let runtimeDir;
    let serverChild;
    let proxy;
    let browser;
    let serverPort = 0;
    let uiPort = 0;

    try {
        if (!existsSync(BINARY) || !existsSync(FIXTURE)) {
            throw new Error('required local binary or fixture is missing');
        }
        await mkdir(OUT_DIR, { recursive: true });
        Object.assign(report, await beforeTextReading());
        const build = await run('npm', ['run', 'build']);
        if (build.code !== 0) {
            throw new Error(`build failed: ${build.out.slice(-800)}`);
        }
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w14-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w14-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: [] });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        extras.ports = { serverPort, uiPort };

        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const context = await browser.newContext({ viewport: VIEWPORT });
        const origin = `http://127.0.0.1:${uiPort}`;
        await context.route('**/*', async (route) => {
            const url = route.request().url();
            if (url.startsWith(origin) || url.startsWith('data:') || url.startsWith('blob:')) {
                await route.continue();
            } else {
                extras.blockedRequests.push(url);
                await route.abort();
            }
        });
        await context.route(`${SIDECAR_ORIGIN}/**`, async (route) => {
            const path = route.request().url().slice(SIDECAR_ORIGIN.length);
            const json = (body) => route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify(body),
            });
            if (path.startsWith('/health')) return json({ status: 'ok' });
            if (path.startsWith('/props')) {
                return json({ model_path: 'models/w14-local-stub.gguf', n_ctx: 4096, total_slots: 1 });
            }
            if (path.startsWith('/v1/models')) {
                return json({ data: [{ id: 'models/w14-local-stub.gguf', object: 'model' }] });
            }
            return route.fulfill({ status: 404, contentType: 'text/plain', body: 'not used' });
        });
        const page = await context.newPage();
        page.on('console', (message) => {
            if (message.type() === 'error') extras.consoleErrors.push(message.text());
        });
        page.on('pageerror', (error) => extras.pageErrors.push(String(error)));
        await openApp(page, origin);

        const maskedTexts = [];
        for (const target of SYMBOLS) {
            await openSymbol(page, target);
            const levels = {};
            for (const level of LEVELS) {
                await setLevel(page, level.value);
                const body = await bodyReading(page);
                if (body === null) throw new Error(`${target.name}/${level.name} has no body`);
                const namesSymbol = body.text.includes(target.name);
                levels[level.name] = {
                    chars: body.text.length,
                    text: body.text,
                    namesSymbol,
                    marks: [...new Set(body.marks)].sort(),
                };
                if (namesSymbol) report.nameInBody += 1;
                const escaped = target.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                maskedTexts.push({
                    symbol: target.name,
                    level: level.name,
                    text: body.text.replace(new RegExp(escaped, 'g'), '<symbol>'),
                });
                if (level.value === 0 && ['query', 'toUser', 'insert'].includes(target.name)) {
                    report.leafLeads.push({ symbol: target.name, text: body.subject });
                    const negatives = body.overview.match(/\b(?:no|not|nothing|never)\b/gi) ?? [];
                    report.negationsPerLeaf = Math.max(report.negationsPerLeaf, negatives.length);
                }
            }
            report.symbols[target.name] = { picked: true, file: target.file, levels };
            extras.facetRuns.push(...await measureFacets(page, target.name));
        }

        const groups = new Map();
        for (const entry of maskedTexts) {
            const list = groups.get(entry.text) ?? [];
            list.push(`${entry.symbol}/${entry.level}`);
            groups.set(entry.text, list);
        }
        report.identicalGroups = [...groups.values()].filter((group) => group.length > 1);
        report.distinctTexts = groups.size;
        report.leafLeadsPositive = report.leafLeads.length >= 3
            && report.leafLeads.every((lead) => /\b\d+\s+lines\b/.test(lead.text)
                && !/^(It does not|It never|Nothing|There is no)/i.test(lead.text));
        report.sharedStateNamed = /\brows\b/.test(report.symbols.query.levels['vibe coder'].text);
        const queryTexts = LEVELS.map((level) => report.symbols.query.levels[level.name].text);
        report.callersNamed = ['getOrder', 'listUsers', 'hotspotScan']
            .filter((name) => queryTexts.every((text) => text.includes(name))).length;

        const facetLabels = [...new Set(extras.facetRuns.map((entry) => entry.label))];
        report.facets = facetLabels.map((label) => {
            const runs = extras.facetRuns.filter((entry) => entry.label === label);
            return {
                label,
                shown: runs.length > 0,
                removesSomething: runs.length === SYMBOLS.length && runs.every((entry) => entry.removesSomething),
                restores: runs.length === SYMBOLS.length && runs.every((entry) => entry.restores),
            };
        });
        report.facetsAllRemove = report.facets.length >= 5
            && report.facets.every((entry) => entry.removesSomething && entry.restores);

        await openSymbol(page, SYMBOLS.find((entry) => entry.name === 'query'));
        await setLevel(page, 0);
        await clearTooltips(page);
        report.screenshotReadings['leaf-vibe.png'] = await screenshotReading(page, 'vibe coder');
        await screenshot(page, 'leaf-vibe.png');
        await setLevel(page, 1);
        await clearTooltips(page);
        report.screenshotReadings['leaf-junior.png'] = await screenshotReading(page, 'junior');
        await screenshot(page, 'leaf-junior.png');

        for (const name of ['query', 'toUser', 'insert', 'createUser']) {
            report.flows[name] = await measureFlow(
                page,
                SYMBOLS.find((entry) => entry.name === name),
                name === 'query',
            );
            if (name === 'query') await screenshot(page, 'flow-empty.png');
        }
        report.noEmptyDiagram = ['query', 'toUser', 'insert'].every((name) => {
            const flow = report.flows[name];
            return flow.steps === 0 && flow.svgBoxes === 0 && flow.pagerButtons === 0 && flow.reason.length > 20;
        });
        report.neverBlankOverlay = Object.values(report.flows)
            .every((flow) => flow.svgBoxes > 0 || flow.reason.length > 20);

        report.importReadings.getOrder = await importReading(page, SYMBOLS[1]);
        report.importReadings.toUser = await importReading(page, SYMBOLS[4]);
        const unusedProbeReading = await importReading(page, UNUSED_IMPORT_PROBE);
        report.importReadings.unusedProbe = {
            symbol: UNUSED_IMPORT_PROBE.name,
            importName: 'insert',
            ...await sourceUseReading(),
            ...unusedProbeReading,
        };
        extras.imports = report.importReadings;
        report.importFindingIsFileLevel = report.importReadings.getOrder.findings === 0
            && report.importReadings.toUser.findings === 0
            && report.importReadings.unusedProbe.findings === 1
            && report.importReadings.unusedProbe.sourceImportCount === 1
            && report.importReadings.unusedProbe.sourceUseCount === 0
            && /insert/i.test(report.importReadings.unusedProbe.text);
        report.selfRelativisingNote = /still be used by the file|another symbol in the same file may/i.test(
            `${report.importReadings.getOrder.text} ${report.importReadings.toUser.text} ${report.importReadings.unusedProbe.text}`,
        );

        const menus = await page.evaluate(() => {
            const tidy = (value) => (value ?? '').replace(/\s+/g, ' ').replace(/\[|\]/g, '').trim();
            return {
                top: [...document.querySelectorAll('[data-testid="atlas-menu-item"]')].map((node) => tidy(node.textContent)),
                tabs: [...document.querySelectorAll('[data-testid="atlas-explain-tab"]')].map((node) => tidy(node.textContent)),
            };
        });
        report.topBar = menus.top;
        report.tabBar = menus.tabs;
        report.menuOverlap = menus.top.filter((name) => menus.tabs.includes(name));
        report.menusDoNotOverlap = report.menuOverlap.length === 0;
        const targetChecks = [
            ['why', menus.top.some((name) => /why am i here/i.test(name))],
            ['bug hunt', menus.tabs.includes('bug hunt')],
            ['change scope', menus.tabs.includes('change scope')],
            ['settings', menus.top.some((name) => /settings/i.test(name))],
            ['flow', menus.tabs.includes('flow')],
            ['walk', menus.tabs.includes('walk')],
            ['chat', menus.tabs.includes('chat')],
        ];
        report.unreachable = targetChecks.filter((entry) => !entry[1]).map((entry) => entry[0]);
        report.everyTargetStillReachable = report.unreachable.length === 0;
        await page.keyboard.press('Alt+b');
        await page.keyboard.press('Alt+c');
        await page.waitForTimeout(150);
        const activated = await page.evaluate(() => globalThis.__atlasSearch?.activatedMenus ?? []);
        report.shortcutsStillWork = activated.includes('b') && activated.includes('c');
        await screenshot(page, 'menus.png');

        await page.click('[data-menu="?"]');
        await page.waitForSelector('[data-testid="atlas-help"]', { timeout: 15000 });
        report.helpText = clean(await page.locator('[data-testid="atlas-help"]').textContent());
        await page.keyboard.press('Escape');
        await page.waitForFunction(
            () => document.querySelector('[data-testid="atlas-help"]') === null,
            undefined,
            { timeout: 15000 },
        );

        await page.click('[data-menu="a-llm"]');
        await page.waitForFunction(
            () => globalThis.__atlasLlm?.state === 'ready',
            undefined,
            { timeout: 30000 },
        );
        await ask(page, 'Was macht @create?');
        await page.waitForFunction(
            () => (globalThis.__atlasChat?.turns ?? []).some((turn) => turn.status === 'needs-choice'),
            undefined,
            { timeout: 60000 },
        );
        report.noTokenLineWithoutCards = await page.evaluate(() => {
            const turn = [...document.querySelectorAll('[data-testid="atlas-chat-turn"]')]
                .find((node) => node.getAttribute('data-status') === 'needs-choice');
            return turn !== undefined
                && turn.querySelectorAll('[data-testid="atlas-chat-card"]').length === 0
                && turn.querySelector('[data-testid="atlas-chat-provenance"]') === null;
        });

        const readability = await measureReadability(page, READABILITY_EXCLUSIONS);
        report.overlapViolations = readability.overlaps.length;
        report.clippingViolations = readability.clipped.length;
        report.cutWithoutHint = readability.clipped.filter((entry) => entry.kind === 'cut-without-hint').length;
    } catch (error) {
        extras.error = error instanceof Error ? error.stack ?? error.message : String(error);
        log(extras.error);
    } finally {
        if (browser !== undefined) await browser.close().catch(() => undefined);
        if (proxy !== undefined) await proxy.close().catch(() => undefined);
        if (serverChild !== undefined) await stopServer(serverChild).catch(() => undefined);
        const ports = [serverPort, uiPort].filter((port) => port >= MIN_PORT);
        for (let look = 0; look < 15; look += 1) {
            const counts = await Promise.all(ports.map((port) => countListeners(port)));
            report.leftoverProcesses = counts.reduce((sum, count) => sum + count, 0);
            if (report.leftoverProcesses === 0) break;
            await sleep(1000);
        }
        if (home !== undefined) await rm(home, { recursive: true, force: true });
        if (runtimeDir !== undefined) await rm(runtimeDir, { recursive: true, force: true });
        report.extras = extras;
        await mkdir(OUT_DIR, { recursive: true });
        await writeFile(OUT_JSON, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
    }

    const passed = report.distinctTexts === 40
        && report.identicalGroups.length === 0
        && report.textsBeforeMeasured === 40
        && report.distinctTextsBefore === 36
        && report.nameInBody === 40
        && report.leafLeadsPositive
        && report.sharedStateNamed
        && report.callersNamed === 3
        && report.facetsAllRemove
        && report.noEmptyDiagram
        && report.neverBlankOverlay
        && report.importFindingIsFileLevel
        && report.importReadings.getOrder.findings === 0
        && report.importReadings.toUser.findings === 0
        && report.importReadings.unusedProbe.findings === 1
        && report.importReadings.unusedProbe.sourceImportCount === 1
        && report.importReadings.unusedProbe.sourceUseCount === 0
        && !report.selfRelativisingNote
        && report.screenshotReadings['leaf-vibe.png']?.symbol === 'query'
        && report.screenshotReadings['leaf-vibe.png']?.level === 'vibe coder'
        && report.screenshotReadings['leaf-vibe.png']?.steps === 0
        && report.screenshotReadings['leaf-vibe.png']?.visibleTooltips === 0
        && report.screenshotReadings['leaf-junior.png']?.symbol === 'query'
        && report.screenshotReadings['leaf-junior.png']?.level === 'junior'
        && report.screenshotReadings['leaf-junior.png']?.steps === 0
        && report.screenshotReadings['leaf-junior.png']?.visibleTooltips === 0
        && report.menusDoNotOverlap
        && report.everyTargetStillReachable
        && report.shortcutsStillWork
        && report.helpText.length > 100
        && /question mark/i.test(report.helpText)
        && /(?:indexed|index) cards/i.test(report.helpText)
        && /AI button/i.test(report.helpText)
        && !/sent to (?:the )?local model|local model as a question/i.test(report.helpText)
        && report.noTokenLineWithoutCards
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.cutWithoutHint === 0
        && report.leftoverProcesses === 0;
    log(passed ? 'PASS' : 'FAIL', OUT_JSON);
    process.exitCode = passed ? 0 : 1;
}

await main();
