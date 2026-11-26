#!/usr/bin/env node

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
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w12a';
const OUT_DIR = join(ROOT, 'verification', 'w12a');
const OUT_JSON = join(OUT_DIR, 'command-overlay.json');
const OUT_PNG = join(OUT_DIR, 'command-overlay.png');
const MIN_PORT = 4700;
const VIEWPORT = { width: 1680, height: 1050 };
const CHROMIUM_ARGS = [
    '--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1, EXCLUDE localhost',
    '--disable-background-networking', '--disable-component-update', '--disable-domain-reliability',
    '--disable-client-side-phishing-detection', '--disable-sync', '--disable-default-apps',
    '--no-first-run', '--no-default-browser-check', '--metrics-recording-only', '--no-pings',
    '--disable-breakpad', '--disable-features=Translate,OptimizationHints,MediaRouter,AutofillServerCommunication',
    '--use-angle=swiftshader', '--enable-unsafe-swiftshader', '--ignore-gpu-blocklist',
];

const log = (...parts) => console.log('[smoke-w12a]', ...parts);

const plain = (value) => (value ?? '').replace(/\s+/g, ' ').trim();

async function atCenter(page, selector) {
    return page.locator(selector).first().evaluate((node) => {
        const box = node.getBoundingClientRect();
        const x = box.left + box.width / 2;
        const y = box.top + box.height / 2;
        const hit = document.elementFromPoint(x, y);
        return {
            rect: { x: box.x, y: box.y, width: box.width, height: box.height },
            hit: hit?.closest('[data-testid]')?.getAttribute('data-testid') ?? '',
        };
    });
}

async function cleanupPorts(ports) {
    let leftoverProcesses = 0;
    for (let attempt = 0; attempt < 20; attempt += 1) {
        const counts = await Promise.all(ports.map((port) => countListeners(port)));
        leftoverProcesses = counts.reduce((sum, count) => sum + count, 0);
        if (leftoverProcesses === 0) {
            break;
        }
        await sleep(150);
    }
    return leftoverProcesses;
}

async function main() {
    if (!existsSync(BINARY) || !existsSync(FIXTURE) || !existsSync(join(DIST, 'index.html'))) {
        throw new Error('W12a braucht Binary, Fixture und einen aktuellen dist/-Bau');
    }
    await mkdir(OUT_DIR, { recursive: true });

    let home = null;
    let runtime = null;
    let server = null;
    let proxy = null;
    let browser = null;
    let context = null;
    let serverPort = 0;
    let uiPort = 0;
    const report = {
        symbolSelected: '', commandEmpty: false, commandFocused: false, examplesVisible: false,
        examplesCount: 0, unfoldVisible: false, measuredGeometry: {}, unfoldHitTarget: '',
        unfoldClickSucceeded: false, explainOpened: false, exampleHitTarget: '',
        exampleClickSucceeded: false, exampleText: '', commandAfterExample: '',
        consoleErrors: 0, uncaughtExceptions: 0, leftoverProcesses: 0,
        port: 0, blockedRequests: [], error: null,
    };
    const consoleErrors = [];
    const pageErrors = [];

    try {
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w12a-home-'));
        runtime = await mkdtemp('/private/tmp/codeatlasweb-w12a-run-');
        process.env.CBM_RUNTIME_DIR = runtime;
        await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        serverPort = await findFreePort(MIN_PORT);
        server = (await startServer(BINARY, { home, port: serverPort, log: [] })).child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        const origin = `http://127.0.0.1:${uiPort}`;

        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        context = await browser.newContext({ viewport: VIEWPORT });
        await context.route('**/*', async (route) => {
            const url = route.request().url();
            if (url.startsWith(origin) || url.startsWith('data:') || url.startsWith('blob:')) {
                await route.continue();
                return;
            }
            report.blockedRequests.push(url);
            await route.abort();
        });
        await context.route('**/favicon.ico', (route) => route.fulfill({ status: 204, body: '' }));
        const page = await context.newPage();
        page.on('console', (message) => {
            if (message.type() === 'error') consoleErrors.push(message.text());
        });
        page.on('pageerror', (error) => pageErrors.push(String(error)));
        await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
        await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });

        const command = page.locator('[data-testid="atlas-command-input"]');
        await command.click();
        await command.fill('createUser');
        await page.waitForSelector('[data-testid="atlas-search-row"][data-name="createUser"]', { timeout: 30000 });
        await page.click('[data-testid="atlas-search-row"][data-name="createUser"]');
        await page.waitForFunction(() => globalThis.__atlasTwin?.symbol === 'createUser', undefined, { timeout: 30000 });
        await page.waitForSelector('[data-testid="atlas-command-examples"]', { timeout: 10000 });

        report.symbolSelected = await page.evaluate(() => globalThis.__atlasTwin?.symbol ?? '');
        report.commandEmpty = (await command.inputValue()) === '';
        report.commandFocused = await command.evaluate((node) => document.activeElement === node);
        report.examplesVisible = await page.locator('[data-testid="atlas-command-examples"]').isVisible();
        report.examplesCount = await page.locator('[data-testid="atlas-command-example"]').count();
        report.unfoldVisible = await page.locator('[data-testid="atlas-explain-collapse"]').isVisible();
        const unfold = await atCenter(page, '[data-testid="atlas-explain-collapse"]');
        report.measuredGeometry.unfold = unfold.rect;
        report.unfoldHitTarget = unfold.hit;
        report.measuredGeometry.examples = await page.locator('[data-testid="atlas-command-examples"]').evaluate((node) => {
            const box = node.getBoundingClientRect();
            return { x: box.x, y: box.y, width: box.width, height: box.height };
        });
        await page.screenshot({ path: OUT_PNG });

        try {
            await page.locator('[data-testid="atlas-explain-collapse"]').click({ timeout: 3500 });
            report.unfoldClickSucceeded = true;
            await page.waitForFunction(
                () => document.querySelector('[data-testid="atlas-explain"]')?.getAttribute('data-open') === 'true',
                undefined,
                { timeout: 5000 },
            );
            report.explainOpened = true;
        } catch (error) {
            report.error = plain(error.message ?? error);
        }

        await command.focus();
        await page.waitForSelector('[data-testid="atlas-command-examples"]', { timeout: 5000 });
        const example = page.locator('[data-testid="atlas-command-example"]').first();
        // Der eigene Text-Span ist der sichtbare Befehl, den der Button an
        // onCommandExample weitergibt. Die separate Notiz gehoert nicht zum
        // einzutragenden Befehl.
        report.exampleText = plain(await example.locator('.atlas-command-example-text').innerText());
        report.exampleHitTarget = (await atCenter(page, '[data-testid="atlas-command-example"]')).hit;
        try {
            await example.click({ timeout: 3500 });
            report.exampleClickSucceeded = true;
            report.commandAfterExample = await command.inputValue();
        } catch (error) {
            report.error ??= plain(error.message ?? error);
        }
        await context.close();
        context = null;
        await browser.close();
        browser = null;
        await proxy.close();
        proxy = null;
        await stopServer(server);
        server = null;
    } catch (error) {
        report.error ??= plain(error.message ?? error);
    } finally {
        if (context !== null) await context.close().catch(() => undefined);
        if (browser !== null) await browser.close().catch(() => undefined);
        if (proxy !== null) await proxy.close().catch(() => undefined);
        await stopServer(server);
        report.leftoverProcesses = await cleanupPorts([serverPort, uiPort].filter(Boolean));
        report.consoleErrors = consoleErrors.length;
        report.uncaughtExceptions = pageErrors.length;
        report.blockedRequests = [...new Set(report.blockedRequests)].slice(0, 20);
        await writeFile(OUT_JSON, JSON.stringify(report, null, 2) + '\n');
        await Promise.all([home, runtime].filter(Boolean).map((path) => rm(path, { recursive: true, force: true })));
    }

    const pass = report.error === null
        && report.symbolSelected === 'createUser'
        && report.commandEmpty && report.commandFocused && report.examplesVisible && report.unfoldVisible
        && report.unfoldHitTarget === 'atlas-explain-collapse' && report.unfoldClickSucceeded && report.explainOpened
        && report.examplesCount >= 3 && report.exampleHitTarget === 'atlas-command-example'
        && report.exampleClickSucceeded && report.commandAfterExample === report.exampleText
        && report.consoleErrors === 0 && report.uncaughtExceptions === 0
        && report.leftoverProcesses === 0 && report.blockedRequests.length === 0;
    if (!pass) {
        console.error('[smoke-w12a] W12a nicht gruen:', JSON.stringify(report));
        process.exitCode = 1;
    } else {
        log('W12a gruen');
    }
}

main().catch((error) => {
    console.error('[smoke-w12a] unerwarteter Fehler:', error);
    process.exitCode = 1;
});
