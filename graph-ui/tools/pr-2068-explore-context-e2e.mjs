#!/usr/bin/env node
// Fresh normal navigation through global path search and the real Selected code inspector.
import assert from 'node:assert/strict';
import { writeFile, mkdir } from 'node:fs/promises';
import { chromium } from 'playwright';
const origin = 'http://127.0.0.1:9749', project = 'cbm-pr2068', file = 'src/daemon/application.c';
const out = 'verification/pr-2068'; await mkdir(out, { recursive: true });
const report = { startedAt: new Date().toISOString(), origin, project, file, method: 'Fresh browser, global search, ordinary clicks; no storage seeding, API mocks or repository changes.', actions: [], screenshots: [], pageErrors: [], externalRequests: [], rpc: [], checks: {} };
const browser = await chromium.launch({ headless: true, args: ['--use-angle=swiftshader'] });
const context = await browser.newContext({ viewport: { width: 1600, height: 1000 }, reducedMotion: 'reduce' });
const page = await context.newPage();
page.on('pageerror', error => report.pageErrors.push(error.message));
page.on('request', request => {
    if (/^https?:/.test(request.url()) && !request.url().startsWith(`${origin}/`)) report.externalRequests.push(request.url());
    if (request.url() === `${origin}/rpc`) { try { const body = request.postDataJSON(); if (body.params?.name) report.rpc.push({ tool: body.params.name, arguments: body.params.arguments }); } catch {} }
});
const click = async (locator, description) => { console.log(description); report.actions.push({ description, before: await locator.boundingBox() }); await locator.click(); };
const capture = async name => { await page.screenshot({ path: `${out}/${name}`, fullPage: false }); report.screenshots.push({ name, at: new Date().toISOString() }); };
try {
    await page.goto(`${origin}/?project=${project}`, { waitUntil: 'domcontentloaded' });
    await page.locator('[data-workspace-tab="explore"]').waitFor();
    const welcome = page.getByRole('button', { name: 'Start exploring', exact: true });
    if (await welcome.isVisible()) await click(welcome, 'Start exploring');
    await click(page.locator('[data-workspace-tab="explore"]'), 'Explore');
    await click(page.getByRole('button', { name: 'Open search and commands', exact: true }), 'Global Search');
    const searchInput = page.locator('.atlas-command-palette[open] input');
    await searchInput.fill(file);
    const results = page.getByTestId('atlas-search-row');
    // Do not wait for the server before checking the keyboard action offered now.
    report.checks.immediateSearch = await page.evaluate(() => ({
        status: document.querySelector('[data-testid="atlas-search-results"]')?.getAttribute('data-status'),
        rows: [...document.querySelectorAll('[data-testid="atlas-search-row"]')].map(row => ({ text: row.textContent,
            path: row.querySelector('.atlas-search-path')?.textContent })),
    }));
    assert.ok(report.checks.immediateSearch.rows.every(row => row.path === file), 'A full path must not offer a differently named provisional file');
    report.actions.push({ description: 'Immediate Enter after typing the complete path, before waiting for the index reply' });
    await searchInput.press('Enter');
    const inspector = page.getByTestId('selected-code-panel');
    report.checks.fileAfterImmediateEnter = await inspector.locator('.selected-code-path').allTextContents();
    assert.ok(report.checks.fileAfterImmediateEnter.every(path => path === file || path.startsWith(`${file}:`)), 'Enter must not open a substituted header or unrelated file');
    if (await page.locator('.atlas-command-palette[open]').isVisible()) {
        await page.locator('[data-testid="atlas-search-results"][data-status="ready"]').waitFor({ timeout: 60000 });
        await results.first().waitFor({ timeout: 60000 });
        report.checks.searchResults = await results.allTextContents();
        report.checks.firstSearchPath = await results.first().locator('.atlas-search-path').innerText();
        assert.equal(report.checks.firstSearchPath, file, 'A complete path should select that exact file before similar symbol names');
        await capture('explore-after-exact-path-search.png');
        await click(results.first(), 'First exact file search result');
    } else {
        report.checks.exactFileOpenedImmediately = true;
        await capture('explore-after-exact-path-search.png');
    }
    await inspector.locator('.selected-code-path').filter({ hasText: file }).waitFor({ timeout: 60000 });
    const symbols = inspector.getByTestId('selected-code-file');
    await symbols.getByRole('button').first().waitFor({ timeout: 60000 });
    report.checks.symbols = await symbols.innerText();
    assert.ok(!/queries? failed|could not load|No symbols found/i.test(report.checks.symbols));
    report.checks.symbolKinds = await symbols.getByRole('button').locator('span:last-child').allTextContents();
    assert.ok(report.checks.symbolKinds.some(value => value.startsWith('function')));
    assert.ok(report.checks.symbolKinds.some(value => value.startsWith('class')));
    report.checks.fileIdentity = await inspector.locator('.selected-code-path').innerText();
    await page.getByTestId('atlas-reader').locator('.view-lines').waitFor({ timeout: 60000 });
    report.checks.reader = (await page.getByTestId('atlas-reader').innerText()).slice(0, 1800);
    await symbols.getByRole('button').first().scrollIntoViewIfNeeded();
    await capture('explore-after-symbol-list.png');
    const symbol = symbols.getByRole('button').filter({ hasText: 'application_project_lock_release_fully' });
    await click(symbol, 'Select a real function from the file inspector');
    await inspector.locator('.selected-code-symbol strong').filter({ hasText: 'application_project_lock_release_fully' }).waitFor({ timeout: 60000 });
    const selection = inspector.getByTestId('selection-context');
    await selection.waitFor();
    report.checks.selectionContext = await selection.innerText();
    report.checks.selectedIdentity = await inspector.locator('.selected-code-identity').innerText();
    assert.ok(report.checks.selectionContext.includes('application_project_lock_release_fully'));
    assert.ok(report.checks.selectionContext.toLowerCase().includes('graph evidence'));
    assert.ok(report.checks.selectionContext.includes('Observed agent activity'));
    await selection.scrollIntoViewIfNeeded();
    await capture('explore-after-selection-context.png');
    await click(inspector.getByRole('button', { name: /^Assess change impact/ }), 'Selection context opens the shared Change impact workspace');
    const analysis = page.getByTestId('change-analysis-workspace');
    await analysis.waitFor();
    await analysis.getByTestId('selection-impact').locator('.selection-impact-risk').waitFor({ timeout: 90000 });
    report.checks.analysisScope = await analysis.locator('.change-analysis-selection').innerText();
    report.checks.analysisSymbol = await analysis.getByRole('combobox', { name: 'Analysis scope' }).inputValue();
    assert.ok(report.checks.analysisSymbol.endsWith('.application_project_lock_release_fully'));
    assert.equal(await analysis.getByRole('tab', { name: 'Current selection', exact: true }).getAttribute('aria-selected'), 'true');
    assert.equal(await page.locator('.atlas-impact').filter({ visible: true }).count(), 0);
    await capture('explore-after-shared-impact.png');
    await click(analysis.getByRole('button', { name: 'Close change impact', exact: true }), 'Close analysis before search race verification');
    await click(page.getByRole('button', { name: 'Open search and commands', exact: true }), 'Global Search: delayed genuine response test');
    // Hold a genuine daemon response, never synthesize graph rows. This makes
    // the otherwise timing-dependent old-response/new-query race repeatable.
    let held, resolveHeld;
    const captured = new Promise(resolve => { resolveHeld = resolve; });
    const delayed = async route => {
        let body;
        try { body = route.request().postDataJSON(); } catch {}
        if (!held && body?.params?.name === 'query_graph' && body.params.arguments?.query?.includes('n.file_path = "src/main.c"')) {
            const response = await route.fetch();
            held = { route, response, query: body.params.arguments.query, status: response.status(), body: await response.text() };
            resolveHeld();
        } else await route.continue();
    };
    await page.route(`${origin}/rpc`, delayed);
    report.checks.searchRace = { method: 'Delayed delivery of an unchanged actual daemon response; no synthetic response data.', canceledRequests: [] };
    page.on('requestfailed', request => {
        if (request.url() === `${origin}/rpc` && request.postData()?.includes('src/main.c'))
            report.checks.searchRace.canceledRequests.push(request.failure());
    });
    const raceInput = page.locator('.atlas-command-palette[open] input');
    await raceInput.fill('src/main.c');
    let deadline;
    try { await Promise.race([captured, new Promise((_, reject) => { deadline = setTimeout(() => reject(new Error('Expected genuine src/main.c request was not captured')), 15000); })]); }
    finally { clearTimeout(deadline); }
    report.checks.searchRace.oldResponse = { query: held.query, status: held.status, body: held.body };
    assert.equal(held.status, 200);
    await raceInput.evaluate(input => input.addEventListener('input', () => {
        input.dataset.proofQueryChangedAt = String(Date.now());
    }, { once: true }));
    await raceInput.fill(file);
    const changedAt = Number(await raceInput.getAttribute('data-proof-query-changed-at'));
    try { await held.route.fulfill({ response: held.response }); }
    catch (error) { report.checks.searchRace.releaseError = String(error); }
    report.checks.searchRace.releaseDelayMs = Date.now() - changedAt;
    assert.ok(report.checks.searchRace.releaseDelayMs < 90, 'The old response was released during the actual 90ms debounce window');
    await page.waitForTimeout(30);
    report.checks.searchRace.afterRelease = await page.evaluate(() => ({ query: document.querySelector('.atlas-command-palette[open] input')?.value,
        paths: [...document.querySelectorAll('[data-testid="atlas-search-row"] .atlas-search-path')].map(node => node.textContent),
        status: document.querySelector('[data-testid="atlas-search-results"]')?.getAttribute('data-status') }));
    assert.equal(report.checks.searchRace.afterRelease.query, file);
    assert.ok(report.checks.searchRace.afterRelease.paths.every(path => path === file), 'A late previous-query reply must not replace current file candidates');
    await page.locator('[data-testid="atlas-search-results"][data-status="ready"]').waitFor({ timeout: 60000 });
    assert.equal(await page.getByTestId('atlas-search-row').first().locator('.atlas-search-path').innerText(), file);
    await capture('explore-after-delayed-search-response.png');
    await raceInput.press('Enter');
    assert.ok((await inspector.locator('.selected-code-path').innerText()).startsWith(file));
    await page.unroute(`${origin}/rpc`, delayed);
    assert.deepEqual(report.pageErrors, []); assert.deepEqual(report.externalRequests, []);
    report.success = true;
} catch (error) {
    report.success = false; report.error = String(error); process.exitCode = 1;
    await capture('explore-context-e2e-failure.png');
    await writeFile(`${out}/explore-context-e2e-failure-dom.txt`, await page.locator('body').innerText());
} finally {
    report.finishedAt = new Date().toISOString();
    await writeFile(`${out}/explore-context-e2e.json`, `${JSON.stringify(report, null, 2)}\n`);
    await browser.close();
}
console.log(JSON.stringify({ success: report.success, error: report.error, checks: Object.keys(report.checks), screenshots: report.screenshots }, null, 2));
