#!/usr/bin/env node
// Real, read-only repository/browser flow. No fixtures, storage seeding or mocked API.
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import { chromium } from 'playwright';

const origin = process.env.CBM_PREVIEW_ORIGIN ?? 'http://127.0.0.1:9749';
const project = 'cbm-pr2068', out = 'verification/pr-2068';
await mkdir(out, { recursive: true });
const report = { origin, project, startedAt: new Date().toISOString(), method: 'Fresh normal UI navigation; no localStorage seeding, graph mocks or repository writes.',
    actions: [], screenshots: [], rpc: [], pageErrors: [], externalRequests: [], checks: {} };
const browser = await chromium.launch({ headless: true, args: ['--use-angle=swiftshader'] });
const context = await browser.newContext({ viewport: { width: 1600, height: 1000 }, reducedMotion: 'reduce', permissions: ['clipboard-read', 'clipboard-write'] });
const page = await context.newPage();
page.on('pageerror', error => report.pageErrors.push(error.message));
page.on('request', request => {
    if (/^https?:/.test(request.url()) && !request.url().startsWith(`${origin}/`)) report.externalRequests.push(request.url());
    if (request.url() === `${origin}/rpc`) {
        try { const body = request.postDataJSON(); if (body.params?.name) report.rpc.push({ tool: body.params.name, arguments: body.params.arguments }); } catch {}
    }
});
const click = async (locator, description) => { console.log(description); report.actions.push({ description, before: await locator.boundingBox() }); await locator.click(); };
const capture = async name => { await page.screenshot({ path: `${out}/${name}`, fullPage: false }); report.screenshots.push({ name, at: new Date().toISOString() }); };
const workspace = page.getByTestId('change-analysis-workspace');
const evidence = workspace.getByTestId('selection-impact');
try {
    await page.goto(`${origin}/?project=${project}`, { waitUntil: 'domcontentloaded' });
    await page.locator('[data-workspace-tab="explore"]').waitFor();
    const welcome = page.getByRole('button', { name: 'Start exploring', exact: true });
    if (await welcome.isVisible()) await click(welcome, 'Standard welcome: Start exploring');
    await click(page.locator('[data-workspace-tab="explore"]'), 'Explore workspace');
    // The longstanding visible entry must lead to the same new workspace.
    await click(page.getByRole('tab', { name: /^change (scope|impact)$/i }), 'Change scope entry');
    await workspace.waitFor();
    await workspace.getByText(/of \d+ changed paths loaded/).waitFor({ timeout: 90000 });
    report.checks.changeSet = await workspace.locator('.change-analysis-files').innerText();
    assert.ok(!report.checks.changeSet.includes('Change set unavailable'));
    assert.ok(report.rpc.some(call => call.tool === 'detect_changes' && call.arguments.since === 'HEAD' && call.arguments.scope === 'files'));
    assert.equal(await page.locator('.atlas-impact').filter({ visible: true }).count(), 0);
    await workspace.getByRole('searchbox', { name: 'Filter changed paths' }).fill('src/daemon/application.c');
    await click(workspace.getByRole('button', { name: 'src/daemon/application.c', exact: true }), 'Actual modified daemon/application.c');
    await evidence.locator('.selection-impact-risk').waitFor({ timeout: 90000 });
    const symbol = workspace.getByRole('combobox', { name: 'Analysis scope' });
    await page.waitForFunction(() => document.querySelector('select[aria-label="Analysis scope"]')?.options.length > 2, null, { timeout: 60000 });
    const options = await symbol.locator('option').evaluateAll(options => options.map(option => ({ value: option.value, label: option.textContent })));
    const selected = options.find(option => option.label.includes('application_project_lock_release_fully'));
    assert.ok(selected, 'Indexed function choices include the known production declaration');
    await symbol.selectOption(selected.value);
    await evidence.locator('.selection-impact-risk').waitFor({ timeout: 90000 });
    report.checks.selectedSymbol = selected;
    report.checks.scope = await workspace.locator('.change-analysis-selection').innerText();
    report.checks.risk = await evidence.locator('.selection-impact-risk').innerText();
    report.checks.dependencies = await evidence.getByRole('tabpanel').filter({ visible: true }).innerText();
    assert.ok(report.checks.risk.includes('data limitations'));
    assert.ok(report.checks.dependencies.includes('Structural impact'));
    await capture('change-analysis-after-dependencies.png');
    await click(evidence.getByRole('tab', { name: /^Tests to review/ }), 'Tests to review: inspect actual indexed test paths');
    const testPanel = evidence.getByRole('tabpanel').filter({ visible: true });
    report.checks.tests = await testPanel.innerText();
    const testSource = testPanel.getByRole('button', { name: /^Open test source:/ }).first();
    assert.ok(await testSource.count(), 'Real selection has at least one recorded test candidate');
    await click(testSource, 'Open test source evidence while preserving analysis');
    const drawer = page.getByRole('dialog', { name: 'Source evidence', exact: true });
    await drawer.locator('.source-evidence-lines, [role=alert]').first().waitFor({ timeout: 60000 });
    report.checks.testSourceReadable = await drawer.locator('.source-evidence-lines').isVisible();
    report.checks.testSource = await drawer.innerText();
    assert.ok(await workspace.isVisible());
    assert.equal(await symbol.inputValue(), selected.value);
    await capture('change-analysis-after-test-source.png');
    await click(drawer.getByRole('button', { name: 'Close source evidence' }), 'Close source evidence, keep test selection');
    assert.ok(await workspace.isVisible());
    assert.ok(await testSource.evaluate(button => document.activeElement === button), 'Focus returns to referring test source action');
    await click(evidence.getByRole('tab', { name: /^Git history/ }), 'Git history tab');
    const history = evidence.getByRole('tabpanel').filter({ visible: true });
    await click(history.locator('summary').filter({ hasText: 'Inspect local Git evidence' }).first(), 'Expand a recorded local commit');
    report.checks.history = await history.innerText();
    await click(history.getByRole('button', { name: 'Copy local Git evidence command' }).first(), 'Copy exact local Git evidence command');
    report.checks.copiedCommand = await page.evaluate(() => navigator.clipboard.readText());
    assert.match(report.checks.copiedCommand, /^git show [0-9a-f]{40,64} -- /);
    assert.ok(report.checks.copiedCommand.includes('src/daemon/application.c'));
    assert.ok((await history.innerText()).includes('Nothing was executed or sent'));
    await capture('change-analysis-after-git-evidence.png');
    await click(evidence.getByRole('tab', { name: /^Data quality/ }), 'Review snapshot, dirty status and uncertainty');
    report.checks.quality = await evidence.getByRole('tabpanel').filter({ visible: true }).innerText();
    report.checks.graphGeneration = await evidence.getByRole('tabpanel').filter({ visible: true }).locator('code').first().innerText();
    assert.ok(report.checks.quality.includes('Git HEAD:'));
    assert.ok(report.checks.quality.includes('uncommitted changes'));
    await page.setViewportSize({ width: 1024, height: 800 });
    await click(evidence.getByRole('tab', { name: /^Dependencies/ }), 'Dependencies at 1024 × 800');
    report.checks.narrowLayout = await page.evaluate(() => {
        const panel = document.querySelector('.selection-impact-panels');
        const root = document.querySelector('[data-testid="change-analysis-workspace"]');
        const bounds = element => { const rect = element.getBoundingClientRect(); return { x: rect.x, y: rect.y, width: rect.width, height: rect.height }; };
        return { width: innerWidth, documentWidth: document.documentElement.scrollWidth, panel: bounds(panel), root: bounds(root),
            toolbarButtons: [...root.querySelectorAll('.change-analysis-header button, .change-analysis-toolbar button, .selection-impact-tabs button')].map(button => ({ text: button.textContent, ...bounds(button) })) };
    });
    assert.ok(report.checks.narrowLayout.documentWidth <= 1024, 'No document horizontal overflow');
    assert.ok(report.checks.narrowLayout.panel.height >= 140, 'Evidence retains a usable independent scroll area');
    for (const button of report.checks.narrowLayout.toolbarButtons) assert.ok(button.x >= 0 && button.x + button.width <= 1024 && button.y + button.height <= 800, `Visible unclipped control: ${button.text}`);
    await capture('change-analysis-after-1024.png');
    await click(workspace.getByRole('tab', { name: 'Since ref', exact: true }), 'Since ref uses the same change workspace');
    await workspace.getByRole('textbox', { name: 'Baseline revision' }).fill('HEAD~1');
    await click(workspace.getByRole('button', { name: 'Read changes', exact: true }), 'Read actual changes since HEAD~1');
    await workspace.getByText(/of \d+ changed paths loaded/).waitFor({ timeout: 90000 });
    report.checks.sinceRef = await workspace.locator('.change-analysis-files').innerText();
    assert.ok(report.rpc.some(call => call.tool === 'detect_changes' && call.arguments.since === 'HEAD~1'));
    assert.ok(!report.checks.sinceRef.includes('Change set unavailable'));
    await capture('change-analysis-after-since-ref.png');
    await click(workspace.getByRole('button', { name: 'Close change impact', exact: true }), 'Close shared analysis');
    assert.equal(await workspace.count(), 0);
    assert.ok(report.checks.testSourceReadable, 'Indexed test source must resolve from a fresh Explore workflow');
    assert.deepEqual(report.pageErrors, []);
    assert.deepEqual(report.externalRequests, []);
    report.success = true;
} catch (error) {
    report.success = false; report.error = String(error);
    await capture('change-analysis-e2e-failure.png');
    await writeFile(`${out}/change-analysis-e2e-failure-dom.txt`, await page.locator('body').innerText());
    process.exitCode = 1;
} finally {
    report.finishedAt = new Date().toISOString();
    await writeFile(`${out}/change-analysis-e2e.json`, `${JSON.stringify(report, null, 2)}\n`);
    await browser.close();
}
console.log(JSON.stringify({ success: report.success, error: report.error, checks: Object.keys(report.checks), screenshots: report.screenshots }, null, 2));
