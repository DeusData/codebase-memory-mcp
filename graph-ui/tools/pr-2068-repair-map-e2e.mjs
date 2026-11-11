// Product repair verification: the actual repository, no seeded application state.
import { chromium } from 'playwright';
import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
const out = 'verification/pr-2068';
await mkdir(out, { recursive: true });
const report = { started: new Date().toISOString(), project: 'cbm-pr2068', origin: 'http://127.0.0.1:9749', stages: [], errors: [], externalRequests: [], failedRequests: [] };
const browser = await chromium.launch({ headless: true, args: ['--use-angle=swiftshader'] });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, reducedMotion: 'reduce' });
const page = await context.newPage();
page.on('pageerror', error => report.errors.push(error.message));
page.on('requestfailed', request => report.failedRequests.push({ url: request.url(), failure: request.failure() }));
page.on('request', request => { if (/^https?:/.test(request.url()) && new URL(request.url()).origin !== report.origin) report.externalRequests.push(request.url()); });
async function capture(name) {
    const filename = `repair-${name}.png`;
    await page.screenshot({ path: `${out}/${filename}` });
    report.stages.push({ name, screenshot: filename, text: await page.locator('body').innerText(), at: new Date().toISOString() });
}
try {
    await page.goto(`${report.origin}/?project=${report.project}`, { waitUntil: 'domcontentloaded' });
    await page.locator('[data-workspace-tab="architecture"]').waitFor();
    const welcome = page.getByRole('button', { name: 'Start exploring', exact: true });
    if (await welcome.isVisible()) await welcome.click();
    await page.locator('[data-workspace-tab="architecture"]').click();
    await page.getByRole('button', { name: 'Overview', exact: true }).click();
    await page.locator('.repo-map-purpose blockquote').waitFor({ timeout: 60000 });
    await page.locator('.repo-map-component').filter({ hasText: 'src/daemon' }).locator('blockquote').waitFor({ timeout: 30000 });
    await capture('architecture-overview');
    assert.match(await page.locator('.repo-map-purpose').innerText(), /code intelligence/i);
    const mainEntry = page.locator('.repo-map-entry-grid > button').filter({ hasText: 'src/main.c' });
    assert.equal(await mainEntry.count(), 1, 'The real application entry must be visible before tools and fixtures');
    await mainEntry.click();
    await page.locator('.repo-map-route').first().waitFor();
    await capture('static-call-paths');
    await page.locator('.repo-map-route').first().getByRole('button', { name: /Inspect .* call edges/ }).click();
    const evidence = page.locator('.repo-map-focused-evidence');
    assert.ok(await evidence.isVisible());
    const bounds = await evidence.boundingBox();
    assert.ok(bounds && bounds.y < 950 && bounds.y + bounds.height > 100, 'Call evidence is visible immediately');
    await capture('call-path-evidence');
    const site = evidence.getByRole('button', { name: /^Site :/ }).first();
    await site.click();
    const drawer = page.getByRole('dialog', { name: 'Source evidence', exact: true });
    await drawer.locator('.source-evidence-lines').waitFor();
    assert.equal(await page.locator('[data-workspace-tab="architecture"]').getAttribute('aria-selected'), 'true');
    assert.equal(await drawer.locator('[data-selected=true]').count(), 1, 'Requested source line is actually loaded and highlighted');
    await capture('source-beside-map');
    await drawer.getByRole('button', { name: 'Close source evidence', exact: true }).click();
    await page.locator('.repo-map-focused-evidence').getByRole('button', { name: 'Close evidence', exact: true }).click();
    await page.locator('.repo-map-component').filter({ has: page.locator('.repo-map-area > strong', { hasText: 'src/daemon' }) }).locator('.repo-map-area').click();
    await capture('daemon-responsibilities');
    await page.locator('.repo-map-files').getByRole('button', { name: 'src/daemon/application.c', exact: true }).click();
    const symbols = page.locator('.repo-map-nodes button').filter({ hasText: 'application_project_lock_release_fully' });
    for (let i = 0; i < 8 && await symbols.count() === 0; i++) {
        const more = page.getByRole('button', { name: /^More symbols/ });
        if (!(await more.count())) break;
        await more.click();
    }
    await symbols.first().click();
    await page.getByTestId('selection-context').getByRole('button', { name: 'Assess change impact', exact: true }).click();
    const impact = page.getByRole('dialog', { name: 'Change analysis', exact: true });
    await impact.getByTestId('selection-impact').waitFor();
    await capture('shared-analysis-entry');
    assert.deepEqual(report.errors, []);
    assert.deepEqual(report.externalRequests, [], 'The workflow makes no outbound diagnostic or model requests');
    report.completed = true;
} catch (error) {
    report.failure = String(error); await capture('map-flow-failure'); process.exitCode = 1;
} finally {
    report.finished = new Date().toISOString();
    await writeFile(`${out}/repair-map-e2e.json`, JSON.stringify(report, null, 2) + '\n');
    await browser.close();
}
console.log(JSON.stringify({ completed: report.completed, failure: report.failure, stages: report.stages.map(row => row.name), errors: report.errors }));
