// Fresh, real-data frontend comparison. Both ports share the current daemon/index.
import { chromium } from 'playwright';
import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
const out = 'verification/pr-2068';
const browser = await chromium.launch({ headless: true, args: ['--use-angle=swiftshader'] });
const report = { started: new Date().toISOString(), baseline: '80f4f41f9bcb9daf60afc5f607bcbcba24a4b196', comparison: 'Original frontend versus repaired frontend, same current daemon and repository index', views: [] };
try {
    for (const port of [9751, 9749]) {
        const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, reducedMotion: 'reduce' });
        const page = await context.newPage();
        const errors = [];
        page.on('pageerror', error => errors.push(error.message));
        await page.goto(`http://127.0.0.1:${port}/?project=cbm-pr2068&workspace=architecture`, { waitUntil: 'domcontentloaded' });
        await page.locator('[data-workspace-tab="architecture"]').waitFor();
        const welcome = page.getByRole('button', { name: 'Start exploring', exact: true });
        if (await welcome.isVisible()) await welcome.click();
        await page.locator('[data-workspace-tab="architecture"]').click();
        await page.getByRole('button', { name: 'Overview', exact: true }).click();
        if (port === 9749) {
            await page.locator('.repo-map-purpose blockquote').waitFor({ timeout: 60000 });
            await page.locator('.repo-map-component').filter({ hasText: 'src/daemon' }).locator('blockquote').waitFor();
        } else {
            await page.waitForFunction(() => document.body.innerText.includes('408') || /modules|communities|layers/i.test(document.body.innerText));
            // Allow the original overview's independent requests to settle.
            await page.waitForTimeout(2500);
        }
        const screenshot = `final-${port === 9751 ? 'before' : 'after'}-architecture.png`;
        await page.screenshot({ path: `${out}/${screenshot}` });
        report.views.push({ port, screenshot, text: await page.locator('body').innerText(), errors });
        if (port === 9749) {
            await page.setViewportSize({ width: 1024, height: 800 });
            const bounds = await page.evaluate(() => ({ viewport: innerWidth, width: document.documentElement.scrollWidth }));
            assert.ok(bounds.width <= bounds.viewport, 'Map does not overflow the smaller viewport');
            await page.locator('.repo-map-purpose').getByRole('button', { name: 'Read project README', exact: true }).click();
            const drawer = page.getByRole('dialog', { name: 'Source evidence', exact: true });
            await drawer.locator('.source-evidence-lines').waitFor();
            const rect = await drawer.boundingBox();
            assert.ok(rect && rect.x >= 0 && rect.x + rect.width <= 1024 && rect.y + rect.height <= 800);
            await page.screenshot({ path: `${out}/final-map-source-1024.png` });
            await drawer.getByRole('button', { name: 'Close source evidence', exact: true }).focus();
            await page.keyboard.press('Escape');
            assert.equal(await drawer.count(), 0);
            assert.equal(await page.locator('[data-workspace-tab="architecture"]').getAttribute('aria-selected'), 'true');
            // A direct link must override a previously stored Explore preference.
            await page.locator('[data-workspace-tab="explore"]').click();
            await page.goto('http://127.0.0.1:9749/?project=cbm-pr2068&workspace=architecture', { waitUntil: 'domcontentloaded' });
            await page.locator('[data-workspace-tab="architecture"][aria-selected="true"]').waitFor();
            report.responsive = { viewport: '1024x800', bounds, drawer: rect, escapeReturnsToMap: true, architectureDeepLink: true };
            assert.deepEqual(errors, []);
        }
        await context.close();
    }
    report.completed = true;
} catch (error) { report.failure = String(error); process.exitCode = 1; }
finally {
    report.finished = new Date().toISOString();
    await writeFile(`${out}/final-comparison.json`, JSON.stringify(report, null, 2) + '\n');
    await browser.close();
}
console.log(JSON.stringify({ completed: report.completed, failure: report.failure, views: report.views.map(({ port, screenshot, errors }) => ({ port, screenshot, errors })), responsive: report.responsive }));
