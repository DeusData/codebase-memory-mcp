#!/usr/bin/env node
// Explicit local regression fixture through the real daemon and embedded UI.
// Does not start/reindex a server, modify source fixtures, clear history or mock APIs.
// Writes two clearly labelled test events; run only against the PR verification daemon.
import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const ORIGIN = 'http://127.0.0.1:9749';
const PROJECT = 'cbm-pr2068';
const FIXTURE = 'pr2068-error-fixture';
const OUT = resolve(dirname(fileURLToPath(import.meta.url)), '../verification/pr-2068');
const session = `feedback-${Date.now().toString(36)}`;
const marker = `EXPLICIT REGRESSION FIXTURE ${session}: project-attributed test error; no production failure asserted`;
const globalMarker = `EXPLICIT REGRESSION FIXTURE ${session}: unattributed test warning; no production failure asserted`;
const report = { startedAt: new Date().toISOString(), origin: ORIGIN, project: PROJECT, fixtureProject: FIXTURE,
    syntheticEvents: true, session, actions: [], directRequests: [], browserRequests: [], pageErrors: [],
    consoleErrors: [], failedRequests: [], externalRequests: [], screenshots: [], checks: {} };
let browser;
const pages = [];
const delay = (ms) => new Promise(done => setTimeout(done, ms));

async function json(path, body) {
    const response = await fetch(ORIGIN + path, {
        method: body === undefined ? 'GET' : 'POST',
        ...(body === undefined ? {} : { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
        signal: AbortSignal.timeout(30000),
    });
    const text = await response.text();
    report.directRequests.push({ at: new Date().toISOString(), method: body === undefined ? 'GET' : 'POST', path, status: response.status });
    assert.ok(response.ok, `${path}: HTTP ${response.status}: ${text.slice(0, 300)}`);
    return JSON.parse(text);
}

const logs = (project, extra = '') => json(`/api/logs?lines=500&min_level=warn${project === undefined ? '' : `&project=${encodeURIComponent(project)}`}${extra}`);

async function eventually(read, predicate, description, timeout = 20000) {
    const deadline = Date.now() + timeout;
    let last;
    while (Date.now() < deadline) {
        last = await read();
        if (predicate(last)) return last;
        await delay(200);
    }
    throw new Error(`${description}: ${JSON.stringify(last)?.slice(0, 1200)}`);
}

async function capture(page, file, locator) {
    await page.bringToFront();
    if (locator) await locator.scrollIntoViewIfNeeded();
    const path = join(OUT, file);
    await page.screenshot({ path, fullPage: false });
    report.screenshots.push({ file, url: page.url(), at: new Date().toISOString(),
        sha256: createHash('sha256').update(await readFile(path)).digest('hex') });
}

async function loadedAlerts(page, project, expanded = false) {
    const alerts = page.getByTestId('daemon-alerts');
    await alerts.waitFor({ timeout: 60000 });
    await eventually(() => alerts.innerText(), text => text.includes('Last checked') && text.includes(project), 'Expected project-owned event reading');
    await eventually(() => alerts.getAttribute('class'), value => value.includes(expanded ? 'daemon-alerts-expanded' : 'daemon-alerts-collapsed'), 'History expansion state');
    return alerts;
}

async function openPage(context, project) {
    const page = await context.newPage();
    pages.push(page);
    page.on('pageerror', error => report.pageErrors.push({ project, message: error.message }));
    page.on('console', event => { if (event.type() === 'error') report.consoleErrors.push({ project, text: event.text() }); });
    page.on('requestfailed', request => report.failedRequests.push({ project, url: request.url(), failure: request.failure()?.errorText }));
    await page.goto(`${ORIGIN}/?project=${encodeURIComponent(project)}&audit=${session}`, { waitUntil: 'domcontentloaded' });
    const alerts = await loadedAlerts(page, project);
    assert.equal(await alerts.locator('.daemon-alert-error').count(), 0, 'Initial historical errors must not become fresh alarms');
    assert.ok((await alerts.innerText()).includes('Earlier events are in history'));
    return { page, alerts };
}

async function refreshAlerts(page, alerts, project) {
    await page.bringToFront();
    await eventually(() => alerts.getByRole('button', { name: 'Refresh events', exact: true }).isEnabled(), Boolean, 'Refresh is ready');
    const reply = page.waitForResponse(response => {
        const url = new URL(response.url());
        return url.pathname === '/api/logs' && url.searchParams.get('project') === project && response.ok();
    });
    await alerts.getByRole('button', { name: 'Refresh events', exact: true }).click();
    await reply;
    await eventually(() => alerts.getByRole('button', { name: 'Refresh events', exact: true }).isEnabled(), Boolean, 'Refresh completed');
}

async function main() {
    await mkdir(OUT, { recursive: true });
    // Preserve proof that older test records survived the scope migration.
    const before = await logs(undefined, '&scope=unattributed');
    const oldFixture = before.records.find(row => row.level === 'error' && row.message.includes('cbm-2068-error-fixture'));
    assert.ok(oldFixture, 'Expected retained pre-migration/unattributed indexing-fixture error');
    assert.equal(oldFixture.project, null);
    report.checks.oldUnattributedFixtureBefore = oldFixture;

    browser = await chromium.launch({ headless: true, args: ['--disable-background-networking', '--disable-component-update',
        '--disable-background-timer-throttling', '--disable-renderer-backgrounding', '--disable-sync', '--no-first-run'] });
    const context = await browser.newContext({ viewport: { width: 1600, height: 1100 }, reducedMotion: 'reduce' });
    await context.addInitScript(() => {
        localStorage.setItem('cbm.workspace.setup', 'done');
        localStorage.setItem('cbm.workspace', 'architecture');
    });
    context.on('request', request => {
        const url = request.url();
        report.browserRequests.push({ at: new Date().toISOString(), method: request.method(), url });
        if (/^https?:/.test(url) && new URL(url).origin !== ORIGIN) report.externalRequests.push(url);
    });
    const actual = await openPage(context, PROJECT);
    const fixture = await openPage(context, FIXTURE);
    report.checks.initialProjectPages = { actual: await actual.alerts.innerText(), fixture: await fixture.alerts.innerText() };
    await capture(actual.page, 'feedback-after-project-events.png', actual.alerts);

    // User-reported counts must be complete path listings, without claiming full repo coverage.
    await eventually(() => actual.alerts.locator('.daemon-coverage-summary').innerText(),
        text => text.includes('85 partially parsed paths') && text.includes('196 intentionally excluded'), 'Real repository coverage counts');
    const summary = await actual.alerts.locator('.daemon-coverage-summary').innerText();
    assert.ok(!summary.includes('detail listing incomplete'), 'Fully loaded scope must replace truncated status samples');
    await actual.alerts.getByRole('button', { name: 'Inspect index coverage', exact: true }).click();
    const diagnosis = actual.page.getByRole('dialog', { name: 'Index coverage diagnosis', exact: true });
    await diagnosis.waitFor();
    assert.equal(await diagnosis.locator('textarea').count(), 0, 'Opening diagnosis does not generate a report');
    await diagnosis.getByRole('button', { name: 'Run local diagnosis', exact: true }).click();
    await diagnosis.getByRole('textbox', { name: 'Editable local diagnostic report' }).waitFor({ timeout: 60000 });
    const diagnosisText = await diagnosis.innerText();
    assert.ok(diagnosisText.includes('All recorded coverage entries have been loaded.'));
    assert.ok(!diagnosisText.includes('Incomplete list:'), 'No false incomplete-list warning after full coverage pagination');
    assert.match(await diagnosis.locator('.diagnostics-counts').innerText(), /Partial parse:\s*85/);
    assert.match(await diagnosis.locator('.diagnostics-counts').innerText(), /Excluded by index rules:\s*196/);
    assert.ok(diagnosisText.includes('a partial parse alone does not establish a build or runtime failure'));
    report.checks.realCoverage = { summary, text: diagnosisText,
        localReport: await diagnosis.getByRole('textbox', { name: 'Editable local diagnostic report' }).inputValue() };
    await capture(actual.page, 'feedback-after-coverage.png', diagnosis);
    await diagnosis.getByRole('button', { name: 'Close diagnosis', exact: true }).click();

    await actual.alerts.getByRole('button', { name: /^History \(/ }).click();
    await loadedAlerts(actual.page, PROJECT, true);
    const ownBefore = await logs(PROJECT);
    assert.equal(ownBefore.scope, 'project');
    assert.equal(ownBefore.project, PROJECT);
    assert.ok(ownBefore.records.every(row => row.project === PROJECT));
    assert.ok(!(await actual.alerts.innerText()).includes('cbm-2068-error-fixture'));
    report.checks.ownHistoryScope = { generation: ownBefore.generation, total: ownBefore.total, text: await actual.alerts.innerText() };
    await actual.alerts.getByRole('button', { name: 'All daemon history', exact: true }).click();
    await eventually(() => actual.alerts.innerText(), text => text.includes('Daemon history · all projects') && text.includes('Last checked'), 'All-project daemon history loaded');
    assert.ok((await actual.alerts.getAttribute('class')).includes('daemon-alerts-expanded'));
    const allReading = await json('/api/logs?lines=200&min_level=warn');
    const visibleOld = allReading.records.find(row => row.project === null && row.level === 'error' && row.message.includes('cbm-2068-error-fixture'));
    assert.ok(visibleOld, 'An older unattributed fixture error must remain accessible in the actual UI history page');
    await actual.alerts.getByText(`Event #${visibleOld.id}`, { exact: true }).waitFor();
    report.checks.allDaemonHistory = { oldFixture: visibleOld, text: await actual.alerts.innerText() };
    await capture(actual.page, 'feedback-after-daemon-history.png', actual.alerts);
    await actual.alerts.getByRole('button', { name: 'This project only', exact: true }).click();
    await loadedAlerts(actual.page, PROJECT, false);
    assert.equal(await actual.alerts.locator('.daemon-alert-error').count(), 0);

    // Contradicting batch project intentionally proves entry ownership wins.
    const syntheticBatch = { page: '/explicit-feedback-regression-fixture', project: PROJECT, session, entries: [
        { ts: new Date().toISOString(), seq: 1, project: FIXTURE, level: 'error', source: 'ui', message: marker },
        { ts: new Date().toISOString(), seq: 2, project: '', level: 'warn', source: 'ui', message: globalMarker },
    ] };
    report.actions.push({ at: new Date().toISOString(), action: 'Send two explicitly labelled local regression events', batch: syntheticBatch });
    report.checks.syntheticIngest = await json('/api/ui-log', syntheticBatch);
    const fixtureLogs = await eventually(() => logs(FIXTURE), data => data.records.some(row => row.message.includes(marker)), 'Attributed fixture error persisted');
    const event = fixtureLogs.records.find(row => row.message.includes(marker));
    assert.equal(event.project, FIXTURE);
    const exact = fixture.alerts.locator('.daemon-alert-error').filter({ hasText: `Event #${event.id}` });
    await fixture.page.bringToFront();
    await exact.waitFor({ state: 'visible', timeout: 20000 });
    assert.ok((await exact.innerText()).includes(marker));
    assert.ok((await exact.innerText()).includes('! ERROR'));
    assert.ok((await fixture.alerts.innerText()).includes('1 new recorded errors'));
    report.checks.fixtureLiveError = { event, text: await fixture.alerts.innerText(), observedWithoutReload: true,
        appearance: await exact.evaluate(node => ({ color: getComputedStyle(node).color, border: getComputedStyle(node).borderColor })) };
    await capture(fixture.page, 'feedback-after-fixture-live-error.png', fixture.alerts);
    await refreshAlerts(actual.page, actual.alerts, PROJECT);
    assert.equal(await actual.alerts.locator('.daemon-alert-error').count(), 0, 'Other project must not announce the fixture error');
    assert.ok(!(await actual.alerts.innerText()).includes(marker));
    const actualAfter = await logs(PROJECT);
    assert.ok(!actualAfter.records.some(row => row.message.includes(marker) || row.message.includes(globalMarker)), 'Entry project and empty global project must both override batch fallback');
    report.checks.foreignErrorAbsent = { text: await actual.alerts.innerText(), ownedProject: actualAfter.project, cursor: actualAfter.cursor };
    await capture(actual.page, 'feedback-after-other-project-unaffected.png', actual.alerts);

    await fixture.page.bringToFront();
    await fixture.alerts.getByRole('button', { name: 'Acknowledge new events', exact: true }).click();
    assert.equal(await fixture.alerts.locator('.daemon-alert-error').count(), 0);
    assert.ok((await fixture.alerts.getAttribute('class')).includes('daemon-alerts-collapsed'));
    await fixture.alerts.getByRole('button', { name: /^History \(/ }).click();
    await fixture.alerts.getByText(`Event #${event.id}`, { exact: true }).waitFor();
    const retainedAfterAck = await logs(FIXTURE);
    assert.ok(retainedAfterAck.records.some(row => row.id === event.id && row.message.includes(marker)));
    report.checks.acknowledgementPreservesHistory = { id: event.id, text: await fixture.alerts.innerText() };
    await capture(fixture.page, 'feedback-after-acknowledged-history.png', fixture.alerts);
    await fixture.page.reload({ waitUntil: 'domcontentloaded' });
    await loadedAlerts(fixture.page, FIXTURE, false);
    assert.equal(await fixture.alerts.locator('.daemon-alert-error').count(), 0, 'Reopening must not turn retained history back into a new alarm');
    report.checks.reloadDoesNotReannounceHistory = true;

    const unattributedAfter = await logs(undefined, '&scope=unattributed');
    assert.ok(unattributedAfter.records.some(row => row.id === oldFixture.id));
    assert.ok(unattributedAfter.records.some(row => row.project === null && row.message.includes(globalMarker)));
    report.checks.oldAndExplicitGlobalEventsRetained = true;
    assert.deepEqual(report.pageErrors, []);
    assert.deepEqual(report.consoleErrors, []);
    assert.deepEqual(report.externalRequests, []);
    // Browser teardown/reload can cancel transport reads; retain any such observations.
    assert.ok(report.failedRequests.every(entry => entry.failure === 'net::ERR_ABORTED'), 'Unexpected failed browser requests');
    report.completedAt = new Date().toISOString();
    report.success = true;
}

try { await main(); }
catch (error) {
    report.success = false; report.failure = error.stack ?? String(error); process.exitCode = 1;
    for (let index = 0; index < pages.length; index += 1) {
        if (!pages[index].isClosed()) {
            await capture(pages[index], `feedback-after-failure-${index}.png`).catch(() => {});
            await writeFile(join(OUT, `feedback-failure-${index}.txt`), await pages[index].locator('body').innerText()).catch(() => {});
        }
    }
} finally {
    // No log deletion: regression events remain attributable, inspectable history.
    if (browser) await browser.close();
    await mkdir(OUT, { recursive: true });
    await writeFile(join(OUT, 'feedback-e2e.json'), JSON.stringify(report, null, 2) + '\n');
    console.log(JSON.stringify({ report: join(OUT, 'feedback-e2e.json'), success: report.success,
        failure: report.failure, screenshots: report.screenshots }, null, 2));
}
