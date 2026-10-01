#!/usr/bin/env node
// Reproducible, explicitly synthetic repository fixture; real daemon and indexer.
// Requires the existing isolated daemon at 127.0.0.1:9749. No server is started.
import assert from 'node:assert/strict';
import { mkdir, writeFile, readFile, readdir, chmod } from 'node:fs/promises';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { dirname, resolve, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const UI_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const OUT = join(UI_ROOT, 'verification/pr-2068');
const ORIGIN = 'http://127.0.0.1:9749';
const CACHE = '/tmp/cbm-2068-cache';
const FIXTURE = '/tmp/cbm-2068-error-fixture';
const PROJECT = 'pr2068-error-fixture';
const proofPrefix = process.env.CBM_VERIFICATION_PREFIX ?? '';
if (!/^[a-z0-9-]*$/.test(proofPrefix)) throw new Error('Invalid local proof prefix');
const marker = 'PR 2068 EXPLICIT TEST FIXTURE - synthetic files for local error verification\n';
const report = { startedAt: new Date().toISOString(), origin: ORIGIN, fixture: FIXTURE, project: PROJECT, syntheticFixture: true,
    actions: [], directRequests: [], browserRequests: [], blockedExternalRequests: [], screenshots: [], checks: {}, cleanup: [] };
let browser;
let ownsFixture = false;
let rpcId = 0;
const delay = (ms) => new Promise((done) => setTimeout(done, ms));
const saveReport = () => writeFile(join(OUT, `${proofPrefix}errors-e2e.json`), JSON.stringify(report, null, 2) + '\n');

async function json(path, body) {
    const response = await fetch(ORIGIN + path, { method: body ? 'POST' : 'GET', ...(body ? { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) } : {}) });
    const text = await response.text();
    let payload;
    try { payload = JSON.parse(text); } catch { payload = { raw: text }; }
    report.directRequests.push({ at: new Date().toISOString(), method: body ? 'POST' : 'GET', path, status: response.status });
    if (!response.ok) throw new Error(`${path}: HTTP ${response.status}: ${text.slice(0, 300)}`);
    return payload;
}

async function rpc(name, args) {
    const response = await json('/rpc', { jsonrpc: '2.0', id: ++rpcId, method: 'tools/call', params: { name, arguments: { ...args, format: 'json' } } });
    if (response.error || response.result?.isError) throw new Error(JSON.stringify(response));
    return JSON.parse(response.result.content[0].text);
}

async function index(project) {
    const begin = await json('/api/index', { root_path: FIXTURE, project_name: project });
    report.actions.push({ action: 'POST /api/index', project, root: FIXTURE, acknowledgement: begin });
    const deadline = Date.now() + 180000;
    while (Date.now() < deadline) {
        const jobs = await json('/api/index-status');
        const job = jobs.find((item) => item.slot === begin.slot);
        if (job && job.status !== 'indexing') {
            report.actions.push({ action: 'Index terminal state', project, job });
            return job;
        }
        await delay(500);
    }
    throw new Error(`Index job timed out: ${project}`);
}

async function capture(page, name) {
    name = proofPrefix + name;
    await page.screenshot({ path: join(OUT, name), fullPage: false });
    report.screenshots.push({ file: name, url: page.url(), at: new Date().toISOString(), sha256: createHash('sha256').update(await readFile(join(OUT, name))).digest('hex') });
}

async function fixturePrepare() {
    await mkdir(FIXTURE, { recursive: true });
    const old = await readFile(join(FIXTURE, 'TEST-FIXTURE.txt'), 'utf8').catch((error) => { if (error.code === 'ENOENT') return null; throw error; });
    if (old !== null && old !== marker) throw new Error('Refusing to modify an unrecognized existing fixture directory');
    if (old === null && (await readdir(FIXTURE)).length > 0) throw new Error('Refusing to overwrite files in an unmarked existing directory');
    await writeFile(join(FIXTURE, 'TEST-FIXTURE.txt'), marker);
    ownsFixture = true;
    await writeFile(join(FIXTURE, 'readable.c'), '/* EXPLICIT TEST FIXTURE */\nint fixture_readable(void) { return 42; }\n');
    await writeFile(join(FIXTURE, 'partial.c'), '/* EXPLICIT TEST FIXTURE: malformed declaration */\nint fixture_broken( {\n');
    await writeFile(join(FIXTURE, 'excluded.c'), '/* EXPLICIT TEST FIXTURE intentionally excluded */\nint excluded(void) { return 0; }\n');
    await writeFile(join(FIXTURE, '.cbmignore'), 'excluded.c\n');
    const unreadable = join(FIXTURE, 'unreadable.c');
    await chmod(unreadable, 0o600).catch((error) => { if (error.code !== 'ENOENT') throw error; });
    await writeFile(unreadable, '/* EXPLICIT TEST FIXTURE: chmod 000 during this run */\nint inaccessible(void) { return 1; }\n');
    report.actions.push({ action: 'Created synthetic C fixtures initially readable, including malformed partial.c and excluded.c', path: FIXTURE });
}

async function failedJobWhileWatching(page, workspace, project) {
    await page.bringToFront();
    await page.locator(`[data-workspace-tab="${workspace}"]`).click();
    await page.locator('.daemon-alerts-heading > span').filter({ hasText: 'Last checked' }).waitFor({ timeout: 20000 });
    const before = await json('/api/logs?lines=500&min_level=error');
    const job = await index(project);
    assert.equal(job.status, 'error', 'Unreadable fixture input must fail a real daemon index job');
    let event;
    const deadline = Date.now() + 20000;
    while (Date.now() < deadline) {
        const logs = await json('/api/logs?lines=500&min_level=error');
        event = logs.records?.find((entry) => entry.id > (before.cursor ?? 0) && entry.message.includes('ui.index.done') && entry.message.includes(FIXTURE));
        if (event) break;
        await delay(250);
    }
    assert.ok(event, 'A real failed index completion must reach persisted daemon errors');
    const exact = page.locator('.daemon-alert-error').filter({ hasText: `Event #${event.id}` });
    await exact.waitFor({ state: 'visible', timeout: 20000 });
    assert.ok((await exact.innerText()).includes('ERROR'));
    report.checks[`${workspace}LiveError`] = { event, observedWithoutPageReload: true, text: await exact.innerText(), priorityPanelBounds: await page.locator('[data-testid="daemon-alerts"]').boundingBox() };
    if (workspace === 'system') {
        await page.locator('#system-tab-indexes').click();
        await page.locator('.system-job-error').first().waitFor({ timeout: 15000 });
        await page.locator('#system-tab-logs').click();
        await page.getByLabel('Log severity', { exact: true }).selectOption('error');
        await page.locator('.system-log-record.system-log-error').first().waitFor({ timeout: 15000 });
        const daemonErrors = await json('/api/logs?lines=200&min_level=error');
        await page.locator('.system-log-meta').filter({ hasText: `${daemonErrors.total} matching retained events` }).waitFor({ timeout: 15000 });
        report.checks.systemFiltersBeforeLimit = { total: daemonErrors.total, visibleErrorRows: await page.locator('.system-log-record.system-log-error').count(), text: await page.locator('.system-log-meta').innerText() };
        assert.ok(daemonErrors.total > 0, 'Retained historical errors cannot disappear behind newer info logs');
        await page.getByLabel('Log scope', { exact: true }).selectOption('project');
        const scopedErrors = await json(`/api/logs?lines=200&min_level=error&project=${encodeURIComponent(project)}`);
        await page.locator('.system-log-meta').filter({ hasText: `${scopedErrors.total} matching retained events` }).waitFor({ timeout: 15000 });
        await page.locator('.system-log-record.system-log-error').filter({ hasText: `Event #${event.id}` }).waitFor({ timeout: 15000 });
        report.checks.systemProjectScope = { total: scopedErrors.total, project, text: await page.locator('.system-log-meta').innerText() };
        await page.getByPlaceholder('Find a source, path or message…').fill('semantic_manifest.err');
        const searched = await json(`/api/logs?lines=200&min_level=error&project=${encodeURIComponent(project)}&q=semantic_manifest.err`);
        await page.locator('.system-log-meta').filter({ hasText: `${searched.total} matching retained events` }).waitFor({ timeout: 15000 });
        report.checks.systemSearchBeforeLimit = { total: searched.total, query: searched.query, text: await page.locator('.system-log-meta').innerText() };
        assert.ok(searched.total > 0);
        await capture(page, 'errors-system-filtered-history.png');
        await page.getByPlaceholder('Find a source, path or message…').fill('');
    }
    await capture(page, `errors-after-${workspace}.png`);
}

async function main() {
    await mkdir(OUT, { recursive: true });
    await fixturePrepare();
    report.indexWithCoverage = await index(PROJECT);
    assert.equal(report.indexWithCoverage.status, 'done', 'Initial readable fixture must establish a real partial index');
    report.indexStatus = await rpc('index_status', { project: PROJECT });
    report.coverage = await rpc('check_index_coverage', { project: PROJECT, scopes: ['.'], paths: ['unreadable.c', 'partial.c', 'excluded.c'] });
    report.checks.coverageContainsPartial = JSON.stringify(report.coverage).includes('parse_partial');
    report.checks.coverageContainsExclusion = JSON.stringify(report.coverage).includes('cbmignore');
    assert.ok(report.checks.coverageContainsPartial, 'Malformed fixture must produce actual parse_partial coverage');
    assert.ok(report.checks.coverageContainsExclusion, '.cbmignore fixture must produce actual exclusion evidence');
    await chmod(join(FIXTURE, 'unreadable.c'), 0);
    await assert.rejects(readFile(join(FIXTURE, 'unreadable.c')), { code: 'EACCES' });
    report.actions.push({ action: 'Set unreadable.c chmod 000; real EACCES verified; next reindex must fail', path: FIXTURE });
    browser = await chromium.launch({ headless: true, args: ['--disable-background-networking', '--disable-component-update', '--disable-sync', '--no-first-run', '--use-angle=swiftshader'] });
    const context = await browser.newContext({ viewport: { width: 1560, height: 1100 }, reducedMotion: 'reduce' });
    await context.route('**/*', async (route) => {
        const req = route.request(); const url = req.url();
        report.browserRequests.push({ at: new Date().toISOString(), method: req.method(), url, ...(url === `${ORIGIN}/rpc` ? { tool: req.postDataJSON()?.params?.name } : {}) });
        if (url.startsWith(ORIGIN + '/') || url.startsWith('data:') || url.startsWith('blob:')) return route.continue();
        report.blockedExternalRequests.push(url); return route.abort();
    });
    const page = await context.newPage();
    page.on('pageerror', (error) => { (report.browserErrors ??= []).push(error.message); });
    await page.goto(`${ORIGIN}/?project=${PROJECT}`, { waitUntil: 'domcontentloaded' });
    await page.locator('[data-testid="daemon-alerts"]').waitFor({ timeout: 60000 });
    if (await page.locator('.cbm-welcome-backdrop').isVisible()) {
        await page.locator('.cbm-welcome .cbm-primary').click();
        report.actions.push({ action: 'Closed first-visit welcome via its normal start button' });
    }
    await failedJobWhileWatching(page, 'system', PROJECT);
    await failedJobWhileWatching(page, 'galaxy', PROJECT);
    report.coverageAfterFailedRefresh = await rpc('check_index_coverage', { project: PROJECT, scopes: ['.'], paths: ['unreadable.c', 'partial.c', 'excluded.c'] });
    const beforeDiagnosis = report.browserRequests.length;
    await page.getByRole('button', { name: 'Inspect index coverage', exact: true }).click();
    const panel = page.getByRole('dialog', { name: 'Index coverage diagnosis', exact: true });
    await panel.waitFor();
    assert.equal(await panel.locator('textarea').count(), 0, 'Opening diagnosis must not generate a report');
    report.checks.beforeExplicitDiagnosis = { reportAbsent: true, requests: report.browserRequests.slice(beforeDiagnosis) };
    const diagnosisStart = report.browserRequests.length;
    await panel.getByRole('button', { name: 'Run local diagnosis', exact: true }).click();
    await panel.getByRole('textbox', { name: 'Editable local diagnostic report' }).waitFor({ timeout: 30000 });
    report.localReport = await panel.getByRole('textbox', { name: 'Editable local diagnostic report' }).inputValue();
    assert.ok(report.localReport.includes('Source lines: 2-2'), 'Actual parser source range must survive the shared coverage join and local report');
    report.checks.localReportPreservesParserSourceRanges = true;
    report.checks.localDiagnosis = { text: await panel.innerText(), requests: report.browserRequests.slice(diagnosisStart) };
    await capture(page, 'errors-local-diagnosis.png');
    await panel.getByRole('textbox', { name: 'Editable local diagnostic report' }).scrollIntoViewIfNeeded();
    await capture(page, 'errors-local-report.png');
    const draft = 'EXPLICIT TEST FIXTURE: locally reviewed and redacted draft. No publication requested.';
    await panel.getByRole('textbox', { name: 'Editable local diagnostic report' }).fill(draft);
    const downloadPromise = page.waitForEvent('download');
    await panel.getByRole('button', { name: 'Download edited report', exact: true }).click();
    const download = await downloadPromise;
    const draftPath = join(OUT, `${proofPrefix}errors-reviewed-local-report.txt`); await download.saveAs(draftPath);
    assert.equal(await readFile(draftPath, 'utf8'), draft);
    report.checks.downloadContainsOnlyEditedText = true;
    report.checks.onlySameDaemonOrigin = report.browserRequests.filter((entry) => !entry.url.startsWith('blob:') && !entry.url.startsWith('data:')).every((entry) => new URL(entry.url).origin === ORIGIN);
    report.sqliteEvidence = JSON.parse(execFileSync('python3', ['-c', `import sqlite3,json\na=sqlite3.connect('file:${CACHE}/activity.db?mode=ro',uri=True)\ns=sqlite3.connect('file:${CACHE}/${PROJECT}.db?mode=ro',uri=True)\nprint(json.dumps({'logs':a.execute("SELECT id,ts,level,source,message FROM daemon_logs WHERE message LIKE '%cbm-2068-error-fixture%' ORDER BY id").fetchall(),'coverage':s.execute("SELECT project,rel_path,kind,detail FROM index_coverage WHERE project=?",('${PROJECT}',)).fetchall()}))`], { encoding: 'utf8' }));
    const cutoff = report.checks.systemLiveError.event.id;
    report.checks.filterCountsVerifiedInSqlite = JSON.parse(execFileSync('python3', ['-c', `import sqlite3,json,sys
conn=sqlite3.connect('file:${CACHE}/activity.db?mode=ro',uri=True)
cutoff=int(sys.argv[1]); project=sys.argv[2]
print(json.dumps({'as_of_event_id':cutoff,'daemon_errors':conn.execute('SELECT count(*) FROM daemon_logs WHERE level>=3 AND id<=?',(cutoff,)).fetchone()[0],'project_errors':conn.execute('SELECT count(*) FROM daemon_logs WHERE level>=3 AND id<=? AND project=?',(cutoff,project)).fetchone()[0],'project_manifest_errors':conn.execute("SELECT count(*) FROM daemon_logs WHERE level>=3 AND id<=? AND project=? AND instr(lower(message),'semantic_manifest.err')>0",(cutoff,project)).fetchone()[0]}))`, String(cutoff), PROJECT], { encoding: 'utf8' }));
    assert.equal(report.checks.filterCountsVerifiedInSqlite.daemon_errors, report.checks.systemFiltersBeforeLimit.total);
    assert.equal(report.checks.filterCountsVerifiedInSqlite.project_errors, report.checks.systemProjectScope.total);
    assert.equal(report.checks.filterCountsVerifiedInSqlite.project_manifest_errors, report.checks.systemSearchBeforeLimit.total);
    report.completedAt = new Date().toISOString();
}

try { await main(); }
catch (error) { report.failure = error.stack ?? String(error); process.exitCode = 1; }
finally {
    if (ownsFixture) await chmod(join(FIXTURE, 'unreadable.c'), 0o600).then(() => report.cleanup.push('unreadable.c restored to 0600'), () => {});
    if (browser) await browser.close();
    await saveReport();
    console.log(JSON.stringify({ report: join(OUT, `${proofPrefix}errors-e2e.json`), completed: !!report.completedAt, failure: report.failure, screenshots: report.screenshots, cleanup: report.cleanup }, null, 2));
}
