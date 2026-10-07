#!/usr/bin/env node
/* Explicit synthetic hook events, real producer, daemon, SQLite and browser.
 * Usage: node tools/pr-2068-agents-e2e.mjs before-restart
 *        [owner restarts ONLY its isolated daemon on 9749]
 *        node tools/pr-2068-agents-e2e.mjs after-restart
 * This script never starts/stops a daemon or touches another listener. */
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const UI = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const REPO = resolve(UI, '..');
const OUT = join(UI, 'verification/pr-2068');
const ORIGIN = 'http://127.0.0.1:9749';
const PROJECT = 'cbm-pr2068';
const CACHE = process.env.CBM_PR2068_CACHE ?? '/tmp/cbm-2068-cache';
const phase = process.argv[2];
assert.ok(['before-restart', 'after-restart'].includes(phase), 'Explicit lifecycle phase is required');
const delay = ms => new Promise(done => setTimeout(done, ms));
const report = { phase, startedAt: new Date().toISOString(), syntheticFixture: true,
    origin: ORIGIN, cache: CACHE, project: PROJECT, browserRequests: [], externalAttempts: [],
    directRequests: [], screenshots: [], checks: {} };
let browser;

async function json(path, body) {
    const response = await fetch(ORIGIN + path, { method: body ? 'POST' : 'GET',
        ...(body ? { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) } : {}) });
    report.directRequests.push({ path, method: body ? 'POST' : 'GET', status: response.status });
    assert.ok(response.ok, `${path}: HTTP ${response.status}`);
    return response.json();
}

function persistedRows(run) {
    const script = `import json,sqlite3,sys,pathlib
db=sqlite3.connect(pathlib.Path(sys.argv[1]).resolve().as_uri()+'?mode=ro',uri=True)
rows=[]
for id,payload in db.execute('SELECT id,payload FROM agent_events WHERE project=? AND run=? ORDER BY id',(sys.argv[2],sys.argv[3])):
 e=json.loads(payload);rows.append({'id':id,'seq':e['seq'],'ts':e['ts'],'path':e.get('path'),'phase':e['phase']})
print(json.dumps(rows))
db.close()`;
    return JSON.parse(execFileSync('python3', ['-c', script, join(CACHE, 'activity.db'), PROJECT, run], { encoding: 'utf8' }));
}

async function snapshot(page, file) {
    await page.screenshot({ path: join(OUT, file), fullPage: false });
    report.screenshots.push(file);
}

async function openBrowser(agent) {
    browser = await chromium.launch({ headless: true, args: ['--disable-background-networking'] });
    const context = await browser.newContext({ viewport: { width: 1600, height: 1060 }, serviceWorkers: 'block' });
    let interrupt = false;
    await context.route('**/*', async route => {
        const url = route.request().url();
        if (!url.startsWith(ORIGIN + '/') && !url.startsWith('data:') && !url.startsWith('blob:')) {
            report.externalAttempts.push(url);
            return route.abort();
        }
        if (interrupt && new URL(url).pathname === '/api/agent-events') return route.abort('connectionfailed');
        return route.continue();
    });
    await context.addInitScript(() => {
        localStorage.setItem('cbm.workspace.setup', 'done');
        localStorage.setItem('cbm.workspace', 'agents');
    });
    const page = await context.newPage();
    page.on('request', request => report.browserRequests.push(request.url()));
    // A legacy port override must have no effect on the new source.
    await page.goto(`${ORIGIN}/?project=${PROJECT}&agents=4142`);
    await page.locator('.cbm-activity').waitFor({ timeout: 30000 });
    const agentCalls = () => report.browserRequests.filter(url => new URL(url).pathname === '/api/agent-events').length;
    const beforeOff = agentCalls();
    await delay(1200);
    assert.equal(agentCalls(), beforeOff, 'Off mode must not send any activity requests');
    await page.getByRole('button', { name: 'Load live activity', exact: true }).click();
    await page.locator('.cbm-activity-connection [data-state="connected"]').waitFor({ timeout: 20000 });
    await page.getByLabel('Agent', { exact: true }).selectOption({ label: agent });
    return { page, agentCalls, interrupt: value => { interrupt = value; } };
}

async function waitRows(page, expected) {
    await page.waitForFunction(count => document.querySelectorAll('.cbm-activity-row').length === count,
        expected, { timeout: 20000 });
    return page.locator('.cbm-activity-row code').allTextContents();
}

async function beforeRestart() {
    report.run = `pr2068-verification-${Date.now()}`;
    report.agent = `PR2068 verification fixture (${report.run})`;
    report.outboxDirectory = await mkdtemp(join(tmpdir(), 'pr2068-agent-verification-'));
    const before = await json(`/api/agent-events?project=${PROJECT}`);
    report.generation = before.generation;
    report.daemonPid = (await json('/api/processes')).self_pid;
    assert.ok(Number.isInteger(report.daemonPid), 'Daemon process identity must be reported');
    const env = { ...process.env, ATLAS_PROJECT: PROJECT, ATLAS_AGENT_NAME: report.agent,
        ATLAS_DAEMON_URL: ORIGIN, ATLAS_TRACE_FILE: join(report.outboxDirectory, 'events') };
    for (const file of ['src/ui/http_server.c', 'src/store/store.c']) {
        execFileSync('python3', [join(UI, 'agents/hooks/atlas-trace.py')], { env, input: JSON.stringify({
            session_id: report.run, cwd: REPO, tool_name: 'Read',
            tool_input: { file_path: join(REPO, file), offset: 1, limit: 3 },
        }), encoding: 'utf8' });
    }
    const delivered = (await json(`/api/agent-events?project=${PROJECT}&limit=500`)).events.filter(e => e.run === report.run);
    assert.equal(delivered.length, 2, 'Both real hook executions must persist at the daemon');
    assert.deepEqual(delivered.map(e => e.seq), [1, 2]);
    assert.ok(delivered.every(e => e.source === 'tool-hook' && e.agent === report.agent));
    const originalRows = persistedRows(report.run);
    const replay = await json('/api/agent-events', { project: PROJECT, events: delivered });
    assert.equal(replay.accepted, 0);
    assert.equal(replay.duplicates, 2);
    assert.deepEqual(persistedRows(report.run), originalRows, 'Duplicate retry must preserve SQLite IDs and count');
    report.checks.hookPersistenceAndRetry = { originalRows, replay, matchedAfterRetry: true };

    const harness = await openBrowser(report.agent);
    const { page } = harness;
    assert.deepEqual(await waitRows(page, 2), ['src/store/store.c', 'src/ui/http_server.c']);
    await page.locator('.cbm-activity-row').filter({ hasText: 'src/ui/http_server.c' }).click();
    await page.getByRole('button', { name: 'Open source', exact: true }).waitFor({ timeout: 20000 });
    await snapshot(page, 'agents-hook-evidence.png');
    await page.getByRole('button', { name: 'Open source', exact: true }).click();
    await page.locator('[data-workspace-tab="explore"][aria-selected="true"]').waitFor({ timeout: 20000 });
    await page.locator('.selected-code-path').filter({ hasText: 'src/ui/http_server.c' }).waitFor({ timeout: 20000 });
    await page.locator('[data-testid="atlas-reader-editor"] .view-lines').waitFor({ timeout: 20000 });
    report.checks.agentEvidenceOpenedRealSource = true;
    await snapshot(page, 'agents-source-navigation.png');
    await page.locator('[data-workspace-tab="agents"]').click();
    await waitRows(page, 2);
    harness.interrupt(true);
    await page.locator('.cbm-activity-connection [data-state="no-source"]').waitFor({ timeout: 15000 });
    await snapshot(page, 'agents-connection-interrupted.png');
    const now = Date.now();
    const late = [
        { ...delivered[0], seq: 4, ts: now + 1000, path: 'src/ui/atlas_api.c', source: 'verification-fixture' },
        { ...delivered[0], seq: 3, ts: now, path: 'src/daemon/host.c', source: 'verification-fixture' },
    ];
    assert.equal((await json('/api/agent-events', { project: PROJECT, events: late })).accepted, 2);
    harness.interrupt(false);
    await page.locator('.cbm-activity-connection [data-state="connected"]').waitFor({ timeout: 15000 });
    const expected = ['src/ui/atlas_api.c', 'src/daemon/host.c', 'src/store/store.c', 'src/ui/http_server.c'];
    assert.deepEqual(await waitRows(page, 4), expected, 'UI order must reflect event time, not arrival order');
    const gaps = page.locator('.cbm-activity-metrics > div').filter({ hasText: 'Missing sequence numbers' });
    assert.equal(await gaps.locator('strong').innerText(), '0', 'Late sequence must resolve the temporary gap');
    report.checks.reconnectAndOrdering = { displayedPaths: expected, sqliteArrivalSequences: persistedRows(report.run).map(r => r.seq), gaps: 0 };
    await snapshot(page, 'agents-reconnected.png');
    await page.reload();
    await page.locator('.cbm-activity').waitFor({ timeout: 20000 });
    const offCalls = harness.agentCalls();
    await delay(1200);
    assert.equal(harness.agentCalls(), offCalls);
    await page.getByRole('button', { name: 'Load live activity', exact: true }).click();
    await page.locator('.cbm-activity-connection [data-state="connected"]').waitFor({ timeout: 15000 });
    await page.getByLabel('Agent', { exact: true }).selectOption({ label: report.agent });
    assert.deepEqual(await waitRows(page, 4), expected);
    report.checks.browserReloadRebuiltHistory = true;
    report.persistedRows = persistedRows(report.run);
    report.expectedPaths = expected;
    await snapshot(page, 'agents-browser-reload.png');
}

async function afterRestart() {
    const prior = JSON.parse(await readFile(join(OUT, 'agents-before-restart.json'), 'utf8'));
    assert.equal(prior.complete, true, 'Successful pre-restart verification is required');
    report.run = prior.run; report.agent = prior.agent;
    const response = await json(`/api/agent-events?project=${PROJECT}&limit=500`);
    assert.equal(response.generation, prior.generation, 'Daemon restart must preserve the SQLite generation');
    report.daemonPid = (await json('/api/processes')).self_pid;
    assert.notEqual(report.daemonPid, prior.daemonPid, 'This phase requires an actual daemon process restart');
    const rows = persistedRows(prior.run);
    assert.deepEqual(rows, prior.persistedRows, 'Restart must preserve every fixture ID and event');
    const { page } = await openBrowser(prior.agent);
    assert.deepEqual(await waitRows(page, 4), prior.expectedPaths);
    report.checks.daemonRestartPreservedHistory = { previousPid: prior.daemonPid, currentPid: report.daemonPid,
        generation: response.generation, rows };
    await snapshot(page, 'agents-daemon-restart.png');
}

await mkdir(OUT, { recursive: true });
try {
    if (phase === 'before-restart') await beforeRestart(); else await afterRestart();
    assert.equal(report.externalAttempts.length, 0, 'Browser must never request a second port or external endpoint');
    report.checks.noBridgeRequests = !report.browserRequests.some(url => new URL(url).port === '4142');
    assert.equal(report.checks.noBridgeRequests, true);
    report.complete = true;
} catch (error) {
    report.complete = false;
    report.error = error.stack ?? String(error);
    process.exitCode = 1;
} finally {
    report.finishedAt = new Date().toISOString();
    await writeFile(join(OUT, `agents-${phase}.json`), JSON.stringify(report, null, 2) + '\n');
    await browser?.close();
    console.log(JSON.stringify({ phase, complete: report.complete, error: report.error,
        screenshots: report.screenshots, report: join(OUT, `agents-${phase}.json`) }, null, 2));
}
