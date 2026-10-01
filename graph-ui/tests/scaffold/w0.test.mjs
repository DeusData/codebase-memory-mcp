// W0 acceptance tests: the PR-1860 stack is inventoried with evidence, the
// upstream ask list exists for Bernhard to hand over, and the spike proves
// server-delivered source rendered read-only in Monaco with a decoration.
// Run: node --test tests/scaffold/w0.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: INVENTAR.md documents the PR-1860 stack with evidence', () => {
  const doc = read('INVENTAR.md');
  const sections = [
    'Frontend-Framework',
    '/rpc-Allowlist',
    '/api-Routen',
    'Server-Betrieb',
    'CORS',
    'Traces',
    'Galaxy',
    'Embedding',
  ];
  for (const s of sections) {
    assert.ok(doc.includes(s), `INVENTAR.md fehlt die Sektion ${s}`);
  }
  // the twelve read tools of the UI allowlist, verbatim
  for (const tool of ['list_projects', 'get_code_snippet', 'get_graph_schema',
    'search_graph', 'search_code', 'trace_path', 'trace_call_path',
    'get_architecture', 'query_graph', 'detect_changes',
    'check_index_coverage', 'index_status']) {
    assert.ok(doc.includes(tool), `Allowlist-Tool ${tool} fehlt in INVENTAR.md`);
  }
  assert.ok(doc.includes('manage_adr'), 'manage_adr-Sonderfall fehlt');
  assert.ok(doc.includes('tools/call'), 'Request-Format tools/call fehlt');
  assert.ok(/http_server\.c/.test(doc), 'Beleg auf http_server.c fehlt');
  assert.ok(doc.includes('9749'), 'Default-Port 9749 fehlt');
  assert.ok(doc.includes('127.0.0.1'), 'Bind-Adresse 127.0.0.1 fehlt');
  assert.ok(/initialize/i.test(doc), 'MCP-initialize-Handshake fehlt');
});

test('AC2: UPSTREAM-ASKS.md lists the gaps and keeps submission manual', () => {
  const doc = read('UPSTREAM-ASKS.md');
  assert.ok(/Datei-Streaming|file.streaming/i.test(doc), 'Datei-Streaming-Ask fehlt');
  assert.ok(/qualified_name/.test(doc), 'get_code_snippet-Beleg fehlt');
  assert.ok(/Deklaration|resolveSymbol/i.test(doc), 'Symbol-Aufloesungs-Ask fehlt');
  assert.ok(/traces/i.test(doc), 'Traces-Query-Ask fehlt');
  assert.ok(/Static|CORS|Origin/i.test(doc), 'Static-Serving/CORS-Ask fehlt');
  assert.ok(doc.includes('httpd.c:248'), 'erfuellter Bind-Ask (httpd.c:248) fehlt');
  assert.ok(/Bernhard.*(selbst|haendisch|händisch)/is.test(doc),
    'Hinweis auf haendische Einreichung durch Bernhard fehlt');
  assert.ok(!/github\.com\/DeusData.*(pull\/new|issues\/new)/.test(doc),
    'keine vorbereiteten Submission-Links Richtung DeusData');
});

test('AC3: the spike proved server source in read-only Monaco', () => {
  const spike = JSON.parse(read('verification/w0/spike.json'));
  assert.equal(spike.serverStarted, true, 'der gebaute Server muss laufen');
  assert.equal(spike.rpcTool, 'get_code_snippet', 'Quelle muss /rpc get_code_snippet sein');
  assert.equal(spike.monacoReadOnly, true, 'Monaco muss read-only sein');
  assert.ok(spike.decorationCount >= 1, 'mindestens eine Decoration an einer Call-Site');
  assert.ok(Number.isInteger(spike.port) && spike.port >= 4200,
    `Testport muss >= 4200 sein, war ${spike.port}`);
  assert.ok(typeof spike.screenshot === 'string' && spike.screenshot.length > 0,
    'Screenshot-Pfad fehlt in spike.json');
  const shot = join(ROOT, 'verification', 'w0', 'spike.png');
  assert.ok(existsSync(shot), 'verification/w0/spike.png fehlt');
  assert.ok(statSync(shot).size > 10 * 1024, 'Screenshot verdaechtig klein');
  assert.equal(spike.leftoverProcesses, 0, 'der Lauf darf keine Prozesse hinterlassen');
});

test('AC4: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.match(pkg.scripts?.test ?? '', /node --test/, 'npm test muss node --test fahren');
  assert.match(pkg.scripts?.verify ?? '', /w0\.test\.mjs|tests\/scaffold/,
    'verify muss die W0-Abnahme einschliessen');
  for (const dep of ['monaco-editor', 'playwright']) {
    const pin = pkg.devDependencies?.[dep];
    assert.ok(typeof pin === 'string' && pin.length > 0, `${dep} muss devDependency sein`);
    assert.ok(!/[\^~]/.test(pin), `${dep} muss exakt gepinnt sein, war ${pin}`);
  }
  assert.ok(existsSync(join(ROOT, 'tools', 'smoke-spike.mjs')), 'tools/smoke-spike.mjs fehlt');
  const ignore = read('.gitignore');
  assert.ok(/^cbm\/$/m.test(ignore), 'cbm/ muss ignoriert bleiben');
  assert.ok(/^node_modules\/$/m.test(ignore), 'node_modules/ muss ignoriert bleiben');
});
