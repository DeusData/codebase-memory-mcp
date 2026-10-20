// Monaco smoke evidence: server source is read-only and decorated.
// Run: node --test tests/scaffold/w0.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

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
