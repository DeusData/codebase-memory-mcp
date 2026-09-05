// W1a acceptance tests: the app scaffold builds, the rpc transport with the
// compact-rows parser exists and is unit-proven, the semantic IR honesty
// types are ported with provenance, and the net deny gate proved zero
// outbound connections over the unit suite.
// Run: node --test tests/scaffold/w1.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the Vite scaffold builds and the unit suite is green', () => {
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.equal(s.buildExit, 0, 'npm run build muss Exit 0 liefern');
  assert.equal(s.unitExit, 0, 'vitest run muss Exit 0 liefern');
  assert.ok(Number.isInteger(s.unitTests) && s.unitTests >= 10,
    `mindestens 10 Unit-Tests erwartet, war ${s.unitTests}`);
  assert.ok(Number.isInteger(s.distFiles) && s.distFiles >= 2,
    'dist/ muss mindestens index.html und ein Bundle enthalten');
  for (const f of ['index.html', 'src/main.tsx', 'src/App.tsx', 'vite.config.ts']) {
    assert.ok(existsSync(join(ROOT, f)), `${f} fehlt`);
  }
  const pkg = JSON.parse(read('package.json'));
  for (const dep of ['react', 'react-dom']) {
    assert.ok(pkg.dependencies?.[dep], `${dep} muss dependency sein`);
  }
  for (const dep of ['vite', 'typescript', 'vitest']) {
    assert.ok(pkg.devDependencies?.[dep], `${dep} muss devDependency sein`);
  }
  assert.match(pkg.dependencies.react, /^19\./, 'react 19 wie im PR-Frontend');
  assert.match(pkg.devDependencies.vite, /^6\./, 'vite 6 wie im PR-Frontend');
});

test('AC2: rpc transport and compact-rows parser are in place and unit-proven', () => {
  const transport = read('src/provider/rpc-transport.ts');
  assert.ok(transport.includes('tools/call'), 'Transport muss MCP tools/call sprechen');
  assert.ok(/callTool/.test(transport), 'callTool muss exportiert sein');
  assert.ok(/403|not.?allowed/i.test(transport), 'Fehlertaxonomie muss 403/not-allowed kennen');
  const parser = read('src/provider/compact-rows.ts');
  assert.ok(/parseCompactRows/.test(parser), 'parseCompactRows muss exportiert sein');
  const parserTest = read('src/provider/compact-rows.test.ts');
  assert.ok(parserTest.includes('(cols:'), 'Parser-Tests muessen das reale Kopfformat abdecken');
  assert.ok(parserTest.includes('hint'), 'Parser-Tests muessen den Leer-Fall mit hint abdecken');
  assert.ok(parserTest.includes('validateUser'), 'Parser-Tests muessen die aufgezeichnete CALLS-Antwort nutzen');
});

test('AC3: semantic IR honesty types are ported with provenance', () => {
  const ir = read('src/core/semantic-ir.ts');
  assert.ok(ir.includes('CodeAtlasIDE'), 'Herkunftsnotiz auf das Referenz-Repo fehlt');
  for (const marker of ['KnowledgeState', 'Evidence', 'Fact', 'unsupported']) {
    assert.ok(ir.includes(marker), `Marker ${marker} fehlt im IR-Port`);
  }
  assert.ok(existsSync(join(ROOT, 'src/core/semantic-ir.test.ts')),
    'Unit-Tests fuer den IR-Port fehlen');
});

test('AC4: the net deny gate proved zero outbound over the unit suite', () => {
  assert.ok(existsSync(join(ROOT, 'tools/net-deny-gate.mjs')), 'tools/net-deny-gate.mjs fehlt');
  const nd = JSON.parse(read('verification/w1/netdeny.json'));
  assert.equal(nd.outboundViolations, 0, 'kein einziges Byte darf die Maschine verlassen');
  assert.ok(Number.isInteger(nd.samples) && nd.samples >= 3,
    `mindestens 3 lsof-Samples erwartet, war ${nd.samples}`);
  assert.ok(typeof nd.command === 'string' && nd.command.length > 0,
    'das geprobte Kommando muss dokumentiert sein');
});

test('AC5: wiring stays honest and pinned', () => {
  const pkg = JSON.parse(read('package.json'));
  for (const script of ['dev', 'build', 'test', 'test:unit', 'verify']) {
    assert.ok(pkg.scripts?.[script], `Script ${script} fehlt`);
  }
  assert.match(pkg.scripts.verify, /tests\/scaffold/, 'verify muss die Scaffold-Abnahmen fahren');
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version),
      `${name} muss exakt gepinnt sein, war ${version}`);
  }
  const ignore = read('.gitignore');
  assert.ok(/^dist\/$/m.test(ignore), 'dist/ muss ignoriert sein');
});
