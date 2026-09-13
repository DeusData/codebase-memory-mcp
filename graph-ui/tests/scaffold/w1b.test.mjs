// W1b acceptance tests: the provider layer from the reference project runs
// against the PR-1860 server over /rpc, behavior preserved, proven live
// against the fixture truths of THIS branch.
// Run: node --test tests/scaffold/w1b.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the provider layer is ported with provenance', () => {
  const files = [
    'src/core/intelligence-provider.ts',
    'src/provider/cypher.ts',
    'src/provider/route-reader.ts',
    'src/provider/rpc-client.ts',
    'src/provider/cbm-rpc-provider.ts',
    'src/ir/semantic-ir-builder.ts',
  ];
  for (const f of files) {
    assert.ok(existsSync(join(ROOT, f)), `${f} fehlt`);
  }
  for (const f of files.filter((x) => !x.includes('rpc-client'))) {
    assert.ok(read(f).includes('CodeAtlasIDE'), `${f} braucht die Herkunftsnotiz`);
  }
  const iface = read('src/core/intelligence-provider.ts');
  for (const marker of ['IntelligenceProvider', 'IrFactSource', 'ResolveResult', 'FactKind']) {
    assert.ok(iface.includes(marker), `Interface-Marker ${marker} fehlt`);
  }
  const provider = read('src/provider/cbm-rpc-provider.ts');
  assert.ok(/id\s*=\s*'cbm'|id:\s*'cbm'|readonly id = 'cbm'/.test(provider),
    'Provider-id muss cbm bleiben (Evidence-Attribution)');
  const cypher = read('src/provider/cypher.ts');
  for (const marker of ['CALLS', 'RAISES', 'THROWS', 'CONFIGURES', 'USAGE', 'IMPORTS']) {
    assert.ok(cypher.includes(marker), `Cypher-Relation ${marker} fehlt`);
  }
});

test('AC2: the rpc client speaks /rpc, not child processes', () => {
  const client = read('src/provider/rpc-client.ts');
  assert.ok(/parseCompactRows/.test(client), 'query_graph muss durch parseCompactRows laufen');
  assert.ok(/search/i.test(client), 'search_graph-Pfad fehlt');
  assert.ok(!client.includes('child_process'), 'kein child_process im Browser-Produktpfad');
  const provider = read('src/provider/cbm-rpc-provider.ts');
  assert.ok(!provider.includes('child_process'), 'kein child_process im Provider');
});

test('AC3: behavior fidelity is unit-proven', () => {
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.equal(s.unitExit, 0, 'vitest run muss Exit 0 liefern');
  assert.ok(s.unitTests >= 60, `Unit-Suite muss >= 60 Tests fahren, war ${s.unitTests}`);
  for (const f of ['src/provider/cbm-rpc-provider.facts.test.ts',
    'src/provider/cbm-rpc-provider.resolve.test.ts',
    'src/ir/semantic-ir-builder.test.ts']) {
    assert.ok(existsSync(join(ROOT, f)), `${f} fehlt`);
  }
  const facts = read('src/provider/cbm-rpc-provider.facts.test.ts');
  assert.ok(/testedBy/.test(facts) && /inferred/.test(facts),
    'testedBy-Heuristik (nie known) muss getestet sein');
  assert.ok(/throw-declaration/.test(facts), 'throws-Merge muss getestet sein');
});

test('AC4: the live proof against the real server holds', () => {
  const p = JSON.parse(read('verification/w1/provider.json'));
  assert.equal(p.serverStarted, true);
  assert.ok(p.port >= 4220, `Port muss >= 4220 sein, war ${p.port}`);
  assert.equal(p.leftoverProcesses, 0);
  assert.match(p.resolvedQualifiedName, /userService\.createUser$/);
  assert.equal(p.callees.state, 'known');
  assert.equal(p.callees.count, 6);
  assert.equal(p.callees.first.name, 'validateUser');
  assert.equal(p.callees.first.line, 24);
  assert.equal(p.callers.state, 'known');
  assert.equal(p.callers.count, 3);
  assert.ok(p.callers.names.includes('registerUserRoutes'));
  assert.equal(p.callers.testCallers, 1);
  assert.equal(p.testedBy.state, 'inferred');
  assert.equal(p.testedBy.count, 1);
  assert.match(p.testedBy.first, /userService\.test\.ts$/);
  assert.equal(p.throws.state, 'known');
  assert.ok(p.throws.types.includes('ValidationError'));
  assert.ok(p.envReads.names.includes('DB_URL'));
  assert.ok(p.typeRefs.names.includes('User'));
  assert.match(p.snippetSha256, /^[0-9a-f]{64}$/);
  // Spezifikations-Korrektur 2026-08-28 (Orchestrator): createUser hat auf
  // diesem Branch KEINEN is_test-Aufrufer (einzige Test-CALLS-Kante im
  // Fixture ist test/userService.test.ts -> listUsers, dreifach am Server
  // belegt). missingTests ist fuer createUser daher wahr; die Gegenrichtung
  // wird am getesteten Symbol listUsers festgenagelt, damit beide
  // Polaritaeten bewiesen sind.
  assert.equal(p.ir.firstStep, 'validateUser');
  assert.equal(p.ir.missingTests, true);
  assert.equal(p.ir.missingTestsState, 'inferred');
  assert.equal(p.ir.complexityState, 'unsupported');
  assert.equal(p.ir.writesState, 'unsupported');
  assert.equal(p.irTested.firstStep, 'query');
  assert.equal(p.irTested.missingTests, false);
  assert.equal(p.irTested.missingTestsState, 'inferred');
  assert.equal(p.createUserTestedBy.state, 'inferred');
  assert.equal(p.createUserTestedBy.count, 0);
});

test('AC5: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w1b'], 'Script smoke:w1b fehlt');
  assert.ok(existsSync(join(ROOT, 'tools/smoke-w1b.mjs')), 'tools/smoke-w1b.mjs fehlt');
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
