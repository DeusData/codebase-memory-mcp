// W2b acceptance tests: the semantic twin runs in the SPA with the reference
// project's honesty rules preserved, the cursor pipeline fills it without
// refetch storms, and the step badges stay in sync with the STEPS list.
// Run: node --test tests/scaffold/w2b.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the twin layer is ported with provenance and fixture-proven', () => {
  const files = [
    'src/twin/render-model.ts',
    'src/twin/twin-view-model.ts',
    'src/twin/strings.ts',
    'src/twin/presentation-profile.ts',
    'src/twin/mini-understanding.ts',
    'src/twin/hop-plan.ts',
    'src/core/step-badge-decorator.ts',
  ];
  for (const f of files) {
    assert.ok(existsSync(join(ROOT, f)), `${f} fehlt`);
    assert.ok(read(f).includes('CodeAtlasIDE'), `${f} braucht die Herkunftsnotiz`);
  }
  assert.ok(existsSync(join(ROOT, 'src/twin/render-model.test.ts')),
    'render-model-Verhaltenstests fehlen');
  const rm = read('src/twin/render-model.test.ts');
  for (const marker of ['Nobody wrote that description', 'confidence', 'validateUser']) {
    assert.ok(rm.includes(marker), `render-model-Test muss ${marker} pruefen`);
  }
  const fixtures = read('src/twin/render-model.test.ts') + (existsSync(join(ROOT, 'src/twin/__fixtures__/create-user-ir.json')) ? 'FIXTURE_OK' : '');
  assert.ok(fixtures.includes('FIXTURE_OK'), 'create-user-ir.json-Fixture fehlt');
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.ok(s.unitTests >= 220, `Unit-Suite muss >= 220 Tests fahren, war ${s.unitTests}`);
  assert.equal(s.unitExit, 0);
});

test('AC2/AC3/AC4: the live twin proof holds', () => {
  const t = JSON.parse(read('verification/w2/twin.json'));
  assert.ok(t.sectionsPopulated >= 5, `>= 5 Sections erwartet, war ${t.sectionsPopulated}`);
  assert.equal(t.stepsCount, 6);
  assert.equal(t.stepsOrdered, true);
  assert.equal(t.firstStep, 'validateUser');
  assert.equal(t.envReadShown, 'DB_URL');
  assert.equal(t.throwShown, 'ValidationError');
  assert.equal(t.missingTestsHonest, true,
    'createUser muss die ehrliche Kein-Test-Aussage zeigen');
  assert.equal(t.depthProseNoQualifiedNames, true);
  assert.equal(t.depthDenseShowsConfidence, true);
  assert.ok(t.badgeCount >= 3, `>= 3 Gutter-Badges erwartet, war ${t.badgeCount}`);
  assert.equal(t.caretSyncNoRefetch, true, 'Caret-Sync darf keinen Refetch ausloesen');
  assert.equal(t.followNavigatesEditor, true, 'Step-Klick muss den Editor bewegen');
  assert.equal(t.evidenceVisible, true);
  assert.ok(t.port >= 4240, `Port >= 4240 erwartet, war ${t.port}`);
  assert.equal(t.leftoverProcesses, 0);
  for (const shot of ['twin.png', 'twin-dense.png']) {
    const p = join(ROOT, 'verification', 'w2', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
});

test('AC5: the twin click path ran under the net deny gate', () => {
  const nd = JSON.parse(read('verification/w2/netdeny-w2b.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10, `>= 10 Samples erwartet, war ${nd.samples}`);
  assert.ok(/smoke-w2b/.test(nd.command));
});

test('AC6: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w2b'], 'Script smoke:w2b fehlt');
  assert.ok(existsSync(join(ROOT, 'tools/smoke-w2b.mjs')), 'tools/smoke-w2b.mjs fehlt');
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
