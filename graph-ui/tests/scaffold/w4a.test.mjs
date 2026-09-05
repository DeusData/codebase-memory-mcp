// W4a acceptance tests: neutral entry modes, the deterministic topsort tour,
// the forward walk that starts where the maintainer chooses, and the quiet
// checklist counters. Run: node --test tests/scaffold/w4a.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the mode layer is ported with provenance', () => {
  const files = [
    'src/tours/tour-generator.ts',
    'src/tours/tour-model.ts',
    'src/provider/closure.ts',
    'src/checklist/checklist-model.ts',
  ];
  for (const f of files) {
    assert.ok(existsSync(join(ROOT, f)), `${f} fehlt`);
  }
  for (const f of ['src/tours/tour-generator.ts', 'src/tours/tour-model.ts',
    'src/checklist/checklist-model.ts']) {
    assert.ok(read(f).includes('CodeAtlasIDE'), `${f} braucht die Herkunftsnotiz`);
  }
  const gen = read('src/tours/tour-generator.ts');
  for (const marker of ['topsort', 'brokenEdges', 'sampleOrder']) {
    assert.ok(gen.includes(marker), `Tour-Generator-Marker ${marker} fehlt`);
  }
  const closure = read('src/provider/closure.ts');
  assert.ok(/truncated/.test(closure) && /visited/.test(closure),
    'Closure muss beide Grenzen sichtbar machen');
  assert.ok(existsSync(join(ROOT, 'src/tours/tour-generator.test.ts')),
    'Topsort-Invarianten-Tests fehlen');
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.ok(s.unitTests >= 300, `Unit-Suite muss >= 300 Tests fahren, war ${s.unitTests}`);
});

test('AC2/AC3: why panel and topsort tour proven live', () => {
  const t = JSON.parse(read('verification/w4/tours.json'));
  assert.equal(t.whyShown, true, 'das Why-Panel muss erscheinen');
  assert.equal(t.modesNeutral, true, 'keine Lern-Vokabeln in sichtbaren Texten');
  assert.ok(t.steps >= 5, `Tour braucht >= 5 Schritte, war ${t.steps}`);
  assert.equal(t.orderCorrect, true, 'config/types muessen vor routes/server kommen');
  assert.equal(t.deterministic, true, 'zweiter Lauf muss byte-identisch sein');
  assert.equal(t.playerKeyboardNavigates, true, 'Enter/ArrowLeft muessen steuern');
  assert.equal(t.twinFollowsStep, true, 'der Twin muss jedem Schritt folgen');
  assert.equal(t.stepMarksVisited, true);
  assert.equal(t.exploredCounterRises, true, 'die stille Anzeige muss steigen');
  assert.ok(t.port >= 4260, `Port >= 4260 erwartet, war ${t.port}`);
  assert.equal(t.leftoverProcesses, 0);
});

test('AC4: the forward walk starts exactly where the maintainer chose', () => {
  const t = JSON.parse(read('verification/w4/tours.json'));
  assert.equal(t.entryPointTourStartsAtChosen, true);
  assert.match(t.entryFirstStep, /createUser/, 'Schritt 1 muss das gewaehlte Symbol sein');
  assert.ok(t.entrySteps >= 3, `Vorwaerts-Walk braucht >= 3 Schritte, war ${t.entrySteps}`);
  assert.equal(t.entryHasNoConfigPrelude, true, 'kein Config-Pflichtprogramm davor');
  assert.equal(t.closureTruncationHonest, true);
});

test('AC5: screenshots prove the cards', () => {
  for (const shot of ['why.png', 'tour.png']) {
    const p = join(ROOT, 'verification', 'w4', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
});

test('AC6: net deny over the whole w4a click path', () => {
  const nd = JSON.parse(read('verification/w4/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10);
  assert.ok(/smoke-w4a/.test(nd.command));
});

test('AC7: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w4a'], 'Script smoke:w4a fehlt');
  assert.ok(existsSync(join(ROOT, 'tools/smoke-w4a.mjs')), 'tools/smoke-w4a.mjs fehlt');
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
