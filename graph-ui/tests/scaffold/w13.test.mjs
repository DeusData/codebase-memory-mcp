// W13 acceptance tests: the slider asks who is reading, not how much, and the
// five levels differ even with the model switched off.
// Run: node --test tests/scaffold/w13.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const reader = () => JSON.parse(read('verification/w13/reader.json'));

const NAMES = ['vibe coder', 'junior', 'medior', 'senior', 'architect'];

test('AC1: five levels, demonstrably different, without a model', () => {
  const r = reader();
  assert.ok(Array.isArray(r.levels) && r.levels.length === 5,
    `fuenf Stufen erwartet, waren ${r.levels?.length}`);
  for (const [index, name] of NAMES.entries()) {
    assert.equal(r.levels[index]?.name, name, `Stufe ${index} muss "${name}" heissen`);
  }
  assert.equal(r.allLevelsDiffer, true, 'keine zwei Stufen duerfen denselben Text erzeugen');
  for (const level of r.levels) {
    assert.ok(level.chars > 0, `${level.name} darf nicht leer sein`);
    assert.ok(typeof level.uniqueElement === 'string' && level.uniqueElement.length > 0,
      `${level.name} braucht ein Element, das keine andere Stufe hat`);
  }
  assert.equal(r.measuredWithModelOff, true,
    'gemessen wird bei ausgeschaltetem Modell: ohne Modell muss die Oberflaeche vollstaendig sein');
});

test('AC2: the slider says who it is for', () => {
  const r = reader();
  assert.equal(r.sliderNamesReader, true,
    'die Beschriftung fragt nach dem Leser, nicht nach der Menge');
  assert.equal(r.levelNameShown, true, 'der Name der gewaehlten Stufe steht daneben');
});

test('AC3/AC4: the model rephrases, it does not invent, and says so', () => {
  const r = reader();
  assert.equal(r.modelRephrasesOnly, true,
    'dieselben Namen, Zahlen, Dateien, Zeilen und dieselbe Reihenfolge');
  assert.ok(Number.isInteger(r.rejectedRewrites),
    'die Zahl der verworfenen Umschreibungen gehoert ins Artefakt');
  assert.equal(r.rejectionShownToReader, true,
    'wurde verworfen, erfaehrt der Leser es, statt es stillschweigend zu schlucken');
  assert.equal(r.provenanceVisible, true,
    'ein formulierter Abschnitt traegt seinen Herkunftsvermerk');
});

test('AC5/AC6/AC7: off stays off, the level holds, and nothing is empty', () => {
  const r = reader();
  assert.equal(r.noRequestsWhileOff, 0,
    'bei ausgeschaltetem Modell geht kein fetch, auch nicht beim Stufenwechsel');
  assert.equal(r.levelSurvivesReload, true, 'die Wahl ueberlebt den Reload');
  assert.equal(r.levelSurvivesSymbolChange, true,
    'sie ist eine Aussage ueber den Leser, nicht ueber das Symbol');
  assert.equal(r.emptyLevelExplainsItself, true,
    'wo eine Stufe nichts beitragen kann, sagt sie das in ihrer eigenen Sprache');
});

test('AC8/AC9: proof run, one picture per level', () => {
  const r = reader();
  assert.equal(r.overlapViolations, 0);
  assert.equal(r.clippingViolations, 0);
  assert.equal(r.cutWithoutHint, 0);
  assert.ok(r.port >= 4600, `Port >= 4600 erwartet, war ${r.port}`);
  assert.equal(r.leftoverProcesses, 0);
  for (const level of ['vibe-coder', 'junior', 'medior', 'senior', 'architect']) {
    const p = join(ROOT, 'verification', 'w13', `level-${level}.png`);
    assert.ok(existsSync(p), `level-${level}.png fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `level-${level}.png verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w13/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w13/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w13'], 'Script smoke:w13 fehlt');
  const evalCheck = JSON.parse(read('verification/w6/evalcheck.json'));
  assert.equal(evalCheck.evalCheckPass, true,
    'die Eval-Kennzahlen der Sieger duerfen dabei nicht fallen');
});
