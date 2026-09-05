// W8c acceptance tests: the pseudocode block leads with what the code does not
// say, every step carries where it goes, and nothing about the block itself
// takes the place of the block. Run: node --test tests/scaffold/w8c.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const block = () => JSON.parse(read('verification/w8c/pseudocode.json'));

test('AC1: the block leads with the finding, not with the habit', () => {
  const b = block();
  assert.equal(b.findingFirst, true,
    'was der Leser im Code NICHT sieht, steht oben; die Schrittliste darunter');
  assert.equal(b.headSaysWhatTheFindingIs, true,
    'der Kopf nennt in einem kurzen Satz, was in diesem Block der Fund ist');
  assert.equal(b.noLengthThreshold, true,
    'es darf keine geratene Zahl geben, ab der eine Funktion "kurz genug" ist');
});

test('AC2: every step carries where it goes', () => {
  const b = block();
  assert.equal(b.stepTargetsClickable, true,
    'jede Schrittzeile nennt Datei und Zeile und ist klickbar');
  assert.equal(b.stepClickOpensReader, true, 'ein Klick oeffnet die Stelle im Reader');
  assert.equal(b.stepsWithoutTargetExplained, true,
    'wo der Index keine Stelle kennt, steht das an der Zeile, statt sie stumm zu lassen');
});

test('AC3: what lies behind a call, as far as the index gives it without a new request', () => {
  const b = block();
  assert.ok(b.enrichmentAvailable && typeof b.enrichmentAvailable === 'object',
    'die Messung, was ohne zusaetzliche Anfrage vorliegt, muss ihr Ergebnis hinterlassen');
  assert.ok(Array.isArray(b.enrichmentAvailable.usable),
    'was ging, gehoert einzeln ins Artefakt');
  assert.ok(Array.isArray(b.enrichmentAvailable.missing),
    'was nicht ging, ebenso; sonst waere die Entscheidung nicht nachvollziehbar');
  if (b.enrichmentAvailable.usable.length > 0) {
    assert.ok(b.enrichedSteps >= 1,
      'was verfuegbar ist, muss auch an der Schrittzeile stehen');
  }
  assert.equal(b.noExtraServerRequest, true,
    'fuer die Anreicherung darf kein zusaetzlicher Serverweg entstehen');
});

test('AC4: the import finding gets the rank it earns, and keeps its limit', () => {
  const b = block();
  assert.equal(b.importFindingProminent, true,
    'der ungenutzte Import ist als Fund erkennbar, nicht als Zeile unter anderen');
  assert.equal(b.importHonestyWordsKept, true,
    '"as far as the index shows" und die Zahl der ungepruefbaren Faelle bleiben stehen');
});

test('AC5: no meta noise', () => {
  const b = block();
  assert.ok(Number.isFinite(b.metaCharsBefore) && b.metaCharsBefore > 0,
    'der Ausgangswert muss gemessen und festgehalten sein');
  assert.ok(b.metaCharsAfter <= b.metaCharsBefore / 4,
    `hoechstens ein Viertel erwartet, war ${b.metaCharsAfter} von ${b.metaCharsBefore}`);
});

test('AC6: nothing is invented', () => {
  const b = block();
  assert.equal(b.modelRequestsWhileOff, 0,
    'bei ausgeschaltetem Modell darf kein Byte Richtung 4141 gehen');
  assert.equal(b.refineStillGated, true,
    'die Umformulierung bleibt vom Leser angestossen und positionsgenau geprueft');
});

test('AC7/AC8: proof run and wiring', () => {
  const b = block();
  assert.equal(b.overlapViolations, 0);
  assert.equal(b.clippingViolations, 0);
  assert.equal(b.cutWithoutHint, 0);
  assert.ok(b.port >= 4560, `Port >= 4560 erwartet, war ${b.port}`);
  assert.equal(b.leftoverProcesses, 0);
  for (const shot of ['pseudocode-short.png', 'pseudocode-long.png']) {
    const p = join(ROOT, 'verification', 'w8c', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w8c/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w8c/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w8c'], 'Script smoke:w8c fehlt');
});
