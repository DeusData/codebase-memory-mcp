// W9 acceptance tests: the graph shows what kind of edge it is drawing, the
// legend counts what is really there, the filter takes a kind out of the
// picture without taking it out of the legend, and the hierarchy stops
// pretending that calls are the only relationship between two symbols.
// Run: node --test tests/scaffold/w9.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const edges = () => JSON.parse(read('verification/w9/edges.json'));

test('AC1: the kinds of edge are told apart in the rendered picture', () => {
  const e = edges();
  assert.ok(e.layoutEdgeTypes && typeof e.layoutEdgeTypes === 'object',
    'die gezaehlten Kantenarten des Layouts fehlen');
  const kinds = Object.keys(e.layoutEdgeTypes);
  assert.ok(kinds.length >= 5,
    `das Fixture soll mindestens 5 Kantenarten tragen, hatte ${kinds.length}`);
  assert.ok(e.distinctColorsRendered >= 5,
    `mindestens 5 unterscheidbare Farben im Bild erwartet, waren ${e.distinctColorsRendered}`);
  assert.ok(Array.isArray(e.renderedColors) && e.renderedColors.length >= 5,
    'die gemessenen Farben gehoeren einzeln ins Artefakt, nicht nur ihre Zahl');
  assert.ok(Number.isFinite(e.colorDistanceThreshold) && e.colorDistanceThreshold > 0,
    'die Schwelle, ab der zwei Farben als verschieden gelten, muss dastehen');
});

test('AC2: the legend counts this project, it does not recite a table', () => {
  const e = edges();
  assert.equal(e.legendShowsCounts, true, 'jede Zeile der Legende braucht ihre Zahl');
  assert.equal(e.legendMatchesLayout, true,
    'die Legende muss genau die Arten zeigen, die das Layout wirklich enthaelt');
  assert.equal(e.legendListsAbsentTypes, false,
    'eine Art, die im Projekt nicht vorkommt, darf nicht in der Legende stehen');
  assert.equal(e.legendSortedByCount, true, 'absteigend nach Zahl sortiert');
});

test('AC3: a kind can be taken out of the picture, but not out of the legend', () => {
  const e = edges();
  assert.equal(e.filterHidesType, true, 'das Abschalten muss die Art aus dem Bild nehmen');
  assert.equal(e.filterKeepsRowVisible, true,
    'die abgeschaltete Zeile bleibt sichtbar, sonst sieht ein Ausblenden aus wie ein Fehlen');
  assert.equal(e.filterRowDimmed, true, 'die abgeschaltete Zeile muss als solche erkennbar sein');
  assert.ok(typeof e.filterHeaderText === 'string' && /\d+\s+of\s+\d+/i.test(e.filterHeaderText),
    `der Kopf muss sagen, wie viel ausgeblendet ist, war "${e.filterHeaderText}"`);
  assert.equal(e.filterSurvivesViewSwitch, true,
    'der Zustand darf beim Wechsel zwischen Galaxy und Hierarchie nicht verfallen');
});

test('AC4: the hierarchy shows more than calls, and says how much more', () => {
  const e = edges();
  assert.ok(e.hierarchyWalkEdges >= 1, 'die Aufrufkanten des Walks muessen gezaehlt sein');
  assert.ok(e.hierarchyExtraEdges >= 1,
    `mindestens eine weitere Beziehung erwartet, waren ${e.hierarchyExtraEdges}`);
  assert.equal(e.hierarchyHeaderExplainsCounts, true,
    'der Kopf muss beide Zahlen nennen, sonst weiss niemand, was dazugekommen ist');
  assert.equal(e.hierarchyExtraEdgesFilterable, true,
    'die zusaetzlichen Kanten gehoeren an denselben Filter');
  assert.equal(e.hierarchyDeterminismUnchanged, true,
    'die Spaltenordnung der Projektion darf sich nicht geaendert haben');
  assert.equal(e.hierarchyColumnsFromCallsOnly, true,
    'die Struktur bleibt der Aufruf-Walk; die anderen Kanten ordnen nichts um');
});

test('AC5/AC6: proof run, readability and wiring', () => {
  const e = edges();
  assert.equal(e.overlapViolations, 0);
  assert.equal(e.clippingViolations, 0);
  assert.ok(e.port >= 4460, `Port >= 4460 erwartet, war ${e.port}`);
  assert.equal(e.leftoverProcesses, 0);
  for (const shot of ['galaxy-edges.png', 'hierarchy-edges.png', 'legend-filter.png']) {
    const p = join(ROOT, 'verification', 'w9', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w9/netdeny-w9.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w9/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w9'], 'Script smoke:w9 fehlt');
});

test('AC6: the ported galaxy files still say where they come from', () => {
  const head = read('src/galaxy/EdgeLines.tsx').slice(0, 4000);
  assert.ok(/MIT/.test(head), 'die MIT-Herkunft muss im Kopf stehen');
  assert.ok(/W9/.test(head),
    'die Aenderung dieses Zyklus gehoert in die Aenderungsliste des Kopfes');
});

test('AC7: one number carries one name', () => {
  // Orchestrator-Spezifikationskorrektur aus W7c: der dortige Abnahmetest
  // verlangte ein Feld `pass`, das tools/eval-check.mjs nicht schrieb, worauf
  // das Werkzeug seinen Wert unter zwei Namen fuehren musste. Innerhalb von W7c
  // war das nicht zu heilen, weil ein eingefrorener Test nicht noch einmal
  // eingefroren wird. Hier faellt der Alias weg, und die Zusicherung bleibt
  // Wort fuer Wort dieselbe.
  const raw = read('verification/w6/evalcheck.json');
  const e = JSON.parse(raw);
  assert.equal(e.evalCheckPass, true, 'die Eval-Kennzahlen der Sieger duerfen nicht fallen');
  assert.equal(Object.hasOwn(e, 'pass'), false,
    'das Artefakt darf denselben Wert nicht unter einem zweiten Namen tragen');
  const tool = read('tools/eval-check.mjs');
  assert.equal(/^\s*pass:\s*evalCheckPass/m.test(tool), false,
    'der Alias muss aus dem Werkzeug verschwunden sein');
  const w7c = read('tests/scaffold/w7c.test.mjs');
  assert.ok(/e\.evalCheckPass/.test(w7c),
    'der W7c-Abnahmetest muss den Namen lesen, den das Werkzeug wirklich schreibt');
  assert.equal(/e\.pass\b/.test(w7c), false, 'und den geratenen Namen nicht mehr');
});
