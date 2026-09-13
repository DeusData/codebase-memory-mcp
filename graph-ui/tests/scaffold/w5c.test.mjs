// W5c acceptance tests: the flow explainer is an overlay available at every
// depth, with a real dark-styled sequence diagram, symbol-grouped steps,
// honest absence sentences and the two honesty paragraphs.
// Run: node --test tests/scaffold/w5c.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1/AC2/AC3/AC4: the overlay explainer proves itself live', () => {
  const f = JSON.parse(read('verification/w5/flowfix.json'));
  assert.equal(f.overlayOpensAtDepth0, true, 'flow muss auf Tiefe 0 verfuegbar sein');
  assert.equal(f.overlayOpensAtDepth3, true, 'flow muss auf Tiefe 3 verfuegbar sein');
  assert.equal(f.overlayNotOccluded, true,
    'nichts darf das Overlay verdecken (elementFromPoint-Messung)');
  assert.ok(f.lifelines >= 3, `>= 3 Lebenslinien erwartet, war ${f.lifelines}`);
  assert.ok(f.arrowCount >= 4, `>= 4 Pfeile erwartet, war ${f.arrowCount}`);
  assert.equal(f.selfLoopShown, true, 'die may-raise-Selbstschleife fehlt');
  assert.ok(f.groupedSymbols >= 3, 'die Liste muss nach Symbolen gruppiert sein');
  assert.ok(f.absenceSentences >= 1, 'die ehrlichen Absenz-Saetze fehlen');
  assert.equal(f.stepperSyncsAll, true, 'Stepper muss Diagramm+Liste+Editor bewegen');
  assert.equal(f.escCloses, true);
  assert.equal(f.darkStyled, true, 'dunkles Token-Design, kein Referenz-Weiss');
  assert.equal(f.honestyParagraphs, 2, 'die zwei Ehrlichkeits-Absaetze fehlen');
  assert.ok(f.port >= 4360, `Port >= 4360 erwartet, war ${f.port}`);
  assert.equal(f.leftoverProcesses, 0);
  for (const [shot, kb] of [['flow-overlay.png', 50], ['flow-overlay-depth0.png', 30]]) {
    const p = join(ROOT, 'verification', 'w5', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > kb * 1024, `${shot} verdaechtig klein`);
  }
});

test('AC7: every file shows its coverage status, including the good case', () => {
  const f = JSON.parse(read('verification/w5/flowfix.json'));
  assert.ok(f.indexedDotsVisible >= 10,
    `auch indexierte Dateien brauchen ihren Status-Punkt, war ${f.indexedDotsVisible}`);
  assert.equal(f.statusTooltipShown, true, 'der Status-Tooltip muss existieren');
  assert.equal(f.legendExplainsGoodCase, true,
    'die Legende muss auch den Gutfall-Punkt erklaeren');
});

test('AC8: the graph panel is permanently visible', () => {
  const f = JSON.parse(read('verification/w5/flowfix.json'));
  assert.equal(f.graphPanelVisibleDuringTour, true,
    'die Galaxy muss waehrend der Tour im Viewport stehen und folgen');
  assert.equal(f.hierarchyVisibleDuringWalk, true,
    'die Hierarchie-Ansicht muss im Entry-Walk im Viewport stehen');
  assert.equal(f.panelNeverScrolledAway, true,
    'Twin-Ueberlaenge scrollt intern, nie das Graph-Panel weg');
  assert.ok(f.graphPanelMinHeight >= 280,
    `Mindesthoehe >= 280px erwartet, war ${f.graphPanelMinHeight}`);
});

test('AC9: the hierarchy is readable, no label overlaps, graph keeps its room', () => {
  const f = JSON.parse(read('verification/w5/flowfix.json'));
  assert.equal(f.hierarchyLabelOverlapsSmallWalk, 0,
    'beim 4-Symbol-Walk darf sich kein Label ueberlagern');
  assert.equal(f.hierarchyLabelOverlapsLargeWalk, 0,
    'beim 8-Symbol-Walk darf sich kein Label ueberlagern');
  assert.equal(f.hierarchyBloomReduced, true,
    'Bloom ist im hierarchy-Modus reduziert oder aus');
  assert.ok(f.legendMaxShare <= 0.4,
    `die offene Legende darf hoechstens 40% des Panels nehmen, war ${f.legendMaxShare}`);
  assert.ok(f.graphMinShare >= 0.6,
    `der Graph behaelt mindestens 60% des Panels, war ${f.graphMinShare}`);
  assert.equal(f.legendDefaultCollapsed, true,
    'die Panel-Legende ist im schmalen Panel default zu');
});

test('AC10: the tab row scrolls instead of colliding', () => {
  const f = JSON.parse(read('verification/w5/flowfix.json'));
  assert.equal(f.tabsOverflowScrolls, true,
    'bei vielen offenen Dateien muss die Tab-Leiste horizontal scrollen');
  assert.equal(f.tabsNoWrap, true, 'die Tab-Leiste darf nie umbrechen');
  assert.equal(f.tabsNoOverlap, true,
    'die Tab-Leiste darf Explorer- und Twin-Kopf nie ueberlappen');
  assert.equal(f.activeTabInView, true,
    'der aktive Tab wird beim Oeffnen in Sicht gescrollt');
  assert.ok(f.tabsOpenedForProof >= 10, 'der Beweis braucht >= 10 offene Tabs');
});

test('AC5: the w4c assurances stay reproducible with the overlay', () => {
  const w4c = JSON.parse(read('verification/w4/flow.json'));
  assert.equal(w4c.flowHeadClickable, true);
  assert.equal(w4c.flowDarkStyled, true);
  assert.equal(w4c.stepperMovesEditor, true);
  assert.ok(Number.isFinite(w4c.regeneratedAtMs) || typeof w4c.generatedAt === 'string',
    'flow.json muss vom ehrlichen Re-Lauf stammen');
});

test('AC6: wiring and net deny', () => {
  const nd = JSON.parse(read('verification/w5/netdeny-w5c.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w5c/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w5c'], 'Script smoke:w5c fehlt');
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
