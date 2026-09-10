// W10b acceptance tests: no leaflet on the handles, the view switch also folds,
// the hierarchy grows from the symbol in focus, and the preview flow is written
// down. Run: node --test tests/scaffold/w10b.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const fixes = () => JSON.parse(read('verification/w10b/fixes.json'));

test('AC1: the handles lost their leaflet, not their ability', () => {
  const f = fixes();
  assert.equal(f.splitterTooltips, 0,
    'an einem Griff mit sichtbarer Marke erklaert ein Kasten nur, was man sieht');
  assert.equal(f.splitterKeyboardStillWorks, true,
    'entfernt wird die Beschreibung, nicht die Faehigkeit');
  assert.equal(f.splitterAriaKept, true,
    'ein Vorleseprogramm muss weiter erfahren, was es anfasst und wo es steht');
});

test('AC2: the view switch also folds and unfolds', () => {
  const f = fixes();
  assert.equal(f.toggleCollapsesActive, true,
    'Klick auf den aktiven Knopf klappt die Sektion zu');
  assert.equal(f.toggleOpensFromCollapsed, true,
    'Klick bei zugeklappter Sektion klappt sie auf und waehlt die Ansicht');
  assert.equal(f.toggleSwitchesView, true, 'der andere Knopf wechselt wie bisher');
  assert.equal(f.toggleAgreesWithLabelledButton, true,
    'beide Wege muessen denselben Zustand ergeben, sonst sind es zwei Wahrheiten');
});

test('AC3: the hierarchy grows from the symbol in focus', () => {
  const f = fixes();
  assert.equal(f.hierarchyFromFocus, true,
    'ein Symbol im Fokus genuegt, ein Einstiegs-Spaziergang ist nicht noetig');
  assert.equal(f.hierarchyHeadNamesRoot, true,
    'der Kopf nennt, woher die Wurzel kommt');
  assert.equal(f.hierarchyDisabledExplains, true,
    'ohne Fokus und ohne Walk bleibt der Knopf grau UND sagt warum');
  assert.equal(f.hierarchyDeterminismUnchanged, true,
    'die Spaltenordnung der Projektion darf sich nicht geaendert haben');
});

test('AC4: the preview flow is written down, not remembered', () => {
  const f = fixes();
  assert.equal(f.previewFlowDocumented, true, 'der Ablauf muss im Repo stehen');
  const readme = read('README.md');
  assert.ok(/dist/.test(readme) && /(neu ?starten|restart)/i.test(readme),
    'die Anleitung muss sagen, dass der Vorschau-Server nach jedem Bau neu startet');
  assert.ok(/index-|Bundle|bundle/.test(readme),
    'und wie man prueft, was wirklich ausgeliefert wird');
});

test('AC5/AC6: proof run and wiring', () => {
  const f = fixes();
  assert.equal(f.overlapViolations, 0);
  assert.equal(f.clippingViolations, 0);
  assert.equal(f.cutWithoutHint, 0);
  assert.ok(f.port >= 4640, `Port >= 4640 erwartet, war ${f.port}`);
  assert.equal(f.leftoverProcesses, 0);
  for (const shot of ['handles.png', 'hierarchy-from-focus.png', 'graph-collapsed.png']) {
    const p = join(ROOT, 'verification', 'w10b', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w10b/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w10b/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w10b'], 'Script smoke:w10b fehlt');
});
