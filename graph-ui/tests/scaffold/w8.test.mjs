// W8 acceptance tests: every surface has a place the reader expects, what
// holds at once sits side by side, what replaces each other shares one place
// behind tabs, and the reader drags the borders.
// Run: node --test tests/scaffold/w8.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: zones instead of a stack', () => {
  const l = JSON.parse(read('verification/w8/layout.json'));
  assert.equal(l.zonesNeverOverlap, true, 'keine zwei Zonen duerfen sich ueberlagern');
  assert.equal(l.singleExplainTabVisible, true,
    'im Erklaeren-Bereich darf immer nur ein Reiter sichtbar sein');
  assert.ok(Array.isArray(l.explainTabs) && l.explainTabs.length >= 4,
    'flow, walk, chat und die Wizard-Flaechen muessen Reiter sein');
  assert.equal(l.disabledTabsExplainThemselves, true,
    'ein Reiter ohne Inhalt sagt warum, statt zu verschwinden');
});

test('AC2/AC3: the reader drags the borders, and finds the way back', () => {
  const l = JSON.parse(read('verification/w8/layout.json'));
  assert.equal(l.allFourSplittersDrag, true, 'alle vier Griffe muessen ziehbar sein');
  assert.equal(l.splittersKeyboard, true, 'jeder Griff muss per Tastatur gehen');
  assert.equal(l.splitterDoubleClickResets, true, 'Doppelklick setzt eine Grenze zurueck');
  assert.equal(l.layoutPersistsReload, true, 'die Masse muessen den Reload ueberleben');
  assert.equal(l.resetLayoutWorks, true, 'ein Befehl setzt das ganze Layout zurueck');
  assert.equal(l.minZoneRespected, true, 'keine Zone darf wegziehbar sein');
});

test('AC4/AC5: quiet by default, and nothing is lost on the way', () => {
  const l = JSON.parse(read('verification/w8/layout.json'));
  assert.equal(l.explainCollapsedOnOpen, true,
    'beim Oeffnen ist der Erklaeren-Bereich eingeklappt');
  assert.equal(l.explainOpensOnDemand, true,
    'er oeffnet sich auf den passenden Reiter, wenn der Leser etwas erklaert haben will');
  assert.equal(l.tabSwitchKeepsState, true,
    'Reiterwechsel darf keinen Zustand kosten');
  assert.ok(l.stateProbes && l.stateProbes.chatLines >= 1
    && Number.isInteger(l.stateProbes.walkStep) && Number.isInteger(l.stateProbes.flowStep),
    'Chat-Verlauf, Walk-Schritt und Flow-Stelle muessen einzeln belegt sein');
});

test('AC6/AC7: readable in every zone size', () => {
  const l = JSON.parse(read('verification/w8/layout.json'));
  assert.equal(l.overlapViolations, 0);
  assert.equal(l.clippingViolations, 0);
  // Die dritte Regel aus W9: kein Kasten am Anfang seines Bildlaufs darf eine
  // Textzeile an der Kante kappen, ohne zu zeigen, dass mehr dahinter steht.
  // Ein Umbau, der Flaechen kleiner macht, ist genau der Anlass dafuer.
  assert.equal(l.cutWithoutHint, 0, 'kein angeschnittener Satz ohne sichtbaren Hinweis');
  assert.ok(Array.isArray(l.extremeLayouts) && l.extremeLayouts.length >= 2,
    'die Extremlagen muessen gemessen sein');
  assert.ok(l.extremeLayouts.every((e) => e.overlapViolations === 0
    && e.clippingViolations === 0 && e.cutWithoutHint === 0),
    'auch in den Extremlagen darf nichts kollidieren oder angeschnitten sein');
  assert.ok(l.port >= 4440, `Port >= 4440 erwartet, war ${l.port}`);
  assert.equal(l.leftoverProcesses, 0);
  for (const shot of ['layout-default.png', 'layout-explain-large.png', 'layout-custom.png']) {
    const p = join(ROOT, 'verification', 'w8', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
});

test('AC8: wiring and net deny', () => {
  const nd = JSON.parse(read('verification/w8/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w8/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w8'], 'Script smoke:w8 fehlt');
  const a = JSON.parse(read('verification/w6/airgap.json'));
  assert.equal(a.overlapViolations, 0, 'der Gesamtlauf muss mit den Zonen gruen bleiben');
  assert.equal(a.clippingViolations, 0);
});

test('AC5b: what W7c won for the chat survives becoming a tab', () => {
  // Der Chat hat in W7c eine ziehbare Hoehe, einen stehenden Kopf, Escape zum
  // Einklappen und die Trennung von "zu" und "geloescht" bekommen, alles auf
  // Nutzerbefunde hin. Als Reiter erbt er die Hoehe der Zone; die anderen drei
  // Zusagen sind seine eigenen und duerfen beim Umbau nicht still verfallen.
  const l = JSON.parse(read('verification/w8/layout.json'));
  assert.equal(l.chatTabKeepsEscape, true,
    'Escape muss den Erklaeren-Bereich weiter einklappen, ohne den Verlauf zu kosten');
  assert.equal(l.chatTabHistorySurvives, true,
    'der Verlauf ueberlebt das Einklappen und den Reiterwechsel');
  assert.equal(l.chatTabClearStillClears, true,
    'clear bleibt der einzige Weg, der loescht');
  assert.equal(l.chatHeightFollowsZone, true,
    'die Hoehe kommt jetzt von der Zone, und der Griff der Zone tut, was der Griff des Chats tat');
});
