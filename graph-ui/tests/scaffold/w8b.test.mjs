// W8b acceptance tests: nothing lays itself over the reader unasked, a switch
// says what it does, a handle looks like a handle, and the command line shows
// how it is used. Run: node --test tests/scaffold/w8b.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const ux = () => JSON.parse(read('verification/w8b/ux.json'));

test('AC1/AC2: no native tooltip explains anything, and ours are measured', () => {
  const u = ux();
  assert.equal(u.nativeTitlesWithExplanation, 0,
    'ein nativer Tooltip liegt ausserhalb des DOM und ist damit unmessbar');
  assert.ok(u.domTooltipsMeasured >= 10,
    `mindestens 10 eigene Tooltips muessen einzeln gemessen sein, waren ${u.domTooltipsMeasured}`);
  assert.equal(u.tooltipCoversNothing, true,
    'kein Tooltip darf verdecken, was der Leser gerade braucht');
  assert.equal(u.tooltipOpensByKeyboard, true, 'ein Tooltip nur fuer die Maus ist keiner');
  assert.equal(u.tooltipClosesByEscape, true, 'Escape muss ihn schliessen');
});

test('AC3/AC4: switches say what they do and what they are', () => {
  const u = ux();
  assert.equal(u.collapseLabelsAreWords, true, 'Wort statt Zeichen an jedem Ein- und Ausklapper');
  assert.equal(u.collapseLabelFollowsView, true,
    'der Name folgt dem, was gerade zu sehen ist (galaxy oder hierarchy)');
  assert.equal(u.viewToggleShowsState, true,
    'galaxy und hierarchy muessen erkennbar ein Paar sein, aus dem eines aktiv ist');
});

test('AC5: the twin never ends mid-line behind the next header', () => {
  const u = ux();
  assert.equal(u.twinCutHasHint, true, 'wo der Twin endet, muss ein Hinweis auf mehr stehen');
  assert.equal(u.twinAtHundredPercentReadable, true,
    'genau die Lage aus dem Nutzerbild: Twin voll, Graph offen, 100 Prozent, kein Ziehen');
});

test('AC6: the honesty block is shorter and more concrete', () => {
  const u = ux();
  assert.ok(u.honestyBlockChars <= 400,
    `hoechstens 400 Zeichen erwartet (heute 954), waren ${u.honestyBlockChars}`);
  assert.ok(u.honestyBlockParagraphs <= 2,
    `hoechstens 2 Absaetze erwartet, waren ${u.honestyBlockParagraphs}`);
  assert.equal(u.walkBoundOnDiagram, true,
    'die Grenze des Walks gehoert an das Bild, nicht in einen Absatz darunter');
  assert.ok(typeof u.unresolvedCallsReported === 'boolean',
    'die Messung, ob der Index unaufgeloeste Aufrufe meldet, muss ein Ergebnis haben');
  assert.equal(u.stepNoteKept, true,
    'der schrittbezogene Satz bleibt: er loest eine Verwirrung an genau ihrer Stelle');
});

test('AC6b: a setting that only applies later offers the way', () => {
  const u = ux();
  assert.equal(u.depthChangeOffersRerun, true,
    'wird die Tiefe geaendert, muss die letzte Antwort ein Angebot tragen');
  assert.equal(u.rerunMakesNewTurn, true, 'das Angebot erzeugt einen NEUEN Zug');
  assert.equal(u.oldTurnKeepsItsDepth, true,
    'der alte Zug bleibt mit seiner alten Tiefe stehen, sonst luegen seine Zitate');
});

test('AC6c/AC6d: the line teaches itself, and a handle looks like one', () => {
  const u = ux();
  assert.equal(u.placeholderShowsExample, true, 'der Platzhalter nennt ein echtes Beispiel');
  assert.ok(u.examplesClickable >= 3, `mindestens 3 anklickbare Beispiele, waren ${u.examplesClickable}`);
  assert.equal(u.examplesUseRealSymbols, true,
    'die Namen darin kommen aus dem geladenen Index, nicht aus einer festen Liste');
  assert.equal(u.handleVisibleWithoutHover, true,
    'ein Griff, den man nur bei Hover sieht, findet niemand');
  assert.ok(u.handleHitAreaPx >= 10, `Trefferflaeche >= 10px erwartet, war ${u.handleHitAreaPx}`);
  assert.equal(u.allZoneHandlesLookAlike, true, 'alle Griffe des Layouts tragen dieselbe Gestalt');
});

test('AC6e/AC6f: the head stays, and opening a file shows the file', () => {
  const u = ux();
  assert.equal(u.chatHeadVisibleAt100, true, 'Kontext-Chips und clear bleiben sichtbar');
  assert.equal(u.chatHeadVisibleAt67, true, 'auch bei 67 Prozent Zoom');
  assert.equal(u.whyClosesOnOpenFile, true,
    'wer eine Datei oeffnet, hat die Frage beantwortet, indem er anfing zu lesen');
  assert.equal(u.whyClosesByEscape, true, 'Escape schliesst sie ebenfalls');
  assert.equal(u.fileOpensBehindNothing, true,
    'nach dem Klick im Explorer steht der Code im Reader, nicht die Frage davor');
});

test('AC7/AC8: proof run and wiring', () => {
  const u = ux();
  assert.equal(u.overlapViolations, 0);
  assert.equal(u.clippingViolations, 0);
  assert.equal(u.cutWithoutHint, 0);
  assert.ok(u.port >= 4540, `Port >= 4540 erwartet, war ${u.port}`);
  assert.equal(u.leftoverProcesses, 0);
  for (const shot of ['tooltip-open.png', 'collapse-words.png', 'twin-full.png', 'flow-short.png']) {
    const p = join(ROOT, 'verification', 'w8b', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w8b/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w8b/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w8b'], 'Script smoke:w8b fehlt');
});
