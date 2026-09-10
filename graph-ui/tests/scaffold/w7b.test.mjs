// W7b acceptance tests: the menu row looks and behaves like one row of
// buttons, the shortcuts are reliable and self-diagnosable, and the search
// answers before the server does. Run: node --test tests/scaffold/w7b.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: every menu entry is one and the same kind of button', () => {
  const s = JSON.parse(read('verification/w7/search.json'));
  assert.equal(s.menuUniform, true, 'alle Eintraege muessen dieselbe Gestalt haben');
  /*
   * Die Zahl ist eine Momentaufnahme und wandert mit dem Katalog, die
   * Zusicherung darunter nicht.
   *
   * W7b hat sie auf sechs festgeschrieben, weil es damals sechs waren. W8 hat
   * den siebten dazugestellt ("[r]eset layout"), weil ein Layout, das man
   * verstellen kann, einen Weg zurueck braucht, und der Contract dort verlangt
   * ihn ausdruecklich als Menuepunkt UND als Befehl. Damit standen zwei
   * eingefrorene Zusagen gegeneinander, und der Implementierer hat richtig
   * gehandelt: Contract erfuellt, diesen Test rot stehen lassen, gemeldet.
   *
   * Der Orchestrator korrigiert hier seine eigene Spezifikation, nicht das
   * Verhalten. Gemeint war nie "genau sechs", gemeint war "kein Eintrag ohne
   * Verdrahtung": jeder ist ein echter Knopf, per Maus und per Tastatur
   * bedienbar, mit sichtbarem Fokus, und die vier Zeilen darunter pruefen
   * genau das fuer jeden einzelnen. Diese Zeile zaehlt nur mit.
   */
  assert.equal(s.menuEntryCount, 7, 'sieben Eintraege erwartet (seit W8 mit reset layout)');
  assert.equal(s.menuAllButtons, true, 'jeder Eintrag muss ein echtes button-Element sein');
  assert.equal(s.menuClickWorks, true, 'jeder Eintrag muss per Maus bedienbar sein');
  assert.equal(s.menuKeyboardWorks, true, 'Tab plus Enter muss jeden Eintrag ausloesen');
  assert.equal(s.menuFocusRingVisible, true, 'der Fokus muss sichtbar sein');
});

test('AC2: shortcuts work and can diagnose themselves', () => {
  const s = JSON.parse(read('verification/w7/search.json'));
  for (const letter of ['a', 'w', 'b', 'c', 'l']) {
    assert.equal(s.altShortcutsWork[letter], true, `Alt+${letter} muss ausloesen`);
  }
  assert.equal(s.keyProbeShown, true, 'die Hilfe braucht den Tastentest');
  assert.equal(s.keyProbeReportsCodeAndAlt, true,
    'der Tastentest muss code, key, altKey und defaultPrevented zeigen');
  assert.equal(s.listenerInCapturePhase, true,
    'der Listener muss in der Capture-Phase haengen');
});

test('AC3: suggestions arrive before the server answers', () => {
  const s = JSON.parse(read('verification/w7/search.json'));
  assert.ok(s.firstSuggestionMedianMs <= 120,
    `erster Vorschlag im Median <= 120ms erwartet, war ${s.firstSuggestionMedianMs}`);
  assert.ok(Array.isArray(s.firstSuggestionSamples) && s.firstSuggestionSamples.length >= 10,
    'die Messreihe braucht >= 10 Eingaben');
  assert.ok(Number.isFinite(s.serverRoundtripMs), 'der Serverweg muss dokumentiert sein');
  assert.equal(s.staleAnswerWins, false, 'eine ueberholte Antwort darf nie gewinnen');
  assert.ok(s.prefixCacheHits >= 1, 'das Verlaengern eines Wortes darf keinen Roundtrip kosten');
  assert.ok(s.debounceMs <= 100, `Debounce <= 100ms erwartet, war ${s.debounceMs}`);
  assert.equal(s.localSuggestionsShownFirst, true,
    'die lokalen Sofort-Vorschlaege muessen vor der Serverantwort stehen');
});

test('AC4: the semantic mode was measured, and the verdict is recorded', () => {
  const s = JSON.parse(read('verification/w7/search.json'));
  assert.ok(['wired', 'measured-and-rejected'].includes(s.semanticMode),
    `semanticMode muss ein Urteil tragen, war ${s.semanticMode}`);
  assert.ok(Number.isFinite(s.semanticLatencyMs), 'die Latenz muss gemessen sein');
  assert.ok(Number.isInteger(s.semanticBridgesFound) && s.semanticBridgesTried >= 6,
    'mindestens 6 Vokabular-Bruecken muessen geprueft sein');
  if (s.semanticMode === 'wired') {
    assert.ok(s.semanticBridgesFound >= 3, 'dazugeschaltet nur bei >= 3 Treffern');
    assert.equal(s.semanticHitsLabelled, true,
      'semantische Treffer muessen als solche erkennbar sein');
  } else {
    assert.ok(typeof s.semanticRejectionReason === 'string'
      && s.semanticRejectionReason.length > 20,
      'eine Ablehnung braucht ihre Begruendung mit Zahlen');
  }
});

test('AC5/AC6: proof run and wiring', () => {
  const s = JSON.parse(read('verification/w7/search.json'));
  assert.equal(s.overlapViolations, 0);
  assert.equal(s.clippingViolations, 0);
  assert.ok(s.port >= 4380, `Port >= 4380 erwartet, war ${s.port}`);
  assert.equal(s.leftoverProcesses, 0);
  for (const shot of ['menu-uniform.png', 'search-fast.png']) {
    const p = join(ROOT, 'verification', 'w7', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w7/netdeny-w7b.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w7b/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w7b'], 'Script smoke:w7b fehlt');
});
