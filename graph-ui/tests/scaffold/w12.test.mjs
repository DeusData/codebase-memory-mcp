// W12 acceptance tests: the run finds for itself what can be operated, touches
// every single one twice, proves that each filter really removes and restores,
// and keeps going until two rounds in a row find nothing new.
// Run: node --test tests/scaffold/w12.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync, readdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const buttons = () => JSON.parse(read('verification/w12/buttons.json'));

test('AC1: the run collects the controls itself, in every state', () => {
  const b = buttons();
  const controls = b.extras?.controls;
  assert.ok(b.statesVisited >= 10,
    `mindestens 10 Zustaende erwartet, waren ${b.statesVisited}`);
  assert.ok(b.uniqueControls >= 60,
    `mindestens 60 eindeutige Bedienelemente erwartet, waren ${b.uniqueControls}`);
  assert.ok(b.controlsByState && typeof b.controlsByState === 'object',
    'die Zahl je Zustand gehoert ins Artefakt, sonst ist die Summe nicht nachvollziehbar');
  assert.ok(Array.isArray(controls), 'die Einzelmessungen fehlen unter extras.controls');
  assert.equal(controls.length, b.uniqueControls,
    'uniqueControls muss aus den tatsaechlichen Einzelmessungen folgen');
  assert.equal(new Set(controls.map((control) => control.key)).size, controls.length,
    'jede Einzelmessung braucht einen eindeutigen, selbst gefundenen Schluessel');
  assert.ok(Object.values(b.controlsByState).every(Number.isInteger),
    'jede Zustandszahl muss eine echte Messzahl sein');
});

test('AC1b: named states prove the surface they claim to measure', () => {
  const store = buttons().extras?.store;
  const pseudocode = store?.states?.['twin-pseudocode'];
  assert.equal(pseudocode?.complete, true, 'der Pseudocode-Zustand muss abgeschlossen sein');
  assert.equal(pseudocode?.coverage?.file, 'src/services/userService.ts',
    'der Pseudocode-Beleg muss die tatsaechlich offene Datei nennen');
  assert.equal(pseudocode?.coverage?.symbol, 'createUser',
    'der Pseudocode-Beleg muss das tatsaechlich aufgeloeste Symbol nennen');
  assert.equal(pseudocode?.coverage?.twinStatus, 'ready',
    'der Twin muss im gemessenen Pseudocode-Zustand fertig sein');
  assert.equal(pseudocode?.coverage?.twinView, 'pseudocode',
    'der gemessene Twin muss wirklich auf pseudocode stehen');
  assert.ok(pseudocode?.coverage?.pseudocodeLines >= 1,
    'mindestens eine echte Pseudocode-Zeile muss im Zustand sichtbar sein');

  const search = store?.states?.search;
  assert.equal(search?.complete, true, 'der Suchzustand muss abgeschlossen sein');
  assert.ok(search?.coverage?.searchRows >= 1,
    'der Suchzustand muss mindestens eine tatsaechliche Trefferzeile belegen');

});

test('AC1c: the entry walk keeps the already ready twin', () => {
  const walk = buttons().extras?.store?.states?.['walk-running'];
  assert.equal(walk?.complete, true,
    'der Entry-Walk muss ohne sachfremden Folgegriff vollstaendig werden');
  assert.equal(walk?.coverage?.file, 'src/services/userService.ts',
    'der Walk muss die tatsaechlich offene Datei belegen');
  assert.equal(walk?.coverage?.symbol, 'createUser',
    'der Walk muss am gewaehlten createUser bleiben');
  assert.equal(walk?.coverage?.twinStatus, 'ready',
    'ein Walk am bereits gelesenen Symbol darf den fertigen Twin nicht in loading stehen lassen');
  assert.equal(walk?.coverage?.walkIndex, 0,
    'der gemessene Walk muss auf seinem ersten Schritt stehen');
  assert.equal(walk?.coverage?.walkRootName, 'createUser',
    'die gemessene Tour braucht createUser als echte Wurzel');
  assert.equal(walk?.coverage?.walkRootPath, 'src/services/userService.ts',
    'die gemessene Tour braucht den echten Wurzelpfad');
  assert.deepEqual(walk?.findings, [],
    'der fertige Walk-Zustand darf keinen vorherigen Ladebefund verstecken');
});

test('AC2: every control is touched twice, with mouse and with keyboard', () => {
  const b = buttons();
  const controls = b.extras.controls;
  assert.equal(b.controlsClicked, b.uniqueControls,
    'jedes gefundene Element muss mit der Maus angefasst worden sein');
  assert.equal(b.controlsByKeyboard, b.uniqueControls,
    'und jedes mit der Tastatur: ein Element nur fuer die Maus ist ein halbes');
  assert.equal(b.focusVisibleAll, true,
    'der Fokus muss bei jedem bedienbaren Element sichtbar sein');
  assert.equal(controls.filter((control) => control.mouse?.done === true).length,
    b.controlsClicked, 'controlsClicked muss aus den Einzelmessungen folgen');
  assert.equal(controls.filter((control) => control.keyboard?.done === true).length,
    b.controlsByKeyboard, 'controlsByKeyboard muss aus den Einzelmessungen folgen');
  for (const control of controls) {
    assert.equal(control.mouse?.done, true, `${control.label}: Mausmessung fehlt`);
    assert.equal(control.keyboard?.done, true, `${control.label}: Tastaturmessung fehlt`);
    const nativeDisabled = control.excuse?.marked === true
      && control.keyboard?.focusable === false
      && /gesperrt/.test(control.keyboard?.via ?? '');
    if (nativeDisabled) {
      assert.equal(control.focusVisible, false,
        `${control.label}: ein natives disabled darf keinen Fokus vortaeuschen`);
      assert.ok(typeof control.noEffect?.reason === 'string'
        && control.noEffect.reason.length > 10,
      `${control.label}: das gesperrte Element braucht seinen sichtbaren Grund`);
    } else {
      assert.equal(control.focusVisible, true, `${control.label}: Fokus ist nicht sichtbar`);
    }
  }
});

test('AC3: what does nothing is a finding, not a result', () => {
  const b = buttons();
  assert.ok(Array.isArray(b.didNothing), 'die Liste muss es geben, auch wenn sie leer ist');
  for (const entry of b.didNothing) {
    assert.ok(typeof entry.reason === 'string' && entry.reason.length > 10,
      `${entry.label ?? entry.selector}: ein Element ohne Wirkung braucht seine Begruendung`);
  }
  const measuredWithoutEffect = b.extras.controls.filter(
    (control) => control.mouse?.changed !== true && control.keyboard?.changed !== true,
  );
  assert.equal(b.didNothing.length, measuredWithoutEffect.length,
    'didNothing muss aus den wirkungslosen Einzelmessungen folgen');
  for (const control of measuredWithoutEffect) {
    assert.ok(typeof control.noEffect?.reason === 'string' && control.noEffect.reason.length > 10,
      `${control.label}: die Begruendung muss aus der gemessenen Oberflaeche kommen`);
  }
});

test('AC3b: a filter must filter, and prove it', () => {
  const b = buttons();
  assert.ok(b.filtersMeasured >= 10,
    `mindestens 10 Filter erwartet (7 Facetten, facts/pseudocode, Kantenarten, Akteure), waren ${b.filtersMeasured}`);
  assert.equal(b.everyFilterRemovesAndRestores, true,
    'Abschalten muss messbar etwas wegnehmen, Einschalten es zurueckbringen');
  assert.equal(b.emptyFilterExplainsItself, true,
    'bewirkt ein Schalter nichts, weil es nichts zu zeigen gibt, muss die Flaeche das sagen');
  assert.ok(Array.isArray(b.filterCounts) && b.filterCounts.length >= 10,
    'je Filter gehoeren die Zahlen vorher und nachher ins Artefakt');
  for (const f of b.filterCounts) {
    assert.ok(Number.isFinite(f.before) && Number.isFinite(f.after) && Number.isFinite(f.again),
      `${f.name}: vorher, nachher und zurueck muessen gemessen sein`);
    const noise = Number.isFinite(f.noise) ? f.noise : 0;
    if (f.before > 0) {
      assert.ok(f.after < f.before - noise,
        `${f.name}: Abschalten hat nichts messbar weggenommen`);
    } else {
      assert.ok(f.emptyCase === true && typeof f.sentence === 'string' && f.sentence.length > 10,
        `${f.name}: der leere Fall muss sich in der Flaeche erklaeren`);
    }
    assert.ok(Math.abs(f.again - f.before) <= noise,
      `${f.name}: Einschalten hat den Vorherwert nicht zurueckgebracht`);
  }
});

test('AC4/AC5: nothing breaks, and the keyboard reaches everywhere', () => {
  const b = buttons();
  assert.equal(b.consoleErrors, 0);
  assert.equal(b.uncaughtExceptions, 0);
  assert.equal(b.overlapViolations, 0);
  assert.equal(b.clippingViolations, 0);
  assert.equal(b.cutWithoutHint, 0);
  assert.equal(b.keyboardTraps, 0,
    'eine Falle, aus der Tab nicht herausfuehrt, ist ein Fehlschlag');
  assert.equal(b.tabOrderFollowsLayout, true,
    'die Tab-Reihenfolge muss der sichtbaren Ordnung folgen');
  assert.ok(Array.isArray(b.extras?.tabWalks) && b.extras.tabWalks.length >= b.statesVisited,
    'die Tastaturwanderung muss je abgeschlossenem Zustand belegt sein');
  for (const walk of b.extras.tabWalks) {
    assert.notEqual(walk.trap, true, `${walk.state}: Tastaturfalle`);
    assert.equal(walk.inDocumentOrder, true, `${walk.state}: Tab-Reihenfolge weicht vom Layout ab`);
  }
});

test('AC4b: fullscreen removes the covered background from the tab route', () => {
  const fullscreen = buttons().extras?.store?.states?.['agents-fullscreen'];
  const routes = Object.keys(fullscreen?.focusVisible ?? {});
  const outside = routes.filter((route) =>
    !/atlas-(?:agents|galaxy|graph-mode)/.test(route));
  assert.deepEqual(outside, [],
    `im Vollbild liegen verdeckte Hintergrundgriffe im Tabpfad: ${outside.join(', ')}`);
  assert.equal(fullscreen?.complete, true,
    'der isolierte Vollbildzustand muss vollstaendig gemessen sein');
  assert.deepEqual(fullscreen?.findings, [],
    'der Vollbildzustand darf keinen verdeckten Hintergrundgriff verstecken');
});

test('AC6: a human can read the result', () => {
  const p = join(ROOT, 'verification', 'w12', 'buttons.md');
  assert.ok(existsSync(p), 'die Uebersicht buttons.md fehlt');
  const text = readFileSync(p, 'utf8');
  assert.ok(text.length > 2000, 'die Uebersicht ist verdaechtig kurz');
  assert.ok(/Maus|mouse/i.test(text) && /Tastatur|keyboard/i.test(text),
    'je Element muessen Maus und Tastatur dastehen');
  const controlRows = text.split('\n').filter((line) => /^\| .* \| .* \| .* \| .* \|$/.test(line));
  assert.ok(controlRows.length >= buttons().uniqueControls,
    'die Uebersicht braucht mindestens eine lesbare Zeile je Einzelmessung');
});

test('AC8a: no state finding is hidden behind a clean stage header', () => {
  const b = buttons();
  const store = b.extras?.store;
  assert.ok(store && store.states && Array.isArray(store.stages),
    'die Rohzustaende und Etappen muessen fuer die trockene Schleife nachpruefbar sein');
  const rawFindings = Object.values(store.states).flatMap((state) =>
    (state.findings ?? []).map((finding) => ({ state: state.id, ...finding })));
  assert.deepEqual(rawFindings, [],
    'kein Zustandsbefund darf bis zum Rundenende im Rohspeicher versteckt bleiben');
});

test('AC8b: a stage resumes instead of repeating completed work', () => {
  const store = buttons().extras?.store;
  assert.ok(store && Array.isArray(store.stages), 'die Roh-Etappen fehlen');
  const completedByPass = new Map();
  for (const stage of store.stages) {
    const completed = completedByPass.get(stage.pass) ?? new Set();
    for (const state of stage.states ?? []) {
      assert.equal(completed.has(state.id), false,
        `Durchgang ${stage.pass}, Etappe ${stage.n}: fertiger Zustand ${state.id} wurde wiederholt`);
      if (state.complete === true) completed.add(state.id);
    }
    completedByPass.set(stage.pass, completed);
  }
});

test('AC8: the loop ran until it was dry', () => {
  const b = buttons();
  assert.ok(Array.isArray(b.rounds) && b.rounds.length >= 2,
    'mindestens zwei Runden, sonst kann nichts trocken gelaufen sein');
  const last = b.rounds.slice(-2);
  for (const round of last) {
    assert.equal(round.complete, true, `Runde ${round.n}: nur ein vollstaendiger Durchgang zaehlt`);
    assert.equal(round.newFindings, 0,
      `Runde ${round.n}: die letzten zwei Runden muessen null neue Befunde haben`);
    assert.deepEqual(round.findings, [], `Runde ${round.n}: null muss zur Befundliste passen`);
  }
  for (const round of b.rounds) {
    assert.ok(Number.isInteger(round.newFindings),
      'jede Runde muss ihre Zahl tragen, damit sichtbar bleibt, wie lange es gedauert hat');
  }
});

test('AC7/AC9: proof run and wiring', () => {
  const b = buttons();
  assert.ok(b.port >= 4580, `Port >= 4580 erwartet, war ${b.port}`);
  assert.equal(b.leftoverProcesses, 0);
  const shots = readdirSync(join(ROOT, 'verification', 'w12', 'states'));
  assert.ok(shots.length >= 10, `mindestens 10 Zustandsbilder erwartet, waren ${shots.length}`);
  for (const shot of shots) {
    assert.ok(statSync(join(ROOT, 'verification', 'w12', 'states', shot)).size > 20 * 1024,
      `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w12/netdeny.json'));
  assert.equal(nd.exitCode, 0, 'der Netz-Deny-Lauf muss selbst erfolgreich beendet sein');
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w12/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w12'], 'Script smoke:w12 fehlt');
  assert.match(pkg.scripts['smoke:w12'], /net-deny-gate\.mjs.*smoke-w12\.mjs/,
    'smoke:w12 muss den Lauf wirklich durch das Netz-Deny-Gate fuehren');
});
