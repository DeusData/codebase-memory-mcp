// W10 acceptance tests: the model belongs to the reader, not to the program,
// and everything that costs computing time lives in one place and names its
// measured effect. Run: node --test tests/scaffold/w10.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const models = () => JSON.parse(read('verification/w10/models.json'));

test('AC1: there is a settings panel, and it shows the running model', () => {
  const m = models();
  assert.equal(m.settingsPanelOpens, true, 'aus der Menuezeile und ueber die Kommandozeile');
  assert.equal(m.runningModelFromProps, true,
    'Name, Quantisierung, Kontext und Gewichte kommen aus dem laufenden Prozess, nicht aus einer Tabelle');
});

test('AC2: switching models works without a restart, or says why not', () => {
  const m = models();
  assert.ok(m.cacheModelsListed >= 2,
    `der Beweislauf braucht zwei Modelle im Cache, hatte ${m.cacheModelsListed}`);
  assert.equal(m.switchModelWorks, true,
    'zwei Anfragen, zwei verschiedene Modellnamen in den Antworten belegt');
  assert.equal(m.switchPersistsReload, true, 'die Wahl ueberlebt den Reload');
  assert.equal(m.statusBarNamesModel, true, 'die Statusleiste nennt das gewaehlte Modell');
  assert.equal(m.noRouterExplained, true,
    'laeuft der Sidecar ohne Router, sagt das Panel das, statt eine wirkungslose Auswahl zu zeigen');
});

test('AC3: fetching a model is offered honestly, not faked', () => {
  const m = models();
  assert.ok(m.suggestionsListed >= 6,
    `die sechs Kandidaten aus der ADR erwartet, waren ${m.suggestionsListed}`);
  assert.equal(m.suggestionsShowMeasuredNumbers, true,
    'jeder Vorschlag traegt seine gemessenen Zahlen aus der Eval, nicht geschaetzte');
  assert.equal(m.freeRepoFieldAccepted, true, 'ein beliebiges Hugging-Face-Repo geht auch');
  assert.equal(m.commandCopyable, true, 'der fertige Befehl ist zu kopieren');
  assert.equal(m.downloadHonestyText, true,
    'darueber steht, dass dieser Befehl ins Netz geht, wohin er laedt, und dass die Anwendung selbst nichts herunterlaedt');
  assert.equal(m.noFakeProgressBar, true,
    'kein Fortschrittsbalken fuer etwas, das die Oberflaeche nicht misst');
});

test('AC4: the program ships no model any more', () => {
  const m = models();
  assert.equal(m.startScriptTakesModel, true,
    'llm/start.sh nimmt das Modell als Parameter oder aus der Umgebung');
  assert.equal(m.startScriptSaysHowToFetch, true,
    'ohne Modell im Cache sagt es, wie man eines holt, statt stumm ins Leere zu starten');
  assert.equal(m.noWeightsInRepo, true, 'models/ enthaelt keine Gewichte und keinen Pfad darauf');
  const sums = read('models/SHA256SUMS');
  assert.ok(sums.length > 0, 'die Pruefsummen der Eval-Laeufe bleiben');
  assert.ok(/eval|Eval|Beleg|belegt/.test(sums),
    'eine Zeile muss erklaeren, wofuer diese Summen noch stehen');
});

test('AC5: off is still off', () => {
  const m = models();
  assert.equal(m.llmOffMakesNoRequests, true,
    'bei ausgeschaltetem Modell geht kein fetch an den Sidecar, auch nicht aus dem neuen Panel');
  assert.equal(m.panelExplainsItselfWhileOff, true,
    'das Panel ist sichtbar und erklaert sich, fragt aber nichts ab');
});

test('AC8: a truncated one-line answer is not counted as perfect', () => {
  const m = models();
  assert.equal(m.singleLineTruncatedIsUnmeasured, true,
    'bleibt nach dem Weglassen der letzten Zeile nichts uebrig, ist die Pruefung nicht gemessen');
  assert.equal(m.unmeasuredOutOfCitationRate, true,
    'eine nicht gemessene Antwort faellt aus der Zitattreue heraus, statt sie zu schoenen');
  assert.equal(m.evalReportsUnmeasured, true, 'die Eval weist die Zahl der nicht gemessenen Antworten aus');
  assert.equal(m.panelShowsUnmeasured, true,
    'das Panel zeigt sie neben der Zitattreue, damit keine Empfehlung auf einer geschrumpften Stichprobe steht');
});

test('AC9: everything that costs computing time is in one place and names its effect', () => {
  const m = models();
  assert.equal(m.twoDimensionalMode, true, 'eine Ansicht von oben, die die dritte Achse fallen laesst');
  assert.ok(Array.isArray(m.effectToggles) && m.effectToggles.length >= 3,
    'Leuchthoefe, Kantendichte, Beschriftungen: einzeln schaltbar');
  assert.equal(m.thriftProfileWorks, true, 'ein Sparprofil setzt mehrere auf einmal');
  assert.equal(m.frameCapWorks, true, 'ein Bildratendeckel');
  assert.equal(m.settingsPersistReload, true, 'die Wahl ueberlebt den Reload');
  assert.equal(m.everyToggleNamesMeasuredEffect, true,
    'jede Einstellung nennt ihren auf DIESER Maschine gemessenen Effekt, keine Versprechen');
  assert.equal(m.noEffectSaysSo, true,
    'wo eine Einstellung nichts messbar bringt, sagt das Panel genau das');
  assert.equal(m.viewOnlyControlsStayed, true,
    'Schalter ohne Rechenkosten bleiben, wo sie sind: gebuendelt wird das Teure');
});

test('AC6/AC7: proof run, papers and wiring', () => {
  const m = models();
  assert.equal(m.overlapViolations, 0);
  assert.equal(m.clippingViolations, 0);
  assert.equal(m.cutWithoutHint, 0);
  assert.ok(m.port >= 4480, `Port >= 4480 erwartet, war ${m.port}`);
  assert.equal(m.leftoverProcesses, 0);
  for (const shot of ['settings.png', 'settings-switch.png', 'settings-fetch.png']) {
    const p = join(ROOT, 'verification', 'w10', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w10/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w10/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w10'], 'Script smoke:w10 fehlt');
  const adr = read('docs/adr/0001-modellwahl.md');
  assert.ok(/W10|Nachtrag/.test(adr),
    'die ADR braucht den Nachtrag: entschieden wurde die Engine, nicht ein Modell');
});
