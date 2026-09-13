// W11a acceptance tests: agents are visible bodies in the code, the instrument
// says what one is looking at, and nothing is claimed that no event carries.
// Run: node --test tests/scaffold/w11a.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync, readdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const agents = () => JSON.parse(read('verification/w11/agents.json'));

test('AC1: off means off, and no source means no source', () => {
  const a = agents();
  assert.equal(a.offMakesNoRequests, true,
    'solange der Live-Modus aus ist, geht kein fetch an die Bruecke');
  assert.equal(a.noBridgeExplained, true,
    'eingeschaltet ohne Bruecke sagt das Instrument den Zustand und den Befehl, statt Ruhe vorzutaeuschen');
  assert.equal(a.bridgeConnects, true, 'mit laufender Bruecke kommen die Ereignisse an');
});

test('AC2/AC3: a body, not a node, and the kind of work is in its behaviour', () => {
  const a = agents();
  assert.ok(a.agentsRendered >= 3, `mindestens 3 Agenten erwartet, waren ${a.agentsRendered}`);
  assert.equal(a.agentColorsDistinct, true, 'jeder Agent hat seine eigene Farbe');
  assert.equal(a.agentColorStableAcrossReload, true,
    'dieselbe Kennung ergibt dieselbe Farbe, auch nach dem Reload');
  assert.equal(a.nodeColorsUnchanged, true,
    'die Knoten- und Kantenfarben des Graphen bleiben unangetastet');
  assert.ok(a.workKindsRendered >= 3,
    `mindestens 3 Arten von Arbeit muessen unterscheidbar sein, waren ${a.workKindsRendered}`);
  assert.equal(a.letterOnBody, true, 'die Unterscheidung haengt nicht allein an der Farbe');
});

test('AC4: the mapping is exact where it can be, and honest where it cannot', () => {
  const a = agents();
  assert.equal(a.rangeMappingExact, true,
    'Datei plus Zeilenbereich trifft genau das erwartete Symbol');
  assert.equal(a.innermostWins, true, 'bei mehreren Treffern gewinnt der engste Bereich');
  assert.equal(a.fileOnlyHitsModule, true, 'nur Datei trifft den Modulknoten');
  assert.equal(a.uncertainMarked, true,
    'ein Knoten ohne Endzeile wird als unsicher gekennzeichnet, nicht als Treffer verkauft');
  assert.equal(a.unmappableListed, true,
    'was sich nicht verorten laesst, steht im Instrument, statt zu verschwinden');
});

test('AC5/AC5b: the instrument is compact, real, and has three sizes', () => {
  const a = agents();
  assert.equal(a.hudCompact, true, 'der Normalzustand ist kompakt');
  assert.ok(a.hudSize && a.hudSize.width > 0 && a.hudSize.height > 0,
    'die gemessene Groesse gehoert ins Artefakt');
  assert.equal(a.hudCountsReal, true, 'die Zahlen im Kopf sind gezaehlt, nicht geschaetzt');
  assert.equal(a.activityStripFromEvents, true,
    'der Aktivitaetsstreifen kommt aus echten Ereignissen, er ist kein Zierbild');
  assert.equal(a.seqGapReported, true, 'eine Luecke in der Reihenfolge wird gemeldet');
  assert.equal(a.hudExpandShowsCards, true, 'EXPAND zeigt die ausfuehrliche Karte je Agent');
  assert.equal(a.hudCollapsedKeepsLine, true, 'eingeklappt bleibt die Zeile mit der Zahl');
  assert.equal(a.hudSizePersists, true, 'die gewaehlte Groesse ueberlebt den Reload');
});

test('AC6/AC6b: the human is an actor too, and the layer switch lives in settings', () => {
  const a = agents();
  assert.equal(a.youActorShown, true, 'die eigene Navigation laeuft als eigener Akteur mit');
  assert.equal(a.filterYouAgentBoth, true, 'der Umschalter you / agent / both filtert wirklich');
  assert.equal(a.youEventsStayLocal, true,
    'die eigenen Ereignisse gehen NICHT in die Ereignisdatei');
  assert.equal(a.layerToggleInSettings, true,
    'der Schalter fuer die ganze Ebene liegt in der Darstellungsgruppe der Einstellungen');
  assert.equal(a.layerToggleNamesEffect, true, 'und nennt dort seinen gemessenen Effekt');
});

test('AC7: nothing is interpreted', () => {
  const a = agents();
  assert.equal(a.intentOnlyWhenReported, true,
    'eine Absichtszeile erscheint nur, wenn das Ereignis sie mitbringt');
  assert.equal(a.intentMarkedAsSelfReport, true,
    'und dann gekennzeichnet als Selbstauskunft des Agenten, nicht als Messung');
  assert.equal(a.noProgressNoScore, true,
    'kein Fortschritt, keine Prozentzahl, keine Bewertung, kein "denkt nach"');
});

test('AC8/AC8b: replayable, and the orbit is shown as a frame series', () => {
  const a = agents();
  assert.equal(a.replayFromFixture, true,
    'der Lauf spielt eine aufgezeichnete Ereignisdatei ab, mit steuerbarer Zeit');
  assert.ok(a.orbitFrames >= 8, `mindestens 8 Einzelbilder einer Umkreisung, waren ${a.orbitFrames}`);
  assert.equal(a.orbitContactSheetWritten, true, 'der Kontaktabzug muss geschrieben sein');
  assert.equal(a.orbitAnglesDiffer, true,
    'die gemessenen Winkel muessen sich unterscheiden, sonst steht der Koerper still');
  const sheet = join(ROOT, 'verification', 'w11', 'orbit-contact-sheet.png');
  assert.ok(existsSync(sheet), 'orbit-contact-sheet.png fehlt');
  assert.ok(statSync(sheet).size > 30 * 1024, 'der Kontaktabzug ist verdaechtig klein');
  const frames = readdirSync(join(ROOT, 'verification', 'w11', 'frames'));
  assert.ok(frames.length >= 8, `mindestens 8 Einzelbilder erwartet, waren ${frames.length}`);
});

test('AC9/AC10: proof run and wiring', () => {
  const a = agents();
  assert.equal(a.overlapViolations, 0);
  assert.equal(a.clippingViolations, 0);
  assert.equal(a.cutWithoutHint, 0);
  assert.ok(a.port >= 4500, `Port >= 4500 erwartet, war ${a.port}`);
  assert.equal(a.leftoverProcesses, 0);
  for (const shot of ['live-agents.png', 'live-agents-nobridge.png', 'live-agents-collapsed.png']) {
    const p = join(ROOT, 'verification', 'w11', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w11/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w11a/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w11a'], 'Script smoke:w11a fehlt');
  const readme = read('README.md');
  assert.ok(/agent-trace|agents\/hooks|Ereignisdatei/i.test(readme),
    'die Bruecke und die Hooks muessen dokumentiert sein');
  assert.ok(/keine Dateiinhalte|no file contents|nur Pfade/i.test(readme),
    'und der Satz, dass die Ereignisdatei niemals Dateiinhalte enthaelt');
});
