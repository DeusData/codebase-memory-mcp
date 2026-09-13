// W4b acceptance tests: the bug wizard tells static from observed with real
// ingested traces and shows divergence as two lists, and the change view
// shows the blast radius with the ported risk rules.
// Run: node --test tests/scaffold/w4b.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: risk rules and impact model are ported, bug paths exist', () => {
  const rules = read('src/impact/risk-rules.ts');
  assert.ok(rules.includes('CodeAtlasIDE'), 'Herkunftsnotiz fehlt in risk-rules');
  for (const marker of ['perSymbolRisk', 'overallRisk', 'transitiveLoopDepth',
    'unguardedRecursion', 'isEntryPoint']) {
    assert.ok(rules.includes(marker), `risk-rules-Marker ${marker} fehlt`);
  }
  const model = read('src/impact/impact-model.ts');
  assert.ok(model.includes('CodeAtlasIDE'), 'Herkunftsnotiz fehlt in impact-model');
  for (const marker of ['mapChangeImpact', 'summaryTiles', 'narrative']) {
    assert.ok(model.includes(marker), `impact-model-Marker ${marker} fehlt`);
  }
  const paths = read('src/traces/bug-paths.ts');
  assert.ok(/staticOnly/.test(paths) && /runtimeOnly/.test(paths),
    'Divergenz muss als zwei Listen existieren');
  assert.ok(/truncated/.test(paths), 'Ketten-Kappung muss sichtbar sein');
  assert.ok(existsSync(join(ROOT, 'src/impact/risk-rules.test.ts')),
    'risk-rules-Tests fehlen');
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.ok(s.unitTests >= 340, `Unit-Suite muss >= 340 Tests fahren, war ${s.unitTests}`);
});

test('AC2/AC4: the bug wizard proves static vs observed live', () => {
  const b = JSON.parse(read('verification/w4/bugwizard.json'));
  assert.ok(b.staticChains >= 1, 'mindestens eine statische Kette');
  assert.equal(b.observedPathShown, true, 'der beobachtete Pfad muss sichtbar sein');
  assert.equal(b.observedCount, 3, 'count 3 aus dem Ingest');
  assert.equal(b.observedLabel, 'smoke-run');
  assert.ok(b.staticOnlyCount >= 1, 'erwartet-nie-beobachtet darf nicht leer sein');
  assert.ok(b.runtimeOnlyCount >= 1, 'beobachtet-nicht-im-Index darf nicht leer sein');
  assert.equal(b.noTracesHonest, true, 'ohne Traces muss der Wizard es sagen und anleiten');
  assert.equal(b.hopClickNavigates, true);
  assert.ok(b.port >= 4270, `Port >= 4270 erwartet, war ${b.port}`);
  assert.equal(b.leftoverProcesses, 0);
  for (const shot of ['bugwizard-divergence.png', 'bugwizard-no-traces.png']) {
    const p = join(ROOT, 'verification', 'w4', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
});

test('AC3/AC4: the blast radius proves itself live', () => {
  const i = JSON.parse(read('verification/w4/impact.json'));
  assert.ok(i.directCount >= 1);
  assert.ok(i.downstreamCount >= 1);
  assert.ok(typeof i.endpointNamed === 'string' && i.endpointNamed.length > 0);
  assert.ok(['low', 'medium', 'high'].includes(i.badge), `Badge unbekannt: ${i.badge}`);
  assert.ok(typeof i.badgeRulesExplained === 'string' && i.badgeRulesExplained.length > 10,
    'die Badge-Begruendung muss die erfuellten Regeln nennen');
  assert.equal(i.narrativeEvidenceOk, true, 'jede Behauptung braucht Evidence');
  assert.equal(i.invalidRefNoEngineCall, true, 'kaputter Ref darf keinen Engine-Call ausloesen');
  assert.ok(i.tilesShown >= 5, `>= 5 Kacheln erwartet, war ${i.tilesShown}`);
  const p = join(ROOT, 'verification', 'w4', 'impact.png');
  assert.ok(existsSync(p) && statSync(p).size > 30 * 1024, 'impact.png fehlt oder klein');
});

test('AC5: net deny over the w4b click path', () => {
  const nd = JSON.parse(read('verification/w4/netdeny-w4b.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10);
  assert.ok(/smoke-w4b/.test(nd.command));
});

test('AC6: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w4b'], 'Script smoke:w4b fehlt');
  assert.ok(existsSync(join(ROOT, 'tools/smoke-w4b.mjs')), 'tools/smoke-w4b.mjs fehlt');
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
