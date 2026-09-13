// W6b acceptance tests: the demo walk is recorded for Martin, the fresh
// context audit closed its findings, and the release gate proves the release.
// Run: node --test tests/scaffold/w6b.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the demo walk is recorded', () => {
  const video = join(ROOT, 'verification', 'w6', 'demo', 'demo.webm');
  assert.ok(existsSync(video), 'demo.webm fehlt');
  assert.ok(statSync(video).size > 1024 * 1024, 'demo.webm verdaechtig klein');
  const d = JSON.parse(read('verification/w6/demo/demo.json'));
  assert.ok(d.steps.length >= 15, `>= 15 Drehbuch-Schritte erwartet, war ${d.steps.length}`);
  assert.equal(d.leftoverProcesses, 0);
  for (const marker of ['why', 'tour', 'twin', 'flow', 'galaxy', 'hierarchy',
    'wizard', 'impact', 'chat']) {
    assert.ok(d.steps.some((s) => new RegExp(marker, 'i').test(s.name)),
      `Drehbuch-Schritt ${marker} fehlt`);
  }
});

test('AC2: the fresh context audit closed its findings', () => {
  const a = JSON.parse(read('verification/w6/audit.json'));
  assert.ok(a.requirementsChecked >= 25,
    `>= 25 geprufte Anforderungen erwartet, war ${a.requirementsChecked}`);
  assert.equal(a.notMet, 0, 'kein Requirement darf offen not-met sein');
  // Spezifikations-Korrektur 2026-08-29 (Orchestrator, Audit-Befund 11):
  // Dem unabhaengigen Audit sein Ergebnis vorzuschreiben hebt seine
  // Unabhaengigkeit auf. Verlangt wird stattdessen: jeder not-provable-
  // Punkt ist einzeln begruendet (strukturell ausserhalb des Repos
  // beweisbar), jeder partially-met-Punkt ist im Finding-Loop geloest
  // ODER als bewusste Nichtaufnahme mit Grund dokumentiert.
  assert.ok(Array.isArray(a.notProvableJustified)
    && a.notProvableJustified.length === a.notProvable
    && a.notProvableJustified.every((e) => e.item && e.reason),
    'jeder not-provable-Punkt braucht seine Begruendung');
  assert.ok(Array.isArray(a.partiallyMetDispositions)
    && a.partiallyMetDispositions.length === a.partiallyMet
    && a.partiallyMetDispositions.every((e) => e.item
      && ['fixed', 'accepted'].includes(e.disposition) && e.detail),
    'jeder partially-met-Punkt braucht seine Disposition (fixed/accepted mit Detail)');
  assert.ok(a.adversarialProbes >= 2, 'das Audit muss eigene Proben gefahren haben');
  assert.equal(a.ranSuitesItself, true, 'das Audit muss die Suiten selbst gefahren haben');
  const md = read('verification/w6/audit.md');
  assert.ok(md.length > 2000, 'der Audit-Bericht muss substanziell sein');
  assert.ok(/requirement/i.test(md) && /commit/i.test(md), 'die Matrix fehlt');
  assert.ok(/Disposition/i.test(md), 'der Bericht braucht den Dispositions-Anhang');
});

test('AC3: the release gate proves the release', () => {
  const r = JSON.parse(read('verification/w6/release.json'));
  assert.equal(r.fullSuitePass, true);
  assert.equal(r.unitPass, true);
  assert.equal(r.allSmokeArtifactsPresent, true);
  assert.ok(Array.isArray(r.planReconciliation) && r.planReconciliation.length >= 12,
    'jeder Plan-Punkt braucht seine Commit-Zuordnung');
  assert.ok(r.planReconciliation.every((e) => e.commit && e.item),
    'Zuordnungen muessen item und commit tragen');
  assert.equal(r.attributionHits, 0);
  assert.equal(r.dashHitsOutsideDocumentedQuotes, 0);
  assert.equal(r.upstreamAsksHandover, true);
  assert.equal(r.cbmPushDisabled, true, 'die cbm-Push-URL muss auf DISABLED stehen');
  const pkg = JSON.parse(read('package.json'));
  // Spezifikations-Korrektur 2026-08-29 (Orchestrator, Nutzerentscheidung):
  // Die Versionsnummer gehoert dem Eigentuemer des Produkts, nicht dem
  // Abnahmetest. Bernhard hat sie auf 0.0.1 gesetzt: v1.0.0 waere eine
  // Reifebehauptung, die er nicht teilt, und damit genau die Sorte
  // leeres Versprechen, die dieser Zyklus austreibt.
  assert.equal(pkg.version, '0.0.1');
});

test('AC4: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  for (const script of ['demo:record', 'gate:release']) {
    assert.ok(pkg.scripts?.[script], `Script ${script} fehlt`);
  }
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
