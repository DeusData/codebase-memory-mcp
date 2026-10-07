// W6a acceptance tests: the whole click path runs air-gapped in one proof,
// the warm twin budget holds, the chrome strings live in a typed catalog,
// and a fresh clone stands on its own. Run: node --test tests/scaffold/w6a.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, readdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the whole click path ran air-gapped in one proof', () => {
  const a = JSON.parse(read('verification/w6/airgap.json'));
  assert.equal(a.outboundViolations, 0, 'kein Byte verlaesst die Maschine');
  assert.ok(a.samples >= 60, `>= 60 Stichproben erwartet, war ${a.samples}`);
  assert.ok(a.clickSteps >= 25, `>= 25 benannte Schritte erwartet, war ${a.clickSteps}`);
  assert.equal(a.pageErrors, 0);
  assert.equal(a.consoleErrors, 0);
  assert.equal(a.leftoverProcesses, 0, 'auch Sidecar und Server muessen weg sein');
  const shots = readdirSync(join(ROOT, 'verification', 'w6', 'walk'))
    .filter((f) => f.endsWith('.png'));
  assert.ok(shots.length >= 10, `>= 10 Screenshots erwartet, war ${shots.length}`);
  assert.equal(a.chatCitationClicked, true, 'die Chat-Zitat-Navigation gehoert zur Strecke');
  assert.equal(a.overlapViolations, 0,
    'kein sichtbarer Text darf sich ueberlagern (Bernhards Schlussanforderung)');
  assert.equal(a.clippingViolations, 0,
    'kein Element darf aus seinem Container ragen');
  assert.ok(a.scrolledRegions >= 6,
    `alle scrollbaren Bereiche muessen durchgescrollt geprueft sein, war ${a.scrolledRegions}`);
});

test('AC2: the warm twin budget holds', () => {
  const b = JSON.parse(read('verification/w6/budgets.json'));
  assert.ok(b.twinWarmP95Ms <= 800,
    `Twin warm p95 muss <= 800ms sein, war ${b.twinWarmP95Ms}`);
  assert.ok(Number.isFinite(b.twinColdMs), 'twinColdMs muss dokumentiert sein');
  assert.ok(Number.isFinite(b.searchOverlayMs), 'searchOverlayMs muss dokumentiert sein');
  assert.ok(Number.isFinite(b.galaxyLoadMs), 'galaxyLoadMs muss dokumentiert sein');
  assert.ok(b.twinWarmSamples >= 10, 'p95 braucht >= 10 warme Messungen');
});

test('AC3: chrome strings live in the typed catalog', () => {
  const catalog = read('src/i18n/messages.ts');
  assert.ok(catalog.includes('as const'), 'der Katalog muss as const typisiert sein');
  for (const marker of ['menu', 'statusbar', 'why', 'tour']) {
    assert.ok(catalog.includes(marker), `Katalog-Bereich ${marker} fehlt`);
  }
  assert.ok(existsSync(join(ROOT, 'src/i18n/messages.test.ts')),
    'Katalog-Vollstaendigkeits-Test fehlt');
  const scan = JSON.parse(read('verification/w6/stylegate.json'));
  assert.equal(scan.hardcodedChromeStrings, 0,
    'keine hartkodierten sichtbaren Chrome-Strings mehr');
});

test('AC4: a fresh clone stands on its own', () => {
  const f = JSON.parse(read('verification/w6/freshclone.json'));
  assert.equal(f.cloneOk, true);
  assert.equal(f.npmCiOk, true);
  assert.equal(f.scaffoldPass, true, 'alle Scaffold-Tests muessen im frischen Clone gruen sein');
  assert.equal(f.unitPass, true, 'die Unit-Suite muss im frischen Clone gruen sein');
});

test('AC5: style, attribution and version gates', () => {
  const s = JSON.parse(read('verification/w6/stylegate.json'));
  assert.equal(s.dashHitsOutsideDocumentedQuotes, 0,
    'lange Striche nur in dokumentierten Server-Zitaten');
  assert.ok(Array.isArray(s.documentedQuoteExceptions),
    'die Zitat-Ausnahmen muessen gelistet sein');
  assert.equal(s.attributionHits, 0, 'keine Claude/Anthropic-Treffer im Repo');
  const pkg = JSON.parse(read('package.json'));
  // Spezifikations-Korrektur 2026-08-29 (Orchestrator, Nutzerentscheidung):
  // Die Versionsnummer gehoert dem Eigentuemer des Produkts, nicht dem
  // Abnahmetest. Bernhard hat sie auf 0.0.1 gesetzt: v1.0.0 waere eine
  // Reifebehauptung, die er nicht teilt, und damit genau die Sorte
  // leeres Versprechen, die dieser Zyklus austreibt.
  assert.equal(pkg.version, '0.0.1', 'die Version muss auf 0.0.1 stehen');
  const a = JSON.parse(read('verification/w6/airgap.json'));
  assert.equal(a.versionChipShown, 'v0.0.1', 'der Header-Chip muss v0.0.1 zeigen');
});

test('AC6: wiring and net deny', () => {
  const nd = JSON.parse(read('verification/w6/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w6-full/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  for (const script of ['smoke:w6', 'check:freshclone']) {
    assert.ok(pkg.scripts?.[script], `Script ${script} fehlt`);
  }
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
