// Current release acceptance: unlike the historical W6b proof, this test
// binds the release report to every tracked proof and commit through W15/W12.
// Run: node --test tests/scaffold/release-current.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (path) => readFileSync(join(ROOT, path), 'utf8');
const git = (...args) => execFileSync('git', args, { cwd: ROOT, encoding: 'utf8' }).trim();
const report = () => JSON.parse(read('verification/w6/release.json'));

const trackedProofs = () => git('ls-files', 'verification')
  .split('\n')
  .filter((path) => path && path !== 'verification/w6/release.json')
  .sort();

test('AC1: the declared and measured proof inventory exactly matches Git', () => {
  const expected = trackedProofs();
  assert.equal(expected.length, 226, `226 Beweise erwartet, war ${expected.length}`);

  const source = read('tools/release-gate.mjs');
  for (const path of expected) {
    assert.ok(source.includes(path), `ausgeschriebener Beweis fehlt im Gate: ${path}`);
  }

  const r = report();
  const measured = r.artifacts.map((entry) => entry.path).sort();
  assert.deepEqual(measured, expected);
  assert.equal(r.artifactsChecked, expected.length);
  assert.deepEqual(r.missingArtifacts, []);
  assert.deepEqual(r.unlistedArtifacts, []);
  assert.equal(r.allSmokeArtifactsPresent, true);
});

test('AC2: every delivered phase and every commit is reconciled', () => {
  const r = report();
  const phases = new Set(r.planReconciliation.map((entry) => entry.phase));
  for (const phase of [
    'W7a', 'W7b', 'W7c', 'W8', 'W8b', 'W8c', 'W9', 'W10', 'W10b',
    'W11a', 'W11b', 'W12a', 'W12b', 'W12c', 'W12d', 'W12', 'W13', 'W14', 'W15',
  ]) {
    assert.ok(phases.has(phase), `Plan-Zuordnung fehlt: ${phase}`);
  }
  assert.deepEqual(r.planItemsWithoutCommit, []);
  assert.deepEqual(r.commitsWithoutPlanItem, []);
  assert.ok(r.planReconciliation.every((entry) => entry.commit && entry.item && entry.evidence));
});

test('AC3: the report is fresh and bound to the current release candidate', () => {
  const r = report();
  const head = git('rev-parse', 'HEAD');
  const parent = git('rev-parse', 'HEAD^');
  let directCandidate = false;
  try {
    directCandidate = git('rev-parse', `${r.sourceHead}^`) === head;
  } catch {
    // An unknown object is neither the current candidate nor its direct child.
  }
  assert.ok([head, parent].includes(r.sourceHead) || directCandidate,
    `Bericht ${r.sourceHead} gehoert nicht zu HEAD ${head}, dessen Elterncommit oder direktem Kandidaten`);
  const ageMs = Date.now() - Date.parse(r.generatedAt);
  assert.ok(Number.isFinite(ageMs) && ageMs >= 0 && ageMs < 24 * 60 * 60 * 1000,
    `Releasebericht ist nicht frisch: ${r.generatedAt}`);
});

test('AC4: the gate enforces a clean, complete candidate instead of merely reporting defects', () => {
  const source = read('tools/release-gate.mjs');
  assert.match(source, /if\s*\(\s*!cleanTree\s*\)/, 'Dirty-Baum muss das Gate rot machen');
  assert.match(source, /if\s*\(\s*unlistedArtifacts\.length\s*>\s*0\s*\)/,
    'ungelistete Artefakte muessen das Gate rot machen');
  assert.match(source, /if\s*\(\s*commitsWithoutPlanItem\.length\s*>\s*0\s*\)/,
    'Commits ohne Planpunkt muessen das Gate rot machen');

  const r = report();
  assert.equal(r.cleanTree, true);
  assert.deepEqual(r.dirtyPaths, []);
  assert.deepEqual(r.unexpectedDirtyPaths, []);
  for (const key of [
    'fullSuitePass', 'unitPass', 'evalCheckPass', 'allSmokeArtifactsPresent',
    'upstreamAsksHandover', 'cbmPushDisabled', 'versionMatches',
  ]) {
    assert.equal(r[key], true, `${key} muss gruen sein`);
  }
  assert.equal(r.attributionHits, 0);
  assert.equal(r.dashHitsOutsideDocumentedQuotes, 0);
});

test('AC5: the final report carries the suites measured in this run', () => {
  const r = report();
  if (r.scaffold?.seeded === true) {
    assert.match(r.note, /Vorabfassung/);
    return;
  }
  assert.equal(r.extras.unit.exit, 0);
  assert.ok(Number.isFinite(r.extras.unit.tests) && r.extras.unit.tests >= 2019,
    `mindestens 2019 Unit-Tests erwartet, war ${r.extras.unit.tests}`);
  assert.equal(r.scaffold.exit, 0);
  assert.equal(r.scaffold.fail, 0);
  assert.equal(r.scaffold.pass, r.scaffold.tests);
  assert.ok(Number.isFinite(r.scaffold.tests) && r.scaffold.tests >= 193,
    `mindestens 193 Scaffold-Tests erwartet, war ${r.scaffold.tests}`);
  assert.equal(r.testCountSync.synced, true);
});

test('AC6: the public scaffold commands run test files on current Node', () => {
  const pkg = JSON.parse(read('package.json'));
  for (const name of ['test', 'verify']) {
    assert.match(pkg.scripts?.[name] ?? '',
      /node --test tests\/scaffold\/\*\.test\.mjs/,
      `${name} muss die Testdateien statt des Verzeichnisses an Node uebergeben`);
  }
});
