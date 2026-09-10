// W12a acceptance: command examples may teach the focused line, but their
// empty surface must not swallow a visible control behind them.
// Run: node --test tests/scaffold/w12a.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (path) => readFileSync(join(ROOT, path), 'utf8');
const report = () => JSON.parse(read('verification/w12a/command-overlay.json'));

function intersectionArea(a, b) {
  const width = Math.max(0, Math.min(a.x + a.width, b.x + b.width) - Math.max(a.x, b.x));
  const height = Math.max(0, Math.min(a.y + a.height, b.y + b.height) - Math.max(a.y, b.y));
  return width * height;
}

test('AC1: the real post-search state keeps its help without covering unfold', () => {
  const r = report();
  assert.equal(r.symbolSelected, 'createUser');
  assert.equal(r.commandEmpty, true);
  assert.equal(r.commandFocused, true);
  assert.equal(r.examplesVisible, true);
  assert.equal(r.unfoldVisible, true);
  assert.ok(r.measuredGeometry?.examples && r.measuredGeometry?.unfold,
    'beide echten Browser-Rechtecke muessen im Beleg stehen');
  assert.equal(intersectionArea(r.measuredGeometry.examples, r.measuredGeometry.unfold), 0,
    'die Beispielbox darf den sichtbaren Knopf auch optisch nicht uebermalen');
  assert.equal(r.unfoldHitTarget, 'atlas-explain-collapse',
    'am sichtbaren Knopf muss der Knopf liegen, nicht die Beispielbox');
  assert.equal(r.unfoldClickSucceeded, true,
    'ein normaler Playwright-Klick muss in einem Schritt gelingen');
  assert.equal(r.explainOpened, true);
});

test('AC2: the command examples themselves remain real controls', () => {
  const r = report();
  assert.ok(r.examplesCount >= 3, `mindestens drei Beispiele erwartet, waren ${r.examplesCount}`);
  assert.equal(r.exampleHitTarget, 'atlas-command-example');
  assert.equal(r.exampleClickSucceeded, true);
  assert.ok(typeof r.exampleText === 'string' && r.exampleText.length >= 2);
  assert.equal(r.commandAfterExample, r.exampleText,
    'das angeklickte Beispiel muss weiter in die Kommandozeile schreiben');
});

test('AC3: isolated proof has no errors, network or process debris', () => {
  const r = report();
  assert.equal(r.consoleErrors, 0);
  assert.equal(r.uncaughtExceptions, 0);
  assert.equal(r.leftoverProcesses, 0);
  const image = join(ROOT, 'verification', 'w12a', 'command-overlay.png');
  assert.ok(existsSync(image));
  assert.ok(statSync(image).size > 20 * 1024, 'der Bildbeleg ist verdaechtig klein');
  const nd = JSON.parse(read('verification/w12a/netdeny.json'));
  assert.equal(nd.exitCode, 0);
  assert.equal(nd.outboundViolations, 0);
});

test('AC4: the isolated smoke is wired through network deny', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w12a']);
  assert.match(pkg.scripts['smoke:w12a'], /net-deny-gate\.mjs.*smoke-w12a\.mjs/);
});
