// W4d acceptance tests: the explorer shows the file truth as discovery saw
// it and joins the coverage metadata visibly: indexed, partial, skipped,
// not indexed. Run: node --test tests/scaffold/w4d.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the tree joins graph files with the index_status lists', () => {
  const model = read('src/app/tree-model.ts');
  for (const marker of ['parse_partial', 'skipped', 'not_indexed', 'coverage']) {
    assert.ok(model.includes(marker), `Join-Marker ${marker} fehlt im tree-model`);
  }
  assert.ok(/truncated/.test(model), 'gekappte Listen muessen sichtbar werden');
  assert.ok(existsSync(join(ROOT, 'src/app/tree-model.test.ts')),
    'Join-Unit-Tests fehlen');
  const tests = read('src/app/tree-model.test.ts');
  assert.ok(/partial/.test(tests) && /not.?indexed/i.test(tests),
    'die Stufen muessen unit-getestet sein');
});

test('AC2/AC3/AC4: coverage is visible and honest, proven live', () => {
  const c = JSON.parse(read('verification/w4/coverage.json'));
  assert.equal(c.treeShowsAllDiscovered, true,
    'jede von der Discovery gesehene Datei muss im Baum stehen');
  assert.ok(Array.isArray(c.states) && new Set(c.states).size >= 2,
    `mindestens zwei verschiedene Stufen erwartet, war ${JSON.stringify(c.states)}`);
  assert.equal(c.partialOrSkippedVisible, true,
    'die praeparierte kaputte Datei muss als partial/skipped sichtbar sein');
  assert.equal(c.legendShown, true, 'die Legende mit der Quellen-Ehrlichkeit fehlt');
  assert.equal(c.brokenFileHonest, true, 'der Reader muss die Grenze erklaeren');
  assert.equal(c.freshnessNoteWorks, true,
    'eine nach dem Index geaenderte Datei muss die Frische-Notiz ausloesen');
  assert.ok(typeof c.undiscoveredGap === 'string' && c.undiscoveredGap.length > 10,
    'die gemessene Aussage zur unentdeckten Datei muss dokumentiert sein');
  assert.equal(c.galaxyLegendToggles, true,
    'die Galaxy-Legende muss auf- und zuklappbar sein (Nutzerfeedback)');
  assert.ok(c.galaxyLegendEntries >= 3,
    `die Legende braucht >= 3 erklaerte Elemente, war ${c.galaxyLegendEntries}`);
  assert.equal(c.galaxyLegendStatePersists, true,
    'der Klappzustand muss den Reload ueberleben');
  assert.ok(c.port >= 4320, `Port >= 4320 erwartet, war ${c.port}`);
  assert.equal(c.leftoverProcesses, 0);
  const p = join(ROOT, 'verification', 'w4', 'explorer-coverage.png');
  assert.ok(existsSync(p) && statSync(p).size > 30 * 1024,
    'explorer-coverage.png fehlt oder klein');
});

test('AC6: net deny and wiring', () => {
  const nd = JSON.parse(read('verification/w4/netdeny-w4d.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10);
  assert.ok(/smoke-w4d/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w4d'], 'Script smoke:w4d fehlt');
  assert.ok(existsSync(join(ROOT, 'tools/smoke-w4d.mjs')), 'tools/smoke-w4d.mjs fehlt');
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.ok(s.unitTests >= 420, `Unit-Suite muss >= 420 Tests fahren, war ${s.unitTests}`);
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
