// W4e acceptance tests: in entry-point mode the graph panel shows the code
// hierarchy of the subgraph (columns per call depth from the chosen entry),
// as a deterministic projection of the closure walk; the full galaxy stays
// one chip away. Run: node --test tests/scaffold/w4e.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the hierarchy projection is pure and deterministic', () => {
  const layout = read('src/galaxy/hierarchy-layout.ts');
  assert.ok(/ClosureResult|closure/i.test(layout), 'die Projektion muss den Walk konsumieren');
  assert.ok(!/Math\.random/.test(layout), 'keine Zufallswerte in der Projektion');
  assert.ok(/truncated/.test(layout), 'die Cap-Info muss durchgereicht werden');
  assert.ok(existsSync(join(ROOT, 'src/galaxy/hierarchy-layout.test.ts')),
    'Determinismus-Unit-Tests fehlen');
  const tests = read('src/galaxy/hierarchy-layout.test.ts');
  assert.ok(/hop|Spalte|column/i.test(tests), 'Spalten-je-Hop muss getestet sein');
});

test('AC2/AC3/AC4: the hierarchy view proves itself live', () => {
  const h = JSON.parse(read('verification/w4/hierarchy.json'));
  assert.equal(h.modeAutoSwitches, true, 'Entry-Modus muss auf hierarchy schalten');
  assert.ok(h.hierarchyNodes >= 4, `>= 4 Knoten erwartet, war ${h.hierarchyNodes}`);
  assert.ok(h.hierarchyDepth >= 2, `>= 2 Ebenen erwartet, war ${h.hierarchyDepth}`);
  assert.equal(h.rootIsChosen, true, 'die Wurzel muss das gewaehlte Symbol sein');
  assert.equal(h.columnsMatchHops, true, 'x-Koordinaten muessen je Hop clustern');
  assert.equal(h.stepPulseFollowsTour, true, 'der aktive Tour-Schritt muss pulsen');
  assert.equal(h.clickFollows, true);
  assert.equal(h.toggleBackToGalaxy, true, 'der galaxy-Chip muss zurueckschalten');
  assert.equal(h.headerHonest, true, 'der Kopf muss Symbol, N, Tiefe und Cap nennen');
  assert.ok(h.port >= 4330, `Port >= 4330 erwartet, war ${h.port}`);
  assert.equal(h.leftoverProcesses, 0);
  const p = join(ROOT, 'verification', 'w4', 'hierarchy.png');
  assert.ok(existsSync(p) && statSync(p).size > 40 * 1024,
    'hierarchy.png fehlt oder klein');
});

test('AC5: net deny and wiring', () => {
  const nd = JSON.parse(read('verification/w4/netdeny-w4e.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10);
  assert.ok(/smoke-w4e/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w4e'], 'Script smoke:w4e fehlt');
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.ok(s.unitTests >= 430, `Unit-Suite muss >= 430 Tests fahren, war ${s.unitTests}`);
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
