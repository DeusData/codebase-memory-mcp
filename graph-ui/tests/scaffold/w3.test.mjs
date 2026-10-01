// W3 acceptance tests: the PR-1860 galaxy runs as a permanent panel with
// two-way focus follow, and the ported semantic search lives in the footer
// command line. MIT attribution for the taken-over DeusData code is law.
// Run: node --test tests/scaffold/w3.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the galaxy takeover carries its license and its edits', () => {
  const files = ['GraphScene.tsx', 'NodeCloud.tsx', 'EdgeLines.tsx',
    'NodeLabels.tsx', 'HaloLayer.tsx'];
  for (const f of files) {
    const src = read(join('src/galaxy', f));
    assert.ok(/MIT/.test(src) && /DeusData/.test(src),
      `${f} braucht den MIT/DeusData-Attributionsheader`);
  }
  const third = read('THIRD_PARTY.md');
  assert.ok(third.includes('DeusData') && third.includes('MIT') && /graph-ui/.test(third),
    'THIRD_PARTY.md muss die Galaxy-Uebernahme dokumentieren');
  const scene = read('src/galaxy/GraphScene.tsx');
  assert.ok(!scene.includes("document.querySelector(\"canvas\")")
    && !scene.includes("document.querySelector('canvas')"),
    'der Canvas-Griff muss auf gl.domElement umgestellt sein');
  const pkg = JSON.parse(read('package.json'));
  for (const dep of ['three', '@react-three/fiber', '@react-three/drei',
    '@react-three/postprocessing', 'postprocessing']) {
    const pin = pkg.dependencies?.[dep] ?? pkg.devDependencies?.[dep];
    assert.ok(typeof pin === 'string' && !/[\^~]/.test(pin),
      `${dep} muss exakt gepinnt sein, war ${pin}`);
  }
  assert.ok(!read('src/galaxy/NodeTooltipCard.tsx').includes('bg-popover'),
    'der eigene Tooltip darf kein Tailwind tragen');
});

test('AC2/AC3/AC5: two-way focus follow proven live', () => {
  const g = JSON.parse(read('verification/w3/galaxy.json'));
  assert.ok(g.searchResultCount >= 1, 'Suche muss Treffer zeigen');
  assert.equal(g.enterOpenedFile, 'src/services/userService.ts');
  assert.match(g.twinSubject, /userService\.createUser$/);
  assert.ok(g.flyToCount >= 2, `>= 2 Kamerafahrten erwartet, war ${g.flyToCount}`);
  assert.equal(g.clickOpenedFile, 'src/util/validate.ts');
  assert.match(g.twinSubjectAfterClick, /validate\.validateUser$/);
  assert.ok(g.highlightedCount >= 2, 'Knoten + Nachbarn muessen markiert sein');
  assert.ok(g.layoutNodes >= 50, `Layout muss >= 50 Knoten liefern, war ${g.layoutNodes}`);
  assert.equal(g.layoutSource, '/api/layout');
  assert.ok(g.port >= 4250, `Port >= 4250 erwartet, war ${g.port}`);
  assert.equal(g.leftoverProcesses, 0);
  const shots = [['galaxy.png', 50], ['search.png', 30]];
  for (const [shot, kb] of shots) {
    const p = join(ROOT, 'verification', 'w3', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > kb * 1024, `${shot} verdaechtig klein`);
  }
});

test('AC4: the semantic search is ported and wired', () => {
  const search = read('src/search/semantic-search.ts');
  assert.ok(search.includes('CodeAtlasIDE'), 'Herkunftsnotiz fehlt');
  assert.ok(existsSync(join(ROOT, 'src/search/semantic-search.test.ts')),
    'Ranking-Unit-Tests fehlen');
  const g = JSON.parse(read('verification/w3/galaxy.json'));
  assert.ok(Array.isArray(g.searchTopNames) && g.searchTopNames.includes('createUser'),
    'createUser muss unter den Suchtreffern sein');
});

test('AC6: net deny over the whole w3 click path', () => {
  const nd = JSON.parse(read('verification/w3/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10);
  assert.ok(/smoke-w3/.test(nd.command));
});

test('AC7: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w3'], 'Script smoke:w3 fehlt');
  assert.ok(existsSync(join(ROOT, 'tools/smoke-w3.mjs')), 'tools/smoke-w3.mjs fehlt');
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.ok(s.unitTests >= 240, `Unit-Suite muss >= 240 Tests fahren, war ${s.unitTests}`);
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
