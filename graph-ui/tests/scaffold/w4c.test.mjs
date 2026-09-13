// W4c acceptance tests: the flow box with its stepper lands in the twin,
// the pseudocode builder is ported, and the W2b debt is paid: the imports
// group is back. Run: node --test tests/scaffold/w4c.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: pseudocode layer ported, imports group reinstated', () => {
  for (const f of ['src/pseudocode/pseudocode-builder.ts',
    'src/pseudocode/flow-model.ts', 'src/pseudocode/imports-group.ts']) {
    assert.ok(existsSync(join(ROOT, f)), `${f} fehlt`);
    assert.ok(read(f).includes('CodeAtlasIDE'), `${f} braucht die Herkunftsnotiz`);
  }
  const builder = read('src/pseudocode/pseudocode-builder.ts');
  for (const marker of ['buildPseudocode', 'sourceRef', 'capped', 'applyRefinedPseudocode']) {
    assert.ok(builder.includes(marker), `Builder-Marker ${marker} fehlt`);
  }
  assert.ok(!/from 'react'|from "react"/.test(builder), 'der Builder bleibt React-frei');
  const rm = read('src/twin/render-model.ts');
  assert.ok(!/noch nicht portiert/.test(rm),
    'die W2b-Auslassungsnotiz muss durch den Vollzug ersetzt sein');
  const tvm = read('src/twin/twin-view-model.ts');
  assert.ok(/importsSection/.test(tvm), 'importsSection muss reaktiviert sein');
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.ok(s.unitTests >= 380, `Unit-Suite muss >= 380 Tests fahren, war ${s.unitTests}`);
});

test('AC2/AC3/AC4: flow box, stepper and pseudocode proven live', () => {
  const f = JSON.parse(read('verification/w4/flow.json'));
  assert.equal(f.flowHeadClickable, true,
    'der flow()-Kopf muss ein fokussierbarer Button sein (Nutzerfeedback)');
  assert.equal(f.flowTogglesOnHeadClick, true,
    'Klick auf den Kopf muss den Kasten schliessen und oeffnen');
  assert.equal(f.flowDarkStyled, true,
    'der Kasten muss im dunklen Token-Design stehen, nicht in weiss');
  assert.ok(f.flowParticipants >= 3, `>= 3 Teilnehmer erwartet, war ${f.flowParticipants}`);
  assert.equal(f.flowSteps, 6);
  assert.equal(f.stepperMovesEditor, true);
  assert.equal(f.stepperMovesDiagram, true);
  assert.equal(f.stepsListSync, true);
  assert.equal(f.mayRaiseShown, true);
  assert.ok(f.pseudocodeLines >= 6, `>= 6 Pseudocode-Zeilen erwartet, war ${f.pseudocodeLines}`);
  assert.equal(f.pseudocodeHasImportsGroup, true, 'What it pulls in muss da sein');
  assert.equal(f.pseudocodeLineClickNavigates, true);
  assert.equal(f.twinImportsGroupShown, true, 'der Data-Block braucht die Imports-Antwort');
  assert.equal(f.honestBlockShown, true);
  assert.ok(f.port >= 4280, `Port >= 4280 erwartet, war ${f.port}`);
  assert.equal(f.leftoverProcesses, 0);
  for (const shot of ['flow.png', 'pseudocode.png']) {
    const p = join(ROOT, 'verification', 'w4', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
});

test('AC5: net deny over the w4c click path', () => {
  const nd = JSON.parse(read('verification/w4/netdeny-w4c.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10);
  assert.ok(/smoke-w4c/.test(nd.command));
});

test('AC6: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w4c'], 'Script smoke:w4c fehlt');
  assert.ok(existsSync(join(ROOT, 'tools/smoke-w4c.mjs')), 'tools/smoke-w4c.mjs fehlt');
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
