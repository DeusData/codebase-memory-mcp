// W5a acceptance tests: the model ADR is committed with checksums, the
// sidecar manager tells the truth about its process, opt-out is the real
// default, and a committed policy file beats any preference.
// Run: node --test tests/scaffold/w5a.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: ADR, checksums and runtime provenance are committed', () => {
  const adr = read('docs/adr/0001-modellwahl.md');
  for (const marker of ['Qwen3.5-2B', 'LFM2.5-1.2B', 'Qwen3.5-4B', 'Gemma 4 E4B',
    'Apache 2.0', 'Eval']) {
    assert.ok(adr.includes(marker), `ADR-Marker ${marker} fehlt`);
  }
  assert.ok(/10 Mio|10M|Umsatz/.test(adr), 'die LFM-Lizenzschwelle muss im ADR stehen');
  assert.ok(existsSync(join(ROOT, 'verification/w5/modellrecherche.md')),
    'die Recherche mit Quellen muss committet sein');
  const sums = read('models/SHA256SUMS');
  // Korrektur 2026-08-29 (Orchestrator): nach Bernhards zwei Nominierungen
  // (MiniCPM5-1B, Qwen2.5-Coder-1.5B) sind es sechs Modelle, nicht vier.
  //
  // Zweite Korrektur am selben Tag, aus demselben Grund wie bei der Menuezeile
  // in W7b: gezaehlt werden PRUEFSUMMEN, nicht Zeilen. W10 hat der Datei eine
  // erklaerende Zeile vorangestellt, weil models/ seit dort der Cache des
  // Lesers ist und niemand mehr denken soll, hier stuende eine Liste von
  // Dateien, die das Programm erwartet. Ein Test, der an einem Kommentar
  // scheitert, misst die Datei und nicht ihre Aussage.
  const checksums = sums.trim().split('\n').filter((line) => !line.trimStart().startsWith('#'));
  assert.equal(checksums.length, 6, 'sechs Modell-Checksummen erwartet');
  const herkunft = read('vendor/llama/HERKUNFT.md');
  assert.ok(/b10675/.test(herkunft) && /cmake|Quellen|source/i.test(herkunft),
    'die Runtime-Herkunft muss Build und Version nennen');
  const m = JSON.parse(read('verification/w5/models.json'));
  assert.equal(m.files, 6);
  assert.equal(m.checksumsOk, true);
  assert.equal(m.llamaServerRuns, true);
  const ignore = read('.gitignore');
  assert.ok(/^models\/$/m.test(ignore) && /^vendor\/$/m.test(ignore),
    'models/ und vendor/ muessen gitignoriert sein');
});

test('AC2/AC3/AC5: the sidecar manager is honest, opt-out is real', () => {
  for (const f of ['llm/start.sh', 'llm/stop.sh', 'src/llm/sidecar.ts']) {
    assert.ok(existsSync(join(ROOT, f)), `${f} fehlt`);
  }
  const s = JSON.parse(read('verification/w5/sidecar.json'));
  assert.equal(s.llmOffByDefault, true, 'Erststart muss LLM aus bedeuten');
  assert.equal(s.zeroLlmRequestsWhileOff, true, 'aus heisst aus: kein Byte Richtung 4141');
  assert.equal(s.notRunningHonest, true, 'ohne Prozess muss die Anleitung stehen');
  assert.equal(s.statusReady, true, 'mit gestartetem Sidecar muss ready stehen');
  assert.match(s.modelShown, /Qwen3\.5-2B/, 'der Modellname muss sichtbar sein');
  assert.equal(s.chipShown, true, 'der Statusleisten-Chip muss da sein');
  assert.equal(s.stopFallsBackHonestly, true);
  assert.ok(s.port >= 4300, `Testserver-Port >= 4300 erwartet, war ${s.port}`);
  assert.equal(s.leftoverProcesses, 0, 'auch der llama-server muss beendet sein');
  for (const shot of ['sidecar-off.png', 'sidecar-ready.png']) {
    const p = join(ROOT, 'verification', 'w5', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
});

test('AC4: a committed policy beats any preference', () => {
  const s = JSON.parse(read('verification/w5/sidecar.json'));
  assert.equal(s.policyBlocks, true, 'policy.json muss das LLM erzwingbar abschalten');
  assert.equal(s.switchIneffective, true, 'der Schalter darf gegen die Policy nichts tun');
  assert.ok(existsSync(join(ROOT, 'src/llm/sidecar.test.ts')) ||
    existsSync(join(ROOT, 'src/llm/policy.test.ts')),
    'die Vorrangregel braucht Unit-Tests');
});

test('AC6: net deny with loopback sidecar allowed', () => {
  const nd = JSON.parse(read('verification/w5/netdeny-w5a.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10);
  assert.ok(/smoke-w5a/.test(nd.command));
});

test('AC7: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w5a'], 'Script smoke:w5a fehlt');
  assert.ok(pkg.scripts?.['verify:models'], 'Script verify:models fehlt');
  assert.ok(existsSync(join(ROOT, 'tools/smoke-w5a.mjs')), 'tools/smoke-w5a.mjs fehlt');
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
