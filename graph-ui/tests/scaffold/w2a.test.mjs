// W2a acceptance tests: the terminal chrome exists per the design reference,
// the tree comes from the graph, the reader shows real server-delivered
// files read-only and stays honest about truncation, and the whole click
// path ran under the net deny gate.
// Run: node --test tests/scaffold/w2a.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: design tokens and chrome are in place', () => {
  const tokens = read('src/styles/tokens.css');
  for (const t of ['--atlas-bg', '--atlas-panel', '--atlas-line', '--atlas-phosphor',
    '--atlas-dim', '--atlas-cyan', '--atlas-alarm']) {
    assert.ok(tokens.includes(t), `Token ${t} fehlt`);
  }
  assert.ok(/monospace/i.test(tokens), 'monospace-Stack fehlt in den Tokens');
  const smoke = JSON.parse(read('verification/w2/reader.json'));
  for (const id of ['atlas-header', 'atlas-menu', 'atlas-tabs', 'atlas-command',
    'atlas-statusbar', 'atlas-tree', 'atlas-breadcrumb']) {
    assert.ok(smoke.testids.includes(id), `data-testid ${id} wurde im Smoke nicht gesehen`);
  }
  assert.ok(/CODEATLAS/.test(smoke.brandText), 'Marke CODEATLAS fehlt in der Kopfzeile');
  assert.match(smoke.versionChip, /^v\d+\.\d+\.\d+/, 'Versions-Chip fehlt oder falsch');
  // Spezifikations-Korrektur 2026-08-29 (Orchestrator, Nutzerauftrag):
  // Der Test nagelte [f]ile fest, also ausgerechnet einen der vier
  // Menuepunkte ohne Verdrahtung. Eine Attrappe darf kein Abnahmekriterium
  // sein. Verlangt bleibt das Buchstaben-Menue als Form, belegt am
  // verdrahteten [a]tlas.
  assert.ok(/\[a\]tlas/.test(smoke.menuText), 'Buchstaben-Menue fehlt');
  assert.ok(/type a command or ask the atlas/.test(smoke.commandPlaceholder),
    'Kommandozeilen-Platzhalter fehlt');
});

test('AC2/AC3: tree from the graph, reader shows real source read-only', () => {
  const smoke = JSON.parse(read('verification/w2/reader.json'));
  assert.ok(smoke.treeEntries >= 10, `Baum muss >= 10 Eintraege zeigen, war ${smoke.treeEntries}`);
  assert.equal(smoke.treeSource, '/api/tree', 'Baum muss aus /api/tree kommen');
  assert.equal(smoke.openedFile, 'src/services/userService.ts');
  assert.ok(smoke.readerContainsCreateUser === true, 'Reader muss createUser zeigen');
  assert.equal(smoke.readerReadOnly, true, 'Reader muss read-only sein');
  assert.equal(smoke.editAttemptChangedContent, false, 'Editier-Versuch darf nichts aendern');
  assert.ok(/src\s*(>|›)\s*services/.test(smoke.breadcrumb),
    `Breadcrumb falsch: ${smoke.breadcrumb}`);
  assert.equal(smoke.tabOpened, true, 'Tab muss erscheinen');
  assert.equal(smoke.rpcTool, 'get_code_snippet', 'Inhalt muss via get_code_snippet kommen');
  assert.ok(/^works|ignored$/.test(smoke.windowSemantics),
    'Fenster-Semantik der grossen Datei muss gemessen sein');
  assert.equal(smoke.truncationHonest, true,
    'gekappter Inhalt muss ehrlich benannt werden (oder Datei vollstaendig geladen)');
  assert.ok(smoke.largeFileLines > 500, 'die grosse Fixture-Datei muss > 500 Zeilen haben');
});

test('AC4: screenshots prove the look', () => {
  for (const shot of ['app-chrome.png', 'reader.png']) {
    const p = join(ROOT, 'verification', 'w2', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const smoke = JSON.parse(read('verification/w2/reader.json'));
  assert.ok(smoke.port >= 4230, `Port muss >= 4230 sein, war ${smoke.port}`);
  assert.equal(smoke.leftoverProcesses, 0);
});

test('AC5: the whole click path ran under the net deny gate', () => {
  const nd = JSON.parse(read('verification/w2/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10, `>= 10 Samples erwartet, war ${nd.samples}`);
  assert.ok(/smoke-w2a/.test(nd.command), 'das Gate muss den w2a-Smoke gefahren haben');
});

test('AC6: the docs carry the module snippet finding', () => {
  const inv = read('INVENTAR.md');
  assert.ok(/Modul-QN|Module-Knoten|Modul-Knoten/i.test(inv),
    'INVENTAR.md muss den Modul-Snippet-Befund tragen');
  const asks = read('UPSTREAM-ASKS.md');
  assert.ok(/Modul/i.test(asks) && /mtime/i.test(asks),
    'Ask 1 muss auf den Interim-Weg und den mtime-Rest verweisen');
});

test('AC7: wiring', () => {
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w2a'], 'Script smoke:w2a fehlt');
  assert.ok(existsSync(join(ROOT, 'tools/smoke-w2a.mjs')), 'tools/smoke-w2a.mjs fehlt');
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
