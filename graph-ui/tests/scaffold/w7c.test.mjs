// W7c acceptance tests: the chat resolves the symbol the reader means, no
// matter how it was spelled, and the panel behaves like a panel: resizable,
// header always in reach, closable without losing the history.
// Run: node --test tests/scaffold/w7c.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1/AC2/AC3: spelling does not decide, and a focus is never wasted', () => {
  const c = JSON.parse(read('verification/w7/chat.json'));
  assert.equal(c.lowercaseMentionResolves, true,
    '@createuser muss dasselbe Symbol finden wie das Such-Overlay');
  assert.ok(c.lowercaseMentionCitations >= 1,
    'die Antwort darauf muss mindestens ein K-Zitat tragen');
  assert.equal(c.mentionCandidateListShown, true,
    'bei Mehrdeutigkeit muessen die Kandidaten angeboten werden');
  assert.ok(c.mentionCandidateCount >= 2, 'die Kandidatenliste braucht mehrere Eintraege');
  assert.equal(c.focusFallbackAnswered, true,
    'ein unaufloesbares mention bei vorhandenem Fokus darf nicht abbrechen');
  assert.equal(c.focusFallbackExplained, true,
    'der Rueckfall auf den Fokus muss in einer eigenen Zeile begruendet sein');
  assert.equal(c.noCardStillHonest, true,
    'ohne Symbol und ohne Fokus bleibt der vereinbarte Satz');
  const resolver = read('src/compiler/subject-resolver.ts');
  assert.ok(/toLowerCase|localeCompare|caseInsensitive|ignoreCase/i.test(resolver),
    'die Aufloesung muss Schreibweisen ausdruecklich behandeln');
  assert.ok(existsSync(join(ROOT, 'src/compiler/subject-resolver.test.ts')),
    'die Aufloesung braucht ihre Unit-Tests');
});

test('AC4/AC5/AC5b: the panel behaves like a panel', () => {
  const c = JSON.parse(read('verification/w7/chat.json'));
  assert.equal(c.chatResizeWorks, true, 'die Hoehe muss ziehbar sein');
  assert.equal(c.chatResizePersists, true, 'die gewaehlte Hoehe muss den Reload ueberleben');
  assert.equal(c.chatResizeByKeyboard, true, 'der Griff muss auch per Tastatur gehen');
  assert.equal(c.chatHeaderStaysVisible, true,
    'die Kontext-Auswahl darf beim Scrollen nie verschwinden');
  assert.equal(c.chatClosesByEscape, true, 'Escape muss einklappen');
  assert.equal(c.chatClosesByButton, true, 'ein sichtbarer Knopf muss einklappen');
  assert.equal(c.historySurvivesClose, true, 'Einklappen darf den Verlauf nicht kosten');
  assert.equal(c.clearStillClears, true, 'clear bleibt der einzige Weg zum Loeschen');
  assert.match(c.llmMenuLabel, /local llm (on|off)/i,
    'der Menuepunkt muss "local llm" heissen, nicht nur "llm"');
  assert.match(c.llmStatusChip, /local llm/i, 'der Statusleisten-Chip ebenso');
  assert.match(c.llmPanelHeading, /local model|local llm/i,
    'die Panel-Ueberschrift muss dieselbe Sprache sprechen');
});

test('AC6/AC7: proof run, budgets and wiring', () => {
  const c = JSON.parse(read('verification/w7/chat.json'));
  assert.equal(c.overlapViolations, 0);
  assert.equal(c.clippingViolations, 0);
  assert.ok(c.port >= 4420, `Port >= 4420 erwartet, war ${c.port}`);
  assert.equal(c.leftoverProcesses, 0, 'auch der eigene Sidecar muss beendet sein');
  for (const shot of ['chat-answer.png', 'chat-resized.png']) {
    const p = join(ROOT, 'verification', 'w7', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w7/netdeny-w7c.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w7c/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w7c'], 'Script smoke:w7c fehlt');
  const e = JSON.parse(read('verification/w6/evalcheck.json'));
  assert.equal(e.evalCheckPass, true, 'die Eval-Kennzahlen der Sieger duerfen nicht fallen');
});
