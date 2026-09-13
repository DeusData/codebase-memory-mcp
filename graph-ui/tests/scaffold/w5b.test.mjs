// W5b acceptance tests: the graph thinks, the model phrases. Classifier,
// card compiler with hard budgets, the citation contract in a live chat,
// and the head-to-head eval of all four ADR candidates.
// Run: node --test tests/scaffold/w5b.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1/AC2: classifier, recipes and card compiler are pure and budgeted', () => {
  const cls = read('src/compiler/question-classifier.ts');
  for (const marker of ['what-is', 'who-calls', 'what-if', 'where-entry',
    'why-error', 'compare', 'overview', 'other']) {
    assert.ok(cls.includes(marker), `Frageklasse ${marker} fehlt`);
  }
  assert.ok(existsSync(join(ROOT, 'src/compiler/fact-recipes.ts')), 'fact-recipes fehlt');
  const cards = read('src/compiler/card-compiler.ts');
  assert.ok(/3000/.test(cards) && /8000/.test(cards),
    'die harten Token-Budgets 3000/8000 muessen im Code stehen');
  assert.ok(/nicht gelistet|not listed/.test(cards),
    'die ehrliche Kappungsnotiz muss existieren');
  for (const f of ['src/compiler/question-classifier.test.ts',
    'src/compiler/card-compiler.test.ts']) {
    assert.ok(existsSync(join(ROOT, f)), `${f} fehlt`);
  }
});

test('AC5: the eval ran all four candidates head to head', () => {
  const questions = JSON.parse(read('eval/questions.json'));
  assert.ok(questions.length >= 40, `>= 40 goldene Fragen erwartet, war ${questions.length}`);
  for (const q of questions.slice(0, 5)) {
    assert.ok(q.expected && q.expected.length > 0, 'jede Frage braucht Kernaussagen');
  }
  const e = JSON.parse(read('verification/w5/eval.json'));
  assert.equal(e.models.length, 6,
    'vier ADR-Kandidaten plus die zwei Nutzernominierungen muessen gefahren sein');
  assert.ok(e.models.some((m) => /MiniCPM5-1B/.test(m.name)),
    'MiniCPM5-1B muss im Head-to-Head mitfahren');
  assert.ok(e.models.some((m) => /Qwen2\.5-Coder-1\.5B/i.test(m.name)),
    'Qwen2.5-Coder-1.5B-Instruct muss im Head-to-Head mitfahren');
  for (const m of e.models) {
    assert.equal(m.answered, questions.length, `${m.name} muss alle Fragen beantwortet haben`);
    assert.ok(typeof m.passRate === 'number' && typeof m.citationCompliance === 'number');
  }
  for (const winner of [e.winnerClassA, e.winnerClassB]) {
    assert.ok(winner && typeof winner.name === 'string', 'Sieger je Klasse muss benannt sein');
    assert.ok(winner.passRate >= 0.6,
      `Sieger-passRate muss >= 0.6 sein, war ${winner.passRate} (${winner.name})`);
    assert.ok(winner.citationCompliance >= 0.9,
      `Sieger-Zitattreue muss >= 0.9 sein, war ${winner.citationCompliance}`);
  }
  assert.equal(e.temperature, 0);
  assert.ok(Number.isInteger(e.seed), 'der Seed muss fest und dokumentiert sein');
});

test('AC3/AC6: the live chat cites cards and stays honest', () => {
  const c = JSON.parse(read('verification/w5/chat.json'));
  assert.equal(c.answered, true);
  assert.ok(c.citationsInAnswer >= 1, 'die Antwort muss [K]-Zitate tragen');
  assert.equal(c.citationClickNavigates, true, 'Zitat-Klick muss zur Quelle fuehren');
  assert.ok(c.cardsShown >= 2, 'die benutzten Karten muessen sichtbar sein');
  assert.equal(c.offHonest, true, 'LLM aus heisst: kein Senden, ehrlicher Hinweis');
  assert.equal(c.noCardHonest, true, 'ohne Karte faellt der vereinbarte Satz');
  assert.equal(c.neighborDepthDefault, 1,
    'Default-Kontext ist Fokus + erste Nachbarschaft (Martins Regel)');
  assert.equal(c.neighborDepthAdjustable, true,
    'die Nachbarschafts-Tiefe muss einstellbar sein (0/1/2)');
  assert.equal(c.neighborQualityNoteShown, true,
    'der Hinweis auf Qualitaetswirkung der Tiefe muss sichtbar sein');
  assert.ok(c.port >= 4310);
  assert.equal(c.leftoverProcesses, 0, 'auch der Sidecar muss beendet sein');
  const p = join(ROOT, 'verification', 'w5', 'chat.png');
  assert.ok(existsSync(p) && statSync(p).size > 30 * 1024, 'chat.png fehlt oder klein');
});

test('AC4: refine is wired under the contract', () => {
  const pseudo = readFileSync(join(ROOT, 'src/pseudocode/pseudocode-builder.ts'), 'utf8');
  assert.ok(pseudo.includes('applyRefinedPseudocode'), 'der Validator muss existieren');
  const c = JSON.parse(read('verification/w5/chat.json'));
  assert.equal(c.refineGuarded, true,
    'refine darf nur mit LLM ready erscheinen und validiert die Antwort');
});

test('AC7: wiring and net deny', () => {
  const nd = JSON.parse(read('verification/w5/netdeny-w5b.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(nd.samples >= 10);
  const pkg = JSON.parse(read('package.json'));
  for (const script of ['smoke:w5b', 'eval:llm']) {
    assert.ok(pkg.scripts?.[script], `Script ${script} fehlt`);
  }
  const s = JSON.parse(read('verification/w1/scaffold.json'));
  assert.ok(s.unitTests >= 430, `Unit-Suite muss >= 430 Tests fahren, war ${s.unitTests}`);
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
