// Current release proof acceptance: report and eval measurement are one proof.
// Run: node --test tests/scaffold/release-proof-binding.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { citationComplianceOf } from '../../tools/lib/eval-citation-summary.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (path) => readFileSync(join(ROOT, path), 'utf8');
const git = (...args) => execFileSync('git', args, { cwd: ROOT, encoding: 'utf8' }).trim();
const report = () => JSON.parse(read('verification/w6/release.json'));
const PROOF_FILES = ['verification/w6/evalcheck.json', 'verification/w6/release.json'];

test('AC1: the report binds the exact eval artifact by path, hash and fresh time', () => {
    const r = report();
    assert.deepEqual(r.generatedProofFiles, PROOF_FILES);
    assert.equal(r.evalcheck?.path, PROOF_FILES[0]);
    const raw = read(r.evalcheck.path);
    const hash = createHash('sha256').update(raw).digest('hex');
    assert.match(r.evalcheck.sha256 ?? '', /^[a-f0-9]{64}$/);
    assert.equal(r.evalcheck.sha256, hash, 'Evalcheck-Hash passt nicht zur Datei im selben Baum');
    const evalArtifact = JSON.parse(raw);
    assert.equal(r.evalcheck.generatedAt, evalArtifact.generatedAt,
        'Releasebericht und Eval-Artefakt nennen nicht dieselbe Messung');
    const age = Date.now() - Date.parse(r.evalcheck.generatedAt);
    assert.ok(Number.isFinite(age) && age >= 0 && age < 24 * 60 * 60 * 1000,
        `Eval-Messung ist nicht frisch: ${r.evalcheck.generatedAt}`);
});

test('AC2: a parent-bound proof commit contains both and only both generated files', () => {
    const r = report();
    const head = git('rev-parse', 'HEAD');
    const parent = git('rev-parse', 'HEAD^');
    if (r.sourceHead === parent) {
        const files = git('diff-tree', '--no-commit-id', '--name-only', '-r', 'HEAD')
            .split('\n').filter(Boolean).sort();
        assert.deepEqual(files, [...PROOF_FILES].sort(),
            'der Elternfall ist nur fuer einen reinen Zwei-Dateien-Beweiscommit erlaubt');
        return;
    }
    if (r.sourceHead === head) return;
    let candidateParent = '';
    try {
        candidateParent = git('rev-parse', `${r.sourceHead}^`);
    } catch {
        // An unknown object is not an isolated direct candidate.
    }
    assert.equal(candidateParent, head,
        `sourceHead ${r.sourceHead} ist weder HEAD noch dessen Elternfall oder direkter Kandidat`);
});

test('AC3: unmeasurable citations are excluded from both sides of the denominator', () => {
    const summary = citationComplianceOf([
        { check: { measured: true, ok: true } },
        { check: { measured: true, ok: false } },
        { check: { measured: false, ok: false } },
    ]);
    assert.deepEqual(summary, {
        citationCompliance: 0.5,
        citationMeasured: 2,
        citationUnmeasured: 1,
    });
});

test('AC4: every measured model records the citation denominator explicitly', () => {
    const evalcheck = JSON.parse(read(PROOF_FILES[0]));
    assert.ok(Array.isArray(evalcheck.measured) && evalcheck.measured.length >= 2);
    for (const model of evalcheck.measured) {
        assert.ok(Number.isInteger(model.citationMeasured) && model.citationMeasured > 0,
            `${model.name}: citationMeasured fehlt`);
        assert.ok(Number.isInteger(model.citationUnmeasured) && model.citationUnmeasured >= 0,
            `${model.name}: citationUnmeasured fehlt`);
        assert.equal(model.citationMeasured + model.citationUnmeasured, model.questions,
            `${model.name}: Nenner und unmessbare Antworten ergeben nicht alle Fragen`);
    }
});
