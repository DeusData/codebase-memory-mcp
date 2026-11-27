'use strict';

const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const { CANDIDATE_TIMEOUT_MS } = require('../install.js');

// A slow host can need far more than 15 s to run a fresh binary's --version
// (#2877), while a candidate that never answers must still release the start.
// The Go wrapper (candidateVerifyBound) and the PyPI wrapper use the same bound.
test('candidate verification waits minutes, not seconds', () => {
  assert.ok(CANDIDATE_TIMEOUT_MS >= 120_000, `bound is ${CANDIDATE_TIMEOUT_MS} ms`);
});

test('the launcher probes a cached binary with the same bound', () => {
  const bin = fs.readFileSync(path.join(__dirname, '..', 'bin.js'), 'utf8');
  const declared = bin.match(/const CANDIDATE_TIMEOUT_MS = ([\d_]+);/);
  assert.ok(declared, 'bin.js declares its own CANDIDATE_TIMEOUT_MS');
  assert.strictEqual(Number(declared[1].replace(/_/g, '')), CANDIDATE_TIMEOUT_MS);
  assert.match(bin, /timeout: CANDIDATE_TIMEOUT_MS/);
  assert.doesNotMatch(bin, /timeout:\s*\d/);
});
