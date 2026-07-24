'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

test('postinstall exits nonzero when installation fails', async () => {
  const installPath = path.join(__dirname, '..', 'install.js');
  const source = fs.readFileSync(installPath, 'utf8');
  const output = { stdout: '', stderr: '' };
  const observed = { exitCode: undefined };
  const fakeProcess = {
    // Force a deterministic failure before any filesystem or network access.
    platform: 'unsupported-test-platform',
    arch: 'x64',
    env: {},
    pid: 12345,
    stdout: { write: (text) => { output.stdout += String(text); } },
    stderr: { write: (text) => { output.stderr += String(text); } },
    exit: (code) => { observed.exitCode = code; },
    exitCode: undefined,
  };

  const sandbox = {
    Buffer,
    URL,
    __dirname: path.dirname(installPath),
    __filename: installPath,
    clearTimeout,
    console,
    exports: {},
    module: { exports: {} },
    process: fakeProcess,
    require: (specifier) => {
      if (specifier === './package.json') return { version: '0.0.0-test' };
      return require(specifier);
    },
    setTimeout,
  };

  vm.runInNewContext(source, sandbox, { filename: installPath });
  await new Promise((resolve) => setImmediate(resolve));

  const exitCode = observed.exitCode ?? fakeProcess.exitCode ?? 0;
  assert.match(output.stderr, /install failed/);
  assert.notEqual(
    exitCode,
    0,
    'npm postinstall reported success after a deterministic installation failure',
  );
});
