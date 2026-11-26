'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
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

test('postinstall refuses a non-loopback http CBM_DOWNLOAD_URL before any download', async () => {
  const installPath = path.join(__dirname, '..', 'install.js');
  const source = fs.readFileSync(installPath, 'utf8');
  // A fresh package directory: no cached binary, and nothing may be created.
  const packageDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-npm-override-test-'));
  const output = { stdout: '', stderr: '' };
  const observed = { exitCode: undefined };
  const fakeProcess = {
    platform: 'linux',
    arch: 'x64',
    env: { CBM_DOWNLOAD_URL: 'http://example.com/releases' },
    pid: 12345,
    stdout: { write: (text) => { output.stdout += String(text); } },
    stderr: { write: (text) => { output.stderr += String(text); } },
    exit: (code) => { observed.exitCode = code; },
    exitCode: undefined,
  };
  const refusingTransport = {
    get(url) { throw new Error(`unexpected request to ${url}`); },
  };

  try {
    const sandbox = {
      Buffer,
      URL,
      __dirname: packageDir,
      __filename: path.join(packageDir, 'install.js'),
      clearTimeout,
      console,
      exports: {},
      module: { exports: {} },
      process: fakeProcess,
      require: (specifier) => {
        if (specifier === './package.json') return { version: '0.0.0-test' };
        if (specifier === 'https' || specifier === 'http') return refusingTransport;
        return require(specifier);
      },
      setTimeout,
    };

    vm.runInNewContext(source, sandbox, { filename: installPath });
    await new Promise((resolve) => setImmediate(resolve));

    assert.match(
      output.stderr,
      /install failed .*Refusing non-HTTPS download URL \(CBM_DOWNLOAD_URL\): http:\/\/example\.com\/releases/,
    );
    assert.equal(observed.exitCode, 1);
    assert.doesNotMatch(output.stdout, /downloading/);
    assert.deepEqual(fs.readdirSync(packageDir), []);
  } finally {
    fs.rmSync(packageDir, { recursive: true, force: true });
  }
});
