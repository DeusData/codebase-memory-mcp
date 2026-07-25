'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

class ExitSignal extends Error {
  constructor(code) {
    super(`process.exit(${code})`);
    this.code = code;
  }
}

function runShim(targetPlatform, arguments_, childStatus) {
  const shimPath = path.join(__dirname, '..', 'bin.js');
  const source = fs.readFileSync(shimPath, 'utf8').replace(/^#![^\n]*\n/, '');
  const calls = [];
  let stderr = '';

  const fakeProcess = {
    platform: targetPlatform,
    argv: ['node.exe', shimPath, ...arguments_],
    execPath: 'node.exe',
    stderr: { write: (text) => { stderr += String(text); } },
    exit: (code) => { throw new ExitSignal(code); },
  };
  const fakeFs = {
    existsSync: () => true,
  };
  const fakeChildProcess = {
    spawnSync: (executable, args, options) => {
      calls.push({ executable, args, options });
      return { status: calls.length === 1 ? 0 : childStatus };
    },
  };
  const sandbox = {
    __dirname: path.dirname(shimPath),
    __filename: shimPath,
    Buffer,
    clearTimeout,
    console,
    exports: {},
    module: { exports: {} },
    process: fakeProcess,
    require: (specifier) => {
      if (specifier === 'fs') return fakeFs;
      if (specifier === 'child_process') return fakeChildProcess;
      return require(specifier);
    },
    setTimeout,
  };

  let exitCode = null;
  try {
    vm.runInNewContext(source, sandbox, { filename: shimPath });
  } catch (error) {
    if (!(error instanceof ExitSignal)) throw error;
    exitCode = error.code;
  }
  return { calls, exitCode, stderr };
}

test('Windows npm shim probes and executes the cached launcher', () => {
  const observed = runShim('win32', ['--version'], 0);

  assert.equal(observed.exitCode, 0);
  assert.equal(observed.calls.length, 2);
  for (const call of observed.calls) {
    assert.equal(path.basename(call.executable), 'codebase-memory-mcp.exe');
    assert.notEqual(
      path.basename(call.executable),
      'codebase-memory-mcp.payload.exe',
    );
  }
  assert.deepEqual(Array.from(observed.calls[1].args), ['--version']);
  assert.equal(observed.calls[1].options.stdio, 'inherit');
});

test('Windows npm shim keeps package-manager guidance after launcher refusal', () => {
  const observed = runShim('win32', ['update'], 1);

  assert.equal(observed.exitCode, 1);
  assert.equal(path.basename(observed.calls[1].executable), 'codebase-memory-mcp.exe');
  assert.match(observed.stderr, /npm install codebase-memory-mcp@latest/);
  assert.match(observed.stderr, /install --yes/);
});

test('non-Windows npm shim keeps its native payload execution path', () => {
  const observed = runShim('darwin', ['--version'], 0);

  assert.equal(observed.exitCode, 0);
  assert.equal(observed.calls.length, 1);
  assert.equal(path.basename(observed.calls[0].executable), 'codebase-memory-mcp');
});

test('PowerShell install mutation runs through the downloaded launcher', () => {
  const installer = fs.readFileSync(
    path.join(__dirname, '..', '..', '..', 'install.ps1'),
    'utf8',
  );

  assert.match(
    installer,
    /^\s*\$candidateVersion\s*=\s*&\s*\$DownloadedLauncher\s+--version\b/m,
  );
  assert.match(
    installer,
    /^\s*&\s*\$DownloadedLauncher\s+@InstallArgs\b/m,
  );
  assert.doesNotMatch(
    installer,
    /^\s*&\s*\$DownloadedPayload\s+@InstallArgs\b/m,
  );
});
