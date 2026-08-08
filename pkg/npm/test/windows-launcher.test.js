'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const UI_PACK_BYTES = Buffer.from('authenticated UI pack');
const UI_PACK_NAME = `cbm-ui-${crypto.createHash('sha256').update(UI_PACK_BYTES).digest('hex')}.pack`;

class ExitSignal extends Error {
  constructor(code) {
    super(`process.exit(${code})`);
    this.code = code;
  }
}

function runShim(targetPlatform, arguments_, childStatus, options = {}) {
  const shimPath = path.join(__dirname, '..', 'bin.js');
  const source = fs.readFileSync(shimPath, 'utf8').replace(/^#![^\n]*\n/, '');
  const calls = [];
  const lockEvents = [];
  let stderr = '';
  let installed = false;

  const fakeProcess = {
    platform: targetPlatform,
    env: {
      CBM_VARIANT: options.variant || '',
      LOCALAPPDATA: 'C:\\Users\\test\\AppData\\Local',
    },
    argv: ['node.exe', shimPath, ...arguments_],
    execPath: 'node.exe',
    stderr: { write: (text) => { stderr += String(text); } },
    exit: (code) => { throw new ExitSignal(code); },
  };
  const fakeFs = {
    lstatSync: (candidate) => {
      if (options.missingSidecar && !installed &&
          path.basename(candidate) === 'cbm-integrations.json') {
        const error = new Error('missing sidecar');
        error.code = 'ENOENT';
        throw error;
      }
      return { isFile: () => true, isSymbolicLink: () => false, nlink: 1 };
    },
    readFileSync: () => options.corruptPack && !installed
      ? Buffer.from('corrupt UI pack')
      : UI_PACK_BYTES,
    readdirSync: () => options.variant === 'ui'
      ? [UI_PACK_NAME]
      : [],
  };
  const fakeChildProcess = {
    spawn: (executable, args, options) => {
      calls.push({ executable, args, options });
      return {
        on: (event, callback) => {
          if (event === 'close') callback(childStatus);
          return this;
        },
      };
    },
    spawnSync: (executable, args, options) => {
      calls.push({ executable, args, options });
      if (executable === fakeProcess.execPath) {
        installed = true;
        return { status: 0 };
      }
      if (args[0] === '--verify-runtime-assets') return { status: 0 };
      return { status: childStatus };
    },
  };
  const fakeLifecycle = {
    acquireRuntimeLock: () => {
      lockEvents.push('acquire');
      return { lockPath: 'lock', token: 'token', fd: 1 };
    },
    refreshRuntimeLock: () => { lockEvents.push('refresh'); },
    releaseRuntimeLock: () => { lockEvents.push('release'); },
    runtimeSetReady: () => true,
    runtimeSetReadyLocked: (directory, binaryName) => {
      const structurallyReady = installed || (!options.missingSidecar && !options.corruptPack);
      if (!structurallyReady) return false;
      const probe = fakeChildProcess.spawnSync(
        path.join(directory, binaryName), ['--verify-runtime-assets'], { stdio: 'ignore' },
      );
      return probe.status === 0;
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
      if (specifier === './install.js') return fakeLifecycle;
      return require(specifier);
    },
    clearInterval: () => {},
    setInterval: () => 1,
    setTimeout,
  };

  let exitCode = null;
  try {
    vm.runInNewContext(source, sandbox, { filename: shimPath });
  } catch (error) {
    if (!(error instanceof ExitSignal)) throw error;
    exitCode = error.code;
  }
  return { calls, exitCode, lockEvents, stderr };
}

test('Windows npm shim authenticates assets and executes the cached binary', () => {
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
  assert.deepEqual(Array.from(observed.calls[0].args), ['--verify-runtime-assets']);
  assert.deepEqual(Array.from(observed.calls[1].args), ['--version']);
  assert.equal(observed.calls[1].options.stdio, 'inherit');
});

test('npm update is routed to the package manager without invoking absent install.sh', () => {
  const observed = runShim('win32', ['update'], 1);

  assert.equal(observed.exitCode, 2);
  assert.equal(observed.calls.length, 0, 'update guidance must not launch cached code');
  assert.match(observed.stderr, /npm install codebase-memory-mcp@latest/);
  assert.match(observed.stderr, /codebase-memory-mcp install --yes/);
  assert.doesNotMatch(observed.stderr, /install\.sh/);
});

test('nested update token is forwarded instead of treated as package update', () => {
  const observed = runShim('win32', ['daemon', 'update'], 0);

  assert.equal(observed.exitCode, 0);
  assert.equal(observed.calls.length, 2);
  assert.deepEqual(Array.from(observed.calls[1].args), ['daemon', 'update']);
  assert.doesNotMatch(observed.stderr, /npm install codebase-memory-mcp@latest/);
});

test('non-Windows npm shim executes its cached binary directly', () => {
  const observed = runShim('darwin', ['--version'], 0);

  assert.equal(observed.exitCode, 0);
  assert.equal(observed.calls.length, 2);
  assert.deepEqual(Array.from(observed.calls[0].args), ['--verify-runtime-assets']);
  assert.equal(path.basename(observed.calls[1].executable), 'codebase-memory-mcp');
});

test('npm shim selects a separate UI runtime set', () => {
  const observed = runShim('darwin', ['--version'], 0, { variant: 'ui' });

  assert.equal(observed.exitCode, 0);
  assert.equal(path.basename(path.dirname(observed.calls[1].executable)), 'ui');
});

test('npm shim repairs a cache whose integrations sidecar is missing', () => {
  const observed = runShim(
    'darwin', ['--version'], 0, { missingSidecar: true },
  );

  assert.equal(observed.exitCode, 0);
  assert.equal(observed.calls[0].executable, 'node.exe');
  assert.equal(path.basename(observed.calls[0].args[0]), 'install.js');
  assert.deepEqual(Array.from(observed.calls[1].args), ['--verify-runtime-assets']);
  assert.equal(path.basename(observed.calls[2].executable), 'codebase-memory-mcp');
});

test('npm shim repairs a UI cache whose pack digest does not match its name', () => {
  const observed = runShim(
    'darwin', ['--version'], 0, { variant: 'ui', corruptPack: true },
  );

  assert.equal(observed.exitCode, 0);
  assert.equal(observed.calls[0].executable, 'node.exe');
  assert.equal(path.basename(observed.calls[0].args[0]), 'install.js');
  assert.deepEqual(Array.from(observed.calls[1].args), ['--verify-runtime-assets']);
  assert.equal(path.basename(observed.calls[2].executable), 'codebase-memory-mcp');
});

test('wrapper uninstall locks the cache and targets the managed install by default', () => {
  const observed = runShim('win32', ['uninstall', '--yes'], 0);

  assert.equal(observed.exitCode, 0);
  assert.deepEqual(observed.lockEvents, ['acquire', 'refresh', 'release']);
  assert.deepEqual(
    Array.from(observed.calls[1].args),
    [
      'uninstall', '--yes', '--dir',
      'C:\\Users\\test\\AppData\\Local/Programs/codebase-memory-mcp',
    ],
  );
});

test('PowerShell install mutation runs through the downloaded binary', () => {
  const installer = fs.readFileSync(
    path.join(__dirname, '..', '..', '..', 'install.ps1'),
    'utf8',
  );

  assert.match(
    installer,
    /^\s*\$candidateVersion\s*=\s*&\s*\$DownloadedBinary\s+--version\b/m,
  );
  assert.match(
    installer,
    /^\s*&\s*\$DownloadedBinary\s+@InstallArgs\b/m,
  );
  // One binary ships per platform — no launcher/payload pair to resolve.
  assert.doesNotMatch(installer, /payload/i);
  // The candidate owns activation and rollback under the native guard. The
  // script must not mutate the installed binary before handing it the request.
  const handoff = installer.indexOf('& $DownloadedBinary @InstallArgs');
  assert.ok(handoff > 0, 'downloaded candidate handoff is missing');
  assert.doesNotMatch(
    installer.slice(0, handoff),
    /(?:Move-Item|Copy-Item|Remove-Item)[^\n]*(?:\$Dest|\$InstallDir[^\n]*\$BinName)/,
  );
});
