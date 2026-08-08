#!/usr/bin/env node
'use strict';
// CLI shim: resolves the downloaded binary and replaces the current process with it.
// If the binary is missing (e.g. --ignore-scripts), attempts a one-time download.

const path = require('path');
const os = require('os');
const { spawn, spawnSync } = require('child_process');
const {
  acquireRuntimeLock,
  refreshRuntimeLock,
  releaseRuntimeLock,
  runtimeSetReady,
  runtimeSetReadyLocked,
} = require('./install.js');

const isWindows = process.platform === 'win32';
const variant = (process.env.CBM_VARIANT || '').toLowerCase() === 'ui'
  ? 'ui'
  : 'standard';
// npm is a portable one-shot wrapper. Every platform ships exactly one binary,
// which is executed directly beside its authenticated runtime sidecars.
const binName = isWindows ? 'codebase-memory-mcp.exe' : 'codebase-memory-mcp';
const binDir = path.join(__dirname, 'bin', variant);
const binPath = path.join(binDir, binName);
const executionPath = binPath;
const cacheReady = () => runtimeSetReadyLocked(binDir, binName, variant);

function main() {
const requestedMutation = runtimeMutationAction(process.argv.slice(2));
if (requestedMutation === 'update') {
  printUpdateGuidance();
  process.exit(2);
}
if (!cacheReady()) {
  // Binary missing — try running the install script (handles --ignore-scripts case)
  process.stderr.write('codebase-memory-mcp: binary not found, downloading...\n');
  const installResult = spawnSync(process.execPath, [path.join(__dirname, 'install.js')], {
    stdio: 'inherit',
  });
  if (installResult.status !== 0 || !cacheReady()) {
    process.stderr.write(
      'codebase-memory-mcp: download failed.\n' +
      'Try reinstalling: npm install -g codebase-memory-mcp\n'
    );
    process.exit(1);
  }
}

function runtimeMutationAction(args) {
  for (const argument of args) {
    if (argument === '--help' || argument === '-h' || argument === '--version') {
      return null;
    }
    if (argument.startsWith('-')) continue;
    if (argument === 'install' || argument === 'update' || argument === 'uninstall') {
      return argument;
    }
    if (argument === 'cli' || argument === 'hook-augment' || argument === 'config') {
      return null;
    }
    return null;
  }
  return null;
}

function defaultManagedInstallDir() {
  if (!isWindows) return path.join(os.homedir(), '.local', 'bin');
  const localAppData = process.env.LOCALAPPDATA ||
    path.join(os.homedir(), 'AppData', 'Local');
  return path.join(localAppData, 'Programs', 'codebase-memory-mcp');
}

function nativeArgs(args) {
  const result = [...args];
  if (runtimeMutationAction(result) === 'uninstall' &&
      !result.some((argument) => argument === '--dir' || argument.startsWith('--dir='))) {
    result.push('--dir', defaultManagedInstallDir());
  }
  return result;
}

function printUpdateGuidance() {
  process.stderr.write(
    'This npm copy is maintained by npm. Update it with ' +
    '"npm install codebase-memory-mcp@latest" (add -g for a global install). ' +
    'For a managed standalone installation, run ' +
    '"codebase-memory-mcp install --yes".\n',
  );
}

const forwardedArgs = nativeArgs(process.argv.slice(2));
const mutation = requestedMutation;

if (mutation === 'install' || mutation === 'uninstall') {
  const lock = acquireRuntimeLock(binDir);
  let heartbeatError = null;
  try {
    refreshRuntimeLock(lock);
    if (!runtimeSetReady(binDir, binName, variant)) {
      throw new Error('cached runtime assets changed before mutation launch');
    }
  } catch (error) {
    try { releaseRuntimeLock(lock); } catch (_) { /* preserve first error */ }
    process.stderr.write(`codebase-memory-mcp: ${error.message}\n`);
    process.exit(1);
  }
  let child = null;
  const heartbeat = setInterval(() => {
    try {
      refreshRuntimeLock(lock);
    } catch (error) {
      heartbeatError = error;
      if (child) child.kill();
    }
  }, 20_000);
  child = spawn(executionPath, forwardedArgs, {
    stdio: 'inherit',
    windowsHide: false,
  });
  child.on('error', (error) => {
    clearInterval(heartbeat);
    try { releaseRuntimeLock(lock); } catch (_) { /* report spawn error */ }
    process.stderr.write(`codebase-memory-mcp: ${error.message}\n`);
    process.exit(1);
  });
  child.on('close', (status) => {
    clearInterval(heartbeat);
    let releaseError = heartbeatError;
    try { releaseRuntimeLock(lock); } catch (error) { releaseError ||= error; }
    if (releaseError) {
      process.stderr.write(`codebase-memory-mcp: ${releaseError.message}\n`);
      process.exit(1);
    }
    if (isWindows && status !== 0 && mutation === 'uninstall') {
      process.stderr.write(
        'This npm Windows copy is portable. Use "npm uninstall codebase-memory-mcp" ' +
        'for package maintenance (add -g for a global install). ' +
        'To create or repair a managed standalone installation, run ' +
        '"codebase-memory-mcp install --yes".\n',
      );
    }
    process.exit(status ?? 0);
  });
  return;
}

const result = spawnSync(executionPath, forwardedArgs, {
  stdio: 'inherit',
  windowsHide: false,
});

if (result.error) {
  process.stderr.write(`codebase-memory-mcp: ${result.error.message}\n`);
  process.exit(1);
}

process.exit(result.status ?? 0);
}

main();
