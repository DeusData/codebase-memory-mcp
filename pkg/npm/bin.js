#!/usr/bin/env node
'use strict';
// CLI shim: resolves the downloaded binary and replaces the current process with it.
// If the binary is missing (e.g. --ignore-scripts), attempts a one-time download.

const path = require('path');
const fs = require('fs');
const { spawnSync } = require('child_process');

const isWindows = process.platform === 'win32';
const windowsLauncherName = 'codebase-memory-mcp.exe';
// npm is a portable one-shot wrapper. On Windows the public .exe in the
// release archive is the permanent launcher, so privately validate the
// release pair, cache both files, and enter the native process through that
// launcher. The adjacent payload remains the immutable cache readiness signal.
const binName = isWindows ? 'codebase-memory-mcp.payload.exe' : 'codebase-memory-mcp';
const binPath = path.join(__dirname, 'bin', binName);
const launcherPath = path.join(__dirname, 'bin', windowsLauncherName);
const executionPath = isWindows ? launcherPath : binPath;
const cacheReady = () => {
  if (!fs.existsSync(binPath) || (isWindows && !fs.existsSync(launcherPath))) {
    return false;
  }
  if (!isWindows) return true;
  // The launcher resolving --version through its adjacent payload validates
  // both halves of the immutable Windows cache pair.
  const probe = spawnSync(launcherPath, ['--version'], {
    stdio: 'ignore',
    timeout: 15_000,
    windowsHide: true,
  });
  return !probe.error && probe.status === 0;
};

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

function portableMutationAction(args) {
  for (const argument of args) {
    if (['cli', 'hook-augment', 'config', 'install', '--help', '-h', '--version'].includes(argument)) {
      return null;
    }
    if (argument === 'update' || argument === 'uninstall') return argument;
  }
  return null;
}

const result = spawnSync(executionPath, process.argv.slice(2), {
  stdio: 'inherit',
  windowsHide: false,
});

if (result.error) {
  process.stderr.write(`codebase-memory-mcp: ${result.error.message}\n`);
  process.exit(1);
}

const mutation = isWindows && result.status !== 0
  ? portableMutationAction(process.argv.slice(2))
  : null;
if (mutation) {
  const packageCommand = mutation === 'update'
    ? 'npm install codebase-memory-mcp@latest'
    : 'npm uninstall codebase-memory-mcp';
  process.stderr.write(
    `This npm Windows copy is portable. Use "${packageCommand}" for package ` +
    'maintenance (add -g for a global install), or run ' +
    '"codebase-memory-mcp install --yes" once to create ' +
    'a managed launcher with coordinated self-update/uninstall.\n',
  );
}

process.exit(result.status ?? 0);
