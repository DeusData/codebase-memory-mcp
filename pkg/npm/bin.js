#!/usr/bin/env node
'use strict';
// CLI shim: resolves the downloaded binary and replaces the current process with it.
// If the binary is missing (e.g. --ignore-scripts), attempts a one-time download.

const path = require('path');
const fs = require('fs');
const { spawnSync } = require('child_process');

const isWindows = process.platform === 'win32';
const binName = isWindows ? 'codebase-memory-mcp.exe' : 'codebase-memory-mcp';
const binPath = path.join(__dirname, 'bin', binName);

if (!fs.existsSync(binPath)) {
  // Binary missing — try running the install script (handles --ignore-scripts case)
  process.stderr.write('codebase-memory-mcp: binary not found, downloading...\n');
  const installResult = spawnSync(process.execPath, [path.join(__dirname, 'install.js')], {
    stdio: 'inherit',
  });
  if (installResult.status !== 0 || !fs.existsSync(binPath)) {
    process.stderr.write(
      'codebase-memory-mcp: download failed.\n' +
      'Try reinstalling: npm install -g codebase-memory-mcp\n'
    );
    process.exit(1);
  }
}

const result = spawnSync(binPath, process.argv.slice(2), {
  stdio: 'inherit',
  windowsHide: false,
});

if (result.error) {
  process.stderr.write(`codebase-memory-mcp: ${result.error.message}\n`);
  process.exit(1);
}

process.exit(result.status ?? 0);
