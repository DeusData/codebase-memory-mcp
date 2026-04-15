#!/usr/bin/env node
'use strict';
// Postinstall script: downloads the platform-appropriate binary from GitHub Releases.
// Runs automatically via `postinstall` in package.json.

const https = require('https');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { execFileSync } = require('child_process');

const REPO = 'DeusData/codebase-memory-mcp';
const VERSION = require('./package.json').version;
const BIN_DIR = path.join(__dirname, 'bin');

function getPlatform() {
  switch (process.platform) {
    case 'linux':  return 'linux';
    case 'darwin': return 'darwin';
    case 'win32':  return 'windows';
    default: throw new Error(`Unsupported platform: ${process.platform}`);
  }
}

function getArch() {
  switch (process.arch) {
    case 'arm64': return 'arm64';
    case 'x64':   return 'amd64';
    default: throw new Error(`Unsupported architecture: ${process.arch}`);
  }
}

// Security: only follow HTTPS URLs (defense-in-depth).
function validateUrl(url) {
  if (!url.startsWith('https://')) {
    throw new Error(`Refusing non-HTTPS URL: ${url}`);
  }
}

function download(url, dest) {
  validateUrl(url);
  return new Promise((resolve, reject) => {
    function follow(u, depth) {
      if (depth > 5) return reject(new Error('Too many redirects'));
      validateUrl(u);
      https.get(u, (res) => {
        if (res.statusCode === 301 || res.statusCode === 302) {
          const loc = res.headers.location;
          if (!loc) return reject(new Error('Redirect with no location'));
          const next = loc.startsWith('/') ? new URL(loc, u).href : loc;
          return follow(next, depth + 1);
        }
        if (res.statusCode !== 200) {
          return reject(new Error(`HTTP ${res.statusCode} for ${u}`));
        }
        const file = fs.createWriteStream(dest);
        res.pipe(file);
        file.on('finish', () => file.close(resolve));
        file.on('error', reject);
      }).on('error', reject);
    }
    follow(url, 0);
  });
}

// Fetch checksums.txt and verify the archive hash.
async function verifyChecksum(archivePath, archiveName) {
  const url = `https://github.com/${REPO}/releases/download/v${VERSION}/checksums.txt`;
  const tmpChecksums = archivePath + '.checksums';
  try {
    await download(url, tmpChecksums);
    const lines = fs.readFileSync(tmpChecksums, 'utf-8').split('\n');
    const match = lines.find((l) => l.includes(archiveName));
    if (!match) return; // checksum line not found — non-fatal
    const expected = match.split(/\s+/)[0];
    const actual = crypto
      .createHash('sha256')
      .update(fs.readFileSync(archivePath))
      .digest('hex');
    if (expected !== actual) {
      throw new Error(
        `Checksum mismatch for ${archiveName}:\n  expected: ${expected}\n  actual:   ${actual}`,
      );
    }
    process.stdout.write('codebase-memory-mcp: checksum verified.\n');
  } catch (err) {
    if (err.message.startsWith('Checksum mismatch')) throw err;
    // Non-fatal: checksum unavailable (network issue, pre-release, etc.)
  } finally {
    try { fs.unlinkSync(tmpChecksums); } catch (_) { /* ignore */ }
  }
}

async function main() {
  const platform = getPlatform();
  const arch = getArch();
  const ext = platform === 'windows' ? 'zip' : 'tar.gz';
  const binName = platform === 'windows' ? 'codebase-memory-mcp.exe' : 'codebase-memory-mcp';
  const binPath = path.join(BIN_DIR, binName);

  if (fs.existsSync(binPath)) {
    return; // already installed, nothing to do
  }

  fs.mkdirSync(BIN_DIR, { recursive: true });

  const archive = `codebase-memory-mcp-${platform}-${arch}.${ext}`;
  const url = `https://github.com/${REPO}/releases/download/v${VERSION}/${archive}`;

  process.stdout.write(`codebase-memory-mcp: downloading v${VERSION} for ${platform}/${arch}...\n`);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-install-'));
  const tmpArchive = path.join(tmpDir, `cbm.${ext}`);

  try {
    await download(url, tmpArchive);
    await verifyChecksum(tmpArchive, archive);

    // Extract using execFileSync (array args — no shell injection).
    if (ext === 'tar.gz') {
      execFileSync('tar', ['-xzf', tmpArchive, '-C', tmpDir, '--no-same-owner']);
    } else {
      execFileSync('powershell', [
        '-NoProfile', '-Command',
        `Expand-Archive -Path '${tmpArchive}' -DestinationPath '${tmpDir}' -Force`,
      ]);
    }

    // Validate extracted path doesn't escape tmpDir (tar-slip defense).
    const extracted = path.join(tmpDir, binName);
    const resolvedExtracted = path.resolve(extracted);
    const resolvedTmpDir = path.resolve(tmpDir);
    if (!resolvedExtracted.startsWith(resolvedTmpDir + path.sep)) {
      throw new Error(`Path traversal detected in archive: ${binName}`);
    }
    if (!fs.existsSync(extracted)) {
      throw new Error(`Binary not found after extraction at ${extracted}`);
    }

    fs.copyFileSync(extracted, binPath);
    fs.chmodSync(binPath, 0o755);

    process.stdout.write('codebase-memory-mcp: ready.\n');
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

main().catch((err) => {
  process.stderr.write(`\ncodebase-memory-mcp: install failed — ${err.message}\n`);
  process.stderr.write(`You can install manually: https://github.com/${REPO}#installation\n`);
  // Non-fatal: don't block the rest of npm install
  process.exit(0);
});
