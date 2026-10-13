#!/usr/bin/env node
'use strict';
// Postinstall script: downloads the platform-appropriate binary from GitHub Releases.
// Runs automatically via `postinstall` in package.json.

const https = require('https');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { execFileSync, spawn } = require('child_process');
const { pipeline } = require('stream');

const REPO = 'DeusData/codebase-memory-mcp';
const VERSION = require('./package.json').version;
const BIN_DIR = path.join(__dirname, 'bin');
const MAX_REDIRECTS = 5;
const DOWNLOAD_HOP_TIMEOUT_MS = 120_000;
const CANDIDATE_TIMEOUT_MS = 15_000;
const MAX_CHECKSUM_MANIFEST_BYTES = 1024 * 1024;
// Release archive resource limits. Keep in sync with the Go wrapper
// (pkg/go/cmd/codebase-memory-mcp/main.go) and the PyPI wrapper (_cli.py).
const MAX_RELEASE_ARCHIVE_BYTES = 256 * 1024 * 1024;
const MAX_ARCHIVE_MEMBERS = 64;
const MAX_ARCHIVE_MEMBER_BYTES = 256 * 1024 * 1024;
const MAX_ARCHIVE_EXPANDED_BYTES = 512 * 1024 * 1024;
const DEFAULT_ARCHIVE_LIMITS = Object.freeze({
  compressedBytes: MAX_RELEASE_ARCHIVE_BYTES,
  members: MAX_ARCHIVE_MEMBERS,
  memberBytes: MAX_ARCHIVE_MEMBER_BYTES,
  expandedBytes: MAX_ARCHIVE_EXPANDED_BYTES,
});
// Bytes of tar's own diagnostics kept for the failure message.
const MAX_TAR_STDERR_CHARS = 4096;
const WINDOWS_BINARY_NAME = 'codebase-memory-mcp.exe';
const UNIX_ARCHIVE_NAMES = [
  'codebase-memory-mcp',
  'LICENSE',
  'install.sh',
  'THIRD_PARTY_NOTICES.md',
];
const WINDOWS_ARCHIVE_NAMES = [
  WINDOWS_BINARY_NAME,
  'LICENSE',
  'install.ps1',
  'THIRD_PARTY_NOTICES.md',
];
const WINDOWS_PAIR_LOCK_NAME = '.codebase-memory-mcp-pair.lock';
const WINDOWS_PAIR_LOCK_WAIT_MS = 45_000;
const WINDOWS_PAIR_OWNERLESS_STALE_MS = 30_000;
const WINDOWS_PAIR_LOCK_POLL_MS = 25;
const SLEEP_WORD = new Int32Array(new SharedArrayBuffer(4));

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

// Security: only follow HTTPS URLs (defense-in-depth). Parse the URL instead
// of relying on a string prefix so every redirect is checked unambiguously.
function validateUrl(rawUrl) {
  let parsed;
  try {
    parsed = new URL(rawUrl);
  } catch (err) {
    throw new Error(`Invalid download URL ${rawUrl}: ${err.message}`);
  }
  if (parsed.protocol !== 'https:' || !parsed.hostname || parsed.username || parsed.password) {
    throw new Error(`Refusing non-HTTPS or credentialed URL: ${rawUrl}`);
  }
  return parsed.href;
}

function validateExactTarMemberListing(listing, expectedNames) {
  if (listing.includes('\0')) {
    throw new Error('tar member listing contains a NUL byte');
  }
  const members = listing.split(/\r?\n/);
  if (members[members.length - 1] === '') members.pop();
  const expected = new Set(expectedNames);
  const seen = new Set();
  for (const member of members) {
    if (!member || !expected.has(member) || seen.has(member)) {
      throw new Error(`archive contains an unexpected or duplicate root member: ${JSON.stringify(member)}`);
    }
    seen.add(member);
  }
  if (members.length !== expectedNames.length || seen.size !== expected.size) {
    throw new Error(
      `archive must contain exactly these root files: ${expectedNames.join(', ')}`,
    );
  }
}

function validateArchiveLimits(limits) {
  if (!limits || !(limits.compressedBytes > 0) || !(limits.members > 0) ||
      !(limits.memberBytes > 0) || !(limits.expandedBytes > 0)) {
    throw new Error('invalid archive resource safety limits');
  }
}

function requireCompressedArchiveWithinLimit(archivePath, limits) {
  const status = fs.statSync(archivePath);
  if (!status.isFile()) {
    throw new Error('release archive is not a regular file');
  }
  if (status.size > limits.compressedBytes) {
    throw new Error(
      `release archive exceeds the ${limits.compressedBytes}-byte compressed safety limit`,
    );
  }
}

// Streams one member through `tar -xzOf` into a file this call creates,
// counting every byte against the per-member limit. The file is removed only
// when this call created it: a pre-existing target fails untouched.
function extractTarMemberWithLimit(
  archivePath, target, memberName, limits, spawnFile = spawn,
) {
  return new Promise((resolve, reject) => {
    let fd;
    try {
      fd = fs.openSync(target, 'wx', 0o600);
    } catch (err) {
      reject(err);
      return;
    }
    let written = 0;
    let failure = null;
    let settled = false;
    let stderr = '';
    const finish = (err) => {
      if (settled) return;
      settled = true;
      try { fs.closeSync(fd); } catch (closeError) { err = err || closeError; }
      if (err) {
        try { fs.unlinkSync(target); } catch (_) { /* already gone */ }
        reject(err);
        return;
      }
      resolve(written);
    };
    const child = spawnFile('tar', ['-xzOf', archivePath, memberName], {
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
    });
    const fail = (err) => {
      if (failure) return;
      failure = err;
      child.kill();
    };
    child.stdout.on('data', (chunk) => {
      if (failure) return;
      written += chunk.length;
      if (written > limits.memberBytes) {
        fail(new Error(
          `archive member ${JSON.stringify(memberName)} exceeds the ${limits.memberBytes}-byte expanded safety limit`,
        ));
        return;
      }
      try {
        let offset = 0;
        while (offset < chunk.length) {
          offset += fs.writeSync(fd, chunk, offset, chunk.length - offset);
        }
      } catch (err) {
        fail(err);
      }
    });
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (text) => {
      if (stderr.length < MAX_TAR_STDERR_CHARS) stderr += text;
    });
    child.on('error', (err) => {
      failure = failure || err;
      // A process that never started (no tar on PATH) has no exit to wait for.
      if (child.pid === undefined) finish(failure);
    });
    child.on('close', (code, signal) => {
      const detail = stderr.trim();
      let err = failure;
      if (!err && signal) {
        err = new Error(
          `tar was terminated by ${signal} while extracting ${memberName}: ${detail}`,
        );
      } else if (!err && code !== 0) {
        err = new Error(
          `tar exited with status ${code} while extracting ${memberName}: ${detail}`,
        );
      } else if (!err && written === 0) {
        err = new Error(`archive member ${memberName} is empty`);
      }
      finish(err);
    });
  });
}

async function extractExactTarArchive(
  archivePath, destPath, expectedNames, targetName,
  runFile = execFileSync, limits = DEFAULT_ARCHIVE_LIMITS, spawnFile = spawn,
) {
  validateArchiveLimits(limits);
  requireCompressedArchiveWithinLimit(archivePath, limits);
  // The exact root namespace is the member bound: its four names lie well
  // within MAX_ARCHIVE_MEMBERS, which the zip path enforces by count.
  if (expectedNames.length > limits.members) {
    throw new Error(`archive namespace exceeds the ${limits.members}-member safety limit`);
  }
  const listing = runFile('tar', ['-tzf', archivePath], {
    encoding: 'utf8',
    maxBuffer: MAX_CHECKSUM_MANIFEST_BYTES,
    windowsHide: true,
  });
  validateExactTarMemberListing(listing, expectedNames);
  // Only the fixed root executable is ever written, through the counted
  // writer; the companions are listed but never extracted.
  const target = path.join(destPath, targetName);
  await extractTarMemberWithLimit(archivePath, target, targetName, limits, spawnFile);
  fs.chmodSync(target, 0o755);
}

function downloadHop(url, dest, maxBytes) {
  return new Promise((resolve, reject) => {
    let settled = false;
    let response = null;
    let timer = null;

    function finish(err, result) {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      if (err) reject(err);
      else resolve(result);
    }

    const req = https.get(url, (res) => {
      response = res;
      let piped = false;
      // Before the body is piped, a response error ends the hop directly;
      // afterwards the pipeline reports it, so the partial file is discarded
      // before the hop settles.
      res.on('error', (err) => { if (!piped) finish(err); });
      const redirectCodes = new Set([301, 302, 303, 307, 308]);
      if (redirectCodes.has(res.statusCode)) {
        const location = res.headers.location;
        if (!location) {
          res.destroy();
          finish(new Error(`Redirect with no location for ${url}`));
          return;
        }
        let next;
        try {
          next = validateUrl(new URL(location, url).href);
        } catch (err) {
          res.destroy();
          finish(err);
          return;
        }
        res.destroy();
        finish(null, { redirect: next });
        return;
      }
      if (res.statusCode !== 200) {
        res.destroy();
        finish(new Error(`HTTP ${res.statusCode} for ${url}`));
        return;
      }
      const declared = Number(res.headers['content-length']);
      if (maxBytes && Number.isFinite(declared) && declared > maxBytes) {
        res.destroy();
        finish(new Error(`Download exceeds the ${maxBytes}-byte safety limit`));
        return;
      }

      let received = 0;
      res.on('data', (chunk) => {
        received += chunk.length;
        if (maxBytes && received > maxBytes) {
          res.destroy(new Error(`Download exceeds the ${maxBytes}-byte safety limit`));
        }
      });
      const file = fs.createWriteStream(dest, { flags: 'w' });
      piped = true;
      pipeline(res, file, (err) => {
        if (!err) {
          finish(null, { redirect: null });
          return;
        }
        // Discard the partial file once its handle is closed (Windows refuses
        // to unlink an open file), then report the failure.
        const discard = () => {
          try { fs.unlinkSync(dest); } catch (_) { /* nothing was written */ }
          finish(err);
        };
        if (file.closed) discard();
        else file.once('close', discard);
      });
    });
    req.on('error', (err) => finish(err));
    timer = setTimeout(() => {
      const err = new Error(`Download hop timed out after ${DOWNLOAD_HOP_TIMEOUT_MS} ms: ${url}`);
      req.destroy(err);
      if (response) response.destroy(err);
    }, DOWNLOAD_HOP_TIMEOUT_MS);
  });
}

async function download(rawUrl, dest, maxBytes = 0) {
  let url = validateUrl(rawUrl);
  for (let redirects = 0; redirects <= MAX_REDIRECTS; redirects += 1) {
    const result = await downloadHop(url, dest, maxBytes);
    if (!result.redirect) return;
    if (redirects === MAX_REDIRECTS) throw new Error('Too many redirects');
    url = result.redirect;
  }
}

function parseExpectedChecksum(manifest, archiveName) {
  let expected = null;
  for (const line of manifest.split('\n')) {
    const fields = line.trim().split(/\s+/);
    if (fields.length < 2) continue;
    if (fields[1] !== archiveName && fields[1] !== `*${archiveName}`) continue;

    const digest = fields[0].toLowerCase();
    if (!/^[0-9a-f]{64}$/.test(digest)) {
      throw new Error(`Invalid SHA-256 checksum for ${archiveName}`);
    }
    if (expected !== null && expected !== digest) {
      throw new Error(`Conflicting SHA-256 checksums for ${archiveName}`);
    }
    expected = digest;
  }
  if (expected === null) {
    throw new Error(`No checksum for ${archiveName} in checksums.txt`);
  }
  return expected;
}

function verifyCandidate(candidatePath) {
  execFileSync(candidatePath, ['--version'], {
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout: CANDIDATE_TIMEOUT_MS,
    windowsHide: true,
  });
}

function fileSha256(candidatePath) {
  const digest = crypto.createHash('sha256');
  const fd = fs.openSync(candidatePath, 'r');
  const buffer = Buffer.allocUnsafe(64 * 1024);
  try {
    for (;;) {
      const count = fs.readSync(fd, buffer, 0, buffer.length, null);
      if (count === 0) break;
      digest.update(buffer.subarray(0, count));
    }
  } finally {
    fs.closeSync(fd);
  }
  return digest.digest('hex');
}

function filesEqualSha256(leftPath, rightPath) {
  try {
    const left = fs.statSync(leftPath);
    const right = fs.statSync(rightPath);
    return left.isFile()
      && right.isFile()
      && left.size === right.size
      && fileSha256(leftPath) === fileSha256(rightPath);
  } catch (_) {
    return false;
  }
}

function sleepSync(milliseconds) {
  Atomics.wait(SLEEP_WORD, 0, 0, milliseconds);
}

function processIsAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    // Access denial is conservative evidence of a live process. Only an
    // explicit missing-process result permits reclamation.
    return err && err.code !== 'ESRCH';
  }
}

function readBinaryLockOwner(lockPath) {
  try {
    const owner = JSON.parse(
      fs.readFileSync(path.join(lockPath, 'owner.json'), 'utf8'),
    );
    if (!Number.isSafeInteger(owner.pid) || owner.pid <= 0 ||
        typeof owner.token !== 'string' || !/^[0-9a-f]{32}$/.test(owner.token)) {
      return null;
    }
    return owner;
  } catch (_) {
    return null;
  }
}

function tryReclaimBinaryLock(lockPath, contenderToken) {
  let reclaim = false;
  const owner = readBinaryLockOwner(lockPath);
  if (owner) {
    reclaim = !processIsAlive(owner.pid);
  } else {
    try {
      const age = Date.now() - fs.statSync(lockPath).mtimeMs;
      reclaim = age >= WINDOWS_PAIR_OWNERLESS_STALE_MS;
    } catch (_) {
      return true; // The owner released it between observation and inspection.
    }
  }
  if (!reclaim) return false;

  const reclaimed = `${lockPath}.reclaimed-${contenderToken}`;
  try {
    fs.renameSync(lockPath, reclaimed);
  } catch (err) {
    if (err.code === 'ENOENT') return true;
    return false;
  }
  fs.rmSync(reclaimed, { recursive: true, force: true });
  return true;
}

function acquireWindowsBinaryLock(destDir) {
  const lockPath = path.join(destDir, WINDOWS_PAIR_LOCK_NAME);
  const token = crypto.randomBytes(16).toString('hex');
  const deadline = Date.now() + WINDOWS_PAIR_LOCK_WAIT_MS;
  for (;;) {
    try {
      fs.mkdirSync(lockPath, { mode: 0o700 });
      try {
        fs.writeFileSync(
          path.join(lockPath, 'owner.json'),
          `${JSON.stringify({ pid: process.pid, token })}\n`,
          { encoding: 'utf8', flag: 'wx', mode: 0o600 },
        );
      } catch (err) {
        fs.rmSync(lockPath, { recursive: true, force: true });
        throw err;
      }
      return { lockPath, token };
    } catch (err) {
      if (err.code !== 'EEXIST') throw err;
      if (tryReclaimBinaryLock(lockPath, token)) continue;
      if (Date.now() >= deadline) {
        throw new Error('timed out waiting for Windows package-cache publication lock');
      }
      sleepSync(WINDOWS_PAIR_LOCK_POLL_MS);
    }
  }
}

function releaseWindowsBinaryLock(lock) {
  const owner = readBinaryLockOwner(lock.lockPath);
  if (!owner || owner.token !== lock.token || owner.pid !== process.pid) {
    throw new Error('Windows package-cache publication lock ownership changed');
  }
  fs.unlinkSync(path.join(lock.lockPath, 'owner.json'));
  fs.rmdirSync(lock.lockPath);
}

function windowsBinaryReady(destDir, verifier = verifyCandidate) {
  const binary = path.join(destDir, WINDOWS_BINARY_NAME);
  try {
    const status = fs.lstatSync(binary);
    if (!status.isFile() || status.isSymbolicLink()) {
      return false;
    }
    verifier(binary);
    return true;
  } catch (_) {
    return false;
  }
}

function pathMatchesDigest(candidatePath, expectedDigest) {
  try {
    const status = fs.lstatSync(candidatePath);
    return status.isFile() && !status.isSymbolicLink() &&
      fileSha256(candidatePath) === expectedDigest;
  } catch (_) {
    return false;
  }
}

function removeOwnedPublishedBinary(destDir, publishedDigests) {
  for (const name of [WINDOWS_BINARY_NAME]) {
    const digest = publishedDigests.get(name);
    const target = path.join(destDir, name);
    if (digest && pathMatchesDigest(target, digest)) {
      try { fs.unlinkSync(target); } catch (_) { /* recovery continues below */ }
    }
  }
}

function restoreBinaryBackups(destDir, backups) {
  for (const name of [WINDOWS_BINARY_NAME]) {
    const backup = backups.get(name);
    const target = path.join(destDir, name);
    if (!backup || fs.existsSync(target)) continue;
    try { fs.renameSync(backup, target); } catch (_) { /* preserve backup */ }
  }
}

function installWindowsBinaryAtomically(sourceDir, destDir, verifier = verifyCandidate) {
  // Authenticate the source binary before it can contend for the cache.
  verifier(path.join(sourceDir, WINDOWS_BINARY_NAME));
  const lock = acquireWindowsBinaryLock(destDir);
  let operationError = null;
  try {
    // A contender may have completed while this process waited. Its executable
    // wins and is never moved or deleted.
    if (windowsBinaryReady(destDir, verifier)) return;

    const transaction = crypto.randomBytes(16).toString('hex');
    const stagedPaths = new Map();
    const stagedDigests = new Map();
    const backups = new Map();
    const publishedDigests = new Map();
    try {
      for (const name of [WINDOWS_BINARY_NAME]) {
        const staged = path.join(destDir, `.cbm-pair-stage-${transaction}-${name}`);
        fs.copyFileSync(path.join(sourceDir, name), staged, fs.constants.COPYFILE_EXCL);
        fs.chmodSync(staged, 0o755);
        stagedPaths.set(name, staged);
        stagedDigests.set(name, fileSha256(staged));
      }

      for (const name of [WINDOWS_BINARY_NAME]) {
        const target = path.join(destDir, name);
        const status = fs.lstatSync(target, { throwIfNoEntry: false });
        if (!status) continue;
        if (!status.isFile() || status.isSymbolicLink()) {
          throw new Error(`refusing unsafe Windows package-cache target: ${target}`);
        }
        const backup = path.join(destDir, `.cbm-pair-backup-${transaction}-${name}`);
        fs.renameSync(target, backup);
        backups.set(name, backup);
      }

      for (const name of [WINDOWS_BINARY_NAME]) {
        const target = path.join(destDir, name);
        fs.renameSync(stagedPaths.get(name), target);
        stagedPaths.delete(name);
        publishedDigests.set(name, stagedDigests.get(name));
      }
      if (!windowsBinaryReady(destDir, verifier)) {
        throw new Error('published Windows package-cache binary failed verification');
      }
      for (const backup of backups.values()) {
        try { fs.unlinkSync(backup); } catch (_) { /* valid binary is already committed */ }
      }
      return;
    } catch (err) {
      // If a non-cooperating contender nevertheless published a valid binary,
      // preserve that winner. Otherwise remove only our exact staged bytes and
      // restore the prior files when their target names are still absent.
      if (windowsBinaryReady(destDir, verifier)) {
        for (const backup of backups.values()) {
          try { fs.unlinkSync(backup); } catch (_) { /* winner remains authoritative */ }
        }
        return;
      }
      removeOwnedPublishedBinary(destDir, publishedDigests);
      restoreBinaryBackups(destDir, backups);
      throw err;
    } finally {
      for (const staged of stagedPaths.values()) {
        try { fs.unlinkSync(staged); } catch (_) { /* renamed or already absent */ }
      }
    }
  } catch (err) {
    operationError = err;
    throw err;
  } finally {
    try {
      releaseWindowsBinaryLock(lock);
    } catch (releaseError) {
      if (!operationError) throw releaseError;
    }
  }
}

function extractZipOnWindows(
  archivePath, destPath, requiredNames, extractNames,
  limits = DEFAULT_ARCHIVE_LIMITS, shell = 'powershell',
) {
  validateArchiveLimits(limits);
  requireCompressedArchiveWithinLimit(archivePath, limits);
  // -EncodedCommand is a constant program. Paths travel only through the child
  // environment and are consumed with -LiteralPath, so PowerShell never parses
  // user/TEMP path bytes as source code or wildcard syntax.
  const script = [
    "$ErrorActionPreference = 'Stop'",
    'Add-Type -AssemblyName System.IO.Compression.FileSystem',
    '$maxMembers = [int]$env:CBM_NPM_MAX_MEMBERS',
    '$maxMemberBytes = [long]$env:CBM_NPM_MAX_MEMBER_BYTES',
    '$maxExpandedBytes = [long]$env:CBM_NPM_MAX_EXPANDED_BYTES',
    '$memberCount = 0',
    '$declaredExpanded = [long]0',
    '$zip = [System.IO.Compression.ZipFile]::OpenRead($env:CBM_NPM_ARCHIVE_PATH)',
    "$seen = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)",
    "$requiredNames = @($env:CBM_NPM_REQUIRED_NAMES.Split('|'))",
    '$targetCounts = @{}',
    'foreach ($requiredName in $requiredNames) { $targetCounts[$requiredName] = 0 }',
    'try { foreach ($entry in $zip.Entries) { ' +
      '$memberCount = $memberCount + 1; ' +
      'if ($memberCount -gt $maxMembers) { ' +
      'throw "archive exceeds the $maxMembers-member safety limit" }; ' +
      'if ($entry.Length -gt $maxMemberBytes) { ' +
      'throw "archive member $($entry.FullName) exceeds the $maxMemberBytes-byte expanded safety limit" }; ' +
      'if ($declaredExpanded -gt ($maxExpandedBytes - $entry.Length)) { ' +
      'throw "archive exceeds the $maxExpandedBytes-byte aggregate expanded safety limit" }; ' +
      '$declaredExpanded = $declaredExpanded + $entry.Length; ' +
      "$name = $entry.FullName.Replace('\\', '/'); " +
      "$directory = $name.EndsWith('/'); " +
      "$segmentsPath = if ($directory) { $name.TrimEnd('/') } else { $name }; " +
      "$segments = @($segmentsPath.Split('/')); " +
      "if ([string]::IsNullOrEmpty($segmentsPath) -or $name.StartsWith('/') -or " +
      "$name.Contains(':') -or $segments -contains '' -or " +
      "$segments -contains '.' -or $segments -contains '..' -or " +
      "@($segments | Where-Object { $_.EndsWith('.') -or $_.EndsWith(' ') }).Count -gt 0) { " +
      "throw \"unsafe zip entry path: $($entry.FullName)\" }; " +
      'if (-not $seen.Add($segmentsPath)) { ' +
      "throw \"duplicate or case-conflicting zip entry: $($entry.FullName)\" }; " +
      'foreach ($requiredName in $requiredNames) { ' +
      'if ($name -ceq $requiredName) { ' +
      '$targetCounts[$requiredName] = $targetCounts[$requiredName] + 1 } } ' +
      '} } finally { $zip.Dispose() }',
    'foreach ($requiredName in $requiredNames) { ' +
      'if ($targetCounts[$requiredName] -ne 1) { ' +
      'throw "archive must contain exactly one $requiredName" } }',
    'if ($seen.Count -ne $requiredNames.Count) { ' +
      'throw "archive does not match the exact release root-file allowlist" }',
    "$extractNames = @($env:CBM_NPM_EXTRACT_NAMES.Split('|'))",
    '$actualExpanded = [long]0',
    '$buffer = New-Object byte[] 65536',
    '$extractZip = [System.IO.Compression.ZipFile]::OpenRead($env:CBM_NPM_ARCHIVE_PATH)',
    // Copy by hand with a byte counter: ExtractToFile would write whatever the
    // compressed stream yields, regardless of the declared size.
    'try { foreach ($extractName in $extractNames) { ' +
      '$entry = @($extractZip.Entries | Where-Object { $_.FullName -ceq $extractName })[0]; ' +
      '$targetPath = Join-Path $env:CBM_NPM_DEST_PATH $extractName; ' +
      '$remaining = $maxExpandedBytes - $actualExpanded; ' +
      '$copied = [long]0; ' +
      '$source = $entry.Open(); ' +
      '$target = [System.IO.File]::Open($targetPath, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write); ' +
      'try { while ($true) { ' +
      '$count = $source.Read($buffer, 0, $buffer.Length); ' +
      'if ($count -le 0) { break }; ' +
      '$copied = $copied + $count; ' +
      'if ($copied -gt $maxMemberBytes) { ' +
      'throw "archive member $extractName exceeds the $maxMemberBytes-byte actual expanded safety limit" }; ' +
      'if ($copied -gt $remaining) { ' +
      'throw "archive exceeds the $maxExpandedBytes-byte aggregate actual expanded safety limit" }; ' +
      '$target.Write($buffer, 0, $count) } ' +
      '} catch { $target.Dispose(); ' +
      'Remove-Item -LiteralPath $targetPath -Force -ErrorAction SilentlyContinue; throw ' +
      '} finally { $target.Dispose(); $source.Dispose() }; ' +
      'if ($copied -ne $entry.Length) { ' +
      'Remove-Item -LiteralPath $targetPath -Force -ErrorAction SilentlyContinue; ' +
      'throw "archive member $extractName actual size $copied does not match declared size $($entry.Length)" }; ' +
      '$actualExpanded = $actualExpanded + $copied ' +
      '} } finally { $extractZip.Dispose() }',
  ].join('; ');
  const encoded = Buffer.from(script, 'utf16le').toString('base64');
  try {
    execFileSync(shell, [
      '-NoLogo', '-NoProfile', '-NonInteractive', '-EncodedCommand', encoded,
    ], {
      env: {
        ...process.env,
        CBM_NPM_ARCHIVE_PATH: archivePath,
        CBM_NPM_DEST_PATH: destPath,
        CBM_NPM_REQUIRED_NAMES: requiredNames.join('|'),
        CBM_NPM_EXTRACT_NAMES: extractNames.join('|'),
        CBM_NPM_MAX_MEMBERS: String(limits.members),
        CBM_NPM_MAX_MEMBER_BYTES: String(limits.memberBytes),
        CBM_NPM_MAX_EXPANDED_BYTES: String(limits.expandedBytes),
      },
      encoding: 'utf8',
      stdio: ['ignore', 'inherit', 'pipe'],
      windowsHide: true,
    });
  } catch (err) {
    // Surface the script's own reason (for example which limit was hit)
    // instead of a bare "Command failed".
    const detail = err && err.stderr ? String(err.stderr).trim() : '';
    throw new Error(detail ? `zip extraction failed: ${detail}` : err.message);
  }
}

// Fetch checksums.txt and verify the archive hash.
async function verifyChecksum(archivePath, archiveName) {
  const url = `https://github.com/${REPO}/releases/download/v${VERSION}/checksums.txt`;
  const tmpChecksums = archivePath + '.checksums';
  try {
    await download(url, tmpChecksums, MAX_CHECKSUM_MANIFEST_BYTES);
    const manifestSize = fs.statSync(tmpChecksums).size;
    if (manifestSize > MAX_CHECKSUM_MANIFEST_BYTES) {
      throw new Error('checksums.txt exceeds the 1 MiB safety limit');
    }
    const expected = parseExpectedChecksum(
      fs.readFileSync(tmpChecksums, 'utf-8'), archiveName,
    );
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
  } finally {
    try { fs.unlinkSync(tmpChecksums); } catch (_) { /* ignore */ }
  }
}

async function main() {
  const platform = getPlatform();
  const arch = getArch();
  const ext = platform === 'windows' ? 'zip' : 'tar.gz';
  // Package-manager shims are portable one-shot instances: one cached binary
  // per platform, entered directly.
  const binName = platform === 'windows'
    ? WINDOWS_BINARY_NAME
    : 'codebase-memory-mcp';
  const binPath = path.join(BIN_DIR, binName);
  const cacheNames = platform === 'windows'
    ? [WINDOWS_BINARY_NAME]
    : [binName];
  const extractedNames = platform === 'windows'
    ? [WINDOWS_BINARY_NAME]
    : [binName];
  const archiveNames = platform === 'windows'
    ? WINDOWS_ARCHIVE_NAMES
    : UNIX_ARCHIVE_NAMES;
  const cachePaths = cacheNames.map((name) => path.join(BIN_DIR, name));

  if (platform === 'windows' && windowsBinaryReady(BIN_DIR)) return;
  if (platform !== 'windows' &&
      cachePaths.every((candidate) => fs.existsSync(candidate))) {
    try {
      verifyCandidate(binPath);
      return; // already installed and runnable, nothing to do
    } catch (_) {
      // Fall through and atomically replace the invalid binary.
    }
  }

  fs.mkdirSync(BIN_DIR, { recursive: true });

  // Linux ships a fully-static "-portable" build; the standard linux binary
  // dynamically links glibc 2.38+ and fails on older distros. macOS/Windows
  // have no such variant. Keep in sync with install.sh / pypi _cli.py / cli.c.
  const variant = platform === 'linux' ? '-portable' : '';
  // No UI/standard split since v0.10.0: one archive per platform, graph UI
  // always embedded. The old CBM_VARIANT=ui opt-in pointed at ui-* archives
  // that no longer exist, so honoring it could only 404 (#1538).
  const archive = `codebase-memory-mcp-${platform}-${arch}${variant}.${ext}`;
  const url = `https://github.com/${REPO}/releases/download/v${VERSION}/${archive}`;

  process.stdout.write(`codebase-memory-mcp: downloading v${VERSION} for ${platform}/${arch}...\n`);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-install-'));
  const tmpArchive = path.join(tmpDir, `cbm.${ext}`);

  try {
    await download(url, tmpArchive, MAX_RELEASE_ARCHIVE_BYTES);
    await verifyChecksum(tmpArchive, archive);

    // Validate the complete archive namespace, then extract only executables.
    if (ext === 'tar.gz') {
      await extractExactTarArchive(
        tmpArchive, tmpDir, archiveNames, binName,
      );
    } else {
      extractZipOnWindows(tmpArchive, tmpDir, archiveNames, extractedNames);
    }

    // Validate extracted paths don't escape tmpDir (tar-slip defense).
    const resolvedTmpDir = path.resolve(tmpDir);
    const extractedPaths = new Map();
    for (const name of extractedNames) {
      const extracted = path.join(tmpDir, name);
      const resolvedExtracted = path.resolve(extracted);
      if (!resolvedExtracted.startsWith(resolvedTmpDir + path.sep)) {
        throw new Error(`Path traversal detected in archive: ${name}`);
      }
      const status = fs.lstatSync(extracted, { throwIfNoEntry: false });
      if (!status || !status.isFile() || status.isSymbolicLink()) {
        throw new Error(`Required binary not found as a regular file: ${extracted}`);
      }
      fs.chmodSync(extracted, 0o755);
      extractedPaths.set(name, extracted);
    }

    if (platform === 'windows') {
      // The launcher resolves the adjacent portable payload in this directory.
      verifyCandidate(extractedPaths.get(WINDOWS_BINARY_NAME));
    } else {
      verifyCandidate(extractedPaths.get(binName));
    }

    if (platform === 'windows') {
      installWindowsBinaryAtomically(tmpDir, BIN_DIR);
    } else {
      const staged = path.join(
        BIN_DIR,
        `.${binName}.${process.pid}.${crypto.randomBytes(8).toString('hex')}.tmp`,
      );
      try {
        fs.copyFileSync(extractedPaths.get(binName), staged, fs.constants.COPYFILE_EXCL);
        fs.chmodSync(staged, 0o755);
        verifyCandidate(staged);
        try {
          fs.renameSync(staged, binPath);
        } catch (publishError) {
          if (!filesEqualSha256(staged, binPath)) throw publishError;
        }
        verifyCandidate(binPath);
      } finally {
        try { fs.unlinkSync(staged); } catch (_) { /* renamed or already absent */ }
      }
    }

    process.stdout.write('codebase-memory-mcp: ready.\n');
    if (platform === 'windows') {
      process.stdout.write(
        'Windows package cache is portable. Run "codebase-memory-mcp install --yes" ' +
        'to create the managed launcher (use "npx codebase-memory-mcp install --yes" ' +
        'for a local npm install). Package updates/removal remain ' +
        '"npm install codebase-memory-mcp@latest" and ' +
        '"npm uninstall codebase-memory-mcp" (add -g for a global install).\n',
      );
    }
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

if (require.main === module || module.parent == null) {
  main().catch((err) => {
    process.stderr.write(`\ncodebase-memory-mcp: install failed — ${err.message}\n`);
    process.stderr.write(`You can install manually: https://github.com/${REPO}#installation\n`);
    process.exit(1);
  });
}

module.exports = {
  DEFAULT_ARCHIVE_LIMITS,
  UNIX_ARCHIVE_NAMES,
  WINDOWS_ARCHIVE_NAMES,
  WINDOWS_BINARY_NAME,
  download,
  extractExactTarArchive,
  extractZipOnWindows,
  installWindowsBinaryAtomically,
  validateExactTarMemberListing,
  windowsBinaryReady,
};
