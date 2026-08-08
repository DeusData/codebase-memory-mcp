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
const { pipeline } = require('stream');

const REPO = 'DeusData/codebase-memory-mcp';
const VERSION = require('./package.json').version;
const BIN_ROOT = path.join(__dirname, 'bin');
const MAX_REDIRECTS = 5;
const DOWNLOAD_HOP_TIMEOUT_MS = 120_000;
const CANDIDATE_TIMEOUT_MS = 15_000;
const MAX_CHECKSUM_MANIFEST_BYTES = 1024 * 1024;
const WINDOWS_BINARY_NAME = 'codebase-memory-mcp.exe';
const INTEGRATIONS_NAME = 'cbm-integrations.json';
const UI_PACK_PATTERN = /^cbm-ui-[0-9a-f]{64}\.pack$/;
const UNIX_ARCHIVE_NAMES = [
  'codebase-memory-mcp',
  INTEGRATIONS_NAME,
  'LICENSE',
  'install.sh',
  'THIRD_PARTY_NOTICES.md',
];
const WINDOWS_ARCHIVE_NAMES = [
  WINDOWS_BINARY_NAME,
  INTEGRATIONS_NAME,
  'LICENSE',
  'install.ps1',
  'THIRD_PARTY_NOTICES.md',
];
const RUNTIME_LOCK_NAME = '.codebase-memory-mcp-runtime.lock';
const RUNTIME_LOCK_WAIT_MS = 45_000;
const RUNTIME_OWNERLESS_STALE_MS = 30_000;
const RUNTIME_LOCK_LEASE_MS = 300_000;
const RUNTIME_LOCK_HEARTBEAT_MS = 20_000;
const RUNTIME_LEGACY_STALE_MS = 3_600_000;
const RUNTIME_LOCK_POLL_MS = 25;
const RUNTIME_BACKUP_PREFIX = '.cbm-runtime-backup-';
const RUNTIME_BACKUP_DIRECTORY_PATTERN = /^\.cbm-runtime-backup-[0-9a-f]{32}$/;
const RUNTIME_BACKUP_RETIRED_MARKER = '.retirement-complete';
const RUNTIME_BACKUP_CLEANUP_MARKER = '.cleanup-only';
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

function validateExactTarMemberListing(listing, expectedNames, ui = false) {
  if (listing.includes('\0')) {
    throw new Error('tar member listing contains a NUL byte');
  }
  const members = listing.split(/\r?\n/);
  if (members[members.length - 1] === '') members.pop();
  const expected = new Set(expectedNames);
  const seen = new Set();
  let uiPackName = null;
  for (const member of members) {
    const isUiPack = ui && UI_PACK_PATTERN.test(member);
    if (!member || (!expected.has(member) && !isUiPack) || seen.has(member) ||
        (isUiPack && uiPackName !== null)) {
      throw new Error(`archive contains an unexpected or duplicate root member: ${JSON.stringify(member)}`);
    }
    seen.add(member);
    if (isUiPack) uiPackName = member;
  }
  const expectedCount = expectedNames.length + (ui ? 1 : 0);
  if (members.length !== expectedCount || seen.size !== expectedCount ||
      expectedNames.some((name) => !seen.has(name)) || (ui && uiPackName === null)) {
    throw new Error(
      `archive must contain exactly these root files${ui ? ' plus one content-addressed UI pack' : ''}: ${expectedNames.join(', ')}`,
    );
  }
  return uiPackName;
}

function validateRegularTarMemberListing(listing, expectedCount) {
  const entries = listing.split(/\r?\n/).filter((line) => line.length > 0);
  if (entries.length !== expectedCount ||
      entries.some((line) => line[0] !== '-')) {
    throw new Error('archive members must be independent regular files (no links)');
  }
}

function extractExactTarArchive(
  archivePath, destPath, expectedNames, targetNames, ui = false,
  runFile = execFileSync,
) {
  const listing = runFile('tar', ['-tzf', archivePath], {
    encoding: 'utf8',
    maxBuffer: MAX_CHECKSUM_MANIFEST_BYTES,
    windowsHide: true,
  });
  const uiPackName = validateExactTarMemberListing(listing, expectedNames, ui);
  const verboseListing = runFile('tar', ['-tvzf', archivePath], {
    encoding: 'utf8',
    maxBuffer: MAX_CHECKSUM_MANIFEST_BYTES,
    windowsHide: true,
  });
  validateRegularTarMemberListing(
    verboseListing, expectedNames.length + (ui ? 1 : 0),
  );
  const names = Array.isArray(targetNames) ? [...targetNames] : [targetNames];
  if (uiPackName !== null) names.push(uiPackName);
  // Extract only the authenticated runtime set. Even a malformed tar
  // implementation cannot write an unvalidated non-runtime member.
  runFile('tar', ['-xzf', archivePath, '-C', destPath, ...names], {
    stdio: 'inherit',
    windowsHide: true,
  });
  return names;
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
      res.on('error', (err) => finish(err));
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

      let received = 0;
      res.on('data', (chunk) => {
        received += chunk.length;
        if (maxBytes && received > maxBytes) {
          res.destroy(new Error(`Download exceeds the ${maxBytes}-byte safety limit`));
        }
      });
      const file = fs.createWriteStream(dest, { flags: 'w' });
      pipeline(res, file, (err) => {
        finish(err, { redirect: null });
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
  execFileSync(candidatePath, ['--verify-runtime-assets'], {
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

function sleepSync(milliseconds) {
  Atomics.wait(SLEEP_WORD, 0, 0, milliseconds);
}

function runtimeVariant(env = process.env) {
  return (env.CBM_VARIANT || '').toLowerCase() === 'ui' ? 'ui' : 'standard';
}

function cacheDirForVariant(variant) {
  return path.join(BIN_ROOT, variant);
}

function requireSafeRuntimeDirectory(directory) {
  const status = fs.lstatSync(directory);
  if (!status.isDirectory() || status.isSymbolicLink()) {
    throw new Error(`refusing unsafe package-cache directory: ${directory}`);
  }
}

function regularFile(pathname) {
  try {
    const status = fs.lstatSync(pathname);
    return status.isFile() && !status.isSymbolicLink() && status.nlink === 1;
  } catch (_) {
    return false;
  }
}

function uiPackMatchesDigest(directory, name) {
  try {
    const expected = name.slice('cbm-ui-'.length, -'.pack'.length);
    return fileSha256(path.join(directory, name)) === expected;
  } catch (_) {
    return false;
  }
}

function runtimeSetNames(directory, binaryName, variant) {
  if (!regularFile(path.join(directory, binaryName)) ||
      !regularFile(path.join(directory, INTEGRATIONS_NAME))) {
    return null;
  }
  let packLikeNames;
  try {
    packLikeNames = fs.readdirSync(directory)
      .filter((name) => name.startsWith('cbm-ui-') && name.endsWith('.pack'));
  } catch (_) {
    return null;
  }
  const validPackNames = packLikeNames.filter(
    (name) => UI_PACK_PATTERN.test(name) &&
      regularFile(path.join(directory, name)) &&
      uiPackMatchesDigest(directory, name),
  );
  if (variant === 'ui') {
    if (packLikeNames.length !== 1 || validPackNames.length !== 1) {
      return null;
    }
  } else if (packLikeNames.length !== 0) {
    return null;
  }
  return [
    INTEGRATIONS_NAME,
    ...(variant === 'ui' ? validPackNames : []),
    binaryName,
  ];
}

function runtimeSetReady(
  directory, binaryName, variant, verifier = verifyCandidate,
) {
  if (runtimeSetNames(directory, binaryName, variant) === null) return false;
  try {
    verifier(path.join(directory, binaryName));
    return true;
  } catch (_) {
    return false;
  }
}

function runtimeSetReadyLocked(
  directory, binaryName, variant, verifier = verifyCandidate,
) {
  fs.mkdirSync(directory, { recursive: true, mode: 0o755 });
  requireSafeRuntimeDirectory(directory);
  const lock = acquireRuntimeLock(directory);
  try {
    refreshRuntimeLock(lock);
    reconcileRuntimeBackups(
      directory, binaryName, variant, verifier, lock,
    );
    return runtimeSetReady(directory, binaryName, variant, verifier);
  } finally {
    releaseRuntimeLock(lock);
  }
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

function lockPathStatus(lockPath) {
  try {
    return fs.lstatSync(lockPath, { bigint: true });
  } catch (_) {
    return null;
  }
}

function sameLockObject(left, right) {
  return left !== null && right !== null &&
    left.dev === right.dev && left.ino === right.ino &&
    left.isFile() === right.isFile() && left.isDirectory() === right.isDirectory();
}

function readRuntimeLockOwner(lockPath) {
  try {
    const status = fs.lstatSync(lockPath);
    if (status.isSymbolicLink() || (!status.isFile() && !status.isDirectory())) {
      return null;
    }
    const ownerPath = status.isDirectory()
      ? path.join(lockPath, 'owner.json')
      : lockPath;
    const owner = JSON.parse(
      fs.readFileSync(ownerPath, 'utf8'),
    );
    if (!Number.isSafeInteger(owner.pid) || owner.pid <= 0 ||
        typeof owner.token !== 'string' || !/^[0-9a-f]{32}$/.test(owner.token)) {
      return null;
    }
    const leaseExpiresMs = Number.isSafeInteger(owner.lease_expires_ms)
      ? owner.lease_expires_ms
      : null;
    return { ...owner, lease_expires_ms: leaseExpiresMs };
  } catch (_) {
    return null;
  }
}

function restoreDisplacedRuntimeLock(reclaimed, lockPath, status) {
  if (!status || !status.isFile()) return;
  try {
    fs.linkSync(reclaimed, lockPath);
    fs.unlinkSync(reclaimed);
  } catch (_) {
    // A new owner already holds the canonical name. Preserve the displaced
    // object for its owner to diagnose; never delete an unverified successor.
  }
}

function tryReclaimRuntimeLock(lockPath, contenderToken) {
  const observedStatus = lockPathStatus(lockPath);
  if (observedStatus === null) return true;
  if (observedStatus.isSymbolicLink() ||
      (!observedStatus.isFile() && !observedStatus.isDirectory())) {
    return false;
  }
  let reclaim;
  const owner = readRuntimeLockOwner(lockPath);
  if (owner) {
    reclaim = !processIsAlive(owner.pid);
  } else {
    reclaim = Date.now() - Number(observedStatus.mtimeMs) >= RUNTIME_OWNERLESS_STALE_MS;
  }
  if (!reclaim) return false;

  const reclaimed = `${lockPath}.reclaimed-${contenderToken}`;
  try {
    fs.renameSync(lockPath, reclaimed);
  } catch (err) {
    if (err.code === 'ENOENT') return true;
    return false;
  }
  const movedStatus = lockPathStatus(reclaimed);
  if (!sameLockObject(observedStatus, movedStatus)) {
    restoreDisplacedRuntimeLock(reclaimed, lockPath, movedStatus);
    return false;
  }
  const movedOwner = readRuntimeLockOwner(reclaimed);
  const stillReclaimable = movedOwner === null
    ? Date.now() - Number(movedStatus.mtimeMs) >= RUNTIME_OWNERLESS_STALE_MS
    : !processIsAlive(movedOwner.pid);
  if (!stillReclaimable) {
    restoreDisplacedRuntimeLock(reclaimed, lockPath, movedStatus);
    return false;
  }
  if (movedStatus.isDirectory()) {
    // Legacy directory locks are accepted only as real, non-symlink
    // directories, then quarantined and removed after identity revalidation.
    fs.rmSync(reclaimed, { recursive: true, force: true });
  } else {
    fs.unlinkSync(reclaimed);
  }
  return true;
}

function writeRuntimeLockOwner(fd, token) {
  const record = Buffer.from(`${JSON.stringify({
    pid: process.pid,
    token,
    lease_expires_ms: Date.now() + RUNTIME_LOCK_LEASE_MS,
  })}\n`, 'utf8');
  fs.ftruncateSync(fd, 0);
  let offset = 0;
  while (offset < record.length) {
    offset += fs.writeSync(fd, record, offset, record.length - offset, offset);
  }
  fs.fsyncSync(fd);
}

function assertRuntimeLockOwner(lock) {
  const descriptorStatus = fs.fstatSync(lock.fd, { bigint: true });
  const canonicalStatus = lockPathStatus(lock.lockPath);
  const owner = readRuntimeLockOwner(lock.lockPath);
  if (!sameLockObject(descriptorStatus, canonicalStatus) || !owner ||
      owner.token !== lock.token || owner.pid !== process.pid) {
    throw new Error('package-cache publication lock ownership changed');
  }
}

function refreshRuntimeLock(lock) {
  assertRuntimeLockOwner(lock);
  writeRuntimeLockOwner(lock.fd, lock.token);
  assertRuntimeLockOwner(lock);
}

function acquireRuntimeLock(destDir, beforeClaimPublish = null) {
  const lockPath = path.join(destDir, RUNTIME_LOCK_NAME);
  const token = crypto.randomBytes(16).toString('hex');
  const claimPath = `${lockPath}.claim-${token}`;
  const deadline = Date.now() + RUNTIME_LOCK_WAIT_MS;
  for (;;) {
    let fd = null;
    try {
      fd = fs.openSync(claimPath, 'wx', 0o600);
      try {
        writeRuntimeLockOwner(fd, token);
        if (beforeClaimPublish) beforeClaimPublish(lockPath);
        fs.linkSync(claimPath, lockPath);
        fs.unlinkSync(claimPath);
        const descriptorStatus = fs.fstatSync(fd, { bigint: true });
        if (!sameLockObject(descriptorStatus, lockPathStatus(lockPath))) {
          throw new Error('package-cache publication lock ownership changed');
        }
      } catch (err) {
        fs.closeSync(fd);
        fd = null;
        try { fs.unlinkSync(claimPath); } catch (_) { /* unique claim is gone */ }
        throw err;
      }
      return { lockPath, token, fd };
    } catch (err) {
      if (fd !== null) {
        try { fs.closeSync(fd); } catch (_) { /* already closed */ }
      }
      if (err.code !== 'EEXIST') throw err;
      if (tryReclaimRuntimeLock(lockPath, token)) continue;
      if (Date.now() >= deadline) {
        throw new Error('timed out waiting for package-cache publication lock');
      }
      sleepSync(RUNTIME_LOCK_POLL_MS);
    }
  }
}

function releaseRuntimeLock(lock) {
  try {
    assertRuntimeLockOwner(lock);
  } catch (error) {
    try { fs.closeSync(lock.fd); } catch (_) { /* already closed */ }
    throw error;
  }
  const released = `${lock.lockPath}.released-${lock.token}`;
  try {
    fs.renameSync(lock.lockPath, released);
  } catch (error) {
    fs.closeSync(lock.fd);
    throw error;
  }
  const descriptorStatus = fs.fstatSync(lock.fd, { bigint: true });
  const releasedStatus = lockPathStatus(released);
  const owner = readRuntimeLockOwner(released);
  if (!sameLockObject(descriptorStatus, releasedStatus) || !owner ||
      owner.token !== lock.token || owner.pid !== process.pid) {
    restoreDisplacedRuntimeLock(released, lock.lockPath, releasedStatus);
    fs.closeSync(lock.fd);
    throw new Error('package-cache publication lock ownership changed');
  }
  try {
    fs.unlinkSync(released);
  } finally {
    fs.closeSync(lock.fd);
  }
}

function pathMatchesDigest(candidatePath, expectedDigest) {
  try {
    const status = fs.lstatSync(candidatePath);
    return status.isFile() && !status.isSymbolicLink() && status.nlink === 1 &&
      fileSha256(candidatePath) === expectedDigest;
  } catch (_) {
    return false;
  }
}

function copyRuntimeStageWithLease(source, staged, lock) {
  const input = fs.openSync(source, 'r');
  let output = null;
  const buffer = Buffer.allocUnsafe(1024 * 1024);
  let lastRefresh = Date.now();
  try {
    output = fs.openSync(staged, 'wx', 0o600);
    for (;;) {
      const count = fs.readSync(input, buffer, 0, buffer.length, null);
      if (count === 0) break;
      let offset = 0;
      while (offset < count) {
        offset += fs.writeSync(output, buffer, offset, count - offset, null);
      }
      if (Date.now() - lastRefresh >= RUNTIME_LOCK_HEARTBEAT_MS) {
        refreshRuntimeLock(lock);
        lastRefresh = Date.now();
      }
    }
    fs.fsyncSync(output);
  } catch (error) {
    try { fs.unlinkSync(staged); } catch (_) { /* never published */ }
    throw error;
  } finally {
    fs.closeSync(input);
    if (output !== null) fs.closeSync(output);
  }
}

// A grouped backup is a tiny crash journal. Before the retired marker, only
// moved members are restored; after it, partial published targets are replaced.
// cleanup-only is written only after the destination is accepted or restored,
// so a second crash can finish deletion without interpreting a partial journal.
function runtimeBackupTargetName(name, binaryName) {
  return name === binaryName || name === INTEGRATIONS_NAME ||
    (name.startsWith('cbm-ui-') && name.endsWith('.pack'));
}

function createRuntimeBackupDirectory(destDir) {
  for (let attempt = 0; attempt < 128; attempt += 1) {
    const token = crypto.randomBytes(16).toString('hex');
    const backupDir = path.join(destDir, `${RUNTIME_BACKUP_PREFIX}${token}`);
    try {
      fs.mkdirSync(backupDir, { mode: 0o700 });
      return backupDir;
    } catch (err) {
      if (!err || err.code !== 'EEXIST') throw err;
    }
  }
  throw new Error('could not reserve a package-cache backup transaction');
}

function createRuntimeBackupMarker(backupDir, markerName) {
  const marker = path.join(backupDir, markerName);
  const fd = fs.openSync(marker, 'wx', 0o600);
  try {
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
}

function inspectRuntimeBackupDirectory(destDir, entryName, binaryName) {
  const backupDir = path.join(destDir, entryName);
  const directoryStatus = fs.lstatSync(backupDir);
  if (!directoryStatus.isDirectory() || directoryStatus.isSymbolicLink()) {
    throw new Error(`refusing unsafe package-cache backup: ${backupDir}`);
  }
  const backup = {
    path: backupDir,
    retired: false,
    cleanupOnly: false,
    files: new Map(),
  };
  for (const name of fs.readdirSync(backupDir).sort()) {
    const candidate = path.join(backupDir, name);
    const status = fs.lstatSync(candidate);
    if (name === RUNTIME_BACKUP_RETIRED_MARKER ||
        name === RUNTIME_BACKUP_CLEANUP_MARKER) {
      if (!status.isFile() || status.isSymbolicLink() ||
          status.nlink !== 1 || status.size !== 0) {
        throw new Error(`refusing unsafe package-cache backup marker: ${candidate}`);
      }
      if (name === RUNTIME_BACKUP_RETIRED_MARKER) backup.retired = true;
      if (name === RUNTIME_BACKUP_CLEANUP_MARKER) backup.cleanupOnly = true;
      continue;
    }
    if (!runtimeBackupTargetName(name, binaryName) ||
        !status.isFile() || status.isSymbolicLink() || status.nlink !== 1) {
      throw new Error(`refusing unsafe package-cache backup member: ${candidate}`);
    }
    backup.files.set(name, {
      path: candidate,
      digest: fileSha256(candidate),
    });
  }
  return backup;
}

function listRuntimeBackupDirectories(destDir, binaryName) {
  return fs.readdirSync(destDir)
    .filter((name) => RUNTIME_BACKUP_DIRECTORY_PATTERN.test(name))
    .sort()
    .map((name) => inspectRuntimeBackupDirectory(destDir, name, binaryName));
}

function currentRuntimeTargets(destDir, binaryName) {
  const targets = new Map();
  const names = fs.readdirSync(destDir)
    .filter((name) => runtimeBackupTargetName(name, binaryName))
    .sort();
  for (const name of names) {
    const target = path.join(destDir, name);
    const status = fs.lstatSync(target);
    if (!status.isFile() || status.isSymbolicLink() || status.nlink !== 1) {
      throw new Error(`refusing unsafe package-cache target: ${target}`);
    }
    targets.set(name, target);
  }
  return targets;
}

function cleanupRuntimeBackup(backup, lock) {
  assertRuntimeLockOwner(lock);
  if (!backup.cleanupOnly) {
    createRuntimeBackupMarker(backup.path, RUNTIME_BACKUP_CLEANUP_MARKER);
    backup.cleanupOnly = true;
  }
  for (const name of [...backup.files.keys()].sort()) {
    assertRuntimeLockOwner(lock);
    const member = backup.files.get(name);
    if (!member || !pathMatchesDigest(member.path, member.digest)) {
      throw new Error(`package-cache backup member changed during cleanup: ${name}`);
    }
    assertRuntimeLockOwner(lock);
    fs.unlinkSync(member.path);
  }
  if (backup.retired) {
    assertRuntimeLockOwner(lock);
    fs.unlinkSync(path.join(backup.path, RUNTIME_BACKUP_RETIRED_MARKER));
  }
  assertRuntimeLockOwner(lock);
  fs.unlinkSync(path.join(backup.path, RUNTIME_BACKUP_CLEANUP_MARKER));
  fs.rmdirSync(backup.path);
}

function copyRuntimeBackupFile(member, target, executable, lock) {
  const staged = path.join(
    path.dirname(target),
    `.cbm-runtime-recovery-${crypto.randomBytes(16).toString('hex')}`,
  );
  try {
    copyRuntimeStageWithLease(member.path, staged, lock);
    fs.chmodSync(staged, executable ? 0o755 : 0o644);
    if (fileSha256(staged) !== member.digest) {
      throw new Error('package-cache backup changed while it was being restored');
    }
    assertRuntimeLockOwner(lock);
    fs.renameSync(staged, target);
  } finally {
    try { fs.unlinkSync(staged); } catch (_) { /* renamed or never completed */ }
  }
}

function reconcileRuntimeBackups(
  destDir, binaryName, variant, verifier, lock,
) {
  refreshRuntimeLock(lock);
  let backups = listRuntimeBackupDirectories(destDir, binaryName);
  for (const backup of backups.filter((candidate) => candidate.cleanupOnly)) {
    cleanupRuntimeBackup(backup, lock);
  }
  backups = backups.filter((candidate) => !candidate.cleanupOnly);
  if (backups.length === 0) return;

  if (runtimeSetReady(destDir, binaryName, variant, verifier)) {
    for (const backup of backups) cleanupRuntimeBackup(backup, lock);
    return;
  }
  if (backups.length === 0) return;

  for (const backup of backups.filter(
    (candidate) => !candidate.retired && candidate.files.size === 0,
  )) {
    cleanupRuntimeBackup(backup, lock);
  }
  backups = backups.filter(
    (candidate) => candidate.retired || candidate.files.size !== 0,
  );
  if (backups.length === 0) return;
  if (backups.length !== 1) {
    throw new Error('multiple package-cache backup transactions require manual recovery');
  }

  const backup = backups[0];
  const targets = currentRuntimeTargets(destDir, binaryName);
  if (!backup.retired) {
    for (const [name, member] of backup.files.entries()) {
      const target = targets.get(name);
      if (target && !pathMatchesDigest(target, member.digest)) {
        throw new Error(`package-cache backup conflicts with current target: ${name}`);
      }
    }
  } else {
    const binaryTarget = targets.get(binaryName);
    if (binaryTarget) {
      assertRuntimeLockOwner(lock);
      fs.unlinkSync(binaryTarget);
      targets.delete(binaryName);
    }
    for (const name of [...targets.keys()].sort()) {
      assertRuntimeLockOwner(lock);
      fs.unlinkSync(targets.get(name));
      targets.delete(name);
    }
  }

  const restoreNames = [...backup.files.keys()]
    .filter((name) => name !== binaryName)
    .sort();
  if (backup.files.has(binaryName)) restoreNames.push(binaryName);
  for (const name of restoreNames) {
    const member = backup.files.get(name);
    const target = path.join(destDir, name);
    if (!pathMatchesDigest(target, member.digest)) {
      copyRuntimeBackupFile(member, target, name === binaryName, lock);
    }
  }
  for (const [name, member] of backup.files.entries()) {
    if (!pathMatchesDigest(path.join(destDir, name), member.digest)) {
      throw new Error(`package-cache backup restoration failed: ${name}`);
    }
  }
  cleanupRuntimeBackup(backup, lock);
}

function publishRuntimeSetWithRecovery(
  sourceDir, destDir, binaryName, variant, verifier = verifyCandidate,
) {
  const sourceNames = runtimeSetNames(sourceDir, binaryName, variant);
  if (sourceNames === null) {
    throw new Error('source release does not contain a complete runtime set');
  }
  // Authenticate the source binary before it can contend for the cache.
  verifier(path.join(sourceDir, binaryName));
  fs.mkdirSync(destDir, { recursive: true });
  requireSafeRuntimeDirectory(destDir);
  const lock = acquireRuntimeLock(destDir);
  let operationError = null;
  try {
    // A contender may have completed while this process waited. Its complete
    // runtime set wins and is never moved or deleted.
    refreshRuntimeLock(lock);
    reconcileRuntimeBackups(destDir, binaryName, variant, verifier, lock);
    if (runtimeSetReady(destDir, binaryName, variant, verifier)) return;

    const publicationToken = crypto.randomBytes(16).toString('hex');
    const stagedPaths = new Map();
    let backupDirectory = null;
    try {
      for (const name of sourceNames) {
        const staged = path.join(destDir, `.cbm-runtime-stage-${publicationToken}-${name}`);
        copyRuntimeStageWithLease(path.join(sourceDir, name), staged, lock);
        fs.chmodSync(staged, name === binaryName ? 0o755 : 0o644);
        stagedPaths.set(name, staged);
      }
      refreshRuntimeLock(lock);
      backupDirectory = createRuntimeBackupDirectory(destDir);

      const stalePackNames = fs.readdirSync(destDir)
        .filter((name) => name.startsWith('cbm-ui-') && name.endsWith('.pack'));
      const retireNames = [...new Set([
        binaryName,
        INTEGRATIONS_NAME,
        ...stalePackNames,
      ])];
      // Retire the binary first so an incomplete publication is never ready.
      for (const name of retireNames) {
        assertRuntimeLockOwner(lock);
        const target = path.join(destDir, name);
        const status = fs.lstatSync(target, { throwIfNoEntry: false });
        if (!status) continue;
        if (!status.isFile() || status.isSymbolicLink() || status.nlink !== 1) {
          throw new Error(`refusing unsafe package-cache target: ${target}`);
        }
        const backup = path.join(backupDirectory, name);
        fs.renameSync(target, backup);
      }
      assertRuntimeLockOwner(lock);
      createRuntimeBackupMarker(
        backupDirectory, RUNTIME_BACKUP_RETIRED_MARKER,
      );

      // Sidecars are committed first. The executable is the final readiness
      // signal, so no observer can accept a newly published partial set.
      for (const name of sourceNames) {
        assertRuntimeLockOwner(lock);
        const target = path.join(destDir, name);
        fs.renameSync(stagedPaths.get(name), target);
        stagedPaths.delete(name);
      }
      if (!runtimeSetReady(destDir, binaryName, variant, verifier)) {
        throw new Error('published package-cache runtime set failed verification');
      }
      assertRuntimeLockOwner(lock);
      cleanupRuntimeBackup(
        inspectRuntimeBackupDirectory(
          destDir, path.basename(backupDirectory), binaryName,
        ),
        lock,
      );
      return;
    } catch (err) {
      assertRuntimeLockOwner(lock);
      const preserveWinner = runtimeSetReady(
        destDir, binaryName, variant, verifier,
      );
      if (backupDirectory !== null) {
        try {
          reconcileRuntimeBackups(
            destDir, binaryName, variant, verifier, lock,
          );
        } catch (recoveryError) {
          throw new Error(
            `${err.message}; package-cache recovery failed: ${recoveryError.message}`,
          );
        }
      }
      if (preserveWinner) return;
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
      releaseRuntimeLock(lock);
    } catch (releaseError) {
      if (!operationError) throw releaseError;
    }
  }
}

function extractZipOnWindows(
  archivePath, destPath, requiredNames, extractNames, ui = false,
) {
  // -EncodedCommand is a constant program. Paths travel only through the child
  // environment and are consumed with -LiteralPath, so PowerShell never parses
  // user/TEMP path bytes as source code or wildcard syntax.
  const script = [
    "$ErrorActionPreference = 'Stop'",
    'Add-Type -AssemblyName System.IO.Compression.FileSystem',
    '$zip = [System.IO.Compression.ZipFile]::OpenRead($env:CBM_NPM_ARCHIVE_PATH)',
    "$seen = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)",
    "$requiredNames = @($env:CBM_NPM_REQUIRED_NAMES.Split('|'))",
    "$uiRequired = $env:CBM_NPM_UI_REQUIRED -eq '1'",
    '$uiPackName = $null',
    '$targetCounts = @{}',
    'foreach ($requiredName in $requiredNames) { $targetCounts[$requiredName] = 0 }',
    'try { foreach ($entry in $zip.Entries) { ' +
      "$name = $entry.FullName.Replace('\\', '/'); " +
      "$directory = $name.EndsWith('/'); " +
      "$segmentsPath = if ($directory) { $name.TrimEnd('/') } else { $name }; " +
      "$segments = @($segmentsPath.Split('/')); " +
      '$unixType = (($entry.ExternalAttributes -shr 16) -band 0xF000); ' +
      "if ([string]::IsNullOrEmpty($segmentsPath) -or $name.StartsWith('/') -or " +
      "$name.Contains(':') -or $segments -contains '' -or " +
      "$segments -contains '.' -or $segments -contains '..' -or " +
      "@($segments | Where-Object { $_.EndsWith('.') -or $_.EndsWith(' ') }).Count -gt 0 -or " +
      '($unixType -ne 0 -and $unixType -ne 0x8000)) { ' +
      "throw \"unsafe zip entry path: $($entry.FullName)\" }; " +
      'if (-not $seen.Add($segmentsPath)) { ' +
      "throw \"duplicate or case-conflicting zip entry: $($entry.FullName)\" }; " +
      "$isUiPack = $uiRequired -and ($name -cmatch '^cbm-ui-[0-9a-f]{64}\\.pack$'); " +
      '$isRequired = $false; ' +
      'foreach ($requiredName in $requiredNames) { ' +
      'if ($name -ceq $requiredName) { ' +
      '$isRequired = $true; ' +
      '$targetCounts[$requiredName] = $targetCounts[$requiredName] + 1 } }; ' +
      'if ($isUiPack) { if ($null -ne $uiPackName) { ' +
      'throw "archive contains more than one UI pack" }; $uiPackName = $name }; ' +
      'if ($directory -or (-not $isRequired -and -not $isUiPack)) { ' +
      'throw "archive contains an unexpected root member: $($entry.FullName)" } ' +
      '} } finally { $zip.Dispose() }',
    'foreach ($requiredName in $requiredNames) { ' +
      'if ($targetCounts[$requiredName] -ne 1) { ' +
      'throw "archive must contain exactly one $requiredName" } }',
    'if ($uiRequired -and $null -eq $uiPackName) { ' +
      'throw "archive must contain exactly one content-addressed UI pack" }',
    '$expectedCount = $requiredNames.Count; if ($uiRequired) { $expectedCount++ }',
    'if ($seen.Count -ne $expectedCount) { ' +
      'throw "archive does not match the exact release root-file allowlist" }',
    "$extractNames = @($env:CBM_NPM_EXTRACT_NAMES.Split('|'))",
    'if ($uiRequired) { $extractNames += $uiPackName }',
    '$extractZip = [System.IO.Compression.ZipFile]::OpenRead($env:CBM_NPM_ARCHIVE_PATH)',
    'try { foreach ($extractName in $extractNames) { ' +
      '$entry = @($extractZip.Entries | Where-Object { $_.FullName -ceq $extractName })[0]; ' +
      '[System.IO.Compression.ZipFileExtensions]::ExtractToFile(' +
      '$entry, (Join-Path $env:CBM_NPM_DEST_PATH $extractName), $false) ' +
      '} } finally { $extractZip.Dispose() }',
  ].join('; ');
  const encoded = Buffer.from(script, 'utf16le').toString('base64');
  execFileSync('powershell', [
    '-NoLogo', '-NoProfile', '-NonInteractive', '-EncodedCommand', encoded,
  ], {
    env: {
      ...process.env,
      CBM_NPM_ARCHIVE_PATH: archivePath,
      CBM_NPM_DEST_PATH: destPath,
      CBM_NPM_REQUIRED_NAMES: requiredNames.join('|'),
      CBM_NPM_EXTRACT_NAMES: extractNames.join('|'),
      CBM_NPM_UI_REQUIRED: ui ? '1' : '0',
    },
    stdio: 'inherit',
    windowsHide: true,
  });
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
  const selectedVariant = runtimeVariant();
  const cacheDir = cacheDirForVariant(selectedVariant);
  // Package-manager shims are portable one-shot instances: one cached binary
  // per platform, entered directly.
  const binName = platform === 'windows'
    ? WINDOWS_BINARY_NAME
    : 'codebase-memory-mcp';
  const extractedNames = [binName, INTEGRATIONS_NAME];
  const archiveNames = platform === 'windows'
    ? WINDOWS_ARCHIVE_NAMES
    : UNIX_ARCHIVE_NAMES;

  if (runtimeSetReadyLocked(cacheDir, binName, selectedVariant)) return;

  fs.mkdirSync(cacheDir, { recursive: true });

  // Linux ships a fully-static "-portable" build; the standard linux binary
  // dynamically links glibc 2.38+ and fails on older distros. macOS/Windows
  // have no such variant. Keep in sync with install.sh / pypi _cli.py / cli.c.
  const variant = platform === 'linux' ? '-portable' : '';
  // Opt into the UI build, whose verified asset pack is published beside the
  // binary, with CBM_VARIANT=ui. Default is the standard (headless) build.
  const ui = selectedVariant === 'ui' ? 'ui-' : '';
  const archive = `codebase-memory-mcp-${ui}${platform}-${arch}${variant}.${ext}`;
  const url = `https://github.com/${REPO}/releases/download/v${VERSION}/${archive}`;

  const uiLabel = ui ? '(ui) ' : '';
  process.stdout.write(`codebase-memory-mcp: downloading v${VERSION} ${uiLabel}for ${platform}/${arch}...\n`);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-install-'));
  const tmpArchive = path.join(tmpDir, `cbm.${ext}`);

  try {
    await download(url, tmpArchive);
    await verifyChecksum(tmpArchive, archive);

    // Validate the complete archive namespace, then extract only the runtime set.
    let runtimeNames;
    if (ext === 'tar.gz') {
      runtimeNames = extractExactTarArchive(
        tmpArchive, tmpDir, archiveNames, extractedNames,
        selectedVariant === 'ui',
      );
    } else {
      extractZipOnWindows(
        tmpArchive, tmpDir, archiveNames, extractedNames,
        selectedVariant === 'ui',
      );
      const packNames = fs.readdirSync(tmpDir).filter((name) => UI_PACK_PATTERN.test(name));
      runtimeNames = [
        ...extractedNames,
        ...(selectedVariant === 'ui' ? packNames : []),
      ];
    }

    // Validate extracted paths don't escape tmpDir (tar-slip defense).
    const resolvedTmpDir = path.resolve(tmpDir);
    const extractedPaths = new Map();
    for (const name of runtimeNames) {
      const extracted = path.join(tmpDir, name);
      const resolvedExtracted = path.resolve(extracted);
      if (!resolvedExtracted.startsWith(resolvedTmpDir + path.sep)) {
        throw new Error(`Path traversal detected in archive: ${name}`);
      }
      const status = fs.lstatSync(extracted, { throwIfNoEntry: false });
      if (!status || !status.isFile() || status.isSymbolicLink()) {
        throw new Error(`Required runtime member not found as a regular file: ${extracted}`);
      }
      fs.chmodSync(extracted, name === binName ? 0o755 : 0o644);
      extractedPaths.set(name, extracted);
    }

    verifyCandidate(extractedPaths.get(binName));
    publishRuntimeSetWithRecovery(
      tmpDir, cacheDir, binName, selectedVariant,
    );

    process.stdout.write('codebase-memory-mcp: ready.\n');
    if (platform === 'windows') {
      process.stdout.write(
        'Windows package cache is portable. Run "codebase-memory-mcp install --yes" ' +
        'to create the managed installation (use "npx codebase-memory-mcp install --yes" ' +
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
  INTEGRATIONS_NAME,
  UNIX_ARCHIVE_NAMES,
  UI_PACK_PATTERN,
  WINDOWS_BINARY_NAME,
  acquireRuntimeLock,
  cacheDirForVariant,
  extractExactTarArchive,
  publishRuntimeSetWithRecovery,
  releaseRuntimeLock,
  refreshRuntimeLock,
  runtimeSetNames,
  runtimeSetReady,
  runtimeSetReadyLocked,
  runtimeVariant,
  tryReclaimRuntimeLock,
  validateExactTarMemberListing,
  validateRegularTarMemberListing,
};
