'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const {
  INTEGRATIONS_NAME,
  UNIX_ARCHIVE_NAMES,
  WINDOWS_BINARY_NAME,
  acquireRuntimeLock,
  cacheDirForVariant,
  extractExactTarArchive,
  publishRuntimeSetWithRecovery,
  runtimeSetReady,
  runtimeSetReadyLocked,
  releaseRuntimeLock,
  tryReclaimRuntimeLock,
  validateExactTarMemberListing,
  validateRegularTarMemberListing,
} = require('../install.js');

function exactUnixListing(extra = []) {
  return [...UNIX_ARCHIVE_NAMES, ...extra].join('\n') + '\n';
}

test('Unix archive validation rejects traversal and unexpected members', () => {
  assert.throws(
    () => validateExactTarMemberListing(
      exactUnixListing(['../../.ssh/authorized_keys']), UNIX_ARCHIVE_NAMES,
    ),
    /unexpected or duplicate/,
  );
  assert.throws(
    () => validateExactTarMemberListing(
      exactUnixListing(['unexpected-root-file']), UNIX_ARCHIVE_NAMES,
    ),
    /unexpected or duplicate/,
  );
});

test('release archives require the integrations sidecar', () => {
  assert.ok(
    UNIX_ARCHIVE_NAMES.includes(INTEGRATIONS_NAME),
    'the authenticated archive namespace must include the runtime integrations sidecar',
  );
});

test('UI Unix archive validation accepts exactly one content-addressed pack', () => {
  const pack = `cbm-ui-${'a'.repeat(64)}.pack`;
  assert.doesNotThrow(() => validateExactTarMemberListing(
    exactUnixListing([pack]), UNIX_ARCHIVE_NAMES, true,
  ));
  assert.throws(
    () => validateExactTarMemberListing(
      exactUnixListing([pack, `cbm-ui-${'b'.repeat(64)}.pack`]),
      UNIX_ARCHIVE_NAMES,
      true,
    ),
    /exactly one|unexpected or duplicate/,
  );
});

test('Unix extraction requests only the validated runtime set', () => {
  const calls = [];
  const runner = (command, args) => {
    calls.push({ command, args: [...args] });
    if (args[0] === '-tzf') return exactUnixListing();
    if (args[0] === '-tvzf') {
      return UNIX_ARCHIVE_NAMES.map(
        (name) => `-rw-r--r-- 0 owner group 1 Jan 1 00:00 ${name}`,
      ).join('\n') + '\n';
    }
    return Buffer.alloc(0);
  };

  extractExactTarArchive(
    '/tmp/release.tar.gz', '/tmp/extract', UNIX_ARCHIVE_NAMES,
    ['codebase-memory-mcp', INTEGRATIONS_NAME], false, runner,
  );

  assert.deepEqual(calls[0].args, ['-tzf', '/tmp/release.tar.gz']);
  assert.deepEqual(calls[1].args, ['-tvzf', '/tmp/release.tar.gz']);
  assert.deepEqual(
    calls[2].args,
    [
      '-xzf', '/tmp/release.tar.gz', '-C', '/tmp/extract',
      'codebase-memory-mcp', INTEGRATIONS_NAME,
    ],
  );
});

test('Unix archive validation rejects hardlink members', () => {
  const listing = UNIX_ARCHIVE_NAMES.map(
    (name, index) => `${index === 0 ? 'h' : '-'}rw-r--r-- ${name}`,
  ).join('\n') + '\n';
  assert.throws(
    () => validateRegularTarMemberListing(listing, UNIX_ARCHIVE_NAMES.length),
    /independent regular files/,
  );
});

function writeRuntimeSet(directory, tag, variant = 'standard') {
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, WINDOWS_BINARY_NAME), `binary:${tag}`);
  fs.writeFileSync(path.join(directory, INTEGRATIONS_NAME), `integrations:${tag}`);
  if (variant === 'ui') {
    const contents = Buffer.from(`pack:${tag}`);
    const digest = crypto.createHash('sha256').update(contents).digest('hex');
    fs.writeFileSync(
      path.join(directory, `cbm-ui-${digest}.pack`),
      contents,
    );
    return `cbm-ui-${digest}.pack`;
  }
  return null;
}

function fakeBinaryVerifier(binaryPath) {
  const binary = fs.readFileSync(binaryPath, 'utf8');
  if (!/^binary:(.+)$/.test(binary)) {
    throw new Error('cached Windows binary failed verification');
  }
}

function withBinaryDirectories(callback) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-npm-binary-test-'));
  const source = path.join(root, 'source');
  const destination = path.join(root, 'destination');
  fs.mkdirSync(destination);
  try {
    callback({ source, destination });
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

function assertBinary(directory, tag) {
  assert.equal(
    fs.readFileSync(path.join(directory, WINDOWS_BINARY_NAME), 'utf8'),
    `binary:${tag}`,
  );
  assert.equal(
    fs.readFileSync(path.join(directory, INTEGRATIONS_NAME), 'utf8'),
    `integrations:${tag}`,
  );
}

async function waitForCrashMarker(marker, child, stderr) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (fs.existsSync(marker)) return;
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(`publication helper exited before crash gate: ${stderr()}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`publication helper did not reach crash gate: ${stderr()}`);
}

function waitForChildExit(child) {
  return new Promise((resolve) => {
    child.once('exit', (code, signal) => resolve({ code, signal }));
  });
}

test('Windows publication repairs a corrupt cached binary', () => {
  withBinaryDirectories(({ source, destination }) => {
    writeRuntimeSet(source, 'candidate');
    writeRuntimeSet(destination, 'old');
    fs.writeFileSync(path.join(destination, WINDOWS_BINARY_NAME), 'corrupt');

    publishRuntimeSetWithRecovery(
      source, destination, WINDOWS_BINARY_NAME, 'standard', fakeBinaryVerifier,
    );

    assertBinary(destination, 'candidate');
  });
});

test('Windows publication installs into an empty cache', () => {
  withBinaryDirectories(({ source, destination }) => {
    writeRuntimeSet(source, 'candidate');

    publishRuntimeSetWithRecovery(
      source, destination, WINDOWS_BINARY_NAME, 'standard', fakeBinaryVerifier,
    );

    assertBinary(destination, 'candidate');
  });
});

test('Windows publication preserves a valid concurrent winner', () => {
  withBinaryDirectories(({ source, destination }) => {
    writeRuntimeSet(source, 'loser');
    writeRuntimeSet(destination, 'winner');

    publishRuntimeSetWithRecovery(
      source, destination, WINDOWS_BINARY_NAME, 'standard', fakeBinaryVerifier,
    );

    assertBinary(destination, 'winner');
  });
});

test('runtime readiness requires the complete variant-specific set', () => {
  withBinaryDirectories(({ destination }) => {
    writeRuntimeSet(destination, 'standard');
    assert.equal(
      runtimeSetReady(destination, WINDOWS_BINARY_NAME, 'standard', fakeBinaryVerifier),
      true,
    );
    fs.unlinkSync(path.join(destination, INTEGRATIONS_NAME));
    assert.equal(
      runtimeSetReady(destination, WINDOWS_BINARY_NAME, 'standard', fakeBinaryVerifier),
      false,
    );

    writeRuntimeSet(destination, 'ui', 'ui');
    assert.equal(
      runtimeSetReady(destination, WINDOWS_BINARY_NAME, 'ui', fakeBinaryVerifier),
      true,
    );
    fs.writeFileSync(
      path.join(destination, `cbm-ui-${'b'.repeat(64)}.pack`),
      'second pack',
    );
    assert.equal(
      runtimeSetReady(destination, WINDOWS_BINARY_NAME, 'ui', fakeBinaryVerifier),
      false,
    );
  });
});

test('UI readiness rejects and publication repairs a digest-mismatched pack', () => {
  withBinaryDirectories(({ source, destination }) => {
    const sourcePack = writeRuntimeSet(source, 'candidate', 'ui');
    const corruptPack = writeRuntimeSet(destination, 'old', 'ui');
    fs.writeFileSync(path.join(destination, corruptPack), 'corrupt pack bytes');

    assert.equal(
      runtimeSetReady(destination, WINDOWS_BINARY_NAME, 'ui', fakeBinaryVerifier),
      false,
      'a well-shaped pack filename must not authenticate different bytes',
    );
    publishRuntimeSetWithRecovery(
      source, destination, WINDOWS_BINARY_NAME, 'ui', fakeBinaryVerifier,
    );

    assertBinary(destination, 'candidate');
    assert.equal(fs.readFileSync(path.join(destination, sourcePack), 'utf8'), 'pack:candidate');
    assert.equal(fs.existsSync(path.join(destination, corruptPack)), false);
  });
});

test('standard and UI cache directories do not collide', () => {
  assert.notEqual(cacheDirForVariant('standard'), cacheDirForVariant('ui'));
});

test('runtime publication commits sidecars before the binary', () => {
  withBinaryDirectories(({ source, destination }) => {
    writeRuntimeSet(source, 'candidate', 'ui');
    const committed = [];
    const renameSync = fs.renameSync;
    fs.renameSync = (sourcePath, destinationPath) => {
      const name = path.basename(destinationPath);
      if (path.dirname(destinationPath) === destination &&
          (name === WINDOWS_BINARY_NAME || name === INTEGRATIONS_NAME ||
           /^cbm-ui-[0-9a-f]{64}\.pack$/.test(name))) {
        committed.push(name);
      }
      return renameSync(sourcePath, destinationPath);
    };
    try {
      publishRuntimeSetWithRecovery(
        source, destination, WINDOWS_BINARY_NAME, 'ui', fakeBinaryVerifier,
      );
    } finally {
      fs.renameSync = renameSync;
    }

    assert.equal(committed[0], INTEGRATIONS_NAME);
    assert.match(committed[1], /^cbm-ui-[0-9a-f]{64}\.pack$/);
    assert.equal(committed[2], WINDOWS_BINARY_NAME);
  });
});

test('a killed publisher is reconciled on the next locked readiness check', async () => {
  const helperSource = String.raw`
    'use strict';
    const fs = require('node:fs');
    const path = require('node:path');
    const [installPath, source, destination, marker, crashName] = process.argv.slice(1);
    const install = require(installPath);
    const renameSync = fs.renameSync;
    fs.renameSync = (sourcePath, destinationPath) => {
      const result = renameSync(sourcePath, destinationPath);
      if (destinationPath === path.join(destination, crashName)) {
        fs.writeFileSync(marker, 'reached\n');
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 60_000);
      }
      return result;
    };
    const verifier = (binaryPath) => {
      if (!/^binary:(.+)$/.test(fs.readFileSync(binaryPath, 'utf8'))) {
        throw new Error('test binary failed verification');
      }
    };
    install.publishRuntimeSetWithRecovery(
      source, destination, install.WINDOWS_BINARY_NAME, 'standard', verifier,
    );
  `;

  for (const [crashName, expectedReady] of [
    [INTEGRATIONS_NAME, false],
    [WINDOWS_BINARY_NAME, true],
  ]) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-npm-crash-test-'));
    const source = path.join(root, 'source');
    const destination = path.join(root, 'destination');
    const marker = path.join(root, 'crash-reached');
    fs.mkdirSync(destination);
    writeRuntimeSet(source, 'candidate');
    writeRuntimeSet(destination, 'old');
    fs.writeFileSync(
      path.join(destination, WINDOWS_BINARY_NAME), 'corrupt:old',
    );
    let stderr = '';
    const child = spawn(process.execPath, [
      '-e',
      helperSource,
      path.join(__dirname, '..', 'install.js'),
      source,
      destination,
      marker,
      crashName,
    ], { stdio: ['ignore', 'ignore', 'pipe'] });
    child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });
    const exitPromise = waitForChildExit(child);
    try {
      await waitForCrashMarker(marker, child, () => stderr);
      const backups = fs.readdirSync(destination)
        .filter((name) => name.startsWith('.cbm-runtime-backup-'));
      assert.equal(backups.length, 1, 'killed publisher did not retain one transaction');
      assert.equal(
        fs.lstatSync(path.join(destination, backups[0])).isDirectory(),
        true,
      );
      assert.equal(
        fs.existsSync(path.join(
          destination, backups[0], '.retirement-complete',
        )),
        true,
      );

      assert.equal(child.kill('SIGKILL'), true);
      const exit = await exitPromise;
      assert.ok(
        exit.signal !== null || exit.code !== 0,
        'publication helper was not killed',
      );

      const ready = runtimeSetReadyLocked(
          destination,
          WINDOWS_BINARY_NAME,
          'standard',
          fakeBinaryVerifier,
      );
      assert.equal(ready, expectedReady);
      if (!ready) {
        assert.equal(
          fs.readFileSync(path.join(destination, WINDOWS_BINARY_NAME), 'utf8'),
          'corrupt:old',
        );
        assert.equal(
          fs.readFileSync(path.join(destination, INTEGRATIONS_NAME), 'utf8'),
          'integrations:old',
        );
        publishRuntimeSetWithRecovery(
          source,
          destination,
          WINDOWS_BINARY_NAME,
          'standard',
          fakeBinaryVerifier,
        );
      }
      assertBinary(destination, 'candidate');
      assert.equal(
        fs.readdirSync(destination).some(
          (name) => name.startsWith('.cbm-runtime-backup-'),
        ),
        false,
      );
      assert.equal(
        fs.existsSync(path.join(destination, '.codebase-memory-mcp-runtime.lock')),
        false,
      );
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill('SIGKILL');
        await exitPromise;
      }
      fs.rmSync(root, { recursive: true, force: true });
    }
  }
});

test('a stalled creator never deletes a successor runtime lock', () => {
  withBinaryDirectories(({ destination }) => {
    let successor;
    let injected = false;
    assert.throws(
      () => acquireRuntimeLock(destination, () => {
        if (injected) return;
        injected = true;
        successor = acquireRuntimeLock(destination);
        throw new Error('injected stalled creator abort');
      }),
      /injected stalled creator abort/,
    );

    assert.ok(successor, 'the successor must acquire the reclaimed lock');
    assert.ok(
      fs.existsSync(successor.lockPath),
      'the failed creator must not recursively remove its successor',
    );
    releaseRuntimeLock(successor);
  });
});

test('runtime readiness rejects multiply-linked cache leaves', () => {
  withBinaryDirectories(({ destination }) => {
    writeRuntimeSet(destination, 'linked');
    fs.linkSync(
      path.join(destination, INTEGRATIONS_NAME),
      path.join(destination, 'integrations-hardlink'),
    );
    assert.equal(
      runtimeSetReady(destination, WINDOWS_BINARY_NAME, 'standard', fakeBinaryVerifier),
      false,
    );
  });
});

test('orphan reconciliation rejects multiply-linked backup members', () => {
  withBinaryDirectories(({ destination }) => {
    const backup = path.join(
      destination, `.cbm-runtime-backup-${'a'.repeat(32)}`,
    );
    fs.mkdirSync(backup);
    fs.writeFileSync(path.join(backup, '.retirement-complete'), '');
    const member = path.join(backup, WINDOWS_BINARY_NAME);
    fs.writeFileSync(member, 'binary:old');
    fs.linkSync(member, path.join(destination, 'backup-hardlink-copy'));

    assert.throws(
      () => runtimeSetReadyLocked(
        destination,
        WINDOWS_BINARY_NAME,
        'standard',
        fakeBinaryVerifier,
      ),
      /unsafe package-cache backup member/,
    );
    assert.equal(fs.existsSync(backup), true, 'unsafe backup was mutated');
    assert.equal(
      fs.existsSync(path.join(destination, '.codebase-memory-mcp-runtime.lock')),
      false,
    );
  });
});

test('an expired lease never reclaims a live owner', () => {
  withBinaryDirectories(({ destination }) => {
    const lockPath = path.join(destination, '.codebase-memory-mcp-runtime.lock');
    const owner = {
      pid: process.pid,
      token: 'a'.repeat(32),
      lease_expires_ms: Date.now() - 1,
    };
    const ownerRecord = `${JSON.stringify(owner)}\n`;
    fs.writeFileSync(lockPath, ownerRecord);
    const before = fs.lstatSync(lockPath, { bigint: true });

    assert.equal(tryReclaimRuntimeLock(lockPath, 'b'.repeat(32)), false);
    const after = fs.lstatSync(lockPath, { bigint: true });
    assert.equal(fs.readFileSync(lockPath, 'utf8'), ownerRecord);
    assert.equal(JSON.parse(fs.readFileSync(lockPath, 'utf8')).token, owner.token);
    assert.equal(after.dev, before.dev);
    assert.equal(after.ino, before.ino);
  });
});

test('a stale legacy lock never reclaims a live owner', () => {
  withBinaryDirectories(({ destination }) => {
    const lockPath = path.join(destination, '.codebase-memory-mcp-runtime.lock');
    const ownerRecord = `${JSON.stringify({
      pid: process.pid,
      token: 'a'.repeat(32),
    })}\n`;
    fs.mkdirSync(lockPath);
    fs.writeFileSync(path.join(lockPath, 'owner.json'), ownerRecord);
    const stale = new Date(Date.now() - 3_600_001);
    fs.utimesSync(lockPath, stale, stale);
    const before = fs.lstatSync(lockPath, { bigint: true });

    assert.equal(tryReclaimRuntimeLock(lockPath, 'b'.repeat(32)), false);
    const after = fs.lstatSync(lockPath, { bigint: true });
    assert.equal(fs.readFileSync(path.join(lockPath, 'owner.json'), 'utf8'), ownerRecord);
    assert.equal(after.dev, before.dev);
    assert.equal(after.ino, before.ino);
  });
});

test('a stale ownerless lock is reclaimed', () => {
  withBinaryDirectories(({ destination }) => {
    const lockPath = path.join(destination, '.codebase-memory-mcp-runtime.lock');
    fs.writeFileSync(lockPath, '');
    const stale = new Date(Date.now() - 30_001);
    fs.utimesSync(lockPath, stale, stale);

    assert.equal(tryReclaimRuntimeLock(lockPath, 'b'.repeat(32)), true);
    assert.equal(fs.existsSync(lockPath), false);
  });
});
