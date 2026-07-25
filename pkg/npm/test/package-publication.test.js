'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const {
  UNIX_ARCHIVE_NAMES,
  WINDOWS_LAUNCHER_NAME,
  WINDOWS_PAYLOAD_NAME,
  extractExactTarArchive,
  installWindowsPairAtomically,
  validateExactTarMemberListing,
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

test('Unix extraction requests only the validated root executable', () => {
  const calls = [];
  const runner = (command, args) => {
    calls.push({ command, args: [...args] });
    return calls.length === 1 ? exactUnixListing() : Buffer.alloc(0);
  };

  extractExactTarArchive(
    '/tmp/release.tar.gz', '/tmp/extract', UNIX_ARCHIVE_NAMES,
    'codebase-memory-mcp', runner,
  );

  assert.deepEqual(calls[0].args, ['-tzf', '/tmp/release.tar.gz']);
  assert.deepEqual(
    calls[1].args,
    ['-xzf', '/tmp/release.tar.gz', '-C', '/tmp/extract', 'codebase-memory-mcp'],
  );
});

function writePair(directory, tag) {
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, WINDOWS_LAUNCHER_NAME), `launcher:${tag}`);
  fs.writeFileSync(path.join(directory, WINDOWS_PAYLOAD_NAME), `payload:${tag}`);
}

function fakePairVerifier(launcherPath) {
  const launcher = fs.readFileSync(launcherPath, 'utf8');
  const payload = fs.readFileSync(
    path.join(path.dirname(launcherPath), WINDOWS_PAYLOAD_NAME), 'utf8',
  );
  const match = /^launcher:(.+)$/.exec(launcher);
  if (!match || payload !== `payload:${match[1]}`) {
    throw new Error('launcher/payload test pair mismatch');
  }
}

function withPairDirectories(callback) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-npm-pair-test-'));
  const source = path.join(root, 'source');
  const destination = path.join(root, 'destination');
  fs.mkdirSync(destination);
  try {
    callback({ source, destination });
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

function assertPair(directory, tag) {
  assert.equal(
    fs.readFileSync(path.join(directory, WINDOWS_LAUNCHER_NAME), 'utf8'),
    `launcher:${tag}`,
  );
  assert.equal(
    fs.readFileSync(path.join(directory, WINDOWS_PAYLOAD_NAME), 'utf8'),
    `payload:${tag}`,
  );
}

test('Windows publication repairs a corrupt launcher', () => {
  withPairDirectories(({ source, destination }) => {
    writePair(source, 'candidate');
    writePair(destination, 'old');
    fs.writeFileSync(path.join(destination, WINDOWS_LAUNCHER_NAME), 'corrupt');

    installWindowsPairAtomically(source, destination, fakePairVerifier);

    assertPair(destination, 'candidate');
  });
});

test('Windows publication repairs corrupt or missing payload state', () => {
  for (const payload of ['corrupt', null]) {
    withPairDirectories(({ source, destination }) => {
      writePair(source, 'candidate');
      fs.writeFileSync(
        path.join(destination, WINDOWS_LAUNCHER_NAME), 'launcher:partial',
      );
      if (payload !== null) {
        fs.writeFileSync(
          path.join(destination, WINDOWS_PAYLOAD_NAME), payload,
        );
      }

      installWindowsPairAtomically(source, destination, fakePairVerifier);

      assertPair(destination, 'candidate');
    });
  }
});

test('Windows publication preserves a valid concurrent winner', () => {
  withPairDirectories(({ source, destination }) => {
    writePair(source, 'loser');
    writePair(destination, 'winner');

    installWindowsPairAtomically(source, destination, fakePairVerifier);

    assertPair(destination, 'winner');
  });
});
