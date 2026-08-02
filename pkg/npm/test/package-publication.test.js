'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const {
  UNIX_ARCHIVE_NAMES,
  WINDOWS_BINARY_NAME,
  extractExactTarArchive,
  installWindowsBinaryAtomically,
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

function writeBinary(directory, tag) {
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, WINDOWS_BINARY_NAME), `binary:${tag}`);
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
}

test('Windows publication repairs a corrupt cached binary', () => {
  withBinaryDirectories(({ source, destination }) => {
    writeBinary(source, 'candidate');
    writeBinary(destination, 'old');
    fs.writeFileSync(path.join(destination, WINDOWS_BINARY_NAME), 'corrupt');

    installWindowsBinaryAtomically(source, destination, fakeBinaryVerifier);

    assertBinary(destination, 'candidate');
  });
});

test('Windows publication installs into an empty cache', () => {
  withBinaryDirectories(({ source, destination }) => {
    writeBinary(source, 'candidate');

    installWindowsBinaryAtomically(source, destination, fakeBinaryVerifier);

    assertBinary(destination, 'candidate');
  });
});

test('Windows publication preserves a valid concurrent winner', () => {
  withBinaryDirectories(({ source, destination }) => {
    writeBinary(source, 'loser');
    writeBinary(destination, 'winner');

    installWindowsBinaryAtomically(source, destination, fakeBinaryVerifier);

    assertBinary(destination, 'winner');
  });
});
