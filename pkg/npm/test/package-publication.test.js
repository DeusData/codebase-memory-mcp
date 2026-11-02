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
const { TAR_PATH_SKIP, writeTarGz } = require('./archive-fixtures.js');

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

test('Unix extraction lists through the system tar and writes only the root executable', { skip: TAR_PATH_SKIP }, async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-npm-extract-test-'));
  try {
    const archive = writeTarGz(
      root, 'release.tar.gz',
      UNIX_ARCHIVE_NAMES.map((name) => ({ name, data: `member:${name}` })),
    );
    const destination = path.join(root, 'extract');
    fs.mkdirSync(destination);
    const calls = [];
    const runner = (command, args) => {
      calls.push({ command, args: [...args] });
      return exactUnixListing();
    };

    await extractExactTarArchive(
      archive, destination, UNIX_ARCHIVE_NAMES, 'codebase-memory-mcp', runner,
    );

    // The listing runner sees the namespace query only; the executable is
    // written by the counted `tar -xzOf` writer, never by `tar -x` into the
    // tree, so the companions never touch the disk.
    assert.deepEqual(calls.map((call) => call.args), [['-tzf', archive]]);
    assert.equal(
      fs.readFileSync(path.join(destination, 'codebase-memory-mcp'), 'utf8'),
      'member:codebase-memory-mcp',
    );
    assert.deepEqual(fs.readdirSync(destination), ['codebase-memory-mcp']);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
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
