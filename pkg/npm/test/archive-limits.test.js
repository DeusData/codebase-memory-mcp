'use strict';

const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Readable } = require('node:stream');
const test = require('node:test');
const vm = require('node:vm');

const {
  DEFAULT_ARCHIVE_LIMITS,
  UNIX_ARCHIVE_NAMES,
  WINDOWS_ARCHIVE_NAMES,
  WINDOWS_BINARY_NAME,
  extractExactTarArchive,
  extractZipOnWindows,
} = require('../install.js');
const {
  TAR_PATH_SKIP, buildZip, patternBytes, writeTarGz,
} = require('./archive-fixtures.js');

const EXECUTABLE = 'codebase-memory-mcp';
// Spans several pipe chunks, so an over-limit member is cut off mid-stream.
const LARGE_EXECUTABLE = patternBytes(300 * 1024);

function withScratch(callback) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-npm-archive-test-'));
  return Promise.resolve()
    .then(() => callback(root))
    .finally(() => fs.rmSync(root, { recursive: true, force: true }));
}

function limitsWith(overrides) {
  return { ...DEFAULT_ARCHIVE_LIMITS, ...overrides };
}

function releaseEntries(names, executableData) {
  return names.map((name) => ({
    name,
    data: executableData !== undefined && (name === EXECUTABLE || name === WINDOWS_BINARY_NAME)
      ? executableData
      : `payload of ${name}`,
  }));
}

function makeDestination(root) {
  const destination = path.join(root, 'extract');
  fs.mkdirSync(destination);
  return destination;
}

// Stands in for the system tar listing where the archive itself is damaged
// on purpose and the real listing would fail before the writer runs.
function fakeTarListing(names) {
  return (command, args) => {
    assert.equal(command, 'tar');
    assert.equal(args[0], '-tzf');
    return `${names.join('\n')}\n`;
  };
}

function neverLists() {
  throw new Error('the listing must not run');
}

test('tar extraction refuses an archive larger than the compressed limit', { skip: TAR_PATH_SKIP }, () =>
  withScratch(async (root) => {
    const archive = writeTarGz(root, 'release.tar.gz', releaseEntries(UNIX_ARCHIVE_NAMES));
    const destination = makeDestination(root);

    await assert.rejects(
      extractExactTarArchive(
        archive, destination, UNIX_ARCHIVE_NAMES, EXECUTABLE,
        neverLists, limitsWith({ compressedBytes: 16 }),
      ),
      /16-byte compressed safety limit/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('tar extraction refuses an exact namespace wider than the member limit', { skip: TAR_PATH_SKIP }, () =>
  withScratch(async (root) => {
    const archive = writeTarGz(root, 'release.tar.gz', releaseEntries(UNIX_ARCHIVE_NAMES));
    const destination = makeDestination(root);

    await assert.rejects(
      extractExactTarArchive(
        archive, destination, UNIX_ARCHIVE_NAMES, EXECUTABLE,
        neverLists, limitsWith({ members: 2 }),
      ),
      /2-member safety limit/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('tar extraction stops an over-limit member in the counted writer and leaves no file', { skip: TAR_PATH_SKIP }, () =>
  withScratch(async (root) => {
    const archive = writeTarGz(
      root, 'release.tar.gz', releaseEntries(UNIX_ARCHIVE_NAMES, LARGE_EXECUTABLE),
    );
    const destination = makeDestination(root);

    // The real listing passes (names only); the limit trips while tar is
    // still streaming the executable into the writer.
    await assert.rejects(
      extractExactTarArchive(
        archive, destination, UNIX_ARCHIVE_NAMES, EXECUTABLE,
        undefined, limitsWith({ memberBytes: 64 * 1024 }),
      ),
      /"codebase-memory-mcp" exceeds the 65536-byte expanded safety limit/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('tar extraction leaves a pre-existing target untouched', { skip: TAR_PATH_SKIP }, () =>
  withScratch(async (root) => {
    const archive = writeTarGz(root, 'release.tar.gz', releaseEntries(UNIX_ARCHIVE_NAMES));
    const destination = makeDestination(root);
    const target = path.join(destination, EXECUTABLE);
    fs.writeFileSync(target, 'keep');

    await assert.rejects(
      extractExactTarArchive(archive, destination, UNIX_ARCHIVE_NAMES, EXECUTABLE),
      /EEXIST/,
    );
    assert.equal(fs.readFileSync(target, 'utf8'), 'keep');
  }));

test('tar extraction leaves no file when tar fails mid-stream', { skip: TAR_PATH_SKIP }, () =>
  withScratch(async (root) => {
    const complete = writeTarGz(
      root, 'complete.tar.gz', releaseEntries(UNIX_ARCHIVE_NAMES, LARGE_EXECUTABLE),
    );
    const bytes = fs.readFileSync(complete);
    const archive = path.join(root, 'truncated.tar.gz');
    fs.writeFileSync(archive, bytes.subarray(0, Math.floor(bytes.length * 0.6)));
    const destination = makeDestination(root);

    await assert.rejects(
      extractExactTarArchive(
        archive, destination, UNIX_ARCHIVE_NAMES, EXECUTABLE,
        fakeTarListing(UNIX_ARCHIVE_NAMES),
      ),
      // Non-zero exit, and tar's own diagnostic text after the colon.
      /tar exited with status [1-9]\d* while extracting codebase-memory-mcp: \S/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('tar extraction rejects an empty executable', { skip: TAR_PATH_SKIP }, () =>
  withScratch(async (root) => {
    const archive = writeTarGz(
      root, 'release.tar.gz', releaseEntries(UNIX_ARCHIVE_NAMES, Buffer.alloc(0)),
    );
    const destination = makeDestination(root);

    await assert.rejects(
      extractExactTarArchive(archive, destination, UNIX_ARCHIVE_NAMES, EXECUTABLE),
      /archive member codebase-memory-mcp is empty/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('tar extraction installs the executable byte-identical with the executable bit set', { skip: TAR_PATH_SKIP }, () =>
  withScratch(async (root) => {
    const archive = writeTarGz(
      root, 'release.tar.gz', releaseEntries(UNIX_ARCHIVE_NAMES, LARGE_EXECUTABLE),
    );
    const destination = makeDestination(root);

    await extractExactTarArchive(archive, destination, UNIX_ARCHIVE_NAMES, EXECUTABLE);

    const target = path.join(destination, EXECUTABLE);
    assert.ok(fs.readFileSync(target).equals(LARGE_EXECUTABLE));
    if (process.platform !== 'win32') {
      // Windows has no mode bits; everywhere else the file must be runnable.
      assert.equal(fs.statSync(target).mode & 0o111, 0o111);
    }
    assert.deepEqual(fs.readdirSync(destination), [EXECUTABLE]);
  }));

function loadInstallerWithHttps(fakeHttps) {
  const installPath = path.join(__dirname, '..', 'install.js');
  const source = fs.readFileSync(installPath, 'utf8');
  const sandbox = {
    Buffer,
    URL,
    __dirname: path.dirname(installPath),
    __filename: installPath,
    clearTimeout,
    console,
    exports: {},
    // A parent keeps the postinstall entry point from running on load.
    module: { exports: {}, parent: {} },
    process,
    require: (specifier) => {
      if (specifier === './package.json') return { version: '0.0.0-test' };
      if (specifier === 'https') return fakeHttps;
      return require(specifier);
    },
    setTimeout,
  };
  vm.runInNewContext(source, sandbox, { filename: installPath });
  return sandbox.module.exports;
}

// Answers every request with one HTTP 200 response shaped by `configure`.
function fakeHttpsServing(configure) {
  return {
    get(url, callback) {
      const request = new EventEmitter();
      request.destroy = (err) => { if (err) request.emit('error', err); };
      setImmediate(() => {
        const response = new Readable({ read() {} });
        response.statusCode = 200;
        response.headers = {};
        configure(response);
        callback(response);
      });
      return request;
    },
  };
}

test('download refuses a response whose declared length exceeds the limit', () =>
  withScratch(async (root) => {
    const { download } = loadInstallerWithHttps(fakeHttpsServing((response) => {
      response.headers['content-length'] = '9';
      response.push(null);
    }));
    const destination = path.join(root, 'release.tar.gz');

    await assert.rejects(
      download('https://example.invalid/release.tar.gz', destination, 8),
      /8-byte safety limit/,
    );
    assert.equal(fs.existsSync(destination), false);
  }));

test('download refuses a body that exceeds the limit and removes the partial file', () =>
  withScratch(async (root) => {
    const { download } = loadInstallerWithHttps(fakeHttpsServing((response) => {
      response.push(Buffer.alloc(1024, 0x78));
      response.push(null);
    }));
    const destination = path.join(root, 'release.tar.gz');

    await assert.rejects(
      download('https://example.invalid/release.tar.gz', destination, 8),
      /8-byte safety limit/,
    );
    assert.equal(fs.existsSync(destination), false);
  }));

test('download within the limit stores the body', () =>
  withScratch(async (root) => {
    const { download } = loadInstallerWithHttps(fakeHttpsServing((response) => {
      response.headers['content-length'] = '7';
      response.push('payload');
      response.push(null);
    }));
    const destination = path.join(root, 'release.tar.gz');

    await download('https://example.invalid/release.tar.gz', destination, 1024);

    assert.equal(fs.readFileSync(destination, 'utf8'), 'payload');
  }));

// The Windows path runs a constant PowerShell program through powershell.exe,
// which only a Windows host provides (pwsh is not a wrapper dependency, and
// no PowerShell is installed on the Unix wrapper hosts), so these cases run
// in the Windows wrapper leg and are skipped elsewhere.
const WINDOWS_ONLY = process.platform === 'win32'
  ? false
  : 'zip extraction runs through powershell.exe, which this host does not provide';

test('Windows zip extraction refuses an archive with more members than the limit', { skip: WINDOWS_ONLY }, () =>
  withScratch(async (root) => {
    const archive = path.join(root, 'members.zip');
    fs.writeFileSync(archive, buildZip([
      { name: 'one', data: 'a' },
      { name: 'two', data: 'b' },
      { name: 'three', data: 'c' },
    ]));
    const destination = makeDestination(root);

    assert.throws(
      () => extractZipOnWindows(
        archive, destination, ['one', 'two', 'three'], ['one'],
        limitsWith({ members: 2 }),
      ),
      /2-member safety limit/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('Windows zip extraction refuses a member whose declared size exceeds the limit', { skip: WINDOWS_ONLY }, () =>
  withScratch(async (root) => {
    const archive = path.join(root, 'member.zip');
    fs.writeFileSync(archive, buildZip([
      { name: WINDOWS_BINARY_NAME, data: Buffer.alloc(65, 0x78) },
    ]));
    const destination = makeDestination(root);

    assert.throws(
      () => extractZipOnWindows(
        archive, destination, [WINDOWS_BINARY_NAME], [WINDOWS_BINARY_NAME],
        limitsWith({ memberBytes: 64 }),
      ),
      /64-byte expanded safety limit/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('Windows zip extraction stops a member that yields more bytes than it declares', { skip: WINDOWS_ONLY }, () =>
  withScratch(async (root) => {
    const archive = path.join(root, 'longer.zip');
    fs.writeFileSync(archive, buildZip([
      { name: WINDOWS_BINARY_NAME, data: Buffer.alloc(600, 0x78), declaredSize: 10 },
    ]));
    const destination = makeDestination(root);

    assert.throws(
      () => extractZipOnWindows(
        archive, destination, [WINDOWS_BINARY_NAME], [WINDOWS_BINARY_NAME],
        limitsWith({ memberBytes: 64 }),
      ),
      /64-byte actual expanded safety limit/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('Windows zip extraction within the limits installs only the executable', { skip: WINDOWS_ONLY }, () =>
  withScratch(async (root) => {
    const archive = path.join(root, 'release.zip');
    fs.writeFileSync(archive, buildZip(releaseEntries(WINDOWS_ARCHIVE_NAMES)));
    const destination = makeDestination(root);

    extractZipOnWindows(
      archive, destination, WINDOWS_ARCHIVE_NAMES, [WINDOWS_BINARY_NAME],
    );

    assert.equal(
      fs.readFileSync(path.join(destination, WINDOWS_BINARY_NAME), 'utf8'),
      `payload of ${WINDOWS_BINARY_NAME}`,
    );
    assert.deepEqual(fs.readdirSync(destination), [WINDOWS_BINARY_NAME]);
  }));
