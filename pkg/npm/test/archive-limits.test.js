'use strict';

const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const { Readable } = require('node:stream');
const test = require('node:test');
const vm = require('node:vm');

const {
  UNIX_ARCHIVE_NAMES,
  WINDOWS_ARCHIVE_NAMES,
  WINDOWS_BINARY_NAME,
  download,
  extractExactTarArchive,
  extractZipOnWindows,
  resolveDownloadBase,
} = require('../install.js');
const {
  TAR_PATH_SKIP, buildZip, patternBytes, writeTarGz,
} = require('./archive-fixtures.js');

const EXECUTABLE = 'codebase-memory-mcp';
// Spans several pipe chunks, so the counted writer sees a multi-chunk stream.
const LARGE_EXECUTABLE = patternBytes(300 * 1024);
// The size cap the 0.12.0 wrappers enforced, which refused every release.
const FORMER_MEMBER_CAP_BYTES = 256 * 1024 * 1024;

function withScratch(callback) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cbm-npm-archive-test-'));
  return Promise.resolve()
    .then(() => callback(root))
    .finally(() => fs.rmSync(root, { recursive: true, force: true }));
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

function loadInstallerWithTransports({ fakeHttps, fakeHttp }) {
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
      if (specifier === 'https' && fakeHttps) return fakeHttps;
      if (specifier === 'http' && fakeHttp) return fakeHttp;
      return require(specifier);
    },
    setTimeout,
  };
  vm.runInNewContext(source, sandbox, { filename: installPath });
  return sandbox.module.exports;
}

// Answers every request with one HTTP 200 response shaped by `configure`;
// `requests` records each (url, options) the installer issued.
function fakeTransportServing(configure, requests = []) {
  return {
    get(url, ...rest) {
      const callback = rest[rest.length - 1];
      requests.push({ url, options: rest.length > 1 ? rest[0] : undefined });
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

function refusingTransport() {
  return {
    get(url) {
      throw new Error(`unexpected request to ${url}`);
    },
  };
}

test('download stores a body whose declared length exceeds the former 256 MiB cap', () =>
  withScratch(async (root) => {
    const { download: fakeDownload } = loadInstallerWithTransports({
      fakeHttps: fakeTransportServing((response) => {
        response.headers['content-length'] = String(FORMER_MEMBER_CAP_BYTES + 1);
        response.push('payload');
        response.push(null);
      }),
    });
    const destination = path.join(root, 'release.tar.gz');

    await fakeDownload('https://example.invalid/release.tar.gz', destination);

    assert.equal(fs.readFileSync(destination, 'utf8'), 'payload');
  }));

test('download refuses plain HTTP unless the base is a loopback override', () =>
  withScratch(async (root) => {
    const { download: fakeDownload } = loadInstallerWithTransports({
      fakeHttps: refusingTransport(),
      fakeHttp: refusingTransport(),
    });
    const destination = path.join(root, 'release.tar.gz');

    await assert.rejects(
      fakeDownload('http://127.0.0.1/release.tar.gz', destination),
      /Refusing non-HTTPS or credentialed URL/,
    );
    await assert.rejects(
      fakeDownload('http://example.invalid/release.tar.gz', destination, true),
      /Refusing non-loopback HTTP URL/,
    );
    assert.equal(fs.existsSync(destination), false);
  }));

test('a loopback download uses a fresh agent, so no proxy applies', () =>
  withScratch(async (root) => {
    const requests = [];
    const { download: fakeDownload } = loadInstallerWithTransports({
      fakeHttps: refusingTransport(),
      fakeHttp: fakeTransportServing((response) => {
        response.push('payload');
        response.push(null);
      }, requests),
    });
    const destination = path.join(root, 'release.tar.gz');

    await fakeDownload('http://[::1]:8080/release.tar.gz', destination, true);

    assert.equal(fs.readFileSync(destination, 'utf8'), 'payload');
    assert.equal(requests.length, 1);
    assert.equal(requests[0].url, 'http://[::1]:8080/release.tar.gz');
    assert.equal(requests[0].options.agent, false);
  }));

function withLoopbackServer(handler, callback) {
  const server = http.createServer(handler);
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  }).then(() => Promise.resolve()
    .then(() => callback(`http://127.0.0.1:${server.address().port}`))
    .finally(() => new Promise((resolve) => server.close(resolve))));
}

test('a loopback download fetches from a local server and refuses its redirects', () =>
  withScratch((root) => withLoopbackServer((request, response) => {
    if (request.url === '/release/codebase-memory-mcp.tar.gz') {
      response.writeHead(200, { 'content-type': 'application/octet-stream' });
      response.end('loopback payload');
      return;
    }
    response.writeHead(302, { location: 'https://example.invalid/elsewhere' });
    response.end();
  }, async (origin) => {
    const archive = path.join(root, 'release.tar.gz');
    await download(`${origin}/release/codebase-memory-mcp.tar.gz`, archive, true);
    assert.equal(fs.readFileSync(archive, 'utf8'), 'loopback payload');

    const redirected = path.join(root, 'redirected.tar.gz');
    await assert.rejects(
      download(`${origin}/release/checksums.txt`, redirected, true),
      /Refusing redirect from loopback download/,
    );
    assert.equal(fs.existsSync(redirected), false);
  })));

test('CBM_DOWNLOAD_URL follows install.sh: HTTPS or exact-loopback HTTP only', () => {
  const { version } = require('../package.json');
  const githubBase = `https://github.com/DeusData/codebase-memory-mcp/releases/download/v${version}`;
  assert.deepEqual(resolveDownloadBase({}), { base: githubBase, loopback: false });
  assert.deepEqual(
    resolveDownloadBase({ CBM_DOWNLOAD_URL: '' }), { base: githubBase, loopback: false },
  );

  for (const base of [
    'https://mirror.example/releases/v1',
    'https://127.0.0.1:8443/fixture',
  ]) {
    assert.deepEqual(
      resolveDownloadBase({ CBM_DOWNLOAD_URL: base }), { base, loopback: false }, base,
    );
  }
  for (const base of [
    'http://localhost',
    'http://localhost:8080/fixture',
    'http://127.0.0.1:41234/standard',
    'http://[::1]:9/x?y#z',
  ]) {
    assert.deepEqual(
      resolveDownloadBase({ CBM_DOWNLOAD_URL: base }), { base, loopback: true }, base,
    );
  }
  for (const base of [
    'http://example.com/releases',
    'http://127.0.0.1.example.com/',
    'http://localhost.example.com:8080/',
    'http://user@localhost/',
    'http://127.0.0.2/',
    'HTTP://localhost/',
    'file:///tmp/releases',
    'ftp://localhost/releases',
    'github.com/DeusData/codebase-memory-mcp',
  ]) {
    assert.throws(
      () => resolveDownloadBase({ CBM_DOWNLOAD_URL: base }),
      /Refusing non-HTTPS download URL \(CBM_DOWNLOAD_URL\)/,
      base,
    );
  }
});

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
        archive, destination, ['one', 'two', 'three'], ['one'], 2,
      ),
      /2-member safety limit/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('Windows zip extraction rejects a member that yields more bytes than it declares', { skip: WINDOWS_ONLY }, () =>
  withScratch(async (root) => {
    const archive = path.join(root, 'longer.zip');
    fs.writeFileSync(archive, buildZip([
      { name: WINDOWS_BINARY_NAME, data: Buffer.alloc(600, 0x78), declaredSize: 10 },
    ]));
    const destination = makeDestination(root);

    assert.throws(
      () => extractZipOnWindows(
        archive, destination, [WINDOWS_BINARY_NAME], [WINDOWS_BINARY_NAME],
      ),
      /actual size \d+ does not match declared size 10/,
    );
    assert.deepEqual(fs.readdirSync(destination), []);
  }));

test('Windows zip extraction installs only the executable', { skip: WINDOWS_ONLY }, () =>
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
