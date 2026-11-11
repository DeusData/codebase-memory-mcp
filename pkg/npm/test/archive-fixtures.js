'use strict';
// Small synthetic release archives for the wrapper tests.

const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

// The npm wrapper takes the zip path whenever Node reports win32 (see
// getPlatform and `ext` in install.js), so the tar path is unreachable on a
// Windows host and the tar cases would test nothing a user can reach there.
// They cannot even build their fixtures on the Windows runner: the first
// `tar` on its PATH is Git for Windows' GNU tar, which reads a drive-letter
// path such as C:\... as a remote host ("tar (child): Cannot connect to C:
// resolve failed"; every tar case failed that way in the first Windows run),
// while Windows' own bsdtar accepts it. Under WSL Node is a Linux binary,
// takes the tar path with POSIX paths, and these cases run there.
const TAR_PATH_SKIP = process.platform === 'win32'
  ? 'the npm wrapper never takes the tar path on win32 (it installs the zip asset)'
  : false;

// Deterministic, poorly compressible bytes (xorshift32), so a fixture of a
// few hundred KiB still spans several pipe chunks after gzip.
function patternBytes(length, seed = 0x9e3779b9) {
  const out = Buffer.alloc(length);
  let state = seed >>> 0;
  for (let index = 0; index < length; index += 1) {
    state ^= state << 13;
    state >>>= 0;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    out[index] = state & 0xff;
  }
  return out;
}

// Writes entries ({ name, data }) into a staging directory under `root` and
// packs them with the system tar, the producer the release itself uses.
function writeTarGz(root, archiveName, entries) {
  const stage = fs.mkdtempSync(path.join(root, 'stage-'));
  for (const entry of entries) {
    fs.writeFileSync(path.join(stage, entry.name), entry.data);
  }
  const archive = path.join(root, archiveName);
  execFileSync(
    'tar',
    ['-czf', archive, '-C', stage, ...entries.map((entry) => entry.name)],
    {
      env: { ...process.env, COPYFILE_DISABLE: '1' },
      stdio: ['ignore', 'ignore', 'inherit'],
      windowsHide: true,
    },
  );
  return archive;
}

const CRC_TABLE = (() => {
  const table = new Int32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) {
      c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    }
    table[n] = c;
  }
  return table;
})();

function crc32(buffer) {
  let crc = -1;
  for (const byte of buffer) {
    crc = CRC_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ -1) >>> 0;
}

// entries: [{ name, data, declaredSize = data.length }]; stored (no
// compression). Hand-rolled so the directory can claim a size that differs
// from the bytes the entry carries, which no zip writer would produce.
function buildZip(entries) {
  const locals = [];
  const centrals = [];
  let offset = 0;
  for (const entry of entries) {
    const data = Buffer.isBuffer(entry.data) ? entry.data : Buffer.from(entry.data);
    const name = Buffer.from(entry.name, 'utf8');
    const declaredSize = entry.declaredSize === undefined
      ? data.length
      : entry.declaredSize;
    const crc = crc32(data);

    const local = Buffer.alloc(30 + name.length);
    local.writeUInt32LE(0x04034b50, 0);
    local.writeUInt16LE(20, 4);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(data.length, 18);
    local.writeUInt32LE(declaredSize, 22);
    local.writeUInt16LE(name.length, 26);
    name.copy(local, 30);

    const central = Buffer.alloc(46 + name.length);
    central.writeUInt32LE(0x02014b50, 0);
    central.writeUInt16LE(20, 4);
    central.writeUInt16LE(20, 6);
    central.writeUInt32LE(crc, 16);
    central.writeUInt32LE(data.length, 20);
    central.writeUInt32LE(declaredSize, 24);
    central.writeUInt16LE(name.length, 28);
    central.writeUInt32LE(offset, 42);
    name.copy(central, 46);

    locals.push(local, data);
    centrals.push(central);
    offset += local.length + data.length;
  }
  const directory = Buffer.concat(centrals);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(entries.length, 8);
  end.writeUInt16LE(entries.length, 10);
  end.writeUInt32LE(directory.length, 12);
  end.writeUInt32LE(offset, 16);
  return Buffer.concat([...locals, directory, end]);
}

module.exports = { TAR_PATH_SKIP, buildZip, patternBytes, writeTarGz };
