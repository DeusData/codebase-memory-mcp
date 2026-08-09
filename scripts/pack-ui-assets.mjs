#!/usr/bin/env node
/*
 * Build a deterministic, uncompressed, content-addressed UI asset pack.
 *
 * The native binary receives only the pack name, exact byte length and
 * SHA-256. Frontend bytes stay outside ELF/Mach-O/PE and stay unobscured for
 * scanners, SBOM tools and human inspection.
 */
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

const HEADER_BYTES = 80;
const ENTRY_BYTES = 24;
const FORMAT_VERSION = 1;
const MAX_FILES = 1024;
const MAX_PATH_BYTES = 255;
const MAX_PACK_BYTES = 64 * 1024 * 1024;

const MIME_BY_EXTENSION = new Map([
  ['.html', 1],
  ['.js', 2],
  ['.mjs', 2],
  ['.css', 3],
  ['.json', 4],
  ['.svg', 5],
  ['.png', 6],
  ['.jpg', 7],
  ['.jpeg', 7],
  ['.webp', 8],
  ['.avif', 9],
  ['.ico', 10],
  ['.woff2', 11],
  ['.woff', 12],
  ['.wasm', 13],
]);

function fail(message) {
  throw new Error(`pack-ui-assets: ${message}`);
}

function compareBytewise(left, right) {
  return Buffer.compare(Buffer.from(left, 'utf8'), Buffer.from(right, 'utf8'));
}

function validateUrlPath(urlPath) {
  const bytes = Buffer.from(urlPath, 'utf8');
  if (bytes.length < 2 || bytes.length > MAX_PATH_BYTES ||
      !/^[A-Za-z0-9._/-]+$/.test(urlPath) || urlPath.includes('//') ||
      urlPath.endsWith('/') || urlPath.includes('/./') || urlPath.includes('/../') ||
      urlPath.endsWith('/.') || urlPath.endsWith('/..')) {
    fail(`unsupported frontend path ${JSON.stringify(urlPath)}`);
  }
  if (urlPath !== '/index.html' && !urlPath.startsWith('/assets/')) {
    fail(`frontend output must contain only /index.html and /assets/* (got ${urlPath})`);
  }
  return bytes;
}

async function collectFiles(root, directory = root, relative = '') {
  const dirEntries = await fs.readdir(directory, { withFileTypes: true });
  dirEntries.sort((left, right) => compareBytewise(left.name, right.name));
  const files = [];
  for (const entry of dirEntries) {
    const absolute = path.join(directory, entry.name);
    const childRelative = relative ? path.posix.join(relative, entry.name) : entry.name;
    if (entry.isSymbolicLink()) {
      fail(`symbolic links are not allowed (${childRelative})`);
    }
    if (entry.isDirectory()) {
      files.push(...await collectFiles(root, absolute, childRelative));
    } else if (entry.isFile()) {
      const urlPath = `/${childRelative}`;
      const pathBytes = validateUrlPath(urlPath);
      const extension = path.posix.extname(childRelative).toLowerCase();
      const mimeId = MIME_BY_EXTENSION.get(extension);
      if (!mimeId) {
        fail(`no closed MIME mapping for ${urlPath}`);
      }
      const data = await fs.readFile(absolute);
      if (data.length === 0) {
        fail(`zero-length frontend assets are not supported (${urlPath})`);
      }
      files.push({
        urlPath,
        pathBytes,
        mimeId,
        cacheId: urlPath === '/index.html' ? 1 : 2,
        data,
      });
    } else {
      fail(`unsupported filesystem entry (${childRelative})`);
    }
  }
  return files;
}

function writeU64(buffer, value, offset) {
  if (!Number.isSafeInteger(value) || value < 0) fail('pack size exceeds safe integer range');
  buffer.writeBigUInt64LE(BigInt(value), offset);
}

function buildPack(files) {
  files.sort((left, right) => compareBytewise(left.urlPath, right.urlPath));
  if (files.length === 0 || files.length > MAX_FILES) {
    fail(`frontend file count must be between 1 and ${MAX_FILES}`);
  }
  if (files.filter((file) => file.urlPath === '/index.html').length !== 1) {
    fail('frontend output must contain exactly one /index.html');
  }
  for (let index = 1; index < files.length; index += 1) {
    if (compareBytewise(files[index - 1].urlPath, files[index].urlPath) >= 0) {
      fail(`duplicate or unsorted path ${files[index].urlPath}`);
    }
  }

  const indexBytes = files.length * ENTRY_BYTES;
  const pathsBytes = files.reduce((total, file) => total + file.pathBytes.length, 0);
  const payloadBytes = files.reduce((total, file) => total + file.data.length, 0);
  const indexOffset = HEADER_BYTES;
  const pathsOffset = indexOffset + indexBytes;
  const payloadOffset = pathsOffset + pathsBytes;
  const totalBytes = payloadOffset + payloadBytes;
  if (totalBytes > MAX_PACK_BYTES) {
    fail(`pack exceeds ${MAX_PACK_BYTES} bytes`);
  }

  const pack = Buffer.alloc(totalBytes);
  pack.write('CBMUIPK', 0, 7, 'ascii');
  pack[7] = 0;
  pack.writeUInt16LE(FORMAT_VERSION, 8);
  pack.writeUInt16LE(HEADER_BYTES, 10);
  pack.writeUInt32LE(0, 12);
  pack.writeUInt32LE(files.length, 16);
  pack.writeUInt32LE(ENTRY_BYTES, 20);
  writeU64(pack, indexOffset, 24);
  writeU64(pack, indexBytes, 32);
  writeU64(pack, pathsOffset, 40);
  writeU64(pack, pathsBytes, 48);
  writeU64(pack, payloadOffset, 56);
  writeU64(pack, payloadBytes, 64);
  writeU64(pack, totalBytes, 72);

  let pathCursor = 0;
  let payloadCursor = 0;
  files.forEach((file, index) => {
    const entryOffset = indexOffset + index * ENTRY_BYTES;
    pack.writeUInt32LE(pathCursor, entryOffset);
    pack.writeUInt16LE(file.pathBytes.length, entryOffset + 4);
    pack.writeUInt8(file.mimeId, entryOffset + 6);
    pack.writeUInt8(file.cacheId, entryOffset + 7);
    writeU64(pack, payloadCursor, entryOffset + 8);
    writeU64(pack, file.data.length, entryOffset + 16);
    file.pathBytes.copy(pack, pathsOffset + pathCursor);
    file.data.copy(pack, payloadOffset + payloadCursor);
    pathCursor += file.pathBytes.length;
    payloadCursor += file.data.length;
  });
  return pack;
}

async function writeAtomically(destination, bytes) {
  const temporary = `${destination}.tmp-${process.pid}`;
  try {
    await fs.writeFile(temporary, bytes, { flag: 'wx', mode: 0o644 });
    await fs.rename(temporary, destination);
  } catch (error) {
    await fs.rm(temporary, { force: true });
    throw error;
  }
}

async function main() {
  const [sourceRoot, outputDirectory, manifestPath, ...extra] = process.argv.slice(2);
  if (!sourceRoot || !outputDirectory || !manifestPath || extra.length !== 0) {
    fail('usage: pack-ui-assets.mjs <dist-dir> <output-dir> <manifest.c>');
  }
  const rootStat = await fs.lstat(sourceRoot);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) {
    fail('dist-dir must be a real directory, not a symlink');
  }
  const files = await collectFiles(sourceRoot);
  const pack = buildPack(files);
  const digest = crypto.createHash('sha256').update(pack).digest('hex');
  const packName = `cbm-ui-${digest}.pack`;
  const manifest = Buffer.from(
    '/* Generated by scripts/pack-ui-assets.mjs; do not edit. */\n' +
    '#include <stdint.h>\n\n' +
    `const char CBM_UI_ASSET_PACK_NAME[] = "${packName}";\n` +
    `const char CBM_UI_ASSET_SHA256[] = "${digest}";\n` +
    `const uint64_t CBM_UI_ASSET_SIZE = UINT64_C(${pack.length});\n`,
    'utf8',
  );

  await fs.mkdir(outputDirectory, { recursive: true });
  await fs.mkdir(path.dirname(manifestPath), { recursive: true });
  const packPath = path.join(outputDirectory, packName);
  await writeAtomically(packPath, pack);
  await writeAtomically(manifestPath, manifest);

  const oldPacks = (await fs.readdir(outputDirectory))
    .filter((name) => /^cbm-ui-[0-9a-f]{64}\.pack$/.test(name) && name !== packName);
  await Promise.all(oldPacks.map((name) => fs.rm(path.join(outputDirectory, name))));
  process.stdout.write(`${packName} ${pack.length} bytes ${files.length} files\n`);
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
