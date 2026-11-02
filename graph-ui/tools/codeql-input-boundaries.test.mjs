import { afterEach, expect, it } from 'vitest';
import { mkdtempSync, mkdirSync, writeFileSync, symlinkSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Writable } from 'node:stream';
import { literalPattern } from './literal-pattern.mjs';
import { serveStaticAsset } from './smoke-static-assets.mjs';

const temporary = [];
afterEach(() => { for (const path of temporary.splice(0)) rmSync(path, { recursive: true, force: true }); });

it.each(['a.b', 'a+b', '(a+)+$', '[project]', 'a\\b'])('treats a project name as literal text: %s', name => {
    const pattern = literalPattern(name, { start: true, end: true });
    expect(pattern.test(name)).toBe(true);
    expect(pattern.test(`prefix${name}`)).toBe(false);
    expect(pattern.test(`${name}suffix`)).toBe(false);
});

it('does not expand project-name wildcards', () => {
    expect(literalPattern('a.b', { start: true, end: true }).test('axb')).toBe(false);
});

function fixture() {
    const root = mkdtempSync(join(tmpdir(), 'cbm-static-assets-')); temporary.push(root);
    mkdirSync(join(root, 'spike')); mkdirSync(join(root, 'node_modules')); mkdirSync(join(root, 'spike-extra'));
    writeFileSync(join(root, 'spike', 'index.html'), 'fixture');
    writeFileSync(join(root, 'outside.txt'), 'outside-root');
    writeFileSync(join(root, 'spike-extra', 'outside.txt'), 'sibling-root');
    return root;
}
function request(root, url) {
    return new Promise((resolve, reject) => {
        const chunks = []; let status;
        const response = new Writable({ write(chunk, encoding, done) { chunks.push(Buffer.from(chunk)); done(); } });
        response.writeHead = code => { status = code; };
        response.on('error', reject);
        response.on('finish', () => resolve({ status, body: Buffer.concat(chunks).toString() }));
        try { serveStaticAsset(root, { url }, response); } catch (error) { reject(error); }
    });
}

it('serves an allowed static fixture and rejects traversal and sibling prefixes', async () => {
    const root = fixture();
    expect(await request(root, '/spike/index.html')).toEqual({ status: 200, body: 'fixture' });
    expect((await request(root, '/spike/../outside.txt')).status).toBe(404);
    expect((await request(root, '/spike/%2e%2e/outside.txt')).status).toBe(404);
    expect((await request(root, '/spike-extra/outside.txt')).status).toBe(404);
});

it('rejects a symlink escaping an allowed static root', async () => {
    const root = fixture();
    symlinkSync(join(root, 'outside.txt'), join(root, 'spike', 'escape.txt'));
    expect((await request(root, '/spike/escape.txt')).status).toBe(404);
});

it('returns not found for malformed URL encoding instead of throwing', async () => {
    expect((await request(fixture(), '/spike/%zz')).status).toBe(404);
});

it('allows a symlink whose real target stays inside the static root', async () => {
    const root = fixture();
    symlinkSync(join(root, 'spike', 'index.html'), join(root, 'spike', 'alias.html'));
    expect(await request(root, '/spike/alias.html')).toEqual({ status: 200, body: 'fixture' });
});
