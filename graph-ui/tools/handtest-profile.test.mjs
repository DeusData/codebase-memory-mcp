import { afterEach, expect, it } from 'vitest';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { prepareHandtestProfile } from './lib/handtest-profile.mjs';

const fixtures = [];
afterEach(async () => {
    await Promise.all(fixtures.splice(0).map((path) => rm(path, { recursive: true, force: true })));
});

it('preserves an existing profile and creates separate fresh run directories', async () => {
    const parent = await mkdtemp(join(tmpdir(), 'cbm-profile-test-'));
    fixtures.push(parent);
    const marker = join(parent, 'existing-profile-data');
    await writeFile(marker, 'keep this profile');

    const first = await prepareHandtestProfile(parent);
    expect(await readFile(marker, 'utf8').catch(() => undefined)).toBe('keep this profile');
    expect(dirname(first)).toBe(parent);
    expect((await stat(first)).isDirectory()).toBe(true);
    await writeFile(join(first, 'previous-run'), 'keep this run too');

    const second = await prepareHandtestProfile(parent);
    expect(second).not.toBe(first);
    expect(dirname(second)).toBe(parent);
    expect(await readFile(join(first, 'previous-run'), 'utf8')).toBe('keep this run too');
});
