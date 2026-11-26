import { mkdir, mkdtemp } from 'node:fs/promises';
import { join } from 'node:path';

export async function prepareHandtestProfile(parent) {
    await mkdir(parent, { recursive: true });
    return mkdtemp(join(parent, 'run-'));
}
