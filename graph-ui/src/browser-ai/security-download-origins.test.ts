import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const guard = fileURLToPath(new URL('../../../scripts/security-strings.sh', import.meta.url));

/** Exercise the production binary guard with a binary-data fixture, not a text-file exemption. */
function acceptsBinaryUrl(url: string): boolean {
    const directory = mkdtempSync(join(tmpdir(), 'cbm-model-origin-'));
    const fixture = join(directory, 'fixture.bin');
    writeFileSync(fixture, Buffer.concat([Buffer.from([0, 1, 2, 3, 255, 254, 253, 252]), Buffer.from(url), Buffer.from([0, 0, 255, 255])]));
    try {
        execFileSync('bash', [guard, fixture], { stdio: 'pipe' });
        return true;
    } catch { return false; }
    finally { rmSync(directory, { recursive: true, force: true }); }
}

describe('binary audit for the explicit model download origins', () => {
    it('permits the two CSP origins and the pinned model path', () => {
        for (const url of ['https://huggingface.co', 'https://us.aws.cdn.hf.co;',
            'https://huggingface.co/onnx-community/Qwen2.5-Coder-0.5B-Instruct']) {
            expect(acceptsBinaryUrl(url), url).toBe(true);
        }
    });
    it('continues rejecting lookalike origins and unrelated endpoints', () => {
        for (const url of ['https://huggingface.co.attacker.test/model', 'https://us.aws.cdn.hf.co.attacker.test/model',
            'https://unrelated.example.test/model']) {
            expect(acceptsBinaryUrl(url), url).toBe(false);
        }
    });
});
