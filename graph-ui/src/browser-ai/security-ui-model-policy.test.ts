import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const repository = fileURLToPath(new URL('../../../', import.meta.url));

/** Execute the real source guard with real native boundary files and one isolated UI fixture. */
function acceptsSource(path: string, content: string): boolean {
    const root = mkdtempSync(join(tmpdir(), 'cbm-ui-model-policy-'));
    const target = join(root, 'graph-ui/src', path);
    mkdirSync(dirname(target), { recursive: true });
    mkdirSync(join(root, 'scripts'), { recursive: true });
    mkdirSync(join(root, 'src/ui'), { recursive: true });
    writeFileSync(target, content);
    for (const file of ['scripts/security-ui.sh', 'src/ui/httpd.c', 'src/ui/http_server.c']) {
        copyFileSync(join(repository, file), join(root, file));
    }
    try { execFileSync('bash', [join(root, 'scripts/security-ui.sh')], { stdio: 'pipe' }); return true; }
    catch { return false; }
    finally { rmSync(root, { recursive: true, force: true }); }
}

describe('UI source audit for the pinned browser model', () => {
    it('accepts the actual model policy with its exact download and documentation locations', () => {
        const policy = readFileSync(new URL('./model-policy.ts', import.meta.url), 'utf8');
        expect(acceptsSource('browser-ai/model-policy.ts', policy)).toBe(true);
    });
    it('permits negative URL fixtures only in the identified guard tests', () => {
        expect(acceptsSource('browser-ai/security-download-origins.test.ts', 'const value = "https://unrelated.example.test/model";')).toBe(true);
        expect(acceptsSource('browser-ai/ChatMarkdown.test.tsx', 'const value = "https://example.com/image.png";')).toBe(true);
        expect(acceptsSource('browser-ai/ChatMarkdown.tsx', 'const value = "https://example.com/image.png";')).toBe(false);
        expect(acceptsSource('browser-ai/other.test.ts', 'const value = "https://unrelated.example.test/model";')).toBe(false);
    });
    it('rejects lookalike hosts and model URLs outside the policy file', () => {
        expect(acceptsSource('browser-ai/model-policy.ts', 'const value = "https://huggingface.co.attacker.test/model";')).toBe(false);
        expect(acceptsSource('browser-ai/other.ts', 'const value = "https://huggingface.co/onnx-community/Qwen2.5-Coder-0.5B-Instruct";')).toBe(false);
    });
    it('continues rejecting remote scripts even from an approved model host', () => {
        expect(acceptsSource('browser-ai/index.html', '<script src="https://huggingface.co/runtime.js"></script>')).toBe(false);
    });
});
