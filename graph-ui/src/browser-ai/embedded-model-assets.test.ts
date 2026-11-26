import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

/** Run the actual MIME classifier, without executing asset-generation side effects. */
function embeddedMime(path: string): string {
    const script = readFileSync(new URL('../../../scripts/embed-frontend.sh', import.meta.url), 'utf8');
    const classifier = script.match(/content_type_for\(\) \{[\s\S]*?\n\}/)?.[0];
    if (!classifier) throw new Error('The embedded asset MIME classifier was not found.');
    return execFileSync('bash', ['-c', `${classifier}\ncontent_type_for "$1"`, 'mime-test', path], { encoding: 'utf8' }).trim();
}

describe('embedded browser model assets', () => {
    it('loads the configured MJS and WASM pair with the WebGPU backend ABI', () => {
        const worker = readFileSync(new URL('./browser-ai.worker.ts', import.meta.url), 'utf8');
        const mjsImport = worker.match(/import wasmLoaderUrl from '([^']+)\?url'/)?.[1];
        const wasmImport = worker.match(/import wasmUrl from '([^']+)\?url'/)?.[1];
        expect(mjsImport).toBeDefined(); expect(wasmImport).toBeDefined();
        const mjs = fileURLToPath(new URL(mjsImport!, import.meta.url));
        const wasm = fileURLToPath(new URL(wasmImport!, import.meta.url));
        // A fresh Node process loads the real emitted runtime factory without Vitest transforming it.
        // No model or GPU session is created. This catches the jsep/asyncify ABI mismatch directly.
        const check = `
            import { readFileSync } from 'node:fs';
            import { pathToFileURL } from 'node:url';
            import assert from 'node:assert/strict';
            const { default: createRuntime } = await import(pathToFileURL(process.argv[1]).href);
            const runtime = await createRuntime({ wasmBinary: readFileSync(process.argv[2]), numThreads: 1, print() {}, printErr() {} });
            assert.equal(typeof runtime.webgpuInit, 'function', 'Configured factory must expose the WebGPU backend ABI');
            assert.equal(typeof runtime._OrtInit, 'function', 'Configured factory must initialize the paired ONNX WASM');
        `;
        expect(() => execFileSync(process.execPath, ['--input-type=module', '-e', check, mjs, wasm], { stdio: 'pipe', timeout: 15000 })).not.toThrow();
    }, 20000);

    it('serves the runtime ES module as JavaScript under nosniff', () => {
        expect(embeddedMime('assets/ort-wasm-simd-threaded.asyncify-hash.mjs')).toBe('application/javascript');
    });
    it('serves the runtime WebAssembly binary with its actual MIME type', () => {
        expect(embeddedMime('assets/ort-wasm-simd-threaded.asyncify-hash.wasm')).toBe('application/wasm');
    });
    it('preserves ordinary worker scripts and unknown binary assets', () => {
        expect(embeddedMime('assets/browser-ai.worker-hash.js')).toBe('application/javascript');
        expect(embeddedMime('assets/unknown.bin')).toBe('application/octet-stream');
    });
});
