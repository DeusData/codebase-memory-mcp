import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { BrowserWorkerRequest, BrowserWorkerResponse } from './browser-ai-runtime';
import { BROWSER_MODEL, BROWSER_MODELS, browserModelBaseUrl } from './model-policy';

const fake = vi.hoisted(() => ({
    count: 23,
    template: vi.fn(),
    generate: vi.fn(),
    tokenizerLoad: vi.fn(),
    modelLoad: vi.fn(),
    env: { remotePathTemplate: '{model}/resolve/{revision}/', useWasmCache: true, fetch: undefined as typeof fetch | undefined, backends: { onnx: { wasm: {} } } },
}));
vi.mock('@huggingface/transformers', () => ({
    env: fake.env,
    AutoTokenizer: { from_pretrained: fake.tokenizerLoad },
    AutoModelForCausalLM: { from_pretrained: fake.modelLoad },
    InterruptableStoppingCriteria: class {
        interrupted = false;
        interrupt() { this.interrupted = true; }
        reset() { this.interrupted = false; }
    },
    TextStreamer: class {
        callback: (chunk: string) => void;
        constructor(_tokenizer: unknown, options: { callback_function: (chunk: string) => void }) { this.callback = options.callback_function; }
    },
}));

let scope: { location: { href: string }; postMessage: ReturnType<typeof vi.fn>; onmessage?: (event: MessageEvent<BrowserWorkerRequest>) => Promise<void> };
let fetchMock: ReturnType<typeof vi.fn>;
const send = (request: BrowserWorkerRequest) => scope.onmessage!({ data: request } as MessageEvent<BrowserWorkerRequest>);
const replies = (): BrowserWorkerResponse[] => scope.postMessage.mock.calls.map(call => call[0]);
beforeEach(async () => {
    vi.resetModules(); vi.clearAllMocks(); fake.count = 23;
    fake.env.useWasmCache = true; // Transformers.js 4 default in browsers with Cache Storage.
    fake.env.remotePathTemplate = '{model}/resolve/{revision}/';
    fake.template.mockImplementation(() => ({ input_ids: { dims: [1, fake.count], data: new BigInt64Array(fake.count) }, attention_mask: { dims: [1, fake.count] } }));
    fake.tokenizerLoad.mockResolvedValue({ apply_chat_template: fake.template });
    fake.modelLoad.mockResolvedValue({ generate: fake.generate });
    fake.generate.mockImplementation(async ({ streamer }: { streamer: { callback: (chunk: string) => void } }) => { streamer.callback('A '); streamer.callback('reply.'); });
    scope = { location: { href: 'http://localhost/worker.js' }, postMessage: vi.fn() };
    fetchMock = vi.fn(async () => ({ url: browserModelBaseUrl(BROWSER_MODELS[0]) + 'config.json' }));
    fake.env.fetch = fetchMock as unknown as typeof fetch; // v4 snapshots native fetch during import.
    vi.stubGlobal('self', scope);
    vi.stubGlobal('fetch', fetchMock);
    vi.stubGlobal('navigator', { userAgent: 'CBM fixture Chrome', vendor: '', gpu: { requestAdapter: async () => ({ features: { has: () => true } }) } });
    vi.stubGlobal('caches', { open: vi.fn(async () => ({})) });
    await import('./browser-ai.worker');
});
afterEach(() => vi.unstubAllGlobals());

describe('browser generation worker', () => {
    it('pins the real upstream tokenizer metadata probe even when it drops the revision option', async () => {
        const selected = BROWSER_MODELS[0];
        const allowed = browserModelBaseUrl(selected) + 'tokenizer_config.json';
        vi.stubGlobal('caches', { open: vi.fn(async () => ({ match: async () => undefined, put: async () => {} })) });
        fetchMock.mockResolvedValue({ url: allowed, status: 206, headers: new Headers({ 'content-range': 'bytes 0-0/100' }) });
        fake.tokenizerLoad.mockImplementationOnce(async () => {
            // Exercise the installed v4 metadata implementation, not a duplicate URL builder.
            // @ts-expect-error Upstream exposes declarations only through its package entry point.
            const { env: upstreamEnv } = await import('../../node_modules/@huggingface/transformers/src/env.js');
            Object.assign(upstreamEnv, fake.env);
            // @ts-expect-error Upstream exposes declarations only through its package entry point.
            const { get_tokenizer_files } = await import('../../node_modules/@huggingface/transformers/src/utils/model_registry/get_tokenizer_files.js');
            const files = await get_tokenizer_files(selected.id); // This API has no revision argument.
            expect(files).toEqual(['tokenizer.json', 'tokenizer_config.json']);
            return { apply_chat_template: fake.template };
        });
        await send({ id: 1, kind: 'prepare', modelId: selected.id });
        expect(replies().at(-1)).toEqual({ id: 1, kind: 'ready' });
        expect(fetchMock).toHaveBeenCalledExactlyOnceWith(allowed, expect.objectContaining({ method: 'GET', credentials: 'omit' }));
    });

    it('routes Transformers own fetch through the pinned model download boundary', async () => {
        const selected = BROWSER_MODELS[1];
        const allowed = browserModelBaseUrl(selected) + 'config.json';
        fake.tokenizerLoad.mockImplementationOnce(async () => {
            await expect(fake.env.fetch!(allowed, { method: 'POST', body: 'private' })).rejects.toThrow('outside');
            await expect(fake.env.fetch!(browserModelBaseUrl(BROWSER_MODELS[0]) + 'config.json')).rejects.toThrow('outside');
            await fake.env.fetch!(allowed);
            return { apply_chat_template: fake.template };
        });
        await send({ id: 1, kind: 'prepare', modelId: selected.id });
        expect(replies().at(-1)).toEqual({ id: 1, kind: 'ready' });
        expect(fetchMock).toHaveBeenCalledExactlyOnceWith(allowed, expect.objectContaining({ credentials: 'omit' }));
        await expect(fake.env.fetch!(allowed)).rejects.toThrow('outside');
    });

    it('keeps the bundled runtime module at a direct same-origin URL under restrictive CSP', async () => {
        fake.modelLoad.mockImplementationOnce(async () => {
            const wasm = fake.env.backends.onnx.wasm as { wasmPaths: { mjs: string; wasm: string }; proxy: boolean };
            // Upstream v4 turns wasmPaths.mjs into a blob URL when useWasmCache is enabled.
            // Product CSP permits the local module, but intentionally does not permit blob scripts.
            expect(fake.env.useWasmCache).toBe(false);
            expect(new URL(wasm.wasmPaths.mjs).origin).toBe(new URL(scope.location.href).origin);
            expect(new URL(wasm.wasmPaths.wasm).origin).toBe(new URL(scope.location.href).origin);
            expect(wasm.wasmPaths.mjs).toContain('.mjs');
            expect(wasm.wasmPaths.wasm).toContain('.wasm');
            expect(wasm.proxy).toBe(false);
            return { generate: fake.generate };
        });
        await send({ id: 1, kind: 'prepare' });
        expect(replies().at(-1)).toEqual({ id: 1, kind: 'ready' });
    });

    it('requires explicit prepare and WebGPU before accessing model files', async () => {
        expect(fake.modelLoad).not.toHaveBeenCalled();
        await send({ id: 1, kind: 'chat', messages: [{ role: 'user', content: 'hello' }] });
        expect(replies().at(-1)?.error).toContain('Load a browser model');
        vi.stubGlobal('navigator', {});
        await send({ id: 2, kind: 'prepare' });
        expect(replies().at(-1)?.error).toContain('WebGPU');
        expect(fake.tokenizerLoad).not.toHaveBeenCalled();
        expect(fetchMock).not.toHaveBeenCalled();
    });

    it('uses exact template tokens for counting and generation with no truncation and output reserve', async () => {
        await send({ id: 1, kind: 'prepare' });
        const messages = [{ role: 'user' as const, content: '\t' + 'x'.repeat(7000) + '\r\n  ' }];
        fake.count = 7680;
        await send({ id: 2, kind: 'count', messages });
        expect(replies().at(-1)).toEqual({ id: 2, kind: 'count', count: 7680 });
        await send({ id: 3, kind: 'chat', messages });
        expect(fake.template).toHaveBeenLastCalledWith(messages, expect.objectContaining({ truncation: false, enable_thinking: false, add_generation_prompt: true }));
        expect(fake.generate).toHaveBeenCalledWith(expect.objectContaining({ input_ids: expect.objectContaining({ dims: [1, 7680] }), max_new_tokens: 512, do_sample: false }));
        expect(replies().filter(reply => reply.kind === 'token').map(reply => reply.output)).toEqual(['A ', 'reply.']);
        expect(replies().at(-1)).toEqual({ id: 3, kind: 'answer', output: 'A reply.' });
        fake.count = 7681; fake.generate.mockClear();
        await send({ id: 4, kind: 'chat', messages });
        expect(fake.generate).not.toHaveBeenCalled();
        expect(replies().at(-1)?.error).toContain('no code was truncated');
    });

    it('interrupts the active generation and reuses the loaded model for the next turn', async () => {
        await send({ id: 1, kind: 'prepare' });
        let complete!: () => void;
        let criteria!: { interrupted: boolean };
        fake.generate.mockImplementationOnce(({ streamer, stopping_criteria }: { streamer: { callback: (chunk: string) => void }; stopping_criteria: { interrupted: boolean }[] }) => {
            streamer.callback('Partial'); criteria = stopping_criteria[0];
            return new Promise<void>(resolve => { complete = resolve; });
        });
        const generating = send({ id: 2, kind: 'chat', messages: [{ role: 'user', content: 'first' }] });
        await send({ id: 1, kind: 'stop' }); expect(criteria.interrupted).toBe(false);
        await send({ id: 2, kind: 'stop' }); expect(criteria.interrupted).toBe(true);
        complete(); await generating;
        expect(replies().at(-1)).toEqual({ id: 2, kind: 'answer', output: 'Partial' });
        await send({ id: 3, kind: 'chat', messages: [{ role: 'user', content: 'second' }] });
        expect(fake.modelLoad).toHaveBeenCalledOnce();
        expect(criteria.interrupted).toBe(false);
        expect(replies().at(-1)?.output).toBe('A reply.');
    });

    it('pins model loading and permits only selected model GETs during preparation', async () => {
        const selected = BROWSER_MODELS[2];
        const allowed = browserModelBaseUrl(selected) + 'config.json';
        await expect(fetch(allowed)).rejects.toThrow('outside');
        fake.tokenizerLoad.mockImplementationOnce(async () => {
            await expect(fetch(allowed, { method: 'POST', body: 'private' })).rejects.toThrow('outside');
            await expect(fetch(browserModelBaseUrl(BROWSER_MODELS[0]) + 'config.json')).rejects.toThrow('outside');
            await fetch(allowed);
            return { apply_chat_template: fake.template };
        });
        await send({ id: 1, kind: 'prepare', modelId: selected.id });
        expect(fake.modelLoad).toHaveBeenCalledWith(selected.id, expect.objectContaining({ revision: selected.revision, dtype: 'q4f16', device: 'webgpu' }));
        expect(fetchMock).toHaveBeenCalledExactlyOnceWith(allowed, expect.objectContaining({ credentials: 'omit', cache: 'no-store' }));
        await expect(fetch(allowed)).rejects.toThrow('outside');
        expect(fake.env).toMatchObject({ allowLocalModels: false, useCustomCache: true, useBrowserCache: false });
        expect(selected.id).not.toBe(BROWSER_MODEL.id);
    });
});
