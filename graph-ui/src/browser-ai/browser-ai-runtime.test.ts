import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createBrowserChatRuntime } from './browser-ai-runtime';
import type { BrowserWorkerRequest, BrowserWorkerResponse } from './browser-ai-runtime';
import { BROWSER_MODELS } from './model-policy';

class WorkerStub {
    static instances: WorkerStub[] = [];
    onmessage?: (event: MessageEvent<BrowserWorkerResponse>) => void;
    onerror?: (event: ErrorEvent) => void;
    postMessage = vi.fn((_message: BrowserWorkerRequest) => {});
    terminate = vi.fn();
    constructor() { WorkerStub.instances.push(this); }
    send(response: BrowserWorkerResponse) { this.onmessage?.({ data: response } as MessageEvent<BrowserWorkerResponse>); }
    last(): BrowserWorkerRequest { return this.postMessage.mock.calls.at(-1)![0]; }
}

beforeEach(() => { WorkerStub.instances = []; vi.stubGlobal('Worker', WorkerStub); });
afterEach(() => vi.unstubAllGlobals());

describe('browser chat worker boundary', () => {
    it('terminates a poisoned GPU worker, preserves diagnostics, and refuses reuse until a fresh runtime is created', async () => {
        const runtime = createBrowserChatRuntime(), worker = WorkerStub.instances[0], chunks = vi.fn();
        const messages = [{ role: 'user' as const, content: 'Explain the selected function' }];
        const pending = runtime.chat(messages, chunks);
        const detail = 'OrtRun failed: GPUBuffer mapAsync rejected because the buffer is invalid';
        worker.send({ id: 1, kind: 'error', error: detail, ...{ fatal: true } });
        const failure = await pending.catch(error => error);
        expect(failure).toMatchObject({ code: 'BROWSER_RUNTIME_FATAL', detail });
        expect(failure.message).toContain('Reload the model');
        expect(worker.terminate).toHaveBeenCalledOnce();
        worker.send({ id: 1, kind: 'token', output: 'stale poisoned output' });
        expect(chunks).not.toHaveBeenCalled();
        await expect(runtime.prepare(vi.fn())).rejects.toMatchObject({ code: 'BROWSER_RUNTIME_FATAL' });
        expect(worker.postMessage).toHaveBeenCalledOnce();
        runtime.dispose(); expect(worker.terminate).toHaveBeenCalledOnce();
        const fresh = createBrowserChatRuntime(), nextWorker = WorkerStub.instances[1];
        const ready = fresh.prepare(vi.fn()); nextWorker.send({ id: 1, kind: 'ready' }); await ready;
        fresh.dispose();
    });

    it('invalidates an uncaught worker failure even when no request is pending', async () => {
        const runtime = createBrowserChatRuntime(), worker = WorkerStub.instances[0];
        worker.onerror?.({ message: 'WebGPU device lost' } as ErrorEvent);
        expect(worker.terminate).toHaveBeenCalledOnce();
        const next = runtime.prepare(vi.fn());
        // Make a wrongly reused worker settle, so this test cannot hang on the broken implementation.
        if (worker.postMessage.mock.calls.length) worker.send({ id: 1, kind: 'ready' });
        await expect(next).rejects.toMatchObject({ code: 'BROWSER_RUNTIME_FATAL', detail: 'WebGPU device lost' });
        runtime.dispose();
    });

    it('invalidates a GPU failure received from an older worker without a fatal flag', async () => {
        const runtime = createBrowserChatRuntime(), worker = WorkerStub.instances[0];
        const next = runtime.chat([{ role: 'user', content: 'Explain' }], vi.fn());
        worker.send({ id: 1, kind: 'error', error: 'mapAsync failed on an invalid GPUBuffer' });
        await expect(next).rejects.toMatchObject({ code: 'BROWSER_RUNTIME_FATAL' });
        expect(worker.terminate).toHaveBeenCalledOnce(); runtime.dispose();
    });

    it('invalidates an asynchronous fatal report even when no request is pending', async () => {
        const runtime = createBrowserChatRuntime(), worker = WorkerStub.instances[0];
        worker.send({ id: 0, kind: 'error', error: 'GPU device lost', ...{ fatal: true } });
        expect(worker.terminate).toHaveBeenCalledOnce();
        const next = runtime.prepare(vi.fn());
        if (worker.postMessage.mock.calls.length) worker.send({ id: 1, kind: 'ready' });
        await expect(next).rejects.toMatchObject({ code: 'BROWSER_RUNTIME_FATAL' });
        runtime.dispose();
    });

    it('forwards the automatic explanation profile without changing ordinary chat requests', async () => {
        const runtime = createBrowserChatRuntime(), worker = WorkerStub.instances[0];
        const messages = [{ role: 'user' as const, content: 'Explain' }];
        const result = runtime.chat(messages, vi.fn(), { maxOutputTokens: 128, generationProfile: 'automatic-explanation' });
        const request = worker.last(); worker.send({ id: request.id, kind: 'answer', output: 'Short answer' });
        await result;
        expect(request).toMatchObject({ maxOutputTokens: 128, generationProfile: 'automatic-explanation' });
        const ordinary = runtime.chat(messages, vi.fn());
        const next = worker.last(); worker.send({ id: next.id, kind: 'answer', output: 'Ordinary answer' });
        await ordinary;
        expect(next).not.toHaveProperty('maxOutputTokens');
        expect(next).not.toHaveProperty('generationProfile');
        runtime.dispose();
    });

    it('reports the worker stop reason before the answer resolves and never posts the callback', async () => {
        const runtime = createBrowserChatRuntime(), worker = WorkerStub.instances[0];
        const onComplete = vi.fn();
        const result = runtime.chat([{ role: 'user', content: 'List' }], vi.fn(), { maxOutputTokens: 64, onComplete });
        const request = worker.last();
        expect(request).not.toHaveProperty('onComplete');
        worker.send({ id: request.id, kind: 'answer', output: 'Cut', stopReason: 'length' });
        expect(await result).toBe('Cut');
        expect(onComplete).toHaveBeenCalledExactlyOnceWith({ stopReason: 'length' });
        const older = runtime.chat([{ role: 'user', content: 'List' }], vi.fn(), { onComplete });
        worker.send({ id: worker.last().id, kind: 'answer', output: 'Older worker' });
        await older;
        expect(onComplete).toHaveBeenLastCalledWith({ stopReason: 'eos' });
        runtime.dispose();
    });

    it('forwards a bounded token request independently of an explanation profile', async () => {
        const runtime = createBrowserChatRuntime(), worker = WorkerStub.instances[0];
        const result = runtime.chat([{ role: 'user', content: 'Explain' }], vi.fn(), { maxOutputTokens: 192 });
        const request = worker.last(); worker.send({ id: request.id, kind: 'answer', output: 'Short answer' });
        await result;
        expect(request).toMatchObject({ maxOutputTokens: 192 });
        expect(request).not.toHaveProperty('generationProfile');
        runtime.dispose();
    });

    it('notifies the UI once about an idle fatal loss and immediately reports an already failed runtime', () => {
        const runtime = createBrowserChatRuntime() as ReturnType<typeof createBrowserChatRuntime> & { setFatalHandler?: (handler: ((error: Error) => void) | undefined) => void };
        const worker = WorkerStub.instances[0], handler = vi.fn();
        runtime.setFatalHandler?.(handler);
        worker.send({ id: 0, kind: 'error', error: 'GPU device lost', ...{ fatal: true } });
        expect(handler).toHaveBeenCalledExactlyOnceWith(expect.objectContaining({ code: 'BROWSER_RUNTIME_FATAL', detail: 'GPU device lost' }));
        worker.send({ id: 0, kind: 'error', error: 'GPU device lost again', ...{ fatal: true } });
        expect(handler).toHaveBeenCalledOnce();
        const late = vi.fn(); runtime.setFatalHandler?.(late);
        expect(late).toHaveBeenCalledExactlyOnceWith(handler.mock.calls[0][0]);
        runtime.dispose();
    });

    it('creates no model requests until explicit prepare, then binds the selected model', async () => {
        const runtime = createBrowserChatRuntime(BROWSER_MODELS[1].id);
        const worker = WorkerStub.instances[0];
        expect(worker.postMessage).not.toHaveBeenCalled();
        const progress = vi.fn();
        const preparing = runtime.prepare(progress);
        expect(worker.last()).toEqual({ id: 1, kind: 'prepare', modelId: BROWSER_MODELS[1].id });
        worker.send({ id: 1, kind: 'progress', progress: { loaded: 23, total: 100 } });
        expect(progress).toHaveBeenCalledWith({ loaded: 23, total: 100 });
        worker.send({ id: 1, kind: 'ready' });
        await preparing;
        runtime.dispose();
    });

    it('asks the worker for a cache-only load when told to (K10, K24)', async () => {
        const runtime = createBrowserChatRuntime();
        const worker = WorkerStub.instances[0];
        const preparing = runtime.prepare(vi.fn(), { cacheOnly: true });
        expect(worker.last()).toEqual({ id: 1, kind: 'prepare', modelId: BROWSER_MODELS[0].id, cacheOnly: true });
        worker.send({ id: 1, kind: 'ready' });
        await preparing;
        runtime.dispose();
    });

    it('streams only the active request, preserves exact content, and counts via the worker', async () => {
        const runtime = createBrowserChatRuntime(); const worker = WorkerStub.instances[0];
        const messages = [{ role: 'user' as const, content: '\t' + 'x'.repeat(7000) + '\r\n  ' }];
        const count = runtime.countTokens(messages);
        expect(worker.last().messages).toEqual(messages);
        worker.send({ id: 1, kind: 'count', count: 2102 }); expect(await count).toBe(2102);
        const chunks = vi.fn(); const answer = runtime.chat(messages, chunks);
        expect(worker.last().messages).toEqual(messages);
        worker.send({ id: 1, kind: 'token', output: 'stale' });
        worker.send({ id: 2, kind: 'token', output: 'First ' });
        worker.send({ id: 2, kind: 'token', output: 'second' });
        expect(chunks.mock.calls).toEqual([['First '], ['second']]);
        worker.send({ id: 2, kind: 'answer', output: 'First second' });
        expect(await answer).toBe('First second'); runtime.dispose();
    });

    it('stops generation without unloading and keeps busy until the partial answer settles', async () => {
        const runtime = createBrowserChatRuntime(); const worker = WorkerStub.instances[0];
        const messages = [{ role: 'user' as const, content: 'Explain' }];
        const answer = runtime.chat(messages, vi.fn());
        runtime.stop();
        expect(worker.last()).toEqual({ id: 1, kind: 'stop' });
        expect(worker.terminate).not.toHaveBeenCalled();
        await expect(runtime.chat(messages, vi.fn())).rejects.toThrow('already busy');
        worker.send({ id: 1, kind: 'answer', output: 'Partial' });
        expect(await answer).toBe('Partial');
        const next = runtime.chat(messages, vi.fn());
        expect(worker.last().kind).toBe('chat');
        worker.send({ id: 2, kind: 'answer', output: 'Another answer' });
        await next; runtime.dispose();
    });

    it('unloading rejects pending work, ignores late responses and prevents re-use', async () => {
        const runtime = createBrowserChatRuntime(); const worker = WorkerStub.instances[0];
        const chunks = vi.fn(); const pending = runtime.chat([{ role: 'user', content: 'hello' }], chunks);
        const rejection = expect(pending).rejects.toThrow('unloaded');
        runtime.dispose(); await rejection;
        worker.send({ id: 1, kind: 'token', output: 'late' });
        expect(chunks).not.toHaveBeenCalled();
        expect(worker.terminate).toHaveBeenCalledOnce();
        await expect(runtime.prepare(vi.fn())).rejects.toThrow('unloaded');
    });

    it('surfaces worker errors and rejects invalid token counts', async () => {
        const runtime = createBrowserChatRuntime(); const worker = WorkerStub.instances[0];
        const failed = runtime.prepare(vi.fn());
        worker.send({ id: 1, kind: 'error', error: 'No WebGPU' });
        await expect(failed).rejects.toThrow('No WebGPU');
        const count = runtime.countTokens([{ role: 'user', content: 'hello' }]);
        worker.send({ id: 2, kind: 'count', count: -1 });
        await expect(count).rejects.toThrow('invalid token count'); runtime.dispose();
    });
});
