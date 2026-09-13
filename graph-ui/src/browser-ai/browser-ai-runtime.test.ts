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
