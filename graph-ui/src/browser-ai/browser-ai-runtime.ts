import type { BrowserAiProgress, BrowserAiRuntime, BrowserAiSource, BrowserChatMessage, BrowserChatRuntime } from './browser-ai-controller';
import { BROWSER_MODEL, getBrowserModel } from './model-policy';

export type { BrowserAiProgress, BrowserChatMessage, BrowserChatRuntime } from './browser-ai-controller';

export interface BrowserWorkerRequest {
    id: number;
    kind: 'prepare' | 'explain' | 'count' | 'chat' | 'stop';
    modelId?: string;
    source?: BrowserAiSource;
    messages?: readonly BrowserChatMessage[];
}
export interface BrowserWorkerResponse {
    id: number;
    kind: 'progress' | 'token' | 'ready' | 'answer' | 'count' | 'error';
    output?: string;
    count?: number;
    error?: string;
    progress?: BrowserAiProgress;
}

/** One model per worker. Only prepare can authorize a download. Stop keeps GPU/model state alive. */
export function createBrowserChatRuntime(modelId: string = BROWSER_MODEL.id): BrowserChatRuntime {
    const model = getBrowserModel(modelId);
    if (model.availability !== 'available') throw new Error(model.compatibilityNote);
    const worker = new Worker(new URL('./browser-ai.worker.ts', import.meta.url), { type: 'module' });
    let sequence = 0;
    let disposed = false;
    let pending: {
        id: number;
        kind: BrowserWorkerRequest['kind'];
        resolve: (value: BrowserWorkerResponse) => void;
        reject: (error: Error) => void;
        progress?: (value: BrowserAiProgress) => void;
        onToken?: (chunk: string) => void;
    } | undefined;
    worker.onmessage = (event: MessageEvent<BrowserWorkerResponse>) => {
        if (!pending || pending.id !== event.data.id || disposed) return;
        const { kind } = event.data;
        if (kind === 'progress') { pending.progress?.(event.data.progress ?? {}); return; }
        if (kind === 'token') { pending.onToken?.(event.data.output ?? ''); return; }
        const request = pending; pending = undefined;
        if (kind === 'error') request.reject(new Error(event.data.error ?? 'Browser model failed.'));
        else request.resolve(event.data);
    };
    worker.onerror = event => {
        pending?.reject(new Error(event.message || 'Browser model worker failed.'));
        pending = undefined;
    };
    const request = (
        payload: Omit<BrowserWorkerRequest, 'id' | 'modelId'>,
        callbacks: { progress?: (value: BrowserAiProgress) => void; onToken?: (chunk: string) => void } = {},
    ): Promise<BrowserWorkerResponse> => new Promise((resolve, reject) => {
        if (disposed) { reject(new Error('The browser model was unloaded. Load it again to continue.')); return; }
        if (pending) { reject(new Error('The browser model is already busy.')); return; }
        const id = ++sequence;
        pending = { id, kind: payload.kind, resolve, reject, ...callbacks };
        try { worker.postMessage({ ...payload, id, modelId } satisfies BrowserWorkerRequest); }
        catch (error) { pending = undefined; reject(error instanceof Error ? error : new Error(String(error))); }
    });
    return {
        prepare: async progress => { await request({ kind: 'prepare' }, { progress }); },
        countTokens: async messages => {
            const response = await request({ kind: 'count', messages });
            if (!Number.isSafeInteger(response.count) || response.count! < 0) throw new Error('The model returned an invalid token count.');
            return response.count!;
        },
        chat: async (messages, onToken) => (await request({ kind: 'chat', messages }, { onToken })).output ?? '',
        explain: async source => (await request({ kind: 'explain', source })).output ?? '',
        stop: () => {
            if (!disposed && pending && (pending.kind === 'chat' || pending.kind === 'explain')) {
                worker.postMessage({ id: pending.id, kind: 'stop' } satisfies BrowserWorkerRequest);
            }
        },
        dispose: () => {
            disposed = true;
            worker.terminate();
            pending?.reject(new Error('The browser model was unloaded.'));
            pending = undefined;
        },
    };
}

export function createBrowserAiRuntime(): BrowserAiRuntime { return createBrowserChatRuntime(); }
