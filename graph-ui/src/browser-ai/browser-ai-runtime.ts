import type { BrowserAiProgress, BrowserAiRuntime, BrowserAiSource, BrowserChatMessage, BrowserChatOptions, BrowserChatRuntime, BrowserStopReason } from './browser-ai-controller';
import { BROWSER_MODEL, getBrowserModel } from './model-policy';
import { BrowserRuntimeFatalError, isFatalBrowserRuntimeError, isGpuRuntimeFailure, runtimeErrorDetail } from './runtime-fault';
export { BrowserRuntimeFatalError, isFatalBrowserRuntimeError } from './runtime-fault';

export type { BrowserAiProgress, BrowserChatMessage, BrowserChatRuntime } from './browser-ai-controller';

export interface BrowserWorkerRequest {
    id: number;
    kind: 'prepare' | 'explain' | 'count' | 'chat' | 'stop';
    modelId?: string;
    source?: BrowserAiSource;
    messages?: readonly BrowserChatMessage[];
    maxOutputTokens?: number;
    generationProfile?: BrowserChatOptions['generationProfile'];
    /** Prepare from the browser cache only: no model download is allowed. */
    cacheOnly?: boolean;
}
export interface BrowserWorkerResponse {
    id: number;
    kind: 'progress' | 'token' | 'ready' | 'answer' | 'count' | 'error';
    output?: string;
    count?: number;
    error?: string;
    fatal?: boolean;
    progress?: BrowserAiProgress;
    stopReason?: BrowserStopReason;
}

/** One model per worker. Only prepare can authorize a download. Stop keeps GPU/model state alive. */
export function createBrowserChatRuntime(modelId: string = BROWSER_MODEL.id): BrowserChatRuntime {
    const model = getBrowserModel(modelId);
    if (model.availability !== 'available') throw new Error(model.compatibilityNote);
    const worker = new Worker(new URL('./browser-ai.worker.ts', import.meta.url), { type: 'module' });
    let sequence = 0;
    let disposed = false;
    let fatalFailure: BrowserRuntimeFatalError | undefined;
    let fatalHandler: ((error: Error) => void) | undefined;
    let pending: {
        id: number;
        kind: BrowserWorkerRequest['kind'];
        resolve: (value: BrowserWorkerResponse) => void;
        reject: (error: Error) => void;
        progress?: (value: BrowserAiProgress) => void;
        onToken?: (chunk: string) => void;
    } | undefined;
    const failRuntime = (failure: unknown): void => {
        if (disposed) return;
        fatalFailure = isFatalBrowserRuntimeError(failure) ? failure : new BrowserRuntimeFatalError(failure);
        disposed = true;
        worker.terminate();
        const request = pending; pending = undefined;
        request?.reject(fatalFailure);
        fatalHandler?.(fatalFailure);
    };
    worker.onmessage = (event: MessageEvent<BrowserWorkerResponse>) => {
        if (disposed) return;
        const { kind } = event.data;
        // Device loss is worker-wide and may arrive while idle (id 0).
        if (kind === 'error' && (event.data.fatal || isGpuRuntimeFailure(event.data.error))) {
            failRuntime(event.data.error ?? 'Browser GPU runtime failed.'); return;
        }
        if (!pending || pending.id !== event.data.id) return;
        if (kind === 'progress') { pending.progress?.(event.data.progress ?? {}); return; }
        if (kind === 'token') { pending.onToken?.(event.data.output ?? ''); return; }
        const request = pending; pending = undefined;
        if (kind === 'error') request.reject(new Error(event.data.error ?? 'Browser model failed.'));
        else request.resolve(event.data);
    };
    worker.onerror = event => {
        failRuntime(event.message || 'Browser model worker failed.');
    };
    const request = (
        payload: Omit<BrowserWorkerRequest, 'id' | 'modelId'>,
        callbacks: { progress?: (value: BrowserAiProgress) => void; onToken?: (chunk: string) => void } = {},
    ): Promise<BrowserWorkerResponse> => new Promise((resolve, reject) => {
        if (disposed) { reject(fatalFailure ?? new Error('The browser model was unloaded. Load it again to continue.')); return; }
        if (pending) { reject(new Error('The browser model is already busy.')); return; }
        const id = ++sequence;
        pending = { id, kind: payload.kind, resolve, reject, ...callbacks };
        try { worker.postMessage({ ...payload, id, modelId } satisfies BrowserWorkerRequest); }
        catch (error) {
            if (isGpuRuntimeFailure(error)) failRuntime(error);
            else { pending = undefined; reject(error instanceof Error ? error : new Error(runtimeErrorDetail(error))); }
        }
    });
    return {
        prepare: async (progress, options) => { await request({ kind: 'prepare', ...options?.cacheOnly ? { cacheOnly: true } : {} }, { progress }); },
        countTokens: async messages => {
            const response = await request({ kind: 'count', messages });
            if (!Number.isSafeInteger(response.count) || response.count! < 0) throw new Error('The model returned an invalid token count.');
            return response.count!;
        },
        chat: async (messages, onToken, options) => {
            const response = await request({
                kind: 'chat', messages,
                ...(options?.maxOutputTokens === undefined ? {} : { maxOutputTokens: options.maxOutputTokens }),
                ...(options?.generationProfile === undefined ? {} : { generationProfile: options.generationProfile }),
            }, { onToken });
            options?.onComplete?.({ stopReason: response.stopReason ?? 'eos' });
            return response.output ?? '';
        },
        setFatalHandler: handler => {
            fatalHandler = handler;
            if (fatalFailure) handler?.(fatalFailure);
        },
        explain: async source => (await request({ kind: 'explain', source })).output ?? '',
        stop: () => {
            if (!disposed && pending && (pending.kind === 'chat' || pending.kind === 'explain')) {
                worker.postMessage({ id: pending.id, kind: 'stop' } satisfies BrowserWorkerRequest);
            }
        },
        dispose: () => {
            fatalHandler = undefined;
            if (disposed) return;
            disposed = true;
            worker.terminate();
            pending?.reject(new Error('The browser model was unloaded.'));
            pending = undefined;
        },
    };
}

export function createBrowserAiRuntime(): BrowserAiRuntime { return createBrowserChatRuntime(); }
