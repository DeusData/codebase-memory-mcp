// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from 'vitest';
import { keepAgent, offerAgent, peekAgent, switchKeepingAgent, takeAgent, type AgentHandover } from './agent-handover';
import type { BrowserChatRuntime } from './browser-ai-runtime';
import { BROWSER_MODEL, browserModelBaseUrl, BROWSER_MODELS, isBrowserModelCached } from './model-policy';

afterEach(() => { vi.unstubAllGlobals(); });

const runtime = () => ({ prepare: vi.fn(), explain: vi.fn(), countTokens: vi.fn(), chat: vi.fn(), stop: vi.fn(), dispose: vi.fn() }) as unknown as BrowserChatRuntime & { dispose: ReturnType<typeof vi.fn> };

describe('handing the loaded model to the next project (K24)', () => {
    it('takes the model from the offering dock before the new window renders and gives it to the next dock', () => {
        const model = runtime();
        const withdraw = offerAgent(() => ({ runtime: model, modelId: 'qwen' }));
        let seen: AgentHandover | undefined, taken: AgentHandover | undefined;
        switchKeepingAgent(() => { seen = peekAgent('qwen'); taken = takeAgent('qwen'); });
        withdraw();
        expect(seen?.runtime).toBe(model);
        expect(taken?.runtime).toBe(model);
        expect(model.dispose).not.toHaveBeenCalled();
    });

    it('unloads a model no dock takes, and one of another choice', () => {
        const left = runtime();
        const withdraw = offerAgent(() => ({ runtime: left, modelId: 'qwen' }));
        switchKeepingAgent(() => {});
        withdraw();
        expect(left.dispose).toHaveBeenCalledOnce();
        const other = runtime();
        const again = offerAgent(() => ({ runtime: other, modelId: 'qwen' }));
        switchKeepingAgent(() => { expect(peekAgent('lfm')).toBeUndefined(); expect(takeAgent('lfm')).toBeUndefined(); });
        again();
        expect(other.dispose).toHaveBeenCalledOnce();
    });

    it('keeps a model only inside a switch', () => {
        const model = runtime();
        expect(keepAgent({ runtime: model, modelId: 'qwen' })).toBe(false);
        expect(takeAgent('qwen')).toBeUndefined();
        switchKeepingAgent(() => {
            expect(keepAgent({ runtime: model, modelId: 'qwen' })).toBe(true);
            expect(takeAgent('qwen')?.runtime).toBe(model);
        });
        expect(model.dispose).not.toHaveBeenCalled();
    });
});

describe('a cached model (K10)', () => {
    const cacheWith = (urls: string[]) => ({ has: vi.fn(async () => true), open: vi.fn(async () => ({ keys: async () => urls.map(url => new Request(url)) })) });

    it('is cached only when every pinned file is in its dedicated cache', async () => {
        const model = BROWSER_MODELS[0];
        const all = model.files.map(file => `${browserModelBaseUrl(model)}${file}`);
        vi.stubGlobal('caches', cacheWith(all));
        expect(await isBrowserModelCached(model)).toBe(true);
        vi.stubGlobal('caches', cacheWith(all.slice(0, -1)));
        expect(await isBrowserModelCached(model)).toBe(false);
        vi.stubGlobal('caches', cacheWith(all.map(url => url.replace(BROWSER_MODEL.revision, 'main'))));
        expect(await isBrowserModelCached(model)).toBe(false);
    });

    it('never creates the cache it checks, and says not cached without Cache Storage', async () => {
        const storage = { has: vi.fn(async () => false), open: vi.fn() };
        vi.stubGlobal('caches', storage);
        expect(await isBrowserModelCached(BROWSER_MODELS[0])).toBe(false);
        expect(storage.open).not.toHaveBeenCalled();
        vi.stubGlobal('caches', undefined);
        expect(await isBrowserModelCached(BROWSER_MODELS[0])).toBe(false);
    });
});
