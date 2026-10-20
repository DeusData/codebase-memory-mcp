// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { CHAT_HISTORY_SAVE_MS, useChatHistory, type ChatHistoryCache } from './use-chat-history';
import type { BrowserChatHistorySaveResult, BrowserChatHistorySnapshot } from './chat-history-cache';
import type { BrowserChatTurn } from './chat-model';

const turn = (id = 'one', answer = 'answer'): BrowserChatTurn => ({ id, prompt: 'Explain this.', modelId: 'fixture', request: [{ role: 'user', content: 'Explain this.' }], answer, status: 'complete' });
const snapshot = (turns = [turn()], draft = ''): BrowserChatHistorySnapshot => ({ turns, draft, savedAt: 1000, expiresAt: 86401000, trimmed: false });
const saved: BrowserChatHistorySaveResult = { saved: true, trimmed: false, droppedTurns: 0, evictedProjects: 0 };
const deferred = <T,>() => {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail; });
    return { promise, resolve, reject };
};
const fakeCache = () => ({ load: vi.fn<ChatHistoryCache['load']>().mockResolvedValue(null), save: vi.fn<ChatHistoryCache['save']>().mockResolvedValue(saved), delete: vi.fn<ChatHistoryCache['delete']>().mockResolvedValue(undefined) });
let cache: ReturnType<typeof fakeCache>;
let container: HTMLDivElement, root: Root;
let history: ReturnType<typeof useChatHistory>;
function Probe({ historyKey }: { historyKey?: string }) {
    history = useChatHistory(historyKey, cache);
    return <div data-ready={history.ready}><span>{history.turns.map(item => item.answer).join('|')}</span><input readOnly value={history.draft} /></div>;
}
const render = async (key?: string) => { await act(async () => { root.render(<Probe historyKey={key} />); }); };
const advance = async (ms = CHAT_HISTORY_SAVE_MS) => { await act(async () => { await vi.advanceTimersByTimeAsync(ms); }); };
beforeEach(() => {
    vi.useFakeTimers();
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    cache = fakeCache(); container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => { root.unmount(); }); container.remove(); vi.useRealTimers(); });

describe('Project chat history lifecycle', () => {
    it('keeps undefined keys in memory without storage operations', async () => {
        await render();
        expect(history.ready).toBe(true);
        await act(async () => { history.setTurns([turn()]); history.setDraft('local draft'); });
        await advance(5000);
        expect(history.turns).toHaveLength(1); expect(history.draft).toBe('local draft');
        await act(async () => { await history.clearHistory(); });
        expect(history.turns).toEqual([]); expect(history.draft).toBe('');
        expect(cache.load).not.toHaveBeenCalled(); expect(cache.save).not.toHaveBeenCalled(); expect(cache.delete).not.toHaveBeenCalled();
    });
    it('restores stopped turns and draft without renewing their storage TTL', async () => {
        cache.load.mockResolvedValue(snapshot([{ ...turn(), status: 'stopped' }], 'saved draft'));
        await render('origin/project-a');
        expect(history.ready).toBe(true); expect(history.turns[0].status).toBe('stopped'); expect(history.draft).toBe('saved draft');
        await act(async () => { history.setTurns(items => items.map(item => ({ ...item }))); });
        await advance(5000);
        await act(async () => { window.dispatchEvent(new Event('pagehide')); });
        expect(cache.save).not.toHaveBeenCalled();
        await act(async () => { root.render(null); });
        expect(cache.save).not.toHaveBeenCalled();
        await render('origin/project-a');
        expect(history.turns[0].status).toBe('stopped');
        expect(cache.load).toHaveBeenCalledTimes(2); expect(cache.save).not.toHaveBeenCalled();
    });
    it('hides old project data immediately and fences old setters and late loads', async () => {
        const first = deferred<BrowserChatHistorySnapshot | null>();
        cache.load.mockImplementation(key => key === 'a' ? first.promise : Promise.resolve(snapshot([turn('b', 'project B')])));
        await render('a');
        const staleSetTurns = history.setTurns, staleSetDraft = history.setDraft;
        await render('b');
        expect(history.turns).toEqual([]); expect(history.draft).toBe(''); expect(history.ready).toBe(false);
        await act(async () => { staleSetTurns([turn('a', 'late A')]); staleSetDraft('late A draft'); first.resolve(snapshot([turn('a', 'loaded A')])); });
        expect(history.turns.map(item => item.id)).toEqual(['b']);
        expect(history.draft).toBe(''); expect(history.ready).toBe(true);
        expect(cache.save).not.toHaveBeenCalled();
    });
    it('preserves typing during hydration while restoring untouched conversation state', async () => {
        const loading = deferred<BrowserChatHistorySnapshot | null>(); cache.load.mockReturnValue(loading.promise);
        await render('a');
        await act(async () => { history.setDraft('typed before load'); });
        await advance();
        expect(cache.save).not.toHaveBeenCalled();
        await act(async () => { loading.resolve(snapshot([turn()], 'older draft')); });
        expect(history.draft).toBe('typed before load'); expect(history.turns).toHaveLength(1);
        expect(cache.save).toHaveBeenCalledExactlyOnceWith('a', { turns: [turn()], draft: 'typed before load' });
    });
    it('preserves turn edits during hydration without throwing away an untouched saved draft', async () => {
        const loading = deferred<BrowserChatHistorySnapshot | null>(); cache.load.mockReturnValue(loading.promise);
        await render('a');
        await act(async () => { history.setTurns([turn('new', 'user-added conversation')]); loading.resolve(snapshot([turn('old')], 'stored draft')); });
        expect(history.turns[0].id).toBe('new'); expect(history.draft).toBe('stored draft');
        await advance();
        expect(cache.save).toHaveBeenCalledExactlyOnceWith('a', { turns: [turn('new', 'user-added conversation')], draft: 'stored draft' });
    });
    it('throttles streaming updates without postponing persistence until streaming ends', async () => {
        await render('a');
        await act(async () => { history.setTurns([{ ...turn('one', 'a'), status: 'generating' }]); });
        await advance(400);
        await act(async () => { history.setTurns(items => items.map(item => ({ ...item, answer: 'ab' }))); });
        await advance(400);
        await act(async () => { history.setTurns(items => items.map(item => ({ ...item, answer: 'abc' }))); });
        await advance(200);
        expect(cache.save).toHaveBeenCalledTimes(1);
        expect(cache.save.mock.calls[0][1].turns[0].answer).toBe('abc');
        await act(async () => { history.setTurns(items => items.map(item => ({ ...item, answer: 'abcd' }))); });
        await advance();
        expect(cache.save).toHaveBeenCalledTimes(2);
    });
    it('flushes the departing key before loading another key and ignores its stale setters', async () => {
        const events: string[] = [];
        cache.load.mockImplementation(async key => { events.push(`load:${key}`); return null; });
        cache.save.mockImplementation(async (key) => { events.push(`save:${key}`); return saved; });
        await render('a');
        const stale = history.setDraft;
        await act(async () => { history.setDraft('A draft'); });
        await render('b');
        await act(async () => { stale('must not reach B'); });
        expect(events).toEqual(['load:a', 'save:a', 'load:b']);
        expect(history.draft).toBe('');
        expect(cache.save).toHaveBeenCalledExactlyOnceWith('a', { turns: [], draft: 'A draft' });
    });
    it('flushes pending hydration edits before the next project loads', async () => {
        const loading = deferred<BrowserChatHistorySnapshot | null>();
        cache.load.mockImplementation(key => key === 'a' ? loading.promise : Promise.resolve(null));
        await render('a');
        await act(async () => { history.setDraft('edited then cleared'); history.setDraft(''); });
        await render('b');
        await act(async () => { loading.resolve(snapshot([turn()], 'old draft')); });
        expect(cache.save).toHaveBeenCalledExactlyOnceWith('a', { turns: [turn()], draft: '' });
        expect(cache.load.mock.calls.map(call => call[0])).toEqual(['a', 'b']);
        expect(history.turns).toEqual([]);
    });
    it('detaches queued page-hide snapshots and flushes later edits on unmount', async () => {
        const writing = deferred<BrowserChatHistorySaveResult>(); cache.save.mockReturnValueOnce(writing.promise);
        await render('a');
        await act(async () => { history.setDraft('first'); }); await advance();
        const next = turn('second', 'captured answer');
        await act(async () => { history.setTurns([next]); window.dispatchEvent(new Event('pagehide')); });
        next.answer = 'mutated after flush';
        await act(async () => { writing.resolve(saved); });
        expect(cache.save.mock.calls[1][1].turns[0].answer).toBe('captured answer');
        await act(async () => { history.setDraft('last edit'); root.render(null); });
        expect(cache.save.mock.calls.at(-1)?.[1].draft).toBe('last edit');
    });
});

describe('History clear and storage failures', () => {
    it('serializes deletion after an in-flight save and cancels queued saves so clear wins', async () => {
        const writing = deferred<BrowserChatHistorySaveResult>(); cache.save.mockReturnValueOnce(writing.promise);
        await render('a');
        await act(async () => { history.setTurns([turn()]); }); await advance();
        await act(async () => { history.setDraft('queued'); window.dispatchEvent(new Event('pagehide')); });
        let clearing!: Promise<void>;
        await act(async () => { clearing = history.clearHistory(); });
        expect(history.turns).toEqual([]); expect(history.draft).toBe('');
        expect(cache.delete).not.toHaveBeenCalled();
        await act(async () => { writing.resolve(saved); await clearing; });
        expect(cache.save).toHaveBeenCalledTimes(1); expect(cache.delete).toHaveBeenCalledExactlyOnceWith('a');
        await advance(10000);
        expect(cache.save).toHaveBeenCalledTimes(1); expect(history.turns).toEqual([]);
    });
    it('fences a late hydrate after clearing and does not resurrect empty history', async () => {
        const loading = deferred<BrowserChatHistorySnapshot | null>(); cache.load.mockReturnValue(loading.promise);
        await render('a');
        await act(async () => { history.setDraft('temporary'); });
        let clearing!: Promise<void>;
        await act(async () => { clearing = history.clearHistory(); });
        expect(history.ready).toBe(true);
        await act(async () => { loading.resolve(snapshot()); await clearing; });
        await advance(5000);
        expect(history.turns).toEqual([]); expect(history.draft).toBe('');
        expect(cache.delete).toHaveBeenCalledExactlyOnceWith('a'); expect(cache.save).not.toHaveBeenCalled();
    });
    it('retains usable memory on a failed load without overwriting unread cached history', async () => {
        cache.load.mockRejectedValue(new Error('Storage unavailable'));
        await render('a');
        expect(history.ready).toBe(true); expect(history.historyNotice).toContain('memory');
        await act(async () => { history.setDraft('still usable'); history.setTurns([turn()]); });
        await advance();
        expect(history.draft).toBe('still usable'); expect(history.turns).toHaveLength(1);
        expect(cache.save).not.toHaveBeenCalled();
    });
    it('retains edits after a failed save and reports a failed delete without restoring visible history', async () => {
        cache.save.mockRejectedValue(new Error('Quota exceeded')); cache.delete.mockRejectedValue(new Error('Storage denied'));
        await render('a');
        await act(async () => { history.setDraft('keep me'); history.setTurns([turn()]); }); await advance();
        expect(history.draft).toBe('keep me'); expect(history.turns).toHaveLength(1);
        expect(history.historyNotice).toContain('Could not save');
        await act(async () => { await history.clearHistory(); });
        expect(history.turns).toEqual([]); expect(history.draft).toBe('');
        expect(history.historyNotice).toContain('Could not clear');
    });
});
