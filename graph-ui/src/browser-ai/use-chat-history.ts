import { useCallback, useLayoutEffect, useMemo, useReducer, useRef, type Dispatch, type SetStateAction } from 'react';
import { browserChatHistoryCache, type BrowserChatHistory } from './chat-history-cache';
import type { BrowserChatTurn } from './chat-model';

export type ChatHistoryCache = Pick<typeof browserChatHistoryCache, 'load' | 'save' | 'delete'>;
export const CHAT_HISTORY_SAVE_MS = 1000;
interface View<T> { turns: T[]; draft: string; ready: boolean; historyNotice: string }
interface Capture<T> { turns: T[]; draft: string; epoch: number; version: number; hydrating: boolean; turnsEdited: boolean; draftEdited: boolean }
interface Session<T extends BrowserChatTurn> {
    key: string | undefined;
    cache: ChatHistoryCache;
    value: View<T>;
    baseline: { turns: T[]; draft: string };
    persisted: string;
    dirty: boolean;
    turnsEdited: boolean;
    draftEdited: boolean;
    epoch: number;
    version: number;
    live: boolean;
    started: boolean;
    loadFailed: boolean;
    timer?: ReturnType<typeof setTimeout>;
    queued?: { capture: Capture<T> };
    notify: () => void;
    enqueue: (operation: () => Promise<void>) => Promise<void>;
}

const fingerprint = (history: { turns: BrowserChatTurn[]; draft: string }) => JSON.stringify({ turns: history.turns, draft: history.draft });
const copy = <T,>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
function publish<T extends BrowserChatTurn>(session: Session<T>, patch: Partial<View<T>>) {
    session.value = { ...session.value, ...patch }; session.notify();
}
function schedule<T extends BrowserChatTurn>(session: Session<T>) {
    if (session.key === undefined || session.timer !== undefined || !session.live) return;
    // Do not reset this timer on each token: continuous output still reaches storage.
    session.timer = setTimeout(() => { session.timer = undefined; flush(session); }, CHAT_HISTORY_SAVE_MS);
}
function flush<T extends BrowserChatTurn>(session: Session<T>) {
    clearTimeout(session.timer); session.timer = undefined;
    if (session.key === undefined || (session.value.ready ? !session.dirty : !session.turnsEdited && !session.draftEdited)) return;
    const capture: Capture<T> = { ...copy({ turns: session.value.turns, draft: session.value.draft }), epoch: session.epoch, version: session.version,
        hydrating: !session.value.ready, turnsEdited: session.turnsEdited, draftEdited: session.draftEdited };
    if (session.queued) { session.queued.capture = capture; return; }
    const queued = { capture }; session.queued = queued;
    void session.enqueue(async () => {
        if (session.queued === queued) session.queued = undefined;
        const saved = queued.capture;
        if (saved.epoch !== session.epoch || session.loadFailed) return;
        const snapshot = { turns: saved.hydrating && !saved.turnsEdited ? copy(session.baseline.turns) : saved.turns,
            draft: saved.hydrating && !saved.draftEdited ? session.baseline.draft : saved.draft };
        const signature = fingerprint(snapshot);
        if (signature === session.persisted) {
            if (saved.version === session.version) session.dirty = false;
            return;
        }
        try {
            const result = await session.cache.save(session.key!, snapshot as BrowserChatHistory);
            if (saved.epoch !== session.epoch) return;
            session.persisted = signature;
            session.dirty = saved.version !== session.version;
            publish(session, { historyNotice: result.trimmed ? 'Saved recent history; older or oversized content was omitted.' : '' });
            if (session.dirty) schedule(session);
        } catch {
            if (saved.epoch === session.epoch) publish(session, { historyNotice: 'Could not save chat history; kept in memory.' });
        }
    });
}

/** One storage queue owns loads, writes and clears across this hook's project sessions. */
export function useChatHistory<T extends BrowserChatTurn = BrowserChatTurn>(key?: string, cache: ChatHistoryCache = browserChatHistoryCache) {
    const [, redraw] = useReducer((value: number) => value + 1, 0);
    const current = useRef<Session<T> | null>(null);
    const queue = useRef<Promise<void>>(Promise.resolve());
    const session = useMemo(() => {
        const value: View<T> = { turns: [], draft: '', ready: key === undefined, historyNotice: '' };
        const next: Session<T> = { key, cache, value, baseline: { turns: [], draft: '' }, persisted: fingerprint(value),
            dirty: false, turnsEdited: false, draftEdited: false, epoch: 0, version: 0, live: false, started: false, loadFailed: false,
            notify: () => { if (next.live && current.current === next) redraw(); },
            enqueue: operation => {
                const result = queue.current.then(operation, operation);
                queue.current = result.catch(() => undefined);
                return result;
            },
        };
        return next;
    }, [key, cache]);
    // A render for a new key immediately exposes its empty state and fences old setters.
    current.current = session;
    useLayoutEffect(() => {
        session.live = true;
        if (session.key !== undefined && !session.started) {
            session.started = true;
            const epoch = session.epoch;
            void session.enqueue(async () => {
                try {
                    const saved = await session.cache.load(session.key!);
                    if (epoch !== session.epoch) return;
                    session.baseline = { turns: copy((saved?.turns ?? []) as T[]), draft: saved?.draft ?? '' };
                    session.persisted = fingerprint(session.baseline);
                    publish(session, { turns: session.turnsEdited ? session.value.turns : session.baseline.turns,
                        draft: session.draftEdited ? session.value.draft : session.baseline.draft, ready: true,
                        historyNotice: saved?.trimmed ? 'Restored recent history; older or oversized content was omitted.' : '' });
                    session.dirty = fingerprint(session.value) !== session.persisted;
                    if (session.dirty && !session.queued) schedule(session);
                } catch {
                    if (epoch !== session.epoch) return;
                    // Do not overwrite a cache whose existing contents could not be read.
                    session.loadFailed = true;
                    publish(session, { ready: true, historyNotice: 'Chat history is unavailable; this conversation stays in memory.' });
                }
            });
        }
        const pageHide = () => flush(session);
        window.addEventListener('pagehide', pageHide);
        return () => { session.live = false; window.removeEventListener('pagehide', pageHide); flush(session); };
    }, [session]);

    const setTurns: Dispatch<SetStateAction<T[]>> = useCallback(update => {
        if (!session.live || current.current !== session) return;
        const turns = typeof update === 'function' ? update(session.value.turns) : update;
        if (turns === session.value.turns) return;
        session.turnsEdited = true; session.version++;
        publish(session, { turns });
        session.dirty = true;
        schedule(session);
    }, [session]);
    const setDraft: Dispatch<SetStateAction<string>> = useCallback(update => {
        if (!session.live || current.current !== session) return;
        const draft = typeof update === 'function' ? update(session.value.draft) : update;
        if (draft === session.value.draft) return;
        session.draftEdited = true; session.version++;
        publish(session, { draft });
        session.dirty = true;
        schedule(session);
    }, [session]);
    const clearHistory = useCallback(async () => {
        if (!session.live || current.current !== session) return;
        const epoch = ++session.epoch; session.version++;
        clearTimeout(session.timer); session.timer = undefined; session.queued = undefined;
        session.baseline = { turns: [], draft: '' }; session.persisted = fingerprint(session.baseline);
        session.dirty = false; session.turnsEdited = true; session.draftEdited = true; session.loadFailed = false;
        publish(session, { turns: [], draft: '', ready: true, historyNotice: '' });
        if (session.key === undefined) return;
        await session.enqueue(async () => {
            try { await session.cache.delete(session.key!); }
            catch { if (epoch === session.epoch) publish(session, { historyNotice: 'Could not clear saved chat history; the visible conversation is empty.' }); }
        });
    }, [session]);
    return { ...session.value, setTurns, setDraft, clearHistory };
}
