import type { BrowserChatTurn } from './chat-model';
import type { PreparedExplanationContext } from './explanation-context';

export const BROWSER_CHAT_HISTORY_TTL_MS = 24 * 60 * 60 * 1000;
export const BROWSER_CHAT_HISTORY_MAX_TURNS = 50;
export const BROWSER_CHAT_HISTORY_MAX_PROJECT_BYTES = 2 * 1024 * 1024;
export const BROWSER_CHAT_HISTORY_MAX_TOTAL_BYTES = 10 * 1024 * 1024;

export interface BrowserChatHistory {
    turns: BrowserChatTurn[];
    draft?: string;
    /** Optional project evidence. Only plain, JSON-serializable snapshots are accepted. */
    evidence?: unknown;
}

export interface BrowserChatHistorySnapshot extends BrowserChatHistory {
    savedAt: number;
    expiresAt: number;
    trimmed: boolean;
}

export interface BrowserChatHistorySaveResult {
    saved: boolean;
    trimmed: boolean;
    droppedTurns: number;
    evictedProjects: number;
}

/** The synchronous callback and its replacements must commit atomically or reject. */
export interface BrowserChatHistoryStorage {
    transact<T>(operation: (entries: Map<string, unknown>) => T): Promise<T>;
}

interface HistoryRecord {
    version: 1;
    key: string;
    savedAt: number;
    expiresAt: number;
    trimmed: boolean;
    history: BrowserChatHistory;
}

/** A project name alone is insufficient when the browser connects to different servers. */
export function browserChatHistoryProjectKey(serverOrigin: string, project: string): string {
    return JSON.stringify([new URL(serverOrigin).origin, project]);
}

function isObject(value: unknown): value is Record<string, unknown> {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isPlainJson(value: unknown, ancestors = new Set<object>(), depth = 0): boolean {
    if (value === null || typeof value === 'string' || typeof value === 'boolean') return true;
    if (typeof value === 'number') return Number.isFinite(value);
    if (typeof value !== 'object' || depth > 64 || ancestors.has(value)) return false;
    if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) return false;
    ancestors.add(value);
    const valid = Array.isArray(value)
        ? Array.from(value).every(item => isPlainJson(item, ancestors, depth + 1))
        : Object.values(value).every(item => item === undefined || isPlainJson(item, ancestors, depth + 1));
    ancestors.delete(value);
    return valid;
}

function isAttachment(value: unknown): boolean {
    return isObject(value) && ['id', 'text', 'path', 'project', 'sourceVersion'].every(key => typeof value[key] === 'string')
        && ['startLine', 'startColumn', 'endLine', 'endColumn'].every(key => Number.isSafeInteger(value[key]) && Number(value[key]) >= 1);
}

function isReaderContext(value: unknown): boolean {
    if (!isObject(value) || typeof value.project !== 'string' || (value.path !== undefined && typeof value.path !== 'string')
        || !['ready', 'loading', 'unavailable', 'empty'].includes(String(value.status))) return false;
    return value.source === undefined || (isObject(value.source) && isAttachment(value.source)
        && ['file', 'selection'].includes(String(value.source.kind))
        && (value.source.partial === undefined || typeof value.source.partial === 'string'));
}

function isPreparedEvidence(value: unknown): value is PreparedExplanationContext {
    return isObject(value) && typeof value.label === 'string' && typeof value.fallback === 'string'
        && typeof value.characterCount === 'number' && Number.isSafeInteger(value.characterCount) && value.characterCount >= 0
        && Array.isArray(value.limitations) && value.limitations.every(item => typeof item === 'string')
        && Array.isArray(value.evidence) && value.evidence.every(item => isObject(item)
            && typeof item.id === 'string' && typeof item.text === 'string' && ['code', 'graph'].includes(String(item.source))
            && (item.location === undefined || (isObject(item.location)
                && typeof item.location.path === 'string' && typeof item.location.sourceVersion === 'string'
                && ['startLine', 'startColumn', 'endLine', 'endColumn'].every(key =>
                    Number.isSafeInteger((item.location as Record<string, unknown>)[key]) && Number((item.location as Record<string, unknown>)[key]) >= 1))));
}

function isTurn(value: unknown): boolean {
    return isObject(value) && ['id', 'prompt', 'modelId', 'answer'].every(key => typeof value[key] === 'string')
        && ['counting', 'generating', 'complete', 'stopped', 'error'].includes(String(value.status))
        && (value.error === undefined || typeof value.error === 'string')
        && (value.evidence === undefined || isPreparedEvidence(value.evidence))
        && (value.attachment === undefined || isAttachment(value.attachment))
        && (value.readerContext === undefined || isReaderContext(value.readerContext))
        && (value.context === undefined || (Array.isArray(value.context) && value.context.every(item =>
            isObject(item) && ['id', 'label', 'text'].every(key => typeof item[key] === 'string'))))
        && Array.isArray(value.request) && value.request.every(message => isObject(message)
            && ['system', 'user', 'assistant'].includes(String(message.role)) && typeof message.content === 'string');
}

function isHistory(value: unknown): value is BrowserChatHistory {
    return isObject(value) && isPlainJson(value) && Array.isArray(value.turns) && value.turns.every(isTurn)
        && new Set(value.turns.map(turn => turn.id)).size === value.turns.length
        && (value.draft === undefined || typeof value.draft === 'string');
}

function readRecord(value: unknown, key: string, now: number): HistoryRecord | undefined {
    if (!isObject(value) || !isPlainJson(value) || value.version !== 1 || value.key !== key || typeof value.trimmed !== 'boolean'
        || typeof value.savedAt !== 'number' || !Number.isSafeInteger(value.savedAt) || value.savedAt < 0
        || typeof value.expiresAt !== 'number' || !Number.isSafeInteger(value.expiresAt) || value.expiresAt !== value.savedAt + BROWSER_CHAT_HISTORY_TTL_MS
        || value.expiresAt <= now || !isHistory(value.history)) return;
    return value as unknown as HistoryRecord;
}

function copy<T>(value: T): T { return JSON.parse(JSON.stringify(value)) as T; }
function bytes(value: unknown): number { return new TextEncoder().encode(JSON.stringify(value)).byteLength; }

/** Opens only on an explicit cache operation; no model, network, timer, or background work. */
export function createIndexedDbBrowserChatHistoryStorage(): BrowserChatHistoryStorage {
    const open = (): Promise<IDBDatabase> => new Promise((resolve, reject) => {
        if (!globalThis.indexedDB) { reject(new Error('Browser chat history storage is unavailable.')); return; }
        const request = indexedDB.open('cbm-browser-chat-history', 1);
        let rejected = false;
        request.onupgradeneeded = () => { request.result.createObjectStore('projects'); };
        request.onerror = () => reject(request.error ?? new Error('Could not open browser chat history.'));
        request.onblocked = () => { rejected = true; reject(new Error('Browser chat history storage is blocked.')); };
        request.onsuccess = () => {
            if (rejected) { request.result.close(); return; }
            request.result.onversionchange = () => request.result.close();
            resolve(request.result);
        };
    });
    return {
        async transact<T>(operation: (entries: Map<string, unknown>) => T): Promise<T> {
            const database = await open();
            return new Promise<T>((resolve, reject) => {
                let transaction: IDBTransaction;
                try { transaction = database.transaction('projects', 'readwrite'); }
                catch (error) { database.close(); reject(error); return; }
                let result: T;
                transaction.oncomplete = () => { database.close(); resolve(result); };
                transaction.onabort = transaction.onerror = () => {
                    database.close(); reject(transaction.error ?? new Error('Browser chat history write failed.'));
                };
                const store = transaction.objectStore('projects');
                const entries = new Map<string, unknown>();
                const cursor = store.openCursor();
                cursor.onsuccess = () => {
                    const row = cursor.result;
                    if (row) {
                        if (typeof row.key === 'string') entries.set(row.key, row.value);
                        else row.delete();
                        row.continue(); return;
                    }
                    const original = new Map(entries);
                    try {
                        result = operation(entries);
                        for (const key of original.keys()) if (!entries.has(key)) store.delete(key);
                        for (const [key, value] of entries) if (original.get(key) !== value) store.put(value, key);
                    } catch (error) {
                        transaction.abort(); database.close(); reject(error);
                    }
                };
            });
        },
    };
}

export interface BrowserChatHistoryCacheOptions {
    storage?: BrowserChatHistoryStorage;
    now?: () => number;
    maxTurns?: number;
    maxProjectBytes?: number;
    maxTotalBytes?: number;
}

export function createBrowserChatHistoryCache(options: BrowserChatHistoryCacheOptions = {}) {
    const storage = options.storage ?? createIndexedDbBrowserChatHistoryStorage();
    const now = options.now ?? Date.now;
    const maxTurns = options.maxTurns ?? BROWSER_CHAT_HISTORY_MAX_TURNS;
    const maxProjectBytes = options.maxProjectBytes ?? BROWSER_CHAT_HISTORY_MAX_PROJECT_BYTES;
    const maxTotalBytes = options.maxTotalBytes ?? BROWSER_CHAT_HISTORY_MAX_TOTAL_BYTES;
    for (const bound of [maxTurns, maxProjectBytes, maxTotalBytes]) {
        if (!Number.isSafeInteger(bound) || bound < 1) throw new Error('Chat history bounds must be positive integers.');
    }
    const projectLimit = Math.min(maxProjectBytes, maxTotalBytes);
    const clean = (entries: Map<string, unknown>, time: number): Map<string, HistoryRecord> => {
        const valid = new Map<string, HistoryRecord>();
        for (const [key, value] of entries) {
            const record = readRecord(value, key, time);
            if (!record || record.history.turns.length > maxTurns || bytes(record) > projectLimit) entries.delete(key);
            else valid.set(key, record);
        }
        let total = [...valid.values()].reduce((sum, record) => sum + bytes(record), 0);
        for (const record of [...valid.values()].sort((left, right) => left.savedAt - right.savedAt || left.key.localeCompare(right.key))) {
            if (total <= maxTotalBytes) break;
            entries.delete(record.key); valid.delete(record.key); total -= bytes(record);
        }
        return valid;
    };
    return {
        /** Reads never renew expiry. Expired/corrupt entries are purged in this same transaction. */
        async load(key: string): Promise<BrowserChatHistorySnapshot | null> {
            return storage.transact(entries => {
                const record = clean(entries, now()).get(key);
                if (!record) return null;
                const history = copy(record.history);
                history.turns = history.turns.map(turn => ['counting', 'generating'].includes(turn.status)
                    ? { ...turn, status: 'stopped' } : turn);
                return { ...history, savedAt: record.savedAt, expiresAt: record.expiresAt, trimmed: record.trimmed };
            });
        },
        /** Errors reject so callers can retain their in-memory chat and explain the storage failure. */
        async save(key: string, history: BrowserChatHistory): Promise<BrowserChatHistorySaveResult> {
            if (!isHistory(history)) throw new Error('Chat history contains invalid snapshot data.');
            // Take the snapshot before waiting for IndexedDB; later UI mutations cannot alter it.
            const snapshot = copy({ turns: history.turns,
                ...(history.draft === undefined ? {} : { draft: history.draft }),
                ...(history.evidence === undefined ? {} : { evidence: history.evidence }) });
            return storage.transact(entries => {
                const time = now();
                const valid = clean(entries, time);
                const record: HistoryRecord = { version: 1, key, savedAt: time,
                    expiresAt: time + BROWSER_CHAT_HISTORY_TTL_MS, trimmed: false, history: snapshot };
                const result: BrowserChatHistorySaveResult = { saved: true, trimmed: false, droppedTurns: 0, evictedProjects: 0 };
                const discardOldest = () => { record.history.turns.shift(); result.droppedTurns++; record.trimmed = true; };
                while (record.history.turns.length > maxTurns) discardOldest();
                if (bytes(record) > projectLimit && record.history.evidence !== undefined) {
                    delete record.history.evidence; record.trimmed = true;
                }
                if (bytes(record) > projectLimit && record.history.draft !== undefined) {
                    delete record.history.draft; record.trimmed = true;
                }
                while (bytes(record) > projectLimit && record.history.turns.length) discardOldest();
                result.trimmed = record.trimmed;
                if (!record.history.turns.length && !record.history.draft && record.history.evidence === undefined) {
                    entries.delete(key); return { ...result, saved: false };
                }
                if (bytes(record) > projectLimit) {
                    entries.delete(key); return { ...result, saved: false, trimmed: true };
                }
                const previous = valid.get(key);
                // A redundant flush is not a meaningful write and must not keep old history alive.
                if (previous && JSON.stringify(previous.history) === JSON.stringify(record.history)) {
                    record.savedAt = previous.savedAt; record.expiresAt = previous.expiresAt;
                    record.trimmed ||= previous.trimmed;
                    result.trimmed = record.trimmed;
                }
                entries.set(key, record); valid.set(key, record);
                let total = [...valid.values()].reduce((sum, item) => sum + bytes(item), 0);
                const oldest = [...valid.values()].filter(item => item.key !== key)
                    .sort((left, right) => left.savedAt - right.savedAt || left.key.localeCompare(right.key));
                for (const item of oldest) {
                    if (total <= maxTotalBytes) break;
                    entries.delete(item.key); total -= bytes(item); result.evictedProjects++;
                }
                return result;
            });
        },
        async delete(key: string): Promise<void> {
            await storage.transact(entries => { clean(entries, now()); entries.delete(key); });
        },
    };
}

export const browserChatHistoryCache = createBrowserChatHistoryCache();
