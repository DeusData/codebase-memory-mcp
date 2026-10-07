import { describe, expect, it } from 'vitest';
import {
    BROWSER_CHAT_HISTORY_TTL_MS,
    browserChatHistoryProjectKey,
    createBrowserChatHistoryCache,
    createIndexedDbBrowserChatHistoryStorage,
    type BrowserChatHistoryStorage,
} from './chat-history-cache';
import type { BrowserChatTurn } from './chat-model';
import type { PreparedExplanationContext } from './explanation-context';

/** Atomic storage seam: tests exercise cache policy without pretending to implement IndexedDB. */
class MemoryStorage implements BrowserChatHistoryStorage {
    entries = new Map<string, unknown>();
    failure?: Error;
    async transact<T>(operation: (entries: Map<string, unknown>) => T): Promise<T> {
        if (this.failure) throw this.failure;
        const next = structuredClone(this.entries);
        const result = operation(next);
        this.entries = next;
        return result;
    }
}

const key = browserChatHistoryProjectKey('http://localhost:8080', 'project-a');
const turn = (id = 'first'): BrowserChatTurn => ({
    id, prompt: `Explain ${id}`, modelId: 'local-model', status: 'complete', answer: 'The exact answer.\r\n\t',
    request: [{ role: 'system', content: 'Source data\r\n\t' }, { role: 'user', content: `Explain ${id}` }],
    attachment: { id: 'attachment', text: '  code(\r\n\t', path: 'src/file.ts', project: 'project-a',
        startLine: 3, startColumn: 2, endLine: 4, endColumn: 8, sourceVersion: 'version-1' },
    context: [{ id: 'graph', label: 'Caller evidence', text: 'entry calls helper' }],
});
const preparedEvidence: PreparedExplanationContext = {
    label: 'src/file.ts', fallback: 'Source available.', characterCount: 42,
    limitations: ['An exact selection excerpt.'],
    evidence: [{ id: 'source-1', source: 'code', text: '\tverbatim\r\n', location: { path: 'src/file.ts',
        startLine: 3, startColumn: 2, endLine: 4, endColumn: 8, sourceVersion: 'version-1' } },
    { id: 'graph-1', source: 'graph', text: 'entry calls helper' }],
};

function setup(limits: { maxTurns?: number; maxProjectBytes?: number; maxTotalBytes?: number } = {}) {
    const storage = new MemoryStorage();
    let time = 1_000;
    const cache = createBrowserChatHistoryCache({ storage, now: () => time, ...limits });
    return { cache, storage, setTime: (next: number) => { time = next; } };
}

describe('browser-local chat history', () => {
    it('round-trips detached exact turns, sources, requests, drafts and plain evidence', async () => {
        const { cache } = setup();
        const original = turn();
        original.readerContext = { project: 'project-a', status: 'ready', path: original.attachment!.path,
            source: { ...original.attachment!, kind: 'selection', partial: 'Selected range only' } };
        const expected = structuredClone(original);
        const evidence = { summary: 'snapshot', files: [{ path: 'file.ts', text: '\tfull text\r\n' }] };
        const saving = cache.save(key, { turns: [original], draft: '  unfinished question\n', evidence });
        original.request[0].content = 'mutated'; original.attachment!.text = 'mutated'; evidence.summary = 'mutated';
        expect(await saving).toEqual({ saved: true, trimmed: false, droppedTurns: 0, evictedProjects: 0 });
        const loaded = (await cache.load(key))!;
        expect(loaded.turns).toEqual([expected]);
        expect(loaded.draft).toBe('  unfinished question\n');
        expect(loaded.evidence).toEqual({ summary: 'snapshot', files: [{ path: 'file.ts', text: '\tfull text\r\n' }] });
        loaded.turns[0].attachment!.text = 'mutated after load';
        expect((await cache.load(key))!.turns).toEqual([expected]);
    });

    it('expires at exactly 24 hours, never extends on reads, and purges on any next access', async () => {
        const { cache, storage, setTime } = setup();
        await cache.save(key, { turns: [turn()] });
        setTime(1_000 + BROWSER_CHAT_HISTORY_TTL_MS - 1);
        expect((await cache.load(key))?.savedAt).toBe(1_000);
        expect((await cache.load(key))?.expiresAt).toBe(1_000 + BROWSER_CHAT_HISTORY_TTL_MS);
        setTime(1_000 + BROWSER_CHAT_HISTORY_TTL_MS);
        await cache.load('another-project');
        expect(storage.entries.has(key)).toBe(false);
        expect(await cache.load(key)).toBeNull();
    });

    it('renews only on changed content, including a changed draft', async () => {
        const { cache, setTime } = setup();
        await cache.save(key, { turns: [turn()] });
        setTime(2_000);
        await cache.save(key, { turns: [turn()] });
        expect((await cache.load(key))?.savedAt).toBe(1_000);
        await cache.save(key, (await cache.load(key))!);
        expect((await cache.load(key))?.savedAt).toBe(1_000);
        await cache.save(key, { turns: [turn()], draft: 'next question' });
        expect((await cache.load(key))?.expiresAt).toBe(2_000 + BROWSER_CHAT_HISTORY_TTL_MS);
    });

    it('namespaces by normalized server origin and project and deletes only the requested history', async () => {
        const { cache } = setup();
        const otherProject = browserChatHistoryProjectKey('http://localhost:8080', 'project-b');
        const otherServer = browserChatHistoryProjectKey('http://localhost:8081', 'project-a');
        expect(browserChatHistoryProjectKey('http://localhost:8080/some/path', 'project-a')).toBe(key);
        for (const item of [key, otherProject, otherServer]) await cache.save(item, { turns: [turn(item)] });
        await cache.delete(key);
        expect(await cache.load(key)).toBeNull();
        expect((await cache.load(otherProject))?.turns[0].id).toBe(otherProject);
        expect((await cache.load(otherServer))?.turns[0].id).toBe(otherServer);
    });

    it.each(['counting', 'generating'])('restores interrupted %s work as stopped without altering snapshots', async status => {
        const { cache, storage, setTime } = setup();
        const interrupted = { ...turn(), status } as BrowserChatTurn;
        await cache.save(key, { turns: [interrupted] });
        const stored = structuredClone(storage.entries.get(key));
        setTime(2_000);
        expect(await cache.load(key)).toMatchObject({ turns: [{ ...interrupted, status: 'stopped' }],
            savedAt: 1_000, expiresAt: 1_000 + BROWSER_CHAT_HISTORY_TTL_MS });
        expect(storage.entries.get(key)).toEqual(stored);
        expect(interrupted.status).toBe(status);
    });

    it('does not recreate an empty history after explicit deletion or an empty flush', async () => {
        const { cache, storage } = setup();
        await cache.save(key, { turns: [turn()] });
        await cache.delete(key);
        expect(await cache.save(key, { turns: [], draft: '' })).toMatchObject({ saved: false, trimmed: false });
        expect(storage.entries.has(key)).toBe(false);
        await cache.save(key, { turns: [turn()] });
        await cache.save(key, { turns: [] });
        expect(await cache.load(key)).toBeNull();
    });

    it('preserves complete, stopped and error terminal states', async () => {
        const { cache } = setup();
        const turns = (['complete', 'stopped', 'error'] as const).map(status => ({ ...turn(status), status }));
        await cache.save(key, { turns });
        expect((await cache.load(key))?.turns).toEqual(turns);
    });

    it('retains well-formed per-turn explanation evidence verbatim', async () => {
        const { cache } = setup();
        const turns = [{ ...turn(), evidence: preparedEvidence }];
        await cache.save(key, { turns });
        expect((await cache.load(key))?.turns).toEqual(turns);
    });

    it('keeps the graph evidence a listed answer was listed from, and rejects a damaged one (K14)', async () => {
        const { cache } = setup();
        const listedFrom = { id: 'galaxy', label: 'JSONBAgg', text: '{"kind":"galaxy-scope-evidence"}' };
        const turns = [{ ...turn(), answeredFrom: 'graph' as const, listedFrom }];
        await cache.save(key, { turns });
        expect((await cache.load(key))?.turns).toEqual(turns);
        await expect(cache.save(key, { turns: [{ ...turn(), listedFrom: { id: 'galaxy', label: 'JSONBAgg', text: 42 } } as unknown as BrowserChatTurn] })).rejects.toThrow('invalid snapshot');
    });

    it('rejects failed reads, saves and deletes so callers can retain in-memory state', async () => {
        const { cache, storage } = setup();
        await cache.save(key, { turns: [turn()] });
        storage.failure = new Error('Quota or privacy restriction');
        await expect(cache.load(key)).rejects.toThrow('Quota or privacy restriction');
        await expect(cache.save(key, { turns: [] })).rejects.toThrow('Quota or privacy restriction');
        await expect(cache.delete(key)).rejects.toThrow('Quota or privacy restriction');
        storage.failure = undefined;
        expect((await cache.load(key))?.turns).toEqual([turn()]);
    });

    it('rejects explicitly when IndexedDB is unavailable', async () => {
        const descriptor = Object.getOwnPropertyDescriptor(globalThis, 'indexedDB');
        Object.defineProperty(globalThis, 'indexedDB', { configurable: true, value: undefined });
        try {
            await expect(createIndexedDbBrowserChatHistoryStorage().transact(() => null)).rejects.toThrow('unavailable');
        } finally {
            if (descriptor) Object.defineProperty(globalThis, 'indexedDB', descriptor);
            else Reflect.deleteProperty(globalThis, 'indexedDB');
        }
    });

    it.each([
        { version: 99 }, { expiresAt: Infinity }, { key: 'wrong-project' },
        { history: { turns: [{ id: 'missing required fields' }] } },
        { history: { turns: [{ ...turn(), request: [{ role: 'tool', content: 'unexpected' }] }] } },
        { history: { turns: [{ ...turn(), attachment: { ...turn().attachment, startLine: -1 } }] } },
        { history: { turns: [turn('duplicate'), turn('duplicate')] } },
        { history: { turns: [{ ...turn(), evidence: { label: 'Missing required evidence arrays' } }] } },
        { history: { turns: [{ ...turn(), evidence: { ...preparedEvidence, limitations: 'not an array' } }] } },
        { history: { turns: [{ ...turn(), evidence: { ...preparedEvidence, evidence: [null] } }] } },
        { history: { turns: [{ ...turn(), evidence: { ...preparedEvidence, evidence: [{ id: 'bad', text: 12, source: 'graph' }] } }] } },
        { history: { turns: [{ ...turn(), evidence: { ...preparedEvidence, evidence: [{ id: 'bad', text: 'text', source: 'code', location: { path: 'missing-range.ts' } }] } }] } },
    ])('purges a corrupt record without returning it: %j', async corruption => {
        const { cache, storage } = setup();
        await cache.save(key, { turns: [turn()] });
        storage.entries.set(key, { ...(storage.entries.get(key) as object), ...corruption });
        expect(await cache.load(key)).toBeNull();
        expect(storage.entries.has(key)).toBe(false);
    });

    it('rejects cyclic/non-JSON evidence and invalid turns before storage changes', async () => {
        const { cache, storage } = setup();
        const cycle: { self?: unknown } = {}; cycle.self = cycle;
        for (const evidence of [cycle, new Date(), { value: Infinity }, { fn: () => undefined }]) {
            await expect(cache.save(key, { turns: [turn()], evidence })).rejects.toThrow('invalid snapshot');
        }
        await expect(cache.save(key, { turns: [{ ...turn(), status: 'unknown' } as unknown as BrowserChatTurn] })).rejects.toThrow('invalid snapshot');
        expect(storage.entries.size).toBe(0);
    });

    it('keeps the newest whole turns and records trimming without changing their text', async () => {
        const { cache } = setup({ maxTurns: 2 });
        const turns = [turn('oldest'), turn('middle'), turn('newest')];
        expect(await cache.save(key, { turns })).toMatchObject({ saved: true, trimmed: true, droppedTurns: 1 });
        expect(await cache.load(key)).toMatchObject({ turns: turns.slice(1), trimmed: true });
        expect(turns).toHaveLength(3);
    });

    it('applies byte bounds to multibyte snapshots by discarding whole older turns', async () => {
        const { cache, storage } = setup({ maxProjectBytes: 2_000 });
        const older = { ...turn('oldest'), answer: '🙂'.repeat(400) };
        const newest = turn('newest');
        expect(await cache.save(key, { turns: [older, newest] })).toMatchObject({ trimmed: true, droppedTurns: 1 });
        expect((await cache.load(key))?.turns).toEqual([newest]);
        expect(new TextEncoder().encode(JSON.stringify(storage.entries.get(key))).byteLength).toBeLessThanOrEqual(2_000);
    });

    it('discards oversized optional draft/evidence before sacrificing exact chat turns', async () => {
        const { cache } = setup({ maxProjectBytes: 2_000 });
        expect(await cache.save(key, { turns: [turn()], draft: 'd'.repeat(3_000), evidence: { text: 'e'.repeat(3_000) } }))
            .toMatchObject({ saved: true, trimmed: true, droppedTurns: 0 });
        const loaded = (await cache.load(key))!;
        expect(loaded.turns).toEqual([turn()]);
        expect(loaded.draft).toBeUndefined(); expect(loaded.evidence).toBeUndefined();
    });

    it('evicts the oldest project on the global bound; reads do not make it newer', async () => {
        const { cache, storage, setTime } = setup({ maxProjectBytes: 2_000, maxTotalBytes: 1_600 });
        await cache.save('oldest', { turns: [turn('a')] });
        setTime(2_000); await cache.save('middle', { turns: [turn('b')] });
        expect(storage.entries.size).toBe(2);
        await cache.load('oldest');
        setTime(3_000);
        expect(await cache.save('newest', { turns: [turn('c')] })).toMatchObject({ evictedProjects: 1 });
        expect(await cache.load('oldest')).toBeNull();
        expect((await cache.load('middle'))?.turns[0].id).toBe('b');
        expect((await cache.load('newest'))?.turns[0].id).toBe('c');
        const totalBytes = [...storage.entries.values()].reduce<number>((sum, item) => sum + new TextEncoder().encode(JSON.stringify(item)).byteLength, 0);
        expect(totalBytes).toBeLessThanOrEqual(1_600);
    });

    it('does not claim to save if even the empty record exceeds the configured bound', async () => {
        const { cache, storage } = setup({ maxProjectBytes: 1 });
        expect(await cache.save(key, { turns: [turn()] })).toMatchObject({ saved: false, trimmed: true, droppedTurns: 1 });
        expect(storage.entries.size).toBe(0);
    });
});
