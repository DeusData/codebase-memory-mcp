/*
 * The frontend log buffer: what it batches, what it caps, what it drops and
 * says it dropped, and how it behaves while the server does not answer.
 * Timers and the clock are handed in, so nothing here waits.
 */

import { describe, expect, it } from 'vitest';

import { capField, repeatCount, UI_LOG_FIELD_MAX, UiLogBuffer } from './ui-log';
import type { UiLogPayload, UiLogTransport } from './ui-log';

interface Timer {
    fn: () => void;
    ms: number;
    cancelled: boolean;
}

function harness(answers: () => boolean = () => true) {
    const posts: { payload: UiLogPayload; final: boolean }[] = [];
    const timers: Timer[] = [];
    const transport: UiLogTransport = {
        send: async (payload, final) => {
            posts.push({ payload, final });
            return answers();
        },
    };
    let tick = 0;
    const buffer = new UiLogBuffer({
        page: '/?project=demo',
        session: 'abc',
        transport,
        now: () => new Date(Date.UTC(2026, 8, 8, 10, 0, tick++)),
        schedule: (fn, ms) => {
            const timer: Timer = { fn, ms, cancelled: false };
            timers.push(timer);
            return timer;
        },
        cancel: (handle) => {
            (handle as Timer).cancelled = true;
        },
        bufferMax: 5,
        batchMax: 3,
    });
    /** Fire the newest live timer, as the browser would when its time comes. */
    const fire = async (): Promise<void> => {
        const live = timers.filter((timer) => !timer.cancelled);
        const timer = live[live.length - 1];
        if (timer === undefined) {
            throw new Error('no timer armed');
        }
        timer.cancelled = true;
        timer.fn();
        await Promise.resolve();
        await Promise.resolve();
    };
    return { buffer, posts, timers, fire };
}

describe('UiLogBuffer', () => {
    it('preserves the recorded project across project changes and failed batch retries', async () => {
        let project = 'project-A';
        let accepted = false;
        const posts: UiLogPayload[] = [];
        const buffer = new UiLogBuffer({
            page: '/', session: 'projects', getProject: () => project,
            transport: { send: async (payload) => { posts.push(payload); return accepted; } },
            schedule: () => 0, cancel: () => undefined,
        });
        buffer.record('error', 'console', 'A failed');
        await buffer.flush();
        project = 'project-B';
        buffer.record('warn', 'console', 'B warning');
        buffer.record('error', 'rpc', 'late A request failed', { project: 'project-A' });
        buffer.record('error', 'api', 'daemon request failed', { project: '' });
        accepted = true;
        await buffer.flush();
        expect(posts[1]?.entries.map((entry) => [entry.message, entry.project])).toEqual([
            ['A failed', 'project-A'], ['B warning', 'project-B'],
            ['late A request failed', 'project-A'], ['daemon request failed', ''],
        ]);
    });

    it('posts each entry under the page it was recorded on, also when the address changed in the page (K24)', async () => {
        // A project switch stays in the page and only changes the address; the server files a post's entries under its one page.
        let page = '/?project=django-demo&workspace=galaxy';
        const posts: { payload: UiLogPayload; final: boolean }[] = [];
        const buffer = new UiLogBuffer({
            page, getPage: () => page, session: 'switch', getProject: () => new URLSearchParams(page.slice(1)).get('project') ?? '',
            transport: { send: async (payload, final) => { posts.push({ payload, final }); return true; } },
            schedule: () => 0, cancel: () => undefined,
        });
        buffer.record('warn', 'console', 'before the switch');
        page = '/?project=cbm&workspace=galaxy';
        buffer.record('warn', 'console', 'after the switch');
        buffer.record('info', 'console', 'still after');
        await buffer.flush();
        await buffer.flush();
        expect(posts.map(({ payload }) => [payload.page, payload.entries.map((entry) => [entry.message, entry.project])])).toEqual([
            ['/?project=django-demo&workspace=galaxy', [['before the switch', 'django-demo']]],
            ['/?project=cbm&workspace=galaxy', [['after the switch', 'cbm'], ['still after', 'cbm']]],
        ]);
        expect(buffer.page).toBe('/?project=cbm&workspace=galaxy');
        // A page that is going away sends the entries of every page it was on.
        buffer.record('warn', 'console', 'cbm again');
        page = '/?project=django-demo&workspace=explore';
        buffer.record('warn', 'console', 'django-demo again');
        await buffer.flush(true);
        expect(posts.slice(2).map(({ payload, final }) => [payload.page, payload.entries.map((entry) => entry.message), final])).toEqual([
            ['/?project=cbm&workspace=galaxy', ['cbm again'], true],
            ['/?project=django-demo&workspace=explore', ['django-demo again'], true],
        ]);
    });

    it('queues entries with a sequence and a timestamp, and posts them as one batch on the timer', async () => {
        const { buffer, posts, timers, fire } = harness();
        buffer.record('error', 'rpc', 'get_code_snippet returned no source', { detail: 'HTTP 200' });
        buffer.record('log', 'console', 'galaxy ready');
        expect(timers.length).toBe(1);
        expect(timers[0]?.ms).toBe(1500);
        expect(posts.length).toBe(0);

        await fire();
        expect(posts.length).toBe(1);
        expect(posts[0]?.final).toBe(false);
        expect(posts[0]?.payload.page).toBe('/?project=demo');
        expect(posts[0]?.payload.session).toBe('abc');
        expect(posts[0]?.payload.entries).toEqual([
            {
                ts: '2026-09-08T10:00:00.000Z',
                seq: 1,
                level: 'error',
                source: 'rpc',
                message: 'get_code_snippet returned no source',
                detail: 'HTTP 200',
            },
            { ts: '2026-09-08T10:00:01.000Z', seq: 2, level: 'log', source: 'console', message: 'galaxy ready' },
        ]);
        expect(buffer.stats()).toEqual({ queued: 0, sent: 2, dropped: 0, failedPosts: 0 });
    });

    it('sends more than one batch when the queue is longer than a batch', async () => {
        const { buffer, posts, fire } = harness();
        for (let i = 0; i < 5; i++) {
            buffer.record('info', 'console', `line ${i}`);
        }
        await fire();
        expect(posts.length).toBe(1);
        expect(posts[0]?.payload.entries.map((entry) => entry.message)).toEqual(['line 0', 'line 1', 'line 2']);
        await fire();
        expect(posts.length).toBe(2);
        expect(posts[1]?.payload.entries.map((entry) => entry.message)).toEqual(['line 3', 'line 4']);
    });

    it('caps every text field at the shared limit and marks the cut', () => {
        const long = 'x'.repeat(UI_LOG_FIELD_MAX + 10);
        expect(capField(long)).toBe(`${'x'.repeat(UI_LOG_FIELD_MAX)} [cut at ${UI_LOG_FIELD_MAX}]`);
        expect(capField('short')).toBe('short');
    });

    it('drops the oldest beyond the cap and says so at the head of the next batch', async () => {
        const { buffer, posts, fire } = harness();
        for (let i = 0; i < 7; i++) {
            buffer.record('info', 'console', `line ${i}`);
        }
        expect(buffer.stats().dropped).toBe(2);
        await fire();
        const first = posts[0]?.payload.entries[0];
        expect(first?.level).toBe('warn');
        expect(first?.source).toBe('ui-log');
        expect(first?.message).toContain('2 entries were dropped');
        expect(posts[0]?.payload.entries.slice(1).map((entry) => entry.message)).toEqual(['line 2', 'line 3', 'line 4']);
    });

    it('keeps a refused batch, backs off, and resends it in order when the server answers', async () => {
        let ok = false;
        const { buffer, posts, timers, fire } = harness(() => ok);
        buffer.record('error', 'window', 'boom');
        await fire();
        expect(posts.length).toBe(1);
        expect(buffer.stats()).toEqual({ queued: 1, sent: 0, dropped: 0, failedPosts: 1 });
        const retry = timers[timers.length - 1];
        expect(retry?.ms).toBe(2000);

        buffer.record('log', 'console', 'after');
        await fire();
        expect(posts.length).toBe(2);
        expect(timers[timers.length - 1]?.ms).toBe(5000);

        ok = true;
        await fire();
        expect(posts.length).toBe(3);
        expect(posts[2]?.payload.entries.map((entry) => entry.message)).toEqual(['boom', 'after']);
        expect(buffer.stats()).toEqual({ queued: 0, sent: 2, dropped: 0, failedPosts: 2 });
    });

    it('sends the rest with final on flush(true) and arms nothing after it', async () => {
        const { buffer, posts, timers } = harness();
        buffer.record('warn', 'api', '/api/tree answered with HTTP 500');
        await buffer.flush(true);
        expect(posts.length).toBe(1);
        expect(posts[0]?.final).toBe(true);
        expect(timers.every((timer) => timer.cancelled)).toBe(true);
    });

    it('records a repeated identical entry once and counts the repeats instead of queueing each one', async () => {
        const posts: UiLogPayload[] = [];
        const buffer = new UiLogBuffer({ page: '/', session: 'repeats', schedule: () => 0, cancel: () => undefined, bufferMax: 200, batchMax: 200,
            transport: { send: async (payload) => { posts.push(payload); return true; } } });
        const clock = 'THREE.THREE.Clock: This module has been deprecated. Please use THREE.Timer instead.';
        for (let i = 0; i < 25; i++) buffer.record('warn', 'console', clock, { project: 'cbm' });
        buffer.record('warn', 'console', 'a different warning', { project: 'cbm' });
        buffer.record('warn', 'console', clock, { project: 'django-demo' });
        await buffer.flush();
        const sent = posts.flatMap((post) => post.entries);
        // The first one in full, one count at ten, and the same text in another project as its own first entry.
        expect(sent.map((entry) => [entry.message === clock ? 'clock' : entry.message, entry.project, repeatCount(entry.detail)])).toEqual([
            ['clock', 'cbm', undefined], ['clock', 'cbm', 10], ['a different warning', 'cbm', undefined], ['clock', 'django-demo', undefined]]);
        // Leaving the page reports the exact total.
        await buffer.flush(true);
        expect(posts.at(-1)?.entries.map((entry) => [entry.project, repeatCount(entry.detail)])).toEqual([['cbm', 25]]);
        expect(buffer.stats().sent).toBe(5);
    });

    it('counts only truly identical entries: the same message from another place or with another detail is sent in full', async () => {
        const posts: UiLogPayload[] = [];
        const buffer = new UiLogBuffer({ page: '/', session: 'places', schedule: () => 0, cancel: () => undefined, bufferMax: 200, batchMax: 200,
            transport: { send: async (payload) => { posts.push(payload); return true; } } });
        const typeError = "Uncaught TypeError: Cannot read properties of undefined (reading 'x')";
        buffer.record('error', 'window', typeError, { project: 'cbm', url: 'a.js', line: 10, col: 3, stack: 'at A (a.js:10)' });
        buffer.record('error', 'window', typeError, { project: 'cbm', url: 'b.js', line: 99, col: 7, stack: 'at B (b.js:99)' });
        buffer.record('error', 'rpc', '/rpc query_graph: HTTP 500', { project: 'cbm', detail: 'body one' });
        buffer.record('error', 'rpc', '/rpc query_graph: HTTP 500', { project: 'cbm', detail: 'body two' });
        for (let i = 0; i < 11; i++) buffer.record('error', 'window', typeError, { project: 'cbm', url: 'b.js', line: 99, col: 7, stack: 'at B (b.js:99)' });
        await buffer.flush(true);
        const sent = posts.flatMap((post) => post.entries);
        // Both places and both response bodies reach the log in full; the twelve from b.js:99 become one entry and its count.
        expect(sent.map((entry) => [entry.source, entry.url ?? entry.detail?.split('\n').at(-1), entry.line, repeatCount(entry.detail)])).toEqual([
            ['window', 'a.js', 10, undefined], ['window', 'b.js', 99, undefined], ['rpc', 'body one', undefined, undefined],
            ['rpc', 'body two', undefined, undefined], ['window', 'b.js', 99, 10], ['window', 'b.js', 99, 12]]);
        // A count names the place it counts, so System › Logs can put it beside its first entry.
        expect(sent.at(-1)).toMatchObject({ url: 'b.js', line: 99, col: 7, stack: 'at B (b.js:99)' });
    });

    it('does not throw when the transport does', async () => {
        const throwing: UiLogTransport = {
            send: async () => {
                throw new Error('no network');
            },
        };
        const buffer = new UiLogBuffer({ page: '/', session: 's', transport: throwing, schedule: () => 0, cancel: () => undefined });
        buffer.record('info', 'console', 'x');
        await expect(buffer.flush()).resolves.toBeUndefined();
        expect(buffer.stats().failedPosts).toBe(1);
        expect(buffer.stats().queued).toBe(1);
    });
});
