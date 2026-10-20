// @vitest-environment jsdom
/*
 * The frontend log wired into a page: the console still prints, and what it
 * printed is recorded; uncaught errors, rejected promises and the failures
 * the clients announce all reach the buffer; uninstall puts the console
 * back. The transport is a recorder, so nothing here touches a network.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';

import { reportError } from '../provider/error-observer';
import { repeatCount, UiLogBuffer } from './ui-log';
import type { UiLogPayload } from './ui-log';
import { describeArgs, installUiLog, safeStringify } from './ui-log-install';
import type { UiLogHandle } from './ui-log-install';
import { httpUiLogTransport } from './ui-log-transport';

let handle: UiLogHandle | undefined;

afterEach(() => {
    handle?.uninstall();
    handle = undefined;
});

function install(getProject?: () => string) {
    const posts: { payload: UiLogPayload; final: boolean }[] = [];
    const buffer = new UiLogBuffer({
        page: '/?project=demo',
        session: 'test',
        transport: {
            send: async (payload, final) => {
                posts.push({ payload, final });
                return true;
            },
        },
        schedule: () => 0,
        cancel: () => undefined,
    });
    const printed: unknown[][] = [];
    const fakeConsole = {
        debug: (...args: unknown[]) => printed.push(['debug', ...args]),
        log: (...args: unknown[]) => printed.push(['log', ...args]),
        info: (...args: unknown[]) => printed.push(['info', ...args]),
        warn: (...args: unknown[]) => printed.push(['warn', ...args]),
        error: (...args: unknown[]) => printed.push(['error', ...args]),
    } as unknown as Console;
    handle = installUiLog({ buffer, console: fakeConsole, session: 'test', getProject });
    return { buffer, posts, printed, fakeConsole };
}

async function entries(buffer: UiLogBuffer, posts: { payload: UiLogPayload }[]) {
    await buffer.flush();
    return posts.flatMap((post) => post.payload.entries);
}

describe('installUiLog', () => {
    it('captures console ownership before flushing and retains the failed request project', async () => {
        let project = 'A';
        const { buffer, posts, fakeConsole } = install(() => project);
        fakeConsole.warn('A warning');
        project = 'B';
        fakeConsole.error('B error');
        reportError({ project: 'A', source: 'rpc', level: 'error', message: 'late A request' });
        reportError({ project: '', source: 'api', level: 'error', message: 'daemon request' });
        const all = await entries(buffer, posts);
        expect(all.map((entry) => [entry.message, entry.project])).toEqual([
            ['session test started on /', 'A'], ['A warning', 'A'], ['B error', 'B'],
            ['late A request', 'A'], ['daemon request', ''],
        ]);
    });

    it('starts the session with a line that names page, build and browser', async () => {
        const { buffer, posts } = install();
        const all = await entries(buffer, posts);
        expect(all[0]?.source).toBe('ui-log');
        expect(all[0]?.message).toBe('session test started on /');
        expect(all[0]?.detail).toContain('build v');
    });

    it('posts a line under the address it was written on, also after a project switch in the page (K24)', async () => {
        window.history.replaceState(null, '', '/?project=django-demo&workspace=galaxy');
        const posts: UiLogPayload[] = [];
        const quiet = { debug: () => {}, log: () => {}, info: () => {}, warn: () => {}, error: () => {} } as unknown as Console;
        try {
            handle = installUiLog({ console: quiet, session: 'switch', transport: { send: async (payload) => { posts.push(payload); return true; } } });
            // The project switch changes the address with pushState and keeps the page (app/project-windows.tsx).
            window.history.pushState(null, '', '/?project=cbm&workspace=galaxy');
            quiet.warn('after the switch');
            await handle.buffer.flush();
            await handle.buffer.flush();
            expect(posts.map((post) => [post.page, post.entries.map((entry) => [entry.message, entry.project])])).toEqual([
                ['/?project=django-demo&workspace=galaxy', [['session switch started on /?project=django-demo&workspace=galaxy', 'django-demo']]],
                ['/?project=cbm&workspace=galaxy', [['after the switch', 'cbm']]],
            ]);
        } finally {
            window.history.replaceState(null, '', '/');
        }
    });

    it('lets the console print as before and records what it printed', async () => {
        const { buffer, posts, printed, fakeConsole } = install();
        const failure = new Error('twin failed');
        fakeConsole.error('reader', failure);
        fakeConsole.warn('slow frame', { ms: 41 });
        fakeConsole.log('plain');
        expect(printed).toEqual([['error', 'reader', failure], ['warn', 'slow frame', { ms: 41 }], ['log', 'plain']]);

        const all = await entries(buffer, posts);
        const fromConsole = all.filter((entry) => entry.source === 'console');
        expect(fromConsole.map((entry) => [entry.level, entry.message])).toEqual([
            ['error', 'reader Error: twin failed'],
            ['warn', 'slow frame {"ms":41}'],
            ['log', 'plain'],
        ]);
        expect(fromConsole[0]?.stack).toContain('twin failed');
    });

    it('records an uncaught error with its file, line and column', async () => {
        const { buffer, posts } = install();
        window.dispatchEvent(new ErrorEvent('error', {
            message: 'Uncaught TypeError: x is not a function',
            filename: 'http://127.0.0.1:9749/assets/index.js',
            lineno: 12,
            colno: 7,
            error: new TypeError('x is not a function'),
        }));
        const all = await entries(buffer, posts);
        const uncaught = all.find((entry) => entry.source === 'window');
        expect(uncaught?.level).toBe('error');
        expect(uncaught?.message).toBe('Uncaught TypeError: x is not a function');
        expect(uncaught?.url).toBe('http://127.0.0.1:9749/assets/index.js');
        expect(uncaught?.line).toBe(12);
        expect(uncaught?.col).toBe(7);
        expect(uncaught?.stack).toContain('x is not a function');
    });

    it('records an unhandled rejection', async () => {
        const { buffer, posts } = install();
        const event = new Event('unhandledrejection') as Event & { reason: unknown };
        event.reason = new Error('nobody caught this');
        window.dispatchEvent(event);
        const all = await entries(buffer, posts);
        const rejected = all.find((entry) => entry.source === 'promise');
        expect(rejected?.level).toBe('error');
        expect(rejected?.message).toBe('Error: nobody caught this');
    });

    it('records what the clients announce through the observer seam', async () => {
        const { buffer, posts } = install();
        reportError({ source: 'rpc', level: 'error', message: '/rpc get_code_snippet: HTTP 500', detail: 'oom' });
        const all = await entries(buffer, posts);
        const announced = all.find((entry) => entry.source === 'rpc');
        expect(announced?.message).toBe('/rpc get_code_snippet: HTTP 500');
        expect(announced?.detail).toBe('oom');
    });

    it('flushes with final when the page hides', () => {
        const { buffer } = install();
        const flush = vi.spyOn(buffer, 'flush');
        window.dispatchEvent(new Event('pagehide'));
        expect(flush).toHaveBeenCalledWith(true);
    });

    it('puts the console back on uninstall and stops listening', async () => {
        const { buffer, posts, printed, fakeConsole } = install();
        const wrapped = fakeConsole.log;
        handle?.uninstall();
        handle = undefined;
        expect(fakeConsole.log).not.toBe(wrapped);
        fakeConsole.log('after');
        reportError({ source: 'api', level: 'warn', message: 'late' });
        expect(printed[printed.length - 1]).toEqual(['log', 'after']);
        const all = await entries(buffer, posts);
        expect(all.some((entry) => entry.message === 'after')).toBe(false);
        expect(all.some((entry) => entry.message === 'late')).toBe(false);
    });
});

describe('describeArgs and safeStringify', () => {
    it('renders strings as they are, errors with their name, objects as JSON with cycles held', () => {
        const loop: Record<string, unknown> = { name: 'loop' };
        loop['self'] = loop;
        expect(describeArgs(['a', 1, true, null, undefined, loop]).message)
            .toBe('a 1 true null undefined {"name":"loop","self":"[circular]"}');
        expect(safeStringify(10n)).toBe('10n');
        expect(safeStringify(() => 1)).toBe('[function anonymous]');
        // A string is passed through as it is; the field cap applies later in
        // the buffer. An object is cut here, so one huge argument cannot
        // crowd out the rest of the line.
        expect(safeStringify('x'.repeat(2500)).length).toBe(2500);
        expect(safeStringify({ s: 'x'.repeat(2500) }).length).toBeLessThan(2100);
    });
});

describe('httpUiLogTransport', () => {
    it('posts JSON to /api/ui-log and reports the server verdict', async () => {
        const calls: { url: string; init: RequestInit | undefined }[] = [];
        const transport = httpUiLogTransport({
            base: 'http://127.0.0.1:9749',
            fetch: ((url: string, init?: RequestInit) => {
                calls.push({ url, init });
                return Promise.resolve({ ok: true } as Response);
            }) as unknown as typeof globalThis.fetch,
        });
        const payload: UiLogPayload = { page: '/', session: 's', entries: [] };
        expect(await transport.send(payload, false)).toBe(true);
        expect(calls[0]?.url).toBe('http://127.0.0.1:9749/api/ui-log');
        expect(calls[0]?.init?.method).toBe('POST');
        expect((calls[0]?.init?.headers as Record<string, string>)['Content-Type']).toBe('application/json');
        expect(calls[0]?.init?.body).toBe(JSON.stringify(payload));
        expect(calls[0]?.init?.keepalive).toBe(false);
    });

    it('uses the beacon for a final batch and never throws', async () => {
        const beacons: { url: string; type: string }[] = [];
        const transport = httpUiLogTransport({
            beacon: (url, body) => {
                beacons.push({ url, type: body.type });
                return true;
            },
            fetch: (() => {
                throw new Error('must not be used for a final batch');
            }) as unknown as typeof globalThis.fetch,
        });
        expect(await transport.send({ page: '/', session: 's', entries: [] }, true)).toBe(true);
        expect(beacons).toEqual([{ url: '/api/ui-log', type: 'application/json' }]);

        const failing = httpUiLogTransport({
            fetch: (() => Promise.reject(new Error('down'))) as unknown as typeof globalThis.fetch,
        });
        expect(await failing.send({ page: '/', session: 's', entries: [] }, false)).toBe(false);
    });

    it('sends 120 identical console warnings through the transport as one entry with a count', async () => {
        const bodies: UiLogPayload[] = [];
        const fetch = vi.fn(async (_url: string | URL | Request, init?: RequestInit) => { bodies.push(JSON.parse(String(init?.body))); return new Response('{}'); });
        const beaconed: Promise<void>[] = [];
        const beacon = vi.fn((_url: string, body: Blob) => { beaconed.push(new Promise<string>((resolve) => { const reader = new FileReader(); reader.onload = () => resolve(String(reader.result)); reader.readAsText(body); }).then((text) => { bodies.push(JSON.parse(text)); })); return true; });
        const buffer = new UiLogBuffer({ page: '/?project=cbm', session: 'three', transport: httpUiLogTransport({ fetch: fetch as typeof globalThis.fetch, beacon }),
            schedule: () => 0, cancel: () => undefined, bufferMax: 200, batchMax: 200 });
        const printed: unknown[] = [];
        const fakeConsole = { debug: () => undefined, log: () => undefined, info: () => undefined, error: () => undefined,
            warn: (...args: unknown[]) => printed.push(args[0]) } as unknown as Console;
        handle = installUiLog({ buffer, console: fakeConsole, session: 'three', getProject: () => 'cbm' });
        const clock = 'THREE.THREE.Clock: This module has been deprecated. Please use THREE.Timer instead.';
        for (let i = 0; i < 120; i++) fakeConsole.warn(clock);
        await buffer.flush();
        window.dispatchEvent(new Event('pagehide'));
        await vi.waitFor(() => expect(beaconed).toHaveLength(1));
        await Promise.all(beaconed);
        // The console still prints every call; the server gets the first in full and the counts.
        expect(printed).toHaveLength(120);
        const posted = bodies.flatMap((body) => body.entries).filter((entry) => entry.message === clock);
        expect(posted.map((entry) => repeatCount(entry.detail) ?? 1)).toEqual([1, 10, 100, 120]);
        expect(beacon).toHaveBeenCalledOnce();
    });
});
