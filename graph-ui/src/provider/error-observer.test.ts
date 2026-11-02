/*
 * The observer seam: a failing /rpc or /api call is announced before it is
 * thrown, with a level that tells a refused tool from a broken server, and
 * a listener that fails does not make a second error out of the first.
 */

import { afterEach, describe, expect, it } from 'vitest';

import { AtlasApi } from '../app/atlas-api';
import { hasErrorObservers, observeErrors, reportError } from './error-observer';
import type { ErrorReport } from './error-observer';
import { callToolJson, callToolText } from './rpc-transport';

let stop: (() => void) | undefined;

afterEach(() => {
    stop?.();
    stop = undefined;
});

function listen(): ErrorReport[] {
    const reports: ErrorReport[] = [];
    stop = observeErrors((report) => reports.push(report));
    return reports;
}

function replying(status: number, body: string): typeof globalThis.fetch {
    return (() => Promise.resolve({
        ok: status >= 200 && status < 300,
        status,
        statusText: '',
        text: () => Promise.resolve(body),
    })) as unknown as typeof globalThis.fetch;
}

describe('reportError', () => {
    it('reaches every listener, survives one that throws, and is silent without any', () => {
        expect(hasErrorObservers()).toBe(false);
        reportError({ source: 'ui', level: 'info', message: 'nobody listens' });
        const seen: string[] = [];
        const stopFirst = observeErrors(() => {
            throw new Error('a bad listener');
        });
        stop = observeErrors((report) => seen.push(report.message));
        expect(hasErrorObservers()).toBe(true);
        reportError({ source: 'ui', level: 'warn', message: 'both' });
        expect(seen).toEqual(['both']);
        stopFirst();
    });
});

describe('the /rpc client announces', () => {
    it('retains request ownership for transport and payload failures and marks project-free calls global', async () => {
        const reports = listen();
        await expect(callToolText('query_graph', { project: 'A' }, { fetch: replying(500, 'oom') })).rejects.toThrow();
        await expect(callToolJson('index_status', { project: 'B' }, {
            fetch: replying(200, '{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"not json"}]}}'),
        })).rejects.toThrow();
        await expect(callToolText('list_projects', {}, { fetch: replying(500, 'down') })).rejects.toThrow();
        expect(reports.map((report) => report.project)).toEqual(['A', 'B', '']);
    });

    it('a refused tool as info, with the tool name in the message', async () => {
        const reports = listen();
        await expect(callToolText('index_repository', {}, {
            fetch: replying(403, '{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"not on /rpc"}}'),
        })).rejects.toThrow();
        expect(reports.length).toBe(1);
        expect(reports[0]?.source).toBe('rpc');
        expect(reports[0]?.level).toBe('info');
        expect(reports[0]?.message).toContain('/rpc index_repository: HTTP 403');
    });

    it('a broken server as error, with the body as detail', async () => {
        const reports = listen();
        await expect(callToolText('query_graph', {}, { fetch: replying(500, 'oom') })).rejects.toThrow();
        expect(reports[0]?.level).toBe('error');
        expect(reports[0]?.detail).toBe('oom');
    });

    it('a tool that said no as warn, and an unreadable answer as error', async () => {
        const reports = listen();
        await expect(callToolText('get_code_snippet', {}, {
            fetch: replying(200, '{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"no such symbol"}],"isError":true}}'),
        })).rejects.toThrow();
        await expect(callToolJson('list_projects', {}, {
            fetch: replying(200, '{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"not json"}]}}'),
        })).rejects.toThrow();
        expect(reports.map((report) => report.level)).toEqual(['warn', 'error']);
        expect(reports[1]?.message).toContain('/rpc list_projects');
    });

    it('an unreachable server as error, and an aborted call not at all', async () => {
        const reports = listen();
        await expect(callToolText('search_graph', {}, {
            fetch: (() => Promise.reject(new TypeError('Failed to fetch'))) as unknown as typeof globalThis.fetch,
        })).rejects.toThrow();
        const aborted = new Error('The user aborted a request.');
        aborted.name = 'AbortError';
        await expect(callToolText('search_graph', {}, {
            fetch: (() => Promise.reject(aborted)) as unknown as typeof globalThis.fetch,
        })).rejects.toThrow();
        expect(reports.length).toBe(1);
        expect(reports[0]?.message).toBe('/rpc search_graph: Failed to fetch');
    });
});

describe('the /api client announces', () => {
    it('a refused route as warn, a broken one as error, an unreachable one as error', async () => {
        const reports = listen();
        await expect(new AtlasApi({ fetch: replying(423, '{"error":"busy"}') }).saveAdr('p', 'x')).rejects.toThrow();
        await expect(new AtlasApi({ fetch: replying(500, 'no') }).tree('p')).rejects.toThrow();
        await expect(new AtlasApi({
            fetch: (() => Promise.reject(new TypeError('Failed to fetch'))) as unknown as typeof globalThis.fetch,
        }).processes()).rejects.toThrow();
        await expect(new AtlasApi({ fetch: replying(200, 'not json') }).indexJobs()).rejects.toThrow();
        expect(reports.map((report) => [report.source, report.level])).toEqual([
            ['api', 'warn'],
            ['api', 'error'],
            ['api', 'error'],
            ['api', 'error'],
        ]);
        expect(reports[0]?.message).toContain('/api/adr');
        expect(reports[0]?.detail).toBe('{"error":"busy"}');
    });
});
