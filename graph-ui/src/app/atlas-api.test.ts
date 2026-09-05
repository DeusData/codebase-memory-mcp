/*
 * The /api client of the projects panel: which route, which method, which
 * body each call sends, and what it makes of the answer. A recording fetch
 * stands in for the server; nothing here touches the network.
 */

import { describe, expect, it } from 'vitest';

import { AtlasApi, AtlasApiError } from './atlas-api';

interface FetchCall {
    url: string;
    init: RequestInit | undefined;
}

function recordingFetch(
    reply: { ok?: boolean; status?: number; body: string },
    calls: FetchCall[],
): typeof globalThis.fetch {
    return ((url: string, init?: RequestInit) => {
        calls.push({ url, init });
        return Promise.resolve({
            ok: reply.ok ?? true,
            status: reply.status ?? 200,
            statusText: '',
            text: () => Promise.resolve(reply.body),
        });
    }) as unknown as typeof globalThis.fetch;
}

function api(reply: { ok?: boolean; status?: number; body: string }): { api: AtlasApi; calls: FetchCall[] } {
    const calls: FetchCall[] = [];
    return { api: new AtlasApi({ base: 'http://127.0.0.1:9749', fetch: recordingFetch(reply, calls) }), calls };
}

describe('AtlasApi, the projects routes', () => {
    it('starts an index job with a JSON body over POST', async () => {
        const { api: client, calls } = api({ status: 202, body: '{"status":"indexing","slot":1,"path":"/repo"}' });
        const started = await client.startIndex('/repo', 'repo');
        expect(started).toEqual({ slot: 1, path: '/repo' });
        expect(calls[0]?.url).toBe('http://127.0.0.1:9749/api/index');
        expect(calls[0]?.init?.method).toBe('POST');
        expect(calls[0]?.init?.body).toBe('{"root_path":"/repo","project_name":"repo"}');
        expect((calls[0]?.init?.headers as Record<string, string>)['Content-Type']).toBe('application/json');
    });

    it('reads the job table', async () => {
        const { api: client, calls } = api({ body: '[{"slot":0,"status":"done","path":"/a","error":""}]' });
        const jobs = await client.indexJobs();
        expect(jobs).toEqual([{ slot: 0, status: 'done', path: '/a', error: '' }]);
        expect(calls[0]?.url).toBe('http://127.0.0.1:9749/api/index-status');
        expect(calls[0]?.init?.method).toBe('GET');
        expect(calls[0]?.init?.body).toBeUndefined();
    });

    it('browses with the path in the query, and without one for the server root', async () => {
        const { api: client, calls } = api({ body: '{"path":"/Users/x","dirs":["a"],"parent":"/Users"}' });
        const level = await client.browse('/Users/x');
        expect(level.dirs).toEqual(['a']);
        expect(calls[0]?.url).toBe('http://127.0.0.1:9749/api/browse?path=%2FUsers%2Fx');
        await client.browse('');
        expect(calls[1]?.url).toBe('http://127.0.0.1:9749/api/browse');
    });

    it('deletes over DELETE with the name in the query', async () => {
        const { api: client, calls } = api({ body: '{"deleted":true}' });
        await client.deleteProject('my project');
        expect(calls[0]?.url).toBe('http://127.0.0.1:9749/api/project?name=my+project');
        expect(calls[0]?.init?.method).toBe('DELETE');
    });

    it('reads the health verdict', async () => {
        const { api: client, calls } = api({ body: '{"status":"healthy","nodes":1,"edges":2,"size_bytes":3}' });
        expect(await client.projectHealth('p')).toEqual({ status: 'healthy', nodes: 1, edges: 2, sizeBytes: 3, reason: '' });
        expect(calls[0]?.url).toBe('http://127.0.0.1:9749/api/project-health?name=p');
    });

    it('reads and stores the decision record', async () => {
        const { api: client, calls } = api({ body: '{"has_adr":true,"content":"# x","updated_at":"now"}' });
        expect(await client.adr('p')).toEqual({ hasAdr: true, content: '# x', updatedAt: 'now' });
        expect(calls[0]?.url).toBe('http://127.0.0.1:9749/api/adr?project=p');
        await client.saveAdr('p', '# y');
        expect(calls[1]?.url).toBe('http://127.0.0.1:9749/api/adr');
        expect(calls[1]?.init?.method).toBe('POST');
        expect(calls[1]?.init?.body).toBe('{"project":"p","content":"# y"}');
    });

    it('reads the log tail and the process report', async () => {
        const { api: client, calls } = api({ body: '{"lines":["a"],"total":9}' });
        expect(await client.logs(200)).toEqual({ lines: ['a'], total: 9 });
        expect(calls[0]?.url).toBe('http://127.0.0.1:9749/api/logs?lines=200');
        const processes = api({ body: '{"self_pid":7,"self_rss_mb":1.5,"processes":[]}' });
        expect(await processes.api.processes()).toEqual({ selfPid: 7, selfRssMb: 1.5, processes: [] });
        expect(processes.calls[0]?.url).toBe('http://127.0.0.1:9749/api/processes');
    });

    it('carries the status of a refusal, so busy can be told from forbidden', async () => {
        const { api: client } = api({ ok: false, status: 423, body: '{"error":"project is busy; retry after indexing"}' });
        await expect(client.saveAdr('p', 'x')).rejects.toSatisfy((error: unknown) =>
            error instanceof AtlasApiError && error.status === 423 && error.route === '/api/adr');
    });
});
