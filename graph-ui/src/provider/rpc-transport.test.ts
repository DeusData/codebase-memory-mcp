import { beforeEach, describe, expect, it } from 'vitest';
import {
    callTool,
    callToolJson,
    callToolText,
    isNotAllowed,
    resetRequestIds,
    RpcError,
} from './rpc-transport';

/*
 * Kein Netz. Jeder Test reicht sein eigenes fetch herein; das globale fetch
 * wird zusaetzlich vergiftet, damit ein vergessenes Mock als Testfehler
 * auffliegt statt als stiller Verbindungsversuch.
 */
type FetchCall = { url: string; init: RequestInit };

function recordingFetch(
    reply: { ok?: boolean; status?: number; body: string },
    calls: FetchCall[],
): typeof globalThis.fetch {
    return ((url: string, init: RequestInit) => {
        calls.push({ url, init });
        return Promise.resolve({
            ok: reply.ok ?? true,
            status: reply.status ?? 200,
            statusText: '',
            text: () => Promise.resolve(reply.body),
        });
    }) as unknown as typeof globalThis.fetch;
}

function jsonReply(payload: unknown): string {
    return JSON.stringify(payload);
}

const okResult = jsonReply({
    jsonrpc: '2.0',
    id: 1,
    result: { content: [{ type: 'text', text: '{"projects":[]}' }] },
});

beforeEach(() => {
    resetRequestIds();
    globalThis.fetch = (() => {
        throw new Error('kein Netz im Unit-Test: fetch muss gemockt werden');
    }) as unknown as typeof globalThis.fetch;
});

describe('callTool', () => {
    it('postet den MCP-Umschlag auf /rpc', async () => {
        const calls: FetchCall[] = [];
        await callTool('list_projects', {}, { fetch: recordingFetch({ body: okResult }, calls) });

        expect(calls).toHaveLength(1);
        expect(calls[0].url).toBe('/rpc');
        expect(calls[0].init.method).toBe('POST');
        const body = JSON.parse(String(calls[0].init.body));
        expect(body).toMatchObject({
            jsonrpc: '2.0',
            method: 'tools/call',
            params: { name: 'list_projects', arguments: {} },
        });
        expect(typeof body.id).toBe('number');
    });

    it('haengt /rpc an die uebergebene base und reicht Argumente durch', async () => {
        const calls: FetchCall[] = [];
        await callTool(
            'get_code_snippet',
            { project: 'p', qualified_name: 'p.src.a.b' },
            { base: 'http://127.0.0.1:4200', fetch: recordingFetch({ body: okResult }, calls) },
        );

        expect(calls[0].url).toBe('http://127.0.0.1:4200/rpc');
        expect(JSON.parse(String(calls[0].init.body)).params.arguments).toEqual({
            project: 'p',
            qualified_name: 'p.src.a.b',
        });
    });

    it('zaehlt die Request-Id hoch', async () => {
        const calls: FetchCall[] = [];
        const fetchImpl = recordingFetch({ body: okResult }, calls);
        await callTool('list_projects', {}, { fetch: fetchImpl });
        await callTool('index_status', {}, { fetch: fetchImpl });

        const ids = calls.map((c) => JSON.parse(String(c.init.body)).id);
        expect(ids[1]).toBe(ids[0] + 1);
    });
});

describe('callToolText und callToolJson', () => {
    it('callToolText liefert content[0].text roh', async () => {
        const text = await callToolText(
            'query_graph',
            {},
            {
                fetch: recordingFetch(
                    {
                        body: jsonReply({
                            result: { content: [{ text: 'rows: 0  (cols: a)\ntotal: 0\n' }] },
                        }),
                    },
                    [],
                ),
            },
        );
        expect(text).toBe('rows: 0  (cols: a)\ntotal: 0\n');
    });

    it('callToolJson parst denselben Text als JSON', async () => {
        const value = await callToolJson<{ projects: string[] }>(
            'list_projects',
            {},
            { fetch: recordingFetch({ body: okResult }, []) },
        );
        expect(value).toEqual({ projects: [] });
    });

    it('callToolJson meldet einen Formfehler, wenn der Text kein JSON ist', async () => {
        const failing = callToolJson('query_graph', {}, {
            fetch: recordingFetch({ body: jsonReply({ result: { content: [{ text: 'rows: 0' }] } }) }, []),
        });
        await expect(failing).rejects.toMatchObject({ kind: 'shape', toolName: 'query_graph' });
    });
});

describe('Fehlertaxonomie', () => {
    it('403 mit JSON-RPC-Body wird als http-Fehler mit notAllowed gemeldet', async () => {
        const reply = jsonReply({
            jsonrpc: '2.0',
            id: 1,
            error: { code: -32601, message: 'UI RPC method is not allowed' },
        });
        let caught: unknown;
        try {
            await callTool('index_repository', {}, {
                fetch: recordingFetch({ ok: false, status: 403, body: reply }, []),
            });
        } catch (err) {
            caught = err;
        }
        expect(caught).toBeInstanceOf(RpcError);
        const error = caught as RpcError;
        expect(error.kind).toBe('http');
        expect(error.status).toBe(403);
        expect(error.code).toBe(-32601);
        expect(error.notAllowed).toBe(true);
        expect(isNotAllowed(error)).toBe(true);
    });

    it('ein anderer HTTP-Fehler ist http, aber nicht notAllowed', async () => {
        const failing = callTool('list_projects', {}, {
            fetch: recordingFetch({ ok: false, status: 500, body: 'boom' }, []),
        });
        await expect(failing).rejects.toMatchObject({ kind: 'http', status: 500, notAllowed: false });
    });

    it('ein error-Objekt bei HTTP 200 ist ein rpc-Fehler', async () => {
        const failing = callTool('query_graph', {}, {
            fetch: recordingFetch(
                { body: jsonReply({ error: { code: -32600, message: 'invalid request' } }) },
                [],
            ),
        });
        await expect(failing).rejects.toMatchObject({
            kind: 'rpc',
            code: -32600,
            notAllowed: false,
        });
    });

    it('-32601 im Body bleibt auch bei HTTP 200 notAllowed', async () => {
        const failing = callTool('ingest_traces', {}, {
            fetch: recordingFetch(
                { body: jsonReply({ error: { code: -32601, message: 'not allowed' } }) },
                [],
            ),
        });
        await expect(failing).rejects.toMatchObject({ kind: 'rpc', notAllowed: true });
    });

    it('isError im MCP-Result ist ein tool-Fehler, kein Formfehler', async () => {
        const failing = callTool('get_code_snippet', {}, {
            fetch: recordingFetch(
                {
                    body: jsonReply({
                        result: {
                            isError: true,
                            content: [{ type: 'text', text: 'Symbol not found: p.src.a.missing' }],
                        },
                    }),
                },
                [],
            ),
        });
        await expect(failing).rejects.toMatchObject({ kind: 'tool', toolName: 'get_code_snippet' });
        await expect(failing).rejects.toThrow(/Symbol not found/);
    });

    it('eine Antwort ohne content[0].text ist ein Formfehler', async () => {
        const failing = callTool('list_projects', {}, {
            fetch: recordingFetch({ body: jsonReply({ result: { content: [] } }) }, []),
        });
        await expect(failing).rejects.toMatchObject({ kind: 'shape' });
    });

    it('eine Antwort, die kein JSON ist, ist ein Formfehler', async () => {
        const failing = callTool('list_projects', {}, {
            fetch: recordingFetch({ body: '<html>proxy</html>' }, []),
        });
        await expect(failing).rejects.toMatchObject({ kind: 'shape' });
    });
});
