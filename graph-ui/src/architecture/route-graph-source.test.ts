import { describe, expect, it, vi } from 'vitest';
import { loadRouteGraph } from './route-graph-source';
import type { QueryGraphResult } from '../provider/rpc-schemas';

const columns = ['source', 'source_qn', 'source_file', 'source_line', 'source_name', 'target', 'target_qn',
    'target_file', 'target_line', 'target_name', 'edge_id', 'edge_type', 'route_path', 'via'];
const row = (id: number) => [String(id), `fixture.caller${id}`, 'src/client.ts', '10', `caller${id}`,
    '50', 'fixture.route.users', '', '', '/users', String(100 + id), 'HTTP_CALLS', '/users', 'fetch'];
const empty: QueryGraphResult = { columns, rows: [], total: 0 };
const isHttpFunction = (query: string) => query.includes('(s:Function)') && query.includes('HTTP_CALLS');

describe('route relationship query loader', () => {
    it('queries fixed labeled endpoints and retains source-free route evidence', async () => {
        const queryGraph = vi.fn(async (_project: string, query: string) => isHttpFunction(query)
            ? { columns, rows: [row(1)], total: 1 } : empty);
        const result = await loadRouteGraph('actual-project', { client: { queryGraph } });
        expect(queryGraph).toHaveBeenCalledTimes(6);
        expect(queryGraph.mock.calls.every(call => call[0] === 'actual-project'
            && call[1].includes('id(s) AS source') && call[1].includes('LIMIT 301'))).toBe(true);
        expect(result.relationships[0]).toMatchObject({ type: 'HTTP_CALLS', routePath: '/users', via: 'fetch',
            source: { id: 1, qualified_name: 'fixture.caller1', file_path: 'src/client.ts' },
            target: { id: 50, label: 'Route', file_path: undefined } });
        expect(result.truncated).toBe(false);
    });
    it('follows valid continuations and does not silently claim a repeated cursor is complete', async () => {
        const queryGraph = vi.fn(async (_project: string, query: string, cursor?: string) => {
            if (!isHttpFunction(query)) return empty;
            return cursor ? { columns, rows: [row(2)], total: 3, offset: 1, nextOffset: 2, hasMore: true, nextCursor: 'one' }
                : { columns, rows: [row(1)], total: 3, offset: 0, nextOffset: 1, hasMore: true, nextCursor: 'one' };
        });
        const result = await loadRouteGraph('p', { client: { queryGraph } });
        expect(result.relationships).toHaveLength(2);
        expect(result.truncated).toBe(true);
        expect(result.warnings.join(' ')).toContain('limited');
    });
    it('preserves prior pages when a later page changes its snapshot or a family is unavailable', async () => {
        const queryGraph = vi.fn(async (_project: string, query: string, cursor?: string) => {
            if (!isHttpFunction(query)) throw new Error('Unsupported relationship');
            return cursor ? { columns, rows: [row(2)], total: 2, offset: 9 }
                : { columns, rows: [row(1)], total: 2, offset: 0, nextOffset: 1, nextCursor: 'next', hasMore: true };
        });
        const result = await loadRouteGraph('p', { client: { queryGraph } });
        expect(result.relationships.map(edge => edge.source.id)).toEqual([1]);
        expect(result.truncated).toBe(true);
        expect(result.warnings.join(' ')).toContain('changed its snapshot');
        expect(result.warnings.join(' ')).toContain('Unsupported relationship');
    });
    it('reads one sentinel past the per-family cap and reports omitted relationships', async () => {
        const queryGraph = vi.fn(async (_project: string, query: string) => isHttpFunction(query)
            ? { columns, rows: Array.from({ length: 301 }, (_, id) => row(id)), total: 301 } : empty);
        const result = await loadRouteGraph('p', { client: { queryGraph } });
        expect(result.relationships).toHaveLength(300);
        expect(result.truncated).toBe(true);
        expect(result.warnings.join(' ')).toContain('300 rows');
    });
    it('refuses malformed IDs and unexpected relationship types without inventing graph identities', async () => {
        const invalid = row(1); invalid[0] = '';
        const unknown = row(2); unknown[11] = 'CALLS';
        const queryGraph = vi.fn(async (_project: string, query: string) => isHttpFunction(query)
            ? { columns, rows: [invalid, unknown], total: 2 } : empty);
        const result = await loadRouteGraph('p', { client: { queryGraph } });
        expect(result.relationships).toEqual([]);
        expect(result.truncated).toBe(true);
    });
    it('propagates cancellation so an old project cannot produce a ready result', async () => {
        const controller = new AbortController(); controller.abort();
        const queryGraph = vi.fn(async () => empty);
        await expect(loadRouteGraph('old-project', { signal: controller.signal, client: { queryGraph } })).rejects.toThrow('Aborted');
        expect(queryGraph).not.toHaveBeenCalled();
    });
});
