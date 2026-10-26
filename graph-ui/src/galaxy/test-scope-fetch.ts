/**
 * Testhilfe: /api/layout und die beiden query_graph-Formen des Scope-Laders,
 * ueber dieselbe RPC-Strecke wie im Betrieb (dieselbe Form wie in
 * GalaxyPanel.stability.test.tsx, hier fuer beliebige Knoten und Kanten).
 *
 * `delay` haelt jede Kantenabfrage an, bis der Test sie freigibt; so laesst
 * sich ein laufendes Laden beobachten und abbrechen.
 */
import { vi } from 'vitest';
import type { GraphEdge, GraphNode } from './types';

export interface ScopeFetchOptions {
    nodes: GraphNode[];
    edges: GraphEdge[];
    /** Jede Kantenabfrage wartet auf dieses Versprechen, wenn es gesetzt ist. */
    gate?: () => Promise<void>;
    /** Die CALLS je Knoten, die der Index zaehlt (`n.in_degree`, `n.out_degree`); fehlt einer, sind es keine. */
    degrees?: Record<number, { in: number; out: number }>;
}

export function scopeNode(id: number, extra: Partial<GraphNode> = {}): GraphNode {
    return { id, name: `n${id}`, qualified_name: `sample.n${id}`, label: 'Function', file_path: `src/n${id}.ts`,
        start_line: 1, end_line: 5, x: id * 10, y: 0, z: 0, size: 2, color: '#999999', ...extra };
}

export function scopeFetch({ nodes, edges, gate, degrees = {} }: ScopeFetchOptions) {
    const calls: { tool: string; args: Record<string, unknown> }[] = [];
    const fetch = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes('/api/layout')) return new Response(JSON.stringify({ nodes, edges, total_nodes: nodes.length }));
        const request = JSON.parse(String(init?.body)) as { params: { name: string; arguments: Record<string, unknown> } };
        calls.push({ tool: request.params.name, args: request.params.arguments });
        if (request.params.name === 'index_status') {
            return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { content: [{ type: 'text', text: JSON.stringify({ indexed_at: 'generation-1' }) }] } }));
        }
        const query = String(request.params.arguments.query);
        const columns = (prefix: string) => ['id', 'label', 'name', 'qn', 'file', 'start_line', 'end_line'].map(key => prefix + key);
        const values = (entry: GraphNode) => [entry.id, entry.label, entry.name, entry.qualified_name ?? '', entry.file_path ?? '', entry.start_line ?? '', entry.end_line ?? ''].map(String);
        const names = [...query.matchAll(/qualified_name = "([^"]+)"/g)].map(match => match[1]);
        let cols: string[], rows: string[][];
        if (query.includes('n.in_degree')) {
            cols = ['id', 'calls_in', 'calls_out'];
            rows = nodes.filter(entry => names.includes(entry.qualified_name ?? ''))
                .map(entry => [String(entry.id), String(degrees[entry.id]?.in ?? 0), String(degrees[entry.id]?.out ?? 0)]);
        } else if (query.startsWith('MATCH (n)')) {
            // A file scope finds its roots by path.
            const files = [...query.matchAll(/n\.file_path = "([^"]+)"/g)].map(match => match[1]);
            cols = columns(''); rows = nodes.filter(entry => names.includes(entry.qualified_name ?? '') || files.includes(entry.file_path ?? '')).map(values);
        } else {
            if (gate) await gate();
            const inbound = query.startsWith('MATCH (b)<-');
            cols = ['edge_id', 'edge_type', 'edge_line', ...columns('a_'), ...columns('b_')];
            rows = edges.flatMap((edge, i) => {
                const a = nodes.find(entry => entry.id === edge.source)!, b = nodes.find(entry => entry.id === edge.target)!;
                const seed = inbound ? b : a;
                return names.includes(seed.qualified_name ?? '')
                    ? [[String(edge.id ?? i + 1), edge.type, String(edge.line ?? ''), ...values(a), ...values(b)]] : [];
            });
        }
        const text = `rows: ${rows.length} (cols: ${cols.join(' ')})\n${rows.map(row => '  ' + row.map(value => JSON.stringify(value || '-')).join(' ')).join('\n')}\ntotal: ${rows.length}\nhas_more: false\ntruncated: false`;
        return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { content: [{ type: 'text', text }] } }));
    });
    return { fetch, calls };
}
