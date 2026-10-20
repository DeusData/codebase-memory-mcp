// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel, { type GalaxyPanelProps } from './GalaxyPanel';
import type { GraphData, GraphNode } from './types';

/** The layout can be capped; focused relationships come independently from RPC. */
function graphFetch(nodes: GraphNode[], edges: GraphData['edges'], total = nodes.length) {
    return vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes('/api/layout')) return new Response(JSON.stringify({ nodes, edges, total_nodes: total }));
        const request = JSON.parse(String(init?.body)) as { params: { name: string; arguments: { query: string } } };
        if (request.params.name === 'index_status') return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { content: [{ type: 'text', text: JSON.stringify({ indexed_at: 'generation-1' }) }] } }));
        const query = request.params.arguments.query;
        const columns = (prefix: string) => ['id', 'label', 'name', 'qn', 'file', 'start_line', 'end_line'].map(key => prefix + key);
        const values = (node: GraphNode) => [node.id, node.label, node.name, node.qualified_name ?? '', node.file_path ?? '', node.start_line ?? '', node.end_line ?? ''].map(String);
        let cols: string[], rows: string[][];
        if (query.startsWith('MATCH (n)')) {
            const qualifiedName = /n\.qualified_name = "([^"]+)"/.exec(query)?.[1];
            cols = columns(''); rows = nodes.filter(node => qualifiedName ? node.qualified_name === qualifiedName : node.file_path === 'src/service.ts' || node.file_path === 'src/a.ts').map(values);
        } else {
            cols = ['edge_id', 'edge_type', 'edge_line', ...columns('a_'), ...columns('b_')];
            const names = [...query.matchAll(/qualified_name = "([^"]+)"/g)].map(match => match[1]);
            rows = edges.flatMap((edge, i) => {
                const a = nodes.find(node => node.id === edge.source)!, b = nodes.find(node => node.id === edge.target)!;
                const include = query.includes('file_path') ? ['src/service.ts', 'src/a.ts'].includes(a.file_path ?? '') || ['src/service.ts', 'src/a.ts'].includes(b.file_path ?? '')
                    : names.includes(a.qualified_name ?? '') || names.includes(b.qualified_name ?? '');
                return include ? [[String(i + 1), edge.type, String(edge.line ?? ''), ...values(a), ...values(b)]] : [];
            });
        }
        const text = `rows: ${rows.length} (cols: ${cols.join(' ')})\n${rows.map(row => '  ' + row.map(value => JSON.stringify(value || '-')).join(' ')).join('\n')}\ntotal: ${rows.length}\nhas_more: false\ntruncated: false`;
        return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { content: [{ type: 'text', text }] } }));
    });
}

vi.mock('./GraphScene', async importOriginal => ({
    ...await importOriginal<typeof import('./GraphScene')>(),
    GraphScene: ({ highlightedIds, data, idleRotation }: { highlightedIds: Set<number> | null; data: GraphData; idleRotation?: boolean }) => <>
        <output data-testid="graph-selection">{JSON.stringify([...highlightedIds ?? []].sort((a, b) => a - b))}</output>
        <output data-testid="graph-data" data-idle-rotation={String(idleRotation)}>{JSON.stringify(data)}</output>
    </>,
}));
let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});

it('opens a file hierarchy without a caret symbol and follows literal marked ranges', async () => {
    const common = { file_path: 'src/service.ts', x: 0, y: 0, z: 0, size: 1, color: '#999999' };
    const nodes: GraphNode[] = [
        { ...common, id: 10, name: 'service.ts', qualified_name: 'sample.file', label: 'File', start_line: 1, end_line: 50 },
        { ...common, id: 11, name: 'Service', qualified_name: 'sample.Service', label: 'Class', start_line: 2, end_line: 49 },
        { ...common, id: 12, name: 'read', qualified_name: 'sample.Service.read', label: 'Method', start_line: 5, end_line: 15 },
        { ...common, id: 13, name: 'write', qualified_name: 'sample.Service.write', label: 'Method', start_line: 20, end_line: 35 },
        { ...common, id: 14, name: 'isolated', qualified_name: 'sample.isolated', label: 'Function', start_line: 40, end_line: 45 },
        { ...common, id: 15, name: 'caller', qualified_name: 'sample.caller', label: 'Function', file_path: 'src/client.ts' },
        { ...common, id: 16, name: 'database', qualified_name: 'sample.database', label: 'Function', file_path: 'src/db.ts' },
        { ...common, id: 17, name: 'unrelated', qualified_name: 'sample.unrelated', label: 'Function', file_path: 'src/other.ts' },
    ];
    const edges = [
        { source: 10, target: 11, type: 'DEFINES' },
        { source: 11, target: 12, type: 'DEFINES_METHOD' },
        { source: 11, target: 13, type: 'DEFINES_METHOD' },
        { source: 15, target: 12, type: 'CALLS', line: 8 },
        { source: 12, target: 16, type: 'CALLS', line: 10 },
        { source: 13, target: 16, type: 'USES_TYPE' },
        { source: 16, target: 17, type: 'CALLS' },
    ];
    const fetchLayout = graphFetch(nodes, edges, 100);
    const props: GalaxyPanelProps = { project: 'sample', visible: true, onOpenNode: vi.fn(), fetch: fetchLayout,
        focusFilePath: 'src/service.ts' };
    const names = () => ((JSON.parse(host.querySelector('[data-testid="graph-data"]')!.textContent!) as GraphData)
        .nodes.map(node => node.qualified_name).sort());
    const expectNames = (expected: string[]) => vi.waitFor(async () => {
        await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
        expect(names()).toEqual(expected);
    });
    const relationships = () => {
        const data = JSON.parse(host.querySelector('[data-testid="graph-data"]')!.textContent!) as GraphData;
        const nameById = new Map(data.nodes.map(node => [node.id, node.qualified_name]));
        return data.edges.map(edge => `${nameById.get(edge.source)}>${nameById.get(edge.target)}:${edge.type}`).sort();
    };
    await act(async () => root.render(<GalaxyPanel {...props} />));
    await act(async () => host.querySelector<HTMLButtonElement>('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.click());
    const wholeFile = ['sample.file', 'sample.Service', 'sample.Service.read', 'sample.Service.write', 'sample.isolated', 'sample.caller', 'sample.database'].sort();
    await expectNames(wholeFile);
    expect(host.querySelector('.atlas-galaxy-placeholder')).toBeNull();
    expect(host.querySelector('[data-testid="graph-data"]')?.getAttribute('data-idle-rotation')).toBe('false');
    expect(relationships()).toContain('sample.Service.write>sample.database:USES_TYPE');
    expect(relationships()).not.toContain('sample.database>sample.unrelated:CALLS');
    expect(host.textContent).toContain('All indexed direct dependencies included.');
    await act(async () => root.render(<GalaxyPanel {...props} focusSourceRange={{ startLine: 8, endLine: 10 }} />));
    await expectNames(['sample.Service', 'sample.Service.read', 'sample.caller', 'sample.database'].sort());
    expect(relationships()).toEqual([
        'sample.Service>sample.Service.read:DEFINES_METHOD',
        'sample.Service.read>sample.database:CALLS',
        'sample.caller>sample.Service.read:CALLS',
    ].sort());
    await act(async () => root.render(<GalaxyPanel {...props} />));
    await expectNames(wholeFile);
    expect(fetchLayout.mock.calls.filter(([url]) => String(url).includes('/api/layout'))).toHaveLength(1);
});

it('does not report a file missing from a capped layout while its indexed scope is loading', async () => {
    const node: GraphNode = { id: 15, name: 'outside', qualified_name: 'sample.outside', label: 'Function',
        file_path: 'src/a.ts', start_line: 1, end_line: 10, x: 0, y: 0, z: 0, size: 1, color: '#999999' };
    const source = graphFetch([node], []);
    let release!: () => void;
    const pending = new Promise<void>(resolve => { release = resolve; });
    const fetchImpl = vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes('/api/layout')) return new Response(JSON.stringify({ nodes: [], edges: [], total_nodes: 100 }));
        if (String(init?.body).includes('MATCH (n)')) await pending;
        return source(url, init);
    });
    await act(async () => root.render(<GalaxyPanel project="sample" visible focusFilePath="src/a.ts" onOpenNode={vi.fn()} fetch={fetchImpl} />));
    // Hand test K8: the status names the layer that is loading instead of an endless "Loading relationships".
    // It may first say "Checking index…" while the index generation is read; wait for the load itself.
    await vi.waitFor(() => expect(host.textContent).toContain('Loading layer 1…'));
    expect(host.querySelector('[data-testid="atlas-galaxy-note"]')).toBeNull();
    await act(async () => { release(); });
    expect(host.textContent).toContain('All indexed direct dependencies included.');
    expect(host.textContent).not.toContain('not in the loaded graph');
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); globalThis.__atlasGalaxy = undefined; });

it('selects the whole file, narrows only to marked code, and restores the file when the mark clears', async () => {
    const common = { file_path: 'src/a.ts', x: 0, y: 0, z: 0, size: 1, color: '#999999' };
    const nodes: GraphNode[] = [
        { ...common, id: 1, name: 'first', qualified_name: 'sample.first', label: 'Function', start_line: 2, end_line: 8 },
        { ...common, id: 2, name: 'second', qualified_name: 'sample.second', label: 'Function', start_line: 10, end_line: 20 },
        { ...common, id: 3, name: 'isolated', qualified_name: 'sample.isolated', label: 'Function', start_line: 25, end_line: 28 },
        { ...common, id: 4, name: 'a.ts', label: 'File', start_line: 1, end_line: 40 },
        { ...common, id: 5, name: 'owner', label: 'Class', start_line: 1, end_line: 30 },
        { ...common, id: 6, name: 'external', label: 'Function', file_path: 'src/b.ts', start_line: 1, end_line: 10 },
    ];
    const edges = [{ source: 1, target: 6, type: 'CALLS' }, { source: 5, target: 2, type: 'DEFINES_METHOD' }];
    for (const node of nodes) node.qualified_name ??= `sample.n${node.id}`;
    const fetchLayout = graphFetch(nodes, edges);
    const props: GalaxyPanelProps = { project: 'sample', visible: true, onOpenNode: vi.fn(), fetch: fetchLayout,
        focusFilePath: 'src/a.ts', focusQualifiedName: 'sample.first' };
    const selected = () => JSON.parse(host.querySelector('[data-testid="graph-selection"]')!.textContent!) as number[];
    const expectSelection = (ids: number[]) => vi.waitFor(async () => {
        await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
        expect(selected()).toEqual(ids);
    });
    await act(async () => root.render(<GalaxyPanel {...props} />));
    await expectSelection([1, 2, 3, 4, 5]);
    await act(async () => root.render(<GalaxyPanel {...props} focusQualifiedName="sample.second" />));
    await expectSelection([1, 2, 3, 4, 5]);
    await act(async () => root.render(<GalaxyPanel {...props} focusSourceRange={{ startLine: 12, endLine: 14 }} />));
    await expectSelection([2]);
    await act(async () => root.render(<GalaxyPanel {...props} focusSourceRange={{ startLine: 6, endLine: 12 }} />));
    await expectSelection([1, 2]);
    await act(async () => root.render(<GalaxyPanel {...props} />));
    await expectSelection([1, 2, 3, 4, 5]);
    expect(fetchLayout.mock.calls.filter(([url]) => String(url).includes('/api/layout'))).toHaveLength(1);
});
