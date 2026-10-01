// @vitest-environment jsdom
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, expect, it, vi } from 'vitest';
import { useGraphScope } from './use-graph-scope';
import type { ScopedGraph } from './graph-scope';
import type { GraphData, GraphNode } from './types';

const state = vi.hoisted(() => ({ generation: 'first', load: vi.fn() }));
vi.mock('../provider/rpc-client', () => ({ RpcIntelligenceClient: class {
    async indexStatusPayload() { return { indexed_at: state.generation }; }
} }));
vi.mock('./graph-scope', async original => ({ ...await original<typeof import('./graph-scope')>(), loadGraphScope: state.load }));
let dispose: (() => Promise<void>) | undefined;
afterEach(async () => { await dispose?.(); state.load.mockReset(); state.generation = 'first'; });

it('reuses only the same publication and refreshes after a same-project reindex', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.append(host); const root = createRoot(host);
    dispose = async () => { await act(async () => root.unmount()); host.remove(); };
    state.load.mockImplementation(async () => ({ data: { nodes: [], edges: [], total_nodes: 0 }, roots: new Set(), depth: 1, exhausted: true }));
    function Probe({ file }: { file?: string }) {
        const scope = useGraphScope({ project: 'p', filePath: file });
        return <output>{scope.complete ? 'complete' : 'pending'}</output>;
    }
    await act(async () => root.render(<Probe file="src/a.ts" />));
    expect(state.load).toHaveBeenCalledTimes(1);
    await act(async () => root.render(<Probe />));
    await act(async () => root.render(<Probe file="src/a.ts" />));
    expect(state.load).toHaveBeenCalledTimes(1);
    await act(async () => root.render(<Probe />));
    state.generation = 'reindexed';
    await act(async () => root.render(<Probe file="src/a.ts" />));
    expect(state.load).toHaveBeenCalledTimes(2);
    expect(host.textContent).toBe('complete');
});

it('does not retain a complete result if publication changes during retrieval', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.append(host); const root = createRoot(host);
    dispose = async () => { await act(async () => root.unmount()); host.remove(); };
    state.load.mockImplementation(async () => {
        state.generation = 'changed';
        return { data: { nodes: [], edges: [], total_nodes: 0 }, roots: new Set(), depth: 1, exhausted: true };
    });
    function Probe() { const scope = useGraphScope({ project: 'p', filePath: 'a.ts' }); return <output>{scope.complete ? 'complete' : scope.error}</output>; }
    await act(async () => root.render(<Probe />));
    expect(host.textContent).toContain('index changed');
    expect(host.textContent).not.toBe('complete');
});

it('does not reuse a local result from another backend with the same publication name', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.append(host); const root = createRoot(host);
    dispose = async () => { await act(async () => root.unmount()); host.remove(); };
    state.load.mockImplementation(async () => ({ data: { nodes: [], edges: [], total_nodes: 0 }, roots: new Set(), depth: 1, exhausted: true }));
    function Probe({ transport }: { transport: typeof globalThis.fetch }) {
        const scope = useGraphScope({ project: 'p', filePath: 'src/a.ts', fetch: transport });
        return <output>{scope.complete ? 'complete' : 'pending'}</output>;
    }
    const first = vi.fn() as typeof globalThis.fetch, second = vi.fn() as typeof globalThis.fetch;
    await act(async () => root.render(<Probe transport={first} />));
    await act(async () => root.render(<Probe transport={second} />));
    expect(state.load).toHaveBeenCalledTimes(2);
    expect(state.load.mock.calls[1]![5].previous).toBeUndefined();
});

const graphNode = (id: number): GraphNode => ({ id, name: `n${id}`, qualified_name: `p.n${id}`, label: 'Function',
    file_path: id === 1 ? 'src/a.ts' : `src/${id}.ts`, x: 0, y: 0, z: 0, size: 3, color: '#abcdef' });
const graph: GraphData = { nodes: [1, 2, 3, 4, 5].map(graphNode), total_nodes: 5, edges: [
    { source: 1, target: 2, type: 'CALLS' }, { source: 1, target: 3, type: 'IMPORTS' },
    { source: 3, target: 4, type: 'CALLS' }, { source: 2, target: 5, type: 'CALLS' },
] };
const scoped = (ids: number[], depth = 1): ScopedGraph => ({ data: { nodes: ids.map(graphNode), edges: [], total_nodes: ids.length },
    roots: new Set([1]), depth, exhausted: false });

it('keys cached layers by normalized edge types and recomputes from roots at the current depth', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.append(host); const root = createRoot(host);
    dispose = async () => { await act(async () => root.unmount()); host.remove(); };
    state.load.mockImplementation(async (...args: unknown[]) => scoped([1], Number(args[2])));
    let latest: ReturnType<typeof useGraphScope> | undefined;
    function Probe({ types }: { types?: readonly string[] }) {
        const scope = useGraphScope({ project: 'p', filePath: 'src/a.ts', edgeTypes: types });
        latest = scope;
        return <output>{scope.complete ? 'complete' : 'pending'}</output>;
    }
    await act(async () => root.render(<Probe />));
    await act(async () => latest!.setDepth(2));
    expect(state.load).toHaveBeenCalledTimes(2);
    expect(state.load.mock.calls[1]![5].previous).toBeDefined();
    await act(async () => root.render(<Probe types={['IMPORTS', 'CALLS']} />));
    expect(state.load).toHaveBeenCalledTimes(3);
    expect(state.load.mock.calls[2]![2]).toBe(2);
    expect(state.load.mock.calls[2]![5].previous).toBeUndefined();
    expect(state.load.mock.calls[2]![5].edgeTypes).toEqual(['CALLS', 'IMPORTS']);
    await act(async () => root.render(<Probe types={['CALLS', 'CALLS', 'IMPORTS']} />));
    expect(state.load).toHaveBeenCalledTimes(3);
    await act(async () => root.render(<Probe types={[]} />));
    expect(state.load).toHaveBeenCalledTimes(4);
    expect(state.load.mock.calls[3]![5].edgeTypes).toEqual([]);
    await act(async () => root.render(<Probe />));
    expect(state.load).toHaveBeenCalledTimes(4); // Reuses only the all-types publication.
    expect(host.textContent).toBe('complete');
});

it('does not display excluded prior-layer nodes while the filtered request is pending', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.append(host); const root = createRoot(host);
    dispose = async () => { await act(async () => root.unmount()); host.remove(); };
    state.load.mockResolvedValueOnce(scoped([1, 2, 3]));
    let latest: ReturnType<typeof useGraphScope> | undefined;
    function Probe({ types }: { types?: readonly string[] }) {
        const scope = useGraphScope({ project: 'p', filePath: 'src/a.ts', layout: graph, edgeTypes: types });
        latest = scope;
        return <output>{scope.result?.data.nodes.map(node => node.id).join(',')}:{scope.complete ? 'complete' : 'pending'}</output>;
    }
    await act(async () => root.render(<Probe />));
    let finish: ((value: ScopedGraph) => void) | undefined;
    state.load.mockImplementation(() => new Promise<ScopedGraph>(resolve => { finish = resolve; }));
    await act(async () => latest!.setDepth(2));
    await act(async () => root.render(<Probe types={['CALLS']} />));
    expect(host.textContent).toBe('1,2,5:pending');
    expect(state.load.mock.calls.at(-1)![5].previous).toBeUndefined();
    await act(async () => finish!(scoped([1, 2, 5], 2)));
    expect(host.textContent).toBe('1,2,5:complete');
    await act(async () => root.render(<Probe types={[]} />));
    expect(host.textContent).toBe('1:pending');
});
