// @vitest-environment jsdom
/*
 * Review-Befund G1: der Canvas wurde bei jedem Scope-Schritt neu aufgebaut und
 * die Kamera bei jedem Bild neu eingepasst. Hier steht die Szene als Attrappe,
 * die mitzaehlt, wie oft sie auf- und abgebaut wird; die Beziehungen kommen
 * ueber dieselbe RPC-Strecke wie im Betrieb.
 */
import { act, useEffect, useLayoutEffect } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel from './GalaxyPanel';
import type { GraphData, GraphEdge, GraphNode } from './types';
import type { ScenePath } from './PathLayer';
import { GALAXY_LEGEND_KEY } from './galaxy-legend';

const scene = vi.hoisted(() => ({ mounts: 0, unmounts: 0, renders: 0, roots: undefined as ReadonlySet<number> | undefined,
    highlighted: null as Set<number> | null, path: undefined as ScenePath | undefined,
    /** Every committed picture: what the scene draws and what the edge type filter lists beside it. */
    frames: [] as { nodes: number; types: string[]; traceRows: number }[] }));
vi.mock('./GraphScene', async importOriginal => ({
    ...await importOriginal<typeof import('./GraphScene')>(),
    GraphScene: ({ data, rootIds, highlightedIds, path }: { data: GraphData; rootIds?: ReadonlySet<number>; highlightedIds: Set<number> | null; path?: ScenePath }) => {
        scene.renders += 1; scene.roots = rootIds; scene.highlighted = highlightedIds; scene.path = path;
        useEffect(() => { scene.mounts += 1; return () => { scene.unmounts += 1; }; }, []);
        useLayoutEffect(() => {
            scene.frames.push({ nodes: data.nodes.length, types: [...new Set(data.edges.map(edge => edge.type))].sort(),
                traceRows: document.querySelectorAll('.atlas-trace-edge-row').length });
        });
        return <output data-testid="scene-nodes">{data.nodes.length}</output>;
    },
}));

const node = (id: number): GraphNode => ({ id, name: `n${id}`, qualified_name: `sample.n${id}`, label: 'Function',
    file_path: `src/n${id}.ts`, start_line: 1, end_line: 5, x: id * 10, y: 0, z: 0, size: 2, color: '#999999' });
const nodes = [1, 2, 3, 4, 5].map(node);
const edges: GraphEdge[] = [
    { source: 1, target: 2, type: 'CALLS', line: 3 }, { source: 3, target: 1, type: 'CALLS', line: 4 },
    { source: 2, target: 4, type: 'CALLS', line: 2 }, { source: 4, target: 5, type: 'IMPORTS' },
    { source: 1, target: 5, type: 'CALLS', line: 1 },
];

/** /api/layout plus the two query_graph shapes the scope loader sends. */
function graphFetch() {
    return vi.fn(async (url: RequestInfo | URL, init?: RequestInit) => {
        if (String(url).includes('/api/layout')) return new Response(JSON.stringify({ nodes, edges, total_nodes: nodes.length }));
        const request = JSON.parse(String(init?.body)) as { params: { name: string; arguments: { query: string } } };
        if (request.params.name === 'index_status') return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { content: [{ type: 'text', text: JSON.stringify({ indexed_at: 'generation-1' }) }] } }));
        const query = request.params.arguments.query;
        const columns = (prefix: string) => ['id', 'label', 'name', 'qn', 'file', 'start_line', 'end_line'].map(key => prefix + key);
        const values = (entry: GraphNode) => [entry.id, entry.label, entry.name, entry.qualified_name ?? '', entry.file_path ?? '', entry.start_line ?? '', entry.end_line ?? ''].map(String);
        const names = [...query.matchAll(/qualified_name = "([^"]+)"/g)].map(match => match[1]);
        let cols: string[], rows: string[][];
        if (query.startsWith('MATCH (n)')) {
            cols = columns(''); rows = nodes.filter(entry => names.includes(entry.qualified_name ?? '')).map(values);
        } else {
            cols = ['edge_id', 'edge_type', 'edge_line', ...columns('a_'), ...columns('b_')];
            rows = edges.flatMap((edge, i) => {
                const a = nodes.find(entry => entry.id === edge.source)!, b = nodes.find(entry => entry.id === edge.target)!;
                return names.includes(a.qualified_name ?? '') || names.includes(b.qualified_name ?? '')
                    ? [[String(i + 1), edge.type, String(edge.line ?? ''), ...values(a), ...values(b)]] : [];
            });
        }
        const text = `rows: ${rows.length} (cols: ${cols.join(' ')})\n${rows.map(row => '  ' + row.map(value => JSON.stringify(value || '-')).join(' ')).join('\n')}\ntotal: ${rows.length}\nhas_more: false\ntruncated: false`;
        return new Response(JSON.stringify({ jsonrpc: '2.0', id: 1, result: { content: [{ type: 'text', text }] } }));
    });
}

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    scene.mounts = 0; scene.unmounts = 0; scene.renders = 0; scene.frames = [];
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); globalThis.__atlasGalaxy = undefined; });

const seam = () => globalThis.__atlasGalaxy!;
const settle = (check: () => void) => vi.waitFor(async () => {
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
    check();
});

it('keeps one scene mounted through selection and expansion and fits once per selected root', async () => {
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={graphFetch()} />));
    await settle(() => expect(seam().nodes).toBe(5));
    expect(scene.mounts).toBe(1);
    const fitsBefore = seam().fits;
    // The whole graph keeps its render limits in the toolbar row.
    expect(host.querySelector('.atlas-graph-limits')).toBeNull();
    expect(host.querySelector('select[aria-label="Rendered node limit"]')).not.toBeNull();

    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => {
        expect(seam().nodes).toBe(4);
        expect(host.querySelector('.atlas-graph-scope-count')?.textContent).toBe('4 nodes · 3 edges');
    });
    expect(scene.mounts).toBe(1); expect(scene.unmounts).toBe(0);
    expect(seam().fits).toBe(fitsBefore + 1);
    // In a scope the limits step back into a compact menu so the row does not wrap.
    expect(host.querySelector('details.atlas-graph-limits select[aria-label="Rendered node limit"]')).not.toBeNull();
    expect(host.querySelector('details.atlas-graph-limits select[aria-label="Rendered edge limit"]')).not.toBeNull();
    // The root is marked, sits at the origin and the fit is centred on it.
    expect([...scene.roots ?? []]).toEqual([1]);
    expect(seam().lastFit?.center?.map(value => value + 0)).toEqual([0, 0, 0]);

    const expand = [...host.querySelectorAll('button')].find(button => button.textContent === 'Expand +1')!;
    await act(async () => expand.click());
    await settle(() => expect(seam().nodes).toBe(5));
    expect(scene.mounts).toBe(1); expect(scene.unmounts).toBe(0);
    // An expansion is the same scope: no new fit, the scene keeps the reader's camera.
    expect(seam().fits).toBe(fitsBefore + 1);

    await act(async () => { seam().clickNode('sample.n4'); });
    await settle(() => expect(seam().lastFit?.nodes).toBe(3));
    expect(scene.mounts).toBe(1);
    expect(seam().fits).toBe(fitsBefore + 2);
});

const button = (label: string) => [...host.querySelectorAll('button')].find(entry => entry.textContent === label)!;
const steps = () => [...host.querySelectorAll('[data-testid="atlas-galaxy-path-panel"] li code')].map(entry => entry.textContent);
const heading = () => host.querySelector('[data-testid="atlas-galaxy-path-panel"] strong')?.textContent;

it('highlights a path and the call order of the root inside the loaded scope, and clears with Escape or a new trace', async () => {
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={graphFetch()} />));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(seam().nodes).toBe(4));
    await act(async () => button('Expand +1').click());
    await settle(() => expect(seam().nodes).toBe(5));

    await act(async () => button('Call order').click());
    expect(heading()).toBe('Calls of n1 · 2 calls');
    expect(steps()).toEqual(['n1 --CALLS--> n5', 'n1 --CALLS--> n2']);
    expect([...scene.highlighted ?? []].sort()).toEqual([1, 2, 5]);
    expect(scene.path).toMatchObject({ active: 0, labels: 'active' });
    await act(async () => button('Next').click());
    expect(scene.path?.active).toBe(1);

    host.querySelector<HTMLDetailsElement>('.atlas-graph-path-picker')!.open = true;
    const target = [...host.querySelectorAll<HTMLButtonElement>('.atlas-graph-path-menu li button')]
        .find(entry => entry.querySelector('strong')?.textContent === 'n4')!;
    await act(async () => target.click());
    expect(heading()).toBe('Path to n4 · 2 hops');
    expect(steps()).toEqual(['n1 --CALLS--> n2', 'n2 --CALLS--> n4']);
    expect([...scene.highlighted ?? []].sort()).toEqual([1, 2, 4]);
    expect(scene.path).toMatchObject({ active: 0, labels: 'all' });
    expect(seam().nodes).toBe(5);

    await act(async () => { window.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape' })); });
    expect(host.querySelector('[data-testid="atlas-galaxy-path-panel"]')).toBeNull();
    expect(scene.path).toBeUndefined();

    // Incoming: the path walks the CALLS edge backwards and says so.
    const direction = (value: string) => act(async () => {
        const select = host.querySelector<HTMLSelectElement>('select[aria-label="Trace direction"]')!;
        select.value = value; select.dispatchEvent(new Event('change', { bubbles: true }));
    });
    await direction('inbound');
    await settle(() => expect(seam().nodes).toBe(2));
    host.querySelector<HTMLDetailsElement>('.atlas-graph-path-picker')!.open = true;
    const caller = [...host.querySelectorAll<HTMLButtonElement>('.atlas-graph-path-menu li button')]
        .find(entry => entry.querySelector('strong')?.textContent === 'n3')!;
    await act(async () => caller.click());
    expect(heading()).toBe('Path to n3 · 1 hop');
    expect(steps()).toEqual(['n1 <--CALLS-- n3']);
    // A new trace direction is a new scope: the path view returns to the normal picture.
    await direction('outbound');
    expect(host.querySelector('[data-testid="atlas-galaxy-path-panel"]')).toBeNull();
});

const panel = () => host.querySelector('[data-testid="atlas-galaxy-path-panel"]');
const traceDirection = (value: string) => act(async () => {
    const select = host.querySelector<HTMLSelectElement>('select[aria-label="Trace direction"]')!;
    select.value = value; select.dispatchEvent(new Event('change', { bubbles: true }));
});
const pickPath = (name: string) => act(async () => {
    host.querySelector<HTMLDetailsElement>('.atlas-graph-path-picker')!.open = true;
    [...host.querySelectorAll<HTMLButtonElement>('.atlas-graph-path-menu li button')]
        .find(entry => entry.querySelector('strong')?.textContent === name)!.click();
});

it('drops a path on a new scope key, so returning to the old key does not bring it back', async () => {
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={graphFetch()} />));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(seam().nodes).toBe(4));
    await pickPath('n2');
    expect(heading()).toBe('Path to n2 · 1 hop');

    // Incoming and back to both directions: the same key as before, but the path is gone.
    await traceDirection('inbound');
    await settle(() => expect(seam().nodes).toBe(2));
    expect(panel()).toBeNull();
    await traceDirection('both');
    await settle(() => expect(seam().nodes).toBe(4));
    expect(panel()).toBeNull();
    expect(scene.path).toBeUndefined();

    // The whole graph and the same root again: a fresh picture without the old path.
    await act(async () => button('Call order').click());
    expect(heading()).toBe('Calls of n1 · 2 calls');
    await act(async () => button('All graph').click());
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(seam().nodes).toBe(4));
    expect(panel()).toBeNull();
    expect(button('Call order').getAttribute('aria-pressed')).toBe('false');
});

it('leaves Escape to an overlay above the galaxy and to a field being typed in', async () => {
    const fetch = graphFetch();
    const panelWith = (escapeTaken: boolean) => <GalaxyPanel project="sample" visible workspaceExpanded escapeTaken={escapeTaken} onOpenNode={vi.fn()} fetch={fetch} />;
    await act(async () => root.render(panelWith(false)));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(seam().nodes).toBe(4));
    await pickPath('n2');
    expect(panel()).not.toBeNull();
    const escape = (target: EventTarget) => {
        const event = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true });
        act(() => { target.dispatchEvent(event); });
        return event;
    };

    // Help, settings or projects lie above: the key is theirs.
    await act(async () => root.render(panelWith(true)));
    expect(escape(window).defaultPrevented).toBe(false);
    expect(panel()).not.toBeNull();

    // Typing in a field: Escape leaves the field, not the path.
    await act(async () => root.render(panelWith(false)));
    const field = host.querySelector<HTMLInputElement>('input[aria-label="Find a graph node"]')!;
    expect(escape(field).defaultPrevented).toBe(false);
    expect(panel()).not.toBeNull();

    expect(escape(window).defaultPrevented).toBe(true);
    expect(panel()).toBeNull();
});

it('keeps the picked name when a removed layer takes the path target out of the picture', async () => {
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={graphFetch()} />));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(seam().nodes).toBe(4));
    await act(async () => button('Expand +1').click());
    await settle(() => expect(seam().nodes).toBe(5));
    await pickPath('n4');
    expect(heading()).toBe('Path to n4 · 2 hops');

    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Remove graph layer"]')!.click());
    await settle(() => expect(seam().nodes).toBe(4));
    expect(heading()).toBe('Path to n4 · 0 hops');
    expect(panel()?.textContent).toContain('No path to n4 over the loaded relationships');
    expect(panel()?.textContent).not.toContain('#4');
});

it('shows the whole graph with its hidden edge kinds until the first scoped picture, and lists only current kinds', async () => {
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={graphFetch()}
        legendStore={{ getItem: (key: string) => (key === GALAXY_LEGEND_KEY ? 'open' : null), setItem: vi.fn() } as unknown as Storage} />));
    await settle(() => expect(seam().nodes).toBe(5));
    await act(async () => host.querySelector<HTMLButtonElement>('[data-testid="atlas-galaxy-legend-swatch"][data-type="CALLS"]')!.click());
    expect(seam().hiddenKinds).toEqual(['CALLS']);
    expect(seam().drawnEdges).toBe(1);

    scene.frames = [];
    await act(async () => { seam().clickNode('sample.n1'); });
    await settle(() => expect(seam().nodes).toBe(4));
    const placeholder = scene.frames.filter(frame => frame.nodes === 5);
    expect(placeholder.length).toBeGreaterThan(0);
    // The whole graph that stays on screen keeps the reader's choice ...
    expect(placeholder.every(frame => frame.types.join() === 'IMPORTS')).toBe(true);
    // ... and the trace filter beside it never offers the whole graph's kinds as the scope's.
    expect(placeholder.every(frame => frame.traceRows === 0)).toBe(true);
    expect(scene.frames.at(-1)).toMatchObject({ nodes: 4, types: ['CALLS'], traceRows: 1 });
});
