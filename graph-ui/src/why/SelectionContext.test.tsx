// @vitest-environment jsdom
/*
 * Handtest K13: in Galaxy nennt "Selection details" die Beziehungen aus dem
 * geladenen Ausschnitt und sagt, wenn er unvollstaendig ist. Bis dahin las das
 * Panel den gekappten Repository-Schnappschuss und zeigte bei JSONBAgg nur
 * "Outgoing relationships · 2".
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import SelectionContextPanel from './SelectionContext';
import type { GraphData, GraphNode } from '../galaxy/types';

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

const node = (id: number, extra: Partial<GraphNode> = {}): GraphNode => ({ id, name: `s${id}`, qualified_name: `p.s${id}`, file_path: `src/${id}.py`,
    label: 'Function', x: 0, y: 0, z: 0, size: 1, color: '#fff', ...extra });
const selected = node(1, { name: 'JSONBAgg', qualified_name: 'p.JSONBAgg', file_path: 'general.py', label: 'Class' });
const tests = Array.from({ length: 11 }, (_, at) => node(10 + at));
const scopeGraph: GraphData = { total_nodes: 15, nodes: [selected, ...tests, node(30, { label: 'Module', file_path: 'general.py' }), node(40), node(41)], edges: [
    ...tests.flatMap(test => [{ source: test.id, target: 1, type: 'CALLS', line: 5 }, { source: test.id, target: 1, type: 'TESTS' }]),
    { source: 30, target: 1, type: 'DEFINES' }, { source: 1, target: 40, type: 'INHERITS' }, { source: 1, target: 41, type: 'INHERITS' }] };
// The capped repository snapshot knows the class and its bases, but not the tests behind the 20,000-node cap.
const snapshot: GraphData = { total_nodes: 3, nodes: [selected, node(40), node(41)], edges: scopeGraph.edges.filter(edge => edge.type === 'INHERITS') };
const summaries = () => [...host.querySelectorAll('summary')].map(entry => entry.textContent);

it('K13: lists incoming and outgoing relationships of the loaded scope by type', async () => {
    await act(async () => root.render(<SelectionContextPanel graph={snapshot} selected={selected} path="general.py" onNavigate={vi.fn()}
        scope={{ graph: scopeGraph, complete: true, direction: 'both' }} />));
    expect(summaries()).toContain('Incoming relationships · 23 (CALLS 11 · TESTS 11 · DEFINES 1)');
    expect(summaries()).toContain('Outgoing relationships · 2 (INHERITS 2)');
    expect(host.textContent).toContain('From the loaded Galaxy scope');
});

it('K13: says when the scope is partial, still loading or traced in one direction or for some types only', async () => {
    const render = (scope: Parameters<typeof SelectionContextPanel>[0]['scope']) => act(async () => root.render(
        <SelectionContextPanel graph={snapshot} selected={selected} path="general.py" onNavigate={vi.fn()} scope={scope} />));
    await render({ graph: scopeGraph, complete: false, direction: 'both' });
    expect(host.textContent).toContain('The scope is still loading');
    await render({ graph: scopeGraph, complete: true, direction: 'both', partial: { layer: 3, nodes: 9139, edges: 43010, limit: 'nodes' } });
    expect(host.textContent).toContain('Layer 3 of this scope stopped at the render limit');
    await render({ graph: scopeGraph, complete: true, direction: 'outbound' });
    expect(host.textContent).toContain('Incoming relationships are not traced in this scope');
    await render({ graph: scopeGraph, complete: true, direction: 'both', edgeTypes: ['CALLS'] });
    expect(host.textContent).toContain('Only these relationship types are traced: CALLS');
});

it('K13: without a scope that contains the selection, it keeps the repository snapshot and says so', async () => {
    await act(async () => root.render(<SelectionContextPanel graph={snapshot} selected={selected} path="general.py" onNavigate={vi.fn()}
        scope={{ graph: { total_nodes: 1, nodes: [node(99)], edges: [] }, complete: true, direction: 'both' }} />));
    expect(summaries()).toContain('Outgoing relationships · 2 (INHERITS 2)');
    expect(host.textContent).toContain('From the repository map snapshot');
});
