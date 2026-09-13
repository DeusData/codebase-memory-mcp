// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import BehaviorJourney, { journeyPage, type BehaviorJourneyProps } from './BehaviorJourney';
import { behaviorJourney } from './behavior-journey-model';
import type { SystemSceneModel } from './system-architecture-model';
import type { SystemProjection, SystemSymbol } from './system-architecture-source';

vi.mock('./BehaviorSourceEvidence', () => ({ default: () => <div data-source-evidence /> }));
vi.mock('./SystemArchitectureScene', () => ({ default: ({ model, onSelectNode, onExpandNode, onClearSelection, selectedNode, presentation }: {
    model: SystemSceneModel; onSelectNode: (id: string) => void; onExpandNode: (id: string) => void; presentation: string; onClearSelection?: () => void; selectedNode?: string;
}) => <div data-scene={presentation} data-selected={selectedNode}><button onClick={onClearSelection}>Empty background</button>{model.nodes.map(node => <button key={node.id} data-node={node.id} onClick={() => onSelectNode(node.id)} onDoubleClick={() => onExpandNode(node.id)}>{node.label}</button>)}
    {model.edges.map(edge => <span key={edge.id} data-edge={edge.pathEdge?.id} />)}</div> }));
const symbol = (id: number): SystemSymbol => ({ id, name: `operation${id}`, qualified_name: `sample.operation${id}`, label: 'Function',
    component_id: `component${id % 2}`, file_path: `src/part${id % 2}.ts`, start_line: id });
function fixture(): SystemProjection {
    const nodes = Array.from({ length: 9 }, (_, index) => symbol(index + 1));
    return { schema_version: 1, status: 'ready', kind: 'static_projection', complete: true, components: [], dependencies: [], cycles: [],
        entrypoints: [nodes[0]], paths: [{ entrypoint_id: 1, nodes, edges: nodes.slice(1).map((node, index) => ({
            id: 10 + index, source_id: nodes[index].id, target_id: node.id, type: 'CALLS', callsite: { file_path: nodes[index].file_path!, line: 20 + index },
        })) }], totals: {}, limits: {}, warnings: [] };
}
let container: HTMLDivElement, root: Root;
beforeEach(() => { (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true; container = document.createElement('div'); document.body.append(container); root = createRoot(container); });
afterEach(async () => { await act(async () => root.unmount()); container.remove(); });
async function render(data: SystemProjection, changes: Partial<BehaviorJourneyProps> = {}) {
    const props: BehaviorJourneyProps = { project: 'sample', generation: 'g1', data, entries: data.entrypoints,
        targets: data.paths[0].nodes.slice(1).map((node, index) => ({ ...node, distance: index + 1 })), entryId: 1,
        active: true, pending: false, filter: '', onRequest: vi.fn(), onRefresh: vi.fn(), onNavigate: vi.fn(), onSelectSymbol: vi.fn(), ...changes };
    await act(async () => root.render(<BehaviorJourney {...props} />)); return props;
}
async function click(text: string) { const button = [...container.querySelectorAll('button')].find(button => button.textContent === text); expect(button).toBeDefined(); await act(async () => button!.click()); }

describe('behavior journeys', () => {
    it('opens with unordered immediate calls rather than the system overview or a fabricated full journey', async () => {
        await render(fixture());
        expect(container.querySelector('[data-scene]')?.getAttribute('data-scene')).toBe('journey');
        expect(container.querySelectorAll('[data-node]')).toHaveLength(2);
        expect(container.querySelectorAll('[data-edge]')).toHaveLength(1);
        expect(container.textContent).toContain('DIRECT CALLS · UNORDERED');
        expect(container.querySelector('[aria-label="Walk the call chain"]')).toBeNull();
    });
    it('walks a long path through readable windows while retaining access to every operation', async () => {
        await render(fixture(), { targetId: 9 });
        expect(container.querySelectorAll('[data-node]')).toHaveLength(5);
        expect(container.querySelectorAll('[aria-label="All operations in this call chain"] li')).toHaveLength(9);
        for (let i = 0; i < 5; i++) await click('Next →');
        expect(container.textContent).toContain('Showing operations 5 to 9 of 9');
        expect([...container.querySelectorAll('[data-edge]')].map(item => item.getAttribute('data-edge'))).toEqual(['14', '15', '16', '17']);
        expect(container.querySelector('[aria-label="Call-chain position"]')?.getAttribute('value')).toBe('5');
    });
    it('double-clicking an operation requests its actual symbol and Back restores the previous scope', async () => {
        const props = await render(fixture());
        const button = container.querySelector('[data-node="journey-choice:2"]')!;
        await act(async () => button.dispatchEvent(new MouseEvent('dblclick', { bubbles: true })));
        expect(props.onRequest).toHaveBeenLastCalledWith(expect.objectContaining({ id: 2 }), undefined);
        await click('← Back');
        expect(props.onRequest).toHaveBeenLastCalledWith(expect.objectContaining({ id: 1 }), undefined);
    });
    it('clears inspection and returns from a followed operation to the original entry on empty background', async () => {
        const clear = vi.fn(); const props = await render(fixture(), { onClearSelection: clear });
        await act(async () => container.querySelector('[data-node="journey-choice:2"]')!.dispatchEvent(new MouseEvent('dblclick', { bubbles: true })));
        const next = fixture(); next.entrypoints = [next.paths[0].nodes[1]];
        next.paths = [{ ...next.paths[0], entrypoint_id: 2, nodes: next.paths[0].nodes.slice(1), edges: next.paths[0].edges.slice(1) }];
        await render(next, { ...props, data: next, entryId: 2 });
        await click('Empty background');
        expect(props.onRequest).toHaveBeenLastCalledWith(expect.objectContaining({ id: 1 }));
        expect(container.querySelector('[data-scene]')?.getAttribute('data-selected')).toBeNull();
        expect(container.querySelector('[aria-label="Behavior evidence inspector"]')?.textContent).toContain('Select an operation or a call');
        expect(clear).toHaveBeenCalledOnce();
        expect([...container.querySelectorAll('button')].find(item => item.textContent === '← Back')?.disabled).toBe(true);
    });
    it('shows exact argument expressions and declaration facts without inventing argument-to-parameter bindings', async () => {
        const data = fixture();
        data.paths[0].nodes[1].signature = 'operation2(options: Options): Result';
        data.paths[0].nodes[1].parameters = { names: ['options'], types: ['Options'], count: 1 };
        data.paths[0].nodes[1].return_type = 'Result';
        Object.assign(data.paths[0].edges[0], { arguments: [{ i: 0, e: '...settings' }], argument_limit: 8, arguments_complete: false });
        await render(data, { targetId: 9 });
        const contract = container.querySelector('[aria-label="Indexed call contract"]')!;
        expect(contract.textContent).toContain('...settings'); expect(contract.textContent).toContain('options: Options');
        expect(contract.textContent).toContain('Declared return typeResult');
        expect(contract.textContent).toContain('Parameter binding is not established.');
    });
    it('never keeps an old scene under a pending or failed new request', async () => {
        const data = fixture(); await render(data, { targetId: 9 }); expect(container.querySelector('[data-scene]')).not.toBeNull();
        await render(data, { data: undefined, pending: true, targetId: 8 });
        expect(container.querySelector('[data-scene]')).toBeNull(); expect(container.textContent).toContain('Preparing the selected operation');
        await render(data, { data: undefined, pending: false, error: 'Snapshot changed.' });
        expect(container.querySelector('[data-scene]')).toBeNull(); expect(container.textContent).toContain('Snapshot changed.');
    });
    it('pages only real edges and component lanes within the selected path window', () => {
        const scene = behaviorJourney(fixture(), { entryId: 1, targetId: 9 }).scene;
        const page = journeyPage(scene, 6), ids = new Set(page.nodes.map(node => node.id));
        expect(page.nodes).toHaveLength(5); expect(page.edges).toHaveLength(4);
        expect(page.edges.every(edge => ids.has(edge.source) && ids.has(edge.target))).toBe(true);
        expect(page.edges.map(edge => edge.pathEdge!.id)).toEqual([14, 15, 16, 17]);
        expect(scene.nodes).toHaveLength(9);
        const compact = journeyPage(scene, 5, false, 3);
        expect(compact.nodes.map(node => node.symbol!.id)).toEqual([5, 6, 7]);
        expect(compact.edges.map(edge => edge.pathEdge!.id)).toEqual([14, 15]);
    });
});
