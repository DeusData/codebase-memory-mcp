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
    it('publishes the current call and path evidence, follows steps, and clears the background selection', async () => {
        const onSelectionEvidence = vi.fn();
        await render(fixture(), { targetId: 9, onSelectionEvidence });
        let snapshot = JSON.parse(onSelectionEvidence.mock.lastCall![0].text).evidence;
        expect(snapshot.selected.call).toMatchObject({ source_id: 1, target_id: 2, type: 'CALLS' });
        expect(snapshot.relationships.nodeCount).toBe(9);
        await click('Next →');
        snapshot = JSON.parse(onSelectionEvidence.mock.lastCall![0].text).evidence;
        expect(snapshot.selected.operation.id).toBe(2);
        expect(snapshot.scope.step).toBe(1);
        expect(snapshot.limitations.interpretation).toContain('not execution order');
        await click('Empty background');
        expect(onSelectionEvidence).toHaveBeenLastCalledWith(undefined);
    });
    it('keeps destination selection available without a separate search bar', async () => {
        const targets = Array.from({ length: 20 }, (_, index) => ({ ...symbol(index + 2), distance: 1 }));
        await render(fixture(), { targets });
        expect(container.querySelector('[aria-label="Find a destination"]')).toBeNull();
        expect(container.querySelectorAll('[aria-label="Behavior destination"] option')).toHaveLength(21);
    });
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
    it('double-clicking an operation requests its actual symbol as a hop from the start; Back is the workspace Back, not one of its own (K27)', async () => {
        const props = await render(fixture());
        const button = container.querySelector('[data-node="journey-choice:2"]')!;
        await act(async () => button.dispatchEvent(new MouseEvent('dblclick', { bubbles: true })));
        expect(props.onRequest).toHaveBeenLastCalledWith(expect.objectContaining({ id: 2 }), undefined, { from: expect.objectContaining({ id: 1 }) });
        expect([...container.querySelectorAll('button')].some(item => item.textContent === '← Back')).toBe(false);
        // A second hop still names the operation the hops started from.
        const next = fixture(); next.entrypoints = [next.paths[0].nodes[1]];
        next.paths = [{ ...next.paths[0], entrypoint_id: 2, nodes: next.paths[0].nodes.slice(1), edges: next.paths[0].edges.slice(1) }];
        await render(next, { ...props, data: next, entryId: 2, from: fixture().entrypoints[0] });
        await act(async () => container.querySelector('[data-node="journey-choice:3"]')!.dispatchEvent(new MouseEvent('dblclick', { bubbles: true })));
        expect(props.onRequest).toHaveBeenLastCalledWith(expect.objectContaining({ id: 3 }), undefined, { from: expect.objectContaining({ id: 1 }) });
        // A start picked in the field begins anew.
        await act(async () => { const select = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!; select.value = '1'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(props.onRequest).toHaveBeenLastCalledWith(expect.objectContaining({ id: 1 }), undefined, {});
    });
    it('clears inspection and returns from a followed operation to the original entry on empty background', async () => {
        const clear = vi.fn(); const props = await render(fixture(), { onClearSelection: clear });
        const next = fixture(); next.entrypoints = [next.paths[0].nodes[1]];
        next.paths = [{ ...next.paths[0], entrypoint_id: 2, nodes: next.paths[0].nodes.slice(1), edges: next.paths[0].edges.slice(1) }];
        await render(next, { ...props, data: next, entryId: 2, from: fixture().entrypoints[0] });
        await click('Empty background');
        expect(props.onRequest).toHaveBeenLastCalledWith(expect.objectContaining({ id: 1 }), undefined, {});
        expect(container.querySelector('[data-scene]')?.getAttribute('data-selected')).toBeNull();
        expect(container.querySelector('[aria-label="Behavior evidence inspector"]')?.textContent).toContain('Select an operation or a call');
        expect(clear).toHaveBeenCalledOnce();
    });
    it('reads where it stands and its camera from a lifted place and reports changes there (K27)', async () => {
        const onPlace = vi.fn();
        const props = await render(fixture(), { targetId: 9, place: { step: 3, planar: true }, onPlace });
        expect(container.querySelector('[aria-label="Walk the call chain"] span')?.textContent).toBe('4 / 9');
        expect(container.querySelector('[aria-label="Behavior camera"] button[aria-pressed="true"]')?.textContent).toBe('Plan');
        await click('Next →');
        expect(onPlace).toHaveBeenLastCalledWith({ step: 4 }, undefined);
        await click('3D');
        expect(onPlace).toHaveBeenLastCalledWith({ planar: false }, undefined);
        // The lifted place stays what it is handed: a new start brings its own, and Back brings the one it had.
        onPlace.mockClear();
        await render(fixture(), { ...props, targetId: undefined, place: { step: 3, planar: true }, onPlace });
        expect(onPlace).not.toHaveBeenCalled();
    });
    it('keeps its own place when rendered on its own, and starts the chain over for another destination', async () => {
        const props = await render(fixture(), { targetId: 9 });
        await click('Next →'); await click('Next →'); await click('Plan');
        expect(container.querySelector('[aria-label="Walk the call chain"] span')?.textContent).toBe('3 / 9');
        await render(fixture(), { ...props, targetId: undefined });
        await render(fixture(), { ...props, targetId: 9 });
        expect(container.querySelector('[aria-label="Walk the call chain"] span')?.textContent).toBe('1 / 9');
        expect(container.querySelector('[aria-label="Behavior camera"] button[aria-pressed="true"]')?.textContent).toBe('Plan');
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
    it('pages through every direct callee the heading counts and says what lies beyond the drawing limit', async () => {
        const data = fixture();
        const callees = Array.from({ length: 14 }, (_, index) => symbol(index + 20));
        data.paths = callees.map((target, index) => ({ entrypoint_id: 1, nodes: [data.entrypoints[0], target], edges: [{ id: 500 + index, source_id: 1, target_id: target.id, type: 'CALLS' }] }));
        await render(data);
        expect(container.querySelector('.behavior-navigation')?.textContent).toContain('14 direct callees with returned evidence');
        expect(container.querySelector('[aria-label="Direct call pages"]')?.textContent).toContain('Calls 1 to 4 of 14');
        for (let page = 0; page < 3; page++) await click('More calls →');
        expect(container.querySelector('[aria-label="Direct call pages"]')?.textContent).toContain('Calls 13 to 14 of 14');
        // Every page draws its calls and the start beside them.
        expect(container.querySelectorAll('[data-node]')).toHaveLength(3);
    });
    it('names a start operation that calls itself instead of counting it as a callee or claiming a filter', async () => {
        const data = fixture();
        const callees = Array.from({ length: 6 }, (_, index) => symbol(index + 20));
        data.paths = [...callees.map((target, index) => ({ entrypoint_id: 1, nodes: [data.entrypoints[0], target], edges: [{ id: 500 + index, source_id: 1, target_id: target.id, type: 'CALLS' }] })),
            { entrypoint_id: 1, nodes: [data.entrypoints[0], data.entrypoints[0]], edges: [{ id: 599, source_id: 1, target_id: 1, type: 'CALLS' }] }];
        await render(data);
        expect(container.querySelector('.behavior-navigation span')?.textContent).toBe('6 direct callees with returned evidence · also calls itself');
        const pages = container.querySelector('[aria-label="Direct call pages"]')?.textContent ?? '';
        expect(pages).toContain('Calls 1 to 4 of 6');
        expect(pages).not.toContain('matching the filter');
    });
    it('shows the operation the journey opened with in the Start field, not "Choose an operation"', async () => {
        // Without a requested entry the journey falls back to the first entry point (Django: main in manage.py-tpl).
        await render(fixture(), { entryId: undefined });
        const start = container.querySelector<HTMLSelectElement>('[aria-label="Behavior entry point"]')!;
        expect(container.querySelector('.behavior-heading h2')?.textContent).toBe('What can operation1 call?');
        expect(start.value).toBe('1');
        expect(start.selectedOptions[0]?.textContent).toBe('operation1 · src/part1.ts');
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

/* Hand test 2026-10-04 (A4): the "Start" list had no recognizable order, and "handle · …/loaddata.py" could not be found. */
describe('Behavior start field', () => {
    const LOADDATA = 'django/core/management/commands/loaddata.py';
    const entries = [symbol(1), { ...symbol(21), name: 'handle', file_path: 'django/core/management/commands/makemigrations.py' },
        { ...symbol(22), name: 'handle', file_path: LOADDATA }, { ...symbol(23), name: 'as_sql', file_path: 'django/db/models/fields/json.py' }];
    const select = () => container.querySelector<HTMLSelectElement>('select[aria-label="Behavior entry point"]')!;
    const filterField = () => container.querySelector<HTMLInputElement>('input[aria-label="Filter operations"]')!;
    const group = (index: number) => [...select().querySelectorAll('optgroup')][index];
    const options = (index: number) => [...group(index).querySelectorAll('option')].map(option => option.textContent);
    async function type(value: string) {
        await act(async () => {
            Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(filterField(), value);
            filterField().dispatchEvent(new Event('input', { bubbles: true }));
        });
    }

    it('A4: offers the suggestions first, then every operation alphabetically, behind a filter field', async () => {
        await render(fixture(), { entries, suggestedEntries: [1, 22] });
        expect(filterField().placeholder).toBe('Filter operations…');
        expect(filterField().compareDocumentPosition(select()) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
        expect([...select().querySelectorAll('optgroup')].map(item => item.label)).toEqual(['Suggested', 'All operations · 4']);
        expect(options(0)).toEqual(['operation1 · src/part1.ts', `handle · ${LOADDATA}`]);
        expect(options(1)).toEqual(['as_sql · django/db/models/fields/json.py', `handle · ${LOADDATA}`,
            'handle · django/core/management/commands/makemigrations.py', 'operation1 · src/part1.ts']);
        expect(select().value).toBe('1');
        expect(select().selectedOptions[0]?.textContent).toBe('operation1 · src/part1.ts');
    });

    it('A4: narrows the list to what the filter names, keeps the chosen start, and starts the operation picked there', async () => {
        const props = await render(fixture(), { entries, suggestedEntries: [1, 21] });
        await type('loaddata');
        expect(group(1).label).toBe('Matching operations · 1 of 4');
        expect(options(1)).toEqual([`handle · ${LOADDATA}`]);
        // The chosen start stands apart while the filter does not match it (review of K43).
        expect(group(0).label).toBe('Current start, not matching the filter');
        expect(options(0)).toEqual(['operation1 · src/part1.ts']);
        expect(select().value).toBe('1');
        await act(async () => { select().value = '22'; select().dispatchEvent(new Event('change', { bubbles: true })); });
        expect(props.onRequest).toHaveBeenLastCalledWith(expect.objectContaining({ id: 22, file_path: LOADDATA }), undefined, {});
        await type('no such operation');
        expect(options(0)).toEqual(['operation1 · src/part1.ts']);
        expect(options(1)).toEqual(['No operation matches "no such operation"']);
        expect(group(1).querySelector('option:last-child')?.hasAttribute('disabled')).toBe(true);
    });
});
