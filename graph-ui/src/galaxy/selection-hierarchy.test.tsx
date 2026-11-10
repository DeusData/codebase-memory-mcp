// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { hierarchySymbolOf, loadSelectionHierarchy, useSelectionHierarchy } from './selection-hierarchy';
import type { ClosureSource } from '../provider/closure';
import type { SymbolRef } from '../core/focus-protocol';
import type { SymbolFacts } from '../core/intelligence-provider';
import { projectHierarchy } from './hierarchy-layout';

const rootSymbol = (name = 'selected'): SymbolRef => ({ name, qualifiedName: `project.${name}`, kind: 'function', uri: 'file:///workspace/file.ts', range: { start: { line: 0, character: 0 }, end: { line: 5, character: 0 } } });
const noCalls: SymbolFacts = { callees: { value: [], state: 'known', evidence: [] } };
const source = (): ClosureSource => ({ getFacts: vi.fn().mockResolvedValue(noCalls), resolveSymbolAt: vi.fn().mockResolvedValue({ kind: 'not-found' }) });

describe('selected hierarchy', () => {
    it('uses the selected graph identity without requiring a reader or source file', async () => {
        const target = hierarchySymbolOf({ id: 77, name: 'start', qualified_name: 'p.start', label: 'Function', x: 0, y: 0, z: 0, size: 2, color: '#fff' }, 'p')!;
        expect(target).toMatchObject({ nodeId: 'p.start', qualifiedName: 'p.start', projectName: 'p', uri: '' });
        const api = source();
        const result = await loadSelectionHierarchy(api, '/workspace', target, { projectName: 'p', depth: 2, cap: 5 });
        expect(api.getFacts).toHaveBeenCalledWith('/workspace', target, ['callees'], { projectName: 'p' });
        expect(result.status).toBe('ready');
    });
    it('renders a valid zero-outgoing-call symbol as a single hierarchy root', async () => {
        const target = rootSymbol();
        const result = await loadSelectionHierarchy(source(), '/workspace', target, { depth: 2, cap: 5 });
        expect(result.walk?.nodes).toEqual([{ symbol: target, hop: 0 }]);
        expect(result.message).toContain('No outgoing calls are recorded');
        const graph = projectHierarchy(result.walk!);
        expect(graph.data.nodes).toHaveLength(1);
        expect(graph.data.edges).toEqual([]);
    });
    it('distinguishes unavailable calls from a valid leaf', async () => {
        for (const getFacts of [vi.fn().mockRejectedValue(new Error('Offline')), vi.fn().mockResolvedValue({ callees: { value: [], state: 'unknown', evidence: [] } })]) {
            const result = await loadSelectionHierarchy({ ...source(), getFacts }, '/workspace', rootSymbol(), {});
            expect(result.status).toBe('unavailable');
            expect(result.walk).toBeUndefined();
            expect(result.message).not.toContain('No outgoing');
        }
    });
    it('reports an incomplete traversal when a reached symbol cannot be read', async () => {
        const api = source();
        api.getFacts = vi.fn().mockResolvedValueOnce({ callees: { value: [{ targetName: 'child', targetQualifiedName: 'p.child' }], state: 'known', evidence: [] } }).mockRejectedValue(new Error('Unavailable child'));
        const result = await loadSelectionHierarchy(api, '/workspace', rootSymbol(), { depth: 2, cap: 5 });
        expect(result.status).toBe('ready');
        expect(result.walk?.nodes).toHaveLength(2);
        expect(result.message).toContain('incomplete');
    });
});

describe('hierarchy request identity', () => {
    let container: HTMLDivElement;
    let reactRoot: Root;
    function Harness({ api, selected, project = 'p' }: { api: ClosureSource; selected?: SymbolRef; project?: string }) {
        const result = useSelectionHierarchy(api, '/workspace', project, selected, { depth: 2, cap: 5 });
        return <output>{JSON.stringify(result)}</output>;
    }
    const read = () => JSON.parse(container.querySelector('output')!.textContent!);
    beforeEach(() => {
        (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
        container = document.createElement('div');
        reactRoot = createRoot(container);
    });
    afterEach(async () => { await act(async () => reactRoot.unmount()); });

    it('clears old results during rapid selection and discards the old response', async () => {
        let answerOld!: (value: SymbolFacts) => void;
        let answerNew!: (value: SymbolFacts) => void;
        const api = source();
        api.getFacts = vi.fn().mockImplementationOnce(() => new Promise<SymbolFacts>((resolve) => { answerOld = resolve; })).mockImplementationOnce(() => new Promise<SymbolFacts>((resolve) => { answerNew = resolve; }));
        const old = rootSymbol('old');
        const current = rootSymbol('current');
        await act(async () => { reactRoot.render(<Harness api={api} selected={old} />); });
        await act(async () => { reactRoot.render(<Harness api={api} selected={current} />); });
        expect(read().status).toBe('loading');
        expect(read().walk).toBeUndefined();
        await act(async () => { answerNew(noCalls); });
        expect(read().walk.root.name).toBe('current');
        await act(async () => { answerOld(noCalls); });
        expect(read().walk.root.name).toBe('current');
    });
    it('does not retain a walk after clearing the selection or changing project', async () => {
        const api = source();
        const selected = rootSymbol();
        await act(async () => { reactRoot.render(<Harness api={api} selected={selected} />); });
        expect(read().status).toBe('ready');
        await act(async () => { reactRoot.render(<Harness api={api} />); });
        expect(read()).toMatchObject({ status: 'empty' });
        expect(read().walk).toBeUndefined();
        api.getFacts = vi.fn().mockImplementation(() => new Promise(() => {}));
        await act(async () => { reactRoot.render(<Harness api={api} selected={selected} project="other" />); });
        expect(read().status).toBe('loading');
        expect(read().walk).toBeUndefined();
    });
});
