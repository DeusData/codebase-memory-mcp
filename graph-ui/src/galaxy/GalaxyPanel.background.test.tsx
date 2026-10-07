// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel, { type GalaxyPanelProps } from './GalaxyPanel';

vi.mock('./GraphScene', async importOriginal => ({ ...await importOriginal<typeof import('./GraphScene')>(), GraphScene: ({ onBackgroundClick }: { onBackgroundClick?: () => void }) =>
    <button onClick={onBackgroundClick}>Empty canvas</button> }));
let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); globalThis.__atlasGalaxy = undefined; });
const nodes = [1, 2, 3].map(id => ({ id, name: `node${id}`, qualified_name: `sample.node${id}`, file_path: `src/file${id}.ts`,
    label: 'Function', x: id * 10, y: 0, z: 0, start_line: 1, end_line: 10, size: 1, color: '#999999' }));

it('keeps the overall graph after clearing while the code caret stays put, and follows the next explicit source change', async () => {
    const clear = vi.fn(), fetchLayout = vi.fn(async () => new Response(JSON.stringify({ nodes, edges: [{ source: 1, target: 2, type: 'CALLS' }], total_nodes: 3 })));
    const props: GalaxyPanelProps = { project: 'sample', visible: true, onOpenNode: vi.fn(), onClearSelection: clear,
        fetch: fetchLayout, focusQualifiedName: 'sample.node1', focusFilePath: 'src/file1.ts', focusSourceRange: { startLine: 1, endLine: 1 } };
    await act(async () => root.render(<GalaxyPanel {...props} />));
    await vi.waitFor(async () => {
        await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
        expect(globalThis.__atlasGalaxy?.highlightedCount).toBe(1);
    });
    const fits = globalThis.__atlasGalaxy!.fits;
    const background = [...host.querySelectorAll('button')].find(button => button.textContent === 'Empty canvas')!;
    await act(async () => background.click());
    expect(globalThis.__atlasGalaxy?.highlightedCount).toBe(0);
    expect(globalThis.__atlasGalaxy!.fits).toBeGreaterThan(fits);
    expect(clear).toHaveBeenCalledOnce();
    await act(async () => root.render(<GalaxyPanel {...props} visible={false} focusSourceRange={{ startLine: 1, endLine: 1 }} />));
    await act(async () => root.render(<GalaxyPanel {...props} focusSourceRange={{ startLine: 1, endLine: 1 }} />));
    expect(globalThis.__atlasGalaxy?.highlightedCount).toBe(0);
    await act(async () => root.render(<GalaxyPanel {...props} focusQualifiedName="sample.node3" focusFilePath="src/file3.ts" />));
    await vi.waitFor(async () => {
        await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
        expect(globalThis.__atlasGalaxy?.highlightedCount).toBe(1);
    });
    expect(globalThis.__atlasGalaxy?.lastTargetQn).toBe('src/file3.ts');
});

it('returns a standalone hierarchy selection to the overall Galaxy without stale walk prose', async () => {
    const onSelectionEvidence = vi.fn();
    const fetchLayout = vi.fn(async () => new Response(JSON.stringify({ nodes, edges: [], total_nodes: nodes.length })));
    await act(async () => root.render(<GalaxyPanel project="sample" visible workspaceExpanded onOpenNode={vi.fn()} fetch={fetchLayout} onSelectionEvidence={onSelectionEvidence} />));
    await act(async () => host.querySelector<HTMLInputElement>('[aria-label="Find a graph node"]')!.focus());
    const select = [...host.querySelectorAll<HTMLButtonElement>('.atlas-galaxy-node-picker button')]
        .find(button => button.querySelector('strong')?.textContent === 'node1')!;
    await act(async () => select.click());
    const evidence = JSON.parse(onSelectionEvidence.mock.lastCall![0].text).evidence;
    expect(evidence.selected.scope).toMatchObject({ kind: 'node', id: 1, name: 'node1' });
    expect(evidence.scope).toMatchObject({ depth: 1, direction: 'both', edgeTypes: 'all' });
    expect(evidence.limitations.state).not.toBe('complete-indexed-scope');
    await act(async () => host.querySelector<HTMLButtonElement>('[data-mode="hierarchy"]')!.click());
    expect(globalThis.__atlasGalaxy?.mode).toBe('hierarchy');
    const all = [...host.querySelectorAll('button')].find(button => button.textContent === 'All graph')!;
    await act(async () => all.click());
    expect(globalThis.__atlasGalaxy?.mode).toBe('galaxy');
    expect(globalThis.__atlasGalaxy?.nodes).toBe(nodes.length);
    expect(host.textContent).not.toContain('nothing of this walk');
    expect(onSelectionEvidence).toHaveBeenLastCalledWith(undefined);
});
