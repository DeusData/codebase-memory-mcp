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
    expect(globalThis.__atlasGalaxy?.highlightedCount).toBe(1);
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
    expect(globalThis.__atlasGalaxy?.highlightedCount).toBe(1);
    expect(globalThis.__atlasGalaxy?.lastTargetQn).toBe('src/file3.ts');
});
