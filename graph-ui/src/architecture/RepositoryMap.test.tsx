// @vitest-environment jsdom
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import RepositoryMapView from './RepositoryMap';
import type { GraphData, GraphNode } from '../galaxy/types';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';

it('walks repository to an area, relationship evidence, file, symbol and source', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const container = document.createElement('div'); document.body.append(container);
    const root = createRoot(container); const onNavigate = vi.fn(); const onSelect = vi.fn();
    const makeNode = (id: number, name: string, path: string): GraphNode => ({ id, name, file_path: path,
        start_line: 12, x: 0, y: 0, z: 0, label: 'Function', size: 1, color: '#fff' });
    const graph: GraphData = { total_nodes: 2, nodes: [makeNode(1, 'serve', 'src/api/server.ts'), makeNode(2, 'save', 'src/store/db.ts')], edges: [{ source: 1, target: 2, type: 'CALLS' }] };
    const overview: ArchitectureOverviewDto = { totalSymbols: 2, totalRelations: 1, symbolKinds: [], relationKinds: [], languages: [], groups: [], entryPoints: [], routes: [], clusters: [], layers: [], boundaries: [], hotspots: [], files: [] };
    const click = async (text: string) => { const buttons = [...container.querySelectorAll('button')]; const button = buttons.find(button => button.textContent === text) ?? buttons.find(button => button.textContent?.includes(text)); expect(button, text).toBeTruthy(); await act(async () => button!.click()); };
    try {
        await act(async () => root.render(<RepositoryMapView graph={graph} overview={overview} filter="" onNavigate={onNavigate} onSelect={onSelect} />));
        await click('src/api');
        expect(container.textContent).toContain('How this area connects');
        await click('1 edges · inspect');
        expect(container.querySelector('[aria-label="Relationship evidence"]')?.textContent).toContain('CALLS');
        await click('save'); expect(onSelect).toHaveBeenCalledWith(graph.nodes[1]);
        await click('src/api/server.ts');
        await click('Read file'); expect(onNavigate).toHaveBeenCalledWith('src/api/server.ts');
        await click('serve'); expect(onSelect).toHaveBeenCalledWith(graph.nodes[0]);
        await click('Repository');
        expect(container.textContent).toContain('Components and responsibilities');
    } finally { await act(async () => root.unmount()); container.remove(); }
});
