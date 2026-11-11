import { Children, isValidElement, type ReactElement, type ReactNode } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { OrbitControls } from '@react-three/drei';
import { describe, expect, it, vi } from 'vitest';
import type { SemanticGraph } from './semantic-graph';

const canvas = vi.hoisted(() => ({ children: undefined as unknown }));
vi.mock('@react-three/fiber', async (importOriginal) => ({
    ...await importOriginal<typeof import('@react-three/fiber')>(),
    Canvas: ({ children }: { children: unknown }) => { canvas.children = children; return null; },
}));
import { ArchitectureScene } from './ArchitectureScene';
import SystemArchitectureScene from './SystemArchitectureScene';

function elements(children: ReactNode): ReactElement<Record<string, unknown>>[] {
    return Children.toArray(children).flatMap((child) => {
        if (!isValidElement<Record<string, unknown>>(child)) return [];
        return [child, ...elements(child.props['children'] as ReactNode)];
    });
}
const orbit = () => elements(canvas.children as ReactNode).filter(element => element.type === OrbitControls);
const model: SemanticGraph = { view: 'overview', scopeKey: 'overview', title: 'Repository structure', positionMeaning: '', nodes: [], edges: [],
    totalNodes: 0, totalEdges: 0, omittedNodes: 0, omittedEdges: 0, warnings: [] };

describe('architecture scene controls', () => {
    it('zooms the repository map toward the cursor', () => {
        renderToStaticMarkup(<ArchitectureScene model={model} onSelect={vi.fn()} onSelectEdge={vi.fn()} />);
        expect(orbit()).toHaveLength(1);
        expect(orbit()[0]!.props['zoomToCursor']).toBe(true);
    });
    it('zooms the system structure and behavior scenes toward the cursor', () => {
        for (const presentation of ['system', 'journey'] as const) {
            canvas.children = undefined;
            renderToStaticMarkup(<SystemArchitectureScene model={{ nodes: [], edges: [], lanes: [], omittedNodes: 0, omittedEdges: 0 }} resetKey={0}
                onSelectNode={vi.fn()} onSelectEdge={vi.fn()} presentation={presentation} />);
            expect(orbit()).toHaveLength(1);
            expect(orbit()[0]!.props['zoomToCursor']).toBe(true);
        }
    });
});
