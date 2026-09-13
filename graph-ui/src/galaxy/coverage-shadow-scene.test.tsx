import { Children, isValidElement, type ReactElement, type ReactNode } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it, vi } from 'vitest';
import { buildCoverageShadow } from './coverage-shadow';
import type { CoverageShadowNode } from './coverage-shadow';
import type { GraphData, GraphNode } from './types';

const canvas = vi.hoisted(() => ({ children: undefined as unknown }));
vi.mock('@react-three/fiber', async (importOriginal) => ({
    ...await importOriginal<typeof import('@react-three/fiber')>(),
    Canvas: ({ children }: { children: unknown }) => { canvas.children = children; return null; },
}));
import { GraphScene } from './GraphScene';
import { NodeCloud } from './NodeCloud';

function elements(children: ReactNode): ReactElement<Record<string, unknown>>[] {
    return Children.toArray(children).flatMap((child) => {
        if (!isValidElement<Record<string, unknown>>(child)) return [];
        return [child, ...elements(child.props['children'] as ReactNode)];
    });
}
const node = (id: number): GraphNode => ({ id, x: 0, y: 0, z: 0, label: 'File', name: 'file', size: 3, color: '#00ff88' });
const data: GraphData = { nodes: [node(1)], edges: [], total_nodes: 1, missed_graph: { nodes: [node(1)], edges: [], offset: { x: 200, y: 0, z: 0 } } };

describe('coverage shadow scene selection', () => {
    it('routes a shadow click only to the coverage callback despite duplicate source IDs', () => {
        const shadow = buildCoverageShadow(data)!;
        const onCode = vi.fn();
        const onShadow = vi.fn();
        renderToStaticMarkup(<GraphScene data={data} coverageShadow={shadow} highlightedIds={null} cameraTarget={null} showLabels={false} onNodeClick={onCode} onShadowNodeClick={onShadow} />);
        const clouds = elements(canvas.children as ReactNode).filter((element) => element.type === NodeCloud);
        expect(clouds).toHaveLength(2);
        const shadowClick = clouds[1]!.props['onClick'] as (node: CoverageShadowNode) => void;
        shadowClick(shadow.nodes[0]!);
        expect(onShadow).toHaveBeenCalledWith(shadow.nodes[0]);
        expect(onCode).not.toHaveBeenCalled();
        const codeClick = clouds[0]!.props['onClick'] as (node: GraphNode) => void;
        codeClick(data.nodes[0]!);
        expect(onCode).toHaveBeenCalledWith(data.nodes[0]);
    });
    it('never falls through to code selection without a shadow callback', () => {
        const shadow = buildCoverageShadow(data)!;
        const onCode = vi.fn();
        renderToStaticMarkup(<GraphScene data={data} coverageShadow={shadow} highlightedIds={null} cameraTarget={null} showLabels={false} onNodeClick={onCode} />);
        const cloud = elements(canvas.children as ReactNode).filter((element) => element.type === NodeCloud)[1]!;
        (cloud.props['onClick'] as (node: GraphNode) => void)(shadow.nodes[0]!);
        expect(onCode).not.toHaveBeenCalled();
    });
    it('does not render a second point cloud when coverage is disabled', () => {
        renderToStaticMarkup(<GraphScene data={data} highlightedIds={null} cameraTarget={null} showLabels={false} onNodeClick={vi.fn()} />);
        expect(elements(canvas.children as ReactNode).filter((element) => element.type === NodeCloud)).toHaveLength(1);
    });
});
