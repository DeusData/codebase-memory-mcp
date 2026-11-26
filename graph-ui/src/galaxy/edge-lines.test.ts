import { describe, expect, it } from 'vitest';
import * as THREE from 'three';
import { createEdgeGeometry, EDGE_INTENSITY_FOCUS, EDGE_INTENSITY_MUTED, edgeIntensityFor } from './EdgeLines';
import { edgeIntensityScale } from './density';
import { edgeColor } from '../graph/edge-style';
import type { GraphNode } from './types';

const node = (id: number, x = id, y = 0, z = 0): GraphNode => ({
    id, x, y, z, name: `node${id}`, label: 'Function', size: 1, color: '#ffffff',
});

describe('Galaxy edge geometry', () => {
    it('binds pulse progress to source and target, regardless of node order', () => {
        const geometry = createEdgeGeometry({
            nodes: [node(2, -5, 6, 7), node(1, 2, 3, 4)],
            edges: [{ source: 1, target: 2, type: 'CALLS' }],
            highlightedIds: null,
        });
        const positions = geometry.getAttribute('position');
        const flow = geometry.getAttribute('edgeFlow');
        expect(Array.from(positions.array)).toEqual([2, 3, 4, -5, 6, 7]);
        expect([flow.getX(0), flow.getX(1)]).toEqual([0, 1]);
        expect([flow.getZ(0), flow.getZ(1)]).toEqual([1, 1]);
        expect(flow.getY(0)).toBe(flow.getY(1));
        expect(flow.getY(0)).toBeGreaterThanOrEqual(0);
        expect(flow.getY(0)).toBeLessThan(1);
        geometry.dispose();
    });

    it('keeps symmetric and unknown relations static while hierarchy containment stays directed', () => {
        const types = ['SIMILAR_TO', 'SEMANTICALLY_RELATED', 'FILE_CHANGES_WITH', 'UNKNOWN', 'CONTAINS'];
        const geometry = createEdgeGeometry({
            nodes: [node(1), node(2)],
            edges: types.map((type) => ({ source: 1, target: 2, type })),
            highlightedIds: null,
        });
        const flow = geometry.getAttribute('edgeFlow');
        expect(types.map((_, index) => flow.getZ(index * 2))).toEqual([0, 0, 0, 0, 1]);
        expect(types.map((_, index) => flow.getZ(index * 2 + 1))).toEqual([0, 0, 0, 0, 1]);
        geometry.dispose();
    });

    it('compacts invalid and out-of-focus edges without shifting the surviving pulse attributes', () => {
        const nodes = [node(1), node(2), node(3)];
        const kept = { source: 2, target: 3, type: 'IMPORTS' };
        const single = createEdgeGeometry({ nodes, edges: [kept], highlightedIds: null });
        const filtered = createEdgeGeometry({
            nodes,
            edges: [
                { source: 404, target: 2, type: 'CALLS' },
                { source: 1, target: 2, type: 'CALLS' },
                kept,
            ],
            highlightedIds: new Set([3]),
        });
        expect(filtered.getAttribute('position').count).toBe(2);
        expect(filtered.getAttribute('color').count).toBe(2);
        expect(Array.from(filtered.getAttribute('edgeFlow').array))
            .toEqual(Array.from(single.getAttribute('edgeFlow').array));
        expect(Array.from(filtered.getAttribute('position').array))
            .toEqual(Array.from(single.getAttribute('position').array));
        single.dispose();
        filtered.dispose();
    });

    it('preserves source-to-target direction with an alternate target array and edge offset', () => {
        const geometry = createEdgeGeometry({
            nodes: [node(1, 0, 0, 1)],
            targetNodes: [node(2, 4, 0, 2)],
            edges: [{ source: 1, target: 2, type: 'CALLS', offset: 3 }],
            highlightedIds: null,
        });
        expect(Array.from(geometry.getAttribute('position').array)).toEqual([0, 3, 1, 4, 3, 2]);
        const flow = geometry.getAttribute('edgeFlow');
        expect([flow.getX(0), flow.getX(1)]).toEqual([0, 1]);
        geometry.dispose();
    });

    it('emphasizes incoming and outgoing selection edges without expanding the selected roots', () => {
        const nodes = [node(1), node(2), node(3), node(4)];
        const edges = [
            { source: 1, target: 2, type: 'CALLS' },
            { source: 3, target: 1, type: 'IMPORTS' },
            { source: 2, target: 3, type: 'CALLS' },
            { source: 1, target: 4, type: 'CONTAINS' },
        ];
        const highlightedIds = new Set([1, 4]);
        const normal = createEdgeGeometry({ nodes, edges, highlightedIds });
        const focused = createEdgeGeometry({ nodes, edges, highlightedIds, emphasizeIncidentEdges: true });
        const expectedPositions = [1, 0, 0, 2, 0, 0, 3, 0, 0, 1, 0, 0, 1, 0, 0, 4, 0, 0];
        expect(Array.from(normal.getAttribute('position').array)).toEqual(expectedPositions);
        expect(Array.from(focused.getAttribute('position').array)).toEqual(expectedPositions);
        expect(Array.from(highlightedIds)).toEqual([1, 4]);
        expect(Array.from(focused.getAttribute('edgeFlow').array))
            .toEqual(Array.from(normal.getAttribute('edgeFlow').array));

        for (const [index, type] of ['CALLS', 'IMPORTS', 'CONTAINS'].entries()) {
            const color = new THREE.Color(edgeColor(type));
            const normalIntensity = index === 2 ? EDGE_INTENSITY_FOCUS : EDGE_INTENSITY_MUTED * edgeIntensityScale(edges.length);
            for (const [geometry, intensity] of [[normal, normalIntensity], [focused, EDGE_INTENSITY_FOCUS]] as const) {
                const rendered = geometry.getAttribute('color');
                for (const vertex of [index * 2, index * 2 + 1]) {
                    expect(rendered.getX(vertex)).toBeCloseTo(color.r * intensity, 6);
                    expect(rendered.getY(vertex)).toBeCloseTo(color.g * intensity, 6);
                    expect(rendered.getZ(vertex)).toBeCloseTo(color.b * intensity, 6);
                }
            }
        }
        normal.dispose();
        focused.dispose();
    });

    it('leaves the overall graph unchanged when incident emphasis has no selected roots', () => {
        const nodes = [node(1), node(2), node(3)];
        const edges = [{ source: 1, target: 2, type: 'CALLS' }, { source: 2, target: 3, type: 'IMPORTS' }];
        for (const highlightedIds of [null, new Set<number>()]) {
            const normal = createEdgeGeometry({ nodes, edges, highlightedIds });
            const focused = createEdgeGeometry({ nodes, edges, highlightedIds, emphasizeIncidentEdges: true });
            for (const attribute of ['position', 'color', 'edgeFlow']) {
                expect(Array.from(focused.getAttribute(attribute).array))
                    .toEqual(Array.from(normal.getAttribute(attribute).array));
            }
            normal.dispose();
            focused.dispose();
        }
    });

    it('retains the shared relationship hue under density scaling and focus', () => {
        const nodes = [node(1), node(2)];
        const edges = [{ source: 1, target: 2, type: 'IMPORTS' }];
        const color = new THREE.Color(edgeColor('IMPORTS'));
        for (const highlightedIds of [null, new Set([1, 2])]) {
            const geometry = createEdgeGeometry({ nodes, edges, highlightedIds });
            const rendered = geometry.getAttribute('color');
            const intensity = highlightedIds ? EDGE_INTENSITY_FOCUS : edgeIntensityFor(true, 1);
            expect(rendered.getX(0)).toBeCloseTo(color.r * intensity, 6);
            expect(rendered.getY(0)).toBeCloseTo(color.g * intensity, 6);
            expect(rendered.getZ(0)).toBeCloseTo(color.b * intensity, 6);
            expect(Array.from(rendered.array).slice(0, 3)).toEqual(Array.from(rendered.array).slice(3));
            geometry.dispose();
        }
    });
});
