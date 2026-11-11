import { describe, expect, it } from 'vitest';
import { separateScreenNodes, type ScreenNode } from './screen-node-spacing';

function smallestGap(nodes: readonly ScreenNode[]): number {
    let gap = Infinity;
    for (let a = 0; a < nodes.length; a++) for (let b = a + 1; b < nodes.length; b++) {
        const first = nodes[a]!, second = nodes[b]!;
        gap = Math.min(gap, Math.hypot(first.x - second.x, first.y - second.y) - first.radius - second.radius);
    }
    return gap;
}
const sortedPositions = (nodes: readonly ScreenNode[]) => [...nodes].sort((a, b) => a.id - b.id)
    .map(({ id, x, y }) => ({ id, x, y }));

describe('projected node spacing', () => {
    it('separates two nodes at exactly the same screen coordinate without dropping either', () => {
        const nodes = [{ id: 1, x: 100, y: 90, radius: 8 }, { id: 2, x: 100, y: 90, radius: 8 }];
        const result = separateScreenNodes(nodes, 4);
        expect(result.map(node => node.id)).toEqual([1, 2]);
        expect(smallestGap(result)).toBeGreaterThanOrEqual(4 - 1e-8);
        expect(result[0]).toBe(nodes[0]);
        expect(Math.hypot(result[1]!.x - 100, result[1]!.y - 90)).toBeLessThan(50);
    });

    it('adds a visible gap even when node disks merely touch', () => {
        const result = separateScreenNodes([{ id: 1, x: 0, y: 0, radius: 5 }, { id: 2, x: 10, y: 0, radius: 5 }], 6);
        expect(smallestGap(result)).toBeGreaterThanOrEqual(6 - 1e-8);
    });

    it('honors unequal radii and keeps the largest disk at its preferred location', () => {
        const nodes = [{ id: 2, x: 30, y: 40, radius: 3 }, { id: 1, x: 30, y: 40, radius: 50 },
            { id: 3, x: 81, y: 40, radius: 9 }];
        const result = separateScreenNodes(nodes, 4);
        expect(smallestGap(result)).toBeGreaterThanOrEqual(4 - 1e-8);
        expect(result[1]).toBe(nodes[1]);
    });

    it('places a dense group without residual overlaps, lost identities, or non-finite positions', () => {
        const nodes = Array.from({ length: 240 }, (_, id) => ({ id, x: 200 + id % 3, y: 150 + id % 5, radius: 2 + id % 11 }));
        const result = separateScreenNodes(nodes, 4);
        expect(result.map(node => node.id)).toEqual(nodes.map(node => node.id));
        expect(smallestGap(result)).toBeGreaterThanOrEqual(4 - 1e-8);
        expect(result.every(node => Number.isFinite(node.x) && Number.isFinite(node.y))).toBe(true);
    });

    it('is deterministic across input ordering and preserves input coordinates and arbitrary metadata', () => {
        const nodes = Object.freeze(Array.from({ length: 25 }, (_, id) => Object.freeze({
            id, x: 20, y: 30, radius: 3 + id % 4, qualifiedName: `example.node${id}`, color: '#8a9ba8',
        })));
        const before = structuredClone(nodes);
        const result = separateScreenNodes(nodes, 4);
        expect(sortedPositions(result)).toEqual(sortedPositions(separateScreenNodes([...nodes].reverse(), 4)));
        expect(nodes).toEqual(before);
        expect(result.map(({ x: _x, y: _y, ...metadata }) => metadata))
            .toEqual(nodes.map(({ x: _x, y: _y, ...metadata }) => metadata));
    });

    it('keeps already separated nodes unchanged and handles an empty view', () => {
        const nodes = [{ id: 1, x: -100, y: 40, radius: 4 }, { id: 2, x: 100, y: 90, radius: 12 }];
        expect(separateScreenNodes(nodes)).toEqual(nodes);
        expect(separateScreenNodes(nodes)[0]).toBe(nodes[0]);
        expect(separateScreenNodes([])).toEqual([]);
    });

    it('never moves a pinned root, even when larger disks sit on top of it', () => {
        const nodes = [{ id: 1, x: 0, y: 0, radius: 3 }, ...Array.from({ length: 30 }, (_, index) =>
            ({ id: index + 2, x: (index % 5) - 2, y: Math.floor(index / 5) - 3, radius: 6 + index % 9 }))];
        const result = separateScreenNodes(nodes, 4, new Set([1]));
        expect(result[0]).toBe(nodes[0]);
        expect(smallestGap(result)).toBeGreaterThanOrEqual(4 - 1e-8);
    });
});
