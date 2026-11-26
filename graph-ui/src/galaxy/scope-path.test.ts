import { describe, expect, it } from 'vitest';
import { callOrder, pathNodes, shortestScopePath } from './scope-path';
import type { GraphEdge } from './types';

const edge = (id: number, source: number, target: number, type = 'CALLS', line?: number): GraphEdge => ({ id, source, target, type, line });
const hops = (steps: ReturnType<typeof shortestScopePath>) => steps?.map(step => `${step.from}>${step.to}:${step.edge.id}`);

describe('shortest path inside the loaded scope', () => {
    // 1 -> 2 -> 3 -> 4, a shortcut 1 -> 4 typed IMPORTS, and a caller 5 -> 1.
    const edges = [edge(10, 1, 2), edge(11, 2, 3), edge(12, 3, 4), edge(13, 1, 4, 'IMPORTS'), edge(14, 5, 1), edge(15, 6, 6)];
    const roots = new Set([1]);

    it('follows outgoing relationships and takes the fewest hops', () => {
        expect(hops(shortestScopePath(edges, roots, 4, 'outbound'))).toEqual(['1>4:13']);
        expect(hops(shortestScopePath(edges, roots, 3, 'outbound'))).toEqual(['1>2:10', '2>3:11']);
        expect(shortestScopePath(edges, roots, 5, 'outbound')).toBeUndefined();
    });

    it('walks edges backwards for incoming traces and keeps their indexed direction', () => {
        const steps = shortestScopePath(edges, roots, 5, 'inbound')!;
        expect(hops(steps)).toEqual(['1>5:14']);
        expect(steps[0]!.edge.source).toBe(5);
        expect(shortestScopePath(edges, roots, 2, 'inbound')).toBeUndefined();
    });

    it('ignores direction for both and returns the empty path for a root', () => {
        expect(hops(shortestScopePath(edges, roots, 5, 'both'))).toEqual(['1>5:14']);
        expect(hops(shortestScopePath(edges, roots, 3, 'both'))).toHaveLength(2);
        expect(shortestScopePath(edges, roots, 1, 'both')).toEqual([]);
        expect(shortestScopePath(edges, roots, 6, 'both')).toBeUndefined();
    });

    it('is deterministic under edge order and starts from the nearest of several roots', () => {
        const diamond = [edge(1, 1, 2), edge(2, 1, 3), edge(3, 2, 4), edge(4, 3, 4)];
        const forward = hops(shortestScopePath(diamond, roots, 4, 'outbound'));
        expect(hops(shortestScopePath([...diamond].reverse(), roots, 4, 'outbound'))).toEqual(forward);
        expect(forward).toEqual(['1>2:1', '2>4:3']);
        expect(hops(shortestScopePath(diamond, new Set([1, 3]), 4, 'outbound'))).toEqual(['3>4:4']);
    });

    it('collects every node a path touches', () => {
        const steps = shortestScopePath(edges, roots, 3, 'outbound')!;
        expect([...pathNodes(steps, roots)].sort()).toEqual([1, 2, 3]);
        expect([...pathNodes([], roots)]).toEqual([1]);
    });
});

describe('call order of the root', () => {
    it('lists outgoing CALLS by call-site line, unknown lines last, every call site once', () => {
        const edges = [edge(1, 1, 4, 'CALLS', 30), edge(2, 1, 2, 'CALLS', 12), edge(3, 1, 3, 'IMPORTS', 1),
            edge(4, 5, 1, 'CALLS', 2), edge(5, 1, 3, 'CALLS'), edge(6, 1, 2, 'CALLS', 40), edge(7, 1, 1, 'CALLS', 5)];
        expect(callOrder(edges, 1).map(step => `${step.to}@${step.edge.line ?? '-'}`)).toEqual(['2@12', '4@30', '2@40', '3@-']);
        expect(callOrder(edges, 1).every(step => step.from === 1 && step.edge.source === 1)).toBe(true);
        expect(callOrder(edges, 9)).toEqual([]);
    });
});
