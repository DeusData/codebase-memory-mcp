import { expect, it } from 'vitest';
import { HIERARCHY_LABEL_PAD_X, hierarchyFrame, type HierarchyProjection } from './hierarchy-layout';

/*
 * Hand test 2026-10-04, round 4 (N1), seen in the browser: with its shown
 * name "django-demo · detached HEAD" the Branch node on the left of the
 * .github hierarchy began 23 px left of the canvas. The frame added a fixed
 * 55 units beside the outermost nodes, but a name in the hierarchy of a scope
 * is up to about 300 units wide and stands centred over its node. With the
 * widths of the names the frame reaches as far as the widest outer name.
 */
const projection = (xs: number[]): HierarchyProjection => ({
    data: { nodes: [], edges: [], total_nodes: 0 }, rootId: 0, rootKey: 'r', rootName: 'r', symbols: xs.length, depth: 2, truncated: false, cap: xs.length,
    walkDepth: 1, missing: 0, placements: xs.map((x, id) => ({ id, key: `k${id}`, name: `n${id}`, hop: id === 0 ? 0 : 1, x, y: 0 })),
} as unknown as HierarchyProjection);

it('N1: the frame of a hierarchy reaches past the widest outer name', () => {
    const plain = hierarchyFrame(projection([0, -173, 160]));
    expect(plain.width).toBeCloseTo(333 + 2 * HIERARCHY_LABEL_PAD_X, 5);
    // The left name is 204 units wide: half of it, and a margin, stand left of its node.
    const widths = [62, 204, 80];
    const named = hierarchyFrame(projection([0, -173, 160]), (placement) => widths[placement.id]!);
    const left = named.centerX - named.width / 2, right = named.centerX + named.width / 2;
    expect(left).toBeLessThanOrEqual(-173 - 204 / 2);
    // A narrow name keeps the old allowance.
    expect(right).toBeCloseTo(160 + HIERARCHY_LABEL_PAD_X, 5);
});
