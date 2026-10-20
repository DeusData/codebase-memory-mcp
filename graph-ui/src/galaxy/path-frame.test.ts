import { describe, expect, it } from 'vitest';
import { PATH_FRAME_MARGIN, pathFrame, placeAlongSegment, type ScreenRect } from './path-frame';

/* A camera looking down -z with y up, as the scene sets it; projection by hand. */
function project(point: { x: number; y: number; z: number }, frame: ReturnType<typeof pathFrame>, viewport: { width: number; height: number }, fov: number) {
    const depth = frame.position.z - point.z;
    const half = Math.tan((fov * Math.PI) / 360) * depth;
    const aspect = viewport.width / viewport.height;
    return { x: ((point.x - frame.position.x) / (half * aspect) + 1) / 2 * viewport.width, y: (1 - (point.y - frame.position.y) / half) / 2 * viewport.height };
}

describe('hand test K6: the camera frames a shown path', () => {
    const view = { direction: { x: 0, y: 0, z: -1 }, up: { x: 0, y: 1, z: 0 } };
    const viewport = { width: 1170, height: 800 };

    it('zooms in on a short path, so root and target stand far apart on screen and inside the free area', () => {
        const root = { x: 0, y: 0, z: 0 }, target = { x: 12, y: 4, z: 0 };
        const frame = pathFrame([root, target], view, viewport, 50, { left: 440, top: 0 });
        const a = project(root, frame, viewport, 50), b = project(target, frame, viewport, 50);
        expect(Math.hypot(a.x - b.x, a.y - b.y)).toBeGreaterThan(250);
        for (const point of [a, b]) {
            expect(point.x).toBeGreaterThanOrEqual(440 + PATH_FRAME_MARGIN.x - 1);
            expect(point.x).toBeLessThanOrEqual(viewport.width - PATH_FRAME_MARGIN.x + 1);
            expect(point.y).toBeGreaterThanOrEqual(PATH_FRAME_MARGIN.y - 1);
            expect(point.y).toBeLessThanOrEqual(viewport.height - PATH_FRAME_MARGIN.y + 1);
        }
    });

    it('steps back for a long path until every node fits, keeping the view direction', () => {
        const points = [{ x: -400, y: 0, z: 0 }, { x: 0, y: 250, z: 0 }, { x: 380, y: -200, z: 0 }];
        const frame = pathFrame(points, view, viewport, 50, { left: 0, top: 0 });
        expect(frame.position.z).toBeGreaterThan(400);
        expect(frame.position.x).toBeCloseTo(frame.lookAt.x);
        for (const point of points.map(entry => project(entry, frame, viewport, 50))) {
            expect(point.x).toBeGreaterThanOrEqual(PATH_FRAME_MARGIN.x - 1);
            expect(point.x).toBeLessThanOrEqual(viewport.width - PATH_FRAME_MARGIN.x + 1);
        }
    });

    it('never moves closer than the minimum distance for a single point', () => {
        const frame = pathFrame([{ x: 5, y: 5, z: 5 }], view, viewport, 50, { left: 0, top: 0 });
        expect(frame.position.z - 5).toBeGreaterThanOrEqual(30);
    });
});

describe('hand test K6: edge labels avoid node names', () => {
    const size = { width: 40, height: 14 };
    const overlaps = (a: ScreenRect, b: ScreenRect) => !(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top);

    it('keeps the midpoint when nothing is in the way', () => {
        expect(placeAlongSegment({ x: 0, y: 0 }, { x: 200, y: 0 }, size, [], []).t).toBe(0.5);
    });

    it('slides along the edge away from a name that covers the midpoint', () => {
        const name = { left: 70, right: 130, top: -10, bottom: 10 };
        const placed = placeAlongSegment({ x: 0, y: 0 }, { x: 200, y: 0 }, size, [name], []);
        expect(placed.overlap).toBe(0);
        expect(overlaps(placed.rect, name)).toBe(false);
        expect(placed.side).toBe(0);
        expect(placed.t).not.toBe(0.5);
    });

    it('steps aside from the edge when every point along it is covered, and avoids labels placed before', () => {
        const wide = { left: -20, right: 220, top: -9, bottom: 9 };
        const placed = placeAlongSegment({ x: 0, y: 0 }, { x: 200, y: 0 }, size, [wide], []);
        expect(placed.overlap).toBe(0);
        expect(overlaps(placed.rect, wide)).toBe(false);
        const second = placeAlongSegment({ x: 0, y: 0 }, { x: 200, y: 0 }, size, [], [placed.rect]);
        expect(overlaps(second.rect, placed.rect)).toBe(false);
    });

    it('steps far enough aside on a short steep edge between two names (Func and Aggregate in django-demo)', () => {
        const from = { x: 1224, y: 618 }, to = { x: 1197, y: 536 };
        const names = [{ left: 1170, right: 1226, top: 506, bottom: 526 }, { left: 1185, right: 1264, top: 578, bottom: 598 }];
        const placed = placeAlongSegment(from, to, { width: 62, height: 15 }, names, []);
        expect(placed.overlap).toBe(0);
        expect(names.some(name => overlaps(placed.rect, name))).toBe(false);
    });

    /*
     * Hand test 2026-10-04 (G2): a wide heading (the band title of the
     * hierarchy) covers the middle of a steep edge. Every point from 0.2 to
     * 0.8 lies under it, and a side step on a steep edge only moves sideways
     * along the heading. The label still finds the free stretch of its edge
     * just before or after the heading instead of lying on it.
     */
    it('finds the free stretch of its edge beyond a wide heading that covers every usual spot', () => {
        const heading = { left: -500, right: 500, top: 50, bottom: 250 };
        const placed = placeAlongSegment({ x: 0, y: 0 }, { x: 10, y: 300 }, size, [heading], []);
        expect(placed.overlap).toBe(0);
        expect(overlaps(placed.rect, heading)).toBe(false);
        // On the edge, not beside it, and not on its end points.
        expect(placed.side).toBe(0);
        expect(placed.t).toBeGreaterThan(0.05);
        expect(placed.t).toBeLessThan(0.95);
    });
});
