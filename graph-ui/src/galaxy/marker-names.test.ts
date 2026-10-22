import { expect, it } from 'vitest';
import { MARKER_RING_RADIUS, placeMarkerNames, type MarkerName } from './marker-names';
import type { ScreenRect } from './path-frame';

const hit = (a: ScreenRect, b: ScreenRect) => !(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top);

it('keeps a lone root name under its ring, where the stylesheet puts it', () => {
    const [only] = placeMarkerNames([{ id: 1, x: 400, y: 300, width: 80, height: 19 }], { left: 0, top: 0, right: 800, bottom: 600 });
    expect(only).toMatchObject({ id: 1, slot: 'below' });
    expect(only!.rect).toEqual({ left: 360, right: 440, top: 300 + MARKER_RING_RADIUS + 4, bottom: 300 + MARKER_RING_RADIUS + 4 + 19 });
});

/*
 * Hand test K12: in Explore the mini-Galaxy of
 * .github/workflows/new_contributor_pr.yml marks six definitions as roots,
 * and their names lay on each other ("permissions", "new_contributor_pr.yml",
 * ".github/workflows/new_contributor_pr.yml", "jobs"). The anchors and sizes
 * are the ones measured in that panel (419 x 186 px at 1600 px with the chat open).
 */
it('gives clustered root names free places around their rings inside the canvas, and no two names overlap', () => {
    const canvas = { left: 1181, top: 787, right: 1600, bottom: 973 };
    const names: MarkerName[] = [
        { id: 1, x: 1444.4, y: 879.3, width: 261.9, height: 18.8 }, // .github/workflows/new_contributor_pr.yml
        { id: 2, x: 1382.1, y: 909.5, width: 36.9, height: 18.8 }, // jobs
        { id: 3, x: 1378.8, y: 872.9, width: 44.3, height: 18.8 }, // name
        { id: 4, x: 1377.0, y: 874.2, width: 152.0, height: 18.8 }, // new_contributor_pr.yml
        { id: 5, x: 1362.1, y: 861.5, width: 26.7, height: 18.8 }, // on
        { id: 6, x: 1393.9, y: 865.0, width: 83.8, height: 18.8 }, // permissions
    ];
    // As drawn before: every name under its own ring.
    const under = names.map(name => ({ left: name.x - name.width / 2, right: name.x + name.width / 2, top: name.y + MARKER_RING_RADIUS + 4, bottom: name.y + MARKER_RING_RADIUS + 4 + name.height }));
    expect(under.some((rect, i) => under.some((other, j) => i < j && hit(rect, other)))).toBe(true);

    const placed = placeMarkerNames(names, canvas);
    const shown = placed.filter(entry => entry.rect !== undefined);
    for (const [i, entry] of shown.entries()) {
        for (const other of shown.slice(i + 1)) expect(hit(entry.rect!, other.rect!), `${entry.id} x ${other.id}`).toBe(false);
        expect(entry.rect!.left).toBeGreaterThanOrEqual(canvas.left);
        expect(entry.rect!.right).toBeLessThanOrEqual(canvas.right);
        expect(entry.rect!.top).toBeGreaterThanOrEqual(canvas.top);
        expect(entry.rect!.bottom).toBeLessThanOrEqual(canvas.bottom);
    }
    // The four names of the hand test are all still there.
    expect(shown.map(entry => entry.id).sort()).toEqual(expect.arrayContaining([1, 2, 4, 6]));
    // The order is stable: the same input gives the same places.
    expect(placeMarkerNames(names, canvas)).toEqual(placed);
});

it('moves a long name under a ring near the edge sideways into the canvas, still spanning its ring', () => {
    const canvas = { left: 0, top: 0, right: 400, bottom: 300 };
    const [name] = placeMarkerNames([{ id: 1, x: 360, y: 100, width: 200, height: 18 }], canvas);
    expect(name!.slot).toBe('below');
    expect(name!.rect!.right).toBeLessThanOrEqual(400);
    expect(name!.rect!.left).toBeLessThan(360 - MARKER_RING_RADIUS);
    // A ring outside the canvas has no name inside it.
    expect(placeMarkerNames([{ id: 2, x: 520, y: 100, width: 200, height: 18 }], canvas)[0]!.slot).toBe('hidden');
});

it('drops a name that finds no free place instead of laying it over another', () => {
    const canvas = { left: 0, top: 0, right: 120, bottom: 80 };
    const placed = placeMarkerNames([{ id: 1, x: 60, y: 35, width: 110, height: 18 }, { id: 2, x: 62, y: 36, width: 110, height: 18 }], canvas);
    expect(placed[0]!.rect).toBeDefined();
    expect(placed[1]!.rect).toBeUndefined();
});
