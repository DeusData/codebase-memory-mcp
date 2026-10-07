/*
 * Review zu K30: "ueberlappen nie" galt fuer die Kantenlabels des Pfades nicht.
 * Fand ein Label keinen freien Platz, nahm placeAlongSegment den mit der
 * kleinsten Ueberdeckung, und PathLayer zeichnete es dort, auch mitten auf der
 * Ueberschrift des Bandes. Die Kantenschilder der Hierarchie fallen in diesem
 * Fall weg; die Labels des Pfades jetzt auch. Die Art jeder Kante steht
 * weiter in der Schrittliste.
 */
import { expect, it } from 'vitest';
import { pathLabelSpots } from './PathLayer';
import type { ScreenRect } from './path-frame';

const overlaps = (a: ScreenRect, b: ScreenRect) => !(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top);

// Measured in the browser (CSS px): JSONBAgg <-DEFINES- general.py -DEFINES-> __init__ at two layers of the hierarchy.
const JSONBAGG = { x: 1050, y: 368 }, GENERAL = { x: 808, y: 572 }, INIT = { x: 1160, y: 757 };
const NAMES: ScreenRect[] = [{ left: 1018, right: 1082, top: 336, bottom: 353 }, { left: 778, right: 838, top: 540, bottom: 557 }, { left: 1135, right: 1185, top: 725, bottom: 742 }];
const LABEL = { width: 50, height: 14 };
const edges = [{ index: 0, from: GENERAL, to: JSONBAGG, size: LABEL }, { index: 1, from: GENERAL, to: INIT, size: LABEL }];

it('K30: a path label that finds no free spot on the band title is left out, not drawn on the title', () => {
    // A band title that covers the edge from general.py to __init__ from end to end (the title of the browser run, made larger).
    const title = { left: 512, right: 1590, top: 503, bottom: 835 };
    const spots = pathLabelSpots(edges, [...NAMES, title]);
    expect(spots.get(1)).toBeUndefined();
    // The other edge runs above the title and keeps its label, clear of every name and of the title.
    const kept = spots.get(0);
    expect(kept).toBeDefined();
    for (const blocker of [...NAMES, title]) expect(overlaps(kept!.rect, blocker)).toBe(false);
});

it('K30: with the title at its real size both labels stand, and none touches a name, the title or the other label', () => {
    const title = { left: 834, right: 1268, top: 652, bottom: 687 };
    const spots = pathLabelSpots(edges, [...NAMES, title]);
    const rects = [spots.get(0)?.rect, spots.get(1)?.rect];
    expect(rects.every(Boolean)).toBe(true);
    for (const rect of rects) for (const blocker of [...NAMES, title]) expect(overlaps(rect!, blocker)).toBe(false);
    expect(overlaps(rects[0]!, rects[1]!)).toBe(false);
});

it('K30: a later label also keeps clear of an earlier one, and gives way when only its place is left', () => {
    // Two labels on the same short edge: the second has nowhere to go but onto the first.
    const short = [{ index: 0, from: { x: 100, y: 100 }, to: { x: 130, y: 100 }, size: LABEL }, { index: 1, from: { x: 100, y: 100 }, to: { x: 130, y: 100 }, size: LABEL }];
    const walls = [{ left: 0, right: 400, top: 0, bottom: 88 }, { left: 0, right: 400, top: 112, bottom: 300 }, { left: 0, right: 56, top: 0, bottom: 300 }, { left: 174, right: 400, top: 0, bottom: 300 }];
    const spots = pathLabelSpots(short, walls);
    expect(spots.get(0)).toBeDefined();
    expect(spots.get(1)).toBeUndefined();
});
