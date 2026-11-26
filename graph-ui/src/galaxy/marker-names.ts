/*
 * Die Namen der markierten Wurzeln, ohne dass einer auf dem anderen liegt
 * (Handtest K12).
 *
 * Ein Ausschnitt um eine Datei markiert jede ihrer Definitionen als Wurzel,
 * und jede Marke trug ihren Namen fest unter dem Ring. Im Mini-Galaxy von
 * Explore (419 x 186 px bei offenem Chat) lagen so bei
 * .github/workflows/new_contributor_pr.yml sechs Namen aufeinander:
 * "permissions", "new_contributor_pr.yml", ".github/workflows/..." und "jobs"
 * waren nicht mehr zu lesen.
 *
 * Dieselbe Regel wie fuer die Schilder an Pfaden und in der Hierarchie
 * (path-frame.ts, HierarchyEdgeLabels.tsx): ein Name steht an einem freien
 * Platz oder gar nicht. Die Plaetze liegen um den eigenen Ring, der erste ist
 * der der Stilvorlage (unter dem Ring), damit eine einzelne Wurzel aussieht
 * wie bisher. Von den Plaetzen ohne fremden Namen gewinnt der, der am
 * wenigsten von den fremden Ringen deckt. Ein Name ohne freien Platz wird
 * ausgeblendet; die Karte beim Ueberfahren des Knotens nennt ihn weiter.
 */
import type { ScreenRect } from './path-frame';

/** Der halbe Ring einer Marke (graph-exploration.css: 30 px) und der Abstand des Namens davon (dort `top: 19px`). */
export const MARKER_RING_RADIUS = 15;
const NAME_GAP = 4;
const ROW_GAP = 2;

export type MarkerNameSlot = 'below' | 'above' | 'right' | 'left' | 'below-2' | 'above-2' | 'right-up' | 'right-down' | 'left-up' | 'left-down';

/** Eine Marke: die Mitte ihres Rings auf dem Schirm und die Groesse ihres Namens. */
export interface MarkerName { id: number; x: number; y: number; width: number; height: number }
export interface PlacedMarkerName { id: number; slot: MarkerNameSlot | 'hidden'; rect?: ScreenRect }

/** Der Kasten eines Namens an einem Platz, in Bildschirmpixeln. */
export function markerNameRect(marker: MarkerName, slot: MarkerNameSlot): ScreenRect {
    const { x, y, width: w, height: h } = marker;
    const reach = MARKER_RING_RADIUS + NAME_GAP;
    const at = (left: number, top: number): ScreenRect => ({ left, top, right: left + w, bottom: top + h });
    switch (slot) {
        case 'below': return at(x - w / 2, y + reach);
        case 'above': return at(x - w / 2, y - reach - h);
        case 'right': return at(x + reach, y - h / 2);
        case 'left': return at(x - reach - w, y - h / 2);
        case 'below-2': return at(x - w / 2, y + reach + h + ROW_GAP);
        case 'above-2': return at(x - w / 2, y - reach - 2 * h - ROW_GAP);
        case 'right-up': return at(x + reach, y - h / 2 - h - ROW_GAP);
        case 'right-down': return at(x + reach, y + h / 2 + ROW_GAP);
        case 'left-up': return at(x - reach - w, y - h / 2 - h - ROW_GAP);
        case 'left-down': return at(x - reach - w, y + h / 2 + ROW_GAP);
    }
}

const SLOTS: readonly MarkerNameSlot[] = ['below', 'above', 'right', 'left', 'below-2', 'above-2', 'right-up', 'right-down', 'left-up', 'left-down'];
const hit = (a: ScreenRect, b: ScreenRect) => !(a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top);
const overlapArea = (a: ScreenRect, b: ScreenRect) => Math.max(0, Math.min(a.right, b.right) - Math.max(a.left, b.left))
    * Math.max(0, Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top));
const inside = (rect: ScreenRect, bounds: ScreenRect) => rect.left >= bounds.left && rect.right <= bounds.right && rect.top >= bounds.top && rect.bottom <= bounds.bottom;
const CENTRED: ReadonlySet<MarkerNameSlot> = new Set(['below', 'above', 'below-2', 'above-2']);
/* Ein Name ueber oder unter seinem Ring rueckt seitlich in die Zeichenflaeche, solange er den Ring noch ueberspannt. */
function nudged(marker: MarkerName, slot: MarkerNameSlot, bounds: ScreenRect): ScreenRect {
    const rect = markerNameRect(marker, slot);
    if (!CENTRED.has(slot) || marker.width > bounds.right - bounds.left) return rect;
    const shift = rect.left < bounds.left ? bounds.left - rect.left : rect.right > bounds.right ? bounds.right - rect.right : 0;
    return Math.abs(shift) <= marker.width / 2 - MARKER_RING_RADIUS / 2 ? { ...rect, left: rect.left + shift, right: rect.right + shift } : rect;
}

/**
 * Ein Platz je Name, in der Reihenfolge der Marken: wer zuerst kommt, behaelt
 * den Platz der Stilvorlage. Kein Name liegt auf einem anderen oder ausserhalb
 * von `bounds` (der Zeichenflaeche); was keinen Platz findet, ist `hidden`.
 * Eine Marke, deren Ring ausserhalb liegt, hat dort auch keinen Namen.
 */
export function placeMarkerNames(markers: readonly MarkerName[], bounds: ScreenRect): PlacedMarkerName[] {
    const rings = markers.map((marker) => ({ id: marker.id, rect: { left: marker.x - MARKER_RING_RADIUS, right: marker.x + MARKER_RING_RADIUS,
        top: marker.y - MARKER_RING_RADIUS, bottom: marker.y + MARKER_RING_RADIUS } }));
    const placed: ScreenRect[] = [];
    return markers.map((marker) => {
        // Among the places clear of other names, the one that covers the least of the other rings; the order of SLOTS breaks ties.
        let best: { slot: MarkerNameSlot; rect: ScreenRect; covered: number } | undefined;
        for (const slot of SLOTS) {
            const rect = nudged(marker, slot, bounds);
            if (!inside(rect, bounds) || placed.some((other) => hit(rect, other))) continue;
            const covered = rings.reduce((sum, ring) => sum + (ring.id === marker.id ? 0 : overlapArea(rect, ring.rect)), 0);
            if (!best || covered < best.covered) best = { slot, rect, covered };
            if (covered === 0) break;
        }
        if (!best) return { id: marker.id, slot: 'hidden' };
        placed.push(best.rect);
        return { id: marker.id, slot: best.slot, rect: best.rect };
    });
}
