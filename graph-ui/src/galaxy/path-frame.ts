/**
 * Ein gezeigter Pfad wird gerahmt, und seine Kantenlabels weichen den Namen
 * aus (Handtest K6).
 *
 * Bis hierher blieb die Kamera beim Anzeigen eines Pfades stehen. Lagen Ziel
 * und Wurzel nah beieinander, standen ihre Namen und das Label der Kante auf
 * wenigen Pixeln, und der Name des Ziels deckte "CALLS" zu. Zwei reine
 * Rechnungen, ohne three und ohne DOM, damit sie ohne Browser pruefbar sind:
 *
 *  - {@link pathFrame}: wohin die Kamera fliegt. Sie behaelt ihre Blickrichtung
 *    (ein Pfad ist keine neue Ansicht, nur ein naeherer Ausschnitt) und tritt so
 *    weit vor oder zurueck, dass alle Knoten des Pfades mit Rand im freien Teil
 *    der Flaeche stehen; links liegt die Schrittliste.
 *  - {@link placeAlongSegment}: wo ein Kantenlabel steht. Es gleitet auf seiner
 *    Kante, bis es keinen Namen und kein anderes Label mehr beruehrt, und tritt
 *    erst dann seitlich neben die Kante.
 */

export interface Vec3 { x: number; y: number; z: number }
export interface ScreenRect { left: number; right: number; top: number; bottom: number }

/** Der Rand um den gerahmten Pfad, in Pixeln: Namen stehen neben und ueber ihrem Knoten. */
export const PATH_FRAME_MARGIN = { x: 140, y: 70 };
/** Naeher kommt die Kamera nicht, auch nicht fuer einen Pfad aus zwei fast gleichen Punkten. */
export const PATH_FRAME_MIN_DISTANCE = 30;
/*
 * Hoechstens so viel der freien Flaeche, von der Mitte aus gemessen: der Pfad
 * fuellt etwa die Haelfte, der Rest seiner Nachbarschaft bleibt sichtbar. Ohne
 * diese Grenze stand ein Pfad aus einem Hop ueber die ganze Breite.
 */
export const PATH_FRAME_FILL = 0.25;

const sub = (a: Vec3, b: Vec3): Vec3 => ({ x: a.x - b.x, y: a.y - b.y, z: a.z - b.z });
const dot = (a: Vec3, b: Vec3) => a.x * b.x + a.y * b.y + a.z * b.z;
const cross = (a: Vec3, b: Vec3): Vec3 => ({ x: a.y * b.z - a.z * b.y, y: a.z * b.x - a.x * b.z, z: a.x * b.y - a.y * b.x });
const scale = (a: Vec3, factor: number): Vec3 => ({ x: a.x * factor, y: a.y * factor, z: a.z * factor });
const add = (...vectors: Vec3[]): Vec3 => vectors.reduce((sum, value) => ({ x: sum.x + value.x, y: sum.y + value.y, z: sum.z + value.z }), { x: 0, y: 0, z: 0 });
const unit = (a: Vec3): Vec3 => { const length = Math.hypot(a.x, a.y, a.z) || 1; return scale(a, 1 / length); };

/**
 * Das Kameraziel fuer einen Pfad.
 *
 * @param inset der verdeckte Rand der Flaeche in Pixeln (die Schrittliste links oben)
 * @returns Blickpunkt, Kameraposition und die Pixel je Welteinheit am Pfad
 */
export function pathFrame(points: readonly Vec3[], view: { direction: Vec3; up: Vec3 }, viewport: { width: number; height: number },
    fovDegrees: number, inset: { left: number; top: number }): { lookAt: Vec3; position: Vec3; pixelsPerUnit: number } {
    const direction = unit(view.direction);
    const right = unit(cross(direction, view.up));
    const up = cross(right, direction);
    const center = scale(add(...points), 1 / Math.max(1, points.length));
    const tangent = Math.tan((fovDegrees * Math.PI) / 360);
    const freeWidth = Math.max(1, viewport.width - inset.left), freeHeight = Math.max(1, viewport.height - inset.top);
    const halfX = Math.max(1, Math.min(freeWidth / 2 - PATH_FRAME_MARGIN.x, freeWidth * PATH_FRAME_FILL));
    const halfY = Math.max(1, Math.min(freeHeight / 2 - PATH_FRAME_MARGIN.y, freeHeight * PATH_FRAME_FILL));
    let pixelsPerUnit = viewport.height / (2 * PATH_FRAME_MIN_DISTANCE * tangent);
    for (const point of points) {
        const offset = sub(point, center);
        const x = Math.abs(dot(offset, right)), y = Math.abs(dot(offset, up));
        if (x > 1e-6) pixelsPerUnit = Math.min(pixelsPerUnit, halfX / x);
        if (y > 1e-6) pixelsPerUnit = Math.min(pixelsPerUnit, halfY / y);
    }
    const distance = viewport.height / (2 * pixelsPerUnit * tangent);
    // Der Pfad steht in der Mitte des FREIEN Teils, nicht der ganzen Flaeche.
    const shiftX = inset.left + freeWidth / 2 - viewport.width / 2, shiftY = inset.top + freeHeight / 2 - viewport.height / 2;
    const lookAt = add(center, scale(right, -shiftX / pixelsPerUnit), scale(up, shiftY / pixelsPerUnit));
    return { lookAt, position: sub(lookAt, scale(direction, distance)), pixelsPerUnit };
}

const LABEL_STEPS = [0.5, 0.4, 0.6, 0.3, 0.7, 0.2, 0.8];
const LABEL_PAD = 2;

function overlapArea(a: ScreenRect, b: ScreenRect): number {
    const width = Math.min(a.right, b.right + LABEL_PAD) - Math.max(a.left, b.left - LABEL_PAD);
    const height = Math.min(a.bottom, b.bottom + LABEL_PAD) - Math.max(a.top, b.top - LABEL_PAD);
    return width > 0 && height > 0 ? width * height : 0;
}

const SIDES = [0, 1, -1, 2, -2, 3, -3];
/* Wie nah ein Label seinen Enden kommen darf, wenn es nur so frei steht: dort laufen die Linien eines Knotens zusammen. */
const LABEL_CLEAR_RANGE = [0.1, 0.9] as const;

/**
 * Wo das Label einer Kante steht: der erste Punkt auf der Kante, an dem es
 * nichts beruehrt, sonst daneben, sonst der Platz mit der kleinsten
 * Ueberdeckung. `t` ist der Anteil auf der Kante, `side` der seitliche Versatz
 * in Labelhoehen, hoechstens `maxSide` davon (die Hierarchie bleibt mit ihren
 * vielen Linien bei einem, damit ein Schild bei seiner Linie steht).
 *
 * Handtest 2026-10-04 (G2): eine breite Ueberschrift (die des Bandes der
 * Hierarchie) deckte auf einer steilen Kante jeden der Punkte von 0.2 bis 0.8,
 * und ein Schritt zur Seite fuehrt dort nur an ihr entlang. Bevor das Label
 * sich mit der kleinsten Ueberdeckung begnuegt, versucht es darum die Stellen
 * der Kante, an denen es gerade aus einem Hindernis heraustritt, davor und
 * dahinter, zwischen {@link LABEL_CLEAR_RANGE}.
 */
export function placeAlongSegment(from: { x: number; y: number }, to: { x: number; y: number }, size: { width: number; height: number },
    blockers: readonly ScreenRect[], placed: readonly ScreenRect[], maxSide = 3): { t: number; side: number; rect: ScreenRect; overlap: number } {
    const length = Math.hypot(to.x - from.x, to.y - from.y) || 1;
    const normal = { x: -(to.y - from.y) / length, y: (to.x - from.x) / length };
    // One side step clears the label's own extent across the edge: its height on a flat edge, its width on a steep one.
    const step = (size.width / 2) * Math.abs(normal.x) + (size.height / 2) * Math.abs(normal.y) + 6;
    const others = [...blockers, ...placed];
    let best: { t: number; side: number; rect: ScreenRect; overlap: number } | undefined;
    const tryAt = (t: number, side: number) => {
        const offset = side * step;
        const x = from.x + (to.x - from.x) * t + normal.x * offset, y = from.y + (to.y - from.y) * t + normal.y * offset;
        const rect = { left: x - size.width / 2, right: x + size.width / 2, top: y - size.height / 2, bottom: y + size.height / 2 };
        const overlap = others.reduce((sum, other) => sum + overlapArea(rect, other), 0);
        if (!best || overlap < best.overlap) best = { t, side, rect, overlap };
        return overlap === 0;
    };
    for (const side of SIDES.filter((value) => Math.abs(value) <= maxSide)) {
        for (const t of LABEL_STEPS) if (tryAt(t, side)) return best!;
    }
    // The points on the edge where the label just leaves an obstacle, on each of its four sides, nearest the middle first.
    const dx = to.x - from.x, dy = to.y - from.y, margin = LABEL_PAD + 0.5;
    const leaving = others.flatMap((other) => [
        dx ? (other.left - margin - size.width / 2 - from.x) / dx : NaN, dx ? (other.right + margin + size.width / 2 - from.x) / dx : NaN,
        dy ? (other.top - margin - size.height / 2 - from.y) / dy : NaN, dy ? (other.bottom + margin + size.height / 2 - from.y) / dy : NaN,
    ]).filter((t) => t >= LABEL_CLEAR_RANGE[0] && t <= LABEL_CLEAR_RANGE[1]).sort((a, b) => Math.abs(a - 0.5) - Math.abs(b - 0.5));
    for (const t of leaving) if (tryAt(t, 0)) return best!;
    return best!;
}
