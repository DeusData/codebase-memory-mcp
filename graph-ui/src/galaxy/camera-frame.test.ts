/*
 * Die Rahmung, gerechnet statt betrachtet.
 *
 * Der Befund, aus dem diese Datei kommt, war ein Bild: ein Walk aus vier
 * Symbolen stand als Briefmarke in einem leeren Panel, weil die Wolken-Rahmung
 * der Uebernahme eine Untergrenze von 300 Einheiten hat. Was ein Test hier
 * beweisen kann und ein Screenshot nicht, ist die Umkehrung: bei der
 * gerechneten Entfernung fuellt das Rechteck das Bild bis auf den Rand, und
 * zwar unabhaengig davon, wie gross es ist.
 */

import { describe, expect, it } from 'vitest';

import {
    FLAT_MIN_ZOOM,
    FRAME_MARGIN,
    FRAME_MIN_DISTANCE,
    fitCamera,
    flatBounds,
    frameDistance,
    frameFill,
    orthographicZoom,
    principalFrame,
} from './camera-frame';
import type { Vector3Like } from './camera-frame';

const FOV = 50;

describe('frameDistance', () => {
    it('rahmt ein hohes Rechteck ueber die Hoehe', () => {
        const box = { centerX: 0, centerY: 0, width: 100, height: 400 };
        const distance = frameDistance(box, FOV, 1.3);
        const fill = frameFill(box, distance, FOV, 1.3);
        expect(fill.vertical).toBeCloseTo(1 / FRAME_MARGIN, 5);
        expect(fill.horizontal).toBeLessThan(fill.vertical);
    });

    it('rahmt ein breites Rechteck ueber die Breite', () => {
        const box = { centerX: 0, centerY: 0, width: 900, height: 100 };
        const distance = frameDistance(box, FOV, 1.3);
        const fill = frameFill(box, distance, FOV, 1.3);
        expect(fill.horizontal).toBeCloseTo(1 / FRAME_MARGIN, 5);
        expect(fill.vertical).toBeLessThan(fill.horizontal);
    });

    it('faehrt fuer ein kleines Bild naeher heran, statt auf einer Untergrenze zu bleiben', () => {
        const small = frameDistance({ centerX: 0, centerY: 0, width: 170, height: 150 }, FOV, 1.3);
        const large = frameDistance({ centerX: 0, centerY: 0, width: 260, height: 420 }, FOV, 1.3);
        expect(small).toBeLessThan(large);
        // Genau das war der Befund: die Uebernahme haelt hier 300 Einheiten
        // Abstand, egal wie klein das Bild ist.
        expect(small).toBeLessThan(300);
    });

    it('fuellt das Bild in beiden Groessen gleich gut', () => {
        for (const box of [
            { centerX: 45, centerY: 0, width: 174, height: 210 },
            { centerX: 90, centerY: 0, width: 264, height: 470 },
        ]) {
            const distance = frameDistance(box, FOV, 1.3);
            const fill = frameFill(box, distance, FOV, 1.3);
            expect(Math.max(fill.vertical, fill.horizontal)).toBeCloseTo(1 / FRAME_MARGIN, 5);
        }
    });

    it('braucht bei einem breiteren Panel weniger Abstand fuer dieselbe Breite', () => {
        const box = { centerX: 0, centerY: 0, width: 600, height: 100 };
        expect(frameDistance(box, FOV, 2)).toBeLessThan(frameDistance(box, FOV, 1));
    });

    it('antwortet auf unsinnige Eingaben mit der Untergrenze statt mit NaN', () => {
        const box = { centerX: 0, centerY: 0, width: 0, height: 0 };
        expect(frameDistance(box, FOV, 1.3)).toBe(FRAME_MIN_DISTANCE);
        expect(frameDistance(box, Number.NaN, Number.NaN)).toBe(FRAME_MIN_DISTANCE);
        expect(frameDistance({ centerX: 0, centerY: 0, width: -5, height: -5 }, FOV, 0))
            .toBe(FRAME_MIN_DISTANCE);
    });
});

/*
 * Die zweite Rahmung (W10): die flache Ansicht.
 *
 * Sie hat mit der ersten nichts gemein ausser dem Rand. Eine orthografische
 * Kamera hat keinen Ausschnittkegel, also gibt es hier keine Trigonometrie und
 * keine Entfernung: der Ausschnitt ist Pixel je Welteinheit, und mehr nicht.
 */
describe('die flache Ansicht', () => {
    const NODES = [
        { x: -100, y: -50 },
        { x: 100, y: 50 },
        { x: 0, y: 0 },
    ];

    it('umschliesst die Punkte in der Ebene und laesst z fallen', () => {
        expect(flatBounds(NODES)).toEqual({ centerX: 0, centerY: 0, width: 200, height: 100 });
    });

    it('rechnet den Mittelpunkt und nicht den Schwerpunkt', () => {
        // Drei Punkte links und einer rechts: der Schwerpunkt laege links, das
        // umschliessende Rechteck liegt in der Mitte. Gerahmt wird das Bild und
        // nicht die Dichte.
        expect(flatBounds([{ x: 0, y: 0 }, { x: 1, y: 0 }, { x: 2, y: 0 }, { x: 100, y: 0 }]).centerX)
            .toBe(50);
    });

    it('uebergeht Punkte, die keine Zahlen tragen', () => {
        expect(flatBounds([{ x: 0, y: 0 }, { x: Number.NaN, y: 10 }, { x: 10, y: 10 }]))
            .toEqual({ centerX: 5, centerY: 5, width: 10, height: 10 });
    });

    it('ergibt ohne Punkte ein Rechteck ohne Ausdehnung', () => {
        expect(flatBounds([])).toEqual({ centerX: 0, centerY: 0, width: 0, height: 0 });
        expect(flatBounds([{ x: Number.NaN, y: Number.NaN }]).width).toBe(0);
    });

    it('nimmt den Zoom der engeren Richtung, damit beide hineinpassen', () => {
        const box = { centerX: 0, centerY: 0, width: 200, height: 100 };
        // 800 / (200 * 1.12) = 3.571, 400 / (100 * 1.12) = 3.571: hier gleich.
        expect(orthographicZoom(box, 800, 400)).toBeCloseTo(800 / (200 * FRAME_MARGIN), 5);
        // Ein schmaleres Fenster: die Breite entscheidet.
        expect(orthographicZoom(box, 400, 400)).toBeCloseTo(400 / (200 * FRAME_MARGIN), 5);
        // Ein niedrigeres Fenster: die Hoehe entscheidet.
        expect(orthographicZoom(box, 800, 200)).toBeCloseTo(200 / (100 * FRAME_MARGIN), 5);
    });

    it('laesst den Rand des Rahmens stehen', () => {
        const box = { centerX: 0, centerY: 0, width: 200, height: 100 };
        const zoom = orthographicZoom(box, 800, 400);
        // Bei diesem Zoom nimmt das Rechteck genau den Bruchteil 1/Rand des
        // Bildes ein, also bleibt ringsum Platz.
        expect((box.width * zoom) / 800).toBeCloseTo(1 / FRAME_MARGIN, 5);
    });

    it('antwortet auf unsinnige Eingaben mit der Untergrenze statt mit NaN', () => {
        const box = { centerX: 0, centerY: 0, width: 200, height: 100 };
        expect(orthographicZoom(box, 0, 400)).toBe(FLAT_MIN_ZOOM);
        expect(orthographicZoom(box, 800, 0)).toBe(FLAT_MIN_ZOOM);
        expect(orthographicZoom({ centerX: 0, centerY: 0, width: 0, height: 0 }, 800, 400))
            .toBe(FLAT_MIN_ZOOM);
        expect(orthographicZoom(box, Number.NaN, Number.NaN)).toBe(FLAT_MIN_ZOOM);
    });
});

/*
 * Die Ausrichtung (W10b, AC5).
 *
 * Der Befund, aus dem dieser Teil kommt, war wieder ein Bild: "mich nervt, dass
 * die Galaxy manchmal einfach flach ist" (Nutzer, 2026-08-30). Eine Wolke, die
 * in einer Richtung duenn ist, zeigt einer Kamera in genau dieser Richtung einen
 * Strich. Was ein Test hier beweisen kann und ein Screenshot nicht, ist die
 * Umkehrung fuer JEDEN Punkt: bei der gerechneten Lage liegt keiner ausserhalb
 * des Bildes, und zwar unabhaengig davon, wie die Wolke im Raum liegt und wie
 * viele Punkte sie hat.
 */

/** Die Bildkoordinaten eines Punktes, gerechnet wie eine perspektivische Kamera. */
function project(
    point: Vector3Like,
    fit: { eye: Vector3Like; center: Vector3Like; up: Vector3Like; normal: Vector3Like },
    fovDegrees: number,
    aspect: number,
): { x: number; y: number; z: number } {
    const sub = (a: Vector3Like, b: Vector3Like): Vector3Like =>
        ({ x: a.x - b.x, y: a.y - b.y, z: a.z - b.z });
    const dot = (a: Vector3Like, b: Vector3Like): number => a.x * b.x + a.y * b.y + a.z * b.z;
    const cross = (a: Vector3Like, b: Vector3Like): Vector3Like => ({
        x: a.y * b.z - a.z * b.y,
        y: a.z * b.x - a.x * b.z,
        z: a.x * b.y - a.y * b.x,
    });
    // Dieselbe Basis, die three aus lookAt baut: z zurueck zur Kamera, x =
    // up x z, y = z x x.
    const zAxis = fit.normal;
    const xAxis = cross(fit.up, zAxis);
    const yAxis = cross(zAxis, xAxis);
    const d = sub(point, fit.eye);
    const depth = -dot(d, zAxis);
    const half = Math.tan((fovDegrees * Math.PI) / 360);
    return {
        x: dot(d, xAxis) / (depth * half * aspect),
        y: dot(d, yAxis) / (depth * half),
        z: depth,
    };
}

/** Eine Wolke mit vorgegebener Ausdehnung, gedreht, deterministisch erzeugt. */
function cloud(count: number, extents: [number, number, number], seed = 7): Vector3Like[] {
    let state = seed;
    const random = (): number => {
        state = (state * 1103515245 + 12345) % 2147483648;
        return state / 2147483648;
    };
    // Eine feste Drehung um alle drei Achsen: die Wolke soll NICHT achsenparallel
    // liegen, sonst faende die Rechnung ihre Achsen zufaellig richtig.
    const angle = 0.6;
    const c = Math.cos(angle);
    const s = Math.sin(angle);
    const points: Vector3Like[] = [];
    for (let i = 0; i < count; i += 1) {
        const a = (random() - 0.5) * extents[0];
        const b = (random() - 0.5) * extents[1];
        const g = (random() - 0.5) * extents[2];
        // um z, dann um x
        const x1 = a * c - b * s;
        const y1 = a * s + b * c;
        const z1 = g;
        points.push({ x: x1, y: y1 * c - z1 * s, z: y1 * s + z1 * c });
    }
    return points;
}

describe('principalFrame', () => {
    it('findet die weiteste und die duennste Richtung einer gedrehten Wolke', () => {
        const frame = principalFrame(cloud(2000, [1200, 600, 80]));
        expect(frame.counted).toBe(2000);
        expect(frame.extents[0]).toBeGreaterThan(frame.extents[1]);
        expect(frame.extents[1]).toBeGreaterThan(frame.extents[2]);
        // Die Groessenordnungen der Ausdehnungen, nicht ihre exakten Werte: eine
        // Zufallswolke fuellt ihren Kasten nicht bis auf den letzten Punkt.
        expect(frame.extents[0]).toBeGreaterThan(1000);
        expect(frame.extents[2]).toBeLessThan(200);
    });

    it('steht senkrecht aufeinander und ist rechtshaendig', () => {
        const { axes } = principalFrame(cloud(500, [900, 400, 120], 13));
        const dot = (a: Vector3Like, b: Vector3Like): number => a.x * b.x + a.y * b.y + a.z * b.z;
        expect(dot(axes[0], axes[1])).toBeCloseTo(0, 6);
        expect(dot(axes[0], axes[2])).toBeCloseTo(0, 6);
        expect(dot(axes[1], axes[2])).toBeCloseTo(0, 6);
        const cross = {
            x: axes[0].y * axes[1].z - axes[0].z * axes[1].y,
            y: axes[0].z * axes[1].x - axes[0].x * axes[1].z,
            z: axes[0].x * axes[1].y - axes[0].y * axes[1].x,
        };
        expect(dot(cross, axes[2])).toBeCloseTo(1, 6);
    });

    it('gibt fuer dieselbe Wolke zweimal dasselbe Dreibein', () => {
        const points = cloud(300, [500, 300, 90], 3);
        expect(principalFrame(points)).toEqual(principalFrame([...points]));
    });

    it('laesst eine flache Zeichnung flach: die duennste Achse ist z', () => {
        const flat = [
            { x: 0, y: 0, z: 0 },
            { x: 120, y: 36, z: 0 },
            { x: 240, y: -36, z: 0 },
            { x: 120, y: -72, z: 0 },
        ];
        const frame = principalFrame(flat);
        expect(frame.extents[2]).toBeCloseTo(0, 9);
        expect(Math.abs(frame.axes[2].z)).toBeCloseTo(1, 9);
    });

    it('antwortet auf nichts mit den Weltachsen statt mit NaN', () => {
        const empty = principalFrame([]);
        expect(empty.counted).toBe(0);
        expect(empty.extents).toEqual([0, 0, 0]);
        expect(empty.axes[2]).toEqual({ x: 0, y: 0, z: 1 });
        expect(principalFrame([{ x: Number.NaN, y: 0, z: 0 }]).counted).toBe(0);
    });
});

describe('fitCamera', () => {
    const ASPECT = 1.42;

    /** Die Zusicherung aus AC5, ueber jeden einzelnen Punkt. */
    const everyPointInside = (points: Vector3Like[], aspect = ASPECT): number => {
        const fit = fitCamera(points, FOV, aspect);
        expect(fit).not.toBeNull();
        let worst = 1;
        for (const point of points) {
            const seen = project(point, fit!, FOV, aspect);
            expect(seen.z).toBeGreaterThan(0);
            expect(Math.abs(seen.x)).toBeLessThanOrEqual(1);
            expect(Math.abs(seen.y)).toBeLessThanOrEqual(1);
            worst = Math.min(worst, 1 - Math.max(Math.abs(seen.x), Math.abs(seen.y)));
        }
        return worst;
    };

    it('haelt jeden Knoten im Bild, mit Rand, bei 76 Knoten', () => {
        expect(everyPointInside(cloud(76, [900, 520, 140], 11))).toBeGreaterThan(0.02);
    });

    it('haelt jeden Knoten im Bild, mit Rand, bei 5000 Knoten', () => {
        // Die Groesse der Vorschau, in der der Nutzer sein eigenes Projekt
        // indiziert hat (5000 Knoten). Der Deckel des Panels ist dieselbe Zahl.
        expect(everyPointInside(cloud(5000, [2400, 1500, 380], 23))).toBeGreaterThan(0.02);
    });

    it('haelt jeden Knoten im Bild, auch in einem schmalen und in einem breiten Panel', () => {
        const points = cloud(800, [1400, 900, 260], 5);
        expect(everyPointInside(points, 0.6)).toBeGreaterThan(0.02);
        expect(everyPointInside(points, 3.1)).toBeGreaterThan(0.02);
    });

    it('sieht die duennste Richtung entlang, also auf die groesste Flaeche', () => {
        const points = cloud(1200, [1600, 700, 60], 17);
        const frame = principalFrame(points);
        const fit = fitCamera(points, FOV, ASPECT)!;
        const dot = (a: Vector3Like, b: Vector3Like): number => a.x * b.x + a.y * b.y + a.z * b.z;
        expect(Math.abs(dot(fit.normal, frame.axes[2]))).toBeCloseTo(1, 6);
        // Und die Oben-Richtung ist die zweitweiteste: sonst laege die lange
        // Achse der Wolke senkrecht im Bild und braeuchte mehr Abstand.
        expect(Math.abs(dot(fit.up, frame.axes[1]))).toBeCloseTo(1, 6);
        expect(fit.width).toBeGreaterThan(fit.height);
        expect(fit.height).toBeGreaterThan(fit.depth);
    });

    it('legt die halbe Tiefe auf den Abstand, damit auch die vordere Haelfte passt', () => {
        const points = cloud(400, [800, 800, 600], 29);
        const fit = fitCamera(points, FOV, ASPECT)!;
        const planar = frameDistance(
            { centerX: 0, centerY: 0, width: fit.width, height: fit.height },
            FOV,
            ASPECT,
        );
        expect(fit.distance).toBeCloseTo(planar + fit.depth / 2, 6);
    });

    it('antwortet ohne Punkte mit nichts statt mit einer Kamera im Nirgendwo', () => {
        expect(fitCamera([], FOV, ASPECT)).toBeNull();
        expect(fitCamera([{ x: Number.NaN, y: Number.NaN, z: Number.NaN }], FOV, ASPECT)).toBeNull();
    });

    it('steht bei einem einzigen Punkt an der Untergrenze und sieht ihn an', () => {
        const fit = fitCamera([{ x: 40, y: -12, z: 5 }], FOV, ASPECT)!;
        expect(fit.distance).toBe(FRAME_MIN_DISTANCE);
        expect(fit.center).toEqual({ x: 40, y: -12, z: 5 });
    });
});
