/**
 * Die Deckkraft einer Kante, als Kurve ueber die Kantenzahl (W9).
 *
 * Der Befund, gegen den sie gebaut ist, ist ein Bild: bei 178 Kanten sahen
 * zwoelf Farben aus wie eine (Martin, 2026-08-29). Ob das Bild jetzt anders
 * aussieht, misst tools/smoke-w9.mjs an gerenderten Pixeln. Hier steht die
 * andere Haelfte der Zusicherung, und die ist an einem Screenshot nicht zu
 * sehen: dass ein grosses Repository durch diese Aenderung NICHT heller wird.
 * Genau daran haengt, ob die Kurve eine Verbesserung ist oder nur eine
 * Verschiebung des Problems.
 */

import { describe, expect, it } from 'vitest';

import {
    EDGE_FULL_COUNT,
    EDGE_INTENSITY_FAR,
    EDGE_INTENSITY_NEAR,
    EDGE_WASH_COUNT,
    PORTED_INTENSITY_FAR,
    PORTED_INTENSITY_NEAR,
    edgeIntensityFor,
} from './EdgeLines';
import { edgeIntensityScale } from './density';

/** Die Rechnung der Uebernahme, Zeichen fuer Zeichen. */
const ported = (sameCluster: boolean, count: number): number =>
    (sameCluster ? PORTED_INTENSITY_NEAR : PORTED_INTENSITY_FAR) * edgeIntensityScale(count);

describe('edgeIntensityFor', () => {

    it('zeichnet ein kleines Projekt deutlich, statt es zu verschleiern', () => {
        // Das Demo-Fixture: 178 Kanten.
        expect(edgeIntensityFor(true, 178)).toBe(EDGE_INTENSITY_NEAR);
        expect(edgeIntensityFor(false, 178)).toBe(EDGE_INTENSITY_FAR);
        // Und das ist der Punkt: deutlich mehr als vorher.
        expect(edgeIntensityFor(true, 178)).toBeGreaterThan(ported(true, 178) * 2);
        expect(edgeIntensityFor(false, 178)).toBeGreaterThan(ported(false, 178) * 4);
    });

    it('haelt den vollen Wert bis zur Vollgrenze', () => {
        expect(edgeIntensityFor(true, EDGE_FULL_COUNT)).toBe(EDGE_INTENSITY_NEAR);
        expect(edgeIntensityFor(false, EDGE_FULL_COUNT)).toBe(EDGE_INTENSITY_FAR);
        expect(edgeIntensityFor(true, 0)).toBe(EDGE_INTENSITY_NEAR);
    });

    it('rechnet ab der Schleiergrenze genau wie die Uebernahme', () => {
        for (const count of [EDGE_WASH_COUNT, 45000, 120000, 400000]) {
            expect(edgeIntensityFor(true, count)).toBeCloseTo(ported(true, count), 10);
            expect(edgeIntensityFor(false, count)).toBeCloseTo(ported(false, count), 10);
        }
    });

    it('wird zwischen den Grenzen nirgends heller als vorher plus die Haelfte', () => {
        /*
         * Die eine Zahl, an der die ganze Aenderung haengt: zwischen 2000 und
         * 30000 Kanten liegt die neue Kurve ueber der alten (das ist der Sinn),
         * aber sie laeuft monoton auf sie zu und trifft sie bei 30000. Ein
         * Ausreisser nach oben in der Mitte waere eine Wand, die dieser Zyklus
         * gerade verhindern soll.
         */
        for (const count of [2500, 4000, 8000, 16000, 25000, 29999]) {
            const near = edgeIntensityFor(true, count);
            const far = edgeIntensityFor(false, count);
            expect(near).toBeGreaterThan(ported(true, count));
            expect(far).toBeGreaterThan(ported(false, count));
            expect(near).toBeLessThanOrEqual(EDGE_INTENSITY_NEAR);
            expect(far).toBeLessThanOrEqual(EDGE_INTENSITY_FAR);
        }
        // Bei 8000 Kanten: 0.206 statt 0.140 innerhalb, 0.070 statt 0.034 aussen.
        expect(edgeIntensityFor(true, 8000)).toBeCloseTo(0.206, 3);
        expect(edgeIntensityFor(false, 8000)).toBeCloseTo(0.070, 3);
    });

    it('faellt monoton, damit das Bild an keiner Stelle springt', () => {
        let previousNear = Number.POSITIVE_INFINITY;
        let previousFar = Number.POSITIVE_INFINITY;
        for (let count = 1; count <= 200000; count = Math.ceil(count * 1.07)) {
            const near = edgeIntensityFor(true, count);
            const far = edgeIntensityFor(false, count);
            expect(near).toBeLessThanOrEqual(previousNear + 1e-12);
            expect(far).toBeLessThanOrEqual(previousFar + 1e-12);
            previousNear = near;
            previousFar = far;
        }
    });

    it('zeichnet eine Kante innerhalb eines Clusters immer staerker als eine darueber hinaus', () => {
        for (const count of [1, 178, 2000, 8000, 30000, 120000]) {
            expect(edgeIntensityFor(true, count)).toBeGreaterThan(edgeIntensityFor(false, count));
        }
    });

    it('antwortet auf eine unbrauchbare Zahl mit der Rechnung der Uebernahme', () => {
        expect(edgeIntensityFor(true, Number.NaN)).toBeNaN();
        expect(edgeIntensityFor(true, Number.POSITIVE_INFINITY)).toBe(PORTED_INTENSITY_NEAR * 0.05);
    });
});
