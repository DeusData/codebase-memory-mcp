/**
 * Die Rechnung hinter "vorher X, nachher Y", ohne einen einzigen gezeichneten
 * Pixel.
 *
 * Gemessen wird im Browser; WAS aus zwei Messungen folgt, ist reine Arithmetik
 * und gehoert deshalb hierher. Der Kern der Datei ist das Rauschband: ein
 * Vergleich ohne Schranke macht aus jeder Schwankung einen Effekt, und genau das
 * verbietet AC9.
 */

import { afterEach, describe, expect, it } from 'vitest';

import {
    FRAME_STALE_MS,
    frameHistory,
    frameRateOf,
    frameRateSnapshot,
    meanOf,
    noiseBandOf,
    recordFrameWindow,
    recordSceneFacts,
    resetFrameRate,
    subscribeFrameRate,
    verdictOf,
} from './frame-rate';

afterEach(() => {
    resetFrameRate();
});

describe('Bilder je Sekunde aus einem Fenster', () => {
    it('rechnet Bilder mal tausend durch die Fensterlaenge', () => {
        expect(frameRateOf(30, 500)).toBe(60);
        expect(frameRateOf(15, 500)).toBe(30);
    });

    it('ergibt bei einem sinnlosen Fenster 0 und nicht NaN', () => {
        expect(frameRateOf(30, 0)).toBe(0);
        expect(frameRateOf(0, 500)).toBe(0);
        expect(frameRateOf(Number.NaN, 500)).toBe(0);
        expect(frameRateOf(30, Number.NaN)).toBe(0);
    });
});

describe('das Rauschband', () => {
    it('ist die Spanne der Fenster', () => {
        expect(noiseBandOf([58, 60, 61])).toBe(3);
    });

    it('gibt es unter zwei Fenstern nicht', () => {
        // Und zwar ausdruecklich nicht als 0: eine Null waere die Behauptung,
        // diese Maschine messe exakt.
        expect(noiseBandOf([])).toBeUndefined();
        expect(noiseBandOf([60])).toBeUndefined();
    });
});

describe('das Urteil ueber zwei Messungen', () => {
    it('nennt einen Unterschied innerhalb des Rauschens keinen', () => {
        expect(verdictOf(60, 62, 3)).toBe('no-difference');
        expect(verdictOf(60, 58, 3)).toBe('no-difference');
        // Genau auf der Schranke ebenfalls: die Schranke gehoert zum Rauschen.
        expect(verdictOf(60, 63, 3)).toBe('no-difference');
    });

    it('nennt eine Richtung erst jenseits des Rauschens', () => {
        expect(verdictOf(30, 58, 3)).toBe('higher');
        expect(verdictOf(58, 30, 3)).toBe('lower');
    });

    it('urteilt ohne Band gar nicht', () => {
        expect(verdictOf(30, 58, undefined)).toBe('not-measured');
    });

    it('urteilt ohne Zahlen gar nicht', () => {
        expect(verdictOf(undefined, 58, 3)).toBe('not-measured');
        expect(verdictOf(58, undefined, 3)).toBe('not-measured');
        expect(verdictOf(0, 58, 3)).toBe('not-measured');
    });
});

describe('der Mittelwert', () => {
    it('mittelt', () => {
        expect(meanOf([30, 60])).toBe(45);
    });

    it('ist ohne Werte nichts und nicht null', () => {
        expect(meanOf([])).toBeUndefined();
    });
});

describe('die Naht globalThis.__atlasGalaxyPerf', () => {
    it('traegt dasselbe Fenster, das gemeldet wurde', () => {
        recordSceneFacts(76, 178, 30);
        const now = Date.now();
        recordFrameWindow(30, 500, now);
        const seam = globalThis.__atlasGalaxyPerf;
        expect(seam?.fps).toBe(60);
        expect(seam?.frames).toBe(30);
        expect(seam?.windowMs).toBe(500);
        expect(seam?.samples).toBe(1);
        expect(seam?.running).toBe(true);
        expect(seam?.nodes).toBe(76);
        expect(seam?.edges).toBe(178);
        expect(seam?.cap).toBe(30);
    });

    it('sagt nach einem alten Fenster, dass nichts laeuft, und nennt keine Bildrate', () => {
        const then = Date.now() - FRAME_STALE_MS - 1000;
        recordFrameWindow(30, 500, then);
        const seam = frameRateSnapshot(Date.now());
        expect(seam.running).toBe(false);
        // Kein stehengebliebener Wert: an einem stehenden Bild wird nichts
        // gemessen, und die letzte Zahl von vorhin waere eine Behauptung ueber
        // jetzt.
        expect(seam.fps).toBe(0);
    });

    it('hebt die letzten Fenster auf und rechnet ihre Spanne aus', () => {
        const now = Date.now();
        recordFrameWindow(30, 500, now);
        recordFrameWindow(29, 500, now + 500);
        const seam = frameRateSnapshot(now + 500);
        expect(seam.recent).toEqual([60, 58]);
        expect(seam.noiseBand).toBe(2);
    });

    it('meldet jedes Fenster an die Anmeldungen', () => {
        let calls = 0;
        const stop = subscribeFrameRate(() => {
            calls += 1;
        });
        recordFrameWindow(30, 500);
        recordFrameWindow(30, 500);
        stop();
        recordFrameWindow(30, 500);
        expect(calls).toBe(2);
        expect(frameHistory()).toHaveLength(3);
    });
});
