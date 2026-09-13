/**
 * Die Rechnung hinter dem eigenen Tooltip, ohne Browser.
 *
 * Der Fall, der diesen Zyklus ausgeloest hat, steht als erster Test da: der
 * Screenshot des Nutzers vom 2026-08-29 zeigte den Tooltip der Kopfzeile ueber
 * dem Detail-Regler und ueber den Chips darunter. Genau diese Lage darf die
 * Rechnung nicht mehr waehlen.
 */

import { describe, expect, it } from 'vitest';

import {
    HINT_PROTECTED,
    explainsMore,
    overlapArea,
    placeHint,
} from './tooltip-model';

const viewport = { width: 1680, height: 1050 };

describe('placeHint', () => {
    it('stellt den Kasten unter den Ausloeser, wenn dort nichts Geschuetztes liegt', () => {
        const placed = placeHint({
            anchor: { x: 100, y: 100, width: 80, height: 20 },
            size: { width: 200, height: 40 },
            viewport,
            protect: [],
        });
        expect(placed.side).toBe('below');
        expect(placed.covered).toBe(0);
        expect(placed.fits).toBe(true);
        expect(placed.y).toBeGreaterThan(120);
    });

    it('verdeckt nie seinen eigenen Ausloeser', () => {
        for (const side of ['below', 'above', 'right', 'left'] as const) {
            void side;
        }
        const anchor = { x: 400, y: 400, width: 120, height: 24 };
        const placed = placeHint({ anchor, size: { width: 300, height: 60 }, viewport, protect: [] });
        expect(overlapArea({ ...anchor }, {
            x: placed.x,
            y: placed.y,
            width: 300,
            height: 60,
        })).toBe(0);
    });

    /*
     * Der Befund vom 2026-08-29, als Zahlen.
     *
     * Die Kopfzeile des Graph-Panels liegt oben in der rechten Spalte, der
     * Detail-Regler und die Chips liegen unmittelbar darunter. "Unter dem
     * Ausloeser" ist damit besetzt, und die Rechnung muss ausweichen statt
     * darueberzulegen.
     */
    it('weicht aus, wenn unter dem Ausloeser ein Regler steht', () => {
        const anchor = { x: 1240, y: 120, width: 120, height: 18 };
        const slider = { x: 1240, y: 150, width: 400, height: 90 };
        const placed = placeHint({
            anchor,
            size: { width: 300, height: 70 },
            viewport,
            protect: [slider],
        });
        expect(placed.covered).toBe(0);
        expect(overlapArea(slider, { x: placed.x, y: placed.y, width: 300, height: 70 })).toBe(0);
    });

    it('bleibt im Fenster, auch wenn der Ausloeser am Rand steht', () => {
        const placed = placeHint({
            anchor: { x: 1660, y: 1030, width: 16, height: 16 },
            size: { width: 300, height: 70 },
            viewport,
            protect: [],
        });
        expect(placed.x).toBeGreaterThanOrEqual(0);
        expect(placed.y).toBeGreaterThanOrEqual(0);
        expect(placed.x + 300).toBeLessThanOrEqual(viewport.width);
        expect(placed.y + 70).toBeLessThanOrEqual(viewport.height);
    });

    /*
     * Total: gibt es gar keine freie Lage, kommt trotzdem eine heraus, und sie
     * sagt mit `covered`, was sie kostet. Eine Rechnung, die in diesem Fall
     * nichts zurueckgibt, waere eine Oberflaeche ohne Tooltip an genau der
     * Stelle, an der jemand ihn braucht.
     */
    it('waehlt die guenstigste Lage, wenn keine ganz frei ist', () => {
        const anchor = { x: 800, y: 500, width: 40, height: 20 };
        const everywhere = { x: 0, y: 0, width: viewport.width, height: viewport.height };
        const placed = placeHint({
            anchor,
            size: { width: 200, height: 40 },
            viewport,
            protect: [everywhere],
        });
        expect(placed.covered).toBeGreaterThan(0);
        expect(['below', 'above', 'right', 'left']).toContain(placed.side);
    });

    /*
     * Der Befund vom 2026-08-30 (`npm run smoke:w8b`): der Tooltip einer
     * Schrittzeile lag ueber der Kommandozeile. Der Ausloeser stand so weit
     * unten, dass jede der zwoelf Lagen an den unteren Fensterrand geklemmt
     * wurde, und dort steht die Kommandozeile. Zehn Pixel darueber war frei.
     */
    it('schiebt den Kasten aus der Kommandozeile, statt ihn an den Rand zu klemmen', () => {
        const commandLine = { x: 0, y: 1010, width: viewport.width, height: 40 };
        const placed = placeHint({
            anchor: { x: 700, y: 1020, width: 100, height: 20 },
            size: { width: 228, height: 27 },
            viewport,
            protect: [commandLine],
        });
        expect(placed.covered).toBe(0);
        expect(overlapArea(
            { x: placed.x, y: placed.y, width: 228, height: 27 },
            commandLine,
        )).toBe(0);
        expect(placed.y + 27).toBeLessThanOrEqual(commandLine.y);
    });

    /*
     * Geschoben wird nur, wenn es ganz frei wird. Ein Kasten, der von einem
     * Hindernis auf das naechste rutscht, waere derselbe Fehler mit anderen
     * Zahlen; dann bleibt die guenstigste Lage stehen und sagt in `covered`,
     * was sie kostet.
     */
    it('bleibt stehen, wenn daneben auch kein Platz ist', () => {
        const everywhere = { x: 0, y: 0, width: viewport.width, height: viewport.height };
        const placed = placeHint({
            anchor: { x: 800, y: 500, width: 40, height: 20 },
            size: { width: 200, height: 40 },
            viewport,
            protect: [everywhere],
        });
        expect(placed.covered).toBeGreaterThan(0);
    });

    it('laesst die freien Lagen unangetastet', () => {
        const placed = placeHint({
            anchor: { x: 100, y: 100, width: 80, height: 20 },
            size: { width: 200, height: 40 },
            viewport,
            protect: [{ x: 900, y: 900, width: 100, height: 100 }],
        });
        expect(placed.covered).toBe(0);
        expect(placed.x).toBe(100);
        expect(placed.y).toBe(126);
    });

    it('nennt seine geschuetzten Flaechen als Daten, mit Grund', () => {
        expect(HINT_PROTECTED.length).toBeGreaterThanOrEqual(2);
        for (const entry of HINT_PROTECTED) {
            expect(entry.selector.length).toBeGreaterThan(0);
            expect(entry.reason.length).toBeGreaterThan(20);
        }
    });
});

describe('explainsMore', () => {
    it('erkennt eine blosse Wiederholung des sichtbaren Textes', () => {
        expect(explainsMore('GALAXY', 'GALAXY')).toBe(false);
        expect(explainsMore('galaxy', 'GALAXY')).toBe(false);
        expect(explainsMore('76 nodes', 'the graph shows 76 nodes today')).toBe(false);
    });

    it('erkennt einen Satz, der mehr sagt als das Etikett', () => {
        expect(explainsMore('hide the graph panel: the header stays', 'collapse galaxy')).toBe(true);
        expect(explainsMore('src/services/userService.ts', 'userService.ts')).toBe(true);
    });

    it('haelt einen leeren Satz fuer keine Erklaerung', () => {
        expect(explainsMore('', 'anything')).toBe(false);
        expect(explainsMore('   ', 'anything')).toBe(false);
    });
});
