/*
 * Was der Monaco-Adapter aus einer Badge-Menge macht.
 *
 * Geprueft wird hier die Uebersetzung und nicht die Numerierung: welche Zeile
 * welche Nummer bekommt, steht in src/core/step-badge-decorator.ts und hat dort
 * seine eigenen Tests. Was hier zaehlt, sind die vier Eigenschaften, an denen
 * die Vorlage haengt: Randklasse statt Inline-Klasse, numerierte Klasse fuer
 * die Ziffer, leere Spanne auf einer Zeile, und eine eigene Menge fuer die
 * Zeilen-Hervorhebung.
 */

import { describe, expect, it } from 'vitest';

import { badgesForLines } from '../core/step-badge-decorator';
import {
    NEVER_GROWS_WHEN_TYPING_AT_EDGES,
    STEP_LINE_HIGHLIGHT_CLASS,
    badgeDecorations,
    highlightDecorations,
    stepBadgeClasses,
} from './step-badges';

describe('stepBadgeClasses', () => {
    it('traegt die gemeinsame und die numerierte Klasse', () => {
        expect(stepBadgeClasses(3, false)).toBe('codeatlas-step-badge codeatlas-step-badge-3');
    });

    it('haengt den Puls an, wenn der Caret auf der Zeile steht', () => {
        expect(stepBadgeClasses(1, true)).toBe(
            'codeatlas-step-badge codeatlas-step-badge-1 codeatlas-step-badge-pulse',
        );
    });
});

describe('badgeDecorations', () => {
    const badges = badgesForLines([
        { line: 24, label: 'validateUser' },
        { line: 27, label: 'ValidationError' },
    ]);

    it('malt in den Rand und nicht in den Text', () => {
        const [first] = badgeDecorations(badges);
        expect(first.options.linesDecorationsClassName).toContain('codeatlas-step-badge');
        expect(first.options.className).toBeUndefined();
        expect(first.options.isWholeLine).toBeUndefined();
    });

    it('haelt die Spanne leer und auf einer Zeile', () => {
        for (const decoration of badgeDecorations(badges)) {
            expect(decoration.range.startLineNumber).toBe(decoration.range.endLineNumber);
            expect(decoration.range.startColumn).toBe(1);
            expect(decoration.range.endColumn).toBe(1);
        }
    });

    it('nimmt die Graph-Zeile direkt als Monaco-Zeilennummer', () => {
        expect(badgeDecorations(badges).map((decoration) => decoration.range.startLineNumber)).toEqual([
            24, 27,
        ]);
    });

    it('pulst genau das Badge auf der Caret-Zeile', () => {
        const decorated = badgeDecorations(badges, 27);
        expect(decorated[0].options.linesDecorationsClassName).not.toContain('pulse');
        expect(decorated[1].options.linesDecorationsClassName).toContain('codeatlas-step-badge-pulse');
    });

    it('pulst nichts, wenn der Caret auf keiner Aufrufstelle steht', () => {
        const decorated = badgeDecorations(badges, 25);
        for (const decoration of decorated) {
            expect(decoration.options.linesDecorationsClassName).not.toContain('pulse');
        }
    });

    it('haengt den Namen des Aufrufs an den Hover, wenn es einen gibt', () => {
        expect(badgeDecorations(badges)[0].options.hoverMessage).toEqual({
            value: 'Step 1: validateUser',
        });
        expect(badgeDecorations(badgesForLines([{ line: 4 }]))[0].options.hoverMessage).toBeUndefined();
    });

    it('setzt die Klebrigkeit, die die Vorlage setzt', () => {
        expect(badgeDecorations(badges)[0].options.stickiness).toBe(NEVER_GROWS_WHEN_TYPING_AT_EDGES);
    });
});

describe('highlightDecorations', () => {
    it('hebt genau eine Zeile hervor, ganz', () => {
        const [decoration] = highlightDecorations(19);
        expect(decoration.options.className).toBe(STEP_LINE_HIGHLIGHT_CLASS);
        expect(decoration.options.isWholeLine).toBe(true);
        expect(decoration.range.startLineNumber).toBe(19);
    });

    it('ist eine leere Menge, wenn nichts hervorzuheben ist', () => {
        expect(highlightDecorations(undefined)).toEqual([]);
        expect(highlightDecorations(0)).toEqual([]);
    });
});
