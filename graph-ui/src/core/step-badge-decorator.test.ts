/*
 * Die Numerierungsregel der Rand-Badges, ohne Editor.
 *
 * Sie ist die einzige Stelle, die entscheidet, welcher Aufruf Schritt drei ist,
 * und der Twin listet dieselben Schritte in derselben Reihenfolge. Waeren die
 * beiden uneinig, waere das Badge im Rand eine Zahl, die auf die falsche Zeile
 * zeigt, und ein Leser haette keinen Weg, das zu merken.
 */

import { describe, expect, it } from 'vitest';

import { MAX_STEP_BADGES, badgesForLines } from './step-badge-decorator';
import { CREATE_USER_IR } from '../test-support/twin-fixtures';

describe('badgesForLines', () => {
    it('numeriert in der Reihenfolge der Schritte und sortiert nach Zeile', () => {
        const badges = badgesForLines([{ line: 30 }, { line: 10 }, { line: 20 }]);
        expect(badges.map((badge) => [badge.line, badge.ordinal])).toEqual([
            [10, 2],
            [20, 3],
            [30, 1],
        ]);
    });

    it('gibt einer Zeile genau ein Badge, und der fruehere Schritt gewinnt', () => {
        const badges = badgesForLines([{ line: 29, label: 'UserEntity' }, { line: 29, label: 'listUsers' }]);
        expect(badges).toHaveLength(1);
        expect(badges[0].ordinal).toBe(1);
        expect(badges[0].label).toBe('UserEntity');
    });

    it('hoert bei neun auf, statt eine falsche oder abgeschnittene Zahl zu zeigen', () => {
        const sites = Array.from({ length: 12 }, (_unused, index) => ({ line: index + 1 }));
        const badges = badgesForLines(sites);
        expect(badges).toHaveLength(MAX_STEP_BADGES);
        expect(Math.max(...badges.map((badge) => badge.ordinal))).toBe(MAX_STEP_BADGES);
        expect(badges.map((badge) => badge.line)).not.toContain(10);
    });

    it('deckelt nach der Position im Schritt-Modell, nicht nach der Zahl der Badges', () => {
        // Neun Schritte auf einer Zeile plus ein zehnter woanders: der zehnte
        // liegt hinter dem Deckel und bekommt kein Badge, obwohl bisher erst
        // eines gezeichnet wurde. Alles andere waere eine Zahl, die nicht mehr
        // die Position im Twin ist.
        const sites = [...Array.from({ length: 9 }, () => ({ line: 5 })), { line: 40 }];
        const badges = badgesForLines(sites);
        expect(badges.map((badge) => badge.line)).toEqual([5]);
    });

    it('laesst eine Aufrufstelle ohne Zeile weg, statt sie auf Zeile eins zu legen', () => {
        const badges = badgesForLines([{ label: 'nirgends' }, { line: 7, label: 'hier' }]);
        expect(badges.map((badge) => badge.line)).toEqual([7]);
        expect(badges[0].ordinal).toBe(2);
    });

    it('verwirft Zeilen unter eins, weil Graph-Zeilen 1-basiert sind', () => {
        expect(badgesForLines([{ line: 0 }, { line: -3 }])).toEqual([]);
    });

    it('gibt fuer eine leere Schrittliste eine leere Menge und keinen Fehler', () => {
        expect(badgesForLines([])).toEqual([]);
    });

    it('numeriert die aufgezeichneten Schritte von createUser wie der Twin sie listet', () => {
        const steps = CREATE_USER_IR.steps.value.map((call) => ({
            line: call.line,
            label: call.targetName,
        }));
        const badges = badgesForLines(steps);
        // Zwei Aufrufe stehen auf Zeile 29 (`new UserEntity(...)` und
        // `listUsers()` im selben Ausdruck), also fuenf Badges fuer sechs
        // Schritte, und Zeile 29 traegt die kleinere Nummer.
        expect(badges.map((badge) => badge.line)).toEqual([24, 27, 29, 30, 35]);
        expect(badges.find((badge) => badge.line === 29)?.label).toBe('UserEntity');
        expect(badges[0]).toEqual({ line: 24, ordinal: 1, label: 'validateUser' });
    });
});
