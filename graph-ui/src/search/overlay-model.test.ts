/*
 * Das Modell des Suchfensters: Zeilen, Kopfzeile, Tasten, Auswahl.
 *
 * Alles hier ist die Sorte Rechnung, die man beim Klicken erst bemerkt, wenn
 * sie falsch ist: eine Auswahl, die am Rand ueberlaeuft, eine Kopfzeile, die
 * zehn von zehn sagt, wo es dreissig waren, oder ein Escape, das im Fenster
 * gefangen bleibt.
 */

import { describe, expect, it } from 'vitest';

import {
    MAX_SEARCH_ROWS,
    isSearchable,
    moveSelection,
    overlayIntent,
    searchHeadline,
    searchRows,
} from './overlay-model';
import type { RankedHit } from './semantic-search';

function ranked(name: string, filePath?: string, line?: number, matched: string[] = ['user']): RankedHit {
    return {
        hit: {
            name,
            kind: 'function',
            qualifiedName: `atlas.${name}`,
            ...(filePath === undefined ? {} : { filePath }),
            ...(line === undefined ? {} : { line }),
        },
        score: 10,
        matched,
        fanIn: 0,
    };
}

describe('searchRows', () => {

    it('macht aus einem Treffer eine Zeile mit Name, Pfad, Zeile und Begruendung', () => {
        const [row] = searchRows([
            ranked('createUser', 'src/services/userService.ts', 23, ['create', 'user']),
        ]);
        expect(row.name).toBe('createUser');
        expect(row.path).toBe('src/services/userService.ts');
        expect(row.line).toBe('L23');
        expect(row.matched).toBe('create + user');
        expect(row.key).toBe('atlas.createUser');
    });

    it('laesst leer, was der Treffer nicht traegt', () => {
        const [row] = searchRows([ranked('createUser')]);
        expect(row.path).toBe('');
        expect(row.line).toBe('');
    });

    it('kappt auf zehn Zeilen', () => {
        const many = Array.from({ length: 30 }, (_, index) => ranked(`n${index}`));
        expect(searchRows(many)).toHaveLength(MAX_SEARCH_ROWS);
        expect(searchRows(many, 3)).toHaveLength(3);
    });
});

describe('searchHeadline', () => {

    it('sagt, wie viele es waren und wie viele davon zu sehen sind', () => {
        expect(searchHeadline('user', 30, 10)).toBe('30 hits for "user", top 10');
        expect(searchHeadline('user', 4, 4)).toBe('4 hits for "user"');
        expect(searchHeadline('user', 1, 1)).toBe('1 hit for "user"');
    });

    it('sagt bei nichts, dass nichts antwortet, statt zu schweigen', () => {
        expect(searchHeadline('zzz', 0, 0)).toBe('no symbol answers "zzz"');
    });
});

describe('overlayIntent', () => {

    it('kennt genau die vier Tasten des Fensters', () => {
        expect(overlayIntent('ArrowUp')).toBe('up');
        expect(overlayIntent('ArrowDown')).toBe('down');
        expect(overlayIntent('Enter')).toBe('choose');
        expect(overlayIntent('Escape')).toBe('close');
    });

    it('laesst jeden Buchstaben durch, damit man weitertippen kann', () => {
        for (const key of ['a', 'U', ' ', 'Backspace', 'ArrowLeft', 'Home']) {
            expect(overlayIntent(key)).toBe('none');
        }
    });
});

describe('moveSelection', () => {

    it('bleibt am Rand stehen, statt umzulaufen', () => {
        expect(moveSelection(3, 0, -1)).toBe(0);
        expect(moveSelection(3, 2, 1)).toBe(2);
    });

    it('bewegt sich sonst um genau einen Schritt', () => {
        expect(moveSelection(3, 0, 1)).toBe(1);
        expect(moveSelection(3, 2, -1)).toBe(1);
    });

    it('bleibt bei einer leeren Liste bei null', () => {
        expect(moveSelection(0, 0, 1)).toBe(0);
    });
});

describe('isSearchable', () => {

    it('braucht zwei Zeichen, und Leerzeichen zaehlen nicht', () => {
        expect(isSearchable('a', 2)).toBe(false);
        expect(isSearchable(' a ', 2)).toBe(false);
        expect(isSearchable('us', 2)).toBe(true);
        expect(isSearchable('  ', 2)).toBe(false);
    });
});
