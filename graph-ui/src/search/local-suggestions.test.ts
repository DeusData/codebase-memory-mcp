/**
 * Die Vorschlaege aus dem schon Geladenen, ohne Server.
 *
 * Zwei Zusicherungen tragen den ganzen Zyklus an dieser Stelle: sie kosten
 * keinen Serverweg, und sie sind mit DERSELBEN Funktion geordnet wie die
 * Antwort des Index. Die zweite ist die wichtigere: eine eigene Ordnung fuer
 * die vorlaeufigen Zeilen waere die teuerste Art, den Sprung zu bauen, den
 * dieser Zyklus abschaffen soll.
 */

import { describe, expect, it } from 'vitest';

import type { SymbolSearchHit } from '../core/intelligence-provider';
import {
    EMPTY_LOCAL_INDEX,
    fileCandidates,
    fileStem,
    localCandidates,
    localSuggestions,
} from './local-suggestions';
import { rankHits } from './semantic-search';

const SYMBOLS: SymbolSearchHit[] = [
    { name: 'createUser', qualifiedName: 'a.userService.createUser', kind: 'function', filePath: 'src/services/userService.ts', line: 23 },
    { name: 'validateUser', qualifiedName: 'a.validate.validateUser', kind: 'function', filePath: 'src/util/validate.ts', line: 8 },
    { name: 'insert', qualifiedName: 'a.db.insert', kind: 'function', filePath: 'src/repo/db.ts', line: 4 },
];

const FILES = ['src/services/userService.ts', 'src/repo/db.ts', 'src/routes/users.ts'];

const index = { symbols: SYMBOLS, files: FILES };

describe('fileStem', () => {

    it('nimmt der Datei die Endung, weil niemand die Endung tippt', () => {
        expect(fileStem('src/services/userService.ts')).toBe('userService');
        expect(fileStem('README')).toBe('README');
        expect(fileStem('src/a.b.c.ts')).toBe('a.b.c');
    });

    it('laesst einen Namen, der mit einem Punkt beginnt, ganz', () => {
        expect(fileStem('.gitignore')).toBe('.gitignore');
    });
});

describe('fileCandidates', () => {

    it('macht aus einem Pfad einen Kandidaten, den die Rangfolge nicht wegwirft', () => {
        // isNavigable wirft weg, was nach seiner eigenen Datei heisst. Genau
        // deshalb traegt ein Dateikandidat den Stamm und nicht den Basisnamen.
        const candidates = fileCandidates(['src/services/userService.ts']);
        expect(candidates).toHaveLength(1);
        expect(candidates[0].name).toBe('userService');
        expect(candidates[0].filePath).toBe('src/services/userService.ts');
        expect(rankHits(candidates, 'userService')).toHaveLength(1);
    });

    it('zeigt denselben Pfad nur einmal', () => {
        expect(fileCandidates(['src/a.ts', 'src/a.ts'])).toHaveLength(1);
    });
});

describe('localSuggestions', () => {

    it('beantwortet ein Wort ohne einen einzigen Aufruf nach draussen', () => {
        const suggestions = localSuggestions(index, 'validateUser');
        expect(suggestions[0]?.hit.name).toBe('validateUser');
    });

    it('nimmt die Datei mit, wenn der Leser sie meint', () => {
        const names = localSuggestions(index, 'users').map((entry) => entry.hit.filePath);
        expect(names).toContain('src/routes/users.ts');
    });

    it('ordnet mit derselben Funktion wie die Antwort des Index', () => {
        const own = localSuggestions(index, 'user').map((entry) => entry.hit.name);
        const same = rankHits(localCandidates(index), 'user')
            .slice(0, own.length)
            .map((entry) => entry.hit.name);
        expect(own).toEqual(same);
    });

    it('erfindet nichts, wenn nichts geladen ist', () => {
        expect(localSuggestions(EMPTY_LOCAL_INDEX, 'user')).toEqual([]);
    });

    it('erfindet auch dann nichts, wenn geladen ist, aber nichts passt', () => {
        expect(localSuggestions(index, 'zzzzzz')).toEqual([]);
    });

    it('haelt sich an den Deckel, damit das Fenster nicht laenger wird als sein Kasten', () => {
        const many = Array.from({ length: 40 }, (_, i) => `src/user${i}.ts`);
        expect(localSuggestions({ symbols: [], files: many }, 'user', () => 0, 10)).toHaveLength(10);
    });
});
