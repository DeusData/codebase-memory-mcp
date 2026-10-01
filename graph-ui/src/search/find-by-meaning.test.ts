/*
 * Die Beschaffung der Kandidaten, ohne Server.
 *
 * Geprueft wird die Regel, die den Unterschied zwischen "findet nichts" und
 * "findet, was gemeint war" ausmacht: eine Anfrage je Wort, die Vereinigung
 * der Antworten, und ein Wort, das die Engine nicht beantwortet, kostet die
 * anderen nichts.
 */

import { describe, expect, it, vi } from 'vitest';

import type { SymbolSearchHit } from '../core/intelligence-provider';
import { CANDIDATES_PER_TERM, SEARCH_DEBOUNCE_MS, findByMeaning, searchByMeaning } from './find-by-meaning';
import type { SymbolSearcher } from './find-by-meaning';

const INDEX: Record<string, SymbolSearchHit[]> = {
    create: [
        { name: 'create', kind: 'function', filePath: 'src/services/orderService.ts', qualifiedName: 'a.orderService.create' },
        { name: 'createUser', kind: 'function', filePath: 'src/services/userService.ts', qualifiedName: 'a.userService.createUser' },
    ],
    user: [
        { name: 'createUser', kind: 'function', filePath: 'src/services/userService.ts', qualifiedName: 'a.userService.createUser' },
        { name: 'toUser', kind: 'function', filePath: 'src/services/userService.ts', qualifiedName: 'a.userService.toUser' },
        { name: 'validateUser', kind: 'function', filePath: 'src/util/validate.ts', qualifiedName: 'a.validate.validateUser' },
    ],
};

function searcher(overrides: Partial<SymbolSearcher> = {}): SymbolSearcher & { calls: [string, number | undefined][] } {
    const calls: [string, number | undefined][] = [];
    return {
        calls,
        async searchSymbols(_root, pattern, limit) {
            calls.push([pattern, limit]);
            return INDEX[pattern] ?? [];
        },
        ...overrides,
    };
}

describe('findByMeaning', () => {

    it('fragt je Wort einmal und nicht einmal fuer die Phrase', async () => {
        const index = searcher();
        await findByMeaning(index, '/workspace', 'createUser');
        expect(index.calls.map((call) => call[0])).toEqual(['create', 'user']);
        expect(index.calls[0][1]).toBe(CANDIDATES_PER_TERM);
    });

    it('rankt die Vereinigung, also gewinnt, was beide Woerter beantwortet', async () => {
        const ranked = await findByMeaning(searcher(), '/workspace', 'createUser');
        expect(ranked[0]?.hit.name).toBe('createUser');
        expect(ranked[0]?.matched).toEqual(['create', 'user']);
        expect(ranked.map((entry) => entry.hit.name)).toContain('validateUser');
    });

    it('zeigt jeden Treffer genau einmal, auch wenn zwei Woerter ihn nennen', async () => {
        const names = (await findByMeaning(searcher(), '/workspace', 'createUser'))
            .map((entry) => entry.hit.name);
        expect(names.filter((name) => name === 'createUser')).toHaveLength(1);
    });

    it('laesst ein Wort, das die Engine nicht beantwortet, die anderen nicht mitnehmen', async () => {
        const failing = searcher({
            async searchSymbols(_root, pattern, _limit) {
                if (pattern === 'create') {
                    throw new Error('engine unavailable');
                }
                return INDEX[pattern] ?? [];
            },
        });
        const ranked = await findByMeaning(failing, '/workspace', 'createUser');
        expect(ranked.map((entry) => entry.hit.name)).toContain('createUser');
    });

    it('fragt bei einer Anfrage ohne Woerter gar nicht erst', async () => {
        const index = searcher();
        expect(await findByMeaning(index, '/workspace', '  a  ')).toEqual([]);
        expect(index.calls).toEqual([]);
    });

    it('reicht das Projekt durch, weil der Server ohne Projekt nichts findet', async () => {
        const searchSymbols = vi.fn(async () => []);
        await findByMeaning({ searchSymbols } as unknown as SymbolSearcher, '/workspace', 'user', {
            projectName: 'atlas-sample',
        });
        expect(searchSymbols).toHaveBeenCalledWith('/workspace', 'user', CANDIDATES_PER_TERM, {
            projectName: 'atlas-sample',
        });
    });

    /*
     * W7b, Nutzerbefund vom 2026-08-29: die Vorschlaege erscheinen zu langsam.
     * Die Entprellung war der groessere der beiden Anteile und ist deshalb eine
     * Zusicherung mit einer Zahl, nicht eine Einstellung.
     */
    it('laesst die Zeile hoechstens 100 ms still stehen, bevor gefragt wird', () => {
        expect(SEARCH_DEBOUNCE_MS).toBeLessThanOrEqual(100);
    });

    it('bricht einen Gleichstand mit dem Fan-in, das der Aufrufer mitbringt', async () => {
        const fanIn = new Map([['a.userService.toUser', 9]]);
        const ranked = await findByMeaning(
            searcher(),
            '/workspace',
            'user',
            {},
            (hit) => fanIn.get(hit.qualifiedName ?? '') ?? 0,
        );
        const names = ranked.map((entry) => entry.hit.name);
        expect(names.indexOf('toUser')).toBeLessThan(names.indexOf('createUser'));
    });
});

/*
 * Was der Praefix-Cache und der Abbruch von dieser Schicht brauchen (W7b).
 *
 * Beides sind Zusicherungen ueber Wartezeit, und beide koennen still falsch
 * sein: ein Cache, der eine abgeschnittene Runde fuer vollstaendig haelt,
 * verschluckt Treffer, und ein Abbruch, der nur so tut, laesst die Anfragen
 * trotzdem laufen und meldet ihre Ergebnisse.
 */
describe('searchByMeaning', () => {

    it('nennt eine Runde vollstaendig, solange kein Wort den Deckel erreicht hat', async () => {
        const answer = await searchByMeaning(searcher(), '/workspace', 'createUser');
        expect(answer.complete).toBe(true);
        expect(answer.candidates.length).toBeGreaterThan(0);
        expect(answer.aborted).toBe(false);
    });

    it('nennt eine Runde unvollstaendig, sobald ein Wort den Deckel ausschoepft', async () => {
        const full: SymbolSearchHit[] = Array.from({ length: CANDIDATES_PER_TERM }, (_, i) => ({
            name: `user${i}`,
            kind: 'function',
            filePath: `src/u${i}.ts`,
            qualifiedName: `a.u${i}`,
        }));
        const answer = await searchByMeaning(
            { async searchSymbols() { return full; } },
            '/workspace',
            'user',
        );
        // Der Server hat moeglicherweise abgeschnitten. Diese Runde darf ein
        // laengeres Wort nicht beantworten, weil niemand weiss, was fehlt.
        expect(answer.complete).toBe(false);
    });

    it('haelt die rohen Kandidaten fest, damit ein laengeres Wort sie neu ranken kann', async () => {
        const answer = await searchByMeaning(searcher(), '/workspace', 'createUser');
        expect(answer.candidates.map((hit) => hit.name)).toContain('validateUser');
    });

    it('bricht ab, statt die restlichen Woerter noch zu fragen', async () => {
        const controller = new AbortController();
        const calls: string[] = [];
        const answer = await searchByMeaning(
            {
                async searchSymbols(_root, pattern) {
                    calls.push(pattern);
                    controller.abort();
                    return INDEX[pattern] ?? [];
                },
            },
            '/workspace',
            'createUser',
            { signal: controller.signal },
        );
        expect(calls).toEqual(['create']);
        expect(answer.aborted).toBe(true);
        // Eine abgebrochene Runde traegt keine Treffer: sie dann doch anzuzeigen
        // waere genau die ueberholte Antwort, die nie gewinnen darf.
        expect(answer.hits).toEqual([]);
    });

    it('fragt gar nicht erst, wenn schon vor der ersten Runde abgebrochen wurde', async () => {
        const controller = new AbortController();
        controller.abort();
        const index = searcher();
        const answer = await searchByMeaning(index, '/workspace', 'user', { signal: controller.signal });
        expect(index.calls).toEqual([]);
        expect(answer.aborted).toBe(true);
    });
});
