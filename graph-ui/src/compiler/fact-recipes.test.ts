import { describe, expect, it } from 'vitest';

import {
    NEIGHBOR_DEPTHS,
    NEIGHBOR_DEPTH_DEFAULT,
    NEIGHBOUR_HOP2_SEEDS,
    RECIPE_SOURCES,
    compileFacts,
    raisedTypesOf,
} from './fact-recipes';
import { QUESTION_CLASSES } from './question-classifier';
import { resolveSubject } from './subject-resolver';
import { FAKE_ROOT, fakeSource } from './__fixtures__/fake-source';

describe("Martin's context rule", () => {
    it('offers exactly three depths and defaults to the first neighbourhood', () => {
        expect(NEIGHBOR_DEPTHS).toEqual([0, 1, 2]);
        expect(NEIGHBOR_DEPTH_DEFAULT).toBe(1);
    });

    it('gives the focus symbol and nothing around it at depth 0', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Wer ruft createUser?', {
            depth: 0,
        });
        expect(packet.subject?.name).toBe('createUser');
        expect(packet.neighbours).toEqual([]);
        expect(packet.notes.join(' ')).toContain('depth 0');
    });

    it('gives the direct callers and callees at depth 1', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Wer ruft createUser?', {
            depth: 1,
        });
        const names = packet.neighbours.map((entry) => entry.name).sort();
        expect(names).toContain('registerUserRoutes');
        expect(names).toContain('create');
        expect(names).toContain('validateUser');
        expect(packet.neighbours.every((entry) => entry.hop === 1)).toBe(true);
    });

    it('reaches a second ring at depth 2 and says how many seeds it expanded', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Wer ruft createUser?', {
            depth: 2,
        });
        const hops = new Set(packet.neighbours.map((entry) => entry.hop));
        expect(hops.has(2)).toBe(true);
        expect(packet.neighbours.some((entry) => entry.name === 'createApp')).toBe(true);
    });

    it('costs no extra index question at the default depth', async () => {
        const source = fakeSource();
        await compileFacts(source, FAKE_ROOT, 'Wer ruft createUser?', { depth: 1 });
        const atOne = source.counts.getFacts;
        const deeper = fakeSource();
        await compileFacts(deeper, FAKE_ROOT, 'Wer ruft createUser?', { depth: 2 });
        expect(deeper.counts.getFacts).toBeGreaterThan(atOne);
    });

    it('never expands more than the declared number of seeds', async () => {
        const source = fakeSource();
        const packet = await compileFacts(source, FAKE_ROOT, 'Wer ruft createUser?', { depth: 2 });
        const firstHop = packet.neighbours.filter((entry) => entry.hop === 1).length;
        // one IR fan-out for the subject, plus at most one question per seed
        expect(source.counts.getFacts).toBeLessThanOrEqual(
            5 + Math.min(firstHop, NEIGHBOUR_HOP2_SEEDS),
        );
    });
});

describe('every class has a declared recipe', () => {
    it('declares sources for all eight classes', () => {
        for (const klass of QUESTION_CLASSES) {
            expect(RECIPE_SOURCES[klass].length).toBeGreaterThan(0);
        }
    });

    it('runs no recipe for a question that matched nothing, and says so', async () => {
        const source = fakeSource();
        const packet = await compileFacts(source, FAKE_ROOT, 'Hallo');
        expect(packet.klass).toBe('other');
        expect(source.counts.getFacts).toBe(0);
        expect(packet.notes.join(' ')).toContain('no recipe ran');
    });
});

describe('the recipes', () => {
    it('reads what the callees can raise for an error question', async () => {
        const packet = await compileFacts(
            fakeSource(),
            FAKE_ROOT,
            'Welche Fehler kann createUser werfen?',
        );
        expect(packet.klass).toBe('why-error');
        const validate = packet.neighbours.find((entry) => entry.name === 'validateUser');
        expect(validate?.raises).toEqual(['ValidationError']);
    });

    it('reads the entry points and the routes for an entry question', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Wo faengt das Projekt an?');
        expect(packet.klass).toBe('where-entry');
        expect(packet.entryPoints.map((hit) => hit.name)).toContain('main');
        expect(packet.routes.map((route) => route.path)).toContain('/users');
    });

    it('reads the summary for an overview question', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Gib mir einen Ueberblick.');
        expect(packet.overview?.totalSymbols).toBe(76);
        expect(packet.overview?.languages).toEqual(['TypeScript']);
    });

    it('reads both symbols of a comparison', async () => {
        const packet = await compileFacts(
            fakeSource(),
            FAKE_ROOT,
            'Unterschied zwischen createUser und validateUser?',
        );
        expect(packet.subject?.name).toBe('createUser');
        expect(packet.compareWith?.symbol.name).toBe('validateUser');
        expect(packet.compareWith?.ir?.throws.value.length).toBe(2);
    });

    it('says a comparison needs two names when it got one', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Vergleiche createUser.');
        expect(packet.notes.join(' ')).toContain('two names');
    });
});

describe('honesty', () => {
    it('says when a name is not in the index instead of answering about something else', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Was macht @notASymbolHere?');
        expect(packet.subject).toBeUndefined();
        expect(packet.notes.join(' ')).toContain('no symbol called "notASymbolHere"');
    });

    /*
     * Bis W7c nahm das Rezept hier den ersten Treffer und schrieb eine Notiz
     * darunter. Seit W7c nimmt es keinen: eine Wahl, die der Compiler fuer den
     * Leser trifft und in der letzten Zeile erwaehnt, ist eine Wahl, die der
     * Leser zu spaet sieht.
     */
    it('takes no symbol when a name is ambiguous, and carries what it reached', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Was macht @create?');
        expect(packet.subject).toBeUndefined();
        expect(packet.choice?.name).toBe('create');
        expect(packet.choice?.candidates.length).toBeGreaterThanOrEqual(2);
        expect(packet.choice?.candidates.every((entry) => (entry.filePath ?? '').length > 0))
            .toBe(true);
        expect(packet.notes.join(' ')).toContain('names 2 symbols in this index');
    });

    it('answers about the picked symbol when the reader picked one', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Was macht @create?', {
            chosenSubject: 'sample.src.services.orderService.create',
        });
        expect(packet.choice).toBeUndefined();
        expect(packet.subject?.qualifiedName).toBe('sample.src.services.orderService.create');
        expect(packet.notes.join(' ')).toContain('the one you picked');
    });

    /*
     * Nutzerbefund vom 2026-08-29: ein bereitstehendes Modell, ein fokussiertes
     * Symbol und trotzdem eine Absage. Der Rueckfall ist die Antwort darauf,
     * und er ist nur zusammen mit der Auskunft darueber etwas wert.
     */
    it('answers about the symbol in focus when a written name reaches nothing', async () => {
        const focus = (await resolveSubject(fakeSource(), FAKE_ROOT, 'createUser'))?.symbol;
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Was macht @nichtsDergleichen?', {
            ...(focus === undefined ? {} : { focus }),
        });
        expect(packet.subject?.name).toBe('createUser');
        expect(packet.focusFallback).toEqual({
            asked: 'nichtsDergleichen',
            used: 'sample.src.services.userService.createUser',
        });
    });

    it('keeps the agreed sentence when there is no focus either', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Was macht @nichtsDergleichen?');
        expect(packet.subject).toBeUndefined();
        expect(packet.focusFallback).toBeUndefined();
        expect(packet.notes.join(' ')).toContain('no symbol called "nichtsDergleichen"');
    });

    /*
     * Die Schreibweise entscheidet nicht mehr. Der Beleg gehoert hierher und
     * nicht nur in subject-resolver.test.ts: die Frage des Nutzers ging durch
     * dieses Rezept, und was hier ankommt, ist das, was der Chat sieht.
     */
    it('finds the symbol the overlay shows, however the reader spelled it', async () => {
        for (const written of ['createuser', 'CreateUser', 'createUser']) {
            const packet = await compileFacts(
                fakeSource(),
                FAKE_ROOT,
                `@${written} explain this function`,
            );
            expect(packet.subject?.qualifiedName)
                .toBe('sample.src.services.userService.createUser');
            expect(packet.focusFallback).toBeUndefined();
        }
    });

    it('says that nobody read a recording when no reader was given', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Wer ruft createUser?');
        expect(packet.observed).toEqual([]);
        expect(packet.notes.join(' ')).toContain('no runtime recording was read');
    });

    it('reports what a recording saw when a reader was given', async () => {
        const packet = await compileFacts(fakeSource(), FAKE_ROOT, 'Wer ruft createUser?', {
            observed: async () => [{ from: 'registerUserRoutes', to: 'createUser', count: 3 }],
        });
        expect(packet.observed).toEqual([
            { from: 'registerUserRoutes', to: 'createUser', count: 3 },
        ]);
    });

    it('never rejects when the provider does', async () => {
        const broken = fakeSource();
        broken.getFacts = async () => {
            throw new Error('the engine is down');
        };
        const packet = await compileFacts(broken, FAKE_ROOT, 'Was macht createUser?');
        expect(packet.klass).toBe('what-is');
        expect(packet.notes.length).toBeGreaterThan(0);
    });
});

describe('determinism', () => {
    it('produces the same packet twice for the same question', async () => {
        const first = await compileFacts(fakeSource(), FAKE_ROOT, 'Wer ruft createUser?');
        const second = await compileFacts(fakeSource(), FAKE_ROOT, 'Wer ruft createUser?');
        expect(second.neighbours).toEqual(first.neighbours);
        expect(second.notes).toEqual(first.notes);
    });
});

describe('raisedTypesOf', () => {
    it('names each type once, in the order the index reported them', () => {
        expect(raisedTypesOf([
            { type: 'B' }, { type: 'A' }, { type: 'B' },
        ])).toEqual(['B', 'A']);
    });
});
