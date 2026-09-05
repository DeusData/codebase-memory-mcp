import { describe, expect, it } from 'vitest';

import {
    SUBJECT_SPELLING_BONUS,
    bareNameOf,
    orderCandidates,
    resolveSubject,
    subjectScore,
    subjectSpecificity,
} from './subject-resolver';
import { FAKE_ROOT, fakeSource } from './__fixtures__/fake-source';

describe('bareNameOf', () => {
    it('takes the last dotted segment', () => {
        expect(bareNameOf('a.b.createUser')).toBe('createUser');
        expect(bareNameOf('createUser')).toBe('createUser');
    });
});

describe('subjectScore', () => {
    const hit = {
        name: 'create',
        qualifiedName: 'sample.src.services.userService.create',
        kind: 'function' as const,
    };

    it('scores an exact qualified name highest', () => {
        expect(subjectScore(hit, 'sample.src.services.userService.create'))
            .toBe(SUBJECT_SPELLING_BONUS + 4);
    });

    it('scores a qualified suffix above a bare name', () => {
        expect(subjectScore(hit, 'userService.create')).toBeGreaterThan(
            subjectScore(hit, 'create'),
        );
    });

    it('scores a name the hit does not carry as nothing', () => {
        expect(subjectScore(hit, 'validateUser')).toBe(0);
    });
});

describe('spelling is a rank and not a filter', () => {
    const camel = {
        name: 'createUser',
        qualifiedName: 'sample.src.services.userService.createUser',
        kind: 'function' as const,
    };

    it('still scores a name whose case differs', () => {
        expect(subjectScore(camel, 'createuser')).toBeGreaterThan(0);
        expect(subjectScore(camel, 'CREATEUSER')).toBeGreaterThan(0);
        expect(subjectScore(camel, 'userservice.createuser')).toBeGreaterThan(0);
    });

    it('ranks the exact spelling above the folded one', () => {
        expect(subjectScore(camel, 'createUser')).toBeGreaterThan(subjectScore(camel, 'createuser'));
    });

    it('lets the weakest exact match outrank the strongest folded one', () => {
        const weakestExact = SUBJECT_SPELLING_BONUS + 1;
        expect(weakestExact).toBeGreaterThan(subjectSpecificity(camel, 'createuser', false));
    });

    it('keeps a name the hit does not carry at zero, folded or not', () => {
        expect(subjectScore(camel, 'validateuser')).toBe(0);
    });
});

describe('orderCandidates over spellings', () => {
    it('puts the exactly spelled symbol first', () => {
        const ordered = orderCandidates([
            { name: 'createuser', qualifiedName: 'a.createuser', kind: 'function' },
            { name: 'createUser', qualifiedName: 'b.createUser', kind: 'function' },
        ], 'createUser');
        expect(ordered[0].qualifiedName).toBe('b.createUser');
    });

    it('still prefers the qualified suffix over a bare name of the same spelling', () => {
        const ordered = orderCandidates([
            { name: 'create', qualifiedName: 'sample.orderService.create', kind: 'function' },
            { name: 'create', qualifiedName: 'sample.userService.create', kind: 'function' },
        ], 'userService.create');
        expect(ordered[0].qualifiedName).toBe('sample.userService.create');
    });
});

describe('orderCandidates', () => {
    it('puts non-test code before test code at the same score', () => {
        const ordered = orderCandidates([
            { name: 'create', qualifiedName: 'b.create', kind: 'function', isTest: true },
            { name: 'create', qualifiedName: 'a.create', kind: 'function', isTest: false },
        ], 'create');
        expect(ordered[0].isTest).toBe(false);
    });

    it('orders ties ordinally, not by collation', () => {
        const ordered = orderCandidates([
            { name: 'create', qualifiedName: 'b.create', kind: 'function' },
            { name: 'create', qualifiedName: 'a.create', kind: 'function' },
        ], 'create');
        expect(ordered.map((hit) => hit.qualifiedName)).toEqual(['a.create', 'b.create']);
    });
});

describe('resolveSubject', () => {
    it('resolves a bare name to the symbol the index knows', async () => {
        const resolved = await resolveSubject(fakeSource(), FAKE_ROOT, 'createUser');
        expect(resolved?.symbol.name).toBe('createUser');
        expect(resolved?.symbol.nodeId).toBeDefined();
        expect(resolved?.ambiguous).toBe(false);
    });

    it('resolves at the declaration line and not at a call line', async () => {
        const source = fakeSource();
        await resolveSubject(source, FAKE_ROOT, 'validateUser');
        expect(source.counts.resolveSymbolAt).toBe(1);
    });

    it('reports an ambiguous bare name and still picks deterministically', async () => {
        const first = await resolveSubject(fakeSource(), FAKE_ROOT, 'create');
        const second = await resolveSubject(fakeSource(), FAKE_ROOT, 'create');
        expect(first?.ambiguous).toBe(true);
        expect(first?.alternatives.length).toBeGreaterThan(0);
        expect(second?.symbol.qualifiedName).toBe(first?.symbol.qualifiedName);
    });

    it('lets a qualified suffix disambiguate the same bare name', async () => {
        const resolved = await resolveSubject(fakeSource(), FAKE_ROOT, 'orderService.create');
        expect(resolved?.symbol.qualifiedName).toContain('orderService');
        expect(resolved?.ambiguous).toBe(false);
    });

    it('answers with nothing for a name the index does not hold', async () => {
        expect(await resolveSubject(fakeSource(), FAKE_ROOT, 'nothingLikeThis')).toBeUndefined();
        expect(await resolveSubject(fakeSource(), FAKE_ROOT, '')).toBeUndefined();
    });

    /*
     * The four spellings of the user's screenshot, plus the name that really is
     * not there. All five go through the same search the overlay uses, so the
     * two readings of one index can no longer disagree on the same screen.
     */
    it.each([
        ['createuser', 'the spelling of the reader'],
        ['CreateUser', 'the spelling of a sentence'],
        ['createUser', 'the spelling of the index'],
        ['userService.createUser', 'the qualified path'],
    ])('resolves @%s to the symbol the overlay shows (%s)', async (written) => {
        const resolved = await resolveSubject(fakeSource(), FAKE_ROOT, written);
        expect(resolved?.symbol.name).toBe('createUser');
        expect(resolved?.symbol.qualifiedName).toBe('sample.src.services.userService.createUser');
        expect(resolved?.ambiguous).toBe(false);
    });

    it('still answers with nothing for a name no spelling reaches', async () => {
        expect(await resolveSubject(fakeSource(), FAKE_ROOT, 'nichtsDergleichenImIndex'))
            .toBeUndefined();
    });

    it('carries every candidate, the winner included, so a choice can be offered', async () => {
        const resolved = await resolveSubject(fakeSource(), FAKE_ROOT, 'create');
        expect(resolved?.ambiguous).toBe(true);
        expect(resolved?.candidates.length).toBeGreaterThanOrEqual(2);
        expect(resolved?.candidates[0].qualifiedName).toBe(resolved?.symbol.qualifiedName);
        expect(resolved?.candidates.length).toBe((resolved?.alternatives.length ?? 0) + 1);
    });

    it('never rejects when the search does', async () => {
        const source = fakeSource();
        source.searchSymbols = async () => {
            throw new Error('the engine is down');
        };
        expect(await resolveSubject(source, FAKE_ROOT, 'createUser')).toBeUndefined();
    });
});
