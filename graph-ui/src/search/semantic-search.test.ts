/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/test/semantic-search.test.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Aenderungen gegenueber dem Original:
 * nur die beiden Importpfade. Die Kandidaten sind unveraendert die Zeilen, die
 * der 0.9.0-Index fuer "user validation" ueber fixtures/atlas-sample liefert,
 * und genau dieses Fixture liegt auch hier: die Suite prueft damit dieselbe
 * Behauptung ueber dasselbe Repository wie im Referenzprojekt.
 */

/**
 * The deterministic search, tested against the fixture it has to answer for.
 *
 * The hits below are the ones the 0.9.0 index actually returns for the two
 * words of "user validation" against `fixtures/atlas-sample`, transcribed row
 * for row including the flags. That is deliberate: the ranking's whole claim is
 * that a reader typing two ordinary words finds the two functions that are
 * about them, and a suite that invented friendly candidates would be proving
 * something about the invention rather than about the repository.
 *
 * Three groups. The splitter, because every other signal is built on it. The
 * score, because each rung has to be worth strictly less than the one above it.
 * The order, because it has to be total: two runs over one index produce the
 * same list, which is what lets a picker be photographed and a driver assert on
 * the top five.
 */

import { describe, expect, it } from 'vitest';

import type { SymbolSearchHit } from '../core/intelligence-provider';

import {
    isNavigable,
    queryTerms,
    rankHits,
    scoreHit,
    sharedPrefixLength,
    tokenize
} from './semantic-search';

const QUERY = 'user validation';

/**
 * What the index answers for `user` and for `validation`, in its own order.
 *
 * Fan-in is carried beside each row rather than on it, because a search row
 * does not report one: the caller asks for the readings in a separate batch and
 * hands them to the ranking. The two numbers that matter are `createUser` at
 * two and `toUser` at one, which is the tie the fixture actually contains.
 */
const FAN_IN: Record<string, number> = {
    'atlas.src.types.User': 4,
    'atlas.src.types.UserEntity': 1,
    'atlas.src.util.validate.UserInput': 1,
    'atlas.src.services.userService.createUser': 2,
    'atlas.src.services.userService.listUsers': 3,
    'atlas.src.routes.users.registerUserRoutes': 1,
    'atlas.src.services.userService.toUser': 1,
    'atlas.src.util.validate.validateUser': 1,
    'atlas.src.util.validate.ValidationError': 3
};

function hit(
    name: string,
    kind: SymbolSearchHit['kind'],
    filePath: string,
    qualifiedName: string,
    flags: { isExported?: boolean; isTest?: boolean } = {}
): SymbolSearchHit {
    return { name, kind, filePath, qualifiedName, ...flags };
}

/** The union of the two per-word searches, in the order the index returned them. */
const CANDIDATES: SymbolSearchHit[] = [
    hit('User', 'interface', 'src/types.ts', 'atlas.src.types.User', { isExported: true }),
    hit('UserEntity', 'class', 'src/types.ts', 'atlas.src.types.UserEntity', { isExported: true }),
    hit('UserInput', 'interface', 'src/util/validate.ts', 'atlas.src.util.validate.UserInput', { isExported: true }),
    hit('createUser', 'function', 'src/services/userService.ts', 'atlas.src.services.userService.createUser', { isExported: true }),
    hit('listUsers', 'function', 'src/services/userService.ts', 'atlas.src.services.userService.listUsers', { isExported: true }),
    hit('registerUserRoutes', 'function', 'src/routes/users.ts', 'atlas.src.routes.users.registerUserRoutes', { isExported: true }),
    // The index answers a name search with modules and files as well as symbols.
    hit('src/routes/users.ts', 'module', 'src/routes/users.ts', 'atlas.src.routes.users', { isExported: true }),
    hit('src/services/userService.ts', 'module', 'src/services/userService.ts', 'atlas.src.services.userService', { isExported: true }),
    hit('test/userService.test.ts', 'module', 'test/userService.test.ts', 'atlas.test.userService.test', { isExported: true }),
    hit('toUser', 'function', 'src/services/userService.ts', 'atlas.src.services.userService.toUser', { isExported: true }),
    hit('userService.test.ts', 'module', 'test/userService.test.ts', 'atlas.test.userService.test.file'),
    hit('userService.ts', 'module', 'src/services/userService.ts', 'atlas.src.services.userService.file'),
    hit('users.ts', 'module', 'src/routes/users.ts', 'atlas.src.routes.users.file'),
    hit('validateUser', 'function', 'src/util/validate.ts', 'atlas.src.util.validate.validateUser', { isExported: true }),
    hit('ValidationError', 'class', 'src/util/validate.ts', 'atlas.src.util.validate.ValidationError', { isExported: true })
];

const ranked = (query = QUERY, hits = CANDIDATES) =>
    rankHits(hits, query, candidate => FAN_IN[candidate.qualifiedName ?? ''] ?? 0);

const names = (query = QUERY, hits = CANDIDATES) => ranked(query, hits).map(entry => entry.hit.name);

describe('tokenize', () => {

    it('splits camel case, snake case, paths and phrases the same way', () => {
        expect(tokenize('validateUser')).toEqual(['validate', 'user']);
        expect(tokenize('HTTPServerConfig')).toEqual(['http', 'server', 'config']);
        expect(tokenize('src/services/userService.ts')).toEqual(['src', 'services', 'user', 'service', 'ts']);
        expect(tokenize('user validation')).toEqual(['user', 'validation']);
        expect(tokenize('sha256_digest')).toEqual(['sha256', 'digest']);
    });
});

describe('queryTerms', () => {

    it('keeps the reader\'s order, drops the filler and the one-letter noise', () => {
        expect(queryTerms('the user validation')).toEqual(['user', 'validation']);
        expect(queryTerms('a b user')).toEqual(['user']);
        expect(queryTerms('user user')).toEqual(['user']);
        expect(queryTerms('   ')).toEqual([]);
    });

    it('caps a long query rather than ranking noise', () => {
        expect(queryTerms('one two three four five six seven eight')).toHaveLength(6);
    });
});

describe('sharedPrefixLength', () => {

    it('is what makes validation and validate the same stem', () => {
        expect(sharedPrefixLength('validation', 'validate')).toBe(7);
        expect(sharedPrefixLength('user', 'util')).toBe(1);
    });
});

describe('scoreHit', () => {

    it('is worth more in a name than in a path', () => {
        const inName = scoreHit(hit('user', 'function', 'src/other/thing.ts', 'a.user'), ['user'], 0);
        const inPath = scoreHit(hit('thing', 'function', 'src/user/thing.ts', 'a.thing'), ['user'], 0);
        expect(inName.score).toBeGreaterThan(inPath.score);
    });

    it('pays an exact word more than a prefix and a prefix more than a stem', () => {
        const exact = scoreHit(hit('user', 'function', '', 'a.user'), ['user'], 0).score;
        const prefix = scoreHit(hit('users', 'function', '', 'a.users'), ['user'], 0).score;
        const stem = scoreHit(hit('validate', 'function', '', 'a.validate'), ['validation'], 0).score;
        expect(exact).toBeGreaterThan(prefix);
        expect(prefix).toBeGreaterThan(stem);
    });

    it('pays for answering a second word of the query', () => {
        const both = scoreHit(hit('validateUser', 'function', '', 'a.validateUser'), ['user', 'validation'], 0);
        const one = scoreHit(hit('createUser', 'function', '', 'a.createUser'), ['user', 'validation'], 0);
        expect(both.matched).toEqual(['user', 'validation']);
        expect(one.matched).toEqual(['user']);
        expect(both.score).toBeGreaterThan(one.score);
    });

    it('prefers what the repository exports and demotes what only its tests use', () => {
        const exported = scoreHit(hit('user', 'function', '', 'a.user', { isExported: true }), ['user'], 0).score;
        const plain = scoreHit(hit('user', 'function', '', 'a.user'), ['user'], 0).score;
        const test = scoreHit(hit('user', 'function', '', 'a.user', { isTest: true }), ['user'], 0).score;
        expect(exported).toBeGreaterThan(plain);
        expect(plain).toBeGreaterThan(test);
    });
});

describe('isNavigable', () => {

    it('drops the module and file rows, which have no declaration to open', () => {
        expect(isNavigable(hit('createUser', 'function', 'src/services/userService.ts', 'a.createUser'))).toBe(true);
        expect(isNavigable(hit('src/routes/users.ts', 'module', 'src/routes/users.ts', 'a.users'))).toBe(false);
        expect(isNavigable(hit('userService.ts', 'module', 'src/services/userService.ts', 'a.file'))).toBe(false);
    });
});

describe('rankHits against the atlas-sample fixture', () => {

    it('puts validateUser and createUser in the top five for "user validation"', () => {
        expect(names().slice(0, 5)).toContain('validateUser');
        expect(names().slice(0, 5)).toContain('createUser');
    });

    it('ranks the symbol that answers both words first', () => {
        expect(names()[0]).toBe('validateUser');
    });

    it('breaks a tie by fan-in, so createUser beats toUser', () => {
        const order = names();
        expect(order.indexOf('createUser')).toBeLessThan(order.indexOf('toUser'));
    });

    it('returns no module or file row', () => {
        expect(names().some(name => name.includes('/') || name.endsWith('.ts'))).toBe(false);
    });

    it('drops a candidate that answered no word of the query', () => {
        const noise = hit('hotspotScan', 'function', 'src/repo/db.ts', 'atlas.src.repo.db.hotspotScan');
        expect(names(QUERY, [...CANDIDATES, noise])).not.toContain('hotspotScan');
    });

    it('answers nothing at all for a query with no words in it', () => {
        expect(ranked('   ')).toEqual([]);
        expect(ranked('a')).toEqual([]);
    });

    it('is stable: two runs over one answer produce one order', () => {
        expect(names()).toEqual(names());
        expect(names(QUERY, [...CANDIDATES].reverse())).toEqual(names());
    });

    it('carries the reason each hit is in the list', () => {
        const top = ranked()[0];
        expect(top.matched).toEqual(['user', 'validation']);
    });
});
