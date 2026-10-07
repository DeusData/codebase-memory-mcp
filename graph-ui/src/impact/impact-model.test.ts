import { describe, expect, it } from 'vitest';
import {
    affectedEndpoints,
    badgeRules,
    buildComplexityLookup,
    endpointLabel,
    entryPointNames,
    mapChangeImpact,
    reasonsFor,
    refRejection,
    riskInputFor,
    summariseTests,
} from './impact-model';
import type { ImpactTestLookup } from './impact-model';
import { NO_TEST_LOOKUP } from './impact-model';
import type {
    ArchitectureOverviewDto,
    ChangeImpactDto,
    ChangeImpactSymbol,
    SymbolComplexity,
} from '../core/intelligence-provider';
import type { TestRef } from '../core/semantic-ir';

/**
 * Der Zusammenbau wird an einer erfundenen, aber realistisch geformten
 * Aenderungsmenge geprueft: dieselben Felder, die der Provider fuellt, und
 * dieselbe Mischung aus Modul-Zeilen, geaenderten Symbolen und Aufrufern.
 */
const symbol = (over: Partial<ChangeImpactSymbol>): ChangeImpactSymbol => ({
    name: 'createUser',
    qualifiedName: 'p.src.services.userService.createUser',
    filePath: 'src/services/userService.ts',
    line: 22,
    kind: 'function',
    changeKind: 'declared',
    distance: 0,
    ...over,
});

const CHANGE: ChangeImpactDto = {
    baselineRef: undefined,
    changedFiles: ['src/services/userService.ts'],
    impacted: [],
    walkedDistance: 2,
    symbols: [
        symbol({ name: 'src/services/userService.ts', qualifiedName: undefined, changeKind: 'module', kind: 'module' }),
        symbol({}),
        symbol({
            name: 'listUsers',
            qualifiedName: 'p.src.services.userService.listUsers',
            line: 18,
        }),
        symbol({
            name: 'registerUserRoutes',
            qualifiedName: 'p.src.routes.users.registerUserRoutes',
            filePath: 'src/routes/users.ts',
            line: 7,
            changeKind: 'caller',
            distance: 1,
        }),
        symbol({
            name: 'createApp',
            qualifiedName: 'p.src.server.createApp',
            filePath: 'src/server.ts',
            line: 30,
            changeKind: 'caller',
            distance: 2,
        }),
    ],
};

const ARCHITECTURE: ArchitectureOverviewDto = {
    totalSymbols: 76,
    totalRelations: 178,
    symbolKinds: [],
    relationKinds: [],
    languages: [],
    groups: [],
    entryPoints: [
        { name: 'main', qualifiedName: 'p.src.server.main', kind: 'function', filePath: 'src/server.ts', line: 40 },
        { name: 'createUser', qualifiedName: 'p.src.services.userService.createUser', kind: 'function' },
    ],
    routes: [
        { method: 'GET', path: '/users', filePath: 'src/routes/users.ts', line: 8, origin: 'source' },
        { method: 'POST', path: '/users', filePath: 'src/routes/users.ts', line: 13, origin: 'source' },
        { method: 'GET', path: '/orders/:id', filePath: 'src/routes/orders.ts', line: 9, origin: 'source' },
    ],
    clusters: [],
    layers: [],
    boundaries: [],
    hotspots: [
        { name: 'createUser', qualifiedName: 'p.src.services.userService.createUser', fanIn: 2 },
    ],
    files: [],
};

const COMPLEXITY: SymbolComplexity[] = [
    {
        name: 'createUser',
        qualifiedName: 'p.src.services.userService.createUser',
        complexity: 4,
        cognitive: 3,
        loopDepth: 0,
        allocationInLoop: false,
        scanInLoop: false,
        unguardedRecursion: false,
    },
];

describe('entryPointNames', () => {

    it('keeps the program start and the route handlers, never a bare export', () => {
        const names = entryPointNames(ARCHITECTURE);
        expect(names.has('main')).toBe(true);
        expect(names.has('p.src.server.main')).toBe(true);
        // createUser is flagged by the index and is only an export, so the
        // narrow reading must leave it out.
        expect(names.has('createUser')).toBe(false);
        expect(names.has('p.src.services.userService.createUser')).toBe(false);
    });

    it('takes a named handler from a route', () => {
        const names = entryPointNames({
            ...ARCHITECTURE,
            routes: [{ path: '/users', handler: 'listUsers', origin: 'index' }],
        });
        expect(names.has('listUsers')).toBe(true);
    });

    it('answers an absent summary with an empty set rather than throwing', () => {
        expect(entryPointNames(undefined).size).toBe(0);
    });
});

describe('mapChangeImpact', () => {

    const lookup = buildComplexityLookup(COMPLEXITY, ARCHITECTURE);

    it('separates changed symbols from callers and never counts a file as a symbol', () => {
        const model = mapChangeImpact(CHANGE, ARCHITECTURE, lookup);
        expect(model.direct.map((row) => row.name)).toEqual(['createUser', 'listUsers']);
        expect(model.summaryTiles.directSymbols).toBe(2);
        expect(model.summaryTiles.changedFiles).toBe(1);
        expect(model.downstream.map((group) => group.distance)).toEqual([1, 2]);
    });

    it('names the endpoints the change reaches and says how it tied them', () => {
        const model = mapChangeImpact(CHANGE, ARCHITECTURE, lookup);
        // users.ts holds a caller, orders.ts holds nothing this change reaches.
        expect(model.endpoints.map(endpointLabel)).toEqual(['GET /users', 'POST /users']);
        expect(model.endpoints.every((endpoint) => endpoint.via === 'file')).toBe(true);
        expect(model.summaryTiles.endpoints).toBe(2);
    });

    it('gives every claim of the narrative its own evidence row', () => {
        const model = mapChangeImpact(CHANGE, ARCHITECTURE, lookup);
        expect(model.narrative.evidence.length).toBeGreaterThanOrEqual(4);
        expect(model.narrative.text).toBe(
            model.narrative.evidence.map((entry) => entry.claim).join(' '),
        );
        for (const entry of model.narrative.evidence) {
            expect(entry.claim.length).toBeGreaterThan(0);
            expect(entry.value.length).toBeGreaterThan(0);
        }
    });

    it('comes back empty rather than inventing a page while the answer is missing', () => {
        const model = mapChangeImpact(undefined, undefined, new Map());
        expect(model.direct).toEqual([]);
        expect(model.downstream).toEqual([]);
        expect(model.risk).toBe('low');
        expect(model.summaryTiles.changedFiles).toBe(0);
    });

    it('says how many symbols carry no complexity reading at all', () => {
        const model = mapChangeImpact(CHANGE, ARCHITECTURE, lookup);
        // Only createUser was measured; the other three rest on absence.
        expect(model.completeness).toEqual({ measured: 1, unmeasured: 3, total: 4 });
        expect(model.narrative.text).toContain('Complexity readings were unavailable');
    });
});

describe('summariseTests', () => {

    const covering: TestRef[] = [{ name: 'creates a user', file: 'test/userService.test.ts', line: 12, kind: 'unit' }];

    const lookup: ImpactTestLookup = {
        bySymbol: new Map([['p.src.services.userService.createUser', covering]]),
        checked: new Set([
            'p.src.services.userService.createUser',
            'p.src.services.userService.listUsers',
        ]),
        cappedAt: 2,
    };

    it('keeps unchecked apart from uncovered', () => {
        const tests = summariseTests(CHANGE.symbols, lookup);
        expect(tests.checked).toBe(2);
        expect(tests.covering.map((entry) => entry.name)).toEqual(['creates a user']);
        expect(tests.missing.map((entry) => entry.name)).toEqual(['listUsers']);
        // registerUserRoutes was never looked up, so it is in neither list.
        expect(tests.missing.some((entry) => entry.name === 'registerUserRoutes')).toBe(false);
        expect(tests.cappedAt).toBe(2);
    });

    it('checks nothing when nothing was looked up', () => {
        const tests = summariseTests(CHANGE.symbols, NO_TEST_LOOKUP);
        expect(tests).toEqual({ covering: [], missing: [], checked: 0 });
    });
});

describe('affectedEndpoints', () => {

    it('prefers the handler match and never reports a route twice', () => {
        const architecture: ArchitectureOverviewDto = {
            ...ARCHITECTURE,
            routes: [
                { method: 'GET', path: '/users', filePath: 'src/routes/users.ts', line: 8, handler: 'listUsers', origin: 'index' },
            ],
        };
        const endpoints = affectedEndpoints(CHANGE, architecture);
        expect(endpoints).toHaveLength(1);
        expect(endpoints[0].via).toBe('handler');
    });

    it('reports nothing when the summary recovered no route', () => {
        expect(affectedEndpoints(CHANGE, { ...ARCHITECTURE, routes: [] })).toEqual([]);
    });
});

describe('riskInputFor and reasonsFor', () => {

    it('leaves an unmeasured field undefined instead of defaulting it to zero', () => {
        const input = riskInputFor(
            symbol({ name: 'listUsers', qualifiedName: 'p.src.services.userService.listUsers' }),
            new Map(),
            new Set(),
            NO_TEST_LOOKUP,
        );
        expect(input.cyclomatic).toBeUndefined();
        expect(input.fanIn).toBeUndefined();
        expect(reasonsFor(input)).toEqual([]);
    });

    it('gives one phrase per reading that reached a threshold', () => {
        const reasons = reasonsFor({
            isEntryPoint: true,
            unguardedRecursion: true,
            transitiveLoopDepth: 3,
            allocInLoop: true,
            linearScanInLoop: true,
            cognitive: 12,
            cyclomatic: 7,
            fanIn: 4,
        });
        expect(reasons).toEqual([
            'reachable from outside the program',
            'recursion with no visible base case',
            'loops nested 3 deep',
            'allocates inside a loop',
            'scans a list inside a loop',
            'cognitive complexity 12',
            '7 branches to follow',
            'reached by 4 symbols',
        ]);
    });
});

describe('badgeRules', () => {

    it('names the rules that produced the word and nothing quieter', () => {
        const model = mapChangeImpact(CHANGE, ARCHITECTURE, buildComplexityLookup(COMPLEXITY, ARCHITECTURE), {
            bySymbol: new Map(),
            checked: new Set(['p.src.services.userService.createUser']),
        });
        expect(model.risk).toBe('medium');
        const rules = badgeRules(model);
        expect(rules.join(' ')).toContain('2 endpoints are registered in a file this change reaches.');
        expect(rules.join(' ')).toContain('No test caller was found for 1 affected symbol.');
    });

    it('says nothing when nothing fired', () => {
        const model = mapChangeImpact(undefined, undefined, new Map());
        expect(badgeRules(model)).toEqual([]);
    });
});

describe('refRejection', () => {

    for (const good of ['main', 'v1.2.3', 'feature/atlas-r1', 'HEAD', '9f2a1c4', 'origin/main']) {
        it(`accepts the shape of ${good}`, () => {
            expect(refRejection(good)).toBeUndefined();
        });
    }

    const bad: [string, RegExp][] = [
        ['', /empty/],
        ['   ', /empty/],
        ['not a ref', /space/],
        ['-main', /dash/],
        ['main..dev', /consecutive dots/],
        ['main^', /~ \^/],
        ['main:dev', /~ \^/],
        ['refs//heads', /empty path component/],
        ['main@{1}', /@\{/],
        ['main.lock', /\.lock/],
        ['main.', /dot/],
        ['@', /single character/],
    ];
    for (const [value, rule] of bad) {
        it(`refuses ${JSON.stringify(value)} with the rule it broke`, () => {
            const rejection = refRejection(value);
            expect(rejection).toBeDefined();
            expect(rejection!).toMatch(rule);
        });
    }
});
