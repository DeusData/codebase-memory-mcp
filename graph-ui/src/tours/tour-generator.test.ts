/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/test/tour-generator.test.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Die Fixture-Konstanten FILES,
 * IMPORTS, LAYERS und ENTRY_POINTS sind 1:1 mitkopiert, weil eine
 * nachgebaute Fixture eine andere Fixture ist: die Duplikate in IMPORTS
 * stammen aus einem echten 0.9.0-Lauf ueber fixtures/atlas-sample, und genau
 * sie machen die Deduplizierung zu einer geprueften Eigenschaft statt zu einer
 * Annahme. Aenderungen gegenueber dem Original: die Importpfade, und die
 * Zusicherungen sind von chai/vitest-expect auf dieselbe vitest-expect-API
 * gehoben, die dieses Projekt ueberall benutzt.
 */
/**
 * The reading order a workspace is put into, and the four properties it stands
 * or falls on.
 *
 * **Determinism.** Two runs must produce the same bytes and so must two
 * machines. The suite proves that by generating twice from inputs shuffled into
 * a different order and comparing the serialised documents, which is the only
 * comparison that catches an ordering that happens to be stable on this machine
 * because a Map preserved an insertion order.
 *
 * **Cycles.** Import cycles are ordinary, and a topological sort has no answer
 * for one. What matters is not that the generator survives them but that it
 * survives them by a stated rule, records what it dropped, and still places
 * every file exactly once.
 *
 * **Ordering.** The point of the whole feature: on the fixture's dependency
 * graph, what everything rests on comes before what rests on it. The assertion
 * is written the way the acceptance criteria are written, on config and types
 * against routes and the server.
 *
 * **Grouping and honesty.** A step's role and its sentences are evidence: a path
 * segment, or a layer the index assigned. The suite pins the fact that a step
 * says which, and that a file the index holds no exported symbol in gets a file
 * step rather than an invented symbol.
 */

import { describe, expect, it } from 'vitest';

import type {
    ArchitectureLayerAssignment,
    ModuleDependency,
    SymbolSearchHit,
} from '../core/intelligence-provider';
import {
    MAX_TOUR_STEPS,
    generateHeuristicTour,
    groupOf,
    roleOf,
    sampleOrder,
    topsortFiles,
} from './tour-generator';

/** The fixture's own files, as the index lists them. */
const FILES = [
    'src/config.ts',
    'src/repo/db.ts',
    'src/routes/orders.ts',
    'src/routes/users.ts',
    'src/server.ts',
    'src/services/orderService.ts',
    'src/services/userService.ts',
    'src/types.ts',
    'src/util/validate.ts',
    'test/userService.test.ts',
];

/**
 * The fixture's import edges, with the duplicates the engine really emits.
 *
 * Recorded from a real index of fixtures/atlas-sample: the 0.9.0 analysis writes
 * one edge per import statement, so `userService.ts` importing three names from
 * `db.ts` appears three times. Keeping the duplicates here is what makes the
 * deduplication in the generator a tested property rather than an assumption.
 */
const IMPORTS: ModuleDependency[] = [
    { from: 'src/services/orderService.ts', to: 'src/repo/db.ts' },
    { from: 'src/services/orderService.ts', to: 'src/repo/db.ts' },
    { from: 'src/services/orderService.ts', to: 'src/types.ts' },
    { from: 'src/services/orderService.ts', to: 'src/util/validate.ts' },
    { from: 'src/routes/orders.ts', to: 'src/services/orderService.ts' },
    { from: 'src/routes/orders.ts', to: 'src/types.ts' },
    { from: 'src/routes/orders.ts', to: 'src/types.ts' },
    { from: 'src/server.ts', to: 'src/config.ts' },
    { from: 'src/server.ts', to: 'src/routes/orders.ts' },
    { from: 'src/server.ts', to: 'src/routes/users.ts' },
    { from: 'src/server.ts', to: 'src/types.ts' },
    { from: 'test/userService.test.ts', to: 'src/services/userService.ts' },
    { from: 'src/services/userService.ts', to: 'src/repo/db.ts' },
    { from: 'src/services/userService.ts', to: 'src/repo/db.ts' },
    { from: 'src/services/userService.ts', to: 'src/repo/db.ts' },
    { from: 'src/services/userService.ts', to: 'src/types.ts' },
    { from: 'src/services/userService.ts', to: 'src/util/validate.ts' },
    { from: 'src/routes/users.ts', to: 'src/services/userService.ts' },
    { from: 'src/routes/users.ts', to: 'src/types.ts' },
];

/** The layer assignments the whole-project summary reports for the fixture. */
const LAYERS: ArchitectureLayerAssignment[] = [
    { group: 'config', layer: 'leaf', reason: 'only inbound calls, no outbound' },
    { group: 'repo', layer: 'core', reason: 'high fan-in (4 in, 0 out)' },
    { group: 'routes', layer: 'internal', reason: 'fan-in=2, fan-out=3' },
    { group: 'server', layer: 'entry', reason: 'has entry points, only outbound calls' },
    { group: 'services', layer: 'internal', reason: 'fan-in=3, fan-out=8' },
    { group: 'types', layer: 'leaf', reason: 'only inbound calls, no outbound' },
    { group: 'util', layer: 'leaf', reason: 'only inbound calls, no outbound' },
];

/** A few of the exported symbols the summary flags, enough to exercise the pick. */
const ENTRY_POINTS: SymbolSearchHit[] = [
    { name: 'isProduction', qualifiedName: 'p.src.config.isProduction', kind: 'function', filePath: 'src/config.ts', line: 20 },
    { name: 'loadConfig', qualifiedName: 'p.src.config.loadConfig', kind: 'function', filePath: 'src/config.ts', line: 11 },
    { name: 'query', qualifiedName: 'p.src.repo.db.query', kind: 'function', filePath: 'src/repo/db.ts', line: 17 },
    { name: 'registerUserRoutes', qualifiedName: 'p.src.routes.users.registerUserRoutes', kind: 'function', filePath: 'src/routes/users.ts', line: 7 },
    { name: 'registerOrderRoutes', qualifiedName: 'p.src.routes.orders.registerOrderRoutes', kind: 'function', filePath: 'src/routes/orders.ts', line: 7 },
    { name: 'createApp', qualifiedName: 'p.src.server.createApp', kind: 'function', filePath: 'src/server.ts', line: 44 },
    { name: 'main', qualifiedName: 'p.src.server.main', kind: 'function', filePath: 'src/server.ts', line: 60 },
    { name: 'listUsers', qualifiedName: 'p.src.services.userService.listUsers', kind: 'function', filePath: 'src/services/userService.ts', line: 19 },
    { name: 'createUser', qualifiedName: 'p.src.services.userService.createUser', kind: 'function', filePath: 'src/services/userService.ts', line: 25 },
    { name: 'getOrder', qualifiedName: 'p.src.services.orderService.getOrder', kind: 'function', filePath: 'src/services/orderService.ts', line: 12 },
    { name: 'validateUser', qualifiedName: 'p.src.util.validate.validateUser', kind: 'function', filePath: 'src/util/validate.ts', line: 19 },
    // Deliberately incomplete: the index knows the name and not where it is, so
    // this must not become a step's primary.
    { name: 'nowhere', qualifiedName: 'p.src.types.nowhere', kind: 'function', filePath: 'src/types.ts' },
];

function fixtureTour(overrides: Partial<Parameters<typeof generateHeuristicTour>[0]> = {}) {
    return generateHeuristicTour({
        files: FILES,
        imports: IMPORTS,
        layers: LAYERS,
        groups: LAYERS.map((entry) => ({ name: entry.group, symbolCount: 1, fanIn: 0, fanOut: 0 })),
        entryPoints: ENTRY_POINTS,
        engineVersion: '0.9.0',
        ...overrides,
    });
}

/** Where one file ends up in the walk, by path. */
function stepIndexOf(steps: { primary: { filePath: string } }[], filePath: string): number {
    return steps.findIndex((step) => step.primary.filePath === filePath);
}

/** A shuffle that is itself deterministic, so a failure can be reproduced. */
function rotated<T>(entries: readonly T[], by: number): T[] {
    const offset = ((by % entries.length) + entries.length) % entries.length;
    return [...entries.slice(offset), ...entries.slice(0, offset)];
}

describe('the dependency order', () => {
    it('places every file exactly once', () => {
        const { order } = topsortFiles(FILES, IMPORTS);
        expect([...order].sort()).toEqual([...FILES].sort());
        expect(new Set(order).size).toBe(FILES.length);
    });

    it('puts what a file imports before the file itself', () => {
        const { order } = topsortFiles(FILES, IMPORTS);
        const at = (path: string) => order.indexOf(path);
        for (const edge of IMPORTS) {
            expect(at(edge.to), `${edge.to} before ${edge.from}`).toBeLessThan(at(edge.from));
        }
    });

    it('breaks no edge on an acyclic project', () => {
        expect(topsortFiles(FILES, IMPORTS).brokenEdges).toEqual([]);
    });

    it('counts the distinct edges rather than the rows it was handed', () => {
        // Nineteen rows, four of them repeats of a pair already counted.
        expect(topsortFiles(FILES, IMPORTS).edgeCount).toBe(15);
    });

    it('ignores an edge naming a file the index does not hold', () => {
        const withStranger = [...IMPORTS, { from: 'src/server.ts', to: 'vendor/elsewhere.ts' }];
        expect(topsortFiles(FILES, withStranger).order).toEqual(topsortFiles(FILES, IMPORTS).order);
    });

    it('drains ties in path order, whatever order the edges arrived in', () => {
        const first = topsortFiles(FILES, IMPORTS).order;
        for (const by of [1, 5, 11]) {
            expect(topsortFiles(rotated(FILES, by), rotated(IMPORTS, by)).order).toEqual(first);
        }
    });
});

describe('a dependency cycle', () => {
    const CYCLE_FILES = ['a.ts', 'b.ts', 'c.ts'];
    const CYCLE: ModuleDependency[] = [
        { from: 'a.ts', to: 'b.ts' },
        { from: 'b.ts', to: 'a.ts' },
        { from: 'c.ts', to: 'a.ts' },
    ];

    it('still places every file', () => {
        const { order } = topsortFiles(CYCLE_FILES, CYCLE);
        expect([...order].sort()).toEqual(CYCLE_FILES);
    });

    it('breaks the lexicographically smallest edge holding a file back', () => {
        // The live edges are a->b, b->a and c->a; the smallest by (from, to) is
        // a->b, and dropping it frees a.ts, which frees the other two.
        expect(topsortFiles(CYCLE_FILES, CYCLE).brokenEdges).toEqual([{ from: 'a.ts', to: 'b.ts' }]);
    });

    it('records what it broke in the generated document', () => {
        const tour = generateHeuristicTour({ files: CYCLE_FILES, imports: CYCLE });
        expect(tour.generated.brokenEdges).toEqual(['a.ts -> b.ts']);
    });

    it('reaches the same order however the edges are presented', () => {
        const first = topsortFiles(CYCLE_FILES, CYCLE).order;
        expect(topsortFiles(rotated(CYCLE_FILES, 2), rotated(CYCLE, 1)).order).toEqual(first);
    });

    it('survives a workspace that is nothing but one long cycle', () => {
        const files = ['x.ts', 'y.ts', 'z.ts'];
        const ring: ModuleDependency[] = [
            { from: 'x.ts', to: 'y.ts' },
            { from: 'y.ts', to: 'z.ts' },
            { from: 'z.ts', to: 'x.ts' },
        ];
        const { order, brokenEdges } = topsortFiles(files, ring);
        expect([...order].sort()).toEqual(files);
        expect(brokenEdges.length).toBeGreaterThan(0);
    });
});

describe('the generated tour', () => {
    it('has at least five steps on the fixture', () => {
        expect(fixtureTour().steps.length).toBeGreaterThanOrEqual(5);
    });

    it('reads configuration and types before routes and the server', () => {
        const steps = fixtureTour().steps;
        const config = stepIndexOf(steps, 'src/config.ts');
        const types = stepIndexOf(steps, 'src/types.ts');
        const orders = stepIndexOf(steps, 'src/routes/orders.ts');
        const users = stepIndexOf(steps, 'src/routes/users.ts');
        const server = stepIndexOf(steps, 'src/server.ts');
        for (const early of [config, types]) {
            expect(early).toBeGreaterThanOrEqual(0);
            for (const late of [orders, users, server]) {
                expect(early).toBeLessThan(late);
            }
        }
    });

    it('numbers its steps in the order they are walked', () => {
        expect(fixtureTour().steps.map((step) => step.order)).toEqual([...Array(FILES.length).keys()]);
    });

    it('gives every step an id of its own', () => {
        const ids = fixtureTour().steps.map((step) => step.id);
        expect(new Set(ids).size).toBe(ids.length);
    });

    it('names the strategy and the engine that answered', () => {
        expect(fixtureTour().generated.strategy).toBe('topsort');
        expect(fixtureTour().generated.engineVersion).toBe('0.9.0');
    });

    it('records nothing that changes between two runs', () => {
        const serialised = JSON.stringify(fixtureTour());
        expect(serialised).not.toMatch(/\d{4}-\d{2}-\d{2}T/);
        expect(serialised).not.toMatch(/file:\/\//);
        expect(serialised).not.toMatch(/\/Users\/|\/home\/|[A-Z]:\\\\/);
    });
});

describe('determinism', () => {
    it('produces byte-identical documents on two runs', () => {
        expect(JSON.stringify(fixtureTour())).toBe(JSON.stringify(fixtureTour()));
    });

    it('produces byte-identical documents from shuffled inputs', () => {
        const shuffled = generateHeuristicTour({
            files: rotated(FILES, 4),
            imports: rotated(IMPORTS, 7),
            layers: rotated(LAYERS, 3),
            groups: rotated(LAYERS, 2).map((entry) => ({ name: entry.group, symbolCount: 1, fanIn: 0, fanOut: 0 })),
            entryPoints: rotated(ENTRY_POINTS, 5),
            engineVersion: '0.9.0',
        });
        expect(JSON.stringify(shuffled)).toBe(JSON.stringify(fixtureTour()));
    });

    it('says the same thing about a workspace held at a different path', () => {
        // Nothing in the input names a directory, which is the point: the
        // generator is handed workspace-relative paths and can therefore not
        // produce anything that depends on where the checkout lives.
        expect(JSON.stringify(fixtureTour())).not.toContain('atlas-sample/');
    });
});

describe('what a step points at', () => {
    it('picks the earliest declared exported symbol in the file', () => {
        const step = fixtureTour().steps.find((entry) => entry.primary.filePath === 'src/config.ts');
        expect(step?.primary).toMatchObject({
            kind: 'symbol',
            name: 'loadConfig',
            qualifiedName: 'p.src.config.loadConfig',
            line: 11,
        });
    });

    it('falls back to the file when the index flagged no symbol in it', () => {
        const step = fixtureTour().steps.find((entry) => entry.primary.filePath === 'test/userService.test.ts');
        expect(step?.primary).toEqual({ kind: 'file', filePath: 'test/userService.test.ts' });
    });

    it('refuses a symbol the index could not place on a line', () => {
        const step = fixtureTour().steps.find((entry) => entry.primary.filePath === 'src/types.ts');
        expect(step?.primary.kind).toBe('file');
    });
});

describe('grouping and roles', () => {
    it('carries the group and the layer the index assigned', () => {
        const steps = fixtureTour().steps;
        const byPath = (path: string) => steps.find((step) => step.primary.filePath === path);
        expect(byPath('src/services/userService.ts')).toMatchObject({ group: 'services', layer: 'internal' });
        expect(byPath('src/config.ts')).toMatchObject({ group: 'config', layer: 'leaf' });
        expect(byPath('src/server.ts')).toMatchObject({ group: 'server', layer: 'entry' });
        expect(byPath('src/repo/db.ts')).toMatchObject({ group: 'repo', layer: 'core' });
    });

    it('leaves group and layer off a file no group matched', () => {
        const step = fixtureTour().steps.find((entry) => entry.primary.filePath === 'test/userService.test.ts');
        expect(step?.group).toBeUndefined();
        expect(step?.layer).toBeUndefined();
    });

    it('quotes the layer reason the index gave, and nothing else about the code', () => {
        const step = fixtureTour().steps.find((entry) => entry.primary.filePath === 'src/services/userService.ts');
        expect(step?.description).toContain('The index places the group services in the internal layer');
        expect(step?.description).toContain('fan-in=3, fan-out=8');
    });

    it('counts the imports in both directions', () => {
        const step = fixtureTour().steps.find((entry) => entry.primary.filePath === 'src/types.ts');
        expect(step?.description).toContain('It imports nothing else in this workspace, and 5 files import it.');
    });

    it('reads a role off the path before it reads one off the index', () => {
        expect(roleOf('src/routes/users.ts', 'internal')).toBe('route');
        expect(roleOf('src/services/userService.ts', 'internal')).toBe('service');
        expect(roleOf('src/util/validate.ts', 'leaf')).toBe('util');
        expect(roleOf('src/config.ts', 'leaf')).toBe('config');
        expect(roleOf('src/server.ts', 'entry')).toBe('entry');
        expect(roleOf('test/userService.test.ts', undefined)).toBe('test');
        expect(roleOf('src/thing.ts', 'core')).toBe('layer-core');
        expect(roleOf('src/thing.ts', undefined)).toBe('unclassified');
    });

    it('recognises a test from a compound basename as well as a directory', () => {
        expect(roleOf('lib/userService.test.ts', undefined)).toBe('test');
        expect(roleOf('lib/userService.spec.js', undefined)).toBe('test');
        // A word that merely ends in "test" is not a test, and a directory that
        // is a role word still decides when the basename says nothing.
        expect(roleOf('src/latest.ts', undefined)).toBe('unclassified');
        expect(roleOf('lib/latest.ts', undefined)).toBe('util');
    });

    it('joins a file to the deepest group its path names', () => {
        expect(groupOf('src/services/userService.ts', ['src', 'services'])).toBe('services');
        expect(groupOf('src/services/userService.ts', ['src'])).toBe('src');
        expect(groupOf('vendor/thing.ts', ['src', 'services'])).toBeUndefined();
    });

    it('says so plainly when neither the path nor the index placed the file', () => {
        const tour = generateHeuristicTour({ files: ['a/thing.ts'], imports: [] });
        expect(tour.steps[0].description).toContain('Neither its path nor the index says what part this file plays');
    });
});

describe('a workspace larger than one sitting', () => {
    const MANY = Array.from({ length: 40 }, (_unused, index) => `src/m${String(index).padStart(3, '0')}.ts`);

    it('caps the walk', () => {
        expect(generateHeuristicTour({ files: MANY, imports: [] }).steps.length).toBe(MAX_TOUR_STEPS);
    });

    it('keeps the first and the last of the order rather than the first n', () => {
        const sampled = sampleOrder(MANY, MAX_TOUR_STEPS);
        expect(sampled[0]).toBe(MANY[0]);
        expect(sampled[sampled.length - 1]).toBe(MANY[MANY.length - 1]);
    });

    it('keeps the sample in the order it sampled from', () => {
        const sampled = sampleOrder(MANY, MAX_TOUR_STEPS);
        expect(sampled).toEqual([...sampled].sort());
    });

    it('samples the same way twice', () => {
        expect(sampleOrder(MANY, 7)).toEqual(sampleOrder(MANY, 7));
    });

    it('leaves a short order alone', () => {
        expect(sampleOrder(FILES, MAX_TOUR_STEPS)).toEqual(FILES);
    });
});

describe('the product does not walk a reader through its own files', () => {
    /**
     * The analysis indexes every file it can parse, and CodeAtlas keeps its pin,
     * its policy and its tours in the workspace as JSON. Without the exclusion
     * the first step of a first tour is a walk through the product's own
     * bookkeeping, and worse: a tour written into `.codeatlas/tours/` gives the
     * next generation a file to place that the previous one did not have, and
     * the artefact would change every time it was regenerated.
     */
    const WITH_STATE = [...FILES, '.codeatlas/project.json', '.codeatlas/tours/getting-started.json'];

    it('leaves the pin and the tour itself out of the walk', () => {
        const paths = generateHeuristicTour({ files: WITH_STATE, imports: IMPORTS })
            .steps.map((step) => step.primary.filePath);
        expect(paths.some((path) => path.startsWith('.codeatlas/'))).toBe(false);
    });

    it('generates the same tour whether or not it has run before', () => {
        const before = generateHeuristicTour({ files: FILES, imports: IMPORTS });
        const after = generateHeuristicTour({ files: WITH_STATE, imports: IMPORTS });
        expect(JSON.stringify(after)).toBe(JSON.stringify(before));
    });
});

describe('a workspace with nothing in it', () => {
    it('generates an empty tour rather than throwing', () => {
        const tour = generateHeuristicTour({ files: [], imports: [] });
        expect(tour.steps).toEqual([]);
        expect(tour.generated.strategy).toBe('topsort');
    });

    it('records a truncated dependency read', () => {
        const tour = generateHeuristicTour({ files: FILES, imports: IMPORTS, importsTruncated: true });
        expect(tour.generated.truncated).toBe(true);
    });
});
