import { describe, expect, it, vi } from 'vitest';
import {
    BUG_PATH_DEFAULT_DEPTH,
    bugPaths,
    chainEdges,
    edgeKey,
    flowObservations,
    flowTouches,
    nodeKey,
    observedRuns,
    pickHit,
    resolveHop,
    staticChains,
} from './bug-paths';
import type { BugPathIndex, BugPathNode, BugPathObservations } from './bug-paths';
import { COLUMNS } from '../provider/cypher';
import type { FlowDetail, FlowSummary, TraceAnswer } from './trace-schemas';
import type { SymbolSearchHit } from '../core/intelligence-provider';

/**
 * Die Fixture ist der Aufrufgraph von fixtures/atlas-sample, so wie ihn eine
 * echte Indizierung meldet, plus genau die Beobachtungen, die der Beweislauf
 * einspeist. Der Grund, ihn nachzubauen statt einen Server zu starten: die
 * Regeln dieser Datei sind Mengenoperationen ueber zwei Lesungen, und die
 * pruefen sich an erfundenen Zeilen genauer als an einem Lauf, dessen
 * Zeitverhalten mitgeprueft wuerde.
 */
const PROJECT = 'p';

interface Caller {
    name: string;
    qualifiedName: string;
    filePath: string;
    line: number;
}

/** Wer ruft wen, als qualifizierte Namen. Der Schluessel ist das Ziel. */
const CALLERS: Record<string, Caller[]> = {
    'p.src.services.userService.createUser': [
        { name: 'registerUserRoutes', qualifiedName: 'p.src.routes.users.registerUserRoutes', filePath: 'src/routes/users.ts', line: 7 },
        { name: 'create', qualifiedName: 'p.src.services.userService.create', filePath: 'src/services/userService.ts', line: 41 },
    ],
    'p.src.routes.users.registerUserRoutes': [
        { name: 'createApp', qualifiedName: 'p.src.server.createApp', filePath: 'src/server.ts', line: 30 },
    ],
    'p.src.server.createApp': [
        { name: 'main', qualifiedName: 'p.src.server.main', filePath: 'src/server.ts', line: 37 },
    ],
    'p.src.util.validate.validateUser': [
        { name: 'createUser', qualifiedName: 'p.src.services.userService.createUser', filePath: 'src/services/userService.ts', line: 22 },
    ],
};

/** Datei und Name auf qualifizierten Namen, wie die Deklarations-Lesung sie meldet. */
const DECLARATIONS: { name: string; qualifiedName: string; filePath: string; line: number }[] = [
    ...Object.values(CALLERS).flat(),
    { name: 'createUser', qualifiedName: 'p.src.services.userService.createUser', filePath: 'src/services/userService.ts', line: 22 },
    { name: 'validateUser', qualifiedName: 'p.src.util.validate.validateUser', filePath: 'src/util/validate.ts', line: 19 },
    { name: 'listUsers', qualifiedName: 'p.src.services.userService.listUsers', filePath: 'src/services/userService.ts', line: 18 },
];

const TARGET: BugPathNode = {
    name: 'createUser',
    qualifiedName: 'p.src.services.userService.createUser',
    filePath: 'src/services/userService.ts',
    line: 22,
};

function fakeIndex(): BugPathIndex & { queries: string[] } {
    const queries: string[] = [];
    return {
        queries,
        async queryRows(project, query) {
            expect(project).toBe(PROJECT);
            queries.push(query);
            const incoming = /b\.qualified_name = "([^"]+)"/.exec(query);
            if (incoming !== null) {
                return (CALLERS[incoming[1]] ?? []).map((caller) => ({
                    [COLUMNS.callsIn[0]]: caller.name,
                    [COLUMNS.callsIn[1]]: caller.qualifiedName,
                    [COLUMNS.callsIn[2]]: caller.filePath,
                    [COLUMNS.callsIn[3]]: String(caller.line),
                    [COLUMNS.callsIn[4]]: 'false',
                    [COLUMNS.callsIn[5]]: '1',
                }));
            }
            if (query.includes('MATCH (n:Function)') || query.includes('MATCH (n:Method)')) {
                const wanted = [...query.matchAll(/n\.file_path = "([^"]+)"/g)].map((match) => match[1]);
                return DECLARATIONS
                    .filter((entry) => wanted.includes(entry.filePath) && query.includes('(n:Function)'))
                    .map((entry) => ({
                        [COLUMNS.declarations[0]]: entry.name,
                        [COLUMNS.declarations[1]]: entry.qualifiedName,
                        [COLUMNS.declarations[2]]: entry.filePath,
                        [COLUMNS.declarations[3]]: String(entry.line),
                        [COLUMNS.declarations[4]]: String(entry.line + 4),
                        [COLUMNS.declarations[5]]: 'false',
                    }));
            }
            return [];
        },
    };
}

const OBSERVED = { count: 3, label: 'smoke-run', lastSeen: '2026-08-28T19:42:14Z' };

/** Der Weg, den /api/trace von main nach createUser meldet, mit einem beobachteten Hop. */
const MAIN_TO_CREATE_USER: TraceAnswer = {
    mode: 'calls',
    reachable: true,
    hops: 3,
    path: [
        { name: 'main', filePath: 'src/server.ts' },
        { name: 'createApp', filePath: 'src/server.ts' },
        { name: 'registerUserRoutes', filePath: 'src/routes/users.ts' },
        { name: 'createUser', filePath: 'src/services/userService.ts', observed: OBSERVED },
    ],
};

const CREATE_TO_CREATE_USER: TraceAnswer = {
    mode: 'calls',
    reachable: true,
    hops: 1,
    path: [
        { name: 'create', filePath: 'src/services/userService.ts' },
        { name: 'createUser', filePath: 'src/services/userService.ts' },
    ],
};

/** Der Ablauf createUser -> query, mit der beobachteten Kante unterhalb des Ziels. */
const FLOW_FROM_CREATE_USER: FlowDetail = {
    id: 4,
    entry: { name: 'createUser', filePath: 'src/services/userService.ts' },
    terminal: { name: 'query', filePath: 'src/repo/db.ts' },
    steps: [
        { name: 'createUser', filePath: 'src/services/userService.ts', depth: 0, parent: -1 },
        { name: 'listUsers', filePath: 'src/services/userService.ts', depth: 1, parent: 0 },
        { name: 'validateUser', filePath: 'src/util/validate.ts', depth: 1, parent: 0, observed: OBSERVED },
    ],
};

const FLOW_SUMMARIES: FlowSummary[] = [
    {
        id: 4,
        label: 'createUser -> query',
        entry: { name: 'createUser', filePath: 'src/services/userService.ts' },
        terminal: { name: 'query', filePath: 'src/repo/db.ts' },
        steps: 3,
    },
    {
        id: 6,
        label: 'getOrder -> query',
        entry: { name: 'getOrder', filePath: 'src/services/orderService.ts' },
        terminal: { name: 'query', filePath: 'src/repo/db.ts' },
        steps: 2,
    },
];

const UNRELATED_FLOW: FlowDetail = {
    id: 6,
    entry: { name: 'getOrder', filePath: 'src/services/orderService.ts' },
    terminal: { name: 'query', filePath: 'src/repo/db.ts' },
    steps: [
        { name: 'getOrder', filePath: 'src/services/orderService.ts', depth: 0, parent: -1 },
        { name: 'query', filePath: 'src/repo/db.ts', depth: 1, parent: 0 },
    ],
};

/** Was der Server auf einen Trace von einem nicht-aufrufbaren Knoten sagt. */
const NOT_A_CALLABLE: TraceAnswer = {
    mode: 'calls',
    reachable: false,
    error: 'source is not an indexed callable',
    path: [],
};

function fakeObservations(over: Partial<BugPathObservations> = {}): BugPathObservations {
    return {
        async trace(_project, from) {
            if (from === 'p.src.server.main') {
                return MAIN_TO_CREATE_USER;
            }
            if (from === 'p.src.services.userService.create') {
                return CREATE_TO_CREATE_USER;
            }
            return NOT_A_CALLABLE;
        },
        async flows() {
            return FLOW_SUMMARIES;
        },
        async flow(_project, id) {
            return id === 4 ? FLOW_FROM_CREATE_USER : UNRELATED_FLOW;
        },
        ...over,
    };
}

/** Eine Fassung ohne jede Beobachtung: derselbe Graph, kein Lauf eingespielt. */
function noRecording(): BugPathObservations {
    return fakeObservations({
        async trace() {
            return {
                ...MAIN_TO_CREATE_USER,
                path: MAIN_TO_CREATE_USER.path.map((node) => ({ name: node.name, filePath: node.filePath })),
            };
        },
        async flow(_project, id) {
            return id === 4
                ? { ...FLOW_FROM_CREATE_USER, steps: FLOW_FROM_CREATE_USER.steps.map(({ observed: _drop, ...rest }) => rest) }
                : UNRELATED_FLOW;
        },
    });
}

describe('staticChains', () => {

    it('walks upwards to what nothing calls and marks the head', async () => {
        const { chains, truncated } = await staticChains(fakeIndex(), PROJECT, TARGET, BUG_PATH_DEFAULT_DEPTH, 6);
        expect(chains.map((chain) => chain.map((node) => node.name))).toEqual([
            ['main', 'createApp', 'registerUserRoutes', 'createUser'],
            ['create', 'createUser'],
        ]);
        expect(chains[0][0].entryPoint).toBe(true);
        expect(chains[1][0].entryPoint).toBe(true);
        expect(truncated).toBe(false);
    });

    it('says so when the depth ran out with callers still to follow', async () => {
        const { chains, truncated } = await staticChains(fakeIndex(), PROJECT, TARGET, 2, 6);
        expect(truncated).toBe(true);
        // The cut chain is still shown, because half a way in is a reading.
        expect(chains.some((chain) => chain[0].name === 'createApp')).toBe(true);
        expect(chains.every((chain) => chain[0].entryPoint !== true || chain[0].name === 'create')).toBe(true);
    });

    it('stops at a cycle instead of walking it, and keeps the chain', async () => {
        const cyclic: BugPathIndex = {
            async queryRows(_project, query) {
                const incoming = /b\.qualified_name = "([^"]+)"/.exec(query);
                const target = incoming?.[1];
                const row = (name: string, qualifiedName: string): Record<string, string> => ({
                    [COLUMNS.callsIn[0]]: name,
                    [COLUMNS.callsIn[1]]: qualifiedName,
                    [COLUMNS.callsIn[2]]: 'src/a.ts',
                    [COLUMNS.callsIn[3]]: '1',
                    [COLUMNS.callsIn[4]]: 'false',
                    [COLUMNS.callsIn[5]]: '1',
                });
                if (target === 'p.a') {
                    return [row('b', 'p.b')];
                }
                if (target === 'p.b') {
                    return [row('a', 'p.a')];
                }
                return [];
            },
        };
        const { chains } = await staticChains(
            cyclic,
            PROJECT,
            { name: 'a', qualifiedName: 'p.a', filePath: 'src/a.ts' },
            6,
            6,
        );
        expect(chains).toHaveLength(1);
        expect(chains[0].map((node) => node.name)).toEqual(['b', 'a']);
    });

    it('reports a caller list the engine refused as the end of the walk, not as an entry point', async () => {
        const refusing: BugPathIndex = {
            async queryRows() {
                throw new Error('refused');
            },
        };
        const { chains } = await staticChains(refusing, PROJECT, TARGET, 4, 6);
        expect(chains).toHaveLength(1);
        expect(chains[0][0].entryPoint).toBe(true);
    });
});

describe('observedRuns', () => {

    it('keeps a run together and never joins two across a silent hop', () => {
        const runs = observedRuns([
            { name: 'a' },
            { name: 'b', observed: OBSERVED },
            { name: 'c' },
            { name: 'd', observed: OBSERVED },
        ]);
        expect(runs.map((run) => run.map((node) => node.name))).toEqual([['a', 'b'], ['c', 'd']]);
    });

    it('reports nothing when nothing was observed', () => {
        expect(observedRuns([{ name: 'a' }, { name: 'b' }])).toEqual([]);
    });
});

describe('flowObservations and flowTouches', () => {

    it('attributes an observation to the pair the server named', () => {
        expect(flowObservations(FLOW_FROM_CREATE_USER).map((entry) => edgeKey(entry.from, entry.to)))
            .toEqual([edgeKey(
                { name: 'createUser', filePath: 'src/services/userService.ts' },
                { name: 'validateUser', filePath: 'src/util/validate.ts' },
            )]);
    });

    it('finds the target by file and name, and misses a walk that does not hold it', () => {
        expect(flowTouches(FLOW_FROM_CREATE_USER, TARGET)).toBe(true);
        expect(flowTouches(UNRELATED_FLOW, TARGET)).toBe(false);
    });
});

describe('chainEdges', () => {

    it('turns chains into ordered pairs and reports one pair once', () => {
        const edges = chainEdges([
            [{ name: 'a' }, { name: 'b' }, { name: 'c' }],
            [{ name: 'a' }, { name: 'b' }],
        ]);
        expect([...edges.keys()]).toEqual([
            edgeKey({ name: 'a' }, { name: 'b' }),
            edgeKey({ name: 'b' }, { name: 'c' }),
        ]);
    });
});

describe('bugPaths', () => {

    it('reads the index, the traces and the flows into one document', async () => {
        const paths = await bugPaths(fakeIndex(), fakeObservations(), TARGET, { project: PROJECT });

        expect(paths.staticPaths).toHaveLength(2);
        expect(paths.observedEvents).toBe(2);
        expect(paths.observedPaths.map((chain) => chain.map((node) => node.name)))
            .toEqual([['registerUserRoutes', 'createUser']]);
        expect(paths.observedPaths[0][1].observed).toEqual(OBSERVED);
        expect(paths.truncated).toBe(false);
        expect(paths.depth).toBe(BUG_PATH_DEFAULT_DEPTH);
        expect(paths.flowsRead).toBe(2);
        expect(paths.flowsTruncated).toBe(false);
    });

    it('splits the divergence into expected-never-observed and observed-off-the-chains', async () => {
        const paths = await bugPaths(fakeIndex(), fakeObservations(), TARGET, { project: PROJECT });

        expect(paths.staticOnly.map((edge) => `${edge.from.name}->${edge.to.name}`).sort()).toEqual([
            'create->createUser',
            'createApp->registerUserRoutes',
            'main->createApp',
        ]);
        expect(paths.runtimeOnly.map((edge) => `${edge.from.name}->${edge.to.name}`))
            .toEqual(['createUser->validateUser']);
        // The row says which of the two things it is, asked of the index rather
        // than assumed: this call is recorded, it is simply not on a way in.
        expect(paths.runtimeOnly[0].indexRecordsCall).toBe(true);
        expect(paths.runtimeOnly[0].observed).toEqual(OBSERVED);
        // And it carries the identity a click needs, recovered from the index.
        expect(paths.runtimeOnly[0].to.qualifiedName).toBe('p.src.util.validate.validateUser');
    });

    it('reports an observed call the index has no relation for as exactly that', async () => {
        const index = fakeIndex();
        const observations = fakeObservations({
            async flow(_project, id) {
                return id === 4
                    ? {
                        ...FLOW_FROM_CREATE_USER,
                        steps: [
                            FLOW_FROM_CREATE_USER.steps[0],
                            { name: 'listUsers', filePath: 'src/services/userService.ts', depth: 1, parent: 0 },
                            {
                                name: 'validateUser',
                                filePath: 'src/util/validate.ts',
                                depth: 2,
                                parent: 1,
                                observed: OBSERVED,
                            },
                        ],
                    }
                    : UNRELATED_FLOW;
            },
        });
        const paths = await bugPaths(index, observations, TARGET, { project: PROJECT });
        const edge = paths.runtimeOnly.find((entry) => entry.from.name === 'listUsers');
        expect(edge).toBeDefined();
        // The fixture records no call from listUsers to validateUser.
        expect(edge!.indexRecordsCall).toBe(false);
    });

    it('asks the next hop when a chain starts at something the trace cannot resolve', async () => {
        // Der Kopf der Kette ist der Modul-Knoten, der `main` auf Dateiebene
        // aufruft: der Index kennt die Kante, der Trace loest den Knoten nicht
        // auf. Wer hier stehen bliebe, verloere jede Beobachtung dieser Kette.
        const withModuleHead: BugPathIndex = {
            async queryRows(project, query) {
                const incoming = /b\.qualified_name = "([^"]+)"/.exec(query);
                if (incoming?.[1] === 'p.src.server.main') {
                    return [{
                        [COLUMNS.callsIn[0]]: 'src/server.ts',
                        [COLUMNS.callsIn[1]]: 'p.src.server',
                        [COLUMNS.callsIn[2]]: 'src/server.ts',
                        [COLUMNS.callsIn[3]]: '1',
                        [COLUMNS.callsIn[4]]: 'false',
                        [COLUMNS.callsIn[5]]: '50',
                    }];
                }
                return fakeIndex().queryRows(project, query);
            },
        };
        const asked: string[] = [];
        const observations = fakeObservations({
            async trace(project, from, to) {
                asked.push(from);
                return fakeObservations().trace(project, from, to);
            },
        });
        const paths = await bugPaths(withModuleHead, observations, TARGET, { project: PROJECT, depth: 5 });
        expect(paths.staticPaths[0].map((node) => node.name)[0]).toBe('src/server.ts');
        // Erst der Modul-Knoten, dann main. Und dann steht die Beobachtung da.
        expect(asked.slice(0, 2)).toEqual(['p.src.server', 'p.src.server.main']);
        expect(paths.observedPaths.map((chain) => chain.map((node) => node.name)))
            .toEqual([['registerUserRoutes', 'createUser']]);
    });

    it('says nothing was observed rather than nothing exists, when no run was fed in', async () => {
        const paths = await bugPaths(fakeIndex(), noRecording(), TARGET, { project: PROJECT });
        expect(paths.observedEvents).toBe(0);
        expect(paths.observedPaths).toEqual([]);
        expect(paths.runtimeOnly).toEqual([]);
        // Every expected call is then expected-never-observed, which is true.
        expect(paths.staticOnly).toHaveLength(4);
    });

    it('keeps the two lists when the ranked walks cannot be read at all', async () => {
        const observations = fakeObservations({
            async flows() {
                throw new Error('no flows here');
            },
        });
        const paths = await bugPaths(fakeIndex(), observations, TARGET, { project: PROJECT });
        expect(paths.flowsRead).toBe(0);
        expect(paths.observedEvents).toBe(1);
        expect(paths.staticOnly).toHaveLength(3);
    });

    it('answers a target with no qualified name without asking anything of the routes', async () => {
        const trace = vi.fn();
        const paths = await bugPaths(
            fakeIndex(),
            fakeObservations({ trace: trace as unknown as BugPathObservations['trace'] }),
            { name: 'createUser' },
            { project: PROJECT },
        );
        expect(trace).not.toHaveBeenCalled();
        expect(paths.staticPaths).toEqual([[{ name: 'createUser' }]]);
    });
});

describe('resolveHop', () => {

    const hit: SymbolSearchHit = {
        name: 'validateUser',
        qualifiedName: 'p.src.util.validate.validateUser',
        kind: 'function',
        filePath: 'src/util/validate.ts',
        line: 19,
    };

    const symbol = {
        name: 'validateUser',
        qualifiedName: 'p.src.util.validate.validateUser',
        kind: 'function' as const,
        uri: 'file:///w/src/util/validate.ts',
        range: { start: { line: 18, character: 0 }, end: { line: 22, character: 0 } },
    };

    it('resolves a hop that already names a file and a line without searching', async () => {
        const searchSymbols = vi.fn(async () => [] as SymbolSearchHit[]);
        const resolveSymbolAt = vi.fn(async () => ({ kind: 'ok' as const, symbol, enclosing: [] }));
        const resolved = await resolveHop(
            { searchSymbols, resolveSymbolAt },
            '/w',
            { name: 'validateUser', filePath: 'src/util/validate.ts', line: 19 },
        );
        expect(resolved).toBe(symbol);
        expect(searchSymbols).not.toHaveBeenCalled();
        expect(resolveSymbolAt).toHaveBeenCalledWith('/w', 'src/util/validate.ts', 19, {});
    });

    it('searches for a bare name and then resolves what the index answered', async () => {
        const searchSymbols = vi.fn(async () => [hit]);
        const resolveSymbolAt = vi.fn(async () => ({ kind: 'ok' as const, symbol, enclosing: [] }));
        const resolved = await resolveHop({ searchSymbols, resolveSymbolAt }, '/w', { name: 'validateUser' });
        expect(resolved).toBe(symbol);
        expect(resolveSymbolAt).toHaveBeenCalledWith('/w', 'src/util/validate.ts', 19, {});
    });

    it('opens nothing when the index does not know the name', async () => {
        const searchSymbols = vi.fn(async () => [] as SymbolSearchHit[]);
        const resolveSymbolAt = vi.fn(async () => ({ kind: 'no-symbol-at-line' as const, filePath: 'x' }));
        expect(await resolveHop({ searchSymbols, resolveSymbolAt }, '/w', { name: 'nowhere' })).toBeUndefined();
    });
});

describe('pickHit', () => {

    const hits: SymbolSearchHit[] = [
        { name: 'create', qualifiedName: 'p.src.services.orderService.create', kind: 'function', filePath: 'src/services/orderService.ts', line: 27 },
        { name: 'create', qualifiedName: 'p.src.services.userService.create', kind: 'function', filePath: 'src/services/userService.ts', line: 41 },
    ];

    it('prefers the qualified name over everything else', () => {
        expect(pickHit(hits, { name: 'create', qualifiedName: 'p.src.services.userService.create' })?.filePath)
            .toBe('src/services/userService.ts');
    });

    it('falls back to the file when no qualified name was carried', () => {
        expect(pickHit(hits, { name: 'create', filePath: 'src/services/orderService.ts' })?.qualifiedName)
            .toBe('p.src.services.orderService.create');
    });

    it('falls back to the bare name last', () => {
        expect(pickHit(hits, { name: 'create' })?.qualifiedName).toBe('p.src.services.orderService.create');
    });
});

describe('nodeKey', () => {

    it('tells two symbols of one name in two files apart', () => {
        expect(nodeKey({ name: 'create', filePath: 'a.ts' })).not.toBe(nodeKey({ name: 'create', filePath: 'b.ts' }));
    });
});
