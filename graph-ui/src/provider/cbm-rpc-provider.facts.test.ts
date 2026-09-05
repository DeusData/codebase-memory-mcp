/*
 * Portiert am 2026-08-28 aus CodeAtlasIDE,
 * theia-extensions/codeatlas-intelligence/test/cbm-provider.facts.test.ts.
 * Dieselben Faelle, dieselben Erwartungen; ersetzt ist nur, was der Provider
 * unter sich hat: statt eines FakeTransport mit CLI-JSON antwortet hier ein
 * fetch mit der kompakten Zeilenform, die der Server aus PR 1860 wirklich
 * schickt. Die Tests laufen also durch rpc-transport, den Parser und den
 * rpc-client hindurch und nicht daran vorbei.
 */

import { describe, expect, it } from 'vitest';
import type { SymbolRef } from '../core/focus-protocol';
import {
    CbmRpcProvider,
    EVIDENCE_RELATIONS,
    STRATEGIES,
    knowledgeStateFor,
} from './cbm-rpc-provider';
import { RpcIntelligenceClient } from './rpc-client';
import {
    FakeRpc,
    RECORDED_PROJECT,
    RECORDED_ROOT,
    listProjectsRoute,
    queryContains,
    rowsText,
} from '../test-support/rpc-recordings';
import type { Route } from '../test-support/rpc-recordings';

const ROOT = RECORDED_ROOT;

const CREATE_USER: SymbolRef = {
    name: 'createUser',
    qualifiedName: `${RECORDED_PROJECT}.src.services.userService.createUser`,
    kind: 'function',
    uri: `file://${ROOT}/src/services/userService.ts`,
    // 1-basierte Graphzeilen 23 bis 36, in 0-basierten Editorraum umgerechnet.
    range: { start: { line: 22, character: 0 }, end: { line: 35, character: 0 } },
    projectName: RECORDED_PROJECT,
};

const LIST_USERS: SymbolRef = {
    ...CREATE_USER,
    name: 'listUsers',
    qualifiedName: `${RECORDED_PROJECT}.src.services.userService.listUsers`,
};

const THROW_COLUMNS = ['b.name', 'b.file_path', 'b.start_line', 'r.line'];
const emptyTable = rowsText(['b.name'], []);

function provider(routes: Route[], generation = 0): { provider: CbmRpcProvider; rpc: FakeRpc } {
    const rpc = new FakeRpc(routes);
    return {
        provider: new CbmRpcProvider(new RpcIntelligenceClient({ fetch: rpc.fetch }), { generation }),
        rpc,
    };
}

describe('the knowledge state table', () => {

    // One row per reachable combination. The table is the specification: any
    // change to it is a change to what the product claims to know.
    const cases: [string, Parameters<typeof knowledgeStateFor>[0], string][] = [
        ['no engine at all outranks everything',
            { engineAvailable: false, indexed: true, supported: true, derived: false }, 'unknown'],
        ['no engine, even for a derived family',
            { engineAvailable: false, indexed: true, supported: true, derived: true }, 'unknown'],
        ['no engine, even for an unsupported family',
            { engineAvailable: false, indexed: false, supported: false, derived: false }, 'unknown'],
        ['an unsupported family stays unsupported however good the index is',
            { engineAvailable: true, indexed: true, supported: false, derived: false }, 'unsupported'],
        ['an unsupported family is not rescued by a heuristic',
            { engineAvailable: true, indexed: true, supported: false, derived: true }, 'unsupported'],
        ['a supported family over an unindexed file is not indexed',
            { engineAvailable: true, indexed: false, supported: true, derived: false }, 'notIndexed'],
        ['a derived family over an unindexed file is not indexed either',
            { engineAvailable: true, indexed: false, supported: true, derived: true }, 'notIndexed'],
        ['a reading from a healthy index is known',
            { engineAvailable: true, indexed: true, supported: true, derived: false }, 'known'],
        ['a heuristic over a healthy index is inferred, never known',
            { engineAvailable: true, indexed: true, supported: true, derived: true }, 'inferred'],
    ];

    it.each(cases)('%s', (_label, context, expected) => {
        expect(knowledgeStateFor(context)).toBe(expected);
    });

    it('never returns the ambiguous state, which belongs to resolution, not to facts', () => {
        for (const [, context] of cases) {
            expect(knowledgeStateFor(context)).not.toBe('ambiguous');
        }
    });
});

describe('outgoing invocations', () => {

    const routes: Route[] = [
        { tool: 'query_graph', when: queryContains('(b:Class)'), recording: 'class-targets' },
        { tool: 'query_graph', when: queryContains('r:CALLS]->(b)'), recording: 'calls-out' },
    ];

    it('reads every target, with its file and its last recorded site', async () => {
        const { provider: p } = provider(routes);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['callees']);
        expect(facts.callees?.state).toBe('known');
        expect(facts.callees?.value).toHaveLength(6);
        const first = facts.callees!.value[0];
        expect(first.targetName).toBe('validateUser');
        expect(first.targetFile).toBe('src/util/validate.ts');
        expect(first.line).toBe(24);
    });

    it('keeps the declaration line of a target apart from the call site', async () => {
        const { provider: p } = provider(routes);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['callees']);
        const first = facts.callees!.value[0];
        // "19" and "24" arrive as quoted strings in the compact form; both have
        // to survive as numbers and neither may take the other's place.
        expect(first.targetLine).toBe(19);
        expect(typeof first.line).toBe('number');
    });

    it('marks a class target as a construction and leaves the rest as calls', async () => {
        const { provider: p } = provider(routes);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['callees']);
        const byName = Object.fromEntries(facts.callees!.value.map((site) => [site.targetName, site.strategy]));
        expect(byName.UserEntity).toBe(STRATEGIES.construction);
        expect(byName.ValidationError).toBe(STRATEGIES.construction);
        expect(byName.validateUser).toBe(STRATEGIES.directCall);
        expect(byName.insert).toBe(STRATEGIES.directCall);
    });

    it('cites one piece of evidence per claim, stamped with the generation and the provider', async () => {
        const { provider: p } = provider(routes, 7);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['callees']);
        expect(facts.callees?.evidence).toHaveLength(6);
        const evidence = facts.callees!.evidence[0];
        expect(evidence.source).toBe('graph-edge');
        expect(evidence.relation).toBe(EVIDENCE_RELATIONS.invocation);
        expect(evidence.engineGeneration).toBe(7);
        expect(evidence.providerId).toBe('cbm');
        expect(evidence.range).toEqual({ startLine: 24, endLine: 24 });
    });

    it('returns an empty list that is still known when the symbol simply calls nothing', async () => {
        const { provider: p } = provider([{ tool: 'query_graph', text: emptyTable }]);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['callees']);
        expect(facts.callees).toEqual({ value: [], state: 'known', evidence: [] });
    });
});

describe('incoming invocations', () => {

    const routes: Route[] = [{ tool: 'query_graph', recording: 'calls-in' }];

    it('coerces the string cells into the shapes the product uses', async () => {
        const { provider: p } = provider(routes);
        const facts = await p.getFacts(ROOT, LIST_USERS, ['callers']);
        expect(facts.callers?.state).toBe('known');
        expect(facts.callers?.value).toHaveLength(3);
        expect(facts.callers!.value[1]).toMatchObject({
            name: 'registerUserRoutes',
            file: 'src/routes/users.ts',
            line: 10,
            isTest: false,
        });
        expect(typeof facts.callers!.value[1].line).toBe('number');
    });

    it('carries the test flag the engine recorded on the caller', async () => {
        const { provider: p } = provider(routes);
        const facts = await p.getFacts(ROOT, LIST_USERS, ['callers']);
        const testCaller = facts.callers!.value.find((caller) => caller.isTest);
        expect(testCaller?.name).toBe('test/userService.test.ts');
        expect(testCaller?.sourceKind).toBe('module');
    });
});

describe('tested by, which is inferred and says so', () => {

    it('derives a test from a caller the engine flagged as test code', async () => {
        const { provider: p } = provider([{ tool: 'query_graph', recording: 'calls-in' }]);
        const facts = await p.getFacts(ROOT, LIST_USERS, ['testedBy']);
        expect(facts.testedBy?.state).toBe('inferred');
        expect(facts.testedBy?.value).toHaveLength(1);
        expect(facts.testedBy!.value[0].name).toBe('test/userService.test.ts');
        expect(facts.testedBy!.value[0].file).toBe('test/userService.test.ts');
        expect(facts.testedBy!.value[0].line).toBe(9);
    });

    it('names the heuristic in the evidence and attributes it to the test source', async () => {
        const { provider: p } = provider([{ tool: 'query_graph', recording: 'calls-in' }]);
        const facts = await p.getFacts(ROOT, LIST_USERS, ['testedBy']);
        expect(facts.testedBy!.evidence[0].strategy).toBe(STRATEGIES.testCaller);
        expect(facts.testedBy!.evidence[0].source).toBe('test');
        expect(facts.testedBy!.evidence[0].providerId).toBe('cbm');
    });

    it('stays inferred when it finds nothing, because a heuristic proves no absence', async () => {
        const { provider: p } = provider([{
            tool: 'query_graph',
            text: rowsText(
                ['a.name', 'a.qualified_name', 'a.file_path', 'a.start_line', 'a.is_test', 'r.line'],
                [['createUser', 'p.createUser', 'src/x.ts', '"23"', 'false', '"29"']],
            ),
        }]);
        const facts = await p.getFacts(ROOT, LIST_USERS, ['callers', 'testedBy']);
        expect(facts.callers?.state).toBe('known');
        expect(facts.callers?.value).toHaveLength(1);
        expect(facts.testedBy?.value).toEqual([]);
        expect(facts.testedBy?.state).toBe('inferred');
        expect(facts.testedBy?.state).not.toBe('known');
    });

    it('asks the engine once even when both caller families are requested', async () => {
        const { provider: p, rpc } = provider([{ tool: 'query_graph', recording: 'calls-in' }]);
        await p.getFacts(ROOT, LIST_USERS, ['callers', 'testedBy']);
        expect(rpc.callsTo('query_graph')).toHaveLength(1);
    });
});

describe('the remaining fact families', () => {

    it('reads raised error types, with the declaration line as the honest fallback', async () => {
        const { provider: p } = provider([
            { tool: 'query_graph', when: queryContains('[r:RAISES]'), recording: 'raises' },
            { tool: 'query_graph', when: queryContains('[r:THROWS]'), recording: 'throws-empty' },
        ]);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['throws']);
        expect(facts.throws?.state).toBe('known');
        // Der Server schreibt fuer r.line einen nackten Bindestrich, also hat
        // die Relation keine Site-Zeile, und die Deklarationszeile des Symbols
        // ist der ehrliche Rueckfall.
        expect(facts.throws?.value).toEqual([
            { type: 'ValidationError', file: 'src/util/validate.ts', line: 23 },
        ]);
        expect(facts.throws?.evidence[0].relation).toBe(EVIDENCE_RELATIONS.raise);
    });

    /**
     * Der Java-Uebertrag, gebaut statt aufgezeichnet.
     *
     * Das eingefrorene Fixture ist TypeScript und hat gar keine deklarierte
     * Ausnahme, also gibt es nichts aufzuzeichnen. Die Form unten ist die, die
     * der Server fuer eine Java-Signatur mit throws-Klausel liefert, in die
     * kompakte Zeilenform gebracht. Dass die Relation existiert und leer
     * antwortet, ist dagegen aufgezeichnet: siehe die Aufnahme throws-empty.
     */
    it('reads a declared exception that only the second relation records', async () => {
        const declared = rowsText(THROW_COLUMNS, [[
            'NoAuthorizationException',
            'src/main/java/io/spring/api/exception/NoAuthorizationException.java',
            '"6"',
            '-',
        ]]);
        const { provider: p, rpc } = provider([
            { tool: 'query_graph', when: queryContains('[r:RAISES]'), text: rowsText(THROW_COLUMNS, []) },
            { tool: 'query_graph', when: queryContains('[r:THROWS]'), text: declared },
        ]);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['throws']);
        expect(facts.throws?.state).toBe('known');
        expect(facts.throws?.value).toEqual([{
            type: 'NoAuthorizationException',
            file: 'src/main/java/io/spring/api/exception/NoAuthorizationException.java',
            line: CREATE_USER.range.start.line + 1,
        }]);
        expect(facts.throws?.evidence[0].relation).toBe(EVIDENCE_RELATIONS.throwDeclaration);
        expect(rpc.callsTo('query_graph')).toHaveLength(2);
    });

    it('merges the two relations and reports one fact per error type', async () => {
        const { provider: p } = provider([
            {
                tool: 'query_graph',
                when: queryContains('[r:RAISES]'),
                text: rowsText(THROW_COLUMNS, [['ValidationError', 'src/util/validate.ts', '"4"', '"23"']]),
            },
            {
                tool: 'query_graph',
                when: queryContains('[r:THROWS]'),
                text: rowsText(THROW_COLUMNS, [
                    // Derselbe Befund ueber die andere Relation: eine Tatsache,
                    // nicht zwei.
                    ['ValidationError', 'src/util/validate.ts', '"4"', '"23"'],
                    ['NoAuthorizationException', 'src/exception/NoAuthorizationException.java', '"6"', '-'],
                ]),
            },
        ]);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['throws']);
        expect(facts.throws?.value.map((entry) => entry.type)).toEqual([
            'ValidationError',
            'NoAuthorizationException',
        ]);
        // Die Evidenz haelt die beiden Relationen auseinander, und genau das
        // laesst einen Leser sehen, welche den Befund getragen hat.
        expect(facts.throws?.evidence.map((entry) => entry.relation)).toEqual([
            EVIDENCE_RELATIONS.raise,
            EVIDENCE_RELATIONS.throwDeclaration,
        ]);
        // Und die Woerter selbst, nicht nur die Konstanten: sie stehen in der
        // Evidenz, die ein Leser zu sehen bekommt, und sind damit Teil des
        // Vertrags. Eine umbenannte Konstante mit unveraendertem Wert soll
        // hier durchgehen, ein umbenannter Wert nicht.
        expect(facts.throws?.evidence.map((entry) => entry.relation))
            .toEqual(['raise', 'throw-declaration']);
    });

    it('reads environment values through the configuration relation', async () => {
        const { provider: p } = provider([{ tool: 'query_graph', recording: 'env-reads' }]);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['envReads']);
        expect(facts.envReads?.state).toBe('known');
        expect(facts.envReads?.value[0].name).toBe('DB_URL');
        expect(facts.envReads?.value[0].kind).toBe('global');
        // Weder Datei noch Zeile stehen in der Relation; die Datei des Symbols
        // ist der Rueckfall, die Zeile bleibt weg statt geraten zu werden.
        expect(facts.envReads?.value[0].file).toBe(CREATE_USER.uri);
        expect(facts.envReads?.value[0].line).toBeUndefined();
        expect(facts.envReads?.evidence[0].relation).toBe(EVIDENCE_RELATIONS.environmentRead);
    });

    it('reads type references, which is what the usage relation actually records', async () => {
        const { provider: p } = provider([{ tool: 'query_graph', recording: 'type-refs' }]);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['typeRefs']);
        expect(facts.typeRefs?.state).toBe('known');
        expect(facts.typeRefs?.value[0].name).toBe('User');
        expect(facts.typeRefs?.value[0].qualifiedName).toContain('src.types.User');
        expect(facts.typeRefs?.evidence[0].relation).toBe(EVIDENCE_RELATIONS.typeReference);
    });

    it('returns only the families that were asked for', async () => {
        const { provider: p } = provider([{ tool: 'query_graph', text: rowsText(THROW_COLUMNS, []) }]);
        const facts = await p.getFacts(ROOT, CREATE_USER, ['throws']);
        expect(Object.keys(facts)).toEqual(['throws']);
        expect(facts.callers).toBeUndefined();
    });
});

describe('facts when something is missing', () => {

    it('reports every requested family as unknown when no engine answered', async () => {
        const { provider: p } = provider([{
            tool: 'list_projects',
            networkError: 'fetch failed: kein Server auf 127.0.0.1:4299',
        }]);
        const symbol = { ...CREATE_USER, projectName: undefined };
        const facts = await p.getFacts(ROOT, symbol, ['callees', 'callers', 'throws', 'testedBy']);
        expect(facts.callees?.state).toBe('unknown');
        expect(facts.callers?.state).toBe('unknown');
        expect(facts.throws?.state).toBe('unknown');
        expect(facts.testedBy?.state).toBe('unknown');
        for (const fact of Object.values(facts)) {
            expect(fact.value).toEqual([]);
            expect(fact.evidence).toEqual([]);
        }
    });

    it('reports a workspace with no project as not indexed, which is a different claim', async () => {
        const { provider: p } = provider([{ tool: 'list_projects', json: { projects: [] } }]);
        const symbol = { ...CREATE_USER, projectName: undefined };
        const facts = await p.getFacts(ROOT, symbol, ['callees', 'testedBy']);
        expect(facts.callees?.state).toBe('notIndexed');
        expect(facts.testedBy?.state).toBe('notIndexed');
    });

    it('reports a symbol with no qualified name as not indexed rather than guessing', async () => {
        const { provider: p } = provider([]);
        const facts = await p.getFacts(ROOT, { ...CREATE_USER, qualifiedName: undefined }, ['throws']);
        expect(facts.throws?.state).toBe('notIndexed');
    });

    it('finds the project through the list when the symbol does not name one', async () => {
        const { provider: p, rpc } = provider([
            listProjectsRoute(),
            { tool: 'query_graph', when: queryContains('(b:Class)'), recording: 'class-targets' },
            { tool: 'query_graph', recording: 'calls-out' },
        ]);
        const facts = await p.getFacts(ROOT, { ...CREATE_USER, projectName: undefined }, ['callees']);
        expect(facts.callees?.state).toBe('known');
        expect(rpc.toolsCalled()[0]).toBe('list_projects');
    });
});
