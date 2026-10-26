/**
 * The forward walk, against an invented graph.
 *
 * The fixture is the one the reference suite uses
 * (CodeAtlasIDE, theia-extensions/codeatlas-intelligence/test/intelligence-server-closure.test.ts):
 * a route registration reaching a service and a repository, with the callees of
 * each symbol deliberately not in alphabetical order, so that any ordering in
 * the answer has to be the walk's doing rather than the provider's.
 *
 * What is proven here and cannot be proven in a browser run: that two walks over
 * one graph are the same document, that a cycle terminates and still shows the
 * edge that closes it, that each bound is visible when it bites, and that a
 * callee the index refuses to resolve is kept without an index identity instead
 * of being dropped or invented.
 */

import { describe, expect, it } from 'vitest';

import type { SymbolRef } from '../core/focus-protocol';
import type { CallSite } from '../core/semantic-ir';
import type { ProviderQueryOptions, ResolveResult, SymbolFacts } from '../core/intelligence-provider';
import { toEditorRange } from '../core/positions';
import {
    CLOSURE_MAX_CAP,
    CLOSURE_MAX_DEPTH,
    clampBound,
    closureKeyOf,
    getClosure,
} from './closure';
import type { ClosureSource } from './closure';

const ROOT = '/workspace';
const PROJECT = 'p';

/** Where each symbol is declared, so a resolution can answer with a real place. */
const DECLARED: Record<string, { file: string; line: number }> = {
    registerUserRoutes: { file: 'src/routes/users.ts', line: 7 },
    createUser: { file: 'src/services/userService.ts', line: 23 },
    listUsers: { file: 'src/services/userService.ts', line: 18 },
    toUser: { file: 'src/services/userService.ts', line: 9 },
    validateUser: { file: 'src/util/validate.ts', line: 19 },
    query: { file: 'src/repo/db.ts', line: 17 },
    insert: { file: 'src/repo/db.ts', line: 31 },
    ping: { file: 'src/ring.ts', line: 2 },
    pong: { file: 'src/ring.ts', line: 6 },
};

const qualified = (name: string): string => `${PROJECT}.${name}`;

function symbolNamed(name: string): SymbolRef {
    const where = DECLARED[name];
    return {
        nodeId: qualified(name),
        name,
        qualifiedName: qualified(name),
        kind: 'function',
        uri: `file://${ROOT}/${where.file}`,
        range: toEditorRange(where.line, where.line),
        selectionRange: toEditorRange(where.line, where.line),
        projectName: PROJECT,
    };
}

/** One call site, as the provider reports it: call line here, declaration there. */
function callTo(name: string, line: number): CallSite {
    const where = DECLARED[name];
    return {
        targetName: name,
        targetQualifiedName: qualified(name),
        targetFile: where.file,
        line,
        targetLine: where.line,
    };
}

/** The fixture's own shape: a route registration reaching a service and a repository. */
const USER_GRAPH: Record<string, CallSite[]> = {
    // Deliberately not alphabetical, so ordering has to be the walk's doing.
    registerUserRoutes: [callTo('listUsers', 10), callTo('createUser', 15)],
    createUser: [
        callTo('toUser', 35),
        callTo('validateUser', 24),
        callTo('insert', 30),
        callTo('listUsers', 29),
    ],
    listUsers: [callTo('query', 19), callTo('toUser', 20)],
    query: [],
    insert: [],
    toUser: [],
    validateUser: [],
};

/** Two symbols that call each other, which is where a naive walk never returns. */
const CYCLE_GRAPH: Record<string, CallSite[]> = {
    ping: [callTo('pong', 2)],
    pong: [callTo('ping', 6)],
};

interface Recorder extends ClosureSource {
    factCalls: string[];
    resolveCalls: string[];
    unresolvable: Set<string>;
}

/** A provider slice over one invented graph, with its calls written down. */
function sourceOver(graph: Record<string, CallSite[]>): Recorder {
    const factCalls: string[] = [];
    const resolveCalls: string[] = [];
    const unresolvable = new Set<string>();
    return {
        factCalls,
        resolveCalls,
        unresolvable,
        async getFacts(
            _root: string,
            symbol: SymbolRef,
            kinds: string[],
            _opts?: ProviderQueryOptions,
        ): Promise<SymbolFacts> {
            expect(kinds).toEqual(['callees']);
            factCalls.push(symbol.qualifiedName ?? symbol.name);
            const short = (symbol.qualifiedName ?? '').slice(PROJECT.length + 1);
            return {
                callees: { value: graph[short] ?? [], state: 'known', evidence: [] },
            };
        },
        async resolveSymbolAt(
            _root: string,
            filePath: string,
            line: number,
            _opts?: ProviderQueryOptions,
        ): Promise<ResolveResult> {
            resolveCalls.push(`${filePath}:${line}`);
            if (unresolvable.has(filePath)) {
                return { kind: 'file-not-indexed', filePath };
            }
            const name = Object.keys(DECLARED).find(
                (key) => DECLARED[key].file === filePath && DECLARED[key].line === line,
            );
            if (name === undefined) {
                return { kind: 'no-symbol-at-line', filePath };
            }
            return { kind: 'ok', symbol: symbolNamed(name), enclosing: [] };
        },
    };
}

const shortNames = (closure: { nodes: { symbol: SymbolRef }[] }): string[] =>
    closure.nodes.map((node) => node.symbol.name);

describe('walking the connected part of the graph', () => {
    it('returns the root first and then each layer, alphabetically within the layer', async () => {
        const closure = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'));
        expect(shortNames(closure)).toEqual([
            'registerUserRoutes',
            // layer 1, alphabetical rather than in the order the provider listed
            'createUser', 'listUsers',
            // layer 2, likewise
            'insert', 'query', 'toUser', 'validateUser',
        ]);
    });

    it('records the hop and the symbol whose call reached each node', async () => {
        const closure = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'));
        expect(closure.nodes[0]).toMatchObject({ hop: 0 });
        expect(closure.nodes[0].via).toBeUndefined();
        const createUser = closure.nodes.find((node) => node.symbol.name === 'createUser');
        expect(createUser).toMatchObject({ hop: 1, via: qualified('registerUserRoutes') });
        const validateUser = closure.nodes.find((node) => node.symbol.name === 'validateUser');
        expect(validateUser).toMatchObject({ hop: 2, via: qualified('createUser') });
    });

    it('answers the same document twice, whatever the provider is asked', async () => {
        const first = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'));
        const second = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'));
        expect(JSON.stringify(second)).toBe(JSON.stringify(first));
    });

    it('joins every edge to a symbol it also returned', async () => {
        const closure = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'));
        const known = new Set(closure.nodes.map((node) => closureKeyOf(node.symbol)));
        for (const edge of closure.edges) {
            expect(known.has(edge.from)).toBe(true);
            expect(known.has(edge.to)).toBe(true);
        }
        expect(closure.edges.length).toBeGreaterThan(0);
    });

    it('carries the call site line on the edge', async () => {
        const closure = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'));
        const edge = closure.edges.find(
            (entry) => entry.from.endsWith('registerUserRoutes') && entry.to.endsWith('createUser'),
        );
        expect(edge?.line).toBe(15);
    });

    it('resolves every published symbol against the declaration, never the call site', async () => {
        const source = sourceOver(USER_GRAPH);
        const closure = await getClosure(source, ROOT, symbolNamed('registerUserRoutes'));
        for (const node of closure.nodes) {
            expect(node.symbol.nodeId).toBeTruthy();
            expect(node.symbol.uri.startsWith('file://')).toBe(true);
        }
        // `createUser` is called from line 15 of users.ts and declared on line
        // 23 of userService.ts. Only the second is ever asked about.
        expect(source.resolveCalls).toContain('src/services/userService.ts:23');
        expect(source.resolveCalls).not.toContain('src/routes/users.ts:15');
    });

    it('resolves each reached symbol once, however many edges point at it', async () => {
        const source = sourceOver(USER_GRAPH);
        await getClosure(source, ROOT, symbolNamed('registerUserRoutes'));
        const listUsers = source.resolveCalls.filter((call) => call === 'src/services/userService.ts:18');
        expect(listUsers).toHaveLength(1);
    });

    it('keeps an unresolved callee rather than dropping it, and gives it no node id', async () => {
        const source = sourceOver(USER_GRAPH);
        source.unresolvable.add('src/util/validate.ts');
        const closure = await getClosure(source, ROOT, symbolNamed('registerUserRoutes'));
        const unresolved = closure.nodes.find((node) => node.symbol.name === 'validateUser');
        expect(unresolved).toBeDefined();
        expect(unresolved?.symbol.nodeId).toBeUndefined();
        expect(unresolved?.symbol.qualifiedName).toBe(qualified('validateUser'));
    });

    it('terminates on a cycle and still records the edge that closes it', async () => {
        const closure = await getClosure(sourceOver(CYCLE_GRAPH), ROOT, symbolNamed('ping'), {
            depth: 5,
            cap: 10,
        });
        expect(shortNames(closure)).toEqual(['ping', 'pong']);
        expect(closure.edges).toEqual([
            { from: qualified('ping'), to: qualified('pong'), line: 2 },
            { from: qualified('pong'), to: qualified('ping'), line: 6 },
        ]);
        expect(closure.truncated).toBe(false);
    });

    it('expands each symbol once, so a cycle costs one fact read per symbol', async () => {
        const source = sourceOver(CYCLE_GRAPH);
        await getClosure(source, ROOT, symbolNamed('ping'), { depth: 5, cap: 10 });
        expect(source.factCalls).toEqual([qualified('ping'), qualified('pong')]);
    });

    it('stops at the cap, says so, and counts what it turned away', async () => {
        const closure = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'), {
            cap: 2,
        });
        expect(closure.nodes).toHaveLength(2);
        expect(closure.truncated).toBe(true);
        expect(closure.visited).toBeGreaterThan(closure.nodes.length);
        const known = new Set(closure.nodes.map((node) => closureKeyOf(node.symbol)));
        for (const edge of closure.edges) {
            expect(known.has(edge.to)).toBe(true);
        }
    });

    it('says so when the depth runs out with somewhere still to go', async () => {
        const closure = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'), {
            depth: 1,
        });
        expect(shortNames(closure)).toEqual(['registerUserRoutes', 'createUser', 'listUsers']);
        expect(closure.truncated).toBe(true);
    });

    it('does not claim truncation when the walk reached the end of the graph', async () => {
        const closure = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'), {
            depth: 4,
        });
        expect(closure.truncated).toBe(false);
        expect(closure.visited).toBe(closure.nodes.length);
    });

    it('carries the bounds it actually honoured', async () => {
        const closure = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'));
        expect(closure.depth).toBe(3);
        expect(closure.cap).toBe(15);
    });

    it('clamps a bound nobody should have asked for', async () => {
        const closure = await getClosure(sourceOver(USER_GRAPH), ROOT, symbolNamed('registerUserRoutes'), {
            depth: 900,
            cap: 9000,
        });
        expect(closure.depth).toBeLessThanOrEqual(CLOSURE_MAX_DEPTH);
        expect(closure.cap).toBeLessThanOrEqual(CLOSURE_MAX_CAP);
    });

    it('answers with the root alone when nothing could be read', async () => {
        const source = sourceOver(USER_GRAPH);
        const failing: ClosureSource = {
            getFacts: () => Promise.reject(new Error('engine unavailable')),
            resolveSymbolAt: source.resolveSymbolAt,
        };
        const closure = await getClosure(failing, ROOT, symbolNamed('registerUserRoutes'));
        expect(shortNames(closure)).toEqual(['registerUserRoutes']);
        expect(closure.truncated).toBe(false);
        expect(closure.visited).toBe(1);
    });

    it('ignores a call the index could not name a target for', async () => {
        const nameless: Record<string, CallSite[]> = {
            registerUserRoutes: [{ targetName: 'anonymous', line: 3 }],
        };
        const closure = await getClosure(sourceOver(nameless), ROOT, symbolNamed('registerUserRoutes'));
        expect(shortNames(closure)).toEqual(['registerUserRoutes']);
        expect(closure.edges).toEqual([]);
    });
});

describe('the bounds themselves', () => {
    it('falls back when nobody named one', () => {
        expect(clampBound(undefined, 3, 6)).toBe(3);
        expect(clampBound(Number.NaN, 3, 6)).toBe(3);
    });

    it('never goes below one hop and never above the ceiling', () => {
        expect(clampBound(0, 3, 6)).toBe(1);
        expect(clampBound(-4, 3, 6)).toBe(1);
        expect(clampBound(99, 3, 6)).toBe(6);
        expect(clampBound(2.7, 3, 6)).toBe(2);
    });
});
