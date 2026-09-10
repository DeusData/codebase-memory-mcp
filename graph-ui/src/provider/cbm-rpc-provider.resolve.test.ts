/*
 * Portiert am 2026-08-28 aus CodeAtlasIDE,
 * theia-extensions/codeatlas-intelligence/test/cbm-provider.resolve.test.ts.
 * Dieselben Faelle; die Aufzeichnungen sind die des Servers aus PR 1860 fuer
 * dieselbe Stelle im Fixture (src/types.ts, Zeile 47: der Konstruktor von
 * UserEntity, umschlossen von der Klasse).
 */

import { describe, expect, it } from 'vitest';
import { CbmRpcProvider, pickInnermost, symbolKindOf } from './cbm-rpc-provider';
import { RpcIntelligenceClient } from './rpc-client';
import {
    FakeRpc,
    RECORDED_PROJECT,
    RECORDED_ROOT,
    listProjectsRoute,
    queryContains,
} from '../test-support/rpc-recordings';
import type { Route } from '../test-support/rpc-recordings';

const ROOT = RECORDED_ROOT;
const FILE = 'src/types.ts';

function providerWith(routes: Route[]): { provider: CbmRpcProvider; rpc: FakeRpc } {
    const rpc = new FakeRpc(routes);
    return { provider: new CbmRpcProvider(new RpcIntelligenceClient({ fetch: rpc.fetch })), rpc };
}

/** Die drei Label-Abfragen fuer die Stelle, an der der Konstruktor steht. */
const enclosingRoutes: Route[] = [
    { tool: 'query_graph', when: queryContains('(n:Method)'), recording: 'enclosing-method' },
    { tool: 'query_graph', when: queryContains('(n:Function)'), recording: 'enclosing-empty' },
    { tool: 'query_graph', when: queryContains('(n:Class)'), recording: 'enclosing-class' },
];

describe('capabilities', () => {

    const bare = (): CbmRpcProvider =>
        new CbmRpcProvider(new RpcIntelligenceClient({ fetch: new FakeRpc([]).fetch }));

    it('declares exactly what this engine can answer, and admits the two gaps', () => {
        expect(bare().capabilities()).toEqual({
            callers: true,
            callees: true,
            throws: true,
            envReads: true,
            typeRefs: true,
            tests: 'heuristic',
            routes: true,
            architecture: true,
            changeImpact: true,
            runtimeTraces: false,
            semanticSearch: true,
            callSiteGranularity: 'per-target',
        });
    });

    it('never claims a first class test relation, because none is recorded for TypeScript', () => {
        expect(bare().capabilities().tests).not.toBe('edges');
    });

    it('identifies itself, so evidence can be attributed and the provider disabled', () => {
        expect(bare().id).toBe('cbm');
    });
});

describe('whether the backend answers', () => {

    it('reports the server as available when it takes the cheapest read', async () => {
        const { provider } = providerWith([listProjectsRoute()]);
        const info = await provider.engineInfo();
        expect(info.available).toBe(true);
        expect(info.providerId).toBe('cbm');
        // Auf /rpc gibt es keine Version. Sie bleibt weg, statt erfunden zu werden.
        expect(info.version).toBeUndefined();
    });

    it('reports a silent server as unavailable and repeats what it said', async () => {
        const { provider } = providerWith([
            { tool: 'list_projects', networkError: 'fetch failed: kein Server auf 127.0.0.1:4299' },
        ]);
        const info = await provider.engineInfo();
        expect(info.available).toBe(false);
        expect(info.detail).toContain('127.0.0.1:4299');
    });

    /**
     * Der Aufruf geht gar nicht erst auf den Draht: die Ablehnung der
     * Allowlist ist aufgezeichnet (403 plus -32601) und steht damit fest, also
     * waere ein Rundlauf nur eine Wiederholung. Was hier geprueft wird, ist
     * dass der Provider ablehnt statt eine Antwort zu erfinden.
     */
    it('says so when the allowlist does not offer a write tool, rather than pretending', async () => {
        const { provider, rpc } = providerWith([]);
        await expect(provider.indexWorkspace(ROOT)).rejects.toThrow(/nicht an/);
        expect(rpc.toolsCalled()).toEqual([]);
    });
});

describe('picking the innermost symbol', () => {

    it('prefers the narrowest declaration span', () => {
        const picked = pickInnermost([
            { label: 'Class', name: 'UserEntity', qualifiedName: 'p.UserEntity', startLine: 37, endLine: 53 },
            { label: 'Method', name: 'constructor', qualifiedName: 'p.UserEntity.constructor', startLine: 43, endLine: 48 },
        ]);
        expect(picked?.name).toBe('constructor');
    });

    it('breaks a tie on span by preferring the narrower label', () => {
        const picked = pickInnermost([
            { label: 'Class', name: 'C', qualifiedName: 'p.C', startLine: 10, endLine: 20 },
            { label: 'Function', name: 'F', qualifiedName: 'p.F', startLine: 10, endLine: 20 },
            { label: 'Method', name: 'M', qualifiedName: 'p.M', startLine: 10, endLine: 20 },
        ]);
        expect(picked?.label).toBe('Method');
    });

    it('answers nothing for nothing', () => {
        expect(pickInnermost([])).toBeUndefined();
    });
});

describe('mapping engine labels to product kinds', () => {

    it('narrows the labels the product models and refuses to guess the rest', () => {
        expect(symbolKindOf('Function')).toBe('function');
        expect(symbolKindOf('Method')).toBe('method');
        expect(symbolKindOf('Class')).toBe('class');
        expect(symbolKindOf('Interface')).toBe('interface');
        expect(symbolKindOf('File')).toBe('module');
        expect(symbolKindOf('EnvVar')).toBe('unknown');
        expect(symbolKindOf(undefined)).toBe('unknown');
    });
});

describe('resolving a caret position', () => {

    it('returns the innermost symbol and the ones that enclose it', async () => {
        const { provider } = providerWith([listProjectsRoute(), ...enclosingRoutes]);
        const result = await provider.resolveSymbolAt(ROOT, FILE, 47);
        expect(result.kind).toBe('ok');
        if (result.kind !== 'ok') {
            return;
        }
        expect(result.symbol.name).toBe('constructor');
        expect(result.symbol.kind).toBe('method');
        expect(result.symbol.qualifiedName).toContain('UserEntity.constructor');
        expect(result.symbol.projectName).toBe(RECORDED_PROJECT);
        // 1-based inclusive graph lines become a 0-based editor range.
        expect(result.symbol.range.start.line).toBe(42);
        expect(result.symbol.range.end.line).toBe(47);
        expect(result.symbol.uri).toBe(`file://${ROOT}/${FILE}`);
        expect(result.enclosing).toHaveLength(1);
        expect(result.enclosing[0].name).toBe('UserEntity');
        expect(result.enclosing[0].kind).toBe('class');
    });

    /**
     * Die Identitaetshaelfte der Antwort, und warum sie nicht kosmetisch ist:
     * das Produkt liest die Anwesenheit von nodeId als "der Index kennt das
     * Ding". Ein aufgeloestes Symbol ohne nodeId wuerde jedem Panel erzaehlen,
     * ein bestens indiziertes Symbol sei nicht indiziert.
     */
    it('gives a resolved symbol a stable node id, so a reveal cannot demote it', async () => {
        const { provider } = providerWith([listProjectsRoute(), ...enclosingRoutes]);
        const result = await provider.resolveSymbolAt(ROOT, FILE, 47);
        expect(result.kind).toBe('ok');
        if (result.kind !== 'ok') {
            return;
        }
        expect(result.symbol.nodeId).toBe(result.symbol.qualifiedName);
        expect(result.symbol.nodeId).toBeTruthy();
        for (const enclosing of result.enclosing) {
            expect(enclosing.nodeId).toBe(enclosing.qualifiedName);
        }
    });

    it('tries one query per label, because a pattern carries at most one', async () => {
        const { provider, rpc } = providerWith([listProjectsRoute(), ...enclosingRoutes]);
        await provider.resolveSymbolAt(ROOT, FILE, 47);
        expect(rpc.callsTo('query_graph')).toHaveLength(3);
    });

    it('separates "nothing at this line" from "this file is not indexed"', async () => {
        const { provider } = providerWith([
            listProjectsRoute(),
            { tool: 'query_graph', when: queryContains('(n:File)'), recording: 'file-exists' },
            { tool: 'query_graph', recording: 'enclosing-empty' },
        ]);
        const result = await provider.resolveSymbolAt(ROOT, FILE, 4);
        expect(result).toEqual({ kind: 'no-symbol-at-line', filePath: FILE });
    });

    it('reports a file the engine has never seen as not indexed', async () => {
        const { provider } = providerWith([
            listProjectsRoute(),
            { tool: 'query_graph', when: queryContains('(n:File)'), recording: 'file-missing' },
            { tool: 'query_graph', recording: 'enclosing-empty' },
        ]);
        const result = await provider.resolveSymbolAt(ROOT, 'src/never-seen.ts', 4);
        expect(result).toEqual({ kind: 'file-not-indexed', filePath: 'src/never-seen.ts' });
    });

    it('reports a workspace the engine holds no project for as not indexed', async () => {
        const { provider } = providerWith([
            { tool: 'list_projects', json: { projects: [{ name: 'something-else', root_path: '/elsewhere' }] } },
        ]);
        const result = await provider.resolveSymbolAt(ROOT, FILE, 47);
        expect(result).toEqual({ kind: 'file-not-indexed', filePath: FILE });
    });

    it('reports a silent server as unavailable, never as an empty file', async () => {
        const { provider } = providerWith([
            { tool: 'list_projects', networkError: 'fetch failed: kein Server auf 127.0.0.1:4299' },
        ]);
        const result = await provider.resolveSymbolAt(ROOT, FILE, 47);
        expect(result.kind).toBe('engine-unavailable');
        if (result.kind === 'engine-unavailable') {
            expect(result.reason).toContain('127.0.0.1:4299');
        }
    });

    it('accepts a pinned project name and then asks nothing about the project list', async () => {
        const { provider, rpc } = providerWith(enclosingRoutes);
        const result = await provider.resolveSymbolAt(ROOT, FILE, 47, { projectName: RECORDED_PROJECT });
        expect(result.kind).toBe('ok');
        expect(rpc.toolsCalled()).not.toContain('list_projects');
    });

    it('matches the project on its root path, trailing separator or not', async () => {
        const { provider, rpc } = providerWith([
            listProjectsRoute(`${ROOT}/`),
            ...enclosingRoutes,
        ]);
        await provider.resolveSymbolAt(`${ROOT}/`, FILE, 47);
        expect(rpc.toolsCalled()[0]).toBe('list_projects');

        // Der Name ist gemerkt, also fragt ein zweiter Aufruf nicht noch einmal.
        await provider.resolveSymbolAt(ROOT, FILE, 47);
        expect(rpc.toolsCalled().filter((tool) => tool === 'list_projects')).toHaveLength(1);
    });
});
