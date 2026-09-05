/*
 * Portiert am 2026-08-28 aus CodeAtlasIDE,
 * theia-extensions/codeatlas-intelligence/test/semantic-ir-builder.test.ts.
 * Dieselben Eigenschaften, dieselbe Fake-Faktenquelle. Der Vergleich des
 * Snippet-Hashes laeuft weiter gegen node:crypto, was hier mehr prueft als
 * dort: er belegt, dass die eigene sha256-Rechnung und WebCrypto dasselbe
 * liefern wie die Bibliothek, gegen die das Referenzprojekt gebaut hat.
 */

import { createHash } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import type { FactKind, IrFactSource, SymbolFacts } from '../core/intelligence-provider';
import type { SymbolRef } from '../core/focus-protocol';

import {
    DERIVED_SUMMARY_STRATEGY,
    RISK_UNTESTED_HUB,
    buildIr,
    countOf,
    orderSteps,
    purposeSentence,
    toFileUri,
    unmeasuredComplexity,
} from './semantic-ir-builder';

const ROOT = '/workspace/atlas-sample';
const PROJECT = 'codeatlas-atlas-sample-deadbeef';

const CREATE_USER: SymbolRef = {
    name: 'createUser',
    qualifiedName: `${PROJECT}.src.services.userService.createUser`,
    kind: 'function',
    uri: `file://${ROOT}/src/services/userService.ts`,
    range: { start: { line: 22, character: 0 }, end: { line: 35, character: 0 } },
    projectName: PROJECT,
};

const evidence = (relation: string, file: string, line: number) => ({
    source: 'graph-edge' as const,
    relation,
    file,
    range: { startLine: line, endLine: line },
    engineGeneration: 0,
    providerId: 'cbm',
});

/** The facts the recorded engine actually returns for `createUser`. */
function fixtureFacts(kinds: FactKind[]): SymbolFacts {
    const facts: SymbolFacts = {};
    for (const kind of kinds) {
        switch (kind) {
            case 'callees':
                facts.callees = {
                    value: [
                        { targetName: 'validateUser', targetQualifiedName: 'p.src.util.validate.validateUser', targetFile: 'src/util/validate.ts', line: 24, targetLine: 19, strategy: 'direct-call' },
                        { targetName: 'ValidationError', targetQualifiedName: 'p.src.util.validate.ValidationError', targetFile: 'src/util/validate.ts', line: 27, targetLine: 4, strategy: 'construction' },
                        { targetName: 'UserEntity', targetQualifiedName: 'p.src.types.UserEntity', targetFile: 'src/types.ts', line: 29, targetLine: 37, strategy: 'construction' },
                        { targetName: 'listUsers', targetQualifiedName: 'p.src.services.userService.listUsers', targetFile: 'src/services/userService.ts', line: 29, targetLine: 18, strategy: 'direct-call' },
                        { targetName: 'insert', targetQualifiedName: 'p.src.repo.db.insert', targetFile: 'src/repo/db.ts', line: 30, targetLine: 31, strategy: 'direct-call' },
                        { targetName: 'toUser', targetQualifiedName: 'p.src.services.userService.toUser', targetFile: 'src/services/userService.ts', line: 35, targetLine: 9, strategy: 'direct-call' },
                    ],
                    state: 'known',
                    evidence: [
                        evidence('invocation', 'src/util/validate.ts', 24),
                        evidence('invocation', 'src/util/validate.ts', 27),
                        evidence('invocation', 'src/types.ts', 29),
                        evidence('invocation', 'src/services/userService.ts', 29),
                        evidence('invocation', 'src/repo/db.ts', 30),
                        evidence('invocation', 'src/services/userService.ts', 35),
                    ],
                };
                break;
            case 'callers':
                facts.callers = {
                    value: [
                        { name: 'registerUserRoutes', qualifiedName: 'p.src.routes.users.registerUserRoutes', file: 'src/routes/users.ts', line: 15, isTest: false },
                        { name: 'create', qualifiedName: 'p.src.services.userService.create', file: 'src/services/userService.ts', line: 41, isTest: false },
                    ],
                    state: 'known',
                    evidence: [
                        evidence('invocation', 'src/routes/users.ts', 15),
                        evidence('invocation', 'src/services/userService.ts', 41),
                    ],
                };
                break;
            case 'testedBy':
                // Never `known`: the engine records no test relation, so an
                // empty list is the result of a search, not a measurement.
                facts.testedBy = { value: [], state: 'inferred', evidence: [] };
                break;
            case 'throws':
                facts.throws = {
                    value: [{ type: 'ValidationError', file: 'src/util/validate.ts', line: 23 }],
                    state: 'known',
                    evidence: [evidence('raise', 'src/util/validate.ts', 4)],
                };
                break;
            case 'envReads':
                facts.envReads = {
                    value: [{ name: 'DB_URL', kind: 'global', file: CREATE_USER.uri, line: undefined }],
                    state: 'known',
                    evidence: [{ source: 'graph-edge', relation: 'environment-read', file: CREATE_USER.uri, engineGeneration: 0, providerId: 'cbm' }],
                };
                break;
            case 'typeRefs':
                facts.typeRefs = {
                    value: [{ name: 'User', kind: 'unknown', qualifiedName: 'p.src.types.User', file: 'src/types.ts', line: 5 }],
                    state: 'known',
                    evidence: [evidence('type-reference', 'src/types.ts', 5)],
                };
                break;
        }
    }
    return facts;
}

interface FakeOptions {
    /** Families whose request rejects, as the engine timing out would. */
    failing?: FactKind[];
    /** Reject the snippet call instead of answering it. */
    failSnippet?: boolean;
    snippet?: string;
    calls?: { kinds: FactKind[]; generation?: number }[];
}

function fakeProvider(options: FakeOptions = {}): IrFactSource {
    const failing = new Set(options.failing ?? []);
    return {
        id: 'cbm',
        async getFacts(_root, _symbol, kinds, opts) {
            options.calls?.push({ kinds: [...kinds], generation: opts?.generation });
            if (kinds.some((kind) => failing.has(kind))) {
                throw new Error(`engine failed for ${kinds.join(',')}`);
            }
            return fixtureFacts(kinds);
        },
        async getSnippet() {
            if (options.failSnippet) {
                throw new Error('snippet unavailable');
            }
            return options.snippet ?? 'export function createUser() {}';
        },
    } as IrFactSource;
}

describe('the purpose sentence', () => {

    it('is grammatical for the singular of every count', () => {
        const sentence = purposeSentence(CREATE_USER, 1, 1, 1);
        expect(sentence).toBe(
            'Function createUser in userService.ts. Makes 1 call, touches 1 environment value, can raise 1 error type.',
        );
    });

    it('is grammatical for zero and for many, which use the same plural', () => {
        expect(purposeSentence(CREATE_USER, 0, 0, 0)).toBe(
            'Function createUser in userService.ts. Makes 0 calls, touches 0 environment values, can raise 0 error types.',
        );
        expect(purposeSentence(CREATE_USER, 6, 1, 1)).toContain('Makes 6 calls');
    });

    it('names the symbol kind as a word rather than as a code identifier', () => {
        const method = { ...CREATE_USER, kind: 'method' as const };
        expect(purposeSentence(method, 2, 0, 0).startsWith('Method createUser')).toBe(true);
    });

    it('pluralises through one helper, so every count in the sentence agrees', () => {
        expect(countOf(1, 'call', 'calls')).toBe('1 call');
        expect(countOf(0, 'call', 'calls')).toBe('0 calls');
        expect(countOf(2, 'call', 'calls')).toBe('2 calls');
    });
});

describe('step ordering', () => {

    it('follows the reader down the body', () => {
        const ordered = orderSteps([
            { targetName: 'c', line: 30 },
            { targetName: 'a', line: 10 },
            { targetName: 'b', line: 20 },
        ]);
        expect(ordered.map((step) => step.targetName)).toEqual(['a', 'b', 'c']);
    });

    it('puts a call the engine could not place last rather than first', () => {
        const ordered = orderSteps([
            { targetName: 'unplaced' },
            { targetName: 'first', line: 3 },
        ]);
        expect(ordered.map((step) => step.targetName)).toEqual(['first', 'unplaced']);
    });

    it('keeps the engine order when two calls share a line', () => {
        const ordered = orderSteps([
            { targetName: 'left', line: 29 },
            { targetName: 'right', line: 29 },
        ]);
        expect(ordered.map((step) => step.targetName)).toEqual(['left', 'right']);
    });
});

describe('file references', () => {

    it('turns a workspace-relative path into an absolute URI', () => {
        expect(toFileUri(ROOT, 'src/util/validate.ts')).toBe(`file://${ROOT}/src/util/validate.ts`);
    });

    it('leaves an already-formed URI alone', () => {
        expect(toFileUri(ROOT, CREATE_USER.uri)).toBe(CREATE_USER.uri);
    });

    it('reports nothing for an absent or empty path rather than inventing the root', () => {
        expect(toFileUri(ROOT, undefined)).toBeUndefined();
        expect(toFileUri(ROOT, '')).toBeUndefined();
    });

    it('escapes a path segment that would otherwise break the URI', () => {
        expect(toFileUri(ROOT, 'src/my file.ts')).toBe(`file://${ROOT}/src/my%20file.ts`);
    });
});

describe('a clean build', () => {

    it('assembles every family, ordered, with the counts the sentence claims', async () => {
        const { ir, warnings } = await buildIr(fakeProvider(), ROOT, CREATE_USER, { generation: 4 });
        expect(warnings).toEqual([]);
        expect(ir.schemaVersion).toBe(1);
        expect(ir.generation).toBe(4);
        expect(ir.steps.value.map((step) => step.line)).toEqual([24, 27, 29, 29, 30, 35]);
        expect(ir.steps.value[0].targetName).toBe('validateUser');
        expect(ir.purpose.value).toContain('Makes 6 calls');
        expect(ir.purpose.value).toContain('touches 1 environment value');
        expect(ir.purpose.value).toContain('can raise 1 error type');
        expect(ir.purpose.state).toBe('inferred');
        expect(ir.purpose.evidence).toHaveLength(1);
        expect(ir.purpose.evidence[0].strategy).toBe(DERIVED_SUMMARY_STRATEGY);
        expect(ir.purpose.evidence[0].source).toBe('graph-node');
    });

    it('hashes the stored source so an edit can invalidate a cached IR', async () => {
        const source = 'export function createUser() {}';
        const { ir } = await buildIr(fakeProvider({ snippet: source }), ROOT, CREATE_USER);
        expect(ir.snippetHash).toBe(createHash('sha256').update(source).digest('hex'));
    });

    it('hands every file reference to the UI as an absolute URI', async () => {
        const { ir } = await buildIr(fakeProvider(), ROOT, CREATE_USER);
        for (const step of ir.steps.value) {
            expect(step.targetFile?.startsWith('file://')).toBe(true);
        }
        for (const caller of ir.calledBy.value) {
            expect(caller.file?.startsWith('file://')).toBe(true);
        }
        for (const citation of ir.throws.evidence) {
            expect(citation.file?.startsWith('file://')).toBe(true);
        }
    });

    it('keeps the declaration line of a target separate from its call site', async () => {
        const { ir } = await buildIr(fakeProvider(), ROOT, CREATE_USER);
        const validate = ir.steps.value.find((step) => step.targetName === 'validateUser');
        expect(validate?.line).toBe(24);
        expect(validate?.targetLine).toBe(19);
    });

    it('marks what the engine does not record as unsupported, never as empty truth', async () => {
        const { ir } = await buildIr(fakeProvider(), ROOT, CREATE_USER);
        expect(ir.writes.state).toBe('unsupported');
        expect(ir.complexity.state).toBe('unsupported');
        expect(ir.complexity.value).toEqual(unmeasuredComplexity());
        expect(ir.externalEffects.state).toBe('unsupported');
    });

    it('keeps "no test caller found" inferred, so it never reads as "not tested"', async () => {
        const { ir } = await buildIr(fakeProvider(), ROOT, CREATE_USER);
        expect(ir.tests.value).toEqual([]);
        expect(ir.tests.state).toBe('inferred');
        expect(ir.missingTests.value).toBe(true);
        expect(ir.missingTests.state).toBe('inferred');
    });

    it('passes the caller\'s generation to every provider request', async () => {
        const calls: { kinds: FactKind[]; generation?: number }[] = [];
        await buildIr(fakeProvider({ calls }), ROOT, CREATE_USER, { generation: 7 });
        expect(calls.length).toBeGreaterThan(0);
        for (const call of calls) {
            expect(call.generation).toBe(7);
        }
    });

    it('asks for callers and tests together, because one is derived from the other', async () => {
        const calls: { kinds: FactKind[] }[] = [];
        await buildIr(fakeProvider({ calls }), ROOT, CREATE_USER);
        const callerLeg = calls.find((call) => call.kinds.includes('callers'));
        expect(callerLeg?.kinds).toEqual(['callers', 'testedBy']);
    });

    it('issues one leg per independent family, with callers and tests sharing one', async () => {
        const calls: { kinds: FactKind[] }[] = [];
        await buildIr(fakeProvider({ calls }), ROOT, CREATE_USER);
        expect(calls.map((call) => call.kinds)).toEqual([
            ['callees'],
            ['callers', 'testedBy'],
            ['throws'],
            ['envReads'],
            ['typeRefs'],
        ]);
    });
});

describe('degradation', () => {

    it('still returns an IR when one family fails, and marks only that family', async () => {
        const { ir, warnings } = await buildIr(fakeProvider({ failing: ['throws'] }), ROOT, CREATE_USER);
        expect(ir.throws.state).toBe('unknown');
        expect(ir.throws.value).toEqual([]);
        expect(ir.throws.evidence).toEqual([]);
        // Everything else survived.
        expect(ir.steps.state).toBe('known');
        expect(ir.calledBy.state).toBe('known');
        expect(ir.reads.state).toBe('known');
        expect(warnings).toHaveLength(1);
        expect(warnings[0]).toContain('throws is unknown');
    });

    it('reports the callers and tests leg once, because it is one request', async () => {
        const { ir, warnings } = await buildIr(fakeProvider({ failing: ['callers'] }), ROOT, CREATE_USER);
        expect(ir.calledBy.state).toBe('unknown');
        expect(ir.tests.state).toBe('unknown');
        expect(ir.missingTests.state).toBe('unknown');
        expect(warnings).toHaveLength(1);
        expect(warnings[0]).toContain('callers and tests');
    });

    it('counts an unknown family as zero in the purpose sentence without claiming zero', async () => {
        const { ir } = await buildIr(fakeProvider({ failing: ['envReads'] }), ROOT, CREATE_USER);
        expect(ir.purpose.value).toContain('touches 0 environment values');
        expect(ir.reads.state).toBe('unknown');
    });

    it('survives every family failing at once', async () => {
        const failing: FactKind[] = ['callees', 'callers', 'testedBy', 'throws', 'envReads', 'typeRefs'];
        const { ir, warnings } = await buildIr(
            fakeProvider({ failing, failSnippet: true }), ROOT, CREATE_USER,
        );
        expect(ir.symbol).toEqual(CREATE_USER);
        expect(ir.steps.state).toBe('unknown');
        expect(ir.snippetHash).toBeUndefined();
        expect(warnings).toHaveLength(6);
    });

    it('tolerates a missing snippet without losing the rest of the build', async () => {
        const { ir, warnings } = await buildIr(fakeProvider({ failSnippet: true }), ROOT, CREATE_USER);
        expect(ir.snippetHash).toBeUndefined();
        expect(ir.steps.value).toHaveLength(6);
        expect(warnings.some((warning) => warning.includes('snippet'))).toBe(true);
    });

    it('treats an empty snippet as no snippet rather than as the hash of nothing', async () => {
        const { ir, warnings } = await buildIr(fakeProvider({ snippet: '' }), ROOT, CREATE_USER);
        expect(ir.snippetHash).toBeUndefined();
        expect(warnings.some((warning) => warning.includes('snippet'))).toBe(true);
    });
});

describe('risk rules', () => {

    const hubFacts = (callerCount: number, tested: boolean): IrFactSource => ({
        id: 'cbm',
        async getFacts(_root, _symbol, kinds) {
            const facts = fixtureFacts(kinds.filter((kind) => kind !== 'callers' && kind !== 'testedBy'));
            if (kinds.includes('callers')) {
                facts.callers = {
                    value: Array.from({ length: callerCount }, (_unused, index) => ({
                        name: `caller${index}`,
                        qualifiedName: `p.caller${index}`,
                        file: 'src/routes/users.ts',
                        line: index + 1,
                    })),
                    state: 'known',
                    evidence: [],
                };
            }
            if (kinds.includes('testedBy')) {
                facts.testedBy = tested
                    ? { value: [{ name: 'covers it', file: 'test/userService.test.ts', line: 8, kind: 'unit' }], state: 'inferred', evidence: [] }
                    : { value: [], state: 'inferred', evidence: [] };
            }
            return facts;
        },
        async getSnippet() {
            return 'source';
        },
    } as IrFactSource);

    it('flags a symbol many things depend on and nothing tests', async () => {
        const { ir } = await buildIr(hubFacts(3, false), ROOT, CREATE_USER);
        expect(ir.risks).toHaveLength(1);
        expect(ir.risks[0].kind).toBe(RISK_UNTESTED_HUB);
        expect(ir.risks[0].severity).toBe('medium');
        expect(ir.risks[0].message).toContain('3 places');
    });

    it('does not flag a hub that has a test caller', async () => {
        const { ir } = await buildIr(hubFacts(5, true), ROOT, CREATE_USER);
        expect(ir.risks).toEqual([]);
    });

    it('does not flag a symbol with too few callers to be a hub', async () => {
        const { ir } = await buildIr(hubFacts(2, false), ROOT, CREATE_USER);
        expect(ir.risks).toEqual([]);
    });

    it('raises no complexity risk while the analyzer records no complexity', async () => {
        const { ir } = await buildIr(fakeProvider(), ROOT, CREATE_USER);
        expect(ir.risks.every((risk) => risk.kind === RISK_UNTESTED_HUB)).toBe(true);
    });

    it('does not treat construction-heavy code as a risk', async () => {
        // The fixture constructs twice and calls four times; a `new` in a
        // factory is the point of the factory, not a finding.
        const { ir } = await buildIr(fakeProvider(), ROOT, CREATE_USER);
        expect(ir.risks.some((risk) => risk.kind.includes('construction'))).toBe(false);
    });
});

describe('the checklist that comes with the build', () => {

    it('gives every item a stable id that does not move with the workspace', async () => {
        const here = await buildIr(fakeProvider(), ROOT, CREATE_USER);
        const elsewhere = await buildIr(fakeProvider(), '/somewhere/else', {
            ...CREATE_USER,
            uri: 'file:///somewhere/else/src/services/userService.ts',
        });
        expect(here.ir.checklist.length).toBeGreaterThan(0);
        expect(elsewhere.ir.checklist.map((item) => item.id)).toEqual(
            here.ir.checklist.map((item) => item.id),
        );
    });

    it('opens a call at the declaration of its target, not at the call site', async () => {
        const { ir } = await buildIr(fakeProvider(), ROOT, CREATE_USER);
        const item = ir.checklist.find((entry) => entry.label.includes('validateUser'));
        expect(item?.target?.uri).toBe(`file://${ROOT}/src/util/validate.ts`);
        // Graphzeile 19 ist Editorzeile 18.
        expect(item?.target?.range.start.line).toBe(18);
    });
});
