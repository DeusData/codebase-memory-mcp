/**
 * A provider made of a handful of rows, for the compiler's tests.
 *
 * The same trick the closure walk and the meaning search use: the recipe takes a
 * slice of the provider, so a test can satisfy the slice with a literal and
 * prove the fetching rules with no server, no transport and no knowledge of how
 * /rpc talks. The rows below are the atlas-sample fixture as the index reports
 * it, so a reader can check them against fixtures/atlas-sample by eye.
 */

import type { SymbolRef } from '../../core/focus-protocol';
import type {
    ArchitectureOverviewDto,
    FactKind,
    ResolveResult,
    SymbolFacts,
    SymbolSearchHit,
} from '../../core/intelligence-provider';
import type { CallerRef, CallSite, DataRef, Fact, TestRef, ThrowRef } from '../../core/semantic-ir';
import { toEditorRange } from '../../core/positions';
import type { RecipeSource } from '../fact-recipes';

const ROOT = '/workspace';
const PROJECT = 'sample';

function known<T>(value: T): Fact<T> {
    return { value, state: 'known', evidence: [] };
}

function inferred<T>(value: T): Fact<T> {
    return { value, state: 'inferred', evidence: [] };
}

/** One declaration the fake index holds. */
export interface FakeDeclaration {
    name: string;
    filePath: string;
    line: number;
    kind: SymbolRef['kind'];
    isTest?: boolean;
    callees?: CallSite[];
    callers?: CallerRef[];
    throws?: ThrowRef[];
    envReads?: DataRef[];
    tests?: TestRef[];
    snippet?: string;
}

const uriOf = (filePath: string): string => `file://${ROOT}/${filePath}`;
const qualifiedOf = (declaration: FakeDeclaration): string =>
    `${PROJECT}.${declaration.filePath.replace(/\.ts$/, '').split('/').join('.')}.${declaration.name}`;

/** The rows: the shape of the atlas-sample fixture, trimmed to what tests need. */
export const DECLARATIONS: readonly FakeDeclaration[] = [
    {
        name: 'createUser',
        filePath: 'src/services/userService.ts',
        line: 23,
        kind: 'function',
        callees: [
            {
                targetName: 'validateUser',
                targetQualifiedName: 'sample.src.util.validate.validateUser',
                targetFile: 'src/util/validate.ts',
                line: 24,
                targetLine: 19,
                strategy: 'direct-call',
            },
            {
                targetName: 'insert',
                targetQualifiedName: 'sample.src.repo.db.insert',
                targetFile: 'src/repo/db.ts',
                line: 30,
                targetLine: 31,
                strategy: 'direct-call',
            },
            {
                targetName: 'UserEntity',
                targetQualifiedName: 'sample.src.types.UserEntity',
                targetFile: 'src/types.ts',
                line: 29,
                targetLine: 37,
                strategy: 'construction',
            },
        ],
        callers: [
            {
                name: 'registerUserRoutes',
                qualifiedName: 'sample.src.routes.users.registerUserRoutes',
                file: 'src/routes/users.ts',
                line: 15,
                isTest: false,
                sourceKind: 'function',
            },
            {
                name: 'create',
                qualifiedName: 'sample.src.services.userService.create',
                file: 'src/services/userService.ts',
                line: 41,
                isTest: false,
                sourceKind: 'function',
            },
        ],
        throws: [{ type: 'ValidationError', file: 'src/services/userService.ts', line: 27 }],
        envReads: [{ name: 'DB_URL', kind: 'global', file: 'src/services/userService.ts', line: 25 }],
        snippet: 'export function createUser(input: unknown): User {}',
    },
    {
        name: 'validateUser',
        filePath: 'src/util/validate.ts',
        line: 19,
        kind: 'function',
        callers: [
            {
                name: 'createUser',
                qualifiedName: 'sample.src.services.userService.createUser',
                file: 'src/services/userService.ts',
                line: 24,
                isTest: false,
                sourceKind: 'function',
            },
        ],
        throws: [
            { type: 'ValidationError', file: 'src/util/validate.ts', line: 22 },
            { type: 'ValidationError', file: 'src/util/validate.ts', line: 25 },
        ],
        snippet: 'export function validateUser(input: unknown): UserInput {}',
    },
    {
        name: 'registerUserRoutes',
        filePath: 'src/routes/users.ts',
        line: 7,
        kind: 'function',
        callers: [
            {
                name: 'createApp',
                qualifiedName: 'sample.src.server.createApp',
                file: 'src/server.ts',
                line: 34,
                isTest: false,
                sourceKind: 'function',
            },
        ],
        snippet: 'export function registerUserRoutes(router: Router): void {}',
    },
    {
        name: 'create',
        filePath: 'src/services/userService.ts',
        line: 40,
        kind: 'function',
        snippet: 'export function create(input: unknown): User {}',
    },
    {
        name: 'create',
        filePath: 'src/services/orderService.ts',
        line: 30,
        kind: 'function',
        snippet: 'export function create(customerId: string, total: number): Order {}',
    },
    {
        name: 'insert',
        filePath: 'src/repo/db.ts',
        line: 31,
        kind: 'function',
        snippet: 'export function insert(): Row {}',
    },
    {
        name: 'UserEntity',
        filePath: 'src/types.ts',
        line: 37,
        kind: 'class',
        snippet: 'export class UserEntity {}',
    },
];

/** The overview the fake index reports. */
export const OVERVIEW: ArchitectureOverviewDto = {
    projectName: PROJECT,
    totalSymbols: 76,
    totalRelations: 178,
    symbolKinds: [{ kind: 'function', count: 20 }],
    relationKinds: [{ kind: 'CALLS', count: 60 }],
    languages: [{ language: 'TypeScript', fileCount: 11 }],
    groups: [
        { name: 'src.services', symbolCount: 8, fanIn: 2, fanOut: 3 },
        { name: 'src.routes', symbolCount: 4, fanIn: 1, fanOut: 2 },
    ],
    entryPoints: [
        { name: 'main', kind: 'function', filePath: 'src/server.ts', line: 39 },
        { name: 'createApp', kind: 'function', filePath: 'src/server.ts', line: 32 },
    ],
    routes: [
        { method: 'POST', path: '/users', filePath: 'src/routes/users.ts', line: 14, handler: 'createUser', origin: 'source' },
        { method: 'GET', path: '/users', filePath: 'src/routes/users.ts', line: 8, handler: 'listUsers', origin: 'source' },
    ],
    clusters: [],
    layers: [],
    boundaries: [],
    hotspots: [],
    files: ['src/server.ts', 'src/services/userService.ts', 'src/util/validate.ts'],
};

function refOf(declaration: FakeDeclaration): SymbolRef {
    return {
        nodeId: qualifiedOf(declaration),
        name: declaration.name,
        qualifiedName: qualifiedOf(declaration),
        kind: declaration.kind,
        uri: uriOf(declaration.filePath),
        range: toEditorRange(declaration.line, declaration.line + 5),
        selectionRange: toEditorRange(declaration.line, declaration.line),
    };
}

/** How many times each method was called, so a test can prove a bound held. */
export interface FakeCounts {
    getFacts: number;
    resolveSymbolAt: number;
    searchSymbols: number;
    architectureOverview: number;
}

/** A recipe source over {@link DECLARATIONS}, plus a call counter. */
export function fakeSource(): RecipeSource & { counts: FakeCounts } {
    const counts: FakeCounts = {
        getFacts: 0,
        resolveSymbolAt: 0,
        searchSymbols: 0,
        architectureOverview: 0,
    };
    const find = (filePath: string, line: number): FakeDeclaration | undefined =>
        DECLARATIONS.find(
            (entry) => entry.filePath === filePath && line >= entry.line && line <= entry.line + 20,
        );

    return {
        counts,
        id: 'fake',
        async getFacts(_root: string, symbol: SymbolRef, kinds: FactKind[]): Promise<SymbolFacts> {
            counts.getFacts += 1;
            const declaration = DECLARATIONS.find(
                (entry) => qualifiedOf(entry) === symbol.qualifiedName,
            );
            const facts: SymbolFacts = {};
            for (const kind of kinds) {
                if (kind === 'callees') {
                    facts.callees = known(declaration?.callees ?? []);
                } else if (kind === 'callers') {
                    facts.callers = known(declaration?.callers ?? []);
                } else if (kind === 'throws') {
                    facts.throws = known(declaration?.throws ?? []);
                } else if (kind === 'envReads') {
                    facts.envReads = known(declaration?.envReads ?? []);
                } else if (kind === 'typeRefs') {
                    facts.typeRefs = known([]);
                } else if (kind === 'testedBy') {
                    facts.testedBy = inferred(declaration?.tests ?? []);
                }
            }
            return facts;
        },
        async getSnippet(_root: string, qualifiedName: string): Promise<string> {
            const declaration = DECLARATIONS.find((entry) => qualifiedOf(entry) === qualifiedName);
            return declaration?.snippet ?? '';
        },
        async searchSymbols(_root: string, pattern: string): Promise<SymbolSearchHit[]> {
            counts.searchSymbols += 1;
            return DECLARATIONS.filter((entry) =>
                entry.name.toLowerCase().includes(pattern.toLowerCase()),
            ).map((entry) => ({
                name: entry.name,
                qualifiedName: qualifiedOf(entry),
                kind: entry.kind,
                filePath: entry.filePath,
                line: entry.line,
                isTest: entry.isTest ?? false,
            }));
        },
        async declarationLineOf(_root: string, filePath: string, name: string): Promise<number | undefined> {
            return DECLARATIONS.find(
                (entry) => entry.filePath === filePath && entry.name === name,
            )?.line;
        },
        async resolveSymbolAt(_root: string, filePath: string, line: number): Promise<ResolveResult> {
            counts.resolveSymbolAt += 1;
            const declaration = find(filePath, line);
            return declaration === undefined
                ? { kind: 'no-symbol-at-line', filePath }
                : { kind: 'ok', symbol: refOf(declaration), enclosing: [] };
        },
        async architectureOverview(): Promise<ArchitectureOverviewDto> {
            counts.architectureOverview += 1;
            return OVERVIEW;
        },
    };
}

/** The workspace root the fake reports files under. */
export const FAKE_ROOT = ROOT;
