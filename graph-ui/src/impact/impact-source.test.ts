import { describe, expect, it, vi } from 'vitest';
import { IMPACT_TEST_LOOKUP_CAP, readImpact, testCandidates } from './impact-source';
import type { ImpactSource } from './impact-source';
import type {
    ArchitectureOverviewDto,
    ChangeImpactDto,
    ChangeImpactSymbol,
    SymbolFacts,
} from '../core/intelligence-provider';

const USERS_SOURCE = `
export function registerUserRoutes(router: Router): void {
    router.get('/users', (req, res) => res.json(listUsers()));
    router.post('/users', (req, res) => res.json(createUser(req.body)));
}
`;

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
    changedFiles: ['src/services/userService.ts'],
    impacted: [],
    walkedDistance: 1,
    symbols: [
        symbol({ name: 'src/services/userService.ts', qualifiedName: undefined, changeKind: 'module', kind: 'module' }),
        symbol({}),
        symbol({
            name: 'registerUserRoutes',
            qualifiedName: 'p.src.routes.users.registerUserRoutes',
            filePath: 'src/routes/users.ts',
            line: 3,
            changeKind: 'caller',
            distance: 1,
        }),
    ],
};

const OVERVIEW: ArchitectureOverviewDto = {
    totalSymbols: 76,
    totalRelations: 178,
    symbolKinds: [],
    relationKinds: [],
    languages: [],
    groups: [],
    entryPoints: [],
    // Genau der Befund am gebauten Server: die Zusammenfassung nennt fuer
    // TypeScript ueberhaupt keine Route.
    routes: [],
    clusters: [],
    layers: [],
    boundaries: [],
    hotspots: [],
    files: ['src/routes/users.ts', 'src/services/userService.ts', 'README.md'],
};

function fakeSource(over: Partial<ImpactSource> = {}): ImpactSource {
    return {
        async changeImpact() {
            return CHANGE;
        },
        async architectureOverview() {
            return OVERVIEW;
        },
        async getComplexity() {
            return [
                {
                    name: 'createUser',
                    qualifiedName: 'p.src.services.userService.createUser',
                    complexity: 4,
                    cognitive: 2,
                    loopDepth: 0,
                },
            ];
        },
        async getFacts() {
            return { testedBy: { value: [], state: 'inferred', evidence: [] } } as SymbolFacts;
        },
        ...over,
    };
}

const readSource = async (filePath: string) =>
    filePath === 'src/routes/users.ts' ? { source: USERS_SOURCE, truncated: false } : { source: '', truncated: false };

describe('testCandidates', () => {

    it('leaves out the changed files themselves and the tests', () => {
        const candidates = testCandidates(
            [...CHANGE.symbols, symbol({ name: 'spec', qualifiedName: 'p.test', isTest: true })],
            10,
        );
        expect(candidates.map((entry) => entry.name)).toEqual(['createUser', 'registerUserRoutes']);
    });

    it('stops at the cap', () => {
        expect(testCandidates(CHANGE.symbols, 1)).toHaveLength(1);
    });
});

describe('readImpact', () => {

    it('recovers the endpoints the summary does not carry, off the source text', async () => {
        const reading = await readImpact(fakeSource(), '/workspace', {
            projectName: 'p',
            readSource,
        });
        expect(reading.model.endpoints.map((endpoint) => `${endpoint.method} ${endpoint.routePath}`))
            .toEqual(['GET /users', 'POST /users']);
        expect(reading.model.summaryTiles.endpoints).toBe(2);
        // Und der Satz sagt, wie viele Dateien dafuer geoeffnet wurden.
        expect(reading.routeNote).toContain('2 indexed source files');
    });

    it('asks for the complexity of the affected symbols and of nothing else', async () => {
        const getComplexity = vi.fn<ImpactSource['getComplexity']>(async () => []);
        await readImpact(fakeSource({ getComplexity }), '/workspace', { projectName: 'p', readSource });
        expect(getComplexity).toHaveBeenCalledTimes(1);
        expect(getComplexity.mock.calls[0][1]).toEqual([
            'p.src.services.userService.createUser',
            'p.src.routes.users.registerUserRoutes',
        ]);
    });

    it('counts a symbol as checked only when somebody answered about it', async () => {
        const reading = await readImpact(
            fakeSource({
                async getFacts(_root, ref) {
                    return ref.name === 'createUser'
                        ? { testedBy: { value: [], state: 'inferred', evidence: [] } }
                        : { testedBy: { value: [], state: 'unknown', evidence: [] } };
                },
            }),
            '/workspace',
            { projectName: 'p', readSource },
        );
        expect(reading.model.tests.checked).toBe(1);
        expect(reading.model.tests.missing.map((entry) => entry.name)).toEqual(['createUser']);
    });

    it('says the test lookup was capped rather than applying the cap in silence', async () => {
        const reading = await readImpact(fakeSource(), '/workspace', {
            projectName: 'p',
            readSource,
            testCap: 1,
        });
        expect(reading.model.tests.cappedAt).toBe(1);
        expect(reading.model.narrative.text).toContain('Test callers were looked up for the first 1');
    });

    it('keeps the page when the complexity read is refused', async () => {
        const reading = await readImpact(
            fakeSource({
                async getComplexity() {
                    throw new Error('refused');
                },
            }),
            '/workspace',
            { projectName: 'p', readSource },
        );
        expect(reading.model.direct).toHaveLength(1);
        expect(reading.model.completeness.unmeasured).toBe(2);
    });

    it('passes the comparison point through untouched', async () => {
        const changeImpact = vi.fn<ImpactSource['changeImpact']>(async () => CHANGE);
        await readImpact(fakeSource({ changeImpact }), '/workspace', {
            projectName: 'p',
            readSource,
            sinceRef: 'v1.0.0',
        });
        expect(changeImpact.mock.calls[0][1]).toBe('v1.0.0');
    });

    it('has a cap on the test lookup so nobody has to remember one', () => {
        expect(IMPACT_TEST_LOOKUP_CAP).toBeGreaterThan(0);
    });
});
