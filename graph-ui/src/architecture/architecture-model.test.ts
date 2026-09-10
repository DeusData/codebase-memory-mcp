import { describe, expect, it } from 'vitest';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';
import {
    boundaryMap, DEFAULT_ARCHITECTURE_CONFIG, matchingArchitecture,
    readArchitectureConfig, saveArchitectureConfig,
} from './architecture-model';

export function architectureFixture(): ArchitectureOverviewDto {
    return {
        projectName: 'sample', totalSymbols: 41, totalRelations: 62,
        symbolKinds: [{ kind: 'Function', count: 37 }], relationKinds: [{ kind: 'CALLS', count: 62 }],
        languages: [{ language: 'TypeScript', fileCount: 8 }],
        groups: [
            { name: 'api', symbolCount: 14, fanIn: 1, fanOut: 2 },
            { name: 'storage', symbolCount: 27, fanIn: 2, fanOut: 0 },
        ],
        boundaries: [{ from: 'api', to: 'storage', callCount: 12 }],
        layers: [{ group: 'api', layer: 'entry', reason: 'Contains registered routes' }],
        clusters: [{ id: '0', label: 'storage', memberCount: 10, cohesion: 0.8, topMembers: ['persist'] }],
        entryPoints: [{ name: 'start', qualifiedName: 'api.start', kind: 'function', filePath: 'src/api.ts', line: 9 }],
        routes: [{ method: 'POST', path: '/users', handler: 'create', origin: 'source', filePath: 'src/api.ts', line: 12 }],
        hotspots: [{ name: 'persist', filePath: 'src/storage.ts', line: 22, fanIn: 12, complexity: 8 }],
        files: ['src/api.ts', 'src/storage.ts'],
    };
}

function memoryStorage() {
    const contents = new Map<string, string>();
    return { getItem: (key: string) => contents.get(key) ?? null, setItem: (key: string, value: string) => { contents.set(key, value); } };
}

describe('architecture workspace configuration', () => {
    it('restores the chosen view and filter for each project independently', () => {
        const store = memoryStorage();
        const config = { view: 'routes' as const, filter: '/users' };
        expect(saveArchitectureConfig(store, 'sample', config)).toBe(true);
        expect(readArchitectureConfig(store, 'sample')).toEqual(config);
        expect(readArchitectureConfig(store, 'other')).toEqual(DEFAULT_ARCHITECTURE_CONFIG);
    });

    it('ignores malformed or unknown saved versions and never breaks exploration when storage is blocked', () => {
        for (const raw of ['{', 'null', '{"view":"admin","filter":true}', '{"version":2,"view":"routes","filter":"x"}']) {
            expect(readArchitectureConfig({ getItem: () => raw, setItem() {} }, 'sample')).toEqual(DEFAULT_ARCHITECTURE_CONFIG);
        }
        const blocked = { getItem(): never { throw new Error('blocked'); }, setItem(): never { throw new Error('blocked'); } };
        expect(readArchitectureConfig(blocked, 'sample')).toEqual(DEFAULT_ARCHITECTURE_CONFIG);
        expect(saveArchitectureConfig(blocked, 'sample', DEFAULT_ARCHITECTURE_CONFIG)).toBe(false);
    });
});

describe('architecture filtering', () => {
    it('matches source paths and route handlers without changing project totals or evidence origin', () => {
        const overview = architectureFixture();
        const filtered = matchingArchitecture(overview, 'API');
        expect(filtered.groups.map(group => group.name)).toEqual(['api']);
        expect(filtered.hotspots).toEqual([]);
        expect(filtered.routes[0].origin).toBe('source');
        expect(filtered.totalSymbols).toBe(41);
        expect(overview.groups).toHaveLength(2);
        expect(matchingArchitecture(overview, 'create').routes).toHaveLength(1);
    });

    it('finds either end of a boundary and members of a cluster', () => {
        const overview = architectureFixture();
        expect(matchingArchitecture(overview, 'storage').boundaries).toHaveLength(1);
        expect(matchingArchitecture(overview, 'persist').clusters).toHaveLength(1);
        expect(matchingArchitecture(overview, 'unrecorded').boundaries).toEqual([]);
    });
});

describe('bounded boundary map', () => {
    it('retains direction and counts and makes omitted groups explicit', () => {
        const boundaries = [
            { from: 'api', to: 'storage', callCount: 12 },
            { from: 'cli', to: 'api', callCount: 2 },
            { from: 'logger', to: 'storage', callCount: 1 },
        ];
        const map = boundaryMap(boundaries, 2);
        expect(map.groups).toEqual(['api', 'storage']);
        expect(map.boundaries).toEqual([boundaries[0]]);
        expect(map.omittedGroups).toBe(2);
        expect(boundaries).toHaveLength(3);
    });

    it('has a deterministic tie order and never adds an inferred edge', () => {
        const input = [{ from: 'z', to: 'b', callCount: 1 }, { from: 'a', to: 'z', callCount: 1 }];
        expect(boundaryMap(input, 2).groups).toEqual(['z', 'a']);
        expect(boundaryMap(input, 2).boundaries).toEqual([input[1]]);
        expect(boundaryMap([], 8)).toEqual({ groups: [], boundaries: [], omittedGroups: 0 });
    });
});
