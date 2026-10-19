/**
 * Woher die Projekt-Fuehrung ihre Eingaben nimmt.
 *
 * Der Generator ist rein und anderswo geprueft; hier geht es um die Uebergabe:
 * dass beide Antworten gelesen werden, dass die Deckel-Meldung des einen Lesers
 * nicht verlorengeht, dass ein leerer Index abgelehnt statt zu einer leeren
 * Fuehrung verarbeitet wird, und dass keine Engine-Version erfunden wird.
 */

import { describe, expect, it, vi } from 'vitest';

import type { ArchitectureOverviewDto, ModuleDependencyGraph } from '../core/intelligence-provider';
import { NO_INDEX_FOR_TOUR, generateProjectTour } from './tour-source';
import type { TourSource } from './tour-source';

function overviewOf(files: string[]): ArchitectureOverviewDto {
    return {
        projectName: 'p',
        totalSymbols: 0,
        totalRelations: 0,
        symbolKinds: [],
        relationKinds: [],
        languages: [],
        groups: [{ name: 'config', symbolCount: 1, fanIn: 0, fanOut: 0 }],
        entryPoints: [
            { name: 'loadConfig', qualifiedName: 'p.src.config.loadConfig', kind: 'function', filePath: 'src/config.ts', line: 11 },
        ],
        routes: [],
        clusters: [],
        layers: [{ group: 'config', layer: 'leaf', reason: 'only inbound calls, no outbound' }],
        boundaries: [],
        hotspots: [],
        files,
    };
}

function sourceOf(files: string[], graph: ModuleDependencyGraph): TourSource & {
    overviewCalls: number;
    dependencyCalls: number;
} {
    const state = { overviewCalls: 0, dependencyCalls: 0 };
    return {
        get overviewCalls() {
            return state.overviewCalls;
        },
        get dependencyCalls() {
            return state.dependencyCalls;
        },
        async architectureOverview() {
            state.overviewCalls += 1;
            return overviewOf(files);
        },
        async moduleDependencies() {
            state.dependencyCalls += 1;
            return graph;
        },
    };
}

const FILES = ['src/config.ts', 'src/server.ts'];
const GRAPH: ModuleDependencyGraph = {
    edges: [{ from: 'src/server.ts', to: 'src/config.ts' }],
    truncated: false,
};

describe('the project walk', () => {
    it('reads the summary and the dependency sweep, once each', async () => {
        const source = sourceOf(FILES, GRAPH);
        await generateProjectTour(source, '/workspace', { projectName: 'p' });
        expect(source.overviewCalls).toBe(1);
        expect(source.dependencyCalls).toBe(1);
    });

    it('hands the reads to the generator and comes back with a walk', async () => {
        const { kind, document } = await generateProjectTour(sourceOf(FILES, GRAPH), '/workspace');
        expect(kind).toBe('project');
        expect(document.steps.map((step) => step.primary.filePath)).toEqual([
            'src/config.ts',
            'src/server.ts',
        ]);
        expect(document.generated.strategy).toBe('topsort');
    });

    it('passes the query options through to both reads', async () => {
        const architectureOverview = vi.fn(async () => overviewOf(FILES));
        const moduleDependencies = vi.fn(async () => GRAPH);
        await generateProjectTour({ architectureOverview, moduleDependencies }, '/workspace', {
            projectName: 'p',
            generation: 4,
        });
        expect(architectureOverview).toHaveBeenCalledWith('/workspace', { projectName: 'p', generation: 4 });
        expect(moduleDependencies).toHaveBeenCalledWith('/workspace', { projectName: 'p', generation: 4 });
    });

    it('records no engine version, because this surface is not told one', async () => {
        const { document } = await generateProjectTour(sourceOf(FILES, GRAPH), '/workspace');
        expect(document.generated.engineVersion).toBeUndefined();
    });

    it('carries a truncated dependency read into the walk and into its end note', async () => {
        const { document, endNote } = await generateProjectTour(
            sourceOf(FILES, { ...GRAPH, truncated: true }),
            '/workspace',
        );
        expect(document.generated.truncated).toBe(true);
        expect(endNote).toContain('stopped at its bound');
    });

    it('says nothing at the end of a walk derived from the whole graph', async () => {
        expect((await generateProjectTour(sourceOf(FILES, GRAPH), '/workspace')).endNote).toBe('');
    });

    it('refuses a project the index holds no file for, rather than walking nothing', async () => {
        const source = sourceOf([], GRAPH);
        await expect(generateProjectTour(source, '/workspace')).rejects.toThrow(NO_INDEX_FOR_TOUR);
        expect(source.dependencyCalls).toBe(0);
    });

    it('is byte-identical on two runs against one index', async () => {
        const first = await generateProjectTour(sourceOf(FILES, GRAPH), '/workspace');
        const second = await generateProjectTour(sourceOf(FILES, GRAPH), '/workspace');
        expect(JSON.stringify(second.document)).toBe(JSON.stringify(first.document));
    });
});
