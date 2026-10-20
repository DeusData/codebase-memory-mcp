/**
 * Where the project walk gets its input, and nowhere else.
 *
 * The generator in tour-generator.ts is pure and knows nothing about a provider.
 * In the reference project the two reads that feed it sit in a Theia backend
 * (`intelligence-server-impl.ts`, `generateTour`); here they sit in the browser,
 * because that is where the provider is. The shape of the call is the same one:
 * the whole-project summary supplies the files, the layers, the groups and the
 * exported symbols, the dependency sweep supplies the edges the order is built
 * from, and the generator is handed plain data.
 *
 * Two differences from the reference are worth naming rather than discovering.
 *
 * **Nothing is written.** The reference writes the document into
 * `.codeatlas/tours/` and hands back a DTO with the path. This surface has no
 * workspace to write into, so the document lives for as long as the walk does. A
 * regenerated walk is byte-identical to the one before it, which is what made
 * the artefact worth committing there and is what makes it worth trusting here.
 *
 * **No engine version is recorded.** The reference asks the engine binary for
 * one. This backend's /rpc surface does not offer a version, so the field stays
 * absent rather than being filled with the frontend's own build number, which
 * would answer a question about the analysis with a fact about the UI. The
 * provider makes the same refusal in `engineInfo`.
 *
 * **A project whose index holds no files is refused.** An empty walk is
 * indistinguishable from a walk of a project whose analysis failed, and offering
 * one would tell a reader their repository has nothing in it.
 */

import type { ArchitectureOverviewDto, ProviderQueryOptions } from '../core/intelligence-provider';
import type { ModuleDependencyGraph } from '../core/intelligence-provider';
import { generateHeuristicTour } from './tour-generator';
import type { ActiveTour } from './entry-walk';

/** The sentence a project with nothing indexed gets, instead of an empty walk. */
export const NO_INDEX_FOR_TOUR =
    'the index holds no file for this project, so there is no reading order to walk';

/** What the project walk needs from a provider, and nothing else. */
export interface TourSource {
    architectureOverview(root: string, opts?: ProviderQueryOptions): Promise<ArchitectureOverviewDto>;
    moduleDependencies(root: string, opts?: ProviderQueryOptions): Promise<ModuleDependencyGraph>;
}

/**
 * The project walk for one workspace.
 *
 * Both reads are made before anything is generated, so a walk is either derived
 * from one consistent pair of answers or not offered at all.
 */
export async function generateProjectTour(
    source: TourSource,
    root: string,
    opts: ProviderQueryOptions = {},
): Promise<ActiveTour> {
    const overview = await source.architectureOverview(root, opts);
    if (overview.files.length === 0) {
        throw new Error(NO_INDEX_FOR_TOUR);
    }
    const graph = await source.moduleDependencies(root, opts);
    const document = generateHeuristicTour({
        files: overview.files,
        imports: graph.edges,
        importsTruncated: graph.truncated,
        layers: overview.layers,
        groups: overview.groups,
        entryPoints: overview.entryPoints,
    });
    return {
        kind: 'project',
        document,
        endNote: graph.truncated
            ? 'the dependency read stopped at its bound, so this order was derived from part of the import graph'
            : '',
    };
}
