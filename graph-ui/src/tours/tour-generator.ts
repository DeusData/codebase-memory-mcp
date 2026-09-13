/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/src/node/tours/tour-generator.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * der Kahn-Lauf ueber die umgekehrte Import-Relation, das Leeren des Ready-Sets
 * in Pfadordnung mit ordinalem Vergleich, der Zyklusbruch samt Protokoll,
 * MAX_TOUR_STEPS mit dem gleichmaessigen Stride, der .codeatlas/-Ausschluss,
 * die Rollenworte und -saetze und primaryOf stehen hier Zeile fuer Zeile so,
 * wie sie dort stehen.
 *
 * Aenderungen gegenueber dem Original: die Importpfade zeigen auf die ebenfalls
 * portierten Dateien (../core/intelligence-provider, ../core/tour-protocol).
 * Sonst keine. Der Generator ist rein und laeuft deshalb im Browser genauso wie
 * im Backend des Referenzprojekts; wer ihn mit Provider-Antworten fuettert,
 * steht in tour-source.ts.
 */
/**
 * The getting-started tour: a workspace put into the order somebody could read
 * it in.
 *
 * The question this answers is the one nobody can answer on their first day in
 * a repository: where do I start, and what has to make sense before the next
 * thing will. The index already holds the answer, in the import relation. A
 * file that imports another cannot be understood before it, so a topological
 * sort of the file-level import graph is a reading order, and it is a reading
 * order that was derived rather than opinionated.
 *
 * ## Five properties carry this file
 *
 * **Deterministic, with nothing to seed.** Two runs over one index produce
 * byte-identical output, and so do two machines and two checkouts of the same
 * repository at different paths. Everything here is a pure function of the
 * paths, the edges and the summary it was handed: the ready set of the Kahn
 * sort is drained in path order rather than in the order the engine returned
 * rows, every list is sorted before it is read, and no clock, no random source
 * and no absolute path is consulted anywhere.
 *
 * **Cycles are broken by a stated rule, not by luck.** Import cycles are
 * ordinary in real code, and a topological sort has no answer for one. When the
 * sort stalls, the edge whose `(from, to)` pair is lexicographically smallest
 * among the ones still holding files back is dropped, and the sort continues.
 * The rule is arbitrary, which is exactly why it is written down and why every
 * dropped edge is recorded in the document's provenance.
 *
 * **A role is evidence, never a guess about behaviour.** A step says "its path
 * names it as configuration" or "the index places its group in the entry
 * layer", and stops. It never says what a file does. The generator has read no
 * source text and is in no position to.
 *
 * **A step points at a symbol only when the index named one.** The exported
 * symbols the summary flagged are the candidates; the earliest declared one in
 * a file is the representative. A file the index holds no exported symbol in
 * gets a file step, which opens the file at its first line, because pointing a
 * reader at an arbitrary line would be inventing a decision.
 *
 * **The tour is bounded.** A repository of two thousand files does not have a
 * two thousand step tour. Past {@link MAX_TOUR_STEPS} the walk is sampled at an
 * even stride over the dependency order, which keeps the first file, the last
 * file and the shape of the order in between.
 *
 * ## Provenance
 *
 * The idea of deriving a heuristic onboarding tour from a project's own
 * structure, rather than asking a model to write one, is ported in concept from
 * Understand-Anything (MIT). No source code is shared; see THIRD_PARTY.md.
 */

import type {
    ArchitectureGroup,
    ArchitectureLayerAssignment,
    ModuleDependency,
    SymbolSearchHit
} from '../core/intelligence-provider';
import {
    GETTING_STARTED_TOUR_ID,
    TOUR_SCHEMA_VERSION
} from '../core/tour-protocol';
import type {
    TourDocument,
    TourStepPrimaryRecord,
    TourStepRecord
} from '../core/tour-protocol';

/**
 * How many steps a generated tour may hold.
 *
 * Twelve is about an hour of honest reading, which is as much as a first
 * sitting in an unfamiliar repository is worth. The cap is on the tour and never
 * on the sort: the whole graph is ordered first and the walk is sampled from it,
 * so a capped tour still starts at the bottom of the dependency order and ends
 * at the top.
 */
export const MAX_TOUR_STEPS = 12;

/** Title of the one tour this generator produces. */
export const GETTING_STARTED_TITLE = 'Getting started';

/**
 * The directory CodeAtlas keeps its own files in, which a tour never walks.
 *
 * The obvious reason is that this tour is about the reader's code. The analysis
 * indexes every file it can parse, and the product's own pin, policy and state
 * documents are JSON files sitting in the workspace, so without this the first
 * step of a first tour is a walk through CodeAtlas's bookkeeping.
 *
 * The load-bearing reason is determinism across regenerations. A tour written
 * into `.codeatlas/tours/` is then indexed by the analysis, so a generator that
 * did not exclude its own output would produce a different tour every time it
 * ran: the first generation would add a file that the second one would then have
 * to place. That would quietly destroy the property the whole artefact rests on.
 */
export const DERIVED_STATE_PREFIX = '.codeatlas/';

/** True when a path is CodeAtlas's own bookkeeping rather than the reader's code. */
export function isDerivedState(filePath: string): boolean {
    return filePath.startsWith(DERIVED_STATE_PREFIX);
}

/** Everything the generator reads. Plain data, so the generator stays pure. */
export interface TourGeneratorInput {
    /** Workspace-relative paths the index holds. */
    files: readonly string[];
    /** File-level import edges: `from` imports `to`. Duplicates are expected. */
    imports: readonly ModuleDependency[];
    /** True when the import read stopped at its bound. */
    importsTruncated?: boolean;
    /** Which layer the index placed each group in. */
    layers?: readonly ArchitectureLayerAssignment[];
    /** The groups themselves, for the sentence that quotes a layer's reason. */
    groups?: readonly ArchitectureGroup[];
    /** Exported symbols the summary flagged, the candidates for a step's primary. */
    entryPoints?: readonly SymbolSearchHit[];
    /** Version of the analysis backend that answered, recorded in the document. */
    engineVersion?: string;
    /** Ceiling on steps. Defaults to {@link MAX_TOUR_STEPS}. */
    maxSteps?: number;
    /** Tour identity, so a second kind of tour can be generated later. */
    id?: string;
    title?: string;
}

/** A dependency order, plus what had to be given up to produce one. */
export interface TopsortResult {
    /** Every input file exactly once: what it depends on comes before it. */
    order: string[];
    /** Edges dropped to break a cycle, in the order they were dropped. */
    brokenEdges: ModuleDependency[];
    /** Distinct edges the sort ran over, after deduplication and filtering. */
    edgeCount: number;
}

/**
 * The role a step's wording is built from.
 *
 * Two families, and the difference between them is the difference between two
 * kinds of evidence. The first seven come from the file's own path, which is
 * something the repository's authors wrote down. The last four come from the
 * layer the index placed the file's group in, which is something the analysis
 * derived. `unclassified` is what is left when neither said anything, and it
 * gets a sentence that says so rather than a guess.
 */
export type TourStepRole =
    | 'entry'
    | 'route'
    | 'service'
    | 'data'
    | 'config'
    | 'types'
    | 'util'
    | 'test'
    | 'layer-entry'
    | 'layer-core'
    | 'layer-internal'
    | 'layer-leaf'
    | 'unclassified';

/**
 * Path segments that name a role, deepest segment first.
 *
 * Read as evidence about intent rather than about behaviour: a directory called
 * `routes` is what the authors of the repository called it, and the sentence a
 * step is given says exactly that much and no more. The tables are disjoint, so
 * a segment can match at most one role and the order of the scan below cannot
 * change an answer.
 */
export const ROLE_PATH_WORDS: Readonly<Record<string, TourStepRole>> = Object.freeze({
    test: 'test', tests: 'test', spec: 'test', specs: 'test', __tests__: 'test', e2e: 'test',
    config: 'config', configs: 'config', configuration: 'config', settings: 'config',
    env: 'config', environment: 'config',
    type: 'types', types: 'types', model: 'types', models: 'types', entity: 'types',
    entities: 'types', schema: 'types', schemas: 'types', dto: 'types', dtos: 'types',
    interfaces: 'types',
    route: 'route', routes: 'route', router: 'route', routers: 'route',
    controller: 'route', controllers: 'route', api: 'route', endpoint: 'route',
    endpoints: 'route', handler: 'route', handlers: 'route',
    service: 'service', services: 'service', usecase: 'service', usecases: 'service',
    domain: 'service', business: 'service',
    repo: 'data', repos: 'data', repository: 'data', repositories: 'data', db: 'data',
    database: 'data', store: 'data', stores: 'data', dao: 'data', persistence: 'data',
    util: 'util', utils: 'util', utility: 'util', utilities: 'util', helper: 'util',
    helpers: 'util', lib: 'util', common: 'util', shared: 'util', support: 'util',
    main: 'entry', index: 'entry', server: 'entry', app: 'entry', application: 'entry',
    bootstrap: 'entry', cli: 'entry', bin: 'entry', entry: 'entry', startup: 'entry'
});

/** The word a step's title leads with, one per role. */
export const ROLE_TITLES: Readonly<Record<TourStepRole, string>> = Object.freeze({
    entry: 'Way in',
    route: 'Routes',
    service: 'Service',
    data: 'Storage',
    config: 'Configuration',
    types: 'Types',
    util: 'Helpers',
    test: 'Test',
    'layer-entry': 'Entry layer',
    'layer-core': 'Core layer',
    'layer-internal': 'Internal layer',
    'layer-leaf': 'Leaf layer',
    unclassified: 'File'
});

/**
 * The first sentence of a step, one per role.
 *
 * Every one of them names the evidence it rests on in its own words, because
 * the sentence is the only place a reader meets that evidence: they are looking
 * at a tour, not at a graph. "Its path names it" is a claim about a directory
 * name and is always true when it is said; "the index places its group" is a
 * claim about the analysis and is quoted with the analysis's own reason beside
 * it. Neither is a claim about what the file does, and none of them may become
 * one: this generator has not read a line of the source.
 */
export const ROLE_SENTENCES: Readonly<Record<TourStepRole, string>> = Object.freeze({
    entry: 'Its path names it as a way into the program, so a run starts somewhere in here.',
    route: 'Its path names it as routing, so what it holds is the mapping from something arriving to the code that answers it.',
    service: 'Its path names it as a service, so what it holds sits between the ways in and the pieces they lean on.',
    data: 'Its path names it as storage, so what it holds is where data is read and written.',
    config: 'Its path names it as configuration, so the values it deals in are set outside the code and read from here.',
    types: 'Its path names it as types, so what it holds is shapes other files agree on rather than behaviour.',
    util: 'Its path names it as shared helpers, so what it holds is reached from several places and depends on little.',
    test: 'Its path names it as a test, so what it holds is the shortest written description of what the code it exercises is supposed to do.',
    'layer-entry': 'Its path says nothing about its role. The index places its group in the entry layer.',
    'layer-core': 'Its path says nothing about its role. The index places its group in the core layer.',
    'layer-internal': 'Its path says nothing about its role. The index places its group in the internal layer.',
    'layer-leaf': 'Its path says nothing about its role. The index places its group in the leaf layer.',
    unclassified: 'Neither its path nor the index says what part this file plays, so this step says only where it sits in the dependency order.'
});

/** `n thing` or `n things`, so a generated sentence never reads as machine output. */
function countOf(count: number, singular: string, plural: string): string {
    return `${count} ${count === 1 ? singular : plural}`;
}

/**
 * The sentence that places a file in the graph.
 *
 * Counts only, and both directions, because both are what a reader is deciding
 * with: a file nothing imports is a place to start or a place nobody uses, and
 * only the reader can tell which. The wording never turns a zero into a verdict.
 */
export function dependencySentence(importsOut: number, importsIn: number): string {
    if (importsOut === 0 && importsIn === 0) {
        return 'The index records no import either way for it: nothing here imports it and it imports nothing here.';
    }
    if (importsOut === 0) {
        return `It imports nothing else in this workspace, and ${countOf(importsIn, 'file imports', 'files import')} it.`;
    }
    if (importsIn === 0) {
        return `It imports ${countOf(importsOut, 'file', 'files')} in this workspace, and nothing here imports it.`;
    }
    return `It imports ${countOf(importsOut, 'file', 'files')} in this workspace, and `
        + `${countOf(importsIn, 'file imports', 'files import')} it.`;
}

/** The clause that quotes a layer assignment, with the analysis's own reason. */
export function layerSentence(group: string, layer: string, reason: string | undefined): string {
    const because = reason === undefined || reason.length === 0 ? '' : `: ${reason}`;
    return `The index places the group ${group} in the ${layer} layer${because}.`;
}

/** Path stems of a file, deepest first: the basename without its extension, then its directories. */
export function pathStems(filePath: string): string[] {
    const segments = filePath.split('/').filter(segment => segment.length > 0);
    if (segments.length === 0) {
        return [];
    }
    const last = segments[segments.length - 1];
    const dot = last.lastIndexOf('.');
    const stem = dot > 0 ? last.slice(0, dot) : last;
    return [stem, ...segments.slice(0, -1).reverse()];
}

/**
 * The role of one file, from its path first and from the index second.
 *
 * The path wins because it is the stronger evidence: a directory called
 * `routes` was named by somebody who meant it, where a layer is a reading of
 * the call graph that can put a routing file in the internal layer for perfectly
 * good reasons of its own. A test is recognised from a compound basename as well
 * as from a directory, because `userService.test.ts` is the common spelling and
 * a stem match alone would miss it.
 */
export function roleOf(filePath: string, layer: string | undefined): TourStepRole {
    const stems = pathStems(filePath);
    const basename = stems[0] ?? '';
    if (/(^|\.)(test|spec)($|\.)/i.test(basename)) {
        return 'test';
    }
    for (const stem of stems) {
        const role = ROLE_PATH_WORDS[stem.toLowerCase()];
        if (role !== undefined) {
            return role;
        }
    }
    switch (layer) {
        case 'entry': return 'layer-entry';
        case 'core': return 'layer-core';
        case 'internal': return 'layer-internal';
        case 'leaf': return 'layer-leaf';
        default: return 'unclassified';
    }
}

/**
 * The group whose name matches a path segment, or undefined when none does.
 *
 * The analysis names a group after the module or directory its symbols sit in
 * and does not say which files are in it, so the join back to a file is this
 * match and there is no stronger one available. The deepest matching segment
 * wins, so a file in `src/services/` is in `services` rather than in `src` when
 * the index knows both, and ties are broken on the group name so the answer
 * cannot depend on the order the summary listed groups in.
 */
export function groupOf(filePath: string, groups: readonly string[]): string | undefined {
    const known = new Set(groups.map(group => group.toLowerCase()));
    for (const stem of pathStems(filePath)) {
        if (known.has(stem.toLowerCase())) {
            const matches = groups.filter(group => group.toLowerCase() === stem.toLowerCase()).sort();
            return matches[0];
        }
    }
    return undefined;
}

/**
 * Distinct file-to-file edges between files the index holds, sorted.
 *
 * Three things happen here and each of them is what makes the sort below
 * reproducible. Duplicates go, because the analysis writes one edge per import
 * statement and a file importing three names from one module is one dependency.
 * Self edges go, because a file cannot come before itself. Edges naming a file
 * outside the given set go, because an order over files the caller does not
 * have is not an order it can walk.
 */
export function normalizeEdges(
    edges: readonly ModuleDependency[],
    files: readonly string[]
): ModuleDependency[] {
    const known = new Set(files);
    const seen = new Set<string>();
    const out: ModuleDependency[] = [];
    for (const edge of edges) {
        if (edge.from === edge.to || !known.has(edge.from) || !known.has(edge.to)) {
            continue;
        }
        const key = `${edge.from} ${edge.to}`;
        if (seen.has(key)) {
            continue;
        }
        seen.add(key);
        out.push({ from: edge.from, to: edge.to });
    }
    return out.sort(compareEdges);
}

/** Edge order: by importer, then by imported. The tie-break the cycle rule names. */
export function compareEdges(a: ModuleDependency, b: ModuleDependency): number {
    return a.from === b.from ? compareText(a.to, b.to) : compareText(a.from, b.from);
}

/**
 * Ordinal comparison, never locale comparison.
 *
 * `localeCompare` is what a reader reaches for and it is wrong here: its answer
 * depends on the machine's collation, so two correct runs on two machines could
 * order two files differently and the generated document would churn. Code unit
 * order is the same everywhere.
 */
export function compareText(a: string, b: string): number {
    if (a === b) {
        return 0;
    }
    return a < b ? -1 : 1;
}

/**
 * A dependency order over the files, breaking cycles by a stated rule.
 *
 * Kahn's algorithm, run over the *reverse* of the import relation: a file's
 * in-degree is the number of files it imports, so the sort starts from the
 * files that import nothing and ends at the ones everything leads to. That is
 * the direction a reader wants. The plain direction would produce the reading
 * order of somebody who already knows the codebase.
 *
 * The ready set is drained in path order, which is what makes the result a
 * single defined order rather than one of many valid ones. Path order is used
 * rather than any measure of importance for one reason: a path is a fact about
 * the repository, and a measure would have to be recomputed from an index that
 * moves, so two generations of the same unchanged project could disagree.
 *
 * When the ready set empties with files left over, every remaining file is in or
 * behind a cycle. The edge dropped is the lexicographically smallest `(from,
 * to)` pair among the edges still holding an unplaced file back. It is dropped,
 * recorded, and the sort continues; the loop terminates because there are
 * finitely many edges and each pass either places a file or removes one.
 */
export function topsortFiles(
    files: readonly string[],
    edges: readonly ModuleDependency[]
): TopsortResult {
    const nodes = [...new Set(files)].sort(compareText);
    const normalized = normalizeEdges(edges, nodes);

    // `dependents.get(x)` is every file that imports x: placing x releases them.
    const dependents = new Map<string, string[]>();
    const inDegree = new Map<string, number>();
    for (const node of nodes) {
        dependents.set(node, []);
        inDegree.set(node, 0);
    }
    const live = new Set<string>();
    for (const edge of normalized) {
        dependents.get(edge.to)!.push(edge.from);
        inDegree.set(edge.from, (inDegree.get(edge.from) ?? 0) + 1);
        live.add(`${edge.from} ${edge.to}`);
    }

    const ready: string[] = nodes.filter(node => inDegree.get(node) === 0);
    const order: string[] = [];
    const placed = new Set<string>();
    const brokenEdges: ModuleDependency[] = [];

    const release = (node: string): void => {
        for (const dependent of dependents.get(node) ?? []) {
            const left = (inDegree.get(dependent) ?? 0) - 1;
            inDegree.set(dependent, left);
            if (left === 0 && !placed.has(dependent)) {
                insertSorted(ready, dependent);
            }
        }
    };

    while (order.length < nodes.length) {
        if (ready.length === 0) {
            const broken = breakCycle(normalized, live, placed, inDegree);
            if (broken === undefined) {
                // Unreachable while the bookkeeping above holds: a file that is
                // neither placed nor blocked by a live edge has in-degree zero
                // and is already in the ready set. Bailing out in path order
                // rather than looping is the safe reading of an impossible
                // state.
                for (const node of nodes) {
                    if (!placed.has(node)) {
                        placed.add(node);
                        order.push(node);
                    }
                }
                break;
            }
            brokenEdges.push(broken);
            live.delete(`${broken.from} ${broken.to}`);
            const left = (inDegree.get(broken.from) ?? 0) - 1;
            inDegree.set(broken.from, left);
            if (left === 0 && !placed.has(broken.from)) {
                insertSorted(ready, broken.from);
            }
            continue;
        }
        const next = ready.shift()!;
        placed.add(next);
        order.push(next);
        release(next);
    }

    return { order, brokenEdges, edgeCount: normalized.length };
}

/**
 * The edge to drop when the sort stalls.
 *
 * The smallest live edge whose importer is still unplaced and still blocked.
 * "Still blocked" matters: an edge into a file that is already free is not what
 * is holding anything back, and dropping it would leave the sort exactly as
 * stuck as it was while quietly recording a broken dependency that was not
 * broken.
 */
export function breakCycle(
    edges: readonly ModuleDependency[],
    live: ReadonlySet<string>,
    placed: ReadonlySet<string>,
    inDegree: ReadonlyMap<string, number>
): ModuleDependency | undefined {
    for (const edge of edges) {
        if (!live.has(`${edge.from} ${edge.to}`)) {
            continue;
        }
        if (placed.has(edge.from) || (inDegree.get(edge.from) ?? 0) === 0) {
            continue;
        }
        return edge;
    }
    return undefined;
}

/** Insert into an already sorted array, keeping it sorted. */
function insertSorted(sorted: string[], value: string): void {
    let low = 0;
    let high = sorted.length;
    while (low < high) {
        const middle = (low + high) >> 1;
        if (compareText(sorted[middle], value) < 0) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    sorted.splice(low, 0, value);
}

/**
 * At most `limit` files from an order, keeping its shape.
 *
 * An even stride rather than the first `limit`, and the difference matters: the
 * first twelve files of a dependency order are twelve leaf utilities, which is
 * the least useful tour a repository could be given. A stride keeps the first
 * file, the last file and a spread of everything between them, so a capped tour
 * still walks from what everything rests on up to what a run starts in.
 */
export function sampleOrder(order: readonly string[], limit: number): string[] {
    if (limit <= 0) {
        return [];
    }
    if (order.length <= limit) {
        return [...order];
    }
    if (limit === 1) {
        return [order[0]];
    }
    const picked: string[] = [];
    const seen = new Set<number>();
    for (let step = 0; step < limit; step++) {
        const index = Math.round((step * (order.length - 1)) / (limit - 1));
        if (seen.has(index)) {
            continue;
        }
        seen.add(index);
        picked.push(order[index]);
    }
    return picked;
}

/**
 * A step id, derived from the path and from nothing else.
 *
 * Readable rather than hashed, because the artefact is read by people:
 * `src-services-userservice-ts` says which step a diff is about where a digest
 * would not. Two paths that reduce to one slug are disambiguated with a
 * counter, in the order the walk visits them, so an id is still unique and still
 * a pure function of the tour.
 */
export function stepId(filePath: string, taken: ReadonlySet<string>): string {
    const base = filePath.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || 'step';
    if (!taken.has(base)) {
        return base;
    }
    let suffix = 2;
    while (taken.has(`${base}-${suffix}`)) {
        suffix += 1;
    }
    return `${base}-${suffix}`;
}

/**
 * The symbol a step points at, or undefined for a file step.
 *
 * The candidates are the exported symbols the summary flagged in this file. The
 * earliest declared one wins, with the qualified name as the tie-break, because
 * the top of a file is where its authors put the thing it is for often enough
 * to be a better rule than any ranking this generator could invent from a
 * summary. A candidate with no qualified name or no line is not a candidate: a
 * step that cannot be asked about is a step that opens a file, which is what the
 * file case already is.
 */
export function representativeSymbol(
    filePath: string,
    entryPoints: readonly SymbolSearchHit[]
): SymbolSearchHit | undefined {
    const candidates = entryPoints.filter(hit =>
        hit.filePath === filePath
        && typeof hit.qualifiedName === 'string'
        && hit.qualifiedName.length > 0
        && typeof hit.line === 'number'
        && Number.isFinite(hit.line));
    if (candidates.length === 0) {
        return undefined;
    }
    return [...candidates].sort((a, b) =>
        (a.line! - b.line!) || compareText(a.qualifiedName!, b.qualifiedName!))[0];
}

/** The whole tour, as a document. */
export function generateHeuristicTour(input: TourGeneratorInput): TourDocument {
    const files = [...new Set(input.files.filter(file => file.length > 0 && !isDerivedState(file)))]
        .sort(compareText);
    const sort = topsortFiles(files, input.imports);
    const walk = sampleOrder(sort.order, input.maxSteps ?? MAX_TOUR_STEPS);

    const normalized = normalizeEdges(input.imports, files);
    const outDegree = new Map<string, number>();
    const inCount = new Map<string, number>();
    for (const edge of normalized) {
        outDegree.set(edge.from, (outDegree.get(edge.from) ?? 0) + 1);
        inCount.set(edge.to, (inCount.get(edge.to) ?? 0) + 1);
    }

    const layerByGroup = new Map<string, ArchitectureLayerAssignment>();
    for (const assignment of input.layers ?? []) {
        if (assignment.group.length > 0 && !layerByGroup.has(assignment.group)) {
            layerByGroup.set(assignment.group, assignment);
        }
    }
    const groupNames = [...layerByGroup.keys(), ...(input.groups ?? []).map(group => group.name)];
    const entryPoints = input.entryPoints ?? [];

    const taken = new Set<string>();
    const steps: TourStepRecord[] = walk.map((filePath, order) => {
        const group = groupOf(filePath, groupNames);
        const assignment = group === undefined ? undefined : layerByGroup.get(group);
        const layer = assignment?.layer;
        const role = roleOf(filePath, layer);
        const sentences = [ROLE_SENTENCES[role]];
        if (group !== undefined && layer !== undefined && !role.startsWith('layer-')) {
            sentences.push(layerSentence(group, layer, assignment?.reason));
        } else if (role.startsWith('layer-')) {
            const reason = assignment?.reason;
            if (reason !== undefined && reason.length > 0) {
                sentences.push(`The reason it gives is: ${reason}.`);
            }
        }
        sentences.push(dependencySentence(outDegree.get(filePath) ?? 0, inCount.get(filePath) ?? 0));
        const id = stepId(filePath, taken);
        taken.add(id);
        return {
            id,
            title: `${ROLE_TITLES[role]}: ${filePath}`,
            description: sentences.join(' '),
            order,
            primary: primaryOf(filePath, entryPoints),
            ...(group === undefined ? {} : { group }),
            ...(layer === undefined ? {} : { layer })
        };
    });

    return {
        schemaVersion: TOUR_SCHEMA_VERSION,
        id: input.id ?? GETTING_STARTED_TOUR_ID,
        title: input.title ?? GETTING_STARTED_TITLE,
        generated: {
            strategy: 'topsort',
            ...(input.engineVersion === undefined ? {} : { engineVersion: input.engineVersion }),
            ...(input.importsTruncated === true ? { truncated: true } : {}),
            edgeCount: sort.edgeCount,
            brokenEdges: sort.brokenEdges.map(edge => `${edge.from} -> ${edge.to}`)
        },
        steps
    };
}

/** A step's target: the representative symbol when the index named one, the file otherwise. */
function primaryOf(filePath: string, entryPoints: readonly SymbolSearchHit[]): TourStepPrimaryRecord {
    const hit = representativeSymbol(filePath, entryPoints);
    if (hit === undefined) {
        return { kind: 'file', filePath };
    }
    return {
        kind: 'symbol',
        filePath,
        line: hit.line!,
        name: hit.name,
        qualifiedName: hit.qualifiedName!,
        symbolKind: hit.kind
    };
}
