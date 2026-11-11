/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-core/src/common/intelligence-provider.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * das ist die Grenze gegen den Lock-in, und eine Web-Fassung, die sie leicht
 * anders schneidet, waere genau das Gegenteil davon. Aenderungen gegenueber
 * dem Original: die Importpfade zeigen auf die ebenfalls portierten Dateien im
 * selben Verzeichnis, und IrFactSource ist hier mit deklariert statt im
 * IR-Builder (Begruendung unten am Typ).
 */
/**
 * The intelligence provider contract: everything CodeAtlas asks an analysis
 * backend for, expressed in product vocabulary only.
 *
 * This file is the anti lock-in boundary. Views, teaching and product code
 * depend on the types declared here and never on a particular analysis engine.
 * No graph query language, no tool names, no relation identifiers and no
 * binary names appear below; a second provider (a language server, a bespoke
 * indexer, a hosted service) must be able to satisfy this interface without
 * the rest of the product noticing the swap.
 *
 * Two rules keep the boundary honest:
 *
 *  1. Every claim that could be wrong is returned as `Fact<T>`, so callers see
 *     the difference between "no results" and "nobody looked".
 *  2. Every optional ability is declared up front in `ProviderCapabilities`,
 *     so the UI can hide what a provider cannot do instead of rendering an
 *     empty panel that reads like an absence of findings.
 */

import {
    CallerRef,
    CallSite,
    DataRef,
    Fact,
    TestRef,
    ThrowRef
} from './semantic-ir';
import { CodeAtlasSymbolKind, SymbolRef } from './focus-protocol';

/**
 * What a provider can answer. Read this before rendering a panel: a `false`
 * means "this provider cannot know", which is a different message to the user
 * than an empty result.
 */
export interface ProviderCapabilities {
    /** Incoming invocations of a symbol can be listed. */
    callers: boolean;
    /** Outgoing invocations of a symbol can be listed. */
    callees: boolean;
    /** Error types a symbol can raise can be listed. */
    throws: boolean;
    /** Environment values a symbol reads can be listed. */
    envReads: boolean;
    /** Type references made by a symbol can be listed. */
    typeRefs: boolean;
    /**
     * How test coverage of a symbol is established.
     * `edges` means the provider records a first class test relation;
     * `heuristic` means it is derived from other signals and is therefore
     * always `inferred`; `none` means the provider cannot say.
     */
    tests: 'edges' | 'heuristic' | 'none';
    /** HTTP routes and their handlers can be recovered. */
    routes: boolean;
    /** A whole-project structural overview can be produced. */
    architecture: boolean;
    /** Symbols impacted by a set of edits can be listed. */
    changeImpact: boolean;
    /** Observed runtime traces can be ingested and replayed. */
    runtimeTraces: boolean;
    /** Symbols can be found by meaning rather than by exact name. */
    semanticSearch: boolean;
    /**
     * `per-site` means each individual invocation is reported separately.
     * `per-target` means invocations are collapsed per target symbol, so a
     * reported line is one representative site and not the only one.
     */
    callSiteGranularity: 'per-site' | 'per-target';
}

/**
 * Outcome of turning a caret position into a symbol. Every failure mode is a
 * distinct variant on purpose: "nothing here" and "not indexed yet" and
 * "no backend" need three different messages in the UI.
 */
export type ResolveResult =
    | { kind: 'ok'; symbol: SymbolRef; enclosing: SymbolRef[] }
    | { kind: 'no-symbol-at-line'; filePath: string }
    | { kind: 'file-not-indexed'; filePath: string }
    | { kind: 'engine-unavailable'; reason: string }
    | { kind: 'ambiguous'; candidates: SymbolRef[] };

/** The fact families a caller can request for one symbol. */
export type FactKind =
    | 'callees'
    | 'callers'
    | 'throws'
    | 'envReads'
    | 'typeRefs'
    | 'testedBy';

/**
 * Facts about one symbol. A missing key means the caller did not ask for that
 * family; a present key with an empty value and a `known` state means the
 * provider looked and found nothing.
 */
export interface SymbolFacts {
    callees?: Fact<CallSite[]>;
    callers?: Fact<CallerRef[]>;
    throws?: Fact<ThrowRef[]>;
    envReads?: Fact<DataRef[]>;
    typeRefs?: Fact<DataRef[]>;
    testedBy?: Fact<TestRef[]>;
}

/** Which way a path walk runs from the root symbol. */
export type TraceDirection = 'callers' | 'callees' | 'both';

/**
 * One symbol reached while walking paths away from the root.
 *
 * The three optional fields below are an enrichment, not part of the walk. A
 * path walk answers "what is reachable"; whether a reached symbol is test code,
 * and whether the root gets to it by constructing a class rather than by
 * calling a function, are separate readings that cost a query each. Providers
 * are free to answer them for as much of the walk as they can afford, and
 * `enriched` is what says how far that reached: a node with `enriched` false or
 * absent is a node nobody asked those questions about, which is a different
 * thing from a node that was asked and answered no. A surface that drew a
 * missing badge as an absence would be inventing a finding.
 */
export interface TraceNode {
    symbol: SymbolRef;
    /** Distance from the root in invocation steps, 1 for a direct neighbour. */
    hop: number;
    /** Whether this node was reached by walking inbound or outbound. */
    via: 'callers' | 'callees';
    /** True when the index flagged this symbol as test code. Only meaningful when `enriched`. */
    isTest?: boolean;
    /**
     * True when the step from the previous symbol to this one constructs a
     * class rather than calling a function. Only meaningful when `enriched`.
     */
    construction?: boolean;
    /** True when the two flags above were actually looked up for this node. */
    enriched?: boolean;
}

/**
 * Result of a path walk. Layers are grouped by hop, so `layers[0]` holds the
 * direct neighbours of the root.
 */
export type TraceResult =
    | { status: 'ok'; direction: TraceDirection; root: SymbolRef; layers: TraceNode[][] }
    | { status: 'ambiguous'; candidates: SymbolRef[] };

/** A lightweight symbol description, used where a full `SymbolRef` is overkill. */
export interface SymbolSearchHit {
    name: string;
    qualifiedName?: string;
    kind: CodeAtlasSymbolKind;
    /** Workspace-relative path as reported by the provider. */
    filePath?: string;
    /** 1-based declaration line. */
    line?: number;
    isTest?: boolean;
    isExported?: boolean;
    /** 0 to 1 when the provider ranked the hit, absent for exact matches. */
    score?: number;
}

/** A named group of symbols the provider considers one unit of the design. */
export interface ArchitectureGroup {
    name: string;
    symbolCount: number;
    /** How many groups depend on this one. */
    fanIn: number;
    /** How many groups this one depends on. */
    fanOut: number;
}

/**
 * One HTTP route the provider was able to recover, and where it was recovered.
 *
 * `origin` is not decoration. A route read out of the index is a finding of the
 * analysis; a route found by reading the source is a weaker claim, and a
 * surface that showed them identically would be overstating the second. Which
 * one a provider can offer depends entirely on the language it is looking at.
 */
export interface RouteRef {
    /** HTTP method in upper case, absent when the provider could not tell. */
    method?: string;
    /** The path as it is written at the registration, parameters and all. */
    path: string;
    /** Workspace-relative file the registration sits in, when known. */
    filePath?: string;
    /** 1-based line of the registration, when known. */
    line?: number;
    /** Name of the handler, when the provider could name it. */
    handler?: string;
    /** `index` means the analysis reported it; `source` means it was read off the text. */
    origin: 'index' | 'source';
}

/** A community of symbols the provider found by clustering the call graph. */
export interface ArchitectureCluster {
    id: string;
    /** Whatever the provider calls the community, usually the shared package. */
    label: string;
    memberCount: number;
    /** 0 to 1 when the provider measured how tightly the members hang together. */
    cohesion?: number;
    /** A few member names, for recognising the cluster. Never the whole membership. */
    topMembers: string[];
}

/** Which layer the provider placed one group in, and the reason it gave. */
export interface ArchitectureLayerAssignment {
    /** The group that was placed, usually a package. */
    group: string;
    /** The layer it was placed in, for example `entry`, `core` or `leaf`. */
    layer: string;
    /** One sentence saying why, when the provider gave one. */
    reason?: string;
}

/** Calls crossing from one group into another, counted. */
export interface ArchitectureBoundary {
    from: string;
    to: string;
    callCount: number;
}

/**
 * A symbol worth looking at before the others, and the readings that say so.
 *
 * `signals` carries the measurements themselves rather than a score, because a
 * score is not a finding: "three nested loops" and "reached by twenty symbols"
 * are different reasons to read something, and a single number would hide which
 * one applies.
 */
export interface ArchitectureHotspot {
    name: string;
    qualifiedName?: string;
    filePath?: string;
    /** 1-based declaration line, when known. */
    line?: number;
    /** How many other symbols reach this one, when the provider counted. */
    fanIn?: number;
    /** Cyclomatic complexity, when the provider measured it. */
    complexity?: number;
    /** Cognitive complexity, when the provider measured it. */
    cognitive?: number;
    /** Deepest loop nesting, when the provider measured it. */
    loopDepth?: number;
    /** True when something is allocated inside a loop. */
    allocationInLoop?: boolean;
    /** True when a linear scan runs inside a loop. */
    scanInLoop?: boolean;
    /** True when the symbol recurses with no reachable base case. */
    unguardedRecursion?: boolean;
}

/**
 * One file depending on another, as the index recorded it.
 *
 * Deliberately file to file rather than symbol to symbol. A dependency between
 * two files is what a reader meeting a repository for the first time can act
 * on: it says which file has to make sense before another one can, which is the
 * order somebody reads a codebase in. Symbol-level edges answer a different
 * question and the call relation already answers it.
 *
 * Both paths are workspace-relative, so a dependency graph is a fact about the
 * repository and not about the machine holding it. That is what lets an artefact
 * derived from one be committed.
 */
export interface ModuleDependency {
    /** Workspace-relative path of the file that declares the dependency. */
    from: string;
    /** Workspace-relative path of the file it depends on. */
    to: string;
}

/**
 * Every file-level dependency the index recorded, plus whether that is all of
 * them.
 *
 * `truncated` is the honest half. A provider reads the edges under a ceiling,
 * and an order derived from a partial graph is an order the repository does not
 * have; a consumer that could not tell the two apart would present a guess as a
 * reading.
 */
export interface ModuleDependencyGraph {
    edges: ModuleDependency[];
    /** True when the read stopped at its bound, so `edges` is a floor. */
    truncated: boolean;
}

/** A whole-project structural summary, deliberately shallow. */
export interface ArchitectureOverviewDto {
    projectName?: string;
    totalSymbols: number;
    totalRelations: number;
    /** Symbol kinds and how many of each the project holds. */
    symbolKinds: { kind: string; count: number }[];
    /** Relation families and how many of each the project holds. */
    relationKinds: { kind: string; count: number }[];
    languages: { language: string; fileCount: number }[];
    groups: ArchitectureGroup[];
    entryPoints: SymbolSearchHit[];
    /**
     * HTTP routes, empty when the provider cannot recover any for this project.
     * Empty is not "there are none": see `ProviderCapabilities.routes`.
     */
    routes: RouteRef[];
    clusters: ArchitectureCluster[];
    layers: ArchitectureLayerAssignment[];
    boundaries: ArchitectureBoundary[];
    hotspots: ArchitectureHotspot[];
    /** Files the index holds, workspace-relative. Empty when the provider does not list them. */
    files: string[];
}

/**
 * The per-symbol readings that say how expensive and how tangled one symbol is.
 *
 * Deliberately the same shape as {@link ArchitectureHotspot}, because it is the
 * same reading. The map asks for the twenty symbols that carry the strongest
 * signals; an impact assessment asks for the signals of a named set. A second
 * type would have to be kept in step with this one forever, and the first time
 * it drifted the two surfaces would disagree about the same function.
 */
export type SymbolComplexity = ArchitectureHotspot;

/**
 * How one symbol came to be in a change set.
 *
 * None of these three says what happened to the symbol. The analysis reports
 * which symbols sit in a file that differs from the baseline; it does not say
 * whether one was added, edited or left alone while its neighbour moved. So the
 * vocabulary here names the reading and never the edit, and a surface that
 * printed "modified" next to one of these rows would be inventing a finding.
 *
 * `declared` is a symbol the analysis named inside a changed file. `module` is
 * the changed file itself, which the analysis reports as a symbol of its own.
 * `caller` is a symbol CodeAtlas reached by walking invocations inwards from
 * those, which is a reading of the index rather than of the diff.
 */
export type ChangeReach = 'declared' | 'module' | 'caller';

/** One symbol a change set reaches, and how far out it sits. */
export interface ChangeImpactSymbol {
    name: string;
    qualifiedName?: string;
    /** Workspace-relative path of the declaring file, when known. */
    filePath?: string;
    /** 1-based declaration line, when known. */
    line?: number;
    kind: CodeAtlasSymbolKind;
    /** How this symbol was reached. Never a claim about what was edited. */
    changeKind: ChangeReach;
    /** 0 for a symbol in a changed file, 1 for its direct callers, and so on. */
    distance: number;
    /** True when the index flagged the symbol as test code. */
    isTest?: boolean;
}

/** What a set of edits touches, as far as the provider can tell. */
export interface ChangeImpactDto {
    /** The comparison point the answer is relative to, when the provider has one. */
    baselineRef?: string;
    /** Workspace-relative paths that differ from the baseline. */
    changedFiles: string[];
    /** Symbols reachable from the changed files within `depth` steps. */
    impacted: SymbolSearchHit[];
    /**
     * The same symbols with the two things `impacted` cannot carry: how far out
     * each one sits, and how it was reached.
     *
     * Separate from `impacted` rather than replacing it, because a
     * `SymbolSearchHit` is what every other surface consumes and a change set is
     * the only place a distance means anything.
     */
    symbols: ChangeImpactSymbol[];
    /** How many steps of reachability the provider walked. */
    depth?: number;
    /**
     * How many declarations the analysis took as the starting point, when it
     * says.
     *
     * Carried because it is the only thing the backend of PR 1860 reports about
     * the changed symbols themselves: it scopes its seeds to the changed hunks
     * and gives their number, never their names. A surface that lists the
     * symbols the index places inside the changed files can therefore say how
     * many of them the analysis actually treated as changed, instead of leaving
     * a reader to assume the two lists are the same list.
     */
    seedSymbols?: number;
    /** Deepest caller distance this answer actually reached. */
    walkedDistance?: number;
    /**
     * True when the walk stopped at a bound rather than at the end of the graph,
     * so the downstream list is a floor and not a total.
     */
    truncated?: boolean;
    /**
     * The analysis answer as it arrived, untouched.
     *
     * Carried so a reader can compare what the backend said with what CodeAtlas
     * derived from it. Everything above this line is a mapping; this is the
     * thing being mapped, and a surface that shows a distance the backend never
     * mentioned should be able to prove where the rest came from.
     */
    raw?: Record<string, unknown>;
}

/** Whether an analysis backend is usable right now. */
export interface EngineInfo {
    available: boolean;
    /** Semantic version string of the backend, when it reports one. */
    version?: string;
    /** Identifier of the provider implementation, for attribution. */
    providerId: string;
    /** Human-readable reason when `available` is false. */
    detail?: string;
}

/** Lifecycle of the workspace index, as far as the product cares. */
export type IndexState =
    /** Never indexed. */
    | 'absent'
    /** A build is running now. */
    | 'indexing'
    /** Usable. */
    | 'ready'
    /** Usable but known to lag the working tree. */
    | 'stale'
    /** The last build failed; `message` says why. */
    | 'failed';

/** A snapshot of index progress. Placeholder shape; the streaming form lands with the index service. */
export interface IndexProgress {
    state: IndexState;
    symbolCount?: number;
    relationCount?: number;
    /** Monotonic counter; evidence from an older generation is discarded, never patched. */
    generation?: number;
    message?: string;
}

/** Knobs for a workspace index build. */
export interface IndexOptions {
    /** `full` rebuilds from scratch, `incremental` updates what changed. */
    mode?: 'full' | 'incremental';
    /** Pin the project identity instead of deriving it from the root path. */
    projectName?: string;
}

/** Cross-cutting knobs accepted by every read call. */
export interface ProviderQueryOptions {
    /** Pin the project identity instead of deriving it from the root path. */
    projectName?: string;
    /**
     * Index generation to stamp on the evidence this call returns.
     *
     * The provider is constructed once and lives as long as the backend, while
     * the generation changes on every rebuild. Passing it per call is what lets
     * a cached answer be recognised as stale instead of being trusted forever;
     * absent, the provider falls back to the generation it was built with.
     */
    generation?: number;
}

/** Knobs for a path walk. */
export interface TraceOptions extends ProviderQueryOptions {
    /** Maximum hops away from the root. Providers may return fewer. */
    maxDepth?: number;
}

/**
 * The single surface the rest of CodeAtlas uses to learn about code.
 *
 * Implementations live in the intelligence extension. Nothing in this
 * interface commits the product to one analysis backend, one storage format
 * or one query language.
 */
export interface IntelligenceProvider {
    /** Stable identifier recorded on every piece of evidence this provider emits. */
    readonly id: string;

    /** What this provider can answer. Cheap and synchronous: never hits the backend. */
    capabilities(): ProviderCapabilities;

    /** Whether the backend is present and which version answered. */
    engineInfo(): Promise<EngineInfo>;

    /** Build or refresh the index for a workspace. Resolves when the build finishes. */
    indexWorkspace(root: string, opts?: IndexOptions): Promise<IndexProgress>;

    /** Current index state for a workspace without triggering a build. */
    indexState(root: string, opts?: ProviderQueryOptions): Promise<IndexProgress>;

    /**
     * Turn a caret position into the innermost symbol that encloses it.
     * `oneBasedLine` is graph line space, not editor line space; convert with
     * positions.ts before calling.
     */
    resolveSymbolAt(
        workspaceRoot: string,
        filePath: string,
        oneBasedLine: number,
        opts?: ProviderQueryOptions
    ): Promise<ResolveResult>;

    /** Facts about one symbol, restricted to the requested families. */
    getFacts(
        workspaceRoot: string,
        symbol: SymbolRef,
        kinds: FactKind[],
        opts?: ProviderQueryOptions
    ): Promise<SymbolFacts>;

    /** Walk invocation paths away from a symbol. */
    tracePaths(
        workspaceRoot: string,
        symbol: SymbolRef,
        direction: TraceDirection,
        opts?: TraceOptions
    ): Promise<TraceResult>;

    /** Whole-project structural summary. */
    architectureOverview(root: string, opts?: ProviderQueryOptions): Promise<ArchitectureOverviewDto>;

    /**
     * Which file depends on which, across the whole project.
     *
     * Separate from the summary above because it is a different size of answer:
     * the summary is a page and this is one row per import statement in the
     * repository. Nothing that renders a panel wants it; the one consumer is
     * the thing that has to put a whole workspace in reading order.
     *
     * An empty graph is not "this project has no dependencies": a provider that
     * records no import relation for a language answers exactly the same way as
     * one reading a project of nine unrelated files. Consumers must say which
     * of the two they are looking at from the capabilities, never from the
     * emptiness.
     */
    moduleDependencies(root: string, opts?: ProviderQueryOptions): Promise<ModuleDependencyGraph>;

    /** What a set of edits touches, relative to `sinceRef` when the provider supports it. */
    changeImpact(root: string, sinceRef?: string, opts?: ProviderQueryOptions): Promise<ChangeImpactDto>;

    /**
     * The complexity readings of a named set of symbols, in one batch.
     *
     * A read rather than a sweep. `architectureOverview` already sweeps every
     * callable and keeps the twenty strongest; asking it for the readings of one
     * arbitrary symbol would mean paying for the whole sweep to look up a row
     * that the ranking may well have dropped.
     *
     * A name the index does not hold is simply absent from the answer, which is
     * why the answer is a list and not a map keyed by the request: "the index
     * has no such symbol" and "the index has it and measured nothing" are
     * different findings, and the caller needs to be able to tell them apart.
     */
    getComplexity(
        root: string,
        qualifiedNames: string[],
        opts?: ProviderQueryOptions
    ): Promise<SymbolComplexity[]>;

    /** Find symbols by name pattern. */
    searchSymbols(
        root: string,
        pattern: string,
        limit?: number,
        opts?: ProviderQueryOptions
    ): Promise<SymbolSearchHit[]>;

    /**
     * The 1-based line one named symbol is declared on, in one file.
     *
     * The missing half of {@link searchSymbols}. A search hit names a symbol and
     * the file it lives in and, at engine 0.9.0, nothing about where in that
     * file: `SymbolSearchHit.line` is optional precisely because the search does
     * not answer it. So a caller that has found a symbol by name and now wants
     * to open it, or to resolve it into a {@link SymbolRef} with a range, has
     * one question left and this is it.
     *
     * Answered from the index and never from the file, which is why it is here
     * rather than in a caller: reading the source to find a declaration would be
     * a second, disagreeing definition of where a symbol starts, and under the
     * `ir-only` data policy it would also be reading source that must not be
     * read at all.
     *
     * `undefined` means the index holds no declaration of that name in that
     * file, which is a finding and not an error.
     */
    declarationLineOf(
        root: string,
        filePath: string,
        name: string,
        opts?: ProviderQueryOptions
    ): Promise<number | undefined>;

    /** Source text of one symbol, as the provider stored it. */
    getSnippet(root: string, qualifiedName: string, opts?: ProviderQueryOptions): Promise<string>;

    /** Drop everything the provider holds about a workspace. */
    deleteProject(root: string, opts?: ProviderQueryOptions): Promise<void>;
}

/** Injection key for the active provider. */
export const IntelligenceProvider = Symbol('IntelligenceProvider');

/**
 * Der Ausschnitt des Providers, den der IR-Builder braucht.
 *
 * Im Referenzprojekt steht dieser Typ im IR-Builder selbst. Hier steht er an
 * der Schnittstelle, weil er nichts anderes ist als eine Teilmenge von ihr:
 * wer die Grenze gegen den Lock-in liest, soll auch sehen, welchen Teil davon
 * ein IR-Bau tatsaechlich anfasst, ohne den Builder aufzuschlagen. Der Builder
 * exportiert ihn weiterhin mit, damit die Importe der portierten Module und
 * ihrer Tests unveraendert bleiben.
 */
export type IrFactSource = Pick<IntelligenceProvider, 'id' | 'getFacts' | 'getSnippet'>;
