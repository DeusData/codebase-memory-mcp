/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/src/node/provider/cbm-provider.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Die Logik ist semantisch
 * unveraendert: die Zustandstabelle, die Zusammenfuehrung der beiden
 * Fehlerrelationen, die immer nur inferierte Test-Heuristik, der
 * Projektnamen-Cache mit Toleranz fuer den Schraegstrich am Ende, die Deckel
 * und die Zeilen-Rueckfaelle stehen hier so, wie sie dort stehen. Die
 * Provider-Id bleibt 'cbm', damit jede Evidenz weiter derselben Quelle
 * zugeschrieben wird.
 *
 * Aenderungen gegenueber dem Original, alle im Transport und alle unten am Ort
 * begruendet:
 *
 * - Der Client ist RpcIntelligenceClient auf POST /rpc statt der CLI ueber
 *   einen Kindprozess. Nichts unter src/** darf einen Prozess starten.
 * - engineInfo fragt den Server statt ein Binaerprogramm.
 * - indexWorkspace und deleteProject sind auf /rpc gesperrt und lehnen ab,
 *   statt eine Antwort zu erfinden.
 * - Der Route-Scan hat keinen Vorgabe-Leser mehr: ohne Dateisystem gibt es
 *   nichts zu lesen, und Routen kommen dann nur aus dem Index.
 * - Der Klassenname heisst CbmRpcProvider, weil im selben Projekt spaeter ein
 *   zweiter Transport denkbar ist und zwei Klassen namens CbmProvider dann
 *   nicht unterscheidbar waeren.
 */
/**
 * The first `IntelligenceProvider` implementation.
 *
 * Its whole job is translation: product questions in, engine queries out,
 * evidence-carrying facts back. Two rules make the translation honest.
 *
 * Rule one: an empty answer is not a fact until we know why it is empty. Every
 * fact family therefore carries the state that explains its emptiness, and the
 * decision is made by one pure function (`knowledgeStateFor`) so it can be
 * tested as a table rather than as a pile of branches.
 *
 * Rule two: never dress a heuristic as a reading. The 0.9.0 engine records no
 * test relation for TypeScript, so `testedBy` is derived from callers that the
 * engine flagged as test code. That derivation is real and useful, and it is
 * reported as `inferred` with the strategy named in the evidence, forever.
 * An empty `testedBy` never becomes `known`, because "no test caller found"
 * and "not tested" are different claims.
 */

import type {
    ArchitectureHotspot,
    ArchitectureOverviewDto,
    ChangeImpactDto,
    ChangeImpactSymbol,
    EngineInfo,
    FactKind,
    IndexOptions,
    IndexProgress,
    IndexState,
    IntelligenceProvider,
    ModuleDependency,
    ModuleDependencyGraph,
    ProviderCapabilities,
    ProviderQueryOptions,
    ResolveResult,
    RouteRef,
    SymbolComplexity,
    SymbolFacts,
    SymbolSearchHit,
    TraceDirection,
    TraceNode,
    TraceOptions,
    TraceResult
} from '../core/intelligence-provider';
import type { CodeAtlasSymbolKind, Range, SymbolRef } from '../core/focus-protocol';
import type {
    CallerRef,
    CallSite,
    DataRef,
    Evidence,
    Fact,
    KnowledgeState,
    TestRef,
    ThrowRef
} from '../core/semantic-ir';

import { EngineError, EngineUnavailableError } from './engine-errors';
import { RpcIntelligenceClient } from './rpc-client';
import { isAmbiguousPath, toBoolean, toOptionalNumber } from './rpc-schemas';
import type { DetectedSymbol, PathHop, PathWalk } from './rpc-schemas';
import type { EnclosingLabel } from './cypher';
import {
    CALLABLE_LABELS,
    COLUMNS,
    COMPLEXITY_BATCH_LIMIT,
    ENCLOSING_LABELS,
    callersOfAny,
    callsIn,
    callsOut,
    classCallTargets,
    complexityOf,
    declarationsInFiles,
    enclosingByLabel,
    envReads,
    fileExists,
    hotspotCandidates,
    IMPORT_EDGE_LIMIT,
    importEdges,
    indexedFiles,
    raises,
    throwsRelation,
    typeRefs
} from './cypher';
import { isRouteSource, readRoutes } from './route-reader';

/**
 * Identifier stamped on every piece of evidence this provider emits.
 *
 * The reference declares the string here and the class field reads it; this
 * port does it the other way round. The field is what actually lands on every
 * `Evidence.providerId`, so that is where the string belongs, and typing the
 * constant from the field means the two cannot drift: change the field and
 * this line stops compiling.
 */
export const CBM_PROVIDER_ID: CbmRpcProvider['id'] = 'cbm';

/** How many hotspots the overview carries. More than a reader looks at in one sitting. */
export const MAX_HOTSPOTS = 20;

/** How many routes the overview carries. */
export const MAX_ROUTES = 200;

/**
 * How many invocation steps a change assessment walks outwards.
 *
 * Two, and the number is a reading decision rather than a performance one. One
 * step answers "who calls what I touched", which is the question; two answers
 * "and who calls them", which is where an endpoint usually turns up. Three
 * steps out of a service function is most of a codebase, and a list nobody
 * finishes reading is the same as no list.
 */
export const IMPACT_WALK_DISTANCE = 2;

/** How many symbols one change assessment carries before it says it stopped early. */
export const MAX_IMPACT_SYMBOLS = 200;

/** How many changed files are looked up in the index to recover identities. */
export const MAX_CHANGED_FILES_READ = 60;

/**
 * CodeAtlas's own state directory inside the workspace.
 *
 * Indexing writes a project pin here, so a workspace that has been opened once
 * has a file in it that git reports as changed and that the reader did not
 * touch. Listing it in an impact assessment would be reporting our bookkeeping
 * back to them as their own work, and worse, it would inflate the changed-file
 * count that the narrative and the risk rules both read. It is dropped from the
 * change set, and this constant is the one place that decision lives.
 */
export const CODEATLAS_STATE_PREFIX = '.codeatlas/';

/** True when a path is CodeAtlas's own state rather than the reader's code. */
export function isCodeAtlasState(path: string): boolean {
    return path === '.codeatlas' || path.startsWith(CODEATLAS_STATE_PREFIX);
}

/** How many files the route scan opens. A bound on cost, not on correctness. */
export const MAX_ROUTE_SCAN_FILES = 800;

/** Files above this size are generated or vendored, and are not read. */
export const MAX_ROUTE_SCAN_BYTES = 512 * 1024;

/** Relation labels written into evidence. Product wording, not engine wording. */
export const EVIDENCE_RELATIONS = {
    invocation: 'invocation',
    raise: 'raise',
    /**
     * A declared error, as opposed to a raised one. Kept apart from `raise` so a
     * reader looking at the evidence can tell "this code throws it" from "this
     * signature declares it", which is the difference between the two relations
     * the engine records.
     */
    throwDeclaration: 'throw-declaration',
    environmentRead: 'environment-read',
    typeReference: 'type-reference'
} as const;

/** Named heuristics. A strategy string in evidence must always match one of these. */
export const STRATEGIES = {
    /** A caller the engine flagged as test code is treated as a test of the callee. */
    testCaller: 'is-test-caller',
    /** The target of the invocation is a class, so the site constructs rather than calls. */
    construction: 'construction',
    /** An ordinary resolved invocation. */
    directCall: 'direct-call'
} as const;

/** Inputs to the state decision. Kept as data so the table can be tested directly. */
export interface FactStateContext {
    /** False when no engine answered at all. */
    engineAvailable: boolean;
    /** False when the symbol's project or file is not in the index. */
    indexed: boolean;
    /** False when this provider cannot answer this family for this language. */
    supported: boolean;
    /** True when the value came from a heuristic rather than from a reading. */
    derived: boolean;
}

/**
 * The one place that decides how much trust a fact carries.
 *
 * Order matters: an unavailable engine outranks everything, because we do not
 * know whether the family is supported for a workspace we could not open.
 * Support outranks indexing, because an unsupported family stays unsupported
 * however complete the index becomes.
 */
export function knowledgeStateFor(ctx: FactStateContext): KnowledgeState {
    if (!ctx.engineAvailable) {
        return 'unknown';
    }
    if (!ctx.supported) {
        return 'unsupported';
    }
    if (!ctx.indexed) {
        return 'notIndexed';
    }
    return ctx.derived ? 'inferred' : 'known';
}

/** Map an engine label to the narrower product symbol kind. */
export function symbolKindOf(label: string | undefined): CodeAtlasSymbolKind {
    switch ((label ?? '').toLowerCase()) {
        case 'function': return 'function';
        case 'method': return 'method';
        case 'class': return 'class';
        case 'interface': return 'interface';
        case 'module':
        case 'file': return 'module';
        case 'variable': return 'variable';
        case 'type': return 'type';
        case 'route': return 'route';
        default: return 'unknown';
    }
}

/** Map an engine label to the caller kinds the semantic IR models. */
export function sourceKindOf(label: string | undefined): 'function' | 'method' | 'module' | undefined {
    const kind = symbolKindOf(label);
    return kind === 'function' || kind === 'method' || kind === 'module' ? kind : undefined;
}

/**
 * The stable identity of an indexed symbol, as this provider can supply one.
 *
 * `SymbolRef.nodeId` is documented as "stable engine graph node id, absent when
 * the symbol is not indexed", and the whole product reads its presence as "the
 * index knows this thing": `EditorSyncService.revealSymbol` publishes
 * `indexed: Boolean(symbol.nodeId)`, so a reveal of a symbol without one tells
 * every panel on the bus that a perfectly well indexed symbol is not indexed.
 * That is exactly the demotion the guided review had to work around in C16.
 *
 * The engine's own numeric node ids are not reachable: the 0.9.0 query subset
 * has no `id()` function and no property holding one, so there is nothing to
 * return. The qualified name is what is available and it is a genuine stable
 * identity rather than a stand-in for one: it is the key the engine itself
 * accepts for every symbol-addressed call this provider makes (`callsIn`,
 * `callsOut`, `raises`, `getCodeSnippet`, `tracePath`), and it is already what
 * the IR cache, the understanding store and the frontend's own caches key on.
 *
 * A symbol with no qualified name gets no node id, which is correct: that is
 * precisely the case where the index cannot name the thing.
 */
export function stableNodeId(qualifiedName: string | undefined): string | undefined {
    return qualifiedName && qualifiedName.length > 0 ? qualifiedName : undefined;
}

function fileUri(workspaceRoot: string, relativePath: string): string {
    const root = workspaceRoot.replace(/\/+$/, '');
    const rest = relativePath.replace(/^\/+/, '');
    return `file://${root}/${rest}`;
}

/** Turn 1-based inclusive graph lines into a 0-based editor range. */
function graphRange(startLine: number | undefined, endLine: number | undefined): Range {
    const start = Math.max(0, (startLine ?? 1) - 1);
    const end = Math.max(start, (endLine ?? startLine ?? 1) - 1);
    return {
        start: { line: start, character: 0 },
        end: { line: end, character: 0 }
    };
}

/**
 * What the first-hop enrichment of a path walk recovers about one reached
 * symbol. Every field is optional because each one comes from a different
 * column and a column the engine did not fill arrives empty rather than absent.
 */
interface HopFacts {
    /** Display name, needed when the reading contributes a row the walk omitted. */
    name?: string;
    /** Workspace-relative path of the declaring file. */
    file?: string;
    /** 1-based graph line of the declaration. */
    line?: number;
    isTest?: boolean;
    construction?: boolean;
}

/** What the index knows about one declaration, recovered by file and name. */
interface DeclarationRow {
    qualifiedName?: string;
    /** 1-based declaration line. */
    line?: number;
    isTest?: boolean;
}

/** Key of a declaration lookup: a name is only unique inside its own file. */
function declarationKey(filePath: string | undefined, name: string): string {
    return `${filePath ?? ''}|${name}`;
}

/**
 * The change tool's symbol list with its repeats removed.
 *
 * It reports the whole list once per repeated changed file, so a change to one
 * file that the diff mentions twice arrives as six rows describing three
 * symbols. Deduplicated on what identifies a row at 0.9.0, which is the file,
 * the name and the label, because that is all a row carries.
 */
export function dedupeDetected(entries: readonly DetectedSymbol[]): DetectedSymbol[] {
    const seen = new Set<string>();
    const out: DetectedSymbol[] = [];
    for (const entry of entries) {
        const key = [
            entry.file ?? entry.file_path ?? '',
            entry.name ?? '',
            entry.label ?? '',
            entry.qualified_name ?? ''
        ].join('|');
        if (seen.has(key)) {
            continue;
        }
        seen.add(key);
        out.push(entry);
    }
    return out;
}

/**
 * Every symbol the index places inside a set of files, as change rows.
 *
 * Used only where the analysis names a count of changed declarations and not
 * their names. Sorted by file and then by line, so two runs against one index
 * produce the same list, and every row carries the identity a surface needs to
 * open it.
 */
export function declarationsIn(
    filePaths: readonly string[],
    declarations: Map<string, DeclarationRow>
): ChangeImpactSymbol[] {
    const wanted = new Set(filePaths);
    const rows: ChangeImpactSymbol[] = [];
    for (const [key, row] of declarations) {
        const separator = key.indexOf('|');
        const filePath = key.slice(0, separator);
        const name = key.slice(separator + 1);
        if (!wanted.has(filePath) || name.length === 0) {
            continue;
        }
        rows.push({
            name,
            qualifiedName: row.qualifiedName,
            filePath,
            line: row.line,
            kind: 'unknown',
            changeKind: 'declared',
            distance: 0,
            ...(row.isTest === undefined ? {} : { isTest: row.isTest })
        });
    }
    // Ordinal, never localeCompare: two machines with two collations would
    // otherwise order one change set two ways. Dieselbe Regel wie im
    // Tour-Generator und im Closure-Walk.
    return rows.sort((a, b) => {
        const left = a.filePath ?? '';
        const right = b.filePath ?? '';
        if (left !== right) {
            return left < right ? -1 : 1;
        }
        return (a.line ?? 0) - (b.line ?? 0);
    });
}

export interface EnclosingCandidate {
    label: EnclosingLabel;
    name: string;
    qualifiedName: string;
    startLine: number;
    endLine: number;
}

/**
 * Innermost candidate wins, measured by declaration span. A tie means two
 * labels describe the same extent, and then the narrower label wins.
 */
export function pickInnermost(candidates: EnclosingCandidate[]): EnclosingCandidate | undefined {
    const rank = (label: EnclosingLabel): number => ENCLOSING_LABELS.indexOf(label);
    return [...candidates].sort((a, b) => {
        const spanA = a.endLine - a.startLine;
        const spanB = b.endLine - b.startLine;
        if (spanA !== spanB) {
            return spanA - spanB;
        }
        return rank(a.label) - rank(b.label);
    })[0];
}

export interface CbmRpcProviderOptions {
    /**
     * Index generation stamped on evidence. Bumped by the index service on each
     * rebuild; evidence from an older generation is discarded, never patched.
     */
    generation?: number;
    /**
     * Reads one workspace file as text, or answers undefined when it cannot.
     *
     * Injected so the route scan can be driven without a file system, and so a
     * provider that reads a workspace over a connection rather than off a disk
     * has somewhere to say so. In the browser there is nothing to inject and
     * the field stays absent, which is why the scan reports nothing there and
     * routes come from the index alone. A Node caller that injects a reader is
     * expected to honour {@link MAX_ROUTE_SCAN_BYTES} itself; the ceiling moved
     * to the reader along with the file system.
     */
    readSource?: (absolutePath: string) => string | undefined;
}

export class CbmRpcProvider implements IntelligenceProvider {

    readonly id = 'cbm';

    /** Cache of workspace root to engine project name, filled from the project list. */
    private readonly projectNames = new Map<string, string>();

    constructor(
        protected readonly client: RpcIntelligenceClient,
        protected readonly options: CbmRpcProviderOptions = {}
    ) { }

    capabilities(): ProviderCapabilities {
        return {
            callers: true,
            callees: true,
            throws: true,
            envReads: true,
            typeRefs: true,
            // No first class test relation is recorded for TypeScript at 0.9.0.
            tests: 'heuristic',
            routes: true,
            architecture: true,
            changeImpact: true,
            // Measured against this server rather than assumed, and false for
            // a second reason here. In the reference the tool accepted events
            // and wrote nothing, so reporting `true` because the call succeeded
            // would have been the exact dishonesty this flag exists to prevent.
            // On /rpc the tool is not offered at all: the read-only allowlist
            // answers `ingest_traces` with 403 and -32601. Either way there is
            // no runtime relation to query, and the product's own trace store
            // is what a surface reads.
            runtimeTraces: false,
            semanticSearch: true,
            // Invocations are deduplicated per target, so a reported line is the
            // last recorded site and not the only one.
            callSiteGranularity: 'per-target'
        };
    }

    /**
     * Whether the backend answers, asked by asking it something.
     *
     * The original probed the engine binary and read its version out of
     * `--version`. There is no binary on this side of the wire and no version
     * on /rpc, so the question is put to the server instead: the cheapest read
     * it offers is the project list, and a surface that answers it is a surface
     * that can be asked the rest. `version` stays absent rather than being
     * filled with the UI's own build number, which would answer a question
     * about the analysis backend with a fact about the frontend.
     */
    async engineInfo(): Promise<EngineInfo> {
        try {
            await this.client.listProjects();
            return { available: true, providerId: this.id };
        } catch (error) {
            return {
                available: false,
                providerId: this.id,
                detail: error instanceof Error ? error.message : String(error)
            };
        }
    }

    /**
     * Build or refresh the index. Not something this surface can do.
     *
     * The server's read-only allowlist does not offer `index_repository` on
     * /rpc, so this rejects with the engine-unavailable error the client
     * raises. It rejects rather than answering `{ state: 'failed' }`, because
     * `failed` means "the last build failed" and would tell a reader something
     * about their workspace. Nothing was attempted, and the caller has to be
     * able to tell those apart.
     */
    async indexWorkspace(_root: string, _opts: IndexOptions = {}): Promise<IndexProgress> {
        return this.client.indexRepository();
    }

    async indexState(root: string, opts?: ProviderQueryOptions): Promise<IndexProgress> {
        const project = await this.projectFor(root, opts);
        if (!project) {
            return { state: 'absent', generation: this.generation() };
        }
        try {
            const status = await this.client.indexStatus(project);
            return {
                state: indexStateOf(status.status),
                symbolCount: status.nodes,
                relationCount: status.edges,
                generation: this.generation(),
                message: status.status
            };
        } catch (error) {
            if (error instanceof EngineUnavailableError) {
                return { state: 'absent', generation: this.generation(), message: error.message };
            }
            if (error instanceof EngineError) {
                return { state: 'absent', generation: this.generation(), message: error.message };
            }
            throw error;
        }
    }

    async resolveSymbolAt(
        workspaceRoot: string,
        filePath: string,
        oneBasedLine: number,
        opts?: ProviderQueryOptions
    ): Promise<ResolveResult> {
        let project: string | undefined;
        try {
            project = await this.projectFor(workspaceRoot, opts);
        } catch (error) {
            if (error instanceof EngineUnavailableError) {
                return { kind: 'engine-unavailable', reason: error.message };
            }
            throw error;
        }
        if (!project) {
            return { kind: 'file-not-indexed', filePath };
        }

        try {
            const candidates: EnclosingCandidate[] = [];
            for (const label of ENCLOSING_LABELS) {
                const rows = await this.client.queryRows(project, enclosingByLabel(label, filePath, oneBasedLine));
                for (const row of rows) {
                    const qualifiedName = row[COLUMNS.enclosing[1]];
                    const startLine = toOptionalNumber(row[COLUMNS.enclosing[2]]);
                    const endLine = toOptionalNumber(row[COLUMNS.enclosing[3]]);
                    if (!qualifiedName || startLine === undefined || endLine === undefined) {
                        continue;
                    }
                    candidates.push({
                        label,
                        name: row[COLUMNS.enclosing[0]] ?? qualifiedName,
                        qualifiedName,
                        startLine,
                        endLine
                    });
                }
            }

            if (candidates.length === 0) {
                const fileRows = await this.client.queryRows(project, fileExists(filePath));
                return fileRows.length > 0
                    ? { kind: 'no-symbol-at-line', filePath }
                    : { kind: 'file-not-indexed', filePath };
            }

            const innermost = pickInnermost(candidates)!;
            const toRef = (candidate: EnclosingCandidate): SymbolRef => ({
                nodeId: stableNodeId(candidate.qualifiedName),
                name: candidate.name,
                qualifiedName: candidate.qualifiedName,
                kind: symbolKindOf(candidate.label),
                uri: fileUri(workspaceRoot, filePath),
                range: graphRange(candidate.startLine, candidate.endLine),
                projectName: project
            });
            const enclosing = candidates
                .filter(candidate => candidate !== innermost)
                .sort((a, b) => (a.endLine - a.startLine) - (b.endLine - b.startLine))
                .map(toRef);
            return { kind: 'ok', symbol: toRef(innermost), enclosing };
        } catch (error) {
            if (error instanceof EngineUnavailableError) {
                return { kind: 'engine-unavailable', reason: error.message };
            }
            if (error instanceof EngineError) {
                // A refused query on a known project means the project no longer
                // holds this file: not indexed, rather than nothing at the line.
                return { kind: 'file-not-indexed', filePath };
            }
            throw error;
        }
    }

    async getFacts(
        workspaceRoot: string,
        symbol: SymbolRef,
        kinds: FactKind[],
        opts?: ProviderQueryOptions
    ): Promise<SymbolFacts> {
        const facts: SymbolFacts = {};
        const qualifiedName = symbol.qualifiedName;

        let project: string | undefined;
        let engineAvailable = true;
        try {
            project = symbol.projectName ?? await this.projectFor(workspaceRoot, opts);
        } catch (error) {
            if (!(error instanceof EngineUnavailableError)) {
                throw error;
            }
            engineAvailable = false;
        }
        const indexed = engineAvailable && !!project && !!qualifiedName;

        // testedBy rides on the caller rows, so callers are fetched whenever
        // either family was requested.
        const wantCallers = kinds.includes('callers');
        const wantTested = kinds.includes('testedBy');
        let callerRows: Record<string, string>[] = [];
        let callersFailed = false;
        if ((wantCallers || wantTested) && indexed) {
            try {
                callerRows = await this.client.queryRows(project!, callsIn(qualifiedName!));
            } catch (error) {
                callersFailed = true;
                engineAvailable = engineAvailable && !(error instanceof EngineUnavailableError);
                if (!(error instanceof EngineError) && !(error instanceof EngineUnavailableError)) {
                    throw error;
                }
            }
        }
        const callersUsable = indexed && !callersFailed;

        for (const kind of kinds) {
            switch (kind) {
                case 'callees':
                    facts.callees = await this.calleesFact(project, qualifiedName, engineAvailable, indexed);
                    break;
                case 'callers':
                    facts.callers = this.callersFact(callerRows, engineAvailable, callersUsable);
                    break;
                case 'testedBy':
                    facts.testedBy = this.testedByFact(callerRows, engineAvailable, callersUsable);
                    break;
                case 'throws':
                    facts.throws = await this.throwsFact(project, qualifiedName, engineAvailable, indexed, symbol);
                    break;
                case 'envReads':
                    facts.envReads = await this.envReadsFact(project, qualifiedName, engineAvailable, indexed, symbol);
                    break;
                case 'typeRefs':
                    facts.typeRefs = await this.typeRefsFact(project, qualifiedName, engineAvailable, indexed);
                    break;
            }
        }
        // The generation belongs to the caller's read, not to the provider's
        // lifetime: the provider is built once and the index is rebuilt many
        // times. Stamping here, at the one boundary where the caller's pinned
        // generation is known, keeps every fact builder free of it.
        return opts?.generation === undefined ? facts : stampGeneration(facts, opts.generation);
    }

    /**
     * Walk invocation paths away from a symbol.
     *
     * Two of the three knobs in the product's contract are honoured here rather
     * than by the engine, because the 0.9.0 engine accepts them and ignores
     * them. Measured against the frozen fixture:
     *
     *  - `direction` only produces an answer when it is `both` or absent.
     *    Asking for `callees` or `callers` returns a walk with neither list on
     *    it, which a caller cannot tell apart from a symbol that reaches
     *    nothing. So the call is made without a direction and the answer is
     *    filtered here.
     *  - `max_depth` does not bound the walk. Asking for one hop still returns
     *    hop two and hop three, so the bound is applied to the answer.
     *
     * Both are translations rather than workarounds: the product asks for a
     * directed, bounded walk, and this is what that means against this backend.
     * Filtering after the fact costs the engine the work of the other direction,
     * which for a call graph walk is the same traversal it was doing anyway.
     */
    async tracePaths(
        workspaceRoot: string,
        symbol: SymbolRef,
        direction: TraceDirection,
        opts?: TraceOptions
    ): Promise<TraceResult> {
        const project = symbol.projectName ?? await this.projectFor(workspaceRoot, opts);
        const target = symbol.qualifiedName ?? symbol.name;
        if (!project) {
            return { status: 'ok', direction, root: symbol, layers: [] };
        }
        // No direction and no depth on the wire: see the note above.
        const result = await this.client.tracePath(project, target, {});
        if (isAmbiguousPath(result)) {
            return {
                status: 'ambiguous',
                candidates: result.suggestions.map(suggestion => ({
                    nodeId: stableNodeId(suggestion.qualified_name),
                    name: suggestion.name,
                    qualifiedName: suggestion.qualified_name,
                    kind: symbolKindOf(suggestion.label),
                    uri: suggestion.file_path ? fileUri(workspaceRoot, suggestion.file_path) : '',
                    range: graphRange(undefined, undefined),
                    projectName: project
                }))
            };
        }

        const maxDepth = opts?.maxDepth;
        const layers: TraceNode[][] = [];
        const push = (hops: PathHop[] | undefined, via: 'callers' | 'callees'): void => {
            if (direction !== 'both' && direction !== via) {
                return;
            }
            for (const hop of hops ?? []) {
                const depth = Math.max(1, hop.hop ?? 1);
                if (maxDepth !== undefined && depth > maxDepth) {
                    continue;
                }
                const index = depth - 1;
                while (layers.length <= index) {
                    layers.push([]);
                }
                layers[index].push({
                    symbol: {
                        nodeId: stableNodeId(hop.qualified_name),
                        name: hop.name,
                        qualifiedName: hop.qualified_name,
                        kind: 'unknown',
                        uri: '',
                        range: graphRange(undefined, undefined),
                        projectName: project
                    },
                    hop: depth,
                    via
                });
            }
        };
        const walk = result as PathWalk;
        push(walk.callees, 'callees');
        push(walk.callers, 'callers');
        if (maxDepth === undefined || maxDepth >= 1) {
            await this.enrichFirstHop(workspaceRoot, project, symbol, direction, layers);
        }
        return { status: 'ok', direction, root: symbol, layers };
    }

    /**
     * Make the first hop complete, and say what it knows.
     *
     * The walk carries a name, a qualified name and a distance per hop and
     * nothing more: no file, no test flag, and no way to tell a construction
     * from a call. Those come from the invocation relations of one symbol,
     * which is one query per direction, and this is where they are read.
     *
     * The reading also completes the layer, which is the less obvious half. The
     * 0.9.0 walk only steps through symbols it treats as callable, so a caller
     * that is file-level code is missing from it even though the invocation
     * relation records it: `listUsers` is called by its own test file, the
     * relation says so, and the walk does not mention it. Adding those rows is
     * not an invention. The relation this reads is the same relation the walk is
     * built from, read directly and one hop out, so a symbol it names is a
     * direct neighbour by the engine's own account. Leaving them out would tell
     * a reader that nothing tests a symbol that a test calls, which is the one
     * claim this product exists not to make.
     *
     * The enrichment stops at hop 1, deliberately and visibly. Asking the same
     * questions for the whole walk would be one query per reached symbol per
     * layer, which on a real repository is hundreds of round trips for a panel
     * that has to feel like a tree control. Every node it touches is stamped
     * `enriched` and the deeper layers are left without it, which is what lets a
     * surface draw a badge only where a badge was looked up. Expanding a deeper
     * node issues a walk rooted at that node, whose own first hop is enriched in
     * turn, so a reader following the tree down never meets a row that could
     * have carried a badge and does not.
     *
     * Failure is not propagated. The walk has already succeeded here, and an
     * enrichment query that cannot run is a reason to show the tree without
     * badges rather than to show no tree at all. Each direction is guarded on
     * its own so one refusal does not cost the other its answer.
     */
    protected async enrichFirstHop(
        workspaceRoot: string,
        project: string,
        root: SymbolRef,
        direction: TraceDirection,
        layers: TraceNode[][]
    ): Promise<void> {
        const qualifiedName = root.qualifiedName;
        if (!qualifiedName) {
            return;
        }
        // Driven by what the caller asked for rather than by what the walk
        // returned: a direction whose walk came back empty is exactly the case
        // where the relation reading has something to add.
        const outgoing = direction === 'callees' || direction === 'both'
            ? await this.outgoingHopFacts(project, qualifiedName)
            : undefined;
        const incoming = direction === 'callers' || direction === 'both'
            ? await this.incomingHopFacts(project, qualifiedName)
            : undefined;
        if (outgoing === undefined && incoming === undefined) {
            return;
        }
        const layer = layers[0] ?? [];
        const seen = { callees: new Set<string>(), callers: new Set<string>() };
        for (const node of layer) {
            const facts = node.via === 'callees' ? outgoing : incoming;
            if (facts === undefined) {
                continue;
            }
            node.enriched = true;
            const target = node.symbol.qualifiedName;
            if (target === undefined) {
                continue;
            }
            seen[node.via].add(target);
            const match = facts.get(target);
            if (match === undefined) {
                continue;
            }
            if (match.isTest !== undefined) {
                node.isTest = match.isTest;
            }
            if (match.construction !== undefined) {
                node.construction = match.construction;
            }
            // A walk hop names no file, so the first thing that does wins. The
            // guard keeps a later enrichment from overwriting a location the
            // engine reported directly.
            if (match.file && node.symbol.uri.length === 0) {
                node.symbol.uri = fileUri(workspaceRoot, match.file);
                node.symbol.range = graphRange(match.line, match.line);
            }
        }
        const added: TraceNode[] = [
            ...this.missingNeighbours(workspaceRoot, project, outgoing, seen.callees, 'callees'),
            ...this.missingNeighbours(workspaceRoot, project, incoming, seen.callers, 'callers')
        ];
        if (added.length === 0) {
            return;
        }
        if (layers.length === 0) {
            layers.push([]);
        }
        layers[0].push(...added);
    }

    /**
     * Direct neighbours the relation records and the walk did not mention.
     *
     * Stamped `enriched` like every other first-hop node, because they come from
     * the reading that does the stamping.
     */
    protected missingNeighbours(
        workspaceRoot: string,
        project: string,
        facts: Map<string, HopFacts> | undefined,
        seen: ReadonlySet<string>,
        via: 'callers' | 'callees'
    ): TraceNode[] {
        if (facts === undefined) {
            return [];
        }
        const out: TraceNode[] = [];
        for (const [qualifiedName, entry] of facts) {
            if (seen.has(qualifiedName)) {
                continue;
            }
            out.push({
                symbol: {
                    nodeId: stableNodeId(qualifiedName),
                    name: entry.name ?? qualifiedName,
                    qualifiedName,
                    kind: 'unknown',
                    uri: entry.file ? fileUri(workspaceRoot, entry.file) : '',
                    range: graphRange(entry.line, entry.line),
                    projectName: project
                },
                hop: 1,
                via,
                ...(entry.isTest === undefined ? {} : { isTest: entry.isTest }),
                ...(entry.construction === undefined ? {} : { construction: entry.construction }),
                enriched: true
            });
        }
        return out;
    }

    /** Files and construction flags for what the root calls, or undefined when nobody could look. */
    protected async outgoingHopFacts(
        project: string,
        qualifiedName: string
    ): Promise<Map<string, HopFacts> | undefined> {
        try {
            const rows = await this.client.queryRows(project, callsOut(qualifiedName));
            const classRows = await this.client.queryRows(project, classCallTargets(qualifiedName));
            const constructed = new Set(classRows.map(row => row[COLUMNS.classTargets[1]]).filter(Boolean));
            const facts = new Map<string, HopFacts>();
            for (const row of rows) {
                const target = row[COLUMNS.callsOut[1]];
                if (!target) {
                    continue;
                }
                facts.set(target, {
                    name: row[COLUMNS.callsOut[0]] || undefined,
                    file: row[COLUMNS.callsOut[2]] || undefined,
                    line: toOptionalNumber(row[COLUMNS.callsOut[3]]),
                    construction: constructed.has(target)
                });
            }
            return facts;
        } catch {
            return undefined;
        }
    }

    /** Files and test flags for what calls the root, or undefined when nobody could look. */
    protected async incomingHopFacts(
        project: string,
        qualifiedName: string
    ): Promise<Map<string, HopFacts> | undefined> {
        try {
            const rows = await this.client.queryRows(project, callsIn(qualifiedName));
            const facts = new Map<string, HopFacts>();
            for (const row of rows) {
                const source = row[COLUMNS.callsIn[1]];
                if (!source) {
                    continue;
                }
                facts.set(source, {
                    name: row[COLUMNS.callsIn[0]] || undefined,
                    file: row[COLUMNS.callsIn[2]] || undefined,
                    line: toOptionalNumber(row[COLUMNS.callsIn[3]]),
                    isTest: toBoolean(row[COLUMNS.callsIn[4]])
                });
            }
            return facts;
        } catch {
            return undefined;
        }
    }

    /**
     * The whole project, in one answer.
     *
     * Three readings rather than one, because the engine's summary alone would
     * be a map with two blanks on it.
     *
     *  - The summary itself: packages, layers, boundaries, clusters, the
     *    entry points it flagged, and the routes it recovered for the languages
     *    where it recovers any.
     *  - The hotspot sweep. The summary ranks hotspots by fan-in alone, which
     *    puts a trivial helper that everything calls above the one function
     *    with three nested loops in it. The complexity readings live on the
     *    nodes, so they are swept once here and ranked in a pure function.
     *  - The route scan, for the languages whose analysis writes no routes at
     *    all. See route-reader.ts for why that is a gap worth filling and for
     *    the rules that keep filling it honest.
     */
    async architectureOverview(root: string, opts?: ProviderQueryOptions): Promise<ArchitectureOverviewDto> {
        const project = await this.projectFor(root, opts);
        if (!project) {
            return emptyOverview();
        }
        const result = await this.client.getArchitecture(project);
        const files = await this.indexedFilesOf(project, result);
        const sweep = await this.sweepCallables(project);
        return {
            projectName: result.project,
            totalSymbols: result.total_nodes ?? 0,
            totalRelations: result.total_edges ?? 0,
            symbolKinds: result.node_labels.map(entry => ({ kind: entry.label, count: entry.count ?? 0 })),
            relationKinds: result.edge_types.map(entry => ({ kind: entry.type, count: entry.count ?? 0 })),
            languages: result.languages.map(entry => ({ language: entry.language, fileCount: entry.file_count ?? 0 })),
            groups: result.packages.map(entry => ({
                name: entry.name,
                symbolCount: entry.node_count ?? 0,
                fanIn: entry.fan_in ?? 0,
                fanOut: entry.fan_out ?? 0
            })),
            // The summary names an entry point and the file it lives in, and
            // stops there. Without a line the product can open the file but not
            // the symbol, so the declaration line is taken from the sweep that
            // has already been paid for.
            entryPoints: result.entry_points.map(entry => ({
                name: entry.name,
                qualifiedName: entry.qualified_name,
                kind: 'unknown' as CodeAtlasSymbolKind,
                filePath: entry.file ?? sweep.get(entry.qualified_name ?? '')?.filePath,
                line: sweep.get(entry.qualified_name ?? '')?.line
            })),
            routes: this.routesOf(result, root, files),
            clusters: result.clusters.map((entry, index) => ({
                id: String(entry.id ?? index),
                label: entry.label ?? '',
                memberCount: entry.members ?? entry.top_nodes.length,
                cohesion: entry.cohesion,
                topMembers: entry.top_nodes
            })),
            layers: result.layers
                .filter(entry => (entry.layer ?? '').length > 0)
                .map(entry => ({
                    group: entry.name ?? '',
                    layer: entry.layer ?? '',
                    reason: entry.reason
                })),
            boundaries: result.boundaries
                .filter(entry => (entry.from ?? '').length > 0 && (entry.to ?? '').length > 0)
                .map(entry => ({ from: entry.from!, to: entry.to!, callCount: entry.call_count ?? 0 })),
            hotspots: this.hotspotsOf(result, sweep),
            files
        };
    }

    /**
     * Which file depends on which, in one sweep.
     *
     * The engine writes an import edge from the importing module to the imported
     * file, and both ends carry `file_path`, so a file-level dependency graph is
     * one query and no per-node lookups. Rows that name a file at either end the
     * index has no path for are dropped rather than carried as an empty string:
     * an edge to nowhere would make a file look like it depends on the project
     * root.
     *
     * A refused query answers with an empty graph rather than throwing, for the
     * same reason the route scan swallows an unreadable file: the caller is
     * building an onboarding artefact, and a language whose analysis records no
     * imports must produce a shorter answer rather than an error page. The
     * emptiness is not a claim, and {@link ModuleDependencyGraph} says so.
     */
    async moduleDependencies(root: string, opts?: ProviderQueryOptions): Promise<ModuleDependencyGraph> {
        const project = await this.projectFor(root, opts);
        if (!project) {
            return { edges: [], truncated: false };
        }
        let rows: Record<string, string>[];
        try {
            rows = await this.client.queryRows(project, importEdges());
        } catch (error) {
            if (error instanceof EngineError || error instanceof EngineUnavailableError) {
                return { edges: [], truncated: false };
            }
            throw error;
        }
        const edges: ModuleDependency[] = [];
        for (const row of rows) {
            const from = row[COLUMNS.imports[0]];
            const to = row[COLUMNS.imports[1]];
            if (!from || !to) {
                continue;
            }
            edges.push({ from, to });
        }
        return { edges, truncated: rows.length >= IMPORT_EDGE_LIMIT };
    }

    /**
     * The files the index holds, workspace-relative and in a stable order.
     *
     * Read from the file nodes rather than from the summary's file tree. The
     * tree is shallow: on the Java project it names six configuration files and
     * none of the hundred and thirty sources, so a route scan driven by it
     * would report "no routes found" after opening nothing. The tree is still
     * the fallback for an engine that answers the listing with a refusal.
     */
    protected async indexedFilesOf(
        project: string,
        result: { file_tree: { path: string; type?: string }[] }
    ): Promise<string[]> {
        try {
            const rows = await this.client.queryRows(project, indexedFiles());
            const paths = rows
                .map(row => row[COLUMNS.files[0]])
                .filter(path => typeof path === 'string' && path.length > 0);
            if (paths.length > 0) {
                return [...new Set(paths)].sort();
            }
        } catch (error) {
            if (!(error instanceof EngineError)) {
                throw error;
            }
        }
        return result.file_tree
            .filter(entry => entry.type !== 'dir' && entry.path.length > 0)
            .map(entry => entry.path)
            .sort();
    }

    /**
     * Routes the index reported, plus the ones only the source knows about.
     *
     * Merged rather than chosen between: a Java project gets paths from the
     * index that carry no file, and a TypeScript project gets nothing at all
     * from the index. Deduplication is on method and path, and the source scan
     * wins a tie because it is the reading that knows where the registration is
     * written, which is what a reader clicks on.
     */
    protected routesOf(
        result: { routes: { method?: string; path?: string; handler?: string; file?: string; file_path?: string; line?: number }[] },
        root: string,
        files: string[]
    ): RouteRef[] {
        const scanned = this.scanRoutes(root, files);
        const seen = new Set(scanned.map(route => `${route.method ?? ''} ${route.path}`));
        const fromIndex: RouteRef[] = [];
        for (const entry of result.routes) {
            const path = entry.path ?? '';
            if (path.length === 0 || seen.has(`${entry.method ?? ''} ${path}`)) {
                continue;
            }
            seen.add(`${entry.method ?? ''} ${path}`);
            fromIndex.push({
                method: entry.method || undefined,
                path,
                filePath: entry.file_path || entry.file || undefined,
                line: entry.line,
                handler: entry.handler || undefined,
                origin: 'index'
            });
        }
        return [...scanned, ...fromIndex].slice(0, MAX_ROUTES);
    }

    /**
     * Read the workspace's own files looking for route registrations.
     *
     * Bounded by the number of files, because this runs on the caret's path for
     * a workspace of any size and a generated bundle is not a file anybody
     * registers a route in. A file that cannot be read is skipped in silence:
     * the index listed it, so its absence is a race with the user's editor and
     * not a finding.
     *
     * With no reader injected there is no scan, and that is the browser's
     * ordinary state: nothing there can open a workspace file. The result is an
     * honest shortfall rather than a wrong answer, and `ProviderCapabilities`
     * is where a surface reads that a route list can be incomplete.
     */
    protected scanRoutes(root: string, files: string[]): RouteRef[] {
        const read = this.options.readSource;
        if (read === undefined) {
            return [];
        }
        const out: RouteRef[] = [];
        let scanned = 0;
        for (const filePath of files) {
            if (scanned >= MAX_ROUTE_SCAN_FILES || out.length >= MAX_ROUTES) {
                break;
            }
            if (!isRouteSource(filePath)) {
                continue;
            }
            scanned += 1;
            const source = read(joinPath(root, filePath));
            if (source === undefined) {
                continue;
            }
            for (const route of readRoutes(source, filePath)) {
                out.push({ ...route, filePath, origin: 'source' });
            }
        }
        return out;
    }

    /**
     * The symbols worth reading first, with the readings that say so.
     *
     * The engine's own hotspot list carries one signal, fan-in, and it is a
     * real one: a function twenty other symbols reach is worth knowing about.
     * It is not the only one, and on a small project it is the weakest: the
     * fixture's most expensive function is called by nothing and would never
     * appear. So both are read, the complexity signals decide the order, and
     * fan-in is folded in as one more signal on the symbols that have it.
     */
    /**
     * Every callable the index holds, with its declaration line and its
     * complexity readings, keyed by qualified name.
     *
     * One sweep serving two answers. The readings are what makes the hotspot
     * list something other than a fan-in ranking, and the declaration lines are
     * what lets an entry point be opened at the symbol rather than at the top
     * of its file. Both are on the same rows, so asking twice would be paying
     * twice for one traversal.
     */
    protected async sweepCallables(project: string): Promise<Map<string, ArchitectureHotspot>> {
        const readings = new Map<string, ArchitectureHotspot>();
        for (const label of CALLABLE_LABELS) {
            let rows: Record<string, string>[];
            try {
                rows = await this.client.queryRows(project, hotspotCandidates(label));
            } catch (error) {
                if (!(error instanceof EngineError)) {
                    throw error;
                }
                // A label the index does not hold is not a reason to lose the
                // other: a project with no methods at all is ordinary.
                continue;
            }
            for (const row of rows) {
                const qualifiedName = row[COLUMNS.hotspots[1]] || undefined;
                const key = qualifiedName ?? row[COLUMNS.hotspots[0]];
                if (key.length === 0 || readings.has(key)) {
                    continue;
                }
                readings.set(key, {
                    name: row[COLUMNS.hotspots[0]] ?? '',
                    qualifiedName,
                    filePath: row[COLUMNS.hotspots[2]] || undefined,
                    line: toOptionalNumber(row[COLUMNS.hotspots[3]]),
                    complexity: toOptionalNumber(row[COLUMNS.hotspots[4]]),
                    cognitive: toOptionalNumber(row[COLUMNS.hotspots[5]]),
                    loopDepth: toOptionalNumber(row[COLUMNS.hotspots[6]]),
                    // Counts, not flags: the index writes how many allocations
                    // and scans sit inside a loop, and one is already a finding.
                    allocationInLoop: (toOptionalNumber(row[COLUMNS.hotspots[7]]) ?? 0) > 0,
                    scanInLoop: (toOptionalNumber(row[COLUMNS.hotspots[8]]) ?? 0) > 0,
                    unguardedRecursion: toBoolean(row[COLUMNS.hotspots[9]])
                });
            }
        }
        return readings;
    }

    protected hotspotsOf(
        result: { hotspots: { name: string; qualified_name?: string; file_path?: string; fan_in?: number }[] },
        sweep: Map<string, ArchitectureHotspot>
    ): ArchitectureHotspot[] {
        const candidates = new Map<string, ArchitectureHotspot>(sweep);
        for (const entry of result.hotspots) {
            const key = entry.qualified_name ?? entry.name;
            const known = candidates.get(key);
            candidates.set(key, {
                // Whatever the summary ranked and the sweep did not reach still
                // belongs on the map: dropping it would lose a finding to a row
                // limit rather than to a reading.
                ...(known ?? {
                    name: entry.name,
                    qualifiedName: entry.qualified_name,
                    filePath: entry.file_path
                }),
                fanIn: entry.fan_in
            });
        }
        return rankHotspots([...candidates.values()]).slice(0, MAX_HOTSPOTS);
    }

    /**
     * What a change reaches, in three readings rather than one.
     *
     * The change tool alone is not an impact assessment, and pretending it is
     * was the single largest gap between what this method used to return and
     * what a reader needs. Measured against the real 0.9.0 answer:
     *
     *  - It names the symbols that sit **inside** the changed files and nothing
     *    further out. Asking for `depth: 3` returns the same rows as asking for
     *    nothing, so the "downstream" half of the question is not answered at
     *    all. That walk is done here, one query per step, against the same
     *    invocation relation the twin and the navigator read.
     *  - It names those symbols by display name and file only: no qualified
     *    name, no line. So they cannot be opened, cannot be asked a follow-up
     *    question about, and cannot be told apart from a same-named symbol in
     *    another file. The identities are recovered from the index.
     *  - It repeats both lists once per repeated changed file.
     *
     * The server of PR 1860 answers the other way round, and the difference is
     * decided per row by the `hop` column: `impacted` holds the transitive
     * CALLERS of the changed declarations, each with its distance, and the
     * changed declarations themselves are reported only as a number
     * (`seed_symbols`), scoped to the changed hunks. So a row with a step is
     * read as a caller at that step and the walk below is left to the server,
     * and the changed symbols are recovered from the index. Which of the two
     * answers arrived is never assumed; it is read off the row.
     *
     * That recovery is the one thing the 0.9.0 reading deliberately refused,
     * and it is refused here too wherever the tool did name declarations: an
     * empty symbol list from a non-empty file list is a finding, and it means
     * the index does not hold those files. It is only done when the tool names
     * a *count* and no names, because then the index is the only place those
     * names exist and answering "no changed symbol" would report a gap in the
     * answer as a fact about the change. What comes back is then exactly "every
     * symbol the index places inside a changed file", which is what
     * `changeKind: 'declared'` has always meant, and never "every symbol that
     * changed": the seed count travels with it so a surface can say both.
     */
    async changeImpact(root: string, sinceRef?: string, opts?: ProviderQueryOptions): Promise<ChangeImpactDto> {
        const project = await this.projectFor(root, opts);
        if (!project) {
            return { baselineRef: sinceRef, changedFiles: [], impacted: [], symbols: [] };
        }
        const result = await this.client.detectChanges(project, sinceRef, { depth: IMPACT_WALK_DISTANCE });
        const changedFiles = [...new Set(result.changed_files.filter(path => path.length > 0 && !isCodeAtlasState(path)))]
            .sort();
        const reported = dedupeDetected(result.impacted_symbols)
            .filter(entry => !isCodeAtlasState(entry.file ?? entry.file_path ?? ''));

        /*
         * Two readings of `impacted`, decided by the `hop` column.
         *
         * Measured against the built server of PR 1860: `impacted` holds the
         * transitive CALLERS of the changed declarations, each with the step at
         * which it was reached, and the changed declarations themselves are not
         * in the list at all (only their number, as `seed_symbols`). The 0.9.0
         * tool this provider was written against reported the other thing: the
         * symbols inside the changed files, with no distance. Both answers
         * arrive under one key, and the column is what tells them apart.
         *
         * So a row that names a step is a caller at that step, and a row that
         * names none is read the way 0.9.0 meant it. Reading the new answer the
         * old way would print a route handler under the heading "changed
         * symbols", which is a claim about somebody's diff that their diff does
         * not make.
         */
        const callerRows = reported.filter(entry => (entry.hop ?? 0) >= 1);
        const declaredRows = reported.filter(entry => (entry.hop ?? 0) < 1);

        // The files to look declarations up in. Every changed file when the
        // tool named no declaration of its own, because then the index is the
        // only place their names are; otherwise the files it did name, which is
        // the reading 0.9.0 needed to recover identities.
        const reportedFiles = [...new Set([
            ...declaredRows.map(entry => entry.file ?? entry.file_path ?? ''),
            ...callerRows.map(entry => entry.file ?? entry.file_path ?? ''),
            ...(declaredRows.length === 0 ? changedFiles : []),
        ])]
            .filter(path => path.length > 0)
            .slice(0, MAX_CHANGED_FILES_READ);
        const declarations = await this.declarationsOf(project, reportedFiles);

        /*
         * The changed symbols, when the tool named none.
         *
         * This is the sweep the 0.9.0 reading deliberately refused, and the
         * refusal's reason no longer holds: there it meant "the index does not
         * hold those files", because the tool would have named the symbols if
         * it could. Here the tool names their number and never their names, so
         * the index is the only place they exist, and answering "no changed
         * symbol" would be reporting a gap in the answer as a fact about the
         * change.
         *
         * What this list is, exactly: every symbol the index places inside a
         * changed file. It is NOT "every symbol that changed" and nothing in
         * the product says it is; the tool scoped its own seeds to the changed
         * hunks and reports how many, which is why {@link ChangeImpactDto.seedSymbols}
         * is carried alongside for a surface to say so.
         */
        const declared: ChangeImpactSymbol[] = declaredRows.length > 0
            ? declaredRows.map(entry => this.directSymbol(entry, declarations))
            : declarationsIn(changedFiles, declarations);

        const symbols: ChangeImpactSymbol[] = [...declared];
        const seen = new Set(symbols.map(symbol => symbol.qualifiedName ?? '').filter(name => name.length > 0));
        let reportedDistance = 0;
        for (const entry of callerRows) {
            const symbol = this.callerSymbol(entry, declarations);
            if (symbol.qualifiedName !== undefined && seen.has(symbol.qualifiedName)) {
                continue;
            }
            if (symbol.qualifiedName !== undefined) {
                seen.add(symbol.qualifiedName);
            }
            reportedDistance = Math.max(reportedDistance, symbol.distance);
            symbols.push(symbol);
        }

        /*
         * The provider's own caller walk runs only when the tool did none.
         *
         * With the hop column present the server has already walked, and
         * walking again from a frontier that holds symbols at step two would
         * label their callers "one step out". One walk or the other, never
         * both halves of two.
         */
        const walk = callerRows.length > 0
            ? { symbols: [] as ChangeImpactSymbol[], distance: reportedDistance, truncated: result.truncated === true }
            : await this.walkCallers(project, symbols);
        symbols.push(...walk.symbols);

        return {
            baselineRef: sinceRef,
            changedFiles,
            depth: result.depth,
            symbols,
            seedSymbols: result.seed_symbols,
            walkedDistance: Math.max(reportedDistance, walk.distance),
            truncated: walk.truncated,
            impacted: symbols.map(symbol => ({
                name: symbol.name,
                qualifiedName: symbol.qualifiedName,
                kind: symbol.kind,
                filePath: symbol.filePath,
                line: symbol.line,
                isTest: symbol.isTest
            })),
            raw: result as unknown as Record<string, unknown>
        };
    }

    /**
     * One reported symbol, with whatever identity the index can give it back.
     *
     * A row the index cannot match is kept rather than dropped. The engine
     * looked at the diff and said this symbol is in it; that is a finding, and
     * losing it because a follow-up lookup came back empty would report fewer
     * changes than were found. What such a row loses is everything that needs a
     * qualified name: it cannot be walked from and it cannot be measured, and
     * both of those absences are visible to the caller.
     */
    /**
     * One reported caller, at the step the tool reached it.
     *
     * The same identity recovery as {@link directSymbol} and one different
     * claim: this symbol was not changed, it calls something that was. The
     * distance is the tool's own, never recomputed, because the tool is the one
     * that walked.
     */
    protected callerSymbol(entry: DetectedSymbol, declarations: Map<string, DeclarationRow>): ChangeImpactSymbol {
        const direct = this.directSymbol(entry, declarations);
        return { ...direct, changeKind: 'caller', distance: Math.max(1, entry.hop ?? 1) };
    }

    protected directSymbol(entry: DetectedSymbol, declarations: Map<string, DeclarationRow>): ChangeImpactSymbol {
        const filePath = entry.file || entry.file_path || undefined;
        const name = entry.name ?? entry.qualified_name ?? '';
        const isModule = (entry.label ?? '').toLowerCase() === 'module' || name === filePath;
        const match = isModule ? undefined : declarations.get(declarationKey(filePath, name));
        return {
            name,
            qualifiedName: entry.qualified_name || match?.qualifiedName,
            filePath,
            line: match?.line,
            kind: symbolKindOf(entry.label),
            changeKind: isModule ? 'module' : 'declared',
            distance: 0,
            ...(match?.isTest === undefined ? {} : { isTest: match.isTest })
        };
    }

    /**
     * Every symbol declared in one of a set of files, keyed by file and name.
     *
     * Two queries for any number of files: one per callable label, each with a
     * disjunction over the paths. A label the index does not hold is skipped in
     * silence, exactly as the hotspot sweep skips it, because a project with no
     * methods is ordinary rather than broken.
     */
    protected async declarationsOf(project: string, filePaths: string[]): Promise<Map<string, DeclarationRow>> {
        const rows = new Map<string, DeclarationRow>();
        if (filePaths.length === 0) {
            return rows;
        }
        for (const label of CALLABLE_LABELS) {
            const query = declarationsInFiles(label, filePaths);
            if (query === undefined) {
                continue;
            }
            let answered: Record<string, string>[];
            try {
                answered = await this.client.queryRows(project, query);
            } catch (error) {
                if (!(error instanceof EngineError)) {
                    throw error;
                }
                continue;
            }
            for (const row of answered) {
                const name = row[COLUMNS.declarations[0]] ?? '';
                const filePath = row[COLUMNS.declarations[2]] || undefined;
                const key = declarationKey(filePath, name);
                if (name.length === 0 || rows.has(key)) {
                    continue;
                }
                rows.set(key, {
                    qualifiedName: row[COLUMNS.declarations[1]] || undefined,
                    line: toOptionalNumber(row[COLUMNS.declarations[3]]),
                    isTest: toBoolean(row[COLUMNS.declarations[5]])
                });
            }
        }
        return rows;
    }

    /**
     * The callers of a change set, level by level.
     *
     * Breadth first and one query per level, never one per symbol: the set at
     * step two is the set at step one's neighbours, and asking the relation for
     * a whole level at once is the same traversal the engine would do anyway.
     * The walk stops at {@link IMPACT_WALK_DISTANCE}, at
     * {@link MAX_IMPACT_SYMBOLS}, or when a level reaches nothing new, and it
     * says which of the first two happened so a surface can tell a complete
     * answer from a bounded one.
     *
     * A refused query ends the walk without ending the call. The direct half of
     * the answer is already correct at that point, and a panel that showed
     * nothing because the second reading failed would be hiding the first.
     */
    protected async walkCallers(
        project: string,
        direct: ChangeImpactSymbol[]
    ): Promise<{ symbols: ChangeImpactSymbol[]; distance: number; truncated: boolean }> {
        const seen = new Set<string>();
        for (const symbol of direct) {
            if (symbol.qualifiedName) {
                seen.add(symbol.qualifiedName);
            }
        }
        const symbols: ChangeImpactSymbol[] = [];
        let frontier = [...seen];
        let distance = 0;
        let truncated = false;

        for (let step = 1; step <= IMPACT_WALK_DISTANCE && frontier.length > 0 && !truncated; step++) {
            const query = callersOfAny(frontier);
            if (query === undefined) {
                break;
            }
            let rows: Record<string, string>[];
            try {
                rows = await this.client.queryRows(project, query);
            } catch (error) {
                if (!(error instanceof EngineError) && !(error instanceof EngineUnavailableError)) {
                    throw error;
                }
                break;
            }
            const next: string[] = [];
            for (const row of rows) {
                const qualifiedName = row[COLUMNS.callsIn[1]];
                if (!qualifiedName || seen.has(qualifiedName)) {
                    continue;
                }
                if (symbols.length >= MAX_IMPACT_SYMBOLS) {
                    truncated = true;
                    break;
                }
                seen.add(qualifiedName);
                next.push(qualifiedName);
                symbols.push({
                    name: row[COLUMNS.callsIn[0]] || qualifiedName,
                    qualifiedName,
                    filePath: row[COLUMNS.callsIn[2]] || undefined,
                    line: toOptionalNumber(row[COLUMNS.callsIn[3]]),
                    kind: symbolKindOf(inferLabelFromRow(row)),
                    changeKind: 'caller',
                    distance: step,
                    isTest: toBoolean(row[COLUMNS.callsIn[4]])
                });
            }
            distance = step;
            // A level that reached nothing new is the end of the graph, not a
            // bound, so the truncation flag stays where it is.
            frontier = next;
        }
        return { symbols, distance, truncated: truncated || frontier.length > 0 };
    }

    /**
     * The complexity readings of a named set of symbols.
     *
     * One query per label per batch, over the same columns the hotspot sweep
     * reads. Reusing those columns is the point: the map's "read these first"
     * list and an impact assessment's risk chips must never disagree about the
     * same function, and the only way to guarantee that is for both to be the
     * same reading of the same properties.
     */
    async getComplexity(
        root: string,
        qualifiedNames: string[],
        opts?: ProviderQueryOptions
    ): Promise<SymbolComplexity[]> {
        const wanted = [...new Set(qualifiedNames.filter(name => name.length > 0))];
        if (wanted.length === 0) {
            return [];
        }
        const project = await this.projectFor(root, opts);
        if (!project) {
            return [];
        }
        const readings = new Map<string, SymbolComplexity>();
        for (let at = 0; at < wanted.length; at += COMPLEXITY_BATCH_LIMIT) {
            const batch = wanted.slice(at, at + COMPLEXITY_BATCH_LIMIT);
            for (const label of CALLABLE_LABELS) {
                const query = complexityOf(label, batch);
                if (query === undefined) {
                    continue;
                }
                let rows: Record<string, string>[];
                try {
                    rows = await this.client.queryRows(project, query);
                } catch (error) {
                    if (!(error instanceof EngineError)) {
                        throw error;
                    }
                    continue;
                }
                for (const row of rows) {
                    const qualifiedName = row[COLUMNS.hotspots[1]] || undefined;
                    if (!qualifiedName || readings.has(qualifiedName)) {
                        continue;
                    }
                    readings.set(qualifiedName, {
                        name: row[COLUMNS.hotspots[0]] ?? '',
                        qualifiedName,
                        filePath: row[COLUMNS.hotspots[2]] || undefined,
                        line: toOptionalNumber(row[COLUMNS.hotspots[3]]),
                        complexity: toOptionalNumber(row[COLUMNS.hotspots[4]]),
                        cognitive: toOptionalNumber(row[COLUMNS.hotspots[5]]),
                        loopDepth: toOptionalNumber(row[COLUMNS.hotspots[6]]),
                        allocationInLoop: (toOptionalNumber(row[COLUMNS.hotspots[7]]) ?? 0) > 0,
                        scanInLoop: (toOptionalNumber(row[COLUMNS.hotspots[8]]) ?? 0) > 0,
                        unguardedRecursion: toBoolean(row[COLUMNS.hotspots[9]])
                    });
                }
            }
        }
        // Answered in the order asked, so a caller zipping the answer against
        // its request does not have to sort first.
        return wanted.map(name => readings.get(name)).filter((entry): entry is SymbolComplexity => entry !== undefined);
    }

    /**
     * Symbole suchen.
     *
     * `signal` ist die eine Zutat, die seit W7b ueber den Protokolltyp
     * hinausgeht, und sie steht hier statt in ProviderQueryOptions, weil sie
     * nur diese eine Methode betrifft: die Suche ist der einzige Aufruf, den
     * ein Tastendruck ueberholt, bevor er beantwortet ist. Wer sie nicht
     * mitgibt, bekommt das Verhalten von vorher.
     */
    async searchSymbols(
        root: string,
        pattern: string,
        limit?: number,
        opts?: ProviderQueryOptions & { signal?: AbortSignal }
    ): Promise<SymbolSearchHit[]> {
        const project = await this.projectFor(root, opts);
        if (!project) {
            return [];
        }
        const result = await this.client.searchGraph(project, {
            namePattern: pattern,
            limit,
            ...(opts?.signal === undefined ? {} : { signal: opts.signal })
        });
        return result.results.map(hit => ({
            name: hit.name,
            qualifiedName: hit.qualified_name,
            kind: symbolKindOf(hit.label),
            filePath: hit.file_path,
            line: hit.start_line,
            isTest: hit.is_test,
            isExported: hit.is_exported
        }));
    }

    /**
     * Where the index says one named symbol is declared.
     *
     * Reuses the declaration read the change assessment already makes, over a
     * single file: one query per callable label, both of them a lookup on
     * `file_path`. Nothing is cached, for the reason nothing else here is
     * either: the answer moves when the index moves, and the caller asking it is
     * about to open the file.
     */
    async declarationLineOf(
        root: string,
        filePath: string,
        name: string,
        opts?: ProviderQueryOptions
    ): Promise<number | undefined> {
        const project = await this.projectFor(root, opts);
        if (!project || filePath.length === 0 || name.length === 0) {
            return undefined;
        }
        const rows = await this.declarationsOf(project, [filePath]);
        return rows.get(declarationKey(filePath, name))?.line;
    }

    async getSnippet(root: string, qualifiedName: string, opts?: ProviderQueryOptions): Promise<string> {
        const project = await this.projectFor(root, opts);
        if (!project) {
            return '';
        }
        const result = await this.client.getCodeSnippet(project, qualifiedName);
        return result.source;
    }

    /**
     * Hand observed calls to the analysis backend, and say what it did.
     *
     * Deliberately not on the `IntelligenceProvider` interface. That interface
     * is the anti lock-in boundary and every optional ability on it is declared
     * in `capabilities()`; this one is declared false there, because the 0.9.0
     * engine builds nothing from what it accepts. Putting the handoff on the
     * interface anyway would invite a second provider to implement a method
     * whose result no surface may read.
     *
     * On this surface the handoff cannot be made at all: `ingest_traces` is one
     * of the three tools the read-only allowlist refuses. The attempt is still
     * made rather than skipped, so the sentence the reader gets names what the
     * server said instead of a guess this method made about it, and so the day
     * the allowlist grows the tool nothing here has to change.
     */
    async ingestRuntimeTraces(
        root: string,
        _events: readonly unknown[],
        opts?: ProviderQueryOptions
    ): Promise<string> {
        const project = await this.projectFor(root, opts).catch(() => undefined);
        if (!project) {
            return 'the analysis backend has no project for this workspace yet, so the events were kept locally only.';
        }
        try {
            await this.client.ingestTraces();
            return 'the analysis backend accepted the events.';
        } catch (error) {
            return `the analysis backend did not take the events: ${String(error)}`;
        }
    }

    /** Drop everything the provider holds about a workspace. Also not on /rpc. */
    async deleteProject(_root: string, _opts?: ProviderQueryOptions): Promise<void> {
        return this.client.deleteProject();
    }

    // Fact builders -------------------------------------------------------

    protected async calleesFact(
        project: string | undefined,
        qualifiedName: string | undefined,
        engineAvailable: boolean,
        indexed: boolean
    ): Promise<Fact<CallSite[]>> {
        if (!engineAvailable || !indexed) {
            return emptyFact<CallSite>(knowledgeStateFor({ engineAvailable, indexed, supported: true, derived: false }));
        }
        const rows = await this.client.queryRows(project!, callsOut(qualifiedName!));
        const classRows = await this.client.queryRows(project!, classCallTargets(qualifiedName!));
        const constructed = new Set(classRows.map(row => row[COLUMNS.classTargets[1]]).filter(Boolean));

        const value: CallSite[] = [];
        const evidence: Evidence[] = [];
        for (const row of rows) {
            const targetQualifiedName = row[COLUMNS.callsOut[1]] || undefined;
            const targetFile = row[COLUMNS.callsOut[2]] || undefined;
            const line = toOptionalNumber(row[COLUMNS.callsOut[4]]);
            value.push({
                targetName: row[COLUMNS.callsOut[0]] ?? '',
                targetQualifiedName,
                targetFile,
                line,
                // Where the target is declared, which is where following this
                // call leads. The call site line above is in the caller's file
                // and means nothing inside `targetFile`.
                targetLine: toOptionalNumber(row[COLUMNS.callsOut[3]]),
                strategy: targetQualifiedName && constructed.has(targetQualifiedName)
                    ? STRATEGIES.construction
                    : STRATEGIES.directCall
            });
            evidence.push(this.evidence(EVIDENCE_RELATIONS.invocation, targetFile, line));
        }
        return { value, state: 'known', evidence };
    }

    protected callersFact(
        rows: Record<string, string>[],
        engineAvailable: boolean,
        usable: boolean
    ): Fact<CallerRef[]> {
        if (!engineAvailable || !usable) {
            return emptyFact<CallerRef>(knowledgeStateFor({ engineAvailable, indexed: usable, supported: true, derived: false }));
        }
        const value: CallerRef[] = [];
        const evidence: Evidence[] = [];
        for (const row of rows) {
            const file = row[COLUMNS.callsIn[2]] || undefined;
            const line = toOptionalNumber(row[COLUMNS.callsIn[5]]);
            value.push({
                name: row[COLUMNS.callsIn[0]] ?? '',
                qualifiedName: row[COLUMNS.callsIn[1]] || undefined,
                file,
                line,
                isTest: toBoolean(row[COLUMNS.callsIn[4]]),
                sourceKind: sourceKindOf(inferLabelFromRow(row))
            });
            evidence.push(this.evidence(EVIDENCE_RELATIONS.invocation, file, line));
        }
        return { value, state: 'known', evidence };
    }

    /**
     * Tests are inferred, never read. The engine flags nodes that live in test
     * files; an invocation from such a node is the best available signal that
     * the callee is exercised by a test. The state stays `inferred` even when
     * the list is empty, because a heuristic that finds nothing has not proved
     * an absence.
     */
    protected testedByFact(
        rows: Record<string, string>[],
        engineAvailable: boolean,
        usable: boolean
    ): Fact<TestRef[]> {
        const state = knowledgeStateFor({ engineAvailable, indexed: usable, supported: true, derived: true });
        if (!engineAvailable || !usable) {
            return emptyFact<TestRef>(state);
        }
        const value: TestRef[] = [];
        const evidence: Evidence[] = [];
        for (const row of rows) {
            if (!toBoolean(row[COLUMNS.callsIn[4]])) {
                continue;
            }
            const file = row[COLUMNS.callsIn[2]] || undefined;
            const line = toOptionalNumber(row[COLUMNS.callsIn[5]]);
            value.push({
                name: row[COLUMNS.callsIn[0]] ?? '',
                file,
                line,
                kind: 'unit'
            });
            evidence.push({
                ...this.evidence(EVIDENCE_RELATIONS.invocation, file, line),
                source: 'test',
                strategy: STRATEGIES.testCaller
            });
        }
        return { value, state, evidence };
    }

    /**
     * The error types one symbol can produce, read from both error relations.
     *
     * Two queries and one answer. The engine names the same product fact twice:
     * `RAISES` for a `throw` statement in TypeScript, JavaScript and Python,
     * `THROWS` for a Java `throws` clause. Reading only the first made every
     * Java method answer "I looked and found nothing" next to a signature that
     * names the exception, which is the worst failure this product has: a
     * confident empty list.
     *
     * The two result sets are merged and deduplicated on the type, file and
     * line, because a symbol that both declares and raises the same exception
     * produces one fact, not two. The evidence keeps them apart: the reader can
     * still see which relation carried the finding.
     */
    protected async throwsFact(
        project: string | undefined,
        qualifiedName: string | undefined,
        engineAvailable: boolean,
        indexed: boolean,
        symbol: SymbolRef
    ): Promise<Fact<ThrowRef[]>> {
        if (!engineAvailable || !indexed) {
            return emptyFact<ThrowRef>(knowledgeStateFor({ engineAvailable, indexed, supported: true, derived: false }));
        }
        const sources: [string, string][] = [
            [EVIDENCE_RELATIONS.raise, raises(qualifiedName!)],
            [EVIDENCE_RELATIONS.throwDeclaration, throwsRelation(qualifiedName!)]
        ];
        const value: ThrowRef[] = [];
        const evidence: Evidence[] = [];
        const seen = new Set<string>();
        for (const [relation, query] of sources) {
            const rows = await this.client.queryRows(project!, query);
            for (const row of rows) {
                // Neither relation records the site line at 0.9.0, so the
                // declaration line of the symbol is the honest fallback.
                const file = row[COLUMNS.raises[1]] || undefined;
                const line = toOptionalNumber(row[COLUMNS.raises[3]]) ?? symbol.range.start.line + 1;
                const type = row[COLUMNS.raises[0]] ?? '';
                const key = `${type}|${file ?? ''}|${line}`;
                if (seen.has(key)) {
                    continue;
                }
                seen.add(key);
                value.push({ type, file, line });
                evidence.push(this.evidence(relation, file, toOptionalNumber(row[COLUMNS.raises[2]])));
            }
        }
        return { value, state: 'known', evidence };
    }

    protected async envReadsFact(
        project: string | undefined,
        qualifiedName: string | undefined,
        engineAvailable: boolean,
        indexed: boolean,
        symbol: SymbolRef
    ): Promise<Fact<DataRef[]>> {
        if (!engineAvailable || !indexed) {
            return emptyFact<DataRef>(knowledgeStateFor({ engineAvailable, indexed, supported: true, derived: false }));
        }
        const rows = await this.client.queryRows(project!, envReads(qualifiedName!));
        const value: DataRef[] = [];
        const evidence: Evidence[] = [];
        for (const row of rows) {
            const name = row[COLUMNS.envReads[1]] || row[COLUMNS.envReads[0]] || '';
            const file = row[COLUMNS.envReads[2]] || fileFromUri(symbol.uri);
            const line = toOptionalNumber(row[COLUMNS.envReads[3]]);
            value.push({ name, kind: 'global', file, line });
            evidence.push(this.evidence(EVIDENCE_RELATIONS.environmentRead, file, line));
        }
        return { value, state: 'known', evidence };
    }

    protected async typeRefsFact(
        project: string | undefined,
        qualifiedName: string | undefined,
        engineAvailable: boolean,
        indexed: boolean
    ): Promise<Fact<DataRef[]>> {
        if (!engineAvailable || !indexed) {
            return emptyFact<DataRef>(knowledgeStateFor({ engineAvailable, indexed, supported: true, derived: false }));
        }
        const rows = await this.client.queryRows(project!, typeRefs(qualifiedName!));
        const value: DataRef[] = [];
        const evidence: Evidence[] = [];
        for (const row of rows) {
            const file = row[COLUMNS.typeRefs[2]] || undefined;
            const line = toOptionalNumber(row[COLUMNS.typeRefs[4]]) ?? toOptionalNumber(row[COLUMNS.typeRefs[3]]);
            value.push({
                name: row[COLUMNS.typeRefs[0]] ?? '',
                kind: 'unknown',
                qualifiedName: row[COLUMNS.typeRefs[1]] || undefined,
                file,
                line
            });
            evidence.push(this.evidence(EVIDENCE_RELATIONS.typeReference, file, line));
        }
        return { value, state: 'known', evidence };
    }

    // Plumbing ------------------------------------------------------------

    protected evidence(relation: string, file: string | undefined, line: number | undefined): Evidence {
        return {
            source: 'graph-edge',
            relation,
            file,
            range: line !== undefined ? { startLine: line, endLine: line } : undefined,
            engineGeneration: this.generation(),
            providerId: this.id
        };
    }

    protected generation(): number {
        return this.options.generation ?? 0;
    }

    /**
     * Engine project name for a workspace root.
     *
     * Until the pinning file lands, an explicit name wins and otherwise the
     * project list is matched on root path. A workspace the engine has never
     * seen resolves to undefined, which every caller reads as "not indexed".
     */
    protected async projectFor(root: string, opts?: ProviderQueryOptions): Promise<string | undefined> {
        if (opts?.projectName) {
            return opts.projectName;
        }
        const key = normalizeRoot(root);
        const cached = this.projectNames.get(key);
        if (cached) {
            return cached;
        }
        let listed;
        try {
            listed = await this.client.listProjects();
        } catch (error) {
            if (error instanceof EngineError) {
                return undefined;
            }
            throw error;
        }
        const match = listed.projects.find(entry => entry.root_path && normalizeRoot(entry.root_path) === key);
        if (match) {
            this.projectNames.set(key, match.name);
            return match.name;
        }
        return undefined;
    }
}

function normalizeRoot(root: string): string {
    return root.replace(/\/+$/, '');
}

/**
 * Workspace root and a workspace-relative path, joined.
 *
 * `node:path` would be one import away and is the one import this file may not
 * have: everything here runs in a browser. The join is the whole of what was
 * used, and a POSIX-shaped one is correct for what it joins, since the paths
 * come from the index and the index writes them with forward slashes.
 */
function joinPath(root: string, relativePath: string): string {
    return `${normalizeRoot(root)}/${relativePath.replace(/^\/+/, '')}`;
}

function fileFromUri(uri: string): string | undefined {
    return uri.length > 0 ? uri : undefined;
}

/**
 * The engine returns rows, not labels, for incoming invocations. A source
 * whose qualified name ends in the file marker is module level code, which is
 * where test runners register their cases; anything else is left unclassified
 * rather than guessed.
 */
function inferLabelFromRow(row: Record<string, string>): string | undefined {
    const qualifiedName = row[COLUMNS.callsIn[1]] ?? '';
    const name = row[COLUMNS.callsIn[0]] ?? '';
    if (qualifiedName.endsWith('.__file__') || name.includes('/')) {
        return 'module';
    }
    return undefined;
}

function emptyFact<T>(state: KnowledgeState): Fact<T[]> {
    return { value: [], state, evidence: [] };
}

/**
 * Rewrite the generation on every piece of evidence in a fact bundle.
 *
 * Exported because it is the whole of the per-call generation contract and a
 * table test is a better proof of it than a mock provider would be. Pure: the
 * input bundle is never mutated, so a cached bundle cannot be re-stamped by
 * accident.
 */
export function stampGeneration(facts: SymbolFacts, generation: number): SymbolFacts {
    const stampFact = <T>(fact: Fact<T> | undefined): Fact<T> | undefined => fact === undefined
        ? undefined
        : { ...fact, evidence: fact.evidence.map(entry => ({ ...entry, engineGeneration: generation })) };
    const stamped: SymbolFacts = {};
    for (const key of Object.keys(facts) as (keyof SymbolFacts)[]) {
        const fact = stampFact(facts[key] as Fact<unknown> | undefined);
        if (fact !== undefined) {
            // The index signature of SymbolFacts is per-family typed; the cast
            // is safe because stampFact only replaces the evidence array.
            (stamped as Record<string, unknown>)[key] = fact;
        }
    }
    return stamped;
}

function indexStateOf(status: string | undefined): IndexState {
    switch (status) {
        case 'indexed':
        case 'ready':
            return 'ready';
        case 'indexing':
            return 'indexing';
        case 'stale':
            return 'stale';
        case 'failed':
        case 'error':
            return 'failed';
        default:
            return 'absent';
    }
}

function emptyOverview(): ArchitectureOverviewDto {
    return {
        totalSymbols: 0,
        totalRelations: 0,
        symbolKinds: [],
        relationKinds: [],
        languages: [],
        groups: [],
        entryPoints: [],
        routes: [],
        clusters: [],
        layers: [],
        boundaries: [],
        hotspots: [],
        files: []
    };
}

/**
 * How urgently one symbol asks to be read first.
 *
 * The weights are a reading order and never a verdict. Nesting counts for more
 * than raw complexity because depth is what turns a loop into a cost that
 * follows the data size; recursion with no visible base case counts for most
 * because it is the one signal here that describes a defect rather than an
 * expense. Fan-in is folded in last and lightly: something everything calls is
 * worth knowing about, but a two-line helper reached forty times is not the
 * function anyone needs to read first.
 *
 * Exported so the weights can be read and argued with in one place instead of
 * being inferred from a rendered list.
 */
export function hotspotScore(hotspot: ArchitectureHotspot): number {
    return (hotspot.cognitive ?? 0)
        + (hotspot.complexity ?? 0)
        + 3 * (hotspot.loopDepth ?? 0)
        + 2 * (hotspot.allocationInLoop ? 1 : 0)
        + 2 * (hotspot.scanInLoop ? 1 : 0)
        + 8 * (hotspot.unguardedRecursion ? 1 : 0)
        + 0.25 * (hotspot.fanIn ?? 0);
}

/**
 * Rank hotspots, keeping only the ones that carry a signal at all.
 *
 * A symbol with every reading at zero is not a quiet hotspot, it is not a
 * hotspot; listing it would fill the section with the alphabetical head of the
 * codebase. Ties break on the name so two runs of the same index produce the
 * same list.
 */
export function rankHotspots(candidates: ArchitectureHotspot[]): ArchitectureHotspot[] {
    return candidates
        .filter(candidate => hotspotScore(candidate) > 0)
        .sort((left, right) => {
            const delta = hotspotScore(right) - hotspotScore(left);
            return delta !== 0 ? delta : (left.qualifiedName ?? left.name).localeCompare(right.qualifiedName ?? right.name);
        });
}
