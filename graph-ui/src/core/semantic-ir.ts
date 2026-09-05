/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-core/src/common/semantic-ir.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * die Ehrlichkeitsregeln des Referenzprojekts gelten hier 1:1, und ein
 * abgewandeltes Typsystem waere genau die stille Abweichung, die spaeter
 * niemand mehr findet. Aenderungen gegenueber dem Original: keine; der
 * Import von SymbolRef zeigt auf die ebenfalls portierte focus-protocol.ts
 * im selben Verzeichnis.
 */
/**
 * Semantic IR: the evidence-carrying description of one symbol.
 *
 * Every derived claim is wrapped in `Fact<T>` so the UI can render what is
 * known, what was inferred and what the engine could not answer. A claim
 * without evidence is not a fact; it is an omission, and its state says so.
 */

import { SymbolRef } from './focus-protocol';

/** How much trust a claim carries. Never collapse these into a boolean. */
export type KnowledgeState =
    /** Read directly from the engine graph. */
    | 'known'
    /** Derived by a heuristic that the evidence records. */
    | 'inferred'
    /** Several candidates matched and none dominates. */
    | 'ambiguous'
    /** The language or construct is out of scope for the analyzer. */
    | 'unsupported'
    /** The file or symbol is not in the index yet. */
    | 'notIndexed'
    /** No provider answered at all. */
    | 'unknown';

/** Where a piece of evidence came from. */
export type EvidenceSource =
    | 'graph-edge'
    | 'graph-node'
    /**
     * A reading of the workspace's own text, made by CodeAtlas rather than by
     * the analysis.
     *
     * The weakest citation this product emits and the one that has to say so.
     * It exists for findings the graph records only in part: an import edge, for
     * instance, names two files and neither the statement's line nor the symbol
     * it brought in, so those two are read off the file and cited as this. A
     * surface must never present one of these as a finding of the index.
     *
     * (Nachgetragen am 2026-08-29 mit dem Pseudocode-Port, W4c. Das Glied stand
     * im Original an dieser Stelle und fehlte hier, solange nichts in diesem
     * Projekt Quelltext gelesen hat; die Imports-Gruppe tut es.)
     */
    | 'source-text'
    | 'runtime-trace'
    | 'test'
    | 'git-history'
    | 'llm';

/** One citation backing a `Fact`. */
export interface Evidence {
    source: EvidenceSource;
    /** Graph relation name when `source` is a graph edge, for example `CALLS`. */
    relation?: string;
    /** Absolute URI or workspace-relative path of the supporting file. */
    file?: string;
    /** 1-based inclusive graph lines. Convert with positions.ts before display. */
    range?: { startLine: number; endLine: number };
    /** 0 to 1. Absent means the source is exact rather than scored. */
    confidence?: number;
    /** Identifier of the heuristic that produced an inferred claim. */
    strategy?: string;
    /**
     * How many times the source observed the thing it is citing.
     *
     * Only a counting source fills this in, which today means an imported
     * runtime trace: a graph edge is recorded once whether the call runs never
     * or a million times, so a count on one would be a number with no meaning.
     * Absent therefore says "this source does not count", not "observed zero
     * times", and a surface must not render it as the second.
     */
    observations?: number;
    /** Index generation this evidence was read from; stale evidence is discarded. */
    engineGeneration: number;
    /** Provider that emitted the evidence, for attribution and disablement. */
    providerId: string;
}

/** A claim plus its state and citations. */
export interface Fact<T> {
    value: T;
    state: KnowledgeState;
    evidence: Evidence[];
}

/** An outgoing call, resolved as far as the analyzer could take it. */
export interface CallSite {
    targetName: string;
    targetQualifiedName?: string;
    targetFile?: string;
    /** 1-based graph line of the call expression, inside the calling symbol's file. */
    line?: number;
    /**
     * 1-based graph line where the target is declared, inside `targetFile`.
     *
     * Separate from `line` on purpose: `line` says where the reader is now and
     * `targetLine` says where following the call would take them. Collapsing
     * the two would navigate to a call-site line number in the callee's file,
     * which lands somewhere plausible and wrong.
     */
    targetLine?: number;
    /** Rendered argument expressions, truncated by the provider. */
    args?: string[];
    /** 0 to 1 for unresolved or overloaded targets. */
    confidence?: number;
    /** Resolution heuristic used, for example `same-module` or `import-alias`. */
    strategy?: string;
}

/**
 * An outgoing call somebody watched happen, counted.
 *
 * Never merged with {@link CallSite}, and the separation is the point. A call
 * site is what the analyzer read in the source; this is what an imported trace
 * says ran. A symbol can have a call site nothing ever reached and an observed
 * call the analyzer cannot see, and a single list would make both of those
 * invisible.
 */
export interface RuntimeCall {
    /** Name of the called symbol, as the imported events spelled it. */
    targetName: string;
    /** Qualified name, when the events carried one. */
    targetQualifiedName?: string;
    /** Workspace-relative file of the call site, when an event recorded one. */
    targetFile?: string;
    /** 1-based line of the call site, when an event recorded one. */
    line?: number;
    /** How many imported events recorded this call. Never zero. */
    count: number;
    /**
     * True when the index records no call from this symbol to that target.
     *
     * A finding rather than a fault: it is the strongest thing a trace can say,
     * and the reason the wizard exists. False means the analyzer records the
     * same call, so the two agree.
     */
    unexpected: boolean;
}

/** An incoming call. */
export interface CallerRef {
    name: string;
    qualifiedName?: string;
    file?: string;
    /** 1-based graph line of the call expression. */
    line?: number;
    nodeId?: string;
    /** True when the caller is test code. Drives the tested-by inference. */
    isTest?: boolean;
    /**
     * What kind of construct the caller is. `module` covers file-level code,
     * which is where test runners register their cases.
     */
    sourceKind?: 'function' | 'method' | 'module';
}

/** State the symbol reads from or writes to. */
export interface DataRef {
    name: string;
    kind: 'field' | 'property' | 'variable' | 'parameter' | 'global' | 'store' | 'unknown';
    qualifiedName?: string;
    file?: string;
    /** 1-based graph line. */
    line?: number;
}

/** An error the symbol can raise. */
export interface ThrowRef {
    /** Exception or error type name as written in the source. */
    type: string;
    file?: string;
    /** 1-based graph line of the raise site. */
    line?: number;
    /** True when the raise is inside a conditional branch rather than unconditional. */
    conditional?: boolean;
}

/** An effect that crosses the process boundary. */
export interface ExternalEffect {
    kind: 'http-call' | 'exposes-route' | 'io-write';
    /** Target of the effect, for example `GET /users/:id` or a file path. */
    detail: string;
}

/** A test that exercises the symbol. */
export interface TestRef {
    name: string;
    file?: string;
    /** 1-based graph line of the test declaration. */
    line?: number;
    kind: 'unit' | 'integration' | 'e2e' | 'unknown';
}

/** Structural signals used to rank review effort and runtime risk. */
export interface ComplexityFlags {
    cyclomatic: number;
    cognitive: number;
    /** Deepest loop nesting inside this symbol alone. */
    loopDepth: number;
    /** Deepest loop nesting including loops in callees. */
    transitiveLoopDepth: number;
    /** A linear scan runs inside a loop, the classic accidental quadratic. */
    linearScanInLoop: boolean;
    allocInLoop: boolean;
    /** Recursive with no visible base case or depth bound. */
    unguardedRecursion: boolean;
    recursive: boolean;
    /** Reachable from a process entry point such as a route or CLI command. */
    isEntryPoint: boolean;
    isExported: boolean;
    routePath?: string;
    routeMethod?: string;
}

/** A named concern raised against the symbol. */
export interface Risk {
    id: string;
    severity: 'low' | 'medium' | 'high';
    /** Machine-readable family, for example `quadratic-scan` or `untested-entry-point`. */
    kind: string;
    /** One sentence, written for a reader who has not seen the code. */
    message: string;
}

/** The review dimension a checklist item belongs to. */
export type ChecklistCategory =
    | 'core-logic'
    | 'inputs'
    | 'implementations'
    | 'state'
    | 'callers'
    | 'error-handling'
    | 'config'
    | 'tests';

/** One thing the reader should confirm before claiming they understand the symbol. */
export interface ChecklistItem {
    id: string;
    category: ChecklistCategory;
    label: string;
    /** What to open when the item is activated. */
    target?: SymbolRef;
    done: boolean;
}

/** Everything CodeAtlas knows about one symbol at one index generation. */
export interface SemanticIR {
    /** Bump only on a breaking shape change; consumers reject unknown versions. */
    schemaVersion: 1;
    symbol: SymbolRef;
    /** Index generation this IR was built from. Stale IR is recomputed, not patched. */
    generation: number;
    /** Hash of the source snippet, so cached IR can be invalidated on edit. */
    snippetHash?: string;
    purpose: Fact<string>;
    signature?: Fact<string>;
    /** Ordered narrative of what the body does, as resolved call sites. */
    steps: Fact<CallSite[]>;
    calls: Fact<CallSite[]>;
    calledBy: Fact<CallerRef[]>;
    reads: Fact<DataRef[]>;
    writes: Fact<DataRef[]>;
    /**
     * Types the symbol names in its signature and body. Optional because a
     * provider that cannot recover type positions must omit the field rather
     * than report an empty list, which would read as "this symbol names no
     * types" instead of "nobody looked".
     */
    typeRefs?: Fact<DataRef[]>;
    /**
     * Calls out of this symbol that imported traces recorded, with counts.
     *
     * Optional for the same reason `typeRefs` is: a build that had no trace
     * store to read must omit the family rather than report an empty list, which
     * would read as "nothing ran here" instead of "nobody imported a recording".
     * Present and empty means the store was read and holds nothing about this
     * symbol, which is a different and equally honest answer.
     */
    runtime?: Fact<RuntimeCall[]>;
    throws: Fact<ThrowRef[]>;
    externalEffects: Fact<ExternalEffect[]>;
    tests: Fact<TestRef[]>;
    /** A fact, not a derived boolean: "no tests found" differs from "not indexed". */
    missingTests: Fact<boolean>;
    complexity: Fact<ComplexityFlags>;
    risks: Risk[];
    checklist: ChecklistItem[];
}
