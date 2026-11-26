/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-core/src/common/focus-protocol.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen,
 * weil semantic-ir.ts SymbolRef von hier bezieht und ein zweiter, leicht
 * abweichender Symbolbegriff die beiden Projekte still auseinanderlaufen
 * liesse. Aenderungen gegenueber dem Original: keine.
 */

/**
 * Focus protocol: the shared vocabulary for "what the user is currently looking at".
 *
 * Line/character convention: every position and range in this file is 0-based
 * editor space (Theia/LSP). The engine graph reports 1-based inclusive lines;
 * conversion between the two happens only in positions.ts.
 */

/** A caret position in 0-based editor space. */
export interface Position {
    /** 0-based line index. */
    line: number;
    /** 0-based UTF-16 code unit offset within the line. */
    character: number;
}

/** A half-open span in 0-based editor space: `start` inclusive, `end` exclusive. */
export interface Range {
    start: Position;
    end: Position;
}

/**
 * Symbol kinds CodeAtlas reasons about. Deliberately narrower than the LSP
 * SymbolKind set: anything the engine cannot classify becomes 'unknown' so
 * downstream code never has to guess.
 */
export type CodeAtlasSymbolKind =
    | 'function'
    | 'method'
    | 'class'
    | 'interface'
    | 'module'
    | 'variable'
    | 'type'
    | 'route'
    | 'unknown';

/** A resolved reference to a symbol in the workspace. */
export interface SymbolRef {
    /** Stable engine graph node id, absent when the symbol is not indexed. */
    nodeId?: string;
    name: string;
    /** Fully qualified name, for example `module.Class.method`. */
    qualifiedName?: string;
    kind: CodeAtlasSymbolKind;
    /** Absolute URI of the file that declares the symbol. */
    uri: string;
    /** Full declaration span, 0-based. */
    range: Range;
    /** Identifier span used for reveal/select, 0-based. */
    selectionRange?: Range;
    /** Owning project as known to the engine, for multi-project workspaces. */
    projectName?: string;
}

/** Which surface produced a focus change. Used for loop-breaking and telemetry. */
export type FocusOrigin =
    | 'editor-cursor'
    | 'twin'
    | 'map'
    | 'navigator'
    | 'impact'
    | 'search'
    | 'tour'
    | 'programmatic';

/**
 * What is in focus. Discriminated on `kind` so every consumer must handle the
 * non-symbol cases explicitly instead of degrading them to a symbol.
 */
export type FocusTarget =
    | { kind: 'symbol'; symbol: SymbolRef }
    | { kind: 'route'; routePath: string; method?: string; handler?: SymbolRef }
    | { kind: 'flow'; flowId: string; label?: string }
    | { kind: 'changeSet'; baselineRef: string }
    | { kind: 'domain'; domainId: string }
    | { kind: 'feature'; label: string; members: SymbolRef[] };

/** Broadcast whenever focus changes, including when it is cleared. */
export interface SymbolFocusEvent {
    /** `undefined` means focus was cleared, not "unchanged". */
    target: FocusTarget | undefined;
    origin: FocusOrigin;
    /** URI of the editor that was active when focus changed, if any. */
    editorUri?: string;
    /** Caret position at the time of the change, 0-based. */
    cursor?: Position;
    /** False when the engine has no index entry backing this target. */
    indexed: boolean;
    /** Epoch milliseconds. */
    timestamp: number;
}
