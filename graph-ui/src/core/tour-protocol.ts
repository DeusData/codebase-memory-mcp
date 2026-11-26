/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-core/src/common/tour-protocol.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * TOUR_SCHEMA_VERSION, GETTING_STARTED_TOUR_ID, TourStrategy, TourProvenance,
 * TourStepPrimaryRecord, TourStepRecord, TourDocument, TourStepTarget,
 * TourStepDto, TourDto und TourProgressDto, samt der beiden Regeln, an denen
 * die Form haengt: nichts in einer gespeicherten Tour nennt eine Maschine, und
 * nichts in ihr ist eine Uhrzeit.
 *
 * Aenderungen gegenueber dem Original: der Import von SymbolRef zeigt auf die
 * ebenfalls portierte focus-protocol.ts im selben Verzeichnis. Sonst keine.
 */
/**
 * Tours: an ordered walk through a workspace, and where a reader has got to.
 *
 * A tour is the one CodeAtlas artefact that is meant to be committed. Everything
 * else the product writes about a workspace is about one reader on one machine.
 * A tour is about the repository: it says which file has to make sense before
 * another one can, and that answer is the same for everybody who checks the
 * repository out. Its shape is constrained by that decision in two ways.
 *
 * **Nothing in a stored tour names a machine.** No absolute path, no `file://`
 * URI, no home directory. A step records a workspace-relative path and, where
 * the index resolved one, a qualified name; the URI a reader's editor opens is
 * built at the moment it is asked for. That is why there are two shapes below
 * rather than one: the record is what would be on disk, the DTO is what a
 * surface receives with the URIs filled in.
 *
 * **Nothing in a stored tour is a clock reading.** No generation timestamp, no
 * "last generated at". Two runs of the generator over one index produce
 * byte-identical documents, which is what makes a regenerated tour a diff a
 * reviewer can read rather than a line that always changes. The one timestamp
 * in this file is on the progress record, which is per reader and is the only
 * thing here anybody would want a clock for.
 *
 * Progress is deliberately not part of the tour. A tour that carried the
 * reader's position would make "where I am" a property of the repository, and
 * two people on one branch would overwrite each other's place in the walk.
 */

import type { CodeAtlasSymbolKind, SymbolRef } from './focus-protocol';

/** Bump only when the document shape changes in a way older builds cannot read. */
export const TOUR_SCHEMA_VERSION = 1;

/** Id and file stem of the one tour the product generates from the whole project. */
export const GETTING_STARTED_TOUR_ID = 'getting-started';

/**
 * How a tour's order was arrived at.
 *
 * A string rather than a boolean, because there is more than one and a reader
 * opening the document is entitled to know which. `topsort` means the order is
 * a topological sort of the file-level import graph: everything a file depends
 * on comes before it, and nothing about the order is a judgement.
 *
 * `forward-walk` is this project's second strategy and it is added here rather
 * than smuggled in as a free string: it means the order is a breadth-first walk
 * over the calls out of one symbol the reader chose, which is the entry-point
 * mode. A surface that has to say where an order came from can then say it
 * without guessing.
 */
export type TourStrategy = 'topsort' | 'forward-walk';

/** What produced a tour, recorded with the document so it can be read back. */
export interface TourProvenance {
    strategy: TourStrategy;
    /** Version of the analysis backend whose index was read, when it named one. */
    engineVersion?: string;
    /**
     * True when the read the order was derived from stopped at its bound.
     *
     * Carried because a reader looking at a tour of a very large repository has
     * to be able to tell an order derived from the whole graph from one derived
     * from part of it.
     */
    truncated?: boolean;
    /** How many import edges the order was derived from, after deduplication. */
    edgeCount?: number;
    /**
     * Edges dropped to break a dependency cycle, as `from -> to`.
     *
     * Empty for an acyclic project. Recorded rather than silently applied: a
     * cycle is a fact about the repository, and a tour that had quietly chosen
     * one of the files in it to come first would be presenting a coin toss as a
     * reading. See `breakCycle` in ../tours/tour-generator.ts for the rule.
     */
    brokenEdges?: string[];
}

/**
 * What one step points at, as it is stored.
 *
 * Two kinds and not one. A file step is the honest answer when the index holds
 * no exported symbol in the file: a tour that invented one would be pointing a
 * reader at a line nobody chose. A symbol step carries the qualified name as
 * well as the line, because the qualified name is what every other CodeAtlas
 * surface keys on and a line alone would make the step navigable but not
 * askable-about.
 */
export type TourStepPrimaryRecord =
    | { kind: 'file'; filePath: string }
    | {
        kind: 'symbol';
        filePath: string;
        /** 1-based declaration line, in graph line space. */
        line: number;
        name: string;
        qualifiedName: string;
        symbolKind: CodeAtlasSymbolKind;
    };

/** One step of a stored tour. */
export interface TourStepRecord {
    id: string;
    title: string;
    /**
     * Why this step is here, assembled from the index and from the path.
     *
     * Every clause of it is traceable: the role comes from a path segment or
     * from the layer the index placed the file's group in, and the counts come
     * from the dependency graph. Nothing in a description is a claim about what
     * the code does.
     */
    description: string;
    /** 0-based position in the walk. Redundant with the array, and read by humans. */
    order: number;
    primary: TourStepPrimaryRecord;
    /** The group the index placed this file's code in, when one matched. */
    group?: string;
    /** The layer the index placed that group in, when it named one. */
    layer?: string;
}

/** A tour as a document: exactly what would be written to `.codeatlas/tours/<id>.json`. */
export interface TourDocument {
    schemaVersion: number;
    id: string;
    title: string;
    generated: TourProvenance;
    steps: TourStepRecord[];
}

/**
 * What one step points at, once a workspace root is known.
 *
 * The symbol case carries a whole `SymbolRef` so a step can be handed to the
 * reader and to the checklist without either of them having to know a tour
 * exists.
 */
export type TourStepTarget =
    | { kind: 'file'; filePath: string; uri: string }
    | { kind: 'symbol'; filePath: string; symbol: SymbolRef };

/** One step of a tour, as a surface receives it. */
export interface TourStepDto {
    id: string;
    title: string;
    description: string;
    order: number;
    primary: TourStepTarget;
    group?: string;
    layer?: string;
}

/** A tour, as a surface receives it. */
export interface TourDto {
    schemaVersion: number;
    id: string;
    title: string;
    generated: TourProvenance;
    steps: TourStepDto[];
    /**
     * Workspace-relative path of the file this tour was read from or written
     * to, so a surface can say where the artefact lives without composing a
     * path of its own. Empty here: this frontend generates a tour in the
     * browser and writes nothing, and an invented path would be the claim that
     * a file exists.
     */
    path: string;
}

/**
 * Where one reader has got to in one tour.
 *
 * Per machine and never committed: it is the reader's place in the walk, not
 * the repository's. `stepIndex` is the step they last arrived at, so an offer
 * to resume puts them back where they were rather than one step past it.
 */
export interface TourProgressDto {
    tourId: string;
    /** 0-based index of the step the reader last arrived at. */
    stepIndex: number;
    /** ISO timestamp of the last move. */
    updatedAt: string;
}
