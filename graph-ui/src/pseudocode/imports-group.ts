/*
 * Herkunft: portiert am 2026-08-29 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/pseudocode/imports-group.ts
 * (380 Zeilen). Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert
 * uebernommen: die drei Antworten used/unused/unknown samt ihrer Begruendung,
 * IMPORTS_GROUP_CAP, buildImportsGroup mit der Zaehlung ueber die ganze Liste
 * statt ueber die gekappte, entryOf mit den drei namenlosen Formen, matchOf mit
 * der Familien-Reihenfolge calls/typeRefs/throws/reads, fullyAnswered und
 * citationOf.
 *
 * Aenderungen gegenueber dem Original: die Importpfade (Evidence, Fact,
 * KnowledgeState und SemanticIR aus src/core/, der Wortlaut aus
 * ./pseudocode-strings), UND die beiden DTOs stehen hier statt in der
 * Provider-Schnittstelle. Grund: im Referenzprojekt beantwortet der Backend-
 * Provider `fileImports(root, filePath)`; der C-Server dieses Projekts bietet
 * kein solches Werkzeug an (INVENTAR Abschnitt 3), also wird die Antwort in
 * ./imports-source.ts aus zwei vorhandenen Lesungen zusammengesetzt. Die DTOs
 * gehoeren damit zu dieser Schicht und nicht zur Provider-Grenze.
 */

/**
 * What a file pulls in, and which names the complete file reading reaches.
 *
 * No React, no DOM, no services, no fetching. Two things in, one group out: the
 * import list the backend read for a file, and the semantic IR of every callable
 * declaration in that file. Both surfaces that draw this (the pseudocode block
 * and the twin's data chips) call this one function, so the block and the panel
 * can never disagree about whether the file reaches `query`.
 *
 * The feature exists because of one sentence of user feedback: the imports are
 * not explained. They are the first thing anybody reads in an unfamiliar file
 * and the last thing this product had anything to say about.
 *
 * Four rules make that safe to put on screen.
 *
 * **Three answers, not two.** The tempting design has a boolean, and the boolean
 * is a lie in one direction. A linter says "unused import" after parsing the
 * file; CodeAtlas has a list of names and recorded facts for every callable in
 * the file. The absence of a match means one of two very different things
 * depending on whether the family that would have carried the match was
 * answered at all. So a name a fact mentions is `used`, a name no fact mentions
 * *in families the index answered* is `unused`, and a name whose file-wide
 * check could not be run is `unknown`.
 * The third one is the whole point: claiming `unused` when `typeRefs` came back
 * `notIndexed` would be inventing a finding out of a gap.
 *
 * **`unused` is about the complete file, never the focused symbol.** `query`
 * is imported at the top of `userService.ts` and `createUser` never touches it;
 * `listUsers`, four lines up, does. A focused symbol alone therefore cannot
 * establish a negative file claim. If the complete callable reading is absent
 * or partial, every named import remains `unknown`.
 *
 * **A positive match wins outright.** If any family names the import, the entry
 * is `used`, whatever state the other families are in. A missing family can only
 * ever weaken a negative answer; it cannot weaken a match that was found.
 *
 * **The two readings stay apart.** The dependency between two files is the
 * analysis speaking. The statement, its line and the name it binds are CodeAtlas
 * reading the file. Every entry carries a citation for each of the two it has,
 * so a reader who opens the evidence on a chip sees exactly which half of the
 * claim came from where; an entry whose only citation is the import is an entry
 * that proves the import and nothing about its use, and it says so.
 */

import type { Evidence, Fact, KnowledgeState, SemanticIR } from '../core/semantic-ir';

import {
    IMPORTS_NOTE_INDEX_ONLY,
    IMPORTS_NOTE_NAMESPACE,
    IMPORTS_NOTE_SIDE_EFFECT,
    IMPORTS_NOTE_UNKNOWN,
    IMPORTS_NOTE_UNUSED,
    IMPORTS_NOTE_USED,
    IMPORT_MARK_UNKNOWN,
    IMPORT_MARK_UNUSED,
    IMPORT_MARK_USED,
    importsCappedNote,
    importsFindingHeading,
    importsIndexOnlyLine,
    importsNamespaceLine,
    importsSideEffectLine,
    importsTally,
    importsUnknownLine,
    importsUnusedLine,
    importsUsedLine,
} from './pseudocode-strings';
import type { PseudocodeSourceRef } from './pseudocode-builder';

/**
 * One name one file pulls in, and where that claim comes from.
 *
 * Assembled from two readings, because neither one answers the question on its
 * own. The index records that a file imports another file and nothing else: no
 * imported name, no statement line, and no edge at all for a module that
 * resolves outside the workspace. The file's own text carries all three and is
 * not a finding of the analysis. So both are read, every entry says which
 * reading produced it, and a surface can show the difference instead of
 * flattening it.
 */
export interface FileImportRef {
    /**
     * The name as the exporting module spells it.
     *
     * Absent for a side-effect import, for a namespace import and for an entry
     * recovered from an index edge alone, and those three absences mean the same
     * thing to a consumer: this entry names no symbol, so nothing about it can
     * be checked against the file's callable facts.
     */
    name?: string;
    /** The local binding, when the statement renamed what it imported. */
    alias?: string;
    /** The module specifier as written, or the imported file's path for an index-only entry. */
    module: string;
    /** 1-based graph line of the import statement, when the text was readable. */
    line?: number;
    /** Workspace-relative path of the imported file, when the index recorded an edge to one. */
    targetPath?: string;
    /**
     * True when the statement binds a whole module under one name.
     *
     * A namespace binding cannot be checked against callable facts: the index
     * records `db.query()` as a call to `query`, so the binding `db` appears
     * nowhere and would read as unused while the file leans on it.
     */
    namespace?: boolean;
    /** `index` means the analysis reported it; `source` means it was read off the text. */
    origin: 'index' | 'source';
    /** Citations backing this entry, in the same grammar every other fact uses. */
    evidence: Evidence[];
}

/**
 * What one file pulls in, plus everything a consumer needs in order not to
 * overstate it.
 *
 * The three fields beside `entries` exist because an empty list has four
 * different meanings here and a surface must be able to tell them apart: the
 * file imports nothing, the analysis records no import relation for this
 * language, the file could not be read, or a bound cut the reading short.
 */
export interface FileImportsDto {
    /** One entry per imported name, in the order the statements appear. */
    entries: FileImportRef[];
    /** True when the read stopped at its bound, so `entries` is a floor. */
    truncated: boolean;
    /**
     * Workspace-relative paths of the files the index records this one
     * importing, deduplicated and sorted.
     *
     * Kept beside the entries rather than folded into them, because it is the
     * one half of the answer the analysis itself stands behind.
     */
    indexedTargets: string[];
    /**
     * True when the file's own text was read.
     *
     * False means every entry below came from an index edge, so none of them
     * names a symbol or a line. That is a statement about this reading and not
     * about the file.
     */
    sourceRead: boolean;
    /** Facts for every callable declaration in the file, only when the read completed. */
    fileIrs?: SemanticIR[];
}

/**
 * What CodeAtlas is willing to say about one import and its containing file.
 *
 * Ordered by strength on purpose: a reader scanning a list of these should be
 * able to treat the first as a finding, the second as a finding about an
 * absence, and the third as an admission.
 */
export type ImportUsage =
    /** A fact recorded for a callable in this file names this import. */
    | 'used'
    /** No fact recorded for the complete file names it, in families the index answered. */
    | 'unused'
    /** The check could not be run, so nothing is claimed either way. */
    | 'unknown';

/** One name a file pulls in, as this group presents it. */
export interface ImportEntry {
    /** Stable within one group, used as a React key and as an evidence anchor. */
    id: string;
    /** Address of this entry's citations, in the `fact[row]` grammar the popover parses. */
    factPath: string;
    /** What the entry is called on screen: the imported name, the binding, or the module. */
    label: string;
    /** The module specifier exactly as the statement wrote it. */
    module: string;
    usage: ImportUsage;
    /** The short word beside the label. Never the only carrier of the status. */
    marker: string;
    /** The whole sentence the pseudocode block draws for this entry. */
    text: string;
    /** One sentence saying why this entry carries the status it does. */
    note: string;
    /** The import statement, when the file's text was read. */
    sourceRef?: PseudocodeSourceRef;
    /** Workspace-relative path of the imported file, when the index named one. */
    targetPath?: string;
    /**
     * True when this entry is the finding of the group rather than a line in
     * it (W8c).
     *
     * `used` is the expected case and reads as background; the other two are
     * the reason the group is worth a reader's time, and until W8c they sat in
     * the same typography as everything else, under a heading that sounded like
     * a footnote. The flag does not change what is said: the entry keeps its
     * own sentence, its marker and its limit.
     */
    finding: boolean;
    /** Which IR family produced the match, for a `used` entry. */
    usedBy?: ImportUsageFamily;
    /** `index` means the analysis reported it; `source` means it was read off the text. */
    origin: 'index' | 'source';
    /** The import's own citations, plus the one that proves the use when there is one. */
    evidence: Evidence[];
}

/** Which family of recorded facts named an import. */
export type ImportUsageFamily = 'calls' | 'typeRefs' | 'throws' | 'reads';

/** One file's imports, judged against its complete callable reading, ready to render. */
export interface ImportsGroup {
    heading: string;
    /** The entries the surface draws, already capped. */
    entries: ImportEntry[];
    /** How many entries the cap left out. Zero when nothing was left out. */
    hidden: number;
    /** The honest sentence about the cap, present exactly when `hidden` is not zero. */
    cappedNote?: string;
    /** The tally under the entries. Counted over the whole list, not over the capped one. */
    tally: string;
    used: number;
    unused: number;
    unknown: number;
    /** True when the file's own text was read; false means no entry names a symbol. */
    sourceRead: boolean;
}

/** What the group is built from. */
export interface ImportsGroupInput {
    /** The backend's answer for the file the anchor symbol lives in. */
    imports: FileImportsDto;
    /**
     * Kept for callers that already hold the focused IR. It is deliberately not
     * used to decide a file-level import claim: only `imports.fileIrs` can do
     * that, because another declaration in the same file may use the name.
     */
    irs: readonly SemanticIR[];
    /** Absolute URI of that file, so an entry can point at its own statement. */
    uri: string;
    /** How many entries the group may draw. Defaults to {@link IMPORTS_GROUP_CAP}. */
    cap?: number;
}

/**
 * How many entries one group draws.
 *
 * Twelve is about where a list of names stops being scannable and starts being
 * a wall, and it is comfortably above what an ordinary source file imports. A
 * file past it is read partially and says so, exactly as a bounded walk does.
 */
export const IMPORTS_GROUP_CAP = 12;

/**
 * Judge one file's imports against the complete callable reading of that file.
 *
 * Total: a file with no imports, a file whose text could not be read and a
 * symbol whose families are all missing each produce a group rather than
 * nothing, because every one of those is an answer a reader needs to be able to
 * tell from the others.
 */
export function buildImportsGroup(input: ImportsGroupInput): ImportsGroup {
    const cap = Math.max(1, input.cap ?? IMPORTS_GROUP_CAP);
    // `fileIrs` is present only after every declaration was resolved and built.
    // The focused IR is intentionally never a fallback: a positive match in it
    // proves one use, but its absence cannot prove that the whole file is quiet.
    const scopedInput = { ...input, irs: input.imports.fileIrs ?? [] };
    // `finding` is set here and not in the six branches of `entryOf`: it is one
    // rule over the answer they produce ("anything but `used` is why this group
    // exists"), and six copies of one rule is how two of them end up disagreeing.
    const judged: ImportEntry[] = input.imports.entries.map((entry, index) => {
        const built = entryOf(entry, index, scopedInput);
        return { ...built, finding: built.usage !== 'used' };
    });
    const counts = { used: 0, unused: 0, unknown: 0 };
    for (const entry of judged) {
        counts[entry.usage] += 1;
    }
    const hidden = Math.max(0, judged.length - cap);
    return {
        heading: importsFindingHeading(counts.unused, counts.unknown, judged.length),
        entries: judged.slice(0, cap),
        hidden,
        cappedNote: hidden > 0 ? importsCappedNote(hidden, Math.min(cap, judged.length)) : undefined,
        tally: importsTally(counts.used, counts.unused, counts.unknown),
        used: counts.used,
        unused: counts.unused,
        unknown: counts.unknown,
        sourceRead: input.imports.sourceRead,
    };
}

// ---------------------------------------------------------------------------

/** One import, with everything decided about it except which of them is the finding. */
function entryOf(ref: FileImportRef, index: number, input: ImportsGroupInput): Omit<ImportEntry, 'finding'> {
    const base = {
        id: `import-${index}`,
        factPath: `imports[${index}]`,
        module: ref.module,
        origin: ref.origin,
        sourceRef: ref.line === undefined ? undefined : { uri: input.uri, line: ref.line },
        targetPath: ref.targetPath,
        evidence: [...ref.evidence],
    };

    // The three shapes that name no symbol. None of them can be checked against
    // the file's callable facts, and each says why in its own words rather than sharing
    // one sentence that would have to be vague enough to cover all three.
    if (ref.namespace === true) {
        const binding = ref.alias ?? ref.name ?? ref.module;
        return {
            ...base,
            label: binding,
            usage: 'unknown',
            marker: IMPORT_MARK_UNKNOWN,
            text: importsNamespaceLine(binding, ref.module),
            note: IMPORTS_NOTE_NAMESPACE,
        };
    }
    if (ref.origin === 'index') {
        return {
            ...base,
            label: ref.targetPath ?? ref.module,
            usage: 'unknown',
            marker: IMPORT_MARK_UNKNOWN,
            text: importsIndexOnlyLine(ref.targetPath ?? ref.module),
            note: IMPORTS_NOTE_INDEX_ONLY,
        };
    }
    if (ref.name === undefined) {
        return {
            ...base,
            label: ref.module,
            usage: 'unknown',
            marker: IMPORT_MARK_UNKNOWN,
            text: importsSideEffectLine(ref.module),
            note: IMPORTS_NOTE_SIDE_EFFECT,
        };
    }

    const label = ref.alias === undefined ? ref.name : `${ref.name} as ${ref.alias}`;
    const match = matchOf(ref, input.irs);
    if (match !== undefined) {
        return {
            ...base,
            label,
            usage: 'used',
            marker: IMPORT_MARK_USED,
            text: importsUsedLine(label, ref.module),
            note: IMPORTS_NOTE_USED,
            usedBy: match.family,
            evidence: [...base.evidence, ...match.evidence],
        };
    }
    if (!fullyAnswered(input.irs)) {
        return {
            ...base,
            label,
            usage: 'unknown',
            marker: IMPORT_MARK_UNKNOWN,
            text: importsUnknownLine(label, ref.module),
            note: IMPORTS_NOTE_UNKNOWN,
        };
    }
    return {
        ...base,
        label,
        usage: 'unused',
        marker: IMPORT_MARK_UNUSED,
        text: importsUnusedLine(label, ref.module),
        note: IMPORTS_NOTE_UNUSED,
    };
}

/** The names one import statement could be matched under: what it exports, and what it binds. */
function namesOf(ref: FileImportRef): string[] {
    return [ref.name, ref.alias].filter((name): name is string => name !== undefined && name.length > 0);
}

/**
 * The first family of the file's callable facts that names the import, with the
 * citation that backs it.
 *
 * The order is the order of strength. A recorded call is the strongest thing the
 * index can say about a symbol reaching a name; a type reference is the next,
 * because a signature naming a type is a real use of the import that brought it
 * in; a raised type and an environment read come last because both are usually
 * already visible as one of the first two and are here so that a symbol whose
 * only mention of `ValidationError` is the raise site still counts.
 */
function matchOf(
    ref: FileImportRef,
    irs: readonly SemanticIR[],
): { family: ImportUsageFamily; evidence: Evidence[] } | undefined {
    const names = namesOf(ref);
    if (names.length === 0) {
        return undefined;
    }
    // Family before symbol: a call recorded for the second method of a class
    // outranks a type reference recorded for the first, because the ordering is
    // about how strong the finding is and not about whose facts it came from.
    for (const family of ['calls', 'typeRefs', 'throws', 'reads'] as const) {
        for (const ir of irs) {
            const found = matchIn(family, names, ir);
            if (found !== undefined) {
                return { family, evidence: found };
            }
        }
    }
    return undefined;
}

/** One family of one callable declaration, searched for any import binding. */
function matchIn(family: ImportUsageFamily, names: readonly string[], ir: SemanticIR): Evidence[] | undefined {
    switch (family) {
        case 'calls': {
            const index = ir.calls.value.findIndex((call) => names.includes(call.targetName));
            return index < 0 ? undefined : citationOf(ir.calls, index);
        }
        case 'typeRefs': {
            const index = (ir.typeRefs?.value ?? []).findIndex((entry) => names.includes(entry.name));
            return index < 0 ? undefined : citationOf(ir.typeRefs, index);
        }
        case 'throws': {
            const index = ir.throws.value.findIndex((entry) => names.includes(entry.type));
            return index < 0 ? undefined : citationOf(ir.throws, index);
        }
        default: {
            const index = ir.reads.value.findIndex((entry) => names.includes(entry.name));
            return index < 0 ? undefined : citationOf(ir.reads, index);
        }
    }
}

/**
 * True when every family the match would have come from was actually answered,
 * for every callable declaration in the file.
 *
 * This is what separates `unused` from `unknown`, and it is deliberately strict:
 * one family the index could not answer is enough to stop the product claiming
 * an import is untouched. `typeRefs` is the one that matters most in practice,
 * because a provider that cannot recover type positions omits the field
 * altogether, and a type-only import would otherwise be reported as unused in
 * every file of the project.
 *
 * A scope with no facts at all answers false. Nothing was checked, so nothing
 * may be denied.
 */
function fullyAnswered(irs: readonly SemanticIR[]): boolean {
    if (irs.length === 0) {
        return false;
    }
    return irs.every((ir) =>
        [ir.calls.state, ir.typeRefs?.state, ir.throws.state, ir.reads.state].every(answered));
}

/** True when a family's state is a reading rather than a gap. */
function answered(state: KnowledgeState | undefined): boolean {
    return state === 'known' || state === 'inferred';
}

/**
 * The citation backing one row of one fact, or none.
 *
 * The same lockstep rule the twin's popover follows: providers build the
 * evidence array in step with the value array, and where that does not hold the
 * honest answer is no citation rather than the wrong line.
 */
function citationOf<T>(fact: Fact<T[]> | undefined, index: number): Evidence[] {
    if (fact === undefined || fact.value.length !== fact.evidence.length) {
        return [];
    }
    const entry = fact.evidence[index];
    return entry === undefined ? [] : [entry];
}
