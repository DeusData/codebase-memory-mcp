/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-core/src/common/positions.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Uebernommen wurden die Umrechnungen
 * zwischen Graph- und Editorzeilen, weil der Checklisten-Generator und der
 * IR-Builder sie brauchen und eine zweite, leicht abweichende Umrechnung genau
 * die Art Fehler waere, die niemand sieht: eine Zeile daneben liest sich
 * plausibel. Aenderungen gegenueber dem Original: die Monaco-Helfer
 * (toEditorCharacter, toMonacoColumn, toEditorRangeFromMonaco, MonacoRangeLike)
 * sind noch nicht mitportiert, weil dieser Zyklus keinen Editor anfasst; sie
 * kommen mit dem Reader-Zyklus.
 */

/**
 * The only place where line numbering systems are converted.
 *
 * Convention: editor space is 0-based; the engine graph reports 1-based
 * inclusive lines. Nothing outside this module may add or subtract 1 from a
 * line number. If a conversion is needed elsewhere, call these functions
 * instead of reimplementing them.
 */

import { Range } from './focus-protocol';

/**
 * Convert a 1-based inclusive graph line span into a 0-based editor range that
 * covers the whole lines. The end is expressed as the start of the following
 * line, the LSP-idiomatic way to say "through the end of `endLine`".
 */
export function toEditorRange(startLine: number, endLine: number): Range {
    const first = toEditorLine(startLine);
    const last = Math.max(first, toEditorLine(endLine));
    return {
        start: { line: first, character: 0 },
        end: { line: last + 1, character: 0 },
    };
}

/** 0-based editor line to 1-based graph line. */
export function toGraphLine(editorLine: number): number {
    return editorLine + 1;
}

/** 1-based graph line to 0-based editor line, clamped so it never goes negative. */
export function toEditorLine(graphLine: number): number {
    return Math.max(0, graphLine - 1);
}
