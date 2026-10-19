/**
 * Letting the local model word the twin's sentences for the selected reader,
 * and refusing when it did anything else.
 *
 * The same shape as `src/pseudocode/refine.ts`, on purpose and not by accident:
 * this project already has one answer to "may a model touch a deterministic
 * block", and a second answer with different rules would mean two different
 * promises about the same guarantee. So the promise is repeated here word for
 * word, and only the thing being checked is different.
 *
 * ## Why a rewrite is worth doing at all
 *
 * The sentences of a level are assembled from counts and names. "It hands work
 * to 4 other pieces of code." is exactly right and reads like a form letter. A
 * model that turns it into "it leans on four other pieces of code to get its
 * work done" has added nothing and removed nothing, and the vibe coder this
 * level exists for gets through it faster. The moment it adds anything, the
 * rewrite is worthless and dangerous in the same breath, because the whole
 * authority of the sentence is that every fact in it came out of the index.
 *
 * ## What is checked, and why positionally
 *
 * Line by line, same count, same order. Inside a line, the *fact tokens* have
 * to be identical as a sequence: the same names, the same numbers, the same
 * files, the same lines, in the same order. A fact token is a number, a dotted
 * or slashed path, or an identifier with a capital inside it, which between
 * them are every shape a name, a file or a figure takes in these sentences.
 *
 * The required sequence is derived from the built sentence itself rather than
 * from a list somebody has to remember to fill in. That is the whole reason the
 * check can be trusted: a builder that forgot to declare one of its facts would
 * make the guard blind to exactly that fact, and a guard whose coverage depends
 * on a second list is a guard with a hole in the shape of that list.
 *
 * What it cannot catch is a sentence that keeps every token and lies in the
 * English between them. That is what the prompt is for, and it is why the
 * reader can always bring the built text back with one click, and why the
 * marker beside a reworded sentence says who worded it.
 *
 * ## The reason is shown, never swallowed
 *
 * A refused rewrite produces a named reason, the built text stays on screen and
 * the reader is told. A silent refusal would look exactly like a model that had
 * nothing to say, and the difference matters: one is the guard working and the
 * other is the sidecar being down.
 */

import { refinedLines, unfence } from '../pseudocode/refine';

/**
 * One sentence a level put on screen, in the form a rewrite works on.
 *
 * `facts` is not an input to the check. It is what the check extracted, carried
 * along so a proof run and a reader can both see what had to survive.
 */
export interface ReaderLine {
    /** Stable within a level, so a rewritten sentence can be put back where it was. */
    id: string;
    text: string;
    /** The names, numbers, files and lines this sentence carries, in order. */
    facts: string[];
}

/** What a rewrite attempt produced. */
export type ReaderRewriteOutcome =
    | { kind: 'applied'; lines: ReaderLine[] }
    | { kind: 'refused'; reason: string };

/**
 * The three shapes a fact takes in these sentences.
 *
 * In this order, and the order is load bearing: a path has to be matched whole
 * before the identifier rule gets at its first segment, or `validate.ts` would
 * come apart into two tokens and a model that wrote `validate.js` would pass.
 *
 *  1. a dotted or slashed path: `validate.ts`, `src/services/userService.ts`
 *  2. an identifier with a capital inside it: `createUser`, `ValidationError`,
 *     and also `CodeAtlas`, which is a name too and has to survive like one
 *  3. a number, whole or decimal: a line number, a count, a confidence
 *
 * A plain lower-case word is deliberately not a fact token. "errors" in an
 * English sentence is grammar, and requiring it to survive would refuse every
 * rewrite that used "failures" instead, which is precisely the freedom the
 * model is being handed.
 */
export const FACT_TOKEN =
    /[A-Za-z_$][A-Za-z0-9_$]*(?:[./\\][A-Za-z0-9_$]+)+|[A-Za-z_$][A-Za-z0-9_$]*[A-Z][A-Za-z0-9_$]*|\d+(?:\.\d+)?/g;

/** Every fact token of one sentence, in the order it says them. */
export function factsOf(text: string): string[] {
    return text.match(FACT_TOKEN) ?? [];
}

/**
 * How much longer a reworded sentence may be than the one it replaces.
 *
 * A ceiling rather than a taste: a model that keeps every token and pads three
 * hundred words of commentary around them passes the token check and destroys
 * the page. Twice the length plus forty characters is room to rephrase and not
 * room to write an essay.
 */
export function lengthCeiling(original: string): number {
    return original.length * 2 + 40;
}

/** Build the `ReaderLine` list for a set of sentences, facts already extracted. */
export function readerLines(entries: { id: string; text: string }[]): ReaderLine[] {
    return entries
        .filter((entry) => entry.text.trim().length > 0)
        .map((entry) => ({ id: entry.id, text: entry.text, facts: factsOf(entry.text) }));
}

/** The text a rewrite is asked to reword: one sentence per line, in order. */
export function readerSubjectText(lines: readonly ReaderLine[]): string {
    return lines.map((line) => line.text).join('\n');
}

/**
 * How many tokens a rewrite may cost.
 *
 * The answer has to hold every sentence, so a fixed ceiling would refuse a long
 * level for a reason that has nothing to do with the model. Same rule as the
 * pseudocode rewrite: half a token per character, with a floor.
 */
export function readerMaxTokens(lines: readonly ReaderLine[]): number {
    return Math.max(256, Math.ceil(readerSubjectText(lines).length / 2));
}

/**
 * The words of a sentence, lowercased, as a set.
 *
 * Used only to answer "which built sentence does this rewrite belong to". Not a
 * fact check and not a quality measure: a rewrite is supposed to change words,
 * and this is the coarsest possible reading of whether it changed which
 * sentence it is.
 */
function wordSet(text: string): Set<string> {
    return new Set(text
        .toLowerCase()
        .split(/[^a-z0-9_$./]+/i)
        // A dot inside a word is part of a file name and stays; a dot at the
        // end of one is the end of a sentence. Without this, `code.` and `code`
        // are two different words and every rewrite that moved a full stop
        // would read as a reordering.
        .map((word) => word.replace(/^[./]+/, '').replace(/[./]+$/, ''))
        .filter((word) => word.length > 0));
}

/** How much two sentences overlap, 0 to 1. */
function overlap(one: Set<string>, other: Set<string>): number {
    if (one.size === 0 || other.size === 0) {
        return one.size === other.size ? 1 : 0;
    }
    let shared = 0;
    for (const word of one) {
        if (other.has(word)) {
            shared += 1;
        }
    }
    return shared / (one.size + other.size - shared);
}

/**
 * The order of the statements, checked where the facts cannot check it.
 *
 * The fact check compares position by position, which catches a model that
 * moved a name or a number. It cannot catch two sentences that carry no fact
 * at all being swapped: both sides look identical to it, and the reader ends up
 * with the senior's sentence under the junior's heading.
 *
 * So each rewritten sentence is asked which built sentence it most resembles.
 * If some other built sentence is a strictly better match than the one it is
 * standing in for, the sentences were reordered and the whole rewrite goes. The
 * comparison is deliberately crude (shared words over all words) because it is
 * answering a crude question: a genuine rewording keeps most of its own
 * sentence's words and almost none of a different sentence's.
 */
function orderRefusal(lines: readonly ReaderLine[], incoming: readonly string[]): string {
    const built = lines.map((line) => wordSet(line.text));
    for (let index = 0; index < incoming.length; index += 1) {
        const rewritten = wordSet(incoming[index]);
        const mine = overlap(rewritten, built[index]);
        for (let other = 0; other < built.length; other += 1) {
            if (other !== index && overlap(rewritten, built[other]) > mine) {
                return `sentence ${index + 1} reads like the built sentence ${other + 1}, `
                    + 'so the statements came back in a different order';
            }
        }
    }
    return '';
}

/** Why one line was rejected, or the empty string when it was fine. */
function lineRefusal(original: ReaderLine, incoming: string, index: number): string {
    const where = `sentence ${index + 1}`;
    if (incoming.trim().length === 0) {
        return `${where} came back empty`;
    }
    if (incoming.length > lengthCeiling(original.text)) {
        return `${where} came back ${incoming.length} characters long, over the ceiling of `
            + `${lengthCeiling(original.text)} for a sentence of ${original.text.length}`;
    }
    const got = factsOf(incoming);
    const want = original.facts;
    for (let at = 0; at < Math.max(want.length, got.length); at += 1) {
        if (want[at] === got[at]) {
            continue;
        }
        if (want[at] === undefined) {
            return `${where} added "${got[at]}", which is not in the sentence it was given`;
        }
        if (got[at] === undefined) {
            return `${where} dropped "${want[at]}"`;
        }
        return `${where} says "${got[at]}" where the built sentence says "${want[at]}"`;
    }
    return '';
}

/**
 * Put a model answer back onto the level's sentences, or refuse with a reason.
 *
 * Total: never throws, and an empty answer is a refusal like any other.
 */
export function applyReaderRewrite(
    lines: readonly ReaderLine[],
    answer: string,
): ReaderRewriteOutcome {
    const incoming = refinedLines(unfence(answer));
    if (lines.length === 0) {
        return { kind: 'refused', reason: 'there was nothing on this level to reword' };
    }
    if (incoming.length === 0) {
        return { kind: 'refused', reason: 'the model answered with nothing' };
    }
    if (incoming.length !== lines.length) {
        return {
            kind: 'refused',
            reason: `the rewrite has ${incoming.length} sentences instead of ${lines.length}`,
        };
    }
    for (let index = 0; index < lines.length; index += 1) {
        const refusal = lineRefusal(lines[index], incoming[index], index);
        if (refusal.length > 0) {
            return { kind: 'refused', reason: refusal };
        }
    }
    const reordered = orderRefusal(lines, incoming);
    if (reordered.length > 0) {
        return { kind: 'refused', reason: reordered };
    }
    return {
        kind: 'applied',
        lines: lines.map((line, index) => ({ ...line, text: incoming[index] })),
    };
}

/** The reworded sentences by id, for a renderer that has to put them back. */
export function rewriteMap(lines: readonly ReaderLine[]): Readonly<Record<string, string>> {
    const out: Record<string, string> = {};
    for (const line of lines) {
        out[line.id] = line.text;
    }
    return out;
}
