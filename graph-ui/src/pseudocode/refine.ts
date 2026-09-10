/**
 * Letting the local model rephrase a deterministic block, and refusing when it
 * did anything else.
 *
 * W4c ported `applyRefinedPseudocode` and left it unwired, with the note that
 * the wiring lands with the local model. This is that wiring, and it is a thin
 * file on purpose: the validator already decides what a legal rewrite is, and
 * everything here does is put a prompt on one side of it and a reason on the
 * other.
 *
 * ## Why a rewrite is worth doing at all
 *
 * The block is a list of findings in canonical vocabulary: "1. call
 * validateUser". That is exactly right and reads like a machine. A model that
 * turns it into "1. validate the input with validateUser" has added nothing and
 * removed nothing, and a reader gets through it faster. The moment it adds
 * anything, the rewrite is worthless and dangerous in the same breath, because
 * the block's whole authority is that every line is a fact of the index.
 *
 * ## What is checked, and why positionally
 *
 * `applyRefinedPseudocode` maps line by line: same count, same order, same
 * leading numbers, unnumbered lines still unnumbered. That is a strong check for
 * a cheap one, and it is the only check that cannot be talked around: a model
 * that dropped a step, merged two or renumbered anything fails it. What it
 * cannot catch is a line that keeps its number and lies inside it, which is why
 * the prompt forbids adding facts and why the reader can always bring the
 * original back with one click.
 *
 * ## The reason is shown, never swallowed
 *
 * A refused rewrite produces a named reason and the original block stays on
 * screen. A silent refusal would look exactly like a model that had nothing to
 * say, and the difference matters: one is the guard working and the other is the
 * sidecar being down.
 */

import type { PseudocodeDocument } from './pseudocode-builder';
import { applyRefinedPseudocode, leadingNumberOf, pseudocodeText } from './pseudocode-builder';
import {
    REFINE_REASON_COUNT,
    REFINE_REASON_EMPTY,
    REFINE_REASON_NUMBERS,
} from '../chat/chat-strings';

/** What a rewrite attempt produced. */
export type RefineOutcome =
    | { kind: 'applied'; document: PseudocodeDocument }
    | { kind: 'refused'; reason: string };

/** The lines of a model answer, trimmed and without the blanks. */
export function refinedLines(answer: string): string[] {
    return answer
        .split('\n')
        .map((line) => line.trim())
        .filter((line) => line.length > 0);
}

/**
 * Strip what a model puts around an answer even when told not to.
 *
 * Only two things are removed and both are unambiguous: a fenced code block's
 * fences, and a leading line that is nothing but a heading the prompt forbade.
 * Nothing else is normalised. A rewrite that needs cleaning to pass is a rewrite
 * that did not follow the contract, and cleaning it into shape would be this
 * file quietly doing the model's job.
 */
export function unfence(answer: string): string {
    const trimmed = answer.trim();
    if (!trimmed.startsWith('```')) {
        return trimmed;
    }
    const lines = trimmed.split('\n');
    const body = lines.slice(1, lines[lines.length - 1].trim().startsWith('```')
        ? lines.length - 1
        : lines.length);
    return body.join('\n').trim();
}

/**
 * Say exactly why a rewrite was refused.
 *
 * The validator answers `undefined` for every refusal, which is right for it and
 * useless for a reader. This repeats its two checks in order so the panel can
 * name which one fired.
 */
export function refusalReason(document: PseudocodeDocument, answer: string): string {
    const incoming = refinedLines(unfence(answer));
    if (incoming.length === 0) {
        return REFINE_REASON_EMPTY;
    }
    if (incoming.length !== document.lines.length) {
        return `${REFINE_REASON_COUNT} (${incoming.length} instead of ${document.lines.length})`;
    }
    for (let index = 0; index < document.lines.length; index++) {
        const expected = document.lines[index].order;
        const actual = leadingNumberOf(incoming[index]);
        if (expected !== actual) {
            return `${REFINE_REASON_NUMBERS} (line ${index + 1}: expected `
                + `${expected === undefined ? 'no number' : expected} and got `
                + `${actual === undefined ? 'no number' : actual})`;
        }
    }
    return 'the rewrite did not map onto the block it was built from';
}

/**
 * Put a model answer back onto the block, or refuse with a reason.
 *
 * Total: never throws, and an empty answer is a refusal like any other.
 */
export function applyRefinement(document: PseudocodeDocument, answer: string): RefineOutcome {
    const cleaned = unfence(answer);
    const applied = applyRefinedPseudocode(document, cleaned);
    if (applied === undefined) {
        return { kind: 'refused', reason: refusalReason(document, answer) };
    }
    return { kind: 'applied', document: applied };
}

/** The text a rewrite is asked to rephrase. The block, as a reader copies it. */
export function refineSubjectText(document: PseudocodeDocument): string {
    return pseudocodeText(document);
}

/**
 * How many tokens a rewrite may cost.
 *
 * The answer has to hold every line of the block, so a fixed ceiling would refuse
 * long blocks for a reason that has nothing to do with the model. Four tokens per
 * character of the block plus a floor is generous and still bounded.
 */
export function refineMaxTokens(document: PseudocodeDocument): number {
    return Math.max(256, Math.ceil(pseudocodeText(document).length / 2));
}
