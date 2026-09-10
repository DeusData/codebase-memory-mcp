/**
 * The one sentence over the block: what the finding is, in this block, today.
 *
 * No React, no DOM, no fetching. Two things in (what the file pulls in, and the
 * block itself), one sentence out, so the surface renders a decision instead of
 * making one.
 *
 * ## Why the block leads with this at all
 *
 * The block used to open with its heading and then its steps. Beside an
 * eighteen line function that read "1. call validateId, 2. call query", and the
 * code next to it showed a try, a catch, an early return and an object being
 * built: a summary shorter than the thing it summarised and no faster to read.
 * Meanwhile the one thing on the screen a reader could not have worked out for
 * themselves, an import this symbol never reaches, sat below a heading that
 * sounded like a footnote.
 *
 * So the order is by contribution: what the code does not show first, the steps
 * after. This sentence is the top of that order.
 *
 * ## Two rules
 *
 * **There is no length threshold anywhere in this decision.** Nothing here asks
 * how long the function is, how many steps it has or whether the block is
 * "worth it". A number that decided when a function is short enough would be
 * guessed, and a guessed number that removes content is the worst kind: it is
 * invisible in the result. Every case below produces a lead, and the steps are
 * drawn in every one of them.
 *
 * **The sentence always stands, including when there is nothing to report.** A
 * head that only appeared when something was found would make its absence a
 * claim, and an absence is exactly what this product refuses to sell as
 * knowledge. When the imports check out and nothing is recorded behind the
 * steps, the lead says so in those words.
 */

import type { ImportsGroup } from './imports-group';
import type { PseudocodeDocument } from './pseudocode-builder';
import {
    PSEUDOCODE_LEAD_PENDING,
    pseudocodeLeadBehind,
    pseudocodeLeadNone,
    pseudocodeLeadUnchecked,
    pseudocodeLeadUnused,
} from './pseudocode-strings';

/** Which of the four the lead is. Also what a proof run reads to check the order. */
export type BlockLeadKind = 'unused-imports' | 'unchecked-imports' | 'behind-calls' | 'nothing' | 'pending';

export interface BlockLead {
    kind: BlockLeadKind;
    text: string;
    /** How many steps carry a note about the symbol they call. */
    behindSteps: number;
}

/**
 * The lead of one block.
 *
 * The order of the cases is the order of strength, and it is the same order the
 * block itself is laid out in. An import the index cannot tie to this symbol is
 * the strongest thing this block says about an unfamiliar file; what lies
 * behind the calls is next; the admission that nothing was found is last, and
 * it is still a sentence.
 */
export function blockLeadOf(
    document: PseudocodeDocument,
    imports: ImportsGroup | undefined,
    symbol: string,
): BlockLead {
    const behindSteps = document.lines.filter((line) => (line.behind?.length ?? 0) > 0).length;
    if (imports === undefined) {
        return { kind: 'pending', text: PSEUDOCODE_LEAD_PENDING, behindSteps };
    }
    const total = imports.used + imports.unused + imports.unknown;
    if (imports.unused > 0) {
        return {
            kind: 'unused-imports',
            text: pseudocodeLeadUnused(symbol, imports.unused, total),
            behindSteps,
        };
    }
    if (imports.unknown > 0) {
        return {
            kind: 'unchecked-imports',
            text: pseudocodeLeadUnchecked(symbol, imports.unknown),
            behindSteps,
        };
    }
    if (behindSteps > 0) {
        return { kind: 'behind-calls', text: pseudocodeLeadBehind(behindSteps), behindSteps };
    }
    return { kind: 'nothing', text: pseudocodeLeadNone(symbol, total), behindSteps };
}
