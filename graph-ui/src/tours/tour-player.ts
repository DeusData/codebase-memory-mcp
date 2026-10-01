/**
 * The arithmetic and the key meanings of the step card, without a card.
 *
 * The same reason `overlay-model.ts` exists for the search window: where a walk
 * stops, what the progress chain looks like, which key means what and what a
 * capped walk is allowed to say are the places a player feels wrong when it is
 * wrong, and all of them are provable here without a DOM.
 */

/**
 * The two block characters the progress chain is drawn from.
 *
 * From design/design.png, where the header of the step card is `STEP 10/10`
 * followed by a run of shaded blocks. They are text, not a bar element, for the
 * same reason everything else here is monospace: a graphical bar in a terminal
 * would be the one element that does not line up with anything.
 */
export const PROGRESS_FILLED = '▓';
export const PROGRESS_EMPTY = '░';

/**
 * How many blocks the chain may hold.
 *
 * A walk longer than this draws a chain of this length scaled to it, because a
 * chain that grew with the walk would wrap the header on a long one. Sixteen is
 * comfortably above {@link MAX_TOUR_STEPS} in tour-generator.ts, so the project
 * walk always draws one block per step and only a long forward walk is scaled.
 */
export const PROGRESS_WIDTH = 16;

/** `STEP 2/10`. One-based, because the reader is not counting from zero. */
export function progressLabel(index: number, total: number): string {
    if (total <= 0) {
        return 'STEP 0/0';
    }
    const at = Math.min(Math.max(index, 0), total - 1);
    return `STEP ${at + 1}/${total}`;
}

/**
 * The chain of blocks beside the label.
 *
 * Filled up to and including the step the reader is on, so the first step is
 * already one block rather than none: they are standing on it.
 */
export function progressBar(index: number, total: number): string {
    if (total <= 0) {
        return '';
    }
    const width = Math.min(total, PROGRESS_WIDTH);
    const at = Math.min(Math.max(index, 0), total - 1);
    const filled = Math.max(1, Math.round(((at + 1) / total) * width));
    return PROGRESS_FILLED.repeat(filled) + PROGRESS_EMPTY.repeat(Math.max(0, width - filled));
}

/** Where a move lands. Clamped, so the ends of a walk are ends and not wraps. */
export function stepMove(total: number, index: number, delta: number): number {
    if (total <= 0) {
        return 0;
    }
    return Math.min(Math.max(index + delta, 0), total - 1);
}

/** True when the reader is standing on the last step, so `next` reads `finish`. */
export function isLastStep(index: number, total: number): boolean {
    return total > 0 && index >= total - 1;
}

/** What the keys of the card mean while a walk is running. */
export type PlayerIntent = 'next' | 'prev' | 'exit' | 'diagram' | 'none';

/**
 * The meaning of a key press during a walk.
 *
 * Three keys move the walk. Enter moves on and finishes at the end, which is why
 * the card relabels the same button rather than growing a fourth: finishing is
 * moving past the last step, not a different act.
 *
 * The fourth key does not move the walk at all, and that is why it is a fourth
 * key rather than a fourth position of Enter. `d` opens the flow of the symbol
 * the reader is standing on and leaves the step where it is: PLAN paragraph 4
 * lists `[d] diagram` among the actions of the lower card, the product had put
 * the explainer on the twin's `flow()` head instead, and audit finding 13 of
 * 2026-08-29 recorded the gap. The card is the place a reader is looking while a
 * walk runs, so the way into the picture belongs on the card too. Whether the
 * key does anything on THIS step is a question about the step and is answered by
 * the caller: a file step has no symbol and no flow.
 */
export function playerIntent(key: string): PlayerIntent {
    switch (key) {
        case 'Enter':
            return 'next';
        case 'ArrowLeft':
            return 'prev';
        case 'q':
        case 'Q':
            return 'exit';
        case 'd':
        case 'D':
            return 'diagram';
        default:
            return 'none';
    }
}

/**
 * What a walk that stopped at a bound is allowed to say, and nothing more.
 *
 * One sentence with both numbers in it. A walk that reached the end of the graph
 * says nothing at all rather than "complete": this side cannot see what the
 * index did not record, so "you have seen everything" is a claim it is in no
 * position to make.
 */
export function capNote(truncated: boolean, cap: number, depth: number): string {
    if (!truncated) {
        return '';
    }
    return `walk capped at ${cap} symbols (depth ${depth}), so what follows this is not shown`;
}
