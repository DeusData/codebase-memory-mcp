/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/tour/tour-model.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * TOUR_MARK_CATEGORY, TourCoverage, emptyCoverage, coveragePercent mit
 * Math.floor, sumCoverage, markableItemId samt Rueckfall auf das erste Item,
 * resumeStep mit seinen drei Absagen und stepSymbol.
 *
 * Aenderungen gegenueber dem Original: die Importpfade zeigen auf die
 * portierten Dateien; `ChecklistItemState` und `UnderstandingState` kommen aus
 * ../checklist/checklist-model statt aus dem RPC-Vertrag des Referenzprojekts
 * (Begruendung steht dort im Kopf).
 */
/**
 * The three decisions a tour player makes that are worth proving on their own.
 *
 * No DOM, no services, no JSX: where a reader is offered to resume, which
 * checklist item a step's arrival stands for, and what "coverage went up" counts
 * are answers a suite can pin, and a widget is a bad place to pin anything.
 *
 * The coverage figures here are the strictest thing in the file. They count
 * exploration and only exploration: checklist items the reader has been taken
 * to, across the symbols the tour visits. They never touch confirmations, they
 * are never averaged with them, and they are never presented as a measure of
 * understanding. A tour moves somebody through a workspace in a few minutes,
 * which makes it the one surface where a number on screen could most easily be
 * read as a certificate; the arithmetic below is deliberately incapable of
 * producing one.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { ChecklistItemState, UnderstandingState } from '../checklist/checklist-model';
import type { TourStepDto } from '../core/tour-protocol';
import type { TourDto, TourProgressDto } from '../core/tour-protocol';

/**
 * The checklist category a tour step's arrival stands for.
 *
 * A step takes a reader to a symbol, and the obligation that is about the symbol
 * itself rather than about somewhere else is the core-logic one: what it
 * delegates to. Marking that item is the smallest true claim available.
 */
export const TOUR_MARK_CATEGORY = 'core-logic';

/** Exploration across the symbols one tour visits. */
export interface TourCoverage {
    /** Checklist items the reader has been taken to. */
    visited: number;
    /** Checklist items those symbols have between them. */
    total: number;
    /** `visited` as a whole percentage of `total`, and zero when there is no total. */
    percent: number;
    /** How many of the tour's steps had a checklist to count at all. */
    symbols: number;
}

/** Nothing counted yet, which is different from nothing to count. */
export function emptyCoverage(): TourCoverage {
    return { visited: 0, total: 0, percent: 0, symbols: 0 };
}

/**
 * A percentage that never rounds a partial answer up to a whole one.
 *
 * `Math.floor` rather than `Math.round`, because 99.6% of a checklist is not all
 * of it and a reader who saw 100% would reasonably conclude they had been
 * everywhere. The one exception is exactness: visited equal to total is 100.
 */
export function coveragePercent(visited: number, total: number): number {
    if (total <= 0) {
        return 0;
    }
    if (visited >= total) {
        return 100;
    }
    return Math.max(0, Math.floor((visited * 100) / total));
}

/** Add up the exploration counts of the states a tour's steps resolved to. */
export function sumCoverage(states: readonly (UnderstandingState | undefined)[]): TourCoverage {
    let visited = 0;
    let total = 0;
    let symbols = 0;
    for (const state of states) {
        if (state === undefined) {
            continue;
        }
        symbols += 1;
        visited += state.exploration.visited;
        total += state.exploration.total;
    }
    return { visited, total, percent: coveragePercent(visited, total), symbols };
}

/**
 * The item a step's arrival marks, or undefined when there is nothing to mark.
 *
 * The first core-logic item, and the first item of any category when the symbol
 * has none. The fallback is deliberate and it is not a widening of the claim:
 * every checklist item is an obligation about the symbol the reader has just
 * been taken to, and being taken somewhere is exactly what `visited` records.
 * What the fallback is not allowed to become is a loop over every item, which
 * would credit a reader who was shown one file with having followed all of its
 * obligations.
 */
export function markableItemId(items: readonly ChecklistItemState[]): string | undefined {
    const core = items.find((item) => item.category === TOUR_MARK_CATEGORY);
    return (core ?? items[0])?.id;
}

/**
 * The step to offer to resume at, or undefined when there is nothing to offer.
 *
 * Three refusals. Progress against another tour is not this tour's position.
 * Progress at the first step is not a place worth resuming to: the offer would
 * be "carry on from the beginning", which is what the start button already says.
 * Progress past the end is a tour that has been regenerated shorter since, and
 * pointing at a step that no longer exists would be worse than starting over.
 */
export function resumeStep(tour: TourDto | undefined, progress: TourProgressDto | undefined): number | undefined {
    if (tour === undefined || progress === undefined || progress.tourId !== tour.id) {
        return undefined;
    }
    if (progress.stepIndex <= 0 || progress.stepIndex >= tour.steps.length) {
        return undefined;
    }
    return progress.stepIndex;
}

/**
 * The symbol a step points at, or undefined for a file step.
 *
 * The one place the two kinds of step are told apart, so every caller that has
 * to branch on it branches the same way: a file step is not a symbol with a
 * missing name, it is a step the index gave no symbol for, and nothing in the
 * player may invent one.
 */
export function stepSymbol(step: TourStepDto | undefined): SymbolRef | undefined {
    return step?.primary.kind === 'symbol' ? step.primary.symbol : undefined;
}
