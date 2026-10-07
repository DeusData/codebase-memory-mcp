/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/twin/hop-plan.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * MAX_HOP_STOPS, HopStop, GuidedHop und planHop.
 *
 * Nicht mitportiert wurde guided-hop-service.ts, das die Wanderung ausfuehrt:
 * es haengt am Fokus-Bus und am Editor-Manager von Theia. Der Plan selbst ist
 * reine Lesart der Checkliste und darum genau der Teil, der hier ohne Schale
 * beweisbar bleibt.
 *
 * Aenderungen gegenueber dem Original: nur die Importpfade.
 */

/**
 * What a guided walk visits. No DOM, no services, no JSX.
 *
 * Split from the service that runs a walk for the same reason `twin-view-model`
 * is split from the widget that renders it: deciding where a reader is taken is
 * a reading of the checklist, and a reading of the checklist should be provable
 * without a shell around it.
 *
 * The route is the checklist and never a second opinion about it. The backend
 * already decided what a reader owes for one symbol; this takes the items of one
 * category, in their own order, and stops. A planner that ranked them, or added
 * one, would let the strip, the walk and the review panel disagree about the
 * same question, and the reader has no way to tell which of the three is right.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { ChecklistCategory, ChecklistItem } from '../core/semantic-ir';

import { hopWhy } from './strings';

/**
 * How many places one walk visits.
 *
 * Three is where a guided detour stops being a detour. A category with more
 * obligations than this has all of them in the review checklist, which is the
 * surface built for working through a list; this one is built for being shown
 * the shape of an answer and getting back to what you were doing.
 */
export const MAX_HOP_STOPS = 3;

/** One place a walk takes the reader, and why it is on the route. */
export interface HopStop {
    /** The checklist item this stop stands for, so arriving can mark it explored. */
    itemId: string;
    /** The item's own wording, so the popover names what the reader asked about. */
    label: string;
    /** One sentence about why this place answers the question. */
    why: string;
    target: SymbolRef;
}

/** A walk in progress. */
export interface GuidedHop {
    /** The symbol the walk is about, which is not the symbol it is currently showing. */
    symbol: SymbolRef;
    category: ChecklistCategory;
    stops: HopStop[];
    /** 0-based index of the stop on screen. */
    index: number;
}

/**
 * The route for one category, from the checklist the backend already generated.
 *
 * Items with nowhere to go are dropped rather than rendered as a stop that opens
 * nothing: a route with a dead leg in it is worse than a shorter route.
 */
export function planHop(
    checklist: readonly ChecklistItem[],
    category: ChecklistCategory,
    limit: number = MAX_HOP_STOPS
): HopStop[] {
    const stops: HopStop[] = [];
    for (const item of checklist) {
        if (item.category !== category || item.target === undefined) {
            continue;
        }
        stops.push({ itemId: item.id, label: item.label, why: hopWhy(category), target: item.target });
        if (stops.length === limit) {
            break;
        }
    }
    return stops;
}
