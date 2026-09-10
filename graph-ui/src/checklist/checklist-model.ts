/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/checklist-model.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * groupByCategory in der CATEGORY_ORDER aus twin/mini-understanding, die Regel,
 * dass eine dort nicht genannte Kategorie hinten angehaengt statt weggelassen
 * wird, percentOf mit Math.round, und vor allem die eine Regel, um derentwillen
 * die Datei existiert: Exploration und Bestaetigung werden getrennt gezaehlt,
 * ueberall, auch je Gruppe. Es gibt hier bewusst keine Funktion, die beides zu
 * einer Zahl mischt.
 *
 * Eine Abweichung, benannt statt versteckt: das Original importiert
 * `ChecklistItemState` und `UnderstandingState` aus
 * codeatlas-core/common/intelligence-rpc, also aus dem RPC-Vertrag zwischen
 * Theia-Backend und -Frontend. Dieses Projekt hat diesen Vertrag nicht: hier
 * gibt es kein Backend, das den Lesestand haelt, sondern den lokalen Speicher
 * dieses Browsers (understanding-store.ts). Die beiden Formen sind darum hier
 * deklariert, mit denselben Feldern wie im Original, direkt neben ihrem einzigen
 * Leser. Die Rechnung darunter ist dieselbe.
 */
/**
 * The checklist arranged for reading. No React, no DOM, no services.
 *
 * Deciding which group an item belongs in, what order the groups come in and
 * what each counter says is a reading of the state rather than a rendering
 * concern, and keeping it here means the next surface that wants the same
 * content writes different JSX rather than a second interpretation.
 *
 * The one rule the whole file exists to keep is that exploration and
 * verification are counted separately, everywhere, including per group. There
 * is deliberately no function here that returns a single combined figure: a
 * blended number would have to mean one of the two and would be read as the
 * other.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { ChecklistCategory } from '../core/semantic-ir';
import { CATEGORY_ORDER } from '../twin/mini-understanding';

/**
 * One checklist item plus what this reader has done with it.
 *
 * `visited` is written for them, when they are taken to the item. `confirmed`
 * is only ever written by an explicit tick. Two fields and not one, because
 * "I have been shown this" and "I have understood this" are different claims
 * and the second is not the product's to make.
 */
export interface ChecklistItemState {
    id: string;
    category: ChecklistCategory;
    label: string;
    /** What opens when the item is activated, when the generator could resolve one. */
    target?: SymbolRef;
    /** True when the reader has been taken to this item. */
    visited: boolean;
    /** True when the reader has ticked it. Never set by anything but a tick. */
    confirmed: boolean;
}

/** One symbol's checklist and the two counts over it, never merged. */
export interface UnderstandingState {
    /** The symbol this state is about, as the index names it. */
    symbolQualifiedName: string;
    items: ChecklistItemState[];
    /** Items the reader has been taken to, out of the items there are. */
    exploration: { visited: number; total: number };
    /** Items the reader has ticked, out of the items there are. */
    verification: { confirmed: number; total: number };
}

/** One category's items and the two counts that go beside its heading. */
export interface ChecklistGroup {
    category: ChecklistCategory;
    items: ChecklistItemState[];
    /** Items in this group the reader has opened. */
    visited: number;
    /** Items in this group the reader has ticked. */
    confirmed: number;
}

/**
 * The items grouped by category, in the order not knowing the answer hurts.
 *
 * The same order the strip above the twin offers, taken from the same constant,
 * so a reader who glances at the strip and then opens this list finds the
 * categories where they left them. A category the order does not mention still
 * appears, at the end, rather than being silently dropped: a checklist item
 * nobody can see is worse than one in an unexpected place.
 */
export function groupByCategory(items: readonly ChecklistItemState[]): ChecklistGroup[] {
    const grouped = new Map<ChecklistCategory, ChecklistItemState[]>();
    for (const item of items) {
        const bucket = grouped.get(item.category);
        if (bucket) {
            bucket.push(item);
        } else {
            grouped.set(item.category, [item]);
        }
    }
    const ordered: ChecklistGroup[] = [];
    const take = (category: ChecklistCategory, bucket: ChecklistItemState[]): void => {
        ordered.push({
            category,
            items: bucket,
            visited: bucket.filter((item) => item.visited).length,
            confirmed: bucket.filter((item) => item.confirmed).length,
        });
    };
    for (const category of CATEGORY_ORDER) {
        const bucket = grouped.get(category);
        if (bucket) {
            take(category, bucket);
            grouped.delete(category);
        }
    }
    for (const [category, bucket] of grouped) {
        take(category, bucket);
    }
    return ordered;
}

/**
 * One counter as a whole-number percentage.
 *
 * Zero of zero is zero rather than an error or a dash: a symbol with no
 * checklist has nothing to confirm, and "0%" is a true statement about it. The
 * rounding matches the twin strip's, because the same figure is shown in both
 * places and a reader who saw 6% here and 7% there would be right to distrust
 * both.
 */
export function percentOf(done: number, total: number): number {
    return total === 0 ? 0 : Math.round((100 * done) / total);
}
