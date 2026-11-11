/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/twin/mini-understanding.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * MINI_MAX_ITEMS, CATEGORY_ORDER, die Ersatzziele, die Beschriftungen je
 * Kategorie und buildMiniUnderstanding samt der Regel, dass die Prozentzahl
 * ueber die Checkliste und nie ueber die Zeilen des Streifens rechnet.
 *
 * Aenderungen gegenueber dem Original: nur die Importpfade.
 */

/**
 * The short answer to "what do I still not know about this?".
 *
 * The backend already generates a full checklist: one item per callee, one per
 * caller, one per raised type, one per environment value. For a symbol of any
 * size that is twenty to forty items, which is the right shape for the review
 * checklist panel and the wrong shape for a strip above the twin. A reader
 * glancing at the twin needs to know there is work left and roughly what kind,
 * not to read a work breakdown.
 *
 * So this collapses the checklist by category into at most six lines, each of
 * which is a thing to go and do rather than a thing to go and read. Three
 * properties are load-bearing.
 *
 * **It summarises, it does not re-derive.** The counts come from the checklist
 * the backend generated, so the strip and the full panel can never disagree
 * about how much is left. Where a sentence needs a name rather than a count,
 * the name is read from the fact the checklist item was generated from, which
 * is the same source.
 *
 * **Every line goes somewhere.** An item the reader cannot act on is an
 * accusation. The target is taken from the checklist item when it carries one,
 * and otherwise from the first fact of that category, so the line opens the
 * thing it is asking about. The generator now attaches a target to every item
 * it emits, so the fallback below is reached only for an IR built before it
 * did; it is kept rather than deleted because a checklist item without a
 * target is still a shape this type allows.
 *
 * **The percentage counts confirmations and nothing else.** It is
 * `confirmed / total` over the checklist items themselves, never over the six
 * collapsed lines, so the number here and the number in the checklist panel are
 * the same number computed the same way. Visits are deliberately absent: the
 * strip has room for one figure, and the one figure a reader would mistake for
 * a claim of understanding must be the one that only their own confirmations
 * can move.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type {
    ChecklistCategory,
    ChecklistItem,
    SemanticIR
} from '../core/semantic-ir';

import {
    MINI_TESTS_NONE,
    miniCallers,
    miniConfig,
    miniConstructions,
    miniErrorPath,
    miniReadCalls,
    miniShape,
    miniTests
} from './strings';
import { CONSTRUCTION_STRATEGY, evidenceTarget, refAt, stepTarget } from './twin-view-model';

/** One line of the strip. */
export interface MiniItem {
    /** Stable within a symbol: the category it summarises. */
    id: string;
    /**
     * The checklist family this line collapses.
     *
     * Carried rather than parsed back out of the id, because it is what a
     * consumer that wants the items behind the line has to ask the checklist
     * for. Recovering it from `mini-<category>` would be a second derivation of
     * the same fact, and the id would quietly become a format instead of a key.
     */
    category: ChecklistCategory;
    label: string;
    /** True only when every checklist item this line stands for is confirmed. */
    done: boolean;
    /** What opens when the line is activated. */
    target?: SymbolRef;
}

/** The strip: a handful of lines and how far through them the reader is. */
export interface MiniUnderstanding {
    items: MiniItem[];
    /** 0 to 100, whole numbers: `confirmed` over `total`, rounded. */
    percent: number;
    /** Checklist items this reader has confirmed. Never mixed with visits. */
    confirmed: number;
    /** Checklist items there are. Not the number of lines above. */
    total: number;
}

/**
 * How many lines the strip shows.
 *
 * Six is the point at which a glance becomes a read. A symbol with more
 * categories than this has them in the review checklist, which is the surface
 * built for going through a list rather than noticing one.
 */
export const MINI_MAX_ITEMS = 6;

/**
 * The order categories are offered in.
 *
 * Not the generator's order and not alphabetical: this is the order in which
 * not knowing the answer hurts. What a symbol delegates to comes before how it
 * fails, which comes before who depends on it and what it needs configured.
 * Classes being constructed come last because that obligation is already half
 * covered by the calls line above it, which is the one this list can afford to
 * lose when a symbol has more than six kinds of unknown.
 */
export const CATEGORY_ORDER: readonly ChecklistCategory[] = [
    'core-logic',
    'error-handling',
    'callers',
    'config',
    'inputs',
    'tests',
    'implementations'
];

/** Keep the first occurrence of each key, so the engine's own order survives. */
function distinct(values: (string | undefined)[]): string[] {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const value of values) {
        if (value === undefined || value.length === 0 || seen.has(value)) {
            continue;
        }
        seen.add(value);
        out.push(value);
    }
    return out;
}

/**
 * Where a category's line goes when its checklist items carry no target.
 *
 * The generator attaches a target to every item it emits, so this is no longer
 * the usual path: it is reached only for an IR built before it did, or by a
 * consumer assembling a checklist of its own. The checklist item always wins
 * when it has a target, because it was derived from the same fact and knows
 * which one of several this line stands for.
 */
function fallbackTarget(ir: SemanticIR, category: ChecklistCategory): SymbolRef | undefined {
    switch (category) {
        case 'core-logic': {
            const call = ir.steps.value[0];
            return call ? stepTarget(call) : undefined;
        }
        case 'implementations': {
            const call = ir.steps.value.find(entry => entry.strategy === CONSTRUCTION_STRATEGY);
            return call ? stepTarget(call) : undefined;
        }
        case 'inputs': {
            const type = (ir.typeRefs?.value ?? []).find(entry => entry.file !== undefined);
            return type?.file ? refAt(type.file, type.line, type.name, type.qualifiedName) : undefined;
        }
        case 'callers': {
            const caller = ir.calledBy.value.find(entry => entry.file !== undefined);
            return caller?.file ? refAt(caller.file, caller.line, caller.name, caller.qualifiedName) : undefined;
        }
        case 'error-handling': {
            const raise = ir.throws.value.find(entry => entry.file !== undefined);
            if (raise?.file) {
                return refAt(raise.file, raise.line, raise.type);
            }
            // The engine records no raise-site line, so the citation on the
            // throws family points at the error type's own declaration, which
            // is the one place worth opening.
            const citation = ir.throws.evidence[0];
            return citation ? evidenceTarget(citation, ir.throws.value[0]?.type ?? '') : undefined;
        }
        case 'config': {
            const read = ir.reads.value.find(entry => entry.file !== undefined);
            return read?.file ? refAt(read.file, read.line, read.name, read.qualifiedName) : undefined;
        }
        case 'tests': {
            const test = ir.tests.value.find(entry => entry.file !== undefined);
            return test?.file ? refAt(test.file, test.line, test.name) : undefined;
        }
        default:
            return undefined;
    }
}

/** One sentence per category, counting the checklist and naming the facts. */
function labelFor(category: ChecklistCategory, items: ChecklistItem[], ir: SemanticIR): string {
    switch (category) {
        case 'core-logic':
            return miniReadCalls(items.length);
        case 'error-handling':
            return miniErrorPath(distinct(ir.throws.value.map(entry => entry.type)));
        case 'callers':
            return miniCallers(items.length);
        case 'config':
            return miniConfig(distinct(ir.reads.value.map(entry => entry.name)));
        case 'inputs':
            return miniShape(distinct((ir.typeRefs?.value ?? []).map(entry => entry.name)));
        case 'implementations':
            return miniConstructions(items.length);
        case 'tests':
            // The generator emits exactly one item for the empty case, and it is
            // a finding rather than a task, so it keeps its own wording.
            return ir.tests.value.length === 0 ? MINI_TESTS_NONE : miniTests(ir.tests.value.length);
        default:
            return items[0]?.label ?? '';
    }
}

/**
 * The strip for one symbol.
 *
 * Pure and total: an IR with an empty checklist produces an empty strip and a
 * percentage of zero, which the renderer omits rather than showing as a heading
 * over nothing.
 *
 * `confirmed` is the set of checklist item ids this reader has confirmed, as
 * the understanding service holds it. It is passed in rather than read from the
 * IR because the IR is a fact about the workspace and a confirmation is a fact
 * about a person: the same IR is served to every window, and only the caller
 * knows whose state to draw over it. When it is omitted the item's own `done`
 * is used, which is what a caller with an IR and no reader has.
 */
export function buildMiniUnderstanding(
    ir: SemanticIR,
    confirmed?: ReadonlySet<string>
): MiniUnderstanding {
    const isConfirmed = (item: ChecklistItem): boolean =>
        confirmed === undefined ? item.done : confirmed.has(item.id);

    const grouped = new Map<ChecklistCategory, ChecklistItem[]>();
    for (const item of ir.checklist) {
        const bucket = grouped.get(item.category);
        if (bucket) {
            bucket.push(item);
        } else {
            grouped.set(item.category, [item]);
        }
    }

    const items: MiniItem[] = [];
    for (const category of CATEGORY_ORDER) {
        const group = grouped.get(category);
        if (group === undefined || group.length === 0) {
            continue;
        }
        const label = labelFor(category, group, ir);
        if (label.length === 0) {
            continue;
        }
        items.push({
            id: `mini-${category}`,
            category,
            label,
            // A line is only ticked when everything behind it is: a category
            // that is half confirmed is not a category the reader is done with.
            done: group.every(isConfirmed),
            target: group.find(item => item.target !== undefined)?.target ?? fallbackTarget(ir, category)
        });
        if (items.length === MINI_MAX_ITEMS) {
            break;
        }
    }

    // Over the checklist, never over the lines above: the checklist panel counts
    // the same way, and two surfaces showing the same reader a different
    // percentage is the failure this arithmetic exists to prevent.
    const total = ir.checklist.length;
    const done = ir.checklist.filter(isConfirmed).length;
    return { items, confirmed: done, total, percent: total === 0 ? 0 : Math.round((100 * done) / total) };
}
