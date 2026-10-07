/**
 * The quiet record of what a reader has been taken to, per project.
 *
 * In the reference project this lives in the workspace, written by a backend
 * (`node/understanding/`) into `.codeatlas/state/understanding.json`. There is
 * no backend here and no workspace to write into: the server this frontend talks
 * to answers a read-only allowlist and offers nothing that stores a reader's
 * state. So the record lives in this browser's own `localStorage`, and the
 * surface says so in as many words. That is a weaker promise than the reference
 * makes and it is stated rather than hidden: clearing site data forgets it, and
 * a colleague opening the same project on their machine starts empty.
 *
 * Three rules survive the move unchanged, because they are what the record is:
 *
 * **Item ids are the generator's ids.** They come from
 * `src/ir/checklist-generator.ts`, which hashes the category and what the item
 * points at, so a mark survives a reindex and a move of the repository. Nothing
 * here derives an id of its own; a second derivation is one refactor away from
 * agreeing with nothing.
 *
 * **Only exploration is written automatically.** `markVisited` records that the
 * reader was taken to an item. Confirmation is a separate field that only an
 * explicit tick may set, and this module keeps them apart rather than folding
 * them into one number.
 *
 * **Time spent is never evidence.** Nothing here records a duration, a visit
 * count or a timestamp. Being somewhere is not understanding it, and a record
 * that counted seconds would be inviting exactly that reading.
 */

import type { ChecklistItem } from '../core/semantic-ir';
import type { ChecklistItemState, UnderstandingState } from './checklist-model';

/** Prefix of the key one project's record lives under. */
export const UNDERSTANDING_KEY_PREFIX = 'atlas-understanding:';

/** The key one project's record lives under. */
export function understandingKey(project: string): string {
    return `${UNDERSTANDING_KEY_PREFIX}${project}`;
}

/**
 * The stored shape: per symbol, the ids that have been visited and the ids that
 * have been confirmed. Two lists and not one, for the reason in the header.
 */
export interface UnderstandingRecord {
    visited: Record<string, string[]>;
    confirmed: Record<string, string[]>;
}

/** Only what this module needs of `Storage`, so a test can hand it a map. */
export interface KeyValueStore {
    getItem(key: string): string | null;
    setItem(key: string, value: string): void;
}

function emptyRecord(): UnderstandingRecord {
    return { visited: {}, confirmed: {} };
}

function readList(source: unknown): Record<string, string[]> {
    if (typeof source !== 'object' || source === null || Array.isArray(source)) {
        return {};
    }
    const out: Record<string, string[]> = {};
    for (const [key, value] of Object.entries(source as Record<string, unknown>)) {
        if (Array.isArray(value)) {
            out[key] = value.filter((entry): entry is string => typeof entry === 'string');
        }
    }
    return out;
}

/**
 * The record of one project, or an empty one.
 *
 * A stored value that will not parse is treated as absent rather than as an
 * error: it is one browser's own bookkeeping, and refusing to show a checklist
 * because a previous build wrote a shape this one cannot read would punish the
 * reader for our version skew.
 */
export function readUnderstanding(store: KeyValueStore, project: string): UnderstandingRecord {
    if (project.length === 0) {
        return emptyRecord();
    }
    const raw = store.getItem(understandingKey(project));
    if (raw === null) {
        return emptyRecord();
    }
    try {
        const parsed = JSON.parse(raw) as Record<string, unknown>;
        return { visited: readList(parsed['visited']), confirmed: readList(parsed['confirmed']) };
    } catch {
        return emptyRecord();
    }
}

/**
 * Record that the reader was taken to one item of one symbol.
 *
 * Returns the record as it now stands, so a caller can render from the value it
 * just wrote rather than reading the store back and hoping.
 */
export function markVisited(
    store: KeyValueStore,
    project: string,
    symbolQualifiedName: string,
    itemId: string,
): UnderstandingRecord {
    const record = readUnderstanding(store, project);
    if (project.length === 0 || symbolQualifiedName.length === 0 || itemId.length === 0) {
        return record;
    }
    const marks = record.visited[symbolQualifiedName] ?? [];
    if (marks.includes(itemId)) {
        return record;
    }
    const next: UnderstandingRecord = {
        visited: { ...record.visited, [symbolQualifiedName]: [...marks, itemId] },
        confirmed: record.confirmed,
    };
    write(store, project, next);
    return next;
}

/** Tick or untick one item. Only ever called from an explicit tick. */
export function setConfirmed(
    store: KeyValueStore,
    project: string,
    symbolQualifiedName: string,
    itemId: string,
    confirmed: boolean,
): UnderstandingRecord {
    const record = readUnderstanding(store, project);
    if (project.length === 0 || symbolQualifiedName.length === 0 || itemId.length === 0) {
        return record;
    }
    const marks = record.confirmed[symbolQualifiedName] ?? [];
    const has = marks.includes(itemId);
    if (has === confirmed) {
        return record;
    }
    const next: UnderstandingRecord = {
        visited: record.visited,
        confirmed: {
            ...record.confirmed,
            [symbolQualifiedName]: confirmed ? [...marks, itemId] : marks.filter((entry) => entry !== itemId),
        },
    };
    write(store, project, next);
    return next;
}

function write(store: KeyValueStore, project: string, record: UnderstandingRecord): void {
    try {
        store.setItem(understandingKey(project), JSON.stringify(record));
    } catch {
        // A full or refused storage is not a reason to lose the click that was
        // just made: the marks stay in the value returned above for as long as
        // this page lives, and the next read simply finds fewer of them.
    }
}

/**
 * One symbol's checklist with this reader's marks drawn over it.
 *
 * The items are the generated ones and the marks are the reader's, which is why
 * they are joined here and nowhere else: the checklist is a fact about the
 * workspace and a mark is a fact about a person, and the same generated
 * checklist is shown to everybody.
 */
export function understandingOf(
    record: UnderstandingRecord,
    symbolQualifiedName: string,
    items: readonly ChecklistItem[],
): UnderstandingState {
    const visited = new Set(record.visited[symbolQualifiedName] ?? []);
    const confirmed = new Set(record.confirmed[symbolQualifiedName] ?? []);
    const states: ChecklistItemState[] = items.map((item) => ({
        id: item.id,
        category: item.category,
        label: item.label,
        target: item.target,
        visited: visited.has(item.id),
        confirmed: confirmed.has(item.id),
    }));
    return {
        symbolQualifiedName,
        items: states,
        exploration: { visited: states.filter((item) => item.visited).length, total: states.length },
        verification: { confirmed: states.filter((item) => item.confirmed).length, total: states.length },
    };
}

/** How many marks this project holds in total. The one figure that only grows. */
export function totalMarks(record: UnderstandingRecord): number {
    return Object.values(record.visited).reduce((sum, ids) => sum + ids.length, 0);
}

/**
 * What the status bar says about the symbol in front of the reader.
 *
 * Undefined when there is no checklist to count, and that is the whole point of
 * the return type. "explored 0 of 0" reads as a finding about the symbol; it is
 * really a statement about the reader not having asked yet, or about the index
 * not holding the symbol. Nothing at all is the honest rendering of that.
 */
export function exploredLabel(state: UnderstandingState | undefined): string | undefined {
    if (state === undefined || state.exploration.total === 0) {
        return undefined;
    }
    return `${state.exploration.visited} of ${state.exploration.total}`;
}

/**
 * The browser's own storage, or a store that forgets everything.
 *
 * `localStorage` throws rather than returning null in a browser with site data
 * switched off, so the access is guarded and the fallback keeps the surface
 * working with no record at all, which is the same state a first visit is in.
 */
export function browserStore(): KeyValueStore {
    try {
        const probe = globalThis.localStorage;
        if (probe !== undefined && probe !== null) {
            return probe;
        }
    } catch {
        // falls through to the forgetful store
    }
    const memory = new Map<string, string>();
    return {
        getItem: (key) => memory.get(key) ?? null,
        setItem: (key, value) => {
            memory.set(key, value);
        },
    };
}
