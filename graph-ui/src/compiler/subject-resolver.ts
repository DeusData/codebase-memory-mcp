/**
 * From a name somebody typed to the symbol the index knows.
 *
 * The classifier answers "which symbol is this question about" with a string,
 * because that is all a sentence carries. Every recipe below needs a
 * {@link SymbolRef}: a file, a range, and the `nodeId` that says the index
 * really holds this symbol. This file is the one bridge between the two, and it
 * is the same bridge the bug wizard's `resolveHop` crosses, for the same reason:
 * publishing a reference the index never confirmed would tell the rest of the
 * product that a perfectly well indexed symbol is not indexed.
 *
 * Three rules:
 *
 * **The index decides, not the string.** A name is searched, the hits are
 * filtered by how well they answer the name, and the winner is resolved at its
 * declaration line so the reference carries the range the index has. A name the
 * index does not hold resolves to nothing, which is a finding the recipe
 * reports rather than an error it throws.
 *
 * **A qualified suffix beats a bare name.** `userService.create` and `create`
 * both name something; the first one names less. So a candidate whose qualified
 * name ends with the requested dotted path wins over one that merely carries
 * the same last segment, and an exact bare-name match beats a substring hit.
 *
 * **Ambiguity is reported, not resolved by luck.** Two symbols named `create`
 * live in this project's own fixture on purpose. When several candidates tie,
 * the first in a deterministic order is returned and the others travel with it
 * in {@link ResolvedSubject.alternatives}, so a card can say that the name was
 * ambiguous instead of quietly picking one.
 *
 * ## How a reader spells a name, and why that stopped deciding (W7c)
 *
 * A user wrote `@createuser explain this funktion`. The search overlay showed
 * `createUser` in src/services/userService.ts on the same screen, and the chat
 * answered "the index holds no symbol called 'createuser'". Two readings of one
 * index contradicting each other in one window is the worst kind of wrong: the
 * product proves against itself that it knows the symbol.
 *
 * The cause was here. The comparisons below were letter for letter, while the
 * server search behind BOTH surfaces is BM25 over a camelCase-splitting index
 * and returns `createUser` for `createuser` without hesitating. So the hit was
 * fetched, scored zero and thrown away.
 *
 * Since W7c the comparison is folded, and the spelling became the FIRST rank
 * instead of a filter:
 *
 *  1. a hit matched with the exact spelling beats one matched by folding,
 *  2. within that, the specificity ladder of the three rules above decides,
 *  3. then non-test before test,
 *  4. then ordinal by qualified name.
 *
 * Nothing else moved: the same search, the same limit, the same deterministic
 * order. A name the index really does not hold still resolves to nothing.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type {
    ProviderQueryOptions,
    ResolveResult,
    SymbolSearchHit,
} from '../core/intelligence-provider';
import { twinTargetOf } from '../twin/twin-target';

/** What resolving a name needs from a provider, and nothing else. */
export interface SubjectSource {
    searchSymbols(
        root: string,
        pattern: string,
        limit?: number,
        opts?: ProviderQueryOptions,
    ): Promise<SymbolSearchHit[]>;
    declarationLineOf(
        root: string,
        filePath: string,
        name: string,
        opts?: ProviderQueryOptions,
    ): Promise<number | undefined>;
    resolveSymbolAt(
        root: string,
        filePath: string,
        oneBasedLine: number,
        opts?: ProviderQueryOptions,
    ): Promise<ResolveResult>;
}

/** How many hits one name may be ranked over. More is not a shorter list. */
export const SUBJECT_SEARCH_LIMIT = 20;

/** One resolved name, plus what else carried it. */
export interface ResolvedSubject {
    symbol: SymbolRef;
    /**
     * Every candidate the name reached, best first, the winner included.
     *
     * The panel needs the whole list and not only the losers: an ambiguous name
     * is offered to the reader as a choice, and a choice that hides the option
     * the compiler would have taken is not a choice.
     */
    candidates: SymbolSearchHit[];
    /** Other symbols the same name reached, in the same deterministic order. */
    alternatives: SymbolSearchHit[];
    /** True when more than one candidate scored the same. */
    ambiguous: boolean;
}

/** The last dotted segment of a name, which is what a reader usually types. */
export function bareNameOf(name: string): string {
    const segments = name.split('.');
    return segments[segments.length - 1] ?? name;
}

/**
 * How specific a match is, on the ladder the three rules of the header state.
 *
 * Four steps: an exact qualified name, then a qualified name ending in the
 * requested dotted path, then the bare name itself, then a qualified name
 * ending in the bare name. Zero means this hit does not answer this name.
 *
 * `exact` decides whether the comparison reads the letters or folds them. It is
 * a parameter and not two copies of the ladder, because two copies is how the
 * two halves would come to disagree about what "more specific" means.
 */
export function subjectSpecificity(hit: SymbolSearchHit, name: string, exact: boolean): number {
    const fold = (text: string): string => (exact ? text : text.toLowerCase());
    const qualified = fold(hit.qualifiedName ?? '');
    const wanted = fold(name);
    const bare = fold(bareNameOf(name));
    if (qualified === wanted) {
        return 4;
    }
    if (name.includes('.') && qualified.endsWith(`.${wanted}`)) {
        return 3;
    }
    if (fold(hit.name) === bare) {
        return 2;
    }
    if (qualified.endsWith(`.${bare}`)) {
        return 1;
    }
    return 0;
}

/**
 * What an exactly spelled match is worth over a folded one.
 *
 * One more than the top of the specificity ladder, so that the weakest exact
 * match still outranks the strongest folded one. That is the first rank of the
 * header, expressed as the one number it takes to hold it.
 */
export const SUBJECT_SPELLING_BONUS = 4;

/**
 * How well one hit answers one requested name. Higher is better.
 *
 * Spelling first, specificity second: `@createUser` prefers the symbol written
 * that way, and `@createuser` still finds it when nothing is written that way.
 * Zero still means "this hit does not answer this name", and a zero is still
 * dropped before anything is ordered.
 */
export function subjectScore(hit: SymbolSearchHit, name: string): number {
    const exact = subjectSpecificity(hit, name, true);
    return exact > 0 ? SUBJECT_SPELLING_BONUS + exact : subjectSpecificity(hit, name, false);
}

/**
 * Order candidates so two readings of one index agree.
 *
 * Score first, then non-test before test (a reader asking about `createUser`
 * means the function, not the test that calls it), then ordinally by qualified
 * name. Never `localeCompare`: that is the determinism rule the closure walk
 * and the tour generator both state.
 */
export function orderCandidates(hits: readonly SymbolSearchHit[], name: string): SymbolSearchHit[] {
    return [...hits].sort((a, b) => {
        const byScore = subjectScore(b, name) - subjectScore(a, name);
        if (byScore !== 0) {
            return byScore;
        }
        const byTest = Number(a.isTest ?? false) - Number(b.isTest ?? false);
        if (byTest !== 0) {
            return byTest;
        }
        const left = a.qualifiedName ?? a.name;
        const right = b.qualifiedName ?? b.name;
        if (left === right) {
            return 0;
        }
        return left < right ? -1 : 1;
    });
}

/**
 * Resolve one written name into the symbol the index holds, or nothing.
 *
 * Never rejects: a search that fails and a name the index does not know produce
 * the same `undefined`, and the caller states which of the two it is from the
 * note it writes. A recipe that threw here would turn one unknown name into a
 * chat turn with no answer at all.
 */
export async function resolveSubject(
    source: SubjectSource,
    root: string,
    name: string,
    opts: ProviderQueryOptions = {},
): Promise<ResolvedSubject | undefined> {
    const bare = bareNameOf(name);
    if (bare.length === 0) {
        return undefined;
    }
    const hits = await source
        .searchSymbols(root, bare, SUBJECT_SEARCH_LIMIT, opts)
        .catch(() => [] as SymbolSearchHit[]);
    const usable = hits.filter(
        (hit) => subjectScore(hit, name) > 0 && (hit.filePath ?? '').length > 0,
    );
    const ordered = orderCandidates(usable, name);
    const best = ordered[0];
    if (best === undefined) {
        return undefined;
    }
    const filePath = best.filePath ?? '';
    const line = best.line ?? (await source
        .declarationLineOf(root, filePath, best.name, opts)
        .catch(() => undefined));

    // The declaration line is what is resolved against, never a call line: the
    // same rule the closure walk states, and for the same reason.
    if (line !== undefined) {
        const resolved = await source
            .resolveSymbolAt(root, filePath, line, opts)
            .catch(() => undefined);
        if (resolved?.kind === 'ok') {
            return {
                symbol: resolved.symbol,
                candidates: ordered,
                alternatives: ordered.slice(1),
                ambiguous: ordered.length > 1
                    && subjectScore(ordered[1], name) === subjectScore(best, name),
            };
        }
    }

    // The index named a place but would not resolve it. The place is still the
    // honest answer; what it lacks is a `nodeId`, and every consumer reads that
    // absence as "not indexed as a node", which is exactly what happened.
    const fallback = twinTargetOf({
        name: best.name,
        qualifiedName: best.qualifiedName,
        kind: best.kind,
        filePath,
        startLine: line,
    });
    if (fallback === undefined) {
        return undefined;
    }
    return {
        symbol: fallback,
        candidates: ordered,
        alternatives: ordered.slice(1),
        ambiguous: ordered.length > 1 && subjectScore(ordered[1], name) === subjectScore(best, name),
    };
}
