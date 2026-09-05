/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-views/src/browser/search/semantic-search.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Die Rangfolge ist Zeile fuer Zeile
 * die des Originals: dieselben Stufen, dieselben Gewichte, dieselbe totale
 * Ordnung. Sie hier "leicht anzupassen" hiesse, zwei Produkte mit zwei
 * Bedeutungen von "passt am besten" zu haben, und die Abweichung faende
 * niemand, weil beide Listen plausibel aussehen.
 *
 * Aenderungen gegenueber dem Original: nur der Importpfad von
 * SymbolSearchHit. Was diese Datei an Signalen NICHT bekommt, entscheidet der
 * Aufrufer: an diesem Backend bleiben `isTest` und `isExported` undefiniert,
 * weil die flache search_graph-Form sie nicht traegt (UPSTREAM-ASKS.md,
 * Ask 5). Undefiniert heisst hier weder wahr noch falsch: der Bonus und der
 * Abzug fallen einfach aus, und niemand erfindet eine Flagge.
 */

/**
 * Search by meaning, without asking anybody what a meaning is.
 *
 * ## Why there is no model behind this
 *
 * "Semantic search" normally means an embedding, which means a second thing
 * that can leave the machine and a ranking nobody can explain when it is wrong.
 * This product already has one place where a byte may leave and one reason for
 * it to, and neither is a search box. So the ranking here is arithmetic over
 * words: the query is split, the candidate names and the paths they live in are
 * split the same way, and what is scored is the overlap. Every hit can be
 * explained in one clause, which is what {@link RankedHit.matched} carries and
 * what the picker prints under each row.
 *
 * That is a weaker promise than an embedding makes and it is one this product
 * can keep. A reader typing "user validation" is not asking for a vector; they
 * are asking which of the things in this repository are about users and about
 * validating, and a repository names those things after what they do.
 *
 * ## The signals, and why each one is here
 *
 * **A name is worth more than a path.** `validateUser` is about validating
 * users; `toUser` in `userService.ts` is about users because of where it lives,
 * which is a weaker claim about the same word.
 *
 * **An exact word beats a prefix beats a shared stem.** "user" and "users" are
 * the same word to a reader and not to a string comparison, and "validation"
 * and "validate" share seven letters and a meaning. Each of those is worth
 * something and each is worth less than the one above it, so a symbol that
 * carries the reader's word exactly is never beaten by one that nearly does.
 *
 * **Covering two words beats covering one twice.** A query of two words is two
 * questions, and a symbol that answers both is what the reader is looking for.
 * This is a bonus rather than a tier, deliberately: a symbol that answers one
 * word perfectly and lives in the right file is a better answer than one that
 * answers both words faintly, and a hard tier would rank them the other way.
 *
 * **Exported beats internal.** A reader searching a codebase is looking for
 * something they can reach from elsewhere. A helper that happens to share a
 * word is a worse answer to that question, and test code is a worse one still.
 *
 * **Fan-in breaks ties, and then the name does.** Two symbols with identical
 * word overlap are separated by how much of the repository reaches them, which
 * is the index's own measurement of which one matters more. The qualified name
 * is the last resort, so the order is total: two runs over one index produce the
 * same list, which is what lets a picker be photographed and a driver assert on
 * the top five.
 *
 * Nothing in this file talks to anything. It is given candidates and it returns
 * an order, so the whole ranking is unit tested against hand written hits with
 * no engine, no workspace and no window.
 */

import type { SymbolSearchHit } from '../core/intelligence-provider';

/**
 * How many words one query may carry.
 *
 * Six, which is a sentence fragment rather than a paragraph. Past that the
 * coverage bonus stops discriminating between candidates and the search is
 * ranking noise; a reader with six meaningful words is describing something
 * they should be reading the map for.
 */
export const MAX_QUERY_TERMS = 6;

/**
 * Words too common to carry meaning in a code search.
 *
 * Deliberately tiny and deliberately English. A long stop list is a second
 * place where a search silently ignores what somebody typed; these five are the
 * ones that appear in a phrase like "the user validation" without saying
 * anything about which symbol is wanted.
 */
export const STOP_TERMS: ReadonlySet<string> = new Set(['the', 'a', 'an', 'of', 'for']);

/** How short a word may be and still count. Two letters is `id`, one is noise. */
export const MIN_TERM_LENGTH = 2;

/** How many letters two words must share before they are treated as the same stem. */
export const MIN_SHARED_PREFIX = 5;

/** What one query word is worth when a symbol's own name carries it. */
export const NAME_EXACT = 10;
export const NAME_PREFIX = 7;
export const NAME_STEM = 5;
export const NAME_SUBSTRING = 4;

/** The same word, in the path or the group the symbol lives in. Always worth less. */
export const PATH_EXACT = 3;
export const PATH_PREFIX = 2;
export const PATH_STEM = 2;
export const PATH_SUBSTRING = 1;

/** Answering a second word of the query, and every word after it. */
export const COVERAGE_BONUS = 6;

/** Reachable from elsewhere in the repository, as the index reports it. */
export const EXPORTED_BONUS = 4;

/** Test code. A hit, but not the one somebody searching a codebase means. */
export const TEST_PENALTY = 8;

/** One candidate, ranked, with the reason it is where it is. */
export interface RankedHit {
    hit: SymbolSearchHit;
    score: number;
    /** Which words of the query this candidate answered, in query order. */
    matched: string[];
    /** How many symbols reach it, when the index measured that. */
    fanIn: number;
}

/**
 * Split a phrase or an identifier into the words a reader would say.
 *
 * One splitter for the query, the names and the paths, because the whole
 * ranking is a comparison between the three and two splitters would make
 * `userService` match "user" in a name and not in a path. Camel case, snake
 * case, kebab case, dots and slashes all become boundaries; digits stay with
 * the word they are attached to, because `v2` and `sha256` are words.
 */
export function tokenize(value: string): string[] {
    return value
        .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
        .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
        .split(/[^A-Za-z0-9]+/)
        .map(part => part.toLowerCase())
        .filter(part => part.length > 0);
}

/**
 * The words of a query, deduplicated, in the order they were typed.
 *
 * Order is kept because it is the reader's order of importance and because the
 * explanation under each hit reads back in the order they wrote. The cap takes
 * from the end for the same reason the chat's mention cap does: somebody who
 * types seven words meant the first ones most.
 */
export function queryTerms(query: string): string[] {
    const terms: string[] = [];
    for (const term of tokenize(query)) {
        if (term.length < MIN_TERM_LENGTH || STOP_TERMS.has(term) || terms.includes(term)) {
            continue;
        }
        terms.push(term);
        if (terms.length >= MAX_QUERY_TERMS) {
            break;
        }
    }
    return terms;
}

/** How many leading letters two words share. */
export function sharedPrefixLength(a: string, b: string): number {
    const bound = Math.min(a.length, b.length);
    let shared = 0;
    while (shared < bound && a[shared] === b[shared]) {
        shared++;
    }
    return shared;
}

/**
 * What one query word is worth against one candidate word.
 *
 * The four rungs in one place, so a name and a path are scored by the same
 * ladder at two different weights and cannot drift apart.
 */
function rungOf(term: string, candidate: string, weights: readonly [number, number, number, number]): number {
    if (candidate === term) {
        return weights[0];
    }
    if (candidate.startsWith(term) || term.startsWith(candidate)) {
        return weights[1];
    }
    if (sharedPrefixLength(term, candidate) >= MIN_SHARED_PREFIX) {
        return weights[2];
    }
    if (candidate.includes(term) || term.includes(candidate)) {
        return weights[3];
    }
    return 0;
}

/** The best rung any of a set of words reaches for one query word. */
function bestRung(term: string, words: readonly string[], weights: readonly [number, number, number, number]): number {
    let best = 0;
    for (const word of words) {
        best = Math.max(best, rungOf(term, word, weights));
    }
    return best;
}

const NAME_WEIGHTS: readonly [number, number, number, number] =
    [NAME_EXACT, NAME_PREFIX, NAME_STEM, NAME_SUBSTRING];
const PATH_WEIGHTS: readonly [number, number, number, number] =
    [PATH_EXACT, PATH_PREFIX, PATH_STEM, PATH_SUBSTRING];

/**
 * Score one candidate against one query.
 *
 * The name and the path contribute separately for the same word rather than the
 * better of the two winning, because a symbol whose name and whose file both
 * say "user" is more about users than one where only the name does. The
 * coverage bonus is paid once per word answered beyond the first.
 */
export function scoreHit(hit: SymbolSearchHit, terms: readonly string[], fanIn: number): RankedHit {
    const nameWords = tokenize(hit.name);
    const pathWords = tokenize(hit.filePath ?? '');
    let score = 0;
    const matched: string[] = [];
    for (const term of terms) {
        const value = bestRung(term, nameWords, NAME_WEIGHTS) + bestRung(term, pathWords, PATH_WEIGHTS);
        if (value > 0) {
            score += value;
            matched.push(term);
        }
    }
    if (matched.length > 1) {
        score += (matched.length - 1) * COVERAGE_BONUS;
    }
    if (hit.isExported === true) {
        score += EXPORTED_BONUS;
    }
    if (hit.isTest === true) {
        score -= TEST_PENALTY;
    }
    return { hit, score, matched, fanIn };
}

/**
 * Whether a hit is something a reader can be sent to.
 *
 * The index answers a name search with modules and files as well as symbols,
 * and both of those are named after their own path. They are dropped rather
 * than ranked: this search ends in a jump to a declaration, and a file has no
 * declaration to jump to. Nothing is hidden by doing so, because the file is
 * reachable from every symbol row that names it.
 */
export function isNavigable(hit: SymbolSearchHit): boolean {
    if (hit.name.length === 0 || hit.name.includes('/')) {
        return false;
    }
    if (hit.kind === 'module') {
        return false;
    }
    const filePath = hit.filePath ?? '';
    return filePath.length === 0 || !filePath.endsWith(`/${hit.name}`);
}

/**
 * The candidates, ranked.
 *
 * `fanInOf` is passed in rather than read from the hit, because the index does
 * not report it on a search row: it is a separate measurement the caller asks
 * for in one batch. A candidate nobody measured has a fan-in of zero, which
 * only ever affects a tie.
 *
 * A candidate that matched no word of the query is dropped. It is in the list
 * because the index matched its name against one of the words as a substring
 * pattern, which is a weaker rule than this file's, and keeping it would mean
 * ranking things the reader was not asking about.
 */
export function rankHits(
    hits: readonly SymbolSearchHit[],
    query: string,
    fanInOf: (hit: SymbolSearchHit) => number = () => 0
): RankedHit[] {
    const terms = queryTerms(query);
    if (terms.length === 0) {
        return [];
    }
    const ranked: RankedHit[] = [];
    const seen = new Set<string>();
    for (const hit of hits) {
        if (!isNavigable(hit)) {
            continue;
        }
        const key = hit.qualifiedName ?? `${hit.filePath ?? ''}#${hit.name}`;
        if (seen.has(key)) {
            continue;
        }
        seen.add(key);
        const scored = scoreHit(hit, terms, fanInOf(hit));
        if (scored.matched.length === 0) {
            continue;
        }
        ranked.push(scored);
    }
    return ranked.sort(compareRanked);
}

/**
 * The order, and it is total.
 *
 * Score, then fan-in, then the qualified name. The last one is what makes two
 * runs over one index produce the same list: without it the order would be
 * whatever the index happened to return, which is not a promise it makes.
 */
export function compareRanked(a: RankedHit, b: RankedHit): number {
    return b.score - a.score
        || b.fanIn - a.fanIn
        || (a.hit.qualifiedName ?? a.hit.name).localeCompare(b.hit.qualifiedName ?? b.hit.name);
}
