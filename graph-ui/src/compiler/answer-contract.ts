/**
 * What counts as a kept promise in a model's answer.
 *
 * One file, read by three consumers that must never disagree: the chat panel,
 * which turns `[K3]` into a button; the eval, which marks an answer red when a
 * claim carries no citation; and the refine path, which rejects a rewrite that
 * changed the list. Two implementations of "does this line cite a card" would
 * eventually let the panel render a citation the eval had already called a
 * violation.
 *
 * ## The rule, stated once
 *
 * **Every claim line carries a citation.** A claim line is a non-empty line of
 * the answer that is not the agreed no-card sentence. It must contain at least
 * one `[Kn]`, and every `[Kn]` it contains must name a card that was actually
 * given. A number nobody handed over is an invented source, which is worse than
 * no source at all: it looks checkable and is not.
 *
 * **The no-card sentence is exempt, and only it.** A model that was given cards
 * which do not answer the question is supposed to say so, and that sentence
 * cites nothing because there is nothing to cite. It is recognised by its
 * marker rather than by an exact string match, so a German answer that says
 * "keine Karte" is accepted as readily as the English one the contract asks
 * for. Everything else with no citation is a violation.
 *
 * **A citation is a range or a single number.** `[K3]` and `[K3, K4]` both
 * occur naturally; both are read, and both are checked.
 */

/** The exact sentence the contract asks a model to fall back to. */
export const NO_CARD_SENTENCE = 'No card covers this. Fetch it with @name.';

/**
 * How that sentence is recognised in an answer.
 *
 * By its marker and not by equality, in both languages: a 1B model asked in
 * German will reach for "keine Karte", and refusing that as a violation would
 * punish the model for being honest in the language it was addressed in.
 */
export const NO_CARD_MARKERS: readonly RegExp[] = [
    /\bno card\b/i,
    /\bkeine karte\b/i,
];

/** Every card id an answer cites, in the order it cites them, without repeats. */
export function citationsIn(answer: string): string[] {
    const found: string[] = [];
    const seen = new Set<string>();
    const pattern = /\[([^\]]{1,80})\]/g;
    let match = pattern.exec(answer);
    while (match !== null) {
        for (const id of match[1].matchAll(/\bK(\d+)\b/g)) {
            const key = `K${Number(id[1])}`;
            if (!seen.has(key)) {
                seen.add(key);
                found.push(key);
            }
        }
        match = pattern.exec(answer);
    }
    return found;
}

/** Whether one line is the agreed no-card sentence in either language. */
export function isNoCardLine(line: string): boolean {
    return NO_CARD_MARKERS.some((marker) => marker.test(line));
}

/**
 * The lines of an answer that make a claim.
 *
 * Blank lines are not claims. Everything else is, including a heading and
 * including a line that only names a symbol: a reader takes both as statements
 * about their code, so the contract does too.
 *
 * The one exemption is the no-card answer, and it is exempt only when it is the
 * WHOLE answer. That is not a technicality; it closes a hole a live model walked
 * straight through. Asked who calls `insert`, one candidate answered two uncited
 * lines and then "No card says that no other symbol calls insert." A rule that
 * excused any line carrying the marker would have excused that one, and the two
 * uncited claims above it would have gone unmarked. The contract says the
 * fallback stands alone; so does the check.
 */
export function claimLines(answer: string): string[] {
    const lines = answer
        .split('\n')
        .map((line) => line.trim())
        .filter((line) => line.length > 0);
    if (lines.length === 1 && isNoCardLine(lines[0])) {
        return [];
    }
    return lines;
}

/** One violation of the contract, named so a report can be read. */
export interface CitationViolation {
    line: string;
    reason: 'no-citation' | 'unknown-card';
    /** The card ids the line named that were never given. */
    unknown?: string[];
}

/** What checking one answer against one card set produced. */
export interface CitationCheck {
    /** True when every claim line cites, and every citation exists. */
    ok: boolean;
    /** Cards the answer cited that exist. */
    cited: string[];
    /** Card ids the answer cited that were never given. */
    unknown: string[];
    violations: CitationViolation[];
    /** True when the answer is the agreed no-card sentence and nothing else. */
    noCardOnly: boolean;
    /**
     * Whether this check looked at a single line of the answer.
     *
     * False exactly when nothing was left to look at: no claim line survived,
     * and the answer is not the no-card sentence. That happens in one shape
     * above all, and it is the reason this field exists (finding from W7c,
     * measured in verification/w7/chat.json): an answer of exactly ONE line that
     * the token ceiling cut off. The truncation rule drops the last line,
     * correctly, because half a sentence has no room left for its citation. When
     * the answer WAS that line, the rule drops the whole answer, and the result
     * reads like a perfect record: no violations, no citations, nothing.
     *
     * Zero violations out of zero lines is not compliance, it is an absent
     * measurement, and the two must not be added up. A consumer that averages
     * citation compliance therefore leaves an unmeasured check out of the
     * average and reports how many it left out, rather than counting it as a
     * clean answer (which flatters the model) or as a violation (which punishes
     * it for the ceiling). {@link ok} is unchanged and still false here: an
     * answer nobody could check has not kept the contract either.
     */
    measured: boolean;
}

/** Knobs for the check. Only one, and it is about the generation, not the model. */
export interface CitationOptions {
    /**
     * True when the generation stopped at the token ceiling.
     *
     * The last line of a truncated answer is half a sentence, and half a
     * sentence has no room left for its citation. Scoring it as an uncited claim
     * would measure the ceiling rather than the model, so the last line is
     * dropped from the check when this is set. Everything before it is still
     * checked in full: truncation excuses one line, not an answer.
     */
    truncated?: boolean;
}

/**
 * Check one answer against the cards it was given.
 *
 * `cardIds` is the list as the compiler emitted it (`K1`, `K2`, ...). An empty
 * list means the model was given nothing, in which case the only answer that
 * keeps the contract is the no-card sentence.
 */
export function checkCitations(
    answer: string,
    cardIds: readonly string[],
    options: CitationOptions = {},
): CitationCheck {
    const known = new Set(cardIds);
    const all = claimLines(answer);
    const lines = options.truncated === true && all.length > 0 ? all.slice(0, -1) : all;
    const violations: CitationViolation[] = [];
    const cited: string[] = [];
    const unknown: string[] = [];
    const seenCited = new Set<string>();
    const seenUnknown = new Set<string>();

    for (const line of lines) {
        const ids = citationsIn(line);
        if (ids.length === 0) {
            violations.push({ line, reason: 'no-citation' });
            continue;
        }
        const bad = ids.filter((id) => !known.has(id));
        for (const id of ids) {
            if (known.has(id) && !seenCited.has(id)) {
                seenCited.add(id);
                cited.push(id);
            }
        }
        for (const id of bad) {
            if (!seenUnknown.has(id)) {
                seenUnknown.add(id);
                unknown.push(id);
            }
        }
        if (bad.length > 0) {
            violations.push({ line, reason: 'unknown-card', unknown: bad });
        }
    }

    const trimmed = answer.trim();
    const noCardOnly = trimmed.length > 0
        && all.length === 0
        && isNoCardLine(trimmed);

    /*
     * Whether anything was looked at. See {@link CitationCheck.measured}.
     *
     * The condition is the same one `ok` already carried; it is named here
     * because the two questions it answered were never the same question. "Are
     * there violations" and "was there anything to violate" only look alike
     * while every answer has at least two lines.
     */
    const measured = lines.length > 0 || noCardOnly;

    return {
        ok: violations.length === 0 && measured,
        cited,
        unknown,
        violations,
        noCardOnly,
        measured,
    };
}

/** One renderable piece of an answer: plain text or a citation button. */
export type AnswerSegment =
    | { kind: 'text'; text: string }
    | { kind: 'citation'; cardId: string; text: string; known: boolean };

/**
 * Split one line into text and citations, for rendering.
 *
 * The panel needs the citations as buttons and the rest as text, and it needs to
 * know which citations name a card that exists: an unknown one is rendered as
 * plain text with a warning rather than as a button that would navigate nowhere.
 */
export function segmentsOf(line: string, cardIds: readonly string[]): AnswerSegment[] {
    const known = new Set(cardIds);
    const out: AnswerSegment[] = [];
    const pattern = /\[[^\]]{1,80}\]/g;
    let cursor = 0;
    let match = pattern.exec(line);
    while (match !== null) {
        const ids = citationsIn(match[0]);
        if (ids.length === 0) {
            match = pattern.exec(line);
            continue;
        }
        if (match.index > cursor) {
            out.push({ kind: 'text', text: line.slice(cursor, match.index) });
        }
        for (const id of ids) {
            out.push({ kind: 'citation', cardId: id, text: `[${id}]`, known: known.has(id) });
        }
        cursor = match.index + match[0].length;
        match = pattern.exec(line);
    }
    if (cursor < line.length) {
        out.push({ kind: 'text', text: line.slice(cursor) });
    }
    return out;
}
