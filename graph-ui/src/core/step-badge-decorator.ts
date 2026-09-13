/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-core/src/browser/step-badge-decorator.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen:
 * STEP_BADGE_CLASS, STEP_BADGE_PULSE_CLASS, MAX_STEP_BADGES, StepBadge und
 * badgesForLines samt Deckel, Ein-Badge-je-Zeile-Regel und Sortierung.
 *
 * Nicht mitgekommen ist die Klasse `CodeAtlasStepBadgeDecorator`: sie ist ein
 * `@injectable()` ueber Theias `EditorDecorator` und `TextEditor`, also die
 * Verdrahtung an eine Werkbank, die es hier nicht gibt. Ihr Inhalt ist nicht
 * verlorengegangen, sondern liegt in src/reader/step-badges.ts als
 * Monaco-Adapter: dieselben Klassennamen, dieselbe leere Ein-Zeilen-Spanne,
 * dieselbe Regel, dass die Ziffer aus dem Stylesheet kommt. Die REGEL, welche
 * Zeile welche Nummer bekommt, steht nur hier, damit Panel und Rand sich nicht
 * ueber Schritt drei uneinig werden koennen.
 */

/**
 * Numbered badges in the editor gutter, one per call site of the focused symbol.
 *
 * The twin already lists what a symbol does, in order. Until now that list and
 * the code sat side by side with nothing joining them: the reader had to match
 * `validateUser` in the panel against `validateUser` somewhere in thirty lines
 * of body, and do it again for every step. The badges are that join, drawn where
 * the eye already is.
 *
 * Two decisions are load-bearing.
 *
 * **The number is the step's position, and it stops at nine.** Past nine a
 * two-digit badge is wider than the gutter it lives in, and a reader counting
 * past nine call sites is reading the panel rather than the margin. Later sites
 * carry no badge at all rather than a wrong one or a truncated one; the twin
 * still lists them, so nothing is hidden, only unnumbered.
 *
 * **One badge per line, never two.** Two calls on the same line would otherwise
 * draw two badges into the same few pixels and the reader would see one of them
 * with no way to tell which. The earlier step wins, because the badges number
 * the order the body reaches things and the earlier one is the one the reader
 * arrives at first.
 */

/** Class every badge carries, which is also how a driver counts them. */
export const STEP_BADGE_CLASS = 'codeatlas-step-badge';

/** Added to the badge whose line the caret is currently on. */
export const STEP_BADGE_PULSE_CLASS = 'codeatlas-step-badge-pulse';

/**
 * Highest number a badge can show.
 *
 * Nine because the gutter is sized for one digit. Steps past this are listed in
 * the twin like every other step and simply carry no badge, which is the honest
 * shape: an unnumbered call site says "not one of the first nine", where a badge
 * reading `9` on the twelfth site would say something false.
 */
export const MAX_STEP_BADGES = 9;

/** One numbered call site inside the focused symbol's own file. */
export interface StepBadge {
    /** 1-based graph line of the call site. */
    line: number;
    /** 1-based position of the step in the twin's ordered list. */
    ordinal: number;
    /** What is called there, for the badge's hover. */
    label?: string;
}

/**
 * The badges for one ordered list of call sites.
 *
 * Pure, and the only place the cap and the one-per-line rule are applied, so a
 * caller cannot produce a badge set that disagrees with the twin about which
 * step is number three.
 */
export function badgesForLines(sites: readonly { line?: number; label?: string }[]): StepBadge[] {
    const byLine = new Map<number, StepBadge>();
    sites.forEach((site, index) => {
        const line = site.line;
        if (line === undefined || line < 1 || index >= MAX_STEP_BADGES) {
            return;
        }
        // The earlier step keeps the line: the numbers follow the order the body
        // reaches things, so the first arrival is the one the reader meets.
        if (!byLine.has(line)) {
            byLine.set(line, { line, ordinal: index + 1, label: site.label });
        }
    });
    return [...byLine.values()].sort((left, right) => left.line - right.line);
}
