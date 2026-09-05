/**
 * What Enter means in the command line, now that the line does two jobs.
 *
 * PLAN paragraph 4 says the footer is the command palette and the chat entry in
 * one, and W3 already made it a meaning search. So one key has to serve two
 * behaviours without either of them becoming unreliable, and the contract is
 * explicit about which one wins: **the search keeps priority.** A reader who
 * typed three letters and sees the symbol they wanted must still be able to
 * press Enter and land in it, exactly as before this cycle.
 *
 * That leaves the question of what "free text" is, and it is decided here by two
 * signals rather than by a mode switch:
 *
 * **A line that ends with a question mark is a question.** It is the one mark
 * that nobody types into a symbol search, in either language, and it means the
 * same thing in both. "Wer ruft createUser?" is a question even though the
 * search would happily find `createUser`; without this rule the flagship
 * question of this cycle would navigate instead of asking.
 *
 * **A line that starts with `@` is a question.** The answer contract tells the
 * model to say "fetch it with @name" when a card is missing, so `@name` has to
 * do something when the reader types it back.
 *
 * **A line the search answered with nothing is a question.** This is the free
 * text of the contract in its literal sense: text that names no symbol. The
 * search must have finished for this to fire, which is why {@link CommandState}
 * carries `answered`: sending a question because a debounce has not elapsed
 * would ask the model about a line the reader is still typing.
 *
 * Everything else is what it was before: Enter picks the selected hit.
 */

/** What pressing Enter should do. */
export type CommandIntent =
    /** Open the selected search hit. The behaviour W3 proved. */
    | 'search-hit'
    /** Send the line to the atlas as a question. */
    | 'ask'
    /** Do nothing: nothing is typed, or the search has not answered yet. */
    | 'nothing';

/** What the command line knows about itself at the moment Enter is pressed. */
export interface CommandState {
    line: string;
    /** How many hits the meaning search has for this line. */
    hitCount: number;
    /** True when the search has finished for exactly this line. */
    answered: boolean;
}

/** Whether the shape of the line marks it as a question on its own. */
export function looksLikeQuestion(line: string): boolean {
    const trimmed = line.trim();
    return trimmed.endsWith('?') || trimmed.startsWith('@');
}

/** Decide what Enter does. Pure, and the only place that decides it. */
export function commandIntent(state: CommandState): CommandIntent {
    const trimmed = state.line.trim();
    if (trimmed.length === 0) {
        return 'nothing';
    }
    if (looksLikeQuestion(trimmed)) {
        return 'ask';
    }
    if (state.hitCount > 0) {
        return 'search-hit';
    }
    return state.answered ? 'ask' : 'nothing';
}
