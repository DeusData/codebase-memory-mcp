/**
 * The prompt: what the model is allowed to do, and the cards it may do it with.
 *
 * Everything above this file is deterministic. This is the one place where the
 * product hands something to a process it does not control, so it is also the
 * one place where the rules have to be written down in a way a 1B model can
 * follow. Four decisions carry it.
 *
 * **The rules are numbered and short.** A small model follows eleven numbered
 * imperatives far better than it follows a paragraph, and the eval measures
 * exactly that: an answer that breaks rule one is red no matter how right it is.
 *
 * **The only concrete line is a template nobody can mistake for an answer.**
 * Every finished sentence this prompt ever carried came back in the answers
 * verbatim, about the wrong symbols. The long note on {@link SYSTEM_PROMPT}
 * lists all six measured attempts and what each one cost.
 *
 * **The cards are the whole world.** They arrive under a heading that says so,
 * and the honest block underneath names what could not be fetched. A gap that
 * were left out of the prompt would be a gap the model fills in itself, which is
 * exactly the failure the citation rule exists to catch.
 *
 * **Thinking is switched off, per model, measured rather than assumed.**
 * {@link nonThinkingFor} says how, and the measurements behind it are in the
 * table on that function. Every one of the six candidates was asked the same
 * question against the running llama-server before this table was written.
 */

import type { Card, CardSet } from './card-compiler';
import { renderCards } from './card-compiler';
import type { QuestionClass } from './question-classifier';

/**
 * What the answer must look like, in the model's own second person.
 *
 * Every rule below is here because a candidate broke it in a measured run, and
 * the note says which. None of them is general prompt wisdom; the eval passes
 * that produced them are recorded in tools/eval-llm.mjs under `ITERATIONS`.
 *
 *  - **Rule 3** (no heading, no closing line) came from models that wrote "The
 *    symbols that call listUsers are:" and "No other symbol is mentioned." Both
 *    are lines with no card behind them, so both are violations, and both were
 *    the model being helpful rather than the model being wrong.
 *  - **Rule 5** (copy names exactly) came from `userCreate`, which one model
 *    wrote where the card said `createUser`. A rule against inventing behaviour
 *    did not read as a rule against re-spelling a name.
 *  - **Rule 6** (never repeat a line) and the repeat penalty in the chat client
 *    came from four answers that degenerated into one sentence repeated until
 *    the token ceiling cut it in half.
 *  - **Rule 9** (answer only what was asked) came from an answer that listed
 *    every card it had been given, callers and callees alike, for a question
 *    about callers.
 *
 * ## The one concrete line, and the five versions of it that failed
 *
 * The template above the rules is the only finished-looking line in this
 * prompt, and rule 2 exists to stop even that from being copied. It reads as it
 * does because five earlier versions were measured and rejected:
 *
 *  1. a worked example built from the demo fixture's own names: the smaller
 *     candidates put `createUser is called by registerUserRoutes` into answers
 *     about entirely different symbols, citations and all;
 *  2. the same example with placeholder names: they copied
 *     `alphaWorker is called by betaHandler at line 12 [K1]` verbatim;
 *  3. no example at all, the shape stated only as a rule: citation compliance
 *     fell from 0.84 to 0.23, because with no shape to follow they copied the
 *     shape of the cards and wrote `K2: registerUserRoutes ...`, where the
 *     number is not a citation;
 *  4. the labels named inside a sentence ("[K1], [K2] and so on") instead of a
 *     template: 0.36 citation compliance, because fluent prose with no trailing
 *     bracket is the easier continuation of a sentence;
 *  5. the fallback sentence quoted in full in rule 7: models appended
 *     `No card covers this. Fetch it with @name.` to answers that had four
 *     perfectly good cited lines above them. It is now described, not quoted,
 *     and the checker recognises the fallback by its marker in either language.
 *
 * What makes the sixth version work is not in this file at all: {@link renderCard}
 * labels every card `[K1]`, `[K2]`, in the exact syntax a citation uses, so the
 * cheapest possible continuation of the card list is already a compliant
 * citation. The template only has to point at that.
 */
export const SYSTEM_PROMPT = [
    'You answer questions about a codebase. The numbered cards in the user message are',
    'everything you know. They were compiled from a code index; they are not a guess.',
    '',
    'Every line of your answer has this shape, with your own words in the slots:',
    '',
    '    <one short sentence taken from a card> [K<number of that card>]',
    '',
    'Rules:',
    '1. The card number always stands in square brackets at the end of the line.',
    '   A number without square brackets is not a citation and does not count.',
    '2. Never write an angle bracket or the words inside one. They mark the slots.',
    '3. Write only fact lines. No heading, no introduction, no closing remark, no summary.',
    '4. Use only card numbers that appear in the list below. Never invent one.',
    '5. Copy every name, file and line number exactly as the card spells it.',
    '6. Never write the same line twice.',
    '7. If none of the cards answers the question, write one single line saying that no',
    '   card covers it, and write nothing else at all.',
    '8. No advice, no opinions, no code, no suggestions. Only what is on the cards.',
    '9. Answer only what was asked. Leave out cards that are about something else.',
    '10. Name every card that answers the question, not just the first one.',
    '11. Answer in the language of the question. At most four short lines.',
].join('\n');

/** The heading over the cards, so the model can tell them from the question. */
export const CARDS_HEADING = 'CARDS (this is everything you know):';

/** The heading over what could not be fetched. */
export const GAPS_HEADING = 'NOT AVAILABLE (do not treat these as facts):';

/** The heading over the question. */
export const QUESTION_HEADING = 'QUESTION';

/**
 * One sentence per class, telling the model which shape of answer is wanted.
 *
 * Not a second set of rules: a nudge towards the family of fact the reader asked
 * for, so a caller question is not answered with a list of callees that happens
 * to be on a card.
 */
export const CLASS_HINTS: Readonly<Record<QuestionClass, string>> = {
    'what-is':
        'Name the symbols it calls, with their lines, from the card that lists them. Not the counts.',
    'who-calls':
        'Name every symbol a card says CALLS it, one per line, with the line number. '
        + 'Ignore cards about what it calls itself.',
    'what-if':
        'Name every symbol a card says calls it, one per line: those are what a change would reach.',
    'where-entry': 'Name only the entry points and routes the cards list, with their files.',
    'why-error':
        'Name only the error types a card says can be raised, and where. If no card names one, say so.',
    compare: 'One line per symbol: what each one calls and what each one raises.',
    overview: 'State the counts, the languages and the groups from the overview card.',
    other: 'Answer only if a card covers the question.',
};

/** What a prompt plan carries into the client. */
export interface PromptPlan {
    system: string;
    user: string;
    /** The cards, in the order they were numbered, for the panel and the check. */
    cards: Card[];
    /** Card ids, for the citation check. */
    cardIds: string[];
    /** Estimated tokens of the whole prompt. */
    estimatedTokens: number;
    /** How thinking is switched off for the chosen model. */
    nonThinking: NonThinkingPlan;
}

/**
 * How one model is put into non-thinking mode.
 *
 * `chat_template_kwargs` is what llama-server passes into the jinja chat
 * template; `enable_thinking: false` is the switch Qwen3.5, MiniCPM5 and Gemma
 * read. Two of the six candidates never think, and for them the same field is
 * inert, which was measured rather than assumed: the answer bytes are identical
 * with and without it. So there is one code path and not six, and the table on
 * {@link NON_THINKING_TABLE} records what each model actually did.
 */
export interface NonThinkingPlan {
    /** Sent as `chat_template_kwargs` in the request body. */
    chatTemplateKwargs: Record<string, unknown>;
    /** What this model does without the switch, as measured on 2026-08-29. */
    note: string;
}

/**
 * Measured against llama-server b10675 on 2026-08-29, one question each, with
 * and without `chat_template_kwargs {"enable_thinking": false}`, and with
 * `/no_think` in the system prompt as a third arm.
 *
 * The result decided the single code path: `/no_think` in the system prompt
 * changed nothing for any of the three thinking models (their content stayed
 * empty and `reasoning_content` stayed full), and the template switch worked for
 * all three. For the two non-reasoning models the switch produced byte-identical
 * answers, so it is sent unconditionally.
 */
export const NON_THINKING_TABLE: Readonly<Record<string, string>> = {
    'Qwen3.5-2B':
        'thinks by default (content empty, reasoning_content 410 chars); '
        + 'enable_thinking:false moves the answer into content; /no_think has no effect.',
    'Qwen3.5-4B':
        'thinks by default (content empty, reasoning_content 411 chars); '
        + 'enable_thinking:false moves the answer into content; /no_think has no effect.',
    'MiniCPM5-1B':
        'thinks by default (content empty, reasoning_content 473 chars); '
        + 'enable_thinking:false moves the answer into content; /no_think has no effect.',
    'gemma-4-E4B':
        'thinks by default (content empty, reasoning_content 423 chars); '
        + 'enable_thinking:false moves the answer into content; /no_think has no effect.',
    'LFM2.5-1.2B':
        'never thinks: content is filled and no reasoning_content key is returned; '
        + 'enable_thinking:false leaves the answer byte-identical.',
    'Qwen2.5-Coder-1.5B':
        'never thinks: content is filled and no reasoning_content key is returned; '
        + 'enable_thinking:false leaves the answer byte-identical.',
};

/** The one switch, plus whatever the table knows about this model. */
export function nonThinkingFor(modelName: string): NonThinkingPlan {
    const key = Object.keys(NON_THINKING_TABLE).find(
        (name) => modelName === name || modelName.startsWith(name),
    );
    return {
        chatTemplateKwargs: { enable_thinking: false },
        note: key === undefined
            ? 'not measured for this model; the template switch is sent regardless, which is '
                + 'inert for a model whose template does not read it.'
            : NON_THINKING_TABLE[key],
    };
}

/** What building a prompt needs. */
export interface PromptInput {
    question: string;
    klass: QuestionClass;
    cards: CardSet;
    /** The model the prompt is for, so the non-thinking switch can be named. */
    modelName: string;
}

/**
 * Build the two messages.
 *
 * Pure: the same question and the same cards always produce the same bytes,
 * which is what makes a temperature-zero run with a fixed seed reproducible.
 */
export function buildPrompt(input: PromptInput): PromptPlan {
    const cards = input.cards.cards;
    const cardIds = cards.map((card) => card.id);
    const parts: string[] = [CARDS_HEADING, renderCards(cards)];
    if (input.cards.gaps.length > 0) {
        parts.push('', GAPS_HEADING, ...input.cards.gaps.map((gap) => `- ${gap}`));
    }
    parts.push('', QUESTION_HEADING, input.question, '', CLASS_HINTS[input.klass]);
    const user = parts.join('\n');
    const system = SYSTEM_PROMPT;
    return {
        system,
        user,
        cards,
        cardIds,
        estimatedTokens: Math.ceil((system.length + user.length) / 4),
        nonThinking: nonThinkingFor(input.modelName),
    };
}

/**
 * The refine prompt: rewrite a deterministic block, keep every number.
 *
 * A different contract from the answer above and deliberately narrower. There is
 * nothing to cite here, because every line already is a citation: the block was
 * built from the index and the model is only allowed to rephrase it. The
 * validator on the way back (`applyRefinedPseudocode`) checks that promise
 * position by position, so the prompt only has to make keeping it easy.
 */
export const REFINE_SYSTEM_PROMPT = [
    'You rewrite a numbered list of findings about code so it reads better.',
    '',
    'Rules:',
    '1. Return exactly as many lines as you were given, in the same order.',
    '2. A line that starts with a number keeps that number and the dot after it.',
    '3. A line that starts with no number must still start with no number.',
    '4. Never add a line, never drop a line, never merge two lines.',
    '5. Never add a fact, a name, a file or a number that is not already in the line.',
    '6. Return the lines and nothing else: no heading, no commentary, no code fence.',
].join('\n');

/** The user half of the refine prompt. */
export function buildRefinePrompt(blockText: string): string {
    return [
        `Rewrite these ${blockText.split('\n').length} lines. Same count, same order, same numbers.`,
        '',
        blockText,
    ].join('\n');
}

/**
 * The reader prompt: word the same sentences for the person reading them.
 *
 * The third contract in this file and the narrowest of the three. There is
 * nothing to cite, because every sentence handed over was assembled from the
 * index; there is nothing to select, because the level already selected; there
 * is nothing to add, because everything the model could add would be something
 * nobody recorded. All that is left is the wording, and that is the one thing a
 * 1B model is genuinely good at.
 *
 * The rules are the refine rules with two changes, and both are forced by what
 * is being rewritten. Sentences are not a numbered list, so rule 2 of the
 * refine prompt has nothing to hold onto and its job is done by rule 5 here:
 * every name, number, file and line survives, in order. And the reader is named
 * out loud, because "who is this for" is the entire reason the rewrite is
 * happening; without it the model averages towards the middle of its training
 * data, which is a competent senior, and the vibe coder gets a paragraph about
 * resolution strategies.
 */
export const READER_SYSTEM_PROMPT = [
    'You reword sentences about code so they read well for one particular reader.',
    'The sentences were assembled from a code index. Every fact in them is already correct.',
    '',
    'Rules:',
    '1. Return exactly as many lines as you were given, in the same order.',
    '2. One rewritten sentence per line. Never merge two, never split one.',
    '3. Never add a fact, a name, a file, a number or a claim that is not already in the line.',
    '4. Never drop a name, a file or a number that is in the line.',
    '5. Keep every name, file and number spelled exactly as it is, and in the same order.',
    '6. Say nothing about what the code should do, only what the line already says it does.',
    '7. Keep each line about as long as it was. Do not explain, do not add an example.',
    '8. Return the lines and nothing else: no heading, no commentary, no code fence.',
].join('\n');

/**
 * One sentence per reader, telling the model whose ear it is writing for.
 *
 * Keyed by the reader's name as the product spells it, so the panel can pass
 * the label straight through and there is no second table mapping levels to
 * names. An unknown key falls back to no voice line at all rather than to a
 * guess: a rewrite with no voice hint is still a legal rewrite, and inventing
 * a reader would be the one invention this whole file exists to prevent.
 */
export const READER_VOICES: Readonly<Record<string, string>> = {
    'vibe coder':
        'The reader is new to this code and wants to know what happens here and why it matters to them. '
        + 'Use plain words. Avoid jargon you can avoid. Keep every name exactly as it is written.',
    junior:
        'The reader is learning this codebase and wants the order things happen in. Keep the sequence '
        + 'obvious. Explain nothing that is not already explained in the line.',
    medior:
        'The reader knows the language and wants the recorded facts without groundwork. Be direct and '
        + 'short. No encouragement, no summary.',
    senior:
        'The reader is deciding what a change here will cost them. Lead with the consequence. No hedging '
        + 'and no reassurance.',
    architect:
        'The reader is judging what this rests on and where the knowledge ends. Be precise about what is '
        + 'known and what is not. Never soften a limit.',
};

/** The user half of the reader prompt. */
export function buildReaderPrompt(reader: string, text: string): string {
    const lines = text.split('\n').length;
    const voice = READER_VOICES[reader];
    return [
        `Reword these ${lines} sentences for a ${reader}. Same count, same order, same names and numbers.`,
        ...(voice === undefined ? [] : ['', voice]),
        '',
        text,
    ].join('\n');
}
