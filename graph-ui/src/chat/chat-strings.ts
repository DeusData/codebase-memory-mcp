/**
 * Everything the chat says in its own voice.
 *
 * A separate file for the same reason twin/strings.ts and llm/strings.ts are
 * separate: these sentences are the contract with the reader, the proof run
 * reads them word for word, and they should be checkable without the state
 * machine next to them.
 *
 * The rule behind all of them: the panel says what happened, and when nothing
 * happened it says that instead of showing an empty box. "The model was not
 * asked" and "the model had nothing to say" are different sentences here.
 */

import { SIDECAR_PORT } from '../llm/sidecar';

/** The word over the panel. */
export const CHAT_TITLE = 'ATLAS_CHAT';

/** What the command line hint says when a question would be sent. */
export const CHAT_HINT_READY = 'enter asks the atlas';

/** What it says when the line looks like a question and the model is off. */
export const CHAT_HINT_OFF = 'enter asks the atlas: the local model is off';

/** The sentence when the model is off and a question was typed. */
export const CHAT_OFF_MESSAGE =
    `the local model is off, so nothing was sent to 127.0.0.1:${SIDECAR_PORT}. `
    + 'Turn it on in the [a]tlas menu or on the LOCAL LLM card, then ask again.';

/** The model is off, but the index-built cards still answer the question. */
export const CHAT_OFF_WITH_CARDS =
    `the local model is off, so the answer below is built from the cards and nothing was sent to 127.0.0.1:${SIDECAR_PORT}.`;

/** The compiler still answered; this only names why its optional model pass did not run. */
export function chatBuiltUnavailableMessage(reason: 'off' | 'not-running' | 'policy'): string {
    switch (reason) {
        case 'policy':
            return 'the local model is denied by the project policy, so the answer below is built from the cards and nothing was sent.';
        case 'not-running':
            return `the local model is on, but nothing answers on 127.0.0.1:${SIDECAR_PORT}; the answer below is built from the cards.`;
        default:
            return CHAT_OFF_WITH_CARDS;
    }
}

export const CHAT_AI_LABEL = 'AI version';
export const CHAT_AI_RESTORE = 'built version';
export const CHAT_BUILT_PROVENANCE = 'built from indexed cards and their citations.';
export function chatAiProvenance(model: string): string {
    return `worded by local model ${model}; every required fact passed the rewrite guard.`;
}

/** The sentence when the model is on but no process answers. */
export const CHAT_NOT_RUNNING_MESSAGE =
    `the local model is on, but nothing answers on 127.0.0.1:${SIDECAR_PORT}. `
    + 'Start the sidecar with llm/start.sh, then ask again.';

/** The sentence when a policy denies the model. */
export const CHAT_POLICY_MESSAGE =
    'the local model is denied by the project policy, so nothing was sent.';

/** The sentence while the compiler is working. */
export const CHAT_COMPILING = 'compiling the cards from the index ...';

/** The sentence while the model is answering. */
export const CHAT_ASKING = 'the local model is phrasing an answer from the cards ...';

/** Why the agreed sentence is there, under it. */
export const CHAT_NO_CARDS_HINT =
    'The index holds nothing this question could be answered from, so the model was not asked. '
    + 'Name a symbol with @name to fetch its facts.';

// ------------------------------------------------- the name and the symbol --

/**
 * The line over a candidate list.
 *
 * It says the number, because two candidates and eleven are different problems,
 * and it says what a click does, because a list without that is a list.
 */
export function chatChoiceHeadline(name: string, count: number): string {
    return `"${name}" names ${count} symbols in this index. Pick the one you meant and the same `
        + 'question is asked again about it; nothing was sent to the model in the meantime.';
}

/** One candidate, as the list writes it: the name, the file, the line. */
export function chatCandidateLabel(
    name: string,
    filePath: string | undefined,
    line: number | undefined,
): string {
    const where = filePath === undefined || filePath.length === 0
        ? 'no file in the index'
        : line === undefined ? filePath : `${filePath}:${line}`;
    return `${name} (${where})`;
}

/** The tooltip of a candidate. It names the act and not the thing. */
export function chatCandidateTitle(name: string): string {
    return `ask this question again about ${name}`;
}

/** The mark on a candidate the index flagged as test code. */
export const CHAT_CANDIDATE_TEST_MARK = 'test';

/**
 * The line that says the answer is about something else than what was asked.
 *
 * Its own line, above the answer and not under it, and in the product's voice
 * rather than the model's: the model was never told that the name failed, so it
 * cannot be the one to say it. A reader who sees "createUser" in an answer to
 * "@createuser" has to be able to see, in the same glance, that the two are the
 * same symbol and how the compiler got from one to the other.
 */
export function chatFocusFallbackLine(asked: string, used: string): string {
    return `"@${asked}" was not found in the index; answered about the symbol in focus instead: `
        + `${used}.`;
}


/** The line over the cards, which fold open. */
export function chatCardsLabel(count: number): string {
    return count === 1 ? 'the 1 card this answer was given' : `the ${count} cards this answer was given`;
}

/** The honest line under the answer, naming the class and the rule that fired. */
export function chatProvenance(klass: string, rule: string, tokens: number, budget: number): string {
    return `question class ${klass} (rule ${rule}); the cards cost about ${tokens} of ${budget} `
        + 'tokens, estimated at four characters per token.';
}

/** What the panel says when a claim came back without a citation. */
export function chatCitationWarning(count: number): string {
    return count === 1
        ? '1 line of this answer carries no card citation and is shown as the model wrote it.'
        : `${count} lines of this answer carry no card citation and are shown as the model wrote it.`;
}

/** What it says when the model cited a card that was never given. */
export function chatUnknownCardWarning(ids: readonly string[]): string {
    return `this answer cites ${ids.join(', ')}, which was never handed over. `
        + 'Nothing was navigated to and nothing was believed.';
}

/** What it says when a model returned only its own monologue. */
export const CHAT_THOUGHT_ONLY =
    'the model returned only its internal monologue and no answer. Nothing of it is shown: '
    + 'a monologue is not a finding.';

/** What it says when the token ceiling cut the answer off. */
export const CHAT_TRUNCATED =
    'the answer stopped at the token limit, so its last line is incomplete. It is shown as it '
    + 'arrived and is not counted as a claim.';

/** What it says when the request failed. */
export function chatFailed(detail: string): string {
    return `the local model could not be asked: ${detail}`;
}

// ------------------------------------------------ the neighbourhood setting --

/** The label of the depth control. */
export const CHAT_DEPTH_LABEL = 'context';

/** The three options, in order. */
export const CHAT_DEPTH_OPTIONS: readonly { value: 0 | 1 | 2; label: string; title: string }[] = [
    {
        value: 0,
        label: '0 hops',
        title: 'the focus symbol alone: no caller and no callee goes into the cards',
    },
    {
        value: 1,
        label: '1 hop',
        title: 'the focus symbol plus its direct callers and callees. The default.',
    },
    {
        value: 2,
        label: '2 hops',
        title: 'the focus symbol plus two rings of neighbours. More context, more to confuse.',
    },
];

/**
 * The honest note under the depth control.
 *
 * It says what a wider context does, not what it improves. More neighbours is
 * more for a small model to mix up, and the token budget cuts the far ones
 * anyway; a control labelled "better answers" would be selling the setting
 * rather than describing it.
 *
 * The last sentence is the one a reader would otherwise have to guess: changing
 * the depth does not re-ask anything. An answer that silently changed under a
 * setting would be an answer nobody could quote afterwards, and the whole panel
 * is built so that a citation still means what it meant when it was written.
 */
export const CHAT_DEPTH_NOTE =
    'How much of the neighbourhood goes into the cards. More neighbours change the answer and '
    + 'can make it worse: a small model has more to confuse, and the token budget cuts the '
    + 'furthest cards first.';

/**
 * The offer on the last answer when the depth has been changed since.
 *
 * The reader's finding of 2026-08-29: "wenn ich 1 hop, 2 hop, 3 hop klicke,
 * kein anderer Text, wieso?" The setting applies to the NEXT question on
 * purpose, because an answer that changed under the hand would void its own
 * citations. That sentence stood in the head of the panel and was read past.
 *
 * So the explanation becomes an offer, at the answer it is about. Pressing it
 * asks the same question again as a NEW turn; the old turn stays with its old
 * depth, so the two can be held against each other and every citation still
 * means what it meant. The same pattern W7c introduced for picking between
 * ambiguous candidates.
 */
export function chatRerunLabel(depth: number): string {
    return depth === 1 ? 'ask again with 1 hop' : `ask again with ${depth} hops`;
}

/** Why the offer is there, on the button itself. */
export function chatRerunTooltip(was: number, now: number): string {
    return `This answer was compiled with ${was === 1 ? '1 hop' : `${was} hops`} of context and the `
        + `setting now says ${now === 1 ? '1 hop' : `${now} hops`}. Asking again makes a new turn; `
        + 'this one stays as it is, so the two can be compared.';
}

/**
 * What the fold over the cards says, in both states.
 *
 * Until W8b it said `[-]` and `[+]`. The reader did not understand the signs
 * when testing the galaxy panel on 2026-08-29, and the same two signs were used
 * here. A word in front of the count answers the question the sign asked.
 */
export function chatCardsFoldLabel(open: boolean): string {
    return open ? 'collapse' : 'open';
}

// -------------------------------------------------------------- the refine --

/** The label on the refine button in the pseudocode view. */
export const REFINE_LABEL = 'rephrase';

/** Its tooltip when it can be pressed. */
export const REFINE_TITLE =
    'let the local model rewrite these lines. Every number and every line must survive, or the '
    + 'rewrite is refused and the deterministic block stays.';

/** The label that brings the deterministic block back. */
export const REFINE_RESTORE_LABEL = 'original';

/** Its tooltip. */
export const REFINE_RESTORE_TITLE = 'show the block as the index produced it';

/** What the view says while the model works. */
export const REFINE_RUNNING = 'the local model is rephrasing these lines ...';

/** What it says when the rewrite was accepted. */
export const REFINE_APPLIED =
    'rephrased by the local model. Every line and every number is the one the index produced.';

/** What it says when the rewrite was refused, with the reason. */
export function refineRejected(reason: string): string {
    return `the rewrite was refused and the original block is still shown. Reason: ${reason}`;
}

/** The three reasons a rewrite is refused. Read by the proof run. */
export const REFINE_REASON_COUNT = 'the answer holds a different number of lines than were sent';
export const REFINE_REASON_NUMBERS = 'a line came back with a different number than it was sent with';
export const REFINE_REASON_EMPTY = 'the model returned nothing to put back';
