/**
 * One question, end to end: classify, fetch, compile, ask, check.
 *
 * The five steps live in five files and this one puts them in order. It exists
 * so there is exactly one path from a typed sentence to a rendered answer, and
 * so the eval can walk that same path without a browser. A second orchestration
 * for the measurement would measure something the product does not do.
 *
 * ## The order is the argument
 *
 * 1. **classify** (no model, no round trip)
 * 2. **fetch** the class's recipe over the provider, bounded by the depth
 * 3. **compile** the facts into numbered cards, bounded by the token budget
 * 4. **ask**, but only if there is at least one card
 * 5. **check** every claim line against the cards that were given
 *
 * Step 4 is conditional and that is the honest half of the design. A question
 * the index cannot answer produces no cards, and a model asked with no cards can
 * only invent. So it is not asked: the agreed sentence is returned without a
 * request, which is both cheaper and truer than teaching a 1B model to refuse.
 *
 * Since W7c there is a second reason not to ask: the name in the question
 * reached several symbols equally well. The model cannot resolve that, because
 * the ambiguity is in the index and not in the sentence. The turn comes back as
 * `needs-choice` with the candidates, the reader picks one, and the same
 * question is asked again with the picked qualified name.
 *
 * Step 5 never rewrites the answer. A line without a citation is shown as the
 * model wrote it, with a warning above it, because hiding it would hide the
 * failure the eval exists to count.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { ProviderQueryOptions } from '../core/intelligence-provider';
import { compileFacts } from '../compiler/fact-recipes';
import type {
    FactPacket,
    FocusFallback,
    NeighborDepth,
    ObservedReader,
    RecipeSource,
    SubjectChoice,
} from '../compiler/fact-recipes';
import { NEIGHBOR_DEPTH_DEFAULT } from '../compiler/fact-recipes';
import { cardBudgetOf, compileCards } from '../compiler/card-compiler';
import type { Card, CardSet, ModelClass } from '../compiler/card-compiler';
import { buildPrompt } from '../compiler/prompt-contract';
import type { PromptPlan } from '../compiler/prompt-contract';
import { checkCitations, NO_CARD_SENTENCE } from '../compiler/answer-contract';
import type { CitationCheck } from '../compiler/answer-contract';
import type { ClassifierContext, QuestionClass } from '../compiler/question-classifier';
import { askModel, ChatError } from './chat-client';
import type { ChatFetch } from './chat-client';

/** How a turn ended. Every value is a different sentence in the panel. */
export type TurnStatus =
    /** The compiler is fetching. */
    | 'compiling'
    /** The model is answering. */
    | 'asking'
    /** An answer came back and was checked. */
    | 'answered'
    /** The name reached several symbols, so the reader is asked which one. */
    | 'needs-choice'
    /** No card covered the question, so nothing was asked. */
    | 'no-cards'
    /** The model was not asked because it is off, denied or not running. */
    | 'refused'
    /** The request failed. */
    | 'failed';

/** Why a turn was refused without being sent. */
export type RefusalReason = 'off' | 'not-running' | 'policy';

/** One question and everything that happened to it. */
export interface ChatTurn {
    id: number;
    question: string;
    status: TurnStatus;
    /** The class the classifier chose, once it has run. */
    klass?: QuestionClass;
    /** Which rule fired, for the honest line under the answer. */
    rule?: string;
    /** The neighbourhood depth this turn used. */
    depth: NeighborDepth;
    /** The cards the model was given. Empty until they are compiled. */
    cards: Card[];
    /** Tokens the cards cost, and the budget they had. */
    tokens?: number;
    budget?: number;
    /** What could not be fetched. Shown under the answer, never as a fact. */
    gaps: string[];
    /**
     * Which index questions produced these cards, one sentence each.
     *
     * Shown under the answer because it is the other half of the honesty: the
     * gaps say what is missing, and this says what was asked for in the first
     * place. A reader who thinks an answer is thin can see whether the recipe
     * never looked, which is a different complaint from the model not saying.
     */
    sources: string[];
    /** The answer as the model wrote it. Never rewritten. */
    answer: string;
    /** The citation check over that answer. */
    check?: CitationCheck;
    /** True when the token ceiling cut the answer off mid-sentence. */
    truncated?: boolean;
    /** The sentence the panel shows instead of, or beside, an answer. */
    message: string;
    /**
     * The candidates of an ambiguous name, when the turn is waiting on a choice.
     *
     * Carried on the turn and not only in the packet, because the panel outlives
     * the packet: a reader can come back to an older turn and pick then.
     */
    choice?: SubjectChoice;
    /** Set when this answer is about the focus because a written name reached nothing. */
    focusFallback?: FocusFallback;
    /** Why a turn was refused, when it was. */
    refusal?: RefusalReason;
    /** Tokens per second of this answer, when the sidecar reported enough to say. */
    tokensPerSecond?: number;
    durationMs?: number;
}

/** What one ask needs. */
export interface AskInput {
    question: string;
    source: RecipeSource;
    root: string;
    /** The sidecar origin. `http://127.0.0.1:4141` in the product. */
    origin: string;
    fetch: ChatFetch;
    modelName: string;
    modelClass: ModelClass;
    /**
     * Which model in the sidecar's cache should answer (W10).
     *
     * Passed straight through to the request and left out when absent. It is
     * separate from `modelName`, which names the model for the PROMPT (the
     * non-thinking switch is chosen per model family): one is what the prompt is
     * built for, the other is what the process is asked to load, and in a router
     * with two instances those are two different questions.
     */
    model?: string;
    depth?: NeighborDepth;
    context?: ClassifierContext;
    focus?: SymbolRef;
    /** The qualified name the reader picked from a candidate list, when they did. */
    chosenSubject?: string;
    observed?: ObservedReader;
    opts?: ProviderQueryOptions;
    maxTokens?: number;
    signal?: AbortSignal;
    /** Compile cards but do not contact the model. */
    useModel?: boolean;
    /** The honest reason why compilation stops before the optional model pass. */
    modelUnavailableReason?: RefusalReason;
}

/** The deterministic answer shown when the cards exist but the model is off. */
function builtAnswer(cards: readonly Card[]): string {
    return cards.map((card) => `${card.lines[0] ?? ''} [${card.id}]`).filter(Boolean).join('\n');
}

/** The compiled half of a turn: everything that happens before the model. */
export interface CompiledQuestion {
    packet: FactPacket;
    cards: CardSet;
    plan?: PromptPlan;
}

/**
 * Steps one to three: everything up to but not including the model.
 *
 * Separate from {@link askAtlas} because the eval compiles once per model class
 * and then reuses the same prompt for every model in that class. Compiling per
 * model would ask the index the same questions six times over and, worse, would
 * let a slow index turn into a difference between two models' scores.
 */
export async function compileQuestion(input: AskInput): Promise<CompiledQuestion> {
    const depth = input.depth ?? NEIGHBOR_DEPTH_DEFAULT;
    const packet = await compileFacts(input.source, input.root, input.question, {
        depth,
        ...(input.context === undefined ? {} : { context: input.context }),
        ...(input.focus === undefined ? {} : { focus: input.focus }),
        ...(input.chosenSubject === undefined ? {} : { chosenSubject: input.chosenSubject }),
        ...(input.observed === undefined ? {} : { observed: input.observed }),
        ...(input.opts ?? {}),
    });
    const cards = compileCards(packet, { budget: cardBudgetOf(input.modelClass) });
    if (cards.cards.length === 0) {
        return { packet, cards };
    }
    return {
        packet,
        cards,
        plan: buildPrompt({
            question: input.question,
            klass: packet.klass,
            cards,
            modelName: input.modelName,
        }),
    };
}

/** Tokens per second, or nothing when the sidecar did not report enough to say. */
export function tokensPerSecond(completionTokens: number | undefined, durationMs: number): number | undefined {
    if (completionTokens === undefined || completionTokens <= 0 || durationMs <= 0) {
        return undefined;
    }
    return completionTokens / (durationMs / 1000);
}

/**
 * The whole path, for one question.
 *
 * Never rejects. Every failure becomes a turn with a status and a sentence,
 * because a chat that throws leaves the reader with a spinner and no idea which
 * of five things went wrong.
 */
export async function askAtlas(input: AskInput, id = 0): Promise<ChatTurn> {
    const depth = input.depth ?? NEIGHBOR_DEPTH_DEFAULT;
    const turn: ChatTurn = {
        id,
        question: input.question,
        status: 'compiling',
        depth,
        cards: [],
        gaps: [],
        sources: [],
        answer: '',
        message: '',
    };

    let compiled: CompiledQuestion;
    try {
        compiled = await compileQuestion(input);
    } catch (error) {
        return {
            ...turn,
            status: 'failed',
            message: `the cards could not be compiled: ${error instanceof Error ? error.message : String(error)}`,
        };
    }

    turn.klass = compiled.packet.klass;
    turn.rule = compiled.packet.classification.rule;
    turn.cards = compiled.cards.cards;
    turn.tokens = compiled.cards.tokens;
    turn.budget = compiled.cards.budget;
    turn.gaps = compiled.cards.gaps;
    turn.sources = compiled.packet.sources;
    if (compiled.packet.focusFallback !== undefined) {
        turn.focusFallback = compiled.packet.focusFallback;
    }

    const plan = compiled.plan;
    /*
     * A name that reached several symbols is not a question the model can help
     * with: whichever cards it got, it would answer about one of them without
     * ever having been told which. The turn stops here and hands the choice back
     * to the reader, and no request is sent.
     */
    if (compiled.packet.choice !== undefined) {
        return {
            ...turn,
            status: 'needs-choice',
            choice: compiled.packet.choice,
            answer: '',
            message: '',
        };
    }
    if (plan === undefined) {
        // Nothing to cite means nothing to ask. See the header.
        return { ...turn, status: 'no-cards', answer: NO_CARD_SENTENCE, message: '' };
    }
    if (input.useModel === false) {
        const answer = builtAnswer(turn.cards);
        return {
            ...turn,
            status: 'answered',
            answer,
            check: checkCitations(answer, plan.cardIds),
            /* A built answer after the optional model has become ready is not
             * a refusal. Only a real unavailable reason gets a visible note. */
            message: input.modelUnavailableReason === undefined
                ? ''
                : `model-${input.modelUnavailableReason}`,
        };
    }

    try {
        const reply = await askModel({
            origin: input.origin,
            system: plan.system,
            user: plan.user,
            chatTemplateKwargs: plan.nonThinking.chatTemplateKwargs,
            fetch: input.fetch,
            ...(input.model === undefined ? {} : { model: input.model }),
            ...(input.maxTokens === undefined ? {} : { maxTokens: input.maxTokens }),
            ...(input.signal === undefined ? {} : { signal: input.signal }),
        });
        const perSecond = tokensPerSecond(reply.completionTokens, reply.durationMs);
        const answered: ChatTurn = {
            ...turn,
            status: 'answered',
            answer: reply.content,
            check: checkCitations(reply.content, plan.cardIds, { truncated: reply.truncated }),
            truncated: reply.truncated,
            durationMs: reply.durationMs,
            message: reply.thoughtOnly ? 'thought-only' : '',
        };
        if (perSecond !== undefined) {
            answered.tokensPerSecond = perSecond;
        }
        return answered;
    } catch (error) {
        const detail = error instanceof ChatError ? error.message : String(error);
        return { ...turn, status: 'failed', message: detail };
    }
}
