/**
 * The one entry the head-to-head eval imports.
 *
 * tools/eval-llm.mjs runs in Node and the compiler is TypeScript, so something
 * has to bridge the two. The bridge is this file plus one esbuild bundle step
 * (tools/lib/compiler-bundle.mjs), and it exists instead of the obvious
 * alternative, which would be to reimplement the classifier, the recipes, the
 * card format and the citation rule in the measurement tool. That alternative
 * would measure a second product: the moment the two drifted, the eval would be
 * scoring cards no reader ever sees.
 *
 * Nothing new is written here. Every export is a re-export, and the one function
 * with a body (`makeProvider`) only wires the existing provider to a base origin
 * instead of to the page it was served from, because a Node process has no
 * origin of its own.
 */

export {
    classifyQuestion,
    QUESTION_CLASSES,
} from './question-classifier';
export type { Classification, QuestionClass } from './question-classifier';

export {
    compileFacts,
    NEIGHBOR_DEPTHS,
    NEIGHBOR_DEPTH_DEFAULT,
    RECIPE_SOURCES,
} from './fact-recipes';
export type { FactPacket, NeighborDepth } from './fact-recipes';

export {
    CARD_BUDGET_CLASS_A,
    CARD_BUDGET_CLASS_B,
    CHARS_PER_TOKEN,
    PROMPT_RESERVE_TOKENS,
    budgetOf,
    cardBudgetOf,
    compileCards,
    estimateTokens,
    renderCards,
} from './card-compiler';
export type { Card, CardSet, ModelClass } from './card-compiler';

export {
    NON_THINKING_TABLE,
    SYSTEM_PROMPT,
    buildPrompt,
    buildRefinePrompt,
    nonThinkingFor,
    REFINE_SYSTEM_PROMPT,
} from './prompt-contract';
export type { PromptPlan } from './prompt-contract';

export {
    NO_CARD_SENTENCE,
    checkCitations,
    citationsIn,
    claimLines,
    isNoCardLine,
} from './answer-contract';
export type { CitationCheck } from './answer-contract';

export { resolveSubject } from './subject-resolver';

export { askAtlas, compileQuestion, tokensPerSecond } from '../chat/ask-atlas';
export type { ChatTurn } from '../chat/ask-atlas';

export { CHAT_MAX_TOKENS, CHAT_SEED, CHAT_TEMPERATURE, askModel } from '../chat/chat-client';

export { ATLAS_WORKSPACE_ROOT } from '../twin/twin-target';

import { CbmRpcProvider } from '../provider/cbm-rpc-provider';
import { RpcIntelligenceClient } from '../provider/rpc-client';

/**
 * The provider, pointed at a server this process is not being served by.
 *
 * `base` is the only thing a Node caller has to supply that a browser caller
 * does not: in the product the frontend is delivered by the same C-server it
 * queries, so same-origin is the default everywhere else.
 */
export function makeProvider(base: string, generation = 1): CbmRpcProvider {
    return new CbmRpcProvider(new RpcIntelligenceClient({ base }), { generation });
}
