/**
 * The one request this product ever sends to a model.
 *
 * OpenAI-compatible `POST /v1/chat/completions` against the sidecar on
 * 127.0.0.1:4141, and nothing else: no streaming, no tools, no function calls,
 * no second endpoint. The reasons are in PLAN paragraph 5 and they are product
 * decisions rather than simplifications. A tool call would let the model decide
 * what to look up, and the whole point of the context compiler is that the graph
 * decides that. Streaming would make an answer visible before the citation check
 * has run, which would put an uncited claim on the screen for as long as it takes
 * to finish.
 *
 * ## Three settings are fixed and one is not
 *
 * `temperature` is 0 and `seed` is {@link CHAT_SEED}, because the eval compares
 * six models against forty questions and a run that could not be repeated would
 * compare nothing. `chat_template_kwargs` carries the non-thinking switch the
 * prompt contract measured per model. `max_tokens` is the one knob a caller
 * moves, because a refine of a twenty-line block needs more room than a
 * four-line answer.
 *
 * Since W10 there is a fifth field and it is optional: `model`. Without a choice
 * it is not in the body at all, so the request is exactly the one this file has
 * always sent. See {@link ChatRequest.model} for why it is not sent by default.
 *
 * ## Reasoning content is read, and it is not an answer
 *
 * A model whose template ignored the non-thinking switch answers with an empty
 * `content` and a full `reasoning_content`. That is reported as
 * {@link ChatReply.thoughtOnly} rather than papered over by falling back to the
 * reasoning text: an internal monologue is not an answer under the citation
 * contract, and a surface that showed one would be showing the model's guesses
 * with the authority of its conclusions.
 */

/** The port llm/start.sh binds. Fixed there, mirrored here. */
export const SIDECAR_CHAT_PATH = '/v1/chat/completions';

/** Temperature zero. The eval is a comparison and a comparison needs repeats. */
export const CHAT_TEMPERATURE = 0;

/** The seed every request carries. Written down so a rerun can be checked. */
export const CHAT_SEED = 42;

/**
 * How many tokens one chat answer may cost.
 *
 * Four short lines of German cost about sixty, so this is generous by three
 * times over. It is not larger for a measured reason: greedy decoding on a 1B
 * model loops, and a loop that is allowed to run for three hundred tokens fills
 * the panel with the same sentence eight times before the limit stops it. The
 * ceiling bounds the damage; {@link CHAT_REPEAT_PENALTY} is what makes the loop
 * rare in the first place.
 */
export const CHAT_MAX_TOKENS = 220;

/**
 * A mild penalty on tokens the answer has already used.
 *
 * Not a departure from greedy decoding: the penalty reshapes the logits and the
 * argmax is still taken, so two runs with the same seed still produce the same
 * bytes, which is what the eval needs. It is here because it was measured to be
 * needed: at 1.0, four of the forty-four eval answers from the 2B candidate
 * degenerated into one line repeated until the token limit cut it in half, and
 * a half line carries no citation, so a sampler default was costing the model
 * points that had nothing to do with what it knew. Applied identically to all
 * six candidates, so the comparison stays a comparison.
 */
export const CHAT_REPEAT_PENALTY = 1.1;

/** How long one request may take before it is abandoned. */
export const CHAT_TIMEOUT_MS = 120000;

/** Only what this file needs from `fetch`, so a test can replace it. */
export type ChatFetch = (
    input: string,
    init: {
        method: string;
        headers: Record<string, string>;
        body: string;
        signal?: AbortSignal;
    },
) => Promise<Response>;

/** One request. */
export interface ChatRequest {
    origin: string;
    system: string;
    user: string;
    /** The per-model non-thinking switch from the prompt contract. */
    chatTemplateKwargs?: Record<string, unknown>;
    maxTokens?: number;
    /**
     * Which model in the sidecar's cache is meant to answer (W10).
     *
     * Left out unless the reader picked one, and the body is then byte for byte
     * what it was before this field existed. That is not caution for its own
     * sake: a sidecar started on one fixed file IGNORES a foreign `model` field
     * SILENTLY (measured on 2026-08-29, see src/llm/sidecar.ts), so sending one
     * anyway would look like a choice and be none. Who decides whether it may
     * be sent is the caller, because only the caller knows whether the process
     * is running as a router.
     */
    model?: string;
    fetch: ChatFetch;
    signal?: AbortSignal;
}

/** What came back. */
export interface ChatReply {
    /** The answer. Empty when the model produced only a monologue. */
    content: string;
    /** The monologue, when the model returned one. Never rendered as an answer. */
    reasoning: string;
    /** True when `content` is empty and `reasoning` is not. */
    thoughtOnly: boolean;
    /**
     * True when the generation stopped at the token ceiling rather than at the
     * end of what the model had to say.
     *
     * Carried because it changes what the last line of the answer means: a line
     * the limit cut in half is not a claim the model chose to leave uncited, and
     * a citation check that scored it as one would be measuring
     * {@link CHAT_MAX_TOKENS} instead of the model. The panel says it out loud
     * for the same reason.
     */
    truncated: boolean;
    /** What the sidecar gave as the reason it stopped. `length` means truncated. */
    finishReason: string;
    promptTokens?: number;
    completionTokens?: number;
    /** Wall clock of the request, for tokens per second. */
    durationMs: number;
}

/** A request that did not produce an answer, with the reason a reader can act on. */
export class ChatError extends Error {
    constructor(readonly reason: 'unreachable' | 'http' | 'malformed', message: string) {
        super(message);
        this.name = 'ChatError';
    }
}

function asRecord(value: unknown): Record<string, unknown> {
    return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : {};
}

function asText(value: unknown): string {
    return typeof value === 'string' ? value : '';
}

function asCount(value: unknown): number | undefined {
    return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

/** The body, built in one place so the eval and the chat send the same bytes. */
export function chatBody(request: ChatRequest): Record<string, unknown> {
    return {
        messages: [
            { role: 'system', content: request.system },
            { role: 'user', content: request.user },
        ],
        temperature: CHAT_TEMPERATURE,
        seed: CHAT_SEED,
        repeat_penalty: CHAT_REPEAT_PENALTY,
        max_tokens: request.maxTokens ?? CHAT_MAX_TOKENS,
        stream: false,
        ...(request.chatTemplateKwargs === undefined
            ? {}
            : { chat_template_kwargs: request.chatTemplateKwargs }),
        ...(request.model === undefined || request.model.length === 0
            ? {}
            : { model: request.model }),
    };
}

/** Ask the sidecar once. Rejects with a {@link ChatError} and never with a string. */
export async function askModel(request: ChatRequest): Promise<ChatReply> {
    const started = Date.now();
    let response: Response;
    try {
        response = await request.fetch(`${request.origin}${SIDECAR_CHAT_PATH}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(chatBody(request)),
            ...(request.signal === undefined ? {} : { signal: request.signal }),
        });
    } catch (error) {
        throw new ChatError(
            'unreachable',
            `the sidecar did not answer: ${error instanceof Error ? error.message : String(error)}`,
        );
    }
    const text = await response.text();
    if (!response.ok) {
        throw new ChatError('http', `the sidecar answered with HTTP ${response.status}: ${text.slice(0, 200)}`);
    }
    let payload: unknown;
    try {
        payload = JSON.parse(text);
    } catch {
        throw new ChatError('malformed', `the sidecar answered with something that is not JSON: ${text.slice(0, 200)}`);
    }
    const record = asRecord(payload);
    const choices = record['choices'];
    const first = Array.isArray(choices) ? asRecord(choices[0]) : {};
    const message = asRecord(first['message']);
    const content = asText(message['content']).trim();
    const reasoning = asText(message['reasoning_content']).trim();
    const usage = asRecord(record['usage']);
    const finishReason = asText(first['finish_reason']);
    const reply: ChatReply = {
        content,
        reasoning,
        thoughtOnly: content.length === 0 && reasoning.length > 0,
        truncated: finishReason === 'length',
        finishReason,
        durationMs: Date.now() - started,
    };
    const promptTokens = asCount(usage['prompt_tokens']);
    if (promptTokens !== undefined) {
        reply.promptTokens = promptTokens;
    }
    const completionTokens = asCount(usage['completion_tokens']);
    if (completionTokens !== undefined) {
        reply.completionTokens = completionTokens;
    }
    return reply;
}
