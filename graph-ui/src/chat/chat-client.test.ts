import { describe, expect, it, vi } from 'vitest';

import {
    CHAT_MAX_TOKENS,
    CHAT_SEED,
    CHAT_TEMPERATURE,
    ChatError,
    SIDECAR_CHAT_PATH,
    askModel,
    chatBody,
} from './chat-client';

const ORIGIN = 'http://127.0.0.1:4141';

function reply(payload: unknown, status = 200): Response {
    return new Response(JSON.stringify(payload), { status });
}

function baseRequest(fetchImpl: ReturnType<typeof vi.fn>) {
    return {
        origin: ORIGIN,
        system: 'rules',
        user: 'cards and a question',
        fetch: fetchImpl as unknown as Parameters<typeof askModel>[0]['fetch'],
    };
}

describe('the request body', () => {
    it('pins temperature and seed, so a run can be repeated', () => {
        const body = chatBody(baseRequest(vi.fn()));
        expect(body.temperature).toBe(0);
        expect(CHAT_TEMPERATURE).toBe(0);
        expect(body.seed).toBe(CHAT_SEED);
        expect(CHAT_SEED).toBe(42);
    });

    it('does not stream', () => {
        expect(chatBody(baseRequest(vi.fn())).stream).toBe(false);
    });

    it('carries the non-thinking switch only when one was given', () => {
        expect(chatBody(baseRequest(vi.fn())).chat_template_kwargs).toBeUndefined();
        const withSwitch = chatBody({
            ...baseRequest(vi.fn()),
            chatTemplateKwargs: { enable_thinking: false },
        });
        expect(withSwitch.chat_template_kwargs).toEqual({ enable_thinking: false });
    });

    it('sends a system and a user message and nothing else', () => {
        expect(chatBody(baseRequest(vi.fn())).messages).toEqual([
            { role: 'system', content: 'rules' },
            { role: 'user', content: 'cards and a question' },
        ]);
    });

    it('defaults the answer length and lets a caller move it', () => {
        expect(chatBody(baseRequest(vi.fn())).max_tokens).toBe(CHAT_MAX_TOKENS);
        expect(chatBody({ ...baseRequest(vi.fn()), maxTokens: 900 }).max_tokens).toBe(900);
    });

    /*
     * Das Modellfeld (W10), und warum es fehlen darf.
     *
     * Ohne Wahl steht es nicht im Rumpf, und der Rumpf ist damit derselbe, den
     * dieses Produkt seit W5b sendet. Das ist keine Vorsicht um ihrer selbst
     * willen: ein Sidecar auf einer festen Datei ignoriert ein fremdes Feld
     * STILLSCHWEIGEND (gemessen, siehe src/llm/sidecar.ts), also waere ein
     * immer mitgesendetes Feld eine Auswahl, die manchmal wirkt und nie sagt,
     * wann.
     */
    it('leaves the model out unless one was chosen', () => {
        expect(chatBody(baseRequest(vi.fn())).model).toBeUndefined();
        expect(chatBody({ ...baseRequest(vi.fn()), model: '' }).model).toBeUndefined();
    });

    it('names the chosen model when there is one', () => {
        expect(chatBody({ ...baseRequest(vi.fn()), model: 'MiniCPM5-1B-Q4_K_M' }).model)
            .toBe('MiniCPM5-1B-Q4_K_M');
    });
});

describe('askModel', () => {
    it('posts to the OpenAI-compatible path of the sidecar', async () => {
        const seen: { url: string; method: string }[] = [];
        const fetchImpl = vi.fn(async (url: string, init: { method: string }) => {
            seen.push({ url, method: init.method });
            return reply({ choices: [{ message: { content: 'ok [K1]' } }] });
        });
        await askModel(baseRequest(fetchImpl as never));
        expect(seen[0]).toEqual({ url: `${ORIGIN}${SIDECAR_CHAT_PATH}`, method: 'POST' });
    });

    it('reads the answer and the usage', async () => {
        const fetchImpl = vi.fn(async () =>
            reply({
                choices: [{ message: { content: ' ok [K1] ' } }],
                usage: { prompt_tokens: 100, completion_tokens: 20 },
            }));
        const result = await askModel(baseRequest(fetchImpl));
        expect(result.content).toBe('ok [K1]');
        expect(result.promptTokens).toBe(100);
        expect(result.completionTokens).toBe(20);
        expect(result.thoughtOnly).toBe(false);
    });

    it('reports a monologue as a monologue and never as an answer', async () => {
        const fetchImpl = vi.fn(async () =>
            reply({ choices: [{ message: { content: '', reasoning_content: 'let me think' } }] }));
        const result = await askModel(baseRequest(fetchImpl));
        expect(result.content).toBe('');
        expect(result.reasoning).toBe('let me think');
        expect(result.thoughtOnly).toBe(true);
    });

    it('names an unreachable sidecar as unreachable', async () => {
        const fetchImpl = vi.fn(async () => {
            throw new Error('connection refused');
        });
        await expect(askModel(baseRequest(fetchImpl))).rejects.toBeInstanceOf(ChatError);
        await askModel(baseRequest(fetchImpl)).catch((error: ChatError) => {
            expect(error.reason).toBe('unreachable');
        });
    });

    it('names an HTTP failure as one, with the status', async () => {
        const fetchImpl = vi.fn(async () => reply({ error: 'boom' }, 500));
        await askModel(baseRequest(fetchImpl)).catch((error: ChatError) => {
            expect(error.reason).toBe('http');
            expect(error.message).toContain('500');
        });
    });

    it('names an answer that is not JSON', async () => {
        const fetchImpl = vi.fn(async () => new Response('<html>', { status: 200 }));
        await askModel(baseRequest(fetchImpl)).catch((error: ChatError) => {
            expect(error.reason).toBe('malformed');
        });
    });
});
