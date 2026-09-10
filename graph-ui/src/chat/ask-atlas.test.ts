import { describe, expect, it, vi } from 'vitest';

import { FAKE_ROOT, fakeSource } from '../compiler/__fixtures__/fake-source';
import { NO_CARD_SENTENCE } from '../compiler/answer-contract';
import { askAtlas, compileQuestion, tokensPerSecond } from './ask-atlas';
import type { AskInput } from './ask-atlas';

function answering(content: string, completionTokens = 20) {
    return vi.fn(async () =>
        new Response(
            JSON.stringify({
                choices: [{ message: { content } }],
                usage: { prompt_tokens: 400, completion_tokens: completionTokens },
            }),
            { status: 200 },
        ));
}

function inputFor(question: string, fetchImpl: ReturnType<typeof answering>): AskInput {
    return {
        question,
        source: fakeSource(),
        root: FAKE_ROOT,
        origin: 'http://127.0.0.1:4141',
        fetch: fetchImpl as unknown as AskInput['fetch'],
        modelName: 'Qwen3.5-2B',
        modelClass: 'A',
    };
}

describe('compileQuestion', () => {
    it('produces cards and a prompt for a question the index can answer', async () => {
        const compiled = await compileQuestion(inputFor('Wer ruft createUser?', answering('x')));
        expect(compiled.cards.cards.length).toBeGreaterThan(1);
        expect(compiled.plan?.user).toContain('[K1]');
    });

    it('produces no prompt at all when nothing could be put on a card', async () => {
        const compiled = await compileQuestion(inputFor('Was macht @nothingHere?', answering('x')));
        expect(compiled.cards.cards).toEqual([]);
        expect(compiled.plan).toBeUndefined();
    });

    it('gives a class B model more room than a class A model', async () => {
        const small = await compileQuestion(inputFor('Wer ruft createUser?', answering('x')));
        const large = await compileQuestion({
            ...inputFor('Wer ruft createUser?', answering('x')),
            modelClass: 'B',
        });
        expect(large.cards.budget).toBeGreaterThan(small.cards.budget);
    });
});

describe('askAtlas', () => {
    it('answers, checks the citations and reports the speed', async () => {
        const fetchImpl = answering('createUser is called by create at line 41 [K2].');
        const turn = await askAtlas(inputFor('Wer ruft createUser?', fetchImpl));
        expect(turn.status).toBe('answered');
        expect(turn.check?.ok).toBe(true);
        expect(turn.check?.cited).toEqual(['K2']);
        expect(turn.tokensPerSecond).toBeGreaterThan(0);
    });

    it('marks an answer whose line carries no citation, without rewriting it', async () => {
        const fetchImpl = answering('createUser is called by create.');
        const turn = await askAtlas(inputFor('Wer ruft createUser?', fetchImpl));
        expect(turn.check?.ok).toBe(false);
        expect(turn.answer).toBe('createUser is called by create.');
    });

    it('never sends a request when there is no card to cite', async () => {
        const fetchImpl = answering('anything');
        const turn = await askAtlas(inputFor('Was macht @nothingHere?', fetchImpl));
        expect(turn.status).toBe('no-cards');
        expect(turn.answer).toBe(NO_CARD_SENTENCE);
        expect(fetchImpl).not.toHaveBeenCalled();
    });

    it('keeps a normal built turn neutral when an optional model is ready', async () => {
        const fetchImpl = answering('the model must not be called');
        const turn = await askAtlas({
            ...inputFor('Wer ruft createUser?', fetchImpl),
            useModel: false,
        });
        expect(turn.status).toBe('answered');
        expect(turn.message).toBe('');
        expect(turn.check?.ok).toBe(true);
        expect(fetchImpl).not.toHaveBeenCalled();
    });

    it('turns a failed request into a turn with a reason, not an exception', async () => {
        const broken = vi.fn(async () => {
            throw new Error('connection refused');
        });
        const turn = await askAtlas(inputFor('Wer ruft createUser?', broken as never));
        expect(turn.status).toBe('failed');
        expect(turn.message).toContain('connection refused');
    });

    it('carries the depth it used into the turn', async () => {
        const turn = await askAtlas({
            ...inputFor('Wer ruft createUser?', answering('a [K1]')),
            depth: 2,
        });
        expect(turn.depth).toBe(2);
    });

    it('reports which class and which rule decided the question', async () => {
        const turn = await askAtlas(inputFor('Wer ruft createUser?', answering('a [K1]')));
        expect(turn.klass).toBe('who-calls');
        expect(turn.rule).toBe('who-calls-words');
    });
});

describe('tokensPerSecond', () => {
    it('divides tokens by seconds', () => {
        expect(tokensPerSecond(20, 1000)).toBe(20);
        expect(tokensPerSecond(10, 500)).toBe(20);
    });

    it('says nothing when the sidecar said nothing', () => {
        expect(tokensPerSecond(undefined, 1000)).toBeUndefined();
        expect(tokensPerSecond(0, 1000)).toBeUndefined();
        expect(tokensPerSecond(10, 0)).toBeUndefined();
    });
});
