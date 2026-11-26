import { describe, expect, it } from 'vitest';

import { NO_CARD_SENTENCE } from './answer-contract';
import { cardBudgetOf, compileCards } from './card-compiler';
import { compileFacts } from './fact-recipes';
import {
    CARDS_HEADING,
    CLASS_HINTS,
    GAPS_HEADING,
    NON_THINKING_TABLE,
    QUESTION_HEADING,
    REFINE_SYSTEM_PROMPT,
    SYSTEM_PROMPT,
    buildPrompt,
    buildRefinePrompt,
    nonThinkingFor,
} from './prompt-contract';
import { QUESTION_CLASSES } from './question-classifier';
import { FAKE_ROOT, fakeSource } from './__fixtures__/fake-source';

async function planFor(question: string, modelName = 'Qwen3.5-2B') {
    const packet = await compileFacts(fakeSource(), FAKE_ROOT, question);
    const cards = compileCards(packet, { budget: cardBudgetOf('A') });
    return buildPrompt({ question, klass: packet.klass, cards, modelName });
}

describe('the system prompt', () => {
    it('points at the cards for the shape instead of demonstrating it', () => {
        expect(SYSTEM_PROMPT).toContain('<one short sentence taken from a card> [K<number of that card>]');
        expect(SYSTEM_PROMPT).toContain('always stands in square brackets at the end');
        expect(SYSTEM_PROMPT).toContain('Never write an angle bracket');
    });

    it('forbids inventing a card number', () => {
        expect(SYSTEM_PROMPT).toContain('Never invent one');
    });

    it('describes the fallback line instead of quoting one to copy', () => {
        expect(SYSTEM_PROMPT).toContain('saying that no');
        expect(SYSTEM_PROMPT).not.toContain(NO_CARD_SENTENCE);
    });

    it('forbids advice and code', () => {
        expect(SYSTEM_PROMPT).toContain('No advice');
    });

    it('carries no finished sentence a model could copy as an answer', () => {
        // The one concrete line is a template whose slots are angle brackets,
        // and rule 2 forbids writing those. Nothing else in the prompt is a
        // sentence about code that could be pasted into an answer.
        expect(SYSTEM_PROMPT).not.toMatch(/\[K\d+\]/);
        expect(SYSTEM_PROMPT.toLowerCase()).not.toContain('example');
    });

    it('forbids the three shapes the first eval pass measured as violations', () => {
        expect(SYSTEM_PROMPT).toContain('No heading, no introduction, no closing remark');
        expect(SYSTEM_PROMPT).toContain('Copy every name, file and line number exactly');
        expect(SYSTEM_PROMPT).toContain('Answer only what was asked');
        expect(SYSTEM_PROMPT).toContain('Never write the same line twice');
        expect(SYSTEM_PROMPT).toContain('write nothing else at all');
    });

    it('asks for the language of the question', () => {
        expect(SYSTEM_PROMPT).toContain('Answer in the language of the question');
    });
});

describe('the user prompt', () => {
    it('puts the cards under a heading that says they are everything', async () => {
        const plan = await planFor('Wer ruft createUser?');
        expect(plan.user).toContain(CARDS_HEADING);
        expect(plan.user).toContain('[K1] createUser');
        expect(plan.user).toContain(QUESTION_HEADING);
        expect(plan.user).toContain('Wer ruft createUser?');
    });

    it('names what could not be fetched, outside the cards', async () => {
        const plan = await planFor('Wer ruft createUser?');
        expect(plan.user).toContain(GAPS_HEADING);
        expect(plan.user).toContain('no runtime recording was read');
    });

    it('nudges towards the family the reader asked for', async () => {
        const plan = await planFor('Wer ruft createUser?');
        expect(plan.user).toContain(CLASS_HINTS['who-calls']);
    });

    it('has a hint for every class', () => {
        for (const klass of QUESTION_CLASSES) {
            expect(CLASS_HINTS[klass].length).toBeGreaterThan(0);
        }
    });

    it('lists the card ids it handed over, for the check afterwards', async () => {
        const plan = await planFor('Wer ruft createUser?');
        expect(plan.cardIds[0]).toBe('K1');
        expect(plan.cardIds.length).toBe(plan.cards.length);
    });

    it('is byte-identical for the same question and the same cards', async () => {
        const first = await planFor('Wer ruft createUser?');
        const second = await planFor('Wer ruft createUser?');
        expect(second.user).toBe(first.user);
        expect(second.system).toBe(first.system);
    });

    it('stays inside the class budget it was compiled for', async () => {
        const plan = await planFor('Wer ruft createUser?');
        expect(plan.estimatedTokens).toBeLessThan(3000);
    });
});

describe('non-thinking, measured per model', () => {
    it('sends the same template switch for every candidate', () => {
        for (const name of Object.keys(NON_THINKING_TABLE)) {
            expect(nonThinkingFor(name).chatTemplateKwargs).toEqual({ enable_thinking: false });
        }
    });

    it('records what each of the six candidates does without it', () => {
        expect(Object.keys(NON_THINKING_TABLE).length).toBe(6);
        for (const note of Object.values(NON_THINKING_TABLE)) {
            expect(note.length).toBeGreaterThan(20);
        }
    });

    it('names the reasoning models as thinking by default', () => {
        for (const name of ['Qwen3.5-2B', 'Qwen3.5-4B', 'MiniCPM5-1B', 'gemma-4-E4B']) {
            expect(NON_THINKING_TABLE[name]).toContain('thinks by default');
        }
    });

    it('names the two non-reasoning models as never thinking', () => {
        for (const name of ['LFM2.5-1.2B', 'Qwen2.5-Coder-1.5B']) {
            expect(NON_THINKING_TABLE[name]).toContain('never thinks');
        }
    });

    it('says so when a model was not measured, instead of pretending it was', () => {
        expect(nonThinkingFor('SomethingElse-9B').note).toContain('not measured');
    });

    it('matches a model name that carries a quantisation suffix', () => {
        expect(nonThinkingFor('Qwen3.5-2B-Instruct').note).toContain('thinks by default');
    });
});

describe('the refine prompt', () => {
    it('demands the same line count, order and numbers', () => {
        expect(REFINE_SYSTEM_PROMPT).toContain('exactly as many lines');
        expect(REFINE_SYSTEM_PROMPT).toContain('keeps that number');
        expect(REFINE_SYSTEM_PROMPT).toContain('Never add a fact');
    });

    it('tells the model how many lines it is holding', () => {
        expect(buildRefinePrompt('1. a\n2. b')).toContain('Rewrite these 2 lines');
    });
});
