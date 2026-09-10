import { describe, expect, it } from 'vitest';

import { CREATE_USER_IR } from '../test-support/twin-fixtures';
import {
    CARD_BUDGET_CLASS_A,
    CARD_BUDGET_CLASS_B,
    CHARS_PER_TOKEN,
    MAX_CARD_LINES,
    budgetOf,
    cardBudgetOf,
    compileCards,
    estimateTokens,
    listOf,
    modelClassOf,
    priorityOf,
    renderCard,
    renderCards,
    whereOf,
} from './card-compiler';
import type { FactPacket, NeighbourFact } from './fact-recipes';
import { classifyQuestion } from './question-classifier';
import type { QuestionClass } from './question-classifier';

/** A packet the way a recipe would have produced it, without a server. */
function packetOf(overrides: Partial<FactPacket> = {}): FactPacket {
    const question = overrides.question ?? 'Wer ruft createUser?';
    const classification = classifyQuestion(question);
    return {
        klass: overrides.klass ?? classification.klass,
        question,
        classification,
        depth: 1,
        subject: CREATE_USER_IR.symbol,
        subjectIr: CREATE_USER_IR,
        ambiguous: [],
        neighbours: [],
        routes: [],
        entryPoints: [],
        observed: [],
        sources: [],
        notes: [],
        ...overrides,
    };
}

function caller(name: string, line: number): NeighbourFact {
    return {
        name,
        qualifiedName: `p.src.routes.users.${name}`,
        filePath: 'src/routes/users.ts',
        line,
        hop: 1,
        direction: 'caller',
        via: 'createUser',
    };
}

describe('the two hard budgets', () => {
    it('holds the numbers PLAN paragraph 5 sets, in this file', () => {
        expect(CARD_BUDGET_CLASS_A).toBe(3000);
        expect(CARD_BUDGET_CLASS_B).toBe(8000);
        expect(budgetOf('A')).toBe(3000);
        expect(budgetOf('B')).toBe(8000);
    });

    it('reserves room for everything that is not a card', () => {
        expect(cardBudgetOf('A')).toBeLessThan(CARD_BUDGET_CLASS_A);
        expect(cardBudgetOf('B')).toBeLessThan(CARD_BUDGET_CLASS_B);
        expect(cardBudgetOf('A', 3000)).toBe(0);
    });

    it('reads the class off the window the process opened', () => {
        expect(modelClassOf(3072)).toBe('A');
        expect(modelClassOf(8192)).toBe('B');
        expect(modelClassOf(undefined)).toBe('A');
    });

    it('estimates four characters to a token, and says so', () => {
        expect(CHARS_PER_TOKEN).toBe(4);
        expect(estimateTokens('a'.repeat(400))).toBe(100);
        expect(estimateTokens('')).toBe(0);
    });
});

describe('the card format', () => {
    it('numbers the cards K1..Kn in the order they are emitted', () => {
        const set = compileCards(packetOf(), { budget: cardBudgetOf('B') });
        expect(set.cards.length).toBeGreaterThan(0);
        expect(set.cards.map((card) => card.id)).toEqual(
            set.cards.map((_, index) => `K${index + 1}`),
        );
    });

    it('gives no card more than three lines', () => {
        const set = compileCards(
            packetOf({ neighbours: [caller('registerUserRoutes', 15), caller('create', 41)] }),
            { budget: cardBudgetOf('B') },
        );
        for (const card of set.cards) {
            expect(card.lines.length).toBeLessThanOrEqual(MAX_CARD_LINES);
            expect(card.lines.every((line) => line.length > 0)).toBe(true);
        }
    });

    it('writes the canonical shape PLAN paragraph 6 names', () => {
        const set = compileCards(packetOf({ question: 'Was macht createUser?' }), {
            budget: cardBudgetOf('B'),
        });
        const rendered = renderCards(set.cards);
        // name (path:line), and calls with their line in brackets.
        expect(rendered).toMatch(/createUser \(atlas-sample\/src\/services\/userService\.ts:23\)/);
        expect(rendered).toMatch(/calls validateUser \[line 24\]/);
        expect(rendered).toMatch(/^\[K1\] /m);
    });

    it('carries a source with a file and a line, so a citation can be clicked', () => {
        const set = compileCards(
            packetOf({ neighbours: [caller('registerUserRoutes', 15)] }),
            { budget: cardBudgetOf('B') },
        );
        const callerCard = set.cards.find((card) => card.kind === 'caller');
        expect(callerCard?.source?.filePath).toBe('src/routes/users.ts');
        expect(callerCard?.source?.line).toBe(15);
    });

    it('writes the number in citation syntax, on every line of the card', () => {
        const rendered = renderCard({ id: 'K7', kind: 'subject', lines: ['one', 'two'] });
        expect(rendered).toBe('[K7] one\n    two [K7]');
    });

    it('writes name (path:line) or as much of it as there is', () => {
        expect(whereOf({ name: 'a', filePath: 'x.ts', line: 3 })).toBe('a (x.ts:3)');
        expect(whereOf({ name: 'a', filePath: 'x.ts' })).toBe('a (x.ts)');
        expect(whereOf({ name: 'a' })).toBe('a');
        expect(whereOf(undefined)).toBe('');
    });
});

describe('the budget is hard', () => {
    it('never emits more tokens than it was given', () => {
        const many = Array.from({ length: 60 }, (_, index) => caller(`caller${index}`, index + 1));
        for (const budget of [40, 120, 400, cardBudgetOf('A'), cardBudgetOf('B')]) {
            const set = compileCards(packetOf({ neighbours: many }), { budget });
            expect(set.tokens).toBeLessThanOrEqual(budget);
            expect(estimateTokens(renderCards(set.cards))).toBeLessThanOrEqual(budget);
        }
    });

    it('drops whole cards and never half of one', () => {
        const many = Array.from({ length: 60 }, (_, index) => caller(`caller${index}`, index + 1));
        const set = compileCards(packetOf({ neighbours: many }), { budget: 200 });
        expect(set.dropped).toBeGreaterThan(0);
        for (const card of set.cards) {
            expect(card.lines.every((line) => line.trim().length > 0)).toBe(true);
        }
    });

    it('says how much it cut, in the words the contract asks for', () => {
        const many = Array.from({ length: 60 }, (_, index) => caller(`caller${index}`, index + 1));
        const set = compileCards(packetOf({ neighbours: many }), { budget: 200 });
        const note = set.cards[set.cards.length - 1];
        expect(note.kind).toBe('absence');
        expect(note.lines.join(' ')).toMatch(/\d+ more callers not listed/);
    });

    it('emits no cut note when nothing was cut', () => {
        const set = compileCards(packetOf(), { budget: cardBudgetOf('B') });
        expect(set.dropped).toBe(0);
        expect(set.cards.some((card) => card.kind === 'absence')).toBe(false);
    });

    it('gives a class A packet fewer cards than a class B packet', () => {
        const many = Array.from({ length: 400 }, (_, index) => caller(`caller${index}`, index + 1));
        const small = compileCards(packetOf({ neighbours: many }), { budget: cardBudgetOf('A') });
        const large = compileCards(packetOf({ neighbours: many }), { budget: cardBudgetOf('B') });
        expect(small.cards.length).toBeLessThan(large.cards.length);
    });

    it('produces no cards at all when the packet holds nothing', () => {
        const empty = compileCards(
            packetOf({ subject: undefined, subjectIr: undefined, klass: 'other' }),
            { budget: cardBudgetOf('A') },
        );
        expect(empty.cards).toEqual([]);
        expect(empty.tokens).toBe(0);
    });
});

describe('determinism', () => {
    it('produces byte-identical cards for the same packet', () => {
        const packet = packetOf({
            neighbours: [caller('zeta', 9), caller('alpha', 3), caller('mid', 5)],
        });
        const first = compileCards(packet, { budget: cardBudgetOf('A') });
        const second = compileCards(packet, { budget: cardBudgetOf('A') });
        expect(renderCards(second.cards)).toBe(renderCards(first.cards));
    });

    it('does not depend on the order the recipe found the neighbours in', () => {
        const forward = [caller('alpha', 3), caller('mid', 5), caller('zeta', 9)];
        const backward = [...forward].reverse();
        const a = compileCards(packetOf({ neighbours: forward }), { budget: cardBudgetOf('A') });
        const b = compileCards(packetOf({ neighbours: backward }), { budget: cardBudgetOf('A') });
        expect(b.cards.map((card) => card.lines[0])).toEqual(a.cards.map((card) => card.lines[0]));
    });
});

describe('the class decides the order', () => {
    it('puts callers before the call list for a caller question', () => {
        const rank = priorityOf('who-calls');
        expect(rank('caller')).toBeLessThan(rank('calls'));
    });

    it('puts what can be raised first for an error question', () => {
        const rank = priorityOf('why-error');
        expect(rank('raises')).toBeLessThan(rank('caller'));
    });

    it('always puts the subject first, whatever the class', () => {
        const classes: QuestionClass[] = [
            'what-is', 'who-calls', 'what-if', 'where-entry', 'why-error', 'compare', 'overview', 'other',
        ];
        for (const klass of classes) {
            const rank = priorityOf(klass);
            expect(rank('subject')).toBe(0);
        }
    });
});

describe('enumerations are honest', () => {
    it('names the overflow rather than truncating in silence', () => {
        expect(listOf(['a', 'b'], 5)).toBe('a, b');
        expect(listOf(['a', 'b', 'c'], 2)).toBe('a, b, and 1 more not listed');
        expect(listOf([], 2)).toBe('');
    });
});

describe('gaps travel with the cards', () => {
    it('carries the recipe notes without turning them into facts', () => {
        const set = compileCards(packetOf({ notes: ['no runtime recording was read.'] }), {
            budget: cardBudgetOf('A'),
        });
        expect(set.gaps).toEqual(['no runtime recording was read.']);
        expect(renderCards(set.cards)).not.toContain('no runtime recording was read.');
    });
});
