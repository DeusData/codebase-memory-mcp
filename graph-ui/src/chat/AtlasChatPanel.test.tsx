// @vitest-environment jsdom
/**
 * The answer panel in jsdom.
 *
 * What is worth proving here and nowhere else: a `[K3]` really becomes a
 * control that carries the card's source, the cards really fold, and the depth
 * setting really shows all three values with the honest note under it. The
 * browser run proves the same three things against a real model; this suite
 * catches them while they are being written.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import AtlasChatPanel from './AtlasChatPanel';
import type { ChatTurn } from './ask-atlas';
import { CHAT_DEPTH_NOTE, CHAT_OFF_MESSAGE } from './chat-strings';
import { checkCitations } from '../compiler/answer-contract';
import type { Card } from '../compiler/card-compiler';

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
});

afterEach(async () => {
    await act(async () => {
        root.unmount();
    });
    container.remove();
});

const CARDS: Card[] = [
    {
        id: 'K1',
        kind: 'subject',
        lines: ['createUser (src/services/userService.ts:23) is a function in this project.'],
        source: { name: 'createUser', filePath: 'src/services/userService.ts', line: 23 },
    },
    {
        id: 'K2',
        kind: 'caller',
        lines: ['registerUserRoutes (src/routes/users.ts:15) calls createUser at line 15.'],
        source: { name: 'registerUserRoutes', filePath: 'src/routes/users.ts', line: 15 },
    },
];

function turnOf(overrides: Partial<ChatTurn> = {}): ChatTurn {
    const answer = overrides.answer
        ?? 'createUser is called by registerUserRoutes at line 15 [K2].';
    return {
        id: 1,
        question: 'Wer ruft createUser?',
        status: 'answered',
        klass: 'who-calls',
        rule: 'who-calls-words',
        depth: 1,
        cards: CARDS,
        tokens: 120,
        budget: 2300,
        gaps: [],
        sources: ['semantic IR of the subject (caller rows with their call-site lines)'],
        answer,
        check: checkCitations(answer, CARDS.map((card) => card.id)),
        message: '',
        ...overrides,
    };
}

async function render(props: Partial<Parameters<typeof AtlasChatPanel>[0]> = {}): Promise<{
    onOpenCard: ReturnType<typeof vi.fn>;
    onDepth: ReturnType<typeof vi.fn>;
    onClear: ReturnType<typeof vi.fn>;
    onPickCandidate: ReturnType<typeof vi.fn>;
    onAskAgain: ReturnType<typeof vi.fn>;
}> {
    const onOpenCard = vi.fn();
    const onDepth = vi.fn();
    const onClear = vi.fn();
    const onPickCandidate = vi.fn();
    const onAskAgain = vi.fn();
    await act(async () => {
        root.render(
            <AtlasChatPanel
                turns={[turnOf()]}
                depth={1}
                onDepth={onDepth}
                onOpenCard={onOpenCard}
                onClear={onClear}
                onPickCandidate={onPickCandidate}
                onAskAgain={onAskAgain}
                {...props}
            />,
        );
    });
    return { onOpenCard, onDepth, onClear, onPickCandidate, onAskAgain };
}

const find = (testid: string): HTMLElement | null =>
    container.querySelector<HTMLElement>(`[data-testid="${testid}"]`);
const all = (testid: string): HTMLElement[] =>
    [...container.querySelectorAll<HTMLElement>(`[data-testid="${testid}"]`)];

describe('the panel', () => {
    it('carries the mark the proof run looks for', async () => {
        await render();
        expect(find('atlas-chat')).not.toBeNull();
        expect(find('atlas-chat')?.getAttribute('data-turns')).toBe('1');
    });

    it('shows the question and the answer', async () => {
        await render();
        expect(find('atlas-chat-question')?.textContent).toContain('Wer ruft createUser?');
        expect(find('atlas-chat-answer')?.textContent).toContain('registerUserRoutes');
    });
});

describe('citations are controls', () => {
    it('renders a [K] as a button', async () => {
        await render();
        const citations = all('atlas-chat-citation');
        expect(citations.length).toBe(1);
        expect(citations[0].textContent).toBe('[K2]');
        expect(citations[0].getAttribute('data-card')).toBe('K2');
    });

    it('navigates to the card source when the button is pressed', async () => {
        const { onOpenCard } = await render();
        await act(async () => {
            all('atlas-chat-citation')[0].click();
        });
        expect(onOpenCard).toHaveBeenCalledWith(CARDS[1].source);
    });

    it('renders a citation of a card that was never given as text with a mark', async () => {
        await render({ turns: [turnOf({ answer: 'invented [K9].' })] });
        expect(all('atlas-chat-citation').length).toBe(0);
        expect(find('atlas-chat-citation-unknown')?.textContent).toBe('[K9]');
        expect(find('atlas-chat-warning')?.textContent).toContain('K9');
    });

    it('warns about a line with no citation, and still shows the line', async () => {
        await render({ turns: [turnOf({ answer: 'createUser is called by routes.' })] });
        expect(find('atlas-chat-warning')?.textContent).toContain('no card citation');
        expect(find('atlas-chat-answer')?.textContent).toContain('createUser is called by routes.');
    });
});

describe('the cards fold', () => {
    it('names how many there are and shows none until asked', async () => {
        await render();
        expect(find('atlas-chat-cards-toggle')?.textContent).toContain('2 cards');
        expect(all('atlas-chat-card').length).toBe(0);
    });

    it('shows the cards when the fold is opened', async () => {
        const { onOpenCard } = await render();
        await act(async () => {
            find('atlas-chat-cards-toggle')?.click();
        });
        const rows = all('atlas-chat-card');
        expect(rows.length).toBe(2);
        expect(rows[0].getAttribute('data-card')).toBe('K1');
        await act(async () => {
            all('atlas-chat-card-open')[0].click();
        });
        expect(onOpenCard).toHaveBeenCalledWith(CARDS[0].source);
    });
});

describe("Martin's context setting", () => {
    it('offers all three depths and marks the current one', async () => {
        await render();
        const options = all('atlas-chat-depth-option');
        expect(options.map((option) => option.getAttribute('data-value'))).toEqual(['0', '1', '2']);
        expect(find('atlas-chat-depth')?.getAttribute('data-depth')).toBe('1');
        expect(options[1].getAttribute('data-on')).toBe('true');
    });

    it('reports a change instead of applying one itself', async () => {
        const { onDepth } = await render();
        await act(async () => {
            all('atlas-chat-depth-option')[2].click();
        });
        expect(onDepth).toHaveBeenCalledWith(2);
    });

    it('says out loud that more neighbours change the answer, not that they improve it', async () => {
        await render();
        const note = find('atlas-chat-depth-note')?.textContent ?? '';
        expect(note).toBe(CHAT_DEPTH_NOTE);
        expect(note).toContain('can make it worse');
        expect(note.toLowerCase()).not.toContain('better answers');
    });
});

describe('honesty', () => {
    it('says that nothing was sent when the model is off', async () => {
        await render({
            turns: [turnOf({ status: 'refused', refusal: 'off', answer: '', check: undefined })],
        });
        expect(find('atlas-chat-message')?.textContent).toBe(CHAT_OFF_MESSAGE);
        expect(find('atlas-chat-answer')).toBeNull();
    });

    it('says which class and which rule produced the cards, and what they cost', async () => {
        await render();
        const line = find('atlas-chat-provenance')?.textContent ?? '';
        expect(line).toContain('who-calls');
        expect(line).toContain('who-calls-words');
        expect(line).toContain('120');
        expect(line).toContain('2300');
    });

    it('lists what could not be fetched, outside the answer', async () => {
        await render({ turns: [turnOf({ gaps: ['no runtime recording was read.'] })] });
        expect(find('atlas-chat-gaps')?.textContent).toContain('no runtime recording');
    });

    it('names the index questions the cards were built from', async () => {
        await render();
        expect(find('atlas-chat-sources')?.textContent).toContain('semantic IR of the subject');
    });

    it('never renders a monologue as an answer', async () => {
        await render({
            turns: [turnOf({ answer: '', message: 'thought-only', check: undefined })],
        });
        expect(find('atlas-chat-answer')).toBeNull();
        expect(find('atlas-chat-message')?.textContent).toContain('internal monologue');
    });
});

describe('an ambiguous name is a question back to the reader', () => {
    const CHOICE_TURN = turnOf({
        question: 'Was macht @create?',
        status: 'needs-choice',
        answer: '',
        check: undefined,
        cards: [],
        choice: {
            name: 'create',
            candidates: [
                {
                    name: 'create',
                    qualifiedName: 'sample.src.services.orderService.create',
                    filePath: 'src/services/orderService.ts',
                    line: 30,
                },
                {
                    name: 'create',
                    qualifiedName: 'sample.src.services.userService.create',
                    filePath: 'src/services/userService.ts',
                    line: 40,
                },
            ],
        },
    });

    it('offers every candidate with its file and its line', async () => {
        await render({ turns: [CHOICE_TURN] });
        const candidates = all('atlas-chat-candidate');
        expect(candidates.length).toBe(2);
        expect(candidates[0].textContent).toContain('src/services/orderService.ts:30');
        expect(candidates[1].textContent).toContain('src/services/userService.ts:40');
        expect(find('atlas-chat-choice-head')?.textContent).toContain('2 symbols');
    });

    it('reports the pick instead of resolving it itself', async () => {
        const { onPickCandidate } = await render({ turns: [CHOICE_TURN] });
        await act(async () => {
            all('atlas-chat-candidate')[1].click();
        });
        expect(onPickCandidate).toHaveBeenCalledWith(
            CHOICE_TURN,
            CHOICE_TURN.choice?.candidates[1],
        );
    });

    it('shows no answer and no agreed sentence while it waits', async () => {
        await render({ turns: [CHOICE_TURN] });
        expect(find('atlas-chat-answer')).toBeNull();
        expect(find('atlas-chat-message')).toBeNull();
    });
});

describe('a wasted focus is worse than a wrong answer', () => {
    it('says in its own line that the answer is about the symbol in focus', async () => {
        await render({
            turns: [turnOf({
                question: '@createuser explain this function',
                focusFallback: { asked: 'createuser', used: 'createUser' },
            })],
        });
        const line = find('atlas-chat-fallback')?.textContent ?? '';
        expect(line).toContain('createuser');
        expect(line).toContain('createUser');
        expect(line).toContain('not found in the index');
    });

    it('says nothing of the sort when the name resolved', async () => {
        await render();
        expect(find('atlas-chat-fallback')).toBeNull();
    });
});

describe('the panel behaves like a panel', () => {
    /*
     * Since W8 the chat is a tab of the explain zone. The handle, the fold
     * button and the folded one-liner belong to the zone, and they are proven
     * where they live (src/layout, tools/smoke-w8.mjs). What is proven HERE is
     * that they are not here twice: two handles on top of each other, or two
     * buttons of which one folds the panel and one folds the zone, would be the
     * duplication this cycle removed.
     */
    it('leaves the height and the folding to the zone it sits in', async () => {
        await render();
        expect(find('atlas-chat-resize')).toBeNull();
        expect(find('atlas-chat-close')).toBeNull();
        expect(find('atlas-chat-collapsed')).toBeNull();
        expect(find('atlas-chat')?.style.height).toBe('');
    });

    /*
     * Die eine Zusage aus W7c, die dieses Panel selbst haelt: clear ist der
     * einzige Knopf, der loescht, und er sagt es. Sein Tooltip nennt seit W8
     * ausserdem, was Zuklappen NICHT kostet, weil das die Frage ist, die der
     * Nutzer am 2026-08-29 gestellt hat.
     */
    it('keeps clear as the only button that deletes, and says what it costs', async () => {
        const { onClear } = await render();
        const clear = find('atlas-chat-clear');
        expect(clear?.getAttribute('data-hint')).toContain('deleted');
        expect(clear?.getAttribute('data-hint')).toContain('Folding');
        await act(async () => {
            clear?.click();
        });
        expect(onClear).toHaveBeenCalled();
    });

    it('keeps the turns it was given, and says how many', async () => {
        await render();
        expect(find('atlas-chat')?.getAttribute('data-turns')).toBe('1');
        expect(container.querySelector('.atlas-chat-turns')).not.toBeNull();
    });

    it('keeps the head out of the scrolling box, so the context chips stay put', async () => {
        await render();
        const head = find('atlas-chat-head');
        const scroll = find('atlas-chat-scroll');
        expect(head).not.toBeNull();
        expect(scroll).not.toBeNull();
        expect(scroll?.contains(head ?? null)).toBe(false);
        expect(head?.contains(find('atlas-chat-depth'))).toBe(true);
        expect(scroll?.querySelector('.atlas-chat-turns')).not.toBeNull();
    });
});
