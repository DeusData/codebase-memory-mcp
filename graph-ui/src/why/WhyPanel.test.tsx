// @vitest-environment jsdom
/**
 * Die Frage in jsdom.
 *
 * Die eine Pruefung, die hier mehr wert ist als alle anderen: der sichtbare Text
 * der ganzen Flaeche gegen die Wortliste. Der Browser-Lauf prueft dasselbe am
 * fertigen Bild; diese Suite faengt es beim Schreiben ab.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import WhyPanel from './WhyPanel';
import { WHY_CARDS, WHY_HEADLINE, avoidedWordsIn } from './why-model';

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

async function render(onChoose = vi.fn(), onDecline = vi.fn()): Promise<void> {
    await act(async () => {
        root.render(<WhyPanel project="atlas-sample" onChoose={onChoose} onDecline={onDecline} />);
    });
}

const cards = (): HTMLButtonElement[] =>
    [...container.querySelectorAll<HTMLButtonElement>('[data-testid="atlas-why-card"]')];

describe('the panel', () => {
    it('carries the mark the proof run looks for and asks its question', async () => {
        await render();
        expect(container.querySelector('[data-testid="atlas-why"]')).not.toBeNull();
        expect(container.querySelector('[data-testid="atlas-why-headline"]')?.textContent).toBe(WHY_HEADLINE);
    });

    it('offers the four ways in, in the order of a working day', async () => {
        await render();
        expect(cards().map((card) => card.getAttribute('data-intent'))).toEqual([
            'bug',
            'change',
            'understand',
            'entry',
        ]);
    });

    it('draws no note about a card doing less than it says, because none of them does', async () => {
        await render();
        expect([...container.querySelectorAll('[data-testid="atlas-why-stub"]')]).toHaveLength(0);
    });

    it('hands the chosen intent back untouched', async () => {
        const onChoose = vi.fn();
        await render(onChoose);
        await act(async () => {
            cards()[2].click();
        });
        expect(onChoose).toHaveBeenCalledWith('understand');
    });

    it('treats the decline as its own answer', async () => {
        const onDecline = vi.fn();
        await render(vi.fn(), onDecline);
        await act(async () => {
            (container.querySelector('[data-testid="atlas-why-decline"]') as HTMLButtonElement).click();
        });
        expect(onDecline).toHaveBeenCalledTimes(1);
    });

    it('names the project the answer is about', async () => {
        await render();
        expect(container.textContent).toContain('atlas-sample');
    });

    it('uses none of the words this product does not use about its reader', async () => {
        await render();
        expect(avoidedWordsIn(container.textContent ?? '')).toEqual([]);
        // And the check is not vacuous: the cards really are on screen.
        expect(container.textContent).toContain(WHY_CARDS[2].label);
    });
});
