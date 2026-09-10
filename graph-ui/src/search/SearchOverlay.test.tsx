// @vitest-environment jsdom
/*
 * Ein Suchtreffer ist ein echter Tab-Stopp. Deshalb muss er seine eigene
 * Tastatur bedienen koennen; der Cursorweg ueber die Kommandozeile ist ein
 * zweiter Weg und kein Ersatz fuer Enter oder Space auf dem Treffer selbst.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import SearchOverlay from './SearchOverlay';

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
});

afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
});

async function render(onChoose: (index: number) => void): Promise<HTMLButtonElement> {
    await act(async () => {
        root.render(
            <SearchOverlay
                headline="one result"
                rows={[{
                    key: 'createUser',
                    name: 'createUser',
                    path: 'src/services/userService.ts',
                    line: 'L23',
                    matched: 'user',
                    source: 'index',
                }]}
                selected={0}
                status="ready"
                message=""
                onChoose={onChoose}
                onPoint={() => undefined}
            />,
        );
    });
    return container.querySelector('[data-testid="atlas-search-row"]') as HTMLButtonElement;
}

describe('die Tastatur am Suchtreffer selbst', () => {
    it.each([
        ['Enter', 'Enter'],
        ['Space', ' '],
    ])('%s waehlt denselben Treffer wie die Maus', async (_name, key) => {
        const choose = vi.fn();
        const row = await render(choose);

        await act(async () => {
            row.focus();
            row.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true }));
        });

        expect(choose).toHaveBeenCalledOnce();
        expect(choose).toHaveBeenCalledWith(0);
    });
});
