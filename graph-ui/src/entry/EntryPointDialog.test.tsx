// @vitest-environment jsdom
/**
 * Der Einstiegsdialog in jsdom.
 *
 * Zwei Listen und ein Feld, und die Pruefungen sind genau die drei Stellen, an
 * denen der Dialog etwas behaupten koennte: dass eine Zeile ihre Herkunft nennt,
 * dass eine Zeile ohne Datei nicht anklickbar ist, und dass das Feld dieselbe
 * Suche fuettert, die auch die Kommandozeile fuettert.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import EntryPointDialog, { ENTRY_TITLE } from './EntryPointDialog';
import type { EntryPointDialogProps } from './EntryPointDialog';
import { avoidedWordsIn } from '../why/why-model';

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

function props(overrides: Partial<EntryPointDialogProps> = {}): EntryPointDialogProps {
    return {
        headline: '3 ways in the index flagged',
        rows: [
            {
                key: 'symbol:p.createUser',
                name: 'createUser',
                where: 'src/services/userService.ts:23',
                origin: 'entry point',
                target: {
                    name: 'createUser',
                    qualifiedName: 'p.createUser',
                    kind: 'function',
                    filePath: 'src/services/userService.ts',
                    startLine: 23,
                },
            },
            { key: 'symbol:p.nowhere', name: 'nowhere', where: '', origin: 'entry point' },
            { key: 'route:GET /users', name: 'GET /users', where: 'src/routes/users.ts:9', origin: 'route (read from the source)' },
        ],
        query: '',
        onQueryChange: vi.fn(),
        hits: [],
        status: 'idle',
        message: '',
        routeNote: '',
        onChooseFlagged: vi.fn(),
        onChooseHit: vi.fn(),
        onClose: vi.fn(),
        ...overrides,
    };
}

async function render(given: EntryPointDialogProps): Promise<void> {
    await act(async () => {
        root.render(<EntryPointDialog {...given} />);
    });
}

const rows = (): HTMLButtonElement[] =>
    [...container.querySelectorAll<HTMLButtonElement>('[data-testid="atlas-entry-row"]')];

describe('the dialog', () => {
    it('carries the mark the proof run looks for', async () => {
        await render(props());
        expect(container.querySelector('[data-testid="atlas-entry"]')).not.toBeNull();
        expect(container.textContent).toContain(ENTRY_TITLE);
    });

    it('lists the ways in with where they came from', async () => {
        await render(props());
        expect(rows().map((row) => row.getAttribute('data-name'))).toEqual([
            'createUser',
            'nowhere',
            'GET /users',
        ]);
        expect(rows()[2].textContent).toContain('route (read from the source)');
    });

    it('shows a way in with no file, and refuses to pretend it opens', async () => {
        await render(props());
        expect(rows()[1].disabled).toBe(true);
        expect(rows()[1].getAttribute('data-openable')).toBe('false');
        expect(rows()[1].getAttribute('data-hint') ?? '').toContain('no file');
    });

    it('hands the chosen row back by its key', async () => {
        const onChooseFlagged = vi.fn();
        await render(props({ onChooseFlagged }));
        await act(async () => {
            rows()[0].dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
        });
        expect(onChooseFlagged).toHaveBeenCalledWith('symbol:p.createUser');
    });

    it.each([
        ['Enter', 'Enter'],
        ['Space', ' '],
    ])('%s chooses a focused flagged row directly', async (_name, key) => {
        const onChooseFlagged = vi.fn();
        await render(props({ onChooseFlagged }));
        await act(async () => {
            rows()[0].focus();
            rows()[0].dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true }));
        });
        expect(onChooseFlagged).toHaveBeenCalledOnce();
        expect(onChooseFlagged).toHaveBeenCalledWith('symbol:p.createUser');
    });

    it('reports what is typed without deciding anything about it', async () => {
        const onQueryChange = vi.fn();
        await render(props({ onQueryChange }));
        const input = container.querySelector('[data-testid="atlas-entry-input"]') as HTMLInputElement;
        // Der native Setter, weil React den eigenen Wert verfolgt und ein
        // direkt gesetztes `value` als "unveraendert" liest.
        const setValue = Object.getOwnPropertyDescriptor(
            window.HTMLInputElement.prototype,
            'value',
        )?.set;
        await act(async () => {
            setValue?.call(input, 'createUser');
            input.dispatchEvent(new Event('input', { bubbles: true }));
        });
        expect(onQueryChange).toHaveBeenCalledWith('createUser');
    });

    it('draws no hit list before anything was searched for', async () => {
        await render(props());
        expect(container.querySelector('[data-testid="atlas-entry-hits"]')).toBeNull();
    });

    it('draws the hits of the meaning search and hands one back by index', async () => {
        const onChooseHit = vi.fn();
        await render(props({
            query: 'create',
            status: 'ready',
            hits: [
                { key: 'p.createUser', name: 'createUser', path: 'src/services/userService.ts', line: 'L23', matched: 'create', source: 'index' },
                { key: 'p.create', name: 'create', path: 'src/services/orderService.ts', line: 'L30', matched: 'create', source: 'index' },
            ],
            onChooseHit,
        }));
        const hits = [...container.querySelectorAll<HTMLButtonElement>('[data-testid="atlas-entry-hit"]')];
        expect(hits.map((hit) => hit.getAttribute('data-name'))).toEqual(['createUser', 'create']);
        await act(async () => {
            hits[1].dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
        });
        expect(onChooseHit).toHaveBeenCalledWith(1);
    });

    it.each([
        ['Enter', 'Enter'],
        ['Space', ' '],
    ])('%s chooses a focused meaning-search hit directly', async (_name, key) => {
        const onChooseHit = vi.fn();
        await render(props({
            query: 'create',
            status: 'ready',
            hits: [
                { key: 'p.createUser', name: 'createUser', path: 'src/services/userService.ts', line: 'L23', matched: 'create', source: 'index' },
            ],
            onChooseHit,
        }));
        const hit = container.querySelector('[data-testid="atlas-entry-hit"]') as HTMLButtonElement;
        await act(async () => {
            hit.focus();
            hit.dispatchEvent(new KeyboardEvent('keydown', { key, bubbles: true }));
        });
        expect(onChooseHit).toHaveBeenCalledOnce();
        expect(onChooseHit).toHaveBeenCalledWith(0);
    });

    it('says what went wrong where the reader is looking', async () => {
        await render(props({ status: 'failed', message: 'the search could not be answered' }));
        const message = container.querySelector('[data-testid="atlas-entry-message"]');
        expect(message?.textContent).toContain('could not be answered');
        expect(message?.getAttribute('data-status')).toBe('failed');
    });

    it('explains an absent route family where the list would be, or not at all', async () => {
        await render(props());
        expect(container.querySelector('[data-testid="atlas-entry-route-note"]')).toBeNull();
        await render(props({ routeNote: 'no route is listed: the index reported none' }));
        expect(container.querySelector('[data-testid="atlas-entry-route-note"]')?.textContent)
            .toContain('the index reported none');
    });

    it('closes through the callback it was handed', async () => {
        const onClose = vi.fn();
        await render(props({ onClose }));
        await act(async () => {
            (container.querySelector('[data-testid="atlas-entry-close"]') as HTMLButtonElement).click();
        });
        expect(onClose).toHaveBeenCalledTimes(1);
    });

    it('uses none of the words this product does not use about its reader', async () => {
        await render(props());
        expect(avoidedWordsIn(container.textContent ?? '')).toEqual([]);
    });
});
