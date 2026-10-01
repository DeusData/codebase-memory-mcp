// @vitest-environment jsdom
/*
 * Der Erklaeren-Bereich in jsdom: die Reiterleiste, der eine sichtbare Inhalt,
 * der Grund eines leeren Reiters und der eingeklappte Streifen.
 *
 * Die Zusicherung "immer nur ein Reiter sichtbar" wird hier an der FORM
 * gemessen und nicht an einer Farbe: es steht hoechstens ein Inhaltskasten im
 * Baum. Dass sich im Browser auch wirklich nichts ueberlagert, misst
 * tools/smoke-w8.mjs; das ist die andere Haelfte derselben Frage.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import ExplainZone from './ExplainZone';
import type { ExplainZoneProps } from './ExplainZone';
import { explainTabs } from './explain-tabs';

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

const ALL_ENABLED = explainTabs({
    hasProject: true,
    flowSubject: 'createUser',
    flowStep: 2,
    walkRunning: true,
    walkStep: 1,
    walkSteps: 5,
    chatTurns: 3,
});

const NONE_ENABLED = explainTabs({
    hasProject: false,
    flowSubject: '',
    flowStep: -1,
    walkRunning: false,
    walkStep: 0,
    walkSteps: 0,
    chatTurns: 0,
});

function props(overrides: Partial<ExplainZoneProps> = {}): ExplainZoneProps {
    return {
        tabs: ALL_ENABLED,
        active: 'flow',
        onSelect: vi.fn(),
        open: true,
        onToggle: vi.fn(),
        height: 340,
        children: <div data-testid="fake-panel" />,
        ...overrides,
    };
}

async function render(overrides: Partial<ExplainZoneProps> = {}): Promise<ExplainZoneProps> {
    const next = props(overrides);
    await act(async () => {
        root.render(<ExplainZone {...next} />);
    });
    return next;
}

const find = (testid: string): HTMLElement | null =>
    container.querySelector<HTMLElement>(`[data-testid="${testid}"]`);
const all = (testid: string): HTMLElement[] =>
    [...container.querySelectorAll<HTMLElement>(`[data-testid="${testid}"]`)];

describe('die Reiterleiste', () => {

    it('zeichnet jeden Reiter, auch die ohne Inhalt', async () => {
        await render({ tabs: NONE_ENABLED });
        expect(all('atlas-explain-tab').map((tab) => tab.getAttribute('data-tab')))
            .toEqual(['flow', 'walk', 'chat', 'bug', 'change']);
    });

    it('markiert genau einen Reiter als gewaehlt', async () => {
        await render({ active: 'chat' });
        const on = all('atlas-explain-tab').filter((tab) => tab.getAttribute('data-on') === 'true');
        expect(on).toHaveLength(1);
        expect(on[0]?.getAttribute('data-tab')).toBe('chat');
        expect(on[0]?.getAttribute('aria-selected')).toBe('true');
    });

    it('gibt einen Reiterwechsel nach oben weiter', async () => {
        const { onSelect } = await render();
        await act(async () => {
            all('atlas-explain-tab').find((tab) => tab.getAttribute('data-tab') === 'walk')?.click();
        });
        expect(onSelect).toHaveBeenCalledWith('walk');
    });

    /*
     * Ein Reiter ohne Inhalt bleibt anklickbar. `disabled` waere die bequeme
     * Loesung und die stumme: wer ihn drueckt, bekommt den Grund zu lesen.
     */
    it('laesst einen leeren Reiter gedimmt, aber bedienbar', async () => {
        const { onSelect } = await render({ tabs: NONE_ENABLED });
        const flow = all('atlas-explain-tab').find((tab) => tab.getAttribute('data-tab') === 'flow');
        expect(flow?.getAttribute('data-enabled')).toBe('false');
        expect(flow?.hasAttribute('disabled')).toBe(false);
        await act(async () => {
            flow?.click();
        });
        expect(onSelect).toHaveBeenCalledWith('flow');
    });
});

describe('immer genau ein Inhalt', () => {

    it('zeichnet genau einen Inhaltskasten, wenn der Bereich offen ist', async () => {
        await render();
        expect(all('atlas-explain-panel')).toHaveLength(1);
        expect(find('fake-panel')).not.toBeNull();
    });

    it('zeichnet gar keinen, wenn der Bereich eingeklappt ist', async () => {
        await render({ open: false });
        expect(all('atlas-explain-panel')).toHaveLength(0);
        expect(find('fake-panel')).toBeNull();
        expect(find('atlas-explain')?.getAttribute('data-open')).toBe('false');
    });

    /*
     * Der Kern der Ehrlichkeitsregel: ein Reiter ohne Inhalt sagt WARUM, und
     * zwar im Feld und nicht in einem Tooltip.
     */
    it('setzt den Grund an die Stelle des Inhalts, wenn der Reiter nichts hat', async () => {
        await render({ tabs: NONE_ENABLED, active: 'walk' });
        expect(find('fake-panel')).toBeNull();
        const empty = find('atlas-explain-empty');
        expect(empty?.getAttribute('data-tab')).toBe('walk');
        expect(empty?.textContent ?? '').toContain('No walk is running');
        expect(find('atlas-explain-panel')?.getAttribute('data-state')).toBe('empty');
    });
});

describe('der eingeklappte Streifen', () => {

    it('sagt, was hinter dem gewaehlten Reiter noch dasteht', async () => {
        await render({ open: false, active: 'chat' });
        expect(find('atlas-explain-note')?.textContent).toContain('3 questions');
    });

    it('zeigt die Zeile nicht, solange der Inhalt selbst dasteht', async () => {
        await render({ open: true, active: 'chat' });
        expect(find('atlas-explain-note')).toBeNull();
    });

    it('bietet denselben Knopf zum Auf- und Zuklappen an', async () => {
        const closed = await render({ open: false });
        expect(find('atlas-explain-collapse')?.getAttribute('aria-expanded')).toBe('false');
        await act(async () => {
            find('atlas-explain-collapse')?.click();
        });
        expect(closed.onToggle).toHaveBeenCalledTimes(1);
        const open = await render({ open: true });
        expect(find('atlas-explain-collapse')?.getAttribute('aria-expanded')).toBe('true');
        expect(find('atlas-explain-collapse')?.getAttribute('data-hint')).toContain('Nothing in it is lost');
        await act(async () => {
            find('atlas-explain-collapse')?.click();
        });
        expect(open.onToggle).toHaveBeenCalledTimes(1);
    });

    it('traegt die Hoehe der Zone nur, solange sie offen ist', async () => {
        await render({ open: true, height: 420 });
        expect(find('atlas-explain')?.style.height).toBe('420px');
        await render({ open: false, height: 420 });
        expect(find('atlas-explain')?.style.height).toBe('');
    });
});
