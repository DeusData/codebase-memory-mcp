// @vitest-environment jsdom
/*
 * Der Griff in jsdom: was er meldet, was die Tastatur mit ihm macht und was ein
 * Doppelklick.
 *
 * Was hier NICHT geprueft wird, ist der Zug mit der Maus: jsdom hat keine
 * Zeigererfassung und kein Layout, also waere ein nachgestellter Zug eine
 * Pruefung des Nachbaus. Dass gezogen werden kann und dass sich dabei wirklich
 * ein Rechteck aendert, misst der Beweislauf im Browser (tools/smoke-w8.mjs,
 * allFourSplittersDrag).
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import Splitter from './Splitter';
import type { SplitterProps } from './Splitter';
import { LAYOUT_BIG_STEP, LAYOUT_STEP } from './layout-model';

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

function props(overrides: Partial<SplitterProps> = {}): SplitterProps {
    return {
        testId: 'atlas-split-left',
        orientation: 'vertical',
        label: 'width of the explorer',
        value: 260,
        min: 180,
        max: 520,
        onChange: vi.fn(),
        onReset: vi.fn(),
        ...overrides,
    };
}

async function render(overrides: Partial<SplitterProps> = {}): Promise<SplitterProps> {
    const next = props(overrides);
    await act(async () => {
        root.render(<Splitter {...next} />);
    });
    return next;
}

const handle = (): HTMLElement =>
    container.querySelector<HTMLElement>('[data-testid="atlas-split-left"]')!;

const press = async (key: string, shiftKey = false): Promise<void> => {
    await act(async () => {
        handle().dispatchEvent(new KeyboardEvent('keydown', { key, shiftKey, bubbles: true }));
    });
};

describe('der Griff sagt, was er ist', () => {

    it('traegt die Rolle einer verschiebbaren Trennlinie mit ihrem Bereich', async () => {
        await render();
        const node = handle();
        expect(node.getAttribute('role')).toBe('separator');
        expect(node.getAttribute('aria-orientation')).toBe('vertical');
        expect(node.getAttribute('aria-valuenow')).toBe('260');
        expect(node.getAttribute('aria-valuemin')).toBe('180');
        expect(node.getAttribute('aria-valuemax')).toBe('520');
        expect(node.getAttribute('aria-label')).toBe('width of the explorer');
    });

    /*
     * Ein Griff, den nur die Maus erreicht, ist ein halber Griff. Der Fokus ist
     * die Bedingung dafuer, dass die Pfeiltasten ihn ueberhaupt treffen koennen.
     */
    it('ist fokussierbar und sagt in Worten, wo er steht', async () => {
        await render();
        expect(handle().tabIndex).toBe(0);
        expect(handle().getAttribute('aria-valuetext')).toBe('260 pixels');
    });

    /*
     * Der Beipackzettel ist weg (W10b, AC1).
     *
     * Nutzerauftrag vom 2026-08-29: "Bitte an allen Bordern die Meldung
     * entfernen." Gemessen wird die Abwesenheit des Kastens und die Anwesenheit
     * von allem, was er nicht ersetzt hat: Name, Lage, Fokus, Tasten. Die Tasten
     * selbst pruefen die Faelle darunter.
     */
    it('traegt keinen Kasten mehr, der erklaert, was man sieht', async () => {
        await render();
        const node = handle();
        expect(node.getAttribute('data-hint')).toBe(null);
        expect(node.getAttribute('title')).toBe(null);
        expect(node.getAttribute('aria-describedby')).toBe(null);
        expect(document.querySelectorAll('[data-testid="atlas-hint"]').length).toBe(0);
    });
});

describe('die Tastatur am Griff', () => {

    it('bewegt eine senkrechte Linie mit links und rechts', async () => {
        const { onChange } = await render();
        await press('ArrowRight');
        expect(onChange).toHaveBeenLastCalledWith(260 + LAYOUT_STEP);
        await press('ArrowLeft');
        expect(onChange).toHaveBeenLastCalledWith(260 - LAYOUT_STEP);
    });

    it('bewegt eine waagerechte Linie mit oben und unten', async () => {
        const { onChange } = await render({ orientation: 'horizontal', value: 340, min: 150, max: 600 });
        await press('ArrowDown');
        expect(onChange).toHaveBeenLastCalledWith(340 + LAYOUT_STEP);
        await press('ArrowUp');
        expect(onChange).toHaveBeenLastCalledWith(340 - LAYOUT_STEP);
    });

    it('macht mit Shift den grossen Schritt', async () => {
        const { onChange } = await render();
        await press('ArrowRight', true);
        expect(onChange).toHaveBeenLastCalledWith(260 + LAYOUT_BIG_STEP);
    });

    /*
     * Die Umkehr ist kein Geschmack: der Bereich unter dem Reader waechst nach
     * OBEN. Ohne das Vorzeichen liefe die Kante vor dem Zeiger weg.
     */
    it('dreht die Richtung, wo die Zone nach oben waechst', async () => {
        const { onChange } = await render({ orientation: 'horizontal', invert: true, value: 340, min: 150, max: 600 });
        await press('ArrowUp');
        expect(onChange).toHaveBeenLastCalledWith(340 + LAYOUT_STEP);
    });

    it('laesst die Grenzen nicht ueberschreiten', async () => {
        const atMax = await render({ value: 520 });
        await press('ArrowRight');
        expect(atMax.onChange).toHaveBeenLastCalledWith(520);
        const atMin = await render({ value: 180 });
        await press('ArrowLeft');
        expect(atMin.onChange).toHaveBeenLastCalledWith(180);
    });

    it('nimmt keine Taste, die ihn nichts angeht', async () => {
        const { onChange } = await render();
        await press('ArrowUp');
        await press('Enter');
        expect(onChange).not.toHaveBeenCalled();
    });
});

describe('der Weg zurueck', () => {

    it('meldet einen Doppelklick als Ruecksetzen genau dieser Grenze', async () => {
        const { onReset, onChange } = await render();
        await act(async () => {
            handle().dispatchEvent(new MouseEvent('dblclick', { bubbles: true }));
        });
        expect(onReset).toHaveBeenCalledTimes(1);
        expect(onChange).not.toHaveBeenCalled();
    });
});
