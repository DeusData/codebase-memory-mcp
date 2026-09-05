// @vitest-environment jsdom
/**
 * Die Schrittkarte in jsdom: die Testmarken, die der Beweislauf im Browser
 * sucht, die Beschriftung der Tasten und die Stelle, an der ein gekappter Walk
 * das zugibt.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import type { TourStepRecord } from '../core/tour-protocol';
import TourCard from './TourCard';
import type { TourCardProps } from './TourCard';

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

function stepAt(order: number): TourStepRecord {
    return {
        id: `s${order}`,
        title: `Configuration: src/c${order}.ts`,
        description: `Sentence about step ${order}.`,
        order,
        primary: { kind: 'file', filePath: `src/c${order}.ts` },
    };
}

/** Ein Schritt, den der Index auf ein Symbol aufgeloest hat. */
function symbolStepAt(order: number): TourStepRecord {
    return {
        ...stepAt(order),
        primary: {
            kind: 'symbol',
            filePath: `src/c${order}.ts`,
            line: 12,
            name: 'createUser',
            qualifiedName: 'src.services.userService.createUser',
            symbolKind: 'function',
        },
    };
}

function props(overrides: Partial<TourCardProps> = {}): TourCardProps {
    return {
        title: 'Getting started',
        steps: [stepAt(0), stepAt(1), stepAt(2)],
        index: 0,
        endNote: '',
        onPrev: vi.fn(),
        onNext: vi.fn(),
        onExit: vi.fn(),
        ...overrides,
    };
}

async function render(given: TourCardProps): Promise<void> {
    await act(async () => {
        root.render(<TourCard {...given} />);
    });
}

const at = (id: string): HTMLElement | null => container.querySelector(`[data-testid="${id}"]`);

describe('the step card', () => {
    it('carries the mark the proof run looks for, and where it stands', async () => {
        await render(props({ index: 1 }));
        const card = at('atlas-tour');
        expect(card).not.toBeNull();
        expect(card?.getAttribute('data-step')).toBe('2');
        expect(card?.getAttribute('data-steps')).toBe('3');
        expect(card?.getAttribute('data-tour-id')).toBe('s1');
    });

    it('shows the step count and a chain of blocks', async () => {
        await render(props({ index: 1 }));
        expect(at('atlas-tour-progress')?.textContent).toBe('STEP 2/3');
        expect(container.querySelector('.atlas-tour-bar')?.textContent).toBe('▓▓░');
    });

    it('shows the title and the sentences of the step it is on', async () => {
        await render(props({ index: 2 }));
        expect(at('atlas-tour-title')?.textContent).toContain('Configuration: src/c2.ts');
        expect(at('atlas-tour-description')?.textContent).toBe('Sentence about step 2.');
    });

    it('writes every key into the label of its own button', async () => {
        await render(props());
        expect(at('atlas-tour-prev')?.textContent).toBe('[<-] prev');
        expect(at('atlas-tour-next')?.textContent).toBe('[Enter] next');
        expect(at('atlas-tour-diagram')?.textContent).toBe('[d] diagram');
        expect(at('atlas-tour-exit')?.textContent).toBe('[q] exit');
    });

    it('calls the last step finish rather than growing a fourth button', async () => {
        await render(props({ index: 2 }));
        expect(at('atlas-tour-next')?.textContent).toBe('[Enter] finish');
    });

    it('offers no way back from the first step', async () => {
        await render(props({ index: 0 }));
        expect((at('atlas-tour-prev') as HTMLButtonElement).disabled).toBe(true);
        expect(at('atlas-tour-prev')?.getAttribute('data-hint')).toContain('first step');
        expect(at('atlas-tour-prev')?.getAttribute('data-hint')).toContain('no previous step');
        await render(props({ index: 1 }));
        expect((at('atlas-tour-prev') as HTMLButtonElement).disabled).toBe(false);
    });

    it('moves on a click, through the callback it was handed', async () => {
        const onNext = vi.fn();
        const onPrev = vi.fn();
        const onExit = vi.fn();
        await render(props({ index: 1, onNext, onPrev, onExit }));
        await act(async () => {
            (at('atlas-tour-next') as HTMLButtonElement).click();
            (at('atlas-tour-prev') as HTMLButtonElement).click();
            (at('atlas-tour-exit') as HTMLButtonElement).click();
        });
        expect(onNext).toHaveBeenCalledTimes(1);
        expect(onPrev).toHaveBeenCalledTimes(1);
        expect(onExit).toHaveBeenCalledTimes(1);
    });

    it('draws nothing at all for a walk with no steps', async () => {
        await render(props({ steps: [] }));
        expect(at('atlas-tour')).toBeNull();
    });
});

/*
 * Befund 13 des unabhaengigen Audits vom 2026-08-29.
 *
 * PLAN Abschnitt 4 zaehlt `[d] diagram` zu den Aktionen der unteren Karte; das
 * Produkt hatte den Erklaerer stattdessen an den flow()-Kopf des Twins gehaengt.
 * Die Karte hat die Aktion jetzt, und der interessantere Teil ist der Fall, in
 * dem es nichts zu zeichnen gibt.
 */
describe('die Aktion [d] diagram', () => {
    it('ist an einem Schritt mit Symbol bedienbar und ruft ihren Rueckruf', async () => {
        const onDiagram = vi.fn();
        await render(props({ steps: [symbolStepAt(0)], index: 0, onDiagram }));
        const button = at('atlas-tour-diagram') as HTMLButtonElement;
        expect(button.disabled).toBe(false);
        expect(button.getAttribute('data-available')).toBe('true');
        await act(async () => {
            button.click();
        });
        expect(onDiagram).toHaveBeenCalledTimes(1);
    });

    it('ist an einem Dateischritt aus, statt zu verschwinden', async () => {
        const onDiagram = vi.fn();
        await render(props({ steps: [stepAt(0)], index: 0, onDiagram }));
        const button = at('atlas-tour-diagram') as HTMLButtonElement;
        expect(button).not.toBeNull();
        expect(button.disabled).toBe(true);
        expect(button.getAttribute('data-available')).toBe('false');
    });

    it('sagt im Tooltip, warum es aus ist, und nennt dabei den Schritt', async () => {
        await render(props({ steps: [stepAt(0)], index: 0, onDiagram: vi.fn() }));
        const title = at('atlas-tour-diagram')?.getAttribute('data-hint') ?? '';
        expect(title).toContain('this step');
        expect(title).toContain('not at a symbol');
    });

    it('sagt im Tooltip etwas anderes, wenn es an ist', async () => {
        await render(props({ steps: [symbolStepAt(0)], index: 0, onDiagram: vi.fn() }));
        const title = at('atlas-tour-diagram')?.getAttribute('data-hint') ?? '';
        expect(title).toContain('flow');
        expect(title).not.toContain('not at a symbol');
    });

    it('bleibt aus, wenn der Aufrufer gar kein Bild anzubieten hat', async () => {
        await render(props({ steps: [symbolStepAt(0)], index: 0 }));
        expect((at('atlas-tour-diagram') as HTMLButtonElement).disabled).toBe(true);
    });

    it('wechselt mit dem Schritt, nicht mit dem Walk', async () => {
        const steps = [symbolStepAt(0), stepAt(1)];
        await render(props({ steps, index: 0, onDiagram: vi.fn() }));
        expect((at('atlas-tour-diagram') as HTMLButtonElement).disabled).toBe(false);
        await render(props({ steps, index: 1, onDiagram: vi.fn() }));
        expect((at('atlas-tour-diagram') as HTMLButtonElement).disabled).toBe(true);
    });
});

describe('what a bounded walk says, and where', () => {
    const capped = 'walk capped at 3 symbols (depth 3), so what follows this is not shown';

    it('says it at the end of the walk', async () => {
        await render(props({ index: 2, endNote: capped }));
        expect(at('atlas-tour-cap')?.textContent).toBe(capped);
    });

    it('does not say it on every step, which would make it a fact about each one', async () => {
        await render(props({ index: 0, endNote: capped }));
        expect(at('atlas-tour-cap')).toBeNull();
    });

    it('says nothing when the walk reached the end of the graph', async () => {
        await render(props({ index: 2, endNote: '' }));
        expect(at('atlas-tour-cap')).toBeNull();
    });
});
