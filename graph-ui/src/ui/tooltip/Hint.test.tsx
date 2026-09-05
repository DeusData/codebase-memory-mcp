// @vitest-environment jsdom
/*
 * Der eigene Tooltip in jsdom: wann er da ist, wo er steht und wie man ihn
 * wieder los wird.
 *
 * Was hier NICHT geprueft wird, ist seine Lage in Pixeln: jsdom hat kein
 * Layout, jedes Rechteck ist null mal null, und eine Rechnung ueber Nullen
 * bewiese nichts. Die Rechnung selbst steht ohne DOM in tooltip-model.test.ts,
 * und dass sie in einem echten Fenster nichts verdeckt, misst der Beweislauf
 * (tools/smoke-w8b.mjs, tooltipCoversNothing).
 *
 * Hier steht die andere Haelfte, und sie ist die, die ein Browserlauf schlecht
 * sieht: dass es den Kasten UEBERHAUPT nur gibt, wenn er etwas zu sagen hat,
 * dass die Tastatur ihn oeffnet, dass Escape ihn schliesst, und dass der
 * Ausloeser seine eigenen Rueckrufe behaelt.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import Hint from './Hint';

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

const tooltip = (): Element | null => document.body.querySelector('[data-testid="atlas-hint"]');
const trigger = (): HTMLButtonElement =>
    container.querySelector('button') as HTMLButtonElement;

async function render(node: React.ReactElement): Promise<void> {
    await act(async () => {
        root.render(node);
    });
}

describe('Hint', () => {
    it('zeichnet keinen Kasten, solange niemand hinsieht', async () => {
        await render(<Hint name="probe" text="what this does"><button type="button">go</button></Hint>);
        expect(tooltip()).toBeNull();
        expect(trigger().getAttribute('data-hint')).toBe('what this does');
        expect(trigger().hasAttribute('title')).toBe(false);
    });

    it('oeffnet bei Hover und schliesst wieder', async () => {
        await render(<Hint name="probe" text="what this does"><button type="button">go</button></Hint>);
        await act(async () => {
            trigger().dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
        });
        expect(tooltip()?.textContent).toBe('what this does');
        expect(tooltip()?.getAttribute('role')).toBe('tooltip');
        await act(async () => {
            trigger().dispatchEvent(new MouseEvent('mouseout', { bubbles: true }));
        });
        expect(tooltip()).toBeNull();
    });

    /*
     * Die Zusicherung, die dem Contract wichtiger ist als jede andere: ein
     * Tooltip, den nur die Maus oeffnet, ist keiner. Diese Oberflaeche ist an
     * der Tastatur entworfen (PLAN Abschnitt 4).
     */
    it('oeffnet bei Fokus und traegt dann aria-describedby am Ausloeser', async () => {
        await render(<Hint name="probe" text="what this does"><button type="button">go</button></Hint>);
        await act(async () => {
            trigger().focus();
        });
        const box = tooltip();
        expect(box).not.toBeNull();
        expect(trigger().getAttribute('aria-describedby')).toBe(box?.getAttribute('id'));
    });

    it('schliesst mit Escape', async () => {
        await render(<Hint name="probe" text="what this does"><button type="button">go</button></Hint>);
        await act(async () => {
            trigger().focus();
        });
        expect(tooltip()).not.toBeNull();
        await act(async () => {
            window.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
        });
        expect(tooltip()).toBeNull();
    });

    /*
     * Ein `title`, der nur den sichtbaren Text wiederholt, faellt in W8b
     * ersatzlos weg, und die Aufrufer geben dafuer nichts oder einen leeren
     * Satz weiter. Der Kasten muss den Ausloeser dann unveraendert
     * durchreichen: eine Huelle, die nichts sagt, waere ein Element mehr im
     * Baum, das jede Messung mitzaehlt.
     */
    it('reicht den Ausloeser unveraendert durch, wenn es nichts zu sagen gibt', async () => {
        await render(<Hint name="probe" text=""><button type="button">go</button></Hint>);
        expect(trigger().hasAttribute('data-hint')).toBe(false);
        await act(async () => {
            trigger().dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
        });
        expect(tooltip()).toBeNull();
    });

    it('laesst dem Ausloeser seine eigenen Rueckrufe', async () => {
        const onClick = vi.fn();
        const onFocus = vi.fn();
        await render(
            <Hint name="probe" text="what this does">
                <button type="button" onClick={onClick} onFocus={onFocus}>go</button>
            </Hint>,
        );
        await act(async () => {
            trigger().focus();
        });
        expect(onFocus).toHaveBeenCalledTimes(1);
        expect(tooltip()).not.toBeNull();
        await act(async () => {
            trigger().click();
        });
        expect(onClick).toHaveBeenCalledTimes(1);
    });

    /*
     * Ein Klick schliesst ihn, und der Grund steht an der Stelle, an der es
     * gemacht wird (src/ui/tooltip/Hint.tsx): der Satz sagt, was ein Knopf TUN
     * WIRD, und nach dem Druecken ist er die Beschreibung von etwas, das schon
     * passiert ist. An den Schaltern dieser Oberflaeche ist er dann obendrein
     * falsch, weil ihre Beschriftung mit dem Zustand kippt.
     */
    it('schliesst sich, sobald der Ausloeser gedrueckt wurde', async () => {
        await render(<Hint name="probe" text="what this does"><button type="button">go</button></Hint>);
        await act(async () => {
            trigger().dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
        });
        expect(tooltip()).not.toBeNull();
        await act(async () => {
            trigger().click();
        });
        expect(tooltip()).toBeNull();
    });

    /*
     * Der Befund W10-1: ein Ausloeser, der NICHTS tut ausser diesen Satz zu
     * tragen, ist auf einem Zeigegeraet ohne Hover unerreichbar, und als
     * `<button>` verspricht er ausserdem eine Handlung, die es nicht gibt. Wer
     * den Griff nimmt, bekommt den Klick: einmal haelt fest, einmal laesst los.
     */
    describe('der Klick-Griff', () => {
        const held = (): string | null => trigger().getAttribute('data-hint-held');

        async function renderHeld(): Promise<void> {
            await render(
                <Hint name="probe" text="where this comes from">
                    {(hold) => (
                        <button type="button" aria-expanded={hold.held} onClick={hold.toggle}>?</button>
                    )}
                </Hint>,
            );
        }

        it('haelt den Kasten fest, ohne dass jemand hovert', async () => {
            await renderHeld();
            expect(tooltip()).toBeNull();
            await act(async () => {
                trigger().click();
            });
            expect(tooltip()?.textContent).toBe('where this comes from');
            expect(held()).toBe('true');
            expect(trigger().getAttribute('aria-expanded')).toBe('true');
        });

        it('bleibt stehen, wenn der Zeiger weiterzieht', async () => {
            await renderHeld();
            await act(async () => {
                trigger().dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
                trigger().click();
            });
            await act(async () => {
                trigger().dispatchEvent(new MouseEvent('mouseout', { bubbles: true }));
            });
            expect(tooltip()).not.toBeNull();
        });

        it('laesst ihn beim zweiten Klick wieder los', async () => {
            await renderHeld();
            await act(async () => {
                trigger().dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
                trigger().click();
            });
            expect(tooltip()).not.toBeNull();
            await act(async () => {
                trigger().click();
            });
            expect(tooltip()).toBeNull();
            expect(held()).toBe('false');
        });

        it('laesst ihn mit Escape los', async () => {
            await renderHeld();
            await act(async () => {
                trigger().click();
            });
            expect(tooltip()).not.toBeNull();
            await act(async () => {
                window.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
            });
            expect(tooltip()).toBeNull();
            expect(held()).toBe('false');
        });

        /*
         * Wer weiterarbeitet, meint dasselbe wie Escape. Ohne diese Zusicherung
         * bliebe ein festgehaltener Kasten ueber der Stelle stehen, an der es
         * gerade weitergeht.
         */
        it('laesst ihn los, wenn der naechste Griff woanders zupackt', async () => {
            await renderHeld();
            await act(async () => {
                trigger().click();
            });
            expect(tooltip()).not.toBeNull();
            await act(async () => {
                document.body.dispatchEvent(new MouseEvent('pointerdown', { bubbles: true }));
            });
            expect(tooltip()).toBeNull();
        });
    });
});
