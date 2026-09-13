// @vitest-environment jsdom
/*
 * Die Hilfeseite in jsdom: dass sie die sieben Abschnitte in der verlangten
 * Reihenfolge traegt, dass die Grenzen VOR den Faehigkeiten stehen und woertlich
 * dastehen, dass die Tastentabelle die Verdrahtung zeigt, und dass sie offline
 * vollstaendig ist (kein einziger Netz-Link).
 *
 * Was hier NICHT geprueft wird, prueft der Beweislauf im Browser: dass ein Klick
 * auf [?]help sie aufschlaegt, dass die Taste ? dasselbe tut, dass Escape sie
 * schliesst und dass nichts darin sich ueberlagert. Das sind Fragen an ein
 * Layout, und jsdom rechnet keins.
 */

import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ATLAS_SHORTCUTS } from '../app/shortcuts';
import { messages } from '../i18n/messages';
import HelpOverlay from './HelpOverlay';

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

async function render(onClose = vi.fn()): Promise<ReturnType<typeof vi.fn>> {
    await act(async () => {
        root.render(<HelpOverlay onClose={onClose} />);
    });
    return onClose;
}

const sections = (): string[] =>
    [...container.querySelectorAll('[data-testid="atlas-help-section"]')]
        .map((node) => node.getAttribute('data-section') ?? '');

const visibleText = (): string =>
    (container.querySelector('[data-testid="atlas-help"]') as HTMLElement | null)?.textContent ?? '';

describe('HelpOverlay', () => {

    it('traegt die sieben Abschnitte in der Reihenfolge des Contracts', async () => {
        await render();
        expect(sections()).toEqual([
            'what', 'limits', 'panels', 'shortcuts', 'operations', 'honesty', 'references',
        ]);
    });

    it('stellt die Grenzen vor die Panels, nicht ans Ende', async () => {
        await render();
        const order = sections();
        expect(order.indexOf('limits')).toBeLessThan(order.indexOf('panels'));
        expect(order.indexOf('limits')).toBe(1);
    });

    it('nennt die drei Verbote woertlich, samt Grund', async () => {
        await render();
        const text = visibleText();
        for (const phrase of ['read-only', 'cannot edit', 'cannot run', 'no terminal', 'no cloud']) {
            expect(text, `"${phrase}" fehlt`).toContain(phrase);
        }
        expect(text).toContain('no backend of its own');
    });

    it('zeigt jede verdrahtete Taste, und keine erfundene', async () => {
        await render();
        const rows = [...container.querySelectorAll('[data-testid="atlas-help-shortcut"]')];
        expect(rows).toHaveLength(ATLAS_SHORTCUTS.length);
        expect(rows.map((row) => `${row.getAttribute('data-scope')}:${row.getAttribute('data-key')}`))
            .toEqual(ATLAS_SHORTCUTS.map((entry) => `${entry.scope}:${entry.key}`));
        // Jede Zeile sagt auch, was die Taste tut: eine leere Spalte waere eine
        // Taste, die genannt und nicht erklaert wird.
        for (const row of rows) {
            const cells = [...row.querySelectorAll('td')];
            expect(cells).toHaveLength(3);
            expect(cells[2]?.textContent?.trim().length ?? 0).toBeGreaterThan(0);
        }
    });

    it('verlinkt nirgends ins Netz und nennt die Verweise als Pfade', async () => {
        await render();
        expect(container.querySelectorAll('a')).toHaveLength(0);
        expect(visibleText()).not.toMatch(/https?:\/\//);
        const paths = [...container.querySelectorAll('[data-testid="atlas-help-path"]')]
            .map((node) => node.textContent?.trim() ?? '');
        for (const expected of ['PLAN.md', 'INVENTAR.md', 'UPSTREAM-ASKS.md',
            'docs/adr/0001-modellwahl.md', 'verification/']) {
            expect(paths, `${expected} fehlt in den Verweisen`).toContain(expected);
        }
    });

    it('sagt, wie der Betrieb laeuft, an den Namen, die es wirklich gibt', async () => {
        await render();
        const text = visibleText();
        expect(text).toContain('llm/start.sh class-a');
        expect(text).toContain('ingest_traces');
    });

    it('geht auf Escape und auf den Knopf zu, und ruft dafuer denselben Ausgang', async () => {
        const onClose = await render();
        const page = container.querySelector('[data-testid="atlas-help"]') as HTMLElement;
        await act(async () => {
            page.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
        });
        expect(onClose).toHaveBeenCalledTimes(1);
        await act(async () => {
            (container.querySelector('[data-testid="atlas-help-close"]') as HTMLButtonElement).click();
        });
        expect(onClose).toHaveBeenCalledTimes(2);
    });

    /*
     * Der Tastentest (W7b, Nutzerbefund 2026-08-29 "alt plus letter
     * funktioniert nur fuer atlas"). Er war an der laufenden Vorschau nicht
     * nachzustellen, also raet dieser Zyklus nicht an einer Ursache, sondern
     * baut die Selbstauskunft: der Nutzer drueckt, und die Seite sagt, was
     * ankam.
     */
    describe('der Tastentest', () => {

        const field = (name: string): string =>
            container.querySelector(`[data-field="${name}"] [data-testid="atlas-help-keyprobe-value"]`)
                ?.textContent ?? '';

        it('steht da, bevor irgendetwas gedrueckt wurde, und behauptet nichts', async () => {
            await render();
            const probe = container.querySelector('[data-testid="atlas-help-keyprobe"]');
            expect(probe).not.toBeNull();
            expect(probe?.getAttribute('data-pressed')).toBe('false');
            expect(field('code')).toBe(messages.help.keyProbeNone);
            expect(field('shortcut')).toBe(messages.help.keyProbeNone);
        });

        it('nennt die physische Taste, das Zeichen, die Modifikatoren und das Urteil', async () => {
            await render();
            await act(async () => {
                window.dispatchEvent(new KeyboardEvent('keydown', {
                    key: 'w', code: 'KeyW', altKey: true, bubbles: true,
                }));
            });
            expect(field('code')).toBe('KeyW');
            expect(field('key')).toBe('w');
            expect(field('modifiers')).toContain('alt');
            expect(field('defaultPrevented')).toBe('false');
            expect(field('consumedBy')).toBe(messages.help.keyProbeConsumers['nobody']);
            expect(field('shortcut')).toBe(messages.help.keyProbeShortcut('w'));
        });

        it('sagt bei einer Taste ohne Kuerzel, dass es keines ist', async () => {
            await render();
            await act(async () => {
                // `z` ist an keinen Menuepunkt vergeben, `g` seit W11a schon.
                window.dispatchEvent(new KeyboardEvent('keydown', {
                    key: 'z', code: 'KeyZ', altKey: true, bubbles: true,
                }));
            });
            expect(field('shortcut')).toBe(messages.help.keyProbeNoShortcut);
        });

        it('hoert auf zu hoeren, sobald die Seite zu ist', async () => {
            await render();
            await act(async () => {
                root.render(<div />);
            });
            // Ein Griff, der die Seite ueberlebt, waere ein Leck und ausserdem
            // ein zweiter Leser jeder Taste dieser Oberflaeche.
            await act(async () => {
                window.dispatchEvent(new KeyboardEvent('keydown', { key: 'w', code: 'KeyW' }));
            });
            expect(container.querySelector('[data-testid="atlas-help-keyprobe"]')).toBeNull();
        });
    });

    it('holt sich den Fokus, damit Escape sie auch wirklich trifft', async () => {
        await render();
        expect(document.activeElement).toBe(container.querySelector('[data-testid="atlas-help"]'));
    });

    it('schreibt keinen Satz zweimal: der Text kommt aus dem Katalog', async () => {
        await render();
        const text = visibleText();
        expect(text).toContain(messages.help.title);
        expect(text).toContain(messages.help.limitsWhy);
        expect(text).toContain(messages.help.referencesNote);
    });

    it('erklaert Fragen als gebaute Indexantwort und die Modellfassung als ausdrueckliche Aktion', async () => {
        await render();
        const text = visibleText();
        expect(text).toMatch(/question mark/i);
        expect(text).toMatch(/indexed cards/i);
        expect(text).toContain('AI button');
        expect(text).not.toMatch(/sent to (?:the )?local model|local model as a question/i);
    });
});
