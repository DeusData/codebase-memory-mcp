// @vitest-environment jsdom
/*
 * Das Overlay in jsdom: die Testmarken, die der Beweislauf im Browser sucht,
 * die Gruppierung der Liste, die ehrlichen Absenz-Saetze und die zwei
 * Ehrlichkeits-Absaetze.
 *
 * Was hier NICHT geprueft wird, ist das Aussehen: in jsdom gibt es kein
 * Stylesheet, also misst der Beweislauf im Browser, dass der Grund dunkel ist
 * und dass nichts vor dem Overlay liegt (tools/smoke-w5c.mjs). Geprueft wird
 * hier, was ein Browserlauf nur umstaendlich fragen koennte: dass die Liste
 * genau die Zeilen des Blocks in genau seiner Reihenfolge zeigt, dass ein
 * Symbol ohne Fakten seinen Satz traegt und dass der Stepper an denselben
 * Rueckruf geht wie ein Klick auf eine Zeile.
 */

import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

vi.mock('../chat/chat-client', () => ({ askModel: vi.fn() }));

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { SemanticIR } from '../core/semantic-ir';
import FlowOverlay from './FlowOverlay';
import type { FlowOverlayProps } from './FlowOverlay';
import { askModel } from '../chat/chat-client';
import { buildFlowView } from './flow-view';
import type { FlowView } from './flow-view';
import type { ClosureDocument } from './pseudocode-builder';

const HERE = dirname(fileURLToPath(import.meta.url));

function flowFixture(): FlowView {
    const closure = JSON.parse(readFileSync(
        join(HERE, '__fixtures__', 'closure-userService-create.json'),
        'utf8',
    )) as ClosureDocument;
    const ir = (name: string): SemanticIR => JSON.parse(readFileSync(
        join(HERE, '..', 'twin', '__fixtures__', `ir-${name}.json`),
        'utf8',
    )) as SemanticIR;
    return buildFlowView({
        closure,
        irs: [ir('userService-create'), ir('createUser'), ir('listUsers'), ir('validateUser'), ir('insert')],
    });
}

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

function props(overrides: Partial<FlowOverlayProps> = {}): FlowOverlayProps {
    return {
        symbolName: 'create',
        flow: flowFixture(),
        step: -1,
        onStep: () => undefined,
        ...overrides,
    };
}

async function render(overrides: Partial<FlowOverlayProps> = {}): Promise<void> {
    await act(async () => {
        root.render(<FlowOverlay {...props(overrides)} />);
    });
}

const q = (selector: string) => container.querySelector(selector);
const all = (selector: string) => [...container.querySelectorAll(selector)];
const text = () => container.textContent ?? '';

describe('das Overlay und seine Kopfzeile', () => {
    it('nennt das Symbol, ueber das es spricht', async () => {
        await render();
        expect(q('[data-testid="atlas-flow-overlay-title"]')?.textContent).toBe('Flow from create');
    });

    /*
     * Seit W8 ist der Erklaerer ein Reiter und kein Fenster mehr. Zwei Dinge,
     * die er als Fenster hatte, hat er darum NICHT mehr, und das steht hier als
     * Pruefung und nicht nur als Kommentar: ein Knopf, der die Flaeche zumacht,
     * und ein Griff auf Escape. Beides gehoert der Zone, und ein zweiter Knopf
     * daneben waere genau die Verdopplung, die dieser Zyklus abgeschafft hat.
     */
    it('traegt weder einen eigenen Schliess-Knopf noch die Rolle eines Fensters', async () => {
        await render();
        expect(q('[data-testid="atlas-flow-close"]')).toBeNull();
        expect(q('[data-testid="atlas-flow-esc"]')).toBeNull();
        expect(q('[data-testid="atlas-flow-overlay"]')?.getAttribute('role')).toBe('region');
    });

    it('sagt ohne Walk, warum da nichts steht, statt leer zu bleiben', async () => {
        await render({ flow: undefined, message: 'Reading what this symbol reaches.' });
        expect(q('[data-testid="atlas-flow-message"]')?.textContent)
            .toContain('Reading what this symbol');
        expect(q('[data-testid="atlas-flow"]')).toBeNull();
    });
});

describe('die optionale AI-Fassung', () => {
    it('bleibt nach einem Parent-Rerender derselben gebauten Zeilen aktiv', async () => {
        const flow = flowFixture();
        vi.mocked(askModel).mockResolvedValue({
            content: flow.document.lines.map((line) => line.text).filter(Boolean).join('\n'),
            durationMs: 1,
            reasoning: '',
            thoughtOnly: false,
            truncated: false,
            finishReason: 'stop',
        });
        await render({ flow, aiAvailable: true, modelName: 'w15-local-stub' });
        await act(async () => {
            (q('[data-testid="atlas-flow-ai-btn"]') as HTMLButtonElement).click();
            await Promise.resolve();
        });
        expect(q('[data-testid="atlas-flow-ai-restore"]')).not.toBeNull();
        await render({ flow: { ...flow }, aiAvailable: true, modelName: 'w15-local-stub' });
        expect(q('[data-testid="atlas-flow-ai-restore"]')).not.toBeNull();
    });
});

describe('das Sequenzdiagramm im Overlay', () => {
    const flow = flowFixture();

    it('zeichnet ein SVG mit einer Lebenslinie je Datei', async () => {
        await render({ flow });
        expect(q('[data-testid="atlas-flow-diagram"]')?.tagName.toLowerCase()).toBe('svg');
        expect(all('[data-testid="atlas-flow-lifeline"]'))
            .toHaveLength(flow.sequence.participants.length);
        expect(all('[data-testid="atlas-flow-participant"]').length).toBeGreaterThanOrEqual(3);
        expect(all('[data-testid="atlas-flow-participant"]').map((node) => node.getAttribute('data-label')))
            .toEqual(flow.sequence.participants);
    });

    it('zeichnet einen beschrifteten Pfeil je Aufruf', async () => {
        await render({ flow });
        const arrows = all('[data-testid="atlas-flow-arrow"]');
        expect(arrows).toHaveLength(flow.sequence.interactions.length);
        expect(arrows.length).toBeGreaterThanOrEqual(4);
        expect(arrows.map((node) => node.getAttribute('data-message')))
            .toEqual(flow.sequence.interactions.map((interaction) => interaction.message));
    });

    it('zeichnet die Fehlerpfade als Selbstschleife und nicht als Pfeil', async () => {
        await render({ flow });
        const loops = all('[data-testid="atlas-flow-raise"]');
        expect(loops.length).toBeGreaterThan(0);
        expect(loops[0].textContent).toContain('may raise');
    });

    it('faerbt den Pfeil des aktiven Schritts und nur den', async () => {
        const step = flow.arrows.findIndex((arrow) => arrow >= 0);
        await render({ flow, step });
        const current = all('[data-testid="atlas-flow-arrow"][data-current="true"]');
        expect(current).toHaveLength(1);
        expect(current[0].getAttribute('data-index')).toBe(String(flow.arrows[step]));
    });

    it('leuchtet bei einem Fehlerpfad die Schleife an und keinen Pfeil', async () => {
        const step = flow.steps.findIndex((entry) => entry.line.kind === 'raise');
        await render({ flow, step });
        expect(all('[data-testid="atlas-flow-arrow"][data-current="true"]')).toHaveLength(0);
        expect(all('[data-testid="atlas-flow-raise"][data-current="true"]')).toHaveLength(1);
        // Das Bild hat einen Treffer, also raeumt nichts eine Fehlanzeige ein.
        expect(q('[data-testid="atlas-flow-no-hit"]')).toBeNull();
    });
});

describe('die Schritt-Liste', () => {
    const flow = flowFixture();

    it('gruppiert nach Symbolen, in der Reihenfolge des Blocks', async () => {
        await render({ flow });
        const groups = all('[data-testid="atlas-flow-group"]');
        expect(groups.length).toBeGreaterThanOrEqual(3);
        expect(groups.map((node) => node.textContent))
            .toEqual(flow.document.lines.filter((line) => line.kind === 'group').map((line) => line.text));
    });

    it('traegt fuer ein Symbol ohne Fakten den ehrlichen Satz statt einer leeren Gruppe', async () => {
        await render({ flow });
        const absences = all('[data-testid="atlas-flow-absence"]');
        expect(absences.length).toBeGreaterThanOrEqual(1);
        expect(absences[0].textContent)
            .toMatch(/^the index recorded no calls, raised errors or environment reads for /);
    });

    it('nummeriert genau die Halte des Steppers und markiert den aktiven', async () => {
        await render({ flow, step: 2 });
        expect(all('[data-testid="atlas-flow-step"]')).toHaveLength(flow.steps.length);
        const active = all('[data-testid="atlas-flow-step"][data-active="true"]');
        expect(active).toHaveLength(1);
        expect(active[0].getAttribute('data-step')).toBe('2');
    });

    it('schickt einen Klick auf eine Zeile an denselben Rueckruf wie der Stepper', async () => {
        const onStep = vi.fn();
        await render({ flow, onStep });
        await act(async () => {
            (all('[data-testid="atlas-flow-step-button"]')[3] as HTMLButtonElement).click();
        });
        expect(onStep).toHaveBeenCalledWith(3);
    });
});

describe('der Stepper und die Ehrlichkeit', () => {
    const flow = flowFixture();

    it('sagt vor dem ersten Schritt, wie viele es sind, statt "0 of n"', async () => {
        await render({ flow, step: -1 });
        expect(q('[data-testid="atlas-flow-position"]')?.textContent).not.toContain('0 of');
        expect((q('[data-testid="atlas-flow-prev"]') as HTMLButtonElement).disabled).toBe(true);
    });

    it('schreibt die Position als "n of m", wie der Referenz-Explainer', async () => {
        await render({ flow, step: 1 });
        expect(q('[data-testid="atlas-flow-position"]')?.textContent)
            .toBe(`2 of ${flow.steps.length}`);
        expect(q('[data-testid="atlas-flow-prev"]')?.textContent).toBe('Previous');
        expect(q('[data-testid="atlas-flow-next"]')?.textContent).toBe('Next');
    });

    it('bewegt sich vorwaerts und rueckwaerts ueber denselben Rueckruf', async () => {
        const onStep = vi.fn();
        await render({ flow, step: 2, onStep });
        await act(async () => {
            (q('[data-testid="atlas-flow-next"]') as HTMLButtonElement).click();
        });
        await act(async () => {
            (q('[data-testid="atlas-flow-prev"]') as HTMLButtonElement).click();
        });
        expect(onStep.mock.calls.map((call) => call[0])).toEqual([3, 1]);
    });

    /*
     * Der Ehrlichkeitsblock nach W8b.
     *
     * Bis dahin standen hier vier Absaetze mit zusammen 954 Zeichen, drei davon
     * mit derselben Aussage in drei Anlaeufen. Der Nutzer am 2026-08-29: "sind
     * diese Texte wirklich hilfreich oder bullshit?" Gekuerzt wurde die
     * WIEDERHOLUNG und nicht die Aussage: die drei Herkunftssaetze stehen
     * vollstaendig hinter dem Fragezeichen daneben, in genau dem Idiom, das
     * dieses Produkt fuer Herkunft schon hat. Der Test prueft darum beides: dass
     * unten EIN Satz steht, und dass keiner der drei verschwunden ist.
     */
    it('traegt zwei kurze Ehrlichkeits-Absaetze, und die Herkunft hinter dem Fragezeichen', async () => {
        await render({ flow });
        const honesty = all('[data-testid="atlas-flow-honesty"]');
        expect(honesty).toHaveLength(2);
        expect(honesty[0].textContent).toContain('a call the index did not resolve');
        expect(honesty[1].textContent).toContain('one hop further than this box draws');
        // Aus 954 Zeichen sind es diese beiden geworden. Der Contract von W8b
        // nennt 400 als Grenze; gemessen wird sie am gerenderten Baum, hier
        // steht sie als Zusicherung ueber den Wortlaut selbst.
        expect(honesty.reduce((sum, node) => sum + (node.textContent ?? '').length, 0))
            .toBeLessThanOrEqual(400);
        const provenance = q('[data-testid="atlas-flow-provenance"]');
        expect(provenance).not.toBeNull();
        const said = provenance?.getAttribute('data-hint') ?? '';
        expect(said).toContain('two readings of one walk');
        expect(said).toContain('Derived from the index and nothing else');
        expect(said).toContain('Nobody drew this picture');
    });

    it('fuehrt den aktiven Schritt an den Ort, aus dem er gelesen wurde', async () => {
        const onOpenLine = vi.fn();
        await render({ flow, step: 0, onOpenLine });
        await act(async () => {
            (q('.atlas-flow-current-line') as HTMLButtonElement).click();
        });
        expect(onOpenLine).toHaveBeenCalledTimes(1);
        expect(onOpenLine.mock.calls[0][0]).toHaveProperty('line');
    });

    /*
     * Die Grenze wird seit W8b GEZEICHNET statt beschrieben.
     *
     * Sie stand als vierter von vier Absaetzen unter dem Bild, also an der
     * Stelle, an der sie nach drei Absaetzen Ehrlichkeit niemand mehr liest.
     * Jetzt steht sie am unteren Rand des Kastens, den sie begrenzt, und der
     * lange Satz dazu steht hinter dem Fragezeichen.
     */
    it('sagt die Grenze des Walks am Rand des Bildes, statt an ihr einfach aufzuhoeren', async () => {
        await render({ flow });
        expect(text()).not.toContain('the walk went');
        const bound = q('[data-testid="atlas-flow-walk-bound"]');
        expect(bound).not.toBeNull();
        expect(bound?.textContent).toContain('walk:');
        expect(bound?.textContent).toContain('hop');
        expect(bound?.textContent).toContain('at most');
        expect(q('[data-testid="atlas-flow-provenance"]')?.getAttribute('data-hint') ?? '')
            .toContain('the walk went');
    });
});
