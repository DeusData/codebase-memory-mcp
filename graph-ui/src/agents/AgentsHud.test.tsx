// @vitest-environment jsdom
/*
 * Das Instrument, ohne einen gerenderten Pixel.
 *
 * Was hier geprueft wird, ist die Ehrlichkeitsregel dieser Flaeche und nicht
 * ihr Aussehen: eine Absichtszeile NUR dort, wo ein Ereignis sie mitbringt,
 * und dann gekennzeichnet; keine Zahl, die nicht aus der Sicht kommt; kein
 * Fortschritt; und die drei Lagen mit genau dem, was in jeder von ihnen
 * stehen soll. Wie gross der Kasten wirklich wird, misst der Beweislauf im
 * Browser (tools/smoke-w11a.mjs); jsdom hat kein Layout.
 */

import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import AgentsHud from './AgentsHud';
import type { AgentsHudProps } from './AgentsHud';
import { readAgentEvent } from './agent-event';
import { pulseOf } from './agent-motion';
import type { AgentEvent } from './agent-event';
import type { ActorView, AgentsView } from './agent-view';
import type { AgentSourceStatus } from './agent-source';

let container: HTMLDivElement;
let root: Root;

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
});

afterEach(() => {
    act(() => root.unmount());
    container.remove();
});

const event = (over: Partial<AgentEvent>): AgentEvent => readAgentEvent({
    ts: 1788040000000, agent: 'implementer', run: 'r1', seq: 3, phase: 'end',
    tool: 'Edit', path: 'src/services/userService.ts', lines: [24, 30], detail: 'changed it',
    ...over,
}) as AgentEvent;

const actor = (over: Partial<ActorView> & { id: string }): ActorView => ({
    name: over.id,
    you: false,
    color: 'hsl(200 92% 72%)',
    letter: over.id.slice(0, 1).toUpperCase(),
    kind: 'write',
    kindLetter: 'W',
    placement: {
        kind: 'range', nodeId: 51, name: 'createUser', qualifiedName: 'p.createUser',
        uncertain: false, why: '', ghostIds: [],
    },
    node: undefined,
    testedNode: undefined,
    ghostNodes: [],
    last: event({}),
    lastTs: 1788040000000,
    hereMs: 12000,
    count: 9,
    missed: 0,
    strip: new Array<number>(30).fill(0).map((_, index) => (index === 29 ? 2 : 0)),
    stripTotal: 2,
    intent: '',
    paths: ['src/services/userService.ts'],
    sinceMs: 4000,
    idle: false,
    recentEvents: 9,
    pulse: pulseOf(9),
    trail: [],
    waves: [],
    drawn: true,
    ...over,
});

const view = (over: Partial<AgentsView> = {}): AgentsView => {
    const actors = over.actors ?? [
        actor({ id: 'implementer', intent: 'tightening the input check in createUser' }),
        actor({ id: 'checker', kind: 'test', kindLetter: 'T' }),
    ];
    return {
        actors,
        all: over.all ?? actors,
        unmapped: over.unmapped ?? [{
            ts: 1788039000000, agent: 'checker', tool: 'Read', path: 'package.json',
            detail: 'read it', why: 'the index has no node for this path',
        }],
        events: 45,
        missed: 2,
        perMinute: 7,
        unreadable: 0,
        cap: 8,
        capped: 0,
        ticker: [],
        ...over,
    };
};

const status = (over: Partial<AgentSourceStatus> = {}): AgentSourceStatus => ({
    state: 'connected',
    origin: 'http://127.0.0.1:4142',
    requests: 1,
    drops: 0,
    hello: { mode: 'live', file: '/home/x/.atlas-trace/events.jsonl', events: 45, unreadable: 0 },
    error: '',
    ...over,
});

const props = (over: Partial<AgentsHudProps> = {}): AgentsHudProps => ({
    view: view(),
    status: status(),
    port: 4142,
    size: 'compact',
    onSize: vi.fn(),
    filter: 'both',
    onFilter: vi.fn(),
    switches: { follow: false, trails: false, fullscreen: false },
    onSwitch: vi.fn(),
    trailWindowMs: 60000,
    onTrailWindow: vi.fn(),
    layerOn: true,
    ...over,
});

async function render(over: Partial<AgentsHudProps> = {}): Promise<AgentsHudProps> {
    const use = props(over);
    await act(async () => {
        root.render(<AgentsHud {...use} />);
    });
    return use;
}

const all = (testId: string): HTMLElement[] =>
    [...container.querySelectorAll(`[data-testid="${testId}"]`)] as HTMLElement[];
const one = (testId: string): HTMLElement | null =>
    container.querySelector(`[data-testid="${testId}"]`);
const textOf = (testId: string): string =>
    (one(testId)?.textContent ?? '').replace(/\s+/g, ' ').trim();

describe('die Absichtszeile', () => {
    it('steht NUR dort, wo ein Ereignis sie mitbringt', async () => {
        await render();
        const rows = all('atlas-agents-row');
        expect(rows).toHaveLength(2);
        const withIntent = rows.filter(
            (row) => row.querySelector('[data-testid="atlas-agents-intent"]') !== null,
        );
        expect(withIntent).toHaveLength(1);
        expect(withIntent[0]?.getAttribute('data-actor')).toBe('implementer');
    });

    it('ist als Selbstauskunft gekennzeichnet und nicht als Messung', async () => {
        await render();
        const intent = one('atlas-agents-intent');
        expect(intent?.getAttribute('data-self-reported')).toBe('true');
        expect(intent?.textContent).toContain('agent says:');
        expect(intent?.textContent).toContain('tightening the input check in createUser');
    });

    it('erscheint nicht, wenn das Ereignis das Feld verliert', async () => {
        await render({
            view: view({
                actors: [actor({ id: 'implementer' })],
                all: [actor({ id: 'implementer' })],
            }),
        });
        expect(all('atlas-agents-intent')).toHaveLength(0);
    });
});

describe('was nicht gedeutet wird', () => {
    it('zeichnet kein einziges Fortschrittselement', async () => {
        await render({ size: 'expanded' });
        expect(container.querySelectorAll('progress, [role="progressbar"], [aria-valuenow]'))
            .toHaveLength(0);
    });

    it('nennt keine Prozentzahl und keine Bewertung', async () => {
        await render({ size: 'expanded' });
        const text = (container.textContent ?? '').toLowerCase();
        for (const word of ['%', 'progress', 'thinking', 'almost done', 'score']) {
            expect(text, word).not.toContain(word);
        }
    });
});

describe('die Zahlen im Kopf', () => {
    it('kommen aus der Sicht und werden nicht gerechnet', async () => {
        await render();
        expect(one('atlas-agents-count')?.getAttribute('data-count')).toBe('2');
        expect(one('atlas-agents-rate')?.getAttribute('data-per-minute')).toBe('7');
        expect(one('atlas-agents-order')?.getAttribute('data-missed')).toBe('2');
        expect(textOf('atlas-agents-order')).toBe('2 events missed');
    });

    it('sagen "in Ordnung", wenn nichts fehlte', async () => {
        await render({ view: view({ missed: 0 }) });
        expect(textOf('atlas-agents-order')).toBe('order intact');
    });

    it('malen den Streifen aus den gezaehlten Ereignissen', async () => {
        await render();
        const strip = one('atlas-agents-strip');
        expect(strip?.getAttribute('data-bars')?.split(',')).toHaveLength(30);
        expect(strip?.getAttribute('data-total')).toBe('2');
        expect(strip?.querySelectorAll('.atlas-agents-bar')).toHaveLength(30);
    });
});

describe('die drei Lagen', () => {
    it('zeigt eingeklappt eine Zeile mit der Zahl und keine Reihe', async () => {
        await render({ size: 'collapsed' });
        expect(textOf('atlas-agents-line')).toBe('2 actors on the graph');
        expect(all('atlas-agents-row')).toHaveLength(0);
        expect(all('atlas-agents-switch')).toHaveLength(0);
    });

    it('zeigt kompakt je Akteur eine Zeile und die Zahl der unverortbaren', async () => {
        await render({ size: 'compact' });
        expect(all('atlas-agents-row')).toHaveLength(2);
        expect(all('atlas-agents-card')).toHaveLength(0);
        expect(one('atlas-agents-unmapped')?.getAttribute('data-count')).toBe('1');
        expect(all('atlas-agents-unmapped-row')).toHaveLength(0);
    });

    it('zeigt gross die Karte je Akteur und die Rohereignisse', async () => {
        await render({ size: 'expanded' });
        expect(all('atlas-agents-card')).toHaveLength(2);
        const raw = all('atlas-agents-unmapped-row');
        expect(raw).toHaveLength(1);
        expect(raw[0]?.getAttribute('data-path')).toBe('package.json');
        expect(raw[0]?.textContent).toContain('the index has no node for this path');
    });

    it('meldet jeden Wechsel der Lage nach oben, statt ihn selbst zu behalten', async () => {
        const use = await render({ size: 'compact' });
        act(() => {
            (one('atlas-agents-expand') as HTMLButtonElement).click();
        });
        expect(use.onSize).toHaveBeenCalledWith('expanded');
        act(() => {
            (one('atlas-agents-fold') as HTMLButtonElement).click();
        });
        expect(use.onSize).toHaveBeenCalledWith('collapsed');
    });
});

describe('die Quelle', () => {
    it('sagt bei ausgeschaltetem Modus, dass nichts gefragt wird', async () => {
        await render({ status: status({ state: 'off', hello: undefined, requests: 0 }) });
        expect(textOf('atlas-agents-reading')).toContain('live mode is off');
        expect(one('atlas-agents-source')?.getAttribute('data-state')).toBe('off');
    });

    it('nennt ohne Bruecke den Befehl, der sie startet', async () => {
        await render({
            status: status({ state: 'no-source', hello: undefined, error: 'refused' }),
            port: 4711,
        });
        expect(textOf('atlas-agents-reading')).toContain('no bridge is answering');
        expect(one('atlas-agents-command')?.getAttribute('data-command'))
            .toBe('node tools/agent-bridge.mjs --port 4711');
    });

    it('gibt eine Wiedergabe als Wiedergabe aus und nicht als Gegenwart', async () => {
        const replay = status({
            hello: {
                mode: 'replay', file: 'fixtures/agent-events/w11a-replay.jsonl',
                events: 45, unreadable: 0,
            },
        });
        // Kompakt sagt die eine Kopfzeile es, gross steht der Satz ueber die
        // verschobenen Zeitstempel daneben. Beides zusammen ist die Auskunft;
        // drei Zeilen dafuer in einem Instrument von 320 Pixeln waeren keine.
        await render({ status: replay });
        expect(textOf('atlas-agents-reading')).toContain('replaying');
        expect(all('atlas-agents-replay')).toHaveLength(0);
        await render({ status: replay, size: 'expanded' });
        expect(textOf('atlas-agents-replay')).toContain('recorded earlier');
    });

    it('sagt, wenn die Ebene in den Einstellungen aus ist', async () => {
        await render({ layerOn: false });
        expect(textOf('atlas-agents-layer-off')).toContain('off in the settings');
    });
});

describe('die Umschalter', () => {
    it('melden ihre Wahl nach oben', async () => {
        const use = await render();
        act(() => {
            (container.querySelector('[data-testid="atlas-agents-filter-option"][data-option="you"]') as HTMLButtonElement).click();
        });
        expect(use.onFilter).toHaveBeenCalledWith('you');
        act(() => {
            (container.querySelector('[data-testid="atlas-agents-switch"][data-switch="follow"]') as HTMLButtonElement).click();
        });
        expect(use.onSwitch).toHaveBeenCalledWith('follow');
    });

    it('zeigen den Zeitraum der Wegzeile erst, wenn es eine gibt', async () => {
        await render();
        expect(all('atlas-agents-window-option')).toHaveLength(0);
        expect(all('atlas-agents-path')).toHaveLength(0);
        await render({ switches: { follow: false, trails: true, fullscreen: false } });
        expect(all('atlas-agents-window-option').length).toBeGreaterThanOrEqual(3);
        expect(all('atlas-agents-path')).toHaveLength(2);
    });

    it('traegt keinen Knopf ohne Griff', async () => {
        await render({ size: 'expanded' });
        for (const button of container.querySelectorAll('button')) {
            expect(button.getAttribute('type'), button.textContent ?? '').toBe('button');
        }
        expect(all('atlas-agents-switch').map((node) => node.getAttribute('data-switch')))
            .toEqual(['follow', 'trails', 'fullscreen']);
    });
});

describe('der Ort in Worten', () => {
    it('nennt die Zeilen, wo es welche gibt', async () => {
        await render();
        expect(textOf('atlas-agents-place')).toBe('createUser, lines 24 to 30');
    });

    it('sagt bei einem Knoten ohne Endzeile, dass er keinen Bereich traegt', async () => {
        await render({
            view: view({
                actors: [actor({
                    id: 'explorer',
                    kind: 'search',
                    kindLetter: 'S',
                    placement: {
                        kind: 'file', nodeId: 5, name: 'src', qualifiedName: 'p.src',
                        uncertain: true, why: '', ghostIds: [],
                    },
                })],
                all: [],
            }),
        });
        expect(textOf('atlas-agents-place')).toBe('src (no line range in the index)');
        expect(one('atlas-agents-row')?.getAttribute('data-uncertain')).toBe('true');
    });

    it('sagt bei einem Ereignis ohne Ort, dass es sich nicht verorten laesst', async () => {
        await render({
            view: view({
                actors: [actor({
                    id: 'checker',
                    placement: {
                        kind: 'none', name: '', qualifiedName: '', uncertain: false,
                        why: 'the index has no node for this path', ghostIds: [],
                    },
                })],
                all: [],
            }),
        });
        expect(textOf('atlas-agents-place')).toBe('not placeable on the graph');
    });
});
