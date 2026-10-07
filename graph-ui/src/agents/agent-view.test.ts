/*
 * Die eine Rechnung, aus der die Ebene zeichnet und das Instrument schreibt.
 *
 * Der Punkt dieses Moduls ist, dass es nur EINE gibt: waeren es zwei, koennte
 * ein Koerper an einem Knoten stehen und die Zeile daneben einen anderen
 * nennen, und niemandem fiele es auf. Geprueft wird darum vor allem, dass die
 * Zeile und der Ort aus demselben Ergebnis kommen.
 */

import { describe, expect, it } from 'vitest';

import type { GraphNode } from '../galaxy/types';
import { readAgentEvent } from './agent-event';
import type { AgentEvent } from './agent-event';
import { emptyAgentsState, withEvent, withYouEvent } from './agent-store';
import { UNMAPPED_LIMIT, buildAgentsView } from './agent-view';

const NOW = 1788040000000;

const node = (over: Partial<GraphNode> & { id: number }): GraphNode => ({
    x: 0, y: 0, z: 0, label: 'Function', name: 'x', size: 1, color: '#ffffff', ...over,
});

const NODES: GraphNode[] = [
    node({
        id: 50, label: 'Module', name: 'src/services/userService.ts',
        file_path: 'src/services/userService.ts', start_line: 1, end_line: 43,
    }),
    node({
        id: 51, name: 'createUser', file_path: 'src/services/userService.ts',
        start_line: 23, end_line: 36, qualified_name: 'p.createUser',
    }),
    node({ id: 5, label: 'Folder', name: 'src', file_path: 'src' }),
    node({
        id: 69, name: 'validateUser', file_path: 'src/util/validate.ts',
        start_line: 19, end_line: 31,
    }),
];

const event = (over: Partial<AgentEvent>): AgentEvent => readAgentEvent({
    ts: NOW - 1000, agent: 'implementer', run: 'r1', seq: 1, phase: 'end',
    tool: 'Edit', path: 'src/services/userService.ts', lines: [24, 30], detail: 'x', ...over,
}) as AgentEvent;

const build = (events: readonly AgentEvent[], over = {}) => buildAgentsView({
    state: events.reduce((state, entry) => withEvent(state, entry), emptyAgentsState()),
    nodes: NODES,
    now: NOW,
    filter: 'both',
    trailWindowMs: 0,
    ...over,
});

describe('die Sicht', () => {
    it('gibt jedem Akteur Farbe, Buchstabe, Art und Ort aus einer Rechnung', () => {
        const view = build([event({})]);
        expect(view.actors).toHaveLength(1);
        const actor = view.actors[0]!;
        expect(actor.kind).toBe('write');
        expect(actor.kindLetter).toBe('W');
        expect(actor.letter).toBe('I');
        expect(actor.placement.name).toBe('createUser');
        expect(actor.node?.id).toBe(51);
        expect(actor.color).toMatch(/^hsl\(/);
    });

    it('haelt die Ghost-Pings am selben Ergebnis wie den Koerper', () => {
        const view = build([event({ tool: 'Grep', path: 'src', lines: undefined, detail: 'validate' })]);
        const actor = view.actors[0]!;
        expect(actor.kind).toBe('search');
        expect(actor.placement.uncertain).toBe(true);
        expect(actor.ghostNodes.map((ghost) => ghost.id)).toContain(69);
    });

    it('laesst ein unverortbares Ereignis stehen, statt es fallen zu lassen', () => {
        const view = build([
            event({ seq: 1, path: 'package.json', lines: undefined }),
            event({ seq: 2, ts: NOW - 500 }),
        ]);
        expect(view.actors).toHaveLength(1);
        expect(view.actors[0]?.placement.name).toBe('createUser');
        expect(view.unmapped.map((entry) => entry.path)).toContain('package.json');
    });

    it('fragt die Art je Ereignis und nicht am Akteur', () => {
        // Ein Testlauf von eben macht ein Lesen von vorhin nicht zu einem.
        const view = build([
            event({ seq: 1, tool: 'Read', path: 'package.json', lines: undefined }),
            event({ seq: 2, ts: NOW - 500, tool: 'Bash', path: '', lines: undefined, detail: 'npx vitest run' }),
        ]);
        expect(view.actors[0]?.kind).toBe('test');
        const reasons = view.unmapped.map((entry) => entry.why);
        expect(reasons).toContain('the index has no node for this path');
        expect(reasons).toContain('the command names no file this index knows');
    });

    it('zeigt hoechstens so viele unverortbare Zeilen, wie die Grenze erlaubt', () => {
        const many = Array.from({ length: UNMAPPED_LIMIT + 4 }, (_, index) => event({
            seq: index + 1, ts: NOW - 1000 - index, path: `nowhere-${index}.json`, lines: undefined,
        }));
        expect(build(many).unmapped).toHaveLength(UNMAPPED_LIMIT);
    });

    it('filtert wirklich, und die Zahl im Kopf zaehlt trotzdem alle', () => {
        const state = withYouEvent(
            withEvent(emptyAgentsState(), event({})),
            event({ agent: 'you', run: 'this-window', tool: 'Open', ts: NOW - 200 }),
        );
        const base = { state, nodes: NODES, now: NOW, trailWindowMs: 0 } as const;
        expect(buildAgentsView({ ...base, filter: 'both' }).actors).toHaveLength(2);
        expect(buildAgentsView({ ...base, filter: 'you' }).actors.map((a) => a.id)).toEqual(['you']);
        expect(buildAgentsView({ ...base, filter: 'agent' }).actors.map((a) => a.id))
            .toEqual(['implementer']);
        expect(buildAgentsView({ ...base, filter: 'you' }).all).toHaveLength(2);
    });

    it('traegt die Absicht des LETZTEN Ereignisses und keine von vorhin', () => {
        const view = build([
            event({ seq: 1, intent: 'an old plan' }),
            event({ seq: 2, ts: NOW - 500 }),
        ]);
        expect(view.actors[0]?.intent).toBe('');
    });
});
