/*
 * Die Rechnung hinter der Bewegung, ohne einen gerenderten Pixel.
 *
 * Das BILD der Ebene prueft der Beweislauf im Browser (acht Einzelbilder ueber
 * eine Umkreisung, tools/smoke-w11a.mjs). Hier steht, was die Bewegung
 * ausrechnet, und das sind drei Zusicherungen:
 *
 *  1. Die Art bestimmt Weite und Takt, und die vier Arten sind voneinander
 *     unterscheidbar.
 *  2. Derselbe Akteur startet immer an derselben Stelle seiner Bahn.
 *  3. Zwei Akteure am SELBEN Knoten bekommen getrennte Bahnen, sonst waeren
 *     zwei Wesen ein Fleck.
 */

import { describe, expect, it } from 'vitest';

import { ORBITS, ORBIT_SEPARATION, angleAt, orbitRadii, phaseOf } from './AgentLayer';
import { pulseOf } from '../agents/agent-motion';
import type { ActorView } from '../agents/agent-view';
import type { GraphNode } from './types';
import type { WorkKind } from '../agents/agent-event';

const node = (id: number): GraphNode => ({
    id, x: 0, y: 0, z: 0, label: 'Function', name: `n${id}`, size: 1, color: '#ffffff',
});

const actor = (id: string, kind: WorkKind, nodeId: number): ActorView => ({
    id,
    name: id,
    you: false,
    color: '#ffffff',
    letter: id.slice(0, 1).toUpperCase(),
    kind,
    kindLetter: 'X',
    placement: {
        kind: 'file', nodeId, name: 'n', qualifiedName: 'n', uncertain: false, why: '', ghostIds: [],
    },
    node: node(nodeId),
    testedNode: undefined,
    ghostNodes: [],
    last: {
        ts: 0, agent: id, run: 'r', seq: 1, phase: 'end', tool: 'Read', path: '',
        detail: '', source: '', replay: false,
    },
    lastTs: 0,
    hereMs: 0,
    count: 1,
    missed: 0,
    strip: [],
    stripTotal: 0,
    intent: '',
    paths: [],
    sinceMs: 0,
    idle: false,
    recentEvents: 1,
    pulse: pulseOf(1),
    trail: [],
    waves: [],
    drawn: true,
});

describe('die vier Bahnen', () => {
    it('macht das Lesen weit und ruhig und das Schreiben eng und schnell', () => {
        expect(ORBITS.write.radius).toBeLessThan(ORBITS.read.radius / 2);
        expect(ORBITS.write.periodMs).toBeLessThan(ORBITS.read.periodMs / 2);
    });

    it('gibt jeder Art einen Takt, den man von den anderen unterscheiden kann', () => {
        const periods = Object.values(ORBITS).map((orbit) => orbit.periodMs);
        expect(new Set(periods).size).toBe(periods.length);
    });
});

describe('der Winkel', () => {
    it('startet fuer dieselbe Kennung immer an derselben Stelle', () => {
        expect(phaseOf('implementer')).toBe(phaseOf('implementer'));
        expect(angleAt('implementer', 'read', 0)).toBe(phaseOf('implementer'));
    });

    it('laeuft in einer Umlaufzeit genau einmal herum', () => {
        const period = ORBITS.read.periodMs;
        expect(angleAt('a', 'read', period)).toBeCloseTo(angleAt('a', 'read', 0), 6);
        expect(angleAt('a', 'read', period / 2))
            .toBeCloseTo((phaseOf('a') + 180) % 360, 6);
    });

    it('steht nie still: acht Zeitpunkte einer Umkreisung sind acht Winkel', () => {
        const period = ORBITS.search.periodMs;
        const series = [0, 1, 2, 3, 4, 5, 6, 7]
            .map((step) => Number(angleAt('explorer', 'search', (period / 8) * step).toFixed(3)));
        expect(new Set(series).size).toBe(8);
    });
});

describe('zwei Akteure am selben Knoten', () => {
    it('bekommen getrennte Bahnen, damit sie nicht zu einem Fleck werden', () => {
        const radii = orbitRadii([
            actor('you', 'read', 51),
            actor('implementer', 'write', 51),
        ]);
        const values = [...radii.values()].sort((a, b) => a - b);
        expect(values).toHaveLength(2);
        expect((values[1] ?? 0) - (values[0] ?? 0)).toBeGreaterThanOrEqual(ORBIT_SEPARATION);
    });

    it('behalten an verschiedenen Knoten die Bahn ihrer Art', () => {
        const radii = orbitRadii([
            actor('you', 'read', 51),
            actor('implementer', 'write', 60),
        ]);
        expect(radii.get('you')).toBe(ORBITS.read.radius);
        expect(radii.get('implementer')).toBe(ORBITS.write.radius);
    });

    it('rechnet unabhaengig von der Reihenfolge, in der die Akteure ankommen', () => {
        const forward = orbitRadii([
            actor('a', 'read', 7), actor('b', 'write', 7), actor('c', 'test', 7),
        ]);
        const backward = orbitRadii([
            actor('c', 'test', 7), actor('b', 'write', 7), actor('a', 'read', 7),
        ]);
        expect([...forward.entries()].sort()).toEqual([...backward.entries()].sort());
    });

    it('zeichnet nichts fuer einen Akteur ohne Knoten', () => {
        const homeless = { ...actor('x', 'read', 1), node: undefined };
        expect(orbitRadii([homeless]).size).toBe(0);
    });
});
