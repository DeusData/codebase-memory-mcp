/*
 * Die Rechnung hinter der Bewegung, ohne einen gerenderten Pixel.
 *
 * Das BILD prueft der Beweislauf im Browser (tools/smoke-w11b.mjs): dort werden
 * die Punkte gelesen, die der Koerper wirklich gezeichnet hat. Hier steht, was
 * die Formeln dahinter zusichern, und das sind fuenf Dinge:
 *
 *  1. Die Bahn faengt am Anfang an, endet am Ende und liegt DAZWISCHEN abseits
 *     der geraden Verbindung. Genau das ist die Kruemmung aus AC1.
 *  2. Derselbe Akteur weicht immer auf dieselbe Seite aus.
 *  3. Der Puls kommt aus der Zahl der Ereignisse, und ohne Ereignisse gibt es
 *     keinen. Das ist AC3b und zugleich AC6.
 *  4. Ein Bruch ist eine Folge an DEMSELBEN Knoten in kurzer Folge, und er
 *     ergibt EINE Welle, nicht eine je Ereignis.
 *  5. Die Feder ist kritisch gedaempft: sie kommt an und schwingt nicht ueber.
 */

import { describe, expect, it } from 'vitest';

import {
    BURST_GAP_MS,
    DRAWN_BODIES_CAP,
    IDLE_MS,
    NO_PULSE,
    PULSE_BUSY_EVENTS,
    PULSE_FAST_MS,
    PULSE_SLOW_MS,
    TRAIL_NODE_LIMIT,
    TRANSITION_BEND,
    TRANSITION_MS,
    bendSignOf,
    controlPointOf,
    currentBursts,
    distanceFromChord,
    easeInOutCubic,
    pulseOf,
    pulseScaleAt,
    springStep,
    transitionPointAt,
    writeBurstsOf,
} from './agent-motion';
import type { BurstInput } from './agent-motion';

const from = { x: 0, y: 0, z: 0 };
const to = { x: 100, y: 0, z: 0 };

describe('die Bahn', () => {
    it('faengt am Ausgangspunkt an und endet am Ziel', () => {
        const control = controlPointOf(from, to, 1);
        expect(transitionPointAt(from, to, control, 0)).toEqual(from);
        expect(transitionPointAt(from, to, control, 1)).toEqual(to);
    });

    it('liegt in der Mitte messbar ABSEITS der geraden Verbindung', () => {
        const control = controlPointOf(from, to, 1);
        const middle = transitionPointAt(from, to, control, 0.5);
        const offset = distanceFromChord(middle, from, to);
        // Die groesste Abweichung einer quadratischen Bezierkurve von ihrer
        // Sehne ist die Haelfte des Steuerpunkt-Versatzes.
        expect(offset).toBeCloseTo((TRANSITION_BEND * 100) / 2, 5);
        expect(offset / 100).toBeGreaterThan(0.05);
    });

    it('haelt die Kruemmung als ANTEIL der Strecke, nicht als feste Zahl', () => {
        const far = { x: 1000, y: 0, z: 0 };
        const near = { x: 10, y: 0, z: 0 };
        const farOffset = distanceFromChord(
            transitionPointAt(from, far, controlPointOf(from, far, 1), 0.5), from, far,
        );
        const nearOffset = distanceFromChord(
            transitionPointAt(from, near, controlPointOf(from, near, 1), 0.5), from, near,
        );
        expect(farOffset / 1000).toBeCloseTo(nearOffset / 10, 6);
    });

    it('weicht fuer dieselbe Kennung immer auf dieselbe Seite aus', () => {
        expect(bendSignOf(4)).toBe(bendSignOf(4));
        expect(bendSignOf(4)).toBe(1);
        expect(bendSignOf(5)).toBe(-1);
    });

    it('faengt weich an und hoert weich auf', () => {
        expect(easeInOutCubic(0)).toBe(0);
        expect(easeInOutCubic(1)).toBe(1);
        expect(easeInOutCubic(0.5)).toBeCloseTo(0.5, 6);
        expect(easeInOutCubic(-1)).toBe(0);
        expect(easeInOutCubic(2)).toBe(1);
    });

    it('dauert die halbe Sekunde aus dem Contract', () => {
        expect(TRANSITION_MS).toBeGreaterThanOrEqual(250);
        expect(TRANSITION_MS).toBeLessThanOrEqual(900);
    });
});

describe('der Puls', () => {
    it('gibt es ohne Ereignisse gar nicht', () => {
        expect(pulseOf(0)).toEqual(NO_PULSE);
        expect(pulseOf(-3)).toEqual(NO_PULSE);
        expect(pulseScaleAt(NO_PULSE, 1234)).toBe(1);
        expect(pulseScaleAt(NO_PULSE, 99999)).toBe(1);
    });

    it('schlaegt bei viel Arbeit schnell und kraeftig, bei wenig langsam und schwach', () => {
        const busy = pulseOf(PULSE_BUSY_EVENTS);
        const quiet = pulseOf(1);
        expect(busy.periodMs).toBe(PULSE_FAST_MS);
        expect(quiet.periodMs).toBe(PULSE_SLOW_MS);
        expect(busy.periodMs).toBeLessThan(quiet.periodMs);
        expect(busy.amplitude).toBeGreaterThan(quiet.amplitude);
    });

    it('laeuft zwischen den beiden Enden monoton', () => {
        const periods = [1, 3, 6, 9, 12].map((count) => pulseOf(count).periodMs);
        expect(periods).toEqual([...periods].sort((a, b) => b - a));
    });

    it('bleibt ueber der Ruhegroesse und geht nie darunter', () => {
        const pulse = pulseOf(8);
        const sizes = Array.from({ length: 24 }, (_, i) => pulseScaleAt(pulse, i * 50));
        expect(Math.min(...sizes)).toBeGreaterThanOrEqual(1);
        expect(Math.max(...sizes)).toBeGreaterThan(1.1);
        expect(pulseScaleAt(pulse, 0)).toBeCloseTo(1, 6);
    });

    it('haelt die Ruhegrenze bei einer Minute', () => {
        expect(IDLE_MS).toBe(60000);
    });
});

describe('der Schreib-Bruch', () => {
    const write = (ts: number, nodeId: number): BurstInput =>
        ({ ts, nodeId, write: true, key: `k${ts}` });

    it('macht aus fuenf Aenderungen in zwei Sekunden EINEN Bruch', () => {
        const bursts = writeBurstsOf([
            write(0, 7), write(400, 7), write(900, 7), write(1400, 7), write(1900, 7),
        ]);
        expect(bursts).toHaveLength(1);
        expect(bursts[0]?.events).toBe(5);
        expect(bursts[0]?.from).toBe(0);
        expect(bursts[0]?.to).toBe(1900);
    });

    it('trennt bei einer langen Pause', () => {
        const bursts = writeBurstsOf([
            write(0, 7), write(400, 7), write(400 + BURST_GAP_MS + 1, 7),
        ]);
        expect(bursts).toHaveLength(2);
        expect(bursts.map((burst) => burst.events)).toEqual([2, 1]);
    });

    it('trennt, wenn dazwischen an einer anderen Stelle geschrieben wird', () => {
        const bursts = writeBurstsOf([write(0, 7), write(100, 9), write(200, 7)]);
        expect(bursts.map((burst) => burst.nodeId)).toEqual([7, 9, 7]);
    });

    it('zaehlt nur Schreibereignisse mit einem Ort', () => {
        const bursts = writeBurstsOf([
            { ts: 0, nodeId: 7, write: false, key: 'a' },
            { ts: 10, nodeId: -1, write: true, key: 'b' },
        ]);
        expect(bursts).toHaveLength(0);
    });

    it('traegt die Welle nur, solange der Bruch die laufende Arbeit ist', () => {
        const bursts = writeBurstsOf([write(0, 7), write(400, 7)]);
        expect(currentBursts(bursts, 400)).toHaveLength(1);
        // Der Akteur hat danach etwas anderes getan: die Welle ist weg.
        expect(currentBursts(bursts, 900)).toHaveLength(0);
        expect(currentBursts([], 400)).toHaveLength(0);
    });
});

describe('die Feder der Kamera', () => {
    it('kommt an und schwingt nicht ueber', () => {
        let state = { value: 0, velocity: 0 };
        let overshoot = 0;
        for (let i = 0; i < 300; i += 1) {
            state = springStep(state, 100, 1 / 60);
            overshoot = Math.max(overshoot, state.value - 100);
        }
        expect(state.value).toBeCloseTo(100, 1);
        expect(overshoot).toBeLessThanOrEqual(0);
    });

    it('faehrt monoton auf das Ziel zu', () => {
        let state = { value: 0, velocity: 0 };
        const path: number[] = [];
        for (let i = 0; i < 120; i += 1) {
            state = springStep(state, 100, 1 / 60);
            path.push(state.value);
        }
        expect(path).toEqual([...path].sort((a, b) => a - b));
    });

    it('deckelt einen sehr langen Zeitschritt, statt in einem Sprung anzukommen', () => {
        const jump = springStep({ value: 0, velocity: 0 }, 100, 5);
        expect(jump.value).toBeLessThan(100);
    });

    it('bewegt sich ohne Zeit gar nicht', () => {
        const state = { value: 3, velocity: 7 };
        expect(springStep(state, 100, 0)).toBe(state);
    });
});

describe('die Deckel', () => {
    it('nennt die Spur zwischen sechs und zehn Knoten', () => {
        expect(TRAIL_NODE_LIMIT).toBe(10);
        expect(TRAIL_NODE_LIMIT).toBeGreaterThanOrEqual(6);
    });

    it('deckelt die gezeichneten Koerper', () => {
        expect(DRAWN_BODIES_CAP).toBeGreaterThan(0);
    });
});
