/*
 * Der Zeitstrahl, ohne einen gerenderten Pixel.
 *
 * Vier Zusicherungen, und alle vier sind die Ehrlichkeitsregeln dieser Flaeche:
 *
 *  1. Ein Strich je Ereignis, keine Glaettung. Was ausserhalb des Fensters
 *     liegt, steht nicht darin, und was darin liegt, steht an der Stelle seiner
 *     Zeit.
 *  2. Die Pause haelt das Fenster an. Ereignisse laufen weiter ein.
 *  3. Ein Sprung in die Vergangenheit ist als Wiedergabe gekennzeichnet und
 *     schlaegt eine Pause.
 *  4. `unbegrenzt` reicht bis zum ersten behaltenen Ereignis.
 */

import { describe, expect, it } from 'vitest';

import { TIMELINE_MIN_WIDTH, TIMELINE_WINDOWS, buildTimeline, timeAtFraction } from './agent-timeline';
import type { TimelineActor } from './agent-timeline';

const NOW = 1788139345000;

const actor = (id: string, offsets: number[]): TimelineActor => ({
    id,
    name: id,
    color: '#ffffff',
    letter: id.slice(0, 1).toUpperCase(),
    you: false,
    idle: false,
    firstTs: NOW - Math.max(...offsets, 0),
    events: offsets.map((offset) => ({ ts: NOW - offset, kind: 'read' as const })),
});

describe('das Fenster', () => {
    it('kennt die vier Stufen des Designbildes', () => {
        expect([...TIMELINE_WINDOWS]).toEqual([60000, 300000, 900000, 0]);
    });

    it('laesst weg, was ausserhalb liegt, und setzt den Rest an seine Zeit', () => {
        const timeline = buildTimeline({
            actors: [actor('implementer', [0, 30000, 90000])],
            now: NOW,
            windowMs: 60000,
        });
        expect(timeline.ticks).toBe(2);
        const ticks = timeline.tracks[0]?.ticks ?? [];
        // Die Reihenfolge ist die der behaltenen Ereignisse, und die stehen
        // hier neuestes zuerst: der Strahl zeichnet sie an ihre Zeit, nicht in
        // ihrer Reihenfolge.
        expect([...ticks.map((tick) => Number(tick.at.toFixed(2)))].sort())
            .toEqual([0.5, 1]);
    });

    it('reicht bei "alles Behaltene" bis zum ersten Ereignis', () => {
        const timeline = buildTimeline({
            actors: [actor('implementer', [0, 30000, 900000])],
            now: NOW,
            windowMs: 0,
        });
        expect(timeline.windowMs).toBe(900000);
        expect(timeline.ticks).toBe(3);
    });

    it('zaehlt eine Spur je Akteur', () => {
        const timeline = buildTimeline({
            actors: [actor('a', [0]), actor('b', [1000]), actor('c', [2000])],
            now: NOW,
            windowMs: 60000,
        });
        expect(timeline.tracks).toHaveLength(3);
        expect(timeline.tracks.map((track) => track.count)).toEqual([1, 1, 1]);
    });
});

describe('die drei Lagen', () => {
    const actors = [actor('implementer', [0, 20000])];

    it('laeuft mit, solange niemand anhaelt', () => {
        const timeline = buildTimeline({ actors, now: NOW, windowMs: 60000 });
        expect(timeline.mode).toBe('live');
        expect(timeline.to).toBe(NOW);
    });

    it('haelt das Fenster an, wo der Leser angehalten hat', () => {
        const timeline = buildTimeline({
            actors, now: NOW + 5000, windowMs: 60000, pausedAt: NOW,
        });
        expect(timeline.mode).toBe('paused');
        expect(timeline.to).toBe(NOW);
    });

    it('nennt einen Sprung in die Vergangenheit Wiedergabe und schlaegt die Pause', () => {
        const timeline = buildTimeline({
            actors, now: NOW, windowMs: 60000, pausedAt: NOW - 1000, replayAt: NOW - 20000,
        });
        expect(timeline.mode).toBe('replay');
        expect(timeline.to).toBe(NOW - 20000);
    });
});

describe('der Klick auf den Strahl', () => {
    it('rechnet die Stelle in eine Zeit um und bleibt im Fenster', () => {
        const timeline = buildTimeline({
            actors: [actor('implementer', [0])], now: NOW, windowMs: 60000,
        });
        expect(timeAtFraction(timeline, 0)).toBe(NOW - 60000);
        expect(timeAtFraction(timeline, 1)).toBe(NOW);
        expect(timeAtFraction(timeline, 0.5)).toBe(NOW - 30000);
        expect(timeAtFraction(timeline, -2)).toBe(NOW - 60000);
        expect(timeAtFraction(timeline, 9)).toBe(NOW);
    });
});

describe('die Breite', () => {
    it('nennt die Grenze, unter der ein Strich nicht mehr vom naechsten zu trennen ist', () => {
        expect(TIMELINE_MIN_WIDTH).toBeGreaterThanOrEqual(600);
    });
});
