/*
 * Die Buchfuehrung ueber den Strom: Akteure, Reihenfolge, Zahlen.
 *
 * Geprueft wird vor allem, was NICHT passiert: keine Luecke, wo die Oberflaeche
 * spaeter zugehoert hat; keine Zahl, die geschaetzt statt gezaehlt ist; kein
 * Koerper, der zurueckspringt, weil ein aelteres Ereignis nachgekommen ist.
 */

import { describe, expect, it } from 'vitest';

import { readAgentEvent } from './agent-event';
import type { AgentEvent } from './agent-event';
import {
    ACTIVE_WINDOW_MS,
    activeActors,
    activityStrip,
    emptyAgentsState,
    eventsPerMinute,
    isActive,
    recentPaths,
    timeAtPlace,
    withEvent,
    withYouEvent,
} from './agent-store';

const NOW = 1788040000000;

const event = (over: Partial<AgentEvent>): AgentEvent => readAgentEvent({
    ts: NOW, agent: 'implementer', run: 'r1', seq: 1, phase: 'end',
    tool: 'Edit', path: 'src/a.ts', detail: '', ...over,
}) as AgentEvent;

const fold = (events: readonly AgentEvent[]) =>
    events.reduce((state, entry) => withEvent(state, entry), emptyAgentsState());

describe('der Zustand des Stroms', () => {
    it('fuehrt einen Akteur je Anzeigename und nicht je Lauf', () => {
        const state = fold([
            event({ run: 'r1', seq: 1 }),
            event({ run: 'r2', seq: 1, ts: NOW + 10 }),
        ]);
        expect(state.actors).toHaveLength(1);
        expect(state.actors[0]?.runs).toHaveLength(2);
        expect(state.actors[0]?.count).toBe(2);
    });

    it('meldet eine Luecke in der Reihenfolge, und zwar genau die fehlenden', () => {
        const state = fold([
            event({ seq: 5 }),
            event({ seq: 6, ts: NOW + 10 }),
            event({ seq: 9, ts: NOW + 20 }),
        ]);
        expect(state.missed).toBe(2);
        expect(state.actors[0]?.missed).toBe(2);
    });

    it('haelt das erste Ereignis eines Laufs NIE fuer eine Luecke', () => {
        // Die Oberflaeche ist bei Nummer 40 eingestiegen. Das sind keine
        // verlorenen Nachrichten, das ist ein spaeter Zuhoerer.
        const state = fold([event({ seq: 40 }), event({ seq: 41, ts: NOW + 10 })]);
        expect(state.missed).toBe(0);
        expect(state.actors[0]?.runs[0]?.joinedAt).toBe(40);
    });

    it('merkt sich je Lauf die hoechste Nummer, fuer die Wiederaufnahme', () => {
        const state = fold([
            event({ run: 'r1', seq: 3 }),
            event({ run: 'r2', seq: 7, ts: NOW + 1 }),
            event({ run: 'r1', seq: 2, ts: NOW + 2 }),
        ]);
        expect(state.seen.get('r1')).toBe(3);
        expect(state.seen.get('r2')).toBe(7);
    });

    it('laesst den Koerper nicht zurueckspringen, wenn ein aelteres Ereignis nachkommt', () => {
        const state = fold([
            event({ seq: 2, ts: NOW, path: 'src/new.ts' }),
            event({ seq: 1, ts: NOW - 5000, path: 'src/old.ts' }),
        ]);
        expect(state.actors[0]?.last.path).toBe('src/new.ts');
        expect(state.actors[0]?.lastTs).toBe(NOW);
    });

    it('kennzeichnet die eigene Bewegung des Lesers als solche', () => {
        const state = withYouEvent(emptyAgentsState(), event({ agent: 'you', tool: 'Open' }));
        expect(state.actors[0]?.you).toBe(true);
        expect(fold([event({})]).actors[0]?.you).toBe(false);
    });
});

describe('die Zahlen des Kopfes', () => {
    it('zaehlt den Aktivitaetsstreifen aus den Ereignissen und malt nichts dazu', () => {
        const strip = activityStrip([
            event({ ts: NOW - 500 }),
            event({ ts: NOW - 700 }),
            event({ ts: NOW - 5500 }),
            event({ ts: NOW - 40000 }), // ausserhalb des Fensters
        ], NOW, 30);
        expect(strip).toHaveLength(30);
        expect(strip[29]).toBe(2);
        expect(strip[24]).toBe(1);
        expect(strip.reduce((sum, value) => sum + value, 0)).toBe(3);
    });

    it('setzt ein Ereignis, das der Uhr voraus ist, in die juengste Sekunde', () => {
        // Die Uhr des Instruments tickt im Sekundentakt; ein eigenes Ereignis
        // des Lesers entsteht zwischen zwei Ticks. Es wegzulassen hiesse, "in
        // den letzten dreissig Sekunden ist nichts passiert" zu zeigen, waehrend
        // gerade etwas passiert ist.
        const strip = activityStrip([event({ ts: NOW + 400 })], NOW, 30);
        expect(strip[29]).toBe(1);
        expect(strip.reduce((sum, value) => sum + value, 0)).toBe(1);
    });

    it('laesst weg, was weiter in der Zukunft liegt als der Taktversatz', () => {
        const strip = activityStrip([event({ ts: NOW + 9000 })], NOW, 30);
        expect(strip.reduce((sum, value) => sum + value, 0)).toBe(0);
    });

    it('zaehlt die Ereignisse der letzten Minute und schaetzt sie nicht', () => {
        const state = fold([
            event({ seq: 1, ts: NOW - 1000 }),
            event({ seq: 2, ts: NOW - 30000 }),
            event({ seq: 3, ts: NOW - 90000 }),
        ]);
        expect(eventsPerMinute(state, NOW)).toBe(2);
    });

    it('rechnet die Verweildauer ueber die zusammenhaengende Kette an EINEM Ort', () => {
        const state = fold([
            event({ seq: 1, ts: NOW - 30000, path: 'src/b.ts' }),
            event({ seq: 2, ts: NOW - 20000, path: 'src/a.ts' }),
            event({ seq: 3, ts: NOW - 10000, path: 'src/a.ts' }),
            event({ seq: 4, ts: NOW - 2000, path: 'src/a.ts' }),
        ]);
        expect(timeAtPlace(state.actors[0]!, NOW)).toBe(20000);
    });

    it('zeigt einen Akteur, solange sein letztes Ereignis im Fenster liegt', () => {
        const fresh = fold([event({ ts: NOW - 1000 })]);
        const stale = fold([event({ ts: NOW - ACTIVE_WINDOW_MS - 1 })]);
        expect(isActive(fresh.actors[0]!, NOW)).toBe(true);
        expect(isActive(stale.actors[0]!, NOW)).toBe(false);
        expect(activeActors(stale, NOW)).toHaveLength(0);
    });

    it('sortiert die Akteure nach der juengsten Bewegung', () => {
        const state = fold([
            event({ agent: 'a', run: 'ra', ts: NOW - 5000 }),
            event({ agent: 'b', run: 'rb', ts: NOW - 1000 }),
        ]);
        expect(activeActors(state, NOW).map((actor) => actor.id)).toEqual(['b', 'a']);
    });

    it('baut die Wegzeile aus Pfaden, die wirklich in Ereignissen stehen', () => {
        const state = fold([
            event({ seq: 1, ts: NOW - 4000, path: 'src/a.ts' }),
            event({ seq: 2, ts: NOW - 3000, path: 'src/b.ts' }),
            event({ seq: 3, ts: NOW - 2000, path: 'src/b.ts' }),
            event({ seq: 4, ts: NOW - 1000, tool: 'Bash', path: '', detail: 'ls' }),
        ]);
        expect(recentPaths(state.actors[0]!, NOW, 0)).toEqual(['src/b.ts', 'src/a.ts']);
        // Ein Fenster nimmt weg, was aelter ist, und erfindet nichts dazu.
        expect(recentPaths(state.actors[0]!, NOW, 2500)).toEqual(['src/b.ts']);
    });
});
