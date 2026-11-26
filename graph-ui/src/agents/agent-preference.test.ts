/*
 * Was gespeichert wird, und was ausdruecklich nicht.
 *
 * Die Groesse des Instruments, der Umschalter und die drei Schalter des
 * laufenden Blicks ueberleben den Reload. Der Live-Modus tut es nicht, und das
 * ist keine Luecke: eine Seite, die nach dem naechsten Laden von selbst zu
 * reden anfaengt, waere ein Schalter, ueber den der Leser nur einmal
 * entschieden hat.
 */

import { describe, expect, it } from 'vitest';

import {
    AGENTS_KEY_PREFIX,
    DEFAULT_AGENTS_PREFERENCE,
    agentsKey,
    clampAgentsPreference,
    loadAgentsPreference,
    saveAgentsPreference,
} from './agent-preference';
import type { PreferenceStore } from './agent-preference';

function memoryStore(seed: Record<string, string> = {}): PreferenceStore & { map: Map<string, string> } {
    const map = new Map<string, string>(Object.entries(seed));
    return {
        map,
        getItem: (key) => map.get(key) ?? null,
        setItem: (key, value) => {
            map.set(key, value);
        },
    };
}

describe('die Wahl am Instrument', () => {
    it('startet kompakt, mit beiden Akteursarten und ohne einen einzigen Schalter an', () => {
        expect(DEFAULT_AGENTS_PREFERENCE).toEqual({
            size: 'compact',
            filter: 'both',
            follow: false,
            trails: false,
            fullscreen: false,
            trailWindowMs: 60000,
        });
    });

    it('fuehrt keinen Live-Modus, weil er nicht gespeichert wird', () => {
        expect(Object.keys(DEFAULT_AGENTS_PREFERENCE)).not.toContain('live');
        expect(Object.keys(DEFAULT_AGENTS_PREFERENCE)).not.toContain('on');
    });

    it('haengt am Projekt', () => {
        expect(agentsKey('demo')).toBe(`${AGENTS_KEY_PREFIX}demo`);
    });

    it('legt die Wahl ab und liest sie wieder', () => {
        const store = memoryStore();
        saveAgentsPreference(store, 'demo', { ...DEFAULT_AGENTS_PREFERENCE, size: 'collapsed' });
        expect(loadAgentsPreference(store, 'demo').size).toBe('collapsed');
    });

    it('faellt bei jedem Zweifel auf die Vorgabe zurueck', () => {
        expect(loadAgentsPreference(memoryStore(), 'demo')).toEqual(DEFAULT_AGENTS_PREFERENCE);
        expect(loadAgentsPreference(memoryStore({ 'atlas-agents:demo': '{' }), 'demo'))
            .toEqual(DEFAULT_AGENTS_PREFERENCE);
        expect(loadAgentsPreference(undefined, 'demo')).toEqual(DEFAULT_AGENTS_PREFERENCE);
        expect(loadAgentsPreference(memoryStore(), '')).toEqual(DEFAULT_AGENTS_PREFERENCE);
    });

    it('stutzt einen unbekannten Wert auf die Vorgabe und nicht auf den naechstbesten', () => {
        const clamped = clampAgentsPreference({
            size: 'huge', filter: 'nobody', follow: 'yes', trailWindowMs: 7,
        });
        expect(clamped).toEqual(DEFAULT_AGENTS_PREFERENCE);
    });

    it('kostet ein verweigerter Speicher nur das erneute Einstellen', () => {
        const angry: PreferenceStore = {
            getItem: () => { throw new Error('denied'); },
            setItem: () => { throw new Error('denied'); },
        };
        expect(loadAgentsPreference(angry, 'demo')).toEqual(DEFAULT_AGENTS_PREFERENCE);
        expect(saveAgentsPreference(angry, 'demo', DEFAULT_AGENTS_PREFERENCE))
            .toEqual(DEFAULT_AGENTS_PREFERENCE);
    });
});
