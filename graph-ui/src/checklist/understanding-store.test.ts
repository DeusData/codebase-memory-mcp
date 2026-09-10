/**
 * Der stille Lesestand dieses Browsers.
 *
 * Vier Eigenschaften, die alle vier die Ehrlichkeitsregel tragen: es wird nur
 * vermerkt, wozu jemand gebracht wurde, ein Vermerk zaehlt einmal, ein Haken ist
 * etwas anderes als ein Vermerk, und ein Symbol ohne Checkliste bekommt gar
 * keinen Zaehler statt einer Null.
 */

import { describe, expect, it } from 'vitest';

import type { ChecklistItem } from '../core/semantic-ir';
import {
    exploredLabel,
    markVisited,
    readUnderstanding,
    setConfirmed,
    totalMarks,
    understandingOf,
    understandingKey,
} from './understanding-store';
import type { KeyValueStore } from './understanding-store';

function memoryStore(seed: Record<string, string> = {}): KeyValueStore & { map: Map<string, string> } {
    const map = new Map<string, string>(Object.entries(seed));
    return {
        map,
        getItem: (key) => map.get(key) ?? null,
        setItem: (key, value) => {
            map.set(key, value);
        },
    };
}

const items: ChecklistItem[] = [
    { id: 'a', category: 'core-logic', label: 'Understand the call to validateUser', done: false },
    { id: 'b', category: 'core-logic', label: 'Understand the call to insert', done: false },
    { id: 'c', category: 'callers', label: 'See who calls this: registerUserRoutes', done: false },
];

describe('the record', () => {
    it('is empty before anything happened', () => {
        expect(readUnderstanding(memoryStore(), 'p')).toEqual({ visited: {}, confirmed: {} });
    });

    it('lives under a key named after the project', () => {
        expect(understandingKey('atlas-sample')).toBe('atlas-understanding:atlas-sample');
    });

    it('treats an unreadable stored value as absent rather than as an error', () => {
        const store = memoryStore({ 'atlas-understanding:p': 'not json' });
        expect(readUnderstanding(store, 'p')).toEqual({ visited: {}, confirmed: {} });
    });

    it('has nothing to read for a project with no name', () => {
        expect(readUnderstanding(memoryStore(), '')).toEqual({ visited: {}, confirmed: {} });
    });
});

describe('marking a visit', () => {
    it('writes the item under its symbol', () => {
        const store = memoryStore();
        const record = markVisited(store, 'p', 'p.s.createUser', 'a');
        expect(record.visited['p.s.createUser']).toEqual(['a']);
        expect(readUnderstanding(store, 'p').visited['p.s.createUser']).toEqual(['a']);
    });

    it('counts one visit once, however often the reader arrives', () => {
        const store = memoryStore();
        markVisited(store, 'p', 'p.s.createUser', 'a');
        markVisited(store, 'p', 'p.s.createUser', 'a');
        const record = markVisited(store, 'p', 'p.s.createUser', 'a');
        expect(record.visited['p.s.createUser']).toEqual(['a']);
        expect(totalMarks(record)).toBe(1);
    });

    it('keeps the symbols apart', () => {
        const store = memoryStore();
        markVisited(store, 'p', 'p.s.createUser', 'a');
        const record = markVisited(store, 'p', 'p.s.listUsers', 'a');
        expect(totalMarks(record)).toBe(2);
    });

    it('records nothing without a project, a symbol or an item', () => {
        const store = memoryStore();
        expect(totalMarks(markVisited(store, '', 'p.s', 'a'))).toBe(0);
        expect(totalMarks(markVisited(store, 'p', '', 'a'))).toBe(0);
        expect(totalMarks(markVisited(store, 'p', 'p.s', ''))).toBe(0);
    });

    it('never touches the confirmations', () => {
        const store = memoryStore();
        setConfirmed(store, 'p', 'p.s.createUser', 'b', true);
        const record = markVisited(store, 'p', 'p.s.createUser', 'a');
        expect(record.confirmed['p.s.createUser']).toEqual(['b']);
        expect(record.visited['p.s.createUser']).toEqual(['a']);
    });
});

describe('confirming', () => {
    it('is written only by an explicit tick, and can be taken back', () => {
        const store = memoryStore();
        expect(setConfirmed(store, 'p', 'p.s', 'a', true).confirmed['p.s']).toEqual(['a']);
        expect(setConfirmed(store, 'p', 'p.s', 'a', false).confirmed['p.s']).toEqual([]);
    });

    it('is not a visit', () => {
        const store = memoryStore();
        const record = setConfirmed(store, 'p', 'p.s', 'a', true);
        expect(totalMarks(record)).toBe(0);
    });
});

describe('the state one symbol is in', () => {
    it('draws the reader marks over the generated checklist', () => {
        const store = memoryStore();
        markVisited(store, 'p', 'p.s.createUser', 'a');
        setConfirmed(store, 'p', 'p.s.createUser', 'a', true);
        const state = understandingOf(readUnderstanding(store, 'p'), 'p.s.createUser', items);
        expect(state.items.map((entry) => entry.visited)).toEqual([true, false, false]);
        expect(state.exploration).toEqual({ visited: 1, total: 3 });
        expect(state.verification).toEqual({ confirmed: 1, total: 3 });
    });

    it('says nothing about a symbol whose checklist is empty', () => {
        const state = understandingOf({ visited: {}, confirmed: {} }, 'p.s', []);
        expect(exploredLabel(state)).toBeUndefined();
    });

    it('says nothing at all when there is no state', () => {
        expect(exploredLabel(undefined)).toBeUndefined();
    });

    it('reads as the status bar shows it once there is something to count', () => {
        const store = memoryStore();
        markVisited(store, 'p', 'p.s.createUser', 'a');
        const state = understandingOf(readUnderstanding(store, 'p'), 'p.s.createUser', items);
        expect(exploredLabel(state)).toBe('1 of 3');
    });
});
