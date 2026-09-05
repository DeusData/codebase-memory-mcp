/**
 * Was dieser Browser sich ueber die Frage merkt.
 *
 * Die eine Eigenschaft, die zaehlt: "Not now" ist eine Antwort. Wer sie gibt,
 * wird nicht wieder gefragt, und der Unterschied zwischen "gefragt und
 * abgelehnt" und "nie gefragt" bleibt lesbar.
 */

import { describe, expect, it } from 'vitest';

import type { KeyValueStore } from '../checklist/understanding-store';
import { readWhyAnswer, recordWhyAnswer, whyKey } from './why-store';

function memoryStore(seed: Record<string, string> = {}): KeyValueStore {
    const map = new Map<string, string>(Object.entries(seed));
    return {
        getItem: (key) => map.get(key) ?? null,
        setItem: (key, value) => {
            map.set(key, value);
        },
    };
}

/** A store that refuses everything, as a browser with site data switched off does. */
const refusing: KeyValueStore = {
    getItem: () => {
        throw new Error('access denied');
    },
    setItem: () => {
        throw new Error('access denied');
    },
};

describe('the recorded answer', () => {
    it('lives under a key named after the project', () => {
        expect(whyKey('atlas-sample')).toBe('atlas-why:atlas-sample');
    });

    it('is "never asked" before anything happened', () => {
        expect(readWhyAnswer(memoryStore(), 'p')).toEqual({ asked: false });
    });

    it('remembers which way in was chosen', () => {
        const store = memoryStore();
        recordWhyAnswer(store, 'p', 'understand');
        expect(readWhyAnswer(store, 'p')).toEqual({ asked: true, intent: 'understand' });
    });

    it('treats a decline as an answer, with no intent on it', () => {
        const store = memoryStore();
        recordWhyAnswer(store, 'p');
        expect(readWhyAnswer(store, 'p')).toEqual({ asked: true });
    });

    it('keeps two projects apart', () => {
        const store = memoryStore();
        recordWhyAnswer(store, 'one', 'bug');
        expect(readWhyAnswer(store, 'two')).toEqual({ asked: false });
    });

    it('reads an unknown intent as asked without one, rather than as a mode', () => {
        const store = memoryStore({ 'atlas-why:p': JSON.stringify({ asked: true, intent: 'teach' }) });
        expect(readWhyAnswer(store, 'p')).toEqual({ asked: true });
    });

    it('reads an unreadable or half-written value as never asked', () => {
        expect(readWhyAnswer(memoryStore({ 'atlas-why:p': '{' }), 'p')).toEqual({ asked: false });
        expect(readWhyAnswer(memoryStore({ 'atlas-why:p': '{"intent":"bug"}' }), 'p')).toEqual({ asked: false });
    });

    it('survives a storage that refuses, at the cost of one repeated question', () => {
        expect(readWhyAnswer(refusing, 'p')).toEqual({ asked: false });
        expect(recordWhyAnswer(refusing, 'p', 'bug')).toEqual({ asked: true, intent: 'bug' });
    });

    it('has nothing to remember for a project with no name', () => {
        expect(recordWhyAnswer(memoryStore(), '', 'bug')).toEqual({ asked: true, intent: 'bug' });
        expect(readWhyAnswer(memoryStore(), '')).toEqual({ asked: false });
    });
});
