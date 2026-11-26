/*
 * Das Gedaechtnis fuer die Frage "wer liest".
 *
 * Der eine Satz, den dieser Speicher tragen muss: jeder Zweifel endet in der
 * Mitte. Ein unlesbarer Wert, eine gesperrte Ablage, eine Zahl ausserhalb der
 * Leiter: alles davon heisst "niemand hat es gesagt", und wer nichts gesagt
 * hat, bekommt die Stufe, die die aufgezeichneten Fakten zeigt, wie sie sind.
 */

import { describe, expect, it } from 'vitest';

import type { KeyValueStore } from '../checklist/understanding-store';
import {
    READER_LEVEL_DEFAULT,
    READER_LEVEL_KEY,
    readReaderLevel,
    writeReaderLevel,
} from './reader-level-store';

function memoryStore(initial: Record<string, string> = {}): KeyValueStore & { data: Record<string, string> } {
    const data = { ...initial };
    return {
        data,
        getItem: (key) => (key in data ? data[key] : null),
        setItem: (key, value) => {
            data[key] = value;
        },
    };
}

const lockedStore: KeyValueStore = {
    getItem: () => {
        throw new Error('site data is switched off in this browser');
    },
    setItem: () => {
        throw new Error('site data is switched off in this browser');
    },
};

describe('readReaderLevel', () => {
    it('gibt zurueck, was dieser Browser zuletzt gesagt hat', () => {
        expect(readReaderLevel(memoryStore({ [READER_LEVEL_KEY]: '4' }))).toBe(4);
        expect(readReaderLevel(memoryStore({ [READER_LEVEL_KEY]: '0' }))).toBe(0);
    });

    it('faellt auf die Medior-Stufe zurueck, wenn nichts gesagt wurde', () => {
        expect(readReaderLevel(memoryStore())).toBe(READER_LEVEL_DEFAULT);
        expect(READER_LEVEL_DEFAULT).toBe(2);
    });

    it('faellt auf dieselbe Stufe zurueck, wenn der Wert unbrauchbar ist', () => {
        for (const raw of ['', 'senior', '9', '-1', '2.5', 'null']) {
            expect(readReaderLevel(memoryStore({ [READER_LEVEL_KEY]: raw })), raw)
                .toBe(READER_LEVEL_DEFAULT);
        }
    });

    it('faellt zurueck statt zu werfen, wenn die Ablage die Auskunft verweigert', () => {
        expect(readReaderLevel(lockedStore)).toBe(READER_LEVEL_DEFAULT);
    });
});

describe('writeReaderLevel', () => {
    it('schreibt die Stufe unter den einen Schluessel, ohne Projektnamen', () => {
        const store = memoryStore();
        writeReaderLevel(store, 3);
        expect(store.data).toEqual({ 'atlas-reader': '3' });
    });

    it('schweigt, wenn die Ablage die Schreibung verweigert', () => {
        // Ein privates Fenster ist keine Stoerung, ueber die jemand eine
        // Meldung lesen muss: die Stufe gilt fuer diese Sitzung weiter.
        expect(() => writeReaderLevel(lockedStore, 1)).not.toThrow();
    });
});
