import { describe, expect, it } from 'vitest';

import {
    LAYOUT_BIG_STEP,
    LAYOUT_CEILING,
    LAYOUT_DEFAULT,
    LAYOUT_KEYS,
    LAYOUT_MIN,
    LAYOUT_STEP,
    type LayoutStore,
    clampLayout,
    clampLayoutValue,
    defaultLayout,
    layoutBounds,
    layoutStorageKey,
    readLayout,
    sameLayout,
    writeLayout,
} from './layout-model';

const FRAME = { width: 1680, height: 1050 };

function storeOf(entries: Record<string, string> = {}): LayoutStore & { entries: Record<string, string> } {
    return {
        entries,
        getItem: (key: string) => entries[key] ?? null,
        setItem: (key: string, value: string) => {
            entries[key] = value;
        },
    };
}

describe('die Grenzen einer Zone', () => {

    it('laesst keine Zone unter ihr Mindestmass', () => {
        for (const key of LAYOUT_KEYS) {
            expect(clampLayoutValue(key, 0, FRAME)).toBe(LAYOUT_MIN[key]);
            expect(clampLayoutValue(key, -400, FRAME)).toBe(LAYOUT_MIN[key]);
        }
    });

    it('laesst keine Zone ueber ihren Anteil am Fenster', () => {
        for (const key of LAYOUT_KEYS) {
            const { max } = layoutBounds(key, FRAME);
            expect(clampLayoutValue(key, 99999, FRAME)).toBe(max);
            expect(max).toBeLessThanOrEqual(LAYOUT_CEILING[key]);
        }
    });

    it('haelt die Vorgabe in ihren eigenen Grenzen', () => {
        for (const key of LAYOUT_KEYS) {
            const { min, max } = layoutBounds(key, FRAME);
            expect(LAYOUT_DEFAULT[key]).toBeGreaterThanOrEqual(min);
            expect(LAYOUT_DEFAULT[key]).toBeLessThanOrEqual(max);
        }
    });

    /*
     * Der Fall, den eine feste Tabelle nicht kann: ein kleines Fenster. Die
     * Hoechstmasse sinken mit, und wenn sie unter das Mindestmass fallen,
     * bleibt die Zone bei ihrem Mindestmass stehen, statt zu verschwinden.
     */
    it('kippt in einem winzigen Fenster nicht in einen umgekehrten Bereich', () => {
        const tiny = { width: 320, height: 300 };
        for (const key of LAYOUT_KEYS) {
            const { min, max } = layoutBounds(key, tiny);
            expect(max).toBeGreaterThanOrEqual(min);
            expect(clampLayoutValue(key, 9999, tiny)).toBeGreaterThanOrEqual(min);
        }
    });

    it('nimmt Unsinn nicht als Mass, sondern faellt auf die Vorgabe', () => {
        for (const key of LAYOUT_KEYS) {
            expect(clampLayoutValue(key, Number.NaN, FRAME))
                .toBe(clampLayoutValue(key, LAYOUT_DEFAULT[key], FRAME));
        }
    });

    it('laesst die beiden Spalten zusammen Platz fuer den Reader', () => {
        const left = layoutBounds('leftWidth', FRAME).max;
        const right = layoutBounds('rightWidth', FRAME).max;
        expect(FRAME.width - left - right).toBeGreaterThan(380);
    });

    it('laesst das Erklaeren-Feld dem Reader Hoehe uebrig', () => {
        expect(layoutBounds('explainHeight', FRAME).max).toBeLessThan(FRAME.height - 300);
    });

    it('gibt der Tastatur einen sichtbaren und einen grossen Schritt', () => {
        expect(LAYOUT_STEP).toBeGreaterThan(8);
        expect(LAYOUT_BIG_STEP).toBeGreaterThan(LAYOUT_STEP * 2);
    });
});

describe('das Gedaechtnis der Masse', () => {

    it('fuehrt jedes Projekt unter seinem eigenen Schluessel', () => {
        expect(layoutStorageKey('alpha')).not.toBe(layoutStorageKey('beta'));
        expect(layoutStorageKey('')).toBe(layoutStorageKey('   '));
    });

    it('liefert ohne Eintrag die Vorgabe', () => {
        expect(readLayout(storeOf(), 'alpha', FRAME)).toEqual(defaultLayout(FRAME));
        expect(readLayout(undefined, 'alpha', FRAME)).toEqual(defaultLayout(FRAME));
    });

    it('schreibt und liest dieselben vier Zahlen', () => {
        const store = storeOf();
        const wanted = { leftWidth: 300, explainHeight: 420, rightWidth: 500, twinHeight: 300 };
        writeLayout(store, 'alpha', wanted);
        expect(readLayout(store, 'alpha', FRAME)).toEqual(wanted);
        expect(sameLayout(readLayout(store, 'alpha', FRAME), wanted)).toBe(true);
    });

    it('haelt einen gespeicherten Riesen in den Grenzen des Fensters', () => {
        const store = storeOf();
        writeLayout(store, 'alpha', { ...LAYOUT_DEFAULT, explainHeight: 99999 });
        expect(readLayout(store, 'alpha', FRAME).explainHeight)
            .toBe(layoutBounds('explainHeight', FRAME).max);
    });

    it('nimmt einen kaputten Eintrag nicht als Layout', () => {
        const key = layoutStorageKey('alpha');
        expect(readLayout(storeOf({ [key]: 'not json' }), 'alpha', FRAME)).toEqual(defaultLayout(FRAME));
        expect(readLayout(storeOf({ [key]: 'null' }), 'alpha', FRAME)).toEqual(defaultLayout(FRAME));
        expect(readLayout(storeOf({ [key]: '{"leftWidth":"wide"}' }), 'alpha', FRAME))
            .toEqual(defaultLayout(FRAME));
    });

    it('nimmt aus einem halben Eintrag nur, was eine Zahl ist', () => {
        const key = layoutStorageKey('alpha');
        const read = readLayout(storeOf({ [key]: '{"leftWidth":300}' }), 'alpha', FRAME);
        expect(read.leftWidth).toBe(300);
        expect(read.rightWidth).toBe(LAYOUT_DEFAULT.rightWidth);
    });

    it('faellt nicht um, wenn der Speicher verweigert', () => {
        const broken: LayoutStore = {
            getItem: () => {
                throw new Error('denied');
            },
            setItem: () => {
                throw new Error('denied');
            },
        };
        expect(readLayout(broken, 'alpha', FRAME)).toEqual(defaultLayout(FRAME));
        expect(() => writeLayout(broken, 'alpha', LAYOUT_DEFAULT)).not.toThrow();
    });

    it('bringt jedes gelesene Layout in die Grenzen', () => {
        const clamped = clampLayout(
            { leftWidth: 10, explainHeight: 10, rightWidth: 10, twinHeight: 10 },
            FRAME,
        );
        expect(clamped).toEqual(LAYOUT_MIN);
    });
});
