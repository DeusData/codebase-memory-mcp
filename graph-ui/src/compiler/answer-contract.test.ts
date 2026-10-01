import { describe, expect, it } from 'vitest';

import {
    NO_CARD_SENTENCE,
    checkCitations,
    citationsIn,
    claimLines,
    isNoCardLine,
    segmentsOf,
} from './answer-contract';

const CARDS = ['K1', 'K2', 'K3'];

describe('citationsIn', () => {
    it('reads one citation', () => {
        expect(citationsIn('createUser is called by create [K2].')).toEqual(['K2']);
    });

    it('reads several in one bracket and several brackets', () => {
        expect(citationsIn('a [K1, K2] b [K3]')).toEqual(['K1', 'K2', 'K3']);
    });

    it('names each card once', () => {
        expect(citationsIn('[K1] and [K1] again')).toEqual(['K1']);
    });

    it('reads nothing out of a line with no citation', () => {
        expect(citationsIn('createUser is called by create.')).toEqual([]);
        expect(citationsIn('an array index [0] is not a citation')).toEqual([]);
    });
});

describe('claim lines', () => {
    it('counts every non-empty line that is not the no-card sentence', () => {
        expect(claimLines('a [K1]\n\nb [K2]\n')).toEqual(['a [K1]', 'b [K2]']);
    });

    it('does not count the agreed sentence in either language', () => {
        expect(claimLines(NO_CARD_SENTENCE)).toEqual([]);
        expect(claimLines('Dazu liegt keine Karte vor.')).toEqual([]);
        expect(isNoCardLine('No card covers this.')).toBe(true);
        expect(isNoCardLine('keine Karte dazu')).toBe(true);
        expect(isNoCardLine('createUser is called by create')).toBe(false);
    });
});

describe('checkCitations', () => {
    it('accepts an answer where every line cites an existing card', () => {
        const check = checkCitations('createUser is called by create [K2].\nAlso by routes [K3].', CARDS);
        expect(check.ok).toBe(true);
        expect(check.cited).toEqual(['K2', 'K3']);
        expect(check.violations).toEqual([]);
    });

    it('refuses a line with no citation', () => {
        const check = checkCitations('createUser is called by create.', CARDS);
        expect(check.ok).toBe(false);
        expect(check.violations[0].reason).toBe('no-citation');
    });

    it('refuses a citation of a card that was never given', () => {
        const check = checkCitations('createUser is called by create [K9].', CARDS);
        expect(check.ok).toBe(false);
        expect(check.unknown).toEqual(['K9']);
        expect(check.violations[0].reason).toBe('unknown-card');
    });

    it('accepts the no-card sentence on its own, with no cards at all', () => {
        const check = checkCitations(NO_CARD_SENTENCE, []);
        expect(check.ok).toBe(true);
        expect(check.noCardOnly).toBe(true);
    });

    it('refuses an empty answer', () => {
        expect(checkCitations('', CARDS).ok).toBe(false);
    });

    it('refuses an answer that mixes the sentence with an uncited claim', () => {
        const check = checkCitations(`${NO_CARD_SENTENCE}\nBut it probably calls insert.`, CARDS);
        expect(check.ok).toBe(false);
        expect(check.noCardOnly).toBe(false);
    });
});

/*
 * Die drei Faelle des W7c-Befundes, an einer Stelle.
 *
 * Der Befund ist gemessen (verification/w7/chat.json,
 * extras.lowercase.citationsInSeam ist 0, waehrend im Bild sieben Zitat-Knoepfe
 * standen): bei einer Antwort aus GENAU EINER gekuerzten Zeile laesst die
 * Kuerzungsregel nichts uebrig, und was dann herauskommt, sieht aus wie eine
 * makellose Bilanz. Die drei Faelle unterscheiden sich nur in einer Zeile und
 * in einem Schalter, und genau daran haengt der Unterschied zwischen "geprueft
 * und sauber" und "nicht geprueft".
 */
describe('eine gekuerzte Antwort und was an ihr gemessen wurde', () => {
    it('mehrzeilig gekuerzt: die letzte Zeile faellt weg, der Rest wird gemessen', () => {
        const check = checkCitations(
            'createUser is called by create [K2].\nAnd it also touches the',
            CARDS,
            { truncated: true },
        );
        expect(check.measured).toBe(true);
        expect(check.ok).toBe(true);
        expect(check.cited).toEqual(['K2']);
        // Die abgeschnittene Zeile ist keine Fundstelle: sie hatte keinen Platz
        // mehr fuer ihr Zitat.
        expect(check.violations).toEqual([]);
    });

    it('einzeilig gekuerzt: es bleibt nichts uebrig, also ist nichts gemessen', () => {
        const check = checkCitations('createUser is called by the', CARDS, { truncated: true });
        expect(check.measured).toBe(false);
        expect(check.ok).toBe(false);
        expect(check.cited).toEqual([]);
        expect(check.violations).toEqual([]);
    });

    it('einzeilig vollstaendig: eine Zeile bleibt stehen und wird gemessen', () => {
        const check = checkCitations('createUser is called by create [K2].', CARDS);
        expect(check.measured).toBe(true);
        expect(check.ok).toBe(true);
        expect(check.cited).toEqual(['K2']);
    });

    it('einzeilig vollstaendig und ohne Zitat: gemessen, und der Verstoss steht da', () => {
        const check = checkCitations('createUser is called by create.', CARDS);
        expect(check.measured).toBe(true);
        expect(check.ok).toBe(false);
        expect(check.violations).toHaveLength(1);
    });

    it('die No-Card-Antwort gilt als gemessen, auch gekuerzt', () => {
        expect(checkCitations(NO_CARD_SENTENCE, [], { truncated: true }).measured).toBe(true);
        expect(checkCitations(NO_CARD_SENTENCE, []).measured).toBe(true);
    });

    it('eine leere Antwort ist nichts, woran man Zitattreue messen koennte', () => {
        // Sie faellt damit aus der Zitattreue heraus und nicht durch: die
        // Trefferquote der Eval zaehlt sie weiter als nicht bestanden, weil dort
        // die erwarteten Namen fehlen.
        expect(checkCitations('', CARDS).measured).toBe(false);
        expect(checkCitations('', CARDS).ok).toBe(false);
    });
});

describe('segmentsOf', () => {
    it('splits a line into text and citation buttons', () => {
        expect(segmentsOf('a [K1] b', CARDS)).toEqual([
            { kind: 'text', text: 'a ' },
            { kind: 'citation', cardId: 'K1', text: '[K1]', known: true },
            { kind: 'text', text: ' b' },
        ]);
    });

    it('marks a citation of a card that was never given', () => {
        const segments = segmentsOf('a [K9]', CARDS);
        expect(segments[1]).toEqual({ kind: 'citation', cardId: 'K9', text: '[K9]', known: false });
    });

    it('leaves a line with no citation as one piece of text', () => {
        expect(segmentsOf('plain text', CARDS)).toEqual([{ kind: 'text', text: 'plain text' }]);
    });

    it('keeps brackets that are not citations as text', () => {
        expect(segmentsOf('rows[0] is a row', CARDS)).toEqual([
            { kind: 'text', text: 'rows[0] is a row' },
        ]);
    });
});
