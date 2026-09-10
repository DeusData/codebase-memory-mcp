/**
 * Die Rechnung der Schrittkarte, ohne Karte.
 *
 * Drei Dinge werden hier festgenagelt, weil sie sich in einem Widget nur
 * schlecht festnageln lassen und sich falsch anfuehlen, wenn sie falsch sind:
 * dass die Enden eines Walks Enden sind und keine Umbrueche, dass die
 * Fortschrittskette bei einem langen Walk nicht mitwaechst, und dass ein
 * ungekappter Walk nichts ueber seine Vollstaendigkeit behauptet.
 */

import { describe, expect, it } from 'vitest';

import {
    PROGRESS_EMPTY,
    PROGRESS_FILLED,
    PROGRESS_WIDTH,
    capNote,
    isLastStep,
    playerIntent,
    progressBar,
    progressLabel,
    stepMove,
} from './tour-player';

describe('the step counter', () => {
    it('counts from one, because the reader is not counting from zero', () => {
        expect(progressLabel(0, 10)).toBe('STEP 1/10');
        expect(progressLabel(9, 10)).toBe('STEP 10/10');
    });

    it('clamps an index that is past the walk rather than printing it', () => {
        expect(progressLabel(40, 10)).toBe('STEP 10/10');
        expect(progressLabel(-3, 10)).toBe('STEP 1/10');
    });

    it('says nothing about a walk with no steps', () => {
        expect(progressLabel(0, 0)).toBe('STEP 0/0');
        expect(progressBar(0, 0)).toBe('');
    });
});

describe('the progress chain', () => {
    it('draws one block per step while the walk is short', () => {
        expect(progressBar(0, 4)).toBe(PROGRESS_FILLED + PROGRESS_EMPTY.repeat(3));
        expect(progressBar(3, 4)).toBe(PROGRESS_FILLED.repeat(4));
    });

    it('fills the block the reader is standing on', () => {
        // Standing on step two of ten is two blocks, not one: they are on it.
        expect([...progressBar(1, 10)].filter((block) => block === PROGRESS_FILLED)).toHaveLength(2);
    });

    it('stops growing past its width, so a long walk keeps one line', () => {
        expect(progressBar(0, 200)).toHaveLength(PROGRESS_WIDTH);
        expect(progressBar(199, 200)).toHaveLength(PROGRESS_WIDTH);
    });

    it('never shows an empty chain on the first step of a long walk', () => {
        expect(progressBar(0, 200).startsWith(PROGRESS_FILLED)).toBe(true);
    });
});

describe('moving through a walk', () => {
    it('clamps at both ends', () => {
        expect(stepMove(5, 0, -1)).toBe(0);
        expect(stepMove(5, 4, 1)).toBe(4);
        expect(stepMove(5, 2, 1)).toBe(3);
        expect(stepMove(5, 2, -1)).toBe(1);
    });

    it('has nowhere to move in an empty walk', () => {
        expect(stepMove(0, 0, 1)).toBe(0);
    });

    it('knows when next means finish', () => {
        expect(isLastStep(4, 5)).toBe(true);
        expect(isLastStep(3, 5)).toBe(false);
        expect(isLastStep(0, 0)).toBe(false);
    });
});

describe('the four keys', () => {
    it('maps exactly the keys the card offers', () => {
        expect(playerIntent('Enter')).toBe('next');
        expect(playerIntent('ArrowLeft')).toBe('prev');
        expect(playerIntent('q')).toBe('exit');
        expect(playerIntent('Q')).toBe('exit');
    });

    /* Audit-Befund 13: PLAN Abschnitt 4 nennt `[d] diagram` als Aktion der Karte. */
    it('reads d as the diagram, in both cases', () => {
        expect(playerIntent('d')).toBe('diagram');
        expect(playerIntent('D')).toBe('diagram');
    });

    it('leaves every other key alone', () => {
        for (const key of ['ArrowRight', 'Escape', 'a', ' ', 'Backspace', 'e']) {
            expect(playerIntent(key)).toBe('none');
        }
    });

    /*
     * Die Gegenprobe zu den Menuekuerzeln (src/app/keyboard.test.ts): waehrend
     * einer Fuehrung gelten beide Griffe am selben Fenster, und ein Buchstabe,
     * den beide fuer sich beanspruchen, taete zwei Dinge auf einen Druck.
     */
    it('claims none of the letters the atlas row uses', () => {
        for (const key of ['w', 'b', 'c', 'l', 'a']) {
            expect(playerIntent(key), key).toBe('none');
        }
    });
});

describe('what a bounded walk is allowed to say', () => {
    it('names both numbers when a bound bit', () => {
        expect(capNote(true, 3, 2)).toBe(
            'walk capped at 3 symbols (depth 2), so what follows this is not shown',
        );
    });

    it('says nothing at all when it did not', () => {
        // Deliberately not "complete": this side cannot see what the index did
        // not record, so it is in no position to say the reader saw everything.
        expect(capNote(false, 15, 3)).toBe('');
    });
});
