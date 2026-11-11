/**
 * Der Tastentest, ohne Fenster.
 *
 * Geprueft wird die eine Frage, fuer die er gebaut ist und die der Nutzerbefund
 * vom 2026-08-29 stellt: WER hat den Tastendruck verbraucht. `defaultPrevented`
 * allein beantwortet sie nicht, weil der Griff dieser Oberflaeche ein erkanntes
 * Kuerzel selbst abbestellt; ein Test, der die beiden Faelle nicht trennt, wuerde
 * bei jedem funktionierenden Kuerzel "jemand anderes war es" melden und damit
 * genau in dem Fall irrefuehren, fuer den er da ist.
 */

import { describe, expect, it } from 'vitest';

import { handledShortcutOf, markHandled, NO_KEY_READING, readKeyEvent } from './key-probe';
import { WIRED_MENU_SHORTCUTS } from './shortcuts';

const BODY = { tagName: 'BODY' };
const INPUT = { tagName: 'INPUT' };

const alt = (letter: string) => ({
    key: letter,
    code: `Key${letter.toUpperCase()}`,
    altKey: true,
});

describe('readKeyEvent', () => {

    it('schreibt die physische Taste UND das Zeichen auf, weil sie sich unterscheiden', () => {
        // Option+A macht unter macOS ein Ring-A. Genau deshalb liest die
        // Verdrahtung `code`, und genau deshalb muss der Test beides zeigen.
        const reading = readKeyEvent(
            { key: 'å', code: 'KeyA', altKey: true },
            BODY,
            WIRED_MENU_SHORTCUTS,
        );
        expect(reading.code).toBe('KeyA');
        expect(reading.key).toBe('å');
        expect(reading.altKey).toBe(true);
        expect(reading.shortcut).toBe('a');
    });

    it('nennt kein Kuerzel, wo keines ist', () => {
        // `z` ist an keinen Menuepunkt vergeben. `g` war es bis W11a und ist
        // seitdem der Live-Modus der Agenten; ein Test, der die Abwesenheit an
        // einem vergebenen Buchstaben prueft, prueft irgendwann das Gegenteil.
        expect(readKeyEvent(alt('z'), BODY, WIRED_MENU_SHORTCUTS).shortcut).toBe('');
        expect(readKeyEvent({ key: 'a', code: 'KeyA' }, BODY, WIRED_MENU_SHORTCUTS).shortcut).toBe('');
    });

    it('sagt, dass ein Kuerzel auch im Eingabefeld gegolten haette', () => {
        const reading = readKeyEvent(alt('w'), INPUT, WIRED_MENU_SHORTCUTS);
        expect(reading.shortcut).toBe('w');
        expect(reading.typingTarget).toBe(true);
        expect(reading.targetTag).toBe('INPUT');
    });

    it('trennt "wir haben es genommen" von "jemand vor uns hat es genommen"', () => {
        const mine: Record<string, unknown> = { ...alt('w'), defaultPrevented: true };
        markHandled(mine, 'w');
        expect(readKeyEvent(mine as never, BODY, WIRED_MENU_SHORTCUTS).consumedBy).toBe('this-window');

        const theirs = { ...alt('w'), defaultPrevented: true };
        expect(readKeyEvent(theirs, BODY, WIRED_MENU_SHORTCUTS).consumedBy).toBe('something-else');

        expect(readKeyEvent(alt('w'), BODY, WIRED_MENU_SHORTCUTS).consumedBy).toBe('nobody');
    });

    it('beantwortet die Frage "war das ein Kuerzel" auch fuer ein verbrauchtes Ereignis', () => {
        // Sonst waere der Test bei genau dem Befund stumm, den er klaeren soll:
        // "die Taste tut nichts" plus "sie war schon verbraucht" ist die
        // Auskunft, ohne die niemand weiterkommt.
        const eaten = { ...alt('b'), defaultPrevented: true };
        expect(readKeyEvent(eaten, BODY, WIRED_MENU_SHORTCUTS).shortcut).toBe('b');
    });

    it('wirft nicht an einem Ereignis ohne Zeichen', () => {
        // Bruecken (Erweiterungen, Automatisierung) schicken solche Ereignisse.
        // Ein Test, der daran stirbt, waere selbst die Stoerung.
        const reading = readKeyEvent(
            { code: 'KeyW', altKey: true } as never,
            BODY,
            WIRED_MENU_SHORTCUTS,
        );
        expect(reading.key).toBe('');
        expect(reading.shortcut).toBe('w');
    });

    it('faengt mit einer leeren Ablesung an, statt eine Taste zu erfinden', () => {
        expect(NO_KEY_READING.code).toBe('');
        expect(NO_KEY_READING.shortcut).toBe('');
        expect(NO_KEY_READING.consumedBy).toBe('nobody');
    });
});

describe('markHandled', () => {

    it('zeichnet genau das Ereignis, das verbraucht wurde, und kein zweites', () => {
        const first: Record<string, unknown> = { key: 'w' };
        const second: Record<string, unknown> = { key: 'w' };
        markHandled(first, 'w');
        expect(handledShortcutOf(first)).toBe('w');
        expect(handledShortcutOf(second)).toBeUndefined();
    });
});
