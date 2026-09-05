/*
 * Wer eine Taste bekommt.
 *
 * Der Befund, gegen den diese Datei geschnitten ist, stammt vom 2026-08-29 und
 * ist an einem gruenen Bildschirm nicht zu sehen: nach dem Laden liegt der Fokus
 * auf BODY, und wer "create" tippt, sah bis dahin nichts in der Kommandozeile
 * ankommen, waehrend das "c" im Hintergrund die Aenderungsansicht aufschlug. Die
 * Regeln dagegen stehen in keyboard.ts; hier stehen sie als Faelle.
 */

import { describe, expect, it } from 'vitest';

import { IMPACT_MENU_LABEL } from '../impact/impact-strings';
import { llmMenuLabel } from '../llm/strings';
import { BUG_WIZARD_MENU_LABEL } from '../traces/bug-wizard-strings';
import { WHY_MENU_LABEL } from '../why/why-model';
import {
    EDITOR_SURFACE,
    FOCUS_COMMAND_KEY,
    RESERVED_BARE_SHORTCUTS,
    commandLineIntent,
    isTypingTarget,
    menuShortcutFor,
} from './keyboard';
import { WIRED_MENU_SHORTCUTS } from './shortcuts';

const BODY = { tagName: 'BODY' };
const alt = (letter: string) => ({ key: letter, code: `Key${letter.toUpperCase()}`, altKey: true });

describe('isTypingTarget', () => {

    it('erkennt die Felder, in denen getippt wird', () => {
        expect(isTypingTarget({ tagName: 'INPUT' })).toBe(true);
        expect(isTypingTarget({ tagName: 'textarea' })).toBe(true);
        expect(isTypingTarget({ tagName: 'SELECT' })).toBe(true);
        expect(isTypingTarget({ tagName: 'DIV', isContentEditable: true })).toBe(true);
    });

    /*
     * Der Editor, und zwar an seiner Flaeche und nicht an seinem inneren
     * Element.
     *
     * Der Kopf von keyboard.ts nahm bis W8 an, Monaco haenge seine Tastatur an
     * ein verstecktes `textarea`, und damit sei der Fall von der Zeile darueber
     * schon erledigt. Seit Monaco 0.56 ist es ein `div` mit der
     * EditContext-API; die Annahme war still falsch geworden, und ein
     * Tastendruck im Editor landete in der Kommandozeile. Gefunden hat es der
     * Beweislauf von W2a (die Datei im Reader wechselte beim Tippen), und
     * gemessen wurde es am Stand VOR W8: der Befund ist aelter als der Umbau.
     */
    it('erkennt die Flaeche des Editors, egal welches Element darin das Ziel ist', () => {
        const inEditor = {
            tagName: 'DIV',
            closest: (selector: string) => (selector === EDITOR_SURFACE ? {} : null),
        };
        expect(isTypingTarget(inEditor)).toBe(true);
        expect(commandLineIntent({ key: 'a' }, inEditor)).toBeUndefined();
    });

    it('haelt alles andere fuer eine Flaeche, an der ein Kuerzel gilt', () => {
        const outside = { tagName: 'BODY', closest: () => null };
        expect(isTypingTarget(outside)).toBe(false);
        expect(isTypingTarget({ tagName: 'BODY' })).toBe(false);
        expect(isTypingTarget({ tagName: 'UL' })).toBe(false);
        expect(isTypingTarget(null)).toBe(false);
        expect(isTypingTarget(undefined)).toBe(false);
    });
});

describe('menuShortcutFor', () => {

    it('gibt den Buchstaben, wenn Alt/Option dabei ist', () => {
        expect(menuShortcutFor(alt('a'), BODY, WIRED_MENU_SHORTCUTS)).toBe('a');
    });

    /*
     * Der Grund fuer `event.code`: unter macOS erzeugt Option+A ein `å`. Ein
     * Kuerzel, das auf `event.key` schaut, waere dort taub, und der Fehler
     * zeigte sich erst auf einer anderen Maschine als der, auf der er entstand.
     */
    it('liest die physische Taste und nicht das Zeichen, das sie erzeugt', () => {
        expect(menuShortcutFor({ key: 'å', code: 'KeyA', altKey: true }, BODY, WIRED_MENU_SHORTCUTS))
            .toBe('a');
    });

    it('gilt auch waehrend jemand tippt: dafuer traegt es den Modifikator', () => {
        expect(menuShortcutFor(alt('a'), { tagName: 'INPUT' }, WIRED_MENU_SHORTCUTS)).toBe('a');
        expect(menuShortcutFor(alt('a'), { tagName: 'TEXTAREA' }, WIRED_MENU_SHORTCUTS)).toBe('a');
    });

    it('macht aus einem blanken Buchstaben kein Kuerzel mehr', () => {
        expect(menuShortcutFor({ key: 'a', code: 'KeyA' }, BODY, WIRED_MENU_SHORTCUTS)).toBeUndefined();
        expect(menuShortcutFor({ key: 'c', code: 'KeyC' }, BODY, WIRED_MENU_SHORTCUTS)).toBeUndefined();
    });

    it('laesst die Kombinationen des Systems in Ruhe', () => {
        expect(menuShortcutFor({ ...alt('a'), metaKey: true }, BODY, WIRED_MENU_SHORTCUTS)).toBeUndefined();
        expect(menuShortcutFor({ ...alt('a'), ctrlKey: true }, BODY, WIRED_MENU_SHORTCUTS)).toBeUndefined();
    });

    it('liest ein verbrauchtes Ereignis nicht ein zweites Mal', () => {
        expect(menuShortcutFor({ ...alt('a'), defaultPrevented: true }, BODY, WIRED_MENU_SHORTCUTS))
            .toBeUndefined();
    });

    it('kennt nur die Buchstaben, die wirklich verdrahtet sind', () => {
        expect(menuShortcutFor(alt('f'), BODY, WIRED_MENU_SHORTCUTS)).toBeUndefined();
        expect(menuShortcutFor({ key: 'Enter' }, BODY, WIRED_MENU_SHORTCUTS)).toBeUndefined();
    });

    it('haelt die Hilfetaste blank, weil sie kein Wort beginnt', () => {
        expect(RESERVED_BARE_SHORTCUTS).toEqual(['?']);
        expect(menuShortcutFor({ key: '?' }, BODY, WIRED_MENU_SHORTCUTS)).toBe('?');
        // In einem Feld ist ein Fragezeichen ein Fragezeichen.
        expect(menuShortcutFor({ key: '?' }, { tagName: 'INPUT' }, WIRED_MENU_SHORTCUTS)).toBeUndefined();
    });
});

describe('commandLineIntent', () => {

    it('schickt jeden blanken Buchstaben in die Zeile', () => {
        for (const letter of [...'create']) {
            expect(commandLineIntent({ key: letter }, BODY)).toEqual({ kind: 'type', text: letter });
        }
    });

    it('holt die Zeile mit der Fokustaste, ohne sie hineinzuschreiben', () => {
        expect(commandLineIntent({ key: FOCUS_COMMAND_KEY }, BODY)).toEqual({ kind: 'focus' });
    });

    it('laesst das Leerzeichen in Ruhe: es bedient Knoepfe und den Baum', () => {
        expect(commandLineIntent({ key: ' ' }, BODY)).toBeUndefined();
    });

    it('nimmt keine Taste, die schon jemandem gehoert', () => {
        expect(commandLineIntent({ key: '?' }, BODY)).toBeUndefined();
        expect(commandLineIntent(alt('a'), BODY)).toBeUndefined();
        expect(commandLineIntent({ key: 'a', metaKey: true }, BODY)).toBeUndefined();
        expect(commandLineIntent({ key: 'a', ctrlKey: true }, BODY)).toBeUndefined();
        expect(commandLineIntent({ key: 'a', defaultPrevented: true }, BODY)).toBeUndefined();
    });

    it('nimmt nichts, was kein Zeichen ist', () => {
        for (const key of ['Enter', 'Escape', 'ArrowDown', 'Tab', 'F5', 'Shift']) {
            expect(commandLineIntent({ key }, BODY), key).toBeUndefined();
        }
    });

    it('mischt sich nicht ein, wo schon getippt wird', () => {
        expect(commandLineIntent({ key: 'a' }, { tagName: 'INPUT' })).toBeUndefined();
        expect(commandLineIntent({ key: 'a' }, { tagName: 'TEXTAREA' })).toBeUndefined();
    });
});

/**
 * Die Naht zwischen dem, was auf einem Menuepunkt STEHT, und dem, was der Griff
 * am Fenster HOERT.
 *
 * Befund 12 des unabhaengigen Audits vom 2026-08-29: die vier Eintraege der
 * Atlas-Zeile trugen keinen Buchstaben, obwohl PLAN Abschnitt 4 verlangt, dass
 * jeder Menuepunkt seinen traegt. Der Fehler, gegen den dieser Block geschnitten
 * ist, ist der naechste: ein Etikett, das `[b]ug hunt` sagt, waehrend `b` nichts
 * tut, ist schlimmer als eines ohne Klammer, weil es eine Bedienung verspricht.
 */
describe('jeder Eintrag der Atlas-Zeile traegt einen Buchstaben, der etwas tut', () => {

    /** Der Buchstabe in der Klammer, oder nichts. */
    const letterOf = (label: string): string | undefined => /\[([a-z])\]/.exec(label)?.[1];

    const entries = [
        { name: 'why', label: WHY_MENU_LABEL, expected: 'w' },
        { name: 'bug', label: BUG_WIZARD_MENU_LABEL, expected: 'b' },
        { name: 'impact', label: IMPACT_MENU_LABEL, expected: 'c' },
        { name: 'llm off', label: llmMenuLabel('off'), expected: 'l' },
        { name: 'llm on', label: llmMenuLabel('ready'), expected: 'l' },
    ];

    for (const entry of entries) {
        it(`schreibt den Buchstaben von ${entry.name} in sein Etikett`, () => {
            expect(letterOf(entry.label), entry.label).toBe(entry.expected);
        });

        it(`laesst den Buchstaben von ${entry.name} auch wirklich gelten`, () => {
            const letter = letterOf(entry.label) ?? '';
            expect(WIRED_MENU_SHORTCUTS, `${entry.label} verspricht ${letter}`).toContain(letter);
            expect(menuShortcutFor(alt(letter), BODY, WIRED_MENU_SHORTCUTS)).toBe(letter);
        });
    }

    /*
     * Die Gegenprobe zur obersten Zeile. Sie trug bis zum 2026-08-29 vier
     * Punkte ohne Verdrahtung; seit W7a gibt es sie nicht mehr, und ihre
     * Buchstaben duerfen auch nicht im Vorbeigehen wieder belegt werden.
     */
    it('belegt keinen Buchstaben der geloeschten Attrappen', () => {
        for (const letter of ['f', 'e', 'v', 't']) {
            expect(WIRED_MENU_SHORTCUTS, `${letter} gehoerte einer Attrappe`).not.toContain(letter);
        }
        expect(WIRED_MENU_SHORTCUTS).toContain('a');
    });
});
