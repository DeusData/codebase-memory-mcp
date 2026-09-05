/**
 * Die Naht zwischen dem, was in der Menuezeile STEHT, und dem, was die Tastatur
 * wirklich hoert.
 *
 * Nutzerauftrag vom 2026-08-29: die Zeile trug `[f]ile [e]dit [v]iew
 * [t]erminal`, vier Punkte, hinter denen nichts lag, mit einem Tooltip als
 * Fussnote. Diese Datei haelt fest, dass das nicht wiederkommen kann, und zwar
 * strukturell: geprueft wird nicht eine Liste erlaubter Punkte (die man
 * erweitern kann, ohne etwas zu verdrahten), sondern die Beziehung zwischen den
 * beiden Listen, die es wirklich gibt.
 */

import { describe, expect, it } from 'vitest';

import { messages } from '../i18n/messages';
import { overlayIntent } from '../search/overlay-model';
import { playerIntent } from '../tours/tour-player';
import { FOCUS_COMMAND_KEY, RESERVED_BARE_SHORTCUTS, menuShortcutFor } from './keyboard';
import { ATLAS_SHORTCUTS, PROBED_KEYS, WIRED_MENU_SHORTCUTS, needsAlt, shortcutId } from './shortcuts';

describe('die Menuezeile traegt nur, was etwas tut', () => {

    it('gibt jedem Menuepunkt einen Buchstaben, der verdrahtet ist', () => {
        for (const item of messages.menu.items) {
            expect(WIRED_MENU_SHORTCUTS, `[${item.key}]${item.rest} ist nicht verdrahtet`)
                .toContain(item.key);
        }
    });

    it('traegt ueberhaupt Punkte, damit die Regel oben nicht leer laeuft', () => {
        expect(messages.menu.items.length).toBeGreaterThanOrEqual(2);
        expect(messages.menu.items.map((item) => item.key)).toEqual(['a', '?']);
    });

    it('hat die vier Attrappen des Vorbilds nicht mehr', () => {
        const keys = messages.menu.items.map((item) => item.key);
        for (const gone of ['f', 'e', 'v', 't']) {
            expect(keys, `[${gone}] war ein Punkt ohne Verdrahtung`).not.toContain(gone);
        }
    });

    it('kennt keinen Tooltip mehr fuer einen Punkt, der nichts tut', () => {
        expect(Object.keys(messages.menu)).not.toContain('notWired');
    });

    it('vergibt keinen Buchstaben zweimal', () => {
        expect(new Set(WIRED_MENU_SHORTCUTS).size).toBe(WIRED_MENU_SHORTCUTS.length);
    });

    it('laesst jedes Kuerzel mit Alt gelten, auch waehrend jemand tippt', () => {
        for (const key of WIRED_MENU_SHORTCUTS.filter((entry) => !RESERVED_BARE_SHORTCUTS.includes(entry))) {
            const event = { key, code: `Key${key.toUpperCase()}`, altKey: true };
            expect(menuShortcutFor(event, { tagName: 'BODY' }, WIRED_MENU_SHORTCUTS)).toBe(key);
            expect(menuShortcutFor(event, { tagName: 'INPUT' }, WIRED_MENU_SHORTCUTS)).toBe(key);
        }
    });

    /*
     * Die Kehrseite, und der eigentliche Nutzerbefund vom 2026-08-29: ein
     * blanker Buchstabe ist Text. Er darf kein Panel oeffnen, weder hier noch
     * irgendwo sonst, weil das Wort "create" sonst ueber sein "c" die
     * Aenderungsansicht aufschlaegt, waehrend der Leser auf die leere
     * Kommandozeile schaut.
     */
    it('macht aus keinem blanken Buchstaben ein Kuerzel', () => {
        for (const key of WIRED_MENU_SHORTCUTS.filter((entry) => !RESERVED_BARE_SHORTCUTS.includes(entry))) {
            const event = { key, code: `Key${key.toUpperCase()}` };
            expect(menuShortcutFor(event, { tagName: 'BODY' }, WIRED_MENU_SHORTCUTS)).toBeUndefined();
        }
    });

    it('haelt die reservierte Hilfetaste blank, aber nur ausserhalb eines Feldes', () => {
        for (const key of RESERVED_BARE_SHORTCUTS) {
            expect(menuShortcutFor({ key }, { tagName: 'BODY' }, WIRED_MENU_SHORTCUTS)).toBe(key);
            expect(menuShortcutFor({ key }, { tagName: 'INPUT' }, WIRED_MENU_SHORTCUTS)).toBeUndefined();
        }
    });
});

describe('die Tastenliste der Hilfe ist die Verdrahtung selbst', () => {

    it('liest die Tasten des Walks aus playerIntent statt sie aufzuschreiben', () => {
        const walk = ATLAS_SHORTCUTS.filter((entry) => entry.scope === 'walk').map((entry) => entry.key);
        expect(walk.length).toBeGreaterThanOrEqual(4);
        for (const key of PROBED_KEYS) {
            const wired = playerIntent(key) !== 'none';
            expect(walk.includes(key), `${key}: Verdrahtung ${wired}, Hilfe ${walk.includes(key)}`)
                .toBe(wired);
        }
    });

    it('liest die Tasten des Suchfensters aus overlayIntent', () => {
        const search = ATLAS_SHORTCUTS.filter((entry) => entry.scope === 'search').map((entry) => entry.key);
        for (const key of PROBED_KEYS) {
            const wired = overlayIntent(key) !== 'none';
            expect(search.includes(key), `${key}: Verdrahtung ${wired}, Hilfe ${search.includes(key)}`)
                .toBe(wired);
        }
    });

    it('nimmt die Menuetasten aus genau der Liste, die das Fenster hoert', () => {
        const menu = ATLAS_SHORTCUTS
            .filter((entry) => entry.scope === 'mnemonic' || entry.scope === 'bare')
            .map((entry) => entry.key);
        expect(menu).toEqual([...WIRED_MENU_SHORTCUTS]);
    });

    it('trennt die Kuerzel mit Alt von der einen, die ohne gilt', () => {
        const bare = ATLAS_SHORTCUTS.filter((entry) => entry.scope === 'bare').map((entry) => entry.key);
        expect(bare).toEqual([...RESERVED_BARE_SHORTCUTS]);
        for (const entry of ATLAS_SHORTCUTS.filter((item) => item.scope === 'mnemonic')) {
            expect(needsAlt(entry)).toBe(true);
        }
    });

    it('nennt die Taste, die die Kommandozeile holt', () => {
        const line = ATLAS_SHORTCUTS.filter((entry) => entry.scope === 'line').map((entry) => entry.key);
        expect(line).toEqual([FOCUS_COMMAND_KEY]);
    });

    it('sagt zu jeder Taste, was sie tut, und zu keiner anderen', () => {
        const documented = Object.keys(messages.help.shortcutDoes).sort();
        const shown = ATLAS_SHORTCUTS.map(shortcutId).sort();
        expect(shown).toEqual(documented);
        for (const id of shown) {
            expect(messages.help.shortcutDoes[id]?.trim().length, `${id} hat keinen Satz`)
                .toBeGreaterThan(0);
        }
    });

    it('schreibt jede Taste so, wie sie auf einer Tastatur heisst', () => {
        for (const shortcut of ATLAS_SHORTCUTS) {
            const label = messages.help.keyNames[shortcut.key] ?? shortcut.key;
            expect(label.trim().length).toBeGreaterThan(0);
            // Kein DOM-Name in der Hilfe: `ArrowUp` steht auf keiner Taste.
            expect(/^(Arrow|Page)/.test(label)).toBe(false);
        }
    });
});
