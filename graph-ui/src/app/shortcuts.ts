/**
 * Welche Tasten in dieser Oberflaeche etwas tun, an einer Stelle.
 *
 * Bis zum 2026-08-29 stand die Liste der Menuebuchstaben in App.tsx, direkt
 * neben den Handlungen, die sie ausloest. Das war richtig, solange sie nur
 * einen Leser hatte. Seit die Hilfe (src/help/HelpOverlay.tsx) eine Tabelle
 * aller Tastenkuerzel zeigt, hat sie zwei, und ein zweiter Ort fuer dieselbe
 * Liste waere genau der Fehler, gegen den die Hilfe geschrieben ist: eine
 * Tabelle, die eine Taste verspricht, die nichts mehr tut.
 *
 * ## Warum die drei Bereiche verschieden entstehen
 *
 * Die Menuebuchstaben stehen hier als Liste, weil sie hier entstehen: der Griff
 * am Fenster in App.tsx liest genau diese Liste, und was nicht darin steht,
 * wird nicht gehoert.
 *
 * Die Tasten des Walks und des Suchfensters stehen NICHT als Liste da. Sie
 * werden erfragt: `playerIntent` und `overlayIntent` sind die Funktionen, die
 * entscheiden, was eine Taste dort bedeutet, und dieses Modul probiert sie
 * gegen ein Alphabet moeglicher Tasten durch. Damit ist die Hilfe kein zweiter
 * Katalog, sondern eine Ablesung: verschwindet ein `case` aus einer der beiden
 * Funktionen, verschwindet die Zeile aus der Hilfe, ohne dass jemand daran
 * denken muss.
 */

import { overlayIntent } from '../search/overlay-model';
import { playerIntent } from '../tours/tour-player';
import { FOCUS_COMMAND_KEY, RESERVED_BARE_SHORTCUTS } from './keyboard';

/**
 * Wo eine Taste gilt.
 *
 * Fuenf Bereiche, und der Unterschied zwischen den ersten beiden ist genau der,
 * den ein Leser wissen muss: ein `mnemonic` traegt Alt/Option und gilt auch
 * waehrend des Tippens, eine `bare` Taste gilt nur, solange nirgends getippt
 * wird. Ein Bereich mehr ist billiger als eine Tabelle, die beides gleich
 * aussehen laesst.
 */
export type ShortcutScope = 'mnemonic' | 'bare' | 'line' | 'walk' | 'search';

/** Eine Taste, die etwas tut, und der Bereich, in dem sie es tut. */
export interface AtlasShortcut {
    readonly scope: ShortcutScope;
    /** Der Wert von `KeyboardEvent.key`, kleingeschrieben, wo es ein Buchstabe ist. */
    readonly key: string;
}

/**
 * Die Buchstaben, die wirklich etwas tun.
 *
 * `a` klappt die Galaxie auf und zu; die Eintraege der Atlas-Zeile tragen seit
 * dem 2026-08-29 ihre eigenen: `w` die Frage nach dem Warum, `b` den
 * BUG-Assistenten, `c` die Aenderungsansicht, `l` den Schalter des lokalen
 * Modells, `r` seit W8 den Weg zurueck zum Vorgabe-Layout, `s` seit W10 das
 * Einstellungen-Panel, `g` seit W11a den Live-Modus der Agenten, `p` das
 * Projekte-Panel (Index anlegen, entfernen, Entscheidungsakte, Serverzustand).
 * `?` schlaegt seit W7a die Hilfe auf und wieder zu.
 *
 * Warum das vorher nicht so war und warum es jetzt so ist: die Zeile war ein
 * Menue, dessen Eintraege nur mit der Maus erreichbar waren, in einer
 * Oberflaeche, deren ganzes Vorbild die Tastatur ist (PLAN Abschnitt 4, "jeder
 * Menuepunkt traegt seinen Shortcut"). Das unabhaengige Audit hat es als
 * Befund 12 aufgeschrieben.
 *
 * Die Gegenrichtung ist seit W7a die schaerfere Zusicherung: die Menuezeile
 * traegt keinen Punkt mehr, der hier NICHT steht. Geprueft wird das strukturell
 * ueber `messages.menu.items` (src/app/shortcuts.test.ts) und nicht ueber eine
 * gepflegte Liste, denn eine gepflegte Liste ist genau die Stelle, an der ein
 * Punkt ohne Verdrahtung wieder hereinrutscht.
 */
export const WIRED_MENU_SHORTCUTS: readonly string[] = ['a', 'w', 'b', 'c', 'l', 'r', 's', 'g', 'p', '?'];

/**
 * Das Alphabet, gegen das die beiden Absichtsfunktionen befragt werden.
 *
 * Absichtlich grosszuegig: es kostet nichts, und eine Taste, die eine der
 * Funktionen kuenftig belegt, faellt der Hilfe von selbst zu, statt vergessen
 * zu werden. Grossbuchstaben stehen nicht darin, weil beide Funktionen sie auf
 * denselben Sinn abbilden wie den kleinen und die Hilfe die Taste und nicht die
 * Schreibweise nennt.
 */
export const PROBED_KEYS: readonly string[] = [
    ...'abcdefghijklmnopqrstuvwxyz0123456789',
    '?', '/', '.', ',', '-', ' ',
    'Enter', 'Escape', 'Tab', 'Backspace', 'Delete',
    'ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight',
    'Home', 'End', 'PageUp', 'PageDown',
];

/** Die Tasten eines Bereichs, erfragt statt aufgeschrieben. */
function probed(scope: ShortcutScope, meaning: (key: string) => string): AtlasShortcut[] {
    return PROBED_KEYS.filter((key) => meaning(key) !== 'none').map((key) => ({ scope, key }));
}

/** Ob diese Taste Alt/Option braucht, um zu gelten. */
export function needsAlt(shortcut: AtlasShortcut): boolean {
    return shortcut.scope === 'mnemonic';
}

/**
 * Jede Taste, die diese Oberflaeche hoert, in der Reihenfolge, in der die Hilfe
 * sie zeigt: erst die Menuekuerzel, dann die Kommandozeile, dann der Walk, dann
 * das Suchfenster.
 *
 * Auch die Aufteilung in `mnemonic` und `bare` wird abgelesen und nicht
 * gepflegt: welche Taste ohne Alt/Option gilt, weiss keyboard.ts, und diese
 * Liste fragt dort nach.
 */
export const ATLAS_SHORTCUTS: readonly AtlasShortcut[] = [
    ...WIRED_MENU_SHORTCUTS.map((key) => ({
        scope: (RESERVED_BARE_SHORTCUTS.includes(key) ? 'bare' : 'mnemonic') as ShortcutScope,
        key,
    })),
    { scope: 'line', key: FOCUS_COMMAND_KEY },
    ...probed('walk', playerIntent),
    ...probed('search', overlayIntent),
];

/** Der Schluessel, unter dem der Katalog den Satz zu dieser Taste fuehrt. */
export function shortcutId(shortcut: AtlasShortcut): string {
    return `${shortcut.scope}:${shortcut.key}`;
}
