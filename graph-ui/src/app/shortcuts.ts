/** Shared menu and workspace shortcut catalog used by keyboard handling and help. */
import { playerIntent } from '../tours/tour-player';
import { RESERVED_BARE_SHORTCUTS } from './keyboard';

/**
 * Wo eine Taste gilt.
 *
 * Vier Bereiche, und der Unterschied zwischen den ersten beiden ist genau der,
 * den ein Leser wissen muss: ein `mnemonic` traegt Alt/Option und gilt auch
 * waehrend des Tippens, eine `bare` Taste gilt nur, solange nirgends getippt
 * wird. Ein Bereich mehr ist billiger als eine Tabelle, die beides gleich
 * aussehen laesst.
 */
import { experimentalAgentsEnabled } from './feature-flags';

export type ShortcutScope = 'mnemonic' | 'bare' | 'walk' | 'galaxy';

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
 * BUG-Assistenten, `l` den Schalter des lokalen
 * Modells, `r` seit W8 den Weg zurueck zum Vorgabe-Layout, `s` seit W10 das
 * Einstellungen-Panel, `g` seit W11a den Live-Modus der Agenten, `p` das
 * Dialog zum Anlegen eines Projektindexes.
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
export const WIRED_MENU_SHORTCUTS: readonly string[] = ['a', 'w', 'b', 'l', 'r', 's', 'g', 'p', '?']
    .filter(key => experimentalAgentsEnabled || key !== 'g');

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
 * sie zeigt: Menuekuerzel, Galaxy-Suche, dann der Walk.
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
    { scope: 'galaxy', key: 'Cmd/Ctrl+K' },
    ...probed('walk', playerIntent),
];

/** Der Schluessel, unter dem der Katalog den Satz zu dieser Taste fuehrt. */
export function shortcutId(shortcut: AtlasShortcut): string {
    return `${shortcut.scope}:${shortcut.key}`;
}
