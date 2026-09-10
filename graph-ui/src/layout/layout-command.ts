/**
 * Die Befehle, die die Kommandozeile ausser Suchen und Fragen kennt.
 *
 * ## Warum es sie gibt
 *
 * AC3 des W8-Contracts verlangt einen Weg zurueck, der in EINEM Schritt geht:
 * wer sich mit vier Griffen ein Layout gebaut hat, das er nicht mehr mag, soll
 * nicht vier Doppelklicks suchen muessen. Seit W10 steht `settings` daneben, aus
 * demselben Grund: das Einstellungen-Panel ist die Stelle, an der ein Leser mit
 * einer schwachen Maschine die teuren Effekte abschaltet, und es ueber ein
 * Menuekuerzel allein erreichbar zu machen hiesse, es hinter einer Taste zu
 * verstecken, die man kennen muss. Seit W11a steht `live agents` daneben, und dort ist der Grund noch
 * schaerfer: der Live-Modus ist per Vorgabe AUS, und ein Modus, den man nur
 * ueber ein Menuekuerzel findet, ist fuer den, der das Kuerzel nicht kennt, gar
 * nicht da. Alle drei gibt es darum zweimal, im Menue und in der Zeile, und
 * beide Wege rufen dieselbe Funktion.
 *
 * ## Warum die Erkennung hier steht und nicht in commandIntent
 *
 * `src/chat/command-intent.ts` beantwortet eine andere Frage, naemlich ob eine
 * Zeile eine Suche oder eine Frage an das Modell ist. Ein Befehl ist keins von
 * beidem, und ihn dort einzubauen hiesse, die Prioritaetsregel jenes Moduls
 * anzufassen ("die Suche behaelt den Vortritt"), die auf einem Nutzerbefund
 * beruht. Er wird darum VOR jener Frage geprueft, an einer eigenen Stelle, die
 * genau drei Zeilen kennt.
 *
 * Drei Befehle an einer Stelle und nicht an dreien: die Frage "ist diese Zeile
 * ein Befehl" wird einmal beantwortet, mit einer Normalisierung und einem
 * Vergleich. Drei Module mit je einer Zeile waeren drei Normalisierungen, die
 * auseinanderlaufen koennen.
 *
 * ## Warum die Erkennung so eng ist
 *
 * Nur `reset layout`, `settings` und `live agents`, ohne Gross-/Kleinschreibung und ohne
 * doppelte Leerzeichen. Kein Praefix, kein Teilwort, keine Abkuerzung. Der Grund
 * ist derselbe wie beim Fragezeichen in command-intent.ts: diese Zeile sucht
 * auch, und jede Unschaerfe hier ist eine Suche, die stattdessen das Layout
 * umbaut oder ein Panel aufschlaegt. "reset" allein ist ein Wort, nach dem
 * jemand suchen wird; "reset layout" ist keins.
 *
 * `settings` ist ein einzelnes Wort und damit der engere Fall von beiden. Es ist
 * trotzdem vertretbar, und der Grund ist messbar: dieses Produkt sucht Symbole
 * eines indizierten Repositories, und ein Symbol heisst `getSettings`,
 * `SettingsPanel` oder `settingsStore`, aber nicht `settings` allein; die Suche
 * greift ausserdem ab zwei Zeichen und zeigt ihre Treffer, waehrend getippt
 * wird, also sieht ein Leser, der wirklich sucht, seine Treffer schon vor dem
 * achten Buchstaben. Wer die Zeile trotzdem abschickt, bekommt das Panel und
 * schliesst es mit Escape.
 */

/** Was diese Zeile ist, soweit dieses Modul es beurteilt. */
export type LineCommand =
    | 'reset-layout'
    | 'open-settings'
    | 'toggle-live-agents'
    | 'toggle-fullscreen'
    | 'none';

/** Die Zeile, die das Layout zuruecksetzt. Woertlich, damit die Hilfe sie zeigen kann. */
export const RESET_LAYOUT_COMMAND = 'reset layout';

/** Die Zeile, die das Einstellungen-Panel aufschlaegt. */
export const SETTINGS_COMMAND = 'settings';

/**
 * Die Zeile, die den Live-Modus der Agenten umlegt (W11a).
 *
 * Zwei Woerter und nicht eines, aus demselben Grund wie bei "reset layout":
 * "agents" allein waere ein Wort, nach dem jemand sucht, "live agents" ist
 * keins. Sie SCHALTET und oeffnet nicht nur, weil der Modus zwei Lagen hat und
 * eine Zeile, die nur einschalten kann, den Weg zurueck an das Menue abgibt.
 */
export const LIVE_AGENTS_COMMAND = 'live agents';

/**
 * Die Zeile, die das Vollbild des Graphen umlegt (W11b).
 *
 * Sie heisst `fullscreen` und nicht `cinema`, und das ist keine Kosmetik: der
 * Nutzer hat den Modus am 2026-08-30 benutzt und woertlich gesagt, "bin jetzt
 * im cinema mode, sollte fullscreen heissen". Ein Schalter, der in der
 * Oberflaeche anders heisst als in der Zeile, waere zwei Namen fuer eine Sache.
 *
 * Ein einzelnes Wort, also der engere Fall, dieselbe Abwaegung wie bei
 * `settings`: ein Symbol heisst `toggleFullscreen` oder `FullscreenButton`,
 * aber nicht `fullscreen` allein, und wer wirklich sucht, sieht seine Treffer
 * schon beim Tippen. Der Modus haengt am Live-Modus der Agenten (er ist der
 * Rahmen JENER Ansicht); ist der aus, schaltet diese Zeile ihn mit an, statt
 * still nichts zu tun.
 */
export const FULLSCREEN_COMMAND = 'fullscreen';

/** Ob diese Zeile einer der vier Befehle ist. */
export function lineCommandOf(line: string): LineCommand {
    const normalised = line.trim().toLowerCase().replace(/\s+/g, ' ');
    if (normalised === RESET_LAYOUT_COMMAND) {
        return 'reset-layout';
    }
    if (normalised === SETTINGS_COMMAND) {
        return 'open-settings';
    }
    if (normalised === FULLSCREEN_COMMAND) {
        return 'toggle-fullscreen';
    }
    return normalised === LIVE_AGENTS_COMMAND ? 'toggle-live-agents' : 'none';
}
