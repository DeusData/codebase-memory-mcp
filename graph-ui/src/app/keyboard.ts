/**
 * Wer eine Taste bekommt: die Kommandozeile, ein Menuekuerzel, oder niemand.
 *
 * ## Der Befund, gegen den diese Datei geschnitten ist (2026-08-29)
 *
 * Gemessen an der laufenden Vorschau: nach dem Laden liegt der Fokus auf `BODY`.
 * Wer dann lostippt, sieht in der Kommandozeile NICHTS ankommen, und im
 * Hintergrund gehen Panels auf, weil die blanken Buchstaben Menuekuerzel waren:
 * das Wort "create" oeffnete ueber sein `c` die Aenderungsansicht. Aus
 * Nutzersicht ist das "ich tippe und es passiert nichts", waehrend in Wahrheit
 * etwas passiert, das niemand wollte. Beides zusammen ist die schlimmste Sorte
 * Fehler: die Oberflaeche tut etwas anderes als das, was der Leser sieht.
 *
 * ## Die Regel, und warum diese und nicht die andere
 *
 * Der Contract stellte zwei Wege zur Wahl: (a) die Menuekuerzel auf
 * Alt/Option umstellen, oder (b) type-to-search, bei dem blanke Buchstaben in
 * die Zeile fallen und die Panel-Kuerzel eine Bestaetigung brauchen. Gewaehlt
 * ist (a) ALS TRAEGER und (b) als notwendige Ergaenzung, und das ist keine
 * Verwaesserung der Wahl, sondern ihr Ergebnis: (a) allein loest nur die zweite
 * Haelfte des Befundes. Waeren die Buchstaben nur frei, wuerde Tippen ohne Klick
 * weiterhin nichts tun, und genau das ist der Satz, den der Nutzer geschrieben
 * hat. Und sobald blanke Buchstaben in die Zeile fallen, MUSS ein Kuerzel einen
 * Modifikator tragen, sonst waere es unerreichbar, sobald die Zeile den Fokus
 * hat. Die beiden Wege sind also nicht Alternativen, sondern die zwei Haelften
 * derselben Loesung; die Wahl liegt darin, welcher Modifikator es ist, und das
 * ist Alt/Option, weil genau das die Konvention einer Menueleiste ist.
 *
 * Also gilt, an einer Stelle und deshalb pruefbar:
 *
 *  1. **Alt/Option + Buchstabe ist ein Menuekuerzel**, ueberall, auch waehrend
 *     jemand tippt. Gelesen wird `event.code` und nicht `event.key`: unter macOS
 *     erzeugt Option+A ein `å`, und ein Kuerzel, das auf den Buchstaben schaut,
 *     waere dort taub.
 *  2. **`?` bleibt ohne Modifikator die Hilfe**, solange nirgends getippt wird.
 *     Es ist die eine Ausnahme, und sie ist begruendet: `?` ist seit
 *     Jahrzehnten die Hilfetaste, es beginnt kein Wort, das jemand sucht, und
 *     sobald die Zeile den Fokus hat, tippt es sich ganz normal.
 *  3. **`/` holt die Kommandozeile**, ohne selbst in ihr zu landen. Dieselbe
 *     Konvention wie anderswo, und die Hilfe nennt sie.
 *  4. **Jedes andere druckbare Zeichen gehoert der Kommandozeile.** Die
 *     Oberflaeche holt sie sich und schreibt das Zeichen hinein.
 *  5. **Das Leerzeichen nicht.** Es aktiviert einen Knopf und klappt eine
 *     Baumzeile auf; es zu stehlen hiesse, die Bedienung mit der Tastatur
 *     ueberall sonst kaputtzumachen, und keine Suche beginnt mit einem
 *     Leerzeichen.
 *
 * Was WEITER gilt und hier nicht steht: die vier Tasten einer laufenden
 * Fuehrung (tour-player.ts) bleiben blank. Ihre Karte steht auf dem Bildschirm
 * und schreibt sie hin; wer eine Fuehrung laufen laesst, sieht `[q] exit` und
 * `[d] diagram` vor sich. Die Oberflaeche laesst der Fuehrung ihre vier Tasten
 * und schickt alles andere in die Zeile (siehe App.tsx, dort steht die
 * Reihenfolge).
 */

/** Nur das, was diese Pruefung von einem Ereignisziel braucht. */
export interface KeyTargetLike {
    tagName?: string;
    isContentEditable?: boolean;
    /**
     * Der naechste Vorfahr, der zu diesem Selektor passt.
     *
     * Optional, damit ein Test ein Ziel als schlichtes Objekt hinstellen kann;
     * im Browser bringt jedes Element die Funktion mit. Gebraucht wird sie fuer
     * genau eine Frage, und die steht bei {@link EDITOR_SURFACE}.
     */
    closest?: (selector: string) => unknown;
}

/**
 * Die eine fremde Klasse, die diese Datei kennt, und warum sie sie kennen muss.
 *
 * Der Kopf dieser Datei sagt seit W7b: "Monaco faellt darunter, ohne dass es
 * hier erwaehnt werden muesste: der Editor haengt seine Tastatur an ein
 * verstecktes `textarea`". Das stimmte, als der Satz geschrieben wurde, und
 * stimmt seit Monaco 0.56 nicht mehr: der Editor haengt sie an ein `div` mit
 * der EditContext-API. Damit war die Regel taub, wo sie am wichtigsten ist, und
 * das war an einem gruenen Testlauf nicht zu sehen, aber im Beweislauf von W2a:
 * ein Tastendruck IM Editor landete in der Kommandozeile, und das darauf
 * folgende Enter oeffnete den ersten Suchtreffer. Gemessen am Stand vor W8
 * (git archive HEAD, eigener Bau, derselbe Lauf): `lengthBefore 1437,
 * lengthAfter 637`, also eine andere Datei im Reader. Der Befund ist damit
 * aelter als dieser Zyklus und wird hier repariert, weil er sonst eine
 * Zusicherung aus W2a stillschweigend verfallen liesse.
 *
 * Warum eine Klasse einer fremden Bibliothek und nicht ein eigenes Attribut:
 * das Ziel eines Tastendrucks ist ein Element, das Monaco selbst erzeugt und
 * austauscht, und ein Attribut, das dieses Produkt daran haengt, waere ein
 * Attribut, das die naechste Fassung der Bibliothek wieder wegwirft. Die Klasse
 * ist der einzige Anker, den Monaco ueber alle seine inneren Umbauten hinweg
 * behaelt. Dieselbe Ueberlegung, aus demselben Grund, steht in
 * tools/lib/readability.mjs, wo der Editor an genau dieser Klasse von der
 * Messung ausgenommen ist.
 */
export const EDITOR_SURFACE = '.monaco-editor';

/** Nur das, was diese Pruefung von einem Tastenereignis braucht. */
export interface KeyEventLike {
    key: string;
    /** Die physische Taste. Sie traegt unter macOS auch dann `KeyA`, wenn `key` `å` sagt. */
    code?: string;
    metaKey?: boolean;
    ctrlKey?: boolean;
    altKey?: boolean;
    shiftKey?: boolean;
    defaultPrevented?: boolean;
}

/**
 * Die eine druckbare Taste, die ohne Modifikator ein Kuerzel bleibt.
 *
 * Als Liste und nicht als Vergleich mit `'?'`, damit die Ausnahme zaehlbar ist:
 * eine zweite Taste hier hinein zu schreiben, ist eine Entscheidung, die man
 * sieht.
 */
export const RESERVED_BARE_SHORTCUTS: readonly string[] = ['?'];

/** Die Taste, die die Kommandozeile holt, ohne selbst in ihr zu landen. */
export const FOCUS_COMMAND_KEY = '/';

/**
 * Wie der Griff am Fenster haengt: in der EINFANGENDEN Phase.
 *
 * Seit dem 2026-08-29, und der Grund ist ein Nutzerbefund, der sich nicht
 * nachstellen liess ("alt plus letter funktioniert nur fuer atlas"). Er ist an
 * der laufenden Vorschau nicht aufgetreten, also wird hier nicht an einer
 * Ursache geraten, sondern die eine Klasse von Ursachen ausgeschlossen, die
 * eine Anwendung selbst in der Hand hat: ein Zwischenhaendler auf dem Weg nach
 * oben. In der aufsteigenden Phase sieht das Fenster einen Tastendruck als
 * LETZTER; jeder Griff an einem Element dazwischen (Monaco, ein Panel, eine
 * Bibliothek) kann ihn vorher abbestellen oder das Aufsteigen abbrechen, und
 * dann kommt das Kuerzel nie an. In der einfangenden Phase sieht das Fenster
 * ihn als ERSTER, und keiner dieser Wege kann ihm zuvorkommen.
 *
 * Als Konstante und nicht als literales `true` an der Aufrufstelle, weil
 * `removeEventListener` dieselbe Angabe braucht: ein Griff, der mit `capture`
 * angemeldet und ohne abgemeldet wird, bleibt haengen, und das ist ein Fehler,
 * den man erst nach dem zweiten Aufhaengen sieht.
 */
export const KEY_LISTENER_OPTIONS = { capture: true } as const;

/**
 * Ob an diesem Ziel gerade getippt wird.
 *
 * Drei Wege hinein: ein Feld, das man beschreiben kann, ein editierbarer
 * Bereich, und die Flaeche des Editors. Der dritte steht ausdruecklich da und
 * nicht nur als Folge des ersten; der Grund steht bei {@link EDITOR_SURFACE}.
 */
export function isTypingTarget(target: KeyTargetLike | null | undefined): boolean {
    if (target === null || target === undefined) {
        return false;
    }
    if (target.isContentEditable === true) {
        return true;
    }
    const tag = (target.tagName ?? '').toUpperCase();
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') {
        return true;
    }
    return target.closest?.(EDITOR_SURFACE) != null;
}

/** Der Buchstabe hinter einer physischen Taste, oder nichts. */
function letterOfCode(code: string): string | undefined {
    const match = /^Key([A-Z])$/.exec(code);
    return match === null ? undefined : match[1].toLowerCase();
}

/**
 * Das Menuekuerzel eines Tastendrucks, oder nichts.
 *
 * Zwei Wege hinein, und beide sind oben begruendet: Alt/Option + Buchstabe
 * ueberall, und die reservierte blanke Taste, solange nirgends getippt wird.
 */
export function menuShortcutFor(
    event: KeyEventLike,
    target: KeyTargetLike | null | undefined,
    shortcuts: readonly string[],
): string | undefined {
    if (event.defaultPrevented === true) {
        return undefined;
    }
    if (event.metaKey === true || event.ctrlKey === true) {
        return undefined;
    }
    if (event.altKey === true) {
        const letter = letterOfCode(event.code ?? '');
        return letter !== undefined && shortcuts.includes(letter) ? letter : undefined;
    }
    if (isTypingTarget(target)) {
        return undefined;
    }
    // `?? ''` und nicht `event.key.toLowerCase()`: ein Ereignis ohne `key` ist
    // im Browser nicht vorgesehen und kommt aus Bruecken (Erweiterungen,
    // Automatisierung, synthetische Ereignisse) trotzdem an. Ein Griff, der
    // daran wirft, verschluckt die Taste UND alle danach, und das saehe von
    // aussen genau so aus wie der Nutzerbefund vom 2026-08-29.
    const key = (event.key ?? '').toLowerCase();
    return RESERVED_BARE_SHORTCUTS.includes(key) && shortcuts.includes(key) ? key : undefined;
}

/** Was ein blanker Tastendruck fuer die Kommandozeile bedeutet. */
export type CommandLineIntent =
    /** Die Zeile holen, ohne etwas hineinzuschreiben. */
    | { kind: 'focus' }
    /** Die Zeile holen und dieses Zeichen anhaengen. */
    | { kind: 'type'; text: string };

/**
 * Ob dieser Tastendruck der Kommandozeile gehoert, und was sie damit tut.
 *
 * Sie bekommt ihn nur, wenn sonst niemand ihn hat: kein Modifikator, kein
 * Eingabefeld unter dem Zeiger, kein reserviertes Kuerzel, und ein Zeichen, das
 * man wirklich sieht. Alles andere (Enter, Escape, die Pfeile, F-Tasten) traegt
 * einen Namen mit mehr als einem Zeichen und faellt hier von selbst heraus.
 */
export function commandLineIntent(
    event: KeyEventLike,
    target: KeyTargetLike | null | undefined,
): CommandLineIntent | undefined {
    if (event.defaultPrevented === true) {
        return undefined;
    }
    if (event.metaKey === true || event.ctrlKey === true || event.altKey === true) {
        return undefined;
    }
    if (isTypingTarget(target)) {
        return undefined;
    }
    const key = event.key ?? '';
    if (key.length !== 1 || key === ' ') {
        return undefined;
    }
    if (RESERVED_BARE_SHORTCUTS.includes(key.toLowerCase())) {
        return undefined;
    }
    return key === FOCUS_COMMAND_KEY ? { kind: 'focus' } : { kind: 'type', text: key };
}

/**
 * Ob eine Taste waehrend einer laufenden Fuehrung die Fuehrung meint.
 *
 * Fast dieselbe Frage wie oben und mit einem bewussten Unterschied: ein
 * `textarea` zaehlt hier NICHT als Tippen. Monaco haengt seine Tastatur an ein
 * verstecktes `textarea`, und der Reader dieses Projekts ist read-only, also
 * gibt es dort nichts zu tippen, was ein Enter verbrauchen koennte. Wer im
 * Editor steht und Enter drueckt, meint den naechsten Schritt; wuerde die
 * Fuehrung dort schweigen, muesste der Leser vor jedem Schritt erst aus dem
 * Editor herausklicken.
 *
 * Ein echtes Eingabefeld ist die Ausnahme, die die Regel traegt: in der
 * Kommandozeile und im Suchfeld des Einstiegsdialogs ist Enter das Absenden
 * der Eingabe, und ein `q` ist der Buchstabe q.
 */
export function tourKeyForEvent(
    event: KeyEventLike,
    target: KeyTargetLike | null | undefined,
): string | undefined {
    if (event.defaultPrevented === true) {
        return undefined;
    }
    if (event.metaKey === true || event.ctrlKey === true || event.altKey === true) {
        return undefined;
    }
    if (target !== null && target !== undefined) {
        if (target.isContentEditable === true) {
            return undefined;
        }
        const tag = (target.tagName ?? '').toUpperCase();
        if (tag === 'INPUT' || tag === 'SELECT') {
            return undefined;
        }
    }
    return event.key;
}
