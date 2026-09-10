/**
 * Der Tastentest: was von einem Tastendruck wirklich ankommt, und was diese
 * Oberflaeche daraus gemacht haette.
 *
 * ## Warum es diese Datei gibt
 *
 * Nutzerbefund vom 2026-08-29: "alt plus letter funktioniert nur fuer atlas".
 * An der laufenden Vorschau war er nicht nachzustellen: Alt+a, Alt+w, Alt+b,
 * Alt+c und Alt+l loesen in Chromium alle aus, bei Fokus auf dem Koerper wie im
 * Editor, und der Tastendruck kommt mit `code: "KeyW"` und `defaultPrevented:
 * false` am Fenster an. Ein nicht reproduzierbarer Befund laesst genau zwei
 * ehrliche Antworten zu: raten, oder messen lassen. Diese Datei ist die zweite.
 *
 * Sie beantwortet die vier Fragen, an denen ein Kuerzel scheitern kann, und sie
 * beantwortet sie an dem Geraet, an dem es scheitert:
 *
 *  1. **Kommt der Tastendruck ueberhaupt an?** Steht nach dem Druck nichts im
 *     Test, hat ihn die Tastaturbelegung, das Betriebssystem oder eine
 *     Erweiterung vorher abgefangen; dann ist die Anwendung nicht beteiligt.
 *  2. **Traegt er Alt/Option?** Manche Belegungen schicken Option als
 *     Zeichenmodifikator und setzen `altKey` nicht.
 *  3. **Welche physische Taste war es?** Unter macOS macht Option+A ein `å`.
 *     Deshalb liest die Verdrahtung `code` und nicht `key`, und der Test zeigt
 *     beides nebeneinander, damit der Unterschied sichtbar ist.
 *  4. **Hat ihn jemand vor uns verbraucht?** `defaultPrevented` allein sagt das
 *     nicht: der Griff dieser Oberflaeche bestellt ein erkanntes Kuerzel selbst
 *     ab. Der Test trennt die beiden Faelle, indem der Griff das Ereignis
 *     zeichnet, das er verbraucht hat.
 *
 * Alles hier ist eine reine Funktion ueber ein Ereignis. Kein DOM, kein React,
 * keine Zeitmessung: der Test der Hilfeseite zeichnet nur, was hier
 * herauskommt, und die Unit-Tests pruefen es ohne Fenster.
 */

import { isTypingTarget, menuShortcutFor } from './keyboard';
import type { KeyEventLike, KeyTargetLike } from './keyboard';

/**
 * Die Marke, mit der der Griff am Fenster ein von IHM verbrauchtes Ereignis
 * zeichnet.
 *
 * Am Ereignis selbst und nicht in einer Nebenbuchhaltung, weil die Frage
 * "wurde GENAU DIESER Druck von uns verbraucht" sonst ueber Zeitstempel
 * beantwortet werden muesste, und zwei Tastendruecke in derselben
 * Millisekunde sind kein erfundener Fall.
 */
const HANDLED = '__atlasHandledShortcut';

/** Zeichnet ein Ereignis als von dieser Oberflaeche verbraucht. */
export function markHandled(event: object, shortcut: string): void {
    (event as Record<string, unknown>)[HANDLED] = shortcut;
}

/** Das Kuerzel, als das diese Oberflaeche das Ereignis verbraucht hat, oder nichts. */
export function handledShortcutOf(event: object): string | undefined {
    const value = (event as Record<string, unknown>)[HANDLED];
    return typeof value === 'string' && value.length > 0 ? value : undefined;
}

/**
 * Wer den Tastendruck verbraucht hat.
 *
 * Drei Werte und nicht zwei: "niemand" und "wir" sind beide der gute Fall,
 * "jemand anderes" ist der einzige, der den Nutzerbefund erklaeren wuerde.
 */
export type KeyProbeConsumer = 'nobody' | 'this-window' | 'something-else';

/** Was von einem Tastendruck ankam, und was daraus geworden waere. */
export interface KeyProbeReading {
    /** Die physische Taste, zum Beispiel `KeyW`. Leer, wenn der Browser keine nennt. */
    code: string;
    /** Das erzeugte Zeichen. Unter macOS mit Option oft ein anderes als der Buchstabe. */
    key: string;
    altKey: boolean;
    ctrlKey: boolean;
    metaKey: boolean;
    shiftKey: boolean;
    defaultPrevented: boolean;
    consumedBy: KeyProbeConsumer;
    /**
     * Das Menuekuerzel, das die Anwendung erkannt haette. Leer heisst: keines.
     *
     * Gerechnet wird mit derselben Funktion, die der Griff am Fenster ruft, und
     * ausdruecklich so, als haette niemand das Ereignis abbestellt: die Frage
     * lautet "war das ein Kuerzel", nicht "war es noch zu haben".
     */
    shortcut: string;
    /** Das Element, an dem der Druck ankam. Leer, wenn keines genannt wurde. */
    targetTag: string;
    /** Ob dort gerade getippt wird. Ein blankes Kuerzel gilt dann nicht. */
    typingTarget: boolean;
}

/** Die leere Ablesung, solange noch nichts gedrueckt wurde. */
export const NO_KEY_READING: KeyProbeReading = {
    code: '',
    key: '',
    altKey: false,
    ctrlKey: false,
    metaKey: false,
    shiftKey: false,
    defaultPrevented: false,
    consumedBy: 'nobody',
    shortcut: '',
    targetTag: '',
    typingTarget: false,
};

/** Was von diesem Tastendruck ankam. */
export function readKeyEvent(
    event: KeyEventLike,
    target: KeyTargetLike | null | undefined,
    shortcuts: readonly string[],
): KeyProbeReading {
    const defaultPrevented = event.defaultPrevented === true;
    /*
     * Die Felder werden einzeln abgeschrieben und das Ereignis NICHT kopiert.
     *
     * `{ ...event }` liefert bei einem echten KeyboardEvent ein leeres Objekt:
     * `code`, `key` und die Modifikatoren liegen auf dem Prototyp und nicht am
     * Objekt selbst. Ein Test, der so kopiert, meldet zu jedem Tastendruck
     * "kein Kuerzel", also genau den Befund, den er aufklaeren soll.
     */
    const arrived: KeyEventLike = {
        key: event.key ?? '',
        code: event.code ?? '',
        altKey: event.altKey === true,
        ctrlKey: event.ctrlKey === true,
        metaKey: event.metaKey === true,
        shiftKey: event.shiftKey === true,
        // Ausdruecklich false: die Frage lautet "war das ein Kuerzel", nicht
        // "war es noch zu haben".
        defaultPrevented: false,
    };
    const handled = handledShortcutOf(event as unknown as object);
    // Der Griff am Fenster bestellt ein erkanntes Kuerzel selbst ab. Ohne die
    // Marke sagte `defaultPrevented` also bei jedem funktionierenden Kuerzel
    // "jemand hat es verbraucht", und der Test waere in genau dem Fall
    // irrefuehrend, fuer den er gebaut ist.
    const consumedBy: KeyProbeConsumer =
        handled !== undefined ? 'this-window' : defaultPrevented ? 'something-else' : 'nobody';
    return {
        code: arrived.code ?? '',
        key: arrived.key,
        altKey: arrived.altKey === true,
        ctrlKey: arrived.ctrlKey === true,
        metaKey: arrived.metaKey === true,
        shiftKey: arrived.shiftKey === true,
        defaultPrevented,
        consumedBy,
        shortcut: menuShortcutFor(arrived, target, shortcuts) ?? '',
        targetTag: (target?.tagName ?? '').toUpperCase(),
        typingTarget: isTypingTarget(target),
    };
}
