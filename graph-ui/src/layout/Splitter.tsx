/**
 * Ein Griff zwischen zwei Zonen. Vier davon gibt es, und sie sind dieselbe
 * Datei, weil sie dasselbe tun.
 *
 * ## Was ein Griff koennen muss, damit er einer ist
 *
 * **Die Maus.** Druecken, ziehen, loslassen, und der Zeiger bleibt beim Griff,
 * auch wenn er dabei ueber den Editor faehrt (`setPointerCapture`). Ohne das
 * Einfangen reisst der Zug ab, sobald der Zeiger eine Flaeche erwischt, die
 * selbst auf Zeiger hoert, und das ist bei einem 6 Pixel breiten Griff nach zwei
 * Pixeln der Fall.
 *
 * **Die Tastatur.** Fokussierbar, Pfeiltasten, und Shift fuer den grossen
 * Schritt. Ein Griff, den nur die Maus erreicht, ist ein halber Griff; das gilt
 * hier doppelt, weil diese Oberflaeche ihr ganzes Vorbild aus der Tastatur zieht
 * (PLAN Abschnitt 4).
 *
 * **Den Weg zurueck.** Doppelklick setzt genau diese eine Grenze auf ihre
 * Vorgabe. Wer eine Zone zu weit gezogen hat, will nicht das ganze Layout
 * zuruecksetzen, sondern die eine Grenze, an der er gerade steht.
 *
 * **Sagen, wo er steht.** `role="separator"` mit `aria-valuenow`, `aria-valuemin`
 * und `aria-valuemax`, denn genau das ist er: eine verschiebbare Trennlinie mit
 * einem Bereich. Die drei Zahlen sind ausserdem das, woran der Beweislauf ihn
 * misst, statt aus Pixeln zu raten, was die Oberflaeche gerade meint.
 *
 * ## Was er NICHT mehr tut: sich erklaeren
 *
 * Bis W10b hing an jedem der vier Griffe ein vierzeiliger Kasten, der das Ziehen,
 * die Pfeiltasten und den Doppelklick beschrieb. Nutzerauftrag vom 2026-08-29,
 * 23:14, mit Screenshot: "diese Meldung nicht anzeigen, das wird klar durch alles
 * andere. Bitte an allen Bordern die Meldung entfernen." Er hat recht, und der
 * Grund liegt eine Zeile weiter oben in dieser Datei: seit W8b traegt jeder Griff
 * eine sichtbare Marke und zehn Pixel Trefferflaeche. Der Kasten erklaerte damit
 * etwas, das man sieht, und verdeckte dafuer Inhalt.
 *
 * Entfernt ist die BESCHREIBUNG, nicht die Faehigkeit: `aria-label` sagt weiter,
 * welche Grenze das ist, `aria-valuetext` sagt, wo sie steht, und die Tastatur
 * bedient sie unveraendert. Ein Vorleseprogramm erfaehrt also genau so viel wie
 * vorher; nur das Fenster ueber dem Text ist weg.
 *
 * ## Warum der Zug nicht im Zustand liegt
 *
 * Zwischen zwei Bildern passieren zwanzig Zeigerereignisse. Jedes davon als
 * Zustandsaenderung waere zwanzig Bilder, in denen nichts zu sehen ist ausser
 * der Bewegung, die der Browser ohnehin schon kennt. Was gezeichnet wird, ist
 * die Groesse, und die gehoert dem Aufrufer; der Zug selbst liegt in einem Ref.
 * Dieselbe Entscheidung wie beim Griff des Antwort-Panels in W7c, und sie hat
 * dort ebenso wenig etwas mit Geschwindigkeit zu tun wie hier: sie haelt den
 * Unterschied zwischen "der Leser bewegt gerade etwas" und "die Oberflaeche hat
 * eine neue Groesse" sichtbar.
 */

import type { JSX, KeyboardEvent as ReactKeyboardEvent, PointerEvent as ReactPointerEvent } from 'react';
import { useRef } from 'react';

import { messages } from '../i18n/messages';
import { LAYOUT_BIG_STEP, LAYOUT_STEP } from './layout-model';

export interface SplitterProps {
    /** Die Testmarke, an der der Beweislauf ihn anfasst. Zugleich `data-split`. */
    testId: string;
    /**
     * Welche Achse er verschiebt.
     *
     * `vertical` ist eine SENKRECHTE Linie, die eine BREITE aendert (Explorer,
     * rechte Spalte); `horizontal` ist eine waagerechte Linie, die eine HOEHE
     * aendert. Die Benennung folgt `aria-orientation`, wo sie die Richtung der
     * Linie meint und nicht die der Bewegung, und der Kommentar steht hier, weil
     * genau das die Stelle ist, an der man es falsch herum liest.
     */
    orientation: 'vertical' | 'horizontal';
    /** Wie er heisst. Vier Griffe mit demselben Namen sind fuer eine Vorlesehilfe einer. */
    label: string;
    value: number;
    min: number;
    max: number;
    /**
     * Ob ein Zug in die positive Richtung die Zone kleiner macht.
     *
     * Der Griff ueber dem Erklaeren-Feld waechst nach OBEN: der Zeiger geht nach
     * oben, die Zone wird groesser. Der Griff unter dem Twin waechst nach UNTEN.
     * Beide Male folgt die Kante dem Zeiger, und genau darum ist das Vorzeichen
     * verschieden. Ein Griff, der vor dem Zeiger weglaeuft, fuehlt sich kaputt
     * an, auch wenn die Zahl stimmt.
     */
    invert?: boolean;
    onChange: (value: number) => void;
    /** Doppelklick: genau diese Grenze zurueck auf ihre Vorgabe. */
    onReset: () => void;
}

export default function Splitter(props: SplitterProps): JSX.Element {
    const drag = useRef<{ start: number; from: number } | null>(null);
    const sign = props.invert === true ? -1 : 1;

    const clamped = (value: number): number =>
        Math.round(Math.max(props.min, Math.min(props.max, value)));

    const onPointerDown = (event: ReactPointerEvent<HTMLDivElement>): void => {
        if (event.button !== 0) {
            return;
        }
        drag.current = {
            start: props.orientation === 'vertical' ? event.clientX : event.clientY,
            from: props.value,
        };
        event.currentTarget.setPointerCapture?.(event.pointerId);
        // Sonst markiert der Zug den Text, ueber den er faehrt.
        event.preventDefault();
    };

    const onPointerMove = (event: ReactPointerEvent<HTMLDivElement>): void => {
        const state = drag.current;
        if (state === null) {
            return;
        }
        const now = props.orientation === 'vertical' ? event.clientX : event.clientY;
        props.onChange(clamped(state.from + sign * (now - state.start)));
    };

    const onPointerUp = (event: ReactPointerEvent<HTMLDivElement>): void => {
        drag.current = null;
        event.currentTarget.releasePointerCapture?.(event.pointerId);
    };

    const onKeyDown = (event: ReactKeyboardEvent<HTMLDivElement>): void => {
        const forward = props.orientation === 'vertical' ? 'ArrowRight' : 'ArrowDown';
        const backward = props.orientation === 'vertical' ? 'ArrowLeft' : 'ArrowUp';
        if (event.key !== forward && event.key !== backward) {
            return;
        }
        event.preventDefault();
        const step = event.shiftKey ? LAYOUT_BIG_STEP : LAYOUT_STEP;
        props.onChange(clamped(props.value + sign * (event.key === forward ? step : -step)));
    };

    return (
        <div
            className="atlas-splitter"
            data-testid={props.testId}
            data-split={props.testId}
            data-orientation={props.orientation}
            role="separator"
            aria-orientation={props.orientation}
            aria-label={props.label}
            aria-valuenow={props.value}
            aria-valuemin={props.min}
            aria-valuemax={props.max}
            aria-valuetext={messages.layout.splitter.value(props.value)}
            tabIndex={0}
            onPointerDown={onPointerDown}
            onPointerMove={onPointerMove}
            onPointerUp={onPointerUp}
            onPointerCancel={onPointerUp}
            onKeyDown={onKeyDown}
            onDoubleClick={props.onReset}
        />
    );
}
