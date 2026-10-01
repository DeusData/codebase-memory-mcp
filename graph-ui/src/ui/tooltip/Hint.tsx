/**
 * Ein Tooltip, den diese Oberflaeche selbst zeichnet, selbst platziert und
 * selbst messen laesst.
 *
 * ## Warum es ihn gibt
 *
 * Bis W8b erklaerte sich diese Oberflaeche mit dem `title`-Attribut, 78 Mal.
 * Der Browser zeichnet daraus einen Kasten AUSSERHALB des Dokuments, unter dem
 * Mauszeiger und ohne Rechteck. Zwei Folgen, und beide sind Fehler:
 *
 *  1. Er legt sich ueber das, was gerade unter ihm liegt. Im Screenshot des
 *     Nutzers vom 2026-08-29 lag "76 nodes, 178 edges from /api/layout" ueber
 *     dem Detail-Regler und ueber den Chips Logic, Calls, Data.
 *  2. Keine Messung sieht ihn. Die Ueberlagerungsregel dieses Projekts liest
 *     `getBoundingClientRect`, und ein nativer Tooltip hat keine. Der Fehler
 *     war also nicht nur da, er war unsichtbar fuer jeden gruenen Beweislauf.
 *
 * Dieser Kasten hat beides umgekehrt: er steht im Dokument, er rechnet seine
 * Lage (src/ui/tooltip/tooltip-model.ts), und er traegt eine Testmarke, an der
 * ein Lauf ihn einzeln oeffnen und vermessen kann.
 *
 * ## Warum er in den Rumpf portiert wird und nicht neben den Ausloeser
 *
 * Weil er sonst die Geschwisterordnung seines Ausloesers aendert. Ein Kasten,
 * der nur manchmal zwischen zwei Chips steht, ist ein Layout, das beim
 * Beruehren zuckt, und CSS-Regeln, die auf Nachbarschaft zeigen, wuerden
 * gelegentlich danebengreifen. Im Rumpf liegt er fest positioniert und
 * beruehrt die Anordnung von nichts.
 *
 * ## Die Tastatur ist keine Zugabe
 *
 * Er oeffnet bei Hover UND bei Fokus, und Escape schliesst ihn. Ein Tooltip,
 * den nur die Maus oeffnet, waere fuer die Haelfte dieser Oberflaeche gar
 * nicht da: sie ist an der Tastatur entworfen (PLAN Abschnitt 4). Escape haengt
 * in der EINFANGENDEN Phase am Fenster, damit er vor den Griffen der grossen
 * Flaechen drankommt: wer einen Tooltip offen hat und Escape drueckt, meint
 * ihn, und nicht den Bereich dahinter. Die Griffe dahinter pruefen
 * `defaultPrevented` und lassen die Taste dann liegen.
 *
 * ## Der Klick-Griff, und warum ihn der Ausloeser holen muss
 *
 * Hover und Fokus sind zwei Wege, und auf einem Zeigegeraet ohne Hover ist
 * keiner davon einer: wer tippt, hovert nicht, und wer die Maus nicht auf ein
 * Zeichen legt, um zu sehen ob etwas passiert, erfaehrt nie, dass dort etwas
 * steht. Ein Ausloeser, der nichts anderes tut als diesen Satz zu tragen, ist
 * damit unerreichbar, und wenn er ausserdem ein `<button>` ist, verspricht er
 * eine Handlung, die es nicht gibt (Befund W10-1, `npm run check:promises`).
 *
 * Dafuer gibt es {@link HintHold}: ein Klick HAELT den Kasten fest, ein zweiter
 * Klick, Escape oder ein Klick daneben laesst ihn los. Der Griff wird aber
 * nicht jedem Ausloeser aufgedraengt, sondern nur dem, der ihn nimmt (die
 * Funktionsform von `children`). Der Grund ist der Absatz weiter unten an
 * `onClick`: an einem Schalter, der etwas TUT, ist der Satz nach dem Druecken
 * die Beschreibung von etwas, das schon passiert ist, und meist ausserdem
 * falsch. Wuerde ein Klick dort festhalten statt zu schliessen, bliebe genau
 * der veraltete Satz stehen, den der Beweislauf aus W9 im Bild gefunden hat.
 * Ein Griff fuer die stummen Ausloeser, kein Griff fuer die handelnden: das ist
 * EINE Regel an EINER Stelle und gilt fuer alle Hints dieser Oberflaeche.
 *
 * ## Wo er NICHT hingehoert
 *
 * Ein Tooltip ist kein Ersatz fuer eine fehlende Beschriftung. Wo ein `title`
 * nur den sichtbaren Text wiederholt hat, ist er in W8b ersatzlos entfallen;
 * wo eine Flaeche ihren Grund IM FELD sagen muss (die gedimmten Reiter des
 * Erklaeren-Bereichs), sagt sie ihn weiterhin dort.
 */

import type { JSX, ReactElement, ReactNode } from 'react';
import { cloneElement, useCallback, useEffect, useId, useLayoutEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

import {
    HINT_MAX_WIDTH,
    HINT_PROTECTED_SELECTOR,
    placeHint,
} from './tooltip-model';
import type { HintPlacement, HintRect } from './tooltip-model';

/**
 * Der Griff, mit dem ein Ausloeser seinen Kasten festhaelt.
 *
 * Er wird der Funktionsform von `children` uebergeben, damit der Klick-Griff
 * dort steht, wo er hingehoert: am Element, das ihn traegt. Das ist auch die
 * Form, in der ein Leser des Quelltextes (und tools/promise-scan.mjs) sieht,
 * dass dieser Knopf auf einen Klick antwortet.
 */
export interface HintHold {
    /** Ob der Kasten gerade festgehalten wird. */
    held: boolean;
    /** Festhalten, oder loslassen, wenn er schon haengt. */
    toggle: () => void;
}

export interface HintProps {
    /**
     * Der Satz. Leer oder fehlend heisst: kein Tooltip, und der Ausloeser wird
     * unveraendert durchgereicht.
     *
     * Der Fall ist keine Bequemlichkeit, sondern der Normalfall an mehreren
     * Stellen: eine Zeile, die als Chip gezeichnet wird, traegt ihren Ort im
     * Tooltip, und dieselbe Zeile als volle Zeile traegt ihn sichtbar daneben.
     */
    text?: string | undefined;
    /** Woran ein Beweislauf diesen Tooltip wiedererkennt. */
    name: string;
    /**
     * Genau ein Element: der Ausloeser.
     *
     * Als Funktion geschrieben bekommt er den {@link HintHold} und bindet den
     * Klick selbst. Dann und nur dann haelt ein Klick den Kasten fest, statt
     * ihn zu schliessen.
     */
    children: ReactElement | ((hold: HintHold) => ReactElement);
}

/** Die Rechtecke, die an dieser Stelle nicht verdeckt werden duerfen. */
function protectedRects(anchor: Element): HintRect[] {
    const out: HintRect[] = [];
    for (const node of document.querySelectorAll(HINT_PROTECTED_SELECTOR)) {
        if (node === anchor) {
            continue;
        }
        const style = window.getComputedStyle(node);
        if (style.display === 'none' || style.visibility === 'hidden') {
            continue;
        }
        const rect = node.getBoundingClientRect();
        if (rect.width <= 0 || rect.height <= 0) {
            continue;
        }
        out.push({ x: rect.left, y: rect.top, width: rect.width, height: rect.height });
    }
    return out;
}

/** Zwei Rueckrufe hintereinander, wo der Ausloeser schon einen hatte. */
function both<T>(first: ((value: T) => void) | undefined, second: (value: T) => void) {
    return (value: T): void => {
        first?.(value);
        second(value);
    };
}

export default function Hint(props: HintProps): JSX.Element {
    const text = (props.text ?? '').replace(/\s+/g, ' ').trim();
    const id = `atlas-hint-${useId().replace(/:/g, '')}`;
    /*
     * Zwei Gruende, offen zu stehen, und sie muessen getrennt bleiben.
     *
     * `near` ist der Zeiger oder der Fokus: er kommt und geht, ohne dass jemand
     * etwas entschieden hat. `held` ist eine Entscheidung des Lesers, und die
     * ueberlebt es, dass der Zeiger weiterzieht. Ein einziges `open` koennte
     * das nicht auseinanderhalten: das Verlassen des Ausloesers wuerde eine
     * Entscheidung zuruecknehmen, die niemand zurueckgenommen hat.
     */
    const [near, setNear] = useState(false);
    const [held, setHeld] = useState(false);
    const open = near || held;
    const [placed, setPlaced] = useState<HintPlacement | null>(null);
    const anchor = useRef<HTMLElement | null>(null);
    const box = useRef<HTMLSpanElement | null>(null);

    /** Zu, aus beiden Gruenden. Alles, was schliesst, schliesst ganz. */
    const release = useCallback(() => {
        setNear(false);
        setHeld(false);
    }, []);

    /*
     * Der Griff, und er ist leer, wenn es nichts zu sagen gibt: ein Ausloeser
     * ohne Satz reicht sein Element unveraendert durch, und ein Klick darf dann
     * auch keinen Zustand anfassen, den niemand sehen kann.
     */
    const hold: HintHold = {
        held: text.length > 0 && held,
        toggle: () => {
            if (text.length === 0) {
                return;
            }
            if (held) {
                /*
                 * Der zweite Klick nimmt auch den Zeiger aus der Rechnung. Sonst
                 * stuende der Kasten weiter da, weil die Maus nach dem Klick
                 * liegen bleibt, und ein Leser, der ihn eben weggeklickt hat,
                 * saehe keinen Unterschied.
                 */
                release();
                return;
            }
            setHeld(true);
        },
    };

    /*
     * Erst zeichnen, dann messen, dann stellen.
     *
     * Die Lage haengt an der Groesse des Kastens, und die Groesse haengt am
     * Text und an der Breite, die der Umbruch daraus macht. Sie zu schaetzen
     * hiesse, an zwei Stellen zu wissen, wie diese Schrift bricht. Der Kasten
     * steht darum im ersten Bild unsichtbar da (`visibility: hidden`, also auch
     * kein Kandidat der Lesbarkeitsmessung) und im zweiten an seinem Platz.
     */
    useLayoutEffect(() => {
        if (!open) {
            setPlaced(null);
            return;
        }
        const trigger = anchor.current;
        const node = box.current;
        if (trigger === null || node === null) {
            return;
        }
        const rect = trigger.getBoundingClientRect();
        setPlaced(placeHint({
            anchor: { x: rect.left, y: rect.top, width: rect.width, height: rect.height },
            size: { width: node.offsetWidth, height: node.offsetHeight },
            viewport: { width: window.innerWidth, height: window.innerHeight },
            protect: protectedRects(trigger),
        }));
    }, [open, text]);

    // Escape, in der einfangenden Phase. Warum, steht im Kopf.
    useEffect(() => {
        if (!open) {
            return;
        }
        const onKey = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape') {
                return;
            }
            event.preventDefault();
            release();
        };
        window.addEventListener('keydown', onKey, true);
        return () => window.removeEventListener('keydown', onKey, true);
    }, [open, release]);

    /*
     * Ein Klick daneben laesst den Kasten los.
     *
     * Escape und der zweite Klick sind die beiden Wege, die der Contract nennt,
     * aber sie setzen beide voraus, dass der Leser an den Ausloeser
     * zurueckfindet. Wer stattdessen einfach weiterarbeitet, meint damit
     * dasselbe. Ohne diese Zeile bliebe ein festgehaltener Kasten ueber der
     * Stelle stehen, an der es gerade weitergeht, und das ist genau der Fehler,
     * gegen den dieses ganze Modul geschrieben ist.
     *
     * `pointerdown` und nicht `click`, damit er weg ist, bevor die Flaeche
     * darunter antwortet, und in der einfangenden Phase aus demselben Grund wie
     * Escape.
     */
    useEffect(() => {
        if (!held) {
            return;
        }
        const onDown = (event: Event): void => {
            const trigger = anchor.current;
            const target = event.target;
            if (trigger !== null && target instanceof Node && trigger.contains(target)) {
                return;
            }
            release();
        };
        window.addEventListener('pointerdown', onDown, true);
        return () => window.removeEventListener('pointerdown', onDown, true);
    }, [held, release]);

    /*
     * Die Funktionsform gibt dem Ausloeser den Griff; die Elementform ist die
     * alte und bleibt es. `bound` merkt sich, welche es war, denn davon haengt
     * unten ab, was ein Klick bedeutet.
     */
    const bound = typeof props.children === 'function';
    const child = bound
        ? (props.children as (value: HintHold) => ReactElement)(hold)
        : (props.children as ReactElement);

    if (text.length === 0) {
        return child;
    }

    const childProps = child.props as Record<string, unknown>;
    const trigger = cloneElement(child, {
        'aria-describedby': open ? id : undefined,
        'data-hint': text,
        'data-hint-name': props.name,
        'data-hint-open': open,
        /* Getrennt gemeldet, weil es zwei verschiedene Zustaende sind: offen, weil
         * jemand hinsieht, und offen, weil jemand ihn festgehalten hat. Ein
         * Beweislauf, der nur `data-hint-open` liest, koennte den zweiten nicht
         * vom ersten unterscheiden. */
        'data-hint-held': held,
        ref: (node: HTMLElement | null) => {
            anchor.current = node;
            const given = (child as unknown as { ref?: unknown }).ref;
            if (typeof given === 'function') {
                (given as (value: HTMLElement | null) => void)(node);
            } else if (given !== null && typeof given === 'object') {
                (given as { current: HTMLElement | null }).current = node;
            }
        },
        onMouseEnter: both(childProps.onMouseEnter as ((value: unknown) => void) | undefined, () => setNear(true)),
        onMouseLeave: both(childProps.onMouseLeave as ((value: unknown) => void) | undefined, () => setNear(false)),
        onFocus: both(childProps.onFocus as ((value: unknown) => void) | undefined, () => setNear(true)),
        /*
         * Der Fokus geht, der Kasten geht mit, auch der festgehaltene. Er haengt
         * an seinem Ausloeser; wer weitertabbt, hat ihn verlassen.
         */
        onBlur: both(childProps.onBlur as ((value: unknown) => void) | undefined, release),
        /*
         * Ein Klick schliesst ihn, und das ist keine Bequemlichkeit.
         *
         * Ein Tooltip sagt, was ein Knopf TUN WIRD. Ist er gedrueckt, ist der
         * Satz von vorhin die Beschreibung einer Handlung, die schon passiert
         * ist, und an den meisten Schaltern dieser Oberflaeche ausserdem
         * falsch: "hide the CALLS edges" steht dann ueber einem Knopf, der sie
         * gerade wieder einblenden wuerde. Der Zeiger bleibt nach einem Klick
         * liegen, `mouseover` feuert kein zweites Mal, und der veraltete Satz
         * stuende bis zur naechsten Mausbewegung da.
         *
         * Der Beweislauf von W9 hat genau das aufgedeckt: er klickt eine
         * Kantenart aus, laesst den Zeiger dort und nimmt ein Bild der Szene
         * auf. Der offene Kasten lag im Bild, und die Differenzmessung, die
         * daraus die Farbe einer Kante rechnet, sah eine Flaeche, die keine
         * Kante war.
         *
         * Wer den Griff aus {@link HintHold} selbst gebunden hat, bekommt das
         * nicht dazu: sein Klick IST die Entscheidung ueber den Kasten, und ein
         * Schliessen dahinter wuerde sie sofort wieder aufheben.
         */
        onClick: bound
            ? (childProps.onClick as ((value: unknown) => void) | undefined)
            : both(childProps.onClick as ((value: unknown) => void) | undefined, release),
    } as Record<string, unknown>);

    const overlay: ReactNode = open ? (
        <span
            className="atlas-hint"
            data-testid="atlas-hint"
            data-hint-for={props.name}
            data-side={placed?.side ?? 'below'}
            data-covered={placed?.covered ?? -1}
            id={id}
            role="tooltip"
            ref={box}
            style={{
                position: 'fixed',
                maxWidth: `${HINT_MAX_WIDTH}px`,
                left: `${placed?.x ?? 0}px`,
                top: `${placed?.y ?? 0}px`,
                visibility: placed === null ? 'hidden' : 'visible',
            }}
        >
            {text}
        </span>
    ) : null;

    return (
        <>
            {trigger}
            {overlay !== null && typeof document !== 'undefined'
                ? createPortal(overlay, document.body)
                : null}
        </>
    );
}
