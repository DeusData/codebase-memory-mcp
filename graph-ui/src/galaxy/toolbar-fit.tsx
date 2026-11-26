/*
 * Die Leiste eines Ausschnitts passt sich ihrer Breite an (Handtest K3 und
 * das Review dazu).
 *
 * Zuerst hielt sie die eine Zeile, indem sie nicht mehr umbrach, und schnitt
 * dabei ab: bei offenem Chat und 1.494 px Fensterbreite, der Breite des
 * Handtests, lagen Zaehler und Menue "⋯" mit den Limits jenseits des Randes,
 * also genau das Menue, auf das der Hinweis einer abgeschnittenen Ebene
 * verweist. Jetzt wird gemessen, in drei Stufen:
 *
 *  - `full`: alle Beschriftungen ausgeschrieben ("← Back", "Forward →"), und
 *    der Name der Wurzel steht dabei ganz (`toolbarSqueezes`).
 *  - `compact`: knappe Beschriftungen ("+1" statt "Expand +1", "All",
 *    "Path…", "Types · All", "Calls"); so passt sie bei offenem Chat und
 *    1.494 px, auch mit der Liste der letzten Wurzeln.
 *  - `tight`: dazu wird der Zaehler kuerzer, nie unter 8,5rem und mit vollem
 *    Text im Tooltip; "Partial:" steht vorn und bleibt. So passt sie bis rund
 *    1.440 px.
 *  - `wrap`: die knappe Leiste bricht um. Zwei Zeilen sind besser als ein
 *    Knopf, den niemand mehr sieht.
 *
 * Die Stufe steht als `data-fit` direkt am Element und nicht im Zustand von
 * React: das Probieren der Stufen ist dann ein paar Layout-Abfragen in einem
 * Durchgang und kein Rendern je Stufe. Die knappen Worte zeichnet CSS
 * (`attr(data-label)`), so bleiben Text und zugaenglicher Name jedes Knopfes,
 * wie sie waren.
 */
import { useLayoutEffect, type RefObject } from 'react';

export type ToolbarFit = 'full' | 'compact' | 'tight' | 'wrap';
const FITS: readonly ToolbarFit[] = ['full', 'compact', 'tight', 'wrap'];

/** Die erste Stufe, bei der nichts hinausragt; die letzte passt immer, denn sie bricht um. */
export function chooseToolbarFit(overflowsAt: (fit: ToolbarFit) => boolean): ToolbarFit {
    for (const fit of FITS.slice(0, -1)) {
        if (!overflowsAt(fit)) return fit;
    }
    return 'wrap';
}

/** Ob ein Kind der Leiste ueber ihren Inhaltsrand ragt. Aufgeklappte Menues liegen absolut in ihren Kindern und zaehlen nicht. */
export function toolbarOverflows(bar: HTMLElement): boolean {
    const style = getComputedStyle(bar);
    const edge = bar.getBoundingClientRect().right - (parseFloat(style.paddingRight) || 0) - (parseFloat(style.borderRightWidth) || 0);
    for (const child of bar.children) {
        const box = child.getBoundingClientRect();
        if (box.width > 0 && box.right > edge + 0.5) return true;
    }
    return false;
}

/*
 * Ob ein Element, das in voller Stufe ganz zu lesen sein soll
 * (`data-fit-whole`, der Name der Wurzel), schmaler steht, als sein Text und
 * seine Hoechstbreite es wollen (Handtest K2, im Browser gesehen: mit "← Back"
 * und "Forward →" in Worten schrumpfte die Wurzel bei offenem Chat auf
 * "JS…", und nichts ragte hinaus). Gerechnet in der Innenbreite (`clientWidth`).
 */
export function toolbarSqueezes(bar: HTMLElement): boolean {
    for (const element of bar.querySelectorAll<HTMLElement>('[data-fit-whole]')) {
        const style = getComputedStyle(element);
        const max = parseFloat(style.maxWidth);
        const padding = (parseFloat(style.paddingLeft) || 0) + (parseFloat(style.paddingRight) || 0);
        const border = (parseFloat(style.borderLeftWidth) || 0) + (parseFloat(style.borderRightWidth) || 0);
        const room = Number.isFinite(max) ? (style.boxSizing === 'border-box' ? max - border : max + padding) : Number.POSITIVE_INFINITY;
        if (element.clientWidth + 1 < Math.min(element.scrollWidth, room)) return true;
    }
    return false;
}

/** Setzt die Leiste auf die erste Stufe, die passt, und gibt sie zurueck. In voller Stufe passt sie nur, wenn auch die Wurzel ganz steht. */
export function fitToolbar(bar: HTMLElement): ToolbarFit {
    const fit = chooseToolbarFit(level => { bar.dataset.fit = level; return toolbarOverflows(bar) || (level === 'full' && toolbarSqueezes(bar)); });
    bar.dataset.fit = fit;
    return fit;
}

/*
 * Neu gemessen wird, wenn die Leiste ihre Breite aendert (Chat auf oder zu,
 * Fenster) und wenn sich ihr Inhalt aendert (Zaehler, Wurzelname, Knoepfe).
 * Beides erst im naechsten Bild: die Messung aendert die Hoehe der Leiste, und
 * im Rueckruf des ResizeObservers waere das eine Schleife.
 */
export function useToolbarFit(ref: RefObject<HTMLElement | null>, active: boolean): void {
    useLayoutEffect(() => {
        const bar = ref.current;
        if (!bar) return;
        if (!active) { delete bar.dataset.fit; return; }
        fitToolbar(bar);
        let frame = 0;
        const schedule = () => {
            if (frame) return;
            frame = requestAnimationFrame(() => { frame = 0; fitToolbar(bar); });
        };
        const resize = typeof ResizeObserver === 'undefined' ? undefined : new ResizeObserver(schedule);
        resize?.observe(bar);
        const content = typeof MutationObserver === 'undefined' ? undefined : new MutationObserver(schedule);
        content?.observe(bar, { childList: true, subtree: true, characterData: true });
        return () => {
            resize?.disconnect(); content?.disconnect();
            if (frame) cancelAnimationFrame(frame);
            delete bar.dataset.fit;
        };
    }, [ref, active]);
}

/** Eine Beschriftung mit knapper Form ab `data-fit=compact`; im DOM steht nur die volle. */
export function FitLabel({ wide, narrow }: { wide: string; narrow: string }) {
    return <><span className="atlas-fit-wide">{wide}</span><span className="atlas-fit-narrow" data-label={narrow} aria-hidden="true" /></>;
}
