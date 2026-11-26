/*
 * Die Ebene der Hover-Karten (Review zu K29).
 *
 * Seit G1 bildet die Huelle des Canvas ihren eigenen Stapelkontext: jedes
 * <Html> der Szene bleibt darin und damit unter den Bedienflaechen der
 * Galaxie. Fuer Namen ist das richtig, fuer eine Hover-Karte nicht: sie
 * erscheint nur, solange der Zeiger auf einem Knoten steht, und ist dann das,
 * was der Leser lesen will. Neben "Selection details" war sie abgeschnitten.
 *
 * Darum steht neben der Huelle eine eigene Ebene, so gross wie die Szene, und
 * die Karten haengen sich dort hinein (drei `portal`). drei rechnet ihre Lage
 * weiter mit der Kamera der Szene; die Ebene liegt deckungsgleich ueber dem
 * Canvas, also steht die Karte am selben Punkt wie vorher. Ihr z-index liegt
 * ueber jeder Flaeche der Galaxie (Selection details 4, Hierarchie-Notiz 20,
 * Pfadliste 25) und unter den Menues der Werkzeugleiste (30, die Suche 35) und
 * den Tooltips (40): ein offenes Menue bleibt lesbar, auch wenn eine Karte
 * darunter steht.
 */
import { createContext, useContext, useMemo } from 'react';
import type { CSSProperties, JSX, ReactNode, RefObject } from 'react';
import { Html } from '@react-three/drei';

export const HOVER_LAYER_Z_INDEX = 28;

export const HOVER_LAYER_STYLE: CSSProperties = {
    position: 'absolute', inset: 0, zIndex: HOVER_LAYER_Z_INDEX, pointerEvents: 'none', overflow: 'hidden',
};

/*
 * Die Ebene, in die eine Hover-Karte zeichnet; ohne sie bleibt die Karte, wie
 * drei es tut, in der Huelle. Das Element selbst und kein Ref: drei liest sein
 * Ziel beim Rendern, und erst ein neuer Wert sagt der Karte, dass die Ebene
 * jetzt steht.
 */
export const HoverLayerContext = createContext<HTMLElement | null>(null);

/** Eine Karte an einem Punkt der Szene, in der Ebene der Hover-Karten. */
export function HoverCardHtml({ position, children }: { position: [number, number, number]; children: ReactNode }): JSX.Element {
    const layer = useContext(HoverLayerContext);
    const portal = useMemo(() => (layer ? { current: layer } : undefined), [layer]);
    return (
        <Html position={position} center style={{ pointerEvents: 'none' }}
            {...(portal ? { portal: portal as RefObject<HTMLElement>, zIndexRange: [1, 0] } : {})}>
            {children}
        </Html>
    );
}
