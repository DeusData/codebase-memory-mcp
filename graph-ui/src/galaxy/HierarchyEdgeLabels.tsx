/*
 * Die Kantenarten an den Linien der Hierarchie (Handtest K5).
 *
 * In der Hierarchie eines Ausschnitts war nicht zu erkennen, welche Linie ein
 * Aufruf, ein Test oder eine Definition ist. Jetzt steht die Art an der Linie,
 * eine Beschriftung je Knotenpaar: ein Test, der JSONBAgg aufruft UND testet,
 * traegt "CALLS · TESTS →" statt zweier Schilder auf derselben Linie. Der
 * Pfeil zeigt von der Quelle zum Ziel, so wie die beiden im Bild stehen. Die
 * Labels weichen den Namen der Knoten aus, wie die Labels eines Pfades
 * (src/galaxy/path-frame.ts); die Namen sind hier Sprites der Szene, ihre
 * Kaesten kommen aus NodeLabels (`onLabelLayout`) in Weltkoordinaten.
 */
import { useMemo, useRef, type RefObject } from 'react';
import { useFrame, useThree } from '@react-three/fiber';
import { Html, Line } from '@react-three/drei';
import * as THREE from 'three';
import { placeAlongSegment, type ScreenRect } from './path-frame';
import { screenBlockers } from './PathLayer';
import type { LabelBox } from './NodeLabels';
import { HIERARCHY_LABEL_FONT_SIZE } from './hierarchy-layout';
import { galaxyHierarchyText } from './galaxy-strings';
import type { GraphEdge, GraphNode } from './types';
import type { HierarchyBand } from './graph-scope';

/** Mehr Schilder auf einmal waeren kein Text mehr, sondern eine Flaeche. */
export const HIERARCHY_EDGE_LABEL_BUDGET = 80;
/** So viele Schilder liegen bereit; gezeigt werden davon die, deren Linie im Bild liegt und die Platz finden. */
export const HIERARCHY_EDGE_LABEL_RENDER_CAP = 400;

export interface HierarchyEdgeLabel { key: string; source: number; target: number; text: string }

/** Wo ein Knoten in der Hierarchie steht: Ebene und Lage. */
export type HierarchySpot = { hop: number; x: number; y: number };

const typeList = (types: ReadonlySet<string>) => [...types].sort((x, y) => Number(y === 'CALLS') - Number(x === 'CALLS') || x.localeCompare(y)).join(' · ');

/** Der Pfeil von `from` nach `to`, so wie die beiden im Bild stehen (y waechst nach oben). */
export function edgeArrow(from: { x: number; y: number }, to: { x: number; y: number }): '→' | '←' | '↑' | '↓' {
    const dx = to.x - from.x, dy = to.y - from.y;
    return Math.abs(dx) >= Math.abs(dy) ? (dx >= 0 ? '→' : '←') : (dy > 0 ? '↑' : '↓');
}
const withArrow = (types: string, arrow: string) => (arrow === '←' ? `${arrow} ${types}` : `${types} ${arrow}`);

/*
 * Ein Schild je Knotenpaar, die Arten ohne Doppelte, CALLS zuerst.
 *
 * Mit `layout` (zweites Review zu K5): bei zwei Ebenen fasste ein Faecher
 * draussen seine Linien zu "CALLS · TESTS ×8" zusammen, ohne Richtung, und wer
 * wen aufruft, war nicht zu lesen. Jetzt traegt jede Linie ihr eigenes Schild
 * mit ihren Arten und dem Pfeil von der Quelle zum Ziel; laufen zwischen zwei
 * Knoten Kanten in beide Richtungen, stehen beide Haelften da
 * ("INHERITS → · ← USAGE"). Die Reihenfolge geht von der Wurzel nach aussen,
 * damit eine Obergrenze die aeusseren Schilder trifft und nicht die inneren.
 */
export function hierarchyEdgeLabels(edges: readonly GraphEdge[], layout?: ReadonlyMap<number, HierarchySpot>): HierarchyEdgeLabel[] {
    const pairs = new Map<string, { source: number; target: number; forward: Set<string>; backward: Set<string> }>();
    for (const edge of edges) {
        if (edge.source === edge.target) continue;
        const [a, b] = edge.source < edge.target ? [edge.source, edge.target] : [edge.target, edge.source];
        const key = `${a}:${b}`;
        const pair = pairs.get(key) ?? { source: edge.source, target: edge.target, forward: new Set<string>(), backward: new Set<string>() };
        (edge.source === pair.source ? pair.forward : pair.backward).add(edge.type);
        pairs.set(key, pair);
    }
    const labels = [...pairs].map(([key, pair]) => {
        const from = layout?.get(pair.source), to = layout?.get(pair.target);
        if (!from || !to) return { key, source: pair.source, target: pair.target, text: typeList(new Set([...pair.forward, ...pair.backward])) };
        const parts = [pair.forward.size ? withArrow(typeList(pair.forward), edgeArrow(from, to)) : '',
            pair.backward.size ? withArrow(typeList(pair.backward), edgeArrow(to, from)) : ''].filter(Boolean);
        return { key, source: pair.source, target: pair.target, text: parts.join(' · ') };
    });
    if (!layout) return labels;
    const hop = (label: HierarchyEdgeLabel) => Math.min(layout.get(label.source)?.hop ?? Number.MAX_SAFE_INTEGER, layout.get(label.target)?.hop ?? Number.MAX_SAFE_INTEGER);
    return labels.map((label, order) => ({ label, order, hop: hop(label) })).sort((x, y) => x.hop - y.hop || x.order - y.order).map(entry => entry.label);
}

const PLACE_EVERY_FRAMES = 3;

/*
 * Ein Schild nur dort, wo die Namen zu lesen sind (Review zu K5, im Browser
 * gesehen): bei zwei Ebenen standen die Namen im eingepassten Bild bei rund
 * 5 px, und siebzig Schilder in festen 10 px deckten es zu. Darunter traegt die
 * Farbe der Linie die Art, wie in der Legende; wer hineinzoomt, bekommt die
 * Schilder an den Linien, die er sieht.
 */
export const EDGE_LABEL_MIN_NAME_PIXELS = 9;

/*
 * Ein freier Platz fuer ein Schild der Hierarchie, sonst keiner: hier liegt
 * kein Schild auf einem Namen, die Farbe der Linie sagt die Art auch. Das
 * Schild steht auf seiner Linie oder direkt daneben (zweites Review zu K5:
 * drei Labelbreiten daneben stand es bei zwei Ebenen im Leeren, und welcher
 * Linie es gehoert, war nicht mehr zu sehen).
 */
export const HIERARCHY_LABEL_MAX_SIDE = 1;
export function hierarchyLabelSpot(from: { x: number; y: number }, to: { x: number; y: number }, size: { width: number; height: number },
    blockers: readonly ScreenRect[], placed: readonly ScreenRect[]): { t: number; rect: ScreenRect } | undefined {
    const choice = placeAlongSegment(from, to, size, blockers, placed, HIERARCHY_LABEL_MAX_SIDE);
    return choice.overlap > 0 ? undefined : choice;
}
export function edgeLabelVisible(namePixels: number, at: { x: number; y: number }, canvas: ScreenRect): boolean {
    return namePixels >= EDGE_LABEL_MIN_NAME_PIXELS && at.x >= canvas.left && at.x <= canvas.right && at.y >= canvas.top && at.y <= canvas.bottom;
}

export function HierarchyEdgeLabels({ nodes, edges, layout, nameBoxes }: {
    nodes: readonly GraphNode[];
    edges: readonly GraphEdge[];
    /** Ebene und Lage je Knoten: ordnet die Schilder von der Wurzel nach aussen und gibt ihnen den Pfeil (siehe `hierarchyEdgeLabels`). */
    layout?: ReadonlyMap<number, HierarchySpot>;
    /** Die gezeichneten Namen, in Weltkoordinaten (NodeLabels). */
    nameBoxes: RefObject<LabelBox[]>;
}) {
    const byId = useMemo(() => new Map(nodes.map((node) => [node.id, node])), [nodes]);
    const labels = useMemo(() => hierarchyEdgeLabels(edges, layout).flatMap((label) => {
        const from = byId.get(label.source), to = byId.get(label.target);
        return from && to ? [{ ...label, from, to }] : [];
    }).slice(0, HIERARCHY_EDGE_LABEL_RENDER_CAP), [edges, layout, byId]);
    const groups = useRef(new Map<string, THREE.Group>());
    const elements = useRef(new Map<string, HTMLElement>());
    const sizes = useRef(new Map<string, { width: number; height: number }>());
    const camera = useThree((state) => state.camera);
    const gl = useThree((state) => state.gl);
    // drei hangs Html into the element the events are connected to, not into the canvas parent.
    const connected = useThree((state) => state.events?.connected) as unknown;
    const tick = useRef(0);
    const point = useMemo(() => new THREE.Vector3(), []);
    const ends = useMemo(() => ({ from: new THREE.Vector3(), to: new THREE.Vector3() }), []);
    useFrame(() => {
        tick.current = (tick.current + 1) % PLACE_EVERY_FRAMES;
        if (tick.current !== 0) return;
        const box = gl.domElement.getBoundingClientRect();
        const toScreen = (x: number, y: number, z = 0) => {
            point.set(x, y, z).project(camera);
            return { x: box.left + ((point.x + 1) / 2) * box.width, y: box.top + ((1 - point.y) / 2) * box.height };
        };
        const blockers: ScreenRect[] = (nameBoxes.current ?? []).map((name) => {
            const a = toScreen(name.x - name.width / 2, name.y + name.height / 2), b = toScreen(name.x + name.width / 2, name.y - name.height / 2);
            return { left: Math.min(a.x, b.x), right: Math.max(a.x, b.x), top: Math.min(a.y, b.y), bottom: Math.max(a.y, b.y) };
        });
        // Was ausser den Namen frei bleibt, als DOM wie die Schilder selbst: die Ueberschrift des Bandes (dieselbe Liste wie der Pfad).
        const host = connected instanceof HTMLElement ? connected : gl.domElement.parentElement;
        if (host) blockers.push(...screenBlockers(host));
        const placed: ScreenRect[] = [];
        // So hoch steht ein Name auf dem Schirm: die Schriftgroesse der Hierarchie, durch dieselbe Kamera.
        const anchor = labels[0]?.from;
        const namePixels = anchor ? Math.abs(toScreen(anchor.x, anchor.y + HIERARCHY_LABEL_FONT_SIZE, anchor.z).y - toScreen(anchor.x, anchor.y, anchor.z).y) : 0;
        const canvas = { left: box.left, top: box.top, right: box.right, bottom: box.bottom };
        for (const { key, text, from, to } of labels) {
            const group = groups.current.get(key), element = elements.current.get(key);
            if (!group || !element) continue;
            const a = toScreen(from.x, from.y, from.z), b = toScreen(to.x, to.y, to.z);
            // Die Groesse eines Schildes einmal messen: Schreiben und Lesen im Wechsel hiesse ein Layout je Schild und Durchgang.
            // Nach dem Text und nicht nach dem Paar: die Hierarchie zaehlt ihre Knoten je Bild neu, und dasselbe Paar
            // traegt im naechsten Ausschnitt einen anderen, laengeren Text (im Browser gesehen: Schilder auf Namen).
            let size = sizes.current.get(text);
            if (!size) {
                element.style.display = '';
                size = { width: element.offsetWidth, height: element.offsetHeight };
                if (size.width > 0) sizes.current.set(text, size);
            }
            const choice = placed.length < HIERARCHY_EDGE_LABEL_BUDGET && size.width > 0
                && edgeLabelVisible(namePixels, { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 }, canvas)
                ? hierarchyLabelSpot(a, b, size, blockers, placed) : undefined;
            const display = choice ? '' : 'none';
            if (element.style.display !== display) element.style.display = display;
            if (!choice) continue;
            placed.push(choice.rect);
            group.position.lerpVectors(ends.from.set(from.x, from.y, from.z), ends.to.set(to.x, to.y, to.z), choice.t);
            const onEdge = { x: a.x + (b.x - a.x) * choice.t, y: a.y + (b.y - a.y) * choice.t };
            element.style.transform = `translate(${(choice.rect.left + choice.rect.right) / 2 - onEdge.x}px, ${(choice.rect.top + choice.rect.bottom) / 2 - onEdge.y}px)`;
        }
    });
    return (
        <>
            {labels.map(({ key, text, from, to }) => (
                <group key={key} position={[(from.x + to.x) / 2, (from.y + to.y) / 2, (from.z + to.z) / 2]}
                    ref={(group) => { if (group) groups.current.set(key, group); else groups.current.delete(key); }}>
                    <Html center zIndexRange={[8, 0]} style={{ pointerEvents: 'none' }}>
                        <span className="atlas-galaxy-path-label atlas-hierarchy-edge-label" data-testid="atlas-hierarchy-edge-label"
                            ref={(element) => { if (element) elements.current.set(key, element); else elements.current.delete(key); }}>{text}</span>
                    </Html>
                </group>
            ))}
        </>
    );
}

/*
 * Die Ueberschrift des Bandes unter den Spalten (zweites Review zu K5): was
 * dort steht, ist weder eingehend noch ausgehend, und das steht im Bild, nicht
 * erst im Tooltip. Ein gestrichelter Rahmen um das ganze Band, die
 * Ueberschrift auf seiner oberen Kante, trennt es von den Spalten darueber.
 */
const BAND_FRAME_COLOR = '#6f8f80';
export function HierarchyBandLabel({ band }: { band: HierarchyBand }) {
    const frame = useMemo(() => [[band.left, band.y, -1], [band.right, band.y, -1], [band.right, band.bottom, -1], [band.left, band.bottom, -1], [band.left, band.y, -1]] as [number, number, number][],
        [band.left, band.right, band.y, band.bottom]);
    return (
        <>
            <Line points={frame} color={BAND_FRAME_COLOR} lineWidth={1} dashed dashSize={10} gapSize={7} transparent opacity={0.8} />
            <Html position={[band.x, band.y, 0]} center zIndexRange={[9, 0]} style={{ pointerEvents: 'none' }}>
                <span className="atlas-hierarchy-band-label" data-testid="atlas-hierarchy-band-label" data-count={band.count}>
                    <strong>{galaxyHierarchyText.bandTitle(band.count)}</strong>{' '}
                    <small>{galaxyHierarchyText.bandDetail}</small>
                </span>
            </Html>
        </>
    );
}
