/*
 * Der Pfad in der Szene (Review-Befund G5).
 *
 * Die Kanten des Pfades liegen als eigene Linien UEBER der Kantenebene, in der
 * Farbe ihrer Art und ohne Tiefentest, damit sie im abgedunkelten Rest nicht
 * verschwinden. Ihre Art steht als Beschriftung daneben, und nur an ihnen:
 * eine Beschriftung an jeder Kante waere bei hundert Kanten kein Text mehr.
 * Der Schritt, auf dem der Leser steht, ist heller und traegt einen Ring an
 * seinem Ziel. Gezeichnet wird an den Positionen, die die Szene wirklich
 * zeichnet, also nach der Trennung auf dem Schirm.
 *
 * Seit Handtest K6 tragen alle Knoten des Pfades ihren Namen hier, als DOM wie
 * die Kantenlabels, und die Szene zeichnet fuer sie keinen zweiten. Die Labels
 * der Kanten weichen diesen Namen aus: sie gleiten auf ihrer Kante, bis sie
 * keinen Namen und kein anderes Label beruehren (src/galaxy/path-frame.ts).
 * Vorher lag der Name des Ziels ueber "CALLS", sobald Ziel und Wurzel nah
 * beieinander standen.
 */
import { useEffect, useMemo, useRef } from 'react';
import { useFrame, useThree } from '@react-three/fiber';
import { Html } from '@react-three/drei';
import * as THREE from 'three';
import { edgeColor } from '../graph/edge-style';
import { graphNodeName } from './node-names';
import { placeAlongSegment, type ScreenRect } from './path-frame';
import type { ScopePathStep } from './scope-path';
import type { GraphNode } from './types';

export interface ScenePath {
    steps: readonly ScopePathStep[];
    active: number;
    /** `all` beschriftet jede Kante des Pfades, `active` nur den aktuellen Schritt. */
    labels: 'all' | 'active';
}

/** Die Knoten eines Pfades, in der Reihenfolge, in der er sie betritt. */
export function pathNodeIds(path: ScenePath): number[] {
    const ids = new Set<number>();
    for (const step of path.steps) { ids.add(step.from); ids.add(step.to); }
    return [...ids];
}

/**
 * Welchen Texten jedes Kantenlabel der Szene ausweicht: den Namen des Pfades,
 * denen der markierten Wurzeln und der Ueberschrift des Bandes der Hierarchie.
 * Die Ueberschrift fehlte hier bis zum Handtest vom 2026-10-04 (G2): ein Pfad
 * durch das Band legte "DEFINES" auf "Mixed directions · 55 nodes". Die
 * Kantenschilder der Hierarchie (HierarchyEdgeLabels.tsx) lesen dieselbe Liste.
 */
export const LABEL_BLOCKER_SELECTOR = '.atlas-galaxy-path-node b, .atlas-galaxy-root-marker b, .atlas-hierarchy-band-label';

/** Die Kaesten dieser Texte auf dem Schirm; was nicht gezeichnet ist (Breite null), steht niemandem im Weg. */
export function screenBlockers(host: ParentNode): ScreenRect[] {
    return [...host.querySelectorAll<HTMLElement>(LABEL_BLOCKER_SELECTOR)]
        .map((element) => element.getBoundingClientRect()).filter((rect) => rect.width > 0)
        .map((rect) => ({ left: rect.left, right: rect.right, top: rect.top, bottom: rect.bottom }));
}
/* Nur jedes dritte Bild: die Lage der Namen aendert sich mit der Kamera, nicht schneller. */
const PLACE_EVERY_FRAMES = 3;

/**
 * Wo die Kantenlabels des Pfades stehen, in der Reihenfolge des Pfades, und
 * welches keinen Platz bekommt.
 *
 * Review zu K30: fand ein Label keinen freien Platz, stand es dort, wo es am
 * wenigsten verdeckte, und das konnte die Ueberschrift des Bandes der
 * Hierarchie sein (ein breiter Titel ueber einer steilen Kante). Wie die
 * Kantenschilder der Hierarchie (HierarchyEdgeLabels.tsx) faellt es jetzt
 * weg, statt zwei Texte unlesbar zu machen: die Art der Kante steht weiter in
 * der Schrittliste, und mit der naechsten Kamera kommt es wieder, sobald es
 * frei steht. Ein spaeteres Label weicht den frueheren aus wie den Namen.
 */
export function pathLabelSpots(labels: readonly { index: number; from: { x: number; y: number }; to: { x: number; y: number }; size: { width: number; height: number } }[],
    blockers: readonly ScreenRect[]): Map<number, { t: number; rect: ScreenRect }> {
    const placed: ScreenRect[] = [];
    const spots = new Map<number, { t: number; rect: ScreenRect }>();
    for (const { index, from, to, size } of labels) {
        const choice = placeAlongSegment(from, to, size, blockers, placed);
        if (choice.overlap > 0) continue;
        placed.push(choice.rect);
        spots.set(index, { t: choice.t, rect: choice.rect });
    }
    return spots;
}

export function PathLayer({ nodes, path, namedRoots }: {
    nodes: readonly GraphNode[];
    path: ScenePath;
    /** Wurzeln, deren Name schon an ihrem Ring steht; sie bekommen hier keinen zweiten. */
    namedRoots?: ReadonlySet<number> | undefined;
}) {
    const byId = useMemo(() => new Map(nodes.map((node) => [node.id, node])), [nodes]);
    const segments = useMemo(() => path.steps.flatMap((step, index) => {
        const from = byId.get(step.from), to = byId.get(step.to);
        return from && to ? [{ step, index, from, to }] : [];
    }), [path.steps, byId]);
    const geometry = useMemo(() => {
        const positions = new Float32Array(segments.length * 6), colors = new Float32Array(segments.length * 6);
        const white = new THREE.Color('#ffffff');
        segments.forEach(({ step, index, from, to }, at) => {
            positions.set([from.x, from.y, from.z, to.x, to.y, to.z], at * 6);
            const color = new THREE.Color(edgeColor(step.edge.type));
            if (index === path.active) color.lerp(white, 0.55);
            colors.set([color.r, color.g, color.b, color.r, color.g, color.b], at * 6);
        });
        const result = new THREE.BufferGeometry();
        result.setAttribute('position', new THREE.BufferAttribute(positions, 3));
        result.setAttribute('color', new THREE.BufferAttribute(colors, 3));
        return result;
    }, [segments, path.active]);
    useEffect(() => () => geometry.dispose(), [geometry]);
    const goal = segments.find((segment) => segment.index === path.active);
    const labelled = useMemo(() => segments.filter(({ index }) => path.labels === 'all' || index === path.active), [segments, path.labels, path.active]);
    const named = useMemo(() => pathNodeIds(path).filter((id) => !namedRoots?.has(id)).flatMap((id) => byId.get(id) ?? []),
        [path, namedRoots, byId]);

    const groups = useRef(new Map<number, THREE.Group>());
    const labels = useRef(new Map<number, HTMLElement>());
    // Die Groesse eines Labels nach seinem Text: ein weggelassenes Label (display: none) hat keine, die man messen koennte.
    const sizes = useRef(new Map<string, { width: number; height: number }>());
    const camera = useThree((state) => state.camera);
    const gl = useThree((state) => state.gl);
    // drei hangs Html into the element the events are connected to, not into the canvas parent.
    const connected = useThree((state) => state.events.connected) as unknown;
    const tick = useRef(0);
    const scratch = useMemo(() => ({ from: new THREE.Vector3(), to: new THREE.Vector3(), point: new THREE.Vector3() }), []);
    useFrame(() => {
        tick.current = (tick.current + 1) % PLACE_EVERY_FRAMES;
        const host = connected instanceof HTMLElement ? connected : gl.domElement.parentElement;
        if (tick.current !== 0 || host === null) return;
        const box = gl.domElement.getBoundingClientRect();
        const blockers = screenBlockers(host);
        const toScreen = (node: GraphNode) => {
            scratch.point.set(node.x, node.y, node.z).project(camera);
            return { x: box.left + ((scratch.point.x + 1) / 2) * box.width, y: box.top + ((1 - scratch.point.y) / 2) * box.height };
        };
        const measured = labelled.flatMap(({ step, index, from, to }) => {
            const group = groups.current.get(index), label = labels.current.get(index);
            if (!group || !label) return [];
            let size = sizes.current.get(step.edge.type);
            if (!size) {
                label.style.display = '';
                size = { width: label.offsetWidth, height: label.offsetHeight };
                if (size.width === 0) return [];
                sizes.current.set(step.edge.type, size);
            }
            return [{ index, group, label, from, to, a: toScreen(from), b: toScreen(to), size }];
        });
        const spots = pathLabelSpots(measured.map(({ index, a, b, size }) => ({ index, from: a, to: b, size })), blockers);
        for (const { index, group, label, from, to, a, b } of measured) {
            const spot = spots.get(index);
            const display = spot ? '' : 'none';
            if (label.style.display !== display) label.style.display = display;
            if (!spot) continue;
            group.position.lerpVectors(scratch.from.set(from.x, from.y, from.z), scratch.to.set(to.x, to.y, to.z), spot.t);
            // The side step from placeAlongSegment, applied in screen space around the point on the edge.
            const dx = (spot.rect.left + spot.rect.right) / 2, dy = (spot.rect.top + spot.rect.bottom) / 2;
            const onEdge = { x: a.x + (b.x - a.x) * spot.t, y: a.y + (b.y - a.y) * spot.t };
            label.style.transform = `translate(${dx - onEdge.x}px, ${dy - onEdge.y}px)`;
        }
    });

    return (
        <>
            <lineSegments geometry={geometry} renderOrder={18}>
                <lineBasicMaterial vertexColors transparent depthTest={false} depthWrite={false} toneMapped={false} />
            </lineSegments>
            {labelled.map(({ step, index, from, to }) => (
                <group key={index} position={[(from.x + to.x) / 2, (from.y + to.y) / 2, (from.z + to.z) / 2]}
                    ref={(group) => { if (group) groups.current.set(index, group); else groups.current.delete(index); }}>
                    <Html center zIndexRange={[9, 0]} style={{ pointerEvents: 'none' }}>
                        <span className="atlas-galaxy-path-label" data-active={index === path.active}
                            ref={(label) => { if (label) labels.current.set(index, label); else labels.current.delete(index); }}>{step.edge.type}</span>
                    </Html>
                </group>
            ))}
            {named.map((node) => {
                const active = node.id === goal?.to.id;
                return (
                    <Html key={node.id} position={[node.x, node.y, node.z]} zIndexRange={[10, 0]} style={{ pointerEvents: 'none' }}>
                        <span className="atlas-galaxy-path-node" data-active={active} data-node={node.id}
                            {...(active ? { 'data-testid': 'atlas-galaxy-path-step' } : {})}>
                            <i aria-hidden="true" />
                            <b>{graphNodeName(node)}</b>
                        </span>
                    </Html>
                );
            })}
        </>
    );
}
