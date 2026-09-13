/*
 * MIT License. Copyright (c) 2025 DeusData.
 *
 * Uebernommen am 2026-08-28 aus DeusData/codebase-memory-mcp, Branch
 * feat/atlas-r1, Datei graph-ui/src/components/EdgeLines.tsx. Der Lizenztext
 * und die Liste aller uebernommenen Dateien stehen in THIRD_PARTY.md.
 *
 * Aenderungen gegenueber dem Original:
 *  - Importpfade: ../lib/types und ../lib/density liegen hier neben der Datei.
 *  - Formatierung an den Stil dieses Projekts angeglichen (4 Leerzeichen,
 *    einfache Anfuehrungszeichen), lange Gedankenstriche in den Kommentaren
 *    ersetzt.
 *  - Die Prop `targetNodes` bleibt erhalten: sie bediente im
 *    Original die Kanten zwischen zwei Galaxien. Dieses Projekt setzt sie
 *    nicht, sie bleibt aber stehen, damit die Datei gegen das Original
 *    vergleichbar bleibt.
 *  - EDGE_TYPE_COLORS und DEFAULT_EDGE_COLOR bleiben exportiert (W4d);
 *    seit der gemeinsamen Kantenpalette kommen sie aus graph/edge-style.
 *  - W9, Aenderung 1: die Deckkraft haengt an der Kantenzahl, statt fest zu
 *    sein. Die Zahlen und der Grund stehen an {@link edgeIntensityFor}. Ab
 *    30000 Kanten rechnet weiter die Kurve der Uebernahme, Zeichen fuer
 *    Zeichen; darunter wird die Kante so hell, dass ihre Farbe eine Aussage
 *    ist.
 *  - W9, Aenderung 3: eine Kante darf einen seitlichen Versatz tragen
 *    (`GraphEdge.offset`), damit zwei Beziehungen zwischen denselben zwei
 *    Symbolen nicht auf demselben Strich liegen. Ohne den Wert aendert sich
 *    nichts; die Galaxie setzt ihn nicht.
 *  - W9, Aenderung 2 ergaenzte die Engine-Kantenarten. Die gemeinsame
 *    Palette traegt diese Arten jetzt fuer alle Graphansichten.
 *  - Ein GPU-Puls laeuft entlang gerichteter Kanten von Quelle zu Ziel.
 *    Geometrie und Material bleiben gebuendelt; nur eine Zeit-Uniform wird
 *    je Bild geaendert. Pause, reduzierte Bewegung und verborgene Ansichten
 *    stoppen den Puls ohne Geometrie-Neubau.
 */

import { useCallback, useEffect, useMemo, useRef } from 'react';
import { useFrame } from '@react-three/fiber';
import * as THREE from 'three';
import type { GraphNode, GraphEdge } from './types';
import { edgeIntensityScale } from './density';
import { edgeColor, edgePhase, isDirectedEdge } from '../graph/edge-style';
import { useEdgeMotion } from '../graph/edge-motion';

// Keep the existing import surface; every graph now reads the shared palette.
export { EDGE_TYPE_COLORS, DEFAULT_EDGE_COLOR } from '../graph/edge-style';

interface EdgeLinesProps {
    active?: boolean;
    nodes: GraphNode[];
    edges: GraphEdge[];
    highlightedIds: Set<number> | null;
    /* Emphasize relationships touching a selected file/range without adding
     * neighboring nodes to the selection. Other graph views keep their default. */
    emphasizeIncidentEdges?: boolean;
    opacity?: number;
    /* User edge-brightness multiplier (see DisplaySettings). Layered on top of
     * the automatic density scale. */
    brightness?: number;
    /* When set, edge.target is looked up in this array instead of `nodes`.
     * Used for cross-galaxy edges where source lives in the primary graph
     * and target lives in a linked project's offset-adjusted nodes. */
    targetNodes?: GraphNode[];
}

function getClusterKey(fp?: string): string {
    if (!fp) return '';
    const parts = fp.split('/');
    return parts.slice(0, Math.min(2, parts.length)).join('/');
}

/* --------------------------------------------------------- Deckkraft (W9) --
 *
 * Bis W9 war die Deckkraft einer Kante eine Konstante: 0.25 innerhalb eines
 * Clusters, 0.06 sonst, mal der Dichte-Skala aus density.ts. Diese Zahlen sind
 * fuer den Fall gebaut, gegen den die Uebernahme antritt: Zehntausende additiv
 * geblendeter Linien, die sonst zu einem weissen Schleier verschmelzen. Fuer
 * ein Projekt mit 178 Kanten sind sie zu vorsichtig; dort ist das Ergebnis ein
 * gleichmaessig blasser Nebel, in dem zwoelf Farben wie eine aussehen
 * (Martins Befund vom 2026-08-29).
 *
 * Also haengt die Deckkraft jetzt an der Kantenzahl. Die Kurve in Zahlen:
 *
 *   Kanten | innerhalb   | ausserhalb  | vorher (innerhalb / ausserhalb)
 *   -------|-------------|-------------|--------------------------------
 *      178 | 0.62        | 0.30        | 0.25   / 0.06
 *     2000 | 0.62        | 0.30        | 0.25   / 0.06
 *     8000 | 0.206       | 0.070       | 0.140  / 0.034
 *    30000 | 0.0722      | 0.0173      | 0.0722 / 0.0173
 *   120000 | 0.0361      | 0.0087      | 0.0361 / 0.0087
 *
 * Zwei Eigenschaften, auf die es ankommt:
 *
 * 1. **Ab 30000 Kanten aendert sich nichts.** Dort und darueber rechnet weiter
 *    genau die Kurve der Uebernahme (`edgeIntensityScale` aus density.ts, mal
 *    den urspruenglichen 0.25 beziehungsweise 0.06). Ein grosses Repository
 *    wird durch diese Aenderung um keinen Hauch heller, und die Wand, gegen die
 *    die Uebernahme gebaut ist, bleibt verhindert.
 * 2. **Dazwischen wird geometrisch interpoliert**, in log(Kantenzahl). Das ist
 *    stetig an beiden Enden, monoton fallend, und es gibt keinen Punkt, an dem
 *    das Bild springt.
 *
 * Der Fokus-Zweig bleibt, wie er war (0.5 fuer die markierte Nachbarschaft,
 * 0.04 mal Dichte fuer den Rest). Er beantwortet eine andere Frage als diese
 * Kurve: nicht "welche Art ist das", sondern "was gehoert zu dem, was ich
 * gerade angesehen habe". Ihn mit anzuheben hiesse, den Kontrast wegzunehmen,
 * der eine Auswahl ueberhaupt sichtbar macht.
 */

/** Bis hierher wird nichts gedaempft. */
export const EDGE_FULL_COUNT = 2000;

/** Ab hier gilt wieder die Kurve der Uebernahme, unveraendert. */
export const EDGE_WASH_COUNT = 30000;

/** Die Deckkraft bei wenigen Kanten, innerhalb und ausserhalb eines Clusters. */
export const EDGE_INTENSITY_NEAR = 0.62;
export const EDGE_INTENSITY_FAR = 0.3;

/** Die Werte der Uebernahme. Sie sind der Anschluss bei EDGE_WASH_COUNT. */
export const PORTED_INTENSITY_NEAR = 0.25;
export const PORTED_INTENSITY_FAR = 0.06;

/** Der Fokus-Zweig, unveraendert aus der Uebernahme. */
export const EDGE_INTENSITY_FOCUS = 0.5;
export const EDGE_INTENSITY_MUTED = 0.04;

/**
 * Wie stark eine Kante gezeichnet wird, wenn nichts im Fokus steht.
 *
 * Rein und ohne Zustand, damit die Kurve oben pruefbar ist, ohne einen
 * WebGL-Kontext zu oeffnen.
 */
export function edgeIntensityFor(sameCluster: boolean, edgeCount: number): number {
    const ported = sameCluster ? PORTED_INTENSITY_NEAR : PORTED_INTENSITY_FAR;
    const washed = ported * edgeIntensityScale(edgeCount);
    if (!Number.isFinite(edgeCount) || edgeCount >= EDGE_WASH_COUNT) {
        return washed;
    }
    const readable = sameCluster ? EDGE_INTENSITY_NEAR : EDGE_INTENSITY_FAR;
    if (edgeCount <= EDGE_FULL_COUNT) {
        return readable;
    }
    const atWash = ported * edgeIntensityScale(EDGE_WASH_COUNT);
    const share =
        Math.log(edgeCount / EDGE_FULL_COUNT) / Math.log(EDGE_WASH_COUNT / EDGE_FULL_COUNT);
    return readable * Math.pow(atWash / readable, share);
}

export function createEdgeGeometry({
    nodes,
    edges,
    highlightedIds,
    emphasizeIncidentEdges = false,
    brightness = 1.0,
    targetNodes,
}: EdgeLinesProps): THREE.BufferGeometry {
    /* Shrink per-edge glow as the edge count grows so the additively-blended
     * center doesn't saturate to white; the user multiplier rides on top.
     * Seit W9 nur noch fuer den abgedunkelten Rest einer Auswahl: der
     * Grundwert bringt seine eigene Kurve mit (edgeIntensityFor).
     * `edges.length` ist die Zahl der WIRKLICH gezeichneten Kanten, also
     * die nach dem Kantenart-Filter: wer die Haelfte ausblendet, hat auch
     * die Haelfte des Schleiers, und die uebrigen duerfen dafuer heller
     * werden. */
    const densityScale = edgeIntensityScale(edges.length) * brightness;
    const srcMap = new Map<number, number>();
    for (let i = 0; i < nodes.length; i++) {
        srcMap.set(nodes[i].id, i);
    }
    const tgtArr = targetNodes ?? nodes;
    const tgtMap = targetNodes ? new Map<number, number>() : srcMap;
    if (targetNodes) {
        for (let i = 0; i < targetNodes.length; i++) {
            tgtMap.set(targetNodes[i].id, i);
        }
    }

    const hasHighlight = highlightedIds && highlightedIds.size > 0;
    const positions = new Float32Array(edges.length * 6);
    const colors = new Float32Array(edges.length * 6);
    const flow = new Float32Array(edges.length * 6);
    const colorByType = new Map<string, THREE.Color>();
    let validCount = 0;

    for (const edge of edges) {
        const si = srcMap.get(edge.source);
        const ti = tgtMap.get(edge.target);
        if (si === undefined || ti === undefined) continue;

        const s = nodes[si];
        const t = tgtArr[ti];

        const sHL = !hasHighlight || highlightedIds.has(s.id);
        const tHL = !hasHighlight || highlightedIds.has(t.id);
        if (hasHighlight && !sHL && !tHL) continue;

        const sameCluster =
            getClusterKey(s.file_path) === getClusterKey(t.file_path);

        /* Intensity based on cluster membership and highlight.
         * With additive blending + dark background, these glow nicely.
         * Seit W9 haengt der Grundwert an der Kantenzahl: siehe
         * edgeIntensityFor. Der Fokus-Zweig ist der der Uebernahme. */
        let intensity = edgeIntensityFor(sameCluster, edges.length) * brightness;
        if (hasHighlight) {
            /* A selection stays at full strength (never density-scaled) so it
             * pops against the dimmed rest; only the un-selected bulk is scaled. */
            intensity = (sHL && tHL) || emphasizeIncidentEdges
                ? EDGE_INTENSITY_FOCUS
                : EDGE_INTENSITY_MUTED * densityScale;
        }

        /*
         * Der seitliche Versatz (W9, Aenderung 3).
         *
         * Zwei Symbole koennen mehr als eine Beziehung haben: im
         * Demo-Fixture ruft createUser die Klasse ValidationError UND wirft
         * sie. Beide Linien auf denselben Strich zu legen hiesse, sie
         * additiv zu einer dritten Farbe zu mischen, die in keiner Legende
         * steht. Also wird die zweite Linie um `edge.offset` Welteinheiten
         * neben die erste gelegt, senkrecht zur Verbindung in der
         * xy-Ebene. Die Enden bleiben dieselben Symbole; nur der Strich
         * dazwischen weicht aus.
         *
         * Ohne `offset` (die Galaxie setzt keinen) ist das ein
         * Vergleich und sonst nichts.
         */
        let ox = 0;
        let oy = 0;
        const offsetBy = edge.offset ?? 0;
        if (offsetBy !== 0) {
            const dx = t.x - s.x;
            const dy = t.y - s.y;
            const length = Math.sqrt(dx * dx + dy * dy);
            if (length > 0) {
                ox = (-dy / length) * offsetBy;
                oy = (dx / length) * offsetBy;
            }
        }

        const off = validCount * 6;
        positions[off] = s.x + ox;
        positions[off + 1] = s.y + oy;
        positions[off + 2] = s.z;
        positions[off + 3] = t.x + ox;
        positions[off + 4] = t.y + oy;
        positions[off + 5] = t.z;

        /* Color from edge TYPE (correlates with edge type filter) */
        let color = colorByType.get(edge.type);
        if (!color) {
            color = new THREE.Color(edgeColor(edge.type));
            colorByType.set(edge.type, color);
        }
        colors[off] = color.r * intensity;
        colors[off + 1] = color.g * intensity;
        colors[off + 2] = color.b * intensity;
        colors[off + 3] = color.r * intensity;
        colors[off + 4] = color.g * intensity;
        colors[off + 5] = color.b * intensity;

        const phase = edgePhase(`${edge.source}:${edge.target}:${edge.type}`);
        const directed = isDirectedEdge(edge.type) ? 1 : 0;
        flow[off] = 0;
        flow[off + 1] = phase;
        flow[off + 2] = directed;
        flow[off + 3] = 1;
        flow[off + 4] = phase;
        flow[off + 5] = directed;
        validCount++;
    }

    const geo = new THREE.BufferGeometry();
    geo.setAttribute(
        'position',
        new THREE.BufferAttribute(positions.slice(0, validCount * 6), 3),
    );
    geo.setAttribute(
        'color',
        new THREE.BufferAttribute(colors.slice(0, validCount * 6), 3),
    );
    geo.setAttribute(
        'edgeFlow',
        new THREE.BufferAttribute(flow.slice(0, validCount * 6), 3),
    );
    return geo;
}

const EDGE_PULSE_SECONDS = 4.8;

export function EdgeLines({ active = true, opacity = 1.0, ...props }: EdgeLinesProps) {
    const { nodes, edges, highlightedIds, emphasizeIncidentEdges, targetNodes, brightness } = props;
    const moving = useEdgeMotion(active);
    const geometry = useMemo(
        () => createEdgeGeometry({ nodes, edges, highlightedIds, emphasizeIncidentEdges, targetNodes, brightness }),
        [nodes, edges, highlightedIds, emphasizeIncidentEdges, targetNodes, brightness],
    );
    const uniforms = useMemo(() => ({ edgeTime: { value: 0 }, edgeMotion: { value: 0 } }), []);
    const lastFrame = useRef<number | null>(null);

    useEffect(() => () => geometry.dispose(), [geometry]);
    useEffect(() => {
        uniforms.edgeMotion.value = moving ? 1 : 0;
        lastFrame.current = null;
    }, [moving, uniforms]);

    useFrame(() => {
        if (!moving) return;
        // FrameCapDriver supplies milliseconds to Fiber; wall time keeps the
        // pulse at the same speed with capped and uncapped render loops.
        const now = performance.now();
        if (lastFrame.current !== null) {
            uniforms.edgeTime.value =
                (uniforms.edgeTime.value + (now - lastFrame.current) / 1000) % EDGE_PULSE_SECONDS;
        }
        lastFrame.current = now;
    });

    const prepareMaterial = useCallback<THREE.Material['onBeforeCompile']>((shader) => {
        shader.uniforms.edgeTime = uniforms.edgeTime;
        shader.uniforms.edgeMotion = uniforms.edgeMotion;
        shader.vertexShader = shader.vertexShader
            .replace('#include <common>', `#include <common>
attribute vec3 edgeFlow;
varying vec3 vEdgeFlow;`)
            .replace('#include <begin_vertex>', `#include <begin_vertex>
vEdgeFlow = edgeFlow;`);
        shader.fragmentShader = shader.fragmentShader
            .replace('#include <common>', `#include <common>
uniform float edgeTime;
uniform float edgeMotion;
varying vec3 vEdgeFlow;`)
            .replace('#include <color_fragment>', `#include <color_fragment>
// A single soft band traverses the source (0) to target (1). Its padded
// range enters and leaves the endpoints without wrapping across the line.
float edgeHead = fract(edgeTime / ${EDGE_PULSE_SECONDS.toFixed(1)} + vEdgeFlow.y) * 1.24 - 0.12;
float edgePulse = 1.0 - smoothstep(0.0, 0.10, abs(vEdgeFlow.x - edgeHead));
// The pulse never exceeds the previous density-scaled intensity, including
// in a dense additive cloud. All modulation preserves the relationship hue.
diffuseColor.rgb *= 0.72 + 0.28 * edgeMotion * vEdgeFlow.z * edgePulse;`);
    }, [uniforms]);

    return (
        <lineSegments geometry={geometry}>
            <lineBasicMaterial
                vertexColors
                transparent
                opacity={opacity}
                blending={THREE.AdditiveBlending}
                depthWrite={false}
                toneMapped={false}
                onBeforeCompile={prepareMaterial}
                customProgramCacheKey={() => 'directed-edge-pulse-v1'}
            />
        </lineSegments>
    );
}
