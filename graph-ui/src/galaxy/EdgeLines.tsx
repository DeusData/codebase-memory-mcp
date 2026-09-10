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
 *  - Logik unveraendert, inklusive der Prop `targetNodes`: sie bediente im
 *    Original die Kanten zwischen zwei Galaxien. Dieses Projekt setzt sie
 *    nicht, sie bleibt aber stehen, damit die Datei gegen das Original
 *    vergleichbar bleibt.
 *  - EDGE_TYPE_COLORS und DEFAULT_EDGE_COLOR sind exportiert (W4d). Die Werte
 *    sind unveraendert; die Galaxy-Legende liest die Tabelle, statt sie zu
 *    wiederholen. Eine zweite Farbtabelle waere die Sorte Legende, die nach
 *    der ersten Aenderung an dieser Datei etwas anderes behauptet als das
 *    Bild daneben.
 *  - W9, Aenderung 1: die Deckkraft haengt an der Kantenzahl, statt fest zu
 *    sein. Die Zahlen und der Grund stehen an {@link edgeIntensityFor}. Ab
 *    30000 Kanten rechnet weiter die Kurve der Uebernahme, Zeichen fuer
 *    Zeichen; darunter wird die Kante so hell, dass ihre Farbe eine Aussage
 *    ist. Das ist die eine echte Abweichung dieser Datei vom Original.
 *  - W9, Aenderung 3: eine Kante darf einen seitlichen Versatz tragen
 *    (`GraphEdge.offset`), damit zwei Beziehungen zwischen denselben zwei
 *    Symbolen nicht auf demselben Strich liegen. Ohne den Wert aendert sich
 *    nichts; die Galaxie setzt ihn nicht.
 *  - W9, Aenderung 2: die Farbtabelle bekommt die Kantenarten dazu, die die
 *    Engine dieses Projekts wirklich liefert (USAGE, CONFIGURES, RAISES,
 *    CALL_REFERENCE, HAS_BRANCH). Die uebernommenen Eintraege bleiben Wert
 *    fuer Wert stehen und liegen dafuer in einer eigenen Tabelle, damit die
 *    Abweichung nachlesbar ist statt untergemischt.
 */

import { useMemo } from 'react';
import * as THREE from 'three';
import type { GraphNode, GraphEdge } from './types';
import { edgeIntensityScale } from './density';

interface EdgeLinesProps {
    nodes: GraphNode[];
    edges: GraphEdge[];
    highlightedIds: Set<number> | null;
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

/* Edge type to color (matches the filter panel) */
const PORTED_EDGE_TYPE_COLORS: Record<string, string> = {
    CALLS: '#1DA27E',
    IMPORTS: '#3b82f6',
    DEFINES: '#a855f7',
    DEFINES_METHOD: '#a855f7',
    CONTAINS_FILE: '#22c55e',
    CONTAINS_FOLDER: '#22c55e',
    CONTAINS_PACKAGE: '#22c55e',
    HANDLES: '#eab308',
    IMPLEMENTS: '#f97316',
    HTTP_CALLS: '#e11d48',
    ASYNC_CALLS: '#ec4899',
    GRPC_CALLS: '#f59e0b',
    GRAPHQL_CALLS: '#e879f9',
    TRPC_CALLS: '#a78bfa',
    CROSS_HTTP_CALLS: '#fb923c',
    CROSS_ASYNC_CALLS: '#fb7185',
    CROSS_GRPC_CALLS: '#fbbf24',
    CROSS_GRAPHQL_CALLS: '#f0abfc',
    CROSS_TRPC_CALLS: '#c4b5fd',
    CROSS_CHANNEL: '#fdba74',
    MEMBER_OF: '#64748b',
    TESTS_FILE: '#06b6d4',
};

/**
 * Die Kantenarten, die die Engine dieses Projekts liefert und die die
 * uebernommene Tabelle nicht kennt (W9).
 *
 * Gemessen an der laufenden Vorschau, fixtures/atlas-sample: von den zwoelf
 * Arten, die /api/layout dort schickt, standen fuenf in keiner Zeile der
 * Tabelle (USAGE 37, CONFIGURES 6, RAISES 3, CALL_REFERENCE 1, HAS_BRANCH 1)
 * und teilten sich deshalb {@link DEFAULT_EDGE_COLOR} mit allem anderen
 * Unbekannten. Das ist der Teil von Martins Befund vom 2026-08-29, den die
 * Deckkraft nicht heilt: eine Kante, die es nur in EINER Farbe gibt, kann noch
 * so hell gezeichnet sein und sagt trotzdem nicht, was sie ist. Der
 * Vorgabewert bleibt fuer alles, was auch diese Liste nicht kennt: eine
 * erfundene Farbe waere eine Behauptung ueber eine Beziehung, die dieses
 * Projekt nie gesehen hat.
 *
 * Die Werte sind gegen die schon belegten Toene gewaehlt: der kleinste Abstand
 * zwischen zwei Farben, die im Demo-Fixture zusammen im Bild stehen, ist
 * 47.6 (CALLS #1DA27E gegen CONTAINS_FILE #22c55e, euklidisch in RGB), der
 * kleinste unter den fuenf neuen 92 (USAGE gegen CONFIGURES). Der Beweislauf
 * misst mit einer Schwelle von 40.
 */
export const ENGINE_EDGE_TYPE_COLORS: Record<string, string> = {
    /* Ein Symbol benutzt ein anderes, ohne es aufzurufen: Limette. */
    USAGE: '#a3e635',
    /* Zugriff auf eine Umgebungsvariable: Gelb, die Farbe der Einstellung. */
    CONFIGURES: '#fde047',
    /* Ein Fehlerpfad. Rot, und ausdruecklich nicht das Orange von IMPLEMENTS. */
    RAISES: '#ff4d4d',
    /* Ein Aufruf, den der Index nur als Verweis kennt: helles Blau. */
    CALL_REFERENCE: '#7dd3fc',
    /* Reine Struktur des Repositoriums, wie MEMBER_OF: gedecktes Grau. */
    HAS_BRANCH: '#94a3b8',
};

/**
 * Die Tabelle, aus der die Szene und die Legende ihre Farben nehmen.
 *
 * Die uebernommenen Eintraege stehen unveraendert in
 * {@link PORTED_EDGE_TYPE_COLORS}; was dieses Projekt dazugelegt hat, steht in
 * {@link ENGINE_EDGE_TYPE_COLORS}. Zwei Tabellen und ein Zusammenschluss, damit
 * die Abweichung vom Original nachlesbar bleibt: eine gemischte Liste waere in
 * einem Jahr nicht mehr auseinanderzuhalten.
 */
export const EDGE_TYPE_COLORS: Record<string, string> = {
    ...PORTED_EDGE_TYPE_COLORS,
    ...ENGINE_EDGE_TYPE_COLORS,
};

export const DEFAULT_EDGE_COLOR = '#1C8585';

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

export function EdgeLines({
    nodes,
    edges,
    highlightedIds,
    opacity = 1.0,
    brightness = 1.0,
    targetNodes,
}: EdgeLinesProps) {
    const geometry = useMemo(() => {
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
                intensity = sHL && tHL
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
            const edgeColor = new THREE.Color(
                EDGE_TYPE_COLORS[edge.type] ?? DEFAULT_EDGE_COLOR,
            );
            colors[off] = edgeColor.r * intensity;
            colors[off + 1] = edgeColor.g * intensity;
            colors[off + 2] = edgeColor.b * intensity;
            colors[off + 3] = edgeColor.r * intensity;
            colors[off + 4] = edgeColor.g * intensity;
            colors[off + 5] = edgeColor.b * intensity;
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
        return geo;
    }, [nodes, edges, highlightedIds, targetNodes, brightness]);

    return (
        <lineSegments geometry={geometry}>
            <lineBasicMaterial
                vertexColors
                transparent
                opacity={opacity}
                blending={THREE.AdditiveBlending}
                depthWrite={false}
                toneMapped={false}
            />
        </lineSegments>
    );
}
