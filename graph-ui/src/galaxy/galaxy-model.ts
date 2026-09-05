/**
 * Was die Galaxie ueber ihre Knoten weiss, ohne dass ein Bild dafuer noetig
 * waere.
 *
 * Die Szene aus der Uebernahme (GraphScene.tsx und ihre vier Nachbarn) redet
 * in Knoten-IDs und Vektoren. Die Oberflaeche dieses Projekts redet in
 * qualifizierten Namen, Dateipfaden und Zeilen, weil Reader und Twin das tun.
 * Diese Datei ist die Uebersetzung zwischen beiden, und sie ist absichtlich
 * frei von React und von three: alles hier laesst sich an einem Handvoll
 * Knoten pruefen, ohne einen WebGL-Kontext zu oeffnen.
 *
 * Drei Entscheidungen stehen hier fest:
 *
 * 1. **Ein Symbol wird ueber seinen qualifizierten Namen gefunden.** Nicht
 *    ueber Datei plus Zeile: das Layout traegt `start_line` nur, wo die Engine
 *    eine kennt, und zwei Symbole in einer Datei haetten dieselbe Datei. Der
 *    qualifizierte Name ist der einzige Schluessel, den Graph, Twin und Layout
 *    gleich schreiben (probe: `projekt.src.services.userService.createUser`).
 * 2. **Nachbarn sind die direkten Nachbarn, in beide Richtungen.** Das ist das
 *    Muster des Originals (GraphTab.tsx: Knoten plus alles, was eine Kante mit
 *    ihm teilt). Zwei Schritte weit waere bei einem Hub die halbe Galaxie.
 * 3. **Ein Knoten ohne Datei ist kein Navigationsziel.** Die Engine legt
 *    EnvVar-, Projekt- und Ordner-Knoten in dieselbe Wolke; sie haben keinen
 *    Ort im Quelltext. Ein Klick darauf sagt das, statt still nichts zu tun.
 */

import { symbolKindOf } from '../provider/cbm-rpc-provider';
import { twinTargetOf } from '../twin/twin-target';
import type { SymbolRef } from '../core/focus-protocol';
import type { GraphData, GraphEdge, GraphNode } from './types';

/**
 * Wie viele Knoten vom Layout angefordert werden.
 *
 * Dieselbe Zahl wie im Vorbild (GRAPH_RENDER_NODE_LIMIT in
 * graph-ui/src/hooks/useGraphData.ts). Sie ist ein Deckel und wird als solcher
 * benannt: was darueber liegt, ist nicht im Bild, und das Panel sagt es, wenn
 * ein gesuchtes Symbol fehlt.
 */
export const LAYOUT_NODE_BUDGET = 5000;

/** Die Route, aus der die Galaxie kommt. Der Beweislauf schreibt sie mit. */
export const LAYOUT_ROUTE = '/api/layout';

/** Die vollstaendige Adresse einer Layout-Anfrage. */
export function layoutUrl(project: string, maxNodes: number = LAYOUT_NODE_BUDGET): string {
    const query = new URLSearchParams({ project, max_nodes: String(maxNodes) });
    return `${LAYOUT_ROUTE}?${query.toString()}`;
}

/**
 * Ein Verzeichnis der Knoten nach qualifiziertem Namen.
 *
 * Einmal je Datenstand gebaut und nicht je Suche: das Layout kommt als eine
 * Antwort und aendert sich erst mit dem naechsten Laden.
 *
 * Knoten ohne qualifizierten Namen fallen heraus. Der erste Knoten unter einem
 * Namen gewinnt; ein zweiter waere eine Doppelung im Index, und die stillschweigend
 * zu ueberschreiben hiesse, den Fokus mal hierhin und mal dorthin zu schicken.
 */
export function nodesByQualifiedName(nodes: readonly GraphNode[]): Map<string, GraphNode> {
    const index = new Map<string, GraphNode>();
    for (const node of nodes) {
        const qualifiedName = node.qualified_name;
        if (qualifiedName === undefined || qualifiedName.length === 0) {
            continue;
        }
        if (!index.has(qualifiedName)) {
            index.set(qualifiedName, node);
        }
    }
    return index;
}

/**
 * Der Knoten und seine direkten Nachbarn.
 *
 * Genau die Menge, die das Original hervorhebt und auf die es die Kamera
 * richtet. Die Menge enthaelt den Knoten selbst, auch wenn er keine Kante hat:
 * ein Symbol ohne Nachbarn ist trotzdem das, was gesucht wurde.
 */
export function neighbourIds(nodeId: number, edges: readonly GraphEdge[]): Set<number> {
    const ids = new Set<number>([nodeId]);
    for (const edge of edges) {
        if (edge.source === nodeId) {
            ids.add(edge.target);
        }
        if (edge.target === nodeId) {
            ids.add(edge.source);
        }
    }
    return ids;
}

/**
 * Ein Knoten als Navigationsziel, oder nichts.
 *
 * Nichts heisst: dieser Knoten hat keinen Ort im Quelltext, so wie die
 * EnvVar-, Projekt- und Ordner-Knoten, die in derselben Wolke liegen.
 *
 * Die Umrechnung selbst steht in src/twin/twin-target.ts und nicht hier: eine
 * zweite Fassung davon waere ein zweiter Weg, aus einem Pfad eine URI zu
 * machen, und die beiden liefen frueher oder spaeter auseinander. Die
 * Layout-`id` bleibt dabei absichtlich draussen; warum, steht dort.
 */
export function targetRefOfNode(node: GraphNode): SymbolRef | undefined {
    return twinTargetOf({
        name: node.name,
        qualifiedName: node.qualified_name,
        kind: symbolKindOf(node.label),
        filePath: node.file_path,
        startLine: node.start_line,
        endLine: node.end_line,
    });
}

/** Was das Panel ueber einen geladenen Stand sagt. */
export function layoutSummary(data: GraphData, budget: number = LAYOUT_NODE_BUDGET): string {
    const shown = data.nodes.length;
    const total = data.total_nodes;
    const head = `${shown} nodes, ${data.edges.length} edges from ${LAYOUT_ROUTE}`;
    if (total > shown) {
        return `${head}; ${total} nodes indexed, ${shown} fit the ${budget} node budget`;
    }
    return head;
}

/** Was das Panel sagt, wenn das gesuchte Symbol nicht in der Wolke liegt. */
export function missingNodeNote(name: string, budget: number = LAYOUT_NODE_BUDGET): string {
    return `${name} is not in the loaded layout (${budget} node budget)`;
}

/** Was das Panel sagt, wenn ein angeklickter Knoten keine Datei hat. */
export function unopenableNodeNote(node: GraphNode): string {
    return `${node.name} is a ${node.label} node and carries no file in the index, so there is nothing to open`;
}

/** Was das Panel sagt, solange kein Symbol im Fokus steht. */
export const GALAXY_NO_FOCUS_NOTE = 'no symbol in focus: the galaxy follows the twin';
