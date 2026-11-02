/**
 * Wo im Graphen ein Ereignis liegt, und wie sicher das ist.
 *
 * Ein Ereignis nennt einen Pfad und manchmal einen Zeilenbereich. Der Graph
 * kennt Knoten mit `file_path`, `start_line` und `end_line`. Dieses Modul
 * bringt beides zusammen und macht dabei drei Unterschiede, die die Oberflaeche
 * dann auch zeigt:
 *
 * 1. **Datei plus Bereich ist genau.** Getroffen werden die Knoten, deren
 *    Bereich sich mit dem des Ereignisses ueberschneidet; bei mehreren gewinnt
 *    der ENGSTE. Der Modulknoten umspannt die ganze Datei und ist damit fast
 *    immer einer der Treffer; er zu gewinnen hiesse, jede Aenderung auf "irgendwo
 *    in dieser Datei" zu runden.
 * 2. **Nur Datei ist die Datei.** Ohne Bereich gibt es nichts Engeres zu
 *    treffen, also trifft es den Modulknoten. Das ist keine Naeherung, sondern
 *    genau das, was das Ereignis sagt.
 * 3. **Ein Knoten ohne Endzeile ist unsicher.** Ordner- und Dateiknoten tragen
 *    keinen Bereich. Man kann an ihnen zeigen, wo etwas passiert ist, aber nicht
 *    behaupten, das Ereignis liege in ihrem Bereich, denn sie haben keinen. Der
 *    Treffer wird darum als unsicher gekennzeichnet und nicht als Treffer
 *    verkauft.
 *
 * Was gar nicht geht, bleibt sichtbar: `kind: 'none'` traegt den Grund, und das
 * Instrument zeigt das Rohereignis. Ein Ereignis, das aus der Ansicht faellt,
 * weil der Index seinen Pfad nicht kennt, waere die stille Behauptung, es habe
 * nicht stattgefunden.
 */

import type { GraphNode } from '../galaxy/types';
import type { AgentEvent, WorkKind } from './agent-event';

/** Wie genau ein Ereignis verortet ist. */
export type PlacementKind =
    /** Datei plus Zeilenbereich, auf dem engsten passenden Knoten. */
    | 'range'
    /** Nur die Datei; der Knoten ist ihr Modul. */
    | 'file'
    /** Nirgends: der Index kennt diesen Pfad nicht, oder es gibt keinen. */
    | 'none';

/** Wo ein Ereignis liegt. */
export interface Placement {
    kind: PlacementKind;
    /** Die Layout-Kennung des getroffenen Knotens. Fehlt bei `none`. */
    nodeId?: number;
    /** Der Name, den das Instrument zeigt. */
    name: string;
    /** Der qualifizierte Name, wenn der Knoten einen traegt. */
    qualifiedName: string;
    /**
     * Ob der getroffene Knoten seinen Bereich nicht nennt.
     *
     * Dann steht der Ort, aber nicht die Aussage "das Ereignis liegt darin".
     */
    uncertain: boolean;
    /** Warum es nicht weiter ging. Leer, wenn es weiter ging. */
    why: string;
    /**
     * Weitere Knoten, die dasselbe Ereignis beruehrt.
     *
     * Nur beim Suchen, und nur solche, deren NAME das Suchmuster enthaelt. Das
     * ist eine Ablesung am Index und keine Vermutung darueber, was die Suche
     * wirklich gelesen hat: was ein `grep` an Dateien angefasst hat, weiss
     * dieses Fenster nicht, und ein Ping an geratenen Stellen waere eine
     * Behauptung ueber fremde Arbeit.
     */
    ghostIds: readonly number[];
    /**
     * Der geprueften Bereich eines Testlaufs.
     *
     * Gefuellt, wenn der Befehl eine Datei nennt, die der Graph kennt. Fehlt er,
     * gibt es keine gestrichelte Verbindung, und das Instrument sagt, dass der
     * Befehl keine bekannte Datei genannt hat.
     */
    testedNodeId?: number;
}

/** Der Index, den dieses Modul braucht: Knoten je Datei und die Namensliste. */
export interface PlacementIndex {
    /** Je repo-relativem Pfad die Knoten, die ihn tragen. */
    byPath: ReadonlyMap<string, readonly GraphNode[]>;
    /** Alle Knoten mit einer Datei, fuer die Suche nach Namen. */
    named: readonly GraphNode[];
}

/** Der Etikettwert, den der Server einem Modulknoten gibt. */
export const MODULE_LABEL = 'Module';

/** Hoechstens so viele Ghost-Pings je Suchereignis. */
export const GHOST_LIMIT = 6;

/** Ein Pfad ohne fuehrende Punkte und Schraegstriche, mit `/` als Trenner. */
export function normalizePath(path: string): string {
    let value = path.trim().replace(/\\/g, '/');
    while (value.startsWith('./')) {
        value = value.slice(2);
    }
    while (value.startsWith('/')) {
        value = value.slice(1);
    }
    while (value.endsWith('/') && value.length > 1) {
        value = value.slice(0, -1);
    }
    return value;
}

/** Den Index einmal bauen. */
export function buildPlacementIndex(nodes: readonly GraphNode[]): PlacementIndex {
    const byPath = new Map<string, GraphNode[]>();
    const named: GraphNode[] = [];
    for (const node of nodes) {
        const path = normalizePath(node.file_path ?? '');
        if (path.length === 0) {
            continue;
        }
        const bucket = byPath.get(path);
        if (bucket === undefined) {
            byPath.set(path, [node]);
        } else {
            bucket.push(node);
        }
        named.push(node);
    }
    return { byPath, named };
}

/** Wie viele Zeilen ein Knoten umspannt. Unbekannt heisst: unendlich weit. */
function spanOf(node: GraphNode): number {
    if (node.start_line === undefined || node.end_line === undefined) {
        return Number.POSITIVE_INFINITY;
    }
    return node.end_line - node.start_line;
}

function overlaps(node: GraphNode, from: number, to: number): boolean {
    if (node.start_line === undefined || node.end_line === undefined) {
        return false;
    }
    return node.start_line <= to && node.end_line >= from;
}

/** Der Modulknoten dieser Datei, sonst der Knoten, der die Datei am ehesten ist. */
function fileNodeOf(nodes: readonly GraphNode[]): GraphNode | undefined {
    const module = nodes.find((node) => node.label === MODULE_LABEL);
    if (module !== undefined) {
        return module;
    }
    // Sonst der weiteste: ein Datei- oder Ordnerknoten ohne Bereich zaehlt als
    // unendlich weit und gewinnt damit gegen ein einzelnes Symbol darin.
    return [...nodes].sort((a, b) => spanOf(b) - spanOf(a))[0];
}

function displayNameOf(node: GraphNode): string {
    return node.name.length > 0 ? node.name : node.qualified_name ?? String(node.id);
}

/** Die Knoten, deren Name das Suchmuster enthaelt. Hoechstens {@link GHOST_LIMIT}. */
export function ghostsFor(pattern: string, index: PlacementIndex): number[] {
    const needle = pattern.trim().toLowerCase();
    if (needle.length < 2) {
        return [];
    }
    const hits: GraphNode[] = [];
    for (const node of index.named) {
        if (node.name.toLowerCase().includes(needle)) {
            hits.push(node);
        }
    }
    return hits
        .sort((a, b) => a.name.length - b.name.length || a.id - b.id)
        .slice(0, GHOST_LIMIT)
        .map((node) => node.id);
}

/**
 * Die Datei, die ein Befehl nennt und die der Graph kennt.
 *
 * Der Befehl wird in Wortteile zerlegt und jeder gegen den Index gehalten. Kein
 * Raten an Endungen, kein Teilpfad: entweder der Index kennt genau dieses Wort
 * als Pfad, oder der Befehl hat keine bekannte Datei genannt.
 */
export function testedPathOf(command: string, index: PlacementIndex): string {
    for (const token of command.split(/[\s'"`;,()]+/)) {
        const candidate = normalizePath(token);
        if (candidate.length > 0 && index.byPath.has(candidate)) {
            return candidate;
        }
    }
    return '';
}

/** Wo dieses Ereignis liegt. */
export function placeEvent(
    event: AgentEvent,
    kind: WorkKind,
    index: PlacementIndex,
): Placement {
    const empty = { ghostIds: [] as number[] };
    const path = normalizePath(event.path);

    if (kind === 'test') {
        const tested = testedPathOf(event.detail, index);
        if (tested.length > 0) {
            const node = fileNodeOf(index.byPath.get(tested) ?? []);
            if (node !== undefined) {
                return {
                    kind: 'file',
                    nodeId: node.id,
                    name: displayNameOf(node),
                    qualifiedName: node.qualified_name ?? '',
                    uncertain: node.end_line === undefined,
                    why: '',
                    ghostIds: [],
                    testedNodeId: node.id,
                };
            }
        }
        if (path.length === 0) {
            return {
                kind: 'none',
                name: '',
                qualifiedName: '',
                uncertain: false,
                why: 'the command names no file this index knows',
                ...empty,
            };
        }
    }

    if (path.length === 0) {
        return {
            kind: 'none',
            name: '',
            qualifiedName: '',
            uncertain: false,
            why: 'the event names no path',
            ...empty,
        };
    }

    const nodes = index.byPath.get(path);
    if (nodes === undefined || nodes.length === 0) {
        return {
            kind: 'none',
            name: '',
            qualifiedName: '',
            uncertain: false,
            why: 'the index has no node for this path',
            ...empty,
        };
    }

    const ghostIds = kind === 'search' ? ghostsFor(event.detail, index) : [];

    if (event.lines !== undefined) {
        const [from, to] = event.lines;
        const hits = nodes.filter((node) => overlaps(node, from, to));
        if (hits.length > 0) {
            const best = [...hits].sort((a, b) => spanOf(a) - spanOf(b) || a.id - b.id)[0] as GraphNode;
            return {
                kind: 'range',
                nodeId: best.id,
                name: displayNameOf(best),
                qualifiedName: best.qualified_name ?? '',
                uncertain: false,
                why: '',
                ghostIds,
            };
        }
    }

    const node = fileNodeOf(nodes);
    if (node === undefined) {
        return {
            kind: 'none',
            name: '',
            qualifiedName: '',
            uncertain: false,
            why: 'the index has no node for this path',
            ghostIds,
        };
    }
    return {
        kind: 'file',
        nodeId: node.id,
        name: displayNameOf(node),
        qualifiedName: node.qualified_name ?? '',
        uncertain: node.end_line === undefined,
        why: '',
        ghostIds,
    };
}
