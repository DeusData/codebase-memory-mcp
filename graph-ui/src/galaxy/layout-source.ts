/**
 * Woher die Galaxie ihre Punkte bekommt.
 *
 * `/api/layout` ist die einzige Route, die ein fertiges 3D-Layout liefert: die
 * Koordinaten, die Groessen und die Farben rechnet der C-Server (layout3d.c),
 * nicht der Browser. Es gibt hier also keine Kraeftesimulation und keinen
 * zweiten Layout-Begriff, und das ist der Grund, warum die uebernommene Szene
 * ueberhaupt so klein sein kann.
 *
 * Das Vorbild (graph-ui/src/hooks/useGraphData.ts) liest den Rumpf stueckweise
 * mit, um einen Fortschritt anzuzeigen. Das ist hier weggelassen: der Deckel
 * liegt bei 5000 Knoten, das sind wenige hundert Kilobyte, und ein
 * Fortschrittsbalken, der immer sofort voll ist, ist eine Anzeige, die nichts
 * anzeigt.
 *
 * Was NICHT weggelassen ist: der Fehlerfall. Eine Galaxie, die bei einem 500er
 * einfach leer bleibt, behauptet, das Projekt habe keine Knoten. Also traegt
 * jeder Fehlweg hier seinen Text, und das Panel zeigt ihn.
 */

import { layoutUrl, LAYOUT_NODE_BUDGET, LAYOUT_ROUTE } from './galaxy-model';
import type { GraphData, GraphEdge, GraphNode, NodeStatus } from './types';

export interface LayoutSourceOptions {
    /** Ursprung des Servers, ohne Schraegstrich am Ende. Leer heisst same-origin. */
    base?: string;
    /** Ersetzbares fetch, damit Tests ohne Netz laufen. */
    fetch?: typeof globalThis.fetch;
    /** Knotendeckel der Anfrage. */
    maxNodes?: number;
    /** Abbruchsignal. */
    signal?: AbortSignal;
}

/** Ein Fehler der Layout-Route, mit dem Status, den sie geliefert hat. */
export class LayoutError extends Error {
    constructor(readonly status: number, message: string) {
        super(message);
        this.name = 'LayoutError';
    }
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function num(value: unknown): number | undefined {
    if (typeof value === 'number' && Number.isFinite(value)) {
        return value;
    }
    if (typeof value === 'string' && value.trim().length > 0) {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : undefined;
    }
    return undefined;
}

function text(value: unknown): string | undefined {
    return typeof value === 'string' && value.length > 0 ? value : undefined;
}

/**
 * Die Antwort der Route als Szenendaten.
 *
 * Streng gegenueber dem, was die Szene zum Zeichnen braucht (Identitaet, Ort,
 * Groesse, Farbe): fehlt eines davon, faellt der Knoten weg, denn ein Knoten
 * ohne Ort waere ein Punkt im Ursprung, den niemand dort erwartet. Nachsichtig
 * gegenueber allem anderen: eine fehlende Datei, ein fehlender qualifizierter
 * Name und eine fehlende Zeile bleiben leer statt erfunden.
 *
 * Kanten auf Knoten, die nicht in der Antwort stehen, bleiben drin: die Szene
 * laesst sie beim Zeichnen selbst fallen (EdgeLines), und sie hier zu filtern
 * waere dieselbe Regel an zwei Stellen.
 */
export function readGraphData(value: unknown): GraphData {
    const raw = isRecord(value) ? value : {};
    const nodes: GraphNode[] = [];
    for (const entry of Array.isArray(raw['nodes']) ? raw['nodes'] : []) {
        if (!isRecord(entry)) {
            continue;
        }
        const id = num(entry['id']);
        const x = num(entry['x']);
        const y = num(entry['y']);
        const z = num(entry['z']);
        const size = num(entry['size']);
        const color = text(entry['color']);
        const name = text(entry['name']);
        if (id === undefined || x === undefined || y === undefined || z === undefined) {
            continue;
        }
        nodes.push({
            id,
            x,
            y,
            z,
            label: text(entry['label']) ?? '',
            name: name ?? String(id),
            file_path: text(entry['file_path']),
            qualified_name: text(entry['qualified_name']),
            start_line: num(entry['start_line']),
            end_line: num(entry['end_line']),
            size: size !== undefined && size > 0 ? size : 1,
            color: color ?? '#8899aa',
            status: text(entry['status']) as NodeStatus | undefined,
            in_calls: num(entry['in_calls']),
            out_calls: num(entry['out_calls']),
        });
    }

    const edges: GraphEdge[] = [];
    for (const entry of Array.isArray(raw['edges']) ? raw['edges'] : []) {
        if (!isRecord(entry)) {
            continue;
        }
        const source = num(entry['source']);
        const target = num(entry['target']);
        if (source === undefined || target === undefined) {
            continue;
        }
        edges.push({ source, target, type: text(entry['type']) ?? '' });
    }

    return { nodes, edges, total_nodes: num(raw['total_nodes']) ?? nodes.length };
}

/**
 * Das Layout eines Projekts holen.
 *
 * Wirft einen `LayoutError` mit Status 0, wenn niemand geantwortet hat, und
 * mit dem HTTP-Status, wenn der Server nein gesagt hat. Beide Texte nennen die
 * Route, weil ein Fehler ohne Adresse den Leser raten laesst, welche der zwei
 * Flaechen dieses Servers gerade schweigt.
 */
export async function loadLayout(
    project: string,
    options: LayoutSourceOptions = {},
): Promise<GraphData> {
    const route = layoutUrl(project, options.maxNodes ?? LAYOUT_NODE_BUDGET);
    const url = `${options.base ?? ''}${route}`;
    const doFetch = options.fetch ?? globalThis.fetch;
    let response: Response;
    try {
        response = await doFetch(url, {
            headers: { Accept: 'application/json' },
            ...(options.signal !== undefined ? { signal: options.signal } : {}),
        });
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new LayoutError(0, `${LAYOUT_ROUTE} war nicht erreichbar: ${message}`);
    }
    const body = await response.text();
    if (!response.ok) {
        throw new LayoutError(
            response.status,
            `${LAYOUT_ROUTE} antwortete mit HTTP ${response.status}: ${body.slice(0, 200)}`,
        );
    }
    let parsed: unknown;
    try {
        parsed = JSON.parse(body) as unknown;
    } catch {
        throw new LayoutError(response.status, `${LAYOUT_ROUTE} lieferte kein JSON: ${body.slice(0, 200)}`);
    }
    return readGraphData(parsed);
}
