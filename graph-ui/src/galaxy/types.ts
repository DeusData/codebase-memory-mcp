/*
 * MIT License. Copyright (c) 2025 DeusData.
 *
 * Uebernommen am 2026-08-28 aus DeusData/codebase-memory-mcp, Branch
 * feat/atlas-r1, Datei graph-ui/src/lib/types.ts. Der Lizenztext und die Liste
 * aller uebernommenen Dateien stehen in THIRD_PARTY.md.
 *
 * Aenderungen gegenueber dem Original:
 *  - nur die Typen der Szene behalten: GraphNode, NodeStatus, GraphEdge,
 *    GraphData. Region, RegionEdge, RegionsPayload, Project, SchemaInfo,
 *    TabId, ProcessInfo und RepoInfo gehoeren zu Panels, die dieses Projekt
 *    nicht uebernimmt.
 *  - LinkedProject und MissedGraph entfernt, samt der beiden Felder in
 *    GraphData. Die Szene hier zeigt eine Galaxie und keine Satelliten; die
 *    Zweige, die sie zeichnen wuerden, sind in GraphScene.tsx ebenfalls
 *    gestrichen.
 *  - GraphEdge hat seit W9 ein optionales Feld `offset`. Begruendung an dem
 *    Feld selbst und im Kopf von EdgeLines.tsx.
 */

/* Graph data types matching the C layout3d.c JSON output */

export interface GraphNode {
    id: number;
    x: number;
    y: number;
    z: number;
    label: string;
    name: string;
    file_path?: string;
    qualified_name?: string;
    start_line?: number;
    end_line?: number;
    size: number;
    color: string;
    /* Dead-code classification from the backend layout (layout3d.c). */
    status?: NodeStatus;
    in_calls?: number;
    out_calls?: number;
}

export type NodeStatus =
    | 'dead'
    | 'single'
    | 'entry'
    | 'test'
    | 'exported'
    | 'normal'
    | 'structural';

export interface GraphEdge {
    source: number;
    target: number;
    type: string;
    /**
     * Seitlicher Versatz beim Zeichnen, in Welteinheiten (W9, neu).
     *
     * Zwei Symbole koennen mehr als eine Beziehung haben. Ohne Versatz liegt
     * die zweite Linie auf der ersten und mischt sich additiv zu einer Farbe,
     * die keine Legende kennt. Die Layout-Antwort des Servers traegt dieses
     * Feld nicht; gesetzt wird es nur von der Hierarchie-Projektion.
     */
    offset?: number;
}

export interface GraphData {
    nodes: GraphNode[];
    edges: GraphEdge[];
    total_nodes: number;
}
