/*
 * Die Uebersetzung zwischen der Sprache der Szene (IDs, Punkte) und der des
 * Produkts (qualifizierte Namen, Dateien, Zeilen).
 *
 * Drei Gruppen, und jede prueft eine Stelle, an der ein Fehler sich als
 * Funktion tarnt: ein Verzeichnis, das den falschen Knoten zurueckgibt, eine
 * Nachbarschaft, die eine Richtung vergisst, und ein Navigationsziel, das eine
 * Datei erfindet, die es nicht gibt.
 */

import { describe, expect, it } from 'vitest';

import {
    GALAXY_NO_FOCUS_NOTE,
    LAYOUT_NODE_BUDGET,
    LAYOUT_ROUTE,
    layoutSummary,
    layoutUrl,
    missingNodeNote,
    neighbourIds,
    nodesByQualifiedName,
    targetRefOfNode,
    unopenableNodeNote,
} from './galaxy-model';
import type { GraphData, GraphNode } from './types';

function node(id: number, overrides: Partial<GraphNode> = {}): GraphNode {
    return {
        id,
        x: id,
        y: 0,
        z: 0,
        label: 'Function',
        name: `n${id}`,
        size: 4,
        color: '#ffffff',
        ...overrides,
    };
}

/** Ein Ausschnitt in der Form, die /api/layout wirklich liefert. */
const NODES: GraphNode[] = [
    node(51, {
        name: 'createUser',
        qualified_name: 'atlas.src.services.userService.createUser',
        file_path: 'src/services/userService.ts',
        start_line: 23,
        end_line: 36,
        in_calls: 2,
    }),
    node(69, {
        name: 'validateUser',
        qualified_name: 'atlas.src.util.validate.validateUser',
        file_path: 'src/util/validate.ts',
        start_line: 19,
        end_line: 31,
    }),
    node(74, { name: 'DB_URL', label: 'EnvVar', qualified_name: '__env__DB_URL', file_path: '' }),
    node(10, { name: 'userService', label: 'Module', qualified_name: 'atlas.src.services.userService', file_path: 'src/services/userService.ts' }),
];

const EDGES = [
    { source: 10, target: 51, type: 'DEFINES' },
    { source: 51, target: 69, type: 'CALLS' },
    { source: 51, target: 74, type: 'CONFIGURES' },
    { source: 69, target: 74, type: 'CONFIGURES' },
];

describe('nodesByQualifiedName', () => {

    it('findet einen Knoten unter genau dem Namen, den auch der Twin fuehrt', () => {
        const index = nodesByQualifiedName(NODES);
        expect(index.get('atlas.src.services.userService.createUser')?.id).toBe(51);
        expect(index.get('atlas.src.util.validate.validateUser')?.id).toBe(69);
    });

    it('laesst Knoten ohne qualifizierten Namen weg, statt sie unter leer zu fuehren', () => {
        const index = nodesByQualifiedName([...NODES, node(99, { qualified_name: '' })]);
        expect(index.has('')).toBe(false);
        expect(index.size).toBe(4);
    });

    it('behaelt bei einer Doppelung den ersten, damit der Fokus nicht springt', () => {
        const twice = [
            node(1, { qualified_name: 'a.b.c' }),
            node(2, { qualified_name: 'a.b.c' }),
        ];
        expect(nodesByQualifiedName(twice).get('a.b.c')?.id).toBe(1);
    });
});

describe('neighbourIds', () => {

    it('nimmt beide Richtungen und den Knoten selbst', () => {
        expect([...neighbourIds(51, EDGES)].sort((a, b) => a - b)).toEqual([10, 51, 69, 74]);
    });

    it('gibt einen Knoten ohne Kanten allein zurueck, statt ihn zu verlieren', () => {
        expect([...neighbourIds(4242, EDGES)]).toEqual([4242]);
    });

    it('geht nur einen Schritt weit', () => {
        // 10 ist ueber 51 mit 69 verbunden, aber nicht direkt.
        expect(neighbourIds(10, EDGES).has(69)).toBe(false);
    });
});

describe('targetRefOfNode', () => {

    it('macht aus einem Knoten mit Datei ein Ziel, das der Twin oeffnen kann', () => {
        const target = targetRefOfNode(NODES[0]);
        expect(target?.uri).toBe('file:///workspace/src/services/userService.ts');
        expect(target?.name).toBe('createUser');
        expect(target?.qualifiedName).toBe('atlas.src.services.userService.createUser');
        expect(target?.kind).toBe('function');
        // 1-basierte Graphzeile 23 wird zur 0-basierten Editorzeile 22.
        expect(target?.selectionRange?.start.line).toBe(22);
    });

    it('gibt fuer einen Knoten ohne Datei kein Ziel zurueck', () => {
        expect(targetRefOfNode(NODES[2])).toBeUndefined();
    });

    it('traegt keine nodeId, weil die Layout-Zahl nicht die Knoten-Identitaet ist', () => {
        expect(targetRefOfNode(NODES[0])?.nodeId).toBeUndefined();
    });

    it('oeffnet einen Knoten ohne Zeile am Anfang der Datei', () => {
        const target = targetRefOfNode(NODES[3]);
        expect(target?.selectionRange?.start.line).toBe(0);
    });
});

describe('die Saetze, die das Panel sagt', () => {

    it('nennt die Route und den Deckel', () => {
        expect(layoutUrl('atlas')).toBe(`${LAYOUT_ROUTE}?project=atlas&max_nodes=${LAYOUT_NODE_BUDGET}`);
        expect(layoutUrl('atlas', 20)).toContain('max_nodes=20');
    });

    it('sagt bei einem gekappten Layout, dass es gekappt ist', () => {
        const data: GraphData = { nodes: NODES, edges: EDGES, total_nodes: 4 };
        expect(layoutSummary(data)).toBe(`4 nodes, 4 edges from ${LAYOUT_ROUTE}`);
        const capped: GraphData = { nodes: NODES, edges: EDGES, total_nodes: 9000 };
        expect(layoutSummary(capped, 4)).toContain('9000 nodes indexed, 4 fit the 4 node budget');
    });

    it('sagt bei einem fehlenden Knoten, warum er fehlen kann', () => {
        expect(missingNodeNote('createUser')).toBe(
            `createUser is not in the loaded layout (${LAYOUT_NODE_BUDGET} node budget)`,
        );
    });

    it('sagt bei einem Knoten ohne Datei, dass es nichts zu oeffnen gibt', () => {
        expect(unopenableNodeNote(NODES[2])).toContain('EnvVar');
        expect(unopenableNodeNote(NODES[2])).toContain('nothing to open');
    });

    it('haelt einen Satz fuer die Lage bereit, in der nichts im Fokus steht', () => {
        expect(GALAXY_NO_FOCUS_NOTE).toContain('follows the twin');
    });
});
