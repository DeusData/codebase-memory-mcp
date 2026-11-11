/**
 * Die uebrigen Beziehungen im Hierarchie-Bild (W9).
 *
 * Eine eigene Datei neben hierarchy-layout.test.ts, und das ist Absicht: dort
 * steht die Zusicherung, dass die Projektion aus dem Walk und NUR aus dem Walk
 * entsteht, und die soll durch diese Erweiterung nicht angefasst werden. Hier
 * steht, was daneben gezeichnet wird.
 *
 * Vier Fragen, die man beim Lesen sofort stellt, und ihre Antworten:
 *
 *  1. Wird eine Kante erfunden? Nein: gezeichnet wird nur, was in der geladenen
 *     Layout-Antwort steht, und nur zwischen Symbolen, die schon im Bild sind.
 *  2. Wird eine Kante doppelt gezeichnet? Nein: was der Walk schon gemalt hat,
 *     kommt nicht ein zweites Mal.
 *  3. Was passiert, wenn zwei Symbole mehr als eine Beziehung haben? Jede
 *     bekommt ihre eigene Spur, weil zwei Linien auf einem Strich sich additiv
 *     zu einer dritten Farbe mischen.
 *  4. Verschiebt das die Spalten? Nein, und das ist die eigentliche Zusicherung
 *     dieses Zyklus: die Projektion sieht diese Kanten gar nicht.
 */

import { describe, expect, it } from 'vitest';

import type { SymbolRef } from '../core/focus-protocol';
import { toEditorRange } from '../core/positions';
import type { ClosureNode, ClosureResult } from '../provider/closure';
import {
    HIERARCHY_LANE_SPACING,
    hierarchyEdgeNote,
    hierarchyHeadline,
    hierarchyIndexEdges,
    projectHierarchy,
} from './hierarchy-layout';
import type { GraphData } from './types';

const PROJECT = 'atlas';
const qn = (name: string): string => `${PROJECT}.${name}`;

const DECLARED: Record<string, { file: string; from: number; to: number }> = {
    createUser: { file: 'src/services/userService.ts', from: 23, to: 36 },
    validateUser: { file: 'src/util/validate.ts', from: 19, to: 31 },
    listUsers: { file: 'src/services/userService.ts', from: 18, to: 21 },
    ValidationError: { file: 'src/util/validate.ts', from: 4, to: 9 },
};

function symbolNamed(name: string, options: { qualified?: boolean } = {}): SymbolRef {
    const where = DECLARED[name] as { file: string; from: number; to: number };
    const symbol: SymbolRef = {
        name,
        kind: 'function',
        uri: `file:///workspace/${where.file}`,
        range: toEditorRange(where.from, where.to),
        selectionRange: toEditorRange(where.from, where.from),
    };
    if (options.qualified !== false) {
        symbol.qualifiedName = qn(name);
        symbol.nodeId = symbol.qualifiedName;
    }
    return symbol;
}

/** createUser ruft validateUser und listUsers; validateUser wirft ValidationError. */
function walk(overrides: Partial<ClosureResult> = {}): ClosureResult {
    const nodes: ClosureNode[] = [
        { symbol: symbolNamed('createUser'), hop: 0 },
        { symbol: symbolNamed('validateUser'), hop: 1, via: qn('createUser') },
        { symbol: symbolNamed('listUsers'), hop: 1, via: qn('createUser') },
        { symbol: symbolNamed('ValidationError'), hop: 2, via: qn('validateUser') },
    ];
    return {
        root: symbolNamed('createUser'),
        nodes,
        edges: [
            { from: qn('createUser'), to: qn('validateUser'), line: 24 },
            { from: qn('createUser'), to: qn('listUsers'), line: 29 },
            { from: qn('validateUser'), to: qn('ValidationError'), line: 21 },
        ],
        truncated: false,
        visited: 4,
        depth: 3,
        cap: 15,
        ...overrides,
    };
}

const layoutNode = (id: number, name: string, qualified: string | undefined) => ({
    id,
    x: id,
    y: id,
    z: 0,
    label: 'Function',
    name,
    size: 5,
    color: '#ffffff',
    ...(qualified === undefined ? {} : { qualified_name: qualified }),
});

/**
 * Das Layout: dieselben vier Symbole, plus eines, das im Walk nicht vorkommt.
 *
 * Die Kanten sind mit Absicht bunt gemischt: eine, die der Walk schon gemalt
 * hat (CALLS), zwei, die er nicht kennt (RAISES auf einem Paar, das schon eine
 * Linie hat, und USAGE auf einem Paar ohne), eine, die aus dem Bild
 * herausfuehrt, und eine ohne Typ.
 */
const LAYOUT: GraphData = {
    nodes: [
        layoutNode(1, 'createUser', qn('createUser')),
        layoutNode(2, 'validateUser', qn('validateUser')),
        layoutNode(3, 'listUsers', qn('listUsers')),
        layoutNode(4, 'ValidationError', qn('ValidationError')),
        layoutNode(5, 'somewhereElse', qn('somewhereElse')),
        layoutNode(6, 'nameless', undefined),
    ],
    edges: [
        { source: 1, target: 2, type: 'CALLS' },
        { source: 1, target: 2, type: 'RAISES' },
        { source: 3, target: 4, type: 'USAGE' },
        { source: 1, target: 5, type: 'IMPORTS' },
        { source: 1, target: 6, type: 'USAGE' },
        { source: 1, target: 3, type: '' },
    ],
    total_nodes: 6,
};

const projection = () => projectHierarchy(walk(), { layout: LAYOUT });

const named = (edges: ReturnType<typeof hierarchyIndexEdges>) =>
    edges.map((edge) => ({
        type: edge.type,
        from: projection().data.nodes[edge.source]?.name,
        to: projection().data.nodes[edge.target]?.name,
        offset: edge.offset ?? 0,
    }));

describe('hierarchyIndexEdges', () => {

    it('nimmt nur, was zwischen zwei gezeigten Symbolen liegt', () => {
        expect(named(hierarchyIndexEdges(projection(), LAYOUT))).toEqual([
            { type: 'RAISES', from: 'createUser', to: 'validateUser', offset: HIERARCHY_LANE_SPACING },
            { type: 'USAGE', from: 'listUsers', to: 'ValidationError', offset: 0 },
        ]);
    });

    it('zeichnet keine Kante zweimal, die der Walk schon gemalt hat', () => {
        const extras = hierarchyIndexEdges(projection(), LAYOUT);
        expect(extras.some((edge) => edge.type === 'CALLS')).toBe(false);
    });

    it('gibt der zweiten Beziehung eines Paares eine eigene Spur', () => {
        const extras = hierarchyIndexEdges(projection(), LAYOUT);
        const doubled = extras.find((edge) => edge.type === 'RAISES');
        const alone = extras.find((edge) => edge.type === 'USAGE');
        // Das Paar createUser/validateUser traegt schon die Aufrufkante.
        expect(doubled?.offset).toBe(HIERARCHY_LANE_SPACING);
        // Das Paar listUsers/ValidationError nicht: die Linie bleibt mittig.
        expect(alone?.offset).toBeUndefined();
    });

    it('zaehlt eine dritte Beziehung desselben Paares auf die naechste Spur', () => {
        const crowded: GraphData = {
            ...LAYOUT,
            edges: [
                ...LAYOUT.edges,
                { source: 1, target: 2, type: 'USAGE' },
                { source: 1, target: 2, type: 'CALL_REFERENCE' },
            ],
        };
        const offsets = hierarchyIndexEdges(projection(), crowded)
            .filter((edge) => edge.source === 0 && edge.target === 2)
            .map((edge) => edge.offset);
        expect(offsets).toEqual([
            HIERARCHY_LANE_SPACING,
            HIERARCHY_LANE_SPACING * 2,
            HIERARCHY_LANE_SPACING * 3,
        ]);
    });

    it('laesst eine Kante ohne Typ weg, statt sie unter einem erfundenen Namen zu fuehren', () => {
        expect(hierarchyIndexEdges(projection(), LAYOUT).every((edge) => edge.type.length > 0))
            .toBe(true);
    });

    it('gibt zweimal dieselbe Liste zurueck', () => {
        expect(JSON.stringify(hierarchyIndexEdges(projection(), LAYOUT)))
            .toBe(JSON.stringify(hierarchyIndexEdges(projection(), LAYOUT)));
    });

    it('kommt ohne Layout aus und liefert dann nichts', () => {
        expect(hierarchyIndexEdges(projection(), undefined)).toEqual([]);
    });

    it('liefert nichts, wenn kein Symbol einen qualifizierten Namen traegt', () => {
        const anonymous = walk();
        anonymous.nodes = [{ symbol: symbolNamed('createUser', { qualified: false }), hop: 0 }];
        anonymous.edges = [];
        anonymous.root = symbolNamed('createUser', { qualified: false });
        expect(hierarchyIndexEdges(projectHierarchy(anonymous), LAYOUT)).toEqual([]);
    });

    it('laesst die Spalten der Projektion unberuehrt', () => {
        // Die Probe auf die eigentliche Zusicherung: die Projektion bekommt das
        // Layout, rechnet aber nur mit dem Walk. Zwei Layouts mit ganz
        // verschiedenen Kanten geben dieselben Plaetze.
        const withoutEdges: GraphData = { ...LAYOUT, edges: [] };
        expect(JSON.stringify(projectHierarchy(walk(), { layout: withoutEdges }).placements))
            .toBe(JSON.stringify(projectHierarchy(walk(), { layout: LAYOUT }).placements));
    });
});

describe('hierarchyEdgeNote', () => {

    it('nennt, was aus dem Walk kommt und was dazugekommen ist', () => {
        expect(hierarchyEdgeNote(3, 2))
            .toBe('3 calls from the walk, 2 links from the index');
    });

    it('sagt "call" und "link" im Singular', () => {
        expect(hierarchyEdgeNote(1, 1)).toBe('1 call from the walk, 1 link from the index');
    });

    it('sagt auch die Null, statt zu schweigen', () => {
        expect(hierarchyEdgeNote(8, 0)).toBe('8 calls from the walk, 0 links from the index');
    });

    /*
     * Der Satz ueber dem Bild bleibt, wie er war.
     *
     * Ausdruecklich geprueft, weil es die Versuchung gab, die beiden Zahlen
     * dort anzuhaengen: der Beweislauf aus W4e liest diesen Satz bis zu seinem
     * Ende, und eine Zeile, an die immer noch etwas angehaengt wird, ist keine
     * Zusicherung mehr.
     */
    it('laesst die Kopfzeile unangetastet', () => {
        expect(hierarchyHeadline(projection()))
            .toBe('hierarchy of createUser: 4 symbols, depth 3');
        const capped = projectHierarchy(walk({ truncated: true, cap: 3 }), { layout: LAYOUT });
        expect(hierarchyHeadline(capped))
            .toBe('hierarchy of createUser: 4 symbols, depth 3; '
                + 'walk capped at 3 symbols (depth 3), so what follows this is not shown');
    });
});
