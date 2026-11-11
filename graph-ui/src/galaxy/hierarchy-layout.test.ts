/**
 * Die Projektion des Vorwaerts-Walks, an einem Walk mit Zyklus und Deckel.
 *
 * Was hier bewiesen wird und im Browser nicht zu beweisen waere: dass zweimal
 * dasselbe Bild herauskommt, dass die Spalte wirklich am Hop haengt und nicht
 * an der Reihenfolge der Eingabe, dass die Kante, die einen Zyklus schliesst,
 * gezeichnet wird statt versteckt, und dass Farbe und Groesse vom Server
 * kommen, wo es sie gibt, und sonst neutral bleiben. Ein WebGL-Kontext kommt
 * hier nicht vor: die Projektion ist eine Funktion ueber Daten.
 */

import { describe, expect, it } from 'vitest';

import type { SymbolRef } from '../core/focus-protocol';
import { toEditorRange } from '../core/positions';
import type { ClosureEdge, ClosureNode, ClosureResult } from '../provider/closure';
import {
    HIERARCHY_COLUMN_WIDTH,
    HIERARCHY_DEFAULT_COLOR,
    HIERARCHY_DEFAULT_SIZE,
    HIERARCHY_LABEL_PAD_BOTTOM,
    HIERARCHY_LABEL_PAD_TOP,
    HIERARCHY_LABEL_PAD_X,
    HIERARCHY_MAX_SIZE,
    HIERARCHY_MIN_SIZE,
    HIERARCHY_ROW_HEIGHT,
    HIERARCHY_SIZE_SCALE,
    hierarchyFrame,
    hierarchyHeadline,
    projectHierarchy,
} from './hierarchy-layout';
import type { GraphData } from './types';

const PROJECT = 'atlas';

/** Wo die Symbole des Beispiels deklariert sind. */
const DECLARED: Record<string, { file: string; from: number; to: number }> = {
    createUser: { file: 'src/services/userService.ts', from: 23, to: 36 },
    validateUser: { file: 'src/util/validate.ts', from: 19, to: 31 },
    listUsers: { file: 'src/services/userService.ts', from: 18, to: 21 },
    insert: { file: 'src/repo/db.ts', from: 31, to: 40 },
    query: { file: 'src/repo/db.ts', from: 17, to: 25 },
    toUser: { file: 'src/services/userService.ts', from: 9, to: 16 },
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
        symbol.qualifiedName = `${PROJECT}.${name}`;
        symbol.nodeId = symbol.qualifiedName;
    }
    return symbol;
}

const qn = (name: string): string => `${PROJECT}.${name}`;

/**
 * Der Beispiel-Walk: createUser erreicht drei Symbole, eines davon erreicht
 * zwei weitere, und eines davon ruft die Wurzel zurueck an. Die Ebene 1 ist
 * absichtlich NICHT alphabetisch aufgeschrieben, damit die Ordnung im Bild die
 * Leistung der Projektion ist und nicht die der Eingabe.
 */
function walk(overrides: Partial<ClosureResult> = {}): ClosureResult {
    const nodes: ClosureNode[] = [
        { symbol: symbolNamed('createUser'), hop: 0 },
        { symbol: symbolNamed('validateUser'), hop: 1, via: qn('createUser') },
        { symbol: symbolNamed('listUsers'), hop: 1, via: qn('createUser') },
        { symbol: symbolNamed('insert'), hop: 1, via: qn('createUser') },
        { symbol: symbolNamed('query'), hop: 2, via: qn('listUsers') },
        { symbol: symbolNamed('toUser'), hop: 2, via: qn('listUsers') },
    ];
    const edges: ClosureEdge[] = [
        { from: qn('createUser'), to: qn('validateUser'), line: 24 },
        { from: qn('createUser'), to: qn('listUsers'), line: 29 },
        { from: qn('createUser'), to: qn('insert'), line: 30 },
        { from: qn('listUsers'), to: qn('query'), line: 19 },
        { from: qn('listUsers'), to: qn('toUser'), line: 20 },
        // Die Kante, die den Zyklus schliesst: zurueck in die Wurzel.
        { from: qn('toUser'), to: qn('createUser'), line: 12 },
    ];
    return {
        root: symbolNamed('createUser'),
        nodes,
        edges,
        truncated: false,
        visited: 6,
        depth: 3,
        cap: 15,
        ...overrides,
    };
}

/** Ein Layout, das zwei der sechs Symbole kennt und ein fremdes dazu. */
const LAYOUT: GraphData = {
    nodes: [
        {
            id: 51, x: -244, y: -453, z: -46, label: 'Function', name: 'createUser',
            file_path: 'src/services/userService.ts', qualified_name: qn('createUser'),
            start_line: 23, end_line: 36, size: 7.6, color: '#ffe080', in_calls: 2, out_calls: 4,
            status: 'exported',
        },
        {
            id: 69, x: -143, y: -599, z: -43, label: 'Function', name: 'query',
            file_path: 'src/repo/db.ts', qualified_name: qn('query'),
            start_line: 17, end_line: 25, size: 5.5, color: '#ffa060',
        },
        {
            id: 74, x: 12, y: 8, z: 3, label: 'EnvVar', name: 'DB_URL',
            qualified_name: '__env__DB_URL', size: 3, color: '#88aaff',
        },
    ],
    edges: [{ source: 51, target: 69, type: 'CALLS' }],
    total_nodes: 76,
};

const nodeNamed = (projection: ReturnType<typeof projectHierarchy>, name: string) =>
    projection.data.nodes.find((node) => node.name === name);

describe('projectHierarchy', () => {

    it('baut zweimal dasselbe Bild', () => {
        const first = projectHierarchy(walk(), { layout: LAYOUT });
        const second = projectHierarchy(walk(), { layout: LAYOUT });
        expect(JSON.stringify(second)).toBe(JSON.stringify(first));
    });

    it('setzt jede Spalte auf ihren Hop, die Wurzel ganz links', () => {
        const projection = projectHierarchy(walk());
        for (const placement of projection.placements) {
            const node = projection.data.nodes[placement.id];
            expect(node?.x).toBe(placement.hop * HIERARCHY_COLUMN_WIDTH);
        }
        // Drei Ebenen, also drei verschiedene x-Werte, und keine zwei Hops
        // teilen sich eine Spalte.
        const columns = new Map<number, Set<number>>();
        for (const placement of projection.placements) {
            const seen = columns.get(placement.hop) ?? new Set<number>();
            seen.add(placement.x);
            columns.set(placement.hop, seen);
        }
        expect([...columns.keys()].sort()).toEqual([0, 1, 2]);
        for (const seen of columns.values()) {
            expect(seen.size).toBe(1);
        }
        const xs = projection.data.nodes.map((node) => node.x);
        expect(Math.min(...xs)).toBe(0);
        expect(projection.data.nodes[projection.rootId]?.name).toBe('createUser');
        expect(projection.data.nodes[projection.rootId]?.x).toBe(0);
        expect(projection.rootKey).toBe(qn('createUser'));
        expect(projection.depth).toBe(3);
        expect(projection.symbols).toBe(6);
    });

    it('ordnet eine Ebene ordinal nach Namen und zentriert sie', () => {
        const projection = projectHierarchy(walk());
        const level = projection.placements
            .filter((placement) => placement.hop === 1)
            .map((placement) => placement.name);
        expect(level).toEqual(['insert', 'listUsers', 'validateUser']);
        const ys = projection.placements
            .filter((placement) => placement.hop === 1)
            .map((placement) => placement.y);
        expect(ys).toEqual([HIERARCHY_ROW_HEIGHT, 0, -HIERARCHY_ROW_HEIGHT]);
        // Die Wurzel steht allein in ihrer Spalte und damit auf der Mittellinie.
        expect(projection.placements[0]?.y).toBe(0);
        // Flach, damit die Spalte eine Spalte bleibt und keine Wolke.
        expect(projection.data.nodes.every((node) => node.z === 0)).toBe(true);
    });

    it('behaelt jede Kante, auch die, die den Zyklus schliesst', () => {
        const source = walk();
        const projection = projectHierarchy(source);
        expect(projection.data.edges.length).toBe(source.edges.length);
        const named = projection.data.edges.map((edge) => [
            projection.data.nodes[edge.source]?.name,
            projection.data.nodes[edge.target]?.name,
        ]);
        expect(named).toContainEqual(['toUser', 'createUser']);
        expect(named).toContainEqual(['createUser', 'validateUser']);
        expect(projection.data.edges.every((edge) => edge.type === 'CALLS')).toBe(true);
    });

    it('laesst eine Kante ins Nichts weg, statt sie an einen erfundenen Punkt zu haengen', () => {
        const source = walk();
        source.edges = [...source.edges, { from: qn('createUser'), to: qn('nowhere') }];
        const projection = projectHierarchy(source);
        expect(projection.data.edges.length).toBe(source.edges.length - 1);
    });

    it('nimmt Farbe, Groesse und Label des Servers, wo das Layout das Symbol kennt', () => {
        const projection = projectHierarchy(walk(), { layout: LAYOUT });
        const root = nodeNamed(projection, 'createUser');
        expect(root?.color).toBe('#ffe080');
        // Das Verhaeltnis ist das des Servers, die Einheit die dieses Bildes.
        expect(root?.size).toBe(7.6 * HIERARCHY_SIZE_SCALE);
        expect(root?.size).toBe((nodeNamed(projection, 'query')?.size ?? 0) * (7.6 / 5.5));
        expect(root?.label).toBe('Function');
        expect(root?.status).toBe('exported');
        expect(root?.in_calls).toBe(2);
        expect(root?.out_calls).toBe(4);
    });

    it('bleibt neutral, wo das Layout das Symbol nicht kennt', () => {
        const projection = projectHierarchy(walk(), { layout: LAYOUT });
        const unknown = nodeNamed(projection, 'listUsers');
        expect(unknown?.color).toBe(HIERARCHY_DEFAULT_COLOR);
        expect(unknown?.size).toBe(HIERARCHY_DEFAULT_SIZE * HIERARCHY_SIZE_SCALE);
        expect(unknown?.label).toBe('function');
        expect(unknown?.status).toBeUndefined();
        expect(unknown?.in_calls).toBeUndefined();
    });

    it('kommt ohne geladenes Layout aus', () => {
        const projection = projectHierarchy(walk());
        expect(projection.data.nodes.every((node) => node.color === HIERARCHY_DEFAULT_COLOR)).toBe(true);
        expect(projection.symbols).toBe(6);
    });

    it('traegt Datei und Zeile aus dem Walk, damit ein Klick etwas oeffnen kann', () => {
        const projection = projectHierarchy(walk(), { layout: LAYOUT });
        const validate = nodeNamed(projection, 'validateUser');
        expect(validate?.file_path).toBe('src/util/validate.ts');
        expect(validate?.start_line).toBe(19);
        expect(validate?.end_line).toBe(31);
        expect(validate?.qualified_name).toBe(qn('validateUser'));
    });

    it('gibt einem Symbol ohne qualifizierten Namen trotzdem einen Platz', () => {
        const source = walk();
        source.nodes = [
            source.nodes[0] as ClosureNode,
            { symbol: symbolNamed('validateUser', { qualified: false }), hop: 1, via: qn('createUser') },
        ];
        source.edges = [];
        const projection = projectHierarchy(source, { layout: LAYOUT });
        const orphan = nodeNamed(projection, 'validateUser');
        expect(orphan?.qualified_name).toBeUndefined();
        expect(orphan?.x).toBe(HIERARCHY_COLUMN_WIDTH);
        expect(projection.symbols).toBe(2);
    });

    it('reicht die Grenzen des Walks durch', () => {
        const capped = projectHierarchy(walk({ truncated: true, cap: 3, depth: 2, visited: 11 }));
        expect(capped.truncated).toBe(true);
        expect(capped.cap).toBe(3);
        expect(capped.walkDepth).toBe(2);
        expect(capped.missing).toBe(11 - capped.symbols);
        expect(capped.data.total_nodes).toBe(11);
    });
});

describe('hierarchyHeadline', () => {

    it('nennt Symbol, Zahl und Tiefe', () => {
        const headline = hierarchyHeadline(projectHierarchy(walk()));
        expect(headline).toBe('hierarchy of createUser: 6 symbols, depth 3');
    });

    it('nennt den Deckel, wenn er gegriffen hat', () => {
        const headline = hierarchyHeadline(projectHierarchy(walk({ truncated: true, cap: 3 })));
        expect(headline).toContain('hierarchy of createUser:');
        expect(headline).toContain('walk capped at 3 symbols (depth 3)');
    });

    it('sagt "symbol" im Singular', () => {
        const alone = walk();
        alone.nodes = [alone.nodes[0] as ClosureNode];
        alone.edges = [];
        expect(hierarchyHeadline(projectHierarchy(alone)))
            .toBe('hierarchy of createUser: 1 symbol, depth 1');
    });

    /*
     * Die Herkunft der Wurzel (W10b, AC3).
     *
     * Ein Einstiegs-Spaziergang ist eine Entscheidung, ein Fokus ist ein Ort.
     * Dasselbe Bild bedeutet in beiden Faellen etwas anderes, also sagt der Kopf,
     * welcher Fall vorliegt. Der Satz fuer den Spaziergang bleibt woertlich der
     * von W4e: mehrere Beweislaeufe lesen ihn bis zu seinem Ende.
     */
    it('nennt die Herkunft, wenn die Wurzel aus dem Fokus kommt', () => {
        expect(hierarchyHeadline(projectHierarchy(walk()), 'focus'))
            .toBe('hierarchy of createUser (in focus): 6 symbols, depth 3');
    });

    /*
     * Die Herkunft steht frueh im Satz, und das ist die Bedingung dafuer, dass
     * man sie sieht: die Zeile bricht nicht um und endet in einer 440 Pixel
     * breiten Spalte bei rund dreiundsechzig Zeichen. Ein Zusatz dahinter waere
     * genau der Teil, der abgeschnitten wird.
     */
    it('stellt die Herkunft vor die Zahlen, nicht hinter sie', () => {
        const headline = hierarchyHeadline(projectHierarchy(walk()), 'focus');
        expect(headline.indexOf('in focus')).toBeLessThan(headline.indexOf('symbols'));
        expect(headline.length).toBeLessThan(63);
    });

    it('sagt beim Spaziergang genau denselben Satz wie vorher', () => {
        const projection = projectHierarchy(walk());
        expect(hierarchyHeadline(projection, 'walk')).toBe(hierarchyHeadline(projection));
    });

    it('nennt Herkunft und Deckel nebeneinander, ohne den einen zu verschlucken', () => {
        const headline = hierarchyHeadline(
            projectHierarchy(walk({ truncated: true, cap: 3 })),
            'focus',
        );
        expect(headline).toContain('createUser (in focus)');
        expect(headline).toContain('walk capped at 3 symbols (depth 3)');
    });
});

/*
 * Die Lesbarkeit des Bildes (W5c).
 *
 * Zwei Zusicherungen, die man sonst erst am fertigen Bild merkt: die
 * Knotengroesse bleibt in einem Band, damit eine Beschriftung nicht in die
 * naechste geschoben wird, und die Rahmung schliesst den Platz der
 * Beschriftungen ein, damit die aeussersten Namen nicht abgeschnitten werden.
 */
describe('das Band der Knotengroessen', () => {
    it('deckelt einen sehr grossen Knoten des Servers', () => {
        const huge: GraphData = {
            ...LAYOUT,
            nodes: LAYOUT.nodes.map((node) => ({ ...node, size: 400 })),
        };
        const projection = projectHierarchy(walk(), { layout: huge });
        for (const node of projection.data.nodes) {
            expect(node.size).toBeLessThanOrEqual(HIERARCHY_MAX_SIZE);
        }
    });

    it('hebt einen sehr kleinen an, statt ihn unter seinem Namen verschwinden zu lassen', () => {
        const tiny: GraphData = {
            ...LAYOUT,
            nodes: LAYOUT.nodes.map((node) => ({ ...node, size: 0.1 })),
        };
        const projection = projectHierarchy(walk(), { layout: tiny });
        for (const node of projection.data.nodes) {
            expect(node.size).toBeGreaterThanOrEqual(HIERARCHY_MIN_SIZE);
        }
    });

    it('laesst das Verhaeltnis im Band unangetastet', () => {
        const projection = projectHierarchy(walk(), { layout: LAYOUT });
        const root = nodeNamed(projection, 'createUser');
        const query = nodeNamed(projection, 'query');
        expect(root?.size).toBe(7.6 * HIERARCHY_SIZE_SCALE);
        expect(query?.size).toBe(5.5 * HIERARCHY_SIZE_SCALE);
    });
});

describe('hierarchyFrame', () => {
    it('schliesst den Platz der Beschriftungen ein', () => {
        const projection = projectHierarchy(walk());
        const xs = projection.placements.map((placement) => placement.x);
        const ys = projection.placements.map((placement) => placement.y);
        const frame = hierarchyFrame(projection);
        expect(frame.width).toBe(Math.max(...xs) - Math.min(...xs) + 2 * HIERARCHY_LABEL_PAD_X);
        expect(frame.height).toBe(
            Math.max(...ys) - Math.min(...ys) + HIERARCHY_LABEL_PAD_TOP + HIERARCHY_LABEL_PAD_BOTTOM,
        );
        expect(frame.centerX).toBe((Math.min(...xs) + Math.max(...xs)) / 2);
    });

    it('waechst mit dem Walk, statt fuer jede Groesse gleich zu sein', () => {
        const small = walk();
        small.nodes = small.nodes.slice(0, 2);
        small.edges = small.edges.filter((edge) => edge.to === qn('validateUser'));
        expect(hierarchyFrame(projectHierarchy(small)).height)
            .toBeLessThan(hierarchyFrame(projectHierarchy(walk())).height);
    });

    it('antwortet auf eine Projektion ohne Knoten mit einem Rechteck statt mit NaN', () => {
        const empty = walk();
        empty.nodes = [];
        empty.edges = [];
        const frame = hierarchyFrame(projectHierarchy(empty));
        expect(Number.isFinite(frame.width)).toBe(true);
        expect(Number.isFinite(frame.height)).toBe(true);
        expect(frame.width).toBeGreaterThan(0);
        expect(frame.height).toBeGreaterThan(0);
    });
});
