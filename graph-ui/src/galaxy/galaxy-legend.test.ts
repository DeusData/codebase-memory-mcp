/*
 * Die Galaxy-Legende: geprueft wird, dass sie das Bild beschreibt und nicht
 * eine Vorstellung davon.
 *
 * Die Kantenfarben kommen aus derselben Tabelle, die die Linien malt. Der Test
 * vergleicht sie deshalb gegen den Import und nicht gegen abgeschriebene
 * Hexwerte: ein Test mit eigenen Farben waere eine dritte Wahrheit neben
 * Legende und Szene.
 */
import { describe, expect, it } from 'vitest';
import {
    GALAXY_LEGEND_DEFAULT_OPEN,
    GALAXY_LEGEND_KEY,
    edgeColorFor,
    edgeKindNote,
    edgeKinds,
    edgeSwatches,
    galaxyLegendEntries,
    hierarchyLegendEntries,
    readLegendOpen,
    withoutEdgeKinds,
    writeLegendOpen,
} from './galaxy-legend';
import { DEFAULT_EDGE_COLOR, EDGE_TYPE_COLORS } from './EdgeLines';
import { HIERARCHY_DEFAULT_COLOR } from './hierarchy-layout';
import type { GraphData } from './types';

const node = (id: number) => ({
    id,
    x: 0,
    y: 0,
    z: 0,
    label: 'Function',
    name: `n${id}`,
    size: 1,
    color: '#fff',
});

const DATA: GraphData = {
    nodes: [node(1), node(2), node(3)],
    edges: [
        { source: 1, target: 2, type: 'CALLS' },
        { source: 2, target: 3, type: 'IMPORTS' },
        { source: 3, target: 1, type: 'CALLS' },
        { source: 1, target: 3, type: 'WHAT_IS_THIS' },
    ],
    total_nodes: 3,
};

/** Ein Speicher, der sich verhaelt wie localStorage, aber im Test lebt. */
function memoryStore(initial: Record<string, string> = {}): Storage {
    const map = new Map(Object.entries(initial));
    return {
        get length() {
            return map.size;
        },
        clear: () => map.clear(),
        getItem: (key: string) => map.get(key) ?? null,
        key: (index: number) => [...map.keys()][index] ?? null,
        removeItem: (key: string) => {
            map.delete(key);
        },
        setItem: (key: string, value: string) => {
            map.set(key, value);
        },
    } as Storage;
}

describe('edgeColorFor', () => {

    it('nimmt die Farbe, mit der die Szene diesen Typ malt', () => {
        expect(edgeColorFor('CALLS')).toBe(EDGE_TYPE_COLORS['CALLS']);
        expect(edgeColorFor('IMPORTS')).toBe(EDGE_TYPE_COLORS['IMPORTS']);
    });

    it('faellt fuer einen unbekannten Typ auf dieselbe Vorgabe zurueck wie die Szene', () => {
        expect(edgeColorFor('WHAT_IS_THIS')).toBe(DEFAULT_EDGE_COLOR);
    });
});

describe('edgeKinds', () => {

    it('zaehlt, was das Layout wirklich traegt, und sortiert absteigend', () => {
        expect(edgeKinds(DATA)).toEqual([
            { type: 'CALLS', count: 2, color: EDGE_TYPE_COLORS['CALLS'] },
            { type: 'IMPORTS', count: 1, color: EDGE_TYPE_COLORS['IMPORTS'] },
            { type: 'WHAT_IS_THIS', count: 1, color: DEFAULT_EDGE_COLOR },
        ]);
    });

    it('entscheidet bei gleicher Zahl ordinal nach dem Namen', () => {
        const tied: GraphData = {
            nodes: [node(1)],
            edges: [
                { source: 1, target: 1, type: 'ZETA' },
                { source: 1, target: 1, type: 'ALPHA' },
                { source: 1, target: 1, type: 'MIKE' },
            ],
            total_nodes: 1,
        };
        expect(edgeKinds(tied).map((kind) => kind.type)).toEqual(['ALPHA', 'MIKE', 'ZETA']);
    });

    it('zeigt keine Typen, die in diesem Layout nicht vorkommen', () => {
        expect(edgeKinds(DATA).map((kind) => kind.type)).not.toContain('TESTS_FILE');
    });

    /*
     * Bis W9 schnitt die Liste nach acht Arten ab, damit die Legende nicht
     * laenger wird als die Szene hoch ist. Seit W5c scrollt der Kasten selbst,
     * und seit W9 ist jede Zeile ein Schalter: eine abgeschnittene Art waere
     * eine, die man nicht mehr ausblenden kann.
     */
    it('laesst keine Art weg, auch wenn es viele sind', () => {
        const many: GraphData = {
            nodes: [node(1)],
            edges: Array.from({ length: 20 }, (_unused, i) => ({ source: 1, target: 1, type: `T${i}` })),
            total_nodes: 1,
        };
        expect(edgeKinds(many)).toHaveLength(20);
    });

    it('zaehlt eine Kante ohne Typ nirgends mit', () => {
        const nameless: GraphData = {
            nodes: [node(1)],
            edges: [{ source: 1, target: 1, type: '' }, { source: 1, target: 1, type: 'CALLS' }],
            total_nodes: 1,
        };
        expect(edgeKinds(nameless)).toEqual([
            { type: 'CALLS', count: 1, color: EDGE_TYPE_COLORS['CALLS'] },
        ]);
    });

    it('hat ohne Layout keine Arten', () => {
        expect(edgeKinds(undefined)).toEqual([]);
        expect(edgeSwatches(undefined)).toEqual([]);
    });
});

describe('edgeSwatches', () => {

    it('traegt Name, Farbe und Zahl jeder Art', () => {
        expect(edgeSwatches(DATA)[0])
            .toEqual({ label: 'CALLS', color: EDGE_TYPE_COLORS['CALLS'], count: 2 });
    });
});

describe('edgeKindNote', () => {

    it('nennt die Zahl der Arten, auch wenn nichts ausgeblendet ist', () => {
        expect(edgeKindNote(12, 0)).toBe('12 edge kinds');
    });

    it('sagt beim Ausblenden die Zahl und ihr Ganzes', () => {
        expect(edgeKindNote(12, 3)).toBe('12 edge kinds, 3 of 12 hidden');
    });

    it('sagt die eine Art im Singular', () => {
        expect(edgeKindNote(1, 0)).toBe('1 edge kind');
    });

    it('schweigt, wo es keine einzige Art gibt', () => {
        expect(edgeKindNote(0, 0)).toBe('');
    });
});

describe('withoutEdgeKinds', () => {

    it('nimmt genau die ausgeblendeten Arten aus den Kanten', () => {
        const left = withoutEdgeKinds(DATA, new Set(['CALLS']));
        expect(left.edges.map((edge) => edge.type)).toEqual(['IMPORTS', 'WHAT_IS_THIS']);
    });

    it('laesst jeden Knoten stehen, auch den ohne Kante', () => {
        const left = withoutEdgeKinds(DATA, new Set(['CALLS', 'IMPORTS', 'WHAT_IS_THIS']));
        expect(left.edges).toEqual([]);
        expect(left.nodes).toBe(DATA.nodes);
        expect(left.total_nodes).toBe(DATA.total_nodes);
    });

    it('gibt ohne ausgeblendete Art dasselbe Objekt zurueck', () => {
        expect(withoutEdgeKinds(DATA, new Set())).toBe(DATA);
    });
});

describe('galaxyLegendEntries', () => {

    const entries = galaxyLegendEntries(DATA);

    it('erklaert mindestens die fuenf Erscheinungen des Bildes, die Kanten zuerst', () => {
        // Die Reihenfolge ist eine Aussage und kein Zufall: der Kanten-Eintrag
        // ist der einzige, der auch ein Schalter ist, und die Legende ist
        // niedriger als ihr Inhalt. Was man anklicken koennen soll, steht darum
        // ganz oben und nicht hinter drei Absaetzen Fliesstext (W9-1).
        expect(entries.map((entry) => entry.key)).toEqual([
            'edge-color',
            'node-color',
            'node-size',
            'focus',
            'positions',
            'agent-trail',
        ]);
        expect(entries.length).toBeGreaterThanOrEqual(3);
    });

    it('nennt fuer Farbe und Groesse den Server als Urheber', () => {
        const color = entries.find((entry) => entry.key === 'node-color');
        const size = entries.find((entry) => entry.key === 'node-size');
        expect(color?.detail).toContain('stellar_color');
        expect(color?.detail).toContain('layout3d.c');
        expect(size?.detail).toContain('computed by the server');
    });

    it('haengt die echten Kantenfarben mit ihrer Zahl an den Kanten-Eintrag', () => {
        const edges = entries.find((entry) => entry.key === 'edge-color');
        expect(edges?.swatches[0])
            .toEqual({ label: 'CALLS', color: EDGE_TYPE_COLORS['CALLS'], count: 2 });
        expect(edges?.filterable).toBe(true);
        expect(edges?.detail).toContain('Counted in this layout');
    });

    it('sagt beim Fokus, was hell bleibt und was dunkel wird', () => {
        const focus = entries.find((entry) => entry.key === 'focus');
        expect(focus?.detail).toContain('direct neighbours');
        expect(focus?.detail).toContain('dims');
    });

    it('nennt die Positionen deterministisch und serverseitig', () => {
        const positions = entries.find((entry) => entry.key === 'positions');
        expect(positions?.detail).toContain('deterministic');
        expect(positions?.detail).toContain('not by this browser');
    });

    it('behaelt die Saetze auch ohne Layout und laesst nur die Punkte weg', () => {
        const blind = galaxyLegendEntries(undefined);
        expect(blind).toHaveLength(6);
        expect(blind.find((entry) => entry.key === 'edge-color')?.swatches).toEqual([]);
    });
});

describe('hierarchyLegendEntries', () => {

    const entries = hierarchyLegendEntries(DATA);

    it('erklaert dieselben Erscheinungen unter denselben Schluesseln', () => {
        expect([...entries.map((entry) => entry.key)].sort())
            .toEqual([...galaxyLegendEntries(DATA).map((entry) => entry.key)].sort());
        expect(entries.length).toBeGreaterThanOrEqual(3);
    });

    it('stellt auch hier die Kanten voran', () => {
        expect(entries[0]?.key).toBe('edge-color');
    });

    it('sagt bei den Positionen die Aufruf-Tiefe und widerruft das Server-Layout', () => {
        const positions = entries.find((entry) => entry.key === 'positions');
        expect(positions?.detail).toContain('call depth from the entry point');
        expect(positions?.detail).toContain('not the server layout');
        expect(positions?.detail).toContain('deterministic projection');
        // Der Galaxie-Satz behauptet das Gegenteil und darf hier nicht stehen.
        expect(positions?.detail).not.toContain('computed by the server');
    });

    it('nennt die Zyklus-Kanten ausdruecklich als sichtbar', () => {
        const edges = entries.find((entry) => entry.key === 'edge-color');
        expect(edges?.detail).toContain('cycles');
        expect(edges?.detail).toContain('rather than hidden');
        expect(edges?.swatches[0])
            .toEqual({ label: 'CALLS', color: EDGE_TYPE_COLORS['CALLS'], count: 2 });
    });

    it('sagt, dass ausser den Aufrufen auch die uebrigen Beziehungen dastehen', () => {
        const edges = entries.find((entry) => entry.key === 'edge-color');
        expect(edges?.detail).toContain('in its own colour');
        expect(edges?.detail).toContain('the calls make the columns');
        expect(edges?.filterable).toBe(true);
    });

    it('sagt, dass Farbe und Groesse nur fuer Symbole aus dem Layout gelten', () => {
        const color = entries.find((entry) => entry.key === 'node-color');
        expect(color?.detail).toContain('stellar_color');
        expect(color?.detail).toContain('neutral grey');
        expect(color?.swatches).toEqual([
            { label: 'no layout entry', color: HIERARCHY_DEFAULT_COLOR },
        ]);
        const size = entries.find((entry) => entry.key === 'node-size');
        expect(size?.detail).toContain('not more importance');
    });

    it('behaelt die Saetze auch ohne Layout', () => {
        expect(hierarchyLegendEntries(undefined)).toHaveLength(6);
    });
});

describe('der Klappzustand', () => {

    it('faengt ohne gespeicherte Antwort in der Vorgabelage an', () => {
        expect(readLegendOpen(memoryStore())).toBe(GALAXY_LEGEND_DEFAULT_OPEN);
        expect(readLegendOpen(undefined)).toBe(GALAXY_LEGEND_DEFAULT_OPEN);
    });

    it('liest genau die beiden Worte, die geschrieben werden', () => {
        expect(readLegendOpen(memoryStore({ [GALAXY_LEGEND_KEY]: 'closed' }))).toBe(false);
        expect(readLegendOpen(memoryStore({ [GALAXY_LEGEND_KEY]: 'open' }))).toBe(true);
        expect(readLegendOpen(memoryStore({ [GALAXY_LEGEND_KEY]: 'irgendwas' })))
            .toBe(GALAXY_LEGEND_DEFAULT_OPEN);
    });

    it('merkt sich eine Entscheidung', () => {
        const store = memoryStore();
        writeLegendOpen(store, false);
        expect(store.getItem(GALAXY_LEGEND_KEY)).toBe('closed');
        expect(readLegendOpen(store)).toBe(false);
        writeLegendOpen(store, true);
        expect(readLegendOpen(store)).toBe(true);
    });

    it('versteckt die Legende nicht, wenn der Speicher nicht antwortet', () => {
        const broken = {
            getItem: () => {
                throw new Error('denied');
            },
            setItem: () => {
                throw new Error('denied');
            },
        } as unknown as Storage;
        expect(readLegendOpen(broken)).toBe(GALAXY_LEGEND_DEFAULT_OPEN);
        expect(() => writeLegendOpen(broken, false)).not.toThrow();
    });
});
