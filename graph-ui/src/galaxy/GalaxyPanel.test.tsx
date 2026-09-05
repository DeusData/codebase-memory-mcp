// @vitest-environment jsdom
/*
 * Der Fokus in beide Richtungen, ohne einen einzigen gerenderten Pixel.
 *
 * Das Panel wird hier ausdruecklich unsichtbar gefahren (`visible={false}`).
 * Das ist kein Trick, um die Szene loszuwerden, sondern genau die Lage, die
 * das Panel auch im Betrieb hat, solange niemand es aufgeklappt hat: geladen
 * wird trotzdem, und der Fokus folgt trotzdem, damit die Kamera schon dort
 * steht, wo sie hingehoert, wenn jemand aufklappt. Was dabei ungeprueft
 * bleibt, ist das Bild selbst, und das prueft der Beweislauf im Browser mit
 * einem echten WebGL-Kontext (tools/smoke-w3.mjs).
 */

import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import GalaxyPanel from './GalaxyPanel';
import type { GalaxyPanelProps } from './GalaxyPanel';
import { GALAXY_LEGEND_KEY } from './galaxy-legend';
import { HIERARCHY_COLUMN_WIDTH, HIERARCHY_LANE_SPACING } from './hierarchy-layout';
import { toEditorRange } from '../core/positions';
import type { SymbolRef } from '../core/focus-protocol';
import type { ClosureResult } from '../provider/closure';

let container: HTMLDivElement;
let root: Root;

const LAYOUT = {
    nodes: [
        {
            id: 51, x: -244, y: -453, z: -46, label: 'Function', name: 'createUser',
            file_path: 'src/services/userService.ts',
            qualified_name: 'atlas.src.services.userService.createUser',
            start_line: 23, end_line: 36, size: 7.6, color: '#ffe080', in_calls: 2,
        },
        {
            id: 69, x: -143, y: -599, z: -43, label: 'Function', name: 'validateUser',
            file_path: 'src/util/validate.ts',
            qualified_name: 'atlas.src.util.validate.validateUser',
            start_line: 19, end_line: 31, size: 4, color: '#ffa060',
        },
        {
            id: 74, x: 12, y: 8, z: 3, label: 'EnvVar', name: 'DB_URL',
            qualified_name: '__env__DB_URL', size: 3, color: '#88aaff',
        },
    ],
    edges: [
        { source: 51, target: 69, type: 'CALLS' },
        /*
         * Eine Art, die die Farbtabelle NICHT kennt.
         *
         * Bis W9 stand hier CONFIGURES; seitdem hat dieser Typ eine eigene
         * Farbe (die Engine dieses Projekts liefert ihn wirklich), und die
         * Zusicherung "was die Tabelle nicht kennt, bekommt die Vorgabefarbe"
         * braucht darum einen Namen, den sie auch morgen nicht kennt.
         */
        { source: 51, target: 74, type: 'WHAT_IS_THIS' },
    ],
    total_nodes: 76,
};

const okFetch = () => vi.fn(async () => new Response(JSON.stringify(LAYOUT), { status: 200 }));

/**
 * Ein `fetch` je Test, nicht je Render.
 *
 * Das ist keine Bequemlichkeit: das Panel laedt neu, wenn sich seine
 * Ladequelle aendert, und ein frisches Mock bei jedem Render waere ein
 * Neuladen bei jedem Render. Die Zaehler unten wuerden dann etwas messen, das
 * es im Betrieb nicht gibt.
 */
let sharedFetch: ReturnType<typeof okFetch>;

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    sharedFetch = okFetch();
});

afterEach(async () => {
    await act(async () => {
        root.unmount();
    });
    container.remove();
    globalThis.__atlasGalaxy = undefined;
});

function props(overrides: Partial<GalaxyPanelProps> = {}): GalaxyPanelProps {
    return {
        project: 'atlas-sample',
        visible: false,
        onOpenNode: vi.fn(),
        fetch: sharedFetch as unknown as typeof fetch,
        ...overrides,
    };
}

async function render(next: GalaxyPanelProps): Promise<void> {
    await act(async () => {
        root.render(<GalaxyPanel {...next} />);
    });
}

const seam = () => globalThis.__atlasGalaxy!;
const noteText = () =>
    container.querySelector('[data-testid="atlas-galaxy-note"]')?.textContent ?? '';
const headlineText = () =>
    container.querySelector('[data-testid="atlas-galaxy-headline"]')?.textContent ?? '';
/** Die zweite Zeile des Kopfes (W9): woraus die Linien bestehen, was fehlt. */
const edgeNoteText = () =>
    container.querySelector('[data-testid="atlas-galaxy-edgenote"]')?.textContent ?? '';

describe('GalaxyPanel', () => {

    it('laedt das Layout und sagt, was es geladen hat', async () => {
        await render(props());
        expect((sharedFetch.mock.calls[0] as unknown as [string])[0])
            .toBe('/api/layout?project=atlas-sample&max_nodes=5000');
        expect(container.querySelector('[data-testid="atlas-galaxy"]')).not.toBeNull();
        expect(seam().nodes).toBe(3);
        expect(headlineText()).toContain('3 nodes, 2 edges from /api/layout');
        expect(headlineText()).toContain('76 nodes indexed');
    });

    it('zeigt einen Ladefehler, statt leer zu bleiben', async () => {
        const failing = vi.fn(async () => new Response('{"error":"no such project"}', { status: 404 }));
        await render(props({ fetch: failing as unknown as typeof fetch }));
        expect(headlineText()).toContain('layout unavailable');
        expect(headlineText()).toContain('404');
        expect(seam().nodes).toBe(0);
    });

    it('fliegt das Twin-Subjekt an und hebt Knoten plus Nachbarn hervor', async () => {
        await render(props());
        expect(seam().targetChanges).toBe(0);
        await render(props({ focusQualifiedName: 'atlas.src.services.userService.createUser' }));
        expect(seam().targetChanges).toBe(1);
        expect(seam().highlightedCount).toBe(3);
        expect(seam().lastTargetQn).toBe('atlas.src.services.userService.createUser');
        expect(noteText()).toBe('');
    });

    it('sagt ehrlich, wenn das Subjekt nicht im geladenen Layout liegt', async () => {
        await render(props());
        await render(props({
            focusQualifiedName: 'atlas.src.nowhere.hidden',
            focusName: 'hidden',
        }));
        expect(noteText()).toContain('hidden is not in the loaded layout');
        expect(noteText()).toContain('5000 node budget');
        expect(seam().targetChanges).toBe(0);
    });

    it('oeffnet einen angeklickten Knoten und fliegt ihn an', async () => {
        const onOpenNode = vi.fn();
        await render(props({ onOpenNode }));
        let found = false;
        await act(async () => {
            found = seam().clickNode('atlas.src.util.validate.validateUser');
        });
        expect(found).toBe(true);
        expect(onOpenNode).toHaveBeenCalledTimes(1);
        expect(onOpenNode.mock.calls[0][0]).toMatchObject({
            name: 'validateUser',
            file_path: 'src/util/validate.ts',
        });
        expect(seam().targetChanges).toBe(1);
        expect(seam().highlightedCount).toBe(2);
    });

    it('oeffnet nichts fuer einen Knoten ohne Datei und sagt, warum', async () => {
        const onOpenNode = vi.fn();
        await render(props({ onOpenNode }));
        await act(async () => {
            seam().clickNode('__env__DB_URL');
        });
        expect(onOpenNode).not.toHaveBeenCalled();
        expect(noteText()).toContain('DB_URL');
        expect(noteText()).toContain('nothing to open');
        // Angeflogen wird er trotzdem: gezeigt wird er ja.
        expect(seam().targetChanges).toBe(1);
    });

    it('meldet einen unbekannten Namen am Testgriff, statt still nichts zu tun', async () => {
        await render(props());
        let found = true;
        await act(async () => {
            found = seam().clickNode('gibt.es.nicht');
        });
        expect(found).toBe(false);
    });

    it('zaehlt jede Fahrt, auch die zweite auf dasselbe Symbol', async () => {
        const focusQualifiedName = 'atlas.src.services.userService.createUser';
        await render(props({ focusQualifiedName }));
        expect(seam().targetChanges).toBe(1);
        await act(async () => {
            seam().clickNode(focusQualifiedName);
        });
        expect(seam().targetChanges).toBe(2);
    });
});

/*
 * Die Legende unter dem Kopf (W4d).
 *
 * Der Speicher wird von aussen gesetzt, damit "der Zustand ueberlebt den
 * Reload" ohne einen echten Reload pruefbar ist: neu gemountet mit demselben
 * Speicher ist genau die Lage nach einem Reload, und der Beweislauf im Browser
 * laedt danach zusaetzlich wirklich neu.
 */

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

const legendEntries = () =>
    [...container.querySelectorAll('[data-testid="atlas-galaxy-legend-entry"]')];
const toggle = () =>
    container.querySelector<HTMLButtonElement>('[data-testid="atlas-galaxy-legend-toggle"]');

/**
 * Ein Speicher, in dem die Legende schon aufgeklappt ist.
 *
 * Seit W5c ist ZU die Vorgabe (das Panel ist schmal, und die aufgeklappte
 * Legende nahm den Platz des Bildes, das sie erklaert). Wer den Inhalt der
 * Legende pruefen will, klappt sie also erst auf, und das ist hier die
 * gespeicherte Antwort eines Lesers, der das getan hat.
 */
const openStore = () => memoryStore({ [GALAXY_LEGEND_KEY]: 'open' });

describe('GalaxyPanel und die Legende', () => {

    it('ist im schmalen Panel default zu, damit der Graph den Platz behaelt', async () => {
        await render(props({ legendStore: memoryStore() }));
        expect(container.querySelector('[data-testid="atlas-galaxy-legend"]')).toBeNull();
        expect(toggle()?.getAttribute('aria-expanded')).toBe('false');
        expect(seam().legendOpen).toBe(false);
    });

    it('steht unter dem Kopf und erklaert mindestens drei Elemente', async () => {
        await render(props({ legendStore: openStore() }));
        expect(container.querySelector('[data-testid="atlas-galaxy-legend"]')).not.toBeNull();
        expect(legendEntries().length).toBeGreaterThanOrEqual(3);
        expect(seam().legendEntries).toBe(legendEntries().length);
    });

    it('zeigt die Kantenfarben dieses Layouts als farbige Punkte', async () => {
        await render(props({ legendStore: openStore() }));
        const swatches = [...container.querySelectorAll('[data-testid="atlas-galaxy-legend-swatch"]')];
        expect(swatches.map((swatch) => swatch.getAttribute('data-type')))
            .toEqual(['CALLS', 'WHAT_IS_THIS']);
        // CALLS steht in der Tabelle, WHAT_IS_THIS nicht: der zweite bekommt
        // die Vorgabefarbe, die auch die Szene malt.
        expect(swatches[0]?.getAttribute('data-color')).toBe('#1DA27E');
        expect(swatches[1]?.getAttribute('data-color')).toBe('#1C8585');
    });

    /*
     * Die Legende als Filter (W9).
     *
     * Gemessen wird am DOM und am Griff und nicht am Bild: ob die Linie
     * wirklich verschwindet, ist eine Frage an einen WebGL-Kontext, und die
     * stellt tools/smoke-w9.mjs mit einer Pixelanalyse. Hier zaehlt, dass die
     * Szene die Kante gar nicht erst bekommt, dass die Zeile stehen bleibt und
     * dass der Kopf die Zahl nennt.
     */
    it('zaehlt jede Art und nennt ihre Zahl in der Zeile', async () => {
        await render(props({ legendStore: openStore() }));
        const swatches = [...container.querySelectorAll('[data-testid="atlas-galaxy-legend-swatch"]')];
        expect(swatches.map((swatch) => swatch.getAttribute('data-count'))).toEqual(['1', '1']);
        expect(swatches[0]?.textContent).toContain('CALLS 1');
        expect(seam().edgeKinds.map((kind) => kind.type)).toEqual(['CALLS', 'WHAT_IS_THIS']);
        expect(seam().edgeKinds.every((kind) => kind.hidden === false)).toBe(true);
        expect(seam().drawnEdges).toBe(2);
        expect(seam().edgeNote).toBe('2 edge kinds');
    });

    it('nimmt eine angeklickte Art aus dem Bild und laesst ihre Zeile stehen', async () => {
        await render(props({ legendStore: openStore() }));
        const row = () => container.querySelector<HTMLButtonElement>(
            '[data-testid="atlas-galaxy-legend-swatch"][data-type="CALLS"]',
        );
        expect(row()?.tagName).toBe('BUTTON');
        await act(async () => {
            row()?.click();
        });
        expect(row()).not.toBeNull();
        expect(row()?.getAttribute('data-hidden')).toBe('true');
        expect(row()?.getAttribute('aria-pressed')).toBe('false');
        expect(row()?.textContent).toContain('CALLS 1');
        expect(seam().hiddenKinds).toEqual(['CALLS']);
        expect(seam().drawnEdges).toBe(1);
        expect(seam().edgeNote).toBe('2 edge kinds, 1 of 2 hidden');
        expect(edgeNoteText()).toBe('2 edge kinds, 1 of 2 hidden');
        expect(headlineText()).toContain('3 nodes, 2 edges from /api/layout');
        // Und ein zweiter Klick holt sie zurueck.
        await act(async () => {
            row()?.click();
        });
        expect(row()?.getAttribute('data-hidden')).toBe('false');
        expect(seam().drawnEdges).toBe(2);
        expect(edgeNoteText()).toBe('2 edge kinds');
    });

    it('laesst die Nachbarschaft am ganzen Bild haengen, nicht am Filter', async () => {
        await render(props({ legendStore: openStore() }));
        await act(async () => {
            container.querySelector<HTMLButtonElement>(
                '[data-testid="atlas-galaxy-legend-swatch"][data-type="WHAT_IS_THIS"]',
            )?.click();
        });
        await act(async () => {
            seam().clickNode('atlas.src.services.userService.createUser');
        });
        // Beide Nachbarn, obwohl die Kante zu DB_URL gerade nicht gezeichnet
        // wird: Nachbar ist, wen der Index nennt.
        expect(seam().highlightedCount).toBe(3);
    });

    it('traegt am Kopf einen Schalter mit einem Wort und aria-expanded', async () => {
        await render(props({ legendStore: openStore() }));
        expect(toggle()?.getAttribute('aria-expanded')).toBe('true');
        // Wort statt Pfeil seit W8b: der Nutzer hat die Zeichen nicht verstanden.
        expect(toggle()?.textContent).toContain('hide legend');
    });

    it('klappt auf Klick auf und wieder zu', async () => {
        await render(props({ legendStore: memoryStore() }));
        await act(async () => {
            toggle()?.click();
        });
        expect(container.querySelector('[data-testid="atlas-galaxy-legend"]')).not.toBeNull();
        expect(toggle()?.getAttribute('aria-expanded')).toBe('true');
        // Wort statt Pfeil seit W8b: der Nutzer hat die Zeichen nicht verstanden.
        expect(toggle()?.textContent).toContain('hide legend');
        expect(seam().legendOpen).toBe(true);
        await act(async () => {
            toggle()?.click();
        });
        expect(container.querySelector('[data-testid="atlas-galaxy-legend"]')).toBeNull();
    });

    it('merkt sich den Zustand ueber ein neues Mounten hinweg', async () => {
        const store = memoryStore();
        await render(props({ legendStore: store }));
        await act(async () => {
            toggle()?.click();
        });
        await act(async () => {
            root.unmount();
        });
        root = createRoot(container);
        await render(props({ legendStore: store }));
        expect(container.querySelector('[data-testid="atlas-galaxy-legend"]')).not.toBeNull();
        expect(toggle()?.getAttribute('aria-expanded')).toBe('true');
    });
});

/*
 * Die Hierarchie-Ansicht (W4e).
 *
 * Auch hier faellt kein Pixel: gemessen wird der Griff und das DOM des Kopfes.
 * Dass die Szene die Projektion wirklich zeichnet und dabei denselben Canvas
 * behaelt, beweist der Browserlauf (tools/smoke-w4e.mjs); was hier bewiesen
 * wird, ist die Entscheidung darueber, welches Bild dransteht, was der Kopf
 * dazu sagt und woran der Klick danach geht.
 */

const WALK_QN = {
    createUser: 'atlas.src.services.userService.createUser',
    validateUser: 'atlas.src.util.validate.validateUser',
    listUsers: 'atlas.src.services.userService.listUsers',
    query: 'atlas.src.repo.db.query',
};

function walkSymbol(name: keyof typeof WALK_QN, file: string, line: number): SymbolRef {
    return {
        name,
        qualifiedName: WALK_QN[name],
        kind: 'function',
        uri: `file:///workspace/${file}`,
        range: toEditorRange(line, line + 4),
        selectionRange: toEditorRange(line, line),
    };
}

function walkOf(overrides: Partial<ClosureResult> = {}): ClosureResult {
    const root = walkSymbol('createUser', 'src/services/userService.ts', 23);
    return {
        root,
        nodes: [
            { symbol: root, hop: 0 },
            {
                symbol: walkSymbol('validateUser', 'src/util/validate.ts', 19),
                hop: 1,
                via: WALK_QN.createUser,
            },
            {
                symbol: walkSymbol('listUsers', 'src/services/userService.ts', 18),
                hop: 1,
                via: WALK_QN.createUser,
            },
            {
                symbol: walkSymbol('query', 'src/repo/db.ts', 17),
                hop: 2,
                via: WALK_QN.listUsers,
            },
        ],
        edges: [
            { from: WALK_QN.createUser, to: WALK_QN.validateUser, line: 24 },
            { from: WALK_QN.createUser, to: WALK_QN.listUsers, line: 29 },
            { from: WALK_QN.listUsers, to: WALK_QN.query, line: 19 },
        ],
        truncated: false,
        visited: 4,
        depth: 3,
        cap: 15,
        ...overrides,
    };
}

const modeChip = (mode: string) =>
    container.querySelector<HTMLButtonElement>(
        `[data-testid="atlas-graph-mode-chip"][data-mode="${mode}"]`,
    );
const legendKeys = () =>
    [...container.querySelectorAll('[data-testid="atlas-galaxy-legend-entry"]')]
        .map((entry) => entry.getAttribute('data-entry'));

describe('GalaxyPanel und die Hierarchie', () => {

    it('bleibt bei der Galaxie, solange es keinen Walk gibt', async () => {
        await render(props());
        expect(container.querySelector('[data-testid="atlas-graph-mode"]')?.getAttribute('data-mode'))
            .toBe('galaxy');
        expect(seam().mode).toBe('galaxy');
        expect(seam().hierarchyAvailable).toBe(false);
        expect(seam().hierarchy).toBeUndefined();
        /*
         * Seit W10b `aria-disabled` statt `disabled`: ein vom Browser
         * gesperrter Knopf bekommt keine Zeigerereignisse und kann seinen
         * Tooltip darum nicht oeffnen. AC3 verlangt aber beides, deaktiviert UND
         * sagt warum.
         */
        expect(modeChip('hierarchy')?.getAttribute('aria-disabled')).toBe('true');
        expect(modeChip('hierarchy')?.getAttribute('data-hint')).toContain('open a symbol');
        expect(headlineText()).toContain('3 nodes, 2 edges from /api/layout');
    });

    it('schaltet von selbst auf hierarchy, sobald ein Walk laeuft', async () => {
        await render(props({ walk: walkOf() }));
        expect(seam().mode).toBe('hierarchy');
        expect(modeChip('hierarchy')?.getAttribute('aria-pressed')).toBe('true');
        expect(modeChip('galaxy')?.getAttribute('aria-pressed')).toBe('false');
        expect(headlineText()).toBe('hierarchy of createUser: 4 symbols, depth 3');
        expect(seam().headline).toBe(headlineText());
        // Woraus die Linien bestehen, steht in der zweiten Zeile des Kopfes.
        expect(edgeNoteText())
            .toBe('3 calls from the walk, 0 links from the index; 1 edge kind');
    });

    it('legt eine Spalte je Hop an und nennt sie im Griff', async () => {
        await render(props({ walk: walkOf() }));
        const hierarchy = seam().hierarchy!;
        expect(hierarchy.nodes).toBe(4);
        expect(hierarchy.depth).toBe(3);
        expect(hierarchy.root).toBe(WALK_QN.createUser);
        const byHop = new Map<number, number[]>();
        for (const placement of hierarchy.placements) {
            byHop.set(placement.hop, [...(byHop.get(placement.hop) ?? []), placement.x]);
        }
        expect([...byHop.keys()].sort()).toEqual([0, 1, 2]);
        expect(byHop.get(0)).toEqual([0]);
        expect(byHop.get(1)).toEqual([HIERARCHY_COLUMN_WIDTH, HIERARCHY_COLUMN_WIDTH]);
        expect(byHop.get(2)).toEqual([HIERARCHY_COLUMN_WIDTH * 2]);
        expect(hierarchy.edges).toEqual([
            { from: WALK_QN.createUser, to: WALK_QN.validateUser },
            { from: WALK_QN.createUser, to: WALK_QN.listUsers },
            { from: WALK_QN.listUsers, to: WALK_QN.query },
        ]);
    });

    it('nennt den Deckel im Kopf, wenn der Walk gekappt wurde', async () => {
        await render(props({ walk: walkOf({ truncated: true, cap: 3, visited: 9 }) }));
        expect(headlineText()).toContain('hierarchy of createUser: 4 symbols, depth 3');
        expect(headlineText()).toContain('walk capped at 3 symbols (depth 3)');
    });

    it('schaltet auf Klick zurueck zur Galaxie und behaelt die Wahl', async () => {
        await render(props({ walk: walkOf() }));
        await act(async () => {
            modeChip('galaxy')?.click();
        });
        expect(seam().mode).toBe('galaxy');
        expect(headlineText()).toContain('3 nodes, 2 edges from /api/layout');
        // Ein neues Bild derselben Sitzung darf die Entscheidung nicht kippen.
        await render(props({ walk: walkOf(), focusQualifiedName: WALK_QN.createUser }));
        expect(seam().mode).toBe('galaxy');
        await act(async () => {
            modeChip('hierarchy')?.click();
        });
        expect(seam().mode).toBe('hierarchy');
    });

    /*
     * Die uebrigen Beziehungen im Bild (W9).
     *
     * Das Layout dieses Falls traegt eine RAISES-Kante zwischen zwei Symbolen,
     * die auch im Walk stehen. Gemessen wird dreierlei: dass sie im Bild landet,
     * dass sie eine eigene Spur bekommt (sonst laege sie auf der Aufrufkante und
     * mischte sich additiv zu einer Farbe, die in keiner Legende steht), und
     * dass die Spalten davon unberuehrt bleiben.
     */
    const raisingLayout = () => ({
        ...LAYOUT,
        edges: [...LAYOUT.edges, { source: 51, target: 69, type: 'RAISES' }],
    });
    const raisingFetch = () =>
        vi.fn(async () => new Response(JSON.stringify(raisingLayout()), { status: 200 })) as
            unknown as typeof fetch;

    it('legt die uebrigen Beziehungen des Index dazu, jede auf ihrer Spur', async () => {
        await render(props({ walk: walkOf(), fetch: raisingFetch() }));
        const hierarchy = seam().hierarchy!;
        expect(hierarchy.walkEdges).toBe(3);
        expect(hierarchy.extraEdges).toBe(1);
        expect(hierarchy.extras).toEqual([
            {
                type: 'RAISES',
                from: WALK_QN.createUser,
                to: WALK_QN.validateUser,
                offset: HIERARCHY_LANE_SPACING,
            },
        ]);
        expect(seam().drawnEdges).toBe(4);
        expect(edgeNoteText())
            .toBe('3 calls from the walk, 1 link from the index; 2 edge kinds');
        expect(headlineText()).toBe('hierarchy of createUser: 4 symbols, depth 3');
    });

    it('ordnet mit den zusaetzlichen Kanten nichts um', async () => {
        await render(props({ walk: walkOf() }));
        const plain = seam().hierarchy!.placements;
        await act(async () => {
            root.unmount();
        });
        root = createRoot(container);
        await render(props({ walk: walkOf(), fetch: raisingFetch() }));
        expect(seam().hierarchy!.placements).toEqual(plain);
    });

    it('haengt die zusaetzlichen Kanten an denselben Filter', async () => {
        await render(props({ walk: walkOf(), fetch: raisingFetch(), legendStore: openStore() }));
        expect(seam().edgeKinds.map((kind) => kind.type)).toEqual(['CALLS', 'RAISES']);
        expect(seam().edgeKinds.map((kind) => kind.count)).toEqual([3, 1]);
        await act(async () => {
            container.querySelector<HTMLButtonElement>(
                '[data-testid="atlas-galaxy-legend-swatch"][data-type="RAISES"]',
            )?.click();
        });
        expect(seam().drawnEdges).toBe(3);
        expect(edgeNoteText())
            .toBe('3 calls from the walk, 1 link from the index; 2 edge kinds, 1 of 2 hidden');
        // Der Zustand ueberlebt den Wechsel in die andere Ansicht.
        await act(async () => {
            modeChip('galaxy')?.click();
        });
        expect(seam().mode).toBe('galaxy');
        expect(seam().hiddenKinds).toEqual(['RAISES']);
    });

    it('tauscht die Legende gegen die Saetze der Projektion', async () => {
        await render(props({ walk: walkOf(), legendStore: openStore() }));
        expect(legendKeys())
            .toEqual(['edge-color', 'positions', 'node-color', 'node-size', 'focus', 'agent-trail']);
        const positions = container.querySelector('[data-entry="positions"]')?.textContent ?? '';
        expect(positions).toContain('call depth from the entry point');
        expect(positions).toContain('not the server layout');
        await act(async () => {
            modeChip('galaxy')?.click();
        });
        expect(legendKeys())
            .toEqual(['edge-color', 'node-color', 'node-size', 'focus', 'positions', 'agent-trail']);
        expect(container.querySelector('[data-entry="positions"]')?.textContent)
            .toContain('computed by the server');
    });

    it('faerbt aus dem Layout, wo es das Symbol kennt, und bleibt sonst neutral', async () => {
        await render(props({ walk: walkOf() }));
        // Der Griff nennt keine Farben; geprueft wird die Quelle ueber die
        // Projektion selbst in hierarchy-layout.test.ts. Hier zaehlt, dass das
        // Panel das geladene Layout ueberhaupt weiterreicht: ohne es waere die
        // Zahl der Knoten dieselbe, aber der Kopf spraeche von einem Bild ohne
        // jede Server-Angabe.
        expect(seam().nodes).toBe(3);
        expect(seam().hierarchy?.nodes).toBe(4);
    });

    it('folgt mit dem Ring dem Symbol vor dem Leser', async () => {
        await render(props({ walk: walkOf(), focusQualifiedName: WALK_QN.createUser }));
        expect(seam().pulsedQn).toBe(WALK_QN.createUser);
        await render(props({ walk: walkOf(), focusQualifiedName: WALK_QN.query }));
        expect(seam().pulsedQn).toBe(WALK_QN.query);
    });

    it('faellt auf den Schritt zurueck, wenn der Leser ausserhalb des Walks steht', async () => {
        await render(props({
            walk: walkOf(),
            focusQualifiedName: 'atlas.src.nowhere.hidden',
            stepQualifiedName: WALK_QN.listUsers,
        }));
        expect(seam().pulsedQn).toBe(WALK_QN.listUsers);
    });

    it('sagt es, wenn nichts von diesem Walk im Fokus steht', async () => {
        await render(props({ walk: walkOf() }));
        expect(seam().pulsedQn).toBe('');
        expect(noteText()).toContain('nothing of this walk is in focus');
    });

    it('oeffnet einen angeklickten Knoten der Projektion ueber denselben Weg', async () => {
        const onOpenNode = vi.fn();
        await render(props({ walk: walkOf(), onOpenNode }));
        let found = false;
        await act(async () => {
            found = seam().clickNode(WALK_QN.query);
        });
        expect(found).toBe(true);
        expect(onOpenNode).toHaveBeenCalledTimes(1);
        expect(onOpenNode.mock.calls[0][0]).toMatchObject({
            name: 'query',
            file_path: 'src/repo/db.ts',
            start_line: 17,
        });
        // Ein Knoten der Galaxie, der nicht im Walk vorkommt, ist hier kein Ziel.
        let alien = true;
        await act(async () => {
            alien = seam().clickNode('__env__DB_URL');
        });
        expect(alien).toBe(false);
    });
});

/*
 * Die vier Kleinigkeiten aus W10b, soweit sie ohne einen Pixel messbar sind.
 *
 * Der Ansichts-Schalter klappt (AC2), die Hierarchie waechst aus dem Fokus
 * (AC3), und der Kopf sagt, woher ihre Wurzel kommt. Was hier NICHT geprueft
 * wird, ist die Einpassung der Kamera: sie ist eine Aussage ueber ein Bild, und
 * in jsdom gibt es keines. Sie steht als Rechnung in camera-frame.test.ts und
 * als Messung am gerenderten Bild in tools/smoke-w10b.mjs.
 */
/**
 * Ein ResizeObserver, der nie etwas meldet.
 *
 * Die Tests darunter fahren das Panel AUFGEKLAPPT (`visible: true`), denn genau
 * darum geht es in AC2: was ein Klick tut, haengt daran, ob die Sektion offen
 * ist. Aufgeklappt haengt die Szene im Baum, und der Canvas der Uebernahme misst
 * sich selbst mit react-use-measure, das ohne ResizeObserver sofort abbricht.
 * Der Stummel meldet nie eine Groesse, der Canvas bleibt damit bei null mal null
 * und baut gar keinen WebGL-Kontext auf: was hier gemessen wird, ist das DOM des
 * Kopfes, und das Bild misst tools/smoke-w10b.mjs im Browser.
 */
class SilentResizeObserver {
    observe(): void {
        /* meldet nichts: siehe Kopf */
    }

    unobserve(): void {
        /* dito */
    }

    disconnect(): void {
        /* dito */
    }
}

const withScene = (): void => {
    (globalThis as unknown as Record<string, unknown>).ResizeObserver ??= SilentResizeObserver;
};

describe('GalaxyPanel: der Ansichts-Schalter klappt auch', () => {

    beforeEach(withScene);

    it('klappt auf einen Klick auf den aktiven Knopf zu', async () => {
        const onToggleVisible = vi.fn();
        await render(props({ visible: true, onToggleVisible }));
        expect(seam().mode).toBe('galaxy');
        await act(async () => {
            modeChip('galaxy')?.click();
        });
        expect(onToggleVisible).toHaveBeenCalledTimes(1);
        // Die Ansicht bleibt gewaehlt: zugeklappt ist keine dritte Ansicht.
        expect(seam().mode).toBe('galaxy');
        expect(modeChip('galaxy')?.getAttribute('data-action')).toBe('collapse');
    });

    it('klappt bei zugeklappter Sektion auf und waehlt dabei die Ansicht', async () => {
        const onToggleVisible = vi.fn();
        await render(props({ visible: false, walk: walkOf(), onToggleVisible }));
        expect(modeChip('hierarchy')?.getAttribute('data-action')).toBe('open');
        await act(async () => {
            modeChip('hierarchy')?.click();
        });
        expect(onToggleVisible).toHaveBeenCalledTimes(1);
        expect(seam().mode).toBe('hierarchy');
    });

    it('wechselt die Ansicht wie bisher, solange die Sektion offen ist', async () => {
        const onToggleVisible = vi.fn();
        await render(props({ visible: true, walk: walkOf(), onToggleVisible }));
        await act(async () => {
            modeChip('galaxy')?.click();
        });
        expect(onToggleVisible).not.toHaveBeenCalled();
        expect(seam().mode).toBe('galaxy');
        expect(modeChip('hierarchy')?.getAttribute('data-action')).toBe('switch');
    });

    it('geht denselben Weg wie der beschriftete Schalter daneben', async () => {
        const onToggleVisible = vi.fn();
        await render(props({ visible: true, onToggleVisible }));
        await act(async () => {
            container.querySelector<HTMLButtonElement>(
                '[data-testid="atlas-galaxy-collapse"]',
            )?.click();
        });
        const viaLabel = onToggleVisible.mock.calls.length;
        await act(async () => {
            modeChip('galaxy')?.click();
        });
        // Beide Wege rufen denselben Rueckruf, also koennen sie nicht in zwei
        // verschiedene Zustaende fuehren.
        expect(onToggleVisible.mock.calls.length).toBe(viaLabel + 1);
    });

    it('sagt am grauen Knopf, was fehlt, statt stumm zu sein', async () => {
        await render(props({ visible: true }));
        const chip = modeChip('hierarchy');
        expect(chip?.getAttribute('aria-disabled')).toBe('true');
        expect(chip?.getAttribute('data-action')).toBe('none');
        await act(async () => {
            chip?.click();
        });
        expect(seam().mode).toBe('galaxy');
        expect(noteText()).toContain('open a symbol or pick a way in');
    });
});

describe('GalaxyPanel: die Hierarchie aus dem Fokus', () => {

    beforeEach(withScene);

    it('baut die Projektion auch ohne Walk, aus dem Symbol im Fokus', async () => {
        await render(props({ visible: true, focusWalk: walkOf() }));
        expect(seam().hierarchyAvailable).toBe(true);
        expect(seam().hierarchyOrigin).toBe('focus');
        expect(modeChip('hierarchy')?.getAttribute('aria-disabled')).toBe('false');
        // Aber sie waehlt sich nicht selbst: ein Fokus entsteht bei jedem Klick
        // in den Code (Entscheidung 17).
        expect(seam().mode).toBe('galaxy');
    });

    it('zeigt sie auf Klick und nennt im Kopf die Herkunft der Wurzel', async () => {
        await render(props({ visible: true, focusWalk: walkOf() }));
        await act(async () => {
            modeChip('hierarchy')?.click();
        });
        expect(seam().mode).toBe('hierarchy');
        expect(headlineText()).toBe('hierarchy of createUser (in focus): 4 symbols, depth 3');
        expect(seam().hierarchy?.nodes).toBe(4);
    });

    it('laesst dem echten Spaziergang den Vortritt', async () => {
        const other = walkOf();
        const entry = {
            ...other,
            root: { ...other.root, name: 'listUsers' },
        };
        await render(props({ visible: true, walk: entry, focusWalk: walkOf() }));
        expect(seam().hierarchyOrigin).toBe('walk');
        expect(seam().mode).toBe('hierarchy');
        expect(headlineText()).toBe('hierarchy of listUsers: 4 symbols, depth 3');
    });

    it('ordnet die Spalten aus dem Fokus genau wie aus einem Walk', async () => {
        await render(props({ visible: true, walk: walkOf() }));
        const fromWalk = seam().hierarchy!.placements;
        await act(async () => {
            root.unmount();
        });
        root = createRoot(container);
        await render(props({ visible: true, focusWalk: walkOf() }));
        await act(async () => {
            modeChip('hierarchy')?.click();
        });
        expect(seam().hierarchy!.placements).toEqual(fromWalk);
    });
});
