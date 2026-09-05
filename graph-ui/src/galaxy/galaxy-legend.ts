/**
 * Was in der Galaxie zu sehen ist, in Worten, die der Server deckt.
 *
 * Das Panel zeigt farbige Punkte in verschiedenen Groessen, helle und dunkle
 * Linien und eine Kamera, die irgendwohin fliegt. Ohne Legende ist jede dieser
 * vier Erscheinungen eine Einladung zum Raten, und Raten wird in einem
 * Verstehens-Werkzeug schnell zu einer Behauptung ("die grossen roten sind die
 * wichtigen"). Diese Datei sagt stattdessen, woher jede Erscheinung kommt.
 *
 * Drei Regeln, an die sich jeder Eintrag haelt:
 *
 * 1. **Nur, was der Server wirklich rechnet.** Knotenfarbe und -groesse kommen
 *    aus cbm/src/ui/layout3d.c: die Farbe ist `stellar_color(degree)`, eine
 *    Spektralklassen-Skala ueber den Knotengrad, die Groesse ist eine Basis je
 *    Label plus ein Zuschlag aus dem Grad. Diese Oberflaeche faerbt keinen
 *    Knoten selbst; sie malt, was im Layout steht.
 * 2. **Die Kantenfarben kommen aus der Tabelle, die sie malt.** Importiert aus
 *    EdgeLines.tsx, nicht abgeschrieben. Gezeigt werden nur die Typen, die im
 *    geladenen Layout wirklich vorkommen: eine Legende mit zwanzig Kantenarten
 *    fuer einen Graphen mit dreien ist eine Legende fuer ein anderes Bild.
 *    Seit W9 steht neben jeder Art ihre Zahl, GEZAEHLT in der geladenen
 *    Antwort und nicht aus der Tabelle geraten, und die Liste ist absteigend
 *    sortiert: die haeufigste Beziehung dieses Projekts steht oben. Der Deckel,
 *    der die Liste frueher nach acht Arten abschnitt, ist damit weg. Er war
 *    gegen eine zu lange Legende gedacht, dagegen hilft seit W5c der eigene
 *    Bildlauf des Kastens, und seit W9 waere eine abgeschnittene Art eine, die
 *    man nicht mehr ausblenden kann.
 * 3. **Der Zustand des Kastens ueberlebt den Reload.** Wer die Legende
 *    zuklappt, hat eine Entscheidung getroffen, und sie beim naechsten Laden
 *    zu vergessen macht aus der Entscheidung eine Geste.
 * 4. **Jede Ansicht bekommt ihre eigenen Saetze.** Seit W4e zeigt dasselbe
 *    Panel zwei Bilder: die Galaxie und die Hierarchie des Vorwaerts-Walks. In
 *    der zweiten kommen die Positionen nicht vom Server, sondern sind die
 *    Aufruf-Tiefe. Es gibt darum zwei Saetze Eintraege und keine Formulierung,
 *    die fuer beide passen soll.
 * 5. **Die Kanten stehen oben, und die Saetze sind kurz.** Beides ist eine
 *    Korrektur aus dem Beweisbild von W9: die Legende ist 75 Pixel hoch, die
 *    zwoelf Arten dieses Fixtures brauchen davon vier Zeilen, und darunter
 *    standen frueher fuenf Absaetze Fliesstext. Wer die Legende oeffnete, sah
 *    darum als Erstes einen Satz ueber Knotenfarben und musste zu den Arten
 *    scrollen, die er anklicken wollte. Seit W9-1 steht der Kanten-Eintrag
 *    vorn: er ist der einzige, der auch ein Schalter ist, und er ist der, der
 *    zum Bild gehoert, das dieser Zyklus geaendert hat. Die Erklaersaetze sind
 *    dabei auf hoechstens drei Zeilen gekuerzt worden, ohne dass eine Aussage
 *    weggefallen ist: gestrichen wurde Wiederholung, nicht Inhalt.
 */

import { DEFAULT_EDGE_COLOR, EDGE_TYPE_COLORS } from './EdgeLines';
import { HIERARCHY_DEFAULT_COLOR } from './hierarchy-layout';
import type { GraphData } from './types';

/** Der Schluessel, unter dem der Klappzustand liegt. */
export const GALAXY_LEGEND_KEY = 'codeatlasweb.galaxy.legend';

/**
 * Ob die Legende ohne gespeicherte Antwort offen ist.
 *
 * Seit W5c: ZU. Bis dahin stand hier "offen", mit der Begruendung, ein Leser
 * solle die Erklaerung nicht suchen muessen. Die Begruendung stimmt und die
 * Folge war trotzdem falsch: in einem 440 Pixel breiten Panel nahm die
 * aufgeklappte Legende fast die Haelfte der Hoehe, und das Bild, das sie
 * erklaert, war der Rest (Nutzerfeedback 2026-08-29). Der Schalter steht
 * weiterhin sichtbar im Kopf, die Antwort merkt sich der Browser, und
 * aufgeklappt nimmt die Legende jetzt hoechstens einen kleinen Teil des Panels
 * und scrollt intern.
 */
export const GALAXY_LEGEND_DEFAULT_OPEN = false;

/** Ein farbiger Punkt in der Legende. */
export interface LegendSwatch {
    label: string;
    color: string;
    /** Wie oft diese Art im gezeigten Bild vorkommt. Fehlt, wo nichts zu zaehlen ist. */
    count?: number;
}

/** Ein erklaertes Element des Bildes. */
export interface LegendEntry {
    /** Stabiler Schluessel, auch als `data-entry` im DOM. */
    key: string;
    /** Woruber gesprochen wird. */
    title: string;
    /** Was es bedeutet, und woher es kommt. */
    detail: string;
    /** Farbige Punkte, wenn die Erklaerung welche hat. */
    swatches: LegendSwatch[];
    /**
     * Ob die Punkte dieses Eintrags Schalter sind (W9).
     *
     * Nur der Kanten-Eintrag traegt das: seine Punkte stehen fuer Arten, die
     * man aus dem Bild nehmen kann. Der graue Punkt der Hierarchie steht fuer
     * eine Abwesenheit ("zu diesem Symbol liegt keine Layout-Angabe vor"), und
     * eine Abwesenheit laesst sich nicht ausblenden.
     */
    filterable?: boolean;
}

/** Eine Kantenart des gezeigten Bildes, mit ihrer Zahl und ihrer Farbe. */
export interface EdgeKind {
    type: string;
    /** Wie viele Kanten dieser Art im gezeigten Bild stehen. */
    count: number;
    color: string;
}

/** Die Farbe eines Kantentyps, so wie EdgeLines sie malt. */
export function edgeColorFor(type: string): string {
    return EDGE_TYPE_COLORS[type] ?? DEFAULT_EDGE_COLOR;
}

/**
 * Die Kantenarten des gezeigten Bildes, mit ihrer Zahl und ihrer Farbe,
 * absteigend nach Zahl.
 *
 * Gezaehlt wird die Antwort, die wirklich geladen wurde, und nicht die
 * Farbtabelle abgefragt: eine Art, die dieses Projekt nicht hat, gehoert nicht
 * in seine Legende, und eine Zahl daneben, die nicht aus den Daten kommt, waere
 * eine Schaetzung im Gewand einer Messung.
 *
 * Bei gleicher Zahl entscheidet der Name, ordinal verglichen. Zwei Maschinen
 * mit verschiedenen Sortierregeln sollen die Legende nicht anders anordnen; das
 * ist dieselbe Regel wie in der Hierarchie-Projektion.
 *
 * Kanten ohne Typ (die Antwort laesst das Feld weg) zaehlen nirgends mit: sie
 * unter einem erfundenen Namen zu fuehren waere eine Art, die es nicht gibt.
 */
export function edgeKinds(data: GraphData | undefined): EdgeKind[] {
    if (data === undefined) {
        return [];
    }
    const counts = new Map<string, number>();
    for (const edge of data.edges) {
        const type = typeof edge.type === 'string' ? edge.type : '';
        if (type.length === 0) {
            continue;
        }
        counts.set(type, (counts.get(type) ?? 0) + 1);
    }
    return [...counts.entries()]
        .map(([type, count]) => ({ type, count, color: edgeColorFor(type) }))
        .sort((a, b) => (b.count - a.count) || compareText(a.type, b.type));
}

/** Ordinaler Vergleich, wie in hierarchy-layout.ts. Siehe {@link edgeKinds}. */
function compareText(a: string, b: string): number {
    if (a === b) {
        return 0;
    }
    return a < b ? -1 : 1;
}

/** Dieselben Arten als Punkte der Legende. */
export function edgeSwatches(data: GraphData | undefined): LegendSwatch[] {
    return edgeKinds(data).map((kind) => ({
        label: kind.type,
        color: kind.color,
        count: kind.count,
    }));
}

/**
 * Was im Kopf des Panels ueber die Kantenarten steht.
 *
 * Zwei Entscheidungen stecken in diesen wenigen Worten:
 *
 * 1. **Die Zahl der Arten steht immer da**, auch wenn nichts ausgeblendet ist.
 *    Sie ist die Einladung zur Legende ("es gibt hier zwoelf Arten"), und sie
 *    haelt die Zeile im Kopf am Leben: eine Zeile, die beim Ausblenden erst
 *    entsteht, macht das Panel in genau dem Moment hoeher, in dem der Leser
 *    sich das Bild ansieht, und verschiebt ihm die Szene unter dem Blick weg.
 *    (Dieselbe Verschiebung hat im Beweislauf die Pixelmessung zerlegt, bevor
 *    sie hier behoben wurde.)
 * 2. **Die ausgeblendeten kommen als Zahl UND als Ganzes**, damit "es fehlt
 *    etwas" nicht mit "es gibt nichts" verwechselt werden kann.
 *
 * Ohne eine einzige Art steht hier nichts: dann ist auch nichts zu sagen.
 */
export function edgeKindNote(total: number, hidden: number): string {
    if (total <= 0) {
        return '';
    }
    const head = `${total} edge ${total === 1 ? 'kind' : 'kinds'}`;
    return hidden <= 0 ? head : `${head}, ${hidden} of ${total} hidden`;
}

/**
 * Dasselbe Bild ohne die ausgeblendeten Kantenarten.
 *
 * Die Knoten bleiben alle stehen, auch die, die danach keine Linie mehr
 * beruehrt. Sie aus dem Bild zu nehmen waere die zweite, viel groessere
 * Aussage: der Filter sagt "diese Art von Beziehung nicht", nicht "diese
 * Symbole gibt es nicht".
 *
 * Ist nichts ausgeblendet, kommt das Objekt selbst zurueck: eine Kopie waere
 * eine neue Identitaet bei jedem Bild und damit ein neuer Aufbau der
 * Szene-Puffer, ohne dass sich etwas geaendert haette.
 */
export function withoutEdgeKinds(data: GraphData, hidden: ReadonlySet<string>): GraphData {
    if (hidden.size === 0) {
        return data;
    }
    return { ...data, edges: data.edges.filter((edge) => !hidden.has(edge.type)) };
}

/**
 * Die Eintraege der Legende, passend zum geladenen Layout.
 *
 * Ohne Layout bleiben die Saetze stehen und die Punkte fehlen: die Herkunft von
 * Farbe, Groesse und Position ist unabhaengig davon, ob schon etwas geladen
 * ist, die konkreten Kantenarten sind es nicht.
 */
export function galaxyLegendEntries(data: GraphData | undefined): LegendEntry[] {
    const swatches = edgeSwatches(data);
    const edgeDetail =
        swatches.length === 0
            ? 'one colour per edge type, from the table that draws the lines. The types appear once a layout is loaded.'
            : 'one colour per edge type, from the table that draws the lines. Counted in this '
                + 'layout, most frequent first. Click a kind to take it out; it stays here, dimmed.';
    return [
        {
            key: 'edge-color',
            title: 'edge colour',
            detail: edgeDetail,
            swatches,
            filterable: true,
        },
        {
            key: 'node-color',
            title: 'node colour',
            detail:
                'the server maps a symbol degree to a stellar spectral colour (layout3d.c, stellar_color): '
                + 'few edges give a red dwarf, many a blue giant. This panel paints what the layout sent.',
            swatches: [],
        },
        {
            key: 'node-size',
            title: 'node size',
            detail:
                'a base size per node label plus a boost from the same degree, both computed by the server. '
                + 'A bigger dot means more edges, not more importance.',
            swatches: [],
        },
        {
            key: 'focus',
            title: 'focus',
            detail:
                'the focused symbol and its direct neighbours stay bright, everything else dims. '
                + 'The focus follows the twin, and a click in the sky sends it back.',
            swatches: [],
        },
        {
            key: 'positions',
            title: 'positions',
            detail:
                'a deterministic layout computed by the server, not by this browser: '
                + 'the same index gives the same sky.',
            swatches: [],
        },
        /*
         * Die Spur der Agenten (W11b AC2).
         *
         * Sie steht hier, weil die Legende die Stelle ist, an der ein Leser
         * nachsieht, was eine Linie bedeutet, und weil diese eine Linie GENAU
         * DAS NICHT bedeutet, was alle anderen bedeuten: sie kommt aus
         * Ereignissen und nicht aus dem Index. Der Eintrag steht auch dann da,
         * wenn der Live-Modus aus ist; eine Legende, die nur erklaert, was
         * gerade zu sehen ist, waere eine, in der man nicht nachschlagen kann.
         * Ein Farbpunkt gehoert nicht dazu: die Farbe der Spur ist die Farbe
         * ihres Akteurs, und die steht am Koerper.
         */
        {
            key: 'agent-trail',
            title: 'agent trail (dashed)',
            detail:
                'a dashed line behind a live agent is the path it walked: the last symbols one actor '
                + 'touched, newest first, drawn UNDER the real edges. It is not a relation in the '
                + 'code. The solid coloured lines above come from the index and stay; this one comes '
                + 'from events and fades with the window picked at the timeline.',
            swatches: [],
        },
    ];
}

/**
 * Dieselben fuenf Elemente, erklaert fuer die Hierarchie-Ansicht.
 *
 * Ein eigener Satz und nicht derselbe: in der Projektion bedeuten zwei der
 * fuenf Erscheinungen etwas anderes. Die Position kommt nicht mehr vom Server,
 * sondern ist die Aufruf-Tiefe, und Farbe und Groesse gelten nur fuer die
 * Symbole, die im geladenen Layout ueberhaupt vorkommen. Die Legende weiter die
 * Galaxie-Saetze zeigen zu lassen waere die teuerste Sorte Legende: eine, die
 * das Bild daneben falsch erklaert.
 *
 * Die Schluessel sind dieselben wie in {@link galaxyLegendEntries}, damit ein
 * Leser die Zeile, die er sucht, an derselben Stelle findet.
 */
export function hierarchyLegendEntries(data: GraphData | undefined): LegendEntry[] {
    const swatches = edgeSwatches(data);
    return [
        {
            key: 'edge-color',
            title: 'edges',
            /*
             * Drei Zeilen, und die Zahl ist gemessen: in der Hierarchie steht
             * ueber diesem Satz eine Zeile mit den Arten, und die Legende
             * fasst vier Zeilen. Was laenger ist, endet an der Kante des
             * Kastens, und genau das war der Befund an den Beweisbildern von
             * W9 (Bernhard, 2026-08-29). Gestrichen ist die Ausfuehrung, nicht
             * die Aussage: die Zyklen stehen weiter da, die uebrigen
             * Beziehungen auch, und der Griff zum Ausblenden steht am
             * Punkt selbst (`title`).
             */
            detail:
                'one line per call on this walk, cycles included rather than hidden. '
                + 'What else the index records between these symbols is drawn in its own colour: '
                + 'the calls make the columns.',
            swatches,
            filterable: true,
        },
        {
            key: 'positions',
            title: 'positions',
            detail:
                'columns are the call depth from the entry point, and inside a column the symbols '
                + 'are ordered by name. A deterministic projection of the walk, not the server layout: '
                + 'the same walk always draws the same picture.',
            swatches: [],
        },
        {
            key: 'node-color',
            title: 'node colour',
            detail:
                'taken from the loaded galaxy layout, where the server maps a symbol degree to a '
                + 'stellar spectral colour (layout3d.c, stellar_color). A symbol the layout does not '
                + `carry stays neutral grey (${HIERARCHY_DEFAULT_COLOR}): nothing here knows its degree.`,
            swatches: [{ label: 'no layout entry', color: HIERARCHY_DEFAULT_COLOR }],
        },
        {
            key: 'node-size',
            title: 'node size',
            detail:
                'from the same layout entry, so a bigger dot means more edges in the whole graph, '
                + 'not more importance and not a deeper place in this walk. Sizes are held inside a '
                + 'band, so that one very large dot cannot push its own name into its neighbour\'s.',
            swatches: [],
        },
        {
            key: 'focus',
            title: 'focus',
            detail:
                'the symbol the reader stands on carries a ring that follows every step of the walk. '
                + 'Nothing else dims here: the whole subgraph is the answer. A click on a node opens '
                + 'its file and takes the twin with it, exactly as in the galaxy.',
            swatches: [],
        },
        {
            key: 'agent-trail',
            title: 'agent trail (dashed)',
            detail:
                'a dashed line behind a live agent is the path it walked, drawn under the real '
                + 'edges. It is not a relation in the code: the solid lines come from the index, '
                + 'this one comes from events and fades with the window picked at the timeline.',
            swatches: [],
        },
    ];
}

/** Der gespeicherte Klappzustand. Ein unlesbarer Speicher entscheidet nichts. */
export function readLegendOpen(store: Storage | undefined): boolean {
    try {
        const raw = store?.getItem(GALAXY_LEGEND_KEY);
        if (raw === 'open') {
            return true;
        }
        if (raw === 'closed') {
            return false;
        }
    } catch {
        // Ein Speicher, der nicht antwortet (privates Fenster, gesperrte Domain),
        // ist kein Grund, die Legende zu verstecken.
    }
    return GALAXY_LEGEND_DEFAULT_OPEN;
}

/** Den Klappzustand merken. Schlaegt es fehl, bleibt es bei dieser Sitzung. */
export function writeLegendOpen(store: Storage | undefined, open: boolean): void {
    try {
        store?.setItem(GALAXY_LEGEND_KEY, open ? 'open' : 'closed');
    } catch {
        // siehe readLegendOpen
    }
}
