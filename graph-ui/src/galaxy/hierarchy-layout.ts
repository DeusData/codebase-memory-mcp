/**
 * Der Vorwaerts-Walk als Bild: eine Spalte je Aufruf-Tiefe, ab dem gewaehlten
 * Einstiegspunkt.
 *
 * Die Galaxie zeigt, wo ein Symbol im ganzen Projekt liegt. Das ist eine
 * Antwort auf "wo bin ich" und keine auf "wie tief geht das hier": im
 * Server-Layout liegen die drei Symbole, die einander der Reihe nach aufrufen,
 * irgendwo in einer Wolke aus tausend anderen, und die Kette dazwischen ist
 * eine von zehntausend Linien. Diese Datei rechnet denselben Walk, den der
 * Einstiegsmodus ohnehin schon laeuft (src/provider/closure.ts), in ein Bild
 * um, in dem die Tiefe die Bildachse ist: die Wurzel links, was sie aufruft in
 * der naechsten Spalte, was jene aufrufen in der uebernaechsten.
 *
 * Fuenf Regeln, und jede ist eine Antwort auf eine Frage, die man beim Lesen
 * sofort stellt:
 *
 * 1. **Die Projektion ist rein und deterministisch.** Kein Zufallsgenerator,
 *    kein Kraefte-Layout, keine Uhr: die Spalte ist der Hop, die Zeile ist die
 *    ordinale Position des Namens in seiner Ebene. Zweimal dieselbe Eingabe
 *    gibt zweimal dasselbe Bild, und zwei Leser, die dasselbe Symbol waehlen,
 *    reden ueber dasselbe Bild. Verglichen wird ordinal und nie mit
 *    `localeCompare`, aus demselben Grund wie im Walk selbst: zwei Maschinen
 *    mit verschiedenen Sortierregeln sollen die Ebene nicht anders anordnen.
 * 2. **Es wird keine Kante erfunden und keine versteckt.** Gezeichnet werden
 *    genau die `via`-Kanten des Walks, einschliesslich der Kanten, die in ein
 *    schon besuchtes Symbol zurueckfuehren. Genau die sind die Zyklen, und ein
 *    Bild, das sie weglaesst, zeigt einen Baum, wo ein Graph ist.
 * 3. **Die Grenze reist mit.** `truncated`, `cap` und die Tiefe des Walks
 *    stehen im Ergebnis, damit die Kopfzeile sagen kann, dass hinter der
 *    letzten Spalte noch etwas liegt. Ein Bild, das an seiner Grenze einfach
 *    aufhoert, behauptet, dort sei das Ende des Codes.
 * 4. **Farbe, Groesse und Label kommen vom Server, wo es sie gibt.** Sie sind
 *    Aussagen ueber den Knotengrad im GANZEN Graphen (layout3d.c,
 *    `stellar_color`), und die bleibt wahr, egal in welchem Bild der Punkt
 *    liegt. Findet sich das Symbol nicht im geladenen Layout, bekommt es einen
 *    neutralen Grauton und eine mittlere Groesse: eine gerechnete Sternfarbe
 *    waere eine Behauptung ueber einen Grad, den diese Datei nicht kennt.
 * 5. **Datei und Zeile kommen aus dem Walk.** Der Walk hat jedes erreichte
 *    Symbol beim Index aufgeloest; das ist die genauere Quelle, und sie ist
 *    auch fuer die Symbole da, die im Layout gar nicht vorkommen. Ein Klick auf
 *    einen Knoten dieses Bildes fuehrt deshalb ueber denselben Weg wie ein
 *    Klick in die Galaxie.
 *
 * Seit W9 kommt eine sechste dazu, und sie ist ausdruecklich NEBEN die
 * Projektion gebaut und nicht in sie hinein:
 *
 * 6. **Die uebrigen Beziehungen werden gezeichnet, nicht gerechnet.** Was
 *    dieselben Symbole ausser dem Aufruf noch verbindet (ein Fehlerpfad, ein
 *    Verweis, eine Typnutzung), steht in der schon geladenen Layout-Antwort und
 *    wird von {@link hierarchyIndexEdges} dazugelegt. Die Spalten entstehen
 *    weiter allein aus dem Walk: {@link projectHierarchy} sieht diese Kanten
 *    nie, kann also auch nichts nach ihnen umordnen. Das ist keine Vorsicht,
 *    sondern die Zusicherung selbst (Regel 1): eine Beziehung, die nicht der
 *    Aufruf ist, darf die Aufruf-Tiefe nicht verschieben.
 */

import { closureKeyOf } from '../provider/closure';
import type { ClosureNode, ClosureResult } from '../provider/closure';
import { toGraphLine } from '../core/positions';
import { twinLocationOf } from '../twin/twin-target';
import { capNote } from '../tours/tour-player';
import { nodesByQualifiedName } from './galaxy-model';
import type { FrameBox } from './camera-frame';
import type { GraphData, GraphEdge, GraphNode } from './types';

/**
 * Der Abstand zweier Spalten, in den Einheiten dieses Bildes.
 *
 * Dieses Bild hat sein eigenes Koordinatensystem, und das muss es haben: die
 * Zahlen des Server-Layouts spannen eine Wolke aus tausenden Knoten auf, hier
 * liegen hoechstens sechzig. Die Zahlen sind zusammen mit
 * {@link HIERARCHY_ROW_HEIGHT} und {@link HIERARCHY_SIZE_SCALE} an einer
 * Bedingung ausgerichtet, die man sonst erst am fertigen Bild merkt: die Kamera
 * der Szene stellt sich auf den dreifachen Radius der Knotenkugel
 * (`computeCameraTarget`), und die Beschriftungen sind in Weltmasse gross
 * (NodeLabels: rund das Vierfache der Knotengroesse). Zu weit auseinander
 * gelegte Spalten heissen deshalb nicht "mehr Platz", sondern eine Kamera, die
 * zurueckweicht, bis die Namen unlesbar sind.
 */
export const HIERARCHY_COLUMN_WIDTH = 120;

/**
 * Der Abstand zweier Knoten innerhalb einer Spalte.
 *
 * Seit W5c 72 statt 55, und das ist kein Geschmacksurteil, sondern die
 * Bedingung, unter der zwei Namen in einer Spalte sich nicht beruehren koennen:
 * eine Beschriftung ist in dieser Ansicht {@link HIERARCHY_LABEL_FONT_SIZE}
 * Weltmasse hoch und sitzt um bis zu {@link HIERARCHY_MAX_SIZE} mal 0.7 ueber
 * ihrem Knoten. Zusammen mit dem Deckel auf der Knotengroesse bleibt zwischen
 * zwei Kaesten immer Luft (Nutzerfeedback 2026-08-29: ueberlagerte Namen bei
 * kleinen Walks).
 */
export const HIERARCHY_ROW_HEIGHT = 72;

/**
 * Die Farbe eines Symbols, das im geladenen Layout nicht vorkommt.
 *
 * Ein neutraler Grauton, ausdruecklich ausserhalb der Spektralskala des
 * Servers: er sagt "hierzu liegt keine Layout-Angabe vor" und nicht "dieses
 * Symbol hat wenige Verbindungen".
 */
export const HIERARCHY_DEFAULT_COLOR = '#8FA3B0';

/** Die Groesse eines solchen Symbols, vor der Umrechnung. Siehe unten. */
export const HIERARCHY_DEFAULT_SIZE = 5;

/**
 * Womit die Groessen des Servers in die Einheiten dieses Bildes umgerechnet
 * werden.
 *
 * Das VERHAELTNIS bleibt: ein Knoten, den der Server doppelt so gross malt,
 * bleibt hier doppelt so gross, und die Aussage dahinter (mehr Kanten im ganzen
 * Graphen) bleibt dieselbe. Die absolute Zahl bleibt nicht, und sie darf es
 * nicht: sie ist in den Koordinaten des Server-Layouts gemessen, und in denen
 * rechnet dieses Bild nicht. Ein Punkt, der fuer eine Wolke aus fuenftausend
 * Knoten bemessen ist, waere hier ein Staubkorn mit einer unlesbaren Fahne
 * darueber.
 */
export const HIERARCHY_SIZE_SCALE = 4;

/**
 * Das Band, in dem die Knotengroessen dieser Ansicht liegen.
 *
 * Ein Deckel und ein Boden, und beide sind eine Aenderung an Regel 4 des Kopfes,
 * die genannt sein will: das VERHAELTNIS bleibt im ganzen mittleren Bereich
 * erhalten, an den Raendern nicht mehr. Der Grund steht im Bild selbst. Die
 * Beschriftung sitzt in Weltmasse ueber ihrem Knoten, mit einem Abstand, der an
 * dessen Groesse haengt; ein einzelner sehr grosser Knoten schiebt seinen Namen
 * so weit nach oben, dass er den Namen darueber trifft, und ein sehr kleiner
 * legt seinen Namen in den eigenen Punkt. Die Legende sagt dieses Band mit.
 */
export const HIERARCHY_MIN_SIZE = 10;
export const HIERARCHY_MAX_SIZE = 34;

/**
 * Die feste Schriftgroesse der Beschriftungen dieser Ansicht, in Weltmasse.
 *
 * Fest und nicht aus der Knotengroesse abgeleitet: in der Galaxie ist die
 * Groesse eines Namens ein zweiter Traeger derselben Aussage wie die Groesse
 * des Punktes, in einem Bild aus hoechstens sechzig Punkten ist sie nur noch
 * der Grund, warum zwei Namen verschieden viel Platz brauchen und sich
 * ausgerechnet dort ueberlagern, wo viel steht.
 */
export const HIERARCHY_LABEL_FONT_SIZE = 12;

/**
 * Die Breitengrenze eines Namens, in Texturpixeln.
 *
 * So gewaehlt, dass ein Kasten schmaler bleibt als {@link
 * HIERARCHY_COLUMN_WIDTH}: bei dieser Schriftgroesse wird aus 500 Pixeln Text
 * knapp 106 Einheiten Weltbreite, und zwei Spalten stehen 120 auseinander. Ein
 * laengerer Name wird gekuerzt und nicht kleiner gesetzt.
 */
export const HIERARCHY_LABEL_MAX_TEXT_WIDTH = 500;

/** Wie viel Platz die Beschriftungen um die Knoten herum brauchen, fuer die Rahmung. */
export const HIERARCHY_LABEL_PAD_X = 55;
export const HIERARCHY_LABEL_PAD_TOP = 46;
export const HIERARCHY_LABEL_PAD_BOTTOM = 22;

/**
 * Der Kantentyp, unter dem die Linien gemalt werden.
 *
 * Jede Kante des Walks ist ein aufgezeichneter Aufruf, also traegt sie den
 * Namen, den der Graph dafuer fuehrt, und bekommt in der Szene die Farbe, die
 * die Kantentabelle fuer Aufrufe haelt.
 */
export const HIERARCHY_EDGE_TYPE = 'CALLS';

/**
 * Bis zu wie vielen Knoten das Bild beschriftet wird.
 *
 * Der harte Deckel des Walks liegt bei 60 Symbolen (CLOSURE_MAX_CAP), also ist
 * das die Zahl, bei der die Beschriftung noch an ist: ein Bild aus dieser Datei
 * hat nie mehr Punkte, und ohne Namen waere eine Hierarchie eine Reihe Punkte.
 */
export const HIERARCHY_LABEL_BUDGET = 60;

/** Wo ein Symbol im Bild sitzt, und warum dort. */
export interface HierarchyPlacement {
    /** Die Identitaet des Symbols im Walk (qualifizierter Name, wo es einen gibt). */
    key: string;
    /** Der Anzeigename. */
    name: string;
    /** Hops von der Wurzel. Die Wurzel selbst ist null. */
    hop: number;
    /** Die `id` des erzeugten Szene-Knotens. */
    id: number;
    x: number;
    y: number;
}

/** Der Walk als Szene, plus alles, was die Kopfzeile darueber sagen muss. */
export interface HierarchyProjection {
    /** Die Daten, so wie GraphScene sie erwartet. */
    data: GraphData;
    /** Die `id` des Wurzelknotens. Die Wurzel ist markiert und nicht geraten. */
    rootId: number;
    /** Die Identitaet der Wurzel im Walk. */
    rootKey: string;
    /** Ihr Anzeigename, fuer die Kopfzeile. */
    rootName: string;
    /** Wie viele Symbole im Bild stehen. */
    symbols: number;
    /** Wie viele Ebenen es hat, die Wurzel als erste gezaehlt. */
    depth: number;
    /** Ob eine Grenze den Walk gestoppt hat, waehrend noch etwas offen war. */
    truncated: boolean;
    /** Der Symbol-Deckel, mit dem gelaufen wurde. */
    cap: number;
    /** Die Hop-Grenze, mit der gelaufen wurde. */
    walkDepth: number;
    /** Symbole, die der Walk gesehen und nicht zurueckgegeben hat. */
    missing: number;
    /** Je Knoten: wo er sitzt und warum. Der Beweislauf liest daran die Spalten. */
    placements: HierarchyPlacement[];
}

/** Die Knoten, die eine Projektion aus dem Server-Layout uebernehmen darf. */
export interface HierarchyOptions {
    /** Das geladene Galaxy-Layout, wenn eines dasteht. */
    layout?: GraphData | undefined;
}

/** Die Groesse in das Band dieser Ansicht holen. Begruendung an den Konstanten. */
function clampSize(size: number): number {
    if (!Number.isFinite(size)) {
        return HIERARCHY_DEFAULT_SIZE * HIERARCHY_SIZE_SCALE;
    }
    return Math.min(HIERARCHY_MAX_SIZE, Math.max(HIERARCHY_MIN_SIZE, size));
}

/** Ordinaler Vergleich. Siehe Regel 1 im Kopf. */
function compareText(a: string, b: string): number {
    if (a === b) {
        return 0;
    }
    return a < b ? -1 : 1;
}

/**
 * Wonach eine Ebene sortiert wird.
 *
 * Der Name zuerst, weil die Reihenfolge im Bild eine fuer Leser ist. Der
 * Schluessel dahinter, damit zwei gleichnamige Symbole aus zwei Dateien eine
 * feste Reihenfolge haben statt der, in der der Walk sie zufaellig fand.
 */
function sortKeyOf(node: ClosureNode): string {
    return `${node.symbol.name} ${closureKeyOf(node.symbol)}`;
}

/**
 * Die letzte Zeile einer Deklaration, in Graphzeilen.
 *
 * `range.end` ist der Anfang der FOLGENDEN Zeile (die LSP-uebliche Art, "bis
 * zum Ende dieser Zeile" zu sagen, siehe core/positions.ts), also ist die
 * letzte Zeile eine weniger. Nie kleiner als die erste.
 */
function endLineOf(node: ClosureNode, startLine: number): number {
    return Math.max(startLine, toGraphLine(node.symbol.range.end.line) - 1);
}

/**
 * Einen Walk in ein Bild uebersetzen.
 *
 * Rein: dieselbe Eingabe gibt dasselbe Ergebnis, und es wird nichts geladen,
 * nichts gemessen und nichts geraten.
 */
export function projectHierarchy(
    closure: ClosureResult,
    options: HierarchyOptions = {},
): HierarchyProjection {
    const server =
        options.layout === undefined
            ? new Map<string, GraphNode>()
            : nodesByQualifiedName(options.layout.nodes);

    // Ebenen ueber den Hop und nicht ueber die Reihenfolge des Arrays: der Walk
    // liefert seine Knoten zwar schon nach Hop geordnet, aber die Spalte soll an
    // der Zahl haengen, die den Abstand meint, nicht an einer Reihenfolge, die
    // eines Tages jemand anders sortiert.
    const levels = new Map<number, ClosureNode[]>();
    for (const node of closure.nodes) {
        const hop = Number.isFinite(node.hop) ? Math.max(0, Math.floor(node.hop)) : 0;
        const level = levels.get(hop);
        if (level === undefined) {
            levels.set(hop, [node]);
        } else {
            level.push(node);
        }
    }

    const hops = [...levels.keys()].sort((a, b) => a - b);
    const nodes: GraphNode[] = [];
    const placements: HierarchyPlacement[] = [];
    const idByKey = new Map<string, number>();

    for (const hop of hops) {
        const level = [...(levels.get(hop) ?? [])].sort((a, b) =>
            compareText(sortKeyOf(a), sortKeyOf(b)));
        const x = hop * HIERARCHY_COLUMN_WIDTH;
        for (const [position, entry] of level.entries()) {
            const key = closureKeyOf(entry.symbol);
            if (idByKey.has(key)) {
                // Ein Symbol steht einmal im Bild. Der Walk gibt es ohnehin nur
                // einmal zurueck; die Pruefung steht hier, damit eine von Hand
                // gebaute Eingabe nicht zwei Punkte an zwei Orten fuer dasselbe
                // Symbol erzeugt und die Kanten dann raten muessten, welcher
                // gemeint ist.
                continue;
            }
            // Zentriert, und der alphabetisch erste sitzt oben: in der Szene
            // zeigt +y nach oben.
            const y = ((level.length - 1) / 2 - position) * HIERARCHY_ROW_HEIGHT;
            const id = nodes.length;
            const qualifiedName = entry.symbol.qualifiedName;
            const fromServer =
                qualifiedName === undefined || qualifiedName.length === 0
                    ? undefined
                    : server.get(qualifiedName);
            const location = twinLocationOf(entry.symbol);

            const node: GraphNode = {
                id,
                x,
                y,
                z: 0,
                // Ohne Layout-Knoten steht hier die Symbolart, wie der Index sie
                // beim Aufloesen genannt hat, und nicht das Label der Engine:
                // eines zu erfinden hiesse, eine Klassifikation zu behaupten.
                label: fromServer?.label ?? entry.symbol.kind,
                name: entry.symbol.name,
                size: clampSize((fromServer?.size ?? HIERARCHY_DEFAULT_SIZE) * HIERARCHY_SIZE_SCALE),
                color: fromServer?.color ?? HIERARCHY_DEFAULT_COLOR,
            };
            if (qualifiedName !== undefined && qualifiedName.length > 0) {
                node.qualified_name = qualifiedName;
            }
            if (location.path.length > 0) {
                node.file_path = location.path;
                node.start_line = location.line;
                node.end_line = endLineOf(entry, location.line);
            }
            if (fromServer?.status !== undefined) {
                node.status = fromServer.status;
            }
            if (fromServer?.in_calls !== undefined) {
                node.in_calls = fromServer.in_calls;
            }
            if (fromServer?.out_calls !== undefined) {
                node.out_calls = fromServer.out_calls;
            }

            nodes.push(node);
            idByKey.set(key, id);
            placements.push({ key, name: entry.symbol.name, hop, id, x, y });
        }
    }

    /*
     * Die Kanten, in der Reihenfolge des Walks.
     *
     * Eine Kante, deren Enden nicht beide im Bild stehen, wird weggelassen und
     * nicht mit einem erfundenen Punkt verbunden. Aus `getClosure` kommt so eine
     * Kante nicht (dort ist genau das eine Invariante); die Pruefung steht hier,
     * weil eine Linie auf eine `undefined`-Position in der Szene keine Linie
     * waere, sondern ein NaN.
     */
    const edges: GraphEdge[] = [];
    for (const edge of closure.edges) {
        const source = idByKey.get(edge.from);
        const target = idByKey.get(edge.to);
        if (source === undefined || target === undefined) {
            continue;
        }
        edges.push({ source, target, type: HIERARCHY_EDGE_TYPE });
    }

    const rootKey = closureKeyOf(closure.root);
    const lastHop = hops.length === 0 ? -1 : (hops[hops.length - 1] as number);

    return {
        data: {
            nodes,
            edges,
            // Was der Walk insgesamt gesehen hat. `total_nodes` heisst in der
            // Szene "wie viele es gaebe", und das ist hier genau diese Zahl.
            total_nodes: closure.visited,
        },
        rootId: idByKey.get(rootKey) ?? 0,
        rootKey,
        rootName: closure.root.name,
        symbols: nodes.length,
        depth: lastHop + 1,
        truncated: closure.truncated,
        cap: closure.cap,
        walkDepth: closure.depth,
        missing: Math.max(0, closure.visited - nodes.length),
        placements,
    };
}

/**
 * Der seitliche Abstand zweier Linien zwischen denselben zwei Symbolen.
 *
 * Neun Welteinheiten, gemessen an den beiden Zahlen, die dieses Bild sonst
 * bestimmen: eine Spalte ist {@link HIERARCHY_COLUMN_WIDTH} (120) breit, eine
 * Zeile {@link HIERARCHY_ROW_HEIGHT} (72) hoch. Neun ist damit klein genug,
 * dass die zweite Linie erkennbar zu denselben zwei Punkten gehoert, und gross
 * genug, dass sie bei der Rahmung dieses Bildes (rund 360 Einheiten Breite in
 * einem 440 Pixel breiten Panel) als eigener Strich zu sehen ist und nicht als
 * dickere Kante.
 */
export const HIERARCHY_LANE_SPACING = 9;

/**
 * Alles, was der Index ausser den Aufrufen zwischen den GEZEIGTEN Symbolen
 * kennt.
 *
 * Aus der schon geladenen Layout-Antwort und nicht aus einem zweiten Serverweg:
 * die Antwort liegt vor, sie enthaelt die Beziehung, und sie noch einmal zu
 * holen waere eine zweite Wahrheit ueber denselben Index. Verbunden werden
 * beide Seiten ueber den qualifizierten Namen, den einzigen Schluessel, den
 * Layout und Walk gleich schreiben; ein Symbol ohne qualifizierten Namen kommt
 * darum nicht vor, und das ist ehrlicher, als es ueber Datei und Zeile zu
 * raten.
 *
 * Drei Regeln:
 *
 * 1. **Was schon im Bild steht, wird nicht zweimal gezeichnet.** Der Walk hat
 *    seine Aufrufe schon gemalt; dieselbe Kante aus dem Layout noch einmal
 *    darueberzulegen waere eine doppelt so helle Linie und keine Aussage.
 * 2. **Mehrere Beziehungen zwischen denselben zwei Symbolen bekommen eigene
 *    Spuren.** Additive Blendung macht aus einer roten Linie auf einer gruenen
 *    eine gelbe, und Gelb steht in keiner Legende. Die Aufrufkante behaelt die
 *    Mitte, jede weitere ruecht um {@link HIERARCHY_LANE_SPACING} zur Seite.
 * 3. **Die Reihenfolge ist fest.** Sortiert wird nach Quelle, Ziel und Typ,
 *    ordinal; damit ist auch die Spur einer Kante bei jedem Aufruf dieselbe.
 */
export function hierarchyIndexEdges(
    projection: HierarchyProjection,
    layout: GraphData | undefined,
): GraphEdge[] {
    if (layout === undefined) {
        return [];
    }
    const idByName = new Map<string, number>();
    for (const node of projection.data.nodes) {
        const qualified = node.qualified_name;
        if (qualified !== undefined && qualified.length > 0 && !idByName.has(qualified)) {
            idByName.set(qualified, node.id);
        }
    }
    if (idByName.size === 0) {
        return [];
    }
    const nameById = new Map<number, string>();
    for (const node of layout.nodes) {
        const qualified = node.qualified_name;
        if (qualified !== undefined && qualified.length > 0) {
            nameById.set(node.id, qualified);
        }
    }

    const pairKey = (a: number, b: number): string => (a <= b ? `${a}:${b}` : `${b}:${a}`);
    /** Wie viele Linien auf einem Paar schon liegen. Die Walk-Kanten zuerst. */
    const lanes = new Map<string, number>();
    const drawn = new Set<string>();
    for (const edge of projection.data.edges) {
        const key = pairKey(edge.source, edge.target);
        lanes.set(key, Math.max(1, lanes.get(key) ?? 0));
        drawn.add(`${edge.source}>${edge.target}:${edge.type}`);
    }

    const found: GraphEdge[] = [];
    for (const edge of layout.edges) {
        const fromName = nameById.get(edge.source);
        const toName = nameById.get(edge.target);
        if (fromName === undefined || toName === undefined) {
            continue;
        }
        const source = idByName.get(fromName);
        const target = idByName.get(toName);
        if (source === undefined || target === undefined) {
            continue;
        }
        const type = typeof edge.type === 'string' ? edge.type : '';
        if (type.length === 0) {
            continue;
        }
        const key = `${source}>${target}:${type}`;
        if (drawn.has(key)) {
            continue;
        }
        drawn.add(key);
        found.push({ source, target, type });
    }

    found.sort((a, b) =>
        (a.source - b.source)
        || (a.target - b.target)
        || compareText(a.type, b.type));

    return found.map((edge) => {
        const key = pairKey(edge.source, edge.target);
        const used = lanes.get(key) ?? 0;
        lanes.set(key, used + 1);
        return used === 0 ? edge : { ...edge, offset: used * HIERARCHY_LANE_SPACING };
    });
}

/**
 * Das Rechteck, das die Kamera rahmen muss, damit alles im Bild steht.
 *
 * Nicht die Knoten allein: eine Beschriftung steht neben und ueber ihrem
 * Knoten, und eine Kamera, die nur die Punkte rahmt, schneidet die aeussersten
 * Namen ab. Die Zuschlaege sind grosszuegig und ausdruecklich keine Messung: was
 * ein Name wirklich einnimmt, weiss nur die Szene, die ihn zeichnet, und sie
 * meldet es (NodeLabels.onLayout). Hier wird gerahmt, nicht behauptet.
 *
 * Total: eine Projektion ohne Knoten ergibt ein Rechteck um den Ursprung.
 */
export function hierarchyFrame(projection: HierarchyProjection): FrameBox {
    if (projection.placements.length === 0) {
        return { centerX: 0, centerY: 0, width: HIERARCHY_COLUMN_WIDTH, height: HIERARCHY_ROW_HEIGHT };
    }
    let minX = Infinity;
    let maxX = -Infinity;
    let minY = Infinity;
    let maxY = -Infinity;
    for (const placement of projection.placements) {
        minX = Math.min(minX, placement.x);
        maxX = Math.max(maxX, placement.x);
        minY = Math.min(minY, placement.y);
        maxY = Math.max(maxY, placement.y);
    }
    const left = minX - HIERARCHY_LABEL_PAD_X;
    const right = maxX + HIERARCHY_LABEL_PAD_X;
    const bottom = minY - HIERARCHY_LABEL_PAD_BOTTOM;
    const top = maxY + HIERARCHY_LABEL_PAD_TOP;
    return {
        centerX: (left + right) / 2,
        centerY: (bottom + top) / 2,
        width: right - left,
        height: top - bottom,
    };
}

/**
 * Woher die Wurzel dieses Bildes kommt (W10b).
 *
 * `walk` ist der Einstiegs-Spaziergang: jemand hat einen Einstiegspunkt gewaehlt
 * und laeuft ihn ab. `focus` ist das Symbol, das der Leser gerade vor sich hat;
 * daraus entsteht derselbe Vorwaerts-Closure mit denselben Grenzen, aber es ist
 * kein Spaziergang, und niemand hat einen Einstieg gewaehlt.
 */
export type HierarchyRootOrigin = 'walk' | 'focus';

/**
 * Was ueber dem Bild steht.
 *
 * Das gewaehlte Symbol, die Zahl der gezeigten Symbole, die Zahl der Ebenen und,
 * wenn eine Grenze gegriffen hat, derselbe Satz, mit dem auch die Schrittkarte
 * einen gekappten Walk beschliesst. Zwei verschiedene Formulierungen fuer
 * dieselbe Grenze waeren zwei Wahrheiten.
 *
 * Seit W10b nennt der Kopf ausserdem die HERKUNFT der Wurzel, wenn sie nicht aus
 * einem Einstiegs-Spaziergang kommt (AC3). Der Unterschied ist keine Feinheit:
 * ein Spaziergang ist eine Entscheidung ("hier fange ich an"), ein Fokus ist ein
 * Ort ("hier stehe ich gerade"), und dasselbe Bild bedeutet in beiden Faellen
 * etwas anderes. Der Satz fuer den Spaziergang bleibt woertlich der von W4e:
 * mehrere Beweislaeufe lesen ihn bis zu seinem Ende.
 *
 * Die Herkunft steht direkt hinter dem NAMEN und nicht am Satzende, und das ist
 * gemessen und nicht Geschmack: diese Zeile ist rund 417 Pixel breit, bricht
 * nicht um und endet bei etwa dreiundsechzig Zeichen in einem
 * Auslassungszeichen. Ein Zusatz am Ende waere im Beweisbild von W10b genau das
 * gewesen, was man nicht mehr liest; hier steht er da, bevor die Zeile knapp
 * wird.
 */
export function hierarchyHeadline(
    projection: HierarchyProjection,
    origin: HierarchyRootOrigin = 'walk',
): string {
    const head =
        `hierarchy of ${projection.rootName}${origin === 'focus' ? ' (in focus)' : ''}: `
        + `${projection.symbols} ${projection.symbols === 1 ? 'symbol' : 'symbols'}, `
        + `depth ${projection.depth}`;
    const cap = capNote(projection.truncated, projection.cap, projection.walkDepth);
    return cap.length === 0 ? head : `${head}; ${cap}`;
}

/**
 * Woraus die Linien dieses Bildes bestehen (W9).
 *
 * Eine eigene Zeile im Kopf und ausdruecklich nicht ein Anhang an
 * {@link hierarchyHeadline}: der Satz dort beantwortet "worauf sehe ich" und
 * ist an seinem Ende zitierfaehig (der Beweislauf aus W4e liest ihn genau so).
 * Dieser hier beantwortet "woraus besteht das Bild", und die Antwort hat zwei
 * Zahlen, weil das Bild aus zwei Quellen kommt: die Aufrufe des Walks sind die
 * Struktur, alles andere liegt daneben. Ohne die Trennung waere an keiner
 * Stelle zu sehen, was die Spalten macht und was nur mitgezeichnet ist.
 *
 * Kurz gehalten, und das ist gemessen und nicht Geschmack: der Kopf ist rund
 * 412 Pixel breit, die Schrift 11 Pixel monospace, und zusammen mit der Zahl
 * der Kantenarten dahinter passt dieser Satz in EINE Zeile. Die laengere
 * Fassung ("3 more links from the index") brauchte zwei, und die zweite Zeile
 * ging dem Bild darunter ab: der Anteil der Szene am Panel fiel von 0.68 auf
 * 0.60, also auf die Grenze, die W5c dafuer gezogen hat.
 */
export function hierarchyEdgeNote(walkEdges: number, indexEdges: number): string {
    return `${walkEdges} ${walkEdges === 1 ? 'call' : 'calls'} from the walk, `
        + `${indexEdges} ${indexEdges === 1 ? 'link' : 'links'} from the index`;
}
