/**
 * Das Sequenzdiagramm als Geometrie: wo jede Lebenslinie steht, wo jeder Pfeil
 * ansetzt, und wo die Selbstschleife eines Fehlerpfades sitzt.
 *
 * Kein React, kein DOM, keine Diagramm-Bibliothek. Diese Datei rechnet aus der
 * {@link FlowView} ein Koordinatenmodell, das ein Renderer eins zu eins in SVG
 * ausgibt. Vier Entscheidungen, und keine davon ist Geschmack:
 *
 * **Die Rechnung steht hier, nicht im Renderer.** Ein Renderer, der selbst
 * ausrechnet, wo ein Pfeil hingehoert, ist nicht pruefbar, ohne einen Browser zu
 * starten. So ist jede Zahl dieses Bildes ein Unit-Test entfernt, und der
 * Beweislauf im Browser prueft danach das, was ein Test nicht kann: dass man es
 * sieht.
 *
 * **Ein Aufruf innerhalb einer Datei ist eine Selbstschleife.** Die
 * Lebenslinien sind Dateien (so baut `flowSequence` sie), und `createUser` ruft
 * `listUsers` in derselben Datei. Einen solchen Pfeil quer ueber das Bild zu
 * zeichnen waere eine Linie von einer Spalte zu sich selbst, also gar keine
 * Linie. Er wird als Schleife an seiner Spalte gezeichnet und traegt denselben
 * Namen wie jeder andere Pfeil.
 *
 * **Ein erhobener Fehlertyp bekommt eine eigene Schleife, kein Pfeil.** Ein
 * `may raise` ist keine Aufrufkante: der Index kennt die Stelle, an der der Typ
 * herkommt, und nicht einen Aufruf, der ihn erzeugt. Die Schleife sitzt an der
 * Datei, in der der Fehlertyp deklariert ist, weil das die einzige Datei ist,
 * die der Fakt nennt. Kommt sie im Bild nicht vor, sitzt die Schleife an der
 * Wurzel und die Beschriftung nennt trotzdem den Typ.
 *
 * **Zwei gleiche Schleifen sind eine.** Zwei Symbole des Walks koennen denselben
 * Fehlertyp erheben. Zwei identisch beschriftete Schleifen uebereinander waeren
 * dieselbe Aussage zweimal; die Liste rechts sagt weiterhin, welcher Schritt
 * welcher war, und beide Schritte leuchten dieselbe Schleife an.
 */

import { baseNameOf, sanitizeLabel } from './flow-model';
import type { FlowView } from './flow-view';

// ---------------------------------------------------------------------------
// Das Raster
// ---------------------------------------------------------------------------

/** Rand links und rechts, in Bildeinheiten. */
export const DIAGRAM_PAD_X = 14;

/** Rand oben und unten. */
export const DIAGRAM_PAD_TOP = 10;
export const DIAGRAM_PAD_BOTTOM = 14;

/** Breite und Hoehe einer Kopf- und Fussbox. */
export const DIAGRAM_BOX_WIDTH = 128;
export const DIAGRAM_BOX_HEIGHT = 24;

/** Abstand zweier Boxen. Der Spaltenabstand ist Breite plus Abstand. */
export const DIAGRAM_COLUMN_GAP = 22;

/** Wie weit unter der Kopfbox die erste Zeile liegt. */
export const DIAGRAM_FIRST_ROW = 34;

/** Der Abstand zweier gerader Pfeile. */
export const DIAGRAM_ROW_PITCH = 40;

/** Der Abstand, den eine Schleife braucht: sie ist zwei Linien hoch. */
export const DIAGRAM_LOOP_PITCH = 54;

/** Wie weit die Schleife von ihrer Lebenslinie nach rechts ausholt. */
export const DIAGRAM_LOOP_WIDTH = 34;

/** Wie weit die Lebenslinie unter der letzten Zeile weiterlaeuft. */
export const DIAGRAM_TAIL = 24;

/** Die Halbbreite der Pfeilspitze. */
export const DIAGRAM_HEAD_SIZE = 5;

/**
 * Wie breit ein Zeichen einer Beschriftung veranschlagt wird.
 *
 * Eine Reservierung und ausdruecklich keine Messung: was ein Zeichen wirklich
 * einnimmt, weiss nur der Browser, der es setzt, und diese Datei rechnet ohne
 * einen. Die Zahl passt zu der Schrift, die das Bild traegt (monospace, 11
 * Pixel), und sie ist eher zu gross als zu klein. Der Grund, dass es sie
 * ueberhaupt gibt: eine Selbstschleife an der letzten Spalte schreibt ihren
 * Satz nach rechts, und ohne diesen Zuschlag endet er ausserhalb des Bildes.
 */
export const DIAGRAM_LABEL_CHAR_WIDTH = 6.8;

// ---------------------------------------------------------------------------
// Das Modell
// ---------------------------------------------------------------------------

/** Eine Lebenslinie: die Datei als Kopf- und Fussbox mit der Linie dazwischen. */
export interface DiagramLifeline {
    index: number;
    label: string;
    /** Mitte der Spalte. */
    x: number;
    /** Linke Kante der beiden Boxen. */
    boxX: number;
    boxWidth: number;
    boxHeight: number;
    /** Oberkante der Kopfbox. */
    headY: number;
    /** Oberkante der Fussbox. */
    footY: number;
    /** Anfang und Ende der senkrechten Linie. */
    lineTop: number;
    lineBottom: number;
}

/** Ein Pfeil: ein Aufruf, den der Walk aufgezeichnet hat. */
export interface DiagramArrow {
    /** Index in {@link FlowSequence.interactions}. Der Abgleich der Schritte laeuft darueber. */
    index: number;
    from: number;
    to: number;
    fromX: number;
    toX: number;
    y: number;
    label: string;
    /** Wahr, wenn Anfang und Ziel dieselbe Datei sind: gezeichnet als Schleife. */
    self: boolean;
    cycle: boolean;
    /** Wo die Beschriftung sitzt, und wie sie ausgerichtet wird. */
    labelX: number;
    labelY: number;
    labelAnchor: 'start' | 'middle' | 'end';
}

/** Eine Selbstschleife fuer einen erhobenen Fehlertyp. */
export interface DiagramLoop {
    index: number;
    /** Die Lebenslinie, an der sie haengt. */
    lifeline: number;
    x: number;
    y: number;
    width: number;
    height: number;
    /** Der ganze Satz, so wie die Zeile des Blocks ihn traegt. */
    label: string;
    /** Der Fehlertyp allein, fuer den Abgleich. */
    type: string;
    /** Die Schritte des Blocks, die auf diese Schleife zeigen. */
    steps: number[];
    labelX: number;
    labelY: number;
    labelAnchor: 'start' | 'end';
}

/** Ein Walk, fertig gezeichnet. */
export interface FlowDiagram {
    width: number;
    height: number;
    lifelines: DiagramLifeline[];
    arrows: DiagramArrow[];
    loops: DiagramLoop[];
    /** Wie viele Pfeile eine Kette schliessen. Null, wenn keiner. */
    cycles: number;
    /** Was die Deckel des Kastens weggelassen haben. */
    omitted: number;
}

/** Eine Zeile des Bildes, bevor sie eine y-Koordinate hat. */
interface Row {
    kind: 'arrow' | 'loop';
    /** Index in `arrows` beziehungsweise `loops`. */
    at: number;
    height: number;
}

/**
 * Den Walk als Bild rechnen.
 *
 * Total: eine Sequenz ohne Pfeile ergibt ein Bild mit den Lebenslinien, die es
 * gibt (mindestens der Wurzel), und ohne Zeilen. Eine leere Flaeche waere die
 * Behauptung, es sei nichts geladen.
 */
export function buildFlowDiagram(view: FlowView): FlowDiagram {
    const sequence = view.sequence;
    const labels = sequence.participants;
    const indexOf = new Map<string, number>();
    labels.forEach((label, index) => indexOf.set(label, index));

    const lifelines: DiagramLifeline[] = labels.map((label, index) => {
        const boxX = DIAGRAM_PAD_X + index * (DIAGRAM_BOX_WIDTH + DIAGRAM_COLUMN_GAP);
        return {
            index,
            label,
            x: boxX + DIAGRAM_BOX_WIDTH / 2,
            boxX,
            boxWidth: DIAGRAM_BOX_WIDTH,
            boxHeight: DIAGRAM_BOX_HEIGHT,
            headY: DIAGRAM_PAD_TOP,
            // Wird gesetzt, sobald die Zeilen stehen: die Fussbox sitzt unter
            // der letzten Zeile und nicht an einer geratenen Stelle.
            footY: 0,
            lineTop: DIAGRAM_PAD_TOP + DIAGRAM_BOX_HEIGHT,
            lineBottom: 0,
        };
    });

    const arrows: DiagramArrow[] = sequence.interactions.map((interaction, index) => {
        const from = indexOf.get(interaction.from) ?? 0;
        const to = indexOf.get(interaction.to) ?? 0;
        return {
            index,
            from,
            to,
            fromX: lifelines[from]?.x ?? DIAGRAM_PAD_X,
            toX: lifelines[to]?.x ?? DIAGRAM_PAD_X,
            y: 0,
            label: interaction.message,
            self: from === to,
            cycle: interaction.cycle,
            labelX: 0,
            labelY: 0,
            labelAnchor: from === to ? 'start' : 'middle',
        };
    });

    const loops = loopsOf(view, indexOf, lifelines);

    // Die Zeilen in der Reihenfolge, in der der Kasten sie haelt: die Pfeile in
    // Indexreihenfolge, und eine Schleife direkt hinter dem letzten Pfeil, der
    // ihre Datei erreicht. Wo kein Pfeil dorthin geht, steht sie am Ende: eine
    // Schleife vor dem Aufruf, der sie ausloest, waere eine erfundene Ordnung.
    const rows: Row[] = [];
    const loopsAfterArrow = new Map<number, number[]>();
    const trailing: number[] = [];
    loops.forEach((loop, at) => {
        let last = -1;
        arrows.forEach((arrow) => {
            if (arrow.to === loop.lifeline) {
                last = arrow.index;
            }
        });
        if (last < 0) {
            trailing.push(at);
            return;
        }
        const list = loopsAfterArrow.get(last) ?? [];
        list.push(at);
        loopsAfterArrow.set(last, list);
    });
    for (const arrow of arrows) {
        rows.push({ kind: 'arrow', at: arrow.index, height: arrow.self ? DIAGRAM_LOOP_PITCH : DIAGRAM_ROW_PITCH });
        for (const at of loopsAfterArrow.get(arrow.index) ?? []) {
            rows.push({ kind: 'loop', at, height: DIAGRAM_LOOP_PITCH });
        }
    }
    for (const at of trailing) {
        rows.push({ kind: 'loop', at, height: DIAGRAM_LOOP_PITCH });
    }

    const firstRowY = DIAGRAM_PAD_TOP + DIAGRAM_BOX_HEIGHT + DIAGRAM_FIRST_ROW;
    let y = firstRowY;
    let lastRowBottom = firstRowY;
    for (const row of rows) {
        if (row.kind === 'arrow') {
            const arrow = arrows[row.at];
            arrow.y = y;
            if (arrow.self) {
                const side = sideFor(arrow.fromX, arrow.label, lifelines.length);
                arrow.labelAnchor = side;
                arrow.labelX = side === 'start'
                    ? arrow.fromX + DIAGRAM_LOOP_WIDTH + 8
                    : arrow.fromX - 8;
                arrow.labelY = y + 2;
            } else {
                arrow.labelX = (arrow.fromX + arrow.toX) / 2;
                arrow.labelY = y - 7;
            }
        } else {
            const loop = loops[row.at];
            loop.y = y;
            const side = sideFor(loop.x, loop.label, lifelines.length);
            loop.labelAnchor = side;
            loop.labelX = side === 'start' ? loop.x + loop.width + 8 : loop.x - 8;
            loop.labelY = y + 2;
        }
        // Eine Schleife und ein Selbstaufruf reichen unter ihre Zeile hinunter;
        // die Linie muss darunter noch weiterlaufen, sonst endet sie mitten im
        // Bogen.
        lastRowBottom = y + (row.height === DIAGRAM_LOOP_PITCH ? 22 : 0);
        y += row.height;
    }

    const lineBottom = lastRowBottom + DIAGRAM_TAIL;
    for (const lifeline of lifelines) {
        lifeline.lineBottom = lineBottom;
        lifeline.footY = lineBottom;
    }

    const columns = lifelines.length === 0
        ? DIAGRAM_PAD_X * 2 + DIAGRAM_BOX_WIDTH
        : DIAGRAM_PAD_X * 2
            + lifelines.length * DIAGRAM_BOX_WIDTH
            + (lifelines.length - 1) * DIAGRAM_COLUMN_GAP;

    // Das Bild ist so breit wie seine Spalten ODER wie die Beschriftung, die am
    // weitesten nach rechts reicht. Ohne das zweite endet der Satz einer
    // Selbstschleife an der letzten Spalte ausserhalb des Bildes.
    let rightMost = columns - DIAGRAM_PAD_X;
    const rightEdge = (x: number, text: string, anchor: 'start' | 'middle' | 'end'): number => {
        const width = text.length * DIAGRAM_LABEL_CHAR_WIDTH;
        if (anchor === 'start') {
            return x + width;
        }
        return anchor === 'end' ? x : x + width / 2;
    };
    for (const arrow of arrows) {
        rightMost = Math.max(rightMost, rightEdge(arrow.labelX, arrow.label, arrow.labelAnchor));
        if (arrow.self) {
            rightMost = Math.max(rightMost, arrow.fromX + DIAGRAM_LOOP_WIDTH);
        }
    }
    for (const loop of loops) {
        rightMost = Math.max(rightMost, rightEdge(loop.labelX, loop.label, loop.labelAnchor));
        rightMost = Math.max(rightMost, loop.x + loop.width);
    }

    return {
        width: Math.max(columns, rightMost + DIAGRAM_PAD_X),
        height: lineBottom + DIAGRAM_BOX_HEIGHT + DIAGRAM_PAD_BOTTOM,
        lifelines,
        arrows,
        loops,
        cycles: arrows.filter((arrow) => arrow.cycle).length,
        omitted: sequence.omitted,
    };
}

/**
 * Auf welcher Seite ihrer Spalte eine Schleife ihren Satz schreibt.
 *
 * Rechts, wo Platz ist; links, wo keiner mehr ist. Der Befund am ersten Bild
 * war genau dieser: die Schleife der letzten Spalte schrieb ihren Satz nach
 * rechts aus dem Bild heraus, und die Alternative, das Bild dafuer um die
 * Satzbreite zu verbreitern, haette das ganze Diagramm auf die halbe
 * Schriftgroesse gestaucht, weil es in die Spalte passen muss. Ein Satz links
 * von seiner Linie steht ueber dem Zwischenraum der Spalten davor, und dort
 * steht in derselben Zeile nichts anderes.
 */
function sideFor(x: number, label: string, columns: number): 'start' | 'end' {
    if (columns <= 1) {
        return 'start';
    }
    const middle = DIAGRAM_PAD_X
        + (columns * DIAGRAM_BOX_WIDTH + (columns - 1) * DIAGRAM_COLUMN_GAP) / 2;
    if (x <= middle) {
        return 'start';
    }
    // Nach links nur, wenn der Satz dort auch hinpasst. Sonst waere die
    // Verbesserung dieselbe Kappung an der anderen Kante.
    const width = label.length * DIAGRAM_LABEL_CHAR_WIDTH;
    return x - 8 - width >= DIAGRAM_PAD_X ? 'end' : 'start';
}

/**
 * Die Selbstschleifen: eine je Fehlertyp und Datei, in der Reihenfolge, in der
 * der Block sie erhebt.
 */
function loopsOf(
    view: FlowView,
    indexOf: ReadonlyMap<string, number>,
    lifelines: readonly DiagramLifeline[],
): DiagramLoop[] {
    const out: DiagramLoop[] = [];
    const seen = new Map<string, DiagramLoop>();
    view.steps.forEach((step, stepIndex) => {
        if (step.line.kind !== 'raise') {
            return;
        }
        const type = sanitizeLabel(step.line.targetName ?? '');
        const file = step.line.targetFile === undefined
            ? ''
            : sanitizeLabel(baseNameOf(step.line.targetFile));
        // Ohne Lebenslinie fuer die Datei haengt die Schleife an der Wurzel:
        // der Fakt nennt sie, das Bild zeigt sie nicht, und die Beschriftung
        // sagt trotzdem, welcher Typ es ist.
        const lifeline = indexOf.get(file) ?? 0;
        const key = `${lifeline} ${type}`;
        const known = seen.get(key);
        if (known !== undefined) {
            known.steps.push(stepIndex);
            return;
        }
        const loop: DiagramLoop = {
            index: out.length,
            lifeline,
            x: lifelines[lifeline]?.x ?? DIAGRAM_PAD_X,
            y: 0,
            width: DIAGRAM_LOOP_WIDTH,
            height: 18,
            label: step.line.text.replace(/^\d+\.\s*/, ''),
            type,
            steps: [stepIndex],
            labelX: 0,
            labelY: 0,
            labelAnchor: 'start',
        };
        out.push(loop);
        seen.set(key, loop);
    });
    return out;
}

/**
 * Welche Schleife ein Schritt anleuchtet, oder -1.
 *
 * Ueber den Schritt-Index und nicht ueber den Text: die Schleifen tragen die
 * Schritte, die sie erzeugt haben, also ist der Abgleich eine Zugehoerigkeit
 * und keine Suche, die bei zwei gleichen Saetzen die falsche treffen koennte.
 */
export function loopForStep(diagram: FlowDiagram, step: number): number {
    if (step < 0) {
        return -1;
    }
    return diagram.loops.findIndex((loop) => loop.steps.includes(step));
}
