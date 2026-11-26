/**
 * Der Flow-Erklaerer als ein Modell: der Kasten, die Schritte, und was beide
 * ueber ihre Grenzen sagen.
 *
 * Kein React, kein DOM, kein Provider. Diese Datei setzt zusammen, was
 * `flow-model.ts` und `pseudocode-builder.ts` (beide portiert) getrennt
 * liefern, und sie tut es an einer Stelle, weil das die Behauptung des
 * Erklaerers ist: das Bild links und die Liste rechts sind zwei Lesungen
 * derselben Antwort. Zwei Flaechen, die sich das je selbst zusammenrechnen,
 * waeren zwei Antworten, die irgendwann auseinanderlaufen.
 *
 * **Eine Grenze, und sie ist eine Entscheidung.** Der Walk geht einen Hop weit
 * ({@link FLOW_CLOSURE_DEPTH}). Der Kasten zeichnet, was dieses Symbol selbst
 * ruft, und ein Pfeil je Aufruf ist genau das, was der Stepper durchgeht. Was
 * diese Aufrufe ihrerseits erreichen, ist die Frage des Vorwaerts-Walks aus W4a
 * und wuerde hier Pfeile zeichnen, die kein Schritt anfassen kann. Der Kasten
 * sagt diese Grenze mit einem Satz, statt sie zu verschweigen.
 *
 * **Ein Schritt ohne Pfeil ist ein Schritt.** Ein erhobener Fehlertyp und eine
 * Umgebungslesung sind nummerierte Zeilen des Blocks und haben in einer Folge
 * von Aufrufen keine Lebenslinie. Sie werden trotzdem angelaufen: der Editor
 * folgt ihnen, und der Kasten sagt, warum er still bleibt. Irgendeinen Pfeil
 * zu faerben waere die eine Luege, die diese Flaeche billig haette.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { SemanticIR } from '../core/semantic-ir';
import { CLOSURE_DEFAULT_CAP } from '../provider/closure';

import type { FlowSequence, FlowStep } from './flow-model';
import { baseNameOf, flowArrowFor, flowNodes, flowSequence, flowSteps } from './flow-model';
import type { ClosureDocument, PseudocodeDocument } from './pseudocode-builder';
import { buildPseudocode, identityOf } from './pseudocode-builder';
import { EXPLAINER_EMPTY, explainerCappedNote } from './pseudocode-strings';

/**
 * Wie weit der Walk hinter dem Kasten geht.
 *
 * Ein Hop. Begruendung im Kopf dieser Datei.
 */
export const FLOW_CLOSURE_DEPTH = 1;

/** Wie viele Symbole der Walk zurueckgeben darf. Die Vorgabe des Walks selbst. */
export const FLOW_CLOSURE_CAP = CLOSURE_DEFAULT_CAP;

/** Der Erklaerer, fertig zum Zeichnen. */
export interface FlowView {
    /** Das Symbol, ueber das der Kasten spricht. */
    root: SymbolRef;
    /** Die Ueberschrift des Blocks, aus dem die Schritte kommen. */
    title: string;
    document: PseudocodeDocument;
    sequence: FlowSequence;
    /** Die Halte des Steppers: genau die nummerierten Zeilen des Blocks. */
    steps: FlowStep[];
    /**
     * Der Pfeil je Schritt, oder -1.
     *
     * Vorgerechnet statt bei jedem Bild neu, damit der Kasten und die Liste
     * denselben Abgleich benutzen und nicht zwei.
     */
    arrows: number[];
    /**
     * Die Zeile der STEPS-Sektion je Schritt, oder -1.
     *
     * Nur die Aufruf-Zeilen der Wurzel haben eine: die STEPS-Sektion des Twin
     * listet die Aufrufstellen des fokussierten Symbols, und ein Schritt, der
     * zu einem anderen Symbol des Walks gehoert, steht dort nicht. -1 heisst
     * "in dieser Liste gibt es dazu nichts", nie "Zeile 0".
     */
    stepRows: number[];
    /** Ehrliche Saetze unter dem Kasten. Leer, wenn es nichts einzuraeumen gibt. */
    notes: string[];
    /**
     * Die Grenzen, mit denen der Walk gelaufen ist, als Zahlen statt als Satz.
     *
     * Bis W8b standen sie in einem Absatz UNTER dem Bild, als vierter von vier,
     * und wurden nach drei Absaetzen Ehrlichkeit nicht mehr gelesen. Seit W8b
     * stehen sie als Beschriftung AM RAND des Bildes: eine Grenze gehoert
     * dorthin, wo sie gilt, und wird dort gezeichnet statt beschrieben.
     */
    bound: { depth: number; cap: number };
    /**
     * Wie viele Symbole dieses Walks der Index zwar genannt, aber nicht
     * aufgeloest hat.
     *
     * Sie kommen als `kind: 'unknown'` aus dem Walk (siehe `unresolvedCallee`
     * in src/provider/closure.ts). Was hier NICHT drinsteht, gehoert zur
     * Auskunft und ist im Kopf von FlowOverlay beschrieben: ein Aufruf, den der
     * Index ganz ohne qualifizierten Namen meldet, faellt schon im Walk heraus
     * und ist an dieser Stelle nicht mehr zaehlbar. Die Zahl ist darum eine
     * UNTERGRENZE und wird als solche formuliert.
     */
    unresolved: number;
}

/** Was der Erklaerer braucht. Alles davon liegt schon vor, nichts wird hier geholt. */
export interface FlowViewInput {
    closure: ClosureDocument;
    /** Die IRs der Symbole des Walks, so viele wie angekommen sind. */
    irs: SemanticIR[];
    /** Die Grenzen, mit denen der Walk gelaufen ist, fuer den Satz darunter. */
    depth?: number;
    cap?: number;
}

/**
 * Den Erklaerer bauen.
 *
 * Total: ein Walk ohne Kanten, ein Walk ohne IRs und ein Symbol, das nichts
 * ruft, ergeben je einen Erklaerer mit einem Satz statt einer leeren Flaeche.
 */
export function buildFlowView(input: FlowViewInput): FlowView {
    const closure = input.closure;
    const root = closure.root;
    const document = buildPseudocode(
        { kind: 'closure', label: root.name },
        { irs: input.irs, closure },
    );
    const nodes = flowNodes(closure);
    const sequence = flowSequence(baseNameOf(root.uri), nodes);
    const steps = flowSteps(document);
    const arrows = steps.map((step) => flowArrowFor(sequence, step.line));

    const notes: string[] = [];
    if (sequence.interactions.length === 0) {
        notes.push(EXPLAINER_EMPTY);
    } else {
        notes.push(explainerCappedNote(input.depth ?? FLOW_CLOSURE_DEPTH, input.cap ?? FLOW_CLOSURE_CAP));
    }

    return {
        root,
        title: document.title,
        document,
        sequence,
        steps,
        arrows,
        stepRows: stepRowsOf(document, root, steps),
        notes,
        bound: {
            depth: input.depth ?? FLOW_CLOSURE_DEPTH,
            cap: input.cap ?? FLOW_CLOSURE_CAP,
        },
        unresolved: closure.symbols.filter((symbol) => symbol.kind === 'unknown').length,
    };
}

/**
 * Welche Zeile der STEPS-Sektion zu welchem Schritt gehoert.
 *
 * Gezaehlt wird ueber die Aufruf-Zeilen der Wurzelgruppe, in ihrer Reihenfolge,
 * weil die STEPS-Sektion des Twin genau daraus gebaut ist: eine Zeile je
 * aufgezeichneter Aufrufstelle, in der Reihenfolge der IR. Beide lesen also
 * dieselbe Liste, und der Abgleich ist eine Position und keine Namenssuche, die
 * bei zwei Aufrufen desselben Ziels die falsche Zeile treffen koennte.
 */
function stepRowsOf(
    document: PseudocodeDocument,
    root: SymbolRef,
    steps: readonly FlowStep[],
): number[] {
    const rootKey = identityOf(root);
    let seen = 0;
    const byLineIndex = new Map<number, number>();
    document.lines.forEach((line, index) => {
        if (line.kind === 'step' && line.group === rootKey) {
            byLineIndex.set(index, seen);
            seen += 1;
        }
    });
    return steps.map((step) => byLineIndex.get(step.lineIndex) ?? -1);
}
