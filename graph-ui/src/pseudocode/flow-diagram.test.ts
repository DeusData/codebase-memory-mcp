/*
 * Die Geometrie des Sequenzdiagramms, ohne einen einzigen gerenderten Pixel.
 *
 * Genau darum ist die Rechnung eine eigene Datei: was ein Renderer selbst
 * ausrechnet, kann man nur mit einem Browser pruefen, und dann prueft man das
 * Bild und nicht die Rechnung. Hier wird gefragt, was der Beweislauf im Browser
 * nicht fragen kann: dass eine Spalte je Datei entsteht, dass ein Aufruf
 * innerhalb einer Datei als Schleife und nicht als Nullinie gezeichnet wird,
 * dass zwei gleiche Fehlerpfade eine Schleife ergeben und beide Schritte sie
 * anleuchten, und dass keine Zeile die andere ueberdeckt.
 */

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

import type { SemanticIR } from '../core/semantic-ir';
import {
    DIAGRAM_BOX_HEIGHT,
    DIAGRAM_BOX_WIDTH,
    DIAGRAM_COLUMN_GAP,
    DIAGRAM_LABEL_CHAR_WIDTH,
    DIAGRAM_PAD_X,
    buildFlowDiagram,
    loopForStep,
} from './flow-diagram';
import { buildFlowView } from './flow-view';
import type { FlowView } from './flow-view';
import type { ClosureDocument } from './pseudocode-builder';

const HERE = dirname(fileURLToPath(import.meta.url));

function view(): FlowView {
    const closure = JSON.parse(readFileSync(
        join(HERE, '__fixtures__', 'closure-userService-create.json'),
        'utf8',
    )) as ClosureDocument;
    const ir = (name: string): SemanticIR => JSON.parse(readFileSync(
        join(HERE, '..', 'twin', '__fixtures__', `ir-${name}.json`),
        'utf8',
    )) as SemanticIR;
    return buildFlowView({
        closure,
        irs: [ir('userService-create'), ir('createUser'), ir('listUsers'), ir('validateUser'), ir('insert')],
    });
}

describe('das Sequenzdiagramm als Geometrie', () => {
    const flow = view();
    const diagram = buildFlowDiagram(flow);

    it('zeichnet eine Lebenslinie je Datei, in der Reihenfolge des Kastens', () => {
        expect(diagram.lifelines.map((lifeline) => lifeline.label))
            .toEqual(flow.sequence.participants);
        expect(diagram.lifelines.length).toBeGreaterThanOrEqual(3);
    });

    it('setzt die Spalten in gleichem Abstand, die Wurzel links', () => {
        diagram.lifelines.forEach((lifeline, index) => {
            expect(lifeline.boxX).toBe(DIAGRAM_PAD_X + index * (DIAGRAM_BOX_WIDTH + DIAGRAM_COLUMN_GAP));
            expect(lifeline.x).toBe(lifeline.boxX + DIAGRAM_BOX_WIDTH / 2);
        });
    });

    it('gibt jeder Lebenslinie Kopf- und Fussbox mit der Linie dazwischen', () => {
        for (const lifeline of diagram.lifelines) {
            expect(lifeline.lineTop).toBe(lifeline.headY + DIAGRAM_BOX_HEIGHT);
            expect(lifeline.lineBottom).toBeGreaterThan(lifeline.lineTop);
            expect(lifeline.footY).toBe(lifeline.lineBottom);
            expect(diagram.height).toBeGreaterThan(lifeline.footY + DIAGRAM_BOX_HEIGHT);
        }
    });

    it('zeichnet einen Pfeil je Aufruf, in Indexreihenfolge', () => {
        expect(diagram.arrows).toHaveLength(flow.sequence.interactions.length);
        expect(diagram.arrows.map((arrow) => arrow.index))
            .toEqual(flow.sequence.interactions.map((_, index) => index));
        expect(diagram.arrows.map((arrow) => arrow.label))
            .toEqual(flow.sequence.interactions.map((interaction) => interaction.message));
    });

    it('macht aus einem Aufruf innerhalb derselben Datei eine Schleife', () => {
        const self = diagram.arrows.filter((arrow) => arrow.self);
        expect(self.length).toBeGreaterThan(0);
        for (const arrow of self) {
            expect(arrow.from).toBe(arrow.to);
            expect(arrow.fromX).toBe(arrow.toX);
            // Die Beschriftung steht NEBEN der Linie, auf der Seite, auf der
            // sie hinpasst: sonst laege sie ueber dem Bogen, an dem sie
            // ansetzt, und waere unlesbar.
            expect(arrow.labelAnchor === 'start' || arrow.labelAnchor === 'end').toBe(true);
            expect(arrow.labelX).not.toBe(arrow.fromX);
            if (arrow.labelAnchor === 'start') {
                expect(arrow.labelX).toBeGreaterThan(arrow.fromX);
            } else {
                expect(arrow.labelX).toBeLessThan(arrow.fromX);
            }
        }
    });

    it('haelt jeden Satz einer Schleife im Bild', () => {
        // Der Befund am ersten Bild: die Schleife der letzten Spalte schrieb
        // ihren Satz nach rechts aus dem Bild heraus. Entweder passt er dorthin,
        // oder er steht links von seiner Linie, und in beiden Faellen ganz
        // innerhalb des Bildes.
        const span = (x: number, text: string, anchor: 'start' | 'middle' | 'end'): number[] => {
            const width = text.length * DIAGRAM_LABEL_CHAR_WIDTH;
            return anchor === 'end' ? [x - width, x] : [x, x + width];
        };
        const boxed = [
            ...diagram.loops.map((loop) => span(loop.labelX, loop.label, loop.labelAnchor)),
            ...diagram.arrows
                .filter((arrow) => arrow.self)
                .map((arrow) => span(arrow.labelX, arrow.label, arrow.labelAnchor)),
        ];
        expect(boxed.length).toBeGreaterThan(0);
        for (const [left, right] of boxed) {
            expect(left).toBeGreaterThanOrEqual(0);
            expect(right).toBeLessThanOrEqual(diagram.width);
        }
    });

    it('legt keine zwei Zeilen auf dieselbe Hoehe', () => {
        const ys = [...diagram.arrows.map((arrow) => arrow.y), ...diagram.loops.map((loop) => loop.y)];
        expect(new Set(ys).size).toBe(ys.length);
    });

    it('haengt die may-raise-Schleife an die Datei des Fehlertyps', () => {
        expect(diagram.loops.length).toBeGreaterThan(0);
        const loop = diagram.loops[0];
        expect(loop.label).toContain('may raise');
        // Kein "7." davor: die Nummer gehoert der Liste rechts, das Bild
        // beschriftet den Fakt.
        expect(loop.label.startsWith('may raise')).toBe(true);
        expect(diagram.lifelines[loop.lifeline]?.label).toBe('validate.ts');
    });

    it('fasst zwei gleiche Fehlerpfade zu einer Schleife zusammen, ohne einen Schritt zu verlieren', () => {
        const raises = flow.steps
            .map((step, index) => ({ step, index }))
            .filter((entry) => entry.step.line.kind === 'raise');
        expect(raises.length).toBeGreaterThan(1);
        const hit = raises.map((entry) => loopForStep(diagram, entry.index));
        // Jeder Schritt findet seine Schleife, und die Schleifen sind weniger
        // als die Schritte: derselbe Fehlertyp an derselben Datei wird einmal
        // gezeichnet.
        expect(hit.every((index) => index >= 0)).toBe(true);
        expect(new Set(hit).size).toBeLessThan(raises.length);
    });

    it('antwortet fuer einen Schritt ohne Schleife mit -1 statt mit der naechstbesten', () => {
        expect(loopForStep(diagram, -1)).toBe(-1);
        const call = flow.steps.findIndex((step) => step.line.kind === 'step');
        expect(call).toBeGreaterThanOrEqual(0);
        expect(loopForStep(diagram, call)).toBe(-1);
    });

    it('bleibt bei einem Walk ohne Aufrufe ein Bild und keine leere Flaeche', () => {
        const empty = buildFlowDiagram({
            ...flow,
            sequence: { participants: ['userService.ts'], interactions: [], omitted: 0 },
            steps: [],
        });
        expect(empty.lifelines).toHaveLength(1);
        expect(empty.arrows).toHaveLength(0);
        expect(empty.loops).toHaveLength(0);
        expect(empty.width).toBeGreaterThan(0);
        expect(empty.height).toBeGreaterThan(0);
    });
});
