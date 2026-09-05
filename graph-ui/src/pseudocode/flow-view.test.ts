/**
 * Was der Erklaerer aus einem Walk macht, und was er dabei einraeumt.
 *
 * Drei Aussagen, und jede ist eine, die auf dem Bildschirm falsch aussehen
 * wuerde, ohne falsch zu wirken:
 *
 *  1. Der Kasten und die Schritte kommen aus derselben Antwort: jeder Pfeil ist
 *     eine Kante des Walks, jeder Schritt eine nummerierte Zeile des Blocks.
 *  2. Ein Schritt ohne Pfeil bekommt keinen. -1 statt 0 ist der Unterschied
 *     zwischen "dazu gibt es nichts" und "der erste Pfeil".
 *  3. Die Zuordnung zur STEPS-Liste des Twin ist eine Position in der Liste der
 *     Aufrufstellen der Wurzel, nicht eine Namenssuche.
 */

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import type { SemanticIR } from '../core/semantic-ir';
import { buildFlowView } from './flow-view';
import type { ClosureDocument } from './pseudocode-builder';

const HERE = dirname(fileURLToPath(import.meta.url));
const IR_FIXTURES = join(HERE, '..', 'twin', '__fixtures__');
const FIXTURES = join(HERE, '__fixtures__');

const ir = (name: string): SemanticIR =>
    JSON.parse(readFileSync(join(IR_FIXTURES, `ir-${name}.json`), 'utf8')) as SemanticIR;

const CLOSURE: ClosureDocument = JSON.parse(
    readFileSync(join(FIXTURES, 'closure-userService-create.json'), 'utf8'),
) as ClosureDocument;

const IRS = [ir('createUser'), ir('listUsers'), ir('validateUser'), ir('insert'), ir('userService-create')];

describe('der Erklaerer aus einem Walk', () => {
    const view = buildFlowView({ closure: CLOSURE, irs: IRS });

    it('spricht ueber die Wurzel des Walks und sagt es in der Ueberschrift', () => {
        expect(view.root).toBe(CLOSURE.root);
        expect(view.title).toBe('Steps in create and the code it reaches');
    });

    it('zeichnet eine Lebenslinie je Datei, die Wurzel zuerst', () => {
        expect(view.sequence.participants[0]).toBe('userService.ts');
        expect(view.sequence.participants.length).toBeGreaterThanOrEqual(3);
    });

    it('haelt genau die nummerierten Zeilen als Halte', () => {
        expect(view.steps).toHaveLength(view.document.lines.filter((line) => line.order !== undefined).length);
        expect(view.arrows).toHaveLength(view.steps.length);
        expect(view.stepRows).toHaveLength(view.steps.length);
    });

    it('gibt jedem Aufruf-Schritt einen Pfeil, den es wirklich gibt', () => {
        view.steps.forEach((step, index) => {
            if (step.line.kind !== 'step') {
                return;
            }
            const at = view.arrows[index];
            if (at < 0) {
                return;
            }
            expect(view.sequence.interactions[at]).toBeDefined();
            expect(view.sequence.interactions[at].message).toBe(step.line.targetName);
        });
    });

    it('faerbt fuer einen Fehlerpfad und eine Umgebungslesung keinen Pfeil', () => {
        view.steps.forEach((step, index) => {
            if (step.line.kind === 'raise' || step.line.kind === 'env') {
                expect(view.arrows[index]).toBe(-1);
            }
        });
    });

    it('ordnet nur die Aufrufstellen der Wurzel einer Zeile der STEPS-Liste zu', () => {
        const rootSteps = view.stepRows.filter((row) => row >= 0);
        expect(rootSteps).toEqual([0]);
        // `create` ruft genau einmal, also gibt es genau eine STEPS-Zeile.
        expect(ir('userService-create').steps.value).toHaveLength(1);
    });

    it('nennt die Grenze des Walks, statt das Bild fuer vollstaendig auszugeben', () => {
        expect(view.notes.join(' ')).toContain('one hop');
    });
});

describe('ein Symbol, das nichts ruft', () => {
    const alone: ClosureDocument = {
        root: CLOSURE.root,
        symbols: [CLOSURE.root],
        edges: [],
        truncated: false,
        visited: 1,
    };

    it('sagt, dass der Index keinen Aufruf fuehrt, statt einen leeren Kasten zu zeichnen', () => {
        const view = buildFlowView({ closure: alone, irs: [] });
        expect(view.sequence.interactions).toEqual([]);
        expect(view.notes.join(' ')).toContain('no call out of this symbol');
        expect(view.steps).toEqual([]);
    });
});
