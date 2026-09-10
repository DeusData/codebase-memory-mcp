/*
 * Portiert am 2026-08-29 aus CodeAtlasIDE,
 * theia-extensions/codeatlas-views/test/flow-model.test.ts (183 Zeilen).
 *
 * Ein Unterschied: dort wird der Kasten als Mermaid-Text gebaut und die letzten
 * beiden Zusicherungen pruefen, dass die Bibliothek ihn annimmt und welche
 * Zeilen darin stehen. Dieses Projekt liefert keine Diagramm-Bibliothek aus, und
 * `flowSequence` gibt dasselbe Ergebnis als Modell zurueck (siehe Kopf von
 * flow-model.ts). Die zwei Zusicherungen pruefen deshalb dieselben Aussagen am
 * Modell: eine Lebenslinie je Datei, ein Pfeil je behaltenem Aufruf.
 */

/**
 * What the flow explainer is allowed to draw and to walk.
 *
 * Three properties, and the first is the one the whole surface rests on.
 *
 * **The picture and the list come from one answer.** Both are derived from the
 * closure, so every arrow in the drawing is an edge the walk recorded and every
 * step in the list is a line the block holds. The assertions below check the
 * unfolding rather than the drawing: whether a cycle terminates, whether the
 * depths are the ones the sequence builder needs to recover callers from, and
 * whether the result is a connected set of arrows.
 *
 * **A walk stops on a step, never on a heading.** Prev and Next exist to move
 * through what the code does; stopping on "createUser:" or on a note about a
 * bound would be the buttons walking through the furniture.
 *
 * **A highlight names labels the drawing actually holds.** The sequence builder
 * sanitises what it draws, so the labels a step is matched against have to be
 * sanitised the same way or the highlight silently matches nothing.
 */

import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

import type { SemanticIR } from '../core/semantic-ir';
import {
    FLOW_NODE_CAP,
    SEQUENCE_INTERACTION_CAP,
    flowArrowFor,
    flowHighlightOf,
    flowNodes,
    flowSequence,
    flowSteps,
    sanitizeLabel,
} from './flow-model';
import type { ClosureDocument } from './pseudocode-builder';
import { buildPseudocode } from './pseudocode-builder';

const HERE = dirname(fileURLToPath(import.meta.url));
const IR_FIXTURES = join(HERE, '..', 'twin', '__fixtures__');
const FIXTURES = join(HERE, '__fixtures__');

const CLOSURE: ClosureDocument = JSON.parse(
    readFileSync(join(FIXTURES, 'closure-userService-create.json'), 'utf8'),
) as ClosureDocument;

const ir = (name: string): SemanticIR =>
    JSON.parse(readFileSync(join(IR_FIXTURES, `ir-${name}.json`), 'utf8')) as SemanticIR;

const DOCUMENT = buildPseudocode(
    { kind: 'closure', label: 'create' },
    {
        irs: [ir('createUser'), ir('listUsers'), ir('validateUser'), ir('insert'), ir('userService-create')],
        closure: CLOSURE,
    },
);

/** A closure of two symbols that call each other, which is where a naive walk never returns. */
function cyclicClosure(): ClosureDocument {
    const symbol = (name: string) => ({
        nodeId: `p.${name}`,
        name,
        qualifiedName: `p.${name}`,
        kind: 'function' as const,
        uri: `file:///workspace/src/${name}.ts`,
        range: { start: { line: 0, character: 0 }, end: { line: 4, character: 0 } },
        selectionRange: { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } },
    });
    const ping = symbol('ping');
    const pong = symbol('pong');
    return {
        root: ping,
        symbols: [ping, pong],
        edges: [
            { from: 'p.ping', to: 'p.pong', line: 2 },
            { from: 'p.pong', to: 'p.ping', line: 6 },
        ],
        truncated: false,
        visited: 2,
    };
}

describe('unfolding a closure into a walk', () => {
    const nodes = flowNodes(CLOSURE);

    it('leaves the root out, because the drawing takes it as the first lifeline', () => {
        expect(nodes.every((node) => node.ref.qualifiedName !== CLOSURE.root.qualifiedName)).toBe(true);
    });

    it('emits parents before children, which is how the drawing recovers each caller', () => {
        expect(nodes.map((node) => `${node.depth} ${node.ref.name}`)).toEqual([
            '1 createUser',
            '2 insert',
            '2 listUsers',
            '3 toUser',
            '2 toUser',
            '2 UserEntity',
            '2 ValidationError',
            '2 validateUser',
            '3 ValidationError',
        ]);
    });

    it('never invents an arrow the walk did not record', () => {
        const recorded = new Set(CLOSURE.edges.map((edge) => edge.to));
        for (const node of nodes) {
            expect(recorded.has(node.ref.qualifiedName!)).toBe(true);
        }
    });

    it('terminates on a cycle and marks where the chain closes', () => {
        const cyclic = flowNodes(cyclicClosure());
        expect(cyclic.map((node) => `${node.depth} ${node.ref.name}`)).toEqual(['1 pong', '2 ping']);
        expect(cyclic[1].badges.cycle).toBe(true);
    });

    it('claims no badge it did not look up', () => {
        for (const node of nodes) {
            expect(node.enriched).toBe(false);
            expect(node.badges.isTest).toBeUndefined();
            expect(node.badges.construction).toBeUndefined();
        }
    });

    it('stays under its own bound', () => {
        expect(nodes.length).toBeLessThanOrEqual(FLOW_NODE_CAP);
    });

    it('draws one lifeline per file and one arrow per call it kept', () => {
        const sequence = flowSequence('userService.ts', nodes);
        expect(sequence.participants[0]).toBe('userService.ts');
        expect(sequence.participants).toEqual(['userService.ts', 'db.ts', 'types.ts', 'validate.ts']);
        expect(sequence.interactions.map((arrow) => arrow.message)).toContain('createUser');
        expect(sequence.interactions.map((arrow) => arrow.message)).toContain('validateUser');
        expect(sequence.interactions).toHaveLength(nodes.length);
        expect(sequence.omitted).toBe(0);
    });

    it('marks the arrow that closes a chain rather than drawing it like any other', () => {
        const sequence = flowSequence('ping.ts', flowNodes(cyclicClosure()));
        expect(sequence.interactions.map((arrow) => arrow.cycle)).toEqual([false, true]);
    });

    it('counts what a bound left out instead of drawing a shorter picture in silence', () => {
        const many = flowNodes(CLOSURE);
        const stretched = Array.from(
            { length: SEQUENCE_INTERACTION_CAP + 4 },
            (_unused, index) => ({ ...many[0], id: `flow.${index}` }),
        );
        const sequence = flowSequence('userService.ts', stretched);
        expect(sequence.interactions).toHaveLength(SEQUENCE_INTERACTION_CAP);
        expect(sequence.omitted).toBe(4);
    });
});

describe('the steps a reader walks', () => {
    const steps = flowSteps(DOCUMENT);

    it('holds every numbered line and nothing else', () => {
        expect(steps).toHaveLength(DOCUMENT.lines.filter((line) => line.order !== undefined).length);
        for (const step of steps) {
            expect(step.line.order).toBeDefined();
            expect(step.line.kind).not.toBe('group');
            expect(step.line.kind).not.toBe('note');
        }
    });

    it('points back at the line it came from', () => {
        for (const step of steps) {
            expect(DOCUMENT.lines[step.lineIndex]).toBe(step.line);
        }
    });

    it('keeps the order the block holds them in', () => {
        expect(steps.map((step) => step.line.order))
            .toEqual([...steps.map((step) => step.line.order)].sort((a, b) => a! - b!));
    });
});

describe('what one step lights up', () => {
    it('names the arrow and the lifeline, sanitised as the drawing sanitises them', () => {
        const step = DOCUMENT.lines.find((line) => line.targetName === 'validateUser')!;
        expect(flowHighlightOf(step)).toEqual({ participant: 'validate.ts', message: 'validateUser' });
    });

    it('lights nothing up for a raised type, which has no lifeline of its own', () => {
        const raise = DOCUMENT.lines.find((line) => line.kind === 'raise')!;
        expect(flowHighlightOf(raise)).toEqual({});
    });

    it('lights nothing up for a heading', () => {
        const group = DOCUMENT.lines.find((line) => line.kind === 'group')!;
        expect(flowHighlightOf(group)).toEqual({});
    });

    it('finds the arrow the step belongs to, by the labels the box holds', () => {
        const sequence = flowSequence('userService.ts', flowNodes(CLOSURE));
        const step = DOCUMENT.lines.find((line) => line.targetName === 'validateUser')!;
        const at = flowArrowFor(sequence, step);
        expect(at).toBeGreaterThanOrEqual(0);
        expect(sequence.interactions[at]).toMatchObject({ to: 'validate.ts', message: 'validateUser' });
    });

    it('answers with no arrow rather than an arbitrary one', () => {
        const sequence = flowSequence('userService.ts', flowNodes(CLOSURE));
        const raise = DOCUMENT.lines.find((line) => line.kind === 'raise')!;
        expect(flowArrowFor(sequence, raise)).toBe(-1);
        const env = DOCUMENT.lines.find((line) => line.kind === 'env')!;
        expect(flowArrowFor(sequence, env)).toBe(-1);
    });
});

describe('the labels the box holds', () => {
    it('drops what a diagram grammar would choke on rather than escaping it', () => {
        expect(sanitizeLabel('render<T>')).toBe('renderT');
        expect(sanitizeLabel('a;b')).toBe('ab');
    });

    it('cuts a label that would widen the box off screen', () => {
        expect(sanitizeLabel('x'.repeat(80))).toHaveLength(60);
        expect(sanitizeLabel('x'.repeat(80)).endsWith('.')).toBe(true);
    });
});
