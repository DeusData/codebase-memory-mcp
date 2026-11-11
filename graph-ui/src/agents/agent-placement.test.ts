/*
 * Die Zuordnung, an einem Ausschnitt des echten Layouts.
 *
 * Die Knoten hier sind abgelesen (fixtures/atlas-sample, indiziert, `/api/layout`)
 * und nicht erfunden: der Modulknoten umspannt die ganze Datei, der Dateiknoten
 * traegt gar keine Zeilen, der Ordnerknoten auch nicht. Genau diese drei Formen
 * sind der Grund, warum die Zuordnung drei Antworten kennt.
 */

import { describe, expect, it } from 'vitest';

import type { GraphNode } from '../galaxy/types';
import { readAgentEvent } from './agent-event';
import type { AgentEvent } from './agent-event';
import {
    buildPlacementIndex,
    ghostsFor,
    normalizePath,
    placeEvent,
    testedPathOf,
} from './agent-placement';

const node = (over: Partial<GraphNode> & { id: number }): GraphNode => ({
    x: 0, y: 0, z: 0, label: 'Function', name: 'x', size: 1, color: '#ffffff', ...over,
});

const NODES: GraphNode[] = [
    node({
        id: 50, label: 'Module', name: 'src/services/userService.ts',
        file_path: 'src/services/userService.ts', start_line: 1, end_line: 43,
        qualified_name: 'p.src.services.userService',
    }),
    node({
        id: 51, label: 'Function', name: 'createUser',
        file_path: 'src/services/userService.ts', start_line: 23, end_line: 36,
        qualified_name: 'p.src.services.userService.createUser',
    }),
    node({
        id: 52, label: 'Function', name: 'listUsers',
        file_path: 'src/services/userService.ts', start_line: 18, end_line: 21,
        qualified_name: 'p.src.services.userService.listUsers',
    }),
    node({
        id: 53, label: 'File', name: 'userService.ts',
        file_path: 'src/services/userService.ts',
        qualified_name: 'p.src.services.userService.ts.__file__',
    }),
    node({ id: 5, label: 'Folder', name: 'src', file_path: 'src', qualified_name: 'p.src' }),
    node({
        id: 60, label: 'Module', name: 'test/userService.test.ts',
        file_path: 'test/userService.test.ts', start_line: 1, end_line: 17,
        qualified_name: 'p.test.userService.test',
    }),
    node({
        id: 69, label: 'Function', name: 'validateUser',
        file_path: 'src/util/validate.ts', start_line: 19, end_line: 31,
        qualified_name: 'p.src.util.validate.validateUser',
    }),
    node({
        id: 70, label: 'Function', name: 'validateId',
        file_path: 'src/util/validate.ts', start_line: 33, end_line: 38,
        qualified_name: 'p.src.util.validate.validateId',
    }),
];

const INDEX = buildPlacementIndex(NODES);

const event = (over: Partial<AgentEvent>): AgentEvent => readAgentEvent({
    ts: 1, agent: 'a', run: 'r', seq: 1, phase: 'end', tool: 'Read', path: '', detail: '',
    ...over,
}) as AgentEvent;

describe('die Zuordnung', () => {
    it('trifft mit Datei und Zeilen den ENGSTEN Knoten und nicht das Modul', () => {
        const placed = placeEvent(
            event({ tool: 'Edit', path: 'src/services/userService.ts', lines: [24, 30] }),
            'write',
            INDEX,
        );
        expect(placed.kind).toBe('range');
        expect(placed.name).toBe('createUser');
        expect(placed.nodeId).toBe(51);
        expect(placed.uncertain).toBe(false);
    });

    it('nimmt bei zwei gleich engen Treffern immer denselben, damit das Bild ruhig bleibt', () => {
        const twins = buildPlacementIndex([
            node({ id: 91, name: 'b', file_path: 'a.ts', start_line: 1, end_line: 5 }),
            node({ id: 90, name: 'a', file_path: 'a.ts', start_line: 1, end_line: 5 }),
        ]);
        const placed = placeEvent(event({ path: 'a.ts', lines: [2, 3] }), 'read', twins);
        expect(placed.nodeId).toBe(90);
    });

    it('trifft ohne Zeilen den Modulknoten', () => {
        const placed = placeEvent(
            event({ path: 'src/services/userService.ts' }),
            'read',
            INDEX,
        );
        expect(placed.kind).toBe('file');
        expect(placed.nodeId).toBe(50);
        expect(placed.name).toBe('src/services/userService.ts');
        expect(placed.uncertain).toBe(false);
    });

    it('kennzeichnet einen Knoten ohne Endzeile als unsicher, statt ihn als Treffer zu verkaufen', () => {
        const placed = placeEvent(event({ tool: 'Grep', path: 'src', detail: 'validate' }), 'search', INDEX);
        expect(placed.kind).toBe('file');
        expect(placed.nodeId).toBe(5);
        expect(placed.uncertain).toBe(true);
    });

    it('faellt auf die Datei zurueck, wenn kein Bereich passt, und sagt es nicht anders', () => {
        const placed = placeEvent(
            event({ path: 'src/services/userService.ts', lines: [200, 210] }),
            'read',
            INDEX,
        );
        expect(placed.kind).toBe('file');
        expect(placed.nodeId).toBe(50);
    });

    it('laesst nichts verschwinden, was der Index nicht kennt', () => {
        const placed = placeEvent(event({ path: 'package.json' }), 'read', INDEX);
        expect(placed.kind).toBe('none');
        expect(placed.nodeId).toBeUndefined();
        expect(placed.why).toBe('the index has no node for this path');

        const noPath = placeEvent(event({ tool: 'Bash', detail: 'ls -la' }), 'other', INDEX);
        expect(noPath.kind).toBe('none');
        expect(noPath.why).toBe('the event names no path');
    });

    it('pingt beim Suchen die Knoten, deren NAME das Muster traegt, und keine anderen', () => {
        const placed = placeEvent(
            event({ tool: 'Grep', path: 'src', detail: 'validate' }),
            'search',
            INDEX,
        );
        expect(placed.ghostIds).toContain(69);
        expect(placed.ghostIds).toContain(70);
        expect(placed.ghostIds).not.toContain(51);
        // Ein Muster von einem Zeichen ergibt nichts: das waere jeder Knoten.
        expect(ghostsFor('v', INDEX)).toEqual([]);
        expect(ghostsFor('', INDEX)).toEqual([]);
    });

    it('zieht einen Testlauf zu der Datei, die sein Befehl nennt, und sonst nirgendwohin', () => {
        const placed = placeEvent(
            event({ tool: 'Bash', detail: 'npx vitest run test/userService.test.ts' }),
            'test',
            INDEX,
        );
        expect(placed.testedNodeId).toBe(60);
        expect(placed.nodeId).toBe(60);

        const blind = placeEvent(
            event({ tool: 'Bash', detail: 'npm run test:unit' }),
            'test',
            INDEX,
        );
        expect(blind.kind).toBe('none');
        expect(blind.testedNodeId).toBeUndefined();
        expect(blind.why).toBe('the command names no file this index knows');
    });

    it('liest aus einem Befehl nur einen Pfad, den der Index wirklich fuehrt', () => {
        expect(testedPathOf('npx vitest run test/userService.test.ts', INDEX))
            .toBe('test/userService.test.ts');
        expect(testedPathOf('npx vitest run test/other.test.ts', INDEX)).toBe('');
        expect(testedPathOf('vitest ./src/services/userService.ts --run', INDEX))
            .toBe('src/services/userService.ts');
    });

    it('bringt Pfade auf eine Form, bevor es sie vergleicht', () => {
        expect(normalizePath('./src/config.ts')).toBe('src/config.ts');
        expect(normalizePath('/src/config.ts')).toBe('src/config.ts');
        expect(normalizePath('src\\config.ts')).toBe('src/config.ts');
        expect(normalizePath('src/')).toBe('src');
    });
});
