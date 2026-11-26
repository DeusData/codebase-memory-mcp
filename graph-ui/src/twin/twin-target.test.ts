/*
 * Die eine Umrechnung zwischen dem, was der Twin traegt, und dem, was der
 * Reader oeffnen kann.
 */

import { describe, expect, it } from 'vitest';

import type { SymbolRef } from '../core/focus-protocol';
import { CREATE_USER_IR } from '../test-support/twin-fixtures';
import { stepTarget } from './twin-view-model';
import { ATLAS_WORKSPACE_ROOT, twinLocationOf, twinTargetOf, workspacePathOf } from './twin-target';

describe('workspacePathOf', () => {
    it('schaelt das Praefix wieder ab', () => {
        expect(workspacePathOf(`file://${ATLAS_WORKSPACE_ROOT}/src/util/validate.ts`)).toBe(
            'src/util/validate.ts',
        );
    });

    it('laesst eine URI ausserhalb des Praefix unveraendert, statt sie zurechtzubiegen', () => {
        expect(workspacePathOf('file:///anderswo/src/a.ts')).toBe('/anderswo/src/a.ts');
    });

    it('ist leer, wenn es keine URI gibt', () => {
        expect(workspacePathOf(undefined)).toBe('');
        expect(workspacePathOf('')).toBe('');
    });
});

describe('twinLocationOf', () => {
    it('macht aus der 0-basierten Editorzeile wieder eine 1-basierte Graph-Zeile', () => {
        const target = stepTarget(CREATE_USER_IR.steps.value[0])!;
        // Die Aufzeichnung nennt validate.ts:19 als Deklaration von validateUser.
        expect(twinLocationOf(target).line).toBe(19);
        expect(twinLocationOf(target).path).toContain('validate.ts');
    });

    it('nimmt selectionRange, wenn ein aufgeloestes Symbol eine traegt', () => {
        const symbol: SymbolRef = {
            name: 'createUser',
            kind: 'function',
            uri: `file://${ATLAS_WORKSPACE_ROOT}/src/services/userService.ts`,
            range: { start: { line: 22, character: 0 }, end: { line: 35, character: 0 } },
            selectionRange: { start: { line: 30, character: 0 }, end: { line: 30, character: 0 } },
        };
        expect(twinLocationOf(symbol)).toEqual({ path: 'src/services/userService.ts', line: 31 });
    });
});

describe('twinTargetOf', () => {
    it('macht aus einer gemeldeten Deklaration ein Ziel, das der Reader oeffnen kann', () => {
        const target = twinTargetOf({
            name: 'validateUser',
            qualifiedName: 'atlas.src.util.validate.validateUser',
            kind: 'function',
            filePath: 'src/util/validate.ts',
            startLine: 19,
            endLine: 31,
        });
        expect(target?.uri).toBe(`file://${ATLAS_WORKSPACE_ROOT}/src/util/validate.ts`);
        expect(twinLocationOf(target!)).toEqual({ path: 'src/util/validate.ts', line: 19 });
        expect(target?.range.end.line).toBe(31);
    });

    it('gibt ohne Datei kein Ziel zurueck, statt eine URI zu erfinden', () => {
        const base = { name: 'DB_URL', kind: 'variable' as const };
        expect(twinTargetOf(base)).toBeUndefined();
        expect(twinTargetOf({ ...base, filePath: '' })).toBeUndefined();
    });

    it('faellt ohne Zeile auf den Anfang der Datei zurueck', () => {
        const target = twinTargetOf({ name: 'userService', kind: 'module', filePath: 'src/services/userService.ts' });
        expect(twinLocationOf(target!).line).toBe(1);
    });

    it('behauptet keine Knoten-Identitaet, die es nicht gemessen hat', () => {
        const target = twinTargetOf({ name: 'x', kind: 'function', filePath: 'src/a.ts', startLine: 3 });
        expect(target?.nodeId).toBeUndefined();
    });
});
