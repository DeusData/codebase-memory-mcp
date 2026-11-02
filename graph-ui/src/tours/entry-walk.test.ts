/**
 * Der Vorwaerts-Walk als Dokument.
 *
 * Drei Aussagen, und die erste ist der Grund fuer den ganzen Modus: Schritt eins
 * ist das gewaehlte Symbol, ohne Pflichtprogramm davor. Dazu die beiden
 * Ehrlichkeitsregeln: ein Deckel wird am Ende benannt, und ein erreichtes
 * Symbol ohne Datei wird nicht zu einem Schritt, der ins Leere fuehrt.
 */

import { describe, expect, it } from 'vitest';

import type { SymbolRef } from '../core/focus-protocol';
import { toEditorRange } from '../core/positions';
import type { ClosureResult } from '../provider/closure';
import { entryTourId, entryWalkTour } from './entry-walk';

const ROOT = '/workspace';

function ref(name: string, file: string, line: number, qualified = true): SymbolRef {
    return {
        ...(qualified ? { nodeId: `p.${name}`, qualifiedName: `p.${name}` } : {}),
        name,
        kind: 'function',
        uri: file.length === 0 ? '' : `file://${ROOT}/${file}`,
        range: toEditorRange(line, line),
        selectionRange: toEditorRange(line, line),
    };
}

const CLOSURE: ClosureResult = {
    root: ref('createUser', 'src/services/userService.ts', 23),
    nodes: [
        { symbol: ref('createUser', 'src/services/userService.ts', 23), hop: 0 },
        { symbol: ref('insert', 'src/repo/db.ts', 31), hop: 1, via: 'p.createUser' },
        { symbol: ref('validateUser', 'src/util/validate.ts', 19), hop: 1, via: 'p.createUser' },
        { symbol: ref('query', 'src/repo/db.ts', 17), hop: 2, via: 'p.insert' },
    ],
    edges: [
        { from: 'p.createUser', to: 'p.validateUser', line: 24 },
        { from: 'p.createUser', to: 'p.insert', line: 30 },
        { from: 'p.insert', to: 'p.query', line: 33 },
    ],
    truncated: false,
    visited: 4,
    depth: 3,
    cap: 15,
};

describe('the forward walk', () => {
    it('starts at the symbol the reader chose', () => {
        const { document } = entryWalkTour(CLOSURE);
        expect(document.steps[0].title).toBe('Start here: createUser');
        expect(document.steps[0].primary).toMatchObject({
            kind: 'symbol',
            filePath: 'src/services/userService.ts',
            line: 23,
            qualifiedName: 'p.createUser',
        });
    });

    it('has no preamble in front of the chosen symbol', () => {
        const { document } = entryWalkTour(CLOSURE);
        const before = document.steps.slice(0, document.steps.findIndex((step) => step.title.includes('createUser')));
        expect(before).toEqual([]);
        expect(document.steps.every((step) => !step.primary.filePath.endsWith('config.ts'))).toBe(true);
    });

    it('keeps the closure order and numbers the steps by it', () => {
        const { document } = entryWalkTour(CLOSURE);
        expect(document.steps.map((step) => step.title)).toEqual([
            'Start here: createUser',
            'Hop 1: insert',
            'Hop 1: validateUser',
            'Hop 2: query',
        ]);
        expect(document.steps.map((step) => step.order)).toEqual([0, 1, 2, 3]);
    });

    it('names who calls what and on which line, and nothing about the code', () => {
        const { document } = entryWalkTour(CLOSURE);
        expect(document.steps[1].description).toContain('a call from createUser to insert on line 30');
        expect(document.steps[3].description).toContain('a call from insert to query on line 33');
        expect(document.steps[3].description).toContain('2 hops out from where you started');
    });

    it('counts the calls out of the chosen symbol on its own step', () => {
        const { document } = entryWalkTour(CLOSURE);
        expect(document.steps[0].description).toContain('2 calls out of it');
    });

    it('says so when the chosen symbol reaches nothing', () => {
        const alone: ClosureResult = { ...CLOSURE, nodes: [CLOSURE.nodes[0]], edges: [], visited: 1 };
        expect(entryWalkTour(alone).document.steps[0].description)
            .toContain('records no call out of it');
    });

    it('gives every step an id of its own, derived from the name', () => {
        const { document } = entryWalkTour(CLOSURE);
        const ids = document.steps.map((step) => step.id);
        expect(new Set(ids).size).toBe(ids.length);
        expect(ids[0]).toBe('p-createuser');
    });

    it('names the walk after the symbol it starts at', () => {
        const { document } = entryWalkTour(CLOSURE);
        expect(document.title).toBe('Forward from createUser');
        expect(document.id).toBe(entryTourId('p.createUser'));
        expect(document.generated.strategy).toBe('forward-walk');
    });

    it('is byte-identical on two runs over one closure', () => {
        expect(JSON.stringify(entryWalkTour(CLOSURE))).toBe(JSON.stringify(entryWalkTour(CLOSURE)));
    });
});

describe('what a bounded walk admits', () => {
    it('ends with the cap and the depth when a bound bit', () => {
        const capped: ClosureResult = { ...CLOSURE, truncated: true, cap: 3, visited: 9 };
        const walk = entryWalkTour(capped);
        expect(walk.endNote).toContain('walk capped at 3 symbols (depth 3)');
        expect(walk.document.generated.truncated).toBe(true);
    });

    it('ends with nothing when the walk reached the end of the graph', () => {
        expect(entryWalkTour(CLOSURE).endNote).toBe('');
        expect(entryWalkTour(CLOSURE).document.generated.truncated).toBeUndefined();
    });
});

describe('a reached symbol with nowhere to go', () => {
    const withoutFile: ClosureResult = {
        ...CLOSURE,
        nodes: [...CLOSURE.nodes, { symbol: ref('elsewhere', '', 1), hop: 2, via: 'p.insert' }],
    };

    it('is not a step, because a step that cannot be opened is a dead row', () => {
        const { document } = entryWalkTour(withoutFile);
        expect(document.steps.map((step) => step.title)).not.toContain('Hop 2: elsewhere');
    });

    it('is counted out loud rather than swallowed', () => {
        expect(entryWalkTour(withoutFile).endNote)
            .toContain('1 reached symbol carries no file in the index');
    });
});

describe('a callee the index would not name', () => {
    const unresolved: ClosureResult = {
        ...CLOSURE,
        nodes: [CLOSURE.nodes[0], { symbol: ref('mystery', 'src/repo/db.ts', 44, false), hop: 1, via: 'p.createUser' }],
    };

    it('opens the file rather than claiming a symbol', () => {
        const { document } = entryWalkTour(unresolved);
        expect(document.steps[1].primary).toEqual({ kind: 'file', filePath: 'src/repo/db.ts' });
        expect(document.steps[1].description).toContain('could not resolve this name to a symbol of its own');
    });
});
