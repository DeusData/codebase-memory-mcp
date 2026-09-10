import { describe, expect, it } from 'vitest';

import type { SymbolRef } from '../core/focus-protocol';
import type { SemanticIR } from '../core/semantic-ir';
import { keepsReadyTwinForTarget } from './ready-twin-target';

const symbol = (qualifiedName: string | undefined): SymbolRef => ({
    name: 'createUser',
    qualifiedName,
    kind: 'function',
    uri: 'file:///workspace/src/services/userService.ts',
    range: { start: { line: 22, character: 0 }, end: { line: 35, character: 1 } },
});

const irFor = (value: SymbolRef): SemanticIR => ({ symbol: value } as SemanticIR);

describe('keepsReadyTwinForTarget', () => {
    it('keeps a ready IR for the same uniquely qualified target', () => {
        const current = symbol('sample.user.createUser');

        expect(keepsReadyTwinForTarget(current, current, irFor(current), 'ready')).toBe(true);
    });

    it('does not keep a ready twin for a different target', () => {
        const current = symbol('sample.user.createUser');
        const other = symbol('sample.user.validateUser');

        expect(keepsReadyTwinForTarget(other, current, irFor(current), 'ready')).toBe(false);
    });

    it('does not keep a twin without a completed IR or unique target identity', () => {
        const current = symbol('sample.user.createUser');

        expect(keepsReadyTwinForTarget(current, current, undefined, 'ready')).toBe(false);
        expect(keepsReadyTwinForTarget(symbol(undefined), current, irFor(current), 'ready')).toBe(false);
        expect(keepsReadyTwinForTarget(current, current, irFor(current), 'loading')).toBe(false);
    });
});
