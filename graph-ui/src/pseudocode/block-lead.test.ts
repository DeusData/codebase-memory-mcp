/**
 * The sentence over the block, and the two rules it must never break.
 *
 * It always stands, and it never counts steps. The first is why a block with
 * nothing to report still has a head: a sentence that only appears when
 * something was found makes its own absence a claim. The second is the rule
 * this cycle was explicitly written under: there is no length at which a
 * function is "short enough" to lose its steps, because such a number would be
 * guessed and would then quietly remove content that was never wrong.
 */

import { describe, expect, it } from 'vitest';

import { blockLeadOf } from './block-lead';
import { buildImportsGroup } from './imports-group';
import type { FileImportsDto } from './imports-group';
import { buildPseudocode } from './pseudocode-builder';
import type { SemanticIR } from '../core/semantic-ir';

const known = <T>(value: T) => ({ value, state: 'known' as const, evidence: [] });

function irOf(steps: SemanticIR['steps']['value']): SemanticIR {
    return {
        symbol: {
            name: 'getOrder',
            qualifiedName: 'p.src.services.orderService.getOrder',
            kind: 'function',
            uri: 'file:///w/src/services/orderService.ts',
            range: { start: { line: 7, character: 0 }, end: { line: 25, character: 0 } },
            selectionRange: { start: { line: 7, character: 16 }, end: { line: 7, character: 24 } },
        },
        steps: known(steps),
        calls: known(steps),
        calledBy: known([]),
        reads: known([]),
        writes: known([]),
        throws: known([]),
        tests: known([]),
        typeRefs: known([]),
    } as unknown as SemanticIR;
}

const IR = irOf([
    {
        targetName: 'validateId',
        targetQualifiedName: 'p.src.util.validate.validateId',
        targetFile: 'file:///w/src/util/validate.ts',
        line: 10,
        targetLine: 33,
    },
]);

const GRAPH = {
    nodes: [
        { id: 1, name: 'validateId', qualified_name: 'p.src.util.validate.validateId' },
        { id: 2, name: 'ValidationError', qualified_name: 'p.src.util.validate.ValidationError' },
    ],
    edges: [{ source: 1, target: 2, type: 'RAISES' }],
};

function importsOf(entries: FileImportsDto['entries']) {
    return buildImportsGroup({
        imports: { entries, truncated: false, indexedTargets: [], sourceRead: true, fileIrs: [IR] },
        irs: [IR],
        uri: IR.symbol.uri,
    });
}

const USED = { name: 'validateId', module: '../util/validate', line: 3, origin: 'source' as const, evidence: [] };
const UNUSED = { name: 'insert', module: '../repo/db', line: 4, origin: 'source' as const, evidence: [] };
const UNCHECKABLE = { module: './setup', line: 5, origin: 'source' as const, evidence: [] };

describe('the sentence over the block', () => {
    const document = buildPseudocode({ kind: 'symbol', label: 'getOrder' }, { irs: [IR], graph: GRAPH });

    it('leads with an import this symbol does not reach, and keeps the limit in the sentence', () => {
        const lead = blockLeadOf(document, importsOf([USED, UNUSED]), 'getOrder');
        expect(lead.kind).toBe('unused-imports');
        expect(lead.text).toBe(
            'You cannot see this in the code: 1 of the 2 names this file pulls in is not used by '
            + 'getOrder as far as the index shows.',
        );
    });

    it('leads with what cannot be checked when nothing is unused', () => {
        const lead = blockLeadOf(document, importsOf([USED, UNCHECKABLE]), 'getOrder');
        expect(lead.kind).toBe('unchecked-imports');
        expect(lead.text).toContain('CodeAtlas cannot check either way');
    });

    it('leads with what lies behind the calls when the imports check out', () => {
        const lead = blockLeadOf(document, importsOf([USED]), 'getOrder');
        expect(lead.kind).toBe('behind-calls');
        expect(lead.text).toContain('1 step below');
        expect(lead.behindSteps).toBe(1);
    });

    it('still says something when there is nothing to report', () => {
        const bare = buildPseudocode({ kind: 'symbol', label: 'getOrder' }, { irs: [IR] });
        const lead = blockLeadOf(bare, importsOf([USED]), 'getOrder');
        expect(lead.kind).toBe('nothing');
        expect(lead.text).toContain('Nothing stands above the steps');
        expect(lead.behindSteps).toBe(0);
    });

    it('says that the import answer is still on its way rather than that there is none', () => {
        const lead = blockLeadOf(document, undefined, 'getOrder');
        expect(lead.kind).toBe('pending');
        expect(lead.text).toContain('has not arrived yet');
    });

    it('reads the same for one step as for many: no length decides anything', () => {
        const long = buildPseudocode(
            { kind: 'symbol', label: 'getOrder' },
            {
                irs: [irOf(Array.from({ length: 20 }, (_, at) => ({
                    targetName: `step${at}`,
                    targetQualifiedName: 'p.src.util.validate.validateId',
                    targetFile: 'file:///w/src/util/validate.ts',
                    line: 10 + at,
                    targetLine: 33,
                })))],
                graph: GRAPH,
            },
        );
        expect(long.lines).toHaveLength(20);
        expect(blockLeadOf(long, importsOf([USED]), 'getOrder').kind)
            .toBe(blockLeadOf(document, importsOf([USED]), 'getOrder').kind);
    });
});
