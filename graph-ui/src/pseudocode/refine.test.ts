import { describe, expect, it } from 'vitest';

import { CREATE_USER_IR } from '../test-support/twin-fixtures';
import { buildPseudocode, pseudocodeText } from './pseudocode-builder';
import type { PseudocodeDocument } from './pseudocode-builder';
import {
    applyRefinement,
    refineMaxTokens,
    refineSubjectText,
    refinedLines,
    refusalReason,
    unfence,
} from './refine';

const DOCUMENT: PseudocodeDocument = buildPseudocode(
    { kind: 'symbol', label: 'createUser' },
    { irs: [CREATE_USER_IR] },
);

/** The block, rewritten the way a well-behaved model would rewrite it. */
function goodRewrite(document: PseudocodeDocument): string {
    return document.lines
        .map((line) => (line.order === undefined ? `${line.text} (rephrased)` : `${line.order}. rephrased ${line.text.replace(/^\d+\.\s*/, '')}`))
        .join('\n');
}

describe('a rewrite that keeps the promise', () => {
    it('is applied, line for line', () => {
        const outcome = applyRefinement(DOCUMENT, goodRewrite(DOCUMENT));
        expect(outcome.kind).toBe('applied');
        if (outcome.kind === 'applied') {
            expect(outcome.document.lines.length).toBe(DOCUMENT.lines.length);
            expect(outcome.document.lines[0].text).toContain('rephrased');
            expect(outcome.document.lines[0].order).toBe(DOCUMENT.lines[0].order);
            expect(outcome.document.lines[0].sourceRef).toEqual(DOCUMENT.lines[0].sourceRef);
        }
    });

    it('survives a code fence the prompt forbade', () => {
        const fenced = '```\n' + goodRewrite(DOCUMENT) + '\n```';
        expect(applyRefinement(DOCUMENT, fenced).kind).toBe('applied');
    });
});

describe('a rewrite that breaks it is refused, with a reason', () => {
    it('refuses a different number of lines and says so', () => {
        const short = DOCUMENT.lines.slice(0, 3).map((line) => line.text).join('\n');
        const outcome = applyRefinement(DOCUMENT, short);
        expect(outcome.kind).toBe('refused');
        if (outcome.kind === 'refused') {
            expect(outcome.reason).toContain('different number of lines');
            expect(outcome.reason).toContain(String(DOCUMENT.lines.length));
        }
    });

    it('refuses a renumbered line and names the position', () => {
        const renumbered = DOCUMENT.lines
            .map((line, index) => (index === 0 ? '99. call validateUser' : line.text))
            .join('\n');
        const outcome = applyRefinement(DOCUMENT, renumbered);
        expect(outcome.kind).toBe('refused');
        if (outcome.kind === 'refused') {
            expect(outcome.reason).toContain('different number than it was sent with');
            expect(outcome.reason).toContain('line 1');
        }
    });

    it('refuses an empty answer', () => {
        const outcome = applyRefinement(DOCUMENT, '   \n\n');
        expect(outcome.kind).toBe('refused');
        if (outcome.kind === 'refused') {
            expect(outcome.reason).toContain('nothing to put back');
        }
    });

    it('refuses free prose that ignored the block entirely', () => {
        const outcome = applyRefinement(
            DOCUMENT,
            'This function creates a user. It validates the input and writes a row.',
        );
        expect(outcome.kind).toBe('refused');
    });

    it('refuses an answer that dropped the numbers off every line', () => {
        const unnumbered = DOCUMENT.lines
            .map((line) => line.text.replace(/^\d+\.\s*/, ''))
            .join('\n');
        const outcome = applyRefinement(DOCUMENT, unnumbered);
        expect(outcome.kind).toBe('refused');
    });
});

describe('the pieces', () => {
    it('sends the block a reader would copy', () => {
        expect(refineSubjectText(DOCUMENT)).toBe(pseudocodeText(DOCUMENT));
    });

    it('reads lines without the blanks', () => {
        expect(refinedLines(' a \n\n b ')).toEqual(['a', 'b']);
    });

    it('takes exactly the fences off and nothing else', () => {
        expect(unfence('```\na\nb\n```')).toBe('a\nb');
        expect(unfence('a\nb')).toBe('a\nb');
    });

    it('gives a longer block more room to come back in', () => {
        expect(refineMaxTokens(DOCUMENT)).toBeGreaterThanOrEqual(256);
    });

    it('names a reason even for an answer the validator merely dislikes', () => {
        expect(refusalReason(DOCUMENT, '').length).toBeGreaterThan(0);
    });
});
