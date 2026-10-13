import { describe, expect, it } from 'vitest';
import { DEFAULT_EDGE_COLOR, EDGE_TYPE_COLORS, edgeColor, edgePhase, isDirectedEdge } from './edge-style';

describe('shared edge appearance', () => {
    it('keeps equivalent type spelling and mixed aggregates honest', () => {
        expect(edgeColor('calls')).toBe(EDGE_TYPE_COLORS.CALLS);
        expect(edgeColor('http-calls')).toBe(EDGE_TYPE_COLORS.HTTP_CALLS);
        expect(edgeColor('RELATIONSHIPS', ['CALLS', 'CALLS'])).toBe(edgeColor('CALLS'));
        expect(edgeColor('RELATIONSHIPS', ['CALLS', 'IMPORTS'])).toBe(DEFAULT_EDGE_COLOR);
        expect(edgeColor('future_relation')).toBe(DEFAULT_EDGE_COLOR);
        expect(edgeColor(undefined as unknown as string)).toBe(DEFAULT_EDGE_COLOR);
        expect(isDirectedEdge(undefined as unknown as string)).toBe(false);
    });
    it('animates only known directed relationships, including directed mixed aggregates', () => {
        for (const type of ['CALLS', 'IMPORTS', 'DATA_FLOWS', 'INHERITS', 'CONTAINS_FILE', 'CONTAINS_FOLDER', 'CALL_REFERENCE', 'READS', 'WRITES', 'THROWS', 'TESTS']) expect(isDirectedEdge(type)).toBe(true);
        for (const type of ['SIMILAR_TO', 'SEMANTICALLY_RELATED', 'FILE_CHANGES_WITH', 'RELATIONSHIPS', 'future_relation', '']) expect(isDirectedEdge(type)).toBe(false);
        expect(isDirectedEdge('RELATIONSHIPS', ['CALLS', 'IMPORTS'])).toBe(true);
        expect(isDirectedEdge('RELATIONSHIPS', ['CALLS', 'SIMILAR_TO'])).toBe(false);
        expect(isDirectedEdge('RELATIONSHIPS', ['CALLS', 'future_relation'])).toBe(false);
    });
    it('uses stable, bounded phases independent of render order', () => {
        const ids = Array.from({ length: 40 }, (_, index) => `source:${index}:target`);
        const phases = new Map(ids.map(id => [id, edgePhase(id)]));
        for (const id of [...ids].reverse()) { expect(edgePhase(id)).toBe(phases.get(id)); expect(edgePhase(id)).toBeGreaterThanOrEqual(0); expect(edgePhase(id)).toBeLessThan(1); }
        expect(new Set(phases.values()).size).toBe(ids.length);
    });
});
