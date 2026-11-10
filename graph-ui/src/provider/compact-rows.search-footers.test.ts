import { describe, expect, it } from 'vitest';

import { parseSearchResults } from './compact-rows';

/*
 * Handtest 04.10.: In cbm meldete die Galaxy-Suche bei vielen Begriffen
 * "Index search unavailable". Der Server schreibt unter eine Suchantwort mehr
 * Fusszeilen, als der Parser kannte (bm25_emit_metadata_tree in src/mcp/mcp.c):
 * next_offset und truncation_reason schon bei einer vollen Seite,
 * candidate_window_saturated bei sehr vielen Kandidaten, und bei einem zu
 * kleinen Ausgabebudget max_output_bytes, continuation_requires_higher_budget,
 * output_budget_floor_exceeded und hint. Jede davon liess die Suche scheitern.
 */

/** Wie der Server auf search_graph {project: cbm, query: working, limit: 3} antwortet. */
const PAGED = [
    'results: 3  (cols: qn label file lines rank)',
    '  cbm.graph-ui.tools.freshclone-check.workingTreeChanges Function graph-ui/tools/freshclone-check.mjs 112-138 -15.29',
    '  cbm.__branch__.working-tree Branch {} - -14.24',
    '  cbm.tests.test_cypher.cypher_with_alias_stays_in_scope_issue1919 Function tests/test_cypher.c 2151-2164 -12.2',
    'total: 14',
    'total_relation: eq',
    'search_mode: bm25',
    'returned: 3',
    'has_more: true',
    'next_offset: 3',
    'truncated: true',
    'truncation_reason: page_limit',
    '',
].join('\n');

describe('parseSearchResults reads every footer the server writes', () => {
    it('reads a full page with next_offset and truncation_reason', () => {
        const parsed = parseSearchResults(PAGED);
        expect(parsed.rows).toHaveLength(3);
        expect(parsed.total).toBe(14);
        expect(parsed.hasMore).toBe(true);
        expect(parsed.nextOffset).toBe(3);
        expect(parsed.truncated).toBe(true);
        expect(parsed.truncationReason).toBe('page_limit');
    });

    it('reads a saturated candidate window', () => {
        const saturated = [
            'results: 1  (cols: qn label)',
            '  a.b.c Function',
            'total: 1',
            'total_relation: gte',
            'candidate_window_saturated: true',
            'search_mode: bm25',
            'returned: 1',
            'has_more: false',
            'truncated: true',
            'truncation_reason: candidate_window',
            '',
        ].join('\n');
        const parsed = parseSearchResults(saturated);
        expect(parsed.totalRelation).toBe('gte');
        expect(parsed.candidateWindowSaturated).toBe(true);
        expect(parsed.truncationReason).toBe('candidate_window');
        expect(parsed.rows).toHaveLength(1);
    });

    it('reads the lines of a too small output budget', () => {
        const budget = [
            'results: 0  (cols: qn label)',
            'total: 5',
            'total_relation: eq',
            'search_mode: bm25',
            'returned: 0',
            'has_more: true',
            'continuation_requires_higher_budget: true',
            'truncated: true',
            'truncation_reason: output_budget',
            'max_output_bytes: 200',
            'output_budget_floor_exceeded: true',
            'hint: "first identity row exceeds max_output_bytes; raise max_output_bytes"',
            '',
        ].join('\n');
        const parsed = parseSearchResults(budget);
        expect(parsed.rows).toHaveLength(0);
        expect(parsed.truncationReason).toBe('output_budget');
        expect(parsed.maxOutputBytes).toBe(200);
        expect(parsed.continuationRequiresHigherBudget).toBe(true);
        expect(parsed.outputBudgetFloorExceeded).toBe(true);
        expect(parsed.hint).toContain('max_output_bytes');
    });

    it('still rejects a line it does not know', () => {
        expect(() => parseSearchResults(PAGED.replace('truncation_reason: page_limit', 'surprise: 1'))).toThrow(/unbekannte Zeile/);
    });
});
