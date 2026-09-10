import { describe, expect, it } from 'vitest';
import { buildCoverageIndex, readCoverageAnswer } from '../app/tree-model';
import type { CoverageRecord } from '../app/tree-model';
import { diagnosticCategory, freshnessText, localDiagnosticReport } from './diagnostics-model';

describe('Existing index evidence in diagnostics', () => {
    const row = (state: CoverageRecord['state'], reason: string): CoverageRecord => ({ path: 'src/a.unknown', kind: 'file', state, reason, sources: ['check_index_coverage'] });
    it('does not infer unsupported languages from paths or arbitrary skip messages', () => {
        expect(diagnosticCategory(row('skipped', 'read failed / read'))).toBe('skipped');
        expect(diagnosticCategory(row('skipped', 'unsupported language / extract'))).toBe('unsupported');
        expect(diagnosticCategory(row('partial', '2-4'))).toBe('partial');
        expect(diagnosticCategory(row('not-indexed', 'gitignore'))).toBe('excluded');
        expect(diagnosticCategory(row('skipped', 'unknown'))).toBe('skipped');
    });
    it('does not claim metadata matches prove equal content or untracked means pending', () => {
        expect(freshnessText('metadata_match')).toContain('not a content-hash guarantee');
        expect(freshnessText('not_tracked')).toContain('pending work is not confirmed');
        expect(freshnessText('metadata_changed')).toContain('changed since indexing');
        expect(freshnessText('')).toContain('not a clean result');
    });
    it('bounds a reviewable local report and retains source and uncertainty', () => {
        const entries = Array.from({ length: 205 }, (_, i) => ({ path: `src/f${i}.ts`, kind: 'parse_partial', detail: '2-3' }));
        const scopes = [{ requestedScope: '.', scope: '.', status: 'complete', total: 205, hasMore: false, entries }];
        const index = buildCoverageIndex({ scopes });
        const report = localDiagnosticReport({ project: 'fixture', checkedAt: '2026-09-09T12:00:00Z', reading: { index, scopes, answer: readCoverageAnswer({ metadata: { generation: 'g1', generation_matches: false }, caveat: 'best effort' }) } });
        expect(report).toContain('Index generation: g1');
        expect(report).toContain('not confirmed');
        expect(report).toContain('Report limited to 200 of 205 recorded paths');
        expect(report).toContain('Evidence source: check_index_coverage');
        expect(report).toContain('No source text or daemon log is attached');
        expect(report).not.toMatch(/\d+%/);
    });
});
