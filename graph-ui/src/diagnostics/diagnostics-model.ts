import type { CoverageReading } from '../app/coverage-source';
import type { CoverageIndex, CoveragePathAnswer, CoverageRecord } from '../app/tree-model';

export type DiagnosticCategory = 'partial' | 'excluded' | 'unsupported' | 'skipped' | 'unknown';
export const DIAGNOSTIC_LABELS: Record<DiagnosticCategory, string> = {
    partial: 'Partial parse', excluded: 'Excluded by index rules', unsupported: 'Unsupported language',
    skipped: 'Indexing skipped', unknown: 'Unclassified coverage record',
};

/** Display the existing coverage join; only a specific recorded reason identifies unsupported input. */
export function diagnosticCategory(record: CoverageRecord): DiagnosticCategory {
    if (record.state === 'partial') return 'partial';
    if (record.state === 'ignored' || record.state === 'not-indexed') return 'excluded';
    // cbm.c emits this exact error; don't guess support from a file extension.
    if (record.state === 'skipped' && /^unsupported language(?:\s*\/|$)/.test(record.reason)) return 'unsupported';
    return record.state === 'skipped' ? 'skipped' : 'unknown';
}

export function diagnosticAction(record: CoverageRecord): string {
    const category = diagnosticCategory(record);
    if (category === 'partial') return 'Read the reported source ranges; verify missing symbols before relying on graph paths.';
    if (category === 'excluded') return 'Review the recorded exclusion rule; open the source directly or adjust that rule before reindexing.';
    if (category === 'unsupported') return 'Read this file directly; check parser support before requesting another index run.';
    if (category === 'skipped') return 'Inspect the recorded reason and source; correct the cause before reindexing.';
    return 'Inspect the existing coverage evidence; completeness cannot be inferred from this record.';
}

export function diagnosticRecords(index: CoverageIndex | null | undefined): CoverageRecord[] {
    const priority: Record<DiagnosticCategory, number> = { partial: 0, skipped: 1, unsupported: 2, unknown: 3, excluded: 4 };
    return [...(index?.records.values() ?? [])].sort((a, b) => priority[diagnosticCategory(a)] - priority[diagnosticCategory(b)] || a.path.localeCompare(b.path));
}

export function freshnessText(freshness: string): string {
    if (freshness === 'metadata_match') return 'File size and modification time match the index record. This is not a content-hash guarantee.';
    if (freshness === 'metadata_changed') return 'File size or modification time changed since indexing. Read the current source and reindex.';
    if (freshness === 'not_tracked') return 'No indexed file metadata exists for this path. It may be unindexed; pending work is not confirmed.';
    if (freshness === 'missing') return 'The requested file is missing on disk.';
    if (freshness === 'outside_project') return 'This path is outside the selected project.';
    return 'Freshness is unavailable. Missing evidence is not a clean result.';
}

export interface LocalDiagnosis {
    project: string;
    requestedPath?: string;
    checkedAt: string;
    reading: CoverageReading;
    pathAnswer?: CoveragePathAnswer;
}

/** A bounded, deterministic draft produced only after a user's local diagnosis action. */
export function localDiagnosticReport(diagnosis: LocalDiagnosis): string {
    const { reading, pathAnswer } = diagnosis;
    const records = diagnosticRecords(reading.index);
    const metadata = reading.answer.metadata;
    const lines = [
        '# Local index diagnosis: review before sharing', '',
        `Project: ${diagnosis.project}`, `Checked locally: ${diagnosis.checkedAt}`,
        `Index generation: ${metadata.generation || 'unavailable'}`,
        `Coverage recorded at: ${metadata.recordedAt || 'unavailable'}`,
        `Recording status: ${metadata.recordingStatus || 'unavailable'}`,
        `Coverage generation matches index: ${metadata.generationMatches ? 'yes' : 'not confirmed'}`,
        'Evidence: existing index_status + check_index_coverage. No source text or daemon log is attached.',
        'This is a best-effort record, not a repository-wide coverage percentage.',
        'Paths and reasons below may be sensitive. Edit this draft before sharing it.', '',
    ];
    if (diagnosis.requestedPath) {
        lines.push(`Selected path: ${diagnosis.requestedPath}`);
        lines.push(pathAnswer ? `Path status: ${pathAnswer.status || 'unavailable'}; freshness: ${pathAnswer.freshness || 'unavailable'}` : 'The daemon returned no evidence for the selected path.');
        if (pathAnswer) {
            lines.push(freshnessText(pathAnswer.freshness), `Reported next action: ${pathAnswer.recommendedAction || 'unavailable'}`);
            for (const row of pathAnswer.coverage) lines.push(`Path evidence: ${row.path} | ${row.kind} | ${row.detail || 'No detail recorded'}`);
        }
        lines.push('');
    }
    lines.push(`Recorded paths: ${records.length} (files and directories; not a file-coverage denominator).`);
    lines.push(`Ignored file records stored: ${metadata.ignoredFilesStored}; total reported: ${metadata.ignoredFilesTotal}.`);
    for (const note of reading.index.truncations) lines.push(`Incomplete list: ${note}`);
    if (reading.answer.caveat) lines.push(`Daemon caveat: ${reading.answer.caveat}`);
    if (!records.length) lines.push('No gap records returned. Discovery omissions and unsupported files may be unrecorded.');
    for (const record of records.slice(0, 200)) {
        lines.push('', `- ${record.path} (${record.kind}): ${DIAGNOSTIC_LABELS[diagnosticCategory(record)]}`,
            `  Recorded reason: ${record.reason || 'No reason recorded'}`,
            `  Evidence source: ${record.sources.join(', ') || 'Not reported'}`,
            `  Next check: ${diagnosticAction(record)}`);
    }
    if (records.length > 200) lines.push('', `Report limited to 200 of ${records.length} recorded paths.`);
    return lines.join('\n');
}
