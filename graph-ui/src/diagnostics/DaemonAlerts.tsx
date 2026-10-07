import { useEffect, useMemo, useState } from 'react';
import type { LogTail } from '../projects/projects-model';
import { logLevel } from '../system/system-model';
import type { SystemReading } from '../system/useSystemPoll';
import type { CoverageIndex } from '../app/tree-model';
import { diagnosticCategory, diagnosticRecords } from './diagnostics-model';
import './diagnostics.css';

function readableMessage(raw: string): string {
    try {
        const record: unknown = JSON.parse(raw);
        if (record && typeof record === 'object' && !Array.isArray(record) && 'message' in record && typeof record.message === 'string') return record.message;
    } catch { /* Plain daemon log lines already are the display message. */ }
    return raw;
}

export interface DaemonAlertsProps {
    project?: string;
    scope?: 'project' | 'daemon';
    onScopeChange?: (scope: 'project' | 'daemon') => void;
    reading: SystemReading<LogTail> & { refresh?: () => void };
    onDiagnose?: () => void;
    coverage?: CoverageIndex | null;
    coverageKnown?: boolean;
    coverageError?: string | null;
}

/** Priority feed independent of graph selection; these are daemon events, not graph facts. */
export default function DaemonAlerts({ reading, onDiagnose, coverage, coverageKnown, coverageError, project, scope = 'project', onScopeChange }: DaemonAlertsProps) {
    const [expanded, setExpanded] = useState(scope === 'daemon');
    const [seen, setSeen] = useState<{ generation: string; id: number }>();
    const generation = reading.data?.generation ?? 'legacy';
    const cursor = Math.max(0, ...(reading.data?.records ?? []).map(row => row.id));
    useEffect(() => {
        if (reading.data && seen?.generation !== generation) setSeen({ generation, id: cursor });
    }, [reading.data, seen?.generation, generation, cursor]);
    const records = useMemo(() => {
        const rows = reading.data?.records?.map((record) => ({
            ...record, key: `${reading.data?.generation ?? 'daemon'}:${record.id}`,
            severity: logLevel(`level=${record.level}`),
        })) ?? (reading.data?.lines ?? []).map((line, index) => ({
            key: `legacy:${index}`, id: index, ts: '', level: '', source: 'Daemon log', message: line, severity: logLevel(line),
        }));
        return rows.filter((row) => row.severity === 'error' || row.severity === 'warn')
            .sort((left, right) => (left.severity === 'error' ? 0 : 1) - (right.severity === 'error' ? 0 : 1) || right.id - left.id);
    }, [reading.data]);
    const errors = records.filter((record) => record.severity === 'error').length;
    const fresh = seen?.generation === generation ? records.filter(row => row.id > seen.id) : [];
    const newErrors = scope === 'project' ? fresh.filter(row => row.severity === 'error') : [];
    const visible = expanded ? records : newErrors.slice(0, 1);
    const acknowledge = () => { setSeen({ generation, id: cursor }); setExpanded(false); };
    const coverageSummary = useMemo(() => {
        if (coverageError) return 'Index coverage unavailable; the map may be incomplete.';
        if (!coverage || coverageKnown === false) return 'Index coverage unknown.';
        const rows = diagnosticRecords(coverage);
        if (!rows.length) return 'No coverage gaps recorded; complete indexing is not guaranteed.';
        const partial = rows.filter((row) => row.state === 'partial').length;
        const excluded = rows.filter((row) => diagnosticCategory(row) === 'excluded').length;
        const skipped = rows.filter((row) => row.state === 'skipped').length;
        return `Index coverage: ${partial} partially parsed paths · ${skipped} skipped · ${excluded} intentionally excluded${coverage.truncations.length ? ' · detail listing incomplete' : ''}.`;
    }, [coverage, coverageKnown, coverageError]);
    return <section className={`daemon-alerts${newErrors.length || reading.error ? ' daemon-alerts-has-errors' : ''}${expanded ? ' daemon-alerts-expanded' : ' daemon-alerts-collapsed'}`} aria-label="Daemon errors and warnings" data-testid="daemon-alerts">
        <div className="daemon-alerts-heading">
            <strong>{scope === 'daemon' ? 'Daemon history · all projects' : newErrors.length ? `! ${newErrors.length} new recorded errors · ${project ?? 'this project'}` : `Project events${project ? ` · ${project}` : ''}`}</strong>
            <span>{reading.error ? 'Connection interrupted · last reading' : reading.loading && !reading.data ? 'Reading events…' : reading.updatedAt ? `Last checked ${new Date(reading.updatedAt).toLocaleTimeString()}` : 'No event reading yet'}</span>
            <button type="button" onClick={() => setExpanded(!expanded)}>{expanded ? 'Close history' : `History (${reading.data?.total ?? records.length} retained)`}</button>
            {newErrors.length > 0 && <button type="button" onClick={acknowledge}>Acknowledge new events</button>}
            {expanded && onScopeChange && <button type="button" onClick={() => onScopeChange(scope === 'project' ? 'daemon' : 'project')}>{scope === 'project' ? 'All daemon history' : 'This project only'}</button>}
            {expanded && reading.refresh && <button type="button" onClick={reading.refresh} disabled={reading.loading}>Refresh events</button>}
            {onDiagnose && <button type="button" onClick={onDiagnose}>Inspect index coverage</button>}
        </div>
        {(coverageKnown !== undefined || coverage !== undefined || coverageError) && <p className="daemon-coverage-summary" title={coverageSummary}>{coverageSummary}</p>}
        {reading.error && <p role="alert" className="diagnostics-error">Event updates failed: {reading.error}. A missing update does not mean the daemon recovered.</p>}
        {reading.data?.persistent !== true && reading.data && <p className="diagnostics-caveat">Persistent event history unavailable. This log tail may not survive a restart.</p>}
        {expanded && <p className="diagnostics-caveat">Recorded history, not a count of active failures. {scope === 'project' ? 'Only events attributed to this project; older unattributed records remain in daemon history.' : 'Includes other projects and older events without project attribution.'} Acknowledging events does not resolve their causes or delete them.</p>}
        {visible.length > 0 && <ol className="daemon-alert-list">{visible.map((record) => {
            const message = readableMessage(record.message);
            return <li key={record.key} className={`daemon-alert-${record.severity}`}>
            <div><strong>{record.severity === 'error' ? '! ERROR' : '△ WARNING'}</strong> <span>{record.source || 'Source not reported'}</span> {record.ts && <time dateTime={record.ts}>{record.ts}</time>} {reading.data?.records && <span>Event #{record.id}</span>}{scope === 'daemon' && <span>Project: {'project' in record && typeof record.project === 'string' ? record.project : 'not recorded'}</span>}</div>
            <pre>{message.length > 400 ? `${message.slice(0, 400)}…` : message}</pre>
            <details><summary>Full recorded event</summary><pre>{record.message}</pre></details>
        </li>})}</ol>}
        {expanded && records.length === 0 && reading.data && <p>No attributed warning/error events in this retained reading. Unrecorded events and indexing gaps may still exist.</p>}
        {reading.data && expanded && <p className="diagnostics-caveat">{records.length} of {reading.data.total} matching retained events shown · {errors} errors in this page{reading.data.retentionLimit !== undefined ? ` · shared history limit ${reading.data.retentionLimit}` : ''}. Older events may have expired. Project and file details appear only when recorded.</p>}
    </section>;
}
