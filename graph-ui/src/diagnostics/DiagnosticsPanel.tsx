import { useEffect, useMemo, useRef, useState } from 'react';
import { loadCoverage, loadPathCoverage, type CoverageReading } from '../app/coverage-source';
import type { CoverageIndex } from '../app/tree-model';
import type { RpcIntelligenceClient } from '../provider/rpc-client';
import { DIAGNOSTIC_LABELS, diagnosticAction, diagnosticCategory, diagnosticRecords, freshnessText, localDiagnosticReport, type LocalDiagnosis } from './diagnostics-model';
import './diagnostics.css';

export interface DiagnosticsPanelProps {
    project: string;
    client: RpcIntelligenceClient;
    coverage?: CoverageIndex | null;
    coverageError?: string | null;
    active: boolean;
    onClose: () => void;
    path?: string;
    onNavigate: (path: string) => void;
    /** App can update its shared coverage reading from the same diagnosis. */
    onCoverage?: (reading: CoverageReading) => void;
}

export default function DiagnosticsPanel({ project, client, coverage, coverageError, active, onClose, path, onNavigate, onCoverage }: DiagnosticsPanelProps) {
    const [diagnosis, setDiagnosis] = useState<LocalDiagnosis | null>(null);
    const [report, setReport] = useState('');
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [status, setStatus] = useState('');
    const [query, setQuery] = useState('');
    const request = useRef(0);
    useEffect(() => {
        request.current += 1;
        setDiagnosis(null); setReport(''); setBusy(false); setError(null); setStatus(''); setQuery('');
        return () => { request.current += 1; };
    }, [project, path, active]);
    const index = diagnosis?.reading.index ?? coverage;
    const records = useMemo(() => diagnosticRecords(index), [index]);
    const filtered = useMemo(() => records.filter((record) => `${record.path} ${record.reason}`.toLowerCase().includes(query.toLowerCase())), [records, query]);
    const run = async () => {
        const ticket = ++request.current;
        setBusy(true); setError(null); setStatus('');
        try {
            const reading = await loadCoverage(client, project);
            if (ticket !== request.current) return;
            const pathAnswer = path ? await loadPathCoverage(client, project, path) : undefined;
            if (ticket !== request.current) return;
            const result: LocalDiagnosis = { project, requestedPath: path, checkedAt: new Date().toISOString(), reading, pathAnswer };
            setDiagnosis(result); setReport(localDiagnosticReport(result)); onCoverage?.(reading);
        } catch (reason) {
            if (ticket === request.current) setError(reason instanceof Error ? reason.message : String(reason));
        } finally {
            if (ticket === request.current) setBusy(false);
        }
    };
    const copy = async () => {
        try {
            if (!navigator.clipboard?.writeText) throw new Error('Clipboard unavailable; select the draft and copy it manually.');
            await navigator.clipboard.writeText(report); setStatus('Edited report copied locally. Nothing was sent.');
        } catch (reason) { setStatus(reason instanceof Error ? reason.message : String(reason)); }
    };
    const download = () => {
        const url = URL.createObjectURL(new Blob([report], { type: 'text/plain;charset=utf-8' }));
        const anchor = document.createElement('a'); anchor.href = url; anchor.download = 'local-index-diagnosis.txt';
        anchor.click(); URL.revokeObjectURL(url); setStatus('Edited report downloaded locally. Nothing was sent.');
    };
    if (!active) return null;
    return <section className="diagnostics-panel" role="dialog" aria-label="Index coverage diagnosis" data-testid="diagnostics-panel">
        <header><div><p className="diagnostics-eyebrow">LOCAL EVIDENCE</p><h2>Index coverage diagnosis</h2><p><code>{project}</code>{path && <> · Selected <code>{path}</code></>}</p></div><button type="button" onClick={onClose}>Close diagnosis</button></header>
        <p>The map can omit excluded files, unsupported inputs and failed or partial parses. Inspect the recorded reasons before relying on missing graph relationships.</p>
        {(error || coverageError) && <p role="alert" className="diagnostics-error">Coverage reading failed: {error || coverageError}. {index ? 'Previous records remain visible; this is not a fresh reading.' : 'Completeness is unknown.'}</p>}
        <div className="diagnostics-actions"><button type="button" disabled={busy || !project} onClick={() => { void run(); }}>{busy ? 'Reading local diagnosis…' : 'Run local diagnosis'}</button><span>Reads existing coverage metadata through this daemon. No agent, upload or issue creation.</span></div>
        {!index && <p>Coverage evidence has not been loaded. Run a local diagnosis to inspect the index records.</p>}
        {index && <>
            <div className="diagnostics-counts">{(['partial', 'excluded', 'unsupported', 'skipped', 'unknown'] as const).map((category) => <span key={category}>{DIAGNOSTIC_LABELS[category]}: <strong>{records.filter((record) => diagnosticCategory(record) === category).length}</strong></span>)}</div>
            <p className="diagnostics-caveat">Counts describe returned path records, including directories. They do not measure all repository files. Unsupported inputs and files never discovered may be absent from these records.</p>
            <p className="diagnostics-caveat">Partial parses mark source ranges the index parser could not interpret reliably. Grammar or preprocessor limitations and malformed source can cause this; a partial parse alone does not establish a build or runtime failure. Exclusions follow indexing rules.</p>
            {index.listingComplete && <p className="diagnostics-caveat">All recorded coverage entries have been loaded. The parser limitations and exclusions listed below still apply.</p>}
            {index.truncations.map((note) => <p key={note} className="diagnostics-caveat">Incomplete list: {note}</p>)}
        </>}
        {diagnosis && <div className="diagnostics-reading"><p>Checked {diagnosis.checkedAt} · Index generation <code>{diagnosis.reading.answer.metadata.generation || 'unavailable'}</code> · Recording {diagnosis.reading.answer.metadata.recordingStatus || 'unavailable'}</p>
            {!diagnosis.reading.answer.metadata.generationMatches && <p className="diagnostics-error">Coverage generation does not confirm the current index. Treat the map as potentially stale.</p>}
            {path && <p>{diagnosis.pathAnswer ? freshnessText(diagnosis.pathAnswer.freshness) : 'No path-specific evidence returned. Freshness is unknown.'}</p>}
            {diagnosis.pathAnswer?.coverage.map((row, i) => <p key={`${row.path}:${i}`}><code>{row.path}</code> · {row.kind} · {row.detail || 'No detail recorded'}</p>)}
            {diagnosis.reading.answer.caveat && <p className="diagnostics-caveat">{diagnosis.reading.answer.caveat}</p>}
        </div>}
        {records.length > 0 && <><label className="diagnostics-filter">Filter recorded paths or reasons<input type="search" value={query} onChange={(event) => setQuery(event.currentTarget.value)} /></label>
            <ol className="diagnostics-records">{filtered.slice(0, 100).map((record) => <li key={record.path} className={`diagnostics-record-${diagnosticCategory(record)}`}>
                <div><strong>{DIAGNOSTIC_LABELS[diagnosticCategory(record)]}</strong> · {record.kind} <button type="button" onClick={() => onNavigate(record.path)}><code>{record.path}</code></button></div>
                <p><strong>Recorded reason:</strong> {record.reason || 'No reason recorded'}</p>
                <p><strong>Evidence:</strong> {record.sources.join(', ') || 'Source not reported'}</p>
                <p>{diagnosticAction(record)}</p>
            </li>)}</ol><p className="diagnostics-caveat">Showing {Math.min(100, filtered.length)} of {filtered.length} matching records. Filter to locate another path.</p></>}
        {index && records.length === 0 && <p>No gap records returned. This does not prove complete indexing; undiscovered or unsupported files may be unrecorded.</p>}
        {diagnosis && <section aria-label="Local diagnostic report"><h3>Review the local report</h3><p>This draft contains paths and recorded reasons. Edit it before sharing. Copy and download stay local; publication would require a separate, explicit action outside this panel.</p>
            <textarea aria-label="Editable local diagnostic report" value={report} onChange={(event) => setReport(event.currentTarget.value)} spellCheck={false} />
            <div className="diagnostics-actions"><button type="button" onClick={() => { void copy(); }}>Copy edited report</button><button type="button" onClick={download}>Download edited report</button><span role="status">{status}</span></div>
        </section>}
    </section>;
}
