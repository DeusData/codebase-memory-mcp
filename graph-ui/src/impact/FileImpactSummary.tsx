import { useEffect, useId, useMemo, useState } from 'react';
import type { JSX } from 'react';
import { evidenceTarget, fetchSelectionImpact } from './selection-impact';
import type { SelectionImpact, SelectionImpactFinding, SelectionImpactTarget } from './selection-impact';
import { FILE_IMPACT_REFRESH } from './impact-strings';
import { RefreshControl, useRefreshFeedback } from '../ui/refresh/refresh-feedback';
import './file-impact-summary.css';

export interface FileImpactSummaryProps {
    project: string;
    target?: SelectionImpactTarget;
    onOpen: (target: SelectionImpactTarget) => void;
    expectedGeneration?: string;
    /** Production reads the same-origin graph and local Git endpoint. */
    load?: typeof fetchSelectionImpact;
}

export interface ImpactFileGroup {
    filePath: string;
    symbols: number;
    distance: number;
    testCandidate: boolean;
    target: SelectionImpactTarget;
}

/** File totals come only from returned evidence, never from a node-count estimate. */
export function aggregateFileImpact(data: SelectionImpact): {
    files: ImpactFileGroup[];
    testFiles: number;
    filesIncomplete: boolean;
    cochanges: SelectionImpact['history']['cochanges'];
} {
    const byFile = new Map<string, { findings: Map<number, SelectionImpactFinding> }>();
    if (data.structural.available) for (const finding of data.structural.findings) {
        if (!finding.file_path) continue;
        let group = byFile.get(finding.file_path);
        if (!group) { group = { findings: new Map() }; byFile.set(finding.file_path, group); }
        group.findings.set(finding.id, finding);
    }
    const files = [...byFile.entries()].map(([filePath, group]): ImpactFileGroup => {
        const findings = [...group.findings.values()].sort((a, b) => a.distance - b.distance || a.line - b.line || a.id - b.id);
        const first = findings[0]!;
        return { filePath, symbols: findings.length, distance: first.distance,
            testCandidate: findings.some(finding => finding.test_candidate), target: evidenceTarget(first) };
    }).sort((a, b) => a.distance - b.distance || b.symbols - a.symbols || a.filePath.localeCompare(b.filePath));
    const cochanges = data.history.available
        ? [...new Map(data.history.cochanges.map(row => [row.file_path, row])).values()]
            .sort((a, b) => b.shared_commits - a.shared_commits || a.file_path.localeCompare(b.file_path))
        : [];
    return { files, testFiles: files.filter(file => file.testCandidate).length,
        filesIncomplete: data.structural.truncated || data.structural.reachable > data.structural.findings.length,
        cochanges };
}

type Reading = { key: string } & (
    | { status: 'loading' | 'pending' | 'busy' }
    | { status: 'failed'; error: string }
    | { status: 'ready'; data: SelectionImpact }
);

function countLabel(count: number, noun: string, incomplete = false): string {
    return `${incomplete ? 'at least ' : ''}${count} ${noun}${count === 1 ? '' : 's'}`;
}

export default function FileImpactSummary({ project, target, onOpen, expectedGeneration,
    load = fetchSelectionImpact }: FileImpactSummaryProps): JSX.Element {
    const [reading, setReading] = useState<Reading>();
    // The last ready evidence and the selection it belongs to: a refresh of the same selection shows it until the new one is there.
    const [lastReady, setLastReady] = useState<{ key: string; data: SelectionImpact }>();
    const [retry, setRetry] = useState({ key: '', count: 0 });
    const [expandedKey, setExpandedKey] = useState('');
    const [limits, setLimits] = useState({ key: '', files: 6, history: 4 });
    const detailsId = useId();
    const file = target?.filePath, qualifiedName = target?.qualifiedName, id = target?.id;
    const selectionKey = JSON.stringify([project, file, qualifiedName, id, expectedGeneration]);
    const retryCount = retry.key === selectionKey ? retry.count : 0;
    const requestKey = JSON.stringify([selectionKey, retryCount]);
    // Bind every state (including errors) before effects run; a newly selected
    // file or symbol must never inherit the previous selection's evidence.
    const current = reading?.key === requestKey ? reading : undefined;
    const waiting = !current || ['loading', 'pending', 'busy'].includes(current.status);
    /*
     * Review of K42: Refresh folded the details into "Reading dependencies…"
     * and then showed the same numbers, with nothing left to say that it had
     * run. A refresh of the same selection keeps the last evidence on screen,
     * the button says that it runs, and the status beside it says when and
     * whether anything changed. Another selection never shows it.
     */
    const kept = waiting && lastReady?.key === selectionKey ? lastReady.data : undefined;
    const data = current?.status === 'ready' ? current.data : kept;
    const aggregate = useMemo(() => data ? aggregateFileImpact(data) : undefined, [data]);
    const expanded = expandedKey === selectionKey;
    const fileLimit = limits.key === selectionKey ? limits.files : 6;
    const historyLimit = limits.key === selectionKey ? limits.history : 4;
    const retryNow = (): void => setRetry(value => ({ key: selectionKey,
        count: value.key === selectionKey ? value.count + 1 : 1 }));

    useEffect(() => {
        if (!project || !file) return;
        const controller = new AbortController();
        let timer: ReturnType<typeof setTimeout> | undefined;
        let attempts = 0;
        const read = async (): Promise<void> => {
            setReading({ key: requestKey, status: 'loading' });
            try {
                const reply = await load(project, { filePath: file, qualifiedName, id }, controller.signal,
                    retryCount > 0 && attempts === 0);
                if (controller.signal.aborted) return;
                attempts++;
                if (reply.status === 'ready') {
                    if (reply.project !== project || reply.file_path !== file)
                        throw new Error('The response belongs to another selection.');
                    setReading({ key: requestKey, status: 'ready', data: reply });
                    setLastReady({ key: selectionKey, data: reply });
                } else if (reply.status === 'failed') {
                    setReading({ key: requestKey, status: 'failed', error: reply.error });
                } else if (attempts < 80) {
                    setReading({ key: requestKey, status: reply.status });
                    timer = setTimeout(() => { void read(); }, 500);
                } else setReading({ key: requestKey, status: 'failed', error: 'Still waiting for the local analysis.' });
            } catch (cause) {
                if (!controller.signal.aborted) setReading({ key: requestKey, status: 'failed',
                    error: cause instanceof Error ? cause.message : String(cause) });
            }
        };
        void read();
        return () => { controller.abort(); if (timer !== undefined) clearTimeout(timer); };
    }, [project, file, qualifiedName, id, load, requestKey, retryCount, selectionKey]);
    // What counts as a change: the evidence shown, not the time stamp of its computation.
    const refresh = useRefreshFeedback({ key: requestKey, settled: !waiting, error: current?.status === 'failed' ? '' : undefined,
        value: data && aggregate ? { generation: data.snapshot.generation, revision: data.snapshot.index_revision, direct: data.structural.direct,
            files: aggregate.files.map(row => [row.filePath, row.distance, row.symbols, row.testCandidate]), cochanges: aggregate.cochanges.map(row => [row.file_path, row.shared_commits]),
            commits: data.history.selection_commits } : undefined });

    const notes: string[] = [];
    if (data && aggregate) {
        if (expectedGeneration && expectedGeneration !== data.snapshot.generation) notes.push('Index snapshot differs');
        else if (data.snapshot.index_revision === 'unknown') notes.push('Index freshness unverified');
        if (aggregate.filesIncomplete && data.structural.available) notes.push('Graph sample limited');
        if (data.snapshot.coverage.some(row => row.count > 0)) notes.push('Index gaps');
        else if (data.snapshot.coverage_recording !== 'complete') notes.push('Coverage incomplete');
        if (data.history.available && (data.history.truncated || data.history.shallow || data.history.cochanges_omitted > 0)) notes.push('Git sample limited');
        if (data.history.selection_uncommitted) notes.push('File has uncommitted changes');
        if (data.scope === 'file' && (qualifiedName || id !== undefined)) notes.push('File-level evidence');
    }
    const scope = qualifiedName || id !== undefined ? target?.name ?? qualifiedName ?? 'Selected symbol' : 'File';
    return <section className="file-impact-summary" aria-label="File impact summary"
        data-status={file ? current?.status ?? 'loading' : 'idle'}>
        <div className="file-impact-summary-row">
            <span className="file-impact-summary-label" title={file}>{scope} impact</span>
            {!file && <span className="file-impact-summary-muted">Select a file to see its dependents.</span>}
            {file && waiting && !kept
                && <span role="status" className="file-impact-summary-muted">{current?.status === 'busy' ? 'Waiting for local analysis…' : 'Reading dependencies…'}</span>}
            {current?.status === 'failed' && <><span role="alert" className="file-impact-summary-warning"
                title={current.error}>Impact unavailable</span><button type="button" onClick={retryNow}>Retry</button></>}
            {data && aggregate && <>
                <div className="file-impact-summary-metrics">
                    {data.structural.available ? <>
                        <span title="Indexed caller and importer nodes; not a runtime count.">{countLabel(data.structural.direct, 'direct dependent', data.structural.truncated)}</span>
                        <span>{countLabel(aggregate.files.length, 'affected file', aggregate.filesIncomplete)}</span>
                        <span title="Distinct returned files with a test-candidate dependency. File-path classification does not prove test coverage.">{countLabel(aggregate.testFiles, 'test file', aggregate.filesIncomplete)}</span>
                    </> : <span className="file-impact-summary-warning" title={data.structural.error}>Graph unavailable</span>}
                    {data.history.available ? <span title={`Local Git sample: ${data.history.commits_scanned} commits, up to ${data.history.window_days} days. Historical association is not a dependency.`}>
                        Git: {countLabel(data.history.selection_commits, 'sampled commit')} · {countLabel(aggregate.cochanges.length, 'co-changed file', data.history.cochanges_omitted > 0)}
                    </span> : <span className="file-impact-summary-muted" title={data.history.error}>Git unavailable</span>}
                </div>
                <button type="button" className="file-impact-summary-toggle" aria-expanded={expanded} aria-controls={detailsId}
                    onClick={() => setExpandedKey(expanded ? '' : selectionKey)}>{expanded ? 'Less' : 'Details'} <span aria-hidden="true">{expanded ? '▴' : '▾'}</span></button>
            </>}
        </div>
        {notes.length > 0 && <p className="file-impact-summary-note">{notes.join(' · ')}</p>}
        {data?.structural.available && data.structural.reachable === 0
            && <p className="file-impact-summary-note">No dependents found in this index; this does not establish safety.</p>}
        {expanded && data && aggregate && <div className="file-impact-summary-details" id={detailsId}>
            <div className="file-impact-summary-detail-grid">
                <section aria-label="Affected files"><h4>Affected files</h4>
                    {!data.structural.available ? <p>{data.structural.error ?? 'No usable graph evidence for this selection.'}</p>
                        : aggregate.files.length === 0 ? <p>No file paths returned by this graph walk.</p>
                            : <ul>{aggregate.files.slice(0, fileLimit).map(row => <li key={row.filePath}>
                                <button type="button" onClick={() => onOpen(row.target)} title={row.filePath}>{row.filePath}</button>
                                <span>{row.distance === 1 ? 'direct' : `${row.distance} hops`} · {countLabel(row.symbols, 'symbol')}{row.testCandidate ? ' · test candidate' : ''}</span>
                            </li>)}</ul>}
                    {aggregate.files.length > fileLimit && <button type="button" onClick={() => setLimits({ key: selectionKey, files: fileLimit + 6, history: historyLimit })}>More files ({aggregate.files.length - fileLimit})</button>}
                </section>
                <section aria-label="Files changed together"><h4>Changed together <span>· Git sample</span></h4>
                    {!data.history.available ? <p>{data.history.error ?? 'Local Git history is unavailable.'}</p>
                        : aggregate.cochanges.length === 0 ? <p>No co-changed files in this sample.</p>
                            : <ul>{aggregate.cochanges.slice(0, historyLimit).map(row => <li key={row.file_path}>
                                <button type="button" onClick={() => onOpen({ filePath: row.file_path })} title={row.file_path}>{row.file_path}</button>
                                <span>{countLabel(row.shared_commits, 'shared commit')}</span>
                            </li>)}</ul>}
                    {aggregate.cochanges.length > historyLimit && <button type="button" onClick={() => setLimits({ key: selectionKey, files: fileLimit, history: historyLimit + 4 })}>More co-changed files ({aggregate.cochanges.length - historyLimit})</button>}
                </section>
            </div>
            <footer><span>Indexed calls/imports · up to {data.structural.max_depth} hops. Test files are candidates; Git co-changes are associations.</span>
                <RefreshControl labels={FILE_IMPACT_REFRESH} feedback={refresh.feedback} onRefresh={() => { refresh.begin(); retryNow(); }} /></footer>
        </div>}
    </section>;
}
