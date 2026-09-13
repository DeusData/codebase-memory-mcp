import { useEffect, useId, useState } from 'react';
import type { JSX } from 'react';
import { evidenceTarget, fetchSelectionImpact, localCommitCommand, selectionRisk } from './selection-impact';
import type { SelectionImpact, SelectionImpactTarget } from './selection-impact';
import LocalEvidenceCommand from './LocalEvidenceCommand';
import './selection-impact.css';

export interface SelectionImpactWidgetProps {
    project: string;
    target?: SelectionImpactTarget;
    onOpen: (target: SelectionImpactTarget) => void;
    onDiagnose?: () => void;
    /** Shared repository-map snapshot, when available. */
    expectedGeneration?: string;
    /** The shared workspace already displays file and symbol controls. */
    compact?: boolean;
    /** Test seam; production always reads the same daemon origin. */
    load?: typeof fetchSelectionImpact;
}

function stamp(value: number): string {
    return new Date(value * 1000).toLocaleString();
}

export default function SelectionImpactWidget({ project, target, onOpen, onDiagnose,
    expectedGeneration, compact = false, load = fetchSelectionImpact }: SelectionImpactWidgetProps): JSX.Element {
    const [reading, setReading] = useState<{ key: string; data: SelectionImpact }>();
    const [error, setError] = useState('');
    const [state, setState] = useState('idle');
    const [refresh, setRefresh] = useState(0);
    const [findingLimit, setFindingLimit] = useState(8);
    const [historyLimit, setHistoryLimit] = useState(5);
    const [tab, setTab] = useState('dependencies');
    const panelId = useId();
    const file = target?.filePath, qn = target?.qualifiedName, id = target?.id;
    const selectionKey = JSON.stringify([project, file, qn, id, expectedGeneration]);
    // A prop change can paint before its effect runs: never label an older
    // result with the new selection, including two symbols in the same file.
    const data = reading?.key === selectionKey ? reading.data : undefined;
    useEffect(() => {
        setReading(undefined); setError(''); setState('idle');
        setFindingLimit(8); setHistoryLimit(5); setTab('dependencies');
        if (!project || !file) return;
        const controller = new AbortController();
        let timer: ReturnType<typeof setTimeout> | undefined;
        let attempts = 0;
        const selection = { filePath: file, qualifiedName: qn, id };
        const read = async (): Promise<void> => {
            try {
                setState('loading');
                const reply = await load(project, selection, controller.signal, refresh > 0 && attempts === 0);
                if (controller.signal.aborted) return;
                attempts++;
                if (reply.status === 'ready') { setReading({ key: selectionKey, data: reply }); setState('ready'); }
                else if (reply.status === 'failed') { setError(reply.error); setState('failed'); }
                else if (attempts < 80) {
                    setState(reply.status);
                    timer = setTimeout(() => { void read(); }, 500);
                } else { setError('Analysis is taking longer than expected. Refresh to retry.'); setState('failed'); }
            } catch (cause) {
                if (!controller.signal.aborted) {
                    setError(cause instanceof Error ? cause.message : String(cause)); setState('failed');
                }
            }
        };
        void read();
        return () => { controller.abort(); if (timer !== undefined) clearTimeout(timer); };
    }, [project, file, qn, id, refresh, load, expectedGeneration, selectionKey]);
    const risk = data ? selectionRisk(data) : undefined;
    return <section className="selection-impact" data-testid="selection-impact" data-status={state}
        aria-label="Selection impact analysis">
        {!compact && <header><div><h3>Selection evidence</h3></div>
            {file && <button type="button" onClick={() => setRefresh(value => value + 1)}>Refresh analysis</button>}
        </header>}
        {compact && file && !data && <div><button type="button" onClick={() => setRefresh(value => value + 1)}>Refresh analysis</button></div>}
        {!file && <p>Select an indexed file or symbol to inspect its dependency paths and local Git evidence.</p>}
        {file && !compact && <p className="selection-impact-scope"><strong>{target?.name ?? file}</strong><br />{file}
            {(qn || id !== undefined) ? ' · selected symbol' : ' · whole file'}</p>}
        {(state === 'loading' || state === 'pending' || state === 'busy')
            && <p role="status">{state === 'busy' ? 'Waiting for the current local analysis…' : 'Reading the graph snapshot and bounded local Git history…'}</p>}
        {error && <p role="alert">Impact evidence unavailable: {error} Risk remains unresolved.</p>}
        {data && risk && <>
            {expectedGeneration && expectedGeneration !== data.snapshot.generation
                && <p role="alert">The repository map and this impact analysis use different index generations.
                    Refresh the map and analysis before comparing their findings.</p>}
            <div className="selection-impact-risk" data-level={risk.level}>
                <div className="selection-impact-risk-heading"><strong>{risk.label}</strong>
                    {compact ? <button type="button" onClick={() => setRefresh(value => value + 1)}>Refresh analysis</button>
                        : <span>Heuristic, no defect probability</span>}</div>
                {compact && <small>Transparent heuristic, no defect probability</small>}
                <ul>{risk.reasons.map(reason => <li key={reason}>{reason}</li>)}</ul>
                <button type="button" className="selection-impact-quality-link" onClick={() => setTab('quality')}>{risk.uncertainties.length} data limitations · inspect before relying on absence of findings</button>
            </div>
            <nav className="selection-impact-tabs" aria-label="Impact evidence" role="tablist">
                {([['dependencies', `Dependencies · ${data.structural.reachable}`],
                    ['tests', `Tests to review · ${data.structural.test_candidates}`],
                    ['history', `Git history · ${data.history.cochanges.length}`],
                    ['quality', `Data quality · ${risk.uncertainties.length}`]] as const).map(([value, label]) =>
                    <button type="button" role="tab" key={value} id={`${panelId}-${value}-tab`}
                        aria-controls={`${panelId}-${value}`} aria-selected={tab === value} onClick={() => setTab(value)}>{label}</button>)}
            </nav>
            <div className="selection-impact-panels">
            <section className="selection-impact-quality" role="tabpanel" id={`${panelId}-quality`}
                aria-labelledby={`${panelId}-quality-tab`} hidden={tab !== 'quality'}>
                <h4>Data quality and revision · {risk.uncertainties.length} limitations</h4>
                <p>Computed {stamp(data.computed_at)} · cached for up to {data.cache_seconds}s.<br />
                    Index: {data.snapshot.indexed_at || 'unknown timestamp'} · generation <code>{data.snapshot.generation || 'unknown'}</code><br />
                    Git HEAD: <code>{data.history.head ?? 'unavailable'}</code><br />
                    Scope: {data.scope === 'symbol' ? 'selected symbol; Git evidence is file-level' : 'selected file, bounded indexed declarations'}.<br />
                    Graph scope: {data.structural.seed_count} starting nodes · at most {data.structural.visit_cap} visited nodes · {data.structural.result_cap} returned findings.</p>
                <ul>{risk.uncertainties.map(reason => <li key={reason}>{reason}</li>)}</ul>
                {onDiagnose && <button type="button" onClick={onDiagnose}>Inspect local indexing diagnostics</button>}
            </section>
            <section role="tabpanel" id={`${panelId}-dependencies`} aria-labelledby={`${panelId}-dependencies-tab`} hidden={tab !== 'dependencies'}><h4>Structural impact · indexed dependencies</h4>
                <p>{data.structural.direct} direct · {data.structural.reachable} total dependents within {data.structural.max_depth} hops.
                    {' '}CALLS means a static call relation; IMPORTS means an import dependency. Neither proves runtime execution or data flow.</p>
                {data.structural.findings.length === 0 && <p>No dependency path was established for this selection in the current bounded index. This is not evidence of safety.</p>}
                <p>Showing {Math.min(findingLimit, data.structural.findings.length)} of {data.structural.findings.length} returned evidence paths.</p>
                <ol className="selection-impact-findings">{data.structural.findings.slice(0, findingLimit).map(finding => <li key={finding.id}>
                    <button type="button" onClick={() => onOpen(evidenceTarget(finding))}>{finding.name}</button>
                    <span>{finding.distance === 1 ? 'direct dependency' : `${finding.distance} hops`}{finding.test_candidate ? ' · test candidate (path heuristic)' : ''}</span>
                    <small>{finding.file_path}:{finding.line}</small>
                    <details><summary>Show indexed path ({finding.path.length} edge{finding.path.length === 1 ? '' : 's'})</summary>
                        <ol>{finding.path.map(edge => <li key={edge.edge_id}>
                            <button type="button" onClick={() => onOpen(evidenceTarget(edge.from))}>{edge.from.name}</button>
                            {' '}<code>{edge.type}</code>{' '}
                            <button type="button" onClick={() => onOpen(evidenceTarget(edge.to))}>{edge.to.name}</button>
                            <small>Graph edge #{edge.edge_id} · source {edge.from.file_path}:{edge.from.line}</small>
                        </li>)}</ol>
                    </details>
                </li>)}</ol>
                {findingLimit < data.structural.findings.length && <button type="button"
                    onClick={() => setFindingLimit(value => value + 8)}>
                    Show more affected symbols ({findingLimit} of {data.structural.findings.length})
                </button>}
            </section>
            <section role="tabpanel" id={`${panelId}-history`} aria-labelledby={`${panelId}-history-tab`} hidden={tab !== 'history'}><h4>Historical evidence · files changed together</h4>
                <p>{data.history.selection_commits} ordinary commits touched this file in a sample of {data.history.commits_scanned} commits,
                    up to {data.history.window_days} days. Excluded: {data.history.merges_excluded} merges and {data.history.mass_changes_excluded} changes
                    touching more than {data.history.mass_change_threshold} files. Rename history is not followed.</p>
                <p>Co-changes are historical association, not functional dependency or evidence of a later defect.</p>
                {data.history.cochanges.length === 0 && <p>No co-change evidence is available in this sample.</p>}
                {data.history.cochanges.length > 0 && <p>Showing {Math.min(historyLimit, data.history.cochanges.length)} of {data.history.cochanges.length} returned files.
                    {data.history.cochanges_omitted > 0 && ` ${data.history.cochanges_omitted} further co-change files were omitted by the server limit.`}</p>}
                <ul className="selection-impact-history">{data.history.cochanges.slice(0, historyLimit).map(row => <li key={row.file_path}>
                    <button type="button" onClick={() => onOpen({ filePath: row.file_path, name: row.file_path })}>{row.file_path}</button>
                    {' '}· {row.shared_commits} shared commit{row.shared_commits === 1 ? '' : 's'}
                    <details><summary>Inspect local Git evidence</summary>
                        {row.commit_refs.map(hash => {
                            const commit = data.history.commits.find(commit => commit.hash === hash);
                            return <div className="selection-impact-commit" key={hash}><p><code>{hash}</code>{commit && <> · {stamp(commit.time)}<br />{commit.subject} · {commit.files} changed files</>}</p>
                                <LocalEvidenceCommand command={localCommitCommand(hash, data.file_path, row.file_path)} label="Copy local Git evidence command" /></div>;
                        })}
                    </details>
                </li>)}</ul>
                {historyLimit < data.history.cochanges.length && <button type="button"
                    onClick={() => setHistoryLimit(value => value + 5)}>
                    Show more historical associations ({historyLimit} of {data.history.cochanges.length})
                </button>}
            </section>
            <section role="tabpanel" id={`${panelId}-tests`} aria-labelledby={`${panelId}-tests-tab`} hidden={tab !== 'tests'}>
                <h4>Tests to review</h4>
                <p>{data.structural.test_candidates} test candidates reach the selection in the bounded graph walk. File-path classification is heuristic; a test relationship does not prove assertion coverage.</p>
                {data.structural.test_candidates === 0 && <p>No test dependency was established. Search the test directories and add a focused regression check; missing evidence is not a passing test.</p>}
                <ul className="selection-impact-tests">{data.structural.findings.filter(finding => finding.test_candidate).map(finding => <li key={finding.id}>
                    <button type="button" onClick={() => onOpen(evidenceTarget(finding))}>Open test source: {finding.name}</button>
                    <small>{finding.file_path}:{finding.line} · {finding.distance} indexed hop{finding.distance === 1 ? '' : 's'}</small>
                    <details><summary>Inspect test dependency path</summary><ol>{finding.path.map(edge => <li key={edge.edge_id}>
                        <button type="button" onClick={() => onOpen(evidenceTarget(edge.from))}>{edge.from.name}</button>{' '}
                        <code>{edge.type}</code>{' '}<button type="button" onClick={() => onOpen(evidenceTarget(edge.to))}>{edge.to.name}</button>
                        <small>Graph edge #{edge.edge_id} · source {edge.from.file_path}:{edge.from.line}</small>
                    </li>)}</ol></details>
                </li>)}</ul>
                {data.structural.test_candidates > data.structural.findings.filter(finding => finding.test_candidate).length
                    && <p role="alert">Some test candidates are outside the returned evidence limit. Refine the selection to inspect their paths.</p>}
                <h4>Next checks</h4><ol>{risk.next.map(next => <li key={next}>{next}</li>)}</ol>
            </section>
            </div>
        </>}
    </section>;
}
