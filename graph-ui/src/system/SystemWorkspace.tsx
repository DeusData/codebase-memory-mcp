import { useCallback, useEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react';
import type { AtlasApi } from '../app/atlas-api';
import { cpuText, filterLogs, logLevel, memoryLabel, memoryText, type LogLevel } from './system-model';
import { useSystemPoll, type SystemReading } from './useSystemPoll';
import './system.css';

export interface SystemWorkspaceProps {
    api: Pick<AtlasApi, 'processes' | 'logs' | 'indexJobs'>;
    onOpenProjects: () => void;
    active?: boolean;
    version?: string;
    pollMs?: number;
}

type SystemTab = 'overview' | 'indexes' | 'logs';
const TABS: SystemTab[] = ['overview', 'indexes', 'logs'];

function ReadStatus({ reading, paused }: { reading: SystemReading<unknown>; paused: boolean }) {
    return <div className="system-read-status">
        <span className={reading.error ? 'system-warning' : ''}>
            {reading.error ? reading.data ? 'Update failed · showing last reading' : 'Update failed' : paused ? 'Live updates paused' : reading.loading ? 'Updating…' : 'Live updates on'}
        </span>
        <span>{reading.updatedAt === null ? 'No reading yet' : <>Updated <time dateTime={new Date(reading.updatedAt).toISOString()}>{new Date(reading.updatedAt).toLocaleTimeString()}</time></>}</span>
        {reading.error && <p role="alert">{reading.error}</p>}
    </div>;
}

/** Read-only daemon dashboard; project mutations stay in the existing dialog. */
export default function SystemWorkspace({ api, onOpenProjects, active = true, version, pollMs = 3000 }: SystemWorkspaceProps) {
    const [tab, setTab] = useState<SystemTab>('overview');
    const [paused, setPaused] = useState(false);
    const [query, setQuery] = useState('');
    const [level, setLevel] = useState<LogLevel | 'all'>('all');
    const [copyStatus, setCopyStatus] = useState('');
    const [followTail, setFollowTail] = useState(true);
    const logRef = useRef<HTMLDivElement>(null);
    const readProcesses = useCallback(() => api.processes(), [api]);
    const readLogs = useCallback(() => api.logs(200), [api]);
    const readJobs = useCallback(() => api.indexJobs(), [api]);
    const processes = useSystemPoll(readProcesses, active, paused, pollMs);
    const logs = useSystemPoll(readLogs, active && tab === 'logs', paused, pollMs);
    const jobs = useSystemPoll(readJobs, active && tab === 'indexes', paused, pollMs);
    const report = processes.data;
    const self = report?.processes.find((process) => process.isSelf);
    const visibleLines = useMemo(() => filterLogs(logs.data?.lines ?? [], query, level), [logs.data, query, level]);
    const selectedReading = tab === 'logs' ? logs : tab === 'indexes' ? jobs : processes;

    useEffect(() => {
        if (followTail && logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
    }, [visibleLines, followTail, tab]);
    useEffect(() => { setCopyStatus(''); }, [query, level, logs.data]);

    const copyLogs = async () => {
        try {
            if (!navigator.clipboard?.writeText) throw new Error('Clipboard access is unavailable. Select and copy the visible text.');
            await navigator.clipboard.writeText(visibleLines.join('\n'));
            setCopyStatus('Visible lines copied');
        } catch (error) {
            setCopyStatus(error instanceof Error ? error.message : 'Could not copy the log.');
        }
    };
    const moveTab = (event: KeyboardEvent<HTMLButtonElement>, current: SystemTab) => {
        const index = TABS.indexOf(current);
        const target = event.key === 'ArrowRight' ? TABS[(index + 1) % TABS.length]
            : event.key === 'ArrowLeft' ? TABS[(index + TABS.length - 1) % TABS.length]
            : event.key === 'Home' ? TABS[0] : event.key === 'End' ? TABS[TABS.length - 1] : undefined;
        if (!target) return;
        event.preventDefault();
        setTab(target);
        document.getElementById(`system-tab-${target}`)?.focus();
    };

    return <section className="system-workspace" aria-label="System" hidden={!active} data-testid="system-workspace">
        <header className="system-heading">
            <div><p className="system-eyebrow">DAEMON</p><h1>System</h1><p>Processes, indexes, and the serving daemon log.</p></div>
            <div className="system-heading-actions">
                {version && <span className="system-version">{version}</span>}
                <button type="button" onClick={() => selectedReading.refresh()} disabled={selectedReading.loading}>Refresh</button>
                <button type="button" aria-label={paused ? 'Resume live updates' : 'Pause live updates'} aria-pressed={paused} onClick={() => setPaused(!paused)}>{paused ? 'Resume live' : 'Pause live'}</button>
            </div>
        </header>
        <div className="system-tabs" role="tablist" aria-label="System views">
            {TABS.map((name) => <button key={name} id={`system-tab-${name}`} type="button" role="tab" aria-selected={tab === name} aria-controls={`system-panel-${name}`} tabIndex={tab === name ? 0 : -1} onKeyDown={(event) => moveTab(event, name)} onClick={() => setTab(name)}>{name === 'overview' ? 'Overview' : name === 'indexes' ? 'Indexes' : 'Logs'}</button>)}
        </div>
        <ReadStatus reading={selectedReading} paused={paused} />

        <div id="system-panel-overview" role="tabpanel" aria-labelledby="system-tab-overview" hidden={tab !== 'overview'}>
            <div className="system-metrics">
                <article><span>Serving process</span><strong>{report && report.selfPid > 0 ? `PID ${report.selfPid}` : 'Unavailable'}</strong><small>{self?.elapsed ? `Running ${self.elapsed}` : 'Uptime unavailable'}</small></article>
                <article><span>{memoryLabel(report?.telemetry?.selfMemoryKind)}</span><strong>{memoryText(report?.telemetry?.selfMemoryMb)}</strong><small>Serving daemon</small></article>
                <article><span>{report?.telemetry?.cpuUnit === 'seconds' ? 'CPU time' : 'CPU'}</span><strong>{cpuText(self?.telemetry?.cpu, report?.telemetry?.cpuUnit)}</strong><small>{report?.telemetry?.cpuUnit === 'seconds' ? 'Cumulative process time' : report?.telemetry?.cpuUnit === 'percent' ? 'Reported by the host process monitor' : 'Server did not specify a unit'}</small></article>
                <article><span>Processes listed</span><strong>{report ? report.processes.length : 'Unavailable'}</strong><small>Visible to the serving daemon</small></article>
            </div>
            <section className="system-section" aria-label="Daemon processes">
                <div className="system-section-heading"><h2>Processes</h2><span>{memoryLabel(report?.telemetry?.memoryKind)} · MiB</span></div>
                <div className="system-table-wrap"><table><thead><tr><th>Process</th><th>CPU</th><th>Memory</th><th>Uptime</th><th>Command</th></tr></thead><tbody>
                    {report?.processes.map((process, index) => <tr key={`${process.pid}-${index}`}>
                        <th scope="row">{process.pid > 0 ? process.pid : 'Unavailable'} {process.isSelf && <span className="system-tag">Serving</span>}</th>
                        <td>{cpuText(process.telemetry?.cpu, report.telemetry?.cpuUnit)}</td><td>{memoryText(process.telemetry?.memoryMb)}</td><td>{process.elapsed || 'Unavailable'}</td><td><code>{process.command || 'Unavailable'}</code></td>
                    </tr>)}
                </tbody></table></div>
                {!report && <p className="system-empty">{processes.loading ? 'Reading processes…' : 'No process reading available.'}</p>}
                {report?.processes.length === 0 && <p className="system-empty">The server returned no process entries.</p>}
            </section>
        </div>

        <div id="system-panel-indexes" role="tabpanel" aria-labelledby="system-tab-indexes" hidden={tab !== 'indexes'}>
            <section className="system-section">
                <div className="system-section-heading"><div><h2>Indexes</h2><p>Open, add, reindex, or inspect your projects.</p></div><button type="button" onClick={onOpenProjects}>Manage indexes</button></div>
                <h3>Index activity</h3><p className="system-muted">Current daemon job slots. Slots may be reused; this is not a complete history.</p>
                <div className="system-table-wrap"><table><thead><tr><th>Repository</th><th>State</th><th>Details</th></tr></thead><tbody>
                    {jobs.data?.map((job, index) => <tr key={`${job.slot}-${index}`}><th scope="row"><code>{job.path || 'Path unavailable'}</code></th><td><span className={`system-job-state system-job-${job.status}`}>{job.status === 'indexing' ? 'Indexing' : job.status === 'done' ? 'Complete' : job.status === 'error' ? 'Failed' : 'Unknown'}</span></td><td>{job.error || 'No details reported'}</td></tr>)}
                </tbody></table></div>
                {jobs.data?.length === 0 && <p className="system-empty">No index activity reported by this daemon.</p>}
                {!jobs.data && <p className="system-empty">{jobs.loading ? 'Reading index activity…' : 'No index reading available.'}</p>}
            </section>
        </div>

        <div id="system-panel-logs" role="tabpanel" aria-labelledby="system-tab-logs" hidden={tab !== 'logs'}>
            <section className="system-section">
                <div className="system-section-heading"><div><h2>Daemon log</h2><p>{report && report.selfPid > 0 ? `Serving process ${report.selfPid}` : 'Serving daemon'} · Latest 200 lines</p></div><button type="button" disabled={visibleLines.length === 0} onClick={() => { void copyLogs(); }}>Copy visible</button></div>
                <div className="system-log-controls">
                    <label><span>Filter</span><input type="search" value={query} onChange={(event) => setQuery(event.currentTarget.value)} placeholder="Find in this tail…" /></label>
                    <label><span>Level</span><select value={level} onChange={(event) => setLevel(event.currentTarget.value as LogLevel | 'all')}>{(['all', 'error', 'warn', 'info', 'debug', 'trace', 'other'] as const).map((value) => <option value={value} key={value}>{value === 'all' ? 'All levels' : value === 'other' ? 'Unclassified' : value.toUpperCase()}</option>)}</select></label>
                    <label className="system-tail-toggle"><input type="checkbox" checked={followTail} onChange={(event) => setFollowTail(event.currentTarget.checked)} /> Follow tail</label>
                </div>
                <div className="system-log-meta"><span>{visibleLines.length} shown · {logs.data?.total ?? 'Unknown'} lines reported</span><span role="status">{copyStatus}</span></div>
                <div className="system-log" aria-label="Daemon log lines" tabIndex={0} ref={logRef} onScroll={() => {
                    const element = logRef.current;
                    if (element && element.scrollHeight - element.scrollTop - element.clientHeight > 32) setFollowTail(false);
                }}>
                    {visibleLines.map((line, index) => <div className={`system-log-line system-log-${logLevel(line)}`} key={index}>{line || '\u00a0'}</div>)}
                    {visibleLines.length === 0 && <p>{logs.loading ? 'Reading log…' : !logs.data ? 'No log reading available.' : logs.data.lines.length === 0 ? 'The daemon returned an empty log tail.' : 'No lines match these filters.'}</p>}
                </div>
            </section>
        </div>
    </section>;
}
