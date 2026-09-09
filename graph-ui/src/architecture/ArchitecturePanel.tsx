import { useEffect, useId, useMemo, useState } from 'react';
import type { JSX, ReactNode } from 'react';
import type { ArchitectureBoundary, ArchitectureHotspot, ArchitectureOverviewDto } from '../core/intelligence-provider';
import {
    ARCHITECTURE_VIEWS, boundaryMap, DEFAULT_ARCHITECTURE_CONFIG,
    matchingArchitecture, readArchitectureConfig, saveArchitectureConfig,
} from './architecture-model';
import type { ArchitectureView, ConfigStorage } from './architecture-model';
import { architectureText as text } from './strings';
import './architecture.css';
import RepositoryMapView, { RelationshipEvidence } from './RepositoryMap';
import type { RepositoryMapProps } from './RepositoryMap';
import { repositoryMap } from './repository-map';

export interface ArchitecturePanelProps extends Pick<RepositoryMapProps, 'graph' | 'selection' | 'selectionPanel' | 'onSelect' | 'graphNote' | 'readSource'> {
    projectName: string;
    graphGeneration?: string;
    overview?: ArchitectureOverviewDto;
    loading?: boolean;
    error?: string;
    onRefresh?: () => void;
    onProjectWalk?: () => void;
    /** The declaration line is 1-based, as returned by the provider. */
    onNavigate: (filePath: string, line?: number, name?: string) => void;
}

function browserStorage(): ConfigStorage | undefined {
    try { return window.localStorage; } catch { return undefined; }
}

function Empty({ children }: { children?: ReactNode }): JSX.Element {
    return <div className="atlas-arch-empty">{children ?? text.noData}</div>;
}

function SectionHeading({ title, note }: { title: string; note?: string }): JSX.Element {
    return <div className="atlas-arch-section-heading"><div><h2>{title}</h2>{note && <p>{note}</p>}</div></div>;
}

/** Pagination bounds DOM work while keeping every returned finding reachable. */
function Collection<T>({ items, children, empty, pageSize = 12 }: {
    items: T[]; children: (visible: T[]) => ReactNode; empty: string; pageSize?: number;
}): JSX.Element {
    const [limit, setLimit] = useState(pageSize);
    if (items.length === 0) return <Empty>{empty}</Empty>;
    return <>
        {children(items.slice(0, limit))}
        {items.length > pageSize && <div className="atlas-arch-pager">
            <span>{text.results(Math.min(limit, items.length), items.length)}</span>
            {limit < items.length && <button className="atlas-arch-action" onClick={() => setLimit(limit + pageSize)}>{text.showMore}</button>}
        </div>}
    </>;
}

function SourceLink({ path, line, name, onNavigate }: {
    path?: string; line?: number; name?: string; onNavigate: ArchitecturePanelProps['onNavigate'];
}): JSX.Element {
    if (!path) return <span className="atlas-arch-muted" title={text.sourceMissing}>{text.unknown}</span>;
    return <button className="atlas-arch-source" aria-label={text.openSource(name ?? path)}
        onClick={() => onNavigate(path, line, name)}>{path}{line !== undefined ? `:${line}` : ''}</button>;
}

function BoundaryDiagram({ boundaries, onGroup }: {
    boundaries: ArchitectureBoundary[]; onGroup: (group: string) => void;
}): JSX.Element | null {
    const markerId = useId().replace(/:/g, '');
    const map = useMemo(() => boundaryMap(boundaries), [boundaries]);
    if (map.groups.length === 0) return null;
    const positions = new Map(map.groups.map((group, index) => {
        const angle = -Math.PI / 2 + index * Math.PI * 2 / map.groups.length;
        return [group, { x: 330 + Math.cos(angle) * 215, y: 153 + Math.sin(angle) * 104 }];
    }));
    const maxCalls = Math.max(1, ...map.boundaries.map(boundary => boundary.callCount));
    return <div className="atlas-arch-map">
        <svg viewBox="0 0 660 320" role="group" aria-label={text.mapLabel}>
            <defs><marker id={markerId} markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto">
                <polygon points="0 0, 7 3.5, 0 7" />
            </marker></defs>
            {map.boundaries.map((boundary, index) => {
                const from = positions.get(boundary.from)!;
                const to = positions.get(boundary.to)!;
                const distance = Math.hypot(to.x - from.x, to.y - from.y);
                const dx = distance ? (to.x - from.x) / distance : 0;
                const dy = distance ? (to.y - from.y) / distance : 0;
                const start = { x: from.x + dx * 25, y: from.y + dy * 25 };
                const end = { x: to.x - dx * 30, y: to.y - dy * 30 };
                // Offset reciprocal edges so each arrow retains its direction.
                const control = { x: (start.x + end.x) / 2 - dy * 18, y: (start.y + end.y) / 2 + dx * 18 };
                const d = distance ? `M${start.x},${start.y} Q${control.x},${control.y} ${end.x},${end.y}`
                    : `M${from.x - 15},${from.y - 18} C${from.x - 50},${from.y - 55} ${from.x + 50},${from.y - 55} ${from.x + 18},${from.y - 20}`;
                return <path key={`${boundary.from}:${boundary.to}:${index}`} d={d}
                    markerEnd={`url(#${markerId})`} strokeWidth={1 + 2 * boundary.callCount / maxCalls}>
                    <title>{`${boundary.from} → ${boundary.to}: ${boundary.callCount} ${text.calls}`}</title>
                </path>;
            })}
            {map.groups.map(group => {
                const position = positions.get(group)!;
                const label = group.length > 25 ? `${group.slice(0, 22)}...` : group;
                return <g key={group} role="button" tabIndex={0} aria-label={text.focusGroup(group)}
                    onClick={() => onGroup(group)} onKeyDown={event => {
                        if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onGroup(group); }
                    }}>
                    <title>{group}</title><circle cx={position.x} cy={position.y} r="24" />
                    <text x={position.x} y={position.y + 4} data-map-count="true">{group.split(/[/.]/).filter(Boolean).at(-1)?.slice(0, 3) ?? group.slice(0, 3)}</text>
                    <text x={position.x} y={position.y + 41}>{label}</text>
                </g>;
            })}
        </svg>
        {map.omittedGroups > 0 && <p className="atlas-arch-map-note">{text.mapLimit(map.groups.length, map.omittedGroups)}</p>}
    </div>;
}

function Overview({ data, filtered, filter, onGroup, onNavigate }: {
    data: ArchitectureOverviewDto; filtered: ArchitectureOverviewDto; filter: string;
    onGroup: (group: string) => void; onNavigate: ArchitecturePanelProps['onNavigate'];
}): JSX.Element {
    const maxSymbols = Math.max(1, ...filtered.groups.map(group => group.symbolCount));
    const empty = filter.trim() ? text.noMatches : text.noData;
    return <>
        <div className="atlas-arch-metrics">
            {[[text.symbols, data.totalSymbols], [text.relations, data.totalRelations],
                [text.modules, data.groups.length], [text.files, data.files.length]].map(([label, count]) =>
                <div className="atlas-arch-metric" key={label}><strong>{Number(count).toLocaleString()}</strong><span>{label}</span></div>)}
        </div>
        <section>
            <SectionHeading title={text.modules} note={text.moduleNote} />
            <Collection key={`modules:${filter}`} items={filtered.groups} empty={empty}>
                {groups => <div className="atlas-arch-module-grid">{groups.map(group =>
                    <button key={group.name} className="atlas-arch-module" onClick={() => onGroup(group.name)} aria-label={text.focusGroup(group.name)}>
                        <strong>{group.name}</strong><span>{text.groupStats(group.symbolCount)}</span>
                        <div className="atlas-arch-bar" aria-hidden="true"><i style={{ width: `${group.symbolCount / maxSymbols * 100}%` }} /></div>
                    </button>)}</div>}
            </Collection>
        </section>
        <div className="atlas-arch-columns">
            <section><SectionHeading title={text.layers} note={text.layersNote} />
                <Collection key={`layers:${filter}`} items={filtered.layers} empty={empty} pageSize={8}>
                    {layers => <div className="atlas-arch-detail-list">{layers.map((layer, index) =>
                        <div key={`${layer.group}:${index}`} className="atlas-arch-detail"><div>
                            <button className="atlas-arch-link" onClick={() => onGroup(layer.group)}>{layer.group}</button>
                            {layer.reason && <p>{layer.reason}</p>}
                        </div><span className="atlas-arch-badge">{layer.layer}</span></div>)}</div>}
                </Collection>
            </section>
            <section><SectionHeading title={text.clusters} note={text.clustersNote} />
                <Collection key={`clusters:${filter}`} items={filtered.clusters} empty={empty} pageSize={8}>
                    {clusters => <div className="atlas-arch-detail-list">{clusters.map(cluster =>
                        <div key={cluster.id} className="atlas-arch-detail"><div>
                            <strong>{cluster.label || cluster.id}</strong><p>{cluster.topMembers.join(', ')}</p>
                            {cluster.cohesion !== undefined && <p>{text.cohesion(cluster.cohesion)}</p>}
                        </div><span className="atlas-arch-badge">{text.numberOfMembers(cluster.memberCount)}</span></div>)}</div>}
                </Collection>
            </section>
        </div>
        <section><SectionHeading title={text.filesTitle} />
            <Collection key={`files:${filter}`} items={filtered.files} empty={empty} pageSize={8}>
                {files => <div className="atlas-arch-detail-list atlas-arch-files">{files.map(file =>
                    <SourceLink key={file} path={file} onNavigate={onNavigate} />)}</div>}
            </Collection>
        </section>
        <div className="atlas-arch-columns">
            <section><SectionHeading title={text.languages} /><div className="atlas-arch-count-list">{data.languages.map(language =>
                <span className="atlas-arch-badge" key={language.language}>{language.language} · {language.fileCount.toLocaleString()}</span>)}</div></section>
            <section><SectionHeading title={text.kindsTitle} /><div className="atlas-arch-count-list">{data.symbolKinds.map(kind =>
                <span className="atlas-arch-badge" key={kind.kind}>{kind.kind} · {kind.count.toLocaleString()}</span>)}</div></section>
        </div>
    </>;
}

function Dependencies({ data, empty, onGroup, graph, onNavigate }: { data: ArchitectureOverviewDto; empty: string; onGroup: (group: string) => void } & Pick<ArchitecturePanelProps, 'graph' | 'onNavigate'>): JSX.Element {
    const boundaries = useMemo(() => [...data.boundaries].sort((a, b) => b.callCount - a.callCount), [data.boundaries]);
    const [chosen, setChosen] = useState<ArchitectureBoundary>();
    const edges = useMemo(() => graph && chosen ? repositoryMap(graph).evidence.filter(edge => edge.type === 'CALLS'
        && edge.source.package_name === chosen.from && edge.target.package_name === chosen.to) : [], [graph, chosen]);
    return <>
        <section><SectionHeading title={text.dependencyTitle} note={text.dependencyNote} />
            <BoundaryDiagram boundaries={boundaries} onGroup={onGroup} />
        </section>
        <section><SectionHeading title={text.tableTitle} />
            <Collection items={boundaries} empty={empty} pageSize={24}>{rows =>
                <div className="atlas-arch-table-wrap"><table className="atlas-arch-table"><thead><tr>
                    <th scope="col">{text.from}</th><th scope="col">{text.to}</th><th scope="col" className="atlas-arch-number">{text.calls}</th><th scope="col">{text.evidence}</th>
                </tr></thead><tbody>{rows.map((boundary, index) => <tr key={`${boundary.from}:${boundary.to}:${index}`}>
                    <td><button className="atlas-arch-link" onClick={() => onGroup(boundary.from)}>{boundary.from}</button></td>
                    <td><button className="atlas-arch-link" onClick={() => onGroup(boundary.to)}>{boundary.to}</button></td>
                    <td className="atlas-arch-number">{boundary.callCount.toLocaleString()}</td>
                    <td><button className="atlas-arch-link" onClick={() => setChosen(boundary)}>{text.inspectEdges}</button></td>
                </tr>)}</tbody></table></div>}
            </Collection>
        </section>
        {chosen && <section key={`${chosen.from}:${chosen.to}`}><SectionHeading title={`${chosen.from} → ${chosen.to}`} note={text.retainedEdges(edges.length, chosen.callCount)} />
            {edges.length ? <RelationshipEvidence edges={edges} onNavigate={onNavigate} /> : <Empty>{text.edgeEvidenceUnavailable}</Empty>}
        </section>}
    </>;
}

function HotspotSignals({ hotspot }: { hotspot: ArchitectureHotspot }): JSX.Element {
    const signals = [hotspot.allocationInLoop && text.allocationInLoop,
        hotspot.scanInLoop && text.scanInLoop, hotspot.unguardedRecursion && text.unguardedRecursion].filter(Boolean);
    return <div className="atlas-arch-signals">{signals.map(signal => <span key={String(signal)} className="atlas-arch-badge">{signal}</span>)}</div>;
}

function Findings({ view, data, empty, onNavigate }: {
    view: ArchitectureView; data: ArchitectureOverviewDto; empty: string; onNavigate: ArchitecturePanelProps['onNavigate'];
}): JSX.Element {
    if (view === 'entryPoints') return <section><SectionHeading title={text.views.entryPoints} note={text.entryNote} />
        <Collection items={data.entryPoints} empty={empty} pageSize={24}>{entries =>
            <div className="atlas-arch-table-wrap"><table className="atlas-arch-table"><thead><tr><th scope="col">{text.name}</th><th scope="col">{text.source}</th></tr></thead>
                <tbody>{entries.map((entry, index) => <tr key={`${entry.qualifiedName ?? entry.name}:${index}`}>
                    <td>{entry.name}</td><td><SourceLink path={entry.filePath} line={entry.line} name={entry.name} onNavigate={onNavigate} /></td>
                </tr>)}</tbody></table></div>}
        </Collection>
    </section>;
    if (view === 'routes') return <section><SectionHeading title={text.views.routes} note={text.routeNote} />
        <Collection items={data.routes} empty={empty} pageSize={24}>{routes =>
            <div className="atlas-arch-table-wrap"><table className="atlas-arch-table"><thead><tr>
                {[text.method, text.path, text.handler, text.evidence, text.source].map(label => <th scope="col" key={label}>{label}</th>)}
            </tr></thead><tbody>{routes.map((route, index) => <tr key={`${route.method}:${route.path}:${index}`}>
                <td><span className="atlas-arch-badge">{route.method ?? text.unknown}</span></td><td>{route.path}</td>
                <td>{route.handler ?? <span className="atlas-arch-muted">{text.unknown}</span>}</td>
                <td><span className="atlas-arch-badge" data-origin={route.origin}>{route.origin === 'source' ? text.sourceOrigin : text.indexOrigin}</span></td>
                <td><SourceLink path={route.filePath} line={route.line} name={route.handler ?? route.path} onNavigate={onNavigate} /></td>
            </tr>)}</tbody></table></div>}
        </Collection>
    </section>;
    return <section><SectionHeading title={text.views.hotspots} note={text.hotspotNote} />
        <Collection items={data.hotspots} empty={empty} pageSize={24}>{hotspots =>
            <div className="atlas-arch-table-wrap"><table className="atlas-arch-table"><thead><tr>
                {[text.name, text.fanIn, text.complexity, text.cognitive, text.loopDepth, text.signals, text.source].map(label => <th scope="col" key={label}>{label}</th>)}
            </tr></thead><tbody>{hotspots.map((hotspot, index) => <tr key={`${hotspot.qualifiedName ?? hotspot.name}:${index}`}>
                <td>{hotspot.name}</td>{[hotspot.fanIn, hotspot.complexity, hotspot.cognitive, hotspot.loopDepth].map((value, column) =>
                    <td key={column} className="atlas-arch-number">{value !== undefined ? value.toLocaleString() : <span className="atlas-arch-muted">{text.unknown}</span>}</td>)}
                <td><HotspotSignals hotspot={hotspot} /></td>
                <td><SourceLink path={hotspot.filePath} line={hotspot.line} name={hotspot.name} onNavigate={onNavigate} /></td>
            </tr>)}</tbody></table></div>}
        </Collection>
    </section>;
}

function ArchitectureWorkspace({ projectName, overview, loading = false, error, onRefresh, onProjectWalk, onNavigate, graph, selection, selectionPanel, onSelect, graphNote, graphGeneration, readSource }: ArchitecturePanelProps): JSX.Element {
    const storage = useMemo(browserStorage, []);
    const [config, setConfig] = useState(() => readArchitectureConfig(storage, projectName));
    const [saved, setSaved] = useState(true);
    useEffect(() => { setSaved(saveArchitectureConfig(storage, projectName, config)); }, [config, projectName, storage]);
    // A cached summary from another project must never appear here.
    const data = overview?.projectName && overview.projectName !== projectName ? undefined : overview;
    const filtered = useMemo(() => data ? matchingArchitecture(data, config.filter) : undefined, [data, config.filter]);
    const onGroup = (group: string) => setConfig({ view: 'dependencies', filter: group });
    const empty = config.filter.trim() ? text.noMatches : text.noData;
    const ready = Boolean(data && filtered && !loading && !error && projectName);
    return <section className="atlas-architecture" data-testid="atlas-architecture" aria-label={text.title} aria-busy={loading}>
        <header className="atlas-arch-heading"><div><span className="atlas-arch-project">{projectName}</span>
            <h1>{text.title}</h1><p>{text.subtitle}</p></div>
            <div className="repo-map-directions">{onProjectWalk && <button onClick={onProjectWalk} disabled={!projectName}>{text.importWalk}</button>}
                {onRefresh && <button className="atlas-arch-action" onClick={onRefresh} disabled={loading || !projectName}>{text.refresh}</button>}</div>
        </header>
        <nav className="atlas-arch-tabs" aria-label={text.navigation}>{ARCHITECTURE_VIEWS.map(view =>
            <button className="atlas-arch-tab" key={view} aria-pressed={config.view === view} data-view={view}
                onClick={() => setConfig(current => ({ ...current, view }))}>{text.views[view]}</button>)}</nav>
        <div className="atlas-arch-toolbar">
            <label className="atlas-arch-filter"><svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><circle cx="6.5" cy="6.5" r="4.5" fill="none" stroke="currentColor" strokeWidth="1.4" /><path d="m10 10 4 4" stroke="currentColor" strokeWidth="1.4" /></svg>
                <input aria-label={text.filter} placeholder={text.filterPlaceholder} type="search" value={config.filter}
                    onChange={event => setConfig(current => ({ ...current, filter: event.target.value }))} />
            </label><span className="atlas-arch-saved">{saved ? text.saved : text.sessionOnly}</span>
            <button className="atlas-arch-action" onClick={() => setConfig({ ...DEFAULT_ARCHITECTURE_CONFIG })}>{text.reset}</button>
        </div>
        {!projectName ? <Empty>{text.chooseProject}</Empty> : loading ? <div role="status"><Empty>{text.loading}</Empty></div>
            : error ? <div role="alert" className="atlas-arch-empty" data-error="true"><p>{text.loadFailed}</p><p className="atlas-arch-error-detail">{error}</p>
                {onRefresh && <button className="atlas-arch-action" onClick={onRefresh}>{text.retry}</button>}</div>
                : !data ? <Empty>{text.unavailable}</Empty> : null}
        {ready && data && filtered && <div className="atlas-arch-content" key={`${config.view}:${config.filter}:${graphGeneration ?? ""}`} data-testid="atlas-architecture-content">
            {config.view === 'overview' ? <><RepositoryMapView graph={graph} graphNote={graphNote} overview={data} filter={config.filter} onNavigate={onNavigate} onSelect={onSelect} selection={selection} selectionPanel={selectionPanel} readSource={readSource} />
                <details><summary>{text.summaryDetails}</summary><Overview data={data} filtered={filtered} filter={config.filter} onGroup={onGroup} onNavigate={onNavigate} /></details></>
                : config.view === 'dependencies' ? <Dependencies data={filtered} empty={empty} onGroup={onGroup} graph={graph} onNavigate={onNavigate} />
                    : <Findings view={config.view} data={filtered} empty={empty} onNavigate={onNavigate} />}
        </div>}
        {ready && <details className="atlas-arch-evidence"><summary>{text.evidenceSummary}</summary><p>{text.evidenceDetail}</p></details>}
    </section>;
}

export default function ArchitecturePanel(props: ArchitecturePanelProps): JSX.Element {
    return <ArchitectureWorkspace key={props.projectName} {...props} />;
}
