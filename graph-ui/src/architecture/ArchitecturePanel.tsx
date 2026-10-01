import { lazy, Suspense, useEffect, useMemo, useState } from 'react';
import type { JSX, ReactNode } from 'react';
import type { ArchitectureHotspot, ArchitectureOverviewDto } from '../core/intelligence-provider';
import {
    ARCHITECTURE_VIEWS, readArchitectureConfig, saveArchitectureConfig,
} from './architecture-model';
import type { ArchitectureView, ConfigStorage } from './architecture-model';
import { architectureText as text } from './strings';
import './architecture.css';
import type { RepositoryMapProps } from './RepositoryMap';
import SpatialArchitecture from './SpatialArchitecture';
import RoutesArchitecture from './RoutesArchitecture';
import type { CoverageIndex } from '../app/tree-model';
import type { SystemArchitectureLoader } from './system-architecture-source';
import type { SelectionEvidenceListener } from '../galaxy/selection-evidence';

const SystemArchitecture = lazy(() => import('./SystemArchitecture'));

export interface ArchitecturePanelProps extends Pick<RepositoryMapProps, 'graph' | 'selection' | 'selectionPanel' | 'onSelect' | 'graphNote' | 'readSource'> {
    projectName: string;
    graphGeneration?: string;
    active?: boolean;
    coverage?: CoverageIndex;
    overview?: ArchitectureOverviewDto;
    loading?: boolean;
    error?: string;
    onRefresh?: () => void;
    onClearSelection?: () => void;
    onSelectionEvidence?: SelectionEvidenceListener;
    systemArchitectureLoader?: SystemArchitectureLoader;
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

function ArchitectureWorkspace({ projectName, overview, loading = false, error, onRefresh, onNavigate, graph, selectionPanel, onSelect, onClearSelection, onSelectionEvidence, graphNote, graphGeneration, active = true, coverage, systemArchitectureLoader }: ArchitecturePanelProps): JSX.Element {
    const storage = useMemo(browserStorage, []);
    const [config, setConfig] = useState(() => ({ ...readArchitectureConfig(storage, projectName), filter: '' }));
    useEffect(() => { saveArchitectureConfig(storage, projectName, config); }, [config, projectName, storage]);
    // A cached summary from another project must never appear here.
    const data = overview?.projectName && overview.projectName !== projectName ? undefined : overview;
    const ready = Boolean(data && !loading && !error && projectName);
    const systemView = config.view === 'structure' || config.view === 'behavior' ? config.view : undefined;
    useEffect(() => {
        if (active && (!projectName || (!systemView && (!ready || !graph)))) onSelectionEvidence?.(undefined);
    }, [active, projectName, systemView, ready, graph, onSelectionEvidence]);
    const SpatialView = config.view === 'routes' ? RoutesArchitecture : SpatialArchitecture;
    return <section className="atlas-architecture" data-testid="atlas-architecture" data-system-view={systemView} aria-label={text.title} aria-busy={!systemView && loading}>
        <nav className="atlas-arch-tabs" aria-label={text.navigation}>{ARCHITECTURE_VIEWS.map(view =>
            <button className="atlas-arch-tab" key={view} aria-pressed={config.view === view || (view === 'overview' && ['dependencies', 'entryPoints'].includes(config.view))} data-view={view}
                onClick={() => { if (view !== config.view) onSelectionEvidence?.(undefined); setConfig(current => ({ ...current, view })); }}>{text.views[view]}</button>)}</nav>
        {systemView && projectName ? <Suspense fallback={<div role="status"><Empty>Preparing system analysis…</Empty></div>}><SystemArchitecture
            project={projectName} generation={graphGeneration} view={systemView} filter={config.filter} active={active}
            graph={graph} onSelect={onSelect} onClearSelection={onClearSelection} onSelectionEvidence={onSelectionEvidence} onNavigate={onNavigate} loader={systemArchitectureLoader} /></Suspense> : null}
        {!systemView && (!projectName ? <Empty>{text.chooseProject}</Empty> : loading ? <div role="status"><Empty>{text.loading}</Empty></div>
            : error ? <div role="alert" className="atlas-arch-empty" data-error="true"><p>{text.loadFailed}</p><p className="atlas-arch-error-detail">{error}</p>
                {onRefresh && <button className="atlas-arch-action" onClick={onRefresh}>{text.retry}</button>}</div>
                : !data ? <Empty>{text.unavailable}</Empty> : null)}
        {systemView && !projectName && <Empty>{text.chooseProject}</Empty>}
        {!systemView && ready && data && graph && <SpatialView project={projectName} generation={graphGeneration} graph={graph} overview={data}
            view={config.view === 'dependencies' || config.view === 'structure' || config.view === 'behavior' ? 'overview' : config.view} filter={config.filter} active={active}
            graphNote={graphNote} coverage={coverage} onSelect={onSelect} onClearSelection={onClearSelection} onSelectionEvidence={onSelectionEvidence} selectionPanel={selectionPanel} onNavigate={onNavigate} onView={view => { onSelectionEvidence?.(undefined); setConfig(current => ({ ...current, view: view === 'dependencies' ? 'overview' : view })); }} />}
        {!systemView && ready && data && !graph && <div className="atlas-arch-content" data-testid="atlas-architecture-content">
            <p className="atlas-arch-fallback-summary">{data.files.length.toLocaleString()} files · {data.groups.length.toLocaleString()} source areas · {data.boundaries.length.toLocaleString()} cross-area connections</p>
            <Findings view={config.view === 'overview' || config.view === 'dependencies' ? 'entryPoints' : config.view} data={data} empty={text.noData} onNavigate={onNavigate} />
        </div>}
    </section>;
}

export default function ArchitecturePanel(props: ArchitecturePanelProps): JSX.Element {
    return <ArchitectureWorkspace key={props.projectName} {...props} />;
}
