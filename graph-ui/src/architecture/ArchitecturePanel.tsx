import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { JSX, ReactNode } from 'react';
import type { ArchitectureHotspot, ArchitectureOverviewDto } from '../core/intelligence-provider';
import {
    ARCHITECTURE_VIEWS, readArchitectureConfig, saveArchitectureConfig,
} from './architecture-model';
import type { ArchitectureView, ConfigStorage } from './architecture-model';
import { architectureHistoryText as historyText, architectureText as text } from './strings';
import { peekNavigation, type NavigationHistory } from '../graph/navigation-history';
import { useDismissibleMenu } from '../graph/use-dismissible-menu';
import { FitLabel, useToolbarFit } from '../galaxy/toolbar-fit';
import {
    architectureEntryDetail, architectureEntryLabel, architectureEntryName, architectureHistoryOptions, initialArchitecturePlace,
    type ArchitectureHistoryEntry, type PlaceChange, type RoutesPerspective, type SpatialPlace, type SystemPlace,
} from './architecture-history';
import { useArchitectureHistory } from './use-architecture-history';
import './architecture.css';
import { scenePaletteStyle } from './scene-palette';
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
    /** Another surface (help, settings, a dialog) takes the keys: Alt+Left and Alt+Right stay with it. */
    escapeTaken?: boolean;
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

const paletteStyle = scenePaletteStyle();

/**
 * Back, Forward and the Recent list for the whole workspace (K27). The
 * tooltips name the target ("Back to Overview · django"); the list holds the
 * last distinct places, newest first, and the current one is not a target.
 * They stand at the start of the subtab row and read as in Galaxy, "← Back",
 * "Forward →" and "▾", with the glyphs alone once the tabs would scroll
 * (hand test 2026-10-04, A1). The menu closes as Galaxy's does (A2).
 */
function HistoryControls({ history, place, onGo, onJump }: {
    history: NavigationHistory<ArchitectureHistoryEntry>; place: ArchitectureHistoryEntry;
    onGo: (step: -1 | 1) => void; onJump: (entry: ArchitectureHistoryEntry) => void;
}): JSX.Element {
    const back = peekNavigation(history, -1), forward = peekNavigation(history, 1);
    const here = architectureHistoryOptions.recentKey?.(place);
    const menu = useDismissibleMenu(architectureHistoryOptions.key(place));
    return <div className="atlas-arch-history" role="group" aria-label={historyText.group} data-position={`${history.index + 1}/${history.entries.length}`}>
        <button type="button" aria-label={historyText.back} disabled={!back} onClick={() => onGo(-1)}
            title={back ? historyText.backTo(architectureEntryLabel(back)) : historyText.noBack}>
            <FitLabel wide={historyText.backWide} narrow={historyText.backGlyph} /></button>
        <button type="button" aria-label={historyText.forward} disabled={!forward} onClick={() => onGo(1)}
            title={forward ? historyText.forwardTo(architectureEntryLabel(forward)) : historyText.noForward}>
            <FitLabel wide={historyText.forwardWide} narrow={historyText.forwardGlyph} /></button>
        {history.recent.length > 1 && <details className="atlas-arch-recent" ref={menu.ref}>
            <summary title={historyText.recentTitle} aria-label={historyText.recent}>{historyText.recentGlyph}</summary>
            <ul className="atlas-arch-recent-menu" aria-label={historyText.recentList}>{history.recent.map(entry => {
                const id = architectureHistoryOptions.recentKey?.(entry);
                const current = id === here;
                const detail = architectureEntryDetail(entry);
                return <li key={id}>
                    <button type="button" disabled={current} aria-current={current ? 'true' : undefined} title={architectureEntryLabel(entry)} onClick={() => {
                        menu.close();
                        onJump(entry);
                    }}><strong>{architectureEntryName(entry)}</strong>{detail && <span>{detail}</span>}</button>
                </li>;
            })}</ul>
        </details>}
    </div>;
}

function ArchitectureWorkspace({ projectName, overview, loading = false, error, onRefresh, onNavigate, graph, selectionPanel, onSelect, onClearSelection, onSelectionEvidence, graphNote, graphGeneration, active = true, escapeTaken = false, coverage, systemArchitectureLoader }: ArchitecturePanelProps): JSX.Element {
    const storage = useMemo(browserStorage, []);
    /*
     * The place is the whole history entry (K27): subtab, opened area or file,
     * route perspective and filter, System structure focus and groups, Behavior
     * start, destination and position, and Plan or 3D of each scene. The views
     * below read their part of it and report changes back, so Back and Forward
     * restore every part at once; the one Back for all of them sits before the
     * subtabs. A filter is a search for this visit: one saved by an earlier
     * session would silently hide routes, so only the subtab is read back.
     */
    const navigation = useArchitectureHistory(() => initialArchitecturePlace(readArchitectureConfig(storage, projectName).view), {
        active, escapeTaken, onRestore: (from, to) => { if (from.view !== to.view) onSelectionEvidence?.(undefined); },
    });
    const { place, navigate } = navigation;
    // The row measures itself as Galaxy's toolbar does (toolbar-fit.tsx): it counts as narrow once the tabs would scroll.
    const tabRow = useRef<HTMLDivElement>(null);
    useToolbarFit(tabRow, true);
    useEffect(() => { saveArchitectureConfig(storage, projectName, { view: place.view, filter: place.filter }); }, [place.view, place.filter, projectName, storage]);
    const setView = useCallback((view: ArchitectureView) => navigate(current => ({ ...current, view: view === 'dependencies' ? 'overview' : view })), [navigate]);
    const changeSpatial = useCallback<PlaceChange<SpatialPlace>>((change, automatic) =>
        navigate(current => ({ ...current, spatial: { ...current.spatial, ...change } }), automatic), [navigate]);
    const changeSystem = useCallback<PlaceChange<SystemPlace>>((change, automatic) =>
        navigate(current => ({ ...current, system: { ...current.system, ...change } }), automatic), [navigate]);
    const changePerspective = useCallback((routes: RoutesPerspective) => navigate(current => ({ ...current, routes })), [navigate]);
    // An opened route group ("Show these N routes") is a step at once; typing waits for a pause.
    const openRouteGroup = useCallback((filter: string) => navigate(current => ({ ...current, filter })), [navigate]);
    // A cached summary from another project must never appear here.
    const data = overview?.projectName && overview.projectName !== projectName ? undefined : overview;
    const ready = Boolean(data && !loading && !error && projectName);
    const systemView = place.view === 'structure' || place.view === 'behavior' ? place.view : undefined;
    useEffect(() => {
        if (active && (!projectName || (!systemView && (!ready || !graph)))) onSelectionEvidence?.(undefined);
    }, [active, projectName, systemView, ready, graph, onSelectionEvidence]);
    // Only the Routes view offers the filter, so no other view may receive a stale one.
    const filter = place.view === 'routes' ? navigation.filter : '';
    const spatialProps = !systemView && ready && data && graph ? {
        project: projectName, generation: graphGeneration, graph, overview: data,
        view: place.view === 'dependencies' || place.view === 'structure' || place.view === 'behavior' ? 'overview' as const : place.view,
        filter, onFilter: openRouteGroup, active, graphNote, coverage, onSelect, onClearSelection, onSelectionEvidence, selectionPanel, onNavigate,
        onView: (view: ArchitectureView) => { onSelectionEvidence?.(undefined); setView(view); }, place: place.spatial, onPlace: changeSpatial,
    } : undefined;
    // Every scene below draws with the same palette; the DOM reads it as CSS variables.
    return <section className="atlas-architecture" data-testid="atlas-architecture" data-system-view={systemView} aria-label={text.title} aria-busy={!systemView && loading} style={paletteStyle}>
        <div className="atlas-arch-tabrow" ref={tabRow}>
            <HistoryControls history={navigation.history} place={place} onGo={navigation.go} onJump={navigation.jump} />
            <nav className="atlas-arch-tabs" aria-label={text.navigation} data-fit-whole="">{ARCHITECTURE_VIEWS.map(view =>
                <button className="atlas-arch-tab" key={view} aria-pressed={place.view === view || (view === 'overview' && ['dependencies', 'entryPoints'].includes(place.view))} data-view={view}
                    onClick={() => { if (view !== place.view) onSelectionEvidence?.(undefined); setView(view); }}>{text.views[view]}</button>)}</nav>
        </div>
        {systemView && projectName ? <Suspense fallback={<div role="status"><Empty>Preparing system analysis…</Empty></div>}><SystemArchitecture
            project={projectName} generation={graphGeneration} view={systemView} filter={filter} active={active}
            graph={graph} onSelect={onSelect} onClearSelection={onClearSelection} onSelectionEvidence={onSelectionEvidence} onNavigate={onNavigate} loader={systemArchitectureLoader}
            place={place.system} onPlace={changeSystem} /></Suspense> : null}
        {!systemView && (!projectName ? <Empty>{text.chooseProject}</Empty> : loading ? <div role="status"><Empty>{text.loading}</Empty></div>
            : error ? <div role="alert" className="atlas-arch-empty" data-error="true"><p>{text.loadFailed}</p><p className="atlas-arch-error-detail">{error}</p>
                {onRefresh && <button className="atlas-arch-action" onClick={onRefresh}>{text.retry}</button>}</div>
                : !data ? <Empty>{text.unavailable}</Empty> : null)}
        {systemView && !projectName && <Empty>{text.chooseProject}</Empty>}
        {spatialProps && place.view === 'routes' && <div className="atlas-arch-toolbar"><label className="atlas-arch-filter">
            <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><circle cx="6.5" cy="6.5" r="4.5" fill="none" stroke="currentColor" strokeWidth="1.4" /><path d="m10 10 4 4" stroke="currentColor" strokeWidth="1.4" /></svg>
            <input aria-label={text.filter} placeholder={text.filterPlaceholder} type="search" value={navigation.filter} onChange={event => navigation.type(event.target.value)} /></label></div>}
        {spatialProps && (place.view === 'routes'
            ? <RoutesArchitecture {...spatialProps} perspective={place.routes} onPerspective={changePerspective} />
            : <SpatialArchitecture {...spatialProps} />)}
        {!systemView && ready && data && !graph && <div className="atlas-arch-content" data-testid="atlas-architecture-content">
            <p className="atlas-arch-fallback-summary">{data.files.length.toLocaleString()} files · {data.groups.length.toLocaleString()} source areas · {data.boundaries.length.toLocaleString()} cross-area connections</p>
            <Findings view={place.view === 'overview' || place.view === 'dependencies' ? 'entryPoints' : place.view} data={data} empty={text.noData} onNavigate={onNavigate} />
        </div>}
    </section>;
}

/** A project switch remounts the workspace: a fresh history, as in Galaxy. */
export default function ArchitecturePanel(props: ArchitecturePanelProps): JSX.Element {
    return <ArchitectureWorkspace key={props.projectName} {...props} />;
}
