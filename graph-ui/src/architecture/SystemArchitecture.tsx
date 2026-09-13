import { lazy, Suspense, useEffect, useMemo, useState } from 'react';
import type { GraphData, GraphNode } from '../galaxy/types';
import { loadSystemArchitecture, type SystemArchitectureLoader, type SystemArchitectureResponse, type SystemProjection, type SystemSymbol, type SystemWitness, type SystemCallEvidence } from './system-architecture-source';
import { componentBasis, isContiguousPath, systemOverviewGraph, systemComponents, type SystemSceneEdge, type SystemSceneModel } from './system-architecture-model';
import { connectionLoad } from '../graph/connection-load';
import BehaviorJourney from './BehaviorJourney';
import './system-architecture.css';

const Scene = lazy(() => import('./SystemArchitectureScene'));
export interface SystemArchitectureProps {
    project: string; generation?: string; view: 'structure' | 'behavior'; filter: string; active: boolean;
    graph?: GraphData; onSelect?: (node: GraphNode) => void; onClearSelection?: () => void;
    onNavigate: (path: string, line?: number, name?: string) => void;
    loader?: SystemArchitectureLoader;
}

function EvidenceSource({ symbol, onOpen }: { symbol: SystemSymbol; onOpen: (symbol: SystemSymbol) => void }) {
    return <div className="system-source"><button disabled={!symbol.file_path} onClick={() => onOpen(symbol)} title={symbol.qualified_name}>{symbol.name}</button>
        <small>{symbol.file_path ? `${symbol.file_path}${symbol.start_line ? `:${symbol.start_line}` : ''}` : 'Source location unavailable'}</small></div>;
}

/** Prefer a conventional executable entry when one exists; keep every indexed entry selectable. */
export function suggestedBehaviorEntry(entries: SystemSymbol[]): SystemSymbol | undefined {
    return entries.filter(entry => /^(main|Main)$/.test(entry.name)
        && entry.file_path && !/(^|\/)(tests?|__tests__|examples?|fixtures?|vendor|node_modules)(\/|$)/i.test(entry.file_path)
        && /\.(c|cc|cpp|go|rs|py|java|cs|js|ts)$/.test(entry.file_path))
        .sort((a, b) => Number(!/^src\/main\./.test(a.file_path!)) - Number(!/^src\/main\./.test(b.file_path!))
            || a.file_path!.split('/').length - b.file_path!.split('/').length || a.file_path!.localeCompare(b.file_path!))[0];
}

const handoffs = (symbols: SystemSymbol[]) => symbols.slice(1).filter((symbol, index) => symbol.component_id !== symbols[index].component_id).length;
type ConnectionView = 'calls' | 'types' | 'all';
const connectionAllowed = (type: string, view: ConnectionView) => view === 'all'
    || (view === 'calls' ? ['CALLS', 'IMPORTS', 'HTTP_CALLS', 'ASYNC_CALLS'] : ['INHERITS', 'IMPLEMENTS']).includes(type);

/** Poll only while this view is active. A new request key hides stale results before its effect runs. */
export default function SystemArchitecture({ project, generation, view, filter, active, graph, onSelect, onClearSelection, onNavigate, loader = loadSystemArchitecture }: SystemArchitectureProps) {
    const [entryChoice, setEntryChoice] = useState<{ project: string; generation?: string; expectedGeneration?: string; id?: number; targetId?: number }>();
    const [entryChoices, setEntryChoices] = useState<{ project: string; generation?: string; analysisGeneration: string; entries: SystemSymbol[] }>();
    const [snapshot, setSnapshot] = useState<{ project: string; generation?: string; analysisGeneration: string; data: SystemProjection }>();
    const [targetCatalog, setTargetCatalog] = useState<{ project: string; generation?: string; source: number; analysisGeneration: string; targets: (SystemSymbol & { distance: number })[] }>();
    const [expandedGroups, setExpandedGroups] = useState<string[]>([]);
    const [scopeHistory, setScopeHistory] = useState<(string | undefined)[]>([]);
    const [revision, setRevision] = useState(0);
    const [reading, setReading] = useState<{ key: string; response?: SystemArchitectureResponse; error?: string }>();
    const [cyclesOnly, setCyclesOnly] = useState(false);
    const [includeUnconnected, setIncludeUnconnected] = useState(false);
    const [includeTests, setIncludeTests] = useState(false);
    const [connectionView, setConnectionView] = useState<ConnectionView>('calls');
    const [pathIndex, setPathIndex] = useState(0);
    const [selection, setSelection] = useState<{ node?: string; edge?: string; dependencyIndex?: number }>();
    const [listLimit, setListLimit] = useState(20);
    const [resetKey, setResetKey] = useState(0);
    const [planar, setPlanar] = useState(false);
    const [focusId, setFocusId] = useState<string>();
    const [stepIndex, setStepIndex] = useState(0);
    const [showConnectionLoad, setShowConnectionLoad] = useState(true);
    useEffect(() => { setSelection(undefined); setPathIndex(0); setStepIndex(0); }, [filter, cyclesOnly, includeUnconnected, includeTests, connectionView]);
    const requestedEntry = view === 'behavior' && entryChoice?.project === project && entryChoice.generation === generation ? entryChoice.id : undefined;
    const requestedTarget = requestedEntry === undefined ? undefined : entryChoice?.targetId;
    const expectedGeneration = requestedEntry === undefined ? undefined : entryChoice?.expectedGeneration;
    const requestKey = JSON.stringify([project, generation, view, requestedEntry, requestedTarget, expectedGeneration, revision]);
    useEffect(() => {
        if (!active || !project) return;
        const controller = new AbortController();
        let timer: ReturnType<typeof setTimeout> | undefined;
        setReading({ key: requestKey }); setSelection(undefined); setPathIndex(0); setStepIndex(0); setListLimit(20);
        const poll = async () => {
            try {
                const response = await loader({ project, entryNodeId: requestedEntry, ...(requestedTarget === undefined ? {} : { targetNodeId: requestedTarget }), ...(expectedGeneration === undefined ? {} : { expectedGeneration }), ...(view === 'behavior' ? { includeBehaviorEvidence: true } : {}) }, controller.signal);
                if (controller.signal.aborted) return;
                // The numeric identity belongs to the projection that supplied the dropdown.
                // A stale-entry rejection reopens the overview once, rather than reusing that ID.
                if (response.status === 'failed' && requestedEntry !== undefined && expectedGeneration !== undefined && response.generation !== expectedGeneration) {
                    setEntryChoice({ project, generation }); setEntryChoices(undefined); setTargetCatalog(undefined); setSnapshot(undefined); return;
                }
                setReading({ key: requestKey, response });
                if (response.status === 'pending') timer = setTimeout(() => { void poll(); }, Math.min(5000, Math.max(500, response.retry_after_ms ?? 500)));
                else if (response.status === 'ready' && response.result) {
                    setSnapshot({ project, generation, analysisGeneration: response.generation, data: response.result });
                    if (response.result.behavior?.mode === 'targets') {
                        const behavior = response.result.behavior;
                        const reachable = new Map(behavior.reachable_targets.map(target => [target.id, target]));
                        // Bounded source witnesses also expose useful deeper destinations beyond the catalog page.
                        for (const path of response.result.paths) {
                            if (path.entrypoint_id !== behavior.source_id || !isContiguousPath(path)) continue;
                            path.nodes.forEach((symbol, hop) => {
                                if (hop > 0 && hop <= behavior.max_hops && !reachable.has(symbol.id)) reachable.set(symbol.id, { ...symbol, distance: hop });
                            });
                        }
                        setTargetCatalog({ project, generation, source: behavior.source_id, analysisGeneration: response.generation, targets: [...reachable.values()] });
                    }
                    setEntryChoices(current => requestedEntry !== undefined && current?.project === project && current.generation === generation ? current : { project, generation, analysisGeneration: response.generation, entries: response.result!.entrypoints });
                }
            } catch (error) {
                if (!controller.signal.aborted) setReading({ key: requestKey, error: error instanceof Error ? error.message : 'Architecture analysis failed.' });
            }
        };
        void poll();
        return () => { controller.abort(); if (timer !== undefined) clearTimeout(timer); };
    }, [project, generation, active, view, requestedEntry, requestedTarget, expectedGeneration, revision, requestKey, loader]);
    const current = reading?.key === requestKey ? reading : undefined;
    const queryData = current?.response?.status === 'ready' ? current.response.result : undefined;
    const matchingSnapshot = snapshot?.project === project && snapshot.generation === generation ? snapshot : undefined;
    const data = queryData ?? matchingSnapshot?.data;
    const projectionGeneration = queryData ? current?.response?.generation : matchingSnapshot?.analysisGeneration;
    const sourceGeneration = expectedGeneration ?? projectionGeneration;
    const matchesSourceGeneration = (analysisGeneration: string | undefined) => analysisGeneration === sourceGeneration
        && (projectionGeneration === undefined || analysisGeneration === projectionGeneration);
    const matchingTargetCatalog = targetCatalog?.project === project && targetCatalog.generation === generation
        && targetCatalog.source === requestedEntry && matchesSourceGeneration(targetCatalog.analysisGeneration) ? targetCatalog : undefined;
    const currentTargets = requestedEntry !== undefined && queryData?.behavior?.source_id === requestedEntry && matchesSourceGeneration(current?.response?.generation)
        ? queryData.behavior.reachable_targets : [];
    const targets = matchingTargetCatalog?.targets ?? currentTargets;
    const queryPending = !current?.error && current?.response?.status !== 'failed' && !queryData;
    useEffect(() => { setFocusId(undefined); setExpandedGroups([]); setScopeHistory([]); setSelection(undefined); }, [project, generation]);
    const structureData = useMemo(() => data ? { ...data, dependencies: data.dependencies.filter(edge => connectionAllowed(edge.type, connectionView)) } : undefined, [data, connectionView]);
    const error = current?.error ?? (current?.response?.status === 'failed' ? current.response.error ?? 'Architecture analysis failed.' : undefined);
    const entries = entryChoices?.project === project && entryChoices.generation === generation ? entryChoices.entries : data?.entrypoints ?? [];
    useEffect(() => {
        if (view !== 'behavior' || !data || (entryChoice?.project === project && entryChoice.generation === generation)) return;
        const suggested = suggestedBehaviorEntry(data.entrypoints);
        if (suggested) setEntryChoice({ project, generation, expectedGeneration: current?.response?.generation ?? snapshot?.analysisGeneration, id: suggested.id });
    }, [view, data, project, generation, entryChoice, current?.response?.generation, snapshot?.analysisGeneration]);
    const returnedPaths = useMemo(() => {
        const valid = queryData?.paths.filter(path => isContiguousPath(path)) ?? [];
        if (queryData?.behavior && requestedTarget === undefined) return [];
        const entry = requestedEntry ?? valid[0]?.entrypoint_id;
        return valid.filter(path => path.entrypoint_id === entry)
            .sort((a, b) => requestedTarget !== undefined ? a.edges.length - b.edges.length : b.edges.length - a.edges.length || handoffs(b.nodes) - handoffs(a.nodes));
    }, [queryData, requestedEntry, requestedTarget]);
    const paths = useMemo(() => {
        const query = filter.trim().toLocaleLowerCase();
        return returnedPaths.filter(path => !query || path.nodes.some(symbol => [symbol.name, symbol.file_path ?? ''].some(value => value.toLocaleLowerCase().includes(query))));
    }, [returnedPaths, filter]);
    const activePathIndex = Math.min(pathIndex, Math.max(0, paths.length - 1));
    const highlightedPathIndex = paths.length ? activePathIndex : undefined;
    const path = paths[activePathIndex];
    const activeStep = Math.min(stepIndex, Math.max(0, (path?.nodes.length ?? 1) - 1));
    const relationshipTypes = view === 'behavior' || connectionView === 'calls' ? ['CALLS', 'IMPORTS', 'HTTP_CALLS', 'ASYNC_CALLS']
        : connectionView === 'all' ? undefined : ['INHERITS', 'IMPLEMENTS'];
    const layoutKey = JSON.stringify([project, current?.response?.generation ?? snapshot?.analysisGeneration ?? generation]);
    const overviewModel = useMemo(() => data && view === 'structure' ? systemOverviewGraph(data, {
        filter, cyclesOnly, includeTests, includeUnconnected,
        relationshipTypes, expandedGroupIds: expandedGroups, focusId, layoutKey,
    }) : { nodes: [], edges: [], lanes: [], omittedNodes: 0, omittedEdges: 0 } as SystemSceneModel,
    [data, filter, view, cyclesOnly, includeTests, includeUnconnected, connectionView, expandedGroups, focusId, layoutKey]);
    const model = overviewModel;
    const loads = useMemo(() => connectionLoad(model.nodes, model.edges), [model]);
    const node = model.nodes.find(candidate => candidate.id === selection?.node);
    const listedDependency = selection?.dependencyIndex === undefined ? undefined : data?.dependencies[selection.dependencyIndex];
    const edge: SystemSceneEdge | undefined = model.edges.find(candidate => candidate.id === selection?.edge)
        ?? (listedDependency ? { ...listedDependency, id: `listed:${selection?.dependencyIndex}`, dependency: listedDependency } : undefined);
    const component = node?.component;
    const selectedSymbol = node?.symbol ?? (view === 'behavior' && !edge && (!selection?.node || selection?.node.startsWith('step:')) ? path?.nodes[activeStep] : undefined);
    const allComponents = useMemo(() => structureData && view === 'structure' ? systemComponents(structureData, filter, cyclesOnly, includeUnconnected, includeTests) : [], [structureData, view, filter, cyclesOnly, includeUnconnected, includeTests]);
    const listedDependencies = useMemo(() => {
        const ids = new Set(allComponents.map(item => item.id));
        return data?.dependencies.map((dependency, index) => ({ dependency, index })).filter(item => connectionAllowed(item.dependency.type, connectionView) && ids.has(item.dependency.source) && ids.has(item.dependency.target)) ?? [];
    }, [data, allComponents, connectionView]);
    const sceneComponents = model.nodes.flatMap(item => item.component ? [item.component] : []);
    const selectedComponent = component ?? sceneComponents.find(item => item.id === selection?.node) ?? allComponents.find(candidate => candidate.id === (selection?.node ?? model.focusId))
        ?? allComponents.find(candidate => candidate.id === model.focusId) ?? (filter.trim() && sceneComponents.length === 1 ? sceneComponents[0] : undefined);
    const neighbors = useMemo(() => {
        const eligible = new Set(sceneComponents.map(item => item.id));
        const incoming = new Map<string, Set<string>>(), outgoing = new Map<string, Set<string>>();
        for (const dependency of model.edges) {
            if (dependency.source === dependency.target) continue;
            const target = dependency.target === selectedComponent?.id ? incoming : dependency.source === selectedComponent?.id ? outgoing : undefined;
            const id = target === incoming ? dependency.source : dependency.target;
            if (target && eligible.has(id)) {
                if (!target.has(id)) target.set(id, new Set());
                for (const type of dependency.types ?? [dependency.type]) target.get(id)!.add(type);
            }
        }
        return { incoming, outgoing };
    }, [model, selectedComponent]);
    function selectSymbol(symbol: SystemSymbol, open = false) {
        // The response can come from a newer graph snapshot; numeric IDs are not portable between snapshots.
        const existing = symbol.qualified_name ? graph?.nodes.find(candidate => candidate.qualified_name === symbol.qualified_name) : undefined;
        if (existing) onSelect?.(existing);
        if (open && symbol.file_path) onNavigate(symbol.file_path, symbol.start_line || undefined, symbol.name);
    }
    function selectNode(id: string) {
        setSelection({ node: id });
        const found = model.nodes.find(candidate => candidate.id === id);
        // Inspection deliberately leaves topology and camera unchanged.
        if (found?.pathIndices?.length && !found.pathIndices.includes(activePathIndex)) setPathIndex(found.pathIndices[0]);
        if (found?.depth !== undefined) setStepIndex(found.depth);
        const symbol = found?.symbol;
        if (symbol) selectSymbol(symbol);
    }
    function selectPath(index: number) { setPathIndex(index); setStepIndex(0); setSelection(undefined); }
    function selectStep(index: number) {
        if (!path?.nodes[index]) return;
        setStepIndex(index);
        const found = model.nodes.find(item => item.depth === index && item.pathIndices?.includes(activePathIndex))
            ?? model.nodes.find(item => item.componentIds?.includes(path.nodes[index].component_id) || item.id === path.nodes[index].group_id);
        setSelection(data?.overview ? undefined : found ? { node: found.id } : undefined); selectSymbol(path.nodes[index]);
    }
    function selectEdge(id: string) {
        const found = model.edges.find(item => item.id === id);
        const nextPath = found?.pathIndices?.includes(activePathIndex) ? activePathIndex : found?.pathIndices?.[0] ?? activePathIndex;
        setPathIndex(nextPath);
        setStepIndex(Math.max(0, paths[nextPath]?.edges.findIndex(item => item.id === found?.pathEdge?.id) ?? 0));
        setSelection({ edge: id });
    }
    function open(symbol: SystemSymbol) { selectSymbol(symbol, true); }
    function revealEvidence() {
        const details = document.getElementById(`system-evidence-${view}`) as HTMLDetailsElement | null;
        if (details) { details.open = true; details.scrollIntoView({ block: 'nearest' }); }
    }
    const pathSource = edge?.pathEdge && path?.nodes.find(symbol => symbol.id === edge.pathEdge!.source_id);
    const pathTarget = edge?.pathEdge && path?.nodes.find(symbol => symbol.id === edge.pathEdge!.target_id);
    const pending = !error && !data;
    function clearSelection() {
        setSelection(undefined); setFocusId(undefined); setScopeHistory([]); setExpandedGroups([]);
        setResetKey(value => value + 1); onClearSelection?.();
    }
    function focus(id: string | undefined) { setScopeHistory(previous => [...previous, focusId]); setFocusId(id); }
    function toggleGroup(id: string) { setExpandedGroups(previous => previous.includes(id) ? previous.filter(value => value !== id) : [...previous, id]); }
    const selectedGroup = node?.group;
    const corridorWitnesses: SystemWitness[] = view === 'behavior' ? (edge?.behaviorEdges ?? []).flatMap(evidence => {
        const source = queryData?.behavior?.nodes.find(symbol => symbol.id === evidence.source_id);
        const target = queryData?.behavior?.nodes.find(symbol => symbol.id === evidence.target_id);
        return source && target ? [{ ...evidence, edge_id: evidence.id, source, target }] : [];
    }) : [];
    const selectedDependencies = corridorWitnesses.length ? [] : edge?.dependencies ?? (edge?.dependency ? [edge.dependency] : []);
    const selectedSceneNode = selection?.node ?? (selectedSymbol ? model.nodes.find(item => item.symbol?.id === selectedSymbol.id
        || item.id === selectedSymbol.group_id || item.componentIds?.includes(selectedSymbol.component_id))?.id : undefined);
    const behavior = queryData?.behavior;
    const completeOverview = data?.overview?.complete;
    const limitationCount = data ? Object.entries(data.limits).filter(([key, value]) => (key.startsWith('omitted_') || key.endsWith('_truncated')) && Boolean(value)).length : 0;
    const behaviorPage = view === 'behavior' ? <BehaviorJourney project={project} generation={projectionGeneration} data={queryData}
        entries={entries} targets={targets} entryId={requestedEntry} targetId={requestedTarget} active={active} pending={queryPending}
        error={error} filter={filter} onRefresh={() => setRevision(value => value + 1)} onNavigate={onNavigate} onSelectSymbol={selectSymbol} onClearSelection={onClearSelection}
        onRequest={(entry, targetId) => {
            const analysisGeneration = projectionGeneration ?? sourceGeneration;
            if (entry && analysisGeneration) setEntryChoices(previous => ({ project, generation, analysisGeneration,
                entries: [...new Map([...(previous?.project === project && previous.generation === generation ? previous.entries : entries), entry].map(item => [item.id, item])).values()] }));
            setEntryChoice({ project, generation, id: entry?.id, targetId, expectedGeneration: analysisGeneration });
        }} /> : undefined;
    if (behaviorPage) return behaviorPage;
    return <section className="system-architecture" data-testid="system-architecture" aria-label={view === 'structure' ? 'System structure' : 'Behavior'} aria-busy={active && pending}>
        <div className="system-toolbar">
            <p>{view === 'structure' ? 'Inspect the parts and the code that connects them.' : 'Follow a question through the same system map.'}</p>
            <button className="atlas-arch-action system-refresh" onClick={() => { setRevision(value => value + 1); setResetKey(value => value + 1); }}>Refresh analysis</button>
        </div><div className="system-controls">
            <div className="system-scope-actions"><button disabled={!scopeHistory.length} onClick={() => { setFocusId(scopeHistory.at(-1)); setScopeHistory(previous => previous.slice(0, -1)); }}>← Back</button>
                <button disabled={!focusId} onClick={clearSelection}>Whole system</button></div>
            {view === 'structure' && <label>Connections <select aria-label="Connection view" value={connectionView} onChange={event => { setConnectionView(event.target.value as ConnectionView); setCyclesOnly(false); }}>
                <option value="calls">Calls &amp; imports</option><option value="types">Type relationships</option><option value="all">All relationships</option>
            </select></label>}
            {view === 'structure' && <label title="Cycles in the displayed relationship lens; aggregate cycles do not establish symbol recursion."><input type="checkbox" checked={cyclesOnly} onChange={event => { setCyclesOnly(event.target.checked); setSelection(undefined); }} />{data?.overview ? 'Group cycles' : 'Component cycles'}</label>}
            {view === 'structure' && <label title="Include components with no connection in the returned projection. Analysis limits can hide relationships."><input type="checkbox" checked={includeUnconnected} onChange={event => setIncludeUnconnected(event.target.checked)} />Include unconnected</label>}
            {view === 'structure' && <label><input type="checkbox" checked={includeTests} onChange={event => { setIncludeTests(event.target.checked); setSelection(undefined); }} />Include test components</label>}
            {view === 'behavior' && <span className="system-small">Calls &amp; imports · all code groups</span>}
            {view === 'behavior' && <label>From <select aria-label="Behavior entry point" value={requestedEntry ?? ''} onChange={event => setEntryChoice({ project, generation,
                expectedGeneration: entryChoices?.analysisGeneration ?? current?.response?.generation, id: event.target.value ? Number(event.target.value) : undefined })}>
                <option value="">{path ? `Suggested · ${path.nodes[0].name}${path.nodes[0].file_path ? ` · ${path.nodes[0].file_path}` : ''}` : 'Choose an entry point'}</option>{entries.map(entry => <option key={entry.id} value={entry.id}>{entry.name}{entry.file_path ? ` · ${entry.file_path}` : ''}</option>)}
            </select></label>}
            {view === 'behavior' && (data?.behavior || requestedEntry !== undefined) && <label>To <select aria-label="Behavior destination" disabled={requestedEntry === undefined || (!targets.length && queryPending)} value={requestedTarget ?? ''}
                onChange={event => setEntryChoice({ project, generation, id: requestedEntry, targetId: event.target.value ? Number(event.target.value) : undefined,
                    expectedGeneration: matchingTargetCatalog?.analysisGeneration ?? sourceGeneration })}>
                <option value="">Choose a reachable operation…</option>{targets.map(target => <option key={target.id} value={target.id}>{target.name} · {target.file_path}</option>)}
            </select></label>}
        </div>
        {error && !data ? <div className="system-analysis-state" role="alert"><strong>Could not analyze this project.</strong><p>{error}</p><button className="atlas-arch-action" onClick={() => setRevision(value => value + 1)}>Try again</button></div>
            : pending ? <div className="system-analysis-state" role="status"><span className="system-progress" /><strong>{active ? 'Analyzing interactions…' : 'Analysis paused'}</strong><p>The workspace stays available while the server prepares this view.</p></div>
                : data && <>
                    {error && <p className="system-small" role="alert">{error} <button onClick={() => setRevision(value => value + 1)}>Try again</button></p>}
                    <div className="system-reading-status"><span>{queryPending ? 'Updating path…' : view === 'structure'
                        ? data.overview ? `${data.overview.groups.length} groups · ${model.nodes.filter(item => !item.parentId).length} shown · ${data.overview.totals.components ?? data.overview.components.length} components accounted for`
                            : `${data.components.length} components returned · ${model.nodes.length} shown · ${structureData?.dependencies.length} of ${data.dependencies.length} returned connections`
                        : behavior?.mode === 'corridor' ? `${behavior.nodes.length} symbols · ${behavior.edges.length} calls in this corridor`
                        : data.behavior ? 'Choose a destination to reveal its call paths' : `${paths.length} indexed paths`}</span>
                        <span>{(view === 'structure' && completeOverview === true) || (view === 'behavior' && (behavior?.mode === 'corridor' ? behavior.corridor_complete ?? behavior.complete : behavior?.complete)) || (!data.overview && data.complete && !limitationCount) ? 'Indexed snapshot' : 'Partial analysis'} · <button onClick={revealEvidence}>Evidence and limits</button></span></div>
                    <div className="system-display-controls">
                        <span className="system-layout-hint" title="Directory ancestry determines placement. Connections still come from the indexed graph. Representatives provide location hints when no directory group is reported.">{data.overview || view === 'structure' ? 'Grouped by source location' : 'Ordered by call paths'}</span>
                        <label title="Ring area follows log(1 + visible links), relative to the busiest item in this view. Each directed relationship type counts once; this is visual load, not runtime activity."><input type="checkbox" checked={showConnectionLoad} onChange={event => setShowConnectionLoad(event.target.checked)} />Connection load</label>
                        <div className="system-view-switch" role="group" aria-label="System camera"><button aria-pressed={!planar} onClick={() => setPlanar(false)}>3D</button><button aria-pressed={planar} onClick={() => setPlanar(true)}>Plan</button></div>
                    </div>
                    <div className="system-workspace"><div className="system-map">
                        {model.nodes.length ? <Suspense fallback={<div className="system-scene-empty">Preparing 3D view…</div>}><Scene model={model} selectedNode={selectedSceneNode} selectedEdge={selection?.edge}
                            onSelectNode={selectNode} onSelectEdge={selectEdge} onExpandNode={id => { toggleGroup(id); focus(id); }} onClearSelection={clearSelection} highlightedPathIndex={view === 'behavior' ? highlightedPathIndex : undefined} resetKey={resetKey} planar={planar} active={active} showConnectionLoad={showConnectionLoad} /></Suspense>
                            : <div className="system-scene-empty">{view === 'behavior' ? filter ? 'No source paths match this filter. Clear it to see the entry point’s returned paths.' : 'No connected source path is available for this entry point.' : cyclesOnly ? 'No component cycles match the current filters in the returned analysis.' : filter ? 'No matching components. Try a broader filter or include unconnected components.' : data.components.length && !includeUnconnected ? 'No connected components match the current view. Change the connection filter or include unconnected components.' : 'No component projection is available within this analysis budget.'}</div>}
                        <div className="system-map-caption"><span>{view === 'behavior' ? 'Arrows are indexed calls, not an execution timeline.' : 'Select to inspect · expand a group for its members'}{showConnectionLoad ? ' · rings show link load' : ''}</span>
                            <button onClick={() => setResetKey(value => value + 1)}>Fit view</button></div>
                    </div><aside className="system-inspector" aria-label="System evidence inspector">
                        {edge ? <><span className="system-eyebrow">{edge.type.replaceAll('_', ' ')}</span><h3>{model.nodes.find(item => item.id === edge.source)?.label ?? data.components.find(item => item.id === edge.source)?.label ?? edge.source} → {model.nodes.find(item => item.id === edge.target)?.label ?? data.components.find(item => item.id === edge.target)?.label ?? edge.target}</h3>
                            {corridorWitnesses.length > 0 && <><p>{corridorWitnesses.length} calls in the selected corridor.</p>{corridorWitnesses.slice(0, listLimit).map(witness => <CallWitness key={witness.edge_id} witness={witness} type={edge.behaviorEdges?.find(item => item.id === witness.edge_id)?.type ?? edge.type} onOpen={open} onNavigate={onNavigate} />)}
                                {corridorWitnesses.length > listLimit && <button onClick={() => setListLimit(value => value + 20)}>More call sites</button>}</>}
                            {selectedDependencies.length > 0 && <><p>{selectedDependencies.reduce((sum, item) => sum + item.count, 0).toLocaleString()} indexed relationships · {selectedDependencies.reduce((sum, item) => sum + item.witnesses.length, 0)} source examples.</p>
                                {selectedDependencies.map(dependency => <div key={`${dependency.source}:${dependency.target}:${dependency.type}`} className="system-connection-evidence"><h4>{dependency.type.replaceAll('_', ' ')} <span>{dependency.count}</span></h4>
                                    {dependency.witnesses.map(witness => <CallWitness key={witness.edge_id} witness={witness} type={dependency.type} onOpen={open} onNavigate={onNavigate} />)}
                                    {!dependency.witnesses.length && <p className="system-small">No source example returned for this relationship.</p>}
                                </div>)}<p className="system-small">Counts describe indexed relationships. Examples may be a sample.</p></>}
                            {pathSource && pathTarget && edge.pathEdge && <CallWitness witness={{ ...edge.pathEdge, edge_id: edge.pathEdge.id, source: pathSource, target: pathTarget }} type={edge.type} onOpen={open} onNavigate={onNavigate} />}
                            {edge.type === 'CALL_REFERENCE' && <p>A callable reference does not establish invocation.</p>}
                        </> : selectedSymbol ? <><span className="system-eyebrow">Step {activeStep + 1} of {path?.nodes.length} · selected path</span><h3>{selectedSymbol.name}</h3><EvidenceSource symbol={selectedSymbol} onOpen={open} />
                            <p>{data.components.find(item => item.id === selectedSymbol.component_id)?.label ?? selectedSymbol.file_path ?? 'Component outside returned summary'}</p>
                            {path?.edges[activeStep] && <CallEvidence evidence={path.edges[activeStep]} caller={selectedSymbol} onNavigate={onNavigate} />}
                            <div className="system-step-controls"><button disabled={activeStep <= 0} onClick={() => selectStep(activeStep - 1)}>← Previous step</button><button disabled={!path || activeStep >= path.nodes.length - 1} onClick={() => selectStep(activeStep + 1)}>Next step →</button></div>
                            {path && <p className="system-small">{handoffs(path.nodes)} component handoffs. This is a connected static witness; execution conditions are not established.</p>}</>
                            : selectedComponent ? <><span className="system-eyebrow">{componentBasis(selectedComponent.basis)}</span><h3>{selectedComponent.label}</h3>
                                {selectedComponent.role === 'test' && <span className="system-test-badge" title={selectedComponent.role_basis}>Test code</span>}
                                <div className="system-selection-actions">{selectedGroup && <button onClick={() => toggleGroup(selectedGroup.id)}>{expandedGroups.includes(selectedGroup.id) ? 'Collapse group' : 'Expand group'}</button>}
                                    <button onClick={() => focus(selectedComponent.id)}>Focus here</button></div>
                                {showConnectionLoad && node && <p className="system-small">{loads.get(node.id)?.links ?? 0} visible links · {loads.get(node.id)?.neighbors ?? 0} connected items in this view.</p>}
                                {selectedGroup && <p className="system-small">{selectedGroup.component_count} code groups share this boundary. Grouping follows source organization; functional responsibilities are not inferred from its name.</p>}
                                {selectedGroup && <label className="system-member-picker">Browse members <select aria-label="Group member" value="" onChange={event => {
                                    const id = event.target.value; if (!id) return;
                                    setExpandedGroups(previous => previous.includes(selectedGroup.id) ? previous : [...previous, selectedGroup.id]);
                                    focus(id); setSelection({ node: `member:${selectedGroup.id}:${id}` });
                                }}><option value="">Choose a component…</option>{(data.overview?.components ?? []).filter(item => selectedGroup.component_ids.includes(item.id)).map(item => <option key={item.id} value={item.id}>{item.label}</option>)}</select></label>}
                                <p>{selectedComponent.member_count.toLocaleString()} symbols across {selectedComponent.file_count.toLocaleString()} files.</p>
                                {(['incoming', 'outgoing'] as const).map(direction => <div className="system-neighbors" key={direction}><h4>{direction === 'incoming' ? 'Used by' : 'Uses'} <span>{neighbors[direction].size}</span></h4>
                                    {neighbors[direction].size ? [...neighbors[direction]].slice(0, 8).map(([id, types]) => <div className="system-neighbor-row" key={id}>
                                        <button onClick={() => selectNode(id)} title={model.nodes.find(item => item.id === id)?.label}><strong>{model.nodes.find(item => item.id === id)?.label ?? id}</strong><small>{[...types].map(type => type.toLowerCase().replaceAll('_', ' ')).join(' · ')}</small></button>
                                        <button className="system-neighbor-evidence" aria-label={`Inspect ${direction} connection ${direction === 'incoming' ? 'from' : 'to'} ${model.nodes.find(item => item.id === id)?.label ?? id}`} onClick={() => {
                                            const connection = model.edges.find(item => direction === 'incoming' ? item.source === id && item.target === selectedComponent.id : item.source === selectedComponent.id && item.target === id);
                                            if (connection) selectEdge(connection.id);
                                        }}>Evidence</button>
                                    </div>) : <p className="system-small">None in the current view.</p>}
                                    {neighbors[direction].size > 8 && <small>{neighbors[direction].size - 8} more in the dependency list below.</small>}
                                </div>)}
                                <details className="system-representatives"><summary>Representative sources</summary>{selectedComponent.representatives.map(symbol => <EvidenceSource key={symbol.id} symbol={symbol} onOpen={open} />)}
                                    <p className="system-small">Sources shown are examples; the membership count includes the whole returned group.</p></details>
                            </> : <><span className="system-eyebrow">Inspect the evidence</span><h3>{view === 'structure' ? 'How the parts connect' : 'A path through the system'}</h3>
                                <p>Select a component, symbol or connection to inspect its source evidence.</p><p>{view === 'structure' ? 'Collaboration groups are inferred from indexed interactions. They do not establish deployment boundaries.' : 'Each arrow follows a recorded relationship. Static paths describe possible interactions, not observed execution.'}</p></>}
                    </aside></div>
                    {(model.omittedNodes > 0 || model.omittedEdges > 0) && <p className="system-limit">{view === 'structure' ? 'This scope has additional detail.' : 'Additional detail is outside this scene.'} {model.omittedNodes} nodes and {model.omittedEdges} connections are outside this scene. Returned findings remain available below.</p>}
                    {view === 'structure' && !data.overview ? <details className="system-findings"><summary>Browse components · {allComponents.length}</summary>
                        <div className="system-component-list">{allComponents.slice(0, listLimit).map(item => <button key={item.id} aria-pressed={model.focusId === item.id} onClick={() => selectNode(item.id)}>
                            <strong>{item.label}</strong><span>{componentBasis(item.basis)}{item.role === 'test' && <> · <span title={item.role_basis}>Test code</span></>}</span><small>{item.member_count.toLocaleString()} symbols · {item.file_count.toLocaleString()} files</small></button>)}</div>
                        {allComponents.length > listLimit && <button className="atlas-arch-action" onClick={() => setListLimit(value => value + 20)}>More components</button>}
                        <details><summary>Directed dependencies · {listedDependencies.length}</summary><div className="system-dependency-list">{listedDependencies.slice(0, listLimit).map(({ dependency: item, index }) => <button key={index} onClick={() => {
                            const visible = model.edges.find(candidate => candidate.dependency === item);
                            if (visible) setSelection({ edge: visible.id }); else setSelection({ dependencyIndex: index });
                        }}>{model.nodes.find(component => component.id === item.source)?.label ?? item.source} → {model.nodes.find(component => component.id === item.target)?.label ?? item.target}<small>{item.type} · {item.count}</small></button>)}</div>
                            {listedDependencies.length > listLimit && <button className="atlas-arch-action" onClick={() => setListLimit(value => value + 20)}>More dependencies</button>}
                        </details>
                    </details> : view === 'behavior' && path && <><div className="system-branch-list" role="group" aria-label="Indexed paths">{paths.map((item, index) => <button key={index} aria-pressed={activePathIndex === index} onClick={() => selectPath(index)}>
                        <span>Path {index + 1}</span><strong>{item.nodes.at(-1)?.name}</strong><small>{item.edges.length} calls · {handoffs(item.nodes)} handoffs</small>
                    </button>)}</div><details className="system-findings"><summary>Connected source path · {path.nodes.length} symbols</summary><ol className="system-path-list">{path.nodes.map((symbol, index) => <li key={`${symbol.id}:${index}`} data-selected={activeStep === index}>
                        <button className="system-step-select" aria-label={`Select step ${index + 1}: ${symbol.name}`} aria-pressed={activeStep === index} onClick={() => selectStep(index)}>{index + 1}</button>
                        <EvidenceSource symbol={symbol} onOpen={open} />{path.edges[index] && <span>↓ {path.edges[index].type.replaceAll('_', ' ')}{symbol.component_id !== path.nodes[index + 1].component_id ? ' · component handoff' : ''}</span>}</li>)}</ol></details></>}
                    {view === 'behavior' && data.behavior && !queryPending && !requestedTarget && <p className="system-small">The destination list shows reachable indexed symbols within {data.behavior.max_hops} calls.
                        {(data.behavior.totals.omitted_targets ?? 0) > 0 && <> This bounded list omits {data.behavior.totals.omitted_targets.toLocaleString()} reachable targets.</>} Select an operation to see its connecting paths and source evidence.</p>}
                    {view === 'behavior' && behavior?.mode === 'corridor' && !paths.length && <p className="system-small" role="status">{returnedPaths.length
                        ? 'No source paths match this filter. The returned call corridor remains visible; clear the filter to inspect its witnesses.'
                        : 'No connected witness was returned within this query’s limits. This does not establish that the operation is unreachable.'}</p>}
                    <EvidenceDetails data={data} view={view} invalidPaths={data.paths.filter(item => !isContiguousPath(item)).length} />
                </>}
    </section>;
}

function EvidenceDetails({ data, view, invalidPaths }: { data: SystemProjection; view: string; invalidPaths: number }) {
    const omitted = Object.entries(data.limits).filter(([key, value]) => (key.startsWith('omitted_') || key.endsWith('_truncated')) && Boolean(value));
    return <details className="system-findings" id={`system-evidence-${view}`} open={invalidPaths > 0}>
        <summary>Evidence and limits</summary><p>This view uses indexed source relationships. Definition links point to indexed declarations; call-site links use recorded invocation locations where available. Changes since indexing are not included. Group boundaries may follow declared modules, inferred interactions or common source directories; the inspector identifies their basis. Component dependency cycles can involve different symbols at each boundary; they do not establish function recursion, runtime loops or a design defect. Behavior paths separately require connected source relationships. Missing relationships can reflect indexing coverage or unresolved bindings.</p>
        <p>{data.totals.accounted_nodes?.toLocaleString() ?? 'Unknown'} nodes assigned to components; {data.totals.structural_nodes?.toLocaleString() ?? 'Unknown'} structural nodes reported separately.</p>
        {omitted.map(([key, value]) => <p key={key}>{key.replaceAll('_', ' ')}: {String(value)}</p>)}
        {data.overview && <p>Overview: {data.overview.totals.components ?? data.overview.components.length} components across {data.overview.groups.length} groups. {data.overview.complete ? 'All analyzed components are represented at this level.' : 'This overview is limited.'} {Object.entries(data.overview.limits).filter(([, value]) => Boolean(value)).map(([key, value]) => `${key.replaceAll('_', ' ')}: ${value}`).join(' · ')}</p>}
        {data.behavior && <p>Static query limited to {data.behavior.max_hops} calls. {data.behavior.limits_hit.join(' · ')}</p>}
        {data.warnings.map((warning, index) => <p key={index}>{warning}</p>)}
        {invalidPaths > 0 && <p>{invalidPaths} disconnected path responses were excluded because their source relationships did not join.</p>}
    </details>;
}

function CallEvidence({ evidence, caller, onNavigate }: { evidence: SystemCallEvidence; caller: SystemSymbol; onNavigate: SystemArchitectureProps['onNavigate'] }) {
    return <div className="system-call-evidence">{evidence.callsite ? <button onClick={() => onNavigate(evidence.callsite!.file_path, evidence.callsite!.line, caller.name)}>Open call site · {evidence.callsite.file_path}:{evidence.callsite.line}</button>
        : <span>Call site unavailable · definition links below</span>}
        {evidence.resolution?.strategy && <small>Resolved by {evidence.resolution.strategy.replaceAll('_', ' ')}{evidence.resolution.candidates !== undefined ? ` · ${evidence.resolution.candidates} candidate${evidence.resolution.candidates === 1 ? '' : 's'}` : ''}</small>}</div>;
}
function CallWitness({ witness, type, onOpen, onNavigate }: { witness: SystemWitness; type: string; onOpen: (symbol: SystemSymbol) => void; onNavigate: SystemArchitectureProps['onNavigate'] }) {
    return <article className="system-witness"><CallEvidence evidence={witness} caller={witness.source} onNavigate={onNavigate} /><EvidenceSource symbol={witness.source} onOpen={onOpen} /><span>→ {type.replaceAll('_', ' ')}</span><EvidenceSource symbol={witness.target} onOpen={onOpen} /></article>;
}
