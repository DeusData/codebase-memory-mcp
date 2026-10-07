import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import { behaviorJourney } from './behavior-journey-model';
import { projectionLimits, type SystemSceneModel } from './system-architecture-model';
import { architectureText as text } from './strings';
import type { JourneyPlace, PlaceChange } from './architecture-history';
import { useLiftedPlace, useOnIdentityChange } from './lifted-place';
import type { SystemProjection, SystemSymbol } from './system-architecture-source';
import BehaviorSourceEvidence, { type BehaviorSourceSnapshot } from './BehaviorSourceEvidence';
import { useSelectionEvidence, type SelectionEvidenceListener } from '../galaxy/selection-evidence';
import { RefreshControl, type RefreshFeedback } from '../ui/refresh/refresh-feedback';
import { operationChoices } from './operation-choices';
import './behavior-journey.css';

const startText = text.behaviorStart;

const Scene = lazy(() => import('./SystemArchitectureScene'));
export interface BehaviorJourneyProps {
    project: string; generation?: string; data?: SystemProjection; entries: SystemSymbol[];
    targets: (SystemSymbol & { distance: number })[]; entryId?: number; targetId?: number;
    active: boolean; pending: boolean; error?: string; filter: string;
    onRequest: (entry: SystemSymbol | undefined, targetId?: number, detail?: BehaviorRequestDetail) => void;
    /** The start the followed calls began from (K27); empty background returns there. */
    from?: SystemSymbol;
    /**
     * Where the journey stands (path, operation, page of direct calls) and Plan or 3D, lifted to the workspace
     * for Back and Forward (K27). Rendered on its own, the journey keeps them itself.
     */
    place?: JourneyPlace;
    onPlace?: PlaceChange<JourneyPlace>;
    /** Reports the start shown when none was requested, so a Back or Forward tooltip can name it (K27). */
    onShownStart?: (name: string) => void;
    onClearSelection?: () => void;
    onSelectionEvidence?: SelectionEvidenceListener;
    onRefresh: () => void; onSelectSymbol: (symbol: SystemSymbol) => void;
    /** What the last Refresh did, shown beside its button (hand test 2026-10-04, A3). */
    refresh?: RefreshFeedback;
    /** The automatic start and the ranked suggestions, in their order, offered first in "Start" (hand test 2026-10-04, A4). */
    suggestedEntries?: number[];
    onNavigate: (path: string, line?: number, name?: string) => void;
}

/** What a request carries besides start and destination: where followed calls began, and the destination's name. */
export interface BehaviorRequestDetail { from?: SystemSymbol; targetName?: string }

/** Overlapping windows keep every operation legible and retain actual call adjacency. */
export function journeyPage(scene: SystemSceneModel, step: number, choices = false, pathWindow = 5): SystemSceneModel {
    const size = choices ? 5 : Math.max(2, Math.min(5, pathWindow));
    if (scene.nodes.length <= size) return scene;
    const start = choices ? step * 4 + 1 : Math.min(Math.floor(step / (size - 1)) * (size - 1), Math.max(0, scene.nodes.length - size));
    const window = choices ? [scene.nodes[0], ...scene.nodes.slice(start, start + 4)] : scene.nodes.slice(start, start + size);
    const nodes = choices ? window.map((node, index) => ({ ...node, position: (index === 0 ? [0, 0, 0] : [88, ((window.length - 2) / 2 - index + 1) * 30, 0]) as [number, number, number] })) : window;
    const ids = new Set(nodes.map(node => node.id));
    const lanes = scene.lanes.flatMap(lane => {
        const members = nodes.filter(node => `journey-component:${node.symbol?.component_id}` === lane.id);
        if (!members.length) return [];
        const xs = members.map(node => node.position[0]), ys = members.map(node => node.position[1]);
        return [{ ...lane, position: [(Math.min(...xs) + Math.max(...xs)) / 2, (Math.min(...ys) + Math.max(...ys)) / 2, -2] as [number, number, number],
            width: Math.max(...xs) - Math.min(...xs) + 62, height: Math.max(...ys) - Math.min(...ys) + 32 }];
    });
    return { ...scene, nodes, lanes, edges: scene.edges.filter(edge => ids.has(edge.source) && ids.has(edge.target)), scopeKey: `${scene.scopeKey}:page:${start}` };
}

export default function BehaviorJourney({ project, generation, data, entries, targets, entryId, targetId, active, pending, error, filter,
    onRequest, onRefresh, refresh, suggestedEntries, onSelectSymbol, onNavigate, onClearSelection, onSelectionEvidence, from, place: liftedPlace, onPlace, onShownStart }: BehaviorJourneyProps) {
    const [place, changePlace] = useLiftedPlace<JourneyPlace>(liftedPlace, onPlace, () => ({}));
    const { path: pathIndex = 0, step = 0, page: branchPage = 0, planar = false } = place;
    const [overview, setOverview] = useState(false);
    const [selection, setSelection] = useState<{ node?: string; edge?: string }>();
    const [resetKey, setResetKey] = useState(0);
    const visual = useRef<HTMLDivElement>(null);
    const [pathWindow, setPathWindow] = useState(5);
    const [sourceEvidence, setSourceEvidence] = useState<BehaviorSourceSnapshot>();
    const lifted = liftedPlace !== undefined;
    const top = { path: 0, step: 0, page: 0 };
    // A lifted journey is handed its position with every start, and Back hands the one it had; on its own it starts over.
    useOnIdentityChange(JSON.stringify([project, generation, entryId, targetId]), () => { setSelection(undefined); if (!lifted) changePlace(top); });
    useOnIdentityChange(filter, () => { setSelection(undefined); changePlace(top); });
    const journey = useMemo(() => data ? behaviorJourney(data, { entryId, targetId, pathIndex, filter }) : undefined,
        [data, entryId, targetId, pathIndex, filter]);
    const path = journey?.path;
    const activeStep = Math.min(step, Math.max(0, (path?.nodes.length ?? 1) - 1));
    const scene = useMemo(() => journey ? journeyPage(journey.scene, path ? activeStep : branchPage, !path, pathWindow) : undefined, [journey, path, activeStep, branchPage, pathWindow]);
    const hasScene = Boolean(scene);
    useEffect(() => {
        const element = visual.current;
        if (!element) return;
        const resize = () => { const width = element.getBoundingClientRect().width; if (width > 0) setPathWindow(width < 800 ? 3 : 5); };
        resize();
        if (typeof ResizeObserver === 'undefined') return;
        const observer = new ResizeObserver(resize); observer.observe(element);
        return () => observer.disconnect();
    }, [pending, hasScene]);
    const allNodes = journey?.scene.nodes ?? [];
    const selectedEdge = journey?.scene.edges.find(edge => edge.id === selection?.edge);
    const selectedNode = overview ? undefined : allNodes.find(node => node.id === selection?.node) ?? (path ? allNodes[activeStep] : allNodes[0]);
    const symbol = overview ? undefined : selectedNode?.symbol ?? journey?.entry;
    const call = selectedEdge?.pathEdge ?? (path && !overview && !selection?.edge ? path.edges[activeStep] : undefined);
    const caller = call ? allNodes.find(node => node.symbol?.id === call.source_id)?.symbol : symbol;
    const callee = call ? allNodes.find(node => node.symbol?.id === call.target_id)?.symbol : undefined;
    const components = new Map([...(data?.components ?? []), ...(data?.overview?.components ?? [])].map(component => [component.id, component]));
    const related = symbol ? journey?.scene.edges.filter(edge => edge.pathEdge?.source_id === symbol.id) ?? [] : [];
    const targetOptions = [...new Map([...targets, ...(journey?.choices ?? []).map(item => ({ ...item, distance: item.distance ?? 1 }))]
        .filter(item => item.id !== (entryId ?? journey?.entry?.id)).map(item => [item.id, item])).values()];
    const entry = journey?.entry ?? entries.find(item => item.id === entryId);
    const availableEntries = useMemo(() => entry && !entries.some(item => item.id === entry.id) ? [entry, ...entries] : entries, [entry, entries]);
    const [startQuery, setStartQuery] = useState('');
    const startChoices = useMemo(() => operationChoices(availableEntries, { suggested: suggestedEntries, query: startQuery, keep: entry?.id }),
        [availableEntries, suggestedEntries, startQuery, entry?.id]);
    const reportShown = useRef(onShownStart);
    reportShown.current = onShownStart;
    const shownStart = entryId === undefined && !pending ? entry?.name : undefined;
    useEffect(() => { if (shownStart !== undefined) reportShown.current?.(shownStart); }, [shownStart]);
    const selectedTarget = targetOptions.find(item => item.id === targetId) ?? path?.nodes.at(-1);
    const limits = data?.status === 'limited' ? projectionLimits(data) : [];
    const sourceKey = caller ? JSON.stringify([project, generation, caller.qualified_name, call?.callsite?.file_path ?? caller.file_path, call?.callsite?.line ?? caller.start_line]) : undefined;
    useSelectionEvidence(onSelectionEvidence, !overview && !pending && journey && symbol ? {
        project, generation, view: 'architecture-behavior', source: 'indexed behavior projection and call-site evidence',
        label: caller && callee ? `${caller.name} → ${callee.name}` : symbol.name,
        selected: { operation: symbol, caller, callee, call, component: components.get(symbol.component_id),
            currentSource: sourceEvidence?.key === sourceKey ? { ...sourceEvidence, provenance: 'Current local source; may differ from the indexed snapshot.' } : undefined },
        relationships: path ? { nodes: path.nodes, edges: path.edges, nodeCount: path.nodes.length, edgeCount: path.edges.length }
            : { count: related.length, calls: related.map(edge => edge.pathEdge) },
        scope: { entry, destination: selectedTarget, pathIndex, step: activeStep, mode: path ? 'connected-call-chain' : 'immediate-calls' },
        limitations: { analysis: data?.limits, warnings: data?.warnings, journey: journey.limits, counts: journey.counts,
            interpretation: 'Static call evidence only. Call-chain depth is not execution order. Branch feasibility, parameter binding, runtime values and data transformation are not established.' },
    } : undefined, active);
    /** Empty background leaves a destination and returns from followed calls to where they began. */
    function clearSelection() {
        const origin = from ?? entry;
        setOverview(true); setSelection(undefined); changePlace(top);
        setResetKey(value => value + 1); onClearSelection?.();
        if (from || targetId !== undefined) onRequest(origin, undefined, {});
    }
    /**
     * Every request is a step of the shared Architecture history (K27). `origin` is where followed calls began:
     * kept for a destination and for further hops, dropped for a start picked in the field.
     */
    function request(next: SystemSymbol | undefined, destination?: number, origin?: SystemSymbol) {
        setOverview(false);
        // Lifted, the request itself is the new place and starts at the top.
        setSelection(undefined); if (!lifted) changePlace({ path: 0, step: 0 });
        const targetName = destination === undefined ? undefined : targetOptions.find(item => item.id === destination)?.name;
        onRequest(next, destination, { ...(origin && origin.id !== next?.id ? { from: origin } : {}), ...(targetName ? { targetName } : {}) });
    }
    function selectNode(id: string) {
        setOverview(false);
        const node = allNodes.find(item => item.id === id);
        setSelection({ node: id });
        if (path && node?.depth !== undefined) changePlace({ step: node.depth });
        if (!path && node) changePlace({ page: Math.max(0, Math.floor((allNodes.indexOf(node) - 1) / 4)) });
        if (node?.symbol) onSelectSymbol(node.symbol);
    }
    function selectEdge(id: string) {
        setOverview(false);
        const edge = journey?.scene.edges.find(item => item.id === id);
        setSelection({ edge: id });
        if (path && edge?.depth) changePlace({ step: edge.depth - 1 });
        if (!path && edge) changePlace({ page: Math.max(0, Math.floor((allNodes.findIndex(node => node.id === edge.target) - 1) / 4)) });
    }
    function selectStep(index: number) {
        setOverview(false);
        const node = allNodes[index];
        if (!node || !path) return;
        changePlace({ step: index }); setSelection({ node: node.id }); if (node.symbol) onSelectSymbol(node.symbol);
    }
    function follow(id: string) {
        const operation = allNodes.find(node => node.id === id)?.symbol;
        if (operation && operation.id !== entryId) request(operation, undefined, from ?? entry);
    }
    return <section className="system-architecture behavior-journey" aria-label="Behavior" data-testid="behavior-journey" aria-busy={pending}>
        <header className="behavior-heading"><div><span className="system-eyebrow">Behavior · source-guided exploration</span>
            <h2>{targetId !== undefined ? `${entry?.name ?? 'Operation'} → ${selectedTarget?.name ?? 'destination'}` : `What can ${entry?.name ?? 'this operation'} call?`}</h2>
            <p>{path ? 'Follow one recorded call chain across the parts it touches.' : 'Choose a starting operation. Explore its calls, or follow a path to a destination.'}</p></div>
            <RefreshControl labels={text.refreshFeedback.behavior} feedback={refresh} onRefresh={onRefresh} /></header>
        <div className="behavior-requests">
            {/*
              * The field names the operation the journey shows, also when the projection chose it. Suggestions come
              * first, then every operation alphabetically; the filter before it narrows both (hand test 2026-10-04, A4).
              */}
            <label className="behavior-start">{startText.label}
                <input type="search" aria-label={startText.filter} placeholder={startText.filterPlaceholder} value={startQuery} onChange={event => setStartQuery(event.target.value)} />
                <select aria-label={startText.field} value={entry?.id ?? ''} onChange={event => request(availableEntries.find(item => item.id === Number(event.target.value)))}>
                    <option value="">{startText.choose}</option>
                    {startChoices.current && <optgroup label={startText.current}><option value={startChoices.current.id}>{startChoices.current.label}</option></optgroup>}
                    {startChoices.suggested.length > 0 && <optgroup label={startText.suggested}>{startChoices.suggested.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}</optgroup>}
                    <optgroup label={startQuery.trim() ? startText.matching(startChoices.matching, startChoices.total) : startText.all(startChoices.total)}>
                        {startChoices.all.map(item => <option key={item.id} value={item.id}>{item.label}</option>)}
                        {startQuery.trim() && !startChoices.matching && <option disabled value="-1">{startText.none(startQuery.trim())}</option>}
                    </optgroup>
                </select></label>
            <label>Reach <select aria-label="Behavior destination" value={targetId ?? ''} disabled={!entry || pending} onChange={event => request(entry, event.target.value ? Number(event.target.value) : undefined, from)}>
                <option value="">Explore immediate calls</option>{targetOptions.map(item => <option key={item.id} value={item.id}>{item.name} · {item.file_path ?? ''}</option>)}
            </select></label>
        </div>
        <div className="behavior-navigation">{targetId !== undefined && <button onClick={() => request(entry, undefined, from)}>Immediate calls</button>}
            <span>{pending ? 'Updating this journey…' : path ? `${path.edges.length} call-chain hops · ${new Set(path.nodes.map(item => item.component_id)).size} components`
                : text.directCallees(journey?.counts.directChoices ?? 0, journey?.counts.selfCalls ?? false)}</span>
            <span className="behavior-static-badge" title="Graph relationships describe possible calls. They do not prove execution order, path feasibility or actual runtime values.">Static evidence</span>
        </div>
        {error && <p role="alert">{error} <button onClick={onRefresh}>Try again</button></p>}
        {pending ? <div className="behavior-loading" role="status">Preparing the selected operation…</div> : !scene || !journey ? <div className="behavior-loading">Choose an operation to explore.</div> : <>
            {journey.paths.length > 1 && <div className="behavior-paths" role="group" aria-label="Indexed paths">{journey.paths.map((item, index) => <button key={index} aria-pressed={pathIndex === index} onClick={() => { changePlace({ path: index, step: 0 }); setSelection(undefined); }}>
                Path {index + 1}<small>{item.edges.length} hops{new Set(item.nodes.map(node => node.id)).size < item.nodes.length ? ' · revisits a symbol' : ''}</small></button>)}</div>}
            <div className="behavior-stage"><div className="behavior-visual" ref={visual}>
                <div className="behavior-visual-toolbar"><span>{path ? 'CALL CHAIN →' : 'DIRECT CALLS · UNORDERED'}</span>
                    <div className="system-view-switch" role="group" aria-label="Behavior camera"><button aria-pressed={!planar} onClick={() => changePlace({ planar: false })}>3D</button><button aria-pressed={planar} onClick={() => changePlace({ planar: true })}>Plan</button></div>
                    <button onClick={() => setResetKey(value => value + 1)}>Fit</button></div>
                <div className="system-map behavior-map">
                    {scene.nodes.length ? <Suspense fallback={<div className="behavior-loading">Preparing the journey…</div>}><Scene model={scene}
                        selectedNode={selectedEdge ? undefined : selectedNode?.id} selectedEdge={selectedEdge?.id}
                        onSelectNode={selectNode} onSelectEdge={selectEdge} onExpandNode={follow} onClearSelection={clearSelection}
                        resetKey={resetKey} planar={planar} active={active} showConnectionLoad={false} presentation="journey" /></Suspense>
                        : limits.length ? <div className="behavior-loading" role="status"><div className="behavior-limited"><strong>{text.behaviorLimited}</strong>{limits.map(limit => <p key={limit}>{limit}</p>)}</div></div>
                        : <div className="behavior-loading">{filter ? 'No connected path matches this filter.' : targetId !== undefined ? 'No connected call-chain witness was returned for this destination.' : 'No direct call witnesses were returned for this operation.'}</div>}
                    <div className="system-map-caption"><span>{path ? `Showing operations ${(scene.nodes[0]?.depth ?? 0) + 1} to ${(scene.nodes.at(-1)?.depth ?? 0) + 1} of ${path.nodes.length}` : 'Branches are possible calls, not an execution sequence.'}</span>
                        <span>Double-click an operation to follow its calls</span></div>
                </div>
                {path && <nav className="behavior-walk" aria-label="Walk the call chain"><button disabled={activeStep === 0} onClick={() => selectStep(activeStep - 1)}>← Previous</button>
                    <input type="range" aria-label="Call-chain position" min={0} max={path.nodes.length - 1} value={activeStep} onChange={event => selectStep(Number(event.target.value))} />
                    <span>{activeStep + 1} / {path.nodes.length}</span><button disabled={activeStep >= path.nodes.length - 1} onClick={() => selectStep(activeStep + 1)}>Next →</button></nav>}
                {!path && journey.scene.nodes.length > 5 && <nav className="behavior-walk" aria-label="Direct call pages"><button disabled={branchPage === 0} onClick={() => { changePlace({ page: branchPage - 1 }); setSelection(undefined); }}>← Earlier calls</button>
                    <span>{text.callPage(branchPage * 4 + 1, Math.min(branchPage * 4 + 4, journey.scene.nodes.length - 1), journey.scene.nodes.length - 1,
                        journey.counts.omittedNodes, Boolean(filter.trim()) && journey.scene.nodes.length - 1 + journey.counts.omittedNodes < journey.counts.directChoices)}</span>
                    <button disabled={(branchPage + 1) * 4 >= journey.scene.nodes.length - 1} onClick={() => { changePlace({ page: branchPage + 1 }); setSelection(undefined); }}>More calls →</button></nav>}
            </div><aside className="behavior-inspector" aria-label="Behavior evidence inspector">
                {caller ? <><span className="system-eyebrow">{call ? call.type.replaceAll('_', ' ') : path ? `Operation ${activeStep + 1}` : 'Starting operation'}</span>
                    <h3>{caller.name}{callee && <><span className="behavior-call-arrow">↓</span>{callee.name}</>}</h3>
                    <p>{call && callee ? caller.component_id === callee.component_id ? 'A call within the same component.' : `Crosses from ${components.get(caller.component_id)?.label ?? caller.component_id} into ${components.get(callee.component_id)?.label ?? callee.component_id}.`
                        : components.get(caller.component_id)?.label ?? caller.file_path}</p>
                    {call?.resolution && <p className="behavior-resolution">{call.resolution.strategy?.replaceAll('_', ' ') ?? 'Indexed resolution'}{call.resolution.candidates && call.resolution.candidates > 1 ? ` · ${call.resolution.candidates} candidate targets` : ''}</p>}
                    <div className="behavior-inspector-actions">{symbol && symbol.id !== entryId && <button onClick={() => request(symbol, undefined, from ?? entry)}>Follow calls from here</button>}
                        {symbol?.file_path && <button onClick={() => onNavigate(symbol.file_path!, symbol.start_line, symbol.name)}>Open definition ↗</button>}</div>
                    <div className="behavior-contract" aria-label="Indexed call contract">
                        {call?.arguments && <section><h4>Passed expressions</h4>{call.arguments.length ? <ol>{call.arguments.map((argument, index) => <li key={`${argument.i}:${index}`}><code>{argument.e}</code>
                            {argument.v !== undefined && <small>Resolved string: <code>{JSON.stringify(argument.v)}</code></small>}</li>)}</ol> : <p>No argument expressions were captured.</p>}
                            <small>Indexed expressions{call.argument_limit ? ` · up to ${call.argument_limit}` : ''}. Parameter binding is not established.</small></section>}
                        {(callee ?? symbol)?.signature && <section><h4>{callee ? 'Called declaration' : 'Declaration'}</h4><code className="behavior-signature">{(callee ?? symbol)!.signature}</code></section>}
                        {(callee ?? symbol)?.parameters && <section><h4>Declared parameters</h4><ul>{(callee ?? symbol)!.parameters!.names.map((name, index) => <li key={`${index}:${name}`}><code>{name}{(callee ?? symbol)!.parameters!.types[index] ? `: ${(callee ?? symbol)!.parameters!.types[index]}` : ''}</code></li>)}</ul>
                            <small>{(callee ?? symbol)!.parameters!.count} reported parameters</small></section>}
                        {(callee ?? symbol)?.return_type && <section><h4>Declared return type</h4><code>{(callee ?? symbol)!.return_type}</code></section>}
                    </div>
                    <BehaviorSourceEvidence project={project} generation={generation} symbol={caller} call={call} active={active} onNavigate={onNavigate} onSourceEvidence={setSourceEvidence} />
                    {callee && <div className="behavior-next-operation"><span>Continues into</span><button onClick={() => { const node = allNodes.find(node => node.symbol?.id === callee.id && (node.depth ?? 0) > activeStep) ?? allNodes.find(node => node.symbol?.id === callee.id); if (node) selectNode(node.id); }}>{callee.name} →</button></div>}
                    {!path && related.length > 0 && <div className="behavior-immediate-list"><h4>Calls from this operation</h4>{related.map(edge => <button key={edge.id} onClick={() => selectEdge(edge.id)}>
                        {allNodes.find(node => node.id === edge.target)?.symbol?.name ?? edge.target}<small>{edge.pathEdge?.callsite ? `line ${edge.pathEdge.callsite.line}` : edge.type.toLowerCase().replaceAll('_', ' ')}</small></button>)}</div>}
                </> : <p>Select an operation or a call to inspect its evidence.</p>}
            </aside></div>
            {path && <details className="behavior-limits"><summary>Call chain · {path.nodes.length} operations</summary><ol className="behavior-operation-strip" aria-label="All operations in this call chain">{path.nodes.map((item, index) => <li key={`${index}:${item.id}`}>
                <button aria-label={`Select step ${index + 1}: ${item.name}`} aria-pressed={!overview && activeStep === index} onClick={() => selectStep(index)}><span>{index + 1}</span><strong>{item.name}</strong><small>{item.file_path}</small></button></li>)}</ol></details>}
            <details className="behavior-limits"><summary>Evidence and limits{journey.limits.sampled ? ' · partial analysis' : ''}</summary>
                <p>Each arrow is a recorded invocation. Component lanes provide orientation; numbered operations follow call-chain depth. Neither proves execution order, branch feasibility, data transformation or runtime values. Opening a call reads its current local source.</p>
                <p>{journey.counts.omittedChoices} choices, {journey.counts.omittedNodes} nodes and {journey.counts.omittedEdges} edges omitted by display limits. {journey.counts.invalidPaths} disconnected or unsupported paths excluded.</p>
                {(data?.behavior?.totals.omitted_targets ?? 0) > 0 && <p>{data!.behavior!.totals.omitted_targets} reachable destinations are outside the returned catalog.</p>}
                {journey.limits.hit.length > 0 && <p>{journey.limits.hit.join(' · ')}</p>}
                {data?.warnings.map((warning, index) => <p key={index}>{warning}</p>)}
            </details>
        </>}
    </section>;
}
