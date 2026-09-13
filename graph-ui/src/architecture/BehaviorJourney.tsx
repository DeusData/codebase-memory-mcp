import { lazy, Suspense, useEffect, useMemo, useRef, useState } from 'react';
import { behaviorJourney } from './behavior-journey-model';
import type { SystemSceneModel } from './system-architecture-model';
import type { SystemProjection, SystemSymbol } from './system-architecture-source';
import BehaviorSourceEvidence from './BehaviorSourceEvidence';
import './behavior-journey.css';

const Scene = lazy(() => import('./SystemArchitectureScene'));
export interface BehaviorJourneyProps {
    project: string; generation?: string; data?: SystemProjection; entries: SystemSymbol[];
    targets: (SystemSymbol & { distance: number })[]; entryId?: number; targetId?: number;
    active: boolean; pending: boolean; error?: string; filter: string;
    onRequest: (entry: SystemSymbol | undefined, targetId?: number) => void;
    onClearSelection?: () => void;
    onRefresh: () => void; onSelectSymbol: (symbol: SystemSymbol) => void;
    onNavigate: (path: string, line?: number, name?: string) => void;
}

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
    onRequest, onRefresh, onSelectSymbol, onNavigate, onClearSelection }: BehaviorJourneyProps) {
    const [pathIndex, setPathIndex] = useState(0), [step, setStep] = useState(0);
    const [branchPage, setBranchPage] = useState(0);
    const [overview, setOverview] = useState(false);
    const [selection, setSelection] = useState<{ node?: string; edge?: string }>();
    const [planar, setPlanar] = useState(false), [resetKey, setResetKey] = useState(0);
    const [history, setHistory] = useState<{ entry: SystemSymbol; targetId?: number }[]>([]);
    const [targetSearch, setTargetSearch] = useState('');
    const visual = useRef<HTMLDivElement>(null);
    const [pathWindow, setPathWindow] = useState(5);
    useEffect(() => { setSelection(undefined); setPathIndex(0); setStep(0); setBranchPage(0); }, [project, generation, entryId, targetId, filter]);
    useEffect(() => { setHistory([]); setTargetSearch(''); }, [project, generation]);
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
    const matchingTargets = targetOptions.filter(item => item.id === targetId || !targetSearch.trim()
        || `${item.name} ${item.file_path ?? ''}`.toLocaleLowerCase().includes(targetSearch.trim().toLocaleLowerCase()));
    const entry = journey?.entry ?? entries.find(item => item.id === entryId);
    const availableEntries = entry && !entries.some(item => item.id === entry.id) ? [entry, ...entries] : entries;
    const selectedTarget = targetOptions.find(item => item.id === targetId) ?? path?.nodes.at(-1);
    function clearSelection() {
        const origin = history[0]?.entry ?? entry;
        setOverview(true); setSelection(undefined); setStep(0); setBranchPage(0); setPathIndex(0); setHistory([]);
        setResetKey(value => value + 1); onClearSelection?.();
        if (history.length || targetId !== undefined) onRequest(origin);
    }
    function request(next: SystemSymbol | undefined, destination?: number, remember = true) {
        setOverview(false);
        if (remember && entry) setHistory(items => [...items.slice(-19), { entry, targetId }]);
        setSelection(undefined); setStep(0); setPathIndex(0); setTargetSearch(''); onRequest(next, destination);
    }
    function selectNode(id: string) {
        setOverview(false);
        const node = allNodes.find(item => item.id === id);
        setSelection({ node: id });
        if (path && node?.depth !== undefined) setStep(node.depth);
        if (!path && node) setBranchPage(Math.max(0, Math.floor((allNodes.indexOf(node) - 1) / 4)));
        if (node?.symbol) onSelectSymbol(node.symbol);
    }
    function selectEdge(id: string) {
        setOverview(false);
        const edge = journey?.scene.edges.find(item => item.id === id);
        setSelection({ edge: id });
        if (path && edge?.depth) setStep(edge.depth - 1);
        if (!path && edge) setBranchPage(Math.max(0, Math.floor((allNodes.findIndex(node => node.id === edge.target) - 1) / 4)));
    }
    function selectStep(index: number) {
        setOverview(false);
        const node = allNodes[index];
        if (!node || !path) return;
        setStep(index); setSelection({ node: node.id }); if (node.symbol) onSelectSymbol(node.symbol);
    }
    function follow(id: string) {
        const operation = allNodes.find(node => node.id === id)?.symbol;
        if (operation && operation.id !== entryId) request(operation);
    }
    return <section className="system-architecture behavior-journey" aria-label="Behavior" data-testid="behavior-journey" aria-busy={pending}>
        <header className="behavior-heading"><div><span className="system-eyebrow">Behavior · source-guided exploration</span>
            <h2>{targetId !== undefined ? `${entry?.name ?? 'Operation'} → ${selectedTarget?.name ?? 'destination'}` : `What can ${entry?.name ?? 'this operation'} call?`}</h2>
            <p>{path ? 'Follow one recorded call chain across the parts it touches.' : 'Choose a starting operation. Explore its calls, or follow a path to a destination.'}</p></div>
            <button onClick={onRefresh}>Refresh</button></header>
        <div className="behavior-requests">
            <label>Start <select aria-label="Behavior entry point" value={entryId ?? ''} onChange={event => request(availableEntries.find(item => item.id === Number(event.target.value)))}>
                <option value="">Choose an operation…</option>{availableEntries.map(item => <option key={item.id} value={item.id}>{item.name} · {item.file_path ?? item.qualified_name}</option>)}
            </select></label>
            <label>Reach <select aria-label="Behavior destination" value={targetId ?? ''} disabled={!entry || pending} onChange={event => request(entry, event.target.value ? Number(event.target.value) : undefined)}>
                <option value="">Explore immediate calls</option>{matchingTargets.map(item => <option key={item.id} value={item.id}>{item.name} · {item.file_path ?? ''}</option>)}
            </select></label>
            {targetOptions.length > 15 && <input aria-label="Find a destination" placeholder="Find a destination…" value={targetSearch} onChange={event => setTargetSearch(event.target.value)} />}
        </div>
        <div className="behavior-navigation"><button disabled={!history.length} onClick={() => { const previous = history.at(-1)!; setHistory(items => items.slice(0, -1)); request(previous.entry, previous.targetId, false); }}>← Back</button>
            {targetId !== undefined && <button onClick={() => request(entry)}>Immediate calls</button>}
            <span>{pending ? 'Updating this journey…' : path ? `${path.edges.length} call-chain hops · ${new Set(path.nodes.map(item => item.component_id)).size} components`
                : `${journey?.counts.directChoices ?? 0} direct callees with returned evidence`}</span>
            <span className="behavior-static-badge" title="Graph relationships describe possible calls. They do not prove execution order, path feasibility or actual runtime values.">Static evidence</span>
        </div>
        {error && <p role="alert">{error} <button onClick={onRefresh}>Try again</button></p>}
        {pending ? <div className="behavior-loading" role="status">Preparing the selected operation…</div> : !scene || !journey ? <div className="behavior-loading">Choose an operation to explore.</div> : <>
            {journey.paths.length > 1 && <div className="behavior-paths" role="group" aria-label="Indexed paths">{journey.paths.map((item, index) => <button key={index} aria-pressed={pathIndex === index} onClick={() => { setPathIndex(index); setStep(0); setSelection(undefined); }}>
                Path {index + 1}<small>{item.edges.length} hops{new Set(item.nodes.map(node => node.id)).size < item.nodes.length ? ' · revisits a symbol' : ''}</small></button>)}</div>}
            <div className="behavior-stage"><div className="behavior-visual" ref={visual}>
                <div className="behavior-visual-toolbar"><span>{path ? 'CALL CHAIN →' : 'DIRECT CALLS · UNORDERED'}</span>
                    <div className="system-view-switch" role="group" aria-label="Behavior camera"><button aria-pressed={!planar} onClick={() => setPlanar(false)}>3D</button><button aria-pressed={planar} onClick={() => setPlanar(true)}>Plan</button></div>
                    <button onClick={() => setResetKey(value => value + 1)}>Fit</button></div>
                <div className="system-map behavior-map">
                    {scene.nodes.length ? <Suspense fallback={<div className="behavior-loading">Preparing the journey…</div>}><Scene model={scene}
                        selectedNode={selectedEdge ? undefined : selectedNode?.id} selectedEdge={selectedEdge?.id}
                        onSelectNode={selectNode} onSelectEdge={selectEdge} onExpandNode={follow} onClearSelection={clearSelection}
                        resetKey={resetKey} planar={planar} active={active} showConnectionLoad={false} presentation="journey" /></Suspense>
                        : <div className="behavior-loading">{filter ? 'No connected path matches this filter.' : targetId !== undefined ? 'No connected call-chain witness was returned for this destination.' : 'No direct call witnesses were returned for this operation.'}</div>}
                    <div className="system-map-caption"><span>{path ? `Showing operations ${(scene.nodes[0]?.depth ?? 0) + 1} to ${(scene.nodes.at(-1)?.depth ?? 0) + 1} of ${path.nodes.length}` : 'Branches are possible calls, not an execution sequence.'}</span>
                        <span>Double-click an operation to follow its calls</span></div>
                </div>
                {path && <nav className="behavior-walk" aria-label="Walk the call chain"><button disabled={activeStep === 0} onClick={() => selectStep(activeStep - 1)}>← Previous</button>
                    <input type="range" aria-label="Call-chain position" min={0} max={path.nodes.length - 1} value={activeStep} onChange={event => selectStep(Number(event.target.value))} />
                    <span>{activeStep + 1} / {path.nodes.length}</span><button disabled={activeStep >= path.nodes.length - 1} onClick={() => selectStep(activeStep + 1)}>Next →</button></nav>}
                {!path && journey.scene.nodes.length > 5 && <nav className="behavior-walk" aria-label="Direct call pages"><button disabled={branchPage === 0} onClick={() => { setBranchPage(page => page - 1); setSelection(undefined); }}>← Earlier calls</button>
                    <span>Calls {branchPage * 4 + 1} to {Math.min(branchPage * 4 + 4, journey.scene.nodes.length - 1)} of {journey.scene.nodes.length - 1} shown</span>
                    <button disabled={(branchPage + 1) * 4 >= journey.scene.nodes.length - 1} onClick={() => { setBranchPage(page => page + 1); setSelection(undefined); }}>More calls →</button></nav>}
            </div><aside className="behavior-inspector" aria-label="Behavior evidence inspector">
                {caller ? <><span className="system-eyebrow">{call ? call.type.replaceAll('_', ' ') : path ? `Operation ${activeStep + 1}` : 'Starting operation'}</span>
                    <h3>{caller.name}{callee && <><span className="behavior-call-arrow">↓</span>{callee.name}</>}</h3>
                    <p>{call && callee ? caller.component_id === callee.component_id ? 'A call within the same component.' : `Crosses from ${components.get(caller.component_id)?.label ?? caller.component_id} into ${components.get(callee.component_id)?.label ?? callee.component_id}.`
                        : components.get(caller.component_id)?.label ?? caller.file_path}</p>
                    {call?.resolution && <p className="behavior-resolution">{call.resolution.strategy?.replaceAll('_', ' ') ?? 'Indexed resolution'}{call.resolution.candidates && call.resolution.candidates > 1 ? ` · ${call.resolution.candidates} candidate targets` : ''}</p>}
                    <div className="behavior-inspector-actions">{symbol && symbol.id !== entryId && <button onClick={() => request(symbol)}>Follow calls from here</button>}
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
                    <BehaviorSourceEvidence project={project} generation={generation} symbol={caller} call={call} active={active} onNavigate={onNavigate} />
                    {callee && <div className="behavior-next-operation"><span>Continues into</span><button onClick={() => { const node = allNodes.find(node => node.symbol?.id === callee.id && (node.depth ?? 0) > activeStep) ?? allNodes.find(node => node.symbol?.id === callee.id); if (node) selectNode(node.id); }}>{callee.name} →</button></div>}
                    {!path && related.length > 0 && <div className="behavior-immediate-list"><h4>Calls from this operation</h4>{related.map(edge => <button key={edge.id} onClick={() => selectEdge(edge.id)}>
                        {allNodes.find(node => node.id === edge.target)?.symbol?.name ?? edge.target}<small>{edge.pathEdge?.callsite ? `line ${edge.pathEdge.callsite.line}` : edge.type.toLowerCase().replaceAll('_', ' ')}</small></button>)}</div>}
                </> : <p>Select an operation or a call to inspect its evidence.</p>}
            </aside></div>
            {path && <ol className="behavior-operation-strip" aria-label="All operations in this call chain">{path.nodes.map((item, index) => <li key={`${index}:${item.id}`}>
                <button aria-label={`Select step ${index + 1}: ${item.name}`} aria-pressed={!overview && activeStep === index} onClick={() => selectStep(index)}><span>{index + 1}</span><strong>{item.name}</strong><small>{item.file_path}</small></button></li>)}</ol>}
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
