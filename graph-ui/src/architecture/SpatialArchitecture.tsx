import { Component, Fragment, useEffect, useMemo, useState, type ReactNode } from 'react';
import type { PlaceChange, SpatialPlace } from './architecture-history';
import { useLiftedPlace, useOnIdentityChange } from './lifted-place';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';
import type { GraphData, GraphNode } from '../galaxy/types';
import { graphNodeEvidence, useSelectionEvidence, type SelectionEvidenceListener } from '../galaxy/selection-evidence';
import { ArchitectureScene } from './ArchitectureScene';
import { buildSemanticGraph, semanticEntryPoints, type SemanticNode, type SemanticView } from './semantic-graph';
import { loadRouteGraph, type RouteGraphSnapshot } from './route-graph-source';
import { areaLevels } from './repository-map';
import { architectureText as text } from './strings';
import { collectSourceMetrics, measureSourceNode } from './source-metrics';
import SourceMetricsDetails from './SourceMetricsDetails';
import type { CoverageIndex } from '../app/tree-model';
import { buildHotspotGraph, collectHotspots, hotspotsForNode, hotspotSignals, hotspotIdentity } from './hotspot-map';
import { hotspotAreas } from './hotspot-areas';
import { useViewPreferences } from '../settings/view-preferences';
import { RefreshControl, useRefreshFeedback } from '../ui/refresh/refresh-feedback';
import './spatial-architecture.css';

interface Props {
    project: string; generation?: string; graph: GraphData; overview: ArchitectureOverviewDto;
    view: SemanticView; filter: string; active: boolean; graphNote?: string;
    onSelect?: (node: GraphNode) => void; onClearSelection?: () => void; selectionPanel?: ReactNode;
    onSelectionEvidence?: SelectionEvidenceListener;
    onView: (view: SemanticView) => void;
    /** Routes view: narrows the endpoints, for example to the routes of one group. */
    onFilter?: (filter: string) => void;
    onNavigate: (path: string, line?: number, name?: string) => void;
    coverage?: CoverageIndex;
    /** The opened area or file, the hotspot area and Plan or 3D, lifted to the workspace for Back and Forward (K27). */
    place?: SpatialPlace;
    onPlace?: PlaceChange<SpatialPlace>;
}

class SceneBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
    state = { failed: false };
    static getDerivedStateFromError() { return { failed: true }; }
    render() {
        return this.state.failed ? <div className="spatial-unavailable" role="status">3D rendering is unavailable. Explore the same nodes and relationships in the lists beside the map.</div> : this.props.children;
    }
}

const viewNotes: Record<SemanticView, string> = {
    overview: 'Find the main parts. Select an area to inspect its connections, then open its files.',
    dependencies: 'Follow directed dependencies. Select a connection to see the indexed source behind it.',
    entryPoints: 'Choose a starting point and follow its static call graph. Depth measures call distance, not execution order.',
    routes: 'Explore HTTP and asynchronous connections to indexed endpoints. Registrations without a resolved connection remain visible.',
    hotspots: 'Inspect ranked symbols and their connections. Gravity wells grow with static fan-in; select a finding to read its measured signals.',
};
const relationKinds = ['CALLS', 'IMPORTS', 'USAGE', 'INHERITS', 'IMPLEMENTS', 'DATA_FLOWS'];

export default function SpatialArchitecture({ project, generation, graph, overview, view, filter, active, graphNote, onSelect, onClearSelection, onSelectionEvidence, selectionPanel, onNavigate, onView, onFilter, coverage, place: liftedPlace, onPlace }: Props) {
    const [place, changePlace] = useLiftedPlace<SpatialPlace>(liftedPlace, onPlace, () => ({ planar: false }));
    const { planar } = place;
    // The place keeps the opened area or file and the hotspot area for Back and Forward (K27), but only the view
    // that opened them shows them: Entry points and Endpoints draw the same map without an area, and neither the
    // scene nor the selection evidence for the chat may name one there.
    const opensScope = view === 'overview' || view === 'dependencies';
    const areaPath = opensScope ? place.areaPath : undefined;
    const filePath = opensScope ? place.filePath : undefined;
    const hotspotArea = view === 'hotspots' ? place.hotspotArea : undefined;
    const [entryChoice, setEntryChoice] = useState<{ node: GraphNode; generation?: string }>();
    const [depth, setDepth] = useState(2);
    const [relations, setRelations] = useState<string[]>([]);
    const { preferences, setPreferences } = useViewPreferences(project);
    const { brickHeight: heightMetric, brickColor: colorMetric, fileVisibility, hotspotGravity: showHotspots } = preferences;
    const [inventoryLimit, setInventoryLimit] = useState(24);
    const [resetKey, setResetKey] = useState(0);
    const [selection, setSelection] = useState<{ scope: string; node?: string; edge?: string }>();
    const [routeReading, setRouteReading] = useState<{ key: string; base?: string; snapshot?: RouteGraphSnapshot; error?: string; refreshing?: boolean }>();
    const [routeRevision, setRouteRevision] = useState(0);
    const [memberLimit, setMemberLimit] = useState(5);
    const [includeTestRoutes, setIncludeTestRoutes] = useState(false);
    // A reindex clears the hotspot area; a view that mounts again keeps the place it is handed.
    useOnIdentityChange(`${project}:${generation ?? ''}`, () => { if (place.hotspotArea) changePlace({ hotspotArea: undefined }, true); });
    const routeBase = `${project}:${generation ?? ''}`;
    const routeKey = `${routeBase}:${routeRevision}`;
    useEffect(() => {
        if (view !== 'routes' || !active) return;
        const controller = new AbortController();
        // A refresh of the same index keeps the connections on screen until the new ones arrive (hand test 2026-10-04, A3).
        const kept = (current?: typeof routeReading) => current?.base === routeBase && current.snapshot ? { snapshot: current.snapshot } : {};
        setRouteReading(current => ({ key: routeKey, base: routeBase, ...kept(current), refreshing: true }));
        void loadRouteGraph(project, { signal: controller.signal }).then(snapshot => {
            if (!controller.signal.aborted) setRouteReading({ key: routeKey, base: routeBase, snapshot });
        }).catch((error: unknown) => {
            const message = error instanceof Error ? error.message : String(error);
            if (!controller.signal.aborted) setRouteReading(current => ({ key: routeKey, base: routeBase, ...kept(current), error: message }));
        });
        return () => controller.abort();
    }, [project, routeKey, routeBase, view, active]);
    const routeRefresh = useRefreshFeedback({ key: routeKey, settled: routeReading?.key === routeKey && !routeReading.refreshing,
        value: routeReading?.key === routeKey ? routeReading.snapshot : undefined, error: routeReading?.key === routeKey ? routeReading.error : undefined });
    const entries = useMemo(() => semanticEntryPoints(graph, overview.entryPoints.flatMap(entry => entry.qualifiedName ? [entry.qualifiedName] : [])), [graph, overview.entryPoints]);
    const currentEntry = entries.find(node => entryChoice?.node.qualified_name
        ? node.qualified_name === entryChoice.node.qualified_name && node.file_path === entryChoice.node.file_path
        : entryChoice?.generation === generation && node.id === entryChoice?.node.id && node.file_path === entryChoice.node.file_path) ?? entries[0];
    // A refresh of the same index shows the earlier connections until its own arrive.
    const routeSnapshot = routeReading?.base === routeBase ? routeReading.snapshot : undefined;
    // Which routes are test code is decided by their registration, handler and caller evidence.
    const routesChecking = view === 'routes' && !routeSnapshot && !(routeReading?.key === routeKey && routeReading.error);
    const knownFiles = useMemo(() => [...new Set([...overview.files, ...[...(coverage?.records.values() ?? [])].filter(record => record.kind === 'file').map(record => record.path)])], [overview.files, coverage]);
    const catalog = useMemo(() => collectSourceMetrics(graph, knownFiles), [graph, knownFiles]);
    const hotspotCatalog = useMemo(() => collectHotspots(overview.hotspots, graph), [overview.hotspots, graph]);
    const visibleFiles = useMemo(() => fileVisibility === 'all' || !['overview', 'dependencies'].includes(view) ? undefined : new Set([...catalog.files.values()]
        .filter(file => fileVisibility === 'connected' ? file.connection === 'connected' : file.connection !== 'connected').map(file => file.path)), [catalog, fileVisibility, view]);
    const inventoryFiles = useMemo(() => [...catalog.files.values()].filter(file => !visibleFiles || visibleFiles.has(file.path))
        .sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0), [catalog, visibleFiles]);
    const areas = useMemo(() => hotspotAreas(hotspotCatalog, graph), [hotspotCatalog, graph]);
    const focusedHotspots = useMemo(() => hotspotArea ? collectHotspots(hotspotCatalog.byArea.get(hotspotArea)?.findings ?? [], graph) : hotspotCatalog, [hotspotArea, hotspotCatalog, graph]);
    const model = useMemo(() => view === 'hotspots' ? buildHotspotGraph(graph, focusedHotspots, filter) : buildSemanticGraph(graph, {
        view, areaPath, filePath, entryId: currentEntry?.id, entryQualifiedNames: overview.entryPoints.flatMap(entry => entry.qualifiedName ? [entry.qualifiedName] : []), depth, filter, relations,
        routes: overview.routes, routeSnapshot, knownFiles: [...catalog.files.keys()], visibleFiles, groupRoutes: true, hideTestRoutes: !includeTestRoutes,
    }), [graph, view, areaPath, filePath, currentEntry?.id, depth, filter, relations, overview.routes, overview.entryPoints, routeSnapshot, catalog, visibleFiles, focusedHotspots, includeTestRoutes]);
    const hasSourceBricks = model.nodes.some(node => node.kind === 'area' || node.kind === 'file');
    const scope = `${project}:${generation ?? ''}:${model.scopeKey}:${view}:${filter}:${hotspotArea ?? ''}`;
    const selectedNode = selection?.scope === scope ? model.nodes.find(node => node.id === selection.node) : undefined;
    const selectedEdge = selection?.scope === scope ? model.edges.find(edge => edge.id === selection.edge) : undefined;
    const selectedMeasure = selectedNode ? measureSourceNode(selectedNode, catalog) : undefined;
    // Inside an opened area or file the inspector speaks about that scope, not the repository.
    const openedScope = filePath || areaPath ? filePath
        ? { eyebrow: text.openedFile, title: filePath.split('/').at(-1) ?? filePath, path: filePath, measure: measureSourceNode({ id: `file:${filePath}`, kind: 'file', label: filePath, detail: '', position: [0, 0, 0], count: 0, members: [], filePath }, catalog) }
        : { eyebrow: text.openedArea, title: areaPath!, path: areaPath!, measure: catalog.areas.get(areaPath!) } : undefined;
    const selectedHotspots = selectedNode ? hotspotsForNode(selectedNode, hotspotCatalog) : undefined;
    const nodesById = useMemo(() => new Map(model.nodes.map(node => [node.id, node])), [model.nodes]);
    const edges = selectedNode ? model.edges.filter(edge => edge.source === selectedNode.id || edge.target === selectedNode.id) : model.edges;
    const nodeEvidence = (node: SemanticNode) => ({ id: node.id, kind: node.kind, label: node.label, detail: node.detail,
        filePath: node.filePath, areaPath: node.areaPath, count: node.count, memberCount: node.members.length,
        members: node.members.slice(0, 24).map(graphNodeEvidence), omittedMembers: Math.max(0, node.members.length - 24) });
    const edgeEvidence = (edge: typeof model.edges[number]) => ({ id: edge.id, source: nodesById.get(edge.source)?.label ?? edge.source,
        target: nodesById.get(edge.target)?.label ?? edge.target, type: edge.type, count: edge.count,
        evidence: edge.evidence.slice(0, 24).map(item => ({ ...item, source: graphNodeEvidence(item.source), target: graphNodeEvidence(item.target) })),
        omittedEvidence: Math.max(0, edge.evidence.length - 24) });
    // Inside an opened area, file or hotspot area with nothing selected, the chat explains that scope (K7):
    // its measure, the parts shown, its hotspot findings and the connections between the parts.
    const openedEvidence = () => {
        const scopeNode = (kind: 'area' | 'file', path: string): SemanticNode => ({ id: `${kind}:${path}`, kind, label: path, detail: '', position: [0, 0, 0], count: 0, members: [],
            ...kind === 'area' ? { areaPath: path } : { filePath: path } });
        // Largest first, areas before files: the first parts named are the ones that make up the area.
        const parts = model.nodes.filter(node => (node.kind === 'area' || node.kind === 'file') && !node.external)
            .map(node => ({ node, measure: measureSourceNode(node, catalog) }))
            .sort((a, b) => a.node.kind !== b.node.kind ? a.node.kind === 'area' ? -1 : 1 : (b.measure?.lines ?? -1) - (a.measure?.lines ?? -1) || (a.node.label < b.node.label ? -1 : 1));
        const links = [...model.edges].sort((a, b) => b.count - a.count);
        const outside = model.nodes.filter(node => node.external);
        return {
            selected: { areaPath, filePath, hotspotArea, measurement: openedScope?.measure,
                hotspots: hotspotArea ? hotspotCatalog.byArea.get(hotspotArea) : filePath ? hotspotsForNode(scopeNode('file', filePath), hotspotCatalog) : areaPath ? hotspotsForNode(scopeNode('area', areaPath), hotspotCatalog) : undefined,
                parts: hotspotArea ? [] : parts.slice(0, 24).map(({ node, measure }) => ({ kind: node.kind, label: node.label, files: measure?.files, lines: measure?.lines })),
                partCount: hotspotArea ? 0 : parts.length, outside: hotspotArea ? [] : outside.slice(0, 24).map(node => node.label), outsideCount: hotspotArea ? 0 : outside.length },
            relationships: hotspotArea ? undefined : { count: links.length, items: links.slice(0, 24).map(edge => ({ source: nodesById.get(edge.source)?.label ?? edge.source,
                target: nodesById.get(edge.target)?.label ?? edge.target, type: edge.type, count: edge.count })), omitted: Math.max(0, links.length - 24) },
        };
    };
    const opened = !selectedNode && !selectedEdge && (areaPath || filePath || hotspotArea) ? openedEvidence() : undefined;
    useSelectionEvidence(onSelectionEvidence, selectedNode || selectedEdge || areaPath || filePath || hotspotArea ? {
        project, generation, view: `architecture-${view}`, source: 'indexed repository graph and architecture summary',
        label: selectedNode?.label ?? (selectedEdge ? `${nodesById.get(selectedEdge.source)?.label} → ${nodesById.get(selectedEdge.target)?.label}` : filePath ?? areaPath ?? hotspotArea ?? model.title),
        selected: selectedEdge ? edgeEvidence(selectedEdge) : selectedNode ? { ...nodeEvidence(selectedNode), measurement: selectedMeasure, hotspots: selectedHotspots } : opened?.selected,
        relationships: selectedNode ? { count: edges.length, items: edges.slice(0, 24).map(edgeEvidence), omitted: Math.max(0, edges.length - 24) } : opened?.relationships,
        scope: { view, areaPath, filePath, hotspotArea, visibleNodes: model.nodes.length, visibleEdges: model.edges.length },
        limitations: { omittedNodes: model.omittedNodes, omittedEdges: model.omittedEdges, warnings: model.warnings,
            graphNote, interpretation: 'Source areas group source locations. Hotspots measure static references, not runtime frequency. Relationships do not prove execution.' },
    } : undefined, active);
    const selectNode = (id: string) => {
        setSelection({ scope, node: id }); setMemberLimit(5);
        const node = nodesById.get(id);
        if (node?.graphNode) onSelect?.(node.graphNode);
    };
    const openGroup = (node: SemanticNode) => {
        if (node.kind === 'area') changePlace({ areaPath: node.areaPath, filePath: undefined });
        else if (node.kind === 'file') changePlace({ filePath: node.filePath });
        if (view === 'routes' || view === 'hotspots') onView('overview');
        setSelection(undefined);
    };
    const clearScope = () => { changePlace({ hotspotArea: undefined, areaPath: undefined, filePath: undefined }); setSelection(undefined); setResetKey(value => value + 1); onClearSelection?.(); };
    const openSource = (node: GraphNode) => node.file_path && onNavigate(node.file_path, node.start_line, node.name);
    const inspectMember = (node: GraphNode) => {
        const current = graph.nodes.find(candidate => candidate.qualified_name && candidate.qualified_name === node.qualified_name && candidate.file_path === node.file_path);
        if (current) onSelect?.(current);
        openSource(node);
    };
    return <section className="spatial-architecture" data-testid="spatial-architecture" aria-label="Spatial architecture explorer">
        <div className="spatial-heading"><div><span className="spatial-eyebrow">Repository atlas / {view === 'entryPoints' ? 'entry points' : view}</span><h2>{model.title}</h2><p>{viewNotes[view]}</p></div>
            <div className="spatial-camera-controls" role="group" aria-label="Map camera">
                <button aria-pressed={!planar} onClick={() => changePlace({ planar: false })}>3D</button><button aria-pressed={planar} onClick={() => changePlace({ planar: true })}>Plan</button>
                <button onClick={() => setResetKey(value => value + 1)}>{text.fitMap}</button>
            </div>
        </div>
        <div className="spatial-controls">
            {['overview', 'dependencies', 'entryPoints'].includes(view) && <div className="spatial-projection-modes" role="group" aria-label="Overview mode"><button aria-pressed={view !== 'entryPoints'} onClick={() => onView('overview')}>Structure</button><button aria-pressed={view === 'entryPoints'} onClick={() => onView('entryPoints')}>Entry points</button></div>}
            {(view === 'overview' || view === 'dependencies') && <nav aria-label="Architecture location"><button onClick={clearScope}>{project}</button>
                {areaPath && areaLevels(areaPath).map((level, index, levels) => <Fragment key={level}><span>/</span><button onClick={() => { changePlace({ areaPath: level, filePath: undefined }); setSelection(undefined); }}>{index ? level.slice(levels[index - 1].length + 1) : level}</button></Fragment>)}{filePath && <><span>/</span><span>{filePath.split('/').at(-1)}</span></>}
            </nav>}
            {view === 'entryPoints' && <><label>Start <select aria-label="Entry point" value={currentEntry?.id ?? ''} onChange={event => { const node = entries.find(node => node.id === Number(event.target.value)); setEntryChoice(node ? { node, generation } : undefined); setSelection(undefined); }}>
                {!entries.length && <option value="">No indexed entry points</option>}{entries.map(node => <option key={node.id} value={node.id}>{node.name} · {node.file_path}</option>)}
            </select></label><label>Call depth <select aria-label="Call depth" value={depth} onChange={event => setDepth(Number(event.target.value))}>{[1, 2, 3, 4].map(value => <option key={value}>{value}</option>)}</select></label></>}
            {view === 'hotspots' && hotspotArea && <nav aria-label="Hotspot area"><button onClick={clearScope}>All hotspots</button><span>/ {hotspotArea}</span></nav>}
            {view === 'routes' && <RefreshControl labels={text.refreshFeedback.routes} feedback={routeRefresh.feedback}
                onRefresh={() => { routeRefresh.begin(); setRouteRevision(value => value + 1); }} />}
            {view === 'routes' && <label className="spatial-gravity-toggle"><input type="checkbox" checked={includeTestRoutes} onChange={event => { setIncludeTestRoutes(event.target.checked); setSelection(undefined); }} />{routesChecking ? text.includeTestRoutesChecking : text.includeTestRoutes(model.hiddenRoutes ?? 0)}</label>}
            {filePath && (view === 'overview' || view === 'dependencies') && <button onClick={() => onNavigate(filePath, 1)}>Read this file</button>}
            {(view === 'overview' || view === 'dependencies') && <div className="spatial-relations" role="group" aria-label="Relationship types">
                <button aria-pressed={!relations.length} onClick={() => setRelations([])}>All</button>{relationKinds.map(type => <button key={type} aria-pressed={relations.includes(type)} onClick={() => setRelations(current => current.includes(type) ? current.filter(item => item !== type) : [...current, type])}>{type.replaceAll('_', ' ').toLowerCase()}</button>)}
            </div>}
        </div>
        <div className="spatial-encoding-controls" aria-label="Map appearance">
            {hasSourceBricks && <label>Height <select aria-label="Brick height" value={heightMetric} onChange={event => setPreferences({ brickHeight: event.target.value as 'lines' | 'uniform' })}><option value="lines">Source size</option><option value="uniform">Uniform</option></select></label>}
            <label>Color <select aria-label="Brick color" value={colorMetric} onChange={event => setPreferences({ brickColor: event.target.value as 'language' | 'kind' })}><option value="language">Language</option><option value="kind">Node kind</option></select></label>
            <label className="spatial-gravity-toggle"><input aria-label="Hotspot gravity" type="checkbox" checked={showHotspots} onChange={event => setPreferences({ hotspotGravity: event.target.checked })} />Hotspot gravity</label>
            {(view === 'overview' || view === 'dependencies') && <label>Files <select aria-label="File visibility" value={fileVisibility} onChange={event => { setPreferences({ fileVisibility: event.target.value as typeof fileVisibility }); setSelection(undefined); setInventoryLimit(24); }}><option value="all">All known files</option><option value="connected">With connections</option><option value="unconnected">Without known connections</option></select></label>}
            {colorMetric === 'language' && <div className="spatial-language-legend" aria-label="Language colors">{catalog.total.languages.slice(0, 5).map(language => <span key={language.name}><i style={{ background: language.color }} />{language.name}</span>)}{catalog.total.languages.length > 5 && <span>+{catalog.total.languages.length - 5} types</span>}</div>}
        </div>
        <div className="spatial-map-layout">
            <div className="spatial-map"><SceneBoundary key={project}>
                {model.nodes.length ? <ArchitectureScene model={model} selectedId={selectedNode?.id} selectedEdgeId={selectedEdge?.id} onSelect={selectNode}
                    onSelectEdge={id => { setSelection({ scope, edge: id }); setMemberLimit(5); }} onOpen={id => { const node = nodesById.get(id); if (node?.routePrefix) onFilter?.(node.routePrefix); else if (node && (node.kind === 'area' || node.kind === 'file')) openGroup(node); }} onClearSelection={clearScope} active={active} planar={planar} resetKey={resetKey} catalog={catalog} heightMetric={heightMetric} colorMetric={colorMetric} hotspots={hotspotCatalog} showHotspots={showHotspots}
                    adaptiveLabels={view === 'overview' || view === 'dependencies' || view === 'routes'} />
                    : <div className="spatial-unavailable">{filter ? 'No matching graph evidence. Try a broader filter.' : view === 'hotspots' ? 'No ranked hotspot measurements are available in this snapshot.' : view === 'entryPoints' ? 'No indexed entry points in this snapshot.' : view === 'routes' ? 'No endpoint evidence is available in this snapshot.' : filePath ? 'No indexed symbols in this file. Use “Read this file” to inspect its source.' : 'No source nodes are available in this repository snapshot.'}</div>}
            </SceneBoundary><div className="spatial-map-caption"><span title={model.positionMeaning}>{view === 'hotspots' ? 'Gravity: incoming references' : view === 'entryPoints' ? 'Static call paths' : 'Source folders and indexed relationships'}</span><span>Drag to orbit · scroll to zoom · select to inspect</span></div></div>
            <aside className="spatial-inspector" aria-label="Architecture inspector">
                {selectedEdge ? <><span className="spatial-eyebrow">Indexed connection</span><h3>{nodesById.get(selectedEdge.source)?.label} <span>→</span> {nodesById.get(selectedEdge.target)?.label}</h3>
                    <p>{selectedEdge.type} · {selectedEdge.count} retained relationships</p>
                    <div className="spatial-evidence">{selectedEdge.evidence.slice(0, memberLimit).map((evidence, index) => <article key={`${evidence.id}:${index}`}>
                        <button disabled={!evidence.source.file_path} onClick={() => openSource(evidence.source)}>{evidence.source.name}</button><span>→ {evidence.type}</span>
                        <button disabled={!evidence.target.file_path} onClick={() => openSource(evidence.target)}>{evidence.target.name}</button>
                        {evidence.routePath && <code>{evidence.routePath}</code>}<small>{evidence.source.file_path}{evidence.source.start_line ? `:${evidence.source.start_line}` : ''}</small>
                        <small>{evidence.id === undefined ? 'Index relationship' : `Edge #${evidence.id}`}{evidence.strategy ? ` · ${evidence.strategy}` : ''}{evidence.via ? ` · ${evidence.via}` : ''}</small>
                    </article>)}</div>{selectedEdge.evidence.length > memberLimit && <button onClick={() => setMemberLimit(value => value + 16)}>More source evidence</button>}
                </> : selectedNode ? <><span className="spatial-eyebrow">{selectedNode.kind === 'area' ? 'Inferred source area' : selectedNode.kind}</span><h3>{selectedNode.label}</h3><p>{selectedNode.detail}</p>
                    {selectedMeasure && <details className="spatial-selection-details"><summary>{selectedMeasure.files.toLocaleString()} files · {selectedMeasure.lines?.toLocaleString() ?? "Unknown"} indexed lines</summary><SourceMetricsDetails measure={selectedMeasure} catalog={catalog} /></details>}
                    {selectedHotspots && <details className="spatial-hotspot-details"><summary>{selectedHotspots.findings.length} hotspot findings · peak fan-in {selectedHotspots.maxFanIn ?? 'unknown'}</summary><p>Static incoming references, not runtime frequency.</p>{selectedHotspots.findings.slice(0, memberLimit).map(finding => <article key={hotspotIdentity(finding)}><button disabled={!finding.filePath} onClick={() => onNavigate(finding.filePath!, finding.line, finding.name)}>{finding.name}</button><div>{hotspotSignals(finding).map(signal => <span key={signal}>{signal}</span>)}</div></article>)}{selectedHotspots.findings.length > memberLimit && <button onClick={() => setMemberLimit(value => value + 16)}>More hotspot findings</button>}</details>}
                    {(selectedNode.kind === 'area' || selectedNode.kind === 'file') && <button className="spatial-primary" onClick={() => openGroup(selectedNode)}>Open {selectedNode.kind === 'area' ? 'area' : 'file symbols'} →</button>}
                    {selectedNode.routePrefix && onFilter && <button className="spatial-primary" onClick={() => onFilter(selectedNode.routePrefix!)}>{text.showRoutes(selectedNode.count)}</button>}
                    {selectedNode.filePath && <button onClick={() => onNavigate(selectedNode.filePath!, selectedNode.line, selectedNode.label)}>Open source</button>}
                    {!!selectedNode.members.length && <details className="spatial-selection-details"><summary>Source members · {selectedNode.members.length}</summary><div className="spatial-members">{selectedNode.members.slice(0, memberLimit).map(node => <button key={`${node.qualified_name}:${node.id}`} onClick={() => inspectMember(node)}>{node.name}<small>{node.file_path}{node.start_line ? `:${node.start_line}` : ''}</small></button>)}</div>
                        {selectedNode.members.length > memberLimit && <button onClick={() => setMemberLimit(value => value + 16)}>More members</button>}</details>}
                    {selectedNode.graphNode && selectionPanel}
                </> : openedScope ? <><span className="spatial-eyebrow">{openedScope.eyebrow}</span><h3 title={openedScope.path}>{openedScope.title}</h3>
                    {openedScope.measure && <details className="spatial-selection-details"><summary>{text.scopeMeasure(openedScope.measure.files, openedScope.measure.lines)}</summary><SourceMetricsDetails measure={openedScope.measure} catalog={catalog} /></details>}
                    <p>{text.openedScopeHint}</p>
                </> : <><span className="spatial-eyebrow">{view === 'hotspots' ? 'Review areas' : 'Repository'}</span><h3>{view === 'hotspots' ? `${hotspotCatalog.findings.length} hotspot findings` : `${catalog.files.size.toLocaleString()} files`}</h3>
                    {view === 'hotspots' ? <><p>Grouped by source area. Outside dependents are distinct files with direct incoming relationships in the loaded graph.</p><div className="spatial-area-summary" aria-label="Hotspots by source area">{areas.slice(0, 5).map(area => <button key={area.path} aria-pressed={hotspotArea === area.path} onClick={() => { changePlace({ hotspotArea: area.path }); setSelection(undefined); setResetKey(value => value + 1); }}><strong>{area.path}</strong><small>{area.findings} findings · {area.files} files</small><small>{area.dependentFiles} outside dependent files</small></button>)}</div>{areas.length > 5 && <details className="spatial-selection-details"><summary>{areas.length - 5} more areas</summary><div className="spatial-area-summary">{areas.slice(5).map(area => <button key={area.path} onClick={() => { changePlace({ hotspotArea: area.path }); setSelection(undefined); setResetKey(value => value + 1); }}><strong>{area.path}</strong><small>{area.findings} findings · {area.dependentFiles} outside dependent files</small></button>)}</div></details>}</>
                        : <p>Select a part or connection to inspect it. Open an area to explore its files.</p>}
                </>}
                <details className="spatial-node-list"><summary>Browse map · {model.nodes.length} parts</summary>{model.nodes.map(node => <button key={node.id} aria-pressed={selectedNode?.id === node.id} onClick={() => selectNode(node.id)}><span>{node.label}</span><small>{node.kind} · {node.count}</small></button>)}</details>
            </aside>
        </div>
        <div className="spatial-bottom"><span>{model.nodes.length} parts · {model.edges.length} connections</span><span>{model.omittedNodes || model.omittedEdges ? 'Partial map · open an area to see more' : 'Select to inspect · double-click to explore'}</span></div>
        {view === 'routes' && !routeSnapshot && <p role="status" className="spatial-notice">{routeReading?.key === routeKey && routeReading.error ? `Connections unavailable: ${routeReading.error}` : 'Reading indexed endpoint connections…'}</p>}
        <details className="spatial-map-details"><summary>Browse files, connections and map details</summary>
        <details className="spatial-file-inventory"><summary>File inventory · {catalog.files.size.toLocaleString()} known files</summary>
            <p>Every returned file remains reachable here, even without connections or outside the 3D display limit. Unindexed folders may contain additional files that the index has not listed.</p>
            <div className="spatial-inventory-list">{inventoryFiles.slice(0, inventoryLimit).map(file => <article key={file.path}><button onClick={() => { changePlace({ areaPath: undefined, filePath: file.path }); setSelection(undefined); onView('overview'); }}>{file.path}</button><small>{file.language} · {file.lines === undefined ? 'size unknown' : `${file.lines.toLocaleString()} indexed lines`} · {file.connection === 'connected' ? 'connected' : file.connection === 'none' ? 'no indexed connections' : 'connections unknown'}{coverage?.records.get(file.path) ? ` · ${coverage.records.get(file.path)!.state}` : ''}</small><button aria-label={`Read ${file.path}`} onClick={() => onNavigate(file.path, 1)}>Read source</button></article>)}</div>
            <small>{Math.min(inventoryLimit, inventoryFiles.length)} / {inventoryFiles.length} files</small>{inventoryLimit < inventoryFiles.length && <button onClick={() => setInventoryLimit(value => value + 24)}>More files</button>}
            {(overview.symbolKinds.find(kind => kind.kind === 'File')?.count ?? 0) > overview.files.length && <p className="spatial-notice">The file inventory query is limited. Additional files are available through the explorer tree.</p>}
            {coverage?.truncations.map(note => <p key={note} className="spatial-notice">{note}</p>)}
        </details>
        {!!(model.omittedNodes || model.omittedEdges) && <p className="spatial-notice">The map is bounded for readability. {model.omittedNodes} nodes and {model.omittedEdges} connections are outside this view; open an area to inspect a smaller scope.</p>}
        {model.warnings.map(warning => <p className="spatial-notice" key={warning}>{warning}</p>)}
        <details className="spatial-relationship-list"><summary>Connections{selectedNode ? ` for ${selectedNode.label}` : ''} · {edges.length}</summary><div aria-label="Architecture relationships">
            {!edges.length && <p>No matching indexed connections in this view.</p>}{edges.map(edge => <button key={edge.id} aria-pressed={selectedEdge?.id === edge.id} onClick={() => { setSelection({ scope, edge: edge.id }); setMemberLimit(5); }}>
                <span>{nodesById.get(edge.source)?.label} → {nodesById.get(edge.target)?.label}</span><small>{edge.type} · {edge.count}</small></button>)}
        </div></details>
        <p className="spatial-measure-note">Height represents source size; color follows file type. Gravity represents static fan-in. {catalog.total.measuredFiles.toLocaleString()} / {catalog.total.files.toLocaleString()} files have line measurements{catalog.partial ? '; totals cover measured files only' : ''}.</p>
        {graphNote && <details className="spatial-coverage"><summary>Index coverage and limits</summary><p>{graphNote}</p></details>}
        </details>
    </section>;
}
