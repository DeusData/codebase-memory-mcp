import { Component, useEffect, useMemo, useState, type ReactNode } from 'react';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';
import type { GraphData, GraphNode } from '../galaxy/types';
import { ArchitectureScene } from './ArchitectureScene';
import { buildSemanticGraph, semanticEntryPoints, type SemanticNode, type SemanticView } from './semantic-graph';
import { loadRouteGraph, type RouteGraphSnapshot } from './route-graph-source';
import { collectSourceMetrics, measureSourceNode } from './source-metrics';
import SourceMetricsDetails from './SourceMetricsDetails';
import type { CoverageIndex } from '../app/tree-model';
import { buildHotspotGraph, collectHotspots, hotspotsForNode, hotspotSignals, hotspotIdentity, gravityPercent } from './hotspot-map';
import './spatial-architecture.css';

interface Props {
    project: string; generation?: string; graph: GraphData; overview: ArchitectureOverviewDto;
    view: SemanticView; filter: string; active: boolean; graphNote?: string;
    onSelect?: (node: GraphNode) => void; onClearSelection?: () => void; selectionPanel?: ReactNode;
    onView: (view: SemanticView) => void;
    onNavigate: (path: string, line?: number, name?: string) => void;
    coverage?: CoverageIndex;
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

export default function SpatialArchitecture({ project, generation, graph, overview, view, filter, active, graphNote, onSelect, onClearSelection, selectionPanel, onNavigate, onView, coverage }: Props) {
    const [areaPath, setAreaPath] = useState<string>();
    const [filePath, setFilePath] = useState<string>();
    const [entryChoice, setEntryChoice] = useState<{ node: GraphNode; generation?: string }>();
    const [depth, setDepth] = useState(2);
    const [relations, setRelations] = useState<string[]>([]);
    const [planar, setPlanar] = useState(false);
    const [heightMetric, setHeightMetric] = useState<'lines' | 'uniform'>('lines');
    const [colorMetric, setColorMetric] = useState<'language' | 'kind'>('language');
    const [fileVisibility, setFileVisibility] = useState<'all' | 'connected' | 'unconnected'>('all');
    const [inventoryFilter, setInventoryFilter] = useState('');
    const [inventoryLimit, setInventoryLimit] = useState(24);
    const [showHotspots, setShowHotspots] = useState(true);
    const [resetKey, setResetKey] = useState(0);
    const [selection, setSelection] = useState<{ scope: string; node?: string; edge?: string }>();
    const [routeReading, setRouteReading] = useState<{ key: string; snapshot?: RouteGraphSnapshot; error?: string }>();
    const [routeRevision, setRouteRevision] = useState(0);
    const [memberLimit, setMemberLimit] = useState(16);
    const routeKey = `${project}:${generation ?? ''}:${routeRevision}`;
    useEffect(() => {
        if (view !== 'routes' || !active) return;
        const controller = new AbortController();
        setRouteReading({ key: routeKey });
        void loadRouteGraph(project, { signal: controller.signal }).then(snapshot => {
            if (!controller.signal.aborted) setRouteReading({ key: routeKey, snapshot });
        }).catch((error: unknown) => {
            if (!controller.signal.aborted) setRouteReading({ key: routeKey, error: error instanceof Error ? error.message : String(error) });
        });
        return () => controller.abort();
    }, [project, routeKey, view, active]);
    const entries = useMemo(() => semanticEntryPoints(graph, overview.entryPoints.flatMap(entry => entry.qualifiedName ? [entry.qualifiedName] : [])), [graph, overview.entryPoints]);
    const currentEntry = entries.find(node => entryChoice?.node.qualified_name
        ? node.qualified_name === entryChoice.node.qualified_name && node.file_path === entryChoice.node.file_path
        : entryChoice?.generation === generation && node.id === entryChoice?.node.id && node.file_path === entryChoice.node.file_path) ?? entries[0];
    const routeSnapshot = routeReading?.key === routeKey ? routeReading.snapshot : undefined;
    const knownFiles = useMemo(() => [...new Set([...overview.files, ...[...(coverage?.records.values() ?? [])].filter(record => record.kind === 'file').map(record => record.path)])], [overview.files, coverage]);
    const catalog = useMemo(() => collectSourceMetrics(graph, knownFiles), [graph, knownFiles]);
    const hotspotCatalog = useMemo(() => collectHotspots(overview.hotspots, graph), [overview.hotspots, graph]);
    const visibleFiles = useMemo(() => fileVisibility === 'all' || !['overview', 'dependencies'].includes(view) ? undefined : new Set([...catalog.files.values()]
        .filter(file => fileVisibility === 'connected' ? file.connection === 'connected' : file.connection !== 'connected').map(file => file.path)), [catalog, fileVisibility, view]);
    const inventoryFiles = useMemo(() => [...catalog.files.values()].filter(file => (!visibleFiles || visibleFiles.has(file.path))
        && file.path.toLowerCase().includes(inventoryFilter.trim().toLowerCase())).sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0), [catalog, visibleFiles, inventoryFilter]);
    const model = useMemo(() => view === 'hotspots' ? buildHotspotGraph(graph, hotspotCatalog, filter) : buildSemanticGraph(graph, {
        view, areaPath, filePath, entryId: currentEntry?.id, entryQualifiedNames: overview.entryPoints.flatMap(entry => entry.qualifiedName ? [entry.qualifiedName] : []), depth, filter, relations,
        routes: overview.routes, routeSnapshot, knownFiles: [...catalog.files.keys()], visibleFiles,
    }), [graph, view, areaPath, filePath, currentEntry?.id, depth, filter, relations, overview.routes, overview.entryPoints, routeSnapshot, catalog, visibleFiles, hotspotCatalog]);
    const hasSourceBricks = model.nodes.some(node => node.kind === 'area' || node.kind === 'file');
    const scope = `${project}:${generation ?? ''}:${model.scopeKey}:${view}:${filter}`;
    const selectedNode = selection?.scope === scope ? model.nodes.find(node => node.id === selection.node) : undefined;
    const selectedEdge = selection?.scope === scope ? model.edges.find(edge => edge.id === selection.edge) : undefined;
    const selectedMeasure = selectedNode ? measureSourceNode(selectedNode, catalog) : undefined;
    const selectedHotspots = selectedNode ? hotspotsForNode(selectedNode, hotspotCatalog) : undefined;
    const nodesById = useMemo(() => new Map(model.nodes.map(node => [node.id, node])), [model.nodes]);
    const edges = selectedNode ? model.edges.filter(edge => edge.source === selectedNode.id || edge.target === selectedNode.id) : model.edges;
    const selectNode = (id: string) => {
        setSelection({ scope, node: id }); setMemberLimit(16);
        const node = nodesById.get(id);
        if (node?.graphNode) onSelect?.(node.graphNode);
    };
    const openGroup = (node: SemanticNode) => {
        if (node.kind === 'area') { setAreaPath(node.areaPath); setFilePath(undefined); }
        else if (node.kind === 'file') setFilePath(node.filePath);
        if (view === 'routes' || view === 'hotspots') onView('overview');
        setSelection(undefined);
    };
    const clearScope = () => { setAreaPath(undefined); setFilePath(undefined); setSelection(undefined); setResetKey(value => value + 1); onClearSelection?.(); };
    const openSource = (node: GraphNode) => node.file_path && onNavigate(node.file_path, node.start_line, node.name);
    const inspectMember = (node: GraphNode) => {
        const current = graph.nodes.find(candidate => candidate.qualified_name && candidate.qualified_name === node.qualified_name && candidate.file_path === node.file_path);
        if (current) onSelect?.(current);
        openSource(node);
    };
    return <section className="spatial-architecture" data-testid="spatial-architecture" aria-label="Spatial architecture explorer">
        <div className="spatial-heading"><div><span className="spatial-eyebrow">Repository atlas / {view === 'entryPoints' ? 'entry points' : view}</span><h2>{model.title}</h2><p>{viewNotes[view]}</p></div>
            <div className="spatial-camera-controls" role="group" aria-label="Map camera">
                <button aria-pressed={!planar} onClick={() => setPlanar(false)}>3D</button><button aria-pressed={planar} onClick={() => setPlanar(true)}>Plan</button>
                <button onClick={() => setResetKey(value => value + 1)}>Fit map</button>
            </div>
        </div>
        <div className="spatial-controls">
            {['overview', 'dependencies', 'entryPoints'].includes(view) && <div className="spatial-projection-modes" role="group" aria-label="Overview mode"><button aria-pressed={view !== 'entryPoints'} onClick={() => onView('overview')}>Structure</button><button aria-pressed={view === 'entryPoints'} onClick={() => onView('entryPoints')}>Entry points</button></div>}
            {(view === 'overview' || view === 'dependencies') && <nav aria-label="Architecture location"><button onClick={clearScope}>{project}</button>
                {areaPath && <><span>/</span><button onClick={() => { setFilePath(undefined); setSelection(undefined); }}>{areaPath}</button></>}{filePath && <><span>/</span><span>{filePath.split('/').at(-1)}</span></>}
            </nav>}
            {view === 'entryPoints' && <><label>Start <select aria-label="Entry point" value={currentEntry?.id ?? ''} onChange={event => { const node = entries.find(node => node.id === Number(event.target.value)); setEntryChoice(node ? { node, generation } : undefined); setSelection(undefined); }}>
                {!entries.length && <option value="">No indexed entry points</option>}{entries.map(node => <option key={node.id} value={node.id}>{node.name} · {node.file_path}</option>)}
            </select></label><label>Call depth <select aria-label="Call depth" value={depth} onChange={event => setDepth(Number(event.target.value))}>{[1, 2, 3, 4].map(value => <option key={value}>{value}</option>)}</select></label></>}
            {view === 'routes' && <button onClick={() => setRouteRevision(value => value + 1)}>Refresh connections</button>}
            {filePath && (view === 'overview' || view === 'dependencies') && <button onClick={() => onNavigate(filePath, 1)}>Read this file</button>}
            {(view === 'overview' || view === 'dependencies') && <div className="spatial-relations" role="group" aria-label="Relationship types">
                <button aria-pressed={!relations.length} onClick={() => setRelations([])}>All</button>{relationKinds.map(type => <button key={type} aria-pressed={relations.includes(type)} onClick={() => setRelations(current => current.includes(type) ? current.filter(item => item !== type) : [...current, type])}>{type.replaceAll('_', ' ').toLowerCase()}</button>)}
            </div>}
        </div>
        <div className="spatial-encoding-controls" aria-label="Map appearance">
            {hasSourceBricks && <label>Height <select aria-label="Brick height" value={heightMetric} onChange={event => setHeightMetric(event.target.value as 'lines' | 'uniform')}><option value="lines">Source size</option><option value="uniform">Uniform</option></select></label>}
            <label>Color <select aria-label="Brick color" value={colorMetric} onChange={event => setColorMetric(event.target.value as 'language' | 'kind')}><option value="language">Language</option><option value="kind">Node kind</option></select></label>
            <label className="spatial-gravity-toggle"><input aria-label="Hotspot gravity" type="checkbox" checked={showHotspots} onChange={event => setShowHotspots(event.target.checked)} />Hotspot gravity</label>
            {(view === 'overview' || view === 'dependencies') && <label>Files <select aria-label="File visibility" value={fileVisibility} onChange={event => { setFileVisibility(event.target.value as typeof fileVisibility); setSelection(undefined); setInventoryLimit(24); }}><option value="all">All known files</option><option value="connected">With connections</option><option value="unconnected">Without known connections</option></select></label>}
            {colorMetric === 'language' && <div className="spatial-language-legend" aria-label="Language colors">{catalog.total.languages.slice(0, 5).map(language => <span key={language.name}><i style={{ background: language.color }} />{language.name}</span>)}{catalog.total.languages.length > 5 && <span>+{catalog.total.languages.length - 5} types</span>}</div>}
        </div>
        <div className="spatial-map-layout">
            <div className="spatial-map"><SceneBoundary key={project}>
                {model.nodes.length ? <ArchitectureScene model={model} selectedId={selectedNode?.id} selectedEdgeId={selectedEdge?.id} onSelect={selectNode}
                    onSelectEdge={id => setSelection({ scope, edge: id })} onClearSelection={clearScope} active={active} planar={planar} resetKey={resetKey} catalog={catalog} heightMetric={heightMetric} colorMetric={colorMetric} hotspots={hotspotCatalog} showHotspots={showHotspots} />
                    : <div className="spatial-unavailable">{filter ? 'No matching graph evidence. Try a broader filter.' : view === 'hotspots' ? 'No ranked hotspot measurements are available in this snapshot.' : view === 'entryPoints' ? 'No indexed entry points in this snapshot.' : view === 'routes' ? 'No endpoint evidence is available in this snapshot.' : filePath ? 'No indexed symbols in this file. Use “Read this file” to inspect its source.' : 'No source nodes are available in this repository snapshot.'}</div>}
            </SceneBoundary><div className="spatial-map-caption"><span>{model.positionMeaning}</span><span>Drag to orbit · scroll to zoom · select to inspect</span></div></div>
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
                    {selectedMeasure && <SourceMetricsDetails measure={selectedMeasure} catalog={catalog} />}
                    {selectedHotspots && <div className="spatial-hotspot-details" aria-label="Hotspot measurements"><h4>Hotspots · {selectedHotspots.findings.length}</h4>{selectedHotspots.maxFanIn !== undefined && <p>Gravity · {Number(gravityPercent(selectedHotspots.maxFanIn, hotspotCatalog.maxFanIn)!.toFixed(1))}% of the strongest measured hotspot.</p>}<p>Strongest measured fan-in: {selectedHotspots.maxFanIn ?? 'unavailable'}. This is an indexed reference count, not runtime frequency.</p>{selectedHotspots.findings.slice(0, memberLimit).map(finding => <article key={hotspotIdentity(finding)}><button disabled={!finding.filePath} onClick={() => onNavigate(finding.filePath!, finding.line, finding.name)}>{finding.name}</button><div>{hotspotSignals(finding).map(signal => <span key={signal}>{signal}</span>)}</div></article>)}{selectedHotspots.findings.length > memberLimit && <button onClick={() => setMemberLimit(value => value + 16)}>More hotspot findings</button>}</div>}
                    {(selectedNode.kind === 'area' || selectedNode.kind === 'file') && <button className="spatial-primary" onClick={() => openGroup(selectedNode)}>Open {selectedNode.kind === 'area' ? 'area' : 'file symbols'} →</button>}
                    {selectedNode.filePath && <button onClick={() => onNavigate(selectedNode.filePath!, selectedNode.line, selectedNode.label)}>Open source</button>}
                    {!!selectedNode.members.length && <><h4>Source members · {selectedNode.members.length}</h4><div className="spatial-members">{selectedNode.members.slice(0, memberLimit).map(node => <button key={`${node.qualified_name}:${node.id}`} onClick={() => inspectMember(node)}>{node.name}<small>{node.file_path}{node.start_line ? `:${node.start_line}` : ''}</small></button>)}</div>
                        {selectedNode.members.length > memberLimit && <button onClick={() => setMemberLimit(value => value + 16)}>More members</button>}</>}
                    {selectedNode.graphNode && selectionPanel}
                </> : <><span className="spatial-eyebrow">Read the map</span><h3>{view === 'hotspots' ? 'Inspect a gravity well.' : 'Follow a connection.'}</h3><p>Select a node to inspect its source members. Select an arrow to see what connects the two parts.</p>{showHotspots && <p className="spatial-hotspot-key">◉ Gravity: square-root scale · strongest hotspot = 100%{hotspotCatalog.maxFanIn > 0 ? ` · maximum ${hotspotCatalog.maxFanIn.toLocaleString()} references` : ` · no measured references`}. Folder fields use their strongest measured hotspot.</p>}<p className="spatial-muted">Regions come from source paths. Their proximity does not establish a dependency.</p></>}
                <details className="spatial-node-list" open={!selectedNode && !selectedEdge}><summary>Map nodes · {model.nodes.length}</summary>{model.nodes.map(node => <button key={node.id} aria-pressed={selectedNode?.id === node.id} onClick={() => selectNode(node.id)}><span>{node.label}</span><small>{node.kind} · {node.count}</small></button>)}</details>
            </aside>
        </div>
        <div className="spatial-bottom"><span>{model.nodes.length} / {model.totalNodes} map nodes · {model.edges.length} / {model.totalEdges} connections displayed</span><span>{graph.nodes.length.toLocaleString()} source nodes · {graph.edges.length.toLocaleString()} source edges loaded</span></div>
        <p className="spatial-measure-note">{hasSourceBricks ? 'Source lines include comments and blank lines.' : 'Gravity represents static fan-in.'} Color is inferred from file type. {catalog.total.measuredFiles.toLocaleString()} / {catalog.total.files.toLocaleString()} known files have a line measurement{catalog.partial ? '; totals cover measured files only' : ''}.</p>
        <details className="spatial-file-inventory"><summary>File inventory · {catalog.files.size.toLocaleString()} known files</summary>
            <p>Every returned file remains reachable here, even without connections or outside the 3D display limit. Unindexed folders may contain additional files that the index has not listed.</p>
            <label>Find a file <input aria-label="Find an architecture file" type="search" value={inventoryFilter} onChange={event => { setInventoryFilter(event.target.value); setInventoryLimit(24); }} /></label>
            <div className="spatial-inventory-list">{inventoryFiles.slice(0, inventoryLimit).map(file => <article key={file.path}><button onClick={() => { setAreaPath(undefined); setFilePath(file.path); setSelection(undefined); onView('overview'); }}>{file.path}</button><small>{file.language} · {file.lines === undefined ? 'size unknown' : `${file.lines.toLocaleString()} indexed lines`} · {file.connection === 'connected' ? 'connected' : file.connection === 'none' ? 'no indexed connections' : 'connections unknown'}{coverage?.records.get(file.path) ? ` · ${coverage.records.get(file.path)!.state}` : ''}</small><button aria-label={`Read ${file.path}`} onClick={() => onNavigate(file.path, 1)}>Read source</button></article>)}</div>
            <small>{Math.min(inventoryLimit, inventoryFiles.length)} / {inventoryFiles.length} matching files</small>{inventoryLimit < inventoryFiles.length && <button onClick={() => setInventoryLimit(value => value + 24)}>More files</button>}
            {(overview.symbolKinds.find(kind => kind.kind === 'File')?.count ?? 0) > overview.files.length && <p className="spatial-notice">The file inventory query is limited. Additional files are available through the explorer tree.</p>}
            {coverage?.truncations.map(note => <p key={note} className="spatial-notice">{note}</p>)}
        </details>
        {!!(model.omittedNodes || model.omittedEdges) && <p className="spatial-notice">The map is bounded for readability. {model.omittedNodes} nodes and {model.omittedEdges} connections are outside this view; filter or open an area to inspect a smaller scope.</p>}
        {view === 'routes' && !routeSnapshot && <p role="status" className="spatial-notice">{routeReading?.key === routeKey && routeReading.error ? `Connections unavailable: ${routeReading.error}` : 'Reading indexed endpoint connections…'}</p>}
        {model.warnings.map(warning => <p className="spatial-notice" key={warning}>{warning}</p>)}
        <details className="spatial-relationship-list"><summary>Connections{selectedNode ? ` for ${selectedNode.label}` : ''} · {edges.length}</summary><div aria-label="Architecture relationships">
            {!edges.length && <p>No matching indexed connections in this view.</p>}{edges.map(edge => <button key={edge.id} aria-pressed={selectedEdge?.id === edge.id} onClick={() => setSelection({ scope, edge: edge.id })}>
                <span>{nodesById.get(edge.source)?.label} → {nodesById.get(edge.target)?.label}</span><small>{edge.type} · {edge.count}</small></button>)}
        </div></details>
        {graphNote && <details className="spatial-coverage"><summary>Index coverage and limits</summary><p>{graphNote}</p></details>}
    </section>;
}
