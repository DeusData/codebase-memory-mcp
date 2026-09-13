import { Component, useEffect, useMemo, useState, type ReactNode } from 'react';
import { RpcIntelligenceClient } from '../provider/rpc-client';
import type { ProjectEntry } from '../provider/rpc-schemas';
import { ArchitectureScene } from './ArchitectureScene';
import { layoutContainers } from './container-layout';
import { loadContainerInventory, loadContainerTopology, type ContainerReading, type ContainerSelection } from './container-source';
import type { ServiceEvidence } from './container-topology';
import './spatial-architecture.css';
import './container-map.css';

interface Props { project: string; generation?: string; active: boolean; filter: string; onNavigate: (path: string, line?: number) => void; onClearSelection?: () => void }
class ContainerSceneBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
    state = { failed: false };
    static getDerivedStateFromError() { return { failed: true }; }
    render() { return this.state.failed ? <p className="spatial-unavailable">3D is unavailable. Use the service and connection lists to inspect the same evidence.</p> : this.props.children; }
}
const evidenceLabel = (kind: string) => kind === 'startup' ? 'Startup dependency' : kind === 'configuration' ? 'Configured destination' : 'Call found in source';
const relativeSource = (absolute: string, selection: ContainerSelection) => {
    const prefix = selection.inventory.rootPath.replaceAll('\\', '/').replace(/\/$/, '') + '/';
    return absolute.startsWith(prefix) ? absolute.slice(prefix.length) : undefined;
};

export default function ContainerMap({ project, generation, active, filter, onNavigate, onClearSelection }: Props) {
    const [available, setAvailable] = useState<ProjectEntry[]>([]);
    const [selections, setSelections] = useState<ContainerSelection[]>([]);
    const [discovery, setDiscovery] = useState('Finding indexed Compose definitions…');
    const [reading, setReading] = useState<{ key: string; result?: ContainerReading; error?: string }>();
    const [revision, setRevision] = useState(0);
    const [discoveryRevision, setDiscoveryRevision] = useState(0);
    const [startup, setStartup] = useState(false);
    const [configured, setConfigured] = useState(true);
    const [planar, setPlanar] = useState(false);
    const [resetKey, setResetKey] = useState(0);
    const [selected, setSelected] = useState<{ key: string; node?: string; edge?: string }>();
    const [focus, setFocus] = useState<string>();
    const [adding, setAdding] = useState(false);
    const [source, setSource] = useState<{ key: string; evidence: ServiceEvidence; text?: string; error?: string }>();
    const [memberLimit, setMemberLimit] = useState(12);
    useEffect(() => {
        if (!active) return;
        const controller = new AbortController(), client = new RpcIntelligenceClient({ signal: controller.signal });
        setDiscovery('Finding indexed Compose definitions…');
        void client.listProjects().then(async response => {
            const projects = response.projects;
            const current = projects.find(entry => entry.name === project);
            if (!current) throw new Error('Project is no longer indexed.');
            const inventory = await loadContainerInventory(current, client);
            if (controller.signal.aborted) return;
            setAvailable(projects); setSelections([{ inventory, manifest: inventory.manifests[0] ?? '' }]);
            setDiscovery(inventory.manifests.length ? '' : 'No indexed Compose file was found. The Endpoints view is still available.');
        }).catch(() => { if (!controller.signal.aborted) setDiscovery('Could not read the indexed deployment files. Refresh to try again.'); });
        return () => controller.abort();
    }, [project, active, generation, discoveryRevision]);
    const selectionKey = JSON.stringify(selections.map(item => [item.inventory.project, item.manifest]));
    const key = `${project}:${generation ?? ''}:${selectionKey}:${revision}`;
    useEffect(() => {
        if (!active || !selections.length || selections[0].inventory.project !== project || !selections.some(item => item.manifest)) return;
        const controller = new AbortController(), client = new RpcIntelligenceClient({ signal: controller.signal });
        setReading({ key }); setFocus(undefined); setSource(undefined);
        void loadContainerTopology(selections, client, controller.signal).then(result => {
            if (!controller.signal.aborted) setReading({ key, result });
        }).catch(() => { if (!controller.signal.aborted) setReading({ key, error: 'Could not complete service inspection. Refresh to try again.' }); });
        return () => controller.abort();
    }, [active, project, key, selections]);
    const result = reading?.key === key ? reading.result : undefined;
    const topology = result?.topology;
    const layout = useMemo(() => topology ? layoutContainers(topology) : undefined, [topology]);
    const selectedService = selected?.key === key ? topology?.services.find(service => service.id === selected.node) : undefined;
    const selectedEdge = selected?.key === key ? topology?.connections.find(edge => edge.id === selected.edge) : undefined;
    const shownEdges = useMemo(() => topology?.connections.filter(edge => (startup || edge.kind !== 'startup') && (configured || edge.kind !== 'configuration')) ?? [], [topology, startup, configured]);
    const graph = useMemo(() => {
        if (!layout || !topology) return undefined;
        const connected = new Set<string>(focus ? [focus] : []);
        if (focus) shownEdges.forEach(edge => { if (edge.source === focus || edge.target === focus) { connected.add(edge.source); connected.add(edge.target); } });
        const query = filter.trim().toLowerCase();
        const ids = new Set(topology.services.filter(service => (!focus || connected.has(service.id))
            && (!query || `${service.name} ${service.project} ${service.image ?? ''}`.toLowerCase().includes(query))).map(service => service.id));
        const edges = new Set(shownEdges.filter(edge => ids.has(edge.source) && ids.has(edge.target)).map(edge => edge.id));
        return { ...layout.graph, scopeKey: `${layout.graph.scopeKey}:${focus ?? ''}`,
            nodes: layout.graph.nodes.filter(node => ids.has(node.id)), edges: layout.graph.edges.filter(edge => edges.has(edge.id)),
            omittedNodes: layout.graph.nodes.length - ids.size, omittedEdges: layout.graph.edges.length - edges.size };
    }, [layout, topology, filter, focus, shownEdges]);
    const servicesById = useMemo(() => new Map(topology?.services.map(service => [service.id, service]) ?? []), [topology]);
    const clearSelection = () => { setSelected(undefined); setFocus(undefined); setSource(undefined); setResetKey(value => value + 1); onClearSelection?.(); };
    const selectService = (id: string) => { setSelected({ key, node: id }); setMemberLimit(12); setSource(undefined); };
    const inspect = (evidence: ServiceEvidence) => {
        if (evidence.project === project) { onNavigate(evidence.path, evidence.line); return; }
        setSource({ key, evidence });
    };
    const sourceEvidence = source?.key === key ? source.evidence : undefined;
    useEffect(() => {
        if (!sourceEvidence) return;
        const inventory = selections.find(selection => selection.inventory.project === sourceEvidence.project)?.inventory;
        const qn = inventory?.files.get(sourceEvidence.path);
        if (!qn) { setSource({ key, evidence: sourceEvidence, error: 'This source location is not in the selected project inventory.' }); return; }
        const controller = new AbortController(), client = new RpcIntelligenceClient({ signal: controller.signal });
        void client.getCodeSnippet(sourceEvidence.project, qn, { startLine: Math.max(1, sourceEvidence.line - 2), maxLines: 12 }).then(snippet => {
            if (!controller.signal.aborted) setSource({ key, evidence: sourceEvidence, text: snippet.source });
        }).catch(() => { if (!controller.signal.aborted) setSource({ key, evidence: sourceEvidence, error: 'Source evidence is unavailable.' }); });
        return () => controller.abort();
    }, [sourceEvidence, key, selections]);
    const addProject = async (name: string) => {
        const entry = available.find(item => item.name === name);
        if (!entry || adding || selections.length >= 4) return;
        setAdding(true);
        try {
            const inventory = await loadContainerInventory(entry, new RpcIntelligenceClient());
            setSelections(current => current.some(item => item.inventory.project === name) ? current : [...current, { inventory, manifest: inventory.manifests[0] ?? '' }]);
        } catch { setDiscovery('Could not read that project. Its index may be unavailable.'); }
        finally { setAdding(false); }
    };
    const evidenceButton = (evidence: ServiceEvidence, index: number) => <button key={`${evidence.project}:${evidence.path}:${evidence.line}:${index}`} onClick={() => inspect(evidence)}>
        {evidence.summary}<small>{evidence.project} / {evidence.path}:{evidence.line}</small></button>;
    const incident = selectedService ? shownEdges.filter(edge => edge.source === selectedService.id || edge.target === selectedService.id) : [];
    const warnings = [...new Set([...(topology?.warnings ?? []), ...(result?.warnings ?? [])])];
    return <section className="spatial-architecture container-map" aria-label="Container service map">
        <header className="spatial-heading"><div><span className="spatial-eyebrow">Routes / Services</span><h2>How the services connect</h2>
            <p>One square per declared service. Follow a connection to see the configuration and code behind it.</p></div>
            <div className="spatial-camera-controls"><button aria-pressed={!planar} onClick={() => setPlanar(false)}>3D</button><button aria-pressed={planar} onClick={() => setPlanar(true)}>Plan</button>
                <button onClick={() => setResetKey(value => value + 1)}>Fit map</button><button onClick={() => {
                    setRevision(value => value + 1);
                    if (!selections.length || !selections[0].inventory.manifests.length) setDiscoveryRevision(value => value + 1);
                }}>Refresh</button></div></header>
        <div className="container-projects" aria-label="Deployment sources">{selections.map((selection, index) => <label key={selection.inventory.project}><span>{selection.inventory.project}</span>
            <select aria-label={`Compose file for ${selection.inventory.project}`} value={selection.manifest} onChange={event => setSelections(current => current.map((item, i) => i === index ? { ...item, manifest: event.target.value } : item))}>
                <option value="">Source only</option>{selection.inventory.manifests.map(path => <option key={path}>{path}</option>)}</select>
            {index > 0 && <button aria-label={`Remove ${selection.inventory.project}`} onClick={() => setSelections(current => current.filter((_, i) => i !== index))}>×</button>}</label>)}
            {selections.length < 4 && <select aria-label="Compare indexed project" value="" disabled={adding} onChange={event => { void addProject(event.target.value); }}><option value="">{adding ? 'Reading project…' : '+ Compare project'}</option>
                {available.filter(entry => !selections.some(selection => selection.inventory.project === entry.name)).map(entry => <option key={entry.name} value={entry.name}>{entry.name}</option>)}</select>}</div>
        <div className="spatial-controls"><nav aria-label="Service navigation"><button onClick={clearSelection}>All services</button>{focus && <span> / {servicesById.get(focus)?.name} and neighbors</span>}</nav>
            <div className="container-edge-key"><span><i className="container-call-key" />Calls</span><label><input type="checkbox" checked={configured} onChange={event => setConfigured(event.target.checked)} />Configured destinations</label>
                <label><input type="checkbox" checked={startup} onChange={event => setStartup(event.target.checked)} />Startup dependencies</label></div></div>
        {discovery && <p role="status" className="spatial-notice">{discovery}</p>}
        {!discovery && selections.length > 0 && !selections.some(selection => selection.manifest) && <p className="spatial-notice">Select a Compose file to show its declared services.</p>}
        {reading?.key === key && !result && <p role="status" className="spatial-notice">{reading.error ?? 'Reading deployment definitions and bounded source evidence…'}</p>}
        {graph && topology && <><div className="spatial-map-layout"><div className="spatial-map">
            {graph.nodes.length ? <ContainerSceneBoundary key={key}><ArchitectureScene model={graph} active={active} planar={planar} resetKey={resetKey} adaptiveLabels={graph.nodes.length > 16}
                selectedId={selectedService?.id} selectedEdgeId={selectedEdge?.id} onSelect={selectService} onSelectEdge={id => { setSelected({ key, edge: id }); setSource(undefined); }}
                onClearSelection={clearSelection} onOpen={id => { selectService(id); setFocus(id); }} /></ContainerSceneBoundary> : <p className="spatial-unavailable">No matching service declarations in the selected files.</p>}
            <div className="spatial-map-caption"><span>Static declarations · not live containers</span><span>Double-click a square to focus · select an arrow for evidence</span></div></div>
            <aside className="spatial-inspector" aria-label="Service inspector">
                {selectedService ? <><span className="spatial-eyebrow">Declared service</span><h3>{selectedService.name}</h3><p className="spatial-muted">{selectedService.project}</p>
                    <button className="spatial-primary" onClick={() => setFocus(selectedService.id)}>Focus service</button>
                    <button onClick={() => inspect({ project: selectedService.project, path: selectedService.manifest, line: selectedService.line, summary: 'Service declaration' })}>Open declaration</button>
                    <dl className="container-facts"><dt>Image</dt><dd>{selectedService.image ?? 'Built locally'}</dd><dt>Networks</dt><dd>{selectedService.networks.map(network => network.split(':').at(-1)).join(', ') || 'No resolved network'}</dd>
                        <dt>Ports</dt><dd>{selectedService.ports.join(', ') || 'Not declared'}</dd>{Boolean(selectedService.profiles?.length) && <><dt>Profiles</dt><dd>{selectedService.profiles!.join(', ')} · optional</dd></>}
                        <dt>Source</dt><dd>{selectedService.sourcePaths.length ? `${selectedService.sourcePaths.length} candidate files` : 'Image-only or unresolved ownership'}</dd></dl>
                    <div className="spatial-members"><h4>Connections</h4>{incident.length ? incident.map(edge => <button key={edge.id} onClick={() => setSelected({ key, edge: edge.id })}>{servicesById.get(edge.source)?.name} → {servicesById.get(edge.target)?.name}<small>{evidenceLabel(edge.kind)} · {edge.protocol}</small></button>) : <p>No detected connections with the current filters.</p>}
                        <h4>Source files</h4>{selectedService.sourcePaths.slice(0, memberLimit).map(absolute => {
                            const owner = selections.find(selection => { const path = relativeSource(absolute, selection); return path !== undefined && selection.inventory.files.has(path); });
                            const path = owner && relativeSource(absolute, owner);
                            return path && owner ? <button key={absolute} onClick={() => inspect({ project: owner.inventory.project, path, line: 1, summary: 'Candidate source file' })}>{path}<small>{owner.inventory.project}</small></button> : <span key={absolute}>{absolute}</span>;
                        })}{selectedService.sourcePaths.length > memberLimit && <button onClick={() => setMemberLimit(value => value + 24)}>Show more files</button>}</div></>
                    : selectedEdge ? <><span className="spatial-eyebrow">{evidenceLabel(selectedEdge.kind)}</span><h3>{servicesById.get(selectedEdge.source)?.name} → {servicesById.get(selectedEdge.target)?.name}</h3><p>{selectedEdge.protocol}</p>
                        <p className="spatial-muted">{selectedEdge.kind === 'startup' ? 'The deployment declares a startup dependency. This does not establish a network call.' : selectedEdge.kind === 'configuration' ? 'Configuration names this destination. An executed call has not been established.' : 'Static source identifies this destination. The arrow shows the caller, not the direction of every payload.'}</p>
                        <div className="spatial-members">{selectedEdge.evidence.map(evidenceButton)}</div></>
                        : <><span className="spatial-eyebrow">Read the map</span><h3>Services, with evidence.</h3><p>Blue squares have candidate source files. Warm squares have an image declaration or unresolved source ownership.</p><p className="spatial-muted">Compare another indexed project to find connections over explicitly shared networks. Unresolved hosts remain findings, not guessed arrows.</p></>}
                {source?.key === key && <div className="container-source"><h4>{source.evidence.project} / {source.evidence.path}:{source.evidence.line}</h4><button onClick={() => setSource(undefined)}>Close source</button><pre>{source.error ?? source.text ?? 'Reading source…'}</pre></div>}
            </aside></div>
            <div className="spatial-bottom"><span>{graph.nodes.length} / {topology.services.length} services · {graph.edges.length} connections shown</span><span>{result?.filesRead} source files inspected · {topology.unresolved.length} unresolved findings</span></div>
            <div className="container-service-list" aria-label="All declared services">{topology.services.map(service => <button key={service.id} aria-pressed={selectedService?.id === service.id} onClick={() => selectService(service.id)} onDoubleClick={() => { selectService(service.id); setFocus(service.id); }}>{service.name}<small>{service.project}{service.profiles?.length ? ' · optional' : ''}</small></button>)}</div>
            {!!layout?.cycles.length && <details className="spatial-coverage"><summary>Dependency cycles · {layout.cycles.length}</summary>{layout.cycles.map(cycle => <p key={cycle.join('|')}>{cycle.map(id => servicesById.get(id)?.name).join(' ↔ ')} · mutually reachable through detected calls or configured destinations</p>)}</details>}
            <details className="spatial-relationship-list"><summary>All connections · {shownEdges.length}</summary><div>{shownEdges.map(edge => <button key={edge.id} onClick={() => setSelected({ key, edge: edge.id })}>{servicesById.get(edge.source)?.name} → {servicesById.get(edge.target)?.name}<small>{evidenceLabel(edge.kind)} · {edge.protocol}</small></button>)}</div></details>
            {!!topology.unresolved.length && <details className="spatial-coverage"><summary>Unresolved destinations · {topology.unresolved.length}</summary><div className="spatial-members">{topology.unresolved.map(evidenceButton)}</div></details>}
        </>}
        <details className="spatial-coverage"><summary>Coverage and interpretation{warnings.length ? ` · ${warnings.length} notes` : ''}</summary><p>One selected Compose file per project. Overrides, includes, host environment values, and running-container state are not evaluated. Source grouping is inferred from build inputs, not guaranteed exclusive ownership.</p>
            {warnings.map(warning => <p key={warning}>{warning}</p>)}</details>
    </section>;
}
