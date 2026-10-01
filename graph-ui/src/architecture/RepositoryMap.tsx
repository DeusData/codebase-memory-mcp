import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';
import type { GraphData, GraphNode } from '../galaxy/types';
import { areaConnections, areaOf, repositoryMap, type MapEvidence } from './repository-map';
import { entryCandidates, entryRole, entryRoutes, sourceCandidates, type SourceReader, type SourceQuote } from './repository-guide';
import { useRepositoryGuide } from './useRepositoryGuide';

export interface RepositoryMapProps {
    graph?: GraphData;
    overview: ArchitectureOverviewDto;
    filter: string;
    onNavigate: (path: string, line?: number, name?: string) => void;
    onSelect?: (node: GraphNode) => void;
    selection?: GraphNode;
    selectionPanel?: ReactNode;
    graphNote?: string;
    readSource?: SourceReader;
}

function Nodes({ nodes, onSelect, onNavigate }: Pick<RepositoryMapProps, 'onSelect' | 'onNavigate'> & { nodes: GraphNode[] }) {
    const [limit, setLimit] = useState(24);
    return <><ul className="repo-map-nodes">{nodes.slice(0, limit).map(node => <li key={node.id}>
        <button onClick={() => onSelect ? onSelect(node) : onNavigate(node.file_path!, node.start_line, node.name)}>
            <strong>{node.name}</strong><span>{node.label} · {node.file_path}{node.start_line ? `:${node.start_line}` : ''}</span>
        </button>
    </li>)}</ul>{nodes.length > limit && <button className="atlas-arch-action" onClick={() => setLimit(limit + 24)}>More symbols ({limit} of {nodes.length})</button>}</>;
}

export function RelationshipEvidence({ edges, onSelect, onNavigate }: Pick<RepositoryMapProps, 'onSelect' | 'onNavigate'> & { edges: MapEvidence[] }) {
    const [limit, setLimit] = useState(12);
    return <div className="repo-map-evidence" aria-label="Relationship evidence">
        <p className="repo-map-note">Static index edges can include heuristic name resolution. Open the declarations to verify the relationship; a call edge does not prove runtime execution.</p>
        {edges.slice(0, limit).map((edge, index) => <div className="repo-map-edge" key={`${edge.source.id}:${edge.type}:${edge.target.id}:${index}`}>
            <Nodes nodes={[edge.source]} onSelect={onSelect} onNavigate={onNavigate} />
            <span className="repo-map-relation">{edge.type} →{edge.id !== undefined && <small>Edge #{edge.id}</small>}
                {edge.line !== undefined && edge.source.file_path && <button onClick={() => onNavigate(edge.source.file_path!, edge.line, edge.source.name)}>Site :{edge.line}</button>}
                {edge.unverifiedLine !== undefined && <small title="The reported line could not be verified against the source declaration range. Inspect the declarations instead.">Site unverified<br />Reported :{edge.unverifiedLine}</small>}
                <details className="repo-map-resolution"><summary>Resolution</summary><small>{edge.strategy ? `Index method: ${edge.strategy}` : 'Resolution method not recorded.'}</small>
                    {edge.confidence !== undefined && <small>Index confidence: {edge.confidence}. This is a resolver score, not a defect probability.</small>}</details></span>
            <Nodes nodes={[edge.target]} onSelect={onSelect} onNavigate={onNavigate} />
        </div>)}
        <p>{Math.min(limit, edges.length)} of {edges.length} retained edges</p>
        {limit < edges.length && <button className="atlas-arch-action" onClick={() => setLimit(limit + 24)}>More evidence</button>}
    </div>;
}

function Quote({ quote, onNavigate }: { quote: SourceQuote; onNavigate: RepositoryMapProps['onNavigate'] }) {
    return <div className="repo-map-quote"><span className="repo-map-eyebrow">{quote.kind === 'readme' ? 'Project README' : 'Source documentation'} · author description</span>
        <blockquote>{quote.text}</blockquote><button onClick={() => onNavigate(quote.path, quote.line)}>Source: {quote.path}:{quote.line}</button></div>;
}

export default function RepositoryMapView({ graph, overview, filter, onNavigate, onSelect, selection, selectionPanel, graphNote, readSource }: RepositoryMapProps) {
    const map = useMemo(() => graph ? repositoryMap(graph) : undefined, [graph]);
    const [areaPath, setAreaPath] = useState<string>();
    const [file, setFile] = useState<string>();
    const [showAll, setShowAll] = useState(false);
    const [direction, setDirection] = useState<'incoming' | 'outgoing'>('outgoing');
    const [evidence, setEvidence] = useState<{ title: string; edges: MapEvidence[] }>();
    const [entryId, setEntryId] = useState<number>();
    const selectionRef = useRef<HTMLElement>(null);
    const evidenceRef = useRef<HTMLElement>(null);
    useEffect(() => { selectionRef.current?.scrollIntoView?.({ block: 'start', behavior: 'smooth' }); }, [selection?.id]);
    useEffect(() => { if (evidence) { evidenceRef.current?.scrollIntoView?.({ block: 'nearest' }); evidenceRef.current?.focus({ preventScroll: true }); } }, [evidence]);
    const area = map?.areas.find(row => row.path === areaPath);
    const openArea = (path?: string) => { setAreaPath(path); setFile(undefined); setEvidence(undefined); setShowAll(false); };
    const query = filter.trim().toLowerCase();
    const entries = useMemo(() => entryCandidates(graph?.nodes ?? []), [graph]);
    const entry = entries.find(node => node.id === entryId) ?? entries[0];
    const routes = useMemo(() => entry && map ? entryRoutes(entry, map.evidence) : [], [entry, map]);
    const primaryAreas = useMemo(() => [...(map?.areas ?? [])].sort((a, b) => {
        const support = (path: string) => /(^|\/)(tests?|tools|scripts|docs|pkg|\.github|test-infrastructure)(\/|$)/.test(path);
        return Number(support(a.path)) - Number(support(b.path));
    }), [map]);
    const summaryEntries = overview.entryPoints.filter(entry => entry.filePath);
    const readme = overview.files.find(path => /^readme\.(md|rst|txt)$/i.test(path));
    const sourcePaths = useMemo(() => [...new Set([...(readme ? [readme] : []), ...(area ? sourceCandidates(area) : []),
        ...primaryAreas.slice(0, 12).flatMap(sourceCandidates)])], [readme, area, primaryAreas]);
    const guide = useRepositoryGuide(sourcePaths, readSource);
    const quoteFor = (files: string[]) => sourcePaths.filter(path => files.includes(path)).map(path => guide.quotes[path]).find(Boolean);
    const areas = primaryAreas.filter(row => !query || row.path.toLowerCase().includes(query)
        || row.nodes.some(node => node.name.toLowerCase().includes(query)) || quoteFor(row.files)?.text.toLowerCase().includes(query));
    const scopedNodes = area?.nodes.filter(node => node.file_path === file && !['File', 'Module'].includes(node.label)) ?? [];
    const connections = area ? areaConnections(area, direction) : [];
    const layers = area ? overview.layers.filter(layer => layer.group && (area.path === layer.group || area.path.endsWith(`/${layer.group}`))) : [];
    return <div className="repo-map" data-testid="repository-map">
        <div className="repo-map-intro">
            <div><p>Repository map · source responsibilities and indexed connections</p></div>
            <details><summary>Evidence legend</summary><p>CALLS: indexed invocations. IMPORTS: static imports. USAGE: references. INHERITS / IMPLEMENTS: type contracts. DATA_FLOWS appears only for recorded data-flow edges. Directory areas and layer roles are navigation heuristics, not confirmed domain boundaries. Agent activity is shown separately.</p></details>
        </div>
        {!map && graphNote && <p role="status" className="repo-map-note">{graphNote}</p>}
        {!map ? <p role="status">Repository graph not available yet. The summary tabs remain available.</p> : <>
            <details className="repo-map-snapshot"><summary>{map.fileCount.toLocaleString()} indexed files · {map.evidence.length.toLocaleString()} retained edges · snapshot and limits</summary>
                {graphNote && <p className="repo-map-note">{graphNote}</p>}
                <p className="repo-map-note">Bounded index snapshot; missing edges do not prove independence.{map.unresolvedEdges > 0 && ` ${map.unresolvedEdges} edges have an endpoint outside these source areas.`}</p>
            </details>
            <nav className="repo-map-breadcrumb" aria-label="Repository map location">
                <button onClick={() => openArea()}>Repository</button>
                {area && <><span>›</span><button onClick={() => { setFile(undefined); setEvidence(undefined); }}>{area.path}</button></>}
                {file && <><span>›</span><span>{file.split('/').at(-1)}</span></>}
            </nav>
            {selection && <section className="repo-map-selection" ref={selectionRef}><div className="repo-map-selection-heading"><div><span className="repo-map-eyebrow">Selected symbol · {areaOf(selection.file_path ?? '')}</span><h2>{selection.name}</h2></div>
                <button className="atlas-arch-action" onClick={() => onNavigate(selection.file_path!, selection.start_line, selection.name)}>Open source · {selection.file_path}:{selection.start_line ?? '?'}</button></div>
                {selection.documentation && <details><summary>Indexed documentation</summary><p>{selection.documentation}</p></details>}
                {selectionPanel}
            </section>}
            {!area ? <>
                <div className="repo-map-onboarding">
                <section className="repo-map-purpose"><h3>What this repository does</h3>
                    {readme && guide.quotes[readme] ? <Quote quote={guide.quotes[readme]} onNavigate={onNavigate} />
                        : <p>{readSource && !guide.finished ? 'Reading the project introduction…' : 'No project introduction was available in the inspected source. Explore the component evidence below.'}</p>}
                    {readme && <button className="atlas-arch-action" onClick={() => onNavigate(readme)}>Read project README</button>}
                </section>
                <section className="repo-map-start"><h3>Where to start</h3><p>Index entry candidates, with roles inferred from file type and location. Select one to follow its static calls.</p>
                    {entries.length ? <div className="repo-map-entry-grid">{entries.slice(0, 4).map(node => <button key={node.id} aria-pressed={node.id === entry?.id} onClick={() => { setEntryId(node.id); setEvidence(undefined); }}>
                        <span className="repo-map-eyebrow">{entryRole(node)} · entry candidate</span><strong>{node.name}</strong><span>{node.file_path}:{node.start_line ?? '?'}</span></button>)}</div>
                        : summaryEntries.length ? <ul className="repo-map-nodes">{summaryEntries.slice(0, 4).map(entry => <li key={`${entry.filePath}:${entry.name}`}><button onClick={() => onNavigate(entry.filePath!, entry.line, entry.name)}><strong>{entry.name}</strong><span>{entry.filePath}</span></button></li>)}</ul>
                            : <p>No entry point was identified. Start with a connected area below and inspect its exported interfaces.</p>}
                    {entry && <details className="repo-map-flow" open={entryId !== undefined}><summary>Follow static call paths from {entry.name} · {routes.length} paths across files</summary><div className="repo-map-section-title"><h3>Static paths from {entry.name}</h3><button onClick={() => onNavigate(entry.file_path!, entry.start_line, entry.name)}>Read entry source</button></div>
                        <p>Each step is a recorded CALLS edge. This shows possible reachability, not an observed runtime sequence or data flow.</p>
                        {routes.length ? routes.map((route, index) => <div key={index} className="repo-map-route"><div className="repo-map-route-steps">{[route.edges[0].source, ...route.edges.map(edge => edge.target)].map((node, step) => <span key={node.id}>
                            {step > 0 && <span aria-label="calls" className="repo-map-route-arrow">→</span>}<button onClick={() => onNavigate(node.file_path!, node.start_line, node.name)}><small>{areaOf(node.file_path!)}</small><strong>{node.name}</strong></button></span>)}</div>
                            <button className="atlas-arch-action" onClick={() => setEvidence({ title: `Static call path from ${entry.name}`, edges: route.edges })}>Inspect {route.edges.length} call edges</button></div>)
                            : <p>No call path across files was found within 5 calls and 500 symbols. Open the entry source or choose another candidate.</p>}
                        {evidence && <section ref={evidenceRef} tabIndex={-1} className="repo-map-focused-evidence"><div className="repo-map-section-title"><h3>{evidence.title}</h3><button onClick={() => setEvidence(undefined)}>Close evidence</button></div><RelationshipEvidence edges={evidence.edges} onSelect={onSelect} onNavigate={onNavigate} /></section>}
                    </details>}
                </section>
                </div>
                <section><h3>Components and responsibilities</h3><p className="repo-map-note">Boundaries follow source locations and are inferred. Descriptions quote source documentation; relationships come from indexed edges. Application areas appear before tests and tools.</p>
                    <div className="repo-map-areas">{areas.slice(0, showAll ? undefined : 12).map(row => { const quote = quoteFor(row.files); return <article key={row.path} className="repo-map-component">
                        <button className="repo-map-area" onClick={() => openArea(row.path)}><span className="repo-map-eyebrow">Source area · inferred boundary</span><strong>{row.path}</strong>
                            <span>{row.files.length} files · {row.entryPoints.length} entry candidates · {row.interfaces.length} exported symbols</span>
                            <span>Used by {new Set(row.incoming.map(edge => areaOf(edge.source.file_path!))).size} areas · Depends on {new Set(row.outgoing.map(edge => areaOf(edge.target.file_path!))).size} areas</span></button>
                        {quote ? <Quote quote={quote} onNavigate={onNavigate} /> : <p className="repo-map-no-description">{readSource && !guide.finished ? 'Checking source documentation…' : 'No responsibility description in the inspected file headers. Inspect interfaces and callers to establish its role.'}</p>}
                        <button className="repo-map-component-action" onClick={() => openArea(row.path)}>Explore connections →</button>
                    </article>; })}</div>
                    {!areas.length && <p>No source areas match. Clear the filter or inspect index diagnostics.</p>}
                    {areas.length > 12 && !showAll && <button className="atlas-arch-action" onClick={() => setShowAll(true)}>All {areas.length} areas, including tests and tools</button>}
                </section>
            </> : <>
                <section className="repo-map-area-heading"><h2>{area.path}</h2><p>{area.files.length} files · {area.nodes.length} indexed nodes</p>
                    {quoteFor(area.files) && <Quote quote={quoteFor(area.files)!} onNavigate={onNavigate} />}
                    {layers.map(layer => <p key={layer.layer}>Inferred role: {layer.layer}. {layer.reason || 'No further reason supplied by the index.'}</p>)}
                    {!file && <><h3>Entry points and interfaces</h3><Nodes nodes={[...area.entryPoints, ...area.interfaces.filter(node => !area.entryPoints.includes(node))].slice(0, 8)} onSelect={onSelect} onNavigate={onNavigate} />
                        {!area.entryPoints.length && !area.interfaces.length && <p>No entry or export classification was supplied. Inspect a file below.</p>}</>}
                </section>
                {evidence && <section ref={evidenceRef} tabIndex={-1} key={evidence.title} className="repo-map-focused-evidence"><div className="repo-map-section-title"><h3>{evidence.title}</h3><button onClick={() => setEvidence(undefined)}>Close evidence</button></div><RelationshipEvidence edges={evidence.edges} onSelect={onSelect} onNavigate={onNavigate} /></section>}
                {!file ? <div className="repo-map-columns"><section><h3>How this area connects</h3>
                    <div className="repo-map-directions"><button aria-pressed={direction === 'outgoing'} onClick={() => { setDirection('outgoing'); setEvidence(undefined); }}>Depends on →</button><button aria-pressed={direction === 'incoming'} onClick={() => { setDirection('incoming'); setEvidence(undefined); }}>Used by ←</button></div>
                    <p className="repo-map-note">Cross-area edges grouped by relation kind. Counts are retained graph edges, not call frequency.</p>
                    <ul className="repo-map-connections">{connections.map(row => <li key={`${row.area}:${row.type}`}>
                        <button onClick={() => openArea(row.area)}>{row.area}</button><span>{row.type}</span><button onClick={() => setEvidence({ title: `${area.path} ${direction === 'incoming' ? '←' : '→'} ${row.area}`, edges: row.evidence })}>{row.evidence.length} edges · inspect</button>
                    </li>)}</ul>{!connections.length && <p>No cross-area edges found in this snapshot. This is not evidence of independence.</p>}
                </section><section><h3>Files</h3><div className="repo-map-files">{area.files.map(path => <button key={path} onClick={() => { setFile(path); setEvidence(undefined); }}>{path}</button>)}</div></section></div>
                    : <section><h3>Symbols in {file}</h3><button className="atlas-arch-action" onClick={() => onNavigate(file)}>Read file</button><Nodes nodes={scopedNodes} onSelect={onSelect} onNavigate={onNavigate} />
                        <button className="atlas-arch-action" onClick={() => setEvidence({ title: file, edges: map.evidence.filter(edge => edge.source.file_path === file || edge.target.file_path === file) })}>Inspect file relationships</button>
                    </section>}
            </>}
        </>}
    </div>;
}
