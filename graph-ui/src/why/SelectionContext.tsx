import { useMemo } from 'react';
import type { GraphData, GraphNode } from '../galaxy/types';
import type { AgentsState } from '../agents/agent-store';
import { selectionContext } from './selection-context';
import { selectionScopeText } from './selection-strings';
import { graphNodeName } from '../galaxy/node-names';
import { RelationshipEvidence } from '../architecture/RepositoryMap';
import type { MapEvidence } from '../architecture/repository-map';
import type { ScopePartial, TraceDirection } from '../galaxy/graph-scope';
import './selection-context.css';

/** The scope that Galaxy has loaded around the selection (hand test K13). */
export interface SelectionScope {
    graph: GraphData;
    complete: boolean;
    direction: TraceDirection;
    edgeTypes?: readonly string[];
    partial?: ScopePartial;
    depth?: number;
}

export default function SelectionContextPanel({ graph, selected, path, agents, onNavigate, onImpact, scope }: {
    graph?: GraphData; selected?: GraphNode; path: string; agents?: AgentsState;
    onNavigate: (path: string, line?: number, name?: string) => void; onImpact?: () => void;
    /**
     * In Galaxy: the loaded scope. Its relationships replace the repository
     * snapshot, which is capped (20,000 nodes by file path) and missed, for
     * JSONBAgg, all 23 incoming relationships from the tests behind the cap.
     */
    scope?: SelectionScope;
}) {
    const inScope = Boolean(scope && selected && scope.graph.nodes.some(node => selected.qualified_name
        ? node.qualified_name === selected.qualified_name && (node.file_path ?? '') === path : node.id === selected.id));
    const source = inScope ? scope!.graph : graph;
    const context = useMemo(() => selectionContext(source, selected, path, agents, { allRelations: inScope }), [source, selected, path, agents, inScope]);
    const callers = context.incoming.filter(edge => edge.type === 'CALLS');
    const callerFiles = new Set(callers.map(edge => edge.source.file_path).filter(Boolean));
    return <section className="selection-context" aria-label="Selection context" data-testid="selection-context">
        <h3>Selection context</h3>
        {/* Round 4 (N1): a selected node without a file, such as a Branch node, keeps its connections and its shown name. */}
        {!path && !selected ? <p>{selectionScopeText.selectSomething}</p> : <>
            <p className="selection-context-subject">{selected ? graphNodeName(selected) : path}</p>
            <h4>Graph evidence</h4>
            {!source ? <p>The repository graph is not available. Relevance cannot be established yet.</p> : <>
                {scope && <p className="selection-context-source" data-source={inScope ? 'scope' : 'snapshot'}>{inScope ? selectionScopeText.fromScope(scope.depth) : selectionScopeText.fromSnapshot}</p>}
                {inScope && !scope!.complete && <p role="status">{selectionScopeText.loading}</p>}
                {inScope && scope!.partial && <p role="status">{selectionScopeText.partial(scope!.partial.layer, scope!.partial.limit)}</p>}
                {inScope && scope!.direction !== 'both' && <p>{selectionScopeText.notTraced(scope!.direction)}</p>}
                {inScope && scope!.edgeTypes && <p>{selectionScopeText.typesOnly(scope!.edgeTypes)}</p>}
                {context.selected?.status === 'entry' && <p>The index classifies this declaration as an entry candidate. Open its source and follow outgoing calls to verify its role.</p>}
                {callers.length > 0 && <p>{callers.length} indexed caller edges from {callerFiles.size} files reach this selection. Check these callers when changing its contract.</p>}
                {context.entryPath.length > 0 && <details><summary>Reachable from {context.entryPath[0].source.name} · inspect {context.entryPath.length} calls</summary>
                    <p>This selection is reachable from an indexed entry candidate. This is static reachability, not an observed execution or data flow.</p>
                    <RelationshipEvidence edges={context.entryPath} onNavigate={onNavigate} />
                </details>}
                {!context.incoming.length && !context.outgoing.length && <p>{/\.(md|rst|txt)$/i.test(path)
                    ? 'This documentation file has no code dependencies in the loaded graph. Read its source for project context.'
                    : 'No supported dependency edge for this selection is present in the loaded graph. Inspect the source and index coverage before assessing its impact.'}</p>}
                {/* RelationshipEvidence prints the type as text, so a scope type outside the map relations reads the same. */}
                {context.incoming.length > 0 && <details><summary>{selectionScopeText.incoming(context.incoming.length, context.incomingByType)}</summary>
                    <RelationshipEvidence edges={context.incoming as MapEvidence[]} onNavigate={onNavigate} /></details>}
                {context.outgoing.length > 0 && <details><summary>{selectionScopeText.outgoing(context.outgoing.length, context.outgoingByType)}</summary>
                    <RelationshipEvidence edges={context.outgoing as MapEvidence[]} onNavigate={onNavigate} /></details>}
                {context.pathSearchLimited && !context.entryPath.length && <p>No entry path found within 4 calls / 500 visited symbols. Longer paths were not evaluated.</p>}
            </>}
            {agents && <details><summary>Observed agent activity · {context.activity.length} retained events for this file</summary>
            {context.activity.length === 0 ? <p>No observed tool event for this file is available in the loaded agent activity. The Agents view shows the connection and event source.</p>
                : <ul>{context.activity.map(({ agent, event }) => <li key={`${agent}:${event.run}:${event.seq}:${event.phase}`}>
                    <details><summary>{agent} · {event.tool} · {event.phase} · <time dateTime={new Date(event.ts).toISOString()}>{new Date(event.ts).toLocaleTimeString()}</time></summary>
                        <p>Observed tool event for this file. A tool name does not reveal intention or prove a successful change.</p>
                        <pre>{JSON.stringify(event, null, 2)}</pre>
                        <button onClick={() => onNavigate(path, event.lines?.[0])}>Open event location</button>
                    </details>
                </li>)}</ul>}
            </details>}
            <div className="selection-context-actions">{path && <button onClick={() => onNavigate(path, context.selected?.start_line, context.selected?.name)}>{selectionScopeText.readSource}</button>}
                {onImpact && <button onClick={onImpact}>Assess change impact</button>}</div>
        </>}
    </section>;
}
