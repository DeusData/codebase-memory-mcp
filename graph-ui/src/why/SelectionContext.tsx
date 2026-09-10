import { useMemo } from 'react';
import type { GraphData, GraphNode } from '../galaxy/types';
import type { AgentsState } from '../agents/agent-store';
import { selectionContext } from './selection-context';
import { RelationshipEvidence } from '../architecture/RepositoryMap';
import './selection-context.css';

export default function SelectionContextPanel({ graph, selected, path, agents, onNavigate, onImpact }: {
    graph?: GraphData; selected?: GraphNode; path: string; agents?: AgentsState;
    onNavigate: (path: string, line?: number, name?: string) => void; onImpact?: () => void;
}) {
    const context = useMemo(() => selectionContext(graph, selected, path, agents), [graph, selected, path, agents]);
    const callers = context.incoming.filter(edge => edge.type === 'CALLS');
    const callerFiles = new Set(callers.map(edge => edge.source.file_path).filter(Boolean));
    return <section className="selection-context" aria-label="Selection context" data-testid="selection-context">
        <h3>Selection context</h3>
        {!path ? <p>Select a file or symbol to inspect its indexed connections and recorded activity.</p> : <>
            <p className="selection-context-subject">{selected?.name ?? path}</p>
            <h4>Graph evidence</h4>
            {!graph ? <p>The repository graph is not available. Relevance cannot be established yet.</p> : <>
                {context.selected?.status === 'entry' && <p>The index classifies this declaration as an entry candidate. Open its source and follow outgoing calls to verify its role.</p>}
                {callers.length > 0 && <p>{callers.length} indexed caller edges from {callerFiles.size} files reach this selection. Check these callers when changing its contract.</p>}
                {context.entryPath.length > 0 && <details><summary>Reachable from {context.entryPath[0].source.name} · inspect {context.entryPath.length} calls</summary>
                    <p>This selection is reachable from an indexed entry candidate. This is static reachability, not an observed execution or data flow.</p>
                    <RelationshipEvidence edges={context.entryPath} onNavigate={onNavigate} />
                </details>}
                {!context.incoming.length && !context.outgoing.length && <p>{/\.(md|rst|txt)$/i.test(path)
                    ? 'This documentation file has no code dependencies in the loaded graph. Read its source for project context.'
                    : 'No supported dependency edge for this selection is present in the loaded graph. Inspect the source and index coverage before assessing its impact.'}</p>}
                {context.incoming.length > 0 && <details><summary>Incoming relationships · {context.incoming.length}</summary><RelationshipEvidence edges={context.incoming} onNavigate={onNavigate} /></details>}
                {context.outgoing.length > 0 && <details><summary>Outgoing relationships · {context.outgoing.length}</summary><RelationshipEvidence edges={context.outgoing} onNavigate={onNavigate} /></details>}
                {context.pathSearchLimited && !context.entryPath.length && <p>No entry path found within 4 calls / 500 visited symbols. Longer paths were not evaluated.</p>}
            </>}
            <details><summary>Observed agent activity · {context.activity.length} retained events for this file</summary>
            {context.activity.length === 0 ? <p>No observed tool event for this file is available in the loaded agent activity. The Agents view shows the connection and event source.</p>
                : <ul>{context.activity.map(({ agent, event }) => <li key={`${agent}:${event.run}:${event.seq}:${event.phase}`}>
                    <details><summary>{agent} · {event.tool} · {event.phase} · <time dateTime={new Date(event.ts).toISOString()}>{new Date(event.ts).toLocaleTimeString()}</time></summary>
                        <p>Observed tool event for this file. A tool name does not reveal intention or prove a successful change.</p>
                        <pre>{JSON.stringify(event, null, 2)}</pre>
                        <button onClick={() => onNavigate(path, event.lines?.[0])}>Open event location</button>
                    </details>
                </li>)}</ul>}
            </details>
            <div className="selection-context-actions"><button onClick={() => onNavigate(path, context.selected?.start_line, context.selected?.name)}>Read source evidence</button>
                {onImpact && <button onClick={onImpact}>Assess change impact</button>}</div>
        </>}
    </section>;
}
