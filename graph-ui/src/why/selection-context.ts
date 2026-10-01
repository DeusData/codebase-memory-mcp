import type { GraphData, GraphNode } from '../galaxy/types';
import type { AgentEvent } from '../agents/agent-event';
import type { AgentsState } from '../agents/agent-store';
import { MAP_RELATIONS, type MapEvidence, type MapRelation } from '../architecture/repository-map';

export interface SelectionContext {
    selected?: GraphNode;
    incoming: MapEvidence[];
    outgoing: MapEvidence[];
    entryPath: MapEvidence[];
    pathSearchLimited: boolean;
    activity: { agent: string; event: AgentEvent }[];
}

/** All sentences are derived from edges or observed events. No intent inference. */
export function selectionContext(graph: GraphData | undefined, selected: GraphNode | undefined,
    path: string, agents?: AgentsState): SelectionContext {
    const result: SelectionContext = { selected, incoming: [], outgoing: [], entryPath: [], pathSearchLimited: false, activity: [] };
    if (!path) return result;
    for (const actor of agents?.actors ?? []) {
        if (actor.you) continue;
        for (const event of actor.events) {
            if (event.path === path) result.activity.push({ agent: actor.name, event });
        }
    }
    result.activity.sort((a, b) => b.event.ts - a.event.ts || b.event.seq - a.event.seq);
    result.activity = result.activity.slice(0, 8);
    if (!graph) return result;
    const nodes = new Map(graph.nodes.map(node => [node.id, node]));
    // Reindexing may recycle numeric IDs. Resolve the stable source identity
    // against this snapshot before attributing any relationship to a selection.
    if (selected) {
        selected = graph.nodes.find(candidate => selected!.qualified_name
            ? candidate.qualified_name === selected!.qualified_name && candidate.file_path === path
            : candidate.id === selected!.id && candidate.name === selected!.name && candidate.file_path === path);
        result.selected = selected;
        if (!selected) return result;
    }
    const incoming = new Map<number, MapEvidence[]>();
    const seen = new Set<string>();
    for (const edge of graph.edges) {
        if (!MAP_RELATIONS.includes(edge.type as MapRelation)) continue;
        const source = nodes.get(edge.source); const target = nodes.get(edge.target);
        if (!source || !target) continue;
        const key = `${source.id}:${edge.type}:${target.id}`;
        if (seen.has(key)) continue;
        seen.add(key);
        const row = { source, target, type: edge.type as MapRelation, id: edge.id, line: edge.line };
        if (selected ? target.id === selected.id : target.file_path === path && source.file_path !== path) result.incoming.push(row);
        if (selected ? source.id === selected.id : source.file_path === path && target.file_path !== path) result.outgoing.push(row);
        if (edge.type === 'CALLS') {
            const rows = incoming.get(target.id) ?? [];
            rows.push(row); incoming.set(target.id, rows);
        }
    }
    // A shortest static caller path within a stated bound. No import edge may
    // appear in a call path, and no topological ordering is called execution.
    if (selected && selected.status !== 'entry') {
        const queue: { id: number; edges: MapEvidence[] }[] = [{ id: selected.id, edges: [] }];
        const visited = new Set([selected.id]);
        for (let i = 0; i < queue.length && i < 500; i++) {
            const current = queue[i];
            if (current.edges.length >= 4) { if (incoming.get(current.id)?.length) result.pathSearchLimited = true; continue; }
            for (const edge of incoming.get(current.id) ?? []) {
                if (visited.has(edge.source.id)) continue;
                visited.add(edge.source.id);
                const edges = [edge, ...current.edges];
                if (edge.source.status === 'entry') { result.entryPath = edges; return result; }
                if (queue.length < 500) queue.push({ id: edge.source.id, edges });
                else result.pathSearchLimited = true;
            }
        }
    }
    return result;
}
