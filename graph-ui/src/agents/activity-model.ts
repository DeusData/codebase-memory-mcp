import type { AgentEvent, WorkKind } from './agent-event';
import { workKindOf } from './agent-event';
import type { AgentsState } from './agent-store';

export interface ActivityFilter { agent: string; run: string; kind: WorkKind | ''; query: string }
export function activityRows(state: AgentsState, filter: ActivityFilter): AgentEvent[] {
    const query = filter.query.trim().toLocaleLowerCase();
    return state.actors.filter((actor) => !actor.you).flatMap((actor) => actor.events)
        .filter((event) => (!filter.agent || event.agent === filter.agent)
            && (!filter.run || event.run === filter.run)
            && (!filter.kind || workKindOf(event.tool, event.detail) === filter.kind)
            && (!query || [event.path, event.tool, event.detail, event.agent, event.run]
                .join(' ').toLocaleLowerCase().includes(query)))
        .sort((a, b) => b.ts - a.ts || b.seq - a.seq || a.agent.localeCompare(b.agent));
}
