import type { AgentEvent, WorkKind } from './agent-event';
import { workKindOf } from './agent-event';
import type { AgentsState } from './agent-store';

export interface ActivityFilter { agent: string; run: string; kind: WorkKind | '' }
export function activityRows(state: AgentsState, filter: ActivityFilter): AgentEvent[] {
    return state.actors.filter((actor) => !actor.you).flatMap((actor) => actor.events)
        .filter((event) => (!filter.agent || event.agent === filter.agent)
            && (!filter.run || event.run === filter.run)
            && (!filter.kind || workKindOf(event.tool, event.detail) === filter.kind))
        .sort((a, b) => b.ts - a.ts || b.seq - a.seq || a.agent.localeCompare(b.agent));
}
