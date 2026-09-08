import { expect, it } from 'vitest';
import { activityRows } from './activity-model';
import { emptyAgentsState, withEvent } from './agent-store';
import type { AgentEvent } from './agent-event';

const event = (patch: Partial<AgentEvent>): AgentEvent => ({
    agent: 'Ada', run: 'one', seq: 1, ts: 1000, tool: 'Read', phase: 'end',
    path: 'src/main.ts', detail: 'inspect', source: 'bridge', replay: false, ...patch,
});
const state = [event({}), event({ seq: 2, ts: 2000, tool: 'Edit', path: 'missing.ts' }),
    event({ agent: 'Ben', run: 'two', ts: 3000, path: '' })]
    .reduce(withEvent, emptyAgentsState());
const all = { agent: '', run: '', kind: '' as const, query: '' };

it('keeps unmapped events and orders recorded activity newest first', () => {
    expect(activityRows(state, all).map((row) => row.path)).toEqual(['', 'missing.ts', 'src/main.ts']);
});
it('intersects agent, run, work kind and text filters', () => {
    expect(activityRows(state, { agent: 'Ada', run: 'one', kind: 'write', query: 'MISSING' }))
        .toEqual([event({ seq: 2, ts: 2000, tool: 'Edit', path: 'missing.ts' })]);
    expect(activityRows(state, { ...all, agent: 'Ben', run: 'one' })).toEqual([]);
});
it('does not change the store when ordering or filtering', () => {
    const before = state.actors.map((actor) => actor.events.map((row) => row.seq));
    activityRows(state, all);
    expect(state.actors.map((actor) => actor.events.map((row) => row.seq))).toEqual(before);
});
