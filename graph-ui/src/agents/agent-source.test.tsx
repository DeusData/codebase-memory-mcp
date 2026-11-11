// @vitest-environment jsdom
import { act, useLayoutEffect } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { POLL_MS, RECONNECT_MS, useAgentStream, type AgentStream } from './agent-source';

let root: Root;
let container: HTMLDivElement;
let stream: AgentStream;
const event = (seq: number) => ({ ts: seq * 1000, agent: 'fixture', run: 'run', seq,
    phase: 'end', tool: 'Read', path: `file${seq}.ts` });
const page = (events: unknown[], cursor: number, generation = 'first') => ({
    ok: true, json: async () => ({ events, cursor, generation, reset: false,
        retained: events.length, has_more: false, truncated: false }),
});

beforeEach(() => {
    vi.useFakeTimers();
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.append(container);
    root = createRoot(container);
});
afterEach(() => {
    act(() => root.unmount());
    container.remove();
    vi.useRealTimers();
});

function Consumer(props: { on: boolean; project?: string; fetch: typeof globalThis.fetch }): null {
    stream = useAgentStream(props);
    return null;
}

it('does not connect when off and always polls the current daemon, never a bridge', async () => {
    const fetch = vi.fn().mockResolvedValue(page([], 0));
    await act(async () => root.render(<Consumer on={false} project="repo / test" fetch={fetch} />));
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(fetch).not.toHaveBeenCalled();
    await act(async () => root.render(<Consumer on project="repo / test" fetch={fetch} />));
    expect(fetch.mock.calls[0]?.[0]).toBe('/api/agent-events?project=repo%20%2F%20test&after=0&limit=200');
    expect(stream.status.state).toBe('connected');
    await act(async () => root.render(<Consumer on={false} project="repo / test" fetch={fetch} />));
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(fetch).toHaveBeenCalledTimes(1);
});

it('resumes after a connection failure and accepts late events without duplicate counts', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(page([event(2)], 1))
        .mockRejectedValueOnce(new Error('connection lost'))
        .mockResolvedValueOnce(page([event(1), event(2)], 2));
    await act(async () => root.render(<Consumer on project="demo" fetch={fetch} />));
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_MS); });
    expect(stream.status.state).toBe('no-source');
    await act(async () => { await vi.advanceTimersByTimeAsync(RECONNECT_MS); });
    expect(fetch.mock.calls[1]?.[0]).toContain('after=1');
    expect(fetch.mock.calls[2]?.[0]).toContain('after=1');
    expect(stream.state.events).toBe(2);
    expect(stream.state.actors[0]?.last.seq).toBe(2);
    expect(stream.status.drops).toBe(1);
});

it('clears another project or replaced database before accepting its history', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(page([event(2)], 5))
        .mockResolvedValueOnce(page([event(1)], 1, 'replacement'))
        .mockResolvedValueOnce(page([event(1)], 1, 'replacement'))
        .mockResolvedValueOnce(page([], 0, 'replacement'));
    await act(async () => root.render(<Consumer on project="one" fetch={fetch} />));
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_MS + 1); });
    expect(fetch.mock.calls[2]?.[0]).toContain('after=0');
    expect(stream.state.events).toBe(1);
    expect(stream.state.actors[0]?.last.seq).toBe(1);
    await act(async () => root.render(<Consumer on project="two" fetch={fetch} />));
    expect(fetch.mock.calls[3]?.[0]).toContain('project=two&after=0');
    expect(stream.state.events).toBe(0);
});


it('hides prior project activity and provenance before passive effects on project change', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(page([event(1)], 1))
        .mockImplementation(() => new Promise(() => undefined));
    const observed: { project: string; events: number; hello: boolean; status: string }[] = [];
    function Observe({ project }: { project: string }): null {
        const current = useAgentStream({ on: true, project, fetch });
        useLayoutEffect(() => {
            observed.push({ project, events: current.state.events,
                hello: current.status.hello !== undefined, status: current.status.state });
        });
        return null;
    }
    await act(async () => root.render(<Observe project="one" />));
    expect(observed.at(-1)).toMatchObject({ project: 'one', events: 1, hello: true, status: 'connected' });
    await act(async () => root.render(<Observe project="two" />));
    const switched = observed.filter(value => value.project === 'two');
    expect(switched.length).toBeGreaterThan(0);
    expect(switched.every(value => value.events === 0 && !value.hello && value.status === 'connecting')).toBe(true);
    expect(fetch.mock.calls[1]?.[0]).toContain('project=two&after=0');
});
