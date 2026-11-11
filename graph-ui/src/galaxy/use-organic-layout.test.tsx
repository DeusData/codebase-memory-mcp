// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useOrganicLayout, type OrganicLayoutReading } from './use-organic-layout';
import type { OrganicClusterLayout, OrganicClusterOptions } from './organic-clusters';
import type { OrganicLayoutRequest, OrganicLayoutResponse } from './organic-layout.worker';
import type { GraphData } from './types';

vi.mock('./organic-clusters', () => ({ layoutOrganicClusters: (data: GraphData) => ({ data, groups: [], groupByNode: new Map() }) }));
class WorkerStub {
    static instances: WorkerStub[] = [];
    onmessage: ((event: MessageEvent<OrganicLayoutResponse>) => void) | null = null;
    onerror: ((event: ErrorEvent) => void) | null = null;
    onmessageerror: (() => void) | null = null;
    postMessage = vi.fn<(request: OrganicLayoutRequest) => void>();
    terminate = vi.fn();
    constructor() { WorkerStub.instances.push(this); }
    reply(sequence: number, data: GraphData) { this.onmessage?.(new MessageEvent<OrganicLayoutResponse>('message', { data: { sequence, result: { data, groups: [], groupByNode: new Map() } } })); }
}

let host: HTMLDivElement, root: Root, observed: OrganicLayoutReading;
const options: OrganicClusterOptions = { rootIds: new Set([1]) };
const first: GraphData = { nodes: [], edges: [], total_nodes: 1 };
const second: GraphData = { nodes: [], edges: [], total_nodes: 2 };
function Harness({ data, enabled = true }: { data?: GraphData; enabled?: boolean }) {
    observed = useOrganicLayout(data, options, enabled);
    return null;
}
async function render(data?: GraphData, enabled = true) { await act(async () => root.render(<Harness data={data} enabled={enabled} />)); }
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    WorkerStub.instances = []; vi.stubGlobal('Worker', WorkerStub);
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.unstubAllGlobals(); vi.useRealTimers(); });

it('ignores obsolete replies and never presents the previous source as current', async () => {
    await render(first);
    const worker = WorkerStub.instances[0]!;
    const firstRequest = worker.postMessage.mock.calls[0]![0];
    expect(observed.loading).toBe(true);
    await act(async () => worker.reply(firstRequest.sequence, first));
    expect(observed.result?.data).toBe(first);
    await render(second);
    expect(observed.result).toBeUndefined(); expect(observed.loading).toBe(true);
    // The previous picture stays available to keep a scene mounted, but only as stale.
    expect(observed.stale?.data).toBe(first);
    await act(async () => worker.reply(firstRequest.sequence, first));
    expect(observed.result).toBeUndefined(); expect(observed.stale?.data).toBe(first);
    const secondRequest = worker.postMessage.mock.calls[1]![0];
    await act(async () => worker.reply(secondRequest.sequence, second));
    expect(observed.result?.data).toBe(second); expect(observed.loading).toBe(false);
    expect(observed.stale).toBeUndefined();
    expect(WorkerStub.instances).toHaveLength(1);
});

it('forgets the stale picture after a gap without input', async () => {
    await render(first);
    const worker = WorkerStub.instances[0]!;
    await act(async () => worker.reply(worker.postMessage.mock.calls[0]![0].sequence, first));
    await render(undefined);
    expect(observed.result).toBeUndefined(); expect(observed.stale).toBeUndefined(); expect(observed.loading).toBe(false);
    await render(second);
    expect(observed.loading).toBe(true); expect(observed.stale).toBeUndefined();
});

it('retains its worker while disabled and terminates it on unmount', async () => {
    await render(first);
    const worker = WorkerStub.instances[0]!;
    await render(first, false);
    expect(observed.loading).toBe(false); expect(observed.result).toBeUndefined();
    expect(worker.terminate).not.toHaveBeenCalled();
    await act(async () => root.render(null));
    expect(worker.terminate).toHaveBeenCalledOnce();
});

it('reports worker errors without silently switching to main-thread computation', async () => {
    await render(first);
    const worker = WorkerStub.instances[0]!;
    await act(async () => worker.onerror?.({ message: 'Worker module unavailable', preventDefault: vi.fn() } as unknown as ErrorEvent));
    expect(observed.error).toBe('Worker module unavailable');
    expect(observed.loading).toBe(false); expect(observed.result).toBeUndefined();
    expect(worker.terminate).toHaveBeenCalledOnce();
});

it('defers computation when Worker is unavailable', async () => {
    vi.stubGlobal('Worker', undefined); vi.useFakeTimers();
    await render(first);
    expect(observed.loading).toBe(true); expect(observed.result).toBeUndefined();
    await act(async () => { await vi.advanceTimersByTimeAsync(0); await vi.dynamicImportSettled(); });
    expect(observed.result).toEqual({ data: first, groups: [], groupByNode: new Map() } satisfies OrganicClusterLayout);
    expect(observed.loading).toBe(false);
});
