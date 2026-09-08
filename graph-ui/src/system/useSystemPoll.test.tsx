// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useSystemPoll } from './useSystemPoll';

let container: HTMLDivElement;
let root: Root;
let visibility: DocumentVisibilityState;
let mounted: boolean;
function Harness({ read, active = true, paused = false }: { read: () => Promise<number>; active?: boolean; paused?: boolean }) {
    const reading = useSystemPoll(read, active, paused, 100);
    return <><output>{JSON.stringify(reading)}</output><button onClick={reading.refresh}>Refresh</button></>;
}
const state = () => JSON.parse(container.querySelector('output')?.textContent ?? '{}');
const render = async (read: () => Promise<number>, active = true, paused = false) => {
    await act(async () => { root.render(<Harness read={read} active={active} paused={paused} />); });
};
const advance = async (milliseconds: number) => { await act(async () => { await vi.advanceTimersByTimeAsync(milliseconds); }); };
const visible = async (next: DocumentVisibilityState) => {
    visibility = next;
    await act(async () => { document.dispatchEvent(new Event('visibilitychange')); });
};

beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-08T20:00:00Z'));
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    visibility = 'visible';
    vi.spyOn(document, 'visibilityState', 'get').mockImplementation(() => visibility);
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    mounted = true;
});
afterEach(async () => {
    if (mounted) await act(async () => { root.unmount(); });
    container.remove();
    vi.restoreAllMocks();
    vi.useRealTimers();
});

describe('System polling lifecycle', () => {
    it('waits for completion before scheduling another read', async () => {
        let resolve!: (value: number) => void;
        const read = vi.fn().mockImplementationOnce(() => new Promise<number>((done) => { resolve = done; })).mockResolvedValue(2);
        await render(read);
        await advance(500);
        expect(read).toHaveBeenCalledTimes(1);
        await act(async () => { resolve(1); });
        expect(state().data).toBe(1);
        await advance(99);
        expect(read).toHaveBeenCalledTimes(1);
        await advance(1);
        expect(read).toHaveBeenCalledTimes(2);
    });
    it('preserves the last success and timestamp after errors, then recovers', async () => {
        const read = vi.fn().mockResolvedValueOnce(7).mockRejectedValueOnce(new Error('Offline')).mockResolvedValue(9);
        await render(read);
        const successTime = state().updatedAt;
        await advance(100);
        expect(state()).toMatchObject({ data: 7, updatedAt: successTime, error: 'Offline', loading: false });
        await advance(100);
        expect(state()).toMatchObject({ data: 9, error: null, loading: false });
        expect(state().updatedAt).toBeGreaterThan(successTime);
    });
    it('pauses recurring updates but permits an explicit refresh', async () => {
        const read = vi.fn().mockResolvedValue(3);
        await render(read);
        await render(read, true, true);
        await advance(500);
        expect(read).toHaveBeenCalledTimes(1);
        await act(async () => { container.querySelector('button')?.click(); });
        expect(read).toHaveBeenCalledTimes(2);
        await advance(500);
        expect(read).toHaveBeenCalledTimes(2);
        await render(read);
        expect(read).toHaveBeenCalledTimes(3);
    });
    it('ignores an older response after leaving and returning to the workspace', async () => {
        let resolve!: (value: number) => void;
        const read = vi.fn().mockImplementationOnce(() => new Promise<number>((done) => { resolve = done; })).mockResolvedValue(2);
        await render(read);
        await render(read, false);
        await advance(300);
        expect(read).toHaveBeenCalledTimes(1);
        await render(read);
        expect(state().data).toBe(2);
        await act(async () => { resolve(1); });
        expect(state().data).toBe(2);
    });
    it('invalidates in-flight reads while the document is hidden', async () => {
        let resolve!: (value: number) => void;
        const read = vi.fn().mockImplementationOnce(() => new Promise<number>((done) => { resolve = done; })).mockResolvedValue(5);
        await render(read);
        await visible('hidden');
        await act(async () => { resolve(1); });
        await advance(500);
        expect(state()).toMatchObject({ data: null, loading: false });
        expect(read).toHaveBeenCalledTimes(1);
        await visible('visible');
        expect(state().data).toBe(5);
    });
    it('cleans up timers and visibility listeners on unmount', async () => {
        const read = vi.fn().mockResolvedValue(1);
        await render(read);
        await act(async () => { root.unmount(); });
        mounted = false;
        await advance(500);
        await visible('hidden');
        await visible('visible');
        expect(read).toHaveBeenCalledTimes(1);
        expect(vi.getTimerCount()).toBe(0);
    });
});
