// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import RenderProgress from './RenderProgress';

let host: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    vi.useFakeTimers();
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.useRealTimers(); });
const render = (busy: boolean, label?: string) => act(async () => root.render(<RenderProgress busy={busy} label={label} />));
const advance = (milliseconds: number) => act(async () => { await vi.advanceTimersByTimeAsync(milliseconds); });

it('shows an accessible status only after the view has been busy for 300ms', async () => {
    await render(true);
    await advance(299);
    expect(host.querySelector('[role="status"]')).toBeNull();
    await advance(1);
    const status = host.querySelector('[role="status"]')!;
    expect(status.textContent).toBe('Updating view…');
    expect(status.getAttribute('aria-live')).toBe('polite');
    expect(status.getAttribute('aria-atomic')).toBe('true');
    expect(status.querySelector('[aria-hidden="true"]')).not.toBeNull();
    expect(status.querySelector('button')).toBeNull();
    await render(false);
    expect(host.querySelector('[role="status"]')).toBeNull();
});

it('cancels fast updates and starts a fresh delay for the next update', async () => {
    await render(true); await advance(200); await render(false); await advance(200);
    expect(host.querySelector('[role="status"]')).toBeNull();
    await render(true); await advance(299);
    expect(host.querySelector('[role="status"]')).toBeNull();
    await advance(1);
    expect(host.querySelector('[role="status"]')).not.toBeNull();
});

it('changes the phase label without restarting a pending reveal', async () => {
    await render(true, 'Reading relationships…'); await advance(200);
    await render(true, 'Arranging nodes…'); await advance(100);
    expect(host.querySelector('[role="status"]')?.textContent).toBe('Arranging nodes…');
});

it('cleans up the timer when the graph view unmounts', async () => {
    await render(true);
    await act(async () => root.render(null));
    expect(vi.getTimerCount()).toBe(0);
    await advance(500);
    expect(host.textContent).toBe('');
});
