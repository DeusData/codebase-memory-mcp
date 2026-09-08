// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import SystemWorkspace, { type SystemWorkspaceProps } from './SystemWorkspace';
import { readProcesses } from '../projects/projects-model';

let container: HTMLDivElement;
let root: Root;
const processes = (unit = 'percent') => readProcesses({ self_pid: 42, self_rss_mb: 150.5, cpu_unit: unit, memory_kind: 'resident', self_memory_kind: 'peak_resident', processes: [{ pid: 42, cpu: 2.5, rss_mb: 120, elapsed: '01:00', command: 'codebase-memory-mcp', is_self: true }] });
const source = (): SystemWorkspaceProps['api'] => ({
    processes: vi.fn().mockResolvedValue(processes()),
    logs: vi.fn().mockResolvedValue({ lines: ['level=INFO msg=ready', 'level=ERROR msg=Index_failed', '{"level":"warn","event":"slow"}'], total: 10 }),
    indexJobs: vi.fn().mockResolvedValue([{ slot: 1, status: 'indexing', path: '/repo', error: '' }]),
});
const click = async (label: string) => {
    const button = [...container.querySelectorAll('button')].find((entry) => entry.textContent === label);
    expect(button).toBeDefined();
    await act(async () => { button?.click(); });
};
const render = async (api = source(), onOpenProjects = vi.fn()) => {
    await act(async () => { root.render(<SystemWorkspace api={api} onOpenProjects={onOpenProjects} pollMs={100} />); });
    return api;
};

beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-09-08T20:00:00Z'));
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
});
afterEach(async () => {
    await act(async () => { root.unmount(); });
    container.remove();
    vi.restoreAllMocks();
    vi.useRealTimers();
});

describe('System workspace', () => {
    it('shows explicit units and serving identity without made-up capacities', async () => {
        await render();
        const overview = container.querySelector('#system-panel-overview')?.textContent;
        expect(overview).toContain('PID 42');
        expect(overview).toContain('Peak resident memory');
        expect(overview).toContain('150.5 MiB');
        expect(overview).toContain('120.0 MiB');
        expect(overview).toContain('2.5%');
        expect(container.querySelector('[role="progressbar"]')).toBeNull();
        const api = source();
        api.processes = vi.fn().mockResolvedValue(processes('seconds'));
        await render(api);
        expect(container.querySelector('#system-panel-overview')?.textContent).toContain('2.5 s total');
    });
    it('shows absent numeric measurements as unavailable', async () => {
        const api = source();
        api.processes = vi.fn().mockResolvedValue(readProcesses({ processes: [{ pid: 42 }] }));
        await render(api);
        const overview = container.querySelector('#system-panel-overview')?.textContent;
        expect(overview).toContain('Unavailable');
        expect(overview).not.toContain('0.0%');
        expect(overview).not.toContain('0.0 MiB');
    });
    it('opens the existing index manager and reads jobs only when needed', async () => {
        const api = source();
        const onOpenProjects = vi.fn();
        await render(api, onOpenProjects);
        expect(api.indexJobs).not.toHaveBeenCalled();
        expect(api.logs).not.toHaveBeenCalled();
        await click('Indexes');
        expect(api.indexJobs).toHaveBeenCalledTimes(1);
        expect(container.querySelector('#system-panel-indexes')?.textContent).toContain('/repo');
        await click('Manage indexes');
        expect(onOpenProjects).toHaveBeenCalledOnce();
        await click('Overview');
        await act(async () => { await vi.advanceTimersByTimeAsync(500); });
        expect(api.indexJobs).toHaveBeenCalledTimes(1);
    });
    it('filters and copies exactly the visible log tail', async () => {
        const writeText = vi.fn().mockResolvedValue(undefined);
        Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
        const api = await render();
        await click('Logs');
        expect(api.logs).toHaveBeenCalledWith(200);
        const select = container.querySelector('select')!;
        await act(async () => { select.value = 'error'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(container.querySelector('.system-log')?.textContent).toBe('level=ERROR msg=Index_failed');
        await click('Copy visible');
        expect(writeText).toHaveBeenCalledWith('level=ERROR msg=Index_failed');
        expect(container.querySelector('[role="status"]')?.textContent).toBe('Visible lines copied');
    });
    it('retains the last successful reading and shows refresh failure', async () => {
        const api = source();
        api.processes = vi.fn().mockResolvedValueOnce(processes()).mockRejectedValue(new Error('Connection lost'));
        await render(api);
        await act(async () => { await vi.advanceTimersByTimeAsync(100); });
        expect(container.querySelector('[role="alert"]')?.textContent).toBe('Connection lost');
        expect(container.textContent).toContain('showing last reading');
        expect(container.textContent).toContain('PID 42');
    });
    it('moves tab selection and focus together with arrow keys', async () => {
        await render();
        const overviewTab = container.querySelector<HTMLButtonElement>('#system-tab-overview')!;
        await act(async () => { overviewTab.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowRight', bubbles: true })); });
        expect(container.querySelector('#system-tab-indexes')?.getAttribute('aria-selected')).toBe('true');
        expect(document.activeElement?.id).toBe('system-tab-indexes');
    });
});
