// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import IndexesPanel from './IndexesPanel';
import { readHealth, type ProjectHealth } from '../projects/projects-model';
import type { ProjectEntry } from '../provider/rpc-schemas';
import type { ConfigSnapshot } from '../settings/config-model';

let container: HTMLDivElement, root: Root;
const configuration = (value = 'true', effective = value, revision = 'r1'): ConfigSnapshot => ({ revision, settings: [{ key: 'watcher_enabled', label: 'Background watcher', description: '', category: 'indexing', type: 'boolean', defaultValue: 'true', value, override: value, effective, source: 'override', applyMode: 'restart', pendingRestart: value !== effective, editable: true }] });
const healthy = (watchRegistered = true, watcherRunning = true) => readHealth({ status: 'healthy', nodes: 42, edges: 73, indexed_at: '2026-09-30T12:00:00Z', watch_registered: watchRegistered, watcher_running: watcherRunning });
const source = () => ({ projectHealth: vi.fn<(name: string) => Promise<ProjectHealth>>().mockResolvedValue(healthy()), configuration: vi.fn().mockResolvedValue(configuration()), saveConfiguration: vi.fn<(revision: string, changes: Record<string, string | null>) => Promise<ConfigSnapshot>>() });
let api: ReturnType<typeof source>;
let listProjects: ReturnType<typeof vi.fn<() => Promise<ProjectEntry[]>>>;
const toggle = () => container.querySelector<HTMLInputElement>('[role="switch"]')!;
const render = async (active = true, refreshToken = 0) => { await act(async () => { root.render(<IndexesPanel api={api} listProjects={listProjects} active={active} paused={false} refreshToken={refreshToken} onOpenProjects={vi.fn()} />); }); };

beforeEach(() => {
    vi.useFakeTimers(); vi.setSystemTime(new Date('2026-10-01T12:00:00Z'));
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
    api = source(); listProjects = vi.fn().mockResolvedValue([{ name: 'persisted', root_path: '/repo/persisted' }]);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); vi.useRealTimers(); });

describe('Persisted index inventory', () => {
    it('shows indexed projects, the recorded time and counts without any job-slot source', async () => {
        await render();
        expect(container.querySelector('tbody')?.textContent).toContain('persisted');
        expect(container.querySelector('tbody time')?.getAttribute('datetime')).toBe('2026-09-30T12:00:00Z');
        expect(container.querySelector('th[title]')?.textContent).toBe('Index timestamp');
        expect(container.querySelector('th[title]')?.getAttribute('title')).toContain('does not prove the current files are indexed');
        expect(container.querySelector('tbody')?.textContent).toContain('42 nodes');
        expect(container.querySelector('tbody')?.textContent).toContain('73 edges');
        expect(container.querySelector('tbody tr td:last-child')?.textContent).toBe('Active');
        expect(container.textContent).not.toMatch(/fresh|up.to.date/i);
        expect(api.projectHealth).toHaveBeenCalledExactlyOnceWith('persisted');
    });
    it('does not infer indexing time or active watching from registration alone', async () => {
        api.projectHealth.mockResolvedValue({ ...healthy(true, false), indexedAt: null });
        await render();
        expect(container.querySelector('tbody time')).toBeNull();
        expect(container.querySelector('tbody td')?.textContent).toBe('Unknown');
        expect(container.querySelector('tbody tr td:last-child')?.textContent).toBe('Registered · daemon watcher stopped');
        api.projectHealth.mockResolvedValue(readHealth({ status: 'healthy' }));
        await render(true, 1);
        expect(container.querySelector('tbody tr td:last-child')?.textContent).toBe('Unknown');
    });
    it('retains previous counts after health or inventory failure while marking watcher state unknown', async () => {
        await render();
        api.projectHealth.mockRejectedValue(new Error('Health unavailable'));
        await render(true, 1);
        expect(container.querySelector('tbody')?.textContent).toContain('42 nodes');
        expect(container.querySelector('tbody')?.textContent).toContain('showing last reading');
        expect(container.querySelector('tbody tr td:last-child')?.textContent).toBe('Unknown');
        listProjects.mockRejectedValue(new Error('Inventory unavailable'));
        await render(true, 2);
        expect(container.querySelector('tbody')?.textContent).toContain('persisted');
        expect(container.textContent).toContain('Showing the last inventory reading');
    });
    it('polls only while active with four concurrent health calls at most', async () => {
        listProjects.mockResolvedValue(Array.from({ length: 6 }, (_, index) => ({ name: `repo-${index}` })));
        const pending: (() => void)[] = [];
        api.projectHealth.mockImplementation(() => new Promise(resolve => { pending.push(() => resolve(healthy())); }));
        await render(false);
        expect(listProjects).not.toHaveBeenCalled();
        expect(api.configuration).not.toHaveBeenCalled();
        await render();
        expect(api.projectHealth).toHaveBeenCalledTimes(4);
        await act(async () => { pending.splice(0).forEach(resolve => resolve()); });
        expect(api.projectHealth).toHaveBeenCalledTimes(6);
        await act(async () => { pending.splice(0).forEach(resolve => resolve()); });
        await render(false);
        await act(async () => { await vi.advanceTimersByTimeAsync(30000); });
        expect(listProjects).toHaveBeenCalledTimes(1);
    });
    it('does not start queued health reads after leaving the page', async () => {
        listProjects.mockResolvedValue(Array.from({ length: 6 }, (_, index) => ({ name: `repo-${index}` })));
        const pending: (() => void)[] = [];
        api.projectHealth.mockImplementation(() => new Promise(resolve => { pending.push(() => resolve(healthy())); }));
        await render(); await render(false);
        await act(async () => { pending.splice(0).forEach(resolve => resolve()); });
        expect(api.projectHealth).toHaveBeenCalledTimes(4);
    });
});

describe('Global background watcher setting', () => {
    it('saves the selected value with the configuration revision and shows pending restart separately', async () => {
        const next = configuration('false', 'true', 'r2');
        api.saveConfiguration.mockImplementation(async () => { api.configuration.mockResolvedValue(next); return next; });
        await render();
        expect(toggle().checked).toBe(true);
        await act(async () => { toggle().click(); });
        expect(api.saveConfiguration).toHaveBeenCalledExactlyOnceWith('r1', { watcher_enabled: 'false' });
        expect(toggle().checked).toBe(false);
        expect(container.textContent).toContain('Saved: Off');
        expect(container.textContent).toContain('Running setting: On');
        expect(container.textContent).toContain('Restart pending');
        expect(container.querySelector('tbody tr td:last-child')?.textContent).toBe('Active');
    });
    it('rolls an optimistic toggle back when a revision-checked save fails', async () => {
        let reject!: (error: Error) => void;
        api.saveConfiguration.mockImplementation(() => new Promise((_resolve, fail) => { reject = fail; }));
        await render();
        await act(async () => { toggle().click(); });
        expect(toggle().checked).toBe(false);
        expect(toggle().disabled).toBe(true);
        await act(async () => { reject(new Error('HTTP 409: configuration changed')); });
        expect(toggle().checked).toBe(true);
        expect(container.querySelector('[role="alert"]')?.textContent).toContain('409');
        expect(container.textContent).toContain('Saved: On');
    });
});
