// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import SystemWorkspace, { type SystemWorkspaceProps } from './SystemWorkspace';
import { readHealth, readLogs, readProcesses } from '../projects/projects-model';

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
const render = async (api = source(), onOpenProjects = vi.fn(), project = 'fixture-a', configuration?: SystemWorkspaceProps['configuration'], listProjects?: SystemWorkspaceProps['listProjects']) => {
    await act(async () => { root.render(<SystemWorkspace api={api} onOpenProjects={onOpenProjects} pollMs={100} project={project} configuration={configuration} listProjects={listProjects} />); });
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
    it('places Configuration second in a vertical navigation and keeps its draft mounted between views', async () => {
        await render(source(), vi.fn(), 'fixture-a', <label>Configuration draft<input aria-label="Configuration draft" defaultValue="saved value" /></label>);
        expect([...container.querySelectorAll('[role="tab"]')].map(tab => tab.textContent)).toEqual(['Overview', 'Configuration', 'Indexes', 'Logs']);
        expect(container.querySelector('[role="tablist"]')?.getAttribute('aria-orientation')).toBe('vertical');
        const panel = container.querySelector<HTMLDivElement>('#system-panel-configuration')!;
        const draft = container.querySelector<HTMLInputElement>('[aria-label="Configuration draft"]')!;
        expect(panel.hidden).toBe(true);
        await click('Configuration');
        expect(panel.hidden).toBe(false);
        expect(container.querySelector('.system-read-status')).toBeNull();
        expect([...container.querySelectorAll('button')].some(button => button.textContent === 'Refresh')).toBe(false);
        draft.value = 'unsaved value';
        await click('Logs');
        expect(panel.hidden).toBe(true);
        await click('Configuration');
        expect(container.querySelector('[aria-label="Configuration draft"]')).toBe(draft);
        expect(draft.value).toBe('unsaved value');
    });
    it('tells embedded configuration when its subpage is active without remounting it', async () => {
        const configuration = vi.fn((active: boolean) => <input aria-label="Embedded settings" data-active={active} defaultValue="draft" />);
        await render(source(), vi.fn(), 'fixture-a', configuration);
        const field = container.querySelector<HTMLInputElement>('[aria-label="Embedded settings"]')!;
        expect(field.getAttribute('data-active')).toBe('false');
        await click('Configuration');
        expect(field.getAttribute('data-active')).toBe('true');
        field.value = 'unsaved';
        await click('Indexes');
        expect(field.getAttribute('data-active')).toBe('false');
        await click('Configuration');
        expect(container.querySelector('[aria-label="Embedded settings"]')).toBe(field);
        expect(field.value).toBe('unsaved');
    });
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
    it('opens the add-project flow and reads jobs only when needed', async () => {
        const api = source();
        const onOpenProjects = vi.fn();
        await render(api, onOpenProjects);
        expect(api.indexJobs).not.toHaveBeenCalled();
        expect(api.logs).not.toHaveBeenCalled();
        await click('Indexes');
        expect(api.indexJobs).toHaveBeenCalledTimes(1);
        expect(container.querySelector('#system-panel-indexes')?.textContent).toContain('/repo');
        await click('Add project index');
        expect(onOpenProjects).toHaveBeenCalledOnce();
        await click('Overview');
        await act(async () => { await vi.advanceTimersByTimeAsync(500); });
        expect(api.indexJobs).toHaveBeenCalledTimes(1);
    });
    it('shows persisted indexes even when the current daemon has no index jobs', async () => {
        const api = source();
        api.indexJobs = vi.fn().mockResolvedValue([]);
        api.projectHealth = vi.fn().mockResolvedValue(readHealth({ status: 'healthy', nodes: 12, edges: 21, indexed_at: '2026-09-07T10:00:00Z', watch_registered: true, watcher_running: false }));
        api.configuration = vi.fn().mockResolvedValue({ revision: 'r1', settings: [] });
        const listProjects = vi.fn().mockResolvedValue([{ name: 'persisted-index', root_path: '/persisted' }]);
        await render(api, vi.fn(), 'fixture-a', undefined, listProjects);
        expect(listProjects).not.toHaveBeenCalled();
        await click('Indexes');
        const inventory = container.querySelector('.system-index-table');
        expect(inventory?.textContent).toContain('persisted-index');
        expect(inventory?.textContent).toContain('12 nodes');
        expect(inventory?.textContent).toContain('Registered · daemon watcher stopped');
        expect(inventory?.querySelector('time')?.getAttribute('datetime')).toBe('2026-09-07T10:00:00Z');
        expect(container.querySelector('.system-index-activity')?.textContent).toContain('No index activity');
    });
    it('fetches errors from retained history rather than filtering the latest 200 info lines', async () => {
        const writeText = vi.fn().mockResolvedValue(undefined);
        Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
        const api = source();
        api.logs = vi.fn().mockResolvedValueOnce({ lines: Array.from({ length: 200 }, (_, i) => `info ${i}`), total: 249, persistent: true })
            .mockResolvedValue({ lines: ['older retained error'], total: 49, persistent: true });
        await render(api); await click('Logs');
        expect(api.logs).toHaveBeenLastCalledWith(200, undefined, undefined, {});
        const select = container.querySelector<HTMLSelectElement>('[aria-label="Log severity"]')!;
        await act(async () => { select.value = 'error'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(api.logs).toHaveBeenLastCalledWith(200, 'error', undefined, {});
        expect(container.querySelector('.system-log')?.textContent).toBe('older retained error');
        expect(container.textContent).toContain('49 matching retained events');
        await click('Copy visible');
        expect(writeText).toHaveBeenCalledWith('older retained error');
        expect(container.querySelector('[role="status"]')?.textContent).toBe('Visible lines copied');
    });
    it('passes scopes to the daemon and refuses ignored scope filters', async () => {
        const api = source();
        api.logs = vi.fn().mockResolvedValueOnce({ lines: ['all history'], total: 10, persistent: true })
            .mockResolvedValueOnce({ lines: ['project A'], total: 1, persistent: true, scope: 'project', project: 'fixture-a' })
            .mockResolvedValue({ lines: ['wrong daemon-wide result'], total: 10, persistent: true, scope: 'daemon' });
        await render(api); await click('Logs');
        const select = container.querySelector<HTMLSelectElement>('[aria-label="Log scope"]')!;
        await act(async () => { select.value = 'project'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(api.logs).toHaveBeenLastCalledWith(200, undefined, 'fixture-a', {});
        expect(container.querySelector('.system-log')?.textContent).toContain('project A');
        await act(async () => { select.value = 'unattributed'; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(api.logs).toHaveBeenLastCalledWith(200, undefined, undefined, { scope: 'unattributed' });
        expect(container.querySelector('[role="alert"]')?.textContent).toContain('did not confirm unattributed scope');
        expect(container.querySelector('.system-log')?.textContent).not.toContain('project A');
        expect(container.querySelector('.system-log')?.textContent).not.toContain('wrong daemon-wide');
    });
    it('loads retained history without a separate text-search input or query', async () => {
        const api = source();
        api.logs = vi.fn().mockResolvedValue({ lines: ['latest retained event'], total: 1, persistent: true });
        await render(api); await click('Logs');
        expect(container.querySelector('input[type="search"]')).toBeNull();
        expect(api.logs).toHaveBeenLastCalledWith(200, undefined, undefined, {});
        expect(container.querySelector('.system-log')?.textContent).toContain('latest retained event');
        expect(container.querySelector('select[aria-label="Log severity"]')).not.toBeNull();
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
        await act(async () => { overviewTab.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowDown', bubbles: true })); });
        expect(container.querySelector('#system-tab-configuration')?.getAttribute('aria-selected')).toBe('true');
        expect(document.activeElement?.id).toBe('system-tab-configuration');
        await act(async () => { document.activeElement?.dispatchEvent(new KeyboardEvent('keydown', { key: 'End', bubbles: true })); });
        expect(container.querySelector('#system-tab-logs')?.getAttribute('aria-selected')).toBe('true');
        expect(document.activeElement?.id).toBe('system-tab-logs');
        await act(async () => { document.activeElement?.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowUp', bubbles: true })); });
        expect(document.activeElement?.id).toBe('system-tab-indexes');
    });
    it('shows persisted error severity, source, receipt time and path in the real log view', async () => {
        const api = source();
        api.logs = vi.fn().mockResolvedValue(readLogs({ lines: ['fixture failure'], total: 1, persistent: true, retention_limit: 5000, generation: 'fixture', records: [{ id: 17, ts: '2026-09-09T12:00:00Z', level: 'error', source: 'ui.index.done', message: 'project=fixture path=src/broken.ts rc=err' }] }));
        await render(api); await click('Logs');
        const record = container.querySelector('.system-log-record.system-log-error');
        expect(record?.textContent).toContain('ERROR'); expect(record?.textContent).toContain('ui.index.done');
        expect(record?.textContent).toContain('src/broken.ts'); expect(record?.textContent).toContain('Event #17');
        expect(record?.querySelector('time')?.dateTime).toBe('2026-09-09T12:00:00Z');
        expect(container.textContent).toContain('SQLite history');
    });
    it('combines repeated identical frontend warnings into one line with a count', async () => {
        const api = source();
        const clock = 'THREE.THREE.Clock: This module has been deprecated. Please use THREE.Timer instead.';
        const records = Array.from({ length: 120 }, (_, index) => ({ id: 100 + index, ts: '2026-10-03T16:05:22Z', level: 'warn', source: 'console', project: 'cbm',
            message: JSON.stringify({ session: 'a31adcaf', seq: index + 2, level: 'warn', source: 'console', message: clock, project: 'cbm' }) }));
        records.push({ id: 400, ts: '2026-10-03T16:06:00Z', level: 'error', source: 'rpc', project: 'cbm', message: JSON.stringify({ session: 'a31adcaf', seq: 300, level: 'error', source: 'rpc', message: '/rpc query_graph: Failed to fetch' }) });
        api.logs = vi.fn().mockResolvedValue(readLogs({ lines: [], total: 121, persistent: true, retention_limit: 5000, generation: 'fixture', records }));
        await render(api); await click('Logs');
        const rows = [...container.querySelectorAll('.system-log-record')];
        expect(rows.filter((row) => row.textContent?.includes('THREE.THREE.Clock'))).toHaveLength(1);
        expect(rows.find((row) => row.textContent?.includes('THREE.THREE.Clock'))?.querySelector('.system-log-repeat')?.textContent).toBe('×120');
        expect(rows.find((row) => row.textContent?.includes('Failed to fetch'))?.querySelector('.system-log-repeat')).toBeNull();
    });
});
