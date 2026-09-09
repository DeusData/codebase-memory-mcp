// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { AtlasApi } from '../app/atlas-api';
import { readLogs } from '../projects/projects-model';
import { buildCoverageIndex } from '../app/tree-model';
import DaemonAlerts from './DaemonAlerts';
import { useDaemonLogFeed } from './useDaemonLogFeed';

let root: Root; let container: HTMLDivElement;
beforeEach(() => {
    vi.useFakeTimers();
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); vi.useRealTimers(); });
const snapshot = (generation = 'db-a') => ({ lines: ['WARN routine', 'ERROR fixture parse failed'], total: 2, persistent: true, generation, retention_limit: 5000, records: [
    { id: 2, ts: '2026-09-09T12:00:01Z', level: 'warn', source: 'index.start', message: 'Routine warning' },
    { id: 1, ts: '2026-09-09T12:00:00Z', level: 'error', source: 'index.file', message: 'Fixture parse failure: src/broken.ts' },
] });
function Harness({ api }: { api: Pick<AtlasApi, 'logs'> }) { return <DaemonAlerts reading={useDaemonLogFeed(api, true, 100)} />; }

describe('Shared persistent daemon event feed', () => {
    it('reads same-origin warn/error snapshots, prioritizes errors and recovers without duplicates', async () => {
        let turn = 0; const calls: string[] = [];
        const fetch = vi.fn(async (url: string | URL | Request) => {
            calls.push(String(url)); turn += 1;
            if (turn === 2) throw new Error('connection interrupted');
            return { ok: true, status: 200, text: async () => JSON.stringify(snapshot(turn > 3 ? 'db-after-restart' : 'db-a')) } as Response;
        });
        const api = new AtlasApi({ fetch: fetch as typeof globalThis.fetch });
        await act(async () => root.render(<Harness api={api} />));
        expect(calls).toEqual(['/api/logs?lines=200&min_level=warn']);
        expect(container.querySelector('li')).toBeNull();
        expect(container.querySelector('[data-testid="daemon-alerts"]')?.className).not.toContain('daemon-alerts-has-errors');
        await act(async () => [...container.querySelectorAll('button')].find(button => button.textContent === 'History (2 retained)')!.click());
        expect(container.querySelector('li')?.className).toBe('daemon-alert-error');
        expect(container.querySelector('li')?.textContent).toContain('! ERROR');
        expect(container.querySelector('li')?.textContent).toContain('src/broken.ts');
        expect(container.querySelector('time')?.dateTime).toBe('2026-09-09T12:00:00Z');
        await act(async () => vi.advanceTimersByTimeAsync(100));
        expect(container.querySelector('[role="alert"]')?.textContent).toContain('connection interrupted');
        expect(container.querySelectorAll('li')).toHaveLength(2);
        await act(async () => vi.advanceTimersByTimeAsync(200));
        expect(container.querySelector('[role="alert"]')).toBeNull();
        expect(container.querySelectorAll('li')).toHaveLength(2);
        expect(container.textContent).toContain('shared history limit 5000');
        expect(calls.every((url) => url === '/api/logs?lines=200&min_level=warn')).toBe(true);
    });
    it('keeps historical explanation and secondary controls inside the expanded history', async () => {
        const data = readLogs(snapshot());
        const refresh = vi.fn(), onScopeChange = vi.fn();
        await act(async () => root.render(<DaemonAlerts project="fixture" reading={{ data, loading: false, updatedAt: 1, error: null, refresh }} onScopeChange={onScopeChange} />));
        expect(container.querySelectorAll('.diagnostics-caveat')).toHaveLength(0);
        expect([...container.querySelectorAll('button')].map(button => button.textContent)).toEqual(['History (2 retained)']);
        await act(async () => container.querySelector('button')!.click());
        expect(container.textContent).toContain('Recorded history, not a count of active failures');
        expect(container.textContent).toContain('All daemon history');
        expect(container.textContent).toContain('Refresh events');
    });
    it('retains legacy tails but marks unavailable persistence and avoids a clean bill of health', async () => {
        const data = readLogs({ lines: ['ERROR legacy failure'], total: 1, persistent: false });
        await act(async () => root.render(<DaemonAlerts reading={{ data, loading: false, updatedAt: null, error: null }} />));
        expect(container.textContent).toContain('Persistent event history unavailable');
        expect(container.querySelector('.daemon-alert-error')).toBeNull();
        await act(async () => [...container.querySelectorAll('button')].find(button => button.textContent === 'History (1 retained)')!.click());
        expect(container.querySelector('.daemon-alert-error')).not.toBeNull();
        await act(async () => root.render(<DaemonAlerts reading={{ data: readLogs({ lines: [], total: 0, persistent: true, records: [] }), loading: false, updatedAt: null, error: null }} />));
        expect(container.textContent).toContain('Unrecorded events and indexing gaps may still exist');
    });
    it('validates and deduplicates durable row IDs', () => {
        const input = snapshot(); input.records.push(input.records[0]!);
        const parsed = readLogs({ ...input, records: [...input.records, { id: -1, message: 'bad' }] });
        expect(parsed.records?.map((row) => row.id)).toEqual([1, 2]);
    });
    it('shows shared index gaps immediately without a percentage or triggering diagnosis', async () => {
        const onDiagnose = vi.fn();
        const coverage = buildCoverageIndex({ scopes: [{ requestedScope: '.', scope: '.', total: 2, status: 'complete', hasMore: false, entries: [{ path: 'broken.ts', kind: 'parse_partial', detail: '3-5' }, { path: 'vendor/', kind: 'not_indexed_dir', detail: 'gitignore' }] }] });
        await act(async () => root.render(<DaemonAlerts reading={{ data: readLogs({ lines: [], records: [], total: 0, persistent: true }), updatedAt: null, loading: false, error: null }} coverage={coverage} coverageKnown onDiagnose={onDiagnose} />));
        expect(container.querySelector('.daemon-coverage-summary')?.textContent).toContain('1 partially parsed paths · 0 skipped · 1 intentionally excluded');
        expect(container.textContent).not.toMatch(/\d+%/); expect(onDiagnose).not.toHaveBeenCalled();
    });
    it('shows the actual JSON message ahead of receipt metadata and retains raw evidence', async () => {
        const raw = JSON.stringify({ received: '2026-09-09T12:00:00Z', session: 'session-id', message: 'Parser failed in src/broken.ts' });
        const data = readLogs({ lines: [raw], records: [{ id: 20, ts: '2026-09-09T12:00:00Z', level: 'error', source: 'parser', message: raw }], total: 1, persistent: true });
        await act(async () => root.render(<DaemonAlerts reading={{ data, loading: false, updatedAt: null, error: null }} />));
        await act(async () => [...container.querySelectorAll('button')].find(button => button.textContent === 'History (1 retained)')!.click());
        expect(container.querySelector('li > pre')?.textContent).toBe('Parser failed in src/broken.ts');
        expect(container.querySelector('details pre')?.textContent).toBe(raw);
        expect(container.textContent).toContain('Recorded history, not a count of active failures');
    });
    it('announces a genuinely new error and acknowledges it without deleting history', async () => {
        const first = readLogs(snapshot());
        const render = async (data: typeof first) => act(async () => root.render(<DaemonAlerts project="real-project" reading={{ data, loading: false, updatedAt: 1, error: null }} />));
        await render(first);
        expect(container.querySelector('.daemon-alert-error')).toBeNull();
        const next = readLogs({ ...snapshot(), total: 3, records: [...snapshot().records, { id: 3, ts: '2026-09-09T12:01:00Z', level: 'error', source: 'index.worker', message: 'New actual failure', project: 'real-project' }] });
        await render(next);
        expect(container.querySelector('.daemon-alert-error')?.textContent).toContain('New actual failure');
        expect(container.textContent).toContain('1 new recorded errors · real-project');
        await act(async () => [...container.querySelectorAll('button')].find(button => button.textContent === 'Acknowledge new events')!.click());
        expect(container.querySelector('.daemon-alert-error')).toBeNull();
        await act(async () => [...container.querySelectorAll('button')].find(button => button.textContent === 'History (3 retained)')!.click());
        expect(container.querySelectorAll('li')).toHaveLength(3);
        expect(container.textContent).toContain('New actual failure');
    });
    it('requires server project attribution and never shows a previous project while a new request fails', async () => {
        const calls: string[] = [];
        const fetch = vi.fn(async (url: string | URL | Request) => {
            calls.push(String(url));
            if (String(url).includes('project=B')) throw new Error('B unavailable');
            return { ok: true, status: 200, text: async () => JSON.stringify({ ...snapshot(), scope: 'project', project: 'A' }) } as Response;
        });
        const api = new AtlasApi({ fetch: fetch as typeof globalThis.fetch });
        function Scoped({ project }: { project: string }) {
            const feed = useDaemonLogFeed(api, true, 100, project);
            return <div>{feed.data?.project}:{feed.error}</div>;
        }
        await act(async () => root.render(<Scoped project="A" />));
        expect(container.textContent).toBe('A:');
        await act(async () => root.render(<Scoped project="B" />));
        expect(container.textContent).toContain('B unavailable');
        expect(container.textContent).not.toContain('A:');
        expect(calls).toEqual(['/api/logs?lines=200&min_level=warn&project=A', '/api/logs?lines=200&min_level=warn&project=B']);
    });
    it('retains B during a failed refresh even when an older A request completes late', async () => {
        let completeA!: (value: ReturnType<typeof readLogs>) => void;
        let bReads = 0;
        const api = { logs: vi.fn((_lines: number, _level?: string, project?: string) => {
            if (project === 'A') return new Promise<ReturnType<typeof readLogs>>(resolve => { completeA = resolve; });
            bReads += 1;
            if (bReads > 1) return Promise.reject(new Error('B refresh interrupted'));
            return Promise.resolve(readLogs({ ...snapshot(), scope: 'project', project: 'B' }));
        }) };
        function Scoped({ project }: { project: string }) {
            const feed = useDaemonLogFeed(api, true, 100, project);
            return <div>{feed.data?.project}:{feed.error}</div>;
        }
        await act(async () => root.render(<Scoped project="A" />));
        await act(async () => root.render(<Scoped project="B" />));
        expect(container.textContent).toBe('B:');
        await act(async () => completeA(readLogs({ ...snapshot(), scope: 'project', project: 'A' })));
        expect(container.textContent).toBe('B:');
        await act(async () => vi.advanceTimersByTimeAsync(100));
        expect(container.textContent).toBe('B:B refresh interrupted');
    });

});
