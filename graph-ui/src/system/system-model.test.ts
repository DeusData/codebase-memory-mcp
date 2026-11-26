import { describe, expect, it } from 'vitest';
import { readProcesses } from '../projects/projects-model';
import { collapseRepeats, cpuText, filterLogs, logLevel, memoryLabel, memoryText } from './system-model';
import { repeatDetail, UiLogBuffer } from '../app/ui-log';
import type { UiLogPayload } from '../app/ui-log';

describe('System measurements', () => {
    it('keeps missing readings distinct from measured zero', () => {
        const report = readProcesses({ processes: [{ pid: 1 }, { pid: 2, cpu: 0, rss_mb: 0 }] });
        expect(report.telemetry).toEqual({ cpuUnit: 'unknown', memoryKind: 'unknown', selfMemoryKind: 'unknown', selfMemoryMb: null });
        expect(report.processes[0]?.telemetry).toEqual({ cpu: null, memoryMb: null });
        expect(report.processes[1]?.telemetry).toEqual({ cpu: 0, memoryMb: 0 });
        expect(memoryText(null)).toBe('Unavailable');
        expect(memoryText(0)).toBe('0.0 MiB');
        expect(cpuText(0, 'percent')).toBe('0.0%');
    });
    it('honors failed measurements and rejects invalid or negative values', () => {
        const report = readProcesses({ self_rss_mb: 0, self_memory_available: false, processes: [
            { cpu: 0, rss_mb: 0, elapsed: '0-00:00:00', cpu_available: false, memory_available: false },
            { cpu: -1, rss_mb: Infinity },
            { cpu: '12', rss_mb: NaN },
        ] });
        expect(report.telemetry?.selfMemoryMb).toBeNull();
        expect(report.processes[0]?.elapsed).toBe('');
        for (const process of report.processes) expect(process.telemetry).toEqual({ cpu: null, memoryMb: null });
    });
    it('does not guess CPU or memory semantics from an older server', () => {
        expect(cpuText(19.5, 'unknown')).toBe('Unit unavailable');
        expect(cpuText(19.5, undefined)).toBe('Unit unavailable');
        expect(cpuText(19.5, 'seconds')).toBe('19.5 s total');
        expect(cpuText(19.5, 'percent')).toBe('19.5%');
        expect(memoryLabel('peak_resident')).toBe('Peak resident memory');
        expect(memoryLabel('working_set')).toBe('Working set');
    });
});

describe('System log levels', () => {
    it('reads actual daemon text and JSON formats as well as prefixed legacy lines', () => {
        expect(logLevel('level=INFO msg=ready')).toBe('info');
        expect(logLevel('{"level":"WARN","event":"slow"}')).toBe('warn');
        expect(logLevel('2026-09-08T19:50:20Z [ERROR] index failed')).toBe('error');
        expect(logLevel('DEBUG worker ready')).toBe('debug');
        expect(logLevel('FATAL: stopped')).toBe('error');
    });
    it('does not treat message contents as a level', () => {
        expect(logLevel('A message about ERROR conditions')).toBe('other');
        expect(logLevel('{"level":"INFO","event":"ERROR"}')).toBe('info');
        expect(logLevel('{"event":"error"}')).toBe('other');
        expect(logLevel('{"level":"error"')).toBe('other');
    });
    it('intersects case-insensitive text and level filters without altering source lines', () => {
        const lines = ['level=INFO msg=Indexer', 'level=ERROR msg=Indexer', 'level=ERROR msg=Daemon'];
        expect(filterLogs(lines, 'INDEX', 'error')).toEqual([lines[1]]);
        expect(filterLogs(lines, '', 'all')).toEqual(lines);
    });

    it('shows identical frontend messages once with how often they occurred', () => {
        const clock = 'THREE.THREE.Clock: This module has been deprecated. Please use THREE.Timer instead.';
        const line = (session: string, seq: number, message: string, detail?: string) => JSON.stringify({ received: '2026-10-03T16:05:22Z', page: '/?project=cbm', session, seq, ts: '2026-10-03T16:05:20.914Z', level: 'warn', source: 'console', message, ...(detail ? { detail } : {}), project: 'cbm' });
        const record = (id: number, message: string, source = 'console') => ({ id, ts: `2026-10-03T16:05:${String(id).padStart(2, '0')}Z`, level: 'warn', source, message, project: 'cbm' });
        const rows = collapseRepeats([
            // Before the fix every occurrence was its own record; after it, one record and its counts.
            record(1, line('old', 3, clock)), record(2, line('old', 5, clock)), record(3, 'level=warn msg=watcher.slow path=src'),
            record(4, line('old', 9, clock)), record(5, line('new', 2, clock)), record(6, line('new', 3, 'Another warning')),
            record(7, line('new', 4, clock, repeatDetail(10))), record(8, line('new', 5, clock, repeatDetail(100))), record(9, line('new', 6, clock, repeatDetail(120))),
        ]);
        expect(rows.map((row) => [row.record.id, row.count])).toEqual([[3, 1], [6, 1], [9, 123]]);
        expect(rows[2]?.first).toBe('2026-10-03T16:05:01Z');
    });

    it('keeps a repeated request failure with a response body on one row with its count, and its body', async () => {
        // What the buffer actually posts for twelve identical failures, stored the way the daemon stores them.
        const posted: UiLogPayload[] = [];
        const buffer = new UiLogBuffer({ page: '/?project=cbm', session: 'rpc12', schedule: () => 0, cancel: () => undefined, bufferMax: 200, batchMax: 200,
            transport: { send: async (payload) => { posted.push(payload); return true; } } });
        for (let i = 0; i < 12; i++) buffer.record('error', 'rpc', '/rpc query_graph: HTTP 500', { project: 'cbm', detail: 'HTTP 500 body text' });
        buffer.record('error', 'rpc', '/rpc query_graph: HTTP 500', { project: 'cbm', detail: 'another body' });
        await buffer.flush(true);
        const records = posted.flatMap((post) => post.entries.map((entry) => ({ ...entry, session: post.session, page: post.page })))
            .map((line, index) => ({ id: index + 1, ts: line.ts, level: line.level, source: line.source, project: line.project, message: JSON.stringify(line) }));
        const rows = collapseRepeats(records);
        expect(rows.map((row) => [JSON.parse(row.record.message).detail.split('\n').at(-1), row.count])).toEqual([['another body', 1], ['HTTP 500 body text', 12]]);
    });
});
