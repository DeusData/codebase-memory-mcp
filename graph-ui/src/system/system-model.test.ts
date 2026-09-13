import { describe, expect, it } from 'vitest';
import { readProcesses } from '../projects/projects-model';
import { cpuText, filterLogs, logLevel, memoryLabel, memoryText } from './system-model';

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
});
