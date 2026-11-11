import type { DaemonLogRecord, ProcessCpuUnit, ProcessMemoryKind } from '../projects/projects-model';
import { countedDetail, repeatCount } from '../app/ui-log';

export function cpuText(value: number | null | undefined, unit: ProcessCpuUnit | undefined): string {
    if (value == null || !Number.isFinite(value) || value < 0) return 'Unavailable';
    if (unit === 'percent') return `${value.toFixed(1)}%`;
    if (unit === 'seconds') return `${value.toFixed(1)} s total`;
    return 'Unit unavailable';
}

export function memoryText(value: number | null | undefined): string {
    return value == null || !Number.isFinite(value) || value < 0 ? 'Unavailable' : `${value.toFixed(1)} MiB`;
}

export function memoryLabel(kind: ProcessMemoryKind | undefined): string {
    if (kind === 'working_set') return 'Working set';
    if (kind === 'resident') return 'Resident memory';
    if (kind === 'peak_resident') return 'Peak resident memory';
    return 'Memory · kind unspecified';
}

export type LogLevel = 'error' | 'warn' | 'info' | 'debug' | 'trace' | 'other';

/** Only recognize a level token in the prefix, never words inside the message. */
export function logLevel(line: string): LogLevel {
    const trimmed = line.trim();
    let raw: unknown;
    if (trimmed.startsWith('{')) {
        try {
            const record: unknown = JSON.parse(trimmed);
            if (typeof record === 'object' && record !== null && !Array.isArray(record)) raw = (record as Record<string, unknown>)['level'];
        } catch { /* A partial JSON line is unclassified. */ }
    } else {
        raw = /^level=(\w+)(?=\s|$)/i.exec(trimmed)?.[1]
            ?? /^(?:(?:\d{4}-\d{2}-\d{2}[T ][\d:.]+(?:Z|[+-]\d{2}:?\d{2})?)\s+)?\[?(ERROR|WARN(?:ING)?|INFO|DEBUG|TRACE|FATAL)\]?(?=\s|:|$)/i.exec(trimmed)?.[1];
    }
    const token = typeof raw === 'string' ? raw.toLowerCase() : undefined;
    if (token === 'fatal') return 'error';
    if (token === 'warning') return 'warn';
    return token === 'error' || token === 'warn' || token === 'info' || token === 'debug' || token === 'trace' ? token : 'other';
}

export function filterLogs(lines: readonly string[], query: string, level: LogLevel | 'all'): string[] {
    const needle = query.trim().toLocaleLowerCase();
    return lines.filter((line) => (level === 'all' || logLevel(line) === level) && line.toLocaleLowerCase().includes(needle));
}

export interface LogRow { record: DaemonLogRecord; count: number; first: string }

/**
 * The fields that make two records the same message, the ones the frontend
 * buffer compares (app/ui-log.ts): a repeat count carries them too, so it joins
 * the row of the entry it counts. The detail is compared by its start, which a
 * field cut on the longer count line leaves intact.
 */
function repeatIdentity(record: DaemonLogRecord): { key: string; session: string; repeat?: number } {
    try {
        const line: unknown = JSON.parse(record.message);
        if (typeof line === 'object' && line !== null && typeof (line as Record<string, unknown>)['message'] === 'string') {
            const entry = line as Record<string, unknown>;
            const text = (name: string) => typeof entry[name] === 'string' ? entry[name] as string : '';
            const detail = typeof entry['detail'] === 'string' ? entry['detail'] : undefined;
            const repeat = repeatCount(detail);
            return { key: JSON.stringify([record.level, record.source, record.project ?? '', text('message'), (countedDetail(detail) ?? '').slice(0, 2048),
                text('stack').slice(0, 2048), text('url'), entry['line'] ?? null, entry['col'] ?? null]),
            session: text('session'), repeat };
        }
    } catch { /* A daemon line is plain text. */ }
    return { key: JSON.stringify([record.level, record.source, record.project ?? '', record.message]), session: '' };
}

/**
 * Identical messages become one row at their latest position, with how often
 * they occurred. A frontend session records a repeated entry once and then
 * reports running counts (app/ui-log.ts); older sessions recorded every
 * occurrence. Per session the larger of the two is the number of occurrences.
 */
export function collapseRepeats(records: readonly DaemonLogRecord[]): LogRow[] {
    const groups = new Map<string, { last: number; first: string; sessions: Map<string, { plain: number; counted: number }> }>();
    records.forEach((record, index) => {
        const { key, session, repeat } = repeatIdentity(record);
        const group = groups.get(key) ?? { last: index, first: record.ts, sessions: new Map() };
        const tally = group.sessions.get(session) ?? { plain: 0, counted: 0 };
        if (repeat === undefined) tally.plain += 1; else tally.counted = Math.max(tally.counted, repeat);
        group.sessions.set(session, tally); group.last = index; groups.set(key, group);
    });
    return [...groups.values()].sort((a, b) => a.last - b.last).map((group) => ({ record: records[group.last]!, first: group.first,
        count: [...group.sessions.values()].reduce((sum, tally) => sum + Math.max(tally.plain, tally.counted), 0) }));
}

/** The tooltip of a combined row. */
export function repeatTitle(row: LogRow): string {
    return `${row.count.toLocaleString()} identical events from ${row.first} to ${row.record.ts}`;
}
