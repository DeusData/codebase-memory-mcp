import type { ProcessCpuUnit, ProcessMemoryKind } from '../projects/projects-model';

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
