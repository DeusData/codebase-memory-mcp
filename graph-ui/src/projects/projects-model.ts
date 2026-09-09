/**
 * What the server says about its projects, read strictly, and the small
 * decisions the projects panel makes on top of it.
 *
 * The panel (src/projects/ProjectsPanel.tsx) is the one surface of this
 * frontend that asks the server to WRITE: index a repository, delete an index,
 * store a decision record. Everything it shows before and after such a request
 * comes from the routes below, and every reader here refuses to invent a
 * field: a number the server did not send stays `undefined`, a status it did
 * not name becomes `'unknown'`, and the panel says so.
 *
 * The routes and their shapes, as src/ui/http_server.c writes them:
 *
 *   GET    /api/index-status         [{slot, status, path, error}]
 *   POST   /api/index                {status: 'indexing', slot, path}   (202)
 *   GET    /api/browse?path=         {path, dirs[], parent, roots?[]}
 *   GET    /api/project-health?name= {status, nodes?, edges?, size_bytes?, reason?}
 *   DELETE /api/project?name=        {deleted: true}
 *   GET    /api/adr?project=         {has_adr, content?, updated_at?}
 *   POST   /api/adr                  {saved: true}
 *   GET    /api/logs?lines=          {lines[], total}
 *   GET    /api/ui-log?lines=        {path, previous_path?, size_bytes, partial, lines[], total}
 *   GET    /api/processes            {self_pid, self_rss_mb, ..., processes[]}
 */

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null && !Array.isArray(value);
}

const text = (value: unknown): string => (typeof value === 'string' ? value : '');

function optionalNumber(value: unknown): number | undefined {
    return typeof value === 'number' && Number.isFinite(value) ? value : undefined;
}

function strings(value: unknown): string[] {
    return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === 'string') : [];
}

// ------------------------------------------------------------ index jobs --

export type IndexJobStatus = 'indexing' | 'done' | 'error' | 'unknown';

export interface IndexJob {
    slot: number;
    status: IndexJobStatus;
    path: string;
    /** Only set when `status` is `'error'`; the server sends an empty string otherwise. */
    error: string;
}

function readJobStatus(value: unknown): IndexJobStatus {
    return value === 'indexing' || value === 'done' || value === 'error' ? value : 'unknown';
}

/** The job table. Anything that is not an array of records reads as no jobs. */
export function readIndexJobs(raw: unknown): IndexJob[] {
    if (!Array.isArray(raw)) {
        return [];
    }
    return raw.filter(isRecord).map((job) => ({
        slot: optionalNumber(job['slot']) ?? -1,
        status: readJobStatus(job['status']),
        path: text(job['path']),
        error: text(job['error']),
    }));
}

export interface IndexStarted {
    slot: number;
    path: string;
}

/** The acknowledgement of POST /api/index. */
export function readIndexStarted(raw: unknown): IndexStarted {
    const record = isRecord(raw) ? raw : {};
    return { slot: optionalNumber(record['slot']) ?? -1, path: text(record['path']) };
}

/** True while at least one job is still running, which is when the panel keeps asking. */
export function anyIndexing(jobs: readonly IndexJob[]): boolean {
    return jobs.some((job) => job.status === 'indexing');
}

/**
 * The paths whose job went from running to finished between two readings.
 *
 * The panel reloads the project list exactly then: a list refreshed on every
 * poll would flicker, and one never refreshed would show the new project only
 * after a reload of the page.
 */
export function finishedSince(before: readonly IndexJob[], after: readonly IndexJob[]): string[] {
    const wasRunning = new Set(before.filter((job) => job.status === 'indexing').map((job) => job.slot));
    return after
        .filter((job) => wasRunning.has(job.slot) && job.status !== 'indexing')
        .map((job) => job.path);
}

// ---------------------------------------------------------------- browse --

export interface BrowseLevel {
    path: string;
    dirs: string[];
    parent: string;
    /** Drive letters on Windows; empty elsewhere. */
    roots: string[];
}

export function readBrowse(raw: unknown): BrowseLevel {
    const record = isRecord(raw) ? raw : {};
    return {
        path: text(record['path']),
        dirs: strings(record['dirs']),
        parent: text(record['parent']),
        roots: strings(record['roots']),
    };
}

/** `path` joined with one of its listed folders, without doubling the separator. */
export function childPath(level: BrowseLevel, dir: string): string {
    const base = level.path;
    if (base.length === 0) {
        return dir;
    }
    return base.endsWith('/') ? `${base}${dir}` : `${base}/${dir}`;
}

// ---------------------------------------------------------------- health --

export type HealthStatus = 'healthy' | 'missing' | 'corrupt' | 'unknown';

export interface ProjectHealth {
    status: HealthStatus;
    nodes?: number;
    edges?: number;
    sizeBytes?: number;
    reason: string;
}

export function readHealth(raw: unknown): ProjectHealth {
    const record = isRecord(raw) ? raw : {};
    const status = record['status'];
    return {
        status: status === 'healthy' || status === 'missing' || status === 'corrupt' ? status : 'unknown',
        nodes: optionalNumber(record['nodes']),
        edges: optionalNumber(record['edges']),
        sizeBytes: optionalNumber(record['size_bytes']),
        reason: text(record['reason']),
    };
}

/** Megabytes with one decimal, for a size the server gave in bytes. */
export function megabytes(bytes: number): string {
    return (bytes / (1024 * 1024)).toFixed(1);
}

// ------------------------------------------------------------------- adr --

export interface AdrRecord {
    hasAdr: boolean;
    content: string;
    updatedAt: string;
}

export function readAdr(raw: unknown): AdrRecord {
    const record = isRecord(raw) ? raw : {};
    return {
        hasAdr: record['has_adr'] === true,
        content: text(record['content']),
        updatedAt: text(record['updated_at']),
    };
}

// ------------------------------------------------------------------ logs --

export interface LogTail {
    query?: string;
    scope?: 'project' | 'daemon' | 'unattributed';
    project?: string | null;
    lines: string[];
    total: number;
    /** Absent on older daemons and when persistent storage is unavailable. */
    records?: DaemonLogRecord[];
    persistent?: boolean;
    generation?: string;
    retentionLimit?: number;
    oldestCursor?: number;
    cursor?: number;
    hasMore?: boolean;
}

export interface DaemonLogRecord {
    project?: string | null;
    id: number;
    ts: string;
    level: string;
    source: string;
    message: string;
}

export function readLogs(raw: unknown): LogTail {
    const record = isRecord(raw) ? raw : {};
    const lines = strings(record['lines']);
    const result: LogTail = { lines, total: optionalNumber(record['total']) ?? lines.length };
    if (typeof record['query'] === 'string') result.query = record['query'];
    if (Array.isArray(record['records'])) {
        // A snapshot replaces the previous snapshot. Stable IDs also collapse a
        // repeated row within one response; never append replayed records.
        const rows = new Map<number, DaemonLogRecord>();
        for (const entry of record['records']) {
            if (!isRecord(entry)) continue;
            const id = optionalNumber(entry['id']);
            if (id === undefined || !Number.isSafeInteger(id) || id < 0 || typeof entry['message'] !== 'string') continue;
            rows.set(id, { id, ts: text(entry['ts']), level: text(entry['level']), source: text(entry['source']), message: entry['message'], ...(typeof entry['project'] === 'string' || entry['project'] === null ? { project: entry['project'] } : {}) });
        }
        result.records = [...rows.values()].sort((left, right) => left.id - right.id);
    }
    if (['project', 'daemon', 'unattributed'].includes(String(record['scope']))) result.scope = record['scope'] as LogTail['scope'];
    if (typeof record['project'] === 'string' || record['project'] === null) result.project = record['project'];
    if (typeof record['persistent'] === 'boolean') result.persistent = record['persistent'];
    if (typeof record['generation'] === 'string') result.generation = record['generation'];
    if (typeof record['has_more'] === 'boolean') result.hasMore = record['has_more'];
    for (const [input, output] of [['retention_limit', 'retentionLimit'], ['oldest_cursor', 'oldestCursor'], ['cursor', 'cursor']] as const) {
        const value = optionalNumber(record[input]);
        if (value !== undefined) result[output] = value;
    }
    return result;
}

// ------------------------------------------------------------- processes --

export interface ServerProcess {
    pid: number;
    cpu: number;
    rssMb: number;
    elapsed: string;
    command: string;
    isSelf: boolean;
    /** Unit-aware measurements; legacy numeric fields above remain compatible. */
    telemetry?: { cpu: number | null; memoryMb: number | null };
}

export type ProcessCpuUnit = 'percent' | 'seconds' | 'unknown';
export type ProcessMemoryKind = 'resident' | 'working_set' | 'peak_resident' | 'unknown';

/** Original frontend JSON lines from the daemon journal (POST /api/ui-log). */
export interface UiLogTail {
    /** The authoritative local storage path reported by this daemon. */
    path: string;
    /** A legacy local export archive, when there is one. Empty otherwise. */
    previousPath: string;
    sizeBytes: number;
    /** True when the displayed window or legacy import is partial. */
    partial: boolean;
    /** One JSON object per line, as written by the server. */
    lines: string[];
    total: number;
}

export function readUiLogTail(raw: unknown): UiLogTail {
    const record = isRecord(raw) ? raw : {};
    const lines = strings(record['lines']);
    return {
        path: text(record['path']),
        previousPath: text(record['previous_path']),
        sizeBytes: optionalNumber(record['size_bytes']) ?? 0,
        partial: record['partial'] === true,
        lines,
        total: optionalNumber(record['total']) ?? lines.length,
    };
}

/**
 * One line of the frontend log as the panel shows it: time, level, source
 * and message, with the detail in brackets. The file keeps the JSON; a
 * reader scanning the panel wants the sentence. A line that is not JSON is
 * shown as it is.
 */
export function uiLogLineText(line: string): string {
    let parsed: unknown;
    try {
        parsed = JSON.parse(line);
    } catch {
        return line;
    }
    if (!isRecord(parsed)) {
        return line;
    }
    const when = text(parsed['ts']).length > 0 ? text(parsed['ts']) : text(parsed['received']);
    const level = text(parsed['level']);
    const source = text(parsed['source']);
    const message = text(parsed['message']);
    const detail = text(parsed['detail']);
    const head = [when, level, source.length > 0 ? `${source}:` : ''].filter((part) => part.length > 0).join(' ');
    return `${head} ${message}${detail.length > 0 ? ` (${detail})` : ''}`.trim();
}

export interface ProcessReport {
    selfPid: number;
    selfRssMb: number;
    processes: ServerProcess[];
    telemetry?: {
        cpuUnit: ProcessCpuUnit;
        memoryKind: ProcessMemoryKind;
        selfMemoryKind: ProcessMemoryKind;
        selfMemoryMb: number | null;
    };
}

function measurement(value: unknown, available: unknown): number | null {
    const number = optionalNumber(value);
    return available !== false && number !== undefined && number >= 0 ? number : null;
}

function memoryKind(value: unknown): ProcessMemoryKind {
    return value === 'resident' || value === 'working_set' || value === 'peak_resident' ? value : 'unknown';
}

export function readProcesses(raw: unknown): ProcessReport {
    const record = isRecord(raw) ? raw : {};
    const list = Array.isArray(record['processes']) ? record['processes'].filter(isRecord) : [];
    return {
        selfPid: optionalNumber(record['self_pid']) ?? -1,
        selfRssMb: optionalNumber(record['self_rss_mb']) ?? 0,
        telemetry: {
            cpuUnit: record['cpu_unit'] === 'percent' || record['cpu_unit'] === 'seconds' ? record['cpu_unit'] : 'unknown',
            memoryKind: memoryKind(record['memory_kind']),
            selfMemoryKind: memoryKind(record['self_memory_kind']),
            selfMemoryMb: measurement(record['self_rss_mb'], record['self_memory_available']),
        },
        processes: list.map((entry) => ({
            pid: optionalNumber(entry['pid']) ?? -1,
            cpu: optionalNumber(entry['cpu']) ?? 0,
            rssMb: optionalNumber(entry['rss_mb']) ?? 0,
            elapsed: entry['cpu_available'] === false ? '' : text(entry['elapsed']),
            command: text(entry['command']),
            isSelf: entry['is_self'] === true,
            telemetry: {
                cpu: measurement(entry['cpu'], entry['cpu_available']),
                memoryMb: measurement(entry['rss_mb'], entry['memory_available']),
            },
        })),
    };
}

// ------------------------------------------------------------ the form ----

/**
 * The project name a path suggests: its last segment.
 *
 * The server accepts the name it is given and stores the index under it, so
 * the suggestion only has to be something a reader recognises. Trailing
 * separators are dropped first, because `/repo/` names `repo` and not an empty
 * string. A path that is only separators, or empty, suggests nothing.
 */
export function projectNameFor(path: string): string {
    const trimmed = path.replace(/[\\/]+$/, '');
    const segment = trimmed.split(/[\\/]/).pop() ?? '';
    return segment;
}

/** The address of a project in this window: the page reloads with it. */
export function projectHref(name: string): string {
    return `?project=${encodeURIComponent(name)}`;
}
