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
    lines: string[];
    total: number;
}

export function readLogs(raw: unknown): LogTail {
    const record = isRecord(raw) ? raw : {};
    const lines = strings(record['lines']);
    return { lines, total: optionalNumber(record['total']) ?? lines.length };
}

// ------------------------------------------------------------- processes --

export interface ServerProcess {
    pid: number;
    cpu: number;
    rssMb: number;
    elapsed: string;
    command: string;
    isSelf: boolean;
}

export interface ProcessReport {
    selfPid: number;
    selfRssMb: number;
    processes: ServerProcess[];
}

export function readProcesses(raw: unknown): ProcessReport {
    const record = isRecord(raw) ? raw : {};
    const list = Array.isArray(record['processes']) ? record['processes'].filter(isRecord) : [];
    return {
        selfPid: optionalNumber(record['self_pid']) ?? -1,
        selfRssMb: optionalNumber(record['self_rss_mb']) ?? 0,
        processes: list.map((entry) => ({
            pid: optionalNumber(entry['pid']) ?? -1,
            cpu: optionalNumber(entry['cpu']) ?? 0,
            rssMb: optionalNumber(entry['rss_mb']) ?? 0,
            elapsed: text(entry['elapsed']),
            command: text(entry['command']),
            isSelf: entry['is_self'] === true,
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
