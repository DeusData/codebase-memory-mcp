/**
 * The frontend's log, on its way to the server.
 *
 * The browser console is where this window has always written, and it stays
 * that way: the console is the reader's view. But the console lives and dies
 * with the tab. Nothing there reaches the process that serves the page, so a
 * reader who saw a panel fail had nothing to hand over, and an agent looking
 * into the failure had nothing to read. This buffer is the second copy: every
 * console call, every uncaught error, every rejected promise and every failed
 * /rpc or /api request is queued here and posted to POST /api/ui-log, which
 * persists it in the local SQLite journal and a bounded compatibility JSONL
 * export on the same daemon (src/ui/http_server.c).
 * The file is what a bug report attaches and what `tail -f` reads.
 *
 * The rules of the buffer, and why each is there:
 *
 *  - **Batched, not chatty.** Entries wait up to UI_LOG_FLUSH_MS and go out
 *    in groups of at most UI_LOG_BATCH_MAX. A page that logs once per frame
 *    must not turn into a request per frame.
 *  - **Bounded.** UI_LOG_BUFFER_MAX entries wait while the server does not
 *    answer; the oldest are dropped first, and the next batch that gets
 *    through starts with a line that says how many were lost. A log that
 *    silently forgets is worse than one that says it forgot.
 *  - **Backs off.** A failed post is retried on a growing delay
 *    (UI_LOG_BACKOFF_MS). An unreachable server is a fact the reader sees in
 *    the header chip; hammering it from here would add nothing.
 *  - **Never loud.** Nothing in this file writes to the console. The console
 *    wrapper (ui-log-install.ts) feeds this buffer, so a line written from
 *    here would come straight back.
 *  - **Capped fields.** Every text field is cut at UI_LOG_FIELD_MAX
 *    characters, the same cap the server applies; a cut is marked so nobody
 *    reads a truncated stack as a complete one.
 *  - **Once per session, then counted.** An entry identical to an earlier
 *    one of this session (level, source, project, message, detail, stack and
 *    place) is counted, not queued. A deprecation warning that a library
 *    prints on every scene mount (THREE.Clock from @react-three/fiber) would
 *    otherwise push the real messages out of System › Logs. The count goes
 *    out as one more entry with the same fields at 10, 100, 1000 repeats and
 *    when the page is left (`final`), so the journal holds a handful of
 *    lines, not hundreds. The same message from another place or with
 *    another response body is another entry and is recorded in full.
 *  - **One page per post.** The server files every entry of a post under
 *    the post's page. A project switch stays in the page and only changes
 *    the address (app/project-windows.tsx), so each entry keeps the page it
 *    was recorded on, and a batch never spans two of them.
 *
 * Time and timers are injectable so a test runs in one tick.
 */

export type UiLogLevel = 'debug' | 'log' | 'info' | 'warn' | 'error';

export interface UiLogEntry {
    /** Project at recording time; an empty string explicitly means daemon-wide. */
    project?: string;
    /** When the entry was recorded on the page, ISO 8601. */
    ts: string;
    /** Position in this page's sequence, from 1. Gaps mean dropped entries. */
    seq: number;
    level: UiLogLevel;
    /** Who recorded it: console, window, promise, rpc, api, reader, ui-log. */
    source: string;
    message: string;
    detail?: string;
    stack?: string;
    url?: string;
    line?: number;
    col?: number;
}

export interface UiLogPayload {
    page: string;
    session: string;
    entries: UiLogEntry[];
}

export interface UiLogTransport {
    /**
     * Deliver one batch. `final` means the page is going away and the answer
     * will not be waited for. Resolves true when the server accepted it.
     */
    send(payload: UiLogPayload, final: boolean): Promise<boolean>;
}

export interface UiLogExtra {
    project?: string;
    detail?: string;
    stack?: string;
    url?: string;
    line?: number;
    col?: number;
}

export interface UiLogStats {
    queued: number;
    sent: number;
    dropped: number;
    failedPosts: number;
}

/** The route the transport posts to; the server tails the same path on GET. */
export const UI_LOG_ROUTE = '/api/ui-log';
/** The cap per text field, the server's as well. */
export const UI_LOG_FIELD_MAX = 4096;
export const UI_LOG_BATCH_MAX = 25;
export const UI_LOG_FLUSH_MS = 1500;
export const UI_LOG_BUFFER_MAX = 200;
export const UI_LOG_BACKOFF_MS: readonly number[] = [2000, 5000, 15000, 60000];
/** Distinct entries counted per session; beyond this, new texts are recorded one by one again. */
export const UI_LOG_REPEAT_KEYS_MAX = 256;

export interface UiLogOptions {
    page: string;
    /** The page as it is now, sampled when recording like the project; `page` without it. */
    getPage?: () => string;
    session: string;
    transport: UiLogTransport;
    /** Sampled when recording, never when sending a delayed or retried batch. */
    getProject?: () => string;
    now?: () => Date;
    schedule?: (fn: () => void, ms: number) => unknown;
    cancel?: (handle: unknown) => void;
    flushMs?: number;
    batchMax?: number;
    bufferMax?: number;
    backoffMs?: readonly number[];
}

/**
 * The detail of a repeat count. The server keeps only the known fields of an
 * entry, so the count travels in `detail`, in words a reader understands and
 * in a form System › Logs reads back (repeatCount). The detail of the counted
 * entry follows on the next line, so the count stays beside its own entry.
 */
export function repeatDetail(count: number, detail?: string): string {
    const words = `${count} identical entries in this session; only the first is recorded in full`;
    return detail ? capField(`${words}\n${detail}`) : words;
}

/** The count of a repeat entry, undefined for an ordinary one. */
export function repeatCount(detail: string | undefined): number | undefined {
    const match = /^(\d+) identical entries in this session\b/.exec(detail ?? '');
    return match ? Number(match[1]) : undefined;
}

/** The detail of the entry a repeat count stands for; an ordinary detail as it is. */
export function countedDetail(detail: string | undefined): string | undefined {
    if (repeatCount(detail) === undefined) return detail;
    const newline = detail!.indexOf('\n');
    return newline < 0 ? undefined : detail!.slice(newline + 1);
}

/** Cut a text field at the shared cap and say so. */
export function capField(text: string, max = UI_LOG_FIELD_MAX): string {
    return text.length > max ? `${text.slice(0, max)} [cut at ${max}]` : text;
}

interface Repeat {
    entry: UiLogEntry;
    count: number;
    reported: number;
}

const powerOfTen = (count: number): boolean => count >= 10 && /^10*$/.test(String(count));

export class UiLogBuffer {
    private readonly queue: UiLogEntry[] = [];
    /** The page each queued entry was recorded on. */
    private readonly pages = new WeakMap<UiLogEntry, string>();
    private readonly repeats = new Map<string, Repeat>();
    private seq = 0;
    private sent = 0;
    private dropped = 0;
    private droppedSinceFlush = 0;
    private failedPosts = 0;
    private timer: unknown = undefined;
    private inFlight = false;
    private backoffStep = 0;

    private readonly now: () => Date;
    private readonly schedule: (fn: () => void, ms: number) => unknown;
    private readonly cancel: (handle: unknown) => void;
    private readonly flushMs: number;
    private readonly batchMax: number;
    private readonly bufferMax: number;
    private readonly backoffMs: readonly number[];

    constructor(private readonly options: UiLogOptions) {
        this.now = options.now ?? (() => new Date());
        this.schedule = options.schedule ?? ((fn, ms) => setTimeout(fn, ms));
        this.cancel = options.cancel ?? ((handle) => clearTimeout(handle as ReturnType<typeof setTimeout>));
        this.flushMs = options.flushMs ?? UI_LOG_FLUSH_MS;
        this.batchMax = options.batchMax ?? UI_LOG_BATCH_MAX;
        this.bufferMax = options.bufferMax ?? UI_LOG_BUFFER_MAX;
        this.backoffMs = options.backoffMs ?? UI_LOG_BACKOFF_MS;
    }

    get session(): string {
        return this.options.session;
    }

    get page(): string {
        return this.options.getPage?.() ?? this.options.page;
    }

    /** Queue one entry. Never throws and never writes to the console. */
    record(level: UiLogLevel, source: string, message: string, extra: UiLogExtra = {}): void {
        const entry: UiLogEntry = {
            ts: this.now().toISOString(),
            seq: ++this.seq,
            level,
            source: source.slice(0, 64),
            message: capField(message),
        };
        const project = extra.project ?? this.options.getProject?.();
        if (project !== undefined) {
            entry.project = project;
        }
        if (extra.detail !== undefined && extra.detail.length > 0) {
            entry.detail = capField(extra.detail);
        }
        if (extra.stack !== undefined && extra.stack.length > 0) {
            entry.stack = capField(extra.stack);
        }
        if (extra.url !== undefined && extra.url.length > 0) {
            entry.url = capField(extra.url, 1024);
        }
        if (typeof extra.line === 'number' && Number.isFinite(extra.line)) {
            entry.line = extra.line;
        }
        if (typeof extra.col === 'number' && Number.isFinite(extra.col)) {
            entry.col = extra.col;
        }
        // Only a truly identical entry is counted: the same text from another place stays its own entry.
        const key = JSON.stringify([entry.level, entry.source, entry.project ?? '', entry.message, entry.detail ?? '',
            entry.stack ?? '', entry.url ?? '', entry.line ?? null, entry.col ?? null]);
        const repeat = this.repeats.get(key);
        if (repeat !== undefined) {
            this.seq -= 1;
            repeat.count += 1;
            if (powerOfTen(repeat.count)) {
                this.report(repeat);
                this.arm(this.flushMs);
            }
            return;
        }
        if (this.repeats.size < UI_LOG_REPEAT_KEYS_MAX) {
            this.repeats.set(key, { entry, count: 1, reported: 1 });
        }
        this.push(entry);
        this.arm(this.flushMs);
    }

    /** Queue the running count of a repeated entry, with every field of its first occurrence. */
    private report(repeat: Repeat): void {
        const { ts: _ts, seq: _seq, detail, ...first } = repeat.entry;
        const entry: UiLogEntry = { ...first, ts: this.now().toISOString(), seq: ++this.seq, detail: repeatDetail(repeat.count, detail) };
        repeat.reported = repeat.count;
        this.push(entry);
    }

    stats(): UiLogStats {
        return {
            queued: this.queue.length,
            sent: this.sent,
            dropped: this.dropped,
            failedPosts: this.failedPosts,
        };
    }

    /**
     * Send what is queued, one batch now and the rest on the timer. With
     * `final` the page is going away: the transport is told so, nothing is
     * rescheduled, and a batch goes out for each page the entries were
     * recorded on.
     */
    async flush(final = false): Promise<void> {
        if (this.timer !== undefined) {
            this.cancel(this.timer);
            this.timer = undefined;
        }
        if (this.inFlight && !final) {
            return;
        }
        if (final) {
            // The page is going away: every count not yet reported goes with it.
            for (const repeat of this.repeats.values()) {
                if (repeat.count > repeat.reported) {
                    this.report(repeat);
                }
            }
        }
        if (this.queue.length === 0) {
            return;
        }
        // One page per post: the batch ends where an entry of another page starts.
        const page = this.pageOf(this.queue[0]);
        let size = 1;
        while (size < Math.min(this.batchMax, this.queue.length) && this.pageOf(this.queue[size]) === page) {
            size += 1;
        }
        const batch = this.queue.splice(0, size);
        if (this.droppedSinceFlush > 0) {
            const lost = this.droppedSinceFlush;
            this.droppedSinceFlush = 0;
            const notice: UiLogEntry = {
                // Loss can span several projects. Do not attribute the aggregate
                // to whichever project happens to be open when delivery resumes.
                project: '',
                ts: this.now().toISOString(),
                seq: ++this.seq,
                level: 'warn',
                source: 'ui-log',
                message: `${lost} entries were dropped before this batch: the page keeps ${this.bufferMax} while the server does not answer`,
            };
            this.pages.set(notice, page);
            batch.unshift(notice);
        }
        const payload: UiLogPayload = {
            page,
            session: this.options.session,
            entries: batch,
        };
        this.inFlight = true;
        let accepted = false;
        try {
            accepted = await this.options.transport.send(payload, final);
        } catch {
            accepted = false;
        } finally {
            this.inFlight = false;
        }
        if (accepted) {
            this.sent += batch.length;
            this.backoffStep = 0;
            if (this.queue.length > 0 && !final) {
                this.arm(0);
            } else if (final && this.queue.length > 0 && this.pageOf(this.queue[0]) !== page) {
                // The page is going away: the entries of the other page it was on go with it.
                await this.flush(true);
            }
            return;
        }
        this.failedPosts += 1;
        // Put the batch back in front so order is kept, then trim to the cap
        // from the oldest end, the same way record() does.
        this.queue.unshift(...batch);
        while (this.queue.length > this.bufferMax) {
            this.queue.shift();
            this.dropped += 1;
            this.droppedSinceFlush += 1;
        }
        if (!final) {
            const step = Math.min(this.backoffStep, this.backoffMs.length - 1);
            const wait = this.backoffMs[step] ?? this.flushMs;
            this.backoffStep += 1;
            this.arm(wait);
        }
    }

    private push(entry: UiLogEntry): void {
        if (this.queue.length >= this.bufferMax) {
            this.queue.shift();
            this.dropped += 1;
            this.droppedSinceFlush += 1;
        }
        this.pages.set(entry, this.page);
        this.queue.push(entry);
    }

    private pageOf(entry: UiLogEntry | undefined): string {
        return (entry === undefined ? undefined : this.pages.get(entry)) ?? this.options.page;
    }

    private arm(ms: number): void {
        if (this.timer !== undefined || this.inFlight) {
            return;
        }
        this.timer = this.schedule(() => {
            this.timer = undefined;
            void this.flush();
        }, ms);
    }
}
