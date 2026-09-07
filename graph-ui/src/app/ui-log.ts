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
 * appends it to <cache_dir>/logs/ui.log on the server (src/ui/http_server.c).
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
 *
 * Time and timers are injectable so a test runs in one tick.
 */

export type UiLogLevel = 'debug' | 'log' | 'info' | 'warn' | 'error';

export interface UiLogEntry {
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

export interface UiLogOptions {
    page: string;
    session: string;
    transport: UiLogTransport;
    now?: () => Date;
    schedule?: (fn: () => void, ms: number) => unknown;
    cancel?: (handle: unknown) => void;
    flushMs?: number;
    batchMax?: number;
    bufferMax?: number;
    backoffMs?: readonly number[];
}

/** Cut a text field at the shared cap and say so. */
export function capField(text: string, max = UI_LOG_FIELD_MAX): string {
    return text.length > max ? `${text.slice(0, max)} [cut at ${max}]` : text;
}

export class UiLogBuffer {
    private readonly queue: UiLogEntry[] = [];
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
        return this.options.page;
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
        this.push(entry);
        this.arm(this.flushMs);
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
     * `final` the page is going away: the transport is told so, and nothing
     * is rescheduled.
     */
    async flush(final = false): Promise<void> {
        if (this.timer !== undefined) {
            this.cancel(this.timer);
            this.timer = undefined;
        }
        if (this.inFlight && !final) {
            return;
        }
        if (this.queue.length === 0) {
            return;
        }
        const batch = this.queue.splice(0, this.batchMax);
        if (this.droppedSinceFlush > 0) {
            const lost = this.droppedSinceFlush;
            this.droppedSinceFlush = 0;
            batch.unshift({
                ts: this.now().toISOString(),
                seq: ++this.seq,
                level: 'warn',
                source: 'ui-log',
                message: `${lost} entries were dropped before this batch: the page keeps ${this.bufferMax} while the server does not answer`,
            });
        }
        const payload: UiLogPayload = {
            page: this.options.page,
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
        this.queue.push(entry);
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
