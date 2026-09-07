/**
 * Where a caught error is announced before it is shown.
 *
 * The clients of this window turn every failed request into a typed error
 * (RpcError in rpc-transport.ts, AtlasApiError in app/atlas-api.ts), and the
 * panels catch those and show a sentence. That is right for the reader in
 * front of the screen and useless for everyone else: a caught error never
 * reaches the console, so it never reached the server's log file either
 * (app/ui-log.ts). This seam is the one place the throwing clients announce
 * an error before throwing it; the log layer listens, and nothing else has
 * to know that it does.
 *
 * A report is a fact about a request, never a decision: a listener may not
 * throw, and a report with no listener is dropped without a trace, which is
 * the state of every test that did not install the log layer.
 */

export type ReportSource = 'rpc' | 'api' | 'reader' | 'ui';
export type ReportLevel = 'info' | 'warn' | 'error';

export interface ErrorReport {
    source: ReportSource;
    level: ReportLevel;
    message: string;
    /** The response body or the argument that explains the message, cut short. */
    detail?: string;
    stack?: string;
}

type Listener = (report: ErrorReport) => void;

const listeners = new Set<Listener>();

/** Listen to reports; the returned function stops listening. */
export function observeErrors(listener: Listener): () => void {
    listeners.add(listener);
    return () => {
        listeners.delete(listener);
    };
}

/** True while someone listens. The clients skip building a report otherwise. */
export function hasErrorObservers(): boolean {
    return listeners.size > 0;
}

/** Announce a report to every listener. Never throws. */
export function reportError(report: ErrorReport): void {
    for (const listener of listeners) {
        try {
            listener(report);
        } catch {
            /* A listener that fails must not turn one error into two. */
        }
    }
}

/** The stack of an unknown thrown value, when it has one. */
export function stackOf(error: unknown): string | undefined {
    return error instanceof Error && typeof error.stack === 'string' && error.stack.length > 0
        ? error.stack
        : undefined;
}
