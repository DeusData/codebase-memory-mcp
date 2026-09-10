/**
 * Wiring the frontend log into a page: console, window, promises, clients.
 *
 * Four feeds, and each one is a place where an error would otherwise stop:
 *
 *  1. **The console.** Every console method still prints, exactly as before;
 *     the wrapper calls the original first and records second. Arguments
 *     are rendered the way a reader would want them in a file: strings as
 *     they are, an Error as name, message and stack, anything else as JSON
 *     with cycles and length held in check.
 *  2. **Uncaught errors** (`window.onerror` as the `error` event), with file,
 *     line and column when the browser has them.
 *  3. **Unhandled promise rejections.**
 *  4. **Caught request failures**, announced by the /rpc and /api clients
 *     through the observer seam (provider/error-observer.ts). These are the
 *     ones the panels show as a sentence and the console never sees.
 *
 * The buffer is flushed with `final` on pagehide and when the tab is hidden,
 * so the last lines of a session reach the file (ui-log-transport.ts uses a
 * beacon for that). A re-entrancy guard makes sure nothing recorded from
 * inside a record call comes back around.
 *
 * The first entry of a session says which page, which build and which
 * browser, because a log file that holds several sessions needs a line
 * where each one starts.
 */

import { observeErrors } from '../provider/error-observer';
import { UiLogBuffer } from './ui-log';
import type { UiLogLevel, UiLogTransport } from './ui-log';
import { httpUiLogTransport } from './ui-log-transport';
import { ATLAS_BUILD_SUFFIX, ATLAS_VERSION } from './build-info';

export interface InstallUiLogOptions {
    target?: Window;
    console?: Console;
    transport?: UiLogTransport;
    buffer?: UiLogBuffer;
    session?: string;
    getProject?: () => string;
}

let activeProject: string | undefined;

/** The application also resolves projects without changing the page URL. */
export function setUiLogProject(project: string): void {
    activeProject = project;
}

export interface UiLogHandle {
    buffer: UiLogBuffer;
    session: string;
    uninstall(): void;
}

const LEVELS: readonly UiLogLevel[] = ['debug', 'log', 'info', 'warn', 'error'];

/** The cap on a rendered console argument; the field cap applies after. */
const ARG_MAX = 2000;

/** JSON for a console argument that is not a string or an Error. */
export function safeStringify(value: unknown, max = ARG_MAX): string {
    if (typeof value === 'string') {
        return value;
    }
    if (typeof value === 'bigint') {
        return `${value.toString()}n`;
    }
    if (typeof value === 'function') {
        return `[function ${value.name || 'anonymous'}]`;
    }
    if (typeof value === 'symbol') {
        return value.toString();
    }
    if (value === undefined) {
        return 'undefined';
    }
    const seen = new WeakSet<object>();
    let text: string;
    try {
        text = JSON.stringify(value, (_key, entry: unknown) => {
            if (typeof entry === 'bigint') {
                return `${entry.toString()}n`;
            }
            if (typeof entry === 'object' && entry !== null) {
                if (seen.has(entry)) {
                    return '[circular]';
                }
                seen.add(entry);
            }
            return entry;
        }) ?? String(value);
    } catch {
        text = String(value);
    }
    return text.length > max ? `${text.slice(0, max)} [cut at ${max}]` : text;
}

/** One line for a console call, plus the first stack among its arguments. */
export function describeArgs(args: readonly unknown[]): { message: string; stack?: string } {
    const parts: string[] = [];
    let stack: string | undefined;
    for (const arg of args) {
        if (arg instanceof Error) {
            parts.push(`${arg.name}: ${arg.message}`);
            if (stack === undefined && typeof arg.stack === 'string' && arg.stack.length > 0) {
                stack = arg.stack;
            }
        } else {
            parts.push(safeStringify(arg));
        }
    }
    const message = parts.join(' ');
    return stack === undefined ? { message } : { message, stack };
}

/** What a rejected promise rejected with, as message and stack. */
export function describeReason(reason: unknown): { message: string; stack?: string } {
    if (reason instanceof Error) {
        const stack = typeof reason.stack === 'string' && reason.stack.length > 0 ? reason.stack : undefined;
        const message = `${reason.name}: ${reason.message}`;
        return stack === undefined ? { message } : { message, stack };
    }
    return { message: safeStringify(reason) };
}

function newSessionId(): string {
    const cryptoApi = (globalThis as { crypto?: Crypto }).crypto;
    if (cryptoApi !== undefined && typeof cryptoApi.randomUUID === 'function') {
        return cryptoApi.randomUUID().slice(0, 8);
    }
    return Math.random().toString(16).slice(2, 10);
}

export function installUiLog(options: InstallUiLogOptions = {}): UiLogHandle {
    const target = options.target ?? window;
    const con = options.console ?? console;
    const session = options.session ?? newSessionId();
    const page = `${target.location.pathname}${target.location.search}`;
    const getProject = options.getProject ?? (() => activeProject
        ?? new URLSearchParams(target.location.search).get('project') ?? '');
    const buffer = options.buffer
        ?? new UiLogBuffer({ page, session, getProject, transport: options.transport ?? httpUiLogTransport() });

    let recording = false;
    const guarded = (fn: () => void): void => {
        if (recording) {
            return;
        }
        recording = true;
        try {
            fn();
        } catch {
            /* Recording must never break the call that was being recorded. */
        } finally {
            recording = false;
        }
    };

    const originals = new Map<UiLogLevel, (...args: unknown[]) => void>();
    for (const level of LEVELS) {
        const original = con[level] as (...args: unknown[]) => void;
        originals.set(level, original);
        con[level] = (...args: unknown[]): void => {
            original.apply(con, args);
            guarded(() => {
                const described = describeArgs(args);
                buffer.record(level, 'console', described.message, {
                    project: getProject(), stack: described.stack,
                });
            });
        };
    }

    const onError = (event: ErrorEvent): void => {
        guarded(() => {
            const described = describeReason(event.error);
            buffer.record('error', 'window', event.message.length > 0 ? event.message : described.message, {
                project: getProject(),
                stack: described.stack,
                url: event.filename,
                line: event.lineno,
                col: event.colno,
            });
        });
    };
    const onRejection = (event: PromiseRejectionEvent): void => {
        guarded(() => {
            const described = describeReason(event.reason);
            buffer.record('error', 'promise', described.message, {
                project: getProject(), stack: described.stack,
            });
        });
    };
    const onHide = (): void => {
        void buffer.flush(true);
    };
    const onVisibility = (): void => {
        if (target.document.visibilityState === 'hidden') {
            void buffer.flush(true);
        }
    };
    const stopObserving = observeErrors((report) => {
        guarded(() => {
            buffer.record(report.level, report.source, report.message, {
                project: report.project ?? getProject(),
                detail: report.detail,
                stack: report.stack,
            });
        });
    });

    target.addEventListener('error', onError);
    target.addEventListener('unhandledrejection', onRejection);
    target.addEventListener('pagehide', onHide);
    target.document.addEventListener('visibilitychange', onVisibility);

    const userAgent = (target.navigator as Navigator | undefined)?.userAgent ?? '';
    buffer.record('info', 'ui-log', `session ${session} started on ${page}`, {
        project: getProject(),
        detail: `build ${ATLAS_VERSION}${ATLAS_BUILD_SUFFIX.length > 0 ? `-${ATLAS_BUILD_SUFFIX}` : ''}; ${userAgent}`,
        url: target.location.href,
    });

    return {
        buffer,
        session,
        uninstall(): void {
            for (const [level, original] of originals) {
                con[level] = original;
            }
            target.removeEventListener('error', onError);
            target.removeEventListener('unhandledrejection', onRejection);
            target.removeEventListener('pagehide', onHide);
            target.document.removeEventListener('visibilitychange', onVisibility);
            stopObserving();
        },
    };
}
