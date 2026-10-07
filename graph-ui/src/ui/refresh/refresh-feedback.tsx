import { useCallback, useEffect, useRef, useState, type JSX } from 'react';
import { refreshText as text } from './strings';
import './refresh.css';

/**
 * What a Refresh button says it did (hand test 2026-10-04, A3: "Refresh
 * connections" fetched everything again and nothing on screen changed). While
 * the reading runs the button reads "Refreshing …"; then a status beside it
 * names the time and whether anything changed, or the error. Made for the
 * four Refresh buttons of Architecture and shared since the review of K42 with
 * those of ADR, the project picker and the file impact in Explore.
 */
export type RefreshFeedback =
    | { phase: 'running' }
    | { phase: 'done'; at: number; changed: boolean }
    | { phase: 'failed'; at: number; error: string };

/**
 * The reading a view shows: its key (a refresh always leads to a new one),
 * whether it has settled, and what it holds. `value` is compared as JSON
 * before and after, so a view hands something small: the route snapshot, the
 * service topology, the analysis generation.
 */
export interface RefreshReading { key: string; settled: boolean; value?: unknown; error?: string }

const signature = (value: unknown) => JSON.stringify(value) ?? '';

export function useRefreshFeedback(reading: RefreshReading, now: () => number = Date.now): { feedback?: RefreshFeedback; begin: () => void } {
    const [feedback, setFeedback] = useState<RefreshFeedback>();
    const started = useRef<{ key: string; before: string } | undefined>(undefined);
    const described = useRef(reading.key);
    const latest = useRef(reading);
    latest.current = reading;
    const { key, settled, value, error } = reading;
    useEffect(() => {
        const start = started.current;
        if (start) {
            if (key === start.key || !settled) return;
            started.current = undefined;
            described.current = key;
            setFeedback(error !== undefined ? { phase: 'failed', at: now(), error } : { phase: 'done', at: now(), changed: signature(value) !== start.before });
            return;
        }
        // Another reading than the refreshed one (another start, a reindex): the status no longer speaks about what is shown.
        if (key !== described.current) { described.current = key; setFeedback(undefined); }
    }, [key, settled, value, error, now]);
    const begin = useCallback(() => {
        const current = latest.current;
        started.current = { key: current.key, before: signature(current.value) };
        setFeedback({ phase: 'running' });
    }, []);
    return { feedback, begin };
}

const two = (value: number) => String(value).padStart(2, '0');
/** The wall clock as the status names it: 13:45:12. */
export function clockTime(at: number): string {
    const date = new Date(at);
    return `${two(date.getHours())}:${two(date.getMinutes())}:${two(date.getSeconds())}`;
}

export function refreshStatus(feedback: RefreshFeedback | undefined, done: (time: string) => string): string {
    if (!feedback || feedback.phase === 'running') return '';
    const time = clockTime(feedback.at);
    return feedback.phase === 'failed' ? text.failed(time, feedback.error) : feedback.changed ? done(time) : text.unchanged(time);
}

/**
 * The button and its status. A running refresh keeps the button in the focus
 * order (`aria-disabled`, not `disabled`), so a keyboard reader stays where
 * they were; a second press while it runs does nothing.
 */
export function RefreshControl({ labels, feedback, onRefresh, className }: {
    labels: { idle: string; busy: string; done: (time: string) => string };
    feedback?: RefreshFeedback; onRefresh: () => void; className?: string;
}): JSX.Element {
    const running = feedback?.phase === 'running';
    return <span className="atlas-refresh" data-phase={feedback?.phase}>
        <button type="button" className={className} aria-disabled={running || undefined} aria-busy={running || undefined}
            onClick={() => { if (!running) onRefresh(); }}>{running ? labels.busy : labels.idle}</button>
        <span className="atlas-refresh-status" role="status" data-phase={feedback?.phase}>{refreshStatus(feedback, labels.done)}</span>
    </span>;
}
