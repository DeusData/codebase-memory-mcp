import { useCallback, useEffect, useRef, useState } from 'react';

export interface SystemReading<T> {
    data: T | null;
    updatedAt: number | null;
    error: string | null;
    loading: boolean;
}

/** Read-only polling. Each lifecycle owns its responses; hidden views never commit them. */
export function useSystemPoll<T>(read: () => Promise<T>, active: boolean, paused: boolean, intervalMs: number) {
    const empty: SystemReading<T> = { data: null, updatedAt: null, error: null, loading: false };
    const [owned, setOwned] = useState({ owner: read, reading: empty });
    const refreshRef = useRef<() => void>(() => {});

    useEffect(() => {
        const setReading = (update: SystemReading<T> | ((previous: SystemReading<T>) => SystemReading<T>)) => {
            setOwned(previous => {
                const current = previous.owner === read ? previous.reading : { data: null, updatedAt: null, error: null, loading: false };
                return { owner: read, reading: typeof update === 'function' ? update(current) : update };
            });
        };
        let disposed = false;
        let generation = 0;
        let timer: ReturnType<typeof setTimeout> | undefined;
        let inFlight = false;
        const visible = () => active && document.visibilityState !== 'hidden';
        const invalidate = () => {
            generation += 1;
            inFlight = false;
            clearTimeout(timer);
        };
        const poll = async () => {
            if (disposed || !visible() || inFlight) return;
            clearTimeout(timer);
            const ticket = generation;
            inFlight = true;
            setReading((previous) => ({ ...previous, loading: true }));
            try {
                const data = await read();
                if (!disposed && ticket === generation && visible()) {
                    setReading({ data, updatedAt: Date.now(), error: null, loading: false });
                }
            } catch (error) {
                if (!disposed && ticket === generation && visible()) {
                    setReading((previous) => ({ ...previous, error: error instanceof Error ? error.message : String(error), loading: false }));
                }
            } finally {
                if (!disposed && ticket === generation) {
                    inFlight = false;
                    if (visible() && !paused) timer = setTimeout(() => { void poll(); }, intervalMs);
                }
            }
        };
        const visibilityChanged = () => {
            invalidate();
            setReading((previous) => ({ ...previous, loading: false }));
            if (visible() && !paused) void poll();
        };
        refreshRef.current = () => { void poll(); };
        document.addEventListener('visibilitychange', visibilityChanged);
        if (visible() && !paused) void poll();
        else setReading((previous) => ({ ...previous, loading: false }));
        return () => {
            disposed = true;
            invalidate();
            refreshRef.current = () => {};
            document.removeEventListener('visibilitychange', visibilityChanged);
        };
    }, [read, active, paused, intervalMs]);

    return { ...(owned.owner === read ? owned.reading : empty), refresh: useCallback(() => refreshRef.current(), []) };
}
