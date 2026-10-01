import { useEffect, useRef, useState } from 'react';
import type { OrganicClusterLayout, OrganicClusterOptions } from './organic-clusters';
import type { OrganicLayoutRequest, OrganicLayoutResponse } from './organic-layout.worker';
import type { GraphData } from './types';

interface Reading {
    data: GraphData;
    roots: OrganicClusterOptions['rootIds'];
    previous: OrganicClusterOptions['previous'];
    result?: OrganicClusterLayout;
    error?: string;
}

export interface OrganicLayoutReading { result?: OrganicClusterLayout; loading: boolean; error?: string }
const EMPTY_OPTIONS: OrganicClusterOptions = {};

/** Off-thread layout leaves the last painted frame responsive while work runs.
 * Input references must be stable; a new source never inherits an old result. */
export function useOrganicLayout(data: GraphData | undefined, options: OrganicClusterOptions = EMPTY_OPTIONS, enabled = true): OrganicLayoutReading {
    const worker = useRef<Worker | undefined>(undefined);
    const sequence = useRef(0);
    const [reading, setReading] = useState<Reading>();
    const roots = options.rootIds, previous = options.previous;

    useEffect(() => () => {
        sequence.current += 1;
        worker.current?.terminate(); worker.current = undefined;
    }, []);

    useEffect(() => {
        const requestId = ++sequence.current;
        if (!enabled || !data) return;
        let active = true;
        let timer: number | undefined;
        const accept = (response: OrganicLayoutResponse) => {
            if (!active || sequence.current !== requestId || response.sequence !== requestId) return;
            setReading({ data, roots, previous, ...('error' in response ? { error: response.error } : { result: response.result }) });
        };
        const fail = (error: unknown) => accept({ sequence: requestId, error: error instanceof Error ? error.message : String(error) });
        const request: OrganicLayoutRequest = { sequence: requestId, data, options: { rootIds: roots, previous } };

        if (typeof Worker === 'undefined') {
            // jsdom and environments without Worker support still yield before
            // computing. Browser worker failures never silently take this path.
            timer = window.setTimeout(() => {
                void import('./organic-clusters').then(({ layoutOrganicClusters }) => {
                    if (!active || sequence.current !== requestId) return;
                    accept({ sequence: requestId, result: layoutOrganicClusters(data, request.options) });
                }).catch(fail);
            }, 0);
        } else {
            try {
                worker.current ??= new Worker(new URL('./organic-layout.worker.ts', import.meta.url), { type: 'module' });
                const instance = worker.current;
                instance.onmessage = (event: MessageEvent<OrganicLayoutResponse>) => accept(event.data);
                const stopWithError = (message: string) => {
                    instance.terminate();
                    if (worker.current === instance) worker.current = undefined;
                    fail(message);
                };
                instance.onerror = event => {
                    event.preventDefault();
                    stopWithError(event.message || 'The graph layout worker failed.');
                };
                instance.onmessageerror = () => stopWithError('Could not read the graph layout worker response.');
                instance.postMessage(request);
            } catch (error: unknown) { fail(error); }
        }
        return () => {
            active = false;
            if (timer !== undefined) window.clearTimeout(timer);
        };
    }, [data, roots, previous, enabled]);

    const current = enabled && data && reading?.data === data && reading.roots === roots && reading.previous === previous ? reading : undefined;
    return { result: current?.result, error: current?.error, loading: Boolean(enabled && data && !current) };
}
