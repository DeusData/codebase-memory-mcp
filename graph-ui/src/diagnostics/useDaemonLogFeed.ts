import { useCallback, useRef } from 'react';
import type { AtlasApi } from '../app/atlas-api';
import type { LogTail } from '../projects/projects-model';
import { useSystemPoll } from '../system/useSystemPoll';

/** Mount once; server-side filtering precedes limits. Each scope owns its results. */
export function useDaemonLogFeed(api: Pick<AtlasApi, 'logs'>, active = true, pollMs = 3000, project?: string) {
    const key = project === undefined ? 'daemon' : `project:${project}`;
    const currentKey = useRef(key);
    currentKey.current = key;
    const last = useRef<{ key: string; data: LogTail; updatedAt: number }>(undefined);
    const read = useCallback(async () => {
        try {
            const data = await api.logs(200, 'warn', project);
            if (project !== undefined && (data.scope !== 'project' || data.project !== project)) {
                throw new Error('Project log attribution is unavailable. Open daemon history for older, unattributed events.');
            }
            const result = { key, data, updatedAt: Date.now() };
            if (currentKey.current === key) last.current = result;
            return { ...result, error: null };
        } catch (cause) {
            const retained = last.current?.key === key ? last.current : undefined;
            return { key, data: retained?.data ?? null, updatedAt: retained?.updatedAt ?? null,
                error: cause instanceof Error ? cause.message : String(cause) };
        }
    }, [api, key, project]);
    const reading = useSystemPoll(read, active, false, pollMs);
    const owned = reading.data?.key === key ? reading.data : undefined;
    // A previous scope can remain in the generic poller during the next request,
    // including an unsuccessful request. Never relabel that data or error.
    return { ...reading, data: owned?.data ?? null, error: owned?.error ?? null,
        loading: reading.loading || !owned, updatedAt: owned?.updatedAt ?? null };
}
