import { useCallback, useSyncExternalStore } from 'react';

export const GALAXY_NODE_LIMITS = [500, 1000, 5000, 10000, 25000, 50000] as const;
export const GALAXY_EDGE_LIMITS = [1000, 5000, 20000, 50000, 200000] as const;

export interface ViewPreferences {
    galaxyNodes: number;
    galaxyEdges: number;
    coverageShadow: boolean;
    brickHeight: 'lines' | 'uniform';
    brickColor: 'language' | 'kind';
    hotspotGravity: boolean;
    fileVisibility: 'all' | 'connected' | 'unconnected';
}

export const DEFAULT_VIEW_PREFERENCES: Readonly<ViewPreferences> = Object.freeze({
    galaxyNodes: 5000, galaxyEdges: 20000, coverageShadow: true,
    brickHeight: 'lines', brickColor: 'language', hotspotGravity: true, fileVisibility: 'all',
});

type Update = Partial<ViewPreferences> | ((current: Readonly<ViewPreferences>) => Partial<ViewPreferences>);
interface Snapshot { raw: string | null; value: Readonly<ViewPreferences>; sessionOnly: boolean }
const snapshots = new Map<string, Snapshot>();
const CHANGE = 'cbm-view-preferences-change';
export const viewPreferencesKey = (project: string): string => `cbm-view-preferences-v1:${encodeURIComponent(project)}`;

function validated(value: unknown): Readonly<ViewPreferences> {
    const row = typeof value === 'object' && value !== null ? value as Record<string, unknown> : {};
    const oneOf = <T extends string | number>(candidate: unknown, choices: readonly T[], fallback: T): T =>
        choices.includes(candidate as T) ? candidate as T : fallback;
    return Object.freeze({
        galaxyNodes: oneOf(row.galaxyNodes, GALAXY_NODE_LIMITS, DEFAULT_VIEW_PREFERENCES.galaxyNodes),
        galaxyEdges: oneOf(row.galaxyEdges, GALAXY_EDGE_LIMITS, DEFAULT_VIEW_PREFERENCES.galaxyEdges),
        coverageShadow: typeof row.coverageShadow === 'boolean' ? row.coverageShadow : DEFAULT_VIEW_PREFERENCES.coverageShadow,
        brickHeight: oneOf(row.brickHeight, ['lines', 'uniform'] as const, DEFAULT_VIEW_PREFERENCES.brickHeight),
        brickColor: oneOf(row.brickColor, ['language', 'kind'] as const, DEFAULT_VIEW_PREFERENCES.brickColor),
        hotspotGravity: typeof row.hotspotGravity === 'boolean' ? row.hotspotGravity : DEFAULT_VIEW_PREFERENCES.hotspotGravity,
        fileVisibility: oneOf(row.fileVisibility, ['all', 'connected', 'unconnected'] as const, DEFAULT_VIEW_PREFERENCES.fileVisibility),
    });
}

function decode(raw: string | null): Readonly<ViewPreferences> {
    try {
        const record: unknown = JSON.parse(raw ?? 'null');
        if (record && typeof record === 'object' && 'version' in record && record.version === 1 && 'preferences' in record) {
            return validated(record.preferences);
        }
    } catch { /* Invalid storage is equivalent to no saved preference. */ }
    return DEFAULT_VIEW_PREFERENCES;
}

/** Stable snapshots prevent a settings change from remounting or resetting either scene. */
export function readViewPreferences(project: string): Readonly<ViewPreferences> {
    const previous = snapshots.get(project);
    if (previous?.sessionOnly) return previous.value;
    let raw: string | null;
    try { raw = project ? window.localStorage.getItem(viewPreferencesKey(project)) : null; }
    catch { return previous?.value ?? DEFAULT_VIEW_PREFERENCES; }
    if (previous?.raw === raw) return previous.value;
    const value = decode(raw);
    snapshots.set(project, { raw, value, sessionOnly: false });
    return value;
}

function notify(project: string): void {
    if (typeof window !== 'undefined') window.dispatchEvent(new CustomEvent(CHANGE, { detail: project }));
}

export function setViewPreferences(project: string, update: Update): void {
    const current = readViewPreferences(project);
    const value = validated({ ...current, ...(typeof update === 'function' ? update(current) : update) });
    if (Object.keys(DEFAULT_VIEW_PREFERENCES).every(key => value[key as keyof ViewPreferences] === current[key as keyof ViewPreferences])) return;
    const raw = JSON.stringify({ version: 1, preferences: value });
    let sessionOnly = !project;
    try { if (project) window.localStorage.setItem(viewPreferencesKey(project), raw); }
    catch { sessionOnly = true; }
    snapshots.set(project, { raw, value, sessionOnly });
    notify(project);
}

export function resetViewPreferences(project: string): void {
    let sessionOnly = !project;
    try { if (project) window.localStorage.removeItem(viewPreferencesKey(project)); }
    catch { sessionOnly = true; }
    snapshots.set(project, { raw: null, value: DEFAULT_VIEW_PREFERENCES, sessionOnly });
    notify(project);
}

export function useViewPreferences(project: string): {
    preferences: Readonly<ViewPreferences>; setPreferences: (update: Update) => void; resetPreferences: () => void;
} {
    const subscribe = useCallback((listener: () => void) => {
        const changed = (event: Event) => { if ((event as CustomEvent<string>).detail === project) listener(); };
        const stored = (event: StorageEvent) => {
            if (event.key !== null && event.key !== viewPreferencesKey(project)) return;
            snapshots.delete(project);
            listener();
        };
        window.addEventListener(CHANGE, changed);
        window.addEventListener('storage', stored);
        return () => { window.removeEventListener(CHANGE, changed); window.removeEventListener('storage', stored); };
    }, [project]);
    const getSnapshot = useCallback(() => readViewPreferences(project), [project]);
    const preferences = useSyncExternalStore(subscribe, getSnapshot, () => DEFAULT_VIEW_PREFERENCES);
    const setPreferences = useCallback((update: Update) => setViewPreferences(project, update), [project]);
    const resetPreferences = useCallback(() => resetViewPreferences(project), [project]);
    return { preferences, setPreferences, resetPreferences };
}
