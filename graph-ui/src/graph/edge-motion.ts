import { useSyncExternalStore } from 'react';

const KEY = 'cbm-edge-motion-v1';
const CHANGE = 'cbm-edge-motion-change';
let fallback = true;
let sessionPreference: boolean | undefined;
export function edgeMotionPreference(): boolean {
    if (sessionPreference !== undefined) return sessionPreference;
    try { const value = window.localStorage.getItem(KEY); return value === null ? fallback : value !== 'off'; } catch { return fallback; }
}
export function setEdgeMotionPreference(enabled: boolean): void {
    fallback = enabled;
    try { window.localStorage.setItem(KEY, enabled ? 'on' : 'off'); sessionPreference = undefined; } catch { sessionPreference = enabled; }
    window.dispatchEvent(new Event(CHANGE));
}
const reducedMotion = () => typeof window.matchMedia === 'function' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
const subscribers = new Set<() => void>();
let detach: (() => void) | undefined;
function subscribe(notify: () => void): () => void {
    if (!subscribers.size) {
        const broadcast = () => subscribers.forEach(listener => listener());
        const query = typeof window.matchMedia === 'function' ? window.matchMedia('(prefers-reduced-motion: reduce)') : undefined;
        window.addEventListener(CHANGE, broadcast); window.addEventListener('storage', broadcast);
        document.addEventListener('visibilitychange', broadcast); query?.addEventListener('change', broadcast);
        detach = () => {
            window.removeEventListener(CHANGE, broadcast); window.removeEventListener('storage', broadcast);
            document.removeEventListener('visibilitychange', broadcast); query?.removeEventListener('change', broadcast);
        };
    }
    subscribers.add(notify);
    return () => { subscribers.delete(notify); if (!subscribers.size) { detach?.(); detach = undefined; } };
}
const motionSnapshot = () => edgeMotionPreference() && !reducedMotion() && !document.hidden;
export function useEdgeMotion(active = true): boolean {
    return useSyncExternalStore(subscribe, motionSnapshot, () => false) && active;
}
export function useEdgeMotionPreference(): { enabled: boolean; reduced: boolean; setEnabled: typeof setEdgeMotionPreference } {
    const enabled = useSyncExternalStore(subscribe, edgeMotionPreference, () => false);
    const reduced = useSyncExternalStore(subscribe, reducedMotion, () => true);
    return { enabled, reduced, setEnabled: setEdgeMotionPreference };
}
