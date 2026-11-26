/**
 * Back and Forward for the Architecture workspace (hand test K27), wired the
 * way Galaxy wires K2: the current place is state, meaningful exploration is
 * a step in the shared bounded model, and a restored entry pushes nothing.
 *
 * Three refinements Architecture needs and Galaxy does not:
 *
 *  - Context controls (subtab, perspective and typed Routes filter) replace
 *    the current step immediately and preserve its forward branch.
 *  - A change the page makes itself (the suggested Behavior start, a reset
 *    after reindexing) also replaces the current step, so Back never lands on
 *    a state the page would immediately fill in again.
 *  - Alt+Left and Alt+Right act only while this workspace is the active one,
 *    never while typing and never while another surface takes the keys.
 *
 * Shared rules and limits: src/graph/navigation-history.ts.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import {
    emptyNavigationHistory, moveNavigation, peekNavigation, pushNavigation, refreshNavigation, replaceNavigation, type NavigationHistory,
} from '../graph/navigation-history';
import { isTypingTarget } from '../app/keyboard';
import { architectureHistoryOptions as options, type ArchitectureHistoryEntry } from './architecture-history';

type Update = (current: ArchitectureHistoryEntry) => ArchitectureHistoryEntry;

export interface ArchitectureHistory {
    place: ArchitectureHistoryEntry;
    /** The Routes filter saved with the current place, including the latest typing. */
    filter: string;
    history: NavigationHistory<ArchitectureHistoryEntry>;
    back?: ArchitectureHistoryEntry;
    forward?: ArchitectureHistoryEntry;
    /** A navigation; `automatic` replaces the current stop for context controls and page-made changes. Stable across renders. */
    navigate: (update: Update, automatic?: boolean) => void;
    type: (filter: string) => void;
    go: (step: -1 | 1) => void;
    /** A recent place is a new navigation: it is pushed and drops the forward branch. */
    jump: (entry: ArchitectureHistoryEntry) => void;
}

export interface ArchitectureHistoryKeys {
    active: boolean;
    escapeTaken: boolean;
    /** Called before Back, Forward or a recent jump replaces the place. */
    onRestore?: (from: ArchitectureHistoryEntry, to: ArchitectureHistoryEntry) => void;
}

export function useArchitectureHistory(initial: () => ArchitectureHistoryEntry, { active, escapeTaken, onRestore }: ArchitectureHistoryKeys): ArchitectureHistory {
    const [place, setPlace] = useState(initial);
    const [history, setHistory] = useState(() => emptyNavigationHistory<ArchitectureHistoryEntry>());
    const latest = useRef(place);
    latest.current = place;
    const restoringKey = useRef<string | undefined>(undefined);
    /** An automatic change: the key it was computed from and the key it leads to. */
    const automaticStep = useRef<{ base: string; next: string } | undefined>(undefined);
    const restore = useRef(onRestore);
    restore.current = onRestore;
    const key = options.key(place);

    useEffect(() => {
        const entry = latest.current;
        const restoring = restoringKey.current;
        restoringKey.current = undefined;
        if (restoring === key) return;
        const automatic = automaticStep.current;
        if (automatic?.next === key) {
            automaticStep.current = undefined;
            setHistory((current) => replaceNavigation(current, entry, options));
            return;
        }
        // Computed from this very state and not rendered yet: it still belongs to the next step.
        if (automatic && automatic.base !== key) automaticStep.current = undefined;
        setHistory((current) => pushNavigation(current, entry, options));
    }, [key]);
    // The current step keeps the newest details of its place (a name known only later); after the push above, so a new key is never a refresh.
    useEffect(() => { setHistory((current) => refreshNavigation(current, place, options)); }, [place]);

    const navigate = useCallback((update: Update, automatic = false) => {
        setPlace((current) => {
            const result = update(current);
            if (automatic) {
                const before = options.key(current), after = options.key(result);
                if (after !== before) automaticStep.current = { base: before, next: after };
            }
            return result;
        });
    }, []);

    const type = useCallback((filter: string) => { navigate((current) => ({ ...current, filter }), true); }, [navigate]);

    const apply = useCallback((entry: ArchitectureHistoryEntry) => {
        restore.current?.(latest.current, entry);
        automaticStep.current = undefined;
        setPlace(entry);
    }, []);
    const go = useCallback((step: -1 | 1) => {
        const entry = peekNavigation(history, step);
        if (entry === undefined) return;
        restoringKey.current = options.key(entry);
        setHistory(moveNavigation(history, step, options));
        apply(entry);
    }, [history, apply]);
    const jump = useCallback((entry: ArchitectureHistoryEntry) => { apply(entry); }, [apply]);

    useEffect(() => {
        if (!active || escapeTaken) return;
        const onKey = (event: globalThis.KeyboardEvent): void => {
            if (event.defaultPrevented || !event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
            if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
            if (isTypingTarget(event.target instanceof Element ? event.target : null)) return;
            const step = event.key === 'ArrowLeft' ? -1 : 1;
            if (peekNavigation(history, step) === undefined) return;
            event.preventDefault();
            go(step);
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [active, escapeTaken, history, go]);

    return { place, filter: place.filter, history, back: peekNavigation(history, -1), forward: peekNavigation(history, 1), navigate, type, go, jump };
}
