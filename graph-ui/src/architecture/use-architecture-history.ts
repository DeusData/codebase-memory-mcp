/**
 * Back and Forward for the Architecture workspace (hand test K27), wired the
 * way Galaxy wires K2: the current place is state, every change of its key is
 * a step in the shared bounded model, and a restored entry pushes nothing.
 *
 * Three refinements Architecture needs and Galaxy does not:
 *
 *  - Typing in the Routes filter becomes one step once it pauses
 *    (`FILTER_STEP_MS`), not one step per key. A navigation, Back, Forward or
 *    a recent jump while typing first records what the field showed, so no
 *    typed text is lost.
 *  - A change the page makes itself (the suggested Behavior start, a reset
 *    after reindexing) replaces the current step instead of adding one, so Back
 *    never lands on a state the page would immediately fill in again.
 *  - Alt+Left and Alt+Right act only while this workspace is the active one,
 *    never while typing and never while another surface takes the keys.
 *
 * Rules and limits: src/graph/navigation-history.ts and
 * docs/development/pr-2068-galaxy-history.md.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import {
    emptyNavigationHistory, moveNavigation, peekNavigation, pushNavigation, refreshNavigation, replaceNavigation, type NavigationHistory,
} from '../graph/navigation-history';
import { isTypingTarget } from '../app/keyboard';
import { architectureHistoryOptions as options, type ArchitectureHistoryEntry } from './architecture-history';

/** Typing in the Routes filter becomes a step once it pauses this long. */
export const FILTER_STEP_MS = 600;

type Update = (current: ArchitectureHistoryEntry) => ArchitectureHistoryEntry;

export interface ArchitectureHistory {
    place: ArchitectureHistoryEntry;
    /** What the filter field shows, including typing that is not a step yet. */
    filter: string;
    history: NavigationHistory<ArchitectureHistoryEntry>;
    back?: ArchitectureHistoryEntry;
    forward?: ArchitectureHistoryEntry;
    /** A navigation; `automatic` marks a change the page made itself. Stable across renders. */
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
    const [draft, setDraft] = useState<string>();
    const [history, setHistory] = useState(() => emptyNavigationHistory<ArchitectureHistoryEntry>());
    const latest = useRef(place);
    latest.current = place;
    const typed = useRef<string | undefined>(undefined);
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

    const clearDraft = useCallback(() => { typed.current = undefined; setDraft(undefined); }, []);

    const navigate = useCallback((update: Update, automatic = false) => {
        const text = automatic ? undefined : typed.current;
        if (text !== undefined) {
            clearDraft();
            // What the field showed is a step of its own, before the navigation that leaves it.
            setHistory((current) => pushNavigation(current, { ...latest.current, filter: text }, options));
        }
        setPlace((current) => {
            const result = update(text === undefined ? current : { ...current, filter: text });
            if (automatic) {
                const before = options.key(current), after = options.key(result);
                if (after !== before) automaticStep.current = { base: before, next: after };
            }
            return result;
        });
    }, [clearDraft]);

    const type = useCallback((value: string) => { typed.current = value; setDraft(value); }, []);
    useEffect(() => {
        if (draft === undefined) return;
        const timer = setTimeout(() => {
            clearDraft();
            setPlace((current) => ({ ...current, filter: draft }));
        }, FILTER_STEP_MS);
        return () => clearTimeout(timer);
    }, [draft, clearDraft]);

    const apply = useCallback((entry: ArchitectureHistoryEntry) => {
        restore.current?.(latest.current, entry);
        clearDraft();
        automaticStep.current = undefined;
        setPlace(entry);
    }, [clearDraft]);
    /** Typing that has not paused yet, as the step it is about to become; undefined while nothing is typed. */
    const typedStep = useCallback((): ArchitectureHistoryEntry | undefined =>
        (typed.current === undefined ? undefined : { ...latest.current, filter: typed.current }), []);
    /** The history with that typing as its newest step, the way the pause would record it. */
    const withTyping = useCallback((): NavigationHistory<ArchitectureHistoryEntry> => {
        const entry = typedStep();
        return entry === undefined ? history : pushNavigation(history, entry, options);
    }, [history, typedStep]);
    const go = useCallback((step: -1 | 1) => {
        const typedEntry = typedStep();
        const base = withTyping();
        const entry = peekNavigation(base, step);
        if (entry === undefined) {
            // Forward while typing: the text is a new step and drops the forward branch, as after the pause.
            if (typedEntry === undefined) return;
            clearDraft();
            setHistory(base);
            setPlace(typedEntry);
            return;
        }
        restoringKey.current = options.key(entry);
        setHistory(moveNavigation(base, step, options));
        apply(entry);
    }, [typedStep, withTyping, apply, clearDraft]);
    const jump = useCallback((entry: ArchitectureHistoryEntry) => {
        setHistory(withTyping());
        apply(entry);
    }, [withTyping, apply]);

    useEffect(() => {
        if (!active || escapeTaken) return;
        const onKey = (event: globalThis.KeyboardEvent): void => {
            if (event.defaultPrevented || !event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
            if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return;
            if (isTypingTarget(event.target instanceof Element ? event.target : null)) return;
            const step = event.key === 'ArrowLeft' ? -1 : 1;
            if (peekNavigation(withTyping(), step) === undefined) return;
            event.preventDefault();
            go(step);
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [active, escapeTaken, withTyping, go]);

    return { place, filter: draft ?? place.filter, history, back: peekNavigation(history, -1), forward: peekNavigation(history, 1), navigate, type, go, jump };
}
