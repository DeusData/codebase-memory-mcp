import { describe, expect, it } from 'vitest';
import {
    NAVIGATION_HISTORY_LIMIT,
    emptyNavigationHistory,
    moveNavigation,
    peekNavigation,
    pushNavigation,
    refreshNavigation,
    replaceNavigation,
    type NavigationHistory,
    type NavigationHistoryOptions,
} from './navigation-history';

interface Entry { root?: string; depth: number }
const options: NavigationHistoryOptions<Entry> = { key: (entry) => `${entry.root ?? '*'}@${entry.depth}`, recentKey: (entry) => entry.root };
const keys = (history: NavigationHistory<Entry>) => history.entries.map(options.key);
const push = (history: NavigationHistory<Entry>, ...entries: Entry[]) => entries.reduce((next, entry) => pushNavigation(next, entry, options), history);

describe('bounded back and forward history', () => {
    it('moves a cursor back and forward like a browser, and stops at both ends', () => {
        let history = push(emptyNavigationHistory<Entry>(), { depth: 0 }, { root: 'A', depth: 1 }, { root: 'B', depth: 1 });
        expect(peekNavigation(history, -1)).toEqual({ root: 'A', depth: 1 });
        expect(peekNavigation(history, 1)).toBeUndefined();
        history = moveNavigation(history, -1, options);
        history = moveNavigation(history, -1, options);
        expect(history.index).toBe(0);
        expect(peekNavigation(history, -1)).toBeUndefined();
        expect(moveNavigation(history, -1, options)).toBe(history);
        history = moveNavigation(history, 1, options);
        expect(history.entries[history.index]).toEqual({ root: 'A', depth: 1 });
        expect(peekNavigation(history, 1)).toEqual({ root: 'B', depth: 1 });
    });

    it('merges a step equal to the current one, but keeps a later revisit as its own step', () => {
        const history = push(emptyNavigationHistory<Entry>(), { root: 'A', depth: 1 }, { root: 'A', depth: 1 }, { root: 'A', depth: 2 },
            { root: 'B', depth: 1 }, { root: 'A', depth: 2 });
        expect(keys(history)).toEqual(['A@1', 'A@2', 'B@1', 'A@2']);
    });

    it('drops the forward branch on a new navigation after going back', () => {
        let history = push(emptyNavigationHistory<Entry>(), { root: 'A', depth: 1 }, { root: 'B', depth: 1 }, { root: 'C', depth: 1 });
        history = moveNavigation(moveNavigation(history, -1, options), -1, options);
        history = push(history, { root: 'D', depth: 1 });
        expect(keys(history)).toEqual(['A@1', 'D@1']);
        expect(peekNavigation(history, 1)).toBeUndefined();
    });

    it('never holds more than the limit and drops the oldest entries first', () => {
        const many = Array.from({ length: NAVIGATION_HISTORY_LIMIT + 40 }, (_, at) => ({ root: `N${at}`, depth: 1 }));
        const history = push(emptyNavigationHistory<Entry>(), ...many);
        expect(history.entries).toHaveLength(NAVIGATION_HISTORY_LIMIT);
        expect(history.entries[0]!.root).toBe('N40');
        expect(history.entries.at(-1)!.root).toBe(`N${NAVIGATION_HISTORY_LIMIT + 39}`);
        expect(history.index).toBe(NAVIGATION_HISTORY_LIMIT - 1);
        const short = push(emptyNavigationHistory<Entry>(), ...many.slice(0, 5));
        expect(pushNavigation(short, { root: 'X', depth: 1 }, { ...options, limit: 3 }).entries.map((entry) => entry.root)).toEqual(['N3', 'N4', 'X']);
    });

    it('keeps the last distinct roots, newest first, independent of the cursor', () => {
        let history = push(emptyNavigationHistory<Entry>(), { depth: 0 }, { root: 'A', depth: 1 }, { root: 'B', depth: 1 },
            { root: 'A', depth: 3 }, { root: 'C', depth: 1 });
        expect(history.recent).toEqual([{ root: 'C', depth: 1 }, { root: 'A', depth: 3 }, { root: 'B', depth: 1 }]);
        history = moveNavigation(history, -1, options);
        // Going back visits A again: it moves to the front, nothing is lost.
        expect(history.recent.map((entry) => entry.root)).toEqual(['A', 'C', 'B']);
        const capped = push(emptyNavigationHistory<Entry>(), ...Array.from({ length: 12 }, (_, at) => ({ root: `R${at}`, depth: 1 })));
        expect(capped.recent).toHaveLength(8);
        expect(capped.recent[0]!.root).toBe('R11');
    });

    it('replaces the current step for a change the page makes on its own, keeping the forward branch (K27)', () => {
        let history = push(emptyNavigationHistory<Entry>(), { root: 'A', depth: 1 }, { root: 'B', depth: 1 }, { root: 'C', depth: 1 });
        history = moveNavigation(history, -1, options);
        history = replaceNavigation(history, { root: 'B2', depth: 1 }, options);
        expect(keys(history)).toEqual(['A@1', 'B2@1', 'C@1']);
        expect(history.index).toBe(1);
        expect(peekNavigation(history, 1)).toEqual({ root: 'C', depth: 1 });
        // The replaced place leaves the recent list with it.
        expect(history.recent.map((entry) => entry.root)).toEqual(['B2', 'C', 'A']);
        // Equal to the current step: nothing changes. Equal to the step before: the two merge.
        expect(replaceNavigation(history, { root: 'B2', depth: 1 }, options)).toBe(history);
        expect(keys(replaceNavigation(history, { root: 'A', depth: 1 }, options))).toEqual(['A@1', 'C@1']);
        expect(replaceNavigation(history, { root: 'A', depth: 1 }, options).index).toBe(0);
        // Without a current step it is an ordinary first step.
        expect(keys(replaceNavigation(emptyNavigationHistory<Entry>(), { root: 'A', depth: 1 }, options))).toEqual(['A@1']);
    });

    it('merges a replaced step with the step after it too, so Forward never leads to the same place (K27)', () => {
        // After Back, the page resets the place it shows (a reindex), and the reset equals the step ahead.
        let history = push(emptyNavigationHistory<Entry>(), { root: 'A', depth: 1 }, { root: 'B', depth: 2 }, { root: 'B', depth: 1 });
        history = moveNavigation(history, -1, options);
        history = replaceNavigation(history, { root: 'B', depth: 1 }, options);
        expect(keys(history)).toEqual(['A@1', 'B@1']);
        expect(history.index).toBe(1);
        expect(peekNavigation(history, 1)).toBeUndefined();
        // Equal to the steps on both sides: all three become one.
        let between = push(emptyNavigationHistory<Entry>(), { root: 'A', depth: 1 }, { root: 'B', depth: 1 }, { root: 'A', depth: 1 }, { root: 'C', depth: 1 });
        between = moveNavigation(moveNavigation(between, -1, options), -1, options);
        between = replaceNavigation(between, { root: 'A', depth: 1 }, options);
        expect(keys(between)).toEqual(['A@1', 'C@1']);
        expect(between.index).toBe(0);
        for (let at = 1; at < between.entries.length; at++) expect(options.key(between.entries[at]!)).not.toBe(options.key(between.entries[at - 1]!));
    });

    it('refreshes the current step with newer details of the same place, without a step or reordering (K27)', () => {
        interface Named extends Entry { name?: string }
        const named: NavigationHistoryOptions<Named> = options;
        let history = [{ root: 'A', depth: 1 }, { root: 'B', depth: 1 }].reduce((next, entry) => pushNavigation(next, entry, named), emptyNavigationHistory<Named>());
        const fresher = { root: 'B', depth: 1, name: 'Bee' };
        history = refreshNavigation(history, fresher, named);
        expect(history.entries).toEqual([{ root: 'A', depth: 1 }, fresher]);
        expect(history.index).toBe(1);
        expect(history.recent).toEqual([fresher, { root: 'A', depth: 1 }]);
        // Another key is a step, not a refresh; the same object changes nothing.
        expect(refreshNavigation(history, { root: 'C', depth: 1 }, named)).toBe(history);
        expect(refreshNavigation(history, fresher, named)).toBe(history);
        expect(refreshNavigation(emptyNavigationHistory<Named>(), fresher, named).entries).toEqual([]);
    });
});
