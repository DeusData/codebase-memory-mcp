/**
 * Die portierte Lesart der Checkliste.
 *
 * Zwei Eigenschaften, und die zweite ist die, um derentwillen die Datei
 * existiert: die Gruppen stehen in der Reihenfolge, in der Nichtwissen weh tut,
 * und eine Kategorie, die dort nicht vorkommt, faellt hinten an statt weg. Und
 * die beiden Zaehler bleiben getrennt, auch je Gruppe.
 */

import { describe, expect, it } from 'vitest';

import { CATEGORY_ORDER } from '../twin/mini-understanding';
import { groupByCategory, percentOf } from './checklist-model';
import type { ChecklistItemState } from './checklist-model';

const item = (
    id: string,
    category: ChecklistItemState['category'],
    visited = false,
    confirmed = false,
): ChecklistItemState => ({ id, category, label: id, visited, confirmed });

describe('grouping', () => {
    it('uses the order of the twin strip, not the order the items arrived in', () => {
        const groups = groupByCategory([
            item('t', 'tests'),
            item('c', 'core-logic'),
            item('e', 'error-handling'),
        ]);
        expect(groups.map((group) => group.category)).toEqual(['core-logic', 'error-handling', 'tests']);
    });

    it('appends a category the order does not name rather than dropping it', () => {
        const groups = groupByCategory([item('s', 'state'), item('c', 'core-logic')]);
        expect(CATEGORY_ORDER).not.toContain('state');
        expect(groups.map((group) => group.category)).toEqual(['core-logic', 'state']);
    });

    it('keeps the items of a category in the order they were generated in', () => {
        const groups = groupByCategory([item('a', 'core-logic'), item('b', 'core-logic')]);
        expect(groups[0].items.map((entry) => entry.id)).toEqual(['a', 'b']);
    });

    it('counts exploration and confirmation apart, per group', () => {
        const groups = groupByCategory([
            item('a', 'core-logic', true, true),
            item('b', 'core-logic', true, false),
            item('c', 'core-logic', false, false),
        ]);
        expect(groups[0].visited).toBe(2);
        expect(groups[0].confirmed).toBe(1);
    });

    it('has nothing to group when there is nothing', () => {
        expect(groupByCategory([])).toEqual([]);
    });
});

describe('one counter as a percentage', () => {
    it('is zero of zero rather than an error', () => {
        expect(percentOf(0, 0)).toBe(0);
    });

    it('rounds the way the twin strip rounds', () => {
        expect(percentOf(1, 3)).toBe(33);
        expect(percentOf(2, 3)).toBe(67);
        expect(percentOf(3, 3)).toBe(100);
    });
});
