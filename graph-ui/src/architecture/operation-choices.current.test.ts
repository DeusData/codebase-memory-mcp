import { expect, it } from 'vitest';
import { operationChoices } from './operation-choices';
import type { SystemSymbol } from './system-architecture-source';

/*
 * Review of K43: with a filter the current start was kept under "Matching
 * operations · N of M" even when it did not match, so the group showed one
 * entry more than its count ("5 of 61" with six entries, the last one
 * "main · …/manage.py-tpl"). The kept start now stands apart, and every
 * group holds exactly what it says.
 */
const operation = (id: number, name: string, file_path: string): SystemSymbol =>
    ({ id, name, qualified_name: '', label: '', file_path, component_id: '' });
const entries = [
    operation(1, 'main', 'django/conf/project_template/manage.py-tpl'),
    operation(2, '__init__', 'django/db/models/aggregates.py'),
    operation(3, '__init__', 'django/forms/fields.py'),
    operation(4, 'handle', 'django/core/management/commands/loaddata.py'),
];

it('K43: a current start that does not match the filter stands apart, not among the matches', () => {
    const choices = operationChoices(entries, { query: '__init__', keep: 1, suggested: [1, 4] });
    expect(choices.current?.id).toBe(1);
    expect(choices.current?.label).toBe('main · django/conf/project_template/manage.py-tpl');
    expect(choices.all.map(item => item.id)).toEqual([2, 3]);
    expect(choices.all).toHaveLength(choices.matching);
    // The suggestions are filtered the same way: the current start is not repeated there either.
    expect(choices.suggested).toEqual([]);
});

it('K43: a current start that matches stays in its place, and there is no separate entry', () => {
    const choices = operationChoices(entries, { query: 'handle', keep: 4, suggested: [1, 4] });
    expect(choices.current).toBeUndefined();
    expect(choices.all.map(item => item.id)).toEqual([4]);
    expect(choices.suggested.map(item => item.id)).toEqual([4]);
    expect(choices.matching).toBe(1);
});

it('K43: without a filter every operation is listed, the current one in its place', () => {
    const choices = operationChoices(entries, { keep: 1, suggested: [1, 4] });
    expect(choices.current).toBeUndefined();
    expect(choices.all).toHaveLength(4);
    expect(choices.suggested.map(item => item.id)).toEqual([1, 4]);
});
