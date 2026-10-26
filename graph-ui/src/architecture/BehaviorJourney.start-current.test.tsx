// @vitest-environment jsdom
/*
 * Review of K43: with a filter, "Matching operations · 5 of 61" held six
 * entries, the last the current start that did not match. The current start
 * now has a group of its own, and the count of every group equals its
 * entries.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import BehaviorJourney, { type BehaviorJourneyProps } from './BehaviorJourney';
import { architectureText } from './strings';
import type { SystemProjection, SystemSymbol } from './system-architecture-source';

vi.mock('./BehaviorSourceEvidence', () => ({ default: () => <div data-source-evidence /> }));
vi.mock('./SystemArchitectureScene', () => ({ default: () => <div data-scene /> }));

const symbol = (id: number, name = `operation${id}`, file_path = `src/part${id % 2}.ts`): SystemSymbol => ({ id, name, qualified_name: `sample.${name}`, label: 'Function',
    component_id: `component${id % 2}`, file_path, start_line: id });
const LOADDATA = 'django/core/management/commands/loaddata.py';
const entries = [symbol(1), symbol(21, 'handle', 'django/core/management/commands/makemigrations.py'), symbol(22, 'handle', LOADDATA), symbol(23, 'as_sql', 'django/db/models/fields/json.py')];
function fixture(): SystemProjection {
    const nodes = [symbol(1), symbol(2)];
    return { schema_version: 1, status: 'ready', kind: 'static_projection', complete: true, components: [], dependencies: [], cycles: [], entrypoints: [nodes[0]!],
        paths: [{ entrypoint_id: 1, nodes, edges: [{ id: 10, source_id: 1, target_id: 2, type: 'CALLS', callsite: { file_path: 'src/part1.ts', line: 20 } }] }],
        totals: {}, limits: {}, warnings: [] };
}

let container: HTMLDivElement, root: Root;
beforeEach(() => { (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true; container = document.createElement('div'); document.body.append(container); root = createRoot(container); });
afterEach(async () => { await act(async () => root.unmount()); container.remove(); });

const select = () => container.querySelector<HTMLSelectElement>('select[aria-label="Behavior entry point"]')!;
const filterField = () => container.querySelector<HTMLInputElement>('input[aria-label="Filter operations"]')!;
const groups = () => [...select().querySelectorAll('optgroup')].map(group => ({ label: group.label,
    options: [...group.querySelectorAll('option')].filter(option => !option.disabled).map(option => option.textContent) }));
async function type(value: string) {
    await act(async () => {
        Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(filterField(), value);
        filterField().dispatchEvent(new Event('input', { bubbles: true }));
    });
}

it('K43: the current start that does not match the filter stands in a group of its own, and each count equals its entries', async () => {
    const data = fixture();
    const props: BehaviorJourneyProps = { project: 'sample', generation: 'g1', data, entries, suggestedEntries: [1, 21],
        targets: [{ ...data.paths[0]!.nodes[1]!, distance: 1 }], entryId: 1, active: true, pending: false, filter: '',
        onRequest: vi.fn(), onRefresh: vi.fn(), onNavigate: vi.fn(), onSelectSymbol: vi.fn() };
    await act(async () => root.render(<BehaviorJourney {...props} />));

    await type('loaddata');
    expect(groups()).toEqual([
        { label: architectureText.behaviorStart.current, options: ['operation1 · src/part1.ts'] },
        { label: 'Matching operations · 1 of 4', options: [`handle · ${LOADDATA}`] },
    ]);
    expect(architectureText.behaviorStart.current).toBe('Current start, not matching the filter');
    // The field still shows the current start.
    expect(select().value).toBe('1');
    expect(select().selectedOptions[0]?.textContent).toBe('operation1 · src/part1.ts');

    // A filter the current start matches: no group of its own, it stands among the matches.
    await type('operation1');
    expect(groups()).toEqual([
        { label: 'Suggested', options: ['operation1 · src/part1.ts'] },
        { label: 'Matching operations · 1 of 4', options: ['operation1 · src/part1.ts'] },
    ]);
    // Nothing matches: the current start alone stands apart, the matches say so.
    await type('no such operation');
    expect(groups().map(group => group.label)).toEqual([architectureText.behaviorStart.current, 'Matching operations · 0 of 4']);
    expect(select().querySelector('optgroup:last-child option:last-child')?.textContent).toBe('No operation matches "no such operation"');
});
