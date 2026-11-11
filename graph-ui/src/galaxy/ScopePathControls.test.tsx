// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { PathPicker, PathSteps } from './ScopePathControls';
import type { GraphNode } from './types';

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });
const node = (id: number, name: string, file = `src/${name}.py`): GraphNode => ({ id, name, file_path: file, qualified_name: `p.${name}`,
    label: 'Function', x: 0, y: 0, z: 0, size: 2, color: '#999999' });
const type = async (input: HTMLInputElement, value: string) => act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, value);
    input.dispatchEvent(new Event('input', { bubbles: true }));
});

it('searches only the loaded scope, sorted by name, and closes after a pick', async () => {
    const onPick = vi.fn();
    await act(async () => root.render(<PathPicker nodes={[node(3, 'save'), node(1, 'as_sql'), node(2, 'resolve', 'other/save_helpers.py')]} onPick={onPick} />));
    const details = host.querySelector('details')!;
    details.open = true;
    const names = () => [...host.querySelectorAll('li strong')].map(entry => entry.textContent);
    expect(names()).toEqual(['as_sql', 'resolve', 'save']);
    await type(host.querySelector<HTMLInputElement>('input[aria-label="Find a path target"]')!, 'save');
    // A name match ranks above a match in the file path.
    expect(names()).toEqual(['save', 'resolve']);
    await type(host.querySelector<HTMLInputElement>('input')!, 'nothing');
    expect(host.textContent).toContain('No matching node in the loaded scope.');
    await type(host.querySelector<HTMLInputElement>('input')!, 'sql');
    await act(async () => host.querySelector<HTMLButtonElement>('li button')!.click());
    expect(onPick).toHaveBeenCalledWith(expect.objectContaining({ id: 1 }));
    expect(details.open).toBe(false);
});

it('lists every hop with its indexed direction and steps a highlight through them', async () => {
    const onStep = vi.fn(), onClear = vi.fn();
    const names = new Map([[1, 'JSONBAgg'], [2, 'test_jsonb_agg'], [3, 'Aggregate']]);
    const steps = [{ edge: { id: 7, source: 2, target: 1, type: 'CALLS' }, from: 1, to: 2 },
        { edge: { id: 8, source: 1, target: 3, type: 'INHERITS' }, from: 1, to: 3 }];
    const render = (active: number) => act(async () => root.render(<PathSteps heading="Path" steps={steps} active={active}
        nameOf={id => names.get(id)!} lines={false} onStep={onStep} onClear={onClear} />));
    await render(0);
    expect([...host.querySelectorAll('li code')].map(entry => entry.textContent))
        .toEqual(['JSONBAgg <--CALLS-- test_jsonb_agg', 'JSONBAgg --INHERITS--> Aggregate']);
    expect([...host.querySelectorAll('.atlas-galaxy-path-hop')].map(entry => entry.textContent)).toEqual(['hop 1', 'hop 2']);
    expect(host.querySelector('[aria-current="step"]')?.textContent).toContain('test_jsonb_agg');
    const button = (label: string) => [...host.querySelectorAll('button')].find(entry => entry.textContent === label)!;
    expect(button('Previous').disabled).toBe(true);
    await act(async () => button('Next').click());
    expect(onStep).toHaveBeenLastCalledWith(1);
    await render(1);
    expect(button('Next').disabled).toBe(true);
    expect(host.textContent).toContain('2 of 2');
    await act(async () => button('Clear').click());
    expect(onClear).toHaveBeenCalledOnce();
});

it('shows call-site lines for a call order and a note without steps', async () => {
    const steps = [{ edge: { id: 1, source: 1, target: 2, type: 'CALLS', line: 14 }, from: 1, to: 2 },
        { edge: { id: 2, source: 1, target: 3, type: 'CALLS' }, from: 1, to: 3 }];
    await act(async () => root.render(<PathSteps heading="Calls" steps={steps} active={0} nameOf={String} lines onStep={vi.fn()} onClear={vi.fn()} />));
    expect([...host.querySelectorAll('.atlas-galaxy-path-hop')].map(entry => entry.textContent)).toEqual(['line 14', 'no line']);
    await act(async () => root.render(<PathSteps heading="Path" note="No path" steps={[]} active={0} nameOf={String} lines={false} onStep={vi.fn()} onClear={vi.fn()} />));
    expect(host.querySelector('ol')).toBeNull();
    expect(host.textContent).toContain('No path');
});
