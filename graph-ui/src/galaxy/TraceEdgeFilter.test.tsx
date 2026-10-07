// @vitest-environment jsdom
import { act, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, expect, it, vi } from 'vitest';
import { TraceEdgeFilter } from './TraceEdgeFilter';

const kinds = [
    { type: 'IMPORTS', count: 7, color: '#93a6b8' },
    { type: 'CALLS', count: 12, color: '#ad95b3' },
    { type: 'HTTP_CALLS', count: 2, color: '#91b0a7' },
];
let dispose: (() => Promise<void>) | undefined;
afterEach(async () => { await dispose?.(); dispose = undefined; });

async function mount(initial?: readonly string[], renderedKinds = kinds, availableTypes?: readonly string[]) {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.appendChild(host);
    const root = createRoot(host), changed = vi.fn();
    dispose = async () => { await act(async () => root.unmount()); host.remove(); };
    function ControlledFilter() {
        const [selected, setSelected] = useState<readonly string[] | undefined>(initial);
        return <TraceEdgeFilter kinds={renderedKinds} availableTypes={availableTypes} selected={selected} onChange={value => { changed(value); setSelected(value); }} />;
    }
    await act(async () => root.render(<ControlledFilter />));
    host.querySelector('details')!.open = true;
    const checkbox = (type: string) => [...host.querySelectorAll('label')]
        .find(label => label.textContent === type)!.querySelector<HTMLInputElement>('input')!;
    const button = (name: string) => [...host.querySelectorAll('button')]
        .find(button => (button.getAttribute('aria-label') ?? button.textContent) === name)!;
    return { host, changed, checkbox, button };
}

it('selects only one relationship type and reflects it in labeled checkboxes', async () => {
    const { host, changed, checkbox, button } = await mount();
    expect(host.querySelector('[role="group"]')?.getAttribute('aria-label')).toBe('Trace edge types');
    expect(kinds.every(kind => checkbox(kind.type).type === 'checkbox' && checkbox(kind.type).checked)).toBe(true);
    await act(async () => button('Only CALLS').click());
    expect(changed).toHaveBeenLastCalledWith(['CALLS']);
    expect(checkbox('CALLS').checked).toBe(true);
    expect(checkbox('IMPORTS').checked).toBe(false);
    expect(checkbox('HTTP_CALLS').checked).toBe(false);
    expect(host.querySelector('summary')?.textContent).toBe('Edge types · 1');
});

it('starts a checkbox change from every available type when All is active', async () => {
    const { host, changed, checkbox } = await mount();
    expect(host.querySelector('summary')?.textContent).toBe('Edge types · All');
    await act(async () => checkbox('IMPORTS').click());
    expect(changed).toHaveBeenLastCalledWith(['CALLS', 'HTTP_CALLS']);
    expect(checkbox('IMPORTS').checked).toBe(false);
    expect(checkbox('CALLS').checked).toBe(true);
    await act(async () => checkbox('IMPORTS').click());
    expect(changed).toHaveBeenLastCalledWith(['CALLS', 'HTTP_CALLS', 'IMPORTS']);
    expect(checkbox('IMPORTS').checked).toBe(true);
});

it('distinguishes explicit None from All and updates every checkbox', async () => {
    const { host, changed, checkbox, button } = await mount(['CALLS']);
    await act(async () => button('No types').click());
    expect(changed).toHaveBeenLastCalledWith([]);
    expect(host.querySelector('summary')?.textContent).toBe('Edge types · None');
    expect(kinds.every(kind => !checkbox(kind.type).checked)).toBe(true);
    await act(async () => button('All types').click());
    expect(changed).toHaveBeenLastCalledWith(undefined);
    expect(host.querySelector('summary')?.textContent).toBe('Edge types · All');
    expect(kinds.every(kind => checkbox(kind.type).checked)).toBe(true);
});

it('adds the checked type from None and returns keyboard focus when dismissed', async () => {
    const { host, changed, checkbox } = await mount([]);
    await act(async () => checkbox('HTTP_CALLS').click());
    expect(changed).toHaveBeenLastCalledWith(['HTTP_CALLS']);
    const input = checkbox('HTTP_CALLS'); input.focus();
    const escape = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true });
    await act(async () => input.dispatchEvent(escape));
    expect(host.querySelector('details')?.open).toBe(false);
    expect(document.activeElement).toBe(host.querySelector('summary'));
});

it('counts only rendered types without excluding relationships outside render limits', async () => {
    const { host, changed, checkbox, button } = await mount(undefined,
        [{ ...kinds[1]!, count: 3 }], kinds.map(kind => kind.type));
    expect(host.querySelectorAll('input[type="checkbox"]')).toHaveLength(1);
    expect(host.querySelector('.atlas-trace-edge-count')?.textContent).toBe('3');
    expect(host.querySelector('.atlas-trace-edge-count')?.getAttribute('aria-label')).toBe('3 rendered CALLS edges');
    await act(async () => checkbox('CALLS').click());
    expect(changed).toHaveBeenLastCalledWith(['HTTP_CALLS', 'IMPORTS']);
    expect(host.querySelector('summary')?.textContent).toBe('Edge types · None');
    await act(async () => button('All types').click());
    expect(changed).toHaveBeenLastCalledWith(undefined);
});

it('keeps All types available when filtering leaves no rendered edges', async () => {
    const { host, changed, button } = await mount([], []);
    expect(host.querySelectorAll('input[type="checkbox"]')).toHaveLength(0);
    expect(host.textContent).toContain('No edges in this view.');
    await act(async () => button('All types').click());
    expect(changed).toHaveBeenLastCalledWith(undefined);
});
