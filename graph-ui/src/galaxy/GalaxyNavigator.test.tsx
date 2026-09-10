// @vitest-environment jsdom
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import GalaxyNavigator from './GalaxyNavigator';
import type { GraphNode } from './types';

it('finds entry points and attaches the exact selected graph node', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.appendChild(host);
    const root = createRoot(host);
    const entry: GraphNode = { id: 7, name: 'main', label: 'Function', status: 'entry', file_path: 'src/main.c', x: 0, y: 0, z: 0, size: 1, color: '#fff' };
    const ordinary: GraphNode = { ...entry, id: 8, name: 'helper', status: 'normal' };
    const select = vi.fn();
    try {
        await act(async () => root.render(<GalaxyNavigator nodes={[entry, ordinary]} onSelect={select} />));
        expect([...host.querySelectorAll('button')].map(button => button.textContent)).toEqual(['mainsrc/main.c']);
        await act(async () => host.querySelector<HTMLButtonElement>('button')!.click());
        expect(select).toHaveBeenCalledWith(entry);
        await act(async () => host.querySelector<HTMLInputElement>('input[type=checkbox]')!.click());
        expect(host.querySelectorAll('button')).toHaveLength(2);
    } finally { await act(async () => root.unmount()); host.remove(); }
});
