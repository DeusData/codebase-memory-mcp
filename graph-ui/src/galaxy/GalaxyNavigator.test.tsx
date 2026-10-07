// @vitest-environment jsdom
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import GalaxyNavigator, { choicesFromSearchHits } from './GalaxyNavigator';
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
        expect(host.querySelectorAll('button')).toHaveLength(2);
        await act(async () => [...host.querySelectorAll<HTMLButtonElement>('button')].find(button => button.querySelector('strong')?.textContent === 'main')!.click());
        expect(select).toHaveBeenCalledWith(entry);
        await act(async () => window.dispatchEvent(new Event('cbm:focus-galaxy-search')));
        expect(host.querySelector('details')?.open).toBe(true);
        expect(document.activeElement).toBe(host.querySelector('input'));
    } finally { await act(async () => root.unmount()); host.remove(); }
});

it('derives distinct selectable file/folder clusters from server hits outside the layout', () => {
    const choices = choicesFromSearchHits([
        { name: 'load', qualified_name: 'p.unloaded.load', file_path: 'services/db/load.ts', label: 'Function' },
        { name: 'save', qualified_name: 'p.unloaded.save', file_path: 'services/db/load.ts', label: 'Function' },
    ]);
    expect(choices.filter(choice => choice.kind === 'File')).toHaveLength(1);
    expect(choices.find(choice => choice.key === 'services/db/')?.scope)
        .toEqual({ kind: 'folder', path: 'services/db', name: 'services/db/' });
    expect(choices.find(choice => choice.key === 'services/db/load.ts')?.scope)
        .toEqual({ kind: 'file', path: 'services/db/load.ts', name: 'services/db/load.ts' });
    expect(choices.filter(choice => choice.scope.kind === 'symbol')).toHaveLength(2);
});

it('embeds lookup directly in the Galaxy controls and dismisses results after selection or Escape', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.appendChild(host);
    const root = createRoot(host), select = vi.fn();
    const node: GraphNode = { id: 1, name: 'start', label: 'Function', x: 0, y: 0, z: 0, size: 1, color: '#fff' };
    try {
        await act(async () => root.render(<GalaxyNavigator embedded nodes={[node]} onSelect={select} />));
        expect(host.querySelector('details')).toBeNull();
        expect(host.querySelector('[role="search"] input')).not.toBeNull();
        expect(host.querySelector('.atlas-galaxy-search-results')).toBeNull();
        await act(async () => window.dispatchEvent(new Event('cbm:focus-galaxy-search')));
        expect(host.querySelector('.atlas-galaxy-search-results')).not.toBeNull();
        await act(async () => host.querySelector<HTMLButtonElement>('button')!.click());
        expect(select).toHaveBeenCalledWith(node);
        expect(host.querySelector('.atlas-galaxy-search-results')).toBeNull();
        await act(async () => window.dispatchEvent(new Event('cbm:focus-galaxy-search')));
        await act(async () => host.querySelector('input')!.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })));
        expect(host.querySelector('.atlas-galaxy-search-results')).toBeNull();
        expect(document.activeElement).toBe(host.querySelector('input'));
    } finally { await act(async () => root.unmount()); host.remove(); }
});
