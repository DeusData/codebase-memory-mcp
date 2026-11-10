// @vitest-environment jsdom
/*
 * Hand test 2026-10-04, round 4 (N1): wherever the Galaxy shows a node by
 * name, a Branch node carries what it is ("django-demo · detached HEAD"),
 * not the bare "DETACHED" that reads like a folder. The real name stays in
 * the tooltip and the detail line.
 */
import { act, type ReactNode } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { PerspectiveCamera } from 'three';
import type { GraphNode } from './types';

const fiber = vi.hoisted(() => ({ frames: [] as ((state: unknown, delta: number) => void)[] }));
let scene: { camera: PerspectiveCamera; gl: { domElement: HTMLCanvasElement }; size: { width: number; height: number } };
vi.mock('@react-three/fiber', async importOriginal => ({
    ...await importOriginal<typeof import('@react-three/fiber')>(),
    useFrame: (callback: (state: unknown, delta: number) => void) => { fiber.frames.push(callback); },
    useThree: (select?: (state: unknown) => unknown) => (select ? select(scene) : scene),
}));
vi.mock('@react-three/drei', async importOriginal => ({
    ...await importOriginal<typeof import('@react-three/drei')>(),
    Html: ({ children }: { children: ReactNode }) => <div data-testid="html">{children}</div>,
}));

const { RootMarkers } = await import('./GraphScene');
const { NodeTooltipCard } = await import('./NodeTooltipCard');
const { NodeLabels } = await import('./NodeLabels');
const { PathPicker } = await import('./ScopePathControls');
const { default: GalaxyNavigator, choicesFromSearchHits } = await import('./GalaxyNavigator');

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const canvas = document.createElement('canvas');
    canvas.getBoundingClientRect = () => ({ left: 0, top: 0, right: 800, bottom: 600, width: 800, height: 600, x: 0, y: 0, toJSON: () => ({}) });
    scene = { camera: new PerspectiveCamera(50, 800 / 600, 0.1, 100000), gl: { domElement: canvas }, size: { width: 800, height: 600 } };
    fiber.frames = [];
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(() => { act(() => root.unmount()); host.remove(); vi.restoreAllMocks(); });

const detached: GraphNode = { id: 2, x: 0, y: 0, z: 0, size: 5.8, color: '#ffc070', label: 'Branch', name: 'DETACHED',
    qualified_name: 'django-demo.__branch__.detached', file_path: '{}', status: 'structural', in_calls: 0, out_calls: 0 };
const workingTree: GraphNode = { ...detached, id: 3, name: 'working-tree', qualified_name: 'cbm.__branch__.working-tree' };
const github: GraphNode = { ...detached, id: 4, label: 'Folder', name: '.github', qualified_name: 'django-demo..github', file_path: '.github' };

it('N1: the root marker names the Branch node for what it is', () => {
    act(() => root.render(<RootMarkers nodes={[detached, github]} />));
    expect([...host.querySelectorAll('[data-testid="atlas-galaxy-root-marker"] b')].map(name => name.textContent))
        .toEqual(['django-demo · detached HEAD', '.github']);
});

it('N1: the hover card shows the name, keeps the real one in its detail line, and offers no file to open', () => {
    // query_graph answers a Branch node with the lines 0 to 0; that is no line range ("lines 1" in the browser).
    act(() => root.render(<NodeTooltipCard node={{ ...workingTree, start_line: 0, end_line: 0 }} />));
    expect([...host.querySelectorAll('.atlas-galaxy-card-row dt')].map(term => term.textContent)).toEqual(['fan-in', 'fan-out']);
    const card = host.querySelector('[data-testid="atlas-galaxy-card"]')!;
    expect(card.querySelector('.atlas-galaxy-card-name')?.textContent).toBe('cbm · working tree');
    expect(card.querySelector('.atlas-galaxy-card-label')?.textContent).toBe('Branch');
    expect(card.querySelector('.atlas-galaxy-card-path')?.textContent).toBe('Name in the index: working-tree (cbm.__branch__.working-tree)');
    expect(card.querySelector('.atlas-galaxy-card-action')?.textContent).toBe('no file in the index: nothing to open');
    expect(card.textContent).not.toContain('{}');
});

it('N1: the name drawn into the label texture of the scene is the shown name', () => {
    const drawn: string[] = [];
    const context = { font: '', textAlign: '', textBaseline: '', lineJoin: '', lineWidth: 0, strokeStyle: '', fillStyle: '',
        measureText: (text: string) => ({ width: text.length * 30 }), scale: () => {}, strokeText: () => {}, fillText: (text: string) => { drawn.push(text); } };
    vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation((() => context) as unknown as HTMLCanvasElement['getContext']);
    vi.spyOn(console, 'error').mockImplementation(() => {});
    act(() => root.render(<NodeLabels nodes={[detached, github]} highlightedIds={null} maxTextWidth={4000} />));
    expect(drawn.sort()).toEqual(['.github', 'django-demo · detached HEAD']);
});

it('N1: Path to lists the Branch node by its shown name and finds it by either name', async () => {
    const pick = vi.fn();
    await act(async () => root.render(<PathPicker nodes={[github, detached]} onPick={pick} />));
    const names = () => [...host.querySelectorAll('.atlas-graph-path-menu li button strong')].map(name => name.textContent);
    expect(names()).toEqual(['.github', 'django-demo · detached HEAD']);
    const input = host.querySelector<HTMLInputElement>('.atlas-graph-path-menu input')!;
    const type = async (value: string) => act(async () => {
        Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, value);
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
    await type('detached head');
    expect(names()).toEqual(['django-demo · detached HEAD']);
    await type('DETACHED');
    expect(names()).toEqual(['django-demo · detached HEAD']);
    // The detail line shows the qualified name, never the "{}" the index stores as its file.
    expect(host.querySelector('.atlas-graph-path-menu li button span')?.textContent).toBe('django-demo.__branch__.detached');
});

it('N1: the search lists the Branch node by its shown name, keeps the real one in the tooltip, and makes no file of "{}"', async () => {
    const choices = choicesFromSearchHits([{ name: 'DETACHED', qualified_name: 'django-demo.__branch__.detached', label: 'Branch', file_path: '{}' }]);
    expect(choices.map(choice => [choice.kind, choice.name, choice.detail])).toEqual([['Branch', 'django-demo · detached HEAD', 'django-demo.__branch__.detached']]);
    // The scope keeps the name of the index: it is the identity the chat and the history read.
    expect(choices[0]!.scope).toEqual({ kind: 'symbol', qualifiedName: 'django-demo.__branch__.detached', name: 'DETACHED' });

    const select = vi.fn();
    await act(async () => root.render(<GalaxyNavigator embedded nodes={[detached, github]} onSelect={select} />));
    const input = host.querySelector<HTMLInputElement>('input[aria-label="Find a graph node"]')!;
    await act(async () => {
        input.focus();
        Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, 'detached');
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
    const results = [...host.querySelectorAll<HTMLButtonElement>('.atlas-galaxy-search-results li button')];
    expect(results.map(button => button.querySelector('strong')?.textContent)).toEqual(['django-demo · detached HEAD']);
    expect(results[0]!.title).toContain('Name in the index: DETACHED (django-demo.__branch__.detached)');
    await act(async () => results[0]!.click());
    expect(select).toHaveBeenCalledWith(detached);
});
