// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ArchitecturePanel from './ArchitecturePanel';
import type { ArchitecturePanelProps } from './ArchitecturePanel';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';
import { architectureText as text } from './strings';

let container: HTMLDivElement;
let root: Root;
let storageDescriptor: PropertyDescriptor | undefined;

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    storageDescriptor = Object.getOwnPropertyDescriptor(window, 'localStorage');
    const contents = new Map<string, string>();
    Object.defineProperty(window, 'localStorage', { configurable: true, value: {
        getItem: (key: string) => contents.get(key) ?? null,
        setItem: (key: string, value: string) => { contents.set(key, value); },
    } });
});

afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
    if (storageDescriptor) Object.defineProperty(window, 'localStorage', storageDescriptor);
    else delete (window as unknown as Record<string, unknown>).localStorage;
});

function overview(): ArchitectureOverviewDto {
    return {
        projectName: 'sample', totalSymbols: 41, totalRelations: 62,
        symbolKinds: [{ kind: 'Function', count: 41 }], relationKinds: [{ kind: 'CALLS', count: 62 }],
        languages: [{ language: 'TypeScript', fileCount: 8 }],
        groups: [{ name: 'api', symbolCount: 14, fanIn: 1, fanOut: 2 }, { name: 'storage', symbolCount: 27, fanIn: 2, fanOut: 0 }],
        boundaries: [{ from: 'api', to: 'storage', callCount: 12 }],
        layers: [{ group: 'api', layer: 'entry', reason: 'Registered route handlers' }],
        clusters: [{ id: '0', label: 'storage', memberCount: 10, cohesion: 0.8, topMembers: ['persist'] }],
        entryPoints: [{ name: 'start', kind: 'function', filePath: 'src/api.ts', line: 9 }, { name: 'noLocation', kind: 'unknown' }],
        routes: [{ method: 'POST', path: '/users', handler: 'create', origin: 'source', filePath: 'src/api.ts', line: 12 }],
        hotspots: [{ name: 'persist', filePath: 'src/storage.ts', line: 22, fanIn: 0, complexity: 8 }],
        files: ['src/api.ts', 'src/storage.ts'],
    };
}

async function render(changes: Partial<ArchitecturePanelProps> = {}): Promise<void> {
    await act(async () => root.render(<ArchitecturePanel projectName="sample" overview={overview()} onNavigate={vi.fn()} {...changes} />));
}

async function click(selector: string): Promise<void> {
    const target = container.querySelector<HTMLElement>(selector);
    expect(target).not.toBeNull();
    await act(async () => target?.click());
}

async function filter(value: string): Promise<void> {
    const input = container.querySelector<HTMLInputElement>('input[type="search"]')!;
    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
    await act(async () => { setter?.call(input, value); input.dispatchEvent(new Event('input', { bubbles: true })); });
}

describe('architecture workspace', () => {
    it('keeps module, layer, community and indexed-file evidence available in Overview', async () => {
        await render();
        expect(container.textContent).toContain('41');
        expect(container.textContent).toContain('Registered route handlers');
        expect(container.textContent).toContain('persist');
        expect(container.textContent).toContain('src/storage.ts');
        expect(container.querySelector('details')?.open).toBe(false);
    });

    it('opens a source location at the exact provider line and leaves unknown locations unlinked', async () => {
        const onNavigate = vi.fn();
        await render({ onNavigate });
        await click('[data-view="entryPoints"]');
        await click('button[aria-label="Open source: start"]');
        expect(onNavigate).toHaveBeenCalledWith('src/api.ts', 9, 'start');
        const missing = [...container.querySelectorAll('tbody tr')].find(row => row.textContent?.includes('noLocation'));
        expect(missing?.querySelector('button')).toBeNull();
        expect(missing?.textContent).toContain(text.unknown);
    });

    it('retains source-derived route evidence when filtering by handler', async () => {
        await render();
        await click('[data-view="routes"]');
        await filter('create');
        expect(container.querySelectorAll('tbody tr')).toHaveLength(1);
        expect(container.querySelector('[data-origin="source"]')?.textContent).toBe(text.sourceOrigin);
        await filter('unrecorded');
        expect(container.textContent).toContain(text.noMatches);
        expect(container.querySelector('tbody')).toBeNull();
    });

    it('restores configuration per project and prevents a cached project summary from leaking across the switch', async () => {
        await render();
        await click('[data-view="routes"]');
        await filter('users');
        await render({ projectName: 'different' });
        expect(container.querySelector('[data-view="overview"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(container.querySelector('[data-testid="atlas-architecture-content"]')).toBeNull();
        await render();
        expect(container.querySelector('[data-view="routes"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(container.querySelector<HTMLInputElement>('input')?.value).toBe('users');
        const reset = [...container.querySelectorAll('button')].find(button => button.textContent === text.reset)!;
        await act(async () => reset.click());
        expect(container.querySelector('[data-view="overview"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(container.querySelector<HTMLInputElement>('input')?.value).toBe('');
    });

    it('links the module card and keyboard-accessible dependency map to the same filter', async () => {
        await render();
        await click('button[aria-label="Show relationships for api"]');
        expect(container.querySelector('[data-view="dependencies"]')?.getAttribute('aria-pressed')).toBe('true');
        const target = container.querySelector<SVGGElement>('g[aria-label="Show relationships for storage"]')!;
        expect(target.getAttribute('tabindex')).toBe('0');
        await act(async () => target.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })));
        expect(container.querySelector<HTMLInputElement>('input')?.value).toBe('storage');
        expect(container.querySelectorAll('tbody tr')).toHaveLength(1);
    });

    it('bounds the diagram while keeping every reported relationship reachable', async () => {
        const data = overview();
        data.boundaries = Array.from({ length: 30 }, (_, index) => ({ from: `module-${index}`, to: 'core', callCount: index + 1 }));
        await render({ overview: data });
        await click('[data-view="dependencies"]');
        expect(container.querySelectorAll('svg g[role="button"]')).toHaveLength(8);
        expect(container.textContent).toContain(text.mapLimit(8, 23));
        expect(container.querySelectorAll('tbody tr')).toHaveLength(24);
        const more = [...container.querySelectorAll('button')].find(button => button.textContent === text.showMore)!;
        await act(async () => more.click());
        expect(container.querySelectorAll('tbody tr')).toHaveLength(30);
    });

    it('distinguishes missing hotspot measurements from a measured zero', async () => {
        await render();
        await click('[data-view="hotspots"]');
        const cells = container.querySelectorAll('tbody tr td');
        expect(cells[1].textContent).toBe('0');
        expect(cells[2].textContent).toBe('8');
        expect(cells[3].textContent).toBe(text.unknown);
        expect(cells[4].textContent).toBe(text.unknown);
    });

    it('reports loading and failures without showing a stale success result and offers a retry', async () => {
        const onRefresh = vi.fn();
        await render({ loading: true, onRefresh });
        expect(container.querySelector('[role="status"]')?.textContent).toContain(text.loading);
        expect(container.querySelector('[data-testid="atlas-architecture-content"]')).toBeNull();
        await render({ error: 'Provider unavailable', onRefresh });
        expect(container.querySelector('[role="alert"]')?.textContent).toContain('Provider unavailable');
        const retry = [...container.querySelectorAll('button')].find(button => button.textContent === text.retry)!;
        await act(async () => retry.click());
        expect(onRefresh).toHaveBeenCalledOnce();
        expect(container.querySelector('[data-testid="atlas-architecture-content"]')).toBeNull();
    });
});
