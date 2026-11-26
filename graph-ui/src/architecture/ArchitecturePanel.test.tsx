// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ArchitecturePanel from './ArchitecturePanel';
import type { ArchitecturePanelProps } from './ArchitecturePanel';
import type { ArchitectureOverviewDto } from '../core/intelligence-provider';
import { architectureText as text } from './strings';
vi.mock('./ArchitectureScene', () => ({ ArchitectureScene: () => <div data-testid="scene" /> }));
vi.mock('./ContainerMap', () => ({ default: ({ filter }: { filter: string }) => <div data-testid="container-map" data-filter={filter} /> }));

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

describe('architecture workspace', () => {
    it('loads system structure independently when the legacy summary is unavailable', async () => {
        const loader = vi.fn().mockResolvedValue({ status: 'failed', generation: 'g1', error: 'System analysis service unavailable.' });
        await render({ overview: undefined, loading: true, error: 'Legacy summary failed.', systemArchitectureLoader: loader });
        await click('[data-view="structure"]');
        await act(async () => { await vi.dynamicImportSettled(); });
        expect(loader).toHaveBeenCalledWith({ project: 'sample', entryNodeId: undefined }, expect.any(AbortSignal));
        expect(container.textContent).not.toContain('Legacy summary failed.');
        expect(container.textContent).not.toContain(text.loading);
        expect(container.querySelector('[data-testid="atlas-architecture"]')?.getAttribute('aria-busy')).toBe('false');
        expect(container.textContent).toContain('System analysis service unavailable.');
    });

    it('adds system views while retaining the repository views and entry points inside Overview', async () => {
        await render({ graph: { nodes: [], edges: [], total_nodes: 0 } });
        const surface = container.querySelector('[data-testid="spatial-architecture"]');
        expect(surface).not.toBeNull();
        expect([...container.querySelectorAll('[data-view]')].map(node => node.getAttribute('data-view'))).toEqual(['overview', 'routes', 'hotspots', 'structure', 'behavior']);
        for (const view of ['hotspots', 'overview']) {
            await click(`[data-view="${view}"]`);
            expect(container.querySelector('[data-testid="spatial-architecture"]')).toBe(surface);
            expect(container.querySelector('[aria-label="Architecture relationships"]')).not.toBeNull();
        }
        await click('[data-view="routes"]');
        await act(async () => { await vi.dynamicImportSettled(); });
        expect(container.querySelector('[data-testid="container-map"]')).not.toBeNull();
        const endpoints = [...container.querySelectorAll('[aria-label="Routes perspective"] button')].find(button => button.textContent === 'Endpoints')!;
        await act(async () => (endpoints as HTMLButtonElement).click());
        expect(container.querySelector('[data-testid="container-map"]')).toBeNull();
        expect(container.querySelector('[data-testid="spatial-architecture"]')).not.toBeNull();
        expect(container.querySelector('[aria-label="Architecture relationships"]')).not.toBeNull();
        await click('[data-view="overview"]');
        const entry = [...container.querySelectorAll('[aria-label="Overview mode"] button')].find(button => button.textContent === 'Entry points')!;
        await act(async () => (entry as HTMLButtonElement).click());
        expect(container.querySelector('[data-view="overview"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(container.querySelector('select[aria-label="Entry point"]')).not.toBeNull();
    });

    it('keeps the graph primary without duplicating the source guide or architecture search', async () => {
        await render({ graph: { nodes: [], edges: [], total_nodes: 0 } });
        expect(container.querySelector('[data-testid="spatial-architecture"]')).not.toBeNull();
        expect(container.querySelector('[data-testid="atlas-architecture-content"]')).toBeNull();
        expect(container.textContent).not.toContain('Source guide and complete findings');
        expect(container.querySelector('input[type="search"]')).toBeNull();
        expect(container.querySelector<HTMLDetailsElement>('.spatial-map-details')?.open).toBe(false);
        expect(container.querySelector<HTMLDetailsElement>('.spatial-node-list')?.open).toBe(false);
    });

    it('provides a compact source fallback when the graph has not loaded', async () => {
        await render();
        expect(container.textContent).toContain('2 files · 2 source areas · 1 cross-area connections');
        expect(container.querySelectorAll('tbody tr')).toHaveLength(2);
        expect(container.textContent).not.toContain('Registered route handlers');
    });

    it('opens a source location at the exact provider line and leaves unknown locations unlinked', async () => {
        const onNavigate = vi.fn();
        await render({ onNavigate });
        await click('button[aria-label="Open source: start"]');
        expect(onNavigate).toHaveBeenCalledWith('src/api.ts', 9, 'start');
        const missing = [...container.querySelectorAll('tbody tr')].find(row => row.textContent?.includes('noLocation'));
        expect(missing?.querySelector('button')).toBeNull();
        expect(missing?.textContent).toContain(text.unknown);
    });

    it('retains source-derived route evidence and pagination in the non-graph fallback', async () => {
        const data = overview();
        data.routes = Array.from({ length: 30 }, (_, index) => ({ ...data.routes[0], path: `/users/${index}` }));
        await render({ overview: data });
        await click('[data-view="routes"]');
        expect(container.querySelectorAll('tbody tr')).toHaveLength(24);
        expect(container.querySelector('[data-origin="source"]')?.textContent).toBe(text.sourceOrigin);
        const more = [...container.querySelectorAll<HTMLButtonElement>('button')].find(button => button.textContent === text.showMore)!;
        await act(async () => more.click());
        expect(container.querySelectorAll('tbody tr')).toHaveLength(30);
    });

    it('offers the filter in Routes only and hands it to the service and endpoint views', async () => {
        await render({ graph: { nodes: [], edges: [], total_nodes: 0 } });
        expect(container.querySelector('input[type="search"]')).toBeNull();
        await click('[data-view="routes"]');
        await act(async () => { await vi.dynamicImportSettled(); });
        const search = container.querySelector<HTMLInputElement>('input[type="search"]')!;
        expect(search.getAttribute('aria-label')).toBe(text.filter);
        await act(async () => {
            Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(search, '/users');
            search.dispatchEvent(new Event('input', { bubbles: true }));
        });
        expect(container.querySelector('[data-testid="container-map"]')?.getAttribute('data-filter')).toBe('/users');
        await click('[data-view="hotspots"]');
        expect(container.querySelector('input[type="search"]')).toBeNull();
        await click('[data-view="routes"]');
        expect(container.querySelector<HTMLInputElement>('input[type="search"]')?.value).toBe('/users');
    });

    it('restores the view per project while discarding old hidden search filters', async () => {
        window.localStorage.setItem('atlas.architecture.v1:sample', JSON.stringify({ version: 1, view: 'routes', filter: 'unrecorded' }));
        await render();
        expect(container.querySelector('[data-view="routes"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(container.querySelectorAll('tbody tr')).toHaveLength(1);
        expect(container.querySelector('input[type="search"]')).toBeNull();
        await render({ projectName: 'different' });
        expect(container.querySelector('[data-view="overview"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(container.querySelector('[data-testid="atlas-architecture-content"]')).toBeNull();
        await render();
        expect(container.querySelector('[data-view="routes"]')?.getAttribute('aria-pressed')).toBe('true');
        expect(JSON.parse(window.localStorage.getItem('atlas.architecture.v1:sample')!).filter).toBe('');
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
