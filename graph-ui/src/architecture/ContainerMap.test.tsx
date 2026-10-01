// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ContainerMap from './ContainerMap';
import { loadContainerInventory, loadContainerTopology } from './container-source';
import type { ContainerReading } from './container-source';
vi.mock('./ArchitectureScene', () => ({ ArchitectureScene: ({ onOpen, onClearSelection }: { onOpen: (id: string) => void; onClearSelection?: () => void }) => <div><button data-testid="open-service" onDoubleClick={() => onOpen('web')}>3D scene</button><button onClick={onClearSelection}>Empty background</button></div> }));
vi.mock('../provider/rpc-client', () => ({ RpcIntelligenceClient: class { listProjects = async () => ({ projects: [{ name: 'p', root_path: '/p' }] }); } }));
vi.mock('./container-source', () => ({ loadContainerInventory: vi.fn(), loadContainerTopology: vi.fn() }));
let container: HTMLDivElement, root: Root;
const sample = (): ContainerReading => ({ projects: [], warnings: [], filesRead: 3, topology: {
    services: ['web', 'db', 'isolated'].map(id => ({ id, name: id, project: 'p', manifest: 'compose.yml', line: id === 'web' ? 2 : 5,
        sourcePaths: [], networks: ['p:default'], ports: [], image: 'fixture:1' })),
    connections: [{ id: 'call', source: 'web', target: 'db', kind: 'call', protocol: 'postgres', evidence: [{ project: 'p', path: 'api.ts', line: 7, summary: 'Connects to db' }] },
        { id: 'startup', source: 'web', target: 'db', kind: 'startup', protocol: 'depends_on', evidence: [{ project: 'p', path: 'compose.yml', line: 4, summary: 'Depends on db' }] }],
    warnings: [], unresolved: [],
} });
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
    vi.mocked(loadContainerInventory).mockResolvedValue({ project: 'p', rootPath: '/p', files: new Map(), manifests: ['compose.yml'], warnings: [] });
    vi.mocked(loadContainerTopology).mockResolvedValue(sample());
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.clearAllMocks(); });
const button = (text: string) => [...container.querySelectorAll('button')].find(element => element.textContent === text)!;
describe('service map interactions', () => {
    it('keeps isolated services visible and distinguishes calls from optional startup edges', async () => {
        await act(async () => root.render(<ContainerMap project="p" active filter="" onNavigate={vi.fn()} />));
        expect(container.textContent).toContain('3 / 3 services · 1 connections shown');
        expect(container.querySelector('[aria-label="All declared services"]')?.textContent).toContain('isolated');
        const startup = [...container.querySelectorAll('label')].find(label => label.textContent === 'Startup dependencies')!.querySelector('input')!;
        await act(async () => startup.click());
        expect(container.textContent).toContain('3 / 3 services · 2 connections shown');
    });
    it('opens exact evidence locations and focuses a service with its neighbors', async () => {
        const navigate = vi.fn();
        await act(async () => root.render(<ContainerMap project="p" active filter="" onNavigate={navigate} />));
        const web = container.querySelector('[aria-label="All declared services"] button')!;
        await act(async () => (web as HTMLButtonElement).click());
        await act(async () => button('Open declaration').click()); expect(navigate).toHaveBeenCalledWith('compose.yml', 2);
        await act(async () => button('Focus service').click()); expect(container.textContent).toContain('2 / 3 services');
        await act(async () => button('All services').click()); expect(container.textContent).toContain('3 / 3 services');
    });
    it('returns from a focused neighborhood to all services and clears the inspector on empty background', async () => {
        const clear = vi.fn();
        await act(async () => root.render(<ContainerMap project="p" active filter="" onNavigate={vi.fn()} onClearSelection={clear} />));
        await act(async () => container.querySelector('[data-testid="open-service"]')!.dispatchEvent(new MouseEvent('dblclick', { bubbles: true })));
        expect(container.textContent).toContain('2 / 3 services');
        expect(button('Open declaration')).toBeDefined();
        await act(async () => button('Empty background').click());
        expect(container.textContent).toContain('3 / 3 services');
        expect(button('Open declaration')).toBeUndefined();
        expect(clear).toHaveBeenCalledOnce();
    });
    it('does not request deployment source while the workspace is inactive', async () => {
        await act(async () => root.render(<ContainerMap project="p" active={false} filter="" onNavigate={vi.fn()} />));
        expect(loadContainerTopology).not.toHaveBeenCalled(); expect(loadContainerInventory).not.toHaveBeenCalled();
    });
});
