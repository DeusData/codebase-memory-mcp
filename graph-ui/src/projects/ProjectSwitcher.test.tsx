// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ProjectSwitcher from './ProjectSwitcher';
import type { ProjectEntry } from '../provider/rpc-schemas';

let container: HTMLDivElement;
let root: Root;
const currentProject = 'alpha';
const onSelectProject = vi.fn();
const onAddProject = vi.fn();

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    vi.clearAllMocks();
});

afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
});

async function render(listProjects: () => Promise<readonly ProjectEntry[]>, indexActivity?: { name: string; status: 'indexing' | 'done' | 'error' }): Promise<void> {
    await act(async () => root.render(<ProjectSwitcher currentProject={currentProject}
        listProjects={listProjects} onSelectProject={onSelectProject} onAddProject={onAddProject} indexActivity={indexActivity} />));
}

async function click(element: HTMLElement | null): Promise<void> {
    expect(element).not.toBeNull();
    await act(async () => element?.click());
}

function button(label: string): HTMLButtonElement | null {
    return [...container.querySelectorAll('button')].find(node => node.textContent?.includes(label)) ?? null;
}

describe('ProjectSwitcher', () => {
    it('opens the complete project list without search and selects the exact project name only on activation', async () => {
        const list = vi.fn().mockResolvedValue([
            { name: 'beta & tools/日本', root_path: '/repos/backend' },
            { name: currentProject, root_path: '/repos/frontend' },
        ]);
        await render(list);
        expect(container.querySelector('summary')?.textContent).toContain(currentProject);
        expect(list).not.toHaveBeenCalled();
        await click(container.querySelector('summary'));
        expect(container.querySelector('input')).toBeNull();
        expect(button(currentProject)?.getAttribute('aria-current')).toBe('true');
        expect(container.querySelectorAll('.atlas-project-results li')).toHaveLength(2);
        expect(onSelectProject).not.toHaveBeenCalled();
        expect(list).toHaveBeenCalledTimes(1);
        await click(button('beta & tools/日本'));
        expect(onSelectProject).toHaveBeenCalledExactlyOnceWith('beta & tools/日本');
        expect(container.querySelector('details')?.open).toBe(false);
    });

    it('keeps selecting the current project a no-op and refreshes the list when reopened', async () => {
        const list = vi.fn().mockResolvedValueOnce([{ name: currentProject }])
            .mockResolvedValueOnce([{ name: currentProject }, { name: 'newly-indexed' }]);
        await render(list);
        await click(container.querySelector('summary'));
        await click(button(currentProject));
        expect(onSelectProject).not.toHaveBeenCalled();
        await click(container.querySelector('summary'));
        expect(button('newly-indexed')).not.toBeNull();
        expect(list).toHaveBeenCalledTimes(2);
    });

    it('closes on Escape with trigger focus, and on an outside pointer', async () => {
        await render(async () => [{ name: currentProject }]);
        await click(container.querySelector('summary'));
        await act(async () => container.querySelector('summary')!.dispatchEvent(
            new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true }),
        ));
        expect(container.querySelector('details')?.open).toBe(false);
        expect(document.activeElement).toBe(container.querySelector('summary'));
        await click(container.querySelector('summary'));
        await act(async () => document.body.dispatchEvent(new Event('pointerdown', { bubbles: true })));
        expect(container.querySelector('details')?.open).toBe(false);
        expect(onSelectProject).not.toHaveBeenCalled();
    });

    it('offers retry and adding a project when listing fails', async () => {
        const list = vi.fn().mockRejectedValueOnce(new Error('offline'))
            .mockResolvedValueOnce([{ name: 'recovered' }]);
        await render(list);
        await click(container.querySelector('summary'));
        expect(container.textContent).toContain('Could not load projects.');
        expect(button('Add project index')).not.toBeNull();
        expect(button('Manage projects')).toBeNull();
        await click(button('Try again'));
        expect(button('recovered')).not.toBeNull();
        await click(button('Add project index'));
        expect(onAddProject).toHaveBeenCalledTimes(1);
        expect(container.querySelector('details')?.open).toBe(false);
        expect(document.activeElement).toBe(container.querySelector('summary'));
    });

    it('distinguishes an empty index from a refreshed list and retains Add project', async () => {
        const list = vi.fn().mockResolvedValueOnce([]).mockResolvedValueOnce([{ name: 'indexed' }]);
        await render(list);
        await click(container.querySelector('summary'));
        expect(container.textContent).toContain('No indexed projects yet.');
        expect(button('Add project index')).not.toBeNull();
        await click(button('Refresh projects'));
        expect(button('indexed')).not.toBeNull();
        expect(button('Add project index')).not.toBeNull();
        expect(container.querySelector('input')).toBeNull();
        expect(container.textContent).not.toContain('No indexed projects yet.');
    });

    it('ignores a late response from an obsolete project source', async () => {
        let resolveOld!: (value: ProjectEntry[]) => void;
        const oldList = (): Promise<ProjectEntry[]> => new Promise(resolve => { resolveOld = resolve; });
        await render(oldList);
        await click(container.querySelector('summary'));
        expect(container.textContent).toContain('Loading projects...');
        await render(async () => [{ name: 'current-source' }]);
        expect(button('current-source')).not.toBeNull();
        await act(async () => resolveOld([{ name: 'obsolete-source' }]));
        expect(button('obsolete-source')).toBeNull();
        expect(button('current-source')).not.toBeNull();
    });

    it('reopens indexing progress without switching the active project', async () => {
        await render(async () => [{ name: currentProject }], { name: 'new-repo', status: 'indexing' });
        await click(container.querySelector('summary'));
        expect(button('Indexing…')?.textContent).toContain('new-repo');
        await click(button('Indexing…'));
        expect(onAddProject).toHaveBeenCalledOnce();
        expect(onSelectProject).not.toHaveBeenCalled();
        expect(container.querySelector('details')?.open).toBe(false);
    });
});
