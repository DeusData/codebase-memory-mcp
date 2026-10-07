// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest';
import AddProjectIndexDialog, { type AddProjectIndexDialogProps } from './AddProjectIndexDialog';
import type { ProjectEntry } from '../provider/rpc-schemas';
import type { BrowseLevel, IndexJob, IndexStarted, ProjectHealth } from './projects-model';

const home = '/home/user';
const repository = `${home}/repo-one`;
const pollMs = 25;
let container: HTMLDivElement;
let root: Root;
let mounted: boolean;
let projects: ProjectEntry[];
let jobs: IndexJob[];
let source: ReturnType<typeof makeSource>;
let onClose: Mock<AddProjectIndexDialogProps['onClose']>;
let onOpenProject: Mock<AddProjectIndexDialogProps['onOpenProject']>;
let onActivityChange: Mock<NonNullable<AddProjectIndexDialogProps['onActivityChange']>>;

function level(path: string, dirs: string[] = []): BrowseLevel {
    return { path, dirs, parent: path.slice(0, path.lastIndexOf('/')) || '/', roots: [] };
}
function makeSource() {
    return {
        browse: vi.fn(async (path: string): Promise<BrowseLevel> => level(path || home, !path || path === home ? ['repo-z', 'repo-one'] : [])),
        startIndex: vi.fn(async (path: string, _name: string): Promise<IndexStarted> => {
            jobs = [{ slot: 4, status: 'indexing', path, error: '' }];
            return { slot: 4, path };
        }),
        indexJobs: vi.fn(async (): Promise<IndexJob[]> => jobs),
        listProjects: vi.fn(async (): Promise<ProjectEntry[]> => projects),
        projectHealth: vi.fn(async (_name: string): Promise<ProjectHealth> => ({ status: 'healthy', reason: '' })),
    };
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    vi.useFakeTimers();
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
    mounted = true;
    projects = [];
    jobs = [];
    source = makeSource();
    onClose = vi.fn<AddProjectIndexDialogProps['onClose']>();
    onOpenProject = vi.fn<AddProjectIndexDialogProps['onOpenProject']>();
    onActivityChange = vi.fn<NonNullable<AddProjectIndexDialogProps['onActivityChange']>>();
});
afterEach(async () => {
    if (mounted) await act(async () => root.unmount());
    container.remove();
    vi.useRealTimers();
    vi.restoreAllMocks();
});
async function render(open = true, overrides: Partial<AddProjectIndexDialogProps> = {}): Promise<void> {
    await act(async () => root.render(<AddProjectIndexDialog open={open} source={source} onClose={onClose}
        onOpenProject={onOpenProject} onActivityChange={onActivityChange} pollMs={pollMs} {...overrides} />));
}
function button(label: string): HTMLButtonElement | undefined {
    return [...container.querySelectorAll('button')].find(node => node.textContent?.trim() === label || node.getAttribute('aria-label') === label);
}
async function click(label: string): Promise<void> {
    const node = button(label);
    expect(node, `button ${label}`).toBeDefined();
    await act(async () => node!.click());
}
async function type(id: string, value: string): Promise<void> {
    const input = container.querySelector<HTMLInputElement>(`#${id}`)!;
    expect(input).not.toBeNull();
    await act(async () => {
        Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, value);
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
}
function inputValue(id: string): string | undefined { return container.querySelector<HTMLInputElement>(`#${id}`)?.value; }
async function tick(): Promise<void> { await act(async () => vi.advanceTimersByTimeAsync(pollMs)); }
async function chooseRepository(): Promise<void> {
    await render(); await click('Open folder repo-one'); await click('Use this folder');
}
async function startRepository(): Promise<void> { await chooseRepository(); await click('Start indexing'); }
function finishJob(): void {
    jobs = [{ slot: 4, status: 'done', path: repository, error: '' }];
    projects = [{ name: 'repo-one', root_path: repository }];
}

describe('AddProjectIndexDialog', () => {
    it('does no work while initially hidden, then browses without starting or polling an index', async () => {
        await render(false);
        expect(source.browse).not.toHaveBeenCalled();
        expect(source.listProjects).not.toHaveBeenCalled();
        await render();
        expect(source.browse).toHaveBeenCalledExactlyOnceWith('');
        expect(source.listProjects).toHaveBeenCalledTimes(1);
        expect(inputValue('add-index-folder')).toBe(home);
        expect(document.activeElement).toBe(container.querySelector('input'));
        await act(async () => vi.advanceTimersByTimeAsync(1000));
        expect(source.startIndex).not.toHaveBeenCalled();
        expect(source.indexJobs).not.toHaveBeenCalled();
        expect(source.projectHealth).not.toHaveBeenCalled();
        expect(source.listProjects).toHaveBeenCalledTimes(1);
    });

    it('sorts folders, navigates up and by path, and waits for explicit Start indexing', async () => {
        await render();
        expect([...container.querySelectorAll('.add-index-folders button')].map(node => node.getAttribute('aria-label')))
            .toEqual(['Open folder repo-one', 'Open folder repo-z']);
        await click('Open folder repo-one');
        expect(inputValue('add-index-folder')).toBe(repository);
        await click('↑ Up');
        expect(source.browse).toHaveBeenLastCalledWith(home);
        await type('add-index-folder', '/repos/My Repository');
        expect(button('Use this folder')?.disabled).toBe(true);
        await click('Go'); await click('Use this folder');
        expect(inputValue('add-index-name')).toBe('My-Repository');
        expect(document.activeElement).toBe(container.querySelector('#add-index-name'));
        expect(source.startIndex).not.toHaveBeenCalled();
        await click('Start indexing');
        expect(source.startIndex).toHaveBeenCalledExactlyOnceWith('/repos/My Repository', 'My-Repository');
        expect(document.activeElement).toBe(container.querySelector('[role="dialog"]'));
        expect(container.querySelector('progress')?.hasAttribute('value')).toBe(false);
    });

    it('ignores a stale browse response after a newer path has loaded', async () => {
        const old = deferred<BrowseLevel>();
        source.browse.mockImplementationOnce(() => old.promise);
        await render();
        await type('add-index-folder', '/new'); await click('Go');
        expect(inputValue('add-index-folder')).toBe('/new');
        await act(async () => old.resolve(level('/old', ['obsolete'])));
        expect(inputValue('add-index-folder')).toBe('/new');
        expect(button('Open folder obsolete')).toBeUndefined();
        expect(button('Use this folder')?.disabled).toBe(false);
    });

    it('preserves a next path typed while an earlier browse is pending', async () => {
        await render();
        const waiting = deferred<BrowseLevel>();
        source.browse.mockImplementationOnce(() => waiting.promise);
        await type('add-index-folder', '/first'); await click('Go');
        await type('add-index-folder', '/next');
        await act(async () => waiting.resolve(level('/first', ['child'])));
        expect(inputValue('add-index-folder')).toBe('/next');
        expect(button('Use this folder')?.disabled).toBe(true);
        await click('Go'); await click('Use this folder');
        expect(inputValue('add-index-name')).toBe('next');
        expect(source.startIndex).not.toHaveBeenCalled();
    });

    it('supports Windows roots and separator normalization', async () => {
        source.browse.mockImplementation(async path => ({ path: (path || 'C:/Users').replace(/\\/g, '/'), dirs: [], parent: 'C:/', roots: ['C:/', 'D:/'] }));
        await render(); await click('D:/');
        expect(source.browse).toHaveBeenLastCalledWith('D:/');
        await type('add-index-folder', 'D:\\repos\\sample'); await click('Go');
        expect(inputValue('add-index-folder')).toBe('D:/repos/sample');
        await click('Use this folder'); await click('Start indexing');
        expect(source.startIndex).toHaveBeenCalledExactlyOnceWith('D:/repos/sample', 'sample');
    });

    it('shows browse errors without allowing the previous folder to be submitted', async () => {
        await render();
        source.browse.mockRejectedValueOnce(new Error('no access'));
        await type('add-index-folder', '/missing'); await click('Go');
        expect(container.textContent).toContain('Could not open this folder.');
        expect(button('Use this folder')?.disabled).toBe(true);
        expect(source.startIndex).not.toHaveBeenCalled();
        await click('Go');
        expect(button('Use this folder')?.disabled).toBe(false);
    });

    it('deduplicates simultaneous submissions and locks the form until the request returns', async () => {
        await chooseRepository();
        const pending = deferred<IndexStarted>();
        source.startIndex.mockImplementationOnce(() => pending.promise);
        const form = container.querySelector('.add-index-confirm')!;
        await act(async () => {
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
            form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
        });
        expect(source.startIndex).toHaveBeenCalledTimes(1);
        expect(container.querySelector<HTMLInputElement>('#add-index-name')?.disabled).toBe(true);
        expect(button('Starting…')?.disabled).toBe(true);
        jobs = [{ slot: 4, status: 'indexing', path: repository, error: '' }];
        await act(async () => pending.resolve({ slot: 4, path: repository }));
        expect(container.textContent).toContain('Indexing repository');
        expect(source.startIndex).toHaveBeenCalledTimes(1);
    });

    it('offers the existing project for an already indexed folder', async () => {
        projects = [{ name: 'already-here', root_path: `${repository}/` }];
        await chooseRepository();
        expect(container.textContent).toContain('This folder is already indexed as already-here.');
        expect(button('Start indexing')?.disabled).toBe(true);
        await click('Open project');
        expect(onOpenProject).toHaveBeenCalledExactlyOnceWith('already-here');
        expect(onClose).toHaveBeenCalledOnce();
        expect(source.startIndex).not.toHaveBeenCalled();
    });

    it('rejects name collisions and rechecks before writing to catch newly created projects', async () => {
        projects = [{ name: 'repo-one', root_path: '/other' }];
        await chooseRepository();
        expect(container.textContent).toContain('already in use');
        expect(button('Start indexing')?.disabled).toBe(true);
        await type('add-index-name', 'unique');
        expect(button('Start indexing')?.disabled).toBe(false);
        projects = [{ name: 'unique', root_path: '/different' }];
        await click('Start indexing');
        expect(container.textContent).toContain('The name unique is already in use.');
        expect(source.startIndex).not.toHaveBeenCalled();
    });

    it('requires a portable project name and a successful existing-project check', async () => {
        source.listProjects.mockRejectedValueOnce(new Error('offline'));
        await chooseRepository();
        expect(container.textContent).toContain('Could not check existing projects.');
        expect(button('Start indexing')?.disabled).toBe(true);
        await click('Retry project check');
        for (const name of ['with spaces', '..hidden', 'two--dashes', 'a'.repeat(201)]) {
            await type('add-index-name', name);
            expect(button('Start indexing')?.disabled).toBe(true);
        }
        await type('add-index-name', 'service_2.core');
        expect(button('Start indexing')?.disabled).toBe(false);
        expect(source.startIndex).not.toHaveBeenCalled();
    });

    it('keeps status polling serial even when a response is delayed', async () => {
        const pending = deferred<IndexJob[]>();
        source.indexJobs.mockImplementationOnce(() => pending.promise);
        await startRepository();
        expect(source.indexJobs).toHaveBeenCalledTimes(1);
        await act(async () => vi.advanceTimersByTimeAsync(1000));
        expect(source.indexJobs).toHaveBeenCalledTimes(1);
        await act(async () => pending.resolve(jobs));
        await tick();
        expect(source.indexJobs).toHaveBeenCalledTimes(2);
        expect(source.startIndex).toHaveBeenCalledTimes(1);
    });

    it('continues tracking while closed and reopens the existing job without another write', async () => {
        await startRepository(); await click('Close for now');
        expect(onClose).toHaveBeenCalledOnce();
        await render(false);
        expect(container.querySelector('[role="dialog"]')).toBeNull();
        await tick();
        expect(source.indexJobs).toHaveBeenCalledTimes(2);
        await render();
        expect(container.textContent).toContain('Indexing repository');
        expect(source.startIndex).toHaveBeenCalledTimes(1);
        finishJob(); await tick();
        expect(container.textContent).toContain('Ready to explore');
        expect(onActivityChange).toHaveBeenLastCalledWith({ name: 'repo-one', status: 'done' });
        expect(source.projectHealth).toHaveBeenCalledExactlyOnceWith('repo-one');
        const calls = source.indexJobs.mock.calls.length;
        await tick();
        expect(source.indexJobs).toHaveBeenCalledTimes(calls);
        await click('Open project');
        expect(onOpenProject).toHaveBeenCalledExactlyOnceWith('repo-one');
    });

    it.each(['missing', 'reused', 'unknown'] as const)('never treats a %s job slot as completion', async mode => {
        await startRepository();
        jobs = mode === 'missing' ? [] : [{ slot: 4, status: mode === 'unknown' ? 'unknown' : 'done', path: mode === 'reused' ? '/other-repo' : repository, error: '' }];
        projects = [{ name: 'repo-one', root_path: repository }];
        await tick();
        expect(container.textContent).toContain('Tracking interrupted');
        expect(container.textContent).toContain('Completion is unconfirmed');
        expect(button('Open project')).toBeUndefined();
        expect(source.projectHealth).not.toHaveBeenCalled();
        expect(onActivityChange).toHaveBeenLastCalledWith({ name: 'repo-one', status: 'error' });
    });

    it('cannot recover an invalid acknowledgment by matching an invalid status slot', async () => {
        source.startIndex.mockResolvedValueOnce({ slot: -1, path: repository });
        jobs = [{ slot: -1, status: 'done', path: repository, error: '' }];
        await startRepository();
        expect(container.textContent).toContain('Completion is unconfirmed');
        expect(button('Retry status')).toBeUndefined();
        expect(source.indexJobs).not.toHaveBeenCalled();
        expect(source.projectHealth).not.toHaveBeenCalled();
    });

    it.each(['wrong-folder', 'corrupt'] as const)('requires correct project readback before completion: %s', async mode => {
        await startRepository(); finishJob();
        if (mode === 'wrong-folder') projects = [{ name: 'repo-one', root_path: '/different' }];
        else source.projectHealth.mockResolvedValue({ status: 'corrupt', reason: 'invalid database' });
        await tick();
        expect(container.textContent).toContain('Indexing failed');
        expect(button('Open project')).toBeUndefined();
        expect(onActivityChange).toHaveBeenLastCalledWith({ name: 'repo-one', status: 'error' });
    });

    it('allows verification retry when the done job has not produced a readable project yet', async () => {
        await startRepository();
        jobs = [{ slot: 4, status: 'done', path: repository, error: '' }];
        await tick();
        expect(container.textContent).toContain('could not be verified yet');
        expect(button('Open project')).toBeUndefined();
        finishJob(); await click('Retry status');
        expect(container.textContent).toContain('Ready to explore');
        expect(source.startIndex).toHaveBeenCalledTimes(1);
    });

    it('distinguishes failed indexing from status outages and allows another project', async () => {
        await startRepository();
        jobs = [{ slot: 4, status: 'error', path: repository, error: 'Permission denied while reading source' }];
        await tick();
        expect(container.textContent).toContain('Indexing failed');
        expect(container.textContent).toContain('Permission denied while reading source');
        expect(button('Retry status')).toBeUndefined();
        expect(button('Open project')).toBeUndefined();
        await click('Add another project');
        expect(container.querySelector('#add-index-folder')).not.toBeNull();
        expect(onActivityChange).toHaveBeenLastCalledWith(undefined);
        expect(source.startIndex).toHaveBeenCalledTimes(1);
    });

    it('preserves the tracked job through an outage and retries only its status read', async () => {
        await startRepository();
        source.indexJobs.mockRejectedValueOnce(new Error('offline'));
        await tick();
        expect(container.textContent).toContain('Status unavailable');
        expect(container.textContent).toContain('may still be running');
        expect(onActivityChange).toHaveBeenLastCalledWith({ name: 'repo-one', status: 'error' });
        expect(button('Open project')).toBeUndefined();
        const calls = source.indexJobs.mock.calls.length;
        await tick();
        expect(source.indexJobs).toHaveBeenCalledTimes(calls);
        await click('Retry status');
        expect(container.textContent).toContain('Indexing repository');
        expect(onActivityChange).toHaveBeenLastCalledWith({ name: 'repo-one', status: 'indexing' });
        expect(source.startIndex).toHaveBeenCalledTimes(1);
        await tick();
        expect(source.indexJobs).toHaveBeenCalledTimes(calls + 2);
    });

    it('keeps an unconfirmed write explicit and never retries it automatically', async () => {
        source.startIndex.mockRejectedValueOnce(new Error('connection lost'));
        await startRepository();
        expect(container.textContent).toContain('The index request was not confirmed.');
        expect(inputValue('add-index-name')).toBe('repo-one');
        await tick();
        expect(source.startIndex).toHaveBeenCalledTimes(1);
        expect(source.indexJobs).not.toHaveBeenCalled();
    });

    it('ignores status responses arriving after unmount', async () => {
        const pending = deferred<IndexJob[]>();
        source.indexJobs.mockImplementationOnce(() => pending.promise);
        await startRepository();
        await act(async () => root.unmount()); mounted = false;
        const calls = onActivityChange.mock.calls.length;
        finishJob(); await act(async () => pending.resolve(jobs));
        expect(onActivityChange).toHaveBeenCalledTimes(calls);
        expect(source.projectHealth).not.toHaveBeenCalled();
        await tick();
        expect(source.indexJobs).toHaveBeenCalledTimes(1);
    });

    it('traps keyboard focus, closes with Escape, restores the opener, and retains the form', async () => {
        const opener = document.createElement('button');
        document.body.appendChild(opener); opener.focus();
        try {
            await chooseRepository();
            await type('add-index-name', 'renamed');
            const first = button('Close add project index')!;
            const last = button('Start indexing')!;
            first.focus();
            await act(async () => first.dispatchEvent(new KeyboardEvent('keydown', { key: 'Tab', shiftKey: true, bubbles: true, cancelable: true })));
            expect(document.activeElement).toBe(last);
            await act(async () => last.dispatchEvent(new KeyboardEvent('keydown', { key: 'Tab', bubbles: true, cancelable: true })));
            expect(document.activeElement).toBe(first);
            await act(async () => first.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true })));
            expect(onClose).toHaveBeenCalledOnce();
            await render(false);
            expect(document.activeElement).toBe(opener);
            await render();
            expect(inputValue('add-index-name')).toBe('renamed');
            expect(document.activeElement).toBe(container.querySelector('#add-index-name'));
            expect(source.startIndex).not.toHaveBeenCalled();
        } finally { opener.remove(); }
    });
});
