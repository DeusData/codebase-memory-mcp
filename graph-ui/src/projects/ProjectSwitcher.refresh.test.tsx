// @vitest-environment jsdom
/*
 * Review of K42 ("other Refresh buttons the same way"): "Refresh projects"
 * in the project picker read the list again, showed "Loading projects..."
 * for about 150 ms and then the same list (measured in the browser). It now
 * says that it runs and then when it ran and whether the list changed.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import ProjectSwitcher from './ProjectSwitcher';
import type { ProjectEntry } from '../provider/rpc-schemas';

let container: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); });

function deferred<T>() {
    let resolve!: (value: T) => void, reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const refresh = () => container.querySelector<HTMLButtonElement>('.atlas-project-picker footer .atlas-refresh button')!;
const status = () => container.querySelector('.atlas-project-picker footer .atlas-refresh-status')?.textContent;
const TIME = '\\d\\d:\\d\\d:\\d\\d';
const alpha: ProjectEntry = { name: 'alpha', root_path: '/repos/alpha' };

it('K42: Refresh projects says that it runs, then when it ran and whether the list changed, or that it failed', async () => {
    let next = deferred<readonly ProjectEntry[]>();
    const list = vi.fn().mockResolvedValueOnce([alpha]).mockImplementation(() => next.promise);
    await act(async () => root.render(<ProjectSwitcher currentProject="alpha" listProjects={list} onSelectProject={vi.fn()} onAddProject={vi.fn()} />));
    await act(async () => container.querySelector('summary')!.click());
    expect(refresh().textContent).toBe('Refresh projects');
    expect(status()).toBe('');

    await act(async () => refresh().click());
    expect(refresh().textContent).toBe('Refreshing projects…');
    expect(refresh().getAttribute('aria-disabled')).toBe('true');
    await act(async () => next.resolve([alpha]));
    expect(refresh().textContent).toBe('Refresh projects');
    expect(status()).toMatch(new RegExp(`^Up to date at ${TIME}: no changes since the last load$`));

    next = deferred<readonly ProjectEntry[]>();
    await act(async () => refresh().click());
    await act(async () => next.resolve([alpha, { name: 'beta', root_path: '/repos/beta' }]));
    expect(status()).toMatch(new RegExp(`^Projects refreshed at ${TIME}$`));
    expect(container.querySelectorAll('.atlas-project-results li')).toHaveLength(2);

    next = deferred<readonly ProjectEntry[]>();
    await act(async () => refresh().click());
    await act(async () => next.reject(new Error('offline')));
    // The list area says what failed and offers Try again; the status beside the button says when.
    expect(status()).toMatch(new RegExp(`^Refresh failed at ${TIME}$`));
    expect(container.querySelector('.atlas-project-results')?.textContent).toContain('Could not load projects.');

    // Reopening the picker reads the list again by itself: that is no press of Refresh, and the status goes.
    next = deferred<readonly ProjectEntry[]>();
    await act(async () => container.querySelector('summary')!.click());
    await act(async () => container.querySelector('summary')!.click());
    await act(async () => next.resolve([alpha]));
    expect(status()).toBe('');
});
