// @vitest-environment jsdom
/*
 * Review of K42 ("other Refresh buttons the same way"): Refresh in the file
 * impact details of Explore collapsed the details into "Reading
 * dependencies…" for about two seconds and then showed the same numbers
 * (measured in the browser), with nothing left to say that it had run. The
 * details now stay on screen, the button says that it runs, and a status
 * beside it says when it ran and whether anything changed.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import FileImpactSummary from './FileImpactSummary';
import type { fetchSelectionImpact, SelectionImpact, SelectionImpactReply } from './selection-impact';

function impact(direct: number, computed_at: number): SelectionImpact {
    const node = (id: number, file: string) => ({ id, name: `symbol${id}`, qualified_name: `fixture.symbol${id}`, file_path: file, line: id });
    return {
        status: 'ready', project: 'fixture', file_path: 'core.ts', scope: 'file', computed_at, cache_seconds: 30,
        snapshot: { indexed_at: '2026-09-19T10:00:00Z', generation: 'generation-one', index_revision: 'abc', freshness: 'Current snapshot', coverage_recording: 'complete', coverage: [] },
        structural: { available: true, basis: 'CALLS and IMPORTS', max_depth: 4, visit_cap: 600, result_cap: 60, seed_count: 1, reachable: direct, direct, test_candidates: 0, truncated: false,
            findings: Array.from({ length: direct }, (_, at) => ({ ...node(at + 2, `src/caller${at}.ts`), distance: 1, test_candidate: false,
                path: [{ edge_id: at + 2, type: 'CALLS', from: node(at + 2, `src/caller${at}.ts`), to: node(1, 'core.ts') }] })) },
        history: { available: true, head: 'a'.repeat(40), shallow: false, worktree_status_known: true, selection_uncommitted: false, truncated: false, commit_cap: 200, window_days: 548,
            mass_change_threshold: 30, commits_scanned: 20, commits_considered: 15, selection_commits: 3, merges_excluded: 0, mass_changes_excluded: 0, read_errors: 0, cochanges_omitted: 0,
            commits: [], cochanges: [] },
    };
}

let container: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); });

function deferred<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((yes) => { resolve = yes; });
    return { promise, resolve };
}
const refresh = () => container.querySelector<HTMLButtonElement>('.file-impact-summary-details footer .atlas-refresh button');
const status = () => container.querySelector('.file-impact-summary-details footer .atlas-refresh-status')?.textContent;
const TIME = '\\d\\d:\\d\\d:\\d\\d';

it('K42: Refresh in the file impact keeps the details, says that it runs, then when it ran and whether anything changed', async () => {
    let next = deferred<SelectionImpactReply>();
    const load = vi.fn<typeof fetchSelectionImpact>().mockResolvedValueOnce(impact(2, 1)).mockImplementation(() => next.promise);
    await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'core.ts' }} load={load} onOpen={vi.fn()} />));
    await act(async () => container.querySelector<HTMLButtonElement>('.file-impact-summary-toggle')!.click());
    expect(refresh()?.textContent).toBe('Refresh');
    expect(status()).toBe('');

    await act(async () => refresh()!.click());
    // While it runs the details stay, and the button says so.
    expect(container.textContent).toContain('2 affected files');
    expect(refresh()?.textContent).toBe('Refreshing…');
    expect(refresh()?.getAttribute('aria-disabled')).toBe('true');
    // A new computation of the same evidence is no change, although its time stamp is another.
    await act(async () => next.resolve(impact(2, 99)));
    expect(refresh()?.textContent).toBe('Refresh');
    expect(status()).toMatch(new RegExp(`^Up to date at ${TIME}: no changes since the last load$`));

    next = deferred<SelectionImpactReply>();
    await act(async () => refresh()!.click());
    await act(async () => next.resolve(impact(3, 120)));
    expect(container.textContent).toContain('3 affected files');
    expect(status()).toMatch(new RegExp(`^Impact refreshed at ${TIME}$`));
});

it('K42: another file never shows the evidence of the previous one while it loads', async () => {
    const next = deferred<SelectionImpactReply>();
    const load = vi.fn<typeof fetchSelectionImpact>().mockResolvedValueOnce(impact(2, 1)).mockImplementation(() => next.promise);
    await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'core.ts' }} load={load} onOpen={vi.fn()} />));
    expect(container.textContent).toContain('2 affected files');
    await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'other.ts' }} load={load} onOpen={vi.fn()} />));
    expect(container.textContent).not.toContain('affected files');
    expect(container.textContent).toContain('Reading dependencies…');
});
