// @vitest-environment jsdom
import { act, useLayoutEffect } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import FileImpactSummary, { aggregateFileImpact } from './FileImpactSummary';
import { fetchSelectionImpact } from './selection-impact';
import type { SelectionImpact, SelectionImpactFinding, SelectionImpactReply } from './selection-impact';

function finding(id: number, file: string, distance = 1, test = false): SelectionImpactFinding {
    const node = { id, name: `symbol${id}`, qualified_name: `fixture.symbol${id}`, file_path: file, line: id };
    return { ...node, distance, test_candidate: test,
        path: [{ edge_id: id, type: 'CALLS', from: node,
            to: { id: 1, name: 'core', qualified_name: 'fixture.core', file_path: 'core.ts', line: 1 } }] };
}

/** Synthetic graph and Git evidence; not findings about the current repository. */
function fixture(file = 'core.ts'): SelectionImpact {
    return {
        status: 'ready', project: 'fixture', file_path: file, scope: 'file', computed_at: 1700000000, cache_seconds: 30,
        snapshot: { indexed_at: '2026-09-19T10:00:00Z', generation: 'generation-one', index_revision: 'abc',
            freshness: 'Current snapshot', coverage_recording: 'complete', coverage: [] },
        structural: { available: true, basis: 'CALLS and IMPORTS', max_depth: 4, visit_cap: 600, result_cap: 60,
            seed_count: 1, reachable: 4, direct: 2, test_candidates: 2, truncated: false,
            findings: [finding(2, 'src/caller.ts'), finding(3, 'src/caller.ts'),
                finding(4, 'tests/core.test.ts', 2, true), finding(5, 'tests/core.test.ts', 2, true)] },
        history: { available: true, head: 'a'.repeat(40), shallow: false, worktree_status_known: true,
            selection_uncommitted: false, truncated: false, commit_cap: 200, window_days: 548,
            mass_change_threshold: 30, commits_scanned: 20, commits_considered: 15, selection_commits: 3,
            merges_excluded: 2, mass_changes_excluded: 3, read_errors: 0, cochanges_omitted: 0,
            commits: [{ hash: 'b'.repeat(40), subject: 'Synthetic fixture change', time: 1700000000, files: 2 }],
            cochanges: [{ file_path: 'peer.ts', shared_commits: 3, commit_refs: ['b'.repeat(40)] }] },
    };
}

let container: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container);
    root = createRoot(container);
});
afterEach(async () => {
    await act(async () => root.unmount()); container.remove(); vi.useRealTimers();
});

async function click(text: string): Promise<void> {
    const button = [...container.querySelectorAll('button')].find(node => node.textContent?.trim().startsWith(text));
    expect(button, `Expected a ${text} button`).toBeDefined();
    await act(async () => button!.click());
}

describe('compact file impact aggregation', () => {
    it('deduplicates files, tests and symbols and picks a navigable source representative', () => {
        const data = fixture();
        data.structural.findings.push(finding(2, 'src/caller.ts'));
        data.history.cochanges.push(data.history.cochanges[0]!);
        const aggregate = aggregateFileImpact(data);
        expect(aggregate.files).toHaveLength(2);
        expect(aggregate.testFiles).toBe(1);
        expect(aggregate.files[0]).toMatchObject({ filePath: 'src/caller.ts', symbols: 2, distance: 1,
            target: { filePath: 'src/caller.ts', id: 2, line: 2 } });
        expect(aggregate.cochanges).toHaveLength(1);
    });

    it('detects a capped evidence list even when its explicit truncated flag is absent', () => {
        const data = fixture();
        data.structural.reachable = 100;
        data.structural.truncated = false;
        expect(aggregateFileImpact(data).filesIncomplete).toBe(true);
    });
});

describe('FileImpactSummary', () => {
    it('does not fetch without a selected file', async () => {
        const load = vi.fn<typeof fetchSelectionImpact>();
        await act(async () => root.render(<FileImpactSummary project="fixture" load={load} onOpen={vi.fn()} />));
        expect(load).not.toHaveBeenCalled();
        expect(container.textContent).toContain('Select a file');
    });

    it('shows a compact file summary, with grouped source links only on demand', async () => {
        const load = vi.fn<typeof fetchSelectionImpact>(async () => fixture()), onOpen = vi.fn();
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'core.ts' }} load={load} onOpen={onOpen} />));
        expect(container.textContent).toContain('2 direct dependents');
        expect(container.textContent).toContain('2 affected files');
        expect(container.textContent).toContain('1 test file');
        expect(container.textContent).toContain('3 sampled commits');
        expect(container.textContent).toContain('1 co-changed file');
        expect(container.querySelector('.file-impact-summary-details')).toBeNull();
        expect(container.textContent).not.toContain('src/caller.ts');
        await click('Details');
        expect(container.querySelectorAll('[aria-label="Affected files"] li')).toHaveLength(2);
        await click('src/caller.ts');
        expect(onOpen).toHaveBeenCalledWith(expect.objectContaining({ filePath: 'src/caller.ts', id: 2, line: 2 }));
        await click('peer.ts');
        expect(onOpen).toHaveBeenCalledWith({ filePath: 'peer.ts' });
        expect(container.textContent).not.toContain('Graph edge #');
        await click('Less');
        expect(container.querySelector('.file-impact-summary-details')).toBeNull();
    });

    it('labels returned file counts as lower bounds and exposes generation differences concisely', async () => {
        const data = fixture();
        data.structural.reachable = 100;
        data.history.cochanges_omitted = 7;
        const load = vi.fn<typeof fetchSelectionImpact>(async () => data);
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'core.ts' }}
            expectedGeneration="different" load={load} onOpen={vi.fn()} />));
        expect(container.textContent).toContain('at least 2 affected files');
        expect(container.textContent).toContain('at least 1 test file');
        expect(container.textContent).toContain('at least 1 co-changed file');
        expect(container.textContent).toContain('Index snapshot differs');
        expect(container.textContent).toContain('Graph sample limited');
        expect(container.textContent).not.toContain('100 affected files');
    });

    it('does not turn unavailable evidence into zero counts', async () => {
        const data = fixture();
        data.structural.available = false;
        data.history.available = false;
        const load = vi.fn<typeof fetchSelectionImpact>(async () => data);
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'core.ts' }} load={load} onOpen={vi.fn()} />));
        expect(container.textContent).toContain('Graph unavailable');
        expect(container.textContent).toContain('Git unavailable');
        expect(container.textContent).not.toContain('affected files');
        expect(container.textContent).not.toContain('sampled commits');
    });

    it('qualifies a real zero result instead of presenting it as safety', async () => {
        const data = fixture();
        data.structural = { ...data.structural, direct: 0, reachable: 0, test_candidates: 0, findings: [] };
        const load = vi.fn<typeof fetchSelectionImpact>(async () => data);
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'core.ts' }} load={load} onOpen={vi.fn()} />));
        expect(container.textContent).toContain('0 affected files');
        expect(container.textContent).toContain('does not establish safety');
    });

    it('polls pending and busy work serially until the result is ready', async () => {
        vi.useFakeTimers();
        const load = vi.fn<typeof fetchSelectionImpact>().mockResolvedValueOnce({ status: 'pending' })
            .mockResolvedValueOnce({ status: 'busy' }).mockResolvedValueOnce(fixture());
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'core.ts' }} load={load} onOpen={vi.fn()} />));
        expect(load).toHaveBeenCalledTimes(1);
        await act(async () => vi.advanceTimersByTimeAsync(500));
        expect(container.textContent).toContain('Waiting for local analysis');
        await act(async () => vi.advanceTimersByTimeAsync(500));
        expect(load).toHaveBeenCalledTimes(3);
        expect(container.textContent).toContain('2 affected files');
        await act(async () => vi.advanceTimersByTimeAsync(2000));
        expect(load).toHaveBeenCalledTimes(3);
    });

    it('aborts old selections and never accepts their late responses', async () => {
        let resolveOld: (reply: SelectionImpactReply) => void = () => {};
        const old = new Promise<SelectionImpactReply>(resolve => { resolveOld = resolve; });
        const load = vi.fn<typeof fetchSelectionImpact>().mockReturnValueOnce(old).mockResolvedValueOnce(fixture('new.ts'));
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'old.ts' }} load={load} onOpen={vi.fn()} />));
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'new.ts' }} load={load} onOpen={vi.fn()} />));
        expect(load.mock.calls[0]?.[2]?.aborted).toBe(true);
        const obsolete = fixture('old.ts'); obsolete.structural.direct = 999;
        await act(async () => resolveOld(obsolete));
        expect(container.textContent).toContain('2 direct dependents');
        expect(container.textContent).not.toContain('999');
    });

    it('clears old metrics before effects when selecting another symbol in the same file', async () => {
        const captures: string[] = [];
        function Capture(): null {
            useLayoutEffect(() => { captures.push(container.textContent ?? ''); });
            return null;
        }
        const load = vi.fn<typeof fetchSelectionImpact>().mockResolvedValueOnce(fixture())
            .mockImplementationOnce(() => new Promise<SelectionImpactReply>(() => {}));
        await act(async () => root.render(<><FileImpactSummary project="fixture" target={{ filePath: 'core.ts', qualifiedName: 'first', name: 'First' }} load={load} onOpen={vi.fn()} /><Capture /></>));
        captures.length = 0;
        await act(async () => root.render(<><FileImpactSummary project="fixture" target={{ filePath: 'core.ts', qualifiedName: 'second', name: 'Second' }} load={load} onOpen={vi.fn()} /><Capture /></>));
        expect(captures[0]).toContain('Second impact');
        expect(captures[0]).not.toContain('affected files');
        expect(load.mock.calls[1]?.[1].qualifiedName).toBe('second');
    });

    it('offers explicit retry after failure without forcing refresh on subsequent selections', async () => {
        const load = vi.fn<typeof fetchSelectionImpact>().mockRejectedValueOnce(new Error('Disconnected'))
            .mockResolvedValueOnce(fixture()).mockResolvedValueOnce(fixture('next.ts'));
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'core.ts' }} load={load} onOpen={vi.fn()} />));
        expect(container.querySelector('[role="alert"]')?.textContent).toBe('Impact unavailable');
        expect(container.textContent).not.toContain('affected files');
        await click('Retry');
        expect(load.mock.calls[1]?.[3]).toBe(true);
        expect(container.textContent).toContain('2 affected files');
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'next.ts' }} load={load} onOpen={vi.fn()} />));
        expect(load.mock.calls[2]?.[3]).toBe(false);
    });

    it('rejects a response identifying a different project or file', async () => {
        const load = vi.fn<typeof fetchSelectionImpact>(async () => fixture('other.ts'));
        await act(async () => root.render(<FileImpactSummary project="fixture" target={{ filePath: 'core.ts' }} load={load} onOpen={vi.fn()} />));
        expect(container.textContent).toContain('Impact unavailable');
        expect(container.textContent).not.toContain('affected files');
    });
});
