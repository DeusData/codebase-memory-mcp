// @vitest-environment jsdom
import { act, useLayoutEffect } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import SelectionImpactWidget from './SelectionImpactWidget';
import { fetchSelectionImpact, localCommitCommand, readSelectionImpact, selectionRisk } from './selection-impact';
import type { SelectionImpact, SelectionImpactReply } from './selection-impact';

/** Explicit test data; no claim about the production repository. */
function fixture(file = 'core.ts'): SelectionImpact {
    const origin = { id: 1, name: 'core', qualified_name: 'fixture.core', file_path: file, line: 3 };
    const caller = { id: 2, name: 'caller', qualified_name: 'fixture.caller', file_path: 'caller.ts', line: 8 };
    return {
        status: 'ready', project: 'fixture', file_path: file, scope: 'file', computed_at: 1700000000, cache_seconds: 30,
        snapshot: { indexed_at: '2026-09-09T10:00:00Z', generation: 'legacy', index_revision: 'unknown',
            freshness: 'Index equivalence to HEAD is unverified.', coverage_recording: 'unavailable', coverage: [] },
        structural: { available: true, basis: 'CALLS and IMPORTS', max_depth: 4, visit_cap: 600, result_cap: 60,
            seed_count: 1, reachable: 1, direct: 1, test_candidates: 0, truncated: false,
            findings: [{ ...caller, distance: 1, test_candidate: false,
                path: [{ edge_id: 7, type: 'CALLS', from: caller, to: origin }] }] },
        history: { available: true, head: 'a'.repeat(40), shallow: false, worktree_status_known: true,
            selection_uncommitted: true, truncated: false, commit_cap: 200, window_days: 548,
            mass_change_threshold: 30, commits_scanned: 5, commits_considered: 3, selection_commits: 2,
            merges_excluded: 1, mass_changes_excluded: 1, read_errors: 0, cochanges_omitted: 0,
            commits: [{ hash: 'b'.repeat(40), subject: 'Explicit fixture commit', time: 1700000000, files: 2 }],
            cochanges: [{ file_path: 'peer.ts', shared_commits: 2, commit_refs: ['b'.repeat(40)] }] },
    };
}

describe('selection impact evidence', () => {
    it('does not turn missing graph, coverage and history into low risk', () => {
        const data = fixture();
        data.structural = { ...data.structural, available: false, reachable: 0, direct: 0, findings: [] };
        data.history = { ...data.history, available: false, cochanges: [], selection_commits: 0, shallow: true };
        const risk = selectionRisk(data);
        expect(risk.level).toBe('unresolved');
        expect(risk.uncertainties.join(' ')).toContain('shallow');
        expect(risk.uncertainties.join(' ')).toContain('uncommitted');
        expect(risk.uncertainties.join(' ')).toContain('coverage recording');
        expect(risk.reasons.join(' ')).toContain('does not establish low risk');
    });
    it('keeps a high structural signal separate from uncertainty and historical association', () => {
        const data = fixture();
        data.structural.direct = 10;
        data.structural.reachable = 12;
        data.history.cochanges[0]!.shared_commits = 3;
        const risk = selectionRisk(data);
        expect(risk.level).toBe('high');
        expect(risk.reasons[0]).toContain('10 direct or 25 total');
        expect(risk.reasons.join(' ')).toContain('historical associations separately');
        expect(risk.uncertainties).toContain('Index equivalence to HEAD is unverified.');
    });
    it('rejects incomplete or disconnected graph evidence instead of inventing a count', () => {
        const data = fixture();
        expect(readSelectionImpact(data)).toEqual(data);
        data.structural.findings[0]!.path[0]!.from.id = 999;
        expect(() => readSelectionImpact(data)).toThrow('Disconnected');
        expect(() => readSelectionImpact({ status: 'ready' })).toThrow();
    });
    it('validates same-origin response identity and sends the selected file and qualified name', async () => {
        const request = vi.fn<typeof fetch>(async () => new Response(JSON.stringify(fixture())));
        const response = await fetchSelectionImpact('fixture', { filePath: 'core.ts', qualifiedName: 'fixture.core' }, undefined, false, request);
        expect(response.status).toBe('ready');
        expect(request.mock.calls[0]?.[0]).toBe('/api/impact-analysis?project=fixture&file=core.ts&node=fixture.core');
        await expect(fetchSelectionImpact('another', { filePath: 'core.ts' }, undefined, false, request))
            .rejects.toThrow('different selection');
    });
    it('quotes displayed Git commands without enabling shell substitutions in file names', () => {
        expect(localCommitCommand('a'.repeat(40), '$(touch unexpected).ts', "it's.ts"))
            .toBe(`git show ${'a'.repeat(40)} -- '$(touch unexpected).ts' 'it'\\''s.ts'`);
        expect(localCommitCommand('--exec=bad', 'core.ts')).toBe('Commit hash unavailable');
    });
});

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

describe('SelectionImpactWidget', () => {
    it('starts from an honest empty state and performs no analysis without a selection', async () => {
        const load = vi.fn();
        await act(async () => root.render(<SelectionImpactWidget project="fixture" onOpen={vi.fn()} load={load} />));
        expect(container.textContent).toContain('Select an indexed file or symbol');
        expect(load).not.toHaveBeenCalled();
    });
    it('shows edge evidence, separate historical evidence, revision and user-initiated diagnostics', async () => {
        const open = vi.fn(), diagnose = vi.fn();
        const load = vi.fn(async () => fixture());
        await act(async () => root.render(<SelectionImpactWidget project="fixture" target={{ filePath: 'core.ts' }}
            expectedGeneration="newer-snapshot" onOpen={open} onDiagnose={diagnose} load={load} />));
        expect(container.textContent).toContain('Graph edge #7');
        expect(container.textContent).toContain('Historical evidence');
        expect(container.textContent).toContain('not functional dependency');
        expect(container.textContent).toContain('uncommitted changes');
        expect(container.textContent).toContain('Git HEAD:');
        expect(container.textContent).toContain('different index generations');
        expect(container.textContent).toContain('Explicit fixture commit');
        expect(diagnose).not.toHaveBeenCalled();
        const buttons = [...container.querySelectorAll('button')];
        await act(async () => buttons.find(button => button.textContent === 'caller')!.click());
        expect(open).toHaveBeenCalledWith({ filePath: 'caller.ts', name: 'caller', qualifiedName: 'fixture.caller', id: 2, line: 8 });
        await act(async () => buttons.find(button => button.textContent === 'Inspect local indexing diagnostics')!.click());
        expect(diagnose).toHaveBeenCalledOnce();
    });
    it('polls pending work and cancels responses for an obsolete selection', async () => {
        vi.useFakeTimers();
        const load = vi.fn(async (): Promise<SelectionImpactReply> => ({ status: 'pending' }));
        load.mockResolvedValueOnce({ status: 'pending' }).mockResolvedValueOnce(fixture());
        await act(async () => root.render(<SelectionImpactWidget project="fixture" target={{ filePath: 'core.ts' }} onOpen={vi.fn()} load={load} />));
        expect(container.textContent).toContain('Reading the graph snapshot');
        await act(async () => vi.advanceTimersByTimeAsync(500));
        expect(container.querySelector('[data-status="ready"]')).not.toBeNull();
        let resolveOld: (value: SelectionImpactReply) => void = () => {};
        const first = new Promise<SelectionImpactReply>(resolve => { resolveOld = resolve; });
        const newer = vi.fn().mockReturnValueOnce(first).mockResolvedValueOnce(fixture('new.ts'));
        await act(async () => root.render(<SelectionImpactWidget project="fixture" target={{ filePath: 'old.ts' }} onOpen={vi.fn()} load={newer} />));
        await act(async () => root.render(<SelectionImpactWidget project="fixture" target={{ filePath: 'new.ts' }} onOpen={vi.fn()} load={newer} />));
        await act(async () => resolveOld(fixture('old.ts')));
        expect(container.textContent).toContain('new.ts');
        expect(container.textContent).not.toContain('old.ts');
        expect(newer.mock.calls[0]?.[2].aborted).toBe(true);
    });
    it('reports connection failure without a reassuring risk badge', async () => {
        const load = vi.fn(async (): Promise<SelectionImpactReply> => { throw new Error('disconnected'); });
        await act(async () => root.render(<SelectionImpactWidget project="fixture" target={{ filePath: 'core.ts' }} onOpen={vi.fn()} load={load} />));
        expect(container.querySelector('[role="alert"]')?.textContent).toContain('Risk remains unresolved');
        expect(container.querySelector('.selection-impact-risk')).toBeNull();
    });

    it('keeps a compact overview and makes test and Git evidence directly navigable through separate tabs', async () => {
        const data = fixture();
        data.structural.test_candidates = 1;
        data.structural.findings[0]!.test_candidate = true;
        const open = vi.fn(), load = vi.fn(async () => data);
        await act(async () => root.render(<SelectionImpactWidget project="fixture" target={{ filePath: 'core.ts' }} onOpen={open} load={load} />));
        const panels = [...container.querySelectorAll<HTMLElement>('[role="tabpanel"]')];
        expect(panels.filter(panel => !panel.hidden)).toHaveLength(1);
        expect(panels.find(panel => !panel.hidden)?.textContent).toContain('Structural impact');
        const buttons = [...container.querySelectorAll<HTMLButtonElement>('button')];
        await act(async () => buttons.find(button => button.textContent === 'Tests to review · 1')!.click());
        expect(panels.find(panel => !panel.hidden)?.textContent).toContain('Test');
        await act(async () => buttons.find(button => button.textContent === 'Open test source: caller')!.click());
        expect(open).toHaveBeenCalledWith(expect.objectContaining({ filePath: 'caller.ts', line: 8 }));
        await act(async () => buttons.find(button => button.textContent === 'Git history · 1')!.click());
        expect(panels.find(panel => !panel.hidden)?.textContent).toContain('Explicit fixture commit');
        expect(panels.find(panel => !panel.hidden)?.textContent).toContain('Copy local Git evidence command');
        expect(container.querySelector('.selection-impact-risk')?.textContent).toContain('data limitations');
    });

    it('keeps refresh and risk available when the shared workspace already owns the selection header', async () => {
        const load = vi.fn<typeof fetchSelectionImpact>(async () => fixture());
        await act(async () => root.render(<SelectionImpactWidget compact project="fixture" target={{ filePath: 'core.ts' }} onOpen={vi.fn()} load={load} />));
        expect(container.querySelector('.selection-impact-scope')).toBeNull();
        expect(container.querySelector('.selection-impact-risk')?.textContent).toContain('Transparent heuristic');
        const refresh = [...container.querySelectorAll('button')].filter(button => button.textContent === 'Refresh analysis');
        expect(refresh).toHaveLength(1);
        await act(async () => refresh[0]!.click());
        expect(load).toHaveBeenCalledTimes(2);
        expect(load.mock.calls[1]?.[3]).toBe(true);
    });

    it('never paints old symbol findings beneath a new symbol label before effects clear state', async () => {
        const captures: string[] = [];
        function BeforeEffects(): null {
            useLayoutEffect(() => { captures.push(container.textContent ?? ''); });
            return null;
        }
        const load = vi.fn().mockResolvedValueOnce(fixture())
            .mockImplementationOnce(() => new Promise<SelectionImpactReply>(() => {}));
        await act(async () => root.render(<><SelectionImpactWidget project="fixture"
            target={{ filePath: 'core.ts', qualifiedName: 'fixture.first', name: 'first symbol' }}
            onOpen={vi.fn()} load={load} /><BeforeEffects /></>));
        expect(container.textContent).toContain('Historical evidence');
        captures.length = 0;
        await act(async () => root.render(<><SelectionImpactWidget project="fixture"
            target={{ filePath: 'core.ts', qualifiedName: 'fixture.second', name: 'second symbol' }}
            onOpen={vi.fn()} load={load} /><BeforeEffects /></>));
        expect(captures[0]).toContain('second symbol');
        expect(captures[0]).not.toContain('Historical evidence');
    });

    it('progressively reveals returned evidence while keeping the actual counts visible', async () => {
        const data = fixture();
        const original = data.structural.findings[0]!;
        data.structural.findings = Array.from({ length: 20 }, (_, index) => ({ ...original, id: index + 10 }));
        data.structural.reachable = 40;
        data.structural.truncated = true;
        data.history.cochanges = Array.from({ length: 12 }, (_, index) => ({ ...data.history.cochanges[0]!, file_path: `peer-${index}.ts` }));
        data.history.cochanges_omitted = 3;
        const load = vi.fn(async () => data);
        await act(async () => root.render(<SelectionImpactWidget project="fixture" target={{ filePath: 'core.ts' }} onOpen={vi.fn()} load={load} />));
        expect(container.querySelectorAll('.selection-impact-findings > li')).toHaveLength(8);
        expect(container.querySelectorAll('.selection-impact-history > li')).toHaveLength(5);
        expect(container.textContent).toContain('40 total dependents');
        expect(container.textContent).toContain('3 further co-change files were omitted');
        const more = [...container.querySelectorAll('button')].find(button => button.textContent?.includes('Show more affected symbols'))!;
        expect(more.textContent).toContain('(8 of 20)');
        await act(async () => more.click());
        expect(container.querySelectorAll('.selection-impact-findings > li')).toHaveLength(16);
        const history = [...container.querySelectorAll('button')].find(button => button.textContent?.includes('Show more historical associations'))!;
        await act(async () => history.click());
        expect(container.querySelectorAll('.selection-impact-history > li')).toHaveLength(10);
    });
});
