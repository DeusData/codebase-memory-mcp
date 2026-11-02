// @vitest-environment jsdom
import { act, useLayoutEffect } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ChangeAnalysisWorkspace from './ChangeAnalysisWorkspace';
import LocalEvidenceCommand from './LocalEvidenceCommand';
import type { ChangeSetReading, loadChangeSet } from './change-set';
import type { SelectionImpactReply, fetchSelectionImpact } from './selection-impact';

let container: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); });
const pendingImpact = async (): Promise<SelectionImpactReply> => new Promise(() => {});
const reading = (project = 'fixture'): ChangeSetReading => ({ project, base: 'HEAD', mergeBase: 'a'.repeat(40),
    checkedAt: '2026-09-09T10:00:00Z', files: ['src/fixture.c'], totalFiles: 1, complete: true, pages: 1, limitations: [] });
const click = async (label: string) => { await act(async () => [...container.querySelectorAll('button')].find(button => button.textContent === label)!.click()); };

describe('shared change impact workspace', () => {
    it('focuses the analysis on entry and restores its initiating control on close', async () => {
        const trigger = document.createElement('button'); document.body.appendChild(trigger); trigger.focus();
        const close = vi.fn();
        await act(async () => root.render(<ChangeAnalysisWorkspace project="fixture" selectedTarget={{ filePath: 'fixture.c' }}
            onOpenSource={vi.fn()} onClose={close} loadImpact={pendingImpact} />));
        expect(document.activeElement?.textContent).toBe('Close change impact');
        await act(async () => document.activeElement?.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })));
        expect(close).toHaveBeenCalledOnce();
        await act(async () => root.render(null));
        expect(document.activeElement).toBe(trigger);
        trigger.remove();
    });
    it('starts with actual changes when no editor selection exists, then opens source without leaving the analysis', async () => {
        const loadChanges = vi.fn<typeof loadChangeSet>(async () => reading()), open = vi.fn();
        const loadImpact = vi.fn<typeof fetchSelectionImpact>(pendingImpact);
        await act(async () => root.render(<ChangeAnalysisWorkspace project="fixture" onOpenSource={open} loadChanges={loadChanges} loadImpact={loadImpact} />));
        expect(loadChanges.mock.calls[0]?.[0]).toBe('fixture');
        expect(container.textContent).toContain('1 of 1 changed paths loaded');
        expect(loadImpact).not.toHaveBeenCalled();
        await click('src/fixture.c');
        expect(loadImpact.mock.calls[0]?.[1]).toMatchObject({ filePath: 'src/fixture.c' });
        await click('Open source evidence');
        expect(open).toHaveBeenCalledWith({ filePath: 'src/fixture.c', name: 'fixture.c' });
        expect(container.querySelector('[data-testid="change-analysis-workspace"]')).not.toBeNull();
        expect(container.textContent).toContain('This file is in the detected change set.');
        expect(container.textContent).toContain('not only edited lines');
    });
    it('keeps incomplete change-set errors visible instead of making a low-risk claim', async () => {
        const data = { ...reading(), totalFiles: 6, complete: false, limitations: ['Cursor unavailable'] };
        await act(async () => root.render(<ChangeAnalysisWorkspace project="fixture" onOpenSource={vi.fn()}
            loadChanges={async () => data} loadImpact={pendingImpact} />));
        expect(container.textContent).toContain('1 of 6 changed paths loaded');
        expect(container.querySelector('[role="alert"]')?.textContent).toContain('incomplete');
    });
    it('does not paint a previous selection under changed props, even before passive effects', async () => {
        const captures: string[] = [];
        function Observe() { useLayoutEffect(() => { captures.push(container.textContent ?? ''); }); return null; }
        const props = { project: 'fixture', onOpenSource: vi.fn(), loadImpact: pendingImpact };
        await act(async () => root.render(<><ChangeAnalysisWorkspace {...props} selectedTarget={{ filePath: 'old.c' }} /><Observe /></>));
        captures.length = 0;
        await act(async () => root.render(<><ChangeAnalysisWorkspace {...props} selectedTarget={{ filePath: 'new.c' }} /><Observe /></>));
        expect(captures[0]).toContain('new.c');
        expect(captures[0]).not.toContain('old.c');
    });
    it('does not show a late change-set response from another project', async () => {
        let resolveOld!: (value: ChangeSetReading) => void;
        const old = new Promise<ChangeSetReading>(resolve => { resolveOld = resolve; });
        const loadChanges = vi.fn().mockReturnValueOnce(old).mockImplementationOnce(() => new Promise(() => {}));
        const props = { onOpenSource: vi.fn(), loadChanges, loadImpact: pendingImpact };
        await act(async () => root.render(<ChangeAnalysisWorkspace {...props} project="old-project" />));
        await act(async () => root.render(<ChangeAnalysisWorkspace {...props} project="new-project" />));
        await act(async () => resolveOld(reading('old-project')));
        expect(container.textContent).not.toContain('src/fixture.c');
        expect(loadChanges.mock.calls[0]?.[2].aborted).toBe(true);
    });
});

describe('local Git evidence action', () => {
    it('copies only after a deliberate click and displays the exact local command', async () => {
        const writeText = vi.fn().mockResolvedValue(undefined);
        Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
        await act(async () => root.render(<LocalEvidenceCommand command="git show aaaa -- 'fixture.c'" />));
        expect(writeText).not.toHaveBeenCalled();
        await click('Copy local command');
        expect(writeText).toHaveBeenCalledWith("git show aaaa -- 'fixture.c'");
        expect(container.textContent).toContain('Nothing was executed or sent');
    });
    it('reports clipboard rejection without claiming success and keeps a manual copy fallback', async () => {
        Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText: vi.fn().mockRejectedValue(new Error('Clipboard denied')) } });
        await act(async () => root.render(<LocalEvidenceCommand command="git diff HEAD -- 'fixture.c'" />));
        await click('Copy local command');
        expect(container.textContent).toContain('Clipboard denied');
        expect(container.textContent).not.toContain('Command copied');
        expect(container.querySelector('code')?.textContent).toBe("git diff HEAD -- 'fixture.c'");
    });
});
