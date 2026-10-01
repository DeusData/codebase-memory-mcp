// @vitest-environment jsdom
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, expect, it, vi } from 'vitest';
import SourceEvidenceDrawer, { fileQualifiedName, type SourceEvidenceTarget } from './SourceEvidenceDrawer';
import type { GraphData, GraphNode } from '../galaxy/types';
import type { CodeSnippetResult } from '../provider/rpc-schemas';
import { COLUMNS, fileNodeForPath, moduleForFile } from '../provider/cypher';
import projectLockEof from './fixtures/project-lock-eof.json';

(globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
const path = 'src/daemon/application.c';
const moduleName = 'cbm-pr2068.src.daemon.application';
const makeNode = (id: number, label: string, qualified_name: string): GraphNode => ({
    id, label, qualified_name, name: 'application.c', file_path: path, x: 0, y: 0, z: 0, size: 1, color: '#fff',
});
const graph: GraphData = { total_nodes: 2, edges: [], nodes: [
    makeNode(1, 'File', 'cbm-pr2068.src.daemon.application.c.__file__'), makeNode(2, 'Module', moduleName),
] };
// Synthetic text with the exact paging shape observed on real Module responses.
function page(first = 2901, overrides: Partial<CodeSnippetResult> = {}): CodeSnippetResult {
    return { qualified_name: moduleName, file_path: `/workspace/${path}`, source_mode: 'full',
        start_line: first, end_line: first + 159, source: Array.from({ length: 160 }, (_, i) => `fixture source line ${first + i}`).join('\n') + '\n',
        source_truncated: true, source_clipped: true, next_start_line: first + 160, original_end_line: 3571, ...overrides };
}
const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => { for (const clean of cleanups.splice(0)) await clean(); vi.restoreAllMocks(); });
async function mount(getCodeSnippet = vi.fn(async (_project: string, _qn: string, window?: { startLine?: number; maxLines?: number }) => page(window?.startLine)),
    queryRows = vi.fn(async (_project: string, _query: string): Promise<Record<string, string>[]> => []), snapshot: GraphData | null = graph) {
    const container = document.createElement('div'); document.body.append(container);
    const root = createRoot(container), onClose = vi.fn(), onExplore = vi.fn();
    let mounted = true;
    const unmount = async () => { if (mounted) { mounted = false; await act(async () => root.unmount()); } container.remove(); };
    cleanups.push(unmount);
    const render = async (target: SourceEvidenceTarget = { filePath: path, line: 2909, kind: 'callsite' }, project = 'cbm-pr2068') => {
        await act(async () => root.render(<SourceEvidenceDrawer project={project} target={target} graph={snapshot ?? undefined}
            client={{ getCodeSnippet, queryRows }} onClose={onClose} onExplore={onExplore} />));
    };
    await render();
    const click = async (name: string) => {
        const button = [...container.querySelectorAll('button')].find(row => row.textContent === name);
        expect(button, name).toBeTruthy(); await act(async () => button!.click());
    };
    return { container, render, click, onClose, onExplore, unmount, getCodeSnippet, queryRows };
}

it('prefers the full-range Module over the File fragment and highlights the actual call-site line', async () => {
    expect(fileQualifiedName(graph, path)).toBe(moduleName);
    const ui = await mount();
    expect(ui.getCodeSnippet).toHaveBeenCalledWith('cbm-pr2068', moduleName, { startLine: 2901, maxLines: 160 });
    expect(ui.queryRows).not.toHaveBeenCalled();
    expect(ui.container.textContent).toContain('Indexed call site');
    expect(ui.container.querySelector('[data-selected=true]')?.textContent).toContain('2909');
    expect(ui.container.querySelectorAll('code > span')).toHaveLength(160); // No phantom trailing line.
    expect(ui.container.querySelectorAll('a')).toHaveLength(0); // Line labels are not 160 empty tab stops.
    expect(ui.container.querySelector('pre')?.tabIndex).toBe(0);
});

it('resolves source directly by project and path when Explore has no architecture snapshot', async () => {
    const queryRows = vi.fn(async () => [{ [COLUMNS.moduleForFile[0]]: moduleName }]);
    const ui = await mount(undefined, queryRows, null);
    expect(queryRows).toHaveBeenCalledExactlyOnceWith('cbm-pr2068', moduleForFile(path));
    expect(ui.getCodeSnippet).toHaveBeenCalledWith('cbm-pr2068', moduleName, { startLine: 2901, maxLines: 160 });
    expect(ui.container.querySelector('[data-selected=true]')?.textContent).toContain('2909');
});

it('uses the verified File fallback and reports an unindexed path without guessing a name', async () => {
    const fileName = moduleName + '.c.__file__';
    const queryRows = vi.fn(async (_project: string, query: string) => query === fileNodeForPath(path)
        ? [{ [COLUMNS.fileNode[0]]: fileName }] : []);
    const snippet = vi.fn(async () => page(2901, { qualified_name: fileName }));
    const ui = await mount(snippet, queryRows, null);
    expect(queryRows.mock.calls.map(call => call[1])).toEqual([moduleForFile(path), fileNodeForPath(path)]);
    expect(snippet).toHaveBeenCalledWith('cbm-pr2068', fileName, { startLine: 2901, maxLines: 160 });
    await ui.render({ filePath: 'missing.c' });
    expect(ui.container.querySelector('[role=alert]')?.textContent).toContain('No indexed Module or File declaration');
    expect(snippet).toHaveBeenCalledTimes(1);
});

it('does not continue a stale lookup after changing projects', async () => {
    const pending: ((rows: Record<string, string>[]) => void)[] = [];
    const queryRows = vi.fn(() => new Promise<Record<string, string>[]>(resolve => pending.push(resolve)));
    const ui = await mount(undefined, queryRows, null);
    await ui.render({ filePath: path, line: 50 }, 'other-project');
    await act(async () => pending[0]([{ [COLUMNS.moduleForFile[0]]: 'old-project.module' }]));
    expect(ui.getCodeSnippet).not.toHaveBeenCalled();
    await act(async () => pending[1]([{ [COLUMNS.moduleForFile[0]]: moduleName }]));
    expect(ui.getCodeSnippet).toHaveBeenCalledExactlyOnceWith('other-project', moduleName, { startLine: 42, maxLines: 160 });
});

it('paginates by the returned continuation, goes back, and resets a changed target location', async () => {
    const ui = await mount();
    await ui.click('Next lines');
    expect(ui.getCodeSnippet).toHaveBeenLastCalledWith('cbm-pr2068', moduleName, { startLine: 3061, maxLines: 160 });
    expect(ui.container.querySelector('[data-selected=true]')).toBeNull();
    await ui.click('Previous lines');
    expect(ui.container.querySelector('[data-selected=true]')?.textContent).toContain('2909');
    await ui.render({ filePath: path, line: 50 });
    expect(ui.getCodeSnippet).toHaveBeenLastCalledWith('cbm-pr2068', moduleName, { startLine: 42, maxLines: 160 });
    expect(ui.container.querySelector('[data-selected=true]')?.textContent).toContain('50');
});

it('does not paint a previous project reply under a new project heading', async () => {
    const pending: ((value: CodeSnippetResult) => void)[] = [];
    const getCodeSnippet = vi.fn(() => new Promise<CodeSnippetResult>(resolve => pending.push(resolve)));
    const ui = await mount(getCodeSnippet);
    await ui.render({ filePath: path, line: 10 }, 'other-project');
    await act(async () => pending[0](page(2901, { source: 'old project secret' })));
    expect(ui.container.textContent).not.toContain('old project secret');
    expect(ui.container.textContent).toContain('Reading local source');
    await act(async () => pending[1](page(2)));
    expect(ui.container.querySelector('[data-selected=true]')?.textContent).toContain('10');
});

it('labels an ignored source window honestly instead of relabeling the returned lines', async () => {
    // The actual File-node reply ignored start_line=2901 and returned 1..51.
    const ui = await mount(vi.fn(async () => page(1, { end_line: 51, source: Array(51).fill('fixture').join('\n'),
        source_truncated: undefined, source_clipped: undefined, next_start_line: undefined })));
    expect(ui.container.textContent).toContain('Requested line 2909 is outside this returned window');
    expect(ui.container.querySelector('[data-selected=true]')).toBeNull();
    expect(ui.container.querySelector('.source-evidence-line-number')?.textContent).toBe('1');
});

it.each([undefined, 2901, 9999])('does not follow a missing, repeating or skipping continuation (%s)', async next => {
    const ui = await mount(vi.fn(async () => page(2901, { next_start_line: next })));
    const nextButton = [...ui.container.querySelectorAll('button')].find(button => button.textContent === 'Next lines');
    expect(nextButton?.disabled).toBe(true);
    expect(ui.container.textContent).toContain('no valid next page');
});

it('disables continuation at the reported end of source', async () => {
    const ui = await mount(vi.fn(async () => page(2901, { source_truncated: false, source_clipped: false, next_start_line: undefined })));
    expect([...ui.container.querySelectorAll('button')].find(button => button.textContent === 'Next lines')?.disabled).toBe(true);
    expect(ui.container.textContent).not.toContain('no valid next page');
});

it('rejects source identity and line-range errors', async () => {
    const ui = await mount(vi.fn(async () => page(2901, { qualified_name: 'other-project.secret' })));
    expect(ui.container.querySelector('[role=alert]')?.textContent).toContain('different indexed file');
    expect(ui.container.querySelector('pre')).toBeNull();
});

it('does not invent line evidence for a malformed source body', async () => {
    const ui = await mount(vi.fn(async () => page(2901, { source: 'one line' })));
    expect(ui.container.querySelector('[role=alert]')?.textContent).toContain('does not match');
    expect(ui.container.querySelector('[data-selected=true]')).toBeNull();
    expect(ui.container.querySelector('.source-evidence-line-number')?.textContent).toBe('');
});

it('preserves the actual Module EOF line reported by the source API and highlights its call site', async () => {
    // Recorded local project_lock.c response, start_line=53/end_line=190.
    // Its trailing empty segment is the indexed EOF line, not a page separator.
    const ui = await mount(vi.fn(async () => page(53, { ...projectLockEof, source_mode: 'full',
        source_truncated: undefined, source_clipped: undefined, next_start_line: undefined })));
    await ui.render({ filePath: path, line: 61 });
    expect(ui.container.querySelector('[role=alert]')).toBeNull();
    expect(ui.container.querySelectorAll('code > span')).toHaveLength(138);
    expect(ui.container.querySelector('code > span:last-child .source-evidence-line-number')?.textContent).toBe('190');
    expect(ui.container.querySelector('[data-selected=true]')?.textContent).toContain('cbm_private_lock_directory_close(directory)');
    expect(ui.container.textContent).toContain('Lines 53 to 190');
    expect([...ui.container.querySelectorAll('button')].find(button => button.textContent === 'Next lines')?.disabled).toBe(true);
});

it('treats source markup as text and never requests external assets', async () => {
    const fetch = vi.spyOn(globalThis, 'fetch');
    const ui = await mount(vi.fn(async () => page(2901, { source: '<img src="https://example.invalid/private">', end_line: 2901 })));
    expect(ui.container.querySelector('img')).toBeNull();
    expect(ui.container.textContent).toContain('<img src=');
    expect(fetch).not.toHaveBeenCalled();
});

it('focuses close, lets Escape close the modeless drawer, then restores its original opener', async () => {
    const opener = document.createElement('button'); document.body.append(opener); opener.focus();
    const ui = await mount();
    expect(document.activeElement?.getAttribute('aria-label')).toBe('Close source evidence');
    expect(ui.container.querySelector('[role=dialog]')?.getAttribute('aria-modal')).toBe('false');
    await act(async () => document.activeElement!.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true })));
    expect(ui.onClose).toHaveBeenCalledTimes(1);
    await ui.unmount();
    expect(document.activeElement).toBe(opener); opener.remove();
});
