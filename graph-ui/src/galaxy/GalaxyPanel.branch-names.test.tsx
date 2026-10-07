// @vitest-environment jsdom
/*
 * Hand test 2026-10-04, round 4. N1: in the hierarchy of .github the node
 * the index puts above the top level folders stood as a bare "DETACHED".
 * Its name now says what it is in the toolbar, the history, Path to and the
 * chat label, and the hierarchy leaves room for that name. N2: the note
 * "nothing of this walk is in focus: the ring follows the symbol in front of
 * the reader" stood under that hierarchy, where the root stands marked in
 * the middle and Explore is not even open. It now stands only beside
 * Explore's reader, in plain words.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GalaxyPanel from './GalaxyPanel';
import { galaxyHierarchyNoteText } from './galaxy-strings';
import { hierarchyLabelWidth, SCOPED_HIERARCHY_LEVEL_GAP } from './graph-scope';
import { HIERARCHY_LABEL_PAD_X } from './hierarchy-layout';
import { scopeFetch, scopeNode } from './test-scope-fetch';
import type { BrowserChatContext } from '../browser-ai/chat-model';
import type { ClosureResult } from '../provider/closure';
import { toEditorRange } from '../core/positions';

vi.mock('./GraphScene', async importOriginal => ({ ...await importOriginal<typeof import('./GraphScene')>(), GraphScene: () => <output data-testid="scene" /> }));

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); globalThis.__atlasGalaxy = undefined; });

const seam = () => globalThis.__atlasGalaxy!;
const settle = (check: () => void) => vi.waitFor(async () => {
    await act(async () => { await new Promise(resolve => setTimeout(resolve, 0)); });
    check();
});
const count = () => host.querySelector('.atlas-graph-scope-count')?.textContent;
const note = () => host.querySelector('[data-testid="atlas-galaxy-note"]')?.textContent ?? '';
const rootName = () => host.querySelector('.atlas-graph-scope-name');

const branch = scopeNode(2, { label: 'Branch', name: 'DETACHED', qualified_name: 'django-demo.__branch__.detached', file_path: '{}', start_line: undefined, end_line: undefined });
const github = scopeNode(4, { label: 'Folder', name: '.github', qualified_name: 'django-demo..github', file_path: '.github', start_line: undefined, end_line: undefined });
const security = scopeNode(5, { label: 'File', name: 'SECURITY.md', qualified_name: 'django-demo..github.SECURITY.md.__file__', file_path: '.github/SECURITY.md' });
const workflows = scopeNode(6, { label: 'Folder', name: 'workflows', qualified_name: 'django-demo..github.workflows', file_path: '.github/workflows', start_line: undefined, end_line: undefined });
const edges = [{ id: 1, source: 2, target: 4, type: 'CONTAINS_FOLDER' }, { id: 2, source: 4, target: 5, type: 'CONTAINS_FILE' }, { id: 3, source: 4, target: 6, type: 'CONTAINS_FOLDER' }];

async function githubHierarchy(onSelectionEvidence = vi.fn()) {
    await act(async () => root.render(<GalaxyPanel project="django-demo" visible workspaceExpanded onOpenNode={vi.fn()} onSelectionEvidence={onSelectionEvidence}
        fetch={scopeFetch({ nodes: [branch, github, security, workflows], edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBe(4));
    await act(async () => { seam().clickNode('django-demo..github'); });
    await settle(() => expect(count()).toBe('4 nodes · 3 edges'));
    await act(async () => host.querySelector<HTMLButtonElement>('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.click());
    expect(seam().mode).toBe('hierarchy');
    return onSelectionEvidence;
}

it('N1: the hierarchy of .github leaves room for the shown name of the Branch node on its left', async () => {
    await githubHierarchy();
    const placed = Object.fromEntries(seam().hierarchy!.placements.map(placement => [placement.name, placement]));
    // The column gap follows the width of the name that is drawn, "django-demo · detached HEAD", not of "DETACHED".
    const gap = Math.max(SCOPED_HIERARCHY_LEVEL_GAP, (hierarchyLabelWidth('.github') + hierarchyLabelWidth('django-demo · detached HEAD')) / 2 + 40);
    expect(placed.DETACHED!.x).toBeCloseTo(-gap, 5);
    expect(placed.DETACHED!.x).toBeLessThan(-SCOPED_HIERARCHY_LEVEL_GAP);
    // The camera frames the whole name, which stands centred over its node (seen cut off at the left edge in the browser).
    const right = Math.max(...seam().hierarchy!.placements.map(placement => placement.x));
    expect(seam().lastFit!.width).toBeGreaterThanOrEqual(right + HIERARCHY_LABEL_PAD_X - placed.DETACHED!.x + hierarchyLabelWidth('django-demo · detached HEAD') / 2);
});

it('N2: the scoped hierarchy in the Galaxy tab carries no note about a ring that follows Explore', async () => {
    await githubHierarchy();
    expect(seam().pulsedQn).toBe('');
    expect(note()).toBe('');
    expect(host.textContent).not.toContain('nothing of this walk');
    expect(host.textContent).not.toContain(galaxyHierarchyNoteText.noFocus);
});

it('N1: with the Branch node as root, the toolbar, the history and the chat label name it, and the real name stays in the tooltip', async () => {
    const evidence = await githubHierarchy();
    await act(async () => host.querySelector<HTMLButtonElement>('[data-testid="atlas-graph-mode-chip"][data-mode="galaxy"]')!.click());
    await act(async () => { seam().clickNode('django-demo.__branch__.detached'); });
    await settle(() => expect(count()).toBe('2 nodes · 1 edge'));
    expect(rootName()?.textContent).toBe('django-demo · detached HEAD');
    // The "{}" the index stores as its file is no source to open: the name is no button.
    expect(rootName()?.tagName).toBe('STRONG');
    expect(rootName()?.getAttribute('title')).toContain('Name in the index: DETACHED (django-demo.__branch__.detached)');
    expect(seam().history.recent).toContain('django-demo · detached HEAD · 1 layer');
    const recent = [...host.querySelectorAll('.atlas-graph-recent-menu strong')].map(name => name.textContent);
    expect(recent).toEqual(expect.arrayContaining(['django-demo · detached HEAD', '.github']));
    // The chat names the selection as the Galaxy does; the snapshot keeps the identity of the index.
    const context = evidence.mock.lastCall![0] as BrowserChatContext;
    expect(context.label).toBe('django-demo · detached HEAD');
    const snapshot = JSON.parse(context.text).evidence;
    expect(snapshot.selected.scope).toMatchObject({ kind: 'node', name: 'DETACHED', qualifiedName: 'django-demo.__branch__.detached' });
    expect(snapshot.selected.roots[0]).toMatchObject({ name: 'DETACHED', kind: 'Branch' });
    expect(snapshot.selected.roots[0].filePath).toBeUndefined();
});

it('N1: a file scope with a single root keeps its path as the name of the root', async () => {
    await act(async () => root.render(<GalaxyPanel project="django-demo" visible workspaceExpanded onOpenNode={vi.fn()}
        fetch={scopeFetch({ nodes: [branch, github, security, workflows], edges }).fetch} />));
    await settle(() => expect(seam().nodes).toBe(4));
    const input = host.querySelector<HTMLInputElement>('input[aria-label="Find a graph node"]')!;
    await act(async () => {
        input.focus();
        Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, 'SECURITY');
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
    const file = [...host.querySelectorAll<HTMLButtonElement>('.atlas-galaxy-search-results li button')]
        .find(button => button.querySelector('.atlas-graph-result-kind')?.textContent === 'File' && button.querySelector('strong')?.textContent === '.github/SECURITY.md')!;
    await act(async () => file.click());
    await settle(() => expect(count()).toMatch(/^1 node · /));
    expect(rootName()?.textContent).toBe('.github/SECURITY.md');
});

it('N1: Path to names the Branch node in its heading', async () => {
    await githubHierarchy();
    host.querySelector<HTMLDetailsElement>('.atlas-graph-path-picker')!.open = true;
    const target = [...host.querySelectorAll<HTMLButtonElement>('.atlas-graph-path-menu li button')]
        .find(entry => entry.querySelector('strong')?.textContent === 'django-demo · detached HEAD')!;
    await act(async () => target.click());
    expect(host.querySelector('[data-testid="atlas-galaxy-path-panel"] strong')?.textContent).toBe('Path to django-demo · detached HEAD · 1 hop');
    expect(seam().history.entries.at(-1)).toContain('path to django-demo · detached HEAD');
});

/* A walk in the Explore panel: the place where the ring follows the symbol open in the reader. */
const qn = { start: 'atlas.src.app.start', helper: 'atlas.src.app.helper' };
const symbol = (name: keyof typeof qn, line: number) => ({ name, qualifiedName: qn[name], kind: 'function' as const, uri: 'file:///workspace/src/app.ts',
    range: toEditorRange(line, line + 3), selectionRange: toEditorRange(line, line) });
const walk: ClosureResult = { root: symbol('start', 1), nodes: [{ symbol: symbol('start', 1), hop: 0 }, { symbol: symbol('helper', 9), hop: 1, via: qn.start }],
    edges: [{ from: qn.start, to: qn.helper, line: 2 }], truncated: false, visited: 2, depth: 2, cap: 15 };
const layout = () => vi.fn(async () => new Response(JSON.stringify({ nodes: [], edges: [], total_nodes: 0 })));

it('N2: beside Explore the note says plainly that no node of the walk is open there, and goes once one is', async () => {
    const fetch = layout();
    await act(async () => root.render(<GalaxyPanel project="atlas" visible onOpenNode={vi.fn()} fetch={fetch} walk={walk} focusQualifiedName="atlas.src.other.elsewhere" />));
    await settle(() => expect(seam().mode).toBe('hierarchy'));
    expect(note()).toBe('None of these nodes is open in Explore; the ring marks the symbol open there.');
    expect(galaxyHierarchyNoteText.noFocus).toBe(note());
    await act(async () => root.render(<GalaxyPanel project="atlas" visible onOpenNode={vi.fn()} fetch={fetch} walk={walk} focusQualifiedName={qn.helper} />));
    expect(seam().pulsedQn).toBe(qn.helper);
    expect(note()).toBe('');
});

it('N2: the hierarchy chip says in plain words what it needs before it can show anything', async () => {
    await act(async () => root.render(<GalaxyPanel project="atlas" visible onOpenNode={vi.fn()} fetch={layout()} />));
    const chip = host.querySelector('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.closest('[data-hint]');
    expect(chip?.getAttribute('data-hint')).toBe(galaxyHierarchyNoteText.unavailable);
    expect(galaxyHierarchyNoteText.unavailable).not.toMatch(/way in/);
    await act(async () => root.render(<GalaxyPanel project="atlas" visible workspaceExpanded onOpenNode={vi.fn()} fetch={layout()} />));
    const workspaceChip = host.querySelector('[data-testid="atlas-graph-mode-chip"][data-mode="hierarchy"]')!.closest('[data-hint]');
    expect(workspaceChip?.getAttribute('data-hint')).toBe(galaxyHierarchyNoteText.unavailableWorkspace);
});
