// @vitest-environment jsdom
/*
 * Hand test 2026-10-04, round 4 (N1): "Selection details" named the Branch
 * node "DETACHED" and its relationships "DETACHED" as well. It names it for
 * what it is now. The node has no file (the index stores "{}" for it, which
 * Galaxy no longer takes for one), and its relationships still come from the
 * loaded scope; there is no source to read.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import SelectionContextPanel from './SelectionContext';
import type { GraphData, GraphNode } from '../galaxy/types';

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

const node = (id: number, extra: Partial<GraphNode>): GraphNode => ({ id, x: 0, y: 0, z: 0, size: 1, color: '#fff', label: 'Folder', name: `n${id}`, ...extra });
const project = node(1, { label: 'Project', name: 'django-demo', qualified_name: 'django-demo' });
const branch = node(2, { label: 'Branch', name: 'DETACHED', qualified_name: 'django-demo.__branch__.detached' });
const github = node(4, { name: '.github', qualified_name: 'django-demo..github', file_path: '.github' });
const readme = node(5, { label: 'File', name: 'README.rst', qualified_name: 'django-demo.README.rst.__file__', file_path: 'README.rst' });
const scope: GraphData = { total_nodes: 4, nodes: [project, branch, github, readme], edges: [
    { source: 1, target: 2, type: 'HAS_BRANCH' }, { source: 2, target: 4, type: 'CONTAINS_FOLDER' }, { source: 2, target: 5, type: 'CONTAINS_FILE' }] };
const open = async () => { for (const details of host.querySelectorAll('details')) details.open = true; };

it('N1: a selected Branch node without a file is named for what it is and keeps its relationships from the scope', async () => {
    const navigate = vi.fn();
    await act(async () => root.render(<SelectionContextPanel selected={branch} path="" onNavigate={navigate} scope={{ graph: scope, complete: true, direction: 'both', depth: 1 }} />));
    await open();
    expect(host.querySelector('.selection-context-subject')?.textContent).toBe('django-demo · detached HEAD');
    expect(host.textContent).not.toContain('Select a file or symbol');
    const summaries = [...host.querySelectorAll('summary')].map(entry => entry.textContent);
    expect(summaries).toContain('Incoming relationships · 1 (HAS_BRANCH 1)');
    expect(summaries).toContain('Outgoing relationships · 2 (CONTAINS_FILE 1 · CONTAINS_FOLDER 1)');
    // Nothing to read: no source button for a node without a file.
    expect([...host.querySelectorAll('button')].map(button => button.textContent)).not.toContain('Read source evidence');
});

it('N1: the relationships of .github name the Branch node above it for what it is', async () => {
    await act(async () => root.render(<SelectionContextPanel selected={github} path=".github" onNavigate={vi.fn()} scope={{ graph: scope, complete: true, direction: 'both', depth: 1 }} />));
    await open();
    const names = [...host.querySelectorAll('.repo-map-nodes strong')].map(name => name.textContent);
    expect(names).toContain('django-demo · detached HEAD');
    expect(names).not.toContain('DETACHED');
});
