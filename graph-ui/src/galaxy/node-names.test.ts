import { expect, it } from 'vitest';
import { branchNodeOf, graphNodeName, graphNodeTitle, nodeDisplayName, nodeFilePath, scopeDisplayName } from './node-names';
import type { GraphNode } from './types';

/*
 * Hand test 2026-10-04, round 4 (N1): in the hierarchy of .github the node
 * the index puts above the top level folders stood as a bare "DETACHED"
 * (django-demo, a detached HEAD) and in cbm as "working-tree". Both read
 * like a folder of that name. Branch nodes now carry what they are.
 */
const node = (extra: Partial<GraphNode>): GraphNode => ({ id: 2, x: 0, y: 0, z: 0, size: 3, color: '#ffc070', label: 'Branch', name: 'DETACHED', ...extra });
const detached = node({ qualified_name: 'django-demo.__branch__.detached', file_path: '{}' });
const workingTree = node({ name: 'working-tree', qualified_name: 'cbm.__branch__.working-tree' });
const main = node({ name: 'main', qualified_name: 'django-demo.__branch__.main' });

it('N1: a Branch node is named for what it is, with its project', () => {
    expect(graphNodeName(detached)).toBe('django-demo · detached HEAD');
    expect(graphNodeName(workingTree)).toBe('cbm · working tree');
    expect(graphNodeName(main)).toBe('django-demo · branch main');
    // A branch name with a slash and a dot keeps its real spelling, not the slug of its qualified name.
    expect(graphNodeName(node({ name: 'fix/atlas-call-feedback', qualified_name: 'cbm.__branch__.fix-atlas-call-feedback' }))).toBe('cbm · branch fix/atlas-call-feedback');
    expect(graphNodeName(node({ name: 'release-1.2', qualified_name: 'my.project.__branch__.release-1.2' }))).toBe('my.project · branch release-1.2');
});

it('N1: the label Branch and the qualified name pattern are the signals; other nodes keep their names', () => {
    expect(branchNodeOf({ name: 'DETACHED', qualifiedName: 'django-demo.__branch__.detached' })).toEqual({ project: 'django-demo', name: 'DETACHED', state: 'detached' });
    expect(branchNodeOf({ name: 'working-tree', label: 'Branch' })).toEqual({ name: 'working-tree', state: 'working-tree' });
    expect(nodeDisplayName({ name: 'working-tree', label: 'Branch' })).toBe('working tree');
    expect(nodeDisplayName({ name: 'DETACHED', label: 'Branch', project: 'django-demo' })).toBe('django-demo · detached HEAD');
    // A search hit has no name column; its name is the slug of the qualified name.
    expect(nodeDisplayName({ name: 'detached', label: 'Branch', qualifiedName: 'django-demo.__branch__.detached' })).toBe('django-demo · detached HEAD');
    expect(nodeDisplayName({ name: 'Detached', label: 'Branch', qualifiedName: 'p.__branch__.Detached' })).toBe('p · branch Detached');
    // Folders, files and symbols stay as they are, also a symbol in a module called __branch__.
    expect(graphNodeName(node({ label: 'Folder', name: '.github', qualified_name: 'django-demo..github' }))).toBe('.github');
    expect(graphNodeName(node({ label: 'Function', name: 'run', qualified_name: 'p.pkg.__branch__.run' }))).toBe('run');
    expect(branchNodeOf({ name: 'run', label: 'Function', qualifiedName: 'p.pkg.__branch__.run' })).toBeUndefined();
    expect(nodeDisplayName({ name: 'JSONBAgg', qualifiedName: 'django-demo.django.contrib.postgres.aggregates.general.JSONBAgg' })).toBe('JSONBAgg');
});

it('N1: the tooltip keeps the real name, and the "{}" the index stores as a Branch file is no file', () => {
    expect(graphNodeTitle(detached)).toBe('django-demo · detached HEAD: the checkout the index read, above its top level folders and files. '
        + 'Name in the index: DETACHED (django-demo.__branch__.detached).');
    expect(graphNodeTitle(node({ label: 'Folder', name: '.github', qualified_name: 'django-demo..github' }))).toBe('.github');
    expect(nodeFilePath(detached)).toBeUndefined();
    expect(nodeFilePath(node({ label: 'File', name: 'a.py', file_path: 'a.py' }))).toBe('a.py');
    // A scope carries no label; its qualified name is enough.
    expect(scopeDisplayName({ kind: 'node', id: 2, name: 'DETACHED', qualifiedName: 'django-demo.__branch__.detached' })).toBe('django-demo · detached HEAD');
    expect(scopeDisplayName({ kind: 'symbol', name: 'working-tree', qualifiedName: 'cbm.__branch__.working-tree' })).toBe('cbm · working tree');
    expect(scopeDisplayName({ kind: 'folder', path: '.github', name: '.github/' })).toBe('.github/');
});
