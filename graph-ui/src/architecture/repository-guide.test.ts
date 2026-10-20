import { expect, it } from 'vitest';
import { entryCandidates, entryRole, entryRoutes, sourceCandidates, sourceQuote } from './repository-guide';
import { repositoryMap } from './repository-map';
import type { GraphData, GraphNode } from '../galaxy/types';

const node = (id: number, file_path: string, name: string, status: GraphNode['status'] = 'normal'): GraphNode => ({
    id, file_path, name, status, label: 'Function', x: 0, y: 0, z: 0, size: 1, color: '',
});
const graph: GraphData = { total_nodes: 3, nodes: [node(1, 'src/api/main.ts', 'main', 'entry'),
    node(2, 'src/service/users.ts', 'users'), node(3, 'src/store/save.ts', 'save')],
    edges: [{ source: 1, target: 2, type: 'CALLS' }, { source: 2, target: 3, type: 'IMPORTS' }, { source: 2, target: 3, type: 'CALLS' }] };

it('separates application entries from test and tool entry candidates', () => {
    const test = node(10, 'tests/server.test.ts', 'main', 'entry');
    const tool = node(11, 'graph-ui/tools/smoke.mjs', 'main', 'entry');
    const app = node(12, 'src/main.c', 'main', 'entry');
    expect(entryRole(test)).toBe('Tests'); expect(entryRole(tool)).toBe('Tools');
    expect(entryRole(node(13, 'pkg/npm/install.js', 'main', 'entry'))).toBe('Tools');
    expect(entryRole(node(14, 'setup.py', 'main', 'entry'))).toBe('Tools');
    expect(entryRole(node(15, '.git/hooks/pre-commit', 'main', 'entry'))).toBe('Tools');
    expect(entryRole(node(16, 'pkg/pypi/src/codebase_memory_mcp/_cli.py', 'main', 'entry'))).toBe('Application');
    expect(entryCandidates([test, tool, app])[0]).toBe(app);
});

it('quotes a real README paragraph while skipping badges, HTML and code', () => {
    expect(sourceQuote('README.md', '# Project\n[![CI](badge.png)](https://example.com)\n\nA local repository index exposes symbols and relationships to coding tools.\n'))
        .toEqual({ path: 'README.md', line: 4, text: 'A local repository index exposes symbols and relationships to coding tools.', kind: 'readme' });
    expect(sourceQuote('README.md', '# Project\n```\nThis is an example string with enough words to look like a description.\n```')).toBeUndefined();
});

it('retains header location, ignores code and does not turn a license into a responsibility', () => {
    expect(sourceQuote('src/store/store.h', '/*\n * Opaque SQLite graph store for repository symbols.\n * One handle per thread.\n */\nvoid store();'))
        .toMatchObject({ line: 2, text: 'Opaque SQLite graph store for repository symbols. One handle per thread.' });
    expect(sourceQuote('src/a.ts', '// Copyright 2026 Example\n// Permission is hereby granted')).toBeUndefined();
    expect(sourceQuote('src/a.ts', 'export function veryImportantRepositoryModule() {}')).toBeUndefined();
});

it('quotes only comments and never includes adjacent code or C preprocessor directives', () => {
    expect(sourceQuote('src/a.h', '#ifndef HEADER_H\n#define HEADER_H\n// A helpful explanation appears after declarations.\nstruct Component {};')).toBeUndefined();
    expect(sourceQuote('src/a.c', '// Coordinates clients and resource subscriptions.\nint private_state = 99;'))
        .toMatchObject({ text: 'Coordinates clients and resource subscriptions.', line: 1 });
    expect(sourceQuote('src/a.c', '/* Coordinates clients and resource subscriptions. */ int private_state = 99;'))
        .toMatchObject({ text: 'Coordinates clients and resource subscriptions.', line: 1 });
    expect(sourceQuote('src/a.py', '#!/usr/bin/env python\n"""Coordinates clients and resource subscriptions."""\nprivate_state = 99'))
        .toMatchObject({ text: 'Coordinates clients and resource subscriptions.', line: 2 });
    expect(sourceQuote('src/a.py', '# Coordinates clients and resource subscriptions.\nprivate_state = 99'))
        .toMatchObject({ text: 'Coordinates clients and resource subscriptions.', line: 1 });
    expect(sourceQuote('src/a.c', '/* Coordinates clients and resource subscriptions. */', 0)).toBeUndefined();
});

it('ranks companion headers using recorded implementation connections, without module-name rules', () => {
    const nodes = [node(1, 'src/widget/alphabet.h', 'config'), node(2, 'src/widget/service.h', 'api'),
        node(3, 'src/widget/service.c', 'handle'), node(4, 'src/client/use.c', 'call')];
    const map = repositoryMap({ total_nodes: nodes.length, nodes,
        edges: [{ source: 4, target: 3, type: 'CALLS' }] });
    expect(sourceCandidates(map.areas.find(area => area.path === 'src/widget')!)[0]).toBe('src/widget/service.h');
});

it('follows only actual call edges across components and retains their evidence', () => {
    const map = repositoryMap(graph);
    const paths = entryRoutes(graph.nodes[0], map.evidence);
    expect(paths[0].edges.map(edge => edge.target.name)).toEqual(['users', 'save']);
    expect(paths[0].edges.every(edge => edge.type === 'CALLS')).toBe(true);
    expect(paths[0].edges[0]).toBe(map.evidence[0]);
    expect(entryRoutes(graph.nodes[2], map.evidence)).toEqual([]);
});
