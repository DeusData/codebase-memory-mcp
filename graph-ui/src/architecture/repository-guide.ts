import type { GraphNode } from '../galaxy/types';
import { areaOf, type RepositoryArea, type MapEvidence } from './repository-map';

export interface SourceQuote { text: string; path: string; line: number; kind: 'readme' | 'header' }
export type SourceReader = (path: string, startLine?: number, maxLines?: number) => Promise<{ source: string; start_line?: number }>;
export type EntryRole = 'Application' | 'Frontend' | 'Tests' | 'Tools';

/** Labels are navigation heuristics; they do not promote a name into a verified entry point. */
export function entryRole(node: GraphNode): EntryRole {
    const path = node.file_path ?? '';
    if (node.status === 'test' || /(^|\/)(tests?|fixtures?|verification|eval)(\/|\.)|\.(test|spec)\./i.test(path)) return 'Tests';
    if (/(^|\/)(scripts?|tools?|hooks?|examples?|benchmarks?)(\/|\.)/i.test(path)
        || /(^|\/)(install|uninstall|setup)\.[^/]+$/i.test(path)) return 'Tools';
    if (/\.(tsx|jsx|vue|svelte)$/.test(path)) return 'Frontend';
    return 'Application';
}

export function entryCandidates(nodes: GraphNode[]): GraphNode[] {
    return nodes.filter(node => node.file_path && node.status === 'entry')
        .sort((a, b) => (Number(entryRole(a) === 'Tests' || entryRole(a) === 'Tools') - Number(entryRole(b) === 'Tests' || entryRole(b) === 'Tools'))
            || Number(/^(main|App|bootstrap|start)$/i.test(b.name)) - Number(/^(main|App|bootstrap|start)$/i.test(a.name))
            || (b.out_calls ?? 0) - (a.out_calls ?? 0) || a.name.localeCompare(b.name));
}

export function sourceCandidates(area: RepositoryArea): string[] {
    const part = area.path.split('/').at(-1);
    const degree = new Map<string, number>();
    for (const edge of [...area.incoming, ...area.outgoing]) for (const node of [edge.source, edge.target]) {
        if (node.file_path && areaOf(node.file_path) === area.path) degree.set(node.file_path, (degree.get(node.file_path) ?? 0) + 1);
    }
    // C declarations often live in a header, while recorded calls belong to its
    // implementation. Let those observed connections rank companion headers;
    // do not special-case a repository's module names.
    const connectedness = (path: string) => /\.(h|hpp)$/.test(path)
        ? Math.max(degree.get(path) ?? 0, ...['c', 'cc', 'cpp', 'cxx'].map(extension => degree.get(path.replace(/\.(h|hpp)$/, `.${extension}`)) ?? 0))
        : degree.get(path) ?? 0;
    const score = (path: string) => (/\/README\.md$/i.test(path) ? 30000 : 0) + (/\.(h|hpp)$/.test(path) ? 10000 : 0) + (path.split('/').at(-1)?.split('.')[0] === part ? 20000 : 0)
        + Math.min(5000, connectedness(path)) - path.split('/').length;
    return area.files.filter(path => /\.(h|hpp|c|cc|cpp|cxx|ts|tsx|js|jsx|py|rs|go|java|cs|md)$/.test(path))
        .sort((a, b) => score(b) - score(a) || a.localeCompare(b)).slice(0, 2);
}

function plainMarkdown(text: string): string {
    return text.replace(/!\[[^\]]*\]\([^)]*\)/g, '').replace(/\[([^\]]+)\]\([^)]*\)/g, '$1').replace(/[*_`]/g, '').replace(/<[^>]*>/g, '').trim();
}

/** Quote only repository-authored prose, never infer a responsibility from an identifier. */
export function sourceQuote(path: string, source: string, start = 1): SourceQuote | undefined {
    if (!Number.isSafeInteger(start) || start < 1) return undefined;
    const lines = source.split('\n');
    if (/(^|\/)readme\.(md|txt|rst)$/i.test(path)) {
        let fenced = false;
        for (let index = 0; index < lines.length; index++) {
            const raw = lines[index].trim();
            if (/^```|^~~~/.test(raw)) { fenced = !fenced; continue; }
            if (fenced || !raw || /^(#|[!\[<|>]|[-=]{3})/.test(raw)) continue;
            const text = plainMarkdown(raw);
            if (text.length >= 45 && /[a-z]{3}\s+[a-z]{3}/i.test(text)) return { text: text.slice(0, 500), path, line: start + index, kind: 'readme' };
        }
        return undefined;
    }
    const first = lines.findIndex(line => line.trim() && !/^#!/.test(line.trim()));
    if (first < 0) return undefined;
    const firstText = lines[first].trim();
    const hashComments = /\.(py|rb|sh|bash|zsh|yaml|yml|toml|r|pl|pm|ps1)$/i.test(path);
    const block = firstText.startsWith('/*') ? ['/*', '*/']
        : firstText.startsWith('"""') ? ['"""', '"""']
            : firstText.startsWith("'''") ? ["'''", "'''"] : undefined;
    const prefix = firstText.startsWith('//') ? '//'
        : hashComments && firstText.startsWith('#') ? '#' : undefined;
    if (!block && !prefix) return undefined;
    const prose: { text: string; line: number }[] = [];
    for (let index = first; index < Math.min(lines.length, first + 40); index++) {
        let raw = lines[index].trim(), closed = false;
        if (block) {
            if (index === first) raw = raw.slice(block[0].length);
            const end = raw.indexOf(block[1]);
            if (end >= 0) { raw = raw.slice(0, end); closed = true; }
            if (block[0] === '/*') raw = raw.replace(/^\*+\s?/, '');
        } else {
            if (!raw.startsWith(prefix!)) break;
            raw = raw.slice(prefix!.length);
        }
        const text = raw.trim();
        if (/copyright|SPDX|permission is hereby|licensed under|all rights reserved/i.test(text)) return undefined;
        if (text) prose.push({ text, line: start + index });
        if (closed) break;
    }
    if (!prose.length) return undefined;
    const text = prose.map(row => row.text).join(' ');
    return text.length >= 30 && /[a-z]{3}\s+[a-z]{3}/i.test(text) ? { text: text.slice(0, 550), path, line: prose[0].line, kind: 'header' } : undefined;
}

export interface StaticRoute { edges: MapEvidence[]; limited: boolean }
/** A bounded actual CALLS walk. Imports and co-change never become execution steps. */
export function entryRoutes(entry: GraphNode, evidence: MapEvidence[]): StaticRoute[] {
    const outgoing = new Map<number, MapEvidence[]>();
    for (const edge of evidence) if (edge.type === 'CALLS' && entryRole(edge.target) !== 'Tests' && entryRole(edge.target) !== 'Tools') {
        const rows = outgoing.get(edge.source.id) ?? []; rows.push(edge); outgoing.set(edge.source.id, rows);
    }
    const queue: { node: GraphNode; edges: MapEvidence[] }[] = [{ node: entry, edges: [] }];
    const visited = new Set<number>([entry.id]); const paths: MapEvidence[][] = [];
    let cursor = 0;
    while (cursor < queue.length && visited.size < 500) {
        const row = queue[cursor++];
        if (row.edges.length >= 5) continue;
        for (const edge of outgoing.get(row.node.id) ?? []) {
            if (visited.has(edge.target.id)) continue;
            visited.add(edge.target.id);
            const edges = [...row.edges, edge];
            if (edge.source.file_path !== edge.target.file_path) paths.push(edges);
            queue.push({ node: edge.target, edges });
            if (visited.size >= 500) break;
        }
    }
    const areas = (edges: MapEvidence[]) => new Set([areaOf(entry.file_path!), ...edges.map(edge => areaOf(edge.target.file_path!))]).size;
    const seen = new Set<string>();
    return paths.sort((a, b) => areas(b) - areas(a) || a.length - b.length).filter(path => {
        const target = path.at(-1)!.target.file_path!; if (seen.has(target)) return false; seen.add(target); return true;
    }).slice(0, 3).map(edges => ({ edges, limited: cursor < queue.length }));
}
