import { useEffect, useMemo, useRef, useState } from 'react';
import { RpcIntelligenceClient } from '../provider/rpc-client';
import type { SearchGraphHit } from '../provider/rpc-schemas';
import type { GraphScope } from './graph-scope';
import type { GraphNode } from './types';

export const FOCUS_GALAXY_SEARCH = 'cbm:focus-galaxy-search';
interface Choice { key: string; name: string; detail: string; kind: string; scope: GraphScope; node?: GraphNode }
/** Server hits also expose selectable file/folder clusters outside the layout. */
export function choicesFromSearchHits(hits: readonly SearchGraphHit[]): Choice[] {
    const choices = new Map<string, Choice>();
    for (const hit of hits) {
        if (hit.qualified_name) choices.set(hit.qualified_name, { key: hit.qualified_name, name: hit.name,
            detail: hit.file_path ?? hit.qualified_name, kind: hit.label ?? 'Symbol',
            scope: { kind: 'symbol', qualifiedName: hit.qualified_name, name: hit.name } });
        if (!hit.file_path) continue;
        const path = hit.file_path.replace(/\\/g, '/');
        choices.set(path, { key: path, name: path, kind: 'File', detail: 'Indexed file', scope: { kind: 'file', path, name: path } });
        const parts = path.split('/');
        for (let i = 1; i < parts.length; i++) {
            const folder = parts.slice(0, i).join('/');
            if (!folder) continue;
            choices.set(folder + '/', { key: folder + '/', name: folder + '/', kind: 'Folder', detail: 'Indexed folder',
                scope: { kind: 'folder', path: folder, name: folder + '/' } });
        }
    }
    return [...choices.values()];
}
/** One search for graph symbols and file/folder clusters, including unloaded symbols. */
export default function GalaxyNavigator({ nodes, onSelect, onSelectScope, project, fetch: fetchImpl, embedded = false }: {
    nodes: readonly GraphNode[]; onSelect: (node: GraphNode) => void; onSelectScope?: (scope: GraphScope) => void;
    project?: string; fetch?: typeof globalThis.fetch;
    embedded?: boolean;
}) {
    const [query, setQuery] = useState('');
    const [remote, setRemote] = useState<Choice[]>([]);
    const [status, setStatus] = useState('');
    const [pickerOpen, setPickerOpen] = useState(false);
    const container = useRef<HTMLDivElement>(null);
    const details = useRef<HTMLDetailsElement>(null), input = useRef<HTMLInputElement>(null);
    useEffect(() => {
        const focus = () => { if (details.current) details.current.open = true; setPickerOpen(true); input.current?.focus(); };
        window.addEventListener(FOCUS_GALAXY_SEARCH, focus);
        return () => window.removeEventListener(FOCUS_GALAXY_SEARCH, focus);
    }, []);
    useEffect(() => {
        if (!embedded || !pickerOpen) return;
        const dismiss = (event: PointerEvent) => {
            if (event.target instanceof Node && !container.current?.contains(event.target)) setPickerOpen(false);
        };
        window.addEventListener('pointerdown', dismiss);
        return () => window.removeEventListener('pointerdown', dismiss);
    }, [embedded, pickerOpen]);
    useEffect(() => {
        setRemote([]); setStatus('');
        if (!project || query.trim().length < 2 || (embedded && !pickerOpen)) return;
        const abort = new AbortController();
        const timer = setTimeout(() => {
            setStatus('Searching index…');
            const client = new RpcIntelligenceClient({ fetch: fetchImpl, signal: abort.signal });
            client.searchGraph(project, { query: query.trim(), limit: 12, signal: abort.signal }).then(result => {
                if (abort.signal.aborted) return;
                setRemote(choicesFromSearchHits(result.results));
                setStatus(result.total && result.total > 12 ? 'Top matches · refine to narrow' : '');
            }).catch(() => { if (!abort.signal.aborted) setStatus('Index search unavailable · loaded matches shown'); });
        }, 250);
        return () => { clearTimeout(timer); abort.abort(); };
    }, [project, query, fetchImpl, embedded, pickerOpen]);
    const matches = useMemo(() => {
        const needle = query.trim().toLocaleLowerCase();
        const choices: Choice[] = [], paths = new Map<string, number>();
        for (const node of nodes) {
            if (node.file_path) {
                paths.set(node.file_path, (paths.get(node.file_path) ?? 0) + 1);
                const parts = node.file_path.split('/');
                for (let i = 1; i < parts.length; i++) {
                    const directory = parts.slice(0, i).join('/') + '/'; paths.set(directory, (paths.get(directory) ?? 0) + 1);
                }
            }
            if (!needle || [node.name, node.qualified_name, node.file_path].some(value => value?.toLocaleLowerCase().includes(needle))) {
                choices.push({ key: node.qualified_name ?? `node:${node.id}`, name: node.name,
                    detail: node.file_path ?? node.qualified_name ?? node.label, kind: node.label,
                    scope: { kind: 'node', id: node.id, name: node.name, qualifiedName: node.qualified_name }, node });
            }
        }
        if (onSelectScope) for (const [path, count] of paths) if (needle && path.toLocaleLowerCase().includes(needle)) {
            const folder = path.endsWith('/');
            choices.push({ key: path, name: path, detail: `${count.toLocaleString()} loaded nodes`, kind: folder ? 'Folder' : 'File',
                scope: { kind: folder ? 'folder' : 'file', path: path.replace(/\/$/, ''), name: path } });
        }
        const priority = (choice: Choice) => choice.name.toLocaleLowerCase() === needle ? 0
            : choice.name.toLocaleLowerCase().startsWith(needle) ? 1 : choice.kind === 'Folder' || choice.kind === 'File' ? 2 : 3;
        const unique = new Map(choices.map(choice => [choice.key, choice]));
        for (const choice of remote) if (!unique.has(choice.key)) unique.set(choice.key, choice);
        return [...unique.values()].sort((a, b) => priority(a) - priority(b) || a.name.localeCompare(b.name)).slice(0, 12);
    }, [nodes, query, remote, onSelectScope]);
    const results = <>
        {status && <small role="status">{status}</small>}
        <ul>{matches.map(choice => <li key={choice.key}>
            <button type="button" onClick={() => {
                if (choice.node) onSelect(choice.node); else onSelectScope?.(choice.scope);
                if (details.current) details.current.open = false;
                setPickerOpen(false);
            }}><span className="atlas-graph-result-kind">{choice.kind}</span><strong>{choice.name}</strong><span>{choice.detail}</span></button>
        </li>)}</ul>
        {matches.length === 0 && <p>No matching indexed nodes.</p>}
    </>;
    if (embedded) return <div ref={container} className="atlas-galaxy-search-container" role="search" aria-label="Galaxy exploration"
        onKeyDown={event => {
            if (event.key === 'Escape') { event.stopPropagation(); setPickerOpen(false); input.current?.focus(); }
        }}>
        <input ref={input} type="search" aria-label="Find a graph node" placeholder="Find node, file or folder…"
            value={query} onFocus={() => setPickerOpen(true)} onChange={event => { setQuery(event.target.value); setPickerOpen(true); }} />
        {pickerOpen && <div className="atlas-galaxy-node-picker atlas-galaxy-search-results">{results}</div>}
    </div>;
    return <details ref={details} className="atlas-galaxy-navigator" onKeyDown={event => {
        if (event.key === 'Escape') { event.stopPropagation(); details.current!.open = false; details.current?.querySelector('summary')?.focus(); }
    }}>
        <summary>Find node or cluster</summary>
        <div className="atlas-galaxy-node-picker">
            <input ref={input} type="search" aria-label="Find a graph node" placeholder="Symbol, file, or folder…"
                value={query} onChange={event => setQuery(event.target.value)} />
            {results}
        </div>
    </details>;
}
