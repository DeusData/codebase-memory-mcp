import { useMemo, useRef, useState } from 'react';
import type { GraphNode } from './types';

/** Search the loaded graph without requiring a precise canvas click. */
export default function GalaxyNavigator({ nodes, onSelect }: {
    nodes: readonly GraphNode[];
    onSelect: (node: GraphNode) => void;
}) {
    const [query, setQuery] = useState('');
    const [entriesOnly, setEntriesOnly] = useState(true);
    const details = useRef<HTMLDetailsElement>(null);
    const matches = useMemo(() => {
        const needle = query.trim().toLocaleLowerCase();
        return nodes.filter(node => (!entriesOnly || node.status === 'entry')
            && (!needle || [node.name, node.qualified_name, node.file_path].some(value => value?.toLocaleLowerCase().includes(needle))));
    }, [nodes, query, entriesOnly]);
    return <details ref={details} className="atlas-galaxy-navigator" onKeyDown={event => {
        if (event.key === 'Escape') {
            event.stopPropagation();
            details.current!.open = false;
            details.current?.querySelector('summary')?.focus();
        }
    }}>
        <summary>Find node / entry point</summary>
        <div className="atlas-galaxy-node-picker">
            <input type="search" aria-label="Find a graph node" placeholder="Name, path, or symbol..."
                value={query} onChange={event => setQuery(event.target.value)} />
            <label><input type="checkbox" checked={entriesOnly}
                onChange={event => setEntriesOnly(event.target.checked)} />Entry points only</label>
            <p>{matches.length.toLocaleString()} matches in the loaded graph{matches.length > 8 ? '; showing the first 8' : ''}.</p>
            <ul>{matches.slice(0, 8).map(node => <li key={node.id}>
                <button type="button" onClick={() => { onSelect(node); if (details.current) details.current.open = false; }}>
                    <strong>{node.name}</strong><span>{node.file_path ?? node.qualified_name ?? node.label}</span>
                </button>
            </li>)}</ul>
            {matches.length === 0 && <p>No matching nodes in this layout. Try another name or include all nodes.</p>}
        </div>
    </details>;
}
