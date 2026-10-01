import type { EdgeKind } from './galaxy-legend';

/** This changes which relationships traversal follows, not just their paint. */
export function TraceEdgeFilter({ kinds, availableTypes, selected, onChange }: {
    kinds: readonly EdgeKind[];
    /** Preserve relationships outside render limits when toggling a visible type. */
    availableTypes?: readonly string[];
    selected?: readonly string[];
    onChange: (types: string[] | undefined) => void;
}) {
    const included = new Set(selected ?? availableTypes ?? kinds.map(kind => kind.type));
    const includedCount = kinds.filter(kind => included.has(kind.type)).length;
    return <details className="atlas-trace-edge-filter" onKeyDown={event => {
        if (event.key === 'Escape') {
            event.stopPropagation(); event.currentTarget.open = false;
            event.currentTarget.querySelector('summary')?.focus();
        }
    }}>
        <summary>Edge types · {selected === undefined ? 'All' : includedCount === 0 ? 'None' : includedCount}</summary>
        <div className="atlas-trace-edge-menu" role="group" aria-label="Trace edge types">
            <div className="atlas-trace-edge-actions">
                <button type="button" onClick={() => onChange(undefined)}>All types</button>
                <button type="button" onClick={() => onChange([])}>No types</button>
            </div>
            <small>Rendered edges · All types restores hidden types.</small>
            {kinds.length === 0 && <small>No edges in this view.</small>}
            {kinds.map(kind => <div className="atlas-trace-edge-row" key={kind.type}>
                <label><input type="checkbox" checked={included.has(kind.type)} onChange={() => {
                    const next = new Set(included);
                    if (!next.delete(kind.type)) next.add(kind.type);
                    onChange([...next].sort());
                }} /><i style={{ backgroundColor: kind.color }} aria-hidden="true" />{kind.type}</label>
                <span className="atlas-trace-edge-count" aria-label={`${kind.count.toLocaleString()} rendered ${kind.type} edges`}>{kind.count.toLocaleString()}</span>
                <button type="button" aria-label={`Only ${kind.type}`} onClick={() => onChange([kind.type])}>Only</button>
            </div>)}
        </div>
    </details>;
}
