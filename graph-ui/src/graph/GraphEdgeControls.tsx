import { useEffect, useRef } from 'react';
import { edgeColor } from './edge-style';
import { useEdgeMotionPreference } from './edge-motion';
import './edge-style.css';

const KEY = [
    ['CALLS', 'Calls'], ['IMPORTS', 'Imports'], ['USAGE', 'Usage'], ['DATA_FLOWS', 'Data flow'],
    ['READS', 'Reads'], ['WRITES', 'Writes'],
    ['HTTP_CALLS', 'HTTP'], ['ASYNC_CALLS', 'Async'], ['INHERITS', 'Inheritance'], ['IMPLEMENTS', 'Implementation'],
    ['CONTAINS', 'Containment'], ['DEFINES', 'Definitions'], ['CONFIGURES', 'Configuration'], ['RAISES', 'Errors'],
    ['CALL_REFERENCE', 'Call references'], ['TESTS', 'Tests'],
    ['SIMILAR_TO', 'Similarity'], ['RELATIONSHIPS', 'Mixed / unknown'],
];

export default function GraphEdgeControls() {
    const { enabled, reduced, setEnabled } = useEdgeMotionPreference();
    const details = useRef<HTMLDetailsElement>(null);
    useEffect(() => {
        const close = (event: PointerEvent) => {
            if (details.current?.open && !details.current.contains(event.target as Node)) details.current.open = false;
        };
        document.addEventListener('pointerdown', close);
        return () => document.removeEventListener('pointerdown', close);
    }, []);
    return <details ref={details} className="graph-edge-controls" onKeyDown={event => {
        if (event.key === 'Escape') { event.currentTarget.open = false; event.currentTarget.querySelector('summary')?.focus(); }
    }}>
        <summary title="3D graph edge colors and motion">Edges</summary>
        <div className="graph-edge-settings" role="group" aria-label="3D edge appearance">
            <label><input type="checkbox" checked={enabled && !reduced} disabled={reduced} onChange={event => setEnabled(event.target.checked)} />Direction pulse</label>
            <p>{reduced ? 'Motion is paused by your reduced-motion setting.' : 'A subtle pulse shows relationship direction across all 3D graphs.'}</p>
            <ul className="graph-edge-key" aria-label="Edge color key">{KEY.map(([type, label]) => <li key={type} title={type}><i style={{ backgroundColor: edgeColor(type) }} />{label}</li>)}</ul>
        </div>
    </details>;
}
