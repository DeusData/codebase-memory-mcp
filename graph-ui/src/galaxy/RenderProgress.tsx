import { useEffect, useState } from 'react';
import './graph-exploration.css';

export interface RenderProgressProps {
    busy: boolean;
    label?: string;
}

/** Slow updates get feedback; fast updates leave the graph undisturbed. */
export default function RenderProgress({ busy, label = 'Updating view…' }: RenderProgressProps) {
    const [visible, setVisible] = useState(false);
    useEffect(() => {
        if (!busy) { setVisible(false); return; }
        const timer = window.setTimeout(() => setVisible(true), 300);
        return () => window.clearTimeout(timer);
    }, [busy]);

    if (!busy || !visible) return null;
    return <div className="atlas-graph-render-progress" role="status" aria-live="polite" aria-atomic="true">
        <span className="atlas-graph-render-spinner" aria-hidden="true" />
        <span>{label}</span>
    </div>;
}
