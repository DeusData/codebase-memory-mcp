import { lazy, Suspense, useState, type ComponentProps } from 'react';
import SpatialArchitecture from './SpatialArchitecture';
import './container-map.css';
const ContainerMap = lazy(() => import('./ContainerMap'));

/** The endpoint graph remains available independently of deployment metadata. */
export default function RoutesArchitecture(props: ComponentProps<typeof SpatialArchitecture>) {
    const [mode, setMode] = useState<'services' | 'endpoints'>('services');
    return <><nav className="container-mode" aria-label="Routes perspective">
        <button aria-pressed={mode === 'services'} onClick={() => { if (mode !== 'services') props.onSelectionEvidence?.(undefined); setMode('services'); }}>Service map</button>
        <button aria-pressed={mode === 'endpoints'} onClick={() => { if (mode !== 'endpoints') props.onSelectionEvidence?.(undefined); setMode('endpoints'); }}>Endpoints</button>
    </nav>{mode === 'services' ? <Suspense fallback={<p role="status">Preparing service map…</p>}><ContainerMap
        project={props.project} generation={props.generation} active={props.active} filter={props.filter} onNavigate={props.onNavigate} onClearSelection={props.onClearSelection} onSelectionEvidence={props.onSelectionEvidence} /></Suspense>
        : <SpatialArchitecture {...props} />}</>;
}
