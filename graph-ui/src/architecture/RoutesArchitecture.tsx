import { lazy, Suspense, useState, type ComponentProps } from 'react';
import SpatialArchitecture from './SpatialArchitecture';
import type { RoutesPerspective } from './architecture-history';
import { architectureText } from './strings';
import './container-map.css';
const ContainerMap = lazy(() => import('./ContainerMap'));
const text = architectureText.routesPerspective;

type Props = ComponentProps<typeof SpatialArchitecture> & {
    /** Service map or Endpoints, lifted to the workspace for Back and Forward (K27). */
    perspective?: RoutesPerspective;
    onPerspective?: (perspective: RoutesPerspective) => void;
};

/** The endpoint graph remains available independently of deployment metadata. */
export default function RoutesArchitecture({ perspective, onPerspective, ...props }: Props) {
    const [own, setOwn] = useState<RoutesPerspective>('services');
    const mode = perspective ?? own;
    const show = (next: RoutesPerspective) => {
        if (mode === next) return;
        props.onSelectionEvidence?.(undefined);
        if (onPerspective) onPerspective(next); else setOwn(next);
    };
    return <><nav className="container-mode" aria-label={text.label}>
        <button aria-pressed={mode === 'services'} onClick={() => show('services')}>{text.services}</button>
        <button aria-pressed={mode === 'endpoints'} onClick={() => show('endpoints')}>{text.endpoints}</button>
    </nav>{mode === 'services' ? <Suspense fallback={<p role="status">{text.preparing}</p>}><ContainerMap
        project={props.project} generation={props.generation} active={props.active} filter={props.filter} onNavigate={props.onNavigate} onClearSelection={props.onClearSelection} onSelectionEvidence={props.onSelectionEvidence}
        onShowEndpoints={() => show('endpoints')} /></Suspense>
        : <SpatialArchitecture {...props} />}</>;
}
