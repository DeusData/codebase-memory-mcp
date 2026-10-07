import type { GraphDisplaySettings } from '../galaxy/density';
import { DEFAULT_GRAPH_DISPLAY } from '../galaxy/density';
import { useEdgeMotionPreference } from '../graph/edge-motion';
import { GALAXY_EDGE_LIMITS, GALAXY_NODE_LIMITS, useViewPreferences } from './view-preferences';

export interface BrowserConfigProps {
    project: string;
    display: GraphDisplaySettings;
    onDisplay: (display: GraphDisplaySettings) => void;
    onOpenBrowserModels: () => void;
    onOpenDisplay: () => void;
}

export default function BrowserConfig({ project, display, onDisplay, onOpenBrowserModels, onOpenDisplay }: BrowserConfigProps) {
    const { preferences, setPreferences, resetPreferences } = useViewPreferences(project);
    const motion = useEdgeMotionPreference();
    return <div className="cbm-config-browser">
        <section><h3>Graph & rendering</h3><p>Applies immediately to {project || 'this project'} in this browser.</p>
            <div className="cbm-config-control-grid">
                <label>Projection<select value={display.projection} onChange={event => onDisplay({ ...display, projection: event.target.value as GraphDisplaySettings['projection'] })}><option value="spatial">3D</option><option value="flat">Plan</option></select></label>
                <label>Edge visibility<select value={display.edges} onChange={event => onDisplay({ ...display, edges: event.target.value as GraphDisplaySettings['edges'] })}><option value="full">Full</option><option value="dim">Dim</option><option value="off">Off</option></select></label>
                <label>Frame limit<select value={display.frameCap} onChange={event => onDisplay({ ...display, frameCap: Number(event.target.value) })}><option value={0}>Uncapped</option><option value={60}>60 FPS</option><option value={30}>30 FPS</option></select></label>
                <label>Label range<select value={display.labelDistanceFactor} onChange={event => onDisplay({ ...display, labelDistanceFactor: Number(event.target.value) })}><option value={0}>All distances</option><option value={2}>Nearby</option><option value={1}>Close</option></select></label>
                <label><input type="checkbox" checked={display.halos} onChange={event => onDisplay({ ...display, halos: event.target.checked })} /> Node halos</label>
                <label><input type="checkbox" checked={display.bloom} onChange={event => onDisplay({ ...display, bloom: event.target.checked })} /> Bloom</label>
                <label><input type="checkbox" checked={motion.enabled} onChange={event => motion.setEnabled(event.target.checked)} /> Animated edges <small>All projects{motion.reduced ? ' · paused by reduced-motion preference' : ''}</small></label>
            </div>
            <button type="button" onClick={() => onDisplay({ ...DEFAULT_GRAPH_DISPLAY })}>Reset display</button>
        </section>
        <section><h3>Galaxy</h3><p>Render limits apply to exploration; indexed dependency completeness is reported in the view.</p>
            <div className="cbm-config-control-grid">
                <label>Node limit<select value={preferences.galaxyNodes} onChange={event => setPreferences({ galaxyNodes: Number(event.target.value) })}>{GALAXY_NODE_LIMITS.map(value => <option key={value} value={value}>{value.toLocaleString()}</option>)}</select></label>
                <label>Edge limit<select value={preferences.galaxyEdges} onChange={event => setPreferences({ galaxyEdges: Number(event.target.value) })}>{GALAXY_EDGE_LIMITS.map(value => <option key={value} value={value}>{value.toLocaleString()}</option>)}</select></label>
                <label><input type="checkbox" checked={preferences.coverageShadow} onChange={event => setPreferences({ coverageShadow: event.target.checked })} /> Coverage shadow</label>
            </div>
        </section>
        <section><h3>Architecture</h3><div className="cbm-config-control-grid">
            <label>Brick height<select value={preferences.brickHeight} onChange={event => setPreferences({ brickHeight: event.target.value as typeof preferences.brickHeight })}><option value="lines">Source size</option><option value="uniform">Uniform</option></select></label>
            <label>Brick color<select value={preferences.brickColor} onChange={event => setPreferences({ brickColor: event.target.value as typeof preferences.brickColor })}><option value="language">Language</option><option value="kind">Node kind</option></select></label>
            <label>Files<select value={preferences.fileVisibility} onChange={event => setPreferences({ fileVisibility: event.target.value as typeof preferences.fileVisibility })}><option value="all">All known files</option><option value="connected">With connections</option><option value="unconnected">Without known connections</option></select></label>
            <label><input type="checkbox" checked={preferences.hotspotGravity} onChange={event => setPreferences({ hotspotGravity: event.target.checked })} /> Hotspot gravity</label>
        </div><button type="button" onClick={resetPreferences}>Reset galaxy & architecture</button></section>
        <section><h3>Local agent</h3><p>Choose the browser model, load or unload its weights, and control automatic explanations. Model setup never runs automatically.</p><button type="button" onClick={onOpenBrowserModels}>Configure local agent →</button></section>
        <section><h3>Display details</h3><p>Performance measurements and experimental agent effects. Panel sizes can also be adjusted by dragging their dividers.</p><button type="button" onClick={onOpenDisplay}>More display controls →</button></section>
    </div>;
}
