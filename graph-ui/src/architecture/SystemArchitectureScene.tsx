import { Component, useLayoutEffect, useMemo, useRef, useState, type CSSProperties, type ReactNode } from 'react';
import { Canvas, useFrame, useThree } from '@react-three/fiber';
import { Edges, Html, Line, OrbitControls, OrthographicCamera } from '@react-three/drei';
import { Box3, Color, MOUSE, TOUCH, OrthographicCamera as ThreeOrthographicCamera, QuadraticBezierCurve3, Quaternion, Vector3 } from 'three';
import type { SystemSceneEdge, SystemSceneLane, SystemSceneModel, SystemSceneNode } from './system-architecture-model';
import { edgeColor, isDirectedEdge, normalizeEdgeType } from '../graph/edge-style';
import { EdgePulseLayer } from '../graph/EdgePulseLayer';
import { useGraphBackgroundReset } from '../graph/useGraphBackgroundReset';
import { connectionLoad, type ConnectionLoad } from '../graph/connection-load';

/** The model keeps its stable XY layout; rendering places that footprint on XZ. */
const groundPosition = ([x, y, z]: [number, number, number]): Vector3 => new Vector3(x, z, -y);
const dimensions = (node: SystemSceneNode): [number, number, number] => {
    const [width, height, depth] = node.size ?? (node.component ? [11, 8, 3.6] : [3, 3, 1.8]);
    return [width, depth, height];
};
const center = (node: SystemSceneNode): Vector3 => groundPosition(node.position).add(new Vector3(0, node.size ? 0 : dimensions(node)[1] / 2, 0));
const activeNode = (model: SystemSceneModel, node: SystemSceneNode, path?: number) => !model.highlightActive
    || (path === undefined ? node.inCorridor : node.pathIndices?.includes(path));
const activeEdge = (model: SystemSceneModel, edge: SystemSceneEdge, path?: number) => !model.highlightActive
    || (path === undefined ? edge.inCorridor : edge.pathIndices?.includes(path));

interface CameraSnapshot { position: [number, number, number]; target: [number, number, number]; zoom: number; far: number; width: number; height: number }
const cameraViews = new Map<string, CameraSnapshot>();

/** Only explicit scope changes or resizing fit the camera; selection is deliberately absent. */
function ScopeCamera({ model, resetKey, planar, presentation }: { model: SystemSceneModel; resetKey: number; planar: boolean; presentation?: 'system' | 'journey' }) {
    const { camera, controls, size, invalidate } = useThree();
    const currentModel = useRef(model); currentModel.current = model;
    const previous = useRef<{ key: string; reset: number; width: number; height: number } | undefined>(undefined);
    const key = `${model.scopeKey ?? 'legacy'}:focus:${model.focusId ?? ''}:projection:${planar ? 'plan' : '3d'}`;
    useLayoutEffect(() => {
        const orbit = controls as unknown as { target: Vector3; update: () => void } | undefined;
        if (!orbit || !(camera instanceof ThreeOrthographicCamera) || size.width < 1 || size.height < 1) return;
        const last = previous.current;
        if (last && last.key !== key) {
            cameraViews.set(last.key, { position: camera.position.toArray() as [number, number, number], target: orbit.target.toArray() as [number, number, number], zoom: camera.zoom, far: camera.far, width: last.width, height: last.height });
            if (cameraViews.size > 24) cameraViews.delete(cameraViews.keys().next().value!);
        }
        const stored = last?.reset === resetKey ? cameraViews.get(key) : undefined;
        previous.current = { key, reset: resetKey, width: size.width, height: size.height };
        camera.up.set(0, planar ? 0 : 1, planar ? -1 : 0);
        if (stored && last?.key !== key && stored.width === size.width && stored.height === size.height) {
            camera.position.fromArray(stored.position); camera.zoom = stored.zoom; camera.far = stored.far; camera.updateProjectionMatrix();
            orbit.target.fromArray(stored.target); orbit.update(); invalidate(); return;
        }
        const scope = currentModel.current;
        const focus = scope.nodes.find(node => node.id === scope.focusId);
        const targets = focus ? scope.nodes.filter(node => node.id === focus.id || node.parentId === focus.id) : scope.nodes;
        const box = new Box3();
        for (const node of targets) {
            const location = center(node), half = new Vector3(...dimensions(node)).multiplyScalar(0.5).add(new Vector3(6, 6, 6));
            box.expandByPoint(location.clone().sub(half));
            box.expandByPoint(location.clone().add(half));
            box.expandByPoint(new Vector3(...labelPosition(node)));
        }
        if (!focus) for (const lane of scope.lanes ?? []) {
            const location = groundPosition(lane.position), half = new Vector3(lane.width / 2 + 2, lane.depth / 2 + 1, (lane.height ?? 24) / 2 + 2);
            box.expandByPoint(location.clone().sub(half));
            box.expandByPoint(location.clone().add(half));
        }
        if (box.isEmpty()) box.set(new Vector3(-12, -2, -12), new Vector3(12, 5, 12));
        const target = box.getCenter(new Vector3()), reach = Math.max(30, box.getSize(new Vector3()).length());
        camera.position.copy(target).addScaledVector(new Vector3(...(planar ? [0, 1, 0] : presentation === 'journey' ? [0.1, 1.6, 0.9] : [0.8, 1.15, 1.05]) as [number, number, number]).normalize(), reach * 2);
        camera.near = 0.1; camera.far = Math.max(1000, reach * 6);
        camera.lookAt(target); camera.updateMatrixWorld();
        const projected = new Box3();
        for (const x of [box.min.x, box.max.x]) for (const y of [box.min.y, box.max.y]) for (const z of [box.min.z, box.max.z]) {
            projected.expandByPoint(new Vector3(x, y, z).applyMatrix4(camera.matrixWorldInverse));
        }
        const span = projected.getSize(new Vector3());
        // Labels use CSS pixels, so reserve margins after projecting the scene bounds.
        camera.zoom = Math.max(0.15, Math.min(90, Math.max(60, size.width - 160) / Math.max(1, span.x), Math.max(60, size.height - 120) / Math.max(1, span.y)));
        camera.updateProjectionMatrix(); orbit.target.copy(target); orbit.update(); invalidate();
    }, [camera, controls, invalidate, key, planar, resetKey, size.width, size.height, presentation]);
    return null;
}

/** Folder backplates are visual containment, never selectable nodes or relationships. */
function FolderBackplate({ lane }: { lane: SystemSceneLane }) {
    const height = lane.height ?? 24;
    return <group position={groundPosition(lane.position)}>
        <mesh raycast={() => null}>
            <boxGeometry args={[lane.width, lane.depth, height]} />
            <meshStandardMaterial color="#536977" roughness={0.95} transparent opacity={0.09} depthWrite={false} />
            <Edges color="#6e8a98" transparent opacity={0.24} />
        </mesh>
        <Html position={[-lane.width / 2 + 3, lane.depth / 2 + 0.2, -height / 2 + 3]} zIndexRange={[5, 1]} style={{ pointerEvents: 'none' }}>
            <span className="system-scene-lane-label" data-folder-id={lane.id} title={lane.label}>{lane.label}</span>
        </Html>
    </group>;
}

/** Source-to-target geometry is shared by the base edge and the GPU pulse batch. */
function connectionGeometry(source: SystemSceneNode, target: SystemSceneNode, offset: number) {
    const start = center(source), end = center(target), sourceSize = dimensions(source);
    start.y += sourceSize[1] / 2 + 0.4;
    end.y += dimensions(target)[1] / 2 + 0.4;
    let control: Vector3;
    if (source.id === target.id) {
        start.add(new Vector3(sourceSize[0] / 2 + 0.3, 0, sourceSize[2] / 4));
        end.add(new Vector3(sourceSize[0] / 4, 0, sourceSize[2] / 2 + 0.3));
        control = center(source).add(new Vector3(sourceSize[0] / 2 + 13, sourceSize[1] / 2 + 8, sourceSize[2] / 2 + 13))
            .addScaledVector(new Vector3(1, 0.3, 1).normalize(), offset);
    } else {
        const direction = end.clone().sub(start).setY(0), normal = direction.clone().normalize();
        const reach = (node: SystemSceneNode) => Math.min(dimensions(node)[0] / 2 / Math.max(1e-6, Math.abs(normal.x)), dimensions(node)[2] / 2 / Math.max(1e-6, Math.abs(normal.z))) + 0.5;
        const from = reach(source), to = reach(target), factor = Math.min(1, Math.max(0, direction.length() - 1) / (from + to));
        start.addScaledVector(normal, from * factor); end.addScaledVector(normal, -to * factor);
        const side = new Vector3(-direction.z, 0, direction.x).normalize().multiplyScalar(offset * (source.id < target.id ? 1 : -1));
        control = start.clone().lerp(end, 0.5).add(side).add(new Vector3(0, Math.min(10, 3.5 + direction.length() * 0.035), 0));
    }
    const curve = new QuadraticBezierCurve3(start, control, end);
    return { curve, points: curve.getPoints(32), arrow: curve.getPoint(0.76), rotation: new Quaternion().setFromUnitVectors(new Vector3(0, 1, 0), curve.getTangent(0.76).normalize()) };
}

interface ConnectionStrand { type: string; geometry: ReturnType<typeof connectionGeometry> }

function Connection({ edge, strands, selected, dimmed, emphasized, onSelect }: {
    edge: SystemSceneEdge; strands: ConnectionStrand[]; selected: boolean;
    dimmed: boolean; emphasized: boolean; onSelect: () => void;
}) {
    const internal = edge.source === edge.target;
    const opacity = selected ? 0.98 : dimmed ? 0.065 : emphasized ? 0.86 : internal ? 0.15 : 0.34;
    return <group onClick={event => { event.stopPropagation(); onSelect(); }}>
        {strands.map(({ type, geometry }) => <group key={type}>
            <Line points={geometry.points} color={edgeColor(type)} lineWidth={selected ? 2 : emphasized ? 1.6 : 0.7}
                transparent opacity={opacity} depthWrite={false} />
            {isDirectedEdge(type) && <mesh position={geometry.arrow} quaternion={geometry.rotation}>
                <coneGeometry args={[selected || emphasized ? 0.85 : 0.62, 2.2, 7]} />
                <meshBasicMaterial color={edgeColor(type)} transparent opacity={opacity} depthWrite={false} />
            </mesh>}
        </group>)}
        {selected && <Html position={strands[0].geometry.curve.getPoint(0.5).lerp(strands.at(-1)!.geometry.curve.getPoint(0.5), 0.5)} center zIndexRange={[15, 10]} style={{ pointerEvents: 'none' }}>
            <span className="system-scene-edge-label" style={{ borderColor: edgeColor(edge.type, edge.types) }}>{internal ? 'Within group · ' : ''}{edge.types?.length ? edge.types.join(' · ').replaceAll('_', ' ') : edge.type.replaceAll('_', ' ')} · {edge.count}</span>
        </Html>}
    </group>;
}

const labelPosition = (node: SystemSceneNode): [number, number, number] => {
    const point = center(node), size = dimensions(node);
    return [point.x, point.y + size[1] / 2 + (node.parentId ? 2.6 : 4), point.z - (node.expanded ? size[2] / 2 + 2 : 0)];
};

/** Corridor labels take priority; neither hovering nor ordinary selection changes membership. */
function NodeLabels({ model, selectedNode, highlightedPathIndex, onSelect, onExpand, loads, presentation }: {
    model: SystemSceneModel; selectedNode?: string; highlightedPathIndex?: number;
    onSelect: (id: string) => void; onExpand?: (id: string) => void;
    loads?: Map<string, ConnectionLoad>; presentation?: 'system' | 'journey';
}) {
    const { camera, size, gl } = useThree(), previous = useRef('');
    const [visible, setVisible] = useState<Set<string>>(new Set());
    const [captionHeight, setCaptionHeight] = useState(36);
    const ordered = useMemo(() => [...model.nodes].sort((a, b) => (model.highlightActive
        ? Number(Boolean(activeNode(model, b, highlightedPathIndex))) - Number(Boolean(activeNode(model, a, highlightedPathIndex))) : 0)
        || Number(Boolean(a.parentId)) - Number(Boolean(b.parentId)) || a.id.localeCompare(b.id)), [model, highlightedPathIndex]);
    useLayoutEffect(() => {
        const caption = gl.domElement.closest('.system-map')?.querySelector<HTMLElement>('.system-map-caption');
        if (!caption) return;
        const measure = () => setCaptionHeight(Math.ceil(caption.getBoundingClientRect().height) + 4);
        measure();
        const observer = new ResizeObserver(measure); observer.observe(caption);
        return () => observer.disconnect();
    }, [gl]);
    useFrame(() => {
        const boxes: { left: number; right: number; top: number; bottom: number }[] = [], ids: string[] = [];
        const zoom = 'zoom' in camera ? Number(camera.zoom) : 1;
        for (const node of ordered) {
            if (node.parentId && zoom < 5) continue;
            const projected = new Vector3(...labelPosition(node)).project(camera);
            if (projected.z < -1 || projected.z > 1) continue;
            const x = (projected.x + 1) * size.width / 2, y = (1 - projected.y) * size.height / 2;
            const compact = node.kind === 'group' && !node.expanded;
            const width = presentation === 'journey' ? 116 : compact ? 76 : node.parentId ? 110 : node.expanded ? 126 : 96, height = presentation === 'journey' ? 58 : compact ? 24 : 28;
            const box = { left: x - width / 2, right: x + width / 2, top: y - height / 2, bottom: y + height / 2 };
            if (box.left < 2 || box.right > size.width - 2 || box.top < 2 || box.bottom > size.height - captionHeight) continue;
            if (boxes.some(other => box.left < other.right && box.right > other.left && box.top < other.bottom && box.bottom > other.top)) continue;
            boxes.push(box); ids.push(node.id);
        }
        const key = ids.join('|');
        if (key !== previous.current) { previous.current = key; setVisible(new Set(ids)); }
    });
    return <>{ordered.filter(node => visible.has(node.id)).map(node => <Html key={node.id}
        position={labelPosition(node)} center zIndexRange={[10, 6]}>
        <button className="system-scene-node-label" data-presentation={presentation} data-kind={node.kind} data-expanded={node.expanded} data-child={Boolean(node.parentId)}
            data-selected={selectedNode === node.id} data-focus={model.focusId === node.id}
            data-node-id={node.id} data-position={center(node).toArray().join(',')} data-size={dimensions(node).join(',')}
            data-color={node.visual?.color} data-color-label={node.visual?.label}
            data-connection-load={loads?.get(node.id)?.links}
            style={{ '--system-node-tint': node.visual?.color ?? '#91a6b4' } as CSSProperties}
            data-muted={!activeNode(model, node, highlightedPathIndex)} title={`${node.label} · ${node.detail}${loads ? ` · ${loads.get(node.id)?.links ?? 0} visible links · ${loads.get(node.id)?.neighbors ?? 0} connected items` : ''}${node.visual ? ` · ${node.visual.label}${node.visual.basis ? ` (${node.visual.basis})` : ''}` : ''}`}
            type="button" aria-label={`${node.label} · ${node.detail}`} aria-pressed={selectedNode === node.id}
            onClick={() => onSelect(node.id)} onDoubleClick={onExpand && (node.group || presentation === 'journey') ? () => onExpand(node.id) : undefined}>
            {presentation === 'journey' && <small>{node.depth === 0 ? 'START' : node.pathIndices?.length ? `HOP ${node.depth}` : 'POSSIBLE CALL'}</small>}
            <strong>{node.label}</strong>
            {presentation === 'journey' && <small>{node.symbol?.file_path?.split('/').at(-1) ?? 'Source unavailable'}</small>}
        </button>
    </Html>)}</>;
}

class SceneBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
    state = { failed: false };
    static getDerivedStateFromError() { return { failed: true }; }
    render() { return this.state.failed ? <div className="system-scene-empty" role="status">3D is unavailable. Components and source evidence remain accessible below.</div> : this.props.children; }
}

export default function SystemArchitectureScene({ model, selectedNode, selectedEdge, highlightedPathIndex, onSelectNode, onSelectEdge, onExpandNode, onClearSelection, resetKey, planar = false, active = true, showConnectionLoad = true, presentation = 'system' }: {
    model: SystemSceneModel; selectedNode?: string; selectedEdge?: string; highlightedPathIndex?: number;
    onSelectNode: (id: string) => void; onSelectEdge: (id: string) => void; onExpandNode?: (id: string) => void; onClearSelection?: () => void; resetKey: number; planar?: boolean; active?: boolean; showConnectionLoad?: boolean; presentation?: 'system' | 'journey';
}) {
    const nodes = useMemo(() => new Map(model.nodes.map(node => [node.id, node])), [model.nodes]);
    const activePath = highlightedPathIndex ?? model.preferredPathIndex;
    const loads = useMemo(() => connectionLoad(model.nodes, model.edges), [model.nodes, model.edges]);
    const neighborhood = useMemo(() => {
        const ids = new Set<string>();
        if (selectedNode && nodes.has(selectedNode)) ids.add(selectedNode);
        for (const edge of model.edges) if (edge.id === selectedEdge || edge.source === selectedNode || edge.target === selectedNode) {
            ids.add(edge.source); ids.add(edge.target);
        }
        return ids;
    }, [model.edges, nodes, selectedNode, selectedEdge]);
    const focusedEdge = (edge: SystemSceneEdge) => !neighborhood.size || edge.id === selectedEdge || edge.source === selectedNode || edge.target === selectedNode;
    const offsets = useMemo(() => {
        const pairs = new Map<string, SystemSceneEdge[]>(), result = new Map<string, number>();
        model.edges.forEach(edge => { const key = JSON.stringify([edge.source, edge.target].sort()); pairs.set(key, [...(pairs.get(key) ?? []), edge]); });
        pairs.forEach(edges => edges.sort((a, b) => a.id.localeCompare(b.id)).forEach((edge, index) => result.set(edge.id, (index - (edges.length - 1) / 2) * 7)));
        return result;
    }, [model.edges]);
    const edgeStrands = useMemo(() => {
        const result = new Map<string, ConnectionStrand[]>();
        for (const edge of model.edges) {
            const source = nodes.get(edge.source), target = nodes.get(edge.target);
            if (!source || !target) continue;
            const types = [...new Set((edge.types?.length ? edge.types : [edge.type]).map(normalizeEdgeType).filter(Boolean))].sort();
            if (!types.length) types.push(normalizeEdgeType(edge.type));
            result.set(edge.id, types.map((type, index) => ({ type,
                geometry: connectionGeometry(source, target, (offsets.get(edge.id) ?? 0) + (index - (types.length - 1) / 2) * 4.5) })));
        }
        return result;
    }, [model.edges, nodes, offsets]);
    const pulsePaths = useMemo(() => model.edges.flatMap(edge => {
        const dimmed = !activeEdge(model, edge, activePath) || !focusedEdge(edge), emphasized = (model.highlightActive || neighborhood.size > 0) && !dimmed;
        return (edgeStrands.get(edge.id) ?? []).filter(strand => isDirectedEdge(strand.type)).map(({ type, geometry }) => ({ id: `${edge.id}:${type}`, type, points: geometry.points,
            opacity: edge.id === selectedEdge ? 0.95 : dimmed ? 0.04 : emphasized ? 0.8 : edge.source === edge.target ? 0.22 : 0.65 }));
    }), [model, edgeStrands, activePath, selectedEdge, selectedNode, neighborhood]);
    const background = useGraphBackgroundReset(onClearSelection);
    const floorY = Math.min(0, ...model.nodes.map(node => center(node).y - dimensions(node)[1] / 2), ...(model.lanes ?? []).map(lane => lane.position[2] - lane.depth / 2)) - 0.2;
    const extent = Math.max(120, ...model.nodes.map(node => Math.max(Math.abs(node.position[0]) + dimensions(node)[0] / 2, Math.abs(node.position[1]) + dimensions(node)[2] / 2) * 2 + 40));
    return <SceneBoundary><div className="system-scene" data-testid="system-scene" data-projection={planar ? 'plan' : '3d'}>
        <Canvas {...background} frameloop={active ? 'demand' : 'never'} dpr={[1, 1.5]} gl={{ antialias: true, alpha: false, powerPreference: 'low-power' }}
            role="img" aria-label={presentation === 'journey' ? 'Behavior call journey. Follow recorded invocations across component lanes. Double-click an operation to explore its calls.' : `System ${planar ? 'plan' : '3D map'}. Groups lie on a ground plane; expanded members sit above their group. Use the adjacent list to inspect source evidence.`}>
            <color attach="background" args={['#0b1118']} />
            <OrthographicCamera makeDefault position={[80, 115, 105]} zoom={4} near={0.1} far={10000} />
            <ambientLight intensity={1.6} /><directionalLight position={[30, 80, 20]} intensity={2.4} color="#dcfff3" />
            <directionalLight position={[-20, 20, -30]} intensity={1.2} color="#a899ff" />
            <gridHelper args={[extent, Math.min(80, Math.ceil(extent / 12)), '#1c3440', '#15242e']} position={[0, floorY, 0]} raycast={() => null} />
            {(model.lanes ?? []).map(lane => <FolderBackplate key={lane.id} lane={lane} />)}
            {showConnectionLoad && model.nodes.map(node => {
                const load = loads.get(node.id)!;
                if (!load.links || node.expanded) return null;
                const position = center(node), size = dimensions(node);
                const muted = !activeNode(model, node, activePath) || (neighborhood.size > 0 && !neighborhood.has(node.id));
                return <mesh key={`load:${node.id}`} position={[position.x, position.y - size[1] / 2 - 0.1, position.z]}
                    rotation={[-Math.PI / 2, 0, 0]} scale={[size[0] * 0.56, size[2] * 0.62, 1]} raycast={() => null}>
                    <ringGeometry args={[1, Math.sqrt(1 + load.strength * 1.1), 48]} />
                    <meshBasicMaterial color="#91a6b4" transparent opacity={muted ? 0.025 : 0.08 + load.strength * 0.12} depthWrite={false} />
                </mesh>;
            })}
            {model.edges.map(edge => {
                const strands = edgeStrands.get(edge.id);
                return strands ? <Connection key={edge.id} edge={edge} strands={strands}
                    selected={selectedEdge === edge.id} dimmed={!activeEdge(model, edge, activePath) || !focusedEdge(edge)}
                    emphasized={Boolean((model.highlightActive || neighborhood.size > 0) && activeEdge(model, edge, activePath) && focusedEdge(edge))}
                    onSelect={() => onSelectEdge(edge.id)} /> : null;
            })}
            <EdgePulseLayer paths={pulsePaths} active={active} />
            {model.nodes.map(node => {
                const selected = selectedNode === node.id, active = activeNode(model, node, activePath) && (!neighborhood.size || neighborhood.has(node.id)), size = dimensions(node);
                const color = node.visual?.color ?? (node.kind === 'remainder' ? '#9c9a8b' : '#91a6b4');
                const fill = new Color('#27333e').lerp(new Color(color), selected ? 0.34 : 0.2);
                return <mesh key={node.id} position={center(node)} onClick={event => { event.stopPropagation(); onSelectNode(node.id); }}
                    onDoubleClick={onExpandNode && (node.group || presentation === 'journey') ? event => { event.stopPropagation(); onExpandNode(node.id); } : undefined}>
                    <boxGeometry args={size} />
                    <meshStandardMaterial color={fill} roughness={0.85} metalness={0.05} emissive={color} emissiveIntensity={selected ? 0.05 : 0.008}
                        transparent opacity={selected ? 0.6 : !active ? 0.045 : node.expanded ? 0.065 : node.parentId ? 0.36 : 0.28} depthWrite={false} />
                    <Edges color={selected ? '#f0f6fa' : color} transparent opacity={selected ? 0.95 : !active ? 0.12 : node.expanded ? 0.36 : 0.46} />
                </mesh>;
            })}
            <NodeLabels model={model} selectedNode={selectedNode} highlightedPathIndex={activePath} onSelect={onSelectNode} onExpand={onExpandNode} loads={showConnectionLoad ? loads : undefined} presentation={presentation} />
            <OrbitControls makeDefault enabled={active} enableRotate={!planar} enableDamping={false}
                mouseButtons={{ LEFT: planar ? MOUSE.PAN : MOUSE.ROTATE, MIDDLE: MOUSE.DOLLY, RIGHT: MOUSE.PAN }}
                touches={{ ONE: planar ? TOUCH.PAN : TOUCH.ROTATE, TWO: TOUCH.DOLLY_PAN }}
                minZoom={0.15} maxZoom={90} rotateSpeed={0.5} zoomSpeed={0.8} maxPolarAngle={Math.PI / 2.05} />
            <ScopeCamera model={model} resetKey={resetKey} planar={planar} presentation={presentation} />
        </Canvas>
    </div></SceneBoundary>;
}
