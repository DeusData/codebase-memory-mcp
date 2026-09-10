import { useEffect, useMemo, useRef, useState, type RefObject } from 'react';
import { Canvas, useThree } from '@react-three/fiber';
import { Edges, Html, OrbitControls, OrthographicCamera } from '@react-three/drei';
import { Box3, CubicBezierCurve3, MOUSE, TOUCH, OrthographicCamera as ThreeOrthographicCamera, Quaternion, QuadraticBezierCurve3, Vector3 } from 'three';
import type { OrbitControls as OrbitControlsImpl } from 'three-stdlib';
import type { SemanticEdge, SemanticGraph, SemanticNode } from './semantic-graph';
import './architecture-scene.css';

export interface ArchitectureSceneProps {
    model: SemanticGraph;
    selectedId?: string;
    selectedEdgeId?: string;
    onSelect: (id: string) => void;
    onSelectEdge: (id: string) => void;
    active?: boolean;
    planar?: boolean;
    resetKey?: number;
}

const PALETTE = { area: '#69e5bc', file: '#8ccff1', symbol: '#c2afff', route: '#ffc98b' } as const;
const EDGE_COLORS: Record<string, string> = {
    CALLS: '#a699ed', IMPORTS: '#65bfdb', HTTP_CALLS: '#f4bd7b', ASYNC_CALLS: '#f4bd7b',
    HANDLES: '#70dbb3', DATA_FLOWS: '#e297bd', INHERITS: '#8ca8ff', IMPLEMENTS: '#8ca8ff',
};

function dimensions(node: SemanticNode): [number, number, number] {
    if (node.kind === 'area') return [12, 0.9, 8];
    if (node.kind === 'file') return [5.5, 2.5, 4];
    if (node.kind === 'route') return [4.5, 2.5, 4.5];
    return [3.2, 3.2, 3.2];
}

function centerOf(node: SemanticNode): Vector3 {
    return new Vector3(...node.position).add(new Vector3(0, dimensions(node)[1] / 2, 0));
}

/** Direction is geometric only; the model supplies every relationship and its evidence. */
function Relationship({ edge, source, target, selected, emphasized, dimmed, showLabel, laneOffset, onSelect, onHover }: {
    edge: SemanticEdge; source: SemanticNode; target: SemanticNode;
    selected: boolean; emphasized: boolean; dimmed: boolean; showLabel: boolean;
    laneOffset: number;
    onSelect: () => void; onHover: (id?: string) => void;
}) {
    const { curve, arrowPosition, arrowRotation, labelPosition } = useMemo(() => {
        const start = centerOf(source);
        const end = centerOf(target);
        const delta = end.clone().sub(start);
        const distance = delta.length();
        // A small sideways bow separates reciprocal edges. Height is routing clearance,
        // never a measurement or an extra dependency.
        const perpendicular = new Vector3(-delta.z, 0, delta.x).normalize();
        const midpoint = start.clone().lerp(end, 0.5)
            .add(new Vector3(0, Math.min(10, 3 + distance * 0.1), 0))
            .addScaledVector(perpendicular, Math.min(4, distance * 0.09) + laneOffset);
        const curve = edge.source === edge.target
            ? new CubicBezierCurve3(start.clone().add(new Vector3(1, 1, 0)), start.clone().add(new Vector3(10 + laneOffset, 10, 7)), start.clone().add(new Vector3(-10 - laneOffset, 10, 7)), end.clone().add(new Vector3(-1, 1, 0)))
            : new QuadraticBezierCurve3(start, midpoint, end);
        const arrowAt = 0.79;
        return {
            curve,
            arrowPosition: curve.getPoint(arrowAt),
            arrowRotation: new Quaternion().setFromUnitVectors(new Vector3(0, 1, 0), curve.getTangent(arrowAt).normalize()),
            labelPosition: curve.getPoint(0.5),
        };
    }, [source, target, edge.source, edge.target, laneOffset]);
    const color = EDGE_COLORS[edge.type] ?? '#85a9b4';
    const opacity = dimmed ? 0.06 : selected ? 1 : emphasized ? 0.95 : 0.55;
    return <group>
        <mesh onClick={event => { event.stopPropagation(); onSelect(); }}
            onPointerOver={event => { event.stopPropagation(); onHover(edge.id); }}
            onPointerOut={() => onHover()}>
            <tubeGeometry args={[curve, 24, 0.34, 4, false]} />
            <meshBasicMaterial transparent opacity={0} depthWrite={false} />
        </mesh>
        <mesh raycast={() => null}>
            <tubeGeometry args={[curve, 24, selected ? 0.11 : emphasized ? 0.085 : 0.055, 4, false]} />
            <meshBasicMaterial color={color} transparent opacity={opacity} depthWrite={false} />
        </mesh>
        <mesh position={arrowPosition} quaternion={arrowRotation}
            onClick={event => { event.stopPropagation(); onSelect(); }}>
            <coneGeometry args={[selected || emphasized ? 0.42 : 0.28, 1.1, 6]} />
            <meshBasicMaterial color={color} transparent opacity={Math.min(1, opacity + 0.18)} />
        </mesh>
        {showLabel && !dimmed && <Html position={labelPosition} center zIndexRange={[30, 20]}>
            <button type="button" className="architecture-edge-label" onClick={onSelect}
                style={{ borderColor: color }} aria-label={`Inspect ${edge.type} relationship, ${edge.count} indexed edges`}>
                {edge.type.replaceAll('_', ' ').toLowerCase()}{edge.count > 1 && <span> ×{edge.count}</span>}
            </button>
        </Html>}
    </group>;
}

function ArchitectureNode({ node, selected, highlighted, dimmed, onSelect, onHover }: {
    node: SemanticNode; selected: boolean; highlighted: boolean; dimmed: boolean;
    onSelect: () => void; onHover: (id?: string) => void;
}) {
    const color = PALETTE[node.kind];
    const size = dimensions(node);
    const labelY = size[1] + 7;
    return <group position={node.position}>
        <mesh position={[0, size[1] / 2, 0]}
            onClick={event => { event.stopPropagation(); onSelect(); }}
            onPointerOver={event => { event.stopPropagation(); onHover(node.id); }}
            onPointerOut={() => onHover()}>
            {node.kind === 'symbol' ? <icosahedronGeometry args={[1.6, 1]} />
                : node.kind === 'route' ? <cylinderGeometry args={[2.2, 2.2, size[1], 6]} />
                    : <boxGeometry args={size} />}
            <meshStandardMaterial color={selected ? color : '#17232c'} roughness={0.7} metalness={0.22}
                emissive={color} emissiveIntensity={selected ? 0.3 : highlighted ? 0.13 : 0.035}
                transparent opacity={dimmed ? 0.28 : 0.95} />
            <Edges color={color} transparent opacity={dimmed ? 0.12 : selected ? 1 : highlighted ? 0.85 : 0.48} />
        </mesh>
        {selected && <mesh position={[0, 0.05, 0]} rotation={[-Math.PI / 2, 0, 0]} raycast={() => null}>
            <ringGeometry args={[Math.max(size[0], size[2]) * 0.65, Math.max(size[0], size[2]) * 0.65 + 0.12, 64]} />
            <meshBasicMaterial color={color} transparent opacity={0.6} depthWrite={false} />
        </mesh>}
        <Html position={[0, labelY, 0]} center zIndexRange={selected ? [60, 50] : [20, 10]}>
            <button type="button" className={`architecture-node-label${selected ? ' is-selected' : ''}${dimmed ? ' is-dimmed' : ''}`}
                data-kind={node.kind} aria-pressed={selected} onClick={onSelect}
                onPointerEnter={() => onHover(node.id)} onPointerLeave={() => onHover()}
                title={`${node.label}\n${node.detail}`}>
                {selected && <span className="architecture-node-kind"><i style={{ background: color }} />{node.kind === 'area' ? 'Source area' : node.kind}</span>}
                <strong>{node.label}</strong>
                {selected && <span className="architecture-node-detail">{node.detail}</span>}
            </button>
        </Html>
    </group>;
}

function FitArchitecture({ model, planar, resetKey, controls, active }: {
    model: SemanticGraph; planar: boolean; resetKey: number;
    controls: RefObject<OrbitControlsImpl | null>; active: boolean;
}) {
    const { camera, size, invalidate } = useThree();
    const layoutKey = model.nodes.map(node => `${node.id}:${node.position.join(',')}`).join('|');
    const nodesRef = useRef(model.nodes);
    nodesRef.current = model.nodes;
    useEffect(() => {
        if (!(camera instanceof ThreeOrthographicCamera) || size.width < 1 || size.height < 1) return;
        const bounds = new Box3();
        for (const node of nodesRef.current) {
            const center = centerOf(node);
            const half = new Vector3(...dimensions(node)).multiplyScalar(0.5).addScalar(2);
            bounds.expandByPoint(center.clone().sub(half));
            bounds.expandByPoint(center.clone().add(half));
        }
        if (bounds.isEmpty()) bounds.set(new Vector3(-12, -2, -12), new Vector3(12, 5, 12));
        const target = bounds.getCenter(new Vector3());
        const reach = Math.max(30, bounds.getSize(new Vector3()).length());
        camera.up.set(0, planar ? 0 : 1, planar ? -1 : 0);
        camera.position.copy(target).addScaledVector(new Vector3(...(planar ? [0, 1, 0] : [0.8, 1.15, 1.05]) as [number, number, number]).normalize(), reach * 2);
        camera.near = 0.1;
        camera.far = Math.max(1000, reach * 6);
        camera.lookAt(target);
        camera.updateMatrixWorld();
        const projected = new Box3();
        for (const x of [bounds.min.x, bounds.max.x]) for (const y of [bounds.min.y, bounds.max.y]) for (const z of [bounds.min.z, bounds.max.z]) {
            projected.expandByPoint(new Vector3(x, y, z).applyMatrix4(camera.matrixWorldInverse));
        }
        const span = projected.getSize(new Vector3());
        // Keep readable labels inside the canvas, including its narrow inspector layout.
        camera.zoom = Math.max(0.4, Math.min(Math.max(80, size.width - 160) / Math.max(1, span.x), Math.max(80, size.height - 150) / Math.max(1, span.y)));
        camera.updateProjectionMatrix();
        if (controls.current) {
            controls.current.target.copy(target);
            controls.current.update();
        }
        invalidate();
    }, [camera, controls, invalidate, layoutKey, model.scopeKey, planar, resetKey, size.width, size.height]);
    useEffect(() => { if (active) invalidate(); }, [active, invalidate]);
    return null;
}

export function ArchitectureScene({ model, selectedId, selectedEdgeId, onSelect, onSelectEdge, active = true, planar = false, resetKey = 0 }: ArchitectureSceneProps) {
    const [hoveredId, setHoveredId] = useState<string>();
    const [hoveredEdgeId, setHoveredEdgeId] = useState<string>();
    const controls = useRef<OrbitControlsImpl | null>(null);
    const nodesById = useMemo(() => new Map(model.nodes.map(node => [node.id, node])), [model.nodes]);
    const edgeOffsets = useMemo(() => {
        const lanes = new Map<string, number>(); const offsets = new Map<string, number>();
        for (const edge of [...model.edges].sort((a, b) => a.id < b.id ? -1 : a.id > b.id ? 1 : 0)) {
            const pair = `${edge.source}→${edge.target}`; const lane = lanes.get(pair) ?? 0;
            offsets.set(edge.id, lane * 2.5); lanes.set(pair, lane + 1);
        }
        return offsets;
    }, [model.edges]);
    const candidateId = hoveredId ?? selectedId;
    const focusId = candidateId && nodesById.has(candidateId) ? candidateId : undefined;
    const candidateEdgeId = hoveredEdgeId ?? selectedEdgeId;
    const focusEdgeId = model.edges.some(edge => edge.id === candidateEdgeId) ? candidateEdgeId : undefined;
    const related = useMemo(() => {
        const ids = new Set<string>();
        if (focusId) ids.add(focusId);
        for (const edge of model.edges) {
            if (edge.source === focusId || edge.target === focusId || edge.id === focusEdgeId) {
                ids.add(edge.source); ids.add(edge.target);
            }
        }
        return ids;
    }, [model.edges, focusId, focusEdgeId]);
    const floorY = Math.min(0, ...model.nodes.map(node => node.position[1])) - 0.14;
    const extent = Math.max(120, ...model.nodes.map(node => Math.max(Math.abs(node.position[0]), Math.abs(node.position[2])) * 2 + 40));
    return <div className="architecture-scene" data-testid="architecture-scene" data-projection={planar ? 'plan' : '3d'}
        style={{ cursor: hoveredId || hoveredEdgeId ? 'pointer' : 'grab' }}>
        <Canvas frameloop={active ? 'demand' : 'never'} dpr={[1, 1.5]}
            gl={{ antialias: true, alpha: false, powerPreference: 'low-power' }}
            role="img" aria-label={`${model.title}. ${model.positionMeaning}. Use the adjacent list to select nodes and inspect relationships.`}>
            <color attach="background" args={['#0b1118']} />
            <OrthographicCamera makeDefault position={[80, 100, 100]} near={0.1} far={2000} zoom={5} />
            <ambientLight intensity={1.6} />
            <directionalLight position={[30, 80, 20]} intensity={2.4} color="#dcfff3" />
            <directionalLight position={[-20, 20, -30]} intensity={1.2} color="#a899ff" />
            <gridHelper args={[extent, Math.min(80, Math.ceil(extent / 12)), '#1c3440', '#15242e']} position={[0, floorY, 0]} />
            {model.edges.map(edge => {
                const source = nodesById.get(edge.source);
                const target = nodesById.get(edge.target);
                if (!source || !target) return null;
                const emphasized = edge.id === focusEdgeId || edge.source === focusId || edge.target === focusId;
                return <Relationship key={edge.id} edge={edge} source={source} target={target}
                    laneOffset={edgeOffsets.get(edge.id) ?? 0}
                    selected={edge.id === selectedEdgeId} emphasized={emphasized} showLabel={edge.id === focusEdgeId}
                    dimmed={related.size > 0 && !emphasized} onSelect={() => onSelectEdge(edge.id)} onHover={setHoveredEdgeId} />;
            })}
            {model.nodes.map(node => <ArchitectureNode key={node.id} node={node}
                selected={node.id === selectedId} highlighted={related.has(node.id)} dimmed={related.size > 0 && !related.has(node.id)}
                onSelect={() => onSelect(node.id)} onHover={setHoveredId} />)}
            <OrbitControls ref={controls} makeDefault enabled={active} enableRotate={!planar} enableDamping={false}
                mouseButtons={{ LEFT: planar ? MOUSE.PAN : MOUSE.ROTATE, MIDDLE: MOUSE.DOLLY, RIGHT: MOUSE.PAN }}
                touches={{ ONE: planar ? TOUCH.PAN : TOUCH.ROTATE, TWO: TOUCH.DOLLY_PAN }}
                minZoom={0.15} maxZoom={60} rotateSpeed={0.5} zoomSpeed={0.8} maxPolarAngle={Math.PI / 2.05} />
            <FitArchitecture model={model} planar={planar} resetKey={resetKey} controls={controls} active={active} />
        </Canvas>
        <div className="architecture-scene-guide" aria-hidden="true"><span>{planar ? 'PLAN' : '3D MAP'}</span>{planar ? 'Drag to pan · Scroll to zoom' : 'Drag to orbit · Right-drag to pan · Scroll to zoom'}</div>
    </div>;
}
