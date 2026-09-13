import { useEffect, useMemo, useRef, useState, type RefObject } from 'react';
import { Canvas, useFrame, useThree } from '@react-three/fiber';
import { Edges, Html, OrbitControls, OrthographicCamera } from '@react-three/drei';
import { Box3, Color, CubicBezierCurve3, MOUSE, TOUCH, OrthographicCamera as ThreeOrthographicCamera, Quaternion, QuadraticBezierCurve3, Vector2, Vector3 } from 'three';
import type { OrbitControls as OrbitControlsImpl } from 'three-stdlib';
import type { SemanticEdge, SemanticGraph, SemanticNode, SemanticPlatform } from './semantic-graph';
import { languageColor, measureSourceNode, sourceBrickHeight, sourceLanguage, sourceNodeSizePercent, type SourceCatalog, type SourceMeasure } from './source-metrics';
import { gravityPercent, gravityStrength, hotspotsForNode, type HotspotCatalog, type HotspotGroup } from './hotspot-map';
import { edgeColor, isDirectedEdge } from '../graph/edge-style';
import { EdgePulseLayer } from '../graph/EdgePulseLayer';
import './architecture-scene.css';
import { useGraphBackgroundReset } from '../graph/useGraphBackgroundReset';

export interface ArchitectureSceneProps {
    model: SemanticGraph;
    selectedId?: string;
    selectedEdgeId?: string;
    onSelect: (id: string) => void;
    onSelectEdge: (id: string) => void;
    onOpen?: (id: string) => void;
    onClearSelection?: () => void;
    active?: boolean;
    planar?: boolean;
    resetKey?: number;
    catalog?: SourceCatalog;
    heightMetric?: 'lines' | 'uniform';
    colorMetric?: 'language' | 'kind';
    hotspots?: HotspotCatalog;
    showHotspots?: boolean;
    adaptiveLabels?: boolean;
}

interface RenderNode extends SemanticNode { height?: number; tint?: string; measure?: SourceMeasure; hotspot?: HotspotGroup; gravity?: number; sizePercent?: number; gravityPercent?: number }
type RenderGraph = Omit<SemanticGraph, 'nodes'> & { nodes: RenderNode[] };

const PALETTE = { area: '#69e5bc', file: '#8ccff1', symbol: '#c2afff', route: '#ffc98b' } as const;

function dimensions(node: RenderNode): [number, number, number] {
    if (node.footprint) return [node.footprint[0], node.height ?? 1.1, node.footprint[1]];
    if (node.kind === 'area') return [12, node.height ?? 0.9, 8];
    if (node.kind === 'file') return [5.5, node.height ?? 2.5, 4];
    if (node.kind === 'route') return [4.5, 2.5, 4.5];
    return [3.2, 3.2, 3.2];
}

function centerOf(node: RenderNode): Vector3 {
    return new Vector3(...node.position).add(new Vector3(0, dimensions(node)[1] / 2, 0));
}

/** Folder containment is scenery, never an extra dependency or selectable symbol. */
function FolderPlatform({ platform }: { platform: SemanticPlatform }) {
    return <group position={platform.position}>
        <mesh position={[0, -0.225, 0]} raycast={() => null}>
            <boxGeometry args={[platform.width, 0.45, platform.depth]} />
            <meshStandardMaterial color="#536977" roughness={0.95} transparent opacity={0.11 + platform.level * 0.015} depthWrite={false} />
            <Edges color="#6e8a98" transparent opacity={0.3} />
        </mesh>
        <Html position={[-platform.width / 2 + 3, 0.12, platform.depth / 2 - 2]} zIndexRange={[5, 1]} style={{ pointerEvents: 'none' }}>
            <span className="architecture-folder-label" data-folder-path={platform.path} data-level={platform.level}
                data-position={platform.position.join(',')} data-width={platform.width} data-depth={platform.depth}
                title={platform.path}>{platform.path ? platform.path.split('/').at(-1) : platform.label}</span>
        </Html>
    </group>;
}

function GravityWell({ node, floorY, dimmed }: { node: RenderNode; floorY: number; dimmed: boolean }) {
    const strength = node.gravity ?? 0;
    const radius = 1.2 + strength * 8.3;
    const depth = 0.35 + strength * 6.65;
    const profile = useMemo(() => [new Vector2(0.25, -depth), new Vector2(radius * 0.2, -depth * 0.85),
        new Vector2(radius * 0.45, -depth * 0.35), new Vector2(radius * 0.7, -depth * 0.08), new Vector2(radius, 0)], [depth, radius]);
    return <group position={[node.position[0], floorY - 0.1, node.position[2]]}>
        <mesh raycast={() => null}><latheGeometry args={[profile, 24]} /><meshBasicMaterial color="#8e8270" wireframe transparent opacity={dimmed ? 0.025 : 0.1 + strength * 0.12} depthWrite={false} /></mesh>
        {[0.45, 0.7, 1].map((fraction, index) => <mesh key={fraction} position={[0, -depth * [0.35, 0.08, 0][index], 0]} rotation={[-Math.PI / 2, 0, 0]} raycast={() => null}>
            <ringGeometry args={[radius * fraction - 0.04, radius * fraction + 0.04, 48]} /><meshBasicMaterial color="#aea08b" transparent opacity={dimmed ? 0.06 : 0.2 + strength * 0.3} depthWrite={false} />
        </mesh>)}
    </group>;
}

/** Base edges and the batched pulses share a single source-to-target curve. */
function relationshipGeometry(source: RenderNode, target: RenderNode, laneOffset: number) {
    const start = centerOf(source), end = centerOf(target), delta = end.clone().sub(start), distance = delta.length();
    // Height is routing clearance, never a measurement or an extra dependency.
    const perpendicular = new Vector3(-delta.z, 0, delta.x).normalize();
    const midpoint = start.clone().lerp(end, 0.5)
        .add(new Vector3(0, Math.min(10, 3 + distance * 0.1), 0))
        .addScaledVector(perpendicular, Math.min(4, distance * 0.09) + laneOffset);
    const curve = source.id === target.id
        ? new CubicBezierCurve3(start.clone().add(new Vector3(1, 1, 0)), start.clone().add(new Vector3(10 + laneOffset, 10, 7)), start.clone().add(new Vector3(-10 - laneOffset, 10, 7)), end.clone().add(new Vector3(-1, 1, 0)))
        : new QuadraticBezierCurve3(start, midpoint, end);
    const arrowAt = 0.79;
    return { curve, points: curve.getPoints(32), arrowPosition: curve.getPoint(arrowAt),
        arrowRotation: new Quaternion().setFromUnitVectors(new Vector3(0, 1, 0), curve.getTangent(arrowAt).normalize()),
        labelPosition: curve.getPoint(0.5) };
}

/** Direction comes from relationship semantics, never navigation or selection. */
function Relationship({ edge, geometry, selected, emphasized, dimmed, showLabel, onSelect, onHover }: {
    edge: SemanticEdge; geometry: ReturnType<typeof relationshipGeometry>;
    selected: boolean; emphasized: boolean; dimmed: boolean; showLabel: boolean;
    onSelect: () => void; onHover: (id?: string) => void;
}) {
    const { curve, arrowPosition, arrowRotation, labelPosition } = geometry;
    const color = edgeColor(edge.type);
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
        {isDirectedEdge(edge.type) && <mesh position={arrowPosition} quaternion={arrowRotation}
            onClick={event => { event.stopPropagation(); onSelect(); }}>
            <coneGeometry args={[selected || emphasized ? 0.42 : 0.28, 1.1, 6]} />
            <meshBasicMaterial color={color} transparent opacity={Math.min(1, opacity + 0.18)} />
        </mesh>}
        {showLabel && !dimmed && <Html position={labelPosition} center zIndexRange={[30, 20]}>
            <button type="button" className="architecture-edge-label" onClick={onSelect}
                style={{ borderColor: color }} aria-label={`Inspect ${edge.type} relationship, ${edge.count} indexed edges`}>
                {edge.type.replaceAll('_', ' ').toLowerCase()}{edge.count > 1 && <span> ×{edge.count}</span>}
            </button>
        </Html>}
    </group>;
}

function ArchitectureNode({ node, selected, highlighted, dimmed, labelVisible = true, onSelect, onOpen, onHover }: {
    node: RenderNode; selected: boolean; highlighted: boolean; dimmed: boolean;
    labelVisible?: boolean;
    onSelect: () => void; onHover: (id?: string) => void;
    onOpen?: () => void;
}) {
    const color = node.tint ?? PALETTE[node.kind];
    const fill = useMemo(() => new Color('#27333e').lerp(new Color(color), selected ? 0.34 : highlighted ? 0.25 : 0.18), [color, selected, highlighted]);
    const size = dimensions(node);
    const labelY = size[1] + 7;
    const measure = node.measure;
    const lineText = measure?.lines === undefined ? 'Lines unavailable'
        : `${measure.measuredFiles < measure.files ? '≥ ' : ''}${measure.lines.toLocaleString()} indexed lines`;
    const mixed = Boolean(measure && measure.languages.length > 1);
    const roofLanguages = node.tint && mixed ? measure!.languages.slice(0, 4) : [];
    const roofTotal = measure?.lines || measure?.files || 1;
    let roofOffset = -size[0] / 2;
    return <group position={node.position}>
        <mesh position={[0, size[1] / 2, 0]}
            onClick={event => { event.stopPropagation(); onSelect(); }}
            onDoubleClick={event => { event.stopPropagation(); onOpen?.(); }}
            onPointerOver={event => { event.stopPropagation(); onHover(node.id); }}
            onPointerOut={() => onHover()}>
            {node.kind === 'symbol' ? <icosahedronGeometry args={[1.6, 1]} />
                : node.kind === 'route' ? <cylinderGeometry args={[2.2, 2.2, size[1], 6]} />
                    : <boxGeometry args={size} />}
            <meshStandardMaterial color={fill} roughness={0.85} metalness={0.05}
                emissive={color} emissiveIntensity={selected ? 0.08 : highlighted ? 0.035 : 0.008}
                transparent opacity={dimmed ? 0.14 : selected ? 0.65 : highlighted ? 0.55 : 0.42} depthWrite={false} />
            <Edges color={selected ? '#f0fff9' : color} transparent opacity={dimmed ? 0.12 : selected ? 1 : highlighted ? 0.85 : 0.48} />
        </mesh>
        {roofLanguages.map(language => {
            const width = size[0] * (measure?.lines ? language.lines : language.files) / roofTotal;
            const x = roofOffset + width / 2; roofOffset += width;
            return width > 0 ? <mesh key={language.name} position={[x, size[1] + 0.025, size[2] / 2 - 0.45]} raycast={() => null}>
                <boxGeometry args={[width, 0.06, 0.9]} /><meshBasicMaterial color={language.color} transparent opacity={dimmed ? 0.1 : selected ? 0.6 : 0.38} depthWrite={false} />
            </mesh> : null;
        })}
        {selected && <mesh position={[0, 0.05, 0]} rotation={[-Math.PI / 2, 0, 0]} raycast={() => null}>
            <ringGeometry args={[Math.max(size[0], size[2]) * 0.65, Math.max(size[0], size[2]) * 0.65 + 0.12, 64]} />
            <meshBasicMaterial color="#ecfff7" transparent opacity={0.85} depthWrite={false} />
        </mesh>}
        <Html position={[0, labelY, 0]} center zIndexRange={selected ? [60, 50] : [20, 10]}>
            <button type="button" className={`architecture-node-label${selected ? ' is-selected' : ''}${dimmed ? ' is-dimmed' : ''}${labelVisible ? '' : ' is-hidden'}`}
                data-kind={node.kind} aria-pressed={selected} onClick={onSelect} onDoubleClick={onOpen}
                data-node-id={node.id} data-position={node.position.join(',')} data-label-visible={labelVisible}
                data-lines={measure?.lines} data-height={size[1]} data-language={measure?.languages[0]?.name} data-gravity={node.gravity ?? 0} data-size-percent={node.sizePercent} data-gravity-percent={node.gravityPercent}
                onPointerEnter={() => onHover(node.id)} onPointerLeave={() => onHover()}
                title={`${node.label}\n${node.detail}${measure ? `\n${lineText}\n${measure.languages.map(language => language.name).join(', ')} (from file types)` : ''}`}>
                {selected && <span className="architecture-node-kind"><i style={{ background: color }} />{node.kindLabel ?? (node.kind === 'area' ? 'Source area' : node.kind)}</span>}
                <span className="architecture-node-title"><strong>{node.hotspot && <span className="architecture-hotspot-mark" title={`${node.hotspot.findings.length} ranked hotspots; strongest measured fan-in ${node.hotspot.maxFanIn ?? 'unavailable'}${node.gravityPercent === undefined ? '' : `; gravity ${Number(node.gravityPercent.toFixed(1))}%`}`}>◉ </span>}{node.label}</strong>{!measure && node.gravityPercent !== undefined && <small className="architecture-node-size" title="Gravity relative to strongest hotspot">{node.gravityPercent > 0 && node.gravityPercent < 0.1 ? '<0.1' : Number(node.gravityPercent.toFixed(1))}%</small>}</span>
                {selected && measure && <span className="architecture-node-metric"><i style={{ background: color }} />{measure.lines === undefined ? '?' : `${measure.measuredFiles < measure.files ? '≥' : ''}${Intl.NumberFormat('en', { notation: 'compact', maximumFractionDigits: 1 }).format(measure.lines)}`} lines{mixed ? ' · mixed' : ''}</span>}
                {selected && <span className="architecture-node-detail">{node.detail}</span>}
            </button>
        </Html>
    </group>;
}

/** Screen-space label density changes with zoom; source positions never do. */
function HotspotLabels({ model, priorityId, onVisible }: { model: RenderGraph; priorityId?: string; onVisible: (ids: Set<string>) => void }) {
    const { camera, size } = useThree();
    const previous = useRef('');
    useFrame(() => {
        const occupied: { left: number; right: number; top: number; bottom: number }[] = [];
        const ids = new Set<string>();
        const ordered = [...model.nodes].sort((a, b) => Number(b.id === priorityId) - Number(a.id === priorityId));
        for (const node of ordered) {
            const point = new Vector3(...node.position).add(new Vector3(0, dimensions(node)[1] + 7, 0)).project(camera);
            const x = (point.x + 1) * size.width / 2; const y = (1 - point.y) * size.height / 2;
            const width = Math.min(178, Math.max(78, node.label.length * 7 + (node.gravityPercent === undefined ? 26 : 68)));
            const halfHeight = node.id === priorityId ? 48 : 19;
            const rect = { left: x - width / 2 - 5, right: x + width / 2 + 5, top: y - halfHeight, bottom: y + halfHeight };
            if (node.id !== priorityId && (rect.right < 0 || rect.left > size.width || rect.bottom < 0 || rect.top > size.height
                || occupied.some(other => rect.left < other.right && rect.right > other.left && rect.top < other.bottom && rect.bottom > other.top))) continue;
            ids.add(node.id); occupied.push(rect);
        }
        const key = [...ids].sort().join('|');
        if (previous.current !== key) { previous.current = key; onVisible(ids); }
    });
    return null;
}

function FitArchitecture({ model, planar, resetKey, controls, active }: {
    model: RenderGraph; planar: boolean; resetKey: number;
    controls: RefObject<OrbitControlsImpl | null>; active: boolean;
}) {
    const { camera, size, invalidate } = useThree();
    const layoutKey = model.nodes.map(node => `${node.id}:${node.position.join(',')}:${dimensions(node)[1]}:${node.gravity ?? 0}`).join('|')
        + (model.platforms ?? []).map(platform => `${platform.id}:${platform.position.join(',')}:${platform.width}:${platform.depth}`).join('|');
    const nodesRef = useRef(model.nodes);
    nodesRef.current = model.nodes;
    const platformsRef = useRef(model.platforms);
    platformsRef.current = model.platforms;
    useEffect(() => {
        if (!(camera instanceof ThreeOrthographicCamera) || size.width < 1 || size.height < 1) return;
        const bounds = new Box3();
        for (const node of nodesRef.current) {
            const center = centerOf(node);
            const half = new Vector3(...dimensions(node)).multiplyScalar(0.5).addScalar(2);
            if (node.gravity) { half.x = Math.max(half.x, 10); half.z = Math.max(half.z, 10); bounds.expandByPoint(new Vector3(node.position[0], -8, node.position[2])); }
            bounds.expandByPoint(center.clone().sub(half));
            bounds.expandByPoint(center.clone().add(half));
        }
        for (const platform of platformsRef.current ?? []) {
            const [x, y, z] = platform.position;
            bounds.expandByPoint(new Vector3(x - platform.width / 2 - 2, y - 0.45, z - platform.depth / 2 - 2));
            bounds.expandByPoint(new Vector3(x + platform.width / 2 + 2, y + 1, z + platform.depth / 2 + 2));
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

export function ArchitectureScene({ model: graphModel, selectedId, selectedEdgeId, onSelect, onSelectEdge, onOpen, onClearSelection, active = true, planar = false, resetKey = 0, catalog, heightMetric = 'uniform', colorMetric = 'kind', hotspots, showHotspots = true, adaptiveLabels = false }: ArchitectureSceneProps) {
    const model: RenderGraph = useMemo(() => ({ ...graphModel, nodes: graphModel.nodes.map(node => {
        const measure = catalog ? measureSourceNode(node, catalog) : undefined;
        const sizePercent = catalog ? sourceNodeSizePercent(node, catalog) : undefined;
        const language = measure?.languages[0]?.name ?? (node.filePath ? sourceLanguage(node.filePath) : undefined);
        const hotspot = showHotspots && hotspots ? hotspotsForNode(node, hotspots) : undefined;
        return { ...node, measure, sizePercent, hotspot, gravity: hotspot && hotspots ? gravityStrength(hotspot.maxFanIn, hotspots.maxFanIn) : 0,
            gravityPercent: hotspot && hotspots ? gravityPercent(hotspot.maxFanIn, hotspots.maxFanIn) : undefined,
            height: catalog && heightMetric === 'lines' && ['area', 'file'].includes(node.kind) ? node.kind === 'area' && node.areaPath === '(root)' ? 0.35 : sourceBrickHeight(sizePercent, 100) : undefined,
            tint: node.tint ?? (colorMetric === 'language' && node.kind !== 'route' ? languageColor(language ?? 'Unknown') : undefined) };
    }) }), [graphModel, catalog, heightMetric, colorMetric, hotspots, showHotspots]);
    const [hoveredId, setHoveredId] = useState<string>();
    const [hoveredEdgeId, setHoveredEdgeId] = useState<string>();
    const [visibleHotspotLabels, setVisibleHotspotLabels] = useState<Set<string>>();
    const background = useGraphBackgroundReset(() => { setHoveredId(undefined); setHoveredEdgeId(undefined); onClearSelection?.(); });
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
    const edgeGeometry = useMemo(() => {
        const result = new Map<string, ReturnType<typeof relationshipGeometry>>();
        for (const edge of model.edges) {
            const source = nodesById.get(edge.source), target = nodesById.get(edge.target);
            if (source && target) result.set(edge.id, relationshipGeometry(source, target, edgeOffsets.get(edge.id) ?? 0));
        }
        return result;
    }, [model.edges, nodesById, edgeOffsets]);
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
    const pulsePaths = useMemo(() => model.edges.flatMap(edge => {
        const geometry = edgeGeometry.get(edge.id);
        if (!geometry || !isDirectedEdge(edge.type)) return [];
        const emphasized = edge.id === focusEdgeId || edge.source === focusId || edge.target === focusId;
        return [{ id: edge.id, type: edge.type, points: geometry.points,
            opacity: related.size > 0 && !emphasized ? 0.04 : edge.id === selectedEdgeId ? 0.95 : emphasized ? 0.8 : 0.65 }];
    }), [model.edges, edgeGeometry, focusEdgeId, focusId, related, selectedEdgeId]);
    const floorY = Math.min(0, ...model.nodes.map(node => node.position[1])) - 0.14;
    const extent = Math.max(120, ...model.nodes.map(node => Math.max(Math.abs(node.position[0]), Math.abs(node.position[2])) * 2 + 40));
    return <div className="architecture-scene" data-testid="architecture-scene" data-projection={planar ? 'plan' : '3d'}
        style={{ cursor: hoveredId || hoveredEdgeId ? 'pointer' : 'grab' }}>
        <Canvas {...background} frameloop={active ? 'demand' : 'never'} dpr={[1, 1.5]}
            gl={{ antialias: true, alpha: false, powerPreference: 'low-power' }}
            role="img" aria-label={`${model.title}. ${model.positionMeaning}. Use the adjacent list to select nodes and inspect relationships.`}>
            <color attach="background" args={['#0b1118']} />
            <OrthographicCamera makeDefault position={[80, 100, 100]} near={0.1} far={2000} zoom={5} />
            <ambientLight intensity={1.6} />
            <directionalLight position={[30, 80, 20]} intensity={2.4} color="#dcfff3" />
            <directionalLight position={[-20, 20, -30]} intensity={1.2} color="#a899ff" />
            <gridHelper args={[extent, Math.min(80, Math.ceil(extent / 12)), '#1c3440', '#15242e']} position={[0, floorY, 0]} />
            {model.platforms?.map(platform => <FolderPlatform key={platform.id} platform={platform} />)}
            {model.nodes.filter(node => node.gravity).map(node => <GravityWell key={`gravity:${node.id}`} node={node} floorY={floorY} dimmed={related.size > 0 && !related.has(node.id)} />)}
            {model.edges.map(edge => {
                const geometry = edgeGeometry.get(edge.id);
                if (!geometry) return null;
                const emphasized = edge.id === focusEdgeId || edge.source === focusId || edge.target === focusId;
                return <Relationship key={edge.id} edge={edge} geometry={geometry}
                    selected={edge.id === selectedEdgeId} emphasized={emphasized} showLabel={edge.id === focusEdgeId}
                    dimmed={related.size > 0 && !emphasized} onSelect={() => onSelectEdge(edge.id)} onHover={setHoveredEdgeId} />;
            })}
            <EdgePulseLayer paths={pulsePaths} active={active} />
            {model.nodes.map(node => <ArchitectureNode key={node.id} node={node}
                selected={node.id === selectedId} highlighted={related.has(node.id)} dimmed={related.size > 0 && !related.has(node.id)}
                labelVisible={(!adaptiveLabels && model.view !== 'hotspots') || visibleHotspotLabels === undefined || visibleHotspotLabels.has(node.id) || node.id === selectedId || (adaptiveLabels && node.id === hoveredId)}
                onSelect={() => onSelect(node.id)} onOpen={onOpen ? () => onOpen(node.id) : undefined} onHover={setHoveredId} />)}
            <OrbitControls ref={controls} makeDefault enabled={active} enableRotate={!planar} enableDamping={false}
                mouseButtons={{ LEFT: planar ? MOUSE.PAN : MOUSE.ROTATE, MIDDLE: MOUSE.DOLLY, RIGHT: MOUSE.PAN }}
                touches={{ ONE: planar ? TOUCH.PAN : TOUCH.ROTATE, TWO: TOUCH.DOLLY_PAN }}
                minZoom={0.15} maxZoom={60} rotateSpeed={0.5} zoomSpeed={0.8} maxPolarAngle={Math.PI / 2.05} />
            <FitArchitecture model={model} planar={planar} resetKey={resetKey} controls={controls} active={active} />
            {(adaptiveLabels || model.view === 'hotspots') && <HotspotLabels model={model} priorityId={selectedId ?? hoveredId} onVisible={setVisibleHotspotLabels} />}
        </Canvas>
        <div className="architecture-scene-guide" aria-hidden="true"><span>{planar ? 'PLAN' : '3D MAP'}</span>{planar ? 'Drag to pan · Scroll to zoom' : 'Drag to orbit · Right-drag to pan · Scroll to zoom'}</div>
    </div>;
}
