import { useEffect, useMemo, useRef } from "react";
import { useThree } from "@react-three/fiber";
import * as THREE from "three";
import type { GraphNode } from "../lib/types";
import { nodeGlowBoost } from "../lib/density";

interface NodeCloudProps {
  nodes: GraphNode[];
  highlightedIds: Set<number> | null;
  onHover: (node: GraphNode | null) => void;
  onClick: (node: GraphNode) => void;
  opacity?: number;
  /* Multiplier on the per-node glow boost. 1 = full boost (sparse graphs),
   * 0 = flat colors (dense graphs). Adaptive default × user setting. */
  boost?: number;
}

/* Above this count instanced spheres stop paying off (vertex + matrix cost)
 * and the cloud switches to point sprites — one position per node. */
const POINT_MODE_THRESHOLD = 75000;

/* Sphere tessellation by node count: nobody can tell a 12-segment sphere from
 * a 32-segment one at 25k nodes, but the GPU can. */
function sphereDetail(count: number): [number, number, number] {
  if (count <= 8000) return [1, 32, 24];
  if (count <= 25000) return [1, 16, 12];
  return [1, 10, 7];
}

function nodeColor(
  node: GraphNode,
  highlightedIds: Set<number> | null,
  opacity: number,
  boost: number,
  tempColor: THREE.Color,
): [number, number, number] {
  const hasHighlight = highlightedIds && highlightedIds.size > 0;
  tempColor.set(node.color);
  if (hasHighlight && !highlightedIds.has(node.id)) {
    tempColor.multiplyScalar(0.15);
  } else {
    /* Boost above 1.0 so bloom picks up the excess as glow corona. Blue hubs
     * glow most, red leaves modestly, white/yellow least (see nodeGlowBoost).
     * The boost amount also fades toward 1.0 (flat color) as density rises so
     * dense graphs stay legible instead of blooming into a white blob. */
    const fullBoost = nodeGlowBoost(tempColor.r, tempColor.g, tempColor.b);
    const applied = 1 + (fullBoost - 1) * boost;
    tempColor.multiplyScalar(applied);
  }
  return [tempColor.r * opacity, tempColor.g * opacity, tempColor.b * opacity];
}

/* Round, soft-edged sprite for point mode (module-level lazy singleton). */
let pointSprite: THREE.CanvasTexture | null = null;
function getPointSprite(): THREE.CanvasTexture {
  if (pointSprite) return pointSprite;
  const size = 64;
  const canvas = document.createElement("canvas");
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext("2d")!;
  const gradient = ctx.createRadialGradient(
    size / 2, size / 2, 0,
    size / 2, size / 2, size / 2,
  );
  gradient.addColorStop(0, "rgba(255,255,255,1)");
  gradient.addColorStop(0.5, "rgba(255,255,255,0.9)");
  gradient.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, size, size);
  pointSprite = new THREE.CanvasTexture(canvas);
  return pointSprite;
}

/* ── Point-sprite mode for very large clouds ──────────────────── */

function NodePoints({
  nodes,
  highlightedIds,
  onHover,
  onClick,
  opacity,
  boost,
}: Required<NodeCloudProps>) {
  const { raycaster } = useThree();

  /* Widen the raycast threshold while points are on screen */
  useEffect(() => {
    const prev = raycaster.params.Points?.threshold ?? 1;
    raycaster.params.Points = { threshold: 3 };
    return () => {
      raycaster.params.Points = { threshold: prev };
    };
  }, [raycaster]);

  const { positions, colors } = useMemo(() => {
    const positions = new Float32Array(nodes.length * 3);
    const colors = new Float32Array(nodes.length * 3);
    const tempColor = new THREE.Color();
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      positions[i * 3] = n.x;
      positions[i * 3 + 1] = n.y;
      positions[i * 3 + 2] = n.z;
      const [r, g, b] = nodeColor(n, highlightedIds, opacity, boost, tempColor);
      colors[i * 3] = r;
      colors[i * 3 + 1] = g;
      colors[i * 3 + 2] = b;
    }
    return { positions, colors };
  }, [nodes, highlightedIds, opacity, boost]);

  return (
    <points
      /* Remount when the buffer size changes so stale attributes never linger */
      key={nodes.length}
      onPointerOver={(e) => {
        e.stopPropagation();
        if (e.index !== undefined && e.index < nodes.length) {
          onHover(nodes[e.index]);
        }
      }}
      onPointerOut={() => onHover(null)}
      onClick={(e) => {
        e.stopPropagation();
        if (e.index !== undefined && e.index < nodes.length) {
          onClick(nodes[e.index]);
        }
      }}
    >
      <bufferGeometry>
        <bufferAttribute attach="attributes-position" args={[positions, 3]} />
        <bufferAttribute attach="attributes-color" args={[colors, 3]} />
      </bufferGeometry>
      <pointsMaterial
        vertexColors
        size={4}
        sizeAttenuation
        map={getPointSprite()}
        alphaTest={0.35}
        transparent
        toneMapped={false}
      />
    </points>
  );
}

/* ── Instanced-sphere mode (default) ──────────────────────────── */

function NodeSpheres({
  nodes,
  highlightedIds,
  onHover,
  onClick,
  opacity,
  boost,
}: Required<NodeCloudProps>) {
  const meshRef = useRef<THREE.InstancedMesh>(null);
  const tempObj = useMemo(() => new THREE.Object3D(), []);
  const tempColor = useMemo(() => new THREE.Color(), []);
  const detail = sphereDetail(nodes.length);

  /* Build instance color attributes — dim non-highlighted nodes */
  const colors = useMemo(() => {
    const arr = new Float32Array(nodes.length * 3);
    for (let i = 0; i < nodes.length; i++) {
      const [r, g, b] = nodeColor(
        nodes[i],
        highlightedIds,
        opacity,
        boost,
        tempColor,
      );
      arr[i * 3] = r;
      arr[i * 3 + 1] = g;
      arr[i * 3 + 2] = b;
    }
    return arr;
  }, [nodes, highlightedIds, tempColor, opacity, boost]);

  /* Node positions are static (the layout is server-computed), so instance
   * matrices only change with the node set or the highlight — never rebuild
   * them per frame. */
  useEffect(() => {
    const mesh = meshRef.current;
    if (!mesh) return;

    const hasHighlight = highlightedIds && highlightedIds.size > 0;
    for (let i = 0; i < nodes.length; i++) {
      const n = nodes[i];
      tempObj.position.set(n.x, n.y, n.z);
      const isHighlighted = !hasHighlight || highlightedIds.has(n.id);
      const s = n.size * (isHighlighted ? 0.5 : 0.2);
      tempObj.scale.set(s, s, s);
      tempObj.updateMatrix();
      mesh.setMatrixAt(i, tempObj.matrix);
    }
    mesh.instanceMatrix.needsUpdate = true;
    mesh.computeBoundingSphere();
  }, [nodes, highlightedIds, tempObj]);

  return (
    <instancedMesh
      /* Remount when the instance count changes so buffers are re-sized */
      key={nodes.length}
      ref={meshRef}
      args={[undefined, undefined, nodes.length]}
      frustumCulled={false}
      onPointerOver={(e) => {
        e.stopPropagation();
        if (e.instanceId !== undefined && e.instanceId < nodes.length) {
          onHover(nodes[e.instanceId]);
        }
      }}
      onPointerOut={() => onHover(null)}
      onClick={(e) => {
        e.stopPropagation();
        if (e.instanceId !== undefined && e.instanceId < nodes.length) {
          onClick(nodes[e.instanceId]);
        }
      }}
    >
      <sphereGeometry args={detail} />
      <meshBasicMaterial vertexColors toneMapped={false} />
      <instancedBufferAttribute
        attach="geometry-attributes-color"
        args={[colors, 3]}
      />
    </instancedMesh>
  );
}

export function NodeCloud({
  nodes,
  highlightedIds,
  onHover,
  onClick,
  opacity = 1.0,
  boost = 1.0,
}: NodeCloudProps) {
  if (nodes.length > POINT_MODE_THRESHOLD) {
    return (
      <NodePoints
        nodes={nodes}
        highlightedIds={highlightedIds}
        onHover={onHover}
        onClick={onClick}
        opacity={opacity}
        boost={boost}
      />
    );
  }
  return (
    <NodeSpheres
      nodes={nodes}
      highlightedIds={highlightedIds}
      onHover={onHover}
      onClick={onClick}
      opacity={opacity}
      boost={boost}
    />
  );
}
