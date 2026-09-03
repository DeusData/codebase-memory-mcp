import { useEffect, useMemo, useRef, useState } from "react";
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

/* ── Point-sprite mode: allocate-once / refill-in-place geometry ──────
 *
 * Same failure mode as EdgeLines (#2039): a fresh BufferGeometry + fresh
 * Float32Arrays on every render, previously handed to declarative JSX
 * (<bufferGeometry><bufferAttribute args={[positions,3]}/>...), orphans the
 * previously-uploaded GPU buffer every time `args` changes (R3F reconstructs
 * the attribute instance, but plain BufferAttribute has no dispose() to
 * release its WebGL buffer). We own the geometry directly instead: allocate
 * once per capacity, refill the same TypedArrays in place, and dispose
 * explicitly on growth/unmount. */

interface PointBuffers {
  geometry: THREE.BufferGeometry;
  positionAttr: THREE.BufferAttribute;
  colorAttr: THREE.BufferAttribute;
  capacity: number;
}

export function createPointBuffers(capacity: number): PointBuffers {
  const geometry = new THREE.BufferGeometry();
  const n = Math.max(1, capacity);
  const positionAttr = new THREE.BufferAttribute(new Float32Array(n * 3), 3);
  const colorAttr = new THREE.BufferAttribute(new Float32Array(n * 3), 3);
  positionAttr.setUsage(THREE.DynamicDrawUsage);
  colorAttr.setUsage(THREE.DynamicDrawUsage);
  geometry.setAttribute("position", positionAttr);
  geometry.setAttribute("color", colorAttr);
  geometry.setDrawRange(0, 0);
  return { geometry, positionAttr, colorAttr, capacity: n };
}

export function fillPointBuffers(
  buffers: PointBuffers,
  nodes: GraphNode[],
  highlightedIds: Set<number> | null,
  opacity: number,
  boost: number,
): void {
  const positions = buffers.positionAttr.array as Float32Array;
  const colors = buffers.colorAttr.array as Float32Array;
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
  buffers.positionAttr.needsUpdate = true;
  buffers.colorAttr.needsUpdate = true;
  buffers.geometry.setDrawRange(0, nodes.length);
  /* Points.raycast() computes geometry.boundingSphere lazily ONLY the first
   * time it is null — mutating positions in place afterward (as every
   * refill here does) leaves that sphere stale, which can break hover/click
   * raycasting and even frustum-cull the whole cloud once the node set has
   * moved outside it. Recompute on every refill (cheap relative to the fill
   * itself, and NodePoints only exists above POINT_MODE_THRESHOLD anyway). */
  buffers.geometry.computeBoundingSphere();
}

export function usePointGeometry(
  nodes: GraphNode[],
  highlightedIds: Set<number> | null,
  opacity: number,
  boost: number,
): THREE.BufferGeometry {
  /* Records the exact (by-reference) deps the mount initializer below filled
   * the buffers with, so the effect's first run — which always fires with
   * those same references, since it belongs to the same render — can skip
   * redoing that fill (see EdgeLines.useEdgeGeometry for the same pattern). */
  const initialFillDeps = useRef<
    [GraphNode[], Set<number> | null, number, number] | null
  >(null);

  const [buffers, setBuffers] = useState<PointBuffers>(() => {
    const buf = createPointBuffers(nodes.length);
    fillPointBuffers(buf, nodes, highlightedIds, opacity, boost);
    initialFillDeps.current = [nodes, highlightedIds, opacity, boost];
    return buf;
  });
  const latest = useRef(buffers);
  latest.current = buffers;

  useEffect(() => {
    const init = initialFillDeps.current;
    initialFillDeps.current = null;
    if (
      init &&
      init[0] === nodes &&
      init[1] === highlightedIds &&
      init[2] === opacity &&
      init[3] === boost
    ) {
      /* Same references the initializer just filled with — skip the
       * redundant refill. */
      return;
    }
    setBuffers((prev) => {
      let buf = prev;
      if (nodes.length > buf.capacity) {
        prev.geometry.dispose();
        buf = createPointBuffers(nodes.length);
      }
      fillPointBuffers(buf, nodes, highlightedIds, opacity, boost);
      return buf;
    });
  }, [nodes, highlightedIds, opacity, boost]);

  useEffect(() => {
    return () => {
      latest.current.geometry.dispose();
    };
  }, []);

  return buffers.geometry;
}

function NodePoints({
  nodes,
  highlightedIds,
  onHover,
  onClick,
  opacity,
  boost,
}: Required<NodeCloudProps>) {
  const { raycaster } = useThree();
  const geometry = usePointGeometry(nodes, highlightedIds, opacity, boost);

  /* Widen the raycast threshold while points are on screen */
  useEffect(() => {
    const prev = raycaster.params.Points?.threshold ?? 1;
    raycaster.params.Points = { threshold: 3 };
    return () => {
      raycaster.params.Points = { threshold: prev };
    };
  }, [raycaster]);

  return (
    <points
      geometry={geometry}
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

  /* One InstancedBufferAttribute per nodes.length epoch — the instancedMesh
   * below remounts (via `key`) when the count changes, which cleanly
   * disposes geometry/material/attributes together through R3F's normal
   * unmount path. Within an epoch, highlight/opacity/boost changes refill
   * this SAME attribute's array in place (never replaced), attached via
   * <primitive> so R3F never tries to reconstruct or auto-dispose it — see
   * the EdgeLines/NodePoints comments for why replacing an attribute leaks
   * its GPU buffer (#2039). */
  const colorAttr = useMemo(() => {
    const attr = new THREE.InstancedBufferAttribute(
      new Float32Array(Math.max(1, nodes.length) * 3),
      3,
    );
    attr.setUsage(THREE.DynamicDrawUsage);
    return attr;
    // eslint-disable-next-line react-hooks/exhaustive-deps -- capacity only, refilled below
  }, [nodes.length]);

  useEffect(() => {
    const arr = colorAttr.array as Float32Array;
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
    colorAttr.needsUpdate = true;
  }, [nodes, highlightedIds, opacity, boost, tempColor, colorAttr]);

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
      /* Remount when the instance count changes so buffers are re-sized —
       * this is a full unmount/remount (not an in-place attribute swap), so
       * R3F's normal dispose-on-unmount path correctly frees the old
       * geometry/material/instance buffers. */
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
      <primitive object={colorAttr} attach="geometry-attributes-color" />
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
