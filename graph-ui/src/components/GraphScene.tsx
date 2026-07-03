import { useState, useRef, useEffect } from "react";
import { Canvas, useThree, useFrame } from "@react-three/fiber";
import { OrbitControls } from "@react-three/drei";
import type { OrbitControls as OrbitControlsImpl } from "three-stdlib";
import { EffectComposer, Bloom } from "@react-three/postprocessing";
import * as THREE from "three";
import { NodeCloud } from "./NodeCloud";
import { EdgeLines } from "./EdgeLines";
import { NodeLabels } from "./NodeLabels";
import { NodeTooltip } from "./NodeTooltip";
import type { GraphData, GraphNode, LinkedProject } from "../lib/types";

/* ── Camera fly-to animation ────────────────────────────── */

interface CameraTarget {
  position: THREE.Vector3;
  lookAt: THREE.Vector3;
}

function CameraAnimator({ target }: { target: CameraTarget | null }) {
  const { camera } = useThree();
  const targetRef = useRef<CameraTarget | null>(null);
  const progress = useRef(1);

  useEffect(() => {
    if (target) {
      targetRef.current = target;
      progress.current = 0;
    }
  }, [target]);

  useFrame(() => {
    if (!targetRef.current || progress.current >= 1) return;

    progress.current = Math.min(1, progress.current + 0.02);
    const t = 1 - Math.pow(1 - progress.current, 3); /* ease-out cubic */

    camera.position.lerp(targetRef.current.position, t * 0.08);
    camera.lookAt(targetRef.current.lookAt);
  });

  return null;
}

/* ── Render tuning ── */

export const GRAPH_CANVAS_DPR: [number, number] = [1, 1.5];
export const GRAPH_COMPOSER_MULTISAMPLING = 0;

/* ── Main scene ─────────────────────────────────────────── */

interface GraphSceneProps {
  data: GraphData;
  highlightedIds: Set<number> | null;
  cameraTarget: CameraTarget | null;
  showLabels: boolean;
  autoRotate: boolean;
  onNodeClick: (node: GraphNode) => void;
}

export type { CameraTarget };

export function GraphScene({
  data,
  highlightedIds,
  cameraTarget,
  showLabels,
  autoRotate,
  onNodeClick,
}: GraphSceneProps) {
  const [hovered, setHovered] = useState<GraphNode | null>(null);
  const controlsRef = useRef<OrbitControlsImpl | null>(null);

  return (
    <Canvas
      camera={{ position: [0, 0, 800], fov: 50, near: 0.1, far: 100000 }}
      style={{ background: "#06090f" }}
      dpr={GRAPH_CANVAS_DPR}
      gl={{
        antialias: false,
        alpha: false,
        powerPreference: "high-performance",
      }}
    >
      <color attach="background" args={["#06090f"]} />
      <ambientLight intensity={0.5} />
      <pointLight position={[500, 500, 500]} intensity={0.6} />
      <pointLight
        position={[-300, -200, -300]}
        intensity={0.4}
        color="#6040ff"
      />

      <EdgeLines
        nodes={data.nodes}
        edges={data.edges}
        highlightedIds={highlightedIds}
      />
      <NodeCloud
        nodes={data.nodes}
        highlightedIds={highlightedIds}
        onHover={setHovered}
        onClick={onNodeClick}
      />
      {showLabels && <NodeLabels nodes={data.nodes} highlightedIds={highlightedIds} />}

      {/* Satellite galaxies for cross-repo linked projects */}
      {data.linked_projects?.map((lp: LinkedProject) => {
        const offsetNodes = lp.nodes.map((n) => ({
          ...n,
          x: n.x + lp.offset.x,
          y: n.y + lp.offset.y,
          z: n.z + lp.offset.z,
        }));
        return (
          <group key={lp.project}>
            <EdgeLines
              nodes={offsetNodes}
              edges={lp.edges}
              highlightedIds={null}
              opacity={0.3}
            />
            <NodeCloud
              nodes={offsetNodes}
              highlightedIds={null}
              onHover={setHovered}
              onClick={onNodeClick}
              opacity={0.5}
            />
            {/* Inter-galaxy CROSS_* edges: source is in primary, target in
             * this linked project's offset nodes. */}
            {lp.cross_edges && lp.cross_edges.length > 0 && (
              <EdgeLines
                nodes={data.nodes}
                targetNodes={offsetNodes}
                edges={lp.cross_edges}
                highlightedIds={highlightedIds}
                opacity={0.85}
              />
            )}
          </group>
        );
      })}

      {hovered && <NodeTooltip node={hovered} />}

      <CameraAnimator target={cameraTarget} />

      <EffectComposer multisampling={GRAPH_COMPOSER_MULTISAMPLING}>
        <Bloom
          luminanceThreshold={0.3}
          luminanceSmoothing={0.7}
          intensity={1.2}
          mipmapBlur
          radius={0.6}
        />
      </EffectComposer>

      <OrbitControls
        ref={controlsRef}
        autoRotate={autoRotate}
        enableDamping
        dampingFactor={0.08}
        rotateSpeed={0.5}
        zoomSpeed={1.5}
        minDistance={10}
        maxDistance={50000}
        autoRotateSpeed={0.4}
      />
    </Canvas>
  );
}

/* ── Helper: compute camera target from node IDs ────────── */

export function computeCameraTarget(
  nodes: GraphNode[],
  ids: Set<number>,
): CameraTarget | null {
  if (ids.size === 0) return null;

  let cx = 0,
    cy = 0,
    cz = 0,
    count = 0;
  for (const node of nodes) {
    if (ids.has(node.id)) {
      cx += node.x;
      cy += node.y;
      cz += node.z;
      count++;
    }
  }
  if (count === 0) return null;

  cx /= count;
  cy /= count;
  cz /= count;

  /* Distance based on cluster spread — ensure we never zoom too close */
  let maxDist = 0;
  for (const node of nodes) {
    if (ids.has(node.id)) {
      const d = Math.sqrt(
        (node.x - cx) ** 2 + (node.y - cy) ** 2 + (node.z - cz) ** 2,
      );
      if (d > maxDist) maxDist = d;
    }
  }

  /* Minimum distance scales with count: single node = 300, cluster = spread-based */
  const spreadDist = maxDist * 3;
  const minDist = count <= 5 ? 300 : 200;
  const distance = Math.max(minDist, spreadDist);
  const lookAt = new THREE.Vector3(cx, cy, cz);
  const position = new THREE.Vector3(
    cx + distance * 0.2,
    cy + distance * 0.15,
    cz + distance,
  );

  return { position, lookAt };
}
