import { useState, useRef, useEffect, useCallback } from "react";
import { Canvas, useThree, useFrame } from "@react-three/fiber";
import { OrbitControls } from "@react-three/drei";
import type { OrbitControls as OrbitControlsImpl } from "three-stdlib";
import { EffectComposer, Bloom } from "@react-three/postprocessing";
import * as THREE from "three";
import { NodeCloud } from "./NodeCloud";
import { HaloLayer } from "./HaloLayer";
import { EdgeLines } from "./EdgeLines";
import { NodeLabels } from "./NodeLabels";
import { NodeTooltip } from "./NodeTooltip";
import type { GraphData, GraphNode, LinkedProject } from "../lib/types";
import {
  DEFAULT_DISPLAY_SETTINGS,
  bloomIntensityScale,
  nodeBoostScale,
  type DisplaySettings,
} from "../lib/density";

const BASE_BLOOM_INTENSITY = 1.45;

/* ── Camera fly-to animation ────────────────────────────── */

interface CameraTarget {
  position: THREE.Vector3;
  lookAt: THREE.Vector3;
}

function CameraAnimator({
  target,
  controlsRef,
}: {
  target: CameraTarget | null;
  controlsRef: React.RefObject<OrbitControlsImpl | null>;
}) {
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

    /* Move the OrbitControls pivot to the focus point as well. Otherwise the
     * controls keep their target at the origin and re-center the view on the
     * next frame, snapping the camera back to the middle after the fly-to. */
    const controls = controlsRef.current;
    if (controls) {
      controls.target.lerp(targetRef.current.lookAt, t * 0.08);
      controls.update();
    } else {
      camera.lookAt(targetRef.current.lookAt);
    }
  });

  return null;
}

/* ── Idle auto-rotation ──────────────────────────────────── */

const IDLE_TIMEOUT_MS = 60_000;
export const GRAPH_CANVAS_DPR: [number, number] = [1, 1.5];
export const GRAPH_COMPOSER_MULTISAMPLING = 0;

function IdleAutoRotate({
  controlsRef,
}: {
  controlsRef: React.RefObject<OrbitControlsImpl | null>;
}) {
  const lastInteraction = useRef(Date.now());

  /* Reset timer on any pointer/wheel event */
  const resetTimer = useCallback(() => {
    lastInteraction.current = Date.now();
    if (controlsRef.current) {
      controlsRef.current.autoRotate = false;
    }
  }, [controlsRef]);

  useEffect(() => {
    const canvas = document.querySelector("canvas");
    if (!canvas) return;

    canvas.addEventListener("pointerdown", resetTimer);
    canvas.addEventListener("wheel", resetTimer);
    return () => {
      canvas.removeEventListener("pointerdown", resetTimer);
      canvas.removeEventListener("wheel", resetTimer);
    };
  }, [resetTimer]);

  useFrame(() => {
    if (!controlsRef.current) return;
    const idle = Date.now() - lastInteraction.current > IDLE_TIMEOUT_MS;
    controlsRef.current.autoRotate = idle;
  });

  return null;
}

/* ── Semantic-zoom approach watcher ──────────────────────────
 * Auto-open fires only when ALL guards pass (the anti-annoyance set from
 * the Understand-Anything study): (1) the movement is user-driven — a
 * wheel/pinch happened within the last 600 ms, so programmatic fly-tos
 * never trigger it; (2) the camera is APPROACHING (distance decreasing);
 * (3) debounced — the same node fires once, with a 1.5 s global grace
 * period so backing out doesn't bounce straight back in. */

function ApproachWatcher({
  nodes,
  onApproachNode,
}: {
  nodes: GraphNode[];
  onApproachNode: (node: GraphNode) => void;
}) {
  const { camera, gl } = useThree();
  const lastWheel = useRef(0);
  const lastDist = useRef(Infinity);
  const lastFiredAt = useRef(0);
  const lastCheck = useRef(0);

  useEffect(() => {
    const canvas = gl.domElement;
    const onWheel = () => {
      lastWheel.current = performance.now();
    };
    canvas.addEventListener("wheel", onWheel, { passive: true });
    return () => canvas.removeEventListener("wheel", onWheel);
  }, [gl]);

  useFrame(() => {
    const now = performance.now();
    if (now - lastCheck.current < 200) return;
    lastCheck.current = now;
    if (now - lastWheel.current > 600) return; /* guard 1: user-driven */
    if (now - lastFiredAt.current < 1500) return; /* guard 3: grace */

    let nearest: GraphNode | null = null;
    let nearestDist = Infinity;
    for (const node of nodes) {
      const dx = camera.position.x - node.x;
      const dy = camera.position.y - node.y;
      const dz = camera.position.z - node.z;
      const dist = Math.sqrt(dx * dx + dy * dy + dz * dz);
      if (dist < nearestDist) {
        nearestDist = dist;
        nearest = node;
      }
    }
    const approaching = nearestDist < lastDist.current - 0.5; /* guard 2 */
    lastDist.current = nearestDist;
    if (!nearest || !approaching) return;
    if (nearestDist > Math.max(nearest.size * 2.2, 30)) return;
    lastFiredAt.current = now;
    onApproachNode(nearest);
  });
  return null;
}

/* ── Orbit-target reporter (throttled) ───────────────────── */

function ViewTargetReporter({
  controlsRef,
  onViewTarget,
}: {
  controlsRef: React.RefObject<OrbitControlsImpl | null>;
  onViewTarget: (x: number, y: number) => void;
}) {
  const last = useRef({ x: NaN, y: NaN, at: 0 });
  useFrame(() => {
    const controls = controlsRef.current;
    if (!controls) return;
    const now = performance.now();
    if (now - last.current.at < 200) return;
    const { x, y } = controls.target;
    if (
      Math.abs(x - last.current.x) < 1 &&
      Math.abs(y - last.current.y) < 1 &&
      !Number.isNaN(last.current.x)
    )
      return;
    last.current = { x, y, at: now };
    onViewTarget(x, y);
  });
  return null;
}

/* ── Main scene ─────────────────────────────────────────── */

interface GraphSceneProps {
  /* False pauses the render loop (hidden-but-mounted tab). */
  active?: boolean;
  data: GraphData;
  /* Missed skeleton (#963): pre-offset, pre-painted white nodes + edges of
   * the not-fully-indexed files, rendered as a ghost cluster beside the
   * galaxy. null hides it. */
  missed?: { nodes: GraphNode[]; edges: GraphData["edges"] } | null;
  highlightedIds: Set<number> | null;
  cameraTarget: CameraTarget | null;
  showLabels: boolean;
  display?: DisplaySettings;
  onNodeClick: (node: GraphNode) => void;
  /* Fired when a click hits empty space (no node). Used to fly back to the
   * overview after focusing the missed skeleton. */
  onBackgroundClick?: () => void;
  /* Custom hover card (the region scene shows region facts, not node ones). */
  renderTooltip?: (node: GraphNode) => React.ReactNode;
  /* Landmark halos on the top hubs (off in the region scene, where every
   * body is already a landmark). */
  landmarks?: boolean;
  /* Reports the orbit target (throttled) so the 2D minimap can show where
   * the camera is looking. */
  onViewTarget?: (x: number, y: number) => void;
  /* Semantic zoom: fired once when the USER zooms close onto a node (the
   * region scene opens that region). Guards live in ApproachWatcher. */
  onApproachNode?: (node: GraphNode) => void;
}

export type { CameraTarget };

export function GraphScene({
  active = true,
  data,
  missed = null,
  highlightedIds,
  cameraTarget,
  showLabels,
  display = DEFAULT_DISPLAY_SETTINGS,
  onNodeClick,
  onBackgroundClick,
  renderTooltip,
  landmarks = false,
  onViewTarget,
  onApproachNode,
}: GraphSceneProps) {
  const [hovered, setHovered] = useState<GraphNode | null>(null);
  const controlsRef = useRef<OrbitControlsImpl | null>(null);

  /* Adaptive density defaults × user multipliers. The automatic scale keeps
   * contrast roughly constant as the graph grows; the sliders nudge it.
   * NodeCloud applies `nodeBoost` directly (no internal density scaling),
   * whereas EdgeLines scales by edge density itself — so it receives only the
   * user edge-brightness multiplier to avoid double-applying. */
  /* Constrained camera: orbit bounds follow the scene's actual extent so
   * you can neither clip through the cloud nor zoom out into the void. */
  const sceneRadius = (() => {
    let max = 100;
    for (const node of data.nodes) {
      const r = Math.sqrt(node.x * node.x + node.y * node.y + node.z * node.z);
      if (r > max) max = r;
    }
    return max;
  })();

  const nodeBoost = nodeBoostScale(data.nodes.length) * display.nodeGlow;
  const bloomIntensity =
    BASE_BLOOM_INTENSITY * bloomIntensityScale(data.nodes.length) * display.bloom;

  return (
    <Canvas
      frameloop={active ? "always" : "never"}
      camera={{ position: [0, 0, 800], fov: 50, near: 0.1, far: 100000 }}
      style={{ background: "#0D0F12" }}
      dpr={GRAPH_CANVAS_DPR}
      gl={{
        antialias: false,
        alpha: false,
        powerPreference: "high-performance",
      }}
      onPointerMissed={onBackgroundClick}
    >
      <color attach="background" args={["#0D0F12"]} />
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
        brightness={display.edgeBrightness}
      />
      <NodeCloud
        nodes={data.nodes}
        highlightedIds={highlightedIds}
        onHover={setHovered}
        onClick={onNodeClick}
        boost={nodeBoost}
      />
      {showLabels && <NodeLabels nodes={data.nodes} highlightedIds={highlightedIds} />}
      {landmarks && <HaloLayer nodes={data.nodes} />}

      {/* Missed skeleton (#963): white ghost of the not-fully-indexed files.
       * Clicks route through the same handler — GraphTab re-centers the
       * camera on the whole skeleton cluster. */}
      {missed && missed.nodes.length > 0 && (
        <group>
          <EdgeLines
            nodes={missed.nodes}
            edges={missed.edges}
            highlightedIds={null}
            opacity={0.28}
            brightness={display.edgeBrightness}
          />
          <NodeCloud
            nodes={missed.nodes}
            highlightedIds={null}
            onHover={setHovered}
            onClick={onNodeClick}
            opacity={0.6}
            boost={nodeBoost * 0.75}
          />
          {showLabels && <NodeLabels nodes={missed.nodes} highlightedIds={null} />}
        </group>
      )}

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
              brightness={display.edgeBrightness}
            />
            <NodeCloud
              nodes={offsetNodes}
              highlightedIds={null}
              onHover={setHovered}
              onClick={onNodeClick}
              opacity={0.5}
              boost={nodeBoost}
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
                brightness={display.edgeBrightness}
              />
            )}
          </group>
        );
      })}

      {hovered &&
        (renderTooltip ? renderTooltip(hovered) : <NodeTooltip node={hovered} />)}

      <CameraAnimator target={cameraTarget} controlsRef={controlsRef} />
      {onViewTarget && (
        <ViewTargetReporter controlsRef={controlsRef} onViewTarget={onViewTarget} />
      )}
      {onApproachNode && (
        <ApproachWatcher nodes={data.nodes} onApproachNode={onApproachNode} />
      )}
      <IdleAutoRotate controlsRef={controlsRef} />

      <EffectComposer multisampling={GRAPH_COMPOSER_MULTISAMPLING}>
        <Bloom
          luminanceThreshold={0.3}
          luminanceSmoothing={0.7}
          intensity={bloomIntensity}
          mipmapBlur
          radius={0.6}
        />
      </EffectComposer>

      <OrbitControls
        ref={controlsRef}
        enableDamping
        dampingFactor={0.08}
        rotateSpeed={0.5}
        zoomSpeed={1.5}
        minDistance={Math.max(5, sceneRadius * 0.02)}
        maxDistance={sceneRadius * 4}
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
