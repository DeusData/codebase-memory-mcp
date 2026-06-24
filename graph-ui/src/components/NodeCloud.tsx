import { useMemo, useRef } from "react";
import { useFrame } from "@react-three/fiber";
import * as THREE from "three";
import type { GraphNode } from "../lib/types";

interface NodeCloudProps {
  nodes: GraphNode[];
  highlightedIds: Set<number> | null;
  onHover: (node: GraphNode | null) => void;
  onClick: (node: GraphNode) => void;
  opacity?: number;
}

export function NodeCloud({
  nodes,
  highlightedIds,
  onHover,
  onClick,
  opacity = 1.0,
}: NodeCloudProps) {
  const meshRef = useRef<THREE.InstancedMesh>(null);
  const tempObj = useMemo(() => new THREE.Object3D(), []);
  const tempColor = useMemo(() => new THREE.Color(), []);

  /* Build instance color attributes — dim non-highlighted nodes */
  const colors = useMemo(() => {
    const arr = new Float32Array(nodes.length * 3);
    const hasHighlight = highlightedIds && highlightedIds.size > 0;

    for (let i = 0; i < nodes.length; i++) {
      tempColor.set(nodes[i].color);
      if (hasHighlight && !highlightedIds.has(nodes[i].id)) {
        tempColor.multiplyScalar(0.15);
      } else {
        /* Boost above 1.0 so bloom picks up the excess as glow corona.
         * Hotter stars (white/blue) get a stronger boost = brighter halo. */
        const brightness = (tempColor.r + tempColor.g + tempColor.b) / 3;
        const boost = 1.2 + brightness * 0.8; /* 1.2x for red, 2.0x for white */
        tempColor.multiplyScalar(boost);
      }
      arr[i * 3] = tempColor.r * opacity;
      arr[i * 3 + 1] = tempColor.g * opacity;
      arr[i * 3 + 2] = tempColor.b * opacity;
    }
    return arr;
  }, [nodes, highlightedIds, tempColor, opacity]);

  useFrame(() => {
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
  });

  return (
    <instancedMesh
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
      <sphereGeometry args={[1, 32, 24]} />
      <meshBasicMaterial vertexColors toneMapped={false} />
      <instancedBufferAttribute
        attach="geometry-attributes-color"
        args={[colors, 3]}
      />
    </instancedMesh>
  );
}
