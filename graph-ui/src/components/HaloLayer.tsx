import { useMemo } from "react";
import * as THREE from "three";
import type { GraphNode } from "../lib/types";

/* Landmark halos: the top hubs get a soft additive corona so the scene has
 * stable visual anchors while panning — the CodeCity landmark idea. Kept to
 * a handful so the halo stays a landmark, not decoration. */

const HALO_COUNT = 12;

let haloSprite: THREE.CanvasTexture | null = null;
function getHaloSprite(): THREE.CanvasTexture {
  if (haloSprite) return haloSprite;
  const size = 128;
  const canvas = document.createElement("canvas");
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext("2d")!;
  const gradient = ctx.createRadialGradient(
    size / 2,
    size / 2,
    0,
    size / 2,
    size / 2,
    size / 2,
  );
  gradient.addColorStop(0, "rgba(255,255,255,0.9)");
  gradient.addColorStop(0.35, "rgba(255,255,255,0.25)");
  gradient.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, size, size);
  haloSprite = new THREE.CanvasTexture(canvas);
  return haloSprite;
}

export function HaloLayer({ nodes }: { nodes: GraphNode[] }) {
  const { geometry, material } = useMemo(() => {
    const landmarks = [...nodes]
      .sort((a, b) => b.size - a.size)
      .slice(0, Math.min(HALO_COUNT, nodes.length));
    const positions = new Float32Array(landmarks.length * 3);
    const colors = new Float32Array(landmarks.length * 3);
    const temp = new THREE.Color();
    landmarks.forEach((node, index) => {
      positions[index * 3] = node.x;
      positions[index * 3 + 1] = node.y;
      positions[index * 3 + 2] = node.z;
      temp.set(node.color);
      colors[index * 3] = temp.r;
      colors[index * 3 + 1] = temp.g;
      colors[index * 3 + 2] = temp.b;
    });
    const geometry = new THREE.BufferGeometry();
    geometry.setAttribute("position", new THREE.BufferAttribute(positions, 3));
    geometry.setAttribute("color", new THREE.BufferAttribute(colors, 3));
    const sizes = landmarks.map((node) => node.size);
    const haloSize = Math.max(...sizes, 1) * 6;
    const material = new THREE.PointsMaterial({
      size: haloSize,
      map: getHaloSprite(),
      vertexColors: true,
      transparent: true,
      opacity: 0.28,
      depthWrite: false,
      blending: THREE.AdditiveBlending,
      sizeAttenuation: true,
    });
    return { geometry, material };
  }, [nodes]);

  if (nodes.length === 0) return null;
  return <points geometry={geometry} material={material} />;
}
