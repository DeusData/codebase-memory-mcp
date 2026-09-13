import { useEffect, useMemo, useRef } from 'react';
import { useFrame, useThree } from '@react-three/fiber';
import { BufferGeometry, Color, Float32BufferAttribute } from 'three';
import { edgeColor, edgePhase, isDirectedEdge } from './edge-style';
import { useEdgeMotion } from './edge-motion';

export interface EdgePulsePath {
    id: string | number; type: string; types?: readonly string[];
    points: readonly { x: number; y: number; z: number }[]; opacity?: number;
}
/** One line batch for every curve; progress is measured from semantic source to target. */
export function edgePulseGeometry(paths: readonly EdgePulsePath[]): BufferGeometry {
    const positions: number[] = [], colors: number[] = [], flow: number[] = [];
    for (const path of paths) {
        if (!isDirectedEdge(path.type, path.types) || path.points.length < 2 || (path.opacity ?? 1) <= 0) continue;
        if (path.points.some(point => ![point.x, point.y, point.z].every(Number.isFinite))) continue;
        const distances = [0];
        for (let i = 1; i < path.points.length; i++) {
            const a = path.points[i - 1], b = path.points[i];
            distances.push(distances[i - 1] + Math.hypot(b.x - a.x, b.y - a.y, b.z - a.z));
        }
        const total = distances.at(-1)!;
        if (!total) continue;
        const color = new Color(edgeColor(path.type, path.types)), phase = edgePhase(path.id);
        for (let i = 1; i < path.points.length; i++) for (const index of [i - 1, i]) {
            const point = path.points[index];
            positions.push(point.x, point.y, point.z); colors.push(color.r, color.g, color.b);
            flow.push(distances[index] / total, phase, Math.max(0, Math.min(1, path.opacity ?? .7)));
        }
    }
    const geometry = new BufferGeometry();
    geometry.setAttribute('position', new Float32BufferAttribute(positions, 3));
    geometry.setAttribute('color', new Float32BufferAttribute(colors, 3));
    geometry.setAttribute('edgeFlow', new Float32BufferAttribute(flow, 3));
    geometry.computeBoundingSphere();
    return geometry;
}

const vertexShader = `
attribute vec3 edgeFlow;
attribute vec3 color;
varying vec3 vFlow;
varying vec3 vColor;
void main() {
    vFlow = edgeFlow;
    vColor = color;
    gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
}`;
const fragmentShader = `
uniform float uTime;
varying vec3 vFlow;
varying vec3 vColor;
void main() {
    float age = fract(uTime / 6.0 + vFlow.y - vFlow.x + 1.0);
    float pulse = 1.0 - smoothstep(0.0, 0.12, age);
    float ends = smoothstep(0.0, 0.025, vFlow.x) * (1.0 - smoothstep(0.975, 1.0, vFlow.x));
    gl_FragColor = vec4(vColor * 1.2, vFlow.z * pulse * ends);
    #include <tonemapping_fragment>
    #include <colorspace_fragment>
}`;

/** A single clock and draw call, sleeping when paused, hidden or motion is reduced. */
export function EdgePulseLayer({ paths, active = true }: { paths: readonly EdgePulsePath[]; active?: boolean }) {
    const motion = useEdgeMotion(active), { invalidate } = useThree();
    const geometry = useMemo(() => edgePulseGeometry(paths), [paths]);
    const uniforms = useMemo(() => ({ uTime: { value: 0 } }), []);
    const lastTime = useRef<number | undefined>(undefined);
    const enabled = motion && geometry.getAttribute('position').count > 0;
    useEffect(() => () => geometry.dispose(), [geometry]);
    useEffect(() => {
        lastTime.current = undefined;
        invalidate();
        if (!enabled) return;
        const timer = window.setInterval(invalidate, 1000 / 24);
        return () => window.clearInterval(timer);
    }, [enabled, invalidate]);
    useFrame(() => {
        if (!enabled) return;
        const now = performance.now() / 1000;
        if (lastTime.current !== undefined) uniforms.uTime.value += now - lastTime.current;
        lastTime.current = now;
    });
    return <lineSegments geometry={geometry} visible={enabled} renderOrder={4} raycast={() => null} dispose={null}>
        <shaderMaterial vertexShader={vertexShader} fragmentShader={fragmentShader} uniforms={uniforms} transparent depthWrite={false} />
    </lineSegments>;
}
