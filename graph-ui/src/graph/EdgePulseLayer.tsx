import { useEffect, useMemo, useRef } from 'react';
import { useFrame, useThree } from '@react-three/fiber';
import { BufferGeometry, Color, Float32BufferAttribute } from 'three';
import { edgeColor, edgePhase, isDirectedEdge } from './edge-style';
import { useEdgeMotion } from './edge-motion';

export interface EdgePulsePath {
    id: string | number; type: string; types?: readonly string[];
    points: readonly { x: number; y: number; z: number }[]; opacity?: number;
    /** The colour the view drew the line in, when it is not the hue of its type. */
    color?: string;
}

/** A full arrow is 1.65 times this scale in CSS pixels. Smaller embedded views
 * get smaller markers; even a large view stays below 4.5px, independent of zoom. */
export function edgeArrowScreenScale(width: number, height: number): number {
    const shortSide = Math.min(width, height);
    return Number.isFinite(shortSide) && shortSide > 0 ? Math.min(2.7, Math.max(1.2, shortSide * .0054)) : 1.2;
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
        const color = new Color(path.color ?? edgeColor(path.type, path.types)), phase = edgePhase(path.id);
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

/** One screen-facing triangle per curve segment; only the segment under the
 * travelling head is visible. This keeps every arrow in one GPU draw call. */
export function edgeArrowGeometry(paths: readonly EdgePulsePath[]): BufferGeometry {
    const position: number[] = [], starts: number[] = [], ends: number[] = [], colors: number[] = [], flows: number[] = [];
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
        const color = new Color(path.color ?? edgeColor(path.type, path.types));
        for (let i = 1; i < path.points.length; i++) {
            if (distances[i] === distances[i - 1]) continue;
            const a = path.points[i - 1], b = path.points[i];
            for (const [x, y] of [[1, 0], [-.65, .4], [-.65, -.4]]) {
                position.push(x, y, 0); starts.push(a.x, a.y, a.z); ends.push(b.x, b.y, b.z);
                colors.push(color.r, color.g, color.b);
                flows.push(distances[i - 1] / total, distances[i] / total, edgePhase(path.id), Math.min(1, path.opacity ?? .7) * .3);
            }
        }
    }
    const geometry = new BufferGeometry();
    geometry.setAttribute('position', new Float32BufferAttribute(position, 3));
    geometry.setAttribute('arrowStart', new Float32BufferAttribute(starts, 3));
    geometry.setAttribute('arrowEnd', new Float32BufferAttribute(ends, 3));
    geometry.setAttribute('color', new Float32BufferAttribute(colors, 3));
    geometry.setAttribute('arrowFlow', new Float32BufferAttribute(flows, 4));
    return geometry;
}

const vertexShader = `
uniform float uTime;
uniform vec2 uViewport;
uniform float uArrowScale;
attribute vec3 arrowStart;
attribute vec3 arrowEnd;
attribute vec4 arrowFlow;
attribute vec3 color;
varying vec3 vColor;
varying float vAlpha;
bool clipPlane(float a, float b, inout vec2 range) {
    if (a < 0.0 && b < 0.0) return false;
    if (a < 0.0) range.x = max(range.x, a / (a - b));
    if (b < 0.0) range.y = min(range.y, a / (a - b));
    return range.x <= range.y;
}
void main() {
    float head = 0.04 + fract(uTime / 6.0 + arrowFlow.z) * 0.92;
    float fraction = clamp((head - arrowFlow.x) / max(0.000001, arrowFlow.y - arrowFlow.x), 0.0, 1.0);
    vec4 a = projectionMatrix * modelViewMatrix * vec4(arrowStart, 1.0);
    vec4 b = projectionMatrix * modelViewMatrix * vec4(arrowEnd, 1.0);
    // Clip before dividing by w. Near-camera crossings must not flip direction
    // or create giant triangles when a source is behind the camera.
    vec2 range = vec2(0.0, 1.0);
    bool visible = clipPlane(a.w - 0.0001, b.w - 0.0001, range);
    visible = clipPlane(a.w + a.z, b.w + b.z, range) && visible;
    visible = clipPlane(a.w - a.z, b.w - b.z, range) && visible;
    visible = clipPlane(a.w + a.x, b.w + b.x, range) && visible;
    visible = clipPlane(a.w - a.x, b.w - b.x, range) && visible;
    visible = clipPlane(a.w + a.y, b.w + b.y, range) && visible;
    visible = clipPlane(a.w - a.y, b.w - b.y, range) && visible;
    if (!visible) {
        gl_Position = vec4(0.0, 0.0, 0.0, 1.0);
        vColor = color; vAlpha = 0.0;
        return;
    }
    vec4 clippedA = mix(a, b, range.x), clippedB = mix(a, b, range.y);
    vec2 direction = (clippedB.xy / max(0.0001, clippedB.w) - clippedA.xy / max(0.0001, clippedA.w)) * uViewport * 0.5;
    float screenLength = length(direction);
    bool safeProjection = a.w > 0.0001 && b.w > 0.0001 && a.z >= -a.w && b.z >= -b.w;
    // Measure original divided endpoints directly: the clipped world-space
    // fraction is not a linear screen-space fraction under perspective.
    float projectedSegment = screenLength;
    if (safeProjection) projectedSegment = length((b.xy / b.w - a.xy / a.w) * uViewport * 0.5);
    float edgeLength = safeProjection ? projectedSegment / max(0.000001, arrowFlow.y - arrowFlow.x) : screenLength;
    bool closeUp = !safeProjection || edgeLength > max(uViewport.x, uViewport.y) * 1.5;
    bool headHere = head >= arrowFlow.x && head < arrowFlow.y && fraction >= range.x && fraction <= range.y;
    vec4 center = mix(a, b, fraction);
    if (closeUp && screenLength > 64.0) {
        // Long edges need a marker in their visible span rather than spending
        // most of the animation off-screen. Require at least 64 visible pixels
        // per fallback marker so curve tessellation cannot flood the view.
        float progress = fract(uTime * 42.0 / max(24.0, screenLength) + arrowFlow.z + arrowFlow.x);
        vec3 from = clippedA.xyz / max(0.0001, clippedA.w);
        vec3 to = clippedB.xyz / max(0.0001, clippedB.w);
        center = vec4(mix(from, to, 0.04 + progress * 0.92), 1.0);
        headHere = true;
    }
    direction = screenLength > 0.0001 ? direction / screenLength : vec2(1.0, 0.0);
    vec2 normal = vec2(-direction.y, direction.x);
    // Distant, subpixel edges must not grow arrow-shaped spikes. Keep the
    // screen cap, then taper the marker with its projected relationship.
    float markerReach = range.x > 0.0 || range.y < 1.0 ? min(edgeLength, screenLength) : edgeLength;
    float arrowScale = min(uArrowScale, max(0.15, markerReach * 0.08));
    vec2 pixelOffset = (direction * position.x + normal * position.y) * arrowScale;
    center.xy += pixelOffset * 2.0 / max(uViewport, vec2(1.0)) * center.w;
    gl_Position = center;
    vColor = color;
    vAlpha = visible && headHere && screenLength > 0.01 ? arrowFlow.w * smoothstep(2.0, 12.0, markerReach) : 0.0;
}`;
const fragmentShader = `
varying vec3 vColor;
varying float vAlpha;
void main() {
    if (vAlpha <= 0.0) discard;
    gl_FragColor = vec4(vColor, vAlpha);
    #include <tonemapping_fragment>
    #include <colorspace_fragment>
}`;

/** Direction remains visible as a stationary arrow when motion is reduced. */
export function EdgePulseLayer({ paths, active = true }: { paths: readonly EdgePulsePath[]; active?: boolean }) {
    const motion = useEdgeMotion(active), { invalidate, size } = useThree();
    const geometry = useMemo(() => edgeArrowGeometry(paths), [paths]);
    const uniforms = useMemo(() => ({ uTime: { value: 0 }, uViewport: { value: [size.width, size.height] },
        uArrowScale: { value: edgeArrowScreenScale(size.width, size.height) } }), []);
    const lastTime = useRef<number | undefined>(undefined);
    const enabled = motion && geometry.getAttribute('position').count > 0;
    useEffect(() => {
        uniforms.uViewport.value = [size.width, size.height];
        uniforms.uArrowScale.value = edgeArrowScreenScale(size.width, size.height);
        invalidate();
    }, [size.width, size.height, uniforms, invalidate]);
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
    return <mesh geometry={geometry} visible={active} frustumCulled={false} renderOrder={4} raycast={() => null} dispose={null}>
        <shaderMaterial vertexShader={vertexShader} fragmentShader={fragmentShader} uniforms={uniforms} transparent depthWrite={false} side={2} />
    </mesh>;
}
