// @vitest-environment jsdom
import { act, createRef } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { PerspectiveCamera, Vector3 } from 'three';
import type { OrbitControls as OrbitControlsImpl } from 'three-stdlib';
import { CameraAnimator, FitContainment, type CameraTarget } from './GraphScene';
import { FRAME_MARGIN, FRAME_MIN_DISTANCE } from './camera-frame';
import type { GraphNode } from './types';

const fiber = vi.hoisted(() => ({ frames: [] as ((state: unknown, delta: number) => void)[] }));
vi.mock('@react-three/fiber', async importOriginal => ({
    ...await importOriginal<typeof import('@react-three/fiber')>(),
    useFrame: (callback: (state: unknown, delta: number) => void) => { fiber.frames.push(callback); },
    useThree: (select?: (state: unknown) => unknown) => (select ? select(scene) : scene),
}));

let scene: { camera: PerspectiveCamera; size: { width: number; height: number } };
let host: HTMLDivElement, root: Root;
const controlsRef = createRef<OrbitControlsImpl | null>() as { current: OrbitControlsImpl | null };
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    scene = { camera: new PerspectiveCamera(50, 1.5, 0.1, 100000), size: { width: 900, height: 600 } };
    scene.camera.position.set(0, 0, 800); scene.camera.lookAt(0, 0, 0); scene.camera.updateMatrixWorld();
    fiber.frames = []; controlsRef.current = null;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(() => { act(() => root.unmount()); host.remove(); });
const runFrames = (count: number) => act(() => { for (let i = 0; i < count; i++) fiber.frames.at(-1)?.({}, 1 / 60); });
const flyTo = (x: number): CameraTarget => ({ position: new Vector3(x, 0, 300), lookAt: new Vector3(x, 0, 0) });
const fitTo = (z: number): CameraTarget => ({ position: new Vector3(0, 0, z), lookAt: new Vector3(0, 0, 0), immediate: true });

describe('CameraAnimator', () => {
    it('does not replay a fly-to that was set before the scene mounted, but flies to the next one', () => {
        act(() => root.render(<CameraAnimator target={flyTo(500)} controlsRef={controlsRef} />));
        runFrames(120);
        expect(scene.camera.position.toArray()).toEqual([0, 0, 800]);
        act(() => root.render(<CameraAnimator target={flyTo(-200)} controlsRef={controlsRef} />));
        runFrames(120);
        // The fly-to eases toward its goal; it is clearly on its way there.
        expect(scene.camera.position.x).toBeLessThan(-150);
    });

    it('still applies a fit that is present at mount', () => {
        act(() => root.render(<CameraAnimator target={fitTo(420)} controlsRef={controlsRef} />));
        expect(scene.camera.position.toArray()).toEqual([0, 0, 420]);
    });
});

describe('FitContainment', () => {
    const node = (id: number, x: number, y: number): GraphNode => ({ id, x, y, z: 0, name: `n${id}`, label: 'Function', size: 3, color: '#abcdef' });
    const moved = { current: false };
    const render = (nodes: GraphNode[], target: CameraTarget | null) => act(() => root.render(
        <FitContainment nodes={nodes} target={target} controlsRef={controlsRef} moved={moved} enabled />));

    const half = Math.tan(25 * Math.PI / 180);
    /** How much of the half-height the outermost node uses, margin included. */
    const reach = (y: number) => y * FRAME_MARGIN / (scene.camera.position.z * half);

    it('frames the next picture along the kept view direction: back for a larger one, forward for a smaller one', () => {
        moved.current = false;
        const fit = fitTo(800);
        render([node(1, 0, 0), node(2, 0, 900)], fit);
        expect(scene.camera.position.x).toBe(0); expect(scene.camera.position.y).toBe(0);
        expect(scene.camera.position.z).toBeGreaterThan(800);
        expect(reach(900)).toBeCloseTo(1, 4);
        render([node(1, 0, 0), node(2, 0, 300)], fit);
        expect(reach(300)).toBeCloseTo(1, 4);
        // Never closer to the pivot than any fit would stand.
        render([node(1, 0, 0)], fit);
        expect(scene.camera.position.z).toBeCloseTo(FRAME_MIN_DISTANCE, 6);
    });

    it('leaves the camera alone after the reader moved it or after a fly-to', () => {
        moved.current = false;
        const fit = fitTo(800);
        render([node(1, 0, 0), node(2, 0, 300)], fit);
        const framed = scene.camera.position.z;
        moved.current = true;
        render([node(1, 0, 0), node(2, 0, 900)], fit);
        expect(scene.camera.position.z).toBe(framed);
        const fly = flyTo(0);
        render([node(1, 0, 0), node(2, 0, 900)], fly);
        render([node(1, 0, 0), node(2, 0, 1900)], fly);
        expect(scene.camera.position.z).toBe(framed);
    });
});
