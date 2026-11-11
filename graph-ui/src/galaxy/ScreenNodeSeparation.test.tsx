// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { OrthographicCamera, PerspectiveCamera } from 'three';
import { ScreenNodeSeparation } from './ScreenNodeSeparation';
import type { GraphNode } from './types';

const frames = vi.hoisted(() => ({ callback: undefined as (() => void) | undefined }));
vi.mock('@react-three/fiber', () => ({
    useFrame: (callback: () => void) => { frames.callback = callback; },
    useThree: () => scene,
}));

let scene: { camera: PerspectiveCamera | OrthographicCamera; size: { width: number; height: number }; invalidate: ReturnType<typeof vi.fn> };
let host: HTMLDivElement;
let root: Root;
let now: number;
let nextFrame: number;
let queued: Map<number, FrameRequestCallback>;
let nodes: GraphNode[];
const onChange = vi.fn<(source: GraphNode[], positioned: GraphNode[]) => void>();
const onBusyChange = vi.fn<(busy: boolean) => void>();

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    now = 0; nextFrame = 0; queued = new Map();
    vi.spyOn(performance, 'now').mockImplementation(() => now);
    vi.stubGlobal('requestAnimationFrame', (callback: FrameRequestCallback) => {
        const id = ++nextFrame; queued.set(id, callback); return id;
    });
    vi.stubGlobal('cancelAnimationFrame', (id: number) => queued.delete(id));
    scene = { camera: new PerspectiveCamera(50, 1, .1, 2000), size: { width: 600, height: 600 }, invalidate: vi.fn() };
    nodes = [0, -100, -200].map((z, id) => ({ id, x: 0, y: 0, z, size: 8, label: 'Function', name: `node${id}`, color: '#abcdef' }));
    onChange.mockReset(); onBusyChange.mockReset();
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(() => {
    act(() => root.unmount()); host.remove();
    vi.restoreAllMocks(); vi.unstubAllGlobals(); frames.callback = undefined;
});

const render = () => act(() => root.render(<ScreenNodeSeparation nodes={nodes} active onChange={onChange} onBusyChange={onBusyChange} />));
const frame = (elapsed = 0) => act(() => { now += elapsed; frames.callback?.(); });
const paint = () => act(() => {
    const pending = [...queued.values()]; queued.clear();
    for (const callback of pending) callback(now);
});
const settle = () => { frame(); frame(120); paint(); paint(); };
const initialLayout = () => {
    scene.camera.position.set(0, 0, 500); scene.camera.lookAt(0, 0, 0); scene.camera.updateMatrixWorld();
    render(); settle();
    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onChange.mock.calls[0]![1]).not.toEqual(nodes);
    onBusyChange.mockClear(); scene.invalidate.mockClear();
};

it.each(['perspective zoom', 'orthographic zoom', 'pan', 'orbit'] as const)('keeps the committed node positions during %s', (movement) => {
    if (movement === 'orthographic zoom') scene.camera = new OrthographicCamera(-150, 150, 150, -150, .1, 2000);
    initialLayout();
    for (const direction of [1, -1]) {
        if (movement === 'perspective zoom') scene.camera.position.z += direction * 150;
        if (movement === 'orthographic zoom') { scene.camera.zoom += direction * .5; scene.camera.updateProjectionMatrix(); }
        if (movement === 'pan') scene.camera.position.x += direction * 80;
        if (movement === 'orbit') { scene.camera.position.x += direction * 150; scene.camera.lookAt(0, 0, 0); }
        settle();
    }
    expect(onChange).toHaveBeenCalledTimes(1);
    expect(onBusyChange).not.toHaveBeenCalled();
    expect(scene.invalidate).not.toHaveBeenCalled();
});

it.each(['graph', 'viewport'] as const)('still arranges a changed %s', (change) => {
    initialLayout();
    if (change === 'graph') nodes = [...nodes, { ...nodes[0]!, id: 9 }];
    else scene.size = { width: 800, height: 600 };
    render(); settle();
    expect(onChange).toHaveBeenCalledTimes(2);
    expect(onChange.mock.calls[1]![0]).toBe(nodes);
    expect(onBusyChange.mock.calls.map(call => call[0])).toEqual([true, false]);
    expect(scene.invalidate).toHaveBeenCalledTimes(1);
});

it('waits for the initial camera fit to settle before arranging a new graph', () => {
    render(); frame();
    scene.camera.position.z = 500; scene.camera.lookAt(0, 0, 0);
    frame(60); frame(60); paint(); paint();
    expect(onChange).not.toHaveBeenCalled();
    frame(100); paint(); paint();
    expect(onChange).toHaveBeenCalledTimes(1);
});
