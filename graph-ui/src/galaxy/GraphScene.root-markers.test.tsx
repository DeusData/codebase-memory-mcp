// @vitest-environment jsdom
/*
 * Handtest K12: im Mini-Galaxy von Explore lagen die Namen der markierten
 * Wurzeln aufeinander. Die Namen nehmen jetzt freie Plaetze um ihre Ringe
 * (marker-names.ts); hier steht, dass die Szene sie dorthin setzt.
 */
import { act, type ReactNode } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { PerspectiveCamera } from 'three';
import { RootMarkers } from './GraphScene';
import type { GraphNode } from './types';

const fiber = vi.hoisted(() => ({ frames: [] as ((state: unknown, delta: number) => void)[] }));
let scene: { camera: PerspectiveCamera; gl: { domElement: HTMLCanvasElement }; size: { width: number; height: number } };
vi.mock('@react-three/fiber', async importOriginal => ({
    ...await importOriginal<typeof import('@react-three/fiber')>(),
    useFrame: (callback: (state: unknown, delta: number) => void) => { fiber.frames.push(callback); },
    useThree: (select?: (state: unknown) => unknown) => (select ? select(scene) : scene),
}));
vi.mock('@react-three/drei', async importOriginal => ({
    ...await importOriginal<typeof import('@react-three/drei')>(),
    Html: ({ children }: { children: ReactNode }) => <div data-testid="html">{children}</div>,
}));

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const canvas = document.createElement('canvas');
    canvas.getBoundingClientRect = () => ({ left: 0, top: 0, right: 800, bottom: 600, width: 800, height: 600, x: 0, y: 0, toJSON: () => ({}) });
    scene = { camera: new PerspectiveCamera(50, 800 / 600, 0.1, 100000), gl: { domElement: canvas }, size: { width: 800, height: 600 } };
    scene.camera.position.set(0, 0, 800); scene.camera.lookAt(0, 0, 0); scene.camera.updateMatrixWorld(); scene.camera.updateProjectionMatrix();
    fiber.frames = [];
    // jsdom lays nothing out: every name is 90 x 18 px.
    Object.defineProperty(HTMLElement.prototype, 'offsetWidth', { configurable: true, get() { return (this as HTMLElement).tagName === 'B' ? 90 : 0; } });
    Object.defineProperty(HTMLElement.prototype, 'offsetHeight', { configurable: true, get() { return (this as HTMLElement).tagName === 'B' ? 18 : 0; } });
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(() => {
    act(() => root.unmount()); host.remove();
    delete (HTMLElement.prototype as unknown as Record<string, unknown>).offsetWidth;
    delete (HTMLElement.prototype as unknown as Record<string, unknown>).offsetHeight;
});
const node = (id: number, x: number, y: number): GraphNode => ({ id, x, y, z: 0, name: `name ${id}`, label: 'Variable', size: 3, color: '#abcdef' });
const runFrames = (count: number) => act(() => { for (let i = 0; i < count; i++) for (const frame of fiber.frames) frame({}, 1 / 60); });

it('K12: root names that would lie on each other take different places, a lone one stays under its ring', () => {
    act(() => root.render(<RootMarkers nodes={[node(1, 0, 0)]} />));
    runFrames(6);
    expect(host.querySelector('[data-testid="atlas-galaxy-root-marker"]')?.getAttribute('data-name-slot')).toBe('below');

    act(() => root.render(<RootMarkers nodes={[node(1, 0, 0), node(2, 2, 1), node(3, -2, -1)]} />));
    runFrames(6);
    const markers = [...host.querySelectorAll<HTMLElement>('[data-testid="atlas-galaxy-root-marker"]')];
    const slots = markers.map(marker => marker.getAttribute('data-name-slot'));
    expect(slots[0]).toBe('below');
    // No two shown names share a place; a name without a free place is hidden, never laid over another.
    const shown = slots.filter(slot => slot !== 'hidden');
    expect(new Set(shown).size).toBe(shown.length);
    expect(shown.length).toBeGreaterThanOrEqual(2);
    for (const marker of markers) {
        const name = marker.querySelector('b')!;
        expect(name.style.visibility).toBe(marker.getAttribute('data-name-slot') === 'hidden' ? 'hidden' : '');
    }
});
