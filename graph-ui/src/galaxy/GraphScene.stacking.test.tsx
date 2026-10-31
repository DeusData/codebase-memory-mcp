// @vitest-environment jsdom
/*
 * Handtest 2026-10-04 (G1): der Name des Pfadziels lag ueber dem Text von
 * "Selection details". drei haengt jedes <Html> der Szene als Geschwister des
 * Canvas in dessen Huelle und gibt ihm einen eigenen z-index (bis 10, die
 * Agentenebene bis 80). Ohne eigenen Stapelkontext der Huelle stehen diese
 * z-indizes im selben Stapel wie die Bedienflaechen der Galaxie, und "Selection
 * details" (4) lag darunter. Hier steht, dass die Huelle ihren Stapel bildet:
 * alles, was die Szene als DOM zeichnet, bleibt darin und damit unter jeder
 * Bedienflaeche mit z-index ab 1, und ueber dem Canvas selbst.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it } from 'vitest';
import { GraphScene } from './GraphScene';

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    // jsdom misst nichts; mit der Groesse null setzt der Canvas keine WebGL-Wurzel auf, die Huelle steht trotzdem.
    (globalThis as unknown as Record<string, unknown>).ResizeObserver ??= class { observe() {} unobserve() {} disconnect() {} };
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(() => { act(() => root.unmount()); host.remove(); });

it('G1: the canvas wrapper that hosts every scene label forms its own stacking context', () => {
    const node = { id: 1, x: 0, y: 0, z: 0, name: 'n1', label: 'Function', size: 3, color: '#abcdef' };
    act(() => root.render(<GraphScene active={false} data={{ nodes: [node], edges: [], total_nodes: 1 }} highlightedIds={null} cameraTarget={null}
        showLabels onNodeClick={() => {}} />));
    const canvas = host.querySelector('canvas');
    expect(canvas).not.toBeNull();
    // r3f: <div (events, Html-Ziel)><div (Messung)><canvas/></div></div>.
    const wrapper = canvas!.parentElement!.parentElement!;
    expect(wrapper.parentElement).toBe(host);
    expect(wrapper.style.position).toBe('relative');
    expect(wrapper.style.isolation).toBe('isolate');
});
