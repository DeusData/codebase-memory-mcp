// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import GraphEdgeControls from './GraphEdgeControls';
import { setEdgeMotionPreference, useEdgeMotion } from './edge-motion';

let container: HTMLDivElement, root: Root, reduced: boolean;
let hidden: ReturnType<typeof vi.spyOn>;
const mediaListeners = new Set<() => void>();
function Probe({ id, active = true }: { id: string; active?: boolean }) { return <output data-testid={id}>{String(useEdgeMotion(active))}</output>; }
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    reduced = false; mediaListeners.clear();
    vi.stubGlobal('matchMedia', vi.fn(() => ({ get matches() { return reduced; },
        addEventListener: (_type: string, listener: () => void) => mediaListeners.add(listener), removeEventListener: (_type: string, listener: () => void) => mediaListeners.delete(listener) })));
    hidden = vi.spyOn(document, 'hidden', 'get').mockReturnValue(false);
    const values = new Map<string, string>();
    vi.stubGlobal('localStorage', {
        getItem: (key: string) => values.get(key) ?? null,
        setItem: (key: string, value: string) => { values.set(key, value); },
        clear: () => values.clear(),
    });
    localStorage.clear(); setEdgeMotionPreference(true);
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });
const state = (id: string) => container.querySelector(`[data-testid="${id}"]`)?.textContent;
async function render() { await act(async () => root.render(<><GraphEdgeControls /><Probe id="first" /><Probe id="second" /><Probe id="inactive" active={false} /></>)); }

describe('global edge motion preference', () => {
    it('pauses every mounted scene through one persistent control', async () => {
        await render();
        expect(state('first')).toBe('true'); expect(state('inactive')).toBe('false');
        await act(async () => container.querySelector<HTMLInputElement>('input')!.click());
        expect(state('first')).toBe('false'); expect(state('second')).toBe('false');
        expect(localStorage.getItem('cbm-edge-motion-v1')).toBe('off');
        await act(async () => container.querySelector<HTMLInputElement>('input')!.click());
        expect(state('first')).toBe('true'); expect(state('second')).toBe('true');
    });
    it('sleeps when hidden or reduced-motion is requested and resumes without changing the preference', async () => {
        await render();
        await act(async () => { hidden.mockReturnValue(true); document.dispatchEvent(new Event('visibilitychange')); });
        expect(state('first')).toBe('false');
        await act(async () => { hidden.mockReturnValue(false); document.dispatchEvent(new Event('visibilitychange')); reduced = true; mediaListeners.forEach(listener => listener()); });
        expect(state('first')).toBe('false'); expect(container.querySelector('input')!.disabled).toBe(true);
        expect(localStorage.getItem('cbm-edge-motion-v1')).toBe('on');
        await act(async () => { reduced = false; mediaListeners.forEach(listener => listener()); });
        expect(state('first')).toBe('true'); expect(state('inactive')).toBe('false');
    });
    it('retains a working session toggle when persistent storage rejects writes', async () => {
        await render();
        vi.spyOn(window.localStorage, 'setItem').mockImplementation(() => { throw new Error('Storage disabled'); });
        await act(async () => container.querySelector<HTMLInputElement>('input')!.click());
        expect(state('first')).toBe('false'); expect(state('second')).toBe('false');
    });
});
