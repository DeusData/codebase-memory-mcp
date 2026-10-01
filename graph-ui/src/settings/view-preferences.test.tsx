// @vitest-environment jsdom
import { act, useState } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
    DEFAULT_VIEW_PREFERENCES, readViewPreferences, resetViewPreferences, setViewPreferences,
    useViewPreferences, viewPreferencesKey, type ViewPreferences,
} from './view-preferences';

type Controls = ReturnType<typeof useViewPreferences>;
let container: HTMLDivElement, root: Root;
const controls = new Map<string, Controls>();
let nextProject = 0;
const project = () => `view-preference-test-${++nextProject}`;
const store = (name: string, preferences: unknown, version = 1) => window.localStorage.setItem(viewPreferencesKey(name), JSON.stringify({ version, preferences }));
function Probe({ name, id = name }: { name: string; id?: string }) {
    const state = useViewPreferences(name);
    const [selected, select] = useState(false);
    controls.set(id, state);
    return <button data-testid={id} onClick={() => select(true)}>{JSON.stringify({ ...state.preferences, selected })}</button>;
}
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    // Node exposes its own storage global; use the actual jsdom browser store.
    vi.stubGlobal('localStorage', (globalThis as unknown as { jsdom: { window: Window } }).jsdom.window.localStorage);
    controls.clear(); window.localStorage.clear();
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe('per-project view preferences', () => {
    it('restores the supported limits, appearance choices and coverage preference', async () => {
        const name = project();
        const saved: ViewPreferences = { galaxyNodes: 25000, galaxyEdges: 200000, coverageShadow: false,
            brickHeight: 'uniform', brickColor: 'kind', hotspotGravity: false, fileVisibility: 'unconnected' };
        store(name, saved);
        await act(async () => root.render(<Probe name={name} />));
        expect(controls.get(name)!.preferences).toEqual(saved);
        await act(async () => root.render(null));
        await act(async () => root.render(<Probe name={name} />));
        expect(controls.get(name)!.preferences).toEqual(saved);
    });

    it('rejects malformed data, unknown versions and values outside the supported bounded choices', () => {
        const name = project();
        window.localStorage.setItem(viewPreferencesKey(name), '{bad json');
        expect(readViewPreferences(name)).toEqual(DEFAULT_VIEW_PREFERENCES);
        store(name, { galaxyNodes: 500 }, 99);
        expect(readViewPreferences(name)).toEqual(DEFAULT_VIEW_PREFERENCES);
        store(name, { galaxyNodes: 50001, galaxyEdges: -1, coverageShadow: 'false', brickHeight: 'huge',
            brickColor: 'random', hotspotGravity: 0, fileVisibility: 'isolated' });
        expect(readViewPreferences(name)).toEqual(DEFAULT_VIEW_PREFERENCES);
        store(name, { galaxyNodes: 500.5, galaxyEdges: '5000', coverageShadow: false, brickColor: 'kind' });
        expect(readViewPreferences(name)).toEqual({ ...DEFAULT_VIEW_PREFERENCES, coverageShadow: false, brickColor: 'kind' });
    });

    it('updates other mounted consumers immediately without losing their local selection state', async () => {
        const name = project();
        await act(async () => root.render(<><Probe name={name} id="config" /><Probe name={name} id="scene" /></>));
        await act(async () => container.querySelector<HTMLButtonElement>('[data-testid="scene"]')!.click());
        await act(async () => controls.get('config')!.setPreferences({ galaxyNodes: 10000, hotspotGravity: false }));
        expect(controls.get('scene')!.preferences.galaxyNodes).toBe(10000);
        expect(controls.get('scene')!.preferences.hotspotGravity).toBe(false);
        expect(container.querySelector('[data-testid="scene"]')!.textContent).toContain('"selected":true');
        expect(JSON.parse(window.localStorage.getItem(viewPreferencesKey(name))!).preferences.galaxyNodes).toBe(10000);
    });

    it('merges successive consumer updates against current state rather than an old render', async () => {
        const name = project();
        await act(async () => root.render(<Probe name={name} />));
        const change = controls.get(name)!.setPreferences;
        await act(async () => {
            change({ galaxyNodes: 1000 });
            change(current => ({ coverageShadow: !current.coverageShadow }));
            change({ brickHeight: 'uniform' });
        });
        expect(controls.get(name)!.preferences).toEqual({ ...DEFAULT_VIEW_PREFERENCES,
            galaxyNodes: 1000, coverageShadow: false, brickHeight: 'uniform' });
    });

    it('keeps each project isolated when switching a mounted consumer and using an older callback', async () => {
        const first = project(), second = project();
        store(first, { galaxyNodes: 500 }); store(second, { galaxyNodes: 25000 });
        await act(async () => root.render(<Probe name={first} id="changing" />));
        const changeFirst = controls.get('changing')!.setPreferences;
        await act(async () => root.render(<Probe name={second} id="changing" />));
        expect(controls.get('changing')!.preferences.galaxyNodes).toBe(25000);
        await act(async () => changeFirst({ galaxyNodes: 1000 }));
        expect(controls.get('changing')!.preferences.galaxyNodes).toBe(25000);
        expect(readViewPreferences(first).galaxyNodes).toBe(1000);
        await act(async () => controls.get('changing')!.setPreferences({ galaxyEdges: 5000 }));
        expect(readViewPreferences(first).galaxyEdges).toBe(20000);
        expect(readViewPreferences(second).galaxyEdges).toBe(5000);
    });

    it('notifies only the matching project on a cross-tab storage update and handles a cleared store', async () => {
        const first = project(), second = project();
        await act(async () => root.render(<><Probe name={first} /><Probe name={second} /></>));
        const secondSnapshot = controls.get(second)!.preferences;
        await act(async () => {
            store(first, { galaxyNodes: 50000 });
            window.dispatchEvent(new StorageEvent('storage', { key: viewPreferencesKey(first), storageArea: localStorage }));
        });
        expect(controls.get(first)!.preferences.galaxyNodes).toBe(50000);
        expect(controls.get(second)!.preferences).toBe(secondSnapshot);
        await act(async () => {
            window.localStorage.clear(); window.dispatchEvent(new StorageEvent('storage', { key: null, storageArea: localStorage }));
        });
        expect(controls.get(first)!.preferences).toEqual(DEFAULT_VIEW_PREFERENCES);
    });

    it('retains working session preferences when storage reads and writes are unavailable', async () => {
        const name = project();
        vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => { throw new Error('Storage disabled'); });
        vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => { throw new Error('Storage disabled'); });
        vi.spyOn(Storage.prototype, 'removeItem').mockImplementation(() => { throw new Error('Storage disabled'); });
        await act(async () => root.render(<><Probe name={name} id="one" /><Probe name={name} id="two" /></>));
        expect(controls.get('one')!.preferences).toEqual(DEFAULT_VIEW_PREFERENCES);
        await act(async () => controls.get('one')!.setPreferences({ coverageShadow: false }));
        expect(controls.get('two')!.preferences.coverageShadow).toBe(false);
        await act(async () => controls.get('two')!.resetPreferences());
        expect(controls.get('one')!.preferences).toEqual(DEFAULT_VIEW_PREFERENCES);
    });

    it('resets only its settings key and leaves other projects and user content intact', async () => {
        const first = project(), second = project();
        setViewPreferences(first, { galaxyNodes: 500 }); setViewPreferences(second, { galaxyNodes: 1000 });
        window.localStorage.setItem('atlas-understanding:user-project', 'retained user content');
        await act(async () => root.render(<Probe name={first} />));
        await act(async () => resetViewPreferences(first));
        expect(controls.get(first)!.preferences).toEqual(DEFAULT_VIEW_PREFERENCES);
        expect(window.localStorage.getItem(viewPreferencesKey(first))).toBeNull();
        expect(readViewPreferences(second).galaxyNodes).toBe(1000);
        expect(window.localStorage.getItem('atlas-understanding:user-project')).toBe('retained user content');
    });

    it('preserves snapshot identity for repeated reads and no-op updates', () => {
        const name = project();
        store(name, { galaxyNodes: 500 });
        const before = readViewPreferences(name);
        expect(readViewPreferences(name)).toBe(before);
        setViewPreferences(name, { galaxyNodes: 500 });
        expect(readViewPreferences(name)).toBe(before);
    });
});
