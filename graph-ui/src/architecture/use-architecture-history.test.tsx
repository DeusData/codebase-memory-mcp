// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { initialArchitecturePlace, type ArchitectureHistoryEntry } from './architecture-history';
import { useArchitectureHistory, type ArchitectureHistory, type ArchitectureHistoryKeys } from './use-architecture-history';

let host: HTMLDivElement, root: Root;
let current: ArchitectureHistory;
let initial: ArchitectureHistoryEntry;
const restored = vi.fn();
function Probe({ active = true, escapeTaken = false }: Partial<ArchitectureHistoryKeys>) {
    current = useArchitectureHistory(() => initial, { active, escapeTaken, onRestore: restored });
    return <input aria-label="Routes filter" readOnly value={current.filter} />;
}
const render = async (keys: Partial<ArchitectureHistoryKeys> = {}) => {
    await act(async () => { root.render(<Probe {...keys} />); });
};
const change = async (update: Parameters<ArchitectureHistory['navigate']>[0], context = false) => {
    await act(async () => { current.navigate(update, context); });
};
const go = async (step: -1 | 1) => { await act(async () => { current.go(step); }); };
const type = async (filter: string) => { await act(async () => { current.type(filter); }); };
const area = (areaPath: string) => change(place => ({ ...place, spatial: { ...place.spatial, areaPath, filePath: undefined } }));
const file = (filePath: string) => change(place => ({ ...place, spatial: { ...place.spatial, filePath } }));
const keypress = async (key: string, target: EventTarget = window, modifiers: KeyboardEventInit = {}) => {
    const event = new KeyboardEvent('keydown', { key, altKey: true, bubbles: true, cancelable: true, ...modifiers });
    await act(async () => { target.dispatchEvent(event); });
    return event;
};

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    initial = initialArchitecturePlace('overview');
    restored.mockReset();
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => { root.unmount(); }); host.remove(); });

describe('Architecture exploration history', () => {
    it('updates tabs, perspective and scene settings in place while keeping Forward available', async () => {
        await render();
        await area('src');
        await file('src/main.ts');
        await go(-1);
        const forward = current.forward;

        await change(place => ({ ...place, view: 'routes' }), true);
        await change(place => ({ ...place, routes: 'endpoints' }), true);
        await change(place => ({ ...place, spatial: { ...place.spatial, planar: true } }));
        await change(place => ({ ...place, system: { ...place.system, expanded: ['g:src'], structurePlanar: true, behaviorPlanar: true } }));

        expect(current.history.entries).toHaveLength(3);
        expect(current.history.index).toBe(1);
        expect(current.forward).toBe(forward);
        expect(current.history.entries[1]).toBe(current.place);
        const context = current.place;
        await go(1);
        expect(current.place).toBe(forward);
        expect(current.place.view).toBe('overview');
        expect(current.place.spatial.planar).toBe(false);
        await go(-1);
        expect(current.place).toBe(context);
        expect(current.place).toMatchObject({ view: 'routes', routes: 'endpoints', spatial: { planar: true }, system: { expanded: ['g:src'], structurePlanar: true, behaviorPlanar: true } });
        expect(current.history.entries).toHaveLength(3);
    });

    it('saves System structure expansion and Plan as context without inserting stops', async () => {
        initial = initialArchitecturePlace('structure');
        await render();
        await change(place => ({ ...place, system: { ...place.system, focus: { id: 'g:src', label: 'src' } } }));
        await change(place => ({ ...place, system: { ...place.system, expanded: ['g:src'], structurePlanar: true } }));
        expect(current.history.entries).toHaveLength(2);
        await change(place => ({ ...place, system: { ...place.system, focus: { id: 'g:api', label: 'api' }, expanded: [], structurePlanar: false } }));
        await go(-1);
        expect(current.place.system).toMatchObject({ focus: { id: 'g:src' }, expanded: ['g:src'], structurePlanar: true });
        expect(current.history.entries).toHaveLength(3);
    });

    it('drops the forward branch only when opening another exploration location', async () => {
        await render();
        await area('src');
        await file('src/old.ts');
        await go(-1);
        await change(place => ({ ...place, spatial: { ...place.spatial, planar: true } }));
        expect(current.forward?.spatial.filePath).toBe('src/old.ts');
        await file('src/new.ts');
        expect(current.history.entries.map(entry => entry.spatial.filePath)).toEqual([undefined, undefined, 'src/new.ts']);
        expect(current.forward).toBeUndefined();
        await go(-1);
        expect(current.place.spatial).toMatchObject({ areaPath: 'src', planar: true });
        await go(1);
        expect(current.place.spatial.filePath).toBe('src/new.ts');
        expect(current.history.entries).toHaveLength(3);
    });

    it('stores every filter edit immediately without a history stop', async () => {
        initial = initialArchitecturePlace('routes');
        await render();
        for (const filter of ['/', '/a', '/api', '']) {
            await type(filter);
            expect(current.filter).toBe(filter);
            expect(current.place.filter).toBe(filter);
            expect(current.history.entries).toHaveLength(1);
            expect(current.history.entries[0].filter).toBe(filter);
            expect(current.back).toBeUndefined();
        }
    });

    it('restores typed filters through immediate Back and Forward while explicit route groups create stops', async () => {
        initial = initialArchitecturePlace('routes');
        await render();
        await type('/typed-at-root');
        await change(place => ({ ...place, routes: 'endpoints', filter: '/users' }));
        await type('/users/active');
        await change(place => ({ ...place, filter: '/orders' }));
        expect(current.history.entries.map(entry => entry.filter)).toEqual(['/typed-at-root', '/users/active', '/orders']);

        await type('/orders/recent');
        await go(-1);
        expect(current.filter).toBe('/users/active');
        await type('/users/new');
        expect(current.forward?.filter).toBe('/orders/recent');
        await go(1);
        expect(current.filter).toBe('/orders/recent');
        await go(-1);
        expect(current.filter).toBe('/users/new');
        expect(current.history.entries.map(entry => entry.filter)).toEqual(['/typed-at-root', '/users/new', '/orders/recent']);
        expect(current.history.index).toBe(1);
    });

    it('replaces automatic starts and reindex resets, and restores them without pushing again', async () => {
        initial = initialArchitecturePlace('behavior');
        await render();
        await change(place => ({ ...place, system: { ...place.system, behavior: { project: 'sample', id: 1, name: 'main' } } }), true);
        expect(current.history.entries).toHaveLength(1);
        await change(place => ({ ...place, system: { ...place.system, behavior: { project: 'sample', id: 2, name: 'save' } } }));
        await go(-1);
        await change(place => ({ ...place, system: { ...place.system, behavior: { project: 'sample', id: 3, name: 'reindexed main', generation: 'next' } } }), true);
        expect(current.history.entries).toHaveLength(2);
        expect(current.forward?.system.behavior?.id).toBe(2);
        await go(1);
        expect(current.place.system.behavior?.id).toBe(2);
        await go(-1);
        expect(current.place.system.behavior?.id).toBe(3);
        expect(current.history.entries).toHaveLength(2);
        expect(restored).toHaveBeenCalledTimes(3);
        expect(restored.mock.calls.at(-1)?.map((entry: ArchitectureHistoryEntry) => entry.system.behavior?.id)).toEqual([2, 3]);
    });

    it('records another Behavior path while saving steps, pages and Plan with that path', async () => {
        initial = { ...initialArchitecturePlace('behavior'), system: { expanded: [], behavior: { project: 'sample', id: 1, targetId: 2 } } };
        await render();
        await change(place => ({ ...place, system: { ...place.system, behaviorPlanar: true, behavior: { ...place.system.behavior!, position: { path: 0, step: 2, page: 1 } } } }));
        expect(current.history.entries).toHaveLength(1);
        await change(place => ({ ...place, system: { ...place.system, behavior: { ...place.system.behavior!, position: { path: 1 } } } }));
        expect(current.history.entries).toHaveLength(2);
        await go(-1);
        expect(current.place.system.behavior?.position).toEqual({ path: 0, step: 2, page: 1 });
        expect(current.place.system.behaviorPlanar).toBe(true);
    });

    it('keeps Alt+Left and Alt+Right available for meaningful stops and leaves typing alone', async () => {
        await render();
        await area('src');
        await file('src/main.ts');
        expect((await keypress('ArrowLeft')).defaultPrevented).toBe(true);
        expect(current.place.spatial.filePath).toBeUndefined();
        expect(current.history.index).toBe(1);
        expect((await keypress('ArrowRight', host.querySelector('input')!)).defaultPrevented).toBe(false);
        expect(current.history.index).toBe(1);
        expect((await keypress('ArrowRight', window, { ctrlKey: true })).defaultPrevented).toBe(false);
        expect((await keypress('ArrowRight')).defaultPrevented).toBe(true);
        expect(current.place.spatial.filePath).toBe('src/main.ts');
        expect(current.history.entries).toHaveLength(3);
        expect((await keypress('ArrowRight')).defaultPrevented).toBe(false);

        await render({ active: false });
        expect((await keypress('ArrowLeft')).defaultPrevented).toBe(false);
        await render({ escapeTaken: true });
        expect((await keypress('ArrowLeft')).defaultPrevented).toBe(false);
        expect(current.history.index).toBe(2);
    });
});
