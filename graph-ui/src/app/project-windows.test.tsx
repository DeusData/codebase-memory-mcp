// @vitest-environment jsdom
import { act, useEffect } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ProjectWindows, useParamInAddress, type ProjectWindow } from './project-windows';

/*
 * K24: a project switch stays in the page. Each project gets a fresh window (fresh state in
 * every panel, as after a reload), the address changes with the history API, Back and Forward
 * switch the same way, and requests of the old window are cancelled.
 */

let container: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    window.history.replaceState(null, '', '/?project=django-demo&workspace=galaxy');
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); });

interface Carry { chatOpen: boolean }
const mounted: string[] = [];
const unmounted: string[] = [];
let current: ProjectWindow<Carry> | undefined;
function Window({ window: shown }: { window: ProjectWindow<Carry> }) {
    current = shown;
    useEffect(() => { mounted.push(shown.project); return () => { unmounted.push(shown.project); }; }, []);
    useEffect(() => { shown.report({ chatOpen: shown.project === 'django-demo' }); });
    return <p data-project={shown.project}>{shown.project}:{String(shown.carried?.chatOpen)}</p>;
}
const shownProject = () => container.querySelector('p')?.getAttribute('data-project');

describe('an in-page project switch (K24)', () => {
    beforeEach(() => { mounted.length = 0; unmounted.length = 0; current = undefined; });

    it('opens another project without reloading: new address, a fresh window, the old one cancelled', async () => {
        const push = vi.spyOn(window.history, 'pushState');
        await act(async () => root.render(<ProjectWindows<Carry>>{shown => <Window key={shown.key} window={shown} />}</ProjectWindows>));
        expect(shownProject()).toBe('django-demo');
        const first = current!;
        await act(async () => first.open('cbm'));
        expect(push).toHaveBeenCalledOnce();
        expect(window.location.search).toBe('?project=cbm');
        expect(shownProject()).toBe('cbm');
        expect(mounted).toEqual(['django-demo', 'cbm']);
        expect(unmounted).toEqual(['django-demo']);
        // The old window's requests are cancelled; the new window's are not.
        expect(first.signal.aborted).toBe(true);
        expect(current!.signal.aborted).toBe(false);
        // What the old window reported travels to the new one.
        expect(container.textContent).toBe('cbm:true');
    });

    it('stays put for the project already shown', async () => {
        const push = vi.spyOn(window.history, 'pushState');
        await act(async () => root.render(<ProjectWindows<Carry>>{shown => <Window key={shown.key} window={shown} />}</ProjectWindows>));
        await act(async () => current!.open('django-demo'));
        expect(push).not.toHaveBeenCalled();
        expect(mounted).toEqual(['django-demo']);
    });

    it('switches back and forward with the browser history in the page', async () => {
        await act(async () => root.render(<ProjectWindows<Carry>>{shown => <Window key={shown.key} window={shown} />}</ProjectWindows>));
        await act(async () => current!.open('cbm'));
        await act(async () => {
            window.history.replaceState(null, '', '/?project=django-demo&workspace=galaxy');
            window.dispatchEvent(new PopStateEvent('popstate'));
        });
        expect(shownProject()).toBe('django-demo');
        expect(mounted).toEqual(['django-demo', 'cbm', 'django-demo']);
        // A popstate that keeps the project (another query parameter) keeps the window.
        await act(async () => {
            window.history.replaceState(null, '', '/?project=django-demo&workspace=explore');
            window.dispatchEvent(new PopStateEvent('popstate'));
        });
        expect(mounted).toHaveLength(3);
    });

    /* Hand test 2026-10-04 (A5): the switcher turned "?project=django-demo&workspace=galaxy" into "?project=cbm". */
    it('keeps the page-wide parameters it is given, the workspace among them, and nothing project specific', async () => {
        window.history.replaceState(null, '', '/?project=django-demo&workspace=galaxy&codeatlasClosureDepth=4&file=django%2Fshortcuts.py');
        await act(async () => root.render(<ProjectWindows<Carry> keepParams={['workspace', 'codeatlasClosureDepth', 'codeatlasClosureCap']}>
            {shown => <Window key={shown.key} window={shown} />}</ProjectWindows>));
        await act(async () => current!.open('atlas sample'));
        expect(window.location.search).toBe('?project=atlas%20sample&workspace=galaxy&codeatlasClosureDepth=4');
        expect(shownProject()).toBe('atlas sample');
        // Back and Forward of the browser still switch between both projects, each with its own address.
        const popped = () => new Promise<void>(resolve => window.addEventListener('popstate', () => resolve(), { once: true }));
        await act(async () => { const done = popped(); window.history.back(); await done; });
        expect(window.location.search).toBe('?project=django-demo&workspace=galaxy&codeatlasClosureDepth=4&file=django%2Fshortcuts.py');
        expect(shownProject()).toBe('django-demo');
        await act(async () => { const done = popped(); window.history.forward(); await done; });
        expect(shownProject()).toBe('atlas sample');
    });

    it('writes the workspace in view into the address without a new history entry', async () => {
        window.history.replaceState(null, '', '/?project=django-demo');
        const length = window.history.length;
        function Workspace({ name }: { name: string }) { useParamInAddress('workspace', name); return null; }
        await act(async () => root.render(<Workspace name="architecture" />));
        expect(window.location.search).toBe('?project=django-demo&workspace=architecture');
        await act(async () => root.render(<Workspace name="galaxy" />));
        expect(window.location.search).toBe('?project=django-demo&workspace=galaxy');
        expect(window.history.length).toBe(length);
    });

    it('commits the switch inside the frame it is given, so the old window is gone and the new one there when it ends', async () => {
        const seen: string[] = [];
        const around = (commit: () => void) => { seen.push(`before:${mounted.join(',')}`); commit(); seen.push(`after:${mounted.join(',')}|${unmounted.join(',')}`); };
        await act(async () => root.render(<ProjectWindows<Carry> around={around}>{shown => <Window key={shown.key} window={shown} />}</ProjectWindows>));
        await act(async () => current!.open('cbm'));
        expect(seen).toEqual(['before:django-demo', 'after:django-demo,cbm|django-demo']);
    });
});
