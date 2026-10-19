// @vitest-environment jsdom
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import WelcomePanel from './WelcomePanel';

it('allows first use without starting or setting up browser AI', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.append(host);
    const root = createRoot(host);
    const onContinue = vi.fn(), onLocalAi = vi.fn();
    try {
        await act(async () => root.render(<WelcomePanel workspace="architecture"
            onWorkspace={vi.fn()} onContinue={onContinue} onLocalAi={onLocalAi} />));
        expect(onLocalAi).not.toHaveBeenCalled();
        expect(document.activeElement).toBe(host.querySelector('.cbm-primary'));
        await act(async () => host.querySelector<HTMLButtonElement>('.cbm-primary')!.click());
        expect(onContinue).toHaveBeenCalledOnce();
        expect(onLocalAi).not.toHaveBeenCalled();
    } finally { await act(async () => root.unmount()); host.remove(); }
});

it.each([false, true])('shows the Agents first-use choice only when opted in: %s', async experimentalAgents => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.append(host);
    const root = createRoot(host);
    const onWorkspace = vi.fn();
    try {
        await act(async () => root.render(<WelcomePanel workspace="architecture" experimentalAgents={experimentalAgents}
            onWorkspace={onWorkspace} onContinue={vi.fn()} onLocalAi={vi.fn()} />));
        const agents = [...host.querySelectorAll<HTMLButtonElement>('.cbm-welcome-choices button')]
            .find(button => button.querySelector('strong')?.textContent === 'Agents');
        expect(agents !== undefined).toBe(experimentalAgents);
        if (agents) {
            await act(async () => agents.click());
            expect(onWorkspace).toHaveBeenCalledWith('agents');
        }
    } finally { await act(async () => root.unmount()); host.remove(); }
});
