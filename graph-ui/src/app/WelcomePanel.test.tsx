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
        await act(async () => root.render(<WelcomePanel workspace="architecture" guidance="brief"
            onWorkspace={vi.fn()} onGuidance={vi.fn()} onContinue={onContinue} onLocalAi={onLocalAi} />));
        expect(onLocalAi).not.toHaveBeenCalled();
        expect(document.activeElement).toBe(host.querySelector('.cbm-primary'));
        await act(async () => host.querySelector<HTMLButtonElement>('.cbm-primary')!.click());
        expect(onContinue).toHaveBeenCalledOnce();
        expect(onLocalAi).not.toHaveBeenCalled();
    } finally { await act(async () => root.unmount()); host.remove(); }
});
