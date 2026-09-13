// @vitest-environment jsdom
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import ActivityPanel from './ActivityPanel';
import { emptyAgentsState, withEvent } from './agent-store';
import type { AgentEvent } from './agent-event';

it('keeps unmapped evidence visible and only opens recorded graph locations', async () => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    const host = document.createElement('div'); document.body.append(host);
    const root = createRoot(host);
    const event: AgentEvent = { ts: 1000, agent: 'Ada', run: 'one', seq: 1, phase: 'end',
        tool: 'Read', path: 'missing.ts', detail: 'inspect', source: 'bridge', replay: false };
    const onOpenNode = vi.fn();
    const props = { state: withEvent(emptyAgentsState(), event), on: false, port: 4142,
        status: { state: 'off' as const, origin: '', requests: 0, drops: 0, hello: undefined, error: '' },
        onToggle: vi.fn(), onOpenNode };
    try {
        await act(async () => root.render(<ActivityPanel {...props} graph={{ nodes: [], edges: [], total_nodes: 100 }} />));
        await act(async () => host.querySelector<HTMLButtonElement>('.cbm-activity-row')!.click());
        expect(host.querySelector('.cbm-activity-inspector')?.textContent).toContain('Unmapped');
        expect(host.querySelector('.cbm-activity-inspector')?.textContent).toContain('loaded graph');
        expect(host.querySelector('.cbm-activity-inspector')?.textContent).not.toContain('the index has no node');
        expect(host.querySelector('.cbm-activity-inspector button')).toBeNull();
        expect(host.textContent).toContain('Its result is not reported');
        const node = { id: 1, x: 0, y: 0, z: 0, size: 1, color: 'green', label: 'Module', name: 'main', file_path: 'missing.ts' };
        await act(async () => root.render(<ActivityPanel {...props} graph={{ nodes: [node], edges: [], total_nodes: 1 }} />));
        await act(async () => host.querySelector<HTMLButtonElement>('.cbm-activity-inspector button')!.click());
        expect(onOpenNode).toHaveBeenCalledWith(node);
    } finally { await act(async () => root.unmount()); host.remove(); }
});
