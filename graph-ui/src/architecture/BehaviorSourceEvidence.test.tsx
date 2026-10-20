// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import BehaviorSourceEvidence from './BehaviorSourceEvidence';
import { callToolJson } from '../provider/rpc-transport';
import type { SystemSymbol } from './system-architecture-source';
vi.mock('../provider/rpc-transport', () => ({ callToolJson: vi.fn() }));
const symbol: SystemSymbol = { id: 1, name: 'start', qualified_name: 'sample.start', label: 'Function', component_id: 'api', file_path: 'src/api.ts', start_line: 10 };
const snippet = { qualified_name: 'sample.start', file_path: '/repo/src/api.ts', source_mode: 'full', source: 'start()\nnext()\n', start_line: 10, end_line: 11 };
let container: HTMLDivElement, root: Root;
beforeEach(() => { (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true; vi.clearAllMocks(); container = document.createElement('div'); document.body.append(container); root = createRoot(container); });
afterEach(async () => { await act(async () => root.unmount()); container.remove(); });
async function render(project = 'sample', active = true) { await act(async () => root.render(<BehaviorSourceEvidence project={project} generation="g1" symbol={symbol} active={active} onNavigate={vi.fn()} />)); }
describe('behavior source evidence', () => {
    it('requests bounded JSON source and highlights only a verified line', async () => {
        vi.mocked(callToolJson).mockResolvedValue(snippet); await render();
        expect(callToolJson).toHaveBeenCalledWith('get_code_snippet', expect.objectContaining({ format: 'json', source_mode: 'full', max_lines: 32 }), expect.objectContaining({ signal: expect.any(AbortSignal) }));
        expect(container.querySelector('[data-selected="true"]')?.textContent).toContain('start()');
    });
    it('rejects source identity/range mismatches instead of attaching it to the selected call', async () => {
        vi.mocked(callToolJson).mockResolvedValue({ ...snippet, qualified_name: 'other.start' }); await render();
        expect(container.querySelector('pre')).toBeNull(); expect(container.textContent).toContain('Source identity changed');
        vi.mocked(callToolJson).mockResolvedValue({ ...snippet, end_line: 15 }); await render('another');
        expect(container.querySelector('pre')).toBeNull(); expect(container.textContent).toContain('Source line range could not be verified');
    });
    it('aborts and ignores a source reply after the project changes', async () => {
        let finish!: (value: unknown) => void;
        vi.mocked(callToolJson).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; })).mockResolvedValue({ ...snippet, source: 'new()\nnext()' });
        await render(); const signal = vi.mocked(callToolJson).mock.calls[0][2]?.signal;
        await render('other'); expect(signal?.aborted).toBe(true);
        await act(async () => finish(snippet)); expect(container.textContent).toContain('new()'); expect(container.textContent).not.toContain('start()');
    });
    it('does not request source for an inactive workspace', async () => { await render('sample', false); expect(callToolJson).not.toHaveBeenCalled(); });
});
