// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import BrowserChatDock from './BrowserChatDock';
import { browserChatHistoryCache, type BrowserChatHistorySnapshot } from './chat-history-cache';

let host: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.appendChild(host); root = createRoot(host);
    vi.spyOn(browserChatHistoryCache, 'save').mockResolvedValue({ saved: true, trimmed: false, droppedTurns: 0, evictedProjects: 0 });
    vi.spyOn(browserChatHistoryCache, 'delete').mockResolvedValue();
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.restoreAllMocks(); });

function history(answer: string): BrowserChatHistorySnapshot {
    return { turns: [{ id: 'local-turn-1', prompt: 'What does it do?', modelId: 'cached-model',
        request: [{ role: 'user', content: 'What does it do?' }], answer, status: 'complete',
        readerContext: { project: 'sample', path: 'sum.ts', status: 'ready', source: {
            id: 'source', project: 'sample', path: 'sum.ts', sourceVersion: 'v1', text: 'return a + b;',
            kind: 'selection', startLine: 1, endLine: 1, startColumn: 1, endColumn: 14,
        } } }], draft: 'Follow-up draft', savedAt: Date.now(), expiresAt: Date.now() + 86400000, trimmed: false };
}
const base = { open: true, proactive: false, onClose: () => {}, onAttachmentConsumed: () => {} };
const click = async (name: string) => {
    const button = [...document.querySelectorAll('button')].find(item => item.textContent === name || item.getAttribute('aria-label') === name);
    expect(button).toBeDefined(); await act(async () => button!.click());
};

it('restores conversation, draft and source without enabling a model or resaving on read', async () => {
    vi.spyOn(browserChatHistoryCache, 'load').mockResolvedValue(history('Cached explanation.'));
    const createRuntime = vi.fn();
    await act(async () => root.render(<BrowserChatDock {...base} historyKey="project-a" createRuntime={createRuntime} />));
    expect(host.textContent).toContain('Cached explanation.');
    expect(host.querySelector('textarea')?.value).toBe('Follow-up draft');
    expect(host.querySelector('.cbm-chat-source-content pre')?.textContent).toBe('return a + b;');
    expect(createRuntime).not.toHaveBeenCalled();
    expect(browserChatHistoryCache.save).not.toHaveBeenCalled();
    expect(host.textContent).not.toContain('cached-model');
});

it('clears this project through agent configuration and does not restore it on folding', async () => {
    vi.spyOn(browserChatHistoryCache, 'load').mockResolvedValue(history('Remove this history.'));
    await act(async () => root.render(<BrowserChatDock {...base} historyKey="project-a" settingsRequest={0} />));
    await act(async () => root.render(<BrowserChatDock {...base} historyKey="project-a" settingsRequest={1} />));
    expect(document.querySelector('dialog')?.textContent).toContain('24 hours');
    vi.spyOn(window, 'confirm').mockReturnValue(true);
    await click('Clear history');
    expect(browserChatHistoryCache.delete).toHaveBeenCalledWith('project-a');
    expect(host.textContent).not.toContain('Remove this history.');
    await click('Close agent configuration');
    await act(async () => root.render(<BrowserChatDock {...base} historyKey="project-a" open={false} settingsRequest={1} />));
    await act(async () => root.render(<BrowserChatDock {...base} historyKey="project-a" settingsRequest={1} />));
    expect(host.textContent).not.toContain('Remove this history.');
});

it('does not display the previous project while another history is loading', async () => {
    let finish!: (value: BrowserChatHistorySnapshot) => void;
    const pending = new Promise<BrowserChatHistorySnapshot>(resolve => { finish = resolve; });
    vi.spyOn(browserChatHistoryCache, 'load').mockImplementation(key => key === 'project-a' ? Promise.resolve(history('Project A answer')) : pending);
    await act(async () => root.render(<BrowserChatDock {...base} historyKey="project-a" />));
    expect(host.textContent).toContain('Project A answer');
    await act(async () => root.render(<BrowserChatDock {...base} historyKey="project-b" />));
    expect(host.textContent).not.toContain('Project A answer');
    await act(async () => finish(history('Project B answer')));
    expect(host.textContent).toContain('Project B answer');
    expect(host.textContent).not.toContain('Project A answer');
});

it('keeps storage diagnostics in agent settings instead of the conversation', async () => {
    vi.spyOn(browserChatHistoryCache, 'load').mockRejectedValue(new Error('Storage blocked'));
    await act(async () => root.render(<BrowserChatDock {...base} historyKey="project-a" />));
    expect(host.textContent).not.toContain('Storage blocked');
    await act(async () => root.render(<BrowserChatDock {...base} historyKey="project-a" settingsRequest={1} />));
    expect(document.querySelector('dialog')?.textContent).toMatch(/history|storage/i);
    expect(host.textContent).not.toContain('Storage blocked');
});
