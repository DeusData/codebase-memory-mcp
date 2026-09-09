// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import AtlasChrome from './AtlasChrome';
import type { AtlasChromeProps } from './AtlasChrome';

let host: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div');
    document.body.appendChild(host);
    root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

const makeProps = (): AtlasChromeProps => ({
    version: 'test', chips: [], tabs: [], onSelectTab: vi.fn(), onCloseTab: vi.fn(),
    tree: { projectName: 'sample', rows: [], cursor: 0, activePath: '', note: '',
        onCursorChange: vi.fn(), onOpen: vi.fn(), onToggle: vi.fn(), onKeyDown: vi.fn() },
    breadcrumb: [], children: <input aria-label="Preserved reader" defaultValue="selected source" />,
    truncationNote: '', commandValue: '', onCommandChange: vi.fn(), commandHint: '', status: [],
});

it('offers the Galaxy workspace and reports the selected task', async () => {
    const onWorkspaceChange = vi.fn();
    await act(async () => root.render(<AtlasChrome {...makeProps()} {...{
        workspace: 'explore', onWorkspaceChange,
    }} />));
    const tabs = [...host.querySelectorAll<HTMLButtonElement>('[data-workspace-tab]')];
    expect(tabs.map(tab => tab.textContent?.trim())).toEqual(['Explore', 'Galaxy', 'Architecture', 'Agents', 'System']);
    expect(tabs[0].getAttribute('aria-selected')).toBe('true');
    await act(async () => tabs[1].click());
    expect(onWorkspaceChange).toHaveBeenCalledWith('galaxy');
});

it('keeps one galaxy mounted and visible alongside chat in its own workspace', async () => {
    const props = makeProps();
    const galaxy = <section className="atlas-galaxy"><canvas data-testid="preserved-galaxy" /></section>;
    const chat = <input aria-label="Graph chat draft" defaultValue="explain this entry" />;
    await act(async () => root.render(<AtlasChrome {...props} galaxy={galaxy} chatDock={chat} workspace="explore" />));
    const canvas = host.querySelector('canvas');
    const draft = host.querySelector('[aria-label="Graph chat draft"]');
    await act(async () => root.render(<AtlasChrome {...props} galaxy={galaxy} chatDock={chat} chatOpen workspace="galaxy" />));
    expect(host.querySelectorAll('canvas')).toHaveLength(1);
    expect(host.querySelector('canvas')).toBe(canvas);
    expect(canvas?.closest('[hidden]')).toBeNull();
    expect(host.querySelector('.atlas-alternate-workspace')?.hasAttribute('hidden')).toBe(true);
    expect(host.querySelector('[aria-label="Graph chat draft"]')).toBe(draft);
    expect(host.querySelector('[data-testid="atlas-split-chat"]')).not.toBeNull();
});

it('keeps the chat dock mounted across collapse and workspace navigation', async () => {
    const props = makeProps();
    const dock = <input aria-label="Chat draft" defaultValue="keep this question" />;
    await act(async () => root.render(<AtlasChrome {...props} chatOpen chatDock={dock} workspace="explore" />));
    const input = host.querySelector('input[aria-label="Chat draft"]');
    expect(host.querySelector('[data-testid="atlas-split-chat"]')).not.toBeNull();
    await act(async () => root.render(<AtlasChrome {...props} chatOpen={false} chatDock={dock} workspace="system" />));
    expect(host.querySelector('input[aria-label="Chat draft"]')).toBe(input);
    expect(host.querySelector('[data-testid="atlas-split-chat"]')).toBeNull();
});

it('moves the same live galaxy below Explore chat and back into Galaxy', async () => {
    const props = makeProps();
    const galaxy = <section className="atlas-galaxy"><canvas /></section>;
    const dock = <input aria-label="Local question" defaultValue="preserve me" />;
    await act(async () => root.render(<AtlasChrome {...props} galaxy={galaxy} chatDock={dock} workspace="galaxy" />));
    const canvas = host.querySelector('canvas');
    const draft = host.querySelector('[aria-label="Local question"]');
    await act(async () => root.render(<AtlasChrome {...props} galaxy={galaxy} chatDock={dock} chatOpen workspace="explore" />));
    expect(canvas?.closest('.atlas-chat-column')).not.toBeNull();
    expect(canvas?.closest('[hidden]')).toBeNull();
    expect(host.querySelector('canvas')).toBe(canvas);
    expect(host.querySelector('[data-testid="atlas-split-chat-graph"]')).not.toBeNull();
    await act(async () => root.render(<AtlasChrome {...props} galaxy={galaxy} chatDock={dock} chatOpen workspace="galaxy" />));
    expect(host.querySelector('canvas')).toBe(canvas);
    expect(canvas?.closest('.atlas-side')).not.toBeNull();
    expect(host.querySelector('[aria-label="Local question"]')).toBe(draft);
});

it('keeps one selected-code inspector and its open details when chat opens and closes', async () => {
    const props = makeProps();
    const inspector = <section data-testid="selected-code"><details><summary>Calls</summary>Source-linked calls</details></section>;
    const galaxy = <section className="atlas-galaxy"><canvas /></section>;
    const chatDock = <input aria-label="Selection question" defaultValue="explain the selection" />;
    const render = (chatOpen: boolean) => root.render(<AtlasChrome {...props} selectionInspector={inspector}
        galaxy={galaxy} chatDock={chatDock} chatOpen={chatOpen} workspace="explore" />);
    await act(async () => render(false));
    const panel = host.querySelector('[data-testid="selected-code"]');
    const details = panel!.querySelector('details')!;
    const canvas = host.querySelector('canvas');
    details.open = true;
    expect(panel?.closest('.atlas-side')).not.toBeNull();
    expect(panel?.closest('[hidden]')).toBeNull();

    await act(async () => render(true));
    const chatColumn = host.querySelector('.atlas-chat-column')!;
    const draft = chatColumn.querySelector('[aria-label="Selection question"]')!;
    expect(host.querySelectorAll('[data-testid="selected-code"]')).toHaveLength(1);
    expect(host.querySelector('[data-testid="selected-code"]')).toBe(panel);
    expect(details.open).toBe(true);
    expect(panel?.closest('.atlas-chat-column')).toBe(chatColumn);
    expect(panel?.closest('[hidden]')).toBeNull();
    expect(canvas?.closest('.atlas-chat-column')).toBe(chatColumn);
    expect(canvas?.closest('[hidden]')).toBeNull();
    expect(panel!.compareDocumentPosition(draft) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(draft.compareDocumentPosition(canvas!) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();

    await act(async () => render(false));
    expect(host.querySelector('[data-testid="selected-code"]')).toBe(panel);
    expect(details.open).toBe(true);
    expect(panel?.closest('.atlas-side')).not.toBeNull();
    expect(panel?.closest('[hidden]')).toBeNull();
    expect(host.querySelector('canvas')).toBe(canvas);
});

it('hides selected-code context outside Explore without losing its state', async () => {
    const props = makeProps();
    const inspector = <input aria-label="Pinned code context" defaultValue="keep context" />;
    await act(async () => root.render(<AtlasChrome {...props} selectionInspector={inspector} chatOpen workspace="explore" />));
    const panel = host.querySelector('[aria-label="Pinned code context"]');
    for (const workspace of ['galaxy', 'architecture', 'agents', 'system'] as const) {
        await act(async () => root.render(<AtlasChrome {...props} selectionInspector={inspector} chatOpen workspace={workspace} />));
        expect(host.querySelector('[aria-label="Pinned code context"]')).toBe(panel);
        expect(panel?.closest('[hidden]')).not.toBeNull();
        expect(host.querySelector('.atlas-chat-column')?.hasAttribute('hidden')).toBe(false);
    }
    await act(async () => root.render(<AtlasChrome {...props} selectionInspector={inspector} chatOpen workspace="explore" />));
    expect(host.querySelector('[aria-label="Pinned code context"]')).toBe(panel);
    expect(panel?.closest('[hidden]')).toBeNull();
});

it('replaces legacy model and twin controls when selected-code context is supplied', async () => {
    const props = { ...makeProps(), twin: <div>Legacy twin</div>, llm: <div>Legacy sidecar</div>,
        splitTwin: <div data-testid="legacy-twin-splitter" /> };
    await act(async () => root.render(<AtlasChrome {...props} selectionInspector={<div>Selected context</div>} />));
    expect(host.textContent).toContain('Selected context');
    expect(host.textContent).not.toContain('Legacy twin');
    expect(host.textContent).not.toContain('Legacy sidecar');
    expect(host.querySelector('[data-testid="legacy-twin-splitter"]')).toBeNull();

    await act(async () => root.render(<AtlasChrome {...props} />));
    expect(host.textContent).toContain('Legacy twin');
    expect(host.textContent).toContain('Legacy sidecar');
    expect(host.querySelector('[data-testid="legacy-twin-splitter"]')).not.toBeNull();
});

it('opens search on demand, retains its query, and returns keyboard focus on Escape', async () => {
    const props = { ...makeProps(), commandValue: 'index', onCommandKeyDown: vi.fn() };
    await act(async () => root.render(<AtlasChrome {...props} />));
    const trigger = host.querySelector<HTMLButtonElement>('[aria-label="Open search and commands"]');
    expect(trigger).not.toBeNull();
    const dialog = host.querySelector<HTMLDialogElement>('dialog');
    expect(dialog?.open).toBe(false);
    await act(async () => { trigger!.focus(); trigger!.click(); });
    expect(dialog?.open).toBe(true);
    const input = host.querySelector<HTMLInputElement>('[data-testid="atlas-command-input"]')!;
    expect(document.activeElement).toBe(input);
    expect(input.value).toBe('index');
    await act(async () => input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true, cancelable: true })));
    expect(dialog?.open).toBe(false);
    expect(document.activeElement).toBe(trigger);
    await act(async () => window.dispatchEvent(new KeyboardEvent('keydown', { key: 'k', ctrlKey: true, bubbles: true, cancelable: true })));
    expect(dialog?.open).toBe(true);
    expect(input.value).toBe('index');
});

it('opens the command palette for existing keyboard and graph-action requests', async () => {
    await act(async () => root.render(<AtlasChrome {...makeProps()} />));
    await act(async () => window.dispatchEvent(new Event('cbm:open-command-search')));
    expect(host.querySelector<HTMLDialogElement>('dialog')?.open).toBe(true);
});

it('resizes local chat by keyboard and exposes daemon navigation', async () => {
    const onOpenSystem = vi.fn();
    await act(async () => root.render(<AtlasChrome {...makeProps()} chatOpen chatDock={<div>Local chat</div>}
        onOpenSystem={onOpenSystem} daemonState="disconnected" />));
    const separator = host.querySelector('[data-testid="atlas-split-chat"]')!;
    expect(separator.getAttribute('aria-valuenow')).toBe('420');
    await act(async () => separator.dispatchEvent(new KeyboardEvent('keydown', { key: 'ArrowLeft', bubbles: true })));
    expect(Number(separator.getAttribute('aria-valuenow'))).toBeGreaterThan(420);
    await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Open System: daemon disconnected"]')!.click());
    expect(onOpenSystem).toHaveBeenCalledOnce();
});

it('keeps the reader mounted while showing a different workspace', async () => {
    const props = makeProps();
    const change = vi.fn();
    await act(async () => root.render(<AtlasChrome {...props} {...{
        workspace: 'explore', onWorkspaceChange: change,
    }} />));
    const reader = host.querySelector('input[aria-label="Preserved reader"]');
    await act(async () => root.render(<AtlasChrome {...props} {...{
        workspace: 'architecture', onWorkspaceChange: change,
        workspacePanel: <div>Architecture evidence</div>,
    }} />));
    expect(host.textContent).toContain('Architecture evidence');
    expect(host.querySelector('input[aria-label="Preserved reader"]')).toBe(reader);
    expect(host.querySelector('[data-testid="atlas-exploration-workspace"]')?.hasAttribute('hidden')).toBe(true);
});

it('changes explanation depth separately from the workspace', async () => {
    const onGuidanceChange = vi.fn();
    await act(async () => root.render(<AtlasChrome {...makeProps()} {...{
        workspace: 'explore', onWorkspaceChange: vi.fn(), guidance: 'brief', onGuidanceChange,
    }} />));
    const choice = host.querySelector<HTMLSelectElement>('[aria-label="Explanation depth"]');
    expect(choice).not.toBeNull();
    await act(async () => {
        choice!.value = 'explained';
        choice!.dispatchEvent(new Event('change', { bubbles: true }));
    });
    expect(onGuidanceChange).toHaveBeenCalledWith('explained');
});

it('keeps pending results visible until activation succeeds and refocuses an open search', async () => {
    const props = { ...makeProps(), onCommandKeyDown: (event: {preventDefault: () => void}) => event.preventDefault() };
    await act(async () => root.render(<AtlasChrome {...props} />));
    await act(async () => window.dispatchEvent(new Event('cbm:open-command-search')));
    const input = host.querySelector<HTMLInputElement>('[data-testid="atlas-command-input"]')!;
    const dialog = host.querySelector<HTMLDialogElement>('dialog')!;
    await act(async () => input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true, cancelable: true })));
    expect(dialog.open).toBe(true);
    await act(async () => host.querySelector<HTMLButtonElement>('[aria-label="Close search"]')!.focus());
    await act(async () => window.dispatchEvent(new Event('cbm:open-command-search')));
    expect(document.activeElement).toBe(input);
    await act(async () => window.dispatchEvent(new Event('cbm:close-command-search')));
    expect(dialog.open).toBe(false);
});
