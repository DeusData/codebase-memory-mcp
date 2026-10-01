// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import BrowserChatDock, { type BrowserChatAttachment, type BrowserChatDockProps, type BrowserChatReaderContext } from './BrowserChatDock';
import type { BrowserAiProgress, BrowserChatMessage } from './browser-ai-runtime';
import { BROWSER_MODELS } from './model-policy';

let container: HTMLDivElement;
let root: Root;
let renderedProps: BrowserChatDockProps;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); });

const selection: BrowserChatAttachment = { id: 'selection-1', text: '\t a +\r\n b  ', path: 'src/sum.ts', project: 'sample', startLine: 3, startColumn: 7, endLine: 4, endColumn: 5, sourceVersion: 'sha256:123' };
const reader = (text: string, kind: 'file' | 'selection' = 'file', path = 'src/sum.ts'): BrowserChatReaderContext => ({ project: 'sample', path, status: 'ready', source: { ...selection, text, path, kind, id: `reader-${path}-${kind}` } });
function fixture() {
    const runtime = {
        prepare: vi.fn(async (_progress: (value: BrowserAiProgress) => void) => {}),
        explain: vi.fn(async () => 'Legacy'),
        countTokens: vi.fn(async (_messages: readonly BrowserChatMessage[]) => 100),
        chat: vi.fn(async (_messages: readonly BrowserChatMessage[], _onToken: (chunk: string) => void) => 'Adds the two values.'),
        stop: vi.fn(), dispose: vi.fn(),
    };
    const props = { proactive: false, open: true, onClose: vi.fn(), onAttachmentConsumed: vi.fn(), onAttachmentRemoved: vi.fn(), createRuntime: vi.fn(() => runtime), removeCache: vi.fn(async () => {}) };
    return { runtime, props };
}
async function render(props: BrowserChatDockProps): Promise<void> { renderedProps = props; await act(async () => root.render(<BrowserChatDock {...props} />)); }
function button(label: string): HTMLButtonElement {
    const target = [...document.body.querySelectorAll('button')].find(item => item.textContent === label || item.getAttribute('aria-label') === label);
    expect(target, `button ${label}`).toBeDefined(); return target!;
}
async function click(label: string): Promise<void> {
    if (['Download & load', 'Load model', 'Reload model'].includes(label) && !document.querySelector('#cbm-chat-model')) await models();
    await act(async () => button(label).click());
}
async function type(value: string): Promise<void> {
    const input = container.querySelector('textarea')!;
    await act(async () => {
        Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')!.set!.call(input, value);
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
}
async function models(): Promise<void> {
    if (!document.querySelector('#cbm-chat-model')) await render({ ...renderedProps, settingsRequest: (renderedProps.settingsRequest ?? 0) + 1 });
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const attachmentData = (message: BrowserChatMessage) => JSON.parse(message.content.slice(message.content.indexOf('\n{') + 1).split('\n')[0]);

describe('persistent local browser chat', () => {
    it('keeps configuration and model names out of chat until agent configuration is requested', async () => {
        const { props } = fixture(); await render({ ...props, attachment: selection });
        expect(props.createRuntime).not.toHaveBeenCalled();
        expect(container.querySelector('select')).toBeNull();
        expect(container.textContent).not.toContain(BROWSER_MODELS[0].displayName);
        expect(container.textContent).not.toContain('Explain selections automatically');
        expect(container.querySelector('pre')?.textContent).toBe(selection.text);
        await click('Enable agent');
        const dialog = document.querySelector('dialog')!;
        expect(dialog.textContent).toContain('Agent configuration');
        expect(dialog.querySelectorAll('option')).toHaveLength(BROWSER_MODELS.length);
        expect(dialog.textContent).toContain('No model downloads automatically');
        expect(container.querySelector('select')).toBeNull();
        expect(button('Download & load').disabled).toBe(false);
        expect(props.createRuntime).not.toHaveBeenCalled();
        await click('Close agent configuration');
        expect(document.querySelector('dialog')).toBeNull();
    });

    it('opens configuration centrally while chat stays collapsed without consuming the selection', async () => {
        const { props } = fixture(); const onOpen = vi.fn();
        await render({ ...props, open: false, showCollapsed: true, onOpen, attachment: selection });
        expect(container.querySelector('form')).toBeNull();
        await click('Open chat'); expect(onOpen).toHaveBeenCalledOnce();
        await render({ ...props, open: false, showCollapsed: true, onOpen, attachment: selection, settingsRequest: 1 });
        expect(document.querySelector('dialog[open]')).not.toBeNull();
        expect(container.querySelector('select')).toBeNull();
        expect(props.createRuntime).not.toHaveBeenCalled();
        expect(props.onAttachmentConsumed).not.toHaveBeenCalled();
    });

    it('preserves the conversation and draft across agent configuration and reports the model only to the toolbar', async () => {
        const { props } = fixture(); const onAgentModelChange = vi.fn();
        await render({ ...props, onAgentModelChange });
        await click('Download & load'); await type('Explain'); await click('Send ↑'); await type('Follow up');
        await models(); await click('Close agent configuration');
        expect(container.querySelector('textarea')?.value).toBe('Follow up');
        expect(container.textContent).toContain('Adds the two values.');
        expect(container.textContent).not.toContain(BROWSER_MODELS[0].displayName);
        expect(onAgentModelChange).toHaveBeenLastCalledWith(BROWSER_MODELS[0].displayName);
    });

    it('keeps an active answer and draft when only the collapsed header is visible', async () => {
        const { props, runtime } = fixture(); const onOpen = vi.fn(); const answer = deferred<string>();
        runtime.chat.mockReturnValueOnce(answer.promise);
        await render({ ...props, showCollapsed: true, onOpen });
        await click('Download & load'); await type('Explain'); await click('Send ↑'); await type('Next question');
        await render({ ...props, open: false, showCollapsed: true, onOpen });
        expect(container.querySelector('[role="log"]')).toBeNull();
        await act(async () => answer.resolve('Finished while folded.'));
        await click('Open chat'); expect(onOpen).toHaveBeenCalledOnce();
        await render({ ...props, showCollapsed: true, onOpen });
        expect(container.textContent).toContain('Finished while folded.');
        expect(container.querySelector('textarea')?.value).toBe('Next question');
        expect(runtime.dispose).not.toHaveBeenCalled();
        expect(props.createRuntime).toHaveBeenCalledOnce();
    });

    it('retains draft, history and loaded model when the dock is collapsed', async () => {
        const { props, runtime } = fixture(); await render(props); await click('Download & load');
        await type('Explain'); await click('Send ↑'); await type('Follow-up draft');
        await click('Collapse local chat'); expect(props.onClose).toHaveBeenCalledOnce();
        await render({ ...props, open: false });
        expect(container.querySelector('aside')?.hidden).toBe(true);
        await render(props);
        expect(container.querySelector('textarea')?.value).toBe('Follow-up draft');
        expect(container.textContent).toContain('Adds the two values.');
        expect(runtime.dispose).not.toHaveBeenCalled();
        expect(props.createRuntime).toHaveBeenCalledOnce();
    });

    it('sends exact selection text once and keeps its immutable snapshot for the sent turn', async () => {
        const { props, runtime } = fixture();
        const original = { ...selection };
        await render({ ...props, attachment: original }); await click('Download & load'); await type('Explain this part'); await click('Send ↑');
        const sent = runtime.chat.mock.calls[0][0].at(-1)!;
        expect(attachmentData(sent)).toMatchObject({ path: selection.path, sourceVersion: selection.sourceVersion });
        expect(sent.content.includes(selection.text)).toBe(true);
        expect(props.onAttachmentConsumed).toHaveBeenCalledExactlyOnceWith(selection.id);
        original.text = 'Different code';
        await render(props); await type('Why?'); await click('Send ↑');
        const second = runtime.chat.mock.calls[1][0];
        expect(second.at(-1)?.content).toBe('Why?');
        expect(second[1].content).toContain(selection.text);
        expect(props.onAttachmentConsumed).toHaveBeenCalledOnce();
        expect(container.querySelector('.cbm-chat-answer .cbm-chat-source-content pre')?.textContent).toBe(selection.text);
    });

    it('retains the draft and attachment when the actual tokenizer rejects context size', async () => {
        const { props, runtime } = fixture(); runtime.countTokens.mockResolvedValue(99999);
        await render({ ...props, attachment: selection }); await click('Download & load'); await type('Too much context'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled(); expect(props.onAttachmentConsumed).not.toHaveBeenCalled();
        expect(container.querySelector('textarea')?.value).toBe('Too much context');
        expect(container.querySelector('pre')?.textContent).toBe(selection.text);
        expect(container.textContent).toContain('Nothing was sent or shortened');
    });

    it('retains unsent input on tokenizer failure and permits a retry', async () => {
        const { props, runtime } = fixture(); runtime.countTokens.mockRejectedValueOnce(new Error('Tokenizer unavailable'));
        await render({ ...props, attachment: selection }); await click('Download & load'); await type('My question'); await click('Send ↑');
        expect(container.querySelector('textarea')?.value).toBe('My question'); expect(props.onAttachmentConsumed).not.toHaveBeenCalled();
        expect(container.textContent).toContain('Tokenizer unavailable');
        await click('Send ↑'); expect(runtime.chat).toHaveBeenCalledOnce();
    });

    it('retries the same request and attachment without consuming a new selection', async () => {
        const { props, runtime } = fixture(); runtime.chat.mockRejectedValueOnce(new Error('GPU interrupted'));
        await render({ ...props, attachment: selection }); await click('Download & load'); await type('Explain'); await click('Send ↑');
        const request = runtime.chat.mock.calls[0][0];
        expect(container.textContent).toContain('GPU interrupted');
        await render({ ...props, attachment: { ...selection, id: 'new-selection', text: 'new source' } });
        await click('Retry');
        expect(runtime.chat.mock.calls[1][0]).toEqual(request);
        expect(props.onAttachmentConsumed).toHaveBeenCalledExactlyOnceWith(selection.id);
        expect(container.querySelector('.cbm-chat-pending pre')?.textContent).toBe('new source');
    });

    it('stops streaming without overlapping a second send and ignores late chunks', async () => {
        const { props, runtime } = fixture();
        const answer = deferred<string>(); let stream!: (chunk: string) => void;
        runtime.chat.mockImplementationOnce((_messages, onToken) => { stream = onToken; return answer.promise; });
        await render(props); await click('Download & load'); await type('Explain'); await click('Send ↑');
        await act(async () => stream('First part.')); await click('Stop');
        expect(runtime.stop).toHaveBeenCalledOnce(); expect(button('Stopping…').disabled).toBe(true);
        await type('Next question');
        await act(async () => container.querySelector('form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
        expect(runtime.chat).toHaveBeenCalledOnce();
        await act(async () => { stream('LATE CHUNK'); answer.resolve('First part. LATE CHUNK'); });
        expect(container.textContent).toContain('First part.'); expect(container.textContent).not.toContain('LATE CHUNK');
        expect(container.textContent).toContain('Stopped · partial answer');
        expect(runtime.dispose).not.toHaveBeenCalled();
        await click('Send ↑'); expect(runtime.chat).toHaveBeenCalledTimes(2);
    });

    it('can stop a pending token count before accepting a send', async () => {
        const { props, runtime } = fixture(); const count = deferred<number>(); runtime.countTokens.mockReturnValueOnce(count.promise);
        await render({ ...props, attachment: selection }); await click('Download & load'); await type('Keep this'); await click('Send ↑'); await click('Stop');
        await act(async () => count.resolve(100));
        expect(runtime.chat).not.toHaveBeenCalled(); expect(props.onAttachmentConsumed).not.toHaveBeenCalled();
        expect(container.querySelector('textarea')?.value).toBe('Keep this');
        expect(button('Send ↑').disabled).toBe(false);
    });

    it('ignores a stale prepare completion after download cancellation', async () => {
        const { props, runtime } = fixture(); const prepare = deferred<void>(); runtime.prepare.mockReturnValueOnce(prepare.promise);
        await render(props); await click('Download & load'); await click('Stop download');
        await act(async () => prepare.resolve());
        expect(runtime.dispose).toHaveBeenCalledOnce(); expect(document.querySelector('.cbm-chat-status')?.textContent).toBe('Off');
        expect(container.querySelector('textarea')).toBeNull();
        expect(button('Download & load').disabled).toBe(false);
    });

    it('unloads and deletes cached files separately while preserving the conversation', async () => {
        const { props, runtime } = fixture(); await render(props); await click('Download & load'); await type('Explain'); await click('Send ↑');
        await models(); await click('Unload model');
        expect(runtime.dispose).toHaveBeenCalledOnce(); expect(props.removeCache).not.toHaveBeenCalled();
        expect(container.textContent).toContain('Adds the two values.');
        await click('Delete cached model');
        expect(props.removeCache).toHaveBeenCalledExactlyOnceWith(BROWSER_MODELS[0].id);
        expect(container.textContent).toContain('Adds the two values.');
    });

    it('changes models without automatic download or losing conversation', async () => {
        const { props, runtime } = fixture(); await render(props); await click('Download & load'); await type('Explain'); await click('Send ↑'); await models();
        await act(async () => { const select = document.querySelector('#cbm-chat-model') as HTMLSelectElement; select.value = BROWSER_MODELS[1].id; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(runtime.dispose).toHaveBeenCalledOnce(); expect(props.createRuntime).toHaveBeenCalledOnce();
        expect(container.textContent).toContain('Adds the two values.'); expect(button('Send ↑').disabled).toBe(true);
        expect((document.querySelector('#cbm-chat-model') as HTMLSelectElement)?.value).toBe(BROWSER_MODELS[1].id);
    });

    it('does not move scroll position while the reader is inspecting earlier output', async () => {
        const { props, runtime } = fixture(); const answer = deferred<string>(); let stream!: (chunk: string) => void;
        runtime.chat.mockImplementationOnce((_messages, onToken) => { stream = onToken; return answer.promise; });
        await render(props); await click('Download & load'); await type('Explain'); await click('Send ↑');
        const log = container.querySelector('.cbm-chat-transcript') as HTMLDivElement;
        Object.defineProperties(log, { scrollHeight: { configurable: true, value: 1000 }, clientHeight: { configurable: true, value: 200 } });
        log.scrollTop = 100;
        await act(async () => log.dispatchEvent(new Event('scroll', { bubbles: true })));
        await act(async () => stream('New answer text'));
        expect(log.scrollTop).toBe(100); expect(button('Latest answer ↓')).toBeDefined();
        await click('Latest answer ↓'); expect(log.scrollTop).toBe(1000);
        await act(async () => answer.resolve('New answer text'));
    });

    it('clears conversation only after explicit confirmation', async () => {
        const { props } = fixture(); await render(props); await click('Download & load'); await type('Explain'); await click('Send ↑');
        const confirm = vi.spyOn(window, 'confirm').mockReturnValueOnce(false).mockReturnValueOnce(true);
        await click('New conversation'); expect(container.textContent).toContain('Adds the two values.');
        await click('New conversation'); expect(container.textContent).not.toContain('Adds the two values.'); expect(confirm).toHaveBeenCalledTimes(2);
    });

    it('unloads an active worker and ignores stale output while retaining its sent question', async () => {
        const { props, runtime } = fixture(); const answer = deferred<string>(); let stream!: (chunk: string) => void;
        runtime.chat.mockImplementationOnce((_messages, onToken) => { stream = onToken; return answer.promise; });
        await render(props); await click('Download & load'); await type('Explain'); await click('Send ↑'); await models();
        await act(async () => stream('Partial answer')); await click('Unload model');
        await act(async () => { stream('Ignored stale chunk'); answer.resolve('Ignored stale final'); });
        expect(runtime.dispose).toHaveBeenCalledOnce(); expect(container.textContent).toContain('Partial answer');
        expect(container.textContent).not.toContain('Ignored stale'); expect(container.textContent).toContain('Stopped · partial answer');
        expect(document.querySelector('.cbm-chat-status')?.textContent).toBe('Off');
    });

    it('includes graph evidence only after explicit choice, freezes it, and clears it after send', async () => {
        const { props, runtime } = fixture(); const evidence = { id: 'callers-1', label: 'Known callers', text: 'entry → sum' };
        await render({ ...props, context: [evidence] }); await click('Download & load'); await type('First question'); await click('Send ↑');
        expect(runtime.chat.mock.calls[0][0].at(-1)?.content).toBe('First question');
        await act(async () => (container.querySelector('input[type=checkbox]') as HTMLInputElement).click());
        evidence.text = 'changed graph';
        await render({ ...props, context: [] });
        expect(container.querySelector('.cbm-chat-pending pre')?.textContent).toBe('entry → sum');
        await type('Second question'); await click('Send ↑');
        expect(runtime.chat.mock.calls[1][0].at(-1)?.content).toContain('entry → sum');
        expect(runtime.chat.mock.calls[1][0].at(-1)?.content).not.toContain('changed graph');
        expect(container.querySelector('.cbm-chat-pending')).toBeNull();
        expect(container.querySelector('.cbm-chat-turn:last-child .cbm-chat-source-content pre')?.textContent).toBe('entry → sum');
        await click('Retry'); expect(runtime.chat.mock.calls[2][0]).toEqual(runtime.chat.mock.calls[1][0]);
    });

    it('sends controlled graph selection once with literal code and retains it for retry', async () => {
        const { props, runtime } = fixture(); const pendingContext = { id: 'galaxy-1', label: 'Graph node main', text: '{"node":"main","generation":"unavailable"}' };
        const callbacks = { onContextConsumed: vi.fn(), onContextRemoved: vi.fn() };
        await render({ ...props, ...callbacks, attachment: selection, pendingContext }); await click('Download & load'); await type('Explain both'); await click('Send ↑');
        const first = runtime.chat.mock.calls[0][0].at(-1)!.content;
        expect(first).toContain(selection.text); expect(first).toContain(pendingContext.text); expect(callbacks.onContextConsumed).toHaveBeenCalledExactlyOnceWith('galaxy-1');
        expect(container.querySelector('[aria-label="Remove graph selection"]')).toBeNull();
        await click('Retry'); expect(runtime.chat.mock.calls[1][0]).toEqual(runtime.chat.mock.calls[0][0]);
        await render({ ...props, ...callbacks, pendingContext }); await type('Follow up'); await click('Send ↑');
        expect(runtime.chat.mock.calls[2][0].at(-1)!.content).toBe('Follow up'); expect(callbacks.onContextConsumed).toHaveBeenCalledOnce();
    });

    it('keeps a controlled graph selection pending on overflow or tokenizer failure', async () => {
        const { props, runtime } = fixture(); runtime.countTokens.mockResolvedValueOnce(99999).mockRejectedValueOnce(new Error('count failed'));
        const pendingContext = { id: 'galaxy-1', label: 'Graph node main', text: 'Selected node facts' }; const onContextConsumed = vi.fn();
        await render({ ...props, pendingContext, onContextConsumed }); await click('Download & load'); await type('Explain'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled(); expect(onContextConsumed).not.toHaveBeenCalled(); expect(button('Remove graph selection')).toBeDefined();
        await click('Send ↑'); expect(onContextConsumed).not.toHaveBeenCalled(); expect(container.querySelector('textarea')?.value).toBe('Explain');
        await click('Send ↑'); expect(onContextConsumed).toHaveBeenCalledExactlyOnceWith('galaxy-1');
    });

    it('allows removing a controlled graph selection without sending or consuming it', async () => {
        const { props, runtime } = fixture(); const pendingContext = { id: 'galaxy-1', label: 'Graph node main', text: 'Selected node facts' }; const onContextConsumed = vi.fn(); const onContextRemoved = vi.fn();
        await render({ ...props, pendingContext, onContextConsumed, onContextRemoved }); await click('Remove graph selection');
        expect(onContextRemoved).toHaveBeenCalledExactlyOnceWith('galaxy-1'); expect(onContextConsumed).not.toHaveBeenCalled();
        await click('Download & load'); await type('Question'); await click('Send ↑'); expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toBe('Question');
    });

    it('keeps a newer graph selection when an earlier send finishes checking context', async () => {
        const { props, runtime } = fixture(); const count = deferred<number>(); runtime.countTokens.mockReturnValueOnce(count.promise);
        const oldContext = { id: 'old', label: 'Old selection', text: 'Old graph facts' }; const newContext = { id: 'new', label: 'New selection', text: 'New graph facts' }; const onContextConsumed = vi.fn();
        await render({ ...props, pendingContext: oldContext, onContextConsumed }); await click('Download & load'); await type('First'); await click('Send ↑');
        await render({ ...props, pendingContext: newContext, onContextConsumed }); await act(async () => count.resolve(100));
        expect(onContextConsumed).toHaveBeenCalledExactlyOnceWith('old');
        expect(container.querySelector('.cbm-chat-pending pre')?.textContent).toBe('New graph facts');
        await type('Second'); await click('Send ↑');
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain('Old graph facts');
        expect(runtime.chat.mock.calls[1][0].at(-1)!.content).toContain('New graph facts');
        expect(onContextConsumed.mock.calls).toEqual([['old'], ['new']]);
    });

    it('formats user and assistant Markdown while keeping requests and code attachments literal', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('## Result\n\n- A **clear** answer\n\n```js\nreturn value;\n```');
        const attachedCode = { ...selection, text: '<b>literal source</b>\n\treturn value;' };
        const prompt = '**Question:** explain `value`.';
        await render({ ...props, attachment: attachedCode }); await click('Download & load'); await type(prompt); await click('Send ↑');
        expect(container.querySelector('.cbm-chat-question strong')?.textContent).toBe('Question:');
        expect(container.querySelector('.cbm-chat-answer h2')?.textContent).toBe('Result');
        expect(container.querySelector('.cbm-chat-answer li strong')?.textContent).toBe('clear');
        expect(container.querySelector('.cbm-chat-answer pre code')?.textContent).toBe('return value;\n');
        expect(container.querySelector('.cbm-chat-source-content .cbm-chat-attachment pre')?.textContent).toBe(attachedCode.text);
        expect(container.querySelector('.cbm-chat-source-content .cbm-chat-attachment b')).toBeNull();
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain(prompt);
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain(attachedCode.text);
    });

    it('keeps ready reader source out of the composer without downloading or offering removal', async () => {
        const { props } = fixture();
        const context = reader('Loaded excerpt'); context.source!.partial = 'Only lines 20–80 are loaded.';
        await render({ ...props, readerContext: context, attachment: { ...selection, text: 'IGNORED_MANUAL_CODE' } });
        expect(props.createRuntime).not.toHaveBeenCalled();
        expect(container.querySelector('[aria-label="Source for next message"]')).toBeNull();
        expect(container.querySelector('.cbm-chat-composer pre')).toBeNull();
        expect(container.textContent).not.toContain('Current file');
        expect(container.textContent).not.toContain('IGNORED_MANUAL_CODE');
        expect(container.querySelector('[aria-label="Remove code attachment"]')).toBeNull();
        expect(container.querySelector('textarea')).toBeNull();
    });

    it('refreshes one system source from file to literal selection to a different file without accumulating code', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, readerContext: reader('WHOLE_FIRST_FILE') });
        await click('Download & load'); await type('Explain the file'); await click('Send ↑');
        await render({ ...props, readerContext: reader(selection.text, 'selection') });
        await type('Explain the marked code'); await click('Send ↑');
        await render({ ...props, readerContext: reader('WHOLE_SECOND_FILE', 'file', 'src/other.ts') });
        await type('Explain this other file'); await click('Send ↑');
        const requests = runtime.chat.mock.calls.map(call => call[0]);
        expect(requests[0][0].content).toContain('WHOLE_FIRST_FILE');
        expect(requests[1][0].content).toContain(selection.text);
        expect(requests[1].map(message => message.content).join('\n')).not.toContain('WHOLE_FIRST_FILE');
        expect(requests[2][0].content).toContain('WHOLE_SECOND_FILE');
        expect(requests[2].map(message => message.content).join('\n')).not.toContain('WHOLE_FIRST_FILE');
        expect(requests[2].map(message => message.content).join('\n')).not.toContain(selection.text);
        expect(requests[2].filter(message => message.role === 'user').map(message => message.content)).toEqual(['Explain the file', 'Explain the marked code', 'Explain this other file']);
        expect(props.onAttachmentConsumed).not.toHaveBeenCalled();
        expect(container.querySelectorAll('.cbm-chat-answer .cbm-chat-attachment')).toHaveLength(3);
        await render(props); await type('Ask from another workspace'); await click('Send ↑');
        expect(runtime.chat.mock.calls[3][0].map(message => message.content).join('\n')).not.toMatch(/WHOLE_FIRST_FILE|WHOLE_SECOND_FILE/);
    });

    it('uses the same frozen reader snapshot for counting and generation despite navigation or prop mutation', async () => {
        const { props, runtime } = fixture(); const count = deferred<number>();
        runtime.countTokens.mockReturnValueOnce(count.promise);
        const original = reader('ORIGINAL_LITERAL\r\n\t  ', 'selection');
        await render({ ...props, readerContext: original }); await click('Download & load'); await type('Explain'); await click('Send ↑');
        const counted = runtime.countTokens.mock.calls[0][0];
        original.source!.text = 'MUTATED_AFTER_COUNT';
        await render({ ...props, readerContext: reader('NEW_CURRENT_FILE', 'file', 'new.ts') });
        await act(async () => count.resolve(100));
        expect(runtime.chat.mock.calls[0][0]).toEqual(counted);
        expect(runtime.chat.mock.calls[0][0][0].content).toContain('ORIGINAL_LITERAL\r\n\t  ');
        expect(container.querySelector('.cbm-chat-answer .cbm-chat-source-content pre')?.textContent).toBe('ORIGINAL_LITERAL\r\n\t  ');
        expect(container.querySelector('.cbm-chat-source-content')?.textContent).not.toContain('NEW_CURRENT_FILE');
        expect(container.querySelector('.cbm-chat-composer pre')).toBeNull();
        expect(runtime.chat.mock.calls[0][0][0].content).not.toContain('MUTATED_AFTER_COUNT');
    });

    it('retries an automatic request exactly while newer source is loading', async () => {
        const { props, runtime } = fixture(); runtime.chat.mockRejectedValueOnce(new Error('GPU interrupted'));
        await render({ ...props, readerContext: reader('ORIGINAL_RETRY_SOURCE') }); await click('Download & load'); await type('Explain'); await click('Send ↑');
        const request = runtime.chat.mock.calls[0][0];
        await render({ ...props, readerContext: { project: 'sample', path: 'loading.ts', status: 'loading' } });
        await type('New question');
        expect(button('Send ↑').disabled).toBe(true);
        await click('Retry');
        expect(runtime.chat.mock.calls[1][0]).toEqual(request);
        expect(runtime.countTokens.mock.calls[1][0]).toEqual(request);
        expect(container.querySelector('textarea')?.value).toBe('New question');
        expect(props.onAttachmentConsumed).not.toHaveBeenCalled();
    });

    it('keeps new sends pending until a switched file is loaded and never falls back to stale code', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, readerContext: reader('OLD_CODE') }); await click('Download & load'); await type('Explain');
        await render({ ...props, attachment: selection, readerContext: { ...reader('STALE_CODE'), status: 'loading' } });
        expect(button('Send ↑').disabled).toBe(true);
        await act(async () => container.querySelector('form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(container.querySelector('.cbm-chat-composer pre')).toBeNull();
        expect(container.textContent).toContain('Loading current file');
        await render({ ...props, attachment: selection, readerContext: { project: 'sample', path: 'new.ts', status: 'unavailable' } });
        await click('Send ↑');
        expect(runtime.chat.mock.calls[0][0][0].content).toContain('"status":"unavailable"');
        expect(runtime.chat.mock.calls[0][0].map(message => message.content).join('\n')).not.toContain(selection.text);
        expect(runtime.chat.mock.calls[0][0].map(message => message.content).join('\n')).not.toMatch(/OLD_CODE|STALE_CODE/);
        await render({ ...props, readerContext: { project: 'sample', status: 'empty' } });
        expect(container.querySelector('.cbm-chat-source-state')?.textContent).toContain('Open a file');
    });

    it('preserves explicitly chosen graph evidence alongside automatic system source', async () => {
        const { props, runtime } = fixture();
        const pendingContext = { id: 'g', label: 'Callers', text: 'entry calls helper' };
        await render({ ...props, readerContext: reader('CURRENT_CODE'), pendingContext, attachment: { ...selection, text: 'MANUAL_IGNORED' } });
        await click('Download & load'); await type('Explain both'); await click('Send ↑');
        expect(runtime.chat.mock.calls[0][0][0].content).toContain('CURRENT_CODE');
        expect(runtime.chat.mock.calls[0][0].at(-1)?.content).toContain(pendingContext.text);
        expect(runtime.chat.mock.calls[0][0].at(-1)?.content).not.toContain('CURRENT_CODE');
        expect(runtime.chat.mock.calls[0][0].map(message => message.content).join('\n')).not.toContain('MANUAL_IGNORED');
        expect(props.onAttachmentConsumed).not.toHaveBeenCalled();
    });

    it('does not steal reader focus when automatic source changes', async () => {
        const { props } = fixture();
        await render({ ...props, readerContext: reader('FIRST') }); await click('Download & load');
        const readerControl = document.createElement('button'); document.body.append(readerControl); readerControl.focus();
        try {
            await render({ ...props, readerContext: reader('SECOND', 'selection'), attachment: selection });
            expect(document.activeElement).toBe(readerControl);
            await render({ ...props, readerContext: reader('THIRD', 'file', 'other.ts'), attachment: { ...selection, id: 'changed-manual' } });
            expect(document.activeElement).toBe(readerControl);
        } finally { readerControl.remove(); }
    });

    it('declines a request that remains oversized after disclosed evidence budgeting', async () => {
        const { props, runtime } = fixture(); runtime.countTokens.mockResolvedValue(99_999);
        const text = 'x'.repeat(30_000) + '\r\n\tlast  ';
        await render({ ...props, readerContext: reader(text) }); await click('Download & load'); await type('Explain exactly'); await click('Send ↑');
        expect(runtime.countTokens.mock.calls[0][0].at(-1)!.content).not.toContain(text);
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(container.querySelector('textarea')?.value).toBe('Explain exactly');
        expect(container.querySelector('.cbm-chat-composer pre')).toBeNull();
        expect(container.textContent).toContain('Nothing was sent or shortened');
    });
});


describe('proactive selection explanations', () => {
    afterEach(() => vi.useRealTimers());
    const graph = (text: string, id = text) => ({ id, label: 'Selected node', text: JSON.stringify({ evidence: { kind: 'current-selection-evidence', project: 'sample', view: 'galaxy', source: 'query_graph', generation: 'v1', selected: { name: text }, relationships: { count: 1 } }, omissions: [] }) });
    async function settleSelection() { await act(async () => { await vi.advanceTimersByTimeAsync(650); }); }

    it('requires explicit loading, debounces selections and ignores event-only identity changes', async () => {
        vi.useFakeTimers();
        const { props, runtime } = fixture();
        await render({ ...props, proactive: true, proactiveSelection: graph('first') });
        await settleSelection(); expect(props.createRuntime).not.toHaveBeenCalled();
        await click('Download & load');
        await render({ ...props, proactive: true, proactiveSelection: graph('latest') });
        await settleSelection();
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain('latest');
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).not.toContain('first');
        expect(container.querySelectorAll('.cbm-chat-turn')).toHaveLength(0);
        expect(container.querySelector('[aria-label="Current selection explanation"]')?.textContent).toContain('Adds the two values.');
        await render({ ...props, proactive: true, proactiveSelection: graph('latest', 'new-event-id') });
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledOnce();
        await render({ ...props, proactive: true });
        expect(container.querySelector('[aria-label="Current selection explanation"]')).toBeNull();
    });

    it('stops stale generation and waits for its settlement before starting the latest selection', async () => {
        vi.useFakeTimers();
        const { props, runtime } = fixture(); const old = deferred<string>();
        runtime.chat.mockReturnValueOnce(old.promise);
        await render({ ...props, proactive: true, proactiveSelection: graph('old') });
        await click('Download & load'); await settleSelection();
        await render({ ...props, proactive: true, proactiveSelection: graph('new') });
        expect(runtime.stop).toHaveBeenCalledOnce();
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledOnce();
        await act(async () => { runtime.chat.mock.calls[0][1]('obsolete token'); old.resolve('obsolete result'); });
        expect(container.textContent).not.toContain('obsolete');
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledTimes(2);
        expect(runtime.chat.mock.calls[1][0].at(-1)!.content).toContain('new');
    });

    it('keeps explaining new selections while preserving drafts and manual history', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        await render({ ...props, proactive: true, proactiveSelection: graph('one') });
        await click('Download & load'); await type('What does it do?');
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledOnce();
        expect(container.querySelector('textarea')?.value).toBe('What does it do?');
        await click('Send ↑');
        expect(runtime.chat.mock.calls[1][0].at(-1)!.content).toContain('one');
        await type('And this?');
        await render({ ...props, proactive: true, proactiveSelection: graph('two') });
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledTimes(3);
        expect(container.querySelector('.cbm-chat-question')?.textContent).toContain('What does it do?');
        expect(runtime.chat.mock.calls[2][0].at(-1)!.content).toContain('two');
        expect(container.querySelector('textarea')?.value).toBe('And this?');
        await click('Send ↑');
        expect(runtime.chat.mock.calls[3][0].at(-1)!.content).toContain('two');
        expect(runtime.chat.mock.calls[3][0].at(-1)!.content).not.toContain('one');
    });

    it('prioritizes a frozen question only after automatic generation settles, then explains the latest selection', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        const automatic = deferred<string>(); const manual = deferred<string>();
        runtime.chat.mockReturnValueOnce(automatic.promise).mockReturnValueOnce(manual.promise);
        const original = reader('ORIGINAL_SOURCE', 'selection');
        await render({ ...props, proactive: true, readerContext: original });
        await click('Download & load'); await settleSelection();
        await type('Explain this exact code'); await click('Send ↑');
        expect(runtime.stop).toHaveBeenCalledOnce();
        expect(container.textContent).toContain('Your question is next');
        expect(runtime.countTokens).toHaveBeenCalledOnce();
        original.source!.text = 'MUTATED_ORIGINAL';
        await render({ ...renderedProps, readerContext: reader('INTERMEDIATE_SOURCE') });
        await render({ ...renderedProps, readerContext: reader('LATEST_SOURCE') });
        await type('Next draft'); await settleSelection();
        expect(runtime.chat).toHaveBeenCalledOnce();
        await act(async () => automatic.resolve('STALE_AUTOMATIC_ANSWER'));
        expect(runtime.chat).toHaveBeenCalledTimes(2);
        const question = runtime.chat.mock.calls[1][0];
        expect(question[0].content).toContain('ORIGINAL_SOURCE');
        expect(question[0].content).not.toMatch(/MUTATED_ORIGINAL|INTERMEDIATE_SOURCE|LATEST_SOURCE/);
        expect(question.at(-1)?.content).toBe('Explain this exact code');
        expect(container.querySelector('textarea')?.value).toBe('Next draft');
        expect(container.textContent).not.toContain('STALE_AUTOMATIC_ANSWER');
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledTimes(2);
        await act(async () => manual.resolve('Manual answer.'));
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledTimes(3);
        expect(runtime.chat.mock.calls[2][0].at(-1)?.content).toContain('LATEST_SOURCE');
        expect(runtime.chat.mock.calls[2][0].at(-1)?.content).not.toContain('INTERMEDIATE_SOURCE');
        expect(container.querySelector('.cbm-chat-answer .cbm-chat-source-content pre')?.textContent).toBe('ORIGINAL_SOURCE');
        expect(container.textContent).toContain('Current selection ↑');
    });

    it('waits for cancelled automatic token counting before counting a manual question', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture(); const counting = deferred<number>();
        runtime.countTokens.mockReturnValueOnce(counting.promise);
        await render({ ...props, proactive: true, readerContext: reader('SOURCE') });
        await click('Download & load'); await settleSelection();
        await type('Question'); await click('Send ↑');
        expect(runtime.stop).toHaveBeenCalledOnce();
        expect(runtime.countTokens).toHaveBeenCalledOnce();
        expect(runtime.chat).not.toHaveBeenCalled();
        await act(async () => counting.resolve(100));
        expect(runtime.countTokens).toHaveBeenCalledTimes(2);
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[0][0].at(-1)?.content).toBe('Question');
    });

    it('cancels a queued question on Stop without silently sending or restarting it', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture(); const automatic = deferred<string>();
        runtime.chat.mockReturnValueOnce(automatic.promise);
        await render({ ...props, proactive: true, readerContext: reader('SOURCE') });
        await click('Download & load'); await settleSelection();
        await type('Keep my unsent question'); await click('Send ↑'); await click('Stop');
        await act(async () => automatic.resolve('CANCELLED'));
        await settleSelection();
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.countTokens).toHaveBeenCalledOnce();
        expect(container.querySelector('textarea')?.value).toBe('Keep my unsent question');
        expect(container.querySelector('.cbm-chat-turn')).toBeNull();
        await click('Send ↑'); expect(runtime.chat).toHaveBeenCalledTimes(2);
    });

    it('discards a queued question after unload even when the cancelled worker later resolves', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture(); const automatic = deferred<string>();
        runtime.chat.mockReturnValueOnce(automatic.promise);
        await render({ ...props, proactive: true, readerContext: reader('SOURCE') });
        await click('Download & load'); await settleSelection();
        await type('Do not send after unload'); await click('Send ↑');
        await models(); await click('Unload model');
        await act(async () => automatic.resolve('LATE_ANSWER'));
        await settleSelection();
        expect(runtime.dispose).toHaveBeenCalledOnce();
        expect(runtime.countTokens).toHaveBeenCalledOnce();
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(container.textContent).not.toContain('LATE_ANSWER');
        expect(container.querySelector('.cbm-chat-turn')).toBeNull();
        await click('Load model'); await click('Send ↑');
        expect(runtime.chat).toHaveBeenCalledTimes(2);
        expect(runtime.chat.mock.calls[1][0].at(-1)?.content).toBe('Do not send after unload');
    });

    it('drains old project work and retains the model without sending its queued question into the new project', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture(); const automatic = deferred<string>();
        runtime.chat.mockReturnValueOnce(automatic.promise);
        await render({ ...props, historyKey: 'project-a', proactive: true, readerContext: reader('PROJECT_A') });
        await click('Download & load'); await settleSelection();
        await type('Old project question'); await click('Send ↑');
        await render({ ...renderedProps, historyKey: 'project-b', readerContext: reader('PROJECT_B') });
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledOnce();
        await act(async () => automatic.resolve('OLD_PROJECT_RESULT'));
        await settleSelection();
        expect(runtime.dispose).not.toHaveBeenCalled();
        expect(props.createRuntime).toHaveBeenCalledOnce();
        expect(runtime.chat).toHaveBeenCalledTimes(2);
        expect(runtime.chat.mock.calls[1][0].at(-1)?.content).toContain('PROJECT_B');
        expect(runtime.chat.mock.calls[1][0].at(-1)?.content).not.toContain('Old project question');
        expect(container.querySelector('.cbm-chat-turn')).toBeNull();
        expect(container.textContent).not.toContain('OLD_PROJECT_RESULT');
    });

    it('drains a manual answer on project change before reusing the loaded model for new source', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture(); const manual = deferred<string>();
        await render({ ...props, historyKey: 'project-a', proactive: true, readerContext: reader('PROJECT_A') });
        await click('Download & load'); await settleSelection();
        runtime.chat.mockReturnValueOnce(manual.promise);
        await type('Old project question'); await click('Send ↑');
        await render({ ...renderedProps, historyKey: 'project-b', readerContext: reader('PROJECT_B') });
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledTimes(2);
        await act(async () => { runtime.chat.mock.calls[1][1]('STALE_CHUNK'); manual.resolve('STALE_MANUAL_ANSWER'); });
        await settleSelection();
        expect(runtime.dispose).not.toHaveBeenCalled();
        expect(runtime.chat).toHaveBeenCalledTimes(3);
        expect(runtime.chat.mock.calls[2][0].at(-1)?.content).toContain('PROJECT_B');
        expect(container.textContent).not.toMatch(/STALE_CHUNK|STALE_MANUAL_ANSWER/);
    });

    it('keeps the manual answer visible when navigation queues a new explanation', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture(); const answer = deferred<string>();
        await render({ ...props, proactive: true, readerContext: reader('FIRST') });
        await click('Download & load'); await settleSelection();
        runtime.chat.mockReturnValueOnce(answer.promise);
        await type('Manual question'); await click('Send ↑');
        const log = container.querySelector('.cbm-chat-transcript') as HTMLDivElement;
        Object.defineProperties(log, { scrollHeight: { configurable: true, value: 1000 }, clientHeight: { configurable: true, value: 100 } });
        log.scrollTop = 900;
        await render({ ...renderedProps, readerContext: reader('SECOND') });
        expect(log.scrollTop).toBe(900);
        await act(async () => answer.resolve('Manual answer'));
        await settleSelection();
        expect(log.scrollTop).not.toBe(0);
        await click('Current selection ↑'); expect(log.scrollTop).toBe(0);
    });

    it('uses exact reader source, never falls back while source is loading, and cancels pending work on unload', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        await render({ ...props, proactive: true, readerContext: reader('literal selection', 'selection'), proactiveSelection: graph('irrelevant graph') });
        await click('Download & load'); await settleSelection();
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain('literal selection');
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).not.toContain('irrelevant graph');
        await render({ ...props, proactive: true, readerContext: { project: 'sample', path: 'next.ts', status: 'loading' }, proactiveSelection: graph('stale') });
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledOnce();
        await render({ ...props, proactive: true, readerContext: reader('next file') });
        await models(); await click('Unload model'); await settleSelection();
        expect(runtime.chat).toHaveBeenCalledOnce();
    });

    it('reports actual runtime status and opens settings on an explicit settings request', async () => {
        const { props } = fixture(); const onAgentStateChange = vi.fn();
        await render({ ...props, onAgentStateChange }); expect(onAgentStateChange).toHaveBeenLastCalledWith('off');
        await click('Download & load'); expect(onAgentStateChange).toHaveBeenLastCalledWith('active');
        expect(container.querySelector('#cbm-chat-model')).toBeNull();
        await render({ ...props, onAgentStateChange, settingsRequest: (renderedProps.settingsRequest ?? 0) + 1 });
        expect(document.querySelector('#cbm-chat-model')).not.toBeNull();
        await click('Unload model'); expect(onAgentStateChange).toHaveBeenLastCalledWith('off');
    });
});


describe('selection explainer controls', () => {
    afterEach(() => vi.useRealTimers());
    it('uses original source for follow-ups without recycling automatic model prose', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        await render({ ...props, proactive: true, readerContext: reader('function sum(a,b) { return a+b; }') });
        await click('Download & load');
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        await type('Why?'); await click('Send ↑');
        const request = runtime.chat.mock.calls[1][0];
        expect(request[0].content).not.toContain('Adds the two values.');
        expect(request[0].content).toContain('function sum(a,b)');
        expect(request.map(message => message.role)).toEqual(['system', 'user']);
    });

    it('pauses automatic explanations and retries a stopped explanation only on request', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture(); const answer = deferred<string>();
        runtime.chat.mockReturnValueOnce(answer.promise);
        await render({ ...props, proactive: true, readerContext: reader('source') });
        await click('Download & load');
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        await click('Stop'); await act(async () => answer.resolve('cancelled'));
        await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
        expect(runtime.chat).toHaveBeenCalledOnce();
        await click('Explain again');
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        expect(runtime.chat).toHaveBeenCalledTimes(2);
        await models();
        const automatic = [...document.querySelectorAll('label')].find(label => label.textContent?.includes('Explain selections automatically'))!.querySelector('input')!;
        await act(async () => automatic.click());
        await render({ ...props, proactive: true, readerContext: reader('different source') });
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        expect(runtime.chat).toHaveBeenCalledTimes(2);
    });
});


it('does not generate from a superseded token count and can return to that selection', async () => {
    vi.useFakeTimers();
    try {
        const { props, runtime } = fixture(); const counting = deferred<number>();
        runtime.countTokens.mockReturnValueOnce(counting.promise);
        await render({ ...props, proactive: true, readerContext: reader('first') });
        await click('Download & load');
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        await render({ ...props, proactive: true, readerContext: reader('second') });
        await render({ ...props, proactive: true, readerContext: reader('first') });
        await act(async () => counting.resolve(100));
        expect(runtime.chat).not.toHaveBeenCalled();
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain('first');
    } finally { vi.useRealTimers(); }
});


describe('live-browser regressions', () => {
    afterEach(() => vi.useRealTimers());
    it('keeps a compact composer and places frozen source beside the manual answer speaker', async () => {
        const { props, runtime } = fixture();
        const original = reader('ORIGINAL_SOURCE'); original.source!.partial = 'Only lines 3–4 were loaded.';
        const pendingContext = { id: 'graph-source', label: 'Callers', text: 'entry calls original' };
        await render({ ...props, readerContext: original, pendingContext }); await click('Download & load');
        const textarea = container.querySelector('textarea')!;
        expect(textarea.rows).toBe(1);
        expect(textarea.parentElement?.querySelector('[aria-label="Send ↑"]')).not.toBeNull();
        expect(container.querySelector('.cbm-chat-clear')).toBeNull();
        expect(container.querySelector('.cbm-chat-reader-source')).toBeNull();
        expect(container.querySelector('[aria-label="Remove graph selection"]')).not.toBeNull();
        await type('Explain the source'); await click('Send ↑');
        const response = container.querySelector('.cbm-chat-answer')!;
        const source = response.querySelector('.cbm-chat-response-source') as HTMLDetailsElement;
        expect(response.firstElementChild).toBe(source);
        expect(source.open).toBe(false);
        expect(source.querySelector('summary')?.textContent).toContain('Agentⓘ Source');
        expect(source.textContent).toContain('Only lines 3–4 were loaded.');
        expect(source.textContent).toContain('entry calls original');
        await act(async () => source.querySelector('summary')!.click());
        expect(source.open).toBe(true);
        await render({ ...renderedProps, readerContext: reader('NEW_SOURCE', 'file', 'new.ts') });
        expect(source.querySelector('pre')?.textContent).toBe('ORIGINAL_SOURCE');
        expect(source.textContent).not.toContain('NEW_SOURCE');
        expect(container.querySelector('.cbm-chat-composer pre')).toBeNull();
        expect(container.querySelector('.cbm-chat-clear')).not.toBeNull();
        expect(runtime.chat.mock.calls[0][0][0].content).toContain('ORIGINAL_SOURCE');
    });

    it('shows automatic explanations from their start with older manual turns and on reopening', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        const answer = deferred<string>();
        await render(props); await click('Download & load');
        await type('Earlier manual question'); await click('Send ↑');
        const log = container.querySelector('.cbm-chat-transcript') as HTMLDivElement;
        Object.defineProperties(log, { scrollHeight: { configurable: true, value: 1000 }, clientHeight: { configurable: true, value: 100 } });
        log.scrollTop = 900;
        await act(async () => log.dispatchEvent(new Event('scroll', { bubbles: true })));
        runtime.chat.mockReturnValueOnce(answer.promise);
        await render({ ...renderedProps, proactive: true, readerContext: reader('const first = 1;') });
        expect(log.scrollTop).toBe(0);
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        expect(log.scrollTop).toBe(0);
        await act(async () => answer.resolve('Declares first with the value 1.'));
        expect(log.scrollTop).toBe(0);
        expect(log.querySelector('[aria-label="Current selection explanation"]')?.textContent).toContain('Declares first');
        expect(log.querySelector('.cbm-chat-question')?.textContent).toContain('Earlier manual question');
        log.scrollTop = 900;
        await act(async () => log.dispatchEvent(new Event('scroll', { bubbles: true })));
        await render({ ...renderedProps, readerContext: reader('const second = 2;') });
        expect(log.scrollTop).toBe(0);
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        expect(log.scrollTop).toBe(0);
        log.scrollTop = 400;
        await act(async () => log.dispatchEvent(new Event('scroll', { bubbles: true })));
        await render({ ...renderedProps, open: false });
        await render({ ...renderedProps, open: true });
        expect(log.scrollTop).toBe(0);
    });

    it('preserves a deliberate history scroll when an automatic explanation finishes', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture(); const answer = deferred<string>();
        runtime.chat.mockReturnValueOnce(answer.promise);
        await render({ ...props, proactive: true, readerContext: reader('const value = 1;') });
        await click('Download & load');
        const log = container.querySelector('.cbm-chat-transcript') as HTMLDivElement;
        Object.defineProperties(log, { scrollHeight: { configurable: true, value: 1000 }, clientHeight: { configurable: true, value: 100 } });
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        log.scrollTop = 350;
        await act(async () => log.dispatchEvent(new Event('scroll', { bubbles: true })));
        await act(async () => answer.resolve('Declares value with the value 1.'));
        expect(log.scrollTop).toBe(350);
        await type('A draft');
        await render({ ...renderedProps, readerContext: reader('const value = 1;') });
        expect(log.scrollTop).toBe(350);
        expect(container.querySelector('.cbm-chat-jump')?.textContent).toBe('Current selection ↑');
        await click('Current selection ↑');
        expect(log.scrollTop).toBe(0);
    });

    it('follows manual replies after an explanation while respecting a later history scroll', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        await render({ ...props, proactive: true, readerContext: reader('const value = 1;') });
        await click('Download & load');
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        const log = container.querySelector('.cbm-chat-transcript') as HTMLDivElement;
        let height = 1000;
        Object.defineProperties(log, { scrollHeight: { configurable: true, get: () => height }, clientHeight: { configurable: true, value: 100 } });
        log.scrollTop = 0;
        await act(async () => log.dispatchEvent(new Event('scroll', { bubbles: true })));
        const answer = deferred<string>(); let stream!: (chunk: string) => void;
        runtime.chat.mockImplementationOnce((_messages, onToken) => { stream = onToken; return answer.promise; });
        await type('Explain more'); await click('Send ↑');
        expect(log.scrollTop).toBe(1000);
        height = 1200;
        await act(async () => stream('Manual response.'));
        expect(log.scrollTop).toBe(1200);
        log.scrollTop = 200;
        await act(async () => log.dispatchEvent(new Event('scroll', { bubbles: true })));
        await act(async () => stream(' More detail.'));
        expect(log.scrollTop).toBe(200);
        expect(button('Latest answer ↓')).toBeDefined();
        await act(async () => answer.resolve('Manual response. More detail.'));
        expect(log.scrollTop).toBe(200);
    });

    it('renders a useful generated explanation as Markdown without requiring citation JSON', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('Casts `CBM_NOT_FOUND` to an unsigned integer and assigns it to `u.i`.');
        await render({ ...props, proactive: true, readerContext: reader('u.i = (uintptr_t)CBM_NOT_FOUND;', 'selection') });
        await click('Download & load');
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        const card = container.querySelector('[aria-label="Current selection explanation"]');
        expect(card?.textContent).toContain('Casts CBM_NOT_FOUND');
        expect(card?.querySelector('code')?.textContent).toBe('CBM_NOT_FOUND');
        expect(card?.textContent).not.toContain('A generated explanation is unavailable');
        expect(card?.querySelector('details')?.open).toBe(false);
        expect(card?.firstElementChild).toBe(card?.querySelector('.cbm-chat-response-source'));
        expect(card?.querySelector('.cbm-chat-source-content pre')?.textContent).toContain('u.i = (uintptr_t)CBM_NOT_FOUND;');
        expect(container.querySelector('.cbm-chat-composer pre')).toBeNull();
    });

    it('does not promote an automatic model guess into follow-up instructions', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('Invented destructor behavior.');
        await render({ ...props, proactive: true, readerContext: reader('u.i = (uintptr_t)CBM_NOT_FOUND;', 'selection') });
        await click('Download & load');
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        await type('Quote the selected assignment.'); await click('Send ↑');
        expect(runtime.chat.mock.calls[1][0][0].content).not.toContain('Invented destructor behavior.');
    });

    it('invalidates a failed GPU runtime and reports error instead of active', async () => {
        const { props, runtime } = fixture(); const onAgentStateChange = vi.fn();
        runtime.chat.mockRejectedValueOnce(new Error("failed to call OrtRun(): GPUBuffer mapAsync invalid buffer"));
        await render({ ...props, onAgentStateChange }); await click('Download & load');
        await type('Explain'); await click('Send ↑');
        expect(runtime.dispose).toHaveBeenCalledOnce();
        expect(onAgentStateChange).toHaveBeenLastCalledWith('error');
        expect(container.textContent).toContain('Reload');
        expect(document.querySelector('dialog')).toBeNull();
    });

    it('prepares a bounded, disclosed excerpt for an oversized file before inference', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        runtime.countTokens.mockImplementation(async messages => Math.ceil(messages.reduce((sum, message) => sum + message.content.length, 0) / 4));
        await render({ ...props, proactive: true, readerContext: reader(Array.from({ length: 12000 }, (_, line) => `const value${line} = ${line};`).join('\n')) });
        await click('Download & load');
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        expect(runtime.chat).toHaveBeenCalledOnce();
        const request = runtime.chat.mock.calls[0][0];
        expect(request.reduce((sum, message) => sum + message.content.length, 0)).toBeLessThan(7000);
        expect(container.querySelector('[aria-label="Current selection explanation"]')?.textContent).toMatch(/excerpt|sample|omitted/i);
    });
});
