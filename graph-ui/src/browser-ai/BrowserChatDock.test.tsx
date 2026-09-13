// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import BrowserChatDock, { type BrowserChatAttachment, type BrowserChatDockProps, type BrowserChatReaderContext } from './BrowserChatDock';
import type { BrowserAiProgress, BrowserChatMessage } from './browser-ai-runtime';
import { BROWSER_MODELS } from './model-policy';

let container: HTMLDivElement;
let root: Root;
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
    const props = { open: true, onClose: vi.fn(), onAttachmentConsumed: vi.fn(), onAttachmentRemoved: vi.fn(), createRuntime: vi.fn(() => runtime), removeCache: vi.fn(async () => {}) };
    return { runtime, props };
}
async function render(props: BrowserChatDockProps): Promise<void> { await act(async () => root.render(<BrowserChatDock {...props} />)); }
function button(label: string): HTMLButtonElement {
    const target = [...container.querySelectorAll('button')].find(item => item.textContent === label || item.getAttribute('aria-label') === label);
    expect(target, `button ${label}`).toBeDefined(); return target!;
}
async function click(label: string): Promise<void> { await act(async () => button(label).click()); }
async function type(value: string): Promise<void> {
    const input = container.querySelector('textarea')!;
    await act(async () => {
        Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')!.set!.call(input, value);
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
}
async function models(): Promise<void> {
    if (!container.querySelector('#cbm-chat-model')) await act(async () => (container.querySelector('.cbm-chat-modelbar button') as HTMLButtonElement).click());
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const attachmentData = (message: BrowserChatMessage) => JSON.parse(message.content.slice(message.content.indexOf('\n{') + 1).split('\n')[0]);

describe('persistent local browser chat', () => {
    it('opens without creating a worker or downloading a model and exposes explicit settings', async () => {
        const { props } = fixture(); await render({ ...props, attachment: selection });
        expect(props.createRuntime).not.toHaveBeenCalled();
        expect(button('Download & load').disabled).toBe(false);
        expect(container.querySelector('pre')?.textContent).toBe(selection.text);
        expect(container.querySelectorAll('option')).toHaveLength(BROWSER_MODELS.length);
        expect(container.textContent).toContain('No model downloads automatically');
        expect(container.querySelector('.cbm-chat-settings h3')?.textContent).toBe('Enable chat');
        expect(container.querySelector('[role="log"]')).toBeNull();
        expect(container.querySelector('textarea')).toBeNull();
    });

    it('offers a compact enable action without starting a download or consuming selected code', async () => {
        const { props } = fixture(); const onOpen = vi.fn();
        await render({ ...props, open: false, showCollapsed: true, onOpen, attachment: selection });
        expect(container.querySelector('aside')?.hidden).toBe(false);
        expect(container.querySelector('form')).toBeNull();
        expect(container.querySelector('select')).toBeNull();
        await click('Enable chat');
        expect(onOpen).toHaveBeenCalledOnce();
        expect(props.createRuntime).not.toHaveBeenCalled();
        expect(props.onAttachmentConsumed).not.toHaveBeenCalled();
        await render({ ...props, showCollapsed: true, onOpen, attachment: selection });
        expect(button('Download & load').disabled).toBe(false);
        expect(container.querySelector('pre')?.textContent).toBe(selection.text);
        expect(props.createRuntime).not.toHaveBeenCalled();
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
        expect(container.querySelector('.cbm-chat-question pre')?.textContent).toBe(selection.text);
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
        expect(runtime.dispose).toHaveBeenCalledOnce(); expect(container.querySelector('.cbm-chat-status')?.textContent).toBe('Off');
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
        await act(async () => { const select = container.querySelector('select')!; select.value = BROWSER_MODELS[1].id; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(runtime.dispose).toHaveBeenCalledOnce(); expect(props.createRuntime).toHaveBeenCalledOnce();
        expect(container.textContent).toContain('Adds the two values.'); expect(button('Send ↑').disabled).toBe(true);
        expect(container.querySelector('select')?.value).toBe(BROWSER_MODELS[1].id);
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
        expect(container.querySelector('.cbm-chat-status')?.textContent).toBe('Off');
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
        expect(container.querySelector('.cbm-chat-turn:last-child .cbm-chat-question pre')?.textContent).toBe('entry → sum');
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
        expect(container.querySelector('.cbm-chat-question .cbm-chat-attachment pre')?.textContent).toBe(attachedCode.text);
        expect(container.querySelector('.cbm-chat-question .cbm-chat-attachment b')).toBeNull();
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain(prompt);
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain(attachedCode.text);
    });

    it('shows automatic source before model setup without downloading or offering removal', async () => {
        const { props } = fixture();
        const context = reader('Loaded excerpt'); context.source!.partial = 'Only lines 20–80 are loaded.';
        await render({ ...props, readerContext: context, attachment: { ...selection, text: 'IGNORED_MANUAL_CODE' } });
        expect(props.createRuntime).not.toHaveBeenCalled();
        expect(container.querySelector('[aria-label="Source for next message"]')?.textContent).toContain('Current file');
        expect(container.querySelector('.cbm-chat-source-partial')?.textContent).toBe(context.source!.partial);
        expect(container.querySelector('pre')?.textContent).toBe('Loaded excerpt');
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
        expect(container.querySelectorAll('.cbm-chat-question .cbm-chat-attachment')).toHaveLength(3);
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
        expect(container.querySelector('.cbm-chat-question pre')?.textContent).toBe('ORIGINAL_LITERAL\r\n\t  ');
        expect(container.querySelector('.cbm-chat-reader-source pre')?.textContent).toBe('NEW_CURRENT_FILE');
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
        expect(container.querySelector('.cbm-chat-reader-source pre')).toBeNull();
        expect(container.textContent).toContain('Loading current file');
        await render({ ...props, attachment: selection, readerContext: { project: 'sample', path: 'new.ts', status: 'unavailable' } });
        await click('Send ↑');
        expect(runtime.chat.mock.calls[0][0][0].content).toContain('"status":"unavailable"');
        expect(runtime.chat.mock.calls[0][0].map(message => message.content).join('\n')).not.toContain(selection.text);
        expect(runtime.chat.mock.calls[0][0].map(message => message.content).join('\n')).not.toMatch(/OLD_CODE|STALE_CODE/);
        await render({ ...props, readerContext: { project: 'sample', status: 'empty' } });
        expect(container.querySelector('.cbm-chat-reader-source')?.textContent).toContain('Open a file');
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

    it('rejects oversized automatic source without shortening it or clearing the question', async () => {
        const { props, runtime } = fixture(); runtime.countTokens.mockResolvedValueOnce(99_999);
        const text = 'x'.repeat(30_000) + '\r\n\tlast  ';
        await render({ ...props, readerContext: reader(text) }); await click('Download & load'); await type('Explain exactly'); await click('Send ↑');
        expect(runtime.countTokens.mock.calls[0][0][0].content).toContain(text);
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(container.querySelector('textarea')?.value).toBe('Explain exactly');
        expect(container.querySelector('.cbm-chat-reader-source pre')?.textContent).toBe(text);
        expect(container.textContent).toContain('Nothing was sent or shortened');
    });
});
