// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import BrowserChatDock, { type BrowserChatAttachment, type BrowserChatDockProps, type BrowserChatReaderContext } from './BrowserChatDock';
import type { BrowserAiProgress, BrowserChatMessage } from './browser-ai-runtime';
import type { BrowserChatOptions } from './browser-ai-controller';
import { BROWSER_MODELS } from './model-policy';
import { JSONB_AGG_CALLERS, jsonbAggEvidence, jsonbAggScope } from './galaxy-evidence.fixture';
import { AGENT_PREFERENCES_KEY } from './agent-preferences';
import { djangoAreaEvidence } from './architecture-evidence.fixture';

let container: HTMLDivElement;
let root: Root;
let renderedProps: BrowserChatDockProps;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    // Agent preferences persist in this browser; every test starts from the defaults.
    window.localStorage.clear();
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); });

const selection: BrowserChatAttachment = { id: 'selection-1', text: '\t a +\r\n b  ', path: 'src/sum.ts', project: 'sample', startLine: 3, startColumn: 7, endLine: 4, endColumn: 5, sourceVersion: 'sha256:123' };
const reader = (text: string, kind: 'file' | 'selection' = 'file', path = 'src/sum.ts'): BrowserChatReaderContext => ({ project: 'sample', path, status: 'ready', source: { ...selection, text, path, kind, id: `reader-${path}-${kind}` } });
function fixture() {
    const runtime = {
        prepare: vi.fn(async (_progress: (value: BrowserAiProgress) => void, _options?: { cacheOnly?: boolean }) => {}),
        explain: vi.fn(async () => 'Legacy'),
        countTokens: vi.fn(async (_messages: readonly BrowserChatMessage[]) => 100),
        chat: vi.fn(async (_messages: readonly BrowserChatMessage[], _onToken: (chunk: string) => void, _options?: BrowserChatOptions) => 'Adds the two values.'),
        stop: vi.fn(), dispose: vi.fn(),
    };
    // The selected symbol's source, as get_code_snippet returns it for JSONBAgg (K14).
    const readSource = vi.fn(async () => ({ source: 'class JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"\n', file_path: 'django/contrib/postgres/aggregates/general.py',
        start_line: 50, end_line: 51, source_mode: 'full' }));
    const props = { proactive: false, open: true, onClose: vi.fn(), onAttachmentConsumed: vi.fn(), onAttachmentRemoved: vi.fn(), createRuntime: vi.fn(() => runtime), removeCache: vi.fn(async () => {}), readSource };
    return { runtime, props };
}
async function render(props: BrowserChatDockProps): Promise<void> { renderedProps = props; await act(async () => root.render(<BrowserChatDock {...props} />)); }
function button(label: string): HTMLButtonElement {
    const target = [...document.body.querySelectorAll('button')].find(item => item.textContent === label || item.getAttribute('aria-label') === label);
    expect(target, `button ${label}`).toBeDefined(); return target!;
}
async function click(label: string): Promise<void> {
    if (['Download & load', 'Load model (cached, no download)', 'Reload model'].includes(label) && !document.querySelector('#cbm-chat-model')) await models();
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
        await render({ ...props, attachment: selection, onAgentModelChange });
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
        await render({ ...props, attachment: selection, showCollapsed: true, onOpen });
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
        const { props, runtime } = fixture(); await render({ ...props, attachment: selection }); await click('Download & load');
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
        await render({ ...props, attachment: original }); await click('Download & load'); await type('How is this part computed?'); await click('Send ↑');
        const sent = runtime.chat.mock.calls[0][0].at(-1)!;
        expect(attachmentData(sent)).toMatchObject({ path: selection.path, sourceVersion: selection.sourceVersion });
        expect(sent.content.includes(selection.text)).toBe(true);
        expect(props.onAttachmentConsumed).toHaveBeenCalledExactlyOnceWith(selection.id);
        original.text = 'Different code';
        await render(props); await type('Why?'); await click('Send ↑');
        const second = runtime.chat.mock.calls[1][0];
        expect(second.at(-1)?.content).toBe('Answer in English.\n\nWhy?');
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
        await render({ ...props, attachment: selection }); await click('Download & load'); await type('How is it computed?'); await click('Send ↑');
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
        await render({ ...props, attachment: selection }); await click('Download & load'); await type('How is it computed?'); await click('Send ↑');
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
        const { props, runtime } = fixture(); await render({ ...props, attachment: selection }); await click('Download & load'); await type('Explain'); await click('Send ↑');
        await models(); await click('Unload model');
        expect(runtime.dispose).toHaveBeenCalledOnce(); expect(props.removeCache).not.toHaveBeenCalled();
        expect(container.textContent).toContain('Adds the two values.');
        await click('Delete cached model');
        expect(props.removeCache).toHaveBeenCalledExactlyOnceWith(BROWSER_MODELS[0].id);
        expect(container.textContent).toContain('Adds the two values.');
    });

    it('changes models without automatic download or losing conversation', async () => {
        const { props, runtime } = fixture(); await render({ ...props, attachment: selection }); await click('Download & load'); await type('Explain'); await click('Send ↑'); await models();
        await act(async () => { const select = document.querySelector('#cbm-chat-model') as HTMLSelectElement; select.value = BROWSER_MODELS[1].id; select.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(runtime.dispose).toHaveBeenCalledOnce(); expect(props.createRuntime).toHaveBeenCalledOnce();
        expect(container.textContent).toContain('Adds the two values.'); expect(button('Send ↑').disabled).toBe(true);
        expect((document.querySelector('#cbm-chat-model') as HTMLSelectElement)?.value).toBe(BROWSER_MODELS[1].id);
    });

    it('does not move scroll position while the reader is inspecting earlier output', async () => {
        const { props, runtime } = fixture(); const answer = deferred<string>(); let stream!: (chunk: string) => void;
        runtime.chat.mockImplementationOnce((_messages, onToken) => { stream = onToken; return answer.promise; });
        await render({ ...props, attachment: selection }); await click('Download & load'); await type('How is it computed?'); await click('Send ↑');
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
        const { props } = fixture(); await render({ ...props, attachment: selection }); await click('Download & load'); await type('Explain'); await click('Send ↑');
        const confirm = vi.spyOn(window, 'confirm').mockReturnValueOnce(false).mockReturnValueOnce(true);
        await click('New conversation'); expect(container.textContent).toContain('Adds the two values.');
        await click('New conversation'); expect(container.textContent).not.toContain('Adds the two values.'); expect(confirm).toHaveBeenCalledTimes(2);
    });

    it('unloads an active worker and ignores stale output while retaining its sent question', async () => {
        const { props, runtime } = fixture(); const answer = deferred<string>(); let stream!: (chunk: string) => void;
        runtime.chat.mockImplementationOnce((_messages, onToken) => { stream = onToken; return answer.promise; });
        await render({ ...props, attachment: selection }); await click('Download & load'); await type('How is it computed?'); await click('Send ↑'); await models();
        await act(async () => stream('Partial answer')); await click('Unload model');
        await act(async () => { stream('Ignored stale chunk'); answer.resolve('Ignored stale final'); });
        expect(runtime.dispose).toHaveBeenCalledOnce(); expect(container.textContent).toContain('Partial answer');
        expect(container.textContent).not.toContain('Ignored stale'); expect(container.textContent).toContain('Stopped · partial answer');
        expect(document.querySelector('.cbm-chat-status')?.textContent).toBe('Off');
    });

    it('includes graph evidence only after explicit choice, freezes it, and clears it after send', async () => {
        const { props, runtime } = fixture(); const evidence = { id: 'callers-1', label: 'Known callers', text: 'entry → sum' };
        await render({ ...props, context: [evidence] }); await click('Download & load'); await type('First question'); await click('Send ↑');
        // Offered but not chosen: the question has no context and the model is not asked.
        expect(runtime.chat).not.toHaveBeenCalled();
        await act(async () => (container.querySelector('input[type=checkbox]') as HTMLInputElement).click());
        evidence.text = 'changed graph';
        await render({ ...props, context: [] });
        expect(container.querySelector('.cbm-chat-pending pre')?.textContent).toBe('entry → sum');
        await type('Second question'); await click('Send ↑');
        expect(runtime.chat.mock.calls[0][0].at(-1)?.content).toContain('entry → sum');
        expect(runtime.chat.mock.calls[0][0].at(-1)?.content).not.toContain('changed graph');
        expect(container.querySelector('.cbm-chat-pending')).toBeNull();
        expect(container.querySelector('.cbm-chat-turn:last-child .cbm-chat-source-content pre')?.textContent).toBe('entry → sum');
        await click('Ask again'); expect(runtime.chat.mock.calls[1][0]).toEqual(runtime.chat.mock.calls[0][0]);
    });

    it('sends controlled graph selection once with literal code and retains it for retry', async () => {
        const { props, runtime } = fixture(); const pendingContext = { id: 'galaxy-1', label: 'Graph node main', text: '{"node":"main","generation":"unavailable"}' };
        const callbacks = { onContextConsumed: vi.fn(), onContextRemoved: vi.fn() };
        await render({ ...props, ...callbacks, attachment: selection, pendingContext }); await click('Download & load'); await type('Explain both'); await click('Send ↑');
        const first = runtime.chat.mock.calls[0][0].at(-1)!.content;
        expect(first).toContain(selection.text); expect(first).toContain(pendingContext.text); expect(callbacks.onContextConsumed).toHaveBeenCalledExactlyOnceWith('galaxy-1');
        expect(container.querySelector('[aria-label="Remove graph selection"]')).toBeNull();
        await click('Ask again'); expect(runtime.chat.mock.calls[1][0]).toEqual(runtime.chat.mock.calls[0][0]);
        await render({ ...props, ...callbacks, pendingContext }); await type('Follow up'); await click('Send ↑');
        expect(runtime.chat.mock.calls[2][0].at(-1)!.content).toBe('Answer in English.\n\nFollow up'); expect(callbacks.onContextConsumed).toHaveBeenCalledOnce();
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
        await click('Download & load'); await type('Question'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(container.querySelector('.cbm-chat-answer-text')?.textContent).toContain('Nothing is selected for me to explain.');
    });

    it('keeps a newer graph selection when an earlier send finishes checking context', async () => {
        const { props, runtime } = fixture(); const count = deferred<number>(); runtime.countTokens.mockReturnValueOnce(count.promise);
        const oldContext = { id: 'old', label: 'Old selection', text: 'Old graph facts' }; const newContext = { id: 'new', label: 'New selection', text: 'New graph facts' }; const onContextConsumed = vi.fn();
        await render({ ...props, pendingContext: oldContext, onContextConsumed }); await click('Download & load'); await type('What does the old selection show?'); await click('Send ↑');
        await render({ ...props, pendingContext: newContext, onContextConsumed }); await act(async () => count.resolve(100));
        expect(onContextConsumed).toHaveBeenCalledExactlyOnceWith('old');
        expect(container.querySelector('.cbm-chat-pending pre')?.textContent).toBe('New graph facts');
        await type('And the new one?'); await click('Send ↑');
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
        const context = reader('Loaded excerpt'); context.source!.partial = 'Only lines 20-80 are loaded.';
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
        await click('Download & load'); await type('Which values does the file add?'); await click('Send ↑');
        await render({ ...props, readerContext: reader(selection.text, 'selection') });
        await type('Which values does the marked code add?'); await click('Send ↑');
        await render({ ...props, readerContext: reader('WHOLE_SECOND_FILE', 'file', 'src/other.ts') });
        await type('Which values does this other file add?'); await click('Send ↑');
        const requests = runtime.chat.mock.calls.map(call => call[0]);
        expect(requests[0][0].content).toContain('WHOLE_FIRST_FILE');
        expect(requests[1][0].content).toContain(selection.text);
        expect(requests[1].map(message => message.content).join('\n')).not.toContain('WHOLE_FIRST_FILE');
        expect(requests[2][0].content).toContain('WHOLE_SECOND_FILE');
        expect(requests[2].map(message => message.content).join('\n')).not.toContain('WHOLE_FIRST_FILE');
        expect(requests[2].map(message => message.content).join('\n')).not.toContain(selection.text);
        // The marked code is in the same file and keeps the conversation; another file starts a new topic (K17).
        expect(requests[1].filter(message => message.role === 'user').map(message => message.content)).toEqual(['Which values does the file add?', 'Answer in English.\n\nWhich values does the marked code add?']);
        expect(requests[2].filter(message => message.role === 'user').map(message => message.content)).toEqual(['Answer in English.\n\nWhich values does this other file add?']);
        expect(props.onAttachmentConsumed).not.toHaveBeenCalled();
        expect(container.querySelectorAll('.cbm-chat-answer .cbm-chat-attachment')).toHaveLength(3);
        await render(props); await type('Ask from another workspace'); await click('Send ↑');
        expect(runtime.chat).toHaveBeenCalledTimes(3);
        expect([...container.querySelectorAll('.cbm-chat-answer-text')].at(-1)?.textContent).not.toMatch(/WHOLE_FIRST_FILE|WHOLE_SECOND_FILE/);
    });

    it('uses the same frozen reader snapshot for counting and generation despite navigation or prop mutation', async () => {
        const { props, runtime } = fixture(); const count = deferred<number>();
        runtime.countTokens.mockReturnValueOnce(count.promise);
        const original = reader('ORIGINAL_LITERAL\r\n\t  ', 'selection');
        await render({ ...props, readerContext: original }); await click('Download & load'); await type('How is it computed?'); await click('Send ↑');
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
        await render({ ...props, readerContext: reader('ORIGINAL_RETRY_SOURCE') }); await click('Download & load'); await type('How is it computed?'); await click('Send ↑');
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
        // No current source and no stale fallback: the model is not asked at all.
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(container.querySelector('.cbm-chat-answer-text')?.textContent).toContain('The source of new.ts is not available.');
        expect(container.querySelector('.cbm-chat-transcript')?.textContent).not.toMatch(/OLD_CODE|STALE_CODE/);
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
        expect(question.at(-1)?.content).toBe('Answer in English.\n\nExplain this exact code');
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
        await type('Which values does it add?'); await click('Send ↑');
        expect(runtime.stop).toHaveBeenCalledOnce();
        expect(runtime.countTokens).toHaveBeenCalledOnce();
        expect(runtime.chat).not.toHaveBeenCalled();
        await act(async () => counting.resolve(100));
        expect(runtime.countTokens).toHaveBeenCalledTimes(2);
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[0][0].at(-1)?.content).toBe('Answer in English.\n\nWhich values does it add?');
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
        await click('Load model (cached, no download)'); await click('Send ↑');
        expect(runtime.chat).toHaveBeenCalledTimes(2);
        expect(runtime.chat.mock.calls[1][0].at(-1)?.content).toBe('Answer in English.\n\nDo not send after unload');
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
        const original = reader('ORIGINAL_SOURCE'); original.source!.partial = 'Only lines 3-4 were loaded.';
        const pendingContext = { id: 'graph-source', label: 'Callers', text: 'entry calls original' };
        await render({ ...props, readerContext: original, pendingContext }); await click('Download & load');
        const textarea = container.querySelector('textarea')!;
        expect(textarea.rows).toBe(1);
        expect(textarea.parentElement?.querySelector('[aria-label="Send ↑"]')).not.toBeNull();
        expect(container.querySelector('.cbm-chat-new')).toBeNull();
        expect(container.querySelector('.cbm-chat-reader-source')).toBeNull();
        expect(container.querySelector('[aria-label="Remove graph selection"]')).not.toBeNull();
        await type('Explain the source'); await click('Send ↑');
        const response = container.querySelector('.cbm-chat-answer')!;
        const source = response.querySelector('.cbm-chat-response-source') as HTMLDetailsElement;
        expect(response.firstElementChild).toBe(source);
        expect(source.open).toBe(false);
        expect(source.querySelector('summary')?.textContent).toContain('Agentⓘ Source');
        expect(source.textContent).toContain('Only lines 3-4 were loaded.');
        expect(source.textContent).toContain('entry calls original');
        await act(async () => source.querySelector('summary')!.click());
        expect(source.open).toBe(true);
        await render({ ...renderedProps, readerContext: reader('NEW_SOURCE', 'file', 'new.ts') });
        expect(source.querySelector('pre')?.textContent).toBe('ORIGINAL_SOURCE');
        expect(source.textContent).not.toContain('NEW_SOURCE');
        expect(container.querySelector('.cbm-chat-composer pre')).toBeNull();
        expect(container.querySelector('.cbm-chat-header .cbm-chat-new')).not.toBeNull();
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
        await render({ ...props, attachment: selection, onAgentStateChange }); await click('Download & load');
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


describe('graph answers and answer limits', () => {
    it('answers a caller question from the loaded graph completely, without the model, and keeps it in history', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await click('Download & load');
        await type('Who calls JSONBAgg? List every caller and the edge type.'); await click('Send ↑');
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        const answer = container.querySelector('.cbm-chat-answer-text')!.textContent!;
        for (const name of JSONB_AGG_CALLERS) expect(answer).toContain(name);
        expect(answer).toContain('CALLS (11):');
        expect(answer).toContain('Other incoming relationships:');
        expect(answer).toContain('TESTS (11):');
        expect(answer).toContain('DEFINES (1): general.py, the file that defines JSONBAgg');
        expect(answer).toContain('not generated by the model');
        expect(container.querySelector('.cbm-chat-answer .cbm-chat-source-content')?.textContent).toContain('Incoming: 23 relationships from 12 symbols.');
        expect([...container.querySelectorAll('button')].some(item => item.textContent === 'Retry')).toBe(false);
        expect(button('Ask the model')).toBeDefined();
        expect(container.querySelector('textarea')?.value).toBe('');
        await type('Which of them are tests?'); await click('Send ↑');
        const request = runtime.chat.mock.calls[0][0];
        expect(request.map(message => message.role)).toEqual(['system', 'user', 'assistant', 'user']);
        expect(request[2].content).toContain('test_values_list');
    });

    it('lists callers for a typo question and offers an uncertain one as a suggestion, without the model (K16)', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await click('Download & load');
        const last = () => [...container.querySelectorAll('.cbm-chat-turn')].at(-1)!;
        await type('wer ruf jsonbagg auf'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(last().querySelector('.cbm-chat-answer-text')?.textContent).toContain('11 Aufrufer (CALLS) von JSONBAgg im geladenen Graphen.');
        expect([...last().querySelectorAll('button.cbm-chat-retry')].map(item => item.textContent)).toEqual(['Modell fragen']);
        await type('jsonbagg aufrufe?'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(last().querySelector('.cbm-chat-answer-text')?.textContent).toContain('Meintest du: Aufrufer von JSONBAgg?');
        // A German suggestion offers its choices in German too.
        expect([...last().querySelectorAll('button.cbm-chat-retry')].map(item => item.textContent)).toEqual(['Liste anzeigen', 'Modell fragen']);
        await click('Liste anzeigen');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(last().querySelector('.cbm-chat-answer-text')?.textContent).toContain('11 Aufrufer (CALLS) von JSONBAgg im geladenen Graphen.');
        for (const name of JSONB_AGG_CALLERS) expect(last().textContent).toContain(name);
        await type('jsonbagg calls'); await click('Send ↑');
        expect(last().querySelector('.cbm-chat-answer-text')?.textContent).toContain('Did you mean: what JSONBAgg calls?');
        expect([...last().querySelectorAll('button.cbm-chat-retry')].map(item => item.textContent)).toEqual(['Show the list', 'Ask the model']);
        await click('Ask the model');
        expect(runtime.chat).toHaveBeenCalledOnce();
        const request = runtime.chat.mock.calls[0][0];
        expect(request.at(-1)!.content).toContain('jsonbagg calls');
        expect(request.some(message => message.content.includes('Meintest du'))).toBe(false);
    });

    it('does not ask the model without any context and says what to select, in the language of the question (K11)', async () => {
        const { props, runtime } = fixture();
        await render(props); await click('Download & load');
        const last = () => [...container.querySelectorAll('.cbm-chat-turn')].at(-1)!;
        await type('was kansnt du mir über den code sagen'); await click('Send ↑');
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(last().querySelector('.cbm-chat-answer-text')?.textContent).toContain('Wähle einen Knoten in Galaxy oder einen Teil in Architecture, oder öffne eine Datei in Explore');
        expect(last().textContent).toContain('Ohne das Modell beantwortet');
        expect([...last().querySelectorAll('button')].some(item => item.textContent === 'Retry')).toBe(false);
        await type('test'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(last().querySelector('.cbm-chat-answer-text')?.textContent).toContain('Select a node in Galaxy or a part in Architecture, or open a file in Explore');
        await render({ ...props, readerContext: { project: 'sample', status: 'empty' } });
        await type('what is in this file?'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(last().querySelector('.cbm-chat-answer-text')?.textContent).toContain('No file is open in Explore.');
        await render({ ...props, proactiveSelection: jsonbAggEvidence() });
        await type('What does this class do?'); await click('Send ↑');
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[0][0].some(message => /Wähle einen Knoten|Select a node in Galaxy/.test(message.content))).toBe(false);
    });

    it('asks the model to answer in the language of the question (C4)', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await click('Download & load');
        await type('Erklär diese Klasse ausführlich, Zeile für Zeile'); await click('Send ↑');
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain('Answer in German.\nUser question:\nErklär diese Klasse ausführlich, Zeile für Zeile');
        await type('Explain this class in detail, line by line.'); await click('Send ↑');
        expect(runtime.chat.mock.calls[1][0].at(-1)!.content).toContain('Answer in English.\nUser question:\nExplain this class in detail, line by line.');
        await render({ ...props, readerContext: reader('name: New contributor message', 'file', '.github/workflows/new_contributor_pr.yml') });
        await type('Wie viele Jobs gibt es?'); await click('Send ↑');
        expect(runtime.chat.mock.calls[2][0].at(-1)!.content).toBe('Answer in German.\n\nWie viele Jobs gibt es?');
    });

    it('does not resend earlier answers about another file or selection and marks the new topic (K17)', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('This runs the flake8 linter on Python files.');
        const workflow = reader('name: New contributor message', 'file', '.github/workflows/new_contributor_pr.yml');
        await render({ ...props, selectionScope: 'django-demo:explore', readerContext: workflow }); await click('Download & load');
        await type('welche Aktion nutzt dieses File?'); await click('Send ↑');
        expect(container.querySelector('.cbm-chat-header')?.textContent).toContain('New conversation');
        await render({ ...props, selectionScope: 'django-demo:galaxy', proactiveSelection: jsonbAggEvidence() });
        await type('What does JSONBAgg do?'); await click('Send ↑');
        const request = runtime.chat.mock.calls[1][0];
        const text = request.map(message => message.content).join('\n');
        expect(text).not.toContain('flake8');
        expect(text).not.toContain('welche Aktion');
        expect(request.filter(message => message.role === 'assistant')).toHaveLength(0);
        expect(container.querySelector('.cbm-chat-topic-break')?.textContent).toBe('New topic: JSONBAgg. Earlier messages are not sent with these questions.');
        // A follow-up on the same selection keeps its own history.
        await type('Which tests use it?'); await click('Send ↑');
        expect(runtime.chat.mock.calls[2][0].filter(message => message.role === 'user').map(message => message.content.split('\n').at(-1)))
            .toEqual(['What does JSONBAgg do?', 'Which tests use it?']);
        expect(runtime.chat.mock.calls[2][0].map(message => message.content).join('\n')).not.toContain('flake8');
    });

    it('carries the earlier turns of a file or selection the reader comes back to, as the divider says (K17, B4)', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('This runs the flake8 linter on Python files.');
        const workflow = reader('name: New contributor message', 'file', '.github/workflows/new_contributor_pr.yml');
        await render({ ...props, selectionScope: 'django-demo:explore', readerContext: workflow }); await click('Download & load');
        await type('welche Aktion nutzt dieses File?'); await click('Send ↑');
        await render({ ...props, selectionScope: 'django-demo:galaxy', proactiveSelection: jsonbAggEvidence() });
        await type('What does JSONBAgg do?'); await click('Send ↑');
        await render({ ...props, selectionScope: 'django-demo:explore', readerContext: workflow });
        await type('Und was noch?'); await click('Send ↑');
        const request = runtime.chat.mock.calls[2][0];
        // Its own earlier turn comes along; nothing of JSONBAgg does.
        expect(request.filter(message => message.role === 'assistant').map(message => message.content)).toEqual(['This runs the flake8 linter on Python files.']);
        expect(request.map(message => message.content).join('\n')).not.toContain('JSONBAgg');
        expect(request.filter(message => message.role === 'user').map(message => message.content.split('\n').at(-1))).toEqual(['welche Aktion nutzt dieses File?', 'Und was noch?']);
        expect([...container.querySelectorAll('.cbm-chat-topic-break')].map(item => item.textContent)).toEqual([
            'New topic: JSONBAgg. Earlier messages are not sent with these questions.',
            'Zurück zu: .github/workflows/new_contributor_pr.yml. Die früheren Nachrichten dazu werden wieder mitgeschickt.']);
        // Within the returned topic the conversation goes on.
        await type('Welche Jobs?'); await click('Send ↑');
        expect(runtime.chat.mock.calls[3][0].filter(message => message.role === 'user').map(message => message.content.split('\n').at(-1)))
            .toEqual(['welche Aktion nutzt dieses File?', 'Und was noch?', 'Welche Jobs?']);
    });

    it('marks names in an answer that are not in the file or the graph facts (K12)', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('Dieses Script ruft `flake8` mit `subprocess.run` auf und prüft `pull_request_target`.');
        const workflow = reader('name: New contributor message\n\non:\n  pull_request_target:\n    types: [opened]', 'file', '.github/workflows/new_contributor_pr.yml');
        await render({ ...props, readerContext: workflow }); await click('Download & load');
        await type('welche Trigger nutzt dieses aktuelle File?'); await click('Send ↑');
        const note = [...container.querySelectorAll('.cbm-chat-turn .cbm-chat-answer-note')].map(item => item.textContent).find(text => /Not in the source/.test(text ?? ''));
        expect(note).toBe('Not in the source or graph facts this answer was given: flake8, subprocess.run. Check these names before relying on them.');
    });

    it('hands a listed answer to the model on request, with its evidence and without the list as history', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await click('Download & load');
        await type('Who calls JSONBAgg?'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled();
        await click('Ask the model');
        expect(runtime.chat).toHaveBeenCalledOnce();
        const request = runtime.chat.mock.calls[0][0];
        expect(request.map(message => message.role)).toEqual(['system', 'user']);
        expect(request[1].content).toContain('Who calls JSONBAgg?');
        expect(request[1].content).toContain('Incoming: 23 relationships from 12 symbols.');
        // The listed answer stays; the model's answer is a turn of its own below it (B1).
        expect(container.querySelectorAll('.cbm-chat-turn')).toHaveLength(2);
        expect(container.querySelector('.cbm-chat-answer-text')?.textContent).toContain('not generated by the model');
        expect([...container.querySelectorAll('.cbm-chat-answer-text')].at(-1)?.textContent).toBe('Adds the two values.');
        expect(button('Ask again')).toBeDefined();
    });

    // 40 callers with long names in their own files: the snapshot names 24, a default prompt fewer.
    const longCallers = () => {
        const callers = Array.from({ length: 40 }, (_, index) => ({ id: 2000 + index, name: `caller_with_a_long_descriptive_name_number_${index}`, label: 'Function',
            file_path: `tests/postgres_tests/callers/test_module_number_${index}.py`, x: 0, y: 0, z: 0, size: 1, color: '#999' }));
        return jsonbAggEvidence({ edges: callers.map(caller => ({ source: caller.id, target: 32360, type: 'CALLS' })), nodes: callers });
    };
    const capacityNote = () => [...[...container.querySelectorAll('.cbm-chat-turn')].at(-1)?.querySelectorAll('.cbm-chat-answer-note') ?? []]
        .map(item => item.textContent ?? '').find(note => note.includes('too large'));

    it('marks an answer cut at the output token limit and shows the capacity of an oversized scope', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockImplementationOnce(async (_messages, _onToken, options) => {
            options?.onComplete?.({ stopReason: 'length' }); return 'A long list that';
        });
        await render({ ...props, proactiveSelection: longCallers() }); await click('Download & load');
        await type('Explain this class in detail'); await click('Send ↑');
        const notes = [...container.querySelectorAll('.cbm-chat-answer-note')].map(item => item.textContent);
        expect(container.querySelector('.cbm-chat-limit-note > summary')?.textContent).toBe('Token limit reached: the answer was cut short');
        const capacity = notes.map(note => /^55 nodes \/ 40 edges: too large for the local Qwen2\.5 Coder 0\.5B model; showing (\d+)$/.exec(note ?? '')).find(Boolean);
        expect(Number(capacity?.[1])).toBeGreaterThan(0);
        expect(Number(capacity?.[1])).toBeLessThan(24);
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toMatch(/\+\d+ more/);
    });

    it('explains a cut answer: current limits, a way to the output limit and larger models with their download (K1)', async () => {
        window.localStorage.setItem(AGENT_PREFERENCES_KEY, JSON.stringify({ version: 1, preferences: { modelId: BROWSER_MODELS[0].id, automatic: true,
            limits: { [BROWSER_MODELS[0].id]: { inputTokens: 2048, outputTokens: 256 } } } }));
        const { props, runtime } = fixture();
        runtime.chat.mockImplementationOnce(async (_messages, _onToken, options) => {
            options?.onComplete?.({ stopReason: 'length' }); return 'A long answer that';
        });
        await render({ ...props, attachment: selection }); await click('Download & load');
        await type('How is this part computed?'); await click('Send ↑');
        const note = container.querySelector<HTMLDetailsElement>('.cbm-chat-turn details.cbm-chat-limit-note');
        expect(note?.querySelector('summary')?.textContent).toBe('Token limit reached: the answer was cut short');
        expect(note?.textContent).toContain('all 256 output tokens');
        expect(note?.textContent).toContain('2,048 tokens');
        expect(note?.textContent).toContain('up to 512 tokens');
        for (const [name, size] of [['Qwen3 0.6B', '579 MB'], ['LFM2.5 1.2B', '764 MB'], ['Qwen3.5 2B', '1.40 GB']]) expect(note?.textContent).toContain(`${name} · ${size} download`);
        expect(note?.textContent).not.toContain(`${BROWSER_MODELS[0].displayName} ·`);
        expect(document.querySelector('dialog')).toBeNull();
        await act(async () => button('Change the output limit').click());
        expect(document.querySelector('dialog')).not.toBeNull();
        expect(document.activeElement?.id).toBe('cbm-chat-output-tokens');
    });

    it('does not offer to change an output limit that is already at its maximum, nor larger models with the same limit (C8, W6)', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockImplementationOnce(async (_messages, _onToken, options) => {
            options?.onComplete?.({ stopReason: 'length' }); return 'A long answer that';
        });
        await render({ ...props, attachment: selection }); await click('Download & load');
        await type('How is this part computed?'); await click('Send ↑');
        const note = container.querySelector<HTMLDetailsElement>('.cbm-chat-turn details.cbm-chat-limit-note');
        expect(note?.textContent).toContain('The output limit is at its maximum of 512 tokens.');
        expect([...note?.querySelectorAll('button') ?? []].map(item => item.textContent)).not.toContain('Change the output limit');
        // Every larger model stops at 512 output tokens as well, so none is offered for a longer answer.
        expect(note?.textContent).toContain('Larger models have the same limit.');
        expect(note?.textContent).not.toContain('Qwen3 0.6B · 579 MB download');
    });

    it('says why an automatic explanation stops early and that a question may answer longer (K1)', async () => {
        vi.useFakeTimers();
        try {
            const { props, runtime } = fixture();
            runtime.chat.mockImplementationOnce(async (_messages, _onToken, options) => {
                options?.onComplete?.({ stopReason: 'length' }); return 'Defines the aggregate and lists the tests that';
            });
            await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() }); await click('Download & load');
            await act(async () => { await vi.advanceTimersByTimeAsync(650); });
            const note = container.querySelector<HTMLDetailsElement>('.cbm-chat-explanation details.cbm-chat-limit-note');
            expect(note?.querySelector('summary')?.textContent).toBe('Token limit reached: the answer was cut short');
            expect(note?.textContent).toContain('Automatic explanations stop after 128 output tokens');
            // Both limits it ran into, as the note of a question names them: the input limit too.
            expect(note?.textContent).toContain('read at most 1,536 input tokens');
            expect(note?.textContent).toContain('up to 512 output tokens');
            expect(note?.textContent).toContain('2,048 input tokens');
        } finally { vi.useRealTimers(); }
    });

    it('does not blame the model when only the snapshot bounds the names', async () => {
        const { props, runtime } = fixture();
        const many = Array.from({ length: 40 }, (_, index) => ({ source: 2000 + index, target: 32360, type: 'CALLS' }));
        await render({ ...props, proactiveSelection: jsonbAggEvidence({ edges: many }) }); await click('Download & load');
        await type('Explain this class in detail'); await click('Send ↑');
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain('+16 more');
        expect(capacityNote()).toBeUndefined();
    });

    it('gives a manual question more evidence when the input limit is raised', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, proactiveSelection: longCallers() }); await click('Download & load');
        await type('Explain this class in detail'); await click('Send ↑');
        const shown = (note?: string) => Number(/showing (\d+)$/.exec(note ?? '')?.[1] ?? 24);
        const before = shown(capacityNote());
        await models();
        const input = document.querySelector<HTMLInputElement>('#cbm-chat-input-tokens')!;
        await act(async () => {
            Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, '6144');
            input.dispatchEvent(new Event('input', { bubbles: true }));
        });
        await act(async () => input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })));
        await type('Explain this class in detail'); await click('Send ↑');
        expect(runtime.chat.mock.calls[1][0].at(-1)!.content.length).toBeGreaterThan(runtime.chat.mock.calls[0][0].at(-1)!.content.length);
        expect(before).toBeLessThan(24);
        expect(capacityNote()).toBeUndefined();
    });

    it('releases the loaded worker when another tab chooses another model', async () => {
        const { props, runtime } = fixture(); await render(props); await click('Download & load');
        const raw = JSON.stringify({ version: 1, preferences: { modelId: BROWSER_MODELS[1].id, automatic: true, limits: {} } });
        await act(async () => {
            window.localStorage.setItem(AGENT_PREFERENCES_KEY, raw);
            window.dispatchEvent(new StorageEvent('storage', { key: AGENT_PREFERENCES_KEY }));
        });
        expect(runtime.dispose).toHaveBeenCalledOnce();
        await models();
        const select = document.querySelector<HTMLSelectElement>('#cbm-chat-model')!;
        expect(select.value).toBe(BROWSER_MODELS[1].id);
        expect([...select.options].some(option => option.textContent?.endsWith('Loaded'))).toBe(false);
        expect(button('Download & load')).toBeDefined();
    });

    it('leaves the oldest history out when the input limit is exceeded and says so', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, readerContext: reader('const a = 1;') }); await click('Download & load');
        for (const question of ['First question', 'Second question', 'Third question']) { await type(question); await click('Send ↑'); }
        runtime.countTokens.mockImplementation(async messages => messages.length > 4 ? 5000 : 100);
        await type('Fourth question'); await click('Send ↑');
        const request = runtime.chat.mock.calls.at(-1)![0];
        expect(request.length).toBeLessThanOrEqual(4);
        expect(request.at(-1)!.content).toBe('Answer in English.\n\nFourth question');
        expect(request.map(message => message.content).join('\n')).not.toContain('First question');
        expect(container.querySelectorAll('.cbm-chat-answer-note')[0]?.textContent).toMatch(/earlier messages were left out to fit the input limit/);
    });
});


describe('per-selection explanation cache', () => {
    afterEach(() => vi.useRealTimers());
    async function settleSelection() { await act(async () => { await vi.advanceTimersByTimeAsync(650); }); }
    const card = () => container.querySelector('[aria-label="Current selection explanation"]')?.textContent ?? '';

    it('shows the cached explanation when returning to a selection without running the model again', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('JSONBAgg is called by its tests.').mockResolvedValueOnce('Two layers around JSONBAgg.');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() }); await click('Download & load');
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledOnce();
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence({ depth: 2 }) });
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledTimes(2);
        expect(card()).toContain('Two layers around JSONBAgg.');
        // Back to the first selection while its scope reloads, then once it is complete again.
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence({ state: 'loading-partial-preview' }) });
        expect(card()).toContain('JSONBAgg is called by its tests.');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() });
        await settleSelection();
        expect(runtime.chat).toHaveBeenCalledTimes(2);
        expect(runtime.countTokens).toHaveBeenCalledTimes(2);
        expect(card()).toContain('JSONBAgg is called by its tests.');
        expect(card()).not.toContain('Explaining selection');
    });

    it('starts an explanation only once the scope is complete', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        const preview = jsonbAggEvidence({ edges: jsonbAggScope().edges.slice(0, 3), state: 'loading-partial-preview' });
        await render({ ...props, proactive: true, proactiveSelection: preview }); await click('Download & load');
        await settleSelection();
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(card()).toContain('Waiting for the complete scope');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() });
        await settleSelection();
        expect(runtime.chat).toHaveBeenCalledOnce();
        // The complete scope's facts are listed in the card; the model reads the symbol's source (K7, K14).
        expect(card()).toContain('Incoming: 23 relationships from 12 symbols (CALLS 11, TESTS 11, DEFINES 1).');
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain('class JSONBAgg(OrderableAggMixin, Aggregate):');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence({ state: 'partial' }) });
        await settleSelection(); expect(runtime.chat).toHaveBeenCalledOnce();
    });

    it('explains again when a returning selection now carries other evidence, as after a re-index', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('Before the re-index.').mockResolvedValueOnce('Two layers.').mockResolvedValueOnce('After the re-index.');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() }); await click('Download & load');
        await settleSelection(); expect(card()).toContain('Before the re-index.');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence({ depth: 2 }) }); await settleSelection();
        // The same selection, loading and then complete, after a re-index removed a caller.
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence({ state: 'loading-partial-preview' }) });
        expect(card()).toContain('Before the re-index.');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence({ edges: jsonbAggScope().edges.slice(2) }) });
        expect(card()).not.toContain('Before the re-index.');
        await settleSelection();
        expect(runtime.chat).toHaveBeenCalledTimes(3);
        expect(card()).toContain('After the re-index.');
    });

    it('regenerates on request and keeps the cache bounded', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        let calls = 0; runtime.chat.mockImplementation(async () => `Explanation number ${++calls}.`);
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() }); await click('Download & load');
        await settleSelection(); expect(card()).toContain('Explanation number 1.');
        await click('Explain again'); await settleSelection();
        expect(card()).toContain('Explanation number 2.');
        for (let depth = 2; depth <= 33; depth++) {
            await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence({ depth }) }); await settleSelection();
        }
        expect(runtime.chat).toHaveBeenCalledTimes(34);
        // 32 later selections pushed the first one out of the cache.
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() }); await settleSelection();
        expect(runtime.chat).toHaveBeenCalledTimes(35);
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence({ depth: 33 }) }); await settleSelection();
        expect(runtime.chat).toHaveBeenCalledTimes(35);
    });
});


describe('agent configuration limits', () => {
    afterEach(() => vi.useRealTimers());
    async function setLimit(id: string, value: number): Promise<void> {
        const input = document.querySelector<HTMLInputElement>(`#${id}`)!;
        await act(async () => {
            Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, String(value));
            input.dispatchEvent(new Event('input', { bubbles: true }));
        });
        await act(async () => input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })));
    }
    const field = (id: string) => document.querySelector<HTMLInputElement>(`#${id}`);

    it('offers input and output limits per model within the model policy', async () => {
        const { props } = fixture(); await render(props); await models();
        const [first] = BROWSER_MODELS;
        expect(document.querySelector('.cbm-chat-token-limits legend')?.textContent).toBe(`Token limits for ${first.displayName}`);
        expect(field('cbm-chat-input-tokens')).toMatchObject({ value: '2048', min: '512', max: String(first.contextTokens - first.maxOutputTokens) });
        expect(field('cbm-chat-output-tokens')).toMatchObject({ value: String(first.maxOutputTokens), min: '32', max: String(first.maxOutputTokens) });
        // Every default and every suggested value is a valid step, so the arrows reach them.
        expect(field('cbm-chat-input-tokens')?.step).toBe('64');
        expect(field('cbm-chat-output-tokens')?.step).toBe('32');
        expect(field('cbm-chat-output-tokens')?.validity.stepMismatch).toBe(false);
        await setLimit('cbm-chat-output-tokens', 99_999);
        expect(field('cbm-chat-output-tokens')?.value).toBe(String(first.maxOutputTokens));
        await setLimit('cbm-chat-output-tokens', 128);
        expect(field('cbm-chat-input-tokens')?.max).toBe(String(first.contextTokens - 128));
    });

    it('keeps the chosen model, automatic explanations and limits across a reload', async () => {
        const { props } = fixture(); await render({ ...props, proactive: true }); await models();
        const [first, second] = BROWSER_MODELS;
        await setLimit('cbm-chat-output-tokens', 256);
        const select = document.querySelector<HTMLSelectElement>('#cbm-chat-model')!;
        await act(async () => { select.value = second.id; select.dispatchEvent(new Event('change', { bubbles: true })); });
        await setLimit('cbm-chat-input-tokens', 3072);
        const automatic = [...document.querySelectorAll('label')].find(label => label.textContent?.includes('Explain selections automatically'))!.querySelector('input')!;
        await act(async () => automatic.click());
        await act(async () => root.unmount());
        root = createRoot(container);
        await render({ ...props, proactive: true }); await models();
        expect(document.querySelector<HTMLSelectElement>('#cbm-chat-model')?.value).toBe(second.id);
        expect(field('cbm-chat-input-tokens')?.value).toBe('3072');
        expect(field('cbm-chat-output-tokens')?.value).toBe(String(second.maxOutputTokens));
        expect([...document.querySelectorAll('label')].find(label => label.textContent?.includes('Explain selections automatically'))!.querySelector('input')!.checked).toBe(false);
        const next = document.querySelector<HTMLSelectElement>('#cbm-chat-model')!;
        await act(async () => { next.value = first.id; next.dispatchEvent(new Event('change', { bubbles: true })); });
        expect(field('cbm-chat-output-tokens')?.value).toBe('256');
    });

    it('applies the limits to the manual chat budget and the output tokens it asks for', async () => {
        const { props, runtime } = fixture(); await render({ ...props, attachment: selection }); await models();
        await setLimit('cbm-chat-output-tokens', 128); await setLimit('cbm-chat-input-tokens', 1024);
        await click('Download & load');
        runtime.countTokens.mockResolvedValueOnce(1500);
        await type('How is it computed?'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(container.textContent).toMatch(/the local working limit is 1\D?024\./);
        await click('Send ↑');
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[0][2]).toMatchObject({ maxOutputTokens: 128 });
    });

    it('bounds automatic explanations by the configured output limit', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        await render({ ...props, proactive: true, readerContext: reader('const value = 1;') }); await models();
        await setLimit('cbm-chat-output-tokens', 64);
        await click('Download & load');
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        expect(runtime.chat.mock.calls[0][2]).toMatchObject({ maxOutputTokens: 64, generationProfile: 'automatic-explanation' });
    });
});

describe('grounded automatic explanations (K14, K7)', () => {
    afterEach(() => vi.useRealTimers());
    const settle = async () => { await act(async () => { await vi.advanceTimersByTimeAsync(650); }); };
    const card = () => container.querySelector('[aria-label="Current selection explanation"]')!;
    const snippet = { source: 'class JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"\n    template = "%(function)s(%(distinct)s%(expressions)s %(order_by)s)"\n    allow_distinct = True\n    output_field = JSONField()\n',
        file_path: '/abs/django/contrib/postgres/aggregates/general.py', start_line: 50, end_line: 54, source_mode: 'full' };

    it('reads the selected symbol source, sends it with the facts and shows listed facts with one model sentence', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        const readSource = vi.fn(async () => snippet);
        runtime.chat.mockResolvedValueOnce('`JSONBAgg` sets `function` to "JSONB_AGG" and allows distinct values. It is called on a list of integers.');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence(), readSource }); await click('Download & load'); await settle();
        expect(readSource).toHaveBeenCalledExactlyOnceWith('django-demo.JSONBAgg', { maxLines: 40 });
        const prompt = runtime.chat.mock.calls[0][0].map(message => message.content).join('\n');
        expect(prompt).toContain('```\nclass JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"');
        expect(prompt).toContain('Describe this class in one short sentence that starts with `JSONBAgg`.');
        expect(prompt).not.toContain('Source unavailable');
        expect(card().textContent).toContain('Incoming: 23 relationships from 12 symbols (CALLS 11, TESTS 11, DEFINES 1).');
        expect(card().textContent).toContain('JSONBAgg sets function to "JSONB_AGG" and allows distinct values.');
        expect(card().textContent).not.toContain('list of integers');
        expect(card().textContent).toContain('Facts listed from the indexed graph; the last sentence is generated by the model.');
        const disclosure = card().querySelector('.cbm-chat-source-content')!;
        expect(disclosure.textContent).not.toContain('Source unavailable');
        expect(disclosure.textContent).toContain('class JSONBAgg(OrderableAggMixin, Aggregate):');
        // A question about the selection gets the same source, read once.
        await type('What does it configure?'); await click('Send ↑');
        expect(readSource).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[1][0].at(-1)!.content).toContain('function = "JSONB_AGG"');
        // A listed answer does not claim the source is unavailable once it was read.
        await type('Who calls JSONBAgg?'); await click('Send ↑');
        const listed = [...container.querySelectorAll('.cbm-chat-turn')].at(-1)!.querySelector('.cbm-chat-source-content')!;
        expect(listed.textContent).not.toContain('Source unavailable');
        expect(listed.textContent).toContain('class JSONBAgg(OrderableAggMixin, Aggregate):');
    });

    it('reads the source for a listed answer and asks the model with it when automatic explanations are off', async () => {
        const { props, runtime } = fixture();
        const readSource = vi.fn(async () => snippet);
        await render({ ...props, proactiveSelection: jsonbAggEvidence(), readSource }); await click('Download & load');
        const last = () => [...container.querySelectorAll('.cbm-chat-turn')].at(-1)!;
        await type('Who calls JSONBAgg?'); await click('Send ↑');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(readSource).toHaveBeenCalledExactlyOnceWith('django-demo.JSONBAgg', { maxLines: 40 });
        const disclosure = last().querySelector('.cbm-chat-source-content')!;
        expect(disclosure.textContent).not.toContain('Source unavailable');
        expect(disclosure.textContent).toContain('class JSONBAgg(OrderableAggMixin, Aggregate):');
        await click('Ask the model');
        const prompt = runtime.chat.mock.calls[0][0].map(message => message.content).join('\n');
        expect(prompt).toContain('function = "JSONB_AGG"');
        expect(prompt).not.toContain('Source unavailable');
        expect(readSource).toHaveBeenCalledOnce();
    });

    it('reads the source again for a suggestion asked of the model after the first read failed', async () => {
        const { props, runtime } = fixture();
        const readSource = vi.fn(async () => snippet).mockRejectedValueOnce(new Error('offline'));
        await render({ ...props, proactiveSelection: jsonbAggEvidence(), readSource }); await click('Download & load');
        const last = () => [...container.querySelectorAll('.cbm-chat-turn')].at(-1)!;
        await type('jsonbagg calls'); await click('Send ↑');
        expect(last().querySelector('.cbm-chat-answer-text')?.textContent).toContain('Did you mean: what JSONBAgg calls?');
        expect(readSource).toHaveBeenCalledOnce();
        await click('Ask the model');
        expect(readSource).toHaveBeenCalledTimes(2);
        const prompt = runtime.chat.mock.calls[0][0].map(message => message.content).join('\n');
        expect(prompt).toContain('function = "JSONB_AGG"');
        expect(prompt).not.toContain('Source unavailable');
    });

    it('keeps the source of a suggestion that is turned into the list', async () => {
        const { props, runtime } = fixture();
        const readSource = vi.fn(async () => snippet);
        await render({ ...props, proactiveSelection: jsonbAggEvidence(), readSource }); await click('Download & load');
        const last = () => [...container.querySelectorAll('.cbm-chat-turn')].at(-1)!;
        await type('jsonbagg calls'); await click('Send ↑');
        await click('Show the list');
        expect(last().querySelector('.cbm-chat-source-content')?.textContent).toContain('class JSONBAgg(OrderableAggMixin, Aggregate):');
        await click('Ask the model');
        expect(runtime.chat.mock.calls[0][0].map(message => message.content).join('\n')).toContain('function = "JSONB_AGG"');
        expect(readSource).toHaveBeenCalledOnce();
    });

    it('shows only the listed facts when the model names what the evidence lacks', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        const readSource = vi.fn(async () => snippet);
        runtime.chat.mockResolvedValueOnce('JSONBAgg is checked by the flake8 linter.');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence(), readSource }); await click('Download & load'); await settle();
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(card().textContent).toContain('Selected: JSONBAgg (Class) in django/contrib/postgres/aggregates/general.py:50-54.');
        expect(card().textContent).not.toContain('checked by the flake8 linter');
        expect(card().textContent).toContain("The model's sentence named something that is in neither the source nor the facts (here: flake8) and was left out.");
    });

    it('does not show a prompt capacity note on a card whose facts are listed in full', async () => {
        vi.useFakeTimers(); const { props } = fixture();
        const callers = Array.from({ length: 40 }, (_, index) => ({ id: 2000 + index, name: `caller_with_a_long_descriptive_name_number_${index}`, label: 'Function',
            file_path: `tests/postgres_tests/callers/test_module_number_${index}.py`, x: 0, y: 0, z: 0, size: 1, color: '#999' }));
        const evidence = jsonbAggEvidence({ edges: callers.map(caller => ({ source: caller.id, target: 32360, type: 'CALLS' })), nodes: callers });
        await render({ ...props, proactive: true, proactiveSelection: evidence, readSource: vi.fn(async () => snippet) }); await click('Download & load'); await settle();
        expect(card().textContent).toContain('Incoming: 40 relationships from 40 symbols (CALLS 40).');
        expect(card().textContent).not.toContain('too large for the local');
    });

    it('does not ask the model at all without source: the listed facts are the explanation', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        const readSource = vi.fn(async () => { throw new Error('source unavailable'); });
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence(), readSource }); await click('Download & load'); await settle();
        expect(readSource).toHaveBeenCalledOnce();
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(card().textContent).toContain('Incoming: 23 relationships from 12 symbols (CALLS 11, TESTS 11, DEFINES 1).');
        expect(card().textContent).toContain('Listed from the indexed graph; not generated by the model.');
    });

    it('lists the jobs, triggers and actions of a workflow file without a model sentence, and asks the model only on request (K12)', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('This workflow greets new contributors on their first pull request.');
        const text = 'name: New contributor message\n\non:\n  pull_request_target:\n    types: [opened]\n\njobs:\n  build:\n    name: Hello new contributor\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/first-interaction@v1\n';
        await render({ ...props, proactive: true, selectionScope: 'django-demo:explore', readerContext: reader(text, 'file', '.github/workflows/new_contributor_pr.yml') });
        await click('Download & load'); await settle();
        // The automatic card is the file's facts: the model is not asked.
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(card().textContent).toContain('1 job: build ("Hello new contributor", runs on ubuntu-latest, 1 step).');
        expect(card().textContent).toContain('Actions used: actions/first-interaction@v1.');
        expect(card().textContent).toContain('Read from the file; not generated by the model.');
        expect(card().textContent).not.toContain('greets');
        // "Ask the model" asks it, and its text stays marked as generated.
        await act(async () => [...card().querySelectorAll('button')].find(item => item.textContent === 'Ask the model')!.click()); await settle();
        expect(runtime.chat).toHaveBeenCalledOnce();
        const prompt = runtime.chat.mock.calls[0][0].map(message => message.content).join('\n');
        expect(prompt).toContain('Facts read from the file (counted, not guessed):');
        expect(prompt).toContain('1 job: `build`');
        expect(card().textContent).toContain('1 job: build ("Hello new contributor", runs on ubuntu-latest, 1 step).');
        expect(card().textContent).toContain('This workflow greets new contributors on their first pull request.');
        expect(card().textContent).toContain('Facts read from the file; the text after them is generated by the model.');
    });

    it.each([
        ['docker-compose.yml', 'version: "3"\nservices:\n  web:\n    image: nginx\n  db:\n    image: postgres\n', 'Top-level keys (2): `version`, `services` (2 keys: `web`, `db`).'],
        ['package.json', '{"name": "demo", "scripts": {"build": "tsc", "test": "vitest"}}', 'Top-level keys (2): `name` (text), `scripts` (2 keys: `build`, `test`).'],
        ['pyproject.toml', '[project]\nname = "demo"\nversion = "1.0"\n\n[tool.ruff]\nline-length = 88\n', 'Tables (2): `[project]` (2 keys), `[tool.ruff]` (1 key).'],
        ['setup.cfg', '[metadata]\nname = demo\n\n[flake8]\nmax-line-length = 119\n', 'Sections (2): `[metadata]` (1 key), `[flake8]` (1 key).'],
        ['README.md', '# Demo\n\nA demo.\n\n## Install\n\n```sh\n# not a heading\npip install demo\n```\n\n## Usage\n', 'Sections (2): `Install`, `Usage`.'],
    ])('shows only the facts read from %s in the automatic card (K12)', async (path, text, fact) => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        await render({ ...props, proactive: true, selectionScope: 'django-demo:explore', readerContext: reader(text, 'file', path) });
        await click('Download & load'); await settle();
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(card().textContent).toContain(fact.replace(/`/g, ''));
        expect(card().textContent).toContain('Read from the file; not generated by the model.');
        expect([...card().querySelectorAll('button')].map(item => item.textContent)).toContain('Ask the model');
    });

    it('keeps the names-not-in check when the model is asked about a configuration file (K12)', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('This file configures `flake8` for the Python sources.');
        await render({ ...props, proactive: true, selectionScope: 'django-demo:explore', readerContext: reader('[metadata]\nname = demo\n', 'file', 'setup.cfg') });
        await click('Download & load'); await settle();
        await act(async () => [...card().querySelectorAll('button')].find(item => item.textContent === 'Ask the model')!.click()); await settle();
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(card().textContent).not.toContain('configures flake8');
        expect(card().textContent).toContain("Read from the file. The model's text named something the file does not show (here: flake8) and was left out.");
    });

    it('explains an Architecture area from readable facts without reading source', async () => {
        vi.useFakeTimers(); const { props, runtime } = fixture();
        const readSource = vi.fn(async () => snippet);
        await render({ ...props, proactive: true, selectionScope: 'django-demo:architecture', proactiveSelection: djangoAreaEvidence(), readSource }); await click('Download & load'); await settle();
        expect(readSource).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(card().textContent).toContain('Selected source area: django (2,310 files · 15,299 indexed nodes).');
        expect(card().textContent).toMatch(/Connections to \(root\): CALLS ×1,743/);
        expect(card().textContent).not.toMatch(/Finding a|Selected\.|members\[\d+\]|startLine/);
        // A question about the area gets the same readable facts.
        await type('What is in this area?'); await click('Send ↑');
        const prompt = runtime.chat.mock.calls[0][0].map(message => message.content).join('\n');
        expect(prompt).toContain('Selected source area: `django` (2,310 files · 15,299 indexed nodes).');
        expect(prompt).not.toMatch(/Selected\.|members\[\d+\]|startLine/);
    });
});

describe('general questions, prompts without a question and configuration files (C5, C6, C7)', () => {
    const snippet = { source: 'class JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"\n    template = "%(function)s(%(distinct)s%(expressions)s %(order_by)s)"\n',
        file_path: '/abs/django/contrib/postgres/aggregates/general.py', start_line: 50, end_line: 54, source_mode: 'full' };
    const last = () => [...container.querySelectorAll('.cbm-chat-turn')].at(-1)!;
    const answer = () => last().querySelector('.cbm-chat-answer-text')?.textContent ?? '';
    const buttons = () => [...last().querySelectorAll('button.cbm-chat-retry')].map(item => item.textContent);

    it('answers a short general question about the selection with the listed facts and one checked model sentence, in its language (C5)', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('`JSONBAgg` setzt `function` auf "JSONB_AGG". Sie gibt eine Liste von JSON-Daten zurück.');
        await render({ ...props, proactiveSelection: jsonbAggEvidence(), readSource: vi.fn(async () => snippet) }); await click('Download & load');
        await type('was macht diese klasse? sehr kurze antwort'); await click('Send ↑');
        expect(runtime.chat).toHaveBeenCalledOnce();
        const [request, , options] = runtime.chat.mock.calls[0];
        expect(options).toMatchObject({ maxOutputTokens: 128, generationProfile: 'automatic-explanation' });
        expect(request.at(-1)!.content).toContain('Describe this class in one short sentence that starts with `JSONBAgg`. Answer in German.');
        expect(request.at(-1)!.content).toContain('function = "JSONB_AGG"');
        expect(answer()).toContain('Ausgewählt: JSONBAgg (Klasse) in django/contrib/postgres/aggregates/general.py:50-54.');
        expect(answer()).toContain('Eingehend: 23 Beziehungen von 12 Symbolen (CALLS 11, TESTS 11, DEFINES 1).');
        expect(answer()).toContain('Ausschnitt: 1 Schritt in beide Richtungen');
        expect(answer()).toContain('JSONBAgg setzt function auf "JSONB_AGG".');
        expect(answer()).not.toContain('Liste von');
        expect(answer()).toContain('Fakten aus dem indizierten Graphen gelistet; der letzte Satz ist vom Modell erzeugt.');
        expect(last().querySelector('.cbm-chat-answer-note')).toBeNull();
        expect(buttons()).toEqual(['Modell fragen']);
        // "Modell fragen" asks the model as before, with the question's language.
        await act(async () => button('Modell fragen').click());
        expect(runtime.chat).toHaveBeenCalledTimes(2);
        expect(runtime.chat.mock.calls[1][2]?.generationProfile).toBeUndefined();
        expect(runtime.chat.mock.calls[1][0].at(-1)!.content).toContain('Answer in German.\nUser question:\nwas macht diese klasse? sehr kurze antwort');
    });

    it('leaves out a sentence that claims what the code does not show, also in German (C5)', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('JSONBAgg gibt eine Liste von Werten zurück.');
        await render({ ...props, proactiveSelection: jsonbAggEvidence(), readSource: vi.fn(async () => snippet) }); await click('Download & load');
        await type('was kansnt du mir über den code sagen'); await click('Send ↑');
        expect(answer()).toContain('Ausgewählt: JSONBAgg (Klasse)');
        expect(answer()).not.toContain('gibt eine Liste');
        expect(answer()).toContain('Der Satz des Modells behauptete etwas, das der Quelltext nicht zeigt (hier: „gibt … zurück“), und wurde weggelassen.');
    });

    it('lists only the facts without source and sends detail questions to the model as before (C5)', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, proactiveSelection: jsonbAggEvidence(), readSource: vi.fn(async () => { throw new Error('offline'); }) }); await click('Download & load');
        await type('what does this do'); await click('Send ↑');
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(answer()).toContain('Selected: JSONBAgg (Class) in django/contrib/postgres/aggregates/general.py:50-54.');
        expect(answer()).toContain('Listed from the indexed graph; not generated by the model.');
        expect(buttons()).toEqual(['Ask the model']);
        await type('Explain this class in detail, line by line.'); await click('Send ↑');
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[0][2]?.generationProfile).toBeUndefined();
    });

    it('answers a prompt without a question locally with example questions, in its language (C6)', async () => {
        const { props, runtime } = fixture();
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await click('Download & load');
        await type('test'); await click('Send ↑');
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(answer()).toContain('No question was recognized in "test". You can ask, for example:');
        // JSONBAgg is a class: what it is, who uses it, what it inherits from (W9).
        for (const example of ['What is JSONBAgg?', 'Who uses JSONBAgg?', 'What does JSONBAgg inherit from?']) expect(answer()).toContain(example);
        // The model only echoed such a prompt, so the hint offers no model (B5).
        expect(buttons()).toEqual([]);
        await type('hallo'); await click('Send ↑');
        expect(answer()).toContain('In „hallo“ wurde keine Frage erkannt. Du kannst zum Beispiel fragen:');
        for (const example of ['Was ist JSONBAgg?', 'Wer verwendet JSONBAgg?']) expect(answer()).toContain(example);
        expect(buttons()).toEqual([]);
        expect(runtime.chat).not.toHaveBeenCalled();
        // A follow-up does not carry the hint as an answer.
        await render({ ...props, readerContext: reader('repos:\n  - repo: x\n', 'file', '.pre-commit-config.yaml') });
        await type('?'); await click('Send ↑');
        expect(answer()).toContain('No question was recognized in "?".');
        expect(answer()).toContain('What does .pre-commit-config.yaml do?');
    });

    it('answers what a configuration file does from the file, in the language of the question, and asks the model only on request (C7)', async () => {
        const { props, runtime } = fixture();
        const text = 'repos:\n  - repo: https://github.com/PyCQA/isort\n    rev: 5.13.2\n    hooks:\n      - id: isort\n  - repo: https://github.com/PyCQA/flake8\n    rev: 7.1.1\n    hooks:\n      - id: flake8\n';
        await render({ ...props, selectionScope: 'django-demo:explore', readerContext: reader(text, 'file', '.pre-commit-config.yaml') }); await click('Download & load');
        for (const question of ['was macht diese datei?', 'erklär mir die datei detailliert']) {
            await type(question); await click('Send ↑');
            expect(answer()).toContain('.pre-commit-config.yaml: YAML-Konfiguration, 9 Zeilen.\npre-commit-Konfiguration: Hooks, die vor jedem Commit laufen.');
            expect(answer()).toContain('repos (Liste mit 2 Einträgen):');
            expect(answer()).toContain('repo https://github.com/PyCQA/flake8; rev 7.1.1; hooks (1): flake8');
            expect(answer()).toContain('Aus der Datei gelesen, nicht vom Modell erzeugt.');
            expect(buttons()).toEqual(['Modell fragen']);
        }
        expect(runtime.chat).not.toHaveBeenCalled();
        await type('What does this file do?'); await click('Send ↑');
        expect(answer()).toContain('.pre-commit-config.yaml: YAML configuration, 9 lines.\npre-commit configuration: hooks that run before each commit.');
        expect(runtime.chat).not.toHaveBeenCalled();
        await act(async () => button('Ask the model').click());
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[0][0][0].content).toContain('--- BEGIN EXACT SOURCE TEXT ---\nrepos:');
        // Other questions about the file go to the model.
        await type('Which hook runs flake8?'); await click('Send ↑');
        expect(runtime.chat).toHaveBeenCalledTimes(2);
    });
});

describe('a cached model across reloads and project switches (K10, K24)', () => {
    const configButtons = () => [...document.querySelectorAll('dialog .cbm-chat-model-actions button')].map(item => item.textContent);

    it('says the model is cached and loads it without a download label', async () => {
        const { props, runtime } = fixture(); const isCached = vi.fn(async (id: string) => id === BROWSER_MODELS[0].id);
        await render({ ...props, isCached }); await models();
        expect(document.querySelector<HTMLSelectElement>('#cbm-chat-model')?.selectedOptions[0].textContent).toBe(`${BROWSER_MODELS[0].displayName} · Cached`);
        expect(configButtons()).toContain('Load model (cached, no download)');
        expect(configButtons()).not.toContain('Download & load');
        expect(props.createRuntime).not.toHaveBeenCalled();
        await act(async () => button('Load model (cached, no download)').click());
        expect(runtime.prepare).toHaveBeenCalledOnce();
        expect(runtime.prepare.mock.calls[0][1]).toEqual({ cacheOnly: true });
    });

    it('offers loading the chosen model on start, off by default, and then loads it from the cache by itself', async () => {
        const first = fixture(); const isCached = vi.fn(async () => true);
        await render({ ...first.props, isCached }); await models();
        const option = document.querySelector<HTMLInputElement>('#cbm-chat-auto-load')!;
        expect(option.checked).toBe(false);
        expect(first.props.createRuntime).not.toHaveBeenCalled();
        await act(async () => option.click());
        await act(async () => root.unmount());
        root = createRoot(container);
        const second = fixture();
        await render({ ...second.props, isCached });
        expect(second.props.createRuntime).toHaveBeenCalledOnce();
        expect(second.runtime.prepare.mock.calls[0][1]).toEqual({ cacheOnly: true });
    });

    it('does not load by itself when the files are not cached', async () => {
        window.localStorage.setItem(AGENT_PREFERENCES_KEY, JSON.stringify({ version: 1, preferences: { modelId: BROWSER_MODELS[0].id, automatic: true, autoLoad: true, limits: {} } }));
        const { props } = fixture();
        await render({ ...props, isCached: vi.fn(async () => false) });
        expect(props.createRuntime).not.toHaveBeenCalled();
    });

    it('does not load by itself on a page that opens after another project, without the automatic option', async () => {
        // A project switch stays in the page and hands the model over (BrowserChatDock.handover.test.tsx);
        // a page that is opened or reloaded starts with the agent off unless asked to load on start.
        const { props } = fixture();
        await render({ ...props, isCached: vi.fn(async () => true) });
        expect(props.createRuntime).not.toHaveBeenCalled();
    });
});

