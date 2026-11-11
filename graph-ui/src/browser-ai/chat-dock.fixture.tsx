import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, vi } from 'vitest';
import BrowserChatDock, { type BrowserChatDockProps, type BrowserChatReaderContext } from './BrowserChatDock';
import type { BrowserAiProgress, BrowserChatMessage } from './browser-ai-runtime';
import type { BrowserChatOptions } from './browser-ai-controller';
import type { SymbolSnippet } from './symbol-source';

/** The chat dock in jsdom with a fake model, for tests that drive it like a reader:
 * type, send, click the buttons under an answer and read what the model was sent. */

/** JSONBAgg as get_code_snippet returns it from django-demo. */
export const JSONB_AGG_SNIPPET: SymbolSnippet = { source: 'class JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"\n'
    + '    template = "%(function)s(%(distinct)s%(expressions)s %(order_by)s)"\n    allow_distinct = True\n    output_field = JSONField()\n',
file_path: '/abs/django/contrib/postgres/aggregates/general.py', start_line: 50, end_line: 54, source_mode: 'full' };

/** An open file or marked code in Explore. */
export const readerOf = (text: string, path: string, kind: 'file' | 'selection' = 'file', lines?: { start: number; end: number }): BrowserChatReaderContext => {
    const count = text.split('\n').length;
    return { project: 'sample', path, status: 'ready', source: { id: `reader-${path}-${kind}`, text, path, kind, project: 'sample', startLine: lines?.start ?? 1, startColumn: 1,
        endLine: lines?.end ?? count, endColumn: 1, sourceVersion: 'sha256:1' } };
};

export function dockHarness() {
    let container: HTMLDivElement;
    let root: Root;
    let props: BrowserChatDockProps;
    beforeEach(() => {
        (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
        window.localStorage.clear();
        container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
    });
    afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); });

    const runtime = () => ({
        prepare: vi.fn(async (_progress: (value: BrowserAiProgress) => void, _options?: { cacheOnly?: boolean }) => {}),
        explain: vi.fn(async () => 'Legacy'),
        countTokens: vi.fn(async (_messages: readonly BrowserChatMessage[]) => 100),
        chat: vi.fn(async (_messages: readonly BrowserChatMessage[], _onToken: (chunk: string) => void, _options?: BrowserChatOptions) => 'Adds the two values.'),
        stop: vi.fn(), dispose: vi.fn(),
    });
    /** A dock with a fake model that is loaded at once; `readSource` returns JSONBAgg. */
    const setup = () => {
        const model = runtime();
        const readSource = vi.fn(async () => JSONB_AGG_SNIPPET);
        const base: BrowserChatDockProps = { proactive: false, open: true, onClose: vi.fn(), onAttachmentConsumed: vi.fn(), onAttachmentRemoved: vi.fn(),
            createRuntime: vi.fn(() => model), removeCache: vi.fn(async () => {}), readSource };
        return { runtime: model, props: base, readSource };
    };
    const render = async (next: BrowserChatDockProps) => { props = next; await act(async () => root.render(<BrowserChatDock {...next} />)); };
    const button = (label: string, scope: ParentNode = document.body) => {
        const found = [...scope.querySelectorAll('button')].find(item => item.textContent === label || item.getAttribute('aria-label') === label);
        expect(found, `button ${label}`).toBeDefined();
        return found!;
    };
    const click = async (label: string, scope?: ParentNode) => { await act(async () => button(label, scope).click()); };
    /** Opens the agent configuration and loads the fake model. */
    const load = async () => {
        await render({ ...props, settingsRequest: (props.settingsRequest ?? 0) + 1 });
        await click('Download & load');
    };
    const type = async (value: string) => {
        const input = container.querySelector('textarea')!;
        await act(async () => {
            Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')!.set!.call(input, value);
            input.dispatchEvent(new Event('input', { bubbles: true }));
        });
    };
    const ask = async (question: string) => { await type(question); await click('Send ↑'); };
    const turns = () => [...container.querySelectorAll('.cbm-chat-turn')];
    const last = () => turns().at(-1)!;
    const answerOf = (turn: Element) => turn.querySelector('.cbm-chat-answer-text')?.textContent ?? '';
    const buttonsOf = (turn: Element) => [...turn.querySelectorAll('button.cbm-chat-retry')].map(item => item.textContent);
    const notesOf = (turn: Element) => [...turn.querySelectorAll('.cbm-chat-answer-note, .cbm-chat-model-note')].map(item => item.textContent ?? '');
    const dividers = () => [...container.querySelectorAll('.cbm-chat-topic-break')].map(item => item.textContent);
    const card = () => container.querySelector('[aria-label="Current selection explanation"]');
    /** The user messages of a request, each by its last line (the question). */
    const questionsIn = (request: readonly BrowserChatMessage[]) => request.filter(message => message.role === 'user').map(message => message.content.split('\n').at(-1));
    return { setup, render, load, click, button, type, ask, turns, last, answerOf, buttonsOf, notesOf, dividers, card, questionsIn, container: () => container };
}
