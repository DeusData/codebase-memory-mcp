// @vitest-environment jsdom
import { act, StrictMode } from 'react';
import { flushSync } from 'react-dom';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import BrowserChatDock, { type BrowserChatDockProps, type BrowserChatReaderContext } from './BrowserChatDock';
import type { BrowserAiProgress, BrowserChatMessage } from './browser-ai-runtime';
import type { BrowserChatOptions } from './browser-ai-controller';
import { switchKeepingAgent } from './agent-handover';

/*
 * K24: a project switch stays in the page. The window of the old project unmounts and the
 * window of the new one mounts in the same commit; the dock of the old project hands its
 * loaded model to the dock of the new one, which neither creates a worker nor loads again.
 */

let container: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    window.localStorage.clear();
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); vi.useRealTimers(); });

const reader = (project: string, text: string): BrowserChatReaderContext => ({ project, path: 'src/sum.ts', status: 'ready',
    source: { id: `reader-${project}`, text, path: 'src/sum.ts', project, startLine: 1, startColumn: 1, endLine: 1, endColumn: 1, sourceVersion: 'sha256:1', kind: 'file' } });
function fixture() {
    const runtime = {
        prepare: vi.fn(async (_progress: (value: BrowserAiProgress) => void, _options?: { cacheOnly?: boolean }) => {}),
        explain: vi.fn(async () => 'Legacy'),
        countTokens: vi.fn(async (_messages: readonly BrowserChatMessage[]) => 100),
        chat: vi.fn(async (_messages: readonly BrowserChatMessage[], _onToken: (chunk: string) => void, _options?: BrowserChatOptions) => 'Adds the two values.'),
        stop: vi.fn(), dispose: vi.fn(), setFatalHandler: vi.fn(),
    };
    const states: string[] = [];
    const props: BrowserChatDockProps = { proactive: false, open: true, onClose: vi.fn(), onAttachmentConsumed: vi.fn(), createRuntime: vi.fn(() => runtime),
        removeCache: vi.fn(async () => {}), isCached: vi.fn(async () => true), onAgentStateChange: state => { states.push(state); } };
    return { runtime, props, states };
}
const dock = (props: BrowserChatDockProps, project: string, strict = false) => {
    const element = <BrowserChatDock key={project} {...props} historyKey={`origin:${project}`} readerContext={reader(project, `SOURCE_OF_${project}`)} />;
    return strict ? <StrictMode>{element}</StrictMode> : element;
};
function button(label: string): HTMLButtonElement {
    const target = [...document.body.querySelectorAll('button')].find(item => item.textContent === label || item.getAttribute('aria-label') === label);
    expect(target, `button ${label}`).toBeDefined(); return target!;
}
/** Opens the agent configuration (a new settings request) and loads the cached model. */
async function configure(props: BrowserChatDockProps, project: string, strict = false): Promise<void> {
    await act(async () => root.render(dock(props, project, strict)));
    await act(async () => root.render(dock({ ...props, settingsRequest: 1 }, project, strict)));
}
async function load(props: BrowserChatDockProps, project: string, strict = false): Promise<void> {
    await configure(props, project, strict);
    await act(async () => button('Load model (cached, no download)').click());
}
async function type(value: string): Promise<void> {
    const input = container.querySelector('textarea')!;
    await act(async () => {
        Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')!.set!.call(input, value);
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
}
/** The switch as the project window runs it: the new window replaces the old one in one commit. */
async function switchTo(props: BrowserChatDockProps, project: string, strict = false): Promise<void> {
    await act(async () => switchKeepingAgent(() => flushSync(() => root.render(dock(props, project, strict)))));
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>(yes => { resolve = yes; });
    return { promise, resolve };
}

describe('the loaded model across an in-page project switch (K24)', () => {
    it('hands the loaded model to the next project without a new worker, a second load or an "off" lamp', async () => {
        const { props, runtime, states } = fixture();
        await load(props, 'django-demo');
        expect(states.at(-1)).toBe('active');
        states.length = 0;
        await switchTo(props, 'cbm');
        expect(props.createRuntime).toHaveBeenCalledOnce();
        expect(runtime.prepare).toHaveBeenCalledOnce();
        expect(runtime.dispose).not.toHaveBeenCalled();
        expect(states.filter(state => state !== 'active')).toEqual([]);
        expect(container.textContent).not.toContain('Enable the local agent');
        // The new project's dock answers on the same worker, with the new project's file.
        await type('What does this file do?');
        await act(async () => button('Send ↑').click());
        expect(runtime.chat).toHaveBeenCalledOnce();
        const sent = runtime.chat.mock.calls[0][0].map(message => message.content).join('\n');
        expect(sent).toContain('SOURCE_OF_cbm');
        expect(sent).not.toContain('SOURCE_OF_django-demo');
        // The worker's fatal faults now reach the new dock.
        expect(runtime.setFatalHandler).toHaveBeenCalledTimes(2);
    });

    it('lets an answer of the old project settle first and never shows it in the new one', async () => {
        const { props, runtime } = fixture(); const old = deferred<string>();
        await load(props, 'django-demo');
        runtime.chat.mockReturnValueOnce(old.promise);
        await type('Old project question');
        await act(async () => button('Send ↑').click());
        await switchTo(props, 'cbm');
        expect(runtime.stop).toHaveBeenCalled();
        expect(runtime.dispose).not.toHaveBeenCalled();
        expect(container.querySelector('textarea')).not.toBeNull();
        await act(async () => old.resolve('OLD_PROJECT_ANSWER'));
        await type('New project question');
        await act(async () => button('Send ↑').click());
        expect(runtime.chat).toHaveBeenCalledTimes(2);
        expect(runtime.chat.mock.calls[1][0].at(-1)?.content).toContain('New project question');
        expect(container.textContent).not.toContain('OLD_PROJECT_ANSWER');
        expect(container.textContent).not.toContain('Old project question');
    });

    it.each(['token counting', 'answer generation'] as const)('keeps the old %s barrier across successive project switches', async stage => {
        const { props, runtime } = fixture();
        const counting = deferred<number>(), answer = deferred<string>();
        await load(props, 'django-demo');
        if (stage === 'token counting') runtime.countTokens.mockReturnValueOnce(counting.promise);
        else runtime.chat.mockReturnValueOnce(answer.promise);
        await type('Old project question');
        await act(async () => button('Send ↑').click());
        expect(runtime.countTokens).toHaveBeenCalledOnce();
        expect(runtime.chat).toHaveBeenCalledTimes(stage === 'token counting' ? 0 : 1);

        try {
            await switchTo(props, 'cbm');
            await switchTo(props, 'third-project');
            await type('Final project question');
            // The first operation stays blocked by our promise, independent of timing.
            await act(async () => { container.querySelector('textarea')!.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })); });
            expect(runtime.countTokens).toHaveBeenCalledOnce();
            expect(runtime.chat).toHaveBeenCalledTimes(stage === 'token counting' ? 0 : 1);
            expect(button('Stopping…').disabled).toBe(true);
            expect(runtime.dispose).not.toHaveBeenCalled();
        } finally {
            await act(async () => { counting.resolve(100); answer.resolve('OLD_PROJECT_ANSWER'); });
        }

        await act(async () => button('Send ↑').click());
        expect(runtime.countTokens).toHaveBeenCalledTimes(2);
        expect(runtime.chat).toHaveBeenCalledTimes(stage === 'token counting' ? 1 : 2);
        const sent = runtime.chat.mock.calls.at(-1)![0].map(message => message.content).join('\n');
        expect(sent).toContain('Final project question');
        expect(sent).toContain('SOURCE_OF_third-project');
        expect(sent).not.toContain('SOURCE_OF_django-demo');
        expect(container.textContent).not.toContain('OLD_PROJECT_ANSWER');
        expect(props.createRuntime).toHaveBeenCalledOnce();
        expect(runtime.prepare).toHaveBeenCalledOnce();
    });

    it('keeps a model that is still loading and finishes loading it in the new project', async () => {
        const { props, runtime, states } = fixture(); const loading = deferred<void>();
        runtime.prepare.mockReturnValueOnce(loading.promise);
        await load(props, 'django-demo');
        expect(states.at(-1)).toBe('loading');
        await switchTo(props, 'cbm');
        expect(runtime.dispose).not.toHaveBeenCalled();
        await act(async () => loading.resolve());
        expect(runtime.prepare).toHaveBeenCalledOnce();
        expect(props.createRuntime).toHaveBeenCalledOnce();
        expect(states.at(-1)).toBe('active');
    });

    it('shows the progress of a download still running at the switch, and its later progress, in the new project', async () => {
        const { props, runtime, states } = fixture(); const loading = deferred<void>();
        let report: (value: BrowserAiProgress) => void = () => {};
        runtime.prepare.mockImplementationOnce(async (progress) => { report = progress; return loading.promise; });
        props.isCached = vi.fn(async () => false);
        await configure(props, 'django-demo');
        await act(async () => button('Download & load').click());
        await act(async () => report({ file: 'onnx/model_q4f16.onnx', progress: 40 }));
        const bar = () => document.body.querySelector<HTMLProgressElement>('.cbm-chat-loading progress');
        expect(bar()?.value).toBe(40);
        await switchTo(props, 'cbm');
        // The configuration of the new project shows how far the download got, not an empty bar.
        await act(async () => root.render(dock({ ...props, settingsRequest: 1 }, 'cbm')));
        expect(bar()?.value).toBe(40);
        expect(document.body.querySelector('.cbm-chat-loading')?.textContent).toContain('onnx/model_q4f16.onnx');
        await act(async () => report({ file: 'onnx/model_q4f16.onnx', progress: 75 }));
        expect(bar()?.value).toBe(75);
        await act(async () => loading.resolve());
        expect(bar()).toBeNull();
        expect(states.at(-1)).toBe('active');
        expect(props.createRuntime).toHaveBeenCalledOnce();
        expect(runtime.prepare).toHaveBeenCalledOnce();
    });

    it('unloads the model when no dock takes it, and on an unmount that is not a switch', async () => {
        const first = fixture();
        await load(first.props, 'django-demo');
        await act(async () => switchKeepingAgent(() => flushSync(() => root.render(<div />))));
        expect(first.runtime.dispose).toHaveBeenCalledOnce();
        const second = fixture();
        await load(second.props, 'cbm');
        await act(async () => root.render(<div />));
        expect(second.runtime.dispose).toHaveBeenCalledOnce();
    });

    it('says in the configuration what a project switch does with a loaded model', async () => {
        const { props } = fixture();
        await configure(props, 'django-demo');
        const title = document.querySelector('#cbm-chat-auto-load')?.closest('label')?.getAttribute('title') ?? '';
        expect(title).toContain('A project switch stays in this page and keeps a loaded model without loading it again');
        expect(title).toContain('when the page is opened or reloaded');
    });

    it('survives the development double effect of StrictMode', async () => {
        const { props, runtime, states } = fixture();
        await load(props, 'django-demo', true);
        states.length = 0;
        await switchTo(props, 'cbm', true);
        expect(runtime.dispose).not.toHaveBeenCalled();
        expect(props.createRuntime).toHaveBeenCalledOnce();
        expect(states.filter(state => state !== 'active')).toEqual([]);
    });
});
