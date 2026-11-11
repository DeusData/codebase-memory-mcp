// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import BrowserChatDock, { type BrowserChatDockProps, type BrowserChatReaderContext } from './BrowserChatDock';
import type { BrowserAiProgress, BrowserChatMessage } from './browser-ai-runtime';
import type { BrowserChatOptions } from './browser-ai-controller';
import { jsonbAggEvidence } from './galaxy-evidence.fixture';
import { AGENT_PREFERENCES_KEY } from './agent-preferences';
import { BROWSER_MODELS } from './model-policy';

/* The chat dock shows the wording of the third review (W5 to W10) where it builds it. */

let container: HTMLDivElement;
let root: Root;
let renderedProps: BrowserChatDockProps;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    window.localStorage.clear();
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); vi.useRealTimers(); });

const snippet = { source: 'class JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"\n    allow_distinct = True\n',
    file_path: '/abs/django/contrib/postgres/aggregates/general.py', start_line: 50, end_line: 52, source_mode: 'full' };
function fixture() {
    const runtime = {
        prepare: vi.fn(async (_progress: (value: BrowserAiProgress) => void, _options?: { cacheOnly?: boolean }) => {}),
        explain: vi.fn(async () => 'Legacy'),
        countTokens: vi.fn(async (_messages: readonly BrowserChatMessage[]) => 100),
        chat: vi.fn(async (_messages: readonly BrowserChatMessage[], _onToken: (chunk: string) => void, _options?: BrowserChatOptions) => 'Adds the two values.'),
        stop: vi.fn(), dispose: vi.fn(),
    };
    const props = { proactive: false, open: true, onClose: vi.fn(), onAttachmentConsumed: vi.fn(), onAttachmentRemoved: vi.fn(), createRuntime: vi.fn(() => runtime),
        removeCache: vi.fn(async () => {}), readSource: vi.fn(async () => snippet) };
    return { runtime, props };
}
const reader = (text: string, path: string): BrowserChatReaderContext => ({ project: 'sample', path, status: 'ready',
    source: { id: `reader-${path}`, text, path, kind: 'file', project: 'sample', startLine: 1, startColumn: 1, endLine: text.split('\n').length, endColumn: 1, sourceVersion: 'sha256:1' } });
async function render(props: BrowserChatDockProps): Promise<void> { renderedProps = props; await act(async () => root.render(<BrowserChatDock {...props} />)); }
function button(label: string): HTMLButtonElement {
    const target = [...document.body.querySelectorAll('button')].find(item => item.textContent === label || item.getAttribute('aria-label') === label);
    expect(target, `button ${label}`).toBeDefined(); return target!;
}
async function load(): Promise<void> {
    await render({ ...renderedProps, settingsRequest: (renderedProps.settingsRequest ?? 0) + 1 });
    await act(async () => button('Download & load').click());
}
async function ask(value: string): Promise<void> {
    const input = container.querySelector('textarea')!;
    await act(async () => {
        Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')!.set!.call(input, value);
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
    await act(async () => button('Send ↑').click());
}
const last = () => [...container.querySelectorAll('.cbm-chat-turn')].at(-1)!;
const answer = () => last().querySelector('.cbm-chat-answer-text')?.textContent ?? '';
const card = () => container.querySelector('[aria-label="Current selection explanation"]')?.textContent ?? '';
const settle = async () => { await act(async () => { await vi.advanceTimersByTimeAsync(650); }); };

describe('the note under a left out model sentence (W5)', () => {
    it('names the claim word of a German general question and that it was checked against the source', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('JSON-BAgg ist eine Aggregation, die die Zeilen in einen JSON-Array umwandelt.');
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await load();
        await ask('was macht diese klasse? sehr kurze antwort');
        expect(answer()).not.toContain('JSON-Array');
        expect(answer()).toContain('Der Satz des Modells behauptete etwas, das der Quelltext nicht zeigt (hier: „Array“), und wurde weggelassen.');
        expect(answer()).not.toContain('das die Fakten nicht zeigen');
    });

    it('names the claim word of an English one', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('JSONBAgg aggregates rows into one output value.');
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await load();
        await ask('What does this class do?');
        expect(answer()).toContain('The model\'s sentence claimed something the source does not show (here: "output") and was left out.');
    });

    it('names the unknown name under an automatic explanation and under a configuration file card', async () => {
        vi.useFakeTimers();
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce('JSONBAgg is checked by the flake8 linter.');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() }); await load(); await settle();
        expect(card()).toContain('The model\'s sentence named something that is in neither the source nor the facts (here: flake8) and was left out.');
        runtime.chat.mockResolvedValueOnce('This file configures `flake8` for the Python sources.');
        await render({ ...props, proactive: true, selectionScope: 'django-demo:explore', readerContext: reader('[metadata]\nname = demo\n', 'setup.cfg') }); await settle();
        await act(async () => button('Ask the model').click()); await settle();
        expect(card()).toContain('Read from the file. The model\'s text named something the file does not show (here: flake8) and was left out.');
    });
});

describe('the token limit note (W6)', () => {
    const outputLimit = (outputTokens: number) => window.localStorage.setItem(AGENT_PREFERENCES_KEY, JSON.stringify({ version: 1,
        preferences: { modelId: BROWSER_MODELS[0].id, automatic: true, limits: { [BROWSER_MODELS[0].id]: { inputTokens: 2048, outputTokens } } } }));
    const cut = (runtime: ReturnType<typeof fixture>['runtime'], text = 'A long answer that') => runtime.chat.mockImplementationOnce(async (_messages, _onToken, options) => {
        options?.onComplete?.({ stopReason: 'length' }); return text;
    });
    const note = (scope: string) => container.querySelector<HTMLDetailsElement>(`${scope} details.cbm-chat-limit-note`);
    const noteButtons = (scope: string) => [...note(scope)?.querySelectorAll('button') ?? []].map(item => item.textContent);

    it('at the maximum says that larger models have the same limit and how to get the rest, without offering them', async () => {
        const { props, runtime } = fixture(); cut(runtime);
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await load();
        await ask('Explain this class in detail, line by line.');
        const text = note('.cbm-chat-turn')?.textContent ?? '';
        expect(text).toContain('The output limit is at its maximum of 512 tokens.');
        expect(text).toContain('Larger models have the same limit. Ask about one part of the code, or ask for the rest of the answer.');
        expect(text).not.toMatch(/A larger model|download/);
        expect(note('.cbm-chat-turn')?.querySelectorAll('li')).toHaveLength(0);
        expect(noteButtons('.cbm-chat-turn')).toEqual([]);
    });

    it('below the maximum offers the higher limit and lists larger models without promising a longer answer', async () => {
        outputLimit(256);
        const { props, runtime } = fixture(); cut(runtime);
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await load();
        await ask('Explain this class in detail, line by line.');
        const text = note('.cbm-chat-turn')?.textContent ?? '';
        expect(text).toContain('You can raise the output limit up to 512 tokens in the agent configuration.');
        expect(text).toContain('A larger model may stay closer to the question, but its output limit is the same. Each is a one-time download and needs more memory than its download size:');
        expect(text).not.toContain('memory use is higher than the download');
        expect(text).toContain('Qwen3 0.6B · 579 MB download');
        expect(noteButtons('.cbm-chat-turn')).toEqual(['Change the output limit']);
    });

    it('under an automatic explanation offers a higher limit only below the 128 tokens it can use', async () => {
        vi.useFakeTimers();
        outputLimit(256);
        const { props, runtime } = fixture(); cut(runtime, 'JSONBAgg sets the function to JSONB_AGG and');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() }); await load(); await settle();
        const text = note('.cbm-chat-explanation')?.textContent ?? '';
        expect(text).toContain('Automatic explanations stop after 128 output tokens so they stay short');
        expect(text).toContain('Ask in the chat for a longer answer.');
        expect(text).not.toMatch(/raise the output limit|A larger model/i);
        expect(noteButtons('.cbm-chat-explanation')).toEqual([]);
    });

    it('does not call the reader\'s own lower limit a design choice', async () => {
        vi.useFakeTimers();
        outputLimit(32);
        const { props, runtime } = fixture(); cut(runtime, 'JSONBAgg sets the function to JSONB_AGG and');
        await render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() }); await load(); await settle();
        const text = note('.cbm-chat-explanation')?.textContent ?? '';
        expect(text).toContain('Automatic explanations stop after 32 output tokens, your output limit, and read at most 1,536 input tokens of source and facts.');
        expect(text).not.toContain('so they stay short');
        expect(text).toContain('Raise the output limit in the agent configuration and automatic explanations can use up to 128 output tokens.');
        expect(noteButtons('.cbm-chat-explanation')).toEqual(['Change the output limit']);
    });
});

describe('the note about names the answer was not given (W7)', () => {
    it('counts the names it does not show and says plainly when the answer is likely made up', async () => {
        const { props, runtime } = fixture();
        runtime.chat.mockResolvedValueOnce(`It uses ${Array.from({ length: 9 }, (_, index) => `\`helper_${index}\``).join(', ')}, \`%s\`, \`False\` and runs on PostgreSQL.`);
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await load();
        await ask('Explain this class in detail, line by line.');
        const note = [...last().querySelectorAll('.cbm-chat-answer-note')].map(item => item.textContent ?? '').find(text => text.includes('graph facts')) ?? '';
        expect(note).toBe('9 names in this answer are not in the source or graph facts it was given: helper_0, helper_1, helper_2, helper_3, helper_4, helper_5, +3 more. '
            + 'The answer is likely made up; do not rely on it.');
    });
});

describe('the topic divider in the language of its turn (W8)', () => {
    it('heads a German question with a German divider and an English one with an English divider', async () => {
        const { props } = fixture();
        const workflow = reader('name: New contributor message', '.github/workflows/new_contributor_pr.yml');
        await render({ ...props, selectionScope: 'django-demo:explore', readerContext: workflow }); await load();
        await ask('Which jobs does it run?');
        await render({ ...props, selectionScope: 'django-demo:galaxy', proactiveSelection: jsonbAggEvidence() });
        await ask('Wer ruft JSONBAgg auf?');
        await render({ ...props, selectionScope: 'django-demo:explore', readerContext: { ...workflow, path: 'tox.ini', source: { ...workflow.source!, id: 'reader-tox', path: 'tox.ini' } } });
        await ask('What is in this file?');
        expect([...container.querySelectorAll('.cbm-chat-topic-break')].map(item => item.textContent)).toEqual([
            'Neues Thema: JSONBAgg. Frühere Nachrichten werden bei diesen Fragen nicht mitgeschickt.',
            'New topic: tox.ini. Earlier messages are not sent with these questions.']);
    });
});

describe('the Source block of an answer in its language (W10)', () => {
    it('says plainly where the relationships come from, in German under a German answer', async () => {
        const { props } = fixture();
        await render({ ...props, proactiveSelection: jsonbAggEvidence() }); await load();
        await ask('Wer ruft JSONBAgg auf?');
        const source = () => last().querySelector('.cbm-chat-source-content')?.textContent ?? '';
        expect(source()).toContain('Die Beziehungen hier wurden aus dem Code gelesen; sie zeigen nicht, was zur Laufzeit ausgeführt wird.');
        expect(source()).not.toContain('Static graph relationships');
        await ask('Who calls JSONBAgg?');
        expect(source()).toContain('The relationships here come from reading the code; they do not show what runs at runtime.');
    });
});
