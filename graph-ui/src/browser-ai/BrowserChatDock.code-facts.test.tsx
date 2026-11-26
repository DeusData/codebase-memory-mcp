// @vitest-environment jsdom
import { act } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { jsonbAggEvidence } from './galaxy-evidence.fixture';
import { dockHarness } from './chat-dock.fixture';

const dock = dockHarness();

/** "was macht diese klasse? sehr kurze antwort" on JSONBAgg got only counts: the model's sentence
 * claimed "Arrays" and was dropped, and nothing said what the class declares (B2). */
describe('a general question about a selected symbol says what its code declares (B2)', () => {
    afterEach(() => vi.useRealTimers());

    it('lists the class line and its assignments under "Im Quelltext:" even when the sentence is dropped', async () => {
        const { props, runtime } = dock.setup();
        runtime.chat.mockResolvedValueOnce('JSONBAgg ist eine Aggregation, die Werte in einen JSON-Array umwandelt.');
        await dock.render({ ...props, proactiveSelection: jsonbAggEvidence() }); await dock.load();
        await dock.ask('was macht diese klasse? sehr kurze antwort');
        expect(runtime.chat).toHaveBeenCalledOnce();
        const answer = dock.answerOf(dock.last());
        expect(answer).toContain('Ausgewählt: JSONBAgg (Klasse)');
        expect(answer).toContain('Im Quelltext:');
        for (const line of ['class JSONBAgg(OrderableAggMixin, Aggregate):', 'function = "JSONB_AGG"', 'template = "%(function)s(%(distinct)s%(expressions)s %(order_by)s)"',
            'allow_distinct = True', 'output_field = JSONField()']) expect(dock.last().querySelector('.cbm-chat-answer-text pre')?.textContent).toContain(line);
        expect(answer).not.toContain('JSON-Array');
        expect(answer).toContain('wurde weggelassen');
        // The facts come first, then the code, then who wrote what.
        expect(answer.indexOf('Ausgewählt:')).toBeLessThan(answer.indexOf('Im Quelltext:'));
        expect(answer.indexOf('Im Quelltext:')).toBeLessThan(answer.indexOf('wurde weggelassen'));
    });

    it('says it in English for an English question, with the checked sentence after the code', async () => {
        const { props, runtime } = dock.setup();
        runtime.chat.mockResolvedValueOnce('`JSONBAgg` sets `function` to "JSONB_AGG" and allows distinct values.');
        await dock.render({ ...props, proactiveSelection: jsonbAggEvidence() }); await dock.load();
        await dock.ask('What does this do?');
        const answer = dock.answerOf(dock.last());
        expect(answer).toContain('In the source:');
        expect(answer).toContain('class JSONBAgg(OrderableAggMixin, Aggregate):');
        expect(answer.indexOf('In the source:')).toBeLessThan(answer.indexOf('JSONBAgg sets function to "JSONB_AGG"'));
    });

    it('shows no code without source: the listed facts stay the answer', async () => {
        const { props, runtime } = dock.setup();
        await dock.render({ ...props, proactiveSelection: jsonbAggEvidence(), readSource: vi.fn(async () => { throw new Error('offline'); }) }); await dock.load();
        await dock.ask('What does this do?');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(dock.answerOf(dock.last())).not.toContain('In the source:');
    });

    it('adds the same lines to the automatic explanation card', async () => {
        vi.useFakeTimers(); const { props, runtime } = dock.setup();
        runtime.chat.mockResolvedValueOnce('JSONBAgg turns rows into arrays.');
        await dock.render({ ...props, proactive: true, proactiveSelection: jsonbAggEvidence() }); await dock.load();
        await act(async () => { await vi.advanceTimersByTimeAsync(650); });
        const card = dock.card()!.textContent ?? '';
        expect(card).toContain('Incoming: 23 relationships from 12 symbols');
        expect(card).toContain('In the source:');
        expect(card).toContain('output_field = JSONField()');
        // The dropped sentence stays out; only the note names its claim (W5).
        expect(card).not.toContain('turns rows into arrays');
    });
});
