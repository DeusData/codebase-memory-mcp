// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';
import { jsonbAggEvidence } from './galaxy-evidence.fixture';
import { dockHarness, readerOf } from './chat-dock.fixture';

const dock = dockHarness();
const TESTS = 'from django.test import TestCase\n\n\nclass AggregateTests(TestCase):\n    def test_values_list(self):\n        # test that the list is ok\n        self.assertEqual(1, 1)\n';

/** A prompt that asks nothing gets example questions, also when the open file contains its word (B5). */
describe('prompts without a question about an open file (B5)', () => {
    it('answers "test" locally although the open file contains "test", and offers no model on that hint', async () => {
        const { props, runtime } = dock.setup();
        await dock.render({ ...props, selectionScope: 'django-demo:explore', readerContext: readerOf(TESTS, 'tests/postgres_tests/test_aggregates.py') }); await dock.load();
        for (const prompt of ['test', 'hallo', 'ok']) {
            await dock.ask(prompt);
            expect(dock.answerOf(dock.last())).toMatch(/No question was recognized|wurde keine Frage erkannt/);
            expect(dock.buttonsOf(dock.last())).toEqual([]);
        }
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        // A real name of the file is a question.
        await dock.ask('AggregateTests');
        expect(runtime.chat).toHaveBeenCalledOnce();
    });

    it('offers no model under the hint for a Galaxy selection either', async () => {
        const { props, runtime } = dock.setup();
        await dock.render({ ...props, proactiveSelection: jsonbAggEvidence() }); await dock.load();
        await dock.ask('test');
        expect(dock.answerOf(dock.last())).toContain('What is JSONBAgg?');
        expect(dock.buttonsOf(dock.last())).toEqual([]);
        expect(runtime.chat).not.toHaveBeenCalled();
    });
});
