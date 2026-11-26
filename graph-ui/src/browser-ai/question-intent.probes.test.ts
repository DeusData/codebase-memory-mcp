import { describe, expect, it } from 'vitest';
import { knownNames, noQuestion } from './question-intent';
import { jsonbAggEvidence } from './galaxy-evidence.fixture';
import { readGalaxyEvidence } from './galaxy-evidence';

/** "test" went to the model whenever the open file contained the word, because every word of
 * the file counted as a name the prompt could ask about (B5). */
describe('greetings and probe words are never names of the open file (B5)', () => {
    const file = 'import pytest\n\ndef test(): ...\n# hallo hi hello hey moin ok ja nein danke\nclass Aggregate: pass\n';

    it('reads "test", "hallo" and the like as prompts without a question, even when the file contains them', () => {
        const known = knownNames({ names: ['tests/test_aggregates.py'], texts: [file] });
        for (const prompt of ['test', 'hallo', 'hi', 'hello', 'hey', 'moin', 'ok', 'ja', 'nein', 'danke', '?', 'Test', 'hallo test']) expect(noQuestion(prompt, known), prompt).toBe(true);
        // Real words of the file still ask about it.
        expect(noQuestion('Aggregate', known)).toBe(false);
        expect(noQuestion('pytest', known)).toBe(false);
    });

    it('counts a probe word when it is exactly the name of the selection', () => {
        expect(noQuestion('test', knownNames({ names: ['test'], texts: [file] }))).toBe(false);
        expect(noQuestion('test', knownNames({ names: ['django-demo.tests.test'], texts: [] }))).toBe(false);
        // A longer name that only contains the word does not.
        expect(noQuestion('test', knownNames({ names: ['test_jsonb_agg'], texts: [file] }))).toBe(true);
        const galaxy = readGalaxyEvidence(jsonbAggEvidence().text)!;
        expect(noQuestion('test', knownNames({ galaxy, names: ['JSONBAgg'], texts: [] }))).toBe(true);
    });
});
