import { describe, expect, it } from 'vitest';
import { generalQuestion, noQuestion } from './question-intent';

const names = ['JSONBAgg', 'django.contrib.postgres.aggregates.general.JSONBAgg'];

describe('short general questions about the selection (C5)', () => {
    it.each([
        'was kannst du mir über den code sagen', 'was kansnt du mir über den code sagen', 'was macht diese klasse? sehr kurze antwort', 'Was macht diese Klasse?',
        'was ist das', 'erklär das', 'Erkläre mir diese Funktion', 'Erklaer das bitte kurz', 'wass macht dise klase', 'was tut diese methode?',
        'what does this do', 'What does this class do?', 'what is this', "what's this?", 'tell me about this code', 'explain this', 'Explain this class',
        'explain it briefly', 'What can you tell me about this code?', 'What does JSONBAgg do?', 'Was macht JSONBAgg?', 'was macht jsonbag?', 'explain JSONBAgg',
        'What does this file do?', 'was macht diese datei?', 'was kannst du mir über dieses aktuelle File sagen', 'what is in this file?', 'Describe this file',
    ])('reads %s as a general question', prompt => {
        expect(generalQuestion(prompt, names)).toBe('general');
    });

    it.each([
        'Explain this class in detail, line by line.', 'erklär mir die datei detailliert', 'Erklär diese Klasse ausführlich', 'Erklär diese Klasse Zeile für Zeile',
        'kannst du das genau erklären?', 'explain this file step by step', 'Summarize this file in detail',
    ])('reads %s as a question that asks for detail', prompt => {
        expect(generalQuestion(prompt, names)).toBe('detail');
    });

    it.each([
        'What does it configure?', 'Which tests use it?', 'What is in this area?', 'Hat diese Klasse Luft?', 'Who calls JSONBAgg?', 'What does test_jsonb_agg do?',
        'Why does JSONBAgg inherit from Aggregate?', 'Wie viele Jobs gibt es in dieser Datei?', 'Welche Jobs gibt es in dieser Datei?', 'How many jobs does this workflow have?',
        'Und was noch?', 'Write a test for JSONBAgg', 'what does this class do with distinct values?', 'Explain how the callers use JSONBAgg', 'test', 'what does BaseCommand do?',
    ])('leaves %s to the other answers', prompt => {
        expect(generalQuestion(prompt, names)).toBeUndefined();
    });
});

describe('prompts that hold no question (C6)', () => {
    const known = (word: string) => ['jsonbagg', 'test_jsonb_agg', 'general.py'].includes(word.toLowerCase());
    it.each(['test', 'hallo', 'hi', '?', '...', 'Hallo du', 'ok danke', 'Thank you', 'Guten Morgen', 'tests'])('finds no question in %s', prompt => {
        expect(noQuestion(prompt, known)).toBe(true);
    });
    it.each(['warum?', 'why', 'JSONBAgg', 'jsonbagg', 'test_jsonb_agg', 'explain', 'who calls it?', 'What does this do?', 'mehr', 'und?', 'test JSONBAgg'])('finds a question or a name in %s', prompt => {
        expect(noQuestion(prompt, known)).toBe(false);
    });
});
