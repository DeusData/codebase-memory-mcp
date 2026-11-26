import { describe, expect, it } from 'vitest';
import { chatTopic, followedTopic, missingContextAnswer, questionLanguage, topicHistory } from './chat-context';
import type { BrowserChatTurn } from './chat-model';
import { jsonbAggEvidence } from './galaxy-evidence.fixture';

const attachment = { id: 'a', text: 'x', path: 'src/sum.ts', project: 'p', startLine: 1, startColumn: 1, endLine: 1, endColumn: 2, sourceVersion: 'v' };
const turn = (topic: BrowserChatTurn['topic'], extra: Partial<BrowserChatTurn> = {}): BrowserChatTurn =>
    ({ id: String(Math.random()), prompt: 'q', modelId: 'm', request: [], answer: 'a', status: 'complete', topic, ...extra });

describe('questions without context (K11)', () => {
    it.each([
        ['was kansnt du mir über den code sagen', 'de'], ['was macht das', 'de'], ['Erklär mir die Datei', 'de'],
        ['test', 'en'], ['What does this do?', 'en'], ['explain the code', 'en'], ['Why was this added?', 'en'],
        // Short German questions with typos and greetings (C4).
        ['was macht diese klasse? sehr kurze antwort', 'de'], ['was mcht die klasse', 'de'], ['wass macht dise klase?', 'de'], ['erklaer das', 'de'],
        ['hallo', 'de'], ['was ist das', 'de'], ['kurz bitte', 'de'], ['was tut die funktion', 'de'], ['zeile für zeile', 'de'],
        ['hi', 'en'], ['what is this', 'en'], ['explain this class in detail', 'en'], ['Who calls it?', 'en'],
    ] as const)('answers %s in %s', (prompt, language) => {
        expect(questionLanguage(prompt)).toBe(language);
    });

    it('names what is missing and how to give context, without blaming the model', () => {
        expect(missingContextAnswer('was kannst du mir über den code sagen')).toMatch(/^Es ist nichts ausgewählt.*Wähle einen Knoten in Galaxy.*öffne eine Datei in Explore/);
        expect(missingContextAnswer('test')).toMatch(/^Nothing is selected for me to explain\. Select a node in Galaxy/);
        expect(missingContextAnswer('what is this?', { project: 'p', status: 'empty' })).toMatch(/^No file is open in Explore\./);
        expect(missingContextAnswer('what is this?', { project: 'p', path: 'a.yml', status: 'unavailable' })).toMatch(/^The source of `a\.yml` is not available\./);
        expect(missingContextAnswer('test')).toContain('_Answered without the model');
    });
});

describe('what a question is about', () => {
    it('keys a Galaxy selection by the selected item, not by how its scope is drawn', () => {
        const one = chatTopic('django-demo:galaxy', { graph: jsonbAggEvidence() });
        const deeper = chatTopic('django-demo:galaxy', { graph: jsonbAggEvidence({ depth: 2, direction: 'inbound' }) });
        expect(one).toEqual(deeper);
        expect(one).toMatchObject({ kind: 'graph', label: 'JSONBAgg' });
        expect(chatTopic('django-demo:architecture', { graph: jsonbAggEvidence() })?.key).not.toBe(one?.key);
    });

    it('prefers the open file, then the graph selection, then attached code and context', () => {
        const reader = { project: 'p', path: 'a.yml', status: 'ready' as const, source: { ...attachment, path: 'a.yml', kind: 'file' as const } };
        expect(chatTopic('p:explore', { reader, graph: jsonbAggEvidence(), attachment })?.kind).toBe('file');
        expect(chatTopic('p:galaxy', { attachment, context: [{ id: 'c', label: 'Callers', text: 't' }] })?.kind).toBe('attachment');
        expect(chatTopic('p:galaxy', { context: [{ id: 'c', label: 'Callers', text: 't' }] })).toMatchObject({ kind: 'context', label: 'Callers' });
        expect(chatTopic('p:galaxy', {})).toBeUndefined();
        expect(chatTopic('p:explore', { reader: { project: 'p', path: 'a.yml', status: 'unavailable' } })).toBeUndefined();
    });

    it('follows up on attached code in the same view, never on a selection that is gone', () => {
        const attached = chatTopic('p:galaxy', { attachment })!;
        const selected = chatTopic('p:galaxy', { graph: jsonbAggEvidence() })!;
        expect(followedTopic([turn(attached)], 'p:galaxy')).toEqual(attached);
        expect(followedTopic([turn(attached), turn(undefined, { answeredFrom: 'local' })], 'p:galaxy')).toEqual(attached);
        expect(followedTopic([turn(attached)], 'p:explore')).toBeUndefined();
        expect(followedTopic([turn(attached), turn(selected)], 'p:galaxy')).toBeUndefined();
        expect(followedTopic([], 'p:galaxy')).toBeUndefined();
        expect(followedTopic([turn({ key: '{damaged', label: 'x', kind: 'attachment' })], 'p:galaxy')).toBeUndefined();
    });
});

describe('which earlier turns a question carries (K17)', () => {
    const file = chatTopic('p:explore', { reader: { project: 'p', path: 'a.yml', status: 'ready', source: { ...attachment, path: 'a.yml', kind: 'file' } } })!;
    const selected = chatTopic('p:galaxy', { graph: jsonbAggEvidence() })!;
    it('carries the turns of its own topic, never those of another one', () => {
        const [first, second] = [turn(file), turn(file)];
        expect(topicHistory([first, second], file)).toEqual([first, second]);
        // Coming back to a file after another topic carries its earlier turns again (B4).
        expect(topicHistory([first, turn(selected)], file)).toEqual([first]);
        expect(topicHistory([first, turn(selected), second], file)).toEqual([first, second]);
        expect(topicHistory([turn(selected), first, second], file)).toEqual([first, second]);
    });

    it('steps over replies without a topic, which never start one', () => {
        const [first, second] = [turn(file), turn(file)];
        expect(topicHistory([first, turn(undefined, { answeredFrom: 'local' }), second], file)).toEqual([first, second]);
        expect(topicHistory([first], undefined)).toEqual([]);
    });
});
