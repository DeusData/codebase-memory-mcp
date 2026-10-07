import { describe, expect, it } from 'vitest';
import { chatTopic, contextFreeFollowUp, topicDivider, topicHistory } from './chat-context';
import type { BrowserChatTurn } from './chat-model';
import { jsonbAggEvidence } from './galaxy-evidence.fixture';

const source = { id: 'a', text: 'x', path: 'a.yml', project: 'p', startLine: 1, startColumn: 1, endLine: 1, endColumn: 2, sourceVersion: 'v', kind: 'file' as const };
const file = chatTopic('p:explore', { reader: { project: 'p', path: 'a.yml', status: 'ready', source } })!;
const selected = chatTopic('p:galaxy', { graph: jsonbAggEvidence() })!;
const turn = (topic: BrowserChatTurn['topic'], extra: Partial<BrowserChatTurn> = {}): BrowserChatTurn =>
    ({ id: String(Math.random()), prompt: 'q', modelId: 'm', request: [], answer: 'a', status: 'complete', topic, ...extra });

/** Coming back to a file was called "New topic", its earlier turns were dropped, and "Und was
 * noch?" got "Ja, und noch." (B4). */
describe('a topic the reader comes back to (B4)', () => {
    it('carries the earlier turns of the same topic again, never those of another one', () => {
        const [first, second] = [turn(file), turn(file)];
        const other = turn(selected);
        expect(topicHistory([first, other, second], file)).toEqual([first, second]);
        expect(topicHistory([first, other], file)).toEqual([first]);
        expect(topicHistory([first, other, second], selected)).toEqual([other]);
        expect(topicHistory([first, turn(undefined, { answeredFrom: 'local' }), second], file)).toEqual([first, second]);
    });

    it('marks a new topic and a return to an earlier one, and nothing within a topic', () => {
        const turns = [turn(file), turn(file), turn(selected), turn(selected), turn(file), turn(undefined, { answeredFrom: 'local' }), turn(file)];
        expect(turns.map((_, index) => topicDivider(turns, index))).toEqual([undefined, undefined, 'new', undefined, 'back', undefined, undefined]);
    });

    it.each(['und', 'Und?', 'mehr', 'und was noch?', 'Und was noch?', 'was noch', 'noch mehr', 'und sonst?', 'weiter', 'warum?',
        'and?', 'more', 'what else?', 'anything else', 'and then?', 'go on', 'continue', 'why?'])('reads "%s" as a follow-up without context of its own', prompt => {
        expect(contextFreeFollowUp(prompt)).toBe(true);
    });

    it.each(['Und was macht JSONBAgg?', 'mehr über die Jobs', 'more about the hooks', 'what else does it call?', 'Welche Jobs?', 'test', 'warum ruft es super auf?'])(
        'reads "%s" as a question of its own', prompt => {
            expect(contextFreeFollowUp(prompt)).toBe(false);
        });
});
