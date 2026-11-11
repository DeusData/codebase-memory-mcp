// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';
import { jsonbAggEvidence } from './galaxy-evidence.fixture';
import { dockHarness, readerOf } from './chat-dock.fixture';

const dock = dockHarness();
const WORKFLOW = 'name: New contributor message\n\non:\n  pull_request_target:\n    types: [opened]\n';
const PATH = '.github/workflows/new_contributor_pr.yml';

/** Back on the same file the divider said "New topic", its earlier turns were not sent, and
 * "Und was noch?" got "Ja, und noch." (B4). */
describe('coming back to a topic and follow-ups without context (B4)', () => {
    it('says "Back to" and sends the earlier turns of that topic again, never those of another one', async () => {
        const { props, runtime } = dock.setup();
        runtime.chat.mockResolvedValueOnce('It uses actions/first-interaction@v1.').mockResolvedValueOnce('JSONBAgg is an aggregate.');
        await dock.render({ ...props, selectionScope: 'django-demo:explore', readerContext: readerOf(WORKFLOW, PATH) }); await dock.load();
        await dock.ask('welche Aktion nutzt dieses File?');
        await dock.render({ ...props, selectionScope: 'django-demo:galaxy', proactiveSelection: jsonbAggEvidence() });
        await dock.ask('Explain JSONBAgg line by line.');
        await dock.render({ ...props, selectionScope: 'django-demo:explore', readerContext: readerOf(WORKFLOW, PATH) });
        await dock.ask('Und was noch?');
        const request = runtime.chat.mock.calls.at(-1)![0];
        expect(dock.questionsIn(request)).toEqual(['welche Aktion nutzt dieses File?', 'Und was noch?']);
        expect(request.filter(message => message.role === 'assistant').map(message => message.content)).toEqual(['It uses actions/first-interaction@v1.']);
        expect(request.map(message => message.content).join('\n')).not.toContain('JSONBAgg');
        expect(dock.dividers()).toEqual(['New topic: JSONBAgg. Earlier messages are not sent with these questions.',
            `Zurück zu: ${PATH}. Die früheren Nachrichten dazu werden wieder mitgeschickt.`]);
    });

    it('says "Back to" in English for an English question', async () => {
        const { props } = dock.setup();
        await dock.render({ ...props, selectionScope: 'django-demo:explore', readerContext: readerOf(WORKFLOW, PATH) }); await dock.load();
        await dock.ask('Which action does it use?');
        await dock.render({ ...props, selectionScope: 'django-demo:galaxy', proactiveSelection: jsonbAggEvidence() });
        await dock.ask('Who calls JSONBAgg?');
        await dock.render({ ...props, selectionScope: 'django-demo:explore', readerContext: readerOf(WORKFLOW, PATH) });
        await dock.ask('Which trigger starts it?');
        expect(dock.dividers().at(-1)).toBe(`Back to: ${PATH}. Earlier messages about it are sent again.`);
    });

    it('answers a follow-up right after a topic change locally and asks for the full question', async () => {
        const { props, runtime } = dock.setup();
        await dock.render({ ...props, selectionScope: 'django-demo:explore', readerContext: readerOf(WORKFLOW, PATH) }); await dock.load();
        await dock.ask('welche Aktion nutzt dieses File?');
        await dock.render({ ...props, selectionScope: 'django-demo:galaxy', proactiveSelection: jsonbAggEvidence() });
        const calls = runtime.chat.mock.calls.length;
        await dock.ask('und was noch?');
        expect(runtime.chat.mock.calls.length).toBe(calls);
        const answer = dock.answerOf(dock.last());
        expect(answer).toContain('„und was noch?“ bezieht sich auf frühere Nachrichten. Die betrafen ein anderes Thema und werden mit Fragen zu JSONBAgg nicht mitgeschickt.');
        expect(answer).toContain('Stell die Frage bitte vollständig, zum Beispiel:');
        for (const example of ['Was ist JSONBAgg?', 'Wer verwendet JSONBAgg?']) expect(answer).toContain(example);
        expect(answer).toContain('Ohne das Modell beantwortet.');
        expect(dock.buttonsOf(dock.last())).toEqual([]);
        await dock.ask('more');
        expect(dock.answerOf(dock.last())).toContain('"more" refers to earlier messages. Those were about another topic and are not sent with questions about JSONBAgg.');
        expect(runtime.chat.mock.calls.length).toBe(calls);
        // The hints are not earlier turns: a real question afterwards goes to the model without them.
        await dock.ask('Explain JSONBAgg line by line.');
        expect(dock.questionsIn(runtime.chat.mock.calls.at(-1)![0])).toEqual(['Explain JSONBAgg line by line.']);
    });

    // Without another topic before it, a follow-up may refer to the explanation card and goes to
    // the model as before ("uses original source for follow-ups" in BrowserChatDock.test.tsx).
    it('reads "mehr" as German', async () => {
        const { props, runtime } = dock.setup();
        await dock.render({ ...props, selectionScope: 'django-demo:galaxy', proactiveSelection: jsonbAggEvidence() }); await dock.load();
        await dock.ask('Who calls JSONBAgg?');
        await dock.render({ ...props, selectionScope: 'django-demo:explore', readerContext: readerOf(WORKFLOW, PATH) });
        await dock.ask('mehr');
        expect(runtime.chat).not.toHaveBeenCalled();
        expect(dock.answerOf(dock.last())).toContain(`„mehr“ bezieht sich auf frühere Nachrichten. Die betrafen ein anderes Thema und werden mit Fragen zu ${PATH} nicht mitgeschickt.`);
        expect(dock.answerOf(dock.last())).toContain('Was macht new_contributor_pr.yml?');
    });
});
