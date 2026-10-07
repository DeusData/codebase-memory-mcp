// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';
import { githubFolderEvidence, jsonbAggEvidence } from './galaxy-evidence.fixture';
import { dockHarness } from './chat-dock.fixture';

const dock = dockHarness();

/** With .github selected in the Galaxy hierarchy, "kkannst du mir die heirarchie erklären" and
 * "erkläre die aktuelle hierarchy" went to the free model, which answered "Ich kann dir die
 * Heirarchy erklären." and "Die aktuelle Hierarchie erklärt." (hand test of 2026-10-04, H1, H2). */
describe('questions about the current view are answered from the loaded scope (H1)', () => {
    it('lists the middle, the left and the right of the hierarchy in German, without the model', async () => {
        const { props, runtime } = dock.setup();
        await dock.render({ ...props, proactiveSelection: githubFolderEvidence('hierarchy') }); await dock.load();
        for (const question of ['kkannst du mir die heirarchie erklären', 'erkläre die aktuelle hierarchy']) {
            await dock.ask(question);
            const answer = dock.answerOf(dock.last()).replace(/\s+/g, ' ');
            expect(answer).toContain('.github (Ordner) steht in der Mitte; der Ausschnitt geht 1 Schritt in beide Richtungen.');
            expect(answer).toContain('Links, eingehend: CONTAINS_FOLDER (1): django-demo · detached HEAD');
            expect(answer).toContain('Rechts, ausgehend: CONTAINS_FILE (4): CODE_OF_CONDUCT.md, FUNDING.yml, pull_request_template.md, SECURITY.md CONTAINS_FOLDER (1): workflows');
            expect(answer).toContain('So liest du die Hierarchie: eingehende Beziehungen stehen links, ausgehende rechts.');
            expect(answer).toContain('Aus dem indizierten Graphen gelistet, nicht vom Modell erzeugt.');
            expect(dock.buttonsOf(dock.last())).toEqual(['Modell fragen']);
            expect(dock.notesOf(dock.last())).toEqual([]);
        }
        expect(runtime.countTokens).not.toHaveBeenCalled();
        expect(runtime.chat).not.toHaveBeenCalled();
        // "Modell fragen" asks the model the same question about the same scope, below the list.
        runtime.chat.mockResolvedValueOnce('Die Hierarchie zeigt `.github` mit vier Dateien und dem Ordner `workflows`.');
        await dock.click('Modell fragen', dock.last());
        expect(runtime.chat).toHaveBeenCalledOnce();
        expect(runtime.chat.mock.calls[0][0].at(-1)!.content).toContain('erkläre die aktuelle hierarchy');
        expect(dock.turns()).toHaveLength(3);
        expect(dock.answerOf(dock.last())).toBe('Die Hierarchie zeigt .github mit vier Dateien und dem Ordner workflows.');
    });

    it('says in English that the galaxy view shows the same scope as a cloud', async () => {
        const { props, runtime } = dock.setup();
        await dock.render({ ...props, proactiveSelection: githubFolderEvidence('galaxy') }); await dock.load();
        await dock.ask('what does this view show?');
        const answer = dock.answerOf(dock.last()).replace(/\s+/g, ' ');
        expect(answer).toContain('.github (Folder) is in the middle; the scope reaches 1 hop in both directions.');
        expect(answer).toContain('Outgoing: CONTAINS_FILE (4)');
        expect(answer).toContain('The galaxy view shows this scope as a cloud.');
        expect(answer).not.toContain('Links');
        expect(dock.buttonsOf(dock.last())).toEqual(['Ask the model']);
        expect(runtime.chat).not.toHaveBeenCalled();
    });
});

describe('a model answer that only restates the question is not shown as an answer (H2)', () => {
    it('says the model gave no answer, lists the facts of the selection and offers to ask again', async () => {
        const { props, runtime } = dock.setup();
        runtime.chat.mockResolvedValueOnce('Ich kann dir die Ordner erklären.').mockResolvedValueOnce('Weil GitHub dort Vorlagen und Workflows sucht.');
        await dock.render({ ...props, proactiveSelection: githubFolderEvidence('hierarchy') }); await dock.load();
        await dock.ask('kannst du mir die ordner erklären, die es hier gibt?');
        expect(runtime.chat).toHaveBeenCalledOnce();
        const answer = dock.answerOf(dock.last());
        expect(answer).toContain('Das Modell hat keine Antwort gegeben, es hat nur die Frage wiederholt („Ich kann dir die Ordner erklären.“).');
        expect(answer).toContain('Ausgewählt: .github (Ordner).');
        expect(answer).toContain('Ausgehend: 5 Beziehungen zu 5 Symbolen (CONTAINS_FILE 4, CONTAINS_FOLDER 1).');
        expect(answer).toContain('Aus dem indizierten Graphen gelistet, nicht vom Modell erzeugt.');
        expect(dock.notesOf(dock.last())).toEqual([]);
        expect(dock.buttonsOf(dock.last())).toEqual(['Erneut fragen']);
        // Asked again, the model is told not to restate the question; its real answer replaces the note.
        await dock.click('Erneut fragen', dock.last());
        expect(runtime.chat).toHaveBeenCalledTimes(2);
        expect(runtime.chat.mock.calls[1][0].at(-1)!.content).toContain('Do not restate the question.');
        expect(dock.turns()).toHaveLength(1);
        expect(dock.answerOf(dock.last())).toBe('Weil GitHub dort Vorlagen und Workflows sucht.');
        expect(dock.notesOf(dock.last())).toContain('Vom lokalen Modell erzeugt; kann falsch sein.');
    });

    it('keeps a real short answer', async () => {
        const { props, runtime } = dock.setup();
        runtime.chat.mockResolvedValueOnce('Nein, 4.').mockResolvedValueOnce('Ja, 11.');
        await dock.render({ ...props, proactiveSelection: githubFolderEvidence('hierarchy') }); await dock.load();
        await dock.ask('Sind es mehr als zehn Dateien?');
        expect(dock.answerOf(dock.last())).toBe('Nein, 4.');
        await dock.ask('Wie viele Tests hat das Projekt ungefähr?');
        expect(dock.answerOf(dock.last())).toBe('Ja, 11.');
        expect(dock.notesOf(dock.last())).toContain('Vom lokalen Modell erzeugt; kann falsch sein.');
    });

    it('leaves out a restating sentence under the facts of a general question', async () => {
        const { props, runtime } = dock.setup();
        runtime.chat.mockResolvedValueOnce('Ich erkläre dir das.');
        await dock.render({ ...props, proactiveSelection: jsonbAggEvidence() }); await dock.load();
        await dock.ask('erklär mir das');
        expect(runtime.chat).toHaveBeenCalledOnce();
        const answer = dock.answerOf(dock.last());
        expect(answer).toContain('Ausgewählt: JSONBAgg (Klasse)');
        expect(answer).not.toContain('Ich erkläre dir das.');
        expect(answer).toContain('Der Satz des Modells wurde weggelassen');
    });
});
