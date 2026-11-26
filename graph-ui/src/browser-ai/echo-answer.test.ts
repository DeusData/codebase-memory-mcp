import { describe, expect, it } from 'vitest';
import { echoAnswer } from './echo-answer';

/** "Ich kann dir die Heirarchy erklären." and "Die aktuelle Hierarchie erklärt." were shown as
 * answers: they hold nothing beyond the words of the question (hand test of 2026-10-04, H2). */
describe('answers that only restate the question (H2)', () => {
    it.each([
        ['kkannst du mir die heirarchie erklären', 'Ich kann dir die Heirarchy erklären.'],
        ['erkläre die aktuelle hierarchy', 'Die aktuelle Hierarchie erklärt.'],
        ['erkläre die aktuelle hierarchy', 'Gerne erkläre ich dir die aktuelle Hierarchie!'],
        ['erkläre die aktuelle hierarchy', 'Ich kann dir die aktuelle Struktur des Projekts erklären.'],
        ['explain the hierarchy', 'I can explain the hierarchy.'],
        ['explain the hierarchy', 'Sure, I can explain the current hierarchy for you.'],
        ['what does this view show?', 'This view shows the view.'],
        ['erklär mir das', 'Ich erkläre dir das.'],
        ['Was zeigt diese Ansicht?', 'Ja, ich kann dir zeigen, was diese Ansicht zeigt.'],
    ])('finds no answer to %s in %s', (question, answer) => {
        expect(echoAnswer(answer, question)).toBe(true);
    });

    it.each([
        ['Wie viele Aufrufer hat JSONBAgg?', 'Ja, 11.'],
        ['Hat JSONBAgg 11 Aufrufer?', 'Ja, 11.'],
        ['Ist .github ein Ordner?', 'Ja.'],
        ['Is this a class?', 'No.'],
        ['Wie viele Dateien enthält .github?', '3 Dateien enthält .github.'],
        ['was ist .github?', 'Das ist ein Ordner.'],
        ['what is JSONBAgg?', 'An aggregate for PostgreSQL.'],
        ['erkläre die aktuelle hierarchy', 'Die Hierarchie zeigt `.github` in der Mitte, links `DETACHED` und rechts vier Dateien und den Ordner `workflows`.'],
        ['explain the hierarchy', 'I can explain it: `.github` contains four files and the `workflows` folder.'],
        ['erkläre die aktuelle hierarchy', ''],
    ])('keeps the answer to %s: %s', (question, answer) => {
        expect(echoAnswer(answer, question)).toBe(false);
    });
});
