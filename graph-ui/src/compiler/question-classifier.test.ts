import { describe, expect, it } from 'vitest';

import {
    CLASS_RULES,
    QUESTION_CLASSES,
    SUBJECT_FALLBACK_RULE,
    classifyQuestion,
    identifierNamesIn,
    mentionsIn,
    quotedNamesIn,
} from './question-classifier';
import type { QuestionClass } from './question-classifier';

/**
 * The examples the classifier is held to.
 *
 * Deliberately more than the twenty-five the contract asks for and deliberately
 * mixed German and English, because the eval asks in both and a rule list that
 * only ever saw one language would sort half the golden questions into `other`.
 * Every row is a question somebody could type into the command line; none of
 * them was written backwards from a pattern.
 */
const EXAMPLES: readonly { question: string; klass: QuestionClass }[] = [
    // --- who-calls -----------------------------------------------------------
    { question: 'Wer ruft createUser?', klass: 'who-calls' },
    { question: 'Wer benutzt validateUser eigentlich?', klass: 'who-calls' },
    { question: 'Von wo wird insert aufgerufen?', klass: 'who-calls' },
    { question: 'Who calls listUsers?', klass: 'who-calls' },
    { question: 'What are the callers of query?', klass: 'who-calls' },
    { question: 'Where is this called from?', klass: 'who-calls' },

    // --- what-is -------------------------------------------------------------
    { question: 'Was macht createUser?', klass: 'what-is' },
    { question: 'Was ist ValidationError?', klass: 'what-is' },
    { question: 'Erklaere mir loadConfig.', klass: 'what-is' },
    { question: 'What does hotspotScan do?', klass: 'what-is' },
    { question: 'Explain registerUserRoutes.', klass: 'what-is' },
    { question: '@toUser', klass: 'what-is' },

    // --- what-if -------------------------------------------------------------
    { question: 'Was passiert, wenn ich validateUser aendere?', klass: 'what-if' },
    { question: 'Was waere, wenn insert eine andere Signatur haette?', klass: 'what-if' },
    { question: 'Welche Auswirkung hat eine Aenderung an query?', klass: 'what-if' },
    { question: 'What happens if createUser changes?', klass: 'what-if' },
    { question: 'What is the blast radius of insert?', klass: 'what-if' },

    // --- why-error -----------------------------------------------------------
    { question: 'Welche Fehler kann createUser werfen?', klass: 'why-error' },
    { question: 'Warum schlaegt validateUser fehl?', klass: 'why-error' },
    { question: 'Was fuer eine Ausnahme kommt aus validateId?', klass: 'why-error' },
    { question: 'What errors can getOrder raise?', klass: 'why-error' },
    { question: 'Why does this fail?', klass: 'why-error' },

    // --- where-entry ---------------------------------------------------------
    { question: 'Wo faengt dieses Projekt an?', klass: 'where-entry' },
    { question: 'Welche Routen gibt es?', klass: 'where-entry' },
    { question: 'Was ist der Einstiegspunkt?', klass: 'where-entry' },
    { question: 'Where do I start reading?', klass: 'where-entry' },
    { question: 'Which HTTP route reaches createUser?', klass: 'where-entry' },

    // --- compare -------------------------------------------------------------
    { question: 'Was ist der Unterschied zwischen createUser und create?', klass: 'compare' },
    { question: 'Vergleiche listUsers und query.', klass: 'compare' },
    { question: 'What is the difference between insert and query?', klass: 'compare' },
    { question: 'createUser vs create?', klass: 'compare' },

    // --- overview ------------------------------------------------------------
    { question: 'Gib mir einen Ueberblick.', klass: 'overview' },
    { question: 'Wie ist die Architektur aufgebaut?', klass: 'overview' },
    { question: 'What is this project?', klass: 'overview' },
    { question: 'Show me the big picture.', klass: 'overview' },

    // --- other ---------------------------------------------------------------
    { question: 'Hallo', klass: 'other' },
    { question: 'bitte', klass: 'other' },
    { question: '', klass: 'other' },
];

describe('classifyQuestion', () => {
    it('has at least 25 worked examples across both languages', () => {
        expect(EXAMPLES.length).toBeGreaterThanOrEqual(25);
        const german = EXAMPLES.filter((entry) => /[äöüÄÖÜ]|ae|oe|ue|Wer |Was |Wo |Wie |Welche/.test(entry.question));
        expect(german.length).toBeGreaterThanOrEqual(10);
    });

    for (const example of EXAMPLES) {
        it(`sorts "${example.question}" as ${example.klass}`, () => {
            expect(classifyQuestion(example.question).klass).toBe(example.klass);
        });
    }

    it('reaches every one of the eight classes', () => {
        const reached = new Set(EXAMPLES.map((entry) => entry.klass));
        for (const klass of QUESTION_CLASSES) {
            expect(reached.has(klass)).toBe(true);
        }
    });

    it('is deterministic: the same words always land in the same class', () => {
        for (const example of EXAMPLES) {
            const first = classifyQuestion(example.question, { focusName: 'createUser' });
            const second = classifyQuestion(example.question, { focusName: 'createUser' });
            expect(second).toEqual(first);
        }
    });

    it('names the rule that fired', () => {
        expect(classifyQuestion('Wer ruft createUser?').rule).toBe('who-calls-words');
        expect(classifyQuestion('Hallo').rule).toBe('no-rule');
    });
});

describe('subject resolution', () => {
    it('prefers a mention over everything else', () => {
        const result = classifyQuestion('Was macht @validateUser in createUser?', {
            focusName: 'listUsers',
        });
        expect(result.subject).toBe('validateUser');
        expect(result.subjectFrom).toBe('mention');
        expect(result.mentions).toEqual(['validateUser']);
    });

    it('takes a quoted name when there is no mention', () => {
        const result = classifyQuestion('Was macht `insert`?', { focusName: 'listUsers' });
        expect(result.subject).toBe('insert');
        expect(result.subjectFrom).toBe('quoted');
    });

    it('takes an identifier-shaped word when nothing is quoted', () => {
        const result = classifyQuestion('Wer ruft createUser?', { focusName: 'listUsers' });
        expect(result.subject).toBe('createUser');
        expect(result.subjectFrom).toBe('identifier');
    });

    it('falls back to the focus when the question names nobody', () => {
        const result = classifyQuestion('Wer ruft das hier auf?', { focusName: 'listUsers' });
        expect(result.subject).toBe('listUsers');
        expect(result.subjectFrom).toBe('focus');
    });

    it('has no subject when nothing names one and nothing is in focus', () => {
        const result = classifyQuestion('Wer ruft das auf?');
        expect(result.subject).toBeUndefined();
        expect(result.subjectFrom).toBe('none');
    });

    it('never turns a plain lowercase noun into a symbol', () => {
        const result = classifyQuestion('Was macht der user hier?');
        expect(result.subject).toBeUndefined();
    });

    it('carries a second name only for a comparison', () => {
        const compare = classifyQuestion('Unterschied zwischen createUser und listUsers?');
        expect(compare.subject).toBe('createUser');
        expect(compare.other).toBe('listUsers');
        const single = classifyQuestion('Wer ruft createUser und listUsers?');
        expect(single.other).toBeUndefined();
    });
});

describe('the mode only decides what the words do not', () => {
    it('lets a matching rule beat the open panel', () => {
        expect(classifyQuestion('Wer ruft createUser?', { mode: 'change' }).klass).toBe('who-calls');
    });

    it('uses the open panel when no rule fires', () => {
        expect(classifyQuestion('createUser', { mode: 'bug' }).klass).toBe('why-error');
        expect(classifyQuestion('createUser', { mode: 'change' }).klass).toBe('what-if');
        expect(classifyQuestion('createUser', { mode: 'entry' }).klass).toBe('where-entry');
        expect(classifyQuestion('createUser', { mode: 'tour' }).klass).toBe('overview');
    });

    /*
     * Ohne offenes Panel entscheidet seit dem 2026-08-29 der genannte Name.
     *
     * Bis dahin war ein hingeschriebenes `createUser` ohne Modus `other` und
     * damit null Karten. Der Name ist aber genau die Angabe, die der Leser
     * gemacht hat, und `@createUser` lieferte schon immer what-is; die beiden
     * Schreibweisen derselben Bitte auseinanderlaufen zu lassen war der Fehler.
     */
    it('nimmt ohne Panel den selbst genannten Namen als schwaechste Frage', () => {
        const result = classifyQuestion('createUser', { mode: 'none' });
        expect(result.klass).toBe('what-is');
        expect(result.rule).toBe(SUBJECT_FALLBACK_RULE);
        expect(result.subjectFrom).toBe('identifier');
    });
});

/*
 * Befund 8 des unabhaengigen Audits vom 2026-08-29.
 *
 * Zwei von fuenf legitimen Proben fielen auf `other` und erzeugten null
 * Karten. Beide stehen hier woertlich, und sie stehen mit VERSCHIEDENEN
 * Erwartungen da, weil sie verschiedene Faelle sind: die eine nennt ein
 * Symbol, die andere nur ein Wort.
 */
describe('die zwei Fragen des Audits', () => {
    it('gibt der Frage nach der Laufzeit die Fakten des genannten Symbols', () => {
        const result = classifyQuestion('How many times was createUser called at runtime?');
        expect(result.klass).toBe('what-is');
        expect(result.rule).toBe(SUBJECT_FALLBACK_RULE);
        expect(result.subject).toBe('createUser');
        expect(result.subjectFrom).toBe('identifier');
    });

    /*
     * Und die andere bleibt `other`, mit Absicht. `insert` ist kleingeschrieben
     * und damit ein Wort und keine Symbolform; ein Fallback darauf holte die
     * Fakten von irgendetwas, das der Index `insert` nennt, und legte sie als
     * Antwort auf eine Frage nach der Datenbank hin. Der Index fuehrt ohnehin
     * keine Datenbank-Relation, also ist der ehrliche Keine-Karte-Satz hier
     * die richtige Antwort und nicht die schlechtere.
     */
    it('bleibt bei der Datenbank-Frage ehrlich other, weil sie kein Symbol nennt', () => {
        const result = classifyQuestion('Welche Datenbank benutzt insert?');
        expect(result.klass).toBe('other');
        expect(result.rule).toBe('no-rule');
        expect(result.subject).toBeUndefined();
        expect(result.subjectFrom).toBe('none');
    });

    it('nennt der zweiten Frage mit Symbolform sehr wohl ein Subjekt', () => {
        const result = classifyQuestion('Welche Datenbank benutzt `insert`?');
        expect(result.klass).toBe('what-is');
        expect(result.subject).toBe('insert');
        expect(result.subjectFrom).toBe('quoted');
    });
});

describe('rule ordering', () => {
    it('lets a comparison beat every single-symbol class', () => {
        expect(classifyQuestion('Wer ruft createUser, und was ist der Unterschied zu create?').klass)
            .toBe('compare');
    });

    it('lets a consequence beat a raise', () => {
        expect(classifyQuestion('Was passiert, wenn validateUser wirft?').klass).toBe('what-if');
        expect(classifyQuestion('Was wirft validateUser?').klass).toBe('why-error');
    });

    it('lets a caller question beat a route question', () => {
        expect(classifyQuestion('Wer ruft die Route /users auf?').klass).toBe('who-calls');
    });

    it('keeps the declared rule order', () => {
        expect(CLASS_RULES.map((rule) => rule.klass)).toEqual([
            'compare', 'what-if', 'who-calls', 'why-error', 'where-entry', 'overview', 'what-is',
        ]);
    });
});

describe('the name readers', () => {
    it('reads mentions in order', () => {
        expect(mentionsIn('@a and @b.c')).toEqual(['a', 'b.c']);
        expect(mentionsIn('nothing here')).toEqual([]);
    });

    it('reads quoted names in all three quote styles', () => {
        expect(quotedNamesIn('`a` "b" \'c\'')).toEqual(['a', 'b', 'c']);
    });

    it('reads identifier-shaped words and skips plain ones', () => {
        expect(identifierNamesIn('createUser calls validateUser in the module')).toEqual([
            'createUser', 'validateUser',
        ]);
        expect(identifierNamesIn('src.util.validate is a module')).toEqual(['src.util.validate']);
        expect(identifierNamesIn('this that what')).toEqual([]);
    });
});
