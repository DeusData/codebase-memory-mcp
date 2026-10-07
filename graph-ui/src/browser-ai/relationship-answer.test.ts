import { describe, expect, it } from 'vitest';
import { selectionEvidenceContext } from '../galaxy/selection-evidence';
import { JSONB_AGG_CALLERS, jsonbAggEvidence, jsonbAggRenderLimited, largeFolderScope } from './galaxy-evidence.fixture';
import { correctRelationWords, relationshipAnswer, relationshipQuestion, relationshipSuggestion } from './relationship-answer';

const line = (markdown: string, type: string) => markdown.split('\n').find(item => item.startsWith(`- **${type} (`)) ?? '';
/** How a listed answer about JSONBAgg begins: its callers, or that it calls nothing (it only inherits). */
const heading = (side: 'incoming' | 'outgoing', language: 'en' | 'de') => side === 'incoming'
    ? language === 'de' ? '11 Aufrufer (CALLS) von `JSONBAgg` im geladenen Graphen.' : '11 callers (CALLS) of `JSONBAgg` in the loaded graph.'
    : language === 'de' ? '`JSONBAgg` hat in diesem Ausschnitt keine ausgehende CALLS-Kante.' : '`JSONBAgg` has no outgoing CALLS edge in this scope.';

describe('caller and callee questions', () => {
    it.each([
        ['Who calls JSONBAgg?', ['incoming'], 'en', 'JSONBAgg'],
        ['Who calls JSONBAgg? List every caller and the edge type.', ['incoming'], 'en', 'JSONBAgg'],
        ['List the callers of this class', ['incoming'], 'en', undefined],
        ['Where is it called from? Show what calls it.', ['incoming'], 'en', undefined],
        ['What is JSONBAgg called by?', ['incoming'], 'en', 'JSONBAgg'],
        ['Which functions call JSONBAgg?', ['incoming'], 'en', 'JSONBAgg'],
        ['What functions call JSONBAgg?', ['incoming'], 'en', 'JSONBAgg'],
        ['Where is JSONBAgg called?', ['incoming'], 'en', 'JSONBAgg'],
        ['How many callers does JSONBAgg have?', ['incoming'], 'en', undefined],
        ['What does JSONBAgg call?', ['outgoing'], 'en', 'JSONBAgg'],
        ['Which functions does the JSONBAgg class call?', ['outgoing'], 'en', 'JSONBAgg'],
        ['Show the functions called by JSONBAgg', ['outgoing'], 'en', 'JSONBAgg'],
        ['Show the callees', ['outgoing'], 'en', undefined],
        ['Wer ruft JSONBAgg auf?', ['incoming'], 'de', 'JSONBAgg'],
        ['Wer ruft die Funktion auf?', ['incoming'], 'de', undefined],
        ['Welche Aufrufer hat die Klasse?', ['incoming'], 'de', undefined],
        ['Welche Funktion ruft JSONBAgg auf?', ['incoming'], 'de', 'JSONBAgg'],
        ['Welche Funktionen rufen JSONBAgg auf?', ['incoming'], 'de', 'JSONBAgg'],
        ['Welche Tests rufen JSONBAgg auf?', ['incoming'], 'de', 'JSONBAgg'],
        ['Wo wird JSONBAgg aufgerufen?', ['incoming'], 'de', 'JSONBAgg'],
        ['Von wem wird JSONBAgg aufgerufen?', ['incoming'], 'de', 'JSONBAgg'],
        ['Was ruft JSONBAgg auf?', ['outgoing'], 'de', 'JSONBAgg'],
        ['Welche Funktionen ruft JSONBAgg auf?', ['outgoing'], 'de', 'JSONBAgg'],
        ['Was wird von JSONBAgg aufgerufen?', ['outgoing'], 'de', 'JSONBAgg'],
        ['Who calls it and what does it call?', ['incoming', 'outgoing'], 'en', undefined],
        ['Who calls get_queryset?', ['incoming'], 'en', 'get_queryset'],
        ['Wer ruft as_sql auf?', ['incoming'], 'de', 'as_sql'],
        ['What does handle call?', ['outgoing'], 'en', 'handle'],
    ] as const)('recognizes %s', (prompt, sides, language, subject) => {
        expect(relationshipQuestion(prompt)).toEqual({ sides, language, ...subject ? { subject } : {} });
    });

    it.each(['Explain JSONBAgg', 'What does this class do?', 'Was macht diese Klasse?', 'Write a test for JSONBAgg',
        'Explain how the callers use JSONBAgg', 'Why does the caller pass distinct=True?', 'What would break for callers if I rename JSONBAgg?',
        'Who calls JSONBAgg and why?', 'Warum ruft der Test JSONBAgg auf?', 'Wie wird JSONBAgg von den Tests aufgerufen?'])('leaves %s to the model', prompt => {
        expect(relationshipQuestion(prompt)).toBeUndefined();
    });
});

describe('listed relationship answers', () => {
    it('lists the callers first with their count and every other incoming relationship under its own heading (C2)', () => {
        const answer = relationshipAnswer('Who calls JSONBAgg? List every caller and the edge type.', [jsonbAggEvidence()])!;
        expect(answer.markdown).toContain('11 callers (CALLS) of `JSONBAgg` in the loaded graph. All incoming: 23 relationships from 12 symbols.');
        const calls = line(answer.markdown, 'CALLS'), tests = line(answer.markdown, 'TESTS');
        // TESTS and DEFINES are no callers: they follow under their own heading.
        expect(answer.markdown.indexOf(calls)).toBeLessThan(answer.markdown.indexOf('Other incoming relationships:'));
        expect(answer.markdown.indexOf('Other incoming relationships:')).toBeLessThan(answer.markdown.indexOf(tests));
        for (const [text, type] of [[calls, 'CALLS'], [tests, 'TESTS']]) {
            expect(text.startsWith(`- **${type} (11):** `)).toBe(true);
            for (const name of JSONB_AGG_CALLERS) expect(text).toContain(`\`${name}\``);
            expect(text).toContain('(Method, `tests/postgres_tests/test_aggregates.py`)');
            expect(text).not.toContain('more');
        }
        expect(line(answer.markdown, 'DEFINES')).toBe('- **DEFINES (1):** `general.py`, the file that defines `JSONBAgg`');
        expect(answer.markdown).not.toMatch(/\bfrom 11\b/);
        // The outgoing INHERITS edges answer another question.
        expect(answer.markdown).not.toContain('INHERITS');
        expect(answer.markdown).toContain('fully loaded');
        expect(answer.markdown).toContain('not generated by the model');
    });

    it('answers callee questions with the outgoing side only and says when no CALLS edge exists', () => {
        const answer = relationshipAnswer('What does JSONBAgg call?', [jsonbAggEvidence()])!;
        expect(answer.markdown).toContain('`JSONBAgg` has no outgoing CALLS edge in this scope. Outgoing: 2 relationships to 2 symbols.');
        expect(answer.markdown).toContain('`JSONBAgg` has no outgoing CALLS edge in this scope.');
        expect(answer.markdown).toContain('Other outgoing relationships:');
        expect(line(answer.markdown, 'INHERITS')).toMatch(/^- \*\*INHERITS \(2\):\*\* `OrderableAggMixin`.*`Aggregate`/);
        expect(answer.markdown).not.toContain('test_jsonb_agg');
    });

    it('answers in German for a German question', () => {
        const answer = relationshipAnswer('Wer ruft JSONBAgg auf?', [jsonbAggEvidence()])!;
        expect(answer.markdown).toContain('11 Aufrufer (CALLS) von `JSONBAgg` im geladenen Graphen. Alle eingehenden: 23 Beziehungen von 12 Symbolen.');
        expect(line(answer.markdown, 'CALLS')).toMatch(/^- \*\*CALLS \(11\):\*\* `test_default_argument`/);
        expect(answer.markdown.indexOf('Andere eingehende Beziehungen:')).toBeGreaterThan(answer.markdown.indexOf(line(answer.markdown, 'CALLS')));
        expect(line(answer.markdown, 'TESTS')).toMatch(/^- \*\*TESTS \(11\):\*\* /);
        expect(line(answer.markdown, 'DEFINES')).toBe('- **DEFINES (1):** `general.py`, die Datei, die `JSONBAgg` definiert');
        expect(answer.markdown).not.toMatch(/\b(?:CALLS|TESTS|DEFINES) von \d/);
        const callees = relationshipAnswer('Was ruft JSONBAgg auf?', [jsonbAggEvidence()])!.markdown;
        expect(callees).toContain('`JSONBAgg` hat in diesem Ausschnitt keine ausgehende CALLS-Kante. Ausgehend: 2 Beziehungen zu 2 Symbolen.');
        expect(callees).toContain('Andere ausgehende Beziehungen:');
        expect(answer.markdown).toContain('Ausschnitt: 1 Schritt in beide Richtungen');
    });

    it('says in the language of the question that a scope stopped at the render limit, with numbers written in that language (C1)', () => {
        expect(relationshipAnswer('Who calls JSONBAgg?', [jsonbAggRenderLimited()])!.markdown).toContain('Scope: 3 hops in both directions, all relationship types; '
            + '5,548 symbols and 15,673 relationships; partial. Layer 3 stopped loading after the request that took it past the render limit of 5,000 nodes; '
            + 'the scene draws at most 5,000 nodes, so counts and names further out can be incomplete.');
        const german = relationshipAnswer('Wer ruft JSONBAgg auf?', [jsonbAggRenderLimited()])!.markdown;
        expect(german).toContain('Ausschnitt: 3 Schritte in beide Richtungen, alle Beziehungstypen; 5.548 Symbole und 15.673 Beziehungen; '
            + 'unvollständig. Ebene 3 hörte nach der Anfrage auf zu laden, die sie über das Darstellungslimit von 5.000 Knoten brachte; '
            + 'die Szene zeichnet höchstens 5.000 Knoten, daher können Anzahlen und Namen weiter außen fehlen.');
        expect(german).not.toContain('vollständig geladen');
    });

    it('never reports "no callers" for a side the scope did not load or while it is still loading', () => {
        expect(relationshipAnswer('Who calls JSONBAgg?', [jsonbAggEvidence({ direction: 'outbound' })])!.markdown)
            .toContain('does not follow incoming relationships');
        expect(relationshipAnswer('Who calls JSONBAgg?', [jsonbAggEvidence({ depth: 0 })])!.markdown).toContain('Click "Expand +1" in Galaxy');
        expect(relationshipAnswer('Who calls JSONBAgg?', [jsonbAggEvidence({ state: 'loading-partial-preview' })])!.markdown)
            .toContain('still loading; this list can grow');
    });

    it('lists a bounded sample with an explicit remainder when the evidence names fewer than the count', () => {
        const many = Array.from({ length: 40 }, (_, index) => ({ source: 2000 + index, target: 32360, type: 'CALLS' }));
        const answer = relationshipAnswer('Who calls JSONBAgg?', [jsonbAggEvidence({ edges: many })])!;
        expect(line(answer.markdown, 'CALLS')).toMatch(/^- \*\*CALLS \(40\):\*\* .*; \+16 more$/);
    });

    it('answers for a large documented scope with complete counts and every edge type', () => {
        const markdown = relationshipAnswer('What does postgres call?', [selectionEvidenceContext(largeFolderScope())])!.markdown;
        expect(markdown).toContain('`postgres` calls 30 symbols (CALLS) in the loaded graph. All outgoing: 150 relationships to 150 symbols.');
        for (const type of ['CALLS', 'TESTS', 'USAGE', 'IMPORTS', 'DEFINES_METHOD']) expect(line(markdown, type)).toMatch(/^- \*\*\w+ \(30\):\*\* .*; \+\d+ more$/);
        expect(markdown).toContain('fully loaded.');
    });

    it('says so when the snapshot cut relationships instead of claiming a complete list', () => {
        const evidence = largeFolderScope();
        const cut = selectionEvidenceContext({ ...evidence, selected: { ...evidence.selected as object, notes: Array.from({ length: 13 }, () => 'n'.repeat(1200)) } });
        const markdown = relationshipAnswer('What does postgres call?', [cut])!.markdown;
        expect(markdown).not.toMatch(/to 0 symbols/);
        expect(markdown).toContain('the snapshot left part of its relationships out, so counts and names can be incomplete');
        expect(markdown).toMatch(/What `postgres` calls cannot be listed from this scope\. The snapshot left the outgoing relationships out\.|All outgoing: \d+ relationships? to \d+ symbols?\./);
    });

    it('leaves questions about another symbol, attached non-Galaxy evidence and other questions to the model', () => {
        for (const prompt of ['Who calls BaseCommand?', 'Who calls get_queryset?', 'Wer ruft as_sql auf?', 'Who calls Aggregate?', 'What does handle call?',
            'How many callers does get_queryset have?']) expect(relationshipAnswer(prompt, [jsonbAggEvidence()])).toBeUndefined();
        expect(relationshipAnswer('Who calls the JSONBAgg class?', [jsonbAggEvidence()])).toBeDefined();
        expect(relationshipAnswer('Wer ruft django.contrib.postgres.aggregates.general.JSONBAgg auf?', [jsonbAggEvidence()])).toBeDefined();
        expect(relationshipAnswer('Who calls JSONBAgg?', [{ id: 'g', label: 'Callers', text: '{"kind":"graph-context-snapshot"}' }])).toBeUndefined();
        expect(relationshipAnswer('Explain JSONBAgg', [jsonbAggEvidence()])).toBeUndefined();
        expect(relationshipAnswer('Who calls it?', [jsonbAggEvidence()])).toBeDefined();
        expect(relationshipAnswer('Who calls `general.JSONBAgg`?', [jsonbAggEvidence()])).toBeDefined();
    });
});

describe('tolerant caller and callee questions (K16)', () => {
    it.each([
        ['wer ruf jsonbagg auf', ['incoming'], 'de', 'jsonbagg'],
        ['wer ruft jsonbagg auf', ['incoming'], 'de', 'jsonbagg'],
        ['Wer ruft JSONBAgg auf', ['incoming'], 'de', 'JSONBAgg'],
        ['wer rufen jsonbagg auf', ['incoming'], 'de', 'jsonbagg'],
        ['wer rugt jsonbagg auf?', ['incoming'], 'de', 'jsonbagg'],
        ['wer rfut jsonbagg auf?', ['incoming'], 'de', 'jsonbagg'],
        ['wer ruftt jsonbagg auf', ['incoming'], 'de', 'jsonbagg'],
        ['wer ruft eigentlich jsonbagg auf', ['incoming'], 'de', 'jsonbagg'],
        ['von wem wird jsonbagg aufgerufen', ['incoming'], 'de', 'jsonbagg'],
        ['von wem wird jsonbagg aufgerufn', ['incoming'], 'de', 'jsonbagg'],
        ['jsonbagg wird von wem aufgerufen?', ['incoming'], 'de', 'jsonbagg'],
        ['wo wird jsonbagg aufgeruffen', ['incoming'], 'de', 'jsonbagg'],
        ['wer hat jsonbagg aufgerufen', ['incoming'], 'de', 'jsonbagg'],
        ['aufrufer von jsonbagg', ['incoming'], 'de', 'jsonbagg'],
        ['aufrufr von jsonbagg', ['incoming'], 'de', 'jsonbagg'],
        ['was ruf jsonbagg auf', ['outgoing'], 'de', 'jsonbagg'],
        ['who cals jsonbagg', ['incoming'], 'en', 'jsonbagg'],
        ['who call JSONBAgg', ['incoming'], 'en', 'JSONBAgg'],
        ['who calsl JSONBAgg?', ['incoming'], 'en', 'JSONBAgg'],
        ['who is calling JSONBAgg', ['incoming'], 'en', 'JSONBAgg'],
        ['caller of jsonbagg', ['incoming'], 'en', 'jsonbagg'],
        ['calers of jsonbagg', ['incoming'], 'en', 'jsonbagg'],
        ['what does jsonbagg cal', ['outgoing'], 'en', 'jsonbagg'],
        ['wer rft jsonbagg auf', ['incoming'], 'de', 'jsonbagg'],
        ['wer uft jsonbagg auf', ['incoming'], 'de', 'jsonbagg'],
    ] as const)('recognizes %s despite typos, conjugation or a missing question mark', (prompt, sides, language, subject) => {
        expect(relationshipQuestion(prompt)).toEqual({ sides, language, subject });
        const markdown = relationshipAnswer(prompt, [jsonbAggEvidence()])?.markdown ?? '';
        expect(markdown).toContain('`JSONBAgg`');
        expect(markdown).toContain(heading(sides[0], language));
    });

    it.each(['falls jsonbagg fehlt, was passiert?', 'Erkläre jsonbagg', 'who calls jsonbagg and why', 'auf jsonbagg', 'cells of jsonbagg',
        'what is jsonbagg', 'warum ruft der test jsonbagg auf', 'Wie wird JSONBAgg aufgerufen?', 'all jsonbagg tests'])('does not take %s for a list question', prompt => {
        expect(relationshipQuestion(prompt)).toBeUndefined();
        expect(relationshipSuggestion(prompt, [jsonbAggEvidence()])).toBeUndefined();
    });

    it.each([
        ['jsonbagg aufrufe?', 'de', 'incoming'],
        ['aufrufe jsonbagg', 'de', 'incoming'],
        ['wer jsonbagg ruft', 'de', 'incoming'],
        ['calls jsonbagg', 'en', 'incoming'],
        ['jsonbagg calls', 'en', 'outgoing'],
        ['jsonbagg ruft', 'de', 'outgoing'],
    ] as const)('offers the listed answer as a suggestion when %s sounds like a relationship question', (prompt, language, side) => {
        expect(relationshipQuestion(prompt)).toBeUndefined();
        const suggestion = relationshipSuggestion(prompt, [jsonbAggEvidence()])!;
        expect(suggestion.markdown).toContain(language === 'de'
            ? `Meintest du: ${side === 'incoming' ? 'Aufrufer von' : 'von'} \`JSONBAgg\`` : `Did you mean: ${side === 'incoming' ? 'callers of' : 'what'} \`JSONBAgg\``);
        expect(relationshipAnswer(suggestion.question, [jsonbAggEvidence()])?.markdown)
            .toContain(heading(side, language));
    });

    it.each([
        ['Does this class call super?', 'en', 'outgoing'],
        ['does it call anything', 'en', 'outgoing'],
        ['list the calls in this class', 'en', 'outgoing'],
        ['calls from this function', 'en', 'outgoing'],
        ['calls to this class', 'en', 'incoming'],
    ] as const)('reads the direction of %s from the words around the selection', (prompt, language, side) => {
        const suggestion = relationshipSuggestion(prompt, [jsonbAggEvidence()]);
        expect(suggestion?.language).toBe(language);
        expect(suggestion?.markdown).toContain(side === 'incoming' ? 'Did you mean: callers of `JSONBAgg`?' : 'Did you mean: what `JSONBAgg` calls?');
    });

    it.each(['Hat diese Klasse Luft?', 'Riecht diese Klasse nach Duft?', 'summarize the calls in this class', 'Fasse die Aufrufe dieser Klasse zusammen',
        'Is this class calm?'])('neither lists nor suggests for %s', prompt => {
        expect(relationshipQuestion(prompt)).toBeUndefined();
        expect(relationshipSuggestion(prompt, [jsonbAggEvidence()])).toBeUndefined();
    });

    it('keeps real words one edit away from a relation word', () => {
        expect(correctRelationWords('Luft Duft ruht rust raft calm calf mall')).toBe('Luft Duft ruht rust raft calm calf mall');
    });

    it('says in which language a listed answer and a suggestion reply', () => {
        expect(relationshipAnswer('wer ruft jsonbagg auf', [jsonbAggEvidence()])?.language).toBe('de');
        expect(relationshipAnswer('Who calls JSONBAgg?', [jsonbAggEvidence()])?.language).toBe('en');
        expect(relationshipSuggestion('jsonbagg aufrufe?', [jsonbAggEvidence()])?.language).toBe('de');
    });

    it('suggests nothing for a question about another symbol', () => {
        expect(relationshipSuggestion('calls BaseCommand', [jsonbAggEvidence()])).toBeUndefined();
        expect(relationshipSuggestion('BaseCommand aufrufe', [jsonbAggEvidence()])).toBeUndefined();
    });
});

describe('caller questions with a misspelled selection or a free word order (K16)', () => {
    const evidence = () => [jsonbAggEvidence()];
    const listedAnswer = (prompt: string) => relationshipAnswer(prompt, evidence())?.markdown ?? '';

    // The order of the words is free in German: "wo" asks where, it is never the subject.
    it.each([
        ['jsonbagg wird wo aufgerufen', 'de'],
        ['jsonbagg wird wo aufgerufen?', 'de'],
        ['JSONBAgg wird wo überall aufgerufen', 'de'],
        ['wo wird jsonbagg aufgerufen', 'de'],
        ['wo wird jsonbagg überall aufgerufen?', 'de'],
        ['wo überall wird jsonbagg aufgerufen', 'de'],
        ['von wo wird jsonbagg aufgerufen', 'de'],
        ['von wo aus wird jsonbagg aufgerufen', 'de'],
        ['woher wird jsonbagg aufgerufen', 'de'],
        ['jsonbagg wird von wo aufgerufen', 'de'],
        ['jsonbagg wird von wem aufgerufen', 'de'],
        ['jsonbagg wird aufgerufen von wem?', 'de'],
        ['wird jsonbagg irgendwo aufgerufen', 'de'],
        ['wer ruft jsonbagg auf', 'de'],
        ['Wer ruft JSONBAgg eigentlich auf?', 'de'],
        ['welche tests rufen jsonbagg auf', 'de'],
        ['aufrufer von jsonbagg?', 'de'],
        ['where is jsonbagg called', 'en'],
        ['where is jsonbagg called from?', 'en'],
        ['jsonbagg is called by whom?', 'en'],
        ['jsonbagg is called from where', 'en'],
        ['who calls jsonbagg', 'en'],
        ['what calls jsonbagg?', 'en'],
        ['callers of jsonbagg', 'en'],
        ['list the callers of JSONBAgg', 'en'],
        ['who uses jsonbagg', 'en'],
    ] as const)('lists the callers for %s', (prompt, language) => {
        const question = relationshipQuestion(prompt);
        expect(question?.sides).toEqual(['incoming']);
        expect(question?.language).toBe(language);
        expect(question?.subject?.toLowerCase()).not.toMatch(/^(?:wo|woher|irgendwo|überall|where|whom)$/);
        expect(listedAnswer(prompt)).toContain(heading('incoming', language));
    });

    // A typo in the selected name, up to two edits and in any case: never straight to the model.
    it.each([
        ['wer ruft JSONBAg auf', 'de', 'incoming'],
        ['wer ruft jsonbgg auf?', 'de', 'incoming'],
        ['wer ruft jsnbagg auf', 'de', 'incoming'],
        ['wer ruft JSONBAGG auf', 'de', 'listed'],
        ['wer ruft jsonbaggg auf', 'de', 'incoming'],
        ['wer ruft jsonbga auf', 'de', 'incoming'],
        ['wer ruft jsonag auf', 'de', 'incoming'],
        ['wo wird jsonbag aufgerufen', 'de', 'incoming'],
        ['jsonbag wird wo aufgerufen', 'de', 'incoming'],
        ['von wem wird jsnbagg aufgerufen', 'de', 'incoming'],
        ['aufrufer von jsonbgg', 'de', 'incoming'],
        ['was ruft jsonbgg auf', 'de', 'outgoing'],
        ['who calls jsonbag', 'en', 'incoming'],
        ['callers of jsonbag', 'en', 'incoming'],
        ['who calls JSONBAg?', 'en', 'incoming'],
        ['Who calls jsonbagh', 'en', 'incoming'],
        ['where is jsonbgg called', 'en', 'incoming'],
        ['what does jsonbag call?', 'en', 'outgoing'],
        ['callees of jsonbga', 'en', 'outgoing'],
        ['jsonbag aufrufe?', 'de', 'incoming'],
        ['jsonbgg calls', 'en', 'outgoing'],
    ] as const)('answers or suggests the selection for %s', (prompt, language, expected) => {
        const listed = relationshipAnswer(prompt, evidence());
        const suggestion = relationshipSuggestion(prompt, evidence());
        if (expected === 'listed') {
            expect(listed?.markdown).toContain(heading('incoming', language));
            return;
        }
        expect(listed).toBeUndefined();
        expect(suggestion?.language).toBe(language);
        expect(suggestion?.markdown).toContain(language === 'de'
            ? expected === 'incoming' ? 'Meintest du: Aufrufer von `JSONBAgg`?' : 'Meintest du: von `JSONBAgg` aufgerufene Symbole?'
            : expected === 'incoming' ? 'Did you mean: callers of `JSONBAgg`?' : 'Did you mean: what `JSONBAgg` calls?');
        // The uncertain part is the name, and the reply says so.
        if (relationshipQuestion(prompt)) expect(suggestion?.markdown).toContain(language === 'de' ? 'entspricht nicht dem Namen der Auswahl (`JSONBAgg`)' : 'does not match the name of the selection (`JSONBAgg`)');
        // The suggested question lists the selection when chosen.
        expect(relationshipAnswer(suggestion!.question, evidence())?.markdown).toContain(heading(expected, language));
    });

    it.each([
        'who calls jsonb', 'wer ruft json auf', 'who calls Aggregate', 'who calls OrderableAggMixin', 'Wer ruft BaseCommand auf?',
        'who calls test_jsonb_agg', 'wo wird BaseCommand aufgerufen', 'BaseCommand wird wo aufgerufen', 'Who calls JSONBAgg and why?',
    ])('neither lists nor suggests the selection for %s, which names another symbol or asks why', prompt => {
        expect(relationshipAnswer(prompt, evidence())).toBeUndefined();
        expect(relationshipSuggestion(prompt, evidence())).toBeUndefined();
    });
});
