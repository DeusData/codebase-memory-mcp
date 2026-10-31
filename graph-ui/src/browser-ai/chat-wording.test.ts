import { describe, expect, it } from 'vitest';
import { selectionEvidenceContext } from '../galaxy/selection-evidence';
import { prepareExplanationContext, selectionSummary } from './explanation-context';
import { explanationSentence, formatExplanationEvidence, namesNotIn } from './explanation-response';
import { readGalaxyEvidence } from './galaxy-evidence';
import { jsonbAggEvidence, jsonbAggRenderLimited, largeFolderScope } from './galaxy-evidence.fixture';
import { generalQuestion, noQuestionAnswer } from './question-intent';
import { relationshipAnswer, relationshipQuestion, relationshipSuggestion } from './relationship-answer';
import { browserChatText, evidenceNote, groundedText, relationshipWords, topicText } from './strings';
import { djangoAreaEvidence } from './architecture-evidence.fixture';
import { sourceTargetOf, symbolSource } from './symbol-source';

/* Wording of the chat texts after the third review of the hand test (2026-10-04, W1 to W10). */

describe('the render limit in the scope sentence (W1)', () => {
    const english = 'Scope: 3 hops in both directions, all relationship types; 5,548 symbols and 15,673 relationships; partial. '
        + 'Layer 3 stopped loading after the request that took it past the render limit of 5,000 nodes; the scene draws at most 5,000 nodes, '
        + 'so counts and names further out can be incomplete.';
    const german = 'Ausschnitt: 3 Schritte in beide Richtungen, alle Beziehungstypen; 5.548 Symbole und 15.673 Beziehungen; unvollständig. '
        + 'Ebene 3 hörte nach der Anfrage auf zu laden, die sie über das Darstellungslimit von 5.000 Knoten brachte; die Szene zeichnet höchstens 5.000 Knoten, '
        + 'daher können Anzahlen und Namen weiter außen fehlen.';

    it('says what the Galaxy tooltip says, in the card, the prompt and the listed answers', () => {
        expect(selectionSummary(jsonbAggRenderLimited()).at(-1)).toBe(english);
        expect(formatExplanationEvidence(prepareExplanationContext(undefined, jsonbAggRenderLimited(), 3200))).toContain(english);
        expect(relationshipAnswer('Who calls JSONBAgg?', [jsonbAggRenderLimited()])!.markdown).toContain(english);
        expect(selectionSummary(jsonbAggRenderLimited(), 'de').at(-1)).toBe(german);
        expect(relationshipAnswer('Wer ruft JSONBAgg auf?', [jsonbAggRenderLimited()])!.markdown).toContain(german);
    });

    it('keeps one unit in each sentence: symbols with the scope size, nodes with the render limit', () => {
        for (const text of [english, german]) {
            const sentences = text.split(/(?<=\.) (?=[A-ZÄÖÜ])/);
            expect(sentences).toHaveLength(2);
            expect(sentences[0]).not.toMatch(/nodes|Knoten/);
            expect(sentences[1]).not.toMatch(/symbols|Symbole/);
        }
        expect(english).not.toContain('stopped at the render limit');
        expect(german).not.toContain('hielt am Darstellungslimit');
    });

    it('names the direct relationships when the first layer stopped', () => {
        const first = jsonbAggEvidence({ state: 'render-limit-partial', renderLimit: { layer: 1, kind: 'edges', limit: 15_000 } });
        expect(selectionSummary(first).at(-1)).toContain('Layer 1 stopped loading after the request that took it past the render limit of 15,000 edges; '
            + 'the scene draws at most 15,000 edges, so counts and names can be incomplete, the direct relationships included.');
        expect(selectionSummary(first, 'de').at(-1)).toContain('über das Darstellungslimit von 15.000 Kanten brachte; die Szene zeichnet höchstens 15.000 Kanten, '
            + 'daher können Anzahlen und Namen fehlen, auch bei den direkten Beziehungen.');
    });
});

describe('a scope that finished loading (W4)', () => {
    it('says it is fully loaded, not complete for the whole graph', () => {
        expect(selectionSummary(jsonbAggEvidence()).at(-1)).toBe('Scope: 1 hop in both directions, all relationship types; 15 symbols and 25 relationships; fully loaded.');
        expect(selectionSummary(jsonbAggEvidence(), 'de').at(-1)).toBe('Ausschnitt: 1 Schritt in beide Richtungen, alle Beziehungstypen; 15 Symbole und 25 Beziehungen; vollständig geladen.');
        const listed = relationshipAnswer('Who calls JSONBAgg?', [jsonbAggEvidence()])!.markdown + relationshipAnswer('Wer ruft JSONBAgg auf?', [jsonbAggEvidence()])!.markdown;
        expect(listed).not.toMatch(/complete for the indexed graph|vollständig für den indizierten Graphen/);
        expect(listed).toContain('fully loaded.');
        expect(listed).toContain('vollständig geladen.');
    });
});

describe('relationship counts that read as counts, not as a score (W2)', () => {
    it('names the unit beside each number in the summary, in English and German', () => {
        expect(selectionSummary(jsonbAggEvidence()).slice(1, 3)).toEqual([
            'Incoming: 23 relationships from 12 symbols (CALLS 11, TESTS 11, DEFINES 1).',
            'Outgoing: 2 relationships to 2 symbols (INHERITS 2).',
        ]);
        expect(selectionSummary(jsonbAggEvidence(), 'de').slice(1, 3)).toEqual([
            'Eingehend: 23 Beziehungen von 12 Symbolen (CALLS 11, TESTS 11, DEFINES 1).',
            'Ausgehend: 2 Beziehungen zu 2 Symbolen (INHERITS 2).',
        ]);
    });

    it('heads each edge type of the prompt with its count in parentheses, so the model has no "CALLS from 11" to repeat', () => {
        const prompt = formatExplanationEvidence(prepareExplanationContext(undefined, jsonbAggEvidence(), 3200));
        expect(prompt).toContain('Incoming: 23 relationships from 12 symbols.');
        expect(prompt).toContain('Outgoing: 2 relationships to 2 symbols.');
        expect(prompt).toMatch(/^- CALLS \(11\): test_default_argument/m);
        expect(prompt).toMatch(/^- INHERITS \(2\): OrderableAggMixin/m);
        expect(prompt).not.toMatch(/\b(?:CALLS|TESTS|DEFINES|INHERITS) (?:from|to) \d/);
    });

    it('uses the same words where a side has nothing, was not loaded or was cut', () => {
        expect(selectionSummary(jsonbAggEvidence({ direction: 'outbound' }))[1]).toBe('Incoming: not loaded; the scope does not follow incoming edges.');
        expect(selectionSummary(jsonbAggEvidence({ direction: 'outbound' }), 'de')[1]).toBe('Eingehend: nicht geladen; der Ausschnitt folgt keinen eingehenden Kanten.');
        const lonely = jsonbAggEvidence({ edges: [] });
        expect(selectionSummary(lonely).slice(1, 3)).toEqual(['Incoming: no relationships in this scope.', 'Outgoing: no relationships in this scope.']);
        expect(selectionSummary(lonely, 'de').slice(1, 3)).toEqual(['Eingehend: keine Beziehungen in diesem Ausschnitt.', 'Ausgehend: keine Beziehungen in diesem Ausschnitt.']);
    });
});

describe('the heading of a listed caller or callee answer (W3)', () => {
    const answer = (prompt: string, context = jsonbAggEvidence()) => relationshipAnswer(prompt, [context])!.markdown;
    // Two callers of JSONBAgg with CALLS only, and three calls from it.
    const calling = () => jsonbAggEvidence({ edges: [{ source: 100, target: 32360, type: 'CALLS' }, { source: 101, target: 32360, type: 'CALLS' },
        { source: 32360, target: 7, type: 'CALLS' }, { source: 32360, target: 8, type: 'CALLS' }, { source: 32360, target: 9, type: 'CALLS' }, { source: 32360, target: 7, type: 'INHERITS' }] });

    it('counts the callers it lists and gives all incoming relationships as a sentence of their own', () => {
        expect(answer('Who calls JSONBAgg?').split('\n')[0]).toBe('11 callers (CALLS) of `JSONBAgg` in the loaded graph. All incoming: 23 relationships from 12 symbols.');
        expect(answer('Wer ruft JSONBAgg auf?').split('\n')[0]).toBe('11 Aufrufer (CALLS) von `JSONBAgg` im geladenen Graphen. Alle eingehenden: 23 Beziehungen von 12 Symbolen.');
        // With CALLS as the only type there is nothing more to count.
        expect(answer('Who calls JSONBAgg?', calling()).split('\n')[0]).toBe('2 callers (CALLS) of `JSONBAgg` in the loaded graph.');
    });

    it('says what the selection calls as a full sentence, in English and German', () => {
        expect(answer('What does JSONBAgg call?', calling()).split('\n')[0])
            .toBe('`JSONBAgg` calls 3 symbols (CALLS) in the loaded graph. All outgoing: 4 relationships to 3 symbols.');
        expect(answer('Was ruft JSONBAgg auf?', calling()).split('\n')[0])
            .toBe('`JSONBAgg` ruft im geladenen Graphen 3 Symbole auf (CALLS). Alle ausgehenden: 4 Beziehungen zu 3 Symbolen.');
        expect(answer('Was ruft JSONBAgg auf?').split('\n')[0])
            .toBe('`JSONBAgg` hat in diesem Ausschnitt keine ausgehende CALLS-Kante. Ausgehend: 2 Beziehungen zu 2 Symbolen.');
        expect(answer('What does JSONBAgg call?').split('\n')[0])
            .toBe('`JSONBAgg` has no outgoing CALLS edge in this scope. Outgoing: 2 relationships to 2 symbols.');
        expect(answer('Was ruft JSONBAgg auf?')).not.toContain('Was `JSONBAgg` im geladenen Graphen aufruft.');
    });

    it('heads the other edge types "Andere" in German, also right after "keine ausgehende CALLS-Kante"', () => {
        expect(answer('Was ruft JSONBAgg auf?')).toContain('\n\nAndere ausgehende Beziehungen:\n- **INHERITS (2):**');
        expect(answer('Wer ruft JSONBAgg auf?')).toContain('\n\nAndere eingehende Beziehungen:\n- **TESTS (11):**');
        expect(answer('What does JSONBAgg call?')).toContain('\n\nOther outgoing relationships:\n- **INHERITS (2):**');
        expect(answer('Wer ruft JSONBAgg auf?') + answer('Was ruft JSONBAgg auf?')).not.toContain('Weitere');
    });

    it('says in a full sentence why nothing is listed', () => {
        expect(answer('Who calls JSONBAgg?', jsonbAggEvidence({ direction: 'outbound' })).split('\n')[0]).toMatch(/^The callers of `JSONBAgg` cannot be listed from this scope\. /);
        expect(answer('Was ruft JSONBAgg auf?', jsonbAggEvidence({ direction: 'inbound' })).split('\n')[0]).toMatch(/^Was `JSONBAgg` aufruft, lässt sich aus diesem Ausschnitt nicht auflisten\. /);
        expect(answer('Who calls JSONBAgg?', jsonbAggEvidence({ edges: [] })).split('\n')[0]).toBe('`JSONBAgg` has no callers and no other incoming relationships in this scope.');
        expect(answer('Was ruft JSONBAgg auf?', jsonbAggEvidence({ edges: [] })).split('\n')[0]).toBe('`JSONBAgg` ruft in diesem Ausschnitt nichts auf und hat keine anderen ausgehenden Beziehungen.');
    });
});

describe('why the model sentence was left out (W5)', () => {
    const snippet = { source: 'class JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"\n    allow_distinct = True\n',
        file_path: '/abs/django/contrib/postgres/aggregates/general.py', start_line: 50, end_line: 52, source_mode: 'full' };
    const packet = () => prepareExplanationContext(undefined, jsonbAggEvidence(), 3200, symbolSource(sourceTargetOf(jsonbAggEvidence())!, snippet, 'g1')!);

    it('returns the claim word or the unknown name with the dropped sentence, as the sentence wrote it', () => {
        expect(explanationSentence('This method tests the aggregation of JSONB data using a predefined model and expected output.', packet()))
            .toEqual({ dropped: 'unsupported', reason: { kind: 'claim', text: 'output' } });
        expect(explanationSentence('JSON-BAgg ist eine Aggregation, die die Zeilen in einen JSON-Array umwandelt.', packet()))
            .toEqual({ dropped: 'unsupported', reason: { kind: 'claim', text: 'Array' } });
        expect(explanationSentence('JSONBAgg gibt eine Liste von JSON-Daten zurück.', packet()))
            .toEqual({ dropped: 'unsupported', reason: { kind: 'claim', text: 'gibt … zurück' } });
        expect(explanationSentence('It wraps `json_agg_helper` around the query.', packet()))
            .toEqual({ dropped: 'unsupported', reason: { kind: 'name', text: 'json_agg_helper' } });
        expect(explanationSentence('`JSONBAgg` sets `function` to "JSONB_AGG".', packet())).toEqual({ sentence: '`JSONBAgg` sets `function` to "JSONB_AGG".' });
    });

    it('says in the note what was claimed or named and what it was checked against, in both languages', () => {
        expect(groundedText.en.sentenceDropped({ kind: 'claim', text: 'output' }))
            .toBe('Listed from the indexed graph. The model\'s sentence claimed something the source does not show (here: "output") and was left out.');
        expect(groundedText.de.sentenceDropped({ kind: 'claim', text: 'Array' }))
            .toBe('Aus dem indizierten Graphen gelistet. Der Satz des Modells behauptete etwas, das der Quelltext nicht zeigt (hier: „Array“), und wurde weggelassen.');
        expect(groundedText.en.sentenceDropped({ kind: 'name', text: 'flake8' }))
            .toBe('Listed from the indexed graph. The model\'s sentence named something that is in neither the source nor the facts (here: `flake8`) and was left out.');
        expect(groundedText.de.sentenceDropped({ kind: 'name', text: 'flake8' }))
            .toBe('Aus dem indizierten Graphen gelistet. Der Satz des Modells nannte etwas, das weder im Quelltext noch in den Fakten steht (hier: `flake8`), und wurde weggelassen.');
        expect(browserChatText.fileSentenceDropped({ kind: 'name', text: 'flake8' }))
            .toBe('Read from the file. The model\'s text named something the file does not show (here: `flake8`) and was left out.');
        expect(browserChatText.explanationDropped({ kind: 'name', text: 'flake8' }))
            .toBe('The model\'s explanation named something the source does not show (here: `flake8`) and was left out. Ask a question about the code instead.');
        // Without a known reason the note claims none.
        expect(groundedText.de.sentenceDropped()).not.toMatch(/hier:|Fakten nicht zeigen/);
    });
});

describe('the note about names the answer was not given (W7)', () => {
    const given = 'Selected: JSONBAgg (Class) in django/contrib/postgres/aggregates/general.py:50-54.\nclass JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"';

    it('flags no placeholders, literals or tokens without letters', () => {
        const answer = 'It wraps `SUM`, `AVG`, `%()`, `%s`, `%(distinct)s`, `{}`, `{0}`, `False`, `True`, `None`, `null`, `undefined` and `42` in JavaScript.';
        expect(namesNotIn(answer, given)).toEqual(['SUM', 'AVG', 'JavaScript']);
    });

    it('accepts a product name that only extends a long folder name of the selection, and nothing looser', () => {
        expect(namesNotIn('It runs only on PostgreSQL.', given)).toEqual([]);
        expect(namesNotIn('It calls PostgresHelperFactory and postgres_json_helper.', given)).toEqual(['PostgresHelperFactory', 'postgres_json_helper']);
        // A short segment ("aggregates" is long, "py" is not) never widens the check.
        expect(namesNotIn('It uses PyTorch.', given)).toEqual(['PyTorch']);
    });

    it('shows six names with the rest counted, and says plainly when the answer is likely made up', () => {
        const names = (count: number) => Array.from({ length: count }, (_, index) => `made_up_${index}`);
        expect(browserChatText.unsupportedNames(names(7))).toBe('Not in the source or graph facts this answer was given: made_up_0, made_up_1, made_up_2, '
            + 'made_up_3, made_up_4, made_up_5, +1 more. Check these names before relying on them.');
        expect(browserChatText.unsupportedNames(names(21))).toBe('21 names in this answer are not in the source or graph facts it was given: made_up_0, made_up_1, '
            + 'made_up_2, made_up_3, made_up_4, made_up_5, +15 more. The answer is likely made up; do not rely on it.');
        expect(browserChatText.unsupportedNames(names(2))).toBe('Not in the source or graph facts this answer was given: made_up_0, made_up_1. Check these names before relying on them.');
    });
});

describe('German answers in German, with the controls named as the UI shows them (W8)', () => {
    const line = (markdown: string, type: string) => markdown.split('\n').find(item => item.startsWith(`- **${type} (`)) ?? '';

    it('writes the kinds of symbols in German in German answers and leaves English ones as they are', () => {
        expect(selectionSummary(jsonbAggEvidence(), 'de')[0]).toBe('Ausgewählt: JSONBAgg (Klasse) in django/contrib/postgres/aggregates/general.py:50-54.');
        expect(selectionSummary(jsonbAggEvidence())[0]).toBe('Selected: JSONBAgg (Class) in django/contrib/postgres/aggregates/general.py:50-54.');
        const german = relationshipAnswer('Wer ruft JSONBAgg auf?', [jsonbAggEvidence()])!.markdown;
        expect(line(german, 'CALLS')).toContain('(Methode, `tests/postgres_tests/test_aggregates.py`)');
        expect(german).not.toMatch(/\((?:Method|Class|File|Function),/);
        expect(line(relationshipAnswer('Was ruft JSONBAgg auf?', [jsonbAggEvidence()])!.markdown, 'INHERITS')).toContain('(Klasse, `django/contrib/postgres/aggregates/mixins.py`)');
        expect(line(relationshipAnswer('Who calls JSONBAgg?', [jsonbAggEvidence()])!.markdown, 'CALLS')).toContain('(Method, `tests/postgres_tests/test_aggregates.py`)');
        // The prompt stays English.
        expect(formatExplanationEvidence(prepareExplanationContext(undefined, jsonbAggEvidence(), 3200))).toContain('Selected: JSONBAgg (Class) in');
    });

    it('names the Galaxy controls by their labels when a side or a layer is missing', () => {
        expect(relationshipAnswer('Wer ruft JSONBAgg auf?', [jsonbAggEvidence({ direction: 'outbound' })])!.markdown)
            .toContain('Der aktuelle Ausschnitt folgt keinen eingehenden Beziehungen. Wähle in Galaxy „Incoming“ oder „Both directions“ und frage dann noch einmal.');
        expect(relationshipAnswer('Was ruft JSONBAgg auf?', [jsonbAggEvidence({ direction: 'inbound' })])!.markdown)
            .toContain('Wähle in Galaxy „Outgoing“ oder „Both directions“ und frage dann noch einmal.');
        expect(relationshipAnswer('Who calls JSONBAgg?', [jsonbAggEvidence({ direction: 'outbound' })])!.markdown)
            .toContain('The current scope does not follow incoming relationships. Choose "Incoming" or "Both directions" in Galaxy, then ask again.');
        expect(relationshipAnswer('Who calls JSONBAgg?', [jsonbAggEvidence({ depth: 0 })])!.markdown)
            .toContain('The current scope shows the selection only. Click "Expand +1" in Galaxy, then ask again.');
        expect(relationshipAnswer('Wer ruft JSONBAgg auf?', [jsonbAggEvidence({ depth: 0 })])!.markdown)
            .toContain('Der aktuelle Ausschnitt zeigt nur die Auswahl. Klicke in Galaxy auf „Expand +1“ und frage dann noch einmal.');
    });

    it('has the topic divider in both languages', () => {
        expect(topicText.en.topicBreak('tox.ini')).toBe('New topic: tox.ini. Earlier messages are not sent with these questions.');
        expect(topicText.de.topicBreak('tox.ini')).toBe('Neues Thema: tox.ini. Frühere Nachrichten werden bei diesen Fragen nicht mitgeschickt.');
    });
});

describe('German wording and examples that fit the selection (W9)', () => {
    const jsonb = () => readGalaxyEvidence(jsonbAggEvidence().text)!;

    it('suggests questions to a German prompt without a question in natural German', () => {
        const answer = noQuestionAnswer('test', 'de', { kind: 'galaxy', name: 'JSONBAgg', evidence: jsonb() });
        expect(answer.startsWith('In „test“ wurde keine Frage erkannt. Du kannst zum Beispiel fragen:')).toBe(true);
        expect(answer).not.toContain('Frage zum Beispiel');
    });

    it('offers examples for a class that make sense for a class, and that the chat answers as offered', () => {
        const german = noQuestionAnswer('test', 'de', { kind: 'galaxy', name: 'JSONBAgg', evidence: jsonb() });
        expect(german).toContain('- Was ist JSONBAgg?\n- Wer verwendet JSONBAgg?\n- Wovon erbt JSONBAgg?');
        expect(german).not.toContain('Was ruft JSONBAgg auf?');
        const english = noQuestionAnswer('test', 'en', { kind: 'galaxy', name: 'JSONBAgg', evidence: jsonb() });
        expect(english).toContain('- What is JSONBAgg?\n- Who uses JSONBAgg?\n- What does JSONBAgg inherit from?');
        expect(english).not.toContain('What does JSONBAgg call?');
        expect(generalQuestion('What is JSONBAgg?', ['JSONBAgg'])).toBe('general');
        expect(generalQuestion('Was ist JSONBAgg?', ['JSONBAgg'])).toBe('general');
        expect(relationshipQuestion('Who uses JSONBAgg?')?.sides).toEqual(['incoming']);
        expect(relationshipQuestion('Wer verwendet JSONBAgg?')?.sides).toEqual(['incoming']);
        // A class that inherits nothing gets no question about it.
        const plain = { ...jsonb(), relationships: { ...jsonb().relationships, outgoing: [] } };
        expect(noQuestionAnswer('test', 'en', { kind: 'galaxy', name: 'JSONBAgg', evidence: plain })).not.toContain('inherit');
    });

    it('keeps the call questions for a function and asks what a folder contains', () => {
        const method = { ...jsonb(), roots: [{ ...jsonb().roots[0], kind: 'Method' }] };
        expect(noQuestionAnswer('test', 'en', { kind: 'galaxy', name: 'handle', evidence: method })).toContain('- What does handle do?\n- Who calls handle?\n- What does handle call?');
        const folder = readGalaxyEvidence(selectionEvidenceContext(largeFolderScope()).text)!;
        expect(noQuestionAnswer('hallo', 'de', { kind: 'galaxy', name: 'postgres', evidence: folder })).toContain('- Was enthält postgres?\n- Erklär postgres ausführlich.');
        expect(generalQuestion('Was enthält postgres?', ['postgres'])).toBe('general');
    });

    it('offers the list or the model as a choice, not as an order', () => {
        expect(relationshipSuggestion('jsonbagg aufrufe?', [jsonbAggEvidence()])!.markdown)
            .toContain('_Die Frage wurde nicht sicher erkannt. Du kannst dir die Liste aus dem indizierten Graphen anzeigen lassen oder das Modell fragen._');
        expect(relationshipSuggestion('jsonbagg calls', [jsonbAggEvidence()])!.markdown)
            .toContain('_The question was not recognized for certain. You can show the list from the indexed graph or ask the model._');
        expect(relationshipSuggestion('wer ruft JSONBAg auf', [jsonbAggEvidence()])!.markdown)
            .toContain('_`JSONBAg` entspricht nicht dem Namen der Auswahl (`JSONBAgg`). Du kannst dir die Liste aus dem indizierten Graphen anzeigen lassen oder das Modell fragen._');
        expect(relationshipSuggestion('who calls JSONBAg?', [jsonbAggEvidence()])!.markdown)
            .toContain('_`JSONBAg` does not match the name of the selection (`JSONBAgg`). You can show the list from the indexed graph or ask the model._');
    });

    it('names what defines the selection with the right article and pronoun for every kind', () => {
        expect(relationshipWords.de.definer('File', 1, 'X')).toBe('die Datei, die X definiert');
        expect(relationshipWords.de.definer('Module', 1, 'X')).toBe('das Modul, das X definiert');
        expect(relationshipWords.de.definer('Interface', 1, 'X')).toBe('die Schnittstelle, die X definiert');
        expect(relationshipWords.de.definer('Folder', 1, 'X')).toBe('der Ordner, der X definiert');
        expect(relationshipWords.de.definer('Class', 2, 'X')).toBe('die Klassen, die X definieren');
        expect(relationshipWords.de.definer('Gadget', 1, 'X')).toBe('das Symbol (Gadget), das X definiert');
        expect(relationshipWords.en.definer('Class', 2, 'X')).toBe('the classes that define X');
        expect(relationshipWords.en.definer('Interface', 1, 'X')).toBe('the interface that defines X');
    });
});

describe('the Architecture card (W10)', () => {
    const area = (languages?: { name: string; files: number }[]) => {
        const context = djangoAreaEvidence();
        if (!languages) return context;
        const parsed = JSON.parse(context.text);
        parsed.evidence.selected.measurement.languages = languages.map(item => ({ ...item, lines: item.files * 10, color: '#999' }));
        return { ...context, text: JSON.stringify(parsed) };
    };

    it('writes every number of the card the same way', () => {
        const facts = selectionSummary(area());
        expect(facts[0]).toBe('Selected source area: `django` (2,310 files · 15,299 indexed nodes).');
        // Line numbers after a path ("query.py:1487") are places, not counts.
        expect(facts.join('\n')).not.toMatch(/(?<!:)\b\d{4,}\b/);
    });

    it('lists the files by language largest first, with Unknown last', () => {
        expect(selectionSummary(area())[1]).toBe('528,578 indexed lines in 2,163 measured files of 2,310; files by language: Python 883, HTML 162, Unknown 1,227.');
        expect(selectionSummary(area([{ name: 'Unknown', files: 1227 }, { name: 'Python', files: 883 }, { name: 'CSS', files: 14 }, { name: 'HTML', files: 162 },
            { name: 'JavaScript', files: 22 }]))[1]).toContain('files by language: Python 883, HTML 162, JavaScript 22, CSS 14, Unknown 1,227.');
    });

    it('says plainly that the relationships come from reading the code', () => {
        for (const context of [area(), jsonbAggEvidence()]) {
            const limits = prepareExplanationContext(undefined, context, 3200).limitations;
            expect(limits).toContain('The relationships here come from reading the code; they do not show what runs at runtime.');
            expect(limits.join('\n')).not.toContain('Static graph relationships');
        }
        expect(evidenceNote('The relationships here come from reading the code; they do not show what runs at runtime.', 'de'))
            .toBe('Die Beziehungen hier wurden aus dem Code gelesen; sie zeigen nicht, was zur Laufzeit ausgeführt wird.');
        expect(evidenceNote('Source unavailable.', 'de')).toBe('Source unavailable.');
    });
});
