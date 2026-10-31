import { describe, expect, it } from 'vitest';
import { selectionEvidenceContext } from '../galaxy/selection-evidence';
import { behaviorMainEvidence, djangoAreaEvidence } from './architecture-evidence.fixture';
import { prepareExplanationContext, selectionSummary } from './explanation-context';
import { explanationMessages, explanationSentence, formatExplanationEvidence } from './explanation-response';
import { jsonbAggEvidence, jsonbAggRenderLimited, largeFolderScope } from './galaxy-evidence.fixture';
import { carriedSource, sourceTargetOf, symbolSource } from './symbol-source';

describe('Architecture selections as readable facts (K7)', () => {
    it('describes a source area in sentences, without field paths a model would list as "Finding a line number"', () => {
        const prompt = formatExplanationEvidence(prepareExplanationContext(undefined, djangoAreaEvidence(), 3200));
        expect(prompt).not.toMatch(/Selected\.|members\[\d+\]|startLine|Snapshot\.|qualifiedName|snapshot items|graph fields omitted/);
        expect(prompt).toContain('Selected source area: `django` (2,310 files · 15,299 indexed nodes).');
        expect(prompt).toContain('528,578 indexed lines in 2,163 measured files of 2,310; files by language: Python 883, HTML 162, Unknown 1,227.');
        expect(prompt).toContain('3 hotspot findings: `create` (`django/apps/config.py:100`) fan-in 1,278, `filter` (`django/db/models/query.py:1487`) fan-in 1,224');
        expect(prompt).toMatch(/Connections to \(root\): CALLS ×1,743/);
        expect(prompt).toContain('Indexed members include `Member0` (Class)');
    });

    it('keeps the connections of an area in the snapshot instead of cutting them behind member documentation', () => {
        const evidence = JSON.parse(djangoAreaEvidence().text);
        expect(evidence.omissions.filter((item: { path: string }) => item.path.startsWith('$.relationships'))).toEqual([]);
        // The 12 strongest connections, each with one example; the rest is counted, not dropped silently.
        expect(evidence.evidence.relationships.items).toHaveLength(12);
        expect(evidence.evidence.relationships.items[0]).toMatchObject({ source: 'django', target: '(root)', type: 'CALLS', count: 1743, omittedEvidence: 26 });
        expect(evidence.evidence.relationships.omitted).toBe(12);
        expect(evidence.evidence.selected.members[0]).toEqual({ name: 'Member0', kind: 'Class' });
    });

    it('describes a Behavior operation with its call sites and takes the source Behavior already read', () => {
        const context = behaviorMainEvidence('def main():\n    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "{{ project_name }}.settings")');
        const prompt = formatExplanationEvidence(prepareExplanationContext(undefined, context, 3200, carriedSource(context)));
        expect(prompt).toContain('Starting operation: `main` (Function) in `django/conf/project_template/manage.py-tpl:7-18`.');
        expect(prompt).toContain("2 direct calls with call-site evidence: line 9 with 'DJANGO_SETTINGS_MODULE', '{{ project_name }}.settings'; line 18 with sys.argv.");
        expect(prompt).toContain('Source django/conf/project_template/manage.py-tpl:7-8, a Python source template:');
        expect(prompt).not.toContain('Source unavailable');
        expect(sourceTargetOf(context)).toBeUndefined();
        expect(sourceTargetOf(behaviorMainEvidence())).toMatchObject({ qualifiedName: 'django-demo.django.conf.project_template.manage.main', path: 'django/conf/project_template/manage.py-tpl' });
    });

    it('names the function each Behavior call reaches, read from the call-site line of the source', () => {
        const main = ['def main():', '    """Run administrative tasks."""', "    os.environ.setdefault('DJANGO_SETTINGS_MODULE', '{{ project_name }}.settings')", '    try:',
            '        from django.core.management import execute_from_command_line', '    except ImportError as exc:', '        raise ImportError(', '            "Could not import Django. Is it installed and"',
            '            "available on your PYTHONPATH? Did you"', '            "forget to activate a virtual environment?"', '        ) from exc', '    execute_from_command_line(sys.argv)'].join('\n');
        const facts = selectionSummary(behaviorMainEvidence(main));
        expect(facts).toContain("2 direct calls with call-site evidence: line 9 calls `os.environ.setdefault` with 'DJANGO_SETTINGS_MODULE', '{{ project_name }}.settings'; line 18 calls `execute_from_command_line` with sys.argv.");
        // A line that does not hold the call's arguments names nothing.
        expect(selectionSummary(behaviorMainEvidence('def main():\n    pass'))).toContain("2 direct calls with call-site evidence: line 9 with 'DJANGO_SETTINGS_MODULE', '{{ project_name }}.settings'; line 18 with sys.argv.");
    });
});

describe('the selected symbol source in a Galaxy explanation (K14)', () => {
    const snippet = { source: 'class JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"\n    allow_distinct = True\n', file_path: '/abs/django/contrib/postgres/aggregates/general.py',
        start_line: 50, end_line: 54, source_mode: 'full' };

    it('names the single selected symbol as the source to read, never a folder or a file', () => {
        expect(sourceTargetOf(jsonbAggEvidence())).toEqual({ qualifiedName: 'django-demo.JSONBAgg', name: 'JSONBAgg', kind: 'Class', path: 'django/contrib/postgres/aggregates/general.py', startLine: 50, endLine: 54 });
        expect(sourceTargetOf(selectionEvidenceContext(largeFolderScope()))).toBeUndefined();
    });

    it('includes the source with its location and drops "Source unavailable" once it exists', () => {
        const source = symbolSource(sourceTargetOf(jsonbAggEvidence())!, snippet, 'g1')!;
        const packet = prepareExplanationContext(undefined, jsonbAggEvidence(), 3200, source);
        const prompt = formatExplanationEvidence(packet);
        expect(prompt).toContain('Source django/contrib/postgres/aggregates/general.py:50-52, a Python source file:\nclass JSONBAgg(OrderableAggMixin, Aggregate):');
        expect(prompt).toContain('Incoming: 23 relationships from 12 symbols.');
        expect(packet.limitations.join('\n')).not.toContain('Source unavailable');
        expect(prepareExplanationContext(undefined, jsonbAggEvidence(), 3200).limitations.join('\n')).toContain('Source unavailable');
    });

    it('bounds a long symbol to 40 lines and says which lines are included', () => {
        const long = { ...snippet, source: Array.from({ length: 433 }, (_, index) => `line ${index}`).join('\n'), start_line: 187, end_line: 619 };
        const source = symbolSource({ qualifiedName: 'q', name: 'BaseCommand', path: 'django/core/management/base.py', startLine: 187, endLine: 619 }, long, 'g1')!;
        expect(source.text.split('\n')).toHaveLength(40);
        expect(source.endLine).toBe(226);
        expect(source.partial).toBe('Only lines 187-226 of BaseCommand (187-619) are included.');
        expect(symbolSource({ qualifiedName: 'q', name: 'x' }, { ...snippet, source: '(source not available)' }, 'g1')).toBeUndefined();
        expect(symbolSource({ qualifiedName: 'q', name: 'x' }, { ...snippet, source_mode: 'outline' }, 'g1')).toBeUndefined();
    });

    it('asks for one sentence and forbids type and input/output claims the source does not show', () => {
        const withSource = explanationMessages(prepareExplanationContext(undefined, jsonbAggEvidence(), 3200, symbolSource(sourceTargetOf(jsonbAggEvidence())!, snippet, 'g1')),
            { name: 'JSONBAgg', kind: 'Class' });
        expect(withSource[0].content).toBe('Answer only from the code you are given. Never state types, parameters, inputs, outputs, return values or purposes that the code does not show.');
        expect(withSource[1].content).toBe('```\nclass JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"\n    allow_distinct = True\n```\n\nDescribe this class in one short sentence that starts with `JSONBAgg`.');
        // The relationship lists are in the card already; next to the code they made the model describe the tests.
        expect(withSource[1].content).not.toContain('test_jsonb_agg');
        const graphOnly = explanationMessages(prepareExplanationContext(undefined, jsonbAggEvidence(), 3200));
        expect(graphOnly[1].content).toContain('No source is available for this selection.');
        expect(graphOnly[1].content).toContain('Do not describe behavior, types, inputs or outputs');
    });

    it('keeps one model sentence and leaves out one that names things the evidence lacks', () => {
        const source = symbolSource(sourceTargetOf(jsonbAggEvidence())!, snippet, 'g1')!;
        const packet = prepareExplanationContext(undefined, jsonbAggEvidence(), 3200, source);
        expect(explanationSentence('`JSONBAgg` sets `function` to "JSONB_AGG" and allows distinct values. It is used by many tests.', packet))
            .toEqual({ sentence: '`JSONBAgg` sets `function` to "JSONB_AGG" and allows distinct values.' });
        expect(explanationSentence('It is checked by the flake8 linter.', packet)).toMatchObject({ dropped: 'unsupported' });
        expect(explanationSentence('It wraps `json_agg_helper` around the query.', packet)).toMatchObject({ dropped: 'unsupported' });
        // With source, an input or output claim must be one the code shows.
        expect(explanationSentence('JSONBAgg is a class that aggregates a list of values.', packet)).toMatchObject({ dropped: 'unsupported' });
        const graphOnly = prepareExplanationContext(undefined, jsonbAggEvidence(), 3200);
        expect(explanationSentence('The test_jsonb_agg test calls it on a list of integers and the output is a list of strings.', graphOnly)).toMatchObject({ dropped: 'unsupported' });
        expect(explanationSentence('JSONBAgg is called by 11 tests.', graphOnly)).toEqual({ sentence: 'JSONBAgg is called by 11 tests.' });
    });

    it('summarizes the selection from the graph: what it is and its relationships by direction and type', () => {
        expect(selectionSummary(jsonbAggEvidence())).toEqual([
            'Selected: JSONBAgg (Class) in django/contrib/postgres/aggregates/general.py:50-54.',
            'Incoming: 23 relationships from 12 symbols (CALLS 11, TESTS 11, DEFINES 1).',
            'Outgoing: 2 relationships to 2 symbols (INHERITS 2).',
            'Scope: 1 hop in both directions, all relationship types; 15 symbols and 25 relationships; fully loaded.',
        ]);
        expect(selectionSummary(djangoAreaEvidence())[0]).toBe('Selected source area: `django` (2,310 files · 15,299 indexed nodes).');
    });

    it('says that a scope stopped at the render limit is partial, in the card and in the prompt (C1)', () => {
        const partial = 'Scope: 3 hops in both directions, all relationship types; 5,548 symbols and 15,673 relationships; '
            + 'partial. Layer 3 stopped loading after the request that took it past the render limit of 5,000 nodes; the scene draws at most 5,000 nodes, '
            + 'so counts and names further out can be incomplete.';
        expect(selectionSummary(jsonbAggRenderLimited()).at(-1)).toBe(partial);
        expect(formatExplanationEvidence(prepareExplanationContext(undefined, jsonbAggRenderLimited(), 3200))).toContain(partial);
        expect(selectionSummary(jsonbAggRenderLimited()).join('\n')).not.toContain('fully loaded');
    });

    it('checks a German sentence for the same unsupported claims and asks for it in German (C5)', () => {
        const source = symbolSource(sourceTargetOf(jsonbAggEvidence())!, snippet, 'g1')!;
        const packet = prepareExplanationContext(undefined, jsonbAggEvidence(), 3200, source);
        for (const sentence of ['JSONBAgg gibt eine Liste von Werten zurück.', 'Die Rückgabe ist ein JSON-Objekt.', 'JSONBAgg nimmt Parameter entgegen.',
            'Die Argumente werden aggregiert.', 'Die Eingabe sind Zeilen.', 'Die Ausgabe ist JSON.', 'Der Datentyp ist JSONB.', 'Sie wird mit einer Liste von Feldern aufgerufen.']) {
            expect(explanationSentence(sentence, packet)).toMatchObject({ dropped: 'unsupported' });
        }
        expect(explanationSentence('JSONBAgg setzt `function` auf "JSONB_AGG" und erlaubt distinct.', packet)).toEqual({ sentence: 'JSONBAgg setzt `function` auf "JSONB_AGG" und erlaubt distinct.' });
        const german = explanationMessages(packet, { name: 'JSONBAgg', kind: 'Class' }, 'de');
        expect(german[1].content.endsWith('Describe this class in one short sentence that starts with `JSONBAgg`. Answer in German.')).toBe(true);
    });

    it('summarizes the selection in German for a German question (C5)', () => {
        expect(selectionSummary(jsonbAggEvidence(), 'de')).toEqual([
            'Ausgewählt: JSONBAgg (Klasse) in django/contrib/postgres/aggregates/general.py:50-54.',
            'Eingehend: 23 Beziehungen von 12 Symbolen (CALLS 11, TESTS 11, DEFINES 1).',
            'Ausgehend: 2 Beziehungen zu 2 Symbolen (INHERITS 2).',
            'Ausschnitt: 1 Schritt in beide Richtungen, alle Beziehungstypen; 15 Symbole und 25 Beziehungen; vollständig geladen.',
        ]);
    });
});
