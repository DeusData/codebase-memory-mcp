import { describe, expect, it } from 'vitest';
import { codeFacts, codeFactsMarkdown, codeSourceFacts } from './code-facts';

const JSONB_AGG = 'class JSONBAgg(OrderableAggMixin, Aggregate):\n    function = "JSONB_AGG"\n    template = "%(function)s(%(distinct)s%(expressions)s %(order_by)s)"\n'
    + '    allow_distinct = True\n    output_field = JSONField()\n';
const ARRAY_AGG = 'class ArrayAgg(OrderableAggMixin, Aggregate):\n    function = "ARRAY_AGG"\n    template = "%(function)s(%(distinct)s%(expressions)s %(order_by)s)"\n'
    + '    allow_distinct = True\n\n    @property\n    def output_field(self):\n        return ArrayField(self.source_expressions[0].output_field)\n';
const TEST_METHOD = '    def test_jsonb_agg(self):\n        values = AggregateTestModel.objects.aggregate(jsonbagg=JSONBAgg("char_field"))\n'
    + '        self.assertEqual(values, {"jsonbagg": ["Foo1", "Foo2", "Foo4", "Foo3"]})\n';
const at = (text: string, path = 'django/contrib/postgres/aggregates/general.py') => ({ text, path });

/** "was macht diese klasse?" got four lines of counts: the model's sentence was always dropped,
 * and nothing said what the code declares (B2). */
describe('facts read from the selected code (B2)', () => {
    it('gives a class its definition line with bases and its class-level assignments', () => {
        expect(codeFacts(at(JSONB_AGG), { name: 'JSONBAgg', kind: 'Class' })).toEqual({ fence: 'python', more: 0, unit: 'members', lines: [
            'class JSONBAgg(OrderableAggMixin, Aggregate):', '    function = "JSONB_AGG"', '    template = "%(function)s(%(distinct)s%(expressions)s %(order_by)s)"',
            '    allow_distinct = True', '    output_field = JSONField()'] });
    });

    it('adds the methods of a class after its assignments, with their decorators, and bounds the lines', () => {
        expect(codeFacts(at(ARRAY_AGG), { name: 'ArrayAgg', kind: 'Class' })?.lines).toEqual(['class ArrayAgg(OrderableAggMixin, Aggregate):', '    function = "ARRAY_AGG"',
            '    template = "%(function)s(%(distinct)s%(expressions)s %(order_by)s)"', '    allow_distinct = True', '    @property', '    def output_field(self):']);
        const big = `class Big(Base):\n    """A big class."""\n${Array.from({ length: 9 }, (_, index) => `    field_${index} = ${index}`).join('\n')}\n\n    def run(self):\n        pass\n`;
        expect(codeFacts(at(big), { name: 'Big', kind: 'Class' })).toEqual({ fence: 'python', more: 6, unit: 'members',
            lines: ['class Big(Base):', '    """A big class."""', '    field_0 = 0', '    field_1 = 1', '    field_2 = 2', '    field_3 = 3'] });
    });

    it('gives a short method its signature and body, without the indent of its class', () => {
        expect(codeFacts(at(TEST_METHOD, 'tests/postgres_tests/test_aggregates.py'), { name: 'test_jsonb_agg', kind: 'Method' })).toEqual({ fence: 'python', more: 0, unit: 'lines', lines: [
            'def test_jsonb_agg(self):', '    values = AggregateTestModel.objects.aggregate(jsonbagg=JSONBAgg("char_field"))',
            '    self.assertEqual(values, {"jsonbagg": ["Foo1", "Foo2", "Foo4", "Foo3"]})'] });
    });

    it('gives a longer function its signature, joined over lines, and the first line of its docstring', () => {
        const long = 'def execute_from_command_line(\n    argv=None,\n):\n    """\n    Run a ManagementUtility.\n\n    More text.\n    """\n'
            + `${Array.from({ length: 6 }, (_, index) => `    step_${index}()`).join('\n')}\n`;
        expect(codeFacts(at(long, 'django/core/management/__init__.py'), { name: 'execute_from_command_line', kind: 'Function' })).toEqual({ fence: 'python', more: 6, unit: 'lines',
            lines: ['def execute_from_command_line(argv=None):', '    """', '    Run a ManagementUtility.'] });
    });

    it('gives code in other languages at least its definition line', () => {
        const ts = '/** Adds. */\nexport function sum(a: number, b: number): number {\n    return a + b;\n}\n';
        expect(codeFacts(at(ts, 'src/sum.ts'), { name: 'sum', kind: 'Function' })).toEqual({ fence: 'typescript', more: 0, unit: 'lines', lines: ['export function sum(a: number, b: number): number'] });
        expect(codeFacts(at('', 'src/sum.ts'), { name: 'sum' })).toBeUndefined();
    });

    it('describes an open code file and marked code by what is read from them (B3)', () => {
        const file = { text: `"""\nPostgreSQL aggregates.\n"""\n\n${JSONB_AGG}\n\n${ARRAY_AGG}\ndef helper():\n    pass\n`, path: 'django/contrib/postgres/aggregates/general.py', kind: 'file' as const, startLine: 1, endLine: 20 };
        expect(codeSourceFacts(file, 'en')).toEqual({ names: ['JSONBAgg', 'ArrayAgg', 'helper'], summary: ['`general.py`: Python source file, 22 lines.',
            'Module docstring: "PostgreSQL aggregates."', 'Top-level definitions (3): `class JSONBAgg`, `class ArrayAgg`, `def helper`.'] });
        const marked = { ...file, kind: 'selection' as const, text: TEST_METHOD, path: 'tests/postgres_tests/test_aggregates.py', startLine: 200, endLine: 202 };
        const one = codeSourceFacts(marked, 'de');
        expect(one.summary).toEqual(['Markierte Zeilen 200 bis 202 von `test_aggregates.py` (Python-Quelltext).']);
        expect(one.code).toContain('Im Quelltext:\n\n```python\ndef test_jsonb_agg(self):');
        const two = codeSourceFacts({ ...marked, text: `${TEST_METHOD}\n    def test_other(self):\n        pass\n`, startLine: 200, endLine: 205 }, 'en');
        expect(two).toEqual({ names: ['test_jsonb_agg', 'test_other'], summary: ['Marked lines 200-205 of `test_aggregates.py` (Python source file).',
            'Definitions in the marked code (2): `def test_jsonb_agg`, `def test_other`.'] });
        expect(codeSourceFacts({ ...marked, text: 'a + b', path: 'src/sum.ts', startLine: 3, endLine: 3 }, 'en').summary).toEqual(['Marked line 3 of `sum.ts` (TypeScript source file).']);
    });

    it('writes the facts as a code block under "In the source:" in the language of the question', () => {
        const facts = codeFacts(at(JSONB_AGG), { name: 'JSONBAgg', kind: 'Class' })!;
        expect(codeFactsMarkdown(facts, 'en')).toBe(`In the source:\n\n\`\`\`python\n${facts.lines.join('\n')}\n\`\`\``);
        expect(codeFactsMarkdown({ ...facts, more: 2 }, 'de')).toBe(`Im Quelltext:\n\n\`\`\`python\n${facts.lines.join('\n')}\n\`\`\`\n\n+2 weitere Attribute und Methoden`);
        expect(codeFactsMarkdown({ ...facts, unit: 'lines', more: 6 }, 'en').endsWith('+6 more lines')).toBe(true);
    });
});
