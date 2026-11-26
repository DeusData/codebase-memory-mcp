import { describe, expect, it } from 'vitest';
import type { BrowserChatContext, BrowserChatReaderContext } from './chat-model';
import { prepareExplanationContext } from './explanation-context';
import { selectionEvidenceContext } from '../galaxy/selection-evidence';
import { JSONB_AGG_CALLERS, jsonbAggEvidence, jsonbAggScope, largeFolderScope } from './galaxy-evidence.fixture';

function reader(text: string, kind: 'file' | 'selection' = 'file'): BrowserChatReaderContext {
    const lines = text.split('\n');
    return { project: 'sample', path: 'src/example.ts', status: 'ready', source: {
        id: 'reader', text, kind, project: 'sample', path: 'src/example.ts', sourceVersion: 'sha256:example',
        startLine: 1, startColumn: 1, endLine: lines.length, endColumn: lines.at(-1)!.length + 1,
    } };
}

function graph(selected: unknown, relationships?: unknown, rest: Record<string, unknown> = {}): BrowserChatContext {
    return { id: 'snapshot', label: 'Example', text: JSON.stringify({ evidence: {
        kind: 'current-selection-evidence', project: 'sample', view: 'galaxy', source: 'indexed relationships', generation: 'generation-1',
        selected, relationships, limitations: { indexCoverage: 'unavailable' }, ...rest,
    }, omissions: [] }) };
}

const outputText = (prepared: ReturnType<typeof prepareExplanationContext>) => [...prepared.evidence.map(item => item.text), ...prepared.limitations].join('\n');

describe('bounded explanation evidence', () => {
    it('preserves small selections literally, including Unicode, CRLF, and the original range', () => {
        const text = '\tcall("🌿")\r\n  value + 1';
        const context = reader(text, 'selection');
        Object.assign(context.source!, { startLine: 7, startColumn: 4, endLine: 8, endColumn: 12 });
        const prepared = prepareExplanationContext(context);
        expect(prepared.evidence.filter(item => item.source === 'code')).toMatchObject([{ text,
            location: { path: 'src/example.ts', startLine: 7, startColumn: 4, endLine: 8, endColumn: 12 } }]);
        context.source!.text = 'CHANGED';
        expect(prepared.evidence[0].text).toBe(text);
        expect(prepared.limitations.join(' ')).not.toContain('omitted source');
    });

    it('reports explicit character omissions for a selection too large for the budget without splitting Unicode', () => {
        const text = '🌿'.repeat(10_000);
        const prepared = prepareExplanationContext(reader(text, 'selection'), undefined, 1600);
        const code = prepared.evidence.filter(item => item.source === 'code');
        expect(code).toHaveLength(1);
        expect(text.startsWith(code[0].text)).toBe(true);
        expect(code[0].text.length % 2).toBe(0);
        expect(prepared.characterCount).toBeLessThanOrEqual(1600);
        expect(prepared.limitations.join(' ')).toContain(`${text.length - code[0].text.length} source characters omitted`);
        expect(code[0].location?.endColumn).toBe(code[0].text.length + 1);
    });

    it('samples anchored source regions across a large file with exact ranges and omission counts', () => {
        const lines = Array.from({ length: 8000 }, (_, index) => `// filler at ${index + 1}`);
        lines[100] = 'import { first } from "./first";';
        lines[2700] = 'export function middleOne() { return first(); }';
        lines[5300] = 'export function middleTwo() { return middleOne(); }';
        lines[7998] = 'export function finalDeclaration() { return middleTwo(); }';
        const text = lines.join('\n');
        const prepared = prepareExplanationContext(reader(text));
        const code = prepared.evidence.filter(item => item.source === 'code');
        expect(code.length).toBeGreaterThanOrEqual(3);
        expect(code.map(item => item.text).join('\n')).toContain(lines[100]);
        expect(code.map(item => item.text).join('\n')).toContain(lines[7998]);
        for (const item of code) {
            const range = item.location!;
            const start = lines.slice(0, range.startLine - 1).reduce((length, line) => length + line.length + 1, 0) + range.startColumn - 1;
            const end = lines.slice(0, range.endLine - 1).reduce((length, line) => length + line.length + 1, 0) + range.endColumn - 1;
            expect(item.text).toBe(text.slice(start, end));
        }
        const shown = code.reduce((length, item) => length + item.text.length, 0);
        expect(prepared.limitations.join(' ')).toContain(`${text.length - shown} source characters omitted`);
        expect(prepared.limitations.join(' ')).toMatch(/not the full file/i);
        expect(prepared.characterCount).toBeLessThanOrEqual(4000);
    });

    it('falls back to distributed line windows when a language has no recognized declarations', () => {
        const text = Array.from({ length: 1000 }, (_, index) => `value_${index}: ${index}`).join('\n');
        const prepared = prepareExplanationContext(reader(text), undefined, 2000);
        const code = prepared.evidence.filter(item => item.source === 'code');
        expect(code[0].location?.startLine).toBe(1);
        expect(code.at(-1)!.location!.startLine).toBeGreaterThan(900);
        expect(prepared.characterCount).toBeLessThanOrEqual(2000);
    });

    it('keeps graph identity and aggregate counts, but only a few relationships and members', () => {
        const selected = { name: 'Handler', kind: 'Class', members: Array.from({ length: 24 }, (_, id) => ({ id, name: `member${id}` })), omittedMembers: 76 };
        const relationships = { typeCounts: { CALLS: 120, IMPORTS: 8 }, count: 128, omitted: 104,
            items: Array.from({ length: 24 }, (_, id) => ({ id, type: 'CALLS', source: { name: 'Handler' }, target: { name: `target${id}` } })) };
        const prepared = prepareExplanationContext(undefined, graph(selected, relationships));
        const text = outputText(prepared);
        expect(text).toContain('Handler');
        expect(text).toContain('CALLS: 120');
        expect(text).toContain('IMPORTS: 8');
        expect(text).toContain('target0');
        expect(text).not.toContain('target23');
        expect(text).toContain('104');
        expect(text).toContain('76');
        expect(text).toMatch(/omitted/);
        expect(text).toContain('come from reading the code; they do not show what runs at runtime');
        expect(prepared.characterCount).toBeLessThanOrEqual(4000);
        expect(prepared.fallback.length).toBeLessThanOrEqual(400);
    });

    it.each([
        ['architecture-structure', { component: { id: 'c', label: 'Component', symbol_count: 5 }, group: { label: 'Group' } }, 'Component'],
        ['architecture-behavior', { operation: { name: 'operation', file_path: 'a.ts', start_line: 3 }, caller: { name: 'caller' }, callee: { name: 'callee' } }, 'operation'],
        ['architecture-services', { service: { name: 'api', image: 'local:latest' }, connection: { source: 'api', target: 'db', type: 'startup' } }, 'api'],
        ['architecture-hotspots', { kind: 'file', filePath: 'src/hot.ts', count: 9 }, 'src/hot.ts'],
    ])('retains factual selected fields for %s without view-specific purpose claims', (view, selected, identity) => {
        const prepared = prepareExplanationContext(undefined, graph(selected, undefined, { view }));
        expect(outputText(prepared)).toContain(identity);
        // Readable sentences, not field paths such as "Snapshot.view" (K7).
        expect(outputText(prepared)).not.toMatch(/Snapshot\.|Selected\./);
        expect(prepared.fallback).not.toMatch(/responsible for|runs|executes/);
    });

    it('reports missing provenance, unavailable coverage and upstream omissions as limitations', () => {
        const context = graph({ name: 'unverified' }, undefined, { generation: undefined, source: undefined });
        const parsed = JSON.parse(context.text);
        parsed.omissions = [{ path: '$.selected.members', kind: 'items', count: 19 }];
        context.text = JSON.stringify(parsed);
        const prepared = prepareExplanationContext(undefined, context);
        expect(prepared.limitations.join(' ')).toMatch(/provenance.*unavailable/i);
        expect(prepared.limitations.join(' ')).toContain('indexCoverage');
        expect(prepared.limitations.join(' ')).toContain('19');
        expect(prepared.limitations.join(' ')).toContain('unavailable');
    });

    it('rejects malformed and unsupported graph envelopes without promoting arbitrary text to facts', () => {
        for (const text of ['{not-json', JSON.stringify({ selected: 'UNTRUSTED_CONTENT' }), 'UNTRUSTED_CONTENT']) {
            const prepared = prepareExplanationContext(undefined, { id: 'g', label: 'Unknown', text });
            expect(outputText(prepared)).not.toContain('UNTRUSTED_CONTENT');
            expect(prepared.fallback).toMatch(/source unavailable/i);
            expect(prepared.limitations.join(' ')).toMatch(/unsupported|unavailable/i);
        }
    });

    it('does not use stale or mismatched source while still retaining current graph facts', () => {
        for (const context of [{ ...reader('STALE'), status: 'loading' as const }, { ...reader('STALE'), path: 'other.ts' }]) {
            const prepared = prepareExplanationContext(context, graph({ name: 'Current' }));
            expect(outputText(prepared)).not.toContain('STALE');
            expect(outputText(prepared)).toContain('Current');
            expect(prepared.limitations.join(' ')).toMatch(/source unavailable/i);
        }
    });

    it('describes a Galaxy selection as readable facts with every caller, grouped by direction and edge type', () => {
        const prepared = prepareExplanationContext(undefined, jsonbAggEvidence(), 3200);
        const text = outputText(prepared);
        expect(text).toContain('Selected: JSONBAgg (Class) in django/contrib/postgres/aggregates/general.py:50-54.');
        expect(text).toContain('Scope: 1 hop in both directions, all relationship types; 15 symbols and 25 relationships; fully loaded.');
        expect(text).toContain('Incoming: 23 relationships from 12 symbols.');
        const line = (type: string) => text.split('\n').find(item => item.startsWith(`- ${type} `)) ?? '';
        for (const type of ['CALLS', 'TESTS']) {
            expect(line(type)).toMatch(new RegExp(`^- ${type} \\(11\\): `));
            for (const name of JSONB_AGG_CALLERS) expect(line(type)).toContain(name);
            expect(line(type)).toContain('(Method, tests/postgres_tests/test_aggregates.py)');
        }
        expect(line('DEFINES')).toBe('- DEFINES (1): general.py (File, django/contrib/postgres/aggregates/general.py)');
        expect(text).toContain('Outgoing: 2 relationships to 2 symbols.');
        expect(line('INHERITS')).toMatch(/^- INHERITS \(2\): OrderableAggMixin .*Aggregate/);
        // Callers never appear under the outgoing side.
        expect(text.slice(text.indexOf('Outgoing:'))).not.toContain('test_jsonb_agg');
        expect(prepared.capacity).toBeUndefined();
    });

    it('keeps internal snapshot keys, provenance fields and omission paths out of the Galaxy prompt', () => {
        const text = outputText(prepareExplanationContext(undefined, jsonbAggEvidence()));
        expect(text).not.toMatch(/Snapshot|renderedNodes|renderedEdges|generation|rootCount|omittedRoots|qualifiedName|query_graph|\broots\b|\bview\b|\$\.|omissions|typeCounts|\.items\[/);
        expect(text).not.toContain('django-demo.');
    });

    it('lists names until the budget ends, then says how many more there are and reports the capacity', () => {
        const prepared = prepareExplanationContext(undefined, jsonbAggEvidence(), 1900);
        const text = outputText(prepared);
        expect(text).toContain('Incoming: 23 relationships from 12 symbols.');
        const calls = text.split('\n').find(item => item.startsWith('- CALLS (11): '))!;
        const listed = JSONB_AGG_CALLERS.filter(name => calls.includes(`${name},`) || calls.includes(`${name} (`)).length;
        expect(listed).toBeGreaterThan(0);
        expect(calls).toMatch(new RegExp(`; \\+${11 - listed} more$`));
        expect(prepared.capacity).toMatchObject({ nodes: 15, edges: 25 });
        expect(prepared.capacity!.shown).toBeLessThan(25);
        expect(prepared.characterCount).toBeLessThanOrEqual(1900);
    });

    it('classifies before bounding: a large caller set keeps its complete count beyond the named sample', () => {
        const scope = jsonbAggScope();
        const many = Array.from({ length: 60 }, (_, index) => ({ source: 1000 + index, target: 32360, type: 'CALLS' }));
        const context = jsonbAggEvidence({ edges: [...scope.edges, ...many] });
        const text = outputText(prepareExplanationContext(undefined, context, 6000));
        // The extra callers are not in the node list, so they are named by id; the count stays exact.
        expect(text).toContain('- CALLS (71): ');
        expect(text).toMatch(/- CALLS \(71\): .*; \+47 more/);
    });

    it('keeps a large documented scope readable and says when its snapshot cut relationships', () => {
        const whole = outputText(prepareExplanationContext(undefined, selectionEvidenceContext(largeFolderScope()), 6000));
        expect(whole).toContain('Incoming: 150 relationships from 150 symbols.');
        expect(whole).toContain('Outgoing: 150 relationships to 150 symbols.');
        expect(whole).not.toMatch(/Snapshot\.|Selected\.|Relationships\.|incompl/);
        const evidence = largeFolderScope();
        const cut = selectionEvidenceContext({ ...evidence, selected: { ...evidence.selected as object, notes: Array.from({ length: 13 }, () => 'n'.repeat(1200)) } });
        const text = outputText(prepareExplanationContext(undefined, cut, 6000));
        expect(text).toContain('counts and names can be incomplete');
        expect(text).not.toMatch(/Snapshot\.view|to 0 symbols|from 0 symbols/);
    });

    it('says when a side was not loaded instead of reporting no callers', () => {
        const text = outputText(prepareExplanationContext(undefined, jsonbAggEvidence({ direction: 'outbound' })));
        expect(text).toContain('Incoming: not loaded; the scope does not follow incoming edges.');
        expect(text).toContain('outgoing only');
        expect(outputText(prepareExplanationContext(undefined, jsonbAggEvidence({ state: 'loading-partial-preview' }))))
            .toContain('still loading, so this is a partial preview');
    });

    it('bounds hostile giant graph data, source labels and very small requested budgets', () => {
        const context = graph({ name: 'N'.repeat(100_000), members: Array(1000).fill({ detail: 'D'.repeat(1000) }) });
        for (const budget of [0, 100, 800, 4000]) {
            const prepared = prepareExplanationContext(undefined, context, budget);
            expect(prepared.characterCount).toBeLessThanOrEqual(budget);
            expect(prepared.characterCount).toBe(prepared.label.length + prepared.fallback.length
                + prepared.evidence.reduce((count, item) => count + item.text.length, 0)
                + prepared.limitations.reduce((count, item) => count + item.length, 0));
        }
    });
});
