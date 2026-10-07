import { describe, expect, it } from 'vitest';
import { parseCompactRows, parseSearchResults } from './compact-rows';
import { RpcIntelligenceClient } from './rpc-client';
import { FakeRpc, queryContains, rowsText } from '../test-support/rpc-recordings';
import { loadFileSymbols } from '../twin/selected-code-context';
import functions from './fixtures/application-function.compact.txt?raw';
import classes from './fixtures/application-class.compact.txt?raw';
import functionPage2 from './fixtures/application-function-page2.compact.txt?raw';

// Unmodified responses captured 2026-09-09 from the local PR daemon:
// query_graph project=cbm-pr2068, application.c, Function / Class, LIMIT 200.
const prefix = 'rows_refs: 2  (cols: id prefix)\n  0 project.src.\n  1 src/\nrows_ref_rule: @N+suffix=prefix+suffix\n';
const table = 'rows: 1  (cols: n.qualified_name n.file_path)\n  @0+main @1+main.c\ntotal: 1\n';

describe('compact prefix directories', () => {
    it('decodes the actual 127 function and 8 class replies without losing paths or declaration lines', () => {
        const f = parseCompactRows(functions), c = parseCompactRows(classes);
        expect(f.rows).toHaveLength(127);
        expect(c.rows).toHaveLength(8);
        expect(c.rows[0]).toEqual(['cbm_daemon_application', 'cbm-pr2068.src.daemon.application.cbm_daemon_application',
            'src/daemon/application.c', '171', '200', 'false']);
        expect(f.rows.find(row => row[0] === 'cbm_daemon_application_new')?.slice(1, 4)).toEqual([
            'cbm-pr2068.src.daemon.application.cbm_daemon_application_new', 'src/daemon/application.c', '2909']);
        expect(f).toMatchObject({ returned: 127, total: 144, totalRelation: 'eq', hasMore: true, truncated: true,
            truncationReason: 'output_budget', nextOffset: 127 });
        expect(parseCompactRows(functionPage2)).toMatchObject({ returned: 17, total: 144, offset: 127, hasMore: false, truncated: false });
    });

    it('populates the real inspector reader through the shared client, with no Function/Class query failure', async () => {
        const rpc = new FakeRpc([
            { tool: 'query_graph', when: args => typeof args.cursor === 'string', text: functionPage2 },
            { tool: 'query_graph', when: queryContains('(n:Function)'), text: functions },
            { tool: 'query_graph', when: queryContains('(n:Class)'), text: classes },
            { tool: 'query_graph', text: rowsText(['n.name'], []) },
        ]);
        const result = await loadFileSymbols(new RpcIntelligenceClient({ fetch: rpc.fetch }), 'cbm-pr2068', 'src/daemon/application.c');
        expect(result.symbols).toHaveLength(152);
        expect(result.message).toBe('');
        expect(result.symbols.find(symbol => symbol.name === 'cbm_daemon_application_new')?.uri).toBe('file:///workspace/src/daemon/application.c');
    });

    it('expands empty suffixes and leaves quoted ref-looking literals unchanged', () => {
        const response = prefix + 'rows: 1  (cols: literal expanded)\n  "@0+unchanged" @1+\ntotal: 1\n';
        expect(parseCompactRows(response).rows).toEqual([['@0+unchanged', 'src/']]);
    });

    it('does not recursively expand dictionary declarations', () => {
        const response = 'rows_refs: 1  (cols: id prefix)\n  4 "@99+literal/"\nrows_ref_rule: @N+suffix=prefix+suffix\n'
            + 'rows: 1  (cols: path)\n  @4+file.c\ntotal: 1\n';
        expect(parseCompactRows(response).rows).toEqual([['@99+literal/file.c']]);
    });

    it('keeps unquoted ref-looking values literal when no directory is declared', () => {
        expect(parseCompactRows('rows: 1  (cols: name)\n  @0+literal\ntotal: 1\n').rows).toEqual([['@0+literal']]);
    });

    it.each([
        prefix.replace('  1 src/', '  0 src/'),
        prefix.replace('refs: 2', 'refs: 3'),
        prefix.replace('prefix+suffix', 'suffix+prefix'),
        prefix.replace('  1 src/', '  -1 src/'),
    ])('rejects a malformed directory instead of manufacturing source identities', bad => {
        expect(() => parseCompactRows(bad + table)).toThrow();
    });

    it('rejects unresolved references', () => {
        expect(() => parseCompactRows(prefix + table.replace('@0+main', '@7+main'))).toThrow('unknown reference @7');
    });

    it('uses the same literal/reference distinction for compressed search results', () => {
        const response = prefix.replaceAll('rows_', 'results_')
            + 'results: 1  (cols: qn label file lines rank)\n  @0+main Function @1+main.c 1-9 -4.2\n'
            + 'returned: 1\ntotal: 3\ntotal_relation: gte\nsearch_mode: bm25\nhas_more: true\ntruncated: true\n';
        expect(parseSearchResults(response)).toMatchObject({
            rows: [['project.src.main', 'Function', 'src/main.c', '1-9', '-4.2']], hasMore: true, truncated: true,
        });
    });

    it('retains current cursor and truncation metadata', () => {
        const response = prefix + table.replace('total: 1', 'returned: 1\ntotal: 4\ntotal_relation: eq\nhas_more: true\ntruncated: true\ntruncation_reason: output_budget\nnext_cursor: q1.example\nnext_offset: 1\nwarning: "bounded query"');
        expect(parseCompactRows(response)).toMatchObject({ nextCursor: 'q1.example', nextOffset: 1,
            truncationReason: 'output_budget', warning: 'bounded query', truncated: true, total: 4 });
    });

    it.each(['returned: 2\n', 'total_relation: approximate\n'])('rejects contradictory result metadata', footer => {
        expect(() => parseCompactRows(prefix + table + footer)).toThrow();
    });
});
