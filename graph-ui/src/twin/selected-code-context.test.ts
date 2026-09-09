import { describe, expect, it, vi } from 'vitest';
import { EngineError } from '../provider/engine-errors';
import { loadFileSymbols } from './selected-code-context';

const row = (name: string, line: number, path = 'src/main.ts', qn = `app.${name}`) => ({
    'n.name': name, 'n.qualified_name': qn, 'n.file_path': path,
    'n.start_line': String(line), 'n.end_line': String(line + 40), 'n.is_test': 'false',
});

describe('file symbol context', () => {
    it('queries fixed labels in parallel for the exact escaped file and project', async () => {
        const pending: ((value: Record<string, string>[]) => void)[] = [];
        const queryRows = vi.fn((_project: string, _query: string) => new Promise<Record<string, string>[]>((resolve) => pending.push(resolve)));
        const result = loadFileSymbols({ queryRows }, 'selected-project', 'src/user"s file.ts');
        expect(queryRows).toHaveBeenCalledTimes(5);
        expect(queryRows.mock.calls.every((args) => args[0] === 'selected-project')).toBe(true);
        for (const [, query] of queryRows.mock.calls) {
            expect(query).toContain('n.file_path = "src/user\\"s file.ts"');
            expect(query).toContain('LIMIT 200');
        }
        pending.forEach((resolve) => resolve([]));
        await expect(result).resolves.toEqual({ symbols: [], message: '' });
    });

    it('sorts and deduplicates declaration targets without inventing full spans', async () => {
        const queryRows = vi.fn(async (_project: string, query: string) => query.includes('n:Function')
            ? [row('last', 90), row('first', 4)] : query.includes('n:Method') ? [row('first', 4), row('middle', 18)] : []);
        const result = await loadFileSymbols({ queryRows }, 'preview', 'src/main.ts');
        expect(result.symbols.map((symbol) => symbol.name)).toEqual(['first', 'middle', 'last']);
        expect(result.symbols[0]).toMatchObject({ kind: 'function', projectName: 'preview',
            uri: 'file:///workspace/src/main.ts', range: { start: { line: 3, character: 0 } } });
        expect(result.symbols[0].range.end.line).toBe(4);
        expect(result.message).toBe('');
    });

    it('retains successful rows and distinguishes absent labels from failures', async () => {
        const queryRows = vi.fn(async (_project: string, query: string) => {
            if (query.includes('n:Function')) return [row('survives', 6)];
            if (query.includes('n:Interface')) throw new EngineError('query_graph', "Unknown node label 'Interface'");
            if (query.includes('n:Method')) throw new EngineError('query_graph', 'permission denied');
            return [];
        });
        const result = await loadFileSymbols({ queryRows }, 'preview', 'src/main.ts');
        expect(result.symbols.map((symbol) => symbol.name)).toEqual(['survives']);
        expect(result.message).toContain('Partial results: queries failed for Method.');
        expect(result.message).toContain('Labels unavailable in this index: Interface.');
    });

    it('throws when all requests fail instead of claiming that the file has no symbols', async () => {
        const queryRows = vi.fn(async () => { throw new Error('connection lost'); });
        await expect(loadFileSymbols({ queryRows }, 'preview', 'src/main.ts')).rejects.toThrow('Could not load file symbols');
    });

    it('reports both a query ceiling and a combined display ceiling', async () => {
        const queryRows = vi.fn(async (_project: string, query: string) => query.includes('n:Function')
            ? Array.from({ length: 200 }, (_, index) => row(`function${index}`, index + 1))
            : query.includes('n:Class') ? [row('extra', 250)] : []);
        const result = await loadFileSymbols({ queryRows }, 'preview', 'src/main.ts');
        expect(result.symbols).toHaveLength(200);
        expect(result.message).toContain('Function reached the 200-row query limit');
        expect(result.message).toContain('Showing the first 200 returned symbols');
    });

    it('omits rows from a different file and unknown declaration positions with an explicit warning', async () => {
        const queryRows = vi.fn(async (_project: string, query: string) => query.includes('n:Function')
            ? [row('valid', 2), row('other', 3, 'src/other.ts'), row('missing', 0)] : []);
        const result = await loadFileSymbols({ queryRows }, 'preview', 'src/main.ts');
        expect(result.symbols.map((symbol) => symbol.name)).toEqual(['valid']);
        expect(result.message).toContain('2 results without a valid declaration');
    });

    it('rejects missing project identity before making requests', async () => {
        const queryRows = vi.fn();
        await expect(loadFileSymbols({ queryRows }, '', 'src/main.ts')).rejects.toThrow('Select a project');
        expect(queryRows).not.toHaveBeenCalled();
    });
});
