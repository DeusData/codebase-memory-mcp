import { describe, expect, it } from 'vitest';
import { readerGraphFocus, symbolMatchesReader } from './reader-graph-focus';
import type { GraphNode } from './types';
import type { SymbolRef } from '../core/focus-protocol';
const nodes: GraphNode[] = [
    { id: 1, name: 'first', label: 'Function', x: 1, y: 2, z: 3, size: 2, color: '#fff', file_path: 'src/a.ts', start_line: 2, end_line: 8 },
    { id: 2, name: 'second', label: 'Function', x: 4, y: 5, z: 6, size: 2, color: '#fff', file_path: 'src/a.ts', start_line: 10, end_line: 20 },
    { id: 3, name: 'other file', label: 'Function', x: 7, y: 8, z: 9, size: 2, color: '#fff', file_path: 'src/b.ts', start_line: 2, end_line: 8 },
];
describe('reader graph focus', () => {
    it('frames every loaded node in the active file when no symbol is resolved', () => {
        expect([...readerGraphFocus(nodes, 'src/a.ts').ids]).toEqual([1, 2]);
    });
    it('narrows to the literal selected source range', () => {
        expect([...readerGraphFocus(nodes, 'src/a.ts', { startLine: 12, endLine: 14 }).ids]).toEqual([2]);
        expect([...readerGraphFocus(nodes, 'src/a.ts', { startLine: 6, endLine: 12 }).ids]).toEqual([1, 2]);
    });
    it('reports missing ranges and files while clearing unrelated highlights', () => {
        expect(readerGraphFocus(nodes, 'src/a.ts', { startLine: 90, endLine: 90 }).message).toContain('Showing the file');
        expect(readerGraphFocus(nodes, 'src/missing.ts').ids.size).toBe(0);
        expect(readerGraphFocus(nodes, 'src/missing.ts').message).toContain('not in the loaded graph');
    });
    it('rejects a stale twin from another file or outside the current selection', () => {
        const symbol: SymbolRef = { name: 'first', uri: 'file:///workspace/src/a.ts', kind: 'function', range: { start: { line: 1, character: 0 }, end: { line: 8, character: 0 } } };
        expect(symbolMatchesReader(symbol, 'src/a.ts', { startLine: 3, endLine: 5 })).toBe(true);
        expect(symbolMatchesReader(symbol, 'src/b.ts')).toBe(false);
        expect(symbolMatchesReader(symbol, 'src/a.ts', { startLine: 10, endLine: 12 })).toBe(false);
    });
});
