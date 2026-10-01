import { describe, expect, it } from 'vitest';
import type { ReaderDocument } from '../reader/file-source';
import type { ReaderSelection, ReaderStatus } from '../reader/MonacoReader';
import { readerChatContext } from './reader-chat-context';

const document: ReaderDocument = {
    path: 'src/sum.ts', qualifiedName: 'sample.src.sum', qnSource: 'graph-module', derivedQualifiedName: 'sample.src.sum',
    source: 'const a = 1;\r\n\treturn a + 2;\r\n', firstLine: 1, lastLine: 2, fileLastLine: 2,
    truncated: false, truncationNote: '',
};
const input = { project: 'sample', path: document.path, status: 'ready' as const, document, origin: { project: 'sample', version: 'reader-load:1' } };
const selection: ReaderSelection = {
    path: document.path, text: 'a + 2', startLine: 2, startColumn: 9, endLine: 2, endColumn: 14, sourceVersion: 'editor:1:1',
};

describe('automatic Explorer source context', () => {
    it('supplies the entire loaded source without shortening or normalizing it', () => {
        const context = readerChatContext(input);
        expect(context.status).toBe('ready');
        expect(context.source).toMatchObject({ kind: 'file', text: document.source, project: input.project, path: document.path,
            startLine: 1, startColumn: 1, endLine: 3, endColumn: 1, sourceVersion: input.origin.version });
        expect(context.source?.partial).toBeUndefined();
    });

    it('uses only a literal marked range, then restores the file when it is cleared', () => {
        expect(readerChatContext({ ...input, selection }).source).toMatchObject({ ...selection, kind: 'selection' });
        expect(readerChatContext(input).source?.text).toBe(document.source);
    });

    it('preserves multiline selection whitespace and editor line endings', () => {
        const marked = { ...selection, startLine: 1, startColumn: 11, endLine: 2, endColumn: 9, text: '1;\n\treturn ' };
        expect(readerChatContext({ ...input, selection: marked }).source).toMatchObject({ kind: 'selection', text: marked.text });
    });

    it.each([
        { text: '' }, { path: 'another.ts' }, { text: 'stale text' }, { startLine: 0 }, { endLine: 9 },
        { startColumn: 99 }, { endColumn: 99 }, { startLine: 2.5 }, { endColumn: 8 },
    ])('does not narrow the file for an empty, stale or invalid selection %j', patch => {
        expect(readerChatContext({ ...input, selection: { ...selection, ...patch } }).source?.kind).toBe('file');
    });

    it.each(['loading', 'unavailable', 'failed', 'idle'] as ReaderStatus[])('never sends old source while the reader is %s', status => {
        expect(readerChatContext({ ...input, status, selection }).source).toBeUndefined();
    });

    it('never carries old code to another file, project, or closed reader', () => {
        expect(readerChatContext({ ...input, path: 'new.ts', selection }).source).toBeUndefined();
        expect(readerChatContext({ ...input, project: 'other', selection }).source).toBeUndefined();
        expect(readerChatContext({ ...input, document: undefined, selection }).source).toBeUndefined();
        expect(readerChatContext({ ...input, path: '', selection })).toEqual({ project: 'sample', status: 'empty' });
    });

    it('labels missing leading lines and incomplete pages instead of claiming a full file', () => {
        const partial = { ...document, firstLine: 8, lastLine: 9, fileLastLine: 50, truncated: true, truncationNote: 'Lines 10–50 were not loaded.' };
        const source = readerChatContext({ ...input, document: partial }).source!;
        expect(source.text).toBe(document.source);
        expect(source.partial).toContain('Lines 1–7 are not loaded');
        expect(source.partial).toContain('Lines 10–50 were not loaded');
        expect(source.startLine).toBe(8);
        expect(source.endLine).toBe(10);
        expect(readerChatContext({ ...input, document: partial, selection: { ...selection, startLine: 9, endLine: 9 } }).source?.kind).toBe('selection');
    });

    it('discloses unknown file completeness and preserves a large file exactly', () => {
        const large = { ...document, source: 'x'.repeat(50000) + '\n', fileLastLine: undefined };
        const source = readerChatContext({ ...input, document: large }).source!;
        expect(source.text).toBe(large.source);
        expect(source.partial).toContain('File length is unknown');
    });
});
