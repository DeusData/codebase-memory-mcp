import { describe, expect, it } from 'vitest';
import { userMessage } from '../browser-ai/chat-model';
import type { BrowserChatContext } from '../browser-ai/chat-model';
import type { SymbolRef } from '../core/focus-protocol';
import type { ReaderDocument } from '../reader/file-source';
import { CREATE_USER_IR } from '../test-support/twin-fixtures';
import { inspectorChatContext, matchingInspectorIr } from './selected-code-snapshot';
import type { SelectedCodeSnapshot } from './selected-code-snapshot';

const project = CREATE_USER_IR.symbol.projectName!;
const path = 'src/services/userService.ts';
const sourceLines = [
    '// context before the function',
    'export function createUser(input: User) {',
    '  return saveUser(input);',
    '}',
    '// unrelated next declaration',
];

function snapshot(overrides: Partial<SelectedCodeSnapshot> = {}): SelectedCodeSnapshot {
    // Browser symbol URIs use /workspace; project identity travels separately.
    const symbol: SymbolRef = { ...CREATE_USER_IR.symbol, uri: `file:///workspace/${path}`, range: {
        start: { line: 100, character: 0 }, end: { line: 103, character: 0 },
    } };
    const document: ReaderDocument = {
        path, qualifiedName: 'sample.userService', derivedQualifiedName: 'sample.userService',
        qnSource: 'derived', source: sourceLines.join('\n'), firstLine: 100, lastLine: 104,
        truncated: true, truncationNote: 'Only this file window is loaded.',
    };
    return { project, filePath: path, symbol, ir: { ...CREATE_USER_IR, symbol },
        status: 'ready', document, ...overrides };
}

type Payload = {
    project: string;
    path: string;
    symbol?: SymbolRef;
    scope: string;
    source?: { firstLine: number; lastLine: number; text: string; note: string };
    evidence: unknown[];
    symbols?: unknown[];
    fileSymbolsNote?: string;
    coverage?: unknown;
    limitations: string;
};

const payload = (context: BrowserChatContext): Payload => JSON.parse(context.text) as Payload;

describe('matching inspector evidence', () => {
    it('accepts the current project, path, and symbol', () => {
        const current = snapshot();
        expect(matchingInspectorIr(current.ir, current.symbol, project, path)).toBe(current.ir);
    });

    it('rejects a response or selection from a previous project', () => {
        const current = snapshot();
        const staleIr = { ...current.ir!, symbol: { ...current.symbol!, projectName: 'previous-project' } };
        expect(matchingInspectorIr(staleIr, current.symbol, project, path)).toBeUndefined();
        expect(matchingInspectorIr(current.ir, { ...current.symbol!, projectName: 'previous-project' }, project, path)).toBeUndefined();
        expect(matchingInspectorIr(current.ir, current.symbol, 'next-project', path)).toBeUndefined();
    });

    it('rejects stale evidence for a different file or different function in the same file', () => {
        const current = snapshot();
        expect(matchingInspectorIr(current.ir, current.symbol, project, 'src/other.ts')).toBeUndefined();
        const otherFile = { ...current.symbol!, uri: 'file:///workspace/src/other.ts' };
        expect(matchingInspectorIr({ ...current.ir!, symbol: otherFile }, current.symbol, project, path)).toBeUndefined();
        const otherFunction = { ...current.symbol!, name: 'deleteUser', qualifiedName: 'sample.userService.deleteUser',
            nodeId: 'function:deleteUser' };
        expect(matchingInspectorIr(current.ir, otherFunction, project, path)).toBeUndefined();
    });

    it('does not reuse evidence while the next symbol is unresolved', () => {
        const current = snapshot();
        expect(matchingInspectorIr(current.ir, undefined, project, path)).toBeUndefined();
        expect(matchingInspectorIr(undefined, current.symbol, project, path)).toBeUndefined();
    });
});

describe('selected code chat context', () => {
    it('attaches exactly the marked text separately from broader symbol evidence', () => {
        const selection = { path, sourceVersion: 'reader-version-1',
            startLine: 102, startColumn: 3, endLine: 102, endColumn: 27,
            text: '  saveUser(`<user>`, "a");\n\t' };
        const current = snapshot({ selection });
        const context = inspectorChatContext(current, 'selected');
        const body = payload(context);
        expect(body.source).toBeUndefined();
        expect(body.scope).toContain('not just the selected text');
        expect(body.evidence.length).toBeGreaterThan(0);
        expect(context.text).not.toContain(sourceLines[1]);
        const message = userMessage('Explain this exact code', { ...selection, id: 'literal',
            path, project, sourceVersion: 'reader-version-1' }, [context]);
        const exactText = message.split('--- BEGIN EXACT CODE SNAPSHOT ---\n')[1]
            .split('\n--- END EXACT CODE SNAPSHOT ---')[0];
        expect(exactText).toBe(selection.text);
        expect(message).not.toContain(sourceLines[1]);
        expect(message).not.toContain(sourceLines[4]);
    });

    it('extracts complete symbol lines from a file window with an exclusive end range', () => {
        const body = payload(inspectorChatContext(snapshot(), 'symbol'));
        expect(body.source).toEqual({ firstLine: 101, lastLine: 103,
            text: 'export function createUser(input: User) {\n  return saveUser(input);\n}',
            note: expect.stringContaining('not an editor text selection') });
    });

    it('includes the end line when the symbol ends within that line', () => {
        const current = snapshot();
        const symbol = { ...current.symbol!, range: {
            start: { line: 100, character: 0 }, end: { line: 102, character: 1 },
        } };
        const body = payload(inspectorChatContext({ ...current, symbol }, 'symbol'));
        expect(body.source?.lastLine).toBe(103);
        expect(body.source?.text).toBe(sourceLines.slice(1, 4).join('\n'));
    });

    it.each(['missing start', 'missing end', 'different file', 'unloaded'] as const)
    ('omits source when the document is %s', (caseName) => {
        const current = snapshot();
        const document = caseName === 'unloaded' ? undefined : {
            ...current.document!,
            ...(caseName === 'missing start' ? { firstLine: 102, source: sourceLines.slice(2).join('\n') } : {}),
            ...(caseName === 'missing end' ? { lastLine: 102, source: sourceLines.slice(0, 3).join('\n') } : {}),
            ...(caseName === 'different file' ? { path: 'src/other.ts' } : {}),
        };
        const body = payload(inspectorChatContext({ ...current, document }, 'incomplete'));
        expect(body.source).toBeUndefined();
        expect(body.evidence.length).toBeGreaterThan(0);
        expect(body.limitations).toContain('Missing information does not establish absence');
    });

    it('offers file symbols and index limitations without attaching the entire file', () => {
        const current = snapshot({ symbol: undefined, ir: undefined, fileSymbols: [CREATE_USER_IR.symbol],
            fileSymbolsStatus: 'ready', fileSymbolsMessage: 'Showing the first 100 indexed symbols.',
            coverageNote: 'Partial coverage: generated functions are outside this index.' });
        const context = inspectorChatContext(current, 'file');
        const body = payload(context);
        expect(context.label).toBe(`File context · ${path}`);
        expect(body.source).toBeUndefined();
        expect(body.evidence).toEqual([]);
        expect(body.symbols).toEqual([{ name: 'createUser', qualifiedName: CREATE_USER_IR.symbol.qualifiedName,
            kind: 'function', line: 23 }]);
        expect(body.fileSymbolsNote).toBe('Showing the first 100 indexed symbols.');
        expect(body.coverage).toBe('Partial coverage: generated functions are outside this index.');
    });

    it('keeps captured pinned context stable while subsequent reader context changes', () => {
        const retainedSnapshot = snapshot();
        const pinnedContext = inspectorChatContext(retainedSnapshot, 'pinned');
        const originalText = pinnedContext.text;
        retainedSnapshot.document!.source = sourceLines.map(line => line.replaceAll('User', 'Order')).join('\n');
        retainedSnapshot.symbol = { ...retainedSnapshot.symbol!, name: 'createOrder', qualifiedName: 'sample.createOrder' };
        retainedSnapshot.ir = { ...retainedSnapshot.ir!, symbol: retainedSnapshot.symbol };
        const newContext = inspectorChatContext(retainedSnapshot, 'current');
        expect(payload(newContext).source?.text).toContain('createOrder');
        const prompt = userMessage('Explain the pinned function', undefined, [pinnedContext]);
        expect(prompt).toContain('createUser');
        expect(prompt).not.toContain('createOrder');
        expect(pinnedContext.text).toBe(originalText);
    });
});
