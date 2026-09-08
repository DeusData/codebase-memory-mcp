// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { beforeEach, afterEach, expect, it, vi } from 'vitest';
import MonacoReader from './MonacoReader';
import type { ReaderDocument } from './file-source';

const mock = vi.hoisted(() => ({
    text: '\tcall(arg', empty: false,
    selectionChanged: undefined as (() => void) | undefined,
    ask: undefined as (() => void) | undefined,
    model: null as unknown,
    range: { startLineNumber: 2, startColumn: 3, endLineNumber: 3, endColumn: 7, isEmpty: () => false },
    getValueInRange: vi.fn(),
    selectionDispose: vi.fn(), askDispose: vi.fn(),
}));

vi.mock('./monaco-setup', () => ({
    ATLAS_THEME: 'test',
    prepareMonaco: () => ({
        KeyMod: { CtrlCmd: 2048, Shift: 1024 }, KeyCode: { KeyL: 42 },
        Uri: { parse: (value: string) => value },
        editor: {
            EditorOption: { readOnly: 1 }, getModel: () => null,
            createModel: () => ({ id: 'model-7', getVersionId: () => 3, getLineCount: () => 10,
                getValueInRange: mock.getValueInRange, dispose: vi.fn() }),
            create: () => ({
                createDecorationsCollection: () => ({ set: vi.fn() }),
                onDidChangeCursorPosition: () => ({ dispose: vi.fn() }),
                onDidChangeCursorSelection: (callback: () => void) => {
                    mock.selectionChanged = callback; return { dispose: mock.selectionDispose };
                },
                addAction: ({ run }: { run: () => void }) => {
                    mock.ask = run; return { dispose: mock.askDispose };
                },
                getModel: () => mock.model,
                setModel: (model: unknown) => { mock.model = model; },
                getSelection: () => ({ ...mock.range, isEmpty: () => mock.empty }),
                setScrollTop: vi.fn(), dispose: vi.fn(), getOption: () => true,
            }),
        },
    }),
}));

const file: ReaderDocument = {
    path: 'src/example.c', qualifiedName: 'example', qnSource: 'derived',
    derivedQualifiedName: 'example', source: 'first\nsecond\nthird', firstLine: 40,
    lastLine: 42, truncated: false, truncationNote: '',
};
let host: HTMLDivElement;
let root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    mock.selectionChanged = undefined; mock.ask = undefined; mock.model = null; mock.empty = false;
    mock.text = '\tcall(arg\r\n  next';
    mock.getValueInRange.mockReset().mockImplementation(() => mock.text);
    mock.selectionDispose.mockReset(); mock.askDispose.mockReset();
    host = document.createElement('div'); document.body.appendChild(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

it('captures literal partial-line text, whitespace, source range, and editor revision', async () => {
    const onSelectionChange = vi.fn();
    await act(async () => root.render(<MonacoReader status="ready" document={file} message="" onSelectionChange={onSelectionChange} />));
    onSelectionChange.mockClear();
    await act(async () => mock.selectionChanged?.());
    expect(onSelectionChange).toHaveBeenLastCalledWith({
        text: '\tcall(arg\r\n  next', path: file.path,
        startLine: 41, startColumn: 3, endLine: 42, endColumn: 7,
        sourceVersion: 'editor:model-7:3',
    });
    expect(mock.getValueInRange).toHaveBeenCalledWith(expect.objectContaining({ startColumn: 3, endColumn: 7 }));
});

it('captures a fresh snapshot for the keyboard action without replacing an earlier attachment', async () => {
    const onAskSelection = vi.fn();
    await act(async () => root.render(<MonacoReader status="ready" document={file} message="" onAskSelection={onAskSelection} />));
    await act(async () => mock.ask?.());
    expect(onAskSelection).toHaveBeenCalledOnce();
    const snapshot = onAskSelection.mock.calls[0][0];
    mock.text = 'a different selection';
    await act(async () => mock.ask?.());
    expect(snapshot.text).toBe('\tcall(arg\r\n  next');
    expect(onAskSelection.mock.calls[1][0].text).toBe('a different selection');
});

it('clears the draft selection on navigation and ignores an empty selection', async () => {
    const onSelectionChange = vi.fn(), onAskSelection = vi.fn();
    const props = { status: 'ready' as const, message: '', onSelectionChange, onAskSelection };
    await act(async () => root.render(<MonacoReader {...props} document={file} />));
    await act(async () => mock.selectionChanged?.());
    onSelectionChange.mockClear();
    await act(async () => root.render(<MonacoReader {...props} document={{ ...file, path: 'other.c' }} />));
    expect(onSelectionChange).toHaveBeenLastCalledWith(undefined);
    mock.empty = true;
    await act(async () => { mock.selectionChanged?.(); mock.ask?.(); });
    expect(onSelectionChange).toHaveBeenLastCalledWith(undefined);
    expect(onAskSelection).not.toHaveBeenCalled();
});
