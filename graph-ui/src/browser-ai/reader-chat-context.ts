import type { ReaderDocument } from '../reader/file-source';
import type { ReaderSelection, ReaderStatus } from '../reader/MonacoReader';
import type { BrowserChatReaderContext } from './chat-model';

export interface ReaderSourceOrigin {
    project: string;
    /** Session-local load identifier, not a repository revision. */
    version: string;
}

interface ReaderChatInput {
    project: string;
    path: string;
    status: ReaderStatus;
    document?: ReaderDocument;
    origin?: ReaderSourceOrigin;
    selection?: ReaderSelection;
}

/** Match the literal editor range against this document before narrowing context. */
function matchesSource(selection: ReaderSelection, document: ReaderDocument, lines: string[]): boolean {
    if (selection.path !== document.path || !selection.text.length) return false;
    const start = selection.startLine - document.firstLine;
    const end = selection.endLine - document.firstLine;
    const from = selection.startColumn - 1;
    const to = selection.endColumn - 1;
    if (![start, end, from, to].every(Number.isInteger) || start < 0 || end < start || end >= lines.length
        || from < 0 || to < 0 || from > lines[start].length || to > lines[end].length
        || (start === end && to <= from)) return false;
    const selected = start === end ? lines[start].slice(from, to)
        : [lines[start].slice(from), ...lines.slice(start + 1, end), lines[end].slice(0, to)].join('\n');
    // Monaco normalizes a model's line endings. Compare normalized text but send
    // the actual editor snapshot unchanged, including its original whitespace.
    return selected === selection.text.replace(/\r\n|\r/g, '\n');
}

/** One current source block. A caret alone never narrows the file to a symbol. */
export function readerChatContext({ project, path, status, document, origin, selection }: ReaderChatInput): BrowserChatReaderContext {
    if (!path) return { project, status: 'empty' };
    if (status !== 'ready' || !document || document.path !== path || origin?.project !== project) {
        return { project, path, status: status === 'loading' ? 'loading' : 'unavailable' };
    }
    const lines = document.source.split(/\r\n|\r|\n/);
    if (selection && matchesSource(selection, document, lines)) {
        return { project, path, status: 'ready', source: {
            ...selection, project, kind: 'selection',
            id: `selection:${origin.version}:${selection.sourceVersion}:${selection.startLine}:${selection.startColumn}:${selection.endLine}:${selection.endColumn}`,
        } };
    }
    const missing: string[] = [];
    if (document.firstLine > 1) missing.push(`Lines 1–${document.firstLine - 1} are not loaded.`);
    if (document.truncationNote) missing.push(document.truncationNote);
    else if (document.truncated || (document.fileLastLine !== undefined && document.lastLine < document.fileLastLine)) {
        missing.push(`The reader has only lines ${document.firstLine}–${document.lastLine}.`);
    }
    if (document.fileLastLine === undefined) missing.push('File length is unknown; this is the source currently loaded in the reader.');
    return { project, path, status: 'ready', source: {
        id: `file:${origin.version}`, kind: 'file', project, path,
        text: document.source, sourceVersion: origin.version,
        startLine: document.firstLine, startColumn: 1,
        endLine: document.firstLine + lines.length - 1, endColumn: lines[lines.length - 1].length + 1,
        ...(missing.length ? { partial: missing.join(' ') } : {}),
    } };
}
