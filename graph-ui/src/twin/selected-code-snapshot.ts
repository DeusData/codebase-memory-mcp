import type { SymbolRef } from '../core/focus-protocol';
import type { SemanticIR } from '../core/semantic-ir';
import type { ReaderSelection } from '../reader/MonacoReader';
import type { ReaderDocument } from '../reader/file-source';
import type { BrowserChatContext } from '../browser-ai/chat-model';
import { browserGraphContext } from '../browser-ai/graph-context';
import type { PseudocodeDocument } from '../pseudocode/pseudocode-builder';
import type { SelectedCodePanelProps } from './SelectedCodePanel';
import { workspacePathOf } from './twin-target';

export type SelectedCodeSnapshot = Pick<SelectedCodePanelProps,
    'filePath' | 'symbol' | 'ir' | 'status' | 'message' | 'imports' | 'coverageNote'
    | 'fileSymbols' | 'fileSymbolsStatus' | 'fileSymbolsMessage'> & {
        project: string;
        selection?: ReaderSelection;
        document?: ReaderDocument;
        pseudocode?: PseudocodeDocument;
    };

/** Keep late responses from lending evidence to a different symbol. */
export function matchingInspectorIr(ir: SemanticIR | undefined, symbol: SymbolRef | undefined,
    project: string, path: string): SemanticIR | undefined {
    if (!ir || !symbol || !path || workspacePathOf(symbol.uri) !== path
        || workspacePathOf(ir.symbol.uri) !== path
        || (symbol.projectName && symbol.projectName !== project)
        || (ir.symbol.projectName && ir.symbol.projectName !== project)) return undefined;
    const identity = (ref: SymbolRef) => ref.qualifiedName || ref.nodeId
        || `${ref.uri}:${ref.name}:${ref.range.start.line}:${ref.range.start.character}`;
    return identity(ir.symbol) === identity(symbol) ? ir : undefined;
}

/** Explicit context for the next prompt; exact text selections travel separately. */
export function inspectorChatContext(snapshot: SelectedCodeSnapshot, id: string): BrowserChatContext {
    const { project, filePath, symbol, ir, selection, document } = snapshot;
    const firstLine = symbol ? symbol.range.start.line + 1 : undefined;
    const lastLine = symbol ? symbol.range.end.line + (symbol.range.end.character > 0 ? 1 : 0) : undefined;
    const sourceAvailable = !selection && document?.path === filePath && firstLine !== undefined
        && lastLine !== undefined && lastLine >= firstLine
        && firstLine >= document.firstLine && lastLine <= document.lastLine;
    return {
        id,
        label: `${selection ? 'Selection context' : symbol ? 'Symbol context' : 'File context'} · ${symbol?.name ?? filePath}`,
        text: JSON.stringify({
            kind: 'selected-code-context', project, path: filePath, symbol,
            generation: ir?.generation,
            scope: selection ? 'Graph relationships describe the named symbol, not just the selected text.'
                : symbol ? 'Selected symbol and its indexed relationships.' : 'File symbols and import context; file source is not attached.',
            source: sourceAvailable ? {
                firstLine, lastLine,
                text: document.source.split('\n').slice(firstLine - document.firstLine, lastLine - document.firstLine + 1).join('\n'),
                note: 'Complete source lines covering the indexed symbol; this is not an editor text selection.',
            } : undefined,
            evidence: browserGraphContext(ir, project, filePath, symbol ? workspacePathOf(symbol.uri) : '')
                .map(item => JSON.parse(item.text) as unknown),
            symbols: !symbol ? snapshot.fileSymbols?.map(ref => ({ name: ref.name, qualifiedName: ref.qualifiedName,
                kind: ref.kind, line: ref.range.start.line + 1 })) : undefined,
            fileSymbolsNote: !symbol ? snapshot.fileSymbolsMessage : undefined,
            imports: snapshot.imports,
            coverage: snapshot.coverageNote,
            limitations: 'Static indexed evidence is best-effort. Missing information does not establish absence. Select source text to attach its literal contents.',
        }, null, 2),
    };
}
