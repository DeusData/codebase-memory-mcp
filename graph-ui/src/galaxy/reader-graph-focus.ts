import type { SymbolRef } from '../core/focus-protocol';
import type { ReaderSelection } from '../reader/MonacoReader';
import { workspacePathOf } from '../twin/twin-target';
import type { GraphEdge, GraphNode } from './types';

export interface SourceFocusRange { startLine: number; endLine: number }
const normalizedPath = (path: string) => (path.startsWith('file:') ? workspacePathOf(path) : path).replace(/\\/g, '/').replace(/^\.\//, '');

/** Cursor movement alone is not an explicit graph selection. */
export function markedSourceRange(selection: Omit<ReaderSelection, 'sourceVersion'> | undefined, filePath: string): SourceFocusRange | undefined {
    if (!selection?.text.length || normalizedPath(selection.path) !== normalizedPath(filePath)) return undefined;
    return { startLine: selection.startLine,
        endLine: selection.endLine > selection.startLine && selection.endColumn === 1 ? selection.endLine - 1 : selection.endLine };
}

/** Frame incident relationships without selecting their unrelated second-hop edges. */
export function readerFocusFrame(ids: Set<number>, edges: readonly GraphEdge[]): Set<number> {
    const frame = new Set(ids);
    for (const edge of edges) {
        if (ids.has(edge.source) || ids.has(edge.target)) {
            frame.add(edge.source);
            frame.add(edge.target);
        }
    }
    return frame;
}

export function symbolMatchesReader(symbol: SymbolRef | undefined, filePath: string, range?: SourceFocusRange): boolean {
    if (!symbol || !filePath || normalizedPath(symbol.uri) !== normalizedPath(filePath)) return false;
    if (!range) return true;
    const endLine = symbol.range.end.line + (symbol.range.end.character > 0 ? 1 : 0);
    return symbol.range.start.line + 1 <= range.startLine && endLine >= range.endLine;
}

export function readerGraphFocus(nodes: readonly GraphNode[], filePath: string, range?: SourceFocusRange): { ids: Set<number>; message: string } {
    const fileNodes = nodes.filter((node) => node.file_path && normalizedPath(node.file_path) === normalizedPath(filePath));
    if (fileNodes.length === 0) return { ids: new Set(), message: `${filePath} is not in the loaded graph layout.` };
    if (range) {
        const overlapping = fileNodes.filter((node) => !['File', 'Folder', 'Module', 'Package', 'Namespace'].includes(node.label)
            && node.start_line !== undefined && node.start_line <= range.endLine
            && (node.end_line ?? node.start_line) >= range.startLine);
        const symbols = overlapping.filter((node) => !['Class', 'Interface', 'Struct', 'Enum'].includes(node.label));
        const selected = symbols.length > 0 ? symbols : overlapping;
        if (selected.length > 0) return { ids: new Set(selected.map((node) => node.id)), message: '' };
        return { ids: new Set(fileNodes.map((node) => node.id)), message: 'No indexed symbol overlaps this source range. Showing the file.' };
    }
    return { ids: new Set(fileNodes.map((node) => node.id)), message: '' };
}
