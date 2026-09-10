import type { SymbolRef } from '../core/focus-protocol';
import { workspacePathOf } from '../twin/twin-target';
import type { GraphNode } from './types';

export interface SourceFocusRange { startLine: number; endLine: number }
const normalizedPath = (path: string) => (path.startsWith('file:') ? workspacePathOf(path) : path).replace(/\\/g, '/').replace(/^\.\//, '');

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
        const overlapping = fileNodes.filter((node) => node.start_line !== undefined && node.start_line <= range.endLine
            && (node.end_line ?? node.start_line) >= range.startLine);
        if (overlapping.length > 0) return { ids: new Set(overlapping.map((node) => node.id)), message: '' };
        return { ids: new Set(fileNodes.map((node) => node.id)), message: 'No indexed symbol overlaps this source range. Showing the file.' };
    }
    return { ids: new Set(fileNodes.map((node) => node.id)), message: '' };
}
