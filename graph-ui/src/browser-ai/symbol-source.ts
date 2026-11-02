import { architectureFacts } from './architecture-evidence';
import type { BrowserChatContext, BrowserChatSource } from './chat-model';
import { readGalaxyEvidence } from './galaxy-evidence';

/** The selected symbol's source enters an explanation bounded to this many lines. Without
 * it the model guesses types, inputs and outputs from names (K14). */
export const SYMBOL_SOURCE_LINES = 40;

export interface SourceTarget { qualifiedName: string; name: string; kind?: string; path?: string; startLine?: number; endLine?: number }
/** The subset of a get_code_snippet answer this needs; the same call "Read source evidence" makes. */
export interface SymbolSnippet {
    source: string; file_path?: string; start_line?: number; end_line?: number; source_mode?: string;
    source_truncated?: boolean; source_clipped?: boolean; next_start_line?: number;
}
export type SymbolSourceReader = (qualifiedName: string, window: { maxLines: number }) => Promise<SymbolSnippet>;

/** Kinds whose "source" is a whole file or folder, not one symbol. */
const CONTAINERS = new Set(['file', 'folder', 'directory', 'project', 'package', 'module', 'area']);
const record = (value: unknown): Record<string, unknown> | undefined => value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown> : undefined;

/** The one symbol a Galaxy or Architecture selection stands for, if it is one. */
export function sourceTargetOf(context: BrowserChatContext | undefined): SourceTarget | undefined {
    if (!context || context.text.length > 128_000) return undefined;
    const galaxy = readGalaxyEvidence(context.text);
    if (galaxy) {
        const [root] = galaxy.roots;
        if (galaxy.rootCount !== 1 || !root?.qualifiedName || CONTAINERS.has((root.kind ?? '').toLowerCase())) return undefined;
        return { qualifiedName: root.qualifiedName, name: root.name, kind: root.kind, path: root.filePath, startLine: root.startLine, endLine: root.endLine };
    }
    let parsed: Record<string, unknown> | undefined;
    try { parsed = record(record(JSON.parse(context.text))?.evidence); } catch { return undefined; }
    const facts = parsed?.kind === 'current-selection-evidence' ? architectureFacts(parsed) : undefined;
    // Source the view already read needs no second read.
    return facts?.source ? undefined : facts?.target;
}

function architectureTarget(context: BrowserChatContext): SourceTarget | undefined {
    let parsed: Record<string, unknown> | undefined;
    try { parsed = record(record(JSON.parse(context.text))?.evidence); } catch { return undefined; }
    return parsed?.kind === 'current-selection-evidence' ? architectureFacts(parsed)?.target : undefined;
}

/** The symbol an explanation is about, whether its source is read or already carried. */
export function selectionSubject(context: BrowserChatContext | undefined): { name: string; kind?: string } | undefined {
    if (!context || context.text.length > 128_000) return undefined;
    const galaxy = readGalaxyEvidence(context.text);
    if (galaxy) return galaxy.rootCount === 1 && galaxy.roots[0] ? { name: galaxy.roots[0].name, kind: galaxy.roots[0].kind } : undefined;
    return architectureTarget(context);
}

/** A bounded, validated source block for the explanation, or undefined when the reply is
 * not that symbol's source. The repository-relative path of the selection is kept. */
export function symbolSource(target: SourceTarget, snippet: SymbolSnippet, sourceVersion: string): BrowserChatSource | undefined {
    const start = snippet.start_line, end = snippet.end_line;
    if (snippet.source_mode && snippet.source_mode !== 'full') return undefined;
    if (!snippet.source.trim() || snippet.source.trim() === '(source not available)' || !Number.isSafeInteger(start) || !Number.isSafeInteger(end)
        || start! < 1 || end! < start!) return undefined;
    const lines = snippet.source.replace(/\n$/, '').split('\n').slice(0, SYMBOL_SOURCE_LINES);
    const last = start! + lines.length - 1;
    const path = target.path ?? snippet.file_path ?? target.name;
    const symbolEnd = target.endLine && target.endLine >= start! ? target.endLine : end!;
    const cut = last < symbolEnd || snippet.source_truncated === true || snippet.source_clipped === true;
    return { id: `symbol:${target.qualifiedName}:${start}`, kind: 'selection', project: '', path, text: lines.join('\n'),
        startLine: start!, startColumn: 1, endLine: last, endColumn: (lines.at(-1)?.length ?? 0) + 1, sourceVersion,
        ...cut ? { partial: `Only lines ${start}-${last} of ${target.name} (${start}-${Math.max(symbolEnd, last)}) are included.` } : {} };
}

/** Source the selection already carries (Behavior read it for the selected call). */
export function carriedSource(context: BrowserChatContext | undefined): BrowserChatSource | undefined {
    if (!context || context.text.length > 128_000 || readGalaxyEvidence(context.text)) return undefined;
    let parsed: Record<string, unknown> | undefined;
    try { parsed = record(record(JSON.parse(context.text))?.evidence); } catch { return undefined; }
    const facts = parsed?.kind === 'current-selection-evidence' ? architectureFacts(parsed) : undefined;
    const source = facts?.source;
    if (!source) return undefined;
    const lines = source.text.replace(/\n$/, '').split('\n').slice(0, SYMBOL_SOURCE_LINES);
    return { id: `carried:${source.path}:${source.startLine}`, kind: 'selection', project: '', path: source.path, text: lines.join('\n'),
        startLine: source.startLine, startColumn: 1, endLine: source.startLine + lines.length - 1, endColumn: (lines.at(-1)?.length ?? 0) + 1, sourceVersion: 'current-local-source' };
}
