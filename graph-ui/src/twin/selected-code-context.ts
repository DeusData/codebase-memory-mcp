import type { CodeAtlasSymbolKind, SymbolRef } from '../core/focus-protocol';
import { COLUMNS, declarationsInFiles } from '../provider/cypher';
import { EngineError } from '../provider/engine-errors';
import type { RpcIntelligenceClient } from '../provider/rpc-client';
import { twinTargetOf } from './twin-target';

const FILE_SYMBOL_LIMIT = 200;
const LABELS = [
    ['Function', 'function'], ['Method', 'method'], ['Class', 'class'],
    ['Interface', 'interface'], ['Type', 'type'],
] as const satisfies readonly (readonly [string, CodeAtlasSymbolKind])[];

function missingLabel(error: unknown, label: string): boolean {
    if (!(error instanceof EngineError)) return false;
    // Only an explicit absent-label answer is benign. Permissions, a missing
    // project and transport failures must remain partial/error results.
    return new RegExp(`(?:unknown|unrecognized|missing|no such) (?:node )?label[ :'"]+${label}\\b|(?:node )?label[ :'"]+${label}['"]? (?:not found|does not exist)`, 'i')
        .test(error.message);
}

export interface FileSymbolResult {
    /** Declaration navigation targets; ranges are not complete symbol spans. */
    symbols: SymbolRef[];
    /** Empty on a complete answer, otherwise the precise retrieval limitation. */
    message: string;
}

export async function loadFileSymbols(
    client: Pick<RpcIntelligenceClient, 'queryRows'>,
    project: string,
    path: string,
): Promise<FileSymbolResult> {
    if (project.trim().length === 0 || path.trim().length === 0) {
        throw new Error('Select a project and file before loading symbols.');
    }
    const results = await Promise.allSettled(LABELS.map(async ([label]) => {
        const query = declarationsInFiles(label, [path], FILE_SYMBOL_LIMIT);
        if (!query) throw new Error('The file path could not be queried.');
        return client.queryRows(project, query);
    }));
    const symbols = new Map<string, SymbolRef>();
    const failed: string[] = [];
    const absent: string[] = [];
    const capped: string[] = [];
    let answered = 0;
    let invalid = 0;
    for (const [index, result] of results.entries()) {
        const [label, kind] = LABELS[index];
        if (result.status === 'rejected') {
            (missingLabel(result.reason, label) ? absent : failed).push(label);
            continue;
        }
        answered++;
        if (result.value.length >= FILE_SYMBOL_LIMIT) capped.push(label);
        for (const row of result.value) {
            const name = row[COLUMNS.declarations[0]];
            const qualifiedName = row[COLUMNS.declarations[1]] || undefined;
            const filePath = row[COLUMNS.declarations[2]];
            const startLine = Number(row[COLUMNS.declarations[3]]);
            if (!name || filePath !== path || !Number.isSafeInteger(startLine) || startLine < 1) {
                invalid++;
                continue;
            }
            const target = twinTargetOf({ name, qualifiedName, filePath, startLine, kind });
            if (!target) { invalid++; continue; }
            const key = `${filePath}:${qualifiedName ?? `${name}:${startLine}`}`;
            if (!symbols.has(key)) symbols.set(key, { ...target, projectName: project });
        }
    }
    if (answered === 0) {
        throw new Error(failed.length > 0
            ? `Could not load file symbols. Queries failed for ${failed.join(', ')}.${absent.length ? ` Labels unavailable: ${absent.join(', ')}.` : ''}`
            : `The current index does not provide these symbol labels: ${absent.join(', ')}.`);
    }
    const ordered = [...symbols.values()].sort((a, b) => a.range.start.line - b.range.start.line
        || (a.qualifiedName ?? a.name).localeCompare(b.qualifiedName ?? b.name));
    const notes: string[] = [];
    if (failed.length > 0) notes.push(`Partial results: queries failed for ${failed.join(', ')}.`);
    if (absent.length > 0) notes.push(`Labels unavailable in this index: ${absent.join(', ')}.`);
    if (capped.length > 0) notes.push(`Some symbols may be omitted: ${capped.join(', ')} reached the ${FILE_SYMBOL_LIMIT}-row query limit.`);
    if (ordered.length > FILE_SYMBOL_LIMIT) notes.push(`Showing the first ${FILE_SYMBOL_LIMIT} returned symbols by declaration line.`);
    if (invalid > 0) notes.push(`${invalid} result${invalid === 1 ? '' : 's'} without a valid declaration in this file omitted.`);
    return { symbols: ordered.slice(0, FILE_SYMBOL_LIMIT), message: notes.join(' ') };
}
