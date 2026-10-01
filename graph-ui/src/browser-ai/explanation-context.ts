import { snapshotReaderContext, type BrowserChatContext, type BrowserChatReaderContext, type BrowserChatSource } from './chat-model';

export interface ExplanationEvidence {
    id: string;
    text: string;
    source: 'code' | 'graph';
    location?: { path: string; startLine: number; startColumn: number; endLine: number; endColumn: number; sourceVersion: string };
}

export interface PreparedExplanationContext {
    label: string;
    evidence: ExplanationEvidence[];
    limitations: string[];
    fallback: string;
    /** Human-readable strings only; callers must still count the final model tokens. */
    characterCount: number;
}

const record = (value: unknown): Record<string, unknown> | undefined => value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown> : undefined;

/** JavaScript/Monaco offsets are UTF-16. Never leave half of a surrogate pair. */
function prefix(text: string, length: number): string {
    let end = Math.max(0, Math.min(text.length, Math.floor(length)));
    if (end > 0 && end < text.length && /[\uD800-\uDBFF]/.test(text[end - 1]) && /[\uDC00-\uDFFF]/.test(text[end])) end--;
    return text.slice(0, end);
}

function bounded(text: string, limit: number): string {
    if (text.length <= limit) return text;
    const suffix = '…';
    return prefix(text, Math.max(0, limit - suffix.length)) + (limit > 0 ? suffix : '');
}

function sourceEvidence(source: BrowserChatSource, budget: number): { evidence: ExplanationEvidence[]; omitted: number } {
    const { text } = source;
    const location = { path: source.path, startLine: source.startLine, startColumn: source.startColumn,
        endLine: source.endLine, endColumn: source.endColumn, sourceVersion: source.sourceVersion };
    if (text.length <= budget) return { evidence: text ? [{ id: 'source-1', text, source: 'code', location }] : [], omitted: 0 };
    if (budget <= 0) return { evidence: [], omitted: text.length };

    // A single linear scan supplies exact offsets and cheap lexical anchors. These
    // anchors only choose excerpts; they do not establish language or semantics.
    const starts = [0];
    for (let index = 0; index < text.length; index++) if (text.charCodeAt(index) === 10) starts.push(index + 1);
    const position = (offset: number) => {
        let low = 0, high = starts.length;
        while (low + 1 < high) {
            const middle = (low + high) >>> 1;
            if (starts[middle] <= offset) low = middle; else high = middle;
        }
        return { line: source.startLine + low, column: offset - starts[low] + (low === 0 ? source.startColumn : 1) };
    };
    const make = (start: number, end: number, index: number): ExplanationEvidence => {
        const content = prefix(text.slice(start, end), end - start);
        // slice(start, end) may itself end halfway through a pair; inspect the
        // original next character before retaining this prefix.
        const exactEnd = start + content.length;
        let safeEnd = exactEnd < text.length && /[\uD800-\uDBFF]/.test(text[exactEnd - 1] ?? '')
            && /[\uDC00-\uDFFF]/.test(text[exactEnd]) ? exactEnd - 1 : exactEnd;
        if (text[safeEnd - 1] === '\r' && text[safeEnd] === '\n') safeEnd--;
        const first = position(start), last = position(safeEnd);
        return { id: `source-${index + 1}`, text: text.slice(start, safeEnd), source: 'code', location: { ...location,
            startLine: first.line, startColumn: first.column, endLine: last.line, endColumn: last.column } };
    };
    if (source.kind === 'selection') {
        const item = make(0, Math.floor(budget), 0);
        return { evidence: item.text ? [item] : [], omitted: text.length - item.text.length };
    }

    const anchors: number[] = [];
    let firstContent = -1;
    const declaration = /^\s*(?:(?:export|default|async)\s+)*(?:import|from|use|package|class|interface|type|function|def|fn|func|struct|enum|impl|module|namespace|const|let|var|public|private|protected|static)\b|^\s*#include\b/;
    for (let line = 0; line < starts.length; line++) {
        const head = text.slice(starts[line], Math.min(starts[line + 1] ?? text.length, starts[line] + 240));
        if (firstContent < 0 && head.trim() && !/^\s*(?:\/\/|#(?!include\b)|\/\*|\*|<!--|--)/.test(head)) firstContent = line;
        if (declaration.test(head)) anchors.push(line);
    }
    const first = Math.max(0, firstContent);
    const targets = budget >= 256 ? [first, Math.floor(starts.length / 3), Math.floor(2 * starts.length / 3), Math.max(first, starts.length - 3)] : [first];
    const nearestAnchor = (target: number) => {
        if (!anchors.length) return target;
        let low = 0, high = anchors.length;
        while (low < high) {
            const middle = (low + high) >>> 1;
            if (anchors[middle] < target) low = middle + 1; else high = middle;
        }
        const before = anchors[Math.max(0, low - 1)], after = anchors[Math.min(anchors.length - 1, low)];
        const nearest = target - before <= after - target ? before : after;
        // Sparse declarations must not erase the distributed line sampling.
        return Math.abs(nearest - target) <= Math.max(12, starts.length / 8) ? nearest : target;
    };
    const windows = [...new Set(targets.map((target, index) => index === 0 ? first : nearestAnchor(target)))].sort((a, b) => a - b);
    const evidence: ExplanationEvidence[] = [];
    const perWindow = Math.floor(budget / windows.length);
    for (let index = 0; index < windows.length; index++) {
        const line = windows[index], start = starts[line];
        const end = Math.min(text.length, start + perWindow, starts[line + 20] ?? text.length,
            index + 1 < windows.length ? starts[windows[index + 1]] : text.length);
        const item = make(start, end, evidence.length);
        if (item.text) evidence.push(item);
    }
    return { evidence, omitted: text.length - evidence.reduce((count, item) => count + item.text.length, 0) };
}

/** Readable, bounded paths preserve the distinction between reported fields and
 * conclusions. No field name, identifier, or documentation becomes an instruction. */
function graphFacts(value: unknown, name: string, maxCharacters: number): string {
    const lines: string[] = [];
    let remaining = maxCharacters, visited = 0, omittedFields = 0;
    const add = (line: string) => {
        if (line.length + 1 <= remaining) { lines.push(line); remaining -= line.length + 1; }
        else omittedFields++;
    };
    const visit = (current: unknown, path: string, depth: number) => {
        if (current === undefined) return;
        if (++visited > 100 || depth > 5 || remaining < 35) { omittedFields++; return; }
        if (Array.isArray(current)) {
            add(`${path}: ${current.length} snapshot items`);
            const take = Math.min(current.length, 3);
            if (current.length > take) add(`${path}: ${current.length - take} snapshot items omitted from this explanation`);
            for (let index = 0; index < take; index++) visit(current[index], `${path}[${index}]`, depth + 1);
            return;
        }
        const object = record(current);
        if (object) {
            const entries = Object.entries(object);
            // Put counts and scalar identity before potentially large collections.
            const ordered = [...entries.filter(([, item]) => item === null || typeof item !== 'object'),
                ...entries.filter(([, item]) => item !== null && typeof item === 'object')];
            for (const [key, item] of ordered.slice(0, 24)) visit(item, `${path}.${bounded(key, 60)}`, depth + 1);
            if (ordered.length > 24) add(`${path}: ${ordered.length - 24} fields omitted`);
            return;
        }
        if (typeof current === 'string') {
            const literal = prefix(current, 180);
            add(`${path}: ${JSON.stringify(literal)}${literal.length < current.length ? ` (${current.length - literal.length} characters omitted)` : ''}`);
        } else if (current === null || typeof current === 'number' || typeof current === 'boolean') add(`${path}: ${String(current)}`);
    };
    visit(value, name, 0);
    if (omittedFields) {
        const note = `${omittedFields} additional graph fields omitted from this explanation.`;
        while (lines.length && lines.join('\n').length + note.length + 1 > maxCharacters) lines.pop();
        if (note.length <= maxCharacters) lines.push(note);
    }
    return lines.join('\n');
}

interface GraphPreparation { facts: string[]; limitations: string[] }

function fitGraphFact(fact: string, budget: number): string {
    if (fact.length <= budget) return fact;
    const lines = fact.split('\n');
    const included: string[] = [];
    for (const line of lines) {
        const note = `${lines.length - included.length - 1} additional graph summary lines omitted.`;
        if (included.join('\n').length + line.length + note.length + 2 > budget) break;
        included.push(line);
    }
    const note = `${lines.length - included.length} additional graph summary lines omitted.`;
    if (included.length && included.join('\n').length + note.length + 1 <= budget) return [...included, note].join('\n');
    return '';
}

function graphEvidence(context: BrowserChatContext): GraphPreparation {
    // Producer snapshots are already bounded. Refuse arbitrary giant input before
    // parsing it; a malformed/foreign envelope is never treated as graph evidence.
    if (context.text.length > 128_000) return { facts: [], limitations: ['Graph snapshot exceeds the supported evidence size; graph facts unavailable.'] };
    let parsed: Record<string, unknown> | undefined;
    try { parsed = record(JSON.parse(context.text)); } catch { /* Report unsupported data below. */ }
    const evidence = record(parsed?.evidence);
    if (evidence?.kind !== 'current-selection-evidence') return { facts: [], limitations: ['Unsupported graph snapshot; graph facts unavailable.'] };
    const limits = ['Static graph relationships do not establish runtime execution or repository purpose.'];
    if (typeof evidence.source !== 'string' || !evidence.source || typeof evidence.project !== 'string'
        || !evidence.project || typeof evidence.generation !== 'string' || !evidence.generation || evidence.generation === 'unavailable') {
        limits.push('Graph provenance or index generation unavailable; freshness is not established.');
    }
    const declaredLimits = graphFacts(evidence.limitations, 'Graph limitations', 600);
    if (declaredLimits) limits.push(declaredLimits);
    const upstream = graphFacts(parsed?.omissions, 'Upstream omissions', 350);
    if (Array.isArray(parsed?.omissions) && parsed.omissions.length && upstream) limits.push(upstream);
    const facts = [
        graphFacts({ project: evidence.project, view: evidence.view, source: evidence.source, generation: evidence.generation }, 'Snapshot', 500),
        graphFacts(evidence.selected, 'Selected', 1400),
        graphFacts(evidence.relationships, 'Relationships', 1400),
        graphFacts(evidence.scope, 'Scope', 500),
    ].filter(Boolean);
    return { facts, limitations: limits };
}

/** Pure preparation for automatic explanations and bounded current-file chat.
 * Everything returned remains untrusted evidence data, never model instructions. */
export function prepareExplanationContext(reader?: BrowserChatReaderContext,
    graph?: BrowserChatContext | readonly BrowserChatContext[], maxCharacters = 4000): PreparedExplanationContext {
    const budget = Number.isFinite(maxCharacters) ? Math.max(0, Math.floor(maxCharacters)) : 4000;
    const snapshot = snapshotReaderContext(reader), source = snapshot?.source;
    const graphs: readonly BrowserChatContext[] = graph ? Array.isArray(graph) ? graph : [graph as BrowserChatContext] : [];
    const selectedGraphs = graphs.slice(0, 2).map(graphEvidence);
    const label = bounded(source?.path ?? reader?.path ?? graphs[0]?.label ?? 'Current selection', Math.min(120, Math.floor(budget / 10)));
    const fallback = bounded(source?.text
        ? `${source.kind === 'selection' ? 'Selected source' : 'Source'} is available as cited text. A generated explanation is unavailable.`
        : `Source unavailable.${selectedGraphs.some(item => item.facts.length) ? ' Only the supplied static graph facts are available.' : ' There is insufficient evidence to explain this selection.'}`,
    Math.min(250, Math.floor(budget / 5)));
    const limitBudget = Math.min(1000, Math.floor((budget - label.length - fallback.length) / 3));
    const desiredLimits: string[] = [];
    if (!source?.text) desiredLimits.push(`Source unavailable${reader ? ` (reader status: ${snapshot?.status ?? 'unavailable'})` : ''}; implementation details cannot be established.`);
    if (source?.partial) desiredLimits.push(`Reader source limitation: ${bounded(source.partial, 220)}`);
    if (graphs.length > 2) desiredLimits.push(`${graphs.length - 2} graph snapshots omitted.`);
    desiredLimits.push(...selectedGraphs.flatMap(item => item.limitations));
    const evidenceBudget = Math.max(0, budget - label.length - fallback.length - limitBudget);
    const codeBudget = source ? Math.floor(evidenceBudget * (selectedGraphs.some(item => item.facts.length) ? 0.7 : 1)) : 0;
    const code: ReturnType<typeof sourceEvidence> = source ? sourceEvidence(source, codeBudget) : { evidence: [], omitted: 0 };
    if (code.omitted) desiredLimits.unshift(`${code.omitted} source characters omitted; ${source?.kind === 'file'
        ? 'sampled source regions, not the full file' : 'only a prefix of the selected source is included'}. Ranges identify the literal excerpts.`);
    const evidence = code.evidence;
    let remaining = evidenceBudget - evidence.reduce((count, item) => count + item.text.length, 0);
    let omittedFacts = 0;
    const facts = selectedGraphs.flatMap(prepared => prepared.facts);
    for (let index = 0; index < facts.length; index++) {
        // Leave room for relationships as well as selected identity. Compact
        // complete fact lines; never silently cut a quoted field into a new fact.
        const fact = fitGraphFact(facts[index], Math.floor(remaining / (facts.length - index)));
        if (fact) {
            evidence.push({ id: `graph-${evidence.filter(item => item.source === 'graph').length + 1}`, text: fact, source: 'graph' });
            remaining -= fact.length;
        } else omittedFacts++;
    }
    if (omittedFacts) desiredLimits.splice(code.omitted ? 1 : 0, 0, `${omittedFacts} graph fact groups omitted from this explanation.`);
    const limitations: string[] = [];
    let limitRemaining = limitBudget;
    for (let index = 0; index < desiredLimits.length; index++) {
        const limit = desiredLimits[index];
        const omission = `${desiredLimits.length - index} additional limitations omitted.`;
        const reserve = index + 1 < desiredLimits.length ? 44 : 0;
        if (limit.length + reserve <= limitRemaining) {
            limitations.push(limit); limitRemaining -= limit.length;
        } else {
            if (omission.length <= limitRemaining) limitations.push(omission);
            break;
        }
    }
    const characterCount = label.length + fallback.length + evidence.reduce((count, item) => count + item.text.length, 0)
        + limitations.reduce((count, item) => count + item.length, 0);
    return { label, evidence, limitations, fallback, characterCount };
}
