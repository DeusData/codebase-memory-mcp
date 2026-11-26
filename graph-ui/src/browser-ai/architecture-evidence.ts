import { architectureWords as words } from './strings';
import type { SourceTarget } from './symbol-source';

/** Architecture selections as readable facts. The snapshot fields are the views' own
 * data; a model that reads "Selected.members[3].startLine: 13" lists "Finding a line
 * number" (K7). Here they become sentences, and ids, colors and bounds stay out. */
export interface ArchitectureFacts {
    /** Sentences for the prompt and, as bullets, for the explanation card. */
    facts: string[];
    /** The one symbol whose source grounds the explanation, if the selection is one. */
    target?: SourceTarget;
    /** Source the view already read for this selection (Behavior, Service map). */
    source?: { path: string; startLine: number; endLine: number; text: string };
}

type Row = Record<string, unknown>;
const record = (value: unknown): Row | undefined => value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Row : undefined;
const rows = (value: unknown): Row[] => Array.isArray(value) ? value.map(record).filter((item): item is Row => item !== undefined) : [];
function text(value: unknown, limit = 160): string | undefined {
    if (typeof value !== 'string' || !value.trim()) return undefined;
    const clean = value.replace(/\s+/g, ' ').trim();
    return clean.length <= limit ? clean : `${clean.slice(0, limit - 1)}…`;
}
const count = (value: unknown): number | undefined => typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : undefined;
const number = (value: number) => value.toLocaleString('en-US');
const quote = (value: string) => `\`${value.replace(/`/g, "'")}\``;
/** Counts in a view's own detail ("2310 files · 15299 indexed nodes") written as the card writes them;
 * a line after a path ("query.py:1487") stays as it is (W10). */
const grouped = (detail: string | undefined) => detail?.replace(/(?<![\w:.,-])\d{4,}(?![\w.,:-])/g, digits => number(Number(digits)));
const lines = (start: unknown, end: unknown) => {
    const first = count(start), last = count(end);
    return first ? `:${first}${last && last !== first ? `-${last}` : ''}` : '';
};

/** A graph node as the views publish it, in either naming (camelCase or snake_case). */
function symbolOf(value: unknown): { name: string; kind?: string; qualifiedName?: string; path?: string; startLine?: number; endLine?: number } | undefined {
    const row = record(value);
    const name = text(row?.name, 120);
    if (!row || !name) return undefined;
    return { name, kind: text(row.kind ?? row.label, 40), qualifiedName: text(row.qualifiedName ?? row.qualified_name, 400),
        path: text(row.filePath ?? row.file_path, 240), startLine: count(row.startLine ?? row.start_line), endLine: count(row.endLine ?? row.end_line) };
}
const located = (symbol: NonNullable<ReturnType<typeof symbolOf>>) => `${quote(symbol.name)}${symbol.kind ? ` (${symbol.kind})` : ''}${symbol.path ? ` in ${quote(`${symbol.path}${lines(symbol.startLine, symbol.endLine)}`)}` : ''}`;
const targetOf = (symbol: ReturnType<typeof symbolOf>): SourceTarget | undefined => symbol?.qualifiedName
    ? { qualifiedName: symbol.qualifiedName, name: symbol.name, kind: symbol.kind, path: symbol.path, startLine: symbol.startLine, endLine: symbol.endLine } : undefined;

function names(items: Row[], limit: number, total?: number): string {
    const listed = items.map(symbolOf).filter(item => item !== undefined).slice(0, limit).map(item => `${quote(item.name)}${item.kind ? ` (${item.kind})` : ''}`);
    const more = Math.max(0, (total ?? items.length) - listed.length);
    return `${listed.join(', ')}${more ? `; ${words.more(more)}` : ''}`;
}

/** The measured lines and languages of an area or file; languages by files, largest first, Unknown last (W10). */
function measuredFacts(measure: Row | undefined): string[] {
    const languages = rows(measure?.languages).flatMap(item => { const name = text(item.name, 40), files = count(item.files); return name && files ? [{ name, files }] : []; })
        .sort((left, right) => Number(left.name === 'Unknown') - Number(right.name === 'Unknown') || right.files - left.files)
        .map(item => `${item.name} ${number(item.files)}`);
    return measure && count(measure.lines) !== undefined ? [words.measured(count(measure.lines)!, count(measure.measuredFiles) ?? 0, count(measure.files) ?? 0, languages)] : [];
}

/** The ranked hotspot findings, at most five by name, place and measure. */
function hotspotFacts(group: Row | undefined): string[] {
    const findings = rows(group?.findings);
    if (!findings.length) return [];
    const ranked = findings.map(item => ({ name: text(item.name, 120), path: text(item.filePath, 240), line: count(item.line), fanIn: count(item.fanIn), complexity: count(item.complexity) }))
        .filter(item => item.name).slice(0, 5)
        .map(item => `${quote(item.name!)}${item.path ? ` (${quote(`${item.path}${item.line ? `:${item.line}` : ''}`)})` : ''}${item.fanIn !== undefined ? ` ${words.fanIn(item.fanIn)}` : item.complexity !== undefined ? ` ${words.complexity(item.complexity)}` : ''}`);
    return [words.hotspots(findings.length, ranked)];
}

/** Connections summed by the part at the other end (or the pair, without one) and the edge type.
 * Parts in `outside` lie outside the opened area and are named so. */
function connectionLines(items: Row[], label?: string, outside: ReadonlySet<string> = new Set()): string[] {
    const sides = new Map<string, Map<string, number>>();
    const part = (name: string) => outside.has(name) ? words.outside(quote(name)) : quote(name);
    for (const item of items) {
        const from = text(item.source, 120), to = text(item.target, 120), edge = text(item.type, 40);
        if (!from || !to || !edge) continue;
        const key = label === undefined ? `${part(from)} → ${part(to)}` : from === label ? words.to(to) : to === label ? words.from(from) : `${from} → ${to}`;
        const types = sides.get(key) ?? new Map<string, number>();
        types.set(edge, (types.get(edge) ?? 0) + (count(item.count) ?? 1)); sides.set(key, types);
    }
    return [...sides].map(([side, types]) => `${side}: ${[...types].sort((a, b) => b[1] - a[1]).map(([edge, total]) => `${edge} ×${number(total)}`).join(', ')}`);
}

/** An opened area, file or hotspot area with nothing selected inside it (K7): "Opened: django"
 * said nothing, so the scope is described by what the view shows of it. */
function openedFacts(selected: Row, relationships: Row | undefined): ArchitectureFacts {
    const file = text(selected.filePath, 240), area = text(selected.areaPath, 240), hotspotArea = text(selected.hotspotArea, 240);
    const opened = file ?? area ?? hotspotArea;
    if (!opened) return { facts: [] };
    const facts = [file ? words.openedFile(quote(file)) : area ? words.openedArea(quote(area)) : words.openedHotspots(quote(opened))];
    facts.push(...measuredFacts(record(selected.measurement)));
    const parts = rows(selected.parts).flatMap(item => {
        const label = text(item.label, 160), kind = text(item.kind, 20);
        return label && (kind === 'area' || kind === 'file') ? [words.scopePart(quote(label), kind, kind === 'area' ? count(item.files) : undefined, count(item.lines))] : [];
    });
    const outside = new Set((Array.isArray(selected.outside) ? selected.outside : []).flatMap(name => text(name, 120) ?? []));
    // The view counts the parts outside the opened scope with the ones inside it, so the card does too.
    const outsideTotal = Math.max(count(selected.outsideCount) ?? 0, outside.size);
    if (parts.length || outside.size) facts.push(words.parts(parts, Math.max(count(selected.partCount) ?? 0, parts.length), file ? 'file' : 'area',
        [...outside].map(quote), outsideTotal));
    facts.push(...hotspotFacts(record(selected.hotspots)));
    const described = connectionLines(rows(relationships?.items), undefined, outside);
    if (described.length) facts.push(words.partConnections(described, count(relationships?.omitted) ?? 0));
    return { facts };
}

/** Overview, Hotspots, Routes and Entry points: a source area, file, symbol, route or connection. */
function spatialFacts(selected: Row, relationships: Row | undefined, scope: Row | undefined): ArchitectureFacts {
    const facts: string[] = [];
    const source = text(selected.source, 120), target = text(selected.target, 120), type = text(selected.type, 40);
    if (source && target && type) {
        facts.push(words.connection(source, target, type, count(selected.count)));
        const examples = rows(selected.evidence).slice(0, 3).flatMap(item => {
            const from = symbolOf(item.source), to = symbolOf(item.target);
            return from && to ? [words.example(quote(from.name), text(item.type, 40) ?? type, quote(to.name), from.path ? `${from.path}${lines(item.line ?? from.startLine, undefined)}` : undefined)] : [];
        });
        if (examples.length) facts.push(words.examples(examples));
        return { facts };
    }
    const label = text(selected.label, 160);
    const kind = text(selected.kind, 20);
    if (!label || !kind) return openedFacts(selected, relationships);
    const members = rows(selected.members);
    const total = count(selected.memberCount) ?? members.length;
    const symbol = kind === 'symbol' ? symbolOf(members[0]) : undefined;
    facts.push(kind === 'symbol' && symbol ? words.selectedSymbol(located(symbol)) : words.selected(words.kinds[kind as keyof typeof words.kinds] ?? kind, quote(label), grouped(text(selected.detail, 160))));
    facts.push(...measuredFacts(record(selected.measurement)));
    facts.push(...hotspotFacts(record(selected.hotspots)));
    if (kind !== 'symbol' && members.length) facts.push(words.members(names(members, 8, total)));
    // Connections of the selection, summed by the part at the other end and the edge type.
    const described = connectionLines(rows(relationships?.items), label);
    if (described.length) facts.push(words.connections(described, count(relationships?.omitted) ?? 0));
    const visible = count(scope?.visibleNodes);
    if (visible !== undefined) facts.push(words.view(text(scope?.view, 40) ?? 'overview', visible, count(scope?.visibleEdges) ?? 0));
    return { facts, target: targetOf(symbol) };
}

/** The function a call site reaches, read from its source line: the call whose arguments
 * begin with the recorded first argument, or an empty call when none is recorded. The
 * calls carry only the target's id, so the line is the only name at hand. */
function calleeAt(source: string | undefined, start: number | undefined, line: number, first: string | undefined): string | undefined {
    const code = source && start ? source.split('\n')[line - start] : undefined;
    if (!code) return undefined;
    for (const match of code.matchAll(/([A-Za-z_][\w.]*)\s*\(\s*/g)) {
        const rest = code.slice(match.index + match[0].length);
        if (first ? rest.startsWith(first) : rest.startsWith(')')) return text(match[1], 80);
    }
    return undefined;
}

/** Behavior: the starting operation, the selected call and the calls it makes. */
function behaviorFacts(selected: Row, relationships: Row | undefined): ArchitectureFacts {
    const facts: string[] = [];
    const operation = symbolOf(selected.operation), caller = symbolOf(selected.caller), callee = symbolOf(selected.callee);
    if (operation) facts.push(words.operation(located(operation)));
    const call = record(selected.call);
    const site = record(call?.callsite);
    if (caller && callee && caller.name !== callee.name) facts.push(words.call(quote(caller.name), quote(callee.name), site ? `${text(site.file_path, 240) ?? ''}${lines(site.line, undefined)}` : undefined));
    const current = record(record(selected.currentSource)?.source);
    const sourceText = typeof current?.source === 'string' ? current.source : undefined;
    const from = count(current?.start_line), to = count(current?.end_line);
    const calls = rows(relationships?.calls);
    if (calls.length) {
        facts.push(words.directCalls(count(relationships?.count) ?? calls.length, calls.slice(0, 6).flatMap(item => {
            const where = record(item.callsite);
            const line = count(where?.line);
            const args = rows(item.arguments).flatMap(argument => text(argument.e, 60) ?? []).slice(0, 3);
            const name = line ? calleeAt(sourceText, from, line, args[0]) : undefined;
            return line ? [words.callAt(line, args, name ? quote(name) : undefined)] : [];
        })));
    }
    const path = rows(relationships?.nodes);
    if (path.length) facts.push(words.path(path.flatMap(node => symbolOf(node)?.name ?? []).slice(0, 8).map(quote)));
    const component = record(selected.component);
    const label = text(component?.label, 160);
    if (label) facts.push(words.component(quote(label), count(component?.member_count), count(component?.file_count)));
    const sourcePath = text(current?.file_path, 400);
    const symbol = callee ?? operation;
    return { facts, target: targetOf(operation ?? symbol),
        ...sourceText?.trim() && from && to && sourcePath ? { source: { path: operation?.path ?? sourcePath, startLine: from, endLine: to, text: sourceText } } : {} };
}

/** System structure: a component, a group of components or a connection between them. */
function structureFacts(selected: Row, relationships: Row | undefined): ArchitectureFacts {
    const facts: string[] = [];
    const source = text(selected.source, 120), target = text(selected.target, 120);
    if (source && target) {
        const dependencies = rows(selected.dependencies);
        facts.push(words.connection(source, target, text(selected.type, 40) ?? dependencies.map(item => text(item.type, 40)).filter(Boolean).join(', '),
            dependencies.reduce((sum, item) => sum + (count(item.count) ?? 0), 0) || undefined));
        const witnesses = dependencies.flatMap(item => rows(item.witnesses)).slice(0, 3).flatMap(item => {
            const from = symbolOf(item.source), to = symbolOf(item.target);
            const site = record(item.callsite);
            return from && to ? [words.example(quote(from.name), 'CALLS', quote(to.name), site ? `${text(site.file_path, 240) ?? ''}${lines(site.line, undefined)}` : from.path)] : [];
        });
        if (witnesses.length) facts.push(words.examples(witnesses));
        return { facts };
    }
    for (const [kind, value] of [['component', selected.component], ['group', selected.group]] as const) {
        const item = record(value);
        const label = text(item?.label, 160);
        if (!item || !label) continue;
        facts.push(words.part(kind, quote(label), count(item.member_count), count(item.file_count), count(item.component_count), text(item.role, 20)));
        const representatives = rows(item.representatives);
        if (representatives.length) facts.push(words.representatives(names(representatives, 6)));
        break;
    }
    const side = (value: unknown) => rows(value).flatMap(item => { const id = text(item.id, 120); return id ? [`${id.replace(/^component-/, '#')} (${(Array.isArray(item.types) ? item.types : []).map(type => text(type, 30)).filter(Boolean).join(', ')})`] : []; });
    const incoming = side(relationships?.incoming), outgoing = side(relationships?.outgoing);
    if (incoming.length) facts.push(words.incoming(incoming.slice(0, 8), incoming.length));
    if (outgoing.length) facts.push(words.outgoing(outgoing.slice(0, 8), outgoing.length));
    return { facts };
}

/** Anything else: the selected names and counts, never ids or bounds. */
function genericFacts(selected: Row): ArchitectureFacts {
    const facts: string[] = [];
    for (const [key, value] of Object.entries(selected)) {
        const row = record(value);
        const name = text(row?.name ?? row?.label, 160);
        if (name) facts.push(words.selected(key, quote(name), text(row?.summary ?? row?.detail ?? row?.image, 160)));
    }
    return { facts };
}

export function architectureFacts(evidence: Row): ArchitectureFacts | undefined {
    const view = text(evidence.view, 60);
    const selected = record(evidence.selected);
    if (!view?.startsWith('architecture-') || !selected) return undefined;
    const relationships = record(evidence.relationships), scope = record(evidence.scope);
    const result = view === 'architecture-behavior' ? behaviorFacts(selected, relationships)
        : view === 'architecture-structure' ? structureFacts(selected, relationships)
            : view === 'architecture-services' ? genericFacts(selected) : spatialFacts(selected, relationships, scope);
    return result.facts.length ? result : undefined;
}
