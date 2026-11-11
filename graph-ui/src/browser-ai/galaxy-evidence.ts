import { nodeDisplayName } from '../galaxy/node-names';
import type { RelationshipGroup, ScopeRelationships } from '../galaxy/selection-evidence';
import type { RelationshipWords } from './strings';

/** The Galaxy selection snapshot as readable data. Every field was bounded by its
 * producer and is validated again here; names and paths stay untrusted text. */
export interface GalaxyEvidence {
    project: string;
    label: string;
    /** The selected scope itself (node id, qualified name or path): stable while it loads. */
    identity: unknown;
    selectionKind: string;
    roots: { name: string; kind?: string; qualifiedName?: string; filePath?: string; startLine?: number; endLine?: number; documentation?: string }[];
    rootCount: number;
    depth: number;
    direction: 'both' | 'inbound' | 'outbound';
    edgeTypes: 'all' | string[];
    nodes: number;
    edges: number;
    /** `limited`: loaded, but a layer stopped at the render limit (C1). */
    state: 'complete' | 'loading' | 'partial' | 'limited';
    /** Where a `limited` scope stopped. */
    renderLimit?: { layer: number; kind: 'nodes' | 'edges'; limit: number };
    error?: string;
    exhausted: boolean;
    /** Distinct related symbols per side; undefined when the snapshot lost the total. */
    relationships: Omit<ScopeRelationships, 'incomingSymbols' | 'outgoingSymbols'> & { incomingSymbols?: number; outgoingSymbols?: number };
    /** The snapshot budget cut relationship data, so counts and names can be incomplete. */
    truncated: boolean;
    /** Which picture of the scope Galaxy shows; undefined where the snapshot does not say (H1). */
    display?: 'galaxy' | 'hierarchy';
}

const record = (value: unknown): Record<string, unknown> | undefined => value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown> : undefined;
const records = (value: unknown): Record<string, unknown>[] => Array.isArray(value) ? value.map(record).filter(item => item !== undefined) : [];
const count = (value: unknown): number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : 0;
const line = (value: unknown): number | undefined => typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : undefined;
function text(value: unknown, limit: number): string | undefined {
    if (typeof value !== 'string' || !value.trim()) return undefined;
    const clean = value.replace(/\s+/g, ' ').trim();
    return clean.length <= limit ? clean : `${clean.slice(0, limit - 1)}…`;
}

/* A related Branch node carries the project of the snapshot, so a listed answer can name it "django-demo · detached HEAD" (round 4, N1). */
function groups(value: unknown, project: string | undefined): RelationshipGroup[] {
    return records(value).flatMap(group => {
        const type = text(group.type, 60);
        if (!type) return [];
        const files = records(group.files).map(file => ({ path: text(file.path, 240) ?? '', symbols: records(file.symbols)
            .flatMap(symbol => {
                const name = text(symbol.name, 120), kind = text(symbol.kind, 40);
                return name ? [{ name, kind, ...kind === 'Branch' && project ? { project } : {} }] : [];
            }) }))
            .filter(file => file.symbols.length);
        const listed = files.reduce((sum, file) => sum + file.symbols.length, 0);
        return [{ type, count: Math.max(count(group.count), listed), files }];
    });
}
const totals = (value: unknown) => records(value).flatMap(item => { const type = text(item.type, 60); return type ? [{ type, count: count(item.count) }] : []; });

export function readGalaxyEvidence(snapshot: string): GalaxyEvidence | undefined {
    if (snapshot.length > 128_000) return undefined;
    let parsed: Record<string, unknown> | undefined;
    try { parsed = record(JSON.parse(snapshot)); } catch { return undefined; }
    const evidence = record(parsed?.evidence);
    const selected = record(evidence?.selected), scope = record(evidence?.scope), limits = record(evidence?.limitations);
    const identity = record(selected?.scope);
    // A traced Galaxy scope, recognised by its identity and depth even when its relationships were cut.
    if (evidence?.kind !== 'current-selection-evidence' || evidence.view !== 'galaxy' || typeof identity?.kind !== 'string'
        || typeof scope?.depth !== 'number') return undefined;
    const relationships = record(evidence.relationships);
    const truncated = !relationships || !Array.isArray(relationships.incoming) || !Array.isArray(relationships.outgoing)
        || records(parsed?.omissions).some(item => typeof item.path === 'string' && item.path.startsWith('$.relationships'));
    const total = (value: unknown) => typeof value === 'number' ? count(value) : undefined;
    const direction = scope.direction === 'inbound' || scope.direction === 'outbound' ? scope.direction : 'both';
    const edgeTypes = Array.isArray(scope.edgeTypes) ? scope.edgeTypes.flatMap(type => text(type, 60) ?? []) : 'all';
    const stopped = record(limits?.renderLimit);
    const renderLimit = stopped && line(stopped.layer) && line(stopped.limit) && (stopped.kind === 'nodes' || stopped.kind === 'edges')
        ? { layer: stopped.layer as number, kind: stopped.kind as 'nodes' | 'edges', limit: stopped.limit as number } : undefined;
    const state = limits?.state === 'complete-indexed-scope' ? 'complete' : limits?.state === 'loading-partial-preview' ? 'loading'
        : limits?.state === 'render-limit-partial' && renderLimit ? 'limited' : 'partial';
    const roots = records(selected?.roots).flatMap(root => {
        const name = text(root.name, 120);
        return name ? [{ name, kind: text(root.kind, 40), qualifiedName: text(root.qualifiedName, 400), filePath: text(root.filePath, 240), startLine: line(root.startLine),
            endLine: line(root.endLine), documentation: text(root.documentation, 300) }] : [];
    });
    const project = text(evidence.project, 120);
    return {
        project: project ?? '', label: text(identity.name, 120) ?? roots[0]?.name ?? 'selection',
        identity, selectionKind: text(identity.kind, 20) ?? 'node',
        roots, rootCount: Math.max(count(selected?.rootCount), roots.length),
        depth: count(scope.depth), direction, edgeTypes, nodes: count(scope.nodes), edges: count(scope.edges),
        state, ...state === 'limited' ? { renderLimit } : {}, error: text(limits?.error, 200), exhausted: limits?.exhausted === true,
        relationships: { incoming: groups(relationships?.incoming, project), incomingSymbols: total(relationships?.incomingSymbols),
            outgoing: groups(relationships?.outgoing, project), outgoingSymbols: total(relationships?.outgoingSymbols),
            internal: totals(relationships?.internal), beyond: totals(relationships?.beyond) },
        truncated, ...scope.display === 'galaxy' || scope.display === 'hierarchy' ? { display: scope.display } : {},
    };
}

/** Water-filling: parts that fit their fair share stay whole; larger parts split the rest. */
export function fairShares(natural: readonly number[], budget: number): number[] {
    const shares = natural.map(() => 0);
    let pool = Math.max(0, Math.floor(budget));
    const order = natural.map((_, index) => index).sort((left, right) => natural[left] - natural[right]);
    order.forEach((index, position) => {
        shares[index] = Math.min(natural[index], Math.floor(pool / (order.length - position)));
        pool -= shares[index];
    });
    return shares;
}

/** One edge type: complete count, then names until the budget, then an explicit "+N more".
 * A listed answer heads it "**TESTS (11):**", the prompt "TESTS (11):"; "CALLS from 11" was
 * repeated by the model as if it were a sentence (C2, W2). Both sides now head a line alike;
 * the side stays in the signature for its callers. */
export function relationshipLine(group: RelationshipGroup, _side: 'incoming' | 'outgoing', budget: number,
    words: RelationshipWords, markdown = false): { text: string; listed: number } {
    const quote = (value: string) => markdown ? `\`${value.replace(/`/g, "'")}\`` : value;
    const head = markdown ? `- **${words.typeCount(group.type, group.count)}:** ` : `- ${words.typeCount(group.type, group.count)}: `;
    let body = '', listed = 0;
    const more = (shown: number) => group.count > shown ? `${body ? '; ' : ''}${words.more(group.count - shown)}` : '';
    if (head.length + more(0).length > budget) return { text: '', listed: 0 };
    for (const file of group.files) {
        const kinds = new Set(file.symbols.map(symbol => symbol.kind));
        const kind = kinds.size === 1 ? file.symbols[0].kind : undefined;
        const where = [kind ? words.kindName(kind) : undefined, file.path ? quote(file.path) : undefined].filter(Boolean).join(', ');
        const suffix = where ? ` (${where})` : '';
        let chunk = '';
        for (const symbol of file.symbols) {
            const next = `${chunk ? `${chunk}, ` : body ? '; ' : ''}${quote(nodeDisplayName(symbol, words.nodeNames))}`;
            const shown = listed + 1;
            const after = group.count > shown ? `; ${words.more(group.count - shown)}` : '';
            if (head.length + body.length + next.length + suffix.length + after.length > budget) {
                if (chunk) body += chunk + suffix;
                return { text: head + body + more(listed), listed };
            }
            chunk = next; listed = shown;
        }
        if (chunk) body += chunk + suffix;
    }
    return { text: head + body + more(listed), listed };
}

/** The direction, the edge types and how complete the scope is, in words. */
export function scopeParts(evidence: GalaxyEvidence, words: RelationshipWords): { direction: string; types: string; state: string } {
    const direction = evidence.direction === 'inbound' ? words.inbound : evidence.direction === 'outbound' ? words.outbound : words.both;
    const types = evidence.edgeTypes === 'all' ? words.allTypes : words.onlyTypes(evidence.edgeTypes);
    const state = evidence.state === 'complete' ? words.complete : evidence.state === 'loading' ? words.loading
        : evidence.state === 'limited' && evidence.renderLimit ? words.renderLimited(evidence.renderLimit.layer, evidence.renderLimit.limit, evidence.renderLimit.kind)
            : words.partial(evidence.error);
    const notes = [state, ...evidence.state === 'complete' && evidence.exhausted ? [words.exhausted] : [], ...evidence.truncated ? [words.truncated] : []];
    return { direction, types, state: notes.join('; ') };
}

/** "1 hop in both directions, all relationship types; complete." in words, never as fields. */
export function scopeSentence(evidence: GalaxyEvidence, words: RelationshipWords): string {
    const { direction, types, state } = scopeParts(evidence, words);
    return words.scope(`${words.hops(evidence.depth)} ${direction}, ${types}`, words.size(evidence.nodes, evidence.edges), state);
}

/** Whether the loaded scope followed this side at all; otherwise "none" would be a guess. */
export function sideLoaded(evidence: GalaxyEvidence, side: 'incoming' | 'outgoing'): boolean {
    return evidence.depth > 0 && evidence.direction !== (side === 'incoming' ? 'outbound' : 'inbound');
}

/** A root as the answer names it: a Branch node "django-demo · detached HEAD", every other by its name (round 4, N1). */
const rootName = (root: GalaxyEvidence['roots'][number], words: RelationshipWords) => nodeDisplayName(root, words.nodeNames);

/**
 * The selection as the answer names it. The label stays the name of the
 * index, because the chat matches typed names against it; what is written
 * names a Branch node for what it is, in the language of the answer.
 */
export function selectionName(evidence: GalaxyEvidence, words: RelationshipWords): string {
    const [first] = evidence.roots;
    if (evidence.rootCount <= 1 && first && first.name === evidence.label) return rootName(first, words);
    const identity = record(evidence.identity);
    return nodeDisplayName({ name: evidence.label, qualifiedName: typeof identity?.qualifiedName === 'string' ? identity.qualifiedName : undefined }, words.nodeNames);
}

/** What is selected, in the words of the prompt (English) or of a question (C5). */
export function selectionSentence(evidence: GalaxyEvidence, words: RelationshipWords): string[] {
    const [first] = evidence.roots;
    // A folder or a file is its path; "Selected: .github (Folder) in .github." only repeated the name (K47).
    const isPath = (root: GalaxyEvidence['roots'][number]) => /^(?:folder|file|directory)$/i.test(root.kind ?? '')
        && Boolean(root.filePath) && (root.filePath === root.name || root.filePath!.endsWith(`/${root.name}`));
    const shown = (root: GalaxyEvidence['roots'][number]) => isPath(root) ? root.filePath! : rootName(root, words);
    const range = (root: GalaxyEvidence['roots'][number]) => root.filePath && !isPath(root)
        ? ` in ${root.filePath}${root.startLine ? `:${root.startLine}${root.endLine && root.endLine !== root.startLine ? `-${root.endLine}` : ''}` : ''}` : '';
    // Kinds in the words of the answer: "(Klasse)" in a German one (W8).
    const kind = (root: GalaxyEvidence['roots'][number]) => root.kind ? ` (${words.kindName(root.kind)})` : '';
    if (evidence.rootCount <= 1 && first) {
        return [words.selected(`${shown(first)}${kind(first)}${range(first)}`),
            ...first.documentation ? [words.documentation(first.documentation)] : []];
    }
    if (!first) return [words.notInScope(selectionName(evidence, words), words.kindName(evidence.selectionKind))];
    const listed = evidence.roots.map(root => `${rootName(root, words)}${kind(root)}`).join(', ');
    return [words.selectedGroup(words.kindName(evidence.selectionKind), selectionName(evidence, words), evidence.rootCount, listed, evidence.rootCount - evidence.roots.length)];
}
