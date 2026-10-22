import { useEffect } from 'react';
import type { BrowserChatContext } from '../browser-ai/chat-model';
import { fairShares } from '../browser-ai/galaxy-evidence';
import type { GraphScope } from './graph-scope';
import { scopeDisplayName } from './node-names';
import type { GraphEdge, GraphNode } from './types';

export type SelectionEvidenceListener = (context: BrowserChatContext | undefined) => void;
export interface SelectionEvidence {
    project: string;
    view: string;
    label: string;
    source: string;
    generation?: string;
    selected: unknown;
    relationships?: unknown;
    scope?: unknown;
    limitations: unknown;
}

/** Source identity only. Camera coordinates and visual weights are not code facts. */
export function graphNodeEvidence(node: GraphNode) {
    return { id: node.id, name: node.name, kind: node.label, qualifiedName: node.qualified_name,
        filePath: node.file_path, startLine: node.start_line, endLine: node.end_line,
        status: node.status, incomingCalls: node.in_calls, outgoingCalls: node.out_calls,
        documentation: node.documentation, packageName: node.package_name };
}

/** Related symbols of one edge type and direction, grouped by file. `count` is
 * complete; `files` lists at most `RELATED_NAMES_PER_GROUP` symbols. A reader
 * gives a Branch symbol the `project` of the snapshot, for its shown name. */
export interface RelationshipGroup { type: string; count: number; files: { path: string; symbols: { name: string; kind?: string; project?: string }[] }[] }
export interface ScopeRelationships {
    incomingSymbols: number;
    outgoingSymbols: number;
    /** Edge counts between selected roots and between symbols further out. */
    internal: { type: string; count: number }[];
    beyond: { type: string; count: number }[];
    /** Edges into a selected root from outside the selection. */
    incoming: RelationshipGroup[];
    /** Edges from a selected root to outside the selection. */
    outgoing: RelationshipGroup[];
}
export const RELATED_NAMES_PER_GROUP = 24;
/** Characters of names and paths across every group of both sides. With the bounded
 * roots this keeps a scope inside the snapshot budget, so no count or type is cut. */
export const RELATED_NAME_CHARACTERS = 6000;
/** Readers of the evidence use no more of a root's documentation than this. */
export const ROOT_DOCUMENTATION_CHARACTERS = 300;
const ROOTS_LISTED = 8;

type RelatedSymbol = { name: string; kind?: string; path: string };
/** Roughly what a listed symbol, and the first symbol of each file, add to the snapshot. */
function namesCost(symbols: readonly RelatedSymbol[]): number {
    return symbols.reduce((sum, symbol, index) => sum + symbol.name.length + (symbol.kind?.length ?? 0) + 24
        + (index === 0 || symbols[index - 1].path !== symbol.path ? symbol.path.length + 24 : 0), 0);
}

/** Classify every scope edge by its direction relative to the roots before any
 * bound applies, so counts stay complete and callers never mix with callees.
 * Counts come first; names share an explicit budget fairly between the groups. */
export function scopeRelationships(nodes: readonly GraphNode[], edges: readonly GraphEdge[], roots: ReadonlySet<number>,
    nameCharacters = RELATED_NAME_CHARACTERS): ScopeRelationships {
    const byId = new Map(nodes.map(node => [node.id, node]));
    const related = { incoming: new Map<string, Set<number>>(), outgoing: new Map<string, Set<number>>() };
    const counted = { internal: new Map<string, number>(), beyond: new Map<string, number>() };
    for (const edge of edges) {
        const from = roots.has(edge.source), to = roots.has(edge.target);
        if (from !== to) {
            const side = to ? related.incoming : related.outgoing;
            const members = side.get(edge.type) ?? new Set<number>();
            members.add(to ? edge.source : edge.target); side.set(edge.type, members);
        } else {
            const side = from ? counted.internal : counted.beyond;
            side.set(edge.type, (side.get(edge.type) ?? 0) + 1);
        }
    }
    const ranked = (members: ReadonlySet<number>): RelatedSymbol[] => {
        const symbols = [...members].map(id => {
            const node = byId.get(id);
            return { name: node?.name ?? `#${id}`, kind: node?.label || undefined, path: node?.file_path ?? '' };
        });
        const perFile = new Map<string, number>();
        symbols.forEach(symbol => perFile.set(symbol.path, (perFile.get(symbol.path) ?? 0) + 1));
        // Files with the most related symbols first, so a bounded list keeps the densest evidence.
        return symbols.sort((left, right) => perFile.get(right.path)! - perFile.get(left.path)!
            || left.path.localeCompare(right.path) || left.name.localeCompare(right.name)).slice(0, RELATED_NAMES_PER_GROUP);
    };
    const ordered = (side: Map<string, Set<number>>) => [...side].sort(([leftType, left], [rightType, right]) =>
        right.size - left.size || leftType.localeCompare(rightType));
    const sides = [ordered(related.incoming), ordered(related.outgoing)];
    const candidates = sides.flat().map(([, members]) => ranked(members));
    const shares = fairShares(candidates.map(namesCost), nameCharacters);
    let next = 0;
    const [incoming, outgoing] = sides.map(side => side.map(([type, members]): RelationshipGroup => {
        const index = next++;
        let symbols = candidates[index];
        while (symbols.length && namesCost(symbols) > shares[index]) symbols = symbols.slice(0, -1);
        const files = new Map<string, { name: string; kind?: string }[]>();
        for (const { path, ...symbol } of symbols) files.set(path, [...files.get(path) ?? [], symbol]);
        return { type, count: members.size, files: [...files].map(([path, listed]) => ({ path, symbols: listed })) };
    }));
    const distinct = (side: Map<string, Set<number>>) => new Set([...side.values()].flatMap(members => [...members])).size;
    const totals = (side: Map<string, number>) => [...side].map(([type, count]) => ({ type, count }))
        .sort((left, right) => right.count - left.count || left.type.localeCompare(right.type));
    return { incomingSymbols: distinct(related.incoming), outgoingSymbols: distinct(related.outgoing),
        internal: totals(counted.internal), beyond: totals(counted.beyond), incoming, outgoing };
}

/** A traced Galaxy scope as the local agent reads it. */
export interface GalaxyScope {
    project: string;
    identity: GraphScope;
    nodes: readonly GraphNode[];
    edges: readonly GraphEdge[];
    roots: ReadonlySet<number>;
    depth: number;
    direction: string;
    edgeTypes: readonly string[] | 'all';
    /** `render-limit-partial`: loading finished, but a layer stopped at the render limit (C1). */
    state: 'complete-indexed-scope' | 'loading-partial-preview' | 'partial' | 'render-limit-partial';
    error?: string;
    exhausted?: boolean;
    /** The layer that stopped and the limit it stopped at. */
    renderLimit?: { layer: number; kind: 'nodes' | 'edges'; limit: number };
    /** Which picture of the scope Galaxy shows, so the chat can say how to read it (H1). */
    display?: 'galaxy' | 'hierarchy';
}

/** Every scope edge is classified against the roots first; only the names are
 * bounded afterwards. Render budgets are a drawing concern and stay out of it.
 * The label is the name the Galaxy shows ("django-demo · detached HEAD" for a
 * Branch node, round 4, N1); the snapshot keeps the names of the index. */
export function galaxyScopeEvidence(scope: GalaxyScope): SelectionEvidence {
    const roots = scope.nodes.filter(node => scope.roots.has(node.id));
    return { project: scope.project, view: 'galaxy', source: 'query_graph scoped indexed relationships', label: scopeDisplayName(scope.identity),
        selected: { scope: scope.identity, rootCount: roots.length, roots: roots.slice(0, ROOTS_LISTED).map(root => ({ ...graphNodeEvidence(root),
            documentation: root.documentation?.slice(0, ROOT_DOCUMENTATION_CHARACTERS) })), omittedRoots: Math.max(0, roots.length - ROOTS_LISTED) },
        scope: { depth: scope.depth, direction: scope.direction, edgeTypes: scope.edgeTypes, nodes: scope.nodes.length, edges: scope.edges.length,
            ...scope.display ? { display: scope.display } : {} },
        relationships: scopeRelationships(scope.nodes, scope.edges, scope.roots),
        limitations: { state: scope.state, error: scope.error, exhausted: scope.exhausted, ...scope.renderLimit ? { renderLimit: scope.renderLimit } : {}, indexCoverage: 'unavailable',
            interpretation: 'Static indexed relationships, not runtime activity. Scope completeness is relative to the indexed graph and selected depth/types.' } };
}

type Row = Record<string, unknown>;
const row = (value: unknown): Row | undefined => value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Row : undefined;
/** Identity and location of a symbol; documentation, status and counts stay with the view. */
function symbolIdentity(value: unknown, located = true): unknown {
    const node = row(value);
    if (!node) return value;
    const { name, kind, label, qualifiedName, qualified_name, filePath, file_path, startLine, start_line, endLine, end_line } = node;
    return located ? { name, kind: kind ?? label, qualifiedName: qualifiedName ?? qualified_name, filePath: filePath ?? file_path, startLine: startLine ?? start_line, endLine: endLine ?? end_line }
        : { name, kind: kind ?? label };
}
const ARCHITECTURE_CONNECTIONS = 12;
/** An Architecture area carries 24 documented members and 24 connections with 24 examples
 * each. Bounded as they come, the members' documentation used the whole snapshot and the
 * connections were cut. The strongest connections stay, each with one example; members
 * keep their names (a single member, a symbol, also its location). */
function compactArchitecture(evidence: SelectionEvidence): SelectionEvidence {
    const selected = row(evidence.selected), relationships = row(evidence.relationships);
    const list = Array.isArray(selected?.members) ? selected.members : undefined;
    const members = list ? { members: list.map(member => symbolIdentity(member, list.length === 1)) } : {};
    const all = Array.isArray(relationships?.items) ? relationships.items : undefined;
    const strength = (item: unknown) => { const total = row(item)?.count; return typeof total === 'number' ? total : 0; };
    const items = all ? { items: [...all].sort((left, right) => strength(right) - strength(left)).slice(0, ARCHITECTURE_CONNECTIONS).map(item => {
        const edge = row(item);
        if (!edge || !Array.isArray(edge.evidence)) return item;
        const examples = edge.evidence.slice(0, 1).map(example => {
            const pair = row(example), source = row(pair?.source), target = row(pair?.target);
            return pair ? { line: pair.line, source: { name: source?.name, filePath: source?.filePath ?? source?.file_path }, target: { name: target?.name } } : example;
        });
        return { ...edge, evidence: examples, omittedEvidence: (typeof edge.omittedEvidence === 'number' ? edge.omittedEvidence : 0) + edge.evidence.length - examples.length };
    }), omitted: (typeof relationships?.omitted === 'number' ? relationships.omitted : 0) + Math.max(0, all.length - ARCHITECTURE_CONNECTIONS) } : {};
    return { ...evidence, selected: selected ? { ...selected, ...members } : evidence.selected, relationships: relationships ? { ...relationships, ...items } : evidence.relationships };
}

/** Bound every collection/string and the total snapshot; report each omission.
 * Stable content identity prevents camera changes from triggering explanations. */
export function selectionEvidenceContext(original: SelectionEvidence): BrowserChatContext {
    const architecture = original.view.startsWith('architecture-');
    const evidence = architecture ? compactArchitecture(original) : original;
    const omissions: { path: string; kind: string; count: number }[] = [];
    let budget = 16_000;
    const copy = (value: unknown, path: string, depth: number): unknown => {
        if (value === undefined) return undefined;
        if (budget <= 0 || depth > 10) {
            omissions.push({ path, kind: 'value', count: 1 }); return null;
        }
        if (typeof value === 'string') {
            const limit = Math.max(0, Math.min(1200, budget));
            budget -= Math.min(value.length, limit);
            if (value.length > limit) omissions.push({ path, kind: 'characters', count: value.length - limit });
            return value.slice(0, limit);
        }
        if (value === null || typeof value !== 'object') { budget -= 16; return value; }
        if (Array.isArray(value)) {
            const result: unknown[] = [];
            for (let i = 0; i < Math.min(value.length, 24) && budget > 0; i++) result.push(copy(value[i], `${path}[${i}]`, depth + 1));
            if (result.length < value.length) omissions.push({ path, kind: 'items', count: value.length - result.length });
            return result;
        }
        const entries = Object.entries(value), result: Record<string, unknown> = {};
        let processed = 0;
        for (const [key, item] of entries) {
            if (processed >= 48 || budget <= 0) break;
            budget -= key.length + 6; processed++;
            result[key] = copy(item, `${path}.${key}`, depth + 1);
        }
        if (processed < entries.length) omissions.push({ path, kind: 'fields', count: entries.length - processed });
        return result;
    };
    // Keep provenance and limits ahead of potentially large member collections. In
    // Architecture the connections of a part say more than the members listed with it.
    const head = { kind: 'current-selection-evidence', project: evidence.project, view: evidence.view,
        source: evidence.source, generation: evidence.generation ?? 'unavailable', limitations: evidence.limitations, scope: evidence.scope };
    const snapshot = copy(architecture ? { ...head, relationships: evidence.relationships, selected: evidence.selected }
        : { ...head, selected: evidence.selected, relationships: evidence.relationships }, '$', 0);
    const text = JSON.stringify({ evidence: snapshot, omissions });
    // The picture Galaxy draws a scope in is no code fact: in either one the scope keeps its identity (H1).
    const scope = row(row(snapshot)?.scope);
    const identity = scope?.display === undefined ? text : JSON.stringify({ evidence: { ...row(snapshot), scope: { ...scope, display: undefined } }, omissions });
    let hash = 2166136261;
    for (let i = 0; i < identity.length; i++) hash = Math.imul(hash ^ identity.charCodeAt(i), 16777619);
    return { id: `selection:${evidence.project}:${evidence.view}:${(hash >>> 0).toString(16)}:${identity.length}`,
        label: evidence.label.slice(0, 120), text };
}

/** Hidden workspaces must never overwrite evidence from the active workspace. */
export function useSelectionEvidence(listener: SelectionEvidenceListener | undefined, evidence: SelectionEvidence | undefined, active: boolean) {
    const context = evidence ? selectionEvidenceContext(evidence) : undefined;
    const text = context?.text, label = context?.label, id = context?.id;
    useEffect(() => {
        if (active) listener?.(text && id && label ? { id, label, text } : undefined);
    }, [listener, active, text, label, id]);
}
