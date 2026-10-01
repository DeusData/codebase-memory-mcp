import { hierarchyBlockPositions } from './hierarchy-blocks';
import { RpcIntelligenceClient } from '../provider/rpc-client';
import { escapeLiteral } from '../provider/cypher';
import type { QueryGraphResult } from '../provider/rpc-schemas';
import { readerGraphFocus, type SourceFocusRange } from './reader-graph-focus';
import type { GraphData, GraphEdge, GraphNode } from './types';
import type { GraphNeighborhoodTransaction } from './graph-neighborhood-cache';

export type TraceDirection = 'both' | 'inbound' | 'outbound';
export type GraphScope = { kind: 'node'; id: number; name: string; qualifiedName?: string } | { kind: 'symbol'; qualifiedName: string; name: string } | { kind: 'file' | 'folder'; path: string; name: string; range?: SourceFocusRange };
export interface ScopedGraph { data: GraphData; roots: Set<number>; depth: number; exhausted: boolean; frontier?: number[]; levels?: Map<number, number>; traversalKey?: string }
export interface GraphQueryClient { queryGraph(project: string, query: string, cursor?: string): Promise<QueryGraphResult> }
/** Undefined includes every relationship type; an explicit empty list includes none. */
export function graphEdgeTypesKey(edgeTypes?: readonly string[]): string {
    return JSON.stringify(edgeTypes === undefined ? null : [...new Set(edgeTypes)].sort());
}
const BATCH_SIZE = 64;
const number = (value: string | undefined): number | undefined => value !== undefined && value !== '' && Number.isFinite(Number(value)) ? Number(value) : undefined;
const id = (value: string | undefined): number => {
    const parsed = number(value);
    if (parsed === undefined || !Number.isSafeInteger(parsed) || parsed < 0) throw new Error('Graph response contains an invalid identity.');
    return parsed;
};
const nodeColumns = (alias: string, prefix: string) => [`id(${alias}) AS ${prefix}id`, `${alias}.label AS ${prefix}label`, `${alias}.name AS ${prefix}name`, `${alias}.qualified_name AS ${prefix}qn`, `${alias}.file_path AS ${prefix}file`, `${alias}.start_line AS ${prefix}start_line`, `${alias}.end_line AS ${prefix}end_line`].join(', ');

/** The cursor binds a complete query result to one graph generation. Every
 * continuation is consumed; a daemon cap or missing cursor is never silently
 * accepted as the selected file's complete neighborhood. */
export async function readGraphPages(client: GraphQueryClient, project: string, query: string, key: string, signal?: AbortSignal): Promise<Record<string, string>[]> {
    signal?.throwIfAborted();
    let page = await client.queryGraph(project, query);
    const columns = page.columns, total = page.total, records: Record<string, string>[] = [];
    const cursors = new Set<string>(), identities = new Set<number>();
    for (;;) {
        signal?.throwIfAborted();
        if (JSON.stringify(columns) !== JSON.stringify(page.columns) || page.total !== total
            || (page.offset !== undefined && page.offset !== records.length)) throw new Error('Graph pagination changed its result snapshot.');
        for (const row of page.rows) {
            const record = Object.fromEntries(columns.map((column, index) => [column, row[index] ?? '']));
            const identity = id(record[key]);
            if (identities.has(identity)) throw new Error('Graph pagination repeated a row.');
            identities.add(identity); records.push(record);
        }
        if (!page.nextCursor) {
            if (page.hasMore || page.truncated || page.totalRelation === 'gte' || (total !== undefined && records.length < total))
                throw new Error(`Incomplete graph response (${records.length} relationships read); ${page.truncationReason ?? 'the server did not provide a continuation'}.`);
            return records;
        }
        if (!page.rows.length || cursors.has(page.nextCursor)) throw new Error('Graph pagination did not advance.');
        if (page.nextOffset !== undefined && page.nextOffset !== records.length) throw new Error('Graph pagination skipped rows.');
        cursors.add(page.nextCursor); signal?.throwIfAborted();
        page = await client.queryGraph(project, query, page.nextCursor);
    }
}

const languageColors: Record<string, string> = { ts: '#6ea6be', tsx: '#6ea6be', js: '#c3b775', jsx: '#c3b775', c: '#92a8b4', h: '#92a8b4', cpp: '#b28da6', py: '#8daf88', go: '#77b2b4', rs: '#bd947c', java: '#bd9281', cs: '#a392bb', rb: '#b78288', swift: '#bf9b85', kt: '#af97ba' };
function readNode(row: Record<string, string>, prefix: string, layout: Map<number, GraphNode>): GraphNode {
    const identity = id(row[`${prefix}id`]), known = layout.get(identity);
    const path = row[`${prefix}file`] || undefined;
    return { ...known, id: identity, name: row[`${prefix}name`] || String(identity), label: row[`${prefix}label`] || known?.label || '',
        qualified_name: row[`${prefix}qn`] || undefined, file_path: path,
        start_line: number(row[`${prefix}start_line`]), end_line: number(row[`${prefix}end_line`]),
        x: known?.x ?? 0, y: known?.y ?? 0, z: known?.z ?? 0,
        size: known?.size ?? 2.8, color: known?.color ?? languageColors[path?.split('.').pop() ?? ''] ?? '#97a6ad' };
}
const anyNames = (alias: string, names: readonly string[]) => `(${names.map(value => `${alias}.qualified_name = "${escapeLiteral(value)}"`).join(' OR ')})`;

/** Deterministic concentric hop rings. Names within a folder stay adjacent;
 * distances encode relationship depth, never runtime or architectural certainty. */
export function arrangeScopedGraph(nodes: readonly GraphNode[], edges: readonly GraphEdge[], roots: ReadonlySet<number>, discoveredLevels?: ReadonlyMap<number, number>): GraphData {
    const levels = new Map<number, number>(discoveredLevels);
    roots.forEach(value => levels.set(value, 0));
    const adjacency = new Map<number, number[]>();
    for (const edge of edges) {
        const from = adjacency.get(edge.source) ?? [], to = adjacency.get(edge.target) ?? [];
        from.push(edge.target); to.push(edge.source); adjacency.set(edge.source, from); adjacency.set(edge.target, to);
    }
    const queue = [...roots];
    for (let at = 0; !discoveredLevels && at < queue.length; at++) for (const next of adjacency.get(queue[at]!) ?? []) {
        if (levels.has(next)) continue;
        levels.set(next, levels.get(queue[at]!)! + 1); queue.push(next);
    }
    const rings = new Map<number, GraphNode[]>();
    for (const node of nodes) {
        const level = levels.get(node.id) ?? 0;
        const ring = rings.get(level) ?? []; ring.push(node); rings.set(level, ring);
    }
    const arranged: GraphNode[] = [];
    let radius = 0;
    for (const [level, ring] of [...rings].sort(([a], [b]) => a - b)) {
        ring.sort((a, b) => (a.file_path ?? '').localeCompare(b.file_path ?? '') || a.name.localeCompare(b.name) || a.id - b.id);
        radius = level === 0 && ring.length === 1 ? 0 : Math.max(radius + 70, ring.length * 8 / Math.PI);
        ring.forEach((node, index) => {
            const angle = index / ring.length * Math.PI * 2;
            arranged.push({ ...node, x: Math.cos(angle) * radius, y: Math.sin(angle) * radius, z: level * -18,
                size: Math.min(5, Math.max(2, node.size)) });
        });
    }
    return { nodes: arranged, edges: [...edges], total_nodes: arranged.length };
}

export async function loadGraphScope(project: string, scope: GraphScope, depth: number, direction: TraceDirection,
    layout: GraphData | undefined, options: { fetch?: typeof globalThis.fetch; signal?: AbortSignal; client?: GraphQueryClient; previous?: ScopedGraph; edgeTypes?: readonly string[]; cache?: GraphNeighborhoodTransaction } = {}): Promise<ScopedGraph> {
    options.signal?.throwIfAborted();
    const client = options.client ?? new RpcIntelligenceClient({ fetch: options.fetch, signal: options.signal });
    const edgeTypes = options.edgeTypes === undefined ? undefined : [...new Set(options.edgeTypes)].sort();
    if (edgeTypes?.some(type => !/^[A-Za-z_][A-Za-z0-9_]*$/.test(type))) throw new Error('Invalid graph relationship type.');
    const allowedTypes = edgeTypes === undefined ? undefined : new Set(edgeTypes);
    const traversalKey = JSON.stringify([project, scope, direction, graphEdgeTypesKey(edgeTypes)]);
    const relationship = edgeTypes?.length ? `[r:${edgeTypes.join('|')}]` : '[r]';
    const known = new Map(layout?.nodes.map(node => [node.id, node]) ?? []);
    if (scope.kind === 'node') {
        const cachedNode = options.cache?.node(scope.id);
        if (cachedNode && (!scope.qualifiedName || cachedNode.qualified_name === scope.qualifiedName)) known.set(scope.id, cachedNode);
    }
    if (scope.kind === 'node' && !(scope.qualifiedName ?? known.get(scope.id)?.qualified_name))
        throw new Error('This node has no indexed qualified identity; its relationships cannot be retrieved reliably.');
    const predicate = scope.kind === 'node' ? `n.qualified_name = "${escapeLiteral(scope.qualifiedName ?? known.get(scope.id)?.qualified_name ?? '')}"` : scope.kind === 'symbol' ? `n.qualified_name = "${escapeLiteral(scope.qualifiedName)}"` : scope.kind === 'file' ? `n.file_path = "${escapeLiteral(scope.path)}"`
        : `n.file_path STARTS WITH "${escapeLiteral(scope.path.replace(/\/$/, '') + '/')}"`;
    const previous = options.previous?.traversalKey === traversalKey && options.previous.depth <= depth ? options.previous : undefined;
    const cachedNode = scope.kind === 'node' ? options.cache?.node(scope.id) : undefined;
    let rootNodes = cachedNode && cachedNode.qualified_name === (scope.kind === 'node' ? scope.qualifiedName ?? known.get(scope.id)?.qualified_name : '')
        ? [cachedNode] : options.cache?.roots(predicate);
    if (!previous && !rootNodes) {
        const rootRows = await readGraphPages(client, project, `MATCH (n) WHERE ${predicate} RETURN ${nodeColumns('n', '')}`, 'id', options.signal);
        rootNodes = rootRows.map(row => readNode(row, '', known));
        options.cache?.rememberRoots(predicate, rootNodes);
    }
    const candidates = previous ? previous.data.nodes.filter(node => previous.roots.has(node.id))
        : rootNodes!.filter(node => scope.kind !== 'node' || node.id === scope.id);
    const roots = scope.kind === 'file' && scope.range ? readerGraphFocus(candidates, scope.path, scope.range).ids : new Set(candidates.map(node => node.id));
    const nodes = new Map((previous?.data.nodes ?? candidates.filter(node => roots.has(node.id))).map(node => [node.id, node]));
    const levels = new Map<number, number>(previous?.levels ?? [...roots].map(identity => [identity, 0] as const));
    const edges = new Map<number, GraphEdge>(previous?.data.edges.filter(edge => edge.id !== undefined).map(edge => [edge.id!, edge]) ?? []);
    let frontier = edgeTypes?.length === 0 ? [] : previous?.frontier ?? [...roots], reachedDepth = previous?.depth ?? 0;
    for (let hop = reachedDepth; hop < Math.max(depth, roots.size > 1 ? 1 : 0) && frontier.length; hop++) {
        const next = new Set<number>();
        const fileBoundary = hop === 0 && (scope.kind === 'folder' || (scope.kind === 'file' && !scope.range));
        const accept = (evidenceNodes: readonly GraphNode[], evidenceEdges: readonly GraphEdge[]) => {
            const byId = new Map(evidenceNodes.map(node => [node.id, node]));
            for (const edge of evidenceEdges) {
                for (const identity of [edge.source, edge.target]) {
                    const node = byId.get(identity)!;
                    if (!nodes.has(identity)) { next.add(identity); levels.set(identity, hop + 1); }
                    nodes.set(identity, node);
                }
                edges.set(edge.id!, edge);
            }
        };
        const legs = direction === 'both' ? ['outbound', 'inbound'] as const : [direction];
        for (const leg of legs) {
            const missing: number[] = [];
            for (const identity of frontier) {
                const cached = options.cache?.neighborhood(identity, leg, edgeTypes);
                if (cached) accept(cached.nodes, cached.edges); else missing.push(identity);
            }
            const useBoundary = fileBoundary && missing.length === frontier.length;
            const batchSize = useBoundary ? Math.max(1, missing.length) : BATCH_SIZE;
            for (let at = 0; at < missing.length; at += batchSize) {
                const batch = missing.slice(at, at + batchSize), batchIds = new Set(batch);
                const names = batch.map(value => nodes.get(value)?.qualified_name);
                if (!useBoundary && names.some(name => !name)) throw new Error('A selected graph node has no qualified identity; its dependencies cannot be loaded completely.');
                const alias = leg === 'inbound' ? 'b' : 'a';
                const relation = useBoundary ? scope.kind === 'folder'
                    ? `${alias}.file_path STARTS WITH "${escapeLiteral(scope.path.replace(/\/$/, '') + '/')}"`
                    : `${alias}.file_path = "${escapeLiteral(scope.kind === 'file' ? scope.path : '')}"`
                    : anyNames(alias, names as string[]);
                // Keep each predicate on the left seed, so early WHERE prunes before
                // adjacency expansion. An OR spanning a and b materializes the repo.
                const pattern = leg === 'inbound' ? `(b)<-${relationship}-(a)` : `(a)-${relationship}->(b)`;
                const rows = await readGraphPages(client, project, `MATCH ${pattern} WHERE ${relation} RETURN id(r) AS edge_id, type(r) AS edge_type, r.line AS edge_line, ${nodeColumns('a', 'a_')}, ${nodeColumns('b', 'b_')}`, 'edge_id', options.signal);
                const evidenceNodes = new Map(batch.map(identity => [identity, nodes.get(identity)!]));
                const evidenceEdges: GraphEdge[] = [];
                for (const row of rows) {
                    // Filter before discovering a frontier, including on older servers
                    // whose typed-pattern implementation may return extra rows.
                    if (allowedTypes && !allowedTypes.has(row.edge_type ?? '')) continue;
                    const source = readNode(row, 'a_', known), target = readNode(row, 'b_', known);
                    const incident = leg === 'inbound' ? batchIds.has(target.id) : batchIds.has(source.id);
                    if (!incident) continue; // Qualified names are not assumed unique.
                    evidenceNodes.set(source.id, source); evidenceNodes.set(target.id, target);
                    const identity = id(row.edge_id);
                    evidenceEdges.push({ id: identity, source: source.id, target: target.id, type: row.edge_type ?? '', line: number(row.edge_line) });
                }
                const verifiedNodes = [...evidenceNodes.values()];
                options.cache?.rememberNeighborhood(batch, leg, edgeTypes, verifiedNodes, evidenceEdges);
                accept(verifiedNodes, evidenceEdges);
            }
        }
        reachedDepth = hop + 1;
        frontier = [...next];
    }
    // A cluster starts with all of its members and their internal edges.
    if (depth === 0 && roots.size > 1) {
        const initial = [...edges.values()].filter(edge => roots.has(edge.source) && roots.has(edge.target));
        return { data: arrangeScopedGraph([...nodes.values()].filter(node => roots.has(node.id)), initial, roots, levels), roots, depth: 0, exhausted: edgeTypes?.length === 0, frontier: edgeTypes?.length === 0 ? [] : [...roots], levels, traversalKey };
    }
    return { data: arrangeScopedGraph([...nodes.values()], [...edges.values()], roots, levels), roots, depth: reachedDepth, exhausted: edgeTypes?.length === 0 || (depth > 0 && frontier.length === 0), frontier, levels, traversalKey };
}

export function limitGraphRender(data: GraphData, nodeLimit: number, edgeLimit: number, required: ReadonlySet<number> = new Set()): GraphData {
    const nodes = [...data.nodes.filter(node => required.has(node.id)), ...data.nodes.filter(node => !required.has(node.id))]
        .slice(0, Math.max(0, Math.floor(nodeLimit)));
    const ids = new Set(nodes.map(node => node.id));
    const edges = data.edges.filter(edge => ids.has(edge.source) && ids.has(edge.target)).slice(0, edgeLimit);
    return { ...data, nodes, edges };
}

/** The same evidence in successive hop blocks. Small hops keep one column;
 * large hops wrap into compact grids so fitting thousands of neighbors does
 * not turn the entire scene into a single vertical line. */
export function scopedHierarchy(scope: ScopedGraph, name: string): import('./hierarchy-layout').HierarchyProjection {
    const columns = new Map<number, GraphNode[]>();
    for (const node of scope.data.nodes) {
        const hop = Math.max(0, Math.round(-node.z / 18));
        const entries = columns.get(hop) ?? []; entries.push(node); columns.set(hop, entries);
    }
    const nodes: GraphNode[] = [], remap = new Map<number, number>();
    const placements: import('./hierarchy-layout').HierarchyPlacement[] = [];
    const blocks = [...columns].map(([level, entries]) => ({ level,
        entries: entries.sort((a, b) => (a.file_path ?? '').localeCompare(b.file_path ?? '') || a.name.localeCompare(b.name)) }));
    for (const { node, level: hop, x, y } of hierarchyBlockPositions(blocks, { levelGap: 160, rowGap: 32 })) {
        const id = nodes.length;
        remap.set(node.id, id); nodes.push({ ...node, id, x, y, z: 0 });
        placements.push({ id, key: node.qualified_name ?? `node:${node.id}`, name: node.name, hop, x, y });
    }
    return { data: { nodes, edges: scope.data.edges.map(edge => ({ ...edge, source: remap.get(edge.source)!, target: remap.get(edge.target)! })), total_nodes: nodes.length },
        rootId: scope.roots.size === 1 ? remap.get([...scope.roots][0]!) ?? -1 : -1, rootKey: name, rootName: name,
        symbols: nodes.length, depth: columns.size, truncated: false, cap: nodes.length, walkDepth: scope.depth,
        missing: 0, placements };
}
