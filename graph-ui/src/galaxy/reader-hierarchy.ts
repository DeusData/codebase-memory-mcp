import {
    HIERARCHY_COLUMN_WIDTH, HIERARCHY_LABEL_BUDGET, HIERARCHY_LANE_SPACING,
    HIERARCHY_MAX_SIZE, HIERARCHY_MIN_SIZE, HIERARCHY_ROW_HEIGHT, HIERARCHY_SIZE_SCALE,
    type HierarchyProjection,
} from './hierarchy-layout';
import { readerGraphFocus, type SourceFocusRange } from './reader-graph-focus';
import type { GraphData, GraphNode } from './types';

export interface ReaderHierarchyProjection extends HierarchyProjection {
    headline: string;
    edgeNote: string;
    message: string;
}

const compareText = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
const compareNodes = (a: GraphNode, b: GraphNode) => compareText(a.file_path ?? '', b.file_path ?? '')
    || (a.start_line ?? 0) - (b.start_line ?? 0)
    || compareText(a.qualified_name ?? a.name, b.qualified_name ?? b.name) || a.id - b.id;
const definitionEdge = (type: string) => /^(DEFINES(?:_|$)|CONTAINS$|DECLARES$)/.test(type);

/** File definitions and their direct relationships, using only the loaded graph. */
export function projectReaderHierarchy(
    layout: GraphData,
    filePath: string,
    range?: SourceFocusRange,
    cap = HIERARCHY_LABEL_BUDGET,
): ReaderHierarchyProjection | undefined {
    const focus = readerGraphFocus(layout.nodes, filePath, range);
    if (focus.ids.size === 0) return undefined;
    const nodeById = new Map(layout.nodes.map(node => [node.id, node]));
    const roots = layout.nodes.filter(node => focus.ids.has(node.id)).sort(compareNodes);
    const incident = layout.edges.filter(edge => (focus.ids.has(edge.source) || focus.ids.has(edge.target))
        && nodeById.has(edge.source) && nodeById.has(edge.target));
    const contextIds = new Set<number>();
    for (const edge of incident) {
        if (!focus.ids.has(edge.source)) contextIds.add(edge.source);
        if (!focus.ids.has(edge.target)) contextIds.add(edge.target);
    }
    const context = [...contextIds].map(id => nodeById.get(id)!).sort(compareNodes);
    const limit = Number.isFinite(cap) ? Math.max(1, Math.floor(cap)) : HIERARCHY_LABEL_BUDGET;
    // Selected nodes, including isolated definitions, take priority over context.
    const selected = [...roots, ...context].slice(0, limit);
    const included = new Set(selected.map(node => node.id));
    const edges = incident.filter(edge => included.has(edge.source) && included.has(edge.target));
    const includedRoots = selected.filter(node => focus.ids.has(node.id));

    // Only recorded containment/definition edges determine the interior levels.
    // Cycles remain visible at level zero; they cannot force an unbounded layout.
    const levels = new Map(includedRoots.map(node => [node.id, 0]));
    const children = new Map<number, Set<number>>();
    const indegree = new Map(includedRoots.map(node => [node.id, 0]));
    for (const edge of edges) {
        if (!definitionEdge(edge.type) || !levels.has(edge.source) || !levels.has(edge.target)) continue;
        const next = children.get(edge.source) ?? new Set<number>();
        if (next.has(edge.target)) continue;
        next.add(edge.target);
        children.set(edge.source, next);
        indegree.set(edge.target, indegree.get(edge.target)! + 1);
    }
    const queue = includedRoots.filter(node => indegree.get(node.id) === 0).map(node => node.id);
    for (let at = 0; at < queue.length; at += 1) {
        const source = queue[at]!;
        for (const target of children.get(source) ?? []) {
            indegree.set(target, indegree.get(target)! - 1);
            levels.set(target, Math.max(levels.get(target)!, levels.get(source)! + 1));
            if (indegree.get(target) === 0) queue.push(target);
        }
    }
    for (const [id, remaining] of indegree) if (remaining > 0) levels.set(id, 0);
    const lastDefinitionLevel = Math.max(0, ...levels.values());
    const incoming = new Set(edges.filter(edge => focus.ids.has(edge.target)).map(edge => edge.source));
    for (const node of selected) {
        if (!focus.ids.has(node.id)) levels.set(node.id, incoming.has(node.id) ? -1 : lastDefinitionLevel + 1);
    }
    const columns = new Map<number, GraphNode[]>();
    for (const node of selected) {
        const level = levels.get(node.id)!;
        const entries = columns.get(level) ?? [];
        entries.push(node);
        columns.set(level, entries);
    }

    const nodes: GraphNode[] = [];
    const placements: HierarchyProjection['placements'] = [];
    const renderId = new Map<number, number>();
    for (const [level, entries] of [...columns].sort(([a], [b]) => a - b)) {
        for (const [row, node] of entries.sort(compareNodes).entries()) {
            const id = nodes.length;
            const x = level * HIERARCHY_COLUMN_WIDTH;
            const y = ((entries.length - 1) / 2 - row) * HIERARCHY_ROW_HEIGHT;
            const size = Number.isFinite(node.size) ? node.size * HIERARCHY_SIZE_SCALE : HIERARCHY_MIN_SIZE;
            nodes.push({ ...node, id, x, y, z: 0, size: Math.max(HIERARCHY_MIN_SIZE, Math.min(HIERARCHY_MAX_SIZE, size)) });
            renderId.set(node.id, id);
            placements.push({ id, key: node.qualified_name ?? `node:${node.id}`, name: node.name, hop: level, x, y });
        }
    }
    const lanes = new Map<string, number>();
    const projectedEdges = edges.map(edge => ({ ...edge, source: renderId.get(edge.source)!, target: renderId.get(edge.target)! }))
        .sort((a, b) => a.source - b.source || a.target - b.target || compareText(a.type, b.type) || (a.line ?? 0) - (b.line ?? 0))
        .map(edge => {
            const pair = [edge.source, edge.target].sort((a, b) => a - b).join(':');
            const lane = lanes.get(pair) ?? 0;
            lanes.set(pair, lane + 1);
            return lane === 0 ? edge : { ...edge, offset: lane * HIERARCHY_LANE_SPACING };
        });
    const missing = roots.length + context.length - nodes.length;
    const scope = range ? `selected code in ${filePath.split('/').pop()}` : filePath.split('/').pop() ?? filePath;
    const bounds = layout.total_nodes > layout.nodes.length
        ? ` · ${layout.nodes.length} of ${layout.total_nodes} indexed nodes loaded` : '';
    const omitted = missing > 0 ? ` · ${missing} related nodes omitted at the ${limit}-node limit` : '';
    return {
        data: { nodes, edges: projectedEdges, total_nodes: roots.length + context.length },
        rootId: -1, rootKey: filePath, rootName: scope, symbols: nodes.length,
        depth: columns.size, truncated: missing > 0 || layout.total_nodes > layout.nodes.length,
        cap: limit, walkDepth: 1, missing, placements,
        headline: `File relationships · ${scope} · ${nodes.length} nodes`,
        edgeNote: `${projectedEdges.length} recorded relationships from the loaded graph${bounds}${omitted}`,
        message: focus.message,
    };
}
