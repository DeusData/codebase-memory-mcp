import type { RouteRef } from '../core/intelligence-provider';
import type { GraphData, GraphEdge, GraphNode } from '../galaxy/types';
import { areaOf, MAP_RELATIONS } from './repository-map';
import type { RouteGraphSnapshot } from './route-graph-source';

export type SemanticView = 'overview' | 'dependencies' | 'entryPoints' | 'routes';
export interface SemanticNode {
    id: string;
    kind: 'area' | 'file' | 'symbol' | 'route';
    label: string;
    detail: string;
    position: [number, number, number];
    count: number;
    /** Present only for a real node from the current repository snapshot. */
    graphNode?: GraphNode;
    filePath?: string;
    line?: number;
    areaPath?: string;
    members: GraphNode[];
    depth?: number;
}
export interface SemanticEvidence {
    source: GraphNode;
    target: GraphNode;
    type: string;
    id?: number;
    line?: number;
    strategy?: string;
    confidence?: number;
    routePath?: string;
    via?: string;
}
export interface SemanticEdge {
    id: string;
    source: string;
    target: string;
    type: string;
    count: number;
    evidence: SemanticEvidence[];
}
export interface SemanticGraph {
    view: SemanticView;
    scopeKey: string;
    title: string;
    positionMeaning: string;
    nodes: SemanticNode[];
    edges: SemanticEdge[];
    totalNodes: number;
    totalEdges: number;
    omittedNodes: number;
    omittedEdges: number;
    warnings: string[];
}
export interface SemanticGraphOptions {
    view: SemanticView;
    areaPath?: string;
    filePath?: string;
    entryId?: number;
    entryQualifiedNames?: string[];
    depth?: number;
    filter?: string;
    relations?: string[];
    routes?: RouteRef[];
    routeSnapshot?: RouteGraphSnapshot;
    maxNodes?: number;
    maxEdges?: number;
}

const compare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
const identity = (node: GraphNode) => `${node.label}:${node.qualified_name || `#${node.id}`}@${node.file_path ?? ''}`;
const nodeOrder = (a: GraphNode, b: GraphNode) => compare(identity(a), identity(b));
const symbolId = (node: GraphNode) => `symbol:${identity(node)}`;
const sourceNode = (node: GraphNode) => Boolean(node.file_path && node.file_path !== '{}'
    && !['Project', 'Folder', 'Package', 'Branch', 'Route'].includes(node.label));
const symbolNode = (node: GraphNode): SemanticNode => ({ id: symbolId(node),
    kind: node.label === 'Route' ? 'route' : 'symbol', label: node.name,
    detail: node.file_path ?? node.qualified_name ?? node.label, position: [0, 0, 0],
    count: 1, graphNode: node, filePath: node.file_path, line: node.start_line,
    areaPath: node.file_path ? areaOf(node.file_path) : undefined, members: [node] });

/** Use explicit index classifications or exact qualified identities, never a name guess. */
export function semanticEntryPoints(graph: GraphData, qualifiedNames: string[] = []): GraphNode[] {
    const names = new Set(qualifiedNames);
    return graph.nodes.filter(node => ['Function', 'Method'].includes(node.label)
        && node.status !== 'test' && (node.status === 'entry' || Boolean(node.qualified_name && names.has(node.qualified_name))))
        .sort((a, b) => (b.out_calls ?? 0) - (a.out_calls ?? 0) || nodeOrder(a, b));
}

function evidenceFor(edge: GraphEdge, source: GraphNode, target: GraphNode): SemanticEvidence {
    const verifiedLine = edge.line !== undefined && source.start_line !== undefined && source.end_line !== undefined
        && edge.line >= source.start_line && edge.line <= source.end_line ? edge.line : undefined;
    return { source, target, type: edge.type, id: edge.id, line: verifiedLine,
        strategy: edge.strategy, confidence: edge.confidence };
}

function aggregate(evidence: SemanticEvidence[], mapNode: (node: GraphNode, edge: SemanticEvidence, side: 'source' | 'target') => string | undefined): SemanticEdge[] {
    const edges = new Map<string, SemanticEdge>();
    const seen = new Set<string>();
    for (const finding of evidence) {
        const evidenceId = `${identity(finding.source)}:${finding.type}:${identity(finding.target)}:${finding.id ?? ''}:${finding.line ?? ''}`;
        if (seen.has(evidenceId)) continue;
        seen.add(evidenceId);
        const source = mapNode(finding.source, finding, 'source');
        const target = mapNode(finding.target, finding, 'target');
        // Area/file internal edges appear on drill-down; real recursive calls remain evidence.
        if (!source || !target || (source === target && !source.startsWith('symbol:'))) continue;
        const id = `${source}→${finding.type}→${target}`;
        const edge = edges.get(id) ?? { id, source, target, type: finding.type, count: 0, evidence: [] };
        edge.evidence.push(finding); edge.count++; edges.set(id, edge);
    }
    return [...edges.values()].map(edge => ({ ...edge, evidence: edge.evidence.sort((a, b) =>
        compare(`${identity(a.source)}:${identity(a.target)}:${a.id ?? ''}`, `${identity(b.source)}:${identity(b.target)}:${b.id ?? ''}`)) }))
        .sort((a, b) => compare(a.id, b.id));
}

function bounded(value: number | undefined, fallback: number, maximum: number): number {
    return Number.isFinite(value) ? Math.max(1, Math.min(maximum, Math.floor(value!))) : fallback;
}

/** Stable presentation projection. Scene limits are independent of backend snapshot limits. */
export function buildSemanticGraph(graph: GraphData, options: SemanticGraphOptions): SemanticGraph {
    const byId = new Map(graph.nodes.map(node => [node.id, node]));
    const allowed = new Set(options.relations?.length ? options.relations : MAP_RELATIONS);
    const evidence: SemanticEvidence[] = [];
    let unresolved = 0;
    for (const edge of graph.edges) {
        if (!allowed.has(edge.type as typeof MAP_RELATIONS[number]) && options.view !== 'entryPoints' && options.view !== 'routes') continue;
        const source = byId.get(edge.source); const target = byId.get(edge.target);
        if (!source || !target) { unresolved++; continue; }
        evidence.push(evidenceFor(edge, source, target));
    }
    let nodes: SemanticNode[] = [];
    let edges: SemanticEdge[] = [];
    const warnings: string[] = [];
    let title = options.filePath ?? options.areaPath ?? 'Repository structure';
    let positionMeaning = 'Source areas form a stable directory map. Boxes represent source grouping, not deployed services.';
    const depth = bounded(options.depth, 3, 8);
    let rootId: string | undefined;

    if (options.view === 'entryPoints') {
        const entries = semanticEntryPoints(graph, options.entryQualifiedNames);
        const requested = options.entryId === undefined ? undefined : byId.get(options.entryId);
        const root = requested && ['Function', 'Method'].includes(requested.label) && requested.status !== 'test'
            ? requested : entries[0];
        const depths = new Map<number, number>();
        if (root) {
            rootId = symbolId(root); depths.set(root.id, 0);
            const outgoing = new Map<number, number[]>();
            for (const edge of evidence.filter(edge => edge.type === 'CALLS')) {
                const list = outgoing.get(edge.source.id) ?? []; list.push(edge.target.id); outgoing.set(edge.source.id, list);
            }
            const queue = [root.id];
            for (let index = 0; index < queue.length; index++) {
                const current = queue[index]; const currentDepth = depths.get(current)!;
                if (currentDepth >= depth) continue;
                for (const target of (outgoing.get(current) ?? []).sort((a, b) => nodeOrder(byId.get(a)!, byId.get(b)!))) {
                    if (!depths.has(target)) { depths.set(target, currentDepth + 1); queue.push(target); }
                    if (depths.size >= 1000) break;
                }
                if (depths.size >= 1000) { warnings.push('Reachability analysis stopped at 1,000 symbols.'); break; }
            }
            nodes = [...depths].map(([id, distance]) => ({ ...symbolNode(byId.get(id)!), depth: distance }));
            edges = aggregate(evidence.filter(edge => edge.type === 'CALLS' && depths.has(edge.source.id) && depths.has(edge.target.id)), node => symbolId(node));
            title = root.name;
            if ([...depths].some(([id, distance]) => distance === depth && (outgoing.get(id) ?? []).some(target => !depths.has(target)))) {
                warnings.push(`More calls continue beyond the ${depth}-hop boundary.`);
            }
        } else warnings.push('No indexed entry-point classification matches this repository snapshot.');
        positionMeaning = 'Left to right: shortest static call distance from the entry point. This is possible reachability, not runtime execution order.';
    } else if (options.view === 'routes') {
        const currentByIdentity = new Map(graph.nodes.map(node => [identity(node), node]));
        const routeEvidence = evidence.filter(edge => ['HTTP_CALLS', 'ASYNC_CALLS', 'HANDLES'].includes(edge.type));
        for (const relationship of options.routeSnapshot?.relationships ?? []) {
            const remap = (node: GraphNode) => node.qualified_name ? currentByIdentity.get(identity(node)) ?? node : node;
            routeEvidence.push({ ...relationship, source: remap(relationship.source), target: remap(relationship.target) });
        }
        const routeNodes = new Map<string, SemanticNode>();
        const currentNodes = new Set(graph.nodes);
        const routeKey = (node: GraphNode, edge?: SemanticEvidence): string => {
            if (node.label === 'Route') {
                const id = `route:${identity(node)}`;
                if (!routeNodes.has(id)) routeNodes.set(id, { ...symbolNode(node), id, kind: 'route',
                    graphNode: currentNodes.has(node) ? node : undefined,
                    label: edge?.routePath ?? node.name, detail: node.file_path ?? 'Indexed route; source location unavailable' });
                return id;
            }
            const area = node.file_path ? areaOf(node.file_path) : undefined;
            const role = edge?.type === 'HANDLES' ? 'handler' : 'caller';
            const id = area ? `${role}-area:${area}` : `${role}:${identity(node)}`;
            let group = routeNodes.get(id);
            if (!group) {
                group = { id, kind: area ? 'area' : 'symbol', label: area ?? node.name,
                    detail: `${role === 'handler' ? 'Handler' : 'Caller'} source area; inferred from file paths`,
                    position: [0, 0, 0], count: 0, areaPath: area, members: [] };
                routeNodes.set(id, group);
            }
            if (!group.members.some(member => identity(member) === identity(node))) { group.members.push(node); group.count++; }
            return id;
        };
        edges = aggregate(routeEvidence, (node, edge) => routeKey(node, edge));
        for (const node of graph.nodes.filter(node => node.label === 'Route')) routeKey(node);
        for (const route of options.routes ?? []) {
            const alreadyRepresented = graph.nodes.some(node => node.label === 'Route'
                && [route.path, `${route.method ?? ''} ${route.path}`.trim()].includes(node.name)
                && (!route.filePath || node.file_path === route.filePath)
                && (route.origin === 'index' || (route.filePath && route.line && node.start_line === route.line)));
            if (alreadyRepresented) continue;
            // A textual registration is useful navigation, but cannot establish an edge or handler identity.
            const id = `registration:${route.method ?? ''}:${route.path}@${route.filePath ?? ''}:${route.line ?? ''}`;
            routeNodes.set(id, { id, kind: 'route', label: `${route.method ? `${route.method} ` : ''}${route.path}`,
                detail: `${route.origin === 'source' ? 'Source' : 'Index'} registration · handler relationship not resolved`,
                position: [0, 0, 0], count: 1, filePath: route.filePath, line: route.line, members: [] });
        }
        nodes = [...routeNodes.values()];
        for (const node of nodes) node.members.sort(nodeOrder);
        warnings.push(...options.routeSnapshot?.warnings ?? []);
        if (options.routeSnapshot?.truncated) warnings.push('Route relationships are partial; missing lines do not prove that services are disconnected.');
        title = 'Requests across source areas';
        positionMeaning = 'Caller areas → route paths → handler areas. Only recorded HTTP, async and handler relationships become lines; isolated routes are registrations.';
    } else {
        const groups = new Map<string, SemanticNode>();
        const nodeGroups = new Map<number, string>();
        for (const node of graph.nodes.filter(sourceNode).sort(nodeOrder)) {
            const area = areaOf(node.file_path!);
            if (options.areaPath && area !== options.areaPath) continue;
            if (options.filePath && node.file_path !== options.filePath) continue;
            if (options.filePath) {
                if (['File', 'Module'].includes(node.label)) continue;
                const result = symbolNode(node); groups.set(result.id, result); nodeGroups.set(node.id, result.id);
                continue;
            }
            const kind = options.areaPath ? 'file' : 'area';
            const path = options.areaPath ? node.file_path! : area;
            const id = `${kind}:${path}`;
            const group = groups.get(id) ?? { id, kind, label: path, detail: '', position: [0, 0, 0], count: 0,
                filePath: kind === 'file' ? path : undefined, areaPath: area, members: [] };
            group.members.push(node); group.count++; groups.set(id, group); nodeGroups.set(node.id, id);
        }
        let scopedEvidence = evidence;
        if (options.areaPath || options.filePath) {
            const inScope = (node: GraphNode) => sourceNode(node)
                && (!options.areaPath || areaOf(node.file_path!) === options.areaPath)
                && (!options.filePath || node.file_path === options.filePath);
            scopedEvidence = evidence.filter(edge => inScope(edge.source) || inScope(edge.target));
            for (const edge of scopedEvidence) {
                if (inScope(edge.source) === inScope(edge.target)) continue;
                const inside = inScope(edge.source) ? edge.source : edge.target;
                const outside = inside === edge.source ? edge.target : edge.source;
                if (!sourceNode(outside)) continue;
                // File/module imports remain attributed to the real file, never
                // guessed onto an arbitrary symbol in the opened file.
                if (!nodeGroups.has(inside.id) && options.filePath && ['File', 'Module'].includes(inside.label)) {
                    const id = `file:${options.filePath}`;
                    const group: SemanticNode = groups.get(id) ?? { id, kind: 'file', label: options.filePath,
                        detail: 'File-level dependencies', position: [0, 0, 0], count: 0,
                        filePath: options.filePath, areaPath: areaOf(options.filePath), members: [] };
                    if (!group.members.includes(inside)) { group.members.push(inside); group.count++; }
                    groups.set(id, group); nodeGroups.set(inside.id, id);
                }
                if (!nodeGroups.has(inside.id)) continue;
                const areaPath = areaOf(outside.file_path!);
                const id = `area:${areaPath}`;
                const group: SemanticNode = groups.get(id) ?? { id, kind: 'area', label: areaPath,
                    detail: 'External source area · inferred from file paths', position: [0, 0, 0],
                    count: 0, areaPath, members: [] };
                if (!group.members.includes(outside)) { group.members.push(outside); group.count++; }
                groups.set(id, group); nodeGroups.set(outside.id, id);
            }
        }
        nodes = [...groups.values()].map(node => ({ ...node, members: node.members.sort(nodeOrder),
            detail: node.detail || `${new Set(node.members.map(member => member.file_path)).size} files · ${node.count} indexed nodes` }));
        edges = aggregate(scopedEvidence, node => nodeGroups.get(node.id));
        if (options.view === 'dependencies') title = options.filePath ?? options.areaPath ?? 'Dependencies between source areas';
    }

    const filter = options.filter?.trim().toLowerCase();
    const matching = new Set<string>();
    if (filter) {
        for (const node of nodes) {
            if (`${node.label} ${node.detail} ${node.members.map(member => member.name).join(' ')}`.toLowerCase().includes(filter)) matching.add(node.id);
        }
        const context = new Set(matching);
        for (const edge of edges) {
            if (matching.has(edge.source) || matching.has(edge.target)) { context.add(edge.source); context.add(edge.target); }
        }
        nodes = nodes.filter(node => context.has(node.id));
        edges = edges.filter(edge => context.has(edge.source) && context.has(edge.target));
        if (context.size > matching.size) warnings.push('Showing filter matches and their one-hop neighbors for relationship context.');
    }
    const totalNodes = nodes.length; const totalEdges = edges.length;
    const degrees = new Map<string, number>();
    for (const edge of edges) for (const id of [edge.source, edge.target]) degrees.set(id, (degrees.get(id) ?? 0) + edge.count);
    nodes.sort((a, b) => (a.id === rootId ? -1 : b.id === rootId ? 1 : 0)
        || Number(matching.has(b.id)) - Number(matching.has(a.id))
        || (a.depth ?? 0) - (b.depth ?? 0) || (degrees.get(b.id) ?? 0) - (degrees.get(a.id) ?? 0) || compare(a.id, b.id));
    nodes = nodes.slice(0, bounded(options.maxNodes, 40, 80)).sort((a, b) => compare(a.id, b.id));
    const visibleIds = new Set(nodes.map(node => node.id));
    edges = edges.filter(edge => visibleIds.has(edge.source) && visibleIds.has(edge.target))
        .sort((a, b) => Number(matching.has(b.source) || matching.has(b.target)) - Number(matching.has(a.source) || matching.has(a.target))
            || b.count - a.count || compare(a.id, b.id)).slice(0, bounded(options.maxEdges, 100, 200));
    const columns = Math.max(1, Math.ceil(Math.sqrt(nodes.length)));
    const rows = Math.ceil(nodes.length / columns);
    const lanes = new Map<number, SemanticNode[]>();
    for (const node of nodes) {
        const lane = options.view === 'entryPoints' ? node.depth ?? 0
            : node.kind === 'route' ? 0 : node.id.startsWith('handler') ? 1 : -1;
        const items = lanes.get(lane) ?? []; items.push(node); lanes.set(lane, items);
    }
    // Compact each semantic lane into at most five rows. Widen the distance
    // between depth/role bands so a large lane never crosses the next one.
    const widestBand = Math.max(1, ...[...lanes.values()].map(items => Math.ceil(items.length / 5)));
    const laneSpacing = Math.max(options.view === 'routes' ? 32 : 24, (widestBand - 1) * 20 + 24);
    nodes.forEach((node, index) => {
        if (options.view === 'entryPoints' || options.view === 'routes') {
            const lane = options.view === 'entryPoints' ? node.depth ?? 0
                : node.kind === 'route' ? 0 : node.id.startsWith('handler') ? 1 : -1;
            const siblings = lanes.get(lane)!;
            const laneColumns = Math.ceil(siblings.length / 5);
            const laneRows = Math.ceil(siblings.length / laneColumns);
            const siblingIndex = siblings.indexOf(node);
            node.position = [lane * laneSpacing + (Math.floor(siblingIndex / laneRows) - (laneColumns - 1) / 2) * 20,
                options.view === 'entryPoints' ? lane * 3 : 0, (siblingIndex % laneRows - (laneRows - 1) / 2) * 20];
        } else node.position = [(index % columns - (columns - 1) / 2) * 24,
            node.kind === 'symbol' ? 12 : node.kind === 'file' ? 6 : 0,
            (Math.floor(index / columns) - (rows - 1) / 2) * 24];
    });
    if (unresolved) warnings.push(`${unresolved} relationships have an endpoint outside the loaded repository snapshot.`);
    if (graph.total_nodes > graph.nodes.length) warnings.push('The loaded repository snapshot contains only part of the indexed graph.');
    return { view: options.view, scopeKey: `${options.view}:${options.areaPath ?? ''}:${options.filePath ?? ''}:${rootId ?? ''}:${filter ?? ''}`,
        title, positionMeaning, nodes, edges, totalNodes, totalEdges, omittedNodes: totalNodes - nodes.length,
        omittedEdges: totalEdges - edges.length, warnings: [...new Set(warnings)] };
}
