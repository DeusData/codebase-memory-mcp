import type { RouteRef } from '../core/intelligence-provider';
import type { GraphData, GraphEdge, GraphNode } from '../galaxy/types';
import { areaLevels, areaOf, areaTrail, MAP_RELATIONS } from './repository-map';
import type { RouteGraphSnapshot } from './route-graph-source';

export type SemanticView = 'overview' | 'dependencies' | 'entryPoints' | 'routes' | 'hotspots';
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
    /** Optional scene treatment for service declarations; source maps keep their defaults. */
    footprint?: [number, number];
    tint?: string;
    kindLabel?: string;
    /** An area outside the opened area or file; it keeps to its own lane. */
    external?: boolean;
    /** The first path segment shared by the routes of a route group. */
    routePrefix?: string;
    /** What the chip shows when the full label would read like a neighbour's; the tooltip keeps the full label. */
    shortLabel?: string;
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
    /** The line colour a map chose for this relationship; the hue of its type otherwise. */
    tint?: string;
}
/** Visual folder context only; these containers are never graph nodes. */
export interface SemanticPlatform {
    id: string;
    path: string;
    label: string;
    /** Y denotes the top surface of the shallow platform. */
    position: [number, number, number];
    width: number;
    depth: number;
    level: number;
}
export interface SemanticGraph {
    view: SemanticView;
    scopeKey: string;
    title: string;
    positionMeaning: string;
    nodes: SemanticNode[];
    platforms?: SemanticPlatform[];
    edges: SemanticEdge[];
    totalNodes: number;
    totalEdges: number;
    omittedNodes: number;
    omittedEdges: number;
    warnings: string[];
    /** Routes view only: routes left out because all of their evidence lies in test code. */
    hiddenRoutes?: number;
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
    /** File inventory is independent of the dependency graph and its row cap. */
    knownFiles?: string[];
    visibleFiles?: ReadonlySet<string>;
    /** Routes view: fold routes sharing a first path segment into one counted group while no filter is set. */
    groupRoutes?: boolean;
    /** Routes view: leave out routes whose registration, handlers and callers all lie in test code. */
    hideTestRoutes?: boolean;
}

const compare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
const identity = (node: GraphNode) => `${node.label}:${node.qualified_name || `#${node.id}`}@${node.file_path ?? ''}`;
const nodeOrder = (a: GraphNode, b: GraphNode) => compare(identity(a), identity(b));
const symbolId = (node: GraphNode) => `symbol:${identity(node)}`;
/**
 * Conventional test locations: a tests, __tests__ or spec directory, a test
 * root (test/ or src/test/), or a test_*, tests.*, *_test, *.test or *.spec
 * file. A nested test/ package such as django/test/ is product code.
 */
const TEST_SOURCE = /(^|\/)(tests|__tests__|specs?)\/|^(src\/)?test\/|(^|\/)(test_[^/]*|tests?\.[^/.]+|[^/]+[._-](test|spec)\.[^/.]+)$/i;
/** The first path segment of a route label, without its method: '/accounts' for 'GET /accounts/login/'. */
/** Route paths as people read them: '/%C3%A9dit' reads '/édit'. Reserved characters stay encoded
 * (decodeURI), and a path that is not valid percent-encoding stays as indexed. */
export const readableRoute = (path: string): string => { try { return decodeURI(path); } catch { return path; } };
const routePrefixOf = (label: string) => `/${label.replace(/^[A-Z]+\s+/, '').split('/').filter(Boolean)[0] ?? ''}`;
/** Characters a compact route chip shows before it cuts the label (spatial-architecture.css, 112px at 10px). */
const COMPACT_LABEL_CHARS = 14;

/**
 * Labels that a compact chip would cut to the same visible start ("/generic-lastmo…")
 * lose their shared start instead: "…/index.xml" and "…/sitemap.xml". The cut falls on a
 * separator, so the rest begins with a whole segment or word; a slash stays to mark a path.
 * A shared start without a separator inside ("/sitemapindex1.xml") keeps the end that
 * fits the chip, and that end holds the difference: "…mapindex1.xml".
 */
export function distinctLabels(labels: readonly string[], visible = COMPACT_LABEL_CHARS): Map<string, string> {
    const result = new Map<string, string>();
    const long = [...new Set(labels)].filter(label => label.length > visible);
    for (const label of long) {
        let shared = 0;
        for (const other of long) {
            if (other === label || other.slice(0, visible - 1) !== label.slice(0, visible - 1)) continue;
            let common = 0;
            while (common < label.length && label[common] === other[common]) common++;
            shared = Math.max(shared, common);
        }
        if (!shared) continue;
        const cut = Math.max(...['/', '-', '_', '.'].map(separator => label.lastIndexOf(separator, shared - 1)));
        const rest = cut > 0 ? (label[cut] === '/' ? label.slice(cut) : label.slice(cut + 1))
            : label.slice(Math.min(shared, Math.max(1, label.length - (visible - 1))));
        // A label that is the shared start itself keeps its full text; its neighbours carry the difference.
        if (rest.replace('/', '').length > 1) result.set(label, `…${rest}`);
    }
    return result;
}
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

interface HierarchyBox {
    key: string;
    path: string;
    level: number;
    unknown?: boolean;
    node?: SemanticNode;
    children: HierarchyBox[];
    width: number;
    depth: number;
    x: number;
    z: number;
}
const hierarchyPath = (path: string | undefined) => !path || path === '{}' ? undefined
    : path.replaceAll('\\', '/').replace(/^\.\//, '').replace(/\/+/g, '/').replace(/\/$/, '');
const parentPath = (path: string) => path.includes('/') ? path.slice(0, path.lastIndexOf('/')) : '';
const hierarchyNodeOrder = (a: SemanticNode, b: SemanticNode) => compare(hierarchyPath(a.filePath) ?? a.areaPath ?? '', hierarchyPath(b.filePath) ?? b.areaPath ?? '')
    || (a.line ?? 0) - (b.line ?? 0) || compare(a.id, b.id);

/** Deterministic shelf packing; its inputs are source identity and hierarchy only. */
function packHierarchy(box: HierarchyBox): void {
    if (box.node) return;
    const gap = 10;
    const padding = box.level ? 12 : 0;
    const labelEdge = box.level ? 16 : 0;
    for (const child of box.children) packHierarchy(child);
    const target = Math.max(24, ...box.children.map(child => child.width),
        Math.sqrt(box.children.reduce((sum, child) => sum + (child.width + gap) * (child.depth + gap), 0)) * 1.2);
    let x = 0; let z = 0; let rowDepth = 0; let width = 0;
    for (const child of box.children) {
        if (x > 0 && x + child.width > target) { x = 0; z += rowDepth + gap; rowDepth = 0; }
        child.x = x + child.width / 2;
        child.z = z + child.depth / 2;
        width = Math.max(width, x + child.width);
        rowDepth = Math.max(rowDepth, child.depth);
        x += child.width + gap;
    }
    box.width = width + padding * 2;
    box.depth = z + rowDepth + padding + labelEdge;
    for (const child of box.children) {
        child.x += padding - box.width / 2;
        child.z += labelEdge - box.depth / 2;
    }
}

/**
 * Arrange candidates by actual source folders, never relation count or hotspot
 * rank. Only positions change; callers retain all graph identities. Deep paths
 * share their third real ancestor to keep the steps shallow; an opened area
 * passes itself as `base` and counts as the first of those steps.
 */
export function layoutFolderHierarchy(nodes: SemanticNode[], base = ''): SemanticPlatform[] {
    if (!nodes.length) return [];
    const root: HierarchyBox = { key: '', path: '', level: 0, children: [], width: 0, depth: 0, x: 0, z: 0 };
    const folders = new Map<string, HierarchyBox>();
    const areaPaths = nodes.filter(node => node.kind === 'area').map(node => hierarchyPath(node.areaPath)).filter((path): path is string => Boolean(path && path !== '(root)'));
    const areasWithDescendants = new Set<string>();
    for (const path of areaPaths) {
        let ancestor = parentPath(path);
        while (ancestor) { areasWithDescendants.add(ancestor); ancestor = parentPath(ancestor); }
    }
    for (const node of [...nodes].sort(hierarchyNodeOrder)) {
        const path = hierarchyPath(node.kind === 'area' ? node.areaPath : node.filePath);
        const containing = path === '(root)' ? '' : path === undefined ? undefined
            : node.kind === 'area' && areasWithDescendants.has(path) ? path : parentPath(path);
        let owner = root;
        if (containing === undefined) {
            let unknown = folders.get('unknown');
            if (!unknown) {
                unknown = { key: 'unknown', path: '', unknown: true, level: 1, children: [], width: 0, depth: 0, x: 0, z: 0 };
                root.children.push(unknown); folders.set('unknown', unknown);
            }
            owner = unknown;
        } else {
            const parts = (base && (containing === base || containing.startsWith(`${base}/`))
                ? [base, ...containing.slice(base.length + 1).split('/')] : containing.split('/')).filter(Boolean).slice(0, 3);
            parts.forEach((_, index) => {
                const folderPath = parts.slice(0, index + 1).join('/');
                const key = `folder:${folderPath}`;
                let folder = folders.get(key);
                if (!folder) {
                    folder = { key, path: folderPath, level: index + 1, children: [], width: 0, depth: 0, x: 0, z: 0 };
                    owner.children.push(folder); folders.set(key, folder);
                }
                owner = folder;
            });
        }
        owner.children.push({ key: `node:${node.id}`, path: path ?? '', level: owner.level, node, children: [], width: 24, depth: 24, x: 0, z: 0 });
    }
    const sortChildren = (box: HierarchyBox) => {
        box.children.sort((a, b) => a.node && b.node ? hierarchyNodeOrder(a.node, b.node)
            : Number(Boolean(a.node)) - Number(Boolean(b.node)) || Number(Boolean(a.unknown)) - Number(Boolean(b.unknown)) || compare(a.key, b.key));
        box.children.filter(child => !child.node).forEach(sortChildren);
    };
    sortChildren(root);
    packHierarchy(root);
    const platforms: SemanticPlatform[] = [];
    const place = (box: HierarchyBox, x: number, z: number) => {
        if (box.node) { box.node.position = [x, box.level * 1.6 + 0.08, z]; return; }
        if (box.level) platforms.push({ id: `hierarchy:${box.key}`, path: box.path,
            label: box.unknown ? 'Unknown source' : box.path, position: [x, box.level * 1.6, z], width: box.width, depth: box.depth, level: box.level });
        for (const child of box.children) place(child, x + child.x, z + child.z);
    };
    place(root, 0, 0);
    return platforms;
}

/**
 * Areas outside the opened area or file wait in a lane in front of its
 * platforms, so the platforms hold only what lies inside.
 */
function placeOutside(outside: SemanticNode[], inside: SemanticNode[], platforms: SemanticPlatform[]): SemanticPlatform | undefined {
    if (!outside.length) return undefined;
    const spacing = 34;
    const extents = [...platforms.map(platform => [platform.position[0] - platform.width / 2, platform.position[0] + platform.width / 2, platform.position[2] + platform.depth / 2]),
        ...inside.map(node => [node.position[0] - 12, node.position[0] + 12, node.position[2] + 12])];
    const left = extents.length ? Math.min(...extents.map(extent => extent[0])) : 0;
    const right = extents.length ? Math.max(...extents.map(extent => extent[1])) : 0;
    const top = (extents.length ? Math.max(...extents.map(extent => extent[2])) : 0) + 14;
    const columns = Math.max(1, Math.min(outside.length, Math.max(Math.floor((right - left + 10) / spacing), Math.ceil(Math.sqrt(outside.length)))));
    const rows = Math.ceil(outside.length / columns);
    // One extra column on the left keeps the lane's own label clear of the first brick label.
    const width = (columns + 1) * spacing + 14; const depth = rows * spacing + 18; const center = (left + right) / 2;
    outside.forEach((node, index) => {
        node.position = [center + (index % columns + 1 - columns / 2) * spacing, 1.68, top + 28 + Math.floor(index / columns) * spacing];
    });
    return { id: 'hierarchy:outside', path: '', label: 'Outside', position: [center, 1.6, top + depth / 2], width, depth, level: 1 };
}

/** Prune empty containers without repacking the stable unfiltered layout. */
export function pruneHierarchyPlatforms(platforms: SemanticPlatform[], nodes: SemanticNode[]): SemanticPlatform[] {
    return platforms.filter(platform => nodes.some(node => node.position[1] >= platform.position[1]
        && Math.abs(node.position[0] - platform.position[0]) < platform.width / 2
        && Math.abs(node.position[2] - platform.position[2]) < platform.depth / 2));
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
    let positionMeaning = 'Nested platforms follow source folders; areas outside the opened one wait in their own lane. Boxes represent source grouping, not deployed services.';
    const depth = bounded(options.depth, 3, 8);
    let rootId: string | undefined;
    let hiddenRoutes = 0;

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
        const recorded = evidence.filter(edge => ['HTTP_CALLS', 'ASYNC_CALLS', 'HANDLES'].includes(edge.type));
        for (const relationship of options.routeSnapshot?.relationships ?? []) {
            const remap = (node: GraphNode) => node.qualified_name ? currentByIdentity.get(identity(node)) ?? node : node;
            recorded.push({ ...relationship, source: remap(relationship.source), target: remap(relationship.target) });
        }
        // A route is test code only when every located piece of its evidence is:
        // its own registration, its handlers and its callers.
        const verdicts = new Map<string, boolean[]>();
        const record = (route: GraphNode, node: GraphNode) => {
            const file = node.file_path && !['{}', '-'].includes(node.file_path) ? node.file_path : undefined;
            if (!file && node.status !== 'test') return;
            const list = verdicts.get(identity(route)) ?? []; list.push(node.status === 'test' || TEST_SOURCE.test(file!)); verdicts.set(identity(route), list);
        };
        for (const node of graph.nodes) if (node.label === 'Route') record(node, node);
        for (const edge of recorded) {
            const route = edge.target.label === 'Route' ? edge.target : edge.source.label === 'Route' ? edge.source : undefined;
            if (route) { record(route, route); record(route, route === edge.target ? edge.source : edge.target); }
        }
        const hidden = new Set<string>();
        const shown = (route: GraphNode) => {
            const list = verdicts.get(identity(route));
            if (!options.hideTestRoutes || !list?.length || !list.every(Boolean)) return true;
            hidden.add(identity(route)); return false;
        };
        const routeEvidence = recorded.filter(edge => [edge.source, edge.target].every(node => node.label !== 'Route' || shown(node)));
        const routeNodes = new Map<string, SemanticNode>();
        const currentNodes = new Set(graph.nodes);
        const routeKey = (node: GraphNode, edge?: SemanticEvidence): string => {
            if (node.label === 'Route') {
                const id = `route:${identity(node)}`;
                if (!routeNodes.has(id)) routeNodes.set(id, { ...symbolNode(node), id, kind: 'route',
                    graphNode: currentNodes.has(node) ? node : undefined,
                    label: readableRoute(edge?.routePath ?? node.name), detail: node.file_path ?? 'Indexed route; source location unavailable' });
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
        for (const node of graph.nodes.filter(node => node.label === 'Route' && shown(node))) routeKey(node);
        for (const route of options.routes ?? []) {
            const alreadyRepresented = graph.nodes.some(node => node.label === 'Route'
                && [route.path, `${route.method ?? ''} ${route.path}`.trim()].includes(node.name)
                && (!route.filePath || node.file_path === route.filePath)
                && (route.origin === 'index' || (route.filePath && route.line && node.start_line === route.line)));
            if (alreadyRepresented) continue;
            // A textual registration is useful navigation, but cannot establish an edge or handler identity.
            const id = `registration:${route.method ?? ''}:${route.path}@${route.filePath ?? ''}:${route.line ?? ''}`;
            if (options.hideTestRoutes && route.filePath && TEST_SOURCE.test(route.filePath)) { hidden.add(id); continue; }
            routeNodes.set(id, { id, kind: 'route', label: `${route.method ? `${route.method} ` : ''}${readableRoute(route.path)}`,
                detail: `${route.origin === 'source' ? 'Source' : 'Index'} registration · handler relationship not resolved`,
                position: [0, 0, 0], count: 1, filePath: route.filePath, line: route.line, members: [] });
        }
        hiddenRoutes = hidden.size;
        // Without a filter, routes sharing a first path segment become one counted
        // group; a filter lists the matching routes one by one again. The root
        // path '/' shares no segment, so its routes stay single.
        const grouped = new Map<string, string>();
        if (options.groupRoutes && !options.filter?.trim()) {
            const prefixes = new Map<string, SemanticNode[]>();
            for (const node of routeNodes.values()) if (node.kind === 'route') {
                const prefix = routePrefixOf(node.label);
                prefixes.set(prefix, [...prefixes.get(prefix) ?? [], node]);
            }
            for (const [prefix, routes] of prefixes) if (routes.length > 1 && prefix !== '/') {
                const id = `route-group:${prefix}`;
                routeNodes.set(id, { id, kind: 'route', kindLabel: 'Route group', routePrefix: prefix, label: `${prefix} · ${routes.length}`,
                    detail: `${routes.length} routes whose path starts with ${prefix}`, position: [0, 0, 0], count: routes.length,
                    members: routes.flatMap(route => route.members) });
                for (const route of routes) { grouped.set(route.id, id); routeNodes.delete(route.id); }
            }
        }
        if (grouped.size) {
            const merged = new Map<string, SemanticEdge>();
            for (const edge of edges) {
                const source = grouped.get(edge.source) ?? edge.source, target = grouped.get(edge.target) ?? edge.target;
                const id = `${source}→${edge.type}→${target}`;
                const into = merged.get(id) ?? { ...edge, id, source, target, count: 0, evidence: [] };
                into.count += edge.count; into.evidence.push(...edge.evidence); merged.set(id, into);
            }
            edges = [...merged.values()].sort((a, b) => compare(a.id, b.id));
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
        const trails = new Map<string, string[]>();
        const trailOf = (path: string) => { let trail = trails.get(path); if (!trail) trails.set(path, trail = areaTrail(path)); return trail; };
        // An opened area shows its next level: deeper areas, and the files that sit in it directly.
        const scope = options.filePath ? undefined : options.areaPath;
        const levelOf = (path: string): { kind: 'area' | 'file'; path: string } | undefined => {
            const trail = trailOf(path);
            if (!scope) return { kind: 'area', path: trail[0] };
            const at = trail.indexOf(scope);
            return at < 0 ? undefined : at + 1 < trail.length ? { kind: 'area', path: trail[at + 1] } : { kind: 'file', path };
        };
        const scopedLabel = (path: string) => scope && path.startsWith(`${scope}/`) ? path.slice(scope.length + 1) : path;
        for (const node of graph.nodes.filter(sourceNode).sort(nodeOrder)) {
            if (options.visibleFiles && !options.visibleFiles.has(node.file_path!)) continue;
            if (options.filePath) {
                if (node.file_path !== options.filePath || ['File', 'Module'].includes(node.label)) continue;
                const result = symbolNode(node); groups.set(result.id, result); nodeGroups.set(node.id, result.id);
                continue;
            }
            const level = levelOf(node.file_path!);
            if (!level) continue;
            const id = `${level.kind}:${level.path}`;
            const group = groups.get(id) ?? { id, kind: level.kind, label: scopedLabel(level.path), detail: '', position: [0, 0, 0], count: 0,
                filePath: level.kind === 'file' ? level.path : undefined, areaPath: level.kind === 'area' ? level.path : scope, members: [] };
            group.members.push(node); group.count++; groups.set(id, group); nodeGroups.set(node.id, id);
        }
        // Retain files with no graph nodes/edges as navigable inventory objects.
        // They deliberately have no graphNode and cannot fabricate impact evidence.
        const inventory = new Map<string, Set<string>>();
        for (const path of options.knownFiles ?? []) {
            if (!path || path === '{}' || (options.visibleFiles && !options.visibleFiles.has(path))) continue;
            if (options.filePath && path !== options.filePath) continue;
            const level = options.filePath ? { kind: 'file' as const, path } : levelOf(path);
            if (!level) continue;
            const id = `${level.kind}:${level.path}`;
            const paths = inventory.get(id) ?? new Set<string>(); paths.add(path); inventory.set(id, paths);
            if (options.filePath && [...groups.values()].some(group => group.filePath === path)) continue;
            if (!groups.has(id)) groups.set(id, { id, kind: level.kind, label: options.filePath ? path : scopedLabel(level.path),
                detail: options.filePath ? 'File inventory · no symbols in the loaded graph' : '',
                position: [0, 0, 0], count: 0, areaPath: level.kind === 'area' ? level.path : scope ?? areaOf(path),
                filePath: level.kind === 'file' ? path : undefined, members: [] });
        }
        let scopedEvidence = evidence;
        if (options.areaPath || options.filePath) {
            const inScope = (node: GraphNode) => sourceNode(node)
                && (!options.visibleFiles || options.visibleFiles.has(node.file_path!))
                && (options.filePath ? node.file_path === options.filePath : levelOf(node.file_path!) !== undefined);
            // An outside area is named where its trail leaves the opened one: a sibling, not a repository root area.
            const reference = scope ? areaLevels(scope) : trailOf(options.filePath!);
            const outsideArea = (path: string) => {
                const trail = trailOf(path);
                let shared = 0;
                while (shared < trail.length && trail[shared] === reference[shared]) shared++;
                return trail[Math.min(shared, trail.length - 1)];
            };
            scopedEvidence = evidence.filter(edge => inScope(edge.source) || inScope(edge.target));
            for (const edge of scopedEvidence) {
                if (inScope(edge.source) === inScope(edge.target)) continue;
                const inside = inScope(edge.source) ? edge.source : edge.target;
                const outside = inside === edge.source ? edge.target : edge.source;
                if (!sourceNode(outside)) continue;
                if (options.visibleFiles && !options.visibleFiles.has(outside.file_path!)) continue;
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
                const areaPath = outsideArea(outside.file_path!);
                const id = `area:${areaPath}`;
                const group: SemanticNode = groups.get(id) ?? { id, kind: 'area', label: areaPath,
                    detail: 'External source area · inferred from file paths', position: [0, 0, 0],
                    count: 0, areaPath, members: [], external: true };
                if (!group.members.includes(outside)) { group.members.push(outside); group.count++; }
                groups.set(id, group); nodeGroups.set(outside.id, id);
            }
        }
        nodes = [...groups.values()].map(node => ({ ...node, members: node.members.sort(nodeOrder),
            detail: node.detail || `${new Set([...node.members.map(member => member.file_path), ...(inventory.get(node.id) ?? [])]).size} files · ${node.count} indexed nodes` }));
        edges = aggregate(scopedEvidence, node => nodeGroups.get(node.id));
        if (options.view === 'dependencies') title = options.filePath ?? options.areaPath ?? 'Dependencies between source areas';
    }

    const filter = options.filter?.trim().toLowerCase();
    const matching = new Set<string>();
    const capped = (candidates: SemanticNode[], relations: SemanticEdge[]) => {
        const degrees = new Map<string, number>();
        for (const edge of relations) for (const id of [edge.source, edge.target]) degrees.set(id, (degrees.get(id) ?? 0) + edge.count);
        return [...candidates].sort((a, b) => (a.id === rootId ? -1 : b.id === rootId ? 1 : 0)
            || Number(matching.has(b.id)) - Number(matching.has(a.id))
            || (a.depth ?? 0) - (b.depth ?? 0) || (degrees.get(b.id) ?? 0) - (degrees.get(a.id) ?? 0) || compare(a.id, b.id))
            .slice(0, bounded(options.maxNodes, 40, 80)).sort((a, b) => compare(a.id, b.id));
    };
    const unfiltered = capped(nodes, edges);
    if (filter) {
        // A filter that names a route group's prefix selects exactly that group's
        // routes, by the rule that built the group; any other text is a search.
        const prefixed = (node: SemanticNode) => node.kind === 'route' && routePrefixOf(node.label).toLowerCase() === filter;
        const routeGroup = options.view === 'routes' && options.groupRoutes && /^\/[^/\s]+$/.test(filter) && nodes.some(prefixed);
        for (const node of nodes) {
            if (routeGroup ? prefixed(node)
                : `${node.label} ${node.filePath ?? node.areaPath ?? ''} ${node.detail} ${node.members.map(member => member.name).join(' ')}`.toLowerCase().includes(filter)) matching.add(node.id);
        }
        // Neighbours add relationship context; for a route group they never add routes.
        const routeIds = new Set(routeGroup ? nodes.filter(node => node.kind === 'route').map(node => node.id) : []);
        const context = new Set(matching);
        for (const edge of edges) {
            if (matching.has(edge.source) || matching.has(edge.target)) for (const id of [edge.source, edge.target]) if (!routeIds.has(id)) context.add(id);
        }
        nodes = nodes.filter(node => context.has(node.id));
        edges = edges.filter(edge => context.has(edge.source) && context.has(edge.target));
        if (context.size > matching.size) warnings.push('Showing filter matches and their one-hop neighbors for relationship context.');
    }
    const totalNodes = nodes.length; const totalEdges = edges.length;
    nodes = filter ? capped(nodes, edges) : unfiltered;
    // Cap first, then lay out, so the platforms are sized to what is drawn. A
    // filter that only narrows the drawn map keeps its positions.
    let platforms: SemanticPlatform[] | undefined;
    if (options.view !== 'entryPoints' && options.view !== 'routes') {
        const layout = nodes.every(node => unfiltered.includes(node)) ? unfiltered : nodes;
        const inside = layout.filter(node => !node.external);
        platforms = layoutFolderHierarchy(inside, options.filePath ? '' : options.areaPath ?? '');
        const lane = placeOutside(layout.filter(node => node.external), inside, platforms);
        if (lane) platforms.push(lane);
    }
    const visibleIds = new Set(nodes.map(node => node.id));
    edges = edges.filter(edge => visibleIds.has(edge.source) && visibleIds.has(edge.target))
        .sort((a, b) => Number(matching.has(b.source) || matching.has(b.target)) - Number(matching.has(a.source) || matching.has(a.target))
            || b.count - a.count || compare(a.id, b.id)).slice(0, bounded(options.maxEdges, 100, 200));
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
    nodes.forEach(node => {
        if (options.view === 'entryPoints' || options.view === 'routes') {
            const lane = options.view === 'entryPoints' ? node.depth ?? 0
                : node.kind === 'route' ? 0 : node.id.startsWith('handler') ? 1 : -1;
            const siblings = lanes.get(lane)!;
            const laneColumns = Math.ceil(siblings.length / 5);
            const laneRows = Math.ceil(siblings.length / laneColumns);
            const siblingIndex = siblings.indexOf(node);
            node.position = [lane * laneSpacing + (Math.floor(siblingIndex / laneRows) - (laneColumns - 1) / 2) * 20,
                options.view === 'entryPoints' ? lane * 3 : 0, (siblingIndex % laneRows - (laneRows - 1) / 2) * 20];
        }
    });
    if (options.view === 'routes') {
        const short = distinctLabels(nodes.filter(node => node.kind === 'route').map(node => node.label));
        for (const node of nodes) if (node.kind === 'route' && short.has(node.label)) node.shortLabel = short.get(node.label);
    }
    if (unresolved) warnings.push(`${unresolved} relationships have an endpoint outside the loaded repository snapshot.`);
    if (graph.total_nodes > graph.nodes.length) warnings.push('The loaded repository snapshot contains only part of the indexed graph.');
    return { view: options.view, scopeKey: `${options.view}:${options.areaPath ?? ''}:${options.filePath ?? ''}:${rootId ?? ''}:${filter ?? ''}`,
        title, positionMeaning, nodes, ...(platforms ? { platforms: pruneHierarchyPlatforms(platforms, nodes) } : {}),
        edges, totalNodes, totalEdges, omittedNodes: totalNodes - nodes.length,
        omittedEdges: totalEdges - edges.length, warnings: [...new Set(warnings)], ...(options.view === 'routes' ? { hiddenRoutes } : {}) };
}
