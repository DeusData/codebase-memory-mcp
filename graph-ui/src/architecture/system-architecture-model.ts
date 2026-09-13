import type { SystemComponent, SystemDependency, SystemOverviewGroup, SystemPath, SystemPathEdge, SystemProjection, SystemSymbol } from './system-architecture-source';

export interface SystemSceneNode {
    id: string; label: string; detail: string; position: [number, number, number];
    component?: SystemComponent; symbol?: SystemSymbol;
    pathIndices?: number[]; depth?: number; handoff?: boolean; hiddenChildren?: number;
    group?: SystemOverviewGroup; groupId?: string; componentIds?: string[];
    kind?: 'group' | 'component' | 'remainder'; parentId?: string; expanded?: boolean;
    size?: [number, number, number]; inCorridor?: boolean;
    visual?: { color: string; label: string; basis?: string };
}
export interface SystemSceneEdge {
    id: string; source: string; target: string; type: string; count: number;
    dependency?: SystemDependency; pathEdge?: SystemPath['edges'][number];
    pathIndices?: number[]; depth?: number; handoff?: boolean;
    dependencies?: SystemDependency[]; types?: string[]; cycle?: boolean;
    inCorridor?: boolean; behaviorEdges?: SystemPathEdge[];
}
export interface SystemSceneLane {
    id: string; label: string; position: [number, number, number]; width: number; depth: number;
    height?: number; pathIndices?: number[];
}
export interface SystemSceneModel {
    nodes: SystemSceneNode[]; edges: SystemSceneEdge[]; lanes: SystemSceneLane[];
    omittedNodes: number; omittedEdges: number;
    focusId?: string; scopeKey?: string; preferredPathIndex?: number;
    omittedNeighbors?: number;
    highlightActive?: boolean; componentNodeIds?: Record<string, string>; groupNodeIds?: Record<string, string>;
    captions?: { label: string; position: [number, number, number] }[];
}
export const componentBasis = (basis: string): string => basis === 'declared_module' ? 'Declared module'
    : basis === 'interaction_community' ? 'Inferred collaboration' : basis === 'unassigned' ? 'Unassigned'
        : basis === 'common_source_directory_aggregate' ? 'Source directory group' : basis === 'aggregate_remainder' ? 'Other components' : 'Inferred component';

export interface SystemOverviewOptions {
    expandedGroupIds?: readonly string[]; focusId?: string; relationshipTypes?: readonly string[];
    includeTests?: boolean; includeUnconnected?: boolean; filter?: string; cyclesOnly?: boolean; layoutKey?: string;
}

/** Tarjan groups are layout constraints, not a claim about execution cycles. */
function stronglyConnected(ids: readonly string[], pairs: readonly { source: string; target: string }[]): string[][] {
    const allowed = new Set(ids), adjacent = new Map(ids.map(id => [id, new Set<string>()]));
    for (const edge of pairs) if (allowed.has(edge.source) && allowed.has(edge.target)) adjacent.get(edge.source)!.add(edge.target);
    let next = 0;
    const index = new Map<string, number>(), low = new Map<string, number>(), active = new Set<string>(), stack: string[] = [], result: string[][] = [];
    const visit = (id: string) => {
        index.set(id, next); low.set(id, next++); stack.push(id); active.add(id);
        for (const target of [...adjacent.get(id)!].sort()) {
            if (!index.has(target)) { visit(target); low.set(id, Math.min(low.get(id)!, low.get(target)!)); }
            else if (active.has(target)) low.set(id, Math.min(low.get(id)!, index.get(target)!));
        }
        if (low.get(id) !== index.get(id)) return;
        const members: string[] = [];
        let member: string;
        do { member = stack.pop()!; active.delete(member); members.push(member); } while (member !== id);
        result.push(members.sort());
    };
    [...ids].sort().forEach(id => { if (!index.has(id)) visit(id); });
    return result.sort((a, b) => a[0].localeCompare(b[0]));
}

interface OverviewLayout { positions: Map<string, [number, number, number]>; lanes: SystemSceneLane[]; key: string }
const overviewLayouts = new Map<string, OverviewLayout>();
const comparePath = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
const normalizedPath = (path: string) => path.replaceAll('\\', '/').replace(/^\.\//, '').replace(/\/+$/, '');

/** A location hint only: representatives cannot establish full component membership. */
function groupDirectory(group: SystemOverviewGroup, groupingBasis?: string): string | undefined {
    if ((group.basis ?? groupingBasis) === 'common_source_directory_aggregate') {
        const path = normalizedPath(group.label);
        if (['.', '(root)'].includes(path)) return '';
        if (path === '@no-source') return undefined;
        return path;
    }
    const paths = group.representatives.map(symbol => symbol.file_path).filter((path): path is string => Boolean(path));
    if (!paths.length) return undefined;
    const directories = paths.map(path => normalizedPath(path).split('/').slice(0, -1));
    const common = [...directories[0]];
    for (const directory of directories.slice(1)) while (common.some((segment, index) => directory[index] !== segment)) common.pop();
    return common.join('/');
}

/** Directory neighborhoods organize space, never execution order or responsibilities.
 * Calculate from the full snapshot before filters; expansion stays inside fixed footprints. */
function overviewLayout(groups: readonly SystemOverviewGroup[], groupingBasis?: string): OverviewLayout {
    const ordered = groups.map(group => ({ group, path: groupDirectory(group, groupingBasis) }))
        .sort((a, b) => comparePath(a.path ?? '\uffff', b.path ?? '\uffff') || comparePath(a.group.label, b.group.label) || comparePath(a.group.id, b.group.id));
    const key = JSON.stringify(ordered.map(({ group, path }) => [group.id, group.label, path]));
    const cached = overviewLayouts.get(key);
    if (cached) return cached;
    // Skip a common wrapper directory, so src/api and src/store can form distinct
    // neighborhoods in repositories whose entire source lives below src.
    const paths = ordered.flatMap(item => item.path ? [item.path.split('/')] : []);
    const prefix = [...(paths[0] ?? [])];
    for (const path of paths.slice(1)) while (prefix.some((part, index) => path[index] !== part)) prefix.pop();
    const buckets = new Map<string, typeof ordered>();
    for (const item of ordered) {
        const parts = item.path?.split('/') ?? [];
        const directory = item.path === undefined ? '\uffff' : !item.path ? '' : parts.slice(0, Math.min(parts.length, prefix.length + 1)).join('/');
        if (!buckets.has(directory)) buckets.set(directory, []);
        buckets.get(directory)!.push(item);
    }
    const regions = [...buckets].map(([path, members]) => {
        const columns = Math.max(1, Math.min(6, Math.ceil(Math.sqrt(members.length * 34 / 76 * 1.3))));
        return { path, members, columns, width: columns * 76 + 16, height: Math.ceil(members.length / columns) * 34 + 24 };
    });
    const rowWidth = Math.max(0, ...regions.map(region => region.width), Math.sqrt(regions.reduce((area, region) => area + (region.width + 18) * (region.height + 18), 0) * 1.5));
    const positions = new Map<string, [number, number, number]>(), lanes: SystemSceneLane[] = [];
    let x = 0, y = 0, rowHeight = 0, width = 0;
    for (const region of regions) {
        if (x && x + region.width > rowWidth) { x = 0; y += rowHeight + 18; rowHeight = 0; }
        region.members.forEach(({ group }, index) => positions.set(group.id, [x + 46 + index % region.columns * 76, -y - 28 - Math.floor(index / region.columns) * 34, 0]));
        lanes.push({ id: `directory:${region.path}`, label: region.path === '\uffff' ? 'Location unavailable' : region.path || 'Repository root',
            position: [x + region.width / 2, -y - region.height / 2, -4], width: region.width, height: region.height, depth: .5 });
        width = Math.max(width, x + region.width); rowHeight = Math.max(rowHeight, region.height); x += region.width + 18;
    }
    const height = y + rowHeight;
    for (const position of [...positions.values(), ...lanes.map(lane => lane.position)]) { position[0] -= width / 2; position[1] += height / 2; }
    const layout = { positions, lanes, key };
    overviewLayouts.set(key, layout);
    if (overviewLayouts.size > 12) overviewLayouts.delete(overviewLayouts.keys().next().value!);
    return layout;
}

/** The default overview is the complete returned coarse graph, not an ego graph. */
export function systemOverviewGraph(data: SystemProjection, options: SystemOverviewOptions = {}): SystemSceneModel {
    const components = data.overview?.components ?? data.components;
    const componentMap = new Map(components.map(component => [component.id, component]));
    const groups: SystemOverviewGroup[] = data.overview?.groups ?? components.map(component => ({ ...component, component_count: 1, component_ids: [component.id] }));
    const allEdges = data.overview?.connections ?? data.dependencies;
    const layout = overviewLayout(groups, data.overview?.grouping_basis);
    const relationshipTypes = options.relationshipTypes ? new Set(options.relationshipTypes) : undefined;
    const eligibleGroups = groups.filter(group => options.includeTests || group.role !== 'test');
    const eligibleIds = new Set(eligibleGroups.map(group => group.id));
    const edges = allEdges.filter(edge => (!relationshipTypes || relationshipTypes.has(edge.type)) && eligibleIds.has(edge.source) && eligibleIds.has(edge.target));
    const cycleIndex = new Map<string, number>();
    stronglyConnected(eligibleGroups.map(group => group.id), edges).filter(members => members.length > 1)
        .forEach((members, index) => members.forEach(id => cycleIndex.set(id, index)));
    const connected = new Set(edges.filter(edge => edge.source !== edge.target).flatMap(edge => [edge.source, edge.target]));
    const query = options.filter?.trim().toLocaleLowerCase();
    const visibleGroups = eligibleGroups.filter(group => (options.includeUnconnected !== false || connected.has(group.id)) && (!options.cyclesOnly || cycleIndex.has(group.id))
        && (!query || group.label.toLocaleLowerCase().includes(query) || group.component_ids.some(id => componentMap.get(id)?.label.toLocaleLowerCase().includes(query))));
    const visible = new Set(visibleGroups.map(group => group.id)), expanded = new Set(options.expandedGroupIds);
    const nodes: SystemSceneNode[] = [], componentNodeIds: Record<string, string> = {}, groupNodeIds: Record<string, string> = {};
    for (const group of visibleGroups) {
        const position = layout.positions.get(group.id)!, members = group.component_ids.map(id => componentMap.get(id)).filter((item): item is SystemComponent => Boolean(item));
        const isExpanded = expanded.has(group.id) && members.length > 0;
        const groupComponent: SystemComponent = data.overview ? { id: group.id, label: group.label,
            basis: group.basis ?? data.overview.grouping_basis, member_count: group.member_count, file_count: group.file_count,
            role: group.role, representatives: group.representatives } : members[0];
        nodes.push({ id: group.id, label: group.label, detail: `${group.component_count.toLocaleString()} components · ${group.member_count.toLocaleString()} symbols`,
            component: groupComponent, group: data.overview ? group : undefined, kind: data.overview ? 'group' : 'component',
            groupId: group.id, componentIds: [...group.component_ids], expanded: isExpanded, position: [...position], size: [62, 22, 4] });
        groupNodeIds[group.id] = group.id;
        group.component_ids.forEach(id => { componentNodeIds[id] = group.id; });
        if (!isExpanded) continue;
        const sorted = [...members].sort((a, b) => Number(b.id === options.focusId) - Number(a.id === options.focusId) || a.id.localeCompare(b.id));
        const shown = sorted.length > 12 ? sorted.slice(0, 11) : sorted, remainder = sorted.slice(shown.length);
        const tiles = [...shown.map(component => ({ component, ids: [component.id], kind: 'component' as const })), ...(remainder.length ? [{
            component: { id: `${group.id}:remaining`, label: `${remainder.length} other components`, basis: 'aggregate_remainder',
                member_count: remainder.reduce((sum, component) => sum + component.member_count, 0), file_count: remainder.reduce((sum, component) => sum + component.file_count, 0), representatives: [] },
            ids: remainder.map(component => component.id), kind: 'remainder' as const,
        }] : [])];
        tiles.forEach(({ component, ids, kind }, index) => {
            const id = `member:${group.id}:${component.id}`;
            nodes.push({ id, label: component.label, detail: `${component.member_count.toLocaleString()} symbols`, component, componentIds: ids,
                kind, groupId: group.id, parentId: group.id, position: [position[0] + (index % 3 - 1) * 19, position[1] + 6 - Math.floor(index / 3) * 5, 4], size: [13, 3, 2] });
            ids.forEach(componentId => { componentNodeIds[componentId] = id; });
        });
    }
    const aggregates = new Map<string, SystemSceneEdge>();
    for (const dependency of edges) {
        if (!visible.has(dependency.source) || !visible.has(dependency.target)) continue;
        const cycle = dependency.source !== dependency.target && cycleIndex.has(dependency.source) && cycleIndex.get(dependency.source) === cycleIndex.get(dependency.target);
        if (options.cyclesOnly && !cycle) continue;
        const id = `aggregate:${JSON.stringify([dependency.source, dependency.target])}`;
        let aggregate = aggregates.get(id);
        if (!aggregate) { aggregate = { id, source: dependency.source, target: dependency.target, type: dependency.type, count: 0, dependencies: [], types: [], cycle }; aggregates.set(id, aggregate); }
        aggregate.dependencies!.push(dependency); aggregate.count += dependency.count;
        if (!aggregate.types!.includes(dependency.type)) aggregate.types!.push(dependency.type);
        aggregate.types!.sort(); aggregate.type = aggregate.types!.length === 1 ? aggregate.types![0] : 'RELATIONSHIPS';
    }
    const renderedEdges = [...aggregates.values()].sort((a, b) => a.id.localeCompare(b.id));
    const shownDependencyCount = renderedEdges.reduce((sum, edge) => sum + (edge.dependencies?.length ?? 0), 0);
    return { nodes, edges: renderedEdges, lanes: layout.lanes, componentNodeIds, groupNodeIds,
        focusId: options.focusId ? (nodes.some(node => node.id === options.focusId) ? options.focusId : componentNodeIds[options.focusId]) : undefined,
        scopeKey: `overview:${options.layoutKey ?? ''}:${layout.key}`, omittedNodes: groups.length - visibleGroups.length,
        omittedEdges: allEdges.length - shownDependencyCount + Number(data.overview?.limits.omitted_connections ?? 0) };
}

/** Decorate the shared overview with exact corridor evidence; never infer extra joins. */
export function systemBehaviorOverviewGraph(data: SystemProjection, base: SystemSceneModel, paths: readonly SystemPath[] = data.paths, activePathIndex?: number): SystemSceneModel {
    const behavior = data.behavior;
    if (!behavior || behavior.mode !== 'corridor' || behavior.target_id === undefined) return base;
    const symbols = new Map(behavior.nodes.map(symbol => [symbol.id, symbol]));
    const nodeFor = (symbol: SystemSymbol) => base.componentNodeIds?.[symbol.component_id] ?? base.groupNodeIds?.[symbol.group_id ?? ''];
    const groupFor = (symbol: SystemSymbol) => symbol.group_id ?? base.nodes.find(node => node.componentIds?.includes(symbol.component_id))?.groupId;
    const visibleIds = new Set(base.nodes.map(node => node.id)), corridorNodes = new Set<string>();
    const nodePaths = new Map<string, Set<number>>(), edgePaths = new Map<number, Set<number>>();
    for (const [index, path] of paths.entries()) if (isContiguousPath(path)) {
        path.nodes.forEach(symbol => {
            for (const id of new Set([nodeFor(symbol), groupFor(symbol)])) if (id) { if (!nodePaths.has(id)) nodePaths.set(id, new Set()); nodePaths.get(id)!.add(index); }
        });
        path.edges.forEach(edge => { if (!edgePaths.has(edge.id)) edgePaths.set(edge.id, new Set()); edgePaths.get(edge.id)!.add(index); });
    }
    behavior.nodes.forEach(symbol => { const id = nodeFor(symbol), group = groupFor(symbol); if (id) corridorNodes.add(id); if (group) corridorNodes.add(group); });
    const edges = base.edges.map(edge => ({ ...edge, inCorridor: false, behaviorEdges: [] as SystemPathEdge[], pathIndices: [] as number[] }));
    const aggregateByPair = new Map(edges.map(edge => [JSON.stringify([edge.source, edge.target]), edge]));
    for (const evidence of behavior.edges) {
        const source = symbols.get(evidence.source_id), target = symbols.get(evidence.target_id);
        if (!source || !target) continue;
        const from = groupFor(source), to = groupFor(target);
        if (!from || !to || !visibleIds.has(from) || !visibleIds.has(to)) continue;
        const key = JSON.stringify([from, to]);
        let edge = aggregateByPair.get(key);
        if (!edge) {
            edge = { id: `corridor:${key}`, source: from, target: to, type: evidence.type, types: [evidence.type], count: 0,
                dependencies: [], inCorridor: true, behaviorEdges: [], pathIndices: [] };
            edges.push(edge); aggregateByPair.set(key, edge);
        }
        edge.inCorridor = true; edge.behaviorEdges.push(evidence);
        edge.pathIndices = [...new Set([...edge.pathIndices, ...(edgePaths.get(evidence.id) ?? [])])].sort((a, b) => a - b);
        if (!edge.dependencies?.length) edge.count++;
    }
    return { ...base, highlightActive: true, preferredPathIndex: activePathIndex,
        nodes: base.nodes.map(node => ({ ...node, inCorridor: corridorNodes.has(node.id), pathIndices: [...(nodePaths.get(node.id) ?? [])] })), edges };
}

/** A component cycle does not establish a contiguous symbol path through that component. */
export function isContiguousPath(path: SystemPath): boolean {
    return path.nodes.length > 0 && path.entrypoint_id === path.nodes[0].id && path.edges.length === path.nodes.length - 1
        && path.edges.every((edge, index) => edge.source_id === path.nodes[index].id && edge.target_id === path.nodes[index + 1].id);
}

/** Prioritize collaboration breadth, so isolated documents do not consume the scene budget. */
export function systemComponents(data: SystemProjection, filter: string, cyclesOnly: boolean, includeUnconnected = false, includeTests = false): SystemComponent[] {
    const query = filter.trim().toLocaleLowerCase();
    const candidates = data.components.filter(component => includeTests || component.role !== 'test');
    const candidateIds = new Set(candidates.map(component => component.id));
    const neighbors = new Map<string, Set<string>>(), counts = new Map<string, number>();
    for (const edge of data.dependencies) {
        if (edge.source === edge.target || !candidateIds.has(edge.source) || !candidateIds.has(edge.target)) continue;
        for (const [id, other] of [[edge.source, edge.target], [edge.target, edge.source]]) {
            if (!neighbors.has(id)) neighbors.set(id, new Set());
            neighbors.get(id)!.add(other); counts.set(id, (counts.get(id) ?? 0) + edge.count);
        }
    }
    const cycles = new Set(data.cycles.flatMap(cycle => cycle.component_ids));
    return candidates.filter(component => (includeUnconnected || neighbors.has(component.id)) && (!cyclesOnly || cycles.has(component.id))
        && (!query || [component.label, ...component.representatives.flatMap(symbol => [symbol.name, symbol.file_path ?? ''])]
            .some(value => value.toLocaleLowerCase().includes(query))))
        .sort((a, b) => (neighbors.get(b.id)?.size ?? 0) - (neighbors.get(a.id)?.size ?? 0)
            || (counts.get(b.id) ?? 0) - (counts.get(a.id) ?? 0) || a.id.localeCompare(b.id));
}

export function systemComponentGraph(data: SystemProjection, filter: string, cyclesOnly: boolean, includeUnconnected = false, includeTests = false, focusId?: string): SystemSceneModel {
    const cycleGroups = new Map<string, number>();
    data.cycles.forEach((cycle, index) => cycle.component_ids.forEach(id => cycleGroups.set(id, index)));
    const matching = systemComponents(data, filter, cyclesOnly, includeUnconnected, includeTests);
    const focus = matching.find(component => component.id === focusId) ?? matching[0];
    if (!focus) return { nodes: [], edges: [], lanes: [], omittedNodes: 0, omittedEdges: 0, scopeKey: 'structure:empty' };
    const matchingIds = new Set(matching.map(component => component.id));
    const dependencies = data.dependencies.filter(edge => matchingIds.has(edge.source) && matchingIds.has(edge.target)
        && (!cyclesOnly || cycleGroups.get(edge.source) === cycleGroups.get(edge.target)));
    const adjacent = dependencies.filter(edge => edge.source === focus.id || edge.target === focus.id);
    const inbound = new Set(adjacent.filter(edge => edge.target === focus.id).map(edge => edge.source));
    const outbound = new Set(adjacent.filter(edge => edge.source === focus.id).map(edge => edge.target));
    const focalCounts = new Map<string, number>();
    for (const edge of adjacent) {
        const neighbor = edge.source === focus.id ? edge.target : edge.source;
        focalCounts.set(neighbor, (focalCounts.get(neighbor) ?? 0) + edge.count);
    }
    // The neighborhood explains this component, so unrelated global connectivity
    // must not displace its strongest direct relationships from the scene budget.
    const neighbors = matching.filter(component => component.id !== focus.id && (inbound.has(component.id) || outbound.has(component.id)))
        .sort((a, b) => (focalCounts.get(b.id) ?? 0) - (focalCounts.get(a.id) ?? 0) || a.id.localeCompare(b.id));
    const candidates: SystemComponent[][] = [[], [], []];
    for (const component of neighbors) {
        const column = inbound.has(component.id) && outbound.has(component.id) ? 1 : inbound.has(component.id) ? 0 : 2;
        candidates[column].push(component);
    }
    // Keep one prolific direction from producing an unreadable vertical stack.
    // Round-robin admission retains evidence from both directions under the total cap.
    const columns: SystemComponent[][] = [[], [], []];
    for (let row = 0, admitted = 0; row < 7 && admitted < 13; row++) {
        for (const column of [0, 2, 1]) {
            if (admitted >= 13 || (column === 1 && row >= 3) || !candidates[column][row]) continue;
            columns[column].push(candidates[column][row]); admitted++;
        }
    }
    const visible = [focus, ...columns.flat()];
    const ids = new Set(visible.map(component => component.id));
    const columnWidth = columns[0].length && columns[2].length ? 64 : 94;
    const nodes = visible.map(component => {
        const column = inbound.has(component.id) && outbound.has(component.id) ? 1 : inbound.has(component.id) ? 0 : 2;
        const peers = columns[column], index = peers.indexOf(component);
        return { id: component.id, label: component.label, component,
            detail: `${component.member_count.toLocaleString()} symbols · ${component.file_count.toLocaleString()} files`,
            position: component.id === focus.id ? [0, 0, 0] as [number, number, number]
                : [(column - 1) * columnWidth, column === 1 ? 16 + index * 9 : (index - (peers.length - 1) / 2) * 9, column === 1 ? -3 : 0] as [number, number, number] };
    });
    const edges = adjacent.filter(edge => ids.has(edge.source) && ids.has(edge.target)).map(edge => ({
        id: `dependency:${edge.source}:${edge.target}:${edge.type}`, source: edge.source, target: edge.target,
        type: edge.type, count: edge.count, dependency: edge,
    }));
    const headingY = Math.max(...nodes.map(node => node.position[1])) + 17;
    const captions = ['Used by', 'Focus', 'Uses'].flatMap((label, column) => column === 1 || columns[column].length
        ? [{ label, position: [(column - 1) * columnWidth, headingY, 0] as [number, number, number] }] : []);
    return { nodes, lanes: [], edges, captions, focusId: focus.id,
        scopeKey: `structure:${focus.id}:${nodes.map(node => `${node.id}@${node.position.join(':')}`).join(',')}:${edges.map(edge => edge.id).join(',')}`,
        omittedNodes: matching.length - nodes.length, omittedEdges: dependencies.length - edges.length, omittedNeighbors: neighbors.length - visible.length + 1 };
}

/** Prefer informative paths without changing their original indices or claiming runtime order. */
export function rankSystemPaths(paths: readonly SystemPath[]): number[] {
    const handoffs = (path: SystemPath) => path.edges.reduce((count, _, index) => count + Number(path.nodes[index].component_id !== path.nodes[index + 1].component_id), 0);
    return paths.map((path, index) => ({ path, index })).filter(item => isContiguousPath(item.path))
        .sort((a, b) => handoffs(b.path) - handoffs(a.path) || b.path.edges.length - a.path.edges.length || a.index - b.index).map(item => item.index);
}

/** Merge identical prefixes only: reconvergent symbols remain separate path occurrences. */
export function systemBehaviorGraph(data: SystemProjection, input: SystemPath | readonly SystemPath[] | undefined, activePathIndex?: number): SystemSceneModel {
    const paths: readonly SystemPath[] = !input ? [] : Array.isArray(input) ? input : [input as SystemPath];
    const entry = paths.find(isContiguousPath)?.entrypoint_id;
    const ranked = rankSystemPaths(paths).filter(index => paths[index].entrypoint_id === entry);
    if (!ranked.length) return { nodes: [], edges: [], lanes: [], omittedNodes: 0, omittedEdges: 0, scopeKey: 'behavior:empty' };
    const allNodes = new Map<string, SystemSceneNode>(), allEdges = new Map<string, SystemSceneEdge>();
    const children = new Map<string, string[]>(), groups = new Map<string, string>();
    const single = !Array.isArray(input);
    for (const pathIndex of ranked) {
        const path = paths[pathIndex];
        let previous = '', prefix = `branch:${entry}`;
        path.nodes.forEach((symbol, depth) => {
            const sourceEdge = path.edges[depth - 1];
            if (sourceEdge) prefix += `/${sourceEdge.id}:${sourceEdge.type}:${symbol.id}`;
            const id = single ? `step:${depth}` : prefix;
            const handoff = depth > 0 && symbol.component_id !== path.nodes[depth - 1].component_id;
            if (!allNodes.has(id)) {
                allNodes.set(id, { id, label: symbol.name, symbol, detail: symbol.file_path ?? symbol.qualified_name,
                    position: [depth * 18, 0, 0], pathIndices: [], depth, handoff });
                groups.set(id, previous && !handoff ? groups.get(previous)! : id);
                if (previous) children.set(previous, [...(children.get(previous) ?? []), id]);
            }
            allNodes.get(id)!.pathIndices!.push(pathIndex);
            if (sourceEdge) {
                const edgeId = single ? `path:${depth - 1}:${sourceEdge.id}` : `edge:${id}`;
                if (!allEdges.has(edgeId)) allEdges.set(edgeId, { id: edgeId, source: previous, target: id, type: sourceEdge.type,
                    count: 1, pathEdge: sourceEdge, pathIndices: [], depth, handoff });
                allEdges.get(edgeId)!.pathIndices!.push(pathIndex);
            }
            previous = id;
        });
    }
    const ordered = [...allNodes.values()];
    const visibleIds = new Set(ordered.filter(node => activePathIndex !== undefined && node.pathIndices?.includes(activePathIndex)).slice(0, 36).map(node => node.id));
    for (const node of ordered) { if (visibleIds.size >= 36) break; visibleIds.add(node.id); }
    // Preserve original prefix order when the chosen branch already fits. Highlighting alone
    // must not move nodes, reset the camera, or alter what a selected node ID means.
    const nodes = ordered.filter(node => visibleIds.has(node.id));
    const edges = [...allEdges.values()].filter(edge => visibleIds.has(edge.source) && visibleIds.has(edge.target));
    let leaf = 0;
    const place = (id: string): number => {
        const node = allNodes.get(id)!;
        const childIds = (children.get(id) ?? []).filter(child => visibleIds.has(child));
        const ys = childIds.map(place);
        const y = ys.length ? (ys[0] + ys[ys.length - 1]) / 2 : leaf++ * 10;
        node.position[1] = -y;
        node.hiddenChildren = (children.get(id)?.length ?? 0) - childIds.length;
        return y;
    };
    place(nodes[0].id);
    const middle = Math.max(0, leaf - 1) * 5;
    nodes.forEach(node => { node.position[1] += middle; node.pathIndices!.sort((a, b) => a - b); });
    const components = new Map(data.components.map(component => [component.id, component]));
    const laneGroups = new Map<string, SystemSceneNode[]>();
    for (const node of nodes) { const group = groups.get(node.id)!; laneGroups.set(group, [...(laneGroups.get(group) ?? []), node]); }
    const lanes = [...laneGroups.entries()].map(([id, members]) => {
        const xs = members.map(node => node.position[0]), ys = members.map(node => node.position[1]);
        const left = Math.min(...xs), right = Math.max(...xs), bottom = Math.min(...ys), top = Math.max(...ys);
        const componentId = members[0].symbol!.component_id;
        return { id, label: components.get(componentId)?.label ?? componentId,
            position: [(left + right) / 2, (bottom + top) / 2 + 1, -3] as [number, number, number],
            width: right - left + 10, height: top - bottom + 6, depth: 0.65,
            pathIndices: [...new Set(members.flatMap(node => node.pathIndices ?? []))] };
    });
    return { nodes, edges, lanes, omittedNodes: allNodes.size - nodes.length, omittedEdges: allEdges.size - edges.length,
        preferredPathIndex: ranked[0], scopeKey: `behavior:${nodes.map(node => node.id).join('|')}` };
}
