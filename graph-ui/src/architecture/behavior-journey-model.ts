import type { SystemSceneModel, SystemSceneNode } from './system-architecture-model';
import type { SystemPath, SystemPathEdge, SystemProjection, SystemSymbol } from './system-architecture-source';

export interface BehaviorJourneyOptions {
    entryId?: number; targetId?: number; pathIndex?: number; filter?: string;
}
export interface BehaviorJourneyChoice extends SystemSymbol { distance?: number; direct: boolean }
export interface BehaviorJourney {
    scene: SystemSceneModel; paths: SystemPath[]; path?: SystemPath; entry?: SystemSymbol;
    mode: 'choices' | 'path' | 'corridor' | 'empty'; choices: BehaviorJourneyChoice[];
    counts: {
        directChoices: number; reachableTargets: number; validPaths: number; invalidPaths: number;
        filteredPaths: number; omittedChoices: number; omittedNodes: number; omittedEdges: number;
    };
    limits: { maxChoices: number; maxNodes: number; maxEdges: number; hit: string[]; sampled: boolean };
}

const MAX_CHOICES = 12, MAX_NODES = 60, MAX_EDGES = 120;
const INVOCATIONS = new Set(['CALLS', 'HTTP_CALLS', 'ASYNC_CALLS', 'GRPC_CALLS', 'GRAPHQL_CALLS', 'TRPC_CALLS',
    'CROSS_HTTP_CALLS', 'CROSS_ASYNC_CALLS', 'CROSS_GRPC_CALLS', 'CROSS_GRAPHQL_CALLS', 'CROSS_TRPC_CALLS']);
const invocation = (edge: { type: string }) => INVOCATIONS.has(edge.type.trim().replaceAll('-', '_').toUpperCase());
const edgeKey = (edge: SystemPathEdge) => JSON.stringify([edge.id, edge.source_id, edge.target_id, edge.type]);
const compareText = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;

/** Every consecutive hop must have its own invocation witness, including repeated symbols. */
function validPath(path: SystemPath): boolean {
    return path.nodes.length > 0 && path.entrypoint_id === path.nodes[0].id && path.edges.length === path.nodes.length - 1
        && path.edges.every((edge, index) => invocation(edge)
            && edge.source_id === path.nodes[index].id && edge.target_id === path.nodes[index + 1].id);
}

function laneLayout(data: SystemProjection, nodes: SystemSceneNode[]): SystemSceneModel['lanes'] {
    const labels = new Map([...data.components, ...(data.overview?.components ?? [])].map(component => [component.id, component.label]));
    const groups = new Map<string, SystemSceneNode[]>();
    for (const node of nodes) {
        const component = node.symbol!.component_id;
        if (!groups.has(component)) groups.set(component, []);
        groups.get(component)!.push(node);
    }
    return [...groups].map(([id, members]) => {
        const xs = members.map(node => node.position[0]), ys = members.map(node => node.position[1]);
        const left = Math.min(...xs), right = Math.max(...xs), bottom = Math.min(...ys), top = Math.max(...ys);
        return { id: `journey-component:${id}`, label: labels.get(id) ?? id,
            position: [(left + right) / 2, (bottom + top) / 2, -2] as [number, number, number],
            width: right - left + 62, height: top - bottom + 32, depth: 0.4 };
    });
}

function pathScene(data: SystemProjection, path: SystemPath, index: number): SystemSceneModel {
    const components = [...new Set(path.nodes.map(symbol => symbol.component_id))];
    const nodes: SystemSceneNode[] = path.nodes.map((symbol, depth) => {
        const lane = components.indexOf(symbol.component_id);
        return { id: `journey:${path.entrypoint_id}:${depth}:${symbol.id}`, label: `${depth + 1}. ${symbol.name}`,
            symbol, detail: symbol.file_path ?? symbol.qualified_name, depth, pathIndices: [index],
            handoff: depth > 0 && path.nodes[depth - 1].component_id !== symbol.component_id,
            size: [48, 18, 3], position: [depth * 72, (lane - (components.length - 1) / 2) * 34, lane % 3 * 0.45] };
    });
    const edges = path.edges.map((edge, depth) => ({ id: `journey-edge:${depth}:${edgeKey(edge)}`,
        source: nodes[depth].id, target: nodes[depth + 1].id, type: edge.type, count: 1,
        pathEdge: edge, pathIndices: [index], depth: depth + 1, handoff: nodes[depth + 1].handoff }));
    return { nodes, edges, lanes: laneLayout(data, nodes), omittedNodes: 0, omittedEdges: 0,
        preferredPathIndex: index, scopeKey: `journey:path:${path.entrypoint_id}:${path.edges.map(edgeKey).join('|')}` };
}

/** Breadth-first traversal visits each symbol once and never joins component representatives. */
function corridorPath(data: SystemProjection, source: number, target: number, hit: Set<string>): SystemPath | undefined {
    const behavior = data.behavior;
    if (!behavior || behavior.mode !== 'corridor' || behavior.source_id !== source || behavior.target_id !== target) return undefined;
    const symbols = new Map(behavior.nodes.map(symbol => [symbol.id, symbol]));
    if (!symbols.has(source) || !symbols.has(target)) return undefined;
    const adjacent = new Map<number, SystemPathEdge[]>();
    for (const edge of behavior.edges) if (invocation(edge) && symbols.has(edge.source_id) && symbols.has(edge.target_id)) {
        if (!adjacent.has(edge.source_id)) adjacent.set(edge.source_id, []);
        adjacent.get(edge.source_id)!.push(edge);
    }
    for (const outgoing of adjacent.values()) outgoing.sort((a, b) => compareText(a.callsite?.file_path ?? '', b.callsite?.file_path ?? '')
        || (a.callsite?.line ?? 0) - (b.callsite?.line ?? 0) || a.id - b.id
        || a.target_id - b.target_id || compareText(a.type, b.type));
    const queue = [source], seen = new Set(queue), previous = new Map<number, SystemPathEdge>();
    let visitedEdges = 0;
    for (let at = 0; at < queue.length && !seen.has(target); at++) {
        for (const edge of adjacent.get(queue[at]) ?? []) {
            if (visitedEdges >= MAX_EDGES) { hit.add('corridor-edge-search'); break; }
            visitedEdges++;
            if (seen.has(edge.target_id)) continue;
            if (seen.size >= MAX_NODES) { hit.add('corridor-node-search'); continue; }
            seen.add(edge.target_id); previous.set(edge.target_id, edge); queue.push(edge.target_id);
            if (edge.target_id === target) break;
        }
        if (visitedEdges >= MAX_EDGES) break;
    }
    if (!seen.has(target)) return undefined;
    const nodes = [symbols.get(target)!], edges: SystemPathEdge[] = [];
    let current = target;
    while (current !== source) {
        const edge = previous.get(current)!;
        edges.push(edge); current = edge.source_id; nodes.push(symbols.get(current)!);
    }
    return { entrypoint_id: source, nodes: nodes.reverse(), edges: edges.reverse() };
}

/** A focused static journey: positions describe hops, never catalog distance or runtime order. */
export function behaviorJourney(data: SystemProjection, options: BehaviorJourneyOptions = {}): BehaviorJourney {
    const entryId = options.entryId ?? data.behavior?.source_id ?? data.entrypoints[0]?.id
        ?? data.paths.find(validPath)?.entrypoint_id;
    const scopedBehavior = data.behavior?.source_id === entryId ? data.behavior : undefined;
    const dependencies = [...data.dependencies, ...(data.overview?.connections ?? [])];
    const symbols = new Map<number, SystemSymbol>();
    const add = (symbol: SystemSymbol) => { if (!symbols.has(symbol.id)) symbols.set(symbol.id, symbol); };
    data.entrypoints.forEach(add);
    scopedBehavior?.nodes.forEach(add);
    const scopedPaths = data.paths.filter(path => path.entrypoint_id === entryId);
    const valid = scopedPaths.filter(validPath);
    valid.forEach(path => path.nodes.forEach(add));
    let sampledWitnesses = false;
    for (const dependency of dependencies) for (const witness of dependency.witnesses) {
        if (invocation(dependency) && witness.source.id === entryId) {
            add(witness.source); add(witness.target);
            sampledWitnesses ||= dependency.count > dependency.witnesses.length;
        }
    }
    const entry = entryId === undefined ? undefined : symbols.get(entryId);
    const hit = new Set(scopedBehavior?.limits_hit ?? []);
    const result: BehaviorJourney = {
        scene: { nodes: [], edges: [], lanes: [], omittedNodes: 0, omittedEdges: 0, scopeKey: `journey:empty:${entryId ?? ''}:${options.targetId ?? ''}` },
        paths: [], entry, mode: 'empty', choices: [],
        counts: { directChoices: 0, reachableTargets: scopedBehavior?.reachable_targets.length ?? 0,
            validPaths: 0, invalidPaths: scopedPaths.length - valid.length, filteredPaths: 0,
            omittedChoices: 0, omittedNodes: 0, omittedEdges: 0 },
        limits: { maxChoices: MAX_CHOICES, maxNodes: MAX_NODES, maxEdges: MAX_EDGES,
            hit: [], sampled: !data.complete || data.status === 'limited' || scopedBehavior?.complete === false
                || scopedBehavior?.corridor_complete === false || (options.targetId === undefined && (sampledWitnesses || data.overview?.complete === false)) },
    };
    const finish = (): BehaviorJourney => {
        result.limits.hit = [...hit];
        result.limits.sampled ||= hit.size > 0 || result.counts.omittedNodes > 0 || result.counts.omittedEdges > 0 || result.counts.omittedChoices > 0;
        result.scene.omittedNodes = result.counts.omittedNodes;
        result.scene.omittedEdges = result.counts.omittedEdges;
        return result;
    };
    if (!entry) return finish();
    const query = options.filter?.trim().toLocaleLowerCase() ?? '';
    const labels = new Map([...data.components, ...(data.overview?.components ?? [])].map(component => [component.id, component.label]));
    const matches = (symbol: SystemSymbol) => !query || [symbol.name, symbol.qualified_name, symbol.file_path ?? '', labels.get(symbol.component_id) ?? '']
        .some(value => value.toLocaleLowerCase().includes(query));

    if (options.targetId !== undefined) {
        let paths = valid.filter(path => path.nodes.at(-1)!.id === options.targetId);
        let derived = false;
        if (!paths.length) {
            const path = corridorPath(data, entry.id, options.targetId, hit);
            if (path) { paths = [path]; derived = true; }
        }
        result.counts.validPaths = paths.length;
        for (const path of paths) {
            if (!path.nodes.some(matches)) { result.counts.filteredPaths++; continue; }
            if (path.nodes.length > MAX_NODES || path.edges.length > MAX_EDGES) {
                result.counts.omittedNodes += path.nodes.length; result.counts.omittedEdges += path.edges.length;
                hit.add('whole-path-size'); continue;
            }
            result.paths.push(path);
        }
        const index = options.pathIndex !== undefined && Number.isInteger(options.pathIndex) && options.pathIndex >= 0 && options.pathIndex < result.paths.length ? options.pathIndex : 0;
        result.path = result.paths[index];
        if (result.path) { result.mode = derived ? 'corridor' : 'path'; result.scene = pathScene(data, result.path, index); }
        return finish();
    }

    const direct = new Map<string, SystemPathEdge>();
    const recordDirect = (edge: SystemPathEdge) => {
        if (edge.source_id === entry.id && invocation(edge) && symbols.has(edge.target_id)) direct.set(edgeKey(edge), edge);
    };
    scopedBehavior?.edges.forEach(recordDirect);
    valid.forEach(path => { if (path.edges[0]) recordDirect(path.edges[0]); });
    for (const dependency of dependencies) if (invocation(dependency)) {
        for (const witness of dependency.witnesses) if (witness.source.id === entry.id) recordDirect({
            ...witness, id: witness.edge_id, source_id: witness.source.id, target_id: witness.target.id, type: dependency.type,
            callsite: witness.callsite, resolution: witness.resolution,
        });
    }
    const callees = [...new Set([...direct.values()].map(edge => edge.target_id))].map(id => symbols.get(id)!)
        .sort((a, b) => compareText(a.file_path ?? '', b.file_path ?? '') || compareText(a.name, b.name) || a.id - b.id);
    result.counts.directChoices = callees.length;
    const candidates = callees.filter(symbol => matches(entry) || matches(symbol));
    const shown = candidates.slice(0, MAX_CHOICES), shownIds = new Set(shown.map(symbol => symbol.id));
    if (candidates.length > shown.length) hit.add('direct-choices');
    result.choices = shown.map(symbol => ({ ...symbol, distance: 1, direct: true }));
    const calleeIds = new Set(callees.map(symbol => symbol.id));
    const catalog = new Map<number, BehaviorJourneyChoice>();
    for (const target of scopedBehavior?.reachable_targets ?? []) if (matches(target) && !calleeIds.has(target.id)) {
        catalog.set(target.id, { ...target, direct: false });
    }
    // Older projections expose sampled paths without a separate destination catalog.
    if (!scopedBehavior) for (const path of valid) for (let depth = 1; depth < path.nodes.length; depth++) {
        const target = path.nodes[depth];
        if (target.id !== entry.id && !calleeIds.has(target.id) && matches(target) && !catalog.has(target.id))
            catalog.set(target.id, { ...target, distance: depth, direct: false });
    }
    result.choices.push(...[...catalog.values()].slice(0, MAX_NODES - result.choices.length));
    result.counts.omittedChoices = new Set([...candidates.map(symbol => symbol.id), ...catalog.keys()]).size - result.choices.length;
    if (result.counts.omittedChoices > 0) hit.add('target-choices');
    const visible = [entry, ...shown.filter(symbol => symbol.id !== entry.id)];
    const nodes: SystemSceneNode[] = visible.map((symbol, index) => ({ id: `journey-choice:${symbol.id}`, label: symbol.name,
        symbol, detail: symbol.file_path ?? symbol.qualified_name, depth: index === 0 ? 0 : 1,
        size: [48, 18, 3], position: index === 0 ? [0, 0, 0] : [88, (index - 1 - (visible.length - 2) / 2) * 28, 0] }));
    const ids = new Set(visible.map(symbol => symbol.id));
    const eligibleEdges = [...direct.values()].filter(edge => ids.has(edge.target_id));
    // Reserve one witness per visible branch before admitting parallel callsites.
    const admitted = new Set<string>(), connected = new Set<number>();
    for (const edge of eligibleEdges) if (!connected.has(edge.target_id)) { admitted.add(edgeKey(edge)); connected.add(edge.target_id); }
    for (const edge of eligibleEdges) { if (admitted.size >= MAX_EDGES) break; admitted.add(edgeKey(edge)); }
    const edges = eligibleEdges.filter(edge => admitted.has(edgeKey(edge))).map(edge => ({ id: `journey-choice-edge:${edgeKey(edge)}`,
        source: `journey-choice:${entry.id}`, target: `journey-choice:${edge.target_id}`, type: edge.type, count: 1,
        pathEdge: edge, depth: 1, handoff: symbols.get(edge.target_id)!.component_id !== entry.component_id }));
    result.counts.omittedNodes = candidates.filter(symbol => symbol.id !== entry.id && !shownIds.has(symbol.id)).length;
    result.counts.omittedEdges = [...direct.values()].filter(edge => matches(entry) || matches(symbols.get(edge.target_id)!)).length - edges.length;
    if (eligibleEdges.length > edges.length) hit.add('direct-edges');
    result.mode = 'choices';
    result.scene = { nodes, edges, lanes: laneLayout(data, nodes), omittedNodes: 0, omittedEdges: 0,
        scopeKey: `journey:choices:${entry.id}:${nodes.map(node => node.id).join('|')}` };
    return finish();
}
