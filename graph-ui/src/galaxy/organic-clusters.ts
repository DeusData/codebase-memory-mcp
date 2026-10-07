import type { GraphData, GraphNode } from './types';

export interface OrganicCluster {
    id: string;
    nodeIds: number[];
    /** A shared source directory, when available; never an inferred architectural role. */
    label: string;
    center: { x: number; y: number; z: number };
    rootCount: number;
    internalEdges: number;
    boundaryEdges: number;
}

export interface OrganicClusterOptions {
    rootIds?: ReadonlySet<number>;
    /** Existing visible positions stay fixed when another trace layer arrives. */
    previous?: GraphData;
}

export interface OrganicClusterLayout {
    data: GraphData;
    groups: OrganicCluster[];
    groupByNode: Map<number, string>;
}

type Point = { x: number; y: number; z: number };
type Pair = { a: number; b: number; weight: number };
type Particle = Point & { radius: number; fixed: boolean; home: Point };
type Cell = Point & { count: number; radius: number };

type GeometryMemo = {
    nodes: Map<number, { size: number; file?: string; root: boolean; previous?: Point }>;
    edges: [number | undefined, number, number, string][];
    positions: Map<number, Point>;
    hasPrevious: boolean;
    groups: OrganicCluster[];
    groupByNode: Map<number, string>;
    weight: number;
};
const geometryMemo: GeometryMemo[] = [];
const MAX_GEOMETRY_ENTRIES = 4;
// Counts retained node records, member IDs and edge records, not just graphs.
const MAX_GEOMETRY_WEIGHT = 100_000;
const copyGroups = (groups: readonly OrganicCluster[]) => groups.map(group => ({ ...group,
    nodeIds: [...group.nodeIds], center: { ...group.center } }));

function samePoint(a: Point | undefined, b: Point | undefined): boolean {
    return a === undefined || b === undefined ? a === b : a.x === b.x && a.y === b.y && a.z === b.z;
}

/** Exact comparisons avoid hashing or sorting on a warm selection. Metadata
 * unrelated to geometry is intentionally excluded and always read afresh. */
function cachedGeometry(data: GraphData, roots: ReadonlySet<number> | undefined, previous: Map<number, GraphNode>): OrganicClusterLayout | undefined {
    for (let index = 0; index < geometryMemo.length; index++) {
        const cached = geometryMemo[index]!;
        if (cached.nodes.size !== data.nodes.length || cached.edges.length !== data.edges.length || cached.hasPrevious !== (previous.size > 0)) continue;
        if (!data.nodes.every(node => {
            const saved = cached.nodes.get(node.id);
            return saved && Object.is(saved.size, node.size) && saved.file === node.file_path
                && saved.root === Boolean(roots?.has(node.id)) && samePoint(saved.previous, previous.get(node.id));
        })) continue;
        if (!data.edges.every((edge, at) => {
            const saved = cached.edges[at]!;
            return saved[0] === edge.id && saved[1] === edge.source && saved[2] === edge.target && saved[3] === edge.type;
        })) continue;
        geometryMemo.splice(index, 1); geometryMemo.unshift(cached);
        return { data: { ...data, nodes: data.nodes.map(node => ({ ...node, ...cached.positions.get(node.id)! })) },
            groups: copyGroups(cached.groups), groupByNode: new Map(cached.groupByNode) };
    }
    return undefined;
}

function rememberGeometry(data: GraphData, result: OrganicClusterLayout, roots: ReadonlySet<number> | undefined, previous: Map<number, GraphNode>): void {
    const weight = data.nodes.length * 4 + data.edges.length;
    if (weight > MAX_GEOMETRY_WEIGHT) return;
    const entry: GeometryMemo = {
        nodes: new Map(data.nodes.map(node => {
            const prior = previous.get(node.id);
            return [node.id, { size: node.size, file: node.file_path, root: Boolean(roots?.has(node.id)),
                previous: prior ? { x: prior.x, y: prior.y, z: prior.z } : undefined }];
        })),
        edges: data.edges.map(edge => [edge.id, edge.source, edge.target, edge.type]),
        positions: new Map(result.data.nodes.map(node => [node.id, { x: node.x, y: node.y, z: node.z }])),
        hasPrevious: previous.size > 0, groups: copyGroups(result.groups), groupByNode: new Map(result.groupByNode), weight,
    };
    geometryMemo.unshift(entry);
    let retained = geometryMemo.reduce((sum, item) => sum + item.weight, 0);
    while (geometryMemo.length > MAX_GEOMETRY_ENTRIES || retained > MAX_GEOMETRY_WEIGHT) retained -= geometryMemo.pop()!.weight;
}

function hash(value: string): number {
    let result = 2166136261;
    for (let i = 0; i < value.length; i++) result = Math.imul(result ^ value.charCodeAt(i), 16777619);
    result ^= result >>> 16;
    result = Math.imul(result, 0x7feb352d);
    result ^= result >>> 15;
    return result >>> 0;
}

/** Bell-shaped seeded noise avoids shells, grids and concentric rings. */
function cloudPoint(key: string, scale: number): Point {
    const coordinate = (axis: string) => {
        let sum = 0;
        for (let i = 0; i < 6; i++) sum += hash(`${key}:${axis}:${i}`) / 0x100000000;
        return (sum - 3) * scale;
    };
    return { x: coordinate('x'), y: coordinate('y'), z: coordinate('z') };
}

const finitePoint = (point: Point) => [point.x, point.y, point.z].every(Number.isFinite);

/** Only unique neighbor pairs influence grouping. Repeated callsites and
 * reciprocal/type variants cannot inflate a group's apparent connectivity. */
function neighborPairs(data: GraphData, indices: Map<number, number>): Pair[] {
    const pairs = new Map<string, Pair>();
    for (const edge of data.edges) {
        const source = indices.get(edge.source), target = indices.get(edge.target);
        if (source === undefined || target === undefined || source === target) continue;
        const a = Math.min(source, target), b = Math.max(source, target);
        pairs.set(`${a}:${b}`, { a, b, weight: 1 });
    }
    return [...pairs.values()].sort((a, b) => a.a - b.a || a.b - b.b);
}

/** Bounded local modularity refinement. This discovers dense relationship
 * groups, not semantic components or execution stages. Direction remains in
 * the returned graph; attraction itself is deliberately symmetric. */
function communities(nodes: readonly GraphNode[], pairs: readonly Pair[]): number[] {
    const adjacent: number[][] = nodes.map(() => []);
    for (const pair of pairs) { adjacent[pair.a]!.push(pair.b); adjacent[pair.b]!.push(pair.a); }
    const degree = adjacent.map(neighbors => neighbors.length);
    const totals = [...degree], labels = nodes.map((_, i) => i);
    const order = nodes.map((node, index) => ({ index, seed: hash(`visit:${node.id}`) }))
        .sort((a, b) => a.seed - b.seed || a.index - b.index);
    const twiceEdges = Math.max(1, pairs.length * 2);
    for (let pass = 0; pass < 6; pass++) {
        let moved = 0;
        for (const { index } of order) {
            const count = degree[index]!;
            if (!count) continue;
            const current = labels[index]!;
            totals[current]! -= count;
            const links = new Map<number, number>();
            for (const neighbor of adjacent[index]!) {
                const label = labels[neighbor]!;
                links.set(label, (links.get(label) ?? 0) + 1);
            }
            const score = (label: number) => (links.get(label) ?? 0) - 1.25 * count * totals[label]! / twiceEdges;
            let chosen = current, best = score(current);
            for (const candidate of links.keys()) {
                const next = score(candidate);
                if (next > best + 1e-9 || (next > best - 1e-9 && chosen !== current && candidate < chosen)) {
                    best = next; chosen = candidate;
                }
            }
            labels[index] = chosen; totals[chosen]! += count;
            if (chosen !== current) moved++;
        }
        if (!moved) break;
    }
    return labels;
}

/** Aggregated nearby cells bound repulsion to 27 cell reads per particle.
 * There is no all-pairs force loop, including a crowded cell. */
function relax(particles: Particle[], pairs: readonly Pair[], iterations: number, cellWidth: number,
    spring: (pair: Pair) => { distance: number; strength: number }, homePull: number): void {
    if (particles.every(point => point.fixed)) return;
    const dx = new Float64Array(particles.length), dy = new Float64Array(particles.length), dz = new Float64Array(particles.length);
    const springs = pairs.map(spring), pull = new Float64Array(particles.length);
    pairs.forEach((pair, index) => { pull[pair.a]! += springs[index]!.strength; pull[pair.b]! += springs[index]!.strength; });
    // Bound total spring attraction per particle. Otherwise a dense hub's
    // dozens of springs overwhelm local separation and collapse its group.
    const springScale = pull.map(strength => .12 / Math.max(.12, strength));
    for (let iteration = 0; iteration < iterations; iteration++) {
        dx.fill(0); dy.fill(0); dz.fill(0);
        const cells = new Map<number, Map<number, Map<number, Cell>>>();
        const coordinates = particles.map(point => [Math.floor(point.x / cellWidth), Math.floor(point.y / cellWidth), Math.floor(point.z / cellWidth)] as const);
        particles.forEach((point, index) => {
            const [x, y, z] = coordinates[index]!;
            let ys = cells.get(x); if (!ys) { ys = new Map(); cells.set(x, ys); }
            let zs = ys.get(y); if (!zs) { zs = new Map(); ys.set(y, zs); }
            let cell = zs.get(z); if (!cell) { cell = { x: 0, y: 0, z: 0, radius: 0, count: 0 }; zs.set(z, cell); }
            cell.x += point.x; cell.y += point.y; cell.z += point.z; cell.radius += point.radius; cell.count++;
        });
        particles.forEach((point, index) => {
            if (point.fixed) return;
            const [cx, cy, cz] = coordinates[index]!;
            for (let x = cx - 1; x <= cx + 1; x++) {
                const ys = cells.get(x); if (!ys) continue;
                for (let y = cy - 1; y <= cy + 1; y++) {
                    const zs = ys.get(y); if (!zs) continue;
                    for (let z = cz - 1; z <= cz + 1; z++) {
                        const cell = zs.get(z); if (!cell) continue;
                        const own = x === cx && y === cy && z === cz ? 1 : 0;
                        const count = cell.count - own; if (!count) continue;
                        let vx = point.x - (cell.x - point.x * own) / count;
                        let vy = point.y - (cell.y - point.y * own) / count;
                        let vz = point.z - (cell.z - point.z * own) / count;
                        let distance = Math.hypot(vx, vy, vz);
                        if (distance < .001) { const noise = cloudPoint(`separate:${index}`, 1); vx = noise.x; vy = noise.y; vz = noise.z; distance = Math.max(.001, Math.hypot(vx, vy, vz)); }
                        const desired = point.radius + (cell.radius - point.radius * own) / count + cellWidth * .18;
                        const force = Math.max(0, desired - distance) * Math.min(4, count) * .16 / distance;
                        dx[index]! += vx * force; dy[index]! += vy * force; dz[index]! += vz * force;
                    }
                }
            }
            dx[index]! += (point.home.x - point.x) * homePull;
            dy[index]! += (point.home.y - point.y) * homePull;
            dz[index]! += (point.home.z - point.z) * homePull;
        });
        for (let index = 0; index < pairs.length; index++) {
            const pair = pairs[index]!;
            const a = particles[pair.a]!, b = particles[pair.b]!;
            const vx = b.x - a.x, vy = b.y - a.y, vz = b.z - a.z;
            const distance = Math.max(.001, Math.hypot(vx, vy, vz));
            const settings = springs[index]!, force = (distance - settings.distance) * settings.strength / distance;
            const from = force * springScale[pair.a]!, to = force * springScale[pair.b]!;
            dx[pair.a]! += vx * from; dy[pair.a]! += vy * from; dz[pair.a]! += vz * from;
            dx[pair.b]! -= vx * to; dy[pair.b]! -= vy * to; dz[pair.b]! -= vz * to;
        }
        const cooling = 1 - iteration / iterations * .55;
        particles.forEach((point, index) => {
            if (point.fixed) return;
            const step = Math.min(1, cellWidth * .22 / Math.max(.001, Math.hypot(dx[index]!, dy[index]!, dz[index]!))) * cooling;
            point.x += dx[index]! * step; point.y += dy[index]! * step; point.z += dz[index]! * step;
        });
    }
}

function sharedDirectory(nodes: readonly GraphNode[]): string {
    const paths = nodes.flatMap(node => node.file_path ? [node.file_path.replace(/\\/g, '/').split('/').slice(0, -1)] : []);
    if (!paths.length) return 'Relationship group';
    const common = [...paths[0]!];
    for (const path of paths.slice(1)) {
        let length = 0;
        while (length < common.length && common[length] === path[length]) length++;
        common.length = length;
    }
    return common.join('/') || 'Relationship group';
}

/** Pure deterministic multilevel cloud layout. Fixed iteration budgets keep
 * work O(N + E) per pass; metadata/source paths only label the graph-derived
 * groups. Existing positions can be pinned for stable incremental traces. */
export function layoutOrganicClusters(data: GraphData, options: OrganicClusterOptions = {}): OrganicClusterLayout {
    if (!data.nodes.length) return { data, groups: [], groupByNode: new Map() };
    const previous = new Map((options.previous?.nodes ?? []).filter(finitePoint).map(node => [node.id, node]));
    const cached = cachedGeometry(data, options.rootIds, previous);
    if (cached) return cached;
    const nodes = [...data.nodes].sort((a, b) => a.id - b.id);
    const indices = new Map(nodes.map((node, index) => [node.id, index]));
    const pairs = neighborPairs(data, indices), labels = communities(nodes, pairs);
    const members = new Map<number, number[]>();
    labels.forEach((label, index) => { const group = members.get(label) ?? []; group.push(index); members.set(label, group); });
    const orderedGroups = [...members.values()].sort((a, b) => nodes[a[0]!]!.id - nodes[b[0]!]!.id);
    const groupIndex = new Array<number>(nodes.length);
    orderedGroups.forEach((group, index) => group.forEach(member => { groupIndex[member] = index; }));
    const anchors: Particle[] = orderedGroups.map(group => {
        const old = group.flatMap(index => previous.has(nodes[index]!.id) ? [previous.get(nodes[index]!.id)!] : []);
        const center = old.length ? { x: old.reduce((sum, p) => sum + p.x, 0) / old.length,
            y: old.reduce((sum, p) => sum + p.y, 0) / old.length, z: old.reduce((sum, p) => sum + p.z, 0) / old.length }
            : cloudPoint(`group:${nodes[group[0]!]!.id}`, 105 * Math.cbrt(orderedGroups.length));
        return { ...center, home: { ...center }, radius: 20 + 16 * Math.cbrt(group.length), fixed: old.length > 0 };
    });
    const between = new Map<string, Pair>();
    for (const pair of pairs) {
        const a = Math.min(groupIndex[pair.a]!, groupIndex[pair.b]!), b = Math.max(groupIndex[pair.a]!, groupIndex[pair.b]!);
        if (a === b) continue;
        const key = `${a}:${b}`, prior = between.get(key);
        if (prior) prior.weight++; else between.set(key, { a, b, weight: 1 });
    }
    const groupPairs = [...between.values()].sort((a, b) => a.a - b.a || a.b - b.b);
    const work = nodes.length + pairs.length;
    const passes = Math.max(5, Math.min(16, Math.floor(160000 / Math.max(1, work))));
    relax(anchors, groupPairs, passes, 190, pair => ({
        distance: anchors[pair.a]!.radius + anchors[pair.b]!.radius + 90,
        strength: .028 * Math.min(3, 1 + Math.log2(pair.weight)),
    }), .006);
    const adjacent: number[][] = nodes.map(() => []);
    for (const pair of pairs) { adjacent[pair.a]!.push(pair.b); adjacent[pair.b]!.push(pair.a); }
    const rootCount = nodes.filter(node => options.rootIds?.has(node.id)).length;
    const particles: Particle[] = nodes.map((node, index) => {
        const old = previous.get(node.id), anchor = anchors[groupIndex[index]!]!;
        const radius = 8 + 1.5 * Math.sqrt(Math.max(0, Number.isFinite(node.size) ? node.size : 1));
        if (old) return { x: old.x, y: old.y, z: old.z, radius, fixed: true, home: { x: old.x, y: old.y, z: old.z } };
        // Pinned positions cannot be shifted, so a root they do not hold is
        // placed at the origin directly instead of re-centering afterwards.
        if (previous.size && options.rootIds?.has(node.id)) {
            const offset = rootCount > 1 ? cloudPoint(`root:${node.id}`, 20) : { x: 0, y: 0, z: 0 };
            return { ...offset, radius, fixed: rootCount === 1, home: { x: 0, y: 0, z: 0 } };
        }
        const neighboringOld = adjacent[index]!.flatMap(neighbor => previous.has(nodes[neighbor]!.id) ? [previous.get(nodes[neighbor]!.id)!] : []);
        const home = neighboringOld.length ? { x: neighboringOld.reduce((sum, p) => sum + p.x, 0) / neighboringOld.length,
            y: neighboringOld.reduce((sum, p) => sum + p.y, 0) / neighboringOld.length, z: neighboringOld.reduce((sum, p) => sum + p.z, 0) / neighboringOld.length }
            : { x: anchor.x, y: anchor.y, z: anchor.z };
        const offset = cloudPoint(`node:${node.id}`, neighboringOld.length ? 50 : anchor.radius);
        return { x: home.x + offset.x, y: home.y + offset.y, z: home.z + offset.z,
            radius, fixed: false, home };
    });
    relax(particles, pairs, passes, 56, pair => ({
        distance: 42 + hash(`length:${nodes[pair.a]!.id}:${nodes[pair.b]!.id}`) / 0x100000000 * 26,
        strength: groupIndex[pair.a] === groupIndex[pair.b] ? .034 : .0015,
    }), .018);
    // Roots sit at the origin: a fresh layout is re-centered on them, and an
    // expansion keeps the earlier, already centered positions.
    const roots = nodes.flatMap((node, index) => options.rootIds?.has(node.id) ? [particles[index]!] : []);
    const origin = previous.size || !roots.length ? { x: 0, y: 0, z: 0 }
        : { x: roots.reduce((sum, p) => sum + p.x, 0) / roots.length, y: roots.reduce((sum, p) => sum + p.y, 0) / roots.length,
            z: roots.reduce((sum, p) => sum + p.z, 0) / roots.length };
    const positioned = new Map(nodes.map((node, index) => {
        const point = particles[index]!;
        return [node.id, { ...node, x: point.x - origin.x, y: point.y - origin.y, z: point.z - origin.z }] as const;
    }));
    const groupByNode = new Map<number, string>();
    const groups = orderedGroups.map(group => {
        const id = `relationships:${nodes[group[0]!]!.id}`;
        const groupNodes = group.map(index => positioned.get(nodes[index]!.id)!);
        groupNodes.forEach(node => groupByNode.set(node.id, id));
        return { id, nodeIds: groupNodes.map(node => node.id), label: sharedDirectory(groupNodes),
            center: { x: groupNodes.reduce((sum, p) => sum + p.x, 0) / group.length,
                y: groupNodes.reduce((sum, p) => sum + p.y, 0) / group.length, z: groupNodes.reduce((sum, p) => sum + p.z, 0) / group.length },
            rootCount: groupNodes.filter(node => options.rootIds?.has(node.id)).length, internalEdges: 0, boundaryEdges: 0 };
    });
    const byId = new Map(groups.map(group => [group.id, group]));
    for (const edge of data.edges) {
        const a = groupByNode.get(edge.source), b = groupByNode.get(edge.target);
        if (!a || !b) continue;
        if (a === b) byId.get(a)!.internalEdges++;
        else { byId.get(a)!.boundaryEdges++; byId.get(b)!.boundaryEdges++; }
    }
    const result = { data: { ...data, nodes: data.nodes.map(node => positioned.get(node.id)!), edges: data.edges }, groups, groupByNode };
    rememberGeometry(data, result, options.rootIds, previous);
    return result;
}
