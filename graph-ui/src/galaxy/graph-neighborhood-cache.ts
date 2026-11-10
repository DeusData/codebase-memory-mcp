import type { GraphEdge, GraphNode } from './types';

export type NeighborhoodDirection = 'inbound' | 'outbound';
interface Completeness { all: boolean; types: Set<string> }
interface Neighborhood { nodes: GraphNode[]; edges: GraphEdge[] }
interface CacheLimits { nodes: number; edges: number; roots: number; proofs: number }
const DEFAULT_LIMITS: CacheLimits = { nodes: 25_000, edges: 100_000, roots: 256, proofs: 50_000 };
const proofKey = (id: number, direction: NeighborhoodDirection) => `${direction}:${id}`;

class Evidence {
    readonly nodes = new Map<number, GraphNode>();
    readonly edges = new Map<number, GraphEdge>();
    readonly roots = new Map<string, number[]>();
    readonly proofs = new Map<string, Completeness>();
    readonly inbound = new Map<number, Set<number>>();
    readonly outbound = new Map<number, Set<number>>();

    addNodes(nodes: readonly GraphNode[]) { for (const node of nodes) this.nodes.set(node.id, node); }
    addEdges(edges: readonly GraphEdge[]) {
        for (const edge of edges) {
            if (edge.id === undefined) continue;
            this.edges.set(edge.id, edge);
            for (const [index, node] of [[this.outbound, edge.source], [this.inbound, edge.target]] as const) {
                const ids = index.get(node) ?? new Set<number>(); ids.add(edge.id); index.set(node, ids);
            }
        }
    }
    mark(ids: readonly number[], direction: NeighborhoodDirection, types?: readonly string[]) {
        for (const id of ids) {
            const key = proofKey(id, direction), proof = this.proofs.get(key) ?? { all: false, types: new Set<string>() };
            if (types === undefined) { proof.all = true; proof.types.clear(); }
            else if (!proof.all) for (const type of types) proof.types.add(type);
            this.proofs.set(key, proof);
        }
    }
    merge(other: Evidence) {
        this.addNodes([...other.nodes.values()]); this.addEdges([...other.edges.values()]);
        for (const [key, ids] of other.roots) this.roots.set(key, ids);
        for (const [key, incoming] of other.proofs) {
            const proof = this.proofs.get(key);
            if (!proof || incoming.all) this.proofs.set(key, { all: incoming.all, types: new Set(incoming.types) });
            else if (!proof.all) for (const type of incoming.types) proof.types.add(type);
        }
    }
}

/** Only fully consumed query pages enter a transaction. Its owner publishes
 * after checking the same index generation again; abandoned work proves nothing. */
export class GraphNeighborhoodTransaction {
    private readonly staged = new Evidence();
    private committed = false;
    constructor(private readonly owner: GraphNeighborhoodCache) {}
    private get base() { return this.owner.evidence; }

    node(id: number): GraphNode | undefined { return this.staged.nodes.get(id) ?? this.base.nodes.get(id); }
    roots(predicate: string): GraphNode[] | undefined {
        const ids = this.staged.roots.get(predicate) ?? this.base.roots.get(predicate);
        if (!ids) return undefined;
        const nodes = ids.map(id => this.node(id));
        return nodes.every((node): node is GraphNode => node !== undefined) ? nodes : undefined;
    }
    neighborhood(id: number, direction: NeighborhoodDirection, types?: readonly string[]): Neighborhood | undefined {
        const key = proofKey(id, direction), local = this.staged.proofs.get(key), base = this.base.proofs.get(key);
        const complete = local?.all || base?.all || (types !== undefined && types.every(type => local?.types.has(type) || base?.types.has(type)));
        if (!complete) return undefined;
        const allowed = types === undefined ? undefined : new Set(types);
        const ids = new Set([...(this.staged[direction].get(id) ?? []), ...(this.base[direction].get(id) ?? [])]);
        const edges = [...ids].map(id => this.staged.edges.get(id) ?? this.base.edges.get(id))
            .filter((edge): edge is GraphEdge => edge !== undefined && (!allowed || allowed.has(edge.type)));
        const nodeIds = new Set(edges.flatMap(edge => [edge.source, edge.target])); nodeIds.add(id);
        const nodes = [...nodeIds].map(id => this.node(id));
        return nodes.every((node): node is GraphNode => node !== undefined) ? { nodes, edges } : undefined;
    }
    rememberRoots(predicate: string, nodes: readonly GraphNode[]) {
        this.staged.addNodes(nodes); this.staged.roots.set(predicate, nodes.map(node => node.id));
    }
    rememberNeighborhood(ids: readonly number[], direction: NeighborhoodDirection, types: readonly string[] | undefined,
        nodes: readonly GraphNode[], edges: readonly GraphEdge[]) {
        this.staged.addNodes(nodes); this.staged.addEdges(edges); this.staged.mark(ids, direction, types);
    }
    commit() {
        if (this.committed) return;
        this.committed = true; this.owner.publish(this.staged);
    }
}

/** A bounded publication-local adjacency index. The global layout is never
 * inserted: its rendering cap cannot prove neighborhood completeness. */
export class GraphNeighborhoodCache {
    evidence = new Evidence();
    private active = true;
    private readonly limits: CacheLimits;
    constructor(readonly project: string, readonly generation: string, limits: Partial<CacheLimits> = {}) {
        this.limits = { ...DEFAULT_LIMITS, ...limits };
    }
    begin() { return new GraphNeighborhoodTransaction(this); }
    invalidate() { this.active = false; this.evidence = new Evidence(); }
    get valid() { return this.active; }
    get size() { return { nodes: this.evidence.nodes.size, edges: this.evidence.edges.size, roots: this.evidence.roots.size, proofs: this.evidence.proofs.size }; }
    publish(staged: Evidence) {
        if (!this.active) return;
        if (!staged.nodes.size && !staged.edges.size && !staged.roots.size && !staged.proofs.size) return;
        const fits = (left: Evidence, right?: Evidence) => (Object.keys(this.limits) as (keyof CacheLimits)[]).every(key => {
            const ids = new Set<unknown>(left[key].keys()); if (right) for (const id of right[key].keys()) ids.add(id);
            return ids.size <= this.limits[key];
        });
        if (!fits(staged)) return; // A huge one-off scope is rendered, not retained indefinitely.
        if (!fits(this.evidence, staged)) this.evidence = new Evidence();
        this.evidence.merge(staged);
    }
}

/** LRU projects, one publication per project. Revocation also prevents late
 * transactions from repopulating a previous publication after a reindex. */
export class GraphNeighborhoodCachePool {
    private readonly projects = new Map<string, GraphNeighborhoodCache>();
    constructor(private readonly maxProjects = 3, private readonly limits: Partial<CacheLimits> = {}) {}
    get(project: string, generation: string) {
        let cache = this.projects.get(project);
        if (!cache || cache.generation !== generation || !cache.valid) { cache?.invalidate(); cache = new GraphNeighborhoodCache(project, generation, this.limits); }
        this.projects.delete(project); this.projects.set(project, cache);
        while (this.projects.size > this.maxProjects) {
            const oldest = this.projects.keys().next().value!; this.projects.get(oldest)!.invalidate(); this.projects.delete(oldest);
        }
        return cache;
    }
    peek(project: string) { return this.projects.get(project); }
    get size() { return this.projects.size; }
}

let pools = new WeakMap<typeof globalThis.fetch, GraphNeighborhoodCachePool>();
const transportIds = new WeakMap<typeof globalThis.fetch, number>();
let nextTransportId = 0;
export function graphNeighborhoodTransportKey(transport = globalThis.fetch) {
    let id = transportIds.get(transport);
    if (id === undefined) { id = ++nextTransportId; transportIds.set(transport, id); }
    return id;
}
export function graphNeighborhoodCache(project: string, generation: string, transport = globalThis.fetch) {
    let pool = pools.get(transport);
    if (!pool) { pool = new GraphNeighborhoodCachePool(); pools.set(transport, pool); }
    return pool.get(project, generation);
}
export function peekGraphNeighborhoodCache(project: string, transport = globalThis.fetch) { return pools.get(transport)?.peek(project); }
export function clearGraphNeighborhoodCaches() { pools = new WeakMap(); }
