import type { TraceDirection } from './graph-scope';
import type { GraphEdge } from './types';

/** One hop of a path, in path order: `from` is one hop closer to the root.
 * The edge keeps its indexed direction, so `edge.source === from` tells
 * whether the hop follows the relationship or walks it backwards. */
export interface ScopePathStep { edge: GraphEdge; from: number; to: number }

/** The traversal key for deterministic ties: neighbor first, then edge identity. */
const order = (a: { to: number; edge: GraphEdge; index: number }, b: { to: number; edge: GraphEdge; index: number }) =>
    a.to - b.to || (a.edge.id ?? Number.MAX_SAFE_INTEGER) - (b.edge.id ?? Number.MAX_SAFE_INTEGER)
    || a.edge.type.localeCompare(b.edge.type) || a.index - b.index;

/** Shortest path from any root to `target` over the loaded scope edges only.
 * `outbound` follows edges, `inbound` walks them backwards (who reaches the
 * root), `both` ignores direction. Undefined means no path exists inside the
 * loaded scope; a root target has the empty path. Hops are counted, nothing
 * else: there is no runtime or architectural distance in this number. */
export function shortestScopePath(edges: readonly GraphEdge[], roots: ReadonlySet<number>, target: number,
    direction: TraceDirection): ScopePathStep[] | undefined {
    if (roots.has(target)) return [];
    const next = new Map<number, { to: number; edge: GraphEdge; index: number }[]>();
    const link = (from: number, to: number, edge: GraphEdge, index: number) => {
        const list = next.get(from) ?? []; list.push({ to, edge, index }); next.set(from, list);
    };
    edges.forEach((edge, index) => {
        if (edge.source === edge.target) return;
        if (direction !== 'inbound') link(edge.source, edge.target, edge, index);
        if (direction !== 'outbound') link(edge.target, edge.source, edge, index);
    });
    next.forEach(list => list.sort(order));
    const reached = new Map<number, { from: number; edge: GraphEdge } | undefined>([...roots].sort((a, b) => a - b).map(root => [root, undefined]));
    const queue = [...reached.keys()];
    for (let at = 0; at < queue.length && !reached.has(target); at++) {
        const from = queue[at]!;
        for (const hop of next.get(from) ?? []) {
            if (reached.has(hop.to)) continue;
            reached.set(hop.to, { from, edge: hop.edge });
            queue.push(hop.to);
        }
    }
    if (!reached.has(target)) return undefined;
    const steps: ScopePathStep[] = [];
    let node = target, parent = reached.get(node);
    while (parent) {
        steps.push({ edge: parent.edge, from: parent.from, to: node });
        node = parent.from; parent = reached.get(node);
    }
    return steps.reverse();
}

/** The root's outgoing CALLS in source order: by call-site line, calls without
 * a recorded line last, then by callee. Every call site is its own step. */
export function callOrder(edges: readonly GraphEdge[], root: number): ScopePathStep[] {
    return edges.map((edge, index) => ({ edge, index }))
        .filter(({ edge }) => edge.type === 'CALLS' && edge.source === root && edge.target !== root)
        .sort((a, b) => (a.edge.line ?? Number.MAX_SAFE_INTEGER) - (b.edge.line ?? Number.MAX_SAFE_INTEGER)
            || a.edge.target - b.edge.target || a.index - b.index)
        .map(({ edge }) => ({ edge, from: root, to: edge.target }));
}

/** Every node a path touches, roots included. */
export function pathNodes(steps: readonly ScopePathStep[], roots: ReadonlySet<number>): Set<number> {
    const ids = new Set<number>(steps.length ? [steps[0]!.from] : roots);
    for (const step of steps) { ids.add(step.from); ids.add(step.to); }
    return ids;
}
