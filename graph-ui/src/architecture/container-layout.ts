import type { ContainerTopology } from './container-topology';
import type { SemanticGraph } from './semantic-graph';

/** Condense cycles before layering. Direction describes callers, not data movement. */
export function layoutContainers(topology: ContainerTopology): { graph: SemanticGraph; cycles: string[][] } {
    const services = [...topology.services].sort((a, b) => a.id.localeCompare(b.id));
    const ids = new Set(services.map(service => service.id));
    const adjacent = new Map(services.map(service => [service.id, new Set<string>()]));
    for (const edge of topology.connections) if (edge.kind !== 'startup' && ids.has(edge.source) && ids.has(edge.target)) adjacent.get(edge.source)!.add(edge.target);
    const numbers = new Map<string, number>(), low = new Map<string, number>();
    const stack: string[] = [], active = new Set<string>(), groups: string[][] = [];
    const visit = (id: string) => {
        numbers.set(id, numbers.size); low.set(id, numbers.get(id)!); stack.push(id); active.add(id);
        for (const target of [...adjacent.get(id)!].sort()) {
            if (!numbers.has(target)) { visit(target); low.set(id, Math.min(low.get(id)!, low.get(target)!)); }
            else if (active.has(target)) low.set(id, Math.min(low.get(id)!, numbers.get(target)!));
        }
        if (low.get(id) === numbers.get(id)) {
            const group: string[] = []; let next: string;
            do { next = stack.pop()!; active.delete(next); group.push(next); } while (next !== id);
            groups.push(group.sort());
        }
    };
    services.forEach(service => { if (!numbers.has(service.id)) visit(service.id); });
    const membership = new Map(groups.flatMap((group, index) => group.map(id => [id, index] as const)));
    const outgoing = groups.map(() => new Set<number>()), incoming = groups.map(() => 0), layers = groups.map(() => 0);
    for (const [source, targets] of adjacent) for (const target of targets) {
        const a = membership.get(source)!, b = membership.get(target)!;
        if (a !== b && !outgoing[a].has(b)) { outgoing[a].add(b); incoming[b]++; }
    }
    const queue = incoming.flatMap((count, index) => count ? [] : [index]);
    for (let index = 0; index < queue.length; index++) for (const next of outgoing[queue[index]]) {
        layers[next] = Math.max(layers[next], layers[queue[index]] + 1);
        if (--incoming[next] === 0) queue.push(next);
    }
    const rows = new Map<number, typeof services>();
    services.forEach(service => { const layer = layers[membership.get(service.id)!]; rows.set(layer, [...(rows.get(layer) ?? []), service]); });
    const positions = new Map<string, [number, number, number]>();
    let offsetX = 0;
    for (const [, row] of [...rows].sort(([a], [b]) => a - b)) {
        const columns = Math.ceil(Math.sqrt(row.length)), rowCount = Math.ceil(row.length / columns);
        // Pack peers in two dimensions, reserving the whole region before the next
        // layer so every dependency between condensed cycles still points right.
        row.forEach((service, index) => positions.set(service.id, [offsetX + (index % columns) * 28, 0,
            (Math.floor(index / columns) - (rowCount - 1) / 2) * 28]));
        offsetX += (columns - 1) * 28 + 34;
    }
    const graph: SemanticGraph = {
        view: 'routes', scopeKey: services.map(service => service.id).join('|'), title: 'Container service map',
        positionMeaning: 'Squares represent declared services. Dependency layers advance left to right; peers and cycles share compact regions.',
        nodes: services.map(service => ({ id: service.id, kind: 'area', label: service.name,
            detail: `${service.project} · ${service.image ?? 'built from source'}`, position: positions.get(service.id)!,
            count: service.sourcePaths.length, members: [], footprint: [18, 18],
            tint: service.sourcePaths.length ? '#85b4c5' : '#b6a087', kindLabel: 'Declared service' })),
        edges: [...topology.connections].sort((a, b) => a.id.localeCompare(b.id)).map(edge => ({ id: edge.id, source: edge.source, target: edge.target,
            type: edge.kind === 'startup' ? 'STARTUP_DEPENDENCY' : edge.kind === 'configuration' ? 'CONFIGURES'
                : /redis|postgres|mysql|database|sql/i.test(edge.protocol) ? 'SERVICE_CALLS'
                    : /grpc/i.test(edge.protocol) ? 'GRPC_CALLS' : /http/i.test(edge.protocol) ? 'HTTP_CALLS' : 'SERVICE_CALLS',
            count: edge.evidence.length, evidence: [] })),
        totalNodes: services.length, totalEdges: topology.connections.length, omittedNodes: 0, omittedEdges: 0, warnings: [],
    };
    return { graph, cycles: groups.filter(group => group.length > 1 || adjacent.get(group[0])!.has(group[0])) };
}
