import { describe, expect, it } from 'vitest';
import { layoutContainers } from './container-layout';
import type { ContainerTopology } from './container-topology';
import type { SemanticGraph } from './semantic-graph';
const topology = (): ContainerTopology => ({ services: ['frontend', 'worker', 'db', 'isolated'].map(id => ({
    id, name: id, project: 'p', manifest: 'compose.yml', line: 1, sourcePaths: [], networks: [], ports: [],
})), connections: [
    { id: '1', source: 'frontend', target: 'worker', kind: 'call', protocol: 'http', evidence: [] },
    { id: '2', source: 'worker', target: 'db', kind: 'call', protocol: 'postgres', evidence: [] },
], warnings: [], unresolved: [] });
const serviceMap = (count: number): ContainerTopology => ({ ...topology(), connections: [],
    services: Array.from({ length: count }, (_, index) => {
        const id = `service-${String(index).padStart(3, '0')}`;
        return { ...topology().services[0], id, name: id };
    }),
});
const connect = (source: string, target: string): ContainerTopology['connections'][number] => ({
    id: `${source}->${target}`, source, target, kind: 'call', protocol: 'http', evidence: [],
});
function expectSeparatedSquares(graph: SemanticGraph) {
    for (const [index, node] of graph.nodes.entries()) {
        expect(node.position.every(Number.isFinite)).toBe(true);
        expect(node.footprint).toEqual([18, 18]);
        for (const other of graph.nodes.slice(index + 1)) {
            const apartX = Math.abs(node.position[0] - other.position[0]);
            const apartZ = Math.abs(node.position[2] - other.position[2]);
            expect(apartX >= 18 || apartZ >= 18, `${node.id} overlaps ${other.id}`).toBe(true);
        }
    }
}
function expectCompact(graph: SemanticGraph, maxExtent: number) {
    for (const axis of [0, 2]) {
        const coordinates = graph.nodes.map(node => node.position[axis]);
        expect(Math.max(...coordinates) - Math.min(...coordinates) + 18).toBeLessThanOrEqual(maxExtent);
        expect(new Set(coordinates).size).toBeGreaterThan(1);
    }
}
describe('service map layout', () => {
    it('retains isolated services and layers callers before their dependencies', () => {
        const result = layoutContainers(topology()), nodes = new Map(result.graph.nodes.map(node => [node.id, node]));
        expect(nodes.size).toBe(4); expect(nodes.get('frontend')!.position[0]).toBeLessThan(nodes.get('worker')!.position[0]);
        expect(nodes.get('worker')!.position[0]).toBeLessThan(nodes.get('db')!.position[0]);
        expect(nodes.get('isolated')!.footprint).toEqual([18, 18]);
    });
    it('condenses dependency cycles and remains deterministic across input order', () => {
        const input = topology(); input.connections.push({ id: '3', source: 'db', target: 'worker', kind: 'call', protocol: 'http', evidence: [] });
        const result = layoutContainers(input);
        expect(result.cycles).toEqual([['db', 'worker']]);
        expect(result.graph.nodes.every(node => node.position.every(Number.isFinite))).toBe(true);
        expect(layoutContainers({ ...input, services: [...input.services].reverse(), connections: [...input.connections].reverse() }).graph).toEqual(result.graph);
    });
    it('does not treat startup order as a communication layer', () => {
        const input = topology(); input.connections = [{ id: 'start', source: 'isolated', target: 'db', kind: 'startup', protocol: 'depends_on', evidence: [] }];
        expect(layoutContainers(input).graph.nodes).toEqual(layoutContainers({ ...input, connections: [] }).graph.nodes);
    });
    it.each([68, 128])('packs %i unconnected services into a compact map without inventing edges', count => {
        const input = serviceMap(count), result = layoutContainers(input);
        expect(result.graph.nodes).toHaveLength(count);
        expect(result.graph.edges).toEqual([]);
        expectCompact(result.graph, 350);
        expectSeparatedSquares(result.graph);
        expect(layoutContainers({ ...input, services: [...input.services].reverse() })).toEqual(result);
    });
    it('keeps a sparse 68-service map compact while preserving connected dependency order', () => {
        const input = serviceMap(68);
        input.connections = [connect('service-000', 'service-001'), connect('service-001', 'service-002')];
        const result = layoutContainers(input), nodes = new Map(result.graph.nodes.map(node => [node.id, node]));
        expectCompact(result.graph, 350);
        expectSeparatedSquares(result.graph);
        for (const edge of input.connections) expect(nodes.get(edge.source)!.position[0]).toBeLessThan(nodes.get(edge.target)!.position[0]);
        expect(result.graph.edges).toHaveLength(2);
    });
    it('packs a wide dependency layer after its callers and before its shared dependency', () => {
        const input = serviceMap(68), peers = input.services.slice(1, -1);
        input.connections = peers.flatMap(service => [connect('service-000', service.id), connect(service.id, 'service-067')]);
        const result = layoutContainers(input), nodes = new Map(result.graph.nodes.map(node => [node.id, node]));
        expectCompact(result.graph, 350);
        expectSeparatedSquares(result.graph);
        for (const service of peers) {
            expect(nodes.get('service-000')!.position[0]).toBeLessThan(nodes.get(service.id)!.position[0]);
            expect(nodes.get(service.id)!.position[0]).toBeLessThan(nodes.get('service-067')!.position[0]);
        }
        expect(layoutContainers({ ...input, services: [...input.services].reverse(), connections: [...input.connections].reverse() })).toEqual(result);
    });
    it('keeps cycle membership and layout stable across traversal order in a large graph', () => {
        const input = serviceMap(68);
        input.connections = [connect('service-000', 'service-001'), connect('service-000', 'service-003'),
            connect('service-001', 'service-002'), connect('service-002', 'service-001'),
            connect('service-003', 'service-004'), connect('service-004', 'service-003'),
            connect('service-002', 'service-005'), connect('service-004', 'service-005')];
        const result = layoutContainers(input), nodes = new Map(result.graph.nodes.map(node => [node.id, node]));
        expect(result.cycles).toEqual([['service-001', 'service-002'], ['service-003', 'service-004']]);
        for (const id of ['service-001', 'service-002', 'service-003', 'service-004']) {
            expect(nodes.get('service-000')!.position[0]).toBeLessThan(nodes.get(id)!.position[0]);
            expect(nodes.get(id)!.position[0]).toBeLessThan(nodes.get('service-005')!.position[0]);
        }
        expectSeparatedSquares(result.graph);
        expectCompact(result.graph, 400);
        expect(layoutContainers({ ...input, services: [...input.services].reverse(), connections: [...input.connections].reverse() })).toEqual(result);
    });
});
