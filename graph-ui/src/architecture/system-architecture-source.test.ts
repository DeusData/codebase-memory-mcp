import { beforeEach, describe, expect, it, vi } from 'vitest';
import { callToolJson } from '../provider/rpc-transport';
import { loadSystemArchitecture, readSystemArchitecture } from './system-architecture-source';
import type { SystemArchitectureResponse, SystemPathEdge, SystemSymbol } from './system-architecture-source';

vi.mock('../provider/rpc-transport', () => ({ callToolJson: vi.fn() }));

function response(): SystemArchitectureResponse {
    const caller: SystemSymbol = { id: 1, name: 'start', qualified_name: 'app.start', label: 'Function', component_id: 'api', file_path: 'src/api.ts' };
    const callee: SystemSymbol = { id: 2, name: 'save', qualified_name: 'app.save', label: 'Function', component_id: 'store', file_path: 'src/store.ts',
        signature: 'save(value: Input): Result', return_type: 'Result', parameters: { names: ['value'], types: ['Input'], count: 1 } };
    const edge: SystemPathEdge = { id: 10, source_id: 1, target_id: 2, type: 'CALLS',
        arguments: [{ i: 7, e: 'input["key"]', v: 'literal\nvalue' }], argument_limit: 8, arguments_complete: false,
        callsite: { file_path: 'src/api.ts', line: 42 } };
    return { status: 'ready', generation: 'g1', result: { schema_version: 1, status: 'ready', kind: 'static_projection', complete: true,
        components: [], dependencies: [{ source: 'api', target: 'store', type: 'CALLS', count: 1,
            witnesses: [{ ...edge, edge_id: edge.id, source: caller, target: callee }] }], cycles: [], entrypoints: [caller],
        paths: [{ entrypoint_id: 1, nodes: [caller, callee], edges: [edge] }], totals: {}, limits: {}, warnings: [],
        behavior: { mode: 'corridor', source_id: 1, target_id: 2, complete: true, limits_hit: [], reachable_targets: [], totals: {},
            nodes: [caller, callee], edges: [edge], cycles: [], max_hops: 10 },
    } };
}

beforeEach(() => { vi.clearAllMocks(); vi.mocked(callToolJson).mockResolvedValue(response()); });

describe('architecture evidence request opt-in', () => {
    it.each([undefined, false])('omits the wire flag when includeBehaviorEvidence is %s', async includeBehaviorEvidence => {
        const controller = new AbortController();
        await loadSystemArchitecture({ project: 'sample', includeBehaviorEvidence }, controller.signal);
        expect(callToolJson).toHaveBeenCalledWith('get_architecture', {
            project: 'sample', aspects: ['system_structure'], format: 'json',
        }, { signal: controller.signal });
    });

    it('forwards explicit evidence, exact endpoints, generation and cancellation together', async () => {
        const controller = new AbortController();
        await loadSystemArchitecture({ project: 'sample', entryNodeId: 1, targetNodeId: 2,
            expectedGeneration: 'g1', includeBehaviorEvidence: true }, controller.signal);
        expect(callToolJson).toHaveBeenCalledWith('get_architecture', {
            project: 'sample', aspects: ['system_structure'], format: 'json', entry_node_id: 1, target_node_id: 2,
            expected_generation: 'g1', include_behavior_evidence: true,
        }, { signal: controller.signal });
    });
});

describe('optional indexed behavior evidence at the wire boundary', () => {
    it('preserves declarations and argument expressions without changing stored indices or implying completeness', () => {
        const data = readSystemArchitecture(response()).result!;
        expect(data.paths[0].nodes[1].parameters).toEqual({ names: ['value'], types: ['Input'], count: 1 });
        expect(data.paths[0].nodes[1].return_type).toBe('Result');
        expect(data.paths[0].edges[0].arguments).toEqual([{ i: 7, e: 'input["key"]', v: 'literal\nvalue' }]);
        expect(data.behavior!.edges[0].arguments_complete).toBe(false);
        expect(data.dependencies[0].witnesses[0].argument_limit).toBe(8);
        expect(data.dependencies[0].witnesses[0].target.signature).toBe('save(value: Input): Result');
    });

    it.each([
        ['signature', 42], ['return_type', false],
        ['parameters', { names: ['value'], types: [42], count: 1 }],
        ['parameters', { names: 'value', types: ['Input'], count: 1 }],
        ['parameters', { names: ['value'], types: ['Input'], count: -1 }],
    ])('rejects malformed declaration field %s', (field, value) => {
        const data = response();
        (data.result!.paths[0].nodes[1] as unknown as Record<string, unknown>)[field as string] = value;
        expect(() => readSystemArchitecture(data)).toThrow('invalid shape');
    });

    it.each([
        { i: -1, e: 'value' }, { i: 1.5, e: 'value' }, { i: 0, e: 42 }, { i: 0, e: 'value', v: false },
    ])('rejects malformed argument records in corridor edges', argument => {
        const data = response();
        (data.result!.behavior!.edges[0] as unknown as Record<string, unknown>).arguments = [argument];
        expect(() => readSystemArchitecture(data)).toThrow('invalid shape');
    });

    it('validates optional metadata on dependency witnesses as well as paths', () => {
        const data = response();
        (data.result!.dependencies[0].witnesses[0] as unknown as Record<string, unknown>).arguments_complete = 'yes';
        expect(() => readSystemArchitecture(data)).toThrow('invalid shape');
    });
});
