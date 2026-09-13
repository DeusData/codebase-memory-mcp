import { callToolJson } from '../provider/rpc-transport';
import type { CallToolOptions } from '../provider/rpc-transport';

export interface SystemSymbol {
    id: number; name: string; qualified_name: string; label: string;
    file_path?: string; start_line?: number; end_line?: number; component_id: string; group_id?: string;
    signature?: string; return_type?: string;
    parameters?: { names: string[]; types: string[]; count: number };
}
export interface SystemComponent {
    id: string; label: string; basis: string; member_count: number; file_count: number;
    role?: 'test' | 'non_test'; role_basis?: string; group_id?: string;
    representatives: SystemSymbol[];
}
export interface SystemCallEvidence {
    callsite?: { file_path: string; line: number };
    resolution?: { strategy?: string; confidence?: number; candidates?: number };
    arguments?: { i: number; e: string; v?: string }[];
    argument_limit?: number; arguments_complete?: boolean;
}
export interface SystemWitness extends SystemCallEvidence { edge_id: number; source: SystemSymbol; target: SystemSymbol }
export interface SystemDependency {
    source: string; target: string; type: string; count: number; witnesses: SystemWitness[];
}
export interface SystemPathEdge extends SystemCallEvidence { id: number; source_id: number; target_id: number; type: string }
export interface SystemPath {
    entrypoint_id: number; nodes: SystemSymbol[];
    edges: SystemPathEdge[];
}
export interface SystemOverviewGroup {
    id: string; label: string; role?: 'test' | 'non_test'; basis?: string;
    component_count: number; member_count: number; file_count: number;
    component_ids: string[]; representatives: SystemSymbol[];
}
export interface SystemOverview {
    complete: boolean; grouping_basis: string; groups: SystemOverviewGroup[];
    components: SystemComponent[]; connections: SystemDependency[];
    totals: Record<string, number>; limits: Record<string, number | boolean>;
}
export interface SystemBehavior {
    mode: 'targets' | 'corridor'; source_id: number; target_id?: number; complete: boolean;
    corridor_complete?: boolean; reachable?: boolean;
    limits_hit: string[]; reachable_targets: (SystemSymbol & { distance: number })[];
    totals: Record<string, number>; nodes: SystemSymbol[]; edges: SystemPathEdge[];
    cycles: { node_ids: number[] }[]; max_hops: number;
}
export interface SystemProjection {
    schema_version: 1; status: 'ready' | 'limited'; kind: 'static_projection'; complete: boolean;
    components: SystemComponent[]; dependencies: SystemDependency[];
    cycles: { component_ids: string[] }[]; entrypoints: SystemSymbol[]; paths: SystemPath[];
    totals: Record<string, number>; limits: Record<string, number | boolean>; warnings: string[];
    overview?: SystemOverview; behavior?: SystemBehavior;
}
export interface SystemArchitectureResponse {
    status: 'pending' | 'ready' | 'failed'; generation: string; retry_after_ms?: number;
    result?: SystemProjection; error?: string;
}
export interface SystemArchitectureRequest { project: string; entryNodeId?: number; targetNodeId?: number; expectedGeneration?: string; includeBehaviorEvidence?: boolean }
export type SystemArchitectureLoader = (request: SystemArchitectureRequest, signal: AbortSignal) => Promise<SystemArchitectureResponse>;

const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
const list = (value: unknown): unknown[] => {
    if (!Array.isArray(value)) throw new Error('Architecture response is missing a collection.');
    return value;
};
const integer = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
const symbol = (value: unknown): value is SystemSymbol => record(value) && integer(value.id)
    && typeof value.name === 'string' && typeof value.qualified_name === 'string'
    && typeof value.label === 'string' && typeof value.component_id === 'string'
    && (value.group_id === undefined || typeof value.group_id === 'string')
    && (value.file_path === undefined || typeof value.file_path === 'string')
    && (value.start_line === undefined || integer(value.start_line))
    && (value.end_line === undefined || integer(value.end_line))
    && (value.signature === undefined || typeof value.signature === 'string')
    && (value.return_type === undefined || typeof value.return_type === 'string')
    && (value.parameters === undefined || (record(value.parameters) && integer(value.parameters.count)
        && Array.isArray(value.parameters.names) && value.parameters.names.every(name => typeof name === 'string')
        && Array.isArray(value.parameters.types) && value.parameters.types.every(type => typeof type === 'string')));
const counters = (value: unknown): value is Record<string, number> => record(value) && Object.values(value).every(integer);
const limits = (value: unknown) => record(value) && Object.values(value).every(item => integer(item) || typeof item === 'boolean');
const role = (value: unknown) => value === undefined || value === 'test' || value === 'non_test';
const evidence = (value: Record<string, unknown>): boolean => (value.callsite === undefined || (record(value.callsite)
    && typeof value.callsite.file_path === 'string' && integer(value.callsite.line) && value.callsite.line > 0))
    && (value.resolution === undefined || (record(value.resolution)
        && (value.resolution.strategy === undefined || typeof value.resolution.strategy === 'string')
        && (value.resolution.confidence === undefined || (typeof value.resolution.confidence === 'number' && Number.isFinite(value.resolution.confidence)))
        && (value.resolution.candidates === undefined || integer(value.resolution.candidates))))
    && (value.arguments === undefined || (Array.isArray(value.arguments) && value.arguments.every(argument => record(argument)
        && integer(argument.i) && typeof argument.e === 'string' && (argument.v === undefined || typeof argument.v === 'string'))))
    && (value.argument_limit === undefined || integer(value.argument_limit))
    && (value.arguments_complete === undefined || typeof value.arguments_complete === 'boolean');
const component = (value: unknown): value is SystemComponent => record(value) && typeof value.id === 'string'
    && typeof value.label === 'string' && typeof value.basis === 'string' && role(value.role)
    && (value.role_basis === undefined || typeof value.role_basis === 'string')
    && (value.group_id === undefined || typeof value.group_id === 'string')
    && integer(value.member_count) && integer(value.file_count) && list(value.representatives).every(symbol);
const dependency = (value: unknown): value is SystemDependency => record(value) && typeof value.source === 'string' && typeof value.target === 'string'
    && typeof value.type === 'string' && integer(value.count) && list(value.witnesses).every(witness => record(witness)
        && integer(witness.edge_id) && symbol(witness.source) && symbol(witness.target) && evidence(witness));
const pathEdge = (value: unknown): value is SystemPathEdge => record(value) && integer(value.id) && integer(value.source_id)
    && integer(value.target_id) && typeof value.type === 'string' && evidence(value);

function overview(value: unknown): boolean {
    return record(value) && typeof value.complete === 'boolean' && typeof value.grouping_basis === 'string'
        && list(value.groups).every(group => record(group) && typeof group.id === 'string' && typeof group.label === 'string' && role(group.role)
            && (group.basis === undefined || typeof group.basis === 'string') && integer(group.component_count) && integer(group.member_count)
            && integer(group.file_count) && list(group.component_ids).every(id => typeof id === 'string') && list(group.representatives).every(symbol))
        && list(value.components).every(component) && list(value.connections).every(dependency) && counters(value.totals) && limits(value.limits);
}
function behavior(value: unknown): boolean {
    if (!record(value) || !['targets', 'corridor'].includes(String(value.mode)) || !integer(value.source_id)
        || (value.target_id !== undefined && !integer(value.target_id)) || typeof value.complete !== 'boolean'
        || (value.corridor_complete !== undefined && typeof value.corridor_complete !== 'boolean')
        || (value.reachable !== undefined && typeof value.reachable !== 'boolean')
        || !list(value.limits_hit).every(item => typeof item === 'string') || !integer(value.max_hops)
        || !counters(value.totals) || !list(value.reachable_targets).every(item => symbol(item) && integer((item as unknown as Record<string, unknown>).distance))
        || !list(value.nodes).every(symbol) || !list(value.edges).every(pathEdge)
        || !list(value.cycles).every(cycle => record(cycle) && list(cycle.node_ids).every(integer))) return false;
    const ids = new Set((value.nodes as SystemSymbol[]).map(item => item.id));
    return (value.edges as SystemPathEdge[]).every(edge => ids.has(edge.source_id) && ids.has(edge.target_id));
}

/** Validate the wire boundary before letting a response drive rendering or source navigation. */
export function readSystemArchitecture(value: unknown): SystemArchitectureResponse {
    if (!record(value) || !['pending', 'ready', 'failed'].includes(String(value.status)) || typeof value.generation !== 'string') {
        throw new Error('This server returned an unsupported architecture response.');
    }
    if (value.retry_after_ms !== undefined && !integer(value.retry_after_ms)) throw new Error('Invalid architecture retry interval.');
    if (value.error !== undefined && typeof value.error !== 'string') throw new Error('Invalid architecture error.');
    if (value.status !== 'ready') return value as unknown as SystemArchitectureResponse;
    const result = value.result;
    if (!record(result) || result.schema_version !== 1 || result.kind !== 'static_projection'
        || !['ready', 'limited'].includes(String(result.status)) || typeof result.complete !== 'boolean') {
        throw new Error('This server returned an unsupported architecture projection.');
    }
    if (!list(result.components).every(component)
        || !list(result.dependencies).every(dependency)
        || !list(result.cycles).every(cycle => record(cycle) && list(cycle.component_ids).every(id => typeof id === 'string'))
        || !list(result.entrypoints).every(symbol)
        || !list(result.paths).every(path => record(path) && integer(path.entrypoint_id) && list(path.nodes).every(symbol)
            && list(path.edges).every(pathEdge))
        || !list(result.warnings).every(warning => typeof warning === 'string')
        || !record(result.totals) || !Object.values(result.totals).every(integer)
        || !record(result.limits) || !Object.values(result.limits).every(limit => integer(limit) || typeof limit === 'boolean')
        || (result.overview !== undefined && !overview(result.overview)) || (result.behavior !== undefined && !behavior(result.behavior))) {
        throw new Error('Architecture evidence has an invalid shape.');
    }
    return value as unknown as SystemArchitectureResponse;
}

/** The same on-demand backend projection is available to the browser and MCP clients. */
export async function loadSystemArchitecture(request: SystemArchitectureRequest, signal: AbortSignal, options: CallToolOptions = {}): Promise<SystemArchitectureResponse> {
    const value = await callToolJson('get_architecture', {
        project: request.project, aspects: ['system_structure'], format: 'json',
        ...(request.entryNodeId === undefined ? {} : { entry_node_id: request.entryNodeId }),
        ...(request.targetNodeId === undefined ? {} : { target_node_id: request.targetNodeId }),
        ...(request.expectedGeneration === undefined ? {} : { expected_generation: request.expectedGeneration }),
        ...(request.includeBehaviorEvidence === true ? { include_behavior_evidence: true } : {}),
    }, { ...options, signal });
    return readSystemArchitecture(value);
}
