/** Distinct relationship hues. Keep edges subdued with opacity, not gray hues. */
export const DEFAULT_EDGE_COLOR = '#84939d';
export const EDGE_TYPE_COLORS: Record<string, string> = {
    CALLS: '#579fc7', IMPORTS: '#ad7acf', USAGE: '#72b877', DATA_FLOWS: '#49b8b0',
    HTTP_CALLS: '#d58c60', ASYNC_CALLS: '#cb7fa9', GRPC_CALLS: '#b9ad58', GRAPHQL_CALLS: '#bd72ca', TRPC_CALLS: '#898cda',
    CROSS_HTTP_CALLS: '#cda078', CROSS_ASYNC_CALLS: '#b96597', CROSS_GRPC_CALLS: '#a49649', CROSS_GRAPHQL_CALLS: '#a86bb7', CROSS_TRPC_CALLS: '#777fc0', CROSS_CHANNEL: '#c28676',
    INHERITS: '#8294d7', IMPLEMENTS: '#c5b66a', OVERRIDE: '#bc7db8',
    DEFINES: '#c6a05c', DEFINES_METHOD: '#dfb87a',
    CONTAINS: '#719f89', CONTAINS_FILE: '#92ad71', CONTAINS_FOLDER: '#57a88c', CONTAINS_PACKAGE: '#abad65',
    MEMBER_OF: '#649daa', HAS_BRANCH: '#9a79ae', HANDLES: '#aebb61',
    CONFIGURES: '#bda644', RAISES: '#d26b72', THROWS: '#ba606e', CALL_REFERENCE: '#67c1cf', RETURNS: '#a1bd80',
    SERVICE_CALLS: '#7da4ba', STARTUP_DEPENDENCY: '#8a8199',
    READS: '#70b59f', WRITES: '#cc9862', TESTS: '#65a8bb', TESTS_FILE: '#83b9c5',
    SIMILAR_TO: '#cb88b5', SEMANTICALLY_RELATED: '#b58ebf', FILE_CHANGES_WITH: '#b19b81',
    RELATIONSHIPS: DEFAULT_EDGE_COLOR,
};
const symmetric = new Set(['SIMILAR_TO', 'SEMANTICALLY_RELATED', 'FILE_CHANGES_WITH']);
export const normalizeEdgeType = (type: unknown) => typeof type === 'string' ? type.trim().replaceAll('-', '_').toUpperCase() : '';
function constituents(type: string, types?: readonly string[]): string[] {
    return [...new Set((types?.length ? types : [type]).map(normalizeEdgeType))];
}
export function edgeColor(type: string, types?: readonly string[]): string {
    if (!types?.length) return EDGE_TYPE_COLORS[normalizeEdgeType(type)] ?? DEFAULT_EDGE_COLOR;
    const kinds = constituents(type, types);
    return kinds.length === 1 ? EDGE_TYPE_COLORS[kinds[0]] ?? DEFAULT_EDGE_COLOR : DEFAULT_EDGE_COLOR;
}
/** Direction is a relationship property, never evidence of observed execution. */
export function isDirectedEdge(type: string, types?: readonly string[]): boolean {
    if (!types?.length) {
        const kind = normalizeEdgeType(type);
        return Object.hasOwn(EDGE_TYPE_COLORS, kind) && kind !== 'RELATIONSHIPS' && !symmetric.has(kind);
    }
    return constituents(type, types).every(kind => Object.hasOwn(EDGE_TYPE_COLORS, kind) && kind !== 'RELATIONSHIPS' && !symmetric.has(kind));
}
/** Offset each edge deterministically so the graph does not flash in unison. */
export function edgePhase(id: string | number): number {
    let hash = 2166136261;
    for (const character of String(id)) hash = Math.imul(hash ^ character.charCodeAt(0), 16777619) >>> 0;
    return hash / 4294967296;
}
