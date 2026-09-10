import type { GraphNode } from '../galaxy/types';
import { RpcIntelligenceClient, rowsToObjects } from '../provider/rpc-client';
import type { RpcClientOptions } from '../provider/rpc-client';
import type { QueryGraphResult } from '../provider/rpc-schemas';

export interface RouteRelationship {
    source: GraphNode;
    target: GraphNode;
    type: 'HTTP_CALLS' | 'ASYNC_CALLS' | 'HANDLES';
    id?: number;
    routePath?: string;
    via?: string;
}

/** Independent query evidence; its numeric IDs must never select repository-map nodes. */
export interface RouteGraphSnapshot {
    relationships: RouteRelationship[];
    truncated: boolean;
    warnings: string[];
}

interface RouteQuery { source: string; target: string; relations: string }
const QUERIES: RouteQuery[] = ['Function', 'Method'].flatMap(label => [
    { source: label, target: 'Route', relations: 'HTTP_CALLS|ASYNC_CALLS' },
    { source: 'Route', target: label, relations: 'HANDLES' },
    { source: label, target: 'Route', relations: 'HANDLES' },
]);
const ROW_CAP = 300;

function queryText(query: RouteQuery): string {
    // Only fixed schema labels enter Cypher; project is a separate RPC argument.
    return `MATCH (s:${query.source})-[r:${query.relations}]->(t:${query.target}) RETURN `
        + 'id(s) AS source, s.qualified_name AS source_qn, s.file_path AS source_file, '
        + 's.start_line AS source_line, s.name AS source_name, id(t) AS target, '
        + 't.qualified_name AS target_qn, t.file_path AS target_file, t.start_line AS target_line, '
        + 't.name AS target_name, id(r) AS edge_id, type(r) AS edge_type, '
        + `r.url_path AS route_path, r.via AS via LIMIT ${ROW_CAP + 1}`;
}

const clean = (value: string | undefined) => value && value !== '{}' && value !== '-' ? value : undefined;
function integer(value: string | undefined): number | undefined {
    if (!value?.trim()) return undefined;
    const result = Number(value);
    return Number.isSafeInteger(result) && result >= 0 ? result : undefined;
}
function endpoint(row: Record<string, string>, side: 'source' | 'target', label: string): GraphNode | undefined {
    const id = integer(row[side]);
    if (id === undefined) return undefined;
    return { id, label, name: clean(row[`${side}_name`]) ?? clean(row[`${side}_qn`]) ?? label,
        qualified_name: clean(row[`${side}_qn`]), file_path: clean(row[`${side}_file`]),
        start_line: integer(row[`${side}_line`]) || undefined, x: 0, y: 0, z: 0, size: 1, color: '' };
}

export interface RouteGraphOptions extends RpcClientOptions {
    /** Read-only seam for deterministic pagination and incomplete-result tests. */
    client?: Pick<RpcIntelligenceClient, 'queryGraph'>;
}

/** Keep useful pages if one relationship family is unavailable or a continuation fails. */
export async function loadRouteGraph(project: string, options: RouteGraphOptions = {}): Promise<RouteGraphSnapshot> {
    const client = options.client ?? new RpcIntelligenceClient(options);
    const results = await Promise.all(QUERIES.map(async query => {
        const relationships: RouteRelationship[] = [];
        const warnings: string[] = [];
        let truncated = false;
        let first: QueryGraphResult | undefined;
        let cursor: string | undefined;
        let rowCount = 0;
        const seenCursors = new Set<string>();
        try {
            do {
                if (options.signal?.aborted) throw new DOMException('Aborted', 'AbortError');
                const page = await client.queryGraph(project, queryText(query), cursor);
                if (first && (page.offset !== rowCount || page.total !== first.total
                    || JSON.stringify(page.columns) !== JSON.stringify(first.columns) || page.rows.length === 0)) {
                    throw new Error('Query continuation changed its snapshot.');
                }
                first ??= page;
                const rows = rowsToObjects(page.columns, page.rows);
                for (const row of rows.slice(0, Math.max(0, ROW_CAP - rowCount))) {
                    const source = endpoint(row, 'source', query.source);
                    const target = endpoint(row, 'target', query.target);
                    const type = row.edge_type;
                    if (!source || !target || !['HTTP_CALLS', 'ASYNC_CALLS', 'HANDLES'].includes(type)
                        || !query.relations.split('|').includes(type)) {
                        truncated = true;
                        warnings.push('Some route relationship rows had incomplete identities and were omitted.');
                        continue;
                    }
                    relationships.push({ source, target, type: type as RouteRelationship['type'],
                        id: integer(row.edge_id), routePath: clean(row.route_path), via: clean(row.via) });
                }
                rowCount += rows.length;
                const more = Boolean(page.hasMore || page.truncated || page.nextCursor
                    || page.totalRelation === 'gte' || (page.total !== undefined && page.total > rowCount));
                if (page.warning) warnings.push(page.warning);
                if (!more && rowCount <= ROW_CAP) break;
                if (rowCount >= ROW_CAP || !page.nextCursor || seenCursors.has(page.nextCursor)
                    || seenCursors.size >= 20 || (page.nextOffset !== undefined && page.nextOffset !== rowCount)) {
                    truncated = true;
                    warnings.push(`Route relationship lookup was limited to ${Math.min(rowCount, ROW_CAP)} rows for ${query.source} → ${query.target} (${query.relations}).`);
                    break;
                }
                seenCursors.add(page.nextCursor);
                cursor = page.nextCursor;
            } while (cursor);
        } catch (error) {
            if (options.signal?.aborted || (error instanceof Error && error.name === 'AbortError')) throw error;
            truncated = true;
            warnings.push(`Could not finish ${query.source} → ${query.target} (${query.relations}): ${error instanceof Error ? error.message : 'query unavailable'}`);
        }
        return { relationships, truncated, warnings };
    }));
    return { relationships: results.flatMap(result => result.relationships),
        truncated: results.some(result => result.truncated), warnings: [...new Set(results.flatMap(result => result.warnings))] };
}
