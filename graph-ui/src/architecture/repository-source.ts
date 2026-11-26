import { useEffect, useState } from 'react';
import type { GraphData, GraphNode, GraphEdge } from '../galaxy/types';

export interface RepositorySnapshot extends GraphData {
    generation: string;
    indexedAt: string;
    nodesTruncated: boolean;
    edgesTruncated: boolean;
}
const object = (value: unknown): Record<string, unknown> => typeof value === 'object' && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
const string = (value: unknown): string => typeof value === 'string' ? value : '';
const number = (value: unknown): number => typeof value === 'number' && Number.isFinite(value) ? value : 0;

/** Reuse symbol identity with the 3D graph. Coordinates are deliberately unused
 * in this logical map; loading more architecture evidence never expands WebGL. */
export function readRepositorySnapshot(value: unknown): RepositorySnapshot {
    const raw = object(value);
    if (!Array.isArray(raw.nodes) || !Array.isArray(raw.edges) || typeof raw.generation !== 'string') {
        throw new Error('Repository map response has no graph snapshot metadata.');
    }
    const nodes: GraphNode[] = raw.nodes.map(value => {
        const row = object(value);
        if (!Number.isSafeInteger(row.id)) throw new Error('Repository map contains an invalid source identity.');
        return { id: number(row.id), name: string(row.name), label: string(row.label),
            file_path: string(row.file_path) || undefined, qualified_name: string(row.qualified_name),
            start_line: number(row.start_line) || undefined, end_line: number(row.end_line) || undefined,
            status: row.is_entry === true ? 'entry' : row.is_test === true ? 'test'
                : row.is_exported === true ? 'exported' : undefined,
            in_calls: 0, out_calls: 0,
            documentation: string(row.docstring), x: 0, y: 0, z: 0, size: 1, color: '',
            package_name: string(row.package_name),
        };
    });
    const edges: GraphEdge[] = raw.edges.map(value => {
        const row = object(value);
        if (!Number.isSafeInteger(row.source) || !Number.isSafeInteger(row.target) || typeof row.type !== 'string') throw new Error('Repository map contains an invalid edge.');
        return { source: number(row.source), target: number(row.target), type: row.type,
            id: number(row.id) || undefined, line: number(row.line) || undefined,
            strategy: string(row.strategy) || undefined,
            confidence: typeof row.confidence === 'number' && Number.isFinite(row.confidence) ? row.confidence : undefined };
    });
    const byId = new Map(nodes.map(node => [node.id, node]));
    for (const edge of edges) {
        if (edge.type !== 'CALLS') continue;
        const source = byId.get(edge.source); const target = byId.get(edge.target);
        if (source) source.out_calls = (source.out_calls ?? 0) + 1;
        if (target) target.in_calls = (target.in_calls ?? 0) + 1;
    }
    return { nodes, edges, total_nodes: number(raw.total_nodes), generation: raw.generation,
        indexedAt: string(raw.indexed_at), nodesTruncated: raw.nodes_truncated !== false,
        edgesTruncated: raw.edges_truncated !== false };
}

/** Numeric IDs and source locations may change on reindexing. */
export function resolveRepositorySelection(selected: GraphNode | undefined, snapshot?: GraphData): GraphNode | undefined {
    if (!selected || !snapshot) return selected;
    return snapshot.nodes.find(candidate => selected.qualified_name
        ? candidate.qualified_name === selected.qualified_name && candidate.file_path === selected.file_path
        : candidate.id === selected.id && candidate.name === selected.name && candidate.file_path === selected.file_path);
}

/** A cold daemon or interrupted large response can recover without a page reload. */
export async function fetchRepositorySnapshot(project: string, signal: AbortSignal, onRetry?: (attempt: number) => void): Promise<RepositorySnapshot> {
    for (let attempt = 0; ; attempt++) {
        let raw: unknown;
        try {
            const response = await fetch(`/api/repository-map?${new URLSearchParams({ project })}`, { signal });
            if (!response.ok) {
                const error = new Error(`Repository map returned HTTP ${response.status}.`);
                if (![408, 429, 502, 503, 504].includes(response.status)) throw Object.assign(error, { permanent: true });
                throw error;
            }
            raw = await response.json();
        } catch (error) {
            if (signal.aborted || attempt >= 2 || (error instanceof Error && 'permanent' in error)) throw error;
            onRetry?.(attempt + 1);
            await new Promise<void>((resolve, reject) => {
                const abort = () => { clearTimeout(timer); reject(new DOMException('Aborted', 'AbortError')); };
                const timer = setTimeout(() => { signal.removeEventListener('abort', abort); resolve(); }, 750 * (attempt + 1));
                signal.addEventListener('abort', abort, { once: true });
                if (signal.aborted) abort();
            });
            continue;
        }
        // Invalid graph identities are a data error; retries cannot establish their meaning.
        return readRepositorySnapshot(raw);
    }
}

export function useRepositorySnapshot(project: string, revision: number) {
    const [reading, setReading] = useState<{ project: string; snapshot?: RepositorySnapshot; error?: string; loading: boolean }>({ project: '', loading: false });
    useEffect(() => {
        if (!project) return;
        const controller = new AbortController();
        setReading({ project, loading: true });
        void fetchRepositorySnapshot(project, controller.signal, attempt => {
            if (!controller.signal.aborted) setReading({ project, loading: true, error: `Repository connection interrupted. Retrying (${attempt}/2)…` });
        }).then(snapshot => { if (!controller.signal.aborted) setReading({ project, snapshot, loading: false }); })
            .catch((error: unknown) => { if (!controller.signal.aborted) setReading({ project, loading: false, error: error instanceof Error ? error.message : String(error) }); });
        return () => controller.abort();
    }, [project, revision]);
    return reading.project === project ? reading : { project, loading: Boolean(project) };
}
