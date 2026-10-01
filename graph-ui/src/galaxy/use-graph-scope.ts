import { useEffect, useMemo, useRef, useState } from 'react';
import { graphEdgeTypesKey, loadGraphScope, type GraphScope, type ScopedGraph, type TraceDirection } from './graph-scope';
import { RpcIntelligenceClient } from '../provider/rpc-client';
import { readerGraphFocus, type SourceFocusRange } from './reader-graph-focus';
import type { GraphData } from './types';
import { graphNeighborhoodCache, graphNeighborhoodTransportKey, peekGraphNeighborhoodCache } from './graph-neighborhood-cache';

/** Scoped requests are independent of the whole-repository layout budget. */
export function useGraphScope({ project, layout, filePath, range, fetch: fetchImpl, edgeTypes }: {
    project: string; layout?: GraphData; filePath?: string; range?: SourceFocusRange; fetch?: typeof globalThis.fetch; edgeTypes?: readonly string[];
}) {
    const cache = useRef(new Map<string, ScopedGraph>());
    const [chosen, setChosen] = useState<GraphScope>();
    const [depth, setDepth] = useState(0);
    const [direction, setDirection] = useState<TraceDirection>('both');
    const [result, setResult] = useState<{ key: string; scopeKey: string; value: ScopedGraph }>();
    const [cachedPreview, setCachedPreview] = useState<{ key: string; value: ScopedGraph }>();
    const [pendingPhase, setPendingPhase] = useState<{ key: string; validating: boolean }>();
    const [failure, setFailure] = useState<{ key: string; message: string }>();
    const [reload, setReload] = useState(0);
    const transportKey = graphNeighborhoodTransportKey(fetchImpl);
    const readerKey = JSON.stringify([transportKey, project, filePath, range?.startLine, range?.endLine]);
    const [readerDepth, setReaderDepth] = useState<{ key: string; depth: number }>();
    const actualDepth = filePath ? readerDepth?.key === readerKey ? readerDepth.depth : 1 : depth;
    const scope = useMemo<GraphScope | undefined>(() => filePath ? { kind: 'file', path: filePath,
        name: `${filePath.split('/').pop()}${range ? `:${range.startLine}–${range.endLine}` : ''}`, ...(range ? { range } : {}) } : chosen,
    [filePath, range?.startLine, range?.endLine, chosen]);
    const edgeTypesKey = graphEdgeTypesKey(edgeTypes);
    const selectedTypes = useMemo<string[] | undefined>(() => edgeTypesKey === 'null' ? undefined : JSON.parse(edgeTypesKey) as string[], [edgeTypesKey]);
    const actualDirection = filePath ? 'both' : direction;
    const scopeKey = JSON.stringify([transportKey, project, scope, actualDirection, edgeTypesKey, reload]);
    const key = JSON.stringify([scopeKey, actualDepth]);
    useEffect(() => { setChosen(undefined); setDepth(0); setDirection('both'); setResult(undefined); setFailure(undefined); }, [project]);
    useEffect(() => {
        if (!scope || !project) { setResult(undefined); return; }
        const abort = new AbortController();
        const client = new RpcIntelligenceClient({ fetch: fetchImpl, signal: abort.signal });
        setPendingPhase({ key, validating: true });
        const warmCache = peekGraphNeighborhoodCache(project, fetchImpl);
        let checkedGeneration: string | undefined, checked = false;
        if (warmCache && (warmCache.size.nodes || warmCache.size.roots)) {
            // Paint retained evidence while revalidating, never label it current yet.
            void loadGraphScope(project, scope, actualDepth, actualDirection, layout, {
                signal: abort.signal, edgeTypes: selectedTypes, cache: warmCache.begin(),
                client: { queryGraph: async () => { throw new Error('Uncached neighborhood'); } },
            }).then(value => {
                if (!abort.signal.aborted && (!checked || checkedGeneration === warmCache.generation)) setCachedPreview({ key, value });
            }).catch(() => { /* A partial cache is not a complete preview. */ });
        }
        const generation = async (): Promise<string | undefined> => {
            try {
                const raw = await client.indexStatusPayload(project) as Record<string, unknown>;
                return typeof raw.indexed_at === 'string' && raw.indexed_at ? raw.indexed_at : undefined;
            } catch { return undefined; }
        };
        void (async () => {
            const before = await generation();
            checked = true; checkedGeneration = before;
            abort.signal.throwIfAborted();
            if (warmCache?.generation !== before) setCachedPreview(undefined);
            const neighborhood = before ? graphNeighborhoodCache(project, before, fetchImpl) : undefined;
            // Legacy servers without a publication token never reuse cached evidence.
            const cacheKey = JSON.stringify([key, before]);
            const cached = before ? cache.current.get(cacheKey) : undefined;
            if (cached) { setResult({ key, scopeKey, value: cached }); return; }
            const previousKey = JSON.stringify([JSON.stringify([scopeKey, actualDepth - 1]), before]);
            const previous = before && actualDepth > 0 ? cache.current.get(previousKey) : undefined;
            const transaction = neighborhood?.begin();
            const value = await loadGraphScope(project, scope, actualDepth, actualDirection, layout,
                { fetch: fetchImpl, signal: abort.signal, previous, edgeTypes: selectedTypes, cache: transaction,
                    client: { queryGraph: (...args) => {
                        setPendingPhase({ key, validating: false }); return client.queryGraph(...args);
                    } } });
            setPendingPhase({ key, validating: true });
            const after = await generation();
            if (before && after !== before) {
                neighborhood?.invalidate(); setCachedPreview(undefined);
                throw new Error('The index changed while loading this scope. Retry for the current index.');
            }
            if (!abort.signal.aborted) {
                transaction?.commit();
                if (cache.current.size >= 4) cache.current.delete(cache.current.keys().next().value!);
                if (before && value.data.nodes.length <= 25_000 && value.data.edges.length <= 100_000) cache.current.set(cacheKey, value);
                setResult({ key, scopeKey, value }); setFailure(undefined);
            }
        })().catch((error: unknown) => {
            if (!abort.signal.aborted) setFailure({ key, message: error instanceof Error ? error.message : String(error) });
        });
        return () => abort.abort();
        // The global layout supplies colors only; its render budget must not re-fetch the scope.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [key, fetchImpl]);
    const complete = result?.key === key ? result.value : undefined;
    const error = failure?.key === key ? failure.message : undefined;
    const fallback = useMemo<ScopedGraph | undefined>(() => {
        if (!scope || !layout) return undefined;
        const roots = scope.kind === 'file' ? readerGraphFocus(layout.nodes, scope.path, scope.range).ids
            : new Set(layout.nodes.filter(node => scope.kind === 'node' ? node.id === scope.id
                : scope.kind === 'symbol' ? node.qualified_name === scope.qualifiedName
                    : node.file_path?.startsWith(scope.path.replace(/\/$/, '') + '/')).map(node => node.id));
        const allowedTypes = selectedTypes === undefined ? undefined : new Set(selectedTypes);
        const candidates = layout.edges.filter(edge => !allowedTypes || allowedTypes.has(edge.type));
        const ids = new Set(roots), edges = new Set<typeof layout.edges[number]>();
        const levels = new Map<number, number>([...roots].map(identity => [identity, 0] as const));
        let frontier = new Set(roots);
        for (let hop = 0; hop < actualDepth && frontier.size; hop++) {
            const next = new Set<number>();
            for (const edge of candidates) {
                const follows = (actualDirection !== 'inbound' && frontier.has(edge.source))
                    || (actualDirection !== 'outbound' && frontier.has(edge.target));
                if (!follows) continue;
                edges.add(edge);
                for (const identity of [edge.source, edge.target]) if (!ids.has(identity)) {
                    ids.add(identity); next.add(identity); levels.set(identity, hop + 1);
                }
            }
            frontier = next;
        }
        if (actualDepth === 0) for (const edge of candidates) {
            if (roots.has(edge.source) && roots.has(edge.target)) edges.add(edge);
        }
        return { data: { nodes: layout.nodes.filter(node => ids.has(node.id)), edges: [...edges], total_nodes: ids.size },
            roots, depth: actualDepth, exhausted: false, levels };
    }, [scope, layout, actualDepth, actualDirection, selectedTypes]);
    return { scope, depth: actualDepth, direction, setDirection,
        result: complete ?? (cachedPreview?.key === key ? cachedPreview.value : undefined)
            ?? (result?.scopeKey === scopeKey && result.value.depth <= actualDepth ? result.value : fallback), loading: Boolean(scope && !complete && !error), error,
        validating: Boolean(scope && !complete && !error && pendingPhase?.key === key && pendingPhase.validating),
        complete: Boolean(complete), retry: () => setReload(value => value + 1),
        select: (next: GraphScope) => { setChosen(next); setDepth(0); setDirection('both'); },
        reset: () => { setChosen(undefined); setDepth(0); setReaderDepth(undefined); setDirection('both'); },
        setDepth: (next: number) => filePath ? setReaderDepth({ key: readerKey, depth: Math.max(1, next) }) : setDepth(Math.max(0, next)),
    };
}
