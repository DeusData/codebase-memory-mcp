import { useEffect, useState } from 'react';
import type { SymbolRef } from '../core/focus-protocol';
import { symbolKindOf } from '../provider/cbm-rpc-provider';
import { closureKeyOf, getClosure, type ClosureOptions, type ClosureResult, type ClosureSource } from '../provider/closure';
import { targetRefOfNode } from './galaxy-model';
import type { GraphNode } from './types';

export type HierarchyStatus = 'empty' | 'loading' | 'ready' | 'unavailable';
export interface SelectionHierarchy {
    status: HierarchyStatus;
    walk?: ClosureResult;
    message: string;
}

/** Graph identity is enough for a hierarchy; opening a source file is optional. */
export function hierarchySymbolOf(node: GraphNode | undefined, project: string): SymbolRef | undefined {
    if (!node?.qualified_name || !project) return undefined;
    const location = targetRefOfNode(node);
    return { ...location, name: node.name, qualifiedName: node.qualified_name, nodeId: node.qualified_name,
        projectName: project, kind: symbolKindOf(node.label), uri: location?.uri ?? '',
        range: location?.range ?? { start: { line: 0, character: 0 }, end: { line: 0, character: 0 } } };
}

/** Keep provider failures distinct from a valid leaf with no recorded outgoing calls. */
export async function loadSelectionHierarchy(source: ClosureSource, workspaceRoot: string, root: SymbolRef, options: ClosureOptions): Promise<SelectionHierarchy> {
    let rootFailure = '';
    let incomplete = false;
    const wrapped: ClosureSource = {
        resolveSymbolAt: (...args) => source.resolveSymbolAt(...args),
        getFacts: async (...args) => {
            const isRoot = closureKeyOf(args[1]) === closureKeyOf(root);
            try {
                const facts = await source.getFacts(...args);
                if (!facts.callees || !['known', 'inferred'].includes(facts.callees.state)) {
                    incomplete = true;
                    if (isRoot) rootFailure = 'Outgoing calls are unavailable for this symbol.';
                }
                return facts;
            } catch (error) {
                incomplete = true;
                if (isRoot) rootFailure = error instanceof Error ? error.message : String(error);
                throw error;
            }
        },
    };
    const walk = await getClosure(wrapped, workspaceRoot, root, options);
    if (rootFailure) return { status: 'unavailable', message: rootFailure };
    return { status: 'ready', walk, message: incomplete ? 'Some outgoing calls could not be read. This hierarchy is incomplete.'
        : walk.edges.length === 0 && !walk.truncated ? 'No outgoing calls are recorded for this symbol.' : '' };
}

/** Clear old selections immediately and ignore responses from earlier roots or projects. */
export function useSelectionHierarchy(source: ClosureSource, workspaceRoot: string, project: string, root: SymbolRef | undefined,
    bounds: { depth?: number; cap?: number }, enabled = true): SelectionHierarchy {
    const key = root && enabled && project ? JSON.stringify([project, root.qualifiedName, root.uri, root.range, bounds.depth, bounds.cap]) : '';
    const [result, setResult] = useState<{ key: string; source: ClosureSource; value: SelectionHierarchy }>();
    useEffect(() => {
        if (!key || !root) return;
        let cancelled = false;
        setResult({ key, source, value: { status: 'loading', message: `Loading outgoing calls for ${root.name}…` } });
        void loadSelectionHierarchy(source, workspaceRoot, root, { projectName: project, generation: 1, depth: bounds.depth, cap: bounds.cap })
            .then((value) => { if (!cancelled) setResult({ key, source, value }); })
            .catch((error: unknown) => { if (!cancelled) setResult({ key, source, value: { status: 'unavailable', message: error instanceof Error ? error.message : String(error) } }); });
        return () => { cancelled = true; };
    }, [source, workspaceRoot, project, key, root, bounds.depth, bounds.cap]);
    if (!key) return { status: 'empty', message: 'Choose a symbol to see its outgoing call hierarchy.' };
    if (!result || result.key !== key || result.source !== source) return { status: 'loading', message: `Loading outgoing calls for ${root?.name ?? 'the selected symbol'}…` };
    return result.value;
}
