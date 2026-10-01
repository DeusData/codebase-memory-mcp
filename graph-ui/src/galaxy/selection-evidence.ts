import { useEffect } from 'react';
import type { BrowserChatContext } from '../browser-ai/chat-model';
import type { GraphNode } from './types';

export type SelectionEvidenceListener = (context: BrowserChatContext | undefined) => void;
export interface SelectionEvidence {
    project: string;
    view: string;
    label: string;
    source: string;
    generation?: string;
    selected: unknown;
    relationships?: unknown;
    scope?: unknown;
    limitations: unknown;
}

/** Source identity only. Camera coordinates and visual weights are not code facts. */
export function graphNodeEvidence(node: GraphNode) {
    return { id: node.id, name: node.name, kind: node.label, qualifiedName: node.qualified_name,
        filePath: node.file_path, startLine: node.start_line, endLine: node.end_line,
        status: node.status, incomingCalls: node.in_calls, outgoingCalls: node.out_calls,
        documentation: node.documentation, packageName: node.package_name };
}

/** Bound every collection/string and the total snapshot; report each omission.
 * Stable content identity prevents camera changes from triggering explanations. */
export function selectionEvidenceContext(evidence: SelectionEvidence): BrowserChatContext {
    const omissions: { path: string; kind: string; count: number }[] = [];
    let budget = 16_000;
    const copy = (value: unknown, path: string, depth: number): unknown => {
        if (value === undefined) return undefined;
        if (budget <= 0 || depth > 10) {
            omissions.push({ path, kind: 'value', count: 1 }); return null;
        }
        if (typeof value === 'string') {
            const limit = Math.max(0, Math.min(1200, budget));
            budget -= Math.min(value.length, limit);
            if (value.length > limit) omissions.push({ path, kind: 'characters', count: value.length - limit });
            return value.slice(0, limit);
        }
        if (value === null || typeof value !== 'object') { budget -= 16; return value; }
        if (Array.isArray(value)) {
            const result: unknown[] = [];
            for (let i = 0; i < Math.min(value.length, 24) && budget > 0; i++) result.push(copy(value[i], `${path}[${i}]`, depth + 1));
            if (result.length < value.length) omissions.push({ path, kind: 'items', count: value.length - result.length });
            return result;
        }
        const entries = Object.entries(value), result: Record<string, unknown> = {};
        let processed = 0;
        for (const [key, item] of entries) {
            if (processed >= 48 || budget <= 0) break;
            budget -= key.length + 6; processed++;
            result[key] = copy(item, `${path}.${key}`, depth + 1);
        }
        if (processed < entries.length) omissions.push({ path, kind: 'fields', count: entries.length - processed });
        return result;
    };
    // Keep provenance and limits ahead of potentially large member collections.
    const snapshot = copy({ kind: 'current-selection-evidence', project: evidence.project, view: evidence.view,
        source: evidence.source, generation: evidence.generation ?? 'unavailable',
        limitations: evidence.limitations, scope: evidence.scope, selected: evidence.selected,
        relationships: evidence.relationships }, '$', 0);
    const text = JSON.stringify({ evidence: snapshot, omissions });
    let hash = 2166136261;
    for (let i = 0; i < text.length; i++) hash = Math.imul(hash ^ text.charCodeAt(i), 16777619);
    return { id: `selection:${evidence.project}:${evidence.view}:${(hash >>> 0).toString(16)}:${text.length}`,
        label: evidence.label.slice(0, 120), text };
}

/** Hidden workspaces must never overwrite evidence from the active workspace. */
export function useSelectionEvidence(listener: SelectionEvidenceListener | undefined, evidence: SelectionEvidence | undefined, active: boolean) {
    const context = evidence ? selectionEvidenceContext(evidence) : undefined;
    const text = context?.text, label = context?.label, id = context?.id;
    useEffect(() => {
        if (active) listener?.(text && id && label ? { id, label, text } : undefined);
    }, [listener, active, text, label, id]);
}
