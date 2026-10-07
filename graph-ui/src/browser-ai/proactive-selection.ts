import { snapshotReaderContext, selectionLocation, type BrowserChatContext, type BrowserChatReaderContext } from './chat-model';
import { readGalaxyEvidence } from './galaxy-evidence';

export const EXPLANATION_DELAY_MS = 600;
export const EXPLANATION_PROMPT = 'Explain the current selection in at most three short bullets: what it is responsible for, how its visible relationships or code work, and one important condition or limitation if evidenced. Use only the supplied source and graph evidence. Distinguish inference from facts. Do not claim runtime execution from static edges. Do not repeat raw metadata or inventories. If evidence is insufficient, say what is missing briefly.';

export interface ExplanationInput {
    key: string;
    label: string;
    reader?: BrowserChatReaderContext;
    graph?: BrowserChatContext;
    /** A Galaxy scope that is not complete yet is shown, never explained. */
    waiting?: 'loading' | 'partial';
    /** The complete scope's evidence: a cached explanation of other evidence (a re-index) is stale. */
    evidence?: string;
}

/** Snapshot exact evidence; generated UI event IDs are not selection identity. */
export function explanationInput(scope: string, reader?: BrowserChatReaderContext, graph?: BrowserChatContext): ExplanationInput | undefined {
    if (reader) {
        const snapshot = snapshotReaderContext(reader);
        if (!snapshot?.source) return;
        const { id: _id, ...source } = snapshot.source;
        return { key: JSON.stringify([scope, source]), label: source.kind === 'selection' ? selectionLocation(snapshot.source) : source.path, reader: snapshot };
    }
    if (!graph?.text.trim()) return;
    const galaxy = readGalaxyEvidence(graph.text);
    if (galaxy) {
        // The selection and how its scope is drawn; counts and loading state change while it loads.
        const key = JSON.stringify([scope, 'galaxy', galaxy.identity, galaxy.direction, galaxy.edgeTypes, galaxy.depth]);
        // A layer stopped at the render limit is settled: explained, with facts that say it is partial (C1).
        const settled = galaxy.state === 'complete' || galaxy.state === 'limited';
        return { key, label: graph.label, graph: { ...graph }, ...settled ? { evidence: graph.id } : { waiting: galaxy.state === 'loading' ? 'loading' : 'partial' } };
    }
    return { key: JSON.stringify([scope, graph.label, graph.text]), label: graph.label, graph: { ...graph } };
}
