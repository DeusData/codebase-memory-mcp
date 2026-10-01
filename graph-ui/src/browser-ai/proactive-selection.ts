import { snapshotReaderContext, selectionLocation, type BrowserChatContext, type BrowserChatReaderContext } from './chat-model';

export const EXPLANATION_DELAY_MS = 600;
export const EXPLANATION_PROMPT = 'Explain the current selection in at most three short bullets: what it is responsible for, how its visible relationships or code work, and one important condition or limitation if evidenced. Use only the supplied source and graph evidence. Distinguish inference from facts. Do not claim runtime execution from static edges. Do not repeat raw metadata or inventories. If evidence is insufficient, say what is missing briefly.';

export interface ExplanationInput {
    key: string;
    label: string;
    reader?: BrowserChatReaderContext;
    graph?: BrowserChatContext;
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
    return { key: JSON.stringify([scope, graph.label, graph.text]), label: graph.label, graph: { ...graph } };
}
