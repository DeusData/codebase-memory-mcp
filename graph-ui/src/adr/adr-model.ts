import type { AdrRecord } from '../projects/projects-model';

export const ADR_CONTENT_LIMIT = 8000;
export const ADR_BODY_LIMIT = 16_384;

export interface AdrDraft {
    version: 1;
    base: AdrRecord;
    content: string;
}

const memoryDrafts = new Map<string, AdrDraft>();
const draftKey = (project: string) => `cbm.adr.draft:${encodeURIComponent(project)}`;

export function sameAdr(left: AdrRecord, right: AdrRecord): boolean {
    return left.hasAdr === right.hasAdr && left.content === right.content;
}

export function adrSize(project: string, content: string) {
    const encoder = new TextEncoder();
    const bytes = encoder.encode(content).length;
    const bodyBytes = encoder.encode(JSON.stringify({ project, content })).length;
    return { bytes, bodyBytes, valid: bytes <= ADR_CONTENT_LIMIT && bodyBytes <= ADR_BODY_LIMIT };
}

export function readAdrDraft(project: string): AdrDraft | undefined {
    const key = draftKey(project);
    const inMemory = memoryDrafts.get(key);
    if (inMemory) return inMemory;
    try {
        const value: unknown = JSON.parse(sessionStorage.getItem(key) ?? 'null');
        if (!value || typeof value !== 'object') return;
        const draft = value as Partial<AdrDraft>;
        if (draft.version !== 1 || typeof draft.content !== 'string' || !draft.base
            || typeof draft.base.hasAdr !== 'boolean' || typeof draft.base.content !== 'string'
            || typeof draft.base.updatedAt !== 'string') return;
        if (draft.content === draft.base.content) return;
        const restored: AdrDraft = { version: 1, content: draft.content, base: { ...draft.base } };
        memoryDrafts.set(key, restored);
        return restored;
    } catch { return; }
}

/** Memory also retains drafts across project switches when browser storage is unavailable. */
export function storeAdrDraft(project: string, draft: AdrDraft): boolean {
    const key = draftKey(project);
    memoryDrafts.set(key, draft);
    try { sessionStorage.setItem(key, JSON.stringify(draft)); return true; } catch { return false; }
}

export function clearAdrDraft(project: string): void {
    const key = draftKey(project);
    memoryDrafts.delete(key);
    try { sessionStorage.removeItem(key); } catch { /* Storage can be unavailable. */ }
}

/** Generic authoring scaffold; no repository facts are invented. */
export function appendDecision(content: string): string {
    const heading = content.trim() ? '\n\n---\n\n## Decision title' : '# Architecture decisions\n\n## Decision title';
    return `${content.trimEnd()}${heading}\n\n**Status:** Proposed\n\n### Context\n\nWhat problem or constraint led to this decision?\n\n### Decision\n\nWhat will change, and why?\n\n### Alternatives\n\nWhich options were considered?\n\n### Consequences\n\nWhat benefits, costs, and follow-up work should readers know?\n`;
}
