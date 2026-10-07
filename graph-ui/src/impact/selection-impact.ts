/** Selection impact deliberately keeps graph evidence and Git association apart. */
export interface SelectionImpactTarget {
    filePath: string;
    name?: string;
    id?: number;
    qualifiedName?: string;
    line?: number;
}
export interface ImpactEvidenceNode {
    id: number;
    name: string;
    qualified_name: string;
    file_path: string;
    line: number;
}
export interface ImpactEvidenceEdge {
    edge_id: number;
    type: 'CALLS' | 'IMPORTS';
    from: ImpactEvidenceNode;
    to: ImpactEvidenceNode;
}
export interface SelectionImpactFinding extends ImpactEvidenceNode {
    distance: number;
    test_candidate: boolean;
    path: ImpactEvidenceEdge[];
}
export interface SelectionImpact {
    status: 'ready';
    project: string;
    file_path: string;
    scope: 'file' | 'symbol';
    computed_at: number;
    cache_seconds: number;
    snapshot: {
        indexed_at: string;
        generation: string;
        index_revision: string;
        freshness: string;
        coverage_recording: string;
        coverage: { kind: string; count: number }[];
    };
    structural: {
        available: boolean;
        basis: string;
        max_depth: number;
        visit_cap: number;
        result_cap: number;
        seed_count: number;
        reachable: number;
        direct: number;
        test_candidates: number;
        truncated: boolean;
        error?: string;
        findings: SelectionImpactFinding[];
    };
    history: {
        available: boolean;
        head?: string;
        shallow?: boolean;
        worktree_status_known?: boolean;
        selection_uncommitted?: boolean;
        truncated: boolean;
        error?: string;
        commit_cap: number;
        window_days: number;
        mass_change_threshold: number;
        commits_scanned: number;
        commits_considered: number;
        selection_commits: number;
        merges_excluded: number;
        mass_changes_excluded: number;
        read_errors: number;
        cochanges_omitted: number;
        commits: { hash: string; subject: string; time: number; files: number }[];
        cochanges: { file_path: string; shared_commits: number; commit_refs: string[] }[];
    };
}

export type SelectionImpactReply = SelectionImpact | { status: 'pending' | 'busy' }
    | { status: 'failed'; error: string };

function object(value: unknown): Record<string, unknown> {
    if (typeof value !== 'object' || value === null || Array.isArray(value))
        throw new Error('Invalid impact response');
    return value as Record<string, unknown>;
}
function text(value: unknown): string {
    if (typeof value !== 'string') throw new Error('Invalid impact text');
    return value;
}
function count(value: unknown): number {
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
        throw new Error('Invalid impact count');
    return value;
}
function flag(value: unknown): boolean {
    if (typeof value !== 'boolean') throw new Error('Invalid impact flag');
    return value;
}
function list(value: unknown): unknown[] {
    if (!Array.isArray(value)) throw new Error('Invalid impact evidence list');
    return value;
}
function node(value: unknown): ImpactEvidenceNode {
    const row = object(value);
    return { id: count(row.id), name: text(row.name), qualified_name: text(row.qualified_name),
        file_path: text(row.file_path), line: count(row.line) };
}

/** Fail closed: an API error or malformed evidence never becomes a reassuring zero. */
export function readSelectionImpact(value: unknown): SelectionImpactReply {
    const row = object(value);
    if (row.status === 'pending' || row.status === 'busy') return { status: row.status };
    if (row.status === 'failed') return { status: 'failed', error: text(row.error) };
    if (row.status !== 'ready' || (row.scope !== 'symbol' && row.scope !== 'file'))
        throw new Error('Invalid impact snapshot');
    const snapshot = object(row.snapshot), structural = object(row.structural), history = object(row.history);
    const result: SelectionImpact = {
        status: 'ready', project: text(row.project), file_path: text(row.file_path), scope: row.scope,
        computed_at: count(row.computed_at), cache_seconds: count(row.cache_seconds),
        snapshot: {
            indexed_at: text(snapshot.indexed_at), generation: text(snapshot.generation),
            index_revision: text(snapshot.index_revision), freshness: text(snapshot.freshness),
            coverage_recording: text(snapshot.coverage_recording),
            coverage: list(snapshot.coverage).map(value => {
                const item = object(value); return { kind: text(item.kind), count: count(item.count) };
            }),
        },
        structural: {
            available: flag(structural.available), basis: text(structural.basis),
            max_depth: count(structural.max_depth), visit_cap: count(structural.visit_cap),
            result_cap: count(structural.result_cap), seed_count: count(structural.seed_count),
            reachable: count(structural.reachable), direct: count(structural.direct),
            test_candidates: count(structural.test_candidates), truncated: flag(structural.truncated),
            ...(structural.error === undefined ? {} : { error: text(structural.error) }),
            findings: list(structural.findings).map(value => {
                const item = object(value);
                const path = list(item.path).map(value => {
                    const edge = object(value);
                    if (edge.type !== 'CALLS' && edge.type !== 'IMPORTS') throw new Error('Unknown impact relation');
                    return { edge_id: count(edge.edge_id), type: edge.type,
                        from: node(edge.from), to: node(edge.to) } as ImpactEvidenceEdge;
                });
                const finding = { ...node(item), distance: count(item.distance),
                    test_candidate: flag(item.test_candidate), path };
                if (path.length === 0 || path.length !== finding.distance || path[0]?.from.id !== finding.id
                    || path.some((edge, i) => i > 0 && path[i - 1]?.to.id !== edge.from.id))
                    throw new Error('Disconnected impact evidence path');
                return finding;
            }),
        },
        history: {
            available: flag(history.available), truncated: flag(history.truncated),
            commit_cap: count(history.commit_cap), window_days: count(history.window_days),
            mass_change_threshold: count(history.mass_change_threshold),
            commits_scanned: count(history.commits_scanned), commits_considered: count(history.commits_considered),
            selection_commits: count(history.selection_commits), merges_excluded: count(history.merges_excluded),
            mass_changes_excluded: count(history.mass_changes_excluded), read_errors: count(history.read_errors),
            cochanges_omitted: count(history.cochanges_omitted),
            ...(history.head === undefined ? {} : { head: text(history.head) }),
            ...(history.error === undefined ? {} : { error: text(history.error) }),
            ...(history.shallow === undefined ? {} : { shallow: flag(history.shallow) }),
            ...(history.worktree_status_known === undefined ? {} : { worktree_status_known: flag(history.worktree_status_known) }),
            ...(history.selection_uncommitted === undefined ? {} : { selection_uncommitted: flag(history.selection_uncommitted) }),
            commits: list(history.commits).map(value => {
                const item = object(value); return { hash: text(item.hash), subject: text(item.subject),
                    time: count(item.time), files: count(item.files) };
            }),
            cochanges: list(history.cochanges).map(value => {
                const item = object(value); return { file_path: text(item.file_path),
                    shared_commits: count(item.shared_commits), commit_refs: list(item.commit_refs).map(text) };
            }),
        },
    };
    return result;
}

export interface SelectionRisk {
    level: 'high' | 'elevated' | 'unresolved';
    label: string;
    reasons: string[];
    uncertainties: string[];
    next: string[];
}

/** An explicit review-priority heuristic, never a calibrated defect prediction. */
export function selectionRisk(data: SelectionImpact): SelectionRisk {
    const graph = data.structural, history = data.history;
    const reasons: string[] = [], uncertainties: string[] = [];
    let level: SelectionRisk['level'] = 'unresolved';
    if (graph.available && (graph.direct >= 10 || graph.reachable >= 25)) {
        level = 'high';
        reasons.push(`${graph.direct} direct and ${graph.reachable} total indexed dependents meet the broad-impact rule (10 direct or 25 total).`);
    } else if (graph.available && graph.reachable > 0) {
        level = 'elevated';
        reasons.push(`${graph.reachable} indexed dependent${graph.reachable === 1 ? '' : 's'} could be affected; inspect direct paths first.`);
    }
    if (history.available && history.selection_commits >= 10) {
        reasons.push(`This file changed in ${history.selection_commits} sampled ordinary commits (frequent-change threshold: 10).`);
        if (level === 'unresolved') level = 'elevated';
    }
    const repeated = history.cochanges.filter(row => row.shared_commits >= 3);
    if (history.available && repeated.length > 0)
        reasons.push(`${repeated.length} file${repeated.length === 1 ? '' : 's'} changed together with this selection in at least 3 sampled commits; review these historical associations separately.`);
    if (reasons.length === 0) reasons.push('No elevated signal was established in the available sample. This does not establish low risk.');
    if (!graph.available) uncertainties.push(graph.error ?? 'The selection has no usable indexed dependency evidence.');
    if (graph.truncated) uncertainties.push('The graph walk or evidence list reached a bound; further dependents may exist.');
    if (data.snapshot.coverage_recording !== 'complete') uncertainties.push('Index coverage recording is incomplete or unavailable.');
    const gaps = data.snapshot.coverage.reduce((sum, row) => sum + row.count, 0);
    if (gaps > 0) uncertainties.push(`${gaps} persisted index coverage records identify excluded, unsupported, or partially indexed paths; inspect Diagnostics.`);
    if (data.snapshot.index_revision === 'unknown') uncertainties.push(data.snapshot.freshness);
    if (!history.available) uncertainties.push(history.error ?? 'Historical evidence is unavailable or contains read failures.');
    if (history.shallow) uncertainties.push('This is a shallow checkout; earlier co-changes are missing.');
    if (history.truncated) uncertainties.push('The history scan reached a count, time, or file bound.');
    if (history.available && history.selection_commits < 3) uncertainties.push('Fewer than 3 sampled ordinary commits touch this file; historical evidence is sparse.');
    if (history.selection_uncommitted) uncertainties.push('The selected file has uncommitted changes; Git history describes committed versions, and graph freshness is unverified.');
    if (history.worktree_status_known !== true) uncertainties.push('Working-tree status of the selection could not be established.');
    const next = [
        'Inspect the nearest dependent paths and check the interfaces used by their callers or importers.',
        graph.test_candidates > 0
            ? `Review and run the ${graph.test_candidates} indexed test candidate${graph.test_candidates === 1 ? '' : 's'} that reach the selection. Test-file classification is heuristic.`
            : 'No test dependency was found within this walk. Search for integration tests and add a focused regression check; this is not a coverage measurement.',
        'Compare the local change with the indexed source and reindex before relying on absence of downstream findings.',
    ];
    if (history.cochanges.length > 0) next.push('Inspect the linked local commit evidence for repeated co-changes; shared commits alone do not establish a dependency.');
    return { level, label: level === 'high' ? 'Broad impact · high review priority'
        : level === 'elevated' ? 'Review needed · elevated change risk' : 'Risk unresolved', reasons, uncertainties, next };
}

export function evidenceTarget(node: ImpactEvidenceNode): SelectionImpactTarget {
    return { filePath: node.file_path, name: node.name, id: node.id,
        qualifiedName: node.qualified_name, line: node.line };
}

/** Display-only local command; quote even shell metacharacters in Git filenames. */
export function localCommitCommand(hash: string, ...paths: string[]): string {
    if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(hash)) return 'Commit hash unavailable';
    const quote = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;
    return `git show ${hash} -- ${paths.map(quote).join(' ')}`;
}

export async function fetchSelectionImpact(project: string, target: SelectionImpactTarget,
    signal?: AbortSignal, refresh = false, doFetch: typeof fetch = globalThis.fetch): Promise<SelectionImpactReply> {
    const query = new URLSearchParams({ project, file: target.filePath });
    if (target.qualifiedName) query.set('node', target.qualifiedName);
    else if (target.id !== undefined) query.set('node', `#${target.id}`);
    if (refresh) query.set('refresh', '1');
    const response = await doFetch(`/api/impact-analysis?${query}`, { signal });
    if (!response.ok) throw new Error(`Impact analysis returned HTTP ${response.status}`);
    const reply = readSelectionImpact(await response.json());
    if (reply.status === 'ready' && (reply.project !== project || reply.file_path !== target.filePath))
        throw new Error('Impact response belongs to a different selection');
    return reply;
}
