import { galaxyScopeEvidence, selectionEvidenceContext, type GalaxyScope, type SelectionEvidence } from '../galaxy/selection-evidence';
import type { GraphEdge, GraphNode } from '../galaxy/types';
import type { BrowserChatContext } from './chat-model';

/** JSONBAgg in django-demo at one layer, as the call reproduction measured it:
 * 11 tests with CALLS and TESTS, DEFINES from general.py, two outgoing INHERITS. */
export const JSONB_AGG_CALLERS = ['test_default_argument', 'test_empty_result_set', 'test_jsonb_agg', 'test_jsonb_agg_booleanfield_order_by',
    'test_jsonb_agg_charfield_order_by', 'test_jsonb_agg_distinct_false', 'test_jsonb_agg_distinct_true', 'test_jsonb_agg_integerfield_order_by',
    'test_jsonb_agg_jsonfield_order_by', 'test_jsonb_agg_key_index_transforms', 'test_values_list'];

const node = (id: number, name: string, label: string, file_path: string, start_line?: number, end_line?: number): GraphNode =>
    ({ id, name, label, file_path, start_line, end_line, qualified_name: `django-demo.${name}`, x: 0, y: 0, z: 0, size: 1, color: '#999' });

export function jsonbAggScope(): { nodes: GraphNode[]; edges: GraphEdge[]; roots: Set<number> } {
    const root = node(32360, 'JSONBAgg', 'Class', 'django/contrib/postgres/aggregates/general.py', 50, 54);
    const tests = JSONB_AGG_CALLERS.map((name, index) => node(100 + index, name, 'Method', 'tests/postgres_tests/test_aggregates.py', 200 + index * 10));
    const module = node(9, 'general.py', 'File', 'django/contrib/postgres/aggregates/general.py');
    const parents = [node(7, 'OrderableAggMixin', 'Class', 'django/contrib/postgres/aggregates/mixins.py'), node(8, 'Aggregate', 'Class', 'django/db/models/aggregates.py')];
    const edges: GraphEdge[] = [
        ...tests.flatMap(test => [{ source: test.id, target: root.id, type: 'CALLS' }, { source: test.id, target: root.id, type: 'TESTS' }]),
        { source: module.id, target: root.id, type: 'DEFINES' },
        ...parents.map(parent => ({ source: root.id, target: parent.id, type: 'INHERITS' })),
    ];
    return { nodes: [root, ...tests, module, ...parents], edges, roots: new Set([root.id]) };
}

/** The context GalaxyPanel publishes for that scope, with optional scope overrides. */
export function jsonbAggEvidence(overrides: { depth?: number; direction?: string; state?: GalaxyScope['state']; edges?: GraphEdge[]; nodes?: GraphNode[];
    renderLimit?: GalaxyScope['renderLimit'] } = {}): BrowserChatContext {
    const scope = jsonbAggScope();
    const [root] = scope.nodes;
    return selectionEvidenceContext(galaxyScopeEvidence({ project: 'django-demo', identity: { kind: 'symbol', qualifiedName: root.qualified_name!, name: 'JSONBAgg' },
        nodes: [...scope.nodes, ...overrides.nodes ?? []], edges: overrides.edges ?? scope.edges, roots: scope.roots,
        depth: overrides.depth ?? 1, direction: overrides.direction ?? 'both', edgeTypes: 'all',
        state: overrides.state ?? 'complete-indexed-scope', exhausted: false, ...overrides.renderLimit ? { renderLimit: overrides.renderLimit } : {} }));
}

/** JSONBAgg after Expand +1 twice in the hand test of 2026-10-04: layer 3 stopped at the render limit (C1). */
export function jsonbAggRenderLimited(): BrowserChatContext {
    const further = Array.from({ length: 5533 }, (_, index) => node(50_000 + index, `further_${index}`, 'Function', 'django/db/models/further.py'));
    const beyond = Array.from({ length: 15_648 }, (_, index) => ({ source: further[index % further.length].id, target: further[(index * 7 + 1) % further.length].id, type: 'CALLS' }));
    return jsonbAggEvidence({ depth: 3, state: 'render-limit-partial', renderLimit: { layer: 3, kind: 'nodes', limit: 5000 }, nodes: further, edges: [...jsonbAggScope().edges, ...beyond] });
}

/** A folder of 40 documented symbols with five incoming and five outgoing edge types of 30 symbols each. */
export function largeFolderScope(): SelectionEvidence {
    const node = (id: number, name: string, file: string, documentation?: string): GraphNode => ({ id, name, label: 'Function', qualified_name: `pkg.${file}.${name}`,
        file_path: `django/contrib/postgres/aggregates/${file}.py`, start_line: 10, end_line: 20, documentation, x: 0, y: 0, z: 0, size: 1, color: '#999' });
    const roots = Array.from({ length: 40 }, (_, index) => node(index + 1, `postgres_member_${index}`, 'members', 'd'.repeat(1500)));
    const types = ['CALLS', 'TESTS', 'USAGE', 'IMPORTS', 'DEFINES_METHOD'];
    const nodes: GraphNode[] = [...roots], edges: GraphEdge[] = [];
    types.forEach((type, typeIndex) => (['incoming', 'outgoing'] as const).forEach(side => {
        for (let index = 0; index < 30; index++) {
            const id = 1000 + typeIndex * 100 + (side === 'incoming' ? 0 : 50) + index;
            nodes.push(node(id, `${side}_${type.toLowerCase()}_relationship_${index}`, `${side}_module_${Math.floor(index / 3)}`));
            edges.push(side === 'incoming' ? { source: id, target: 1 + index, type } : { source: 1 + index, target: id, type });
        }
    }));
    return galaxyScopeEvidence({ project: 'django-demo', identity: { kind: 'folder', path: 'django/contrib/postgres', name: 'postgres' }, nodes, edges,
        roots: new Set(roots.map(root => root.id)), depth: 1, direction: 'both', edgeTypes: 'all', state: 'complete-indexed-scope', exhausted: false });
}

/** `.github` in django-demo at one layer, as the hand test of 2026-10-04 showed it in the hierarchy:
 * the branch node of the detached checkout above it, four files and the workflows folder below it. */
export function githubFolderEvidence(display?: 'galaxy' | 'hierarchy', overrides: { depth?: number; direction?: string; nodes?: GraphNode[]; edges?: GraphEdge[] } = {}): BrowserChatContext {
    const folder = (id: number, name: string, path: string): GraphNode => ({ id, name, label: 'Folder', file_path: path, qualified_name: `django-demo.${path}`, x: 0, y: 0, z: 0, size: 1, color: '#999' });
    const file = (id: number, name: string): GraphNode => ({ id, name, label: 'File', file_path: `.github/${name}`, qualified_name: `django-demo..github.${name}`, x: 0, y: 0, z: 0, size: 1, color: '#999' });
    const root = folder(1, '.github', '.github');
    const branch: GraphNode = { id: 2, name: 'DETACHED', label: 'Branch', qualified_name: 'django-demo.__branch__.detached', x: 0, y: 0, z: 0, size: 1, color: '#999' };
    const files = ['CODE_OF_CONDUCT.md', 'FUNDING.yml', 'pull_request_template.md', 'SECURITY.md'].map((name, index) => file(10 + index, name));
    const workflows = folder(20, 'workflows', '.github/workflows');
    const edges: GraphEdge[] = [{ source: branch.id, target: root.id, type: 'CONTAINS_FOLDER' }, ...files.map(item => ({ source: root.id, target: item.id, type: 'CONTAINS_FILE' })),
        { source: root.id, target: workflows.id, type: 'CONTAINS_FOLDER' }];
    return selectionEvidenceContext(galaxyScopeEvidence({ project: 'django-demo', identity: { kind: 'folder', path: '.github', name: '.github' },
        nodes: [root, branch, ...files, workflows, ...overrides.nodes ?? []], edges: [...edges, ...overrides.edges ?? []], roots: new Set([root.id]),
        depth: overrides.depth ?? 1, direction: overrides.direction ?? 'both', edgeTypes: 'all', state: 'complete-indexed-scope', exhausted: false, ...display ? { display } : {} }));
}
