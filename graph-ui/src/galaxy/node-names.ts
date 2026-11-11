/*
 * The name a node is shown by (hand test 2026-10-04, round 4, N1).
 *
 * For every node but one kind that is its own name. The exception is the
 * Branch node: the index puts it above the top level folders and files of a
 * project, for the checkout it read (src/pipeline/pipeline.c, pass_structure).
 * Its name is the branch, "DETACHED" for a detached HEAD and "working-tree"
 * where no branch is known, and in the hierarchy of .github a bare "DETACHED"
 * read like a folder of that name. Its qualified name is
 * `<project>.__branch__.<slug>` (src/git/git_context.c,
 * cbm_git_context_branch_qn), so the project comes from there.
 *
 * Two signals, both from the index: the label Branch, and, where a caller only
 * holds a name and a qualified name (a Galaxy scope, a history entry), that
 * pattern. A known label other than Branch always wins, so a function in a
 * module called `__branch__` keeps its name.
 *
 * Only what is shown changes. Scopes, history keys and the chat snapshot keep
 * the name of the index: they are identities, and the chat matches typed
 * names against them.
 */
import type { GraphScope } from './graph-scope';
import { galaxyNodeNameText, type BranchNameWords } from './galaxy-strings';
import type { GraphNode } from './types';

const BRANCH_MARK = '.__branch__.';

/** What a node is known by: its name, and its label or kind and qualified name where the caller has them. */
export interface NodeNameParts { name: string; label?: string; kind?: string; qualifiedName?: string; project?: string }
export interface BranchNode { project?: string; name: string; state: 'detached' | 'working-tree' | 'branch' }

export function branchNodeOf(node: NodeNameParts): BranchNode | undefined {
    const label = node.label || node.kind;
    const at = node.qualifiedName?.indexOf(BRANCH_MARK) ?? -1;
    if (label ? label !== 'Branch' : at <= 0) return undefined;
    const slug = at > 0 ? node.qualifiedName!.slice(at + BRANCH_MARK.length) : undefined;
    const project = at > 0 ? node.qualifiedName!.slice(0, at) : node.project || undefined;
    /*
     * The engine names a detached HEAD "DETACHED" with the slug "detached".
     * A search hit carries no name, only the qualified name, so its name is
     * the slug ("detached"); a branch that is really called "detached" looks
     * the same and is taken for the far more common detached HEAD. The slug
     * keeps the case of the branch, so "Detached" stays a branch.
     */
    const detached = slug === undefined ? node.name === 'DETACHED' : slug === 'detached';
    const state = detached ? 'detached' : node.name === 'working-tree' && (slug === undefined || slug === 'working-tree') ? 'working-tree' : 'branch';
    return { ...project ? { project } : {}, name: node.name, state };
}

/** The shown name: "django-demo · detached HEAD" for a Branch node, the node's own name for every other. */
export function nodeDisplayName(node: NodeNameParts, words: BranchNameWords = galaxyNodeNameText): string {
    const branch = branchNodeOf(node);
    if (!branch) return node.name;
    const what = branch.state === 'detached' ? words.detached : branch.state === 'working-tree' ? words.workingTree : words.branch(branch.name);
    return branch.project ? words.inProject(branch.project, what) : what;
}

const partsOf = (node: GraphNode): NodeNameParts => ({ name: node.name, label: node.label, qualifiedName: node.qualified_name });

export function graphNodeName(node: GraphNode): string {
    return nodeDisplayName(partsOf(node));
}

/** The tooltip: the shown name, and for a Branch node what it is and its name in the index. */
export function graphNodeTitle(node: GraphNode): string {
    const shown = graphNodeName(node);
    return branchNodeOf(partsOf(node)) ? galaxyNodeNameText.branchTitle(shown, node.name, node.qualified_name) : shown;
}

/** The detail line of a Branch node: its name in the index; undefined for every other node. */
export function graphNodeIndexName(node: GraphNode): string | undefined {
    return branchNodeOf(partsOf(node)) ? galaxyNodeNameText.indexName(node.name, node.qualified_name) : undefined;
}

/**
 * The file of a node, without the "{}" the index stores as the file of a
 * Branch node (the layout and query_graph both answer with it). Taken for a
 * path, it made the node look openable: the toolbar offered to open "{}" in
 * Explore and the search listed a file called "{}".
 */
export function nodeFilePath(node: { label?: string; name: string; qualified_name?: string; file_path?: string }): string | undefined {
    if (node.file_path === '{}' && branchNodeOf({ name: node.name, label: node.label, qualifiedName: node.qualified_name })) return undefined;
    return node.file_path;
}

/** The shown name of a Galaxy scope; folders and files keep their paths. */
export function scopeDisplayName(scope: GraphScope): string {
    return scope.kind === 'node' || scope.kind === 'symbol' ? nodeDisplayName({ name: scope.name, qualifiedName: scope.qualifiedName }) : scope.name;
}

/** The tooltip of a Galaxy scope's name. */
export function scopeTitle(scope: GraphScope): string {
    const shown = scopeDisplayName(scope);
    return (scope.kind === 'node' || scope.kind === 'symbol') && shown !== scope.name ? galaxyNodeNameText.branchTitle(shown, scope.name, scope.qualifiedName) : shown;
}
