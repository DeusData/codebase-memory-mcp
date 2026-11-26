import { expect, it } from 'vitest';
import { galaxyScopeEvidence, selectionEvidenceContext } from '../galaxy/selection-evidence';
import type { GraphScope } from '../galaxy/graph-scope';
import type { GraphEdge, GraphNode } from '../galaxy/types';
import { chatTopic } from './chat-context';
import { readGalaxyEvidence, relationshipLine, selectionSentence } from './galaxy-evidence';
import { relationshipAnswer } from './relationship-answer';
import { relationshipWords } from './strings';

/*
 * Hand test 2026-10-04, round 4 (N1): the chat took the name of the Branch
 * node from the index, "DETACHED", and listed it beside .github as if it were
 * a folder. Its evidence and listed answers name it for what it is, in the
 * language of the answer. The snapshot keeps the names of the index, so the
 * chat still matches a typed "DETACHED" as before.
 */
const node = (id: number, extra: Partial<GraphNode>): GraphNode => ({ id, x: 0, y: 0, z: 0, size: 1, color: '#999', label: 'Folder', name: `n${id}`, ...extra });
const project = node(1, { label: 'Project', name: 'django-demo', qualified_name: 'django-demo' });
const branch = node(2, { label: 'Branch', name: 'DETACHED', qualified_name: 'django-demo.__branch__.detached' });
const github = node(4, { name: '.github', qualified_name: 'django-demo..github', file_path: '.github' });
const security = node(5, { label: 'File', name: 'SECURITY.md', qualified_name: 'django-demo..github.SECURITY.md.__file__', file_path: '.github/SECURITY.md' });
const edges: GraphEdge[] = [{ source: 1, target: 2, type: 'HAS_BRANCH' }, { source: 2, target: 4, type: 'CONTAINS_FOLDER' }, { source: 4, target: 5, type: 'CONTAINS_FILE' }];
const context = (root: GraphNode, identity: GraphScope) => selectionEvidenceContext(galaxyScopeEvidence({ project: 'django-demo', identity,
    nodes: [project, branch, github, security], edges, roots: new Set([root.id]), depth: 1, direction: 'both', edgeTypes: 'all', state: 'complete-indexed-scope', exhausted: false }));
const githubContext = () => context(github, { kind: 'node', id: 4, name: '.github', qualifiedName: 'django-demo..github' });
const branchContext = () => context(branch, { kind: 'node', id: 2, name: 'DETACHED', qualifiedName: 'django-demo.__branch__.detached' });

it('N1: the related Branch node of .github is listed by its shown name, in English and in German', () => {
    const evidence = readGalaxyEvidence(githubContext().text)!;
    const [group] = evidence.relationships.incoming;
    expect(relationshipLine(group!, 'incoming', Infinity, relationshipWords.en, true).text).toBe('- **CONTAINS_FOLDER (1):** `django-demo · detached HEAD` (Branch)');
    expect(relationshipLine(group!, 'incoming', Infinity, relationshipWords.de, true).text).toBe('- **CONTAINS_FOLDER (1):** `django-demo · detached HEAD` (Branch-Knoten)');
});

it('N1: a selected Branch node is named for what it is in the evidence, the topic and the listed answer', () => {
    const graph = branchContext();
    expect(graph.label).toBe('django-demo · detached HEAD');
    const evidence = readGalaxyEvidence(graph.text)!;
    // The identity and the names the chat matches stay those of the index.
    expect(evidence.label).toBe('DETACHED');
    expect(evidence.roots[0]).toMatchObject({ name: 'DETACHED', kind: 'Branch', qualifiedName: 'django-demo.__branch__.detached' });
    expect(selectionSentence(evidence, relationshipWords.en)).toEqual(['Selected: django-demo · detached HEAD (Branch).']);
    expect(selectionSentence(evidence, relationshipWords.de)).toEqual(['Ausgewählt: django-demo · detached HEAD (Branch-Knoten).']);
    expect(chatTopic('django-demo:galaxy', { graph })?.label).toBe('django-demo · detached HEAD');
    // A typed "DETACHED" still names the selection; the answer names it as the Galaxy does.
    const english = relationshipAnswer('Who calls DETACHED?', [graph])!;
    expect(english.markdown).toContain('No CALLS edge reaches `django-demo · detached HEAD` in this scope.');
    expect(english.markdown).not.toContain('`DETACHED`');
    const german = relationshipAnswer('Wer ruft DETACHED auf?', [graph])!;
    expect(german.markdown).toContain('`django-demo · detached HEAD`');
    expect(german.markdown).not.toContain('`DETACHED`');
});

it('N1: the working tree of cbm and a named branch read the same way', () => {
    const words = relationshipWords.de.nodeNames;
    // The German answer names the node as the Galaxy label does, so it can be found in the picture.
    expect(words.workingTree).toBe('working tree');
    expect(words.branch('main')).toBe('Branch main');
    expect(relationshipWords.en.nodeNames.workingTree).toBe('working tree');
});
