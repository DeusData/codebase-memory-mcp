import { galaxyNodeContext } from '../browser-ai/galaxy-context';
import type { BrowserChatContext } from '../browser-ai/chat-model';
import type { GraphData, GraphNode } from './types';

/** Hierarchy render IDs are local to a projection; symbol names carry identity. */
export function layoutNodeForSelection(layout: GraphData | undefined, selected: GraphNode): GraphNode | undefined {
    return selected.qualified_name
        ? layout?.nodes.find(node => node.qualified_name === selected.qualified_name)
        : layout?.nodes.find(node => node === selected);
}

/** Keep selected hierarchy evidence usable when its symbol is outside the loaded layout. */
export function selectedGraphContext(layout: GraphData | undefined, selected: GraphNode, project: string, snapshotId: string): BrowserChatContext {
    const canonical = layoutNodeForSelection(layout, selected);
    if (canonical) {
        const context = galaxyNodeContext(layout, canonical.id, project, snapshotId);
        if (context) return context;
    }
    const truncatedFields: string[] = [];
    const field = (value: string | undefined, name: string) => {
        if (value === undefined || value.length <= 256) return value;
        truncatedFields.push(name);
        return `${value.slice(0, 256)}…`;
    };
    const selectedNode = {
        name: field(selected.name, 'name'),
        qualifiedName: field(selected.qualified_name, 'qualifiedName'),
        filePath: field(selected.file_path, 'filePath'),
        startLine: selected.start_line,
        endLine: selected.end_line,
        kind: field(selected.label, 'kind'),
    };
    return {
        id: snapshotId,
        label: `Graph node · ${selected.name.slice(0, 60)}`,
        text: JSON.stringify({
            kind: 'graph-selection-snapshot',
            source: 'selected-graph-node',
            project: field(project, 'project'),
            selectedNode,
            loadedLayout: 'This symbol is outside the loaded layout; its displayed identity and source location are retained.',
            relationships: { state: 'unavailable', reason: 'The loaded layout does not contain this selected symbol.' },
            indexCoverage: { state: 'unavailable' },
            generation: { state: 'unavailable' },
            truncatedFields,
            limitations: 'Selection metadata only. No source code or relationships were fetched. Render IDs are omitted because they are not index identities. Absence from the loaded layout does not imply absent code or calls.',
        }, null, 2),
    };
}
