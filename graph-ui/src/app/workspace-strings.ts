import { experimentalAgentsEnabled } from './feature-flags';

export type Workspace = 'explore' | 'galaxy' | 'architecture' | 'adr' | 'agents' | 'coverage' | 'system';
export type Guidance = 'brief' | 'explained';

export const workspaceStrings = {
    navigation: 'Workspace',
    workspaces: [
        { id: 'explore' as const, label: 'Explore' },
        { id: 'galaxy' as const, label: 'Galaxy' },
        { id: 'architecture' as const, label: 'Architecture' },
        { id: 'adr' as const, label: 'ADR' },
        { id: 'agents' as const, label: 'Agents' },
        { id: 'coverage' as const, label: 'Coverage' },
        { id: 'system' as const, label: 'System' },
    ],
    tools: 'Tools',
    search: 'Search',
    searchOpen: 'Open search and commands',
    searchTitle: 'Search and commands',
    searchPlaceholder: 'Find a symbol, search by meaning, or run a command',
    searchClose: 'Close search',
    loadedSearchHeadline: (query: string, count: number) => `${count} loaded ${count === 1 ? 'match' : 'matches'} for "${query}"; no matching index results.`,
    searchShortcut: '⌘ / Ctrl K',
    searchExamples: 'Try a search or graph command',
    chatGraphHeight: 'Height of graph below chat',
    guidance: 'Explanation depth',
    brief: 'Brief',
    briefValue: 'brief',
    explained: 'Explained',
    explainedValue: 'explained',
    diagnostics: 'Connection details',
    coverage: 'Index details',
    model: 'Model details',
    perspective: 'Reading preferences',
    welcomeEyebrow: 'Codebase memory',
    welcomeTitle: 'A clearer view of your code.',
    welcomeDescription: 'Explore the source and understand its architecture.',
    descriptions: {
        explore: 'Read source with graph context.',
        galaxy: 'Explore the whole graph and its coverage shadow.',
        architecture: 'See modules, dependencies, and entry points.',
        adr: 'Read and edit the project’s architecture decisions.',
        agents: 'Inspect recorded activity and touched code.',
        coverage: 'Inspect indexed paths, exclusions, and parser gaps.',
        system: 'Monitor the daemon, indexes, and logs.',
    },
    changeLater: 'Switch workspaces at any time.',
    localAi: 'Set up browser AI',
    browserAi: 'Chat',
    daemon: 'Daemon',
    daemonNavigation: (state: string) => `Open System: daemon ${state}`,
    chatWidth: 'Width of local chat',
    askSelection: 'Ask about selection',
    assessImpact: 'Assess impact',
    codeDetails: 'Code details',
    indexInventory: 'Index inventory and legend',
    selectionImpact: 'Assess change impact',
    changeAnalysis: 'Change analysis',
    sourceOutsideSnapshot: 'This file is outside the loaded index snapshot.',
    selectionEvidence: 'Selection evidence',
    graphContext: 'Graph context',
    coverageShadowSelection: (name: string) => `Coverage shadow: ${name}`,
    start: 'Start exploring',
    aiOptional: 'Browser AI is optional and off. Setup asks before downloading a model.',
};

export function availableWorkspaces(agentsEnabled = experimentalAgentsEnabled) {
    return workspaceStrings.workspaces.filter(workspace => agentsEnabled || workspace.id !== 'agents');
}

export function allowedWorkspace(workspace: Workspace, agentsEnabled = experimentalAgentsEnabled): Workspace {
    return workspace === 'agents' && !agentsEnabled ? 'explore' : workspace;
}

/** An unavailable explicit or saved workspace falls back to the source reader. */
export function initialWorkspace(requested: string | null, saved: string | null, agentsEnabled = experimentalAgentsEnabled): Workspace {
    const known = (value: string | null): value is Workspace => workspaceStrings.workspaces.some(workspace => workspace.id === value);
    return allowedWorkspace(known(requested) ? requested : known(saved) ? saved : 'architecture', agentsEnabled);
}
