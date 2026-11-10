export const projectSwitcherStrings = {
    project: 'Project',
    choose: 'Choose project',
    switchProject: (name: string) => `Switch project: ${name}`,
    search: 'Find a project',
    searchPlaceholder: 'Search projects or paths',
    loading: 'Loading projects...',
    failed: 'Could not load projects.',
    empty: 'No indexed projects yet.',
    noMatches: 'No matching projects.',
    current: 'Current',
    retry: 'Try again',
    /** Review of K42: the Refresh button says that it runs, then when it ran (shared words in ui/refresh/strings.ts). */
    refreshFeedback: { idle: 'Refresh projects', busy: 'Refreshing projects…', done: (time: string) => `Projects refreshed at ${time}` },
    add: 'Add project index',
    indexActivity: (status: 'indexing' | 'done' | 'error') => status === 'indexing' ? 'Indexing…' : status === 'done' ? 'Index ready' : 'Index needs attention',
};
