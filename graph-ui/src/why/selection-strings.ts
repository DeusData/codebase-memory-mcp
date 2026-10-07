/**
 * The words of the selection context where it reads a Galaxy scope (hand test
 * K13). Every sentence names where the relationships come from and what the
 * scope leaves out; nothing here claims completeness the scope does not have.
 */
const byType = (types: readonly [string, number][]) => types.map(([type, count]) => `${type} ${count.toLocaleString()}`).join(' · ');

export const selectionScopeText = {
    selectSomething: 'Select a file or symbol to inspect its indexed connections.',
    readSource: 'Read source evidence',
    incoming: (count: number, types: readonly [string, number][]) =>
        `Incoming relationships · ${count.toLocaleString()}${types.length ? ` (${byType(types)})` : ''}`,
    outgoing: (count: number, types: readonly [string, number][]) =>
        `Outgoing relationships · ${count.toLocaleString()}${types.length ? ` (${byType(types)})` : ''}`,
    fromScope: (depth?: number) => `From the loaded Galaxy scope${depth ? ` (${depth === 1 ? '1 layer' : `${depth} layers`})` : ''}, every indexed relationship type.`,
    fromSnapshot: 'From the repository map snapshot. It is capped and can miss relationships that the Galaxy scope shows; select the symbol in Galaxy to load them.',
    loading: 'The scope is still loading: relationships may be missing until it is complete.',
    partial: (layer: number, limit: 'nodes' | 'edges') => `Layer ${layer} of this scope stopped at the render limit (${limit}). `
        + (layer > 1 ? 'The direct relationships of the root are complete; deeper ones may be missing.' : 'The direct relationships of the root are incomplete.'),
    notTraced: (direction: 'inbound' | 'outbound') =>
        `${direction === 'outbound' ? 'Incoming' : 'Outgoing'} relationships are not traced in this scope (${direction === 'outbound' ? 'outgoing' : 'incoming'} only).`,
    typesOnly: (types: readonly string[]) => `Only these relationship types are traced: ${types.length ? types.join(', ') : 'none'}.`,
};
