import { nodeDisplayName } from '../galaxy/node-names';
import type { RelationshipGroup } from '../galaxy/selection-evidence';
import { scopeParts, selectionName, sideLoaded, type GalaxyEvidence } from './galaxy-evidence';
import { relationshipWords, viewText, type RelationshipWords } from './strings';

/** The answer to a question about the current Galaxy view, listed from the loaded scope (H1).
 *
 * "kkannst du mir die heirarchie erklären" with .github in the hierarchy went to the free
 * model, which answered "Ich kann dir die Heirarchy erklären.". The scope holds the answer:
 * what is in the middle, what comes in (left in the hierarchy) and what goes out (right),
 * each relationship type with its count and names, how far the scope reaches, and how the
 * picture is read. Names are bounded per type and the rest counted, as in the listed answers. */

const NAMES_PER_TYPE = 8;
const TYPES_PER_SIDE = 6;
const quote = (value: string) => `\`${value.replace(/`/g, "'")}\``;

/** "- **CONTAINS_FILE (4):** `CODE_OF_CONDUCT.md`, `FUNDING.yml`; +2 more". */
function typeLine(group: RelationshipGroup, words: RelationshipWords): string {
    // Named as the Galaxy names them: the branch node as "django-demo · detached HEAD", not "DETACHED" (K47).
    const shown = group.files.flatMap(file => file.symbols.map(symbol => nodeDisplayName(symbol, words.nodeNames))).slice(0, NAMES_PER_TYPE);
    const rest = group.count - shown.length;
    const names = shown.length ? `${shown.map(quote).join(', ')}${rest > 0 ? `; ${words.more(rest)}` : ''}` : words.more(group.count);
    return `- **${words.typeCount(group.type, group.count)}:** ${names}`;
}
const totals = (items: readonly { type: string; count: number }[]) => items.slice(0, TYPES_PER_SIDE).map(item => `${item.type} ${item.count}`).join(', ');

export function viewAnswer(evidence: GalaxyEvidence, language: 'en' | 'de'): string {
    const words = relationshipWords[language], text = viewText[language];
    const hierarchy = evidence.display === 'hierarchy';
    const [first] = evidence.roots;
    const single = evidence.rootCount <= 1 && first !== undefined;
    const center = text.center(quote(single ? nodeDisplayName(first, words.nodeNames) : selectionName(evidence, words)), words.kindName((single ? first.kind : undefined) ?? evidence.selectionKind), single ? 1 : evidence.rootCount);
    const { direction, types, state } = scopeParts(evidence, words);
    const reach = evidence.depth === 0 ? text.selectionOnly : text.reach(words.hops(evidence.depth), direction, evidence.direction === 'both');
    const side = (name: 'incoming' | 'outgoing'): string => {
        if (!sideLoaded(evidence, name)) return name === 'incoming' ? words.incomingNotLoaded : words.outgoingNotLoaded;
        const groups = evidence.relationships[name];
        if (!groups.length) return evidence.truncated ? words.cut(name) : name === 'incoming' ? words.noIncoming : words.noOutgoing;
        const lines = groups.slice(0, TYPES_PER_SIDE).map(group => typeLine(group, words));
        if (groups.length > TYPES_PER_SIDE) lines.push(`- ${words.moreTypes(groups.length - TYPES_PER_SIDE)}`);
        return `${text.side(name, hierarchy)}\n${lines.join('\n')}`;
    };
    const { internal, beyond } = evidence.relationships;
    // The picture is read in the view the reader has; where the evidence does not say which, nothing is said about it.
    const reading = evidence.display === 'hierarchy' ? text.readHierarchy(evidence.direction, evidence.depth >= 2) : evidence.display === 'galaxy' ? text.readGalaxy : undefined;
    return [
        `${center}; ${reach}.`,
        ...evidence.depth === 0 ? [words.notExpanded] : [side('incoming'), side('outgoing')],
        ...internal.length ? [words.internal(totals(internal))] : [],
        ...beyond.length ? [words.beyond(totals(beyond))] : [],
        ...reading ? [reading] : [],
        text.size(words.size(evidence.nodes, evidence.edges), types, state),
        ...evidence.state === 'loading' ? [words.stillLoading] : [],
        `_${words.listedFromGraph}_`,
    ].join('\n\n');
}
