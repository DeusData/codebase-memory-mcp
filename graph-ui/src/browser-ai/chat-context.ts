import type { BrowserChatAttachment, BrowserChatContext, BrowserChatReaderContext, BrowserChatTurn } from './chat-model';
import { readGalaxyEvidence, selectionName } from './galaxy-evidence';
import { browserChatContextText, relationshipWords } from './strings';

/** What a question is about: the open file, the selected graph item, attached code or
 * attached context, within one project and view. Turns about another one are another
 * topic; their answers are not sent along (K17). */
export interface ChatTopic { key: string; label: string; kind: 'file' | 'graph' | 'attachment' | 'context' }

export function chatTopic(scope: string, sources: { reader?: BrowserChatReaderContext; graph?: BrowserChatContext; attachment?: BrowserChatAttachment;
    context?: readonly BrowserChatContext[] }): ChatTopic | undefined {
    const { reader, graph, attachment, context = [] } = sources;
    if (reader?.source) return { kind: 'file', label: reader.source.path, key: JSON.stringify([scope, 'file', reader.source.path]) };
    if (graph) {
        // The selected item itself; depth, direction and edge types only redraw its scope.
        const galaxy = readGalaxyEvidence(graph.text);
        // Named as the Galaxy names it, a Branch node "django-demo · detached HEAD" (round 4, N1); the key stays the identity.
        return { kind: 'graph', label: galaxy ? selectionName(galaxy, relationshipWords.en) : graph.label, key: JSON.stringify([scope, 'graph', galaxy ? galaxy.identity : graph.label]) };
    }
    if (attachment) return { kind: 'attachment', label: attachment.path, key: JSON.stringify([scope, 'attachment', attachment.path]) };
    if (context.length) return { kind: 'context', label: context.map(item => item.label).join(', '), key: JSON.stringify([scope, 'context', ...context.map(item => item.id)]) };
    return undefined;
}

/** The earlier turns a question about `topic` carries: its own, also from before the reader
 * went to another file or selection and came back, and never those of another topic (K17).
 * Coming back started fresh, so "Und was noch?" got "Ja, und noch." (B4). Replies without a
 * topic (no context) belong to none. */
export function topicHistory<T extends BrowserChatTurn>(turns: readonly T[], topic: ChatTopic | undefined): T[] {
    return topic ? turns.filter(turn => turn.topic?.key === topic.key) : [];
}

/** The divider above turn `index`: "new" for the first turn about another topic, "back" for
 * a topic that has earlier turns, nothing within a topic (B4). */
export function topicDivider(turns: readonly BrowserChatTurn[], index: number): 'new' | 'back' | undefined {
    const topic = turns[index]?.topic;
    if (!topic || index === 0) return undefined;
    const earlier = turns.slice(0, index);
    if ([...earlier].reverse().find(item => item.topic)?.topic?.key === topic.key) return undefined;
    return earlier.some(item => item.topic?.key === topic.key) ? 'back' : 'new';
}

/** Follow-ups that ask nothing of their own: they continue the answer before (B4). */
const FOLLOW_UPS = new Set(['und', 'and', 'mehr', 'more', 'noch mehr', 'und mehr', 'and more', 'mehr davon', 'more of that', 'mehr details', 'more details', 'details',
    'was noch', 'und was noch', 'was sonst', 'und sonst', 'sonst', 'sonst noch was', 'sonst noch etwas', 'und sonst noch was', 'noch was', 'noch etwas', 'und noch',
    'weiter', 'und weiter', 'und dann', 'erzähl mehr', 'erzähl mir mehr', 'warum', 'wieso', 'weshalb', 'und warum',
    'what else', 'and what else', 'anything else', 'else', 'and then', 'then', 'continue', 'go on', 'tell me more', 'why', 'and why', 'how so']);

/** A short follow-up that needs the turns before it: "und", "mehr", "und was noch?", "and?", "more". */
export function contextFreeFollowUp(prompt: string): boolean {
    return FOLLOW_UPS.has(prompt.toLowerCase().replace(/[?!.,;:…]+/g, ' ').replace(/\s+/g, ' ').trim());
}

/** A question without its own context follows up on explicitly attached code or context
 * in the same view. A selection or file that is gone leaves nothing to follow up on. */
export function followedTopic(turns: readonly BrowserChatTurn[], scope: string): ChatTopic | undefined {
    for (let index = turns.length - 1; index >= 0; index--) {
        const topic = turns[index].topic;
        if (!topic || turns[index].answeredFrom === 'local' || turns[index].answeredFrom === 'hint') continue;
        // Stored history is data: a damaged key ends the follow-up instead of the send.
        let turnScope: unknown;
        try { [turnScope] = JSON.parse(topic.key) as unknown[]; } catch { return undefined; }
        return turnScope === scope && (topic.kind === 'attachment' || topic.kind === 'context') ? topic : undefined;
    }
    return undefined;
}

/** German when the question reads German; the chat's own replies follow the question.
 * "was" and "die" are English words too and decide nothing on their own. Short questions
 * with typos ("was mcht die klasse") and greetings ("hallo") read German too (C4), and so do
 * follow-ups such as "mehr" (B4). */
const GERMAN = /[äöüß]|\b(?:ich|du|der|das|und|ist|nicht|wie|wer|wo|warum|wieso|weshalb|kannst|kann|mir|mich|diese[rsnm]?|dise[rsnm]?|datei|sagen|erkl\w*|zeig\w*|welche\w*|gibt|wird|macht|mach|mcht|tut|klasse|klase|funktion|methode|modul|hallo|moin|servus|danke|bitte|kurz\w*|antwort\w*|sehr|ein|eine[rsnm]?|sind|noch|auch|hier|zeile\w*|genau|beschreib\w*|detailliert\w*|ausf\w*|mehr|weiter|sonst|dann)\b/i;
export function questionLanguage(prompt: string): 'en' | 'de' {
    return GERMAN.test(prompt) ? 'de' : 'en';
}

/** Said instead of asking the model when a question has nothing to stand on: no
 * selection, no open file, no attached code (K11). */
export function missingContextAnswer(prompt: string, reader?: BrowserChatReaderContext): string {
    const text = browserChatContextText[questionLanguage(prompt)];
    const reason = !reader ? text.nothingSelected : reader.status === 'empty' || !reader.path ? text.noFileOpen : text.sourceUnavailable(reader.path);
    return `${reason}\n\n_${text.notAsked}_`;
}
