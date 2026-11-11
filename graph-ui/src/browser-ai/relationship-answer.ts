import { identifierNamesIn, mentionsIn, quotedNamesIn } from '../compiler/question-classifier';
import type { BrowserChatContext } from './chat-model';
import type { RelationshipGroup } from '../galaxy/selection-evidence';
import { readGalaxyEvidence, relationshipLine, scopeSentence, selectionName, sideLoaded, type GalaxyEvidence } from './galaxy-evidence';
import { relationshipWords, type RelationshipWords } from './strings';

type Side = 'incoming' | 'outgoing';
export interface RelationshipQuestion {
    sides: Side[];
    language: 'en' | 'de';
    /** The symbol named where the question puts its subject; undefined for "it", "this" or none. */
    subject?: string;
}

const KIND = 'function|method|class|symbol|file|module|funktion|methode|klasse|datei|modul';
/** Words people put between the verb and the name: "wer ruft eigentlich X auf". */
const FILLER = 'eigentlich|denn|überhaupt|genau|alles|actually|really|exactly';
/** Where a pattern says {name}: optional fillers, an optional article and kind noun, the name as typed, an optional kind noun. */
const NAME = String.raw`(?:(?:${FILLER})\s+)*(?:(?:the|a|an|der|die|das|den|dem|des)\s+)?(?:(?:${KIND})\s+)?[@\x60'"]?([A-Za-z_][\w.:/]*)[\x60'"]?(?:\s+(?:${KIND})\b)?`;
const pattern = (source: string) => new RegExp(source.replace('{name}', NAME), 'iu');
/** One word between the name and the verb ("wird JSONBAgg überall aufgerufen"), umlauts included. */
const WORD = String.raw`(?:[\p{L}\w]+\s+)?`;
/** Where a German question asks from where; never its subject. */
const WHERE = String.raw`(?:wo|woher|von\s+wo(?:\s+aus)?|von\s+wem)`;

/** Phrasings that fix the direction and, where it is written, the subject. */
const PHRASINGS: { side: Side; pattern: RegExp }[] = [
    ...[
        String.raw`\bwho\s+(?:else\s+)?(?:calls?|invokes?|uses)\s+{name}`,
        String.raw`\bwho\s+(?:is|are)\s+(?:calling|invoking|using)\s+{name}`,
        String.raw`\bwhat\s+(?:calls|invokes)\s+{name}`,
        String.raw`\b(?:which|what)\s+\w+\s+(?:calls?|invokes?)\s+{name}`,
        String.raw`\bcall(?:ers?|\s+sites?)\s+(?:of|for|to)\s+{name}`,
        String.raw`\bwhere\s+(?:is|are|does|do)\s+{name}\s+(?:\w+\s+)?(?:called|invoked|used)\b`,
        String.raw`\b(?:is|are)\s+{name}\s+(?:called|invoked|used)\s+by\b`,
        String.raw`\bwer\s+(?:ruft|rufen|benutzt|benutzen|verwendet|verwenden|nutzt|nutzen)\s+{name}`,
        String.raw`\bwer\s+(?:hat|hatte)\s+{name}\s+${WORD}(?:aufgerufen|benutzt|verwendet|genutzt)\b`,
        // The name first, the question word after it: "JSONBAgg wird wo (überall) aufgerufen", "... aufgerufen von wem".
        String.raw`^\s*{name}\s+(?:wird|werden)\s+(?:überall\s+)?${WHERE}(?:\s+überall)?\s+${WORD}(?:aufgerufen|benutzt|verwendet|genutzt)\b`,
        String.raw`^\s*{name}\s+(?:wird|werden)\s+${WORD}(?:aufgerufen|benutzt|verwendet|genutzt)\s+${WHERE}`,
        String.raw`\bwelche\w*\s+[\p{L}\w]+\s+(?:rufen|benutzen|verwenden|nutzen)\s+{name}`,
        String.raw`\b${WHERE}(?:\s+überall)?\s+(?:wird|werden)\s+{name}\s+${WORD}(?:aufgerufen|benutzt|verwendet|genutzt)\b`,
        String.raw`\bvon\s+wem\s+(?:wird|werden)\s+{name}`,
        String.raw`\baufrufer\s+(?:von|für)\s+{name}`,
        String.raw`\bwird\s+{name}\s+${WORD}aufgerufen\b`,
        String.raw`^\s*{name}\s+(?:is|are|gets|get)\s+(?:called|invoked|used)\s+(?:by\s+(?:whom|who|what|which)|from\s+where|where)\b`,
    ].map(source => ({ side: 'incoming' as const, pattern: pattern(source) })),
    ...[
        String.raw`\bwhat\s+(?:does|do|did|will|can)\s+{name}\s+(?:call|invoke|use)\b`,
        String.raw`\b(?:which|what)\s+\w+\s+(?:does|do|did|will|can)\s+{name}\s+(?:call|invoke|use)\b`,
        String.raw`\bcallees?\s+(?:of|for)\s+{name}`,
        String.raw`\bcalled\s+by\s+{name}`,
        String.raw`\bwas\s+ruft\s+{name}`,
        String.raw`\b(?:wird|werden)\s+von\s+{name}\s+(?:\w+\s+)?aufgerufen\b`,
    ].map(source => ({ side: 'outgoing' as const, pattern: pattern(source) })),
];
/** "Welche Funktion ruft X auf" asks for callers; with a plural noun, "ruft" has X as subject. */
const WHICH_CALLS = pattern(String.raw`\b(welche[rsnm]?)\s+([\p{L}\w]+)\s+ruft\s+{name}`);
/** Direction words without a written subject. */
const GENERAL: { side: Side; pattern: RegExp }[] = [
    { side: 'incoming', pattern: /\b(?:callers?|call\s+sites?|who\s+(?:calls|invokes|uses)|what\s+calls|wer\s+ruft|von\s+wem|aufrufer)\b/i },
    { side: 'outgoing', pattern: /\b(?:callees?|was\s+ruft)\b/i },
];
/** Why, how, what-if and explain questions want reasoning, not a list; "how many" still asks for one. */
const REASONING = /\b(?:why|how(?!\s+(?:many|often))|explain\w*|describe\w*|summar\w*|would|could|should|if|when|rename\w*|break\w*|impact\w*|warum|wieso|weshalb|wie(?!\s+(?:viele|oft))|erkl(?:ä|ae)r\w*|beschreib\w*|fasse|zusammenfass\w*|passiert|wäre|waere|würde|wuerde|könnte|koennte|sollte|wenn|falls|umbenenn\w*|auswirkung\w*)\b/iu;
const GERMAN = /\b(?:wer|welche\w*|ruft|rufen|ruf|aufruf\w*|aufgerufen|wem|wird|werden)\b/i;
/** Subjects that mean the current selection. */
const SELF = new Set(['it', 'this', 'that', 'these', 'those', 'here', 'es', 'dies', 'diese', 'dieser', 'dieses', 'diesen', 'diesem', 'sie', 'ihn',
    'function', 'method', 'class', 'symbol', 'file', 'module', 'selection', 'funktion', 'methode', 'klasse', 'datei', 'modul', 'auswahl']);
/** Words a {name} can land on that are not a subject at all; the match is discarded. */
const NOT_A_NAME = new Set(['in', 'into', 'from', 'at', 'on', 'to', 'for', 'with', 'of', 'and', 'or', 'by', 'anything', 'anyone', 'something', 'someone',
    'what', 'which', 'who', 'whom', 'where', 'when', 'all', 'every', 'auf', 'von', 'im', 'aus', 'und', 'oder', 'irgendwo', 'überhaupt', 'überall', 'etwas', 'jemand',
    'was', 'wer', 'wem', 'wen', 'wo', 'woher', 'wohin', 'wann', 'alle', 'jede', 'jeder', 'jedes']);

/** Relation words a typo is corrected to. */
const RELATION_WORDS = ['ruft', 'rufen', 'aufgerufen', 'aufrufer', 'benutzt', 'verwendet', 'calls', 'call', 'callers', 'caller', 'called', 'calling',
    'callees', 'callee', 'invokes', 'invoked'];
/** Short forms that mean a relation word; words under four letters are not corrected otherwise. */
const VARIANTS: Readonly<Record<string, string>> = { ruf: 'ruft', rufe: 'ruft', rufst: 'ruft', rft: 'ruft', uft: 'ruft', cal: 'call' };
/** Real words one edit away from a relation word; they are never "corrected". */
const OWN_WORDS = new Set(['falls', 'fall', 'cells', 'cell', 'halls', 'hall', 'walls', 'wall', 'balls', 'ball', 'tall', 'all', 'mall', 'gall', 'calm', 'calf',
    'calms', 'calmed', 'falling', 'calming', 'rust', 'raft', 'rift', 'luft', 'duft', 'ruht', 'auf', 'aufruf', 'aufrufe',
    'aufrufen', 'rufen', 'gerufen', 'cache', 'caches']);

/** Optimal string alignment distance of at most `limit`. */
export function withinEdits(left: string, right: string, limit: number): boolean {
    if (Math.abs(left.length - right.length) > limit) return false;
    const rows = [Array.from({ length: right.length + 1 }, (_, index) => index)];
    for (let i = 1; i <= left.length; i++) {
        const row = [i];
        for (let j = 1; j <= right.length; j++) {
            const cost = left[i - 1] === right[j - 1] ? 0 : 1;
            row[j] = Math.min(rows[i - 1][j] + 1, row[j - 1] + 1, rows[i - 1][j - 1] + cost);
            if (i > 1 && j > 1 && left[i - 1] === right[j - 2] && left[i - 2] === right[j - 1]) row[j] = Math.min(row[j], rows[i - 2][j - 2] + 1);
        }
        rows.push(row);
    }
    return rows[left.length][right.length] <= limit;
}
const withinOneEdit = (left: string, right: string) => withinEdits(left, right, 1);

/** "wer ruf X auf", "who cals X", "aufgerufn": a relation word with one typo reads as the word. */
export function correctRelationWords(prompt: string): string {
    return prompt.replace(/\p{L}+/gu, word => {
        const lower = word.toLowerCase();
        if (VARIANTS[lower]) return VARIANTS[lower];
        if (lower.length < 4 || OWN_WORDS.has(lower) || RELATION_WORDS.includes(lower)) return word;
        return RELATION_WORDS.find(candidate => withinOneEdit(lower, candidate)) ?? word;
    });
}

/** Caller and callee questions that ask for a list, in English or German. Anything else goes to the model. */
export function relationshipQuestion(typed: string): RelationshipQuestion | undefined {
    if (REASONING.test(typed)) return undefined;
    const prompt = correctRelationWords(typed);
    const sides = new Set<Side>();
    let subject: string | undefined, named = false;
    const take = (side: Side, name: string | undefined): void => {
        const word = name?.replace(/[.:/]+$/, '');
        if (!word || NOT_A_NAME.has(word.toLowerCase())) return;
        sides.add(side);
        if (!SELF.has(word.toLowerCase()) && !subject) subject = word;
        named = true;
    };
    for (const phrasing of PHRASINGS) take(phrasing.side, phrasing.pattern.exec(prompt)?.[1]);
    const which = WHICH_CALLS.exec(prompt);
    if (which) take(which[1].toLowerCase() === 'welche' && /(?:en|[^s]s)$/iu.test(which[2]) ? 'outgoing' : 'incoming', which[3]);
    if (!named) for (const general of GENERAL) if (general.pattern.test(prompt)) sides.add(general.side);
    if (!sides.size) return undefined;
    return { sides: (['incoming', 'outgoing'] as const).filter(side => sides.has(side)), language: GERMAN.test(prompt) ? 'de' : 'en',
        ...subject ? { subject } : {} };
}

/** The names the selection goes by: its label, the last part of a path, its root symbols. */
const selectionNames = (evidence: GalaxyEvidence): string[] => [...new Set([evidence.label, evidence.label.split('/').pop()!, ...evidence.roots.map(root => root.name)])];
/** Names of the other symbols in the loaded scope: a word that is one of them names that symbol, not a typo. */
const neighbourNames = (evidence: GalaxyEvidence): Set<string> => new Set([...evidence.relationships.incoming, ...evidence.relationships.outgoing]
    .flatMap(group => group.files.flatMap(file => file.symbols.map(symbol => symbol.name.toLowerCase()))));
/** How many typos a name of this length may carry and still mean it: two from seven letters on, one from four. */
export const typoBudget = (name: string) => name.length >= 7 ? 2 : name.length >= 4 ? 1 : 0;
/** "jsonbgg", "JSONBAg": the selection's name with up to two typos, in any case (K16). An exact
 * name of another symbol in the scope is that symbol. */
function nearSelection(word: string, evidence: GalaxyEvidence): boolean {
    const last = word.toLowerCase().split(/[.:]/).pop()!;
    if (neighbourNames(evidence).has(last) || NOT_A_NAME.has(last) || SELF.has(last) || RELATION_WORDS.includes(last) || OWN_WORDS.has(last)) return false;
    return selectionNames(evidence).some(name => { const own = name.toLowerCase(); return own !== last && withinEdits(last, own, typoBudget(own)); });
}

/** A question about another symbol is not about this selection. Without a written
 * subject, any symbol-shaped word in the question still counts as naming one. */
function aboutSelection(prompt: string, question: RelationshipQuestion, evidence: GalaxyEvidence): boolean {
    const selected = new Set(selectionNames(evidence).map(name => name.toLowerCase()));
    const matches = (name: string) => selected.has(name.toLowerCase()) || selected.has(name.split(/[.:]/).pop()!.toLowerCase());
    if (question.subject) return matches(question.subject);
    const snake = prompt.match(/\b[A-Za-z]\w*_\w+\b/g) ?? [];
    const named = [...mentionsIn(prompt), ...quotedNamesIn(prompt), ...identifierNamesIn(prompt), ...snake];
    return !named.length || named.some(matches);
}

const quote = (value: string) => `\`${value.replace(/`/g, "'")}\``;

/** Kinds whose incoming DEFINES edge is where the selection is defined, not a use of it. */
const DEFINERS = new Set(['file', 'module', 'class', 'interface']);
/** One edge type of a listed answer. An incoming DEFINES from a file names it as the file that
 * defines the selection: "`general.py`, the file that defines `JSONBAgg`" (C2). */
function listedLine(group: RelationshipGroup, side: Side, words: RelationshipWords, name: string): string {
    const symbols = group.files.flatMap(file => file.symbols);
    const kinds = new Set(symbols.map(symbol => symbol.kind));
    const [kind] = kinds;
    if (side === 'incoming' && /^DEFINES/.test(group.type) && kinds.size === 1 && kind && DEFINERS.has(kind.toLowerCase())) {
        const more = group.count > symbols.length ? `; ${words.more(group.count - symbols.length)}` : '';
        return `- **${words.typeCount(group.type, group.count)}:** ${symbols.map(symbol => quote(symbol.name)).join(', ')}, ${words.definer(kind, group.count, name)}${more}`;
    }
    return relationshipLine(group, side, Infinity, words, true).text;
}

/** Callers are the CALLS edges: they come first with their count. Every other edge type of
 * the same direction follows under its own heading, so a test or the defining file never
 * reads as a caller (C2). */
function listed(question: RelationshipQuestion, evidence: GalaxyEvidence): string {
    const words = relationshipWords[question.language];
    // The selection as the Galaxy names it: a Branch node is "django-demo · detached HEAD" (round 4, N1).
    const name = quote(selectionName(evidence, words));
    const sections: string[] = [];
    for (const side of question.sides) {
        const unlisted = words.unlisted(side, name);
        if (evidence.depth === 0) { sections.push(`${unlisted} ${words.notExpanded}`); continue; }
        if (!sideLoaded(evidence, side)) { sections.push(`${unlisted} ${words.notLoaded(side)}`); continue; }
        const groups = evidence.relationships[side];
        if (!groups.length) { sections.push(evidence.truncated ? `${unlisted} ${words.cutListed(side)}` : words.nothing(side, name)); continue; }
        const symbols = side === 'incoming' ? evidence.relationships.incomingSymbols : evidence.relationships.outgoingSymbols;
        const total = groups.reduce((sum, group) => sum + group.count, 0);
        const calls = groups.find(group => group.type === 'CALLS');
        const others = groups.filter(group => group !== calls);
        // The heading counts the callers it lists; all relationships of the side follow apart (W3).
        sections.push(calls ? `${side === 'incoming' ? words.callers(name, calls.count) : words.callees(name, calls.count)}${others.length ? ` ${words.all(side, total, symbols)}` : ''}`
            : `${words.noCalls(name, side)} ${side === 'incoming' ? words.incoming(total, symbols) : words.outgoing(total, symbols)}`);
        if (calls) sections.push(listedLine(calls, side, words, name));
        if (others.length) sections.push(`${words.otherRelationships(side)}\n${others.map(group => listedLine(group, side, words, name)).join('\n')}`);
    }
    sections.push(scopeSentence(evidence, words));
    if (evidence.state === 'loading') sections.push(words.stillLoading);
    sections.push(`_${words.listedFromGraph}_`);
    return sections.join('\n\n');
}

/** Words that sound like a relationship question even where no phrasing is recognized. */
const RELATION_HINT = /\b(?:ruft|ruf|rufen|aufruf\w*|aufgerufen|calls?|callers?|called|callees?|invokes?|invoked)\b/iu;

export interface RelationshipSuggestion { markdown: string; question: string; context: BrowserChatContext; language: 'en' | 'de' }

/** "Did you mean: callers of JSONBAgg?" with the question that lists them. */
function suggestionFor(sides: readonly Side[], language: 'en' | 'de', evidence: GalaxyEvidence, context: BrowserChatContext, typedName?: string): RelationshipSuggestion {
    const name = evidence.label;
    const both = sides.length > 1, outgoing = !both && sides[0] === 'outgoing';
    const question = language === 'de' ? both ? `Wer ruft ${name} auf und was ruft ${name} auf?` : outgoing ? `Was ruft ${name} auf?` : `Wer ruft ${name} auf?`
        : both ? `Who calls ${name} and what does ${name} call?` : outgoing ? `What does ${name} call?` : `Who calls ${name}?`;
    const text = relationshipWords[language];
    const heading = both ? text.didYouMeanBoth(quote(name)) : text.didYouMean(outgoing ? 'outgoing' : 'incoming', quote(name));
    return { markdown: `${heading}\n\n_${typedName ? text.uncertainName(quote(typedName), quote(name)) : text.uncertain}_`, question, context, language };
}

/** A question that sounds like callers or callees of the selection, but not certainly:
 * the listed answer is offered, never given in its place. The selection must be named,
 * exactly or with up to two typos ("wer ruft jsonbgg auf?", K16). */
export function relationshipSuggestion(typed: string, contexts: readonly BrowserChatContext[]): RelationshipSuggestion | undefined {
    if (REASONING.test(typed)) return undefined;
    const recognized = relationshipQuestion(typed);
    if (recognized) {
        // The question is certain, its subject is not: a misspelled selection is offered, not listed.
        const subject = recognized.subject;
        if (!subject) return undefined;
        for (const context of contexts) {
            const evidence = readGalaxyEvidence(context.text);
            if (evidence && !aboutSelection(typed, recognized, evidence) && nearSelection(subject, evidence)) return suggestionFor(recognized.sides, recognized.language, evidence, context, subject);
        }
        return undefined;
    }
    const prompt = correctRelationWords(typed);
    if (!RELATION_HINT.test(prompt)) return undefined;
    const words = prompt.toLowerCase().match(/[\p{L}\w.:/]+/gu) ?? [];
    for (const context of contexts) {
        const evidence = readGalaxyEvidence(context.text);
        if (!evidence) continue;
        const names = new Set([evidence.label, ...evidence.roots.map(root => root.name)].map(name => name.toLowerCase()));
        const selection = (word: string) => names.has(word) || names.has(word.split(/[.:]/).pop()!) || SELF.has(word) || nearSelection(word, evidence);
        const at = words.findIndex(selection);
        if (at < 0) continue;
        // The selection may take several words ("this class"); what follows them is the verb.
        let after = at + 1;
        while (after < words.length && SELF.has(words[after])) after++;
        // "X calls" / "X ruft" puts the selection first: what it calls. "calls in/from X" are its
        // own calls too, "calls to X" its callers. Otherwise its callers.
        const relation = words.findIndex(word => /^(?:ruft|calls?|invokes?)$/.test(word));
        const before = relation >= 0 && relation < at ? words[relation + 1] : undefined;
        const outgoing = !/\b(?:wer|who|aufrufer|callers?|aufrufe)\b/i.test(prompt)
            && (/^(?:ruft|calls?|invokes?)$/.test(words[after] ?? '') || /^(?:in|inside|within|from|made)$/.test(before ?? ''));
        return suggestionFor([outgoing ? 'outgoing' : 'incoming'], GERMAN.test(prompt) ? 'de' : 'en', evidence, context);
    }
    return undefined;
}

/** A complete, deterministic answer from the current Galaxy evidence, or undefined
 * when the question is not a list of callers or callees of the selection. */
export function relationshipAnswer(prompt: string, contexts: readonly BrowserChatContext[]): { markdown: string; context: BrowserChatContext; language: 'en' | 'de' } | undefined {
    const question = relationshipQuestion(prompt);
    if (!question) return undefined;
    for (const context of contexts) {
        const evidence = readGalaxyEvidence(context.text);
        if (evidence && aboutSelection(prompt, question, evidence)) return { markdown: listed(question, evidence), context, language: question.language };
    }
    return undefined;
}
