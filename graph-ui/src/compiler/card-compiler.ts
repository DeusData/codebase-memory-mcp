/**
 * Graph facts to numbered cards, under a hard token budget.
 *
 * This is the half of the context compiler that decides what a 1B model gets to
 * see. The recipe before it fetched more than fits; the answer contract after it
 * forbids saying anything that is not on a card. So everything that matters
 * happens here: what becomes a card, in which order, in which words, and what
 * is left out when the budget runs out.
 *
 * ## Four rules
 *
 * **A card is short, numbered and citable.** `K1`, `K2`, at most
 * {@link MAX_CARD_LINES} lines each, and every card carries the place it came
 * from so a `[K3]` in an answer can be turned into a click. A card that could
 * not name a place would be a claim a reader cannot check, which is the one
 * thing this product does not put on a screen.
 *
 * **The vocabulary is canonical, not descriptive.** Cards say `calls`,
 * `raises`, `reads`, `is called by` and nothing else, and every reference is
 * `name (path:line)` or `[line n]`. A card that varied its verbs would be the
 * compiler writing prose, and prose is what the model is for. The shape is the
 * one PLAN paragraph 6 names:
 * `K3: createUser (src/services/userService.ts:23) calls validateUser [line 24]`.
 *
 * **The budget is hard and it is in this file.** {@link CARD_BUDGET_CLASS_A} is
 * 3000 tokens for the roughly 1B class and {@link CARD_BUDGET_CLASS_B} is 8000
 * for the roughly 4B class, which are the two numbers PLAN paragraph 5 sets
 * against the two context windows llm/start.sh opens (3072 and 8192). Cards are
 * added in priority order until the next one would cross the line, and then the
 * adding stops. Nothing is trimmed in the middle of a card: half a fact is
 * worse than a missing one, because a reader cannot see that it is half.
 *
 * **What was cut is counted and said.** Every drop produces a line on the final
 * card, in the form "7 more callers not listed". The note is a card of its own
 * rather than a footer, so the model can cite it: an answer that says "there are
 * further callers I was not given [K9]" is a correct answer, and one that quietly
 * lists five of twelve is not.
 *
 * ## The token estimate is a heuristic, and it says so
 *
 * There is no tokenizer in the browser and shipping one for a budget check would
 * be a megabyte of dependency to avoid a rounding error. {@link CHARS_PER_TOKEN}
 * is four characters per token, which is the usual figure for byte-pair
 * vocabularies on English and code and which errs on the safe side for German
 * compounds (they cost more tokens per character, so the estimate under-counts
 * and the compiler cuts earlier than it has to). The measured prompt size of
 * every eval run is written next to the estimate in verification/w5/eval.json,
 * so the size of the error is on the record rather than assumed away.
 */

import type { SemanticIR } from '../core/semantic-ir';
import type { RouteRef, SymbolSearchHit } from '../core/intelligence-provider';
import { workspacePathOf } from '../twin/twin-target';
import { toGraphLine } from '../core/positions';
import type { FactPacket, NeighbourFact } from './fact-recipes';
import type { QuestionClass } from './question-classifier';

/** The hard budget for the roughly 1B class. PLAN paragraph 5, context 3072. */
export const CARD_BUDGET_CLASS_A = 3000;

/** The hard budget for the roughly 4B class. PLAN paragraph 5, context 8192. */
export const CARD_BUDGET_CLASS_B = 8000;

/** The two model classes, by the budget each one gets. */
export type ModelClass = 'A' | 'B';

/** The budget of one model class. There is no third. */
export function budgetOf(modelClass: ModelClass): number {
    return modelClass === 'A' ? CARD_BUDGET_CLASS_A : CARD_BUDGET_CLASS_B;
}

/**
 * Which class a running model belongs to, from the window it opened.
 *
 * Read off the process rather than off a table of model names, for the reason
 * the sidecar card states: a name table is right for a month and then describes
 * a file somebody swapped. `llm/start.sh` opens 3072 for the 1B class and 8192
 * for the 4B class, so a window that can hold the larger budget is the larger
 * class, and anything else is the smaller one. A model whose window is unknown
 * is the smaller class, because guessing upwards would overrun a real context.
 */
export function modelClassOf(contextTokens: number | undefined): ModelClass {
    return contextTokens !== undefined && contextTokens >= CARD_BUDGET_CLASS_B ? 'B' : 'A';
}

/** Characters per token. A documented heuristic; see the file header. */
export const CHARS_PER_TOKEN = 4;

/**
 * What the budget keeps free for everything that is not a card.
 *
 * The system prompt, the question, the honest block and the answer itself all
 * live inside the same context window as the cards. Reserving for them here is
 * what makes the budget a promise about the window rather than about one part
 * of the prompt.
 */
export const PROMPT_RESERVE_TOKENS = 700;

/** At most three lines per card. PLAN paragraph 6. */
export const MAX_CARD_LINES = 3;

/** How many items one enumeration inside a card may name. */
export const LIST_CAP = 8;

/** What a card can be about. */
export type CardKind =
    | 'subject'
    | 'calls'
    | 'raises'
    | 'caller'
    | 'callee'
    | 'route'
    | 'entry'
    | 'overview'
    | 'compare'
    | 'observed'
    | 'absence';

/** Where a card came from, so `[K3]` can become a click. */
export interface CardSource {
    name: string;
    qualifiedName?: string;
    /** Workspace-relative path, as the tree names it. */
    filePath?: string;
    /** 1-based graph line. */
    line?: number;
}

/** One numbered card. */
export interface Card {
    /** `K1`, `K2`, ... in the order the cards are emitted. */
    id: string;
    kind: CardKind;
    /** At most {@link MAX_CARD_LINES} lines. */
    lines: string[];
    source?: CardSource;
}

/** The compiled context of one question. */
export interface CardSet {
    cards: Card[];
    /** Tokens the cards may cost, after the reserve. */
    budget: number;
    /** Estimated tokens the emitted cards cost. */
    tokens: number;
    /** Cards the budget refused. */
    dropped: number;
    /** What could not be fetched, from the recipe. Rendered outside the cards. */
    gaps: string[];
}

/** The estimate. One number, one place, one heuristic. */
export function estimateTokens(text: string): number {
    return Math.ceil(text.length / CHARS_PER_TOKEN);
}

/** What is left for cards once the rest of the prompt has its share. */
export function cardBudgetOf(modelClass: ModelClass, reserve = PROMPT_RESERVE_TOKENS): number {
    return Math.max(0, budgetOf(modelClass) - reserve);
}

/**
 * One card as the prompt renders it. The same text the budget is measured on.
 *
 * The number is written `[K2]`, in the very syntax a citation uses, and that is
 * the single most valuable line of this file. It was `K2:` for three eval
 * passes, and every candidate did the obvious thing with it: the cheapest
 * continuation of a card list is another card line, so the models answered
 * `K2: registerUserRoutes ... calls createUser`, which is the right fact with
 * the number in a position that is not a citation. Rendering the number the way
 * a citation is written makes the cheapest continuation a *compliant* one: a
 * model that simply echoes a card has cited it. The prompt still asks for the
 * bracket at the end, and the checker accepts it anywhere, because "carries a
 * citation" is the contract and "ends with one" was only ever a way to say it.
 */
export function renderCard(card: Card): string {
    const [first, ...rest] = card.lines;
    const head = `[${card.id}] ${first ?? ''}`;
    return [head, ...rest.map((line) => `    ${line} [${card.id}]`)].join('\n');
}

/** All cards as the prompt renders them. */
export function renderCards(cards: readonly Card[]): string {
    return cards.map(renderCard).join('\n');
}

// ----------------------------------------------------------------- wording --

/** `name (path:line)`, or as much of it as the facts allow. */
export function whereOf(source: CardSource | undefined): string {
    if (source === undefined) {
        return '';
    }
    if (source.filePath === undefined || source.filePath.length === 0) {
        return source.name;
    }
    return source.line === undefined
        ? `${source.name} (${source.filePath})`
        : `${source.name} (${source.filePath}:${source.line})`;
}

/** `a, b and 3 more not listed`. The honest form of every enumeration. */
export function listOf(items: readonly string[], cap = LIST_CAP): string {
    if (items.length === 0) {
        return '';
    }
    if (items.length <= cap) {
        return items.join(', ');
    }
    const shown = items.slice(0, cap).join(', ');
    return `${shown}, and ${items.length - cap} more not listed`;
}

/** The declaration of a symbol, as a card source. */
function sourceOfIr(ir: SemanticIR): CardSource {
    const source: CardSource = {
        name: ir.symbol.name,
        filePath: workspacePathOf(ir.symbol.uri),
        line: toGraphLine((ir.symbol.selectionRange ?? ir.symbol.range).start.line),
    };
    if (ir.symbol.qualifiedName !== undefined) {
        source.qualifiedName = ir.symbol.qualifiedName;
    }
    return source;
}

function sourceOfNeighbour(entry: NeighbourFact): CardSource {
    const source: CardSource = { name: entry.name };
    if (entry.qualifiedName !== undefined) {
        source.qualifiedName = entry.qualifiedName;
    }
    if (entry.filePath !== undefined) {
        source.filePath = entry.filePath;
    }
    if (entry.line !== undefined) {
        source.line = entry.line;
    }
    return source;
}

/** A card without its number yet. The numbering happens after the budget cut. */
interface Draft {
    kind: CardKind;
    lines: string[];
    source?: CardSource;
    /** Lower sorts earlier. Set per class by {@link priorityOf}. */
    rank: number;
}

// ------------------------------------------------------------- the drafts ---

function subjectDrafts(packet: FactPacket, rank: (kind: CardKind) => number): Draft[] {
    const ir = packet.subjectIr;
    if (ir === undefined) {
        return [];
    }
    const source = sourceOfIr(ir);
    const drafts: Draft[] = [];

    /*
     * The counting sentence goes LAST, and that ordering was bought with an
     * eval pass. With it on line two, two of the smaller candidates answered
     * "what does listUsers do" with "it makes 1 call(s), reads 0 environment
     * value(s)" and never reached the card that names the call. A count is a
     * true fact and a useless answer, and a card's second line is the one a
     * small model reaches for first.
     */
    const identity: string[] = [
        `${whereOf(source)} is a ${ir.symbol.kind} in this project.`,
        ir.tests.value.length === 0
            ? 'The index records no test that reaches it.'
            : `The index records ${ir.tests.value.length} test(s) reaching it: `
                + `${listOf(ir.tests.value.map((test) => test.name), 3)}.`,
        `It makes ${ir.calls.value.length} call(s), reads ${ir.reads.value.length} `
        + `environment value(s) and can raise ${ir.throws.value.length} error type(s).`,
    ];
    drafts.push({ kind: 'subject', lines: identity, source, rank: rank('subject') });

    if (ir.steps.value.length > 0) {
        const calls = ir.steps.value.map((call) => {
            const verb = call.strategy === 'construction' ? 'constructs' : 'calls';
            const line = call.line === undefined ? '' : ` [line ${call.line}]`;
            return `${verb} ${call.targetName}${line}`;
        });
        drafts.push({
            kind: 'calls',
            lines: [`${ir.symbol.name}, in body order: ${listOf(calls)}.`],
            source,
            rank: rank('calls'),
        });
    }

    const raiseLines: string[] = [];
    if (ir.throws.value.length > 0) {
        raiseLines.push(
            `${ir.symbol.name} can raise ${listOf(ir.throws.value.map((raise) => {
                const file = workspacePathOf(raise.file);
                const at = raise.line === undefined
                    ? ''
                    : ` [${file.length > 0 ? `${file}:` : 'line '}${raise.line}]`;
                return `${raise.type}${at}`;
            }))}.`,
        );
    } else if (ir.throws.state === 'known') {
        raiseLines.push(`The index records no error type raised by ${ir.symbol.name}.`);
    }
    const envReads = ir.reads.value.filter((read) => read.kind === 'global');
    if (envReads.length > 0) {
        raiseLines.push(
            `${ir.symbol.name} reads the environment value(s) `
            + `${listOf(envReads.map((read) => read.name))}.`,
        );
    }
    if (raiseLines.length > 0) {
        drafts.push({ kind: 'raises', lines: raiseLines, source, rank: rank('raises') });
    }
    return drafts;
}

/**
 * Neighbours in their own order, not in the order they arrived.
 *
 * The recipe already sorts them, and this sorts them again by the same key. Not
 * redundant: a compiler whose output depended on the order a caller happened to
 * assemble a packet in would be one refactor away from producing two different
 * card sets for one index, and the eval compares six models against one card
 * set. Idempotent by construction, since the key is the recipe's key.
 */
function orderedNeighbours(entries: readonly NeighbourFact[]): NeighbourFact[] {
    return [...entries].sort((a, b) => {
        if (a.hop !== b.hop) {
            return a.hop - b.hop;
        }
        if (a.direction !== b.direction) {
            return a.direction < b.direction ? -1 : 1;
        }
        const left = a.qualifiedName ?? a.name;
        const right = b.qualifiedName ?? b.name;
        if (left === right) {
            return 0;
        }
        return left < right ? -1 : 1;
    });
}

function neighbourDrafts(packet: FactPacket, rank: (kind: CardKind) => number): Draft[] {
    const subjectName = packet.subject?.name ?? 'the subject';
    return orderedNeighbours(packet.neighbours).map((entry) => {
        const source = sourceOfNeighbour(entry);
        const lines: string[] = [];
        if (entry.direction === 'caller') {
            const at = entry.line === undefined ? '' : ` at line ${entry.line}`;
            lines.push(
                `${whereOf(source)} calls ${entry.hop === 1 ? subjectName : entry.via}${at}.`,
            );
            if (entry.isTest === true) {
                lines.push('The index flags it as test code.');
            }
        } else {
            const verb = entry.construction === true ? 'constructs' : 'calls';
            lines.push(
                `${entry.hop === 1 ? subjectName : entry.via} ${verb} ${whereOf(source)}.`,
            );
            if (entry.raises !== undefined && entry.raises.length > 0) {
                lines.push(`${entry.name} can raise ${listOf(entry.raises)}.`);
            }
        }
        if (entry.hop > 1) {
            lines.push(`This is ${entry.hop} hops from the subject.`);
        }
        return {
            kind: entry.direction === 'caller' ? ('caller' as const) : ('callee' as const),
            lines,
            source,
            rank: rank(entry.direction === 'caller' ? 'caller' : 'callee') + entry.hop,
        };
    });
}

function routeDrafts(routes: readonly RouteRef[], rank: number): Draft[] {
    return routes.map((route) => {
        const method = route.method ?? 'ANY';
        const handler = route.handler === undefined ? '' : ` handled by ${route.handler}`;
        const source: CardSource = { name: `${method} ${route.path}` };
        if (route.filePath !== undefined) {
            source.filePath = route.filePath;
        }
        if (route.line !== undefined) {
            source.line = route.line;
        }
        return {
            kind: 'route' as const,
            lines: [
                `The route ${method} ${route.path} is registered in `
                + `${route.filePath ?? 'an unnamed file'}`
                + `${route.line === undefined ? '' : `:${route.line}`}${handler}.`,
                route.origin === 'source'
                    ? 'It was read off the source text, not out of the index.'
                    : 'The index reports it.',
            ],
            source,
            rank,
        };
    });
}

function entryDraft(entryPoints: readonly SymbolSearchHit[], rank: number): Draft[] {
    if (entryPoints.length === 0) {
        return [];
    }
    const names = entryPoints.map((hit) => {
        const where = hit.filePath === undefined
            ? ''
            : ` (${hit.filePath}${hit.line === undefined ? '' : `:${hit.line}`})`;
        return `${hit.name}${where}`;
    });
    const first = entryPoints[0];
    const source: CardSource = { name: first.name };
    if (first.qualifiedName !== undefined) {
        source.qualifiedName = first.qualifiedName;
    }
    if (first.filePath !== undefined) {
        source.filePath = first.filePath;
    }
    if (first.line !== undefined) {
        source.line = first.line;
    }
    return [{
        kind: 'entry',
        lines: [`The index flags these symbols as entry points: ${listOf(names)}.`],
        source,
        rank,
    }];
}

function overviewDrafts(packet: FactPacket, rank: number): Draft[] {
    const facts = packet.overview;
    if (facts === undefined) {
        return [];
    }
    const groups = facts.groups.map((group) => `${group.name} (${group.symbolCount})`);
    return [{
        kind: 'overview',
        lines: [
            `The project ${facts.projectName ?? ''} holds ${facts.totalSymbols} symbols and `
            + `${facts.totalRelations} relations across ${facts.fileCount} indexed files.`,
            `Languages: ${listOf(facts.languages)}.`,
            groups.length === 0 ? 'The index names no groups.' : `Groups: ${listOf(groups)}.`,
        ],
        rank,
    }];
}

function compareDrafts(packet: FactPacket, rank: number): Draft[] {
    const other = packet.compareWith;
    if (other === undefined) {
        return [];
    }
    const ir = other.ir;
    if (ir === undefined) {
        return [{
            kind: 'compare',
            lines: [`${other.symbol.name} is in the index, but its facts could not be read.`],
            source: { name: other.symbol.name, filePath: workspacePathOf(other.symbol.uri) },
            rank,
        }];
    }
    const source = sourceOfIr(ir);
    const calls = ir.steps.value.map((call) => {
        const line = call.line === undefined ? '' : ` [line ${call.line}]`;
        return `${call.targetName}${line}`;
    });
    return [{
        kind: 'compare',
        lines: [
            `${whereOf(source)} is a ${ir.symbol.kind}; it is called by `
            + `${ir.calledBy.value.length} symbol(s).`,
            calls.length === 0
                ? `${ir.symbol.name} makes no call the index recorded.`
                : `${ir.symbol.name} calls ${listOf(calls)}.`,
            ir.throws.value.length === 0
                ? `${ir.symbol.name} raises no error type the index recorded.`
                : `${ir.symbol.name} can raise `
                    + `${listOf(raisedNames(ir))}.`,
        ],
        source,
        rank,
    }];
}

function raisedNames(ir: SemanticIR): string[] {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const raise of ir.throws.value) {
        if (!seen.has(raise.type)) {
            seen.add(raise.type);
            out.push(raise.type);
        }
    }
    return out;
}

function observedDrafts(packet: FactPacket, rank: number): Draft[] {
    return packet.observed.map((entry) => ({
        kind: 'observed' as const,
        lines: [
            `An imported recording saw ${entry.from} call ${entry.to} ${entry.count} time(s)`
            + `${entry.lastSeen === undefined ? '' : `, last on ${entry.lastSeen}`}.`,
            'This is an observation, not a reading of the index.',
        ],
        rank,
    }));
}

/**
 * The order the cards go in, per class.
 *
 * The subject is always first, because every answer starts by naming what it is
 * about. After that the class decides: a caller question leads with callers, an
 * error question with what can be raised, an entry question with routes. The
 * order matters twice over, because it is also the order the budget cuts from
 * the back of.
 */
export function priorityOf(klass: QuestionClass): (kind: CardKind) => number {
    const base: Record<CardKind, number> = {
        subject: 0,
        calls: 20,
        raises: 30,
        caller: 40,
        callee: 50,
        route: 60,
        entry: 62,
        overview: 64,
        compare: 66,
        observed: 70,
        absence: 100,
    };
    const lift: Partial<Record<QuestionClass, Partial<Record<CardKind, number>>>> = {
        'who-calls': { caller: 10, observed: 15 },
        'what-if': { caller: 10, observed: 15 },
        'why-error': { raises: 10, callee: 15, observed: 18 },
        'where-entry': { route: 5, entry: 6, overview: 8 },
        overview: { overview: 5, entry: 6, route: 8 },
        compare: { compare: 5 },
        'what-is': { calls: 10, raises: 12 },
    };
    const overrides = lift[klass] ?? {};
    return (kind) => overrides[kind] ?? base[kind];
}

// ------------------------------------------------------------ the compiler --

/**
 * Turn one fact packet into numbered cards under a hard budget.
 *
 * Pure and total: the same packet and the same budget always produce the same
 * cards, and a packet with nothing in it produces no cards at all, which is the
 * signal the chat turns into the agreed sentence instead of asking a model to
 * invent one.
 */
export function compileCards(
    packet: FactPacket,
    options: { budget: number },
): CardSet {
    const rank = priorityOf(packet.klass);
    const drafts: Draft[] = [
        ...subjectDrafts(packet, rank),
        ...neighbourDrafts(packet, rank),
        ...routeDrafts(packet.routes, rank('route')),
        ...entryDraft(packet.entryPoints, rank('entry')),
        ...overviewDrafts(packet, rank('overview')),
        ...compareDrafts(packet, rank('compare')),
        ...observedDrafts(packet, rank('observed')),
    ];

    // Stable order: rank first, then the order the recipe produced them in.
    const ordered = drafts
        .map((draft, index) => ({ draft, index }))
        .sort((a, b) => (a.draft.rank - b.draft.rank) || (a.index - b.index))
        .map((entry) => entry.draft);

    const budget = options.budget;
    const kept: Draft[] = [];
    const droppedByKind = new Map<CardKind, number>();
    let tokens = 0;
    for (const draft of ordered) {
        const lines = draft.lines.filter((line) => line.length > 0).slice(0, MAX_CARD_LINES);
        const candidate: Card = {
            id: `K${kept.length + 1}`,
            kind: draft.kind,
            lines,
            ...(draft.source === undefined ? {} : { source: draft.source }),
        };
        const cost = estimateTokens(renderCard(candidate)) + 1;
        if (tokens + cost > budget) {
            droppedByKind.set(draft.kind, (droppedByKind.get(draft.kind) ?? 0) + 1);
            continue;
        }
        tokens += cost;
        kept.push({ ...draft, lines });
    }

    const cards: Card[] = kept.map((draft, index) => ({
        id: `K${index + 1}`,
        kind: draft.kind,
        lines: draft.lines,
        ...(draft.source === undefined ? {} : { source: draft.source }),
    }));

    let dropped = [...droppedByKind.values()].reduce((sum, count) => sum + count, 0);
    if (dropped > 0 && cards.length > 0) {
        /*
         * The honest note is worth its own tokens: a list that quietly ends is
         * worse than a list one card shorter that says where it ended. So room
         * is made for it by dropping cards from the back, which grows the very
         * number the note reports, so both are rebuilt after every drop.
         *
         * The budget still wins. If the note does not fit even on its own, no
         * cards are emitted at all, and the chat turns that into the agreed
         * sentence: a budget too small for one card is a budget with nothing to
         * give, and pretending otherwise would break the one hard rule here.
         */
        const noteFor = (): Card => ({
            id: `K${cards.length + 1}`,
            kind: 'absence',
            lines: [
                `The token budget of ${budget} stopped this list: `
                + [...droppedByKind.entries()]
                    .sort((a, b) => (a[0] < b[0] ? -1 : 1))
                    .map(([kind, count]) => `${count} more ${labelOf(kind, count)} not listed`)
                    .join(', ') + '.',
                'Anything not on a card above is not part of what was given.',
            ],
        });
        let note = noteFor();
        let cost = estimateTokens(renderCard(note)) + 1;
        while (tokens + cost > budget && cards.length > 0) {
            const removed = cards.pop();
            if (removed === undefined) {
                break;
            }
            tokens -= estimateTokens(renderCard(removed)) + 1;
            droppedByKind.set(removed.kind, (droppedByKind.get(removed.kind) ?? 0) + 1);
            note = noteFor();
            cost = estimateTokens(renderCard(note)) + 1;
        }
        dropped = [...droppedByKind.values()].reduce((sum, count) => sum + count, 0);
        if (tokens + cost <= budget) {
            cards.push(note);
            tokens += cost;
        } else {
            cards.length = 0;
            tokens = 0;
        }
    }

    return {
        cards,
        budget,
        tokens,
        dropped,
        gaps: [...packet.notes],
    };
}

/** The plural word for one card kind, so a cap note reads like a sentence. */
function labelOf(kind: CardKind, count: number): string {
    const singular: Record<CardKind, string> = {
        subject: 'subject fact',
        calls: 'call list',
        raises: 'error fact',
        caller: 'caller',
        callee: 'callee',
        route: 'route',
        entry: 'entry point list',
        overview: 'overview fact',
        compare: 'comparison',
        observed: 'observation',
        absence: 'note',
    };
    const word = singular[kind];
    return count === 1 ? word : `${word}s`;
}
