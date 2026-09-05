/**
 * One fixed recipe per question class: which graph facts are fetched, from
 * where, and how far out.
 *
 * The classifier says what was asked; this file says what to look up. Nothing
 * here decides anything about wording, and nothing here talks to a model. A
 * recipe is a list of provider calls with a bound on it, and the bound is the
 * whole reason a 1B model can answer at all: the compiler that follows turns
 * this packet into cards under a hard token budget, and a packet that grew with
 * the repository would make the budget cut off a different half of the answer
 * every time.
 *
 * ## Martin's context rule (voicemail 2, 2026-08-28)
 *
 * The default context is the code level the reader is looking at plus the FIRST
 * neighbourhood: the symbol itself, its direct callers and its direct callees.
 * A setting in the chat moves that between {@link NEIGHBOR_DEPTHS} 0, 1 and 2
 * hops, with 1 as the default, and the surface says out loud that more
 * neighbours can change the quality of the answer rather than only improve it.
 * The token budget stays the hard limit either way: depth widens what is
 * offered, the card compiler still cuts at the budget and says how much it cut.
 *
 * That rule is why the neighbourhood is gathered the same way for every class.
 * The class decides which family is emphasised and what else is fetched beside
 * it; it never decides how far out "nearby" reaches, because a reader who set
 * the depth to two meant two for every question they ask afterwards.
 *
 * ## Two hops are cheap and the third is not
 *
 * Hop one costs nothing beyond the subject's own IR: a caller row already
 * carries the file and the line of its call site, and a call site already
 * carries the callee's file and declaration line. So the default depth adds no
 * round trip at all. Hop two has to ask the index about each neighbour, so it
 * is capped by {@link NEIGHBOUR_HOP2_SEEDS} and the cap is reported. There is no
 * hop three: a walk that far out is the closure view's question, not a chat
 * answer's.
 *
 * ## What every recipe reads, and what it says when it cannot
 *
 * The sources of each class are declared in {@link RECIPE_SOURCES} and the
 * packet repeats the ones it actually used, so an answer can be traced back to
 * the calls that produced it. Every absence is a note rather than an empty
 * list: "the index records no caller" and "nobody asked for callers" are
 * different findings and the cards below print them differently.
 *
 * ## What a name that does not land does now (W7c)
 *
 * Three outcomes instead of one, and each of them is a sentence somebody can
 * act on:
 *
 *  - **several symbols answer the name equally well**: no subject and no card,
 *    plus {@link FactPacket.choice}, so the chat can offer the candidates. The
 *    old behaviour took the first one and mentioned it in a note under the
 *    answer, which is a choice made for the reader in a place they read last.
 *  - **the name reaches nothing and a symbol is in focus**: the focus becomes
 *    the subject and {@link FactPacket.focusFallback} says so. The user
 *    screenshot behind this cycle showed a ready model, a focused symbol and a
 *    refusal, all at once.
 *  - **the name reaches nothing and nothing is in focus**: the agreed sentence,
 *    unchanged. That is the one case where there is genuinely nothing to say.
 */

import type { SymbolRef } from '../core/focus-protocol';
import type { CallerRef, CallSite, SemanticIR, ThrowRef } from '../core/semantic-ir';
import type {
    ArchitectureOverviewDto,
    IntelligenceProvider,
    ProviderQueryOptions,
    RouteRef,
    SymbolFacts,
    SymbolSearchHit,
} from '../core/intelligence-provider';
import { buildIr } from '../ir/semantic-ir-builder';
import { workspacePathOf } from '../twin/twin-target';
import { classifyQuestion } from './question-classifier';
import type { Classification, ClassifierContext, QuestionClass } from './question-classifier';
import { resolveSubject } from './subject-resolver';
import type { SubjectSource } from './subject-resolver';

/** How much of the neighbourhood goes into the cards. Martin's rule, as a number. */
export type NeighborDepth = 0 | 1 | 2;

/** The three settings the chat offers, in the order they appear. */
export const NEIGHBOR_DEPTHS: readonly NeighborDepth[] = [0, 1, 2];

/**
 * The default: the focus symbol plus its first neighbourhood.
 *
 * Not zero, because a symbol with no neighbours around it answers "what does it
 * call" and nothing else, and not two, because the second hop is where a small
 * model starts mixing up which function a line belongs to.
 */
export const NEIGHBOR_DEPTH_DEFAULT: NeighborDepth = 1;

/** How many neighbours one hop may contribute. */
export const NEIGHBOUR_CAP_PER_HOP = 12;

/** How many hop-one neighbours are expanded when the depth is two. */
export const NEIGHBOUR_HOP2_SEEDS = 6;

/** How many callees are asked what they can raise, for the error recipe. */
export const RAISER_CAP = 6;

/** How many routes and entry points the entry recipe carries. */
export const ENTRY_CAP = 12;

/** How many callers a recording is read for. Each one is its own request. */
export const OBSERVED_CALLER_CAP = 4;

/** One symbol next to the subject, with the place a reader would click. */
export interface NeighbourFact {
    name: string;
    qualifiedName?: string;
    /** Workspace-relative file the click should open. */
    filePath?: string;
    /** 1-based line the click should reveal. */
    line?: number;
    /** Hops away from the subject. 1 is the first neighbourhood. */
    hop: number;
    /** Which side of the subject this one sits on. */
    direction: 'caller' | 'callee';
    /** Name of the symbol whose neighbourhood this one belongs to. */
    via: string;
    /** True when the index called the site a construction rather than a call. */
    construction?: boolean;
    /** True when the index flagged the symbol as test code. */
    isTest?: boolean;
    /** Error types this neighbour can raise, when the recipe asked for them. */
    raises?: string[];
}

/** One call a recording saw, as the two routes that can report it name it. */
export interface ObservedFact {
    from: string;
    to: string;
    count: number;
    lastSeen?: string;
}

/** What a recipe took from the project summary. Never the whole summary. */
export interface OverviewFacts {
    projectName?: string;
    totalSymbols: number;
    totalRelations: number;
    languages: string[];
    /** Group name and how many symbols it holds, largest first. */
    groups: { name: string; symbolCount: number }[];
    fileCount: number;
}

/**
 * One symbol a written name reached, as a choice can offer it.
 *
 * A trimmed {@link SymbolSearchHit}: the three things a reader needs to tell two
 * candidates apart (what it is called, where it lives, on which line) plus the
 * qualified name, which is what picking one sends back into the compiler.
 */
export interface SubjectCandidate {
    name: string;
    qualifiedName?: string;
    filePath?: string;
    line?: number;
    isTest?: boolean;
}

/** A name that reached more than one symbol equally well, with what it reached. */
export interface SubjectChoice {
    /** The name as the reader wrote it. */
    name: string;
    /** Every candidate, best first. */
    candidates: SubjectCandidate[];
}

/** A name that reached nothing, and the symbol in focus that answered instead. */
export interface FocusFallback {
    /** The name the reader wrote, which the index does not hold. */
    asked: string;
    /** The symbol the answer is about instead. */
    used: string;
}

/** Everything one question's recipe brought back. */
export interface FactPacket {
    klass: QuestionClass;
    question: string;
    classification: Classification;
    depth: NeighborDepth;
    /** The symbol the recipe centred on, when a name resolved to one. */
    subject?: SymbolRef;
    subjectIr?: SemanticIR;
    /** Other symbols the subject's name also reached. */
    ambiguous: SymbolSearchHit[];
    /**
     * Set when the name reached several symbols equally well and none was taken.
     *
     * The packet then carries no subject and no card on purpose: picking one of
     * two would be the compiler answering a question the reader did not ask, and
     * answering nothing would be the silence this cycle removed. The chat turns
     * this into a list to choose from.
     */
    choice?: SubjectChoice;
    /**
     * Set when a written name reached nothing and the symbol in focus was used.
     *
     * It travels with the packet so the answer can say so in its own line. A
     * fallback nobody is told about is a wrong answer to a question that was
     * asked, which is worse than a refusal.
     */
    focusFallback?: FocusFallback;
    neighbours: NeighbourFact[];
    /** The second symbol of a comparison, with its own facts. */
    compareWith?: { symbol: SymbolRef; ir?: SemanticIR };
    routes: RouteRef[];
    entryPoints: SymbolSearchHit[];
    overview?: OverviewFacts;
    observed: ObservedFact[];
    /** Which calls produced this packet, one sentence each. */
    sources: string[];
    /** What is missing, and why. Never silence. */
    notes: string[];
}

/**
 * What each recipe reads. Declared, so a reader can check a class without
 * running it and a test can prove no recipe grew a source in silence.
 */
export const RECIPE_SOURCES: Readonly<Record<QuestionClass, readonly string[]>> = {
    'what-is': [
        'semantic IR of the subject (callees, callers, throws, env reads, type refs, tests)',
        'first neighbourhood from the caller rows and the call sites of that IR',
    ],
    'who-calls': [
        'semantic IR of the subject (caller rows with their call-site lines)',
        'first neighbourhood, callers emphasised',
        'observed calls into the subject from GET /api/trace, when a recording was imported',
    ],
    'what-if': [
        'semantic IR of the subject',
        'callers out to the chosen depth: what a change here would reach',
        'observed calls into the subject from GET /api/trace, when a recording was imported',
    ],
    'where-entry': [
        'architecture overview: entry points and recovered HTTP routes',
        'semantic IR of the subject, when the question named one',
    ],
    'why-error': [
        'semantic IR of the subject (raised types with their sites)',
        'what the direct callees can raise, asked per callee',
        'observed calls into the subject from GET /api/trace, when a recording was imported',
    ],
    compare: [
        'semantic IR of both named symbols',
        'first neighbourhood of each',
    ],
    overview: [
        'architecture overview: counts, languages, groups, entry points, routes',
    ],
    other: [
        'nothing: the question matched no rule, so no recipe ran',
    ],
};

/** What a recipe needs from the provider layer. A slice, so a test can fake it. */
export interface RecipeSource extends SubjectSource {
    readonly id: IntelligenceProvider['id'];
    getFacts: IntelligenceProvider['getFacts'];
    getSnippet: IntelligenceProvider['getSnippet'];
    architectureOverview: IntelligenceProvider['architectureOverview'];
}

/** Reads what a recording saw on the way into a symbol. Absent means nobody looked. */
export type ObservedReader = (
    subject: SymbolRef,
    callers: readonly NeighbourFact[],
) => Promise<ObservedFact[]>;

/** Knobs for one recipe run. */
export interface RecipeOptions extends ProviderQueryOptions {
    depth?: NeighborDepth;
    /** Context the classifier gets. The recipe passes it straight through. */
    context?: ClassifierContext;
    /** The already resolved focus, so a question about "this" costs no search. */
    focus?: SymbolRef;
    /**
     * The symbol the reader picked from a candidate list, as its qualified name.
     *
     * It replaces the name in the question and nothing else: the same recipe,
     * the same depth, the same class. A picked name is qualified, so it lands on
     * the top rung of the ladder in subject-resolver.ts and cannot be ambiguous
     * a second time.
     */
    chosenSubject?: string;
    observed?: ObservedReader;
}

// ---------------------------------------------------- what the notes say ---

/** A candidate, as a choice carries it. */
function candidateOf(hit: SymbolSearchHit): SubjectCandidate {
    const entry: SubjectCandidate = { name: hit.name };
    if (hit.qualifiedName !== undefined) {
        entry.qualifiedName = hit.qualifiedName;
    }
    if (hit.filePath !== undefined) {
        entry.filePath = hit.filePath;
    }
    if (hit.line !== undefined) {
        entry.line = hit.line;
    }
    if (hit.isTest === true) {
        entry.isTest = true;
    }
    return entry;
}

/**
 * Why a packet with an ambiguous name carries no card.
 *
 * A note and not the request itself: `notes` says what is missing and why, and
 * the ask ("pick one") belongs to the surface that can be picked on. The two
 * sentences have two jobs and live in two files for that reason.
 */
export function ambiguousSubjectNote(name: string, count: number): string {
    return `no card was built: "${name}" names ${count} symbols in this index, and this compiler `
        + 'does not pick one of them for you.';
}

// ---------------------------------------------------------------- helpers ---

/** Ordinal comparison. The determinism rule this project states everywhere. */
function compareText(a: string, b: string): number {
    if (a === b) {
        return 0;
    }
    return a < b ? -1 : 1;
}

function neighbourKey(entry: NeighbourFact): string {
    return `${entry.direction}:${entry.qualifiedName ?? entry.name}`;
}

/** A caller row as a neighbour: the click lands on the call site. */
function callerNeighbour(caller: CallerRef, via: string, hop: number): NeighbourFact {
    const filePath = workspacePathOf(caller.file);
    const entry: NeighbourFact = { name: caller.name, hop, direction: 'caller', via };
    if (caller.qualifiedName !== undefined) {
        entry.qualifiedName = caller.qualifiedName;
    }
    if (filePath.length > 0) {
        entry.filePath = filePath;
    }
    if (caller.line !== undefined) {
        entry.line = caller.line;
    }
    if (caller.isTest === true) {
        entry.isTest = true;
    }
    return entry;
}

/** A call site as a neighbour: the click lands on the callee's declaration. */
function calleeNeighbour(call: CallSite, via: string, hop: number): NeighbourFact {
    const filePath = workspacePathOf(call.targetFile);
    const entry: NeighbourFact = { name: call.targetName, hop, direction: 'callee', via };
    if (call.targetQualifiedName !== undefined) {
        entry.qualifiedName = call.targetQualifiedName;
    }
    if (filePath.length > 0) {
        entry.filePath = filePath;
    }
    if (call.targetLine !== undefined) {
        entry.line = call.targetLine;
    }
    if (call.strategy === 'construction') {
        entry.construction = true;
    }
    return entry;
}

/** Neighbours in a stable order, deduplicated, capped, with the cap reported. */
function boundNeighbours(
    found: readonly NeighbourFact[],
    notes: string[],
    hop: number,
): NeighbourFact[] {
    const seen = new Set<string>();
    const unique: NeighbourFact[] = [];
    for (const entry of [...found].sort((a, b) => {
        const byDirection = compareText(a.direction, b.direction);
        return byDirection !== 0
            ? byDirection
            : compareText(a.qualifiedName ?? a.name, b.qualifiedName ?? b.name);
    })) {
        const key = neighbourKey(entry);
        if (!seen.has(key)) {
            seen.add(key);
            unique.push(entry);
        }
    }
    if (unique.length <= NEIGHBOUR_CAP_PER_HOP) {
        return unique;
    }
    notes.push(
        `hop ${hop}: ${unique.length - NEIGHBOUR_CAP_PER_HOP} of ${unique.length} neighbours are `
        + `not in this packet, because a hop carries at most ${NEIGHBOUR_CAP_PER_HOP}.`,
    );
    return unique.slice(0, NEIGHBOUR_CAP_PER_HOP);
}

/** The first neighbourhood, straight out of the subject's own IR. Free. */
export function firstNeighbourhood(ir: SemanticIR, notes: string[]): NeighbourFact[] {
    const via = ir.symbol.qualifiedName ?? ir.symbol.name;
    const found: NeighbourFact[] = [
        ...ir.calledBy.value.map((caller) => callerNeighbour(caller, via, 1)),
        ...ir.calls.value.map((call) => calleeNeighbour(call, via, 1)),
    ];
    if (ir.calledBy.state === 'unknown') {
        notes.push('the callers of the subject could not be read, so none are listed.');
    }
    if (ir.calls.state === 'unknown') {
        notes.push('the calls out of the subject could not be read, so none are listed.');
    }
    return boundNeighbours(found, notes, 1);
}

/** Raised types as the card compiler prints them. */
export function raisedTypesOf(throws: readonly ThrowRef[]): string[] {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const raise of throws) {
        if (!seen.has(raise.type)) {
            seen.add(raise.type);
            out.push(raise.type);
        }
    }
    return out;
}

// ------------------------------------------------------------- the recipes --

/**
 * The second hop, when the reader asked for it.
 *
 * One index question per seed and no more: a hop-two walk over a whole
 * repository is the closure view's job and not a chat answer's. The seeds are
 * the hop-one neighbours in their own order, so two runs of one question expand
 * the same six.
 */
async function secondHop(
    source: RecipeSource,
    root: string,
    first: readonly NeighbourFact[],
    opts: ProviderQueryOptions,
    notes: string[],
): Promise<NeighbourFact[]> {
    const seeds = first.slice(0, NEIGHBOUR_HOP2_SEEDS);
    if (first.length > seeds.length) {
        notes.push(
            `depth 2: only the first ${seeds.length} of ${first.length} neighbours were expanded.`,
        );
    }
    const found: NeighbourFact[] = [];
    for (const seed of seeds) {
        const resolved = seed.filePath === undefined || seed.line === undefined
            ? undefined
            : await source
                .resolveSymbolAt(root, seed.filePath, seed.line, opts)
                .then((result) => (result.kind === 'ok' ? result.symbol : undefined))
                .catch(() => undefined);
        if (resolved === undefined) {
            continue;
        }
        const via = seed.qualifiedName ?? seed.name;
        const facts = await source
            .getFacts(root, resolved, seed.direction === 'caller' ? ['callers'] : ['callees'], opts)
            .catch((): SymbolFacts => ({}));
        if (seed.direction === 'caller') {
            for (const caller of facts.callers?.value ?? []) {
                found.push(callerNeighbour(caller, via, 2));
            }
        } else {
            for (const call of facts.callees?.value ?? []) {
                found.push(calleeNeighbour(call, via, 2));
            }
        }
    }
    return boundNeighbours(found, notes, 2);
}

/**
 * What the direct callees can raise.
 *
 * Only for the error recipe, and only for the first {@link RAISER_CAP} of them:
 * an error question is the one place where "what the thing I call can throw at
 * me" is the answer rather than background, and it is also the one place where
 * asking every callee would be worth the round trips.
 */
async function raisesOfCallees(
    source: RecipeSource,
    root: string,
    neighbours: readonly NeighbourFact[],
    opts: ProviderQueryOptions,
    notes: string[],
): Promise<NeighbourFact[]> {
    const callees = neighbours.filter((entry) => entry.direction === 'callee' && entry.hop === 1);
    const asked = callees.slice(0, RAISER_CAP);
    if (callees.length > asked.length) {
        notes.push(
            `${callees.length - asked.length} of ${callees.length} callees were not asked what they `
            + 'can raise, because at most ' + String(RAISER_CAP) + ' are.',
        );
    }
    const enriched = new Map<string, string[]>();
    for (const callee of asked) {
        if (callee.filePath === undefined || callee.line === undefined) {
            continue;
        }
        const resolved = await source
            .resolveSymbolAt(root, callee.filePath, callee.line, opts)
            .then((result) => (result.kind === 'ok' ? result.symbol : undefined))
            .catch(() => undefined);
        if (resolved === undefined) {
            continue;
        }
        const facts = await source.getFacts(root, resolved, ['throws'], opts).catch((): SymbolFacts => ({}));
        const raised = raisedTypesOf(facts.throws?.value ?? []);
        if (raised.length > 0) {
            enriched.set(neighbourKey(callee), raised);
        }
    }
    return neighbours.map((entry) => {
        const raised = enriched.get(neighbourKey(entry));
        return raised === undefined ? entry : { ...entry, raises: raised };
    });
}

/** The part of the project summary the entry and overview recipes carry. */
function overviewFactsOf(dto: ArchitectureOverviewDto): OverviewFacts {
    const facts: OverviewFacts = {
        totalSymbols: dto.totalSymbols,
        totalRelations: dto.totalRelations,
        languages: dto.languages.map((entry) => entry.language),
        groups: [...dto.groups]
            .sort((a, b) => (b.symbolCount - a.symbolCount) || compareText(a.name, b.name))
            .slice(0, ENTRY_CAP)
            .map((group) => ({ name: group.name, symbolCount: group.symbolCount })),
        fileCount: dto.files.length,
    };
    if (dto.projectName !== undefined) {
        facts.projectName = dto.projectName;
    }
    return facts;
}

/**
 * Run the recipe for one question.
 *
 * Never rejects. Every leg is wrapped, and a leg that failed becomes a note
 * rather than an exception: the chat turn that follows is worth more with four
 * families and one honest absence than it is with nothing at all. That is rule
 * one of the IR builder, applied one layer up.
 */
export async function compileFacts(
    source: RecipeSource,
    root: string,
    question: string,
    options: RecipeOptions = {},
): Promise<FactPacket> {
    const depth = options.depth ?? NEIGHBOR_DEPTH_DEFAULT;
    const context = options.context ?? {};
    const classification = classifyQuestion(question, context);
    const opts: ProviderQueryOptions = {
        ...(options.projectName === undefined ? {} : { projectName: options.projectName }),
        ...(options.generation === undefined ? {} : { generation: options.generation }),
    };
    const notes: string[] = [];
    const sources = [...RECIPE_SOURCES[classification.klass]];

    const packet: FactPacket = {
        klass: classification.klass,
        question,
        classification,
        depth,
        ambiguous: [],
        neighbours: [],
        routes: [],
        entryPoints: [],
        observed: [],
        sources,
        notes,
    };

    // --- the subject ------------------------------------------------------
    /*
     * `other` fetches nothing at all, and that is the whole recipe.
     *
     * The tempting alternative is to answer about whatever the reader has in
     * focus, which produces a fluent answer to a question nobody asked. A
     * question the compiler could not sort is a question it does not
     * understand, and the honest response to that is the agreed sentence, which
     * is what an empty packet turns into one layer up.
     */
    let subject: SymbolRef | undefined;
    if (classification.klass === 'other') {
        notes.push(
            'the question matched none of the seven shapes this compiler knows, so no recipe ran '
            + 'and no fact was fetched.',
        );
        return packet;
    }
    if (classification.subject !== undefined) {
        const chosen = options.chosenSubject ?? '';
        if (chosen.length > 0) {
            /*
             * The reader already answered the ambiguity. The picked name is
             * qualified, so it resolves on the top rung and cannot tie again;
             * if the index has meanwhile lost it, the packet says that instead
             * of quietly falling back to the name that was ambiguous.
             */
            const picked = await resolveSubject(source, root, chosen, opts);
            if (picked === undefined) {
                notes.push(`the index holds no symbol called "${chosen}", so no card describes it.`);
            } else {
                subject = picked.symbol;
                notes.push(
                    `"${classification.subject}" named more than one symbol; this answer is about `
                    + `the one you picked: ${picked.symbol.qualifiedName ?? picked.symbol.name}.`,
                );
            }
        } else if (classification.subjectFrom === 'focus' && options.focus !== undefined) {
            subject = options.focus;
        } else {
            const resolved = await resolveSubject(source, root, classification.subject, opts);
            if (resolved === undefined) {
                /*
                 * A name the index does not hold, and a symbol in front of the
                 * reader. Until W7c this ended the recipe with no subject at
                 * all: a ready model, a focused symbol and a refusal on one
                 * screen. It now answers about the focus AND says so, because
                 * the two are only worth having together.
                 */
                if (options.focus !== undefined) {
                    subject = options.focus;
                    /*
                     * Structurally and not as a note: `notes` says what is
                     * MISSING, and this is something that happened. The chat
                     * turns the field into its own line above the answer, which
                     * is where a substitution has to be readable, and a second
                     * copy of the sentence down among the gaps would be a second
                     * place to keep it true.
                     */
                    packet.focusFallback = {
                        asked: classification.subject,
                        used: options.focus.qualifiedName ?? options.focus.name,
                    };
                } else {
                    notes.push(
                        `the index holds no symbol called "${classification.subject}", so no card `
                        + 'describes it.',
                    );
                }
            } else if (resolved.ambiguous) {
                packet.choice = {
                    name: classification.subject,
                    candidates: resolved.candidates.map(candidateOf),
                };
                packet.ambiguous = resolved.alternatives;
                notes.push(ambiguousSubjectNote(
                    classification.subject,
                    resolved.candidates.length,
                ));
            } else {
                subject = resolved.symbol;
                packet.ambiguous = resolved.alternatives;
            }
        }
    } else if (classification.klass !== 'overview' && classification.klass !== 'where-entry') {
        notes.push('the question names no symbol and nothing is in focus.');
    }

    if (subject !== undefined) {
        packet.subject = subject;
        const built = await buildIr(source, root, subject, opts).catch(() => undefined);
        if (built === undefined) {
            notes.push('the facts of the subject could not be read.');
        } else {
            packet.subjectIr = built.ir;
            notes.push(...built.warnings);
        }
    }

    // --- the neighbourhood, the same rule for every class ------------------
    if (packet.subjectIr !== undefined && depth > 0) {
        const first = firstNeighbourhood(packet.subjectIr, notes);
        packet.neighbours = first;
        if (depth === 2) {
            const second = await secondHop(source, root, first, opts, notes);
            packet.neighbours = [...first, ...second];
        }
    } else if (packet.subjectIr !== undefined && depth === 0) {
        notes.push(
            'the neighbourhood is switched off (depth 0), so no caller and no callee is listed.',
        );
    }

    // --- what the class adds on top ---------------------------------------
    if (classification.klass === 'why-error' && packet.neighbours.length > 0) {
        packet.neighbours = await raisesOfCallees(source, root, packet.neighbours, opts, notes);
    }

    if (classification.klass === 'compare') {
        const otherName = classification.other;
        if (otherName === undefined) {
            notes.push('a comparison needs two names and this question carries one.');
        } else {
            const resolved = await resolveSubject(source, root, otherName, opts);
            if (resolved === undefined) {
                notes.push(`the index holds no symbol called "${otherName}".`);
            } else {
                const built = await buildIr(source, root, resolved.symbol, opts).catch(() => undefined);
                packet.compareWith = built === undefined
                    ? { symbol: resolved.symbol }
                    : { symbol: resolved.symbol, ir: built.ir };
            }
        }
    }

    if (classification.klass === 'where-entry' || classification.klass === 'overview') {
        const dto = await source.architectureOverview(root, opts).catch(() => undefined);
        if (dto === undefined) {
            notes.push('the project summary could not be read.');
        } else {
            packet.overview = overviewFactsOf(dto);
            packet.routes = dto.routes.slice(0, ENTRY_CAP);
            packet.entryPoints = dto.entryPoints.slice(0, ENTRY_CAP);
            if (dto.routes.length === 0) {
                notes.push('the index recovered no HTTP route for this project.');
            } else if (dto.routes.length > ENTRY_CAP) {
                notes.push(`${dto.routes.length - ENTRY_CAP} further routes are not listed.`);
            }
            if (dto.entryPoints.length > ENTRY_CAP) {
                notes.push(`${dto.entryPoints.length - ENTRY_CAP} further entry points are not listed.`);
            }
        }
    }

    // --- what a recording saw ---------------------------------------------
    const wantsObserved = classification.klass === 'who-calls'
        || classification.klass === 'what-if'
        || classification.klass === 'why-error';
    if (wantsObserved && subject !== undefined) {
        if (options.observed === undefined) {
            notes.push('no runtime recording was read for this answer.');
        } else {
            const callers = packet.neighbours
                .filter((entry) => entry.direction === 'caller' && entry.hop === 1)
                .slice(0, OBSERVED_CALLER_CAP);
            packet.observed = await options.observed(subject, callers).catch(() => []);
            if (packet.observed.length === 0) {
                notes.push('no imported recording holds a call into this symbol.');
            }
        }
    }

    return packet;
}
