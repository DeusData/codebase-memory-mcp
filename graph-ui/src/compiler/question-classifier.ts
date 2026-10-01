/**
 * Which question was asked, decided without a model.
 *
 * This is the first half of the context compiler and the reason the rest of it
 * can be cheap: the graph does the thinking, the model only phrases. A question
 * is sorted into one of eight classes by its shape, by the symbols it names, by
 * what the reader is looking at and by which mode is open. No inference, no
 * embedding, no round trip; the same words always land in the same class, which
 * is what makes the recipes below it reproducible and the eval comparable.
 *
 * ## Three rules hold the whole file up
 *
 * **The order of the rules is the answer.** A question can carry the words of
 * two classes ("Was passiert, wenn validateUser wirft?" is both a what-if and a
 * why-error), so the patterns are tried in a fixed order and the first match
 * wins. That order is written down in {@link CLASS_RULES} and is part of the
 * contract, not an accident of how the array was typed.
 *
 * **A subject is found, never guessed.** The symbol a recipe centres on comes
 * from an explicit `@mention`, else from a name in backticks or quotes, else
 * from an identifier-shaped word (camelCase or dotted), else from what the
 * reader has in focus. Where it came from travels with it in
 * {@link Classification.subjectFrom}, because "you asked about this" and "you
 * were looking at this" are different claims and the panel says which one it
 * is. A word that is only a word yields no subject at all.
 *
 * **`other` is a real answer.** A question that matches nothing and names
 * nothing is `other`, and the recipe for it says so instead of quietly
 * answering a different question. Falling back to `what-is` on the FOCUS would
 * be the compiler inventing the question, and it still does not happen.
 *
 * ## The one fallback there is, and where its line runs
 *
 * The independent audit of 2026-08-29 asked "How many times was createUser
 * called at runtime?" and got `other` with zero cards. The product stayed
 * honest about it, but a question that spells out a symbol and gets nothing is
 * a recall failure and not a virtue: the reader named the thing they wanted
 * facts about. So a question with no matching rule and no mode now falls back
 * to `what-is` WHEN IT NAMED A SYMBOL ITSELF, through a mention, quotes or an
 * identifier-shaped word, and the rule that fired says
 * {@link SUBJECT_FALLBACK_RULE} so the panel can show where the answer came
 * from.
 *
 * The line runs exactly at `subjectFrom`. A named subject is the reader's own
 * word and asking "what is it" about it answers a question they did ask, in
 * the weakest form the compiler has. A focus subject is what they were looking
 * at a minute ago, and turning that into a question would be the invention the
 * rule above forbids. So "Welche Datenbank benutzt insert?" stays `other`: the
 * lowercase `insert` is a word, not a symbol shape, and this file will not
 * fetch the facts of whatever the index happens to have called `insert` and
 * present them as an answer.
 */

/** The eight families a question can belong to. */
export type QuestionClass =
    /** What is this thing, what does it do. */
    | 'what-is'
    /** Who invokes it, where does it get used. */
    | 'who-calls'
    /** What would a change reach, what happens on this path. */
    | 'what-if'
    /** Where does the program start, which route leads here. */
    | 'where-entry'
    /** Why does this fail, what can be raised here. */
    | 'why-error'
    /** How do two symbols differ. */
    | 'compare'
    /** What is this project, shaped as a whole. */
    | 'overview'
    /** None of the above, and the compiler says so. */
    | 'other';

/** Every class, in the order the rules are tried. Exported so a test can walk it. */
export const QUESTION_CLASSES: readonly QuestionClass[] = [
    'compare',
    'what-if',
    'who-calls',
    'why-error',
    'where-entry',
    'overview',
    'what-is',
    'other',
];

/** Which surface is open, when one is. It only decides questions the words do not. */
export type ActiveMode = 'bug' | 'change' | 'entry' | 'tour' | 'none';

/** What the compiler knows besides the words. */
export interface ClassifierContext {
    /** Bare name of the symbol in the twin, when there is one. */
    focusName?: string | undefined;
    /** Its qualified name, when the index resolved one. */
    focusQualifiedName?: string | undefined;
    /** The panel that is open. Only used when the words decide nothing. */
    mode?: ActiveMode | undefined;
}

/** The sorting, plus everything the recipe below needs to act on it. */
export interface Classification {
    klass: QuestionClass;
    /** Names written as `@name`, in the order they appear, without the sign. */
    mentions: string[];
    /** The symbol the recipe centres on. Absent when nothing named one. */
    subject?: string;
    /** Where {@link subject} came from. `none` when there is no subject. */
    subjectFrom: 'mention' | 'quoted' | 'identifier' | 'focus' | 'none';
    /** A second named symbol, for `compare`. Absent otherwise. */
    other?: string;
    /** Identifier of the rule that fired. The panel shows it under the answer. */
    rule: string;
}

/**
 * One rule: a class, its identifier and the shapes that select it.
 *
 * The patterns are deliberately written as plain lowercase fragments rather
 * than as one clever expression per class. A fragment can be read, argued with
 * and extended by somebody who does not write regular expressions, and this
 * list is the part of the compiler most likely to grow with real questions.
 */
interface ClassRule {
    klass: QuestionClass;
    id: string;
    /**
     * German and English fragments, matched against the folded question.
     *
     * A fragment of one word is matched at word boundaries and a fragment with
     * a space in it is matched as a substring. The distinction is not cosmetic:
     * `route` as a substring fires on `registerUserRoutes`, which would sort
     * "Explain registerUserRoutes" as a question about entry points.
     */
    fragments: readonly string[];
    /** Extra shapes that are easier to state as an expression. */
    patterns?: readonly RegExp[];
}

/**
 * The rules, in the order they are tried.
 *
 * Why this order and not another:
 *
 *  - `compare` first, because a comparison names two symbols and every other
 *    class would happily answer about the first one alone.
 *  - `what-if` before `why-error`, because "was passiert, wenn X wirft" asks
 *    about the consequence and not about the raise.
 *  - `who-calls` before `where-entry`, because "wer ruft die Route auf" is a
 *    caller question that happens to mention a route.
 *  - `where-entry` before `overview`, because "wo faengt das Projekt an" is a
 *    question about entry points and not a request for a summary.
 *  - `what-is` last of the matching rules, because its fragments ("was macht",
 *    "what does") are the most generic in the list and would otherwise swallow
 *    half the others.
 */
export const CLASS_RULES: readonly ClassRule[] = [
    {
        klass: 'compare',
        id: 'compare-words',
        fragments: [
            'unterschied', 'unterscheiden', 'vergleich', 'vergleiche', 'versus',
            'difference between', 'differ', 'compare', 'comparison',
        ],
        patterns: [/\bvs\.?\b/],
    },
    {
        klass: 'what-if',
        id: 'what-if-words',
        fragments: [
            'was passiert, wenn', 'was passiert wenn', 'was waere, wenn', 'was waere wenn',
            'was wäre, wenn', 'was wäre wenn', 'wenn ich', 'wenn sich', 'aendere ich', 'ändere ich',
            'auswirkung', 'auswirkungen', 'folgen hat', 'betrifft eine aenderung',
            'what happens if', 'what happens when', 'what if', 'if i change', 'if this changes',
            'blast radius', 'impact of', 'affected by a change', 'ripple',
        ],
    },
    {
        klass: 'who-calls',
        id: 'who-calls-words',
        fragments: [
            'wer ruft', 'wer benutzt', 'wer verwendet', 'wer nutzt', 'von wo wird',
            'woher wird', 'aufrufer', 'aufgerufen von', 'wird aufgerufen',
            'who calls', 'who invokes', 'who uses', 'called by', 'callers of', 'call sites',
            'used by', 'where is it called', 'where is this called',
        ],
    },
    {
        klass: 'why-error',
        id: 'why-error-words',
        fragments: [
            'warum schlaegt', 'warum schlägt', 'warum scheitert', 'warum faellt', 'warum fällt',
            'welcher fehler', 'welche fehler', 'fehlerpfad', 'wirft', 'erhebt',
            'ausnahme', 'exception', 'fehlschlag',
            'which error', 'what errors', 'error path',
            'throws', 'throw', 'raises', 'raise', 'crash',
        ],
        patterns: [/\bfail(s|ed|ing|ure)?\b/, /\bfehl(t|te|schlaegt|geschlagen)\b/],
    },
    {
        klass: 'where-entry',
        id: 'where-entry-words',
        fragments: [
            'wo faengt', 'wo fängt', 'wo beginnt', 'wo starte', 'wo fange ich an',
            'einstieg', 'einstiegspunkt', 'einstiegspunkte', 'startpunkt', 'route', 'routen',
            'entry point', 'entry points', 'where do i start', 'where does it start',
            'where does the program start', 'starting point', 'endpoint', 'endpoints', 'http route',
        ],
    },
    {
        klass: 'overview',
        id: 'overview-words',
        fragments: [
            'ueberblick', 'überblick', 'architektur', 'aufbau des projekts', 'struktur des projekts',
            'was ist das fuer ein projekt', 'was ist das für ein projekt', 'worum geht es hier',
            'overview', 'architecture', 'big picture', 'shape of the project',
            'what is this project', 'what does this project', 'how is this project',
        ],
    },
    {
        klass: 'what-is',
        id: 'what-is-words',
        fragments: [
            'was ist', 'was macht', 'was tut', 'wozu dient', 'erklaere', 'erkläre', 'erklaer',
            'beschreibe', 'wofuer ist', 'wofür ist',
            'what is', 'what does', 'what do', 'explain', 'describe', 'purpose of', 'tell me about',
        ],
    },
];

/**
 * How a mode maps to a class when the words decided nothing.
 *
 * A weaker signal than the words on purpose: an open panel says what the reader
 * was doing a minute ago, not what they just typed. It is consulted only after
 * every rule above has failed.
 */
export const MODE_CLASSES: Readonly<Record<Exclude<ActiveMode, 'none'>, QuestionClass>> = {
    bug: 'why-error',
    change: 'what-if',
    entry: 'where-entry',
    tour: 'overview',
};

/**
 * Words that look like identifiers but are not symbols.
 *
 * Short and English-and-German only, because the list is a filter over a shape
 * that is already narrow: a candidate has to be camelCase or dotted to get
 * here at all. Anything longer would start refusing real symbol names.
 */
export const SUBJECT_STOP_WORDS: ReadonlySet<string> = new Set([
    'ich', 'du', 'was', 'wer', 'wie', 'wo', 'warum', 'wenn', 'das', 'der', 'die',
    'this', 'that', 'what', 'who', 'how', 'why', 'where', 'when', 'the', 'it',
    'todo', 'http', 'https',
]);

/**
 * The identifier of the rule that catches a named symbol no other rule wanted.
 *
 * Exported because two readers need it by name: the panel, which prints the
 * rule under the answer, and the test that holds the audit's two questions.
 */
export const SUBJECT_FALLBACK_RULE = 'named-subject-fallback';

/** Everything lowercased and with the German sharp s folded, once, in one place. */
function fold(question: string): string {
    return question.toLowerCase().replace(/ß/g, 'ss');
}

/**
 * The `@mentions` of a question, in the order they were written.
 *
 * A mention is the one way a reader can name a symbol the compiler must fetch
 * even when it is nowhere near the focus, which is why the answer contract asks
 * the model to point at this syntax when a card is missing.
 */
export function mentionsIn(question: string): string[] {
    const found: string[] = [];
    const pattern = /@([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)/g;
    let match = pattern.exec(question);
    while (match !== null) {
        found.push(match[1]);
        match = pattern.exec(question);
    }
    return found;
}

/** Names written in backticks, single quotes or double quotes. */
export function quotedNamesIn(question: string): string[] {
    const found: string[] = [];
    const pattern = /[`'"]([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)[`'"]/g;
    let match = pattern.exec(question);
    while (match !== null) {
        found.push(match[1]);
        match = pattern.exec(question);
    }
    return found;
}

/**
 * Words shaped like a symbol: camelCase, PascalCase or dotted.
 *
 * A plain lowercase word is deliberately never a candidate. `createUser` is a
 * symbol and `user` is a noun, and a compiler that could not tell them apart
 * would fetch the facts of whatever the index happens to have called `user`
 * and present them as an answer about the reader's sentence.
 */
export function identifierNamesIn(question: string): string[] {
    const found: string[] = [];
    const pattern = /(?:^|[^\w@`'".])([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+|[a-z][A-Za-z0-9_]*[A-Z][A-Za-z0-9_]*|[A-Z][a-z0-9_]*[A-Z][A-Za-z0-9_]*)/g;
    let match = pattern.exec(question);
    while (match !== null) {
        const candidate = match[1];
        if (!SUBJECT_STOP_WORDS.has(candidate.toLowerCase())) {
            found.push(candidate);
        }
        pattern.lastIndex = match.index + match[0].length;
        match = pattern.exec(question);
    }
    return found;
}

/** Every named candidate, strongest source first, without duplicates. */
function namedCandidates(question: string): { name: string; from: Classification['subjectFrom'] }[] {
    const out: { name: string; from: Classification['subjectFrom'] }[] = [];
    const seen = new Set<string>();
    const push = (name: string, from: Classification['subjectFrom']): void => {
        if (!seen.has(name)) {
            seen.add(name);
            out.push({ name, from });
        }
    };
    for (const name of mentionsIn(question)) {
        push(name, 'mention');
    }
    for (const name of quotedNamesIn(question)) {
        push(name, 'quoted');
    }
    for (const name of identifierNamesIn(question)) {
        push(name, 'identifier');
    }
    return out;
}

/**
 * One fragment as a compiled matcher, built once for the life of the module.
 *
 * A single word gets word boundaries; anything with a space stays a substring.
 * The reasoning is on {@link ClassRule.fragments}.
 */
const FRAGMENT_MATCHERS: ReadonlyMap<string, RegExp> = new Map(
    CLASS_RULES.flatMap((rule) => rule.fragments)
        .filter((fragment) => /^[a-z]+$/.test(fragment))
        .map((fragment) => [fragment, new RegExp(`\\b${fragment}\\b`)]),
);

/** Whether one fragment fires on the folded question. */
export function fragmentMatches(folded: string, fragment: string): boolean {
    const word = FRAGMENT_MATCHERS.get(fragment);
    return word === undefined ? folded.includes(fragment) : word.test(folded);
}

/** Whether one rule fires on the folded question. */
function matches(rule: ClassRule, folded: string): boolean {
    for (const fragment of rule.fragments) {
        if (fragmentMatches(folded, fragment)) {
            return true;
        }
    }
    for (const pattern of rule.patterns ?? []) {
        if (pattern.test(folded)) {
            return true;
        }
    }
    return false;
}

/**
 * Sort one question, deterministically.
 *
 * Total: every input produces a classification, including the empty string,
 * which is `other` with no subject. Nothing here throws and nothing here is
 * asynchronous; the whole file is a pure function over a string and a context.
 */
export function classifyQuestion(question: string, context: ClassifierContext = {}): Classification {
    const folded = fold(question);
    const candidates = namedCandidates(question);
    const mentions = mentionsIn(question);

    let klass: QuestionClass | undefined;
    let rule = '';
    for (const entry of CLASS_RULES) {
        if (matches(entry, folded)) {
            klass = entry.klass;
            rule = entry.id;
            break;
        }
    }

    if (klass === undefined) {
        const mode = context.mode ?? 'none';
        if (mode !== 'none') {
            klass = MODE_CLASSES[mode];
            rule = `mode-${mode}`;
        }
    }

    // A bare `@name` with nothing else around it is a request to look at that
    // symbol, which is the what-is question written in the shortest possible
    // way. It keeps its own rule id because it is the shape and not the
    // fallback below: everything around the name was empty.
    if (klass === undefined && mentions.length > 0 && folded.replace(/@[\w.]+/g, '').trim().length === 0) {
        klass = 'what-is';
        rule = 'bare-mention';
    }
    const first = candidates[0];
    /*
     * The recall fallback. It reads `candidates` and never the focus, and the
     * head of this file says why that line is where it is.
     */
    if (klass === undefined && first !== undefined) {
        klass = 'what-is';
        rule = SUBJECT_FALLBACK_RULE;
    }
    if (klass === undefined) {
        klass = 'other';
        rule = 'no-rule';
    }

    const subject = first?.name ?? (context.focusName !== undefined && context.focusName.length > 0
        ? context.focusName
        : undefined);
    const subjectFrom: Classification['subjectFrom'] =
        first !== undefined ? first.from : subject === undefined ? 'none' : 'focus';

    const result: Classification = { klass, mentions, subjectFrom, rule };
    if (subject !== undefined) {
        result.subject = subject;
    }
    // The second name only means something for a comparison. Carrying it
    // everywhere would invite a recipe to fetch a symbol nobody asked about.
    if (klass === 'compare' && candidates[1] !== undefined) {
        result.other = candidates[1].name;
    }
    return result;
}
