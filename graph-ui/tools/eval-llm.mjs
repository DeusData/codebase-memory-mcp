#!/usr/bin/env node
/*
 * The head-to-head: forty-four golden questions, six candidates, one card set
 * each, temperature zero and a fixed seed.
 *
 * This is the measurement PLAN paragraph 5 promises ("der Sieger der eigenen
 * Eval schlaegt den Benchmark-Ruf") and the one the model ADR was left open
 * for. It answers one question and refuses to answer any other: **given the
 * same compiled cards, which of these six models phrases the most answers
 * correctly without inventing anything.**
 *
 * ## What is held constant, and why that is the whole design
 *
 * Every model sees the SAME cards for a question of its class. The cards are
 * compiled once per model class (A gets the 3000-token budget, B gets 8000),
 * before any model starts, by the product's own compiler through
 * tools/lib/compiler-bundle.mjs. Nothing about the graph, the classifier, the
 * recipe or the card wording can therefore differ between two candidates, which
 * means a difference in score is a difference between the models and not
 * between two runs of an index.
 *
 * Temperature zero and seed 42 are set in src/chat/chat-client.ts, which is the
 * same file the product's chat uses. Non-thinking is the per-model switch
 * measured in src/compiler/prompt-contract.ts.
 *
 * ## What is scored
 *
 * Three things per question, and an answer has to survive all three to pass:
 *
 *  1. every expected substring appears in the answer,
 *  2. no forbidden substring appears,
 *  3. the citation contract holds: every claim line carries a [K], and every
 *     cited card was actually handed over.
 *
 * Rule three is scored separately as well, because it is the one that decides
 * whether this product can ship a model at all. A model that is right and
 * uncited is a model whose answers nobody can check.
 *
 * ## Citation compliance counts the answers it could read, and says how many it could not
 *
 * Since W10, `citationCompliance` is the share of MEASURED checks, and
 * `citationUnmeasured` names how many were not measured. The difference is the
 * finding from W7c (see `measured` in src/compiler/answer-contract.ts): an
 * answer of exactly one line that the token ceiling cut off leaves the check
 * with nothing to look at, and zero violations out of zero lines is not
 * compliance. Counting it as clean would flatter the model; counting it as a
 * violation would score the ceiling. It falls out of the rate, and the number
 * that fell out stands beside the rate so nobody reads a recommendation off a
 * sample that quietly shrank. `passRate` is untouched by this: an answer that
 * could not be measured for citations still misses its expected substrings and
 * still fails there.
 *
 * ## The bounds are a gate, not a remark
 *
 * Since 2026-08-29 (audit finding 6) this run ENFORCES the two bounds the ADR
 * has always claimed: a class winner under passRate 0.6 or under citation 0.9
 * ends this run with a non-zero exit and a named reason. The numbers live in
 * tools/lib/eval-bounds.mjs, they are written into the report as `bounds`, and
 * tools/eval-check.mjs holds the recorded winners to the same two lines. Before
 * that they existed only inside a frozen acceptance test, which reads a file and
 * cannot stop a run: a compiler change that sank the citation rate would have
 * crowned a winner anyway and reported success.
 *
 * ## Two questions are answered without a model, on purpose
 *
 * `honest-01-de` and `honest-02-en` name nothing the index holds, so the
 * compiler produces no cards and the chat returns the agreed sentence without
 * sending a request. That is the product's behaviour, so it is measured as the
 * product's behaviour; the count is reported per model as
 * `deterministicAnswers` so nobody has to work out why six models agree exactly
 * twice.
 *
 * ## Ports
 *
 * The C-server takes a free port from 4310 up. Each model takes a free port
 * from 4400 up: the product port 4141 stays free throughout, so the UI keeps
 * working and a crashed run cannot leave a model sitting on the port the
 * sidecar manager polls.
 *
 * Run: node tools/eval-llm.mjs [--dry] [--only <id-prefix>] [--models a,b]
 *   --dry    compile the cards and score nothing: no model is started
 *   --only   run a subset of the questions, by id prefix
 *   --models run a subset of the candidates, by key
 */

import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
    countListeners,
    findFreePort,
    indexRepository,
    sleep,
    startServer,
    stopServer,
} from './lib/cbm-server.mjs';
import { BUNDLE_DIR, loadCompiler } from './lib/compiler-bundle.mjs';
import { EVAL_BOUNDS, boundViolations } from './lib/eval-bounds.mjs';
import { EVAL_PORT_FLOOR, llamaProps, startLlama, stopLlama } from './lib/llama.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const QUESTIONS = join(ROOT, 'eval', 'questions.json');
const OUT_DIR = join(ROOT, 'verification', 'w5');
const OUT_JSON = join(OUT_DIR, 'eval.json');
const PROJECT = 'codeatlasweb-w5b-eval';
const SERVER_PORT_FLOOR = 4310;

/**
 * The six candidates: the four of ADR 0001 plus the two the user nominated.
 *
 * `class` is the budget class and it is the same split llm/start.sh makes, for
 * the same reason: the context window decides how much context there is.
 *
 * `key` is what `--models` selects by, and it is the same word llm/start.sh
 * takes. It used to be `1b` and `4b`; audit finding 17 of 2026-08-29 pointed out
 * that the class A winner is a 2B model, so calling its selector "1b" states a
 * size the file does not have. `was` keeps the old word working, because a note
 * or a shell history that says `--models 1b` should not break over a label.
 */
const CANDIDATES = [
    { key: 'class-a', was: '1b', name: 'Qwen3.5-2B', file: 'Qwen3.5-2B-Q4_K_M.gguf', class: 'A', ctx: 3072 },
    { key: 'class-a-lfm', was: '1b-lfm', name: 'LFM2.5-1.2B', file: 'LFM2.5-1.2B-Instruct-Q4_K_M.gguf', class: 'A', ctx: 3072 },
    { key: 'class-a-minicpm', was: '1b-minicpm', name: 'MiniCPM5-1B', file: 'MiniCPM5-1B-Q4_K_M.gguf', class: 'A', ctx: 3072 },
    { key: 'class-a-coder', was: '1b-coder', name: 'Qwen2.5-Coder-1.5B', file: 'qwen2.5-coder-1.5b-instruct-q4_k_m.gguf', class: 'A', ctx: 3072 },
    { key: 'class-b', was: '4b', name: 'Qwen3.5-4B', file: 'Qwen3.5-4B-Q4_K_M.gguf', class: 'B', ctx: 8192 },
    { key: 'class-b-gemma', was: '4b-gemma', name: 'gemma-4-E4B', file: 'gemma-4-E4B-it-Q4_K_M.gguf', class: 'B', ctx: 8192 },
];

/**
 * What was changed between measured passes, and what it cost or bought.
 *
 * Written into the report because the numbers below are meaningless without it:
 * the interesting finding of this cycle is not that one model scored 0.68, it
 * is which changes to the CARDS and the PROMPT moved the whole field, and in
 * which direction. Every entry names a change to the compiler, never to a
 * question: the golden questions were fixed before the first model started and
 * were not touched afterwards.
 */
const ITERATIONS = [
    {
        pass: 1,
        change: 'first prompt: numbered rules plus a worked example built from fixture names',
        measured: 'Qwen3.5-2B on the caller questions: passRate 0.625, citation 0.75',
        finding: 'headings ("The symbols that call listUsers are:") and closing lines carried no '
            + 'citation; one model wrote "userCreate" where the card said "createUser".',
    },
    {
        pass: 2,
        change: 'added rules against headings and closing lines, against re-spelling a name, and '
            + 'against answering with cards about something else',
        measured: 'Qwen3.5-2B over all 44: passRate 0.705, citation 0.841',
        finding: 'the worked example was being copied: models put the fixture names from the '
            + 'example into answers about entirely different symbols.',
    },
    {
        pass: 3,
        change: 'example replaced with placeholder names (alphaWorker, betaHandler)',
        measured: 'Qwen3.5-2B: passRate 0.114, citation 0.227',
        finding: 'the placeholders were copied verbatim as well. An example is a pattern to '
            + 'continue, whatever it contains.',
    },
    {
        pass: 4,
        change: 'example removed entirely; the shape stated as a rule',
        measured: 'Qwen3.5-2B: passRate 0.205, citation 0.295',
        finding: 'with no shape to follow the models copied the shape of the cards and wrote '
            + '"K2: registerUserRoutes ...", where the number is not a citation.',
    },
    {
        pass: 5,
        change: 'CARDS now labelled [K1], [K2] in citation syntax instead of "K1:"; a slot '
            + 'template for the answer line; the counting sentence moved to the last line of '
            + 'the subject card; repeat penalty 1.1 and a truncation-aware citation check',
        measured: 'Qwen3.5-2B: passRate 0.636, citation 1.0',
        finding: 'the cheapest continuation of a labelled card list is now a compliant citation. '
            + 'The slot text itself still leaked into some answers.',
    },
    {
        pass: 6,
        change: 'rule against writing the slot markers; the fallback sentence described instead '
            + 'of quoted; the fallback exemption in the checker narrowed to a lone line',
        measured: 'Qwen3.5-2B: passRate 0.682, citation 0.932; the reported run',
        finding: 'both class winners clear the acceptance bounds. The remaining failures are '
            + 'model limitations, not contract violations: names dropped from an answer, or one '
            + 'caller named where the cards list three.',
    },
];

const log = (...parts) => console.log('[eval-llm]', ...parts);

function parseArgs(argv) {
    const args = { dry: false, only: '', models: '' };
    for (let i = 0; i < argv.length; i += 1) {
        if (argv[i] === '--dry') {
            args.dry = true;
        } else if (argv[i] === '--only') {
            args.only = argv[i + 1] ?? '';
            i += 1;
        } else if (argv[i] === '--models') {
            args.models = argv[i + 1] ?? '';
            i += 1;
        }
    }
    return args;
}

/** Case-insensitive containment. Names, paths and numbers, never phrasing. */
const contains = (haystack, needle) => haystack.toLowerCase().includes(needle.toLowerCase());

/** Mean of a list, or zero for an empty one. Never a division by nothing. */
const mean = (values) =>
    values.length === 0 ? 0 : values.reduce((sum, value) => sum + value, 0) / values.length;

/** Rounded to three places, so a report does not carry sixteen digits of noise. */
const round = (value) => Math.round(value * 1000) / 1000;

async function main() {
    const args = parseArgs(process.argv.slice(2));
    const startedAt = Date.now();
    let serverChild = null;
    let home = null;
    let runtimeDir = null;
    let serverPort = 0;
    let failure = null;
    const serverLog = [];
    const models = [];
    const extras = { compile: {}, iterations: ITERATIONS, leftovers: [] };

    try {
        if (!existsSync(BINARY)) {
            throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
        }
        if (!existsSync(FIXTURE)) {
            throw new Error(`Fixture fehlt: ${FIXTURE}`);
        }

        const questions = JSON.parse(await readFile(QUESTIONS, 'utf8'));
        const selected = args.only.length === 0
            ? questions
            : questions.filter((entry) => entry.id.startsWith(args.only));
        log(`${selected.length} von ${questions.length} Fragen`);

        // ------------------------------------------------- HOME und Index --
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w5b-eval-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w5b-eval-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${PROJECT} (${indexed.nodes} Knoten, ${indexed.edges} Kanten)`);

        serverPort = await findFreePort(SERVER_PORT_FLOOR);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        log(`C-Server auf ${serverPort}`);

        // ------------------------------------------- der Context-Compiler --
        const { module: compiler, inputs } = await loadCompiler();
        extras.compilerModules = inputs;
        const provider = compiler.makeProvider(`http://127.0.0.1:${serverPort}`, 1);
        const root = compiler.ATLAS_WORKSPACE_ROOT;
        const opts = { projectName: PROJECT, generation: 1 };

        // Der Fokus jeder Frage, einmal aufgeloest. `focusQn` ist
        // projektrelativ notiert, damit die Datei nicht den Projektnamen dieses
        // Laufs traegt; das Praefix kommt hier davor.
        const focusOf = new Map();
        for (const entry of selected) {
            const resolved = await compiler.resolveSubject(
                provider, root, `${PROJECT}.${entry.focusQn}`, opts,
            );
            if (resolved === undefined) {
                throw new Error(`focusQn nicht aufloesbar: ${entry.focusQn} (${entry.id})`);
            }
            focusOf.set(entry.id, resolved.symbol);
        }
        log(`${focusOf.size} Fokus-Symbole aufgeloest`);

        /*
         * Die Fakten einmal je Frage, die Karten einmal je Modellklasse.
         *
         * Der Schnitt liegt genau dort, weil das Budget die Fakten nicht
         * beeinflusst: der Graph wird gefragt, was die Frageklasse braucht, und
         * erst der Kartenschreiber kappt auf 3000 oder 8000 Token. Ein Packet je
         * Klasse zu holen hiesse, denselben Graphen zweimal zu befragen, und
         * schlimmer: zwei Klassen koennten dann auf zwei verschiedenen Lesungen
         * desselben Index sitzen.
         */
        const packetStarted = Date.now();
        const packets = new Map();
        for (const entry of selected) {
            const focus = focusOf.get(entry.id);
            packets.set(entry.id, await compiler.compileFacts(provider, root, entry.question, {
                depth: entry.depth,
                context: {
                    focusName: focus.name,
                    focusQualifiedName: focus.qualifiedName,
                    mode: 'none',
                },
                focus,
                ...opts,
            }));
        }
        extras.packetMs = Date.now() - packetStarted;
        log(`${packets.size} Fakten-Pakete in ${extras.packetMs} ms`);

        const compiled = { A: new Map(), B: new Map() };
        for (const modelClass of ['A', 'B']) {
            const compileStarted = Date.now();
            for (const entry of selected) {
                const packet = packets.get(entry.id);
                const cards = compiler.compileCards(packet, {
                    budget: compiler.cardBudgetOf(modelClass),
                });
                compiled[modelClass].set(entry.id, {
                    packet,
                    cards,
                    plan: cards.cards.length === 0 ? undefined : compiler.buildPrompt({
                        question: entry.question,
                        klass: packet.klass,
                        cards,
                        modelName: 'compile-only',
                    }),
                });
            }
            extras.compile[modelClass] = {
                ms: Date.now() - compileStarted,
                budget: compiler.cardBudgetOf(modelClass),
                meanCards: round(mean([...compiled[modelClass].values()].map((c) => c.cards.cards.length))),
                meanTokens: round(mean([...compiled[modelClass].values()].map((c) => c.cards.tokens))),
                maxTokens: Math.max(...[...compiled[modelClass].values()].map((c) => c.cards.tokens)),
                droppedCards: [...compiled[modelClass].values()].reduce((sum, c) => sum + c.cards.dropped, 0),
                noCardQuestions: [...compiled[modelClass].entries()]
                    .filter(([, c]) => c.cards.cards.length === 0)
                    .map(([id]) => id),
                classes: [...compiled[modelClass].entries()]
                    .map(([id, c]) => ({ id, klass: c.packet.klass })),
            };
            log(`Karten fuer Klasse ${modelClass}: `
                + `${extras.compile[modelClass].meanCards} im Schnitt, `
                + `${extras.compile[modelClass].meanTokens} Token`);
        }

        /*
         * Ein Vorabbefund, der keinen Punkt vergibt und trotzdem der wichtigste
         * ist: steht jede erwartete Kernaussage ueberhaupt in den Karten?
         * Steht sie nicht drin, misst die Frage den Index und nicht das Modell,
         * und das waere eine Frage, die kein Modell bestehen kann.
         */
        extras.expectedInCards = selected.map((entry) => {
            const modelClass = 'A';
            const plan = compiled[modelClass].get(entry.id);
            const text = compiler.renderCards(plan.cards.cards);
            const missing = entry.expected.filter(
                (needle) => !contains(text, needle) && !contains(plan.packet.notes.join(' '), needle),
            );
            return { id: entry.id, klass: plan.packet.klass, cards: plan.cards.cards.length, missing };
        });
        const unanswerable = extras.expectedInCards.filter(
            (entry) => entry.missing.length > 0 && entry.cards > 0,
        );
        if (unanswerable.length > 0) {
            log('WARNUNG: erwartete Kernaussagen fehlen in den Karten:',
                JSON.stringify(unanswerable));
        }

        // ------------------------------------------------- die Kandidaten --
        /*
         * Der Trockenlauf faellt NICHT frueh aus dieser Funktion heraus.
         *
         * Ein `return` hier uebersprang das Abraeumen unter dem catch, und ein
         * nicht beendeter C-Server haelt die Ereignisschleife von Node offen:
         * der Lauf war fertig, das Programm endete nie, und von aussen sah
         * beides gleich aus. Also entscheidet eine leere Kandidatenliste, und
         * der Weg nach draussen ist fuer jeden Lauf derselbe.
         */
        const wanted = args.dry
            ? []
            : args.models.length === 0
                ? CANDIDATES
                : CANDIDATES.filter((entry) => {
                    const wanted = args.models.split(',').map((word) => word.trim());
                    return wanted.includes(entry.key) || wanted.includes(entry.was);
                });
        if (args.dry) {
            // Der Trockenlauf schreibt NICHT nach verification/: dort stehen
            // Beweise, und ein Lauf ohne Modell beweist nichts ueber Modelle.
            log('--dry: kein Modell wird gefahren.');
            await writeFile(
                join(BUNDLE_DIR, 'eval-dry.json'),
                JSON.stringify({ extras, generatedAt: new Date().toISOString() }, null, 2) + '\n',
                'utf8',
            );
        }

        for (const candidate of wanted) {
            const modelStarted = Date.now();
            const port = await findFreePort(EVAL_PORT_FLOOR, [serverPort]);
            const llamaLog = [];
            log(`starte ${candidate.name} auf ${port} (Kontext ${candidate.ctx})`);
            const running = await startLlama({
                modelFile: candidate.file,
                contextTokens: candidate.ctx,
                port,
                log: llamaLog,
            });
            const props = await llamaProps(port);
            const origin = `http://127.0.0.1:${port}`;
            const nonThinking = compiler.nonThinkingFor(candidate.name);

            const results = [];
            for (const entry of selected) {
                const plan = compiled[candidate.class].get(entry.id);
                const cardIds = plan.cards.cards.map((card) => card.id);

                if (plan.plan === undefined) {
                    // Keine Karte, also keine Anfrage. Der vereinbarte Satz ist
                    // die Antwort des Produkts, und genau die wird bewertet.
                    const answer = compiler.NO_CARD_SENTENCE;
                    const check = compiler.checkCitations(answer, cardIds);
                    results.push({
                        id: entry.id,
                        klass: plan.packet.klass,
                        deterministic: true,
                        answered: true,
                        answer,
                        cards: cardIds.length,
                        citationOk: check.ok,
                        citationMeasured: check.measured,
                        missing: entry.expected.filter((needle) => !contains(answer, needle)),
                        forbiddenHits: (entry.forbidden ?? []).filter((needle) => contains(answer, needle)),
                        tokensPerSecond: 0,
                        completionTokens: 0,
                        promptTokens: 0,
                        answerChars: answer.length,
                    });
                    continue;
                }

                let reply;
                try {
                    reply = await compiler.askModel({
                        origin,
                        system: plan.plan.system,
                        user: plan.plan.user,
                        chatTemplateKwargs: nonThinking.chatTemplateKwargs,
                        fetch: (url, init) => fetch(url, init),
                    });
                } catch (error) {
                    results.push({
                        id: entry.id,
                        klass: plan.packet.klass,
                        deterministic: false,
                        answered: false,
                        answer: '',
                        error: error instanceof Error ? error.message : String(error),
                        cards: cardIds.length,
                        citationOk: false,
                        /*
                         * Ein Lauf, der keine Antwort hervorgebracht hat, hat
                         * auch keine Zitierdisziplin hervorgebracht. Er faellt
                         * aus der Zitattreue heraus und wird als nicht gemessen
                         * ausgewiesen; die Trefferquote zaehlt ihn weiter als
                         * nicht bestanden, denn dort fehlen alle erwarteten
                         * Namen.
                         */
                        citationMeasured: false,
                        missing: entry.expected,
                        forbiddenHits: [],
                        tokensPerSecond: 0,
                        completionTokens: 0,
                        promptTokens: 0,
                        answerChars: 0,
                    });
                    continue;
                }

                const answer = reply.content;
                /*
                 * Die Kappung wird mitgegeben und nicht verschwiegen: eine
                 * Zeile, die das Token-Limit halbiert hat, ist kein Satz, den
                 * das Modell ohne Beleg stehenlassen wollte. Alles davor wird
                 * voll geprueft.
                 */
                const check = compiler.checkCitations(answer, cardIds, {
                    truncated: reply.truncated,
                });
                results.push({
                    id: entry.id,
                    klass: plan.packet.klass,
                    deterministic: false,
                    answered: answer.length > 0,
                    thoughtOnly: reply.thoughtOnly,
                    truncated: reply.truncated,
                    finishReason: reply.finishReason,
                    answer,
                    cards: cardIds.length,
                    citationOk: check.ok,
                    citationMeasured: check.measured,
                    unknownCitations: check.unknown,
                    uncitedLines: check.violations.filter((v) => v.reason === 'no-citation').length,
                    missing: entry.expected.filter((needle) => !contains(answer, needle)),
                    forbiddenHits: (entry.forbidden ?? []).filter((needle) => contains(answer, needle)),
                    tokensPerSecond: round(
                        compiler.tokensPerSecond(reply.completionTokens, reply.durationMs) ?? 0,
                    ),
                    completionTokens: reply.completionTokens ?? 0,
                    promptTokens: reply.promptTokens ?? 0,
                    estimatedPromptTokens: plan.plan.estimatedTokens,
                    answerChars: answer.length,
                    durationMs: reply.durationMs,
                });
            }

            await stopLlama(running.child);
            await sleep(1500);

            const passed = results.filter(
                (entry) => entry.missing.length === 0
                    && entry.forbiddenHits.length === 0
                    && entry.citationOk,
            );
            const speeds = results.filter((entry) => entry.tokensPerSecond > 0)
                .map((entry) => entry.tokensPerSecond);
            /*
             * Die Zitattreue misst nur, was sie lesen konnte. Siehe den Kopf
             * dieser Datei und `measured` in src/compiler/answer-contract.ts.
             *
             * Der Nenner ist die Zahl der gemessenen Pruefungen und nicht die
             * Zahl der Fragen. Ist keine einzige gemessen, ist die Quote 0 und
             * nicht "keine Fundstelle also perfekt": eine Division ohne Nenner
             * ist keine Zahl, und die freundlichere der beiden Auslegungen waere
             * genau die Schoenung, gegen die dieses Feld geschrieben ist.
             */
            const measuredResults = results.filter((entry) => entry.citationMeasured === true);
            const unmeasured = results.length - measuredResults.length;
            const model = {
                name: candidate.name,
                file: candidate.file,
                modelClass: candidate.class,
                mode: nonThinking.note,
                nonThinking: 'chat_template_kwargs {"enable_thinking": false}',
                passRate: round(passed.length / results.length),
                citationCompliance: measuredResults.length === 0
                    ? 0
                    : round(
                        measuredResults.filter((entry) => entry.citationOk).length
                            / measuredResults.length,
                    ),
                citationMeasuredAnswers: measuredResults.length,
                citationUnmeasured: unmeasured,
                meanTokPerSec: round(mean(speeds)),
                answered: results.filter((entry) => entry.answered).length,
                notes: [
                    `context ${props.contextTokens ?? candidate.ctx} tokens, `
                    + `quantisation ${props.quantization || 'unreported'}, `
                    + `ready in ${running.readyMs} ms on port ${port}`,
                    `${results.filter((entry) => entry.deterministic).length} answers came from the `
                    + 'compiler without a request, because no card covered the question',
                    `${results.filter((entry) => entry.thoughtOnly === true).length} answers were a `
                    + 'monologue with no content',
                    `${results.filter((entry) => entry.truncated === true).length} answers stopped at `
                    + 'the token limit; their last line is not counted as a claim',
                    `${unmeasured} answers left the citation check with no line to look at and are `
                    + `out of the rate above; it is the share of the remaining ${measuredResults.length}`,
                    `mean answer length ${Math.round(mean(results.map((entry) => entry.answerChars)))} characters`,
                ],
                deterministicAnswers: results.filter((entry) => entry.deterministic).length,
                meanAnswerChars: Math.round(mean(results.map((entry) => entry.answerChars))),
                meanPromptTokens: Math.round(mean(
                    results.filter((entry) => entry.promptTokens > 0).map((entry) => entry.promptTokens),
                )),
                meanEstimatedPromptTokens: Math.round(mean(
                    results.filter((entry) => entry.estimatedPromptTokens > 0)
                        .map((entry) => entry.estimatedPromptTokens),
                )),
                totalMs: Date.now() - modelStarted,
                perQuestion: results,
            };
            models.push(model);
            log(`${candidate.name}: passRate ${model.passRate}, `
                + `citation ${model.citationCompliance} von ${model.citationMeasuredAnswers} gemessenen `
                + `(${model.citationUnmeasured} nicht gemessen), ${model.meanTokPerSec} tok/s, `
                + `${model.answered} beantwortet, ${Math.round(model.totalMs / 1000)} s`);
        }
    } catch (err) {
        failure = err;
        console.error('[eval-llm] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[eval-llm] Server-Log:\n' + serverLog.slice(-15).join('\n'));
        }
    }

    await stopServer(serverChild);
    await sleep(600);
    for (const port of [serverPort, 4141].filter((value) => value > 0)) {
        extras.leftovers.push({ port, listeners: await countListeners(port) });
    }

    if (!models.length) {
        if (home) {
            await rm(home, { recursive: true, force: true });
        }
        if (runtimeDir) {
            await rm(runtimeDir, { recursive: true, force: true });
        }
        if (failure !== null) {
            process.exitCode = 1;
        }
        return;
    }

    /*
     * Der Sieger je Klasse: Score ist passRate, Gleichstand entscheidet die
     * Zitattreue, dann das Tempo. Genau die Reihenfolge des Contracts, und sie
     * ist nicht beliebig: ein Modell, das mehr Fragen richtig beantwortet, aber
     * seine Quellen nicht nennt, ist fuer dieses Produkt unbrauchbar, und ein
     * schnelleres Modell mit derselben Trefferquote und derselben Zitattreue
     * ist schlicht das bessere.
     */
    const winnerOf = (modelClass) => {
        const field = models.filter((model) => model.modelClass === modelClass);
        if (field.length === 0) {
            return undefined;
        }
        const ranked = [...field].sort((a, b) =>
            (b.passRate - a.passRate)
            || (b.citationCompliance - a.citationCompliance)
            || (b.meanTokPerSec - a.meanTokPerSec));
        const winner = ranked[0];
        const runnerUp = ranked[1];
        return {
            name: winner.name,
            file: winner.file,
            passRate: winner.passRate,
            citationCompliance: winner.citationCompliance,
            meanTokPerSec: winner.meanTokPerSec,
            begruendung: runnerUp === undefined
                ? `einziger Kandidat der Klasse ${modelClass}.`
                : `passRate ${winner.passRate} gegen ${runnerUp.passRate} (${runnerUp.name}); `
                    + `Zitattreue ${winner.citationCompliance} gegen ${runnerUp.citationCompliance}; `
                    + `${winner.meanTokPerSec} gegen ${runnerUp.meanTokPerSec} tok/s. `
                    + 'Score ist die Trefferquote, Gleichstand entscheidet die Zitattreue, dann das Tempo.',
        };
    };

    const report = {
        temperature: 0,
        seed: 42,
        questionCount: JSON.parse(await readFile(QUESTIONS, 'utf8')).length,
        maxTokens: 220,
        repeatPenalty: 1.1,
        fixture: 'fixtures/atlas-sample (nur gelesen)',
        project: PROJECT,
        serverPort,
        evalPortFloor: EVAL_PORT_FLOOR,
        sidecarPortUntouched: 4141,
        budgets: { classA: 3000, classB: 8000 },
        /*
         * Die Grenzen stehen mit im Bericht, und zwar als Zahlen und nicht als
         * Satz: die Datei ist die Quelle, aus der eval:check spaeter prueft, und
         * eine Grenze, die nur im Werkzeug steht, koennte ein spaeterer Lauf
         * senken, ohne dass die Aufzeichnung es zeigt.
         */
        bounds: EVAL_BOUNDS,
        /*
         * Erzwungen wird nur an einem VOLLEN Lauf: alle Kandidaten, alle Fragen.
         * Ein `--only`- oder `--models`-Ausschnitt misst die Modellwahl nicht,
         * und ein Gate, das an einem Ausschnitt zuschlaegt, waere ein Gate, das
         * man beim Debuggen abstellt. Das Feld steht in der Datei, damit ein
         * Leser einen Ausschnitt nicht fuer eine Messung haelt.
         */
        boundsEnforced: args.only.length === 0 && args.models.length === 0 && !args.dry,
        models: models.map((model) => ({
            name: model.name,
            file: model.file,
            modelClass: model.modelClass,
            mode: model.mode,
            nonThinking: model.nonThinking,
            passRate: model.passRate,
            citationCompliance: model.citationCompliance,
            /*
             * Der Nenner der Zeile darueber und die Zahl, die herausgefallen
             * ist. Beide stehen mit in der Datei, weil eine Quote ohne ihren
             * Nenner nicht nachpruefbar ist.
             */
            citationMeasuredAnswers: model.citationMeasuredAnswers,
            citationUnmeasured: model.citationUnmeasured,
            meanTokPerSec: model.meanTokPerSec,
            answered: model.answered,
            notes: model.notes,
            deterministicAnswers: model.deterministicAnswers,
            meanAnswerChars: model.meanAnswerChars,
            meanPromptTokens: model.meanPromptTokens,
            meanEstimatedPromptTokens: model.meanEstimatedPromptTokens,
            totalMs: model.totalMs,
        })),
        winnerClassA: winnerOf('A'),
        winnerClassB: winnerOf('B'),
        generatedAt: new Date().toISOString(),
        totalMs: Date.now() - startedAt,
        error: failure ? failure.message : null,
        extras: { ...extras, perQuestion: models.map((model) => ({
            name: model.name,
            results: model.perQuestion,
        })) },
    };

    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(OUT_JSON, JSON.stringify(report, null, 2) + '\n', 'utf8');
    log('geschrieben:', OUT_JSON);

    if (home) {
        await rm(home, { recursive: true, force: true });
    }
    if (runtimeDir) {
        await rm(runtimeDir, { recursive: true, force: true });
    }

    /*
     * Das Gate, nach dem Schreiben und vor dem Ende.
     *
     * Nach dem Schreiben, weil ein gerissener Grenzwert erst recht nachlesbar
     * sein muss: die Datei mit den schlechten Zahlen ist der Beweis, aus dem der
     * naechste Zyklus lernt. Und trotzdem ein Abbruch, weil ein Lauf, der eine
     * unbrauchbare Modellwahl mit 0 quittiert, dieselbe Auskunft gibt wie ein
     * Lauf, dessen Sieger taugen.
     */
    const violations = report.boundsEnforced
        ? boundViolations({ A: report.winnerClassA, B: report.winnerClassB })
        : [];
    if (!report.boundsEnforced) {
        log('Teillauf: die Grenzen werden NICHT erzwungen, und die Datei sagt das '
            + '(boundsEnforced false). Ein Feld- oder Fragen-Ausschnitt misst die Modellwahl nicht.');
    }
    if (violations.length > 0) {
        console.error('[eval-llm] Die harten Grenzen der Modellwahl sind gerissen '
            + `(passRate >= ${EVAL_BOUNDS.passRate}, Zitattreue >= ${EVAL_BOUNDS.citationCompliance}):`);
        for (const reason of violations) {
            console.error('  -', reason);
        }
        console.error(`[eval-llm] Herkunft der Grenzen: ${EVAL_BOUNDS.source}`);
        console.error(`[eval-llm] Die Zahlen stehen in ${OUT_JSON}.`);
        process.exitCode = 1;
        return;
    }
    log(`Grenzen gehalten: beide Klassensieger ueber passRate ${EVAL_BOUNDS.passRate} `
        + `und Zitattreue ${EVAL_BOUNDS.citationCompliance}.`);

    if (failure !== null) {
        process.exitCode = 1;
    }
}

main().catch((err) => {
    console.error('[eval-llm] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
