#!/usr/bin/env node
/*
 * Die Eval als Regressionstest: die zwei Sieger noch einmal, gegen die
 * aufgezeichneten Zahlen.
 *
 *   node tools/eval-check.mjs [--out verification/w6/evalcheck.json]
 *
 * Befund 9 des unabhaengigen Audits vom 2026-08-29: PLAN Abschnitt 6.5 verlangt
 * die Eval "als Regressionstest", und sie war ein Handlauf. `npm run eval:llm`
 * faehrt sechs Modelle ueber 44 Fragen und dauert ein paar Minuten; kein Gate
 * rief sie, und die Abnahmetests lasen ausschliesslich die aufgezeichnete Datei.
 * Eine Aenderung am Context-Compiler, die die Zitattreue senkt, waere damit in
 * keinem Lauf aufgefallen: verification/w5/eval.json haette weiter 0.932
 * behauptet, weil sie es im Maerz gemessen hat.
 *
 * Dieser Lauf ist die fehlende Haelfte. Er ist ABSICHTLICH nicht die ganze Eval.
 *
 * ## Was er faehrt und was nicht
 *
 * Er faehrt die ZWEI Sieger (Klasse A und Klasse B, gelesen aus eval.json, nicht
 * hier hartkodiert) ueber ALLE Fragen, mit denselben Karten aus demselben
 * Compiler, Temperatur 0 und Seed 42. Er faehrt NICHT die vier Verlierer: die
 * Frage "welches Modell ist das beste" ist im ADR beantwortet und wird nicht bei
 * jedem Release neu gestellt. Die Frage, die hier gestellt wird, ist eine andere
 * und viel engere: **liefert der Compiler von heute mit dem Modell von damals
 * noch dieselben Zahlen.** Vier weitere Modelle wuerden diese Frage nicht besser
 * beantworten, sondern nur die Laufzeit verdreifachen, bis niemand mehr das Gate
 * faehrt.
 *
 * ## Wie verglichen wird, und warum die zwei Seiten verschieden streng sind
 *
 * **passRate mit Toleranz (+-0.05).** Die Trefferquote haengt an den Wortlauten
 * eines Modells, und dieselbe Bitte kann bei gleicher Temperatur ueber eine
 * andere llama.cpp-Fassung oder einen anderen Rechner ein Wort anders fallen.
 * Eine Toleranz von 0.05 sind bei 44 Fragen gut zwei Fragen: genug fuer das
 * Rauschen, zu wenig fuer eine Regression, die diesen Namen verdient.
 *
 * **Zitattreue hart auf der Grenze.** Sie ist keine Frage des Wortlauts, sondern
 * eine Frage des Formats: entweder jede Behauptungszeile traegt ihr [K], oder
 * nicht. Ein Vertrag, der eine Toleranz bekommt, ist kein Vertrag. Die Grenze
 * ist dieselbe wie in tools/lib/eval-bounds.mjs.
 *
 * Der Vergleich laeuft in BEIDE Richtungen, aber nur die eine ist ein Fehler:
 * eine gestiegene Trefferquote wird gemeldet und laesst den Lauf gruen. Ein Gate,
 * das eine Verbesserung rot faerbt, ist ein Gate, das zu Verschlechterungen
 * erzieht.
 *
 * ## Ports und Prozesse
 *
 * C-Server ab 4360, Modelle ab EVAL_PORT_FLOOR (4400). Der Produktport 4141
 * bleibt frei: dieser Lauf startet dort nichts und redet mit niemandem dort.
 *
 * Gezaehlt werden am Ende NUR die Ports, die dieser Lauf selbst belegt hat.
 *
 * Bis zum 2026-08-29 stand 4141 in derselben Liste wie die eigenen Ports, und
 * jeder Prozess darauf zaehlte als Prozessrest DIESES Laufs. Am 2026-08-29 hat
 * das den Release-Gate-Lauf rot gefaerbt, obwohl beide Vergleiche in Toleranz
 * lagen: auf 4141 lief der Sidecar des Eigentuemers, dazu ein Browser mit einer
 * offenen Verbindung dorthin. Zwei Prozesse, die dieser Lauf weder gestartet
 * hat noch beenden darf, und ein Ergebnis, das etwas anderes behauptete als es
 * gemessen hatte.
 *
 * Ein Pruefer raeumt auf und zaehlt, was er selbst gestartet hat, und sonst
 * nichts. Die Belegung von 4141 steht darum weiter im Bericht, aber als eigenes
 * Feld (`sidecarPort`) und zur Kenntnis: sie sagt etwas ueber den Rechner und
 * nichts ueber diesen Lauf, und sie faerbt ihn nicht.
 *
 * Nebenbei genau gesagt, weil der Name es verschweigt: `countListeners` fragt
 * `lsof -ti tcp:<port>` und bekommt damit jeden Prozess mit einem Socket auf
 * dem Port, Server wie Client. Fuer die eigenen Ports ist das die richtige
 * Strenge (wer dort noch haengt, haengt an etwas, das dieser Lauf geoeffnet
 * hat); fuer einen fremden Port ist es die falsche Frage.
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
import { loadCompiler } from './lib/compiler-bundle.mjs';
import { citationComplianceOf } from './lib/eval-citation-summary.mjs';
import { EVAL_BOUNDS, violationsOf } from './lib/eval-bounds.mjs';
import { EVAL_PORT_FLOOR, llamaProps, startLlama, stopLlama } from './lib/llama.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const QUESTIONS = join(ROOT, 'eval', 'questions.json');
const BASELINE = join(ROOT, 'verification', 'w5', 'eval.json');
const DEFAULT_OUT = join(ROOT, 'verification', 'w6', 'evalcheck.json');
const PROJECT = 'codeatlasweb-w6b-evalcheck';

/** Ports ab hier, so wie es der Zyklus W6b verabredet hat. */
const SERVER_PORT_FLOOR = 4360;

/**
 * Der Produktport. Dieser Lauf benutzt ihn nicht und raeumt ihn nicht auf; er
 * schreibt am Ende nur auf, wie er belegt war. Der Grund steht im Dateikopf.
 */
const SIDECAR_PORT = 4141;

/** Wieviel die Trefferquote schwanken darf, ohne dass es eine Regression ist. */
const PASS_RATE_TOLERANCE = 0.05;

/**
 * Die Kontextfenster je Modellklasse. Dieselbe Aufteilung wie llm/start.sh und
 * tools/eval-llm.mjs, aus demselben Grund: die Klasse entscheidet das Budget.
 */
const CONTEXT_OF = { A: 3072, B: 8192 };

const log = (...parts) => console.log('[eval-check]', ...parts);

/** Case-insensitive containment. Wortgleich mit tools/eval-llm.mjs. */
const contains = (haystack, needle) => haystack.toLowerCase().includes(needle.toLowerCase());

const mean = (values) =>
    values.length === 0 ? 0 : values.reduce((sum, value) => sum + value, 0) / values.length;

const round = (value) => Math.round(value * 1000) / 1000;

function parseArgs(argv) {
    let out = DEFAULT_OUT;
    for (let i = 0; i < argv.length; i += 1) {
        if (argv[i] === '--out') {
            out = resolve(argv[i + 1] ?? DEFAULT_OUT);
            i += 1;
        }
    }
    return { out };
}

async function main() {
    const startedAt = Date.now();
    const { out } = parseArgs(process.argv.slice(2));
    let serverChild = null;
    let home = null;
    let runtimeDir = null;
    let serverPort = 0;
    let failure = null;
    const serverLog = [];
    const measured = [];
    const extras = { leftovers: [], perQuestion: [] };

    /*
     * Die Ports, die dieser Lauf selbst belegt hat, in der Reihenfolge der
     * Belegung. Nur sie werden am Ende auf Reste geprueft. Sie stehen hier und
     * nicht als Konstante, weil `findFreePort` sie zur Laufzeit waehlt: welcher
     * Port es wird, weiss der Lauf erst, wenn er ihn hat.
     */
    const ownPorts = [];

    try {
        if (!existsSync(BINARY)) {
            throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
        }
        if (!existsSync(FIXTURE)) {
            throw new Error(`Fixture fehlt: ${FIXTURE}`);
        }
        if (!existsSync(BASELINE)) {
            throw new Error(`${BASELINE} fehlt: erst 'npm run eval:llm' fahren.`);
        }

        const baseline = JSON.parse(await readFile(BASELINE, 'utf8'));
        /*
         * Wer gefahren wird, steht in der Aufzeichnung und nicht hier.
         *
         * Ein hartkodierter Modellname waere die eine Stelle, an der dieser Lauf
         * gegen ein anderes Modell messen koennte als das, das ausgewaehlt wurde,
         * und niemand saehe es: die Datei nennt die Zahlen des Siegers, der Lauf
         * fuehre einen anderen, und die Toleranz verdeckte den Rest.
         */
        const winners = [
            { modelClass: 'A', recorded: baseline.winnerClassA },
            { modelClass: 'B', recorded: baseline.winnerClassB },
        ];
        for (const entry of winners) {
            if (entry.recorded === undefined || entry.recorded === null) {
                throw new Error(`eval.json nennt keinen Sieger der Klasse ${entry.modelClass}`);
            }
            if (!existsSync(join(ROOT, 'models', entry.recorded.file))) {
                throw new Error(`Modell fehlt: models/${entry.recorded.file}`);
            }
        }
        extras.baseline = {
            generatedAt: baseline.generatedAt ?? '',
            temperature: baseline.temperature,
            seed: baseline.seed,
            questionCount: baseline.questionCount,
        };
        log(`Sieger aus eval.json: ${winners.map((e) => `${e.modelClass}=${e.recorded.name}`).join(', ')}`);

        const questions = JSON.parse(await readFile(QUESTIONS, 'utf8'));
        log(`${questions.length} goldene Fragen`);

        // ------------------------------------------------- HOME und Index --
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w6b-evalcheck-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w6b-evalcheck-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${PROJECT} (${indexed.nodes} Knoten, ${indexed.edges} Kanten)`);

        serverPort = await findFreePort(SERVER_PORT_FLOOR);
        ownPorts.push(serverPort);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        log(`C-Server auf ${serverPort}`);

        // ------------------------------------------- der Context-Compiler --
        const { module: compiler, inputs } = await loadCompiler();
        extras.compilerModules = inputs;
        const provider = compiler.makeProvider(`http://127.0.0.1:${serverPort}`, 1);
        const root = compiler.ATLAS_WORKSPACE_ROOT;
        const opts = { projectName: PROJECT, generation: 1 };

        const focusOf = new Map();
        for (const entry of questions) {
            const resolved = await compiler.resolveSubject(
                provider, root, `${PROJECT}.${entry.focusQn}`, opts,
            );
            if (resolved === undefined) {
                throw new Error(`focusQn nicht aufloesbar: ${entry.focusQn} (${entry.id})`);
            }
            focusOf.set(entry.id, resolved.symbol);
        }

        const packets = new Map();
        for (const entry of questions) {
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
        log(`${packets.size} Fakten-Pakete gebaut`);

        /*
         * Die Klassen der Fragen kommen mit in den Bericht.
         *
         * Sie sind der zweite Grund, aus dem dieser Lauf existiert: eine
         * Aenderung am Klassifikator kann die Trefferquote senken, ohne dass ein
         * einziges Modell sich anders verhaelt, und dann steht hier, welche Frage
         * in eine andere Klasse gerutscht ist statt nur, dass die Zahl fiel.
         */
        extras.classes = questions.map((entry) => ({
            id: entry.id,
            declared: entry.class ?? '',
            compiled: packets.get(entry.id).klass,
        }));
        extras.classDrift = extras.classes.filter(
            (entry) => entry.declared.length > 0 && entry.declared !== entry.compiled,
        );
        if (extras.classDrift.length > 0) {
            log('WARNUNG: Frageklassen weichen von eval/questions.json ab:',
                JSON.stringify(extras.classDrift));
        }

        const compiled = { A: new Map(), B: new Map() };
        for (const modelClass of ['A', 'B']) {
            for (const entry of questions) {
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
        }

        // --------------------------------------------------- die zwei Sieger --
        for (const { modelClass, recorded } of winners) {
            const modelStarted = Date.now();
            const port = await findFreePort(EVAL_PORT_FLOOR, [serverPort]);
            ownPorts.push(port);
            const llamaLog = [];
            const ctx = CONTEXT_OF[modelClass];
            log(`starte ${recorded.name} auf ${port} (Kontext ${ctx})`);
            const running = await startLlama({
                modelFile: recorded.file,
                contextTokens: ctx,
                port,
                log: llamaLog,
            });
            const props = await llamaProps(port);
            const origin = `http://127.0.0.1:${port}`;
            const nonThinking = compiler.nonThinkingFor(recorded.name);

            const results = [];
            for (const entry of questions) {
                const plan = compiled[modelClass].get(entry.id);
                const cardIds = plan.cards.cards.map((card) => card.id);

                if (plan.plan === undefined) {
                    const answer = compiler.NO_CARD_SENTENCE;
                    const check = compiler.checkCitations(answer, cardIds);
                    results.push({
                        id: entry.id,
                        klass: plan.packet.klass,
                        deterministic: true,
                        check,
                        citationOk: check.ok,
                        missing: entry.expected.filter((needle) => !contains(answer, needle)),
                        forbiddenHits: (entry.forbidden ?? []).filter((needle) => contains(answer, needle)),
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
                        citationOk: false,
                        missing: entry.expected,
                        forbiddenHits: [],
                        error: error instanceof Error ? error.message : String(error),
                    });
                    continue;
                }

                const answer = reply.content;
                const check = compiler.checkCitations(answer, cardIds, { truncated: reply.truncated });
                results.push({
                    id: entry.id,
                    klass: plan.packet.klass,
                    deterministic: false,
                    check,
                    citationOk: check.ok,
                    truncated: reply.truncated === true,
                    missing: entry.expected.filter((needle) => !contains(answer, needle)),
                    forbiddenHits: (entry.forbidden ?? []).filter((needle) => contains(answer, needle)),
                    answerChars: answer.length,
                    tokensPerSecond: round(
                        compiler.tokensPerSecond(reply.completionTokens, reply.durationMs) ?? 0,
                    ),
                });
            }

            await stopLlama(running.child);
            await sleep(1500);

            const passed = results.filter(
                (entry) => entry.missing.length === 0
                    && entry.forbiddenHits.length === 0
                    && entry.citationOk,
            );
            const speeds = results.filter((entry) => (entry.tokensPerSecond ?? 0) > 0)
                .map((entry) => entry.tokensPerSecond);
            const passRate = round(passed.length / results.length);
            const citationSummary = citationComplianceOf(results);
            const citation = round(citationSummary.citationCompliance);
            measured.push({
                modelClass,
                name: recorded.name,
                file: recorded.file,
                contextTokens: props.contextTokens ?? ctx,
                quantisation: props.quantization || 'unreported',
                port,
                readyMs: running.readyMs,
                questions: results.length,
                passRate,
                citationCompliance: citation,
                citationMeasured: citationSummary.citationMeasured,
                citationUnmeasured: citationSummary.citationUnmeasured,
                meanTokPerSec: round(mean(speeds)),
                failedQuestions: results
                    .filter((entry) => entry.missing.length > 0 || entry.forbiddenHits.length > 0
                        || !entry.citationOk)
                    .map((entry) => entry.id),
                totalMs: Date.now() - modelStarted,
            });
            extras.perQuestion.push({ name: recorded.name, results });
            log(`${recorded.name}: passRate ${passRate} (aufgezeichnet ${recorded.passRate}), `
                + `Zitattreue ${citation} (aufgezeichnet ${recorded.citationCompliance}), `
                + `${Math.round((Date.now() - modelStarted) / 1000)} s`);
        }
    } catch (err) {
        failure = err;
        console.error('[eval-check] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[eval-check] Server-Log:\n' + serverLog.slice(-15).join('\n'));
        }
    }

    await stopServer(serverChild);
    await sleep(600);
    /*
     * Die eigenen Ports. Jeder Prozess, der hier noch haengt, haengt an etwas,
     * das dieser Lauf geoeffnet hat, und ist damit sein Rest. Ein Port, den der
     * Lauf nie belegt hat, kommt in dieser Liste nicht vor.
     */
    for (const port of ownPorts.filter((value) => value > 0)) {
        extras.leftovers.push({ port, listeners: await countListeners(port) });
    }
    const leftoverProcesses = extras.leftovers.reduce((sum, entry) => sum + entry.listeners, 0);

    /*
     * Und der Produktport, zur Kenntnis und ohne Folgen. Wer ihn belegt, hat
     * das getan, bevor dieser Lauf begann, und wird es tun, nachdem er zu Ende
     * ist. Die Zahl steht hier, damit ein spaeterer Leser den Rechner sieht,
     * auf dem gemessen wurde; sie geht in `reasons` nicht ein.
     */
    const sidecarPort = {
        port: SIDECAR_PORT,
        sockets: await countListeners(SIDECAR_PORT),
        usedByThisRun: false,
        affectsResult: false,
        note:
            'Der Produktport. Dieser Lauf startet dort nichts und beendet dort nichts. Die Zahl '
            + 'zaehlt jeden Prozess mit einem Socket auf dem Port, Server wie Client (lsof -ti), '
            + 'und beschreibt den Rechner, nicht diesen Lauf.',
    };
    if (sidecarPort.sockets > 0) {
        log(`Hinweis: Port ${SIDECAR_PORT} ist fremd belegt (${sidecarPort.sockets} Prozesse). `
            + 'Das ist kein Prozessrest dieses Laufs und faerbt ihn nicht.');
    }

    /*
     * Der Vergleich. Er liest die Aufzeichnung noch einmal, damit auch ein
     * abgebrochener Lauf einen lesbaren Bericht hinterlaesst.
     */
    const recordedOf = existsSync(BASELINE)
        ? await readFile(BASELINE, 'utf8').then((text) => {
            const parsed = JSON.parse(text);
            return { A: parsed.winnerClassA, B: parsed.winnerClassB };
        })
        : { A: undefined, B: undefined };

    const comparisons = measured.map((entry) => {
        const recorded = recordedOf[entry.modelClass] ?? {};
        const passRateDelta = round(entry.passRate - Number(recorded.passRate ?? Number.NaN));
        const reasons = [];
        if (!Number.isFinite(passRateDelta)) {
            reasons.push(`${entry.name}: eval.json nennt keine aufgezeichnete passRate`);
        } else if (passRateDelta < -PASS_RATE_TOLERANCE) {
            reasons.push(`${entry.name}: passRate ${entry.passRate} liegt ${Math.abs(passRateDelta)} `
                + `unter der aufgezeichneten ${recorded.passRate} (Toleranz ${PASS_RATE_TOLERANCE})`);
        }
        /*
         * Und dazu die harten Grenzen selbst.
         *
         * Die Toleranz oben vergleicht mit der Aufzeichnung, diese Zeile mit dem
         * Vertrag, und das sind zwei verschiedene Fragen: ein Sieger, dessen
         * Trefferquote von 0.62 auf 0.59 faellt, bleibt in der Toleranz und ist
         * trotzdem kein Modell, das dieses Produkt ausliefern darf. Die
         * Zitattreue wird ueberhaupt nur so geprueft: sie ist Format und nicht
         * Wortlaut, also bekommt sie keine Toleranz.
         */
        reasons.push(...violationsOf(entry.name, {
            passRate: entry.passRate,
            citationCompliance: entry.citationCompliance,
        }));
        return {
            modelClass: entry.modelClass,
            name: entry.name,
            recordedPassRate: recorded.passRate ?? null,
            measuredPassRate: entry.passRate,
            passRateDelta,
            improved: Number.isFinite(passRateDelta) && passRateDelta > 0,
            recordedCitation: recorded.citationCompliance ?? null,
            measuredCitation: entry.citationCompliance,
            citationBound: EVAL_BOUNDS.citationCompliance,
            withinTolerance: reasons.length === 0,
            reasons,
        };
    });

    const missingClasses = ['A', 'B'].filter(
        (modelClass) => !measured.some((entry) => entry.modelClass === modelClass),
    );
    const reasons = [
        ...comparisons.flatMap((entry) => entry.reasons),
        ...missingClasses.map((modelClass) => `Klasse ${modelClass} wurde nicht gefahren`),
    ];
    if (failure !== null) {
        reasons.push(`der Lauf brach ab: ${failure.message}`);
    }
    if (leftoverProcesses !== 0) {
        reasons.push(`${leftoverProcesses} Prozesse lauschen nach dem Lauf noch auf den Ports, `
            + `die dieser Lauf selbst belegt hat (${ownPorts.join(', ')})`);
    }
    const evalCheckPass = reasons.length === 0;

    const report = {
        /*
         * Ein Ergebnis, ein Name.
         *
         * Bis W9 stand derselbe Wert hier zweimal, einmal als `evalCheckPass`
         * und einmal als `pass`. Der zweite Name war kein Feld dieses
         * Werkzeugs, sondern eine Vermutung: der in W7c eingefrorene
         * Abnahmetest las `pass`, weil beim Schreiben des Tests geraten wurde,
         * wie das Feld heisst. Innerhalb von W7c war das nicht zu heilen (ein
         * eingefrorener Test wird nicht noch einmal eingefroren), also trug
         * das Werkzeug den Wert unter beiden Namen. W9 zieht den Abnahmetest
         * auf den Namen, den das Werkzeug wirklich schreibt, und der Alias
         * faellt weg.
         */
        evalCheckPass,
        reasons,
        temperature: 0,
        seed: 42,
        questionCount: extras.baseline?.questionCount ?? null,
        modelsRun: measured.length,
        passRateTolerance: PASS_RATE_TOLERANCE,
        bounds: EVAL_BOUNDS,
        baselineFile: 'verification/w5/eval.json',
        comparisons,
        measured,
        leftoverProcesses,
        leftoverPortsChecked: ownPorts,
        sidecarPort,
        method:
            'Nur die zwei in verification/w5/eval.json aufgezeichneten Klassensieger, alle Fragen aus '
            + 'eval/questions.json, dieselben Karten aus dem Produkt-Compiler, Temperatur 0 und Seed 42 '
            + '(beides in src/chat/chat-client.ts). Verglichen wird die Trefferquote gegen die '
            + `Aufzeichnung mit Toleranz ${PASS_RATE_TOLERANCE} und die Zitattreue hart gegen die Grenze `
            + `${EVAL_BOUNDS.citationCompliance}. Eine GESTIEGENE Trefferquote ist kein Fehler.`,
        generatedAt: new Date().toISOString(),
        totalMs: Date.now() - startedAt,
        error: failure ? failure.message : null,
        extras,
    };

    await mkdir(dirname(out), { recursive: true });
    await writeFile(out, JSON.stringify(report, null, 2) + '\n', 'utf8');
    log('geschrieben:', out);

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }

    for (const entry of comparisons.filter((candidate) => candidate.improved)) {
        log(`besser als aufgezeichnet: ${entry.name} +${entry.passRateDelta} passRate. `
            + 'Das ist kein Fehler und faerbt diesen Lauf nicht rot.');
    }

    if (!evalCheckPass) {
        console.error('[eval-check] Die Eval-Regression ist NICHT durch:');
        for (const reason of reasons) {
            console.error('  -', reason);
        }
        process.exitCode = 1;
        return;
    }
    log(`Eval-Regression durch: ${measured.length} Sieger, `
        + `${Math.round(report.totalMs / 1000)} s, keine Prozessreste auf den eigenen Ports `
        + `(${ownPorts.join(', ')}).`);
}

main().catch((err) => {
    console.error('[eval-check] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
