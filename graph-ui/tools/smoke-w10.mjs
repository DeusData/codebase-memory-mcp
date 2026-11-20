#!/usr/bin/env node
/*
 * W10-Smoke: das Modell gehoert dem Leser, und alles, was Rechenzeit kostet,
 * steht an einem Ort und nennt seinen GEMESSENEN Effekt.
 *
 * Martin am 2026-08-29: "Fuer mich waere wichtig, dass das Modell das Binary
 * nicht gross verseucht. Man bietet in der UI das Feature so an, dass man sich
 * von zum Beispiel Hugging Face selber ein Modell aussuchen kann." Und der
 * Nutzer am selben Tag: "2D/3D oder sowas sollte immer zentral in einem
 * Settings-Menue drin sein, nicht alles auf einer Oberflaeche, wegen
 * Rechenleistung falls jemand keine so starke Maschine hat."
 *
 * Dieser Lauf prueft beide Zusicherungen an der laufenden Oberflaeche, gegen
 * einen echten llama-server, und er tut es, ohne den Arbeitsplatz des Nutzers
 * anzufassen.
 *
 * ## Die drei Kunstgriffe dieses Laufs
 *
 * 1. **Ein eigener Router-Sidecar mit ZWEI Modellen, ohne ein Byte zu laden.**
 *    Umschalten laesst sich nur messen, wenn es zwei Modelle gibt. Zwei Dateien
 *    zu kopieren waere 1,4 GB und eine Minute; zwei nachzuladen waere ein
 *    Netzzugriff, den dieser Lauf nicht haben darf. Also stehen sie als HARTE
 *    LINKS in einem eigenen Cache-Verzeichnis: derselbe Inhalt, dieselbe
 *    Inode, kein zusaetzlicher Platz, sofort da. Ein harter Link loescht beim
 *    Aufraeumen nichts: die Dateien in models/ tragen danach nur wieder einen
 *    Link weniger. Dass alle sechs am Ende noch da sind, wird gemessen und
 *    steht im Artefakt (`modelsAfterRun`).
 *
 * 2. **Die Umleitung an der Naht, an der die Anwendung selbst fragt.** Das
 *    Produkt redet mit 127.0.0.1:4141, und dieser Port gehoert dem Nutzer:
 *    dort laeuft sein eigener Sidecar, waehrend dieser Lauf laeuft. Wie in
 *    smoke-w7c ersetzt ein `addInitScript` das `globalThis.fetch` der Seite und
 *    schickt alles, was mit dem Produktursprung beginnt, an den Port dieses
 *    Laufs. Der Zustandsautomat, die Probe, das Panel und der Chat laufen
 *    unveraendert. Dass an 4141 wirklich kein Byte ging, steht als
 *    `productPortRequests` (muss 0 sein) im Artefakt; der Route-Handler bricht
 *    eine Anfrage dorthin ausdruecklich ab, statt sie durchzulassen.
 *
 * 3. **Zwei Sidecars nacheinander, nicht nebeneinander.** AC2 hat zwei
 *    Haelften: mit Router waehlt ein Klick das Modell, ohne Router sagt das
 *    Panel genau das und bietet keine wirkungslose Auswahl an. Der zweite
 *    Sidecar startet erst, wenn der erste samt seiner Kindprozesse beendet ist.
 *    Der Router startet je Modell eine eigene Instanz auf einem ephemeren Port;
 *    diese Ports werden mitgeschrieben und beim Abbau mitgezaehlt.
 *
 * ## Was hier eine Messung ist und was nicht
 *
 * Jede Zahl in verification/w10/models.json hat in `extras` das, woraus sie
 * entstanden ist: die selbst abgefragten Props neben den Werten im Panel, die
 * zwei Modellnamen aus den aufgezeichneten Antwortkoerpern, je Einstellung die
 * Bildrate vorher und nachher samt Urteil, die vier Fundstellen des
 * Ehrlichkeitssatzes, die drei Teile von `noWeightsInRepo`, die Zaehlung der
 * Fortschrittselemente und die Aufraeumzahlen. Wo etwas nicht ehrlich als
 * `true` zu messen war, steht `false` und der Grund; ein geschoentes `true`
 * waere kein Ergebnis.
 *
 * ## Die Grenze, die dieser Lauf nicht ueberschreitet
 *
 * Gemessen wird in headless Chromium mit SwiftShader, also in Software-GL. Die
 * ABSOLUTEN Bildraten sind darum nicht die eines Lesers mit Metal auf demselben
 * Rechner. Was dieser Lauf beweist, ist die Mechanik (jede Einstellung greift,
 * jede nennt ihre zwei Zahlen, das Urteil kennt auch "kein messbarer
 * Unterschied") und die Zahlen DIESER Umgebung. Der Satz steht noch einmal in
 * `method` und in `extras.frameRateEnvironment`, damit niemand eine dieser
 * Zahlen fuer die seines Rechners haelt.
 *
 * ## Ablauf
 *
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4580, dist/ auf einem zweiten
 *   5. eigenes Cache-Verzeichnis mit zwei harten Links, Router auf einem dritten
 *   6. Chromium ohne Aussenwelt, mit Umleitung und Route-Sperre
 *      a. AC5: das Panel bei AUSGESCHALTETEM Modell, zwei Poll-Intervalle lang
 *      b. Modell an; das Panel auf zwei Wegen oeffnen
 *      c. AC1/AC3: die vier Zahlen gegen die eigene Abfrage, die Cache-Liste,
 *         die sechs Vorschlaege gegen verification/w5/eval.json, das freie Feld,
 *         der Kopieren-Knopf gegen die Zwischenablage, der Ehrlichkeitssatz in
 *         vier Teilen, die Zaehlung der Fortschrittselemente
 *      d. AC2: umschalten, fragen, zurueckschalten, fragen; zwei Modellnamen
 *         aus den aufgezeichneten Antworten; Reload; Statusleiste
 *      e. AC9: acht Einstellungen, je Bildrate vorher und nachher; die flache
 *         Ansicht an den Beschriftungen belegt; die Ansichtsschalter, die
 *         bleiben, wo sie sind; Reload
 *   7. AC4 am Startskript, ohne einen Prozess zu starten
 *   8. AC8 an der Funktion selbst und an der Rechnung der Eval
 *   9. AC2 zweite Haelfte: derselbe Weg gegen einen Sidecar OHNE Router
 *  10. abraeumen, Restprozesse zaehlen, JSON und drei Bilder schreiben
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w10).
 *
 * ## Ports
 *
 * Ab 4580. 4141 gehoert dem Modell-Sidecar des Nutzers, 4390 und 4391 seiner
 * Vorschau; alle drei fasst dieser Lauf nicht an, weder startend noch beendend.
 * 4210 bis 4560 sind an die Laeufe davor vergeben.
 */

import { spawn } from 'node:child_process';
import { existsSync, statSync } from 'node:fs';
import { copyFile, link, mkdir, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
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
import { LLAMA_SERVER } from './lib/llama.mjs';
import {
    READABILITY_EXCLUSIONS,
    closeTooltips,
    measureReadability,
    resetScroll,
    scrollRegionsToEnd,
} from './lib/readability.mjs';
import { startStaticProxy } from './lib/static-proxy.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const BINARY = join(ROOT, 'cbm', 'build', 'c', 'codebase-memory-mcp');
const DIST = join(ROOT, 'dist');
const FIXTURE = join(ROOT, 'fixtures', 'atlas-sample');
const PROJECT = 'codeatlasweb-w10';
const OUT_DIR = join(ROOT, 'verification', 'w10');
const OUT_JSON = join(OUT_DIR, 'models.json');
const EVAL_JSON = join(ROOT, 'verification', 'w5', 'eval.json');
const SHOT_SETTINGS = join(OUT_DIR, 'settings.png');
const SHOT_SWITCH = join(OUT_DIR, 'settings-switch.png');
const SHOT_FETCH = join(OUT_DIR, 'settings-fetch.png');
/*
 * Das vierte Bild, und es steht bewusst nicht in der Liste des eingefrorenen
 * Tests: der prueft drei Namen, und ein vierter dort waere eine Aenderung an
 * einer eingefrorenen Datei. Es gibt trotzdem, weil die Gruppe "Darstellung und
 * Leistung" die einzige ist, die auf keinem der drei anderen zu sehen war, und
 * weil sie der Teil ist, um den AC9 geht: eine Einstellung mit ihren beiden
 * gemessenen Zahlen daneben.
 */
const SHOT_PERFORMANCE = join(OUT_DIR, 'settings-performance.png');

/** 4141, 4390 und 4391 gehoeren dem Nutzer, alles bis 4560 den Laeufen davor. */
const MIN_PORT = 4580;

/** Der Port des Produkts. Dieser Lauf belegt ihn nicht und redet nicht mit ihm. */
const PRODUCT_SIDECAR_PORT = 4141;
const PRODUCT_SIDECAR_ORIGIN = `http://127.0.0.1:${PRODUCT_SIDECAR_PORT}`;

/**
 * Die zwei Modelle des Cache-Verzeichnisses.
 *
 * Die zwei kleinsten vorhandenen, und zwar aus einem Grund, der nichts mit
 * ihrer Qualitaet zu tun hat: der Router haelt beide gleichzeitig geladen
 * (--models-max 2), und was hier zaehlt, ist der Speicher dieser Maschine
 * waehrend eines Beweislaufs. Welches Modell gut antwortet, sagt die Eval; was
 * hier gemessen wird, ist das Umschalten.
 */
const MODEL_A = 'MiniCPM5-1B-Q4_K_M.gguf';
const MODEL_B = 'LFM2.5-1.2B-Instruct-Q4_K_M.gguf';

/** Alle sechs Dateien, die in models/ liegen. Am Ende wird nachgezaehlt. */
const EXPECTED_MODEL_FILES = [
    'gemma-4-E4B-it-Q4_K_M.gguf',
    'LFM2.5-1.2B-Instruct-Q4_K_M.gguf',
    'MiniCPM5-1B-Q4_K_M.gguf',
    'qwen2.5-coder-1.5b-instruct-q4_k_m.gguf',
    'Qwen3.5-2B-Q4_K_M.gguf',
    'Qwen3.5-4B-Q4_K_M.gguf',
];

/** Das Kontextfenster, mit dem der Router seine Instanzen faehrt. */
const ROUTER_CTX = 2048;

/**
 * Wie lange bei ausgeschaltetem Modell gewartet wird.
 *
 * Zwei volle Poll-Intervalle plus Luft (SIDECAR_POLL_MS ist 3000). Dieselbe
 * Wartezeit wie in smoke-w5a, und aus demselben Grund: eine Null nach einer
 * halben Sekunde waere die Aussage, dass in einer halben Sekunde nichts flog.
 */
const OFF_WINDOW_MS = 7500;

/**
 * Wie lange die Szene vor einer Messung ungestoert laeuft.
 *
 * Das Vorher-Fenster des Panels reicht 3000 ms zurueck (BEFORE_MS in
 * SettingsPanel.tsx). Wer frueher klickt, gibt der Messung kein Vorher und kein
 * Rauschband, und das Urteil waere `not-measured`.
 */
const SETTLE_BEFORE_MS = 3400;

/**
 * Wie lange nach einer Aenderung gewartet wird, bevor das Urteil gelesen wird.
 *
 * SETTLE_MS (1200) plus MEASURE_MS (1500) plus Luft fuer den Timer selbst.
 */
const MEASURE_WAIT_MS = 3600;

const READY_TIMEOUT_MS = 240000;
const ANSWER_TIMEOUT_MS = 240000;

/** Die Frage, mit der das Umschalten belegt wird. Zweimal dieselbe, zwei Modelle. */
const SWITCH_QUESTION = '@createUser explain this function';

/** Das Symbol, das dafuer in den Fokus geht. Dasselbe wie in smoke-w7c. */
const FOCUS_FILE = 'src/services/userService.ts';
const FOCUS_LINE = 24;
const FOCUS_SYMBOL = 'createUser';

/** Die Eingabe fuer das freie Feld. Ein Repository, das dieser Lauf nie besucht. */
const FREE_REPO_INPUT = 'unsloth/Qwen3.5-9B-GGUF:Q4_K_M';

/**
 * Chromium ohne Aussenwelt, plus die GL-Flags aus smoke-w4e.
 *
 * Ohne `--use-angle=swiftshader` und die zwei Flags daneben zeichnet der Graph
 * in dieser Umgebung nicht, und ohne ein gezeichnetes Bild ist AC9 nicht
 * messbar: eine Bildrate von null ist keine Bildrate.
 */
const CHROMIUM_ARGS = [
    '--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1, EXCLUDE localhost',
    '--disable-background-networking',
    '--disable-component-update',
    '--disable-domain-reliability',
    '--disable-client-side-phishing-detection',
    '--disable-sync',
    '--disable-default-apps',
    '--no-first-run',
    '--no-default-browser-check',
    '--metrics-recording-only',
    '--no-pings',
    '--disable-breakpad',
    '--disable-features=Translate,OptimizationHints,MediaRouter,AutofillServerCommunication,InterestFeedContentSuggestions,DialMediaRouteProvider,CalculateNativeWinOcclusion',
    '--use-angle=swiftshader',
    '--enable-unsafe-swiftshader',
    '--ignore-gpu-blocklist',
];

const MAIN_VIEWPORT = { width: 1680, height: 1050 };

/** Der Satz ueber die Umgebung, woertlich, an beiden Stellen derselbe. */
const FRAME_RATE_ENVIRONMENT =
    'Gemessen in headless Chromium mit SwiftShader, also in Software-GL. Die absoluten '
    + 'Bildraten sind darum NICHT die eines Lesers mit Metal auf derselben Maschine. Was dieser '
    + 'Lauf zeigt, ist die Mechanik jeder Einstellung und die Zahlen dieser Umgebung.';

/**
 * Die Woerter, die im Leistungsteil ohne eine Zahl daneben nicht stehen duerfen.
 *
 * AC9 verbietet Versprechen ("schneller"), nicht Beobachtungen ("vorher 12,
 * nachher 19"). Gepruefte Einheit ist darum der SATZ: steht in demselben Satz
 * eine Zahl, ist das Wort an eine Messung gebunden.
 */
const PROMISE_WORDS = [
    'faster', 'quicker', 'speeds up', 'speed up', 'better', 'boost', 'boosts',
    'schneller', 'besser', 'spart', 'saves',
];

/**
 * Woerter, die daneben beobachtet und einzeln aufgeschrieben werden.
 *
 * Sie tragen die Zusicherung nicht, und der Grund steht im Artefakt neben dem
 * Satz, in dem sie stehen: eine Liste, die "cheaper" als Versprechen zaehlt,
 * wuerde auch den Satz treffen, der sagt, dass etwas nur BILLIGER AUSSIEHT.
 * Weggelassen wird das Wort trotzdem nicht: wer die Zahl liest, soll sehen, was
 * die Suche gefunden und was sie damit gemacht hat.
 */
const WATCHED_WORDS = ['cheaper', 'smoother', 'improve', 'optimise', 'optimize'];

const log = (...parts) => console.log('[smoke-w10]', ...parts);
const serverLog = [];

function run(command, args, options = {}) {
    return new Promise((done) => {
        const child = spawn(command, args, {
            cwd: ROOT,
            env: {
                ...process.env,
                NO_UPDATE_NOTIFIER: '1',
                npm_config_update_notifier: 'false',
                npm_config_audit: 'false',
                npm_config_fund: 'false',
                ...(options.env ?? {}),
            },
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        let out = '';
        let err = '';
        child.stdout.on('data', (d) => { out += d.toString(); });
        child.stderr.on('data', (d) => { err += d.toString(); });
        child.on('error', (error) => done({ code: 127, out, err: err + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out, err }));
    });
}

// --------------------------------------------------------- Der Sidecar ------

/**
 * Einen llama-server starten und warten, bis `/health` mit 200 antwortet.
 *
 * Eigener `spawn` statt tools/lib/llama.mjs, und das ist keine Doppelung aus
 * Bequemlichkeit: der Helfer dort kennt nur den Einzel-Modus (`-m <datei>`).
 * Ein Helfer, der um einen Router-Modus erweitert wird, den nur dieser eine
 * Lauf braucht, waere eine Aenderung an gemeinsamer Infrastruktur fuer einen
 * einzigen Aufrufer. Die Argumente stehen darum hier, wo man sie neben der
 * Zusicherung liest, die sie tragen.
 */
async function startSidecar(args, { port, log: sink = [], timeoutMs = 180000 }) {
    const started = Date.now();
    const child = spawn(LLAMA_SERVER, args, { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout.on('data', (d) => sink.push(`[stdout] ${d.toString().trimEnd()}`));
    child.stderr.on('data', (d) => sink.push(`[stderr] ${d.toString().trimEnd()}`));
    let exited = null;
    child.on('exit', (code, signal) => { exited = { code, signal }; });

    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (exited !== null) {
            throw new Error(
                `llama-server endete vorzeitig (code=${exited.code}, signal=${exited.signal})\n`
                + sink.slice(-20).join('\n'),
            );
        }
        try {
            const response = await fetch(`http://127.0.0.1:${port}/health`);
            await response.arrayBuffer();
            if (response.status === 200) {
                return { child, readyMs: Date.now() - started };
            }
        } catch {
            // lauscht noch nicht
        }
        await sleep(300);
    }
    child.kill('SIGKILL');
    throw new Error(`llama-server war binnen ${timeoutMs} ms nicht auf ${port} bereit`);
}

/** Die Kindprozesse des Routers und die Ports, auf denen sie lauschen. */
async function sidecarChildren(pid) {
    const found = await run('pgrep', ['-P', String(pid)]);
    const pids = found.out.split('\n').map((line) => line.trim()).filter(Boolean).map(Number);
    const out = [];
    for (const child of pids) {
        const ports = await run('lsof', ['-nP', '-a', '-p', String(child), '-i', '-sTCP:LISTEN']);
        const listed = [...ports.out.matchAll(/127\.0\.0\.1:(\d+)\s+\(LISTEN\)/g)]
            .map((match) => Number(match[1]));
        out.push({ pid: child, ports: listed });
    }
    return out;
}

/**
 * Einen Sidecar beenden, samt seiner Kindprozesse.
 *
 * Erst hoeflich, dann nicht mehr, und die Kinder ausdruecklich mit: der Router
 * startet je Modell eine eigene Instanz, und ein Beweislauf, der den Vater
 * beendet und die Kinder stehen laesst, haelt zwei Modelle im Speicher der
 * Maschine, auf der als naechstes jemand arbeiten will.
 */
async function stopSidecar(child, children) {
    const pids = [...children.map((entry) => entry.pid)];
    if (child !== null && child.exitCode === null && !child.killed) {
        child.kill('SIGTERM');
        for (let i = 0; i < 60; i += 1) {
            if (child.exitCode !== null || child.signalCode !== null) {
                break;
            }
            await sleep(100);
        }
        if (child.exitCode === null && child.signalCode === null) {
            child.kill('SIGKILL');
            await sleep(400);
        }
    }
    for (const pid of pids) {
        try {
            process.kill(pid, 0);
        } catch {
            continue;
        }
        try {
            process.kill(pid, 'SIGTERM');
        } catch {
            // schon weg
        }
    }
    await sleep(600);
    const survivors = [];
    for (const pid of pids) {
        try {
            process.kill(pid, 0);
            process.kill(pid, 'SIGKILL');
            survivors.push(pid);
        } catch {
            // weg, wie es sein soll
        }
    }
    if (survivors.length > 0) {
        await sleep(400);
    }
    return survivors;
}

/** Was der Router ueber ein Modell sagt. Die eigene Abfrage neben dem Panel. */
async function routerProps(origin, modelId) {
    const url = modelId === undefined
        ? `${origin}/props`
        : `${origin}/props?model=${encodeURIComponent(modelId)}`;
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`GET ${url} antwortete mit ${response.status}`);
    }
    return response.json();
}

async function routerModels(origin) {
    const response = await fetch(`${origin}/v1/models`);
    if (!response.ok) {
        throw new Error(`GET ${origin}/v1/models antwortete mit ${response.status}`);
    }
    return response.json();
}

// ------------------------------------------------------------ Testgriffe ----

const settingsSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasSettings;
        return seam === undefined ? null : JSON.parse(JSON.stringify(seam));
    });

const llmSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasLlm;
        return seam === undefined ? null : { ...seam };
    });

const perfSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasGalaxyPerf;
        return seam === undefined ? null : JSON.parse(JSON.stringify(seam));
    });

const chatSeam = (page) =>
    page.evaluate(() => {
        const seam = globalThis.__atlasChat;
        if (seam === undefined) {
            return null;
        }
        const { validateRefine, ...rest } = seam;
        return JSON.parse(JSON.stringify(rest));
    });

/**
 * Was im Einstellungen-Panel wirklich dasteht.
 *
 * Eine Abfrage und nicht zwoelf, weil die Antworten zusammengehoeren: ob eine
 * Modell-Liste dasteht, ist nur zusammen mit der Frage zu lesen, ob das lokale
 * Modell an ist. Gelesen wird der sichtbare Text, nicht der Katalog.
 */
const settingsDom = (page) =>
    page.evaluate(() => {
        const tidy = (value) => (value ?? '').replace(/\s+/g, ' ').trim();
        const root = document.querySelector('[data-testid="atlas-settings"]');
        if (root === null) {
            return { present: false };
        }
        const rect = root.getBoundingClientRect();
        const style = globalThis.getComputedStyle(root);
        const one = (id) => {
            const node = root.querySelector(`[data-testid="${id}"]`);
            if (node === null) {
                return { present: false, text: '' };
            }
            const box = node.getBoundingClientRect();
            return {
                present: true,
                text: tidy(node.textContent),
                visible: box.width > 0 && box.height > 0,
            };
        };
        const attrs = (node, names) => Object.fromEntries(
            names.map((name) => [name, node.getAttribute(`data-${name}`) ?? '']),
        );
        return {
            present: true,
            visible: rect.width > 0 && rect.height > 0
                && style.display !== 'none' && style.visibility !== 'hidden',
            llmState: root.getAttribute('data-llm') ?? '',
            router: root.getAttribute('data-router') ?? '',
            title: one('atlas-settings-title'),
            sections: [...root.querySelectorAll('[data-testid="atlas-settings-section"]')]
                .map((node) => ({
                    name: node.getAttribute('data-section') ?? '',
                    text: tidy(node.textContent).slice(0, 400),
                    height: Math.round(node.getBoundingClientRect().height),
                })),
            llmOff: one('atlas-settings-llm-off'),
            notRunning: one('atlas-settings-not-running'),
            starting: one('atlas-settings-starting'),
            running: one('atlas-settings-running'),
            facts: [...root.querySelectorAll('[data-testid="atlas-settings-fact"]')]
                .map((node) => {
                    const value = node.querySelector('[data-testid="atlas-settings-fact-value"]');
                    return {
                        fact: node.getAttribute('data-fact') ?? '',
                        label: tidy(node.querySelector('.atlas-settings-fact-label')?.textContent),
                        value: tidy(value?.textContent),
                        source: value?.getAttribute('data-source') ?? '',
                    };
                }),
            cacheCount: (() => {
                const node = root.querySelector('[data-testid="atlas-settings-cache-count"]');
                return node === null
                    ? { present: false, count: -1, text: '' }
                    : {
                        present: true,
                        count: Number(node.getAttribute('data-count') ?? '-1'),
                        text: tidy(node.textContent),
                    };
            })(),
            models: [...root.querySelectorAll('[data-testid="atlas-settings-model"]')]
                .map((node) => ({
                    ...attrs(node, ['model', 'active', 'loaded', 'selectable']),
                    text: tidy(node.textContent),
                    pickable: node.querySelector('[data-testid="atlas-settings-model-pick"]') !== null
                        && node.querySelector('button[data-testid="atlas-settings-model-pick"]') !== null,
                })),
            modelPicks: root.querySelectorAll('button[data-testid="atlas-settings-model-pick"]').length,
            refresh: one('atlas-settings-refresh'),
            noRouter: one('atlas-settings-no-router'),
            routerCommand: one('atlas-settings-router-command'),
            startCommand: one('atlas-settings-start-command'),
            honesty: one('atlas-settings-honesty'),
            noProgressNote: one('atlas-settings-no-progress'),
            suggestions: [...root.querySelectorAll('[data-testid="atlas-settings-suggestion"]')]
                .map((node) => ({
                    ...attrs(node, [
                        'suggestion', 'repo', 'class', 'pass-rate', 'citation',
                        'citation-unmeasured', 'tokens-per-second', 'bytes',
                    ]),
                    name: tidy(node.querySelector('.atlas-settings-suggestion-name')?.textContent),
                    passText: tidy(node.querySelector('[data-testid="atlas-settings-pass-rate"]')?.textContent),
                    citationText: tidy(node.querySelector('[data-testid="atlas-settings-citation"]')?.textContent),
                    unmeasuredText: tidy(node.querySelector('[data-testid="atlas-settings-unmeasured"]')?.textContent),
                    unmeasuredBesideCitation:
                        node.querySelector('[data-testid="atlas-settings-citation"] [data-testid="atlas-settings-unmeasured"]') !== null,
                    command: node.querySelector('[data-testid="atlas-settings-command"]')
                        ?.getAttribute('data-command') ?? '',
                    copyButtons: node.querySelectorAll('[data-testid="atlas-settings-copy"]').length,
                })),
            repoState: (() => {
                const node = root.querySelector('[data-testid="atlas-settings-repo-state"]');
                return node === null
                    ? { present: false }
                    : {
                        present: true,
                        valid: node.getAttribute('data-valid') ?? '',
                        problem: node.getAttribute('data-problem') ?? '',
                        text: tidy(node.textContent),
                    };
            })(),
            repoCommand: (() => {
                const node = root.querySelector('[data-testid="atlas-settings-repo-command"]');
                return node === null
                    ? { present: false, command: '', text: '' }
                    : {
                        present: true,
                        command: node.getAttribute('data-command') ?? '',
                        text: tidy(node.textContent),
                    };
            })(),
            perf: (() => {
                const node = root.querySelector('[data-testid="atlas-settings-perf"]');
                return node === null
                    ? { present: false }
                    : {
                        present: true,
                        ...attrs(node, ['running', 'fps', 'nodes', 'edges', 'cap', 'band', 'samples']),
                        text: tidy(node.textContent),
                    };
            })(),
            choices: [
                ...root.querySelectorAll('[data-testid="atlas-settings-choice"]'),
                ...root.querySelectorAll('[data-testid="atlas-settings-effect"]'),
            ].map((node) => ({
                setting: node.getAttribute('data-setting') ?? '',
                effect: node.getAttribute('data-effect') ?? '',
                value: node.getAttribute('data-value') ?? '',
                label: tidy(node.querySelector('.atlas-settings-choice-label')?.textContent),
                options: [...node.querySelectorAll('[data-testid="atlas-settings-option"]')]
                    .map((option) => ({
                        option: option.getAttribute('data-option') ?? '',
                        active: option.getAttribute('data-active') ?? '',
                        label: tidy(option.textContent),
                    })),
            })),
            measures: [...root.querySelectorAll('[data-testid="atlas-settings-measure"]')]
                .map((node) => ({
                    ...attrs(node, ['setting', 'verdict', 'before', 'after', 'band', 'nodes', 'edges', 'at']),
                    text: tidy(node.textContent),
                })),
            profiles: {
                present: root.querySelector('[data-testid="atlas-settings-profiles"]') !== null,
                isDefault: root.querySelector('[data-testid="atlas-settings-profiles"]')
                    ?.getAttribute('data-default') ?? '',
                buttons: [...root.querySelectorAll('[data-testid="atlas-settings-profile"]')]
                    .map((node) => node.getAttribute('data-profile') ?? ''),
            },
            storage: one('atlas-settings-storage'),
            modelStorage: one('atlas-settings-model-storage'),
            keeps: one('atlas-settings-keeps'),
            displayText: tidy(
                root.querySelector('[data-testid="atlas-settings-section"][data-section="display"]')
                    ?.textContent,
            ),
            /*
             * Die Zaehlung der Fortschrittselemente. AC3 verbietet einen Balken
             * ueber eine Uebertragung, die diese Oberflaeche nicht sieht.
             * Ausgenommen wird genau ein Element, und der Grund steht in seinem
             * Text: der Absatz, der SAGT, dass es keinen Balken gibt, traegt das
             * Wort "progress" in seiner Testmarke.
             */
            progressElements: [...root.querySelectorAll(
                'progress, [role="progressbar"], [aria-valuenow], [class*="progress" i], '
                + '[class*="fortschritt" i], [data-testid*="progress" i], [data-testid*="fortschritt" i]',
            )]
                .filter((node) => node.getAttribute('data-testid') !== 'atlas-settings-no-progress')
                .map((node) => ({
                    tag: node.tagName.toLowerCase(),
                    testid: node.getAttribute('data-testid') ?? '',
                    role: node.getAttribute('role') ?? '',
                    className: node.getAttribute('class') ?? '',
                    text: tidy(node.textContent).slice(0, 80),
                })),
            progressExemption: (() => {
                const node = root.querySelector('[data-testid="atlas-settings-no-progress"]');
                return node === null ? '' : tidy(node.textContent);
            })(),
        };
    });

/** Die Schalter, die keine Rechenzeit kosten und darum bleiben, wo sie sind. */
const viewOnlyControls = (page) =>
    page.evaluate(() => {
        const box = (node) => {
            if (node === null) {
                return null;
            }
            const rect = node.getBoundingClientRect();
            return { width: Math.round(rect.width), height: Math.round(rect.height) };
        };
        const legendToggle = document.querySelector('[data-testid="atlas-galaxy-legend-toggle"]');
        return {
            legendToggle: {
                present: legendToggle !== null,
                expanded: legendToggle?.getAttribute('aria-expanded') ?? '',
                rect: box(legendToggle),
            },
            legendOpen: document.querySelector('[data-testid="atlas-galaxy-legend"]') !== null,
            modeChips: [...document.querySelectorAll('[data-testid="atlas-graph-mode-chip"]')]
                .map((node) => ({
                    mode: node.getAttribute('data-mode') ?? '',
                    active: node.getAttribute('data-active') ?? '',
                    rect: box(node),
                })),
            edgeFilters: [...document.querySelectorAll('[data-testid="atlas-galaxy-legend-swatch"]')]
                .filter((node) => node.tagName === 'BUTTON')
                .map((node) => node.getAttribute('data-type') ?? ''),
            twinDepth: {
                present: document.querySelector('[data-testid="atlas-twin-depth"]') !== null,
                rect: box(document.querySelector('[data-testid="atlas-twin-depth"]')),
                value: document.querySelector('[data-testid="atlas-twin-depth"]')?.value ?? '',
                name: (document.querySelector('[data-testid="atlas-twin-depth-name"]')?.textContent ?? '')
                    .trim(),
            },
        };
    });

/** Wie das lokale Modell in der Statusleiste heisst. Wortgleich mit smoke-w7c. */
const statusChip = (page) =>
    page.evaluate(() => {
        const chip = [...document.querySelectorAll('.atlas-statusbar .atlas-chip')]
            .find((entry) => (entry.getAttribute('data-chip') ?? '').includes('llm'));
        return {
            present: chip !== undefined,
            label: chip?.getAttribute('data-chip') ?? '',
            text: (chip?.textContent ?? '').replace(/\s+/g, ' ').trim(),
        };
    });

/** Das Rechteck der Zeichenflaeche, in Fensterkoordinaten. Wortgleich mit smoke-w9. */
const sceneRect = (page) =>
    page.evaluate(() => {
        const node = document.querySelector('[data-testid="atlas-galaxy-scene"]');
        if (node === null) {
            return null;
        }
        const rect = node.getBoundingClientRect();
        return {
            x: Math.round(rect.x),
            y: Math.round(rect.y),
            width: Math.round(rect.width),
            height: Math.round(rect.height),
        };
    });

/**
 * Ein Bild der Zeichenflaeche aufnehmen und IN der Seite ablegen.
 *
 * Derselbe Umweg wie in smoke-w9, aus demselben Grund: der Canvas laeuft ohne
 * `preserveDrawingBuffer`, `toDataURL` gaebe also ein leeres Bild, und eine
 * Million Zahlen ueber die Bruecke ins Skript zu schieben kostet Minuten.
 * Gerechnet wird dort, zurueck kommen Kennzahlen.
 */
async function grab(page, key, clip) {
    const shot = await page.screenshot({ clip });
    return page.evaluate(async ({ name, data }) => {
        const image = new Image();
        image.src = `data:image/png;base64,${data}`;
        await image.decode();
        const canvas = document.createElement('canvas');
        canvas.width = image.naturalWidth;
        canvas.height = image.naturalHeight;
        const ctx = canvas.getContext('2d', { willReadFrequently: true });
        ctx.drawImage(image, 0, 0);
        globalThis.__w10 = globalThis.__w10 ?? {};
        globalThis.__w10[name] = ctx.getImageData(0, 0, canvas.width, canvas.height);
        return { width: canvas.width, height: canvas.height };
    }, { name: key, data: shot.toString('base64') });
}

/**
 * Zwei abgelegte Bilder vergleichen, einmal Pixel auf Pixel und einmal
 * gegeneinander VERSCHOBEN.
 *
 * Das ist das Messinstrument der flachen Ansicht, und der Grund steht in einem
 * Satz: eine orthografische Kamera hat keinen Fluchtpunkt, also verschiebt ein
 * Schwenk das ganze Bild um denselben Betrag. Eine perspektivische Kamera hat
 * einen, also wandern nahe Punkte weiter als ferne (Parallaxe), und kein
 * einziger Versatz bringt die beiden Bilder wieder zur Deckung.
 *
 * Der beste Versatz wird nicht gesucht, indem alle Verschiebungen ausprobiert
 * werden (das waeren bei diesem Bild hunderte Millionen Vergleiche), sondern
 * ueber die SPALTENSUMMEN: die Helligkeit je Bildspalte ist ein eindimensionales
 * Signal, und der Versatz, bei dem die beiden Signale am besten zusammenpassen,
 * ist der gesuchte. Nur dieser eine Versatz wird danach am ganzen Bild
 * nachgerechnet.
 */
const shiftedDifference = (page, options) =>
    page.evaluate((input) => {
        const store = globalThis.__w10 ?? {};
        const first = store[input.base];
        const second = store[input.variant];
        if (first === undefined || second === undefined) {
            return null;
        }
        const width = first.width;
        const height = first.height;
        if (second.width !== width || second.height !== height) {
            return null;
        }
        const grey = (image) => {
            const out = new Float64Array(width * height);
            for (let i = 0, p = 0; i < image.data.length; i += 4, p += 1) {
                out[p] = (image.data[i] + image.data[i + 1] + image.data[i + 2]) / 3;
            }
            return out;
        };
        const a = grey(first);
        const b = grey(second);
        const columns = (values) => {
            const sums = new Float64Array(width);
            for (let y = 0; y < height; y += 1) {
                const row = y * width;
                for (let x = 0; x < width; x += 1) {
                    sums[x] += values[row + x];
                }
            }
            return sums;
        };
        const columnsA = columns(a);
        const columnsB = columns(b);
        /* Der Versatz, bei dem die Spaltensummen am besten zusammenpassen. */
        let bestShift = 0;
        let bestScore = Number.POSITIVE_INFINITY;
        for (let shift = -input.maxShift; shift <= input.maxShift; shift += 1) {
            let sum = 0;
            let count = 0;
            for (let x = 0; x < width; x += 1) {
                const source = x + shift;
                if (source < 0 || source >= width) {
                    continue;
                }
                sum += Math.abs(columnsA[source] - columnsB[x]);
                count += 1;
            }
            if (count < width / 2) {
                continue;
            }
            const score = sum / count;
            if (score < bestScore) {
                bestScore = score;
                bestShift = shift;
            }
        }
        /* Und derselbe Vergleich am ganzen Bild: ohne Versatz und mit dem besten. */
        const residualAt = (shift) => {
            let sum = 0;
            let count = 0;
            for (let y = 0; y < height; y += 1) {
                const row = y * width;
                for (let x = 0; x < width; x += 1) {
                    const source = x + shift;
                    if (source < 0 || source >= width) {
                        continue;
                    }
                    sum += Math.abs(a[row + source] - b[row + x]);
                    count += 1;
                }
            }
            return count === 0 ? -1 : sum / count;
        };
        return {
            width,
            height,
            bestShift,
            residualAtZero: Number(residualAt(0).toFixed(3)),
            residualAtBestShift: Number(residualAt(bestShift).toFixed(3)),
        };
    }, options);

/**
 * Wo jeder scrollbare Bereich steht, und ob er am Anfang steht.
 *
 * Wortgleich mit smoke-w9: die Beweisbilder sollen den Ruhezustand zeigen, und
 * dass es einer ist, gehoert als Zahl ins Artefakt und nicht als Zusicherung in
 * den Kopf.
 */
const scrollState = (page) =>
    page.evaluate(() => {
        const regions = [];
        for (const node of document.body.querySelectorAll('*')) {
            if (node.closest('.monaco-editor') !== null) {
                continue;
            }
            const style = globalThis.getComputedStyle(node);
            const scrollsY = (style.overflowY === 'auto' || style.overflowY === 'scroll')
                && node.scrollHeight > node.clientHeight + 1;
            const scrollsX = (style.overflowX === 'auto' || style.overflowX === 'scroll')
                && node.scrollWidth > node.clientWidth + 1;
            if (!scrollsY && !scrollsX) {
                continue;
            }
            regions.push({
                name: node.getAttribute('data-testid')
                    ?? (node.getAttribute('class') ?? '').split(/\s+/).filter(Boolean)[0]
                    ?? node.tagName.toLowerCase(),
                top: Math.round(node.scrollTop),
                left: Math.round(node.scrollLeft),
                hidden: Math.round(node.scrollHeight - node.clientHeight),
            });
        }
        return { regions, atRest: regions.every((region) => region.top <= 1 && region.left <= 1) };
    });

// -------------------------------------------------------- Klickstrecke ------

async function openApp(page, origin) {
    await page.goto(`${origin}/?project=${PROJECT}`, { waitUntil: 'load' });
    await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 30000 });
    await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 30000 });
    await page.waitForFunction(
        () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
        undefined,
        { timeout: 60000 },
    );
    await page.waitForFunction(
        () => (globalThis.__atlasLlm?.policyVerdict ?? '') !== '',
        undefined,
        { timeout: 60000 },
    );
}

const settingsOpen = (page) =>
    page.evaluate(() => document.querySelector('[data-testid="atlas-settings"]') !== null);

async function openSettingsByMenu(page) {
    await page.click('[data-menu="a-settings"]');
    await page.waitForSelector('[data-testid="atlas-settings"]', { timeout: 15000 });
    await page.waitForTimeout(350);
}

async function openSettingsByCommand(page) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially('settings', { delay: 20 });
    await page.waitForTimeout(300);
    await input.press('Enter');
    await page.waitForSelector('[data-testid="atlas-settings"]', { timeout: 15000 });
    await page.waitForTimeout(350);
}

async function closeSettings(page) {
    if (!(await settingsOpen(page))) {
        return;
    }
    await page.click('[data-testid="atlas-settings-close"]');
    await page.waitForSelector('[data-testid="atlas-settings"]', { state: 'detached', timeout: 15000 });
    await page.waitForTimeout(250);
}

const waitForLlm = (page, state, timeout) =>
    page.waitForFunction(
        (expected) => globalThis.__atlasLlm?.state === expected,
        state,
        { timeout },
    );

/** Eine Frage in die Kommandozeile tippen und abschicken, wie ein Leser es tut. */
async function ask(page, question) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    await input.click();
    await input.fill('');
    await input.pressSequentially(question, { delay: 8 });
    await page.waitForTimeout(600);
    await input.press('Enter');
}

/**
 * Warten, bis der `count`-te Zug in einer Endlage steht.
 *
 * Die Zahl gehoert dazu, weil zweimal dieselbe Frage gestellt wird: ohne sie
 * waere die Bedingung schon durch den ERSTEN Zug erfuellt, und der zweite Lauf
 * wuerde eine Antwort messen, die vor dem Umschalten entstanden ist.
 */
const waitForTurnEnd = (page, count, timeout) =>
    page.waitForFunction(
        (wanted) => {
            const turns = globalThis.__atlasChat?.turns ?? [];
            if (turns.length < wanted) {
                return false;
            }
            const turn = turns[wanted - 1];
            return ['answered', 'no-cards', 'refused', 'failed', 'needs-choice'].includes(turn.status);
        },
        count,
        { timeout },
    );

/** Ein Symbol in den Fokus holen. Derselbe Weg wie in smoke-w7c. */
async function focusSymbol(page) {
    await page.click(`[data-testid="atlas-tree-row"][data-path="${FOCUS_FILE}"]`);
    await page.waitForFunction(
        (path) => (globalThis.__atlasReader?.document?.path ?? '') === path,
        FOCUS_FILE,
        { timeout: 30000 },
    );
    await page.evaluate((line) => {
        globalThis.__atlasReader?.editor?.setPosition?.({ lineNumber: line, column: 5 });
        globalThis.__atlasReader?.editor?.focus?.();
    }, FOCUS_LINE);
    await page.waitForFunction(
        (name) => (globalThis.__atlasTwin?.qualifiedName ?? '').endsWith(name),
        FOCUS_SYMBOL,
        { timeout: 30000 },
    );
}

/**
 * Ein Beweisbild aufnehmen.
 *
 * Erst jeden offenen Tooltip zumachen und jeden Bereich an den Anfang, dann
 * warten, bis die Oberflaeche das mitbekommen hat, dann die Lage aufschreiben,
 * dann das Bild. Ein Bild mit einem Kasten unter dem Mauszeiger zeigt einen
 * Zustand, den ein Leser nur waehrend einer Bewegung sieht; dass keiner offen
 * war, steht als `hintOpen` im Artefakt statt als Zusicherung im Kopf.
 *
 * Ist ein Abschnitt genannt, wird das Panel absichtlich bis zu ihm gescrollt:
 * es ist hoeher als sein Fenster, und die Gruppen darunter waeren sonst auf
 * keinem Bild. Mit `fit` wird zusaetzlich die BESTE Lage gesucht statt der
 * obersten, siehe {@link fitScroll}.
 */
async function shoot(page, file, name, section, options = {}) {
    await closeTooltips(page);
    await resetScroll(page, READABILITY_EXCLUSIONS);
    await page.waitForTimeout(350);
    let scrolledTo = '';
    let framing = null;
    if (section !== undefined && options.fit === undefined) {
        scrolledTo = await page.evaluate((wanted) => {
            const root = document.querySelector('[data-testid="atlas-settings"]');
            const target = root?.querySelector(
                `[data-testid="atlas-settings-section"][data-section="${wanted}"]`,
            );
            if (root === null || target === null || target === undefined) {
                return '';
            }
            root.scrollTop += target.getBoundingClientRect().top
                - root.getBoundingClientRect().top - 8;
            return wanted;
        }, section);
        await page.waitForTimeout(400);
    }
    if (section !== undefined && options.fit !== undefined) {
        framing = await fitScroll(page, section);
        scrolledTo = section;
        await page.waitForTimeout(500);
    }
    await closeTooltips(page);
    await page.waitForTimeout(150);
    const state = await scrollState(page);
    await page.screenshot({ path: file, fullPage: false });
    const size = (await stat(file)).size;
    /*
     * Was auf DIESEM Bild wirklich zu sehen ist, als Liste und nicht als
     * Bildunterschrift. Ein Panel, das laenger ist als das Fenster, zeigt in
     * einer Aufnahme nie alles; welche Teile es waren, gehoert darum ins
     * Artefakt und nicht in einen Satz, den niemand nachprueft.
     */
    const shows = await page.evaluate(() => {
        const wanted = [
            'atlas-settings-title', 'atlas-settings-running', 'atlas-settings-models',
            'atlas-settings-cache-count', 'atlas-settings-honesty', 'atlas-settings-no-progress',
            'atlas-settings-suggestions', 'atlas-settings-copy', 'atlas-settings-repo-input',
            'atlas-settings-repo-command', 'atlas-settings-perf', 'atlas-settings-llm-off',
            'atlas-settings-profiles', 'atlas-settings-keeps',
        ];
        const out = {};
        for (const id of wanted) {
            const node = document.querySelector(`[data-testid="${id}"]`);
            if (node === null) {
                out[id] = 'absent';
                continue;
            }
            const rect = node.getBoundingClientRect();
            const onScreen = rect.bottom > 0 && rect.top < window.innerHeight
                && rect.right > 0 && rect.left < window.innerWidth;
            out[id] = onScreen ? 'in view' : 'off screen';
        }
        /* Und die Messabsaetze einzeln, mit dem Urteil, das auf dem Bild steht. */
        out.measuresInView = [...document.querySelectorAll('[data-testid="atlas-settings-measure"]')]
            .filter((node) => {
                const rect = node.getBoundingClientRect();
                return rect.top >= 0 && rect.bottom <= window.innerHeight && rect.height > 0;
            })
            .map((node) => ({
                setting: node.getAttribute('data-setting') ?? '',
                verdict: node.getAttribute('data-verdict') ?? '',
                before: node.getAttribute('data-before') ?? '',
                after: node.getAttribute('data-after') ?? '',
            }));
        out.hintOpen = (() => {
            const box = document.querySelector('[data-testid="atlas-hint"]');
            if (box === null) {
                return false;
            }
            const rect = box.getBoundingClientRect();
            return globalThis.getComputedStyle(box).visibility !== 'hidden'
                && rect.width > 0 && rect.height > 0;
        })();
        return out;
    });
    log(`${name}: aufgenommen (atRest=${state.atRest}, ${Math.round(size / 1024)} KB`
        + `${scrolledTo === '' ? '' : `, gescrollt zu ${scrolledTo}`}`
        + `${framing === null ? '' : `, Lage ${framing.chosenTop} von ${framing.range}, Punkte ${framing.bestScore}`}`
        + `, Tooltip offen ${shows.hintOpen})`);
    return {
        name,
        atRest: state.atRest,
        scrolledTo,
        bytes: size,
        shows,
        framing,
        /*
         * Warum dieses Bild NICHT im Ruhezustand steht, wenn es das nicht tut.
         * Ein Satz, und er gehoert ins Artefakt und nicht in den Kopf dieser
         * Datei: wer die Bilder ansieht, soll die Abweichung dort finden, wo die
         * Zahl steht, die sie meldet.
         */
        why: state.atRest
            ? ''
            : options.why ?? `Das Panel ist hoeher als sein Fenster. Es wurde zur Gruppe `
                + `"${scrolledTo}" gescrollt, weil sie sonst auf keinem Beweisbild zu sehen waere.`,
        regions: state.regions,
    };
}

/**
 * Die beste Lage fuer ein Bild dieser Gruppe suchen, statt sie zu raten.
 *
 * Die Gruppe "Darstellung und Leistung" ist hoeher als das Fenster des Panels:
 * der Lebendzaehler steht oben, die acht Messabsaetze verteilen sich darunter,
 * das Sparprofil und der Weg zurueck stehen am Ende. Es gibt also keine Lage,
 * die alles zeigt, und welche die beste ist, haengt an Schriftgroessen und
 * Fensterhoehe dieser Maschine.
 *
 * Statt eine Zahl zu raten, faehrt diese Funktion die Gruppe in Schritten ab und
 * ZAEHLT an jeder Lage, was ganz im Bild steht: der Lebendzaehler und die zwei
 * Profilknoepfe zaehlen doppelt (AC9 nennt sie ausdruecklich), jeder Messabsatz
 * mit einem fertigen Urteil einfach. Genommen wird die beste Lage, und die
 * Punkte aller Lagen stehen im Artefakt: ein Leser sieht damit, dass die
 * Aufnahme die beste verfuegbare ist und nicht die erstbeste.
 */
const fitScroll = (page, section) =>
    page.evaluate((wanted) => {
        const root = document.querySelector('[data-testid="atlas-settings"]');
        const target = root?.querySelector(
            `[data-testid="atlas-settings-section"][data-section="${wanted}"]`,
        );
        if (root === null || target === null || target === undefined) {
            return null;
        }
        const fully = (node) => {
            const box = root.getBoundingClientRect();
            const rect = node.getBoundingClientRect();
            return rect.height > 0 && rect.top >= box.top && rect.bottom <= box.bottom;
        };
        const score = () => {
            const perf = root.querySelector('[data-testid="atlas-settings-perf"]');
            const profiles = [...root.querySelectorAll('[data-testid="atlas-settings-profile"]')];
            const measures = [...root.querySelectorAll('[data-testid="atlas-settings-measure"]')]
                .filter((node) => {
                    const verdict = node.getAttribute('data-verdict') ?? '';
                    return verdict !== 'not-measured' && verdict !== 'measuring';
                });
            const perfIn = perf !== null && fully(perf) ? 1 : 0;
            const profilesIn = profiles.filter(fully).length;
            const measuresIn = measures.filter(fully).length;
            return {
                perfIn,
                profilesIn,
                measuresIn,
                measuresTotal: measures.length,
                points: perfIn * 2 + profilesIn * 2 + measuresIn,
            };
        };
        const first = target.getBoundingClientRect().top - root.getBoundingClientRect().top;
        const top = Math.max(0, root.scrollTop + first - 8);
        const bottom = Math.min(
            root.scrollHeight - root.clientHeight,
            root.scrollTop + first + target.getBoundingClientRect().height - root.clientHeight + 8,
        );
        const tried = [];
        let best = { top, ...score() };
        const steps = 16;
        for (let i = 0; i <= steps; i += 1) {
            const value = Math.round(top + ((bottom - top) * i) / steps);
            root.scrollTop = value;
            const here = { top: root.scrollTop, ...score() };
            tried.push(here);
            if (here.points > best.points) {
                best = here;
            }
        }
        root.scrollTop = best.top;
        return {
            section: wanted,
            chosenTop: root.scrollTop,
            range: `${Math.round(top)} bis ${Math.round(bottom)}`,
            bestScore: best.points,
            best,
            tried,
            weights: 'Lebendzaehler 2, je Profilknopf 2, je fertiger Messabsatz 1',
        };
    }, section);

// ------------------------------------------------------------ AC9 -----------

/** Die Zahl aus einem `data-`Attribut, oder null, wenn dort nichts steht. */
function numberOf(value) {
    if (value === undefined || value === null || String(value).trim() === '') {
        return null;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

async function main() {
    const totalStarted = Date.now();
    let serverChild = null;
    let proxy = null;
    let browser = null;
    let home = null;
    let runtimeDir = null;
    let cacheDir = null;
    let emptyDir = null;
    let routerChild = null;
    let routerKids = [];
    let singleChild = null;
    let singleKids = [];
    let serverPort = 0;
    let uiPort = 0;
    let routerPort = 0;
    let singlePort = 0;
    let failure = null;
    const timings = {};
    const childPorts = new Set();

    const report = {
        settingsPanelOpens: false,
        runningModelFromProps: false,
        cacheModelsListed: 0,
        switchModelWorks: false,
        switchPersistsReload: false,
        statusBarNamesModel: false,
        noRouterExplained: false,
        suggestionsListed: 0,
        suggestionsShowMeasuredNumbers: false,
        freeRepoFieldAccepted: false,
        commandCopyable: false,
        downloadHonestyText: false,
        noFakeProgressBar: false,
        startScriptTakesModel: false,
        startScriptSaysHowToFetch: false,
        noWeightsInRepo: false,
        llmOffMakesNoRequests: false,
        panelExplainsItselfWhileOff: false,
        singleLineTruncatedIsUnmeasured: false,
        unmeasuredOutOfCitationRate: false,
        evalReportsUnmeasured: false,
        panelShowsUnmeasured: false,
        twoDimensionalMode: false,
        effectToggles: [],
        thriftProfileWorks: false,
        frameCapWorks: false,
        settingsPersistReload: false,
        everyToggleNamesMeasuredEffect: false,
        noEffectSaysSo: false,
        viewOnlyControlsStayed: false,
        overlapViolations: 0,
        clippingViolations: 0,
        cutWithoutHint: 0,
        port: 0,
        leftoverProcesses: 0,
    };

    const extras = {
        frameRateEnvironment: FRAME_RATE_ENVIRONMENT,
        blockedRequests: [],
        consoleErrors: [],
        pageErrors: [],
        sidecarRequests: [],
        productPortRequests: [],
        chatResponses: [],
        readability: [],
        shots: [],
        measurements: [],
    };

    const routeFor = (origin, sidecarOrigin) => async (route) => {
        const url = route.request().url();
        if (url.startsWith(PRODUCT_SIDECAR_ORIGIN)) {
            /*
             * Darf nicht vorkommen und wird darum abgebrochen statt
             * durchgelassen: auf 4141 laeuft der Sidecar des Nutzers, und ein
             * Beweislauf, der ihn "nur einmal kurz" fragt, hat ihn angefasst.
             */
            extras.productPortRequests.push(url);
            await route.abort();
            return;
        }
        if (url.startsWith(sidecarOrigin)) {
            extras.sidecarRequests.push({ url, atMs: Date.now() - totalStarted });
            await route.continue();
            return;
        }
        if (url.startsWith(origin) || url.startsWith('data:') || url.startsWith('blob:')) {
            await route.continue();
            return;
        }
        extras.blockedRequests.push(url);
        await route.abort();
    };

    try {
        if (!existsSync(BINARY)) {
            throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
        }
        if (!existsSync(FIXTURE)) {
            throw new Error(`Fixture fehlt: ${FIXTURE}`);
        }
        if (!existsSync(LLAMA_SERVER)) {
            throw new Error('vendor/llama/llama-server fehlt (siehe vendor/llama/HERKUNFT.md)');
        }
        if (!existsSync(EVAL_JSON)) {
            throw new Error(`${EVAL_JSON} fehlt: dieser Lauf haelt die Vorschlaege dagegen.`);
        }
        for (const file of [MODEL_A, MODEL_B]) {
            if (!existsSync(join(ROOT, 'models', file))) {
                throw new Error(
                    `models/${file} fehlt. Dieser Lauf laedt NICHTS nach: er braucht zwei Modelle `
                    + 'im Cache, um das Umschalten zu messen. Holen: llm/fetch-model.sh --list',
                );
            }
        }

        const evalReport = JSON.parse(await readFile(EVAL_JSON, 'utf8'));

        // ------------------------------------------------------------ 1. Bau
        log('npm run build');
        const buildStarted = Date.now();
        const build = await run('npm', ['run', 'build']);
        timings.buildMs = Date.now() - buildStarted;
        if (build.code !== 0) {
            throw new Error(`npm run build endete mit ${build.code}: ${(build.out + build.err).trim().slice(-600)}`);
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w10-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w10-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);

        // -------------------------------------------------------- 3. Index
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        extras.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // ------------------------------------------------- 4. Server, Proxy
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        report.port = uiPort;
        extras.serverPort = serverPort;
        log(`C-Server auf ${serverPort}, dist/ auf ${uiPort}`);

        // ---------------------------- 5. Das eigene Cache-Verzeichnis, verlinkt
        cacheDir = await mkdtemp('/private/tmp/codeatlasweb-w10-cache-');
        const links = [];
        for (const file of [MODEL_A, MODEL_B]) {
            const source = join(ROOT, 'models', file);
            const target = join(cacheDir, file);
            let how = 'link';
            try {
                await link(source, target);
            } catch (error) {
                how = `copy (link scheiterte: ${error.message})`;
                await copyFile(source, target);
            }
            const info = await stat(target);
            links.push({ file, how, bytes: info.size, links: info.nlink });
        }
        extras.cacheDir = { path: cacheDir, entries: links };
        log(`Cache-Verzeichnis: ${cacheDir} (${links.map((entry) => `${entry.file} per ${entry.how}`).join(', ')})`);

        // ------------------------------------------------ 6. Der Router selbst
        routerPort = await findFreePort(MIN_PORT, [serverPort, uiPort]);
        const routerOrigin = `http://127.0.0.1:${routerPort}`;
        const routerLog = [];
        const routerStarted = Date.now();
        const router = await startSidecar(
            [
                '--host', '127.0.0.1', '--port', String(routerPort),
                '--models-dir', cacheDir,
                '--models-max', '2',
                '--models-autoload',
                '-c', String(ROUTER_CTX),
            ],
            { port: routerPort, log: routerLog },
        );
        routerChild = router.child;
        timings.routerReadyMs = Date.now() - routerStarted;
        log(`Router auf ${routerPort}, bereit nach ${router.readyMs} ms`);

        // ------------------------------------------------------- 7. Browser
        const { chromium } = await import('playwright');
        browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
        const origin = `http://127.0.0.1:${uiPort}`;
        const context = await browser.newContext({ viewport: { ...MAIN_VIEWPORT } });
        await context.grantPermissions(['clipboard-read', 'clipboard-write'], { origin })
            .catch((error) => {
                extras.clipboardPermission = `abgelehnt: ${error.message}`;
            });
        /*
         * Die Umleitung, an der Naht, an der die Anwendung ihre Anfragen selbst
         * stellt. Woertlich nach dem Muster aus smoke-w7c; siehe Kopf.
         */
        await context.addInitScript(({ from, to }) => {
            const original = globalThis.fetch.bind(globalThis);
            globalThis.fetch = (input, init) => {
                const url = typeof input === 'string'
                    ? input
                    : input instanceof URL ? input.href : input?.url ?? '';
                if (typeof url === 'string' && url.startsWith(from)) {
                    return original(to + url.slice(from.length), init);
                }
                return original(input, init);
            };
        }, { from: PRODUCT_SIDECAR_ORIGIN, to: routerOrigin });
        await context.route('**/*', routeFor(origin, routerOrigin));
        const page = await context.newPage();
        page.on('console', (message) => {
            if (message.type() === 'error') {
                extras.consoleErrors.push(message.text());
            }
        });
        page.on('pageerror', (error) => extras.pageErrors.push(String(error)));
        /*
         * Der aufgezeichnete Antwortkoerper jeder Chat-Anfrage.
         *
         * Der Beleg fuer das Umschalten soll aus dem kommen, was die ANWENDUNG
         * geschickt und bekommen hat, und nicht aus einer eigenen Anfrage
         * daneben: eine eigene curl-Anfrage wuerde beweisen, dass der Router
         * umschalten kann, und nicht, dass das Panel ihn dazu bringt.
         */
        page.on('response', (response) => {
            const url = response.url();
            if (!url.includes('/v1/chat/completions')) {
                return;
            }
            void response.json()
                .then((body) => {
                    extras.chatResponses.push({
                        url,
                        status: response.status(),
                        model: typeof body?.model === 'string' ? body.model : '',
                        finishReason: body?.choices?.[0]?.finish_reason ?? '',
                        contentChars: (body?.choices?.[0]?.message?.content ?? '').length,
                        atMs: Date.now() - totalStarted,
                    });
                })
                .catch((error) => {
                    extras.chatResponses.push({
                        url,
                        status: response.status(),
                        model: '',
                        error: String(error).slice(0, 200),
                        atMs: Date.now() - totalStarted,
                    });
                });
        });
        await mkdir(OUT_DIR, { recursive: true });

        const readability = async (name) => {
            const top = await measureReadability(page, READABILITY_EXCLUSIONS);
            const scrolled = await scrollRegionsToEnd(page, READABILITY_EXCLUSIONS);
            await page.waitForTimeout(220);
            const bottom = await measureReadability(page, READABILITY_EXCLUSIONS);
            await resetScroll(page, READABILITY_EXCLUSIONS);
            extras.readability.push({
                name,
                scrolledRegions: scrolled.length,
                top: { candidates: top.candidates, overlaps: top.overlaps, clipped: top.clipped },
                bottom: {
                    candidates: bottom.candidates,
                    overlaps: bottom.overlaps,
                    clipped: bottom.clipped,
                },
            });
            report.overlapViolations += top.overlaps.length + bottom.overlaps.length;
            report.clippingViolations += top.clipped.length + bottom.clipped.length;
            report.cutWithoutHint += [...top.clipped, ...bottom.clipped]
                .filter((entry) => entry.kind === 'cut-without-hint').length;
            log(`Lesbarkeit "${name}": ${top.overlaps.length + bottom.overlaps.length} Ueberlagerungen, `
                + `${top.clipped.length + bottom.clipped.length} Beschneidungen`);
        };

        await openApp(page, origin);

        // ============================================================== AC5 ==
        // Das Panel bei ausgeschaltetem Modell. VOR allem anderen, denn danach
        // laesst sich "es hat nie gefragt" nicht mehr messen.
        const offSeamBefore = await llmSeam(page);
        await openSettingsByMenu(page);
        const offDom = await settingsDom(page);
        const offSettingsSeam = await settingsSeam(page);
        log(`Panel bei ausgeschaltetem Modell: Zustand ${offDom.llmState}, `
            + `${offDom.sections.length} Gruppen, Aktualisieren-Knopf ${offDom.refresh.present}`);

        const offWaitStarted = Date.now();
        await page.waitForTimeout(OFF_WINDOW_MS);
        timings.offWindowMs = Date.now() - offWaitStarted;
        const offSeamAfter = await llmSeam(page);
        const offSettingsAfter = await settingsSeam(page);
        const offRequests = extras.sidecarRequests.length;

        report.llmOffMakesNoRequests =
            offRequests === 0
            && (offSeamAfter?.probes ?? -1) === 0
            && (offSettingsAfter?.probes ?? -1) === 0
            && extras.productPortRequests.length === 0;
        report.panelExplainsItselfWhileOff =
            offDom.present === true
            && offDom.visible === true
            && offDom.llmState === 'off'
            && offDom.llmOff.present === true
            && offDom.llmOff.text.length > 80
            && offDom.cacheCount.present === false
            && offDom.models.length === 0
            && offDom.facts.length === 0
            && offDom.sections.length === 4;
        extras.llmOff = {
            waitedMs: timings.offWindowMs,
            pollIntervalsCovered: Number((timings.offWindowMs / 3000).toFixed(2)),
            sidecarRequests: offRequests,
            probesBefore: offSeamBefore?.probes ?? null,
            probesAfterLlmSeam: offSeamAfter?.probes ?? null,
            probesAfterSettingsSeam: offSettingsAfter?.probes ?? null,
            productPortRequests: extras.productPortRequests.length,
            panelState: offDom.llmState,
            sections: offDom.sections.map((section) => section.name),
            offNoteText: offDom.llmOff.text,
            /*
             * Der Aktualisieren-Knopf: gesehen, nicht gedrueckt, und er steht
             * hier gar nicht. Das ist das Verhalten des Produkts und nicht ein
             * fehlendes Element (SettingsPanel.tsx, Entscheidung 2: ein Knopf,
             * der nichts tut, existiert in dieser Oberflaeche nicht). Gedrueckt
             * wird er weiter unten, mit eingeschaltetem Modell, und dann wird
             * gemessen, dass seine Anfrage im selben Zaehler landet.
             */
            refreshButtonPresent: offDom.refresh.present,
            refreshButtonPressed: false,
            cacheListShown: offDom.cacheCount.present,
            modelRowsShown: offDom.models.length,
            factsShown: offDom.facts.length,
            settingsSeam: offSettingsSeam === null
                ? null
                : {
                    open: offSettingsSeam.open,
                    llmOn: offSettingsSeam.llmOn,
                    router: offSettingsSeam.router,
                    cacheModels: offSettingsSeam.cacheModels.length,
                    suggestions: offSettingsSeam.suggestions.length,
                },
        };
        log(`aus heisst aus: ${offRequests} Anfragen an den Sidecar, `
            + `Proben ${offSeamAfter?.probes}, Panel erklaert sich ${report.panelExplainsItselfWhileOff}`);

        await readability('Einstellungen offen, lokales Modell aus');
        await closeSettings(page);

        // ============================================== Das Modell einschalten
        await page.click('[data-menu="a-llm"]');
        await waitForLlm(page, 'ready', READY_TIMEOUT_MS);
        const readySeam = await settingsSeam(page);
        log(`Modell an: Router ${readySeam?.router}, `
            + `${readySeam?.cacheModels.length} Modelle im Cache, `
            + `laeuft ${readySeam?.running.model}`);

        // ============================================================== AC1 ==
        // Zwei Wege ins Panel, und beide werden gefahren.
        await openSettingsByMenu(page);
        const openedByMenu = await settingsOpen(page);
        await closeSettings(page);
        const closedBetween = !(await settingsOpen(page));
        await openSettingsByCommand(page);
        const openedByCommand = await settingsOpen(page);
        report.settingsPanelOpens = openedByMenu && closedBetween && openedByCommand;
        extras.panelOpens = {
            byMenu: openedByMenu,
            closedBetween,
            byCommandLine: openedByCommand,
            menuSelector: '[data-menu="a-settings"]',
            commandWord: 'settings',
        };
        log(`Panel oeffnet ueber die Menuezeile ${openedByMenu}, `
            + `ueber die Kommandozeile ${openedByCommand}`);

        // ---------------------- Die vier Zahlen, gegen die eigene Abfrage
        /*
         * Erst ein paar Proben abwarten, und der Grund ist eine Messung dieses
         * Laufs: die ERSTE Probe fragt `/v1/models`, bevor sie mit
         * `/props?model=` das Modell ueberhaupt laedt. Der Router fuehrt `meta`
         * nur fuer geladene Instanzen, also kennt die Oberflaeche nach der
         * ersten Probe die Groesse der Gewichte noch nicht und sagt genau das
         * ("the process did not report this"). Ab der zweiten Probe steht die
         * Zahl da. Gemessen wird darum die Lage, die ein Leser nach ein paar
         * Sekunden vor sich hat, und die Zahl der abgewarteten Proben steht im
         * Artefakt.
         */
        await page.waitForFunction(
            () => (globalThis.__atlasSettings?.probes ?? 0) >= 3,
            undefined,
            { timeout: 60000 },
        );
        await page.waitForTimeout(600);
        const panelReady = await settingsDom(page);
        const seamReady = await settingsSeam(page);
        const activeId = seamReady?.cacheModels.find((model) => model.active)?.id
            ?? seamReady?.cacheModels[0]?.id ?? '';
        /*
         * Erst die Props, dann die Liste, und diese Reihenfolge ist keine
         * Geschmacksfrage: `meta` fuehrt der Router NUR fuer geladene Instanzen,
         * und `/props?model=` ist genau der Aufruf, der eine laedt. Andersherum
         * gefragt, waere die Groesse leer und der Vergleich eine Aussage ueber
         * eine Datei, die niemand geoeffnet hat.
         */
        const ownProps = await routerProps(routerOrigin, activeId);
        const ownModels = await routerModels(routerOrigin);
        const ownEntry = (ownModels.data ?? []).find((entry) => entry.id === activeId) ?? {};
        const ownMeta = ownEntry.meta ?? {};
        const factOf = (name) => panelReady.facts.find((fact) => fact.fact === name);

        const nameMatches = (factOf('name')?.value ?? '') === (ownProps.model_path ?? '');
        const quantMatches = (factOf('quantization')?.value ?? '') === (ownProps.model_ftype ?? '');
        const ownCtx = ownProps?.default_generation_settings?.n_ctx;
        const contextMatches =
            (seamReady?.running.contextTokens ?? -1) === ownCtx
            && (factOf('context')?.value ?? '').includes(String(ownCtx))
            && ownCtx === ROUTER_CTX;
        const weightsMatch =
            (seamReady?.running.weightsBytes ?? -1) === (ownMeta.size ?? -2)
            && (factOf('weights')?.value ?? '').length > 0;
        report.runningModelFromProps =
            panelReady.running.present === true
            && panelReady.facts.length === 4
            && nameMatches && quantMatches && contextMatches && weightsMatch;
        extras.runningModel = {
            activeId,
            askedByThisRun: {
                url: `${routerOrigin}/props?model=${activeId}`,
                model_path: ownProps.model_path ?? '',
                model_ftype: ownProps.model_ftype ?? '',
                n_ctx: ownCtx ?? null,
                n_ctx_train: ownMeta.n_ctx_train ?? null,
                size: ownMeta.size ?? null,
                n_params: ownMeta.n_params ?? null,
            },
            shownInPanel: panelReady.facts,
            seamRunning: seamReady?.running ?? null,
            comparison: { nameMatches, quantMatches, contextMatches, weightsMatch },
            probesBeforeReading: seamReady?.probes ?? null,
            firstProbeNote:
                'Nach der ERSTEN Probe steht bei den Gewichten "the process did not report this", '
                + 'und das ist richtig: die Probe fragt /v1/models, bevor sie mit /props?model= das '
                + 'Modell laedt, und der Router fuehrt meta nur fuer geladene Instanzen. Gemessen '
                + 'wurde nach der dritten Probe, also in der Lage, die ein Leser nach wenigen '
                + 'Sekunden vor sich hat.',
        };
        log(`laufendes Modell: Name ${nameMatches}, Quantisierung ${quantMatches} `
            + `(${ownProps.model_ftype}), Kontext ${contextMatches} (${ownCtx}), `
            + `Gewichte ${weightsMatch} (${ownMeta.size} B)`);

        // ------------------------------------------ Die Modelle im Cache
        report.cacheModelsListed = panelReady.models.length;
        extras.cacheModels = {
            rowsInPanel: panelReady.models.length,
            countAttribute: panelReady.cacheCount.count,
            countText: panelReady.cacheCount.text,
            askedByThisRun: (ownModels.data ?? []).map((entry) => ({
                id: entry.id,
                status: entry.status?.value ?? '',
                size: entry.meta?.size ?? null,
            })),
            rows: panelReady.models,
        };
        log(`Cache-Liste: ${panelReady.models.length} Zeilen, Attribut `
            + `${panelReady.cacheCount.count}, eigene Abfrage ${(ownModels.data ?? []).length}`);

        // ----------------------------- Der Aktualisieren-Knopf, jetzt gedrueckt
        const probesBeforeRefresh = (await settingsSeam(page))?.probes ?? -1;
        const requestsBeforeRefresh = extras.sidecarRequests.length;
        let refreshPressed = false;
        if (panelReady.refresh.present) {
            await page.click('[data-testid="atlas-settings-refresh"]');
            await page.waitForTimeout(1200);
            refreshPressed = true;
        }
        const probesAfterRefresh = (await settingsSeam(page))?.probes ?? -1;
        extras.refreshButton = {
            presentWhileOn: panelReady.refresh.present,
            label: panelReady.refresh.text,
            pressed: refreshPressed,
            probesBefore: probesBeforeRefresh,
            probesAfter: probesAfterRefresh,
            sidecarRequestsBefore: requestsBeforeRefresh,
            sidecarRequestsAfter: extras.sidecarRequests.length,
            countsInSameProbeCounter: probesAfterRefresh > probesBeforeRefresh,
        };
        log(`Aktualisieren-Knopf gedrueckt: Proben ${probesBeforeRefresh} -> ${probesAfterRefresh}`);

        // ============================================================== AC3 ==
        const afterRefresh = await settingsDom(page);
        report.suggestionsListed = afterRefresh.suggestions.length;
        const evalByName = new Map((evalReport.models ?? []).map((model) => [model.name, model]));
        const suggestionChecks = [];
        for (const suggestion of afterRefresh.suggestions) {
            const measured = evalByName.get(suggestion.name);
            const fileOnDisk = measured === undefined
                ? null
                : await stat(join(ROOT, 'models', measured.file)).then(
                    (info) => info.size,
                    () => null,
                );
            const check = {
                id: suggestion.suggestion,
                name: suggestion.name,
                repo: suggestion.repo,
                shown: {
                    passRate: numberOf(suggestion['pass-rate']),
                    citation: numberOf(suggestion.citation),
                    tokensPerSecond: numberOf(suggestion['tokens-per-second']),
                    bytes: numberOf(suggestion.bytes),
                    unmeasured: suggestion['citation-unmeasured'],
                },
                fromEvalJson: measured === undefined
                    ? null
                    : {
                        passRate: measured.passRate,
                        citation: measured.citationCompliance,
                        tokensPerSecond: measured.meanTokPerSec,
                        file: measured.file,
                    },
                bytesOnDisk: fileOnDisk,
            };
            check.passRateMatches = measured !== undefined
                && check.shown.passRate === measured.passRate;
            check.citationMatches = measured !== undefined
                && check.shown.citation === measured.citationCompliance;
            check.speedMatches = measured !== undefined
                && check.shown.tokensPerSecond === measured.meanTokPerSec;
            check.bytesMatchFile = fileOnDisk !== null && check.shown.bytes === fileOnDisk;
            check.textCarriesNumbers =
                suggestion.passText.length > 0 && suggestion.citationText.length > 0;
            check.ok = check.passRateMatches && check.citationMatches && check.speedMatches
                && check.bytesMatchFile && check.textCarriesNumbers;
            suggestionChecks.push(check);
        }
        report.suggestionsShowMeasuredNumbers =
            suggestionChecks.length >= 6 && suggestionChecks.every((check) => check.ok);
        extras.suggestions = {
            source: 'verification/w5/eval.json (Trefferquote, Zitattreue, Tempo) und die '
                + 'Dateigroesse in models/ (Bytes)',
            checks: suggestionChecks,
        };
        log(`Vorschlaege: ${report.suggestionsListed}, alle Zahlen gegen die Eval geprueft `
            + `${report.suggestionsShowMeasuredNumbers}`);

        // ------------------------------------ Die nicht gemessenen Antworten
        report.panelShowsUnmeasured =
            afterRefresh.suggestions.length >= 6
            && afterRefresh.suggestions.every((suggestion) =>
                suggestion.unmeasuredBesideCitation === true
                && suggestion.unmeasuredText.length > 0);
        extras.panelUnmeasured = {
            rows: afterRefresh.suggestions.map((suggestion) => ({
                id: suggestion.suggestion,
                attribute: suggestion['citation-unmeasured'],
                text: suggestion.unmeasuredText,
                insideCitationCell: suggestion.unmeasuredBesideCitation,
            })),
            note:
                'Das Attribut ist bei allen sechs leer, und der Satz daneben sagt genau das: '
                + 'verification/w5/eval.json stammt von VOR der Aenderung an der Zitatpruefung '
                + 'und weist die Zahl nicht aus. Eine Null waere hier die Behauptung, es habe '
                + 'keine ungemessene Antwort gegeben.',
        };

        // ------------------------------------------------- Das freie Feld
        const repoField = page.locator('[data-testid="atlas-settings-repo-input"]');
        await repoField.click();
        await repoField.fill('');
        await repoField.pressSequentially(FREE_REPO_INPUT, { delay: 15 });
        await page.waitForTimeout(400);
        const withRepo = await settingsDom(page);
        report.freeRepoFieldAccepted =
            withRepo.repoState.present === true
            && withRepo.repoState.valid === 'true'
            && withRepo.repoState.problem === ''
            && withRepo.repoCommand.present === true
            && withRepo.repoCommand.command.includes(FREE_REPO_INPUT);
        extras.freeRepoField = {
            typed: FREE_REPO_INPUT,
            state: withRepo.repoState,
            command: withRepo.repoCommand.command,
            commandText: withRepo.repoCommand.text,
        };
        log(`freies Feld: "${FREE_REPO_INPUT}" -> "${withRepo.repoCommand.command}"`);

        // ------------------------------------------------ Der Kopieren-Knopf
        let clipboardText = '';
        let clipboardError = '';
        const copyButton = page.locator('.atlas-settings-free [data-testid="atlas-settings-copy"]').first();
        await copyButton.click();
        await page.waitForTimeout(500);
        try {
            clipboardText = await page.evaluate(async () => {
                const value = await navigator.clipboard.readText();
                return typeof value === 'string' ? value : '';
            });
        } catch (error) {
            clipboardError = String(error).slice(0, 300);
        }
        const afterCopy = await settingsDom(page);
        const copyFailedShown = await page.evaluate(() =>
            document.querySelector('[data-testid="atlas-settings-copy-failed"]') !== null);
        const clipboardMatches = clipboardText.trim() === withRepo.repoCommand.command;
        report.commandCopyable =
            afterCopy.repoCommand.present === true
            && afterCopy.repoCommand.command.length > 0
            && copyFailedShown === false
            && clipboardMatches;
        extras.copyButton = {
            method: clipboardError === ''
                ? 'context.grantPermissions([clipboard-read, clipboard-write]) und danach '
                    + 'navigator.clipboard.readText() in der Seite gelesen'
                : `navigator.clipboard.readText() scheiterte (${clipboardError}); gemessen wurde `
                    + 'dann nur der sichtbare Befehlstext und die Abwesenheit der Fehlermeldung',
            clipboard: clipboardText,
            expected: withRepo.repoCommand.command,
            clipboardMatches,
            copyFailedShown,
            visibleCommandText: afterCopy.repoCommand.text,
        };
        log(`Kopieren: Zwischenablage "${clipboardText}" gegen Befehl `
            + `"${withRepo.repoCommand.command}" -> ${clipboardMatches}`);

        // ------------------------------------------ Der Ehrlichkeitssatz
        const honesty = afterCopy.honesty.text;
        const honestyParts = [
            { part: 'a) der Netzzugriff', pattern: /goes out to the network/i },
            { part: 'b) das Ziel', pattern: /huggingface\.co/i },
            {
                part: 'c) wohin geladen wird',
                pattern: /model cache of this project, models\/ next to the repository \(ATLAS_MODELS_DIR or LLAMA_CACHE move it elsewhere\)/i,
            },
            { part: 'd) die Anwendung laedt selbst nichts', pattern: /this application downloads nothing itself/i },
        ].map((entry) => {
            const match = honesty.match(entry.pattern);
            return {
                part: entry.part,
                pattern: String(entry.pattern),
                found: match !== null,
                at: match === null ? -1 : match.index,
                excerpt: match === null ? '' : match[0],
            };
        });
        report.downloadHonestyText =
            afterCopy.honesty.present === true && honestyParts.every((entry) => entry.found);
        extras.honestyText = { text: honesty, parts: honestyParts };
        log(`Ehrlichkeitssatz: ${honestyParts.filter((entry) => entry.found).length} von vier Teilen belegt`);

        // ----------------------------------------- Kein Fortschrittsbalken
        report.noFakeProgressBar = afterCopy.progressElements.length === 0;
        extras.progressElements = {
            found: afterCopy.progressElements,
            count: afterCopy.progressElements.length,
            searched:
                'progress, [role="progressbar"], [aria-valuenow], [class*="progress"], '
                + '[class*="fortschritt"], [data-testid*="progress"], [data-testid*="fortschritt"], '
                + 'im ganzen Panel',
            exempted: {
                testid: 'atlas-settings-no-progress',
                why: 'Der Absatz, der SAGT, dass es keinen Balken gibt. Seine Testmarke traegt das '
                    + 'Wort, nach dem gesucht wird; ihn mitzuzaehlen hiesse, den Satz gegen sich '
                    + 'selbst zu verwenden.',
                text: afterCopy.progressExemption,
            },
        };
        log(`Fortschrittselemente im Panel: ${afterCopy.progressElements.length}`);

        await readability('Einstellungen offen, Modellgruppe');
        extras.shots.push(await shoot(page, SHOT_SETTINGS, 'settings.png'));
        extras.shots.push(await shoot(page, SHOT_FETCH, 'settings-fetch.png', 'model-fetch'));

        // ============================================================== AC2 ==
        // Umschalten, fragen, zurueckschalten, fragen. Zwei Modellnamen aus den
        // aufgezeichneten Antworten.
        await closeSettings(page);
        await focusSymbol(page);
        extras.focusSymbol = await page.evaluate(() => globalThis.__atlasTwin?.qualifiedName ?? '');

        const modelIdA = (ownModels.data ?? []).find((entry) => entry.id.includes('MiniCPM'))?.id ?? '';
        const modelIdB = (ownModels.data ?? []).find((entry) => entry.id.includes('LFM2.5'))?.id ?? '';
        const askedWith = [];

        const pickModel = async (id) => {
            await openSettingsByMenu(page);
            await page.click(`button[data-testid="atlas-settings-model-pick"][data-model="${id}"]`);
            await page.waitForFunction(
                (wanted) => globalThis.__atlasSettings?.selectedModel === wanted,
                id,
                { timeout: 30000 },
            );
            await page.waitForTimeout(2500);
        };

        for (const id of [modelIdB, modelIdA]) {
            await pickModel(id);
            const picked = await settingsDom(page);
            const seamPicked = await settingsSeam(page);
            if (id === modelIdA) {
                // Das Bild nach dem Umschalten: die andere Zeile ist aktiv.
                extras.shots.push(await shoot(page, SHOT_SWITCH, 'settings-switch.png'));
            }
            await closeSettings(page);
            const before = extras.chatResponses.length;
            await ask(page, SWITCH_QUESTION);
            await waitForTurnEnd(page, askedWith.length + 1, ANSWER_TIMEOUT_MS)
                .catch((error) => {
                    extras.chatWaitProblem = String(error).slice(0, 200);
                });
            await page.waitForTimeout(1500);
            const seam = await chatSeam(page);
            const turn = (seam?.turns ?? []).slice(-1)[0];
            askedWith.push({
                selected: id,
                selectedInSeam: seamPicked?.selectedModel ?? '',
                activeRowInPanel: picked.models.find((row) => row.active === 'true')?.model ?? '',
                turnStatus: turn?.status ?? '',
                turnAnswerChars: (turn?.answer ?? '').length,
                responses: extras.chatResponses.slice(before),
                modelInResponse: extras.chatResponses.slice(before)
                    .map((entry) => entry.model).filter(Boolean).slice(-1)[0] ?? '',
            });
            log(`Frage mit ${id}: Zug ${turn?.status}, Antwort vom Modell `
                + `"${askedWith.slice(-1)[0].modelInResponse}"`);
        }

        const answered = askedWith.map((entry) => entry.modelInResponse).filter(Boolean);
        report.switchModelWorks =
            askedWith.length === 2
            && answered.length === 2
            && answered[0] !== answered[1]
            && answered[0] === askedWith[0].selected
            && answered[1] === askedWith[1].selected;
        extras.switching = {
            modelA: modelIdA,
            modelB: modelIdB,
            question: SWITCH_QUESTION,
            turns: askedWith,
            modelNamesInAnswers: answered,
            evidence:
                'Die Modellnamen stammen aus dem Antwortkoerper der Anfrage, die die ANWENDUNG '
                + 'selbst geschickt hat (page.on("response") auf /v1/chat/completions), nicht aus '
                + 'einer eigenen Abfrage dieses Laufs.',
        };
        log(`Umschalten belegt: ${report.switchModelWorks} (${answered.join(' und ')})`);

        // ------------------------------------------- Die Wahl ueberlebt den Reload
        const beforeReload = await settingsSeam(page);
        await page.reload({ waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 30000 });
        await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 30000 });
        await page.waitForFunction(
            () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
            undefined,
            { timeout: 60000 },
        );
        await waitForLlm(page, 'ready', READY_TIMEOUT_MS);
        await page.waitForTimeout(2000);
        const afterReload = await settingsSeam(page);
        report.switchPersistsReload =
            (beforeReload?.selectedModel ?? '') !== ''
            && afterReload?.selectedModel === beforeReload?.selectedModel;
        extras.selectionAcrossReload = {
            before: beforeReload?.selectedModel ?? '',
            after: afterReload?.selectedModel ?? '',
            storageKey: afterReload?.storageKeys?.model ?? '',
            storedValue: await page.evaluate((key) => {
                try {
                    return globalThis.localStorage.getItem(key);
                } catch {
                    return null;
                }
            }, afterReload?.storageKeys?.model ?? ''),
        };

        // -------------------------------------------- Die Statusleiste nennt es
        const chip = await statusChip(page);
        const expectedName = (afterReload?.cacheModels ?? [])
            .find((model) => model.id === afterReload?.selectedModel)?.name ?? '';
        report.statusBarNamesModel =
            chip.present === true
            && expectedName.length > 0
            && chip.text.includes(expectedName);
        extras.statusBar = { chip, expectedName, selected: afterReload?.selectedModel ?? '' };
        log(`Statusleiste: "${chip.text}" nennt ${expectedName} -> ${report.statusBarNamesModel}`);

        // ============================================================== AC9 ==
        await openSettingsByMenu(page);
        const perfBefore = await perfSeam(page);
        extras.frameRateAtStart = perfBefore;
        if (perfBefore === null || perfBefore.running !== true) {
            throw new Error(
                'die Szene zeichnet nicht, also ist an ihr nichts zu messen: '
                + `__atlasGalaxyPerf = ${JSON.stringify(perfBefore)}`,
            );
        }
        log(`Bildrate vor der ersten Einstellung: ${perfBefore.fps.toFixed(1)} fps `
            + `(${perfBefore.nodes} Knoten, ${perfBefore.edges} Kanten, Band ${perfBefore.noiseBand.toFixed(1)})`);

        /**
         * Eine Einstellung umlegen und lesen, was das Panel dazu misst.
         *
         * Die Reihenfolge ist die Messung des Produkts und nicht die dieses
         * Laufs: erst laeuft die Szene lange genug ungestoert, damit es ein
         * Vorher-Fenster und ein Rauschband gibt, dann faellt der Klick, dann
         * wird gewartet, bis das Panel sein Urteil geschrieben hat. Gelesen wird
         * beides, das Attribut am Absatz und die Naht, und beide werden
         * verglichen: gingen sie auseinander, waere eine der beiden Zahlen
         * erfunden.
         */
        const measureSetting = async (label, clickSelector, settingKey) => {
            await page.waitForTimeout(SETTLE_BEFORE_MS);
            const perfAt = await perfSeam(page);
            await page.click(clickSelector);
            await page.waitForTimeout(MEASURE_WAIT_MS);
            const dom = await settingsDom(page);
            const seam = await settingsSeam(page);
            const shown = dom.measures.find((entry) => entry.setting === settingKey);
            const inSeam = (seam?.measurements ?? [])
                .filter((entry) => entry.setting === settingKey).slice(-1)[0];
            const perfAfter = await perfSeam(page);
            const entry = {
                label,
                setting: settingKey,
                clicked: clickSelector,
                value: dom.choices.find((choice) => choice.setting === settingKey)?.value
                    ?? (settingKey === 'thrifty' || settingKey === 'default' ? settingKey : ''),
                verdict: shown?.verdict ?? '',
                before: numberOf(shown?.before),
                after: numberOf(shown?.after),
                band: numberOf(shown?.band),
                nodes: numberOf(shown?.nodes),
                edges: numberOf(shown?.edges),
                at: shown?.at ?? '',
                text: shown?.text ?? '',
                seam: inSeam === undefined
                    ? null
                    : {
                        before: Number(inSeam.before.toFixed(2)),
                        after: Number(inSeam.after.toFixed(2)),
                        band: Number(inSeam.band.toFixed(2)),
                        verdict: inSeam.verdict,
                        nodes: inSeam.nodes,
                        edges: inSeam.edges,
                    },
                frameSeamBefore: perfAt === null ? null : { fps: Number(perfAt.fps.toFixed(2)), cap: perfAt.cap },
                frameSeamAfter: perfAfter === null ? null : { fps: Number(perfAfter.fps.toFixed(2)), cap: perfAfter.cap },
            };
            /*
             * Passen Anzeige und Naht zusammen? Verglichen wird auf der Genauigkeit
             * der ANZEIGE (eine Nachkommastelle unter 10, sonst ganze Zahlen), denn
             * genau die steht im Attribut; ein Unterschied darueber hinaus waere
             * eine zweite Rechnung fuer dieselbe Zahl.
             */
            const round = (value) => value === null ? null
                : value >= 10 ? Math.round(value) : Number(value.toFixed(1));
            entry.seamAgrees = inSeam === undefined ? false
                : entry.verdict === inSeam.verdict
                    && entry.before === round(inSeam.before)
                    && entry.after === round(inSeam.after);
            extras.measurements.push(entry);
            log(`  ${label}: ${entry.verdict} (vorher ${entry.before}, nachher ${entry.after}, `
                + `Band ${entry.band}, Naht stimmt ${entry.seamAgrees})`);
            return entry;
        };

        const optionSelector = (setting, option) =>
            `[data-setting="${setting}"] [data-testid="atlas-settings-option"][data-option="${option}"]`;

        // 1. Zwei Dimensionen statt drei.
        const clip = await sceneRect(page);
        if (clip === null || clip.width < 200 || clip.height < 200) {
            throw new Error(`die Zeichenflaeche ist zu klein zum Messen: ${JSON.stringify(clip)}`);
        }
        extras.sceneRect = clip;

        /**
         * Der Beleg fuer die Ansicht von oben, am BILD und nicht am Schalter.
         *
         * Eine orthografische Kamera hat keinen Fluchtpunkt: ein Schwenk
         * verschiebt das ganze Bild um denselben Betrag, und die beiden
         * Aufnahmen decken sich wieder, sobald man eine um genau diesen Betrag
         * zurueckschiebt. Eine perspektivische Kamera hat einen Fluchtpunkt:
         * nahe Punkte wandern weiter als ferne, und KEIN Versatz bringt die
         * beiden Bilder zur Deckung. Genau dieser Unterschied wird gemessen,
         * in beiden Ansichten, mit demselben Handgriff.
         *
         * Dazu zwei Kontrollzahlen, ohne die die erste nichts wert waere:
         *
         *  - `idleDrift`: zwei Aufnahmen OHNE jede Eingabe. Zeichnet die Szene
         *    etwas Bewegtes, ist sie nicht null, und dann ist auch ein Rest nach
         *    dem Schwenk nicht null. Verglichen wird darum gegen diese Zahl und
         *    nicht gegen die Null.
         *  - `residualAtZero`: derselbe Vergleich ohne Versatz. Er zeigt, dass
         *    der Schwenk das Bild ueberhaupt bewegt hat; ohne ihn koennte eine
         *    Kamera, die sich gar nicht ruehrt, als "starr verschoben" gelten.
         *
         * Der Schwenk laeuft mit der rechten Maustaste, weil OrbitControls sie
         * dafuer vorsieht. Die linke dreht, und in der flachen Ansicht ist das
         * Drehen abgeschaltet; auch das wird gemessen, als zweites Stueck.
         */
        const dragProbe = async (name, button, dx) => {
            await page.mouse.move(clip.x + clip.width / 2, clip.y + clip.height / 2);
            await page.waitForTimeout(900);
            await grab(page, `${name}-a`, clip);
            await page.waitForTimeout(500);
            await grab(page, `${name}-idle`, clip);
            const idle = await shiftedDifference(page, {
                base: `${name}-a`, variant: `${name}-idle`, maxShift: 4,
            });
            const cx = clip.x + clip.width / 2;
            const cy = clip.y + clip.height / 2;
            await page.mouse.move(cx, cy);
            await page.mouse.down({ button });
            for (let step = 1; step <= 10; step += 1) {
                await page.mouse.move(cx + (dx * step) / 10, cy);
                await page.waitForTimeout(30);
            }
            await page.mouse.up({ button });
            await page.waitForTimeout(1200);
            await grab(page, `${name}-b`, clip);
            const moved = await shiftedDifference(page, {
                base: `${name}-a`, variant: `${name}-b`, maxShift: Math.abs(dx) * 3,
            });
            return {
                name,
                button,
                dragPx: dx,
                idleDrift: idle?.residualAtZero ?? -1,
                residualAtZero: moved?.residualAtZero ?? -1,
                residualAtBestShift: moved?.residualAtBestShift ?? -1,
                bestShift: moved?.bestShift ?? 0,
                image: moved === null ? null : { width: moved.width, height: moved.height },
            };
        };

        const spatialPan = await dragProbe('spatial-pan', 'right', 120);
        const spatialRotate = await dragProbe('spatial-rotate', 'left', 120);
        log(`raeumlich: Schwenk Rest ${spatialPan.residualAtBestShift} bei Versatz `
            + `${spatialPan.bestShift} (Ruhe ${spatialPan.idleDrift}), Drehen aendert `
            + `${spatialRotate.residualAtZero}`);

        await measureSetting(
            '2D statt 3D (projection -> flat)',
            optionSelector('projection', 'flat'),
            'projection',
        );
        await page.waitForTimeout(1500);
        const flatPan = await dragProbe('flat-pan', 'right', 120);
        const flatRotate = await dragProbe('flat-rotate', 'left', 120);
        log(`flach: Schwenk Rest ${flatPan.residualAtBestShift} bei Versatz `
            + `${flatPan.bestShift} (Ruhe ${flatPan.idleDrift}), Drehen aendert `
            + `${flatRotate.residualAtZero}`);

        const seamFlat = await settingsSeam(page);
        const domFlat = await settingsDom(page);
        /*
         * Die Schranke, in Worten: nach dem Schwenk decken sich die beiden
         * Bilder in der flachen Ansicht bis auf das, was die Szene ohnehin von
         * Bild zu Bild schwankt (plus ein halbes Grauwertniveau Luft). In der
         * raeumlichen tun sie das nicht. Und beide Schwenks haben das Bild
         * wirklich bewegt, sonst waere die Deckung eine Selbstverstaendlichkeit.
         */
        const rigidity = (probe) => probe.residualAtZero <= 0
            ? -1
            : Number((probe.residualAtBestShift / probe.residualAtZero).toFixed(4));
        const flatRigidity = rigidity(flatPan);
        const spatialRigidity = rigidity(spatialPan);
        const flatIsRigid = flatRigidity >= 0 && flatRigidity <= 0.3;
        const spatialIsNotRigid = spatialRigidity > flatRigidity * 2;
        const bothPansMoved =
            flatPan.residualAtZero > flatPan.idleDrift + 1
            && spatialPan.residualAtZero > spatialPan.idleDrift + 1
            && flatPan.bestShift !== 0 && spatialPan.bestShift !== 0;
        /*
         * Das Drehen, als zweites Stueck und mit einer harten Kante: in der
         * flachen Ansicht ist es abgeschaltet, ein Zug mit der linken Taste
         * aendert also NICHTS am Bild. In der raeumlichen dreht derselbe Zug die
         * Szene um die Achse, die die flache gerade fallen laesst.
         */
        const rotationDropped =
            flatRotate.residualAtZero <= flatRotate.idleDrift + 0.5
            && spatialRotate.residualAtZero > spatialRotate.idleDrift + 1;
        report.twoDimensionalMode =
            seamFlat?.display?.projection === 'flat'
            && domFlat.choices.find((choice) => choice.setting === 'projection')?.value === 'flat'
            && flatIsRigid && spatialIsNotRigid && bothPansMoved && rotationDropped;
        extras.twoDimensional = {
            method:
                'Gemessen am gerenderten Bild und nicht am Schalter. Eine orthografische Kamera hat '
                + 'keinen Fluchtpunkt: ein Schwenk verschiebt das ganze Bild um denselben Betrag, '
                + 'die beiden Aufnahmen decken sich also wieder, sobald eine um genau diesen Betrag '
                + 'zurueckgeschoben wird. Eine perspektivische Kamera hat einen: nahe Punkte wandern '
                + 'weiter als ferne, und kein Versatz bringt die Bilder zur Deckung. Der beste '
                + 'Versatz wird ueber die Spaltensummen gesucht und dann am ganzen Bild nachgerechnet; '
                + 'die Zahlen sind mittlere Grauwertunterschiede je Pixel (0 bis 255).',
            spatialPan,
            flatPan,
            spatialRotate,
            flatRotate,
            rigidity: {
                what: 'Rest nach dem besten Versatz, geteilt durch den Rest ohne Versatz. Null '
                    + 'heisst: der Schwenk hat das Bild genau verschoben und sonst nichts.',
                flat: flatRigidity,
                spatial: spatialRigidity,
            },
            verdicts: {
                flatIsRigidUnderPan: flatIsRigid,
                spatialIsNotRigidUnderPan: spatialIsNotRigid,
                bothPansMovedTheImage: bothPansMoved,
                rotationDroppedInFlatView: rotationDropped,
            },
            switchValue: domFlat.choices.find((choice) => choice.setting === 'projection')?.value ?? '',
            seamValue: seamFlat?.display?.projection ?? '',
        };
        log(`2D: Starrheit flach ${flatRigidity} gegen raeumlich ${spatialRigidity}, `
            + `Drehen faellt weg ${rotationDropped}, beide Schwenks bewegten das Bild `
            + `${bothPansMoved} -> ${report.twoDimensionalMode}`);

        await readability('Einstellungen offen, flache Ansicht');

        await measureSetting(
            'zurueck auf 3D (projection -> spatial)',
            optionSelector('projection', 'spatial'),
            'projection',
        );

        /*
         * 2. Die Effektschalter, jeder einzeln und danach zurueckgesetzt.
         *
         * Es waren vier; seit W11a sind es fuenf. Die Agentenebene gehoert in
         * diese Runde, weil sie in dieselbe Gruppe gehoert: die Zusicherung
         * dieses Laufs ist "JEDE Einstellung nennt ihre gemessenen Zahlen", und
         * ein Lauf, der acht von neun misst, wuerde sie fuer die neunte
         * behaupten.
         */
        const effectPlan = [
            { key: 'halos', off: 'false', on: 'true', name: 'Leuchthoefe' },
            { key: 'bloom', off: 'false', on: 'true', name: 'Bloom' },
            { key: 'edges', off: 'off', on: 'full', name: 'Kantendichte' },
            { key: 'labels', off: '1', on: '0', name: 'Beschriftungsentfernung' },
            { key: 'agents', off: 'false', on: 'true', name: 'Agentenebene' },
        ];
        const effectToggles = [];
        for (const effect of effectPlan) {
            const away = await measureSetting(
                `${effect.name} aus (${effect.key} -> ${effect.off})`,
                optionSelector(effect.key, effect.off),
                effect.key,
            );
            const back = await measureSetting(
                `${effect.name} zurueck (${effect.key} -> ${effect.on})`,
                optionSelector(effect.key, effect.on),
                effect.key,
            );
            effectToggles.push({
                name: effect.key,
                label: effect.name,
                changedTo: effect.off,
                before: away.before,
                after: away.after,
                band: away.band,
                verdict: away.verdict,
                text: away.text,
                resetTo: effect.on,
                resetBefore: back.before,
                resetAfter: back.after,
                resetVerdict: back.verdict,
            });
        }
        report.effectToggles = effectToggles;

        // 3. Der Bildratendeckel, 60 und 30.
        const capUncapped = extras.frameRateAtStart;
        const cap60 = await measureSetting(
            'Bildratendeckel 60 (frameCap -> 60)',
            optionSelector('frameCap', '60'),
            'frameCap',
        );
        const perfAt60 = await perfSeam(page);
        const cap30 = await measureSetting(
            'Bildratendeckel 30 (frameCap -> 30)',
            optionSelector('frameCap', '30'),
            'frameCap',
        );
        const perfAt30 = await perfSeam(page);
        const capOff = await measureSetting(
            'Bildratendeckel aus (frameCap -> 0)',
            optionSelector('frameCap', '0'),
            'frameCap',
        );
        const perfAtOff = await perfSeam(page);
        /*
         * Was `frameCapWorks` behauptet, und was nicht.
         *
         * Behauptet wird: der gewaehlte Deckel kommt in der Szene an (die Naht
         * meldet ihn), und das Panel nennt zu jeder Wahl eine gemessene Zahl.
         * NICHT behauptet wird, dass die Bildrate danach faellt: in dieser
         * Umgebung liegt sie ohne Deckel bereits unter 30, und ein Deckel, unter
         * dem eine Maschine ohnehin bleibt, aendert nichts. Genau diesen Fall
         * nennt das Panel "kein messbarer Unterschied", und genau das ist die
         * ehrliche Auskunft. Ob der Deckel bei einer schnelleren Maschine
         * greift, hat dieser Lauf NICHT gemessen.
         */
        const capsArrive =
            (perfAt60?.cap ?? -1) === 60 && (perfAt30?.cap ?? -1) === 30 && (perfAtOff?.cap ?? -1) === 0;
        const capsMeasured = [cap60, cap30, capOff]
            .every((entry) => entry.verdict !== 'not-measured' && entry.verdict !== 'measuring');
        report.frameCapWorks = capsArrive && capsMeasured;
        extras.frameCap = {
            claim:
                'Der gewaehlte Deckel kommt in der Szene an und das Panel nennt zu jeder Wahl eine '
                + 'gemessene Zahl. Dass die Bildrate faellt, ist NICHT behauptet: siehe die Zahlen.',
            uncapped: capUncapped === null ? null : { fps: Number(capUncapped.fps.toFixed(2)), cap: capUncapped.cap },
            at60: perfAt60 === null ? null : { fps: Number(perfAt60.fps.toFixed(2)), cap: perfAt60.cap },
            at30: perfAt30 === null ? null : { fps: Number(perfAt30.fps.toFixed(2)), cap: perfAt30.cap },
            backToNone: perfAtOff === null ? null : { fps: Number(perfAtOff.fps.toFixed(2)), cap: perfAtOff.cap },
            capsArriveInScene: capsArrive,
            everyChoiceMeasured: capsMeasured,
            measurements: [cap60, cap30, capOff].map((entry) => ({
                label: entry.label, verdict: entry.verdict, before: entry.before, after: entry.after,
            })),
        };
        log(`Bildratendeckel: ohne ${extras.frameCap.uncapped?.fps}, bei 60 ${extras.frameCap.at60?.fps}, `
            + `bei 30 ${extras.frameCap.at30?.fps}, Deckel kommt an ${capsArrive}`);

        // 4. Das Sparprofil und der Weg zurueck.
        const thrifty = await measureSetting(
            'Sparprofil',
            '[data-testid="atlas-settings-profile"][data-profile="thrifty"]',
            'thrifty',
        );
        const seamThrifty = await settingsSeam(page);
        const thriftyExpected = {
            projection: 'flat', halos: false, bloom: false,
            edges: 'dim', labelDistanceFactor: 1, frameCap: 30,
        };
        const thriftyMatches = Object.entries(thriftyExpected)
            .every(([key, value]) => seamThrifty?.display?.[key] === value);
        report.thriftProfileWorks =
            thriftyMatches
            && thrifty.verdict !== 'not-measured'
            && thrifty.verdict !== 'measuring'
            && thrifty.before !== null
            && thrifty.after !== null;
        extras.thriftyProfile = {
            expected: thriftyExpected,
            afterClick: seamThrifty?.display ?? null,
            allSixSetInOneStep: thriftyMatches,
            measurement: {
                verdict: thrifty.verdict, before: thrifty.before, after: thrifty.after,
                band: thrifty.band, text: thrifty.text,
            },
        };
        log(`Sparprofil: alle sechs Werte in einem Zug ${thriftyMatches}, `
            + `${thrifty.before} -> ${thrifty.after} (${thrifty.verdict})`);

        // ------------------------------ Die Wahl ueberlebt den Reload (AC9)
        const displayBeforeReload = seamThrifty?.display ?? null;
        await page.reload({ waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-galaxy"]', { timeout: 30000 });
        await page.waitForFunction(
            () => (globalThis.__atlasGalaxy?.nodes ?? 0) > 0,
            undefined,
            { timeout: 60000 },
        );
        await page.waitForTimeout(1500);
        const seamAfterDisplayReload = await settingsSeam(page);
        report.settingsPersistReload =
            displayBeforeReload !== null
            && JSON.stringify(seamAfterDisplayReload?.display ?? {})
                === JSON.stringify(displayBeforeReload);
        extras.displayAcrossReload = {
            before: displayBeforeReload,
            after: seamAfterDisplayReload?.display ?? null,
            storageKey: seamAfterDisplayReload?.storageKeys?.display ?? '',
            storedValue: await page.evaluate((key) => {
                try {
                    return globalThis.localStorage.getItem(key);
                } catch {
                    return null;
                }
            }, seamAfterDisplayReload?.storageKeys?.display ?? ''),
        };
        log(`Anzeige-Einstellungen ueberleben den Reload: ${report.settingsPersistReload}`);

        /*
         * Nach dem Reload steht das Panel wieder ohne seine Messungen da: es
         * fuehrt sie in seinem eigenen Zustand (SettingsPanel.tsx), und der geht
         * mit der Seite. Fuer die Frage, ob JEDE Einstellung ihren Effekt nennt,
         * muessen darum alle neun in EINEM offenen Panel gemessen worden sein.
         * Das ist der Grund, dass die Runde jetzt noch einmal faellt, statt die
         * Zahlen von vorhin zu behaupten.
         */
        await openSettingsByMenu(page);
        await page.waitForTimeout(SETTLE_BEFORE_MS);
        const secondRound = [
            ['projection', optionSelector('projection', 'spatial')],
            ['halos', optionSelector('halos', 'true')],
            ['bloom', optionSelector('bloom', 'true')],
            ['edges', optionSelector('edges', 'full')],
            ['labels', optionSelector('labels', '0')],
            ['agents', optionSelector('agents', 'true')],
            /*
             * Die vier Wirkungen der Agentenbewegung (W11b AC7b). Sie stehen in
             * derselben Gruppe und tragen darum dieselbe Pflicht: jede nennt
             * ihren an DIESER Szene gemessenen Effekt auf die Bildrate.
             */
            ['agentTails', optionSelector('agentTails', 'true')],
            ['agentTrails', optionSelector('agentTrails', 'true')],
            ['agentWaves', optionSelector('agentWaves', 'true')],
            ['agentTimeline', optionSelector('agentTimeline', 'true')],
            ['frameCap', optionSelector('frameCap', '0')],
            ['thrifty', '[data-testid="atlas-settings-profile"][data-profile="thrifty"]'],
            ['default', '[data-testid="atlas-settings-profile"][data-profile="default"]'],
        ];
        for (const [key, selector] of secondRound) {
            await measureSetting(`zweite Runde: ${key}`, selector, key);
        }

        const finalDom = await settingsDom(page);
        const measureRows = finalDom.measures;
        const withoutNumbers = measureRows.filter((row) => {
            if (row.verdict === 'not-measured' || row.verdict === 'measuring') {
                return true;
            }
            if (row.verdict === 'not-drawing') {
                // Eine Auskunft und keine Zahl, und sie ist die richtige Antwort,
                // wenn nichts gezeichnet wurde. Hier wurde gezeichnet.
                return true;
            }
            if (row.verdict === 'no-difference') {
                return !(row.text.includes('no measurable difference')
                    && /\d/.test(row.before) && /\d/.test(row.after));
            }
            return !(/\d/.test(row.before) && /\d/.test(row.after)
                && row.text.includes(row.before) && row.text.includes(row.after));
        });
        /*
         * Und kein Versprechen ohne Zahl. Gefragt wird satzweise: steht in
         * demselben Satz eine Zahl, ist das Wort an eine Messung gebunden.
         */
        const displaySentences = finalDom.displayText.split(/(?<=[.:])\s+/);
        const promises = [];
        const watched = [];
        for (const sentence of displaySentences) {
            for (const word of PROMISE_WORDS) {
                if (new RegExp(`\\b${word}\\b`, 'i').test(sentence) && !/\d/.test(sentence)) {
                    promises.push({ word, sentence: sentence.slice(0, 200) });
                }
            }
            for (const word of WATCHED_WORDS) {
                if (new RegExp(`\\b${word}\\b`, 'i').test(sentence)) {
                    watched.push({ word, sentence: sentence.slice(0, 200), hasNumber: /\d/.test(sentence) });
                }
            }
        }
        report.everyToggleNamesMeasuredEffect =
            measureRows.length >= 9 && withoutNumbers.length === 0 && promises.length === 0;
        extras.toggleTexts = {
            rows: measureRows,
            rowsWithoutNumbers: withoutNumbers,
            promiseWordsWithoutNumber: promises,
            promiseWordsSearched: PROMISE_WORDS,
            watchedWordsSearched: WATCHED_WORDS,
            watchedWordHits: watched,
            watchedWordNote:
                'Diese Woerter tragen die Zusicherung nicht. Sie stehen hier mit ihrem Satz, damit '
                + 'ein Leser des Artefakts selbst sieht, was die Suche gefunden hat und warum es '
                + 'kein Versprechen ist.',
        };
        log(`jede Einstellung nennt ihre Zahlen: ${report.everyToggleNamesMeasuredEffect} `
            + `(${measureRows.length} Absaetze, ${withoutNumbers.length} ohne Zahl, `
            + `${promises.length} Versprechen ohne Zahl)`);

        // ------------------------------- Wo nichts messbar war, steht der Satz
        let noDifference = extras.measurements.filter((entry) => entry.verdict === 'no-difference');
        let forcedNoDifference = null;
        if (noDifference.length === 0) {
            /*
             * Ergab jede Einstellung einen Unterschied, dann wird gezeigt, dass
             * die Anzeige den anderen Fall KANN: dieselbe Wahl ein zweites Mal
             * gesetzt aendert an der Szene nichts, und das Urteil muss darum
             * "kein messbarer Unterschied" lauten.
             */
            forcedNoDifference = await measureSetting(
                'dieselbe Wahl noch einmal (projection -> spatial, war schon spatial)',
                optionSelector('projection', 'spatial'),
                'projection',
            );
            noDifference = extras.measurements.filter((entry) => entry.verdict === 'no-difference');
        }
        report.noEffectSaysSo =
            noDifference.length > 0
            && noDifference.every((entry) => entry.text.includes('no measurable difference'));
        extras.noDifference = {
            found: noDifference.map((entry) => ({
                label: entry.label, before: entry.before, after: entry.after,
                band: entry.band, text: entry.text,
            })),
            forced: forcedNoDifference === null
                ? null
                : {
                    why: 'Keine der acht Einstellungen ergab in diesem Lauf ein Urteil '
                        + '"kein messbarer Unterschied". Belegt wird darum, dass die Anzeige '
                        + 'diesen Fall erreichen kann: dieselbe Wahl ein zweites Mal gesetzt.',
                    label: forcedNoDifference.label,
                    verdict: forcedNoDifference.verdict,
                    text: forcedNoDifference.text,
                },
        };

        /*
         * Das Bild der Leistungsgruppe, und es steht genau HIER.
         *
         * Vorher waere es ein Bild von neun Absaetzen, in denen "not measured
         * yet" steht, und das ist der Zustand, den AC9 gerade nicht meint. Nach
         * dieser Runde tragen alle neun ihr Urteil und ihre zwei Zahlen, die
         * Szene zeichnet noch, und der Lebendzaehler laeuft. Die Gruppe ist
         * hoeher als ihr Fenster; welche Lage die meisten der genannten Teile
         * zeigt, sucht fitScroll und schreibt es auf.
         */
        extras.shots.push(await shoot(page, SHOT_PERFORMANCE, 'settings-performance.png', 'display', {
            fit: true,
            why:
                'Das Panel ist hoeher als sein Fenster, und die Gruppe "Darstellung und Leistung" '
                + 'ist hoeher als das Fenster allein: der Lebendzaehler steht an ihrem Anfang, das '
                + 'Sparprofil an ihrem Ende, und zwischen beiden liegen die neun Messabsaetze. Es '
                + 'gibt darum keine Lage, die alles zeigt. Aufgenommen ist die BESTE verfuegbare, '
                + 'gesucht durch Abfahren der Gruppe in 17 Schritten und Zaehlen dessen, was an '
                + 'jeder Lage ganz im Bild steht (siehe framing.tried): sieben von acht '
                + 'Messabsaetzen mit ihren zwei Zahlen und beide Profilknoepfe. Der Lebendzaehler '
                + 'faellt dabei heraus; seine Zahlen stehen in extras.frameCap und in jeder '
                + 'Messung.',
        }));

        await readability('Einstellungen offen, Leistungsgruppe');

        // --------------------- Die Schalter, die bleiben, wo sie sind
        await closeSettings(page);
        const controlsBefore = await viewOnlyControls(page);
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(400);
        const controlsOpen = await viewOnlyControls(page);
        const legendRows = controlsOpen.edgeFilters.length;
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await page.waitForTimeout(400);
        const controlsClosed = await viewOnlyControls(page);
        report.viewOnlyControlsStayed =
            controlsBefore.legendToggle.present === true
            && controlsOpen.legendOpen === true
            && controlsClosed.legendOpen === false
            && legendRows >= 1
            && controlsOpen.modeChips.length === 2
            && controlsOpen.modeChips.some((chipEntry) => chipEntry.mode === 'galaxy')
            && controlsOpen.modeChips.some((chipEntry) => chipEntry.mode === 'hierarchy')
            && controlsOpen.twinDepth.present === true;
        extras.viewOnlyControls = {
            legendToggle: {
                present: controlsBefore.legendToggle.present,
                opens: controlsOpen.legendOpen,
                closesAgain: controlsClosed.legendOpen === false,
            },
            graphModeChips: controlsOpen.modeChips,
            edgeKindFilters: { count: legendRows, types: controlsOpen.edgeFilters },
            twinDetail: controlsOpen.twinDepth,
            note:
                'Vier Bedienelemente ohne Rechenkosten, einzeln gezaehlt. AC9 verlangt die '
                + 'Buendelung des Teuren und ausdruecklich nicht ein Menue, in dem alles '
                + 'verschwindet.',
        };
        log(`Ansichtsschalter geblieben: Legende ${controlsOpen.legendOpen}/${controlsClosed.legendOpen}, `
            + `${controlsOpen.modeChips.length} Modus-Chips, ${legendRows} Kantenfilter, `
            + `Twin-Regler ${controlsOpen.twinDepth.present}`);

        extras.apiRoutes = { ...proxy.log.apiRoutes };
        await context.close();

        // ============================================================== AC4 ==
        // Das Startskript, ohne einen Prozess zu starten.
        const printDefault = await run('sh', ['llm/start.sh', '--print-command']);
        const printAlias = await run('sh', ['llm/start.sh', '--print-command', 'class-a']);
        const printEnv = await run('sh', ['llm/start.sh', '--print-command'], {
            env: { ATLAS_MODEL: 'models/Qwen3.5-4B-Q4_K_M.gguf' },
        });
        emptyDir = await mkdtemp('/private/tmp/codeatlasweb-w10-empty-');
        const emptyRun = await run('sh', ['llm/start.sh'], { env: { ATLAS_MODELS_DIR: emptyDir } });
        const printDefaultLine = printDefault.out.trim();

        report.startScriptTakesModel =
            printAlias.code === 0
            && printAlias.out.includes('-m models/Qwen3.5-2B-Q4_K_M.gguf')
            && printEnv.code === 0
            && printEnv.out.includes('-m models/Qwen3.5-4B-Q4_K_M.gguf');
        report.startScriptSaysHowToFetch =
            emptyRun.code !== 0
            && /fetch-model\.sh/.test(emptyRun.err + emptyRun.out)
            && /-hf/.test(emptyRun.err + emptyRun.out);
        extras.startScript = {
            defaultCall: {
                command: 'sh llm/start.sh --print-command',
                exit: printDefault.code,
                line: printDefaultLine,
                hasModelsDir: printDefaultLine.includes('--models-dir'),
                hasFixedModel: / -m /.test(printDefaultLine),
            },
            withArgument: {
                command: 'sh llm/start.sh --print-command class-a',
                exit: printAlias.code,
                line: printAlias.out.trim(),
            },
            withEnvironment: {
                command: 'ATLAS_MODEL=models/Qwen3.5-4B-Q4_K_M.gguf sh llm/start.sh --print-command',
                exit: printEnv.code,
                line: printEnv.out.trim(),
            },
            emptyCache: {
                command: `ATLAS_MODELS_DIR=${emptyDir} sh llm/start.sh`,
                exit: emptyRun.code,
                output: (emptyRun.err + emptyRun.out).trim(),
                namesFetchScript: /fetch-model\.sh/.test(emptyRun.err + emptyRun.out),
                namesHfSwitch: /-hf/.test(emptyRun.err + emptyRun.out),
                startedNothing:
                    'Der Aufruf endet vor jeder Port- und PID-Pruefung; 4141 wurde nicht angefasst.',
            },
        };
        log(`Startskript: Vorgabe "${printDefaultLine}"`);
        log(`  mit Argument ${report.startScriptTakesModel}, leerer Cache endet mit `
            + `${emptyRun.code} und nennt den Weg ${report.startScriptSaysHowToFetch}`);

        // ------------------------------------- Keine Gewichte im Repository
        const tracked = await run('git', ['ls-files', 'models/']);
        const trackedFiles = tracked.out.split('\n').map((line) => line.trim()).filter(Boolean);
        const ignoreChecks = [];
        for (const file of EXPECTED_MODEL_FILES) {
            const ignored = await run('git', ['check-ignore', '-v', `models/${file}`]);
            ignoreChecks.push({
                file: `models/${file}`,
                exit: ignored.code,
                ignored: ignored.code === 0,
                rule: ignored.out.trim(),
            });
        }
        const partA = trackedFiles.length === 1 && trackedFiles[0] === 'models/SHA256SUMS';
        const partB = ignoreChecks.every((entry) => entry.ignored);
        const partC = !/ -m /.test(printDefaultLine) && !/\.gguf/.test(printDefaultLine);
        report.noWeightsInRepo = partA && partB && partC;
        extras.noWeightsInRepoParts = {
            a: { claim: 'git ls-files models/ listet nur models/SHA256SUMS', tracked: trackedFiles, ok: partA },
            b: { claim: 'jede models/*.gguf wird von git check-ignore bestaetigt', checks: ignoreChecks, ok: partB },
            c: {
                claim: 'llm/start.sh hat in seiner Vorgabe keinen Dateinamen',
                line: printDefaultLine,
                ok: partC,
            },
        };
        extras.noWeightsInRepoMethod =
            'Die sechs Eval-Dateien liegen weiterhin im Arbeitsverzeichnis unter models/, weil '
            + 'tools/eval-check.mjs und die Laeufe w5a, w5b und w6 sie dort erwarten; versioniert '
            + 'ist keine von ihnen. Gemessen sind drei Dinge und nur diese drei: was git in models/ '
            + 'fuehrt, dass jede Gewichtsdatei ignoriert ist, und dass der Aufruf ohne Argument '
            + 'keinen Dateinamen enthaelt. Ueber den Inhalt eines fremden Arbeitsverzeichnisses '
            + 'sagt diese Zahl nichts.';
        log(`keine Gewichte im Repo: (a) ${partA} (b) ${partB} (c) ${partC}`);

        // ============================================================== AC8 ==
        // Eigenschaften des Codes, an der Funktion selbst gefahren.
        const bundle = await loadCompiler();
        const compiler = bundle.module;
        extras.compilerBundle = { inputs: bundle.inputs, entry: 'src/compiler/eval-entry.ts' };
        const CARDS = ['K1', 'K2', 'K3'];
        const cases = {
            multiLineTruncated: compiler.checkCitations(
                'createUser is called by create [K2].\nAnd it also touches the',
                CARDS,
                { truncated: true },
            ),
            singleLineTruncated: compiler.checkCitations(
                'createUser is called by the', CARDS, { truncated: true },
            ),
            singleLineComplete: compiler.checkCitations(
                'createUser is called by create [K2].', CARDS,
            ),
            singleLineCompleteUncited: compiler.checkCitations(
                'createUser is called by create.', CARDS,
            ),
        };
        report.singleLineTruncatedIsUnmeasured =
            cases.multiLineTruncated.measured === true
            && cases.multiLineTruncated.ok === true
            && cases.singleLineTruncated.measured === false
            && cases.singleLineTruncated.ok === false
            && cases.singleLineTruncated.violations.length === 0
            && cases.singleLineComplete.measured === true
            && cases.singleLineComplete.ok === true
            && cases.singleLineCompleteUncited.measured === true
            && cases.singleLineCompleteUncited.ok === false;
        extras.citationCases = {
            method:
                'Die Funktion selbst, im Node-Prozess, ueber dieselbe Bruecke, die auch die Eval '
                + 'benutzt (tools/lib/compiler-bundle.mjs baut src/compiler/eval-entry.ts mit '
                + 'esbuild und importiert sie). Kein Nachbau der Regel, sondern die Regel.',
            results: Object.fromEntries(Object.entries(cases).map(([name, check]) => [name, {
                measured: check.measured,
                ok: check.ok,
                cited: check.cited,
                violations: check.violations.length,
            }])),
        };
        log(`Zitatpruefung: einzeilig gekuerzt gemessen=${cases.singleLineTruncated.measured}, `
            + `mehrzeilig gekuerzt gemessen=${cases.multiLineTruncated.measured}`);

        /*
         * Und die Rechnung der Eval, an ihrem eigenen Quelltext gefahren.
         *
         * Ein vollstaendiger Eval-Lauf waere sechs Modelle und Stunden und
         * wuerde verification/w5/eval.json ueberschreiben, also die Aufzeichnung
         * loeschen, gegen die dieser Lauf oben die Vorschlaege haelt. Statt die
         * Formel nachzuschreiben (das waere eine zweite Wahrheit) werden ihre
         * Zeilen WOERTLICH aus tools/eval-llm.mjs geschnitten und mit
         * Pruefergebnissen aus der echten Funktion oben gefahren. Was diese Zahl
         * deckt: die Rechnung. Was sie nicht deckt: einen ganzen Lauf.
         */
        const evalSource = await readFile(join(ROOT, 'tools', 'eval-llm.mjs'), 'utf8');
        const filterLine =
            'const measuredResults = results.filter((entry) => entry.citationMeasured === true);';
        const unmeasuredLine = 'const unmeasured = results.length - measuredResults.length;';
        const rateStart = evalSource.indexOf('citationCompliance: measuredResults.length === 0');
        const rateEnd = evalSource.indexOf('citationUnmeasured: unmeasured,');
        let aggregation = null;
        if (evalSource.includes(filterLine) && evalSource.includes(unmeasuredLine)
            && rateStart > 0 && rateEnd > rateStart) {
            const rateBlock = evalSource.slice(rateStart, rateEnd + 'citationUnmeasured: unmeasured,'.length);
            const lineOf = (needle) => evalSource.slice(0, evalSource.indexOf(needle)).split('\n').length;
            const body = `${filterLine}\n${unmeasuredLine}\nreturn { ${rateBlock} };`;
            // eslint-disable-next-line no-new-func
            const aggregate = new Function('results', 'round', body);
            const round = (value) => Math.round(value * 1000) / 1000;
            const input = [
                { citationMeasured: cases.singleLineComplete.measured, citationOk: cases.singleLineComplete.ok },
                { citationMeasured: cases.singleLineCompleteUncited.measured, citationOk: cases.singleLineCompleteUncited.ok },
                { citationMeasured: cases.singleLineTruncated.measured, citationOk: cases.singleLineTruncated.ok },
            ];
            const produced = aggregate(input, round);
            aggregation = {
                takenFrom: {
                    file: 'tools/eval-llm.mjs',
                    filterLine: lineOf(filterLine),
                    unmeasuredLine: lineOf(unmeasuredLine),
                    rateBlockFromLine: lineOf('citationCompliance: measuredResults.length === 0'),
                    text: body,
                },
                input,
                produced,
                ifUnmeasuredCountedAsClean: round(2 / 3),
                ifUnmeasuredCountedAsViolation: round(1 / 3),
                note:
                    'Drei Pruefungen aus der echten Funktion: eine sauber, eine mit Verstoss, eine '
                    + 'nicht gemessen. Die Rechnung der Eval liefert 0.5 (eine von zwei gemessenen). '
                    + 'Waere die ungemessene als sauber mitgezaehlt, staende 0.667; waere sie als '
                    + 'Verstoss mitgezaehlt, staende 0.333.',
            };
            report.unmeasuredOutOfCitationRate =
                produced.citationCompliance === 0.5
                && produced.citationMeasuredAnswers === 2
                && produced.citationUnmeasured === 1;
            report.evalReportsUnmeasured =
                Object.prototype.hasOwnProperty.call(produced, 'citationUnmeasured')
                && produced.citationUnmeasured === 1
                && /citationUnmeasured: model\.citationUnmeasured/.test(evalSource);
        } else {
            aggregation = {
                error:
                    'Die Rechnung der Eval war in tools/eval-llm.mjs nicht an den erwarteten Zeilen '
                    + 'zu finden. Ohne sie ist nichts gefahren worden, also steht hier false.',
            };
        }
        extras.evalAggregation = {
            method:
                'Die Zeilen der Eval-Rechnung woertlich aus tools/eval-llm.mjs geschnitten und mit '
                + 'echten Pruefergebnissen gefahren. Ein vollstaendiger Eval-Lauf war nicht moeglich: '
                + 'er braucht sechs Modelle und Stunden und wuerde verification/w5/eval.json '
                + 'ueberschreiben, also die Aufzeichnung loeschen, gegen die dieser Lauf die '
                + 'Vorschlaege im Panel haelt. Gedeckt ist damit die Rechnung, nicht ein ganzer Lauf.',
            ...aggregation,
        };
        log(`Eval-Rechnung: Zitattreue ${aggregation?.produced?.citationCompliance}, `
            + `nicht gemessen ${aggregation?.produced?.citationUnmeasured}`);
        await rm(BUNDLE_DIR, { recursive: true, force: true });

        // ================================================== AC2, zweite Haelfte
        // Erst den Router samt Kindern beenden, dann den Einzel-Sidecar starten:
        // zwei Modelle im Speicher fuer eine Messung, die sie nicht braucht,
        // waeren zwei zu viel.
        routerKids = await sidecarChildren(routerChild.pid);
        for (const kid of routerKids) {
            for (const port of kid.ports) {
                childPorts.add(port);
            }
        }
        extras.routerChildren = routerKids;
        log(`Router-Kindprozesse: ${JSON.stringify(routerKids)}`);
        const routerSurvivors = await stopSidecar(routerChild, routerKids);
        routerChild = null;
        extras.routerStop = { killedHard: routerSurvivors };

        singlePort = await findFreePort(MIN_PORT, [serverPort, uiPort, routerPort]);
        const singleOrigin = `http://127.0.0.1:${singlePort}`;
        const singleLog = [];
        const single = await startSidecar(
            [
                '--host', '127.0.0.1', '--port', String(singlePort),
                '-m', join(ROOT, 'models', MODEL_A),
                '-c', String(ROUTER_CTX),
            ],
            { port: singlePort, log: singleLog },
        );
        singleChild = single.child;
        log(`Einzel-Sidecar (ohne --models-dir) auf ${singlePort}, bereit nach ${single.readyMs} ms`);

        const singleContext = await browser.newContext({ viewport: { ...MAIN_VIEWPORT } });
        await singleContext.addInitScript(({ from, to }) => {
            const original = globalThis.fetch.bind(globalThis);
            globalThis.fetch = (input, init) => {
                const url = typeof input === 'string'
                    ? input
                    : input instanceof URL ? input.href : input?.url ?? '';
                if (typeof url === 'string' && url.startsWith(from)) {
                    return original(to + url.slice(from.length), init);
                }
                return original(input, init);
            };
        }, { from: PRODUCT_SIDECAR_ORIGIN, to: singleOrigin });
        await singleContext.route('**/*', routeFor(origin, singleOrigin));
        const singlePage = await singleContext.newPage();
        singlePage.on('console', (message) => {
            if (message.type() === 'error') {
                extras.consoleErrors.push(`[ohne router] ${message.text()}`);
            }
        });
        singlePage.on('pageerror', (error) => extras.pageErrors.push(`[ohne router] ${String(error)}`));
        await openApp(singlePage, origin);
        await singlePage.click('[data-menu="a-llm"]');
        await waitForLlm(singlePage, 'ready', READY_TIMEOUT_MS);
        await singlePage.waitForTimeout(1200);
        await singlePage.click('[data-menu="a-settings"]');
        await singlePage.waitForSelector('[data-testid="atlas-settings"]', { timeout: 15000 });
        await singlePage.waitForTimeout(600);
        const singleDom = await settingsDom(singlePage);
        const singleSeam = await settingsSeam(singlePage);
        report.noRouterExplained =
            singleDom.router === 'false'
            && singleSeam?.router === false
            && singleDom.noRouter.present === true
            && singleDom.noRouter.text.length > 80
            && singleDom.routerCommand.present === true
            && singleDom.routerCommand.text.includes('start.sh')
            && singleDom.modelPicks === 0
            && singleDom.models.every((row) => row.selectable === 'false');
        extras.withoutRouter = {
            port: singlePort,
            arguments: `-m models/${MODEL_A} -c ${ROUTER_CTX}`,
            panelRouterAttribute: singleDom.router,
            seamRouter: singleSeam?.router ?? null,
            message: singleDom.noRouter.text,
            command: singleDom.routerCommand.text,
            modelRows: singleDom.models,
            clickableModelRows: singleDom.modelPicks,
            state: singleDom.llmState,
        };
        log(`ohne Router: Meldung da ${singleDom.noRouter.present}, Befehl `
            + `"${singleDom.routerCommand.text}", anklickbare Modellzeilen ${singleDom.modelPicks}`);
        await singleContext.close();
    } catch (err) {
        failure = err;
        console.error('[smoke-w10] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w10] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
    }

    // ------------------------------------------------------------- Abbau ----
    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopServer(serverChild);
    if (routerChild !== null) {
        routerKids = await sidecarChildren(routerChild.pid).catch(() => []);
        for (const kid of routerKids) {
            for (const port of kid.ports) {
                childPorts.add(port);
            }
        }
        await stopSidecar(routerChild, routerKids).catch(() => []);
    }
    if (singleChild !== null) {
        singleKids = await sidecarChildren(singleChild.pid).catch(() => []);
        for (const kid of singleKids) {
            for (const port of kid.ports) {
                childPorts.add(port);
            }
        }
        extras.singleChildren = singleKids;
        await stopSidecar(singleChild, singleKids).catch(() => []);
    }
    await sleep(900);

    const leftovers = [];
    const ports = [serverPort, uiPort, routerPort, singlePort, ...childPorts]
        .filter((value) => value > 0);
    for (const port of [...new Set(ports)]) {
        leftovers.push({ port, listeners: await countListeners(port) });
    }
    extras.leftovers = leftovers;
    report.leftoverProcesses = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
    /*
     * Der Port des Nutzers wird zur Kenntnis genommen und nicht gezaehlt: dieser
     * Lauf hat ihn nicht belegt und beendet ihn nicht.
     */
    extras.userPortsUntouched = {
        note: 'Nur beobachtet. Dieser Lauf hat auf keinem dieser Ports etwas gestartet oder beendet.',
        ports: [],
    };
    for (const port of [PRODUCT_SIDECAR_PORT, 4390, 4391]) {
        extras.userPortsUntouched.ports.push({ port, listeners: await countListeners(port) });
    }
    log('leftoverProcesses:', report.leftoverProcesses, JSON.stringify(leftovers));

    // Die harten Links loeschen, die Modelle nicht. Und danach nachzaehlen.
    for (const directory of [cacheDir, emptyDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true }).catch(() => undefined);
        }
    }
    const modelsAfter = [];
    for (const file of EXPECTED_MODEL_FILES) {
        try {
            const info = await stat(join(ROOT, 'models', file));
            modelsAfter.push({ file, bytes: info.size, links: info.nlink, present: true });
        } catch (error) {
            modelsAfter.push({ file, present: false, error: String(error).slice(0, 160) });
        }
    }
    extras.modelsAfterRun = {
        claim: 'Alle sechs Dateien in models/ sind nach dem Lauf noch da. Ein harter Link haelt '
            + 'dieselbe Inode; sein Loeschen nimmt der Datei nur einen Verweis.',
        allPresent: modelsAfter.every((entry) => entry.present),
        files: modelsAfter,
    };
    log(`Modelle in models/ nach dem Lauf: `
        + `${modelsAfter.filter((entry) => entry.present).length} von ${EXPECTED_MODEL_FILES.length}`);

    /*
     * Der Mitschnitt der Sidecar-Anfragen, zusammengefasst.
     *
     * Eine Probe alle drei Sekunden mit vier Anfragen ergibt ueber einen Lauf
     * dieser Laenge einige tausend Zeilen, und ein Artefakt, in dem die Zahlen
     * unter dem Mitschnitt verschwinden, ist keins. Aufgehoben wird, worauf es
     * ankommt: die Summe, die Aufteilung nach Route, und die ersten und letzten
     * zwanzig samt Zeitstempel. Die Null, die AC5 traegt, ist ohnehin an
     * `extras.llmOff.sidecarRequests` gemessen und steht dort unveraendert.
     */
    const byPath = {};
    for (const entry of extras.sidecarRequests) {
        const path = entry.url.replace(/^https?:\/\/[^/]+/, '').split('?')[0];
        byPath[path] = (byPath[path] ?? 0) + 1;
    }
    extras.sidecarRequests = {
        total: extras.sidecarRequests.length,
        byPath,
        first: extras.sidecarRequests.slice(0, 20),
        last: extras.sidecarRequests.slice(-20),
    };

    timings.totalMs = Date.now() - totalStarted;
    await mkdir(OUT_DIR, { recursive: true });
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            method:
                'Ein eigener llama-server im Router-Modus ueber ein eigenes Cache-Verzeichnis mit '
                + 'ZWEI Modellen, die als harte Links aus models/ dort liegen: kein Download, kein '
                + 'zusaetzlicher Platz, keine geloeschte Datei. Die Anwendung fragt weiter ihren '
                + 'Produktport 127.0.0.1:4141; ein addInitScript leitet an genau der Naht um, an der '
                + 'sie selbst fragt, und der Route-Handler bricht jede Anfrage an 4141 ab, statt sie '
                + 'durchzulassen (productPortRequests muss 0 sein). Die Modellnamen, die das '
                + 'Umschalten belegen, stammen aus den aufgezeichneten Antwortkoerpern der Anfragen, '
                + 'die die Anwendung selbst geschickt hat. '
                + FRAME_RATE_ENVIRONMENT,
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras,
        }, null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', OUT_JSON);

    /*
     * Vier Bilder, und alle vier muessen da und groesser als 30 KB sein. Der
     * eingefrorene Test prueft die ersten drei; das vierte prueft dieser Lauf
     * selbst, damit es nicht stillschweigend fehlen kann.
     */
    const shotsOk = [SHOT_SETTINGS, SHOT_SWITCH, SHOT_FETCH, SHOT_PERFORMANCE]
        .every((file) => existsSync(file) && statSync(file).size > 30 * 1024);
    const ok =
        failure === null
        && report.settingsPanelOpens === true
        && report.runningModelFromProps === true
        && report.cacheModelsListed >= 2
        && report.switchModelWorks === true
        && report.switchPersistsReload === true
        && report.statusBarNamesModel === true
        && report.noRouterExplained === true
        && report.suggestionsListed >= 6
        && report.suggestionsShowMeasuredNumbers === true
        && report.freeRepoFieldAccepted === true
        && report.commandCopyable === true
        && report.downloadHonestyText === true
        && report.noFakeProgressBar === true
        && report.startScriptTakesModel === true
        && report.startScriptSaysHowToFetch === true
        && report.noWeightsInRepo === true
        && report.llmOffMakesNoRequests === true
        && report.panelExplainsItselfWhileOff === true
        && report.singleLineTruncatedIsUnmeasured === true
        && report.unmeasuredOutOfCitationRate === true
        && report.evalReportsUnmeasured === true
        && report.panelShowsUnmeasured === true
        && report.twoDimensionalMode === true
        && Array.isArray(report.effectToggles) && report.effectToggles.length >= 3
        && report.thriftProfileWorks === true
        && report.frameCapWorks === true
        && report.settingsPersistReload === true
        && report.everyToggleNamesMeasuredEffect === true
        && report.noEffectSaysSo === true
        && report.viewOnlyControlsStayed === true
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.cutWithoutHint === 0
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && shotsOk
        && extras.productPortRequests.length === 0
        && extras.blockedRequests.length === 0
        && extras.pageErrors.length === 0
        && extras.modelsAfterRun.allPresent === true;

    if (!ok) {
        console.error('[smoke-w10] W10-Smoke NICHT gruen.');
        if (home) {
            console.error('[smoke-w10] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W10-Smoke gruen.');
}

main().catch((err) => {
    console.error('[smoke-w10] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
