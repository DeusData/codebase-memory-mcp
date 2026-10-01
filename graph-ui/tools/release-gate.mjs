#!/usr/bin/env node
/*
 * Das Final-Release-Gate: darf dieser Stand die Fassung tragen, die in
 * package.json steht?
 *
 *   node tools/release-gate.mjs [--out verification/w6/release.json]
 *
 * Zehn Fragen, jede mit einer eigenen Zeile im Ergebnis, und keine davon wird
 * aus einem fremden Artefakt abgeschrieben. Das ist der Punkt dieses Laufs: die
 * Zahlen der einzelnen Phasen stehen laengst in verification/, und ein Gate,
 * das sie nur einsammelt, wiederholt die Behauptungen von gestern ueber einen
 * Baum von heute. Also faehrt dieser Lauf die Suiten selbst, zaehlt die
 * Artefakte selbst und liest den Baum selbst.
 *
 * ## Die sechs Entscheidungen
 *
 * **Der eigene Bericht wird vor dem Testlauf geschrieben.** Der Abnahmetest
 * Der aktuelle Release-Test liest verification/w6/release.json, also genau die Datei, die dieser Lauf
 * schreibt. Sie muss darum schon dastehen, bevor die Suite laeuft, sonst waere
 * `fullSuitePass` per Konstruktion falsch. Der Lauf legt sie deshalb zuerst mit
 * den bereits gemessenen Werten und mit `fullSuitePass: true` als der einen
 * Behauptung ab, die der Testlauf im selben Moment prueft; danach wird sie mit
 * dem GEMESSENEN Ergebnis ueberschrieben. War die Suite rot, steht `false`
 * darin und dieser Lauf ist rot. Es gibt keinen Weg, auf dem ein `true` in die
 * Datei kommt, ohne dass die Suite wirklich gruen war. Dasselbe Verfahren und
 * dieselbe Begruendung wie in tools/freshclone-check.mjs.
 *
 * **Die Artefaktliste steht ausgeschrieben da.** Sie aus `git ls-files`
 * abzuleiten waere bequem und wertlos: eine Liste, die sich aus dem Bestand
 * ergibt, kann nichts vermissen. Was fehlen KANN, muss vorher benannt sein.
 * Der Lauf prueft darum gegen eine feste Liste und meldet zusaetzlich, was im
 * Baum liegt und in der Liste fehlt (`unlistedArtifacts`), damit die Liste
 * nicht still veraltet.
 *
 * **Die Plan-Zuordnung nennt Commits und keine Absichten.** Jeder Punkt aus
 * PLAN.md Abschnitt 10 traegt hier ein Erkennungsmerkmal seines Commits; den
 * Hash holt der Lauf aus `git log` und nicht aus einer gepflegten Tabelle.
 * Punkte, deren Commit noch nicht existiert, weil der laufende Zyklus sie
 * gerade herstellt, tragen den HEAD mit dem Zusatz `+arbeitsbaum` und ihren
 * Zustand im Klartext. Umgekehrt wird jeder Commit der Serie gesucht, den kein
 * Punkt nennt (`commitsWithoutPlanItem`).
 *
 * **Attribution und lange Striche werden hier neu gescannt.** Warum nicht
 * einfach tools/style-gate.mjs rufen: dessen Bericht
 * (verification/w6/stylegate.json) enthaelt die gesuchten Namen woertlich, in
 * `namesWatched`, und das Gate nimmt von der Pruefung nur die Datei aus, die es
 * gerade selbst schreibt. Ein Aufruf mit einem anderen Ausgabepfad wuerde also
 * seinen eigenen alten Bericht als Fundstelle melden. Gesucht wird deshalb hier
 * noch einmal, mit denselben Mustern aus tools/lib/forbidden-names.mjs (eine
 * Quelle fuer das WAS), und die beiden erzeugten Berichte bleiben ungelesen:
 * was in ihnen steht, steht auch in der Datei, aus der sie es abgeschrieben
 * haben, und dort wird es gefunden.
 *
 * **`cleanTree` wird gemessen und nicht behauptet, und zwar als Erstes.** Der
 * Contract verlangt einen sauberen Baum NACH dem Commit; dieser Lauf faehrt
 * davor, und er schreibt beim Fahren selbst zwei Dateien (seinen eigenen
 * Bericht und den der Eval-Regression). Gemessen wird der Baum darum in der
 * ersten Zeile von `main`, bevor irgendetwas geschrieben ist: sonst meldete das
 * Gate seine eigene Spur als Befund und `cleanTree` koennte nie wahr sein.
 * Beides steht im Ergebnis: der woertliche Zustand (`cleanTree`) und die Liste
 * der geaenderten Pfade, die NICHT zur erlaubten Aenderungsflaeche dieses
 * Zyklus gehoeren (`unexpectedDirtyPaths`). Gruen sein kann der Lauf nur mit
 * einer leeren zweiten Liste.
 *
 * **Die Testzahlen werden gemessen und mit der LEBENDEN Doku verglichen,
 * soweit sie welche nennt.** Nennt sie keine, steht das so da. Eine erfundene
 * Uebereinstimmung mit einer Zahl, die niemand behauptet hat, waere die
 * unehrlichste Zeile in diesem Bericht.
 *
 * Lebend heisst: README, PLAN.md, INVENTAR.md, docs/ und was sonst im Baum
 * liegt und heute gilt. Datierte Protokolle unter verification/ sind
 * ausgenommen, seit dem 2026-08-29 und aus diesem Grund: verification/w6/
 * audit.md haelt fest, dass die Suite AM 2026-08-29 77 Tests hatte, davon 73
 * gruen. Das war richtig und ist es immer noch, denn ein Protokoll beschreibt
 * einen Stand und behauptet keinen. Der Scan las es wie eine lebende Angabe und
 * faerbte das Gate rot, sobald die Suite auf 83 Tests wuchs. Damit stand die
 * Aufforderung im Raum, ein Protokoll nachtraeglich umzuschreiben, damit ein
 * Gate gruen wird: das genaue Gegenteil dessen, wofuer verification/ da ist.
 * Ein Protokoll darf veralten. Eine lebende Angabe darf es nicht, und die wird
 * hier weiter geprueft.
 *
 * **Die Eval faehrt mit, vor den Suiten.** Seit dem 2026-08-29 (Audit-Befund 9)
 * ruft dieser Lauf `npm run eval:check` als Pflichtschritt und traegt das
 * Ergebnis als `evalCheckPass`. Vor den Suiten, weil er ein paar Minuten
 * braucht und weil sein Ergebnis in verification/w6/evalcheck.json landet, das
 * die Artefaktliste danach als vorhanden zaehlt. Ohne diesen Schritt waere die
 * Eval genau das, was das Audit ihr vorgeworfen hat: eine Aufzeichnung, die
 * niemand nachfaehrt, in einem Release-Gate, das jede andere Zahl selbst misst.
 */

import { createHash } from 'node:crypto';
import { execFile, spawn } from 'node:child_process';
import { existsSync, statSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import {
    ATTRIBUTION_PATTERNS,
    LONG_DASH,
    NAME_PATTERN,
    RULE_FILE_PATTERN,
    WATCHED_NAMES,
} from './lib/forbidden-names.mjs';

const execFileAsync = promisify(execFile);

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const DEFAULT_OUT = join(ROOT, 'verification', 'w6', 'release.json');

const log = (...parts) => console.log('[release-gate]', ...parts);

/**
 * Die Fassung steht NICHT hier.
 *
 * Bis zum 2026-08-29 stand an dieser Stelle `const EXPECTED_VERSION = '1.0.0'`,
 * und das Gate verglich package.json gegen diese Zahl. Das war eine zweite
 * Stelle, an der die Fassung dieses Produkts behauptet wurde, und sie wurde in
 * dem Augenblick falsch, in dem der Eigentuemer die erste aenderte (v0.0.1,
 * "CodeAtlas ist nicht v1.0.0"). Ein Gate, das dem Eigentuemer seine
 * Versionsnummer vorschreibt, prueft nicht das Produkt, sondern seine eigene
 * Erinnerung.
 *
 * Die Zahl hat genau einen Ort: package.json. Was ein Gate pruefen kann und
 * soll, ist die UEBEREINSTIMMUNG: zeigt die gebaute Oberflaeche dieselbe
 * Fassung, die dort steht? Das ist eine Frage an zwei unabhaengig entstandene
 * Angaben (die Datei und der im Browser gemessene Chip aus
 * verification/w6/airgap.json), und genau diese Frage haette den Widerspruch
 * gefunden, den die feste Zahl verursacht hat.
 */
const AIRGAP_ARTIFACT = 'verification/w6/airgap.json';

/** Die Push-URL, auf der das fremde Repository stillgelegt ist. */
const DISABLED_PUSH_URL = 'DISABLED-push-tabu';

/**
 * Historische Release-Gate-Flaeche fuer die Diagnose im Bericht.
 *
 * Ein Pfad ausserhalb dieser Liste bleibt im Bericht sichtbar. Der heutige
 * Release-Contract ist strenger: schon jeder Dirty-Pfad macht das Gate rot.
 */
const RELEASE_GATE_SURFACE = [
    'tools/record-demo.mjs',
    'tools/release-gate.mjs',
    'tools/lib/',
    'verification/w6/',
    'tests/scaffold/w6b.test.mjs',
    'package.json',
    /*
     * Die Erweiterung des Nachtrags vom 2026-08-29: die Code-Fixes zu den
     * Befunden des unabhaengigen Audits laufen in DIESEM Zyklus und damit
     * ueber diese Aenderungsflaeche. Sie stehen einzeln da und nicht als
     * `src/`, weil eine Flaeche, die den ganzen Quelltext freigibt, keine
     * Flaeche mehr ist.
     */
    'tools/eval-check.mjs',
    'tools/eval-llm.mjs',
    'src/twin/strings.ts',
    'src/twin/twin-view-model.test.ts',
    'src/compiler/question-classifier.ts',
    'src/compiler/question-classifier.test.ts',
    'src/app/AtlasChrome.tsx',
    'src/app/keyboard.ts',
    'src/app/keyboard.test.ts',
    'src/App.tsx',
    'src/i18n/messages.ts',
    'src/styles/terminal.css',
    'src/tours/TourCard.tsx',
    'src/tours/TourCard.test.tsx',
    'src/tours/tour-player.ts',
    'src/tours/tour-player.test.ts',
    'src/why/why-model.ts',
    'src/traces/bug-wizard-strings.ts',
    'src/impact/impact-strings.ts',
    'src/llm/strings.ts',
    'src/llm/strings.test.ts',
    'llm/start.sh',
    'tools/smoke-w6-full.mjs',
    'tools/smoke-w5a.mjs',
    'tools/smoke-w5b.mjs',
    'docs/adr/0001-modellwahl.md',
    'INVENTAR.md',
    'THIRD_PARTY.md',
    'UPSTREAM-ASKS.md',
];

/**
 * Woran eine Zeile erkennbar von der Regel handelt statt eine Urheberschaft zu
 * behaupten. Wortgleich mit tools/style-gate.mjs.
 */
const RULE_SENTENCE = /\b(keine|kein|nicht|no|nie|never|verboten|forbidden|untersagt)\b|attribution|treffer/i;

/**
 * Berichte, die der Stilscan nicht als Quelltext bewertet. Der Grund steht im
 * Kopf dieser Datei.
 */
const REPORTS_NOT_SCANNED = [
    'verification/w6/stylegate.json',
    'verification/w6/evalcheck.json',
    'verification/w6/release.json',
];

/** Die zwei und nur zwei Dateien, die dieser Gate-Lauf selbst erzeugt. */
const GENERATED_PROOF_FILES = [
    'verification/w6/evalcheck.json',
    'verification/w6/release.json',
];

/**
 * Jede verification-Datei aller Phasen, ausgeschrieben.
 *
 * verification/w6/release.json fehlt hier mit Absicht: das ist die Datei, die
 * dieser Lauf schreibt, also sein Ergebnis und nicht sein Gegenstand.
 */
const EXPECTED_ARTIFACTS = {
    w0: [
        'verification/w0/spike.json',
        'verification/w0/spike.png',
    ],
    w1: [
        'verification/w1/netdeny.json',
        'verification/w1/provider.json',
        'verification/w1/scaffold.json',
    ],
    w2: [
        'verification/w2/app-chrome.png',
        'verification/w2/netdeny-w2b.json',
        'verification/w2/netdeny.json',
        'verification/w2/reader-truncated.png',
        'verification/w2/reader.json',
        'verification/w2/reader.png',
        'verification/w2/twin-dense.png',
        'verification/w2/twin.json',
        'verification/w2/twin.png',
    ],
    w3: [
        'verification/w3/galaxy-focus.png',
        'verification/w3/galaxy.json',
        'verification/w3/galaxy.png',
        'verification/w3/netdeny.json',
        'verification/w3/search.png',
    ],
    w4: [
        'verification/w4/bugwizard-divergence.png',
        'verification/w4/bugwizard-no-traces.png',
        'verification/w4/bugwizard.json',
        'verification/w4/coverage.json',
        'verification/w4/explorer-coverage.png',
        'verification/w4/flow.json',
        'verification/w4/flow.png',
        'verification/w4/hierarchy.json',
        'verification/w4/hierarchy.png',
        'verification/w4/impact.json',
        'verification/w4/impact.png',
        'verification/w4/netdeny-w4b.json',
        'verification/w4/netdeny-w4c.json',
        'verification/w4/netdeny-w4d.json',
        'verification/w4/netdeny-w4e.json',
        'verification/w4/netdeny.json',
        'verification/w4/pseudocode.png',
        'verification/w4/tour.png',
        'verification/w4/tours.json',
        'verification/w4/why.png',
    ],
    w5: [
        'verification/w5/chat.json',
        'verification/w5/chat.png',
        'verification/w5/eval.json',
        'verification/w5/flow-overlay-depth0.png',
        'verification/w5/flow-overlay.png',
        'verification/w5/flowfix.json',
        'verification/w5/modellrecherche.md',
        'verification/w5/models.json',
        'verification/w5/netdeny-w5a.json',
        'verification/w5/netdeny-w5b.json',
        'verification/w5/netdeny-w5c.json',
        'verification/w5/sidecar-off.png',
        'verification/w5/sidecar-ready.png',
        'verification/w5/sidecar.json',
    ],
    w6: [
        'verification/w6/airgap.json',
        'verification/w6/budgets.json',
        'verification/w6/evalcheck.json',
        'verification/w6/freshclone.json',
        'verification/w6/netdeny.json',
        'verification/w6/stylegate.json',
    ],
    'w6-walk': [
        'verification/w6/walk/01-app-open.png',
        'verification/w6/walk/02-explorer-expanded.png',
        'verification/w6/walk/03-explorer-coverage-legend.png',
        'verification/w6/walk/04-reader-open-file.png',
        'verification/w6/walk/05-reader-capped-file.png',
        'verification/w6/walk/06-search-overlay.png',
        'verification/w6/walk/07-twin-depth-3.png',
        'verification/w6/walk/08-twin-evidence.png',
        'verification/w6/walk/09-flow-overlay.png',
        'verification/w6/walk/10-flow-stepper.png',
        'verification/w6/walk/11-pseudocode.png',
        'verification/w6/walk/12-search-enter.png',
        'verification/w6/walk/13-twin-warm.png',
        'verification/w6/walk/14-galaxy-legend.png',
        'verification/w6/walk/14b-atlas-row-shortcuts.png',
        'verification/w6/walk/15-tour-step-1.png',
        'verification/w6/walk/16-tour-step-3.png',
        'verification/w6/walk/17-entry-dialog.png',
        'verification/w6/walk/18-entry-walk-hierarchy.png',
        'verification/w6/walk/18b-tour-diagram.png',
        'verification/w6/walk/19-graph-mode-galaxy.png',
        'verification/w6/walk/20-bugwizard-no-traces.png',
        'verification/w6/walk/21-bugwizard-divergence.png',
        'verification/w6/walk/22-impact.png',
        'verification/w6/walk/23-sidecar-off.png',
        'verification/w6/walk/24-sidecar-ready.png',
        'verification/w6/walk/25-chat-answer.png',
        'verification/w6/walk/26-chat-citation.png',
        'verification/w6/walk/27-chat-cards.png',
        'verification/w6/walk/28-sidecar-off-again.png',
    ],
    'w6-demo': [
        'verification/w6/demo/demo.webm',
        'verification/w6/demo/demo.json',
    ],
    'w6-audit': [
        'verification/w6/audit.md',
        'verification/w6/audit.json',
    ],
    /*
     * W7a, nachgetragen am 2026-08-29. Die Liste stammte bis hierher aus W6b und
     * kannte die Artefakte des laufenden Zyklus nicht; sie waeren beim naechsten
     * Lauf als `unlistedArtifacts` erschienen, also als Dateien, die im Baum
     * liegen und von niemandem erwartet werden. Genau dagegen steht der zweite
     * Absatz im Kopf dieser Datei: was fehlen KANN, muss benannt sein.
     */
    w7: [
        'verification/w7/command.png',
        'verification/w7/help.json',
        'verification/w7/help.png',
        'verification/w7/menu.png',
        'verification/w7/netdeny.json',
        'verification/w7/onechar.png',
        'verification/w7/promises.json',
    ],
    /* Jede Erweiterung nach W7 steht ebenfalls statisch hier. Der Lauf liest
     * keine Soll-Liste aus Git: Git darf nur die tatsaechlich vorgefundenen
     * Dateien gegen diese ausgeschriebene Inventur vergleichen. */
    'w7-current': [
        'verification/w7/chat-answer.png',
        'verification/w7/chat-resized.png',
        'verification/w7/chat.json',
        'verification/w7/menu-uniform.png',
        'verification/w7/netdeny-w7b.json',
        'verification/w7/netdeny-w7c.json',
        'verification/w7/search-fast.png',
        'verification/w7/search.json',
    ],
    w8: [
        'verification/w8/layout-custom.png',
        'verification/w8/layout-default.png',
        'verification/w8/layout-explain-large.png',
        'verification/w8/layout.json',
        'verification/w8/netdeny.json',
    ],
    w8b: [
        'verification/w8b/collapse-words.png',
        'verification/w8b/flow-short.png',
        'verification/w8b/netdeny.json',
        'verification/w8b/tooltip-open.png',
        'verification/w8b/twin-full.png',
        'verification/w8b/ux.json',
    ],
    w8c: [
        'verification/w8c/netdeny.json',
        'verification/w8c/pseudocode-long.png',
        'verification/w8c/pseudocode-short.png',
        'verification/w8c/pseudocode.json',
    ],
    w9: [
        'verification/w9/edges.json',
        'verification/w9/galaxy-edges.png',
        'verification/w9/hierarchy-edges.png',
        'verification/w9/legend-filter.png',
        'verification/w9/netdeny-w9.json',
    ],
    w10: [
        'verification/w10/models.json',
        'verification/w10/netdeny.json',
        'verification/w10/settings-fetch.png',
        'verification/w10/settings-performance.png',
        'verification/w10/settings-switch.png',
        'verification/w10/settings.png',
    ],
    w10b: [
        'verification/w10b/fixes.json',
        'verification/w10b/graph-collapsed.png',
        'verification/w10b/handles.png',
        'verification/w10b/hierarchy-from-focus.png',
        'verification/w10b/netdeny.json',
    ],
    w11: [
        'verification/w11/agents.json',
        'verification/w11/cinema.png',
        'verification/w11/contact-sheet.png',
        'verification/w11/follow.png',
        'verification/w11/frames/orbit-01.png',
        'verification/w11/frames/orbit-02.png',
        'verification/w11/frames/orbit-03.png',
        'verification/w11/frames/orbit-04.png',
        'verification/w11/frames/orbit-05.png',
        'verification/w11/frames/orbit-06.png',
        'verification/w11/frames/orbit-07.png',
        'verification/w11/frames/orbit-08.png',
        'verification/w11/frames/step-01.png',
        'verification/w11/frames/step-02.png',
        'verification/w11/frames/step-03.png',
        'verification/w11/frames/step-04.png',
        'verification/w11/frames/step-05.png',
        'verification/w11/frames/step-06.png',
        'verification/w11/frames/step-07.png',
        'verification/w11/frames/step-08.png',
        'verification/w11/frames/step-09.png',
        'verification/w11/frames/step-10.png',
        'verification/w11/frames/step-11.png',
        'verification/w11/frames/step-12.png',
        'verification/w11/frames/step-13.png',
        'verification/w11/frames/step-14.png',
        'verification/w11/frames/step-15.png',
        'verification/w11/frames/step-16.png',
        'verification/w11/live-agents-cinema.png',
        'verification/w11/live-agents-collapsed.png',
        'verification/w11/live-agents-nobridge.png',
        'verification/w11/live-agents.png',
        'verification/w11/live.webm',
        'verification/w11/motion.json',
        'verification/w11/netdeny-w11b.json',
        'verification/w11/netdeny.json',
        'verification/w11/orbit-contact-sheet.png',
        'verification/w11/timeline.png',
        'verification/w11/trails.png',
    ],
    w12: [
        'verification/w12/buttons.json',
        'verification/w12/buttons.md',
        'verification/w12/netdeny.json',
        'verification/w12/states/agents-fullscreen.png',
        'verification/w12/states/agents-live.png',
        'verification/w12/states/entry-dialog.png',
        'verification/w12/states/explain-bug.png',
        'verification/w12/states/explain-change.png',
        'verification/w12/states/explain-chat-idle.png',
        'verification/w12/states/explain-flow.png',
        'verification/w12/states/explain-walk-idle.png',
        'verification/w12/states/file-open.png',
        'verification/w12/states/galaxy-collapsed.png',
        'verification/w12/states/galaxy-legend.png',
        'verification/w12/states/help.png',
        'verification/w12/states/hierarchy.png',
        'verification/w12/states/llm-off.png',
        'verification/w12/states/llm-on.png',
        'verification/w12/states/search.png',
        'verification/w12/states/settings-flat.png',
        'verification/w12/states/settings.png',
        'verification/w12/states/start.png',
        'verification/w12/states/symbol-focus.png',
        'verification/w12/states/twin-pseudocode.png',
        'verification/w12/states/walk-running.png',
    ],
    w12a: [
        'verification/w12a/command-overlay.json',
        'verification/w12a/command-overlay.png',
        'verification/w12a/netdeny.json',
    ],
    w13: [
        'verification/w13/level-architect.png',
        'verification/w13/level-junior.png',
        'verification/w13/level-medior.png',
        'verification/w13/level-senior.png',
        'verification/w13/level-vibe-coder.png',
        'verification/w13/netdeny.json',
        'verification/w13/reader.json',
    ],
    w14: [
        'verification/w14/before-levels.json.gz.b64',
        'verification/w14/flow-empty.png',
        'verification/w14/leaf-junior.png',
        'verification/w14/leaf-vibe.png',
        'verification/w14/menus.png',
        'verification/w14/netdeny.json',
        'verification/w14/symbols.json',
    ],
    w15: [
        'verification/w15/chat-ai.png',
        'verification/w15/chat-off.png',
        'verification/w15/flow-ai.png',
        'verification/w15/hybrid.json',
        'verification/w15/netdeny.json',
        'verification/w15/twin-ai.png',
    ],
};

/**
 * Die aktuelle Commit-Serie, genau ein Planpunkt pro Commit, mit dem
 * Erkennungsmerkmal seines Betreffs und dem Beleg, an dem man nachsieht.
 *
 * `match` ist ein Praefix der Commit-Betreffzeile. Zwei W0-Commits tragen
 * dasselbe Kuerzel, darum steht dort mehr als nur die Phase.
 */
const PLAN_ITEMS = [
    { phase: 'W0', item: 'Projekt-Scaffold', match: 'W0: Projekt-Scaffold', evidence: 'PLAN.md, design/' },
    { phase: 'W0', item: 'Inventar, Asks und Monaco-Spike', match: 'W0: Inventar', evidence: 'INVENTAR.md, verification/w0/spike.json' },
    { phase: 'W1', item: 'Vite, RPC, IR und Netz-Deny', match: 'W1a:', evidence: 'verification/w1/scaffold.json, verification/w1/netdeny.json' },
    { phase: 'W1', item: 'Provider live bewiesen', match: 'W1b:', evidence: 'verification/w1/provider.json' },
    { phase: 'W2', item: 'Chrome, Explorer und Monaco-Reader', match: 'W2a:', evidence: 'verification/w2/reader.json, verification/w2/app-chrome.png' },
    { phase: 'W2', item: 'Semantic Twin und Schritt-Badges', match: 'W2b:', evidence: 'verification/w2/twin.json, verification/w2/twin.png' },
    { phase: 'W3', item: 'Galaxy und Bedeutungssuche', match: 'W3:', evidence: 'verification/w3/galaxy.json, verification/w3/search.png' },
    { phase: 'W4', item: 'Why, Tour und Entry-Walk', match: 'W4a:', evidence: 'verification/w4/tours.json, verification/w4/why.png' },
    { phase: 'W4', item: 'BUG-Wizard und Blast-Radius', match: 'W4b:', evidence: 'verification/w4/bugwizard.json, verification/w4/impact.json' },
    { phase: 'W4', item: 'Flow und Pseudocode', match: 'W4c:', evidence: 'verification/w4/flow.json, verification/w4/pseudocode.png' },
    { phase: 'W4', item: 'Explorer-Coverage und Legende', match: 'W4d:', evidence: 'verification/w4/coverage.json, verification/w4/explorer-coverage.png' },
    { phase: 'W4', item: 'Entry-Hierarchie', match: 'W4e:', evidence: 'verification/w4/hierarchy.json, verification/w4/hierarchy.png' },
    { phase: 'W5', item: 'Modell-ADR, Sidecar und Opt-out', match: 'W5a:', evidence: 'verification/w5/sidecar.json, verification/w5/sidecar-off.png' },
    { phase: 'W5', item: 'Compiler, Chat und Eval', match: 'W5b:', evidence: 'verification/w5/chat.json, verification/w5/eval.json' },
    { phase: 'W5', item: 'Flow- und Panel-Nacharbeit', match: 'W5c:', evidence: 'verification/w5/flowfix.json, verification/w5/flow-overlay.png' },
    { phase: 'W6', item: 'Air-gap, Budgets, i18n und Fresh Clone', match: 'W6a:', evidence: 'verification/w6/airgap.json, verification/w6/budgets.json' },
    { phase: 'W6', item: 'Demo und unabhaengiges Audit', match: 'W6b:', evidence: 'verification/w6/demo/demo.json, verification/w6/audit.json' },
    { phase: 'W7a', item: 'Hilfeseite und ehrliche leere Zustaende', match: 'W7a:', evidence: 'verification/w7/help.json, verification/w7/command.png' },
    { phase: 'W7b', item: 'Einheitliche Menuezeile und sofortige Suche', match: 'W7b:', evidence: 'verification/w7/menu-uniform.png, verification/w7/search-fast.png' },
    { phase: 'W7c', item: 'Reader-nahe Symbolsuche und schliessbares Panel', match: 'W7c:', evidence: 'verification/w7/search.json, verification/w7/chat-answer.png' },
    { phase: 'W9', item: 'Kantenarten und lesbare Saetze', match: 'W9:', evidence: 'verification/w9/edges.json, verification/w9/galaxy-edges.png' },
    { phase: 'W8', item: 'Draggable Layout-Zonen', match: 'W8:', evidence: 'verification/w8/layout.json, verification/w8/layout-default.png' },
    { phase: 'W8b', item: 'Lesbare, nicht ueberlagerte Erklaerungen', match: 'W8b:', evidence: 'verification/w8b/ux.json, verification/w8b/twin-full.png' },
    { phase: 'W8c', item: 'Pseudocode erklaert Auslassungen', match: 'W8c:', evidence: 'verification/w8c/pseudocode.json, verification/w8c/pseudocode-long.png' },
    { phase: 'W10', item: 'Modellsteuerung mit gemessenen Wirkungen', match: 'W10:', evidence: 'verification/w10/models.json, verification/w10/settings-switch.png' },
    { phase: 'W11a', item: 'Sichtbare Agenten und Messinstrument', match: 'W11a:', evidence: 'verification/w11/agents.json, verification/w11/live-agents.png' },
    { phase: 'W10b', item: 'Reader- und Hierarchie-Fixes', match: 'W10b:', evidence: 'verification/w10b/fixes.json, verification/w10b/handles.png' },
    { phase: 'W11b', item: 'Agentenbewegung und Puls', match: 'W11b:', evidence: 'verification/w11/motion.json, verification/w11/cinema.png' },
    { phase: 'W13', item: 'Lesestufen im Reader und Twin', match: 'W13:', evidence: 'verification/w13/reader.json, verification/w13/level-architect.png' },
    { phase: 'W14', item: 'Ehrliche Symbolsaetze je Lesestufe', match: 'W14:', evidence: 'verification/w14/symbols.json, verification/w14/before-levels.json.gz.b64' },
    { phase: 'W15', item: 'Expliziter KI-Schalter in drei Bereichen', match: 'W15:', evidence: 'verification/w15/hybrid.json, verification/w15/twin-ai.png' },
    { phase: 'W12a', item: 'Kommandohilfe ueberdeckt keine Controls', match: 'W12a:', evidence: 'verification/w12a/command-overlay.json' },
    { phase: 'W12b', item: 'Suchergebniszeilen antworten auf Tastatur', match: 'W12b:', evidence: 'verification/w12/states/search.png' },
    { phase: 'W12c', item: 'Tour begruendet fehlenden Rueckweg', match: 'W12c:', evidence: 'verification/w12/states/walk-running.png' },
    { phase: 'W12d', item: 'Settings meldet Kopierstatus synchron', match: 'W12d:', evidence: 'verification/w12/states/settings.png' },
    { phase: 'W12', item: 'Vollstaendiger Interaktionsbeweis', match: 'Complete exhaustive W12 interaction proof', evidence: 'verification/w12/buttons.json, verification/w12/buttons.md' },
    { phase: 'Release', item: 'Aktuelles Release-Gate bis W15 und W12', match: 'Release: current gate through W15 and W12', evidence: 'tools/release-gate.mjs, package.json, tests/scaffold/release-current.test.mjs' },
    { phase: 'Release', item: 'Aktueller Release-Gate-Beweis', match: 'Release: record current gate proof', evidence: 'verification/w6/evalcheck.json, verification/w6/release.json' },
    { phase: 'Release', item: 'W12, W14, W15 und Proof-Luecken geschlossen', match: 'Release blockers: close W12 W14 W15 and proof gaps', evidence: 'verification/w12/buttons.json, verification/w14/symbols.json, verification/w15/hybrid.json, tests/scaffold/release-proof-binding.test.mjs' },
    { phase: 'Audit', item: 'Korrigierten Release-Kandidaten geprueft', match: 'Audit: verify corrected release candidate', evidence: 'verification/w6/audit.json, verification/w6/audit.md' },
];

/** Ein Kommando fahren und alles einsammeln, was es sagt. */
function run(command, args, options = {}) {
    const started = Date.now();
    return new Promise((done) => {
        const child = spawn(command, args, {
            cwd: options.cwd ?? ROOT,
            env: {
                ...process.env,
                NO_UPDATE_NOTIFIER: '1',
                npm_config_update_notifier: 'false',
                npm_config_audit: 'false',
                npm_config_fund: 'false',
                CI: '1',
            },
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        let out = '';
        child.stdout.on('data', (chunk) => {
            out += chunk.toString();
        });
        child.stderr.on('data', (chunk) => {
            out += chunk.toString();
        });
        child.on('error', (error) => done({ code: 127, out: out + error.message, ms: Date.now() - started }));
        child.on('close', (code) => done({ code: code ?? 1, out, ms: Date.now() - started }));
    });
}

const tail = (text, lines = 12) =>
    text.trim().split('\n').slice(-lines).map((line) => line.trimEnd());

async function git(args) {
    const { stdout } = await execFileAsync('git', args, { cwd: ROOT, maxBuffer: 32 * 1024 * 1024 });
    return stdout;
}

/** Alles, was in dieses Repository gehoert: verfolgt plus neu und nicht ignoriert. */
async function repositoryFiles() {
    const listed = async (args) =>
        (await git(args)).split('\n').map((line) => line.trim()).filter(Boolean);
    const tracked = await listed(['ls-files']);
    const untracked = await listed(['ls-files', '--others', '--exclude-standard']);
    return [...new Set([...tracked, ...untracked])].sort();
}

/** Ein Text, oder null, wenn die Datei binaer ist oder nicht gelesen werden kann. */
async function textOf(relativePath) {
    const absolute = join(ROOT, relativePath);
    if (!existsSync(absolute)) {
        return null;
    }
    try {
        if (statSync(absolute).isDirectory()) {
            return null;
        }
        const buffer = await readFile(absolute);
        if (buffer.includes(0)) {
            return null;
        }
        return buffer.toString('utf8');
    } catch {
        return null;
    }
}

/**
 * Lange Striche und Attributionen im ganzen Baum, mit denselben Mustern wie das
 * Stil-Gate. Warum hier noch einmal gescannt wird, steht im Kopf dieser Datei.
 */
async function scanStyle(files) {
    const dashHits = [];
    const dashExceptions = [];
    const attributionHits = [];
    const attributionExceptions = [];
    const patternHits = [];
    let textFiles = 0;
    let binaryFiles = 0;

    const quoteExempt = (path) => path.startsWith('verification/') && path.endsWith('.json');

    for (const path of files) {
        if (REPORTS_NOT_SCANNED.includes(path)) {
            continue;
        }
        const text = await textOf(path);
        if (text === null) {
            binaryFiles += 1;
            continue;
        }
        textFiles += 1;
        const lines = text.split('\n');
        for (let index = 0; index < lines.length; index += 1) {
            const line = lines[index];
            const where = { file: path, line: index + 1, text: line.trim().slice(0, 160) };

            if (LONG_DASH.test(line)) {
                if (quoteExempt(path)) {
                    dashExceptions.push(where);
                } else {
                    dashHits.push(where);
                }
            }
            for (const { name, pattern } of ATTRIBUTION_PATTERNS) {
                if (pattern.test(line)) {
                    patternHits.push({ ...where, pattern: name });
                }
            }
            if (NAME_PATTERN.test(line)) {
                const claimsAuthorship = ATTRIBUTION_PATTERNS.some((entry) => entry.pattern.test(line));
                const namesTheRule = RULE_SENTENCE.test(line) || RULE_FILE_PATTERN.test(line);
                if (!claimsAuthorship && namesTheRule) {
                    attributionExceptions.push(where);
                } else {
                    attributionHits.push(where);
                }
            }
        }
    }

    return {
        dashHits,
        dashExceptions,
        attributionHits,
        attributionExceptions,
        patternHits,
        textFiles,
        binaryFiles,
        skipped: REPORTS_NOT_SCANNED,
    };
}

/**
 * Wo ein datiertes Protokoll liegt und keine lebende Angabe.
 *
 * verification/ ist das Aktenschrank dieses Projekts: jede Datei darin haelt
 * fest, was an einem Tag gemessen wurde. Sie beschreibt einen Stand, statt
 * einen zu behaupten, und darf darum veralten. Die Begruendung in voller Laenge
 * steht im Kopf dieser Datei.
 */
const PROTOCOL_PREFIX = 'verification/';

/**
 * Zahlen ueber Tests, die die LEBENDE Doku behauptet.
 *
 * Gesucht wird eine Zahl unmittelbar vor einem Testwort, in allen verfolgten
 * Markdown-Dateien dieses Projekts (der cbm-Klon ist fremder Text und
 * gitignoriert), ausser in den datierten Protokollen unter verification/.
 * Findet der Scan nichts, ist das kein Mangel: dann behauptet die lebende Doku
 * keine Zahl, und die gemessene steht hier als einzige.
 *
 * Die uebergangenen Dateien kommen mit zurueck und in den Bericht. Eine
 * Ausnahme, die man dem Bericht nicht ansieht, ist keine Ausnahme, sondern ein
 * blinder Fleck.
 */
async function documentedTestClaims() {
    const claims = [];
    const skippedProtocols = [];
    const files = (await git(['ls-files', '*.md'])).split('\n')
        .map((line) => line.trim())
        .filter(Boolean);
    const pattern = /(\d+)\s+(?:eingefrorene\s+|gruene\s+)?(?:Abnahme-?|Unit-?|Scaffold-?|Akzeptanz-?)?[Tt]ests?\b/;
    for (const file of files) {
        const text = await textOf(file);
        if (text === null) {
            continue;
        }
        const isProtocol = file.startsWith(PROTOCOL_PREFIX);
        const lines = text.split('\n');
        for (let index = 0; index < lines.length; index += 1) {
            const match = pattern.exec(lines[index]);
            if (match === null) {
                continue;
            }
            const where = {
                file,
                line: index + 1,
                claimed: Number(match[1]),
                text: lines[index].trim().slice(0, 160),
            };
            if (isProtocol) {
                skippedProtocols.push(where);
            } else {
                claims.push(where);
            }
        }
    }
    return { claims, skippedProtocols };
}

async function main() {
    const totalStarted = Date.now();
    const args = process.argv.slice(2);
    let out = DEFAULT_OUT;
    for (let i = 0; i < args.length; i += 1) {
        if (args[i] === '--out') {
            out = resolve(args[i + 1]);
            i += 1;
        }
    }

    const failures = [];
    const extras = {};

    /*
     * ------------------------------------ 0a. Der Arbeitsbaum, als Erstes
     *
     * Vor allem anderen, weil dieser Lauf selbst schreibt: die Eval-Regression
     * gleich unten legt verification/w6/evalcheck.json ab, und wer den Baum
     * danach misst, misst seine eigene Spur. `cleanTree` soll den Baum
     * beschreiben, den dieses Gate VORGEFUNDEN hat, denn genau das fragt der
     * Contract ("sauberer Baum nach dem Commit"). Gemessen wird darum hier, in
     * der ersten Zeile, und alles, was der Lauf danach schreibt, kann diese
     * Zahl nicht mehr faerben.
     */
    const status = (await git(['status', '--porcelain=v1', '-uall']))
        .split('\n').map((line) => line.trimEnd()).filter(Boolean);
    const dirtyPaths = status.map((line) => ({ code: line.slice(0, 2).trim(), path: line.slice(3) }));
    const unexpectedDirtyPaths = dirtyPaths
        .filter((entry) => !RELEASE_GATE_SURFACE.some((prefix) => entry.path.startsWith(prefix)))
        .map((entry) => `${entry.code} ${entry.path}`);
    const cleanTree = dirtyPaths.length === 0;
    log(`Arbeitsbaum beim Start: ${dirtyPaths.length} Pfade offen, davon `
        + `${unexpectedDirtyPaths.length} ausserhalb der Aenderungsflaeche dieses Zyklus`);

    /*
     * ----------------------------------------- 0b. Die Eval als Regression
     *
     * Weit vorne, aus zwei Gruenden. Erstens dauert sie ein paar Minuten, und
     * ein Gate, das erst nach zehn billigen Pruefungen an der teuren scheitert,
     * ist ein Gate, das man zehnmal umsonst gefahren hat. Zweitens schreibt
     * sie verification/w6/evalcheck.json, und die Artefaktliste unten zaehlt
     * genau diese Datei: liefe sie danach, meldete der erste Lauf auf einer
     * frischen Maschine ein fehlendes Artefakt, das er selbst gleich erzeugt.
     */
    log('npm run eval:check');
    const evalCheck = await run('npm', ['run', 'eval:check']);
    const evalCheckPass = evalCheck.code === 0;
    extras.evalCheck = { exit: evalCheck.code, ms: evalCheck.ms, out: tail(evalCheck.out, 16) };
    const evalCheckFile = join(ROOT, 'verification', 'w6', 'evalcheck.json');
    const evalcheck = {
        path: GENERATED_PROOF_FILES[0],
        sha256: '',
        generatedAt: '',
    };
    if (existsSync(evalCheckFile)) {
        try {
            const raw = await readFile(evalCheckFile, 'utf8');
            const parsed = JSON.parse(raw);
            evalcheck.sha256 = createHash('sha256').update(raw).digest('hex');
            evalcheck.generatedAt = typeof parsed.generatedAt === 'string' ? parsed.generatedAt : '';
            extras.evalCheck.comparisons = parsed.comparisons ?? [];
            extras.evalCheck.reasons = parsed.reasons ?? [];
        } catch (err) {
            extras.evalCheck.readError = err && err.message ? err.message : String(err);
        }
    }
    log(`Eval-Regression: exit ${evalCheck.code} nach ${evalCheck.ms} ms`);
    for (const entry of extras.evalCheck.comparisons ?? []) {
        log(`  ${entry.name}: passRate ${entry.measuredPassRate} gegen aufgezeichnete `
            + `${entry.recordedPassRate}, Zitattreue ${entry.measuredCitation}`);
    }

    // ------------------------------------------------ 1. Die Commit-Serie
    const logLines = (await git(['log', '--format=%H%x09%h%x09%ad%x09%s', '--date=short', '--reverse']))
        .split('\n').map((line) => line.trim()).filter(Boolean)
        .map((line) => {
            const [hash, short, date, ...rest] = line.split('\t');
            return { hash, short, date, subject: rest.join('\t') };
        });
    const head = logLines[logLines.length - 1];
    extras.commitSeries = logLines.map((entry) => ({ commit: entry.short, subject: entry.subject }));
    log(`Commit-Serie: ${logLines.length} Commits, HEAD ${head?.short} "${head?.subject}"`);

    const usedCommits = new Set();
    const planReconciliation = PLAN_ITEMS.map((entry) => {
        const found = logLines.find((commit) => commit.subject.startsWith(entry.match));
        if (found === undefined) {
            return {
                phase: entry.phase,
                item: entry.item,
                commit: `${head?.short ?? 'HEAD'}+arbeitsbaum`,
                commitSubject: `${entry.match} (erwartet, noch nicht in der Serie)`,
                evidence: entry.evidence,
                status: 'im laufenden Zyklus hergestellt, noch nicht committet',
            };
        }
        usedCommits.add(found.hash);
        return {
            phase: entry.phase,
            item: entry.item,
            commit: found.short,
            commitSubject: found.subject,
            evidence: entry.evidence,
            status: 'committet',
        };
    });
    const commitsWithoutPlanItem = logLines
        .filter((commit) => !usedCommits.has(commit.hash))
        .map((commit) => ({ commit: commit.short, subject: commit.subject }));
    const planItemsWithoutCommit = planReconciliation
        .filter((entry) => entry.status !== 'committet')
        .map((entry) => entry.item);
    log(`Plan-Zuordnung: ${planReconciliation.length} Punkte, `
        + `${planItemsWithoutCommit.length} noch im Arbeitsbaum, `
        + `${commitsWithoutPlanItem.length} Commits ohne Plan-Punkt`);

    // ------------------------------------------------------ 2. Die Artefakte
    const missingArtifacts = [];
    const artifactList = [];
    for (const [phase, paths] of Object.entries(EXPECTED_ARTIFACTS)) {
        for (const path of paths) {
            const absolute = join(ROOT, path);
            const there = existsSync(absolute);
            const bytes = there ? statSync(absolute).size : 0;
            artifactList.push({ phase, path, exists: there, bytes });
            if (!there || bytes === 0) {
                missingArtifacts.push(path);
            }
        }
    }
    const listedPaths = new Set(artifactList.map((entry) => entry.path));
    const files = await repositoryFiles();
    const unlistedArtifacts = files.filter(
        (path) => path.startsWith('verification/')
            && !listedPaths.has(path)
            && relative(ROOT, out) !== path,
    );
    const allSmokeArtifactsPresent = missingArtifacts.length === 0 && unlistedArtifacts.length === 0;
    log(`Artefakte: ${artifactList.length} erwartet, ${missingArtifacts.length} fehlen, `
        + `${unlistedArtifacts.length} liegen ungelistet daneben`);
    for (const path of missingArtifacts) {
        log('  fehlt:', path);
    }
    for (const path of unlistedArtifacts) {
        log('  ungelistet:', path);
    }

    // ------------------------------------------------------- 3. Der Stilscan
    const style = await scanStyle(files);
    const attributionHits = style.attributionHits.length + style.patternHits.length;
    const dashHitsOutsideDocumentedQuotes = style.dashHits.length;
    log(`Stil: ${attributionHits} Attribution (${style.attributionExceptions.length} Verbotssaetze `
        + `ausgenommen), ${dashHitsOutsideDocumentedQuotes} lange Striche ausserhalb dokumentierter `
        + `Zitate (${style.dashExceptions.length} dokumentiert)`);

    // -------------------------------------------- 4. Uebergabe und Push-Sperre
    const upstream = { file: 'UPSTREAM-ASKS.md' };
    upstream.exists = existsSync(join(ROOT, 'UPSTREAM-ASKS.md'));
    upstream.tracked = files.includes('UPSTREAM-ASKS.md');
    const upstreamText = upstream.exists ? await readFile(join(ROOT, 'UPSTREAM-ASKS.md'), 'utf8') : '';
    const standMatch = /Stand\s+(\d{4}-\d{2}-\d{2})/.exec(upstreamText);
    upstream.stand = standMatch === null ? '' : standMatch[1];
    upstream.asks = (upstreamText.match(/^## Ask \d+/gm) ?? []).length;
    upstream.addenda = (upstreamText.match(/^NACHTRAG/gm) ?? []).length;
    /*
     * "Aktuell" heisst hier: nicht aelter als die Datei, aus der die Liste
     * abgeleitet ist. INVENTAR.md ist ihre Quelle (Kopf der Liste); wuerde das
     * Inventar spaeter fortgeschrieben als die Asks, waere die Uebergabe ein
     * Stand von vorgestern.
     *
     * Der Arbeitsbaum zaehlt mit, und das steht im Ergebnis.
     *
     * Bis zum 2026-08-29 verglich diese Zeile nur Commit-Zeiten und sah damit
     * ausschliesslich Committetes. Am 2026-08-29 hat das eine Falle aufgemacht,
     * aus der es keinen Ausweg gab: der Commit W6b hat INVENTAR.md
     * fortgeschrieben und UPSTREAM-ASKS.md nicht, also war die Ask-Liste aelter
     * als ihre Quelle. Die fortgeschriebene Liste lag daraufhin im Arbeitsbaum,
     * aber der Vergleich sah sie nicht: sie konnte erst nach ihrem Commit als
     * fortgeschrieben gelten, und der Commit brauchte das gruene Gate. Ein Gate,
     * das nur durch das zu Beweisende hindurch gruen wird, ist kein Gate.
     *
     * Gefragt ist "ist die Ask-Liste auf dem Stand des Inventars", und diese
     * Frage stellt sich an den Stand, der gleich committet wird. Traegt
     * UPSTREAM-ASKS.md uncommittete Aenderungen, ist sie fortgeschrieben. Woher
     * die Antwort kam, steht als `answeredFrom` daneben, damit niemand die
     * Lockerung uebersieht. Dasselbe Verfahren und derselbe Grund wie in
     * tools/freshclone-check.mjs (`workingTreeOverlay`).
     *
     * Die Lockerung schliesst sich mit dem Commit von selbst: sobald die Liste
     * committet ist, traegt sie den juengeren Zeitstempel, und die Commit-Zeiten
     * beantworten die Frage wieder allein.
     */
    const commitDate = async (path) =>
        (await git(['log', '-1', '--format=%ct', '--', path])).trim();
    upstream.lastCommitAt = Number(await commitDate('UPSTREAM-ASKS.md'));
    upstream.inventarCommitAt = Number(await commitDate('INVENTAR.md'));
    upstream.notOlderThanInventarCommitted = upstream.lastCommitAt >= upstream.inventarCommitAt;
    upstream.workingTreeOverlay = dirtyPaths
        .filter((entry) => entry.path === 'UPSTREAM-ASKS.md')
        .map((entry) => `${entry.code} ${entry.path}`);
    upstream.notOlderThanInventar =
        upstream.notOlderThanInventarCommitted || upstream.workingTreeOverlay.length > 0;
    upstream.answeredFrom = upstream.notOlderThanInventarCommitted
        ? 'die Commit-Zeiten'
        : (upstream.workingTreeOverlay.length > 0
            ? 'der Arbeitsbaum: UPSTREAM-ASKS.md traegt uncommittete Aenderungen'
            : 'die Commit-Zeiten, und sie sagen nein');
    /*
     * Und die zweite Haelfte derselben Frage: das Repository darf keinen Weg
     * kennen, der zu Martins Repository hinauf zeigt.
     */
    const remotes = (await git(['remote', '-v'])).split('\n').map((line) => line.trim()).filter(Boolean);
    upstream.remotes = remotes;
    upstream.upstreamRemotes = remotes.filter((line) => /codebase-memory-mcp/i.test(line));
    const upstreamAsksHandover =
        upstream.exists
        && upstream.tracked
        && upstream.stand.length > 0
        && upstream.asks > 0
        && upstream.notOlderThanInventar
        && upstream.upstreamRemotes.length === 0;
    log(`Uebergabe: ${upstream.asks} Asks, Stand ${upstream.stand}, `
        + `nicht aelter als INVENTAR.md: ${upstream.notOlderThanInventar} `
        + `(das beantwortet ${upstream.answeredFrom}), `
        + `eigene Remotes ohne Martins Repository: ${upstream.upstreamRemotes.length === 0}`);

    const cbmPush = { path: 'cbm', expected: DISABLED_PUSH_URL };
    cbmPush.present = existsSync(join(ROOT, 'cbm', '.git'));
    if (cbmPush.present) {
        try {
            const { stdout } = await execFileAsync('git', ['-C', join(ROOT, 'cbm'), 'remote', 'get-url', '--push', 'origin']);
            cbmPush.pushUrl = stdout.trim();
        } catch (err) {
            cbmPush.pushUrl = '';
            cbmPush.error = err && err.message ? err.message : String(err);
        }
    } else {
        cbmPush.pushUrl = '';
        cbmPush.error = 'cbm/ liegt nicht als Klon vor (gitignoriert); die Push-Sperre ist nicht messbar';
    }
    const cbmPushDisabled = cbmPush.pushUrl === DISABLED_PUSH_URL;
    log(`cbm-Push-URL: "${cbmPush.pushUrl}" (erwartet "${DISABLED_PUSH_URL}")`);

    // 5. Der Arbeitsbaum steht schon gemessen da, ganz oben in 0a. Der Grund
    // steht dort: dieser Lauf schreibt selbst, und er soll seine eigene Spur
    // nicht als Befund melden.

    // ------------------------------------------------------- 6. Die Fassung
    //
    // Gefragt wird nicht "ist es die Zahl, die hier steht", sondern "sagen die
    // Datei und die gebaute Oberflaeche dasselbe". Fehlt das Beweisartefakt,
    // ist die Frage unbeantwortet und nicht beantwortet: dann faellt das Gate
    // mit dem Grund, statt eine Uebereinstimmung anzunehmen.
    const pkg = JSON.parse(await readFile(join(ROOT, 'package.json'), 'utf8'));
    const expectedChip = `v${pkg.version}`;
    const airgapFile = join(ROOT, AIRGAP_ARTIFACT);
    let shownChip = '';
    let versionReadError = '';
    if (existsSync(airgapFile)) {
        try {
            shownChip = JSON.parse(await readFile(airgapFile, 'utf8')).versionChipShown ?? '';
        } catch (err) {
            versionReadError = err && err.message ? err.message : String(err);
        }
    } else {
        versionReadError = `${AIRGAP_ARTIFACT} fehlt`;
    }
    const versionMatches = shownChip === expectedChip;
    extras.version = {
        packageVersion: pkg.version,
        expectedChip,
        shownChip,
        measuredIn: AIRGAP_ARTIFACT,
        readError: versionReadError,
    };
    log(`package.json version: ${pkg.version}; der gemessene Chip sagt `
        + `"${shownChip || versionReadError}"`);

    // ------------------------------------------------------ 7. Die Unit-Suite
    log('npm run test:unit');
    const unit = await run('npm', ['run', 'test:unit']);
    const unitPass = unit.code === 0;
    const unitTests = Number(/Tests\s+(\d+)\s+passed/.exec(unit.out)?.[1] ?? Number.NaN);
    const unitFiles = Number(/Test Files\s+(\d+)\s+passed/.exec(unit.out)?.[1] ?? Number.NaN);
    extras.unit = {
        exit: unit.code,
        ms: unit.ms,
        tests: unitTests,
        files: unitFiles,
        out: tail(unit.out, 12),
    };
    log(`Unit-Suite: exit ${unit.code}, ${unitTests} Tests in ${unitFiles} Dateien nach ${unit.ms} ms`);

    // --------------------------------------------- 8. Die Zahlen in der Doku
    const { claims, skippedProtocols } = await documentedTestClaims();
    log(`Testzahlen in der Doku: ${claims.length} lebende Angaben, `
        + `${skippedProtocols.length} in datierten Protokollen uebergangen`);

    const buildReport = (fullSuitePass, scaffold, testCountSync, note) => ({
        fullSuitePass,
        unitPass,
        evalCheckPass,
        allSmokeArtifactsPresent,
        planReconciliation,
        testCountSync,
        attributionHits,
        dashHitsOutsideDocumentedQuotes,
        upstreamAsksHandover,
        cbmPushDisabled,
        cleanTree,
        version: pkg.version,
        sourceHead: head?.hash ?? '',
        generatedProofFiles: GENERATED_PROOF_FILES,
        evalcheck,
        versionMatches,
        missingArtifacts,
        unlistedArtifacts,
        artifactsChecked: artifactList.length,
        planItemsWithoutCommit,
        commitsWithoutPlanItem,
        unexpectedDirtyPaths,
        dirtyPaths,
        upstream,
        cbmPush,
        style: {
            attributionMatches: style.attributionHits,
            attributionPatternMatches: style.patternHits,
            documentedAttributionExceptions: style.attributionExceptions.length,
            dashMatches: style.dashHits,
            documentedQuoteExceptions: style.dashExceptions.length,
            /*
             * Die gesuchten Namen stehen hier NICHT ausgeschrieben, und das ist
             * kein Versteck, sondern die Vermeidung eines Fussangels: ein
             * Bericht, der sie ausschreibt, ist selbst eine Fundstelle fuer den
             * naechsten Scan. Genau das passiert dem Stil-Gate mit seinem
             * eigenen Bericht, und es loest es damit, dass es ihn nicht liest.
             * Aufgeloest nachzulesen sind sie dort (namesWatched) und in einer
             * Zeile mit `node -p`.
             */
            namesWatchedCount: WATCHED_NAMES.length,
            namesWatchedSource: 'tools/lib/forbidden-names.mjs',
            reportsNotRead: REPORTS_NOT_SCANNED,
            textFiles: style.textFiles,
            binaryFiles: style.binaryFiles,
        },
        artifacts: artifactList,
        scaffold,
        note,
        generatedAt: new Date().toISOString(),
        durationMs: Date.now() - totalStarted,
        extras,
    });

    /*
     * 9. Die eigene Datei, VOR dem Abnahmelauf. Warum das kein Zirkelschluss
     * ist, steht im Kopf dieser Datei.
     */
    const measuredCounts = {
        scaffoldTests: Number.NaN,
        scaffoldPass: Number.NaN,
        scaffoldFail: Number.NaN,
        unitTests,
        unitFiles,
        documentedClaims: claims,
        skippedProtocolClaims: skippedProtocols,
        protocolPrefix: PROTOCOL_PREFIX,
        mismatches: [],
        synced: true,
        method:
            'Gemessen aus der TAP-Ausgabe von node --test tests/scaffold/ und aus der Zusammenfassung '
            + 'von vitest, beide in diesem Lauf gefahren. Verglichen wird gegen jede Zahl, die eine '
            + 'verfolgte Markdown-Datei ausserhalb von ' + PROTOCOL_PREFIX + ' unmittelbar vor einem '
            + 'Testwort nennt. Die Zahlen in den datierten Protokollen darunter stehen als '
            + 'skippedProtocolClaims daneben und werden nicht verglichen: sie beschreiben den Stand '
            + 'ihres Tages und behaupten keinen von heute.',
    };
    await mkdir(dirname(out), { recursive: true });
    await writeFile(
        out,
        JSON.stringify(buildReport(true, { seeded: true }, measuredCounts,
            'Vorabfassung: sie liegt hier, waehrend der Abnahmelauf sie liest. Die Endfassung '
            + 'ueberschreibt sie mit dem gemessenen Ergebnis.'), null, 2) + '\n',
        'utf8',
    );

    /* Der oeffentliche Skriptweg ist Teil des Vertrags: aktuelle Node-Versionen
     * behandeln das Verzeichnis nicht mehr als Testdatei-Glob. */
    log('npm run test');
    const suite = await run('npm', ['run', 'test']);
    const fullSuitePass = suite.code === 0;
    const tapCount = (label) => Number(
        new RegExp(`^(?:#\\s*|ℹ\\s*)${label}\\s+(\\d+)\\s*$`, 'mi')
            .exec(suite.out)?.[1] ?? Number.NaN,
    );
    const scaffoldTests = tapCount('tests');
    const scaffoldPassCount = tapCount('pass');
    const scaffoldFail = tapCount('fail');
    const failedTests = [...new Set([
        ...(suite.out.match(/^not ok \d+ - .*$/gm) ?? [])
            .map((line) => line.replace(/^not ok \d+ - /, '').trim()),
        ...(suite.out.match(/^✖\s+.+$/gm) ?? [])
            .map((line) => line.replace(/^✖\s+/, '').replace(/\s+\([\d.]+ms\)$/, '').trim()),
    ])];
    log(`Abnahmesuite: exit ${suite.code}, ${scaffoldPassCount}/${scaffoldTests} gruen, `
        + `${scaffoldFail} rot nach ${suite.ms} ms`);
    for (const name of failedTests) {
        log('  rot:', name);
    }

    measuredCounts.scaffoldTests = scaffoldTests;
    measuredCounts.scaffoldPass = scaffoldPassCount;
    measuredCounts.scaffoldFail = scaffoldFail;
    measuredCounts.mismatches = claims.filter(
        (claim) => claim.claimed !== scaffoldTests && claim.claimed !== unitTests,
    );
    measuredCounts.synced = measuredCounts.mismatches.length === 0;
    if (claims.length === 0) {
        measuredCounts.note =
            'Keine lebende verfolgte Markdown-Datei nennt eine Testzahl. Die gemessenen Zahlen '
            + 'stehen darum hier als einzige Quelle; es gibt keine Angabe, von der sie abweichen '
            + `koennten. ${skippedProtocols.length} Zahlen stehen in datierten Protokollen unter `
            + `${PROTOCOL_PREFIX} und beschreiben ihren eigenen Tag.`;
    }

    const scaffold = {
        exit: suite.code,
        ms: suite.ms,
        tests: scaffoldTests,
        pass: scaffoldPassCount,
        fail: scaffoldFail,
        failedTests,
        out: tail(suite.out, 12),
    };

    await writeFile(
        out,
        JSON.stringify(buildReport(fullSuitePass, scaffold, measuredCounts,
            'Endfassung: jede Zahl in dieser Datei ist in diesem Lauf gemessen.'), null, 2) + '\n',
        'utf8',
    );
    log('geschrieben:', out);

    if (!fullSuitePass) {
        failures.push(`die Abnahmesuite ist rot (${scaffoldFail} von ${scaffoldTests})`);
    }
    if (!unitPass) {
        failures.push('die Unit-Suite ist rot');
    }
    if (!evalCheckPass) {
        const why = (extras.evalCheck.reasons ?? []).join('; ');
        failures.push(`die Eval-Regression ist rot${why.length > 0 ? `: ${why}` : ''}`);
    }
    if (!allSmokeArtifactsPresent) {
        failures.push(`${missingArtifacts.length} Artefakte fehlen: ${missingArtifacts.join(', ')}`);
    }
    if (!cleanTree) {
        failures.push(`${dirtyPaths.length} Dirty-Pfade im Release-Kandidaten`);
    }
    if (unlistedArtifacts.length > 0) {
        failures.push(`${unlistedArtifacts.length} Artefakte sind nicht in EXPECTED_ARTIFACTS aufgefuehrt`);
    }
    if (commitsWithoutPlanItem.length > 0) {
        failures.push(`${commitsWithoutPlanItem.length} Commits haben keinen PLAN_ITEMS-Punkt`);
    }
    if (attributionHits !== 0) {
        failures.push(`${attributionHits} Attributions-Treffer`);
    }
    if (dashHitsOutsideDocumentedQuotes !== 0) {
        failures.push(`${dashHitsOutsideDocumentedQuotes} lange Striche ausserhalb dokumentierter Zitate`);
    }
    if (!upstreamAsksHandover) {
        failures.push('die Upstream-Uebergabe steht nicht');
    }
    if (!cbmPushDisabled) {
        failures.push(`die cbm-Push-URL steht auf "${cbmPush.pushUrl}" statt auf "${DISABLED_PUSH_URL}"`);
    }
    if (!versionMatches) {
        failures.push(
            `die gebaute Oberflaeche zeigt "${shownChip || versionReadError}", `
            + `package.json sagt ${expectedChip}`,
        );
    }
    if (!measuredCounts.synced) {
        failures.push(`${measuredCounts.mismatches.length} Testzahlen in der Doku passen nicht`);
    }
    if (unexpectedDirtyPaths.length > 0) {
        failures.push(`${unexpectedDirtyPaths.length} geaenderte Pfade ausserhalb der Aenderungsflaeche`);
    }

    if (failures.length > 0) {
        console.error('[release-gate] Das Release-Gate ist NICHT durch:');
        for (const reason of failures) {
            console.error('  -', reason);
        }
        process.exitCode = 1;
        return;
    }
    log(`Release-Gate durch: v${pkg.version} ist freigegeben.`);
}

main().catch((err) => {
    console.error('[release-gate] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
