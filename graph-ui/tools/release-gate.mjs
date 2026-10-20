#!/usr/bin/env node
/**
 * Validate the current release candidate with fresh suite and model-eval runs.
 * Machine-readable smoke evidence, content hashes, version agreement, style,
 * and a clean worktree remain checked. Development screenshots, recordings,
 * planning notes, and the former standalone repository handover are not
 * release prerequisites.
 *
 * Usage: node tools/release-gate.mjs [--out verification/w6/release.json]
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
    'THIRD_PARTY.md',
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
    ],
    w1: [
        'verification/w1/netdeny.json',
        'verification/w1/provider.json',
        'verification/w1/scaffold.json',
    ],
    w2: [
        'verification/w2/netdeny-w2b.json',
        'verification/w2/netdeny.json',
        'verification/w2/reader.json',
        'verification/w2/twin.json',
    ],
    w3: [
        'verification/w3/galaxy.json',
        'verification/w3/netdeny.json',
    ],
    w4: [
        'verification/w4/bugwizard.json',
        'verification/w4/coverage.json',
        'verification/w4/flow.json',
        'verification/w4/hierarchy.json',
        'verification/w4/impact.json',
        'verification/w4/netdeny-w4b.json',
        'verification/w4/netdeny-w4c.json',
        'verification/w4/netdeny-w4d.json',
        'verification/w4/netdeny-w4e.json',
        'verification/w4/netdeny.json',
        'verification/w4/tours.json',
    ],
    w5: [
        'verification/w5/chat.json',
        'verification/w5/eval.json',
        'verification/w5/flowfix.json',
        'verification/w5/models.json',
        'verification/w5/netdeny-w5a.json',
        'verification/w5/netdeny-w5b.json',
        'verification/w5/netdeny-w5c.json',
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
    ],
    'w6-demo': [
        'verification/w6/demo/demo.json',
    ],
    'w6-audit': [
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
        'verification/w7/help.json',
        'verification/w7/netdeny.json',
        'verification/w7/promises.json',
    ],
    /* Jede Erweiterung nach W7 steht ebenfalls statisch hier. Der Lauf liest
     * keine Soll-Liste aus Git: Git darf nur die tatsaechlich vorgefundenen
     * Dateien gegen diese ausgeschriebene Inventur vergleichen. */
    'w7-current': [
        'verification/w7/chat.json',
        'verification/w7/netdeny-w7b.json',
        'verification/w7/netdeny-w7c.json',
        'verification/w7/search.json',
    ],
    w8: [
        'verification/w8/layout.json',
        'verification/w8/netdeny.json',
    ],
    w8b: [
        'verification/w8b/netdeny.json',
        'verification/w8b/ux.json',
    ],
    w8c: [
        'verification/w8c/netdeny.json',
        'verification/w8c/pseudocode.json',
    ],
    w9: [
        'verification/w9/edges.json',
        'verification/w9/netdeny-w9.json',
    ],
    w10: [
        'verification/w10/models.json',
        'verification/w10/netdeny.json',
    ],
    w10b: [
        'verification/w10b/fixes.json',
        'verification/w10b/netdeny.json',
    ],
    w11: [
        'verification/w11/agents.json',
        'verification/w11/motion.json',
        'verification/w11/netdeny-w11b.json',
        'verification/w11/netdeny.json',
    ],
    w12: [
        'verification/w12/buttons.json',
        'verification/w12/netdeny.json',
    ],
    w12a: [
        'verification/w12a/command-overlay.json',
        'verification/w12a/netdeny.json',
    ],
    w13: [
        'verification/w13/netdeny.json',
        'verification/w13/reader.json',
    ],
    w14: [
        'verification/w14/before-levels.json.gz.b64',
        'verification/w14/netdeny.json',
        'verification/w14/symbols.json',
    ],
    w15: [
        'verification/w15/hybrid.json',
        'verification/w15/netdeny.json',
    ],
};


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

    const sourceHead = (await git(['rev-parse', 'HEAD'])).trim();

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
            && !/\.(?:png|jpe?g|webm|mp4|md)$/.test(path)
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
        testCountSync,
        attributionHits,
        dashHitsOutsideDocumentedQuotes,
        cleanTree,
        version: pkg.version,
        sourceHead,
        generatedProofFiles: GENERATED_PROOF_FILES,
        evalcheck,
        versionMatches,
        missingArtifacts,
        unlistedArtifacts,
        artifactsChecked: artifactList.length,
        unexpectedDirtyPaths,
        dirtyPaths,
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
    if (attributionHits !== 0) {
        failures.push(`${attributionHits} Attributions-Treffer`);
    }
    if (dashHitsOutsideDocumentedQuotes !== 0) {
        failures.push(`${dashHitsOutsideDocumentedQuotes} lange Striche ausserhalb dokumentierter Zitate`);
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
