#!/usr/bin/env node
/*
 * Das Stil-Gate: lange Striche, Attribution, hartkodierte Chrome-Strings.
 *
 *   node tools/style-gate.mjs [--out verification/w6/stylegate.json]
 *
 * Drei Fragen an alles, was in diesem Repository landet, und alle drei sind an
 * einem gruenen Testlauf nicht zu sehen.
 *
 * ## 1. Lange Striche
 *
 * U+2013 und U+2014 sind in diesem Projekt verboten (PLAN.md Abschnitt 12).
 * Die eine erlaubte Ausnahme ist ein WOERTLICHES Zitat: ein Beweislauf, der
 * aufschreibt, was ein Server geantwortet hat, darf die Antwort nicht
 * umschreiben, nur weil ein Strich darin steht. Ein umgeschriebenes Zitat waere
 * ein Beweis ueber einen Text, den es nicht gab.
 *
 * Die Ausnahme gilt darum nur unter `verification/`, nur in einer JSON-Datei,
 * und jede einzelne Fundstelle wird mit Datei, Zeile und Grund im Ergebnis
 * aufgezaehlt. Eine Ausnahme, die man nicht nachlesen kann, ist keine Ausnahme,
 * sondern eine Luecke.
 *
 * ## 2. Attribution
 *
 * Kein Werkzeug und kein Modell wird in diesem Repository als Urheber genannt.
 * Der Scan sucht darum ZWEI Dinge, und der Unterschied zwischen ihnen ist der
 * ganze Punkt:
 *
 *  - **Urheberschafts-Muster**: eine Mitautoren-Zeile, eine Erzeugt-mit-Zeile,
 *    eine Erwaehnung mit Klammeraffen, eine Mailadresse der Herstellerdomain
 *    oder ein "written by" mit dem Namen dahinter. Das ist eine Attribution.
 *    Davon darf es null geben, ohne jede Ausnahme.
 *  - **Der blosse Name**, ohne Ansehen der Gross-/Kleinschreibung. Davon gibt
 *    es in diesem Repository eine Handvoll, und alle stehen in dem Satz, der
 *    Attribution VERBIETET: die Regel selbst in den beiden Regeldateien und die
 *    Zusicherung im Abnahmetest, der diese Zahl prueft. Ein Verbot, das seinen
 *    eigenen Gegenstand nicht nennen darf, waere unformulierbar.
 *
 * Deshalb steht im Ergebnis `attributionHits` fuer den Namen AUSSERHALB dieser
 * Verbotssaetze, und jede ausgenommene Fundstelle steht mit Datei, Zeile und
 * Wortlaut daneben. Eine Zeile ist nur dann ausgenommen, wenn sie erstens kein
 * Urheberschafts-Muster traegt und zweitens erkennbar von der Regel handelt
 * (ein Verneinungswort, das Wort Attribution, oder der Name der Regeldatei in
 * derselben Zeile). Damit kann die Ausnahme keine echte Attribution
 * verstecken: eine Signaturzeile traegt keines dieser Worte.
 *
 * Die gesuchten Namen und die gesuchten Zeichen stehen nicht in dieser Datei,
 * sondern in tools/lib/forbidden-names.mjs, und dort als Zeichencodes. Der
 * Grund steht im Kopf jener Datei: ein Waechter, der seinen eigenen Suchbegriff
 * ausschreibt, findet sich selbst, und die uebliche Antwort darauf (die eigene
 * Datei von der Pruefung ausnehmen) macht genau die eine Datei blind, in der
 * sich etwas verstecken koennte. Was gesucht wird, steht aufgeloest im
 * Ergebnis, unter `namesWatched`.
 *
 * ## 3. Hartkodierte Chrome-Strings
 *
 * Dieselbe Zahl, die auch der Unit-Test misst (src/i18n/messages.test.ts). Sie
 * steht hier mit, weil der Abnahmetest sie in stylegate.json liest und weil ein
 * Gate, das drei Stilfragen stellt, nicht die vierte an eine andere Datei
 * abgeben sollte. Die Regeln stehen in tools/lib/chrome-scan.mjs.
 *
 * ## Welche Dateien gelesen werden
 *
 * `git ls-files` PLUS die noch nicht aufgenommenen Dateien, die nicht ignoriert
 * sind (`git ls-files --others --exclude-standard`). Nur die verfolgten zu
 * lesen hiesse, genau die neuen Dateien eines Zyklus auszulassen, also die
 * einzigen, die eine neue Verletzung enthalten koennten. Binaerdateien werden
 * an ihrem NUL-Byte erkannt und uebergangen.
 */

import { execFile } from 'node:child_process';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { existsSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import { CHROME_FILES, SCAN_WHITELIST, scanChrome } from './lib/chrome-scan.mjs';
import {
    ATTRIBUTION_PATTERNS,
    LONG_DASH,
    NAME_PATTERN,
    RULE_FILE_PATTERN,
    WATCHED_NAMES,
} from './lib/forbidden-names.mjs';

const execFileAsync = promisify(execFile);

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const DEFAULT_OUT = join(ROOT, 'verification', 'w6', 'stylegate.json');

/**
 * Woran eine Zeile erkennbar von der Regel handelt statt eine Urheberschaft zu
 * behaupten.
 *
 * Ein Verbot nennt seinen Gegenstand und verneint ihn im selben Atemzug, oder
 * es nennt die Datei, in der die Regel steht. Eine Signaturzeile tut weder das
 * eine noch das andere.
 */
const RULE_SENTENCE = /\b(keine|kein|nicht|no|nie|never|verboten|forbidden|untersagt)\b|attribution|treffer/i;

/** Ein Grund, warum ein langer Strich in einem Beweisartefakt stehen darf. */
const QUOTE_REASON =
    'woertliches Zitat aus einer Server- oder Werkzeugantwort in einem Beweisartefakt: '
    + 'ein umgeschriebenes Zitat waere ein Beweis ueber einen Text, den es nicht gab';

const log = (...parts) => console.log('[style-gate]', ...parts);

/** Alles, was in dieses Repository gehoert: verfolgt plus neu und nicht ignoriert. */
async function repositoryFiles() {
    const listed = async (args) => {
        const { stdout } = await execFileAsync('git', args, { cwd: ROOT, maxBuffer: 32 * 1024 * 1024 });
        return stdout.split('\n').map((line) => line.trim()).filter(Boolean);
    };
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
 * Ob ein Strich in dieser Datei als woertliches Zitat durchgeht.
 *
 * Nur JSON unter verification/: dort schreiben die Beweislaeufe auf, was ein
 * Server geantwortet hat. Ein Markdown-Text unter verification/ ist eigener
 * Text und faellt nicht darunter.
 */
const quoteExempt = (path) => path.startsWith('verification/') && path.endsWith('.json');

async function main() {
    const args = process.argv.slice(2);
    let out = DEFAULT_OUT;
    for (let i = 0; i < args.length; i += 1) {
        if (args[i] === '--out') {
            out = resolve(args[i + 1]);
            i += 1;
        }
    }

    /*
     * Der eigene Bericht wird nicht gelesen.
     *
     * Er enthaelt woertlich die Zeilen, die dieser Lauf anderswo gefunden hat,
     * also faende der naechste Lauf jede Fundstelle ein zweites Mal, diesmal in
     * einem Zitat. Das waere keine zweite Beobachtung, sondern dieselbe, und
     * eine Ausnahme fuer eine erzeugte Datei versteckt nichts: was in ihr steht,
     * steht auch in der Datei, aus der sie es abgeschrieben hat.
     */
    const selfReport = relative(ROOT, out);
    const files = (await repositoryFiles()).filter((path) => path !== selfReport);
    const dashHits = [];
    const dashExceptions = [];
    const attributionHits = [];
    const attributionExceptions = [];
    const patternHits = [];
    let textFiles = 0;
    let binaryFiles = 0;

    for (const path of files) {
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
                    dashExceptions.push({ ...where, reason: QUOTE_REASON });
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
                    attributionExceptions.push({
                        ...where,
                        reason:
                            'der Satz, der Attribution verbietet, muss seinen Gegenstand nennen duerfen; '
                            + 'die Zeile traegt kein Urheberschafts-Muster',
                    });
                } else {
                    attributionHits.push(where);
                }
            }
        }
    }

    /*
     * Dateinamen getrennt gezaehlt. Die Regeldatei dieses Projekts traegt den
     * Werkzeugnamen im Dateinamen und ist keine Urheberangabe; sie wird trotzdem
     * genannt, damit die Zahl darueber nicht so aussieht, als habe der Scan sie
     * uebersehen.
     */
    const nameBearingFiles = files.filter((path) => NAME_PATTERN.test(path));

    const chrome = scanChrome();

    const report = {
        dashHitsOutsideDocumentedQuotes: dashHits.length,
        dashHits,
        documentedQuoteExceptions: dashExceptions,
        attributionHits: attributionHits.length,
        attributionMatches: attributionHits,
        attributionPatternHits: patternHits.length,
        attributionPatternMatches: patternHits,
        documentedAttributionExceptions: attributionExceptions,
        namesWatched: WATCHED_NAMES,
        selfReportExcluded: selfReport,
        ruleFileNames: nameBearingFiles,
        hardcodedChromeStrings: chrome.length,
        hardcodedChromeMatches: chrome,
        chromeFilesScanned: CHROME_FILES,
        chromeScanWhitelist: SCAN_WHITELIST,
        filesConsidered: files.length,
        textFiles,
        binaryFiles,
        generatedAt: new Date().toISOString(),
    };

    await mkdir(dirname(out), { recursive: true });
    await writeFile(out, JSON.stringify(report, null, 2) + '\n', 'utf8');
    log('geschrieben:', out);
    log(`${textFiles} Textdateien, ${binaryFiles} binaer uebergangen`);
    log(`lange Striche ausserhalb dokumentierter Zitate: ${report.dashHitsOutsideDocumentedQuotes}`
        + ` (dokumentierte Zitate: ${dashExceptions.length})`);
    log(`Attribution: ${report.attributionHits} Treffer, ${report.attributionPatternHits} Muster,`
        + ` ${attributionExceptions.length} Verbotssaetze ausgenommen`);
    log(`hartkodierte Chrome-Strings: ${report.hardcodedChromeStrings}`);

    const ok =
        report.dashHitsOutsideDocumentedQuotes === 0
        && report.attributionHits === 0
        && report.attributionPatternHits === 0
        && report.hardcodedChromeStrings === 0;
    if (!ok) {
        console.error('[style-gate] NICHT gruen.');
        for (const hit of [...dashHits, ...attributionHits, ...patternHits, ...chrome].slice(0, 40)) {
            console.error(`  ${hit.file}:${hit.line ?? '?'} ${hit.text ?? ''}`);
        }
        process.exitCode = 1;
        return;
    }
    log('Stil-Gate gruen.');
}

main().catch((err) => {
    console.error('[style-gate] Fehler:', err);
    process.exitCode = 1;
});
