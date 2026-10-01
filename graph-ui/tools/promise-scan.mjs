#!/usr/bin/env node
/*
 * Der Versprechen-Scan: sucht im Produktpfad nach Saetzen, die etwas fuer
 * spaeter zusagen, und nach Flaechen, die sich anklicken lassen und nichts tun.
 *
 *   node tools/promise-scan.mjs [--out verification/w7/promises.json]
 *
 * Nutzerauftrag vom 2026-08-29: "entferne alles, was Dinge verspricht, die wir
 * nicht koennen". Der Auftrag ist mit vier Fundstellen erledigt worden (die vier
 * Menuepunkte ohne Panel, die drei Twin-Saetze); dieses Werkzeug ist die Sperre
 * dagegen, dass die fuenfte unbemerkt entsteht. Es laeuft als `npm run
 * check:promises` und schreibt sein Ergebnis nach verification/w7/promises.json.
 *
 * ## Zwei Fragen, und warum es zwei sein muessen
 *
 * **(1) Sagt ein Satz etwas zu?** Ein Versprechen ist ein Satz, der eine
 * Zukunft behauptet, ueber die dieses Produkt nicht verfuegt ("arrives later",
 * "when X lands", "not wired"). Er ist an einem gruenen Testlauf nicht zu
 * sehen, denn er ist syntaktisch tadellos.
 *
 * **(2) Schweigt eine Flaeche?** Ein Knopf ohne Handler ist dasselbe
 * Versprechen ohne Worte. Er ist noch schlechter zu finden, weil er gar keinen
 * Text hat, an dem man ihn suchen koennte. Gefragt wird deshalb der Syntaxbaum:
 * jedes `button` und jedes Element mit `role="button"` muss einen Griff tragen,
 * der auf eine Bedienung antwortet.
 *
 * ## Wo die Muster gelten, und warum nicht ueberall dasselbe
 *
 * Zwei Reichweiten, und der Unterschied ist kein Kompromiss, sondern die
 * Aussage:
 *
 *  - `text`: die harten Muster gelten im GANZEN Quelltext, Kommentare
 *    eingeschlossen. "not wired", "coming soon", "arrives later", "when X
 *    lands", "TODO", "FIXME" und "demnaechst" sind auch in einer Begruendung
 *    keine Begruendung, sondern eine Schuld, die niemand aufgeschrieben hat.
 *  - `visible`: die weichen Muster gelten nur in dem, was auf dem Bildschirm
 *    landet (String-Literale und JSX-Text, erkannt vom TypeScript-Parser).
 *    "noch nicht" ist in einem deutschen Kommentar ueber einen Zustand
 *    ("leer, solange noch nicht gelesen") eine Auskunft ueber die Laufzeit; im
 *    Bildschirmtext einer englischsprachigen Oberflaeche waere es beides
 *    zugleich falsch.
 *
 * `TODO` und `FIXME` werden in Grossbuchstaben gesucht. Das ist die Form, in
 * der sie als Marke gemeint sind; ein `'todo'` in einer Stoppwortliste (so eins
 * steht in src/compiler/question-classifier.ts) ist ein Datenwert und keine
 * offene Aufgabe.
 *
 * ## Was NICHT gescannt wird
 *
 * Tests. Sie beschreiben, was nicht passieren darf, und muessen die verbotenen
 * Formulierungen dafuer aussprechen duerfen. Ein Scan, der ihnen das verbietet,
 * verbietet die Beschreibung des Verbots.
 */

import { readFileSync, existsSync } from 'node:fs';
import { mkdir, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { readdirSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
/** @type {import('typescript')} */
const ts = require('typescript');

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const SRC = join(ROOT, 'src');
const DEFAULT_OUT = join(ROOT, 'verification', 'w7', 'promises.json');

const log = (...parts) => console.log('[promise-scan]', ...parts);

/**
 * Die Muster, mit Reichweite und Grund.
 *
 * Als Daten und nicht als Regex-Kette im Code, weil sie in das Beweisartefakt
 * gehoeren: wer das Ergebnis liest, soll sehen, wonach gesucht wurde, ohne das
 * Werkzeug zu lesen.
 */
export const PROMISE_PATTERNS = [
    {
        name: 'not wired',
        scope: 'text',
        pattern: /not wired/i,
        reason: 'eine Flaeche, die zugibt, nichts zu tun, statt zu verschwinden',
    },
    {
        name: 'coming soon',
        scope: 'text',
        pattern: /coming soon/i,
        reason: 'ein Termin, den niemand halten muss',
    },
    {
        name: 'arrives later',
        scope: 'text',
        pattern: /arrives? later/i,
        reason: 'dieselbe Zusage in der eigenen Stimme des Produkts',
    },
    {
        name: 'when ... lands',
        scope: 'text',
        pattern: /\bwhen\b[^.\n]{0,60}\blands?\b/i,
        reason: 'ein Versprechen ueber eine fremde Roadmap',
    },
    {
        name: 'TODO',
        scope: 'text',
        pattern: /\bTODO\b/,
        reason: 'eine offene Aufgabe im Produktpfad, die niemand verfolgt',
    },
    {
        name: 'FIXME',
        scope: 'text',
        pattern: /\bFIXME\b/,
        reason: 'wie TODO, nur lauter',
    },
    {
        name: 'demnaechst',
        scope: 'text',
        pattern: /demn(ae|ä)chst/i,
        reason: 'dasselbe auf Deutsch',
    },
    {
        name: 'noch nicht',
        scope: 'visible',
        pattern: /noch nicht/i,
        reason:
            'im Bildschirmtext ein Versprechen (und in dieser Oberflaeche ausserdem die falsche '
            + 'Sprache); in einem Kommentar eine Auskunft ueber einen Zustand und deshalb erlaubt',
    },
    {
        name: 'not implemented',
        scope: 'visible',
        pattern: /not implemented|nicht implementiert/i,
        reason: 'ein Zustand, den der Leser nicht aendern kann, als Antwort verkauft',
    },
];

/**
 * Attribute, die eine Flaeche bedienbar machen.
 *
 * `onMouseDown` steht mit in der Liste, und das ist kein Nachgeben: die Zeilen
 * des Suchfensters antworten absichtlich auf `mousedown` statt auf `click`, weil
 * ein Klick, der erst nach dem Fokusverlust der Kommandozeile wirkt, ein Fenster
 * traefe, das dann schon zu waere (src/search/SearchOverlay.tsx). Ein Griff ist
 * ein Griff, auch wenn er frueher zupackt.
 */
const HANDLER_ATTRIBUTES = new Set([
    'onClick',
    'onKeyDown',
    'onKeyUp',
    'onMouseDown',
    'onMouseUp',
    'onPointerDown',
    'onPointerUp',
    'onSelect',
    'onChange',
    'onInput',
    'onSubmit',
    'onToggle',
]);

/**
 * Die dokumentierte Ausnahmeliste, wortgleich so, wie sie im Ergebnis steht.
 *
 * Eine Ausnahme ist hier eine BAUFORM und kein einzelnes Element: "dieser eine
 * Knopf ist in Ordnung" waere eine Liste, die mit jedem Zyklus laenger wird und
 * irgendwann alles enthaelt.
 */
export const AFFORDANCE_WHITELIST = [
    {
        rule: 'disabled',
        reason:
            'Ein Knopf mit `disabled` sagt selbst, dass er gerade nichts tut, und der Browser laesst '
            + 'ihn nicht anklicken. Das ist eine Auskunft und keine Attrappe.',
    },
    {
        rule: 'type=submit',
        reason:
            'Ein Absende-Knopf in einem Formular wird vom Formular bedient und nicht von sich selbst.',
    },
    {
        rule: 'spread-props',
        reason:
            'Ein Element, das `{...props}` bekommt, kann seinen Griff von aussen tragen. Der Parser '
            + 'sieht das nicht, und eine Meldung dazu waere geraten.',
    },
    {
        rule: 'test-files',
        reason:
            'Tests beschreiben, was nicht passieren darf, und bauen dafuer Attrappen. Sie werden gar '
            + 'nicht erst gelesen.',
    },
];

/** Jede Produktdatei unter src/, ohne Tests. */
function productFiles(directory = SRC) {
    const out = [];
    for (const entry of readdirSync(directory)) {
        const full = join(directory, entry);
        if (statSync(full).isDirectory()) {
            out.push(...productFiles(full));
            continue;
        }
        if (!/\.(ts|tsx)$/.test(entry) || /\.test\.(ts|tsx)$/.test(entry)) {
            continue;
        }
        out.push(full);
    }
    return out.sort();
}

/** Der Name eines JSX-Attributs als Text. */
function attributeName(node) {
    const name = node.name;
    if (name === undefined) {
        return '';
    }
    if (ts.isIdentifier(name) || ts.isStringLiteral(name)) {
        return name.text;
    }
    return name.getText?.() ?? '';
}

/** Der Tagname eines JSX-Elements, oder leer. */
function tagNameOf(opening) {
    const tag = opening.tagName;
    return ts.isIdentifier(tag) ? tag.text : (tag.getText?.() ?? '');
}

/**
 * Eine Datei lesen: die Fundstellen der Muster und die stummen Flaechen.
 */
export function scanFile(absolute) {
    const source = readFileSync(absolute, 'utf8');
    const file = ts.createSourceFile(absolute, source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TSX);
    const relativePath = relative(ROOT, absolute);
    const promises = [];
    const dead = [];

    const at = (pos) => file.getLineAndCharacterOfPosition(pos).line + 1;

    // ---------------------------------------------------- (1) die Saetze
    const lines = source.split('\n');
    for (const spec of PROMISE_PATTERNS.filter((entry) => entry.scope === 'text')) {
        lines.forEach((line, index) => {
            if (spec.pattern.test(line)) {
                promises.push({
                    file: relativePath,
                    line: index + 1,
                    pattern: spec.name,
                    scope: spec.scope,
                    text: line.trim().slice(0, 140),
                });
            }
        });
    }

    const visiblePatterns = PROMISE_PATTERNS.filter((entry) => entry.scope === 'visible');

    const checkVisible = (text, pos) => {
        for (const spec of visiblePatterns) {
            if (spec.pattern.test(text)) {
                promises.push({
                    file: relativePath,
                    line: at(pos),
                    pattern: spec.name,
                    scope: spec.scope,
                    text: text.replace(/\s+/g, ' ').trim().slice(0, 140),
                });
            }
        }
    };

    // ------------------------------------------- (2) der Baum, fuer beides
    const visit = (node) => {
        if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
            checkVisible(node.text, node.getStart());
        }
        if (ts.isTemplateHead(node) || ts.isTemplateMiddle(node) || ts.isTemplateTail(node)) {
            checkVisible(node.text, node.getStart());
        }
        if (ts.isJsxText(node)) {
            checkVisible(node.text, node.getStart());
        }

        const opening = ts.isJsxOpeningElement(node) || ts.isJsxSelfClosingElement(node) ? node : undefined;
        if (opening !== undefined) {
            const tag = tagNameOf(opening);
            const attributes = opening.attributes.properties;
            const named = attributes.filter((entry) => ts.isJsxAttribute(entry));
            const hasSpread = attributes.some((entry) => ts.isJsxSpreadAttribute(entry));
            const names = new Set(named.map(attributeName));
            const roleAttribute = named.find((entry) => attributeName(entry) === 'role');
            const role = roleAttribute?.initializer !== undefined
                && ts.isStringLiteral(roleAttribute.initializer)
                ? roleAttribute.initializer.text
                : '';
            const typeAttribute = named.find((entry) => attributeName(entry) === 'type');
            const type = typeAttribute?.initializer !== undefined
                && ts.isStringLiteral(typeAttribute.initializer)
                ? typeAttribute.initializer.text
                : '';

            const clickable = tag === 'button' || role === 'button' || role === 'option' || role === 'tab';
            const handled = [...names].some((entry) => HANDLER_ATTRIBUTES.has(entry));
            if (clickable && !handled && !hasSpread && !names.has('disabled') && type !== 'submit') {
                dead.push({
                    file: relativePath,
                    line: at(opening.getStart()),
                    tag,
                    role,
                    text: opening.getText().replace(/\s+/g, ' ').slice(0, 140),
                });
            }
        }

        ts.forEachChild(node, visit);
    };
    visit(file);

    return { promises, dead };
}

async function main() {
    const args = process.argv.slice(2);
    let out = DEFAULT_OUT;
    for (let i = 0; i < args.length; i += 1) {
        if (args[i] === '--out') {
            out = resolve(args[i + 1]);
            i += 1;
        }
    }
    if (!existsSync(SRC)) {
        throw new Error(`src/ fehlt: ${SRC}`);
    }

    const files = productFiles();
    const promiseHits = [];
    const deadAffordances = [];
    for (const absolute of files) {
        const found = scanFile(absolute);
        promiseHits.push(...found.promises);
        deadAffordances.push(...found.dead);
    }

    const report = {
        promiseHits: promiseHits.length,
        promiseMatches: promiseHits,
        deadAffordances: deadAffordances.length,
        deadAffordanceMatches: deadAffordances,
        filesScanned: files.length,
        whitelist: AFFORDANCE_WHITELIST,
        patterns: PROMISE_PATTERNS.map((entry) => ({
            name: entry.name,
            scope: entry.scope,
            source: entry.pattern.source,
            flags: entry.pattern.flags,
            reason: entry.reason,
        })),
        handlerAttributes: [...HANDLER_ATTRIBUTES],
        scanned: 'src/**/*.{ts,tsx} ohne *.test.*',
        generatedAt: new Date().toISOString(),
    };

    await mkdir(dirname(out), { recursive: true });
    await writeFile(out, JSON.stringify(report, null, 2) + '\n', 'utf8');
    log('geschrieben:', out);
    log(`${files.length} Produktdateien gelesen`);
    log(`Versprechen: ${report.promiseHits}, stumme Flaechen: ${report.deadAffordances}`);

    if (report.promiseHits > 0 || report.deadAffordances > 0) {
        console.error('[promise-scan] NICHT gruen.');
        for (const hit of promiseHits) {
            console.error(`  ${hit.file}:${hit.line} [${hit.pattern}] ${hit.text}`);
        }
        for (const hit of deadAffordances) {
            console.error(`  ${hit.file}:${hit.line} <${hit.tag}> ohne Griff: ${hit.text}`);
        }
        process.exitCode = 1;
        return;
    }
    log('Versprechen-Scan gruen.');
}

main().catch((err) => {
    console.error('[promise-scan] Fehler:', err);
    process.exitCode = 1;
});
