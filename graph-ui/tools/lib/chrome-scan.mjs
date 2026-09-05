/**
 * Der Scan nach hartkodierten sichtbaren Strings im App-Chrome.
 *
 * Er gehoert an eine Stelle, weil ihn zwei Aufrufer brauchen: der Unit-Test
 * src/i18n/messages.test.ts, der ihn bei jedem `npm run test:unit` fahren
 * laesst, und tools/style-gate.mjs, das seine Zahl nach
 * verification/w6/stylegate.json schreibt. Zwei Fassungen derselben Regel
 * waeren zwei Gelegenheiten, eine davon weicher zu machen.
 *
 * ## Warum der TypeScript-Parser und nicht ein regulaerer Ausdruck
 *
 * Ein Ausdruck ueber den Rohtext kann einen Satz in einem Kommentar nicht von
 * einem Satz in der Oberflaeche unterscheiden, und dieser Quelltext besteht zu
 * einem guten Teil aus Begruendungen. Der Parser sieht den Unterschied: ein
 * JSX-Textknoten ist Text auf dem Bildschirm, ein Docblock ist es nicht.
 * TypeScript liegt ohnehin als devDependency da und wird hier nur zum Lesen
 * benutzt (`createSourceFile`), nicht zum Uebersetzen.
 *
 * ## Was als sichtbare Fundstelle gilt
 *
 * Fuenf Stellen, und jede davon ist eine, an der ein Zeichen wirklich auf dem
 * Bildschirm landet:
 *
 *  1. **JSX-Textknoten.** `<span>EXPLORER</span>`.
 *  2. **String- oder Template-Literale direkt in einer JSX-Klammer.**
 *     `{'no file open'}`.
 *  3. **Literale in einem sichtbaren Attribut**: `title`, `placeholder`,
 *     `aria-label`, `alt`, `label`, `headline`, `note`, `empty`, `message`,
 *     `source`. Auch geschachtelt, also in einem Bedingungsausdruck oder einem
 *     Template innerhalb des Attributs.
 *  4. **Eigenschaften eines Objektliterals, die als Beschriftung gelesen
 *     werden**: `title`, `label`, `value`, `message`, `hint`, `note`, `detail`,
 *     `headline`, `placeholder`, `empty`. Darueber laufen die Menuepunkte, die
 *     Chips der Kopfzeile und die Chips der Statusleiste.
 *  5. **Saetze, die ein Zustandssetzer bekommt**, dessen Name auf `Message`,
 *     `Detail`, `Hint` oder `Name` endet. Darueber laufen die Meldungen, die
 *     App.tsx an Reader, Twin, Suche, Assistent und Fuehrung gibt.
 *
 * ## Was ausdruecklich NICHT zaehlt, und warum
 *
 * Die Ausnahmen sind Teil der Aussage und stehen darum im Ergebnis, nicht nur
 * hier im Kopf:
 *
 *  - **Literale ohne Buchstaben.** `'>'`, `'▾'`, `' › '`, `'●'`, Ziffern,
 *    Leerraum. Sie sind Zeichen und keine Saetze, und ein Katalogeintrag fuer
 *    ein Groesserzeichen waere eine Uebersetzung ohne Sprache.
 *  - **Ein einzelner Buchstabe.** Das `x` am Tab-Schliesser. Aus demselben
 *    Grund.
 *  - **data-Attribute, CSS-Klassen, Testmarken, Rollen und Schluessel.** Sie
 *    stehen nie auf dem Bildschirm; `className`, `data-*`, `role`, `key`,
 *    `id`, `htmlFor`, `type`, `href` und `name` werden gar nicht erst gelesen.
 *  - **Einzelne Woerter an Regel 5.** `setTwinStatus('ready')` setzt einen
 *    Zustand und keinen Satz. Regel 5 verlangt darum Leerraum im Literal:
 *    ein Zustandswort ist ein Token, ein Satz ist eine Beschriftung. Die
 *    Regeln 1 bis 4 kennen diese Ausnahme nicht, denn dort landet auch ein
 *    einzelnes Wort auf dem Bildschirm.
 */

import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
/** @type {import('typescript')} */
const ts = require('typescript');

export const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/**
 * Die Dateien, deren sichtbare Flaeche ganz zum App-Chrome gehoert.
 *
 * Die Liste ist die des Contracts (Kopfzeile, Menue, Tabs, Statusleiste,
 * Explorer-Legende, Kommandozeile, Panel-Koepfe, Why-Karten, Tour-Karte und die
 * Rahmen von Wizard, Impact, Sidecar und Chat) und sie endet dort, wo ein Modul
 * anfaengt, eigene Fachtexte zu tragen: TwinPanel, FlowOverlay, GalaxyPanel und
 * die Reader-Flaeche sprechen ueber IHREN Gegenstand und beziehen ihre Saetze
 * aus ko-lokalisierten strings.ts-Modulen. Das ist der PR-Standard und kein
 * Versaeumnis; sie stehen darum nicht in dieser Liste.
 */
export const CHROME_FILES = [
    'src/App.tsx',
    'src/app/AtlasChrome.tsx',
    'src/app/AtlasTree.tsx',
    // Die Hilfeseite (W7a) gehoert ganz hierher: sie spricht ueber das Produkt
    // als Ganzes und ueber kein einzelnes Fachgebiet, ihr Text steht deshalb im
    // Katalog und nicht neben ihr.
    'src/help/HelpOverlay.tsx',
    // Das Einstellungen-Panel (W10) gehoert aus demselben Grund hierher wie die
    // Hilfe: es spricht ueber das Produkt als Ganzes (welches Modell antwortet,
    // was das Zeichnen kostet) und ueber kein einzelnes Fachgebiet. Sein Text
    // steht darum im Katalog und nicht neben ihm.
    'src/settings/SettingsPanel.tsx',
    // The projects panel speaks about the product as a whole too (what is
    // indexed, what the server is doing); its text sits in the catalog.
    'src/projects/ProjectsPanel.tsx',
    'src/why/WhyPanel.tsx',
    'src/tours/TourCard.tsx',
    'src/search/SearchOverlay.tsx',
    'src/entry/EntryPointDialog.tsx',
    'src/llm/SidecarPanel.tsx',
    'src/chat/AtlasChatPanel.tsx',
    'src/traces/BugWizard.tsx',
    'src/impact/ImpactPanel.tsx',
];

/** Attribute, deren Wert ein Leser sieht oder vorgelesen bekommt. */
const VISIBLE_ATTRIBUTES = new Set([
    'title',
    'placeholder',
    'aria-label',
    'alt',
    'label',
    'value',
    'headline',
    'note',
    'empty',
    'message',
    'source',
]);

/** Eigenschaften eines Objektliterals, die als Beschriftung gezeichnet werden. */
const VISIBLE_PROPERTIES = new Set([
    'title',
    'label',
    'value',
    'message',
    'hint',
    'note',
    'detail',
    'headline',
    'placeholder',
    'empty',
]);

/** Setzer, deren Argument als Satz auf dem Bildschirm landet. */
const MESSAGE_SETTER = /^set[A-Za-z]*(Message|Detail|Hint|Name)$/;

/** Die dokumentierten Ausnahmen, wortgleich so wie sie im Ergebnis stehen. */
export const SCAN_WHITELIST = [
    {
        rule: 'no-letter',
        reason:
            'Literale ohne ASCII-Buchstaben sind Zeichen und keine Saetze: der Prompt ">", '
            + 'die Baumpfeile, das Trennzeichen des Breadcrumbs, der Ordnerpunkt, Ziffern und Leerraum.',
    },
    {
        rule: 'single-letter',
        reason: 'Ein einzelner Buchstabe ist ein Zeichen: das "x" am Tab-Schliesser.',
    },
    {
        rule: 'structural-attribute',
        reason:
            'className, data-*, role, key, id, htmlFor, type, href und name stehen nie auf dem '
            + 'Bildschirm und werden gar nicht erst gelesen.',
    },
    {
        rule: 'single-token-state',
        reason:
            'Regel 5 (Zustandssetzer) verlangt Leerraum im Literal: setTwinStatus("ready") setzt '
            + 'einen Zustand, keine Beschriftung.',
    },
    {
        rule: 'comment',
        reason:
            'Docblocks und Zeilenkommentare sind kein Bildschirmtext. Der Parser sieht den '
            + 'Unterschied, ein regulaerer Ausdruck koennte ihn nicht sehen.',
    },
];

const hasLetter = (value) => /[A-Za-z]/.test(value);

/** Ein Literal, das nach den Ausnahmen oben ueberhaupt eine Beschriftung sein kann. */
function isLabelText(value) {
    const trimmed = value.trim();
    if (trimmed.length === 0 || !hasLetter(trimmed)) {
        return false;
    }
    return trimmed.replace(/[^A-Za-z]/g, '').length > 1 || trimmed.length > 1;
}

/** Wie `isLabelText`, aber ein einzelner Buchstabe faellt heraus. */
function isVisibleLiteral(value) {
    const trimmed = value.trim();
    if (!hasLetter(trimmed)) {
        return false;
    }
    return trimmed.length > 1;
}

/**
 * Alle String-Literale eines Ausdrucks einsammeln, auch durch Bedingungen und
 * Templates hindurch.
 *
 * Ohne das Hindurchgehen waere `title={a ? 'x' : 'y'}` eine Fundstelle, die der
 * Scan nicht sieht, und genau diese Form steht an mehreren Knoepfen.
 */
function literalsOf(node, out = []) {
    if (node === undefined) {
        return out;
    }
    if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
        out.push({ text: node.text, pos: node.getStart() });
        return out;
    }
    if (ts.isTemplateExpression(node)) {
        for (const part of [node.head, ...node.templateSpans.map((span) => span.literal)]) {
            if (part.text.length > 0) {
                out.push({ text: part.text, pos: part.getStart() });
            }
        }
        node.templateSpans.forEach((span) => literalsOf(span.expression, out));
        return out;
    }
    if (ts.isConditionalExpression(node)) {
        literalsOf(node.whenTrue, out);
        literalsOf(node.whenFalse, out);
        return out;
    }
    if (ts.isParenthesizedExpression(node)) {
        return literalsOf(node.expression, out);
    }
    if (ts.isBinaryExpression(node) && node.operatorToken.kind === ts.SyntaxKind.PlusToken) {
        literalsOf(node.left, out);
        literalsOf(node.right, out);
        return out;
    }
    return out;
}

/** Den Namen eines JSX-Attributs oder einer Objekt-Eigenschaft als Text. */
function nameOf(node) {
    if (node === undefined) {
        return '';
    }
    if (ts.isIdentifier(node) || ts.isStringLiteral(node)) {
        return node.text;
    }
    if (ts.isJsxNamespacedName?.(node)) {
        return `${node.namespace.text}:${node.name.text}`;
    }
    return node.getText?.() ?? '';
}

/** Eine Datei lesen und die sichtbaren hartkodierten Strings darin melden. */
export function scanFile(relativePath, root = ROOT) {
    const absolute = join(root, relativePath);
    const source = readFileSync(absolute, 'utf8');
    const file = ts.createSourceFile(absolute, source, ts.ScriptTarget.ES2022, true, ts.ScriptKind.TSX);
    const findings = [];

    const report = (rule, text, pos) => {
        const { line, character } = file.getLineAndCharacterOfPosition(pos);
        findings.push({
            file: relative(root, absolute),
            line: line + 1,
            column: character + 1,
            rule,
            text: text.replace(/\s+/g, ' ').trim().slice(0, 120),
        });
    };

    const visit = (node) => {
        // 1. JSX-Textknoten.
        if (ts.isJsxText(node) && isVisibleLiteral(node.text)) {
            report('jsx-text', node.text, node.getStart());
        }

        /*
         * 2. Literale in einer JSX-Klammer, auch durch eine Bedingung hindurch.
         *
         * Ohne das Hindurchgehen waere `{last ? 'finish' : 'next'}` keine
         * Fundstelle, und das ist die haeufigste Form, in der ein Wort in
         * dieser Oberflaeche auf den Bildschirm kommt.
         */
        if (ts.isJsxExpression(node) && node.expression !== undefined
            && (node.parent === undefined || !ts.isJsxAttribute(node.parent))) {
            for (const found of literalsOf(node.expression)) {
                if (isVisibleLiteral(found.text)) {
                    report('jsx-expression', found.text, found.pos);
                }
            }
        }

        // 3. Sichtbare Attribute.
        if (ts.isJsxAttribute(node)) {
            const attribute = nameOf(node.name);
            if (VISIBLE_ATTRIBUTES.has(attribute) && node.initializer !== undefined) {
                const expression = ts.isJsxExpression(node.initializer)
                    ? node.initializer.expression
                    : node.initializer;
                for (const found of literalsOf(expression)) {
                    if (isVisibleLiteral(found.text)) {
                        report(`attribute:${attribute}`, found.text, found.pos);
                    }
                }
            }
        }

        // 4. Beschriftende Eigenschaften eines Objektliterals.
        if (ts.isPropertyAssignment(node)) {
            const property = nameOf(node.name);
            if (VISIBLE_PROPERTIES.has(property)) {
                for (const found of literalsOf(node.initializer)) {
                    if (isVisibleLiteral(found.text)) {
                        report(`property:${property}`, found.text, found.pos);
                    }
                }
            }
        }

        // 5. Saetze an einen Zustandssetzer.
        if (ts.isCallExpression(node) && ts.isIdentifier(node.expression)
            && MESSAGE_SETTER.test(node.expression.text)) {
            for (const argument of node.arguments) {
                for (const found of literalsOf(argument)) {
                    if (isLabelText(found.text) && /\s/.test(found.text)) {
                        report(`setter:${node.expression.text}`, found.text, found.pos);
                    }
                }
            }
        }

        ts.forEachChild(node, visit);
    };

    visit(file);
    return findings;
}

/** Den ganzen Chrome-Satz scannen. */
export function scanChrome(files = CHROME_FILES, root = ROOT) {
    return files.flatMap((entry) => scanFile(entry, root));
}
