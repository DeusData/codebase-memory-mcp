#!/usr/bin/env node
/*
 * W12-Smoke: jeder Knopf wird angefasst, keiner darf sich davor druecken.
 *
 * ## Was dieser Lauf anders macht als die zwoelf davor
 *
 * Die bestehenden Beweislaeufe klappern ein Drehbuch ab: sie wissen, welchen
 * Knopf sie druecken, und pruefen, ob dahinter das Richtige steht. Keiner von
 * ihnen kann sagen, ob etwas VERGESSEN wurde, und genau daher kamen alle
 * Befunde der Sorte "der Knopf macht nichts": vom Nutzer, nicht aus einem Lauf.
 *
 * Dieser Lauf hat kein Drehbuch. Er stellt Zustaende her, SUCHT SICH SELBST
 * ZUSAMMEN, was darin bedienbar ist, und fasst jedes gefundene Element an,
 * einmal mit der Maus und einmal mit der Tastatur. Was er nicht bedienen kann,
 * meldet er; was nichts tut, ist ein Befund.
 *
 * ## Die fuenf Kunstgriffe
 *
 * 1. **Die Liste der Bedienelemente steht nicht in dieser Datei.** Gesammelt
 *    wird ueber die Gattung (`button`, `[role="button"]`, `a[href]`, `input`,
 *    `select`, `[tabindex]:not([tabindex="-1"])`, `[role="separator"]`,
 *    `[role="tab"]`), in jedem Zustand neu. Eine Liste im Quelltext waere eine
 *    Erwartung, die gruen bleibt, wenn das Produkt einen Knopf dazubekommt.
 *
 * 2. **Was sich von selbst bewegt, wird vorher gemessen.** Eine Bildrate, ein
 *    Zaehler, eine Uhr: es gibt Werte, die sich ohne Zutun aendern, und ein
 *    Lauf, der sie mitzaehlt, findet ueberall Wirkung. Vor dem ersten Klick
 *    wird darum in JEDEM Zustand zweimal dasselbe gemessen, ohne etwas zu tun;
 *    was sich dabei unterscheidet, ist fluechtig und faellt aus dem Vergleich.
 *    Welche Schluessel das waren, steht im Artefakt.
 *
 * 3. **Ein Element, das schon an ist, bekommt eine zweite Gelegenheit.** Ein
 *    Klick auf den bereits gewaehlten Reiter aendert nichts, und das ist kein
 *    Fehler des Reiters. Zeigt ein Element seinen Zustand (`aria-pressed`,
 *    `aria-selected`, `data-on`, `data-active`) und steht es auf "an", geht der
 *    Lauf ueber einen Nachbarn derselben Gruppe weg und kommt zurueck. Erst
 *    wenn auch das nichts bewirkt, ist es ein Befund.
 *
 * 4. **Die Tastatur nimmt den Weg, den die Rolle vorsieht.** Ein Knopf hoert
 *    auf die Leertaste, ein Trenner (`role="separator"`) auf die Pfeiltasten,
 *    ein Regler auch, eine Zeile im Explorer ueber den Cursor der Liste, eine
 *    Trefferzeile ueber die Kommandozeile. "Enter oder Leertaste" auf einem
 *    Trenner waere die Pruefung einer Zusicherung, die niemand gegeben hat.
 *    Welche Taste gewirkt hat, steht je Element im Artefakt.
 *
 * 5. **Ein Filter muss WEGNEHMEN.** Fuer jeden Schalter, der etwas ein- oder
 *    ausblendet, genuegt nicht, dass sich irgendetwas aendert: gezaehlt wird,
 *    was vorher und nachher dasteht (Zeilen, Kanten, Koerper, Namen), und
 *    Abschalten muss die Zahl senken, Einschalten sie zurueckbringen. Bleibt
 *    sie bei null, weil es nichts zu zeigen gibt, muss die Flaeche das sagen.
 *
 * ## Die Schleife
 *
 * Ein Aufruf ist eine ETAPPE und misst so viel, wie in seine Zeitgrenze passt;
 * er fuehrt seine Messung mit der vorhandenen verification/w12/buttons.json
 * zusammen. Eine RUNDE ist etwas anderes: ein vollstaendiger Durchgang durch
 * alle Zustaende und alle Filter, aus beliebig vielen Etappen zusammengesetzt.
 * Ein Zyklus ist fertig, wenn zwei Durchgaenge hintereinander null Befunde
 * melden; `newFindings` ist darum die Zahl der Befunde DIESES Durchgangs und
 * nicht die Zahl der neu hinzugekommenen. Der Unterschied ist der ganze Sinn:
 * ein Durchgang, der denselben Befund noch einmal findet, hat nichts geloest.
 * Die Regeln der Etappen stehen weiter unten, bei {@link STAGE_MS}.
 *
 * ## Was dieser Lauf NICHT misst
 *
 * Ob die Wirkung eines Knopfes die RICHTIGE ist. Dass der Reiter "flow" den
 * Flow zeigt und nicht den Chat, prueft smoke-w8b; dass die Fuenf-Stufen des
 * Reglers verschiedene Texte ergeben, prueft smoke-w13. Dieser Lauf prueft
 * Vollstaendigkeit: dass jedes Bedienelement erreichbar ist, dass es antwortet,
 * und dass ein Filter filtert. Er ersetzt keinen der anderen.
 *
 * ## Ablauf
 *
 *   1. `npm run build`
 *   2. isoliertes HOME, eigenes Rendezvous-Verzeichnis (CBM_RUNTIME_DIR)
 *   3. fixtures/atlas-sample indizieren (die Fixture wird nur gelesen)
 *   4. C-Server auf einem freien Port >= 4580, dist/ auf einem zweiten,
 *      die Agenten-Bruecke im Wiedergabemodus auf einem dritten
 *   5. Chromium ohne Aussenwelt, plus Route-Sperre und Sidecar-Stub
 *      a. je Zustand: herstellen, fotografieren, Lesbarkeit messen,
 *         Bedienelemente sammeln, Tab-Reihenfolge abgehen
 *      b. je neues Element: Maus, wiederherstellen, Tastatur, wiederherstellen
 *      c. die Filter, jeder mit seiner Zahl vorher und nachher
 *   6. abraeumen, Restprozesse mehrfach zaehlen, JSON, Bilder, buttons.md
 *
 * Das Ganze laeuft unter dem Netz-Deny-Gate (npm run smoke:w12).
 *
 * ## Ports
 *
 * Ab 4580. 4141 gehoert dem Modell-Sidecar des Nutzers, 4142 seiner
 * Agenten-Bruecke, 4390/4391 und 4392/4393 seinen zwei Vorschauen; alles
 * darunter gehoert den Laeufen davor. Dieser Lauf fasst keinen davon an: der
 * Sidecar wird IM BROWSER beantwortet (Route-Griff, keine Verbindung), die
 * Bruecke bekommt einen eigenen Port.
 */

import { spawn } from 'node:child_process';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { mkdir, mkdtemp, readdir, rm, stat, writeFile } from 'node:fs/promises';
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
const EVENTS = join(ROOT, 'fixtures', 'agent-events', 'w11b-replay.jsonl');
const PROJECT = 'codeatlasweb-w12';
const OUT_DIR = join(ROOT, 'verification', 'w12');
const SHOT_DIR = join(OUT_DIR, 'states');
const OUT_JSON = join(OUT_DIR, 'buttons.json');
const OUT_MD = join(OUT_DIR, 'buttons.md');

/**
 * Contract: alles darunter gehoert dem Nutzer oder frueheren Laeufen.
 *
 * Der Contract verlangt 4580 und aufwaerts; genommen wird 4660, weil 4580 schon
 * smoke-w10 gehoert, 4600 smoke-w13, 4620 smoke-w11a und 4640 smoke-w10b. Zwei
 * Laeufe mit derselben Untergrenze finden dieselben freien Ports, und der
 * einzige Grund, dass das bisher gutging, ist, dass sie nie gleichzeitig
 * liefen. Das ist keine Eigenschaft, auf die man sich verlassen sollte.
 */
const MIN_PORT = 4660;

const VIEWPORT = { width: 1680, height: 1050 };

/** Der Modellport des Nutzers. Wird beantwortet, nie verbunden, nie gestartet. */
const SIDECAR_ORIGIN = 'http://127.0.0.1:4141';

/** Das Symbol, an dem gemessen wird. Begruendung wie in smoke-w13: hier ist jede Faktenfamilie gefuellt. */
const MAIN = { name: 'createUser', file: 'src/services/userService.ts' };

/** Die Datei, die der Zustand "offene Datei" oeffnet. */
const FILE = 'src/services/userService.ts';

/**
 * Die Gattungen, aus denen sich ein Bedienelement zusammensetzt (AC1).
 *
 * Woertlich die Liste des Contracts, als eine Zeichenkette fuer
 * `querySelectorAll`. Sie steht hier und nicht im Browser-Skript, damit sie im
 * Artefakt genannt werden kann: ein Leser soll sehen, WONACH gesucht wurde.
 */
const CONTROL_SELECTOR = [
    'button',
    '[role="button"]',
    'a[href]',
    'input',
    'select',
    '[tabindex]:not([tabindex="-1"])',
    '[role="separator"]',
    '[role="tab"]',
].join(', ');

/**
 * Wo nicht gesammelt wird, und warum.
 *
 * Dieselbe Ausnahme wie im Lesbarkeits-Gate und aus demselben Grund: der Editor
 * baut sein Innenleben selbst, samt versteckter Eingabeflaechen und eigener
 * Tastaturwelt. Sein Inneres gegen die Bedienregeln dieses Produkts zu pruefen
 * hiesse, eine fremde Bibliothek zu pruefen. Die Flaeche des Editors wird
 * trotzdem abgegangen: die Tab-Wanderung laeuft durch ihn hindurch, damit eine
 * Tastaturfalle darin nicht unbemerkt bliebe.
 */
const COLLECT_EXCLUSIONS = [
    {
        selector: '.monaco-editor',
        reason:
            'Der Editor baut sein Innenleben selbst (versteckte Eingabeflaeche, eigene '
            + 'Tastaturbindungen, eigene Ebenen). Gesammelt werden seine Nachbarn; die '
            + 'Tab-Wanderung geht trotzdem durch ihn hindurch, damit eine Falle darin auffiele.',
    },
    {
        selector: '[data-testid="atlas-hint"]',
        reason:
            'Der Tooltip ist keine Bedienung, sondern die Auskunft ueber eine. Er oeffnet beim '
            + 'Beruehren und beim Fokussieren und waere in jedem Zustand ein anderes Element.',
    },
];

/** Chromium ohne Aussenwelt, wortgleich mit smoke-w11b und smoke-w13. */
const CHROMIUM_ARGS = [
    '--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1, EXCLUDE localhost',
    '--disable-background-networking',
    '--disable-component-update',
    '--disable-domain-reliability',
    '--disable-quic',
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

const log = (...parts) => console.log('[smoke-w12]', ...parts);
const serverLog = [];

function run(command, args) {
    return new Promise((done) => {
        const child = spawn(command, args, {
            cwd: ROOT,
            env: {
                ...process.env,
                NO_UPDATE_NOTIFIER: '1',
                npm_config_update_notifier: 'false',
                npm_config_audit: 'false',
                npm_config_fund: 'false',
            },
            stdio: ['ignore', 'pipe', 'pipe'],
        });
        let out = '';
        child.stdout.on('data', (d) => { out += d.toString(); });
        child.stderr.on('data', (d) => { out += d.toString(); });
        child.on('error', (error) => done({ code: 127, out: out + error.message }));
        child.on('close', (code) => done({ code: code ?? 1, out }));
    });
}

// ----------------------------------------------------------------- Bruecke ---

/** Die Agenten-Bruecke im Wiedergabemodus. Eigener Port, nie der des Nutzers. */
async function startBridge(port, sink) {
    const child = spawn(process.execPath, [
        join(ROOT, 'tools', 'agent-bridge.mjs'),
        '--replay', EVENTS,
        '--port', String(port),
    ], { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout.on('data', (d) => sink.push(`[bridge ${port}] ${d.toString().trimEnd()}`));
    child.stderr.on('data', (d) => sink.push(`[bridge ${port}] ${d.toString().trimEnd()}`));
    let exited = null;
    child.on('exit', (code, signal) => { exited = { code, signal }; });

    const deadline = Date.now() + 15000;
    while (Date.now() < deadline) {
        if (exited !== null) {
            throw new Error(`die Bruecke endete vorzeitig (code=${exited.code})\n${sink.join('\n')}`);
        }
        try {
            const response = await fetch(`http://127.0.0.1:${port}/health`);
            const body = await response.json();
            if (response.status === 200) {
                return { child, health: body };
            }
        } catch {
            // lauscht noch nicht
        }
        await sleep(150);
    }
    child.kill('SIGKILL');
    throw new Error(`die Bruecke war binnen 15000 ms nicht auf ${port} bereit`);
}

async function stopBridge(child) {
    if (child === null || child === undefined || child.exitCode !== null) {
        return;
    }
    child.kill('SIGTERM');
    for (let i = 0; i < 40; i += 1) {
        if (child.exitCode !== null || child.signalCode !== null) {
            return;
        }
        await sleep(100);
    }
    child.kill('SIGKILL');
    await sleep(300);
}

// ------------------------------------------------------- Griffe in der Seite --

/**
 * Das Skript, das im Browser lebt.
 *
 * Es wird einmal je Seite eingespielt und haengt seine Funktionen an
 * `window.__w12`. Der Weg ueber `addInitScript` statt ueber `page.evaluate` bei
 * jedem Aufruf ist kein Geschmack: die Funktionen halten den Vergleichszustand
 * (die zuletzt gemessenen Stile), und ein Skript, das bei jedem Aufruf neu
 * entsteht, haette bei jedem Aufruf ein leeres Gedaechtnis.
 */
const PAGE_SCRIPT = `
(() => {
    const IDENT_ATTRS = [
        'data-testid', 'data-menu', 'data-tab', 'data-facet', 'data-option', 'data-switch',
        'data-type', 'data-mode', 'data-split', 'data-profile', 'data-setting', 'data-effect',
        'data-path', 'data-fold-of', 'data-example', 'data-factpath', 'data-entry', 'data-actor',
        'data-name', 'data-qn', 'data-kind', 'data-block-name', 'data-source',
        'name', 'type', 'role', 'aria-label', 'id',
    ];

    /* Werte, die sich ohne Zutun aendern koennen, gehoeren nicht in einen Selektor. */
    const clean = (value) => (value ?? '').replace(/\\s+/g, ' ').trim();

    const usableAttr = (value) =>
        value !== null && value.length > 0 && value.length < 90
        && !value.includes('"') && !value.includes('\\\\') && !value.includes('\\n');

    const selectorOf = (node) => {
        const parts = [node.tagName.toLowerCase()];
        for (const attr of IDENT_ATTRS) {
            const value = node.getAttribute(attr);
            if (usableAttr(value)) {
                parts.push('[' + attr + '="' + value + '"]');
            }
        }
        return parts.join('');
    };

    const place = (node) => {
        const marks = [];
        let current = node.parentElement;
        while (current !== null && current !== document.body && marks.length < 2) {
            const mark = current.getAttribute('data-testid');
            if (mark !== null && mark.length > 0) {
                marks.unshift(mark);
            }
            current = current.parentElement;
        }
        return marks.length > 0 ? marks.join(' > ') : 'atlas-shell';
    };

    const labelOf = (node) => {
        const aria = clean(node.getAttribute('aria-label'));
        const text = clean(node.textContent);
        /* textContent klebt die kleinen Bestandteile einer Zeile zusammen
         * (35toUseruserService.ts:9). Die sichtbaren Kinder haben bereits
         * ihre eigene Trennung; fuer den Menschenbericht setzen wir sie mit
         * Leerzeichen wieder zusammen, ohne Selector oder Rohidentitaet zu
         * veraendern. */
        const childText = [...node.childNodes]
            .map((child) => clean(child.textContent))
            .filter((part) => part.length > 0)
            .join(' ');
        const readable = (node.hasAttribute('data-menu') ? text : (childText.length > 0 ? childText : text))
            .replace(/(\\[[^\\]]+\\])\\s+([A-Za-z])/g, '$1$2')
            .replace(/\\/\\s+(\\d)/g, '/$1');
        const value = node.tagName === 'INPUT' ? clean(node.getAttribute('placeholder')) : '';
        const mark = clean(node.getAttribute('data-testid'));
        /* Ein aria-Name benennt die Bedienung selbst (etwa Evidence oder
         * close userService.ts) und hat daher Vorrang vor einer dekorativen
         * Sichtbeschriftung oder einem riesigen Containertext. */
        return (aria.length > 0 ? aria
            : (readable.length > 0 ? readable : value.length > 0 ? value : mark))
            .slice(0, 70);
    };

    const excluded = (node, selectors) =>
        selectors.some((selector) => node.closest(selector) !== null);

    const shown = (node) => {
        for (let current = node; current !== null; current = current.parentElement) {
            if (current.hasAttribute('inert')) {
                return false;
            }
        }
        const rect = node.getBoundingClientRect();
        if (rect.width <= 0 || rect.height <= 0) {
            return false;
        }
        if (rect.bottom <= 0 || rect.top >= window.innerHeight) {
            return false;
        }
        if (rect.right <= 0 || rect.left >= window.innerWidth) {
            return false;
        }
        const style = window.getComputedStyle(node);
        return style.display !== 'none' && style.visibility !== 'hidden'
            && Number(style.opacity) >= 0.05;
    };

    /*
     * Der sichtbare Anker eines Griffs.
     *
     * button[type=button] ist fuer die Twin-Zeilen absichtlich generisch.
     * Nach einem React-Neuzeichnen kann seine globale Nummer aber zu einer
     * anderen Zeile gehoeren. Text und der naechste benannte Bereich stammen
     * aus dem echten DOM und bleiben mit der Zeile verbunden; die Nummer ist
     * nur noch der Gleichstandsloeser unter wirklich gleichen Nachbarn.
     */
    const anchorOf = (node) => {
        const context = [];
        let current = node.parentElement;
        while (current !== null && current !== document.body && context.length < 3) {
            const mark = clean(current.getAttribute('data-testid'));
            const source = clean(current.getAttribute('data-source'));
            const fact = clean(current.getAttribute('data-factpath'));
            if (mark.length > 0 || source.length > 0 || fact.length > 0) {
                context.push([mark, source, fact].join(':'));
            }
            current = current.parentElement;
        }
        return {
            text: clean(node.textContent).slice(0, 240),
            label: labelOf(node),
            context: context.join(' > '),
        };
    };

    /** Der Beschreiber eines Elements: Selektor plus sichtbarer Anker. */
    const describe = (node) => {
        const selector = selectorOf(node);
        const all = [...document.querySelectorAll(selector)];
        /* Nur der absichtlich globale Twin-Griff braucht einen stabilen
         * sichtbaren Anker. Fuer spezifische Selektoren (etwa Baumzeilen)
         * ist ihre echte DOM-Position bereits die Identitaet; ein Anker ueber
         * ihren wechselnden aufgeklappten Text wuerde dort selbst Befunde
         * erzeugen. */
        const genericButton = selector === 'button[type="button"]';
        const anchor = genericButton ? anchorOf(node) : null;
        const anchored = genericButton ? all.filter((candidate) => {
            const other = anchorOf(candidate);
            return other.text === anchor.text && other.label === anchor.label && other.context === anchor.context;
        }) : [];
        const rect = node.getBoundingClientRect();
        /* Flow-Zeilen legen ihren aktiven Zustand am li ab, der Button darin
         * ist nur die Bedienflaeche. Fuer die Gegenprobe zaehlt deshalb der
         * naechste, sichtbare Zustandsanker genauso wie ein Attribut direkt
         * am Button. */
        const stateOwner = node.closest('[data-active], [data-on], [aria-selected], [aria-pressed]');
        return {
            selector,
            nth: all.indexOf(node),
            anchor: anchor === null ? undefined : { ...anchor, ordinal: anchored.indexOf(node) },
            semantic: node.getAttribute('data-testid') === 'atlas-entry-row' ? 'entry-row'
                : (node.closest('.atlas-bugwizard-chain') !== null
                    && node.classList.contains('atlas-bugwizard-link') ? 'bug-hop' : ''),
            tag: node.tagName.toLowerCase(),
            role: node.getAttribute('role') ?? '',
            type: node.getAttribute('type') ?? '',
            label: labelOf(node),
            place: place(node),
            tabIndex: node.tabIndex,
            /* Ein natives disabled bekommt weder Maus noch Tastatur. Ein
             * aria-disabled bleibt dagegen absichtlich bedienbar: manche
             * Flaechen erklaeren ihre Nichtverfuegbarkeit erst als sichtbare
             * Reaktion auf diese echte Aktivierung (Galaxy/Hierarchy). */
            disabled: node.hasAttribute('disabled'),
            ariaDisabled: node.getAttribute('aria-disabled') === 'true',
            pressed: node.getAttribute('aria-pressed') ?? node.getAttribute('aria-selected')
                ?? node.getAttribute('data-on') ?? node.getAttribute('data-active')
                ?? stateOwner?.getAttribute('aria-pressed') ?? stateOwner?.getAttribute('aria-selected')
                ?? stateOwner?.getAttribute('data-on') ?? stateOwner?.getAttribute('data-active') ?? '',
            rect: {
                x: Math.round(rect.x), y: Math.round(rect.y),
                width: Math.round(rect.width), height: Math.round(rect.height),
            },
        };
    };

    const resolve = (descriptor) => {
        const all = document.querySelectorAll(descriptor.selector);
        if (descriptor.anchor === undefined) {
            return all[descriptor.nth] ?? null;
        }
        const matching = [...all].filter((node) => {
            const anchor = anchorOf(node);
            return anchor.text === descriptor.anchor.text
                && anchor.label === descriptor.anchor.label
                && anchor.context === descriptor.anchor.context;
        });
        if (matching.length === 1) {
            return matching[0];
        }
        /* Gleiche sichtbare Zeilen bekommen ihre Position nur innerhalb
         * derselben sichtbaren Ankerfamilie, nie mehr innerhalb aller Buttons
         * der Seite. Gibt es den Anker nicht, ist das Element wirklich weg. */
        return matching[descriptor.anchor.ordinal] ?? null;
    };

    let claimSerial = 0;
    const claim = (descriptor) => {
        const node = resolve(descriptor);
        if (node === null) {
            return '';
        }
        const token = 'w12-' + (++claimSerial);
        node.setAttribute('data-w12-claim', token);
        return token;
    };
    const release = (token) => {
        const node = document.querySelector('[data-w12-claim="' + token + '"]');
        node?.removeAttribute('data-w12-claim');
    };

    const activations = new Map();
    const armActivation = (descriptor) => {
        const node = resolve(descriptor);
        if (node === null) {
            return '';
        }
        const token = 'w12-activation-' + (++claimSerial);
        const beforeMessage = clean(document.querySelector('[data-testid="atlas-bugwizard-message"]')?.textContent);
        const record = { event: null, beforeMessage, messageTransitions: [], lastMessage: beforeMessage };
        const observeMessage = () => {
            const next = clean(document.querySelector('[data-testid="atlas-bugwizard-message"]')?.textContent);
            if (next !== record.lastMessage) {
                record.messageTransitions.push(next);
                record.lastMessage = next;
            }
        };
        record.observer = new MutationObserver(observeMessage);
        record.observer.observe(document.body, { childList: true, subtree: true, characterData: true });
        activations.set(token, record);
        node.addEventListener('click', (event) => {
            record.event = {
                trusted: event.isTrusted === true,
                detail: event.detail,
                targetMatches: event.target === node || node.contains(event.target),
                beforeMessage,
            };
        }, { capture: true, once: true });
        return token;
    };
    const activation = (token) => activations.get(token) ?? null;
    const releaseActivation = (token) => {
        const record = activations.get(token);
        record?.observer?.disconnect();
        activations.delete(token);
    };
    const fingerprintTransitions = new Map();
    const armFingerprintTransition = (before, volatileKeys) => {
        const token = 'w12-transition-' + (++claimSerial);
        const record = { mutationCount: 0, changed: new Set() };
        record.observer = new MutationObserver(() => {
            record.mutationCount += 1;
            const after = fingerprint();
            for (const key of new Set([...Object.keys(before), ...Object.keys(after)])) {
                if (!volatileKeys.includes(key) && before[key] !== after[key]) record.changed.add(key);
            }
        });
        record.observer.observe(document.body, { childList: true, subtree: true, characterData: true, attributes: true });
        fingerprintTransitions.set(token, record);
        return token;
    };
    const finishFingerprintTransition = (token) => {
        const record = fingerprintTransitions.get(token);
        record?.observer?.disconnect();
        fingerprintTransitions.delete(token);
        return record === undefined ? { mutationCount: 0, changed: [] }
            : { mutationCount: record.mutationCount, changed: [...record.changed] };
    };

    const bugHopEvidence = (descriptor, token) => {
        const armed = activation(token);
        const event = armed?.event ?? null;
        const panel = document.querySelector('[data-testid="atlas-bugwizard"]');
        const target = clean(panel?.getAttribute('data-target'));
        const status = clean(panel?.getAttribute('data-status'));
        const message = clean(document.querySelector('[data-testid="atlas-bugwizard-message"]')?.textContent);
        const twin = clean(globalThis.__atlasTwin?.symbol);
        const expected = clean(descriptor.label);
        const expectedMessage = 'the index does not resolve "' + expected + '", so there is nothing to open.';
        const resolved = status === 'ready' && (target === expected || twin === expected);
        /* Ein nicht aufloesbarer Hop ist nur dann eine echte Wirkung, wenn
         * seine sichtbare, nach dem trusted Event neu gesetzte Meldung den
         * angeforderten Namen traegt. So zaehlt src/server.ts nicht bloss
         * wegen eines alten Paneltexts als behandelt. */
        const transitions = armed?.messageTransitions ?? [];
        const clearedAfterEvent = event !== null && transitions.some((entry) => entry.length === 0);
        const startedClear = event?.beforeMessage.length === 0;
        const reachedExpected = transitions.includes(expectedMessage);
        const unresolved = message === expectedMessage && (clearedAfterEvent || (startedClear && reachedExpected));
        if (event !== null && (resolved || unresolved)) {
            armed.observer?.disconnect();
        }
        return { event, target, twin, status, message, expected, expectedMessage, transitions, startedClear, clearedAfterEvent,
            resolved, unresolved, equivalent: resolved || unresolved };
    };

    /** Ein billiger Hash ueber eine Zeichenkette. Nur zum Vergleichen. */
    const hash = (text) => {
        let value = 5381;
        for (let i = 0; i < text.length; i += 1) {
            value = ((value * 33) ^ text.charCodeAt(i)) >>> 0;
        }
        return value.toString(36);
    };

    const seamPick = (source, keys) => {
        const out = {};
        if (source === undefined || source === null) {
            return out;
        }
        for (const key of keys) {
            const value = source[key];
            if (value === undefined || typeof value === 'function') {
                continue;
            }
            out[key] = typeof value === 'object' ? JSON.stringify(value) : String(value);
        }
        return out;
    };

    /*
     * Der Abdruck einer Seite: die Schluessel, an denen sich eine Wirkung zeigt.
     *
     * Nicht enthalten und mit Absicht: der Tooltip (er oeffnet beim Beruehren
     * und beim Fokussieren, also bei JEDER Betaetigung, und waere damit die
     * Wirkung jedes Elements), das fokussierte Element (der Fokus wandert beim
     * Anfassen, das ist keine Wirkung des Elements) und die Bildlaufstaende
     * (ein Element wird vor dem Anfassen in den Blick geholt).
     */
    const fingerprint = () => {
        const out = {};
        const outsideHint = (node) => node.closest('[data-testid="atlas-hint"]') === null;
        out.url = location.href;
        out.nodes = String([...document.querySelectorAll('*')].filter(outsideHint).length);

        const marks = {};
        for (const node of document.querySelectorAll('[data-testid]')) {
            if (!outsideHint(node)) continue;
            const mark = node.getAttribute('data-testid');
            marks[mark] = (marks[mark] ?? 0) + 1;
        }
        out.marks = JSON.stringify(Object.entries(marks).sort());

        const stateAttrs = [
            'data-on', 'data-active', 'data-state', 'data-open', 'data-selected', 'data-value',
            'data-mode', 'data-hidden', 'data-enabled', 'data-fold', 'data-visible', 'data-level',
            'data-status', 'data-tab', 'data-option', 'data-coverage', 'data-cursor',
            'data-copying', 'data-copied',
            'aria-pressed', 'aria-selected', 'aria-expanded', 'aria-valuenow',
        ];
        const states = [];
        for (const node of document.querySelectorAll('[data-testid], [data-menu], [data-facet], [data-switch], [role]')) {
            if (!outsideHint(node)) continue;
            const bits = [];
            for (const attr of stateAttrs) {
                const value = node.getAttribute(attr);
                if (value !== null) {
                    bits.push(attr + '=' + value);
                }
            }
            if (bits.length > 0) {
                states.push((node.getAttribute('data-testid') ?? node.tagName) + ':' + bits.join(','));
            }
        }
        out.states = hash(states.join('|')) + '#' + states.length;

        const body = document.body.cloneNode(true);
        for (const node of body.querySelectorAll('[data-testid="atlas-hint"]')) {
            node.remove();
        }
        const text = clean(body.textContent);
        out.text = hash(text) + '#' + text.length;

        /*
         * Was in den Feldern steht.
         *
         * Eigener Schluessel, weil der Wert eines Eingabefeldes weder im Text
         * der Seite noch in einem Attribut steht: er lebt allein im
         * Objektzustand. Ohne ihn waere ein Feld, in das man tippt und in dem
         * das Getippte stehenbleibt, ein Element ohne messbare Wirkung, und der
         * Lauf haette einen Befund gemeldet, den es nicht gibt.
         */
        const values = [];
        for (const node of document.querySelectorAll('input, textarea, select')) {
            values.push((node.getAttribute('data-testid') ?? node.name ?? node.type)
                + '=' + String(node.value ?? ''));
        }
        out.values = values.join('|');

        const storage = [];
        for (let i = 0; i < localStorage.length; i += 1) {
            const key = localStorage.key(i);
            storage.push(key + '=' + localStorage.getItem(key));
        }
        out.storage = hash(storage.sort().join('|'));

        const seams = {
            twin: seamPick(globalThis.__atlasTwin, [
                'symbol', 'level', 'levelName', 'mode', 'view', 'flowOpen', 'flowStep',
                'sectionNames', 'voiceState',
            ]),
            galaxy: seamPick(globalThis.__atlasGalaxy, [
                'mode', 'open', 'legendOpen', 'drawnEdges', 'hiddenKinds', 'nodes', 'labelBoxes',
                'fits', 'hierarchyAvailable', 'bloom',
            ]),
            layout: seamPick(globalThis.__atlasLayout, [
                'sizes', 'requested', 'explainOpen', 'explainTab', 'isDefault',
            ]),
            search: seamPick(globalThis.__atlasSearch, [
                'currentQuery', 'shownQuery', 'shownRows', 'activatedMenus',
            ]),
            llm: seamPick(globalThis.__atlasLlm, ['state', 'preferenceOn', 'model']),
            settings: seamPick(globalThis.__atlasSettings, ['open', 'selectedModel']),
            agents: seamPick(globalThis.__atlasAgents, [
                'on', 'layerOn', 'filter', 'size', 'follow', 'trails', 'fullscreen', 'shown',
                'drawn', 'trailWindowMs', 'effects',
            ]),
            /* Die Grenzen eines Live-Zeitfensters wandern von selbst. Sein
             * Modus nicht: pause/live/replay ist die echte sichtbare Wirkung
             * des Pausenknopfs und darf daher nie volatil werden. */
            agentTimeline: seamPick(globalThis.__atlasAgents?.timeline, ['mode']),
            chat: seamPick(globalThis.__atlasChat, ['open', 'depth', 'refineState']),
            reader: {
                ...seamPick(globalThis.__atlasReader?.document, ['path', 'lineCount']),
                /* Pseudocode- und Twin-Zeilen springen innerhalb derselben
                 * Datei. Der Pfad bleibt dabei gleich; die wirkliche Wirkung
                 * ist die vom Reader selbst gehaltene Caret-Zeile. */
                caretLine: String(globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? ''),
            },
            flow: seamPick(globalThis.__atlasFlow, ['step', 'steps']),
            tour: seamPick(globalThis.__atlasTour, ['id', 'index', 'steps', 'title']),
            checklist: seamPick(globalThis.__atlasChecklist, ['marks', 'label', 'symbol']),
        };
        for (const [name, values] of Object.entries(seams)) {
            for (const [key, value] of Object.entries(values)) {
                out['seam.' + name + '.' + key] = value;
            }
        }
        return out;
    };

    /**
     * Die Stile, an denen ein Fokusring zu sehen waere.
     *
     * Das Element selbst, seine beiden Pseudo-Elemente und seine ersten Kinder:
     * diese Oberflaeche zeigt den Fokus mal als Umriss, mal als Rahmenfarbe,
     * mal ueber einen Strich in einem ::after (die Griffe zwischen den Zonen),
     * mal an einem Kind (die Zeile unter dem Cursor des Explorers). Eine Regel,
     * die nur outline liest, wuerde vier von fuenf Faellen fuer unsichtbar
     * halten.
     */
    const FOCUS_PROPS = [
        'outlineStyle', 'outlineWidth', 'outlineColor', 'outlineOffset', 'boxShadow',
        'borderTopColor', 'borderBottomColor', 'borderLeftColor', 'borderRightColor',
        'borderTopWidth', 'borderBottomWidth', 'backgroundColor', 'color', 'textDecorationLine',
        'filter', 'opacity', 'transform',
    ];

    const styleOf = (node) => {
        const read = (target, pseudo) => {
            const style = window.getComputedStyle(target, pseudo);
            return FOCUS_PROPS.map((prop) => style[prop]).join('|')
                + '|' + (pseudo === null ? '' : style.content + '|' + style.background);
        };
        const parts = [read(node, null), read(node, '::before'), read(node, '::after')];
        let index = 0;
        for (const child of node.querySelectorAll('*')) {
            if (index >= 8) {
                break;
            }
            parts.push(read(child, null));
            index += 1;
        }
        /*
         * Und die zwei naechsten Vorfahren.
         *
         * Nicht jede Flaeche zeichnet ihren Fokus an sich selbst: die
         * Kommandozeile faerbt ihren Prompt und ihren Rahmen ueber
         * data-focused am UMSCHLIESSENDEN Kasten, weil der Rahmen zur Zeile
         * gehoert und nicht zum Feld darin. Eine Messung, die nur das Element
         * liest, wuerde dort "kein Fokus zu sehen" melden und damit einen
         * Befund erfinden.
         */
        let up = node.parentElement;
        for (let level = 0; level < 2 && up !== null && up !== document.body; level += 1) {
            parts.push(read(up, null), read(up, '::before'));
            up = up.parentElement;
        }
        const list = node.closest('[role="tree"], [role="listbox"], [role="tablist"]');
        if (list !== null) {
            const cursor = list.querySelector('[data-cursor="true"], [data-selected="true"]');
            if (cursor !== null) {
                parts.push(read(cursor, null));
            }
        }
        return parts.join('#');
    };

    globalThis.__w12 = {
        describe,
        resolve,
        claim,
        release,
        armActivation,
        activation,
        releaseActivation,
        armFingerprintTransition,
        finishFingerprintTransition,
        bugHopEvidence,
        fingerprint,
        styleOf,
        selectorOf,
        labelOf,
        shown,
        excluded,
        clean,

        /** Alle sichtbaren Bedienelemente dieses Zustands, mit ihrem Beschreiber. */
        collect: (selector, exclusions) => {
            const out = [];
            for (const node of document.querySelectorAll(selector)) {
                if (excluded(node, exclusions) || !shown(node)) {
                    continue;
                }
                out.push(describe(node));
            }
            return out;
        },

        /** Der Stil eines Elements, unfokussiert. */
        restingStyle: (descriptor) => {
            const node = resolve(descriptor);
            return node === null ? null : styleOf(node);
        },

        /** Das Element in den Blick holen, damit ein Klick es trifft. */
        bring: (descriptor) => {
            const node = resolve(descriptor);
            if (node === null) {
                return false;
            }
            node.scrollIntoView({ block: 'nearest', inline: 'nearest' });
            return true;
        },

        /** Den Fokus setzen, ohne zu betaetigen. */
        focus: (descriptor) => {
            const node = resolve(descriptor);
            if (node === null) {
                return { found: false, focused: false };
            }
            node.focus({ preventScroll: false });
            return { found: true, focused: document.activeElement === node };
        },

        /** Wo der Fokus gerade steht. */
        active: () => {
            const node = document.activeElement;
            if (node === null || node === document.body) {
                return { body: true, selector: '', label: '', place: '', style: '' };
            }
            return {
                body: false,
                ...describe(node),
                style: styleOf(node),
                focusVisible: (() => {
                    try {
                        return node.matches(':focus-visible');
                    } catch {
                        return false;
                    }
                })(),
                inEditor: node.closest('.monaco-editor') !== null,
            };
        },

        /**
         * Den Nachbarn derselben Gruppe finden, der gerade NICHT gewaehlt ist.
         *
         * Eine gemeinsame, nichtleere Testmarke ist die engste ehrliche
         * Familie: die Zeitfenster teilen etwa nur
         * \`atlas-agents-timeline-window\`, waehrend der Pausenknopf daneben
         * dieselbe CSS-Klasse hat, aber keine Gegenlage ist. Fehlt eine solche
         * Familie (etwa bei individuell markierten Tabs), bleibt die bestehende
         * Klassen-Familie der strukturelle Fallback.
         */
        sibling: (descriptor) => {
            const node = resolve(descriptor);
            if (node === null) {
                return null;
            }
            const group = node.closest('[role="group"], [role="tablist"], [role="tree"], [role="listbox"]')
                ?? node.parentElement?.parentElement
                ?? node.parentElement;
            if (group === null) {
                return null;
            }
            const testId = (node.getAttribute('data-testid') ?? '').trim();
            const sameTestId = testId === '' ? [] : [...group.querySelectorAll('[data-testid]')]
                .filter((candidate) => candidate.getAttribute('data-testid') === testId);
            const klass = (node.getAttribute('class') ?? '').split(/\\s+/).filter(Boolean)[0];
            const family = sameTestId.length > 1
                ? sameTestId
                : [...group.querySelectorAll(
                    klass === undefined
                        ? node.tagName.toLowerCase()
                        : node.tagName.toLowerCase() + '.' + klass,
                )];
            for (const candidate of family) {
                if (candidate === node || !shown(candidate)) {
                    continue;
                }
                const on = candidate.getAttribute('aria-pressed')
                    ?? candidate.getAttribute('aria-selected')
                    ?? candidate.getAttribute('data-on')
                    ?? candidate.getAttribute('data-active');
                if (on === 'false' || on === null) {
                    return describe(candidate);
                }
            }
            return null;
        },

        /**
         * Der Satz, mit dem sich eine Flaeche fuer ihre Wirkungslosigkeit
         * erklaert.
         *
         * Gesucht wird IN DER SEITE und nicht in einer Liste dieses Laufs: eine
         * Begruendung, die nur der Lauf kennt, ist keine. Drei Formen zaehlen,
         * und alle drei stehen sichtbar da: der Grund im Feld unter einem
         * gesperrten Reiter, die Notizzeile eines Panels, und die Zeile, die
         * eine Sektion ueber ihre eigene Leere schreibt.
         */
        excuse: (descriptor) => {
            const node = resolve(descriptor);
            if (node === null) {
                return { found: false, text: '', where: '' };
            }
            const marked = node.getAttribute('aria-disabled') === 'true'
                || node.getAttribute('data-enabled') === 'false'
                || node.getAttribute('data-available') === 'false'
                || node.hasAttribute('disabled');
            /* Das Hint-System schreibt seine produktseitige Erklaerung direkt
             * an den markierten Griff. Das ist eine sichtbare Begruendung
             * dieser Flaeche, keine vom Harness geliehene Ausrede. */
            const hint = clean(node.getAttribute('data-hint'));
            if (marked && hint.length > 20) {
                return { found: true, text: hint.slice(0, 220), where: 'data-hint', marked };
            }
            const zones = [
                '[data-testid="atlas-explain-empty"]',
                '[data-testid="atlas-galaxy-note"]',
                '[data-testid="atlas-twin-empty"]',
                '[data-testid="atlas-agents-layer-off"]',
                '[data-testid="atlas-search-message"]',
                '[data-testid="atlas-settings-llm-off"]',
                '[data-testid="atlas-settings-not-running"]',
                '[data-testid="atlas-impact-no-project"]',
                '[data-testid="atlas-bugwizard-no-target"]',
                '[data-testid="atlas-flow-message"]',
                '[data-testid="atlas-pseudocode-note"]',
                '[data-testid="atlas-tree-truncation"]',
            ];
            /*
             * Gesucht wird NUR in der Flaeche, in der das Element steht.
             *
             * Eine Begruendung, die irgendwo sonst auf der Seite steht, ist
             * keine Begruendung fuer diesen Knopf: der Lauf wuerde sich sonst
             * jede beliebige Notizzeile borgen und damit genau die stille
             * Ausrede erzeugen, gegen die dieser Zyklus gebaut ist.
             */
            const panel = node.closest('section, aside, main, div[role="dialog"]') ?? document.body;
            for (const zone of zones) {
                for (const found of panel.querySelectorAll(zone)) {
                    const text = clean(found.textContent);
                    if (shown(found) && text.length > 20) {
                        return { found: true, text: text.slice(0, 220), where: zone, marked };
                    }
                }
            }
            return { found: false, text: '', where: '', marked };
        },

        /** Der Cursor der Baumliste, damit die Tastatur eine Zeile erreicht. */
        treeCursor: () => {
            const rows = [...document.querySelectorAll('[data-testid="atlas-tree-row"]')];
            return {
                rows: rows.length,
                at: rows.findIndex((row) => row.getAttribute('data-cursor') === 'true'),
            };
        },

        /** Wo die ausgewaehlte Trefferzeile steht. */
        searchCursor: () => {
            const rows = [...document.querySelectorAll('[data-testid="atlas-search-row"]')];
            return {
                rows: rows.length,
                at: rows.findIndex((row) => row.getAttribute('data-selected') === 'true'),
            };
        },

        /**
         * Der Zaehlwert eines Filters, gemessen an dem, was wirklich dasteht.
         *
         * twinRows zaehlt die Zeilen der FAKTEN-Ansicht (codeatlas-twin-*)
         * und nicht alles im Koerper. Das ist die eine Zahl, mit der sich beides
         * messen laesst: eine abgeschaltete Linse nimmt Zeilen weg, und der
         * Wechsel auf pseudocode nimmt sie alle weg. Zaehlte die Zahl auch die
         * Pseudocode-Zeilen mit, waere der Wechsel eine Aenderung ohne Richtung.
         */
        counts: () => {
            const finite = (value) => {
                const number = Number(value);
                return Number.isFinite(number) ? number : 0;
            };
            const labels = globalThis.__atlasGalaxy?.labelBoxes;
            return {
                twinRows: document.querySelectorAll(
                    '.atlas-twin-body [data-testid^="codeatlas-twin-"]',
                ).length,
                twinSections: (globalThis.__atlasTwin?.sectionNames ?? []).length,
                drawnEdges: finite(globalThis.__atlasGalaxy?.drawnEdges),
                /* Die Produktnaht gibt die wirklich gelegten Label-Boxen als
                 * Liste aus. Eine Liste ist kein Zaehlwert; ihre Laenge ist es. */
                labelBoxes: Array.isArray(labels) ? labels.length : finite(labels),
                agentRows: document.querySelectorAll('[data-testid="atlas-agents-row"]').length,
                agentBodies: document.querySelectorAll('[data-testid="atlas-agent-body"]').length,
                /* Die Ghosts gehoeren zum HUD. Die gezeichnete Spur ist allein
                 * die Renderreihenfolge aus dem AgentLayer. */
                agentTrails: finite(globalThis.__atlasAgents?.renderOrders?.trails),
                agentWaves: document.querySelectorAll('[data-testid="atlas-agent-wave"]').length,
                agentTails: Object.values(globalThis.__atlasAgents?.tails ?? {})
                    .filter((value) => Number(value) > 0).length,
                timeline: document.querySelectorAll('[data-testid="atlas-agents-timeline"]').length,
                pseudocodeLines: document.querySelectorAll('[data-testid="atlas-pseudocode-line"]').length,
            };
        },

        /** Die Lage der Anwendung, an der die Wiederherstellung sich orientiert. */
        world: () => {
            const attr = (selector, name) =>
                document.querySelector(selector)?.getAttribute(name) ?? '';
            const has = (selector) => document.querySelector(selector) !== null;
            return {
                loaded: has('[data-testid="atlas-statusbar"]'),
                tree: document.querySelectorAll('[data-testid="atlas-tree-row"]').length,
                file: globalThis.__atlasReader?.document?.path ?? '',
                symbol: globalThis.__atlasTwin?.symbol ?? '',
                twinStatus: attr('[data-testid="atlas-twin"]', 'data-status'),
                twinView: globalThis.__atlasTwin?.view ?? '',
                readerLevel: Number(document.querySelector('[data-testid="atlas-twin-depth"]')?.value ?? -1),
                help: has('[data-testid="atlas-help"]'),
                settings: has('[data-testid="atlas-settings"]'),
                entry: has('[data-testid="atlas-entry"]'),
                why: has('[data-testid="atlas-why"]'),
                search: has('[data-testid="atlas-search-results"]'),
                commandOverlay: has('[data-testid="atlas-search-headline"]'),
                command: document.querySelector('[data-testid="atlas-command-input"]')?.value ?? '',
                readerCaretLine: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? -1,
                explainOpen: attr('[data-testid="atlas-explain"]', 'data-open') === 'true',
                explainTab: attr('[data-testid="atlas-explain"]', 'data-tab'),
                galaxy: attr('[data-testid="atlas-galaxy"]', 'data-visible') === 'true',
                graphMode: attr('[data-testid="atlas-galaxy"]', 'data-mode'),
                legend: has('[data-testid="atlas-galaxy-legend"]'),
                llm: globalThis.__atlasLlm?.preferenceOn === true,
                llmState: globalThis.__atlasLlm?.state ?? '',
                agents: globalThis.__atlasAgents?.on === true,
                agentSource: globalThis.__atlasAgents?.sourceState ?? '',
                agentFilter: globalThis.__atlasAgents?.filter ?? '',
                agentLayer: globalThis.__atlasAgents?.layerOn === true,
                agentTrails: globalThis.__atlasAgents?.trails === true,
                /* Das HUD-Fenster der Spuren ist eine eigene Praeferenz; es
                 * ist nicht das Replay-Fenster des grossen Zeitstrahls. */
                agentTrailWindow: Number(globalThis.__atlasAgents?.trailWindowMs ?? -1),
                agentFullscreen: globalThis.__atlasAgents?.fullscreen === true,
                agentTimelineMode: attr('[data-testid="atlas-agents-timeline"]', 'data-mode'),
                agentTimelineWindow: Number(attr('[data-testid="atlas-agents-timeline"]', 'data-window') ?? -1),
                youActor: (globalThis.__atlasAgents?.actors ?? []).some((actor) => actor.you),
                foreignActor: (globalThis.__atlasAgents?.actors ?? []).some((actor) => !actor.you),
                walk: Number(globalThis.__atlasTour?.steps ?? 0),
                walkId: globalThis.__atlasTour?.id ?? '',
                walkIndex: Number(globalThis.__atlasTour?.index ?? -1),
                walkRootName: String(globalThis.__atlasTour?.titles?.[0] ?? '')
                    .replace(/^Start here:\s*/, '')
                    .trim(),
                walkRootPath: globalThis.__atlasTour?.paths?.[0] ?? '',
                flowStep: Number(globalThis.__atlasFlow?.step ?? -1),
                facets: [...document.querySelectorAll('.atlas-twin-facet')]
                    .map((node) => node.getAttribute('data-facet')
                        + '=' + (node.getAttribute('data-on') === 'true')),
                hiddenKinds: globalThis.__atlasGalaxy?.hiddenKinds ?? [],
                layoutDefault: globalThis.__atlasLayout?.isDefault === true,
                /*
                 * Die Darstellung steht im Speicher dieses Browsers und in
                 * keiner Naht. Sie von dort zu lesen ist derselbe Weg, den die
                 * Anwendung selbst nimmt (src/galaxy/density.ts, displayKey);
                 * ein zweiter Schluessel hier waere ein zweiter Ort, an dem er
                 * auseinanderlaufen kann.
                 */
                display: (() => {
                    try {
                        return JSON.parse(localStorage.getItem('atlas-display:' + PROJECT_NAME) ?? '{}');
                    } catch {
                        return {};
                    }
                })(),
                tabs: document.querySelectorAll('[data-testid="atlas-tab"]').length,
            };
        },
    };
})();
`;

/**
 * Die Tab-Wanderung, als eigene Funktion in der Seite.
 *
 * Sie steht nicht im Skript oben, weil sie den Tastendruck von aussen braucht:
 * ein synthetisches Tab-Ereignis bewegt den Fokus nicht, das macht nur der
 * Browser selbst.
 */

// ----------------------------------------------------------- kleine Helfer ---

const seamOf = (page, name) =>
    page.evaluate((key) => {
        const value = globalThis[key];
        return value === undefined
            ? null
            : JSON.parse(JSON.stringify(value, (_, entry) =>
                (typeof entry === 'function' ? undefined : entry)));
    }, name);

const worldOf = (page) => page.evaluate(() => globalThis.__w12.world());
const countsOf = (page) => page.evaluate(() => globalThis.__w12.counts());
const printOf = (page) => page.evaluate(() => globalThis.__w12.fingerprint());

/* Die Namen eines Zustands sind keine Behauptung: unmittelbar vor dem
 * Sammeln wird die wirklich sichtbare Lage gelesen und im Rohrecord
 * festgehalten. So kann kein Zustand ohne Symbol als Pseudocode-Zustand
 * vollstaendig werden. */
const coverageOf = (page) => page.evaluate(() => {
    const world = globalThis.__w12.world();
    const impact = document.querySelector('[data-testid="atlas-impact-mode-worktree"]');
    return {
        ...world,
        impactMode: impact === null ? ''
            : (impact.getAttribute('data-active') === 'true' ? 'worktree' : 'since-ref'),
        pseudocodeLines: document.querySelectorAll('[data-testid="atlas-pseudocode-line"]').length,
        searchRows: document.querySelectorAll('[data-testid="atlas-search-row"]').length,
    };
});

function coverageProblems(want, coverage) {
    const problems = [];
    const direct = [
        'file', 'symbol', 'twinView', 'readerLevel', 'help', 'settings', 'entry', 'why',
        'explainOpen', 'explainTab', 'galaxy', 'graphMode', 'legend', 'llm', 'agents',
        'agentFilter', 'agentTrails', 'agentTrailWindow',
        'agentTimelineMode', 'agentTimelineWindow',
        'flowStep', 'youActor', 'foreignActor', 'impactMode', 'layoutDefault',
        'walkIndex', 'walkRootName', 'walkRootPath',
    ];
    for (const key of direct) {
        if (want[key] !== undefined && coverage[key] !== want[key]) {
            problems.push(`${key}=${JSON.stringify(coverage[key])}, erwartet ${JSON.stringify(want[key])}`);
        }
    }
    if ((want.symbol ?? '') !== '' && coverage.twinStatus !== 'ready') {
        problems.push(`twinStatus=${JSON.stringify(coverage.twinStatus)}, erwartet "ready"`);
    }
    if (want.fullscreen !== undefined && coverage.agentFullscreen !== want.fullscreen) {
        problems.push(`fullscreen=${JSON.stringify(coverage.agentFullscreen)}, erwartet ${JSON.stringify(want.fullscreen)}`);
    }
    if (want.walk === true && coverage.walk <= 0) problems.push('walk=0, erwartet laufende Fuehrung');
    if (want.walk === true && coverage.walkId.length === 0) problems.push('walkId leer, erwartet dieselbe konkrete Fuehrung');
    if (want.walk === false && coverage.walk !== 0) problems.push(`walk=${coverage.walk}, erwartet 0`);
    if (want.pseudocodeBase === true && coverage.pseudocodeLines < 1) {
        problems.push(`pseudocodeLines=${coverage.pseudocodeLines}, erwartet mindestens 1`);
    }
    if ((want.search ?? '') !== '' && coverage.searchRows < 1) {
        problems.push(`searchRows=${coverage.searchRows}, erwartet mindestens 1`);
    }
    if ((want.search ?? '') === '' && coverage.command !== '') {
        problems.push(`command=${JSON.stringify(coverage.command)}, erwartet leere Kommandozeile`);
    }
    if (want.allKinds === true && coverage.hiddenKinds.length > 0) {
        problems.push(`hiddenKinds=${JSON.stringify(coverage.hiddenKinds)}, erwartet []`);
    }
    if (want.defaultFacets === true) {
        const expected = { logic: true, calls: true, data: true, errors: true, tests: true, runtime: false, changes: false };
        const actual = Object.fromEntries(coverage.facets.map((entry) => entry.split('=').map((part, index) =>
            index === 1 ? part === 'true' : part)));
        for (const [name, value] of Object.entries(expected)) {
            /* Nicht jede optionale, ausgeschaltete Linse wird gerendert. Ihre
             * Abwesenheit ist damit dieselbe wirksame Lage wie data-on=false;
             * eine erforderliche aktive Linse muss dagegen sichtbar sein. */
            const matches = value === true ? actual[name] === true : actual[name] !== true;
            if (!matches) problems.push(`facet ${name}=${JSON.stringify(actual[name])}, erwartet ${value}`);
        }
    }
    return problems;
}

async function requireCoverage(page, state, record) {
    const coverage = await coverageOf(page);
    record.coverage = coverage;
    /* Ein wiederaufgenommener Zustand wird gegen seine JETZT gemessene Lage
     * beurteilt. Ein alter roter Coverage-Schnappschuss darf nach einer
     * erfolgreichen Wiederherstellung nicht im Rohspeicher weiterleben. */
    record.findings = (record.findings ?? []).filter((entry) => entry.kind !== 'state-coverage');
    const problems = coverageProblems(state.want, coverage);
    if (problems.length > 0) {
        record.findings.push({
            kind: 'state-coverage',
            state: state.id,
            label: state.title,
            detail: problems.join('; '),
        });
        record.complete = false;
        return false;
    }
    return true;
}

/** Was sich zwischen zwei Abdruecken unterschieden hat, ohne die fluechtigen. */
function diffKeys(before, after, volatileKeys) {
    const keys = new Set([...Object.keys(before), ...Object.keys(after)]);
    const changed = [];
    for (const key of keys) {
        if (volatileKeys.has(key)) {
            continue;
        }
        if (before[key] !== after[key]) {
            changed.push(key);
        }
    }
    return changed.sort();
}

/** Warten, bis sich der Abdruck unterscheidet, hoechstens aber so lange. */
/*
 * Eine Bedienprobe braucht keine zweieinhalb Sekunden Schweigen.  Die
 * Oberflaeche ist lokal; alle zustandsaendernden Aktionen zeichnen innerhalb
 * eines Animationsframes neu.  Das kleine, feste Zeitfenster bleibt gross
 * genug fuer diesen echten DOM-Abdruck, verhindert aber, dass ein bewusst
 * wirkungsloses oder inzwischen verschwundenes Element den gesamten Crawler
 * minutenlang aufhaelt.
 */
async function settle(page, before, volatileKeys, budgetMs = 450) {
    const started = Date.now();
    let last = before;
    for (;;) {
        await page.waitForTimeout(90);
        last = await printOf(page);
        const changed = diffKeys(before, last, volatileKeys);
        if (changed.length > 0) {
            return { print: last, changed, waitedMs: Date.now() - started };
        }
        if (Date.now() - started > budgetMs) {
            return { print: last, changed: [], waitedMs: Date.now() - started };
        }
    }
}

// ------------------------------------------------------- Zustandsherstellung --

/**
 * Die Anwendung in eine gewuenschte Lage bringen, Schritt fuer Schritt und
 * jeder Schritt idempotent.
 *
 * Das ist der Kern der Wiederherstellung: nach jedem angefassten Element wird
 * dieselbe Funktion noch einmal gerufen, und sie tut nur, was noetig ist. Ein
 * `setup`, das blind klickt, waere nach dem zweiten Aufruf in der Gegenlage.
 */
/**
 * Die Lage, in die JEDER Zustand zurueckgebracht wird, bevor seine eigenen
 * Wuensche gelten.
 *
 * Das ist die Lehre aus der ersten Runde, und sie war teuer: dort nannte jeder
 * Zustand nur die Dinge, die ihn ausmachen, und alles andere blieb so stehen,
 * wie der Zustand davor es hinterlassen hatte. Der Reiter `change` stand noch
 * offen, als der Einstiegsdialog gebraucht wurde, und die Frage nach dem Warum
 * erscheint hinter einem offenen Assistenten nicht: der Lauf wartete dreissig
 * Sekunden auf eine Karte, die es nicht geben konnte. Schlimmer noch war die
 * leise Form desselben Fehlers: ein Knopf, den die Maus umgelegt hatte, stand
 * beim Tastaturversuch noch umgelegt da, tat darum nichts mehr, und der Lauf
 * meldete ihn als taub. Eine Wiederherstellung, die nur die genannten Dinge
 * zurueckstellt, ist keine.
 *
 * `file` und `symbol` stehen NICHT darin, und das ist kein Versehen: ein
 * geoeffnetes Symbol laesst sich nicht zurueckziehen, ohne die Seite neu zu
 * laden, und ein Neuladen nach jedem angefassten Element waere ein Lauf, der
 * eine Stunde braucht, um zu sagen, was er in zehn Minuten sagen kann. Die
 * offenen Reiter werden dafuer geschlossen, denn die wachsen sonst mit jedem
 * Klick und ihre Knoepfe waeren in jedem Durchgang andere.
 */
const BASE_WANT = {
    help: false,
    settings: false,
    entry: false,
    why: false,
    search: '',
    walk: false,
    explainOpen: false,
    explainTab: 'flow',
    twinView: 'facts',
    pseudocodeBase: false,
    flowStep: undefined,
    galaxy: true,
    graphMode: 'galaxy',
    legend: false,
    llm: false,
    agents: false,
    fullscreen: false,
    agentFilter: 'both',
    agentTrails: undefined,
    agentTrailWindow: undefined,
    /* Der Zeitstrahl ist nur im breiten Agents-Vollbild sichtbar. Die
     * Vollbild-Etappe misst ihn in der aktuellen, kompakten Vorgabe: live und
     * mit dem 60-Sekunden-Fenster. */
    agentTimelineMode: undefined,
    agentTimelineWindow: undefined,
    impactMode: 'worktree',
    defaultFacets: true,
    readerLevel: 2,
    allKinds: true,
    layoutDefault: true,
};

async function apply(page, wanted, ctx) {
    const want = { ...BASE_WANT, ...wanted };
    const origin = ctx.origin;
    let state = await pageAlive(page) ? await worldOf(page).catch(() => null) : null;

    const reload = async () => {
        await page.goto(`${origin}/?project=${PROJECT}&agents=${ctx.bridgePort}`, { waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 40000 });
        await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 40000 });
        await page.waitForFunction(() => globalThis.__atlasLayout !== undefined, undefined, { timeout: 40000 });
        await page.waitForTimeout(400);
        state = await worldOf(page);
    };

    if (state === null || state.loaded !== true || state.tree === 0) {
        await reload();
    }

    const refresh = async (ms = 260) => {
        await page.waitForTimeout(ms);
        state = await worldOf(page);
    };

    // 1. Was die Tastatur fuehrt, zuerst schliessen: alles darunter ist sonst
    //    nicht anklickbar.
    if ((state.search || state.commandOverlay || state.command !== '') && (want.search ?? '') === '') {
        const command = page.locator('[data-testid="atlas-command-input"]');
        await command.focus();
        await command.fill('');
        await page.keyboard.press('Escape');
        await refresh();
        await page.waitForFunction(() => {
            const input = document.querySelector('[data-testid="atlas-command-input"]');
            return (input?.value ?? '') === ''
                && document.querySelector('[data-testid="atlas-search-headline"]') === null;
        }, undefined, { timeout: 5000 });
        await refresh();
    }
    /* Jeder sichtbare Help-/Settings-Overlay wird voruebergehend geschlossen,
     * auch wenn er im Zielzustand wieder offen sein soll. Why, Walk, Datei und
     * Symbol muessen erst darunter stabil werden; das Oeffnen passiert spaet. */
    if (state.help) {
        await page.click('[data-menu="?"]');
        await refresh();
    }
    if (state.settings) {
        await page.click('[data-menu="a-settings"]');
        await refresh();
    }
    if (state.entry && want.entry !== true) {
        await page.click('[data-testid="atlas-entry-close"]').catch(() => undefined);
        await refresh();
    }
    /* Splitter koennen die Geometrie einer spaeteren Probe mitnehmen. Der
     * Produktweg Alt+R setzt sie zurueck; eine Naht zu beschreiben waere keine
     * Bedienung und maskierte genau den Layoutbefund, den der Lauf messen soll. */
    if (want.layoutDefault === true && state.layoutDefault !== true) {
        await page.keyboard.press('Alt+r');
        await page.waitForFunction(
            () => globalThis.__atlasLayout?.isDefault === true,
            undefined,
            { timeout: 5000 },
        );
        await refresh();
    }

    /*
     * 1b. Der Erklaeren-Bereich, und er steht hier oben aus einem gemessenen
     * Grund.
     *
     * Die Frage nach dem Warum erscheint nur, wenn weder der Assistent noch die
     * Aenderungsansicht offen sind (App.tsx: `bugOpen`, `impactOpen` sind
     * "Bereich offen UND dieser Reiter"). In der ersten Runde stand der Bereich
     * vom Zustand davor noch auf `change`, und der Zustand "Einstiegsdialog"
     * wartete danach dreissig Sekunden auf eine Karte, die es nicht geben
     * konnte. Was eine Flaeche verdeckt, wird darum geschlossen, bevor die
     * Flaeche angefordert wird.
     */
    const explainBefore = want.explainBefore ?? want.explainOpen;
    const endingWalk = want.walk !== true && state.walk > 0;
    const restoreExplainBefore = async () => {
        if (explainBefore !== undefined && state.explainOpen !== explainBefore) {
            await page.click('[data-testid="atlas-explain-collapse"]');
            await refresh();
        }
    };
    /* Ein laufender Walk besitzt seinen Exit im Explain-Bereich. Fuer ihn
     * kommt die gewuenschte Einklappung erst NACH dem Exit; der Startpfad und
     * jeder andere Zustand behalten die bisherige fruehe Wiederherstellung. */
    const needsCurrentWalkControls = want.walk === true && state.walk > 0;
    if (!endingWalk && !needsCurrentWalkControls) {
        await restoreExplainBefore();
    }

    // 2. Die Fuehrung: sie blendet die Frage aus und schaltet den Reiter walk frei.
    const isMainWalk = (current) => current.walk > 0
        && current.walkId.length > 0
        && current.walkRootName === MAIN.name
        && current.walkRootPath === FILE;
    const exitWalk = async () => {
        await page.mouse.move(2, 2);
        await closeTooltips(page);
        await page.click('[data-testid="atlas-tour-exit"]', { timeout: 4000 });
        await page.waitForFunction(
            () => Number(globalThis.__atlasTour?.steps ?? 0) === 0,
            undefined,
            { timeout: 5000 },
        );
        await refresh(300);
    };
    const endWalkReliably = async () => {
        try {
            await exitWalk();
        } catch (firstError) {
            /* Ein durch React gerade ersetzter Exit-Griff bekommt genau einen
             * frischen Aufbau. Bleibt der Walk danach, ist der zweite Fehler
             * sichtbar und wird nicht hinter einer catch-Klausel versteckt. */
            log(`tour exit erster Versuch fehlgeschlagen; frischer Aufbau vor Retry: ${String(firstError.message ?? firstError).split('\n')[0]}`);
            await reload();
            if (state.walk > 0) {
                await exitWalk();
            } else {
                await page.waitForFunction(
                    () => Number(globalThis.__atlasTour?.steps ?? 0) === 0,
                    undefined,
                    { timeout: 5000 },
                );
            }
        }
    };
    const rewindMainWalk = async () => {
        const walkId = state.walkId;
        for (let remaining = 32; state.walkIndex > 0 && remaining > 0; remaining -= 1) {
            const from = state.walkIndex;
            const previous = page.locator('[data-testid="atlas-tour-prev"]');
            await previous.waitFor({ state: 'visible', timeout: 5000 });
            if (await previous.isDisabled()) {
                throw new Error(`Tour ${walkId} steht auf Schritt ${from}, aber Prev ist native disabled`);
            }
            await page.mouse.move(2, 2);
            await closeTooltips(page);
            await previous.click({ timeout: 4000 });
            await page.waitForFunction(
                ({ id, index }) => globalThis.__atlasTour?.id === id
                    && Number(globalThis.__atlasTour?.index ?? -1) === index,
                { id: walkId, index: from - 1 },
                { timeout: 5000 },
            );
            log(`tour rewind belegt: id=${walkId}; Schritt ${from}->${from - 1}`);
            await refresh(180);
        }
        if (state.walkIndex !== 0) {
            throw new Error(`Tour ${walkId} erreichte Schritt 0 nicht (aktuell ${state.walkIndex})`);
        }
    };
    if (want.walk === true) {
        if (state.walk > 0 && isMainWalk(state)) {
            await rewindMainWalk();
        } else {
            if (state.walk > 0) {
                await endWalkReliably();
                await restoreExplainBefore();
            }
            await startWalk(page, ctx);
            await refresh(400);
        }
        if (!isMainWalk(state) || state.walkIndex !== 0) {
            throw new Error(`Walk-Grundlage ist nicht ${MAIN.name} auf Schritt 0: ${JSON.stringify({
                id: state.walkId, index: state.walkIndex, root: state.walkRootName, path: state.walkRootPath,
            })}`);
        }
    }
    if (endingWalk) {
        await endWalkReliably();
        await restoreExplainBefore();
    }

    // 3. Die Frage nach dem Warum: sie liegt ueber der Editorflaeche.
    if (want.why === true && !state.why) {
        await page.click('[data-menu="a-why"]');
        await refresh();
    }
    if (want.why !== true && state.why) {
        await page.click('[data-testid="atlas-why-decline"]');
        await refresh();
    }

    // 4. Der Einstiegsdialog.
    if (want.entry === true && !state.entry) {
        const entryCard = '[data-testid="atlas-why-card"][data-intent="entry"]';
        const openEntry = async () => {
            if (!state.why) {
                await page.click('[data-menu="a-why"]');
                await refresh();
            }
            await page.mouse.move(2, 2);
            await closeTooltips(page);
            await page.waitForSelector(entryCard, { state: 'visible', timeout: 12000 });
            await page.click(entryCard, { timeout: 4000 });
            await page.waitForSelector('[data-testid="atlas-entry"]', { state: 'visible', timeout: 20000 });
            await refresh(400);
        };
        await openEntry();
    }

    /*
     * 4b. Die offenen Reiter, auf einen.
     *
     * Jeder Klick auf eine Baumzeile, eine Trefferzeile, eine Pseudocode-Zeile
     * oder einen Hop oeffnet eine Datei, und jede offene Datei ist ein Reiter
     * mit zwei Knoepfen. In der ersten Runde standen nach einem Durchgang sechs
     * Reiter da, die beim Sammeln noch nicht existiert hatten, und ihre Knoepfe
     * waren beim Anfassen "nicht mehr da". Was der Zustand nicht braucht, wird
     * darum geschlossen.
     */
    const openTabs = await page.evaluate(() =>
        [...document.querySelectorAll('.atlas-tab')].map((node) => node.getAttribute('data-path')));
    const keep = want.file ?? '';
    for (const path of openTabs) {
        if (path === keep || path === null) {
            continue;
        }
        await page.click(`.atlas-tab[data-path="${path}"] .atlas-tab-close`).catch(() => undefined);
        await page.waitForTimeout(90);
    }
    if (openTabs.some((path) => path !== keep)) {
        await refresh();
    }

    // 5. Datei und Symbol.
    if ((want.file ?? '') !== '' && state.file !== want.file) {
        await page.click(`[data-testid="atlas-tree-row"][data-path="${want.file}"]`);
        await page.waitForFunction(
            (path) => (globalThis.__atlasReader?.document?.path ?? '') === path,
            want.file,
            { timeout: 30000 },
        );
        await refresh(400);
    }
    if ((want.symbol ?? '') !== '' && (state.symbol !== want.symbol || state.twinStatus !== 'ready')) {
        await openSymbol(page, want.symbol, ctx);
        await refresh(300);
    }
    if (want.readerLevel !== undefined && state.readerLevel !== want.readerLevel) {
        await page.locator('[data-testid="atlas-twin-depth"]').fill(String(want.readerLevel));
        await page.waitForTimeout(180);
        await refresh();
    }

    // 5b. Die Ansicht des Twins.
    if ((want.twinView ?? '') !== '' && state.twinView !== want.twinView) {
        await page.click(want.twinView === 'pseudocode'
            ? '[data-testid="atlas-pseudocode-toggle"]'
            : '[data-testid="atlas-twin-tab-facts"]').catch(() => undefined);
        await refresh(400);
    }

    /* Pseudocode-Zeilen springen innerhalb derselben Datei. Die erste
     * sichtbare Quellzeile setzt vor jeder Einzelprobe eine echte, feste
     * Ausgangslage; der Fingerprint misst die Reader-Caret-Zeile dabei mit. */
    if (want.pseudocodeBase === true && state.twinView === 'pseudocode') {
        const baseLine = await page.evaluate(() => {
            const node = [...document.querySelectorAll(
                '[data-testid="atlas-pseudocode-line"] button.atlas-pseudocode-line-btn',
            )]
                .find((entry) => globalThis.__w12.shown(entry));
            return node === undefined ? null : globalThis.__w12.describe(node);
        });
        if (baseLine !== null) {
            await page.mouse.move(2, 2);
            await closeTooltips(page);
            await page.evaluate((entry) => {
                globalThis.__w12.bring(entry);
                return globalThis.__w12.focus(entry).focused;
            }, baseLine);
            await page.keyboard.press('Space');
            await refresh(180);
        }
    }

    /*
     * 6. Der Erklaeren-Bereich, noch einmal.
     *
     * Zweimal und aus demselben Grund wie beim Vollbildmodus: das Oeffnen einer
     * Fuehrung schlaegt den Reiter `walk` auf, und ein Symbolwechsel kann den
     * Bereich mitnehmen. Was oben geschlossen wurde, damit die Frage sichtbar
     * werden konnte, muss danach wieder in die gewuenschte Lage.
     */
    if ((want.explainTab ?? '') !== '' && state.explainTab !== want.explainTab) {
        await page.click(`[data-testid="atlas-explain-tab"][data-tab="${want.explainTab}"]`);
        await refresh();
    }
    if (want.explainOpen !== undefined && state.explainOpen !== want.explainOpen) {
        await page.click('[data-testid="atlas-explain-collapse"]');
        await refresh();
    }
    if (want.flowStep !== undefined && state.flowStep !== want.flowStep) {
        const selector = `[data-testid="atlas-flow-step"][data-step="${want.flowStep}"] `
            + '[data-testid="atlas-flow-step-button"]';
        await page.locator(selector).waitFor({ state: 'visible', timeout: 5000 });
        const step = await page.evaluate((index) => {
            const node = document.querySelector(
                `[data-testid="atlas-flow-step"][data-step="${index}"] `
                + '[data-testid="atlas-flow-step-button"]',
            );
            return node === null ? null : globalThis.__w12.describe(node);
        }, want.flowStep);
        if (step === null) {
            throw new Error(`flow step ${want.flowStep} verschwand vor seiner Aktivierung`);
        }
        await page.mouse.move(2, 2);
        await closeTooltips(page);
        await page.evaluate((entry) => {
            globalThis.__w12.bring(entry);
            return globalThis.__w12.focus(entry).focused;
        }, step);
        await page.keyboard.press('Space');
        await page.waitForFunction(
            (index) => Number(globalThis.__atlasFlow?.step ?? -1) === index,
            want.flowStep,
            { timeout: 5000 },
        );
        await refresh(180);
    }

    /*
     * 6b. Die Aenderungsansicht zurueck auf den Arbeitsbaum.
     *
     * Sie hat zwei Betriebsarten, und die zweite blendet ein Eingabefeld und
     * einen Knopf ein, die es in der ersten nicht gibt. Bleibt sie stehen, dann
     * sammelt der naechste Zustand zwei Bedienelemente ein, die zu einer Lage
     * gehoeren, die er gar nicht herstellen wollte.
     */
    if (want.impactMode !== undefined) {
        const mode = await page.evaluate(() => {
            const node = document.querySelector('[data-testid="atlas-impact-mode-worktree"]');
            return node === null ? '' : (node.getAttribute('data-active') === 'true' ? 'worktree' : 'since-ref');
        });
        if (mode.length > 0 && mode !== want.impactMode) {
            await page.click(`[data-testid="atlas-impact-mode-${want.impactMode}"]`).catch(() => undefined);
            await refresh();
        }
    }

    // 7. Der Graph: sichtbar, Ansicht, Legende.
    if (want.graphMode !== undefined && state.graphMode !== want.graphMode) {
        await page.click(`[data-testid="atlas-graph-mode-chip"][data-mode="${want.graphMode}"]`);
        await refresh(500);
    }
    if (want.galaxy !== undefined && state.galaxy !== want.galaxy) {
        await page.click('[data-testid="atlas-galaxy-collapse"]');
        await refresh(500);
    }
    if (want.legend !== undefined && state.legend !== want.legend) {
        await page.click('[data-testid="atlas-galaxy-legend-toggle"]');
        await refresh();
    }

    // 8. Die Kantenarten: was jemand ausgeblendet hat, kommt zurueck.
    if (want.allKinds === true && state.hiddenKinds.length > 0) {
        for (const kind of [...state.hiddenKinds]) {
            await page.click(`[data-testid="atlas-galaxy-legend-swatch"][data-type="${kind}"]`)
                .catch(() => undefined);
            await page.waitForTimeout(120);
        }
        await refresh();
    }

    // 9. Die Linsen des Twins auf ihre Vorgabe.
    if (want.defaultFacets === true) {
        const wanted = { logic: true, calls: true, data: true, errors: true, tests: true, runtime: false, changes: false };
        for (const entry of state.facets) {
            const [name, on] = entry.split('=');
            const should = wanted[name];
            if (should !== undefined && String(should) !== on) {
                await page.click(`.atlas-twin-facet[data-facet="${name}"]`).catch(() => undefined);
                await page.waitForTimeout(140);
            }
        }
        await refresh();
    }

    /*
     * Der Vollbildmodus braucht zwei Phasen: ausschalten, solange sein
     * sichtbarer Schalter noch IM laufenden Agenteninstrument liegt; einschalten
     * erst nachdem dieses Instrument spaeter wieder da ist. Diese eine Funktion
     * wird deshalb vor Settings UND nach dem Agentenwechsel benutzt.
     */
    const ensureFullscreen = async () => {
        if (want.fullscreen === undefined || state.agentFullscreen === want.fullscreen) {
            return;
        }
        const selector = '[data-testid="atlas-agents-switch"][data-switch="fullscreen"]';
        const switchThere = await page.locator(selector).count();
        if (switchThere === 0) {
            if (state.agentFullscreen) {
                throw new Error('laufendes Agents-Vollbild hat keinen sichtbaren Ausschalter');
            }
            return;
        }
        await page.locator(selector).waitFor({ state: 'visible', timeout: 5000 });
        await page.click(selector, { timeout: 4000 });
        await page.waitForFunction(
            (expected) => (globalThis.__atlasAgents?.fullscreen ?? false) === expected,
            want.fullscreen,
            { timeout: 5000 },
        );
        await refresh(700);
    };
    /* Settings und Display duerfen nie hinter dem Fullscreen-Instrument
     * angefasst werden. Der Rueckweg ist vollstaendig echte UI: erst dessen
     * sichtbarer Schalter, dann der Menuegriff fuer den Live-Modus. */
    const leaveAgentsBeforeSettings = async () => {
        if (want.fullscreen === false && state.agentFullscreen) {
            await ensureFullscreen();
        }
        if (want.agents === false && state.agents) {
            await page.click('[data-menu="a-agents"]', { timeout: 4000 });
            await page.waitForFunction(
                () => globalThis.__atlasAgents?.on !== true,
                undefined,
                { timeout: 5000 },
            );
            await refresh(600);
        }
    };
    await leaveAgentsBeforeSettings();

    // 9b. Die Darstellung: was das Panel schaltet, wird ueber das Panel geschaltet.
    if (want.display !== undefined) {
        const wanted = Object.entries(want.display)
            .filter(([key, value]) => String(state.display[key] ?? '') !== String(value));
        if (wanted.length > 0) {
            if (!state.settings) {
                await page.click('[data-menu="a-settings"]');
                await refresh(300);
            }
            for (const [key, value] of wanted) {
                await page.click(
                    `[data-setting="${key}"] [data-testid="atlas-settings-option"][data-option="${value}"]`,
                ).catch(() => undefined);
                await page.waitForTimeout(220);
            }
            await refresh(400);
        }
        if (state.settings !== (want.settings === true)) {
            await page.click('[data-menu="a-settings"]');
            await refresh(300);
        }
    }

    // 10. Das lokale Modell.
    if (want.llm !== undefined && state.llm !== want.llm) {
        await page.click('[data-menu="a-llm"]');
        await page.waitForTimeout(400);
        if (want.llm) {
            await page.waitForFunction(
                () => (globalThis.__atlasLlm?.state ?? '') === 'ready',
                undefined,
                { timeout: 30000 },
            ).catch(() => undefined);
        }
        await refresh(400);
    }

    await ensureFullscreen();

    // 11. Der Live-Modus der Agenten.
    if (want.agents !== undefined && state.agents !== want.agents) {
        await page.click('[data-menu="a-agents"]');
        if (want.agents) {
            await page.waitForFunction(
                () => (globalThis.__atlasAgents?.sourceState ?? '') === 'connected',
                undefined,
                { timeout: 30000 },
            ).catch(() => undefined);
        }
        await refresh(600);
    }
    /*
     * Die Wiedergabe weiterdrehen, bis es Akteure gibt.
     *
     * Die Bruecke steht im Wiedergabemodus still, bis jemand den Takt setzt
     * (tools/agent-bridge.mjs). Ohne diesen Schritt waere der Live-Modus zwar
     * verbunden, aber leer, und jede Messung an den Akteuren waere eine Messung
     * an null Akteuren.
     */
    if (want.agents === true) {
        const actors = await page.evaluate(() => (globalThis.__atlasAgents?.actors ?? []).length);
        const foreign = await page.evaluate(() =>
            (globalThis.__atlasAgents?.actors ?? []).some((actor) => !actor.you));
        if (actors === 0 || (want.foreignActor === true && !foreign)) {
            await ctx.advance();
            await page.waitForFunction(
                (needsForeign) => {
                    const current = globalThis.__atlasAgents?.actors ?? [];
                    return current.length > 0
                        && (!needsForeign || current.some((actor) => !actor.you));
                },
                want.foreignActor === true,
                { timeout: 25000 },
            );
            await refresh(700);
        }
        if (ctx.agentReplayReading !== null) {
            /* Read-only product seam: this is the live actors list after the
             * real HTTP advance, not a reconstructed list from the fixture. */
            ctx.agentReplayReading.actors = await page.evaluate(() => {
                const actors = globalThis.__atlasAgents?.actors ?? [];
                return {
                    total: actors.length,
                    foreign: actors.filter((actor) => !actor.you).length,
                    you: actors.filter((actor) => actor.you).length,
                };
            });
        }
    }

    /*
     * Der Slider setzt eine echte Replay-Lage; auch die Fensterknöpfe schreiben
     * eine echte Produktpraeferenz. Beides muss vor dem naechsten einzelnen
     * Control wieder ueber die sichtbare Timeline zurueckgestellt werden. Die
     * Leser-Akteurin wird sonst korrekt in der Gegenwart berechnet, aber in
     * einer alten Wiedergabe nicht mehr gezeigt.
     */
    const restoreAgentTimeline = async () => {
        if (want.agentTimelineMode === undefined && want.agentTimelineWindow === undefined) {
            return;
        }
        if (want.agentTimelineMode !== undefined && want.agentTimelineMode !== 'live') {
            throw new Error(`unbekannter gewuenschter Timeline-Modus ${JSON.stringify(want.agentTimelineMode)}`);
        }
        const timeline = page.locator('[data-testid="atlas-agents-timeline"]');
        await timeline.waitFor({ state: 'visible', timeout: 8000 });

        const mode = await timeline.getAttribute('data-mode');
        if (want.agentTimelineMode === 'live' && mode === 'replay') {
            const live = page.locator('[data-testid="atlas-agents-timeline-live"]');
            await live.waitFor({ state: 'visible', timeout: 4000 });
            await live.click({ timeout: 4000 });
        } else if (want.agentTimelineMode === 'live' && mode === 'paused') {
            const pause = page.locator('[data-testid="atlas-agents-timeline-pause"]');
            await pause.waitFor({ state: 'visible', timeout: 4000 });
            await pause.click({ timeout: 4000 });
        } else if (want.agentTimelineMode === 'live' && mode !== 'live') {
            throw new Error(`unbekannter aktueller Timeline-Modus ${JSON.stringify(mode)}`);
        }

        if (want.agentTimelineWindow !== undefined
            && Number(await timeline.getAttribute('data-window')) !== want.agentTimelineWindow) {
            const window = page.locator(
                `[data-testid="atlas-agents-timeline-window"][data-option="${want.agentTimelineWindow}"]`,
            );
            await window.waitFor({ state: 'visible', timeout: 4000 });
            await window.click({ timeout: 4000 });
        }

        await page.waitForFunction(
            ({ mode: expectedMode, window: expectedWindow }) => {
                const node = document.querySelector('[data-testid="atlas-agents-timeline"]');
                return node !== null
                    && node.getAttribute('data-mode') === expectedMode
                    && (expectedWindow === undefined
                        || Number(node.getAttribute('data-window')) === expectedWindow);
            },
            { mode: want.agentTimelineMode ?? mode, window: want.agentTimelineWindow },
            { timeout: 8000 },
        );
        await refresh(350);
    };
    /* Der HUD-Schalter entscheidet, ob der AgentLayer ueberhaupt Spuren
     * zeichnet. Er wird nur fuer die Spurprobe verlangt und ausschliesslich
     * ueber seinen sichtbaren Produktgriff wiederhergestellt. */
    const restoreAgentTrails = async () => {
        if (want.agentTrails === undefined || state.agentTrails === want.agentTrails) {
            return;
        }
        const trails = page.locator('[data-testid="atlas-agents-switch"][data-switch="trails"]');
        await trails.waitFor({ state: 'visible', timeout: 5000 });
        await trails.click({ timeout: 4000 });
        await page.waitForFunction(
            (expected) => globalThis.__atlasAgents?.trails === expected,
            want.agentTrails,
            { timeout: 5000 },
        );
        await refresh(350);
    };
    /*
     * Die kleinen HUD-Fenster steuern die gezeichneten Spuren. Sie sind weder
     * der grosse Replay-Zeitstrahl noch dessen DOM. Nach jeder Mausprobe wird
     * diese sichtbare Produktpraeferenz wieder auf den Zustandswert gesetzt,
     * bevor die Tastatur dieselbe Gegenlage misst.
     */
    const restoreAgentTrailWindow = async () => {
        if (want.agentTrailWindow === undefined || state.agentTrailWindow === want.agentTrailWindow) {
            return;
        }
        const window = page.locator(
            `[data-testid="atlas-agents-window-option"][data-option="${want.agentTrailWindow}"]`,
        );
        await window.waitFor({ state: 'visible', timeout: 5000 });
        await window.click({ timeout: 4000 });
        await page.waitForFunction(
            (expected) => Number(globalThis.__atlasAgents?.trailWindowMs ?? -1) === expected,
            want.agentTrailWindow,
            { timeout: 5000 },
        );
        await refresh(350);
    };

    /* Erst nachdem der Live-Modus sichtbar ist, kann sein Vollbildschalter den
     * Zeitstrahl einblenden. Die zweite Phase gehoert daher vor dessen
     * Wiederherstellung und vor der Leser-Akteurin. */
    await ensureFullscreen();
    await restoreAgentTimeline();
    await restoreAgentTrails();
    await restoreAgentTrailWindow();
    if (want.youActor === true && !state.youActor) {
        await page.evaluate(() => {
            globalThis.__atlasReader?.editor?.setPosition?.({ lineNumber: 24, column: 5 });
            globalThis.__atlasReader?.editor?.focus?.();
        });
        await page.waitForFunction(
            () => (globalThis.__atlasAgents?.actors ?? []).some((actor) => actor.you),
            undefined,
            { timeout: 25000 },
        ).catch(() => undefined);
        await refresh(400);
    }
    if (want.agentFilter !== undefined && state.agentFilter !== want.agentFilter) {
        await page.click(`[data-testid="atlas-agents-filter-option"][data-option="${want.agentFilter}"]`)
            .catch(() => undefined);
        await refresh();
    }

    /* Help und Settings kommen erst nach Walk, Datei, Symbol, Twin und den
     * Agenten-Schaltern. So messen Maus und Tastatur danach dieselbe
     * produktseitige Twin-Lage statt eine vom Overlay verdeckte Restoberflaeche. */
    if (want.settings === true && !state.settings) {
        await page.click('[data-menu="a-settings"]');
        await refresh();
    }
    if (want.help === true && !state.help) {
        await page.click('[data-menu="?"]');
        await refresh();
    }

    // 12. Das Suchfenster ganz zum Schluss: es liegt ueber der Zeile.
    if ((want.search ?? '') !== '' && state.command !== want.search) {
        const input = page.locator('[data-testid="atlas-command-input"]');
        await input.click();
        await input.fill('');
        await input.pressSequentially(want.search, { delay: 25 });
        await page.waitForSelector('[data-testid="atlas-search-row"]', { timeout: 20000 })
            .catch(() => undefined);
        await refresh(400);
    }

    return state;
}

/** Ob die Seite noch lebt und das Skript traegt. */
async function pageAlive(page) {
    try {
        return await page.evaluate(() => globalThis.__w12 !== undefined);
    } catch {
        return false;
    }
}

/** Zu einem Symbol navigieren, ueber dieselbe Suche, die die Kommandozeile fuehrt. */
async function openSymbol(page, name, ctx) {
    const input = page.locator('[data-testid="atlas-command-input"]');
    let last = 'Suche nicht gestartet';
    /* Der gleiche Leserweg darf einmal neu angesetzt werden: Direkt nach einem
     * Hop kann dessen noch sichtbare Suchzeile den ersten Auswahlklick
     * absorbieren. Erst wenn auch die zweite echte Suche nicht das verlangte
     * Symbol UND einen fertigen Twin liefert, wird der Zustand abgebrochen. */
    for (let attempt = 1; attempt <= 2; attempt += 1) {
        await input.click();
        await input.fill('');
        await input.pressSequentially(name, { delay: 25 });
        const row = `[data-testid="atlas-search-row"][data-name="${name}"]`;
        try {
            await page.waitForSelector(row, { timeout: 30000 });
            await page.waitForTimeout(400);
            await page.click(row);
            await page.waitForFunction(
                (expected) => (globalThis.__atlasTwin?.symbol ?? '') === expected,
                name,
                { timeout: 20000 },
            );
            await page.waitForFunction(
                () => document.querySelector('[data-testid="atlas-twin"]')?.getAttribute('data-status') === 'ready',
                undefined,
                { timeout: 20000 },
            );
            await page.waitForTimeout(400);
            return;
        } catch (error) {
            last = String(error.message ?? error).split('\n')[0];
            if (attempt === 1) {
                const recovery = { symbol: name, reason: last, at: new Date().toISOString() };
                ctx.recoveries.push(recovery);
                log(`openSymbol(${name}) erster Versuch fehlgeschlagen; frischer Seitenzustand vor Retry`);
                await page.reload({ waitUntil: 'load' });
                await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 40000 });
                await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 40000 });
                await page.waitForFunction(() => globalThis.__atlasLayout !== undefined, undefined, { timeout: 40000 });
                await page.waitForTimeout(400);
            }
        }
    }
    throw new Error(`openSymbol(${name}) erreichte nach zwei echten Suchauswahlen keinen fertigen Twin: ${last}`);
}

/* Nach einer Einzelprobe kann React den Twin erst im naechsten Takt mit den
 * Fakten des wiederhergestellten Symbols fuellen. Die Wiederherstellung ist
 * erst fertig, wenn sowohl die gewuenschte Lage als auch genau der zuvor
 * gemessene Griff wieder sichtbar sind. Laeuft diese ehrliche, begrenzte
 * Wartezeit aus, bleibt der folgende fehlende Griff ein Befund. */
async function restoreDescriptor(page, state, descriptor, ctx) {
    await apply(page, state.want, ctx);
    return page.waitForFunction(
        ({ entry, want }) => {
            const node = globalThis.__w12.resolve(entry);
            if (node === null || !globalThis.__w12.shown(node)) {
                return false;
            }
            if ((want.symbol ?? '') !== '') {
                if ((globalThis.__atlasTwin?.symbol ?? '') !== want.symbol
                    || document.querySelector('[data-testid="atlas-twin"]')?.getAttribute('data-status') !== 'ready') {
                    return false;
                }
            }
            if (want.explainOpen !== undefined
                && (document.querySelector('[data-testid="atlas-explain"]')?.getAttribute('data-open') === 'true')
                    !== want.explainOpen) {
                return false;
            }
            if ((want.explainTab ?? '') !== ''
                && document.querySelector('[data-testid="atlas-explain"]')?.getAttribute('data-tab') !== want.explainTab) {
                return false;
            }
            return true;
        },
        { entry: descriptor, want: state.want },
        { timeout: 3200 },
    ).then(() => true).catch(() => false);
}

/** Eine Fuehrung von der festen createUser-Wurzel starten, wie ein Leser. */
async function startWalk(page, ctx) {
    let state = await worldOf(page);
    /* Der W12e-Nachweis fragt den echten Same-ready-Fall. Deshalb wird
     * createUser zuerst ueber die vorhandene Such-/openSymbol-Strecke fertig
     * gelesen und erst DANACH die Entry-Zeile wie ein Leser aktiviert. */
    if (state.file !== FILE || state.symbol !== MAIN.name || state.twinStatus !== 'ready') {
        await openSymbol(page, MAIN.name, ctx);
        await page.waitForFunction(
            ({ name, file }) => (globalThis.__atlasReader?.document?.path ?? '') === file
                && (globalThis.__atlasTwin?.symbol ?? '') === name
                && document.querySelector('[data-testid="atlas-twin"]')?.getAttribute('data-status') === 'ready',
            { name: MAIN.name, file: FILE },
            { timeout: 30000 },
        );
        state = await worldOf(page);
    }
    if (!state.why && !state.entry) {
        await page.click('[data-menu="a-why"]');
        await page.waitForSelector('[data-testid="atlas-why"]', { state: 'visible', timeout: 12000 });
    }
    if (!state.entry) {
        await page.click('[data-testid="atlas-why-card"][data-intent="entry"]');
        await page.waitForSelector('[data-testid="atlas-entry"]', { timeout: 20000 });
    }
    const selector = `[data-testid="atlas-entry-row"][data-name="${MAIN.name}"]:visible`;
    const row = page.locator(selector);
    await row.waitFor({ state: 'visible', timeout: 12000 });
    const count = await row.count();
    if (count !== 1) {
        throw new Error(`Entry-Wurzel ${MAIN.name} ist nicht eindeutig sichtbar (${count} Treffer)`);
    }
    await page.mouse.move(2, 2);
    await closeTooltips(page);
    await row.click({ timeout: 4000 });
    try {
        await page.waitForFunction(
            ({ name, file }) => Number(globalThis.__atlasTour?.steps ?? 0) > 0
                && Number(globalThis.__atlasTour?.index ?? -1) === 0
                && String(globalThis.__atlasTour?.titles?.[0] ?? '')
                    .replace(/^Start here:\s*/, '')
                    .trim() === name
                && (globalThis.__atlasTour?.paths?.[0] ?? '') === file
                && (globalThis.__atlasReader?.document?.path ?? '') === file
                && (globalThis.__atlasTwin?.symbol ?? '') === name
                && document.querySelector('[data-testid="atlas-twin"]')?.getAttribute('data-status') === 'ready',
            { name: MAIN.name, file: FILE },
            /* Nach der echten Such-Vorwaermung prueft dieser Schritt nur noch,
             * dass das gleiche fertige Symbol beim Walk erhalten bleibt. */
            { timeout: 30000 },
        );
    } catch (error) {
        const actual = await page.evaluate(() => {
            const rawRoot = String(globalThis.__atlasTour?.titles?.[0] ?? '');
            return {
                steps: Number(globalThis.__atlasTour?.steps ?? 0),
                id: globalThis.__atlasTour?.id ?? '',
                index: Number(globalThis.__atlasTour?.index ?? -1),
                rawRoot,
                root: rawRoot.replace(/^Start here:\s*/, '').trim(),
                path: globalThis.__atlasTour?.paths?.[0] ?? '',
                readerPath: globalThis.__atlasReader?.document?.path ?? '',
                twinSymbol: globalThis.__atlasTwin?.symbol ?? '',
                twinStatus: document.querySelector('[data-testid="atlas-twin"]')?.getAttribute('data-status') ?? '',
            };
        });
        throw new Error(`createUser-Walk erreichte seine Root/Twin-Invariante nicht binnen 30 s: ${JSON.stringify(actual)}; ${String(error.message ?? error).split('\n')[0]}`);
    }
}

// ---------------------------------------------------------------- Zustaende --

/**
 * Die Zustaende, die dieser Lauf selbst herstellt.
 *
 * Woertlich die Liste des Contracts, plus die drei, die aus ihr folgen: eine
 * laufende Fuehrung (sonst waere der Reiter walk nie bedienbar), der
 * Einstiegsdialog (die Kandidatenliste) und das Modell in beiden Lagen.
 */
const STATES = [
    {
        id: 'start',
        title: 'Startbildschirm: die Frage nach dem Warum ueber der leeren Editorflaeche',
        want: { why: true, galaxy: true, legend: false, explainOpen: false },
    },
    {
        id: 'file-open',
        title: 'Eine Datei im Reader, ohne aufgeloestes Symbol',
        want: { why: false, file: FILE, galaxy: true, legend: false, explainOpen: false },
    },
    {
        id: 'symbol-focus',
        title: 'Ein Symbol im Fokus: der Twin steht auf createUser',
        want: {
            why: false, file: FILE, symbol: MAIN.name, galaxy: true, legend: false,
            explainOpen: false, defaultFacets: true, allKinds: true,
        },
    },
    {
        id: 'twin-pseudocode',
        title: 'Der Twin auf dem Reiter pseudocode statt facts',
        want: {
            why: false, file: FILE, symbol: MAIN.name, twinView: 'pseudocode', pseudocodeBase: true,
            galaxy: true, legend: false,
        },
    },
    {
        id: 'explain-flow',
        title: 'Der Erklaeren-Bereich, Reiter flow',
        want: {
            why: false, file: FILE, symbol: MAIN.name, explainTab: 'flow', explainOpen: true,
            flowStep: 0, galaxy: true,
        },
    },
    {
        id: 'explain-walk-idle',
        title: 'Der Erklaeren-Bereich, Reiter walk ohne laufende Fuehrung',
        want: { why: false, file: FILE, symbol: MAIN.name, explainTab: 'walk', explainOpen: true, galaxy: true },
    },
    {
        id: 'explain-chat-idle',
        title: 'Der Erklaeren-Bereich, Reiter chat ohne gestellte Frage',
        want: { why: false, file: FILE, symbol: MAIN.name, explainTab: 'chat', explainOpen: true, galaxy: true },
    },
    {
        id: 'explain-bug',
        title: 'Der Erklaeren-Bereich, Reiter bug',
        want: { why: false, file: FILE, symbol: MAIN.name, explainTab: 'bug', explainOpen: true, galaxy: true },
    },
    {
        id: 'explain-change',
        title: 'Der Erklaeren-Bereich, Reiter change',
        want: { why: false, file: FILE, symbol: MAIN.name, explainTab: 'change', explainOpen: true, galaxy: true },
    },
    {
        id: 'galaxy-legend',
        title: 'Die Galaxie mit offener Legende',
        want: { why: false, file: FILE, symbol: MAIN.name, galaxy: true, graphMode: 'galaxy', legend: true, allKinds: true },
    },
    {
        id: 'galaxy-collapsed',
        title: 'Die Galaxie zugeklappt: der Kopf bleibt bedienbar',
        want: { why: false, file: FILE, symbol: MAIN.name, galaxy: false, legend: false },
    },
    {
        id: 'hierarchy',
        title: 'Die Hierarchie statt der Galaxie',
        want: { why: false, file: FILE, symbol: MAIN.name, galaxy: true, graphMode: 'hierarchy', legend: true },
    },
    {
        id: 'search',
        title: 'Das Suchfenster mit seiner Kandidatenliste',
        want: { why: false, file: FILE, symbol: MAIN.name, galaxy: true, legend: false, search: 'user' },
    },
    {
        id: 'entry-dialog',
        title: 'Der Einstiegsdialog: die Liste der angebotenen Einstiegspunkte',
        /*
         * `explainOpen: false` ist hier keine Kosmetik: die Frage nach dem
         * Warum, ueber die dieser Dialog erreicht wird, erscheint nicht,
         * solange der Assistent oder die Aenderungsansicht offen sind.
         */
        want: {
            why: false, file: FILE, symbol: MAIN.name, entry: true,
            explainOpen: false, galaxy: true, legend: false,
        },
    },
    {
        id: 'walk-running',
        title: 'Eine laufende Fuehrung: die Schrittkarte im Erklaeren-Bereich',
        want: {
            /*
             * `explainBefore` schliesst den Bereich, BEVOR die Fuehrung
             * gestartet wird: sie beginnt am Einstiegsdialog, und der haengt an
             * der Frage nach dem Warum, die hinter einem offenen Assistenten
             * nicht erscheint. Danach soll er offen sein, denn dort steht die
             * Schrittkarte.
             */
            explainBefore: false, file: FILE, symbol: MAIN.name, walk: true,
            walkIndex: 0, walkRootName: MAIN.name, walkRootPath: FILE,
            explainTab: 'walk', explainOpen: true,
            galaxy: true, legend: false,
        },
    },
    {
        id: 'help',
        title: 'Die Hilfeseite',
        want: {
            why: false, help: true, file: FILE, symbol: MAIN.name, walk: false,
            galaxy: true, legend: false,
        },
    },
    {
        id: 'settings',
        title: 'Die Einstellungen mit den Effektschaltern',
        want: {
            why: false, settings: true, file: FILE, symbol: MAIN.name, walk: false,
            galaxy: true, legend: false,
        },
    },
    {
        id: 'llm-on',
        title: 'Das lokale Modell an: die Karte und ihre Schalter',
        want: { why: false, file: FILE, symbol: MAIN.name, llm: true, galaxy: true, legend: false },
    },
    {
        id: 'llm-off',
        title: 'Das lokale Modell aus: die Karte sagt es und bietet nichts an',
        want: { why: false, file: FILE, symbol: MAIN.name, llm: false, galaxy: true, legend: false },
    },
    {
        id: 'agents-live',
        title: 'Der Live-Modus der Agenten, mit eigenem Koerper des Lesers',
        want: {
            why: false, file: FILE, agents: true, youActor: true, foreignActor: true, galaxy: true, legend: false,
            agentFilter: 'both', agentTrails: true, agentTrailWindow: 60000,
        },
    },
    {
        id: 'agents-fullscreen',
        title: 'Der Live-Modus im Vollbild: der Zeitstrahl und seine Schalter',
        want: {
            why: false, file: FILE, agents: true, youActor: true, foreignActor: true, galaxy: true, legend: false,
            agentFilter: 'both', agentTrails: true, agentTrailWindow: 60000, fullscreen: true,
            agentTimelineMode: 'live', agentTimelineWindow: 60000,
        },
    },
    /*
     * Zuletzt, und das ist kein Zufall: dieser Zustand schreibt die Darstellung
     * in den Speicher dieses Browsers und aendert damit, was die Zustaende
     * danach saehen. Er steht am Ende, damit alle anderen mit der Vorgabe
     * gemessen werden, und die Filterproben danach laufen alle in der flachen
     * Ansicht, in der das Bild stillsteht.
     */
    {
        id: 'settings-flat',
        title: 'Die Einstellungen bei flacher Projektion: das Bild steht still und laesst sich zaehlen',
        want: {
            why: false, file: FILE, symbol: MAIN.name, settings: true,
            galaxy: true, graphMode: 'galaxy', legend: false,
            /*
             * Der Live-Modus geht hier aus, und der Vollbildmodus davor.
             * Bewegte Koerper auf dem Graphen waeren beim Zaehlen der
             * Bildpunkte genau das, was dieser Zustand vermeiden soll: eine
             * Zahl, die sich ohne Zutun aendert.
             */
            fullscreen: false, agents: false,
            display: { projection: 'flat' },
        },
    },
];

// ------------------------------------------------------------------- Filter --

/**
 * Die Schalter, die etwas ein- oder ausblenden, und WORAN man das zaehlt.
 *
 * Jeder Eintrag nennt seinen Zustand, den Klick, den er braucht, und die eine
 * Zahl, an der ein Leser die Wirkung nachrechnen kann. Die Zahl ist nie "hat
 * sich etwas geaendert": sie ist eine Menge, die kleiner werden muss.
 */
function filterPlan(edgeKinds) {
    const facets = ['logic', 'calls', 'data', 'errors', 'tests', 'runtime', 'changes'];
    const plan = [];

    for (const facet of facets) {
        plan.push({
            name: `Linse ${facet}`,
            kind: 'facet',
            unit: 'Zeilen im Twin',
            state: 'symbol-focus',
            selector: `.atlas-twin-facet[data-facet="${facet}"]`,
            measure: 'twinRows',
            /*
             * Runtime und Changes stehen per Vorgabe AUS, die anderen fuenf AN.
             * Gemessen wird immer von der Lage, in der die Linse zeigt, zu der,
             * in der sie schweigt: sonst waere "weniger" bei den beiden das
             * Ergebnis des Einschaltens.
             */
            startsOn: facet !== 'runtime' && facet !== 'changes',
            explainWhenEmpty: facet === 'runtime' || facet === 'changes',
        });
    }

    plan.push({
        name: 'Umschalter facts gegen pseudocode',
        kind: 'view',
        unit: 'Zeilen der Fakten-Ansicht',
        state: 'symbol-focus',
        selector: '[data-testid="atlas-pseudocode-toggle"]',
        back: '[data-testid="atlas-twin-tab-facts"]',
        measure: 'twinRows',
        startsOn: true,
    });

    for (const kind of edgeKinds) {
        plan.push({
            name: `Kantenart ${kind}`,
            kind: 'edge-kind',
            unit: 'gezeichnete Kanten',
            state: 'galaxy-legend',
            selector: `[data-testid="atlas-galaxy-legend-swatch"][data-type="${kind}"]`,
            measure: 'drawnEdges',
            startsOn: true,
        });
    }

    for (const option of ['you', 'agent']) {
        plan.push({
            name: `Akteursfilter ${option}`,
            kind: 'actor-filter',
            unit: 'Zeilen im Instrument',
            state: 'agents-live',
            selector: `[data-testid="atlas-agents-filter-option"][data-option="${option}"]`,
            back: '[data-testid="atlas-agents-filter-option"][data-option="both"]',
            measure: 'agentRows',
            startsOn: true,
        });
    }

    /*
     * Die Effektschalter der Einstellungen, jeder mit der Menge, die er
     * wegnimmt.
     *
     * `off` ist der Schaltwert, der wegnimmt, `on` der, der zurueckbringt. Bei
     * den Namen sind das nicht "false" und "true", sondern zwei Zahlen: `0`
     * heisst keine Entfernungsgrenze (also alle Namen), `1` heisst nur die
     * naechsten. Der Schaltwert steht darum je Schalter da und wird nicht
     * geraten.
     */
    const effects = [
        /*
         * NodeLabels meldet nur die aktuell sichtbaren Sprites nach oben.
         * Diese ruhige, endliche Liste misst die sichtbaren Namen genauer als
         * ein Bild und bleibt bei gleichem Zustand exakt wiederholbar.
         */
        {
            name: 'labels',
            measure: 'labelBoxes',
            unit: 'sichtbare Namenskaesten',
            off: '1',
            on: '0',
            state: 'settings-flat',
        },
        { name: 'agents', measure: 'agentBodies', unit: 'Koerper auf dem Graphen', off: 'false', on: 'true' },
        {
            name: 'agentTrails',
            measure: 'agentTrails',
            unit: 'gezeichnete Agentenspuren',
            off: 'false',
            on: 'true',
            trails: true,
            prime: 'agent-trail',
        },
        {
            name: 'agentWaves',
            measure: 'agentWaves',
            unit: 'Wellen',
            off: 'false',
            on: 'true',
            prime: 'agent-wave',
        },
        {
            name: 'agentTimeline',
            measure: 'timeline',
            unit: 'Zeitstrahlen',
            off: 'false',
            on: 'true',
            /* Den gibt es nur im Vollbild, siehe TIMELINE_MIN_WIDTH. */
            state: 'agents-fullscreen',
        },
    ];
    for (const effect of effects) {
        plan.push({
            name: `Effektschalter ${effect.name}`,
            kind: 'effect',
            unit: effect.unit,
            state: effect.state ?? 'agents-live',
            selector: `[data-testid="atlas-settings-effect"][data-effect="${effect.name}"] `
                + `[data-testid="atlas-settings-option"][data-option="${effect.off}"]`,
            back: `[data-testid="atlas-settings-effect"][data-effect="${effect.name}"] `
                + `[data-testid="atlas-settings-option"][data-option="${effect.on}"]`,
            measure: effect.measure,
            startsOn: true,
            needsSettings: true,
            trails: effect.trails,
            prime: effect.prime,
            timelineEffect: effect.name === 'agentTimeline',
        });
    }

    /*
     * Die drei Schalter, die kein Element wegnehmen, sondern Licht.
     *
     * Halos, Bloom und die Kantendichte aendern nichts an der Zahl der Knoten,
     * Namen oder Linien: sie aendern, wie hell das Bild ist. Eine Zahl dafuer
     * gibt es im DOM nicht und in keiner Naht, also wird das Bild selbst
     * gezaehlt, Bildpunkt fuer Bildpunkt (siehe {@link litPixels}). Gemessen
     * wird in der flachen Ansicht, weil die raeumliche von selbst weiterdreht
     * und eine Zahl, die sich ohne Zutun aendert, kein Beweis ist.
     */
    const light = [
        { name: 'halos', off: 'false', on: 'true', unit: 'helle Bildpunkte der Szene' },
        { name: 'bloom', off: 'false', on: 'true', unit: 'helle Bildpunkte der Szene' },
        { name: 'edges', off: 'off', on: 'full', unit: 'helle Bildpunkte der Szene' },
    ];
    for (const entry of light) {
        plan.push({
            name: `Effektschalter ${entry.name}`,
            kind: 'effect',
            unit: entry.unit,
            state: 'settings-flat',
            selector: `[data-testid="atlas-settings-effect"][data-effect="${entry.name}"] `
                + `[data-testid="atlas-settings-option"][data-option="${entry.off}"]`,
            back: `[data-testid="atlas-settings-effect"][data-effect="${entry.name}"] `
                + `[data-testid="atlas-settings-option"][data-option="${entry.on}"]`,
            measure: 'pixels',
            startsOn: true,
            needsSettings: true,
        });
    }

    return plan;
}

/**
 * Wie viele Bildpunkte der Szene wirklich leuchten.
 *
 * Der Weg fuehrt ueber ein Bild und nicht ueber die Zeichenflaeche: eine
 * WebGL-Flaeche ohne `preserveDrawingBuffer` gibt ihre Bildpunkte nicht heraus,
 * und sie dafuer umzukonfigurieren hiesse, das Produkt fuer die Messung zu
 * aendern. Playwright fotografiert den Ausschnitt, die Seite selbst decodiert
 * das Bild in eine 2D-Flaeche und zaehlt: alles bleibt lokal, es entsteht keine
 * Verbindung und kein zweiter Prozess.
 */
async function litPixels(page) {
    const scene = page.locator('[data-testid="atlas-galaxy-scene"]');
    const box = await scene.boundingBox();
    if (box === null) {
        return 0;
    }
    const shot = await page.screenshot({
        clip: {
            x: Math.round(box.x), y: Math.round(box.y),
            width: Math.max(8, Math.round(box.width)), height: Math.max(8, Math.round(box.height)),
        },
    });
    return page.evaluate(async (data) => {
        const image = new Image();
        image.src = `data:image/png;base64,${data}`;
        await image.decode();
        const canvas = document.createElement('canvas');
        canvas.width = image.width;
        canvas.height = image.height;
        const context = canvas.getContext('2d');
        context.drawImage(image, 0, 0);
        const pixels = context.getImageData(0, 0, canvas.width, canvas.height).data;
        let lit = 0;
        for (let i = 0; i < pixels.length; i += 4) {
            if ((pixels[i] + pixels[i + 1] + pixels[i + 2]) / 3 > 26) {
                lit += 1;
            }
        }
        return lit;
    }, shot.toString('base64'));
}

// ------------------------------------------------------------- Die Etappen ---

/**
 * Warum dieser Lauf in Etappen zerfaellt.
 *
 * Der erste Versuch wollte alles in einem Browser und einer Runde erledigen:
 * 22 Zustaende, ihre Bedienelemente und die Filter, am Stueck. Nach 59,9
 * Minuten war der Browser weg ("Target page, context or browser has been
 * closed"), die Runde hatte kein einziges Bedienelement angefasst, und
 * buttons.json trug 21 Zustaende und null Elemente. Eine Stunde Arbeit ohne
 * Ergebnis, weil alles an einem Faden hing.
 *
 * Daraus folgen vier Regeln:
 *
 * 1. Nach JEDEM Zustand und nach jedem angefassten Element wird buttons.json
 *    fortgeschrieben. Was gemessen ist, bleibt gemessen, auch wenn der naechste
 *    Zustand scheitert.
 * 2. Ein Aufruf ist eine Etappe und hat seine eigene Zeitgrenze
 *    (`W12_STAGE_MS`, acht Minuten). Reisst sie, ist das ein Befund DIESER
 *    Etappe; der naechste Aufruf macht dort weiter, wo sie aufhoerte, und der
 *    Befund verschwindet, wenn der Zustand fertig gemessen ist.
 * 3. Ein Browser traegt hoechstens fuenf Einheiten (`W12_BROWSER_UNITS`).
 *    Danach wird er geschlossen und neu geoeffnet, und die Wiederherstellung
 *    navigiert in den naechsten Zustand zurueck. Stirbt er vorher, ist das ein
 *    Befund MIT ZUSTANDSNAMEN und kein stiller Abbruch.
 * 4. Die Elementphase kommt vor den Bildern. Was gemessen werden muss, wird
 *    zuerst gemessen; das Bild ist der Beleg und nicht die Messung.
 *
 * Welche Zustaende eine Etappe faehrt, sagt `W12_STATES` (Namen mit Komma
 * getrennt, `pending` fuer alles, was in diesem Durchgang noch fehlt, `none`
 * fuer keinen); welche Filter, sagt `W12_FILTERS` nach denselben Regeln. Ohne
 * beides laeuft die Etappe ueber alles, wie vorher auch.
 *
 * ## Etappe, Durchgang, Runde
 *
 * Eine ETAPPE ist ein Aufruf. Ein DURCHGANG ist eine vollstaendige Vermessung
 * aller Zustaende und aller Filter; er kann aus beliebig vielen Etappen
 * bestehen. Eine RUNDE im Sinne von AC8 ist ein abgeschlossener Durchgang, und
 * `newFindings` ist die Zahl seiner Befunde. Eine Etappe allein ist keine
 * Runde: sonst waere "zwei Runden ohne Befund" mit zwei kleinen Etappen zu
 * haben, und die Frage, ob die Oberflaeche trocken gelaufen ist, waere nicht
 * mehr gestellt.
 */
const STAGE_MS = Number(process.env.W12_STAGE_MS ?? 8 * 60 * 1000);
const BROWSER_UNITS = Number(process.env.W12_BROWSER_UNITS ?? 5);
/* A completed round is only comparable to one produced by the same collector
 * definition.  The replay-freshness fix changes state construction itself, so
 * its first pass starts clean while retaining the auditable old round history. */
const HARNESS_REVISION = 'w12-replay-freshness-v2';

/** Die Zustaende nach Namen, damit W12_STATES sich vertippen kann und es merkt. */
const STATE_BY_ID = new Map(STATES.map((entry) => [entry.id, entry]));

/** Ein Fehler, nach dem im Browser niemand mehr etwas messen kann. */
const DEAD_BROWSER = /Target (page|closed)|has been closed|Target crashed|browser has disconnected|Protocol error|Session closed/i;

function emptyStore() {
    return {
        pass: 1, states: {}, filters: {}, filterPhase: null, rounds: [], stages: [], meta: {},
        agentReplayReadings: {},
    };
}

/**
 * Den Speicher der Etappen davor lesen.
 *
 * Ein Artefakt aus der Zeit vor den Etappen traegt keinen Speicher; von ihm
 * ueberleben nur die Runden, denn die sind die Geschichte des Zyklus und nicht
 * die Messung. Der naechste Durchgang faengt dann hinter der letzten Runde an.
 */
function loadStore() {
    if (!existsSync(OUT_JSON)) {
        return emptyStore();
    }
    try {
        const old = JSON.parse(readFileSync(OUT_JSON, 'utf8'));
        const rounds = Array.isArray(old.rounds) ? old.rounds : [];
        const kept = old.extras?.store;
        if (kept === undefined || kept === null) {
            return { ...emptyStore(), rounds, pass: rounds.length + 1 };
        }
        if (kept.meta?.harnessRevision !== HARNESS_REVISION) {
            const lastPass = Number(rounds.at(-1)?.pass ?? rounds.length);
            return {
                ...emptyStore(),
                rounds,
                pass: lastPass + 1,
                meta: { ...kept.meta, harnessRevision: HARNESS_REVISION },
            };
        }
        return {
            ...emptyStore(),
            ...kept,
            rounds,
            states: kept.states ?? {},
            filters: kept.filters ?? {},
            stages: kept.stages ?? [],
            meta: { ...(kept.meta ?? {}), harnessRevision: HARNESS_REVISION },
            agentReplayReadings: kept.agentReplayReadings ?? {},
            pass: Number(kept.pass ?? rounds.length + 1),
        };
    } catch {
        return emptyStore();
    }
}

/** Welche Zustaende diese Etappe faehrt. */
function passHasProgress(store) {
    const pass = store.pass;
    return Object.values(store.states ?? {}).some((record) => record.pass === pass)
        || Object.values(store.filters ?? {}).some((record) => record.pass === pass)
        || store.filterPhase?.pass === pass;
}

function pickStates(store, defaultWish) {
    const wish = (process.env.W12_STATES ?? '').trim() || defaultWish;
    if (wish === 'none') {
        return [];
    }
    if (wish.length === 0 || wish === 'all') {
        return [...STATES];
    }
    if (wish === 'pending') {
        return STATES.filter((entry) => {
            const record = store.states[entry.id];
            return record === undefined || record.pass !== store.pass || record.complete !== true;
        });
    }
    const names = wish.split(',').map((entry) => entry.trim()).filter(Boolean);
    const unknown = names.filter((name) => !STATE_BY_ID.has(name));
    if (unknown.length > 0) {
        throw new Error(`W12_STATES kennt diese Zustaende nicht: ${unknown.join(', ')}`);
    }
    /* In der Reihenfolge der Liste und nicht in der des Wunsches: `settings-flat`
     * schreibt die Darstellung in den Speicher des Browsers und gehoert darum
     * hinter alle anderen. */
    return STATES.filter((entry) => names.includes(entry.id));
}

/** Welche Filter diese Etappe misst. */
function pickFilters(plan, store, wish) {
    if (wish === 'none') {
        return [];
    }
    if (wish === 'all') {
        return [...plan];
    }
    if (wish === 'pending') {
        return plan.filter((filter) => {
            const record = store.filters[filter.name];
            return record === undefined || record.pass !== store.pass;
        });
    }
    const names = wish.split(',').map((entry) => entry.trim().toLowerCase()).filter(Boolean);
    return plan.filter((filter) =>
        names.some((name) => filter.name.toLowerCase().includes(name)));
}

/** Die Elemente, die in diesem Durchgang schon ein anderer Zustand angefasst hat. */
function seenKeys(store, pass, exceptId) {
    const keys = new Set();
    for (const record of Object.values(store.states)) {
        if (record.pass !== pass || record.id === exceptId) {
            continue;
        }
        for (const control of record.controls ?? []) {
            keys.add(control.key);
        }
    }
    return keys;
}

/**
 * Identitaet ist nicht Beschriftung.  `local llm off` wird nach einem Klick
 * zu `local llm on`, bleibt aber derselbe DOM-Griff.  Selector (mit seinen
 * data-Attributen), Position innerhalb dieser Selector-Familie und Ort
 * bleiben fuer denselben Griff stabil; das Label bleibt ausschliesslich die
 * lesbare Anzeige im Bericht.
 */
function controlKey(descriptor) {
    return `${descriptor.place}|${descriptor.selector}|${descriptor.nth}`;
}

// ------------------------------------------------------------- Die Zahlen ----

/**
 * Die Zahlen des Artefakts, bei jedem Schreiben neu aus dem Speicher gebildet.
 *
 * Nichts davon wird fortgeschrieben: `uniqueControls` ist die Zahl der
 * Elemente, die im Speicher stehen, und nicht die Zahl, die eine fruehere
 * Etappe einmal gezaehlt hat. Sonst waere die Summe nach einer wiederholten
 * Messung falsch, und niemand saehe es.
 */
function aggregate(store) {
    const order = STATES.map((entry) => entry.id);
    const states = Object.values(store.states).sort(
        (a, b) => (a.pass - b.pass) || (order.indexOf(a.id) - order.indexOf(b.id)),
    );

    /* Ein Element gehoert dem Zustand, der es zuletzt angefasst hat. */
    const byKey = new Map();
    for (const state of states) {
        for (const control of state.controls ?? []) {
            byKey.set(control.key, { ...control, state: state.id });
        }
    }
    const controls = [...byKey.values()];
    const filters = Object.values(store.filters ?? {});

    const controlsByState = {};
    for (const entry of STATES) {
        const record = store.states[entry.id];
        if (record !== undefined) {
            controlsByState[entry.id] = record.controlsFound ?? 0;
        }
    }

    const walked = states.filter((state) => state.tabWalk !== undefined && state.tabWalk !== null);
    const tabWalks = walked.map((state) => ({ state: state.id, ...state.tabWalk }));
    const sum = (pick) => states.reduce((total, state) => total + (pick(state) ?? 0), 0);
    const phase = store.filterPhase ?? { consoleErrors: [], pageErrors: [] };
    const nativeDisabledFocusExempt = (entry) => entry.disabled === true
        && entry.keyboard?.focusable === false
        && entry.mouse?.done === true
        && entry.keyboard?.done === true
        && entry.excuse?.marked === true
        && typeof entry.noEffect?.reason === 'string'
        && entry.noEffect.reason.length > 10;

    return {
        statesVisited: states.filter((state) => state.complete === true).length,
        controlsByState,
        controls,
        uniqueControls: controls.length,
        controlsClicked: controls.filter((entry) => entry.mouse?.done === true).length,
        controlsByKeyboard: controls.filter((entry) => entry.keyboard?.done === true).length,
        focusVisibleAll: controls.length > 0
            && controls.every((entry) => entry.focusVisible === true || nativeDisabledFocusExempt(entry)),
        didNothing: controls
            .filter((entry) => entry.noEffect !== undefined && entry.noEffect !== null)
            .map((entry) => ({
                label: entry.label,
                place: entry.place,
                selector: entry.selector,
                state: entry.state,
                reason: entry.noEffect.reason,
            })),
        filtersMeasured: filters.length,
        everyFilterRemovesAndRestores: filters.length > 0
            && filters.every((entry) => entry.removes === true && entry.restored === true),
        emptyFilterExplainsItself: filters
            .filter((entry) => entry.before === 0 || entry.emptyCase === true)
            .every((entry) => entry.explains === true),
        filterCounts: filters,
        consoleErrors: sum((state) => (state.consoleErrors ?? []).length)
            + (phase.consoleErrors ?? []).length,
        uncaughtExceptions: sum((state) => (state.pageErrors ?? []).length)
            + (phase.pageErrors ?? []).length,
        overlapViolations: sum((state) => state.readability?.overlaps),
        clippingViolations: sum((state) => state.readability?.clipped),
        cutWithoutHint: sum((state) => state.readability?.cutWithoutHint),
        keyboardTraps: walked.filter((state) => state.tabWalk.trap === true).length,
        tabOrderFollowsLayout: walked.length > 0
            && walked.every((state) => state.tabWalk.inDocumentOrder === true),
        tabWalks,
        rounds: store.rounds,
        port: store.meta.port ?? 0,
        leftoverProcesses: store.meta.leftoverProcesses ?? 0,
    };
}

/**
 * Ist der Durchgang vollstaendig, wird er zur Runde.
 *
 * Vollstaendig heisst: jeder Zustand der Liste ist in DIESEM Durchgang fertig
 * gemessen, und jeder Filter des Plans ebenso. Erst dann sagt die Zahl der
 * Befunde etwas ueber die Oberflaeche; bei einem halben Durchgang saehe eine
 * Null nur so aus, als waere nichts gefunden worden.
 */
function closeRound(store) {
    const names = store.meta.filterNames ?? [];
    const missingStates = STATES.filter((entry) => {
        const record = store.states[entry.id];
        return record === undefined || record.pass !== store.pass || record.complete !== true;
    }).map((entry) => entry.id);
    const missingFilters = names.filter((name) => store.filters[name]?.pass !== store.pass);
    if (names.length === 0 || missingStates.length > 0 || missingFilters.length > 0) {
        return { closed: false, missingStates, missingFilters };
    }

    const findings = [];
    let durationMs = 0;
    for (const entry of STATES) {
        const record = store.states[entry.id];
        durationMs += record.durationMs ?? 0;
        findings.push(...(record.findings ?? []));
    }
    for (const name of names) {
        const filter = store.filters[name];
        durationMs += filter.durationMs ?? 0;
        if (filter.removes !== true || filter.restored !== true) {
            findings.push({
                kind: 'filter-without-effect',
                state: filter.state,
                label: filter.name,
                detail: `vorher ${filter.before} ${filter.unit}, nachher ${filter.after}, `
                    + `zurueck ${filter.again}`
                    + (filter.explains ? ' (die Flaeche nennt den Grund)' : ''),
            });
        }
    }
    findings.push(...(store.filterPhase?.findings ?? []));

    const round = {
        n: store.rounds.length + 1,
        at: new Date().toISOString(),
        pass: store.pass,
        newFindings: findings.length,
        complete: true,
        durationMs,
        states: STATES.map((entry) => entry.id),
        filters: names.length,
        findings: findings.slice(0, 200),
    };
    store.rounds.push(round);
    store.pass += 1;
    store.filterPhase = null;
    return { closed: true, round, missingStates: [], missingFilters: [] };
}

// -------------------------------------------------------------- Der Anbau ----

/**
 * Bauen, aber nur wenn dist aelter ist als die Quellen.
 *
 * Zwoelf Etappen sind zwoelf Baulaeufe zu je elf Sekunden, und das an einem
 * Verzeichnis, das sich zwischen zwei Etappen nicht aendert. Gebaut wird darum
 * nach der Uhr der Dateien, und WARUM gebaut oder nicht gebaut wurde, steht im
 * Artefakt: eine Etappe, die auf einem alten dist misst, waere ein Beweis ueber
 * gestern.
 */
async function newestSource() {
    const roots = ['src', 'index.html', 'vite.config.ts', 'package.json', 'tsconfig.json'];
    let newest = 0;
    const walk = async (path) => {
        const info = await stat(path).catch(() => null);
        if (info === null) {
            return;
        }
        if (info.isDirectory()) {
            for (const entry of await readdir(path)) {
                await walk(join(path, entry));
            }
            return;
        }
        newest = Math.max(newest, info.mtimeMs);
    };
    for (const root of roots) {
        await walk(join(ROOT, root));
    }
    return newest;
}

async function buildIfStale() {
    const stamp = join(DIST, 'index.html');
    const built = existsSync(stamp) ? (await stat(stamp)).mtimeMs : 0;
    if (built === 0) {
        return { built: true, why: 'dist/index.html fehlt' };
    }
    const newest = await newestSource();
    if (newest > built) {
        return { built: true, why: 'eine Quelle ist juenger als dist/index.html' };
    }
    return { built: false, why: 'dist ist juenger als jede Quelle, es gibt nichts zu bauen' };
}

// -------------------------------------------------------------------- Lauf ---

async function main() {
    const stageStarted = Date.now();
    const deadline = stageStarted + STAGE_MS;
    const timings = {};
    const store = loadStore();
    const pass = store.pass;

    let home = null;
    let runtimeDir = null;
    let serverChild = null;
    let bridgeChild = null;
    let proxy = null;
    let browser = null;
    let context = null;
    let page = null;
    let serverPort = 0;
    let uiPort = 0;
    let bridgePort = 0;
    let failure = null;

    /* Ein normaler Folgelauf setzt denselben Durchgang fort. Nur ein
     * ausdrueckliches W12_STATES/W12_FILTERS darf davon abweichen: nach einer
     * Deadline bereits fertige Arbeit noch einmal zu fahren waere weder ein
     * neuer Durchgang noch ein sauberer Resume. */
    const stateWish = (process.env.W12_STATES ?? '').trim();
    const filterEnv = (process.env.W12_FILTERS ?? '').trim();
    const resumeWish = passHasProgress(store) ? 'pending' : 'all';
    const wantedStates = pickStates(store, resumeWish);
    const filterWish = filterEnv.length > 0
        ? filterEnv
        : (stateWish.length > 0 ? 'none' : resumeWish);

    const stage = {
        n: (store.stages?.length ?? 0) + 1,
        at: new Date().toISOString(),
        pass,
        budgetMs: STAGE_MS,
        wantedStates: wantedStates.map((entry) => entry.id),
        wantedFilters: filterWish,
        states: [],
        filters: [],
        findings: [],
        browsers: 0,
        browserDeaths: 0,
        hitDeadline: false,
        blockedRequests: [],
        sidecarRequests: 0,
        recoveries: [],
    };

    /* Was der Browser dieser Etappe meldet, geht in die Ablage der Einheit, die
     * gerade laeuft: ein Konsolenfehler ohne Zustand waere ein Fehler ohne Ort. */
    let sink = { consoleErrors: [], pageErrors: [] };

    log(`Etappe ${stage.n}, Durchgang ${pass}: ${wantedStates.length} Zustaende `
        + `(${wantedStates.map((entry) => entry.id).join(', ') || 'keine'}), Filter "${filterWish}", `
        + `Zeitgrenze ${Math.round(STAGE_MS / 1000)} s`);

    const save = async () => {
        store.meta.port = uiPort > 0 ? uiPort : (store.meta.port ?? 0);
        await writeArtifacts(store, stage, timings, failure);
    };

    try {
        if (!existsSync(BINARY)) {
            throw new Error(`Binary fehlt: ${BINARY} (erst 'make -f Makefile.cbm cbm-with-ui' im cbm-Clone bauen)`);
        }
        if (!existsSync(FIXTURE)) {
            throw new Error(`Fixture fehlt: ${FIXTURE}`);
        }

        // ------------------------------------------------------------ 1. Bau
        const decision = await buildIfStale();
        stage.build = decision;
        if (decision.built) {
            log(`npm run build (${decision.why})`);
            const buildStarted = Date.now();
            const build = await run('npm', ['run', 'build']);
            timings.buildMs = Date.now() - buildStarted;
            if (build.code !== 0) {
                throw new Error(`npm run build endete mit ${build.code}: ${build.out.trim().slice(-600)}`);
            }
        } else {
            log(`kein Bau: ${decision.why}`);
            timings.buildMs = 0;
        }

        // ---------------------------------------------- 2. HOME, Rendezvous
        home = await mkdtemp(join(tmpdir(), 'codeatlasweb-w12-home-'));
        runtimeDir = await mkdtemp('/private/tmp/codeatlasweb-w12-run-');
        process.env.CBM_RUNTIME_DIR = runtimeDir;
        log('isoliertes HOME:', home);

        // -------------------------------------------------------- 3. Index
        const indexed = await indexRepository(BINARY, { home, repoPath: FIXTURE, project: PROJECT });
        stage.indexed = { nodes: indexed.nodes, edges: indexed.edges };
        log(`indiziert: ${indexed.nodes} Knoten, ${indexed.edges} Kanten`);

        // ------------------------------------- 4. Server, Proxy, Bruecke
        serverPort = await findFreePort(MIN_PORT);
        const started = await startServer(BINARY, { home, port: serverPort, log: serverLog });
        serverChild = started.child;
        uiPort = await findFreePort(MIN_PORT, [serverPort]);
        proxy = await startStaticProxy({ distDir: DIST, upstreamPort: serverPort, port: uiPort });
        bridgePort = await findFreePort(MIN_PORT, [serverPort, uiPort]);
        stage.bridgeLog = [];
        let bridge = await startBridge(bridgePort, stage.bridgeLog);
        bridgeChild = bridge.child;
        store.meta.port = uiPort;
        stage.ports = { serverPort, uiPort, bridgePort };
        stage.bridgeHealth = bridge.health;
        stage.replaySources = [{ port: bridgePort, reason: 'stage-start', events: bridge.health.events }];
        log(`C-Server ${serverPort}, dist/ ${uiPort}, Bruecke ${bridgePort}`);

        const origin = `http://127.0.0.1:${uiPort}`;
        let bridgeOrigin = `http://127.0.0.1:${bridgePort}`;

        // ------------------------------------------------------- 5. Browser
        const { chromium } = await import('playwright');

        /**
         * Einen frischen Browser oeffnen.
         *
         * Frisch heisst auch: leerer Speicher der Seite. Das ist kein
         * Nebeneffekt, den man in Kauf nimmt, sondern der zweite Grund fuer den
         * Wechsel: die Darstellung (`atlas-display:...`) lebt im localStorage,
         * und ein Zustand, der sie umstellt, faerbt sonst alle Zustaende
         * danach.
         */
        const openBrowser = async () => {
            browser = await chromium.launch({ headless: true, args: CHROMIUM_ARGS });
            context = await browser.newContext({ viewport: { ...VIEWPORT } });
            stage.browsers += 1;

            await context.route('**/*', async (route) => {
                const url = route.request().url();
                if (url.startsWith(origin) || url.startsWith(bridgeOrigin)
                    || url.startsWith('data:') || url.startsWith('blob:')) {
                    await route.continue();
                    return;
                }
                stage.blockedRequests.push(url);
                await route.abort();
            });

            /*
             * Der Sidecar-Stub. Port 4141 gehoert dem Nutzer: hier wird nichts
             * gestartet und nichts verbunden, die Anfrage wird im Griff
             * beantwortet, bevor eine Verbindung entsteht. Wortgleich mit
             * smoke-w13.
             */
            await context.route(`${SIDECAR_ORIGIN}/**`, async (route) => {
                const url = route.request().url();
                const path = url.slice(SIDECAR_ORIGIN.length);
                stage.sidecarRequests += 1;
                const json = (body) => route.fulfill({
                    status: 200,
                    contentType: 'application/json',
                    body: JSON.stringify(body),
                });
                if (path.startsWith('/health')) {
                    return json({ status: 'ok' });
                }
                if (path.startsWith('/props')) {
                    return json({
                        model_path: 'models/stub-for-w12.gguf',
                        n_ctx: 4096,
                        total_slots: 1,
                        default_generation_settings: { n_ctx: 4096 },
                    });
                }
                if (path.startsWith('/v1/models')) {
                    return json({ data: [{ id: 'models/stub-for-w12.gguf', object: 'model' }] });
                }
                if (path.startsWith('/v1/chat/completions')) {
                    return json({
                        choices: [{ message: { content: 'stub' }, finish_reason: 'stop' }],
                        usage: { prompt_tokens: 1, completion_tokens: 1 },
                    });
                }
                return route.fulfill({ status: 404, contentType: 'text/plain', body: 'not answered' });
            });

            /*
             * Das Sinnbild der Adresszeile, beantwortet statt abgewiesen.
             *
             * Chromium fragt jede Seite nach /favicon.ico, dieses Projekt
             * liefert keins, und die 404 waere ein Konsolenfehler in JEDEM
             * Zustand. Er saehe aus wie ein Befund ueber die Anwendung und waere
             * einer ueber den Browser.
             */
            await context.route('**/favicon.ico', (route) =>
                route.fulfill({ status: 204, body: '' }));

            await context.addInitScript(PAGE_SCRIPT.replace('PROJECT_NAME', `'${PROJECT}'`));

            page = await context.newPage();
            page.on('console', (message) => {
                if (message.type() === 'error') {
                    sink.consoleErrors.push({
                        text: message.text().slice(0, 300),
                        at: Date.now() - stageStarted,
                    });
                }
            });
            page.on('pageerror', (error) => sink.pageErrors.push(String(error).slice(0, 300)));
        };

        const closeBrowser = async () => {
            if (context !== null) {
                await context.close().catch(() => undefined);
                context = null;
            }
            if (browser !== null) {
                await browser.close().catch(() => undefined);
                browser = null;
            }
            page = null;
        };

        let unitsOnBrowser = 0;
        const freshBrowser = async () => {
            await closeBrowser();
            await openBrowser();
            unitsOnBrowser = 0;
        };

        await openBrowser();
        await mkdir(SHOT_DIR, { recursive: true });

        /*
         * Der Takt der Wiedergabe wird von hier gesetzt und nicht aus der
         * Seite: die Bruecke nimmt kein Ereignis von aussen entgegen, sie hat
         * genau eine Naht dafuer, und die gehoert dem Lauf.
         */
        const ctx = {
            origin,
            bridgePort,
            recoveries: stage.recoveries,
            agentReplayReading: null,
            advance: async () => {
                const response = await fetch(
                    `http://127.0.0.1:${ctx.bridgePort}/replay/advance?count=${bridge.health.events}`,
                    { method: 'POST' },
                );
                const advance = response.ok ? await response.json() : { failed: response.status };
                stage.replayAdvanced = advance;
                if (ctx.agentReplayReading !== null && ctx.agentReplayReading.advance === null) {
                    ctx.agentReplayReading.advance = advance;
                }
                return advance;
            },
        };
        /*
         * Die Aufzeichnung ist absichtlich ein einmaliger Strom. Nach den
         * Zustandsbrowsern waere sie fuer die Filter bereits vollstaendig
         * ausgegeben. Die Filter beginnen darum mit einer neuen lokalen
         * Replay-Quelle und einem frischen Browser, nie mit einer privaten
         * Aenderung der Seite oder des Agentenzustands.
         */
        const freshReplaySource = async (reason) => {
            await closeBrowser();
            await stopBridge(bridgeChild);
            bridgePort = await findFreePort(MIN_PORT, [serverPort, uiPort, bridgePort]);
            bridge = await startBridge(bridgePort, stage.bridgeLog);
            bridgeChild = bridge.child;
            bridgeOrigin = `http://127.0.0.1:${bridgePort}`;
            ctx.bridgePort = bridgePort;
            stage.replaySources.push({ port: bridgePort, reason, events: bridge.health.events });
            await openBrowser();
            unitsOnBrowser = 0;
            log(`frische Replay-Quelle fuer ${reason}: ${bridgePort}`);
            return bridge.health;
        };

        const freshReplayForAgentState = async (stateId) => {
            const reason = `fresh for ${stateId}`;
            const health = await freshReplaySource(reason);
            /* Health ist der echte Startwert dieser gerade erzeugten lokalen
             * Quelle, nicht ein aus einem vorherigen Browser erhaltenes Feld. */
            store.agentReplayReadings[stateId] = {
                reason,
                source: { port: bridgePort },
                health: {
                    mode: health.mode,
                    events: health.events,
                    emitted: health.emitted,
                },
                advance: null,
                actors: { total: 0, foreign: 0, you: 0 },
            };
            ctx.agentReplayReading = store.agentReplayReadings[stateId];
        };

        const readability = async (name) => {
            await closeTooltips(page);
            const top = await measureReadability(page, READABILITY_EXCLUSIONS);
            const scrolled = await scrollRegionsToEnd(page, READABILITY_EXCLUSIONS);
            await page.waitForTimeout(200);
            const bottom = await measureReadability(page, READABILITY_EXCLUSIONS);
            await resetScroll(page, READABILITY_EXCLUSIONS);
            const overlaps = [...top.overlaps, ...bottom.overlaps];
            const clipped = [...top.clipped, ...bottom.clipped];
            return {
                name,
                scrolledRegions: scrolled.length,
                candidates: { top: top.candidates, bottom: bottom.candidates },
                overlaps: overlaps.length,
                clipped: clipped.length,
                cutWithoutHint: clipped.filter((entry) => entry.kind === 'cut-without-hint').length,
                detail: {
                    overlaps: overlaps.slice(0, 12).map((entry) => ({
                        a: entry.a.path, b: entry.b.path,
                        text: `${entry.a.text} / ${entry.b.text}`.slice(0, 200),
                    })),
                    clipped: clipped.slice(0, 12).map((entry) => ({
                        kind: entry.kind, path: entry.element.path,
                        text: String(entry.element.text).slice(0, 200),
                    })),
                },
            };
        };

        // ------------------------------------------------ 5a. Die Zustaende
        for (const state of wantedStates) {
            if (Date.now() > deadline) {
                stage.hitDeadline = true;
                stage.findings.push({
                    kind: 'stage-deadline',
                    state: state.id,
                    label: `${Math.round(STAGE_MS / 1000)} s`,
                    detail: 'die Zeitgrenze dieser Etappe war erreicht, bevor dieser Zustand an der '
                        + 'Reihe war; der naechste Aufruf faengt hier an',
                });
                log(`Zeitgrenze erreicht, ${state.id} bleibt fuer die naechste Etappe liegen`);
                break;
            }
            if (state.id === 'agents-live' || state.id === 'agents-fullscreen') {
                /* Replay-Zeit beginnt an der Messgrenze: nicht beim
                 * Stage-Start, wo eine lange vorherige Etappe alle fremden
                 * Akteure wieder inaktiv machen koennte. */
                await freshReplayForAgentState(state.id);
            } else {
                ctx.agentReplayReading = null;
            }
            if (unitsOnBrowser >= BROWSER_UNITS) {
                log(`frischer Browser nach ${unitsOnBrowser} Einheiten`);
                await freshBrowser();
            }
            unitsOnBrowser += 1;

            const record = beginState(store, state, pass);
            sink = record;
            log(`Zustand ${state.id}: ${state.title}`);
            const stateStarted = Date.now();
            let outcome = { interrupted: false };
            try {
                outcome = await runState(page, state, record, ctx, {
                    store,
                    pass,
                    deadline,
                    readability,
                    save,
                });
            } catch (error) {
                const message = String(error?.message ?? error)
                    .replace(/\s+/g, ' ').trim().slice(0, 1200);
                if (DEAD_BROWSER.test(message)) {
                    stage.browserDeaths += 1;
                    record.findings.push({
                        kind: 'browser-died',
                        state: state.id,
                        label: message,
                        detail: 'der Browser war mitten in diesem Zustand weg; die Etappe hat ihn neu '
                            + 'geoeffnet und macht mit dem naechsten Zustand weiter',
                    });
                    log(`Browser gestorben in ${state.id}: ${message}`);
                    await freshBrowser();
                    unitsOnBrowser = 1;
                } else {
                    record.findings.push({
                        kind: 'state-unreachable',
                        state: state.id,
                        label: state.title,
                        detail: message,
                    });
                    log(`Zustand ${state.id} nicht erreichbar: ${message}`);
                }
            }
            record.durationMs = (record.durationMs ?? 0) + (Date.now() - stateStarted);
            timings[`state.${state.id}Ms`] = Date.now() - stateStarted;
            store.states[state.id] = record;
            stage.states.push({
                id: state.id,
                complete: record.complete === true,
                controls: (record.controls ?? []).length,
                found: record.controlsFound ?? 0,
                findings: (record.findings ?? []).length,
                ms: Date.now() - stateStarted,
            });
            /* Ein Zustand ist Teil der Etappe. Seine Rohbefunde duerfen nicht
             * hinter einem scheinbar leeren Etappenkopf verschwinden: dieselbe
             * Liste steht darum sowohl am Zustand als auch sichtbar an der
             * Etappe, waehrend die Runde sie weiter aus dem Rohspeicher bildet. */
            for (const finding of record.findings ?? []) {
                stage.findings.push({ ...finding, source: 'state' });
            }
            sink = { consoleErrors: [], pageErrors: [] };
            ctx.agentReplayReading = null;
            await save();
            log(`Zustand ${state.id}: ${record.controlsFound ?? 0} Bedienelemente gefunden, `
                + `${(record.controls ?? []).length} in diesem Zustand angefasst, `
                + `${record.complete === true ? 'fertig' : 'offen'}, `
                + `${Math.round((Date.now() - stateStarted) / 1000)} s`);

            if (state.want.display !== undefined) {
                /* Dieser Zustand hat den Speicher der Seite beschrieben. */
                await freshBrowser();
            }
            if (outcome.interrupted) {
                stage.hitDeadline = true;
                stage.findings.push({
                    kind: 'stage-deadline',
                    state: state.id,
                    label: `${Math.round(STAGE_MS / 1000)} s`,
                    detail: 'die Zeitgrenze riss mitten in diesem Zustand; was gemessen war, steht '
                        + 'im Artefakt, der Rest ist Sache der naechsten Etappe',
                });
                break;
            }
        }

        // -------------------------------------------------- 5b. Die Filter
        if (filterWish !== 'none' && Date.now() < deadline) {
            /*
             * Der erste Filter darf keine bereits ausgespielte Wiedergabe
             * erben. Diese Einheit zaehlt nicht gegen die Fuenfergrenze eines
             * Browsers: sie IST die neue, leere Browser-Einheit der Filter.
             */
            await freshReplaySource('filter phase');
            const legend = STATE_BY_ID.get('galaxy-legend');
            await apply(page, legend.want, ctx);
            const kinds = await page.evaluate(() =>
                (globalThis.__atlasGalaxy?.edgeKinds ?? []).map((entry) => entry.type));
            if (kinds.length > 0) {
                store.meta.edgeKinds = kinds;
            }
            const plan = filterPlan(store.meta.edgeKinds ?? kinds);
            store.meta.filterNames = plan.map((filter) => filter.name);
            const wanted = pickFilters(plan, store, filterWish);
            log(`${wanted.length} von ${plan.length} Filtern in dieser Etappe`);
            if (store.filterPhase === null || store.filterPhase === undefined
                || store.filterPhase.pass !== pass) {
                store.filterPhase = { pass, consoleErrors: [], pageErrors: [], findings: [] };
            }
            sink = store.filterPhase;
            let freshActorsForFilters = false;

            for (const filter of wanted) {
                if (Date.now() > deadline) {
                    stage.hitDeadline = true;
                    stage.findings.push({
                        kind: 'stage-deadline',
                        state: filter.state,
                        label: filter.name,
                        detail: 'die Zeitgrenze dieser Etappe war erreicht, bevor dieser Filter an '
                            + 'der Reihe war',
                    });
                    break;
                }
                if (unitsOnBrowser >= BROWSER_UNITS) {
                    await freshBrowser();
                }
                /*
                 * Spuren und Wellen brauchen eine frische, noch nicht
                 * gealterte Wiedergabe. Beide Quellen bleiben lokal; apply
                 * schaltet den Live-Modus darueber ein und laesst die echten
                 * Ereignisse unmittelbar vor ihrer Messung einlaufen.
                 */
                if (filter.prime === 'agent-wave' || filter.prime === 'agent-trail') {
                    await freshReplaySource(`${filter.prime} filter`);
                }
                if (filter.kind === 'actor-filter' && !freshActorsForFilters) {
                    /* Both actor choices must see the same newly advanced
                     * replay, rather than a source consumed by the preceding
                     * facets and graph filters. */
                    await freshReplaySource('fresh for agent filters');
                    freshActorsForFilters = true;
                }
                unitsOnBrowser += 1;
                const filterStarted = Date.now();
                const state = STATE_BY_ID.get(filter.state);
                const measured = await measureFilter(page, filter, state, ctx);
                measured.pass = pass;
                measured.at = new Date().toISOString();
                measured.durationMs = Date.now() - filterStarted;
                store.filters[filter.name] = measured;
                stage.filters.push(filter.name);
                log(`Filter ${filter.name}: ${measured.before} -> ${measured.after} -> `
                    + `${measured.again} ${filter.unit}`
                    + (measured.why.length > 0 ? ` (${measured.why})` : ''));
                await save();
                if (!(await pageAlive(page))) {
                    stage.browserDeaths += 1;
                    store.filterPhase.findings.push({
                        kind: 'browser-died',
                        state: filter.state,
                        label: filter.name,
                        detail: 'der Browser war nach dieser Filterprobe weg; die Etappe hat ihn neu '
                            + 'geoeffnet',
                    });
                    await freshBrowser();
                    unitsOnBrowser = 1;
                }
            }
            sink = { consoleErrors: [], pageErrors: [] };
        }

        stage.apiRoutes = { ...proxy.log.apiRoutes };
        await closeBrowser();
    } catch (err) {
        failure = err;
        console.error('[smoke-w12] FEHLER:', err.message);
        if (serverLog.length > 0) {
            console.error('[smoke-w12] Server-Log (letzte 20 Zeilen):\n' + serverLog.slice(-20).join('\n'));
        }
        stage.findings.push({
            kind: 'stage-aborted',
            state: '',
            label: String(err.message ?? err).slice(0, 200),
            detail: 'die Etappe kam nicht durch; was bis dahin gemessen war, steht im Artefakt',
        });
    }

    if (context !== null) {
        await context.close().catch(() => undefined);
    }
    if (browser !== null) {
        await browser.close().catch(() => undefined);
    }
    if (proxy !== null) {
        await proxy.close();
    }
    await stopBridge(bridgeChild);
    await stopServer(serverChild);

    /*
     * Mehrfach nachsehen statt einmal: ein Prozess, der eben SIGTERM bekommen
     * hat, gibt seinen Port nicht in derselben Millisekunde frei. Das Muster
     * steht seit dem 2026-08-30 in tools/smoke-w6-full.mjs und misst dort
     * 1557 ms am Modellport.
     */
    const ports = [serverPort, uiPort, bridgePort].filter((value) => value > 0);
    const looks = [];
    let leftovers = [];
    for (let attempt = 0; attempt < 38; attempt += 1) {
        leftovers = [];
        for (const port of ports) {
            leftovers.push({ port, listeners: await countListeners(port) });
        }
        const total = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
        looks.push({ attempt: attempt + 1, atMs: Date.now() - stageStarted, total });
        if (total === 0) {
            break;
        }
        await sleep(400);
    }
    stage.leftovers = leftovers;
    stage.leftoverLooks = looks;
    store.meta.leftoverProcesses = leftovers.reduce((sum, entry) => sum + entry.listeners, 0);
    store.meta.leftoverAt = new Date().toISOString();
    log('leftoverProcesses:', store.meta.leftoverProcesses);

    timings.totalMs = Date.now() - stageStarted;
    stage.durationMs = timings.totalMs;
    stage.timings = timings;

    /* Der Durchgang wird zur Runde, sobald er vollstaendig ist. */
    const closed = closeRound(store);
    stage.closedRound = closed.closed ? closed.round.n : null;
    stage.missingStates = closed.missingStates;
    stage.missingFilters = closed.missingFilters.length;
    stage.bridgeLog = (stage.bridgeLog ?? []).slice(-10);
    stage.blockedRequests = stage.blockedRequests.slice(0, 40);
    store.stages = [...(store.stages ?? []).slice(-60), stage];

    await mkdir(SHOT_DIR, { recursive: true });
    const report = await writeArtifacts(store, stage, timings, failure);
    log('geschrieben:', OUT_JSON, 'und', OUT_MD);
    if (closed.closed) {
        log(`Durchgang ${closed.round.pass} vollstaendig: Runde ${closed.round.n} mit `
            + `${closed.round.newFindings} Befunden`);
    } else {
        log(`Durchgang ${pass} noch offen: ${closed.missingStates.length} Zustaende, `
            + `${closed.missingFilters.length} Filter fehlen`);
    }

    const shots = existsSync(SHOT_DIR) ? readdirSync(SHOT_DIR).length : 0;
    const lastTwo = report.rounds.slice(-2);
    const ok =
        failure === null
        && stage.findings.length === 0
        && report.statesVisited >= 10
        && report.uniqueControls >= 60
        && report.controlsClicked === report.uniqueControls
        && report.controlsByKeyboard === report.uniqueControls
        && report.focusVisibleAll === true
        && report.didNothing.every((entry) => entry.reason.length > 10)
        && report.filtersMeasured >= 10
        && report.everyFilterRemovesAndRestores === true
        && report.emptyFilterExplainsItself === true
        && report.consoleErrors === 0
        && report.uncaughtExceptions === 0
        && report.overlapViolations === 0
        && report.clippingViolations === 0
        && report.cutWithoutHint === 0
        && report.keyboardTraps === 0
        && report.tabOrderFollowsLayout === true
        && report.rounds.length >= 2
        && lastTwo.length === 2
        && lastTwo.every((round) => round.newFindings === 0)
        && report.port >= MIN_PORT
        && report.leftoverProcesses === 0
        && shots >= 10
        && stage.blockedRequests.length === 0;

    if (!ok) {
        console.error('[smoke-w12] W12-Smoke NICHT gruen.');
        console.error(`[smoke-w12] Etappe ${stage.n}: ${stage.states.length} Zustaende, `
            + `${stage.filters.length} Filter, ${stage.findings.length} Etappenbefunde`);
        for (const finding of stage.findings.slice(0, 20)) {
            console.error(`  - [${finding.kind}] ${finding.state} ${finding.label}: ${finding.detail}`);
        }
        const open = closed.closed ? [] : closed.missingStates;
        if (open.length > 0) {
            console.error(`[smoke-w12] offen: ${open.join(', ')}`);
        }
        const round = report.rounds[report.rounds.length - 1];
        if (round !== undefined) {
            console.error(`[smoke-w12] letzte Runde ${round.n}: ${round.newFindings} Befunde`);
            for (const finding of (round.findings ?? []).slice(0, 40)) {
                console.error(`  - [${finding.kind}] ${finding.state} ${finding.label}: ${finding.detail}`);
            }
        }
        if (home) {
            console.error('[smoke-w12] isoliertes HOME bleibt zum Nachsehen liegen:', home);
        }
        process.exitCode = 1;
        return;
    }

    for (const directory of [home, runtimeDir]) {
        if (directory) {
            await rm(directory, { recursive: true, force: true });
        }
    }
    log('W12-Smoke gruen.');
}

// ------------------------------------------------------------ Ein Zustand ----

/**
 * Der Eintrag eines Zustands: entweder frisch oder der halbe von vorhin.
 *
 * Ein halber Eintrag desselben Durchgangs wird fortgesetzt und nicht ersetzt:
 * die Elemente, die eine frueher abgebrochene Etappe schon angefasst hat, sind
 * gemessen, und sie noch einmal anzufassen waere verlorene Zeit. Was verworfen
 * wird, sind die Befunde ueber den Abbruch selbst: sie gehoeren der Etappe, die
 * abbrach, und nicht dem Zustand.
 */
function beginState(store, state, pass) {
    const old = store.states[state.id];
    if (old !== undefined && old.pass === pass && old.complete !== true) {
        return {
            ...old,
            findings: (old.findings ?? []).filter((entry) =>
                entry.kind !== 'browser-died' && entry.kind !== 'stage-deadline'
                && entry.kind !== 'state-unreachable'),
            interrupted: false,
        };
    }
    return {
        id: state.id,
        title: state.title,
        pass,
        at: new Date().toISOString(),
        durationMs: 0,
        complete: false,
        interrupted: false,
        controlsFound: 0,
        controls: [],
        findings: [],
        consoleErrors: [],
        pageErrors: [],
        tabWalk: null,
        focusVisible: {},
        readability: null,
        shot: null,
        volatileKeys: [],
        deferredKeys: [],
    };
}

/*
 * Ein Menueeintrag darf nicht in seiner schon erreichten Zielstellung als
 * wirkungslos gelten.  `a-why` stellt die Frage sichtbar her; im Startzustand
 * ist sie absichtlich bereits sichtbar.  Dort wuerde ein echter Klick zwar
 * eintreffen, aber keinen zweiten sichtbaren Zustand erzeugen.  Wir lassen
 * ihn deshalb in diesem einen Zustand liegen und erfassen ihn im naechsten
 * Zustand mit geschlossener Frage (file-open).  Das ist kein Weglassen: der
 * Schluessel bleibt ungesehen und muss dort wirklich Maus und Tastatur
 * durchlaufen.
 */
function deferControl(state, descriptor) {
    return state.want.why === true && descriptor.selector.includes('data-menu="a-why"');
}

/**
 * Einen Zustand herstellen, seine Bedienelemente anfassen und ihn danach
 * fotografieren.
 *
 * Die Reihenfolge ist Absicht: erst messen, dann abbilden. Der Lauf davor hat
 * jeden Zustand zuerst fotografiert und die Lesbarkeit gemessen, und als die
 * Zeit ausging, lagen 21 Bilder da und null angefasste Elemente. Ein Bild ist
 * der Beleg einer Messung; ohne die Messung belegt es nichts.
 */
async function runState(page, state, record, ctx, options) {
    const { store, pass, deadline, readability } = options;

    await apply(page, state.want, ctx);
    await closeTooltips(page);
    await resetScroll(page, READABILITY_EXCLUSIONS);
    await page.waitForTimeout(320);

    /* Erst die echte, gegenwaertige Lage belegen. Ist eine angeforderte
     * Flaeche nicht hergestellt, darf ihre zufaellig sichtbare Restoberflaeche
     * nicht als vollstaendiger Zustand gesammelt werden. */
    if (!await requireCoverage(page, state, record)) {
        record.controlsFound = 0;
        await options.save();
        return { interrupted: false };
    }

    // Was sich von selbst bewegt, zuerst.
    const restA = await printOf(page);
    await page.waitForTimeout(700);
    const restB = await printOf(page);
    const volatileKeys = new Set(diffKeys(restA, restB, new Set()));
    record.volatileKeys = [...volatileKeys];

    const found = await page.evaluate(
        ({ selector, exclusions }) => globalThis.__w12.collect(selector, exclusions),
        { selector: CONTROL_SELECTOR, exclusions: COLLECT_EXCLUSIONS.map((entry) => entry.selector) },
    );
    record.controlsFound = found.length;

    // Die Tab-Wanderung dieses Zustands (AC5).
    let walk = { focusVisible: record.focusVisible ?? {}, summary: record.tabWalk };
    if (record.tabWalk === null || record.tabWalk === undefined) {
        walk = await tabWalk(page, found.length);
        record.tabWalk = walk.summary;
        record.focusVisible = walk.focusVisible;
        if (walk.summary.trap) {
            record.findings.push({
                kind: 'keyboard-trap',
                state: state.id,
                label: walk.summary.trapAt,
                detail: 'Tab fuehrte aus diesem Element nicht mehr heraus',
            });
        }
        if (!walk.summary.inDocumentOrder) {
            record.findings.push({
                kind: 'tab-order',
                state: state.id,
                label: walk.summary.firstOutOfOrder,
                detail: 'die Tab-Reihenfolge folgt an dieser Stelle nicht der Reihenfolge im Dokument',
            });
        }
        await apply(page, state.want, ctx);
    }

    const seen = seenKeys(store, pass, state.id);
    const mine = new Set((record.controls ?? []).map((entry) => entry.key));
    const deferred = new Set(record.deferredKeys ?? []);
    let interrupted = false;

    for (const descriptor of found) {
        const key = controlKey(descriptor);
        if (deferControl(state, descriptor)) {
            deferred.add(key);
            continue;
        }
        if (seen.has(key) || mine.has(key)) {
            continue;
        }
        if (Date.now() > deadline) {
            interrupted = true;
            record.interrupted = true;
            break;
        }
        mine.add(key);

        const control = await touchOne(page, state, descriptor, key, ctx, {
            volatileKeys,
            focusVisible: record.focusVisible ?? {},
            findings: record.findings,
        });
        record.controls.push(control);
        store.states[state.id] = record;
        await options.save();
    }

    /* Fertig ist der Zustand, wenn kein gefundenes Element mehr offen ist. */
    const open = found.filter((descriptor) => {
        const key = controlKey(descriptor);
        return !seen.has(key) && !mine.has(key) && !deferred.has(key);
    });
    record.deferredKeys = [...deferred];
    record.complete = open.length === 0 && !interrupted;

    // ------------------------------------------------ Erst jetzt das Bild
    if (record.complete || record.shot === null || record.shot === undefined) {
        await apply(page, state.want, ctx);
        /* Die Screenshot-Lage ist ebenfalls eine Messung, nicht bloss ein
         * Bild: React darf das Symbol nach einer Einzelprobe nicht verlieren. */
        await requireCoverage(page, state, record);
        await closeTooltips(page);
        await resetScroll(page, READABILITY_EXCLUSIONS);
        await page.waitForTimeout(260);
        const shot = join(SHOT_DIR, `${state.id}.png`);
        await page.screenshot({ path: shot, fullPage: false });
        record.shot = { file: `${state.id}.png`, bytes: (await stat(shot)).size };
        record.readability = await readability(state.id);
        record.findings = record.findings.filter((entry) =>
            entry.kind !== 'overlap' && entry.kind !== 'clipped'
            && entry.kind !== 'cut-without-hint' && entry.kind !== 'console-error'
            && entry.kind !== 'uncaught');
        for (const overlap of record.readability.detail.overlaps) {
            record.findings.push({
                kind: 'overlap',
                state: state.id,
                label: `${overlap.a} ueber ${overlap.b}`,
                detail: overlap.text,
            });
        }
        for (const clip of record.readability.detail.clipped) {
            record.findings.push({
                kind: clip.kind,
                state: state.id,
                label: clip.path,
                detail: clip.text,
            });
        }
        for (const entry of record.consoleErrors) {
            record.findings.push({
                kind: 'console-error', state: state.id, label: entry.text, detail: 'Konsolenfehler',
            });
        }
        for (const entry of record.pageErrors) {
            record.findings.push({
                kind: 'uncaught', state: state.id, label: entry, detail: 'unbehandelte Ausnahme',
            });
        }
    }

    return { interrupted };
}

/**
 * Ein einzelnes Bedienelement: Maus, Wiederherstellung, Tastatur,
 * Wiederherstellung, und die Frage nach dem Grund, wenn nichts geschah.
 */
async function touchOne(page, state, descriptor, key, ctx, options) {
    const { volatileKeys, findings } = options;
    const control = {
        key,
        state: state.id,
        label: descriptor.label,
        place: descriptor.place,
        selector: descriptor.selector,
        nth: descriptor.nth,
        semantic: descriptor.semantic,
        tag: descriptor.tag,
        role: descriptor.role,
        tabIndex: descriptor.tabIndex,
        disabled: descriptor.disabled === true,
        ariaDisabled: descriptor.ariaDisabled === true,
        mouse: { done: false, changed: false, via: '', keys: [] },
        keyboard: { done: false, changed: false, via: '', keys: [], focusable: false },
        /* Die Tab-Wanderung ist ein zusaetzlicher Gesamtbeleg. Dieser Wert
         * wird nach der individuellen Tastaturbedienung durch eine echte
         * Browsermessung ersetzt, damit auch Cursor-Zeilen mit tabindex=-1
         * (die per Liste erreichbar sind) nicht als unbekannt enden. */
        focusVisible: options.focusVisible[`${descriptor.selector}#${descriptor.nth}`] ?? null,
        noEffect: null,
    };

    /* Eine Pseudocode-Zeile kann selbst die feste Ausgangszeile sein. Dann
     * waere ihr Klick absichtlich idempotent, aber nicht mit einem anderen
     * Griff vergleichbar. Vor jeder der zwei Proben waehlen wir daher eine
     * andere gerade sichtbare Quellzeile ueber deren eigene Tastaturbedienung.
     * Der Reader-Caret im Fingerprint zeigt den echten Wechsel. */
    const resetPseudocodeLine = async () => {
        if (state.want.pseudocodeBase !== true) {
            return;
        }
        const base = await page.evaluate((target) => {
            const mains = [...document.querySelectorAll(
                '[data-testid="atlas-pseudocode-line"] button.atlas-pseudocode-line-btn',
            )].filter((entry) => globalThis.__w12.shown(entry));
            const targetNode = globalThis.__w12.resolve(target);
            if (targetNode === null || targetNode !== mains[0] || mains[1] === undefined) {
                return null;
            }
            return globalThis.__w12.describe(mains[1]);
        }, descriptor);
        if (base === null) {
            return;
        }
        await page.mouse.move(2, 2);
        await closeTooltips(page);
        await page.evaluate((entry) => {
            globalThis.__w12.bring(entry);
            return globalThis.__w12.focus(entry).focused;
        }, base);
        const before = await page.evaluate(() => ({
            path: globalThis.__atlasReader?.document?.path ?? '',
            line: globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? -1,
        }));
        await page.keyboard.press('Space');
        await page.waitForFunction(
            (prior) => (globalThis.__atlasReader?.document?.path ?? '') !== prior.path
                || (globalThis.__atlasReader?.editor?.getPosition?.()?.lineNumber ?? -1) !== prior.line,
            before,
            { timeout: 1200 },
        );
    };

    // ------------------------------------------------------------- Die Maus
    /*
     * Ein Element, das beim Anfassen nicht mehr da ist, bekommt EINE zweite
     * Gelegenheit, und zwar nach einer erneuten Wiederherstellung. Nicht aus
     * Nachsicht: eine Flaeche, die gerade neu gezeichnet wird, ist einen
     * Wimpernschlag lang leer.
     */
    await resetPseudocodeLine();
    let mouse = await touchWithMouse(page, descriptor, volatileKeys);
    if (!mouse.done && mouse.why.includes('nicht mehr da')) {
        await restoreDescriptor(page, state, descriptor, ctx);
        mouse = await touchWithMouse(page, descriptor, volatileKeys);
    }
    control.mouse = mouse;
    if (!mouse.done) {
        findings.push({
            kind: 'not-clickable',
            state: state.id,
            label: descriptor.label,
            detail: `${descriptor.place}: ${mouse.why}`,
        });
    }
    if (descriptor.ariaDisabled === true) {
        /* Diese Flaeche ist absichtlich nicht nativ gesperrt: Maus und
         * Tastatur schreiben jeweils dieselbe Galaxy-Notiz. Ein normales
         * apply() laesst die erste Notiz stehen und wuerde die zweite echte
         * Aktivierung deshalb als wirkungslos messen. Ein frischer Browser-
         * Zustand ist die ehrliche Ausgangslage fuer die zweite Bedienart. */
        await page.reload({ waitUntil: 'load' });
        await page.waitForSelector('[data-testid="atlas-statusbar"]', { timeout: 40000 });
        await page.waitForSelector('[data-testid="atlas-tree-row"]', { timeout: 40000 });
        await page.waitForFunction(() => globalThis.__atlasLayout !== undefined, undefined, { timeout: 40000 });
        await apply(page, state.want, ctx);
        if (!await restoreDescriptor(page, state, descriptor, ctx)) {
            throw new Error(`aria-disabled Griff ${descriptor.label} war nach dem frischen Aufbau nicht wiederhergestellt`);
        }
    } else {
        await apply(page, state.want, ctx);
    }

    // --------------------------------------------------------- Die Tastatur
    await resetPseudocodeLine();
    let keys = await touchWithKeyboard(page, descriptor, volatileKeys);
    if (!keys.done && keys.why.includes('nicht mehr da')) {
        await restoreDescriptor(page, state, descriptor, ctx);
        keys = await touchWithKeyboard(page, descriptor, volatileKeys);
    }
    control.keyboard = keys;
    const tabWalkRing = options.focusVisible[`${descriptor.selector}#${descriptor.nth}`] === true;
    control.focusEvidence = await page.evaluate((entry) => {
        const node = globalThis.__w12.resolve(entry);
        if (node === null) {
            return { visible: false, route: 'Element nicht mehr vorhanden' };
        }
        let nativeRing = false;
        try {
            nativeRing = document.activeElement === node && node.matches(':focus-visible');
        } catch {
            nativeRing = false;
        }
        /* Baum- und Trefferzeilen werden ueber ihren sichtbaren Cursor und
         * nicht ueber einen Tab-Stopp bedient. data-cursor ist die vom Produkt
         * gezeichnete Fokusmarkierung dieser Route. */
        const cursor = node.getAttribute('data-cursor') === 'true'
            || node.getAttribute('data-selected') === 'true';
        return {
            visible: nativeRing || cursor,
            route: nativeRing ? ':focus-visible' : (cursor ? 'sichtbarer Listen-Cursor' : 'kein Fokusindikator'),
        };
    }, descriptor);
    /* Manche Aktivierungen (Hilfe/Einstellungen) verschieben nachweisbar den
     * Fokus in die eben geoeffnete Flaeche. Ihr Ring ist trotzdem nicht
     * geraten: der Tab-Walk dieses Zustands hat ihn unmittelbar vorher am
     * Button gesehen. Ein Cursor mit tabindex=-1 hat diesen Weg nicht und
     * braucht weiterhin den individuellen Nachweis oben. */
    control.focusVisible = control.focusEvidence.visible || tabWalkRing;
    if (tabWalkRing && !control.focusEvidence.visible) {
        control.focusEvidence = { ...control.focusEvidence, route: 'sichtbar im Tab-Walk vor Aktivierung' };
    }
    if (!keys.done) {
        findings.push({
            kind: 'no-keyboard-route',
            state: state.id,
            label: descriptor.label,
            detail: `${descriptor.place}: ${keys.why}`,
        });
    }
    /*
     * Die zwei Formen des halben Elements (PLAN Abschnitt 4).
     *
     * Die erste: die Maus bewirkt etwas und die Tastatur nichts. Das ist ein
     * Element, das nur die Haelfte der Leser bedienen kann. Die zweite ist
     * leiser: das Element ist mit der Tabulatortaste erreichbar, antwortet dort
     * aber auf keine Taste, und nur ein Umweg ueber eine Liste bringt es dazu.
     */
    if (mouse.changed && !keys.changed) {
        findings.push({
            kind: 'keyboard-deaf',
            state: state.id,
            label: descriptor.label,
            detail: `${descriptor.place}: die Maus bewirkt etwas, die Tastatur nicht `
                + `(versucht: ${keys.via})`,
        });
    }
    if (!descriptor.disabled && !descriptor.ariaDisabled && !mouse.changed && keys.changed) {
        findings.push({
            kind: 'mouse-deaf',
            state: state.id,
            label: descriptor.label,
            detail: `${descriptor.place}: die Tastatur bewirkt etwas, die Maus nicht `
                + `(versucht: ${mouse.via})`,
        });
    }
    if (keys.viaList && descriptor.tabIndex >= 0) {
        findings.push({
            kind: 'tab-stop-deaf',
            state: state.id,
            label: descriptor.label,
            detail: `${descriptor.place}: mit der Tabulatortaste erreichbar, antwortet `
                + 'aber auf keine eigene Taste; nur der Umweg ueber die Liste wirkt',
        });
    }
    await apply(page, state.want, ctx);

    // ----------------------------------------------------------- Ohne Wirkung?
    if (mouse.done && keys.done && !mouse.changed && !keys.changed) {
        const excuse = await page.evaluate(
            (entry) => globalThis.__w12.excuse(entry),
            descriptor,
        );
        control.excuse = excuse;
        control.noEffect = { reason: excuse.found ? excuse.text : '', where: excuse.where };
        if (!excuse.found) {
            findings.push({
                kind: 'did-nothing',
                state: state.id,
                label: descriptor.label,
                detail: `${descriptor.place}: weder Maus noch Tastatur bewirkten etwas, `
                    + 'und die Flaeche sagt keinen Grund',
            });
        }
    }
    /* Native disabled Controls koennen keinen Fokus annehmen. Das ist nur
     * dann eine begruendete Ausnahme, wenn beide Messwege ehrlich fertig sind
     * und die markierte Produktflaeche selbst einen hinreichenden Grund zeigt.
     * aria-disabled bleibt dagegen bewusst fokuspflichtig. */
    const focusExempt = control.disabled === true
        && control.keyboard.focusable === false
        && control.mouse.done === true
        && control.keyboard.done === true
        && control.excuse?.marked === true
        && typeof control.noEffect?.reason === 'string'
        && control.noEffect.reason.length > 10;
    if (!control.focusVisible && !focusExempt) {
        findings.push({
            kind: 'focus-not-visible',
            state: state.id,
            label: descriptor.label,
            detail: `${descriptor.place}: ${control.focusEvidence.route}`,
        });
    }
    return control;
}

// ------------------------------------------------------------ Das Artefakt ---

/** buttons.json und buttons.md, aus dem Speicher heraus, bei jedem Fortschritt. */
async function writeArtifacts(store, stage, timings, failure) {
    const report = aggregate(store);
    await writeFile(
        OUT_JSON,
        JSON.stringify({
            ...report,
            project: PROJECT,
            fixture: 'fixtures/atlas-sample (nur gelesen)',
            method:
                'Die Bedienelemente stehen nicht in diesem Lauf: gesammelt wird ueber die Gattung '
                + `(${CONTROL_SELECTOR}) in jedem Zustand, den der Lauf selbst herstellt. Jedes `
                + 'gefundene Element wird einmal mit der Maus angefasst und einmal mit der Tastatur, '
                + 'auf dem Weg, den seine Rolle vorsieht (Leertaste am Knopf, Pfeiltasten am Trenner '
                + 'und am Regler, der Cursor der Liste an einer Baumzeile, die Kommandozeile an einer '
                + 'Trefferzeile). Als Wirkung zaehlt jede Aenderung am Abdruck der Seite ausser den '
                + 'fluechtigen Schluesseln, die in demselben Zustand ohne Zutun gemessen wurden.',
            filterMethod:
                'Ein Filter wird nicht daran gemessen, dass sich etwas aendert, sondern daran, dass '
                + 'eine Menge kleiner wird: Zeilen im Twin, gezeichnete Kanten, Zeilen im Instrument, '
                + 'Koerper auf dem Graphen. Abschalten muss die Zahl senken, Einschalten sie '
                + 'zurueckbringen; beide Zahlen stehen unter filterCounts.',
            tabOrderMethod:
                'tabOrderFollowsLayout heisst: die Tab-Wanderung besucht die fokussierbaren Elemente '
                + 'in der Reihenfolge des Dokuments, und kein Element traegt einen positiven '
                + 'tabindex. Das Raster dieser Oberflaeche zeichnet in Dokumentreihenfolge (Kopf, '
                + 'Reiter, Explorer, Mitte, rechte Spalte, Kommandozeile, Statusleiste), also ist '
                + 'die Dokumentreihenfolge hier die sichtbare Ordnung; wo sie es nicht waere, stuende '
                + 'die Abweichung unter tabWalks.',
            stageMethod:
                'Dieser Lauf faellt in Etappen: ein Aufruf misst eine Teilmenge (W12_STATES, '
                + 'W12_FILTERS), schreibt nach jedem Zustand und nach jedem angefassten Element fort '
                + 'und fuehrt seine Messung mit der vorhandenen buttons.json zusammen. Eine RUNDE '
                + 'ist kein Aufruf, sondern ein vollstaendiger Durchgang durch alle Zustaende und '
                + 'alle Filter; erst wenn der geschlossen ist, sagt seine Zahl der Befunde etwas '
                + 'ueber die Oberflaeche. Ein Browser traegt hoechstens fuenf Einheiten, danach wird '
                + 'er frisch geoeffnet; stirbt er vorher, steht das als Befund mit Zustandsnamen da.',
            pass: store.pass,
            stage: {
                n: stage.n,
                at: stage.at,
                states: stage.states,
                filters: stage.filters,
                findings: stage.findings,
                browsers: stage.browsers,
                browserDeaths: stage.browserDeaths,
                hitDeadline: stage.hitDeadline,
                budgetMs: stage.budgetMs,
                durationMs: stage.durationMs ?? null,
                recoveries: stage.recoveries,
            },
            timings,
            generatedAt: new Date().toISOString(),
            error: failure ? failure.message : null,
            extras: {
                selector: CONTROL_SELECTOR,
                exclusions: COLLECT_EXCLUSIONS,
                /* Die abgeleiteten Gesamtzahlen oben und diese Rohmessungen
                 * gehoeren zusammen: Der Frozen-Test kann so jede Zahl bis
                 * zum einzelnen, im Browser gefundenen Element nachrechnen. */
                controls: report.controls,
                tabWalks: report.tabWalks,
                blockedRequests: stage.blockedRequests,
                /* Die Agentenwerte stammen jeweils direkt aus dem frischen
                 * lokalen Bridge-Health, seinem HTTP-Advance und der danach
                 * sichtbaren Produktnaht. */
                agentReplayReadings: store.agentReplayReadings ?? {},
                store,
            },
        }, null, 2) + '\n',
        'utf8',
    );
    await writeFile(OUT_MD, overview(report, store), 'utf8');
    return report;
}

// ------------------------------------------------------ Die drei Beruehrungen --

/**
 * Einen eben aus dem sichtbaren Anker aufgeloesten Griff fuer Playwright
 * markieren. `Locator.nth()` waere nach einem Neuzeichnen wieder die globale
 * Button-Position und koennte eine andere Twin-Zeile anfassen.
 */
async function claimedLocator(page, descriptor) {
    const token = await page.evaluate((entry) => globalThis.__w12.claim(entry), descriptor);
    if (token.length === 0) {
        return null;
    }
    return {
        locator: page.locator(`[data-w12-claim="${token}"]`),
        release: () => page.evaluate((value) => globalThis.__w12.release(value), token),
    };
}

/**
 * Ein Bug-Hop kann auf das bereits gelesene Symbol zeigen. Dann ist ein
 * unveraenderter Gesamtfingerprint keine taube Tastatur, wenn der echte
 * Browser-Klick am strukturellen Hop ankam und das Produkt denselben Ziel- oder
 * Unaufloesbarkeitszustand ausweist. Andere Buttons bekommen diese Ausnahme
 * nicht: fuer sie bleibt allein die sichtbare Zustandsaenderung ein Beleg.
 */
async function bugHopEquivalent(page, descriptor, activationToken) {
    if (descriptor.semantic !== 'bug-hop' || activationToken.length === 0) {
        return null;
    }
    try {
        let evidence = null;
        for (let attempt = 0; attempt < 8; attempt += 1) {
            evidence = await page.evaluate(
                ({ entry, token }) => globalThis.__w12.bugHopEvidence(entry, token),
                { entry: descriptor, token: activationToken },
            );
            if (evidence.event?.trusted === true && evidence.event.targetMatches === true
                && evidence.equivalent === true) {
                return evidence;
            }
            await page.waitForTimeout(180);
        }
        return evidence;
    } finally {
        await page.evaluate((token) => globalThis.__w12.releaseActivation(token), activationToken)
            .catch(() => undefined);
    }
}

const armTransition = (page, before, volatileKeys) => page.evaluate(
    ({ print, volatile }) => globalThis.__w12.armFingerprintTransition(print, volatile),
    { print: before, volatile: [...volatileKeys] },
);
const finishTransition = (page, token) => page.evaluate(
    (value) => globalThis.__w12.finishFingerprintTransition(value), token,
).catch(() => ({ mutationCount: 0, changed: [] }));

/** Ein Element mit der Maus anfassen und messen, was daraufhin geschah. */
async function touchWithMouse(page, descriptor, volatileKeys) {
    const result = { done: false, changed: false, via: 'click', keys: [], why: '' };
    /*
     * Erst den Zeiger weglegen und den offenen Kasten schliessen.
     *
     * Ein Tooltip, den das vorige Element geoeffnet hat, liegt fest positioniert
     * ueber der Seite. Playwright klickt nur, wenn das Ziel den Zeiger wirklich
     * bekommt, also wartet es vier Sekunden auf einen Kasten, der von selbst
     * nicht weggeht, und meldet dann "nicht anklickbar". In der ersten Runde traf
     * das den Hierarchie-Schalter, der mit der Tastatur einwandfrei ging.
     */
    await page.mouse.move(2, 2);
    await closeTooltips(page);
    const brought = await page.evaluate((entry) => globalThis.__w12.bring(entry), descriptor);
    if (!brought) {
        result.why = 'das Element war beim Anfassen nicht mehr da';
        return result;
    }
    await page.waitForTimeout(140);
    /*
     * Ein vom Browser gesperrtes Element wird nicht geklickt, sondern beruehrt.
     *
     * `disabled` heisst: es nimmt keine Zeigerereignisse und keinen Fokus. Ein
     * Klickversuch darauf endet in einer Wartezeit und saehe im Bericht aus wie
     * "nicht anklickbar", also wie ein Fehler des Layouts. Es ist aber eine
     * ABSICHT, und die Frage an sie ist eine andere: sagt die Flaeche, warum es
     * gesperrt ist? Genau diese Frage stellt AC3, und sie wird weiter unten
     * gestellt, wo jedes wirkungslose Element seinen Grund vorzeigen muss.
    */
    if (descriptor.disabled) {
        const locked = await page.evaluate((entry) => {
            const node = globalThis.__w12.resolve(entry);
            return node !== null && node.hasAttribute('disabled') && node.matches(':disabled');
        }, descriptor);
        if (locked === true) {
            result.done = true;
            result.via = 'gesperrt, kein Klick moeglich';
            return result;
        }
        /* Nach einem Re-Render darf ein frueher disabled Descriptor nie auf
         * einen anderen Button fallen und ihn anklicken. touchOne erkennt
         * diesen ehrlichen Missing-Grund, stellt einmal wieder her und misst
         * dann erneut; bleibt er rot, ist es not-clickable. */
        result.why = 'der native disabled Griff war beim Anfassen nicht mehr da';
        return result;
    }
    const claimed = await claimedLocator(page, descriptor);
    if (claimed === null) {
        result.why = 'das Element war beim Anfassen nicht mehr da';
        return result;
    }
    /* The dialog may have focused its search field while it opened. That is an
     * initial condition, not evidence that this mouse route focused it. */
    if (descriptor.selector.includes('atlas-entry-input')) {
        await page.evaluate((entry) => {
            const node = globalThis.__w12.resolve(entry);
            if (node !== null && document.activeElement === node && node instanceof HTMLElement) {
                node.blur();
            }
        }, descriptor);
        await page.waitForFunction((entry) => {
            const node = globalThis.__w12.resolve(entry);
            return node !== null && document.activeElement !== node;
        }, descriptor, { timeout: 1000 });
    }
    const before = await printOf(page);
    /* Playwright behandelt aria-disabled bei locator.click() wie ein nativ
     * gesperrtes Element. Diese Galaxy-Flaeche ist aber absichtlich anders:
     * sie bleibt per Maus bedienbar und schreibt erst dann ihre sichtbare
     * Begruendung. Deshalb pruefen wir ihren echten Hit-Test und senden den
     * Maus-Klick an genau diesen Punkt, statt den Browser mit force zu
     * ueberreden. */
    const ariaPoint = descriptor.ariaDisabled === true
        ? await page.evaluate((entry) => {
            const node = globalThis.__w12.resolve(entry);
            if (node === null) return null;
            const rect = node.getBoundingClientRect();
            const x = rect.left + rect.width / 2;
            const y = rect.top + rect.height / 2;
            const hit = document.elementFromPoint(x, y);
            return hit !== null && (hit === node || node.contains(hit)) ? { x, y } : null;
        }, descriptor)
        : null;
    if (descriptor.ariaDisabled === true && ariaPoint === null) {
        await claimed.release();
        result.why = 'aria-disabled Flaeche bekam am sichtbaren Mittelpunkt keinen Maus-Hit';
        return result;
    }
    const activationToken = descriptor.semantic === 'bug-hop'
        ? await page.evaluate((entry) => globalThis.__w12.armActivation(entry), descriptor)
        : '';
    const transitionToken = await armTransition(page, before, volatileKeys);
    let mouseRoute = 'click';
    let entryFocused = false;
    try {
        if (descriptor.role === 'separator') {
            const box = await claimed.locator.boundingBox();
            if (box === null) throw new Error('der Separator hat keine sichtbare Mausflaeche');
            const horizontal = box.width >= box.height;
            const from = { x: box.x + box.width / 2, y: box.y + box.height / 2 };
            const to = horizontal
                ? { x: from.x, y: from.y + Math.max(12, Math.min(36, box.height * 3)) }
                : { x: from.x + Math.max(12, Math.min(36, box.width * 3)), y: from.y };
            await page.mouse.move(from.x, from.y);
            await page.mouse.down();
            await page.mouse.move(to.x, to.y, { steps: 6 });
            await page.mouse.up();
            mouseRoute = 'drag';
        } else if (descriptor.selector.includes('atlas-twin-depth')) {
            const box = await claimed.locator.boundingBox();
            if (box === null) throw new Error('der Reader-Range hat keine sichtbare Mausflaeche');
            const position = await page.evaluate((entry) => {
                const node = globalThis.__w12.resolve(entry);
                if (!(node instanceof HTMLInputElement)) return 0.75;
                const min = Number(node.min || 0);
                const max = Number(node.max || 1);
                const value = Number(node.value);
                return value <= (min + max) / 2 ? 0.8 : 0.2;
            }, descriptor);
            await page.mouse.click(box.x + box.width * position, box.y + box.height / 2);
            mouseRoute = 'range position';
        } else if (descriptor.selector.includes('atlas-entry-input')) {
            await claimed.locator.click({ timeout: 4000 });
            entryFocused = await page.evaluate((entry) => {
                const node = globalThis.__w12.resolve(entry);
                return node !== null && document.activeElement === node && globalThis.__w12.shown(node);
            }, descriptor);
            mouseRoute = 'mouse focus';
        } else if (ariaPoint !== null) {
            await page.mouse.click(ariaPoint.x, ariaPoint.y);
        } else {
            await claimed.locator.click({ timeout: 4000 });
        }
    } catch (error) {
        result.why = String(error.message ?? error).replace(/\s+/g, ' ').slice(0, 1200);
        await claimed.release();
        await finishTransition(page, transitionToken);
        if (activationToken.length > 0) await page.evaluate((token) => globalThis.__w12.releaseActivation(token), activationToken);
        return result;
    }
    await claimed.release();
    /* Eine Entry-Zeile schliesst ihren Dialog sofort, startet die Fuehrung aber
     * erst asynchron. Der fruehe Dialogwechsel ist noch keine Wirkung der
     * Zeile: erst die echte Tour liefert den stabilen Folgezustand, aus dem
     * apply anschliessend verlaesslich wieder aussteigen kann. */
    if (descriptor.semantic === 'entry-row') {
        try {
            await page.waitForFunction(
                () => Number(globalThis.__atlasTour?.steps ?? 0) > 0,
                undefined,
                { timeout: 30000 },
            );
        } catch {
            await finishTransition(page, transitionToken);
            result.why = 'die Entry-Zeile startete innerhalb von 30 s keine Fuehrung';
            return result;
        }
    }
    result.done = true;
    const after = await settle(page, before, volatileKeys);
    const transition = await finishTransition(page, transitionToken);
    result.transition = transition;
    result.keys = [...new Set([...after.changed, ...transition.changed])].slice(0, 8);
    result.changed = result.keys.length > 0 || entryFocused;
    result.via = mouseRoute;
    if (entryFocused && !result.keys.includes('focus')) result.keys.push('focus');
    const semantic = await bugHopEquivalent(page, descriptor, activationToken);
    if (semantic !== null) result.semantic = semantic;
    if (!result.changed && semantic !== null && semantic.event?.trusted === true
        && semantic.event.targetMatches === true && semantic.equivalent === true) {
        result.changed = true;
        result.via = 'trusted click mit gleichem Bug-Ziel';
        result.keys = ['bug-hop-equivalent'];
        result.semantic = semantic;
    }
    if (result.changed) {
        return result;
    }

    /*
     * Zweite Gelegenheit, und nur fuer die, die sie verdienen.
     *
     * Ein Element, das seinen Zustand ANZEIGT und auf "an" steht, kann durch
     * einen Klick nichts aendern: es ist schon dort, wo der Klick es
     * hinbraechte. Es geht darum ueber einen Nachbarn derselben Gruppe weg und
     * kommt zurueck. Fuer ein Element ohne angezeigten Zustand waere derselbe
     * Umweg eine Behauptung: dass es zu einer Gruppe gehoert, in der es
     * abwechselnd gilt, und das ist bei einem Knopf, der etwas TUT, nicht so.
     */
    if (descriptor.pressed !== 'true') {
        return result;
    }
    const sibling = await page.evaluate((entry) => globalThis.__w12.sibling(entry), descriptor);
    if (sibling === null) {
        return result;
    }
    try {
        const siblingClaim = await claimedLocator(page, sibling);
        if (siblingClaim === null) {
            return result;
        }
        await siblingClaim.locator.click({ timeout: 4000 });
        await siblingClaim.release();
    } catch {
        return result;
    }
    await page.waitForTimeout(400);
    const middle = await printOf(page);
    try {
        const backClaim = await claimedLocator(page, descriptor);
        if (backClaim === null) {
            return result;
        }
        await backClaim.locator.click({ timeout: 4000 });
        await backClaim.release();
    } catch {
        return result;
    }
    const back = await settle(page, middle, volatileKeys);
    result.via = 'click nach Umweg ueber einen Nachbarn';
    result.keys = back.changed.slice(0, 8);
    result.changed = back.changed.length > 0;
    return result;
}

/**
 * Ein Element mit der Tastatur anfassen, auf dem Weg, den seine Rolle vorsieht.
 *
 * Die Reihenfolge ist die der Wahrscheinlichkeit und nicht des Zufalls: die
 * Leertaste zuerst, weil sie die native Betaetigung eines Knopfes ist und weil
 * diese Oberflaeche sie ausdruecklich in Ruhe laesst (src/app/keyboard.ts:
 * "Das Leerzeichen nicht"). Enter danach, denn waehrend einer Fuehrung gehoert
 * Enter der Fuehrung, und ein Lauf, der damit anfinge, wuerde die Wirkung der
 * Fuehrung fuer die des Knopfes halten.
 */
async function touchWithKeyboard(page, descriptor, volatileKeys) {
    const result = {
        done: false, changed: false, via: '', keys: [], focusable: false, direct: false,
        viaList: false, why: '',
    };
    await page.evaluate((entry) => globalThis.__w12.bring(entry), descriptor);
    await page.waitForTimeout(120);

    const route = keyboardRoute(descriptor);

    if (descriptor.disabled) {
        const claimed = await claimedLocator(page, descriptor);
        const locked = claimed === null ? false : await claimed.locator.isDisabled().catch(() => false);
        await claimed?.release();
        if (locked) {
            result.done = true;
            result.via = 'gesperrt, nimmt keinen Fokus';
            return result;
        }
    }

    const focused = await page.evaluate((entry) => globalThis.__w12.focus(entry), descriptor);
    if (!focused.found) {
        result.why = 'das Element war beim Fokussieren nicht mehr da';
        return result;
    }
    result.focusable = focused.focused;

    /*
     * Zuerst die eigenen Tasten, und zwar IMMER.
     *
     * Auch dann, wenn es fuer dieses Element einen Weg ueber eine Liste gibt.
     * Der Unterschied ist genau die Frage aus PLAN Abschnitt 4: ein Element,
     * das die Tabulatortaste erreicht und das auf die eigene Taste nicht
     * antwortet, ist ein halbes Element, auch wenn ein anderer Weg zu ihm
     * fuehrt. Was hier gelingt, steht als `direct` im Artefakt; was erst ueber
     * die Liste gelingt, als `viaList`.
     */
    if (focused.focused) {
        for (const key of route.keys) {
            const before = await printOf(page);
            /* Der Hop ist nach dem Wiederaufbau ein bestimmtes sichtbares
             * Element, nicht nur der gerade aktive Fokus. `locator.press`
             * sendet weiterhin eine echte Browser-Taste, bindet sie aber an
             * genau diesen strukturellen Hop; das verhindert, dass ein noch
             * nachlaufender Fokuswechsel die Leertaste in die Kommandozeile
             * lenkt. */
            const keyClaim = descriptor.semantic === 'bug-hop'
                ? await claimedLocator(page, descriptor) : null;
            const activationToken = descriptor.semantic === 'bug-hop'
                ? await page.evaluate((entry) => globalThis.__w12.armActivation(entry), descriptor)
                : '';
            const browserKey = key === ' ' ? 'Space' : key;
            const transitionToken = await armTransition(page, before, volatileKeys);
            if (keyClaim !== null) {
                await keyClaim.locator.press(browserKey);
                await keyClaim.release();
            } else {
                await page.keyboard.press(browserKey);
            }
            /* Siehe die Mausroute: das sofortige Schliessen des Dialogs ist
             * nur ein Zwischenzustand. Direkte Tastaturaktivierung gilt erst
             * nach dem beobachteten Start der echten Fuehrung als abgeschlossen. */
            if (descriptor.semantic === 'entry-row') {
                try {
                    await page.waitForFunction(
                        () => Number(globalThis.__atlasTour?.steps ?? 0) > 0,
                        undefined,
                        { timeout: 30000 },
                    );
                } catch {
                    await finishTransition(page, transitionToken);
                    result.why = 'die Entry-Zeile startete innerhalb von 30 s keine Fuehrung';
                    return result;
                }
            }
            const after = await settle(page, before, volatileKeys, 450);
            const transition = await finishTransition(page, transitionToken);
            const changed = [...new Set([...after.changed, ...transition.changed])];
            if (changed.length > 0) {
                const semantic = await bugHopEquivalent(page, descriptor, activationToken);
                result.done = true;
                result.changed = true;
                result.direct = true;
                result.via = browserKey;
                result.keys = changed.slice(0, 8);
                result.transition = transition;
                if (semantic !== null) result.semantic = semantic;
                return result;
            }
            const semantic = await bugHopEquivalent(page, descriptor, activationToken);
            if (semantic !== null && semantic.event?.trusted === true
                && semantic.event.targetMatches === true && semantic.equivalent === true) {
                result.done = true;
                result.changed = true;
                result.direct = true;
                result.via = `${key} mit gleichem Bug-Ziel`;
                result.keys = ['bug-hop-equivalent'];
                result.semantic = semantic;
                return result;
            }
            // Der Fokus kann durch die vorige Taste weggewandert sein.
            await page.evaluate((entry) => globalThis.__w12.focus(entry), descriptor);
        }
        result.done = true;
        result.via = route.keys.join(' dann ');
    } else {
        result.why = 'das Element nahm den Fokus nicht an';
    }

    if (route.list === undefined) {
        /*
         * Ein bereits gewaehlter Reiter ist kein tauber Reiter.  Die
         * Mausprobe stellt oben schon durch seinen echten Nachbarn die
         * Gegenlage her und waehlt ihn dann wieder.  Die Tastatur muss
         * dieselbe Gegenprobe fahren; sonst vergliche der Bericht einen
         * Zustandswechsel der Maus mit einer idempotenten Wiederholung der
         * Tastatur.
         */
        if (result.changed || descriptor.pressed !== 'true' || !result.focusable) {
            return result;
        }
        const sibling = await page.evaluate((entry) => globalThis.__w12.sibling(entry), descriptor);
        if (sibling === null) {
            return result;
        }
        try {
            const siblingClaim = await claimedLocator(page, sibling);
            if (siblingClaim === null) {
                return result;
            }
            await siblingClaim.locator.click({ timeout: 1200 });
            await siblingClaim.release();
            await page.waitForTimeout(120);
            await page.evaluate((entry) => globalThis.__w12.focus(entry), descriptor);
            const before = await printOf(page);
            await page.keyboard.press((route.keys[0] ?? ' ') === ' ' ? 'Space' : (route.keys[0] ?? ' '));
            const after = await settle(page, before, volatileKeys, 450);
            if (after.changed.length > 0) {
                return {
                    ...result,
                    changed: true,
                    direct: true,
                    via: `${(route.keys[0] ?? ' ') === ' ' ? 'Space' : (route.keys[0] ?? ' ')} nach Umweg ueber einen Nachbarn`,
                    keys: after.changed.slice(0, 8),
                };
            }
        } catch {
            // Die urspruengliche Messung bleibt der ehrliche Befund.
        }
        return result;
    }
    const listed = await keyboardThroughList(page, descriptor, volatileKeys, {
        list: route.list,
        cursor: route.cursor,
        result: { ...result },
    });
    if (listed.changed) {
        return { ...listed, direct: false, viaList: true };
    }
    return listed.done ? { ...listed, direct: result.direct, viaList: false } : result;
}

/** Der Weg der Tastatur ueber eine Liste, die ihren eigenen Cursor fuehrt. */
async function keyboardThroughList(page, descriptor, volatileKeys, options) {
    const { result } = options;
    const target = await page.evaluate((entry) => {
        const node = globalThis.__w12.resolve(entry);
        if (node === null) {
            return -1;
        }
        const family = [...document.querySelectorAll(
            `[data-testid="${node.getAttribute('data-testid')}"]`,
        )];
        return family.indexOf(node);
    }, descriptor);
    if (target < 0) {
        result.why = 'die Zeile war beim Anfassen nicht mehr da';
        return result;
    }
    await page.locator(options.list).first().focus();
    result.focusable = true;
    for (let step = 0; step < 80; step += 1) {
        const at = await page.evaluate((name) => globalThis.__w12[name](), options.cursor);
        if (at.at === target) {
            break;
        }
        await page.keyboard.press(at.at < target || at.at < 0 ? 'ArrowDown' : 'ArrowUp');
        await page.waitForTimeout(18);
    }
    const at = await page.evaluate((name) => globalThis.__w12[name](), options.cursor);
    if (at.at !== target) {
        result.why = 'der Cursor der Liste erreichte diese Zeile nicht';
        return result;
    }
    const before = await printOf(page);
    const transitionToken = await armTransition(page, before, volatileKeys);
    await page.keyboard.press('Enter');
    const after = await settle(page, before, volatileKeys, 450);
    const transition = await finishTransition(page, transitionToken);
    const changed = [...new Set([...after.changed, ...transition.changed])];
    result.done = true;
    result.via = 'Cursor der Liste, dann Enter';
    result.changed = changed.length > 0;
    result.keys = changed.slice(0, 8);
    result.transition = transition;
    return result;
}

/**
 * Welche Tasten dieses Element wirklich bedienen.
 *
 * Die Zuordnung folgt der Rolle und nicht dem Geschmack. Ein Trenner
 * (`role="separator"`, die vier Griffe zwischen den Zonen) hoert auf die
 * Pfeiltasten, sonst nichts; ein Schieberegler auch; eine Zeile im Explorer
 * traegt `tabindex="-1"` und wird ueber den Cursor der Liste erreicht, so wie
 * ein Leser es tut; eine Trefferzeile der Suche wird ueber die Kommandozeile
 * gewaehlt, weil dort der Cursor des Fensters liegt.
 */
function keyboardRoute(descriptor) {
    if (descriptor.selector.includes('atlas-tree-row')) {
        return { keys: [' ', 'Enter'], list: '[role="tree"]', cursor: 'treeCursor' };
    }
    if (descriptor.selector.includes('atlas-search-row')) {
        return {
            keys: [' ', 'Enter'],
            list: '[data-testid="atlas-command-input"]',
            cursor: 'searchCursor',
        };
    }
    if (descriptor.role === 'separator') {
        return { keys: ['ArrowRight', 'ArrowDown', 'ArrowLeft', 'ArrowUp'] };
    }
    if (descriptor.role === 'slider' && descriptor.tag !== 'input') {
        return { keys: ['ArrowRight', 'ArrowLeft', 'ArrowDown', 'ArrowUp'] };
    }
    if (descriptor.tag === 'input' && descriptor.type === 'range') {
        return { keys: ['ArrowRight', 'ArrowLeft'] };
    }
    if (descriptor.tag === 'input' || descriptor.tag === 'textarea') {
        return { keys: ['KeyA', 'Backspace', 'Enter'] };
    }
    if (descriptor.tag === 'select') {
        return { keys: ['ArrowDown', 'ArrowUp'] };
    }
    return { keys: [' ', 'Enter'] };
}

// ---------------------------------------------------------- Die Tab-Wanderung -

/**
 * Einmal mit der Tabulatortaste durch den Zustand.
 *
 * Gemessen werden drei Dinge: die Reihenfolge (folgt sie dem Dokument), die
 * Falle (kommt Tab aus einem Element nicht mehr heraus) und der Fokusring (ist
 * an dem Element, auf dem der Fokus steht, ueberhaupt etwas anders als vorher).
 * Der dritte Punkt braucht die Wanderung, weil `:focus-visible` genau dann
 * gilt, wenn der Fokus ueber die Tastatur gekommen ist: ein `focus()` aus einem
 * Skript ist fuer diese Frage nicht dasselbe.
 */
async function tabWalk(page, expected) {
    const resting = await page.evaluate(
        ({ selector, exclusions }) => {
            const out = {};
            for (const node of document.querySelectorAll(selector)) {
                if (globalThis.__w12.excluded(node, exclusions) || !globalThis.__w12.shown(node)) {
                    continue;
                }
                const entry = globalThis.__w12.describe(node);
                out[entry.selector + '#' + entry.nth] = globalThis.__w12.styleOf(node);
            }
            return out;
        },
        { selector: CONTROL_SELECTOR, exclusions: COLLECT_EXCLUSIONS.map((entry) => entry.selector) },
    );

    const order = await page.evaluate(
        ({ selector, exclusions }) => {
            const out = [];
            for (const node of document.querySelectorAll(selector)) {
                if (globalThis.__w12.excluded(node, exclusions) || !globalThis.__w12.shown(node)) {
                    continue;
                }
                if (node.tabIndex < 0) {
                    continue;
                }
                const entry = globalThis.__w12.describe(node);
                out.push(entry.selector + '#' + entry.nth);
            }
            return out;
        },
        { selector: CONTROL_SELECTOR, exclusions: COLLECT_EXCLUSIONS.map((entry) => entry.selector) },
    );

    const positive = await page.evaluate(() =>
        [...document.querySelectorAll('[tabindex]')]
            .filter((node) => node.tabIndex > 0)
            .map((node) => node.getAttribute('data-testid') ?? node.tagName));

    await page.evaluate(() => {
        if (document.activeElement instanceof HTMLElement) {
            document.activeElement.blur();
        }
    });

    const visited = [];
    const focusVisible = {};
    let trap = false;
    let trapAt = '';
    let stuck = 0;
    let previous = '';
    const budget = Math.min(260, Math.max(60, (expected + 12) * 2));
    for (let step = 0; step < budget; step += 1) {
        await page.keyboard.press('Tab');
        const active = await page.evaluate(() => globalThis.__w12.active());
        if (active.body) {
            previous = '';
            stuck = 0;
            continue;
        }
        const id = `${active.selector}#${active.nth}`;
        if (id === previous) {
            stuck += 1;
            if (stuck >= 3 && !active.inEditor) {
                trap = true;
                trapAt = `${active.place}: ${active.label}`;
                break;
            }
            if (stuck >= 8) {
                trap = true;
                trapAt = `${active.place}: ${active.label} (Editorflaeche)`;
                break;
            }
        } else {
            stuck = 0;
        }
        previous = id;
        if (!active.inEditor && resting[id] !== undefined && focusVisible[id] === undefined) {
            focusVisible[id] = active.style !== resting[id];
        }
        if (visited.includes(id)) {
            if (visited.length >= order.length) {
                break;
            }
        } else {
            visited.push(id);
        }
    }

    const reached = order.filter((id) => visited.includes(id));
    const sequence = visited.filter((id) => order.includes(id));
    /*
     * Die Reihenfolge ist ein KREIS, und das ist keine Nachsicht.
     *
     * Die Wanderung faengt dort an, wo der Fokus zuletzt stand, und nicht am
     * Anfang des Dokuments: Chromium merkt sich den Ausgangspunkt der
     * Fokuswanderung, und der Lauf hat vorher geklickt. Sie laeuft also von der
     * Mitte bis ans Ende und dann wieder vom Anfang. Genau EIN solcher
     * Ruecksprung gehoert dazu; ein zweiter waere ein Sprung mitten in der
     * Reihe, und der ist der Fehler, um den es geht.
     */
    const places = sequence.map((id) => order.indexOf(id));
    const descents = places.filter((place, index) => index > 0 && place < places[index - 1]).length;
    let firstOutOfOrder = '';
    let inOrder = positive.length === 0 && descents <= 1;
    if (positive.length > 0) {
        firstOutOfOrder = `positiver tabindex an ${positive.join(', ')}`;
    } else if (descents > 1) {
        const at = places.findIndex((place, index) =>
            index > 0 && place < places[index - 1] && places.slice(0, index)
                .some((earlier, other) => other > 0 && earlier < places[other - 1]));
        firstOutOfOrder = sequence[at] ?? sequence[sequence.length - 1] ?? '';
    }

    return {
        focusVisible,
        summary: {
            focusable: order.length,
            visited: visited.length,
            reached: reached.length,
            missed: order.filter((id) => !visited.includes(id)).slice(0, 12),
            trap,
            trapAt,
            inDocumentOrder: inOrder,
            wraps: descents,
            firstOutOfOrder,
            positiveTabIndex: positive,
        },
    };
}

// ------------------------------------------------------------ Die Filterprobe -

/**
 * Einen Filter messen: was steht vorher da, was nachher, und kommt es zurueck.
 *
 * Der Weg ist immer derselbe, egal ob es eine Linse, eine Kantenart, ein
 * Akteursfilter oder ein Effektschalter ist: Zustand herstellen, zaehlen,
 * abschalten, zaehlen, einschalten, zaehlen. Erst diese dritte Zahl macht aus
 * "es ist weniger geworden" eine Aussage ueber den Schalter statt ueber die
 * Reihenfolge der Klicks.
 */
async function measureFilter(page, filter, state, ctx) {
    const out = {
        name: filter.name,
        kind: filter.kind,
        unit: filter.unit,
        state: filter.state,
        before: 0,
        after: 0,
        again: 0,
        removes: false,
        restored: false,
        explains: false,
        emptyCase: false,
        noise: 0,
        sentence: '',
        why: '',
    };
    try {
        await apply(page, state.want, ctx);
        const read = async () => {
            await page.waitForTimeout(360);
            if (filter.measure === 'pixels') {
                const pixels = await litPixels(page);
                if (!Number.isFinite(pixels)) throw new Error('Pixelprobe lieferte keine endliche Zahl');
                return pixels;
            }
            const counts = await countsOf(page);
            const value = Number(counts[filter.measure] ?? 0);
            if (!Number.isFinite(value)) {
                throw new Error(`Filter ${filter.name} lieferte keinen endlichen Zaehlwert`);
            }
            return value;
        };
        if (filter.timelineEffect === true) {
            const setFullscreen = async (expected) => {
                const current = await page.evaluate(() => globalThis.__atlasAgents?.fullscreen === true);
                if (current === expected) return;
                const control = page.locator('[data-testid="atlas-agents-switch"][data-switch="fullscreen"]');
                await control.waitFor({ state: 'visible', timeout: 5000 });
                await control.click({ timeout: 4000 });
                await page.waitForFunction(
                    (wanted) => globalThis.__atlasAgents?.fullscreen === wanted,
                    expected,
                    { timeout: 5000 },
                );
            };
            const setTimelineEffect = async (selector, visible) => {
                await setFullscreen(false);
                await page.click('[data-menu="a-settings"]', { timeout: 4000 });
                await page.locator(selector).waitFor({ state: 'visible', timeout: 5000 });
                await page.click(selector, { timeout: 4000 });
                await page.click('[data-menu="a-settings"]', { timeout: 4000 });
                await page.waitForSelector('[data-testid="atlas-settings"]', { state: 'hidden', timeout: 5000 });
                await setFullscreen(true);
                await page.waitForSelector(
                    '[data-testid="atlas-agents-timeline"]',
                    { state: visible ? 'visible' : 'hidden', timeout: 8000 },
                );
            };
            out.before = await read();
            await setTimelineEffect(filter.selector, false);
            out.after = await read();
            await setTimelineEffect(filter.back ?? filter.selector, true);
            out.again = await read();
            out.removes = out.after < out.before;
            out.restored = out.again === out.before;
            out.explains = out.before > 0;
            return out;
        }
        if (filter.prime === 'agent-trail') {
            /*
             * Die frische Replay-Quelle endet in der Gegenwart, aber ein
             * 60-Sekunden-Pfad kann schon waehrend der beiden sichtbaren
             * Settingswege altern. Der HUD-Schalter waehlt deshalb vor der
             * Messung das echte 5-Minuten-Fenster; die Produktpraeferenz bleibt
             * danach unveraendert durch AUS und Wiederherstellen der Wirkung.
             */
            const fiveMinutes = page.locator(
                '[data-testid="atlas-agents-window-option"][data-option="300000"]',
            );
            await fiveMinutes.waitFor({ state: 'visible', timeout: 5000 });
            await fiveMinutes.click({ timeout: 4000 });
            await page.waitForFunction(
                () => Number(globalThis.__atlasAgents?.trailWindowMs ?? -1) === 300000,
                undefined,
                { timeout: 5000 },
            );
        }
        if (filter.needsSettings === true) {
            /*
             * Der Trail-Versuch waehlt sein reales 5-Minuten-HUD-Fenster
             * unmittelbar davor. Das Zustands-Want verlangt sonst 1 Minute
             * fuer die einzelnen Bedienproben; waehrend dieser Filtermessung
             * darf es die gerade sichtbare 5-Minuten-Vorbedingung nicht wieder
             * ueberschreiben.
             */
            await apply(page, {
                ...state.want,
                settings: true,
                ...(filter.prime === 'agent-trail' ? { agentTrailWindow: undefined } : {}),
            }, ctx);
        }
        if (filter.prime === 'agent-wave') {
            await page.waitForFunction(
                () => document.querySelectorAll('[data-testid="atlas-agent-wave"]').length > 0,
                undefined,
                { timeout: 8000 },
            );
        }
        const TRAIL_STABILITY_TIMEOUT_MS = 20_000;
        const TRAIL_STABILITY_INTERVAL_MS = 1_200;
        const trailCount = async () => page.evaluate(() =>
            Number(globalThis.__atlasAgents?.renderOrders?.trails ?? 0));
        const waitForTrails = async (expected, timeout = TRAIL_STABILITY_TIMEOUT_MS) => {
            /*
             * SceneProbe schreibt die Renderreihenfolge erst im naechsten
             * Bild-Scan. W11b belegt etwa 1700 ms; die begrenzte Wartezeit
             * laesst diesen echten Produkttakt zu, ohne eine Wirkung zu
             * behaupten, die nicht im Renderprimitiv angekommen ist.
             */
            await page.waitForFunction(
                (wanted) => {
                    const trails = Number(globalThis.__atlasAgents?.renderOrders?.trails ?? 0);
                    return wanted === 'positive' ? trails > 0 : trails === wanted;
                },
                expected,
                { timeout },
            );
        };
        const establishStableTrails = async () => {
            const deadline = Date.now() + TRAIL_STABILITY_TIMEOUT_MS;
            const pairs = [];
            while (Date.now() < deadline) {
                const remaining = deadline - Date.now();
                try {
                    await waitForTrails('positive', Math.max(1, remaining));
                } catch {
                    break;
                }
                const first = await trailCount();
                const afterFirst = deadline - Date.now();
                if (afterFirst < TRAIL_STABILITY_INTERVAL_MS) {
                    break;
                }
                /* SceneProbe aktualisiert das echte Renderprimitiv nur etwa
                 * sekündlich. Zwei gleiche positive Werte mit diesem Abstand
                 * sind die gemessene, nicht geschriebene Vorbedingung. */
                await page.waitForTimeout(TRAIL_STABILITY_INTERVAL_MS);
                const second = await trailCount();
                pairs.push(`${first}->${second}`);
                if (first > 0 && first === second) {
                    return first;
                }
            }
            throw new Error(
                'Agentenspuren wurden innerhalb von '
                + `${TRAIL_STABILITY_TIMEOUT_MS} ms nicht stabil positiv: `
                + (pairs.length > 0 ? pairs.join(', ') : 'kein positiver SceneProbe-Wert'),
            );
        };
        const waitForLabels = async (expected) => {
            await page.waitForFunction(
                (wanted) => {
                    const boxes = globalThis.__atlasGalaxy?.labelBoxes;
                    const count = Array.isArray(boxes) ? boxes.length : Number(boxes ?? 0);
                    return wanted === 'positive' ? count > 0 : count === wanted;
                },
                expected,
                { timeout: 5000 },
            );
        };
        let establishedBefore;
        if (filter.name === 'Effektschalter labels') {
            await waitForLabels('positive');
            const labels = await read();
            await page.waitForTimeout(260);
            await waitForLabels(labels);
            establishedBefore = labels;
        }
        if (filter.prime === 'agent-trail') {
            establishedBefore = await establishStableTrails();
        }
        /*
         * Das Rauschband, und nur dort, wo es eins gibt.
         *
         * Eine gezaehlte Zeile ist eine gezaehlte Zeile: dort ist jede
         * Abweichung eine Abweichung. Ein gezaehlter Bildpunkt ist es nicht,
         * denn zwischen zwei Bildern zittert eine Szene um ein paar Punkte.
         * Gemessen wird das Zittern hier und nicht geschaetzt: zweimal dasselbe
         * Bild, ohne etwas anzufassen.
         */
        if (filter.measure === 'pixels') {
            const first = await read();
            const second = await read();
            out.noise = Math.abs(first - second);
        }

        /*
         * Eine Linse, die aus ist, wird erst eingeschaltet: gemessen wird immer
         * von der Lage, in der sie zeigt, zu der, in der sie schweigt.
         */
        if (filter.kind === 'facet' && !filter.startsOn) {
            await page.click(filter.selector);
            await page.waitForTimeout(400);
        }

        out.before = establishedBefore ?? await read();
        if (filter.explainWhenEmpty === true) {
            const sentence = await page.evaluate((name) => {
                const node = document.querySelector(
                    `[data-testid="codeatlas-twin-empty-${name}"], `
                    + `[data-testid="codeatlas-twin-text-${name}"], `
                    + `[data-testid="codeatlas-twin-section-${name}"]`,
                );
                return node === null ? '' : globalThis.__w12.clean(node.textContent);
            }, filter.name.split(' ')[1]);
            out.sentence = sentence.slice(0, 220);
            out.emptyCase = true;
            out.explains = sentence.length > 40;
        }

        await page.click(filter.selector);
        await page.waitForTimeout(500);
        if (filter.name === 'Effektschalter labels') {
            await waitForLabels(0);
        }
        if (filter.prime === 'agent-trail') {
            await waitForTrails(0);
        }
        out.after = await read();

        const back = filter.back ?? filter.selector;
        await page.click(back);
        await page.waitForTimeout(500);
        if (filter.name === 'Effektschalter labels') {
            await waitForLabels(out.before);
        }
        if (filter.prime === 'agent-trail') {
            await waitForTrails(out.before);
        }
        out.again = await read();

        out.removes = out.after < out.before - out.noise;
        out.restored = Math.abs(out.again - out.before) <= out.noise;
        if (!out.emptyCase) {
            out.explains = out.before > 0;
        }
        if (out.before === 0) {
            const excuse = await page.evaluate((selector) => {
                const node = document.querySelector(selector);
                return node === null ? { found: false, text: '' } : globalThis.__w12.excuse(
                    globalThis.__w12.describe(node),
                );
            }, filter.selector);
            out.explains = excuse.found;
            out.sentence = excuse.text ?? '';
        }
    } catch (error) {
        out.why = String(error.message ?? error).split('\n')[0].slice(0, 180);
    }
    return out;
}

// ---------------------------------------------------------------- Uebersicht --

/** Die Uebersicht fuer einen Menschen: eine Zeile je Element, ohne JSON. */
function overview(report, store) {
    const lines = [];
    const yes = (value) => (value ? 'ja' : 'nein');
    const round = report.rounds[report.rounds.length - 1];
    const order = STATES.map((entry) => entry.id);
    const states = Object.values(store.states ?? {})
        .sort((a, b) => order.indexOf(a.id) - order.indexOf(b.id));

    lines.push('# Jeder Knopf, einmal mit der Maus und einmal mit der Tastatur');
    lines.push('');
    lines.push('Erzeugt von `npm run smoke:w12`. Diese Uebersicht ist die Antwort auf die Frage');
    lines.push('"funktioniert wirklich jeder Button": eine Zeile je Bedienelement, mit dem Ort, an');
    lines.push('dem es steht, und mit dem, was auf einen Klick und auf einen Tastendruck geschah.');
    lines.push('');
    lines.push('## Die Zahlen');
    lines.push('');
    lines.push(`- Zustaende, die der Lauf selbst hergestellt hat: **${report.statesVisited}**`);
    lines.push(`- eindeutige Bedienelemente: **${report.uniqueControls}**`);
    lines.push(`- davon mit der Maus angefasst: **${report.controlsClicked}**`);
    lines.push(`- davon mit der Tastatur angefasst: **${report.controlsByKeyboard}**`);
    lines.push(`- Fokus ueberall sichtbar: **${yes(report.focusVisibleAll)}**`);
    lines.push(`- Elemente ohne messbare Wirkung: **${report.didNothing.length}**`);
    lines.push(`- gemessene Filter: **${report.filtersMeasured}**, alle nehmen weg und geben zurueck: `
        + `**${yes(report.everyFilterRemovesAndRestores)}**`);
    lines.push(`- Tastaturfallen: **${report.keyboardTraps}**, Tab-Reihenfolge in Ordnung: `
        + `**${yes(report.tabOrderFollowsLayout)}**`);
    lines.push(`- Konsolenfehler: **${report.consoleErrors}**, unbehandelte Ausnahmen: `
        + `**${report.uncaughtExceptions}**`);
    lines.push(`- Ueberlagerungen: **${report.overlapViolations}**, Beschneidungen: `
        + `**${report.clippingViolations}**, angeschnittene Saetze ohne Hinweis: `
        + `**${report.cutWithoutHint}**`);
    lines.push('');

    lines.push('## Die Etappen und die Runden');
    lines.push('');
    lines.push('Eine ETAPPE ist ein Aufruf: sie misst eine Teilmenge, schreibt nach jedem Zustand');
    lines.push('fort und fuehrt ihre Messung mit der vorhandenen buttons.json zusammen. Eine RUNDE');
    lines.push('ist etwas anderes: ein vollstaendiger Durchgang durch alle Zustaende und alle');
    lines.push('Filter, aus beliebig vielen Etappen zusammengesetzt. `Befunde` ist die Zahl der');
    lines.push('Befunde DIESES Durchgangs und nicht die der neu hinzugekommenen: ein Durchgang, der');
    lines.push('denselben Befund noch einmal findet, hat nichts geloest.');
    lines.push('');
    lines.push('| Runde | Durchgang | wann | Befunde | Dauer |');
    lines.push('| --- | --- | --- | --- | --- |');
    for (const entry of report.rounds) {
        lines.push(`| ${entry.n} | ${entry.pass ?? '-'} | ${entry.at} | ${entry.newFindings} | `
            + `${Math.round((entry.durationMs ?? 0) / 1000)} s |`);
    }
    lines.push('');
    const stages = (store.stages ?? []).slice(-12);
    if (stages.length > 0) {
        lines.push('Die letzten Etappen, damit nachvollziehbar bleibt, wer was gemessen hat:');
        lines.push('');
        lines.push('| Etappe | Durchgang | Zustaende | Filter | Browser | Zeitgrenze gerissen | Dauer |');
        lines.push('| --- | --- | --- | --- | --- | --- | --- |');
        for (const entry of stages) {
            lines.push(`| ${entry.n} | ${entry.pass} | ${entry.states.map((s) => s.id).join(', ') || '-'} `
                + `| ${entry.filters.length} | ${entry.browsers} (${entry.browserDeaths} gestorben) `
                + `| ${yes(entry.hitDeadline)} | ${Math.round((entry.durationMs ?? 0) / 1000)} s |`);
        }
        lines.push('');
    }
    if ((round?.findings ?? []).length > 0) {
        lines.push('### Was die letzte Runde fand');
        lines.push('');
        for (const finding of round.findings) {
            lines.push(`- **${finding.kind}** in \`${finding.state}\`: ${finding.label}. ${finding.detail}`);
        }
        lines.push('');
    }

    lines.push('## Die Filter, in Zahlen');
    lines.push('');
    lines.push('Abschalten muss etwas WEGNEHMEN und Einschalten es zurueckbringen. `vorher` ist die');
    lines.push('Menge mit dem Schalter an, `nachher` die mit ihm aus, `zurueck` die nach dem');
    lines.push('Wiedereinschalten.');
    lines.push('');
    lines.push('| Schalter | was gezaehlt wird | vorher | nachher | zurueck | nimmt weg | gibt zurueck |');
    lines.push('| --- | --- | --- | --- | --- | --- | --- |');
    for (const entry of report.filterCounts) {
        lines.push(`| ${entry.name} | ${entry.unit} | ${entry.before} | ${entry.after} | `
            + `${entry.again} | ${yes(entry.removes)} | ${yes(entry.restored)} |`);
    }
    lines.push('');
    const explained = report.filterCounts
        .filter((entry) => entry.emptyCase && (entry.sentence ?? '').length > 0);
    if (explained.length > 0) {
        lines.push('Die Schalter, hinter denen es fuer dieses Symbol nichts zu zeigen gibt, sagen es');
        lines.push('selbst:');
        lines.push('');
        for (const entry of explained) {
            lines.push(`- **${entry.name}**: "${entry.sentence}"`);
        }
        lines.push('');
    }

    lines.push('## Jedes einzelne Element');
    lines.push('');
    lines.push('`Maus` und `Tastatur` sagen, ob die Betaetigung gelungen ist und ob danach etwas');
    lines.push('anders war. `Wirkung` nennt die Naht oder den Teil der Seite, an dem die Aenderung');
    lines.push('gemessen wurde; steht dort ein Strich, hat das Element nichts getan, und dann steht');
    lines.push('in der Zeile darunter, womit die Flaeche das begruendet.');
    lines.push('');

    for (const state of states) {
        lines.push(`### Zustand \`${state.id}\``);
        lines.push('');
        lines.push(`${state.title}`);
        lines.push('');
        lines.push(`Gefunden: ${state.controlsFound ?? 0} Bedienelemente, davon zum ersten Mal in`);
        lines.push(`diesem Zustand angefasst: ${(state.controls ?? []).length}. Der Rest stand schon`);
        lines.push('in einem Zustand davor und ist dort gemessen oder wurde bewusst in einen passenden');
        lines.push('Zustand verschoben, damit Maus und Tastatur dieselbe sichtbare Ausgangslage pruefen.');
        if ((state.deferredKeys ?? []).length > 0) {
            lines.push(`${state.deferredKeys.length} Bedienelement(e) wurden aus diesem Zustand verschoben.`);
        }
        lines.push('');
        if (state.shot !== null && state.shot !== undefined) {
            lines.push(`Bild: \`verification/w12/states/${state.shot.file}\``);
            lines.push('');
        }
        if ((state.controls ?? []).length === 0) {
            continue;
        }
        lines.push('| Beschriftung | Ort | Maus | Tastatur | Wirkung |');
        lines.push('| --- | --- | --- | --- | --- |');
        for (const control of state.controls) {
            const mouse = control.mouse.done
                ? (control.mouse.changed ? 'geklickt, wirkte' : 'geklickt, ohne Wirkung')
                : `nicht klickbar (${control.mouse.why})`;
            const keys = control.keyboard.done
                ? (control.keyboard.changed
                    ? `${control.keyboard.via}, wirkte`
                    : `${control.keyboard.via}, ohne Wirkung`)
                : `kein Weg (${control.keyboard.why})`;
            const effect = control.mouse.changed || control.keyboard.changed
                ? [...new Set([...control.mouse.keys, ...control.keyboard.keys])].slice(0, 3).join(', ')
                : '-';
            const label = (control.label.length > 0 ? control.label : control.selector)
                .replace(/\|/g, '/');
            lines.push(`| ${label} | ${control.place} | ${mouse} | ${keys} | ${effect} |`);
            if (control.excuse !== undefined && control.excuse !== null) {
                lines.push(`| | | | | Grund der Flaeche: ${control.excuse.text.replace(/\|/g, '/')} |`);
            }
        }
        lines.push('');
    }

    lines.push('## Wonach gesucht wurde');
    lines.push('');
    lines.push('Die Liste der Bedienelemente steht nicht im Lauf. Gesammelt wird ueber die Gattung:');
    lines.push('');
    lines.push('```');
    lines.push(CONTROL_SELECTOR);
    lines.push('```');
    lines.push('');
    lines.push('Zwei Flaechen bleiben ausgenommen, jede mit ihrem Grund:');
    lines.push('');
    for (const entry of COLLECT_EXCLUSIONS) {
        lines.push(`- \`${entry.selector}\`: ${entry.reason}`);
    }
    return lines.join('\n') + '\n';
}

main().catch((err) => {
    console.error('[smoke-w12] unerwarteter Fehler:', err);
    process.exitCode = 1;
});
