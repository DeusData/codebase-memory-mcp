/**
 * Die Hilfeseite: was dieses Fenster ist, was es NICHT kann, welche Flaeche
 * welche Frage beantwortet, jede Taste, der Betrieb, die Ehrlichkeitsregeln und
 * die Verweise ins Repository.
 *
 * Nutzerauftrag vom 2026-08-29: "[?]help sollte eine Hilfeseite verlinken".
 * Verlinken ist genau das, was hier nicht geht, und darum steht die Hilfe
 * eingebaut da: die Oberflaeche ist per Vorgabe abgeschottet (PLAN Abschnitt 3),
 * und ein Verweis ins Netz waere im Normalbetrieb ein Knopf, der nichts tut.
 * Also ist die Seite Teil des Programms, offline vollstaendig, und die Verweise
 * darin sind Pfade in diesem Repository.
 *
 * Vier Entscheidungen, und keine davon ist Geschmack:
 *
 * 1. **Overlay ueber der Editorflaeche, nach dem Muster des Flow-Erklaerers.**
 *    Das Muster ist erprobt (W5c): eine eigene Ebene, nichts liegt davor, der
 *    Kasten scrollt selbst. Ein Panel in der rechten Spalte waere ein langer
 *    Text in einer 440 Pixel breiten Saeule, und ein Dialog mit Rand haette
 *    dieselbe Hoehe mit weniger Platz darin.
 * 2. **Die Grenzen stehen vor den Faehigkeiten.** Wer die Hilfe aufschlaegt,
 *    liest im zweiten Abschnitt, was hier nicht geht, samt Grund. Erst danach
 *    kommen die Panels. Ein Produkt, das seine Grenzen ans Ende stellt, laesst
 *    den Leser sie selbst finden, und das ist die teuerste Art, sie zu erfahren.
 * 3. **Die Tastentabelle wird abgelesen, nicht gepflegt.** Sie entsteht aus
 *    ATLAS_SHORTCUTS (src/app/shortcuts.ts), also aus der Verdrahtung selbst.
 *    Eine handgeschriebene Tabelle waere die naechste stille Luege: sie waere
 *    beim ersten umbenannten Kuerzel falsch, und niemand haette es gemerkt.
 * 4. **Der ganze Text steht im Katalog** (src/i18n/messages.ts), wie jeder
 *    andere Satz des Rahmens. Hier steht nur, in welcher Reihenfolge er
 *    dasteht.
 */

import type { JSX, ReactNode } from 'react';
import { useEffect, useRef, useState } from 'react';

import { KEY_LISTENER_OPTIONS } from '../app/keyboard';
import { NO_KEY_READING, readKeyEvent } from '../app/key-probe';
import type { KeyProbeReading } from '../app/key-probe';
import { ATLAS_SHORTCUTS, needsAlt, shortcutId, WIRED_MENU_SHORTCUTS } from '../app/shortcuts';
import type { AtlasShortcut } from '../app/shortcuts';
import { messages } from '../i18n/messages';

export interface HelpOverlayProps {
    onClose: () => void;
}

const help = messages.help;

/** Die gedrueckten Modifikatoren, so wie sie auf einer Tastatur heissen. */
function modifiersOf(reading: KeyProbeReading): string {
    const names = [
        reading.altKey ? 'alt (option)' : '',
        reading.ctrlKey ? 'ctrl' : '',
        reading.metaKey ? 'meta (command)' : '',
        reading.shiftKey ? 'shift' : '',
    ].filter((name) => name.length > 0);
    return names.length === 0 ? help.keyProbeNoModifier : names.join(' + ');
}

/**
 * Der Tastentest: was ankam, in sieben Zeilen.
 *
 * Der Griff haengt am Fenster und in derselben einfangenden Phase wie der der
 * Anwendung, also unmittelbar hinter ihm: was der Test zeigt, ist das Ereignis
 * in genau der Lage, in der die Verdrahtung es gesehen hat, samt der Marke, die
 * sie darauf hinterlassen hat. Ein Griff in der aufsteigenden Phase saehe eine
 * spaetere Lage und koennte "verbraucht" melden, wo nichts verbraucht wurde.
 *
 * Er zeichnet nur. Was eine Ablesung ist, entscheidet src/app/key-probe.ts, und
 * ob ein Druck ein Kuerzel waere, entscheidet dieselbe Funktion, die der Griff
 * am Fenster ruft. Ein Test, der die Regel nachbaut, prueft seine Kopie.
 */
function KeyProbe(): JSX.Element {
    const [reading, setReading] = useState<KeyProbeReading | undefined>(undefined);

    useEffect(() => {
        const onKeyDown = (event: globalThis.KeyboardEvent): void => {
            setReading(readKeyEvent(event, event.target as Element | null, WIRED_MENU_SHORTCUTS));
        };
        window.addEventListener('keydown', onKeyDown, KEY_LISTENER_OPTIONS);
        return () => window.removeEventListener('keydown', onKeyDown, KEY_LISTENER_OPTIONS);
    }, []);

    const shown = reading ?? NO_KEY_READING;
    const empty = reading === undefined;
    const or = (value: string): string => (empty || value.length === 0 ? help.keyProbeNone : value);
    const fields: { field: string; value: string }[] = [
        { field: 'code', value: or(shown.code) },
        { field: 'key', value: or(shown.key) },
        { field: 'modifiers', value: empty ? help.keyProbeNone : modifiersOf(shown) },
        { field: 'defaultPrevented', value: empty ? help.keyProbeNone : String(shown.defaultPrevented) },
        {
            field: 'consumedBy',
            value: empty ? help.keyProbeNone : help.keyProbeConsumers[shown.consumedBy] ?? shown.consumedBy,
        },
        { field: 'target', value: or(shown.targetTag) },
        {
            field: 'shortcut',
            value: empty
                ? help.keyProbeNone
                : shown.shortcut.length > 0
                    ? help.keyProbeShortcut(shown.shortcut)
                    : help.keyProbeNoShortcut,
        },
    ];

    return (
        <div className="atlas-help-keyprobe" data-testid="atlas-help-keyprobe" data-pressed={!empty}>
            <h4 className="atlas-help-keyprobe-title">{help.keyProbeTitle}</h4>
            <p className="atlas-help-text">{help.keyProbeIntro}</p>
            <dl className="atlas-help-defs">
                {fields.map((entry) => (
                    <div
                        className="atlas-help-def"
                        data-testid="atlas-help-keyprobe-field"
                        data-field={entry.field}
                        key={entry.field}
                    >
                        <dt className="atlas-help-term">{help.keyProbeFields[entry.field] ?? entry.field}</dt>
                        <dd className="atlas-help-desc" data-testid="atlas-help-keyprobe-value">
                            {entry.value}
                        </dd>
                    </div>
                ))}
            </dl>
            <p className="atlas-help-note">{empty ? help.keyProbeIdle : help.keyProbeNote}</p>
        </div>
    );
}

/**
 * Wie eine Taste geschrieben wird.
 *
 * Zwei Uebersetzungen: `Escape` heisst auf einer Tastatur `esc`, und ein
 * Menuekuerzel traegt seinen Modifikator, weil `a` allein seit dem 2026-08-29
 * ein Buchstabe ist und kein Kommando.
 */
function keyLabel(shortcut: AtlasShortcut): string {
    const name = help.keyNames[shortcut.key] ?? shortcut.key;
    return needsAlt(shortcut) ? `${help.altPrefix}${name}` : name;
}

/** Ein Abschnitt der Seite, mit seiner Ueberschrift und seinem Namen im DOM. */
function Section(props: { name: string; title: string; children: ReactNode }): JSX.Element {
    return (
        <section className="atlas-help-section" data-testid="atlas-help-section" data-section={props.name}>
            <h3 className="atlas-help-section-title">{props.title}</h3>
            {props.children}
        </section>
    );
}

export default function HelpOverlay(props: HelpOverlayProps): JSX.Element {
    const root = useRef<HTMLDivElement | null>(null);

    // Der Fokus geht einmal in die Seite. Ohne das bliebe er dort, wo der
    // Leser gerade war, und Escape traefe die Flaeche, die er verlassen hat.
    useEffect(() => {
        root.current?.focus();
    }, []);

    return (
        <div
            className="atlas-help"
            data-testid="atlas-help"
            role="dialog"
            aria-label={help.title}
            aria-modal="false"
            tabIndex={-1}
            ref={root}
            onKeyDown={(event) => {
                if (event.key === 'Escape') {
                    event.preventDefault();
                    props.onClose();
                }
            }}
        >
            <header className="atlas-help-head">
                <span className="atlas-help-title" data-testid="atlas-help-title">
                    {help.title}
                </span>
                <span className="atlas-help-subtitle">{help.subtitle}</span>
                <button
                    type="button"
                    className="atlas-help-close"
                    data-testid="atlas-help-close"
                    aria-label={help.closeLabel}
                    onClick={props.onClose}
                >
                    {help.close}
                </button>
            </header>

            <Section name="what" title={help.whatTitle}>
                {help.what.map((sentence) => (
                    <p className="atlas-help-text" key={sentence}>
                        {sentence}
                    </p>
                ))}
            </Section>

            {/*
              * Die Grenzen, und zwar als Erstes nach der Einleitung. Der Grund
              * steht mit dabei: "kein Terminal" ohne "diese Flaeche hat kein
              * eigenes Backend" liest sich wie eine Luecke, die als naechstes
              * geschlossen wird, und genau das waere wieder ein Versprechen.
              */}
            <Section name="limits" title={help.limitsTitle}>
                <ul className="atlas-help-list" data-testid="atlas-help-limits">
                    {help.limits.map((sentence) => (
                        <li className="atlas-help-item" data-alarm="true" key={sentence}>
                            {sentence}
                        </li>
                    ))}
                </ul>
                <p className="atlas-help-text">{help.limitsWhy}</p>
            </Section>

            <Section name="panels" title={help.panelsTitle}>
                <dl className="atlas-help-defs">
                    {help.panels.map((panel) => (
                        <div className="atlas-help-def" key={panel.name}>
                            <dt className="atlas-help-term">{panel.name}</dt>
                            <dd className="atlas-help-desc">{panel.answers}</dd>
                        </div>
                    ))}
                </dl>
            </Section>

            <Section name="shortcuts" title={help.shortcutsTitle}>
                <table className="atlas-help-keys" data-testid="atlas-help-keys">
                    <thead>
                        <tr>
                            <th scope="col">{help.columnKey}</th>
                            <th scope="col">{help.columnWhere}</th>
                            <th scope="col">{help.columnDoes}</th>
                        </tr>
                    </thead>
                    <tbody>
                        {ATLAS_SHORTCUTS.map((shortcut) => (
                            <tr
                                data-testid="atlas-help-shortcut"
                                data-key={shortcut.key}
                                data-scope={shortcut.scope}
                                key={shortcutId(shortcut)}
                            >
                                <td className="atlas-help-key">{keyLabel(shortcut)}</td>
                                <td className="atlas-help-scope">{help.scopes[shortcut.scope]}</td>
                                <td>{help.shortcutDoes[shortcutId(shortcut)] ?? ''}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
                <p className="atlas-help-text" data-testid="atlas-help-typing">{help.typingNote}</p>
                <p className="atlas-help-note">{help.shortcutsNote}</p>
                {/*
                  * Der Tastentest steht in DIESEM Abschnitt und nicht in einem
                  * eigenen: er beantwortet keine neue Frage, sondern dieselbe
                  * wie die Tabelle darueber, nur fuer den Fall, dass eine Taste
                  * am Geraet des Lesers etwas anderes tut als in der Tabelle.
                  */}
                <KeyProbe />
            </Section>

            <Section name="operations" title={help.operationsTitle}>
                <ul className="atlas-help-list">
                    {help.operations.map((sentence) => (
                        <li className="atlas-help-item" key={sentence}>
                            {sentence}
                        </li>
                    ))}
                </ul>
            </Section>

            <Section name="honesty" title={help.honestyTitle}>
                <ul className="atlas-help-list">
                    {help.honesty.map((sentence) => (
                        <li className="atlas-help-item" key={sentence}>
                            {sentence}
                        </li>
                    ))}
                </ul>
            </Section>

            <Section name="references" title={help.referencesTitle}>
                <dl className="atlas-help-defs">
                    {help.references.map((reference) => (
                        <div className="atlas-help-def" key={reference.path}>
                            <dt className="atlas-help-term atlas-help-path" data-testid="atlas-help-path">
                                {reference.path}
                            </dt>
                            <dd className="atlas-help-desc">{reference.about}</dd>
                        </div>
                    ))}
                </dl>
                <p className="atlas-help-note">{help.referencesNote}</p>
            </Section>
        </div>
    );
}
