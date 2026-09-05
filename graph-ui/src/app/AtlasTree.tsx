/**
 * Der EXPLORER: der Datei-Baum aus der Vereinigung von Graph und Coverage, mit
 * Maus und mit Tastatur.
 *
 * Rein darstellend. Die Zeilen kommen fertig ausgerechnet herein
 * (flattenTree und mergeCoverageIntoLevels in tree-model.ts), das Nachladen
 * einer Ebene macht der Aufrufer. Diese Trennung ist der Grund, warum der Baum
 * ohne Server pruefbar ist: was hier steht, ist Anordnung und Tastenlogik, und
 * beides braucht kein /api.
 *
 * Tastatur ist Pflicht, nicht Zugabe (PLAN.md Abschnitt 4: keyboard-first).
 * Die Liste selbst nimmt den Fokus, die Zeilen tragen einen wandernden Cursor.
 * Waeren die Zeilen einzeln fokussierbar, muesste man sich durch einen ganzen
 * Baum tabben, um an den Editor zu kommen.
 *
 * Seit W4d traegt jede Zeile ihre Coverage-Stufe, und die ist sichtbar statt
 * nur vorhanden. Drei Entscheidungen dazu:
 *
 * 1. **Dateien tragen ein Zeichen, Ordner einen Punkt.** Der Marker einer Datei
 *    ist eine Aussage ueber sie selbst, der eines Ordners eine ueber seinen
 *    Inhalt. Dasselbe Zeichen fuer beides waere die Behauptung, der Ordner
 *    selbst sei uebersprungen worden.
 * 2. **Gedimmt wird, was keinen Inhalt hat.** `skipped`, `not-indexed` und
 *    `ignored` sind Zeilen, hinter denen dieses Produkt nichts zeigen kann.
 *    Sie ganz wegzulassen waere die Behauptung, es gebe die Datei nicht; sie
 *    normal zu zeigen waere die Behauptung, ein Klick fuehre irgendwohin.
 * 3. **Die Legende zeigt nur Stufen, die vorkommen.** Eine Legende, die vier
 *    Stufen erklaert, von denen drei nirgends stehen, ist eine Legende fuer
 *    einen anderen Baum. Der ehrliche Quellensatz steht dagegen immer da: er
 *    handelt von dem, was NICHT im Baum steht, und ist genau dann noetig, wenn
 *    der Baum vollstaendig aussieht.
 *
 * Seit W5c kommt eine vierte dazu, und sie korrigiert die zweite:
 *
 * 4. **Jede Datei traegt IMMER ihren Status-Punkt, auch der Gutfall.** Bis
 *    dahin blieb eine indizierte Zeile unmarkiert, und ein sauberer Baum sah
 *    aus wie ein Baum, zu dem gar keine Coverage-Antwort da war. Der
 *    Unterschied zwischen "nichts gemeldet" und "nichts gefragt" ist genau der,
 *    den dieses Produkt sonst ueberall benennt. Das Buchstaben-Zeichen bleibt,
 *    wo es war: es traegt WELCHE Stoerung, der Punkt traegt OB eine da ist.
 */
import type { JSX, KeyboardEvent } from 'react';
import { useEffect, useRef } from 'react';
import { COVERAGE_ORDER } from './tree-model';
import type { CoverageState, TreeRow } from './tree-model';
import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import {
    COVERAGE_DESCRIPTIONS,
    COVERAGE_FOLDER_MARK,
    COVERAGE_LABELS,
    COVERAGE_MARKS,
    COVERAGE_SOURCE_NOTE,
    coverageTooltip,
    folderTooltip,
} from './coverage-strings';

export interface AtlasTreeProps {
    /** Der Projektname als Wurzelzeile, so wie das Vorbild ihn zeigt. */
    projectName: string;
    rows: TreeRow[];
    /** Zeile unter dem Tastatur-Cursor. */
    cursor: number;
    /** Pfad der Datei, die gerade im Reader steht. */
    activePath: string;
    /** Ehrliche Zeile unter dem Baum: was der Server geliefert hat und was nicht. */
    note: string;
    /** True, wenn die Notiz eine Abwesenheit benennt und nicht nur eine Zahl. */
    noteIsAbsence?: boolean;
    /**
     * Listen, die der Server gekappt hat, je eine Zeile.
     *
     * Getrennt von `note`, weil es eine andere Art von Aussage ist: die Notiz
     * beschreibt, was da ist, diese Zeilen beschreiben, was der Server selbst
     * als unvollstaendig gemeldet hat.
     */
    truncations?: readonly string[];
    onCursorChange: (index: number) => void;
    onOpen: (row: TreeRow) => void;
    onToggle: (row: TreeRow) => void;
    onKeyDown: (event: KeyboardEvent<HTMLUListElement>) => void;
}

/** Das Pfeil-Zeichen einer Zeile. Dateien tragen keins. */
export function twistyFor(row: TreeRow): string {
    if (row.kind !== 'dir') {
        return ' ';
    }
    return row.expanded ? '▾' : '▸';
}

/**
 * Die Stufe einer Zeile, mit einem Vorgabewert, der nichts behauptet.
 *
 * Eine Zeile ohne Stufe ist eine Zeile, zu der noch keine Coverage-Antwort da
 * war. Sie wird wie `indexed` gezeichnet, weil der Graph sie genannt hat, und
 * traegt darum kein Zeichen.
 */
export function coverageOf(row: TreeRow): CoverageState {
    return row.coverage ?? 'indexed';
}

/** Das Zeichen einer Zeile: Punkt fuer Ordner, Buchstabe fuer Dateien. */
export function markFor(row: TreeRow): string {
    const state = coverageOf(row);
    if (state === 'indexed') {
        return '';
    }
    return row.kind === 'dir' ? COVERAGE_FOLDER_MARK : COVERAGE_MARKS[state];
}

/**
 * Die drei Toene, die ein Status-Punkt tragen kann.
 *
 * Drei und nicht fuenf: der Punkt beantwortet die Frage "steht hinter dieser
 * Datei etwas im Graphen", und darauf gibt es drei Antworten. Welcher der drei
 * Gruende die dritte ausgeloest hat, sagt der Tooltip und die Legende; eine
 * eigene Farbe je Grund waere eine Palette, die niemand mehr auswendig kann.
 */
export function dotToneOf(state: CoverageState): 'indexed' | 'partial' | 'absent' {
    if (state === 'indexed') {
        return 'indexed';
    }
    return state === 'partial' ? 'partial' : 'absent';
}

/** Der Tooltip einer Zeile. */
export function titleFor(row: TreeRow): string {
    const state = coverageOf(row);
    return row.kind === 'dir' ? folderTooltip(state) : coverageTooltip(state, row.coverageReason ?? '');
}

/** Die Stufen, die in diesen Zeilen wirklich vorkommen, in ihrer Ordnung. */
export function shownStates(rows: readonly TreeRow[]): CoverageState[] {
    const present = new Set<CoverageState>(['indexed']);
    for (const row of rows) {
        present.add(coverageOf(row));
    }
    return COVERAGE_ORDER.filter((state) => present.has(state));
}

export default function AtlasTree(props: AtlasTreeProps): JSX.Element {
    const listRef = useRef<HTMLUListElement | null>(null);

    // Den Cursor im Blick behalten. Ohne das wandert er beim Tippen aus dem
    // sichtbaren Bereich und der Baum sieht aus, als reagiere er nicht.
    useEffect(() => {
        const list = listRef.current;
        if (list === null) {
            return;
        }
        const row = list.querySelector<HTMLElement>('[data-cursor="true"]');
        // jsdom kennt scrollIntoView nicht. Das ist kein Grund, den Baum nicht
        // zu pruefen, und kein Grund, jsdom eine Funktion unterzuschieben, die
        // der Browser anders macht.
        if (typeof row?.scrollIntoView === 'function') {
            row.scrollIntoView({ block: 'nearest' });
        }
    }, [props.cursor, props.rows.length]);

    const states = shownStates(props.rows);
    const markedFolders = props.rows.some((row) => row.kind === 'dir' && coverageOf(row) !== 'indexed');

    return (
        <aside className="atlas-tree" data-testid="atlas-tree">
            <h2 className="atlas-tree-title">{messages.explorer.title}</h2>
            <ul
                className="atlas-tree-list"
                ref={listRef}
                role="tree"
                aria-label={messages.explorer.title}
                tabIndex={0}
                onKeyDown={props.onKeyDown}
            >
                <li>
                    <span className="atlas-tree-row" data-kind="dir" data-root="true">
                        <span className="atlas-tree-twisty">{'▾'}</span>
                        {props.projectName}/
                    </span>
                </li>
                {props.rows.map((row, index) => {
                    const state = coverageOf(row);
                    const mark = markFor(row);
                    return (
                        <li key={row.path} role="treeitem" aria-selected={row.path === props.activePath}>
                            <Hint name="tree-row" text={titleFor(row)}>
                            <button
                                type="button"
                                tabIndex={-1}
                                className="atlas-tree-row"
                                style={{ paddingLeft: `${8 + row.depth * 12}px` }}
                                data-kind={row.kind}
                                data-path={row.path}
                                data-active={row.path === props.activePath}
                                data-cursor={index === props.cursor}
                                data-coverage={state}
                                // Nur fuer Ordner eine Aussage. Bei einer Datei
                                // waere "nicht aufgeklappt" keine Lage, sondern
                                // eine Kategorie, die es nicht gibt.
                                data-expanded={row.kind === 'dir' ? row.expanded : undefined}
                                data-testid="atlas-tree-row"
                                onClick={() => {
                                    props.onCursorChange(index);
                                    if (row.kind === 'dir') {
                                        props.onToggle(row);
                                    } else {
                                        props.onOpen(row);
                                    }
                                }}
                            >
                                <span className="atlas-tree-twisty">{twistyFor(row)}</span>
                                {/*
                                  * Der Status-Punkt, und er steht IMMER da.
                                  *
                                  * Nutzerfeedback vom 2026-08-29: bis dahin trug
                                  * nur die Problemzeile ein Zeichen, und ein
                                  * sauber indizierter Baum sah aus wie ein Baum
                                  * ohne Coverage-Antwort. Die Anforderung war
                                  * "sichtbar je Datei", nicht "sichtbar nur bei
                                  * Problemen": dass alles indiziert ist, ist ein
                                  * Befund und muss man sehen koennen.
                                  */}
                                {row.kind === 'file' && (
                                    <span
                                        className="atlas-tree-dot"
                                        data-testid="atlas-tree-dot"
                                        data-coverage={state}
                                        data-tone={dotToneOf(state)}
                                        aria-hidden="true"
                                    />
                                )}
                                {row.kind === 'dir' ? `${row.name}/` : row.name}
                                {mark.length > 0 && (
                                    <span
                                        className="atlas-tree-mark"
                                        data-testid="atlas-tree-mark"
                                        data-coverage={state}
                                        aria-hidden="true"
                                    >
                                        {mark}
                                    </span>
                                )}
                                <span className="atlas-tree-symbols">{row.symbols}</span>
                            </button>
                            </Hint>
                        </li>
                    );
                })}
            </ul>
            {props.note.length > 0 && (
                <p className="atlas-tree-note" data-state={props.noteIsAbsence === true ? 'absent' : 'present'}>
                    {props.note}
                </p>
            )}
            {(props.truncations ?? []).map((line) => (
                <p className="atlas-tree-note" data-state="absent" data-testid="atlas-tree-truncation" key={line}>
                    {line}
                </p>
            ))}
            <div className="atlas-tree-legend" data-testid="atlas-tree-legend">
                {/*
                  * Die Legende erklaert auch den Gutfall.
                  *
                  * Eine Legende, die nur die Stoerungen erklaert, laesst den
                  * haeufigsten Punkt des Baums unerklaert und macht aus einer
                  * Antwort ein Raetsel. Der Punkt steht darum in jeder Zeile
                  * mit, in genau dem Ton, den die Baumzeile traegt.
                  */}
                {states.map((state) => (
                    <span className="atlas-tree-legend-entry" data-testid="atlas-tree-legend-entry" data-coverage={state} key={state}>
                        <span
                            className="atlas-tree-dot"
                            data-testid="atlas-tree-legend-dot"
                            data-coverage={state}
                            data-tone={dotToneOf(state)}
                            aria-hidden="true"
                        />
                        <span className="atlas-tree-mark" data-coverage={state} aria-hidden="true">
                            {COVERAGE_MARKS[state].length > 0 ? COVERAGE_MARKS[state] : ' '}
                        </span>
                        <b>{COVERAGE_LABELS[state]}</b> {COVERAGE_DESCRIPTIONS[state]}
                    </span>
                ))}
                {markedFolders && (
                    <span className="atlas-tree-legend-entry" data-testid="atlas-tree-legend-entry" data-coverage="folder">
                        <span className="atlas-tree-mark" data-coverage="folder" aria-hidden="true">
                            {COVERAGE_FOLDER_MARK}
                        </span>
                        <b>{messages.explorer.folderLabel}</b> {messages.explorer.folderDescription}
                    </span>
                )}
                <span className="atlas-tree-legend-source" data-testid="atlas-tree-legend-source">
                    {COVERAGE_SOURCE_NOTE}
                </span>
            </div>
        </aside>
    );
}
