/**
 * Das SEMANTIC_TWIN-Panel: dieselbe IR, vier Tiefen, und keine Behauptung ohne
 * Beleg daneben.
 *
 * Diese Datei ist JSX und sonst nichts. Was gesagt wird, entscheidet
 * `src/twin/render-model.ts`, portiert aus CodeAtlasIDE; was hier steht, ist
 * die Frage, wie es aussieht. Der Schnitt ist derselbe wie im Referenzprojekt
 * (twin-modes.tsx, twin-sections.tsx, evidence-popover.tsx) und aus demselben
 * Grund: sobald ein Renderer anfaengt, selbst zu entscheiden, was eine Tiefe
 * zeigt, koennen zwei Tiefen ueber dieselbe Zeile verschiedener Meinung sein.
 *
 * Die Regeln, die aus der Vorlage unveraendert gelten:
 *
 *  1. **Eine leere Sektion sagt trotzdem etwas.** Der Leer-Satz ist fuer jede
 *     Sektion ein eigener Befund ("Nothing in the indexed workspace calls this
 *     symbol"), nie ein Achselzucken.
 *  2. **Ein Fakt, der keine direkte Lesung ist, traegt einen Marker**, und der
 *     Marker traegt den ganzen Satz als `title`. Ein farbiger Punkt ohne Satz
 *     waere Dekoration.
 *  3. **Jede Ueberschrift und jede Zeile traegt den Beleg-Knopf**, ausser sie
 *     behauptet gar nichts ueber den Code. Die zwei Linsen, die es als
 *     Ueberschrift vor den Fakten gibt, tragen einen leeren `factPath`, und ein
 *     Beleg-Knopf ueber einer Behauptung, die CodeAtlas nicht aufgestellt hat,
 *     waere ein Angebot, das ins Leere fuehrt.
 *  4. **Eine Zeile mit Ziel ist ein `<button>`**, eine ohne ist es nicht. Was
 *     man anklicken kann, muss man auch mit der Tastatur erreichen koennen; was
 *     man nicht anklicken kann, darf nicht so aussehen.
 *
 * Seit W5c zeichnet dieses Panel den Flow-Erklaerer nicht mehr selbst. Der
 * flow()-Kopf bleibt, wo er war, und oeffnet jetzt das Overlay ueber der
 * Editorflaeche (src/pseudocode/FlowOverlay.tsx). Der Grund ist ein Befund und
 * keine Vorliebe: der Kasten sass in einer 440 Pixel breiten Spalte zwischen
 * Kopfzeile und Detailstufen-Regler und verschwand hinter ihnen, sobald das
 * Bild breiter wurde als die Spalte.
 *
 * Zwei Unterschiede zur Vorlage, beide genannt statt versteckt:
 *
 *  - **Kein Popover, ein aufklappbarer Block.** Die Vorlage positioniert die
 *    Belege in einem `div` neben dem Knopf. Hier stehen sie unter der Zeile.
 *    Der Inhalt ist derselbe (Relation, Ort, Strategie, Urheber plus
 *    Index-Generation); was wegfaellt, ist die Positionierung, und die war im
 *    Referenzprojekt schon ausdruecklich kein Framework.
 *  - **Keine Codicons.** Diese Oberflaeche liefert die Schrift nicht aus. Der
 *    Name aus `SECTION_ICONS` steht als `data-icon` an der Ueberschrift, damit
 *    die portierte Tabelle nicht zur toten Zeile wird; getragen wird die
 *    Bedeutung von der Ueberschrift daneben, so wie in der Vorlage auch.
 */
import type { JSX } from 'react';
import { useCallback, useEffect, useRef, useState } from 'react';

import type { SymbolRef } from '../core/focus-protocol';
import type { Evidence, SemanticIR } from '../core/semantic-ir';
import type { FlowView } from '../pseudocode/flow-view';
import type { ImportsGroup } from '../pseudocode/imports-group';
import type { PseudocodeDocument, PseudocodeSourceRef } from '../pseudocode/pseudocode-builder';
import {
    IMPORTS_LINE_TOOLTIP,
    PSEUDOCODE_BEHIND_NOTE,
    PSEUDOCODE_HONESTY_SHORT,
    PSEUDOCODE_LINE_TOOLTIP,
    PSEUDOCODE_PROVENANCE_TOOLTIP,
    PSEUDOCODE_SOURCE_NOTE,
    PSEUDOCODE_STEPS_HEADING,
    PSEUDOCODE_TAB_LABEL,
    PSEUDOCODE_TAB_TOOLTIP,
    PSEUDOCODE_TARGET_TOOLTIP,
    PSEUDOCODE_TARGET_UNKNOWN,
    TWIN_TAB_LABEL,
    TWIN_TAB_TOOLTIP,
    explainerHeadTooltip,
    pseudocodeCoverage,
    pseudocodeUncovered,
} from '../pseudocode/pseudocode-strings';
import { blockLeadOf } from '../pseudocode/block-lead';
import {
    REFINE_LABEL,
    REFINE_RESTORE_LABEL,
    REFINE_RESTORE_TITLE,
    REFINE_RUNNING,
    REFINE_TITLE,
} from '../chat/chat-strings';
import { buildTwinViewModel, withImportsSection } from './render-model';
import type {
    TwinChip,
    TwinFactRow,
    TwinLimit,
    TwinReaderBlock,
    TwinViewModel,
} from './render-model';
import { clampDepth, FACET_ORDER, Facet, MAX_DEPTH } from './presentation-profile';
import type { DepthLevel, ResolvedPresentation } from './presentation-profile';
import {
    BLOCK_LABEL_DATA,
    BLOCK_LABEL_EFFECTS,
    BLOCK_LABEL_ERRORS,
    DEPTH_LABELS,
    DEPTH_SLIDER_LABEL,
    DEPTH_SLIDER_TOOLTIP,
    EVIDENCE_BUTTON_LABEL,
    EVIDENCE_BUTTON_TOOLTIP,
    EVIDENCE_EMPTY,
    EVIDENCE_NO_LOCATION,
    EVIDENCE_POPOVER_TITLE,
    EVIDENCE_RUNTIME_BADGE,
    EVIDENCE_RUNTIME_TOOLTIP,
    FACET_LABELS,
    GROUND_LEAD_LIMITS,
    GROUND_TITLE_LIMITS,
    GUIDED_FACTS_LEAD,
    JUNIOR_TERMS_LEAD,
    VOICE_APPLIED,
    VOICE_LABEL,
    VOICE_MARKER,
    VOICE_MARKER_NOTE,
    VOICE_RESTORE_LABEL,
    VOICE_RESTORE_TITLE,
    VOICE_RUNNING,
    VOICE_TITLE,
    VOICE_UNAVAILABLE,
    absenceSentence,
    evidenceAttribution,
    evidenceObservations,
    facetTooltip,
    stateMarkerLabel,
    voiceRefused,
} from './strings';
import { evidenceFor, locationLabel } from './twin-view-model';
import type { TwinBlock, TwinRow, TwinSection } from './twin-view-model';
import Hint from '../ui/tooltip/Hint';
import { TWIN_MORE_ABOVE, TWIN_MORE_BELOW, rowCountLabel, twinFoldLabel } from './strings';
import { askModel } from '../chat/chat-client';
import { SIDECAR_ORIGIN } from '../llm/sidecar';
import { READER_SYSTEM_PROMPT, buildReaderPrompt, nonThinkingFor } from '../compiler/prompt-contract';
import {
    applyReaderRewrite,
    readerMaxTokens,
    readerSubjectText,
    rewriteMap,
} from './reader-rewrite';
import { browserStore, readReaderLevel, writeReaderLevel } from './reader-level-store';

/** Wie das Panel gerade dasteht. Nie einfach leer. */
export type TwinStatus = 'empty' | 'loading' | 'ready' | 'not-indexed' | 'failed';

/** Das Wort ueber dem Panel, wie im Vorbild design/design.png. */
export const TWIN_TITLE = 'SEMANTIC_TWIN';

export interface TwinPanelProps {
    status: TwinStatus;
    /** Der Satz, der statt des Koerpers steht. Leer nur, wenn status 'ready' ist. */
    message: string;
    /** Ein zweiter Satz darunter, wenn der erste allein zu wenig sagt. */
    hint?: string;
    /** Der Name in der Kopfzeile. Bloss der Name, nie der qualifizierte. */
    symbolName: string;
    /**
     * Der qualifizierte Name desselben Subjekts, wenn er aufgeloest ist.
     *
     * Er wird nicht gezeigt: er traegt die Projektidentitaet der Engine, die
     * aus einem Pfad dieser Maschine abgeleitet ist und einem Leser nichts
     * sagt. Er steht im Testgriff, weil er der einzige Schluessel ist, unter
     * dem Twin, Graph und Layout dasselbe Symbol gleich schreiben, und ein
     * Beweislauf ohne ihn "createUser" von "orderService.createUser" nicht
     * unterscheiden koennte.
     */
    symbolQualifiedName?: string | undefined;
    ir?: SemanticIR | undefined;
    presentation: ResolvedPresentation;
    onDepth: (depth: DepthLevel) => void;
    onToggleFacet: (facet: Facet) => void;
    /**
     * 1-basierte Graph-Zeile des Carets in der Datei des Symbols, wenn er
     * ueberhaupt dort steht. Eine Zeile, deren `siteLine` passt, wird
     * umrandet; so zeigen Panel und Editor auf dasselbe, ohne dass eins von
     * beiden nachladen muss.
     */
    caretLine?: number | undefined;
    /** Einer Zeile folgen: der Twin wechselt das Subjekt. */
    onFollow: (target: SymbolRef) => void;
    /** Der Leser zeigt auf eine Zeile: der Editor soll die Aufrufstelle anleuchten. */
    onPointRow?: ((row: TwinRow | undefined) => void) | undefined;

    // --- W4c: der Flow-Erklaerer und die Pseudocode-Ansicht ----------------

    /**
     * Was die Datei um dieses Symbol herum hereinholt.
     *
     * Kommt spaeter als die IR und auf einem eigenen Weg (siehe
     * render-model.withImportsSection), darum optional: das Panel ist ohne die
     * Gruppe schon nuetzlich, und eine fehlende Import-Antwort ist kein Grund,
     * die Fakten zu verlieren.
     */
    imports?: ImportsGroup | undefined;
    /**
     * Der Walk, sobald er da ist.
     *
     * Gezeichnet wird er nicht mehr hier: seit W5c steht der Erklaerer als
     * Overlay ueber der Editorflaeche (src/pseudocode/FlowOverlay.tsx). Das
     * Panel braucht ihn trotzdem, weil die STEPS-Sektion die Zeile markiert,
     * auf der der Stepper gerade steht, und weil der Testgriff sagt, was das
     * Overlay haelt.
     */
    flow?: FlowView | undefined;
    /** Ob das Overlay offen ist. Der flow()-Kopf schaltet es. */
    flowOpen: boolean;
    onToggleFlow: () => void;
    /** Der aktive Schritt, oder -1 fuer "noch keiner". Nie 0 als "keiner". */
    flowStep: number;
    /** Welche der beiden Ansichten der Koerper gerade zeigt. */
    view: TwinBodyView;
    onView: (view: TwinBodyView) => void;
    /** Der Fakten-Block des aktiven Symbols, fuer die Pseudocode-Ansicht. */
    pseudocode?: PseudocodeDocument | undefined;
    /** Eine Zeile oeffnen: Datei und Zeile, wie die Zeile sie mitbringt. */
    onOpenLine?: ((ref: PseudocodeSourceRef) => void) | undefined;

    // --- W5b: die Umformulierung durch das lokale Modell -------------------

    /**
     * Ob der Knopf ueberhaupt dasteht.
     *
     * Genau dann wahr, wenn das lokale Modell bereit ist. Ein Knopf, der eine
     * Umformulierung anbietet, waehrend nichts antwortet, waere ein Versprechen
     * in Knopfform, und das ist dieselbe Luege wie ein Startknopf ohne Backend
     * (ADR 0001). Aus heisst: kein Knopf, und die Zeile darunter sagt es.
     */
    refineAvailable?: boolean;
    /** Wo die Umformulierung gerade steht. */
    refineState?: 'idle' | 'running' | 'applied' | 'refused';
    /** Der Satz darunter: der Grund einer Ablehnung, oder die Bestaetigung. */
    refineMessage?: string;
    onRefine?: (() => void) | undefined;
    /** Das deterministische Original zurueckholen. Immer moeglich. */
    onRestoreOriginal?: (() => void) | undefined;
    /** Model name for the visible provenance of an accepted voice. */
    voiceModel?: string;
    /** Router id sent only by the explicit voice request. */
    voiceRequestModel?: string;
}

/** Die zwei Ansichten des Twin-Koerpers. */
export type TwinBodyView = 'facts' | 'pseudocode';

/** Die Augenbraue ueber den drei Bloecken. */
const BLOCK_LABELS: Readonly<Record<TwinBlock, string>> = {
    data: BLOCK_LABEL_DATA,
    errors: BLOCK_LABEL_ERRORS,
    effects: BLOCK_LABEL_EFFECTS,
};

/**
 * Die Sektionen, deren Inhalt ein Fehlerpfad ist.
 *
 * Sie werden in --atlas-alarm gezeichnet, und zwar sparsam: ein Panel, in dem
 * alles ruft, ruft nichts. "may raise" ist die eine Aussage, bei der ein Leser
 * die Farbe schon gesehen haben muss, bevor er den Satz liest.
 */
const ALARM_SECTIONS = new Set(['errors']);

/**
 * The slider that asks who is reading.
 *
 * The test id is unchanged (`atlas-twin-depth`) even though the control no
 * longer asks about depth. Renaming it would have broken four proof runs that
 * reach for it, and a test id is an address rather than a description; what a
 * reader sees is the label beside it, and that says the new question.
 */
function ReaderSlider(props: {
    depth: DepthLevel;
    onDepth: (depth: DepthLevel) => void;
}): JSX.Element {
    return (
        <Hint name="twin-depth" text={DEPTH_SLIDER_TOOLTIP}>
            <label className="atlas-twin-depth">
                <span className="atlas-twin-depth-label">{DEPTH_SLIDER_LABEL}</span>
                <input
                    type="range"
                    min={0}
                    max={MAX_DEPTH}
                    step={1}
                    data-testid="atlas-twin-depth"
                    aria-label={DEPTH_SLIDER_LABEL}
                    aria-valuetext={DEPTH_LABELS[props.depth]}
                    value={props.depth}
                    onChange={(event) => props.onDepth(clampDepth(Number(event.target.value)))}
                />
                <span
                    className="atlas-twin-depth-name"
                    data-testid="atlas-twin-depth-name"
                    data-level={props.depth}
                >
                    {DEPTH_LABELS[props.depth]}
                </span>
            </label>
        </Hint>
    );
}

function FacetChips(props: {
    facets: ReadonlySet<Facet>;
    onToggle: (facet: Facet) => void;
}): JSX.Element {
    return (
        <div className="atlas-twin-facets" data-testid="atlas-twin-facets">
            {FACET_ORDER.map((facet) => {
                const on = props.facets.has(facet);
                const label = FACET_LABELS[facet];
                return (
                    <Hint key={facet} name={`twin-facet-${facet}`} text={facetTooltip(label, on)}>
                        <button
                            type="button"
                            className="atlas-twin-facet"
                            data-facet={facet}
                            data-on={on}
                            aria-pressed={on}
                            onClick={() => props.onToggle(facet)}
                        >
                            {label}
                        </button>
                    </Hint>
                );
            })}
        </div>
    );
}

/** Der Marker plus sein Satz, nur wenn ein Fakt keine direkte Lesung ist. */
function StateMarker(props: { state: TwinSection['state']; subject: string; note?: string | undefined }): JSX.Element | null {
    const label = stateMarkerLabel(props.state);
    if (label.length === 0) {
        return null;
    }
    const sentence = props.note ?? absenceSentence(props.state, props.subject);
    return (
        <Hint name={`twin-state-${props.state}`} text={sentence}>
            <span
                className="atlas-twin-state"
                data-testid="codeatlas-twin-state-marker"
                data-state={props.state}
            >
                {label}
            </span>
        </Hint>
    );
}

interface EvidenceProps {
    factPath: string;
    open: boolean;
    entries: Evidence[];
    onToggle: (factPath: string) => void;
}

/**
 * Der Beleg-Knopf und, wenn er offen ist, was hinter der Behauptung steckt.
 *
 * Der Inhalt eines Eintrags ist der der Vorlage: die Relation in den Worten des
 * Produkts, der Ort als Datei und Zeile, die Strategie falls es eine gab, und
 * darunter, wer das gesagt hat und aus welchem Index-Bau.
 */
function EvidenceBlock(props: EvidenceProps): JSX.Element {
    return (
        <span className="atlas-evidence">
            <Hint name="evidence" text={EVIDENCE_BUTTON_TOOLTIP}>
                <button
                    type="button"
                    className="atlas-evidence-btn"
                    data-testid="codeatlas-evidence-btn"
                    data-factpath={props.factPath}
                    aria-label={EVIDENCE_BUTTON_LABEL}
                    aria-expanded={props.open}
                    onClick={(event) => {
                        // Die Zeile darunter navigiert bei einem Klick, und nach
                        // einem Beleg zu fragen ist nicht dasselbe wie navigieren.
                        event.stopPropagation();
                        event.preventDefault();
                        props.onToggle(props.factPath);
                    }}
                >
                    ?
                </button>
            </Hint>
            {props.open && (
                <span
                    className="atlas-evidence-list"
                    data-testid="codeatlas-evidence-popover"
                    data-factpath={props.factPath}
                    role="group"
                    aria-label={EVIDENCE_POPOVER_TITLE}
                >
                    {props.entries.length === 0 && (
                        <span className="atlas-evidence-empty">{EVIDENCE_EMPTY}</span>
                    )}
                    {props.entries.map((entry, index) => {
                        const where = locationLabel(entry.file, entry.range?.startLine);
                        return (
                            <span
                                className="atlas-evidence-entry"
                                data-testid="codeatlas-evidence-entry"
                                data-source={entry.source}
                                key={index}
                            >
                                <span className="atlas-evidence-relation">
                                    {entry.relation ?? entry.source}
                                    {entry.source === 'runtime-trace' && (
                                        <Hint name="evidence-runtime" text={EVIDENCE_RUNTIME_TOOLTIP}>
                                            <span className="atlas-evidence-runtime">
                                                {EVIDENCE_RUNTIME_BADGE}
                                            </span>
                                        </Hint>
                                    )}
                                </span>
                                {entry.observations !== undefined && (
                                    <span className="atlas-evidence-observations">
                                        {evidenceObservations(entry.observations)}
                                    </span>
                                )}
                                <Hint name="evidence-file" text={entry.file}>
                                    <span className="atlas-evidence-loc">
                                        {where ?? EVIDENCE_NO_LOCATION}
                                    </span>
                                </Hint>
                                {entry.strategy !== undefined && (
                                    <span className="atlas-evidence-strategy">{entry.strategy}</span>
                                )}
                                <span className="atlas-evidence-attribution">
                                    {evidenceAttribution(entry.providerId, entry.engineGeneration)}
                                </span>
                            </span>
                        );
                    })}
                </span>
            )}
        </span>
    );
}

interface RowProps {
    row: TwinRow;
    sectionName: string;
    openEvidence: string | undefined;
    entriesFor: (factPath: string) => Evidence[];
    onToggleEvidence: (factPath: string) => void;
    onActivate: (row: TwinRow) => void;
    onPoint?: ((row: TwinRow | undefined) => void) | undefined;
    /** Wahr, wenn der Caret auf der Aufrufstelle dieser Zeile steht. */
    highlighted: boolean;
}

function RowView(props: RowProps): JSX.Element {
    const { row } = props;
    const chip = row.display === 'chip';
    const classes = [chip ? 'atlas-twin-row-chip' : 'atlas-twin-row'];
    if (props.highlighted) {
        classes.push('atlas-twin-row-current');
    }
    if (row.target === undefined) {
        classes.push('atlas-twin-row-inert');
    }
    const point = props.onPoint;
    const label = (
        <>
            {row.siteLine !== undefined && <span className="atlas-twin-row-line">{row.siteLine}</span>}
            <span className="atlas-twin-row-label">{row.label}</span>
            {row.badge && (
                <Hint name="twin-badge" text={row.badge.tooltip}>
                    <span className="atlas-twin-badge">{row.badge.text}</span>
                </Hint>
            )}
            {/*
              * Ein Chip traegt seinen Ort im Tooltip statt neben dem Namen.
              * Nichts wird verschwiegen: der Ort ist einen Hover entfernt, der
              * Beleg-Knopf daneben nennt dieselbe Datei und Zeile mit ihrer
              * Herkunft, und dem Chip zu folgen oeffnet sie weiterhin.
              */}
            {row.detail !== undefined && !chip && (
                <span className="atlas-twin-row-detail">{row.detail}</span>
            )}
            {row.confidenceLabel !== undefined && (
                <Hint name="twin-confidence" text={row.confidenceNote}>
                    <span
                        className="atlas-twin-row-confidence"
                        data-testid="codeatlas-row-confidence"
                        data-confidence={row.confidence}
                        data-scored={row.confidence !== undefined}
                    >
                        {row.confidenceLabel}
                    </span>
                </Hint>
            )}
            {(row.extras ?? []).map((extra) => (
                <span className="atlas-twin-row-extra" key={extra}>
                    {extra}
                </span>
            ))}
        </>
    );
    return (
        <li
            className={classes.join(' ')}
            data-testid={props.sectionName === 'steps' ? 'codeatlas-twin-step' : 'codeatlas-twin-row'}
            data-line={row.siteLine}
            data-current={props.highlighted}
            onMouseEnter={point === undefined ? undefined : () => point(row)}
            onMouseLeave={point === undefined ? undefined : () => point(undefined)}
        >
            {row.target !== undefined ? (
                <Hint name="twin-row" text={chip ? row.detail : undefined}>
                    <button
                        type="button"
                        className="atlas-twin-row-activate"
                        onFocus={point === undefined ? undefined : () => point(row)}
                        onBlur={point === undefined ? undefined : () => point(undefined)}
                        onClick={() => props.onActivate(row)}
                    >
                        {label}
                    </button>
                </Hint>
            ) : (
                <Hint name="twin-row" text={chip ? row.detail : undefined}>
                    <span className="atlas-twin-row-activate">{label}</span>
                </Hint>
            )}
            {row.factPath.length > 0 && (
                <EvidenceBlock
                    factPath={row.factPath}
                    open={props.openEvidence === row.factPath}
                    entries={props.openEvidence === row.factPath ? props.entriesFor(row.factPath) : []}
                    onToggle={props.onToggleEvidence}
                />
            )}
        </li>
    );
}

/** Die Gruppen-Ueberschrift beim ersten Auftreten einer Gruppe, danach nie wieder. */
function groupHeadingFor(rows: TwinRow[], index: number): JSX.Element | undefined {
    const group = rows[index].group;
    if (group === undefined || (index > 0 && rows[index - 1].group === group)) {
        return undefined;
    }
    return (
        <li className="atlas-twin-group" key={`group-${group}-${index}`}>
            {group}
        </li>
    );
}

interface SectionProps {
    section: TwinSection;
    /**
     * The reworded text of one sentence, when the local model produced one and
     * the guard let it through.
     *
     * A lookup rather than the `say` helper the other levels use, and the reason
     * is an address: these two paragraphs carry test ids that four proof runs
     * reach for by name (`codeatlas-twin-text-steps` and friends), and `say`
     * emits ids of its own. Rewriting them would have moved a landmark to save
     * six lines.
     */
    voiceOf: (id: string) => string | undefined;
    openEvidence: string | undefined;
    entriesFor: (factPath: string) => Evidence[];
    onToggleEvidence: (factPath: string) => void;
    onActivate: (row: TwinRow) => void;
    onPoint?: ((row: TwinRow | undefined) => void) | undefined;
    caretLine?: number | undefined;
    /**
     * Die Zeile der STEPS-Sektion, auf der der Flow-Stepper gerade steht.
     *
     * Getrennt vom Caret, und zwar mit Absicht: der Caret sagt, wo der Leser
     * steht, und ein Schritt des Erklaerers kann in eine ganz andere Datei
     * zeigen. Waere beides dieselbe Zahl, wuerde eine Zeile 23 in validate.ts
     * die Zeile 23 dieser Liste anleuchten, und das waere ein Treffer aus
     * Zufall.
     */
    activeStepRow?: number | undefined;
}

function SectionView(props: SectionProps): JSX.Element {
    const { section } = props;
    const allChips = section.rows.length > 0 && section.rows.every((row) => row.display === 'chip');
    return (
        <section
            className="atlas-twin-section"
            data-testid={`codeatlas-twin-section-${section.name}`}
            data-populated={section.populated}
            data-block={section.block}
            data-alarm={ALARM_SECTIONS.has(section.name)}
        >
            {section.block !== undefined && (
                <div className="atlas-twin-block-label" data-testid={`codeatlas-twin-block-${section.block}`}>
                    {BLOCK_LABELS[section.block]}
                </div>
            )}
            <header className="atlas-twin-section-head">
                <h3 className="atlas-twin-section-title" data-icon={section.icon}>
                    {section.title}
                </h3>
                {section.countLabel !== undefined && (
                    <span className="atlas-twin-section-count" data-testid="codeatlas-section-count">
                        {section.countLabel}
                    </span>
                )}
                <StateMarker state={section.state} subject={section.subject} note={section.stateNote} />
                {section.factPath.length > 0 && (
                    <EvidenceBlock
                        factPath={section.factPath}
                        open={props.openEvidence === section.factPath}
                        entries={props.openEvidence === section.factPath ? props.entriesFor(section.factPath) : []}
                        onToggle={props.onToggleEvidence}
                    />
                )}
            </header>
            {section.text !== undefined && section.text.length > 0 && (
                <p
                    className="atlas-twin-section-text"
                    data-testid={`codeatlas-twin-text-${section.name}`}
                    data-sentence={`section-${section.name}-text`}
                    data-voiced={props.voiceOf(`section-${section.name}-text`) !== undefined}
                >
                    {props.voiceOf(`section-${section.name}-text`) ?? section.text}
                    {props.voiceOf(`section-${section.name}-text`) !== undefined && <VoiceMarker />}
                </p>
            )}
            {section.rows.length === 0 && section.emptyText.length > 0 && (
                <p
                    className="atlas-twin-section-empty"
                    data-testid={`codeatlas-twin-empty-${section.name}`}
                    data-sentence={`section-${section.name}-empty`}
                    data-voiced={props.voiceOf(`section-${section.name}-empty`) !== undefined}
                >
                    {props.voiceOf(`section-${section.name}-empty`) ?? section.emptyText}
                    {props.voiceOf(`section-${section.name}-empty`) !== undefined && <VoiceMarker />}
                </p>
            )}
            {section.rows.length > 0 && (
                <ol className={allChips ? 'atlas-twin-rows atlas-twin-rows-chips' : 'atlas-twin-rows'}>
                    {section.rows.flatMap((row, index) => {
                        const heading = groupHeadingFor(section.rows, index);
                        const line = (
                            <RowView
                                key={row.id}
                                row={row}
                                sectionName={section.name}
                                openEvidence={props.openEvidence}
                                entriesFor={props.entriesFor}
                                onToggleEvidence={props.onToggleEvidence}
                                onActivate={props.onActivate}
                                onPoint={props.onPoint}
                                /*
                                  * Waehrend einer Stepper-Sitzung markiert der
                                  * Stepper, und nur er. Der Caret steht dann
                                  * dort, wo der Stepper ihn hingestellt hat,
                                  * und zwei Quellen fuer dieselbe Markierung
                                  * waeren zwei angeleuchtete Zeilen, von denen
                                  * eine ein Nachlauf ist.
                                  */
                                highlighted={
                                    section.name === 'steps' && (props.activeStepRow ?? -1) >= 0
                                        ? index === props.activeStepRow
                                        : row.siteLine !== undefined && row.siteLine === props.caretLine
                                }
                            />
                        );
                        return heading === undefined ? [line] : [heading, line];
                    })}
                </ol>
            )}
            {/*
              * Die Haelfte der Sektion, die der Index nicht sehen kann, unter den
              * Zeilen, die er sehen kann. Unter und nicht statt: die
              * aufgefuehrten Fehlertypen sind echt, und die fehlenden Handler
              * machen sie nicht weniger echt.
              */}
            {section.note !== undefined && (
                <p
                    className="atlas-twin-section-note"
                    data-testid={`codeatlas-twin-note-${section.name}`}
                    data-sentence={`section-${section.name}-note`}
                    data-voiced={props.voiceOf(`section-${section.name}-note`) !== undefined}
                >
                    {props.voiceOf(`section-${section.name}-note`) ?? section.note}
                    {props.voiceOf(`section-${section.name}-note`) !== undefined && <VoiceMarker />}
                </p>
            )}
        </section>
    );
}

/**
 * How a sentence is put on screen when the local model may have worded it.
 *
 * One function rather than a conditional at every call site, and the reason is
 * AC4 of this cycle: a sentence the model worded has to say so, and a rule that
 * has to be remembered at eleven call sites is a rule that will be forgotten at
 * one of them. `say` is passed down to every view; a view cannot render a
 * sentence without going through it.
 */
type SayKind = 'paragraph' | 'note' | 'lead' | 'limit';
type SayFn = (id: string, text: string, kind?: SayKind) => JSX.Element | null;

/** The marker beside a reworded sentence. Same idiom as the state markers. */
function VoiceMarker(): JSX.Element {
    return (
        <Hint name="twin-voiced" text={VOICE_MARKER_NOTE}>
            <span className="atlas-twin-voiced" data-testid="codeatlas-twin-voiced">
                {VOICE_MARKER}
            </span>
        </Hint>
    );
}

const SAY_CLASS: Readonly<Record<SayKind, string>> = {
    paragraph: 'atlas-twin-paragraph',
    note: 'atlas-twin-note',
    lead: 'atlas-twin-block-lead',
    limit: 'atlas-twin-limit',
};

const SAY_TESTID: Readonly<Record<SayKind, string>> = {
    paragraph: 'codeatlas-twin-paragraph',
    note: 'codeatlas-twin-note',
    lead: 'codeatlas-twin-reader-lead',
    limit: 'codeatlas-twin-limit',
};

function makeSay(voiced: Readonly<Record<string, string>> | undefined): SayFn {
    return (id, text, kind = 'paragraph') => {
        const spoken = voiced?.[id];
        const shown = spoken ?? text;
        if (shown.length === 0) {
            return null;
        }
        return (
            <p
                className={SAY_CLASS[kind]}
                data-testid={SAY_TESTID[kind]}
                data-sentence={id}
                data-voiced={spoken !== undefined}
                key={id}
            >
                {shown}
                {spoken !== undefined && <VoiceMarker />}
            </p>
        );
    };
}

function ProseView(props: {
    model: TwinViewModel;
    say: SayFn;
    onFollow: (chip: TwinChip) => void;
}): JSX.Element {
    return (
        <div className="atlas-twin-prose" data-testid="codeatlas-twin-prose">
            {props.model.paragraphs.map((paragraph, index) => props.say(`p${index}`, paragraph))}
            {props.model.chips.length > 0 && (
                <div className="atlas-twin-chips">
                    {props.model.chips.map((chip) => (
                        <button
                            key={chip.label}
                            type="button"
                            className="atlas-twin-chip"
                            data-testid="codeatlas-twin-chip"
                            disabled={chip.target === undefined}
                            onClick={() => props.onFollow(chip)}
                        >
                            {chip.label}
                        </button>
                    ))}
                </div>
            )}
            {props.model.notes.map((text, index) => props.say(`n${index}`, text, 'note'))}
        </div>
    );
}

/**
 * The junior's level: the order, and the words it took to say it.
 *
 * Two things distinguish it from every other level, and both are the answer to
 * the junior's question rather than decoration. The steps are numbered and
 * carry the connective in the sentence, so the sequence is readable as a
 * sequence rather than as a set. And a word that this level used for the first
 * time is explained under it, once, which is the one place in this panel where
 * a definition earns its space.
 */
function JuniorView(props: {
    model: TwinViewModel;
    say: SayFn;
    onFollow: (target: SymbolRef | undefined) => void;
}): JSX.Element {
    return (
        <div className="atlas-twin-guided" data-testid="codeatlas-twin-guided">
            {props.model.paragraphs.map((paragraph, index) => props.say(`p${index}`, paragraph))}
            {props.model.steps.length > 0 && (
                <ol className="atlas-twin-steps" data-testid="codeatlas-twin-steps">
                    {props.model.steps.map((step) => (
                        <li className="atlas-twin-step" data-testid="codeatlas-twin-step" key={step.id}>
                            <span className="atlas-twin-step-order">{step.order}</span>
                            {step.target !== undefined ? (
                                <button
                                    type="button"
                                    className="atlas-twin-step-text"
                                    data-sentence={step.id}
                                    onClick={() => props.onFollow(step.target)}
                                >
                                    {props.model.rewritable.find((line) => line.id === step.id)?.text ?? step.text}
                                </button>
                            ) : (
                                <span className="atlas-twin-step-text" data-sentence={step.id}>
                                    {step.text}
                                </span>
                            )}
                        </li>
                    ))}
                </ol>
            )}
            {props.model.terms.length > 0 && (
                <>
                    <p className="atlas-twin-paragraph">{JUNIOR_TERMS_LEAD}</p>
                    <dl className="atlas-twin-terms" data-testid="codeatlas-twin-terms">
                        {props.model.terms.map((term) => (
                            <div className="atlas-twin-term" data-testid="codeatlas-twin-term" key={term.term}>
                                <dt className="atlas-twin-term-word">{term.term}</dt>
                                <dd className="atlas-twin-term-text">{term.explanation}</dd>
                            </div>
                        ))}
                    </dl>
                </>
            )}
            {props.model.notes.map((text, index) => props.say(`n${index}`, text, 'note'))}
        </div>
    );
}

/**
 * The list the guided level used to lead with.
 *
 * Kept next to its model function and rendered by nothing today, for the same
 * reason `topFacts` is kept: it is the right shape for "a little of each", and
 * this panel is where that shape would come back.
 */
export function GuidedFactList(props: {
    facts: readonly TwinFactRow[];
    onFollow: (fact: TwinFactRow) => void;
}): JSX.Element | null {
    if (props.facts.length === 0) {
        return null;
    }
    return (
        <>
            <p className="atlas-twin-paragraph">{GUIDED_FACTS_LEAD}</p>
            <ol className="atlas-twin-facts" data-testid="codeatlas-twin-facts">
                {props.facts.map((fact) => (
                    <li className="atlas-twin-fact" data-testid="codeatlas-twin-fact" key={fact.id}>
                        <span className="atlas-twin-fact-label">{fact.label}</span>
                        {fact.target !== undefined ? (
                            <button
                                type="button"
                                className="atlas-twin-fact-name"
                                onClick={() => props.onFollow(fact)}
                            >
                                {fact.name}
                            </button>
                        ) : (
                            <span className="atlas-twin-fact-name">{fact.name}</span>
                        )}
                    </li>
                ))}
            </ol>
        </>
    );
}

/**
 * One question inside the senior's or the architect's level.
 *
 * The same component for both, because they are the same shape: a heading, the
 * sentence that frames the question, the rows that answer it, and the sentence
 * that stands in when there are none. The empty sentence is not optional and
 * not conditional on anything: a block with nothing in it is exactly where a
 * panel goes silent, and silence at that spot reads as "this level is broken"
 * rather than as "nothing was recorded".
 */
function ReaderBlockView(props: {
    block: TwinReaderBlock;
    say: SayFn;
    openEvidence: string | undefined;
    entriesFor: (factPath: string) => Evidence[];
    onToggleEvidence: (factPath: string) => void;
    onActivate: (row: TwinRow) => void;
    onPoint?: ((row: TwinRow | undefined) => void) | undefined;
}): JSX.Element {
    const { block } = props;
    return (
        <section
            className="atlas-twin-block"
            data-testid={`codeatlas-twin-reader-${block.name}`}
            data-block-name={block.name}
            data-populated={block.rows.length > 0}
        >
            <header className="atlas-twin-section-head">
                <h3 className="atlas-twin-section-title">{block.title}</h3>
                {block.weight !== undefined && (
                    <span className="atlas-twin-section-count" data-testid="codeatlas-twin-reader-weight">
                        {block.weight}
                    </span>
                )}
            </header>
            {props.say(`block-${block.name}-lead`, block.lead, 'lead')}
            {block.rows.length === 0
                ? props.say(`block-${block.name}-empty`, block.emptyText, 'note')
                : (
                    <ol className="atlas-twin-rows">
                        {block.rows.flatMap((row, index) => {
                            const heading = groupHeadingFor(block.rows, index);
                            const line = (
                                <RowView
                                    key={row.id}
                                    row={row}
                                    sectionName={block.name}
                                    openEvidence={props.openEvidence}
                                    entriesFor={props.entriesFor}
                                    onToggleEvidence={props.onToggleEvidence}
                                    onActivate={props.onActivate}
                                    onPoint={props.onPoint}
                                    highlighted={false}
                                />
                            );
                            return heading === undefined ? [line] : [heading, line];
                        })}
                    </ol>
                )}
        </section>
    );
}

/**
 * Where this index stops, drawn as part of the answer.
 *
 * Under its own heading and inside the body rather than in a footer, and that
 * placement is the whole of AC7's promise for this level: an architect who
 * cannot see the edge of the index reads every silence as a finding. The three
 * kinds are marked on the row so the reader can tell a gap somebody could close
 * from one nobody can.
 */
/**
 * The one control the local model gets on this panel, and what it says when the
 * model is off.
 *
 * Three rules, all of them the same rules the pseudocode rewrite follows since
 * W5b, because they are the same promise:
 *
 *  1. **No button while nothing answers.** A control that offers a rewrite from
 *     a process that is not running is a promise in button form (ADR 0001).
 *     Off means: no button, and the line beneath says the level is complete
 *     without one.
 *  2. **A refusal is spoken.** What the guard threw away and why is on screen.
 *     A silent refusal looks exactly like a model with nothing to say, and the
 *     difference between those two is the difference between a guard that works
 *     and a sidecar that is down.
 *  3. **The built text is one click away.** Always, whether the rewrite was
 *     applied or refused.
 */
function VoiceBar(props: {
    available: boolean;
    state: 'idle' | 'running' | 'applied' | 'refused';
    message: string;
    sentences: number;
    onAsk: () => void;
    onRestore: () => void;
    model: string;
}): JSX.Element {
    return (
        <div className="atlas-twin-voice" data-testid="codeatlas-twin-voice" data-state={props.state}>
            {props.available && props.sentences > 0 && (
                <Hint name="twin-voice" text={VOICE_TITLE}>
                    <button
                        type="button"
                        className="atlas-twin-voice-btn"
                        data-testid="codeatlas-twin-voice-btn"
                        disabled={props.state === 'running'}
                        onClick={props.onAsk}
                    >
                        {VOICE_LABEL}
                    </button>
                </Hint>
            )}
            {props.available && props.state === 'applied' && (
                <Hint name="twin-voice-restore" text={VOICE_RESTORE_TITLE}>
                    <button
                        type="button"
                        className="atlas-twin-voice-btn"
                        data-testid="codeatlas-twin-voice-restore"
                        onClick={props.onRestore}
                    >
                        {VOICE_RESTORE_LABEL}
                    </button>
                </Hint>
            )}
            <p className="atlas-twin-voice-note" data-testid="codeatlas-twin-built-provenance">
                built from indexed facts and cited source lines.
            </p>
            {(!props.available || props.message.length > 0) && (
                <p
                    className="atlas-twin-voice-note"
                    data-testid="codeatlas-twin-voice-note"
                    data-refused={props.state === 'refused'}
                >
                    {props.available && props.state === 'applied'
                        ? `${props.message} Model: ${props.model}.`
                        : props.available ? props.message : VOICE_UNAVAILABLE}
                </p>
            )}
        </div>
    );
}

function LimitsView(props: { limits: readonly TwinLimit[]; say: SayFn }): JSX.Element {
    return (
        <section
            className="atlas-twin-block"
            data-testid="codeatlas-twin-reader-limits"
            data-block-name="limits"
            data-populated={props.limits.length > 0}
        >
            <header className="atlas-twin-section-head">
                <h3 className="atlas-twin-section-title">{GROUND_TITLE_LIMITS}</h3>
                <span className="atlas-twin-section-count" data-testid="codeatlas-twin-reader-weight">
                    {rowCountLabel(props.limits.length)}
                </span>
            </header>
            {props.say('limits-lead', GROUND_LEAD_LIMITS, 'lead')}
            <ul className="atlas-twin-limits" data-testid="codeatlas-twin-limits">
                {props.limits.map((limit) => (
                    <li className="atlas-twin-limit-row" data-kind={limit.kind} key={limit.id}>
                        {props.say(limit.id, limit.text, 'limit')}
                    </li>
                ))}
            </ul>
        </section>
    );
}

/**
 * Die Pseudocode-Ansicht: dieselben Fakten als nummerierter Block, und der Fund
 * zuerst.
 *
 * Keine Kontrollfluss-Zeile, kein `if`, kein `return`: was hier steht, ist eine
 * Aufrufstelle, ein erhobener Fehlertyp oder eine Umgebungslesung, die der
 * Index gemeldet hat.
 *
 * Die Reihenfolge seit W8c ist die des Beitrags und nicht die der Gewohnheit.
 * Zuerst der Satz, der sagt, was hier der Fund ist; dann die Importgruppe, weil
 * ein Name, den der Index diesem Symbol nicht zuordnen kann, das Einzige auf
 * dieser Flaeche ist, das ein Leser dem Code daneben nicht ansieht; dann die
 * Schritte, jeder mit dem Ort, an den er fuehrt, und mit dem, was der Index
 * ueber das aufgerufene Symbol schon hergibt. Was frueher darunter stand und
 * ueber den Block selbst sprach, steht hinter dem Fragezeichen: dasselbe Idiom
 * wie am Diagramm seit W8b, und aus demselben Grund.
 *
 * Was hier NICHT steht, ist eine Zahl, ab der eine Funktion zu kurz fuer ihre
 * Schrittliste waere. Der Block zeigt jeden Schritt, den das Dokument traegt,
 * bei zwei Schritten wie bei zwanzig.
 */
function PseudocodeView(props: {
    document: PseudocodeDocument;
    imports?: ImportsGroup | undefined;
    onOpenLine?: ((ref: PseudocodeSourceRef) => void) | undefined;
    refineAvailable?: boolean;
    refineState?: 'idle' | 'running' | 'applied' | 'refused';
    refineMessage?: string;
    onRefine?: (() => void) | undefined;
    onRestoreOriginal?: (() => void) | undefined;
    /** Der Name des Subjekts, fuer den Satz ueber dem Block. */
    symbolName: string;
}): JSX.Element {
    const { document, imports } = props;
    const numbered = document.lines.filter((line) => line.order !== undefined).length;
    const open = props.onOpenLine;
    const refineState = props.refineState ?? 'idle';
    const refineMessage = props.refineMessage ?? '';
    const lead = blockLeadOf(document, imports, props.symbolName);
    /*
     * Die Saetze ueber den Block selbst, die bis W8c unter ihm standen.
     *
     * Sie sind nicht weg, sie stehen hinter dem Fragezeichen: Deckung,
     * ungenannte Symbole und die Herkunftsnotiz, WOERTLICH wie vorher, dazu der
     * Satz ueber das, was neben den Schritten steht. Der Grund ist derselbe wie
     * in W8b unter dem Diagramm: eine Ehrlichkeit, die als Textwand erscheint,
     * wird ueberlesen wie ein Cookie-Banner und erreicht das Gegenteil ihres
     * Zwecks.
     */
    const moved = [
        pseudocodeCoverage(document.honest.coveredSymbols, document.scopeSymbols),
        ...(document.honest.uncovered.length > 0
            ? [pseudocodeUncovered(document.honest.uncovered)]
            : []),
        PSEUDOCODE_SOURCE_NOTE,
    ];
    return (
        <div
            className="atlas-pseudocode"
            data-testid="atlas-pseudocode"
            data-lines={numbered}
            data-imports={imports?.entries.length ?? 0}
            data-refine={refineState}
            data-lead={lead.kind}
            data-behind={lead.behindSteps}
        >
            <h3 className="atlas-pseudocode-title" data-testid="atlas-pseudocode-title">
                {document.title}
            </h3>

            {/*
              * Der Kopf: in einem Satz, was in diesem Block der Fund ist.
              *
              * Er steht immer da, auch wenn nichts gefunden wurde, und sagt
              * dann genau das. Ein Kopf, der nur bei einem Fund erscheint,
              * macht sein Fehlen zur Behauptung.
              */}
            <p
                className="atlas-pseudocode-lead"
                data-testid="atlas-pseudocode-lead"
                data-kind={lead.kind}
            >
                {lead.text}
            </p>

            {/*
              * Die Knopfzeile der Umformulierung. Sie steht nur da, wenn das
              * lokale Modell bereit ist; warum, steht an `refineAvailable` in
              * den Props. Der Knopf, der das Original zurueckholt, steht daneben
              * und nicht in einem Menue: eine Umformulierung, aus der man nicht
              * mit einem Klick wieder herauskommt, ist keine Ansicht mehr,
              * sondern ein Zustand.
              */}
            {props.refineAvailable === true && (
                <div className="atlas-pseudocode-refine" data-testid="atlas-pseudocode-refine">
                    <Hint name="pseudocode-refine" text={REFINE_TITLE}>
                        <button
                            type="button"
                            className="atlas-pseudocode-refine-btn"
                            data-testid="atlas-pseudocode-refine-btn"
                            disabled={refineState === 'running' || props.onRefine === undefined}
                            onClick={props.onRefine}
                        >
                            {refineState === 'running' ? REFINE_RUNNING : REFINE_LABEL}
                        </button>
                    </Hint>
                    {refineState === 'applied' && props.onRestoreOriginal !== undefined && (
                        <Hint name="pseudocode-restore" text={REFINE_RESTORE_TITLE}>
                            <button
                                type="button"
                                className="atlas-pseudocode-refine-btn"
                                data-testid="atlas-pseudocode-restore-btn"
                                onClick={props.onRestoreOriginal}
                            >
                                {REFINE_RESTORE_LABEL}
                            </button>
                        </Hint>
                    )}
                </div>
            )}
            {refineMessage.length > 0 && (
                <p
                    className="atlas-pseudocode-refine-note"
                    data-testid="atlas-pseudocode-refine-note"
                    data-state={refineState}
                >
                    {refineMessage}
                </p>
            )}

            {imports !== undefined && (
                <section
                    className="atlas-pseudocode-imports"
                    data-testid="atlas-pseudocode-imports"
                    data-entries={imports.entries.length}
                    data-findings={imports.unused + imports.unknown}
                >
                    <h4 className="atlas-pseudocode-group" data-testid="atlas-pseudocode-group">
                        {imports.heading}
                    </h4>
                    <ul className="atlas-pseudocode-import-list">
                        {imports.entries.map((entry) => (
                            <li
                                className="atlas-pseudocode-import"
                                data-testid="atlas-pseudocode-import"
                                data-usage={entry.usage}
                                data-finding={entry.finding}
                                key={entry.id}
                            >
                                {open !== undefined && entry.sourceRef !== undefined ? (
                                    <Hint name="imports-line" text={IMPORTS_LINE_TOOLTIP}>
                                        <button
                                            type="button"
                                            className="atlas-pseudocode-line-btn"
                                            onClick={() => open(entry.sourceRef!)}
                                        >
                                            {entry.text}
                                        </button>
                                    </Hint>
                                ) : (
                                    <span className="atlas-pseudocode-line-btn">{entry.text}</span>
                                )}
                                <Hint name="imports-mark" text={entry.note}>
                                    <span className="atlas-pseudocode-import-mark">
                                        {entry.marker}
                                    </span>
                                </Hint>
                            </li>
                        ))}
                    </ul>
                    {imports.entries.length > 0 && (
                        <p className="atlas-pseudocode-tally" data-testid="atlas-pseudocode-tally">
                            {imports.tally}
                        </p>
                    )}
                    {imports.cappedNote !== undefined && (
                        <p className="atlas-pseudocode-note">{imports.cappedNote}</p>
                    )}
                </section>
            )}

            <h4 className="atlas-pseudocode-steps-head" data-testid="atlas-pseudocode-steps-head">
                {PSEUDOCODE_STEPS_HEADING}
            </h4>
            {/*
              * Genau ein <li> je Zeile des Dokuments, und nichts anderes in
              * dieser Liste.
              *
              * Der Beweislauf von W5b schickt den Inhalt dieser Liste durch den
              * echten Validator der Umformulierung und erwartet, dass er ihn
              * annimmt; eine Ueberschrift ALS Listeneintrag waere dort eine
              * Zeile mehr, als der Block hat, und die Ablehnung waere richtig
              * aus dem falschen Grund. Deshalb steht die Ueberschrift darueber
              * und nicht darin.
              */}
            <ol className="atlas-pseudocode-lines">
                {document.lines.map((line, index) => {
                    if (line.kind === 'group') {
                        return (
                            <li
                                className="atlas-pseudocode-group"
                                data-testid="atlas-pseudocode-group"
                                key={`group-${index}`}
                            >
                                {line.text}
                            </li>
                        );
                    }
                    if (line.kind === 'note') {
                        return (
                            <li
                                className="atlas-pseudocode-note"
                                data-testid="atlas-pseudocode-note"
                                key={`note-${index}`}
                            >
                                {line.text}
                            </li>
                        );
                    }
                    const place = locationLabel(line.targetFile, line.targetLine);
                    return (
                        <li
                            className="atlas-pseudocode-line"
                            data-testid="atlas-pseudocode-line"
                            data-kind={line.kind}
                            data-order={line.order}
                            data-alarm={line.kind === 'raise'}
                            data-target={place ?? ''}
                            data-behind={line.behind?.length ?? 0}
                            key={`line-${index}`}
                        >
                            {open !== undefined && line.sourceRef !== undefined ? (
                                <Hint name="pseudocode-line" text={PSEUDOCODE_LINE_TOOLTIP}>
                                    <button
                                        type="button"
                                        className="atlas-pseudocode-line-btn"
                                        onClick={() => open(line.sourceRef!)}
                                    >
                                        {line.text}
                                    </button>
                                </Hint>
                            ) : (
                                <span className="atlas-pseudocode-line-btn">{line.text}</span>
                            )}
                            {/*
                              * Wohin der Schritt fuehrt, wie in der
                              * Fakten-Ansicht: dieselbe Funktion rechnet die
                              * Beschriftung, damit "validate.ts:33" hier nicht
                              * eines Tages etwas anderes heisst als dort.
                              *
                              * Ein KNOPF ist das Ziel nur bei einem Schritt.
                              * Bei einer erhobenen Fehlerart und bei einer
                              * Umgebungslesung zeigt schon der Text der Zeile
                              * genau dorthin; ein zweiter Knopf auf dieselbe
                              * Stelle waere eine Bedienung, die zweimal
                              * dasselbe tut.
                              */}
                            {place === undefined ? (
                                <span
                                    className="atlas-pseudocode-target"
                                    data-testid="atlas-pseudocode-target"
                                    data-known="false"
                                >
                                    {PSEUDOCODE_TARGET_UNKNOWN}
                                </span>
                            ) : line.kind === 'step' && open !== undefined && line.targetRef !== undefined ? (
                                <Hint name="pseudocode-target" text={PSEUDOCODE_TARGET_TOOLTIP}>
                                    <button
                                        type="button"
                                        className="atlas-pseudocode-target"
                                        data-testid="atlas-pseudocode-target"
                                        data-known="true"
                                        onClick={() => open(line.targetRef!)}
                                    >
                                        {place}
                                    </button>
                                </Hint>
                            ) : (
                                <span
                                    className="atlas-pseudocode-target"
                                    data-testid="atlas-pseudocode-target"
                                    data-known="true"
                                >
                                    {place}
                                </span>
                            )}
                            {(line.behind ?? []).map((note, at) => (
                                <span
                                    className="atlas-pseudocode-behind"
                                    data-testid="atlas-pseudocode-behind"
                                    data-kind={note.kind}
                                    key={`behind-${at}`}
                                >
                                    {note.text}
                                </span>
                            ))}
                        </li>
                    );
                })}
            </ol>

            <div className="atlas-pseudocode-honest" data-testid="atlas-pseudocode-honest">
                <p data-testid="atlas-pseudocode-honesty-short">{PSEUDOCODE_HONESTY_SHORT}</p>
                {/*
                  * Dasselbe Fragezeichen wie am Diagramm, mit demselben
                  * Klick-Griff (src/ui/tooltip/Hint.tsx, HintHold): ein Knopf,
                  * der nur bei Hover etwas zeigt, verspricht eine Handlung, die
                  * es ohne Zeigegeraet mit Hover gar nicht gibt (W10-1).
                  */}
                <Hint
                    name="pseudocode-provenance"
                    text={[...moved, PSEUDOCODE_BEHIND_NOTE].join(' ')}
                >
                    {(hold) => (
                        <button
                            type="button"
                            className="atlas-pseudocode-provenance"
                            data-testid="atlas-pseudocode-provenance"
                            data-moved={moved.join(' ').length}
                            aria-label={PSEUDOCODE_PROVENANCE_TOOLTIP}
                            aria-expanded={hold.held}
                            onClick={hold.toggle}
                        >
                            ?
                        </button>
                    )}
                </Hint>
            </div>
        </div>
    );
}

/**
 * Der Block, in Zahlen, aus demselben Dokument, aus dem er gezeichnet wird.
 *
 * Absichtlich neben der Ansicht und nicht in ihr: ein Griff, der aus einer
 * zweiten Rechnung entstuende, koennte etwas anderes melden als das, was
 * dasteht. Was hier gezaehlt wird, ist das Dokument; was der Beweislauf im DOM
 * misst, ist die Flaeche; stimmen beide nicht ueberein, ist das ein Befund und
 * kein Rundungsfehler.
 */
function pseudocodeSeamOf(
    document: PseudocodeDocument | undefined,
    imports: ImportsGroup | undefined,
    symbolName: string,
): AtlasPseudocodeSeam {
    if (document === undefined) {
        return {
            lead: '',
            leadKind: '',
            lines: 0,
            steps: 0,
            stepsWithTarget: 0,
            stepsWithoutTarget: 0,
            enrichedSteps: 0,
            enrichment: { usable: [], silent: [], missing: [] },
            imports: imports?.entries.length ?? 0,
            importFindings: imports === undefined ? 0 : imports.unused + imports.unknown,
            metaVisibleChars: 0,
            metaMovedChars: 0,
        };
    }
    const lead = blockLeadOf(document, imports, symbolName);
    const steps = document.lines.filter((line) => line.kind === 'step');
    const moved = [
        pseudocodeCoverage(document.honest.coveredSymbols, document.scopeSymbols),
        ...(document.honest.uncovered.length > 0
            ? [pseudocodeUncovered(document.honest.uncovered)]
            : []),
        PSEUDOCODE_SOURCE_NOTE,
    ].join(' ');
    return {
        lead: lead.text,
        leadKind: lead.kind,
        lines: document.lines.filter((line) => line.order !== undefined).length,
        steps: steps.length,
        stepsWithTarget: steps.filter((line) => line.targetFile !== undefined).length,
        stepsWithoutTarget: steps.filter((line) => line.targetFile === undefined).length,
        enrichedSteps: lead.behindSteps,
        enrichment: {
            usable: document.enrichment.usable.map((entry) => ({ ...entry })),
            silent: document.enrichment.silent.map((entry) => ({ ...entry })),
            missing: document.enrichment.missing.map((entry) => ({ ...entry })),
        },
        imports: imports?.entries.length ?? 0,
        importFindings: imports === undefined ? 0 : imports.unused + imports.unknown,
        metaVisibleChars: PSEUDOCODE_HONESTY_SHORT.length,
        metaMovedChars: moved.length,
    };
}

/**
 * Der Griff, an dem der Beweislauf das Panel anfasst.
 *
 * Absichtlich schmal: der Name des Subjekts, die Zahl der echten Nachladungen,
 * die Namen der gezeigten Sektionen und die Zeilen der Schritte. Alles davon
 * ist auch im DOM zu sehen; der Griff spart dem Lauf das Parsen und macht
 * "gleiches Symbol, kein Nachladen" ueberhaupt messbar.
 */
export interface AtlasTwinSeam {
    symbol: string;
    /** Der qualifizierte Name des Subjekts, leer solange keiner aufgeloest ist. */
    qualifiedName: string;
    fetches: number;
    sectionNames: string[];
    stepLines: number[];
    /** Ob der Flow-Erklaerer offen ist, und was er gerade haelt. */
    flowOpen: boolean;
    /** Die Halte des Steppers: die nummerierten Zeilen des Blocks. */
    flowSteps: number;
    /** Die Pfeile im Kasten: ein Aufruf, den der Walk aufgezeichnet hat. */
    flowArrows: number;
    /** Die Spalten des Kastens: die Dateien der Closure. */
    flowParticipants: number;
    /** Der aktive Schritt, -1 solange keiner gewaehlt ist. */
    flowStep: number;
    /** Welche der beiden Ansichten der Koerper zeigt. */
    view: TwinBodyView;
    /** Der Pseudocode-Block, soweit ein Beweislauf ihn messen muss (W8c). */
    pseudocode: AtlasPseudocodeSeam;

    // --- W13: fuer wen gelesen wird, und was das Modell daran darf ----------

    /** Die gewaehlte Stufe als Zahl, 0 bis 4. */
    level: number;
    /** Ihr Name, so wie er neben dem Regler steht. */
    levelName: string;
    /** Die Frage, die diese Stufe beantwortet. */
    question: string;
    /** Wie der Koerper gebaut ist: prose, guided, sections, cost, ground. */
    mode: string;
    /** Wie viele Saetze dieser Stufe das Modell umformulieren duerfte. */
    rewritableSentences: number;
    /** Wo die Umformulierung steht, und was der Leser darueber liest. */
    voiceState: string;
    voiceMessage: string;
    /** Wie viele Saetze gerade vom Modell formuliert sind. */
    voicedSentences: number;
    /**
     * Der ECHTE Waechter, an derselben Stelle, an der ihn der Knopf ruft.
     *
     * Dieselbe Bauform wie `validateRefine` am Chat-Griff (App.tsx) und aus
     * demselben Grund: ein Beweislauf, der eine kaputte Antwort nachbaut und
     * gegen eine eigene Kopie der Regel haelt, prueft die Kopie. Was hier
     * eingespeist wird, laeuft durch `applyReaderRewrite` und durch nichts
     * sonst.
     */
    validateRewrite: (answer: string) => { applied: boolean; reason: string };
    /** Die Saetze, die eine Umformulierung bekaeme, mit ihren Fakten. */
    subject: { id: string; text: string; facts: string[] }[];
}

/**
 * Was der Beweislauf am Block anfassen muss, und nichts darueber hinaus.
 *
 * Drei Sorten Zahl, und alle drei sind im DOM ebenfalls zu sehen: wie viele
 * Schritte ihr Ziel tragen (und wie viele es nicht koennen), was die Messung
 * aus AC3 ergeben hat, und wie viel Text ueber den Block selbst auf der Flaeche
 * steht gegenueber dem, was hinter dem Fragezeichen liegt. Die letzte ist der
 * Grund, warum sie hier steht: die Saetze, die W8c verschoben hat, sind
 * dieselben Zeichenketten wie vorher, und ihre Laenge ist damit der gemessene
 * Ausgangswert und keine Erinnerung an ihn.
 */
export interface AtlasPseudocodeSeam {
    /** Der Satz oben, und welcher der vier Faelle er ist. */
    lead: string;
    leadKind: string;
    /** Nummerierte Zeilen, davon Schritte mit und ohne bekannten Ort. */
    lines: number;
    steps: number;
    stepsWithTarget: number;
    stepsWithoutTarget: number;
    /** Schritte, an denen steht, was hinter dem Aufruf liegt. */
    enrichedSteps: number;
    /** Die Messung aus AC3, wortgleich mit dem, was im Dokument steht. */
    enrichment: {
        usable: { kind: string; source: string; symbols: number }[];
        silent: { kind: string; source: string }[];
        missing: { kind: string; reason: string }[];
    };
    /** Die Importgruppe: Zahl der Eintraege und der Funde darunter. */
    imports: number;
    importFindings: number;
    /** Zeichen ueber den Block selbst: auf der Flaeche, und hinter dem Fragezeichen. */
    metaVisibleChars: number;
    metaMovedChars: number;
}

declare global {
    // eslint-disable-next-line no-var
    var __atlasTwin: AtlasTwinSeam | undefined;
    // eslint-disable-next-line no-var
    var __atlasTwinFetches: number | undefined;
}

export default function TwinPanel(props: TwinPanelProps): JSX.Element {
    const [openEvidence, setOpenEvidence] = useState<string | undefined>(undefined);
    const ir = props.ir;
    const model = ir === undefined
        ? undefined
        : withImportsSection(buildTwinViewModel(ir, props.presentation), props.imports, props.presentation);
    const levelName = DEPTH_LABELS[props.presentation.depth] ?? '';

    /*
     * Wer liest, ueberlebt den Reload (AC6).
     *
     * Der Zustand selbst bleibt, wo er war: in der Anwendung, als Overlay ueber
     * dem Profil. Hier steht nur das Gedaechtnis, und zwar aus einem Grund, der
     * kein Zufall ist: die Wahl ist eine Aussage ueber den LESER, und dieses
     * Panel ist die einzige Stelle, an der der Leser sie trifft. Ein zweiter
     * Ort, der sie kennt, waere ein zweiter Ort, der sie vergessen kann.
     */
    const restored = useRef(false);
    useEffect(() => {
        if (restored.current) {
            return;
        }
        restored.current = true;
        const store = browserStore();
        if (store === undefined) {
            return;
        }
        const level = readReaderLevel(store);
        if (level !== props.presentation.depth) {
            props.onDepth(level);
        }
    });

    const setLevel = useCallback((level: DepthLevel) => {
        const store = browserStore();
        if (store !== undefined) {
            writeReaderLevel(store, level);
        }
        props.onDepth(level);
    }, [props.onDepth]);

    /*
     * Was das lokale Modell aus den Saetzen dieser Stufe gemacht hat, wenn es
     * an ist und wenn die Pruefung es durchgelassen hat.
     *
     * Getrennt vom gebauten Modell und nie an dessen Stelle: `voiced` ist eine
     * Tabelle von Satz-Kennung auf Wortlaut, und der gebaute Satz bleibt daneben
     * stehen, damit ein Klick ihn zurueckholt. Dieselbe Bauform wie beim
     * Pseudocode (App.tsx, `refined`), und aus demselben Grund.
     */
    const [voiced, setVoiced] = useState<Readonly<Record<string, string>> | undefined>(undefined);
    const [voiceState, setVoiceState] = useState<'idle' | 'running' | 'applied' | 'refused'>('idle');
    const [voiceMessage, setVoiceMessage] = useState('');

    const dropVoice = useCallback(() => {
        setVoiced(undefined);
        setVoiceState('idle');
        setVoiceMessage('');
    }, []);

    // Ein Wortlaut, der ueber den Stufenwechsel oder den Symbolwechsel hinweg
    // stehen bliebe, waere ein Satz ueber etwas anderes: die Kennungen sind je
    // Stufe vergeben, und `p0` heisst auf jeder Stufe etwas anderes.
    useEffect(() => {
        dropVoice();
    }, [dropVoice, ir, props.presentation.depth]);

    const askForVoice = useCallback(() => {
        const lines = model?.rewritable ?? [];
        if (props.refineAvailable !== true || lines.length === 0) {
            return;
        }
        setVoiceState('running');
        setVoiceMessage(VOICE_RUNNING);
        void askModel({
            origin: SIDECAR_ORIGIN,
            system: READER_SYSTEM_PROMPT,
            user: buildReaderPrompt(levelName, readerSubjectText(lines)),
            chatTemplateKwargs: nonThinkingFor(props.voiceRequestModel ?? props.voiceModel ?? '').chatTemplateKwargs,
            maxTokens: readerMaxTokens(lines),
            fetch: (url, init) => window.fetch(url, init),
            ...(props.voiceRequestModel === undefined ? {} : { model: props.voiceRequestModel }),
        })
            .then((reply) => {
                const outcome = applyReaderRewrite(lines, reply.content);
                if (outcome.kind === 'applied') {
                    setVoiced(rewriteMap(outcome.lines));
                    setVoiceState('applied');
                    setVoiceMessage(VOICE_APPLIED);
                    return;
                }
                setVoiced(undefined);
                setVoiceState('refused');
                setVoiceMessage(voiceRefused(outcome.reason));
            })
            .catch((error: unknown) => {
                setVoiced(undefined);
                setVoiceState('refused');
                setVoiceMessage(
                    voiceRefused(error instanceof Error ? error.message : String(error)),
                );
            });
    }, [levelName, model?.rewritable, props.refineAvailable, props.voiceModel, props.voiceRequestModel]);

    const say = makeSay(voiced);

    /*
     * Die Kante des Twin-Koerpers, und warum sie seit W8b gemessen wird.
     *
     * Nutzerbefund vom 2026-08-29, mit Screenshot bei 100 Prozent Zoom: die
     * Zeile "Order exact 1 citation" endete halb hinter dem GALAXY-Kopf. Der
     * Kasten trug seinen mitrollenden Verlauf (das Schatten-Idiom weiter unten
     * in terminal.css), und der zaehlt der Lesbarkeitsregel als Hinweis; einem
     * Leser hat er nicht gereicht. Die Marke sagt es darum in Worten, wie die
     * Legende der Galaxie es seit W9-1 tut, und sie liegt NEBEN dem scrollenden
     * Kasten und nicht darin: was im Kasten steht, scrollt mit und waere genau
     * dann weg, wenn es gebraucht wird.
     */
    const bodyBox = useRef<HTMLDivElement | null>(null);
    const [bodyEdge, setBodyEdge] = useState({ above: false, below: false });

    const measureBodyEdge = useCallback(() => {
        const node = bodyBox.current;
        if (node === null) {
            return;
        }
        const above = node.scrollTop > 1;
        const below = node.scrollTop + node.clientHeight < node.scrollHeight - 1;
        setBodyEdge((edge) =>
            (edge.above === above && edge.below === below ? edge : { above, below }));
    }, []);

    /*
     * Der Inhalt hat sich geaendert (anderes Symbol, andere Tiefe, andere
     * Ansicht): die Kante gilt neu.
     *
     * MIT Abhaengigkeiten und nicht bei jedem Bild, und das ist kein
     * Feinschliff. Ohne die Liste lief der Effekt auch bei den Bildern, die ein
     * Zug am Griff erzeugt (zwanzig Zeigerereignisse zwischen zwei
     * Wimpernschlaegen), setzte dabei jedes Mal Zustand, und React zaehlte die
     * Kette als geschachtelte Aktualisierung: der Beweislauf von W8 meldete
     * daraufhin vier "Maximum update depth exceeded" waehrend der Griffproben.
     * Die GROESSE des Kastens beobachtet ohnehin der ResizeObserver darunter;
     * dieser Effekt ist fuer seinen INHALT da, und der aendert sich genau an
     * diesen sechs Stellen.
     */
    useEffect(() => {
        measureBodyEdge();
    }, [
        measureBodyEdge,
        ir,
        props.status,
        props.view,
        props.pseudocode,
        props.imports,
        props.presentation.depth,
    ]);

    // Und wenn der Kasten selbst seine Hoehe aendert, weil jemand den Griff
    // darunter gezogen hat oder das Fenster kleiner wurde.
    useEffect(() => {
        const node = bodyBox.current;
        if (node === null || typeof ResizeObserver === 'undefined') {
            return;
        }
        const observer = new ResizeObserver(measureBodyEdge);
        observer.observe(node);
        return () => observer.disconnect();
    }, [measureBodyEdge, props.status, props.view]);

    // Ein Beleg, der offen bleibt, waehrend das Subjekt wechselt, wuerde die
    // Belege des alten Symbols ueber die Zeilen des neuen legen.
    useEffect(() => {
        setOpenEvidence(undefined);
    }, [ir]);

    useEffect(() => {
        globalThis.__atlasTwin = {
            symbol: props.symbolName,
            qualifiedName: props.symbolQualifiedName ?? '',
            fetches: globalThis.__atlasTwinFetches ?? 0,
            sectionNames: (model?.sections ?? []).map((section) => section.name),
            flowOpen: props.flowOpen,
            flowSteps: props.flow?.steps.length ?? 0,
            flowArrows: props.flow?.sequence.interactions.length ?? 0,
            flowParticipants: props.flow?.sequence.participants.length ?? 0,
            flowStep: props.flowStep,
            view: props.view,
            stepLines: (model?.sections ?? [])
                .filter((section) => section.name === 'steps')
                .flatMap((section) => section.rows.map((row) => row.siteLine ?? 0)),
            pseudocode: pseudocodeSeamOf(props.pseudocode, props.imports, props.symbolName),
            level: props.presentation.depth,
            levelName,
            question: model?.question ?? '',
            mode: model?.mode ?? '',
            rewritableSentences: model?.rewritable.length ?? 0,
            voiceState,
            voiceMessage,
            voicedSentences: voiced === undefined ? 0 : Object.keys(voiced).length,
            validateRewrite: (answer: string) => {
                const lines = model?.rewritable ?? [];
                if (lines.length === 0) {
                    return { applied: false, reason: 'this level has no assembled sentence' };
                }
                const outcome = applyReaderRewrite(lines, answer);
                return outcome.kind === 'applied'
                    ? { applied: true, reason: '' }
                    : { applied: false, reason: outcome.reason };
            },
            subject: (model?.rewritable ?? []).map((line) => ({
                id: line.id,
                text: line.text,
                facts: [...line.facts],
            })),
        };
    });

    // Welche Zeile der STEPS-Liste der Stepper gerade meint. -1 heisst keine,
    // und keine ist der Normalfall: der Erklaerer ist zu, oder er steht auf
    // einem Schritt, der zu einem anderen Symbol des Walks gehoert.
    const activeStepRow = props.flow !== undefined && props.flowStep >= 0
        ? (props.flow.stepRows[props.flowStep] ?? -1)
        : -1;

    const toggleEvidence = (factPath: string): void => {
        setOpenEvidence((current) => (current === factPath ? undefined : factPath));
    };
    const entriesFor = (factPath: string): Evidence[] =>
        ir === undefined ? [] : evidenceFor(ir, factPath);
    const follow = (target: SymbolRef | undefined): void => {
        if (target !== undefined) {
            props.onFollow(target);
        }
    };

    return (
        <aside className="atlas-twin" data-testid="atlas-twin" data-status={props.status}>
            <header className="atlas-twin-head">
                <span className="atlas-twin-title">{TWIN_TITLE}</span>
                {/*
                  * Der flow()-Kopf ist ein Knopf und kein Etikett.
                  *
                  * Nutzerfeedback vom 2026-08-28: er sah aus, als koennte man
                  * ihn anklicken, und konnte es nicht. Ein Wort, das wie eine
                  * Handlung aussieht und keine ist, ist schlimmer als eines,
                  * das nach nichts aussieht. Also ist es jetzt eine: Klick
                  * oeffnet und schliesst den Erklaerer, aria-expanded sagt es
                  * ansagbar, und der Zeiger und der Hover sagen es sichtbar.
                  */}
                <Hint name="twin-flow" text={explainerHeadTooltip(props.flowOpen)}>
                    <button
                        type="button"
                        className="atlas-twin-subject"
                        data-testid="atlas-twin-subject"
                        aria-expanded={props.flowOpen}
                        aria-controls="atlas-flow-overlay"
                        data-fold={props.flowOpen ? 'collapse' : 'open'}
                        data-fold-of="flow"
                        onClick={props.onToggleFlow}
                    >
                        flow <span className="atlas-twin-subject-name">({props.symbolName})</span>
                        {/*
                          * Ein Wort statt `[-]` und `[+]`. Der Nutzer hat die
                          * Zeichen beim Testen am 2026-08-29 nicht verstanden,
                          * und was sie bedeuteten, stand nur in einem nativen
                          * Tooltip, also in genau der Sache, die dieser Zyklus
                          * abschafft.
                          */}
                        <span className="atlas-twin-subject-fold">
                            {twinFoldLabel(props.flowOpen)}
                        </span>
                    </button>
                </Hint>
            </header>

            <div className="atlas-twin-toolbar" data-hint-keep="twin toolbar">
                <ReaderSlider depth={props.presentation.depth} onDepth={setLevel} />
                <FacetChips
                    facets={props.presentation.facets}
                    onToggle={props.onToggleFacet}
                />
                <div className="atlas-twin-tabs" role="tablist" aria-label="twin body">
                    <Hint name="twin-tab-facts" text={TWIN_TAB_TOOLTIP}>
                        <button
                            type="button"
                            className="atlas-twin-tab"
                            role="tab"
                            data-testid="atlas-twin-tab-facts"
                            data-on={props.view === 'facts'}
                            aria-selected={props.view === 'facts'}
                            onClick={() => props.onView('facts')}
                        >
                            {TWIN_TAB_LABEL}
                        </button>
                    </Hint>
                    <Hint name="twin-tab-pseudocode" text={PSEUDOCODE_TAB_TOOLTIP}>
                        <button
                            type="button"
                            className="atlas-twin-tab"
                            role="tab"
                            data-testid="atlas-pseudocode-toggle"
                            data-on={props.view === 'pseudocode'}
                            aria-selected={props.view === 'pseudocode'}
                            onClick={() => props.onView('pseudocode')}
                        >
                            {PSEUDOCODE_TAB_LABEL}
                        </button>
                    </Hint>
                </div>
            </div>

            {model === undefined || props.status !== 'ready' ? (
                <div
                    className="atlas-twin-body atlas-twin-empty"
                    data-testid="atlas-twin-empty"
                    ref={bodyBox}
                    onScroll={measureBodyEdge}
                >
                    <p className="atlas-twin-empty-message" data-state={props.status}>
                        {props.message}
                    </p>
                    {props.hint !== undefined && props.hint.length > 0 && (
                        <p className="atlas-twin-empty-hint">{props.hint}</p>
                    )}
                </div>
            ) : props.view === 'pseudocode' && props.pseudocode !== undefined ? (
                <div
                    className="atlas-twin-body"
                    data-mode="pseudocode"
                    ref={bodyBox}
                    onScroll={measureBodyEdge}
                >
                    <PseudocodeView
                        document={props.pseudocode}
                        imports={props.imports}
                        symbolName={props.symbolName}
                        onOpenLine={props.onOpenLine}
                        refineAvailable={props.refineAvailable}
                        refineState={props.refineState}
                        refineMessage={props.refineMessage}
                        onRefine={props.onRefine}
                        onRestoreOriginal={props.onRestoreOriginal}
                    />
                </div>
            ) : (
                <div
                    className="atlas-twin-body"
                    data-mode={model.mode}
                    data-level={levelName}
                    ref={bodyBox}
                    onScroll={measureBodyEdge}
                >
                    {/*
                      * Die Frage zuerst, und zwar auf jeder Stufe.
                      *
                      * Sie ist der ganze Umbau von W13 in einer Zeile: der
                      * Regler fragt, fuer wen, und die erste Zeile des Koerpers
                      * sagt, welche Frage daraufhin beantwortet wird. Ein Leser,
                      * der den Regler bewegt und nicht sicher ist, ob sich etwas
                      * geaendert hat, liest eine Zeile und weiss es.
                      */}
                    <p
                        className="atlas-twin-question"
                        data-testid="codeatlas-twin-question"
                        data-level={levelName}
                    >
                        {model.question}
                    </p>
                    <p className="atlas-twin-subject-line" data-testid="codeatlas-twin-subject-line">
                        {model.subject}
                    </p>
                    {model.context.map((text, index) => (
                        <p
                            className="atlas-twin-context-line"
                            data-testid="codeatlas-twin-context-line"
                            key={`context-${index}`}
                        >
                            {text}
                        </p>
                    ))}
                    {model.mode === 'prose' && (
                        <ProseView model={model} say={say} onFollow={(chip) => follow(chip.target)} />
                    )}
                    {model.mode === 'guided' && (
                        <JuniorView model={model} say={say} onFollow={follow} />
                    )}
                    {model.mode === 'sections' &&
                        model.sections.map((section) => (
                            <SectionView
                                key={section.name}
                                section={section}
                                voiceOf={(id) => voiced?.[id]}
                                openEvidence={openEvidence}
                                entriesFor={entriesFor}
                                onToggleEvidence={toggleEvidence}
                                onActivate={(row) => follow(row.target)}
                                onPoint={props.onPointRow}
                                caretLine={props.caretLine}
                                activeStepRow={activeStepRow}
                            />
                        ))}
                    {(model.mode === 'cost' || model.mode === 'ground') && (
                        <>
                            {model.blocks.map((block) => (
                                <ReaderBlockView
                                    key={block.name}
                                    block={block}
                                    say={say}
                                    openEvidence={openEvidence}
                                    entriesFor={entriesFor}
                                    onToggleEvidence={toggleEvidence}
                                    onActivate={(row) => follow(row.target)}
                                    onPoint={props.onPointRow}
                                />
                            ))}
                            {model.mode === 'cost' && model.notes.map((text, index) =>
                                say(`n${index}`, text, 'note'))}
                            {model.mode === 'ground' && (
                                <LimitsView limits={model.limits} say={say} />
                            )}
                        </>
                    )}
                    <VoiceBar
                        available={props.refineAvailable === true}
                        state={voiceState}
                        message={voiceMessage}
                        sentences={model.rewritable.length}
                        onAsk={askForVoice}
                        onRestore={dropVoice}
                        model={props.voiceModel ?? ''}
                    />
                </div>
            )}
            {(bodyEdge.above || bodyEdge.below) && (
                <span
                    className="atlas-twin-more"
                    data-testid="atlas-twin-more"
                    data-scroll-hint={[
                        bodyEdge.above ? 'top' : '',
                        bodyEdge.below ? 'bottom' : '',
                    ].filter((part) => part.length > 0).join(' ')}
                    data-edge={bodyEdge.below ? 'bottom' : 'top'}
                >
                    {bodyEdge.below ? `▾ ${TWIN_MORE_BELOW}` : `▴ ${TWIN_MORE_ABOVE}`}
                </span>
            )}
        </aside>
    );
}
