/**
 * Der Flow-Erklaerer als Overlay ueber der Editorflaeche: links das
 * Sequenzdiagramm, rechts die nach Symbolen gruppierte Schrittliste, darunter
 * der Stepper und die zwei Ehrlichkeits-Absaetze.
 *
 * Vorbild ist der Explainer des Referenzprojekts (CodeAtlasIDE,
 * theia-extensions/codeatlas-views/src/browser/pseudocode/flow-explainer.tsx):
 * derselbe Aufbau, derselbe Wortlaut, dieselbe Stepper-Mechanik. Fuenf
 * Entscheidungen, und jede ist eine Antwort auf einen Befund aus dem
 * Nutzerfeedback vom 2026-08-29:
 *
 * 1. **Ein eigener Platz und kein eingequetschter Kasten.** Der Kasten sass in
 *    der rechten Spalte zwischen Kopfzeile und Detailstufen-Regler und
 *    verschwand hinter ihnen, sobald er breiter wurde als die Spalte. Ein Bild,
 *    das man scrollen muss, um es zu sehen, beantwortet die Frage nicht, fuer
 *    die es da ist. W5c hat es darum ueber den Editor gelegt; seit W8 ist es
 *    der Reiter `flow` des Erklaeren-Bereichs unter dem Reader. Beide Male
 *    dieselbe Antwort auf denselben Befund, und die zweite ist die bessere: es
 *    liegt nicht mehr VOR dem Code, den es erklaert, sondern daneben, und es
 *    streitet sich mit keiner anderen Flaeche mehr um seinen Platz. Der Rahmen
 *    (Zumachen, Escape, Hoehe) gehoert seitdem der Zone; hier steht nur noch
 *    das Bild und was dazugehoert.
 * 2. **Der Erklaerer haengt nicht an der Detailstufe.** Der flow()-Knopf steht
 *    auf allen vier Stufen im Twin-Kopf, und was das Overlay zeigt, ist auf
 *    allen vier dasselbe: der Walk ist eine Antwort ueber den Code und keine
 *    ueber die Lesetiefe des Panels.
 * 3. **Ein echtes Diagramm, aus eigenem SVG.** Keine Bibliothek: diese
 *    Oberflaeche ist air-gapped und liefert keine aus. Die Geometrie rechnet
 *    `flow-diagram.ts`, damit jede Zahl dieses Bildes ohne Browser pruefbar
 *    ist; hier steht nur, wie sie aussieht. Alle Farben kommen aus tokens.css,
 *    der helle Stil der Vorlage kommt ausdruecklich nicht mit.
 * 4. **Die Liste ist nach Symbolen gruppiert und sagt, wo nichts steht.** Ein
 *    Symbol des Walks, fuer das der Index nichts aufgezeichnet hat, traegt
 *    seinen ehrlichen Satz statt einer leeren Zeile. Eine Gruppe ohne alles
 *    liest sich wie ein Symbol, das nichts tut, und das ist eine andere
 *    Behauptung als "der Index hat nichts aufgezeichnet".
 * 5. **Der Stepper bewegt drei Dinge oder keins.** Diagramm, Liste und Editor
 *    folgen demselben Index. Ein Schritt, der im Bild nichts anleuchtet (ein
 *    Fehlerpfad ohne Lebenslinie im Bild, eine Umgebungslesung), sagt das,
 *    statt irgendeinen Pfeil zu faerben.
 */

import type { JSX } from 'react';
import { useEffect, useMemo, useState } from 'react';

import Hint from '../ui/tooltip/Hint';
import { askModel } from '../chat/chat-client';
import { SIDECAR_ORIGIN } from '../llm/sidecar';
import { READER_SYSTEM_PROMPT, buildReaderPrompt, nonThinkingFor } from '../compiler/prompt-contract';
import { applyReaderRewrite, readerLines, readerMaxTokens, readerSubjectText } from '../twin/reader-rewrite';
import type { PseudocodeSourceRef } from './pseudocode-builder';
import type { FlowView } from './flow-view';
import { buildFlowDiagram, loopForStep, DIAGRAM_HEAD_SIZE, DIAGRAM_LOOP_WIDTH } from './flow-diagram';
import type { FlowDiagram } from './flow-diagram';
import { SEQUENCE_INTERACTION_CAP, SEQUENCE_PARTICIPANT_CAP } from './flow-model';
import {
    EXPLAINER_ARROWS_LABEL,
    EXPLAINER_BEYOND_BOX,
    EXPLAINER_EMPTY,
    EXPLAINER_GENERATED_NOTE,
    EXPLAINER_HONESTY_SHORT,
    EXPLAINER_NEXT_LABEL,
    EXPLAINER_NEXT_TOOLTIP,
    EXPLAINER_NO_DIAGRAM_HIT,
    EXPLAINER_NO_STEPS,
    EXPLAINER_PARTICIPANTS_LABEL,
    EXPLAINER_PREV_LABEL,
    EXPLAINER_PREV_TOOLTIP,
    EXPLAINER_PROVENANCE_TOOLTIP,
    EXPLAINER_SOURCE_NOTE,
    EXPLAINER_STEPS_TITLE,
    PSEUDOCODE_LINE_TOOLTIP,
    PSEUDOCODE_SOURCE_NOTE,
    explainerCycleNote,
    explainerNotStarted,
    explainerOmittedNote,
    explainerPosition,
    explainerTitle,
    explainerUnresolvedNote,
    explainerWalkBound,
} from './pseudocode-strings';

export interface FlowOverlayProps {
    /** Der Name des Symbols, ueber das der Erklaerer spricht. */
    symbolName: string;
    /** Der Walk, sobald er da ist. */
    flow?: FlowView | undefined;
    /** Der Satz, der statt des Bildes steht, solange keiner da ist. */
    message?: string | undefined;
    /** Der aktive Schritt, oder -1 fuer "noch keiner". Nie 0 als "keiner". */
    step: number;
    onStep: (index: number) => void;
    /** Eine Zeile oeffnen: Datei und Zeile, wie die Zeile sie mitbringt. */
    onOpenLine?: ((ref: PseudocodeSourceRef) => void) | undefined;
    aiAvailable?: boolean;
    modelName?: string;
    /** Router id for the explicit request; distinct from its visible name. */
    requestModel?: string;
}

/**
 * Der Griff, an dem der Beweislauf das Overlay anfasst.
 *
 * Absichtlich schmal: was das Bild haelt, wo der Stepper steht und wie viele
 * Gruppen und Absenz-Saetze die Liste traegt. Alles davon steht auch im DOM.
 */
export interface AtlasFlowSeam {
    open: boolean;
    lifelines: number;
    arrows: number;
    loops: number;
    steps: number;
    step: number;
    activeArrow: number;
    activeLoop: number;
    groups: number;
    absences: number;
    /** Die Grenzen des Walks, wie sie am Rand des Bildes stehen. */
    walkDepth: number;
    walkCap: number;
    /**
     * Wie viele Symbole dieses Walks der Index nicht aufloesen konnte.
     *
     * Die Zahl steht hier, damit der Beweislauf die Messung aus AC6.2 lesen
     * kann statt sie aus dem Text zu raten: meldet der Index unaufgeloeste
     * Aufrufe an dieser Stelle ueberhaupt? Null ist eine gueltige Antwort und
     * bedeutet "nein, an dieser Stelle nicht".
     */
    unresolved: number;
    subject: readonly { id: string; text: string; facts: readonly string[] }[];
    validateRewrite: (answer: string) => { applied: boolean; reason: string };
    aiState: 'idle' | 'running' | 'applied' | 'refused';
    aiReason: string;
}

declare global {
    // eslint-disable-next-line no-var
    var __atlasFlow: AtlasFlowSeam | undefined;
}

export default function FlowOverlay(props: FlowOverlayProps): JSX.Element {
    const flow = props.flow;
    const total = flow?.steps.length ?? 0;
    const diagram = flow === undefined || total === 0 ? undefined : buildFlowDiagram(flow);
    const activeArrow = flow !== undefined && props.step >= 0 ? (flow.arrows[props.step] ?? -1) : -1;
    const activeLoop = diagram === undefined ? -1 : loopForStep(diagram, props.step);
    const active = flow !== undefined && props.step >= 0 ? flow.steps[props.step] : undefined;
    const builtLines = useMemo(() => readerLines((flow?.document.lines ?? []).map((line, index) => ({
        id: `flow-${index}`,
        text: line.text,
    }))), [flow]);
    // The flow view is often rebuilt by its parent. Its object identity is not
    // a symbol change, so using it as a reset trigger would erase an AI result
    // in the render immediately after the request resolves.
    const readerKey = `${props.symbolName}:${builtLines.map((line) => `${line.id}:${line.text}`).join('\u001f')}`;
    const [aiLines, setAiLines] = useState<readonly string[] | undefined>(undefined);
    const [aiState, setAiState] = useState<'idle' | 'running' | 'applied' | 'refused'>('idle');
    const [aiReason, setAiReason] = useState('');
    useEffect(() => {
        setAiLines(undefined);
        setAiState('idle');
        setAiReason('');
    }, [readerKey]);
    const askAi = (): void => {
        if (props.aiAvailable !== true || builtLines.length === 0) return;
        setAiState('running');
        void askModel({
            origin: SIDECAR_ORIGIN,
            system: READER_SYSTEM_PROMPT,
            user: buildReaderPrompt('flow reader', readerSubjectText(builtLines)),
            chatTemplateKwargs: nonThinkingFor(props.requestModel ?? props.modelName ?? '').chatTemplateKwargs,
            maxTokens: readerMaxTokens(builtLines),
            fetch: (url, init) => window.fetch(url, init),
            ...(props.requestModel === undefined ? {} : { model: props.requestModel }),
        }).then((reply) => {
            const checked = applyReaderRewrite(builtLines, reply.content);
            if (checked.kind === 'applied') {
                setAiLines(checked.lines.map((line) => line.text));
                setAiState('applied');
                setAiReason('');
            } else {
                setAiState('refused');
                setAiReason(checked.reason);
            }
        }).catch((error: unknown) => {
            setAiState('refused');
            setAiReason(error instanceof Error ? error.message : String(error));
        });
    };

    /*
     * Kein Fokusraub mehr.
     *
     * Bis W8 holte sich diese Flaeche beim Aufschlagen den Fokus, weil sie ein
     * Overlay war und Escape sonst die Flaeche getroffen haette, die der Leser
     * gerade verlassen hat. Als Reiter waere derselbe Griff ein Fehler: ein
     * Reiterwechsel wuerde den Cursor aus dem Editor oder aus der
     * Kommandozeile reissen. Escape haengt jetzt am Fenster und gehoert der
     * Zone (App.tsx).
     */

    // Zu heisst zu, und der Griff sagt es, statt die Lage von vorhin
    // stehenzulassen.
    useEffect(() => () => {
        globalThis.__atlasFlow = {
            open: false,
            lifelines: 0,
            arrows: 0,
            loops: 0,
            steps: 0,
            step: -1,
            activeArrow: -1,
            activeLoop: -1,
            groups: 0,
            absences: 0,
            walkDepth: 0,
            walkCap: 0,
            unresolved: 0,
            subject: [],
            validateRewrite: () => ({ applied: false, reason: 'no flow is shown' }),
            aiState: 'idle',
            aiReason: '',
        };
    }, []);

    useEffect(() => {
        globalThis.__atlasFlow = {
            open: true,
            lifelines: diagram?.lifelines.length ?? 0,
            arrows: diagram?.arrows.length ?? 0,
            loops: diagram?.loops.length ?? 0,
            steps: total,
            step: props.step,
            activeArrow,
            activeLoop,
            groups: (flow?.document.lines ?? []).filter((line) => line.kind === 'group').length,
            absences: (flow?.document.lines ?? []).filter((line) => line.kind === 'note').length,
            walkDepth: flow?.bound.depth ?? 0,
            walkCap: flow?.bound.cap ?? 0,
            unresolved: flow?.unresolved ?? 0,
            subject: builtLines.map((line) => ({ id: line.id, text: line.text, facts: [...line.facts] })),
            validateRewrite: (answer: string) => {
                const outcome = applyReaderRewrite(builtLines, answer);
                return outcome.kind === 'applied'
                    ? { applied: true, reason: '' }
                    : { applied: false, reason: outcome.reason };
            },
            aiState,
            aiReason,
        };
    });

    return (
        <div
            className="atlas-flow-overlay"
            data-testid="atlas-flow-overlay"
            data-state={flow === undefined ? 'waiting' : 'ready'}
            role="region"
            aria-label={explainerTitle(props.symbolName)}
        >
            <header className="atlas-flow-overlay-head">
                <span className="atlas-flow-overlay-title" data-testid="atlas-flow-overlay-title">
                    {explainerTitle(props.symbolName)}
                </span>
                {props.aiAvailable === true && builtLines.length > 0 && aiState !== 'applied' && (
                    <button type="button" data-testid="atlas-flow-ai-btn" disabled={aiState === 'running'} onClick={askAi}>AI version</button>
                )}
                {props.aiAvailable === true && aiState === 'applied' && (
                    <button type="button" data-testid="atlas-flow-ai-restore" onClick={() => { setAiLines(undefined); setAiState('idle'); }}>built version</button>
                )}
            </header>
            <p data-testid="atlas-flow-built-provenance">built from indexed flow steps.</p>
            {aiLines !== undefined && <p data-testid="atlas-flow-ai-provenance">worded by local model {props.modelName ?? ''}; every required fact passed the rewrite guard.</p>}
            {aiState === 'refused' && <p data-testid="atlas-flow-ai-refused">{aiReason}</p>}

            {aiLines !== undefined ? (
                <div className="atlas-flow-ai-version" data-testid="atlas-flow-ai-answer">
                    {aiLines.map((line, index) => <p key={index}>{line}</p>)}
                </div>
            ) : flow === undefined ? (
                <p className="atlas-flow-note" data-testid="atlas-flow-message">
                    {props.message ?? ''}
                </p>
            ) : diagram === undefined ? (
                <p className="atlas-flow-note" data-testid="atlas-flow-empty">
                    {EXPLAINER_EMPTY}
                </p>
            ) : (
                <>
                    <div className="atlas-flow-overlay-split">
                        <div
                            className="atlas-flow"
                            data-testid="atlas-flow"
                            data-participants={diagram.lifelines.length}
                            data-arrows={diagram.arrows.length}
                            data-steps={total}
                            data-active-step={props.step}
                            data-active-arrow={activeArrow}
                        >
                            <div className="atlas-flow-legend">
                                {`${EXPLAINER_PARTICIPANTS_LABEL} as columns, ${EXPLAINER_ARROWS_LABEL} as arrows`}
                            </div>
                            <div className="atlas-flow-box" data-testid="atlas-flow-box">
                                <Diagram
                                    diagram={diagram}
                                    activeArrow={activeArrow}
                                    activeLoop={activeLoop}
                                    bound={flow.bound}
                                />
                            </div>
                            {diagram.cycles > 0 && (
                                <p className="atlas-flow-note" data-testid="atlas-flow-cycle-note">
                                    {explainerCycleNote(diagram.cycles)}
                                </p>
                            )}
                            {diagram.omitted > 0 && (
                                <p className="atlas-flow-note" data-testid="atlas-flow-omitted-note">
                                    {explainerOmittedNote(
                                        diagram.omitted,
                                        SEQUENCE_PARTICIPANT_CAP,
                                        SEQUENCE_INTERACTION_CAP,
                                    )}
                                </p>
                            )}
                        </div>

                        <div className="atlas-flow-steps" data-testid="atlas-flow-steps">
                            <h3 className="atlas-flow-steps-title">{EXPLAINER_STEPS_TITLE}</h3>
                            {total === 0 && (
                                <p className="atlas-flow-note" data-testid="atlas-flow-empty">
                                    {EXPLAINER_NO_STEPS}
                                </p>
                            )}
                            <StepList flow={flow} step={props.step} onStep={props.onStep} />
                        </div>
                    </div>

                    <div className="atlas-flow-stepper" data-testid="atlas-flow-stepper">
                        <Hint name="flow-prev" text={EXPLAINER_PREV_TOOLTIP}>
                            <button
                                type="button"
                                className="atlas-flow-step-btn"
                                data-testid="atlas-flow-prev"
                                disabled={total === 0 || props.step <= 0}
                                onClick={() => props.onStep(Math.max(0, props.step - 1))}
                            >
                                {EXPLAINER_PREV_LABEL}
                            </button>
                        </Hint>
                        <span className="atlas-flow-position" data-testid="atlas-flow-position">
                            {props.step < 0
                                ? explainerNotStarted(total)
                                : explainerPosition(props.step + 1, total)}
                        </span>
                        <Hint name="flow-next" text={EXPLAINER_NEXT_TOOLTIP}>
                            <button
                                type="button"
                                className="atlas-flow-step-btn"
                                data-testid="atlas-flow-next"
                                disabled={total === 0 || props.step >= total - 1}
                                onClick={() => props.onStep(Math.min(total - 1, props.step + 1))}
                            >
                                {EXPLAINER_NEXT_LABEL}
                            </button>
                        </Hint>
                    </div>

                    {active !== undefined && (
                        <p
                            className="atlas-flow-current"
                            data-testid="atlas-flow-current"
                            data-kind={active.line.kind}
                            data-alarm={active.line.kind === 'raise'}
                        >
                            {props.onOpenLine !== undefined && active.line.sourceRef !== undefined ? (
                                <Hint name="flow-current-line" text={PSEUDOCODE_LINE_TOOLTIP}>
                                    <button
                                        type="button"
                                        className="atlas-flow-current-line"
                                        onClick={() => props.onOpenLine?.(active.line.sourceRef!)}
                                    >
                                        {active.line.text}
                                    </button>
                                </Hint>
                            ) : (
                                active.line.text
                            )}
                        </p>
                    )}
                    {/*
                      * Ein Schritt, der im Bild nichts anleuchtet, sagt es. Der
                      * Editor folgt ihm trotzdem; irgendeinen Pfeil zu faerben
                      * waere die eine Luege, die diese Flaeche billig haette.
                      */}
                    {active !== undefined && activeArrow < 0 && activeLoop < 0 && (
                        <p className="atlas-flow-note" data-testid="atlas-flow-no-hit">
                            {EXPLAINER_NO_DIAGRAM_HIT}
                        </p>
                    )}

                    {/*
                      * Der Ehrlichkeitsblock, nach W8b.
                      *
                      * Bis dahin standen hier vier Absaetze mit zusammen 954
                      * Zeichen, von denen drei dieselbe Sache in drei Anlaeufen
                      * sagten. Der Nutzer hat am 2026-08-29 gefragt: "sind
                      * diese Texte wirklich hilfreich oder bullshit?" Die
                      * Antwort ist beides: die Aussagen sind noetig, die
                      * Wiederholung nicht. Eine Ehrlichkeit, die als Textwand
                      * erscheint, wird ueberlesen wie ein Cookie-Banner.
                      *
                      * Also EIN Satz, die Zahlen am Rand des Bildes, und die
                      * Herkunft hinter dem Fragezeichen: genau das Idiom, das
                      * dieses Produkt fuer Herkunft schon hat. Der
                      * schrittbezogene Satz weiter oben ("This step is not an
                      * arrow in the picture") bleibt davon unberuehrt: er loest
                      * eine konkrete Verwirrung an genau der Stelle, an der sie
                      * entsteht, und ist damit das Gegenteil der drei
                      * gestrichenen.
                      */}
                    <div className="atlas-flow-honesty" data-testid="atlas-flow-honesty-block">
                        <p
                            className="atlas-flow-note"
                            data-testid="atlas-flow-honesty"
                            data-unresolved={flow.unresolved}
                        >
                            {flow.unresolved > 0
                                ? explainerUnresolvedNote(flow.unresolved)
                                : EXPLAINER_HONESTY_SHORT}
                        </p>
                        {/*
                          * Der zweite Satz, und er sagt etwas anderes als der
                          * erste: der erste handelt davon, was der Index nicht
                          * aufloesen konnte, der zweite davon, wo der Walk
                          * absichtlich aufhoert. Die ZAHLEN dazu stehen am Rand
                          * des Bildes; was hier steht, ist das, was sie
                          * bedeuten.
                          */}
                        <p className="atlas-flow-note" data-testid="atlas-flow-honesty">
                            {flow.sequence.interactions.length === 0
                                ? EXPLAINER_EMPTY
                                : EXPLAINER_BEYOND_BOX}
                        </p>
                        {/*
                          * Das Fragezeichen nimmt den Klick-Griff des Hints
                          * (src/ui/tooltip/Hint.tsx, HintHold). Es ist ein
                          * Knopf, also verspricht es eine Handlung, und bis
                          * W10-1 war Hover die einzige: auf einem Zeigegeraet
                          * ohne Hover war der Satz dahinter unerreichbar. Ein
                          * Klick haelt ihn jetzt fest, ein zweiter, Escape oder
                          * ein Klick daneben laesst ihn los.
                          */}
                        <Hint
                            name="flow-provenance"
                            text={[EXPLAINER_SOURCE_NOTE, PSEUDOCODE_SOURCE_NOTE, EXPLAINER_GENERATED_NOTE, ...flow.notes].join(' ')}
                        >
                            {(hold) => (
                                <button
                                    type="button"
                                    className="atlas-flow-provenance"
                                    data-testid="atlas-flow-provenance"
                                    aria-label={EXPLAINER_PROVENANCE_TOOLTIP}
                                    aria-expanded={hold.held}
                                    onClick={hold.toggle}
                                >
                                    ?
                                </button>
                            )}
                        </Hint>
                    </div>
                </>
            )}
        </div>
    );
}

/**
 * Das Bild.
 *
 * Reines Zeichnen: jede Zahl kommt aus {@link buildFlowDiagram}, jede Farbe aus
 * tokens.css ueber die Klassen. Der aktive Schritt traegt `data-current`, und
 * die Regel dafuer steht in der CSS-Datei neben allen anderen, statt hier als
 * Inline-Stil, den kein Design-Token je wieder einholt.
 */
function Diagram(props: {
    diagram: FlowDiagram;
    activeArrow: number;
    activeLoop: number;
    /** Die Grenzen des Walks. Sie werden an den unteren Rand geschrieben. */
    bound: { depth: number; cap: number };
}): JSX.Element {
    const { diagram } = props;
    return (
        <svg
            className="atlas-flow-svg"
            data-testid="atlas-flow-diagram"
            viewBox={`0 0 ${diagram.width} ${diagram.height}`}
            width="100%"
            height={diagram.height}
            preserveAspectRatio="xMinYMin meet"
            role="img"
            aria-label="sequence of the recorded calls"
        >
            {/*
              * Die Grenze wird gezeichnet statt beschrieben.
              *
              * Sie stand bis W8b als vierter von vier Absaetzen UNTER dem Bild,
              * also an der Stelle, an der sie nach drei Absaetzen Ehrlichkeit
              * niemand mehr liest. Hier steht sie dort, wo sie gilt: am unteren
              * Rand des Kastens, den sie begrenzt.
              */}
            <text
                className="atlas-flow-bound"
                data-testid="atlas-flow-walk-bound"
                data-depth={props.bound.depth}
                data-cap={props.bound.cap}
                x={2}
                y={diagram.height - 3}
            >
                {explainerWalkBound(props.bound.depth, props.bound.cap)}
            </text>
            {diagram.lifelines.map((lifeline) => (
                <g
                    className="atlas-flow-lifeline"
                    data-testid="atlas-flow-lifeline"
                    data-label={lifeline.label}
                    key={`lifeline-${lifeline.index}`}
                >
                    <line
                        className="atlas-flow-lifeline-line"
                        x1={lifeline.x}
                        y1={lifeline.lineTop}
                        x2={lifeline.x}
                        y2={lifeline.lineBottom}
                    />
                    <rect
                        className="atlas-flow-lifeline-box"
                        x={lifeline.boxX}
                        y={lifeline.headY}
                        width={lifeline.boxWidth}
                        height={lifeline.boxHeight}
                        rx={2}
                    />
                    <text
                        className="atlas-flow-lifeline-label"
                        data-testid="atlas-flow-participant"
                        data-label={lifeline.label}
                        x={lifeline.x}
                        y={lifeline.headY + lifeline.boxHeight / 2 + 4}
                        textAnchor="middle"
                    >
                        {lifeline.label}
                    </text>
                    <rect
                        className="atlas-flow-lifeline-box"
                        x={lifeline.boxX}
                        y={lifeline.footY}
                        width={lifeline.boxWidth}
                        height={lifeline.boxHeight}
                        rx={2}
                    />
                    <text
                        className="atlas-flow-lifeline-label"
                        x={lifeline.x}
                        y={lifeline.footY + lifeline.boxHeight / 2 + 4}
                        textAnchor="middle"
                    >
                        {lifeline.label}
                    </text>
                </g>
            ))}

            {diagram.arrows.map((arrow) => {
                const current = arrow.index === props.activeArrow;
                const direction = arrow.toX >= arrow.fromX ? 1 : -1;
                const tip = arrow.self ? arrow.fromX + 2 : arrow.toX - direction * 3;
                return (
                    <g
                        className="atlas-flow-arrow"
                        data-testid="atlas-flow-arrow"
                        data-index={arrow.index}
                        data-to={diagram.lifelines[arrow.to]?.label ?? ''}
                        data-message={arrow.label}
                        data-current={current}
                        data-cycle={arrow.cycle}
                        data-shape={arrow.self ? 'self' : 'across'}
                        key={`arrow-${arrow.index}`}
                    >
                        {arrow.self ? (
                            <path
                                className="atlas-flow-arrow-line"
                                d={selfPath(arrow.fromX, arrow.y)}
                                fill="none"
                            />
                        ) : (
                            <line
                                className="atlas-flow-arrow-line"
                                x1={arrow.fromX}
                                y1={arrow.y}
                                x2={tip}
                                y2={arrow.y}
                            />
                        )}
                        <polygon
                            className="atlas-flow-arrow-head"
                            points={
                                arrow.self
                                    ? headPoints(tip, arrow.y + 16, 1)
                                    : headPoints(tip, arrow.y, direction)
                            }
                        />
                        <text
                            className="atlas-flow-arrow-label"
                            x={arrow.labelX}
                            y={arrow.labelY}
                            textAnchor={arrow.labelAnchor}
                        >
                            {arrow.label}
                        </text>
                    </g>
                );
            })}

            {diagram.loops.map((loop) => (
                <g
                    className="atlas-flow-raise"
                    data-testid="atlas-flow-raise"
                    data-index={loop.index}
                    data-current={loop.index === props.activeLoop}
                    data-participant={diagram.lifelines[loop.lifeline]?.label ?? ''}
                    key={`loop-${loop.index}`}
                >
                    <path className="atlas-flow-raise-line" d={selfPath(loop.x, loop.y)} fill="none" />
                    <polygon
                        className="atlas-flow-raise-head"
                        points={headPoints(loop.x + 2, loop.y + 16, 1)}
                    />
                    <text
                        className="atlas-flow-raise-label"
                        x={loop.labelX}
                        y={loop.labelY}
                        textAnchor={loop.labelAnchor}
                    >
                        {loop.label}
                    </text>
                </g>
            ))}
        </svg>
    );
}

/** Der Bogen einer Schleife: raus, runter, zurueck an dieselbe Linie. */
function selfPath(x: number, y: number): string {
    const out = x + DIAGRAM_LOOP_WIDTH;
    return `M ${x + 2} ${y} H ${out} V ${y + 16} H ${x + 2}`;
}

/** Die Pfeilspitze als Dreieck, mit der Spitze auf der Zielkoordinate. */
function headPoints(x: number, y: number, direction: number): string {
    const back = x + direction * DIAGRAM_HEAD_SIZE * 1.6;
    return `${x},${y} ${back},${y - DIAGRAM_HEAD_SIZE} ${back},${y + DIAGRAM_HEAD_SIZE}`;
}

/**
 * Die Schritte, so gruppiert, wie der Block sie gruppiert.
 *
 * Genau die Zeilen des Dokuments, in genau seiner Reihenfolge: die
 * Ueberschriften als Ueberschrift, die nummerierten Zeilen als Halt des
 * Steppers, und die Saetze ueber leere Gruppen als das, was sie sind. Die
 * Zuordnung Zeile zu Schritt kommt aus `flowSteps`, damit Liste und Stepper
 * dieselbe Nummerierung benutzen und nicht zwei.
 */
function StepList(props: {
    flow: FlowView;
    step: number;
    onStep: (index: number) => void;
}): JSX.Element {
    const stepByLine = new Map<number, number>();
    props.flow.steps.forEach((step, index) => stepByLine.set(step.lineIndex, index));
    return (
        <ol className="atlas-flow-step-list" data-testid="atlas-flow-step-list">
            {props.flow.document.lines.map((line, lineIndex) => {
                if (line.kind === 'group') {
                    return (
                        <li
                            className="atlas-flow-step-group"
                            data-testid="atlas-flow-group"
                            data-group={line.group}
                            key={`group-${lineIndex}`}
                        >
                            {line.text}
                        </li>
                    );
                }
                if (line.kind === 'note') {
                    return (
                        <li
                            className="atlas-flow-step-absence"
                            data-testid="atlas-flow-absence"
                            data-group={line.group}
                            key={`note-${lineIndex}`}
                        >
                            {line.text}
                        </li>
                    );
                }
                const index = stepByLine.get(lineIndex) ?? -1;
                const active = index >= 0 && index === props.step;
                return (
                    <li
                        className="atlas-flow-step"
                        data-testid="atlas-flow-step"
                        data-step={index}
                        data-kind={line.kind}
                        data-active={active}
                        data-alarm={line.kind === 'raise'}
                        ref={active ? scrollIntoView : undefined}
                        key={`step-${lineIndex}`}
                    >
                        <Hint name="flow-step-line" text={PSEUDOCODE_LINE_TOOLTIP}>
                            <button
                                type="button"
                                className="atlas-flow-step-line"
                                data-testid="atlas-flow-step-button"
                                onClick={() => props.onStep(index)}
                            >
                                {line.text}
                            </button>
                        </Hint>
                    </li>
                );
            })}
        </ol>
    );
}

/**
 * Den aktiven Schritt in Sicht holen.
 *
 * Als ref-Rueckruf und nicht als Effekt, weil er an genau dem einen Element
 * haengt, das ihn braucht. `nearest`, damit eine Liste, die den Schritt schon
 * zeigt, nicht unter dem Auge des Lesers springt.
 */
function scrollIntoView(node: HTMLLIElement | null): void {
    // jsdom kennt scrollIntoView nicht, und das ist kein Grund, es dort
    // unterzuschieben.
    if (typeof node?.scrollIntoView === 'function') {
        node.scrollIntoView({ block: 'nearest' });
    }
}
