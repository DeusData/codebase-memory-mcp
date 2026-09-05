/**
 * Das Einstellungen-Panel: welches Modell antwortet, wie man ein anderes
 * bekommt, und alles, was Rechenzeit kostet.
 *
 * Nutzerwunsch vom 2026-08-29, woertlich: "2D/3D oder sowas sollte immer
 * zentral in einem Settings-Menue drin sein, nicht alles auf einer Oberflaeche,
 * wegen Rechenleistung falls jemand keine so starke Maschine hat." Und Martin
 * am selben Tag: "Man bietet in der UI das Feature so an, dass man sich von zum
 * Beispiel Hugging Face selber ein Modell aussuchen kann, das wird dann im
 * local cache gedownloaded."
 *
 * Fuenf Entscheidungen, und keine davon ist Geschmack:
 *
 * 1. **Overlay ueber der Editorflaeche, nach dem Muster der Hilfe.** Dasselbe
 *    Muster wie src/help/HelpOverlay.tsx: eine eigene Ebene, der Kasten scrollt
 *    selbst, Escape schliesst. Ein Panel in der rechten Spalte waere eine
 *    Tabelle mit sechs Zahlenspalten in einer 440 Pixel breiten Saeule.
 * 2. **Aus heisst aus, auch hier.** Dieses Panel hat kein eigenes `fetch`. Was
 *    es ueber den Sidecar weiss, bekommt es als Props von App.tsx, wo die eine
 *    Stelle steht, die ueber das Fragen entscheidet, und der Aktualisieren-Knopf
 *    ruft genau dieselbe Probe, damit er im selben Zaehler landet. Solange das
 *    lokale Modell aus ist, gibt es diesen Knopf nicht, und das Panel steht
 *    trotzdem da und erklaert sich.
 * 3. **Keine Zahl ohne Herkunft.** Die vier Angaben des laufenden Modells
 *    tragen im Tooltip die Anfrage, aus der sie stammen, genau wie in
 *    SidecarPanel.tsx. Die sechs Vorschlaege tragen ihre Eval-Zahlen, und ein
 *    Unit-Test haelt jede davon gegen verification/w5/eval.json und die ADR
 *    (src/settings/model-catalog.test.ts).
 * 4. **Kein Fortschrittsbalken.** Diese Oberflaeche kann keinen Download sehen:
 *    sie hat kein Backend, startet keinen Prozess und schreibt keine Datei. Sie
 *    zeigt den fertigen Befehl, sagt darueber, wohin er laedt, und liest die
 *    Liste danach neu. Ein Balken ueber eine Uebertragung, die ein anderer
 *    Prozess faehrt, waere eine Animation und keine Messung.
 * 5. **Jede Leistungseinstellung misst sich selbst.** Ein Fenster vor der
 *    Aenderung, eine Beruhigungsphase, ein Fenster danach, und davor das
 *    Rauschband dieser Maschine. Liegt der Unterschied darin, sagt das Panel
 *    genau das. Gemessen wird nicht hier, sondern in der Szene
 *    (src/galaxy/frame-rate.ts): eine zweite Rechnung fuer dieselbe Zahl waere
 *    eine zweite Wahrheit.
 *
 * Der ganze Text steht im Katalog (src/i18n/messages.ts), wie bei der Hilfe.
 * Hier steht nur, in welcher Reihenfolge er dasteht und woran er haengt.
 */

import type { JSX, ReactNode } from 'react';
import { useCallback, useEffect, useRef, useState } from 'react';

import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import {
    FRAME_WINDOW_MS,
    frameHistory,
    frameRateSnapshot,
    meanOf,
    noiseBandOf,
    subscribeFrameRate,
    verdictOf,
} from '../galaxy/frame-rate';
import type { FrameVerdict } from '../galaxy/frame-rate';
import {
    DEFAULT_GRAPH_DISPLAY,
    FRAME_CAPS,
    LABEL_DISTANCE_FACTORS,
    THRIFTY_GRAPH_DISPLAY,
    displayKey,
    isDefaultDisplay,
} from '../galaxy/density';
import type { EdgeDensity, GraphDisplaySettings, GraphProjection } from '../galaxy/density';
import { humanBytes } from '../llm/sidecar';
import type { CacheModel, SidecarFacts, SidecarState } from '../llm/sidecar';
import { LLM_START_COMMAND } from '../llm/strings';
import {
    MODEL_SUGGESTIONS,
    fetchCommand,
    percentText,
    readRepoInput,
    speedText,
} from './model-catalog';
import { modelKey } from './model-preference';

const text = messages.settings;

/** Der Router-Aufruf, den die Meldung "kein Router" vorschlaegt. */
export const ROUTER_START_COMMAND = 'llm/start.sh';

/**
 * Wie lange nach einer Aenderung gewartet wird, bevor gemessen wird.
 *
 * Eine Szene, die gerade eine Kameraebene austauscht oder eine Textur wegwirft,
 * braucht ein paar Bilder, bis sie wieder das tut, was sie von nun an tut. Ohne
 * diese Pause waere die erste Messung nach dem Umschalten die Messung des
 * Umschaltens.
 */
export const SETTLE_MS = 1200;

/** Wie lange danach gesammelt wird. Drei Fenster, damit ein Ausreisser untergeht. */
export const MEASURE_MS = 1500;

/** Wie weit zurueck das Fenster VOR der Aenderung reicht. */
export const BEFORE_MS = 3000;

/** Eine abgeschlossene Messung an einer Einstellung. */
export interface SettingsMeasurement {
    /** Welche Einstellung umgelegt wurde. */
    setting: string;
    /** Bilder je Sekunde vorher. 0, wenn nichts gemessen wurde. */
    before: number;
    /** Bilder je Sekunde nachher. 0, wenn nichts gemessen wurde. */
    after: number;
    /** Die Streuung dieser Maschine ohne Aenderung. */
    band: number;
    verdict: FrameVerdict;
    nodes: number;
    edges: number;
    /** Wann gemessen wurde, als ISO-Zeit. */
    at: string;
}

export interface SettingsPanelProps {
    /** Das Projekt, dessen Wahl gespeichert wird. Leer heisst: nichts wird gemerkt. */
    project: string;
    /** Die Lage des Sidecars, so wie die Karte in der Seitenleiste sie zeigt. */
    state: SidecarState;
    /** Die Zahlen des Modells, das gerade antwortet. */
    facts?: SidecarFacts | undefined;
    /** Ob der Sidecar als Router ueber ein Cache-Verzeichnis laeuft. */
    router: boolean;
    /** Was der Prozess in seinem Cache-Verzeichnis fuehrt. */
    models: readonly CacheModel[];
    /** Die id, an die die naechste Frage geht. Leer heisst: keine Wahl. */
    selectedModel: string;
    /** Eine andere id waehlen. */
    onSelectModel: (id: string) => void;
    /**
     * Die Modell-Liste noch einmal lesen.
     *
     * Fehlt, solange das lokale Modell aus ist, und dann steht der Knopf auch
     * nicht da: eine Flaeche, die nichts tut, gibt es in dieser Oberflaeche
     * nicht.
     */
    onRefresh?: (() => void) | undefined;
    /** Die Einstellungen der Darstellung. */
    display: GraphDisplaySettings;
    onDisplay: (next: GraphDisplaySettings) => void;
    /** Eine fertige Messung nach oben melden, damit die Naht sie traegt. */
    onMeasurement?: ((measurement: SettingsMeasurement) => void) | undefined;
    onClose: () => void;
}

/** Ein Abschnitt der Seite, mit seiner Ueberschrift und seinem Namen im DOM. */
function Section(props: { name: string; title: string; children: ReactNode }): JSX.Element {
    return (
        <section
            className="atlas-settings-section"
            data-testid="atlas-settings-section"
            data-section={props.name}
        >
            <h3 className="atlas-settings-section-title">{props.title}</h3>
            {props.children}
        </section>
    );
}

/** Eine Zahl des laufenden Modells. Der Tooltip sagt, aus welcher Anfrage sie kommt. */
function Fact(props: { name: string; label: string; value: string; source: string }): JSX.Element {
    return (
        <div className="atlas-settings-fact" data-testid="atlas-settings-fact" data-fact={props.name}>
            <dt className="atlas-settings-fact-label">{props.label}</dt>
            <Hint name={`settings-fact-${props.name}`} text={messages.llm.readFrom(props.source)}>
                <dd
                    className="atlas-settings-fact-value"
                    data-testid="atlas-settings-fact-value"
                    data-source={props.source}
                >
                    {props.value}
                </dd>
            </Hint>
        </div>
    );
}

/** Eine Zeile mit einer Auswahl aus wenigen Werten. */
function Choice<T extends string | number | boolean>(props: {
    kind: 'setting' | 'effect';
    name: string;
    label: string;
    detail: string;
    value: T;
    /*
     * Die Werte heissen `option` und nicht `value`, und das ist kein Geschmack:
     * `value` gilt dem Chrome-Scan (tools/lib/chrome-scan.mjs) als
     * beschriftende Eigenschaft, weil es an fast jeder anderen Stelle eine ist.
     * Hier ist es ein Schaltwert wie `flat` oder `dim`, den niemand liest; die
     * Beschriftung steht daneben und kommt aus dem Katalog.
     */
    options: readonly { option: T; label: string }[];
    onPick: (value: T) => void;
    children: ReactNode;
}): JSX.Element {
    return (
        <div
            className="atlas-settings-choice"
            data-testid={props.kind === 'effect' ? 'atlas-settings-effect' : 'atlas-settings-choice'}
            data-setting={props.name}
            data-effect={props.kind === 'effect' ? props.name : undefined}
            data-value={String(props.value)}
        >
            <div className="atlas-settings-choice-head">
                <span className="atlas-settings-choice-label">{props.label}</span>
                <span className="atlas-settings-choice-options" role="group" aria-label={props.label}>
                    {props.options.map((option) => (
                        <button
                            key={String(option.option)}
                            type="button"
                            className="atlas-settings-option"
                            data-testid="atlas-settings-option"
                            data-option={String(option.option)}
                            data-active={props.value === option.option}
                            aria-pressed={props.value === option.option}
                            onClick={() => props.onPick(option.option)}
                        >
                            {option.label}
                        </button>
                    ))}
                </span>
            </div>
            <p className="atlas-settings-choice-detail">{props.detail}</p>
            {props.children}
        </div>
    );
}

/** Eine Zahl der Bildrate, wie sie auf dem Schirm steht. */
function fps(value: number): string {
    return value >= 10 ? String(Math.round(value)) : value.toFixed(1);
}

/** Der Satz zu einer Messung, oder der Satz darueber, dass es keine gibt. */
function measurementText(measurement: SettingsMeasurement | undefined, busy: boolean): string {
    if (busy) {
        return text.measureRunning;
    }
    if (measurement === undefined) {
        return text.measureIdle;
    }
    const before = fps(measurement.before);
    const after = fps(measurement.after);
    const band = fps(measurement.band);
    switch (measurement.verdict) {
        case 'not-drawing':
            return text.measureNotDrawing;
        case 'no-difference':
            return text.measureNoDifference(before, after, band);
        case 'higher':
            return text.measureHigher(before, after, band);
        case 'lower':
            return text.measureLower(before, after, band);
        default:
            return text.measureIdle;
    }
}

export default function SettingsPanel(props: SettingsPanelProps): JSX.Element {
    const root = useRef<HTMLDivElement | null>(null);
    const [repoInput, setRepoInput] = useState('');
    const [copied, setCopied] = useState('');
    const [copyFailed, setCopyFailed] = useState('');
    /* Mehrere Zeilen koennen gleichzeitig auf die Zwischenablage warten.
     * Der Zaehler statt eines einzelnen Booleans verliert auch einen zweiten
     * Klick auf dieselbe Zeile nicht vor dem zweiten Abschluss. */
    const [copying, setCopying] = useState<Record<string, number>>({});
    const [measurements, setMeasurements] = useState<Record<string, SettingsMeasurement>>({});
    const [measuring, setMeasuring] = useState('');
    const measureTimer = useRef(0);

    // Der Fokus geht einmal in die Seite. Ohne das bliebe er dort, wo der Leser
    // gerade war, und Escape traefe die Flaeche, die er verlassen hat.
    useEffect(() => {
        root.current?.focus();
    }, []);

    /*
     * Die laufende Bildrate, zweimal angestossen.
     *
     * Einmal, wenn ein Fenster geschlossen wird (dann gibt es eine neue Zahl),
     * und einmal auf einer eigenen Uhr. Das zweite ist kein Ueberfluss: hoert
     * die Szene auf zu zeichnen, kommt kein Fenster mehr, und ohne die Uhr
     * bliebe die letzte Zahl fuer immer stehen und die Anzeige behauptete, der
     * Graph laufe.
     */
    const [, setTick] = useState(0);
    useEffect(() => subscribeFrameRate(() => setTick((value) => value + 1)), []);
    useEffect(() => {
        const timer = window.setInterval(() => setTick((value) => value + 1), FRAME_WINDOW_MS);
        return () => window.clearInterval(timer);
    }, []);
    useEffect(() => () => window.clearTimeout(measureTimer.current), []);

    const perf = frameRateSnapshot();

    /**
     * Eine Einstellung umlegen und dabei messen, was sie tut.
     *
     * Die Reihenfolge ist die Messung: erst das Fenster VOR der Aenderung und
     * das Rauschband daraus, dann die Aenderung, dann eine Beruhigungsphase,
     * dann das Fenster DANACH. Wer zuerst umschaltet und dann nach einem Vorher
     * sucht, misst zwei Zustaende, die es gleichzeitig nie gab.
     */
    const change = useCallback(
        (setting: string, next: GraphDisplaySettings) => {
            const startedAt = Date.now();
            const beforeValues = frameHistory()
                .filter((sample) => startedAt - sample.at <= BEFORE_MS)
                .map((sample) => sample.fps);
            const band = noiseBandOf(beforeValues);
            const scene = frameRateSnapshot(startedAt);

            props.onDisplay(next);

            const finish = (measurement: SettingsMeasurement): void => {
                setMeasurements((current) => ({ ...current, [setting]: measurement }));
                setMeasuring('');
                props.onMeasurement?.(measurement);
            };

            if (!scene.running) {
                // Nichts wird gezeichnet, also ist nichts zu messen. Das ist eine
                // Auskunft und keine Null.
                finish({
                    setting,
                    before: 0,
                    after: 0,
                    band: 0,
                    verdict: 'not-drawing',
                    nodes: scene.nodes,
                    edges: scene.edges,
                    at: new Date(startedAt).toISOString(),
                });
                return;
            }

            setMeasuring(setting);
            window.clearTimeout(measureTimer.current);
            measureTimer.current = window.setTimeout(() => {
                const afterValues = frameHistory()
                    .filter((sample) => sample.at >= startedAt + SETTLE_MS)
                    .map((sample) => sample.fps);
                const after = meanOf(afterValues);
                const before = meanOf(beforeValues);
                const now = Date.now();
                const closing = frameRateSnapshot(now);
                finish({
                    setting,
                    before: before ?? 0,
                    after: after ?? 0,
                    band: band ?? 0,
                    verdict: afterValues.length === 0
                        ? 'not-drawing'
                        : verdictOf(before, after, band),
                    nodes: closing.nodes,
                    edges: closing.edges,
                    at: new Date(now).toISOString(),
                });
            }, SETTLE_MS + MEASURE_MS);
        },
        [props],
    );

    /**
     * Kopieren, mit einem Weg fuer den Fall, dass die Zwischenablage nein sagt.
     *
     * Sie sagt in mehr Lagen nein, als man denkt: ohne sicheren Kontext, ohne
     * Fokus im Dokument, unter einer Berechtigungsregel. Der Befehl steht darum
     * ausserdem als `code` sichtbar da, und wenn beide Wege scheitern, sagt das
     * Panel es, statt einen stillen Knopf zu haben.
     */
    const copy = useCallback((id: string, line: string) => {
        setCopying((current) => ({ ...current, [id]: (current[id] ?? 0) + 1 }));
        const finishCopy = (): void => {
            setCopying((current) => {
                const count = current[id] ?? 0;
                if (count <= 1) {
                    const { [id]: _finished, ...rest } = current;
                    return rest;
                }
                return { ...current, [id]: count - 1 };
            });
        };
        const fallback = (): void => {
            try {
                const field = document.createElement('textarea');
                field.value = line;
                field.setAttribute('readonly', '');
                field.style.position = 'fixed';
                field.style.opacity = '0';
                document.body.appendChild(field);
                field.select();
                const done = document.execCommand('copy');
                document.body.removeChild(field);
                if (done) {
                    setCopied(id);
                    setCopyFailed('');
                    finishCopy();
                    return;
                }
            } catch {
                // faellt in die Meldung darunter
            }
            setCopied('');
            setCopyFailed(id);
            finishCopy();
        };
        try {
            const clipboard = navigator.clipboard;
            if (clipboard !== undefined && typeof clipboard.writeText === 'function') {
                void clipboard.writeText(line).then(
                    () => {
                        setCopied(id);
                        setCopyFailed('');
                        finishCopy();
                    },
                    fallback,
                );
                return;
            }
        } catch {
            // faellt in den zweiten Weg
        }
        fallback();
    }, []);

    const facts = props.facts;
    const llmOn = props.state !== 'off' && props.state !== 'disabled-by-policy';
    const canSwitch = props.router && props.models.length > 1;
    const repo = readRepoInput(repoInput);
    const freeCommand = repo.ok ? fetchCommand(repo.repo, repo.quant) : '';

    const measure = (setting: string): JSX.Element => {
        const found = measurements[setting];
        const busy = measuring === setting;
        return (
            <p
                className="atlas-settings-measure"
                data-testid="atlas-settings-measure"
                data-setting={setting}
                data-verdict={busy ? 'measuring' : found?.verdict ?? 'not-measured'}
                data-before={found === undefined ? '' : fps(found.before)}
                data-after={found === undefined ? '' : fps(found.after)}
                data-band={found === undefined ? '' : fps(found.band)}
                data-nodes={found?.nodes ?? ''}
                data-edges={found?.edges ?? ''}
                data-at={found?.at ?? ''}
            >
                {measurementText(found, busy)}
                {found !== undefined && found.verdict !== 'not-drawing' && (
                    <span className="atlas-settings-measure-scene">
                        {text.measureScene(found.nodes, found.edges, found.at)}
                    </span>
                )}
            </p>
        );
    };

    return (
        <div
            className="atlas-settings"
            data-testid="atlas-settings"
            role="dialog"
            aria-label={text.panelLabel}
            aria-modal="false"
            tabIndex={-1}
            ref={root}
            data-llm={props.state}
            data-router={props.router}
            onKeyDown={(event) => {
                if (event.key === 'Escape') {
                    event.preventDefault();
                    props.onClose();
                }
            }}
        >
            <header className="atlas-settings-head">
                <span className="atlas-settings-title" data-testid="atlas-settings-title">
                    {text.title}
                </span>
                <span className="atlas-settings-subtitle">{text.subtitle}</span>
                <button
                    type="button"
                    className="atlas-settings-close"
                    data-testid="atlas-settings-close"
                    aria-label={text.closeLabel}
                    onClick={props.onClose}
                >
                    {text.close}
                </button>
            </header>

            {/* ---------------------------------------- das laufende Modell */}
            <Section name="model-running" title={text.modelTitle}>
                {!llmOn && (
                    <div className="atlas-settings-note" data-testid="atlas-settings-llm-off">
                        <p className="atlas-settings-text">
                            <b>{props.state === 'off' ? text.offTitle : text.blockedText}</b>
                        </p>
                        <p className="atlas-settings-text">{text.offText}</p>
                        <p className="atlas-settings-text">{text.offStillWorks}</p>
                    </div>
                )}
                {props.state === 'not-running' && (
                    <div className="atlas-settings-note" data-testid="atlas-settings-not-running">
                        <p className="atlas-settings-text">
                            <b>{text.notRunningTitle}</b>
                        </p>
                        <p className="atlas-settings-text">{text.notRunningText}</p>
                        <p className="atlas-settings-command">
                            <code data-testid="atlas-settings-start-command">{LLM_START_COMMAND}</code>
                        </p>
                    </div>
                )}
                {props.state === 'starting' && (
                    <p className="atlas-settings-text" data-testid="atlas-settings-starting">
                        {text.startingText}
                    </p>
                )}
                {props.state === 'ready' && facts !== undefined && (
                    <>
                        <p className="atlas-settings-text">{text.modelIntro}</p>
                        <dl className="atlas-settings-facts" data-testid="atlas-settings-running">
                            <Fact
                                name="name"
                                label={text.rowName}
                                value={facts.modelPath.length > 0 ? facts.modelPath : facts.model}
                                source={props.router ? text.sourceNameRouter : text.sourceName}
                            />
                            <Fact
                                name="quantization"
                                label={text.rowQuantization}
                                value={
                                    facts.quantization.length > 0
                                        ? facts.quantization
                                        : text.valueUnreported
                                }
                                source={
                                    props.router
                                        ? text.sourceQuantizationRouter
                                        : text.sourceQuantization
                                }
                            />
                            <Fact
                                name="context"
                                label={text.rowContext}
                                value={
                                    facts.contextTokens === undefined
                                        ? text.valueUnreported
                                        : text.contextValue(
                                            facts.contextTokens,
                                            facts.trainedContextTokens,
                                        )
                                }
                                source={props.router ? text.sourceContextRouter : text.sourceContext}
                            />
                            <Fact
                                name="weights"
                                label={text.rowWeights}
                                value={
                                    facts.weightsBytes === undefined
                                        ? text.valueUnreported
                                        : text.weightsValue(
                                            humanBytes(facts.weightsBytes),
                                            facts.parameters === undefined
                                                ? undefined
                                                : (facts.parameters / 1e9).toFixed(2),
                                        )
                                }
                                source={text.sourceWeights}
                            />
                        </dl>
                    </>
                )}
            </Section>

            {/* ---------------------------------------- die Modelle im Cache */}
            <Section name="model-cache" title={text.cacheTitle}>
                {llmOn && !props.router && props.state === 'ready' && (
                    <div className="atlas-settings-note" data-testid="atlas-settings-no-router">
                        <p className="atlas-settings-text">
                            <b>{text.noRouterTitle}</b>
                        </p>
                        <p className="atlas-settings-text">{text.noRouterText}</p>
                        <p className="atlas-settings-text">{text.noRouterHow}</p>
                        <p className="atlas-settings-command">
                            <code data-testid="atlas-settings-router-command">
                                {ROUTER_START_COMMAND}
                            </code>
                        </p>
                    </div>
                )}
                {llmOn && props.state === 'ready' && (
                    <>
                        <p className="atlas-settings-text">{text.cacheIntro}</p>
                        <p
                            className="atlas-settings-count"
                            data-testid="atlas-settings-cache-count"
                            data-count={props.models.length}
                        >
                            {props.models.length === 0
                                ? text.cacheEmpty
                                : text.cacheCount(props.models.length)}
                        </p>
                        <ul className="atlas-settings-models" data-testid="atlas-settings-models">
                            {props.models.map((model) => {
                                const active = model.id === props.selectedModel
                                    || (props.selectedModel.length === 0
                                        && model.name === facts?.model);
                                const state = active
                                    ? text.modelAnswering
                                    : model.loaded ? text.modelLoaded : text.modelUnloaded;
                                const row = (
                                    <li
                                        className="atlas-settings-model"
                                        key={model.id}
                                        data-testid="atlas-settings-model"
                                        data-model={model.id}
                                        data-active={active}
                                        data-loaded={model.loaded}
                                        data-selectable={canSwitch}
                                    >
                                        {canSwitch ? (
                                            <Hint
                                                name={`settings-model-${model.id}`}
                                                text={
                                                    active
                                                        ? text.selectedTitle(model.name)
                                                        : text.selectTitle(model.name)
                                                }
                                            >
                                                <button
                                                    type="button"
                                                    className="atlas-settings-model-pick"
                                                    data-testid="atlas-settings-model-pick"
                                                    data-model={model.id}
                                                    aria-pressed={active}
                                                    onClick={() => props.onSelectModel(model.id)}
                                                >
                                                    <span className="atlas-settings-model-id">
                                                        {model.id}
                                                    </span>
                                                    <span className="atlas-settings-model-state">
                                                        {state}
                                                    </span>
                                                </button>
                                            </Hint>
                                        ) : (
                                            <span className="atlas-settings-model-pick">
                                                <span className="atlas-settings-model-id">
                                                    {model.id}
                                                </span>
                                                <span className="atlas-settings-model-state">
                                                    {state}
                                                </span>
                                            </span>
                                        )}
                                    </li>
                                );
                                return row;
                            })}
                        </ul>
                        {props.onRefresh !== undefined && (
                            <Hint name="settings-refresh" text={text.refreshTitle}>
                                <button
                                    type="button"
                                    className="atlas-settings-refresh"
                                    data-testid="atlas-settings-refresh"
                                    onClick={props.onRefresh}
                                >
                                    {text.refresh}
                                </button>
                            </Hint>
                        )}
                    </>
                )}
            </Section>

            {/* ---------------------------------------- eins dazuholen */}
            <Section name="model-fetch" title={text.fetchTitle}>
                <p className="atlas-settings-honesty" data-testid="atlas-settings-honesty">
                    {text.honesty}
                </p>
                <p className="atlas-settings-text" data-testid="atlas-settings-no-progress">
                    {text.noProgressBar}
                </p>
                <h4 className="atlas-settings-subtitle-inline">{text.suggestionsTitle}</h4>
                <p className="atlas-settings-text">{text.suggestionsIntro}</p>
                <ul className="atlas-settings-suggestions" data-testid="atlas-settings-suggestions">
                    {MODEL_SUGGESTIONS.map((suggestion) => {
                        const command = fetchCommand(suggestion.repo, suggestion.quant);
                        return (
                            <li
                                className="atlas-settings-suggestion"
                                key={suggestion.id}
                                data-testid="atlas-settings-suggestion"
                                data-suggestion={suggestion.id}
                                data-repo={suggestion.repo}
                                data-class={suggestion.modelClass}
                                data-pass-rate={suggestion.passRate}
                                data-citation={suggestion.citationCompliance}
                                data-citation-unmeasured={suggestion.citationUnmeasured ?? ''}
                                data-tokens-per-second={suggestion.tokensPerSecond}
                                data-bytes={suggestion.bytes}
                            >
                                <div className="atlas-settings-suggestion-head">
                                    <span className="atlas-settings-suggestion-name">
                                        {suggestion.name}
                                    </span>
                                    <Hint
                                        name={`settings-repo-${suggestion.id}`}
                                        text={text.repoTitle(suggestion.repo)}
                                    >
                                        <span className="atlas-settings-suggestion-repo">
                                            {suggestion.repo}
                                        </span>
                                    </Hint>
                                </div>
                                <dl className="atlas-settings-numbers">
                                    <div className="atlas-settings-number" data-number="class">
                                        <dt>{text.columnClass}</dt>
                                        <dd>{text.classValue(suggestion.modelClass)}</dd>
                                    </div>
                                    <div className="atlas-settings-number" data-number="pass">
                                        <dt>{text.columnPass}</dt>
                                        <dd data-testid="atlas-settings-pass-rate">
                                            {text.passValue(percentText(suggestion.passRate), 44)}
                                        </dd>
                                    </div>
                                    <div className="atlas-settings-number" data-number="citation">
                                        <dt>{text.columnCitation}</dt>
                                        <dd data-testid="atlas-settings-citation">
                                            {text.citationValue(
                                                percentText(suggestion.citationCompliance),
                                            )}
                                            <span
                                                className="atlas-settings-unmeasured"
                                                data-testid="atlas-settings-unmeasured"
                                                data-unmeasured={suggestion.citationUnmeasured ?? ''}
                                            >
                                                {suggestion.citationUnmeasured === undefined
                                                    ? text.unmeasuredMissing
                                                    : text.unmeasuredValue(
                                                        suggestion.citationUnmeasured,
                                                    )}
                                            </span>
                                        </dd>
                                    </div>
                                    <div className="atlas-settings-number" data-number="speed">
                                        <dt>{text.columnSpeed}</dt>
                                        <dd>{text.speedValue(speedText(suggestion.tokensPerSecond))}</dd>
                                    </div>
                                    <div className="atlas-settings-number" data-number="size">
                                        <dt>{text.columnSize}</dt>
                                        <dd>{humanBytes(suggestion.bytes)}</dd>
                                    </div>
                                </dl>
                                <div className="atlas-settings-command-row">
                                    <code
                                        className="atlas-settings-command-text"
                                        data-testid="atlas-settings-command"
                                        data-command={command}
                                    >
                                        {command}
                                    </code>
                                    <Hint name={`settings-copy-${suggestion.id}`} text={text.copyTitle}>
                                        <button
                                            type="button"
                                            className="atlas-settings-copy"
                                            data-testid="atlas-settings-copy"
                                            data-copied={copied === suggestion.id}
                                            data-copying={(copying[suggestion.id] ?? 0) > 0}
                                            onClick={() => copy(suggestion.id, command)}
                                        >
                                            {(copying[suggestion.id] ?? 0) > 0
                                                ? text.copying
                                                : copied === suggestion.id ? text.copied : text.copy}
                                        </button>
                                    </Hint>
                                </div>
                                {copyFailed === suggestion.id && (
                                    <p className="atlas-settings-text" data-testid="atlas-settings-copy-failed">
                                        {text.copyFailed}
                                    </p>
                                )}
                            </li>
                        );
                    })}
                </ul>

                <h4 className="atlas-settings-subtitle-inline">{text.freeTitle}</h4>
                <p className="atlas-settings-text">{text.freeIntro}</p>
                <div className="atlas-settings-free">
                    <input
                        className="atlas-settings-free-input"
                        data-testid="atlas-settings-repo-input"
                        aria-label={text.freeLabel}
                        placeholder={text.freePlaceholder}
                        value={repoInput}
                        autoComplete="off"
                        onChange={(event) => setRepoInput(event.target.value)}
                    />
                    <p
                        className="atlas-settings-free-state"
                        data-testid="atlas-settings-repo-state"
                        data-valid={repo.ok}
                        data-problem={repo.problem}
                    >
                        {repo.ok
                            ? text.freeOk
                            : repo.problem === 'empty' ? text.freeEmpty : text.freeShape}
                    </p>
                    {repo.ok && (
                        <div className="atlas-settings-command-row">
                            <code
                                className="atlas-settings-command-text"
                                data-testid="atlas-settings-repo-command"
                                data-command={freeCommand}
                            >
                                {freeCommand}
                            </code>
                            <Hint name="settings-copy-free" text={text.copyTitle}>
                                <button
                                    type="button"
                                    className="atlas-settings-copy"
                                    data-testid="atlas-settings-copy"
                                    data-copied={copied === 'free'}
                                    data-copying={(copying.free ?? 0) > 0}
                                    onClick={() => copy('free', freeCommand)}
                                >
                                    {(copying.free ?? 0) > 0
                                        ? text.copying
                                        : copied === 'free' ? text.copied : text.copy}
                                </button>
                            </Hint>
                        </div>
                    )}
                </div>
            </Section>

            {/* ---------------------------------------- Darstellung und Leistung */}
            <Section name="display" title={text.displayTitle}>
                <p className="atlas-settings-text">{text.displayIntro}</p>
                <p
                    className="atlas-settings-live"
                    data-testid="atlas-settings-perf"
                    data-running={perf.running}
                    data-fps={fps(perf.fps)}
                    data-nodes={perf.nodes}
                    data-edges={perf.edges}
                    data-cap={perf.cap}
                    data-band={fps(perf.noiseBand)}
                    data-samples={perf.samples}
                >
                    {perf.running ? text.liveValue(fps(perf.fps)) : text.liveIdle}
                    {perf.running && (
                        <span className="atlas-settings-live-scene">
                            {text.liveScene(perf.nodes, perf.edges)}
                        </span>
                    )}
                </p>

                <Choice<GraphProjection>
                    kind="setting"
                    name="projection"
                    label={text.settingProjection}
                    detail={text.settingProjectionDetail}
                    value={props.display.projection}
                    options={[
                        { option: 'spatial' as GraphProjection, label: text.valueSpatial },
                        { option: 'flat' as GraphProjection, label: text.valueFlat },
                    ]}
                    onPick={(value) => change('projection', { ...props.display, projection: value })}
                >
                    {measure('projection')}
                </Choice>

                {/*
                  * Ein Ja/Nein-Effekt als `Choice<boolean>`: der Schaltwert IST
                  * der Zustand, und "on"/"off" waeren zwei Zeichenketten, die
                  * dasselbe noch einmal sagen. Die zwei Beschriftungen stehen im
                  * Katalog, wie jeder andere sichtbare Satz.
                  */}
                <Choice<boolean>
                    kind="effect"
                    name="halos"
                    label={text.settingHalos}
                    detail={text.settingHalosDetail}
                    value={props.display.halos}
                    options={[
                        { option: true, label: text.switchOn },
                        { option: false, label: text.switchOff },
                    ]}
                    onPick={(value) => change('halos', { ...props.display, halos: value })}
                >
                    {measure('halos')}
                </Choice>

                {/*
                  * Ein Ja/Nein-Effekt als `Choice<boolean>`: der Schaltwert IST
                  * der Zustand, und "on"/"off" waeren zwei Zeichenketten, die
                  * dasselbe noch einmal sagen. Die zwei Beschriftungen stehen im
                  * Katalog, wie jeder andere sichtbare Satz.
                  */}
                <Choice<boolean>
                    kind="effect"
                    name="bloom"
                    label={text.settingBloom}
                    detail={text.settingBloomDetail}
                    value={props.display.bloom}
                    options={[
                        { option: true, label: text.switchOn },
                        { option: false, label: text.switchOff },
                    ]}
                    onPick={(value) => change('bloom', { ...props.display, bloom: value })}
                >
                    {measure('bloom')}
                </Choice>

                <Choice<EdgeDensity>
                    kind="effect"
                    name="edges"
                    label={text.settingEdges}
                    detail={text.settingEdgesDetail}
                    value={props.display.edges}
                    options={[
                        { option: 'full' as EdgeDensity, label: text.valueEdgesFull },
                        { option: 'dim' as EdgeDensity, label: text.valueEdgesDim },
                        { option: 'off' as EdgeDensity, label: text.valueEdgesOff },
                    ]}
                    onPick={(value) => change('edges', { ...props.display, edges: value })}
                >
                    {measure('edges')}
                </Choice>

                <Choice<number>
                    kind="effect"
                    name="labels"
                    label={text.settingLabels}
                    detail={text.settingLabelsDetail}
                    value={props.display.labelDistanceFactor}
                    options={LABEL_DISTANCE_FACTORS.map((factor) => ({
                        option: factor,
                        label: factor === 0
                            ? text.valueLabelsAll
                            : factor >= 2 ? text.valueLabelsFar : text.valueLabelsNear,
                    }))}
                    onPick={(value) =>
                        change('labels', { ...props.display, labelDistanceFactor: value })}
                >
                    {measure('labels')}
                </Choice>

                <Choice<number>
                    kind="setting"
                    name="frameCap"
                    label={text.settingFrameCap}
                    detail={text.settingFrameCapDetail}
                    value={props.display.frameCap}
                    options={FRAME_CAPS.map((cap) => ({
                        option: cap,
                        label: cap === 0 ? text.valueCapOff : text.valueCap(cap),
                    }))}
                    onPick={(value) => change('frameCap', { ...props.display, frameCap: value })}
                >
                    {measure('frameCap')}
                </Choice>

                {/*
                  * Die Agentenebene (W11a), und sie steht HIER.
                  *
                  * Der Nutzerwunsch vom 2026-08-29 gilt fuer sie wie fuer die
                  * anderen: was Rechenzeit kostet, steht an EINEM Ort. Ein
                  * zweiter Schalter am Graphen waere der zweite Ort, und die
                  * Frage "wo schalte ich das ab" haette wieder zwei Antworten.
                  * Im Instrument selbst bleibt nur, was den laufenden Blick
                  * steuert (follow, trails, cinema).
                  */}
                <Choice<boolean>
                    kind="effect"
                    name="agents"
                    label={text.settingAgents}
                    detail={text.settingAgentsDetail}
                    value={props.display.agents}
                    options={[
                        { option: true, label: text.switchOn },
                        { option: false, label: text.switchOff },
                    ]}
                    onPick={(value) => change('agents', { ...props.display, agents: value })}
                >
                    {measure('agents')}
                </Choice>

                {/*
                  * Die vier Wirkungen der Bewegung (W11b AC7b).
                  *
                  * Sie stehen einzeln da und nicht als ein Schalter "Bewegung",
                  * weil sie verschieden viel kosten und verschieden viel sagen:
                  * die Spur ist die Auskunft, an der man den Arbeitsweg
                  * ablesen kann, der Schweif ist die billigste Zutat, der
                  * Zeitstrahl ist die teuerste Flaeche. Wer eine schwache
                  * Maschine hat, soll die teure abschalten koennen und die
                  * Auskunft behalten. Was jede auf DIESER Maschine kostet, misst
                  * das Panel selbst und schreibt es daneben.
                  */}
                <Choice<boolean>
                    kind="effect"
                    name="agentTails"
                    label={text.settingAgentTails}
                    detail={text.settingAgentTailsDetail}
                    value={props.display.agentTails}
                    options={[
                        { option: true, label: text.switchOn },
                        { option: false, label: text.switchOff },
                    ]}
                    onPick={(value) => change('agentTails', { ...props.display, agentTails: value })}
                >
                    {measure('agentTails')}
                </Choice>

                <Choice<boolean>
                    kind="effect"
                    name="agentTrails"
                    label={text.settingAgentTrails}
                    detail={text.settingAgentTrailsDetail}
                    value={props.display.agentTrails}
                    options={[
                        { option: true, label: text.switchOn },
                        { option: false, label: text.switchOff },
                    ]}
                    onPick={(value) => change('agentTrails', { ...props.display, agentTrails: value })}
                >
                    {measure('agentTrails')}
                </Choice>

                <Choice<boolean>
                    kind="effect"
                    name="agentWaves"
                    label={text.settingAgentWaves}
                    detail={text.settingAgentWavesDetail}
                    value={props.display.agentWaves}
                    options={[
                        { option: true, label: text.switchOn },
                        { option: false, label: text.switchOff },
                    ]}
                    onPick={(value) => change('agentWaves', { ...props.display, agentWaves: value })}
                >
                    {measure('agentWaves')}
                </Choice>

                <Choice<boolean>
                    kind="effect"
                    name="agentTimeline"
                    label={text.settingAgentTimeline}
                    detail={text.settingAgentTimelineDetail}
                    value={props.display.agentTimeline}
                    options={[
                        { option: true, label: text.switchOn },
                        { option: false, label: text.switchOff },
                    ]}
                    onPick={(value) =>
                        change('agentTimeline', { ...props.display, agentTimeline: value })}
                >
                    {measure('agentTimeline')}
                </Choice>

                <div
                    className="atlas-settings-profiles"
                    data-testid="atlas-settings-profiles"
                    data-default={isDefaultDisplay(props.display)}
                >
                    <span className="atlas-settings-choice-label">{text.profileTitle}</span>
                    <Hint name="settings-profile-thrifty" text={text.profileThriftyDetail}>
                        <button
                            type="button"
                            className="atlas-settings-profile"
                            data-testid="atlas-settings-profile"
                            data-profile="thrifty"
                            onClick={() => change('thrifty', { ...THRIFTY_GRAPH_DISPLAY })}
                        >
                            {text.profileThrifty}
                        </button>
                    </Hint>
                    <Hint name="settings-profile-default" text={text.profileDefaultDetail}>
                        <button
                            type="button"
                            className="atlas-settings-profile"
                            data-testid="atlas-settings-profile"
                            data-profile="default"
                            onClick={() => change('default', { ...DEFAULT_GRAPH_DISPLAY })}
                        >
                            {text.profileDefault}
                        </button>
                    </Hint>
                    <span className="atlas-settings-profile-state">
                        {isDefaultDisplay(props.display) ? text.profileIsDefault : text.profileChanged}
                    </span>
                    {measure('thrifty')}
                    {measure('default')}
                </div>

                <p className="atlas-settings-note-line" data-testid="atlas-settings-storage">
                    {text.displayStored(displayKey(props.project))}
                </p>
                <p className="atlas-settings-note-line" data-testid="atlas-settings-model-storage">
                    {text.displayStored(modelKey(props.project))}
                </p>

                <h4 className="atlas-settings-subtitle-inline">{text.keepsTitle}</h4>
                <p className="atlas-settings-text" data-testid="atlas-settings-keeps">
                    {text.keepsText}
                </p>
            </Section>
        </div>
    );
}
