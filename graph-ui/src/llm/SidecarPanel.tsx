/**
 * Das Panel des lokalen Modells: Lage, Modell, Zahlen, und im Zustand ohne
 * Prozess die Anleitung mit dem Aufruf, der ihn startet.
 *
 * Rein darstellend, wie WhyPanel und TwinPanel: was gefragt wird, entscheidet
 * App.tsx, was hier steht, ist eine Lesung. Die eine Regel, die diese Datei
 * durchhaelt: jede Zahl auf dem Bildschirm kommt aus dem laufenden Prozess und
 * traegt ihre Herkunft im `title`. Eine Karte, die Modellgroessen aus einer
 * Tabelle im Quelltext zeigte, waere im ersten Monat richtig und danach eine
 * Behauptung ueber eine Datei, die jemand ausgetauscht hat.
 *
 * Der Schalter bleibt auch dann sichtbar und bedienbar aussehend, wenn die
 * Policy sperrt: er ist dann `disabled` und sagt im `title`, warum. Ihn
 * wegzunehmen waere die Behauptung, es gebe diese Einstellung nicht, und genau
 * das soll ein Leser unterscheiden koennen.
 */

import type { JSX } from 'react';
import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import type { SidecarFacts, SidecarState } from './sidecar';
import { humanBytes, SIDECAR_PORT } from './sidecar';
import {
    LLM_NOT_RUNNING_HINT,
    LLM_NOT_RUNNING_MESSAGE,
    LLM_OFF_HINT,
    LLM_OFF_MESSAGE,
    LLM_READY_MESSAGE,
    LLM_STARTING_MESSAGE,
    LLM_START_COMMAND,
    LLM_TITLE,
    llmPolicyDetail,
    llmPolicyMessage,
} from './strings';

export interface SidecarPanelProps {
    state: SidecarState;
    /** Was der Prozess ueber sich gesagt hat. Fehlt, solange keiner antwortet. */
    facts?: SidecarFacts | undefined;
    /** Das Projekt, damit der Policy-Satz es nennen kann. */
    project: string;
    /** Der Grund einer Policy-Sperre, wenn sie einen hat. */
    policyDetail: string;
    /** Was bei der letzten Probe schiefging. Leer im Normalfall. */
    detail: string;
    onToggle: () => void;
}

/** Eine Zeile der Faktenliste. Der `title` sagt, woher die Zahl kommt. */
function Row({ label, value, source }: { label: string; value: string; source: string }): JSX.Element {
    return (
        <div className="atlas-llm-row" data-testid="atlas-llm-row" data-row={label}>
            <dt className="atlas-llm-row-label">{label}</dt>
            <Hint name="llm-row" text={messages.llm.readFrom(source)}>
                <dd className="atlas-llm-row-value">{value}</dd>
            </Hint>
        </div>
    );
}

function messageFor(props: SidecarPanelProps): string {
    switch (props.state) {
        case 'off':
            return LLM_OFF_MESSAGE;
        case 'disabled-by-policy':
            return llmPolicyMessage(props.project);
        case 'not-running':
            return LLM_NOT_RUNNING_MESSAGE;
        case 'starting':
            return LLM_STARTING_MESSAGE;
        case 'ready':
            return LLM_READY_MESSAGE;
        default:
            return LLM_OFF_MESSAGE;
    }
}

function hintFor(props: SidecarPanelProps): string {
    switch (props.state) {
        case 'off':
            return LLM_OFF_HINT;
        case 'disabled-by-policy':
            return llmPolicyDetail(props.policyDetail);
        case 'not-running':
            return LLM_NOT_RUNNING_HINT;
        default:
            return props.detail;
    }
}

export default function SidecarPanel(props: SidecarPanelProps): JSX.Element {
    const facts = props.facts;
    const blocked = props.state === 'disabled-by-policy';
    const on = props.state !== 'off' && !blocked;
    const message = messageFor(props);
    const hint = hintFor(props);

    return (
        <section
            className="atlas-llm"
            data-testid="atlas-llm"
            data-state={props.state}
            aria-label={messages.llm.panelLabel}
        >
            <div className="atlas-llm-head">
                <span className="atlas-llm-title">{LLM_TITLE}</span>
                <span className="atlas-llm-state" data-testid="atlas-llm-state" data-state={props.state}>
                    {props.state}
                </span>
                <Hint
                    name="llm-toggle"
                    text={
                        blocked
                            ? messages.llm.toggleBlocked
                            : on
                                ? messages.llm.toggleOff
                                : messages.llm.toggleOn
                    }
                >
                    <button
                        type="button"
                        className="atlas-llm-toggle"
                        data-testid="atlas-llm-toggle"
                        data-on={on}
                        aria-pressed={on}
                        disabled={blocked}
                        onClick={props.onToggle}
                    >
                        {on ? messages.llm.on : messages.llm.off}
                    </button>
                </Hint>
            </div>

            <p className="atlas-llm-message" data-testid="atlas-llm-message" data-state={props.state}>
                {message}
                {props.state === 'ready' && facts !== undefined && (
                    <> <b data-testid="atlas-llm-model">{facts.model}</b></>
                )}
            </p>

            {props.state === 'not-running' && (
                <p className="atlas-llm-command" data-testid="atlas-llm-command">
                    <code>{LLM_START_COMMAND}</code>
                </p>
            )}

            {hint.length > 0 && (
                <p className="atlas-llm-hint" data-testid="atlas-llm-hint">
                    {hint}
                </p>
            )}

            {props.state === 'ready' && facts !== undefined && (
                <dl className="atlas-llm-rows" data-testid="atlas-llm-rows">
                    {/*
                      * Datei und Quantisierung in einer Zeile. Zwei Zeilen
                      * waeren ein Viertel der Hoehe dieser Karte fuer eine
                      * Angabe, die ohne die andere nichts heisst, und die
                      * Hoehe nimmt sie dem Flow-Kasten daneben weg.
                      */}
                    <Row
                        label={messages.llm.rowModel}
                        value={
                            facts.quantization.length > 0
                                ? `${facts.modelPath} (${facts.quantization})`
                                : facts.modelPath
                        }
                        source={messages.llm.sourceModel}
                    />
                    {facts.contextTokens !== undefined && (
                        <Row
                            label={messages.llm.rowContext}
                            value={messages.llm.contextValue(
                                facts.contextTokens,
                                facts.trainedContextTokens,
                            )}
                            source={messages.llm.sourceContext}
                        />
                    )}
                    {facts.weightsBytes !== undefined && (
                        <Row
                            label={messages.llm.rowWeights}
                            value={messages.llm.weightsValue(
                                humanBytes(facts.weightsBytes),
                                facts.parameters === undefined
                                    ? undefined
                                    : (facts.parameters / 1e9).toFixed(2),
                            )}
                            source={messages.llm.sourceWeights}
                        />
                    )}
                    {facts.slots !== undefined && (
                        <Row
                            label={messages.llm.rowSlots}
                            value={messages.llm.slotsValue(facts.slots, SIDECAR_PORT)}
                            source={messages.llm.sourceSlots}
                        />
                    )}
                </dl>
            )}
        </section>
    );
}
