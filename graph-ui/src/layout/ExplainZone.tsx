/**
 * Der Erklaeren-Bereich: EIN Platz unter dem Reader, in dem alles liegt, was den
 * gelesenen Code erklaert, und immer genau eins davon zu sehen ist.
 *
 * ## Der Befund, gegen den dieser Bereich geschnitten ist
 *
 * Nutzerbefund vom 2026-08-29 mit Screenshot: Flow-Erklaerer, Antwort-Panel und
 * Schrittkarte standen gleichzeitig offen, ueberlagerten sich, und alle drei
 * waren angeschnitten. Der Massstab, den der Nutzer dazu genannt hat, ist der
 * ganze Entwurf dieses Bereichs: "es soll intuitiv sein wie Apple und nicht
 * verwirren; die Oberflaeche ist hauptsaechlich dafuer da, dass Devs Code besser
 * verstehen".
 *
 * Daraus folgen drei Entscheidungen, und keine davon ist Geschmack:
 *
 * 1. **Was einander ersetzt, teilt sich einen Platz.** Flow, Walk, Chat und die
 *    beiden Assistenten beantworten alle dieselbe Frage ("was tut der Code, den
 *    ich lese") auf verschiedene Weise. Zwei Antworten auf dieselbe Frage
 *    gleichzeitig sind keine zwei Antworten, sondern eine halbe: der Leser muss
 *    erst entscheiden, welche der beiden er meint. Also gilt immer genau eine,
 *    und der Wechsel ist ein Reiter.
 * 2. **Der Wechsel kostet nichts.** Der Zustand jeder Flaeche liegt oben in
 *    App.tsx und nicht in ihr. Ein Reiterwechsel haengt darum nur ihre
 *    Darstellung aus und nicht ihren Verlauf: der Chat behaelt seine Fragen, der
 *    Walk seinen Schritt, der Flow seine Stelle. Das ist der Grund, aus dem hier
 *    NUR der aktive Reiter im DOM steht: haetten alle fuenf ihren eigenen
 *    Zustand, muessten sie alle haengenbleiben, und dann waeren wieder fuenf
 *    Flaechen an einem Platz.
 * 3. **Ruhe ist der Normalfall.** Beim Oeffnen eines Projekts ist der Bereich
 *    eingeklappt. Er ist dann ein Streifen mit seinen Reitern und einer Zeile,
 *    die sagt, was drinliegt, und keine Leere: ein Bereich, der ganz
 *    verschwaende, waere eine Funktion, die man erst wiederfinden muss.
 *
 * ## Warum der eingeklappte Streifen seine Reiter behaelt
 *
 * Weil er sonst ein Knopf waere, hinter dem etwas Unbekanntes liegt. Mit den
 * Reitern ist er ein Inhaltsverzeichnis: der Leser sieht, dass es einen Chat
 * gibt, dass eine Fuehrung laeuft, und ein Klick oeffnet genau das. Das ist auch
 * die Antwort auf die Frage, die W7c fuer den Chat schon beantworten musste
 * ("ist mein Verlauf noch da?"): die Zeile daneben sagt es.
 */

import type { JSX, ReactNode } from 'react';

import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import { explainTabOf } from './explain-tabs';
import type { ExplainTabId, ExplainTabState } from './explain-tabs';

export interface ExplainZoneProps {
    tabs: readonly ExplainTabState[];
    active: ExplainTabId;
    onSelect: (id: ExplainTabId) => void;
    open: boolean;
    onToggle: () => void;
    /** Die Hoehe des offenen Bereichs. Sie gehoert der Zone und nicht dem Inhalt. */
    height: number;
    /**
     * Der Inhalt des aktiven Reiters, von aussen.
     *
     * Wie der Reader und die rechte Spalte im Chrome: dieser Bereich soll die
     * fuenf Flaechen so wenig kennen wie das Chrome den Editor. Fehlt der Inhalt,
     * steht der Grund des Reiters da, und das ist kein Sonderfall, sondern der
     * Normalfall eines deaktivierten Reiters.
     */
    children?: ReactNode;
}

export default function ExplainZone(props: ExplainZoneProps): JSX.Element {
    const active = explainTabOf(props.tabs, props.active);
    const shown = props.open && props.children !== undefined && active?.enabled === true;

    return (
        <section
            className="atlas-explain"
            data-testid="atlas-explain"
            data-open={props.open}
            data-tab={props.active}
            aria-label={messages.layout.explainLabel}
            style={props.open ? { height: `${props.height}px` } : undefined}
        >
            <div
                className="atlas-explain-tabs"
                data-hint-keep="explain tabs"
                data-testid="atlas-explain-tabs"
                role="tablist"
                aria-label={messages.layout.explainTabsLabel}
            >
                {props.tabs.map((tab) => (
                    <Hint key={tab.id} name={`explain-tab-${tab.id}`} text={tab.title}>
                        <button
                            type="button"
                            className="atlas-explain-tab"
                            data-testid="atlas-explain-tab"
                            data-tab={tab.id}
                            data-enabled={tab.enabled}
                            data-on={tab.id === props.active}
                            role="tab"
                            aria-selected={tab.id === props.active}
                            onClick={() => props.onSelect(tab.id)}
                        >
                            {tab.label}
                        </button>
                    </Hint>
                ))}
                {/*
                  * Die Zeile des eingeklappten Streifens.
                  *
                  * Nur dann, denn offen steht der Inhalt selbst da, und eine
                  * Zusammenfassung ueber dem, was sie zusammenfasst, ist eine
                  * Zeile, die zweimal dasselbe sagt.
                  */}
                {!props.open && (
                    <span className="atlas-explain-note" data-testid="atlas-explain-note">
                        {active?.note ?? ''}
                    </span>
                )}
                <Hint
                    name="explain-fold"
                    text={props.open
                        ? messages.layout.collapseTooltip
                        : messages.layout.expandTooltip}
                >
                    <button
                        type="button"
                        className="atlas-explain-fold"
                        data-testid="atlas-explain-collapse"
                        data-open={props.open}
                        aria-expanded={props.open}
                        data-fold={props.open ? 'collapse' : 'open'}
                        data-fold-of="explain"
                        onClick={props.onToggle}
                    >
                        {props.open ? messages.layout.collapse : messages.layout.expand}
                    </button>
                </Hint>
            </div>

            {props.open && (
                <div
                    className="atlas-explain-body"
                    data-testid="atlas-explain-panel"
                    data-tab={props.active}
                    data-state={shown ? 'ready' : 'empty'}
                >
                    {shown ? props.children : (
                        <p
                            className="atlas-explain-empty"
                            data-testid="atlas-explain-empty"
                            data-tab={props.active}
                        >
                            {active?.reason ?? messages.layout.explainEmptyLabel}
                        </p>
                    )}
                </div>
            )}
        </section>
    );
}
