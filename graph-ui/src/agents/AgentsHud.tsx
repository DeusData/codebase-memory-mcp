/**
 * Das Instrument unten rechts: es erklaert, was auf dem Graphen zu sehen ist.
 *
 * Es ist ausdruecklich KEIN Dashboard. Was es zeigt, sind die Koerper, die
 * daneben kreisen, in Worten: wer, welche Art von Arbeit, an welchem Ort, seit
 * wann, und wie dicht die Ereignisse in den letzten dreissig Sekunden lagen.
 * Jede Zahl darin ist gezaehlt.
 *
 * ## Drei Groessen, und warum es drei sein muessen
 *
 * Die schriftliche Gegenrede zum Entwurf verlangt ein sehr kompaktes Instrument
 * (Richtwert 320 mal 150 Pixel), das Designbild des Nutzers zeigt eine deutlich
 * groessere Karte je Agent mit Wegzeile, und es traegt selbst einen
 * EXPAND-Knopf. Beides ist richtig, aber nicht gleichzeitig:
 *
 *  - **kompakt** ist der Normalzustand, weil das Instrument den Graphen erklaeren
 *    und ihn nicht zudecken soll. Eine Zeile je Akteur, und alles, was in dieser
 *    Zeile nicht Platz hat, kuerzt sich mit einem Auslassungszeichen und steht
 *    vollstaendig im Tooltip und in der grossen Lage.
 *  - **expand** zeigt die ausfuehrliche Karte je Akteur, so wie das Bild sie
 *    zeigt: den ganzen Ort, die Wegzeile, die Zahlen der Laeufe, das letzte
 *    Ereignis im Wortlaut, und die Liste dessen, was sich nicht verorten liess.
 *  - **eingeklappt** bleibt eine Zeile, die sagt, wie viele Akteure laufen. Ein
 *    Instrument, das ganz verschwindet, laesst den Leser vor kreisenden Punkten
 *    ohne Erklaerung sitzen.
 *
 * Die gewaehlte Groesse liegt im Speicher des Browsers und ueberlebt den Reload.
 *
 * ## Die Ehrlichkeitsregel dieser Flaeche
 *
 * Drei Dinge stehen hier NICHT, und alle drei fehlen mit Absicht:
 *
 *  1. **Kein Fortschritt.** Weder ein Balken noch eine Prozentzahl noch ein
 *     "fast fertig". Diese Oberflaeche sieht Werkzeugaufrufe, nicht Vorhaben;
 *     wie weit ein Agent ist, weiss sie nicht und kann sie nicht wissen.
 *  2. **Keine Bewertung.** Kein "gut", kein "haengt", kein "denkt nach". Eine
 *     Pause zwischen zwei Ereignissen ist eine Pause zwischen zwei Ereignissen.
 *  3. **Keine erfundene Absicht.** Eine Absichtszeile erscheint NUR, wenn das
 *     Ereignis ein `intent`-Feld mitbringt, das der Agent selbst geschrieben
 *     hat, und dann in eigener Schriftfarbe, mit dem Praefix "agent says:" und
 *     einem Tooltip, der sagt, dass es gemeldet und nicht gemessen ist. Das
 *     Designbild zeigt an dieser Stelle Zeilen wie "Refactoring
 *     OrderService.createOrder"; genau diese Zeile ist verboten, solange kein
 *     Ereignis sie traegt.
 */

import type { JSX } from 'react';
import { useCallback, useEffect, useRef, useState } from 'react';

import Hint from '../ui/tooltip/Hint';
import { STRIP_SECONDS } from './agent-store';
import { TRAIL_WINDOWS } from './agent-view';
import type { ActorFilter, ActorView, AgentsView, HudSize } from './agent-view';
import { WORK_KIND_WORD, agentStrings as text } from './agent-strings';
import type { AgentSourceStatus } from './agent-source';
import { bridgeCommand } from './agent-source';

/** Die drei Umschalter des laufenden Blicks. */
export interface HudSwitches {
    follow: boolean;
    trails: boolean;
    /**
     * Das Vollbild.
     *
     * Hiess bis W11b `cinema`. Der Nutzer hat es am 2026-08-30 benutzt und
     * woertlich gesagt: "bin jetzt im cinema mode, sollte fullscreen heissen."
     */
    fullscreen: boolean;
}

export interface AgentsHudProps {
    view: AgentsView;
    status: AgentSourceStatus;
    /** Der Port der Bruecke, fuer den Befehl, der sie startet. */
    port: number;
    size: HudSize;
    onSize: (size: HudSize) => void;
    filter: ActorFilter;
    onFilter: (filter: ActorFilter) => void;
    switches: HudSwitches;
    onSwitch: (name: keyof HudSwitches) => void;
    trailWindowMs: number;
    onTrailWindow: (ms: number) => void;
    /** Ob die Zeichenebene ueberhaupt an ist (Einstellungen, W10-Gruppe). */
    layerOn: boolean;
    /**
     * Ob das Instrument als Spalte am Rand steht (Vollbild, W11b AC5).
     *
     * In der Spalte bekommt jeder Akteur ZWEI Zeilen statt einer, und das ist
     * die Antwort auf einen Befund am Beweisbild von W11a: in einer Spalte von
     * dreihundert Pixeln schrumpfen die Namen auf vier Zeichen ("expl...",
     * "chec..."), und sauber gekuerzt ist immer noch gekuerzt. "chec..." sagt
     * niemandem, wer da arbeitet. Zwei Zeilen kosten Hoehe, und Hoehe ist genau
     * das, was eine Spalte im Vollbild hat.
     */
    column?: boolean;
}

const FILTERS: readonly ActorFilter[] = ['you', 'agent', 'both'];

function filterLabel(option: ActorFilter): string {
    return option === 'you' ? text.filterYou : option === 'agent' ? text.filterAgent : text.filterBoth;
}

/** Die Zeitspanne in Worten. Sekunden bis eine Minute, danach Minuten. */
function spell(ms: number): string {
    return text.ago(ms / 1000);
}

/** Der Ort in Worten, samt der Frage, wie sicher er ist. */
function placeText(actor: ActorView): string {
    const place = actor.placement;
    if (place.kind === 'none') {
        return text.placeNone;
    }
    if (place.uncertain) {
        return text.placeUncertain(place.name);
    }
    if (place.kind === 'range' && actor.last.lines !== undefined) {
        return text.placeRange(place.name, actor.last.lines[0], actor.last.lines[1]);
    }
    return text.placeFile(place.name);
}

/** Der Aktivitaetsstreifen: ein Balken je Sekunde, aus echten Ereignissen. */
function Strip(props: { actor: ActorView }): JSX.Element {
    const peak = Math.max(1, ...props.actor.strip);
    return (
        <Hint
            name={`agents-strip-${props.actor.id}`}
            text={text.stripTitle(STRIP_SECONDS, props.actor.stripTotal)}
        >
            <span
                className="atlas-agents-strip"
                data-testid="atlas-agents-strip"
                data-actor={props.actor.id}
                data-bars={props.actor.strip.join(',')}
                data-total={props.actor.stripTotal}
                data-seconds={STRIP_SECONDS}
                aria-hidden="true"
            >
                {props.actor.strip.map((count, index) => (
                    <span
                        // eslint-disable-next-line react/no-array-index-key
                        key={index}
                        className="atlas-agents-bar"
                        data-count={count}
                        style={{
                            height: `${count === 0 ? 1 : Math.round((count / peak) * 100)}%`,
                            background: count === 0 ? undefined : props.actor.color,
                        }}
                    />
                ))}
            </span>
        </Hint>
    );
}

/** Die Absichtszeile. Sie gibt es nur, wenn das Ereignis sie mitbringt. */
function Intent(props: { actor: ActorView }): JSX.Element | null {
    if (props.actor.intent.length === 0) {
        return null;
    }
    return (
        <Hint name={`agents-intent-${props.actor.id}`} text={text.intentTitle}>
            <span
                className="atlas-agents-intent"
                data-testid="atlas-agents-intent"
                data-actor={props.actor.id}
                data-self-reported="true"
            >
                <span className="atlas-agents-intent-prefix">{text.intentPrefix}</span>
                {` ${props.actor.intent}`}
            </span>
        </Hint>
    );
}

/** Der Ort, mit seinem Tooltip. In beiden Lagen derselbe Text. */
function Place(props: { actor: ActorView; className: string }): JSX.Element {
    const { actor } = props;
    return (
        <Hint
            name={`agents-place-${actor.id}`}
            text={
                actor.placement.kind === 'none'
                    ? text.placeNoneTitle(actor.placement.why)
                    : actor.placement.uncertain
                        ? text.placeUncertainTitle
                        : actor.placement.qualifiedName
            }
        >
            <span className={props.className} data-testid="atlas-agents-place">
                {placeText(actor)}
            </span>
        </Hint>
    );
}

/**
 * Eine Zeile je Akteur.
 *
 * Kompakt ist sie EINE Zeile: Farbpunkt, Name, Buchstabe der Art, Ort, Streifen,
 * Dauer. Was nicht hineinpasst, kuerzt sich mit einem Auslassungszeichen; der
 * ganze Wortlaut steht im Tooltip und in der grossen Lage. Eine Absichtszeile
 * kommt darunter, aber nur, wenn das Ereignis eine mitbringt.
 */
function Row(props: {
    actor: ActorView;
    trails: boolean;
    expanded: boolean;
    column: boolean;
}): JSX.Element {
    const { actor, expanded, column } = props;
    const place = <Place actor={actor} className="atlas-agents-place" />;
    return (
        <li
            className="atlas-agents-row"
            data-testid="atlas-agents-row"
            data-actor={actor.id}
            data-kind={actor.kind}
            data-letter={actor.letter}
            data-you={actor.you}
            data-idle={actor.idle}
            data-drawn={actor.drawn}
            data-lines={column ? 2 : 1}
            data-placement={actor.placement.kind}
            data-uncertain={actor.placement.uncertain}
            data-node={actor.placement.nodeId ?? ''}
        >
            <span className="atlas-agents-row-head">
                <span
                    className="atlas-agents-dot"
                    data-testid="atlas-agents-dot"
                    data-color={actor.color}
                    style={{ background: actor.color }}
                    aria-hidden="true"
                />
                <span className="atlas-agents-name" data-testid="atlas-agents-name">
                    {actor.name}
                </span>
                <Hint
                    name={`agents-kind-${actor.id}`}
                    text={text.kindTitle(actor.kind, actor.kindLetter)}
                >
                    <span
                        className="atlas-agents-kind"
                        data-testid="atlas-agents-kind"
                        data-kind={actor.kind}
                        data-letter={actor.kindLetter}
                    >
                        {actor.kindLetter}
                    </span>
                </Hint>
                {!expanded && !column && place}
                {!column && <Strip actor={actor} />}
                <Hint
                    name={`agents-since-${actor.id}`}
                    text={actor.idle ? text.idleTitle(actor.sinceMs / 1000) : text.orderTitle}
                >
                    <span className="atlas-agents-since" data-testid="atlas-agents-since">
                        {spell(actor.hereMs)}
                    </span>
                </Hint>
            </span>
            {/*
              * Die zweite Zeile der Spalte: Ort und Streifen.
              *
              * Sie hat ihren eigenen Kasten und nicht nur einen Umbruch, damit
              * der Ort die ganze Breite bekommt. Das ist der Unterschied
              * zwischen "chec..." und "checker" und zwischen "hots..." und
              * "hotspotScan".
              */}
            {column && (
                <span className="atlas-agents-row-second">
                    {place}
                    <Strip actor={actor} />
                </span>
            )}
            {expanded && !column && (
                <Place actor={actor} className="atlas-agents-place atlas-agents-place-full" />
            )}
            <Intent actor={actor} />
            {expanded && actor.kind === 'test' && (
                <span
                    className="atlas-agents-tested"
                    data-testid="atlas-agents-tested"
                    data-node={actor.testedNode?.id ?? ''}
                >
                    {actor.testedNode === undefined
                        ? text.testedUnknown
                        : text.testedTitle(actor.testedNode.name)}
                </span>
            )}
            {/*
              * Die Verlaufszeile.
              *
              * In der Spalte steht sie immer, denn genau sie ist die
              * "Verlaufszeile je Agent", die AC5 dort verlangt. Im Panel
              * haengt sie weiter am Schalter TRAILS, weil eine Zeile mehr je
              * Akteur dort der Unterschied zwischen einem Instrument und einem
              * Panel ist.
              */}
            {(props.trails || column) && (
                <span
                    className="atlas-agents-path"
                    data-testid="atlas-agents-path"
                    data-count={actor.paths.length}
                >
                    {actor.paths.length === 0 ? text.pathEmpty : actor.paths.join(' < ')}
                </span>
            )}
            {expanded && (
                <span className="atlas-agents-card" data-testid="atlas-agents-card">
                    <span className="atlas-agents-card-line">
                        {`${actor.count} events, ${
                            actor.missed === 0 ? text.orderIntact : text.orderMissed(actor.missed)
                        }`}
                    </span>
                    <span className="atlas-agents-card-line">
                        {`${actor.last.tool} ${actor.last.detail}`}
                    </span>
                </span>
            )}
        </li>
    );
}

export default function AgentsHud(props: AgentsHudProps): JSX.Element {
    const { view, status, size } = props;
    const collapsed = size === 'collapsed';
    const expanded = size === 'expanded';
    const column = props.column === true;

    /*
     * Die Kante des Kastens, gemessen wie bei der Legende der Galaxie.
     *
     * Dieselbe Begruendung wie dort: der Bildlauf dieser Plattform ist eine
     * ueberlagernde Leiste, die im Ruhezustand unsichtbar ist. Ohne einen
     * eigenen Hinweis endet der letzte sichtbare Satz an einer harten Kante, und
     * das liest sich als Fehler und nicht als Fortsetzung.
     */
    const body = useRef<HTMLDivElement | null>(null);
    const [edge, setEdge] = useState({ above: false, below: false });
    const measure = useCallback(() => {
        const node = body.current;
        if (node === null) {
            return;
        }
        const above = node.scrollTop > 1;
        const below = node.scrollTop + node.clientHeight < node.scrollHeight - 1;
        setEdge((current) =>
            (current.above === above && current.below === below ? current : { above, below }));
    }, []);
    useEffect(() => {
        measure();
    }, [measure, size, view.actors, props.switches.trails]);
    useEffect(() => {
        const node = body.current;
        if (node === null || typeof ResizeObserver === 'undefined') {
            return;
        }
        const observer = new ResizeObserver(measure);
        observer.observe(node);
        return () => observer.disconnect();
    }, [measure, size]);

    const sourceHead = (): string => {
        if (status.state === 'off') {
            return text.sourceOff;
        }
        if (status.state === 'connecting') {
            return text.sourceConnecting;
        }
        if (status.state === 'no-source') {
            return text.sourceNone;
        }
        const hello = status.hello;
        return hello?.mode === 'replay'
            ? text.sourceReplay(hello.file)
            : text.sourceConnected(hello?.file ?? '');
    };

    /*
     * Der ausfuehrliche Teil der Quelle steht nur dort, wo er gebraucht wird:
     * wenn es keine Quelle gibt (dann traegt er den Befehl, der sie startet)
     * und in der grossen Lage. Im Normalfall haette er in einem Instrument von
     * 320 Pixeln drei Zeilen belegt, um zu sagen, was die eine Zeile darueber
     * schon sagt.
     */
    const showSourceDetail = expanded || status.state === 'no-source' || status.state === 'off';

    return (
        <aside
            className="atlas-agents"
            data-testid="atlas-agents"
            data-size={size}
            data-source={status.state}
            data-layer={props.layerOn}
            data-filter={props.filter}
            data-column={column}
            aria-label={text.panelLabel}
        >
            <header className="atlas-agents-head" data-testid="atlas-agents-head">
                <span className="atlas-agents-head-row">
                    <span className="atlas-agents-title">{text.title}</span>
                    {collapsed ? (
                        <span
                            className="atlas-agents-line"
                            data-testid="atlas-agents-line"
                            data-count={view.all.length}
                        >
                            {text.collapsedLine(view.all.length)}
                        </span>
                    ) : (
                        <span className="atlas-agents-source-head" data-testid="atlas-agents-reading">
                            {sourceHead()}
                        </span>
                    )}
                    <span className="atlas-agents-head-buttons">
                        {!collapsed && (
                            <Hint name="agents-expand" text={text.expandTitle(expanded)}>
                                <button
                                    type="button"
                                    className="atlas-agents-button"
                                    data-testid="atlas-agents-expand"
                                    aria-pressed={expanded}
                                    onClick={() => props.onSize(expanded ? 'compact' : 'expanded')}
                                >
                                    {text.expand(expanded)}
                                </button>
                            </Hint>
                        )}
                        <Hint name="agents-fold" text={text.foldTitle(!collapsed)}>
                            <button
                                type="button"
                                className="atlas-agents-button"
                                data-testid="atlas-agents-fold"
                                aria-expanded={!collapsed}
                                data-fold={collapsed ? 'open' : 'collapse'}
                                onClick={() => props.onSize(collapsed ? 'compact' : 'collapsed')}
                            >
                                {text.fold(!collapsed)}
                            </button>
                        </Hint>
                    </span>
                </span>
                {!collapsed && (
                    <span className="atlas-agents-head-row">
                        <span
                            className="atlas-agents-count"
                            data-testid="atlas-agents-count"
                            data-count={view.all.length}
                        >
                            {text.agentCount(view.all.length)}
                        </span>
                        <span
                            className="atlas-agents-rate"
                            data-testid="atlas-agents-rate"
                            data-per-minute={view.perMinute}
                            data-events={view.events}
                        >
                            {text.perMinute(view.perMinute)}
                        </span>
                        <Hint name="agents-order" text={text.orderTitle}>
                            <span
                                className="atlas-agents-order"
                                data-testid="atlas-agents-order"
                                data-missed={view.missed}
                            >
                                {view.missed === 0 ? text.orderIntact : text.orderMissed(view.missed)}
                            </span>
                        </Hint>
                    </span>
                )}
            </header>

            {!collapsed && (
                <div
                    className="atlas-agents-body"
                    data-testid="atlas-agents-body"
                    data-more-above={edge.above}
                    data-more-below={edge.below}
                    ref={body}
                    onScroll={measure}
                >
                    {showSourceDetail && (
                        <div
                            className="atlas-agents-source"
                            data-testid="atlas-agents-source"
                            data-state={status.state}
                            data-mode={status.hello?.mode ?? ''}
                        >
                            {status.state === 'off' && (
                                <span className="atlas-agents-source-detail">{text.sourceOffDetail}</span>
                            )}
                            {status.state === 'no-source' && (
                                <>
                                    <span className="atlas-agents-source-detail">
                                        {text.sourceNoneDetail}
                                    </span>
                                    <code
                                        className="atlas-agents-command"
                                        data-testid="atlas-agents-command"
                                        data-command={bridgeCommand(props.port)}
                                    >
                                        {bridgeCommand(props.port)}
                                    </code>
                                </>
                            )}
                            {status.hello?.mode === 'replay' && (
                                <span
                                    className="atlas-agents-source-detail"
                                    data-testid="atlas-agents-replay"
                                >
                                    {text.replayNote}
                                </span>
                            )}
                            {view.unreadable > 0 && (
                                <span
                                    className="atlas-agents-source-detail"
                                    data-testid="atlas-agents-unreadable"
                                >
                                    {text.unreadableLines(view.unreadable)}
                                </span>
                            )}
                        </div>
                    )}

                    {!props.layerOn && (
                        <p className="atlas-agents-note" data-testid="atlas-agents-layer-off">
                            {text.layerOff}
                        </p>
                    )}

                    <div
                        className="atlas-agents-filter"
                        data-testid="atlas-agents-filter"
                        role="group"
                        aria-label={text.filterLabel}
                    >
                        <span className="atlas-agents-filter-label">{text.filterLabel}</span>
                        {FILTERS.map((option) => (
                            <Hint
                                key={option}
                                name={`agents-filter-${option}`}
                                text={text.filterTitle(option)}
                            >
                                <button
                                    type="button"
                                    className="atlas-agents-option"
                                    data-testid="atlas-agents-filter-option"
                                    data-option={option}
                                    data-active={props.filter === option}
                                    aria-pressed={props.filter === option}
                                    onClick={() => props.onFilter(option)}
                                >
                                    {filterLabel(option)}
                                </button>
                            </Hint>
                        ))}
                    </div>

                    <ul
                        className="atlas-agents-rows"
                        data-testid="atlas-agents-rows"
                        data-count={view.actors.length}
                        data-column={column}
                    >
                        {view.actors.map((actor) => (
                            <Row
                                key={actor.id}
                                actor={actor}
                                trails={props.switches.trails}
                                expanded={expanded}
                                column={column}
                            />
                        ))}
                    </ul>

                    {/*
                      * Der Deckel sagt sich selbst.
                      *
                      * Er greift an den Koerpern und nicht an den Zeilen: jeder
                      * Akteur steht oben, mit allen seinen Zahlen. Was fehlt,
                      * ist ein Punkt auf dem Graphen, und ein Bild, dem man
                      * nicht ansieht, dass es unvollstaendig ist, waere die
                      * stille Behauptung, es sei vollstaendig.
                      */}
                    {view.capped > 0 && (
                        <Hint name="agents-cap" text={text.capTitle}>
                            <p
                                className="atlas-agents-cap"
                                data-testid="atlas-agents-cap"
                                data-cap={view.cap}
                                data-capped={view.capped}
                                data-drawn={view.actors.filter((actor) => actor.drawn).length}
                            >
                                {text.capLine(
                                    view.actors.filter((actor) => actor.drawn).length,
                                    view.actors.length,
                                )}
                            </p>
                        </Hint>
                    )}

                    {/*
                      * Der Ereignis-Ticker im Klartext (AC5). Nur in der Spalte:
                      * im Panel waeren sechs weitere Zeilen der Unterschied
                      * zwischen einem Instrument und einem Protokoll.
                      */}
                    {column && view.ticker.length > 0 && (
                        <div
                            className="atlas-agents-ticker"
                            data-testid="atlas-agents-ticker"
                            data-count={view.ticker.length}
                        >
                            <Hint name="agents-ticker" text={text.tickerTitle}>
                                <span className="atlas-agents-ticker-head">{text.tickerLabel}</span>
                            </Hint>
                            {view.ticker.map((entry) => (
                                <span
                                    key={`${entry.ts}-${entry.actor}-${entry.tool}-${entry.path}`}
                                    className="atlas-agents-ticker-row"
                                    data-testid="atlas-agents-ticker-row"
                                    data-actor={entry.actor}
                                    data-kind={entry.kind}
                                    data-place={entry.place}
                                    data-lines={entry.lines.join('-')}
                                    style={{ ['--atlas-agent-color' as string]: entry.color }}
                                >
                                    {text.tickerLine(
                                        entry.name,
                                        WORK_KIND_WORD[entry.kind],
                                        entry.place,
                                        entry.lines,
                                    )}
                                </span>
                            ))}
                        </div>
                    )}

                    {view.unmapped.length > 0 && (
                        <div
                            className="atlas-agents-unmapped"
                            data-testid="atlas-agents-unmapped"
                            data-count={view.unmapped.length}
                        >
                            <Hint name="agents-unmapped" text={text.unmappedIntro}>
                                <span className="atlas-agents-unmapped-head">
                                    {text.unmappedTitle(view.unmapped.length)}
                                </span>
                            </Hint>
                            {expanded && (
                                <span className="atlas-agents-unmapped-intro">
                                    {text.unmappedIntro}
                                </span>
                            )}
                            {expanded && view.unmapped.map((event) => (
                                <span
                                    className="atlas-agents-unmapped-row"
                                    data-testid="atlas-agents-unmapped-row"
                                    data-tool={event.tool}
                                    data-path={event.path}
                                    data-why={event.why}
                                    key={`${event.ts}-${event.tool}-${event.path}`}
                                >
                                    {text.unmappedRow(event.tool, event.path, event.why)}
                                </span>
                            ))}
                        </div>
                    )}

                    <div
                        className="atlas-agents-switches"
                        data-testid="atlas-agents-switches"
                        role="group"
                        aria-label={text.title}
                    >
                        <Hint name="agents-follow" text={text.followTitle(props.switches.follow)}>
                            <button
                                type="button"
                                className="atlas-agents-option"
                                data-testid="atlas-agents-switch"
                                data-switch="follow"
                                data-active={props.switches.follow}
                                aria-pressed={props.switches.follow}
                                onClick={() => props.onSwitch('follow')}
                            >
                                {text.follow}
                            </button>
                        </Hint>
                        <Hint name="agents-trails" text={text.trailsTitle(props.switches.trails)}>
                            <button
                                type="button"
                                className="atlas-agents-option"
                                data-testid="atlas-agents-switch"
                                data-switch="trails"
                                data-active={props.switches.trails}
                                aria-pressed={props.switches.trails}
                                onClick={() => props.onSwitch('trails')}
                            >
                                {text.trails}
                            </button>
                        </Hint>
                        <Hint
                            name="agents-fullscreen"
                            text={text.fullscreenTitle(props.switches.fullscreen)}
                        >
                            <button
                                type="button"
                                className="atlas-agents-option"
                                data-testid="atlas-agents-switch"
                                data-switch="fullscreen"
                                data-active={props.switches.fullscreen}
                                aria-pressed={props.switches.fullscreen}
                                onClick={() => props.onSwitch('fullscreen')}
                            >
                                {text.fullscreen}
                            </button>
                        </Hint>
                        {props.switches.trails && TRAIL_WINDOWS.map((ms) => (
                            <Hint key={ms} name={`agents-window-${ms}`} text={text.windowTitle}>
                                <button
                                    type="button"
                                    className="atlas-agents-option atlas-agents-window-option"
                                    data-testid="atlas-agents-window-option"
                                    data-option={ms}
                                    data-active={props.trailWindowMs === ms}
                                    aria-pressed={props.trailWindowMs === ms}
                                    onClick={() => props.onTrailWindow(ms)}
                                >
                                    {text.windowOption(ms)}
                                </button>
                            </Hint>
                        ))}
                    </div>

                    {expanded && props.switches.fullscreen && (
                        <p className="atlas-agents-note" data-testid="atlas-agents-fullscreen-scope">
                            {text.fullscreenScope}
                        </p>
                    )}
                </div>
            )}
            {(edge.above || edge.below) && !collapsed && (
                <span
                    className="atlas-agents-more"
                    data-testid="atlas-agents-more-mark"
                    data-scroll-hint={[edge.above ? 'top' : '', edge.below ? 'bottom' : '']
                        .filter((part) => part.length > 0)
                        .join(' ')}
                >
                    {edge.below ? '▾ more' : '▴ more'}
                </span>
            )}
        </aside>
    );
}
