/**
 * Der Zeitstrahl unter dem Graphen: eine Spur je Akteur, ein Strich je
 * Ereignis.
 *
 * ## Warum er ein Strich je Ereignis ist und keine Kurve
 *
 * Weil eine Kurve eine Aussage waere. Sie muesste glaetten, und Glaetten heisst
 * hier, zwischen zwei Werkzeugaufrufen eine Dichte zu behaupten, die niemand
 * gemessen hat. Ein Strich je Ereignis ist die Ablesung: wo einer steht, war
 * ein Aufruf fertig; wo keiner steht, war keiner. Die Luecken sind dabei die
 * interessantere Haelfte des Bildes.
 *
 * ## Die drei Lagen und die eine Gefahr
 *
 * `live` laeuft mit, `paused` haelt das Fenster an (die Ereignisse laufen
 * weiter ein), `replay` zeigt den Zustand von damals. Die dritte ist die
 * gefaehrliche: ein alter Zustand sieht aus wie der jetzige. Sie kennzeichnet
 * sich darum nicht nur hier, sondern faerbt die ganze Ansicht (siehe
 * `data-replay` am Panel) und sagt in Worten, wie weit zurueck sie steht.
 */

import type { JSX, MouseEvent as ReactMouseEvent } from 'react';
import { useCallback, useRef } from 'react';

import Hint from '../ui/tooltip/Hint';
import { agentStrings as text } from './agent-strings';
import { TIMELINE_WINDOWS, timeAtFraction } from './agent-timeline';
import type { Timeline } from './agent-timeline';

export interface AgentsTimelineProps {
    timeline: Timeline;
    /** Die Gegenwart, fuer den Satz "so weit zurueck". */
    now: number;
    windowMs: number;
    onWindow: (ms: number) => void;
    /** Anhalten und weiterlaufen lassen. */
    onPause: () => void;
    /** Auf einen Zeitpunkt springen. */
    onScrub: (ts: number) => void;
    /** Zurueck in die Gegenwart. */
    onLive: () => void;
}

function modeLabel(timeline: Timeline): string {
    return timeline.mode === 'replay'
        ? text.timelineReplay
        : timeline.mode === 'paused' ? text.timelinePaused : text.timelineLive;
}

export default function AgentsTimeline(props: AgentsTimelineProps): JSX.Element {
    const { timeline } = props;
    const lanes = useRef<HTMLDivElement | null>(null);

    const scrub = useCallback((event: ReactMouseEvent<HTMLDivElement>) => {
        const box = lanes.current?.getBoundingClientRect();
        if (box === undefined || box.width === 0) {
            return;
        }
        props.onScrub(timeAtFraction(timeline, (event.clientX - box.x) / box.width));
    }, [props, timeline]);

    return (
        <div
            className="atlas-agents-timeline"
            data-testid="atlas-agents-timeline"
            data-mode={timeline.mode}
            data-tracks={timeline.tracks.length}
            data-ticks={timeline.ticks}
            data-window={timeline.windowMs}
            data-from={timeline.from}
            data-to={timeline.to}
        >
            <div className="atlas-agents-timeline-head">
                <span className="atlas-agents-timeline-label">{text.timelineLabel}</span>
                <Hint
                    name="agents-timeline-mode"
                    text={
                        timeline.mode === 'replay'
                            ? text.timelineReplayTitle
                            : timeline.mode === 'paused'
                                ? text.timelinePauseTitle
                                : text.timelineLiveTitle
                    }
                >
                    <span
                        className="atlas-agents-timeline-mode"
                        data-testid="atlas-agents-timeline-mode"
                        data-mode={timeline.mode}
                    >
                        {modeLabel(timeline)}
                    </span>
                </Hint>
                <Hint name="agents-timeline-pause" text={text.timelinePauseTitle}>
                    <button
                        type="button"
                        className="atlas-agents-option"
                        data-testid="atlas-agents-timeline-pause"
                        data-active={timeline.mode === 'paused'}
                        aria-pressed={timeline.mode === 'paused'}
                        onClick={props.onPause}
                    >
                        {timeline.mode === 'paused' ? text.timelineResume : text.timelinePause}
                    </button>
                </Hint>
                {timeline.mode === 'replay' && (
                    <>
                        <span
                            className="atlas-agents-timeline-replay"
                            data-testid="atlas-agents-timeline-replay"
                        >
                            {text.timelineReplayNote(text.ago((props.now - timeline.to) / 1000))}
                        </span>
                        <button
                            type="button"
                            className="atlas-agents-option"
                            data-testid="atlas-agents-timeline-live"
                            onClick={props.onLive}
                        >
                            {text.timelineBackToLive}
                        </button>
                    </>
                )}
                <span className="atlas-agents-timeline-windows">
                    {TIMELINE_WINDOWS.map((ms) => (
                        <Hint key={ms} name={`agents-timeline-window-${ms}`} text={text.windowTitle}>
                            <button
                                type="button"
                                className="atlas-agents-option"
                                data-testid="atlas-agents-timeline-window"
                                data-option={ms}
                                data-active={props.windowMs === ms}
                                aria-pressed={props.windowMs === ms}
                                onClick={() => props.onWindow(ms)}
                            >
                                {text.windowOption(ms)}
                            </button>
                        </Hint>
                    ))}
                </span>
            </div>

            <div className="atlas-agents-timeline-body">
                <div className="atlas-agents-timeline-names">
                    {timeline.tracks.map((track) => (
                        <span
                            key={track.id}
                            className="atlas-agents-timeline-name"
                            data-testid="atlas-agents-timeline-name"
                            data-actor={track.id}
                            data-idle={track.idle}
                            style={{ color: track.color }}
                        >
                            {track.name}
                        </span>
                    ))}
                </div>
                {/*
                  * Der Klickbereich ist die Flaeche der Spuren und nicht ein
                  * eigener Streifen darunter: der Leser zeigt auf den Strich,
                  * den er meint. Ein `div` mit einem Klick und ohne Rolle waere
                  * fuer die Tastatur nicht da; die Rolle `slider` sagt, was es
                  * ist, und die drei `aria`-Werte sagen, wo es steht.
                  */}
                <div
                    className="atlas-agents-timeline-lanes"
                    data-testid="atlas-agents-timeline-lanes"
                    ref={lanes}
                    role="slider"
                    tabIndex={0}
                    aria-label={text.timelineLabel}
                    aria-valuemin={timeline.from}
                    aria-valuemax={timeline.to}
                    aria-valuenow={timeline.to}
                    onClick={scrub}
                    onKeyDown={(event) => {
                        if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
                            event.preventDefault();
                            const step = timeline.windowMs / 20;
                            props.onScrub(timeline.to + (event.key === 'ArrowLeft' ? -step : step));
                        }
                    }}
                >
                    {timeline.tracks.map((track) => (
                        <Hint
                            key={track.id}
                            name={`agents-timeline-track-${track.id}`}
                            text={text.timelineTrackTitle(track.name, track.count)}
                        >
                            <div
                                className="atlas-agents-timeline-track"
                                data-testid="atlas-agents-timeline-track"
                                data-actor={track.id}
                                data-count={track.count}
                                data-idle={track.idle}
                            >
                                {track.ticks.map((tick) => (
                                    <span
                                        key={`${tick.ts}-${tick.at}`}
                                        className="atlas-agents-timeline-tick"
                                        data-testid="atlas-agents-timeline-tick"
                                        data-kind={tick.kind}
                                        style={{
                                            left: `${(tick.at * 100).toFixed(3)}%`,
                                            background: track.color,
                                        }}
                                        aria-hidden="true"
                                    />
                                ))}
                            </div>
                        </Hint>
                    ))}
                </div>
            </div>
        </div>
    );
}
