/**
 * The step card, under the reader and over the command line.
 *
 * Layout from design/design.png: a header that is a step count and a chain of
 * shaded blocks, then the step's title, then two or three sentences, then the
 * keys as buttons. Two things about it are decisions rather than drawing.
 *
 * **It sits in the editor column, not over it.** The card is where the walk is
 * explained and the editor beside it is what the walk is explaining; a card that
 * covered the code would make the reader choose between the two. It pushes the
 * editor up instead, which is what the reference image shows.
 *
 * **Every key is also a button.** The keys are the fast path for somebody who
 * has both hands on the keyboard, and the buttons are what makes the same four
 * moves discoverable and reachable for somebody who does not know them yet. The
 * bracketed key is written into the button's own label, so the two can never
 * drift apart.
 *
 * **The fourth action can be off, and says so instead of hiding.** `[d] diagram`
 * draws the flow of the step's symbol, and a step that points at a file has no
 * symbol to draw. The button stays where it is, dimmed and disabled, and its
 * tooltip says which of the two it is. Removing it on those steps would move the
 * other three buttons under the reader's hand from step to step, and it would
 * turn "this step has no symbol" into "this product has no diagram".
 *
 * Purely presentational, like the chrome and the search window: what the steps
 * are and where the reader is are decided by App.tsx, and this file draws them.
 * That is what lets the whole card be proven in jsdom.
 */

import type { JSX } from 'react';
import type { TourStepRecord } from '../core/tour-protocol';
import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import { isLastStep, progressBar, progressLabel } from './tour-player';

export interface TourCardProps {
    /** The walk's own title, for the reader who forgot which walk they are in. */
    title: string;
    steps: readonly TourStepRecord[];
    /** 0-based index of the step in front of the reader. */
    index: number;
    /**
     * The one sentence a bounded walk ends with. Shown on the last step only:
     * a cap is a fact about the end of the walk, and saying it on every step
     * would make it read as a fact about every step.
     */
    endNote: string;
    onPrev: () => void;
    onNext: () => void;
    onExit: () => void;
    /**
     * Draw the flow of the step in front of the reader.
     *
     * Optional, so the card can still be rendered by a caller that has no flow
     * to offer; the button is then off for the same reason as on a file step and
     * says the same thing.
     */
    onDiagram?: () => void;
}

export default function TourCard(props: TourCardProps): JSX.Element | null {
    const total = props.steps.length;
    const step = props.steps[props.index];
    if (step === undefined) {
        return null;
    }
    const last = isLastStep(props.index, total);
    const first = props.index === 0;
    const showEndNote = last && props.endNote.length > 0;
    /*
     * Ob es an diesem Schritt etwas zu zeichnen gibt, entscheidet der Schritt
     * und nicht die Karte: `primary.kind` ist die eine Angabe, aus der hervorgeht,
     * ob der Index hier ein Symbol aufgeloest hat. Ein Dateischritt hat keins,
     * und ein Flow ohne Symbol waere ein Bild ueber nichts.
     */
    const canDiagram = step.primary.kind === 'symbol' && props.onDiagram !== undefined;
    const previous = (
        <button
            type="button"
            className="atlas-tour-action"
            data-testid="atlas-tour-prev"
            disabled={first}
            onClick={props.onPrev}
        >
            {messages.tour.prev}
        </button>
    );

    return (
        <section
            className="atlas-tour"
            data-testid="atlas-tour"
            data-step={props.index + 1}
            data-steps={total}
            data-tour-id={step.id}
            aria-label={props.title}
        >
            <header className="atlas-tour-head">
                <span className="atlas-tour-progress" data-testid="atlas-tour-progress">
                    {progressLabel(props.index, total)}
                </span>
                <span className="atlas-tour-bar" aria-hidden="true">
                    {progressBar(props.index, total)}
                </span>
                <span className="atlas-tour-name">{props.title}</span>
            </header>

            <h2 className="atlas-tour-title" data-testid="atlas-tour-title">
                <span className="atlas-tour-chevron">{'»'}</span> {step.title}
            </h2>

            <p className="atlas-tour-description" data-testid="atlas-tour-description">
                {step.description}
            </p>

            {showEndNote && (
                <p className="atlas-tour-cap" data-testid="atlas-tour-cap">
                    {props.endNote}
                </p>
            )}

            <div className="atlas-tour-actions">
                {first ? (
                    <Hint name="tour-prev" text={messages.tour.prevUnavailable}>
                        {previous}
                    </Hint>
                ) : previous}
                <button
                    type="button"
                    className="atlas-tour-action"
                    data-primary="true"
                    data-testid="atlas-tour-next"
                    onClick={props.onNext}
                >
                    {messages.tour.nextPrefix}{last ? messages.tour.finish : messages.tour.next}
                </button>
                <Hint
                    name="tour-diagram"
                    text={canDiagram ? messages.tour.diagramTitle : messages.tour.diagramUnavailable}
                >
                    <button
                        type="button"
                        className="atlas-tour-action"
                        data-testid="atlas-tour-diagram"
                        data-available={canDiagram}
                        disabled={!canDiagram}
                        onClick={() => props.onDiagram?.()}
                    >
                        {messages.tour.diagram}
                    </button>
                </Hint>
                <button
                    type="button"
                    className="atlas-tour-action"
                    data-testid="atlas-tour-exit"
                    onClick={props.onExit}
                >
                    {messages.tour.exit}
                </button>
            </div>
        </section>
    );
}
