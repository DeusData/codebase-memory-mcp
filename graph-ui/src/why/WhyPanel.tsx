/**
 * The question that fills the empty editor area: why are you here?
 *
 * A panel and not a modal, for the reference project's reason
 * (`codeatlas-views/src/browser/why-widget.tsx`): a dialog on first open blocks
 * the surface and every automated run through it, where a panel occupies the
 * space that is empty anyway and can be walked past. It appears when no file is
 * open and this browser has no answer recorded for this project, and it can be
 * called back from the [a]tlas menu at any time.
 *
 * The cards are in the order of a working day, and two of them say in their own
 * sentence that they set the reading and nothing more this cycle. Both of those
 * decisions and the wording rules live beside the strings in why-model.ts.
 *
 * Purely presentational: which cards there are is data, what an answer does is
 * App.tsx's business, and this file draws four buttons and a decline.
 */

import type { JSX } from 'react';
import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import {
    WHY_CARDS,
    WHY_DECLINE_LABEL,
    WHY_DECLINE_TOOLTIP,
    WHY_HEADLINE,
    WHY_SUBLINE,
} from './why-model';
import type { WhyIntent } from './why-model';

export interface WhyPanelProps {
    /** The project the answer is about, named so the reader knows what they are answering for. */
    project: string;
    onChoose: (intent: WhyIntent) => void;
    onDecline: () => void;
}

export default function WhyPanel(props: WhyPanelProps): JSX.Element {
    return (
        <section className="atlas-why" data-testid="atlas-why" aria-label={WHY_HEADLINE}>
            <div className="atlas-why-inner">
                <h2 className="atlas-why-headline" data-testid="atlas-why-headline">
                    {WHY_HEADLINE}
                </h2>
                <p className="atlas-why-subline">{WHY_SUBLINE}</p>

                <div className="atlas-why-cards">
                    {WHY_CARDS.map((card, index) => (
                        <button
                            key={card.intent}
                            type="button"
                            className="atlas-why-card"
                            data-testid="atlas-why-card"
                            data-intent={card.intent}
                            onClick={() => props.onChoose(card.intent)}
                        >
                            <span className="atlas-why-card-index">{index + 1}</span>
                            <span className="atlas-why-card-label">{card.label}</span>
                            <span className="atlas-why-card-detail">{card.detail}</span>
                            {card.stub !== undefined && (
                                <span className="atlas-why-card-stub" data-testid="atlas-why-stub">
                                    {card.stub}
                                </span>
                            )}
                        </button>
                    ))}
                </div>

                <div className="atlas-why-foot">
                    <Hint name="why-decline" text={WHY_DECLINE_TOOLTIP}>
                        <button
                            type="button"
                            className="atlas-why-decline"
                            data-testid="atlas-why-decline"
                            onClick={props.onDecline}
                        >
                            {WHY_DECLINE_LABEL}
                        </button>
                    </Hint>
                    <span className="atlas-why-project">
                        {props.project.length > 0 ? props.project : messages.why.noProject}
                    </span>
                </div>
            </div>
        </section>
    );
}
