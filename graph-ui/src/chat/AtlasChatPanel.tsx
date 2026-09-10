/**
 * The answer panel: what the model said, which cards it was given, and how much
 * of the neighbourhood went into them.
 *
 * Purely presentational, like every other panel in this project: what was asked
 * and what came back is decided in App.tsx, and this file decides what that
 * looks like. Four things it does that a plain transcript would not.
 *
 * **Citations are buttons.** A `[K3]` in the answer is rendered as a control
 * that navigates to the card's source, through the same `follow` path a twin row
 * or a galaxy node uses. There is no second navigation route, so there is no
 * second place that could disagree about where a symbol lives. A citation naming
 * a card that was never handed over is rendered as plain text with a warning:
 * a button that navigated nowhere would be worse than no button.
 *
 * **The cards fold open.** The answer is four lines; the evidence under it is
 * ten cards. Showing both at once would bury the answer, and showing only the
 * answer would make a local model's output look like an oracle's. So the cards
 * are one click away and the count is always visible.
 *
 * **The depth control sits next to the answer it changed.** Martin's context
 * rule is a setting, and a setting that lived in a preferences dialog would be
 * a setting nobody connects to the answer in front of them. It carries the
 * honest note that more neighbours change the answer rather than improve it.
 *
 * **Every line under the answer says where it comes from.** The question class,
 * the rule that fired, the token cost against the budget, and what the compiler
 * could not fetch. A reader who disagrees with an answer can see, without
 * asking anybody, whether the model was wrong or whether it was handed the
 * wrong cards.
 *
 * ## What W7c added, and where each of them lives after W8
 *
 * **The head does not scroll away.** The depth control changes the NEXT
 * question, so it has to be in reach while the reader is reading the last one.
 * It sits outside the scrolling area rather than inside it with `position:
 * sticky`, which is the same picture with one fewer way to go wrong. Unchanged.
 *
 * **Closing and clearing are two different acts and say so.** Until W7c there
 * was one button, `clear`, and folding the panel away cost the whole session.
 * Since W8 the chat is a tab of the explain zone, so the folding belongs to the
 * zone and its handle, and Escape folds the zone. What was won here is not
 * weaker for it, it is wider: folding costs nothing for any of the five tabs,
 * the folded strip names what is still in each of them, and `clear` is still
 * the only thing in this product that deletes a session. It says so in its
 * tooltip, which now also says what folding does NOT cost.
 *
 * **The height is the zone's.** W7c gave this panel its own drag handle,
 * because it was the only surface with a height nobody could change. The zone
 * has one now, for all five tabs, with the same bounds, the same keyboard, the
 * same memory across a reload (src/layout/layout-model.ts). A second height
 * inside the zone would be a second answer to the same question.
 *
 * **An ambiguous name is a question back to the reader.** When one written name
 * reached several symbols, the turn carries the candidates instead of an answer
 * and each of them is a button: name, file, line. Pressing one asks the same
 * question again about that symbol. The alternative, taking the first one and
 * saying so in a note under the answer, is a choice made for the reader in the
 * place they read last.
 */

import type { JSX } from 'react';
import { useEffect, useState } from 'react';

import { messages } from '../i18n/messages';
import type { Card, CardSource } from '../compiler/card-compiler';
import { claimLines, segmentsOf } from '../compiler/answer-contract';
import type { NeighborDepth, SubjectCandidate } from '../compiler/fact-recipes';
import type { ChatTurn } from './ask-atlas';
import { askModel } from './chat-client';
import { SIDECAR_ORIGIN } from '../llm/sidecar';
import { READER_SYSTEM_PROMPT, buildReaderPrompt, nonThinkingFor } from '../compiler/prompt-contract';
import { applyReaderRewrite, readerLines, readerMaxTokens, readerSubjectText } from '../twin/reader-rewrite';
import Hint from '../ui/tooltip/Hint';
import {
    CHAT_ASKING,
    CHAT_CANDIDATE_TEST_MARK,
    CHAT_COMPILING,
    CHAT_DEPTH_LABEL,
    CHAT_DEPTH_NOTE,
    CHAT_DEPTH_OPTIONS,
    CHAT_NO_CARDS_HINT,
    CHAT_OFF_MESSAGE,
    CHAT_AI_LABEL,
    CHAT_AI_RESTORE,
    CHAT_BUILT_PROVENANCE,
    CHAT_NOT_RUNNING_MESSAGE,
    CHAT_POLICY_MESSAGE,
    CHAT_THOUGHT_ONLY,
    CHAT_TITLE,
    CHAT_TRUNCATED,
    chatCandidateLabel,
    chatCandidateTitle,
    chatCardsFoldLabel,
    chatCardsLabel,
    chatAiProvenance,
    chatBuiltUnavailableMessage,
    chatChoiceHeadline,
    chatCitationWarning,
    chatFailed,
    chatFocusFallbackLine,
    chatProvenance,
    chatRerunLabel,
    chatRerunTooltip,
    chatUnknownCardWarning,
} from './chat-strings';

export interface AtlasChatPanelProps {
    /** The turns of this session, oldest first. Never persisted. */
    turns: readonly ChatTurn[];
    depth: NeighborDepth;
    onDepth: (depth: NeighborDepth) => void;
    /** Navigate to the source of a card. The same path a twin row takes. */
    onOpenCard: (source: CardSource) => void;
    /** Clear the session. The history lives in memory and nowhere else. */
    onClear: () => void;
    /** The reader picked one of an ambiguous name's candidates. */
    onPickCandidate: (turn: ChatTurn, candidate: SubjectCandidate) => void;
    /**
     * Ask this turn's question again, at whatever depth is set now.
     *
     * A new turn, never a rewrite of the old one: see {@link chatRerunTooltip}.
     */
    onAskAgain: (turn: ChatTurn) => void;
    aiAvailable?: boolean;
    modelName?: string;
    /** Router id for the explicit request; distinct from its visible name. */
    requestModel?: string;
}

/** The sentence a refused or failed turn shows instead of an answer. */
function messageOf(turn: ChatTurn): string {
    switch (turn.status) {
        case 'compiling':
            return CHAT_COMPILING;
        case 'asking':
            return CHAT_ASKING;
        case 'refused':
            return turn.refusal === 'policy'
                ? CHAT_POLICY_MESSAGE
                : turn.refusal === 'not-running'
                    ? CHAT_NOT_RUNNING_MESSAGE
                    : CHAT_OFF_MESSAGE;
        case 'failed':
            return chatFailed(turn.message);
        case 'no-cards':
            return CHAT_NO_CARDS_HINT;
        // The candidate list says it better than a sentence beside it could.
        case 'needs-choice':
            return '';
        default: {
            const unavailable = /^model-(off|not-running|policy)$/.exec(turn.message);
            return turn.message === 'thought-only' ? CHAT_THOUGHT_ONLY
                : unavailable === null ? ''
                    : chatBuiltUnavailableMessage(unavailable[1] as 'off' | 'not-running' | 'policy');
        }
    }
}

/** One card, as the fold shows it. */
function CardRow(props: { card: Card; onOpen: (source: CardSource) => void }): JSX.Element {
    const { card } = props;
    const source = card.source;
    return (
        <li className="atlas-chat-card" data-testid="atlas-chat-card" data-card={card.id} data-kind={card.kind}>
            <span className="atlas-chat-card-id">{card.id}</span>
            <span className="atlas-chat-card-lines">
                {card.lines.map((line, index) => (
                    <span className="atlas-chat-card-line" key={`${card.id}-${index}`}>
                        {line}
                    </span>
                ))}
            </span>
            {source !== undefined && source.filePath !== undefined && (
                <Hint name="chat-card-open" text={messages.chat.openCard(source.filePath, source.line)}>
                    <button
                        type="button"
                        className="atlas-chat-card-open"
                        data-testid="atlas-chat-card-open"
                        onClick={() => props.onOpen(source)}
                    >
                        {source.filePath}
                        {source.line === undefined ? '' : `:${source.line}`}
                    </button>
                </Hint>
            )}
        </li>
    );
}

/**
 * The candidates of an ambiguous name, as a list to choose from.
 *
 * Each entry carries what tells two symbols of the same name apart: the file and
 * the line. A list of three times `create` with nothing beside it would be the
 * same silence in a longer form.
 */
function ChoiceList(props: {
    turn: ChatTurn;
    onPick: (turn: ChatTurn, candidate: SubjectCandidate) => void;
}): JSX.Element | null {
    const choice = props.turn.choice;
    if (choice === undefined || choice.candidates.length === 0) {
        return null;
    }
    return (
        <div
            className="atlas-chat-choice"
            data-testid="atlas-chat-choice"
            data-count={choice.candidates.length}
            data-name={choice.name}
        >
            <p className="atlas-chat-choice-head" data-testid="atlas-chat-choice-head">
                {chatChoiceHeadline(choice.name, choice.candidates.length)}
            </p>
            <ul className="atlas-chat-candidates" aria-label={messages.chat.candidatesLabel}>
                {choice.candidates.map((candidate, index) => {
                    const qualified = candidate.qualifiedName ?? candidate.name;
                    return (
                        <li className="atlas-chat-candidate-row" key={`${qualified}-${index}`}>
                            <Hint name="chat-candidate" text={chatCandidateTitle(qualified)}>
                                <button
                                    type="button"
                                    className="atlas-chat-candidate"
                                    data-testid="atlas-chat-candidate"
                                    data-qualified={qualified}
                                    onClick={() => props.onPick(props.turn, candidate)}
                                >
                                    {chatCandidateLabel(candidate.name, candidate.filePath, candidate.line)}
                                </button>
                            </Hint>
                            {candidate.isTest === true && (
                                <span className="atlas-chat-candidate-mark">
                                    {CHAT_CANDIDATE_TEST_MARK}
                                </span>
                            )}
                        </li>
                    );
                })}
            </ul>
        </div>
    );
}

/** One turn: the question, the answer with its citations, and the evidence. */
function Turn(props: {
    turn: ChatTurn;
    /** The depth the control is set to NOW. The turn carries the one it was asked with. */
    depth: NeighborDepth;
    /** True for the turn the offer may appear on: the last one that carries an answer. */
    offerRerun: boolean;
    onOpenCard: (source: CardSource) => void;
    onPickCandidate: (turn: ChatTurn, candidate: SubjectCandidate) => void;
    onAskAgain: (turn: ChatTurn) => void;
    aiAvailable?: boolean;
    modelName?: string;
    requestModel?: string;
}): JSX.Element {
    const { turn } = props;
    const [open, setOpen] = useState(false);
    const cardIds = turn.cards.map((card) => card.id);
    const byId = new Map(turn.cards.map((card) => [card.id, card] as const));
    const lines = turn.answer.length === 0 ? [] : claimLines(turn.answer);
    const shown = lines.length === 0 && turn.answer.trim().length > 0
        ? [turn.answer.trim()]
        : lines;
    const message = messageOf(turn);
    const uncited = turn.check?.violations.filter((entry) => entry.reason === 'no-citation') ?? [];
    const unknown = turn.check?.unknown ?? [];

    /*
     * Das Angebot aus AC6b.
     *
     * Es steht nur an der LETZTEN Antwort, und nur, wenn die Einstellung seit
     * ihr eine andere ist. An jeder Antwort waere es eine Zeile Rauschen unter
     * jedem alten Zug; an keiner waere es der Satz im Kopf, den der Leser
     * ueberlesen hat.
     */
    const rerun = props.offerRerun && turn.depth !== props.depth;
    const builtLines = readerLines(shown.map((text, index) => ({ id: `chat-${index}`, text })));
    const [aiLines, setAiLines] = useState<readonly string[] | undefined>(undefined);
    const [aiState, setAiState] = useState<'idle' | 'running' | 'applied' | 'refused'>('idle');
    const [aiReason, setAiReason] = useState('');
    useEffect(() => {
        globalThis.__atlasChatReader = {
            subject: builtLines.map((line) => ({ id: line.id, text: line.text, facts: [...line.facts] })),
            validateRewrite: (answer: string) => {
                const outcome = applyReaderRewrite(builtLines, answer);
                return outcome.kind === 'applied'
                    ? { applied: true, reason: '' }
                    : { applied: false, reason: outcome.reason };
            },
        };
        return () => {
            globalThis.__atlasChatReader = undefined;
        };
    }, [builtLines]);
    const askAi = (): void => {
        if (!props.aiAvailable || builtLines.length === 0) return;
        setAiState('running');
        void askModel({
            origin: SIDECAR_ORIGIN,
            system: READER_SYSTEM_PROMPT,
            user: buildReaderPrompt('chat reader', readerSubjectText(builtLines)),
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

    return (
        <article
            className="atlas-chat-turn"
            data-testid="atlas-chat-turn"
            data-status={turn.status}
            data-depth={turn.depth}
        >
            <p className="atlas-chat-question" data-testid="atlas-chat-question">
                <span className="atlas-chat-prompt">{'>'}</span> {turn.question}
            </p>

            {/*
              * The substitution, in its own line and above the answer.
              *
              * Above, because it changes what the lines below are about, and a
              * note under an answer is read after the answer has been believed.
              * In the product's voice, because the model was never told that the
              * written name failed and cannot be the one to report it.
              */}
            {turn.focusFallback !== undefined && (
                <p className="atlas-chat-fallback" data-testid="atlas-chat-fallback">
                    {chatFocusFallbackLine(turn.focusFallback.asked, turn.focusFallback.used)}
                </p>
            )}

            <ChoiceList turn={turn} onPick={props.onPickCandidate} />

            {(aiLines ?? shown).length > 0 && (
                <div
                    className="atlas-chat-answer"
                    data-testid={aiLines === undefined ? 'atlas-chat-answer' : 'atlas-chat-ai-answer'}
                    data-version={aiLines === undefined ? 'built' : 'ai'}
                >
                    {(aiLines ?? shown).map((line, index) => (
                        <p className="atlas-chat-line" data-testid="atlas-chat-line" key={`line-${index}`}>
                            {segmentsOf(line, cardIds).map((segment, position) =>
                                segment.kind === 'text' ? (
                                    <span key={`s-${position}`}>{segment.text}</span>
                                ) : segment.known ? (
                                    <Hint
                                        key={`s-${position}`}
                                        name="chat-citation"
                                        text={(byId.get(segment.cardId)?.lines ?? []).join(' ')}
                                    >
                                        <button
                                            type="button"
                                            className="atlas-chat-citation"
                                            data-testid="atlas-chat-citation"
                                            data-card={segment.cardId}
                                            onClick={() => {
                                                const source = byId.get(segment.cardId)?.source;
                                                if (source !== undefined) {
                                                    props.onOpenCard(source);
                                                }
                                            }}
                                        >
                                            {segment.text}
                                        </button>
                                    </Hint>
                                ) : (
                                    <span
                                        className="atlas-chat-citation-unknown"
                                        data-testid="atlas-chat-citation-unknown"
                                        key={`s-${position}`}
                                    >
                                        {segment.text}
                                    </span>
                                ),
                            )}
                        </p>
                    ))}
                </div>
            )}

            {message.length > 0 && (
                <p className="atlas-chat-message" data-testid="atlas-chat-message" data-status={turn.status}>
                    {message}
                </p>
            )}

            {turn.status === 'answered' && (
                <div className="atlas-chat-ai" data-testid="atlas-chat-ai" data-state={aiState}>
                    {props.aiAvailable && builtLines.length > 0 && aiState !== 'applied' && (
                        <button type="button" data-testid="atlas-chat-ai-btn" disabled={aiState === 'running'} onClick={askAi}>
                            {CHAT_AI_LABEL}
                        </button>
                    )}
                    {props.aiAvailable && aiState === 'applied' && (
                        <button type="button" data-testid="atlas-chat-ai-restore" onClick={() => { setAiLines(undefined); setAiState('idle'); }}>
                            {CHAT_AI_RESTORE}
                        </button>
                    )}
                    <p data-testid="atlas-chat-built-provenance">{CHAT_BUILT_PROVENANCE}</p>
                    {aiLines !== undefined && <>
                        <p data-testid="atlas-chat-ai-provenance">{chatAiProvenance(props.modelName ?? '')}</p>
                    </>}
                    {aiState === 'refused' && <p data-testid="atlas-chat-ai-refused">{aiReason}</p>}
                </div>
            )}

            {turn.truncated === true && (
                <p className="atlas-chat-warning" data-testid="atlas-chat-warning">
                    {CHAT_TRUNCATED}
                </p>
            )}

            {uncited.length > 0 && (
                <p className="atlas-chat-warning" data-testid="atlas-chat-warning">
                    {chatCitationWarning(uncited.length)}
                </p>
            )}
            {unknown.length > 0 && (
                <p className="atlas-chat-warning" data-testid="atlas-chat-warning">
                    {chatUnknownCardWarning(unknown)}
                </p>
            )}

            {turn.cards.length > 0 && (
                <div className="atlas-chat-cards" data-testid="atlas-chat-cards" data-open={open}>
                    <button
                        type="button"
                        className="atlas-chat-cards-toggle"
                        data-testid="atlas-chat-cards-toggle"
                        aria-expanded={open}
                        data-fold={open ? 'collapse' : 'open'}
                        data-fold-of="cards"
                        onClick={() => setOpen((current) => !current)}
                    >
                        {chatCardsFoldLabel(open)}
                        {' '}
                        {chatCardsLabel(turn.cards.length)}
                    </button>
                    {open && (
                        <ul className="atlas-chat-card-list">
                            {turn.cards.map((card) => (
                                <CardRow card={card} key={card.id} onOpen={props.onOpenCard} />
                            ))}
                        </ul>
                    )}
                </div>
            )}

            {turn.cards.length > 0
                && turn.klass !== undefined
                && turn.tokens !== undefined
                && turn.budget !== undefined && (
                <p className="atlas-chat-provenance" data-testid="atlas-chat-provenance">
                    {chatProvenance(turn.klass, turn.rule ?? '', turn.tokens, turn.budget)}
                </p>
            )}

            {turn.sources.length > 0 && (
                <ul className="atlas-chat-gaps" data-testid="atlas-chat-sources">
                    {turn.sources.map((source, index) => (
                        <li key={`source-${index}`}>{messages.chat.readPrefix}{source}</li>
                    ))}
                </ul>
            )}

            {turn.gaps.length > 0 && (
                <ul className="atlas-chat-gaps" data-testid="atlas-chat-gaps">
                    {turn.gaps.map((gap, index) => (
                        <li key={`gap-${index}`}>{gap}</li>
                    ))}
                </ul>
            )}

            {rerun && (
                <Hint
                    name="chat-rerun"
                    text={chatRerunTooltip(turn.depth, props.depth)}
                >
                    <button
                        type="button"
                        className="atlas-chat-rerun"
                        data-testid="atlas-chat-rerun"
                        data-was={turn.depth}
                        data-now={props.depth}
                        onClick={() => props.onAskAgain(turn)}
                    >
                        {chatRerunLabel(props.depth)}
                    </button>
                </Hint>
            )}
        </article>
    );
}

export interface AtlasChatReaderSeam {
    subject: readonly { id: string; text: string; facts: readonly string[] }[];
    validateRewrite: (answer: string) => { applied: boolean; reason: string };
}

declare global {
    // eslint-disable-next-line no-var
    var __atlasChatReader: AtlasChatReaderSeam | undefined;
}

export default function AtlasChatPanel(props: AtlasChatPanelProps): JSX.Element {
    return (
        <section
            className="atlas-chat"
            data-testid="atlas-chat"
            data-turns={props.turns.length}
            aria-label={messages.chat.panelLabel}
        >
            <div className="atlas-chat-head" data-hint-keep="chat head" data-testid="atlas-chat-head">
                <span className="atlas-chat-title">{CHAT_TITLE}</span>
                <span
                    className="atlas-chat-depth"
                    data-testid="atlas-chat-depth"
                    data-depth={props.depth}
                    role="group"
                    aria-label={messages.chat.depthGroupLabel}
                >
                    <span className="atlas-chat-depth-label">{CHAT_DEPTH_LABEL}</span>
                    {CHAT_DEPTH_OPTIONS.map((option) => (
                        <Hint key={option.value} name={`chat-depth-${option.value}`} text={option.title}>
                            <button
                                type="button"
                                className="atlas-chat-depth-option"
                                data-testid="atlas-chat-depth-option"
                                data-value={option.value}
                                data-on={props.depth === option.value}
                                aria-pressed={props.depth === option.value}
                                onClick={() => props.onDepth(option.value)}
                            >
                                {option.label}
                            </button>
                        </Hint>
                    ))}
                </span>
                {/*
                  * Der eine Knopf, der loescht, und er steht allein.
                  *
                  * Bis W7c war er der einzige Weg, das Panel loszuwerden, und
                  * kostete dabei den Verlauf; W7c stellte einen zweiten daneben,
                  * der nur zuklappte. Seit W8 gehoert das Zuklappen der Zone,
                  * also steht hier wieder einer. Der Unterschied, um den es dem
                  * Nutzer ging, ist damit nicht verschwunden, sondern schaerfer:
                  * ES GIBT hier nur noch den, der loescht, und sein Tooltip sagt
                  * ausdruecklich, dass Zuklappen nichts kostet.
                  */}
                <Hint name="chat-clear" text={messages.chat.clearTooltip}>
                    <button
                        type="button"
                        className="atlas-chat-clear"
                        data-testid="atlas-chat-clear"
                        onClick={props.onClear}
                    >
                        {messages.chat.clear}
                    </button>
                </Hint>
            </div>

            <div className="atlas-chat-scroll" data-testid="atlas-chat-scroll">
                <p className="atlas-chat-depth-note" data-testid="atlas-chat-depth-note">
                    {CHAT_DEPTH_NOTE}
                </p>

                <div className="atlas-chat-turns">
                    {props.turns.map((turn, index) => (
                        <Turn
                            key={turn.id}
                            turn={turn}
                            depth={props.depth}
                            offerRerun={index === props.turns.length - 1}
                            onOpenCard={props.onOpenCard}
                            onPickCandidate={props.onPickCandidate}
                            onAskAgain={props.onAskAgain}
                            aiAvailable={props.aiAvailable === true}
                            modelName={props.modelName ?? ''}
                            requestModel={props.requestModel}
                        />
                    ))}
                </div>
            </div>
        </section>
    );
}
